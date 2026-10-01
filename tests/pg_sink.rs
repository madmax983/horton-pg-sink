//! End-to-end: horton (in-memory device) -> archive_plan -> COPY ->
//! upsert + watermark -> archive_commit.
//!
//! Needs a live Postgres. URL comes from `HORTON_PG_SINK_URL`, defaulting
//! to the local unix socket as the current OS user:
//! `host=/var/run/postgresql dbname=postgres`.

use std::sync::{Arc, Mutex};

use horton::{Config, Db};
use horton_pg_sink::device::MemDevice;
use horton_pg_sink::mappers::HarvestEventMapper;
use horton_pg_sink::{PgSink, SinkError};

/// The two tests share one Postgres; serialize them.
static PG_LOCK: Mutex<()> = Mutex::new(());

type SinkDb = Db<MemDevice<4096>, 4096, 32, 2048, 64, 65536, 4, 4, 256, 0>;

fn pg_url() -> String {
    std::env::var("HORTON_PG_SINK_URL")
        .unwrap_or_else(|_| "host=/var/run/postgresql dbname=postgres".to_string())
}

fn test_config() -> Config {
    // WAL [8, 520): 512 blocks, comfortably more than the puts between
    // flushes (the WAL only wraps when full at flush time). Tables
    // [520, 4224); manifests at 0 and 4.
    Config::new(8, 520, 520, 4224, 0, 4)
}

fn target_for(test: &str) -> String {
    format!("sink_test_events_{test}")
}

async fn connect(target: &str) -> PgSink<HarvestEventMapper> {
    let mapper = HarvestEventMapper::new(target);
    let sink = PgSink::connect(&pg_url(), mapper)
        .await
        .expect("connect to Postgres");
    sink.ensure_schema().await.expect("watermark table");
    sink.client_exec(&format!("DROP TABLE IF EXISTS {target}"), &[])
        .await
        .expect("drop target");
    sink.client_exec(
        &format!(
            "CREATE TABLE {target} (\
           id               BIGSERIAL PRIMARY KEY,\
           workflow_exec_id UUID NOT NULL,\
           event_id         INT NOT NULL,\
           event_type       TEXT NOT NULL,\
           event_data       JSONB NOT NULL,\
           timestamp        TIMESTAMPTZ NOT NULL,\
           UNIQUE (workflow_exec_id, event_id)\
         )"
        ),
        &[],
    )
    .await
    .expect("create target");
    sink.client_exec("TRUNCATE horton_pg_sink_flushed", &[])
        .await
        .expect("clear watermark");
    sink
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    id
}

/// 3 workflows x 60 events, flushed in 4 batches -> 4 sealed tables.
/// `wf_base` offsets the workflow ids so two dbs can share one target.
async fn build_db(wf_base: u8) -> SinkDb {
    let mut db = SinkDb::new(MemDevice::new(), test_config());
    db.open().await.expect("open");
    let mut n = 0;
    for w in 0..3u8 {
        let wf = uuid_bytes(wf_base + w);
        for e in 0..60u32 {
            let (k, v) = HarvestEventMapper::encode(
                &wf,
                e,
                "ActivityCompleted",
                "2026-09-30T12:00:00Z",
                &format!(r#"{{"w":{w},"e":{e}}}"#),
            )
            .unwrap();
            db.put(&k, &v).await.expect("put");
            n += 1;
            if n % 45 == 0 {
                db.flush().await.expect("flush");
            }
        }
    }
    db.flush().await.expect("final flush");
    db
}

fn table_ids(db: &SinkDb) -> Vec<u32> {
    db.level_tables(0)
        .expect("level 0")
        .iter()
        .map(|t| t.id)
        .collect()
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // the lock only serializes the two PG tests
async fn archive_to_postgres_end_to_end() {
    let _guard = PG_LOCK.lock().unwrap();
    let target = target_for("e2e");
    let sink = connect(&target).await;
    let count_sql = format!("SELECT count(*) FROM {target}");
    let mut db = build_db(0).await;
    let ids = table_ids(&db);
    assert!(!ids.is_empty(), "expected sealed tables in L0");

    let mut total_copied = 0u64;
    for id in &ids {
        let rep = sink.sink_table(&mut db, 0, *id).await.expect("sink table");
        assert!(rep.uploaded, "first run must COPY");
        assert!(rep.committed, "first run must archive-commit");
        assert!(rep.entries_copied > 0);
        total_copied += rep.entries_copied;
    }
    assert_eq!(total_copied, 180, "3 workflows x 60 events");

    // All rows landed, keyed idempotently.
    let rows: i64 = sink
        .client_query_one(&count_sql, &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(rows, 180);

    // Spot-check one row's decoded fields.
    let wf0 = HarvestEventMapper::fmt_uuid(&uuid_bytes(0));
    let row = sink
        .client_query_one(
            &format!(
                "SELECT event_type, (event_data = '{{\"w\":0,\"e\":7}}'::jsonb) AS data_ok \
                 FROM {target} WHERE workflow_exec_id::text = $1 AND event_id = 7"
            ),
            &[&wf0],
        )
        .await
        .expect("spot check");
    let et: String = row.get(0);
    let data_ok: bool = row.get(1);
    assert_eq!(et, "ActivityCompleted");
    assert!(data_ok, "event_data jsonb mismatch");

    // Tables are gone locally.
    for id in &ids {
        assert!(db.archive_plan(0, *id).is_none(), "table {id} forgotten");
    }

    // Re-sinking is a no-op success (watermark replay): no duplicates,
    // no error.
    for id in &ids {
        let rep = sink.sink_table(&mut db, 0, *id).await.expect("re-sink");
        assert!(!rep.uploaded);
        assert!(!rep.committed);
    }
    let rows: i64 = sink
        .client_query_one(&count_sql, &[])
        .await
        .expect("count2")
        .get(0);
    assert_eq!(rows, 180, "no duplicates on replay");

    // Unknown table id with no watermark is an error, not silent success.
    assert!(sink.sink_table(&mut db, 0, 0xdead_beef).await.is_err());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // the lock only serializes the two PG tests
async fn crash_between_pg_commit_and_archive_commit_replays_cleanly() {
    let _guard = PG_LOCK.lock().unwrap();
    let target = target_for("crash");
    let sink = connect(&target).await;
    let mut db = build_db(0).await;
    let ids = table_ids(&db);
    let victim = ids[0];

    // Simulate the crash: Postgres has the watermark, horton still holds
    // the table (archive_commit never ran).
    sink.client_exec(
        "INSERT INTO horton_pg_sink_flushed (table_id, entries, max_seq) VALUES ($1, 45, 44)",
        &[&(victim as i32)],
    )
    .await
    .expect("fake watermark");
    assert!(db.archive_plan(0, victim).is_some(), "table still local");

    let rep = sink
        .sink_table(&mut db, 0, victim)
        .await
        .expect("replay sink");
    assert!(!rep.uploaded, "COPY must be skipped on watermark hit");
    assert!(rep.committed, "archive_commit must still run");
    assert!(db.archive_plan(0, victim).is_none());

    // And the rows that *were* flushed by the real sink of the other
    // tables are untouched.
    for id in ids.iter().skip(1) {
        sink.sink_table(&mut db, 0, *id).await.expect("sink rest");
    }
    let rows: i64 = sink
        .client_query_one(&format!("SELECT count(*) FROM {target}"), &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(rows, 135, "only the 3 really-sunk tables landed");
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // the lock only serializes the PG tests
async fn backpressure_lag_report_and_shed() {
    let _guard = PG_LOCK.lock().unwrap();
    let target = target_for("shed");
    let mut sink = connect(&target).await;
    let mut db = build_db(0).await;
    let ids = table_ids(&db);
    assert_eq!(ids.len(), 4);

    // Backlog starts at 4 tables.
    let lag = sink.lag_report(&db, 0).await.expect("lag");
    assert_eq!(lag.tables.len(), 4);

    // Threshold 2: sinking refuses with ShedLoad, table untouched.
    sink.set_shed_threshold(Some(2));
    let err = sink.sink_table(&mut db, 0, ids[0]).await.unwrap_err();
    match err {
        SinkError::ShedLoad { pending, threshold } => {
            assert_eq!((pending, threshold), (4, 2));
        }
        other => panic!("expected ShedLoad, got {other:?}"),
    }
    assert!(
        db.archive_plan(0, ids[0]).is_some(),
        "shed table stays local"
    );
    let rows: i64 = sink
        .client_query_one(&format!("SELECT count(*) FROM {target}"), &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(rows, 0, "nothing uploaded while shedding");

    // Watermarked replays are cheap: they skip the shed check.
    sink.client_exec(
        "INSERT INTO horton_pg_sink_flushed (table_id, entries, max_seq) VALUES ($1, 45, 44)",
        &[&(ids[0] as i32)],
    )
    .await
    .expect("fake watermark");
    let rep = sink.sink_table(&mut db, 0, ids[0]).await.expect("replay");
    assert!(!rep.uploaded && rep.committed);

    // Backlog shrank; disable shedding and drain normally.
    let lag = sink.lag_report(&db, 0).await.expect("lag2");
    assert_eq!(lag.tables.len(), 3);
    sink.set_shed_threshold(None);
    for id in ids.iter().skip(1) {
        sink.sink_table(&mut db, 0, *id).await.expect("drain");
    }
    let lag = sink.lag_report(&db, 0).await.expect("lag3");
    assert!(lag.is_empty(), "backlog drained");
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // the lock only serializes the PG tests
async fn concurrent_sinks_share_one_sink() {
    let _guard = PG_LOCK.lock().unwrap();
    let target = target_for("concurrent");
    let sink = Arc::new(connect(&target).await);
    let db = build_db(0).await;
    let ids = table_ids(&db);
    assert_eq!(ids.len(), 4);

    // The Db is !Send (no_alloc internals), so the concurrent tasks
    // run on one thread via a LocalSet. Four tasks, one shared `&self`
    // sink: the client mutex serializes the uploads and the per-table
    // staging names keep them apart. The db moves into the LocalSet
    // future (one copy — the Db is a hundred-KB stack value).
    let local = tokio::task::LocalSet::new();
    let total = local
        .run_until(async {
            let db = std::rc::Rc::new(tokio::sync::Mutex::new(db));
            let mut handles = Vec::new();
            for id in ids {
                let (sink_c, db_c) = (Arc::clone(&sink), std::rc::Rc::clone(&db));
                handles.push(tokio::task::spawn_local(async move {
                    let mut g = db_c.lock().await;
                    sink_c.sink_table(&mut g, 0, id).await
                }));
            }
            let mut total = 0u64;
            for h in handles {
                let rep = h.await.expect("task panicked").expect("sink");
                assert!(rep.uploaded && rep.committed);
                total += rep.entries_copied;
            }
            total
        })
        .await;
    assert_eq!(total, 180);
    let rows: i64 = sink
        .client_query_one(&format!("SELECT count(*) FROM {target}"), &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(rows, 180, "concurrent uploads landed exactly once");
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // the lock only serializes the PG tests
async fn range_tombstone_table_fails_fast() {
    let _guard = PG_LOCK.lock().unwrap();
    let target = target_for("tombstone");
    let sink = connect(&target).await;
    let mut db = SinkDb::new(MemDevice::new(), test_config());
    db.open().await.expect("open");
    let wf = uuid_bytes(0);
    let mut first = Vec::new();
    let mut last = Vec::new();
    for e in 0..10u32 {
        let (k, v) = HarvestEventMapper::encode(&wf, e, "T", "2026-09-30T12:00:00Z", "{}").unwrap();
        if e == 0 {
            first = k.clone();
        }
        last = k.clone();
        db.put(&k, &v).await.expect("put");
    }
    db.delete_range(&first, &last).await.expect("delete_range");
    db.flush().await.expect("flush");
    let ids = table_ids(&db);
    assert_eq!(ids.len(), 1);

    // The sealed table carries range-tombstone blocks: the sink must
    // refuse before burning a COPY, and leave the table local.
    let err = sink.sink_table(&mut db, 0, ids[0]).await.unwrap_err();
    assert!(
        matches!(err, SinkError::TombstonesWouldResurrect { .. }),
        "got {err:?}"
    );
    assert!(db.archive_plan(0, ids[0]).is_some(), "table stays local");
    let rows: i64 = sink
        .client_query_one(&format!("SELECT count(*) FROM {target}"), &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(rows, 0, "nothing uploaded for tombstoned table");
}
