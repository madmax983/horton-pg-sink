//! Live-Postgres tests for [`PgTableSink`][horton_pg_sink::PgTableSink],
//! the byte-preserving Postgres [`ColdSink`][horton_sweeper::ColdSink].
//!
//! Needs a live Postgres. URL comes from `HORTON_PG_SINK_URL`, defaulting
//! to the local unix socket as the current OS user:
//! `host=/var/run/postgresql dbname=postgres` (same as `pg_sink.rs`).
//!
//! The trait is synchronous, so these are plain `#[test]` functions — the
//! sink drives its private runtime internally.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::task::{Context, Poll};

use horton::{BlockDevice, Config, Db};
use horton_pg_sink::PgTableSink;
use horton_pg_sink::device::MemDevice;
use horton_sweeper::{ColdSink, Sweeper, SweeperConfig, TableDescriptor};

/// The tests share one Postgres; serialize them.
static PG_LOCK: Mutex<()> = Mutex::new(());

type SinkDb = Db<MemDevice<4096>, 4096, 32, 2048, 64, 65536, 4, 4, 256, 0>;

fn pg_url() -> String {
    std::env::var("HORTON_PG_SINK_URL")
        .unwrap_or_else(|_| "host=/var/run/postgresql dbname=postgres".to_string())
}

fn test_config() -> Config {
    Config::new(8, 520, 520, 4224, 0, 4)
}

fn connect() -> PgTableSink {
    let sink = PgTableSink::connect(&pg_url()).expect("connect to Postgres");
    sink.clear_all().expect("truncate cold tables");
    sink
}

fn desc(id: u32, blocks: u32) -> TableDescriptor {
    TableDescriptor {
        level: 0,
        table_id: id,
        min_seq: 1,
        max_seq: 50,
        block_count: blocks,
        entry_count: 50,
        node_id: 7,
        seal_wall_clock: 1_759_000_000,
    }
}

fn block_on<F: std::future::Future>(mut f: F) -> F::Output {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: never moved after pinning; dropped before return.
    let mut f = unsafe { std::pin::Pin::new_unchecked(&mut f) };
    loop {
        match std::pin::Pin::as_mut(&mut f).poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn noop_waker() -> std::task::Waker {
    unsafe fn noop(_: *const ()) {}
    unsafe fn clone(data: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(data, &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: no-ops; the null pointer is never touched.
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Reads one raw block from the device.
fn read_block(device: &MemDevice<4096>, id: u64, buf: &mut [u8]) {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match device.poll_read_block(&mut cx, id, buf) {
        Poll::Ready(Ok(())) => {}
        Poll::Ready(Err(e)) => panic!("device read: {e:?}"),
        Poll::Pending => panic!("device pended"),
    }
}

/// Raw table bytes straight from the device, in table order — the
/// byte-exact reference the sink must reproduce.
fn table_bytes(db: &SinkDb, level: usize, id: u32) -> Vec<u8> {
    let plan = db.archive_plan(level, id).expect("plan");
    let mut out = Vec::new();
    let mut buf = vec![0u8; 4096];
    let mut b = plan.table.first_block;
    let end = plan.table.end_block();
    while b < end {
        read_block(db.device(), b, &mut buf);
        out.extend_from_slice(&buf);
        b += 1;
    }
    out
}

#[test]
fn seal_stream_commit_read_roundtrip() {
    let _guard = PG_LOCK.lock().unwrap();
    let mut sink = connect();

    let d = desc(42, 3);
    let mut handle = sink.seal(&d).unwrap();
    assert!(!handle.complete);
    let blocks: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 512]).collect();
    for b in &blocks {
        sink.write_block(&mut handle, b).unwrap();
    }
    let receipt = sink.commit(handle).unwrap();
    assert_eq!(receipt.table_id, 42);
    assert_eq!(receipt.block_count, 3);
    assert_eq!(receipt.bytes, 3 * 512);

    // Byte-exact read-back.
    let stored: Vec<u8> = blocks.concat();
    assert_eq!(sink.read_table(42).unwrap().unwrap(), stored);
    assert!(sink.read_table(43).unwrap().is_none());
    assert_eq!(sink.list_archived().unwrap(), vec![42]);
}

#[test]
fn idempotent_reseal_reissues_receipt() {
    let _guard = PG_LOCK.lock().unwrap();
    let mut sink = connect();

    let d = desc(7, 2);
    let mut h1 = sink.seal(&d).unwrap();
    sink.write_block(&mut h1, &[9u8; 128]).unwrap();
    sink.write_block(&mut h1, &[9u8; 128]).unwrap();
    let r1 = sink.commit(h1).unwrap();

    // Crash-retry: the same table sealed again is already complete;
    // commit re-issues the identical receipt without duplicating rows.
    let mut h2 = sink.seal(&d).unwrap();
    assert!(h2.complete);
    sink.write_block(&mut h2, &[1u8; 128]).unwrap(); // ignored
    let r2 = sink.commit(h2).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(sink.list_archived().unwrap(), vec![7]);
}

#[test]
fn short_stream_fails_commit_without_a_row() {
    let _guard = PG_LOCK.lock().unwrap();
    let mut sink = connect();

    let d = desc(9, 3);
    let mut h = sink.seal(&d).unwrap();
    sink.write_block(&mut h, &[1u8; 64]).unwrap(); // 1 of 3
    let err = sink.commit(h).unwrap_err();
    assert!(
        format!("{err}").contains("1 of 3"),
        "unexpected error: {err}"
    );
    assert!(sink.read_table(9).unwrap().is_none());
    assert!(sink.list_archived().unwrap().is_empty());
}

#[test]
fn reseal_with_different_descriptor_fails() {
    let _guard = PG_LOCK.lock().unwrap();
    let mut sink = connect();

    let d = desc(11, 2);
    let mut h = sink.seal(&d).unwrap();
    sink.write_block(&mut h, &[1u8; 64]).unwrap();
    sink.write_block(&mut h, &[1u8; 64]).unwrap();
    sink.commit(h).unwrap();

    // Same id, different shape: table ids are never reused, so this is
    // corruption, not a retry.
    let mut other = d.clone();
    other.block_count = 5;
    let err = sink.seal(&other).unwrap_err();
    assert!(
        format!("{err}").contains("different descriptor"),
        "unexpected error: {err}"
    );
}

/// End to end: real horton Db on a memory device → sweeper age sweep →
/// [`PgTableSink`] → byte-exact read-back against the source table.
#[test]
fn sweeper_to_postgres_end_to_end() {
    let _guard = PG_LOCK.lock().unwrap();
    let root: PathBuf = std::env::temp_dir().join(format!(
        "pg-cold-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&root).unwrap();

    let mut db = SinkDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..20u8 {
        block_on(db.put(&[i], &[i, i])).unwrap();
    }
    block_on(db.flush()).unwrap();
    for i in 20..25u8 {
        block_on(db.put(&[i], &[i, i])).unwrap();
    }
    block_on(db.flush()).unwrap();

    let ids: Vec<u32> = db.tables(0).unwrap().iter().map(|t| t.id).collect();
    assert_eq!(ids.len(), 2, "expected two sealed tables in L0");

    // Byte-exact references, captured before the sweep archives them away.
    let expected: Vec<Vec<u8>> = ids.iter().map(|id| table_bytes(&db, 0, *id)).collect();

    // Ancient stamps so the age sweep fires deterministically.
    let log_path = root.join("e2e.log");
    {
        let mut f = File::create(&log_path).unwrap();
        for (i, id) in ids.iter().enumerate() {
            writeln!(f, "{},{},{}", id, 20 + i, 1_700_000_000u64).unwrap();
        }
    }

    let config = SweeperConfig {
        age_threshold_secs: 60,
        min_free_slots: 1, // pressure sweep stays quiet
        max_tables_per_sweep: 8,
        node_id: 5,
    };
    let mut sweeper = Sweeper::new(connect(), &log_path, config).unwrap();
    let report = sweeper.sweep(&mut db).unwrap();

    assert_eq!(report.age_archived.len(), 2);
    assert!(report.pressure_archived.is_empty());
    assert!(report.skipped_would_resurrect.is_empty());
    assert_eq!(db.tables(0).unwrap().len(), 0);

    // Every archived table reads back byte-identical from Postgres.
    for (id, want) in ids.iter().zip(expected.iter()) {
        let got = sweeper.sink().read_table(*id).unwrap().unwrap();
        assert_eq!(&got, want, "table {id} bytes differ");
    }
    let mut archived = sweeper.sink().list_archived().unwrap();
    archived.sort_unstable();
    let mut want_ids = ids.clone();
    want_ids.sort_unstable();
    assert_eq!(archived, want_ids);

    std::fs::remove_dir_all(&root).unwrap();
}
