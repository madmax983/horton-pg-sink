# horton-pg-sink

Postgres sink for horton's [archive API](https://github.com/madmax983/horton)
(`archive_plan` / `archive_commit`). Two sinks live here — pick one:

- **`PgSink`** — drains a table's *entries* into Postgres *rows* via
  batched `COPY` (Postgres becomes the queryable source of truth). The
  motivating deployment is the Harvest event tail: horton absorbs a
  write storm as a durable LSM buffer. Insert-only workloads.
- **`PgTableSink`** — implements
  [`ColdSink`](https://github.com/madmax983/horton-sweeper) from the
  `horton-sweeper` crate: stores the table's *immutable bytes* plus its
  descriptor in one `horton_cold_tables` row (`BYTEA` blob, CRC-64
  verified at commit, idempotent per table id). Byte-exact `read_table`
  for the sweeper's cold-read path and replica bootstrap. Any workload;
  tombstones are simply stored as bytes (the sweeper + `archive_commit`
  own the tombstone policy).

## The tier story

- **Default:** Postgres only. One store, nothing to operate.
- **+ Redis:** faster claims (drain side). Already exists in Harvest.
- **+ horton:** faster event writes (write side). This crate is the
  drain half of that tier: sealed SSTable → `COPY` → Postgres.

Each tier is opt-in and killable: if horton (or the sink) is down,
writes fall straight through to Postgres.

## Protocol (per sealed table)

1. `archive_plan(level, table_id)` → descriptor
   (`first_key` / `last_key` / `max_seq` / `entry_count`).
2. If `horton_pg_sink_flushed` already holds the table id, the `COPY`
   is skipped — this is the crash-recovery path. A crash between the
   Postgres commit and `archive_commit` replays here without
   duplicating rows.
3. Otherwise: scan `[first_key, successor(last_key))` with `max_seq`
   pinned, stream entries through the `RowMapper` into a temp staging
   table via `COPY ... FROM STDIN`, then in **one transaction**
   `INSERT ... ON CONFLICT DO NOTHING` into the target plus the
   watermark row.
4. `archive_commit(level, table_id)` drops the table from horton's
   manifest — strictly after the Postgres commit, so no crash can lose
   acknowledged data.

The scan is a merged view, so entries from overlapping newer tables in
the same key range may ride along; the upsert makes that harmless, and
their own tables re-deliver them later. Correct for **insert-only**
workloads (event logs). Tables carrying tombstones are refused by
`archive_commit` with `WouldResurrect`; the sink surfaces
`SinkError::TombstonesWouldResurrect` and leaves the table local —
compact it down first.

## Key/value contract

`RowMapper` owns the contract. The bundled `HarvestEventMapper`
targets a `harvest_events`-shaped table:

- Key (20 B): `workflow_exec_id` (16-byte UUID) ++ `event_id` (`u32` BE).
  Sort order groups one workflow's events in `event_id` order.
- Value envelope: `u8 len ++ event_type ++ u8 len ++ timestamp (RFC 3339)
  ++ event_data (UTF-8 JSON)`. Length-prefixed, no JSON parser needed in
  the mapper; the JSON passes through to the `JSONB` column untouched.
- Conflict key `(workflow_exec_id, event_id)` — the idempotency anchor.

## Running the tests

```sh
# needs a live Postgres; PG 16 cluster on localhost works
export HORTON_PG_SINK_URL="host=/var/run/postgresql dbname=sinktest"
cargo test
```

Integration tests (real Postgres) cover the full archive → COPY →
watermark → commit flow, watermark replay idempotency, the
crash-between-commit-and-archive replay, unknown table ids, the lag
gauge + load shedding, concurrent sinks sharing one sink, and the
tombstone fail-fast.

## Backpressure

`sink.lag_report(&db, level)` lists sealed tables with no watermark
row — the sink's backlog, oldest first. `sink.set_shed_threshold(n)`
arms load shedding: when the backlog reaches `n`, `sink_table`
returns `SinkError::ShedLoad` instead of sinking, so the caller can
route writes straight to Postgres until the backlog drains.
Watermarked replays are cheap and never shed.

## PgTableSink protocol (byte-preserving cold sink)

One `horton_cold_tables` row per sealed table: descriptor columns
(`level`, `min_seq`/`max_seq`, `block_count`, `entry_count`,
`node_id`, `seal_wall_clock`) plus `bytes`, `checksum` (CRC-64), and
the `data` blob.

1. `seal(descriptor)` — if the row exists with an identical
   descriptor, returns a complete handle (idempotent re-seal); if it
   exists with a *different* descriptor, errors (table ids are never
   reused, so that is corruption, not a retry).
2. `write_block` — appends to an in-memory staging buffer, updating a
   running CRC-64; overlong streams are rejected.
3. `commit` — refuses short streams, verifies the checksum, then
   `INSERT ... ON CONFLICT (table_id) DO NOTHING` and re-reads the row
   to confirm descriptor + checksum before issuing the `Receipt`.
4. `read_table` / `list_archived` — byte-exact blobs for the cold-read
   path and replica bootstrap.

The trait is synchronous (the sweeper is a sync policy loop); the sink
owns a private single-worker tokio runtime and drives every statement
through it. Don't call it from inside another tokio runtime context.
Crash-retry is the same story as `PgSink`: a crash anywhere before
`archive_commit` replays the upload, and the committed-row check makes
the replay a no-op.

## Honest gaps

- **The scan re-reads overlapping ranges.** A dedicated per-table
  iterator in horton (there is none public today) would make the COPY
  strictly one-table; the upsert absorbs the overlap for now. The
  merged scan is also the *durable* choice: if an older table's sink
  failed, a newer table's scan carries its entries forward.
- **One horton database per Postgres database.** Watermarks are keyed
  by table id alone, and table ids are only unique within one horton
  database. Two horton databases sinking into the same Postgres
  database would collide on watermarks.
- **WAL sizing is the operator's job.** The WAL only wraps when full at
  flush time — size the WAL region for puts-between-flushes (the test
  uses 512 blocks for 45 puts/flush).
- **No deletes.** Insert-only by design; tombstone-bearing tables are
  refused, not handled. Range tombstones fail fast before the COPY;
  point tombstones still surface at `archive_commit` time.
- **True parallel drain needs horton's cooperation.** `sink_table`
  takes `&self` and concurrent calls are safe (client mutex +
  per-table staging names), but one `Db` still needs `&mut` for
  `archive_commit`, so parallel sinking of a single database would
  need interior mutability in horton itself.
