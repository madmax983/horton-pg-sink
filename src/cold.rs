//! Byte-preserving Postgres [`ColdSink`][horton_sweeper::ColdSink]:
//! sealed table bytes live in Postgres.
//!
//! This is the *other* Postgres sink in this crate, next to
//! [`PgSink`][crate::PgSink], and the two serve different deployments:
//!
//! - [`PgSink`][crate::PgSink] drains a table's *entries* into Postgres
//!   *rows* via `COPY` (Postgres becomes the queryable source of truth;
//!   the Harvest event tail). Table bytes are not preserved.
//! - `PgTableSink` stores the table's *immutable bytes* plus its
//!   [`TableDescriptor`][horton_sweeper::TableDescriptor] in one row, so
//!   [`read_table`][horton_sweeper::ColdSink::read_table] returns
//!   byte-exact blobs for the sweeper's cold-read path and replica
//!   bootstrap. This is the first-party Postgres shape of the
//!   tiered-sweeper contract.
//!
//! # Discipline (reused from `PgSink`)
//!
//! - **Idempotent per table id.** `seal` on an already-stored table with
//!   an identical descriptor returns a complete handle; `commit`
//!   re-issues the same [`Receipt`][horton_sweeper::Receipt]. The row is
//!   written with `INSERT ... ON CONFLICT (table_id) DO NOTHING`, so a
//!   crash anywhere before [`horton::Db::archive_commit`] replays safely.
//! - **Checksum verified.** `commit` refuses short streams and verifies
//!   the streamed bytes against the running CRC-64 before acknowledging.
//! - **Tombstones.** This sink is opaque to entries — it never interprets
//!   tombstones, so there is nothing to fail fast on at the sink level.
//!   The fail-fast lives where it belongs: the sweeper's victim
//!   selection skips tables with range-tombstone blocks, and horton's
//!   [`archive_commit`][horton::Db::archive_commit] remains the final
//!   authority (a `WouldResurrect` refusal leaves the table local).
//!
//! # Sync bridge
//!
//! The [`ColdSink`][horton_sweeper::ColdSink] trait is synchronous (the
//! sweeper is a sync policy loop); Postgres is async. The sink owns a
//! private single-worker tokio runtime and drives every statement
//! through it. Do not call the trait methods from inside another tokio
//! runtime context — `block_on` from within a runtime panics. The
//! sweeper never does; tests use plain `#[test]` functions.

use std::collections::HashMap;

use horton_sweeper::crc64::{self, Rolling};
use horton_sweeper::{ColdSink, Receipt, SealHandle, TableDescriptor};
use tokio_postgres::types::ToSql;

use crate::SinkError;

/// One row of `horton_cold_tables`: the descriptor plus receipt fields.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRow {
    level: i32,
    min_seq: i64,
    max_seq: i64,
    block_count: i32,
    entry_count: i32,
    node_id: i32,
    bytes: i64,
    /// Bit-cast `u64` CRC-64 (values above `i64::MAX` survive the cast).
    checksum: i64,
}

/// Whether a stored row describes the same table the descriptor declares
/// (the idempotency check: same table, same bytes expected).
fn row_matches(desc: &TableDescriptor, row: &StoredRow) -> bool {
    row.level == desc.level as i32
        && row.min_seq == desc.min_seq as i64
        && row.max_seq == desc.max_seq as i64
        && row.block_count == desc.block_count as i32
        && row.entry_count == desc.entry_count as i32
        && row.node_id == desc.node_id as i32
}

fn receipt_from_row(table_id: u32, row: &StoredRow) -> Receipt {
    Receipt {
        table_id,
        bytes: row.bytes as u64,
        checksum: row.checksum as u64,
        block_count: row.block_count as u32,
    }
}

/// An in-progress upload: pure state machine, no I/O (unit-tested
/// directly). The sink keeps one per open seal, keyed by table id.
struct Upload {
    desc: TableDescriptor,
    buf: Vec<u8>,
    rolling: Rolling,
    blocks_written: u32,
}

impl Upload {
    fn new(desc: &TableDescriptor) -> Self {
        Self {
            desc: desc.clone(),
            buf: Vec::new(),
            rolling: Rolling::new(),
            blocks_written: 0,
        }
    }

    fn write_block(&mut self, block: &[u8]) -> Result<(), SinkError> {
        if self.blocks_written >= self.desc.block_count {
            return Err(SinkError::Protocol(format!(
                "table {}: block stream longer than the declared {} blocks",
                self.desc.table_id, self.desc.block_count
            )));
        }
        self.buf.extend_from_slice(block);
        self.rolling.update(block);
        self.blocks_written += 1;
        Ok(())
    }

    /// Verifies the stream (block count, then checksum) and returns the
    /// bytes with their CRC-64.
    fn finish(self) -> Result<(Vec<u8>, u64), SinkError> {
        if self.blocks_written != self.desc.block_count {
            return Err(SinkError::Protocol(format!(
                "table {}: stream has {} of {} declared blocks",
                self.desc.table_id, self.blocks_written, self.desc.block_count
            )));
        }
        let checksum = self.rolling.finish();
        // Belt and braces: the running checksum must agree with a fresh
        // pass over the buffered bytes.
        if crc64::checksum(&self.buf) != checksum {
            return Err(SinkError::Protocol(format!(
                "table {}: checksum mismatch in staged bytes",
                self.desc.table_id
            )));
        }
        Ok((self.buf, checksum))
    }
}

/// Byte-preserving Postgres cold sink. See the [module](self) docs.
pub struct PgTableSink {
    client: tokio_postgres::Client,
    rt: tokio::runtime::Runtime,
    staging: HashMap<u32, Upload>,
}

impl PgTableSink {
    /// Connects, spawns the connection driver on a private runtime, and
    /// creates the storage table if it is missing.
    ///
    /// # Errors
    ///
    /// [`SinkError`] when the runtime cannot start, Postgres is
    /// unreachable, or the schema cannot be created.
    pub fn connect(url: &str) -> Result<Self, SinkError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| SinkError::Protocol(format!("tokio runtime: {e}")))?;
        let client = rt.block_on(async {
            let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Result::<_, tokio_postgres::Error>::Ok(client)
        })?;
        rt.block_on(Self::ensure_schema(&client))?;
        Ok(Self {
            client,
            rt,
            staging: HashMap::new(),
        })
    }

    /// Deletes every archived table row (test/support).
    ///
    /// # Errors
    ///
    /// [`SinkError`] when Postgres fails.
    pub fn clear_all(&self) -> Result<(), SinkError> {
        let client = &self.client;
        self.rt.block_on(async {
            client
                .execute("TRUNCATE horton_cold_tables", &[])
                .await
                .map_err(SinkError::from)?;
            Ok::<(), SinkError>(())
        })
    }

    async fn ensure_schema(client: &tokio_postgres::Client) -> Result<(), SinkError> {
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS horton_cold_tables (\
                   table_id        INTEGER PRIMARY KEY,\
                   level           INTEGER NOT NULL,\
                   min_seq         BIGINT NOT NULL,\
                   max_seq         BIGINT NOT NULL,\
                   block_count     INTEGER NOT NULL,\
                   entry_count     INTEGER NOT NULL,\
                   node_id         INTEGER NOT NULL,\
                   seal_wall_clock BIGINT NOT NULL,\
                   bytes           BIGINT NOT NULL,\
                   checksum        BIGINT NOT NULL,\
                   data            BYTEA NOT NULL,\
                   stored_at       TIMESTAMPTZ NOT NULL DEFAULT now()\
                 )",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn find_row(
        client: &tokio_postgres::Client,
        table_id: u32,
    ) -> Result<Option<StoredRow>, SinkError> {
        let row = client
            .query_opt(
                "SELECT level, min_seq, max_seq, block_count, entry_count, \
                        node_id, bytes, checksum \
                 FROM horton_cold_tables WHERE table_id = $1",
                &[&(table_id as i32)],
            )
            .await?;
        Ok(row.map(|r| StoredRow {
            level: r.get(0),
            min_seq: r.get(1),
            max_seq: r.get(2),
            block_count: r.get(3),
            entry_count: r.get(4),
            node_id: r.get(5),
            bytes: r.get(6),
            checksum: r.get(7),
        }))
    }

    /// Stores the table row; a concurrent or replayed commit for the same
    /// table id is absorbed by `ON CONFLICT DO NOTHING`.
    async fn insert_row(
        client: &tokio_postgres::Client,
        desc: &TableDescriptor,
        bytes: &[u8],
        checksum: u64,
    ) -> Result<(), SinkError> {
        let params: &[&(dyn ToSql + Sync)] = &[
            &(desc.table_id as i32),
            &(desc.level as i32),
            &(desc.min_seq as i64),
            &(desc.max_seq as i64),
            &(desc.block_count as i32),
            &(desc.entry_count as i32),
            &(desc.node_id as i32),
            &(desc.seal_wall_clock as i64),
            &(bytes.len() as i64),
            &(checksum as i64),
            &bytes,
        ];
        client
            .execute(
                "INSERT INTO horton_cold_tables \
                   (table_id, level, min_seq, max_seq, block_count, entry_count, \
                    node_id, seal_wall_clock, bytes, checksum, data) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                 ON CONFLICT (table_id) DO NOTHING",
                params,
            )
            .await?;
        Ok(())
    }
}

impl ColdSink for PgTableSink {
    type Error = SinkError;

    fn seal(&mut self, desc: &TableDescriptor) -> Result<SealHandle, SinkError> {
        let client = &self.client;
        if let Some(row) = self.rt.block_on(Self::find_row(client, desc.table_id))? {
            if !row_matches(desc, &row) {
                return Err(SinkError::Protocol(format!(
                    "table {} already archived with a different descriptor",
                    desc.table_id
                )));
            }
            // Idempotent re-seal: the bytes are already durable.
            return Ok(SealHandle {
                table_id: desc.table_id,
                expected_blocks: desc.block_count,
                blocks_written: desc.block_count,
                checksum: row.checksum as u64,
                complete: true,
            });
        }
        self.staging.insert(desc.table_id, Upload::new(desc));
        Ok(SealHandle {
            table_id: desc.table_id,
            expected_blocks: desc.block_count,
            blocks_written: 0,
            checksum: Rolling::new().finish(),
            complete: false,
        })
    }

    fn write_block(&mut self, handle: &mut SealHandle, block: &[u8]) -> Result<(), SinkError> {
        if handle.complete {
            // Idempotent re-upload of an already-stored table.
            return Ok(());
        }
        let upload = self.staging.get_mut(&handle.table_id).ok_or_else(|| {
            SinkError::Protocol(format!(
                "table {}: write_block without an open seal",
                handle.table_id
            ))
        })?;
        upload.write_block(block)?;
        handle.blocks_written = upload.blocks_written;
        handle.checksum = upload.rolling.finish();
        Ok(())
    }

    fn commit(&mut self, handle: SealHandle) -> Result<Receipt, SinkError> {
        let client = &self.client;
        if handle.complete {
            let row = self
                .rt
                .block_on(Self::find_row(client, handle.table_id))?
                .ok_or_else(|| {
                    SinkError::Protocol(format!(
                        "table {}: complete handle without a stored row",
                        handle.table_id
                    ))
                })?;
            return Ok(receipt_from_row(handle.table_id, &row));
        }
        let upload = self.staging.remove(&handle.table_id).ok_or_else(|| {
            SinkError::Protocol(format!(
                "table {}: commit without an open seal",
                handle.table_id
            ))
        })?;
        let desc = upload.desc.clone();
        let (bytes, checksum) = upload.finish()?;
        self.rt
            .block_on(Self::insert_row(client, &desc, &bytes, checksum))?;
        let row = self
            .rt
            .block_on(Self::find_row(client, handle.table_id))?
            .ok_or_else(|| {
                SinkError::Protocol(format!(
                    "table {}: row missing after insert",
                    handle.table_id
                ))
            })?;
        if !row_matches(&desc, &row) || row.checksum as u64 != checksum {
            return Err(SinkError::Protocol(format!(
                "table {}: stored row disagrees with the committed upload",
                handle.table_id
            )));
        }
        Ok(receipt_from_row(handle.table_id, &row))
    }

    fn read_table(&self, table_id: u32) -> Result<Option<Vec<u8>>, SinkError> {
        let client = &self.client;
        let row = self.rt.block_on(async {
            client
                .query_opt(
                    "SELECT data FROM horton_cold_tables WHERE table_id = $1",
                    &[&(table_id as i32)],
                )
                .await
        })?;
        Ok(row.map(|r| r.get(0)))
    }

    fn list_archived(&self) -> Result<Vec<u32>, SinkError> {
        let client = &self.client;
        let rows = self.rt.block_on(async {
            client
                .query(
                    "SELECT table_id FROM horton_cold_tables ORDER BY table_id",
                    &[],
                )
                .await
        })?;
        Ok(rows.iter().map(|r| r.get::<_, i32>(0) as u32).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc(id: u32) -> TableDescriptor {
        TableDescriptor {
            level: 1,
            table_id: id,
            min_seq: 11,
            max_seq: 99,
            block_count: 2,
            entry_count: 40,
            node_id: 7,
            seal_wall_clock: 1_759_000_000,
        }
    }

    fn row_for(d: &TableDescriptor) -> StoredRow {
        StoredRow {
            level: d.level as i32,
            min_seq: d.min_seq as i64,
            max_seq: d.max_seq as i64,
            block_count: d.block_count as i32,
            entry_count: d.entry_count as i32,
            node_id: d.node_id as i32,
            bytes: 1024,
            checksum: 0x1234_5678_9abc_def0u64 as i64,
        }
    }

    #[test]
    fn row_matches_exact_descriptor() {
        let d = desc(3);
        assert!(row_matches(&d, &row_for(&d)));
    }

    #[test]
    fn row_matches_rejects_every_field() {
        let d = desc(3);
        let mut r = row_for(&d);
        for mutate in [
            |r: &mut StoredRow| r.level += 1,
            |r: &mut StoredRow| r.min_seq += 1,
            |r: &mut StoredRow| r.max_seq += 1,
            |r: &mut StoredRow| r.block_count += 1,
            |r: &mut StoredRow| r.entry_count += 1,
            |r: &mut StoredRow| r.node_id += 1,
        ] {
            let mut r2 = r.clone();
            mutate(&mut r2);
            assert!(!row_matches(&d, &r2), "mutated row still matched");
        }
        // Receipt fields (bytes, checksum) are not part of the match.
        r.bytes += 1;
        r.checksum ^= 1;
        assert!(row_matches(&d, &r));
    }

    #[test]
    fn checksum_bit_cast_roundtrip() {
        for c in [0u64, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(c as i64 as u64, c);
        }
    }

    #[test]
    fn upload_happy_path() {
        let d = desc(5);
        let mut up = Upload::new(&d);
        up.write_block(&[1u8; 64]).unwrap();
        up.write_block(&[2u8; 64]).unwrap();
        let (bytes, checksum) = up.finish().unwrap();
        assert_eq!(bytes.len(), 128);
        assert_eq!(checksum, crc64::checksum(&bytes));
    }

    #[test]
    fn upload_overlong_stream_rejected() {
        let d = desc(6);
        let mut up = Upload::new(&d);
        up.write_block(&[1u8; 8]).unwrap();
        up.write_block(&[1u8; 8]).unwrap();
        let err = up.write_block(&[1u8; 8]).unwrap_err();
        assert!(matches!(err, SinkError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn upload_short_stream_fails_finish() {
        let d = desc(7);
        let mut up = Upload::new(&d);
        up.write_block(&[1u8; 8]).unwrap();
        let err = up.finish().unwrap_err();
        assert!(matches!(err, SinkError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn receipt_from_row_maps_fields() {
        let d = desc(9);
        let r = receipt_from_row(9, &row_for(&d));
        assert_eq!(
            r,
            Receipt {
                table_id: 9,
                bytes: 1024,
                checksum: 0x1234_5678_9abc_def0,
                block_count: 2,
            }
        );
    }
}
