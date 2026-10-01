//! Postgres sink for horton's archive API.
//!
//! The intended deployment is the Harvest event tail: horton absorbs a
//! write storm as a durable LSM buffer, and this sink drains sealed
//! tables into Postgres with batched `COPY`, idempotently. Postgres
//! remains the source of truth; horton is a write accelerator.
//!
//! # Protocol
//!
//! For one sealed table, in order:
//!
//! 1. [`Db::archive_plan`] hands the caller the table's descriptor
//!    (`first_key` / `last_key` / `max_seq` / `entry_count`).
//! 2. If the sink's watermark table already holds the table id, the
//!    `COPY` is skipped — this is the crash-recovery path (a crash
//!    between the Postgres commit and [`Db::archive_commit`] replays
//!    here and must not duplicate rows).
//! 3. Otherwise the sink scans the database over
//!    `[first_key, successor(last_key))` with `max_seq` pinned to the
//!    table's `max_seq` and streams every entry through the
//!    [`RowMapper`] into a temp staging table, then
//!    `INSERT ... ON CONFLICT DO NOTHING` into the target. The scan is a
//!    merged view, so entries from overlapping newer tables in the same
//!    key range may ride along; the upsert makes that harmless, and
//!    their own tables re-deliver them later anyway.
//! 4. The watermark row and the upsert commit in one Postgres
//!    transaction.
//! 5. [`Db::archive_commit`] drops the table from horton's manifest.
//!    The commit runs strictly after the Postgres commit, per the
//!    archive docs: no crash can lose acknowledged data.
//!
//! The sink is correct for insert-only workloads (event logs, sensor
//! streams). Tables carrying tombstones are refused by
//! [`Db::archive_commit`] with [`Error::WouldResurrect`][horton::Error];
//! the sink surfaces that as [`SinkError::TombstonesWouldResurrect`] and
//! leaves the table local — compact the table down first, per the
//! archive documentation.

use bytes::Bytes;
use futures_util::SinkExt;
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

use horton::{BlockDevice, Db, Scan};

pub mod device;
pub mod mappers;

mod escape {
    /// Escape one field for Postgres `COPY ... FROM STDIN` text format.
    ///
    /// Text-format rules: backslash starts an escape, so `\` -> `\\`,
    /// tab/newline/CR become `\t` / `\n` / `\r`. Anything else passes
    /// through byte-identical (UTF-8 safe: no escape byte collides with
    /// a multibyte sequence).
    pub fn copy_escape(field: &str) -> String {
        let mut out = String::with_capacity(field.len());
        for ch in field.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '\t' => out.push_str("\\t"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                _ => out.push(ch),
            }
        }
        out
    }

    /// Exclusive end bound for a scan over `[first, last]`: the byte
    /// successor of `last`. Returns `None` when `last` is empty or all
    /// `0xFF` (no successor representable) — the caller then scans to
    /// the end of the keyspace.
    pub fn exclusive_end(last: &[u8]) -> Option<Vec<u8>> {
        let mut end = last.to_vec();
        loop {
            match end.last_mut() {
                None => return None,
                Some(b) if *b == 0xFF => {
                    end.pop();
                }
                Some(b) => {
                    *b += 1;
                    return Some(end);
                }
            }
        }
    }
}

pub use escape::{copy_escape, exclusive_end};

/// Maps one horton entry to a Postgres row.
///
/// The mapper owns the key/value contract: it decides the key encoding,
/// the value envelope, the target table, and the conflict key that makes
/// re-delivery idempotent.
pub trait RowMapper {
    /// Target table, e.g. `"harvest_events"`.
    fn target_table(&self) -> &str;

    /// `(column name, Postgres type)` pairs, in `COPY` column order.
    fn columns(&self) -> &[(&str, &str)];

    /// Conflict target for the idempotent upsert, e.g.
    /// `"(workflow_exec_id, event_id)"`.
    fn conflict_target(&self) -> &str;

    /// Map one entry to `COPY`-text fields, one per [`columns`][Self::columns].
    /// Fields are escaped by the sink; return them raw.
    fn map(&self, key: &[u8], val: &[u8]) -> Result<Vec<String>, SinkError>;
}

/// What went wrong while sinking one table.
#[derive(Debug)]
pub enum SinkError {
    /// No such table at the level (and no watermark row either — not a replay).
    TableNotFound { level: usize, table_id: u32 },
    /// The table carries tombstones that would resurrect shadowed values
    /// if archived. Compact the table down first.
    TombstonesWouldResurrect { level: usize, table_id: u32 },
    /// The mapper rejected an entry.
    Map(String),
    /// A horton operation failed.
    Horton(String),
    /// A Postgres operation failed.
    Postgres(tokio_postgres::Error),
}

impl core::fmt::Display for SinkError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TableNotFound { level, table_id } => {
                write!(
                    f,
                    "no table {table_id} at level {level} (and not watermarked)"
                )
            }
            Self::TombstonesWouldResurrect { level, table_id } => write!(
                f,
                "table {table_id} at level {level} hides values behind tombstones; compact first"
            ),
            Self::Map(msg) => write!(f, "row mapping failed: {msg}"),
            Self::Horton(msg) => write!(f, "horton error: {msg}"),
            Self::Postgres(e) => write!(f, "postgres error: {e}"),
        }
    }
}

impl std::error::Error for SinkError {}

impl From<tokio_postgres::Error> for SinkError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Postgres(e)
    }
}

/// Outcome of sinking one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkReport {
    /// The archived table's id.
    pub table_id: u32,
    /// Entries streamed into Postgres (`0` when the watermark short-circuited).
    pub entries_copied: u64,
    /// The table's `max_seq` (the scan's pin).
    pub max_seq: u64,
    /// True when the `COPY` ran in this call (false on watermark replay).
    pub uploaded: bool,
    /// True when [`Db::archive_commit`] dropped the table locally.
    pub committed: bool,
}

/// Postgres sink for horton's archive API. See the [module](self) docs
/// for the protocol.
pub struct PgSink<M> {
    client: Client,
    mapper: M,
}

impl<M: RowMapper> PgSink<M> {
    /// Connect, spawn the connection driver on the current tokio runtime,
    /// and return the sink.
    pub async fn connect(url: &str, mapper: M) -> Result<Self, SinkError> {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self { client, mapper })
    }

    /// Create the watermark table if it is missing.
    pub async fn ensure_schema(&self) -> Result<(), SinkError> {
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS horton_pg_sink_flushed (\
                   table_id   INTEGER PRIMARY KEY,\
                   entries    BIGINT NOT NULL,\
                   max_seq    BIGINT NOT NULL,\
                   flushed_at TIMESTAMPTZ NOT NULL DEFAULT now()\
                 )",
                &[],
            )
            .await?;
        Ok(())
    }

    /// Raw `EXECUTE` passthrough (test/support).
    pub async fn client_exec(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, SinkError> {
        Ok(self.client.execute(sql, params).await?)
    }

    /// Raw single-row query passthrough (test/support).
    pub async fn client_query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<tokio_postgres::Row, SinkError> {
        Ok(self.client.query_one(sql, params).await?)
    }

    /// Sink one sealed table into Postgres, then archive-commit it.
    ///
    /// Idempotent: re-running a table whose watermark row exists skips
    /// the `COPY` and goes straight to the commit; re-running a table
    /// that is already gone locally reports success without doing
    /// anything.
    #[allow(clippy::too_many_arguments)]
    pub async fn sink_table<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &mut self,
        db: &mut Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
        level: usize,
        table_id: u32,
    ) -> Result<SinkReport, SinkError>
    where
        D::Error: core::fmt::Debug,
    {
        let Some(plan) = db.archive_plan(level, table_id) else {
            // Table already gone locally: success iff the watermark proves
            // a previous run flushed it; otherwise the id is just wrong.
            let row = self
                .client
                .query_opt(
                    "SELECT 1 FROM horton_pg_sink_flushed WHERE table_id = $1",
                    &[&(table_id as i32)],
                )
                .await?;
            return match row {
                Some(_) => Ok(SinkReport {
                    table_id,
                    entries_copied: 0,
                    max_seq: 0,
                    uploaded: false,
                    committed: false,
                }),
                None => Err(SinkError::TableNotFound { level, table_id }),
            };
        };
        let sealed = plan.sealed();

        let already: Option<i32> = self
            .client
            .query_opt(
                "SELECT table_id FROM horton_pg_sink_flushed WHERE table_id = $1",
                &[&(table_id as i32)],
            )
            .await?
            .map(|r| r.get(0));

        let mut entries_copied: u64 = 0;
        let uploaded: bool = if already.is_none() {
            entries_copied = self
                .copy_table(db, &sealed.first_key, &sealed.last_key, sealed.max_seq)
                .await?;

            // One transaction: the upsert + the watermark commit
            // atomically. The staging table was filled by the COPY above
            // and is dropped at the end of this call.
            let tx = self.client.transaction().await?;
            let cols: Vec<String> = self
                .mapper
                .columns()
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect();
            let col_list = cols.join(", ");
            let target = self.mapper.target_table();
            let conflict = self.mapper.conflict_target();
            tx.execute(
                &format!(
                    "INSERT INTO {target} ({col_list}) SELECT {col_list} FROM pg_sink_staging \
                     ON CONFLICT {conflict} DO NOTHING"
                ),
                &[],
            )
            .await?;
            tx.execute(
                "INSERT INTO horton_pg_sink_flushed (table_id, entries, max_seq) \
                 VALUES ($1, $2, $3) ON CONFLICT (table_id) DO NOTHING",
                &[
                    &(table_id as i32),
                    &(entries_copied as i64),
                    &(sealed.max_seq as i64),
                ],
            )
            .await?;
            tx.commit().await?;
            self.client
                .execute("DROP TABLE pg_sink_staging", &[])
                .await?;
            true
        } else {
            false
        };

        // Commit strictly after the Postgres commit: a crash anywhere
        // before this leaves the table local, and the watermark makes
        // the replay skip the COPY.
        let committed = db
            .archive_commit(level, table_id)
            .await
            .map_err(|e| match e {
                horton::Error::WouldResurrect { .. } => {
                    SinkError::TombstonesWouldResurrect { level, table_id }
                }
                other => SinkError::Horton(format!("{other:?}")),
            })?;

        Ok(SinkReport {
            table_id,
            entries_copied,
            max_seq: sealed.max_seq,
            uploaded,
            committed,
        })
    }

    /// Stream one table's entries into a temp staging table via COPY.
    ///
    /// The scan runs over `[first_key, successor(last_key))` with
    /// `max_seq` pinned, while the table is still in the manifest
    /// (between `archive_plan` and `archive_commit`). The merged view
    /// may include entries from overlapping newer tables; the
    /// `ON CONFLICT DO NOTHING` upsert absorbs the duplicates.
    #[allow(clippy::too_many_arguments)]
    async fn copy_table<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &self,
        db: &Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
        first_key: &horton::KeyBound<KEY_MAX>,
        last_key: &horton::KeyBound<KEY_MAX>,
        max_seq: u64,
    ) -> Result<u64, SinkError>
    where
        D::Error: core::fmt::Debug,
    {
        let end = exclusive_end(last_key.as_slice());

        let mut scan = Scan::new(db);
        scan.seek(first_key.as_slice(), end.as_deref(), max_seq)
            .await
            .map_err(|e| SinkError::Horton(format!("{e:?}")))?;

        let staging_ddl: String = self
            .mapper
            .columns()
            .iter()
            .map(|(name, ty)| format!("{name} {ty}"))
            .collect::<Vec<_>>()
            .join(", ");
        let copy_cols: String = self
            .mapper
            .columns()
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect::<Vec<_>>()
            .join(", ");

        // Staging lives in this session only. A failed run may leave a
        // previous staging table behind, so start from a clean slate.
        let session = &self.client;
        session
            .execute("DROP TABLE IF EXISTS pg_sink_staging", &[])
            .await?;
        session
            .execute(
                &format!("CREATE TEMP TABLE pg_sink_staging ({staging_ddl})"),
                &[],
            )
            .await?;
        // Plain (non-transactional) COPY: a crash here leaves the temp
        // table behind for the session, and the watermark row absent, so
        // the next run replays from scratch — idempotent.
        let copy = session
            .copy_in(&format!("COPY pg_sink_staging ({copy_cols}) FROM STDIN"))
            .await
            .map_err(SinkError::Postgres)?;
        futures_util::pin_mut!(copy);

        let mut key_buf = vec![0u8; KEY_MAX];
        let mut val_buf = vec![0u8; VAL_MAX];
        let mut count: u64 = 0;
        loop {
            let next = scan
                .next(&mut key_buf, &mut val_buf)
                .await
                .map_err(|e| SinkError::Horton(format!("{e:?}")))?;
            let Some((klen, vlen)) = next else { break };
            let fields = self.mapper.map(&key_buf[..klen], &val_buf[..vlen])?;
            if fields.len() != self.mapper.columns().len() {
                return Err(SinkError::Map(format!(
                    "mapper returned {} fields for {} columns",
                    fields.len(),
                    self.mapper.columns().len()
                )));
            }
            let line: String = fields
                .iter()
                .map(|f| copy_escape(f))
                .collect::<Vec<_>>()
                .join("\t")
                + "\n";
            copy.as_mut()
                .send(Bytes::from(line))
                .await
                .map_err(SinkError::Postgres)?;
            count += 1;
        }
        copy.as_mut().close().await.map_err(SinkError::Postgres)?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_escape_round_trip() {
        assert_eq!(copy_escape("plain"), "plain");
        assert_eq!(copy_escape("a\\b"), "a\\\\b");
        assert_eq!(copy_escape("a\tb"), "a\\tb");
        assert_eq!(copy_escape("a\nb"), "a\\nb");
        assert_eq!(copy_escape("a\rb"), "a\\rb");
        assert_eq!(copy_escape("mix\\ed\t\n\r"), "mix\\\\ed\\t\\n\\r");
        // UTF-8 passes through untouched.
        assert_eq!(copy_escape("héllo→world"), "héllo→world");
    }

    #[test]
    fn exclusive_end_successor() {
        assert_eq!(exclusive_end(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(exclusive_end(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(exclusive_end(b"\xff\xff"), None);
        assert_eq!(exclusive_end(b""), None);
        // Successor property: last < end, and nothing fits between for
        // the common prefix case.
        let last = b"key\x00\x09";
        let end = exclusive_end(last).unwrap();
        assert!(last.as_slice() < end.as_slice());
    }
}
