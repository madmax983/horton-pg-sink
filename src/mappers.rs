//! Example [`RowMapper`][crate::RowMapper] implementations.
//!
//! [`HarvestEventMapper`] is the motivating use case: Harvest workflow
//! history events buffered in horton, drained into a
//! `harvest_events`-shaped Postgres table.

use crate::{RowMapper, SinkError};

/// Key/value contract for Harvest history events.
///
/// Key (20 bytes): `workflow_exec_id` (16-byte UUID, big-endian field
/// order) ++ `event_id` (`u32` big-endian). Sort order groups one
/// workflow's events together in `event_id` order — the flusher's scan
/// walks them sequentially.
///
/// Value envelope: `u8 event_type_len ++ event_type ++ u8 ts_len ++
/// timestamp (RFC 3339) ++ event_data (UTF-8 JSON)`. Length-prefixed so
/// the mapper splits it without a JSON parser; the JSON payload passes
/// through to the `JSONB` column untouched.
pub struct HarvestEventMapper {
    /// Target table, e.g. `"harvest_events"`.
    pub table: String,
}

impl HarvestEventMapper {
    /// Mapper for the given target table.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
        }
    }

    /// Encode one event into the `(key, value)` contract.
    pub fn encode(
        workflow_exec_id: &[u8; 16],
        event_id: u32,
        event_type: &str,
        timestamp_rfc3339: &str,
        event_data_json: &str,
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        if event_type.len() > 255 {
            return Err("event_type longer than 255 bytes".to_string());
        }
        if timestamp_rfc3339.len() > 255 {
            return Err("timestamp longer than 255 bytes".to_string());
        }
        let mut key = Vec::with_capacity(20);
        key.extend_from_slice(workflow_exec_id);
        key.extend_from_slice(&event_id.to_be_bytes());
        let mut val = Vec::new();
        val.push(event_type.len() as u8);
        val.extend_from_slice(event_type.as_bytes());
        val.push(timestamp_rfc3339.len() as u8);
        val.extend_from_slice(timestamp_rfc3339.as_bytes());
        val.extend_from_slice(event_data_json.as_bytes());
        Ok((key, val))
    }

    /// Decode a key back into `(workflow_exec_id, event_id)`.
    pub fn decode_key(key: &[u8]) -> Result<([u8; 16], u32), SinkError> {
        if key.len() != 20 {
            return Err(SinkError::Map(format!(
                "event key must be 20 bytes, got {}",
                key.len()
            )));
        }
        let mut id = [0u8; 16];
        id.copy_from_slice(&key[..16]);
        let event_id = u32::from_be_bytes([key[16], key[17], key[18], key[19]]);
        Ok((id, event_id))
    }

    /// Format 16 UUID bytes as canonical `8-4-4-4-12` hex.
    pub fn fmt_uuid(id: &[u8; 16]) -> String {
        let h = |b: u8| format!("{b:02x}");
        let s: String = id.iter().map(|b| h(*b)).collect();
        format!(
            "{}-{}-{}-{}-{}",
            &s[0..8],
            &s[8..12],
            &s[12..16],
            &s[16..20],
            &s[20..32]
        )
    }
}

fn split_field<'a>(val: &'a [u8], what: &str) -> Result<(&'a [u8], &'a [u8]), SinkError> {
    let (len, rest) = val
        .split_first()
        .ok_or_else(|| SinkError::Map(format!("truncated envelope before {what}")))?;
    let len = usize::from(*len);
    if rest.len() < len {
        return Err(SinkError::Map(format!("truncated {what} field")));
    }
    Ok((&rest[..len], &rest[len..]))
}

impl RowMapper for HarvestEventMapper {
    fn target_table(&self) -> &str {
        &self.table
    }

    fn columns(&self) -> &[(&str, &str)] {
        &[
            ("workflow_exec_id", "UUID"),
            ("event_id", "INT"),
            ("event_type", "TEXT"),
            ("event_data", "JSONB"),
            ("timestamp", "TIMESTAMPTZ"),
        ]
    }

    fn conflict_target(&self) -> &str {
        "(workflow_exec_id, event_id)"
    }

    fn map(&self, key: &[u8], val: &[u8]) -> Result<Vec<String>, SinkError> {
        let (wf_id, event_id) = Self::decode_key(key)?;
        let (et_bytes, rest) = split_field(val, "event_type")?;
        let (ts_bytes, data_bytes) = split_field(rest, "timestamp")?;
        let event_type = core::str::from_utf8(et_bytes)
            .map_err(|_| SinkError::Map("event_type is not UTF-8".to_string()))?;
        let timestamp = core::str::from_utf8(ts_bytes)
            .map_err(|_| SinkError::Map("timestamp is not UTF-8".to_string()))?;
        let event_data = core::str::from_utf8(data_bytes)
            .map_err(|_| SinkError::Map("event_data is not UTF-8".to_string()))?;
        // Validate the JSON early: a garbage payload must fail the map,
        // not the COPY.
        if !is_plausible_json(event_data) {
            return Err(SinkError::Map(
                "event_data is not plausible JSON".to_string(),
            ));
        }
        Ok(vec![
            Self::fmt_uuid(&wf_id),
            event_id.to_string(),
            event_type.to_string(),
            event_data.to_string(),
            timestamp.to_string(),
        ])
    }
}

/// Cheap structural check: starts with `{`/`[` and brackets balance.
/// Full validation happens in Postgres when the JSONB cast runs; this
/// just fails fast on obvious garbage.
fn is_plausible_json(s: &str) -> bool {
    let t = s.trim_start();
    let open = t.as_bytes().first().copied();
    if open != Some(b'{') && open != Some(b'[') {
        return false;
    }
    let mut depth: i32 = 0;
    let mut in_str = false;
    let mut esc = false;
    for b in t.bytes() {
        if in_str {
            if esc {
                esc = false;
            } else if b == b'\\' {
                esc = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0 && !in_str
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trip() {
        let wf = [0xABu8; 16];
        let (k, v) = HarvestEventMapper::encode(
            &wf,
            42,
            "ActivityCompleted",
            "2026-09-30T12:00:00Z",
            r#"{"ok":true}"#,
        )
        .unwrap();
        assert_eq!(k.len(), 20);
        let m = HarvestEventMapper::new("harvest_events");
        let fields = m.map(&k, &v).unwrap();
        assert_eq!(fields[0], "abababab-abab-abab-abab-abababababab");
        assert_eq!(fields[1], "42");
        assert_eq!(fields[2], "ActivityCompleted");
        assert_eq!(fields[3], r#"{"ok":true}"#);
        assert_eq!(fields[4], "2026-09-30T12:00:00Z");
    }

    #[test]
    fn rejects_garbage_json() {
        let wf = [0u8; 16];
        let (k, v) =
            HarvestEventMapper::encode(&wf, 1, "X", "2026-09-30T12:00:00Z", "not json").unwrap();
        let m = HarvestEventMapper::new("harvest_events");
        assert!(m.map(&k, &v).is_err());
    }

    #[test]
    fn rejects_bad_key_len() {
        let m = HarvestEventMapper::new("harvest_events");
        assert!(m.map(&[0u8; 19], &[1u8, 65]).is_err());
    }
}
