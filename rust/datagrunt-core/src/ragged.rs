//! Port of _check_csv_ragged_and_warn (bool only; warning stays in Python).

use crate::rows::{csv_reader_streaming, skip_record};
use std::path::Path;

const MAX_DATA_ROWS_CHECKED: usize = 10_000;

/// Returns `true` if the CSV at `path` contains ragged rows (rows whose
/// field count differs from the header's), checking up to the first 10,000
/// data rows.
///
/// Mirrors `_check_csv_ragged_and_warn` (bool only; the `UserWarning` stays
/// in Python). Best-effort: any IO or parse error returns `false`.
pub fn check_ragged(path: &Path, delimiter: u8) -> bool {
    check_ragged_inner(path, delimiter).unwrap_or(false)
}

// Inner: Err only on IO failure; the wrapper maps it to false.
fn check_ragged_inner(path: &Path, delimiter: u8) -> std::io::Result<bool> {
    let mut reader = csv_reader_streaming(path, delimiter)?;
    let mut records = reader.records();

    let mut expected_cols = None;
    for record in records.by_ref() {
        // Unlike row counting (which skips, since Python's CSVRows has no
        // try/except), a parse error here aborts to "not ragged" because the
        // Python original wraps the whole scan in `except Exception: False`.
        // With a streaming File source an `Err` could be a genuine IO error;
        // returning false matches Python's `except Exception: return False`.
        let Ok(record) = record else { return Ok(false) };
        if !skip_record(&record) {
            expected_cols = Some(record.len());
            break;
        }
    }
    let Some(expected_cols) = expected_cols else {
        return Ok(false); // no header found
    };

    let mut row_count = 0usize;
    for record in records {
        let Ok(record) = record else { return Ok(false) };
        if skip_record(&record) {
            continue;
        }
        row_count += 1;
        if record.len() != expected_cols {
            return Ok(true);
        }
        if row_count >= MAX_DATA_ROWS_CHECKED {
            break;
        }
    }
    Ok(false)
}
