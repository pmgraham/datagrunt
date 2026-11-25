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

// Inner: Err on IO failure or record-size bound exceeded; wrapper maps to false.
fn check_ragged_inner(path: &Path, delimiter: u8) -> std::io::Result<bool> {
    let mut reader = csv_reader_streaming(path, delimiter)?;
    let mut record = csv::StringRecord::new();

    let mut expected_cols = None;
    while reader.read_record(&mut record)? {
        if !skip_record(&record) {
            expected_cols = Some(record.len());
            break;
        }
    }
    let Some(expected_cols) = expected_cols else {
        return Ok(false); // no header found
    };

    let mut row_count = 0usize;
    while reader.read_record(&mut record)? {
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
