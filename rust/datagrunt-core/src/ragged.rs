//! Port of _check_csv_ragged_and_warn (bool only; warning stays in Python).

use crate::rows::{csv_reader, csv_text, skip_record};
use std::path::Path;

const MAX_DATA_ROWS_CHECKED: usize = 10_000;

/// Best-effort like Python: any error means "not ragged".
pub fn check_ragged(path: &Path, delimiter: u8) -> bool {
    check_ragged_inner(path, delimiter).unwrap_or(false)
}

fn check_ragged_inner(path: &Path, delimiter: u8) -> std::io::Result<bool> {
    let text = csv_text(path)?;
    let mut reader = csv_reader(&text, delimiter);
    let mut records = reader.records();

    let mut expected_cols = None;
    for record in records.by_ref() {
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
