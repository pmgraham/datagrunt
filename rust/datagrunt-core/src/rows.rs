//! Ports of CSVRows probes and the leading-line counters.

use crate::io::{is_legacy_mac_newlines, read_decoded, universal_lines, universal_newlines};
use std::path::Path;

/// CSVRows.leading_rows: up to `limit` leading non-blank, non-comment rows,
/// each stripped. Streams the file and stops once `limit` rows are found.
pub fn leading_rows(path: &Path, limit: usize) -> std::io::Result<Vec<String>> {
    let mut rows = Vec::new();
    for line in universal_lines(path)? {
        let line = line?;
        let stripped = line.trim();
        if !stripped.is_empty() && !stripped.starts_with('#') {
            rows.push(stripped.to_string());
            if rows.len() >= limit {
                break;
            }
        }
    }
    Ok(rows)
}

/// CSVRows.first_row: first non-comment row stripped, or "".
pub fn first_row(path: &Path) -> std::io::Result<String> {
    Ok(leading_rows(path, 1)?.into_iter().next().unwrap_or_default())
}

/// _count_leading_comments: '#' lines before the header; blanks skipped.
pub fn count_leading_comments(path: &Path) -> std::io::Result<usize> {
    let mut count = 0;
    for line in universal_lines(path)? {
        let line = line?;
        let stripped = line.trim();
        if stripped.starts_with('#') {
            count += 1;
        } else if stripped.is_empty() {
            continue;
        } else {
            break;
        }
    }
    Ok(count)
}

/// _count_leading_physical_lines_before_header: every physical line up to and
/// including the header.
pub fn count_leading_physical_lines_before_header(path: &Path) -> std::io::Result<usize> {
    let mut count = 0;
    for line in universal_lines(path)? {
        let line = line?;
        let stripped = line.trim();
        count += 1;
        if !stripped.is_empty() && !stripped.starts_with('#') {
            break;
        }
    }
    Ok(count)
}

/// The text Python's `csv.reader` sees: universal newlines for legacy-mac
/// files (`newline=None`), raw decoded text otherwise (`newline=""`).
///
/// Python's `open(..., newline=None)` translates `\r` → `\n` before the CSV
/// parser sees anything; `newline=""` passes `\r` through raw.
pub(crate) fn csv_text(path: &Path) -> std::io::Result<String> {
    // Probe newline style first (4 KiB read), like Python, then decode once.
    let legacy_mac = is_legacy_mac_newlines(path);
    let raw = read_decoded(path)?;
    Ok(if legacy_mac { universal_newlines(&raw) } else { raw })
}

/// Build a `csv::Reader` configured to match Python's `csv.reader` behaviour:
/// no header auto-detection, flexible field counts (Python never errors on
/// ragged rows), and the given single-byte delimiter.
pub(crate) fn csv_reader(text: &str, delimiter: u8) -> csv::Reader<&[u8]> {
    csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true) // Python csv.reader never errors on ragged widths
        .delimiter(delimiter)
        .from_reader(text.as_bytes())
}

/// True when the record mirrors Python's skip condition:
/// `not row or row[0].startswith("#")`.
///
/// The `csv` crate silently drops truly-blank lines (it never yields a
/// zero-field record for a blank line), so `is_empty()` here is a safety
/// guard — it will fire only if some future csv-crate version or a crafted
/// input somehow delivers a zero-field record. The `starts_with('#')` check
/// mirrors Python's comment-skipping logic.
pub(crate) fn skip_record(record: &csv::StringRecord) -> bool {
    record.is_empty() || record.get(0).is_some_and(|f| f.starts_with('#'))
}

/// Port of `CSVRows.row_count_with_header`.
///
/// Counts parsed CSV records (quoted fields with embedded newlines count as
/// one record) excluding blank records and comment records (first field starts
/// with `#`), matching the Python implementation exactly.
pub fn row_count_with_header(path: &Path, delimiter: u8) -> std::io::Result<u64> {
    let text = csv_text(path)?;
    let mut count = 0u64;
    for record in csv_reader(&text, delimiter).records() {
        // Err is unreachable today: in-memory pre-decoded UTF-8 source (no IO
        // or UTF-8 errors) and flexible=true (no UnequalLengths). Skipping
        // mirrors Python's csv.reader, which never errors on row shape.
        let Ok(record) = record else { continue };
        if !skip_record(&record) {
            count += 1;
        }
    }
    Ok(count)
}
