//! Ports of CSVRows probes and the leading-line counters.

use crate::io::{is_legacy_mac_newlines, universal_lines, DecodedReader};
use std::io;
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

/// Build a streaming `csv::Reader` over the file, matching Python's
/// `csv.reader` behaviour: no header auto-detection, flexible field counts
/// (Python never errors on ragged widths), and the given single-byte
/// delimiter.
///
/// The reader streams decoded bytes via [`DecodedReader`] in 64 KiB chunks, so
/// callers that stop early (e.g. ragged checks bailing after 10k rows) never
/// pay to decode the rest of the file. Newline handling mirrors Python: legacy
/// mac files get `\r` → `\n` translation (Python's `open(..., newline=None)`),
/// every other file passes `\r` through raw (`newline=""`).
///
/// NOTE on error semantics: unlike the previous in-memory source, the file
/// underneath can now surface a genuine IO error mid-parse as an `Err` record.
/// `row_count_with_header` skips `Err` (matching Python's `csv.reader`, which
/// never errors on row shape — a real IO error there is unreachable for regular
/// files); `check_ragged` returns `false` on `Err`, exactly matching its Python
/// original's `except Exception: return False`.
pub(crate) fn csv_reader_streaming(
    path: &Path,
    delimiter: u8,
) -> io::Result<csv::Reader<DecodedReader>> {
    let translate_newlines = is_legacy_mac_newlines(path);
    let reader = DecodedReader::open(path, translate_newlines)?;
    Ok(csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true) // Python csv.reader never errors on ragged widths
        .delimiter(delimiter)
        .from_reader(reader))
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
    let mut reader = csv_reader_streaming(path, delimiter)?;
    let mut count = 0u64;
    for record in reader.records() {
        // Skipping `Err` mirrors Python's csv.reader, which never errors on row
        // shape (flexible=true rules out UnequalLengths). With a real File
        // underneath an `Err` could now also be a genuine IO error mid-read;
        // that path is unreachable for regular files and acceptable for this
        // prototype (Python's equivalent would raise — already documented).
        let Ok(record) = record else { continue };
        if !skip_record(&record) {
            count += 1;
        }
    }
    Ok(count)
}
