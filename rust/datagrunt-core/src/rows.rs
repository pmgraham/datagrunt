//! Ports of CSVRows probes and the leading-line counters.

use crate::io::{is_empty, is_legacy_mac_newlines, take_chars, universal_lines, DecodedReader, MAX_LINE_CHARS};
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Byte read bound for probe_csv_header: 4x the char cap (UTF-8 ≤4 bytes/char),
/// matching io.rs MAX_LINE_READ_BYTES. Prevents unbounded `segment` growth on
/// malformed newline-free files.
const MAX_LINE_READ_BYTES: usize = MAX_LINE_CHARS * 4;

// ---- probe_csv_header constants ----
/// Python CANDIDATE_SAMPLE_ROWS = 5.
const CANDIDATE_SAMPLE_ROWS: usize = 5;
/// Python CSV_SNIFF_SAMPLE_ROWS = 5.
const SNIFF_SAMPLE_ROWS: usize = 5;

/// Result of a single-pass header probe: the five values both delimiter
/// and dialect inference need.
pub struct HeaderProbe {
    pub empty: bool,
    pub blank: bool,
    pub first_row: String,
    pub sample_rows: Vec<String>,
    pub sample_lines: Vec<String>,
}

/// Single-pass header probe — mirrors `_compute_python.probe_csv_header` exactly.
///
/// `sample_rows` = stripped non-blank non-comment rows, capped at
/// `CANDIDATE_SAMPLE_ROWS` (== Python `leading_rows(CANDIDATE_SAMPLE_ROWS)`).
///
/// `sample_lines` = raw non-comment lines with their original line terminator
/// preserved (blanks kept), capped at `SNIFF_SAMPLE_ROWS`.  Uses
/// `BufReader::read_until(b'\n')` over a `DecodedReader` (universal-newline
/// translation on, matching Python text mode) so the trailing `\n` is included
/// when present and omitted on the final line of a file with no trailing
/// newline — exactly mirroring Python's `for line in f:`.
///
/// `first_row` = `sample_rows[0]` or `""`.
/// `blank`     = no non-whitespace byte seen (single-pass `saw_nonblank`).
/// `empty`     = `io::is_empty` fast-path; returns `blank: false` to match Python.
pub fn probe_csv_header(path: &Path) -> std::io::Result<HeaderProbe> {
    if is_empty(path)? {
        return Ok(HeaderProbe {
            empty: true,
            blank: false,
            first_row: String::new(),
            sample_rows: Vec::new(),
            sample_lines: Vec::new(),
        });
    }
    // Universal-newline translation on (translate=true) matches Python text mode.
    let mut reader = BufReader::new(DecodedReader::open(path, true)?);
    let mut sample_lines: Vec<String> = Vec::new();
    let mut sample_rows: Vec<String> = Vec::new();
    let mut saw_nonblank = false;
    let mut segment: Vec<u8> = Vec::new();
    loop {
        // Bounded read: accumulate bytes up to MAX_LINE_READ_BYTES or until
        // we find a '\n' (whichever comes first). If we hit the byte cap
        // before finding '\n', stream-skip the rest of the physical line so
        // a malformed newline-free file cannot grow `segment` without bound.
        segment.clear();
        let mut total_read = 0usize;
        let mut found_newline = false;
        loop {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                break; // EOF
            }
            let newline_pos = available.iter().position(|&b| b == b'\n');
            let take = if let Some(pos) = newline_pos {
                pos + 1 // include the '\n'
            } else {
                available.len()
            };
            let budget = MAX_LINE_READ_BYTES.saturating_sub(total_read);
            let capped_take = take.min(budget);
            segment.extend_from_slice(&available[..capped_take]);
            reader.consume(capped_take);
            total_read += capped_take;
            if newline_pos.map_or(false, |pos| capped_take >= pos + 1) {
                found_newline = true;
                break;
            }
            if total_read >= MAX_LINE_READ_BYTES {
                // Byte cap hit before newline: stream-skip to end of line.
                loop {
                    let avail2 = reader.fill_buf()?;
                    if avail2.is_empty() {
                        break; // EOF
                    }
                    let nl = avail2.iter().position(|&b| b == b'\n');
                    if let Some(pos) = nl {
                        reader.consume(pos + 1);
                        break;
                    } else {
                        let len = avail2.len();
                        reader.consume(len);
                    }
                }
                break;
            }
        }
        if total_read == 0 && !found_newline {
            break; // EOF
        }
        // DecodedReader yields valid UTF-8 only; apply char cap for parity.
        let raw =
            String::from_utf8(std::mem::take(&mut segment)).expect("DecodedReader yields UTF-8");
        let text = take_chars(&raw, MAX_LINE_CHARS);
        // `stripped` mirrors Python's `line.strip()`.
        let stripped = text.trim();
        if !stripped.is_empty() {
            saw_nonblank = true;
        }
        let is_comment = stripped.starts_with('#');
        // sample_lines: raw text (including its \n if present), blanks kept,
        // comments skipped — matches Python's `sample_lines.append(line)`.
        if !is_comment && sample_lines.len() < SNIFF_SAMPLE_ROWS {
            sample_lines.push(text.clone());
        }
        // sample_rows: stripped non-blank non-comment rows.
        if !stripped.is_empty() && !is_comment && sample_rows.len() < CANDIDATE_SAMPLE_ROWS {
            sample_rows.push(stripped.to_string());
        }
        if saw_nonblank
            && sample_lines.len() >= SNIFF_SAMPLE_ROWS
            && sample_rows.len() >= CANDIDATE_SAMPLE_ROWS
        {
            break;
        }
    }
    let first_row = sample_rows.first().cloned().unwrap_or_default();
    Ok(HeaderProbe {
        empty: false,
        blank: !saw_nonblank,
        first_row,
        sample_rows,
        sample_lines,
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(content).unwrap();
        f
    }

    #[test]
    fn probe_empty_file() {
        let f = tmp(b"");
        let p = probe_csv_header(f.path()).unwrap();
        assert!(p.empty);
        assert!(!p.blank);
        assert!(p.first_row.is_empty());
        assert!(p.sample_rows.is_empty());
        assert!(p.sample_lines.is_empty());
    }

    #[test]
    fn probe_blank_file() {
        let f = tmp(b"  \n\t\n  \n");
        let p = probe_csv_header(f.path()).unwrap();
        assert!(!p.empty);
        assert!(p.blank);
        assert!(p.first_row.is_empty());
        assert!(p.sample_rows.is_empty());
        // blank lines are not comments, so they go into sample_lines
        assert!(!p.sample_lines.is_empty());
    }

    #[test]
    fn probe_normal_file() {
        let f = tmp(b"name,age\nAlice,30\nBob,25\n");
        let p = probe_csv_header(f.path()).unwrap();
        assert!(!p.empty);
        assert!(!p.blank);
        assert_eq!(p.first_row, "name,age");
        assert_eq!(p.sample_rows[0], "name,age");
        // sample_lines include trailing \n to match Python text-mode
        assert_eq!(p.sample_lines[0], "name,age\n");
    }

    #[test]
    fn probe_comment_and_blank_mix() {
        // Comments skipped in both lists; blanks kept in sample_lines, skipped in sample_rows.
        let f = tmp(b"# comment\n\nname,age\nAlice,30\n");
        let p = probe_csv_header(f.path()).unwrap();
        assert!(!p.empty);
        assert!(!p.blank);
        // sample_rows: skips comment AND blank
        assert_eq!(p.sample_rows, vec!["name,age", "Alice,30"]);
        // sample_lines: skips comment, keeps blank line
        assert_eq!(p.sample_lines[0], "\n"); // the blank line
        assert_eq!(p.sample_lines[1], "name,age\n");
        assert_eq!(p.sample_lines[2], "Alice,30\n");
    }
}
