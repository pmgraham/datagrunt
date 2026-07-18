//! Ports of CSVRows probes and the leading-line counters.

use crate::io::{
    decode_ignore, is_empty, py_strip, take_chars, universal_lines, DecodedReader, MAX_LINE_CHARS,
    MAX_LINE_READ_BYTES,
};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

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
            if newline_pos.is_some_and(|pos| capped_take > pos) {
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
        // Decode with `decode_ignore` (not `from_utf8`): the byte-cap path above
        // can stop mid-multibyte-char at MAX_LINE_READ_BYTES, leaving an
        // incomplete trailing sequence in `segment`. `decode_ignore` drops it
        // (matching io.rs and Python's errors="ignore"), so a malformed
        // newline-free file cannot panic across the PyO3 boundary. Then apply
        // the char cap for parity with the Python reference.
        let raw = decode_ignore(&segment);
        let text = take_chars(&raw, MAX_LINE_CHARS);
        // `stripped` mirrors Python's `line.strip()` (incl. C0 separators).
        let stripped = py_strip(&text);
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
        let stripped = py_strip(&line);
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
        let stripped = py_strip(&line);
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
        let stripped = py_strip(&line);
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
    // Open the file once: probe the first 4096 bytes for the legacy-mac
    // newline signature (\r present, \n absent), then rewind to offset 0
    // so DecodedReader decodes from the beginning (including BOM detection).
    // This avoids the redundant open that `is_legacy_mac_newlines(path)`
    // would cause; the probe semantics are byte-identical to that function.
    let mut file = File::open(path)?;
    let mut probe = Vec::with_capacity(4096);
    // Mirror `is_legacy_mac_newlines`: a probe read error degrades to non-legacy
    // (swallowed, NOT propagated), so behavior is byte-identical to the previous
    // two-open path and to the Python oracle, both of which ignore probe IO
    // errors. A genuine read failure surfaces later during the actual scan.
    let legacy = file.by_ref().take(4096).read_to_end(&mut probe).is_ok()
        && probe.contains(&b'\r')
        && !probe.contains(&b'\n');
    file.seek(SeekFrom::Start(0))?;
    let reader = DecodedReader::from_file(file, legacy);
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

    /// Regression (panic-DoS): a newline-free line whose bytes exceed
    /// MAX_LINE_READ_BYTES with a multibyte char straddling the byte cap must
    /// NOT panic. The byte-cap chop leaves an incomplete UTF-8 sequence in the
    /// buffer; `decode_ignore` drops it instead of `from_utf8` panicking across
    /// the PyO3 boundary.
    #[test]
    fn probe_multibyte_straddling_byte_cap_does_not_panic() {
        // (MAX_LINE_READ_BYTES - 1) ASCII bytes + '€' (3 bytes): the byte cap at
        // MAX_LINE_READ_BYTES slices the first byte of '€', leaving an incomplete
        // sequence. No trailing newline (pure #222 scenario).
        let mut content: Vec<u8> = b"a".repeat(MAX_LINE_READ_BYTES - 1);
        content.extend_from_slice("€".as_bytes());
        let f = tmp(&content);
        let p = probe_csv_header(f.path()).unwrap(); // must not panic
        assert_eq!(p.first_row.chars().count(), MAX_LINE_CHARS);
        assert_eq!(p.sample_lines[0].chars().count(), MAX_LINE_CHARS);
    }

    /// Parity-class lock: a giant line of a leading invalid byte + all-4-byte
    /// chars. The probe budgets on the DECODED stream (DecodedReader drops the
    /// invalid byte before the byte budget), so 8 MiB of decoded 4-byte chars is
    /// exactly MAX_LINE_CHARS chars — matching Python's decode-then-slice. This
    /// guards the probe path against the raw-vs-decoded-budget bug class that was
    /// fixed in io.rs (the probe was already correct via DecodedReader; this keeps
    /// it that way). A pure (undelimited) case can't live in the shared parity
    /// corpus because Python's csv.field_size_limit would trip in row-count parity.
    #[test]
    fn probe_giant_4byte_with_leading_invalid_byte_yields_full_char_cap() {
        let mut content: Vec<u8> = vec![0xff];
        content.extend_from_slice("\u{1F600}".repeat(MAX_LINE_CHARS + 50).as_bytes());
        let f = tmp(&content);
        let p = probe_csv_header(f.path()).unwrap();
        assert_eq!(p.first_row.chars().count(), MAX_LINE_CHARS);
        assert_eq!(p.sample_lines[0].chars().count(), MAX_LINE_CHARS);
    }

    // --- single-open path tests (issue #226) ---
    // These verify that the inline probe + seek + from_file in csv_reader_streaming
    // produces byte-identical row counts to the previous two-open path across all
    // newline conventions.

    /// Legacy-mac (\r-only) file: the inline probe must detect the legacy flag,
    /// translate lone \r to \n, and count rows correctly via the single-open path.
    #[test]
    fn single_open_legacy_mac_row_count() {
        // 1 header + 3 data rows separated by lone \r (no \n anywhere).
        let f = tmp(b"header,col\rrow1,a\rrow2,b\rrow3,c\r");
        assert_eq!(row_count_with_header(f.path(), b',').unwrap(), 4);
    }

    /// CRLF file: probe must NOT set the legacy flag (\n present), and the csv
    /// reader must count all rows without double-counting from the \r.
    #[test]
    fn single_open_crlf_row_count() {
        let f = tmp(b"header,col\r\nrow1,a\r\nrow2,b\r\n");
        assert_eq!(row_count_with_header(f.path(), b',').unwrap(), 3);
    }

    /// LF file: the standard case — probe sees no \r, no legacy translation,
    /// rows counted correctly.
    #[test]
    fn single_open_lf_row_count() {
        let f = tmp(b"header,col\nrow1,a\nrow2,b\nrow3,c\n");
        assert_eq!(row_count_with_header(f.path(), b',').unwrap(), 4);
    }
}
