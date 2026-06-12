//! Port of CSVDelimiter.infer_csv_file_delimiter.

use crate::{io, rows};
use std::path::Path;

const SAFE_DELIMITERS: [char; 4] = [',', ';', '|', '\t'];
const SPACE_DELIMITER: char = ' ';
const DEFAULT_DELIMITER: &str = ",";
const DEFAULT_TAB_DELIMITER: &str = "\t";
const CANDIDATE_SAMPLE_ROWS: usize = 5;
const MIN_CONSISTENT_FIELDS: usize = 3;

/// Python DELIMITER_REGEX_PATTERN [^0-9a-zA-Z_ "-]: candidate = any char that
/// is not ASCII-alphanumeric, '_', ' ', '"' or '-'. Unicode letters DO count
/// as candidates, exactly like the Python regex.
fn is_candidate_char(c: char) -> bool {
    !(c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '"' | '-'))
}

/// Counter(...).most_common(): count desc, ties in first-seen order.
///
/// Python calls `first_row.replace(" ", "")` before running the regex, but the
/// regex pattern `[^0-9a-zA-Z_ "-]` already excludes spaces, so removing them
/// first has no effect: the set of matching characters is identical, and their
/// relative positions (first-seen order) are unchanged because inserting or
/// removing non-candidate characters cannot reorder the candidates that remain.
/// Skipping the replace() is therefore a pure no-op and preserves exact parity.
fn candidates_most_common_first(first_row: &str) -> Vec<char> {
    let mut counts: Vec<(char, usize)> = Vec::new();
    for c in first_row.chars().filter(|c| is_candidate_char(*c)) {
        match counts.iter_mut().find(|(k, _)| *k == c) {
            Some((_, n)) => *n += 1,
            None => counts.push((c, 1)),
        }
    }
    counts.sort_by(|a, b| b.1.cmp(&a.1)); // stable sort keeps tie order
    counts.into_iter().map(|(c, _)| c).collect()
}

/// Python _split_row: ' ' uses str.split() (whitespace runs), else split(char).
fn field_count(row: &str, c: char) -> usize {
    if c == SPACE_DELIMITER {
        row.split_whitespace().count()
    } else {
        row.split(c).count()
    }
}

/// _splits_rows_consistently: >=2 rows, all the same field count, count >= 3.
fn splits_rows_consistently(sample: &[String], c: char) -> bool {
    if sample.len() < 2 {
        return false;
    }
    let first = field_count(&sample[0], c);
    first >= MIN_CONSISTENT_FIELDS && sample.iter().all(|r| field_count(r, c) == first)
}

pub fn infer_delimiter(path: &Path) -> std::io::Result<String> {
    if io::is_tsv(path) {
        return Ok(DEFAULT_TAB_DELIMITER.to_string());
    }
    if io::is_empty(path)? || io::is_blank(path) {
        return Ok(DEFAULT_DELIMITER.to_string());
    }
    let first = rows::first_row(path)?;
    let candidates = candidates_most_common_first(&first);
    for c in &candidates {
        if SAFE_DELIMITERS.contains(c) {
            return Ok(c.to_string());
        }
    }
    let sample = rows::leading_rows(path, CANDIDATE_SAMPLE_ROWS)?;
    for c in &candidates {
        if splits_rows_consistently(&sample, *c) {
            return Ok(c.to_string());
        }
    }
    if splits_rows_consistently(&sample, SPACE_DELIMITER) {
        return Ok(SPACE_DELIMITER.to_string());
    }
    Ok(DEFAULT_DELIMITER.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_ordering_is_count_desc_then_first_seen() {
        // '.' seen first, ';' same count -> '.' first; ':' more frequent -> first overall
        assert_eq!(candidates_most_common_first("a.b;c.d;e:f:g:h"), vec![':', '.', ';']);
    }

    #[test]
    fn consistency_requires_three_fields_and_two_rows() {
        let rows2 = vec!["a.b".to_string(), "c.d".to_string()];
        assert!(!splits_rows_consistently(&rows2, '.')); // only 2 fields
        let rows3 = vec!["a.b.c".to_string(), "d.e.f".to_string()];
        assert!(splits_rows_consistently(&rows3, '.'));
        assert!(!splits_rows_consistently(&rows3[..1].to_vec(), '.')); // 1 row
    }

    #[test]
    fn tab_is_candidate_char() {
        assert!(is_candidate_char('\t'));
    }

    #[test]
    fn dash_is_not_candidate_char() {
        assert!(!is_candidate_char('-'));
    }

    #[test]
    fn space_is_not_candidate_char() {
        assert!(!is_candidate_char(' '));
    }
}
