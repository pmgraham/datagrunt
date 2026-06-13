//! Port of CSVColumnNameNormalizer._normalize_column_names.

use std::collections::HashSet;

const EMPTY_NAME_PLACEHOLDER: &str = "column";

/// Fused single-pass equivalent of Python's three-regex pipeline: lowercase,
/// replace each run of non-[a-z0-9] chars with one "_" (which also makes the
/// separate "_+" collapse a no-op), strip edge "_", map "" to "column", and
/// prefix a leading digit with "_".
fn normalize_single(name: &str) -> String {
    let lower = name.to_lowercase();
    // Replace each maximal run of non-[a-z0-9] chars with one underscore.
    let mut replaced = String::with_capacity(lower.len());
    let mut in_run = false;
    for c in lower.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            replaced.push(c);
            in_run = false;
        } else if !in_run {
            replaced.push('_');
            in_run = true;
        }
    }
    let stripped = replaced.trim_matches('_');
    if stripped.is_empty() {
        return EMPTY_NAME_PLACEHOLDER.to_string();
    }
    if stripped.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("_{stripped}")
    } else {
        stripped.to_string()
    }
}

/// Port of _make_unique_column_names: suffix increments until globally unused.
pub fn normalize_columns(names: &[String]) -> Vec<String> {
    let mut emitted: HashSet<String> = HashSet::new();
    let mut result = Vec::with_capacity(names.len());
    for name in names {
        let base = normalize_single(name);
        let mut candidate = base.clone();
        let mut suffix = 0usize;
        while emitted.contains(&candidate) {
            suffix += 1;
            candidate = format!("{base}_{suffix}");
        }
        emitted.insert(candidate.clone());
        result.push(candidate);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(names: &[&str]) -> Vec<String> {
        normalize_columns(&names.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn basic_and_collisions() {
        assert_eq!(norm(&["First Name", "LAST-NAME"]), vec!["first_name", "last_name"]);
        assert_eq!(norm(&["col_a", "col_a", "col_a_1"]), vec!["col_a", "col_a_1", "col_a_1_1"]);
        assert_eq!(norm(&["%", "()"]), vec!["column", "column_1"]);
        assert_eq!(norm(&["123abc"]), vec!["_123abc"]);
    }
}
