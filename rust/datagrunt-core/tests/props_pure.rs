//! Property-based tests over datagrunt-core's pure functions (#315).
//!
//! Deliberately NOT differential: Rust==Python agreement is proven by
//! tests/parity/ on the Python side. These lock in panic-freedom and the
//! structural invariants each function must satisfy in isolation.

use proptest::prelude::*;

use datagrunt_core::dialect::sniff;
use datagrunt_core::io::{decode_ignore, take_chars, universal_newlines};
use datagrunt_core::normalize::normalize_columns;

proptest! {
    // ---- io::take_chars ------------------------------------------------
    // Char-boundary safety is the whole reason this helper exists; naive
    // byte slicing panics mid-codepoint.
    #[test]
    fn take_chars_never_panics_and_bounds_hold(s in ".*", n in 0usize..(3 * 1024)) {
        let out = take_chars(&s, n);
        prop_assert!(out.chars().count() <= n);
        // Prefix property, char-wise.
        prop_assert!(s.starts_with(&out));
    }

    #[test]
    fn take_chars_is_identity_when_n_covers_input(s in ".*") {
        let n = s.chars().count();
        prop_assert_eq!(take_chars(&s, n), s);
    }

    // ---- io::decode_ignore ---------------------------------------------
    #[test]
    fn decode_ignore_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let _ = decode_ignore(&bytes);
    }

    #[test]
    fn decode_ignore_is_identity_on_valid_utf8(s in ".*") {
        prop_assert_eq!(decode_ignore(s.as_bytes()), s);
    }

    // ---- io::universal_newlines ----------------------------------------
    #[test]
    fn universal_newlines_removes_all_carriage_returns(s in ".*") {
        let out = universal_newlines(&s);
        prop_assert!(!out.contains('\r'));
    }

    #[test]
    fn universal_newlines_is_idempotent(s in ".*") {
        let once = universal_newlines(&s);
        prop_assert_eq!(universal_newlines(&once), once);
    }

    #[test]
    fn universal_newlines_preserves_non_newline_content(s in "[^\r\n]*") {
        // A string with no newline chars at all passes through untouched.
        prop_assert_eq!(universal_newlines(&s), s);
    }

    // ---- dialect::sniff -------------------------------------------------
    #[test]
    fn sniff_never_panics_on_arbitrary_samples(sample in ".*") {
        let _ = sniff(&sample, None);
    }

    #[test]
    fn sniff_never_panics_with_candidate_delimiters(sample in ".*", delims in "[,;|\t: ]{1,4}") {
        let _ = sniff(&sample, Some(&delims));
    }

    #[test]
    fn sniff_output_invariants(sample in ".*") {
        if let Some(d) = sniff(&sample, None) {
            prop_assert_eq!(d.delimiter.chars().count(), 1);
            // csv.Sniffer only ever yields '"' or '\'' as quotechar.
            prop_assert!(d.quotechar == "\"" || d.quotechar == "'");
        }
    }

    // ---- normalize::normalize_columns -----------------------------------
    #[test]
    fn normalize_columns_structural_invariants(
        names in proptest::collection::vec(".*", 0..24)
    ) {
        let out = normalize_columns(&names);
        // Length preserved.
        prop_assert_eq!(out.len(), names.len());
        // Every output is a nonempty lowercase identifier: [a-z0-9_]+.
        for name in &out {
            prop_assert!(!name.is_empty());
            prop_assert!(name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
        }
        // All outputs are unique.
        let unique: std::collections::HashSet<&String> = out.iter().collect();
        prop_assert_eq!(unique.len(), out.len());
    }

    #[test]
    fn normalize_columns_is_idempotent(names in proptest::collection::vec(".*", 0..24)) {
        let once = normalize_columns(&names);
        prop_assert_eq!(normalize_columns(&once), once.clone());
    }
}
