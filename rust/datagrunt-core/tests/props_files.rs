//! Panic-freedom battery over datagrunt-core's path-based entry points (#315).
//!
//! CLAUDE.md's rule — a panic reachable from user input is a DoS across the
//! PyO3 boundary — stated as an executable property: every public path-based
//! function must return (Ok, Err, or a value), never panic, for arbitrary
//! file bytes. Value-level Rust==Python agreement lives in tests/parity/.

use std::io::{Read, Write};

use proptest::prelude::*;
use tempfile::NamedTempFile;

use datagrunt_core::io::MAX_LINE_CHARS;

/// Write arbitrary bytes to a temp file and hand back the live handle
/// (dropping it deletes the file).
fn file_with(bytes: &[u8]) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("create temp file");
    f.write_all(bytes).expect("write temp bytes");
    f.flush().expect("flush temp bytes");
    f
}

/// Byte soup biased toward CSV-relevant structure: raw arbitrary bytes, a
/// variant guaranteed to contain delimiters/quotes/newlines so the deeper
/// parsing paths are actually reached, a non-empty whitespace-only variant
/// (targets `HeaderProbe::blank`), and the deterministic empty file (targets
/// `HeaderProbe::empty`'s dedicated fast path, rows.rs:43-51).
///
/// The last two arms exist because neither `empty` nor `blank` is likely to
/// fall out of the first two arms within 256 cases: an empirical check
/// without them found `probe.empty`/`probe.blank` hit 0 times across 257
/// cases in `probe_header_shape_is_consistent`, which would have made that
/// property's `empty`/`blank` assertions vacuously untested despite reading
/// as real coverage.
fn csvish_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..2048),
        "[a-z0-9,;|\t \"'\r\n#=+-]{0,2048}".prop_map(|s| s.into_bytes()),
        "[ \t\r\n]{1,64}".prop_map(|s| s.into_bytes()),
        Just(Vec::new()),
    ]
}

proptest! {
    #[test]
    fn path_entry_points_never_panic(bytes in csvish_bytes(), delim in any::<u8>(), limit in 0usize..64) {
        let f = file_with(&bytes);
        let p = f.path();

        let _ = datagrunt_core::delimiter::infer_delimiter(p);
        let _ = datagrunt_core::io::is_legacy_mac_newlines(p);
        let _ = datagrunt_core::io::is_empty(p);
        let _ = datagrunt_core::io::is_tsv(p);
        let _ = datagrunt_core::rows::probe_csv_header(p);
        let _ = datagrunt_core::rows::leading_rows(p, limit);
        let _ = datagrunt_core::rows::first_row(p);
        let _ = datagrunt_core::rows::count_leading_comments(p);
        let _ = datagrunt_core::rows::count_leading_physical_lines_before_header(p);
        let _ = datagrunt_core::rows::row_count_with_header(p, delim);
        let _ = datagrunt_core::ragged::check_ragged(p, delim);
    }

    #[test]
    fn universal_lines_never_panics_and_caps_line_length(bytes in csvish_bytes()) {
        let f = file_with(&bytes);
        let lines = datagrunt_core::io::universal_lines(f.path())
            .expect("open succeeds on existing file");
        // ADAPTATION (marked spot 1a): `UniversalLines` implements
        // `Iterator<Item = std::io::Result<String>>` (io.rs:576-577), not a
        // bare `String` as the sketch's placeholder loop implied. A genuine
        // IO error mid-stream is a valid non-panic outcome the property must
        // tolerate, so only the decoded `Ok` payload is checked against the
        // per-line cap — `Result::into_iter()` (what `.flatten()` uses here)
        // yields the `Ok` value or nothing, so this is exactly "skip Err".
        for s in lines.flatten() {
            prop_assert!(line_len_chars(&s) <= MAX_LINE_CHARS);
        }
    }

    #[test]
    fn decoded_reader_never_panics_in_both_modes(bytes in csvish_bytes(), translate in any::<bool>()) {
        let f = file_with(&bytes);
        let mut reader = datagrunt_core::io::DecodedReader::open(f.path(), translate)
            .expect("open succeeds");
        let mut out = String::new();
        // ADAPTATION (marked spot 1b): consumption pattern mirrors io.rs's
        // own `drain()` test helper (io.rs:865-874) — `read_to_string` via
        // the `Read` impl (io.rs:336-353). DecodedReader's internal `pending`
        // buffer is always built from a `String` (io.rs:236, :330), so the
        // bytes handed to `read_to_string` are always valid UTF-8; the only
        // possible `Err` here is a genuine IO error, which the property
        // tolerates (the assertion under test is "never panics", not
        // "never errors").
        let _ = reader.read_to_string(&mut out);
    }

    #[test]
    fn leading_rows_respects_limit(bytes in csvish_bytes(), limit in 0usize..64) {
        let f = file_with(&bytes);
        if let Ok(rows) = datagrunt_core::rows::leading_rows(f.path(), limit) {
            // KNOWN (issue #325): limit=0 currently returns 1 row — the cap is
            // checked after the append in BOTH backends, so differential parity
            // cannot see it. The fix PR for #325 must tighten this back to
            // `rows.len() <= limit`; that flip is its red/green test.
            prop_assert!(rows.len() <= limit.max(1));
        }
    }

    #[test]
    fn probe_header_shape_is_consistent(bytes in csvish_bytes()) {
        let f = file_with(&bytes);
        if let Ok(probe) = datagrunt_core::rows::probe_csv_header(f.path()) {
            // ADAPTATION (marked spot 2): HeaderProbe's real field set
            // (rows.rs:19-25) is `empty, blank, first_row, sample_rows,
            // sample_lines`. The sketch's placeholder body already named
            // real fields (`empty`, `first_row`, `sample_rows`) but left out
            // `blank`/`sample_lines`; both are wired in below, in the exact
            // shape `probe_csv_header` (rows.rs:42-146) guarantees.
            if probe.empty {
                // The `is_empty()` fast path (rows.rs:43-51) hard-codes
                // `blank: false` alongside `empty: true` and all three
                // collections empty — a documented Python-parity quirk
                // (mirrored by the `probe_empty_file` unit test), not a
                // general "empty implies blank" rule, so it is asserted
                // explicitly rather than assumed.
                prop_assert!(!probe.blank);
                prop_assert!(probe.first_row.is_empty());
                prop_assert!(probe.sample_rows.is_empty());
                prop_assert!(probe.sample_lines.is_empty());
            }
            if probe.blank {
                // `blank` is `!saw_nonblank`, which only becomes `true` when
                // a stripped line is non-empty (rows.rs:118-120) — the same
                // guard that gates `sample_rows`/`first_row` population
                // (rows.rs:128-130, :138) — so `blank` implies both are
                // empty. `sample_lines` is deliberately NOT asserted empty
                // here: non-comment blank lines are still appended to it
                // (rows.rs:124-126), as the `probe_blank_file` unit test
                // confirms (sample_lines non-empty while blank is true).
                prop_assert!(probe.first_row.is_empty());
                prop_assert!(probe.sample_rows.is_empty());
            }
        }
    }
}

/// Char count without allocating; mirrors the cap's unit (chars, not bytes).
fn line_len_chars(s: &str) -> usize {
    s.chars().count()
}
