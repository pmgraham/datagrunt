//! Port of CPython `csv.Sniffer` (`sniff`, `_guess_quote_and_delimiter`,
//! `_guess_delimiter`) with its 2026 fixes (#378): gh-103925 (line endings are
//! normalised before sniffing), gh-98820 (a quoted-field body stops at the first
//! undoubled quote) and gh-109638 (doubled quotes are sought in whole fields,
//! only once a delimiter is known). The spec is the Python oracle,
//! `_compute_python._sniff_sample` and its helpers, which mirror CPython
//! v3.10.22 `Lib/csv.py` (identical in behaviour to v3.11.18 / v3.13.18 /
//! v3.14.10). Read them side-by-side with this file; the structure mirrors them
//! deliberately, including CPython's oddities.
//!
//! Parity notes (places the real CPython source matters):
//!   * Every dict in the Sniffer iterates in insertion order, and
//!     `max(d, key=d.get)` returns the FIRST key reaching the max. We model this
//!     with insertion-ordered `Vec<(K, V)>` plus strictly-greater comparison.
//!   * `_guess_delimiter` scans `ascii = [chr(c) for c in range(127)]`, i.e.
//!     code points 0..=126 (NOT 0..=127).
//!   * The "mode" of a char's frequency table is adjusted by subtracting the
//!     counts of every OTHER frequency, which can go negative -> `i64`.
//!   * CPython's `delims` map persists across chunk iterations, but its sweep
//!     only runs while `delims` is empty, so stopping at the first chunk that
//!     finds a delimiter (as the oracle does) gives the same answer.
//!   * fancy-regex can stop a search with a runtime error where CPython's `re`
//!     cannot; see `COMPILED_QUOTE_PATTERNS` for what that yields.
//!
//! Only the four data-driven fields live here. `lineterminator` ("\r\n"),
//! `quoting` (QUOTE_MINIMAL) and the absence of `escapechar` are constants the
//! binding layer applies (`rust/datagrunt-python`); they are not part of this
//! core struct.

use fancy_regex::Regex;
use std::borrow::Cow;
use std::sync::LazyLock;

/// The four data-driven facts `csv.Sniffer().sniff` derives from a sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SniffedDialect {
    pub delimiter: String,
    pub quotechar: String,
    pub doublequote: bool,
    pub skipinitialspace: bool,
}

/// Sniffer.preferred — fallback ordering when several delimiters tie.
const PREFERRED: [char; 5] = [',', '\t', ';', ' ', ':'];

/// Insertion-ordered counter: `d[key] = d.get(key, 0) + 1`.
fn bump(map: &mut Vec<(String, usize)>, key: &str) {
    match map.iter_mut().find(|(k, _)| k == key) {
        Some((_, count)) => *count += 1,
        None => map.push((key.to_string(), 1)),
    }
}

/// `max(d, key=d.get)` — the FIRST key reaching the maximum value wins, so we
/// only replace the running best on a STRICTLY greater value.
fn first_max_key<K: Clone>(map: &[(K, usize)]) -> Option<(K, usize)> {
    let mut best: Option<(&K, usize)> = None;
    for (k, v) in map {
        match best {
            Some((_, bv)) if *v > bv => best = Some((k, *v)),
            None => best = Some((k, *v)),
            _ => {}
        }
    }
    best.map(|(k, v)| (k.clone(), v))
}

/// Outcome of `_guess_quote_and_delimiter`. An empty `delimiter` means "could
/// not determine this way" (Python's `delim == ''`/`None`), which sends `sniff`
/// on to the frequency-based `_guess_delimiter`.
struct QuoteGuess {
    quotechar: String,
    doublequote: bool,
    delimiter: String,
    skipinitialspace: bool,
}

/// CPython's `\w`, spelled out — do NOT write `\w` in a pattern here.
///
/// CPython defines `\w` as "alphanumeric characters (as defined by
/// `str.isalnum()`) as well as the underscore", i.e. general categories L* and
/// N* plus `_`. fancy-regex inherits regex-syntax's UTS#18 `perl_word`, which is
/// `Alphabetic ∪ M ∪ Nd ∪ Pc ∪ Join_Control` — a DIFFERENT set, and different in
/// both directions: it excludes `No`/`Nl` (so `²` U+00B2 is a word char to
/// CPython but not to Rust) and includes marks (so U+0301 is a word char to Rust
/// but not to CPython). That divergence made the sniffer pick different
/// delimiters on either side (#318).
///
/// The equivalence below was verified exhaustively: over all 1,114,112 code
/// points, `re.match(r"\w", ch)` agrees with `category(ch)[0] in "LN" or ch ==
/// "_"` with zero mismatches.
///
/// Residual, unchanged by this: CPython's tables come from the running
/// interpreter's Unicode version (13.0 on the 3.10 floor) while regex-syntax
/// pins 16.0, so code points assigned in between can still disagree. That skew
/// predates this fix and is far narrower than the definitional one it replaces —
/// `²` and combining marks have been assigned for decades.
const PY_WORD: &str = r"\p{L}\p{N}_";

/// Any run of characters other than the quote: half of the quoted-field body.
///
/// The body is `other(?:quote{2}other)*` (gh-98820): it ends at the first quote
/// that is not doubled, as it does for a reader. This is CPython v3.10.22's
/// unrolled spelling, which the Python oracle uses; 3.11+ writes the same body
/// as a possessive `(?:quote{2}|(?!quote).)*+`. The pre-fix lazy `.*?` let a
/// match slide across doubled quotes and field boundaries, and rescanned to the
/// end of the sample from every opening quote.
const QUOTE_OTHER: &str = r"(?:(?!\k<quote>).)*";

/// The four DOTALL|MULTILINE patterns, in the order Python tries them, built and
/// compiled once to avoid a per-call `Regex::new`.
///
/// The bool records whether the pattern carries `delim`/`space` groups (the 4th
/// does not — Python hits `KeyError` and `continue`s, so it never records delims
/// or spaces for that pattern). Python `(?P=quote)` backreferences become
/// fancy-regex `\k<quote>`.
///
/// The word class is interpolated from `PY_WORD` rather than written as `\w`, so
/// there is exactly one place to look when asking what "word character" means
/// here — see that constant for why the distinction matters.
///
/// Runtime errors — the one place Rust cannot mirror Python. A fancy-regex
/// search returns `Err` once it exceeds its backtrack limit (1,000,000 by
/// default) or its fixed 1,000,000-entry backtrack stack; CPython's `re` has
/// neither limit. Even a linear scan costs fancy-regex about 2-4 backtracks per
/// character it tries (measured on these patterns), so one search that crosses
/// roughly 250k-500k characters without a match, or a single quoted body of
/// roughly 1M characters, errs whatever the pattern. The sniffer never panics on
/// such an `Err` (a panic would cross the FFI boundary); it treats it as the end
/// of that scan. The quote patterns keep the matches found before it, and with
/// none the next pattern runs, then the frequency guess, exactly as for an empty
/// match set; the doubled-quote search answers `false` unless it already found a
/// doubled quote. CPython keeps scanning, so on a sample with such a stretch the
/// two backends can disagree. `adversarial_lines_stay_within_fancy_regex_runtime_limits`
/// pins that the fixed patterns stay inside both limits on 128k-char adversarial
/// lines; the pre-fix patterns did not.
static COMPILED_QUOTE_PATTERNS: LazyLock<[(Regex, bool); 4]> = LazyLock::new(|| {
    let body = format!(r"{QUOTE_OTHER}(?:\k<quote>{{2}}{QUOTE_OTHER})*");
    let patterns: [(String, bool); 4] = [
        (
            // ,"...",
            format!(
                r#"(?sm)(?P<delim>[^{PY_WORD}\n"'])(?P<space> ?)(?P<quote>["']){body}\k<quote>\k<delim>"#
            ),
            true,
        ),
        (
            //  "...",
            format!(
                r#"(?sm)(?:^|\n)(?P<quote>["']){body}\k<quote>(?P<delim>[^{PY_WORD}\n"'])(?P<space> ?)"#
            ),
            true,
        ),
        (
            // ,"..."
            format!(
                r#"(?sm)(?P<delim>[^{PY_WORD}\n"'])(?P<space> ?)(?P<quote>["']){body}\k<quote>(?:$|\n)"#
            ),
            true,
        ),
        (
            //  "..." (no delim, no space)
            format!(r#"(?sm)(?:^|\n)(?P<quote>["']){body}\k<quote>(?:$|\n)"#),
            false,
        ),
    ];
    patterns.map(|(pattern, has_groups)| {
        (
            Regex::new(&pattern).expect("static sniffer pattern is valid"),
            has_groups,
        )
    })
});

/// One pattern's matches, tallied as CPython tallies them: quote chars and
/// allowed delimiters (insertion-ordered, like its dicts), and the matches with
/// a space after the delimiter.
#[derive(Default)]
struct QuoteTally {
    matches: usize,
    quotes: Vec<(String, usize)>,
    delims: Vec<(String, usize)>,
    spaces: usize,
}

/// Tally every non-overlapping match of one quote pattern (`regexp.findall`).
///
/// The quote and delim groups each match exactly one character, so CPython's
/// emptiness checks on them never fire; they are kept for the mirror. Without
/// `delim`/`space` groups (the 4th pattern) both tallies are skipped, as
/// CPython's `KeyError` `continue` skips them. The `delimiters` restriction is
/// a SUBSTRING check, exactly like Python's `key in delimiters` on a str.
fn tally_quote_pattern(
    regexp: &Regex,
    has_groups: bool,
    data: &str,
    delimiters: Option<&str>,
) -> QuoteTally {
    let mut tally = QuoteTally::default();
    for caps in regexp.captures_iter(data) {
        // A runtime error ends this scan; see `COMPILED_QUOTE_PATTERNS`.
        let Ok(caps) = caps else { break };
        let group = |name: &str| caps.name(name).map_or("", |m| m.as_str());
        tally.matches += 1;
        let quote = group("quote");
        if !quote.is_empty() {
            bump(&mut tally.quotes, quote);
        }
        if !has_groups {
            continue;
        }
        let delim = group("delim");
        if !delim.is_empty() && delimiters.is_none_or(|allowed| allowed.contains(delim)) {
            bump(&mut tally.delims, delim);
        }
        if !group("space").is_empty() {
            tally.spaces += 1;
        }
    }
    tally
}

/// The tally of the first quote pattern that matches at all (CPython's order).
fn first_quote_tally(data: &str, delimiters: Option<&str>) -> QuoteTally {
    for (regexp, has_groups) in COMPILED_QUOTE_PATTERNS.iter() {
        let tally = tally_quote_pattern(regexp, *has_groups, data, delimiters);
        if tally.matches > 0 {
            return tally;
        }
    }
    QuoteTally::default()
}

/// The whole-field doubled-quote pattern (gh-109638), CPython v3.10.22's
/// spelling: a field opens just after the delimiter (lookbehind) or at a line
/// start, skipping spaces unless the delimiter is itself a space; its body
/// (group 1) is the unrolled loop `[^q]*(?:qq[^q]*)*`; it closes before the
/// delimiter or a line end. Matching whole fields stops a match sliding across
/// field boundaries. MULTILINE only. The quotechar is only ever `"` or `'`,
/// which need no escaping (CPython does not escape it either); the delimiter
/// goes through `fancy_regex::escape`, as CPython's goes through `re.escape`.
fn doubled_quote_regex(quotechar: &str, delim: &str) -> Result<Regex, fancy_regex::Error> {
    let d = fancy_regex::escape(delim);
    let q = quotechar;
    // Skipping spaces after a space rescans them.
    let space = if delim == " " { "" } else { " *" };
    Regex::new(&format!(
        r"(?m)(?:(?<={d})|^){space}{q}([^{q}]*(?:{q}{q}[^{q}]*)*){q}(?:{d}|$)"
    ))
}

/// `any(quotechar * 2 in m[1] for m in dq_regexp.finditer(data))`.
///
/// Like `any`, it stops at the first doubled quote. A runtime error ends the
/// scan as "none found" (see `COMPILED_QUOTE_PATTERNS`), as does a failed
/// compile (defensive: the pattern is built from a one-char quotechar and an
/// escaped one-char delimiter), rather than panicking across the FFI boundary.
fn has_doubled_quote(data: &str, quotechar: &str, delim: &str) -> bool {
    let Ok(regexp) = doubled_quote_regex(quotechar, delim) else {
        return false;
    };
    let doubled = quotechar.repeat(2);
    for caps in regexp.captures_iter(data) {
        let Ok(caps) = caps else { return false };
        if caps
            .get(1)
            .is_some_and(|body| body.as_str().contains(&doubled))
        {
            return true;
        }
    }
    false
}

/// Mirror of `Sniffer._guess_quote_and_delimiter` (see `QuoteGuess`).
///
/// CPython's `delim == '\n'` reset is omitted, as in the Python oracle: both
/// delimiter groups exclude `\n`, so it is unreachable.
fn guess_quote_and_delimiter(data: &str, delimiters: Option<&str>) -> QuoteGuess {
    let tally = first_quote_tally(data, delimiters);
    // `quotechar = max(quotes, key=quotes.get)`; "" when nothing matched, as
    // CPython's `('', False, None, 0)` early return gives.
    let quotechar = first_max_key(&tally.quotes)
        .map(|(k, _)| k)
        .unwrap_or_default();
    let Some((delimiter, count)) = first_max_key(&tally.delims) else {
        // No match, or a single column of quoted data. Doubled quotes are only
        // sought once a delimiter is known (gh-109638).
        return QuoteGuess {
            quotechar,
            doublequote: false,
            delimiter: String::new(),
            skipinitialspace: false,
        };
    };
    // `skipinitialspace = delims[delim] == spaces` — the WINNING delim's count
    // vs the total space count.
    let skipinitialspace = count == tally.spaces;
    let doublequote = has_doubled_quote(data, &quotechar, &delimiter);
    QuoteGuess {
        quotechar,
        doublequote,
        delimiter,
        skipinitialspace,
    }
}

/// `data[0].count(d) == data[0].count("%c " % d)` — non-overlapping substring
/// counts of the delimiter and "delimiter + space" on the FIRST surviving line.
fn skipinitialspace_for(first_line: &str, delim: char) -> bool {
    let with_space = format!("{delim} ");
    first_line.matches(delim).count() == first_line.matches(with_space.as_str()).count()
}

/// `(mode_freq, adjusted_count)` for a character — the adjusted count subtracts
/// every other frequency's count and can therefore go negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Mode {
    freq: usize,
    adjusted: i64,
}

/// A char's meta-frequency table: `(freq, lines)` pairs saying how many lines
/// saw it `freq` times, insertion-ordered like the Python dict.
type MetaFrequency = Vec<(usize, usize)>;

/// `char_frequency`: each scanned char's `MetaFrequency`, insertion-ordered.
type CharFrequency = Vec<(char, MetaFrequency)>;

/// `ascii = [chr(c) for c in range(127)]`: code points 0..=126, NOT 127.
const SNIFF_ASCII_END: u8 = 127;
/// `chunkLength`: lines added per sweep.
const SNIFF_CHUNK_LINES: usize = 10;
/// The lowest consistency a sweep tries.
const SNIFF_CONSISTENCY_THRESHOLD: f64 = 0.9;

/// `char_frequency.setdefault(ch, {})`.
fn meta_frequency_of(char_frequency: &mut CharFrequency, ch: char) -> &mut MetaFrequency {
    let idx = char_frequency
        .iter()
        .position(|(c, _)| *c == ch)
        .unwrap_or_else(|| {
            char_frequency.push((ch, MetaFrequency::new()));
            char_frequency.len() - 1
        });
    &mut char_frequency[idx].1
}

/// Mirrors `_count_char_frequencies`: adds each line's count of every scanned
/// char, zeros included, to `char_frequency[char][count]`.
fn count_char_frequencies(lines: &[&str], char_frequency: &mut CharFrequency) {
    for line in lines {
        for ch in (0..SNIFF_ASCII_END).map(char::from) {
            let freq = line.matches(ch).count();
            let meta = meta_frequency_of(char_frequency, ch);
            match meta.iter_mut().find(|(f, _)| *f == freq) {
                Some((_, count)) => *count += 1,
                None => meta.push((freq, 1)),
            }
        }
    }
}

/// One char's entry in `_char_modes`; `None` for a char that never occurred
/// (`{0: n}`). The mode is the FIRST frequency with the highest line count
/// (`max(items, key=...)`); a lone frequency is its own mode, nothing taken off.
fn char_mode(meta: &[(usize, usize)]) -> Option<Mode> {
    if let [(0, _)] = meta {
        return None;
    }
    let (freq, count) = first_max_key(meta)?;
    let others: i64 = meta
        .iter()
        .filter(|(f, _)| *f != freq)
        .map(|(_, c)| *c as i64)
        .sum();
    Some(Mode {
        freq,
        adjusted: count as i64 - others,
    })
}

/// Mirrors `_char_modes`: every occurring char's mode, in insertion order.
fn char_modes(char_frequency: &[(char, MetaFrequency)]) -> Vec<(char, Mode)> {
    char_frequency
        .iter()
        .filter_map(|(ch, meta)| char_mode(meta).map(|mode| (*ch, mode)))
        .collect()
}

/// Mirrors `_consistent_delimiters`: the allowed chars whose positive mode holds
/// on at least `consistency` of `total` rows, at the strictest level any char
/// reaches; empty if none does. `consistency` relaxes from 1.0 by CPython's
/// repeated `-= 0.01` while >= 0.9: ten passes, the last at 0.9099999999999999,
/// so an exact 0.9 never qualifies.
fn consistent_delimiters(
    modes: &[(char, Mode)],
    total: f64,
    delimiters: Option<&str>,
) -> Vec<(char, Mode)> {
    let qualifies = |ch: char, mode: Mode, consistency: f64| {
        mode.freq > 0
            && mode.adjusted > 0
            && mode.adjusted as f64 / total >= consistency
            && delimiters.is_none_or(|allowed| allowed.contains(ch))
    };
    let mut consistency = 1.0f64;
    // Bounded: ten 0.01 steps take 1.0 below the threshold.
    while consistency >= SNIFF_CONSISTENCY_THRESHOLD {
        let found: Vec<(char, Mode)> = modes
            .iter()
            .copied()
            .filter(|&(ch, mode)| qualifies(ch, mode, consistency))
            .collect();
        if !found.is_empty() {
            return found;
        }
        consistency -= 0.01;
    }
    Vec::new()
}

/// Mirrors `_pick_delimiter`: the first `PREFERRED` delimiter in `delims`, else
/// the dominant one, the max `(freq, adjusted, char)` (CPython sorts
/// `(mode, char)` pairs and takes the last). Returns it with its
/// `skipinitialspace` on `first_line`; `("", false)` for an empty `delims`.
fn pick_delimiter(delims: &[(char, Mode)], first_line: &str) -> (String, bool) {
    let preferred = PREFERRED
        .into_iter()
        .find(|pref| delims.iter().any(|(ch, _)| ch == pref));
    let dominant = || {
        delims
            .iter()
            .map(|&(ch, mode)| (mode.freq, mode.adjusted, ch))
            .max()
            .map(|(_, _, ch)| ch)
    };
    match preferred.or_else(dominant) {
        Some(delim) => (delim.to_string(), skipinitialspace_for(first_line, delim)),
        None => (String::new(), false),
    }
}

/// Mirrors `_guess_delimiter`: `(delimiter, skipinitialspace)`, with an empty
/// delimiter meaning "undetermined". Sweeps cumulative chunks of ten lines and
/// stops at the first that finds any delimiter. CPython keeps `delims` across
/// chunks but only sweeps while it is empty, so a later chunk can never change
/// it: stopping there returns what CPython returns.
fn guess_delimiter(data: &str, delimiters: Option<&str>) -> (String, bool) {
    // `list(filter(None, data.split('\n')))` — drop empty lines after splitting.
    let lines: Vec<&str> = data.split('\n').filter(|line| !line.is_empty()).collect();
    let Some(&first_line) = lines.first() else {
        return (String::new(), false);
    };
    let chunk_length = SNIFF_CHUNK_LINES.min(lines.len());
    let mut char_frequency = CharFrequency::new();
    // Bounded: one pass per chunk of `lines`.
    for (idx, chunk) in lines.chunks(chunk_length.max(1)).enumerate() {
        count_char_frequencies(chunk, &mut char_frequency);
        // `min(chunkLength * iteration, len(data))`
        let total = chunk_length.saturating_mul(idx + 1).min(lines.len()) as f64;
        let delims = consistent_delimiters(&char_modes(&char_frequency), total, delimiters);
        if !delims.is_empty() {
            return pick_delimiter(&delims, first_line);
        }
    }
    (String::new(), false)
}

/// gh-103925: `sample.replace('\r\n', '\n').replace('\r', '\n')`, borrowed
/// when there is no `\r` — always, for a file sample, whose probe's universal
/// newlines already translated both.
fn normalise_line_endings(sample: &str) -> Cow<'_, str> {
    if !sample.contains('\r') {
        return Cow::Borrowed(sample);
    }
    Cow::Owned(sample.replace("\r\n", "\n").replace('\r', "\n"))
}

/// `csv.Sniffer().sniff(sample, delimiters)`. Returns `None` everywhere Python
/// raises `csv.Error` (i.e. no delimiter could be determined).
pub fn sniff(sample: &str, delimiters: Option<&str>) -> Option<SniffedDialect> {
    let sample = normalise_line_endings(sample);
    let quote_guess = guess_quote_and_delimiter(&sample, delimiters);

    let (delimiter, doublequote, mut quotechar, skipinitialspace) =
        if quote_guess.delimiter.is_empty() {
            // Quote guess could not determine a delimiter; fall back to frequency.
            let (delim, sis) = guess_delimiter(&sample, delimiters);
            (delim, quote_guess.doublequote, quote_guess.quotechar, sis)
        } else {
            (
                quote_guess.delimiter,
                quote_guess.doublequote,
                quote_guess.quotechar,
                quote_guess.skipinitialspace,
            )
        };

    if delimiter.is_empty() {
        return None;
    }

    // `_csv.reader` won't accept an empty quotechar -> fall back to '"'.
    if quotechar.is_empty() {
        quotechar = "\"".to_string();
    }

    Some(SniffedDialect {
        delimiter,
        quotechar,
        doublequote,
        skipinitialspace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sniff_ok(sample: &str) -> SniffedDialect {
        sniff(sample, None).expect("expected a dialect")
    }

    #[test]
    fn quoted_sample_finds_quote_and_delim() {
        let d = sniff_ok("id,note\n1,\"a,b\"\n2,\"c,d\"\n");
        assert_eq!(d.delimiter, ",");
        assert_eq!(d.quotechar, "\"");
    }

    #[test]
    fn unquoted_sample_uses_frequency_guess() {
        let d = sniff_ok("a;b;c\n1;2;3\n4;5;6\n");
        assert_eq!(d.delimiter, ";");
        assert_eq!(d.quotechar, "\""); // fallback quotechar
        assert!(!d.doublequote);
    }

    #[test]
    fn doubled_quotes_set_doublequote() {
        // Pre-fix CPython (3.12.3 / 3.14.3) answered ' ' here: its lazy `.*?`
        // body slid across the doubled quotes to pair `" ""hi"" `, whose
        // surrounding spaces matched the delim class. The fixed Sniffer
        // (gh-98820) ends the body at the first undoubled quote, giving ','.
        let d = sniff_ok("id,note\n1,\"say \"\"hi\"\" now\"\n2,\"ok\"\n");
        assert_eq!(d.delimiter, ",");
        assert!(d.doublequote);
    }

    #[test]
    fn skipinitialspace_detected() {
        let d = sniff_ok("a, \"b\", c\n1, \"x\", 3\n");
        assert_eq!(d.delimiter, ",");
        assert!(d.skipinitialspace);
    }

    #[test]
    fn undeterminable_sample_returns_a_frequency_delim() {
        // CORRECTED vs prompt skeleton: CPython does NOT raise here. The quote
        // guess finds nothing, so _guess_delimiter runs and picks the most
        // frequent char 'o'. Verified against CPython 3.12:
        //   csv.Sniffer().sniff("justoneword").delimiter == 'o'
        let d = sniff("justoneword", None).expect("frequency guess yields 'o'");
        assert_eq!(d.delimiter, "o");
    }

    #[test]
    fn empty_sample_returns_none() {
        // CPython raises csv.Error("Could not determine delimiter") -> None.
        assert!(sniff("", None).is_none());
    }

    #[test]
    fn delimiters_restriction_is_honored() {
        let d = sniff("a,b;c\n1,2;3\n", Some(";")).expect("restricted sniff");
        assert_eq!(d.delimiter, ";");
    }

    #[test]
    fn preferred_order_breaks_ties() {
        let d = sniff_ok("a,b\tc,d\n1,2\t3,4\n5,6\t7,8\n");
        assert_eq!(d.delimiter, ",");
    }

    /// Every expectation below was generated by running the exact sample through
    /// CPython 3.12 `csv.Sniffer().sniff(...)` during development and recording
    /// delimiter / quotechar / doublequote / skipinitialspace. Re-checked against
    /// the fixed-Sniffer oracle (`_compute_python._sniff_sample`, #378): the
    /// upstream fixes change none of them.
    #[test]
    fn python_oracle_extra_cases() {
        // single-quoted fields -> quotechar "'", delim ","
        let d = sniff_ok("name,val\n'a,b',1\n'c,d',2\n");
        assert_eq!(d.delimiter, ",");
        assert_eq!(d.quotechar, "'");
        assert!(!d.doublequote);

        // colon-delimited, no quotes -> frequency guess
        let d = sniff_ok("a:b:c\n1:2:3\n4:5:6\n");
        assert_eq!(d.delimiter, ":");
        assert_eq!(d.quotechar, "\"");

        // space-delimited with quotes -> quote guess yields delim ' '
        let d = sniff_ok("a \"b\" c\n1 \"x\" 3\n");
        assert_eq!(d.delimiter, " ");
        assert_eq!(d.quotechar, "\"");

        // pipe-delimited -> frequency guess
        let d = sniff_ok("a|b|c\n1|2|3\n");
        assert_eq!(d.delimiter, "|");

        // tab-delimited -> frequency guess
        let d = sniff_ok("a\tb\tc\n1\t2\t3\n");
        assert_eq!(d.delimiter, "\t");

        // twelve data rows -> exercises a SECOND chunk iteration (chunk_length
        // is 10), still resolves to ','.
        let twelve: String = (0..12).map(|i| format!("r{i},a,b\n")).collect();
        let d = sniff_ok(&twelve);
        assert_eq!(d.delimiter, ",");

        // restriction to ':' even though ',' is present and more frequent.
        let d = sniff("a:b,c\n1:2,3\n", Some(":")).expect("restricted");
        assert_eq!(d.delimiter, ":");
    }

    #[test]
    fn adversarial_long_line_does_not_panic() {
        // One ~1.8 MB newline-free line of alternating quotes and delimiters
        // drives fancy_regex past its backtrack limit. Before the graceful-Err
        // fix this PANICKED (BacktrackLimitExceeded) and crossed the FFI
        // boundary. Now the limit still ends the scans (1.8M matchless chars
        // after the first field), but only after the first quote pattern's
        // `,'a',` match, which alone yields the Python oracle's answer.
        let mut sample = String::from(",'a'");
        for _ in 0..600_000 {
            sample.push_str(",a'");
        }
        let d = sniff(&sample, None).expect("a cut-short scan still yields a dialect");
        let oracle = SniffedDialect {
            delimiter: ",".to_string(),
            quotechar: "'".to_string(),
            doublequote: false,
            skipinitialspace: false,
        };
        assert_eq!(d, oracle);
    }

    /// `(id, sample, delimiters restriction, expected)` where `expected` is
    /// `(delimiter, quotechar, doublequote, skipinitialspace)`, or `None` where
    /// CPython raises `csv.Error`.
    type FixedCase = (
        &'static str,
        &'static str,
        Option<&'static str>,
        Option<(&'static str, &'static str, bool, bool)>,
    );

    /// The step-1 table of tests/core_tests/csv_io_tests/
    /// test_sniff_dialect_cpython_sync.py (#378), same ids, inputs and results:
    /// what the FIXED CPython Sniffer (v3.10.22, cross-checked on v3.14.10)
    /// returns. Generated from that file, not retyped. The Python cases go
    /// through a file, whose probe translates CRLF; here the raw sample reaches
    /// `sniff`, so `embedded-quotes-crlf` also exercises gh-103925.
    const FIXED_CPYTHON_CASES: &[FixedCase] = &[
        (
            "corpus-embedded-quotes",
            "id,note\n1,\"say \"\"hi\"\" now\"\n2,ok\n",
            None,
            Some((",", "\"", true, false)),
        ),
        (
            "embedded-quotes-quoted-tail",
            "id,note\n1,\"say \"\"hi\"\" now\"\n2,\"ok\"\n",
            None,
            Some((",", "\"", true, false)),
        ),
        (
            "embedded-quotes-crlf",
            "id,note\r\n1,\"say \"\"hi\"\" now\"\r\n2,ok\r\n",
            None,
            Some((",", "\"", true, false)),
        ),
        (
            "embedded-quotes-semicolon",
            "id;note\n1;\"say \"\"hi\"\" now\"\n2;ok\n",
            None,
            Some((";", "\"", true, false)),
        ),
        (
            "embedded-quotes-tab",
            "id\tnote\n1\t\"say \"\"hi\"\" now\"\n2\tok\n",
            None,
            Some(("\t", "\"", true, false)),
        ),
        (
            "doubled-quote-field-start",
            "a,b\n\"\"\"quoted\"\" start\",1\n\"plain\",2\n",
            None,
            Some((",", "\"", true, false)),
        ),
        (
            "inch-marks-tab",
            "name\theight\n\"x \"\"y\"\" z\"\t5'10\"\n",
            None,
            Some(("\t", "\"", true, false)),
        ),
        (
            "apostrophe-then-quoted-tab",
            "k\tv\n'a'\t\"x \"\"y\"\" z\"\n",
            None,
            Some(("\t", "'", false, false)),
        ),
        (
            "mixed-quotes-pipe",
            "'a'|\"a\"\n\"b\"|12\n",
            None,
            Some(("|", "'", false, false)),
        ),
        (
            "spaced-semicolons",
            "a; b\n \"x\"; \"y\"\n",
            None,
            Some((";", "\"", false, true)),
        ),
        (
            "semicolon-doubled-then-field",
            "note;id\n\"a; \"\"b\"\"\";1\n",
            None,
            Some((";", "\"", true, false)),
        ),
        (
            "quoted-delims-and-doubled",
            "k,v\n\"a,b\",\"say \"\"x, y\"\"\"\n\"c\",\"d\"\n",
            None,
            Some((",", "\"", true, false)),
        ),
        (
            "single-column-doubled",
            "\"a\"\"b\"\n\"c\"\"d\"\n\"e\"\"f\"\n",
            None,
            Some(("\"", "\"", false, false)),
        ),
        (
            "single-column-title-doubled",
            "title\n\"\"\"Dune\"\" novel\"\n",
            None,
            Some(("l", "\"", false, false)),
        ),
        (
            "comma-space-doubled",
            "note\n\"a, \"\"b\"\"\"\n",
            None,
            None,
        ),
        (
            "restricted-semicolon",
            "a;b,c\n1;\"x \"\"y\"\" z\",3\n",
            Some(";"),
            Some((";", "\"", false, false)),
        ),
        (
            "restricted-space",
            "id,note\n1,\"say \"\"hi\"\" now\"\n2,ok\n",
            Some(" "),
            None,
        ),
        (
            "restricted-semicolon-spaced",
            " \"a\";a; \"a\"\n",
            Some(";"),
            Some((";", "\"", false, true)),
        ),
        (
            "restricted-tab-doubled",
            "\"a\tb\"\n\"a\t \"\"b\"\"\"\n",
            Some("\t"),
            Some(("\t", "\"", false, false)),
        ),
        (
            "control-restricted-comma",
            "id,note\n1,\"say \"\"hi\"\" now\"\n2,ok\n",
            Some(","),
            Some((",", "\"", true, false)),
        ),
        (
            "control-skipinitialspace",
            "a, \"b\", c\n1, \"x\", 3\n",
            None,
            Some((",", "\"", false, true)),
        ),
        (
            "control-quoted-delims-inside",
            "name,addr\n\"Smith, J\",\"1 Main St, Apt 2\"\n\"Doe, A\",\"9 Elm\"\n",
            None,
            Some((",", "\"", false, false)),
        ),
        (
            "control-frequency-single",
            "a;b;c\n1;2;3\n4;5;6\n",
            None,
            Some((";", "\"", false, false)),
        ),
        (
            "control-preferred-tie-break",
            "a,b\tc,d\n1,2\t3,4\n5,6\t7,8\n",
            None,
            Some((",", "\"", false, false)),
        ),
        (
            "control-dominant-char-fallback",
            "ab\nab\n",
            None,
            Some(("b", "\"", false, false)),
        ),
    ];

    #[test]
    fn matches_fixed_cpython_sniffer_table() {
        let mut mismatches = Vec::new();
        for (id, sample, delimiters, expected) in FIXED_CPYTHON_CASES {
            let want = expected.map(|(delimiter, quotechar, doublequote, skipinitialspace)| {
                SniffedDialect {
                    delimiter: delimiter.to_string(),
                    quotechar: quotechar.to_string(),
                    doublequote,
                    skipinitialspace,
                }
            });
            let got = sniff(sample, *delimiters);
            if got != want {
                mismatches.push(format!("{id}: got {got:?}, want {want:?}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} cases differ:\n{}",
            mismatches.len(),
            FIXED_CPYTHON_CASES.len(),
            mismatches.join("\n")
        );
    }

    #[test]
    fn sniff_normalises_line_endings() {
        // gh-103925: CRLF and lone CR become LF before any guessing. Pre-fix
        // CPython returned doublequote False for both samples.
        let want = SniffedDialect {
            delimiter: ",".to_string(),
            quotechar: "\"".to_string(),
            doublequote: true,
            skipinitialspace: false,
        };
        for sample in [
            "a,b\r\n1,\"x \"\"y\"\"\"\r\n2,\"z\"\r\n",
            "a,b\r1,\"x \"\"y\"\"\"\r2,\"z\"\r",
        ] {
            assert_eq!(sniff(sample, None).as_ref(), Some(&want), "{sample:?}");
        }
    }

    /// Line shapes aimed at the quoted-field body (gh-98820) and the
    /// doubled-quote search (gh-109638): the four shapes of the #378 ReDoS probe,
    /// then shapes that were super-linear under the pre-fix patterns.
    fn adversarial_lines(n: usize) -> Vec<(&'static str, String)> {
        vec![
            (
                "unterminated_quote",
                format!("id,note\n1,\"{}\n", "a".repeat(n)),
            ),
            ("quote_run", format!("a,b\n{}\n", "\"".repeat(n))),
            (
                "comma_quote_openers",
                format!("a,b\n{}\n", ",\"".repeat(n / 2)),
            ),
            (
                "doubled_unterminated",
                format!("a,\"{}\n", "\"\"".repeat(n / 2)),
            ),
            (
                "mixed_quote_openers",
                format!("a,b\n{}\n", ",'\"".repeat(n / 3)),
            ),
            (
                "comma_space_quote",
                format!("a,b\n{}\n", ", \"".repeat(n / 3)),
            ),
            (
                "doubled_body_unterminated",
                format!("a,b\n1,\"{}\n", "x\"\"".repeat(n / 3)),
            ),
        ]
    }

    #[test]
    fn adversarial_lines_stay_within_fancy_regex_runtime_limits() {
        // The Python oracle has no backtrack limit, so any runtime Err here is
        // a place the Rust result may diverge from it (see the note on
        // `COMPILED_QUOTE_PATTERNS`). The fixed patterns were measured at <= ~4
        // backtracks per char on these shapes, so a 128k-char line keeps every
        // scan at about half the 1M default limit and well inside the 1M-entry
        // stack.
        const LINE_CHARS: usize = 128 * 1024;
        let mut failures = Vec::new();
        for (name, line) in adversarial_lines(LINE_CHARS) {
            for (idx, (regexp, _)) in COMPILED_QUOTE_PATTERNS.iter().enumerate() {
                if let Some(err) = regexp.captures_iter(&line).find_map(Result::err) {
                    failures.push(format!("{name}: quote pattern {}: {err}", idx + 1));
                }
            }
            for quote in ["\"", "'"] {
                for delim in [",", " "] {
                    let regexp = doubled_quote_regex(quote, delim).expect("pattern compiles");
                    if let Some(err) = regexp.captures_iter(&line).find_map(Result::err) {
                        failures.push(format!("{name}: doubled-quote {quote}/{delim:?}: {err}"));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    // ---- guess_delimiter contracts (#383) -------------------------------
    // Characterisation: each expectation is `_compute_python._guess_delimiter`'s
    // answer for the same input. Called directly, so the quote guess never runs.

    fn guess(data: &str, delimiters: Option<&str>) -> (String, bool) {
        guess_delimiter(data, delimiters)
    }

    #[test]
    fn guess_preferred_list_beats_the_dominant_char() {
        // '|' is dominant (freq 2 vs 1), but ';' is preferred.
        assert_eq!(guess("a;b|c|d\n1;2|3|4\n", None), (";".into(), false));
        // PREFERRED order, not code-point order: '\t' (9) sorts before ','
        // (44) and dominates it, yet ',' comes first in PREFERRED.
        assert_eq!(guess("a\tb\tc,d\n1\t2\t3,4\n", None), (",".into(), false));
        // ';' before ' ' in PREFERRED; skipinitialspace from the first line.
        assert_eq!(guess("a; b; c\n1; 2; 3\n", None), (";".into(), true));
    }

    #[test]
    fn guess_dominant_char_orders_by_freq_before_char() {
        // Neither is preferred: '#' (freq 2) beats '|' (freq 1, higher char).
        assert_eq!(guess("a#b#c|d\n1#2#3|4\n", None), ("#".into(), false));
        // Equal (freq, adjusted): the higher code point wins ('|' over '#').
        assert_eq!(guess("a|b#c\n1|2#3\n", None), ("|".into(), false));
        assert_eq!(guess("x#y| z\n1#2|3\n", None), ("|".into(), true));
    }

    #[test]
    fn guess_tied_or_negative_mode_is_never_a_candidate() {
        // ';' occurs 1, 2 and 3 times: a three-way tie whose adjusted count is
        // 1 - 2 = -1. Were it positive (e.g. wrapped), preferred ';' would win.
        assert_eq!(guess(";|\n;;|\n;;;|\n", None), ("|".into(), false));
        // A two-way tie adjusts to 0, also never a candidate.
        assert_eq!(guess(";|\n;;|\n", None), ("|".into(), false));
    }

    #[test]
    fn guess_later_chunks_cannot_add_delimiters() {
        // Chunk 1 (lines 1-10) finds only '|' (',' misses line 1: 8/10). By
        // line 30 ',' reaches 28/30 and would qualify, but the first chunk that
        // finds any delimiter decides; adding ',' would make it win (preferred).
        let single = format!("|\n{}{}", "|,\n".repeat(9), ",\n".repeat(20));
        assert_eq!(guess(&single, None), ("|".into(), false));
        // Same with two candidates from chunk 1 ('#', '|' -> dominant '|').
        let multi = format!("|#\n{}{}", "|#,\n".repeat(9), ",\n".repeat(20));
        assert_eq!(guess(&multi, None), ("|".into(), false));
    }

    #[test]
    fn guess_consistency_of_exactly_point_nine_never_qualifies() {
        // CPython relaxes 1.0 by repeated `-= 0.01`; its last pass is at
        // 0.9099999999999999, so ',' at 18/20 = 0.9 is never accepted
        // (`1.0 - 10.0 * 0.01` would be exactly 0.9 and accept it).
        let twenty = format!("|\n{}", ",\n".repeat(19));
        assert_eq!(guess(&twenty, None), (String::new(), false));
        // At 28/30 it qualifies (the first line has no ',' -> skipinitialspace).
        let thirty = format!("|\n{}", ",\n".repeat(29));
        assert_eq!(guess(&thirty, None), (",".into(), true));
    }

    #[test]
    fn guess_delimiters_filter_can_exclude_every_candidate() {
        assert_eq!(guess("a,b\n1,2\n", Some(";")), (String::new(), false));
    }

    #[test]
    fn guess_skipinitialspace_reads_the_first_surviving_line() {
        // Empty lines are dropped first; only "a| b| c" has '|' + space.
        assert_eq!(
            guess("\n\na| b| c\n1|2|3\n4|5|6\n", None),
            ("|".into(), true)
        );
    }

    // ---- helper contracts (#383) ----------------------------------------
    // Each expectation is the named `_compute_python` helper's answer for the
    // same input. Some contracts cannot be seen through `guess_delimiter`
    // (a tied mode always adjusts to <= 0, and its chars arrive in code-point
    // order), so they are pinned here.

    fn mode(freq: usize, adjusted: i64) -> Mode {
        Mode { freq, adjusted }
    }

    #[test]
    fn count_char_frequencies_counts_every_ascii_char_including_zeros() {
        let mut char_frequency = CharFrequency::new();
        count_char_frequencies(&["a,b", "c"], &mut char_frequency);
        // Code points 0..=126 in order; DEL (127) is never scanned.
        let chars: Vec<char> = char_frequency.iter().map(|(ch, _)| *ch).collect();
        let want: Vec<char> = (0u8..127).map(char::from).collect();
        assert_eq!(chars, want);
        let meta = |ch: char| {
            char_frequency
                .iter()
                .find(|(c, _)| *c == ch)
                .map(|(_, meta)| meta.clone())
        };
        assert_eq!(meta(','), Some(vec![(1, 1), (0, 1)]));
        assert_eq!(meta('c'), Some(vec![(0, 1), (1, 1)]));
        assert_eq!(meta('z'), Some(vec![(0, 2)]));
        // Accumulates across calls (one call per chunk).
        count_char_frequencies(&["x,y"], &mut char_frequency);
        let meta = |ch: char| {
            char_frequency
                .iter()
                .find(|(c, _)| *c == ch)
                .map(|(_, meta)| meta.clone())
        };
        assert_eq!(meta(','), Some(vec![(1, 2), (0, 1)]));
        assert_eq!(meta('x'), Some(vec![(0, 2), (1, 1)]));
    }

    #[test]
    fn char_modes_keeps_the_first_of_tied_frequencies() {
        // `max(items, key=count)` keeps the first; others are subtracted.
        let three_way = vec![(';', vec![(1, 1), (2, 1), (3, 1)])];
        assert_eq!(char_modes(&three_way), vec![(';', mode(1, -1))]);
        let two_way = vec![('x', vec![(2, 3), (5, 3), (1, 1)])];
        assert_eq!(char_modes(&two_way), vec![('x', mode(2, -1))]);
    }

    #[test]
    fn char_modes_skips_absent_chars_and_keeps_insertion_order() {
        let char_frequency = vec![
            ('a', vec![(0, 4)]),
            ('|', vec![(2, 4)]),
            (',', vec![(0, 1), (1, 9)]),
        ];
        assert_eq!(
            char_modes(&char_frequency),
            vec![('|', mode(2, 4)), (',', mode(1, 8))]
        );
    }

    #[test]
    fn consistent_delimiters_returns_only_the_strictest_level() {
        // 'b' holds on every row (1.0); 'a' on 0.995 joins only at 0.99.
        let modes = [('a', mode(1, 199)), ('b', mode(1, 200))];
        assert_eq!(
            consistent_delimiters(&modes, 200.0, None),
            vec![('b', mode(1, 200))]
        );
    }

    #[test]
    fn consistent_delimiters_uses_cpythons_float_steps() {
        let found = |adjusted: i64, total: f64| {
            !consistent_delimiters(&[(',', mode(1, adjusted))], total, None).is_empty()
        };
        assert!(!found(18, 20.0), "0.9 is below the last pass");
        assert!(found(182, 200.0), "0.91 meets the last pass");
        assert!(!found(181, 200.0), "0.905 is below the last pass");
    }

    #[test]
    fn consistent_delimiters_needs_a_positive_mode_and_an_allowed_char() {
        let modes = [
            ('a', mode(0, 5)),
            ('b', mode(1, 0)),
            ('c', mode(1, -2)),
            ('d', mode(2, 5)),
        ];
        assert_eq!(
            consistent_delimiters(&modes, 5.0, None),
            vec![('d', mode(2, 5))]
        );
        let modes = [(',', mode(1, 10)), (';', mode(1, 10))];
        assert_eq!(
            consistent_delimiters(&modes, 10.0, Some(";")),
            vec![(';', mode(1, 10))]
        );
        assert!(consistent_delimiters(&modes, 10.0, Some("|")).is_empty());
    }

    #[test]
    fn pick_delimiter_takes_the_first_preferred_present() {
        let delims = [('|', mode(2, 2)), (';', mode(1, 2))];
        assert_eq!(pick_delimiter(&delims, "a;b|c|d"), (";".into(), false));
        let delims = [('\t', mode(2, 2)), (',', mode(1, 2))];
        assert_eq!(pick_delimiter(&delims, "a\tb\tc,d"), (",".into(), false));
    }

    #[test]
    fn pick_delimiter_otherwise_takes_the_max_of_freq_adjusted_char() {
        // By char on a tie, whatever the insertion order.
        let delims = [('|', mode(1, 5)), ('#', mode(1, 5))];
        assert_eq!(pick_delimiter(&delims, "x#y|z"), ("|".into(), false));
        // freq first, then adjusted, both before char.
        let delims = [('#', mode(2, 5)), ('|', mode(1, 5))];
        assert_eq!(pick_delimiter(&delims, "x#y|z"), ("#".into(), false));
        let delims = [('#', mode(1, 6)), ('|', mode(1, 5))];
        assert_eq!(pick_delimiter(&delims, "x# y|z"), ("#".into(), true));
    }

    #[test]
    fn pick_delimiter_of_nothing_is_undetermined() {
        assert_eq!(pick_delimiter(&[], "a,b"), (String::new(), false));
    }
}
