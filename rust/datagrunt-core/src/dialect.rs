//! Port of CPython `csv.Sniffer` (`sniff`, `_guess_quote_and_delimiter`,
//! `_guess_delimiter`). Read the local `csv.py` Sniffer class side-by-side with
//! this file; the structure mirrors it deliberately, including its oddities.
//!
//! Parity notes (places the real CPython source matters):
//!   * Every dict in the Sniffer iterates in insertion order, and
//!     `max(d, key=d.get)` returns the FIRST key reaching the max. We model this
//!     with insertion-ordered `Vec<(K, V)>` plus strictly-greater comparison.
//!   * `_guess_delimiter` scans `ascii = [chr(c) for c in range(127)]`, i.e.
//!     code points 0..=126 (NOT 0..=127).
//!   * The "mode" of a char's frequency table is adjusted by subtracting the
//!     counts of every OTHER frequency, which can go negative -> `i64`.
//!   * The consistency loop and `delims` map PERSIST across chunk iterations.
//!
//! Only the four data-driven fields live here. `lineterminator` ("\r\n"),
//! `quoting` (QUOTE_MINIMAL) and the absence of `escapechar` are constants the
//! binding layer applies (Task 10); they are not part of this core struct.

use fancy_regex::Regex;

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
fn first_max_key(map: &[(String, usize)]) -> Option<(String, usize)> {
    let mut best: Option<(&str, usize)> = None;
    for (k, v) in map {
        match best {
            Some((_, bv)) if *v > bv => best = Some((k, *v)),
            None => best = Some((k, *v)),
            _ => {}
        }
    }
    best.map(|(k, v)| (k.to_string(), v))
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

/// The four DOTALL|MULTILINE patterns, in the order Python tries them. The bool
/// records whether the pattern carries `delim`/`space` groups (the 4th does
/// not — Python hits `KeyError` and `continue`s, so it never records delims or
/// spaces for that pattern). Python `(?P=quote)` backreferences become
/// fancy-regex `\k<quote>`.
const QUOTE_PATTERNS: [(&str, bool); 4] = [
    (
        r#"(?sm)(?P<delim>[^\w\n"'])(?P<space> ?)(?P<quote>["']).*?\k<quote>\k<delim>"#,
        true,
    ),
    (
        r#"(?sm)(?:^|\n)(?P<quote>["']).*?\k<quote>(?P<delim>[^\w\n"'])(?P<space> ?)"#,
        true,
    ),
    (
        r#"(?sm)(?P<delim>[^\w\n"'])(?P<space> ?)(?P<quote>["']).*?\k<quote>(?:$|\n)"#,
        true,
    ),
    (
        r#"(?sm)(?:^|\n)(?P<quote>["']).*?\k<quote>(?:$|\n)"#,
        false,
    ),
];

fn guess_quote_and_delimiter(data: &str, delimiters: Option<&str>) -> QuoteGuess {
    // (quote, delim, space) per non-overlapping match, plus whether the winning
    // pattern even has delim/space groups. First pattern with ANY match wins.
    let mut matches: Vec<(String, Option<String>, Option<String>)> = Vec::new();
    let mut has_delim_groups = false;
    for (pattern, has_groups) in QUOTE_PATTERNS {
        let regexp = Regex::new(pattern).expect("static sniffer pattern is valid");
        let mut found = Vec::new();
        for caps in regexp.captures_iter(data) {
            let caps = caps.expect("capture iteration");
            let quote = caps.name("quote").map(|m| m.as_str().to_string());
            let delim = caps.name("delim").map(|m| m.as_str().to_string());
            let space = caps.name("space").map(|m| m.as_str().to_string());
            // `quote` is present in all four patterns; if absent the sample
            // could not have matched, so default to empty for safety.
            found.push((quote.unwrap_or_default(), delim, space));
        }
        if !found.is_empty() {
            matches = found;
            has_delim_groups = has_groups;
            break;
        }
    }

    if matches.is_empty() {
        return QuoteGuess {
            quotechar: String::new(),
            doublequote: false,
            delimiter: String::new(),
            skipinitialspace: false,
        };
    }

    let mut quotes: Vec<(String, usize)> = Vec::new();
    let mut delims: Vec<(String, usize)> = Vec::new();
    let mut spaces: usize = 0;
    for (quote, delim, space) in &matches {
        if !quote.is_empty() {
            bump(&mut quotes, quote);
        }
        // The 4th pattern has no delim group: Python raises KeyError and
        // `continue`s, so delims AND spaces are skipped entirely for it.
        if !has_delim_groups {
            continue;
        }
        let delim = delim.as_deref().unwrap_or("");
        if !delim.is_empty() {
            // The `delimiters` restriction is a SUBSTRING check (`key in
            // delimiters`), exactly like Python's `key in delimiters` on a str.
            let allowed = delimiters.is_none_or(|allowed| allowed.contains(delim));
            if allowed {
                bump(&mut delims, delim);
            }
        }
        if space.as_deref().is_some_and(|s| !s.is_empty()) {
            spaces += 1;
        }
    }

    // `quotechar = max(quotes, key=quotes.get)`. `quotes` is non-empty here
    // because every winning pattern records a quote; fall back to "" defensively.
    let quotechar = first_max_key(&quotes).map(|(k, _)| k).unwrap_or_default();

    let mut delim;
    let skipinitialspace;
    if let Some((winning_delim, winning_count)) = first_max_key(&delims) {
        // `skipinitialspace = delims[delim] == spaces` — the WINNING delim's
        // count vs the total space count.
        skipinitialspace = winning_count == spaces;
        delim = winning_delim;
        if delim == "\n" {
            // Most likely a single-column file. Note Python KEEPS the already
            // computed skipinitialspace value here.
            delim = String::new();
        }
    } else {
        delim = String::new();
        skipinitialspace = false;
    }

    // Doubled-quote detection. Built even for empty delim (re.escape("") == "").
    // MULTILINE only, RAW quotechar, fancy_regex::escape on the delim.
    let escaped_delim = fancy_regex::escape(&delim);
    let dq_pattern = format!(
        r"(?m)(({delim})|^)\W*{quote}[^{delim}\n]*{quote}[^{delim}\n]*{quote}\W*(({delim})|$)",
        delim = escaped_delim,
        quote = quotechar,
    );
    let doublequote = Regex::new(&dq_pattern)
        .expect("doublequote pattern is valid")
        .is_match(data)
        .expect("doublequote search");

    QuoteGuess {
        quotechar,
        doublequote,
        delimiter: delim,
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
#[derive(Clone, Copy, PartialEq, Eq)]
struct Mode {
    freq: usize,
    adjusted: i64,
}

/// Faithful port of `_guess_delimiter`. Returns (delimiter, skipinitialspace);
/// an empty delimiter means "undetermined".
fn guess_delimiter(data: &str, delimiters: Option<&str>) -> (String, bool) {
    // `list(filter(None, data.split('\n')))` — drop empty lines after splitting.
    let lines: Vec<&str> = data.split('\n').filter(|line| !line.is_empty()).collect();

    // `ascii = [chr(c) for c in range(127)]` -> code points 0..=126.
    let ascii: Vec<char> = (0u8..127).map(char::from).collect();

    let chunk_length = std::cmp::min(10, lines.len());
    let mut iteration: usize = 0;

    // charFrequency[char] is the meta-frequency table: how many lines saw the
    // char `freq` times. Insertion-ordered to mirror Python dict iteration.
    let mut char_frequency: Vec<(char, Vec<(usize, usize)>)> = Vec::new();
    // modes[char], rebuilt each iteration; insertion-ordered.
    let mut modes: Vec<(char, Mode)> = Vec::new();
    // delims PERSISTS across chunk iterations (Python never clears it).
    let mut delims: Vec<(char, Mode)> = Vec::new();

    let mut start = 0usize;
    let mut end = chunk_length;
    while start < lines.len() {
        iteration += 1;

        // 1) accumulate per-char meta-frequencies over this chunk.
        for line in &lines[start..std::cmp::min(end, lines.len())] {
            for &ch in &ascii {
                let freq = line.matches(ch).count();
                let meta = match char_frequency.iter_mut().find(|(c, _)| *c == ch) {
                    Some((_, meta)) => meta,
                    None => {
                        char_frequency.push((ch, Vec::new()));
                        &mut char_frequency.last_mut().unwrap().1
                    }
                };
                match meta.iter_mut().find(|(f, _)| *f == freq) {
                    Some((_, count)) => *count += 1,
                    None => meta.push((freq, 1)),
                }
            }
        }

        // 2) recompute the mode for every char seen so far.
        modes.clear();
        for (ch, items) in &char_frequency {
            // `if len(items) == 1 and items[0][0] == 0: continue`
            if items.len() == 1 && items[0].0 == 0 {
                continue;
            }
            let mode = if items.len() > 1 {
                // max-by-count, first wins ties.
                let mut best_idx = 0usize;
                for (idx, (_, count)) in items.iter().enumerate() {
                    if *count > items[best_idx].1 {
                        best_idx = idx;
                    }
                }
                let (best_freq, best_count) = items[best_idx];
                // adjusted = best_count - sum(every OTHER count).
                let others: i64 = items
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| *idx != best_idx)
                    .map(|(_, (_, c))| *c as i64)
                    .sum();
                Mode {
                    freq: best_freq,
                    adjusted: best_count as i64 - others,
                }
            } else {
                let (freq, count) = items[0];
                Mode {
                    freq,
                    adjusted: count as i64,
                }
            };
            modes.push((*ch, mode));
        }

        // 3) consistency sweep. `delims` persists, so this only adds entries.
        let total = std::cmp::min(chunk_length * iteration, lines.len()) as f64;
        let mut consistency = 1.0f64;
        let threshold = 0.9f64;
        while delims.is_empty() && consistency >= threshold {
            for (k, v) in &modes {
                if v.freq > 0 && v.adjusted > 0 {
                    let ratio = v.adjusted as f64 / total;
                    let allowed = delimiters.is_none_or(|allowed| allowed.contains(*k));
                    if ratio >= consistency && allowed {
                        // Mirror dict insertion: only add a char once.
                        if !delims.iter().any(|(dk, _)| dk == k) {
                            delims.push((*k, *v));
                        }
                    }
                }
            }
            consistency -= 0.01;
        }

        // Exactly one delimiter found -> early return.
        if delims.len() == 1 {
            let delim = delims[0].0;
            let sis = skipinitialspace_for(lines[0], delim);
            return (delim.to_string(), sis);
        }

        start = end;
        end += chunk_length;
    }

    if delims.is_empty() {
        return (String::new(), false);
    }

    // More than one: fall back to the preferred ordering.
    if delims.len() > 1 {
        for &pref in &PREFERRED {
            if delims.iter().any(|(k, _)| *k == pref) {
                let sis = skipinitialspace_for(lines[0], pref);
                return (pref.to_string(), sis);
            }
        }
    }

    // Otherwise pick the dominant char: Python builds `[(v, k) ...]`, sorts, and
    // takes the LAST. v is the (freq, adjusted) tuple, so the sort key is
    // ((freq, adjusted), char); the maximum such tuple wins.
    let mut items: Vec<(usize, i64, char)> =
        delims.iter().map(|(k, v)| (v.freq, v.adjusted, *k)).collect();
    items.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
    });
    let delim = items.last().unwrap().2;
    let sis = skipinitialspace_for(lines[0], delim);
    (delim.to_string(), sis)
}

/// `csv.Sniffer().sniff(sample, delimiters)`. Returns `None` everywhere Python
/// raises `csv.Error` (i.e. no delimiter could be determined).
pub fn sniff(sample: &str, delimiters: Option<&str>) -> Option<SniffedDialect> {
    let quote_guess = guess_quote_and_delimiter(sample, delimiters);

    let (delimiter, doublequote, mut quotechar, mut skipinitialspace) = if quote_guess
        .delimiter
        .is_empty()
    {
        // Quote guess could not determine a delimiter; fall back to frequency.
        let (delim, sis) = guess_delimiter(sample, delimiters);
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

    // The doublequote/skipinitialspace from a successful quote guess are kept
    // as computed above; this rebind exists only to satisfy the borrow shape.
    let _ = &mut skipinitialspace;

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
        // CORRECTED vs prompt skeleton: the real CPython Sniffer guesses the
        // delimiter ' ' (space) here, NOT ',', because the space surrounding the
        // quoted field matches the delim character class in the winning quote
        // pattern. Verified against CPython 3.12 csv.Sniffer().sniff(...):
        //   delim=' ' quote='"' doublequote=True skipinitialspace=False
        let d = sniff_ok("id,note\n1,\"say \"\"hi\"\" now\"\n2,\"ok\"\n");
        assert_eq!(d.delimiter, " ");
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
    /// delimiter / quotechar / doublequote / skipinitialspace.
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
}
