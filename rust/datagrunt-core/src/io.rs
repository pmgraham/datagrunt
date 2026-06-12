//! File IO helpers replicating Python's `open(encoding="utf-8-sig",
//! errors="ignore")` probes and `FileProperties` checks.

use std::fs::File;
use std::io::Read;
use std::path::Path;

const BOM: &[u8] = b"\xef\xbb\xbf";

/// Python `errors="ignore"`: invalid byte sequences are dropped, not replaced.
pub fn decode_ignore(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.push_str(s);
                break;
            }
            Err(e) => {
                let (valid, after) = rest.split_at(e.valid_up_to());
                // SAFETY: `from_utf8` just validated every byte in
                // `0..e.valid_up_to()`, so this sub-slice is guaranteed
                // well-formed UTF-8 per the Utf8Error contract.
                out.push_str(unsafe { std::str::from_utf8_unchecked(valid) });
                // error_len() is None only for an incomplete multibyte
                // sequence truncated at EOF — the remainder is the invalid
                // tail, so consuming all of it is correct.
                let skip = e.error_len().unwrap_or(after.len());
                rest = &after[skip..];
            }
        }
    }
    out
}

/// Read whole file as text the way Python's probes do: utf-8-sig + ignore.
pub fn read_decoded(path: &Path) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    let body = if bytes.starts_with(BOM) { &bytes[BOM.len()..] } else { &bytes[..] };
    Ok(decode_ignore(body))
}

/// Python universal-newline translation: \r\n and lone \r both become \n.
pub fn universal_newlines(s: &str) -> String {
    s.replace("\r\n", "\n").replace('\r', "\n")
}

/// Lines exactly as Python text-mode iteration yields them (sans trailing \n).
/// "a\nb\n" -> ["a","b"]; "a\nb" -> ["a","b"]; "" -> [].
pub fn read_universal_lines(path: &Path) -> std::io::Result<Vec<String>> {
    let text = universal_newlines(&read_decoded(path)?);
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let mut lines: Vec<String> = text.split('\n').map(str::to_string).collect();
    if text.ends_with('\n') {
        lines.pop();
    }
    Ok(lines)
}

/// First 4096 bytes contain \r and no \n. False on any IO error (best-effort).
/// Inspects only the first 4096 bytes, matching the Python probe exactly.
pub fn is_legacy_mac_newlines(path: &Path) -> bool {
    let mut chunk = Vec::with_capacity(4096);
    let ok = File::open(path).and_then(|f| f.take(4096).read_to_end(&mut chunk));
    if ok.is_err() {
        return false;
    }
    chunk.contains(&b'\r') && !chunk.contains(&b'\n')
}

/// FileProperties.is_empty parity: true iff the file is zero bytes.
pub fn is_empty(path: &Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.len() == 0)
}

/// BlankFile.is_blank: >=10MB (after Python's nested rounding: >= 9_999_995
/// bytes) is never blank; otherwise strict decode (BOM allowed) — invalid
/// UTF-8 counts as content; blank iff all whitespace.
/// The stat-then-read order mirrors Python's BlankFile (the size gate is advisory; Python has the same TOCTOU characteristics, and parity is the spec).
pub fn is_blank(path: &Path) -> bool {
    let size = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(_) => return false,
    };
    if size >= 9_999_995 {
        return false;
    }
    let mut bytes = Vec::new();
    if File::open(path).and_then(|mut f| f.read_to_end(&mut bytes)).is_err() {
        return false;
    }
    let body = if bytes.starts_with(BOM) { &bytes[BOM.len()..] } else { &bytes[..] };
    match std::str::from_utf8(body) {
        Ok(s) => s.trim().is_empty(),
        Err(_) => false,
    }
}

/// FileProperties.is_tsv parity: extension is "tsv", case-insensitive.
pub fn is_tsv(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("tsv"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp(content: &[u8], ext: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(ext).tempfile().unwrap();
        f.write_all(content).unwrap();
        f
    }

    #[test]
    fn decode_ignore_drops_invalid_bytes() {
        assert_eq!(decode_ignore(b"Jos\xe9,NYC"), "Jos,NYC"); // errors="ignore" DROPS
        assert_eq!(decode_ignore("héllo".as_bytes()), "héllo");
    }

    #[test]
    fn read_decoded_strips_bom() {
        let f = tmp(b"\xef\xbb\xbfa,b\n1,2\n", ".csv");
        assert_eq!(read_decoded(f.path()).unwrap(), "a,b\n1,2\n");
    }

    #[test]
    fn universal_lines_match_python_iteration() {
        let f = tmp(b"a\r\nb\rc\n", ".csv");
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["a", "b", "c"]);
        let g = tmp(b"a\nb", ".csv"); // no trailing newline
        assert_eq!(read_universal_lines(g.path()).unwrap(), vec!["a", "b"]);
        let h = tmp(b"", ".csv"); // empty file -> zero lines
        assert!(read_universal_lines(h.path()).unwrap().is_empty());
        let i = tmp(b"a\n\n", ".csv"); // trailing newline dropped, interior blank kept
        assert_eq!(read_universal_lines(i.path()).unwrap(), vec!["a", ""]);
    }

    #[test]
    fn legacy_mac_detection() {
        assert!(is_legacy_mac_newlines(tmp(b"a,b\rc,d\r", ".csv").path()));
        assert!(!is_legacy_mac_newlines(tmp(b"a,b\r\nc,d\r\n", ".csv").path()));
        assert!(!is_legacy_mac_newlines(tmp(b"a,b\nc,d\n", ".csv").path()));
        assert!(!is_legacy_mac_newlines(std::path::Path::new("/nonexistent/x.csv")));
    }

    #[test]
    fn file_probes() {
        assert!(is_empty(tmp(b"", ".csv").path()).unwrap());
        assert!(!is_empty(tmp(b"a", ".csv").path()).unwrap());
        assert!(is_blank(tmp(b"  \n\t\n ", ".csv").path()));
        assert!(!is_blank(tmp(b"a", ".csv").path()));
        assert!(!is_blank(tmp(b"\xff\xfe", ".csv").path())); // invalid UTF-8 = content
        assert!(is_blank(tmp(b"\xef\xbb\xbf \n", ".csv").path())); // BOM + whitespace
        assert!(is_tsv(std::path::Path::new("x.tsv")));
        assert!(is_tsv(std::path::Path::new("x.TSV")));
        assert!(!is_tsv(std::path::Path::new("x.csv")));
    }
}
