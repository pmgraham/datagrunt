//! File IO helpers replicating Python's `open(encoding="utf-8-sig",
//! errors="ignore")` probes and `FileProperties` checks.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

const BOM: &[u8] = b"\xef\xbb\xbf";
const CHUNK_SIZE: usize = 64 * 1024;

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

/// Streaming version of Python text-mode iteration (universal newlines,
/// utf-8-sig, errors="ignore"). Reads in 64 KiB chunks; callers that stop
/// early never pay for the rest of the file.
///
/// A line ends at `\n`, `\r\n`, or a lone `\r` (each counts as one break).
/// Lines are yielded without their terminator; a final line without a
/// trailing terminator is still yielded; a trailing terminator at EOF does
/// not produce an extra empty line; an empty file yields no lines.
pub struct UniversalLines {
    reader: BufReader<File>,
    buf: Vec<u8>, // undecoded carry-over bytes (line content + lookahead)
    pos: usize,   // scan position within `buf`
    eof: bool,
    bom_checked: bool,
    errored: bool,
}

/// Open `path` for streaming Python-text-mode line iteration.
pub fn universal_lines(path: &Path) -> std::io::Result<UniversalLines> {
    Ok(UniversalLines {
        reader: BufReader::new(File::open(path)?),
        buf: Vec::new(),
        pos: 0,
        eof: false,
        bom_checked: false,
        errored: false,
    })
}

impl UniversalLines {
    /// Pull one more chunk from the file into `buf`. Returns the number of
    /// bytes read (0 at EOF). Drops already-scanned bytes first to bound memory.
    fn fill(&mut self) -> std::io::Result<usize> {
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        let start = self.buf.len();
        self.buf.resize(start + CHUNK_SIZE, 0);
        let n = self.reader.read(&mut self.buf[start..])?;
        self.buf.truncate(start + n);
        if n == 0 {
            self.eof = true;
        }
        Ok(n)
    }

    /// Strip a single leading BOM if present, reading enough bytes to decide.
    fn check_bom(&mut self) -> std::io::Result<()> {
        while self.buf.len() < BOM.len() && !self.eof {
            self.fill()?;
        }
        if self.buf.starts_with(BOM) {
            self.pos = BOM.len();
        }
        self.bom_checked = true;
        Ok(())
    }

    /// Decode one universal-newline-delimited line (without terminator), or
    /// `None` at EOF. Errors propagate.
    fn next_line(&mut self) -> std::io::Result<Option<String>> {
        if !self.bom_checked {
            self.check_bom()?;
        }
        loop {
            // Scan the unread region for the next \r or \n.
            let mut i = self.pos;
            while i < self.buf.len() {
                match self.buf[i] {
                    b'\n' => {
                        let line = decode_ignore(&self.buf[self.pos..i]);
                        self.pos = i + 1;
                        return Ok(Some(line));
                    }
                    b'\r' => {
                        // Need the byte after \r to tell \r from \r\n. If it
                        // sits past the current buffer and more may come, read on.
                        if i + 1 == self.buf.len() && !self.eof {
                            break;
                        }
                        let line = decode_ignore(&self.buf[self.pos..i]);
                        let next_is_lf = self.buf.get(i + 1) == Some(&b'\n');
                        self.pos = i + 1 + usize::from(next_is_lf);
                        return Ok(Some(line));
                    }
                    _ => i += 1,
                }
            }
            // No complete line in the buffer. Read more, or flush the tail.
            if self.eof {
                if self.pos < self.buf.len() {
                    let line = decode_ignore(&self.buf[self.pos..]);
                    self.pos = self.buf.len();
                    return Ok(Some(line));
                }
                return Ok(None);
            }
            self.fill()?;
        }
    }
}

impl Iterator for UniversalLines {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        match self.next_line() {
            Ok(Some(line)) => Some(Ok(line)),
            Ok(None) => None,
            Err(e) => {
                self.errored = true;
                Some(Err(e))
            }
        }
    }
}

/// Lines exactly as Python text-mode iteration yields them (sans trailing \n).
/// "a\nb\n" -> ["a","b"]; "a\nb" -> ["a","b"]; "" -> [].
///
/// Thin `collect()` over [`universal_lines`] so there is one line-splitting
/// implementation. Other components rely on this for small samples.
pub fn read_universal_lines(path: &Path) -> std::io::Result<Vec<String>> {
    universal_lines(path)?.collect::<Result<Vec<_>, _>>()
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
    fn streaming_crlf_spanning_chunk_boundary() {
        // Put the \r\n pair astride the 64 KiB read boundary: 65535 'a' then
        // \r\n lands the \r at index 65535 (chunk end) and \n at 65536 (next
        // chunk). The pair must still count as ONE break.
        let mut content = vec![b'a'; CHUNK_SIZE - 1];
        content.extend_from_slice(b"\r\nb\n");
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].len(), CHUNK_SIZE - 1);
        assert_eq!(lines[1], "b");
    }

    #[test]
    fn streaming_lone_cr_file() {
        let f = tmp(b"a\rb\rc", ".csv");
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn streaming_bom_file() {
        let f = tmp(b"\xef\xbb\xbfa,b\n1,2\n", ".csv");
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["a,b", "1,2"]);
    }

    #[test]
    fn streaming_empty_file() {
        let f = tmp(b"", ".csv");
        assert!(read_universal_lines(f.path()).unwrap().is_empty());
    }

    #[test]
    fn streaming_no_trailing_newline() {
        let f = tmp(b"a\nb\nc", ".csv");
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn streaming_trailing_cr_at_eof() {
        // Lone \r at EOF terminates the last line; no extra empty line.
        let f = tmp(b"a\rb\r", ".csv");
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn streaming_invalid_utf8_dropped() {
        // errors="ignore": the lone \xe9 is dropped per line.
        let f = tmp(b"Jos\xe9,NYC\nb\n", ".csv");
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["Jos,NYC", "b"]);
    }

    #[test]
    fn streaming_matches_old_impl_for_mixed_newlines() {
        // Equality check: the streaming collect must reproduce the exact
        // line vector the previous whole-file implementation produced.
        let content = b"a\r\nb\rc\n\nd";
        let f = tmp(content, ".csv");
        let streamed = read_universal_lines(f.path()).unwrap();
        // Reference: old whole-file split implementation.
        let text = decode_ignore(content).replace("\r\n", "\n").replace('\r', "\n");
        let mut expected: Vec<String> = text.split('\n').map(str::to_string).collect();
        if text.ends_with('\n') {
            expected.pop();
        }
        assert_eq!(streamed, expected);
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
