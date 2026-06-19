//! File IO helpers replicating Python's `open(encoding="utf-8-sig",
//! errors="ignore")` probes and `FileProperties` checks.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

const BOM: &[u8] = b"\xef\xbb\xbf";
const CHUNK_SIZE: usize = 64 * 1024;

/// Per-physical-line cap (chars), identical to Python _compute_python.MAX_LINE_CHARS.
pub const MAX_LINE_CHARS: usize = 2 * 1024 * 1024;
/// Byte read bound: 4x the char cap (UTF-8 is ≤4 bytes/char), so the char cap
/// is always the binding limit and memory stays bounded.
const MAX_LINE_READ_BYTES: usize = MAX_LINE_CHARS * 4;

/// Truncate `s` to at most `n` Unicode scalar values (chars).
///
/// Unlike a byte slice, this always lands on a valid char boundary.
pub fn take_chars(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

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

/// Streaming equivalent of [`read_decoded`] (+ optional universal-newline
/// translation): BOM stripped at offset 0, invalid UTF-8 bytes dropped
/// (errors="ignore"), reading the underlying file in 64 KiB chunks so
/// consumers that stop early never pay for the rest of the file.
///
/// Implements [`std::io::Read`] so the `csv` crate (or any `BufReader`) can
/// pull decoded bytes lazily. Output is byte-for-byte identical to
/// `read_decoded(path)` (translate_newlines = false) or
/// `universal_newlines(&read_decoded(path))` (translate_newlines = true).
pub struct DecodedReader {
    inner: BufReader<File>,
    translate_newlines: bool, // \r\n -> \n and lone \r -> \n when true
    /// Un-emitted bytes carried to the next chunk: either an incomplete UTF-8
    /// sequence (<=3 bytes) truncated at a chunk boundary, or a pending `\r`
    /// awaiting its successor byte (translation needs 1-byte lookahead).
    carry: Vec<u8>,
    /// Decoded bytes produced but not yet handed to the caller's buffer.
    pending: Vec<u8>,
    pending_pos: usize,
    bom_checked: bool,
    eof: bool,
}

impl DecodedReader {
    /// Open `path` for streaming decoded reads. `translate_newlines` mirrors
    /// Python's `newline=None` (true) vs `newline=""` (false).
    pub fn open(path: &Path, translate_newlines: bool) -> std::io::Result<Self> {
        Ok(DecodedReader {
            inner: BufReader::new(File::open(path)?),
            translate_newlines,
            carry: Vec::new(),
            pending: Vec::new(),
            pending_pos: 0,
            bom_checked: false,
            eof: false,
        })
    }

    /// Read the next raw chunk from the file into `chunk`. Returns bytes read
    /// (0 at EOF). Strips a leading BOM exactly once, at file offset 0.
    fn read_chunk(&mut self, chunk: &mut Vec<u8>) -> std::io::Result<usize> {
        chunk.clear();
        chunk.resize(CHUNK_SIZE, 0);
        let n = self.inner.read(&mut chunk[..])?;
        chunk.truncate(n);
        if n == 0 {
            self.eof = true;
        }
        Ok(n)
    }

    /// Refill `pending` with the next decoded chunk. Returns false at true EOF
    /// (nothing more will ever be produced), true if it appended output.
    ///
    /// On the first call enough bytes are buffered to settle the BOM question
    /// (a file shorter than 3 bytes at offset 0 cannot be a BOM, so whatever is
    /// there is emitted). Thereafter each call: prepends `carry`, decodes
    /// errors="ignore" up to the last complete UTF-8 boundary, carries an
    /// incomplete trailing sequence, then (if translating) folds newlines while
    /// holding back a trailing `\r` for lookahead.
    fn refill(&mut self) -> std::io::Result<bool> {
        loop {
            if self.eof {
                return self.flush_carry_at_eof();
            }

            let mut chunk = Vec::new();
            self.read_chunk(&mut chunk)?;

            if !self.bom_checked {
                // Accumulate into `carry` until we have >=3 bytes or hit EOF,
                // so a BOM split across the first reads is still recognised.
                self.carry.extend_from_slice(&chunk);
                if self.carry.len() < BOM.len() && !self.eof {
                    continue;
                }
                if self.carry.starts_with(BOM) {
                    self.carry.drain(..BOM.len());
                }
                self.bom_checked = true;
                chunk = std::mem::take(&mut self.carry);
            } else if !self.carry.is_empty() {
                // Prepend carried bytes (incomplete UTF-8 seq and/or pending \r).
                let mut merged = std::mem::take(&mut self.carry);
                merged.extend_from_slice(&chunk);
                chunk = merged;
            }

            if self.eof {
                return self.flush_carry_at_eof_with(chunk);
            }
            if chunk.is_empty() {
                continue;
            }

            // Decode errors="ignore", carrying an incomplete trailing sequence.
            let decoded = self.decode_carry_tail(&chunk);
            let translated = self.translate_carry_tail(decoded);
            if translated.is_empty() {
                // Everything became carry (e.g. a lone trailing \r); read on.
                continue;
            }
            self.pending = translated.into_bytes();
            self.pending_pos = 0;
            return Ok(true);
        }
    }

    /// Decode `bytes` with errors="ignore". If the final bytes are an
    /// incomplete (but not yet invalid) multibyte sequence, push them onto
    /// `self.carry` for the next chunk instead of decoding them now.
    fn decode_carry_tail(&mut self, bytes: &[u8]) -> String {
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
                    // SAFETY: `from_utf8` validated `0..valid_up_to()`.
                    out.push_str(unsafe { std::str::from_utf8_unchecked(valid) });
                    match e.error_len() {
                        // Truly invalid: drop the offending bytes and continue.
                        Some(skip) => rest = &after[skip..],
                        // Incomplete at the end of the chunk (<=3 bytes): not
                        // invalid yet, carry it forward and stop decoding.
                        None => {
                            self.carry.extend_from_slice(after);
                            break;
                        }
                    }
                }
            }
        }
        out
    }

    /// Apply universal-newline translation when enabled, holding back a single
    /// trailing `\r` because it needs 1-byte lookahead to distinguish `\r\n`
    /// from a lone `\r`. The `\r` is carried as a RAW byte and stripped from the
    /// decoded text BEFORE translation, so `universal_newlines` never sees it
    /// (it would otherwise map a chunk-final lone `\r` to `\n` prematurely).
    fn translate_carry_tail(&mut self, mut decoded: String) -> String {
        if !self.translate_newlines {
            return decoded;
        }
        if decoded.ends_with('\r') {
            decoded.pop();
            // Prepend so the source byte order is preserved if an incomplete
            // UTF-8 tail was already carried by `decode_carry_tail` (the `\r`
            // precedes that tail in the original stream).
            self.carry.insert(0, b'\r');
        }
        universal_newlines(&decoded)
    }

    /// At true EOF with no incoming chunk: emit whatever remains in `carry`.
    /// An incomplete UTF-8 tail is dropped (matching Python's errors="ignore"
    /// at EOF); a carried trailing `\r` becomes `\n` under translation.
    fn flush_carry_at_eof(&mut self) -> std::io::Result<bool> {
        if self.carry.is_empty() {
            return Ok(false);
        }
        let carry = std::mem::take(&mut self.carry);
        self.emit_eof_carry(carry)
    }

    /// EOF reached while merging a final `chunk` with `carry`.
    fn flush_carry_at_eof_with(&mut self, chunk: Vec<u8>) -> std::io::Result<bool> {
        if !self.bom_checked {
            // File ended before 3 bytes; `chunk` already absorbed `carry` above.
            self.bom_checked = true;
        }
        self.emit_eof_carry(chunk)
    }

    /// Decode + translate a final byte run at EOF. A pending `\r` carried from a
    /// previous chunk that is still trailing becomes `\n`; an incomplete UTF-8
    /// tail is dropped.
    fn emit_eof_carry(&mut self, bytes: Vec<u8>) -> std::io::Result<bool> {
        if bytes.is_empty() {
            return Ok(false);
        }
        // Decode dropping any incomplete tail (errors="ignore" at EOF).
        let decoded = decode_ignore(&bytes);
        let out = if self.translate_newlines {
            universal_newlines(&decoded)
        } else {
            decoded
        };
        if out.is_empty() {
            return Ok(false);
        }
        self.pending = out.into_bytes();
        self.pending_pos = 0;
        Ok(true)
    }
}

impl Read for DecodedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Refill `pending` until we have output or reach true exhaustion.
        while self.pending_pos >= self.pending.len() {
            if !self.refill()? {
                return Ok(0);
            }
        }
        let available = &self.pending[self.pending_pos..];
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.pending_pos += n;
        Ok(n)
    }
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

    /// Skip bytes in the stream until (and including) the next `\r`, `\n`, or
    /// `\r\n` terminator. Called after capping a giant line to restore
    /// line-stream alignment.
    fn skip_to_next_line(&mut self) -> std::io::Result<()> {
        loop {
            let mut i = self.pos;
            while i < self.buf.len() {
                match self.buf[i] {
                    b'\n' => {
                        self.pos = i + 1;
                        return Ok(());
                    }
                    b'\r' => {
                        // Need lookahead for \r\n.
                        if i + 1 == self.buf.len() && !self.eof {
                            // Keep the \r in the buffer and read more for lookahead.
                            self.pos = i;
                            self.fill()?;
                            // Restart scan; self.pos still points at the \r.
                            break;
                        }
                        let next_is_lf = self.buf.get(i + 1) == Some(&b'\n');
                        self.pos = i + 1 + usize::from(next_is_lf);
                        return Ok(());
                    }
                    _ => i += 1,
                }
            }
            if self.eof {
                self.pos = self.buf.len();
                return Ok(());
            }
            // No terminator found yet; drain scanned bytes and read more.
            self.pos = i;
            self.fill()?;
        }
    }

    /// Decode one universal-newline-delimited line (without terminator), or
    /// `None` at EOF. Lines are capped at `MAX_LINE_CHARS` characters so a
    /// pathological newline-free file cannot force unbounded buffering.
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
                        let raw = decode_ignore(&self.buf[self.pos..i]);
                        let line = take_chars(&raw, MAX_LINE_CHARS);
                        self.pos = i + 1;
                        return Ok(Some(line));
                    }
                    b'\r' => {
                        // Need the byte after \r to tell \r from \r\n. If it
                        // sits past the current buffer and more may come, read on.
                        if i + 1 == self.buf.len() && !self.eof {
                            break;
                        }
                        let raw = decode_ignore(&self.buf[self.pos..i]);
                        let line = take_chars(&raw, MAX_LINE_CHARS);
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
                    let raw = decode_ignore(&self.buf[self.pos..]);
                    let line = take_chars(&raw, MAX_LINE_CHARS);
                    self.pos = self.buf.len();
                    return Ok(Some(line));
                }
                return Ok(None);
            }
            self.fill()?;
            // Cap: if we've buffered more than MAX_LINE_READ_BYTES without
            // finding a newline, emit the capped line and stream-skip the rest.
            // This prevents unbounded allocation on malformed newline-free files.
            let unread = self.buf.len() - self.pos;
            if unread >= MAX_LINE_READ_BYTES {
                let cap_end = self.pos + MAX_LINE_READ_BYTES;
                let raw = decode_ignore(&self.buf[self.pos..cap_end]);
                let line = take_chars(&raw, MAX_LINE_CHARS);
                self.pos = cap_end;
                self.skip_to_next_line()?;
                return Ok(Some(line));
            }
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

/// Convert all line endings to `\n`, matching Python's universal-newlines mode
/// (`newline=None`): `\r\n` → `\n`, then lone `\r` → `\n`.
///
/// Used by the CSV row-counting path for legacy-mac files, where Python calls
/// `open(..., newline=None)` so that `\r`-only line endings are normalised
/// before `csv.reader` sees the text.
pub fn universal_newlines(s: &str) -> String {
    // Replace CRLF first so the lone-\r pass does not double-convert them.
    s.replace("\r\n", "\n").replace('\r', "\n")
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

    /// Drain a `DecodedReader` fully into a String, using `read_to_string` so
    /// the caller buffering matches a realistic large-read consumer.
    fn drain(path: &Path, translate: bool) -> String {
        let mut s = String::new();
        DecodedReader::open(path, translate)
            .unwrap()
            .read_to_string(&mut s)
            .unwrap();
        s
    }

    /// Drain one byte at a time to stress the Read impl under tiny buffers.
    fn drain_byte_by_byte(path: &Path, translate: bool) -> Vec<u8> {
        let mut reader = DecodedReader::open(path, translate).unwrap();
        let mut out = Vec::new();
        let mut one = [0u8; 1];
        loop {
            match reader.read(&mut one).unwrap() {
                0 => break,
                _ => out.push(one[0]),
            }
        }
        out
    }

    /// Reference eager output: read_decoded (+ optional universal_newlines).
    fn eager(path: &Path, translate: bool) -> String {
        let raw = read_decoded(path).unwrap();
        if translate {
            universal_newlines(&raw)
        } else {
            raw
        }
    }

    #[test]
    fn decoded_reader_matches_read_decoded_basic() {
        let f = tmp(b"\xef\xbb\xbfa,b\n1,2\n", ".csv");
        assert_eq!(drain(f.path(), false), "a,b\n1,2\n");
        assert_eq!(drain(f.path(), true), "a,b\n1,2\n");
    }

    #[test]
    fn decoded_reader_bom_split_across_first_reads() {
        // A file whose first chunk is shorter than 3 bytes: feed exactly the
        // BOM in two short reads by making the file the BOM + body. (The reader
        // accumulates until it can settle the BOM question.)
        let f = tmp(b"\xef\xbb\xbfhello", ".csv");
        assert_eq!(drain(f.path(), false), "hello");
        // Partial BOM at offset 0 that is NOT a BOM must be emitted verbatim.
        let g = tmp(b"\xef\xbb", ".csv"); // 2 bytes: incomplete BOM, also invalid UTF-8
        // errors="ignore" drops the incomplete tail at EOF, matching read_decoded.
        assert_eq!(drain(g.path(), false), eager(g.path(), false));
        // A 2-byte valid-UTF8 non-BOM start must round-trip.
        let h = tmp(b"ab", ".csv");
        assert_eq!(drain(h.path(), false), "ab");
    }

    #[test]
    fn decoded_reader_invalid_utf8_spanning_chunk_boundary() {
        // Place the 2-byte prefix of a 3-byte char (\xe2\x82, prefix of € =
        // \xe2\x82\xac) so the byte at CHUNK_SIZE-1 starts an incomplete
        // sequence completed by the next chunk. Must decode as one '€'.
        let mut content = vec![b'a'; CHUNK_SIZE - 1];
        content.extend_from_slice("€".as_bytes()); // \xe2 at idx CHUNK_SIZE-1
        content.extend_from_slice(b"tail");
        let f = tmp(&content, ".csv");
        let out = drain(f.path(), false);
        assert_eq!(out, eager(f.path(), false));
        assert!(out.contains('€'));
        assert_eq!(out.len(), (CHUNK_SIZE - 1) + "€".len() + 4);
    }

    #[test]
    fn decoded_reader_cr_at_chunk_boundary_with_lf_next() {
        // \r at the last byte of chunk 1, \n as the first byte of chunk 2 under
        // translation must collapse to exactly ONE \n.
        let mut content = vec![b'a'; CHUNK_SIZE - 1];
        content.extend_from_slice(b"\r\nb"); // \r at CHUNK_SIZE-1, \n at CHUNK_SIZE
        let f = tmp(&content, ".csv");
        let out = drain(f.path(), true);
        assert_eq!(out, eager(f.path(), true));
        // exactly one newline, no stray \r.
        assert_eq!(out.matches('\n').count(), 1);
        assert!(!out.contains('\r'));
    }

    #[test]
    fn decoded_reader_cr_at_eof_under_translation() {
        let f = tmp(b"a\rb\r", ".csv");
        assert_eq!(drain(f.path(), true), "a\nb\n");
        // Without translation, the \r passes through raw.
        assert_eq!(drain(f.path(), false), "a\rb\r");
    }

    #[test]
    fn decoded_reader_lone_cr_at_chunk_boundary_non_lf_next() {
        // \r at chunk end, next chunk starts with a non-\n byte -> lone \r -> \n.
        let mut content = vec![b'a'; CHUNK_SIZE - 1];
        content.extend_from_slice(b"\rb"); // \r at CHUNK_SIZE-1, 'b' next chunk
        let f = tmp(&content, ".csv");
        assert_eq!(drain(f.path(), true), eager(f.path(), true));
    }

    #[test]
    fn decoded_reader_tiny_buffer_matches_eager_mixed_file() {
        let content = b"\xef\xbb\xbfa,b\r\nJos\xe9,c\rd\ne\r";
        let f = tmp(content, ".csv");
        for translate in [false, true] {
            let bytes = drain_byte_by_byte(f.path(), translate);
            assert_eq!(
                String::from_utf8(bytes).unwrap(),
                eager(f.path(), translate),
                "tiny-buffer mismatch (translate={translate})"
            );
        }
    }

    #[test]
    fn decoded_reader_large_file_equivalence() {
        // ~200 KiB of mixed content spanning several 64 KiB chunks, with an
        // invalid byte and CR/CRLF/LF endings sprinkled in.
        let mut content = Vec::new();
        for i in 0..20000u32 {
            content.extend_from_slice(format!("row{i},val{i}").as_bytes());
            match i % 3 {
                0 => content.extend_from_slice(b"\r\n"),
                1 => content.push(b'\r'),
                _ => content.push(b'\n'),
            }
            if i % 500 == 0 {
                content.push(0xe9); // stray invalid byte, dropped by ignore
            }
        }
        assert!(content.len() > 200 * 1024);
        let f = tmp(&content, ".csv");
        for translate in [false, true] {
            assert_eq!(
                drain(f.path(), translate),
                eager(f.path(), translate),
                "large-file mismatch (translate={translate})"
            );
        }
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

    // --- per-line cap tests (issue #222) ---

    /// A line whose char count slightly exceeds MAX_LINE_CHARS (no newline until
    /// EOF) must be truncated to exactly MAX_LINE_CHARS chars on the EOF flush
    /// path. The file is ~2 MB — no stream-skip needed; the cap applies in the
    /// EOF tail flush branch.
    #[test]
    fn cap_giant_eof_line_truncated_to_max_chars() {
        // MAX_LINE_CHARS + 100 ASCII bytes, no newline until EOF.
        let content: Vec<u8> = b"a".repeat(MAX_LINE_CHARS + 100);
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 1, "should yield exactly one line");
        assert_eq!(
            lines[0].chars().count(),
            MAX_LINE_CHARS,
            "line must be capped at MAX_LINE_CHARS chars"
        );
    }

    /// Giant ASCII line terminated by '\n' followed by a normal line.
    /// The first line exceeds MAX_LINE_CHARS, so it must be capped; the second
    /// line must still be read correctly (alignment after cap applies).
    #[test]
    fn cap_giant_lf_line_then_next_line_reads_correctly() {
        // Build a file: (MAX_LINE_CHARS + 50) 'a' bytes + '\n' + "next_line\n".
        // The first physical line has MAX_LINE_CHARS + 50 chars (all ASCII).
        let mut content: Vec<u8> = b"a".repeat(MAX_LINE_CHARS + 50);
        content.extend_from_slice(b"\nnext_line\n");
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 2, "should yield the giant line and the next line");
        assert_eq!(
            lines[0].chars().count(),
            MAX_LINE_CHARS,
            "giant line capped at MAX_LINE_CHARS"
        );
        assert_eq!(lines[1], "next_line", "second line must read correctly after cap");
    }

    /// Giant line terminated by '\r\n' must be capped and the '\r\n' counted
    /// as a single line terminator (not two). The line after it must be intact.
    #[test]
    fn cap_giant_crlf_line_then_next_line_reads_correctly() {
        let mut content: Vec<u8> = b"b".repeat(MAX_LINE_CHARS + 50);
        content.extend_from_slice(b"\r\nnext_line\n");
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].chars().count(), MAX_LINE_CHARS);
        assert_eq!(lines[1], "next_line");
    }

    /// Multibyte char (€, 3 bytes) placed just before the char boundary:
    /// (MAX_LINE_CHARS - 1) ASCII 'a's + '€' + 200 more 'a's.
    /// The line has MAX_LINE_CHARS + 200 chars total; truncation must yield
    /// exactly MAX_LINE_CHARS chars = (MAX_LINE_CHARS - 1) 'a's + '€'.
    #[test]
    fn cap_multibyte_char_at_boundary() {
        let mut content: Vec<u8> = b"a".repeat(MAX_LINE_CHARS - 1);
        content.extend_from_slice("€".as_bytes()); // 3-byte UTF-8 char
        content.extend_from_slice(&b"a".repeat(200));
        content.push(b'\n');
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 1);
        let line = &lines[0];
        assert_eq!(
            line.chars().count(),
            MAX_LINE_CHARS,
            "truncation must yield exactly MAX_LINE_CHARS chars"
        );
        // The last included char must be '€' (char index MAX_LINE_CHARS - 1).
        assert_eq!(
            line.chars().last().unwrap(),
            '€',
            "multibyte char at boundary must be the last included char"
        );
    }

    /// A file with no newline and exactly MAX_LINE_READ_BYTES bytes triggers
    /// the stream-skip path in next_line (not the EOF flush). The resulting
    /// line must contain exactly MAX_LINE_CHARS chars, and no extra lines are
    /// produced.
    ///
    /// NOTE: this test allocates ~8 MB + small overhead — it is intentionally
    /// larger than the unit tests above because it targets the stream-skip branch
    /// specifically (the EOF-flush branch fires first for smaller inputs).
    #[test]
    fn cap_stream_skip_path_giant_no_newline() {
        // MAX_LINE_READ_BYTES = 4 * MAX_LINE_CHARS bytes, all ASCII.
        // After this many bytes without a '\n', next_line must cap + skip.
        let content: Vec<u8> = b"x".repeat(MAX_LINE_READ_BYTES + 100);
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].chars().count(), MAX_LINE_CHARS);
    }

    /// Stream-skip path followed by a valid next line: the giant (>MAX_LINE_READ_BYTES)
    /// line is capped and the line after the terminator is read intact.
    #[test]
    fn cap_stream_skip_then_next_line() {
        let mut content: Vec<u8> = b"z".repeat(MAX_LINE_READ_BYTES + 100);
        content.extend_from_slice(b"\nafter_giant\n");
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].chars().count(), MAX_LINE_CHARS);
        assert_eq!(lines[1], "after_giant");
    }
}
