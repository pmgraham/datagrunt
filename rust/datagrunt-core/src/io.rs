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
pub const MAX_LINE_READ_BYTES: usize = MAX_LINE_CHARS * 4;

/// Truncate `s` to at most `n` Unicode scalar values (chars).
///
/// Unlike a byte slice, this always lands on a valid char boundary.
pub fn take_chars(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// Python `str.isspace()` for a single char.
///
/// Identical to Rust's `char::is_whitespace` (Unicode `White_Space`) EXCEPT
/// that CPython also treats the C0 information separators FS/GS/RS/US
/// (U+001C-U+001F) as whitespace. Those four are the ONLY divergence between
/// the two definitions across the whole of Unicode (verified exhaustively), so
/// adding them makes Rust `strip`/`split` agree with Python (issue #176).
pub(crate) fn is_python_whitespace(c: char) -> bool {
    c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}')
}

/// Python `str.strip()` with no arguments: trim leading/trailing whitespace
/// using Python's whitespace definition (see [`is_python_whitespace`]).
pub(crate) fn py_strip(s: &str) -> &str {
    s.trim_matches(is_python_whitespace)
}

/// Python `str.split()` with no arguments: split on runs of Python-whitespace,
/// discarding empty leading/trailing fields. Mirrors `str::split_whitespace`
/// but with Python's whitespace definition (see [`is_python_whitespace`]).
pub(crate) fn py_split_whitespace(s: &str) -> impl Iterator<Item = &str> {
    s.split(is_python_whitespace)
        .filter(|field| !field.is_empty())
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

/// Decode bytes with errors="ignore", carrying any incomplete trailing
/// multibyte sequence into `carry` for the next call (boundary-safe).
/// Truly-invalid bytes are dropped. Used by the cap accumulation loop in
/// `UniversalLines::next_line` to avoid splitting multibyte chars at chunk
/// boundaries while streaming through a giant line.
fn decode_chunk_with_carry(bytes: &[u8], carry: &mut Vec<u8>) -> String {
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
                    Some(skip) => rest = &after[skip..], // truly invalid: drop
                    None => {
                        // Incomplete multibyte sequence at chunk boundary: carry forward.
                        carry.extend_from_slice(after);
                        break;
                    }
                }
            }
        }
    }
    out
}

/// Read whole file as text the way Python's probes do: utf-8-sig + ignore.
///
/// Test-only: the eager reference oracle for [`DecodedReader`]'s differential
/// tests. Production paths stream via [`DecodedReader`] instead of loading the
/// whole file.
#[cfg(test)]
pub(crate) fn read_decoded(path: &Path) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    let body = if bytes.starts_with(BOM) {
        &bytes[BOM.len()..]
    } else {
        &bytes[..]
    };
    Ok(decode_ignore(body))
}

/// Streaming equivalent of `read_decoded` (+ optional universal-newline
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
    /// Wrap an already-open `File` for streaming decoded reads.
    ///
    /// The file position is used as-is; callers that probe the file first
    /// must seek back to offset 0 before calling this so decoding starts
    /// from the file's beginning (BOM detection requires offset 0).
    pub fn from_file(file: File, translate_newlines: bool) -> Self {
        DecodedReader {
            inner: BufReader::new(file),
            translate_newlines,
            carry: Vec::new(),
            pending: Vec::new(),
            pending_pos: 0,
            bom_checked: false,
            eof: false,
        }
    }

    /// Open `path` for streaming decoded reads. `translate_newlines` mirrors
    /// Python's `newline=None` (true) vs `newline=""` (false).
    pub fn open(path: &Path, translate_newlines: bool) -> std::io::Result<Self> {
        Ok(Self::from_file(File::open(path)?, translate_newlines))
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
                    // Bytes remaining is a BYTE-level condition; a line is
                    // DECODED text. A tail whose every byte is dropped by
                    // errors="ignore" is not a line: CPython iterates the
                    // decoded string and yields nothing for it, so emitting
                    // Some("") here produced a phantom final line (#317).
                    // Only the unterminated tail is affected — an explicitly
                    // terminated empty line (b"a\n\n") is emitted by the \n
                    // branch above and still counts, matching CPython.
                    if line.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(line));
                }
                return Ok(None);
            }
            self.fill()?;
            // Cap: if we've buffered more than MAX_LINE_READ_BYTES without
            // finding a newline, emit the capped line and stream-skip the rest.
            // Budget on DECODED chars (not raw bytes) for parity with Python's
            // `decode(errors="ignore"); line[:MAX_LINE_CHARS]`. An all-invalid
            // byte stream never accumulates MAX_LINE_CHARS chars, but raw bytes
            // are drained as we go, keeping memory bounded.
            let unread = self.buf.len() - self.pos;
            if unread >= MAX_LINE_READ_BYTES {
                let mut line = String::with_capacity(MAX_LINE_CHARS * 4);
                let mut decoded_char_count: usize = 0;
                let mut carry: Vec<u8> = Vec::new();

                while decoded_char_count < MAX_LINE_CHARS {
                    // Drain already-processed bytes to keep memory bounded.
                    if self.pos > 0 {
                        self.buf.drain(..self.pos);
                        self.pos = 0;
                    }
                    if self.buf.is_empty() {
                        if self.eof {
                            break;
                        }
                        self.fill()?;
                        if self.buf.is_empty() {
                            break;
                        }
                    }
                    // Scan for newline so we don't over-read past the line end.
                    let scan_end = self.buf.len().min(CHUNK_SIZE);
                    let nl_pos = self.buf[..scan_end]
                        .iter()
                        .position(|&b| b == b'\n' || b == b'\r');
                    let chunk_end = nl_pos.unwrap_or(scan_end);

                    // Prepend any incomplete multibyte carry from previous chunk.
                    let to_decode: Vec<u8> = if carry.is_empty() {
                        self.buf[..chunk_end].to_vec()
                    } else {
                        let mut v = std::mem::take(&mut carry);
                        v.extend_from_slice(&self.buf[..chunk_end]);
                        v
                    };

                    let decoded = decode_chunk_with_carry(&to_decode, &mut carry);
                    let need = MAX_LINE_CHARS - decoded_char_count;
                    let added: String = decoded.chars().take(need).collect();
                    decoded_char_count += added.chars().count();
                    line.push_str(&added);
                    self.pos = chunk_end;

                    // If we hit a newline mid-scan, stop accumulating — let
                    // skip_to_next_line handle the terminator.
                    if nl_pos.is_some() {
                        break;
                    }
                }
                // Drain remaining processed bytes before skip.
                if self.pos > 0 {
                    self.buf.drain(..self.pos);
                    self.pos = 0;
                }
                // carry holds an incomplete multibyte seq — drop it (errors="ignore").
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
/// implementation.
///
/// Test-only: the eager reference oracle for the streaming
/// [`universal_lines`] differential tests; production paths consume the
/// iterator directly.
#[cfg(test)]
pub(crate) fn read_universal_lines(path: &Path) -> std::io::Result<Vec<String>> {
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
///
/// Test-only: a Python-parity mirror kept as a documented oracle with its own
/// tests; production blank-detection consolidated into the single-open
/// `probe_csv_header` (`probe.blank`).
#[cfg(test)]
pub(crate) fn is_blank(path: &Path) -> bool {
    let size = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(_) => return false,
    };
    if size >= 9_999_995 {
        return false;
    }
    let mut bytes = Vec::new();
    if File::open(path)
        .and_then(|mut f| f.read_to_end(&mut bytes))
        .is_err()
    {
        return false;
    }
    let body = if bytes.starts_with(BOM) {
        &bytes[BOM.len()..]
    } else {
        &bytes[..]
    };
    match std::str::from_utf8(body) {
        // `py_strip` matches Python's `str.strip()` whitespace set (incl. the
        // C0 separators), so an all-separator file reads as blank (issue #176).
        Ok(s) => py_strip(s).is_empty(),
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
    fn python_whitespace_includes_c0_separators() {
        // The C0 information separators (U+001C-001F) are whitespace to Python
        // but NOT to Rust's char::is_whitespace — the entire divergence set.
        for c in ['\u{1c}', '\u{1d}', '\u{1e}', '\u{1f}'] {
            assert!(is_python_whitespace(c), "{c:?} should be Python whitespace");
            assert!(
                !c.is_whitespace(),
                "{c:?} is not Rust White_Space (precondition)"
            );
        }
        // Ordinary whitespace (incl. \xa0 NBSP, already shared) is unchanged.
        for c in [' ', '\t', '\n', '\r', '\u{0c}', '\u{a0}'] {
            assert!(is_python_whitespace(c));
        }
        // '\u{1b}' (ESC, just below the separators) is whitespace in neither.
        assert!(!is_python_whitespace('\u{1b}'));
        assert!(!is_python_whitespace('a'));
        assert!(!is_python_whitespace('#'));
    }

    #[test]
    fn py_strip_trims_c0_separators_like_python() {
        // "\x1c# c".strip() == "# c"  =>  recognised as a comment after strip.
        assert_eq!(py_strip("\u{1c}# c"), "# c");
        assert_eq!(py_strip("\u{1c}\u{1d}name,age\u{1e}\u{1f}"), "name,age");
        // A line of only separators strips to empty (blank).
        assert_eq!(py_strip("\u{1c}\u{1d}\u{1e}\u{1f}"), "");
        // Mixed with ordinary whitespace.
        assert_eq!(py_strip(" \t\u{1f}x\u{1c} \n"), "x");
    }

    #[test]
    fn py_split_whitespace_splits_on_c0_separators_like_python() {
        // Python: "a\x1cb\x1cc".split() == ["a", "b", "c"].
        assert_eq!(
            py_split_whitespace("a\u{1c}b\u{1c}c").collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        // Mixed separators and ordinary whitespace collapse into one split.
        assert_eq!(
            py_split_whitespace("a\u{1d} \tb").collect::<Vec<_>>(),
            ["a", "b"]
        );
        // Leading/trailing separators produce no empty fields.
        assert_eq!(
            py_split_whitespace("\u{1f}a b\u{1c}").collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(py_split_whitespace("\u{1c}\u{1d}\u{1e}\u{1f}").count(), 0);
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
    fn unterminated_tail_decoding_to_nothing_is_not_a_line() {
        // #317. "Bytes remain" is a BYTE-level condition; a line is DECODED
        // text. A tail whose every byte is dropped by errors="ignore" must
        // yield no line, because CPython iterates the decoded string.
        let a = tmp(b"\x80", ".csv"); // whole file decodes to ""
        assert!(read_universal_lines(a.path()).unwrap().is_empty());
        let b = tmp(b"a\n\x80", ".csv"); // real line, then a vanishing tail
        assert_eq!(read_universal_lines(b.path()).unwrap(), vec!["a"]);
        let c = tmp(b"# c\n\xc3", ".csv"); // truncated multibyte after a comment
        assert_eq!(read_universal_lines(c.path()).unwrap(), vec!["# c"]);
        // BOM + invalid only. The BOM is consumed as an encoding marker (Python
        // reads with utf-8-sig), so nothing survives decoding and there is no
        // line — not a U+FEFF line.
        let d = tmp(b"\xef\xbb\xbf\xe9\xff\xfe", ".csv");
        assert!(read_universal_lines(d.path()).unwrap().is_empty());

        // Guard the boundary the fix must NOT cross: a tail that decodes to
        // real text still counts, and an explicitly terminated empty line is
        // emitted by the \n branch, not this one.
        let e = tmp(b"a\n\x80b", ".csv"); // invalid byte beside surviving text
        assert_eq!(read_universal_lines(e.path()).unwrap(), vec!["a", "b"]);
        let f = tmp(b"a\n   ", ".csv"); // whitespace-only tail is still text
        assert_eq!(read_universal_lines(f.path()).unwrap(), vec!["a", "   "]);
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
        assert_eq!(
            read_universal_lines(f.path()).unwrap(),
            vec!["Jos,NYC", "b"]
        );
    }

    #[test]
    fn streaming_matches_old_impl_for_mixed_newlines() {
        // Equality check: the streaming collect must reproduce the exact
        // line vector the previous whole-file implementation produced.
        let content = b"a\r\nb\rc\n\nd";
        let f = tmp(content, ".csv");
        let streamed = read_universal_lines(f.path()).unwrap();
        // Reference: old whole-file split implementation.
        let text = decode_ignore(content)
            .replace("\r\n", "\n")
            .replace('\r', "\n");
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
        assert!(!is_legacy_mac_newlines(
            tmp(b"a,b\r\nc,d\r\n", ".csv").path()
        ));
        assert!(!is_legacy_mac_newlines(tmp(b"a,b\nc,d\n", ".csv").path()));
        assert!(!is_legacy_mac_newlines(std::path::Path::new(
            "/nonexistent/x.csv"
        )));
    }

    #[test]
    fn file_probes() {
        assert!(is_empty(tmp(b"", ".csv").path()).unwrap());
        assert!(!is_empty(tmp(b"a", ".csv").path()).unwrap());
        assert!(is_blank(tmp(b"  \n\t\n ", ".csv").path()));
        assert!(!is_blank(tmp(b"a", ".csv").path()));
        assert!(!is_blank(tmp(b"\xff\xfe", ".csv").path())); // invalid UTF-8 = content
        assert!(is_blank(tmp(b"\xef\xbb\xbf \n", ".csv").path())); // BOM + whitespace
                                                                   // C0 separators are whitespace to Python's strip, so an all-separator
                                                                   // file is blank (issue #176) — matching BlankFile.is_blank.
        assert!(is_blank(tmp(b"\x1c\x1d\x1e\x1f\n", ".csv").path()));
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
        assert_eq!(
            lines.len(),
            2,
            "should yield the giant line and the next line"
        );
        assert_eq!(
            lines[0].chars().count(),
            MAX_LINE_CHARS,
            "giant line capped at MAX_LINE_CHARS"
        );
        assert_eq!(
            lines[1], "next_line",
            "second line must read correctly after cap"
        );
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

    /// Invalid bytes followed by 4-byte chars (😀): the cap branch must yield
    /// exactly MAX_LINE_CHARS decoded chars, not MAX_LINE_CHARS minus the number
    /// of dropped invalid bytes. Reproduces the raw-byte-budget parity bug.
    #[test]
    fn cap_invalid_bytes_then_4byte_chars_parity() {
        let four_byte: &[u8] = "😀".as_bytes(); // 4 bytes per char
        let n = MAX_LINE_CHARS + 50;
        let mut content = vec![0xFF_u8]; // 1 invalid byte (dropped on decode)
        for _ in 0..n {
            content.extend_from_slice(four_byte);
        }
        // No newline: cap + EOF path fires.
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 1, "should yield exactly one line");
        assert_eq!(
            lines[0].chars().count(),
            MAX_LINE_CHARS,
            "must yield exactly MAX_LINE_CHARS decoded chars (not MAX_LINE_CHARS - dropped_bytes)"
        );
    }

    /// All-invalid-byte input with no newline must terminate with bounded memory
    /// and yield an empty or very short result (all bytes dropped on decode).
    #[test]
    fn cap_all_invalid_bytes_no_newline_bounded_and_completes() {
        let content: Vec<u8> = vec![0xFF; MAX_LINE_READ_BYTES + 100];
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert!(
            lines.is_empty() || (lines.len() == 1 && lines[0].is_empty()),
            "all-invalid no-newline: expected 0 or 1 empty line, got {:?}",
            lines
        );
    }

    /// Invalid byte + 4-byte chars giant line followed by a normal line:
    /// after capping, the next line must still read correctly.
    #[test]
    fn cap_invalid_bytes_4byte_chars_then_next_line() {
        let four_byte: &[u8] = "😀".as_bytes();
        let n = MAX_LINE_CHARS + 50;
        let mut content = vec![0xFF_u8];
        for _ in 0..n {
            content.extend_from_slice(four_byte);
        }
        content.extend_from_slice(b"\nnext_line_ok\n");
        let f = tmp(&content, ".csv");
        let lines = read_universal_lines(f.path()).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].chars().count(), MAX_LINE_CHARS);
        assert_eq!(lines[1], "next_line_ok");
    }
}
