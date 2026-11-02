"""Check an image stream's decoded size against its declared size, in bounded chunks, before pdfium decodes it.

pdfium decodes an image's FlateDecode, LZWDecode, RunLengthDecode,
ASCII85Decode and ASCIIHexDecode stages whole, into memory, when it reads,
extracts or renders the image, whatever the image declares: a stream of a few
hundred kilobytes may decode to gigabytes. (MuPDF stops at the declared size.)

This module decodes those stages here first, from pdfium's raw copy of the
stream, chunk by chunk, holding no more than about one chunk of any stage at a
time and counting what each stage puts out, and stops as soon as a stage puts
out more than it may. pdfium gives an image's ``/Width`` and ``/Height``
without decoding it, but not its colour space, bits or ``/DecodeParms``, so
each of those is taken at its largest:

* the last decoding stage may put out ``/Width`` x ``/Height`` pixels of 8
  bytes (four components of 16 bits, the most a device, calibrated, Lab, ICC
  or indexed space holds), twice that for a predictor's tag bytes, plus a
  small slack;
* a stage that feeds an image codec (DCT, CCITT fax, JPEG 2000, JBIG2) may put
  out a multiple of that plus an allowance (a codec's data may be larger than
  its pixels: headers, metadata, poorly compressible scans);
* an earlier stage may put out a few times what the stage after it may;
* an LZW stage is read both ways ``/EarlyChange`` may be set, as pdfium reads it.

So the work is bounded by the declared size and the stream's length. The check
FAILS CLOSED: a stream it cannot decode for certain (corrupt Flate, ASCII85 or
ASCIIHex data, an unknown filter, a size pdfium cannot give) is reported as
unchecked, and the caller skips the image as it skips one that decodes past
its size.

Pure: no pdfium / pymupdf import.
"""

import base64
import itertools
import re
import zlib
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from typing import NamedTuple

from datagrunt.core.pdf_io.extraction.image_guard import normalize_filter

# The filters pdfium decodes whole, and the image codecs that may follow them.
SIMPLE_FILTERS = frozenset({"FlateDecode", "LZWDecode", "RunLengthDecode", "ASCII85Decode", "ASCIIHexDecode"})
CODEC_FILTERS = frozenset({"DCTDecode", "CCITTFaxDecode", "JPXDecode", "JBIG2Decode"})

# The most bytes a pixel takes: four components of 16 bits.
_PIXEL_BYTES = 8
# A predictor adds at most one tag byte to a row of at least one byte.
_PREDICTOR_FACTOR = 2
# Bytes a stream may decode to past its bound: an end-of-data marker, padding,
# or a trailing partial row some writers add.
_SLACK_BYTES = 1 << 16
# A stage feeding an image codec may put out this many times the raster bound,
# plus this allowance (a codec's headers, metadata segments, poor compression).
_CODEC_INPUT_FACTOR = 4
_CODEC_INPUT_ALLOWANCE = 4 << 20
# An earlier stage may put out this many times what the next stage may (no
# decoding filter needs more than about twice its output as input), plus the slack.
_STAGE_INPUT_FACTOR = 4
# Each stage reads and puts out at most about this many bytes at a time.
_CHUNK_BYTES = 1 << 16
# A check decodes at most this many bytes in all when its caller gives no budget.
_DEFAULT_BUDGET_BYTES = 2 << 30
# zlib calls one chunk of input may take (each puts out a chunk or consumes input).
_MAX_INFLATE_STEPS = 1 << 16
# LZW stages read both ways /EarlyChange may be set: 2 ** count readings.
_MAX_LZW_STAGES = 2
_EARLY_CHANGES = (1, 0)

_LZW_CLEAR = 256
_LZW_END = 257
_LZW_FIRST = 258
# pdfium adds table entries until it holds 4094 less /EarlyChange of them.
_LZW_MAX_ENTRIES = 4094
_LZW_MAX_WIDTH = 12
# The most prefix codes a string's walk follows (pdfium cuts a string at 4000 bytes).
_LZW_MAX_WALK = 1 << 16
_RL_END = 128
_RUN_ENDED = -1
_WHITESPACE = b"\x00\t\n\x0c\r "
_A85_ZEROS = re.compile(rb"z+")


class ByteBudget:
    """The bytes a check may still decode; a decode that would pass it is stopped."""

    def __init__(self, total: int) -> None:
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise ValueError(f"a byte budget must be a whole number of bytes, not {total!r}")
        self._remaining = total

    @property
    def remaining(self) -> int:
        """The bytes left to decode."""
        return self._remaining

    def charge(self, size: int) -> bool:
        """Spend ``size`` bytes; False (and nothing spent) when fewer than ``size`` remain."""
        if not isinstance(size, int) or size < 0:
            raise ValueError(f"a charge must be a whole number of bytes, not {size!r}")
        if size > self._remaining:
            return False
        self._remaining -= size
        return True


class _Stage(NamedTuple):
    """One decoding stage: its filter and the most bytes it may put out."""

    name: str
    limit: int


class _OverLimit(Exception):
    """A stage put out more than it may."""


class _OverBudget(Exception):
    """The decode would pass its byte budget."""


class _Undecodable(Exception):
    """The data cannot be decoded for certain."""


def has_simple_filter(filters: Iterable[object]) -> bool:
    """True when any of ``filters`` is one pdfium decodes whole (FlateDecode, LZWDecode, ...)."""
    return any(normalize_filter(str(name)) in SIMPLE_FILTERS for name in filters)


def stream_size_problem(
    raw: bytes | None,
    filters: Sequence[object],
    size: tuple[int, int] | None,
    budget: ByteBudget | None = None,
) -> str | None:
    """What is wrong with an image's stream for its declared size; None when nothing is.

    Args:
        raw: The image's raw (still-encoded) stream, or None if unreadable.
        filters: Its filter names in decoding order.
        size: Its ``(/Width, /Height)``, or None if unreadable.
        budget: The bytes the check may decode (charged as it decodes); a
            fresh :data:`_DEFAULT_BUDGET_BYTES` budget when None.

    A stream with no decoding filter is not checked (None). Otherwise the text
    says it decodes past its declared size, or that its size could not be
    checked (fail closed).
    """
    if not has_simple_filter(filters):
        return None
    if raw is None:
        return _unchecked("its stream could not be read")
    pixels = _pixel_size(size)
    if pixels is None:
        return _unchecked("its /Width and /Height could not be read")
    plan = _plan([normalize_filter(str(name)) for name in filters], pixels)
    if isinstance(plan, str):
        return _unchecked(plan)
    try:
        _decode_every_reading(raw, plan, budget if budget is not None else ByteBudget(_DEFAULT_BUDGET_BYTES))
    except _OverLimit:
        return "its stream decodes to more than its declared size; skipped (not decoded)"
    except _OverBudget:
        return "its stream's decoded size could not be checked within the byte budget; skipped (not decoded)"
    except _Undecodable as exc:
        return _unchecked(str(exc))
    return None


def _unchecked(why: str) -> str:
    return f"its stream's decoded size could not be checked ({why}); skipped (not decoded)"


def _pixel_size(size: object) -> tuple[int, int] | None:
    """``size`` when it is a ``(width, height)`` pair of positive integers; else None."""
    if not isinstance(size, tuple) or len(size) != 2:
        return None
    width, height = size
    for side in (width, height):
        if not isinstance(side, int) or isinstance(side, bool) or side <= 0:
            return None
    return width, height


# ---------------------------------------------------------------------------
# The plan: each stage's limit, from the declared size backwards.
# ---------------------------------------------------------------------------


def _plan(names: list[str], size: tuple[int, int]) -> list[_Stage] | str:
    """Each decoding stage with the most it may put out; or why the stream cannot be checked.

    pdfium decodes the leading run of decoding filters; the one filter after
    them, if any, must be the last and an image codec.
    """
    lead = _leading_simple(names)
    rest = names[lead:]
    if len(rest) > 1:
        return f"filter {rest[0]!r} is not its last filter"
    if rest and rest[0] not in CODEC_FILTERS:
        return f"unrecognised filter {rest[0]!r}"
    if names[:lead].count("LZWDecode") > _MAX_LZW_STAGES:
        return f"it has more than {_MAX_LZW_STAGES} LZWDecode filters"
    raster = size[0] * size[1] * _PIXEL_BYTES
    if rest:
        limit = _CODEC_INPUT_FACTOR * raster + _CODEC_INPUT_ALLOWANCE
    else:
        limit = _PREDICTOR_FACTOR * raster + _SLACK_BYTES
    stages: list[_Stage] = []
    for index in reversed(range(lead)):  # bounded by the filter count
        stages.append(_Stage(names[index], limit))
        limit = _STAGE_INPUT_FACTOR * limit + _SLACK_BYTES
    return stages[::-1]


def _leading_simple(names: list[str]) -> int:
    """How many of ``names`` lead with decoding filters."""
    for index, name in enumerate(names):
        if name not in SIMPLE_FILTERS:
            return index
    return len(names)


# ---------------------------------------------------------------------------
# The decode: a chain of chunked decoders, each counted against its limit.
# ---------------------------------------------------------------------------


def _decode_every_reading(raw: bytes, plan: list[_Stage], budget: ByteBudget) -> None:
    """Decode ``raw`` once for each way its LZW stages' ``/EarlyChange`` may be set (once if it has none)."""
    lzw_stages = sum(1 for stage in plan if stage.name == "LZWDecode")
    for early_changes in itertools.product(_EARLY_CHANGES, repeat=lzw_stages):  # at most 4 readings
        _decode(raw, plan, budget, early_changes)


def _decode(raw: bytes, plan: list[_Stage], budget: ByteBudget, early_changes: tuple[int, ...]) -> None:
    """Decode ``raw`` through every stage, counting each; raise when a stage passes its limit or the budget."""
    chunks: Iterator[bytes] = (raw[start : start + _CHUNK_BYTES] for start in range(0, len(raw), _CHUNK_BYTES))
    lzw_readings = iter(early_changes)
    for stage in plan:
        early_change = next(lzw_readings) if stage.name == "LZWDecode" else 1
        chunks = _counted(_whole(_DECODERS[stage.name], chunks, early_change), stage.limit, budget)
    for _ in chunks:  # drives every stage; each is bounded by its limit
        pass


def _whole(decoder: "_Decoder", chunks: Iterator[bytes], early_change: int) -> Iterator[bytes]:
    """The stage's output; then the rest of its input is read too, as pdfium decodes every earlier stage whole."""
    yield from decoder(chunks, early_change)
    for _ in chunks:
        pass


def _counted(chunks: Iterator[bytes], limit: int, budget: ByteBudget) -> Iterator[bytes]:
    """Pass ``chunks`` on, charging the budget; raise once more than ``limit`` bytes have passed."""
    total = 0
    for chunk in chunks:
        if not budget.charge(len(chunk)):
            raise _OverBudget
        total += len(chunk)
        if total > limit:
            raise _OverLimit
        yield chunk


def _batched(pieces: Iterable[bytes]) -> Iterator[bytes]:
    """Join small pieces into chunks of about ``_CHUNK_BYTES``."""
    held: list[bytes] = []
    size = 0
    for piece in pieces:
        held.append(piece)
        size += len(piece)
        if size >= _CHUNK_BYTES:
            yield b"".join(held)
            held, size = [], 0
    if held:
        yield b"".join(held)


def _zeros(count: int) -> Iterator[bytes]:
    """``count`` zero bytes, a chunk at a time."""
    for start in range(0, count, _CHUNK_BYTES):
        yield bytes(min(_CHUNK_BYTES, count - start))


def _inflate(chunks: Iterator[bytes], early_change: int) -> Iterator[bytes]:
    """FlateDecode, its output taken a chunk at a time (``max_length``) until the zlib stream ends."""
    inflater = zlib.decompressobj()
    for chunk in chunks:
        yield from _inflate_input(inflater, chunk)
        if inflater.eof:
            return
    yield from _inflate_input(inflater, b"")  # what zlib still holds


def _inflate_input(inflater: "zlib._Decompress", data: bytes) -> Iterator[bytes]:
    """Feed ``data`` to ``inflater`` and yield its output, at most a chunk a call."""
    for _ in range(_MAX_INFLATE_STEPS):
        try:
            out = inflater.decompress(data, _CHUNK_BYTES)
        except zlib.error as exc:
            raise _Undecodable("its FlateDecode data is corrupt") from exc
        data = inflater.unconsumed_tail
        if out:
            yield out
        if inflater.eof or (not out and not data):
            return
    raise _Undecodable("its FlateDecode data did not end within the step bound")


class _LzwDecoder:
    """LZWDecode as pdfium reads it: codes of 9 to 12 bits, most significant bit first.

    The table mirrors pdfium's: each entry is a prefix code and a byte, and a
    code past the table reads as the last string and its first byte, adding an
    entry whose prefix may only later be in the table (its string is then
    found by walking prefixes when it is used, as pdfium does). It stops where
    pdfium stops: the end-of-data code, a code past the byte codes with no
    string before it, or a code after a code past the table. Where pdfium cuts
    a string at its stack's length this counts it whole, so it never counts
    fewer bytes than pdfium decodes.
    """

    def __init__(self, early_change: int) -> None:
        self._early_change = 1 if early_change else 0
        self._bits = 0
        self._count = 0
        self._stopped = False
        self._reset()

    def _reset(self) -> None:
        self._prefixes: list[int] = []
        self._bytes: list[int] = []
        self._strings: list[bytes | None] = []  # an entry's string, or None while its prefix chain may change
        self._width = 9
        self._old: int | None = None
        self._last_byte = 0

    def entries(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        """Each code's bytes, until the end-of-data code, one of pdfium's stops, or the end of the data."""
        for chunk in chunks:
            for code in self._codes(chunk):
                entry = None if code == _LZW_END else self._entry(code)
                if entry is None:
                    return
                yield entry
                if self._stopped:
                    return

    def _codes(self, chunk: bytes) -> Iterator[int]:
        """The codes in ``chunk``, read at the width the table has when each is read (at most one a byte)."""
        for value in chunk:
            self._bits = (self._bits << 8) | value
            self._count += 8
            if self._count >= self._width:
                self._count -= self._width
                code = self._bits >> self._count
                self._bits &= (1 << self._count) - 1
                yield code

    def _entry(self, code: int) -> bytes | None:
        """The bytes of ``code``, adding the table entry it implies; None where pdfium stops before any."""
        if code < 256:
            if self._old is not None:
                self._add(self._old, code)
            self._old, self._last_byte = code, code
            return bytes((code,))
        if code == _LZW_CLEAR:
            self._reset()
            return b""
        old = self._old
        if old is None:
            return None
        if code - _LZW_FIRST >= len(self._strings):  # past the table: the last string and its first byte
            out = self._string(old) + bytes((self._last_byte,))
        else:
            out = self._string(code)
        self._last_byte = out[0]
        if old - _LZW_FIRST >= len(self._strings):  # the code before was past the table: pdfium stops here
            self._stopped = True
            return out
        self._add(old, self._last_byte)
        self._old = code
        return out

    def _string(self, code: int) -> bytes:
        """The string of ``code`` as the table holds it now."""
        index = code - _LZW_FIRST
        if not 0 <= index < len(self._strings):
            return bytes((code & 0xFF,))
        fixed = self._strings[index]
        return fixed if fixed is not None else self._walk(code)

    def _walk(self, code: int) -> bytes:
        """A string found by walking its prefix codes; one that loops is counted to the walk's bound."""
        tail = bytearray()
        for _ in range(_LZW_MAX_WALK):
            index = code - _LZW_FIRST
            if not 0 <= index < len(self._strings):
                return bytes((code & 0xFF,)) + bytes(reversed(tail))
            fixed = self._strings[index]
            if fixed is not None:
                return fixed + bytes(reversed(tail))
            tail.append(self._bytes[index])
            code = self._prefixes[index]
        return bytes(reversed(tail))

    def _add(self, prefix: int, value: int) -> None:
        """Add the entry ``prefix`` + ``value``, and widen the codes where pdfium does."""
        if len(self._strings) + self._early_change >= _LZW_MAX_ENTRIES:
            return
        if prefix < 256:
            base: bytes | None = bytes((prefix,))
        else:
            index = prefix - _LZW_FIRST
            base = self._strings[index] if 0 <= index < len(self._strings) else None
        self._prefixes.append(prefix)
        self._bytes.append(value)
        self._strings.append(None if base is None else base + bytes((value,)))
        if self._width < _LZW_MAX_WIDTH and len(self._strings) + _LZW_FIRST + self._early_change >= (1 << self._width):
            self._width += 1


def _lzw(chunks: Iterator[bytes], early_change: int) -> Iterator[bytes]:
    return _batched(_LzwDecoder(early_change).entries(chunks))


def _run_length(chunks: Iterator[bytes], early_change: int) -> Iterator[bytes]:
    return _batched(_run_length_pieces(chunks))


def _run_length_pieces(chunks: Iterator[bytes]) -> Iterator[bytes]:
    """RunLengthDecode's runs; a run cut short by the end of the data counts at its full length, as pdfium's does."""
    carry = b""
    for chunk in chunks:
        data = carry + chunk
        position = 0
        for _ in range(len(data)):  # each run consumes at least two bytes
            run = _run(data, position)
            if run is None:
                break
            piece, after = run
            if after == _RUN_ENDED:
                return
            yield piece
            position = after
        carry = data[position:]
    if carry and carry[0] != _RL_END:
        yield _short_run(carry)


def _run(data: bytes, position: int) -> tuple[bytes, int] | None:
    """The run at ``position`` and where the next starts (``_RUN_ENDED`` at end of data); None when not all here."""
    if position >= len(data):
        return None
    length = data[position]
    if length == _RL_END:
        return b"", _RUN_ENDED
    if length < _RL_END:
        end = position + length + 2
        return (data[position + 1 : end], end) if end <= len(data) else None
    if position + 2 > len(data):
        return None
    return data[position + 1 : position + 2] * (257 - length), position + 2


def _short_run(carry: bytes) -> bytes:
    """A run cut short by the end of the data, at the length its first byte gives."""
    length = carry[0]
    if length < _RL_END:
        return carry[1:].ljust(length + 1, b"\x00")
    return bytes(257 - length)


def _ascii_hex(chunks: Iterator[bytes], early_change: int) -> Iterator[bytes]:
    """ASCIIHexDecode: pairs of hex digits up to ``>``, white space ignored; an odd last digit is padded with 0."""
    odd = b""
    for chunk in chunks:
        end = chunk.find(b">")
        text = odd + (chunk if end < 0 else chunk[:end]).translate(None, _WHITESPACE)
        odd = text[-1:] if len(text) % 2 else b""
        yield _from_hex(text[: len(text) - len(odd)])
        if end >= 0:
            break
    if odd:
        yield _from_hex(odd + b"0")


def _from_hex(text: bytes) -> bytes:
    try:
        return bytes.fromhex(text.decode("ascii"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise _Undecodable("its ASCIIHexDecode data is corrupt") from exc


def _ascii85(chunks: Iterator[bytes], early_change: int) -> Iterator[bytes]:
    """ASCII85Decode: groups of five characters up to ``~``, ``z`` for four zero bytes, white space ignored."""
    carry = b""
    for chunk in chunks:
        end = chunk.find(b"~")
        text = carry + (chunk if end < 0 else chunk[:end]).translate(None, _WHITESPACE)
        carry = yield from _a85_text(text, final=end >= 0)
        if end >= 0:
            return
    yield from _a85_text(carry, final=True)


def _a85_text(text: bytes, final: bool) -> Generator[bytes, None, bytes]:
    """Decode ``text``'s whole groups and zero runs; return the part group left over (decoded too when ``final``)."""
    position = 0
    for zeros in _A85_ZEROS.finditer(text):  # bounded by the text's length
        groups = text[position : zeros.start()]
        if len(groups) % 5:
            raise _Undecodable("its ASCII85Decode data is corrupt")
        yield _from_a85(groups)
        yield from _zeros(4 * (zeros.end() - zeros.start()))
        position = zeros.end()
    rest = text[position:]
    split = len(rest) - len(rest) % 5
    yield _from_a85(rest[:split])
    if not final:
        return rest[split:]
    yield _from_a85(rest[split:])
    return b""


def _from_a85(text: bytes) -> bytes:
    try:
        return base64.a85decode(text)
    except ValueError as exc:
        raise _Undecodable("its ASCII85Decode data is corrupt") from exc


_Decoder = Callable[[Iterator[bytes], int], Iterator[bytes]]
_DECODERS: dict[str, _Decoder] = {
    "FlateDecode": _inflate,
    "LZWDecode": _lzw,
    "RunLengthDecode": _run_length,
    "ASCII85Decode": _ascii85,
    "ASCIIHexDecode": _ascii_hex,
}
