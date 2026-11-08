"""Bounded decoders for an image stream's simple filters: how long the stream decodes, within a limit.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.
:mod:`datagrunt.pdf._pdfium_images` uses it to check an image's stream
against its declared size before pdfium may decode the image in the
calling process.

pdfium decodes an image behind LZWDecode, ASCII85Decode or ASCIIHexDecode
(and behind a FlateDecode or RunLengthDecode before another filter) in full
to read it, whatever ``/Width`` and ``/Height`` say, holding each filter's
whole output. These decoders read each filter as pdfium does, so the length
they measure is the length pdfium decodes, and stop as soon as it passes
its limit. Agreement with pdfium is pinned by
``tests/pdf_tests/test_stream_decode.py``, which decodes the same streams
with ``FPDFImageObj_GetImageDataDecoded``:

- FlateDecode: zlib, ending at the stream's end (data after it is not
  read). Data zlib rejects cannot be checked.
- LZWDecode: pdfium's decoder, step for step: a code past the table
  decodes as the entry about to be added, one code decodes to at most 4000
  bytes (its decode stack), the table stops growing at 4094 entries less
  ``/EarlyChange``, and decoding stops at the end-of-data code, at a code
  past the table after another, or where pdfium fails (a code past the
  literals before any entry). pdfium reads a stream it fails to decode, or
  that decodes to nothing, as its raw bytes. ``/EarlyChange`` is in
  ``/DecodeParms``, which no pdfium API reads, so a stream is read both
  ways and its length is the longer: a stream can end early read one way
  and decode at length read the other.
- ASCII85Decode: ``z`` is four zero bytes, whitespace (space, tab, CR, LF)
  is skipped and ``~`` ends the data. Anything else (another byte, a ``z``
  inside a group, a group over ``2**32 - 1``, a final group of one
  character) cannot be checked: pdfium stops at some of these, and reads
  others its own way.
- ASCIIHexDecode: pdfium skips every byte that is not a hex digit and stops
  at ``>``; an odd final digit is padded with 0.
- RunLengthDecode: 128 ends the data; a run cut short by the end of the
  data still counts its full length (pdfium fills it with zeros).

A stream decodes one filter after another, each a chunk at a time: no
decoder holds more than one chunk (``_CHUNK``) of output, and the LZW table
(at most 4094 strings of at most 4000 bytes). The last filter's output may
be ``limit`` bytes at most; each filter before it may output up to
:func:`encoded_limit` bytes: its output is encoded data, which may be
somewhat longer than what it decodes to. A filter before another is read to
its end even after the next one stops, as pdfium decodes it in full. Each
piece decoded is passed to ``charge`` (the page's stream budget).
"""

import string
import zlib
from collections.abc import Callable, Iterable, Iterator, Sequence
from types import MappingProxyType
from typing import Protocol

from datagrunt.pdf._compute import backend as compute_backend
from datagrunt.pdf._validate import int_at_least

# The most bytes a decoder hands on at once (and reads at once from the raw stream).
_CHUNK = 1 << 16

# An earlier filter's output (encoded data for the next filter) may be this many times the limit, and this much more.
_ENCODED_GROWTH = 2
_ENCODED_SLACK = 4096

# zlib's most output per input byte (deflate's 258-byte match from about 2 bits): one input chunk's passes.
_FLATE_PASSES = 1032 + 2

# pdfium's LZW decoder: its decode stack (the most bytes one code decodes to) and table size.
_LZW_STACK = 4000
_LZW_TABLE = 4094
_LZW = "LZWDecode"
# The /EarlyChange values a stream is read with: pdfium's default (1) first.
_EARLY_CHANGES = (1, 0)
# The table sizes (plus /EarlyChange) at which pdfium's codes grow to 10, 11 and 12 bits.
_LZW_WIDTHS = MappingProxyType({254: 10, 766: 11, 1790: 12})

_ASCII85_DIGITS = bytes(range(ord("!"), ord("u") + 1))
# Each byte's digit value (``!`` is 0): only digits reach it, so the others' wrapped values are never read.
_ASCII85_VALUES = bytes((value - ord("!")) % 256 for value in range(256))
# A group's digits' place values, its first digit the most significant.
_ASCII85_WEIGHTS = (85**4, 85**3, 85**2, 85, 1)
_ASCII85_SPACE = b" \t\r\n"
_HEX_DIGITS = string.hexdigits.encode()
_NOT_HEX = bytes(value for value in range(256) if value not in _HEX_DIGITS)


class OverLimit(ValueError):
    """A filter's output passes its limit: pdfium would decode more than the image declares."""


class Unchecked(ValueError):
    """The stream cannot be checked: a filter the check does not decode, or data outside its filter's format."""


class BudgetSpent(Exception):
    """A charge did not fit: the page's stream budget ran out during the check."""


class _Decoder(Protocol):
    """One filter's decoder: ``feed`` it the input in order, then ``finish``; ``done`` once it read its end."""

    done: bool

    def feed(self, data: bytes) -> Iterator[bytes]: ...

    def finish(self) -> Iterator[bytes]: ...


def encoded_limit(limit: int) -> int:
    """The most bytes a filter before the last may output, when the last may output ``limit``."""
    return _ENCODED_GROWTH * limit + _ENCODED_SLACK


def decoded_length(raw: bytes, filters: Sequence[str], *, limit: int, charge: Callable[[int], bool]) -> int:
    """The length ``raw`` decodes to through ``filters``, in decoding order, read within ``limit``.

    An LZWDecode stream is read with each ``/EarlyChange`` (see the module
    notes): its length is the longer reading's.

    Raises:
        OverLimit: If a filter's output passes its limit (the last's
            ``limit``, an earlier one's :func:`encoded_limit`).
        Unchecked: If a filter is not one the check decodes, more than one
            filter is LZWDecode, or a filter's data is outside its format.
        BudgetSpent: If ``charge`` returns False.
        TypeError, ValueError: If an argument is invalid.
    """
    _check(raw, filters, limit)
    readings = sum(1 for name in filters if name == _LZW)
    if readings > 1:
        raise Unchecked(f"it has {readings} LZWDecode filters; the check decodes one")
    earlies = _EARLY_CHANGES if readings else _EARLY_CHANGES[:1]
    lengths = (_total(decoded_pieces(raw, filters, early=early, limit=limit, charge=charge)) for early in earlies)
    return max(lengths)


def decoded_pieces(
    raw: bytes, filters: Sequence[str], *, early: int, limit: int, charge: Callable[[int], bool]
) -> Iterator[bytes]:
    """The last filter's output, a piece at a time, each filter's output charged and bounded as it is decoded.

    ``early`` is the ``/EarlyChange`` an LZWDecode filter is read with (0 or
    1). Raises as :func:`decoded_length` does, the decoding errors as the
    pieces are read.
    """
    _check(raw, filters, limit)
    if early not in _EARLY_CHANGES:
        raise ValueError(f"early must be 0 or 1, got {early!r}")
    pieces: Iterator[bytes] = _chunks(raw)
    last = len(filters) - 1
    for index, name in enumerate(filters):
        bound = limit if index == last else encoded_limit(limit)
        pieces = _limited(_decoded(_decoder(name, early, bound), pieces), name, bound, charge)
    return pieces


def _check(raw: object, filters: object, limit: object) -> None:
    """Reject arguments the decoders cannot take."""
    if not isinstance(raw, bytes):
        raise TypeError(f"raw must be bytes, got {type(raw).__name__}")
    if isinstance(filters, str) or not isinstance(filters, Sequence):
        raise TypeError(f"filters must be a sequence of filter names, got {type(filters).__name__}")
    if not filters:
        raise ValueError("filters must name at least one filter")
    if not all(isinstance(name, str) for name in filters):
        raise TypeError("filters must hold only str names")
    int_at_least("limit", limit, 1)


def _total(pieces: Iterable[bytes]) -> int:
    """How many bytes ``pieces`` hold, counted as they come."""
    return sum(len(piece) for piece in pieces)


def _chunks(raw: bytes) -> Iterator[bytes]:
    """``raw`` in pieces of ``_CHUNK`` bytes."""
    view = memoryview(raw)
    for start in range(0, len(raw), _CHUNK):
        yield bytes(view[start : start + _CHUNK])


def _decoder(name: str, early: int, bound: int = _CHUNK) -> _Decoder:
    """A fresh decoder for filter ``name``."""
    makers: dict[str, Callable[[], _Decoder]] = {
        "FlateDecode": _Flate,
        _LZW: lambda: _Lzw(early, bound),
        "ASCII85Decode": _Ascii85,
        "ASCIIHexDecode": _AsciiHex,
        "RunLengthDecode": _RunLength,
    }
    if name not in makers:
        raise Unchecked(f"filter {name!r} is not one the check decodes")
    return makers[name]()


def _decoded(decoder: _Decoder, source: Iterator[bytes]) -> Iterator[bytes]:
    """``decoder``'s output for ``source``; ``source`` is read to its end even once the decoder is done."""
    for data in source:  # bounded: the source's pieces
        if not decoder.done:
            yield from decoder.feed(data)
    yield from decoder.finish()


def _limited(pieces: Iterator[bytes], name: str, bound: int, charge: Callable[[int], bool]) -> Iterator[bytes]:
    """``pieces``, each charged, until their total passes ``bound``."""
    total = 0
    for piece in pieces:  # bounded: stops past ``bound``
        if not piece:
            continue
        if not charge(len(piece)):
            raise BudgetSpent(f"the page's stream budget ran out decoding {name}")
        total += len(piece)
        if total > bound:
            raise OverLimit(f"{name} decodes past {bound} bytes")
        yield piece


def _full_pieces(out: bytearray) -> Iterator[bytes]:
    """Take each whole ``_CHUNK``-byte piece off the front of ``out``."""
    for _ in range(len(out) // _CHUNK):
        piece = bytes(out[:_CHUNK])
        del out[:_CHUNK]
        yield piece


def _all_pieces(out: bytearray) -> Iterator[bytes]:
    """Take everything off ``out``, in pieces of at most ``_CHUNK`` bytes."""
    yield from _full_pieces(out)
    if out:
        yield bytes(out)
        out.clear()


class _Flate:
    """FlateDecode: zlib, ``_CHUNK`` bytes of output at a time, ending at the stream's end."""

    def __init__(self) -> None:
        self.done = False
        self._inflater = zlib.decompressobj()

    def feed(self, data: bytes) -> Iterator[bytes]:
        pending = data
        for _ in range(_FLATE_PASSES):  # bounded: zlib outputs at most 1032 bytes an input byte
            try:
                piece = self._inflater.decompress(pending, _CHUNK)
            except zlib.error as exc:
                raise Unchecked(f"FlateDecode data cannot be inflated ({exc})") from exc
            yield piece
            pending = self._inflater.unconsumed_tail
            if self._inflater.eof:
                self.done = True
                return
            if not pending and len(piece) < _CHUNK:
                return
        raise Unchecked(f"FlateDecode data inflates more than {_FLATE_PASSES} chunks from one")

    def finish(self) -> Iterator[bytes]:
        # A stream cut short counts what it inflated: pdfium inflates no more of it.
        yield from ()


class _Lzw:
    """LZWDecode, read as pdfium reads it (see the module notes), with one ``/EarlyChange``."""

    def __init__(self, early: int, bound: int = _CHUNK) -> None:
        self.done = False
        self._early = early
        self._bound = bound
        self._in = bytearray()

    def feed(self, data: bytes) -> Iterator[bytes]:
        if self.done:
            return
        self._in += data
        if len(self._in) >= _CHUNK:
            pieces = compute_backend().pdf_lzw_decode(bytes(self._in), self._early, self._bound)
            if sum(len(piece) for piece in pieces) > self._bound:
                self.done = True
                self._in.clear()
                yield from pieces

    def finish(self) -> Iterator[bytes]:
        if self._in and not self.done:
            pieces = compute_backend().pdf_lzw_decode(bytes(self._in), self._early, self._bound)
            self._in.clear()
            self.done = True
            yield from pieces


class _Ascii85:
    """ASCII85Decode, strictly: whitespace skipped, ``z`` four zero bytes, ``~`` the end (see the module notes)."""

    def __init__(self) -> None:
        self.done = False
        self._carry = b""
        self._out = bytearray()

    def feed(self, data: bytes) -> Iterator[bytes]:
        end = data.find(b"~")
        if end >= 0:
            data, self.done = data[:end], True
        stray = data.translate(None, _ASCII85_DIGITS + b"z" + _ASCII85_SPACE)
        if stray:
            raise Unchecked(f"ASCII85Decode data holds byte 0x{stray[0]:02x}")
        parts = (self._carry + data.translate(None, _ASCII85_SPACE)).split(b"z")
        for part in parts[:-1]:  # bounded: one part per z
            if len(part) % 5:
                raise Unchecked("ASCII85Decode data holds a z inside a group")
            self._out += _groups(part) + bytes(4)
            yield from _full_pieces(self._out)
        whole = len(parts[-1]) - len(parts[-1]) % 5
        self._out += _groups(parts[-1][:whole])
        self._carry = parts[-1][whole:]
        yield from _full_pieces(self._out)

    def finish(self) -> Iterator[bytes]:
        if len(self._carry) == 1:
            raise Unchecked("ASCII85Decode data ends in a final group of one character")
        if self._carry:
            size = len(self._carry) - 1
            self._out += _groups(self._carry + b"u" * (5 - len(self._carry)))[:size]
        yield from _all_pieces(self._out)


def _groups(text: bytes) -> bytes:
    """Whole 5-character ASCII85 groups (no ``z``) decoded, all at once.

    ``base64.a85decode`` reads a character at a time in Python. Here each
    group is a 64-bit lane of one big integer: the groups' k-th digits are
    laid in the low byte of their lanes and summed in, each times its power
    of 85, so a lane ends holding its group's value (at most ``85**5 - 1``,
    under 2**33: no lane carries into the next). A group's 4 bytes are the
    low half of its lane; a group over ``2**32 - 1`` has a bit in the high
    half.
    """
    count, odd = divmod(len(text), 5)
    if odd or text.translate(None, _ASCII85_DIGITS):
        raise Unchecked("ASCII85Decode data holds a partial group or a byte outside the groups")
    if not count:
        return b""
    digits = text.translate(_ASCII85_VALUES)
    lanes = bytearray(8 * count)
    total = 0
    for place, weight in enumerate(_ASCII85_WEIGHTS):  # bounded: the 5 digits of a group
        lanes[7::8] = digits[place::5]
        total += int.from_bytes(lanes, "big") * weight
    wide = total.to_bytes(8 * count, "big")
    if wide[3::8].count(0) != count:
        raise Unchecked("ASCII85Decode data holds a group over 2**32 - 1")
    out = bytearray(4 * count)
    for offset in range(4):  # bounded: the 4 bytes of a group
        out[offset::4] = wide[4 + offset :: 8]
    return bytes(out)


class _AsciiHex:
    """ASCIIHexDecode as pdfium reads it: hex digits, anything else skipped, ``>`` the end."""

    def __init__(self) -> None:
        self.done = False
        self._carry = b""

    def feed(self, data: bytes) -> Iterator[bytes]:
        end = data.find(b">")
        if end >= 0:
            data, self.done = data[:end], True
        digits = self._carry + data.translate(None, _NOT_HEX)
        whole = len(digits) - len(digits) % 2
        self._carry = digits[whole:]
        yield bytes.fromhex(digits[:whole].decode("ascii"))

    def finish(self) -> Iterator[bytes]:
        if self._carry:
            yield bytes.fromhex((self._carry + b"0").decode("ascii"))


class _RunLength:
    """RunLengthDecode as pdfium reads it: 128 the end, a run cut short counted whole."""

    def __init__(self) -> None:
        self.done = False
        self._literal = 0  # bytes of a literal run still to copy
        self._repeat = 0  # copies of the next byte still owed
        self._out = bytearray()

    def feed(self, data: bytes) -> Iterator[bytes]:
        at = 0
        for _ in range(len(data)):  # bounded: each pass reads at least one byte
            if at >= len(data) or self.done:
                break
            at = self._step(data, at)
            yield from _full_pieces(self._out)

    def _step(self, data: bytes, at: int) -> int:
        """Read from ``data[at]`` on: a literal's bytes, a repeat's byte or a length byte; where reading stopped."""
        if self._literal:
            taken = data[at : at + self._literal]
            self._out += taken
            self._literal -= len(taken)
            return at + len(taken)
        if self._repeat:
            self._out += data[at : at + 1] * self._repeat
            self._repeat = 0
            return at + 1
        length = data[at]
        if length == 128:
            self.done = True
        elif length < 128:
            self._literal = length + 1
        else:
            self._repeat = 257 - length
        return at + 1

    def finish(self) -> Iterator[bytes]:
        self._out += bytes(self._literal + self._repeat)
        self._literal = self._repeat = 0
        yield from _all_pieces(self._out)
