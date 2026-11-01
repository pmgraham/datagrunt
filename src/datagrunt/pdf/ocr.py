"""Page normalization, bands and the tesseract CLI for the v5 vision route (#369, A5a).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Its input is
our own render (:class:`~datagrunt.pdf.render.RenderedPage`): nothing here
decodes PDF content.

:func:`normalized` is the spec's grayscale + 1 % autocontrast (spec §3.3
step 4): Pillow's ``ImageOps.autocontrast(cutoff=1)``, whose histogram runs
in C. It ignores the darkest and the lightest 1 % of pixels and stretches
the grey levels between them to 0..255; a page with nothing left to stretch
(one grey level, or black to white already) comes back unchanged.

:func:`bands` cuts a page into horizontal :class:`Band` s for OCR (spec
§3.3 step 4): 900 px tall, stepping by 840 px, so neighbours share 60 rows
and a line of text up to 60 px tall (14.4 pt at 300 DPI) is whole in one of
them. The last band ends at the page's bottom row, so it may be shorter
than 900 px; every row is in one band or two.

The tesseract CLI (never ``pytesseract``) reads the page:

- :func:`orientation` runs tesseract's orientation detection (``--psm 0``)
  and reads its ``Rotate``: the clockwise turn, 0, 90, 180 or 270 degrees,
  that rights the page. :func:`rotated` makes that turn.
- :func:`ocr_words` reads the righted page's bands (``--psm 3``, TSV) and
  maps each word's box back to the displayed page in points: through its
  band's offset, ``72 / dpi`` and the inverse of the turn. A word read in
  both bands of a shared strip (the same text, boxes overlapping by an IoU
  of 0.5 or more) is kept once, with its higher confidence.
- A line crossing a band's cut edge (the upper band's bottom row, the
  lower band's top row) is read whole by the other band and, cut, as other
  text by its own ("Initiate" over "LILILIdAlC"). Such a fragment is
  OCR's artefact of the cut, not the page's text: a word lying wholly in
  the shared strip within 1 px of its band's cut edge is dropped when a
  word the other band reads whole (farther from its own cut edge)
  overlaps it, before copies are matched, so a whole copy is kept over a
  cut one. A cut word nothing whole overlaps was read once and is kept, as
  is one reaching past the strip. On A5b's 10 sample pages (#416) this
  drops 210 of 3,893 band words; the words' recall of the digital text
  goes from 84.5 % to 84.4 % (one "f"), their precision from 87.9 % to
  93.9 %.

``--psm 3`` (tesseract's automatic page segmentation) is chosen over
``--psm 6`` (one uniform block) by A5b's evidence: forced to OCR, 31 corpus
pages (the 19 datasheets and 12 rulebook pages) read 93.0 % of their
digital words with it, at a precision of 88.0 %, against 91.8 % and 83.1 %
with ``--psm 6``, which reads a multi-column page's lines across its
columns, and art as words. Its cost: on two art-heavy covers it skipped
short text among the art (a contents column's page numbers) that
``--psm 6`` read.

Each call starts ``tesseract`` from an argv list (never a shell), by the
absolute path :func:`~datagrunt.pdf._executables.trusted_executable` finds
in an absolute PATH directory (never the working directory), in its own
session, with ``OMP_THREAD_LIMIT=1`` in its environment, and writes the
image to its stdin as a binary PGM (no temporary image file). Its stdout is
read within :data:`_MAX_STDOUT_BYTES`; its stderr goes to an anonymous
temporary file, of which the last 4 KiB is read. At its timeout a timer
kills it, with everything in its session (a wrapper script's children
too; on Windows only the process itself), and it is reaped within a
bounded wait. tesseract missing, failing, timing out or overflowing its
bound is a status with its reason (:class:`Orientation`,
:class:`OCRResult`), never an empty success and never an exception.
"""

import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass, field
from typing import IO, Literal, NamedTuple

from PIL import Image, ImageOps

from datagrunt.pdf._executables import trusted_executable
from datagrunt.pdf._process import REAP_TIMEOUT_S, close_pipes, reaped, stderr_tail
from datagrunt.pdf._validate import (
    choice_str,
    finite_float,
    finite_tuple,
    int_at_least,
    non_blank_str,
    positive_float,
    typed_tuple,
)
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.render import RenderedPage, render_size

# The spec's autocontrast: the percent of pixels ignored at each end of the histogram.
_AUTOCONTRAST_CUTOFF = 1

# The spec's bands: 900 px tall, neighbours sharing 60 rows.
_BAND_HEIGHT = 900
_BAND_OVERLAP = 60

_TURNS = (0, 90, 180, 270)
_TURN_NAMES = "0, 90, 180 or 270"

OCRStatus = Literal["ok", "unavailable", "failed", "timeout"]
OrientationStatus = Literal["ok", "undetermined", "unavailable", "failed", "timeout"]
_OCR_STATUSES: tuple[OCRStatus, ...] = ("ok", "unavailable", "failed", "timeout")
_ORIENTATION_STATUSES: tuple[OrientationStatus, ...] = ("ok", "undetermined", "unavailable", "failed", "timeout")

# tesseract's TSV, one row a box: words are level 5, with a confidence of 0..100 (-1 on other levels).
_TSV_HEADER = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext"
_TSV_FIELDS = 12
_TSV_LINE = ("block_num", "par_num", "line_num")  # fields 2 to 4
_TSV_BOX = ("left", "top", "width", "height")  # fields 6 to 9
_WORD_LEVEL = "5"
_SAME_WORD_IOU = 0.5
# A word within this many rows of its band's cut edge (the shared strip's) is cut by it (#416).
_CUT_EDGE_PX = 1

_ROTATE = re.compile(r"^Rotate: *(\S+) *$", re.MULTILINE)
_CONFIDENCE = re.compile(r"^Orientation confidence: *(\S+) *$", re.MULTILINE)
_TOO_FEW_CHARACTERS = "Too few characters"  # tesseract's OSD on a page without enough text
# One tesseract language, or several joined by "+" (deu+eng): never an option.
_LANGUAGE = re.compile(r"[A-Za-z0-9_]+(?:\+[A-Za-z0-9_]+)*")

# A band's TSV is ~40 bytes a word: a 900 x 2550 px band of noise reads as ~21 KB, a 900 x 6000 one ~48 KB.
_MAX_STDOUT_BYTES = 16 << 20
_READ_CHUNK = 1 << 16
_VERSION_TIMEOUT_S = 10.0
_NOT_ON_PATH = "no tesseract in an absolute PATH directory"
# POSIX: each call is its own session, so a timeout ends a wrapper script's children too.
_OWN_SESSION = sys.platform != "win32"


@dataclass(frozen=True, slots=True, kw_only=True)
class Band:
    """Rows ``top_px`` to ``top_px + px_height`` of a page, across its whole width.

    Attributes:
        top_px: The band's first row on the page it was cut from.
        pixels: ``px_width * px_height`` bytes, 8-bit gray, row by row as on
            the page. Not in repr.
        px_width: The page's width in pixels.
        px_height: The band's rows.
    """

    top_px: int
    pixels: bytes = field(repr=False)
    px_width: int
    px_height: int

    def __post_init__(self) -> None:
        int_at_least("top_px", self.top_px, 0)
        int_at_least("px_width", self.px_width, 1)
        int_at_least("px_height", self.px_height, 1)
        if not isinstance(self.pixels, bytes):
            raise TypeError(f"pixels must be bytes, got {type(self.pixels).__name__}")
        expected = self.px_width * self.px_height
        if len(self.pixels) != expected:
            message = f"pixels must hold {self.px_width} x {self.px_height} = {expected} bytes, got {len(self.pixels)}"
            raise ValueError(message)


def normalized(page: RenderedPage) -> RenderedPage:
    """``page`` with its grey levels stretched by a 1 % autocontrast (the spec's normalization).

    Raises:
        TypeError: If ``page`` is not a :class:`~datagrunt.pdf.render.RenderedPage`.
        ValueError: If ``page`` is not 8-bit gray (a BGR render).
    """
    image = ImageOps.autocontrast(_image(page), cutoff=_AUTOCONTRAST_CUTOFF)
    return RenderedPage(pixels=image.tobytes(), px_width=page.px_width, px_height=page.px_height)


def _image(page: RenderedPage) -> Image.Image:
    """``page`` as a Pillow 8-bit grey (``L``) image."""
    _checked_page(page)
    return Image.frombytes("L", (page.px_width, page.px_height), page.pixels)


def bands(page: RenderedPage, *, height: int = _BAND_HEIGHT, overlap: int = _BAND_OVERLAP) -> tuple[Band, ...]:
    """``page`` cut into bands ``height`` rows tall whose neighbours share ``overlap`` rows.

    Bands start every ``height - overlap`` rows from the top; the last is the
    first to reach the bottom, and ends there (it may be shorter). A page up
    to ``height`` rows tall is one band. Every row is in one band or two, and
    there are at most as many bands as rows.

    Raises:
        TypeError: If ``page`` is not a :class:`~datagrunt.pdf.render.RenderedPage`,
            or ``height`` or ``overlap`` is not an int (bool rejected).
        ValueError: If ``page`` is not 8-bit gray (a BGR render), ``height``
            or ``overlap`` is under 1, or ``overlap`` is over half of
            ``height`` (a row would be in three bands).
    """
    _checked_page(page)
    int_at_least("height", height, 1)
    int_at_least("overlap", overlap, 1)
    if 2 * overlap > height:
        raise ValueError(f"overlap ({overlap}) must be at most half of height ({height})")
    step = height - overlap
    # The last band is the first whose top is at or past this row: it reaches the bottom.
    last_top = max(page.px_height - height, 0)
    return tuple(_band(page, top, min(height, page.px_height - top)) for top in range(0, last_top + step, step))


def _band(page: RenderedPage, top: int, rows: int) -> Band:
    """Rows ``top`` to ``top + rows`` of ``page``: one contiguous slice of its bytes."""
    start = top * page.px_width
    return Band(
        top_px=top, pixels=page.pixels[start : start + rows * page.px_width], px_width=page.px_width, px_height=rows
    )


def _checked_page(page: RenderedPage) -> RenderedPage:
    """``page``, if it is a RenderedPage in 8-bit gray (a BGR render is never read as gray)."""
    if not isinstance(page, RenderedPage):
        raise TypeError(f"page must be a RenderedPage, got {type(page).__name__}")
    if page.channels != 1:
        raise ValueError(f"page must be 8-bit gray (1 channel), got {page.channels} channels")
    return page


@dataclass(frozen=True, slots=True, kw_only=True)
class Orientation:
    """What tesseract's orientation detection found on a page.

    Attributes:
        degrees: The clockwise turn, 0, 90, 180 or 270, that rights the page
            (tesseract's ``Rotate``): pass it to :func:`rotated`. 0 when not ok.
        confidence: tesseract's orientation confidence when ok, else None.
        status: ``"ok"``; ``"undetermined"`` (too little text to tell);
            ``"unavailable"`` (no tesseract to run); ``"failed"``; ``"timeout"``.
        reason: Why the status is not ok; empty when ok.
    """

    degrees: int
    confidence: float | None
    status: OrientationStatus
    reason: str

    def __post_init__(self) -> None:
        _checked_turn("degrees", self.degrees)
        if self.confidence is not None:
            object.__setattr__(self, "confidence", finite_float("confidence", self.confidence))
        choice_str("status", self.status, _ORIENTATION_STATUSES)
        if self.status == "ok":
            if self.confidence is None:
                raise ValueError("an ok orientation has a confidence, got None")
            if self.reason != "":
                raise ValueError(f"an ok orientation has no reason, got {self.reason!r}")
            return
        if self.confidence is not None:
            raise ValueError(f"an orientation that is not ok has no confidence, got {self.confidence!r}")
        if self.degrees != 0:
            raise ValueError(f"an orientation that is not ok is 0 degrees, got {self.degrees}")
        non_blank_str("reason", self.reason)


@dataclass(frozen=True, slots=True, kw_only=True)
class OCRWord:
    """A word tesseract read, where it sits on the displayed page.

    Attributes:
        text: The word, as tesseract read it (punctuation attached).
        bbox: Its box on the displayed page, in points from the top-left corner.
        confidence: tesseract's confidence, 0 to 100.
        line: The tesseract line it was read on, numbered from 0 across the
            page in the order read: words read on one line share it (#416).
    """

    text: str
    bbox: BBox
    confidence: float
    line: int

    def __post_init__(self) -> None:
        non_blank_str("text", self.text)
        if not isinstance(self.bbox, BBox):
            raise TypeError(f"bbox must be a BBox, got {type(self.bbox).__name__}")
        object.__setattr__(self, "confidence", finite_float("confidence", self.confidence, lo=0.0, hi=100.0))
        int_at_least("line", self.line, 0)


@dataclass(frozen=True, slots=True, kw_only=True)
class OCRResult:
    """The words :func:`ocr_words` read on a page, and how the reading went.

    Attributes:
        words: The words, band by band, each band's in tesseract's reading order.
        status: ``"ok"`` (every band read); ``"unavailable"`` (no tesseract to
            run); ``"failed"`` or ``"timeout"`` (a band's call: ``words`` then
            holds the bands read before it).
        reason: Why the status is not ok, naming the band; empty when ok.
    """

    words: tuple[OCRWord, ...]
    status: OCRStatus
    reason: str

    def __post_init__(self) -> None:
        typed_tuple("words", self.words, OCRWord)
        choice_str("status", self.status, _OCR_STATUSES)
        if self.status != "ok":
            non_blank_str("reason", self.reason)
        elif self.reason != "":
            raise ValueError(f"an ok result has no reason, got {self.reason!r}")


def rotated(page: RenderedPage, degrees: int) -> RenderedPage:
    """``page`` turned clockwise by ``degrees`` (0, 90, 180 or 270); 0 returns it as it is.

    Raises:
        TypeError: If ``page`` is not a RenderedPage, or ``degrees`` is not an int (bool rejected).
        ValueError: If ``page`` is not 8-bit gray (a BGR render), or ``degrees`` is not 0, 90, 180 or 270.
    """
    image = _image(page)
    turn = _checked_turn("degrees", degrees)
    if turn == 0:
        return page
    turned = image.transpose(_transpose(turn))
    return RenderedPage(pixels=turned.tobytes(), px_width=turned.width, px_height=turned.height)


def _transpose(degrees: int) -> Image.Transpose:
    """Pillow's transpose for a clockwise turn of ``degrees`` (Pillow's ROTATE_* turn counter-clockwise)."""
    if degrees == 90:
        return Image.Transpose.ROTATE_270
    if degrees == 180:
        return Image.Transpose.ROTATE_180
    return Image.Transpose.ROTATE_90


def _checked_turn(name: str, value: object) -> int:
    """``value`` if it is 0, 90, 180 or 270 (an int; bool rejected)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {value!r}")
    if value not in _TURNS:
        raise ValueError(f"{name} must be {_TURN_NAMES}, got {value}")
    return value


def tesseract_available() -> bool:
    """Whether a ``tesseract`` in an absolute PATH directory answers ``--version`` as tesseract within a timeout."""
    binary = trusted_executable("tesseract")
    if binary is None:
        return False
    call = _call([binary, "--version"], (), _VERSION_TIMEOUT_S)
    return call.status == "ok" and call.stdout.startswith(b"tesseract ")


def orientation(page: RenderedPage, *, timeout_s: float) -> Orientation:
    """The clockwise turn that rights ``page``, by tesseract's orientation detection (``--psm 0``).

    A page with too little text for tesseract to tell is ``"undetermined"``;
    no tesseract, a failure or a timeout has its status and reason. None
    raises: each is 0 degrees with no confidence.

    Raises:
        TypeError, ValueError: If ``page`` is not a RenderedPage in 8-bit
            gray, or ``timeout_s`` is not a finite number over 0 (before
            anything runs).
    """
    _checked_page(page)
    positive_float("timeout_s", timeout_s)
    binary = trusted_executable("tesseract")
    if binary is None:
        return _unoriented("unavailable", _NOT_ON_PATH)
    call = _call([binary, "stdin", "stdout", "--psm", "0"], _pgm(page), timeout_s)
    if call.status == "failed" and _TOO_FEW_CHARACTERS in call.stderr:
        return _unoriented("undetermined", "tesseract's OSD found too few characters to orient the page")
    if call.status != "ok":
        return _unoriented(call.status, call.reason)
    return _osd(call.stdout)


def _unoriented(status: OrientationStatus, reason: str) -> Orientation:
    """No orientation: 0 degrees, no confidence, and why."""
    return Orientation(degrees=0, confidence=None, status=status, reason=reason)


def _osd(stdout: bytes) -> Orientation:
    """The orientation in tesseract's OSD report; a report that does not parse is a failure."""
    report = stdout.decode("utf-8", errors="replace")
    rotate = _ROTATE.search(report)
    if rotate is None:
        return _unoriented("failed", f"tesseract's OSD report has no Rotate line: {stdout[:200]!r}")
    confidence = _CONFIDENCE.search(report)
    if confidence is None:
        return _unoriented("failed", f"tesseract's OSD report has no Orientation confidence line: {stdout[:200]!r}")
    if rotate.group(1) not in {str(turn) for turn in _TURNS}:
        return _unoriented("failed", f"tesseract's OSD gave Rotate: {rotate.group(1)}, not {_TURN_NAMES}")
    try:
        value = float(confidence.group(1))
    except ValueError:
        value = math.nan
    if not math.isfinite(value):
        return _unoriented("failed", f"tesseract's OSD gave a confidence of {confidence.group(1)}, not a finite number")
    return Orientation(degrees=int(rotate.group(1)), confidence=value, status="ok", reason="")


class _Placed(NamedTuple):
    """A word tesseract read, its box in pixels on the righted page (its band's offset added), and its line.

    ``line`` is its band's top row, then the TSV's block, paragraph and line
    numbers: a tesseract line's key on the page.
    """

    text: str
    x0: int
    y0: int
    x1: int
    y1: int
    confidence: float
    line: tuple[int, int, int, int]


class _Failure(NamedTuple):
    """Why a band was not read."""

    status: OCRStatus
    reason: str


def ocr_words(
    page_bands: tuple[Band, ...],
    *,
    dpi: int,
    rotation: int,
    page_size_pt: tuple[float, float],
    timeout_s: float,
    language: str = "eng",
) -> OCRResult:
    """The words tesseract reads in ``page_bands`` (``--psm 3``, TSV, ``-l language``), on the displayed page.

    ``page_bands`` are the :func:`bands` of the page as :func:`rotated` by
    ``rotation`` degrees righted it; the page was rendered at ``dpi`` from a
    displayed ``page_size_pt`` (width, height). Each band is a call of
    ``timeout_s`` at most. The first band that fails or times out ends the
    reading: its status and reason (naming the band) come back with the words
    of the bands read before it. Words read twice in a shared strip are kept
    once (the higher confidence).

    Raises:
        TypeError, ValueError: If an argument is invalid, or a band does not
            fit the righted page (before anything runs).
    """
    displayed = _displayed_size(page_bands, dpi, rotation, page_size_pt)
    positive_float("timeout_s", timeout_s)
    if not isinstance(language, str) or _LANGUAGE.fullmatch(language) is None:
        raise ValueError(f"language must be tesseract language names (like eng or deu+eng), got {language!r}")
    binary = trusted_executable("tesseract")
    if binary is None:
        return OCRResult(words=(), status="unavailable", reason=_NOT_ON_PATH)
    argv = [binary, "stdin", "stdout", "--psm", "3", "-l", language, "tsv"]
    read: list[tuple[Band, list[_Placed]]] = []
    for number, band in enumerate(page_bands, start=1):
        words = _band_words(argv, band, timeout_s)
        if isinstance(words, _Failure):
            where = f"band {number} of {len(page_bands)} (rows {band.top_px} to {band.top_px + band.px_height - 1})"
            found = _page_words(read, rotation, displayed, dpi)
            return OCRResult(words=found, status=words.status, reason=f"{where}: {words.reason}")
        read.append((band, words))
    return OCRResult(words=_page_words(read, rotation, displayed, dpi), status="ok", reason="")


def _displayed_size(
    page_bands: tuple[Band, ...], dpi: int, rotation: int, page_size_pt: tuple[float, float]
) -> tuple[int, int]:
    """The displayed page's size in pixels, once every band is checked to fit the righted page."""
    typed_tuple("page_bands", page_bands, Band)
    if not page_bands:
        raise ValueError("page_bands must hold at least one band")
    int_at_least("dpi", dpi, 1)
    _checked_turn("rotation", rotation)
    width_pt, height_pt = finite_tuple("page_size_pt", page_size_pt, 2)
    width, height = render_size(
        positive_float("page_size_pt[0]", width_pt), positive_float("page_size_pt[1]", height_pt), dpi
    )
    righted = (height, width) if rotation in (90, 270) else (width, height)
    for number, band in enumerate(page_bands, start=1):
        if band.px_width != righted[0]:
            message = f"band {number} is {band.px_width} px wide; a {width} x {height} px page righted by {rotation}"
            raise ValueError(f"{message} degrees is {righted[0]} px wide")
        if band.top_px + band.px_height > righted[1]:
            rows = f"rows {band.top_px} to {band.top_px + band.px_height - 1}"
            raise ValueError(f"band {number} ({rows}) runs past the {righted[1]} px tall righted page")
    return width, height


def _band_words(argv: list[str], band: Band, timeout_s: float) -> list[_Placed] | _Failure:
    """The words tesseract reads in ``band``, placed on the righted page; or why it read none."""
    call = _call(argv, _pgm(band), timeout_s)
    if call.status != "ok":
        return _Failure(call.status, call.reason)
    words = _tsv_words(call.stdout, band)
    if isinstance(words, str):
        return _Failure("failed", words)
    return words


def _tsv_words(stdout: bytes, band: Band) -> list[_Placed] | str:
    """The word rows of tesseract's TSV for ``band``, placed on the righted page; or why it does not parse."""
    try:
        text = stdout.decode("utf-8")
    except UnicodeDecodeError as exc:
        return f"tesseract's TSV is not UTF-8: {exc}"
    lines = text.split("\n")  # bounded: stdout is within _MAX_STDOUT_BYTES
    if lines[0].rstrip("\r") != _TSV_HEADER:
        return f"tesseract's TSV has an unexpected header: {lines[0][:200]!r}"
    words: list[_Placed] = []
    for number, line in enumerate(lines[1:], start=2):
        if not line:
            continue
        try:
            word = _tsv_word(line.rstrip("\r").split("\t"), band)
        except ValueError as exc:
            return f"tesseract's TSV line {number}: {exc}"
        if word is not None:
            words.append(word)
    return words


def _tsv_word(fields: list[str], band: Band) -> _Placed | None:
    """The word on one TSV row, placed on the righted page; None for a row that is not a word.

    Raises:
        ValueError: If the row is malformed.
    """
    if len(fields) != _TSV_FIELDS:
        raise ValueError(f"{len(fields)} fields, not {_TSV_FIELDS}")
    if fields[0] != _WORD_LEVEL:
        return None
    block, paragraph, line = (_tsv_int(name, value) for name, value in zip(_TSV_LINE, fields[2:5]))
    left, top, width, height = (_tsv_int(name, value) for name, value in zip(_TSV_BOX, fields[6:10]))
    confidence = _tsv_confidence(fields[10])
    text = fields[11]
    if confidence < 0.0 or not text.strip():
        return None
    y0 = band.top_px + top
    return _Placed(text, left, y0, left + width, y0 + height, confidence, (band.top_px, block, paragraph, line))


def _tsv_int(name: str, value: str) -> int:
    """A TSV box field: a non-negative int."""
    try:
        number = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an int, got {value!r}") from None
    return int_at_least(name, number, 0)


def _tsv_confidence(value: str) -> float:
    """A TSV confidence: a finite number up to 100 (-1 on rows that are not words)."""
    try:
        number = float(value)
    except ValueError:
        raise ValueError(f"conf must be a number, got {value!r}") from None
    return finite_float("conf", number, hi=100.0)


def _page_words(
    read: list[tuple[Band, list[_Placed]]], rotation: int, displayed: tuple[int, int], dpi: int
) -> tuple[OCRWord, ...]:
    """The words read, each word read twice kept once, on the displayed page in points, their lines numbered."""
    lines: dict[tuple[int, int, int, int], int] = {}
    return tuple(
        OCRWord(
            text=word.text,
            bbox=_displayed_box(word, rotation, displayed, dpi),
            confidence=word.confidence,
            line=lines.setdefault(word.line, len(lines)),
        )
        for word in _merged(read)
    )


def _merged(read: list[tuple[Band, list[_Placed]]]) -> list[_Placed]:
    """Every band's words, band by band, without the seams' fragments, a word read twice kept once (higher confidence).

    Fragments (:func:`_fragments`) go first, so a copy read twice is never
    kept over a whole copy for being cut.
    """
    fragments: set[tuple[int, int]] = set()
    for index in range(len(read) - 1):
        fragments |= _fragments(read[index], read[index + 1], index)
    dropped = set(fragments)
    for index in range(len(read) - 1):
        dropped |= _read_twice(read[index], read[index + 1], index, fragments)
    return [
        word
        for index, (_band, words) in enumerate(read)
        for position, word in enumerate(words)
        if (index, position) not in dropped
    ]


def _fragments(
    upper: tuple[Band, list[_Placed]], lower: tuple[Band, list[_Placed]], index: int
) -> set[tuple[int, int]]:
    """(band, position) of each word the seam of ``upper`` (band ``index``) and ``lower`` cuts, read whole across it.

    A word is cut when it lies wholly in the strip the bands share and its
    box is within :data:`_CUT_EDGE_PX` rows of its band's cut edge (the upper
    band's bottom, the lower band's top). It is a fragment when a word of
    the other band, more than that from the edge (whole there), overlaps it.
    A cut word nothing whole overlaps is kept: words read once are never
    dropped.
    """
    (upper_band, upper_words), (lower_band, lower_words) = upper, lower
    strip = (lower_band.top_px, upper_band.top_px + upper_band.px_height)
    upper_cut, upper_whole = _at_edge(upper_words, strip, at_top=False)
    lower_cut, lower_whole = _at_edge(lower_words, strip, at_top=True)
    return {
        *((index, at) for at in _overlapped(upper_cut, lower_whole)),
        *((index + 1, at) for at in _overlapped(lower_cut, upper_whole)),
    }


def _at_edge(
    words: list[_Placed], strip: tuple[int, int], *, at_top: bool
) -> tuple[list[tuple[int, _Placed]], list[_Placed]]:
    """A band's words reaching into ``strip`` (its first row, and the row past its last): those cut, those whole.

    The band's cut edge is the strip's top when ``at_top`` (the lower band),
    else its bottom (the upper band). A word farther than
    :data:`_CUT_EDGE_PX` rows from it is whole; one within it is cut, with
    its position, if it lies wholly in the strip, else neither: it reaches a
    row the other band does not hold.
    """
    top, bottom = strip
    cut: list[tuple[int, _Placed]] = []
    whole: list[_Placed] = []
    for position, word in enumerate(words):
        if word.y1 <= top or word.y0 >= bottom:
            continue
        if (word.y0 - top if at_top else bottom - word.y1) > _CUT_EDGE_PX:
            whole.append(word)
        elif top <= word.y0 and word.y1 <= bottom:
            cut.append((position, word))
    return cut, whole


def _overlapped(cut: list[tuple[int, _Placed]], whole: list[_Placed]) -> list[int]:
    """The positions of the ``cut`` words some ``whole`` word overlaps (boxes sharing an area over 0).

    A sweep along x, cut words by their left edges: each is checked against
    the whole words starting before its right edge that reach past its left
    one. O((c + w) log(c + w)) to sort, then O(c a) for a whole words
    active at once (a few on a strip of text lines).
    """
    pending = sorted(whole, key=lambda word: word.x0)
    active: list[_Placed] = []
    taken = 0
    found: list[int] = []
    for position, word in sorted(cut, key=lambda item: item[1].x0):
        while taken < len(pending) and pending[taken].x0 < word.x1:
            active.append(pending[taken])
            taken += 1
        active = [other for other in active if other.x1 > word.x0]
        if any(_overlap(word, other) > 0 for other in active):
            found.append(position)
    return found


def _read_twice(
    upper: tuple[Band, list[_Placed]], lower: tuple[Band, list[_Placed]], index: int, fragments: set[tuple[int, int]]
) -> set[tuple[int, int]]:
    """(band, position) of the lesser copy of each word read in both ``upper`` (band ``index``) and ``lower``.

    Only words reaching into the strip the two bands share can be read in
    both; a copy is the same text with a box IoU of 0.5 or more. A tie keeps
    the upper band's. ``fragments`` take no part.
    """
    (upper_band, upper_words), (lower_band, lower_words) = upper, lower
    strip_top, strip_bottom = lower_band.top_px, upper_band.top_px + upper_band.px_height
    below: dict[str, list[int]] = {}
    for position, word in enumerate(lower_words):
        if word.y0 < strip_bottom and (index + 1, position) not in fragments:
            below.setdefault(word.text, []).append(position)
    lesser: set[tuple[int, int]] = set()
    for position, word in enumerate(upper_words):
        if word.y1 <= strip_top or (index, position) in fragments:
            continue
        for other in below.get(word.text, ()):
            if _iou(word, lower_words[other]) >= _SAME_WORD_IOU:
                lesser.add(
                    (index + 1, other) if lower_words[other].confidence <= word.confidence else (index, position)
                )
    return lesser


def _iou(first: _Placed, second: _Placed) -> float:
    """The boxes' intersection over their union; 0 when the union has no area."""
    overlap = _overlap(first, second)
    union = (first.x1 - first.x0) * (first.y1 - first.y0) + (second.x1 - second.x0) * (second.y1 - second.y0) - overlap
    return overlap / union if union > 0 else 0.0


def _overlap(first: _Placed, second: _Placed) -> int:
    """The area, in px, the boxes share; 0 when they do not overlap."""
    width = min(first.x1, second.x1) - max(first.x0, second.x0)
    height = min(first.y1, second.y1) - max(first.y0, second.y0)
    return max(width, 0) * max(height, 0)


def _displayed_box(word: _Placed, rotation: int, displayed: tuple[int, int], dpi: int) -> BBox:
    """``word``'s box on the displayed page, in points: the inverse of the clockwise ``rotation`` that righted it.

    ``displayed`` is the displayed page's (width, height) in pixels.
    """
    width, height = displayed
    x0, y0, x1, y1 = word.x0, word.y0, word.x1, word.y1
    if rotation == 90:
        box = (y0, height - x1, y1, height - x0)
    elif rotation == 180:
        box = (width - x1, height - y1, width - x0, height - y0)
    elif rotation == 270:
        box = (width - y1, x0, width - y0, x1)
    else:
        box = (x0, y0, x1, y1)
    scale = 72.0 / dpi
    return BBox(x0=box[0] * scale, y0=box[1] * scale, x1=box[2] * scale, y1=box[3] * scale)


def _pgm(image: RenderedPage | Band) -> tuple[bytes, bytes]:
    """``image`` as a binary PGM (P5, 8-bit): its header, then its pixels."""
    return b"P5\n%d %d\n255\n" % (image.px_width, image.px_height), image.pixels


class _Call(NamedTuple):
    """How one tesseract call ended: its status and reason, its whole stdout when ok, and its stderr's tail."""

    status: OCRStatus
    reason: str
    stdout: bytes
    stderr: str


def _call(argv: list[str], payload: tuple[bytes, ...], timeout_s: float) -> _Call:
    """Run ``argv`` with ``payload`` on its stdin, killing it (and its session) past ``timeout_s``.

    tesseract reads all of its stdin before it writes anything, so the
    payload is written, then stdout read (within ``_MAX_STDOUT_BYTES``), in
    this thread; stderr goes to a temporary file and cannot block it.
    """
    with tempfile.TemporaryFile() as errors:
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=errors,
                env={**os.environ, "OMP_THREAD_LIMIT": "1"},
                start_new_session=_OWN_SESSION,
            )
        except OSError as exc:
            return _Call("unavailable", f"cannot start tesseract ({argv[0]}): {exc}", b"", "")
        expired = threading.Event()
        timer = threading.Timer(timeout_s, _expire, (process, expired))
        timer.daemon = True
        timer.start()
        fed, stdout = "", None
        try:
            fed = _fed(process.stdin, payload)
            stdout = _bounded_read(process.stdout, _MAX_STDOUT_BYTES)
        finally:
            # Cancelled and joined before the reap: a kill never reaches a reaped (reusable) pid.
            timer.cancel()
            timer.join(REAP_TIMEOUT_S)
            if stdout is None:  # past its stdout bound, or this call was interrupted
                _kill(process)
            returncode = reaped(process, REAP_TIMEOUT_S)
            close_pipes(process)
        stderr = stderr_tail(errors)
    return _ending(returncode, expired.is_set(), stdout, fed, stderr, timeout_s)


def _fed(stdin: IO[bytes] | None, payload: tuple[bytes, ...]) -> str:
    """Write ``payload`` to the child's stdin and close it: "" once all of it is written, else why not."""
    if stdin is None:
        raise ValueError("tesseract's stdin must be a pipe")
    try:
        for part in payload:
            stdin.write(part)
        stdin.close()
    except OSError as exc:  # it stopped reading: it ended, or was killed
        _discard(stdin)
        return str(exc) or type(exc).__name__
    return ""


def _discard(stdin: IO[bytes]) -> None:
    """Close a stdin whose write failed; its unwritten bytes can only fail the same way (already reported)."""
    try:
        stdin.close()
    except OSError:
        return


def _bounded_read(stdout: IO[bytes] | None, bound: int) -> bytes | None:
    """All of ``stdout`` to its end, or None once it passes ``bound`` bytes.

    Each read asks for a whole chunk (or the one byte past the bound) and
    returns it whole unless the stream ends, so the loop makes at most
    ``bound // _READ_CHUNK + 2`` passes.
    """
    if stdout is None:
        raise ValueError("tesseract's stdout must be a pipe")
    data = bytearray()
    while len(data) <= bound:
        chunk = stdout.read(min(_READ_CHUNK, bound + 1 - len(data)))
        if not chunk:
            return bytes(data)
        data += chunk
    return None


def _expire(process: "subprocess.Popen[bytes]", expired: threading.Event) -> None:
    """The timer's: the call's time is up; kill it."""
    expired.set()
    _kill(process)


def _kill(process: "subprocess.Popen[bytes]") -> None:
    """Kill ``process`` and, on POSIX, everything in its session (its process group)."""
    if not _OWN_SESSION:
        process.kill()
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except OSError:  # its group has ended, or cannot be signalled: kill the process itself
        process.kill()


def _ending(
    returncode: int | None, expired: bool, stdout: bytes | None, fed: str, stderr: str, timeout_s: float
) -> _Call:
    """How a call ended: ok only if it exited 0 with its whole stdout, having read the whole image."""
    if returncode == 0 and stdout is not None and not fed:
        return _Call("ok", "", stdout, stderr)
    said = f"; its stderr ends: {stderr}" if stderr else ""
    if expired:
        return _Call("timeout", f"tesseract did not finish within {timeout_s} s and was killed{said}", b"", stderr)
    if stdout is None:
        return _Call("failed", f"tesseract wrote over {_MAX_STDOUT_BYTES} bytes to stdout and was killed", b"", stderr)
    if returncode is None:
        return _Call("failed", "tesseract could not be reaped", b"", stderr)
    if returncode != 0:
        return _Call("failed", f"tesseract exited with code {returncode}{said}", b"", stderr)
    return _Call("failed", f"tesseract did not read the whole image: {fed}", b"", stderr)
