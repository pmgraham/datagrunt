"""Guarded page render for the v5 vision route (#369, A5a).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.

A page renders at :func:`effective_dpi`: the spec's
``min(target_dpi, int(72 * sqrt(max_render_pixels / (width_pt * height_pt))))``,
so a large-format sheet lowers its DPI rather than its pixel bound. Each
displayed side renders to ``floor(side_pt * dpi / 72)`` pixels
(:func:`render_size`); rounding down keeps the product within the bound,
which the spec's continuous formula alone guarantees only before rounding.

Before a page renders, the image guard
(:func:`~datagrunt.pdf.pdfium_engine.image_refusal`) checks every image
object pdfium exposes on it, forms' included, against A2's decode bounds,
on the same load of the page the render then draws; a refusal, or the
guard's walk reaching its bound, is a :class:`RenderRefused` naming the
object, and nothing is drawn.

:func:`render_page` runs the guard and the render in a worker process
whose memory is capped, because pdfium decodes what the object
walk cannot see (soft masks, patterns, Type3 glyph procedures) and an image
within A2's bounds can still cost ~1.7 GiB to decode. The cap is the
worker's memory over what it holds once started (the interpreter and
datagrunt's imports):

- Linux: ``RLIMIT_AS`` at the address space mapped then plus the cap; past
  it every new mapping fails (Python raises MemoryError).
- macOS, which does not enforce ``RLIMIT_AS``: a watchdog thread in the
  worker ends it once its peak RSS passes its starting peak plus the cap.
  pdfium's ctypes calls release the GIL, so it runs while pdfium allocates;
  it polls every 5 ms, and its overshoot is what is allocated in one poll,
  or in one allocation made holding the GIL (at most an image stream the
  guard copies: the 1 GiB asset limit).
- Windows: a Job Object whose per-process commit limit is the commit then
  plus the cap: the kernel's equivalent of ``RLIMIT_AS``, with no polling
  and no overshoot (preferred to a ``GetProcessMemoryInfo`` watchdog). No
  test runs on Windows: this path is untested.
- Anywhere else the worker refuses to render (a :class:`RenderFailed`).

The worker is a plain subprocess, ``python -I -c BOOTSTRAP``, started by
the launcher the page workers share (:mod:`datagrunt.pdf._process`, worker
key ``"render"``): it never imports the caller's main module, so a caller's
script runs once, however it is written; ``-I`` keeps the caller's working
directory and ``PYTHON*`` environment out of it; the bootstrap installs the
caller's exact ``sys.path``, sent with the request.

The request (with ``sys.path`` and the password) is one JSON message on
the worker's stdin, never in its argv or environment. The worker replies
on its stdout in length-prefixed raw bytes, which a stray print cannot
corrupt. Nothing is pickled either way: the parent decodes nothing but
bytes from a worker (which parses hostile PDFs), each message within a
bound. The worker's stderr goes to a temporary file; a failure without a
reply ends with its last 4 KiB.

Every wait is bounded by ``timeout_s``: at the deadline the worker is
killed and reaped. A worker killed, timed out or failed is a
:class:`RenderFailed`; the caller continues. A frozen application, or one
whose ``sys.executable`` is empty, has no interpreter to start: every
render is a :class:`RenderFailed`, raised before anything starts.

Annotations are not drawn (no ``FPDF_ANNOT``): their appearance streams
are content the guard's object walk cannot see, so a vision page's form
field values, stamps and markup are not rendered, and not read by OCR.

Analysis modes (#400), for the background check of
:mod:`datagrunt.pdf.quality`; the defaults (8-bit gray, with the text) are
A5a's:

- ``colour``: 24-bit BGR (pdfium's ``FPDFBitmap_BGR``: blue, green, red
  bytes a pixel), unpadded rows, drawn with no flag.
- ``without_text``: once the guard passes the page, the worker removes every
  text object of its load, page level and inside forms
  (``FPDFPage_RemoveObject`` / ``FPDFFormObj_RemoveObject``, on the guard's
  bounded object walk), then draws it. The edit lives in that load only:
  nothing is saved, and pdfium regenerates no content.

:func:`render_pages` draws several pages of one document in one worker
(the same key, ``"render"``: a request holding ``pages`` is a batch). The
worker caps its memory once, opens the document once, then guards and
draws each page in turn, from a load of its own, and replies for each in
order. The cap is per worker and one timeout covers the whole batch. A page
refused or failed is that page's outcome and the batch goes on; a worker
that ends (its cap, the timeout, a crash) fails every page it had not
answered, each with why. Every page's pixels are held at once: the caller
bounds the batch (at most :data:`MAX_BATCH_PAGES` pages).
"""

import ctypes
import math
import mmap
import os
import struct
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from functools import partial
from typing import IO, NamedTuple

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c

from datagrunt.pdf._process import MAX_REQUEST_BYTES as _MAX_REQUEST_BYTES
from datagrunt.pdf._process import (
    REAP_TIMEOUT_S,
    MessageTooLong,
    NoInterpreter,
    close_pipes,
    reaped,
    request_payload,
    start_worker,
    stderr_tail,
    worker_command,
)
from datagrunt.pdf._process import read_message as _read_message
from datagrunt.pdf._process import send_message as _send
from datagrunt.pdf._validate import int_at_least, positive_float, strict_bool
from datagrunt.pdf.exceptions import DatagruntPDFError
from datagrunt.pdf.pdfium_engine import (
    DocumentHandle,
    PageGeometry,
    _checked_password,
    _checked_path,
    _page_objects,
    image_refusal,
    loaded_page,
    open_document,
)

if sys.platform != "win32":
    import resource

# The spec's construction checks (PDFReader / PDFWriter): dpi >= 36, max_render_pixels >= 512 x 512.
_MIN_TARGET_DPI = 36
_MIN_RENDER_PIXELS = 262_144
_POINTS_PER_INCH = 72.0

# The objects (forms' contents included) the guard walks before it refuses a
# page: ~250 times the corpus's busiest page (3,977). The walk costs ~0.4 us
# an object here, so the bound is about half a second of guard.
_MAX_GUARD_OBJECTS = 1_000_000

# A worker's replies, raw bytes, each message after its 8-byte length: a kind
# byte, then a UTF-8 message (refused, failed) or the pixel size, followed by
# the pixels as a second message.
_PIXELS, _REFUSED, _FAILED = b"P", b"R", b"F"
_PIXEL_SIZE = struct.Struct("<QQ")
_MAX_STATUS_BYTES = 1 << 20

# A worker's exit codes: it reached its memory cap; its watchdog outlived its
# own deadline (the parent's timeout, which starts first, kills it before).
_CAP_EXIT = 86
_DEADLINE_EXIT = 87

_EXIT_GRACE_S = 1.0  # a worker that closed its pipe without a reply is given this to exit
_WATCH_INTERVAL_S = 0.005
_MAX_PROC_FILE_BYTES = 1 << 16  # /proc/self/status is ~1.5 KB

_WHITE = 0xFFFFFFFF
# Gray output and nothing else: no FPDF_ANNOT (annotations are not drawn), no LCD text, no printing mode.
_RENDER_FLAGS = pdfium_c.FPDF_GRAYSCALE

# The pages one render_pages call draws, at most: each page's pixels are held at once.
MAX_BATCH_PAGES = 64


class _Format(NamedTuple):
    """How a page is drawn: bytes a pixel, pdfium's bitmap format and render flags, and the format's name."""

    channels: int
    bitmap: int
    flags: int
    name: str


_GRAY = _Format(1, pdfium_c.FPDFBitmap_Gray, _RENDER_FLAGS, "gray")
# Colour, and nothing else: no flag (no annotations, no LCD text, no printing mode).
_BGR = _Format(3, pdfium_c.FPDFBitmap_BGR, 0, "BGR")
_CHANNELS = (_GRAY.channels, _BGR.channels)


class RenderRefused(DatagruntPDFError):
    """A page is not rendered: rendering it is refused before pdfium draws anything."""


class RenderFailed(DatagruntPDFError):
    """A page's render worker was killed, timed out or failed: the page is not rendered; the caller continues."""


@dataclass(frozen=True, slots=True, kw_only=True)
class RenderedPage:
    """A page :func:`render_page` drew: in 8-bit gray, or in 24-bit BGR (``colour``).

    Attributes:
        pixels: ``px_width * px_height * channels`` bytes, row by row from
            the displayed page's top-left corner, unpadded: a byte a pixel,
            0 black to 255 white, in gray; blue, green and red bytes a
            pixel in BGR. Not in repr.
        px_width: Pixels across the displayed page (:func:`render_size`).
        px_height: Pixels down it.
        channels: 1 (gray) or 3 (BGR).
    """

    pixels: bytes = field(repr=False)
    px_width: int
    px_height: int
    channels: int = 1

    def __post_init__(self) -> None:
        int_at_least("px_width", self.px_width, 1)
        int_at_least("px_height", self.px_height, 1)
        if int_at_least("channels", self.channels, 1) not in _CHANNELS:
            raise ValueError(f"channels must be 1 (gray) or 3 (BGR), got {self.channels}")
        if not isinstance(self.pixels, bytes):
            raise TypeError(f"pixels must be bytes, got {type(self.pixels).__name__}")
        expected = self.px_width * self.px_height * self.channels
        if len(self.pixels) != expected:
            size = f"{self.px_width} x {self.px_height}" + (f" x {self.channels}" if self.channels > 1 else "")
            raise ValueError(f"pixels must hold {size} = {expected} bytes, got {len(self.pixels)}")


def render_size(width_pt: float, height_pt: float, dpi: int) -> tuple[int, int]:
    """The ``(width, height)`` in pixels a displayed ``width_pt`` x ``height_pt`` page renders to at ``dpi``.

    Each side is rounded down, so a side under one pixel is 0.

    Raises:
        TypeError: If ``dpi`` is not an int (bool rejected) or a side is not a number.
        ValueError: If ``dpi`` is under 1, or a side is not finite and positive.
    """
    int_at_least("dpi", dpi, 1)
    width = positive_float("width_pt", width_pt)
    height = positive_float("height_pt", height_pt)
    return int(width * dpi / _POINTS_PER_INCH), int(height * dpi / _POINTS_PER_INCH)


def effective_dpi(target_dpi: int, max_render_pixels: int, width_pt: float, height_pt: float) -> int:
    """The DPI a displayed ``width_pt`` x ``height_pt`` page renders at: ``target_dpi``, lowered to fit the pixel bound.

    The spec's ``min(target_dpi, int(72 * sqrt(max_render_pixels / (width_pt * height_pt))))``.
    At the DPI returned, :func:`render_size` is at least 1 x 1 and at most
    ``max_render_pixels`` pixels.

    Raises:
        TypeError: If ``target_dpi`` or ``max_render_pixels`` is not an int
            (bool rejected), or a side is not a number.
        ValueError: If ``target_dpi`` is under 36, ``max_render_pixels`` under
            262,144 (512 x 512), or a side is not finite and positive.
        RenderRefused: If no DPI of 1 or more renders the page within
            ``max_render_pixels`` (a page over about 1,000 inches square at
            the default bound), or a side renders under one pixel at the DPI
            the formula gives (``1e8 x 1`` pt at 28 DPI is 0 px tall): rounding
            it up to 1 px would break the bound, and the page cannot be read.
    """
    int_at_least("target_dpi", target_dpi, _MIN_TARGET_DPI)
    int_at_least("max_render_pixels", max_render_pixels, _MIN_RENDER_PIXELS)
    width = positive_float("width_pt", width_pt)
    height = positive_float("height_pt", height_pt)
    area = width * height
    # A product of tiny sides can underflow to 0: such a page fits at any DPI.
    bound = _POINTS_PER_INCH * math.sqrt(max_render_pixels / area) if area > 0.0 else math.inf
    # Compared before int(): int(inf) would raise, and min(target, int(bound)) is this.
    dpi = target_dpi if bound >= target_dpi else int(bound)
    page = f"a {width} x {height} pt page"
    if dpi < 1:
        message = f"{page} renders over the {max_render_pixels}-pixel bound at 1 DPI (bound: {bound:.2f} DPI)"
        raise RenderRefused(message)
    width_px, height_px = render_size(width, height, dpi)
    if width_px < 1:
        raise RenderRefused(f"{page} renders 0 px wide at {dpi} DPI")
    if height_px < 1:
        raise RenderRefused(f"{page} renders 0 px tall at {dpi} DPI")
    return dpi


@contextmanager
def _guarded_page(
    source: str | os.PathLike[str], page_index: int, password: str | None
) -> Iterator[tuple[pdfium.PdfPage, PageGeometry]]:
    """The loaded page at ``page_index`` with its geometry, once the image guard passes it.

    The guard and the render that follows share this one load of the page.
    The document and page are closed on leaving.

    Raises:
        RenderRefused: If :func:`~datagrunt.pdf.pdfium_engine.image_refusal`
            refuses the page; nothing is yielded.
        FileNotFoundError, PermissionError, PDFEncryptedError, PDFCorruptError,
        IndexError: As :func:`~datagrunt.pdf.pdfium_engine.open_document` and
            :func:`~datagrunt.pdf.pdfium_engine.loaded_page` raise them.
    """
    with open_document(source, password=password) as handle, _guarded(handle, page_index) as loaded:
        yield loaded


@contextmanager
def _guarded(handle: DocumentHandle, page_index: int) -> Iterator[tuple[pdfium.PdfPage, PageGeometry]]:
    """The page at ``page_index`` of the open document, loaded, once the image guard passes it; closed on leaving.

    Raises:
        RenderRefused: If the guard refuses the page; nothing is yielded.
        IndexError, PDFCorruptError: As
            :func:`~datagrunt.pdf.pdfium_engine.loaded_page` raises them.
    """
    with loaded_page(handle, page_index) as (page, geometry):
        refusal = image_refusal(page, geometry, max_objects=_MAX_GUARD_OBJECTS)
        if refusal is not None:
            raise RenderRefused(f"page {geometry.page_number}: {refusal}; the page is not rendered")
        yield page, geometry


class _Bitmap(NamedTuple):
    """A page drawn in this process: its size, its pixels (unpadded rows) in anonymous memory, and bytes a pixel."""

    width: int
    height: int
    pixels: mmap.mmap
    channels: int = 1


def _render_guarded(source: str | os.PathLike[str], page_index: int, dpi: int, password: str | None) -> _Bitmap:
    """Guard the page, then draw it at ``dpi`` from the same load, in this process.

    The worker calls this under its memory cap; never call it outside one.

    Raises:
        RenderRefused: If the guard refuses the page, or it draws under one
            pixel across at ``dpi``; nothing is drawn.
        RenderFailed: If pdfium cannot make or fill the bitmap.
        MemoryError: If the bitmap cannot be mapped (a memory cap).
    """
    with _guarded_page(source, page_index, password) as (page, geometry):
        return _draw(page, geometry, dpi)


def _draw(page: pdfium.PdfPage, geometry: PageGeometry, dpi: int, *, colour: bool = False) -> _Bitmap:
    """Draw the loaded page at ``dpi`` on white, in 8-bit gray (24-bit BGR if ``colour``), without its annotations."""
    drawn = _BGR if colour else _GRAY
    width, height = render_size(geometry.width_pt, geometry.height_pt, dpi)
    if width < 1 or height < 1:
        message = f"page {geometry.page_number} renders {width} x {height} px at {dpi} DPI; the page is not rendered"
        raise RenderRefused(message)
    stride = width * drawn.channels  # unpadded rows
    pixels = _blank(stride * height)
    buffer = (ctypes.c_ubyte * (stride * height)).from_buffer(pixels)
    bitmap = pdfium_c.FPDFBitmap_CreateEx(width, height, drawn.bitmap, buffer, stride)
    if not bitmap:
        raise RenderFailed(f"page {geometry.page_number}: pdfium cannot make a {width} x {height} {drawn.name} bitmap")
    try:
        if not pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, width, height, _WHITE):
            raise RenderFailed(f"page {geometry.page_number}: pdfium cannot fill a {width} x {height} bitmap")
        # Rotation 0 draws the page as displayed (its /Rotate applied), filling the bitmap.
        pdfium_c.FPDF_RenderPageBitmap(bitmap, page.raw, 0, 0, width, height, 0, drawn.flags)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    del buffer  # releases the mapping's export, so it can be sent and closed
    return _Bitmap(width, height, pixels, drawn.channels)


def _render_loaded(handle: DocumentHandle, page_index: int, dpi: int, *, colour: bool, without_text: bool) -> _Bitmap:
    """Guard the page of the open document, remove its text if asked, then draw it from the same load.

    The batch worker calls this under its memory cap; never call it outside one.

    Raises:
        RenderRefused: If the guard refuses the page, or it draws under one pixel across at ``dpi``.
        RenderFailed: If pdfium cannot remove a text object, or make or fill the bitmap.
        MemoryError: If the bitmap cannot be mapped (a memory cap).
    """
    with _guarded(handle, page_index) as (page, geometry):
        if without_text:
            _remove_text(page, geometry.page_number)
        return _draw(page, geometry, dpi, colour=colour)


def _remove_text(page: pdfium.PdfPage, page_number: int) -> int:
    """Remove every text object from the loaded page, forms' included; how many. In this load only: nothing is saved.

    Walks the guard's walk (:func:`~datagrunt.pdf.pdfium_engine._page_objects`
    within :data:`_MAX_GUARD_OBJECTS`), so it is bounded by the page the guard
    passed. Each removed object is destroyed (pdfium hands it to the caller).

    Raises:
        RenderFailed: If pdfium cannot remove a text object (named by its walk index).
    """
    objects = _page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS)
    removed = 0
    for index, (item, form) in enumerate(objects):  # bounded: the walk's objects
        if pdfium_c.FPDFPageObj_GetType(item) != pdfium_c.FPDF_PAGEOBJ_TEXT:
            continue
        if form < 0:
            done = pdfium_c.FPDFPage_RemoveObject(page.raw, item)
        else:
            done = pdfium_c.FPDFFormObj_RemoveObject(objects[form][0], item)
        if not done:
            raise RenderFailed(
                f"page {page_number}: pdfium cannot remove text object {index}; the page is not rendered"
            )
        pdfium_c.FPDFPageObj_Destroy(item)
        removed += 1
    return removed


def _blank(size: int) -> mmap.mmap:
    """``size`` bytes of anonymous memory, mapped but untouched: pdfium writes them with the GIL released.

    A ``bytearray`` would be zeroed here, holding the GIL, out of the macOS
    watchdog's sight.
    """
    try:
        return mmap.mmap(-1, size)
    except OSError as exc:
        # Linux refuses a mapping past RLIMIT_AS, and Windows one past the Job Object's commit limit.
        raise MemoryError(f"cannot map {size} bytes for the bitmap: {exc}") from exc


@dataclass(frozen=True, slots=True, kw_only=True)
class _Request:
    """What the parent asks its worker for, in the JSON on the worker's stdin.

    The password is never in repr: it reaches the worker in that JSON only.
    """

    source: str
    page_index: int
    dpi: int
    password: str | None = field(repr=False)
    memory_cap_bytes: int
    timeout_s: float


_REQUEST_FIELDS = tuple(item.name for item in fields(_Request))


@dataclass(frozen=True, slots=True, kw_only=True)
class _BatchRequest:
    """A batch the parent asks its worker for (:func:`render_pages`), in the JSON on the worker's stdin.

    ``pages`` are ``(0-based page index, dpi)`` pairs, drawn in order. The
    password is never in repr: it reaches the worker in that JSON only.
    """

    source: str
    pages: tuple[tuple[int, int], ...]
    password: str | None = field(repr=False)
    memory_cap_bytes: int
    timeout_s: float
    colour: bool
    without_text: bool


_BATCH_FIELDS = tuple(item.name for item in fields(_BatchRequest))


class _Reply(NamedTuple):
    """A worker's whole reply: its kind, its message (refused, failed), and the page it drew (pixels)."""

    kind: bytes
    detail: str
    page: RenderedPage | None


class _Malformed(Exception):
    """A worker's reply the parent refuses (a bad header, a claim past the cap, short pixels): a failure, unread on."""


def render_page(
    source: str | os.PathLike[str],
    page_index: int,
    *,
    dpi: int,
    password: str | None = None,
    memory_cap_bytes: int,
    timeout_s: float,
    colour: bool = False,
    without_text: bool = False,
) -> RenderedPage:
    """Render the page at 0-based ``page_index`` at ``dpi``, in a capped worker process: in 8-bit gray by default.

    The worker (``python -I``, see the module notes) caps its own memory at
    ``memory_cap_bytes`` over what it holds once started, runs the image
    guard, then draws the page from the same load: as displayed (its /Rotate
    applied), on white, without annotations. Pass :func:`effective_dpi`'s
    result as ``dpi``; the page is then at most ``max_render_pixels``.

    Args:
        source: The PDF's path (made absolute here).
        page_index: The 0-based page.
        dpi: Dots per inch, at least 1.
        password: The encrypted PDF's password, or None. Sent on the
            worker's stdin only; never in an error message.
        memory_cap_bytes: The worker's memory past its start, at least 1.
        timeout_s: Seconds the whole render may take, the worker's start
            included (a worker imports datagrunt: ~0.5 s here).
        colour: Draw in 24-bit BGR (``channels`` 3) instead of 8-bit gray.
        without_text: Remove the page's text objects from the worker's load
            before drawing it (see the module notes).

    Either mode draws the page as a one-page :func:`render_pages` batch does.

    Raises:
        TypeError, ValueError: If an argument is invalid (before any worker starts).
        RenderRefused: If the guard refuses the page (naming the object), or
            it draws under one pixel across at ``dpi``. Nothing is drawn.
        RenderFailed: If no worker can start (no interpreter, or the OS
            refuses), or the worker reaches its memory cap, outlives
            ``timeout_s`` (it is killed and reaped), dies, or fails (the PDF
            missing, encrypted without its password, its page unloadable):
            the page is not rendered.
    """
    request = _Request(
        source=str(_checked_path(source, "source").absolute()),
        page_index=int_at_least("page_index", page_index, 0),
        dpi=int_at_least("dpi", dpi, 1),
        password=_checked_password(password),
        memory_cap_bytes=int_at_least("memory_cap_bytes", memory_cap_bytes, 1),
        timeout_s=positive_float("timeout_s", timeout_s),
    )
    modes = (strict_bool("colour", colour), strict_bool("without_text", without_text))
    if any(modes):
        return _one_page_batch(request, colour=colour, without_text=without_text)
    where = f"{request.source}: page {request.page_index + 1}"
    command = _worker_command(where)
    payload = _request_payload(request)
    with tempfile.TemporaryFile() as errors:
        deadline = time.monotonic() + request.timeout_s
        worker = _started(command, errors, where)
        try:
            reply, timed_out, exitcode = _supervised(worker, payload, request.memory_cap_bytes, deadline)
        finally:
            close_pipes(worker)
        tail = "" if reply is not None else stderr_tail(errors)
    return _outcome(reply, timed_out, exitcode, request, tail)


def _one_page_batch(request: _Request, *, colour: bool, without_text: bool) -> RenderedPage:
    """The page of ``request`` drawn as a one-page batch in the modes asked for; its refusal or failure raised."""
    (result,) = render_pages(
        request.source,
        ((request.page_index, request.dpi),),
        password=request.password,
        memory_cap_bytes=request.memory_cap_bytes,
        timeout_s=request.timeout_s,
        colour=colour,
        without_text=without_text,
    )
    if isinstance(result, RenderedPage):
        return result
    raise result


def render_pages(
    source: str | os.PathLike[str],
    pages: tuple[tuple[int, int], ...],
    *,
    password: str | None = None,
    memory_cap_bytes: int,
    timeout_s: float,
    colour: bool = False,
    without_text: bool = False,
) -> tuple[RenderedPage | RenderRefused | RenderFailed, ...]:
    """Render several pages of one PDF in one capped worker process: each page's outcome, in order.

    Each page is drawn as :func:`render_page` draws it, in the modes asked
    for, from a load of its own (see the module notes). The cap is the
    worker's and ``timeout_s`` covers the whole batch, the worker's start
    included. A page the guard refuses is its :class:`RenderRefused`, a page
    that fails (unloadable, past the last) its :class:`RenderFailed`, and the
    batch goes on. When the worker ends (its cap, the timeout, a crash), no
    worker can start, or the PDF cannot be opened, every page not yet
    answered is a :class:`RenderFailed` saying why. Nothing else is raised.

    Args:
        source: The PDF's path (made absolute here).
        pages: ``(0-based page index, dpi)`` pairs, 1 to
            :data:`MAX_BATCH_PAGES` of them; a page may repeat. Pass each
            page's :func:`effective_dpi`.
        password, memory_cap_bytes: As :func:`render_page`'s.
        timeout_s: Seconds the whole batch may take.
        colour, without_text: As :func:`render_page`'s, for every page.

    Raises:
        TypeError, ValueError: If an argument is invalid (before any worker starts).
    """
    request = _BatchRequest(
        source=str(_checked_path(source, "source").absolute()),
        pages=_checked_pages(pages, tuple),
        password=_checked_password(password),
        memory_cap_bytes=int_at_least("memory_cap_bytes", memory_cap_bytes, 1),
        timeout_s=positive_float("timeout_s", timeout_s),
        colour=strict_bool("colour", colour),
        without_text=strict_bool("without_text", without_text),
    )
    try:
        replies, timed_out, exitcode, tail = _run_batch(request)
    except RenderFailed as exc:  # no worker ran: every page fails alike
        return tuple(RenderFailed(f"{request.source}: page {index + 1}: {exc}") for index, _dpi in request.pages)
    return _batch_outcomes(replies, timed_out, exitcode, request, tail)


def _checked_pages(value: object, kind: type) -> tuple[tuple[int, int], ...]:
    """``value`` as ``(page index, dpi)`` pairs, 1 to MAX_BATCH_PAGES of them: a ``kind`` of ``kind`` pairs.

    The parent passes tuples; JSON brings a worker lists.
    """
    if not isinstance(value, (tuple, list)) or not isinstance(value, kind):
        raise TypeError(f"pages must be a {kind.__name__}, got {type(value).__name__}")
    if not 1 <= len(value) <= MAX_BATCH_PAGES:
        raise ValueError(f"pages must hold 1 to {MAX_BATCH_PAGES} (page index, dpi) pairs, got {len(value)}")
    checked = []
    for position, pair in enumerate(value):
        if not isinstance(pair, (tuple, list)) or not isinstance(pair, kind) or len(pair) != 2:
            raise ValueError(f"pages[{position}] must be a (page index, dpi) pair, got {pair!r}")
        index = int_at_least(f"pages[{position}] page index", pair[0], 0)
        checked.append((index, int_at_least(f"pages[{position}] dpi", pair[1], 1)))
    return tuple(checked)


def _run_batch(request: _BatchRequest) -> tuple[list[_Reply], bool, int | None, str]:
    """Run the batch's worker to its replies: them, whether its deadline passed, its exit code and stderr's tail.

    Raises:
        RenderFailed: If no worker can start (no interpreter, no file for
            its stderr, the OS refuses), or the request is over its bound.
    """
    try:
        command = worker_command("a render worker")
    except NoInterpreter as exc:
        raise RenderFailed(str(exc)) from exc
    payload = _batch_payload(request)
    count, channels = len(request.pages), (_BGR if request.colour else _GRAY).channels
    with _stderr_file() as errors:
        deadline = time.monotonic() + request.timeout_s
        try:
            worker = start_worker(command, errors)
        except OSError as exc:
            raise RenderFailed(f"cannot start a render worker: {exc}") from exc
        exchange = partial(
            _exchange_batch, worker.stdin, worker.stdout, payload, request.memory_cap_bytes, channels, count
        )
        try:
            replies, timed_out, exitcode = _watched(worker, deadline, count, exchange)
        finally:
            close_pipes(worker)
        tail = "" if len(replies) == count else stderr_tail(errors)
    return replies, timed_out, exitcode, tail


def _stderr_file() -> IO[bytes]:
    """A temporary file for a worker's stderr; RenderFailed if none can be made."""
    try:
        return tempfile.TemporaryFile()
    except OSError as exc:
        raise RenderFailed(f"cannot make a file for the render worker's stderr: {exc}") from exc


def _batch_payload(request: _BatchRequest) -> bytes:
    """The JSON a batch worker reads from its stdin: the caller's exact sys.path, the worker's key and the batch."""
    try:
        return request_payload("render", asdict(request), _MAX_REQUEST_BYTES)
    except ValueError as exc:
        raise RenderFailed(str(exc)) from exc


def _batch_outcomes(
    replies: list[_Reply], timed_out: bool, exitcode: int | None, request: _BatchRequest, tail: str
) -> tuple[RenderedPage | RenderRefused | RenderFailed, ...]:
    """Each page's outcome: its reply, in order, and past the replies, why the worker ended."""
    ending = _ending(exitcode, timed_out, request)
    unanswered = f"{ending}; its stderr ends: {tail}" if tail else ending
    outcomes: list[RenderedPage | RenderRefused | RenderFailed] = []
    for position, (index, _dpi) in enumerate(request.pages):
        where = f"{request.source}: page {index + 1}"
        if position < len(replies):
            outcomes.append(_page_outcome(replies[position], request.source, where))
        else:
            outcomes.append(RenderFailed(f"{where}: {unanswered}"))
    return tuple(outcomes)


def _page_outcome(reply: _Reply, source: str, where: str) -> RenderedPage | RenderRefused | RenderFailed:
    """The page one reply holds, or the RenderRefused / RenderFailed it says."""
    if reply.page is not None:
        return reply.page
    if reply.kind == _REFUSED:
        return RenderRefused(f"{source}: {reply.detail}")
    return RenderFailed(f"{where}: the render worker failed: {reply.detail}")


def _worker_command(where: str) -> list[str]:
    """The worker's command line, which holds nothing of the request; RenderFailed if there is no interpreter."""
    try:
        return worker_command("a render worker")
    except NoInterpreter as exc:
        raise RenderFailed(f"{where}: {exc}") from exc


def _request_payload(request: _Request) -> bytes:
    """The JSON the worker reads from its stdin: the caller's exact sys.path, the worker's key and the request."""
    try:
        return request_payload("render", asdict(request), _MAX_REQUEST_BYTES)
    except ValueError as exc:
        raise RenderFailed(str(exc)) from exc


def _started(command: list[str], errors: IO[bytes], where: str) -> "subprocess.Popen[bytes]":
    """Start the worker: stdin and stdout piped to this process, stderr to ``errors``."""
    try:
        return start_worker(command, errors)
    except OSError as exc:
        raise RenderFailed(f"{where}: cannot start a render worker: {exc}") from exc


def _supervised(
    worker: "subprocess.Popen[bytes]", payload: bytes, memory_cap_bytes: int, deadline: float
) -> tuple[_Reply | None, bool, int | None]:
    """Exchange with the started worker until its reply or ``deadline``, then reap it (see :func:`_watched`).

    Returns the reply (None: none came whole), whether the deadline passed
    without one, and the exit code.
    """

    def exchange() -> list[_Reply]:
        reply = _exchange(worker.stdin, worker.stdout, payload, memory_cap_bytes)
        return [] if reply is None else [reply]

    replies, timed_out, exitcode = _watched(worker, deadline, 1, exchange)
    return (replies[0] if replies else None), timed_out, exitcode


def _watched(
    worker: "subprocess.Popen[bytes]", deadline: float, count: int, exchange: Callable[[], list[_Reply]]
) -> tuple[list[_Reply], bool, int | None]:
    """Run ``exchange`` with the started worker until it returns or ``deadline``, then reap the worker.

    A timer kills the worker at the deadline, which closes its stdout: so
    no read or write outlives the deadline (a worker that stalls mid-reply
    included). Returns the replies, whether the deadline passed before all
    ``count`` came, and the exit code.
    """
    killer = threading.Timer(max(0.0, deadline - time.monotonic()), worker.kill)
    killer.daemon = True
    killer.start()
    replies: list[_Reply] = []
    try:
        replies = exchange()
    finally:
        # Cancelled and joined before the worker is reaped: a kill never reaches a reaped (reusable) pid.
        killer.cancel()
        killer.join(REAP_TIMEOUT_S)
        complete = len(replies) == count
        timed_out = not complete and time.monotonic() >= deadline
        exitcode = reaped(worker, REAP_TIMEOUT_S if complete else 0.0 if timed_out else _EXIT_GRACE_S)
    return replies, timed_out, exitcode


def _exchange(
    stdin: IO[bytes] | None, stdout: IO[bytes] | None, payload: bytes, memory_cap_bytes: int
) -> _Reply | None:
    """Write ``payload`` to the worker; return its whole reply, or None if none came whole.

    A worker that dies, or is killed at the deadline, closes its stdout and
    ends the exchange. Each message is read within a bound: a refusal's or
    failure's within ``_MAX_STATUS_BYTES``, the pixels within their claimed
    size, itself within the worker's memory cap.
    """
    if stdin is None or stdout is None:
        raise ValueError("the worker's stdin and stdout must be pipes")
    try:
        stdin.write(payload)
        stdin.close()
        return _read_reply(stdout, memory_cap_bytes, _GRAY.channels)
    except _Malformed as exc:
        return _Reply(_FAILED, str(exc), None)
    except (OSError, MessageTooLong):  # the worker stopped reading (it died, or was killed), or overran a bound
        return None


def _exchange_batch(
    stdin: IO[bytes] | None,
    stdout: IO[bytes] | None,
    payload: bytes,
    memory_cap_bytes: int,
    channels: int,
    count: int,
) -> list[_Reply]:
    """Write ``payload`` to the worker; return its replies, one a page, in order, up to ``count``.

    Each is read as :func:`_exchange` reads one. Reading ends early when the
    worker's replies end (it died, or was killed at the deadline): the pages
    left are unanswered. A reply the parent refuses (malformed, or over a
    bound) is that page's failure, and nothing after it is read: the stream
    can no longer be trusted.
    """
    if stdin is None or stdout is None:
        raise ValueError("the worker's stdin and stdout must be pipes")
    replies: list[_Reply] = []
    try:
        stdin.write(payload)
        stdin.close()
        for _ in range(count):  # bounded: one reply a page
            reply = _read_reply(stdout, memory_cap_bytes, channels)
            if reply is None:
                return replies
            replies.append(reply)
    except (_Malformed, MessageTooLong) as exc:
        replies.append(_Reply(_FAILED, str(exc), None))
    except OSError:  # it stopped reading (it died, or was killed): each page left fails with why it ended
        return replies
    return replies


def _read_reply(stdout: IO[bytes], memory_cap_bytes: int, channels: int) -> _Reply | None:
    """One page's whole reply from the worker's ``stdout``, or None if its replies end first.

    Raises:
        _Malformed: If the reply cannot be trusted (see :func:`_pixel_size`), or its pixels are short.
        MessageTooLong: If a message is over its bound (unread).
        OSError: If the pipe fails.
    """
    status = _read_message(stdout, _MAX_STATUS_BYTES)
    if status is None:
        return None
    kind = status[:1]
    if kind != _PIXELS:
        return _message_reply(kind, status[1:])
    width, height = _pixel_size(status[1:], memory_cap_bytes, channels)
    pixels = _read_message(stdout, width * height * channels)
    if pixels is None:
        return None
    if len(pixels) != width * height * channels:
        raise _Malformed(f"it sent {len(pixels)} pixel bytes for a {_px(width, height, channels)} render")
    return _Reply(_PIXELS, "", RenderedPage(pixels=pixels, px_width=width, px_height=height, channels=channels))


def _message_reply(kind: bytes, message: bytes) -> _Reply:
    """A refused or failed reply; any other kind is a failure."""
    if kind not in (_REFUSED, _FAILED):
        return _Reply(_FAILED, f"it sent a reply of unknown kind {kind!r}", None)
    return _Reply(kind, message.decode("utf-8", errors="backslashreplace"), None)


def _pixel_size(header: bytes, memory_cap_bytes: int, channels: int) -> tuple[int, int]:
    """The ``(width, height)`` a pixel reply claims.

    The worker's pixels live within its memory cap, so a claim past it is
    refused before the parent receives (or allocates) a byte of them.

    Raises:
        _Malformed: Naming why the claim is refused.
    """
    if len(header) != _PIXEL_SIZE.size:
        raise _Malformed(f"it sent a {len(header)}-byte pixel header, not {_PIXEL_SIZE.size}")
    width, height = _PIXEL_SIZE.unpack(header)
    if width < 1 or height < 1:
        raise _Malformed(f"it claimed a {_px(width, height, channels)} render")
    if width * height * channels > memory_cap_bytes:
        raise _Malformed(
            f"it claimed a {_px(width, height, channels)} render, over its {memory_cap_bytes}-byte memory cap"
        )
    return width, height


def _px(width: int, height: int, channels: int) -> str:
    """A render's size in words: ``"3 x 2 px"``, and ``"3 x 2 px BGR"`` in colour."""
    return f"{width} x {height} px" + (f" {_BGR.name}" if channels == _BGR.channels else "")


def _outcome(reply: _Reply | None, timed_out: bool, exitcode: int | None, request: _Request, tail: str) -> RenderedPage:
    """The rendered page, or the RenderRefused / RenderFailed the worker's ending makes."""
    where = f"{request.source}: page {request.page_index + 1}"
    if reply is None:
        ending = _ending(exitcode, timed_out, request)
        raise RenderFailed(f"{where}: {ending}; its stderr ends: {tail}" if tail else f"{where}: {ending}")
    result = _page_outcome(reply, request.source, where)
    if isinstance(result, RenderedPage):
        return result
    raise result


def _ending(exitcode: int | None, timed_out: bool, request: _Request | _BatchRequest) -> str:
    """Why a worker ended without a whole reply."""
    if timed_out:
        return f"the render timed out after {request.timeout_s} s; its worker was killed"
    if exitcode is None:
        return "the render worker ended without a reply and could not be reaped"
    if exitcode == _CAP_EXIT:
        return f"the render worker reached its {request.memory_cap_bytes}-byte memory cap and was ended"
    if exitcode < 0:
        return f"the render worker was killed by signal {-exitcode}"
    return f"the render worker exited with code {exitcode} without a reply"


def _worker_main(message: object, replies: IO[bytes]) -> None:
    """The render worker, run by _BOOTSTRAP: one request in, one reply out on ``replies``, as raw bytes.

    A request holding ``pages`` is a batch (:func:`_batch_main`). It caps its
    own memory before it opens the PDF. Reaching the cap (a MemoryError)
    ends it with ``_CAP_EXIT`` and no reply; a refusal and any other error
    are sent back as messages, never swallowed.
    """
    if isinstance(message, dict) and "pages" in message:
        _batch_main(message, replies)
        return
    try:
        request = _request_from(message)
        _apply_memory_cap(request.memory_cap_bytes, request.timeout_s)
        bitmap = _render_guarded(request.source, request.page_index, request.dpi, request.password)
    except MemoryError:
        os._exit(_CAP_EXIT)
    except RenderRefused as exc:
        _send(replies, _REFUSED + str(exc).encode("utf-8", errors="backslashreplace"))
        return
    except Exception as exc:  # every other failure reaches the parent, as a RenderFailed
        _send(replies, _FAILED + f"{type(exc).__name__}: {exc}".encode("utf-8", errors="backslashreplace"))
        return
    _send(replies, _PIXELS + _PIXEL_SIZE.pack(bitmap.width, bitmap.height))
    _send(replies, bitmap.pixels)


def _batch_main(message: dict[str, object], replies: IO[bytes]) -> None:
    """The batch worker: a batch in; each page's reply (its pixels after it) out on ``replies``, in order.

    It caps its own memory once, before it opens the PDF, then opens it once.
    Reaching the cap ends it with ``_CAP_EXIT``: the parent fails the pages
    not yet answered. A batch it cannot read is one failure; a PDF it cannot
    open fails each page with the reason. Nothing is swallowed.
    """
    try:
        request = _batch_request_from(message)
    except Exception as exc:  # sent, never swallowed: the parent fails the batch's pages
        _send(replies, _failure(exc))
        return
    sent = 0
    try:
        _apply_memory_cap(request.memory_cap_bytes, request.timeout_s)
        with open_document(request.source, password=request.password) as handle:
            for index, dpi in request.pages:  # bounded: at most MAX_BATCH_PAGES
                _send_page(replies, handle, index, dpi, request)
                sent += 1
    except MemoryError:
        os._exit(_CAP_EXIT)
    except Exception as exc:  # no page left can be drawn (the PDF cannot be opened): each fails with it
        for _ in request.pages[sent:]:
            _send(replies, _failure(exc))


def _send_page(replies: IO[bytes], handle: DocumentHandle, index: int, dpi: int, request: _BatchRequest) -> None:
    """Draw one page of the batch and send its reply; a refusal or failure is its reply, and the batch goes on.

    A MemoryError (the cap) is raised: it ends the worker.
    """
    try:
        bitmap = _render_loaded(handle, index, dpi, colour=request.colour, without_text=request.without_text)
    except MemoryError:
        raise
    except RenderRefused as exc:
        _send(replies, _REFUSED + str(exc).encode("utf-8", errors="backslashreplace"))
        return
    except Exception as exc:  # this page's failure, sent to the parent
        _send(replies, _failure(exc))
        return
    with bitmap.pixels:  # unmapped once sent: a batch holds one page's pixels at a time
        _send(replies, _PIXELS + _PIXEL_SIZE.pack(bitmap.width, bitmap.height))
        _send(replies, bitmap.pixels)


def _failure(exc: Exception) -> bytes:
    """A failed reply naming ``exc``: its type and message."""
    return _FAILED + f"{type(exc).__name__}: {exc}".encode("utf-8", errors="backslashreplace")


def _request_from(message: object) -> _Request:
    """The request a worker reads (JSON from its parent), its fields checked as render_page checks them."""
    if not isinstance(message, dict):
        raise TypeError(f"the worker's request must be a dict, got {type(message).__name__}")
    if set(message) != set(_REQUEST_FIELDS):
        raise ValueError(f"the worker's request must hold exactly {_REQUEST_FIELDS}, got {sorted(message)}")
    return _Request(
        source=str(_checked_path(message["source"], "source")),
        page_index=int_at_least("page_index", message["page_index"], 0),
        dpi=int_at_least("dpi", message["dpi"], 1),
        password=_checked_password(message["password"]),
        memory_cap_bytes=int_at_least("memory_cap_bytes", message["memory_cap_bytes"], 1),
        timeout_s=positive_float("timeout_s", message["timeout_s"]),
    )


def _batch_request_from(message: dict[str, object]) -> _BatchRequest:
    """The batch a worker reads (JSON from its parent), its fields checked as render_pages checks them."""
    if set(message) != set(_BATCH_FIELDS):
        raise ValueError(f"the batch request must hold exactly {_BATCH_FIELDS}, got {sorted(message)}")
    return _BatchRequest(
        source=str(_checked_path(message["source"], "source")),
        pages=_checked_pages(message["pages"], list),
        password=_checked_password(message["password"]),
        memory_cap_bytes=int_at_least("memory_cap_bytes", message["memory_cap_bytes"], 1),
        timeout_s=positive_float("timeout_s", message["timeout_s"]),
        colour=strict_bool("colour", message["colour"]),
        without_text=strict_bool("without_text", message["without_text"]),
    )


def _apply_memory_cap(cap_bytes: int, timeout_s: float) -> None:
    """Cap this worker's memory at ``cap_bytes`` over what it holds now; call it in a worker only.

    Raises:
        OSError: On a platform with no cap here: the page is not rendered uncapped.
    """
    if sys.platform == "linux":
        _cap_address_space(cap_bytes)
    elif sys.platform == "darwin":
        _watch_peak_rss(_peak_rss_bytes() + cap_bytes, time.monotonic() + timeout_s)
    elif sys.platform == "win32":
        _cap_commit(cap_bytes)
    else:
        raise OSError(f"no memory cap is implemented for platform {sys.platform!r}; the page is not rendered")


def _cap_address_space(cap_bytes: int) -> None:
    """Linux: limit this process's address space (RLIMIT_AS, soft and hard) to what it maps now plus ``cap_bytes``.

    Past it the kernel refuses every new mapping: malloc returns NULL, Python
    raises MemoryError, mmap fails. Lowering the hard limit too means nothing
    in the worker can lift it again.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    limit = _address_space_limit(_mapped_bytes(), cap_bytes, soft, hard)
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def _address_space_limit(mapped: int, cap_bytes: int, soft: int, hard: int) -> int:
    """``mapped + cap_bytes``, never above a limit already set (``RLIM_INFINITY``: none)."""
    limits = [mapped + cap_bytes] + [limit for limit in (soft, hard) if limit != resource.RLIM_INFINITY]
    return min(limits)


def _mapped_bytes() -> int:
    """Linux: this process's mapped address space (VmSize) in bytes, from /proc/self/statm."""
    with open("/proc/self/statm", "rb") as statm:
        pages = int(statm.read(_MAX_PROC_FILE_BYTES).split()[0])
    return pages * os.sysconf("SC_PAGE_SIZE")


def _peak_rss_bytes() -> int:
    """This process's peak resident set size in bytes (Linux and macOS).

    Linux reads VmHWM: its ru_maxrss survives fork + exec, so a spawned
    worker would read its parent's peak. macOS's ru_maxrss is bytes, reset
    at exec.
    """
    if sys.platform == "linux":
        with open("/proc/self/status", "rb") as status:
            for line in status.read(_MAX_PROC_FILE_BYTES).splitlines():
                if line.startswith(b"VmHWM:"):
                    return int(line.split()[1]) * 1024
        raise OSError("no VmHWM line in /proc/self/status")
    if sys.platform == "darwin":
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    raise OSError(f"no peak RSS reader for platform {sys.platform!r}")


def _watch_peak_rss(limit_bytes: int, deadline: float) -> None:
    """End this process once its peak RSS passes ``limit_bytes``, or at ``deadline``.

    A daemon thread polls every 5 ms and exits the process with
    ``_CAP_EXIT`` (past the limit) or ``_DEADLINE_EXIT``. It reads the peak,
    not the current RSS, so a spike between two polls still ends the process.
    """

    def watch() -> None:
        while time.monotonic() < deadline:
            if _peak_rss_bytes() > limit_bytes:
                os._exit(_CAP_EXIT)
            time.sleep(_WATCH_INTERVAL_S)
        os._exit(_DEADLINE_EXIT)

    threading.Thread(target=watch, name="datagrunt-pdf-render-watchdog", daemon=True).start()


class _BasicLimits(ctypes.Structure):
    """JOBOBJECT_BASIC_LIMIT_INFORMATION (winnt.h)."""

    _fields_ = (
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", ctypes.c_uint32),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", ctypes.c_uint32),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", ctypes.c_uint32),
        ("SchedulingClass", ctypes.c_uint32),
    )


class _ExtendedLimits(ctypes.Structure):
    """JOBOBJECT_EXTENDED_LIMIT_INFORMATION (winnt.h), its IO_COUNTERS as six 64-bit counts."""

    _fields_ = (
        ("BasicLimitInformation", _BasicLimits),
        ("IoInfo", ctypes.c_uint64 * 6),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    )


class _MemoryCounters(ctypes.Structure):
    """PROCESS_MEMORY_COUNTERS_EX (psapi.h)."""

    _fields_ = (
        ("cb", ctypes.c_uint32),
        ("PageFaultCount", ctypes.c_uint32),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    )


_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9  # JOBOBJECTINFOCLASS
_JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x100


def _cap_commit(cap_bytes: int) -> None:
    """Windows: put this process in a new Job Object that limits its commit to what it commits now plus ``cap_bytes``.

    Past it the kernel refuses every commit (VirtualAlloc fails, Python
    raises MemoryError), as RLIMIT_AS does on Linux. Jobs nest (Windows 8
    and later), so this works inside a CI runner's or terminal's job. No
    test runs on Windows: this is untested.
    """
    if sys.platform != "win32":
        raise OSError("a Job Object caps a Windows process only")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.CreateJobObjectW.restype = ctypes.c_void_p
    kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p)
    kernel32.SetInformationJobObject.argtypes = (ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32)
    kernel32.AssignProcessToJobObject.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
    kernel32.K32GetProcessMemoryInfo.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32)
    process = kernel32.GetCurrentProcess()
    counters = _MemoryCounters(cb=ctypes.sizeof(_MemoryCounters))
    if not kernel32.K32GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    limits = _ExtendedLimits()
    limits.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_PROCESS_MEMORY
    limits.ProcessMemoryLimit = counters.PrivateUsage + cap_bytes
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    information = ctypes.byref(limits)
    if not kernel32.SetInformationJobObject(
        job, _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, information, ctypes.sizeof(limits)
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    if not kernel32.AssignProcessToJobObject(job, process):
        raise ctypes.WinError(ctypes.get_last_error())
