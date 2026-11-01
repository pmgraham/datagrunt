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
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
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
from datagrunt.pdf._validate import int_at_least, positive_float
from datagrunt.pdf.exceptions import DatagruntPDFError
from datagrunt.pdf.pdfium_engine import (
    PageGeometry,
    _checked_password,
    _checked_path,
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


class RenderRefused(DatagruntPDFError):
    """A page is not rendered: rendering it is refused before pdfium draws anything."""


class RenderFailed(DatagruntPDFError):
    """A page's render worker was killed, timed out or failed: the page is not rendered; the caller continues."""


@dataclass(frozen=True, slots=True, kw_only=True)
class RenderedPage:
    """A page :func:`render_page` drew in 8-bit gray.

    Attributes:
        pixels: ``px_width * px_height`` bytes, row by row from the displayed
            page's top-left corner, 0 black to 255 white, unpadded. Not in
            repr.
        px_width: Pixels across the displayed page (:func:`render_size`).
        px_height: Pixels down it.
    """

    pixels: bytes = field(repr=False)
    px_width: int
    px_height: int

    def __post_init__(self) -> None:
        int_at_least("px_width", self.px_width, 1)
        int_at_least("px_height", self.px_height, 1)
        if not isinstance(self.pixels, bytes):
            raise TypeError(f"pixels must be bytes, got {type(self.pixels).__name__}")
        expected = self.px_width * self.px_height
        if len(self.pixels) != expected:
            message = f"pixels must hold {self.px_width} x {self.px_height} = {expected} bytes, got {len(self.pixels)}"
            raise ValueError(message)


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
    with open_document(source, password=password) as handle, loaded_page(handle, page_index) as (page, geometry):
        refusal = image_refusal(page, geometry, max_objects=_MAX_GUARD_OBJECTS)
        if refusal is not None:
            raise RenderRefused(f"page {geometry.page_number}: {refusal}; the page is not rendered")
        yield page, geometry


class _Bitmap(NamedTuple):
    """A page drawn in this process: its size and its pixels (unpadded rows) in anonymous memory."""

    width: int
    height: int
    pixels: mmap.mmap


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


def _draw(page: pdfium.PdfPage, geometry: PageGeometry, dpi: int) -> _Bitmap:
    """Draw the loaded page at ``dpi`` on white, in 8-bit gray, without its annotations."""
    width, height = render_size(geometry.width_pt, geometry.height_pt, dpi)
    if width < 1 or height < 1:
        message = f"page {geometry.page_number} renders {width} x {height} px at {dpi} DPI; the page is not rendered"
        raise RenderRefused(message)
    pixels = _blank(width * height)
    buffer = (ctypes.c_ubyte * (width * height)).from_buffer(pixels)
    bitmap = pdfium_c.FPDFBitmap_CreateEx(width, height, pdfium_c.FPDFBitmap_Gray, buffer, width)
    if not bitmap:
        raise RenderFailed(f"page {geometry.page_number}: pdfium cannot make a {width} x {height} gray bitmap")
    try:
        if not pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, width, height, _WHITE):
            raise RenderFailed(f"page {geometry.page_number}: pdfium cannot fill a {width} x {height} bitmap")
        # Rotation 0 draws the page as displayed (its /Rotate applied), filling the bitmap.
        pdfium_c.FPDF_RenderPageBitmap(bitmap, page.raw, 0, 0, width, height, 0, _RENDER_FLAGS)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    del buffer  # releases the mapping's export, so it can be sent and closed
    return _Bitmap(width, height, pixels)


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


class _Reply(NamedTuple):
    """A worker's whole reply: its kind, its message (refused, failed), and the page it drew (pixels)."""

    kind: bytes
    detail: str
    page: RenderedPage | None


def render_page(
    source: str | os.PathLike[str],
    page_index: int,
    *,
    dpi: int,
    password: str | None = None,
    memory_cap_bytes: int,
    timeout_s: float,
) -> RenderedPage:
    """Render the page at 0-based ``page_index`` in 8-bit gray at ``dpi``, in a capped worker process.

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
    """Exchange with the started worker until its reply or ``deadline``, then reap it.

    A timer kills the worker at the deadline, which closes its stdout: so
    no read or write outlives the deadline (a worker that stalls mid-reply
    included). Returns the reply (None: none came whole), whether the
    deadline passed without one, and the exit code.
    """
    killer = threading.Timer(max(0.0, deadline - time.monotonic()), worker.kill)
    killer.daemon = True
    killer.start()
    reply: _Reply | None = None
    try:
        reply = _exchange(worker.stdin, worker.stdout, payload, memory_cap_bytes)
    finally:
        # Cancelled and joined before the worker is reaped: a kill never reaches a reaped (reusable) pid.
        killer.cancel()
        killer.join(REAP_TIMEOUT_S)
        timed_out = reply is None and time.monotonic() >= deadline
        exitcode = reaped(worker, REAP_TIMEOUT_S if reply is not None else 0.0 if timed_out else _EXIT_GRACE_S)
    return reply, timed_out, exitcode


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
        status = _read_message(stdout, _MAX_STATUS_BYTES)
        if status is None:
            return None
        kind = status[:1]
        if kind != _PIXELS:
            return _message_reply(kind, status[1:])
        size = _pixel_size(status[1:], memory_cap_bytes)
        if isinstance(size, str):
            return _Reply(_FAILED, size, None)
        width, height = size
        pixels = _read_message(stdout, width * height)
    except (OSError, MessageTooLong):  # the worker stopped reading (it died, or was killed), or overran a bound
        return None
    if pixels is None:
        return None
    if len(pixels) != width * height:
        return _Reply(_FAILED, f"it sent {len(pixels)} pixel bytes for a {width} x {height} px render", None)
    return _Reply(_PIXELS, "", RenderedPage(pixels=pixels, px_width=width, px_height=height))


def _message_reply(kind: bytes, message: bytes) -> _Reply:
    """A refused or failed reply; any other kind is a failure."""
    if kind not in (_REFUSED, _FAILED):
        return _Reply(_FAILED, f"it sent a reply of unknown kind {kind!r}", None)
    return _Reply(kind, message.decode("utf-8", errors="backslashreplace"), None)


def _pixel_size(header: bytes, memory_cap_bytes: int) -> tuple[int, int] | str:
    """The ``(width, height)`` a pixel reply claims, or why the claim is refused.

    The worker's pixels live within its memory cap, so a claim past it is
    refused before the parent receives (or allocates) a byte of them.
    """
    if len(header) != _PIXEL_SIZE.size:
        return f"it sent a {len(header)}-byte pixel header, not {_PIXEL_SIZE.size}"
    width, height = _PIXEL_SIZE.unpack(header)
    if width < 1 or height < 1:
        return f"it claimed a {width} x {height} px render"
    if width * height > memory_cap_bytes:
        return f"it claimed a {width} x {height} px render, over its {memory_cap_bytes}-byte memory cap"
    return width, height


def _outcome(reply: _Reply | None, timed_out: bool, exitcode: int | None, request: _Request, tail: str) -> RenderedPage:
    """The rendered page, or the RenderRefused / RenderFailed the worker's ending makes."""
    where = f"{request.source}: page {request.page_index + 1}"
    if reply is None:
        ending = _ending(exitcode, timed_out, request)
        raise RenderFailed(f"{where}: {ending}; its stderr ends: {tail}" if tail else f"{where}: {ending}")
    if reply.page is not None:
        return reply.page
    if reply.kind == _REFUSED:
        raise RenderRefused(f"{request.source}: {reply.detail}")
    raise RenderFailed(f"{where}: the render worker failed: {reply.detail}")


def _ending(exitcode: int | None, timed_out: bool, request: _Request) -> str:
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

    It caps its own memory before it opens the PDF. Reaching the cap (a
    MemoryError) ends it with ``_CAP_EXIT`` and no reply; a refusal and any
    other error are sent back as messages, never swallowed.
    """
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
