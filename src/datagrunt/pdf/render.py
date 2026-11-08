"""Guarded page render for the v5 vision route (#369, A5a): the caller's side.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Imports no
pdfium: every pdfium call of a render is the render worker's
(:mod:`datagrunt.pdf._render_worker`).

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
whose memory is capped (:mod:`datagrunt.pdf._memory_cap`: a watchdog on
every platform, and a backstop on Linux and Windows), because pdfium decodes
what the object walk cannot see (soft masks, patterns, Type3 glyph
procedures) and an image within A2's bounds can still cost ~1.7 GiB to
decode. A worker past its cap ends, and its page is a :class:`RenderFailed`
saying so.

The worker is a plain subprocess, ``python -I -c BOOTSTRAP``, started by
the launcher the page and figure workers share
(:mod:`datagrunt.pdf._process`, worker key ``"render"``): it never imports
the caller's main module, so a caller's script runs once, however it is
written; ``-I`` keeps the caller's working directory and ``PYTHON*``
environment out of it; the bootstrap installs the caller's exact
``sys.path``, sent with the request.

The request (with ``sys.path`` and the password) is one JSON message on
the worker's stdin, never in its argv or environment. The worker replies
on its stdout in length-prefixed raw bytes, which a stray print cannot
corrupt. Nothing is pickled either way: the caller decodes nothing but
bytes from a worker (which parses PDFs from anywhere), each message within
a bound. The worker's stderr is drained by a thread into a 4 KiB ring
(:class:`~datagrunt.pdf._process.StderrTail`: nothing is written to disk),
joined within a bound on every path; a failure without a reply ends with
its last 4 KiB.

Every wait is bounded by ``timeout_s``: at the deadline the worker is
killed and reaped. A worker killed, timed out or failed is a
:class:`RenderFailed`; the caller continues. A frozen application, or one
whose ``sys.executable`` is empty, has no interpreter to start: every
render is a :class:`RenderFailed`, raised before anything starts.

Each JBIG2 image the guard's object walk reaches is decoded on its own
(``FPDFImageObj_GetBitmap``) before the page is drawn, with the page's
bitmap already mapped and filled. pdfium draws a page whose image it cannot
decode without that image and without an error, and a JBIG2 decode can
fail (pdfium's own bounds, or a backstop's); decoded first, its failure is
the page's :class:`RenderFailed`, naming the image. JBIG2 images the walk
cannot see (in patterns, Type3 glyph procedures or soft masks) are not
decoded first: the watchdog fails a page whose decode passes the cap.

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
that ends (its cap, the timeout, an unexpected exit) fails every page it
had not answered, each with why. Every page's pixels are held at once: the
caller bounds the batch (at most :data:`MAX_BATCH_PAGES` pages).

Within :func:`render_worker_pool` (a page worker's, a writer's, a
bundle's), a render takes a pre-started spare instead of starting a
worker (:class:`RenderWorkerPool`). A spare serves one request exactly as
a worker started for it would (its own cap, timeout and PDF open), then
exits. Once the request reaches it, its outcome is final: its replies, or
why it ended (its cap, its timeout, its exit code and stderr). A spare that
died while idle never receives the request: a worker started for the
render draws it instead, with its full ``timeout_s``.
"""

import json
import logging
import math
import os
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, fields
from functools import partial
from typing import IO, NamedTuple

from datagrunt.pdf._extra import EXTRA_IMPORTS
from datagrunt.pdf._memory_cap import ending as _worker_ending
from datagrunt.pdf._process import ERROR as _PROCESS_ERROR
from datagrunt.pdf._process import (
    LENGTH,
    REAP_TIMEOUT_S,
    MessageTooLong,
    NoInterpreter,
    StderrTail,
    close_pipes,
    reaped,
    request_payload,
    start_worker,
    worker_command,
)
from datagrunt.pdf._process import MAX_REQUEST_BYTES as _MAX_REQUEST_BYTES
from datagrunt.pdf._process import read_error as _read_error
from datagrunt.pdf._process import read_message as _read_message
from datagrunt.pdf._validate import checked_password as _checked_password
from datagrunt.pdf._validate import checked_path as _checked_path
from datagrunt.pdf._validate import int_at_least, positive_float, strict_bool
from datagrunt.pdf.exceptions import DatagruntPDFError

_LOGGER = logging.getLogger(__name__)

# The spec's construction checks (PDFReader / PDFWriter): dpi >= 36, max_render_pixels >= 512 x 512.
_MIN_TARGET_DPI = 36
_MIN_RENDER_PIXELS = 262_144
_POINTS_PER_INCH = 72.0

# A worker's replies, raw bytes, each message after its 8-byte length: a kind
# byte, then a UTF-8 message (refused, failed) or the pixel size, followed by
# the pixels as a second message.
_PIXELS, _REFUSED, _FAILED = b"P", b"R", b"F"
_ERROR = _PROCESS_ERROR  # a worker that cannot import the [pdf] extra says so first (raised: see _read_reply)
_PIXEL_SIZE = struct.Struct("<QQ")
_MAX_STATUS_BYTES = 1 << 20

_EXIT_GRACE_S = 1.0  # a worker that closed its pipe without a reply is given this to exit

# The pages one render_pages call draws, at most: each page's pixels are held at once.
MAX_BATCH_PAGES = 64

# Bytes a pixel: 8-bit gray, and 24-bit BGR (``colour``), named in a size's words.
GRAY_CHANNELS, BGR_CHANNELS = 1, 3
_CHANNELS = (GRAY_CHANNELS, BGR_CHANNELS)


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


# A spare's program (RenderWorkerPool). Once it has replied (its replies flushed), it flushes its stdio and exits at
# once: it holds nothing to tear down (its PDF closed, its request answered), and its parent reaps it on its way out.
_RENDER_POOL_BOOTSTRAP = (
    "import importlib, json, os, sys\n"
    "replies = os.fdopen(os.dup(1), 'wb')\n"
    "os.dup2(2, 1)\n"
    "sys.path[:] = json.loads(sys.stdin.buffer.readline())\n"
    f"extra = {EXTRA_IMPORTS!r}\n"
    "try:\n"
    "    from datagrunt.pdf._render_worker import _pool_worker_main\n"
    "except ImportError as exc:\n"
    "    if (exc.name or '').partition('.')[0] not in extra:\n"
    "        raise\n"
    "    process = importlib.import_module('datagrunt.pdf._process')\n"
    "    process.send_message(replies, process.missing_extra_reply(exc))\n"
    "    raise SystemExit(0)\n"
    "_pool_worker_main(replies)\n"
    "sys.stdout.flush(); sys.stderr.flush(); os._exit(0)\n"
)


_Worker = tuple["subprocess.Popen[bytes]", StderrTail]

# A pool's spares for one memory cap, idle and starting together, at most; and how long acquire waits for one starting.
_MAX_SPARES = 4
_SPARE_WAIT_S = REAP_TIMEOUT_S
# The spares acquire keeps ahead of a caller that renders in turn: the one it hands out is then the older, its imports
# (~50 ms here) done while the one before it was rendering.
_SPARES_AHEAD = 2
_SPARE_THREAD = "datagrunt-render-spare"  # the name of each thread that starts a spare


class RenderWorkerPool:
    """A scoped pool of pre-started, single-use render workers (spares), for :func:`render_worker_pool`.

    A spare is started ahead of its request (``python -I`` running
    :data:`_RENDER_POOL_BOOTSTRAP`: the caller's ``sys.path``, then the render
    worker's imports) and waits on its stdin. It serves exactly one request,
    as a fresh worker serves it (its own memory cap and deadline, from the
    request; the PDF opened for that request alone), then exits: no worker
    serves twice, and an idle spare holds no PDF open. :meth:`acquire` hands
    out the oldest live idle spare (or a worker started then) and starts its
    replacement in the background, keeping :data:`_SPARES_AHEAD` ahead; at
    most :data:`_MAX_SPARES` spares of one memory cap are idle or starting at
    once. A spare that cannot start is logged. :meth:`close` kills and reaps
    the idle spares (they hold nothing).
    """

    __slots__ = ("_closed", "_cond", "_idle", "_starting", "_waiting")

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._idle: list[tuple[int, subprocess.Popen[bytes], StderrTail]] = []
        self._starting: dict[int, int] = {}  # spares being started, by memory cap
        self._waiting: dict[int, int] = {}  # acquire calls waiting for one of them, by memory cap
        self._closed = False

    def warm(self, memory_cap_bytes: int) -> None:
        """Start a spare for ``memory_cap_bytes`` in the background, unless one is idle or starting already."""
        self._start_spare_below(memory_cap_bytes, 1)

    def acquire(self, where: str, memory_cap_bytes: int) -> _Worker:
        """A live idle spare for ``memory_cap_bytes``, or a worker started now; a replacement spare starts too.

        Waits, within :data:`_SPARE_WAIT_S`, for a spare already starting
        that no other caller waits for. Spares found dead are reaped and
        logged. The worker returned serves one request: never return it.

        Raises:
            ValueError: If the pool is closed.
            RenderFailed: If no spare is idle and no worker can start (``where`` heads it).
        """
        dead: list[_Worker] = []
        try:
            with self._cond:
                spare = self._taken(memory_cap_bytes, dead)
        finally:
            for child, stderr in dead:  # bounded: the idle spares
                _LOGGER.warning("a spare render worker ended while idle: %s", _retired_ending(child, stderr))
        worker = spare if spare is not None else _spare_started(where)
        self._start_spare_below(memory_cap_bytes, _MAX_SPARES)  # its replacement
        self._start_spare_below(memory_cap_bytes, _SPARES_AHEAD)  # and another, while fewer are ahead
        return worker

    def _taken(self, memory_cap_bytes: int, dead: list[_Worker]) -> _Worker | None:
        """An idle spare taken from the pool (the lock held), or None; each dead one found is moved to ``dead``."""
        give_up = time.monotonic() + _SPARE_WAIT_S
        while True:  # bounded: by give_up, which every wait ends by
            if self._closed:
                raise ValueError("RenderWorkerPool is closed")
            live = []
            taken: _Worker | None = None
            for cap, child, stderr in self._idle:  # bounded: the idle spares, oldest (most likely ready) first
                if child.poll() is not None:
                    dead.append((child, stderr))
                elif taken is None and cap == memory_cap_bytes:
                    taken = (child, stderr)
                else:
                    live.append((cap, child, stderr))
            self._idle = live
            if taken is not None:
                return taken
            remaining = give_up - time.monotonic()
            unclaimed = self._starting.get(memory_cap_bytes, 0) - self._waiting.get(memory_cap_bytes, 0)
            if remaining <= 0 or unclaimed <= 0:
                return None
            self._waiting[memory_cap_bytes] = self._waiting.get(memory_cap_bytes, 0) + 1
            try:
                self._cond.wait(remaining)
            finally:
                self._waiting[memory_cap_bytes] -= 1

    def _start_spare_below(self, memory_cap_bytes: int, count: int) -> None:
        """Start a spare for ``memory_cap_bytes`` on a thread of its own if under ``count`` are idle or starting."""
        with self._cond:
            idle = sum(1 for cap, child, _ in self._idle if cap == memory_cap_bytes and child.poll() is None)
            starting = self._starting.get(memory_cap_bytes, 0)
            if self._closed or idle + starting >= count:
                return
            self._starting[memory_cap_bytes] = starting + 1
        try:
            threading.Thread(target=self._spawn, args=(memory_cap_bytes,), name=_SPARE_THREAD, daemon=True).start()
        except RuntimeError as exc:  # no thread to start it: said, and the spare is not counted
            _LOGGER.warning("a spare render worker could not start: no thread to start it: %s", exc)
            self._started_one(memory_cap_bytes)

    def _spawn(self, memory_cap_bytes: int) -> None:
        """A spare's thread: start it, then make it idle, or end it if the pool has closed; a failed start is logged."""
        try:
            spare = _spare_started("")
            if not self._kept(memory_cap_bytes, spare):
                _retired(*spare)
        except RenderFailed as exc:  # said, never swallowed: acquire then starts a worker itself, which raises why
            _LOGGER.warning("a spare render worker could not start: %s", exc)
        finally:
            self._started_one(memory_cap_bytes)

    def _kept(self, memory_cap_bytes: int, spare: _Worker) -> bool:
        """Whether ``spare`` joined the idle spares: not once the pool has closed."""
        with self._cond:
            if self._closed:
                return False
            self._idle.append((memory_cap_bytes, *spare))
            return True

    def _started_one(self, memory_cap_bytes: int) -> None:
        """A spare's start has ended (idle, ended or failed): it is no longer counted as starting."""
        with self._cond:
            self._starting[memory_cap_bytes] -= 1
            self._cond.notify_all()

    def close(self) -> None:
        """Close the pool: once the spares starting have started (within a bound), kill and reap the idle ones."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
            give_up = time.monotonic() + REAP_TIMEOUT_S
            while any(self._starting.values()):  # bounded: by give_up
                remaining = give_up - time.monotonic()
                if remaining <= 0:
                    break
                self._cond.wait(remaining)
            idle, self._idle = self._idle, []
        for _cap, child, stderr in idle:  # bounded: the idle spares
            _retired(child, stderr)

    def __enter__(self) -> "RenderWorkerPool":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _spare_started(where: str) -> _Worker:
    """A spare started now: the pool's bootstrap, sent the caller's ``sys.path`` (see :class:`RenderWorkerPool`).

    Raises:
        RenderFailed: If there is no interpreter, the OS refuses to start
            it, or it ends before it takes the caller's ``sys.path`` (it is
            then reaped); ``where`` (if any) heads it.
    """
    lead = f"{where}: " if where else ""
    try:
        command = [worker_command()[0], "-I", "-c", _RENDER_POOL_BOOTSTRAP]
    except NoInterpreter as exc:
        raise RenderFailed(f"{lead}{exc}") from exc
    child, stderr = _started(command, where)
    path = [os.path.abspath(entry) if entry else os.getcwd() for entry in sys.path]
    if child.stdin is not None and _written(child.stdin, (json.dumps(path) + "\n").encode("utf-8")):
        return child, stderr
    raise RenderFailed(f"{lead}cannot start a render worker: {_retired_ending(child, stderr)}")


def _written(stdin: IO[bytes], data: bytes) -> bool:
    """Write ``data`` whole to a spare's stdin, unbuffered; False if the pipe has no reader (the spare has ended).

    Unbuffered, so no bytes that cannot be delivered stay in the parent's
    buffer to fail again when the pipe is closed.
    """
    pending = memoryview(data)
    try:
        for _ in range(len(data)):  # bounded: each write takes at least one byte (a blocking pipe)
            if not pending:
                break
            pending = pending[os.write(stdin.fileno(), pending) :]
    except OSError:  # EPIPE (EINVAL on Windows): the spare has ended, and its end of the pipe with it
        return False
    return not pending


def _retired(child: "subprocess.Popen[bytes]", stderr: StderrTail) -> tuple[int | None, str]:
    """End a worker that serves no request (it holds no PDF): killed and reaped; its exit code and stderr's tail."""
    child.kill()  # a no-op once it has exited
    close_pipes(child)
    exitcode = reaped(child, REAP_TIMEOUT_S)
    return exitcode, stderr.finish(REAP_TIMEOUT_S)


def _retired_ending(child: "subprocess.Popen[bytes]", stderr: StderrTail) -> str:
    """Retire a spare that ended on its own (:func:`_retired`): how it ended, in words."""
    exitcode, tail = _retired(child, stderr)
    ending = f"it ended with exit code {exitcode}" if exitcode is not None else "it could not be reaped"
    return f"{ending}; its stderr ends: {tail}" if tail else ending


_ACTIVE_RENDER_POOL: ContextVar[RenderWorkerPool | None] = ContextVar("_ACTIVE_RENDER_POOL", default=None)


def active_render_worker_pool() -> RenderWorkerPool | None:
    """Return the active :class:`RenderWorkerPool` for the current context, or None."""
    return _ACTIVE_RENDER_POOL.get()


@contextmanager
def use_render_worker_pool(pool: RenderWorkerPool | None) -> Iterator[None]:
    """Bind ``pool`` as the active :class:`RenderWorkerPool` in the current context."""
    token = _ACTIVE_RENDER_POOL.set(pool)
    try:
        yield
    finally:
        _ACTIVE_RENDER_POOL.reset(token)


@contextmanager
def render_worker_pool() -> Iterator[RenderWorkerPool]:
    """Open a :class:`RenderWorkerPool` for the block and close all pooled workers on exit."""
    with RenderWorkerPool() as pool, use_render_worker_pool(pool):
        yield pool


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
    pool = _ACTIVE_RENDER_POOL.get()
    if pool is not None:
        return _render_page_pooled(pool, command, payload, request, where)
    return _render_page_fresh(command, payload, request, where)


def _render_page_fresh(command: list[str], payload: bytes, request: _Request, where: str) -> RenderedPage:
    """The page drawn by a worker started for it, within ``timeout_s`` from now (see :func:`render_page`)."""
    deadline = time.monotonic() + request.timeout_s
    worker, stderr = _started(command, where)
    try:
        reply, timed_out, exitcode = _supervised(worker, payload, request.memory_cap_bytes, deadline)
    finally:
        close_pipes(worker)
        tail = stderr.finish(REAP_TIMEOUT_S)  # the worker is reaped: its drain ends
    return _outcome(reply, timed_out, exitcode, request, "" if reply is not None else tail)


def _render_page_pooled(
    pool: RenderWorkerPool, command: list[str], payload: bytes, request: _Request, where: str
) -> RenderedPage:
    """The page drawn by a spare from ``pool`` (:func:`_pooled`): final once the spare has the request."""
    read = partial(_one_reply, memory_cap_bytes=request.memory_cap_bytes)
    served = _pooled(pool, where, _pool_message(request), request, 1, read)
    if served is None:  # the spare died while idle: a worker started for the page draws it, with its full timeout
        return _render_page_fresh(command, payload, request, where)
    replies, timed_out, exitcode, tail = served
    return _outcome(replies[0] if replies else None, timed_out, exitcode, request, tail)


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
    batch goes on. When the worker ends (its cap, the timeout, an unexpected exit), no
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
        RenderFailed: If no worker can start (no interpreter, no thread to
            drain its stderr, the OS refuses), or the request is over its bound.
    """
    try:
        command = worker_command()
    except NoInterpreter as exc:
        raise RenderFailed(str(exc)) from exc
    payload = _batch_payload(request)
    count, channels = len(request.pages), BGR_CHANNELS if request.colour else GRAY_CHANNELS
    pool = _ACTIVE_RENDER_POOL.get()
    if pool is not None:
        return _run_batch_pooled(pool, command, payload, request, count, channels)
    return _run_batch_fresh(command, payload, request, count, channels)


def _run_batch_fresh(
    command: list[str], payload: bytes, request: _BatchRequest, count: int, channels: int
) -> tuple[list[_Reply], bool, int | None, str]:
    """The batch drawn by a worker started for it, within ``timeout_s`` from now (see :func:`_run_batch`)."""
    deadline = time.monotonic() + request.timeout_s
    worker, stderr = _started(command, "")
    exchange = partial(_exchange_batch, worker.stdin, worker.stdout, payload, request.memory_cap_bytes, channels, count)
    try:
        replies, timed_out, exitcode = _watched(worker, deadline, count, exchange)
    finally:
        close_pipes(worker)
        tail = stderr.finish(REAP_TIMEOUT_S)  # the worker is reaped: its drain ends
    return replies, timed_out, exitcode, "" if len(replies) == count else tail


def _run_batch_pooled(
    pool: RenderWorkerPool, command: list[str], payload: bytes, request: _BatchRequest, count: int, channels: int
) -> tuple[list[_Reply], bool, int | None, str]:
    """The batch drawn by a spare from ``pool`` (:func:`_pooled`): final once the spare has the request."""
    read = partial(_read_replies, memory_cap_bytes=request.memory_cap_bytes, channels=channels, count=count)
    served = _pooled(pool, "", _pool_message(request), request, count, read)
    if served is None:  # the spare died while idle: a worker started for the batch draws it, with its full timeout
        return _run_batch_fresh(command, payload, request, count, channels)
    return served


def _pool_message(request: _Request | _BatchRequest) -> bytes:
    """The request a spare reads (it has the caller's sys.path already): its JSON alone, within the payload's bound."""
    return json.dumps(asdict(request)).encode("ascii")


def _pooled(
    pool: RenderWorkerPool,
    where: str,
    message: bytes,
    request: _Request | _BatchRequest,
    count: int,
    read: Callable[[IO[bytes]], list[_Reply]],
) -> tuple[list[_Reply], bool, int | None, str] | None:
    """Exchange ``message`` with a spare from ``pool`` until its ``count`` replies or ``timeout_s`` from now.

    Returns what the fresh path returns: the replies, whether the deadline
    passed, the exit code and stderr's tail; final once the spare has the
    request. The spare serves this request alone: a timer kills it at the
    deadline, and it is reaped once its replies end (it exits by itself
    after them), never returned to the pool. None if the request never
    reached the spare (it died while idle; logged): the caller then starts
    a worker for it, with a full ``timeout_s`` of its own.
    """
    deadline = time.monotonic() + request.timeout_s
    worker, stderr = pool.acquire(where, request.memory_cap_bytes)
    delivered = False

    def exchange() -> list[_Reply]:
        nonlocal delivered
        replies, delivered = _exchange_pool(worker.stdin, worker.stdout, message, read)
        return replies

    try:
        replies, timed_out, exitcode = _watched(worker, deadline, count, exchange)
    finally:
        close_pipes(worker)
        tail = stderr.finish(REAP_TIMEOUT_S)  # the spare is reaped: its drain ends
    if not delivered and not timed_out:
        said = f"; its stderr ends: {tail}" if tail else ""
        _LOGGER.warning(
            "a spare render worker ended while idle (exit code %s)%s; a new worker draws %s",
            exitcode,
            said,
            where or request.source,
        )
        return None
    return replies, timed_out, exitcode, "" if len(replies) == count else tail


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
        return worker_command()
    except NoInterpreter as exc:
        raise RenderFailed(f"{where}: {exc}") from exc


def _request_payload(request: _Request) -> bytes:
    """The JSON the worker reads from its stdin: the caller's exact sys.path, the worker's key and the request."""
    try:
        return request_payload("render", asdict(request), _MAX_REQUEST_BYTES)
    except ValueError as exc:
        raise RenderFailed(str(exc)) from exc


def _started(command: list[str], where: str) -> tuple["subprocess.Popen[bytes]", StderrTail]:
    """Start the worker, its pipes to this process, and its stderr's drain; ``where`` (if any) heads an error.

    Raises:
        RenderFailed: If the OS refuses to start it, or no thread can drain
            its stderr (it is then killed and reaped).
    """
    try:
        return start_worker(command)
    except (OSError, RuntimeError) as exc:
        lead = f"{where}: " if where else ""
        raise RenderFailed(f"{lead}cannot start a render worker: {exc}") from exc


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


def _exchange_pool(
    stdin: IO[bytes] | None, stdout: IO[bytes] | None, message: bytes, read: Callable[[IO[bytes]], list[_Reply]]
) -> tuple[list[_Reply], bool]:
    """Send ``message`` to a spare, then ``read`` its replies; and whether the request was delivered.

    A spare that died while idle has no reader on its stdin: the request is
    undelivered, and nothing is read (``[]``, False). Once delivered, the
    replies are read as a fresh worker's are (:func:`_one_reply`,
    :func:`_read_replies`); then one byte more is waited for, which is the
    end of its stdout: a spare exits once it has replied, so it is then
    reaped at once, not by polling. That wait is within the caller's
    deadline timer, like every read here.
    """
    if stdin is None or stdout is None:
        raise ValueError("the worker's stdin and stdout must be pipes")
    if not _written(stdin, LENGTH.pack(len(message)) + message):
        return [], False
    replies = read(stdout)
    stdout.read(1)  # its end, once it has exited; a byte past its replies changes nothing: it is not read on
    return replies, True


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
    except OSError:  # the worker stopped reading (it died, or was killed)
        return None
    return _read_single(stdout, memory_cap_bytes)


def _read_single(stdout: IO[bytes], memory_cap_bytes: int) -> _Reply | None:
    """A worker's whole reply to one page's request, or None if none came whole (see :func:`_exchange`)."""
    try:
        return _read_reply(stdout, memory_cap_bytes, GRAY_CHANNELS)
    except _Malformed as exc:
        return _Reply(_FAILED, str(exc), None)
    except (OSError, MessageTooLong):  # the worker stopped writing (it died, or was killed), or overran a bound
        return None


def _one_reply(stdout: IO[bytes], *, memory_cap_bytes: int) -> list[_Reply]:
    """:func:`_read_single`'s reply, as a list: empty if none came whole."""
    reply = _read_single(stdout, memory_cap_bytes)
    return [] if reply is None else [reply]


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
    try:
        stdin.write(payload)
        stdin.close()
    except OSError:  # it stopped reading (it died, or was killed): each page fails with why it ended
        return []
    return _read_replies(stdout, memory_cap_bytes=memory_cap_bytes, channels=channels, count=count)


def _read_replies(stdout: IO[bytes], *, memory_cap_bytes: int, channels: int, count: int) -> list[_Reply]:
    """A batch worker's replies, one a page, in order, up to ``count`` (see :func:`_exchange_batch`)."""
    replies: list[_Reply] = []
    try:
        for _ in range(count):  # bounded: one reply a page
            reply = _read_reply(stdout, memory_cap_bytes, channels)
            if reply is None:
                return replies
            replies.append(reply)
    except (_Malformed, MessageTooLong) as exc:
        replies.append(_Reply(_FAILED, str(exc), None))
    except OSError:  # it stopped writing (it died, or was killed): each page left fails with why it ended
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
    if kind == _ERROR:  # the worker could not import the [pdf] extra: no page of it renders, so it is raised
        raise _read_error(status[1:], "the render worker")
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
    return f"{width} x {height} px" + (" BGR" if channels == BGR_CHANNELS else "")


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
    return _worker_ending(exitcode, "the render worker", request.memory_cap_bytes)
