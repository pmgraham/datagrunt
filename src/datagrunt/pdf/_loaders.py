"""The lazy loaders a document's figures and page images carry (#369 A5a, #371): the caller's side, no pdfium.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A loader
holds where its bytes are (the PDF's path, the page, the image's facts, the
password) and reads them in a worker process whose memory is capped
(:mod:`datagrunt.pdf._memory_cap`), so the caller never hands the PDF to
pdfium. Each is picklable: a page worker builds them, and its pages cross
to the caller with them.

- :class:`PageRender`: a vision page's image and bands, drawn again in the
  render worker (:func:`~datagrunt.pdf.render.render_page`).
- :class:`PageDraw`: a digital or hybrid page's image (P2-A, spec §3.2),
  drawn in the render worker at the reader's effective ``dpi`` only when it
  is read; it has no bands.
- :class:`FigureRead`: a figure's PNG (:meth:`FigureRead.png_bytes`) or raw
  stream (:meth:`FigureRead.raw_bytes`), read in a figure worker (the
  launcher's key ``"figures"``, :mod:`datagrunt.pdf._figure_worker`) by
  the engine's own :class:`~datagrunt.pdf.pdfium_engine.ImageLoader`, which
  verifies the image against its facts before it returns anything.

Each page image's draw is bound to the bytes its document was extracted
from (#453): a :class:`PageRender` or :class:`PageDraw` holds the
:class:`~datagrunt.pdf._source.SourceIdentity` of that file and the
document's sha256, the pipeline's, taken together
(:func:`~datagrunt.pdf._source.check_binding`). Identity is the fast path;
content decides. Each draw stats the path first: a file of that identity
is drawn. A file of another identity (touched; rewritten with the same
bytes by a checkout, a sync tool or a copy back; read on another host,
whose ``st_dev`` is its own) is hashed as the pipeline hashes it, through
one descriptor whose ``fstat`` must be the stat just taken
(:func:`~datagrunt.pdf._source._stream_sha256`), and is drawn only when its
bytes hash to the document's sha256. After the render the path is stat'ed
again and must be the file stat'ed before it: nothing is hashed then. A
file gone or not a regular file (a directory, a named pipe: it is never
opened blocking, :func:`~datagrunt.pdf._source._opened_source`), of other
bytes, replaced between the stat and the hash, cut short as it is hashed,
or changed during the render raises
:class:`~datagrunt.pdf.exceptions.PDFSourceChangedError`, naming the path,
the page and what changed (for other bytes, that they are not the
document's): no image of other bytes than the document's pages is
returned. A loader bound to neither checks nothing. A draw of a file of
another identity reads the whole file first, each time; a bundle write,
which has hashed the file already, draws through copies bound to the
identity it hashed (:func:`~datagrunt.pdf.bundle._draws_bound`), so it
hashes the file once. What this cannot see is the pipeline's limit: a
write that keeps the size and restores the mtime, or a swap away and back
between the stats.

Figure reads are batched: :func:`read_figures` reads up to
:data:`FIGURE_BATCH` figures of one PDF in one worker, and
:func:`figure_pngs` batches a document's figure reads for ``PDFWriter``'s
``extract_images`` and the bundle. A single lazy read is a batch of one.
Each read's outcome comes in order, as its worker sends it: the caller
holds one figure's bytes at a time. Each reply is read within its bound
(:data:`_FIGURE_START_S` for a worker's first, then :data:`_FIGURE_READ_S`
a figure, each counted from the reply before it); a reply longer than the
asset limit is refused unread. A worker that ends early (its cap, a reply
past its bound, an unexpected exit) fails the figure it was reading with
why, a :class:`~datagrunt.pdf.exceptions.DatagruntPDFError`, and the
figures after it are read by a new worker: each worker answers or fails at
least one figure, so a batch starts at most as many workers as it holds
figures. An error a figure's read raises (the file changed, a decode
refused) is that figure's outcome, raised as it was.
"""

import dataclasses
import os
import stat
import subprocess
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import IO, BinaryIO, Literal, NamedTuple

from datagrunt.pdf._image_facts import RawImage
from datagrunt.pdf._memory_cap import ending
from datagrunt.pdf._options import _RENDER_TIMEOUT_S, MEMORY_CAP_BYTES
from datagrunt.pdf._png import grey_png
from datagrunt.pdf._process import (
    MAX_REQUEST_BYTES,
    REAP_TIMEOUT_S,
    MessageTooLong,
    NoInterpreter,
    StderrTail,
    close_pipes,
    next_reply,
    read_error,
    reaped,
    request_payload,
    start_worker,
    worker_command,
)
from datagrunt.pdf._source import SourceIdentity, _opened_source, _stream_sha256, check_binding
from datagrunt.pdf._validate import checked_password, checked_path, int_at_least, positive_float
from datagrunt.pdf.elements import _MAX_ASSET_BYTES, Figure
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFSourceChangedError
from datagrunt.pdf.ocr import Band, bands, normalized, rotated
from datagrunt.pdf.render import RenderedPage, render_page, render_size

__all__ = ["FIGURE_BATCH", "FigureRead", "PageDraw", "PageRender", "figure_pngs", "read_figures"]

_TURNS = (0, 90, 180, 270)

# The figures one worker reads, at most: a batch's request is ~0.5 KB a figure, within the launcher's 1 MiB bound.
FIGURE_BATCH = 64
# A figure worker's first reply is given this to start, and each figure this. Measured here: a worker starts in ~0.2
# s and reads a figure in 12-67 ms (a JPEG 2000 image at the pixel bound in seconds), so each bound is over 100 times
# what it covers.
_FIGURE_START_S = 60.0
_FIGURE_READ_S = 60.0
_EXIT_GRACE_S = 1.0  # a worker whose replies ended early is given this to exit
# A figure worker's replies: the bytes (this kind, then the bytes as a second message), or the read's error, pickled.
_BYTES, _ERROR = b"B", b"E"
_KINDS = ("png", "raw")
Kind = Literal["png", "raw"]
# Why a page image of a file that is not its document's is refused: each PDFSourceChangedError of a draw ends so.
_NOT_RETURNED = "the page image and the document's pages could disagree, so it is not returned"


def _checked_turn(rotation: object) -> int:
    """``rotation`` if it is an int of 0, 90, 180 or 270 (bool rejected)."""
    if type(rotation) is not int or rotation not in _TURNS:
        raise ValueError(f"rotation must be an int of 0, 90, 180 or 270, got {rotation!r}")
    return rotation


@dataclass(frozen=True, slots=True, kw_only=True)
class PageRender:
    """How to draw a vision page again: a :class:`~datagrunt.pdf.elements.PageImage`'s loaders.

    It holds no pixels: each call renders the page in the guarded worker,
    normalizes it as OCR read it, and encodes PNG (8-bit grey). Picklable,
    so a page crosses the process pool. Each load draws only a file at
    ``source`` that is ``identity``'s or whose bytes hash to ``sha256``,
    checked before and after its render (module notes), and raises as
    :func:`~datagrunt.pdf.render.render_page` does (``RenderRefused``,
    ``RenderFailed``).

    Attributes:
        source: The PDF's absolute path.
        page_index: The 0-based page.
        dpi: The render's DPI (the page's effective DPI).
        rotation: The clockwise turn OCR read the page at: the bands' turn.
        password: The PDF's password, or None. Not in repr.
        memory_cap_bytes: The render worker's memory cap past its start.
        timeout_s: Seconds a render may take.
        identity: The identity of the file the document was extracted from;
            given with ``sha256`` or not at all.
        sha256: The document's sha256: a file at ``source`` of another
            identity is drawn only when its bytes hash to it. With
            ``identity`` None, None: nothing is checked.

    Raises:
        TypeError, ValueError: If a field is invalid, or only one of
            ``identity`` and ``sha256`` is given.
    """

    source: Path
    page_index: int
    dpi: int
    rotation: int
    password: str | None = field(default=None, repr=False)
    memory_cap_bytes: int
    timeout_s: float
    identity: SourceIdentity | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", checked_path(self.source, "source").absolute())
        int_at_least("page_index", self.page_index, 0)
        int_at_least("dpi", self.dpi, 1)
        _checked_turn(self.rotation)
        checked_password(self.password)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)
        object.__setattr__(self, "timeout_s", positive_float("timeout_s", self.timeout_s))
        check_binding(self.identity, self.sha256)

    def png_bytes(self) -> bytes:
        """The page as displayed (before OCR's turn), normalized, as PNG.

        Raises:
            PDFSourceChangedError: If the file at ``source`` is gone, not a regular file or its bytes are not the
                document's before the render, or it changed during it (module notes).
        """
        return _png(self._drawn())

    def band_png_bytes(self) -> tuple[bytes, ...]:
        """The bands OCR read (of the page turned by ``rotation``, normalized), top to bottom, each as PNG.

        Raises:
            PDFSourceChangedError: As :meth:`png_bytes` raises it.
        """
        return tuple(_png(band) for band in bands(rotated(self._drawn(), self.rotation)))

    def _drawn(self) -> RenderedPage:
        return normalized(_bound_render(self))


@dataclass(frozen=True, slots=True, kw_only=True)
class PageDraw:
    """How to draw a digital or hybrid page (P2-A): a :class:`~datagrunt.pdf.elements.PageImage`'s loader.

    It holds no pixels: each call renders the page in the guarded worker
    (:func:`~datagrunt.pdf.render.render_page`) and encodes PNG, 8-bit grey
    as drawn, as the bundle's renders are (no normalization: that is OCR's).
    It has no band loader: no OCR read the page. Its size is
    :func:`~datagrunt.pdf.render.render_size`'s of the page at ``dpi``, known
    before any draw, and each draw must be that size. Picklable, so a page
    crosses the process pool. Each load draws only a file at ``source``
    that is ``identity``'s or whose bytes hash to ``sha256``, checked before
    and after its render (module notes), and raises as ``render_page`` does
    (``RenderRefused``, ``RenderFailed``).

    Attributes:
        source: The PDF's absolute path.
        page_index: The 0-based page.
        dpi: The render's DPI: the effective DPI of the reader's ``dpi``
            (:func:`~datagrunt.pdf.render.effective_dpi`).
        width_pt: The page's displayed width.
        height_pt: The page's displayed height.
        password: The PDF's password, or None. Not in repr.
        memory_cap_bytes: The render worker's memory cap past its start.
        timeout_s: Seconds a render may take.
        identity: The identity of the file the document was extracted from;
            given with ``sha256`` or not at all.
        sha256: The document's sha256: a file at ``source`` of another
            identity is drawn only when its bytes hash to it. With
            ``identity`` None, None: nothing is checked.

    Raises:
        TypeError, ValueError: If a field is invalid, only one of
            ``identity`` and ``sha256`` is given, or the page draws under one
            pixel across at ``dpi``.
    """

    source: Path
    page_index: int
    dpi: int
    width_pt: float
    height_pt: float
    password: str | None = field(default=None, repr=False)
    memory_cap_bytes: int = MEMORY_CAP_BYTES
    timeout_s: float = _RENDER_TIMEOUT_S
    identity: SourceIdentity | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", checked_path(self.source, "source").absolute())
        int_at_least("page_index", self.page_index, 0)
        object.__setattr__(self, "width_pt", positive_float("width_pt", self.width_pt))
        object.__setattr__(self, "height_pt", positive_float("height_pt", self.height_pt))
        width, height = render_size(self.width_pt, self.height_pt, int_at_least("dpi", self.dpi, 1))
        if width < 1 or height < 1:
            side = "wide" if width < 1 else "tall"
            raise ValueError(f"a {self.width_pt} x {self.height_pt} pt page draws 0 px {side} at {self.dpi} DPI")
        checked_password(self.password)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)
        object.__setattr__(self, "timeout_s", positive_float("timeout_s", self.timeout_s))
        check_binding(self.identity, self.sha256)

    @property
    def size(self) -> tuple[int, int]:
        """The page's ``(width, height)`` in pixels at ``dpi``."""
        return render_size(self.width_pt, self.height_pt, self.dpi)

    def render_page(self) -> RenderedPage:
        """The page as displayed, 8-bit grey as drawn, as a :class:`~datagrunt.pdf.render.RenderedPage`.

        Raises:
            PDFSourceChangedError: If the file at ``source`` is gone, not a
                regular file or its bytes are not the document's before the
                render, or it changed during it (module notes).
            DatagruntPDFError: If the page draws at another size than
                :attr:`size` (the PDF is not the one read), or as
                ``render_page`` raises (``RenderRefused``, ``RenderFailed``).
        """
        page = _bound_render(self)
        if (page.px_width, page.px_height) != self.size:
            width, height = self.size
            drew = f"page {self.page_index + 1} draws {page.px_width} x {page.px_height} px"
            raise DatagruntPDFError(
                f"{self.source}: {drew}, not the {width} x {height} px of its page image: the PDF is not the one read"
            )
        return page

    def png_bytes(self) -> bytes:
        """The page as displayed, 8-bit grey as drawn, as PNG.

        Raises:
            PDFSourceChangedError: If the file at ``source`` is gone, not a
                regular file or its bytes are not the document's before the
                render, or it changed during it (module notes).
            DatagruntPDFError: If the page draws at another size than
                :attr:`size` (the PDF is not the one read), or as
                ``render_page`` raises (``RenderRefused``, ``RenderFailed``).
        """
        return _png(self.render_page())


def _bound_render(loader: PageRender | PageDraw) -> RenderedPage:
    """The loader's page, drawn by ``render_page`` only from a file of its document's bytes (module notes)."""
    image = f"page {loader.page_index + 1}'s image"
    before = _drawable(loader, f"before {image} was drawn")
    page = render_page(
        loader.source,
        loader.page_index,
        dpi=loader.dpi,
        password=loader.password,
        memory_cap_bytes=loader.memory_cap_bytes,
        timeout_s=loader.timeout_s,
    )
    _check_unchanged(loader.source, before, f"while {image} was drawn")
    return page


def _drawable(loader: PageRender | PageDraw, when: str) -> SourceIdentity | None:
    """The identity of the file at the loader's source if it holds the document's bytes; None if it is bound to none.

    The file of the loader's identity holds them; a file of another identity
    holds them when its bytes hash to the loader's sha256 (module notes).

    Raises:
        PDFSourceChangedError: If the file is gone or not a regular file, or
            of another identity and bytes that are not the document's (naming
            ``when``, what changed and both hashes), or replaced or cut short
            as it is hashed.
        OSError: If the file cannot be stat'ed or read for another reason
            (``PermissionError``, say).
    """
    if loader.identity is None or loader.sha256 is None:
        return None
    now = _identity_at(loader.source, when)
    changes = loader.identity.changes(now)
    if not changes:
        return now
    digest = _sha256_at(loader.source, now, when)
    if digest != loader.sha256:
        mismatch = f"its bytes are not the document's (sha256 {digest}, not {loader.sha256})"
        raise _changed(loader.source, when, changes, mismatch)
    return now


def _identity_at(source: Path, when: str) -> SourceIdentity:
    """The identity of the regular file at ``source`` now, from one ``stat``.

    Raises:
        PDFSourceChangedError: If no file is there, or what is there is not a
            regular file (a directory, a named pipe), naming ``when``.
        OSError: If ``source`` cannot be stat'ed for another reason.
    """
    try:
        status = os.stat(source)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise _removed(source, when) from exc
    if not stat.S_ISREG(status.st_mode):
        raise _not_regular(source, when)
    return SourceIdentity.of(status)


def _check_unchanged(source: Path, before: SourceIdentity | None, when: str) -> None:
    """Raise PDFSourceChangedError if the file at ``source`` is not the one ``before`` was taken of; nothing without."""
    if before is not None:
        _check_same(source, before, _identity_at(source, when), when)


def _check_same(source: Path, before: SourceIdentity, now: SourceIdentity, when: str) -> None:
    """Raise PDFSourceChangedError, naming what changed, if ``now`` is not ``before``."""
    changes = before.changes(now)
    if changes:
        raise _changed(source, when, changes)


def _sha256_at(source: Path, status: SourceIdentity, when: str) -> str:
    """The SHA-256 of the file at ``source``, read through one descriptor that must be the file ``status`` is of.

    Raises:
        PDFSourceChangedError: If the file is gone or not a regular file when
            opened, is not ``status``'s (its ``fstat``), or is cut short as it
            is hashed.
        OSError: If it cannot be opened or read for another reason.
    """
    try:
        stream = _opened_source(source)
    except FileNotFoundError as exc:
        raise _removed(source, when) from exc
    except ValueError as exc:  # not a regular file: put there since the stat
        raise _not_regular(source, when) from exc
    with stream:
        return _hashed(stream, source, status, when)


def _hashed(stream: BinaryIO, source: Path, status: SourceIdentity, when: str) -> str:
    """The SHA-256 of ``stream``'s first ``status.size`` bytes, if ``fstat`` of it is ``status``."""
    _check_same(source, status, SourceIdentity.of(os.fstat(stream.fileno())), when)
    try:
        return _stream_sha256(stream, status.size, source)
    except PDFSourceChangedError as exc:  # cut short: its words, and the draw's
        raise PDFSourceChangedError(f"{exc} {when}; {_NOT_RETURNED}") from exc


def _changed(source: Path, when: str, changes: tuple[str, ...], *why: str) -> PDFSourceChangedError:
    """The error of a draw that found the file at ``source`` changed ``when``: what changed, why, and so no image."""
    return PDFSourceChangedError("; ".join((f"{source} changed {when}: {', '.join(changes)}", *why, _NOT_RETURNED)))


def _not_regular(source: Path, when: str) -> PDFSourceChangedError:
    """The error of a draw that found something other than a regular file at ``source`` ``when``."""
    return _changed(source, when, ("it is not a regular file",))


def _removed(source: Path, when: str) -> PDFSourceChangedError:
    """The error of a draw that found nothing at ``source`` ``when``."""
    return PDFSourceChangedError(f"{source} was removed {when}; {_NOT_RETURNED}")


def _png(image: RenderedPage | Band) -> bytes:
    """8-bit grey pixels as PNG."""
    return grey_png(image.pixels, image.px_width, image.px_height)


@dataclass(frozen=True, slots=True, kw_only=True)
class FigureRead:
    """How to read a figure again: a :class:`~datagrunt.pdf.elements.Figure`'s loaders, in a capped worker.

    It holds no bytes: each call reads the image in a figure worker, which
    reopens ``source`` (with ``password``), finds the object by the walk that
    numbered it and checks it is still ``image`` (its raw stream's hash and
    its pixel facts) before it returns anything, as the engine's
    :class:`~datagrunt.pdf.pdfium_engine.ImageLoader` does. Picklable, so a
    page crosses the process pool with its figures.

    Attributes:
        source: The PDF's path, made absolute at construction.
        page_index: The 0-based page holding the image.
        image: The image's facts as the engine recorded them; it must have a
            ``raw_sha256`` (its stream was read).
        password: The encrypted PDF's password, or None. Excluded from repr
            and equality, and never put in an error message; a pickle of the
            loader carries it (the worker needs it).
        memory_cap_bytes: The figure worker's memory cap past its start.
    """

    source: Path
    page_index: int
    image: RawImage
    password: str | None = field(default=None, repr=False, compare=False)
    memory_cap_bytes: int = MEMORY_CAP_BYTES

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", checked_path(self.source, "source").absolute())
        int_at_least("page_index", self.page_index, 0)
        if not isinstance(self.image, RawImage):
            raise TypeError(f"image must be a RawImage, got {type(self.image).__name__}")
        if self.image.raw_sha256 is None:
            raise ValueError(
                "image has no raw_sha256 (its stream was not read): there is nothing to verify a read against"
            )
        checked_password(self.password)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)

    @property
    def object_index(self) -> int:
        """The image object's index in its page's object walk."""
        return self.image.object_index

    @property
    def raw_sha256(self) -> str:
        """The SHA-256 every read verifies the image's raw stream against."""
        digest = self.image.raw_sha256
        if digest is None:  # refused at construction; this narrows the type
            raise ValueError("image has no raw_sha256")
        return digest

    def png_bytes(self) -> bytes:
        """The image decoded by pdfium at its own pixel size, as PNG bytes (``ImageLoader.png_bytes``'s, in a worker).

        Raises:
            DatagruntPDFError: As ``ImageLoader.png_bytes`` raises it, or if
                its worker cannot start, reaches its memory cap, runs past
                its bound or ends without a reply (saying why).
            PDFCorruptError, FileNotFoundError, PermissionError, PDFEncryptedError:
                As ``ImageLoader.png_bytes`` raises them.
        """
        return _read_one(self, "png")

    def raw_bytes(self) -> bytes:
        """The image's raw stream, verified (``ImageLoader.raw_bytes``'s, in a worker); raises as :meth:`png_bytes`."""
        return _read_one(self, "raw")

    def where(self) -> str:
        """The image in words, for an error: its PDF, page and object (never the password)."""
        return f"{self.source}: page {self.page_index + 1}, object {self.image.object_index}"


def _read_one(read: FigureRead, kind: Kind) -> bytes:
    """One figure's bytes, read in a worker of its own; its error raised."""
    (outcome,) = read_figures((read,), kind)
    if isinstance(outcome, Exception):
        raise outcome
    return outcome


def figure_pngs(figures: Sequence[Figure]) -> Iterator[Callable[[], bytes]]:
    """Each figure's PNG read, in order, as a callable that returns its bytes or raises its error.

    The figures whose PNG loader is a :class:`FigureRead`'s are read in
    batches (:func:`read_figures`), each when its turn comes; any other
    figure (a bundle's, read from its file) is read by its own
    ``read_bytes``.
    """
    index = 0
    while index < len(figures):  # bounded: each pass consumes at least one figure
        run = _run_of_reads(figures, index)
        if not run:
            yield figures[index].read_bytes
            index += 1
            continue
        for outcome in read_figures(run, "png"):  # bounded: one outcome a read
            yield partial(_outcome_of, outcome)
        index += len(run)


def _run_of_reads(figures: Sequence[Figure], start: int) -> list[FigureRead]:
    """The FigureReads of the figures from ``start`` whose PNG loader is a FigureRead's, until one is not."""
    run: list[FigureRead] = []
    for figure in figures[start:]:  # bounded: the figures
        owner = getattr(figure._loader, "__self__", None)
        if not isinstance(owner, FigureRead) or getattr(figure._loader, "__func__", None) is not FigureRead.png_bytes:
            break
        run.append(owner)
    return run


def _outcome_of(outcome: bytes | Exception) -> bytes:
    """``outcome``'s bytes; its error raised."""
    if isinstance(outcome, Exception):
        raise outcome
    return outcome


def read_figures(reads: Sequence[FigureRead], kind: Kind) -> Iterator[bytes | Exception]:
    """Each read's bytes (``kind``: ``"png"`` or ``"raw"``), or the exception it raised, in order (module notes).

    Consecutive reads of one PDF (its path, password and memory cap) share a
    worker, at most :data:`FIGURE_BATCH` of them.

    Raises:
        ValueError: If ``kind`` is neither ``"png"`` nor ``"raw"``.
        TypeError: If a read is not a FigureRead.
    """
    if kind not in _KINDS:
        raise ValueError(f"kind must be one of {_KINDS}, got {kind!r}")
    for read in reads:
        if not isinstance(read, FigureRead):
            raise TypeError(f"each read must be a FigureRead, got {type(read).__name__}")
    for batch in _batches(reads):  # bounded: the reads, FIGURE_BATCH at a time
        yield from _batch_outcomes(batch, kind)


def _batches(reads: Sequence[FigureRead]) -> Iterator[list[FigureRead]]:
    """``reads`` in runs that share a worker: the same PDF, password and cap, at most FIGURE_BATCH long."""
    batch: list[FigureRead] = []
    for read in reads:
        if batch and (len(batch) == FIGURE_BATCH or _worker_key(batch[0]) != _worker_key(read)):
            yield batch
            batch = []
        batch.append(read)
    if batch:
        yield batch


def _worker_key(read: FigureRead) -> tuple[Path, str | None, int]:
    return read.source, read.password, read.memory_cap_bytes


def _batch_outcomes(batch: list[FigureRead], kind: Kind) -> Iterator[bytes | Exception]:
    """The batch's outcomes, in order: one worker, then a new one for the reads after any it fails (module notes)."""
    done = 0
    for _ in range(len(batch)):  # bounded: each worker answers or fails at least one read
        if done == len(batch):
            return
        for outcome in _worker_outcomes(batch[done:], kind):
            done += 1
            yield outcome


def _worker_outcomes(reads: list[FigureRead], kind: Kind) -> Iterator[bytes | Exception]:
    """What one worker answers for ``reads``, in order; when it ends early, the failure of the read it was on.

    Yields at least one outcome. A worker that cannot start fails every read
    with why. The worker is reaped before this ends, on every path: waited
    for once it has answered each read, else killed first.
    """
    try:
        command = worker_command()
    except NoInterpreter as exc:
        yield from (DatagruntPDFError(str(exc)) for _ in reads)
        return
    payload = _batch_payload(reads, kind)
    started = time.monotonic()
    try:
        child, stderr = start_worker(command)
    except (OSError, RuntimeError) as exc:
        yield from (DatagruntPDFError(f"cannot start a figure worker: {exc}") for _ in reads)
        return
    answered = 0
    try:
        _write_request(child.stdin, payload)
        for outcome in _answers(child, stderr, reads, started):  # bounded: one outcome a read
            answered += 1
            yield outcome
    finally:
        if answered < len(reads):  # ended early, or its reader stopped: it is not waited for
            child.kill()
        close_pipes(child)
        reaped(child, REAP_TIMEOUT_S)
        stderr.finish(REAP_TIMEOUT_S)


def _write_request(stdin: IO[bytes] | None, payload: bytes) -> None:
    """Write the request to the worker's stdin and close it; a worker that ended at once is reported by its replies."""
    if stdin is None:
        raise ValueError("the worker's stdin must be a pipe")
    try:
        stdin.write(payload)
        stdin.close()
    except OSError:  # it ended at once: its replies end, and its exit code and stderr say why
        pass


class _Lost(NamedTuple):
    """No answer came for a read: why, in words, or ``""`` when the worker ended (its exit code and stderr say why)."""

    why: str


def _answers(
    child: "subprocess.Popen[bytes]", stderr: StderrTail, reads: list[FigureRead], started: float
) -> Iterator[bytes | Exception]:
    """The worker's outcome for each read, each within its bound; at its early end, the read it was on fails."""
    stdout = child.stdout
    if stdout is None:
        raise ValueError("the worker's stdout must be a pipe")
    since = started
    for position, read in enumerate(reads):  # bounded: one reply a read
        bound_s = _FIGURE_READ_S + (_FIGURE_START_S if position == 0 else 0.0)
        outcome = _answer(child, stdout, since + bound_s, bound_s)
        since = time.monotonic()
        if isinstance(outcome, _Lost):
            why = outcome.why or _ended(child, stderr, read)
            yield DatagruntPDFError(f"{read.where()}: {why}")
            return
        yield outcome


def _answer(
    child: "subprocess.Popen[bytes]", stdout: IO[bytes], due: float, bound_s: float
) -> bytes | Exception | _Lost:
    """One read's outcome from the worker by ``due`` (a monotonic time); or why none came."""
    try:
        head, expired = next_reply(child, stdout, due - time.monotonic(), _MAX_ASSET_BYTES)
        if head is not None and head[:1] == _ERROR:
            return read_error(head[1:], "the figure worker")
        if head is not None and head != _BYTES:
            child.kill()
            return _Lost(f"the figure worker sent a reply of unknown kind {head[:1]!r}: it was killed")
        data, expired = (
            (None, expired) if head is None else next_reply(child, stdout, due - time.monotonic(), _MAX_ASSET_BYTES)
        )
    except MessageTooLong as exc:
        child.kill()  # it may be blocked writing the rest
        return _Lost(f"the figure worker sent {exc}: it was killed")
    if data is None:
        return _Lost(f"the figure worker did not return it within {bound_s:g} s: it was killed" if expired else "")
    return data


def _ended(child: "subprocess.Popen[bytes]", stderr: StderrTail, read: FigureRead) -> str:
    """Why the figure worker ended before it answered ``read``: its exit code and its stderr's last 4 KiB."""
    said = ending(reaped(child, _EXIT_GRACE_S), "the figure worker", read.memory_cap_bytes)
    tail = stderr.finish(REAP_TIMEOUT_S)  # it is reaped: its drain ends
    return f"{said}; its stderr ends: {tail}" if tail else said


def _batch_payload(reads: Sequence[FigureRead], kind: Kind) -> bytes:
    """The JSON a figure worker reads from its stdin: one PDF's reads and a lifetime (the password on stdin only)."""
    first = reads[0]
    request = {
        "source": str(first.source),
        "password": first.password,
        "kind": kind,
        "memory_cap_bytes": first.memory_cap_bytes,
        "timeout_s": _FIGURE_START_S + _FIGURE_READ_S * len(reads),
        "figures": [[read.page_index, dataclasses.asdict(read.image)] for read in reads],
    }
    return request_payload("figures", request, MAX_REQUEST_BYTES)
