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

import base64
import dataclasses
import hashlib
import math
import os
import re
import stat
import subprocess
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import IO, BinaryIO, Literal, NamedTuple

from datagrunt.pdf._image_facts import RawImage
from datagrunt.pdf._memory_cap import ending
from datagrunt.pdf._options import _RENDER_TIMEOUT_S, MEMORY_CAP_BYTES
from datagrunt.pdf._png import grey_png_bytes
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
from datagrunt.pdf._validate import checked_password, checked_path, int_at_least, positive_float, strict_bool
from datagrunt.pdf.elements import _MAX_ASSET_BYTES, Figure
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFSourceChangedError
from datagrunt.pdf.ocr import Band, bands, normalized, rotated
from datagrunt.pdf.render import RenderedPage, render_page, render_size

_default_render_page = render_page

__all__ = [
    "FIGURE_BATCH",
    "FigureRead",
    "PageDraw",
    "PageRender",
    "figure_pngs",
    "read_figures",
    "resolved_picture_keys",
    "verify_source_bytes",
]

_TURNS = (0, 90, 180, 270)

# The figures one worker reads, at most: a batch's request is ~0.5 KB a figure, within the launcher's 1 MiB bound.
FIGURE_BATCH = 64
# Bounded PNG cache for repeated picture keys during figure_pngs streaming (64 MiB).
_MAX_PNG_CACHE_BYTES = 64 << 20
# Bounds for fail-closed PDF image-dictionary uniqueness scan (#449).
_MAX_DICT_SCAN_BYTES = 64 << 20
_MAX_PDF_OBJECTS = 200_000
_MAX_DICT_NESTING = 32
_MAX_CONTENT_STREAM_INFLATE = 1 << 20
_MAX_TOTAL_CONTENT_INFLATE = 16 << 20
_UNIQUE_KEY = object()
_DISQUALIFYING_TOKENS = (b"/Encrypt", b"/DefaultGray", b"/DefaultRGB", b"/DefaultCMYK")
_DICT_TOKEN_RE = re.compile(rb"<<|>>|[%(]")
_OBJ_HEADER_RE = re.compile(rb"(?:^|[\r\n\s])(\d+)\s+(\d+)\s+obj(?=[\s\[\]()<>/{}%])")
_INT_OBJ_RE = re.compile(rb"(?:^|[\r\n\s])(\d+)\s+(\d+)\s+obj\s+(\d+)\s+endobj(?=[\s\r\n]|$)")
_STREAM_START_RE = re.compile(rb"stream(?:\r\n|\n)")
_LENGTH_DIRECT_RE = re.compile(rb"/Length\s+(\d+)(?!\d)")
_LENGTH_INDIRECT_RE = re.compile(rb"/Length\s+(\d+)\s+(\d+)\s+R(?![A-Za-z0-9_])")
_LENGTH_ANY_RE = re.compile(rb"/Length\s+(?:\d+\s+\d+\s+R|\d+)")
_SUBTYPE_IMAGE_RE = re.compile(rb"/Subtype\s*/Image(?![A-Za-z0-9_#.-])")
_SUBTYPE_REF_RE = re.compile(rb"/Subtype\s+\d+\s+\d+\s+R(?![A-Za-z0-9_])")
_FILTER_KEY_RE = re.compile(rb"/Filter(?=[\s/\[\(<>])")
_SINGLE_FLATE_RE = re.compile(rb"/Filter\s*(?:/FlateDecode|\[\s*/FlateDecode\s*\])(?![A-Za-z0-9_#.-])")
_ASCII85_FLATE_RE = re.compile(rb"/Filter\s*\[\s*/ASCII85Decode\s+/FlateDecode\s*\](?![A-Za-z0-9_#.-])")
_DECODE_PARMS_RE = re.compile(rb"/DecodeParms(?=[\s/\[\(<])")
_NON_CONTENT_STREAM_RE = re.compile(
    rb"/(?:Type\s*/(?:ObjStm|XRef|Metadata)|Subtype\s*/(?:Type1C|CIDFontType0C|OpenType)|Length1(?![A-Za-z0-9_]))"
)
_INLINE_IMAGE_RE = re.compile(rb"(?:^|[\s\[\]()<>/{}%])BI[\s\r\n]+/")
_WS_RE = re.compile(rb"\s+")
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
    sandbox: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", checked_path(self.source, "source").absolute())
        int_at_least("page_index", self.page_index, 0)
        int_at_least("dpi", self.dpi, 1)
        _checked_turn(self.rotation)
        checked_password(self.password)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)
        object.__setattr__(self, "timeout_s", positive_float("timeout_s", self.timeout_s))
        check_binding(self.identity, self.sha256)
        strict_bool("sandbox", self.sandbox)

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
    sandbox: bool = False

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
        strict_bool("sandbox", self.sandbox)

    @property
    def size(self) -> tuple[int, int]:
        """The page's ``(width, height)`` in pixels at ``dpi``."""
        return render_size(self.width_pt, self.height_pt, self.dpi)

    def draw_from_handle(self, handle: object) -> RenderedPage:
        """Draw this page on an already-open ``DocumentHandle``, checking source identity before and after."""
        from datagrunt.pdf import _render_worker
        from datagrunt.pdf.pdfium_engine import DocumentHandle

        if not isinstance(handle, DocumentHandle):
            raise TypeError(f"handle must be a DocumentHandle, got {type(handle).__name__}")
        image = f"page {self.page_index + 1}'s image"
        before = _drawable(self, f"before {image} was drawn")
        page = _render_worker.draw_loaded_in_process(
            handle,
            self.source,
            self.page_index,
            dpi=self.dpi,
            memory_cap_bytes=self.memory_cap_bytes,
            annotations=True,
        )
        _check_unchanged(self.source, before, f"while {image} was drawn")
        if (page.px_width, page.px_height) != self.size:
            width, height = self.size
            drew = f"page {self.page_index + 1} draws {page.px_width} x {page.px_height} px"
            raise DatagruntPDFError(
                f"{self.source}: {drew}, not the {width} x {height} px of its page image: the PDF is not the one read"
            )
        return page

    def png_bytes(self) -> bytes:
        """The page as displayed, 8-bit grey as drawn, as PNG."""
        page = _bound_render(self)
        if (page.px_width, page.px_height) != self.size:
            width, height = self.size
            drew = f"page {self.page_index + 1} draws {page.px_width} x {page.px_height} px"
            raise DatagruntPDFError(
                f"{self.source}: {drew}, not the {width} x {height} px of its page image: the PDF is not the one read"
            )
        return _png(page)


def _bound_render(loader: PageRender | PageDraw) -> RenderedPage:
    """The loader's page, drawn only from a file of its document's bytes (module notes)."""
    image = f"page {loader.page_index + 1}'s image"
    before = _drawable(loader, f"before {image} was drawn")
    if not loader.sandbox and render_page is _default_render_page:
        try:
            from datagrunt.pdf import _render_worker
        except ImportError as exc:
            from datagrunt.pdf._extra import missing_in_process

            hint = missing_in_process(type(loader).__name__, exc)
            if hint is not None:
                raise hint from exc
            raise

        page = _render_worker.draw_page_in_process(
            loader.source,
            loader.page_index,
            dpi=loader.dpi,
            password=loader.password,
            memory_cap_bytes=loader.memory_cap_bytes,
            annotations=True,
        )
    else:
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


def verify_source_bytes(
    source: Path,
    expected_sha256: str,
    *,
    identity: SourceIdentity | None,
    when: str,
) -> SourceIdentity:
    """Verify ``source`` holds bytes hashing to ``expected_sha256``, fast-pathing matching ``identity``."""
    now = _identity_at(source, when)
    if identity is not None and not identity.changes(now):
        return now
    digest = _sha256_at(source, now, when)
    if digest != expected_sha256:
        changes = () if identity is None else identity.changes(now)
        mismatch = f"its bytes are not the document's (sha256 {digest}, not {expected_sha256})"
        raise _changed(source, when, changes or ("content",), mismatch)
    return now


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
    return grey_png_bytes(image.px_width, image.px_height, image.pixels)


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
    sandbox: bool = False

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
        strict_bool("sandbox", self.sandbox)

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

    @property
    def picture_key(self) -> tuple[object, ...] | None:
        """Extraction-time picture identity key from ``image``, or None when not safely deduplicable."""
        return self.image.picture_key

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
    outcome = _single_outcome(read, kind)
    if isinstance(outcome, Exception):
        raise outcome
    return outcome


def _single_outcome(read: FigureRead, kind: Kind) -> bytes | Exception:
    """One figure's outcome (bytes or exception), read without raising."""
    (outcome,) = read_figures((read,), kind)
    return outcome


def _read_scan_bytes(source: Path) -> bytes | None:
    """Read ``source`` for dictionary uniqueness scanning when within ``_MAX_DICT_SCAN_BYTES``; else None."""
    try:
        with _opened_source(source) as stream:
            size = os.fstat(stream.fileno()).st_size
            if size < 1 or size > _MAX_DICT_SCAN_BYTES:
                return None
            data = stream.read(size + 1)
    except (OSError, ValueError):
        return None
    return data if len(data) == size else None


def _skip_pdf_string(data: bytes, start: int) -> int:
    """Return index immediately after a PDF literal string ``(...)`` starting at ``start``, or -1."""
    end = len(data)
    if start < 0 or start >= end or data[start] != 0x28:
        return -1
    depth = 0
    pos = start
    while pos < end:  # bounded: pos strictly increases every iteration
        byte = data[pos]
        if byte == 0x5C:  # backslash escape
            pos += 2
            continue
        if byte == 0x28:  # '('
            depth += 1
        if byte == 0x29:  # ')'
            depth -= 1
        pos += 1
        if depth == 0:
            return pos
    return -1


def _skip_ws_and_comments(data: bytes, pos: int) -> int:
    """Advance ``pos`` past PDF whitespace and ``%`` comments."""
    end = len(data)
    while pos < end:  # bounded: pos strictly increases
        byte = data[pos]
        if byte in (0x00, 0x09, 0x0A, 0x0C, 0x0D, 0x20):
            pos += 1
            continue
        if byte == 0x25:  # '%' comment to end of line
            nl = data.find(b"\n", pos)
            cr = data.find(b"\r", pos)
            pos = end if nl < 0 and cr < 0 else min(x for x in (nl, cr) if x >= 0) + 1
            continue
        return pos
    return end


def _find_dict_end(data: bytes, start: int) -> int:
    """Return index immediately after the balanced ``>>`` matching ``<<`` at ``start``, or -1."""
    depth = 0
    pos = start
    end = len(data)
    for _ in range(end - start + 1):  # bounded: pos strictly advances every iteration
        match = _DICT_TOKEN_RE.search(data, pos)
        if match is None:
            return -1
        token = match.group(0)
        if token == b"%":
            pos = _skip_ws_and_comments(data, match.start())
            continue
        if token == b"(":
            pos = _skip_pdf_string(data, match.start())
            if pos < 0:
                return -1
            continue
        if token == b"<<":
            depth += 1
            if depth > _MAX_DICT_NESTING:
                return -1
            pos = match.end()
            continue
        depth -= 1
        pos = match.end()
        if depth == 0:
            return pos
    return -1


def _top_level_dict(dict_bytes: bytes) -> bytes:
    """Keep only top-level dictionary bytes outside nested ``<<...>>``, strings ``(...)``, and comments."""
    out: list[bytes] = []
    depth = 0
    pos = 0
    end = len(dict_bytes)
    for _ in range(end + 1):  # bounded: pos strictly advances
        match = _DICT_TOKEN_RE.search(dict_bytes, pos)
        if match is None:
            if depth == 1:
                out.append(dict_bytes[pos:])
            return b"".join(out)
        if depth == 1:
            out.append(dict_bytes[pos : match.start()])
        token = match.group(0)
        if token == b"%":
            pos = _skip_ws_and_comments(dict_bytes, match.start())
            continue
        if token == b"(":
            pos = _skip_pdf_string(dict_bytes, match.start())
            if pos < 0:
                return b""
            out.append(b" ")
            continue
        depth += 1 if token == b"<<" else -1
        pos = match.end()
        if depth == 1:
            out.append(b" ")
    return b"".join(out)


def _canonical_image_dict(dict_bytes: bytes) -> bytes:
    """Normalize an image dictionary for equality: strip ``/Length`` and collapse whitespace outside strings."""
    parts: list[bytes] = []
    pos = 0
    end = len(dict_bytes)
    for _ in range(end + 1):  # bounded: pos strictly advances
        paren = dict_bytes.find(b"(", pos)
        segment = dict_bytes[pos:] if paren < 0 else dict_bytes[pos:paren]
        cleaned = _WS_RE.sub(b" ", _LENGTH_ANY_RE.sub(b"", segment))
        parts.append(cleaned)
        if paren < 0:
            break
        after = _skip_pdf_string(dict_bytes, paren)
        if after < 0:
            return dict_bytes
        parts.append(dict_bytes[paren:after])
        pos = after
    return b"".join(parts).strip()


def _resolve_stream_length(top: bytes, int_objs: dict[tuple[int, int], int]) -> int | None:
    """Resolve top-level ``/Length`` (direct integer or indirect ``N G R``); None if missing or unknown."""
    indirect = _LENGTH_INDIRECT_RE.search(top)
    if indirect is not None:
        return int_objs.get((int(indirect.group(1)), int(indirect.group(2))))
    direct = _LENGTH_DIRECT_RE.search(top)
    return None if direct is None else int(direct.group(1))


def _stream_end_after(data: bytes, stream_start: int, length: int) -> int:
    """Verify ``endstream`` follows ``length`` bytes from ``stream_start``; return index after ``endstream`` or -1."""
    stream_end = stream_start + length
    if length < 0 or stream_end > len(data):
        return -1
    after = stream_end
    for _ in range(4):  # allow optional trailing whitespace/CRLF before endstream
        if after < len(data) and data[after] in (0x0A, 0x0D, 0x20):
            after += 1
        else:
            break
    if data[after : after + 9] != b"endstream":
        return -1
    return after + 9


def _decode_ascii85(payload: bytes) -> bytes | None:
    """Decode an Adobe ASCII85 PDF stream payload, or return None on syntax error."""
    stripped = payload.rstrip()
    if stripped.endswith(b"~>"):
        stripped = stripped[:-2]
    try:
        return base64.a85decode(stripped, adobe=False, ignorechars=b" \t\r\n\x0c")
    except ValueError:
        return None


def _inflate_bounded(compressed: bytes, cap: int) -> bytes | None:
    """Inflate ``compressed`` whole within ``cap`` bytes, or return None."""
    if cap <= 0:
        return None
    inflater = zlib.decompressobj()
    try:
        inflated = inflater.decompress(compressed, cap + 1)
    except zlib.error:
        return None
    if not inflater.eof or len(inflated) > cap:
        return None
    return inflated


def _content_stream_safe(top: bytes, payload: bytes, inflate_budget: list[int]) -> bool:
    """True when a non-image stream is proven to contain no inline image operator (``BI``)."""
    if _NON_CONTENT_STREAM_RE.search(top):
        return True
    if _DECODE_PARMS_RE.search(top):
        return False
    if not _FILTER_KEY_RE.search(top):
        return _INLINE_IMAGE_RE.search(payload) is None
    cap = min(_MAX_CONTENT_STREAM_INFLATE, inflate_budget[0])
    compressed: bytes | None = None
    if _SINGLE_FLATE_RE.search(top):
        compressed = payload
    elif _ASCII85_FLATE_RE.search(top) and len(payload) <= cap:
        compressed = _decode_ascii85(payload)
    if compressed is None:
        return False
    inflated = _inflate_bounded(compressed, cap)
    if inflated is None:
        return False
    inflate_budget[0] -= len(inflated)
    return _INLINE_IMAGE_RE.search(inflated) is None


def _dedupable_streams(source: Path, candidate_shas: frozenset[str]) -> frozenset[str]:
    """Return subset of ``candidate_shas`` proven to appear with one unique image dictionary in ``source``.

    Fail-closed: encrypted PDFs, ``/DefaultGray``/``/DefaultRGB``/``/DefaultCMYK``,
    incremental updates, hex-escaped names, unparseable streams, or content streams
    holding (or possibly holding) inline images return ``frozenset()``.
    """
    data = _read_scan_bytes(source)
    if data is None or any(token in data for token in _DISQUALIFYING_TOKENS):
        return frozenset()
    int_objs = {(int(m.group(1)), int(m.group(2))): int(m.group(3)) for m in _INT_OBJ_RE.finditer(data)}
    dicts_by_sha: dict[str, set[bytes]] = {}
    seen_objs: set[tuple[int, int]] = set()
    inflate_budget = [_MAX_TOTAL_CONTENT_INFLATE]
    pos = 0
    for _ in range(_MAX_PDF_OBJECTS):
        match = _OBJ_HEADER_RE.search(data, pos)
        if match is None:
            return frozenset(sha for sha, dicts in dicts_by_sha.items() if len(dicts) == 1)
        obj_id = (int(match.group(1)), int(match.group(2)))
        if obj_id in seen_objs:
            return frozenset()
        seen_objs.add(obj_id)
        pos = _inspect_object_stream(data, match.end(), int_objs, candidate_shas, dicts_by_sha, inflate_budget)
        if pos < 0:
            return frozenset()
    return frozenset()


def _inspect_object_stream(
    data: bytes,
    body_start: int,
    int_objs: dict[tuple[int, int], int],
    candidate_shas: frozenset[str],
    dicts_by_sha: dict[str, set[bytes]],
    inflate_budget: list[int],
) -> int:
    """Inspect one indirect object body; return next scan offset, or -1 to fail closed."""
    dict_start = _skip_ws_and_comments(data, body_start)
    if data[dict_start : dict_start + 2] != b"<<":
        return body_start
    dict_end = _find_dict_end(data, dict_start)
    if dict_end < 0:
        return -1
    after_dict = _skip_ws_and_comments(data, dict_end)
    stream_match = _STREAM_START_RE.match(data, after_dict)
    if stream_match is None:
        return dict_end
    dict_bytes = data[dict_start:dict_end]
    if b"#" in dict_bytes:
        return -1
    top = _top_level_dict(dict_bytes)
    length = _resolve_stream_length(top, int_objs)
    if length is None:
        return -1
    after_endstream = _stream_end_after(data, stream_match.end(), length)
    if after_endstream < 0 or _SUBTYPE_REF_RE.search(top):
        return -1
    payload = data[stream_match.end() : stream_match.end() + length]
    if _SUBTYPE_IMAGE_RE.search(top):
        sha = hashlib.sha256(payload).hexdigest()
        if sha in candidate_shas:
            dicts_by_sha.setdefault(sha, set()).add(_canonical_image_dict(dict_bytes))
        return after_endstream
    return after_endstream if _content_stream_safe(top, payload, inflate_budget) else -1


def resolved_picture_keys(reads: Sequence[FigureRead]) -> tuple[object, ...]:
    """Return one deduplication key per read in ``reads``; equal keys are proven to decode to identical PNGs."""
    raw_keys = [None if read.picture_key is None else (_worker_key(read), read.picture_key) for read in reads]
    counts = Counter(key for key in raw_keys if key is not None)
    candidates_by_source: dict[Path, set[str]] = {}
    for read, raw_key in zip(reads, raw_keys, strict=True):
        if raw_key is not None and counts[raw_key] >= 2:
            candidates_by_source.setdefault(read.source, set()).add(read.raw_sha256)
    if not candidates_by_source:
        return tuple((_UNIQUE_KEY, idx) for idx in range(len(reads)))
    dedupable = {src: _dedupable_streams(src, frozenset(shas)) for src, shas in candidates_by_source.items()}
    return tuple(
        raw_key if raw_key is not None and read.raw_sha256 in dedupable.get(read.source, ()) else (_UNIQUE_KEY, idx)
        for idx, (read, raw_key) in enumerate(zip(reads, raw_keys, strict=True))
    )


def figure_pngs(figures: Sequence[Figure], *, workers: int = 1) -> Iterator[Callable[[], bytes]]:
    """Each figure's PNG read, in order, as a callable that returns its bytes or raises its error.

    The figures whose PNG loader is a :class:`FigureRead`'s are deduplicated by
    resolved picture key (reading each distinct picture once) and read in
    batches spread across ``workers`` (:func:`read_figures`); any other figure
    (a bundle's, read from its file) is read by its own ``read_bytes``.
    """
    int_at_least("workers", workers, 1)
    index = 0
    while index < len(figures):  # bounded: each pass consumes at least one figure
        run = _run_of_reads(figures, index)
        if not run:
            yield figures[index].read_bytes
            index += 1
            continue
        yield from _deduped_figure_pngs(run, workers=workers)
        index += len(run)


def _deduped_figure_pngs(run: list[FigureRead], *, workers: int) -> Iterator[Callable[[], bytes]]:
    """Stream PNG outcomes for ``run``, reading each distinct picture key once with bounded refcounted caching."""
    keys = resolved_picture_keys(run)
    remaining = Counter(keys)
    seen: set[object] = set()
    rep_reads: list[FigureRead] = []
    is_first: list[bool] = []
    for read, key in zip(run, keys, strict=True):
        first = key not in seen
        is_first.append(first)
        if first:
            seen.add(key)
            rep_reads.append(read)
    rep_outcomes = read_figures(rep_reads, "png", workers=workers)
    cache: dict[object, bytes] = {}
    cache_bytes = 0
    try:
        for read, key, first in zip(run, keys, is_first, strict=True):
            remaining[key] -= 1
            outcome, cache_bytes = _resolve_run_item(read, key, first, rep_outcomes, cache, remaining[key], cache_bytes)
            yield partial(_outcome_of, outcome)
    finally:
        close = getattr(rep_outcomes, "close", None)
        if callable(close):
            close()


def _resolve_run_item(
    read: FigureRead,
    key: object,
    first: bool,
    rep_outcomes: Iterator[bytes | Exception],
    cache: dict[object, bytes],
    left: int,
    cache_bytes: int,
) -> tuple[bytes | Exception, int]:
    """Obtain one figure's outcome from representative iterator, bounded cache, or per-figure fallback."""
    if first:
        outcome = next(rep_outcomes)
    elif key in cache:
        outcome = cache[key]
        if left == 0:
            cache_bytes -= len(cache.pop(key))
    else:
        outcome = _single_outcome(read, "png")
    if (
        isinstance(outcome, bytes)
        and left > 0
        and key not in cache
        and cache_bytes + len(outcome) <= _MAX_PNG_CACHE_BYTES
    ):
        cache[key] = outcome
        cache_bytes += len(outcome)
    return outcome, cache_bytes


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


def read_figures(reads: Sequence[FigureRead], kind: Kind, *, workers: int = 1) -> Iterator[bytes | Exception]:
    """Each read's bytes (``kind``: ``"png"`` or ``"raw"``), or the exception it raised, in order (module notes).

    Consecutive reads of one PDF (its path, password and memory cap) share a
    worker, at most :data:`FIGURE_BATCH` of them under ``sandbox``; with
    ``workers > 1``, batches spread across up to ``workers`` threads while
    yielding outcomes in deterministic order.

    Raises:
        ValueError: If ``kind`` is neither ``"png"`` nor ``"raw"``, or ``workers < 1``.
        TypeError: If a read is not a FigureRead, or ``workers`` is not an int.
    """
    if kind not in _KINDS:
        raise ValueError(f"kind must be one of {_KINDS}, got {kind!r}")
    worker_count = int_at_least("workers", workers, 1)
    for read in reads:
        if not isinstance(read, FigureRead):
            raise TypeError(f"each read must be a FigureRead, got {type(read).__name__}")
    batches = list(_batches(reads, workers=worker_count))
    if worker_count == 1 or len(batches) <= 1:
        for batch in batches:  # bounded: the reads' batches
            yield from _batch_outcomes(batch, kind)
        return
    yield from _parallel_batch_outcomes(batches, kind, workers=min(worker_count, len(batches)))


def _collect_batch(batch: list[FigureRead], kind: Kind) -> list[bytes | Exception]:
    """Materialize one batch's outcomes inside a worker thread."""
    return list(_batch_outcomes(batch, kind))


def _parallel_batch_outcomes(
    batches: list[list[FigureRead]], kind: Kind, *, workers: int
) -> Iterator[bytes | Exception]:
    """Run ``batches`` across at most ``workers`` threads in a bounded sliding window, yielding in batch order."""
    next_submit = 0
    next_yield = 0
    ready: dict[int, list[bytes | Exception]] = {}
    in_flight: dict[Future[list[bytes | Exception]], int] = {}
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="datagrunt-figures")
    try:
        for _ in range(2 * len(batches) + 1):  # bounded: each pass yields a batch or completes >=1 future
            if next_yield >= len(batches):
                return
            while next_submit < len(batches) and len(in_flight) + len(ready) < workers:
                in_flight[pool.submit(_collect_batch, batches[next_submit], kind)] = next_submit
                next_submit += 1
            if next_yield in ready:
                yield from ready.pop(next_yield)
                next_yield += 1
                continue
            done, _pending = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in done:
                ready[in_flight.pop(future)] = future.result()
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def _batches(reads: Sequence[FigureRead], *, workers: int = 1) -> Iterator[list[FigureRead]]:
    """``reads`` in runs sharing a worker (capped by FIGURE_BATCH under ``sandbox``; split across ``workers``)."""
    run: list[FigureRead] = []
    for read in reads:
        if run and _worker_key(run[0]) != _worker_key(read):
            yield from _split_run(run, workers=workers)
            run = []
        run.append(read)
    if run:
        yield from _split_run(run, workers=workers)


def _split_run(run: list[FigureRead], *, workers: int) -> Iterator[list[FigureRead]]:
    """Split one homogeneous worker-key ``run`` into bounded batches for ``workers``."""
    sandbox = run[0].sandbox
    if workers == 1:
        size = FIGURE_BATCH if sandbox else len(run)
    else:
        per_worker = math.ceil(len(run) / min(workers, len(run)))
        size = min(FIGURE_BATCH, per_worker) if sandbox else per_worker
    for start in range(0, len(run), max(1, size)):
        yield run[start : start + size]


def _worker_key(read: FigureRead) -> tuple[Path, str | None, int, bool]:
    return read.source, read.password, read.memory_cap_bytes, read.sandbox


def _in_process_figure_outcomes(batch: list[FigureRead], kind: Kind) -> Iterator[bytes | Exception]:
    """Read a batch of figures in-process from one open DocumentHandle and FigureSession."""
    try:
        from datagrunt.pdf.pdfium_engine import FigureSession, ImageLoader, open_document
    except ImportError as exc:
        from datagrunt.pdf._extra import missing_in_process

        hint = missing_in_process("FigureRead", exc)
        if hint is not None:
            raise hint from exc
        raise

    first = batch[0]
    try:
        handle_ctx = open_document(first.source, password=first.password)
    except (DatagruntPDFError, OSError) as exc:
        for _ in batch:
            yield exc
        return
    with handle_ctx as handle, FigureSession(handle) as session:
        for read in batch:
            loader = ImageLoader(
                source=read.source,
                page_index=read.page_index,
                image=read.image,
                password=read.password,
            )
            try:
                yield session.read(loader, kind)
            except (DatagruntPDFError, OSError) as exc:
                yield exc


def _batch_outcomes(batch: list[FigureRead], kind: Kind) -> Iterator[bytes | Exception]:
    """The batch's outcomes, in order: in-process by default, or in a worker under ``sandbox``."""
    if not batch[0].sandbox:
        yield from _in_process_figure_outcomes(batch, kind)
        return
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
