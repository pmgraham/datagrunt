"""The v5 PDF document pipeline (#372, A8a): spec §3.3 steps 1-5, page by page, then the document.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A7's
``PDFWriter`` and A8b's ``PDFReader`` stand on it.

:class:`PipelineOptions` holds the reader's and writer's extraction options
and runs the spec's construction checks (invariant 2) when it is built, so
a bad option fails before any work. :class:`PDFSource` is where a document
is opened from (its path and password), in this process or in a worker.

:func:`extract_page_at` is one page's steps, in order; no stage is skipped
(:func:`extract_document` runs them :data:`_WINDOW_PAGES` pages at a time,
steps 1-3 for each page of a window before step 4 for any):

1. A2 :func:`~datagrunt.pdf.pdfium_engine.extract_page`. A
   :class:`~datagrunt.pdf.pdfium_engine.PageFailure` raises
   :class:`~datagrunt.pdf.exceptions.PDFCorruptError` naming the page under
   ``strict``; else it is A5b's :func:`~datagrunt.pdf.vision.fallback_page`'s
   (its geometry read again, then render and OCR: ``"ocr_fallback"``, or an
   error page naming why it was not read).
2. A2 :func:`~datagrunt.pdf.pdfium_engine.recover_dropped_text`, when the
   page's textpage drops text objects (``TextStats.dropped_text_objects``
   over 0).
3. A3 :func:`~datagrunt.pdf.quality.assess_page`, with the ``ocr`` option
   and the recovery; then the background check (#400): a page A3's gate
   (:func:`~datagrunt.pdf.quality.needs_background`) passes is drawn in
   24-bit BGR without its text (:func:`~datagrunt.pdf.render.render_pages`:
   the guarded, capped worker) at :data:`_BACKGROUND_DPI`, lowered to fit
   :data:`_BACKGROUND_MAX_PIXELS` or the reader's ``max_render_pixels`` if
   lower (:func:`~datagrunt.pdf.render.effective_dpi`), and assessed again
   with that backdrop: assess, then flag, then route, so text flagged
   ``matches_background`` counts as hidden. A window's gated pages are drawn
   in one worker, with the vision route's memory cap and a deadline of
   :data:`_BACKGROUND_START_S` + :data:`_BACKGROUND_PAGE_S` a page. A page
   whose render is refused or fails is assessed with why, and fails closed:
   each piece the gate selects is flagged ``unchecked_background`` (hidden,
   never dropped), and it warns ``background not checked: ...``. A render
   call that raises instead is each of its pages' step-5 failure.
4. The route: a digital or hybrid page is A4a's lattice tables, then A4b's
   :func:`~datagrunt.pdf.assembly.assemble_page`; a vision page is A5b's
   :func:`~datagrunt.pdf.vision.vision_page`'s (render and OCR: ``"ok"``,
   or ``"ocr_unavailable"`` with its digital text when OCR cannot read it).
   Both A5b steps take the source's password, ``ocr``,
   ``min_image_dimension`` (a vision page's figures), and ``vision_dpi`` and
   ``max_render_pixels`` (:class:`~datagrunt.pdf.vision.VisionOptions`).
5. Isolation: any other exception (an ``Exception``; an interrupt is never
   caught) is an error page whose ``error`` is ``"<type>: <message>"``,
   recorded, never dropped. Under ``strict`` it raises a PDFCorruptError
   naming the page, chained from it (a PDFCorruptError that already names
   the page is raised as it is).

:func:`extract_document` runs every page's steps in page order, then the
document's:

6. A6 :func:`~datagrunt.pdf.scope.apply_scope`: scope paths, split-table
   links, sections and header context.
7. The A1 :class:`~datagrunt.pdf.elements.PDFDocument`. Its metadata:
   ``source_name`` is the file's name; ``sha256`` is streamed over the
   file's bytes in chunks of :data:`_HASH_CHUNK_BYTES`; ``title`` and
   ``author`` are A2's :func:`~datagrunt.pdf.pdfium_engine.document_metadata`;
   ``doc_id`` is :meth:`~datagrunt.pdf.elements.DocumentMetadata.make_doc_id`.

Workers: with ``workers > 1`` the pages run in worker processes, one per
contiguous chunk (:func:`_chunks`: ``min(workers, page_count)`` chunks whose
sizes differ by at most one), all at once; each worker opens the document
itself (a pdfium handle never crosses processes) and the pages come back in
page order. Pages extract in this process instead when ``workers`` is 1, the
document has one page, or a distributed runner is detected
(:func:`_is_distributed_env`, ported from v4: its executors are the
parallelism). The pool is bounded and always ends:

- A worker is a plain ``python -I`` subprocess started by the launcher the
  render worker shares (:mod:`datagrunt.pdf._process`, worker key
  ``"pages"``): it never imports the caller's main module, so a caller's
  script runs once, however it is written, with or without a ``__main__``
  guard. Its request (the source, the password, the file's identity, the
  chunk and the options) is JSON on its stdin; with no Python interpreter
  to start (a frozen application), ``workers > 1`` raises before any
  worker starts.
- A worker sends each page as one length-prefixed pickled message as soon
  as it is extracted (its window's steps 1-3 and background render first,
  then its step 4), in page order; then, once it has re-checked the file
  (see below), a done message. The exception that stops the chunk is sent
  pickled instead of what is left. Each message is read within
  :data:`_MAX_PAGE_MESSAGE_BYTES` (the 1 GiB asset limit); a longer one is
  refused unread.
- The parent reads every worker's replies as they come, each worker's in a
  thread of its own (#426): no worker waits, its pipe full, for another's
  to be read. It checks each reply is a page, its chunk's next, and the
  count at done.
- Unpickling a worker's pages is acceptable where it would not be for
  bytes from elsewhere: the worker is the caller's own process image, run
  with the caller's privileges and exact ``sys.path``. A worker compromised
  through pdfium could already do anything the caller can, and in this
  process pdfium runs in the caller anyway (``workers=1``). The render
  worker, whose replies are pixels, pickles nothing.
- Each reply must come within its bound (:func:`_reply_bound_s`, #418),
  counted from the worker's reply before it (from its start for its first
  page): the deadline restarts with every page. A page's bound
  (:func:`_page_bound_s`) is its own work (:data:`_WORKER_PAGE_S`), its
  share of its window's background render (:data:`_BACKGROUND_PAGE_S`)
  and, unless ``ocr="never"``, its render and OCR budget
  (``page_budget_s``, :func:`_vision`'s: the vision route stops when it is
  spent) and its last call's reap (:data:`_CALL_REAP_S`). It is never the
  page's size: a giant page's is a letter page's, 705 s at the defaults
  (about 12 minutes; 90 s under ``ocr="never"``). A window's first page is
  also given the window's lead: its other pages' work and background
  shares, and its background render's start and reap
  (:data:`_BACKGROUND_START_S` + :data:`_CALL_REAP_S`); a chunk's first
  page, :data:`_WORKER_START_S`; done, :data:`_WORKER_PAGE_S` after the last
  page. So a worker that hangs is killed within one page's bound (with its
  window's lead), and one whose pages each finish within their bounds is
  never killed, however many pages it holds.
- A worker killed at a bound, that dies without returning (crashed,
  ``os._exit``), or that sends a message over its bound, keeps the pages
  it sent; exactly its chunk's pages it did not send are error pages, never
  a lost page, their error saying which page did not come within which
  bound, or ending with its stderr's last 4 KiB; the other chunks extract.
  One that sent each of its pages but not done keeps them: only its last
  check of the path was not made (this process's own check of the hashed
  file still is). Under ``strict`` any of these raises a DatagruntPDFError
  naming the pages instead.
- An exception a worker raises outside the per-page isolation (``strict``'s
  PDFCorruptError, an open error) is raised here, after the pages before
  its chunk; the workers after it are killed.
- Every worker is reaped (killed first if still running) before the pool
  returns or raises, on every path.

The hash is bound to the bytes extracted. The source is opened once, and
through that open file its identity is taken, once
(:class:`SourceIdentity`: ``st_dev``, ``st_ino``, ``st_size``,
``st_mtime_ns`` from ``fstat``), then its first ``st_size`` bytes are
hashed. pdfium opens the file by path (A2's
:func:`~datagrunt.pdf.pdfium_engine.open_document`, which maps pdfium's
load errors itself), so each opening is checked against the identity:

- right after pdfium opens it here, ``os.stat`` of the path;
- in each worker, right after pdfium opens it and again after its last
  page, ``os.stat`` of the path;
- after every page, ``fstat`` of the file the hash was read through.

Any difference raises :class:`~datagrunt.pdf.exceptions.PDFSourceChangedError`
naming it (inode, size or mtime), ``strict`` or not: no document whose pages
and hash could disagree is returned. A file replaced after pdfium opened it
here needs no check: pdfium keeps reading the file it opened, the hashed one.
What this cannot see: a write that keeps the size and restores the mtime,
or the path swapped away and back in the moment between pdfium's open and
the check that follows it (each is a deliberate race on the caller's own
directory). A figure's loader reopens the path later and verifies each
image's own SHA-256 when it reads.

An error page (A1: no elements, no quality, never verified) is routed
``"vision"``, the spec's route for a page whose extraction fails (render
and OCR, A5b). A page A2 could not read is ``fallback_page``'s; a page
whose step raised (step 5), or a worker's lost page, is this module's: its
size is the page's, read with
:func:`~datagrunt.pdf.pdfium_engine.page_geometry`; when that cannot be
read either, it is pdfium's default page size (612 x 792 pt, what pdfium
reads for an empty MediaBox), and a warning says so and why. A worker's
error pages (it died, or ran out of time) have that default size: their
pages were never read in this process.
"""

import dataclasses
import errno
import hashlib
import os
import pickle
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import IO, BinaryIO, NamedTuple, TypeVar, get_args

from datagrunt.pdf._process import (
    MAX_REQUEST_BYTES,
    REAP_TIMEOUT_S,
    MessageTooLong,
    NoInterpreter,
    close_pipes,
    read_message,
    reaped,
    request_payload,
    send_message,
    start_worker,
    stderr_tail,
    worker_command,
)
from datagrunt.pdf._validate import choice_str, int_at_least, strict_bool
from datagrunt.pdf.assembly import MIN_IMAGE_DIMENSION, assemble_page
from datagrunt.pdf.elements import _MAX_ASSET_BYTES, DocumentMetadata, Page, PDFDocument
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFCorruptError, PDFSourceChangedError
from datagrunt.pdf.layout import find_lattice_tables
from datagrunt.pdf.pdfium_engine import (
    DocumentHandle,
    DocumentInfo,
    PageFailure,
    RawPage,
    RecoveredText,
    _checked_index,
    _checked_password,
    _checked_path,
    document_metadata,
    extract_page,
    open_document,
    page_geometry,
    recover_dropped_text,
)
from datagrunt.pdf.quality import Backdrop, OcrMode, PageAssessment, assess_page, needs_background
from datagrunt.pdf.render import (
    _MIN_RENDER_PIXELS,
    _MIN_TARGET_DPI,
    RenderedPage,
    RenderFailed,
    RenderRefused,
    effective_dpi,
    render_pages,
)
from datagrunt.pdf.scope import apply_scope
from datagrunt.pdf.vision import VisionOptions, fallback_page, vision_page

__all__ = ["PDFSource", "PipelineOptions", "SourceIdentity", "extract_document", "extract_page_at"]

_OCR_MODES: tuple[OcrMode, ...] = get_args(OcrMode)

# pdfium's own default page size (US Letter), as it reads an empty MediaBox: an error page's size when its own
# cannot be read.
_DEFAULT_PAGE_SIZE = (612.0, 792.0)

# The source is hashed in reads of at most this many bytes.
_HASH_CHUNK_BYTES = 1 << 20

# Environment variables that mark a distributed runner (Spark, Beam, Flink, Celery): v4's
# ``core.pdf_io.engines._is_distributed_env``, ported.
_DISTRIBUTED_KEYS = ("SPARK_ENV_LOADED", "SPARK_HOME", "BEAM_WORKER_ID", "FLINK_CONF_DIR", "CELERY_BROKER_URL")

# A worker's first page is given this to start, and every page this for its own work (the module notes' bounds).
# Measured here: a worker imports datagrunt in ~0.6 s, and the corpus's slowest page (95 pages)
# extracts in ~0.5 s, so each bound is over 100 times what it covers.
_WORKER_START_S = 60.0
_WORKER_PAGE_S = 60.0
# A bounded call (a render worker, a tesseract run) can outlast its timeout by its reap: its timer's join, then a wait,
# a kill and a wait for its process, each within REAP_TIMEOUT_S.
_CALL_REAP_S = 3 * REAP_TIMEOUT_S
_EXIT_GRACE_S = 1.0  # a worker whose replies ended without its pages is given this to exit

# A page worker's replies, each one length-prefixed message: a page (pickled), the chunk done, or the
# exception that stopped it (pickled). Each is read within the asset limit.
_PAGE, _DONE, _ERROR = b"P", b"D", b"E"
_MAX_PAGE_MESSAGE_BYTES = _MAX_ASSET_BYTES
_CHUNK_REQUEST_KEYS = ("source", "password", "identity", "pages", "options")

# The background check (#400). Pages go through steps 1-3 this many at a time; the window's gated pages are then
# drawn in one render worker: a bound on the raw pages and backdrops held at once (at most 8 x 6 MB of pixels).
_WINDOW_PAGES = 8
# Its render: 72 DPI (a letter page is 612 x 792 px), lowered to fit 2,000,000 px (6 MB of BGR), or the reader's
# max_render_pixels if lower.
_BACKGROUND_DPI = 72
_BACKGROUND_MAX_PIXELS = 2_000_000
# Its worker's deadline: this, plus _BACKGROUND_PAGE_S for each page it draws. Measured here on the corpus (95 pages,
# every one gated): a worker starts in ~0.2 s and draws a page in ~0.06 s or less, so each bound is over 100 times what
# it covers. One worker a page would cost ~0.26 s a page; a window of 8 costs ~0.1 s a page.
_BACKGROUND_START_S = 60.0
_BACKGROUND_PAGE_S = 30.0

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineOptions:
    """The extraction options of ``PDFReader`` and ``PDFWriter``, checked at construction (spec invariant 2).

    Picklable, so it crosses the process pool.

    Attributes:
        workers: Processes to extract pages with (>= 1); 1 extracts in this one.
        ocr: ``"auto"``, ``"always"`` or ``"never"`` (A3's routing option).
        dpi: The page render's target DPI (>= 36). A5b renders with it.
        vision_dpi: A vision page's render DPI (>= ``dpi``). A5b renders with it.
        max_render_pixels: The pixel bound of any render (>= 262,144, 512 x
            512). A5b renders within it.
        min_image_dimension: The smallest image side, in pixels, that is a
            figure (>= 1).
        strict: Raise :class:`~datagrunt.pdf.exceptions.PDFCorruptError` on a
            bad page instead of recording it as an error page.

    Raises:
        TypeError: If a value is of the wrong type (bool is no int).
        ValueError: If ``workers < 1``, ``dpi < 36``, ``vision_dpi < dpi``,
            ``max_render_pixels < 262_144``, ``min_image_dimension < 1``, or
            ``ocr`` is not one of the three modes.
    """

    workers: int = 1
    ocr: OcrMode = "auto"
    dpi: int = 200
    vision_dpi: int = 300
    max_render_pixels: int = 16_000_000
    min_image_dimension: int = MIN_IMAGE_DIMENSION
    strict: bool = False

    def __post_init__(self) -> None:
        int_at_least("workers", self.workers, 1)
        int_at_least("dpi", self.dpi, _MIN_TARGET_DPI)
        int_at_least("vision_dpi", self.vision_dpi, _MIN_TARGET_DPI)
        if self.vision_dpi < self.dpi:
            raise ValueError(f"vision_dpi must be >= dpi ({self.dpi}), got {self.vision_dpi}")
        int_at_least("max_render_pixels", self.max_render_pixels, _MIN_RENDER_PIXELS)
        int_at_least("min_image_dimension", self.min_image_dimension, 1)
        choice_str("ocr", self.ocr, _OCR_MODES)
        strict_bool("strict", self.strict)


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceIdentity:
    """The identity of the file a document's hash was read through, from one ``fstat`` (see the module notes).

    Attributes:
        device: ``st_dev``.
        inode: ``st_ino``: with ``device``, which file the path named.
        size: ``st_size``, in bytes.
        mtime_ns: ``st_mtime_ns``.
    """

    device: int
    inode: int
    size: int
    mtime_ns: int

    def __post_init__(self) -> None:
        for name in ("device", "inode", "size", "mtime_ns"):
            int_at_least(name, getattr(self, name), 0)

    @classmethod
    def of(cls, status: os.stat_result) -> "SourceIdentity":
        """The identity in a stat result."""
        return cls(device=status.st_dev, inode=status.st_ino, size=status.st_size, mtime_ns=status.st_mtime_ns)

    def changes(self, other: "SourceIdentity") -> tuple[str, ...]:
        """What differs in ``other``, in words: its inode, size and mtime, in that order; empty when nothing does."""
        found: list[str] = []
        if (self.device, self.inode) != (other.device, other.inode):
            found.append(f"inode {self.device}:{self.inode} -> {other.device}:{other.inode}")
        if self.size != other.size:
            found.append(f"size {self.size} -> {other.size} bytes")
        if self.mtime_ns != other.mtime_ns:
            found.append(f"mtime {self.mtime_ns} -> {other.mtime_ns} ns")
        return tuple(found)


@dataclass(frozen=True, slots=True, kw_only=True)
class PDFSource:
    """Where the document is opened from: in this process, or reopened in a worker.

    Picklable, so it crosses the process pool (a pdfium handle cannot).

    Attributes:
        path: The PDF's path, made absolute at construction (a worker's
            ``chdir`` cannot redirect it).
        password: The encrypted PDF's password, or None. Excluded from repr
            and equality, and never put in an error message; a pickle of the
            source carries it (the worker needs it).
        identity: The identity of the file the document's hash was read
            through; every opening of ``path`` is checked against it. None
            checks nothing (a caller that hashed nothing).

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike of str,
            ``password`` is not a str or None, or ``identity`` is not a
            SourceIdentity or None.
        ValueError: If ``password`` contains NUL.
    """

    path: Path
    password: str | None = field(default=None, repr=False, compare=False)
    identity: SourceIdentity | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _checked_path(self.path, "source").absolute())
        _checked_password(self.password)
        if self.identity is not None and not isinstance(self.identity, SourceIdentity):
            raise TypeError(f"identity must be a SourceIdentity or None, got {type(self.identity).__name__}")


def extract_document(
    source: str | os.PathLike[str],
    *,
    password: str | None = None,
    workers: int = 1,
    ocr: OcrMode = "auto",
    dpi: int = 200,
    vision_dpi: int = 300,
    max_render_pixels: int = 16_000_000,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
    strict: bool = False,
) -> PDFDocument:
    """Extract the whole PDF at ``source``: steps 1-5 for every page, then 6-7 (see the module notes).

    The options are :class:`PipelineOptions`', checked before any work. No
    page is dropped: a page that fails is an error page, or, under
    ``strict``, a PDFCorruptError naming it. The document is closed before
    returning, on every path.

    Raises:
        TypeError, ValueError: As :class:`PipelineOptions` and
            :class:`PDFSource` raise them, before the file is opened.
        FileNotFoundError, PermissionError, PDFEncryptedError: As
            :func:`~datagrunt.pdf.pdfium_engine.open_document` raises them.
        PDFCorruptError: If the file is not a readable PDF (``page_number``
            None), or, under ``strict``, for a page that fails (set).
        PDFSourceChangedError: If the file changes while it is extracted
            (see the module notes), ``strict`` or not.
        DatagruntPDFError: If its title or author is over A2's bound, a
            worker returns something that is not its pages, or, under
            ``strict``, a worker dies or runs out of time.
    """
    options = PipelineOptions(
        workers=workers,
        ocr=ocr,
        dpi=dpi,
        vision_dpi=vision_dpi,
        max_render_pixels=max_render_pixels,
        min_image_dimension=min_image_dimension,
        strict=strict,
    )
    pdf = PDFSource(path=_checked_path(source, "source"), password=password)
    with _opened_source(pdf.path) as stream:
        identity = SourceIdentity.of(os.fstat(stream.fileno()))
        digest = _stream_sha256(stream, identity.size, pdf.path)
        bound = dataclasses.replace(pdf, identity=identity)
        info, pages = _extracted_pages(bound, options)
        _check_unchanged(bound, os.fstat(stream.fileno()), "by the end of its extraction")
    return _document(bound, info, digest, pages)


def _extracted_pages(source: PDFSource, options: PipelineOptions) -> tuple[DocumentInfo, tuple[Page, ...]]:
    """The document's /Info entries and its pages, in this process or the pool (see the module notes)."""
    with _opened_checked(source, "when pdfium opened it") as handle:
        info, count = document_metadata(handle), handle.page_count
        if _runs_sequentially(options.workers, count):
            return info, _pages_in(handle, source, range(count), options)
    return info, _pooled_pages(source, _chunks(count, options.workers), options)


def _document(source: PDFSource, info: DocumentInfo, digest: str, pages: tuple[Page, ...]) -> PDFDocument:
    """Steps 6-7: A6's scope over the pages, then the A1 document and its metadata."""
    scoped = apply_scope(pages)
    name = source.path.name
    metadata = DocumentMetadata(
        doc_id=DocumentMetadata.make_doc_id(name, digest),
        source_name=name,
        sha256=digest,
        page_count=len(pages),
        title=info.title,
        author=info.author,
    )
    return PDFDocument(
        metadata=metadata, pages=scoped.pages, sections=scoped.sections, header_context=scoped.header_context
    )


def _opened_source(path: Path) -> BinaryIO:
    """``path`` opened for reading, once: the file the hash is read through and the identity taken from.

    A path that is not a regular file is refused before it is opened (a FIFO
    would block), as A2's ``open_document`` refuses it.
    """
    if not path.is_file():
        raise FileNotFoundError(errno.ENOENT, "not an existing file", str(path))
    return path.open("rb")


def _stream_sha256(stream: BinaryIO, size: int, path: Path) -> str:
    """The SHA-256 of the first ``size`` bytes of ``stream``, read in chunks of :data:`_HASH_CHUNK_BYTES`."""
    digest = hashlib.sha256()
    remaining = size
    for _ in range(-(-size // _HASH_CHUNK_BYTES)):
        block = _read_chunk(stream, min(_HASH_CHUNK_BYTES, remaining))
        if not block:
            break
        digest.update(block)
        remaining -= len(block)
    if remaining:
        raise PDFSourceChangedError(
            f"{path} changed while it was hashed: {size - remaining} of its {size} bytes could be read"
        )
    return digest.hexdigest()


def _read_chunk(stream: BinaryIO, size: int) -> bytes:
    """The next at most ``size`` bytes of ``stream`` (fewer only at its end)."""
    return stream.read(size)


@contextmanager
def _opened_checked(source: PDFSource, when: str) -> Iterator[DocumentHandle]:
    """The document, opened by pdfium from ``source.path`` and checked against ``source.identity`` at once."""
    with open_document(source.path, password=source.password) as handle:
        _check_unchanged(source, os.stat(source.path), when)
        yield handle


def _check_unchanged(source: PDFSource, status: os.stat_result, when: str) -> None:
    """Raise PDFSourceChangedError if ``status`` is not ``source.identity`` (nothing to check without one)."""
    if source.identity is None:
        return
    changes = source.identity.changes(SourceIdentity.of(status))
    if changes:
        disagree = "its pages and its sha256 could disagree, so it is not extracted"
        raise PDFSourceChangedError(f"{source.path} changed {when}: {', '.join(changes)}; {disagree}")


def extract_page_at(handle_source: PDFSource, index: int, options: PipelineOptions) -> Page:
    """Extract the page at 0-based ``index``: steps 1-5 (see the module notes), on a load of its own.

    Picklable in and out (a worker's step): the document is opened from
    ``handle_source`` and closed before returning, on every path.

    Raises:
        TypeError: If ``handle_source`` is not a PDFSource, ``options`` not
            a PipelineOptions, or ``index`` not an int (bool rejected).
        IndexError: If ``index`` is outside ``0..page_count - 1``.
        FileNotFoundError, PermissionError, PDFEncryptedError: As
            :func:`~datagrunt.pdf.pdfium_engine.open_document` raises them;
            and PDFCorruptError for an unreadable file.
        PDFCorruptError: Under ``options.strict``, for a page that fails,
            naming it (``page_number`` is set).
        PDFSourceChangedError: If ``handle_source.identity`` is set and the
            file at its path is not that file, when pdfium opens it or once
            the page is extracted.
    """
    _check_arguments(handle_source, options)
    with _opened_checked(handle_source, "when pdfium opened it") as handle:
        _checked_index(index, handle.page_count)
        page = _isolated_page(handle, handle_source, index, options)
    _check_unchanged(handle_source, os.stat(handle_source.path), f"by the time page {index + 1} was extracted")
    return page


def _check_arguments(handle_source: object, options: object) -> None:
    """Require a PDFSource and a PipelineOptions."""
    if not isinstance(handle_source, PDFSource):
        raise TypeError(f"handle_source must be a PDFSource, got {type(handle_source).__name__}")
    if not isinstance(options, PipelineOptions):
        raise TypeError(f"options must be a PipelineOptions, got {type(options).__name__}")


def _isolated_page(handle: DocumentHandle, source: PDFSource, index: int, options: PipelineOptions) -> Page:
    """Steps 1-4 for the page, each isolated (step 5): a window of one page."""
    (page,) = _window_pages(handle, source, range(index, index + 1), options)
    return page


class _Assessed(NamedTuple):
    """A page through steps 1-3 but its background check.

    ``background_dpi`` is the DPI its background is drawn at (the gate
    passed it), why it cannot be drawn (a str), or None (the gate did not
    pass it).
    """

    page_index: int
    raw: RawPage
    recovered: RecoveredText | None
    assessment: PageAssessment
    background_dpi: int | str | None


# A page's background render: the page drawn, why not, or the exception its batch raised (each page's step 5).
_Drawn = RenderedPage | RenderRefused | RenderFailed | Exception


def _window_pages(handle: DocumentHandle, source: PDFSource, window: range, options: PipelineOptions) -> Iterator[Page]:
    """The window's pages, each as it is done: steps 1-3 for each, its gated pages drawn in one worker, then step 4.

    Each step is isolated.
    """
    staged = [_isolated(handle, index, options, partial(_assessed, handle, source, index, options)) for index in window]
    drawn = _backgrounds(source, staged, options)
    for item in staged:  # bounded: the window's pages
        if isinstance(item, Page):
            yield item
            continue
        yield _isolated(
            handle, item.page_index, options, partial(_finished, item, drawn.get(item.page_index), source, options)
        )


def _isolated(handle: DocumentHandle, index: int, options: PipelineOptions, step: Callable[[], _T]) -> _T | Page:
    """``step()`` of the page at ``index`` (step 5): a failure is an error page; under ``strict`` it raises."""
    page_number = index + 1
    try:
        return step()
    except Exception as exc:  # step 5: per-page isolation; an interrupt (BaseException) is never caught
        if not options.strict:
            return _error_page(handle, index, f"{type(exc).__name__}: {exc}")
        if isinstance(exc, PDFCorruptError) and exc.page_number == page_number:
            raise
        message = f"page {page_number}: {type(exc).__name__}: {exc}"
        raise PDFCorruptError(message, page_number=page_number) from exc


def _assessed(handle: DocumentHandle, source: PDFSource, index: int, options: PipelineOptions) -> Page | _Assessed:
    """Steps 1-3 (A2, recovery when the page drops text, A3, the gate); a page A2 could not read is fallback_page's."""
    raw = extract_page(handle, index)
    if isinstance(raw, PageFailure):
        if options.strict:
            raise PDFCorruptError(raw.message, page_number=raw.page_number)
        return fallback_page(
            handle, raw, source=source.path, password=source.password, ocr=options.ocr, options=_vision(options)
        )
    dropped = raw.text.stats.dropped_text_objects
    recovered = recover_dropped_text(handle, index) if dropped > 0 else None
    assessment = assess_page(raw, ocr=options.ocr, recovered=recovered)
    return _Assessed(index, raw, recovered, assessment, _background_dpi(raw, assessment, options))


def _background_dpi(raw: RawPage, assessment: PageAssessment, options: PipelineOptions) -> int | str | None:
    """The DPI the page's background is drawn at, why it cannot be drawn, or None when the gate does not pass it."""
    if not needs_background(raw, assessment):
        return None
    bound = min(options.max_render_pixels, _BACKGROUND_MAX_PIXELS)
    try:
        return effective_dpi(_BACKGROUND_DPI, bound, raw.geometry.width_pt, raw.geometry.height_pt)
    except RenderRefused as exc:
        return str(exc)


def _backgrounds(source: PDFSource, staged: list[Page | _Assessed], options: PipelineOptions) -> dict[int, _Drawn]:
    """Each gated page's background render, by page index: the window's, drawn in one worker (see the module notes).

    A render call that raises is each of its pages' outcome: each raises it
    in its own step 4, so the window's other pages are untouched.
    """
    pages = tuple(
        (item.page_index, item.background_dpi)
        for item in staged
        if isinstance(item, _Assessed) and isinstance(item.background_dpi, int)
    )
    if not pages:
        return {}
    try:
        drawn = render_pages(
            source.path,
            pages,
            password=source.password,
            memory_cap_bytes=_vision(options).memory_cap_bytes,
            timeout_s=_BACKGROUND_START_S + _BACKGROUND_PAGE_S * len(pages),
            colour=True,
            without_text=True,
        )
    except Exception as exc:  # step 5, page by page: see the docstring
        return dict.fromkeys((index for index, _dpi in pages), exc)
    return {index: result for (index, _dpi), result in zip(pages, drawn, strict=True)}


def _finished(item: _Assessed, drawn: _Drawn | None, source: PDFSource, options: PipelineOptions) -> Page:
    """Step 3's background check, then step 4: the page assessed again with its backdrop (or why not), then routed."""
    background = _background(item, drawn)
    assessment = item.assessment
    if background is not None:
        assessment = assess_page(item.raw, ocr=options.ocr, recovered=item.recovered, background=background)
    return _routed(item.raw, assessment, source, options)


def _background(item: _Assessed, drawn: _Drawn | None) -> Backdrop | str | None:
    """The page's backdrop, why it could not be drawn, or None (the gate did not pass it); its batch's error raised."""
    if isinstance(item.background_dpi, str):
        return item.background_dpi
    if drawn is None:
        return None
    if isinstance(drawn, RenderedPage):
        return Backdrop(pixels=drawn.pixels, px_width=drawn.px_width, px_height=drawn.px_height)
    if isinstance(drawn, (RenderRefused, RenderFailed)):
        return str(drawn)
    raise drawn


def _routed(raw: RawPage, assessment: PageAssessment, source: PDFSource, options: PipelineOptions) -> Page:
    """Step 4: a vision page is A5b's vision_page; a digital or hybrid one is assembled."""
    if assessment.route == "vision":
        return vision_page(
            raw,
            assessment,
            source=source.path,
            password=source.password,
            min_image_dimension=options.min_image_dimension,
            options=_vision(options),
        )
    return _assembled_page(raw, assessment, source, options)


def _vision(options: PipelineOptions) -> VisionOptions:
    """How A5b renders and reads a page under ``options``: its vision DPI and pixel bound, A5b's other defaults."""
    return VisionOptions(vision_dpi=options.vision_dpi, max_render_pixels=options.max_render_pixels)


def _assembled_page(raw: RawPage, assessment: PageAssessment, source: PDFSource, options: PipelineOptions) -> Page:
    """A digital or hybrid page: A4a's lattice tables, then A4b's assembly."""
    lattice = find_lattice_tables(raw.segments.segments, assessment.pieces)
    return assemble_page(
        raw,
        assessment,
        lattice,
        source=source.path,
        password=source.password,
        min_image_dimension=options.min_image_dimension,
    )


def _error_page(handle: DocumentHandle | None, index: int, error: str) -> Page:
    """The A1 error page of the page at ``index``: no elements, no quality, routed vision (see the module notes)."""
    (width, height), warnings = _error_page_size(handle, index)
    return Page(
        page_number=index + 1,
        width_pt=width,
        height_pt=height,
        status="error",
        route="vision",
        quality=None,
        error=error,
        warnings=warnings,
    )


def _error_page_size(handle: DocumentHandle | None, index: int) -> tuple[tuple[float, float], tuple[str, ...]]:
    """The page's displayed size, or pdfium's default page size and a warning saying why.

    With no ``handle`` (a worker's page that never came back) the size is not read.
    """
    if handle is None:
        return _DEFAULT_PAGE_SIZE, (_default_size_warning("its worker process returned nothing"),)
    try:
        geometry = page_geometry(handle, index)
    except Exception as exc:  # an error page is never dropped: its size is read when it can be
        return _DEFAULT_PAGE_SIZE, (_default_size_warning(f"{type(exc).__name__}: {exc}"),)
    return (geometry.width_pt, geometry.height_pt), ()


def _default_size_warning(reason: str) -> str:
    """The warning of an error page whose size is pdfium's default page size, not its own."""
    width, height = _DEFAULT_PAGE_SIZE
    default = f"{width:g} x {height:g} pt is pdfium's default page size, not this page's"
    return f"the page's size could not be read ({reason}); {default}"


def _pages_in(handle: DocumentHandle, source: PDFSource, indices: range, options: PipelineOptions) -> tuple[Page, ...]:
    """The pages at ``indices`` of the open document, in order (:func:`_iter_pages`)."""
    return tuple(_iter_pages(handle, source, indices, options))


def _iter_pages(handle: DocumentHandle, source: PDFSource, indices: range, options: PipelineOptions) -> Iterator[Page]:
    """The pages at ``indices`` of the open document, in order, each as it is done; :data:`_WINDOW_PAGES` at a time."""
    for start in range(indices.start, indices.stop, _WINDOW_PAGES):  # bounded: one window per _WINDOW_PAGES pages
        yield from _window_pages(handle, source, range(start, min(start + _WINDOW_PAGES, indices.stop)), options)


def _is_distributed_env() -> bool:
    """True inside a distributed runner (Spark, Beam, Flink, Celery), as v4 detects one."""
    return any(key in os.environ for key in _DISTRIBUTED_KEYS)


def _runs_sequentially(workers: int, page_count: int) -> bool:
    """True when the pages extract in this process: one worker, one page, or a distributed runner."""
    return workers == 1 or page_count == 1 or _is_distributed_env()


def _chunks(page_count: int, workers: int) -> tuple[range, ...]:
    """``min(workers, page_count)`` contiguous runs of page indices, in order, sizes differing by at most one."""
    count = min(workers, page_count)
    size, extra = divmod(page_count, count)
    starts = [index * size + min(index, extra) for index in range(count + 1)]
    return tuple(range(start, stop) for start, stop in zip(starts, starts[1:]))


def _extract_chunk(source: PDFSource, pages: range, options: PipelineOptions) -> Iterator[Page]:
    """A worker's work: open the document itself, extract ``pages`` in order, each as it is done; close it.

    The file is checked against ``source.identity`` once pdfium opens it and
    again after the last page.
    """
    span = _span(pages)
    with _opened_checked(source, f"when the worker for pages {span} opened it") as handle:
        yield from _iter_pages(handle, source, pages, options)
    _check_unchanged(source, os.stat(source.path), f"by the time the worker for pages {span} finished them")


def _worker_main(message: object, replies: IO[bytes]) -> None:
    """The page worker, run by the launcher's bootstrap (key ``"pages"``): a chunk's request in; replies out.

    It sends each page, pickled, as soon as it is extracted, in page order;
    then, its file re-checked after its last page (and at open), done. An
    exception that stops the chunk (``strict``'s PDFCorruptError,
    PDFSourceChangedError, an open error, a request it cannot read) is sent
    pickled instead of what is left, for the parent to raise.
    """
    try:
        for page in _extract_chunk(*_chunk_request(message)):  # bounded: the chunk's pages
            send_message(replies, _PAGE + pickle.dumps(page))
    except Exception as exc:  # sent, and raised by the parent: never swallowed here
        send_message(replies, _ERROR + pickle.dumps(exc))
        return
    send_message(replies, _DONE)


def _chunk_payload(source: PDFSource, pages: range, options: PipelineOptions) -> dict[str, object]:
    """A chunk's request, as JSON-ready values (the password on the worker's stdin only)."""
    identity = None if source.identity is None else dataclasses.asdict(source.identity)
    return {
        "source": str(source.path),
        "password": source.password,
        "identity": identity,
        "pages": [pages.start, pages.stop],
        "options": dataclasses.asdict(options),
    }


def _chunk_request(message: object) -> tuple[PDFSource, range, PipelineOptions]:
    """The source, pages and options a worker reads (JSON from its parent), each checked as at construction."""
    if not isinstance(message, dict) or set(message) != set(_CHUNK_REQUEST_KEYS):
        found = sorted(message) if isinstance(message, dict) else type(message).__name__
        raise ValueError(f"the page worker's request must hold exactly {_CHUNK_REQUEST_KEYS}, got {found}")
    bounds = message["pages"]
    ints = isinstance(bounds, list) and len(bounds) == 2 and all(type(bound) is int for bound in bounds)
    if not ints or not 0 <= bounds[0] < bounds[1]:
        raise ValueError(f"pages must be [start, stop] with 0 <= start < stop, got {bounds!r}")
    identity = message["identity"]
    source = PDFSource(
        path=message["source"],
        password=message["password"],
        identity=None if identity is None else SourceIdentity(**identity),
    )
    return source, range(*bounds), PipelineOptions(**message["options"])


@dataclass(frozen=True, slots=True)
class _Worker:
    """One running page worker: its chunk, its process, the file its stderr goes to, and when it started."""

    pages: range
    child: "subprocess.Popen[bytes]"
    errors: IO[bytes]
    started: float  # time.monotonic(), just before it was started


class _Replies:
    """What one worker sent, read as it came (in a thread of its own): its pages, and how its replies ended.

    ``ending`` is ``"done"``, ``"ended"`` (its stdout closed: it died),
    ``"expired"`` (killed at the bound it was given, ``bound_s``) or
    ``"too long"`` (a message over its bound: ``detail``; it was killed);
    empty while it is read. ``error`` is what it raised for the pool to raise:
    its pickled exception, or a reply that is not its next page.
    """

    __slots__ = ("bound_s", "detail", "ending", "error", "pages")

    def __init__(self) -> None:
        self.pages: list[Page] = []
        self.ending = ""
        self.detail = ""
        self.bound_s = 0.0
        self.error: Exception | None = None


def _page_bound_s(options: PipelineOptions) -> float:
    """A page's bound in its worker (see the module notes): never its size.

    Its own work (:data:`_WORKER_PAGE_S`) and its share of its window's
    background render (:data:`_BACKGROUND_PAGE_S`); and, unless
    ``ocr="never"`` (then no page is read by OCR), its render and OCR budget
    (:func:`_vision`'s ``page_budget_s``, the one the worker reads with) and
    the reap of its last call (:data:`_CALL_REAP_S`).
    """
    bound = _WORKER_PAGE_S + _BACKGROUND_PAGE_S
    if options.ocr != "never":
        bound += _vision(options).page_budget_s + _CALL_REAP_S
    return bound


def _reply_bound_s(chunk: range, received: int, options: PipelineOptions) -> float:
    """Seconds the worker for ``chunk`` has for its next reply once it has sent ``received`` pages (module notes).

    A page's bound (:func:`_page_bound_s`); for the first page of a window,
    also the window's lead: its other pages' steps 1-3 and background
    shares, and its background render's start and reap; for the chunk's
    first page, also :data:`_WORKER_START_S`. After its last page,
    :data:`_WORKER_PAGE_S` for its close and its last check of the file.
    """
    if received == len(chunk):
        return _WORKER_PAGE_S
    bound = _page_bound_s(options)
    if received % _WINDOW_PAGES == 0:
        others = min(_WINDOW_PAGES, len(chunk) - received) - 1
        bound += others * (_WORKER_PAGE_S + _BACKGROUND_PAGE_S) + _BACKGROUND_START_S + _CALL_REAP_S
    return (bound + _WORKER_START_S) if received == 0 else bound


def _pooled_pages(source: PDFSource, chunks: tuple[range, ...], options: PipelineOptions) -> tuple[Page, ...]:
    """Each chunk's pages from a worker process of its own, all at once; the pages in page order.

    Each worker's replies are read as they come, in a thread of its own.
    Every worker is reaped before this returns or raises (see the module notes).
    """
    command = _pool_command()
    workers: list[_Worker] = []
    readers: list[tuple[threading.Thread, _Replies]] = []
    try:
        for chunk in chunks:
            workers.append(_started(command, chunk))
            _send_request(
                workers[-1], request_payload("pages", _chunk_payload(source, chunk, options), MAX_REQUEST_BYTES)
            )
            readers.append(_reading(workers[-1], options))
        return _gathered(workers, readers, options)
    finally:
        _shut_down(workers, readers)


def _pool_command() -> list[str]:
    """The page worker's command line; DatagruntPDFError, before anything starts, if there is no interpreter."""
    try:
        return worker_command("a page worker")
    except NoInterpreter as exc:
        raise DatagruntPDFError(f"{exc}: pass workers=1 to extract in this process") from exc


def _started(command: list[str], pages: range) -> _Worker:
    """Start the worker for ``pages``, its stderr to a temporary file."""
    errors = tempfile.TemporaryFile()
    try:
        started = time.monotonic()
        return _Worker(pages=pages, child=start_worker(command, errors), errors=errors, started=started)
    except BaseException:
        errors.close()
        raise


def _send_request(worker: _Worker, payload: bytes) -> None:
    """Write the worker's request to its stdin and close it.

    A worker that has already died stops reading: its replies then end
    without its pages, and its exit code and stderr say why.
    """
    stdin = worker.child.stdin
    if stdin is None:
        raise ValueError("the worker's stdin must be a pipe")
    try:
        stdin.write(payload)
        stdin.close()
    except OSError:  # it died at once: _chunk_pages reports it from its exit code and stderr
        pass


def _reading(worker: _Worker, options: PipelineOptions) -> tuple[threading.Thread, _Replies]:
    """Start reading the worker's replies in a thread of its own (#426): no worker waits for another's to be read."""
    replies = _Replies()
    thread = threading.Thread(target=_read_replies, args=(worker, replies, options), daemon=True)
    thread.start()
    return thread, replies


def _read_replies(worker: _Worker, replies: _Replies, options: PipelineOptions) -> None:
    """The reader thread: the worker's replies into ``replies``, each within its bound; what it raises recorded."""
    try:
        _read_into(worker, replies, options)
    except Exception as exc:  # recorded, and raised by the pool in this chunk's turn: never swallowed
        replies.error = exc


def _read_into(worker: _Worker, replies: _Replies, options: PipelineOptions) -> None:
    """Read the worker's pages, then done, each within :func:`_reply_bound_s` of the reply before it (or its start)."""
    lost = f"the worker process extracting pages {_span(worker.pages)}"
    stdout = worker.child.stdout
    if stdout is None:
        raise ValueError("the worker's stdout must be a pipe")
    for _ in range(len(worker.pages) + 1):  # each page, then done
        bound_s = _reply_bound_s(worker.pages, len(replies.pages), options)
        # Counted from the reply before it, read as it came; the first page's from the worker's start.
        waited_s = time.monotonic() - worker.started if not replies.pages else 0.0
        try:
            message, expired = _next_reply(worker, stdout, max(0.0, bound_s - waited_s))
        except MessageTooLong as exc:
            worker.child.kill()  # it may be blocked writing the rest: the pool reaps it
            replies.ending, replies.detail = "too long", str(exc)
            return
        if message is None:
            replies.ending, replies.bound_s = ("expired" if expired else "ended"), bound_s
            return
        if message[:1] == _DONE:
            _check_count(replies.pages, worker.pages, lost)
            replies.ending = "done"
            return
        replies.pages.append(_next_page(message, worker.pages, len(replies.pages), lost))


def _next_reply(worker: _Worker, stdout: IO[bytes], bound_s: float) -> tuple[bytes | None, bool]:
    """The worker's next reply within ``bound_s``, or None when its replies end; and whether it was killed at the bound.

    A timer kills the worker at the bound, which ends its stdout: no read
    outlives it. The timer is stopped before this returns, so a kill never
    reaches a reaped (reusable) pid.

    Raises:
        MessageTooLong: If the reply's length is over its bound (unread).
    """
    expired = threading.Event()
    killer = threading.Timer(bound_s, _expire, args=(worker.child, expired))
    killer.daemon = True
    killer.start()
    try:
        return read_message(stdout, _MAX_PAGE_MESSAGE_BYTES), expired.is_set()
    finally:
        killer.cancel()
        killer.join(REAP_TIMEOUT_S)


def _expire(child: "subprocess.Popen[bytes]", expired: threading.Event) -> None:
    """The timer's: the worker's reply is past its bound; kill it."""
    expired.set()
    child.kill()


def _next_page(message: bytes, chunk: range, received: int, lost: str) -> Page:
    """The page in the worker's reply, if it is its chunk's next one; its error raised; anything else fails loud."""
    page = _reply_page(message, lost)
    if received == len(chunk):
        raise DatagruntPDFError(f"{lost} sent more than its {len(chunk)} page(s)")
    due = chunk.start + received + 1
    if page.page_number != due:
        raise DatagruntPDFError(f"{lost} returned page {page.page_number} where page {due} was due")
    return page


def _reply_page(message: bytes, lost: str) -> Page:
    """The page in one of the worker's replies; its error raised; anything else fails loud."""
    kind, body = message[:1], message[1:]
    if kind == _ERROR:
        error = pickle.loads(body)  # the caller's own process image: see the module notes
        if not isinstance(error, Exception):
            raise DatagruntPDFError(f"{lost} sent a {type(error).__name__} as its error")
        raise error
    if kind != _PAGE:
        raise DatagruntPDFError(f"{lost} sent a reply of unknown kind {kind!r}")
    page = pickle.loads(body)  # the caller's own process image: see the module notes
    if not isinstance(page, Page):
        raise DatagruntPDFError(f"{lost} sent a {type(page).__name__}, not a page")
    return page


def _check_count(pages: list[Page], chunk: range, lost: str) -> None:
    """Fail loud if the worker said done before it sent each of its chunk's pages: a reply is checked, never trusted."""
    if len(pages) != len(chunk):
        raise DatagruntPDFError(f"{lost} said done after {len(pages)} of its {len(chunk)} page(s)")


def _gathered(
    workers: list[_Worker], readers: list[tuple[threading.Thread, _Replies]], options: PipelineOptions
) -> tuple[Page, ...]:
    """Each chunk's pages, in page order, once its reader ends; a chunk's error raised, its later workers killed."""
    pages: list[Page] = []
    for index, (worker, (thread, replies)) in enumerate(zip(workers, readers)):
        try:
            _joined(thread, worker, options)
            pages.extend(_chunk_pages(worker, replies, options))
        except BaseException:
            _kill_all(workers[index + 1 :])
            raise
    return tuple(pages)


def _joined(thread: threading.Thread, worker: _Worker, options: PipelineOptions) -> None:
    """Wait for the reader of ``worker``: within each of its replies' bounds, plus each timer's join."""
    lifetime_s = sum(
        _reply_bound_s(worker.pages, received, options) + REAP_TIMEOUT_S for received in range(len(worker.pages) + 1)
    )
    thread.join(lifetime_s)
    if thread.is_alive():
        worker.child.kill()
        thread.join(REAP_TIMEOUT_S)
    if thread.is_alive():
        raise DatagruntPDFError(
            f"the replies of the worker for pages {_span(worker.pages)} could not be read to their end"
        )


def _chunk_pages(worker: _Worker, replies: _Replies, options: PipelineOptions) -> tuple[Page, ...]:
    """The pages the worker sent, then error pages for those it did not (it died, ran out of time, or overran a bound).

    A worker that sent each of its pages but ended before its last check of
    the file keeps them (see the module notes).
    """
    if replies.error is not None:
        raise replies.error
    if replies.ending == "done":
        reaped(worker.child, REAP_TIMEOUT_S)
        return tuple(replies.pages)
    unsent = range(worker.pages.start + len(replies.pages), worker.pages.stop)
    if not unsent:
        return tuple(replies.pages)
    return (*replies.pages, *_lost_chunk(unsent, _why_lost(worker, replies, unsent), options))


def _why_lost(worker: _Worker, replies: _Replies, unsent: range) -> str:
    """Why the worker's ``unsent`` pages never came: killed at their bound, a message over its bound, or it died."""
    lost = f"the worker process extracting pages {_span(worker.pages)}"
    if replies.ending == "expired":
        since = "its start" if not replies.pages else f"page {unsent.start}"
        return f"{lost} did not return page {unsent.start + 1} within {replies.bound_s:g} s of {since}: it was killed"
    if replies.ending == "too long":
        return f"{lost} sent {replies.detail}: it was killed"
    what = "them" if not replies.pages else f"pages {_span(unsent)}"
    ended = f"{lost} ended (exit code {reaped(worker.child, _EXIT_GRACE_S)}) before returning {what}"
    tail = stderr_tail(worker.errors)
    return f"{ended}; its stderr ends: {tail}" if tail else ended


def _lost_chunk(pages: range, reason: str, options: PipelineOptions) -> tuple[Page, ...]:
    """The error pages of a chunk's pages its worker never returned; under ``strict``, the reason raised."""
    if options.strict:
        raise DatagruntPDFError(reason)
    return tuple(_error_page(None, index, reason) for index in pages)


def _kill_all(workers: list[_Worker]) -> None:
    """Kill each worker in ``workers`` that is still running (a reaped one is never signalled)."""
    for worker in workers:
        worker.child.kill()


def _shut_down(workers: list[_Worker], readers: list[tuple[threading.Thread, _Replies]]) -> None:
    """End the pool: kill each worker still running, wait for its reader, then close and reap it.

    Each reader stops its timers before it ends, so a kill never reaches a reaped (reusable) pid.
    """
    _kill_all(workers)
    for thread, _replies in readers:
        thread.join(REAP_TIMEOUT_S)
    for worker in workers:
        close_pipes(worker.child)
        reaped(worker.child, 0.0)
        worker.errors.close()


def _span(pages: range) -> str:
    """``pages`` (0-based indices) as 1-based page numbers, ``"first-last"``."""
    return f"{pages.start + 1}-{pages.stop}"
