"""The v5 PDF document pipeline (#372, A8a): spec §3.3 steps 1-5, page by page, then the document.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A7's
``PDFWriter`` and A8b's ``PDFReader`` stand on it.

:class:`PipelineOptions` holds the reader's and writer's extraction options
and runs the spec's construction checks (invariant 2) when it is built, so
a bad option fails before any work. :class:`PDFSource` is where a document
is opened from (its path and password), in this process or in a worker.

:func:`extract_page_at` is one page's steps, in order; no stage is skipped:

1. A2 :func:`~datagrunt.pdf.pdfium_engine.extract_page`. A
   :class:`~datagrunt.pdf.pdfium_engine.PageFailure` raises
   :class:`~datagrunt.pdf.exceptions.PDFCorruptError` naming the page under
   ``strict``; else it is :func:`_fallback_page`'s.
2. A2 :func:`~datagrunt.pdf.pdfium_engine.recover_dropped_text`, when the
   page's textpage drops text objects (``TextStats.dropped_text_objects``
   over 0).
3. A3 :func:`~datagrunt.pdf.quality.assess_page`, with the ``ocr`` option
   and the recovery.
4. The route: a digital or hybrid page is A4a's lattice tables, then A4b's
   :func:`~datagrunt.pdf.assembly.assemble_page`; a vision page is
   :func:`_vision_page`'s.
5. Isolation: any other exception (an ``Exception``; an interrupt is never
   caught) is an error page whose ``error`` is ``"<type>: <message>"``,
   recorded, never dropped. Under ``strict`` it raises a PDFCorruptError
   naming the page, chained from it (a PDFCorruptError that already names
   the page is raised as it is).

Two stand-ins hold A5b's place, each one small function A5b replaces:
:func:`_vision_page` (a vision page is ``assemble_page``'s
``"ocr_unavailable"`` page) and :func:`_fallback_page` (a page A2 cannot
read is an error page).

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
- A worker extracts its whole chunk, re-checks the file (see below), then
  sends each page as one length-prefixed pickled message, in page order,
  and a done message; or, instead, the pickled exception that stopped the
  chunk. Each message is read within :data:`_MAX_PAGE_MESSAGE_BYTES` (the
  1 GiB asset limit); a longer one is refused unread. The parent checks
  each reply is a page, the count and the order.
- Unpickling a worker's pages is acceptable where it would not be for
  bytes from elsewhere: the worker is the caller's own process image, run
  with the caller's privileges and exact ``sys.path``. A worker compromised
  through pdfium could already do anything the caller can, and in this
  process pdfium runs in the caller anyway (``workers=1``). The render
  worker, whose replies are pixels, pickles nothing.
- Every worker must return by one deadline,
  :data:`_WORKER_START_S` + :data:`_WORKER_PAGE_S` x the longest chunk,
  counted from the pool's start. One that has not leaves error pages for
  its chunk, and is killed when the pool ends.
- A worker that dies without returning (killed, crashed, ``os._exit``), or
  sends a message over its bound, leaves error pages for exactly its
  chunk's pages, never a lost page, their error ending with its stderr's
  last 4 KiB; the other chunks extract. Under ``strict`` any of these
  raises a DatagruntPDFError naming the pages instead.
- An exception a worker raises outside the per-page isolation (``strict``'s
  PDFCorruptError, an open error) is raised here.
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
and OCR, A5b). Its size is the page's, read with
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
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, BinaryIO, get_args

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
    _checked_index,
    _checked_password,
    _checked_path,
    document_metadata,
    extract_page,
    open_document,
    page_geometry,
    recover_dropped_text,
)
from datagrunt.pdf.quality import OcrMode, PageAssessment, assess_page
from datagrunt.pdf.render import _MIN_RENDER_PIXELS, _MIN_TARGET_DPI
from datagrunt.pdf.scope import apply_scope

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

# A worker's deadline: this, plus _WORKER_PAGE_S for each page of the longest chunk, from the pool's start.
# Measured here: a worker imports datagrunt in ~0.6 s, and the corpus's slowest page (95 pages)
# extracts in ~0.5 s, so each bound is over 100 times what it covers.
_WORKER_START_S = 60.0
_WORKER_PAGE_S = 60.0
_EXIT_GRACE_S = 1.0  # a worker whose replies ended without its pages is given this to exit

# A page worker's replies, each one length-prefixed message: a page (pickled), the chunk done, or the
# exception that stopped it (pickled). Each is read within the asset limit.
_PAGE, _DONE, _ERROR = b"P", b"D", b"E"
_MAX_PAGE_MESSAGE_BYTES = _MAX_ASSET_BYTES
_CHUNK_REQUEST_KEYS = ("source", "password", "identity", "pages", "options")


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
    """Steps 1-4 for the page, isolated (step 5): a failure is an error page, or under ``strict`` raises."""
    page_number = index + 1
    try:
        return _routed_page(handle, source, index, options)
    except Exception as exc:  # step 5: per-page isolation; an interrupt (BaseException) is never caught
        if not options.strict:
            return _error_page(handle, index, f"{type(exc).__name__}: {exc}")
        if isinstance(exc, PDFCorruptError) and exc.page_number == page_number:
            raise
        message = f"page {page_number}: {type(exc).__name__}: {exc}"
        raise PDFCorruptError(message, page_number=page_number) from exc


def _routed_page(handle: DocumentHandle, source: PDFSource, index: int, options: PipelineOptions) -> Page:
    """Steps 1-4: A2, recovery when the page drops text, A3, then the page's route."""
    raw = extract_page(handle, index)
    if isinstance(raw, PageFailure):
        if options.strict:
            raise PDFCorruptError(raw.message, page_number=raw.page_number)
        return _fallback_page(handle, index, raw)
    dropped = raw.text.stats.dropped_text_objects
    recovered = recover_dropped_text(handle, index) if dropped > 0 else None
    assessment = assess_page(raw, ocr=options.ocr, recovered=recovered)
    if assessment.route == "vision":
        return _vision_page(raw, assessment, source, options)
    return _assembled_page(raw, assessment, source, options)


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


def _vision_page(raw: RawPage, assessment: PageAssessment, source: PDFSource, options: PipelineOptions) -> Page:
    """A vision page. A stand-in for A5b's ``vision_page``, which replaces this function.

    Until A5b lands, a vision page is :func:`_assembled_page`'s: A4b's
    :func:`~datagrunt.pdf.assembly.assemble_page` makes it an
    ``"ocr_unavailable"`` page (A1: its digital text kept as its elements,
    never ``integrity_verified``), with a warning that OCR has not run.
    """
    return _assembled_page(raw, assessment, source, options)


def _fallback_page(handle: DocumentHandle, index: int, failure: PageFailure) -> Page:
    """A page A2 could not read. A stand-in for A5b's ``fallback_page``, which replaces this function.

    Until A5b lands (render and OCR, an ``"ocr_fallback"`` page), it is an
    error page whose ``error`` is the stage's PDFCorruptError and the stage.
    """
    return _error_page(handle, index, f"PDFCorruptError: {failure.message} (stage {failure.stage!r})")


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
    """The pages at ``indices`` of the open document, in order, each one's steps isolated."""
    return tuple(_isolated_page(handle, source, index, options) for index in indices)


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


def _extract_chunk(source: PDFSource, pages: range, options: PipelineOptions) -> tuple[Page, ...]:
    """A worker's work: open the document itself, extract ``pages`` in order, close it.

    The file is checked against ``source.identity`` once pdfium opens it and
    again after the last page.
    """
    span = _span(pages)
    with _opened_checked(source, f"when the worker for pages {span} opened it") as handle:
        extracted = _pages_in(handle, source, pages, options)
    _check_unchanged(source, os.stat(source.path), f"by the time the worker for pages {span} finished them")
    return extracted


def _worker_main(message: object, replies: IO[bytes]) -> None:
    """The page worker, run by the launcher's bootstrap (key ``"pages"``): a chunk's request in; replies out.

    It extracts the whole chunk (its file re-checked at open and after its
    last page), then sends each page, pickled, in page order, then done.
    An exception that stops the chunk (``strict``'s PDFCorruptError,
    PDFSourceChangedError, an open error, a request it cannot read) is sent
    pickled instead, for the parent to raise.
    """
    try:
        pages = _extract_chunk(*_chunk_request(message))
    except Exception as exc:  # sent, and raised by the parent: never swallowed here
        send_message(replies, _ERROR + pickle.dumps(exc))
        return
    for page in pages:
        send_message(replies, _PAGE + pickle.dumps(page))
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
    """One running page worker: its chunk, its process, and the file its stderr goes to."""

    pages: range
    child: "subprocess.Popen[bytes]"
    errors: IO[bytes]


def _pooled_pages(source: PDFSource, chunks: tuple[range, ...], options: PipelineOptions) -> tuple[Page, ...]:
    """Each chunk's pages from a worker process of its own, all at once; the pages in page order.

    Every worker is reaped before this returns or raises (see the module notes).
    """
    command = _pool_command()
    budget_s = _WORKER_START_S + _WORKER_PAGE_S * max(len(chunk) for chunk in chunks)
    deadline = time.monotonic() + budget_s
    workers: list[_Worker] = []
    killer = _deadline_killer(workers, deadline)
    try:
        for chunk in chunks:
            workers.append(_started(command, chunk))
            _send_request(
                workers[-1], request_payload("pages", _chunk_payload(source, chunk, options), MAX_REQUEST_BYTES)
            )
        return tuple(page for worker in workers for page in _collected(worker, deadline, budget_s, options))
    finally:
        _shut_down(workers, killer)


def _pool_command() -> list[str]:
    """The page worker's command line; DatagruntPDFError, before anything starts, if there is no interpreter."""
    try:
        return worker_command("a page worker")
    except NoInterpreter as exc:
        raise DatagruntPDFError(f"{exc}: pass workers=1 to extract in this process") from exc


def _deadline_killer(workers: list[_Worker], deadline: float) -> threading.Timer:
    """A timer that kills, at ``deadline``, every worker then in ``workers`` (one still running ends its replies)."""
    killer = threading.Timer(max(0.0, deadline - time.monotonic()), _kill_all, args=(workers,))
    killer.daemon = True
    killer.start()
    return killer


def _kill_all(workers: list[_Worker]) -> None:
    """Kill each worker in ``workers`` that is still running (a reaped one is never signalled)."""
    for worker in tuple(workers):
        worker.child.kill()


def _started(command: list[str], pages: range) -> _Worker:
    """Start the worker for ``pages``, its stderr to a temporary file."""
    errors = tempfile.TemporaryFile()
    try:
        return _Worker(pages=pages, child=start_worker(command, errors), errors=errors)
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
    except OSError:  # it died at once: _collected reports it from its exit code and stderr
        pass


def _collected(worker: _Worker, deadline: float, budget_s: float, options: PipelineOptions) -> tuple[Page, ...]:
    """The worker's pages; its chunk's error pages if it ran out of time, died first or overran a bound."""
    lost = f"the worker process extracting pages {_span(worker.pages)}"
    try:
        pages = _received_pages(worker, lost)
    except MessageTooLong as exc:
        return _lost_chunk(worker.pages, f"{lost} sent {exc}: it was killed", options)
    if pages is not None:
        reaped(worker.child, REAP_TIMEOUT_S)
        return pages
    if time.monotonic() >= deadline:
        return _lost_chunk(worker.pages, f"{lost} did not return them within {budget_s:g} s: it was killed", options)
    ended = f"{lost} ended (exit code {reaped(worker.child, _EXIT_GRACE_S)}) before returning them"
    tail = stderr_tail(worker.errors)
    return _lost_chunk(worker.pages, f"{ended}; its stderr ends: {tail}" if tail else ended, options)


def _received_pages(worker: _Worker, lost: str) -> tuple[Page, ...] | None:
    """The worker's pages, checked, once it says done; None if its replies end first (it died or was killed)."""
    stdout = worker.child.stdout
    if stdout is None:
        raise ValueError("the worker's stdout must be a pipe")
    pages: list[Page] = []
    for _ in range(len(worker.pages) + 1):  # each page, then done
        message = read_message(stdout, _MAX_PAGE_MESSAGE_BYTES)
        if message is None:
            return None
        if message[:1] == _DONE:
            return _checked_chunk(tuple(pages), worker.pages, lost)
        pages.append(_reply_page(message, lost))
    raise DatagruntPDFError(f"{lost} sent more than its {len(worker.pages)} page(s)")


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


def _lost_chunk(pages: range, reason: str, options: PipelineOptions) -> tuple[Page, ...]:
    """The error pages of a chunk its worker never returned; under ``strict``, the reason raised."""
    if options.strict:
        raise DatagruntPDFError(reason)
    return tuple(_error_page(None, index, reason) for index in pages)


def _checked_chunk(pages: tuple[Page, ...], chunk: range, lost: str) -> tuple[Page, ...]:
    """``pages`` if they are the chunk's, in order: a worker's reply is checked, never trusted."""
    expected = [index + 1 for index in chunk]
    numbers = [page.page_number for page in pages]
    if numbers != expected:
        raise DatagruntPDFError(f"{lost} returned pages {numbers}, not pages {expected}")
    return pages


def _shut_down(workers: list[_Worker], killer: threading.Timer) -> None:
    """End the pool: stop the deadline's timer, then close, kill (if still running) and reap each worker.

    The timer is stopped before any worker is reaped, so a kill never reaches a reaped (reusable) pid.
    """
    killer.cancel()
    killer.join(REAP_TIMEOUT_S)
    for worker in workers:
        close_pipes(worker.child)
        reaped(worker.child, 0.0)
        worker.errors.close()


def _span(pages: range) -> str:
    """``pages`` (0-based indices) as 1-based page numbers, ``"first-last"``."""
    return f"{pages.start + 1}-{pages.stop}"
