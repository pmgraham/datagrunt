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
itself (a pdfium handle never crosses processes) and returns its pages,
pickled, and the pages come back in page order. Pages extract in this
process instead when ``workers`` is 1, the document has one page, or a
distributed runner is detected (:func:`_is_distributed_env`, ported from v4:
its executors are the parallelism). The pool is bounded and always ends:

- Workers are started with the ``spawn`` method on every platform, so a
  worker never inherits a forked copy of pdfium's state. As with any
  ``multiprocessing`` pool, a script that extracts with ``workers > 1``
  needs the ``if __name__ == "__main__":`` guard: a spawned worker imports
  the caller's main module.
- Every worker must return by one deadline,
  :data:`_WORKER_START_S` + :data:`_WORKER_PAGE_S` x the longest chunk,
  counted from the pool's start. One that has not is killed, and its
  chunk's pages are error pages.
- A worker that dies without returning (killed, crashed, ``os._exit``)
  leaves error pages for exactly its chunk's pages, never a lost page; the
  other chunks extract. Under ``strict`` either case raises a
  DatagruntPDFError naming the pages instead.
- An exception a worker raises outside the per-page isolation (``strict``'s
  PDFCorruptError, an open error) is raised here.
- Every worker is reaped (killed first if still running) before the pool
  returns or raises, on every path.

An error page (A1: no elements, no quality, never verified) is routed
``"vision"``, the spec's route for a page whose extraction fails (render
and OCR, A5b). Its size is the page's, read with
:func:`~datagrunt.pdf.pdfium_engine.page_geometry`; when that cannot be
read either, it is pdfium's default page size (612 x 792 pt, what pdfium
reads for an empty MediaBox), and a warning says so and why. A worker's
error pages (it died, or ran out of time) have that default size: their
pages were never read in this process.
"""

import hashlib
import multiprocessing
import os
import time
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from multiprocessing.context import SpawnContext, SpawnProcess
from pathlib import Path
from typing import BinaryIO, get_args

from datagrunt.pdf._validate import choice_str, int_at_least, strict_bool
from datagrunt.pdf.assembly import MIN_IMAGE_DIMENSION, assemble_page
from datagrunt.pdf.elements import DocumentMetadata, Page, PDFDocument
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFCorruptError
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
from datagrunt.pdf.render import _MIN_RENDER_PIXELS, _MIN_TARGET_DPI, _REAP_TIMEOUT_S
from datagrunt.pdf.scope import apply_scope

__all__ = ["PDFSource", "PipelineOptions", "extract_document", "extract_page_at"]

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
# Measured here: a spawned worker imports datagrunt in ~0.6 s, and the corpus's slowest page (95 pages)
# extracts in ~0.5 s, so each bound is over 100 times what it covers.
_WORKER_START_S = 60.0
_WORKER_PAGE_S = 60.0


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
class PDFSource:
    """Where the document is opened from: in this process, or reopened in a worker.

    Picklable, so it crosses the process pool (a pdfium handle cannot).

    Attributes:
        path: The PDF's path, made absolute at construction (a worker's
            ``chdir`` cannot redirect it).
        password: The encrypted PDF's password, or None. Excluded from repr
            and equality, and never put in an error message; a pickle of the
            source carries it (the worker needs it).

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike of str, or
            ``password`` is not a str or None.
        ValueError: If ``password`` contains NUL.
    """

    path: Path
    password: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _checked_path(self.path, "source").absolute())
        _checked_password(self.password)


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
        DatagruntPDFError: If the file grows while it is hashed, or its
            title or author is over A2's bound.
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
    with open_document(pdf.path, password=pdf.password) as handle:
        info, digest, count = document_metadata(handle), _source_sha256(pdf.path), handle.page_count
        if _runs_sequentially(options.workers, count):
            return _document(pdf, info, digest, _pages_in(handle, pdf, range(count), options))
    return _document(pdf, info, digest, _pooled_pages(pdf, _chunks(count, options.workers), options))


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


def _source_sha256(path: Path) -> str:
    """The SHA-256 of the file's bytes, read in chunks of :data:`_HASH_CHUNK_BYTES`.

    The reads are bounded by the file's size when it was opened: one that
    grows past it while it is read fails loud rather than hash bytes pdfium
    may never have read.
    """
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        size = _size_at_open(stream)
        for _ in range(size // _HASH_CHUNK_BYTES + 2):
            block = _read_chunk(stream)
            if not block:
                return digest.hexdigest()
            digest.update(block)
    raise DatagruntPDFError(f"{path} grew while it was hashed: it holds more than its {size} bytes at open")


def _size_at_open(stream: BinaryIO) -> int:
    """The size, in bytes, of the open file ``stream`` reads."""
    return os.fstat(stream.fileno()).st_size


def _read_chunk(stream: BinaryIO) -> bytes:
    """The next at most :data:`_HASH_CHUNK_BYTES` bytes of ``stream`` (empty at its end)."""
    return stream.read(_HASH_CHUNK_BYTES)


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
    """
    _check_arguments(handle_source, options)
    with open_document(handle_source.path, password=handle_source.password) as handle:
        _checked_index(index, handle.page_count)
        return _isolated_page(handle, handle_source, index, options)


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
    """A worker's work: open the document itself, extract ``pages`` in order, close it."""
    with open_document(source.path, password=source.password) as handle:
        return _pages_in(handle, source, pages, options)


def _chunk_worker(sender: Connection, source: PDFSource, pages: range, options: PipelineOptions) -> None:
    """A worker process's body: send its pages, or the exception that stopped them (the parent raises it).

    Module level, so a spawned worker imports it by name.
    """
    with sender:
        try:
            outcome: tuple[Page, ...] | Exception = _extract_chunk(source, pages, options)
        except Exception as exc:  # strict's PDFCorruptError, an open error: sent, and raised by the parent
            outcome = exc
        sender.send(outcome)


@dataclass(frozen=True, slots=True)
class _Worker:
    """One running worker: its chunk, its process, and the parent's end of its pipe."""

    pages: range
    process: SpawnProcess
    receiver: Connection


def _pooled_pages(source: PDFSource, chunks: tuple[range, ...], options: PipelineOptions) -> tuple[Page, ...]:
    """Each chunk's pages from a worker process of its own, all at once; the pages in page order.

    Every worker is reaped before this returns or raises (see the module notes).
    """
    context = multiprocessing.get_context("spawn")
    budget_s = _WORKER_START_S + _WORKER_PAGE_S * max(len(chunk) for chunk in chunks)
    deadline = time.monotonic() + budget_s
    workers: list[_Worker] = []
    try:
        for chunk in chunks:
            workers.append(_started(context, source, chunk, options))
        return tuple(page for worker in workers for page in _collected(worker, deadline, budget_s, options))
    finally:
        _shut_down(workers)


def _started(context: SpawnContext, source: PDFSource, pages: range, options: PipelineOptions) -> _Worker:
    """Start the worker for ``pages``; the parent keeps only its pipe's receiving end."""
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_chunk_worker, args=(sender, source, pages, options), name=f"datagrunt-pdf-{_span(pages)}", daemon=True
    )
    try:
        process.start()
    except BaseException:
        receiver.close()
        raise
    finally:
        sender.close()  # the worker holds its own copy: once it ends, the receiver reads end of file
    return _Worker(pages=pages, process=process, receiver=receiver)


def _collected(worker: _Worker, deadline: float, budget_s: float, options: PipelineOptions) -> tuple[Page, ...]:
    """The worker's pages; its chunk's error pages if it ran out of time or died first (see the module notes)."""
    lost = f"the worker process extracting pages {_span(worker.pages)}"
    if not worker.receiver.poll(max(0.0, deadline - time.monotonic())):
        worker.process.kill()
        return _lost_chunk(worker.pages, f"{lost} did not return them within {budget_s:g} s: it was killed", options)
    try:
        outcome = worker.receiver.recv()
    except (EOFError, OSError):
        worker.process.join(_REAP_TIMEOUT_S)
        ended = f"ended (exit code {worker.process.exitcode}) before returning them"
        return _lost_chunk(worker.pages, f"{lost} {ended}", options)
    worker.process.join(_REAP_TIMEOUT_S)
    if isinstance(outcome, Exception):
        raise outcome
    return _checked_chunk(outcome, worker.pages, lost)


def _lost_chunk(pages: range, reason: str, options: PipelineOptions) -> tuple[Page, ...]:
    """The error pages of a chunk its worker never returned; under ``strict``, the reason raised."""
    if options.strict:
        raise DatagruntPDFError(reason)
    return tuple(_error_page(None, index, reason) for index in pages)


def _checked_chunk(outcome: object, pages: range, lost: str) -> tuple[Page, ...]:
    """``outcome`` if it is the chunk's pages, in order: a worker's reply is checked, never trusted."""
    expected = [index + 1 for index in pages]
    if not isinstance(outcome, tuple) or not all(isinstance(page, Page) for page in outcome):
        raise DatagruntPDFError(f"{lost} returned a {type(outcome).__name__}, not its {len(pages)} page(s)")
    if [page.page_number for page in outcome] != expected:
        numbers = [page.page_number for page in outcome]
        raise DatagruntPDFError(f"{lost} returned pages {numbers}, not pages {expected}")
    return outcome


def _shut_down(workers: list[_Worker]) -> None:
    """End the pool: kill each worker still running, reap it and close its pipe and process."""
    for worker in workers:
        worker.receiver.close()
        if worker.process.is_alive():
            worker.process.kill()
        worker.process.join(_REAP_TIMEOUT_S)
        if worker.process.exitcode is not None:
            worker.process.close()


def _span(pages: range) -> str:
    """``pages`` (0-based indices) as 1-based page numbers, ``"first-last"``."""
    return f"{pages.start + 1}-{pages.stop}"
