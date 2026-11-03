"""The v5 document pipeline (#372, A8a): spec §3.3 steps 1-5, page by page, then the document.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A7's
``PDFWriter`` and A8b's ``PDFReader`` stand on it.

:class:`PipelineOptions` holds the reader's and writer's extraction options
and runs the spec's construction checks (invariant 2) when it is built, so
a bad option fails before any work. :class:`PDFSource` is where a document
is opened from (its path and password), in a worker.

This module is the caller's side, and imports no pdfium: every pdfium call
on the PDF (its open, page count, metadata and password check, and each
page's steps) is a page worker's (:mod:`datagrunt.pdf._page_worker`),
whose memory is capped (:mod:`datagrunt.pdf._memory_cap`) before it opens
the PDF. The caller hashes the file, starts the workers and reads their
replies, then runs steps 6-7.

A page's steps, in order, in its worker; no stage is skipped (the worker
runs them :data:`_WINDOW_PAGES` pages at a time, steps 1-3 for each page of
a window before step 4 for any):

1. A2 :func:`~datagrunt.pdf.pdfium_engine.extract_page`. A
   :class:`~datagrunt.pdf.pdfium_engine.PageFailure` (a page pdfium cannot
   read, or one past a walk bound: its objects, path segments or textpage
   chars) raises :class:`~datagrunt.pdf.exceptions.PDFCorruptError` naming
   the page under ``strict``; else the page skips steps 2-3 and its step 4
   is A5b's :func:`~datagrunt.pdf.vision.fallback_page` (its geometry read
   again, then render and OCR: ``"ocr_fallback"``, or an error page naming
   why it was not read).
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
   :func:`~datagrunt.pdf.assembly.assemble_page`, with ``dpi`` and
   ``max_render_pixels`` for its lazy ``page_image``; a vision page is A5b's
   :func:`~datagrunt.pdf.vision.vision_page`'s (render and OCR: ``"ok"``,
   or ``"ocr_unavailable"`` with its digital text when OCR cannot read it);
   a page A2 could not read is ``fallback_page``'s (step 1). Every render
   and OCR of a page is its step 4, so each page reply carries at most one
   page's OCR budget, as its bound (:func:`_reply_bound_s`) counts.
   Both A5b steps take the source's password, ``ocr``,
   ``min_image_dimension`` (a vision page's figures), and ``vision_dpi`` and
   ``max_render_pixels`` (:class:`~datagrunt.pdf.vision.VisionOptions`).
5. Isolation: any other exception (an ``Exception``; an interrupt is never
   caught) is an error page whose ``error`` is ``"<type>: <message>"``,
   recorded, never dropped. Under ``strict`` it raises a PDFCorruptError
   naming the page, chained from it (a PDFCorruptError that already names
   the page is raised as it is).

:func:`extract_document` runs every page's steps, in page order, then the
document's:

6. A6 :func:`~datagrunt.pdf.scope.apply_scope`: scope paths, split-table
   links, sections and header context.
7. The A1 :class:`~datagrunt.pdf.elements.PDFDocument`. Its metadata:
   ``source_name`` is the file's name; ``sha256`` is streamed over the
   file's bytes in chunks of
   :data:`~datagrunt.pdf._source._HASH_CHUNK_BYTES`; ``title`` and
   ``author`` are A2's :func:`~datagrunt.pdf.pdfium_engine.document_metadata`,
   read by the first page worker; ``doc_id`` is
   :meth:`~datagrunt.pdf.elements.DocumentMetadata.make_doc_id`.

Workers: the pages always run in worker processes, one per contiguous
chunk (:func:`_chunks`: ``min(workers, page_count)`` chunks whose sizes
differ by at most one). ``workers=1`` and a one-page document are one
worker for every page, in order; so is a distributed runner
(:func:`_is_distributed_env`, ported from v4: its executors are the
parallelism), whatever ``workers`` says. There is no extraction in the
caller to fall back to: with no Python interpreter to start (a frozen
application, an empty ``sys.executable``) extraction raises a
DatagruntPDFError saying so before anything starts, and an OS that refuses
to start a worker raises its OSError. The pool is bounded and always ends:

- A worker is a plain ``python -I`` subprocess started by the launcher the
  render and figure workers share (:mod:`datagrunt.pdf._process`, worker
  key ``"pages"``): it never imports the caller's main module, so a caller's
  script runs once, however it is written, with or without a ``__main__``
  guard. Its request (the source, the password, the file's identity and
  sha256, its pages, the options and its memory cap) is JSON on its stdin. Each worker
  caps its own memory, at the vision route's cap
  (``VisionOptions.memory_cap_bytes``), before it opens the PDF.
- The first worker opens the document first: its first reply is the
  document's facts (JSON: its page count, title and author), then it
  extracts the first chunk, which it reckons from the page count as the
  caller does. The caller starts the other chunks' workers once the facts
  have come. An error opening the document (``PDFEncryptedError``,
  ``PDFCorruptError``, ``PDFSourceChangedError``, ``FileNotFoundError``,
  ``PermissionError``) is raised here as the worker raised it. A first
  worker that ends before its facts (its cap, its bound, an unexpected
  exit) raises a PDFCorruptError whose ``page_number`` is None, saying
  why: the document is not read, and no page count means no error pages.
- A worker sends a progress message (``S`` and the page's index) when it
  begins a page's steps 1-3, and again at its step 4; each page as one
  length-prefixed pickled message as soon as it is extracted, in page order;
  then, once it has re-checked the file (see below), a done message. The
  exception that stops the chunk is sent pickled instead of what is left
  (:func:`~datagrunt.pdf._process.error_reply`: a type the caller cannot
  import without pdfium crosses as a DatagruntPDFError). Each message is
  read within :data:`_MAX_PAGE_MESSAGE_BYTES` (the 1 GiB asset limit); a
  longer one is refused unread.
- The caller reads every worker's replies as they come, each chunk's in a
  thread of its own (#426): no worker waits, its pipe full, for another's
  to be read. It checks each reply is a page, its chunk's next, and the
  count at done.
- Unpickling a worker's pages is acceptable where it would not be for
  bytes from elsewhere: the worker is the caller's own process image, run
  with the caller's privileges and exact ``sys.path``. Isolating pdfium in
  a worker bounds its memory, not its privileges. The render and figure
  workers, whose replies are pixels and bytes, pickle no data.
- Each page reply must come within its bound (:func:`_reply_bound_s`, #418),
  counted from the worker's page reply before it (from its start for its
  first page): the deadline restarts with every page, never with a
  progress message. A page's bound (:func:`_page_bound_s`) is its own work
  (:data:`_WORKER_PAGE_S`), its share of its window's background render
  (:data:`_BACKGROUND_PAGE_S`) and, unless ``ocr="never"``, its render and
  OCR budget (``page_budget_s``, :func:`_vision`'s: the vision route stops
  when it is spent) and its last call's reap (:data:`_CALL_REAP_S`). It is
  never the page's size: a giant page's is a letter page's, 705 s at the
  defaults (about 12 minutes; 90 s under ``ocr="never"``). A window's first
  page is also given the window's lead: its other pages' work and background
  shares, and its background render's start and reap
  (:data:`_BACKGROUND_START_S` + :data:`_CALL_REAP_S`); a worker's first
  page, :data:`_WORKER_START_S`; done, :data:`_WORKER_PAGE_S` after the last
  page; the first worker's facts, :data:`_WORKER_START_S` +
  :data:`_WORKER_PAGE_S` from its start. The worker's memory watchdog ends
  it at its lifetime (:func:`_lifetime_s`, the sum of those bounds), should
  its caller be gone.
- A worker that ends before it sends each of its pages (killed at a bound,
  past its memory cap, an unexpected exit, a message over its bound) keeps
  the pages it sent. The page it was on (its last progress message; the
  first it had not sent when it began none) is an error page saying why
  (which bound, its cap, or its exit code and the last 4 KiB of its stderr),
  and the rest of its unsent pages go to a new worker. Each new worker
  turns at least one page into a page, so a chunk starts at most as many
  workers as it has pages: no page is lost, and a page that ends its worker
  ends no other page. Under ``strict`` any of these raises a
  DatagruntPDFError naming the page instead. One that sent each of its
  pages but not done keeps them: only its last check of the path was not
  made (this process's own check of the hashed file still is).
- An exception a worker raises outside the per-page isolation (``strict``'s
  PDFCorruptError, an open error) is raised here, after the pages before
  its chunk; the workers after it are killed.
- Every worker is reaped (killed first if still running) before the pool
  returns or raises, on every path. A page worker leads a process group of
  its own (POSIX), and each kill of it (at a bound, at a stop, at its end)
  ends the group: the render workers and tesseract runs it started, and
  any process holding its pipes, end with it, so a reply's bound holds and
  no reader is left blocked on a pipe.

The hash is bound to the bytes extracted. The source is opened once, and
through that open file its identity is taken, once
(:class:`SourceIdentity`: ``st_dev``, ``st_ino``, ``st_size``,
``st_mtime_ns`` from ``fstat``), then its first ``st_size`` bytes are
hashed. pdfium opens the file by path (A2's
:func:`~datagrunt.pdf.pdfium_engine.open_document`, which maps pdfium's
load errors itself), so each opening is checked against the identity:

- in each worker, right after pdfium opens it and again after its last
  page, ``os.stat`` of the path;
- after every page, ``fstat`` of the file the hash was read through.

Any difference raises :class:`~datagrunt.pdf.exceptions.PDFSourceChangedError`
naming it (inode, size or mtime), ``strict`` or not: no document whose pages
and hash could disagree is returned. A file replaced after pdfium opened it
in a worker needs no check: pdfium keeps reading the file it opened. What
this cannot see: a write that keeps the size and restores the mtime, or the
path swapped away and back in the moment between pdfium's open and the
check that follows it (each is a deliberate race on the caller's own
directory). A figure's loader reopens the path later and verifies each
image's own SHA-256 when it reads. A page image's loaders hold the
identity and the hash (the worker's ``PDFSource`` carries both): each draw
of a file of another identity hashes it, and draws it only when its bytes
are the document's (:mod:`datagrunt.pdf._loaders`).

An error page (A1: no elements, no quality, never verified) is routed
``"vision"``, the spec's route for a page whose extraction fails (render
and OCR, A5b). A page A2 could not read is ``fallback_page``'s; a page
whose step raised (step 5) is its worker's: its size is the page's, read
with :func:`~datagrunt.pdf.pdfium_engine.page_geometry`; when that cannot
be read either, it is pdfium's default page size (612 x 792 pt, what
pdfium reads for an empty MediaBox), and a warning says so and why. A page
that ended its worker has that default size: it was never read here.
"""

import dataclasses
import json
import os
import pickle
import struct
import subprocess
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, NamedTuple, get_args

from datagrunt.pdf._memory_cap import ending
from datagrunt.pdf._options import DPI, MAX_RENDER_PIXELS, MIN_IMAGE_DIMENSION, OcrMode, VisionOptions
from datagrunt.pdf._process import (
    MAX_REQUEST_BYTES,
    REAP_TIMEOUT_S,
    MessageTooLong,
    NoInterpreter,
    StderrTail,
    close_pipes,
    kill_group,
    next_reply,
    read_error,
    reaped,
    request_payload,
    start_worker,
    worker_command,
)
from datagrunt.pdf._source import SourceIdentity, _opened_source, _stream_sha256, check_binding
from datagrunt.pdf._validate import checked_password, checked_path, choice_str, int_at_least, strict_bool
from datagrunt.pdf.elements import _MAX_ASSET_BYTES, DocumentMetadata, Page, PDFDocument
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFCorruptError, PDFSourceChangedError
from datagrunt.pdf.render import _MIN_RENDER_PIXELS, _MIN_TARGET_DPI
from datagrunt.pdf.scope import apply_scope

__all__ = ["PDFSource", "PipelineOptions", "SourceIdentity", "extract_document"]

_OCR_MODES: tuple[OcrMode, ...] = get_args(OcrMode)

# pdfium's own default page size (US Letter), as it reads an empty MediaBox: an error page's size when its own
# cannot be read.
_DEFAULT_PAGE_SIZE = (612.0, 792.0)

# Environment variables that mark a distributed runner (Spark, Beam, Flink, Celery): v4's
# ``core.pdf_io.engines._is_distributed_env``, ported.
_DISTRIBUTED_KEYS = ("SPARK_ENV_LOADED", "SPARK_HOME", "BEAM_WORKER_ID", "FLINK_CONF_DIR", "CELERY_BROKER_URL")

# A worker's first page is given this to start, and every page this for its own work (the module notes' bounds).
# Measured here: a worker imports datagrunt in ~0.2 s, and the corpus's slowest page extracts in ~0.5 s, so each
# bound is over 100 times what it covers.
_WORKER_START_S = 60.0
_WORKER_PAGE_S = 60.0
# A bounded call (a render worker, a tesseract run) can outlast its timeout by its reap: its timer's join, then a wait,
# a kill and a wait for its process, each within REAP_TIMEOUT_S.
_CALL_REAP_S = 3 * REAP_TIMEOUT_S
_EXIT_GRACE_S = 1.0  # a worker whose replies ended without its pages is given this to exit

# A page worker's replies, each one length-prefixed message: the document's facts (JSON, the first worker's first),
# a page begun (its index), a page (pickled), the chunk done, or the exception that stopped it (pickled). Each is read
# within the asset limit.
_FACTS, _STARTED, _PAGE, _DONE, _ERROR = b"F", b"S", b"P", b"D", b"E"
_INDEX = struct.Struct("<Q")
_MAX_PAGE_MESSAGE_BYTES = _MAX_ASSET_BYTES
_CHUNK_REQUEST_KEYS = ("source", "password", "identity", "sha256", "pages", "workers", "options", "memory_cap_bytes")
# A chunk's pages are page ranges: one at first, and after a page ends its worker, the rest of the unsent pages. They
# lie in at most two windows, so a chunk is at most this many ranges.
_MAX_CHUNK_RANGES = 64

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


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineOptions:
    """The extraction options of ``PDFReader`` and ``PDFWriter``, checked at construction (spec invariant 2).

    Picklable, so it crosses the process pool.

    Attributes:
        workers: Worker processes to extract pages with (>= 1); 1 extracts
            every page in one worker.
        ocr: ``"auto"``, ``"always"`` or ``"never"`` (A3's routing option).
        dpi: The page render's target DPI (>= 36): a digital or hybrid
            page's image is drawn at it (A4b's lazy ``page_image``).
        vision_dpi: A vision page's render DPI (>= ``dpi``). A5b renders with it.
        max_render_pixels: The pixel bound of any render (>= 262,144, 512 x
            512). A4b's page images and A5b render within it.
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
    dpi: int = DPI
    vision_dpi: int = 300
    max_render_pixels: int = MAX_RENDER_PIXELS
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
    """Where the document is opened from: in a worker.

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
        sha256: The document's sha256, the hash read through ``identity``'s
            file: given with ``identity`` or not at all. The page images
            hold both: a draw of a file of another identity is made only
            when its bytes hash to it (:mod:`datagrunt.pdf._loaders`).

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike of str,
            ``password`` is not a str or None, ``identity`` is not a
            SourceIdentity or None, or ``sha256`` not a str or None.
        ValueError: If ``password`` contains NUL, ``sha256`` is not 64
            lowercase hex characters, or only one of ``identity`` and
            ``sha256`` is given.
    """

    path: Path
    password: str | None = field(default=None, repr=False, compare=False)
    identity: SourceIdentity | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", checked_path(self.path, "source").absolute())
        checked_password(self.password)
        check_binding(self.identity, self.sha256)


class Facts(NamedTuple):
    """The document's facts, as its first page worker read them: its page count, title and author."""

    page_count: int
    title: str | None
    author: str | None


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
    """Extract the whole PDF at ``source``: steps 1-5 for every page, in workers, then 6-7 (see the module notes).

    The options are :class:`PipelineOptions`', checked before any work. No
    page is dropped: a page that fails is an error page, or, under
    ``strict``, a PDFCorruptError naming it. Every worker is reaped before
    returning, on every path.

    Raises:
        TypeError, ValueError: As :class:`PipelineOptions` and
            :class:`PDFSource` raise them, before the file is opened.
        FileNotFoundError, PermissionError, PDFEncryptedError: As
            :func:`~datagrunt.pdf.pdfium_engine.open_document` raises them,
            in the first worker.
        PDFCorruptError: If the file is not a readable PDF, or its first
            worker ends before it reads the document's facts (``page_number``
            None), or, under ``strict``, for a page that fails (set).
        PDFSourceChangedError: If the file changes while it is extracted
            (see the module notes), ``strict`` or not.
        DatagruntPDFError: If no worker can start (a frozen application, an
            empty ``sys.executable``), its title or author is over A2's
            bound, a worker returns something that is not its pages, or,
            under ``strict``, a page ends its worker.
        OSError: If the OS refuses to start a worker.
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
    pdf = PDFSource(path=checked_path(source, "source"), password=password)
    command = _pool_command()
    with _opened_source(pdf.path) as stream:
        identity = SourceIdentity.of(os.fstat(stream.fileno()))
        digest = _stream_sha256(stream, identity.size, pdf.path)
        bound = dataclasses.replace(pdf, identity=identity, sha256=digest)
        facts, pages = _pooled_pages(bound, options, command)
        _check_unchanged(bound, os.fstat(stream.fileno()), "by the end of its extraction")
    return _document(bound, facts, digest, pages)


def _document(source: PDFSource, facts: Facts, digest: str, pages: tuple[Page, ...]) -> PDFDocument:
    """Steps 6-7: A6's scope over the pages, then the A1 document and its metadata."""
    scoped = apply_scope(pages)
    name = source.path.name
    metadata = DocumentMetadata(
        doc_id=DocumentMetadata.make_doc_id(name, digest),
        source_name=name,
        sha256=digest,
        page_count=len(pages),
        title=facts.title,
        author=facts.author,
    )
    return PDFDocument(
        metadata=metadata, pages=scoped.pages, sections=scoped.sections, header_context=scoped.header_context
    )


def _check_unchanged(source: PDFSource, status: os.stat_result, when: str) -> None:
    """Raise PDFSourceChangedError if ``status`` is not ``source.identity`` (nothing to check without one)."""
    if source.identity is None:
        return
    changes = source.identity.changes(SourceIdentity.of(status))
    if changes:
        disagree = "its pages and its sha256 could disagree, so it is not extracted"
        raise PDFSourceChangedError(f"{source.path} changed {when}: {', '.join(changes)}; {disagree}")


def _vision(options: PipelineOptions) -> VisionOptions:
    """How A5b renders and reads a page under ``options``: its vision DPI and pixel bound, A5b's other defaults.

    Its ``memory_cap_bytes`` is each page worker's cap too.
    """
    return VisionOptions(vision_dpi=options.vision_dpi, max_render_pixels=options.max_render_pixels)


def error_page(index: int, error: str, *, size: tuple[float, float] | None = None, why: str = "") -> Page:
    """The A1 error page of the page at ``index``: no elements, no quality, routed vision (see the module notes).

    Its size is ``size``; with none, pdfium's default page size, and a
    warning that its own could not be read and ``why``.
    """
    width, height = _DEFAULT_PAGE_SIZE if size is None else size
    warnings = () if size is not None else (_default_size_warning(why),)
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


def _default_size_warning(reason: str) -> str:
    """The warning of an error page whose size is pdfium's default page size, not its own."""
    width, height = _DEFAULT_PAGE_SIZE
    default = f"{width:g} x {height:g} pt is pdfium's default page size, not this page's"
    return f"the page's size could not be read ({reason}); {default}"


def _is_distributed_env() -> bool:
    """True inside a distributed runner (Spark, Beam, Flink, Celery), as v4 detects one."""
    return any(key in os.environ for key in _DISTRIBUTED_KEYS)


def _pool_size(workers: int) -> int:
    """The worker processes the pool runs at once: one in a distributed runner (its executors are the parallelism)."""
    return 1 if _is_distributed_env() else workers


def _chunks(page_count: int, workers: int) -> tuple[range, ...]:
    """``min(workers, page_count)`` contiguous runs of page indices, in order, sizes differing by at most one."""
    count = min(workers, page_count)
    size, extra = divmod(page_count, count)
    starts = [index * size + min(index, extra) for index in range(count + 1)]
    return tuple(range(start, stop) for start, stop in zip(starts, starts[1:]))


def _ranges(pages: Sequence[int]) -> list[list[int]]:
    """Ascending page indices as ``[start, stop]`` runs."""
    runs: list[list[int]] = []
    for index in pages:  # bounded: the pages
        if runs and runs[-1][1] == index:
            runs[-1][1] += 1
        else:
            runs.append([index, index + 1])
    return runs


def _span(pages: Sequence[int]) -> str:
    """``pages`` (0-based indices, ascending) as 1-based page numbers: ``"1-4"``, or runs ``"1-2, 4-8"``."""
    return ", ".join(f"{start + 1}-{stop}" for start, stop in _ranges(pages))


def _chunk_payload(
    source: PDFSource, pages: Sequence[int] | None, workers: int, options: PipelineOptions
) -> dict[str, object]:
    """A chunk's request, as JSON-ready values (the password on the worker's stdin only).

    ``pages`` None asks the first worker for the document's facts, then the
    first of ``workers`` chunks.
    """
    identity = None if source.identity is None else dataclasses.asdict(source.identity)
    return {
        "source": str(source.path),
        "password": source.password,
        "identity": identity,
        "sha256": source.sha256,
        "pages": None if pages is None else _ranges(pages),
        "workers": workers,
        "options": dataclasses.asdict(options),
        "memory_cap_bytes": _vision(options).memory_cap_bytes,
    }


class ChunkRequest(NamedTuple):
    """A page worker's request, checked: its source, pages (None: the facts, then the first chunk), and so on."""

    source: PDFSource
    pages: tuple[int, ...] | None
    workers: int
    options: PipelineOptions
    memory_cap_bytes: int


def _chunk_request(message: object) -> ChunkRequest:
    """The request a worker reads (JSON from its caller), each field checked as at construction."""
    if not isinstance(message, dict) or set(message) != set(_CHUNK_REQUEST_KEYS):
        found = sorted(message) if isinstance(message, dict) else type(message).__name__
        raise ValueError(f"the page worker's request must hold exactly {_CHUNK_REQUEST_KEYS}, got {found}")
    identity = message["identity"]
    source = PDFSource(
        path=message["source"],
        password=message["password"],
        identity=None if identity is None else SourceIdentity(**identity),
        sha256=message["sha256"],
    )
    pages = None if message["pages"] is None else _checked_ranges(message["pages"])
    workers = int_at_least("workers", message["workers"], 1)
    cap = int_at_least("memory_cap_bytes", message["memory_cap_bytes"], 1)
    return ChunkRequest(source, pages, workers, PipelineOptions(**message["options"]), cap)


def _checked_ranges(value: object) -> tuple[int, ...]:
    """``[start, stop]`` runs, ascending and apart, at most :data:`_MAX_CHUNK_RANGES`, as their page indices."""
    if not isinstance(value, list) or not 1 <= len(value) <= _MAX_CHUNK_RANGES:
        raise ValueError(f"pages must be 1 to {_MAX_CHUNK_RANGES} [start, stop] runs, got {value!r}")
    pages: list[int] = []
    for run in value:
        ints = isinstance(run, list) and len(run) == 2 and all(type(bound) is int for bound in run)
        if not ints or not 0 <= run[0] < run[1] or (pages and run[0] <= pages[-1]):
            raise ValueError(f"pages must be ascending [start, stop] runs with 0 <= start < stop, got {value!r}")
        pages.extend(range(run[0], run[1]))
    return tuple(pages)


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


def _reply_bound_s(count: int, received: int, options: PipelineOptions) -> float:
    """Seconds a worker of ``count`` pages has for its next page reply once it has sent ``received`` (module notes).

    A page's bound (:func:`_page_bound_s`); for the first page of a window,
    also the window's lead: its other pages' steps 1-3 and background
    shares, and its background render's start and reap; for the worker's
    first page, also :data:`_WORKER_START_S`. After its last page,
    :data:`_WORKER_PAGE_S` for its close and its last check of the file.
    """
    if received == count:
        return _WORKER_PAGE_S
    bound = _page_bound_s(options)
    if received % _WINDOW_PAGES == 0:
        others = min(_WINDOW_PAGES, count - received) - 1
        bound += others * (_WORKER_PAGE_S + _BACKGROUND_PAGE_S) + _BACKGROUND_START_S + _CALL_REAP_S
    return (bound + _WORKER_START_S) if received == 0 else bound


def _facts_bound_s() -> float:
    """Seconds the first worker has, from its start, to send the document's facts: its start and its open."""
    return _WORKER_START_S + _WORKER_PAGE_S


def _lifetime_s(count: int, options: PipelineOptions) -> float:
    """The most a worker of ``count`` pages may live: its facts and each of its replies' bounds, each with a reap.

    Its memory watchdog ends it then, should its caller be gone.
    """
    replies = sum(_reply_bound_s(count, received, options) + REAP_TIMEOUT_S for received in range(count + 1))
    return _facts_bound_s() + replies


def _pool_command() -> list[str]:
    """The page worker's command line; DatagruntPDFError, before anything starts, if there is no interpreter."""
    try:
        return worker_command()
    except NoInterpreter as exc:
        raise DatagruntPDFError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class _Worker:
    """One running page worker: its pages, its process, its stderr's drain (a 4 KiB ring), and when it started."""

    pages: tuple[int, ...] | None  # None: the first worker, before its facts
    child: "subprocess.Popen[bytes]"
    stderr: StderrTail
    started: float  # time.monotonic(), just before it was started


def _started(command: list[str], pages: tuple[int, ...] | None) -> _Worker:
    """Start the worker for ``pages``, its stderr drained into a 4 KiB ring (nothing is written to disk)."""
    started = time.monotonic()
    child, stderr = start_worker(command, own_group=True)
    return _Worker(pages=pages, child=child, stderr=stderr, started=started)


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
    except OSError:  # it died at once: its replies end, and its exit code and stderr say why
        pass


class _Replies:
    """What one worker sent, read as it came: its facts, its pages, the page it was on, and how its replies ended.

    ``ending`` is ``"done"``, ``"ended"`` (its stdout closed: it died),
    ``"expired"`` (killed at the bound it was given, ``bound_s``) or
    ``"too long"`` (a message over its bound: ``detail``; it was killed);
    empty while it is read. ``current`` is the page its last progress
    message named (None: it began none). ``error`` is what it raised for the
    pool to raise: its pickled exception, or a reply that is not its next
    page.
    """

    __slots__ = ("bound_s", "current", "detail", "ending", "error", "facts", "pages")

    def __init__(self) -> None:
        self.facts: Facts | None = None
        self.pages: list[Page] = []
        self.current: int | None = None
        self.ending = ""
        self.detail = ""
        self.bound_s = 0.0
        self.error: Exception | None = None


class _Chunk:
    """One chunk's extraction, read in a thread of its own: a worker, then new ones for its unsent pages (notes).

    ``facts`` and ``pages`` are the first worker's once its facts come
    (``ready`` is set then, or when the chunk ends). ``got`` holds its pages
    by index; ``error`` is what the pool raises for it. :meth:`stop` kills
    its worker and starts no other.
    """

    def __init__(
        self,
        source: PDFSource,
        options: PipelineOptions,
        command: list[str],
        pages: tuple[int, ...] | None,
        workers: int,
    ) -> None:
        self.source, self.options, self.command, self.workers = source, options, command, workers
        self.pages = pages
        self.facts: Facts | None = None
        self.got: dict[int, Page] = {}
        self.error: BaseException | None = None
        self.ready = threading.Event()
        self.started: list[int] = []  # each worker's pid, as it started
        self._lock = threading.Lock()
        self._worker: _Worker | None = None
        self._stopped = False
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        """Kill its running worker, if any, and start no other (a reaped worker is never signalled)."""
        with self._lock:
            self._stopped = True
            if self._worker is not None:
                kill_group(self._worker.child)

    def _run(self) -> None:
        """The thread: workers until each page is a page, an error, or a stop; what it raises recorded."""
        try:
            self._extract()
        except BaseException as exc:  # recorded, and raised by the pool in this chunk's turn: never swallowed
            self.error = exc
        finally:
            self.ready.set()

    def _extract(self) -> None:
        """Run a worker for the unsent pages until none is left (each worker settles at least one page)."""
        remaining = self.pages
        for _ in range(_MAX_RESTARTS):  # bounded: each pass settles a page, and a chunk is at most the document
            worker = self._next_worker(remaining)
            if worker is None:
                return
            try:
                replies = self._read(worker)
            finally:
                self._finished(worker)
            remaining = self._settled(worker, replies)
            if not remaining:
                return
        raise DatagruntPDFError(f"the pages {_span(remaining or ())} were not extracted within {_MAX_RESTARTS} workers")

    def _next_worker(self, pages: tuple[int, ...] | None) -> _Worker | None:
        """Start a worker for ``pages`` and send it its request; None once the chunk is stopped."""
        payload = request_payload(
            "pages", _chunk_payload(self.source, pages, self.workers, self.options), MAX_REQUEST_BYTES
        )
        with self._lock:
            if self._stopped:
                return None
            worker = _started(self.command, pages)
            self._worker = worker
            self.started.append(worker.child.pid)
        _send_request(worker, payload)
        return worker

    def _finished(self, worker: _Worker) -> None:
        """Forget, kill and reap ``worker``, and end its stderr's drain (a kill never reaches a reaped pid)."""
        with self._lock:
            self._worker = None
        kill_group(worker.child)
        close_pipes(worker.child)
        reaped(worker.child, REAP_TIMEOUT_S)
        worker.stderr.finish(REAP_TIMEOUT_S)

    def _read(self, worker: _Worker) -> _Replies:
        """The worker's replies, each within its bound; reading ends at done, an error, or the worker's end."""
        replies = _Replies()
        try:
            _read_into(worker, replies, self)
        except Exception as exc:  # recorded, and raised by the pool: never swallowed
            replies.error = exc
        if replies.ending in ("", "done") or replies.error is not None:
            return replies
        replies.detail = _why_lost(worker, replies, self)
        return replies

    def _settled(self, worker: _Worker, replies: _Replies) -> tuple[int, ...]:
        """Keep what the worker sent; its unsent pages but the one it was on (an error page) are left for another."""
        if replies.error is not None:
            raise replies.error
        for page in replies.pages:
            self.got[page.page_number - 1] = page
        if replies.ending == "done":
            return ()
        if self.pages is None:  # the first worker ended before its facts: there is no page count, so no page
            raise PDFCorruptError(f"{self.source.path}: {replies.detail}; the document is not read", page_number=None)
        mine = worker.pages if worker.pages is not None else self.pages or ()
        unsent = tuple(index for index in mine if index not in self.got)
        if not unsent:  # it sent each of its pages, but not done: they are kept (module notes)
            return ()
        if self.options.strict:
            raise DatagruntPDFError(replies.detail)
        culprit = replies.current if replies.current in unsent else unsent[0]
        self.got[culprit] = error_page(culprit, replies.detail, why="its worker process returned nothing")
        return tuple(index for index in unsent if index != culprit)


# A chunk starts at most this many workers: each settles at least one page, and a PDF has at most 2**31 - 1 pages.
_MAX_RESTARTS = 1 << 31


def _read_into(worker: _Worker, replies: _Replies, chunk: _Chunk) -> None:
    """Read the worker's facts (the first's), its progress and pages, then done, each within its bound."""
    stdout = worker.child.stdout
    if stdout is None:
        raise ValueError("the worker's stdout must be a pipe")
    if worker.pages is None:
        if not _read_facts(worker, stdout, replies, chunk):
            return
    pages = worker.pages if worker.pages is not None else chunk.pages or ()
    lost = f"the worker process extracting pages {_span(pages)}"
    since = worker.started
    for _ in range(3 * len(pages) + 2):  # bounded: two progress messages and a page a page, then done
        bound_s = _reply_bound_s(len(pages), len(replies.pages), chunk.options)
        try:
            message, expired = next_reply(
                worker.child, stdout, since + bound_s - time.monotonic(), _MAX_PAGE_MESSAGE_BYTES, kill=kill_group
            )
        except MessageTooLong as exc:
            kill_group(worker.child)  # it may be blocked writing the rest: the pool reaps it
            replies.ending, replies.detail = "too long", str(exc)
            return
        if message is None:
            replies.ending, replies.bound_s = ("expired" if expired else "ended"), bound_s
            return
        kind = message[:1]
        if kind == _STARTED:
            replies.current = _started_index(message, pages, lost)
            continue
        if kind == _DONE:
            _check_count(replies.pages, pages, lost)
            replies.ending = "done"
            return
        replies.pages.append(_next_page(message, pages, len(replies.pages), lost))
        since = time.monotonic()
    raise DatagruntPDFError(f"{lost} sent more replies than its pages")


def _read_facts(worker: _Worker, stdout: IO[bytes], replies: _Replies, chunk: _Chunk) -> bool:
    """Read the first worker's facts within their bound; set the chunk's pages from them. False: none came."""
    bound_s = _facts_bound_s()
    try:
        message, expired = next_reply(
            worker.child, stdout, worker.started + bound_s - time.monotonic(), _MAX_PAGE_MESSAGE_BYTES, kill=kill_group
        )
    except MessageTooLong as exc:
        kill_group(worker.child)
        replies.ending, replies.detail = "too long", str(exc)
        return False
    if message is None:
        replies.ending, replies.bound_s = ("expired" if expired else "ended"), bound_s
        return False
    if message[:1] == _ERROR:
        raise read_error(message[1:], "the first page worker")
    facts = _facts_of(message)
    chunk.facts = replies.facts = facts
    chunk.pages = tuple(_chunks(facts.page_count, chunk.workers)[0])
    chunk.ready.set()
    return True


def _facts_of(message: bytes) -> Facts:
    """The document's facts in the first worker's reply; anything else fails loud."""
    if message[:1] != _FACTS:
        raise DatagruntPDFError(f"the first page worker sent a reply of kind {message[:1]!r} before its facts")
    try:
        data = json.loads(message[1:])
    except ValueError as exc:
        raise DatagruntPDFError(f"the first page worker sent facts that are not JSON: {exc}") from exc
    count = data.get("page_count") if isinstance(data, dict) else None
    if type(count) is not int or count < 1:
        raise DatagruntPDFError(f"the first page worker sent facts without a page count: {data!r}")
    title, author = data.get("title"), data.get("author")
    if not all(value is None or isinstance(value, str) for value in (title, author)):
        raise DatagruntPDFError("the first page worker sent a title or author that is not text")
    return Facts(count, title, author)


def _started_index(message: bytes, pages: tuple[int, ...], lost: str) -> int:
    """The page a progress message names: one of the worker's pages; anything else fails loud."""
    body = message[1:]
    if len(body) != _INDEX.size:
        raise DatagruntPDFError(f"{lost} sent a progress message of {len(body)} bytes")
    (index,) = _INDEX.unpack(body)
    if index not in pages:
        raise DatagruntPDFError(f"{lost} began page {index + 1}, not one of its pages")
    return index


def _next_page(message: bytes, pages: tuple[int, ...], received: int, lost: str) -> Page:
    """The page in the worker's reply, if it is its pages' next one; its error raised; anything else fails loud."""
    page = _reply_page(message, lost)
    if received == len(pages):
        raise DatagruntPDFError(f"{lost} sent more than its {len(pages)} page(s)")
    due = pages[received] + 1
    if page.page_number != due:
        raise DatagruntPDFError(f"{lost} returned page {page.page_number} where page {due} was due")
    return page


def _reply_page(message: bytes, lost: str) -> Page:
    """The page in one of the worker's replies; its error raised; anything else fails loud."""
    kind, body = message[:1], message[1:]
    if kind == _ERROR:
        raise read_error(body, lost)
    if kind != _PAGE:
        raise DatagruntPDFError(f"{lost} sent a reply of unknown kind {kind!r}")
    page = pickle.loads(body)  # the caller's own process image: see the module notes
    if not isinstance(page, Page):
        raise DatagruntPDFError(f"{lost} sent a {type(page).__name__}, not a page")
    return page


def _check_count(pages: list[Page], expected: tuple[int, ...], lost: str) -> None:
    """Fail loud if the worker said done before it sent each of its pages: a reply is checked, never trusted."""
    if len(pages) != len(expected):
        raise DatagruntPDFError(f"{lost} said done after {len(pages)} of its {len(expected)} page(s)")


def _why_lost(worker: _Worker, replies: _Replies, chunk: _Chunk) -> str:
    """Why the worker ended before its pages: past a bound, a message over its bound, its cap, or its exit."""
    pages = worker.pages if worker.pages is not None else chunk.pages
    lost = "the page worker opening it" if pages is None else f"the worker process extracting pages {_span(pages)}"
    unsent = [index for index in (pages or ()) if index not in {page.page_number - 1 for page in replies.pages}]
    on = ""
    if unsent:
        current = replies.current if replies.current in unsent else None
        on = (
            f" while it extracted page {current + 1}"
            if current is not None
            else f" before it began page {unsent[0] + 1}"
        )
    if replies.ending == "expired":
        since = "its start" if not replies.pages else f"page {replies.pages[-1].page_number}"
        what = "its facts" if pages is None else f"page {unsent[0] + 1}"
        return f"{lost} did not return {what} within {replies.bound_s:g} s of {since}: it was killed{on}"
    if replies.ending == "too long":
        return f"{lost} sent {replies.detail}: it was killed{on}"
    said = ending(reaped(worker.child, _EXIT_GRACE_S), lost, _vision(chunk.options).memory_cap_bytes) + on
    tail = worker.stderr.finish(REAP_TIMEOUT_S)  # it is reaped: its drain ends
    return f"{said}; its stderr ends: {tail}" if tail else said


def _pooled_pages(source: PDFSource, options: PipelineOptions, command: list[str]) -> tuple[Facts, tuple[Page, ...]]:
    """The document's facts and every page, from workers (see the module notes); the pages in page order.

    Every worker is reaped before this returns or raises.
    """
    workers = _pool_size(options.workers)
    chunks = [_Chunk(source, options, command, None, workers)]
    try:
        chunks[0].start()
        facts = _facts_from(chunks[0])
        for span in _chunks(facts.page_count, workers)[1:]:
            chunks.append(_Chunk(source, options, command, tuple(span), workers))
            chunks[-1].start()
        return facts, _gathered(chunks, options)
    finally:
        _shut_down(chunks)


def _facts_from(chunk: _Chunk) -> Facts:
    """The first chunk's facts, once they come; its error raised, or a DatagruntPDFError if neither comes in time."""
    chunk.ready.wait(_facts_bound_s() + 2 * REAP_TIMEOUT_S)
    if chunk.facts is not None:
        return chunk.facts
    if chunk.error is not None:
        raise chunk.error
    chunk.stop()
    raise DatagruntPDFError(f"{chunk.source.path}: the first page worker's facts could not be read in time")


def _gathered(chunks: list[_Chunk], options: PipelineOptions) -> tuple[Page, ...]:
    """Each chunk's pages, in page order, once its thread ends; a chunk's error raised, its later chunks stopped."""
    pages: list[Page] = []
    for index, chunk in enumerate(chunks):
        try:
            _joined(chunk, options)
            if chunk.error is not None:
                raise chunk.error
            pages.extend(chunk.got[number] for number in sorted(chunk.got))
        except BaseException:
            for later in chunks[index + 1 :]:
                later.stop()
            raise
    return tuple(pages)


def _joined(chunk: _Chunk, options: PipelineOptions) -> None:
    """Wait for the chunk's thread: within each of its workers' lifetimes, at most one worker a page and one more."""
    count = len(chunk.pages or ())
    chunk.thread.join((count + 1) * (_lifetime_s(count, options) + REAP_TIMEOUT_S))
    if chunk.thread.is_alive():
        chunk.stop()
        chunk.thread.join(REAP_TIMEOUT_S)
    if chunk.thread.is_alive():
        raise DatagruntPDFError(f"the replies of the workers for pages {_span(chunk.pages or ())} could not be read")


def _shut_down(chunks: list[_Chunk]) -> None:
    """End the pool: stop each chunk (its worker killed), then wait for its thread, which reaps its workers."""
    for chunk in chunks:
        chunk.stop()
    for chunk in chunks:
        if chunk.thread.ident is not None:
            chunk.thread.join(REAP_TIMEOUT_S)
