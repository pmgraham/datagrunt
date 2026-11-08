"""The page worker (#372 A8a): a chunk's pages, steps 1-5 each, in a worker process whose memory is capped.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Only a page
worker imports this module (the launcher's key ``"pages"``,
:mod:`datagrunt.pdf._process`); its caller's side, the request, the replies
and the module notes are :mod:`datagrunt.pdf.pipeline`'s.

The worker caps its own memory
(:func:`~datagrunt.pdf._memory_cap.apply_memory_cap`) before it opens the
PDF. The document's first worker then sends its facts (page count, title,
author: JSON) and reckons its chunk from the page count, as its caller
does. Each page is steps 1-5 (:func:`extract_page_at` is one page's, on a
load of its own), :data:`~datagrunt.pdf.pipeline._WINDOW_PAGES` pages at a
time; the worker says which page it begins (steps 1-3, then step 4) before
it works on it, sends each page as soon as it is done, re-checks the file,
then sends done. Reaching the cap (a ``MemoryError`` outside a page's
steps, or the watchdog) ends it with
:data:`~datagrunt.pdf._memory_cap.CAP_EXIT`: its caller makes the page it
was on an error page and gives the rest to a new worker.
"""

import json
import os
import pickle
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from functools import partial
from typing import IO, NamedTuple, TypeVar

from datagrunt.pdf import pipeline
from datagrunt.pdf._memory_cap import CAP_EXIT, Lifetime, apply_memory_cap
from datagrunt.pdf._process import MAX_REQUEST_BYTES, error_reply, read_message, send_message
from datagrunt.pdf.assembly import assemble_page
from datagrunt.pdf.elements import Page
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFCorruptError, PDFSourceChangedError
from datagrunt.pdf.layout import find_lattice_tables
from datagrunt.pdf.pdfium_engine import (
    DocumentHandle,
    PageFailure,
    RawPage,
    RecoveredText,
    _checked_index,
    document_metadata,
    extract_page,
    open_document,
    page_geometry,
    recover_dropped_text,
)
from datagrunt.pdf.pipeline import (
    _DONE,
    _ERROR,
    _FACTS,
    _INDEX,
    _PAGE,
    _STARTED,
    ChunkRequest,
    PDFSource,
    PipelineOptions,
    _check_unchanged,
    _span,
    error_page,
)
from datagrunt.pdf.quality import Backdrop, PageAssessment, assess_page, needs_background
from datagrunt.pdf.render import (
    RenderedPage,
    RenderFailed,
    RenderRefused,
    effective_dpi,
    render_pages,
    render_worker_pool,
)
from datagrunt.pdf.vision import fallback_page, vision_page

__all__ = ["extract_page_at"]

_T = TypeVar("_T")
_IDLE_LIFETIME_S = 3600.0

# Each page's begun steps, said to the caller: the page's index. extract_page_at says nothing.
Progress = Callable[[int], None]


def _quiet(index: int) -> None:
    """No caller to tell: extract_page_at's progress."""


def _worker_main(message: object, replies: IO[bytes]) -> None:
    """The page worker, run by the launcher's bootstrap (key ``"pages"``): a chunk's request in; replies out.

    It sends the document's facts (the first worker only), a progress
    message as it begins each page's steps, each page, pickled, as soon as
    it is extracted, in page order; then, its file re-checked after its last
    page (and at open), done. An exception that stops the chunk
    (``strict``'s PDFCorruptError, PDFSourceChangedError, an open error, a
    request it cannot read) is sent pickled instead of what is left, for the
    caller to raise. Reaching its memory cap ends it.
    """
    try:
        request = pipeline._chunk_request(message)
        lifetime = apply_memory_cap(request.memory_cap_bytes, pipeline._facts_bound_s())
        with render_worker_pool():
            _extract(request, replies, lifetime)
    except MemoryError:
        os._exit(CAP_EXIT)
    except Exception as exc:  # sent, and raised by the caller: never swallowed here
        send_message(replies, _ERROR + error_reply(exc))
        return
    send_message(replies, _DONE)


def _pool_worker_main(replies: IO[bytes]) -> None:
    """A persistent page worker for :class:`~datagrunt.pdf.pipeline.PageWorkerPool`: loops over chunks on stdin."""
    lifetime: Lifetime | None = None
    with render_worker_pool():
        while True:
            if lifetime is not None:
                lifetime.extend(_IDLE_LIFETIME_S)
            raw = read_message(sys.stdin.buffer, MAX_REQUEST_BYTES)
            if raw is None:
                return
            try:
                message = json.loads(raw)
                request = pipeline._chunk_request(message)
                if lifetime is None:
                    lifetime = apply_memory_cap(request.memory_cap_bytes, pipeline._facts_bound_s())
                else:
                    lifetime.extend(pipeline._facts_bound_s())
                _extract(request, replies, lifetime)
            except MemoryError:
                os._exit(CAP_EXIT)
            except Exception as exc:  # sent, and raised by the caller: never swallowed here
                try:
                    send_message(replies, _ERROR + error_reply(exc))
                except OSError:
                    return
                continue
            try:
                send_message(replies, _DONE)
            except OSError:
                return


def _extract(request: ChunkRequest, replies: IO[bytes], lifetime: Lifetime) -> None:
    """Open the document, send its facts (the first worker), then each of the chunk's pages; re-check the file."""
    source, pages = request.source, request.pages
    when = "when pdfium opened it" if pages is None else f"when the worker for pages {_span(pages)} opened it"
    with _opened_checked(source, when) as handle:
        if pages is None:
            info, count = document_metadata(handle), handle.page_count
            facts = {"page_count": count, "title": info.title, "author": info.author}
            send_message(replies, _FACTS + json.dumps(facts).encode("ascii"))
            pages = tuple(pipeline._chunks(count, request.workers)[0])
        lifetime.extend(pipeline._lifetime_s(len(pages), request.options))
        progress = partial(_send_started, replies)
        for page in _iter_pages(handle, source, pages, request.options, progress):  # bounded: the chunk's pages
            send_message(replies, _PAGE + pickle.dumps(page))
    _check_unchanged(source, os.stat(source.path), f"by the time the worker for pages {_span(pages)} finished them")


def _send_started(replies: IO[bytes], index: int) -> None:
    """Tell the caller the page at ``index`` is begun: it is the page an end of this worker now ends."""
    send_message(replies, _STARTED + _INDEX.pack(index))


@contextmanager
def _opened_checked(source: PDFSource, when: str) -> Iterator[DocumentHandle]:
    """The document, opened by pdfium from ``source.path`` and checked against ``source.identity`` at once.

    An open that fails on a file that is no longer the one hashed (cut
    short, replaced) raises PDFSourceChangedError, chained from the open's
    error: the file changed, which is what is said, not what pdfium made of
    the other file.
    """
    try:
        opened = open_document(source.path, password=source.password)
    except (DatagruntPDFError, OSError) as exc:
        _raise_if_changed(source, when, exc)
        raise
    with opened as handle:
        _check_unchanged(source, os.stat(source.path), when)
        yield handle


def _raise_if_changed(source: PDFSource, when: str, error: Exception) -> None:
    """Raise PDFSourceChangedError, from ``error``, if the file at the path is not ``source``'s; else return.

    A path that cannot be stat'ed has nothing to compare: the open's own error, re-raised by the caller, says why.
    """
    try:
        status = os.stat(source.path)
    except OSError:
        return
    try:
        _check_unchanged(source, status, when)
    except PDFSourceChangedError as changed:
        raise changed from error


def _iter_pages(
    handle: DocumentHandle, source: PDFSource, pages: Sequence[int], options: PipelineOptions, progress: Progress
) -> Iterator[Page]:
    """The pages at ``pages`` of the open document, in order, each as it is done; a window of pages at a time."""
    size = pipeline._WINDOW_PAGES
    for start in range(0, len(pages), size):  # bounded: one window per _WINDOW_PAGES pages
        yield from _window_pages(handle, source, pages[start : start + size], options, progress)


def _window_pages(
    handle: DocumentHandle, source: PDFSource, window: Sequence[int], options: PipelineOptions, progress: Progress
) -> Iterator[Page]:
    """The window's pages, each as it is done: steps 1-3 for each, its gated pages drawn in one worker, then step 4.

    Each step is isolated; ``progress`` hears of each page as its steps 1-3
    begin, and again as its step 4 does. A page A2 could not read is read
    by OCR (``fallback_page``) as its step 4, as a vision page is: each page
    reply carries at most one page's render and OCR budget, the one its
    bound gives it (:func:`~datagrunt.pdf.pipeline._reply_bound_s`).
    """
    staged: list[Page | _Assessed | PageFailure] = []
    for index in window:  # bounded: the window's pages
        progress(index)
        staged.append(_isolated(handle, index, options, partial(_assessed, handle, index, options)))
    drawn = _backgrounds(source, staged, options)
    for item in staged:  # bounded: the window's pages
        if isinstance(item, Page):
            yield item
            continue
        index = item.page_number - 1 if isinstance(item, PageFailure) else item.page_index
        progress(index)
        yield _isolated(handle, index, options, partial(_step_four, handle, item, drawn.get(index), source, options))


def _error_page(handle: DocumentHandle, index: int, error: str) -> Page:
    """The A1 error page of the page at ``index``, its size read from ``handle`` when it can be (module notes)."""
    try:
        geometry = page_geometry(handle, index)
    except Exception as exc:  # an error page is never dropped: its size is read when it can be
        return error_page(index, error, why=f"{type(exc).__name__}: {exc}")
    return error_page(index, error, size=(geometry.width_pt, geometry.height_pt))


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
    (page,) = _window_pages(handle, source, (index,), options, _quiet)
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


def _assessed(handle: DocumentHandle, index: int, options: PipelineOptions) -> _Assessed | PageFailure:
    """Steps 1-3 (A2, recovery when the page drops text, A3, the gate); a page A2 could not read is its PageFailure.

    Under ``strict`` that failure raises here; else the page is read by OCR in its step 4 (:func:`_step_four`).
    """
    raw = extract_page(handle, index)
    if isinstance(raw, PageFailure):
        if options.strict:
            raise PDFCorruptError(raw.message, page_number=raw.page_number)
        return raw
    dropped = raw.text.stats.dropped_text_objects
    recovered = recover_dropped_text(handle, index) if dropped > 0 else None
    assessment = assess_page(raw, ocr=options.ocr, recovered=recovered)
    return _Assessed(index, raw, recovered, assessment, _background_dpi(raw, assessment, options))


def _background_dpi(raw: RawPage, assessment: PageAssessment, options: PipelineOptions) -> int | str | None:
    """The DPI the page's background is drawn at, why it cannot be drawn, or None when the gate does not pass it."""
    if not needs_background(raw, assessment):
        return None
    bound = min(options.max_render_pixels, pipeline._BACKGROUND_MAX_PIXELS)
    try:
        return effective_dpi(pipeline._BACKGROUND_DPI, bound, raw.geometry.width_pt, raw.geometry.height_pt)
    except RenderRefused as exc:
        return str(exc)


def _backgrounds(
    source: PDFSource, staged: list[Page | _Assessed | PageFailure], options: PipelineOptions
) -> dict[int, _Drawn]:
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
            memory_cap_bytes=pipeline._vision(options).memory_cap_bytes,
            timeout_s=pipeline._BACKGROUND_START_S + pipeline._BACKGROUND_PAGE_S * len(pages),
            colour=True,
            without_text=True,
        )
    except Exception as exc:  # step 5, page by page: see the docstring
        return dict.fromkeys((index for index, _dpi in pages), exc)
    return {index: result for (index, _dpi), result in zip(pages, drawn, strict=True)}


def _step_four(
    handle: DocumentHandle,
    item: _Assessed | PageFailure,
    drawn: _Drawn | None,
    source: PDFSource,
    options: PipelineOptions,
) -> Page:
    """Step 4: a page A2 could not read is A5b's ``fallback_page`` (render and OCR); else :func:`_finished`."""
    if isinstance(item, PageFailure):
        vision = pipeline._vision(options)
        return fallback_page(
            handle,
            item,
            source=source.path,
            password=source.password,
            identity=source.identity,
            sha256=source.sha256,
            ocr=options.ocr,
            options=vision,
        )
    return _finished(item, drawn, source, options)


def _finished(item: _Assessed, drawn: _Drawn | None, source: PDFSource, options: PipelineOptions) -> Page:
    """Step 3's background check, then step 4: the page assessed again with its backdrop (or why not), then routed."""
    background = _background(item, drawn)
    assessment = item.assessment
    if background is not None:
        assessment = assess_page(
            item.raw, ocr=options.ocr, recovered=item.recovered, background=background, _initial=item.assessment
        )
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
            identity=source.identity,
            sha256=source.sha256,
            min_image_dimension=options.min_image_dimension,
            options=pipeline._vision(options),
        )
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
        identity=source.identity,
        sha256=source.sha256,
        min_image_dimension=options.min_image_dimension,
        dpi=options.dpi,
        max_render_pixels=options.max_render_pixels,
    )
