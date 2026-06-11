"""Regression tests for issue #93: pdfium page/textpage handle leak.

Page and textpage handles must be closed deterministically as each page is
parsed, not left open until the document closes or the cyclic garbage collector
runs. These tests run with ``gc.disable()`` so any reliance on GC for handle
reclamation is exposed, and they inspect handles WHILE THE DOCUMENT IS STILL
OPEN so that the document's own cascade-close on exit cannot mask a per-page
leak.

A pypdfium2 handle (``PdfPage`` / ``PdfTextPage``) is open while its
``weakref.finalize`` is still armed: ``obj._finalizer is not None and
obj._finalizer.alive``. Calling ``close()`` detaches the finalizer (sets it to
``None``) and frees the underlying handle. On the buggy code, parsing N pages
leaves N ``PdfPage`` + N ``PdfTextPage`` finalizers still alive.
"""

import gc

import pytest

# pypdfium2 is an optional extra; skip cleanly if it is unavailable.
pdfium_page = pytest.importorskip("pypdfium2._helpers.page")
pdfium_textpage = pytest.importorskip("pypdfium2._helpers.textpage")

from datagrunt.core.pdf_io.extraction.pdfium_backend import PdfiumBackend
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.core.pdf_io.extraction.pdfium_native import PdfiumNativeReader

PdfPage = pdfium_page.PdfPage
PdfTextPage = pdfium_textpage.PdfTextPage


def _is_open(handle) -> bool:
    """Return True if a pypdfium2 handle is still open (finalizer armed)."""
    finalizer = getattr(handle, "_finalizer", None)
    return finalizer is not None and finalizer.alive


def _open_handle_counts() -> tuple:
    """Return (open_pages, open_textpages) currently alive in the process.

    ``gc.get_objects()`` is queried WITHOUT a preceding ``gc.collect()`` so a
    handle only counts as reclaimed if it was explicitly closed, never because
    the cyclic collector happened to run.
    """
    open_pages = sum(
        1 for obj in gc.get_objects() if isinstance(obj, PdfPage) and _is_open(obj)
    )
    open_textpages = sum(
        1 for obj in gc.get_objects() if isinstance(obj, PdfTextPage) and _is_open(obj)
    )
    return open_pages, open_textpages


@pytest.fixture
def _gc_disabled():
    """Disable the cyclic GC for the duration of a test, then restore it."""
    was_enabled = gc.isenabled()
    gc.collect()  # start from a clean slate
    gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()


class TestPdfiumHandleLeak:
    """No page/textpage handle may stay open after the page it belongs to is parsed."""

    def test_native_reader_closes_page_handles(self, multipage_pdf, _gc_disabled):
        with PdfiumNativeReader(multipage_pdf) as reader:
            for index in range(3):
                reader.parse_page(index)
            # Inspect while the document is still open: a leak shows up here.
            open_pages, open_textpages = _open_handle_counts()

        assert open_pages == 0, f"{open_pages} PdfPage handle(s) left open after parse"
        assert open_textpages == 0, f"{open_textpages} PdfTextPage handle(s) left open after parse"

    def test_backend_closes_page_handles(self, multipage_pdf, _gc_disabled):
        with PdfiumBackend(multipage_pdf) as backend:
            for index in range(3):
                backend.analyze_page(index)
                backend.extract_text_blocks(index)
                backend.extract_images(index)
            open_pages, open_textpages = _open_handle_counts()

        assert open_pages == 0, f"{open_pages} PdfPage handle(s) left open after parse"
        assert open_textpages == 0, f"{open_textpages} PdfTextPage handle(s) left open after parse"

    def test_document_page_is_context_manager(self, multipage_pdf, _gc_disabled):
        with PdfiumDocument(multipage_pdf) as doc:
            with doc.page(0) as page:
                assert page.size()[0] > 0
                raw_page, raw_textpage = page._page, page._textpage
                assert _is_open(raw_page) and _is_open(raw_textpage)
            # Leaving the `with doc.page(...)` block must close both handles.
            assert not _is_open(raw_page), "page handle not closed on context exit"
            assert not _is_open(raw_textpage), "textpage handle not closed on context exit"
