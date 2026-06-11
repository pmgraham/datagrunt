"""Guard tests for issue #102: the PyMuPDF engine must open the document once
per document, not once per page.

The historical ``to_dicts`` submitted bare ``assembler.parse_page`` calls to a
thread pool without holding the backend / table-extractor contexts open, so each
page entered the thread-local context at depth 0 and reran both ``pymupdf.open``
and ``pdfplumber.open`` once per page. For an N-page document that is N + N opens
instead of 1 + 1, which the issue measured at 3.1x slower.

These tests spy on ``pymupdf.open`` and ``pdfplumber.open`` and assert each is
called exactly once for a multi-page sequential parse, while pinning the
``to_dicts`` output so the optimization stays output-preserving.
"""

import copy

import pytest

from datagrunt.core.pdf_io.engines import PDFReaderPyMuPDFEngine


def _normalize(document):
    """Strip volatile fields (timestamps) so two parses compare for equality."""
    doc = copy.deepcopy(document)
    doc["document"]["processing_id"] = "<id>"
    return doc


@pytest.fixture
def open_spy(monkeypatch):
    """Wrap ``pymupdf.open`` and ``pdfplumber.open`` with call counters.

    Both libraries are imported lazily inside the extraction modules, so patching
    the attribute on the real module object is observed by every caller.
    """
    import pdfplumber
    import pymupdf

    counts = {"pymupdf": 0, "pdfplumber": 0}

    real_pymupdf_open = pymupdf.open
    real_pdfplumber_open = pdfplumber.open

    def spy_pymupdf_open(*args, **kwargs):
        counts["pymupdf"] += 1
        return real_pymupdf_open(*args, **kwargs)

    def spy_pdfplumber_open(*args, **kwargs):
        counts["pdfplumber"] += 1
        return real_pdfplumber_open(*args, **kwargs)

    monkeypatch.setattr(pymupdf, "open", spy_pymupdf_open)
    monkeypatch.setattr(pdfplumber, "open", spy_pdfplumber_open)
    return counts


class TestPyMuPDFOpensOncePerDocument:
    """The sequential PyMuPDF engine must hold the document open across pages."""

    def test_pymupdf_opened_once_for_multipage_parse(self, multipage_pdf, open_spy):
        engine = PDFReaderPyMuPDFEngine(multipage_pdf, workers=1)
        document = engine.to_dicts()

        # 3-page fixture: must be opened ONCE, not once per page.
        assert document["document"]["total_pages"] == 3
        assert open_spy["pymupdf"] == 1, (
            f"pymupdf.open called {open_spy['pymupdf']}x; expected 1 (once per document)"
        )

    def test_pdfplumber_opened_at_most_once_for_multipage_parse(self, multipage_pdf, open_spy):
        engine = PDFReaderPyMuPDFEngine(multipage_pdf, workers=1)
        engine.to_dicts()

        # Held-open: at most one pdfplumber.open for the whole document. The
        # multipage fixture is text-only with no line drawings, so the lazy
        # table extractor need never open it at all.
        assert open_spy["pdfplumber"] <= 1, (
            f"pdfplumber.open called {open_spy['pdfplumber']}x; expected <= 1 per document"
        )

    def test_text_only_pages_do_not_open_pdfplumber(self, multipage_pdf, open_spy):
        # The multipage fixture has a text layer and no line drawings, so the
        # table extractor is never consulted: pdfplumber must not be opened.
        engine = PDFReaderPyMuPDFEngine(multipage_pdf, workers=1)
        engine.to_dicts()
        assert open_spy["pdfplumber"] == 0, (
            f"pdfplumber.open called {open_spy['pdfplumber']}x for a text-only "
            "document; expected 0 (lazy table extractor)"
        )


class TestThreadedOpensOncePerWorker:
    """With multiple workers the document is opened once per worker, not per page."""

    def test_pymupdf_opened_once_per_worker(self, multipage_pdf, open_spy):
        # 3-page fixture parsed with 2 workers: at most one open per worker
        # (<= 2), never one per page (which would be 3).
        engine = PDFReaderPyMuPDFEngine(multipage_pdf, workers=2)
        document = engine.to_dicts()
        assert document["document"]["total_pages"] == 3
        assert open_spy["pymupdf"] <= 2, (
            f"pymupdf.open called {open_spy['pymupdf']}x for 2 workers; "
            "expected <= 2 (once per worker)"
        )

    def test_threaded_output_matches_sequential(self, multipage_pdf):
        sequential = PDFReaderPyMuPDFEngine(multipage_pdf, workers=1).to_dicts()
        threaded = PDFReaderPyMuPDFEngine(multipage_pdf, workers=2).to_dicts()
        assert _normalize(threaded) == _normalize(sequential)


class TestOutputUnchanged:
    """The held-open path must produce identical output to per-page parsing."""

    def test_to_dicts_matches_per_page_assembly(self, multipage_pdf):
        from datagrunt.core.pdf_io import pdfcomponents

        engine = PDFReaderPyMuPDFEngine(multipage_pdf, workers=1)
        produced = engine.to_dicts()

        # Reconstruct the expected document the per-page way: parse each page
        # through the public assembler entry point and combine. parse_page is the
        # stable per-page contract that to_dicts must continue to honor.
        assembler = pdfcomponents.DocumentAssembler(multipage_pdf)
        total_pages = engine._total_pages()
        pages = [assembler.parse_page(idx) for idx in range(total_pages)]
        expected = assembler.combine(total_pages, pages, [])

        assert _normalize(produced) == _normalize(expected)

    def test_to_dicts_is_deterministic(self, multipage_pdf):
        engine = PDFReaderPyMuPDFEngine(multipage_pdf, workers=1)
        first = engine.to_dicts()
        second = engine.to_dicts()
        assert _normalize(first) == _normalize(second)
