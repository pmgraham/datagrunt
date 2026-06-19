"""Cross-backend structural parity for PDFium vs PyMuPDF (unified schema).

Unlike ``test_engine_comparison.py`` (local ``data/pdfs/`` corpus, deselected
by default), these tests use a small seeded builder corpus and run in CI.
"""

import pytest
from pdf_corpus import BUILDERS, write_corpus
from pdf_helpers import (
    collect_schema_violations,
    elements_by_type,
    table_cells,
    text_chars,
    text_within_floor,
)

from datagrunt import PDFReader

pytestmark = pytest.mark.usefixtures("pdf_parity_corpus")


@pytest.fixture(scope="session")
def pdf_parity_corpus(tmp_path_factory):
    """Write the seeded PDF corpus once per session."""
    return write_corpus(tmp_path_factory.mktemp("pdf_parity_corpus"))


@pytest.fixture(params=sorted(BUILDERS), ids=lambda name: name)
def structured_pair(request, pdf_parity_corpus):
    """Parse one corpus PDF with both engines in structured (unified) mode."""
    path = pdf_parity_corpus[request.param]
    pymupdf_doc = PDFReader(path, engine="pymupdf").to_dicts()
    pdfium_doc = PDFReader(path, engine="pdfium").to_dicts()
    return request.param, pymupdf_doc, pdfium_doc


class TestStructuredBackendParity:
    """PDFium structured output must stay aligned with PyMuPDF on seeded docs."""

    def test_page_count_matches(self, structured_pair):
        _, pymupdf_doc, pdfium_doc = structured_pair
        assert pymupdf_doc["document"]["total_pages"] == pdfium_doc["document"]["total_pages"]

    def test_unified_element_schema(self, structured_pair):
        _, pymupdf_doc, pdfium_doc = structured_pair
        assert collect_schema_violations(pymupdf_doc) == []
        assert collect_schema_violations(pdfium_doc) == []

    def test_table_cells_identical(self, structured_pair):
        """Tables come from the shared pdfplumber path and must match exactly."""
        _, pymupdf_doc, pdfium_doc = structured_pair
        assert table_cells(pdfium_doc) == table_cells(pymupdf_doc)

    def test_image_count_at_least_pymupdf(self, structured_pair):
        _, pymupdf_doc, pdfium_doc = structured_pair
        pdfium_images = elements_by_type(pdfium_doc).get("image", 0)
        pymupdf_images = elements_by_type(pymupdf_doc).get("image", 0)
        assert pdfium_images >= pymupdf_images

    def test_text_volume_within_floor(self, structured_pair):
        name, pymupdf_doc, pdfium_doc = structured_pair
        mu_chars = text_chars(pymupdf_doc)
        pdf_chars = text_chars(pdfium_doc)
        assert text_within_floor(mu_chars, pdf_chars), (
            f"{name}: text extraction diverges drastically "
            f"(pymupdf={mu_chars}, pdfium={pdf_chars})"
        )

    def test_both_engines_extract_text(self, structured_pair):
        name, pymupdf_doc, pdfium_doc = structured_pair
        assert text_chars(pymupdf_doc) > 0, f"pymupdf extracted no text from {name}"
        assert text_chars(pdfium_doc) > 0, f"pdfium extracted no text from {name}"


class TestMultipageMarkerParity:
    """Multipage corpus: every page marker must appear on both engines."""

    def test_all_page_markers_present(self, pdf_parity_corpus):
        path = pdf_parity_corpus["multipage_markers.pdf"]
        for engine in ("pymupdf", "pdfium"):
            doc = PDFReader(path, engine=engine).to_dicts()
            pages = doc["document"]["pages"]
            assert len(pages) == 3
            for n, page in enumerate(pages, start=1):
                page_text = " ".join(
                    el["content"]
                    for el in page.get("elements", [])
                    if isinstance(el.get("content"), str)
                )
                assert f"Page Marker {n}" in page_text, f"{engine} missing marker on page {n}"
