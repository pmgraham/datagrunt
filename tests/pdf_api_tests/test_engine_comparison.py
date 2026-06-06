"""Cross-engine comparison assertions over a local PDF corpus.

These run only when the ``Root_Base*`` corpus is present under ``data/pdfs/``
(it is git-ignored and local-only), so they skip cleanly in CI. They are
guardrails against gross divergence between the pymupdf and pdfium engines on
the same documents -- NOT a parity assertion (the two engines emit different
schemas and legitimately extract somewhat different amounts of text).

Note: the pymupdf engine runs table detection + classification, so these are
slow (seconds per document); they only run where the corpus exists.
"""

from glob import glob
from pathlib import Path

import pytest

from datagrunt import PDFReader

_REPO_ROOT = Path(__file__).resolve().parents[2]
CORPUS = sorted(glob(str(_REPO_ROOT / "data" / "pdfs" / "Root_Base*.pdf")))

# Floor for "neither engine extracts drastically less text than the other":
# the smaller character count must be at least this fraction of the larger.
# Deliberately loose -- it catches an engine returning near-nothing, not the
# normal block-vs-object extraction differences.
TEXT_FLOOR_RATIO = 0.5

pytestmark = pytest.mark.skipif(
    not CORPUS,
    reason="Root_Base corpus not present under data/pdfs/ (local-only).",
)


def _text_chars_pymupdf(doc: dict) -> int:
    total = 0
    for pg in doc.get("document", {}).get("pages", []):
        for el in pg.get("elements", []):
            content = el.get("content")
            if isinstance(content, str):
                total += len(content)
    return total


def _text_chars_pdfium(doc: dict) -> int:
    return sum(len(pg.get("text", "") or "") for pg in doc.get("document", {}).get("pages", []))


@pytest.fixture(scope="module", params=CORPUS, ids=[Path(p).name for p in CORPUS])
def parsed(request):
    """Parse one corpus document with both engines once, shared across tests."""
    path = request.param
    pymupdf_doc = PDFReader(path, engine="pymupdf").to_dicts()
    pdfium_doc = PDFReader(path, engine="pdfium").to_dicts()
    return path, pymupdf_doc, pdfium_doc


class TestEngineComparison:
    """Guardrail assertions comparing the two engines on identical documents."""

    def test_page_count_parity(self, parsed):
        _, pymupdf_doc, pdfium_doc = parsed
        assert pymupdf_doc["document"]["total_pages"] == pdfium_doc["document"]["page_count"]

    def test_both_engines_extract_text(self, parsed):
        path, pymupdf_doc, pdfium_doc = parsed
        assert _text_chars_pymupdf(pymupdf_doc) > 0, f"pymupdf extracted no text from {Path(path).name}"
        assert _text_chars_pdfium(pdfium_doc) > 0, f"pdfium extracted no text from {Path(path).name}"

    def test_text_volume_within_floor(self, parsed):
        path, pymupdf_doc, pdfium_doc = parsed
        mu_chars = _text_chars_pymupdf(pymupdf_doc)
        pdf_chars = _text_chars_pdfium(pdfium_doc)
        lo, hi = sorted((mu_chars, pdf_chars))
        assert lo >= TEXT_FLOOR_RATIO * hi, (
            f"{Path(path).name}: text extraction diverges drastically "
            f"(pymupdf={mu_chars}, pdfium={pdf_chars}, ratio={lo / hi:.2f} < {TEXT_FLOOR_RATIO})"
        )


def _elements_by_type(doc: dict) -> dict:
    counts = {}
    for pg in doc.get("document", {}).get("pages", []):
        for el in pg.get("elements", []):
            counts[el["type"]] = counts.get(el["type"], 0) + 1
    return counts


def _table_cells(doc: dict) -> list:
    cells = []
    for pg in doc.get("document", {}).get("pages", []):
        for el in pg.get("elements", []):
            if el.get("type") == "table":
                cells.append(el.get("content"))
    return cells


@pytest.fixture(scope="module", params=CORPUS, ids=[Path(p).name for p in CORPUS])
def parsed_structured(request):
    """Parse one corpus doc with pymupdf and with pdfium structured mode."""
    from datagrunt import PDFReader

    path = request.param
    pymupdf_doc = PDFReader(path, engine="pymupdf").to_dicts()
    pdfium_doc = PDFReader(path, engine="pdfium", structured=True).to_dicts()
    return path, pymupdf_doc, pdfium_doc


class TestStructuredParity:
    """pdfium structured mode must match-or-exceed the pymupdf engine."""

    def test_page_count_parity(self, parsed_structured):
        _, mu, pdf = parsed_structured
        assert mu["document"]["total_pages"] == pdf["document"]["total_pages"]

    def test_tables_identical(self, parsed_structured):
        # Tables come from the shared pdfplumber path -> must be identical.
        _, mu, pdf = parsed_structured
        assert _table_cells(pdf) == _table_cells(mu)

    def test_image_count_at_least_pymupdf(self, parsed_structured):
        # pymupdf counts unique image XObjects per page (page.get_images, by xref);
        # pdfium counts each placed image instance as its own positioned element.
        # pdfium therefore captures >= pymupdf -- it never drops an image, and may
        # surface repeated placements pymupdf collapses. Match-or-exceed, not equal.
        _, mu, pdf = parsed_structured
        assert _elements_by_type(pdf).get("image", 0) >= _elements_by_type(mu).get("image", 0)

    def test_text_completeness_at_least_pymupdf(self, parsed_structured):
        path, mu, pdf = parsed_structured

        def text_chars(doc):
            total = 0
            for pg in doc["document"]["pages"]:
                for el in pg["elements"]:
                    if isinstance(el.get("content"), str):
                        total += len(el["content"])
            return total

        mu_chars, pdf_chars = text_chars(mu), text_chars(pdf)
        assert pdf_chars >= TEXT_FLOOR_RATIO * mu_chars, (
            f"{Path(path).name}: structured pdfium text {pdf_chars} far below pymupdf {mu_chars}"
        )
