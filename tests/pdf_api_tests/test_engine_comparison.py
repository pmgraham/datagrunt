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
