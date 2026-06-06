"""Tests for PdfPlumberTableExtractor."""

from datagrunt.core.pdf_io.extraction.shapes import TableBlock
from datagrunt.core.pdf_io.extraction.tables import PdfPlumberTableExtractor


def test_returns_list_of_tableblock(sample_pdf):
    # sample_pdf has no ruled tables; result is a (possibly empty) list of TableBlock.
    result = PdfPlumberTableExtractor(sample_pdf).extract(0)
    assert isinstance(result, list)
    assert all(isinstance(t, TableBlock) for t in result)
