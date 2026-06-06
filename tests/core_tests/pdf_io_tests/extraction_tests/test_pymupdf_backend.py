"""Tests for PyMuPDFBackend."""

import pytest

from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend
from datagrunt.core.pdf_io.extraction.shapes import ImageBlock, PageAnalysis, TextBlock


class TestPyMuPDFBackend:
    def test_analyze_page(self, sample_pdf):
        a = PyMuPDFBackend(sample_pdf).analyze_page(0)
        assert isinstance(a, PageAnalysis) and a.has_text_layer is True

    def test_text_blocks_classified(self, sample_pdf):
        blocks = PyMuPDFBackend(sample_pdf).extract_text_blocks(0)
        assert blocks and all(isinstance(b, TextBlock) for b in blocks)
        assert "header" in {b.classification for b in blocks}

    def test_images(self, sample_pdf, tmp_path):
        imgs = PyMuPDFBackend(sample_pdf).extract_images(0, output_dir=str(tmp_path))
        assert all(isinstance(i, ImageBlock) for i in imgs)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PyMuPDFBackend("nope.pdf")
