"""Tests for the ExtractionBackend ABC."""

import pytest

from datagrunt.core.pdf_io.extraction.base import ExtractionBackend


def test_cannot_instantiate_abstract():
    with pytest.raises(TypeError):
        ExtractionBackend("x.pdf")


def test_missing_file_raises(tmp_path):
    class Dummy(ExtractionBackend):
        def analyze_page(self, page_number): ...
        def extract_text_blocks(self, page_number): ...
        def extract_images(self, page_number, output_dir=None, name_prefix="page"): ...
        def ocr_page(self, page_number, dpi=300): ...

    with pytest.raises(FileNotFoundError):
        Dummy(tmp_path / "nope.pdf")
