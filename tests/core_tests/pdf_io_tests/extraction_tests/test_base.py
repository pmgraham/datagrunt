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


def test_extract_page_default_delegates_and_honors_skip_logic(sample_pdf):
    """The base default extract_page returns the same triple parse_page assembled
    from the three primitives, skipping text when no text layer and images when none."""
    from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend

    backend = PyMuPDFBackend(sample_pdf)
    with backend:
        analysis, text_blocks, images = ExtractionBackend.extract_page(backend, 0)
        # Compare against the primitives called directly.
        expected_analysis = backend.analyze_page(0)
        expected_text = backend.extract_text_blocks(0) if expected_analysis.has_text_layer else []
        expected_images = (
            backend.extract_images(0) if expected_analysis.image_count > 0 else []
        )

    assert analysis == expected_analysis
    assert text_blocks == expected_text
    assert images == expected_images
    assert text_blocks  # sample_pdf has a text layer
    assert images       # sample_pdf has an embedded image
