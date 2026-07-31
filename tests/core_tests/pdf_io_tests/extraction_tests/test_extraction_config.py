"""Tests for the private _PDFExtractionConfig extraction tunables."""

import pickle

import pytest

from datagrunt.core.pdf_io.extraction.config import (
    _DEFAULT_MIN_IMAGE_DIMENSION,
    _PDFExtractionConfig,
)


class TestPDFExtractionConfig:
    def test_default_matches_legacy_constant(self):
        assert _DEFAULT_MIN_IMAGE_DIMENSION == 40
        assert _PDFExtractionConfig().min_image_dimension == 40

    def test_accepts_custom_value(self):
        assert _PDFExtractionConfig(min_image_dimension=15).min_image_dimension == 15

    def test_zero_is_allowed_disables_filter(self):
        assert _PDFExtractionConfig(min_image_dimension=0).min_image_dimension == 0

    def test_negative_raises_value_error(self):
        with pytest.raises(ValueError):
            _PDFExtractionConfig(min_image_dimension=-1)

    def test_float_raises_type_error(self):
        with pytest.raises(TypeError):
            _PDFExtractionConfig(min_image_dimension=12.5)

    def test_bool_raises_type_error(self):
        with pytest.raises(TypeError):
            _PDFExtractionConfig(min_image_dimension=True)

    def test_is_frozen(self):
        cfg = _PDFExtractionConfig()
        with pytest.raises(Exception):
            cfg.min_image_dimension = 99  # frozen → FrozenInstanceError

    def test_is_picklable_and_round_trips(self):
        cfg = _PDFExtractionConfig(min_image_dimension=15)
        assert pickle.loads(pickle.dumps(cfg)) == cfg


class TestPDFExtractionConfigDPIFields:
    def test_defaults_match_legacy_constants(self):
        cfg = _PDFExtractionConfig()
        assert cfg.ocr_standard_dpi == 150
        assert cfg.ocr_large_format_dpi == 75
        assert cfg.ocr_large_format_dimension == 1500
        assert cfg.render_dpi == 300

    @pytest.mark.parametrize(
        "field", ["ocr_standard_dpi", "ocr_large_format_dpi", "ocr_large_format_dimension", "render_dpi"]
    )
    def test_rejects_bool(self, field):
        with pytest.raises(TypeError):
            _PDFExtractionConfig(**{field: True})

    @pytest.mark.parametrize(
        "field", ["ocr_standard_dpi", "ocr_large_format_dpi", "ocr_large_format_dimension", "render_dpi"]
    )
    @pytest.mark.parametrize("bad", [0, -1])
    def test_rejects_non_positive(self, field, bad):
        with pytest.raises(ValueError):
            _PDFExtractionConfig(**{field: bad})

    def test_remains_picklable_with_overrides(self):
        # Same-process round-trip of a locally-constructed, trusted instance
        # (not deserialization of untrusted/external data) — this proves the
        # picklability contract the config relies on to cross the pdfium
        # process-pool worker boundary.
        cfg = _PDFExtractionConfig(ocr_standard_dpi=300, render_dpi=600)
        assert pickle.loads(pickle.dumps(cfg)) == cfg
