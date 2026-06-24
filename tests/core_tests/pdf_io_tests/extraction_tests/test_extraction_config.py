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
