"""Tests for the v5 document pipeline (#372, A8a): ``datagrunt.pdf.pipeline``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``): a
corrupt page with ``break_page``, an encrypted PDF with ``user_password``,
and a split table with A4b's own table fixtures. A stage that raises is a
``monkeypatch`` of the pipeline's module name for it.
"""

import os
import pickle
import re
from pathlib import Path

import pytest

from datagrunt.pdf.pipeline import PDFSource, PipelineOptions

GOOD = {
    "workers": 1,
    "ocr": "auto",
    "dpi": 200,
    "vision_dpi": 300,
    "max_render_pixels": 16_000_000,
    "min_image_dimension": 40,
    "strict": False,
}


class TestPipelineOptions:
    """The spec's construction checks (invariant 2), each with its exact message."""

    def test_the_defaults_are_the_specs(self):
        assert PipelineOptions() == PipelineOptions(**GOOD)

    @pytest.mark.parametrize(
        "change,message",
        [
            pytest.param({"workers": 0}, "workers must be >= 1, got 0", id="workers"),
            pytest.param({"dpi": 35}, "dpi must be >= 36, got 35", id="dpi"),
            pytest.param({"vision_dpi": 199}, "vision_dpi must be >= dpi (200), got 199", id="vision-under-dpi"),
            pytest.param({"dpi": 36, "vision_dpi": 35}, "vision_dpi must be >= 36, got 35", id="vision-floor"),
            pytest.param(
                {"max_render_pixels": 262_143}, "max_render_pixels must be >= 262144, got 262143", id="pixels"
            ),
            pytest.param({"min_image_dimension": 0}, "min_image_dimension must be >= 1, got 0", id="min-image"),
            pytest.param({"ocr": "sometimes"}, "ocr must be 'auto' or 'always' or 'never', got 'sometimes'", id="ocr"),
        ],
    )
    def test_a_bad_value_raises_value_error(self, change, message):
        with pytest.raises(ValueError, match=re.escape(message)):
            PipelineOptions(**{**GOOD, **change})

    @pytest.mark.parametrize(
        "change,message",
        [
            pytest.param({"workers": 2.0}, "workers must be an int, got 2.0 (float)", id="workers"),
            pytest.param({"workers": True}, "workers must be an int, got True (bool)", id="workers-bool"),
            pytest.param({"dpi": "200"}, "dpi must be an int, got '200' (str)", id="dpi"),
            pytest.param({"vision_dpi": 300.0}, "vision_dpi must be an int, got 300.0 (float)", id="vision"),
            pytest.param(
                {"max_render_pixels": None}, "max_render_pixels must be an int, got None (NoneType)", id="pixels"
            ),
            pytest.param(
                {"min_image_dimension": 40.0}, "min_image_dimension must be an int, got 40.0 (float)", id="min-image"
            ),
            pytest.param({"ocr": None}, "ocr must be a str, got None (NoneType)", id="ocr"),
            pytest.param({"strict": 1}, "strict must be a bool, got 1 (int)", id="strict"),
        ],
    )
    def test_a_bad_type_raises_type_error(self, change, message):
        with pytest.raises(TypeError, match=re.escape(message)):
            PipelineOptions(**{**GOOD, **change})

    @pytest.mark.parametrize(
        "change",
        [
            pytest.param({"workers": 1, "dpi": 36, "vision_dpi": 36}, id="floors"),
            pytest.param({"max_render_pixels": 262_144, "min_image_dimension": 1}, id="pixel-floors"),
            pytest.param({"ocr": "always", "strict": True}, id="always-strict"),
            pytest.param({"ocr": "never"}, id="never"),
        ],
    )
    def test_values_at_the_bounds_are_accepted(self, change):
        options = PipelineOptions(**{**GOOD, **change})

        assert {name: getattr(options, name) for name in change} == change

    def test_options_are_frozen_and_picklable(self):
        options = PipelineOptions(workers=4, ocr="never", strict=True)

        assert pickle.loads(pickle.dumps(options)) == options
        with pytest.raises(AttributeError):
            options.workers = 2  # type: ignore[misc]


class TestPDFSource:
    """Where a worker opens the document: an absolute path, and the password (never shown)."""

    def test_a_relative_path_is_made_absolute(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        assert PDFSource(path="doc.pdf").path == tmp_path / "doc.pdf"
        assert PDFSource(path=Path("doc.pdf")).path.is_absolute()

    def test_the_password_is_kept_but_never_shown_or_compared(self, tmp_path):
        source = PDFSource(path=tmp_path / "doc.pdf", password="hunter2")

        assert source.password == "hunter2"
        assert "hunter2" not in repr(source)
        assert source == PDFSource(path=tmp_path / "doc.pdf", password="other")
        assert pickle.loads(pickle.dumps(source)).password == "hunter2"

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"path": b"doc.pdf"}, TypeError, "source must be a str or os.PathLike, got bytes", id="bytes"),
            pytest.param({"path": 3}, TypeError, "source must be a str or os.PathLike, got int", id="int"),
            pytest.param(
                {"path": "doc.pdf", "password": 1234}, TypeError, "password must be a str or None, got int", id="pw"
            ),
            pytest.param(
                {"path": "doc.pdf", "password": "a\x00b"},
                ValueError,
                "password must not contain NUL: pdfium would read only the part before it",
                id="pw-nul",
            ),
        ],
    )
    def test_bad_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            PDFSource(**fields)

    def test_a_password_error_never_shows_the_password(self):
        with pytest.raises(TypeError) as caught:
            PDFSource(path=os.devnull, password=b"s3cret")

        assert "s3cret" not in str(caught.value)
