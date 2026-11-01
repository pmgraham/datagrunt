"""Tests for the v5 PDFWriter (#371, A7b): ``datagrunt.pdf.writer.PDFWriter``.

Sources are ReportLab PDFs (``pdf_tests.builders``) extracted for real, or
hand-built documents (``pdf_tests.documents``) given as the source. A stage
that raises, or one only counted, is a ``monkeypatch`` of the module name
for it: the writer's ``extract_document``, the bundle's ``render_page``.
"""

import os
from pathlib import Path

import pytest

from datagrunt.pdf import writer
from datagrunt.pdf.exceptions import PDFCorruptError
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.writer import PDFWriter
from pdf_tests.builders import PageSpec, TextRun, build_pdf
from pdf_tests.documents import rich_document

OPTIONS = {
    "password": None,
    "workers": 1,
    "ocr": "auto",
    "dpi": 200,
    "vision_dpi": 300,
    "max_render_pixels": 16_000_000,
    "min_image_dimension": 40,
    "strict": False,
}


def text_pdf(directory: Path, name: str = "report.pdf", pages: int = 2) -> Path:
    """A PDF of ``pages`` letter pages, one line of text a page."""
    specs = [PageSpec(items=(TextRun(f"Page {number}", 72.0, 100.0),)) for number in range(1, pages + 1)]
    return build_pdf(directory / name, specs)


def spy_extract(monkeypatch, outcome=None) -> list:
    """Record each call of the writer's extract_document (its path and options); run it, or return/raise ``outcome``."""
    calls: list = []

    def spy(source, **options):
        calls.append((Path(source), options))
        if isinstance(outcome, BaseException):
            raise outcome
        return extract_document(source, **options) if outcome is None else outcome

    monkeypatch.setattr(writer, "extract_document", spy)
    return calls


class TestConstruction:
    @pytest.mark.parametrize(
        ("options", "error", "message"),
        [
            ({"dpi": 35}, ValueError, "dpi must be >= 36"),
            ({"dpi": 300, "vision_dpi": 200}, ValueError, "vision_dpi must be >= dpi"),
            ({"max_render_pixels": 1000}, ValueError, "max_render_pixels must be >= 262144"),
            ({"workers": 0}, ValueError, "workers must be >= 1"),
            ({"workers": True}, TypeError, "workers must be an int"),
            ({"ocr": "sometimes"}, ValueError, "ocr must be"),
            ({"min_image_dimension": 0}, ValueError, "min_image_dimension must be >= 1"),
            ({"strict": 1}, TypeError, "strict must be a bool"),
            ({"password": 5}, TypeError, "password must be a str or None, got int"),
            ({"password": "a\x00b"}, ValueError, "password must not contain NUL"),
        ],
    )
    @pytest.mark.parametrize("kind", ["path", "document"])
    def test_bad_options_are_refused_before_any_work(self, tmp_path, monkeypatch, options, error, message, kind):
        calls = spy_extract(monkeypatch)
        source = text_pdf(tmp_path) if kind == "path" else rich_document()
        with pytest.raises(error, match=message):
            PDFWriter(source, **options)
        assert calls == []

    @pytest.mark.parametrize(
        ("source", "message"),
        [
            (3, "source must be a str, os.PathLike or PDFDocument, got int"),
            (None, "source must be a str, os.PathLike or PDFDocument, got NoneType"),
            (b"report.pdf", "source must be a str, os.PathLike or PDFDocument, got bytes"),
            ({}, "source must be a str, os.PathLike or PDFDocument, got dict"),
        ],
    )
    def test_source_must_be_a_path_or_a_document(self, source, message):
        with pytest.raises(TypeError, match=message):
            PDFWriter(source)

    def test_a_bytes_path_like_is_refused(self, tmp_path):
        class BytesPath:
            def __fspath__(self):
                return os.fsencode(tmp_path / "report.pdf")

        with pytest.raises(TypeError, match="source must be a str or os.PathLike, got a bytes path"):
            PDFWriter(BytesPath())

    def test_construction_reads_no_file(self, tmp_path, monkeypatch):
        calls = spy_extract(monkeypatch)
        PDFWriter(tmp_path / "missing.pdf")
        assert calls == []

    def test_a_missing_source_fails_on_first_use(self, tmp_path):
        made = PDFWriter(tmp_path / "missing.pdf")
        with pytest.raises(FileNotFoundError):
            made._extracted()


class TestDocumentCache:
    def test_the_source_is_extracted_on_first_use_with_the_writers_options(self, tmp_path, monkeypatch):
        path = text_pdf(tmp_path)
        calls = spy_extract(monkeypatch)
        options = {**OPTIONS, "workers": 2, "ocr": "never", "dpi": 100, "vision_dpi": 150, "strict": True}
        options.update(max_render_pixels=1_000_000, min_image_dimension=8)
        made = PDFWriter(path, **options)
        assert calls == []
        document = made._extracted()
        assert calls == [(path, options)]
        assert document == extract_document(path, **options)

    def test_the_document_is_extracted_once_then_cached(self, tmp_path, monkeypatch):
        calls = spy_extract(monkeypatch)
        made = PDFWriter(text_pdf(tmp_path))
        first = made._extracted()
        assert made._extracted() is first
        assert len(calls) == 1

    def test_a_failed_extraction_is_not_cached(self, tmp_path, monkeypatch):
        path = text_pdf(tmp_path)
        made = PDFWriter(path)
        calls = spy_extract(monkeypatch, PDFCorruptError("bad xref"))
        with pytest.raises(PDFCorruptError, match="bad xref"):
            made._extracted()
        spy_extract(monkeypatch)
        assert made._extracted() == extract_document(path)
        assert len(calls) == 1

    def test_a_document_source_is_the_document_never_extracted(self, monkeypatch):
        calls = spy_extract(monkeypatch)
        document = rich_document()
        made = PDFWriter(document, password="ignored")
        assert made._extracted() is document
        assert calls == []

    def test_the_source_path_is_made_absolute_at_construction(self, tmp_path, monkeypatch):
        path = text_pdf(tmp_path)
        calls = spy_extract(monkeypatch)
        monkeypatch.chdir(tmp_path)
        made = PDFWriter("report.pdf")
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        assert made._extracted().metadata.source_name == "report.pdf"
        assert calls[0][0] == path

    def test_an_encrypted_source_is_opened_with_its_password(self, tmp_path):
        path = build_pdf(
            tmp_path / "locked.pdf", [PageSpec(items=(TextRun("Secret", 72.0, 100.0),))], user_password="pw"
        )
        document = PDFWriter(path, password="pw")._extracted()
        assert "Secret" in document.to_markdown()

    def test_the_password_is_not_in_the_writers_repr(self, tmp_path):
        assert "hunter2" not in repr(PDFWriter(text_pdf(tmp_path), password="hunter2"))
