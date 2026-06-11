"""Tests for PDF engines."""

import json
import os

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.core.pdf_io.engines import (
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
    set_export_filename,
)


class TestSetExportFilename:
    """Test suite for the output path resolver."""

    def test_returns_default_when_none(self):
        assert set_export_filename("output.json", None) == "output.json"

    def test_returns_override_when_given(self):
        assert set_export_filename("output.json", "custom.json") == "custom.json"


class TestPDFEngineProperties:
    """Test suite for the engine properties dataclass."""

    def test_defaults(self):
        props = PDFEngineProperties(filepath="x.pdf")
        assert props.json_export_filename == "output.json"
        assert props.json_newline_export_filename == "output.jsonl"
        assert props.images_export_dir == "output_images"
        assert "pymupdf" in props.valid_engines


class TestPDFReaderEngine:
    """Test suite for the PyMuPDF reader engine."""

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PDFReaderPyMuPDFEngine("nope.pdf")

    def test_to_dicts(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        doc = engine.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_get_sample(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        page = engine.get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        df = engine.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 2
        assert {"id", "type", "page", "x", "content", "metadata"} <= set(df.columns)

    def test_to_arrow_table(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        table = engine.to_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows >= 2

    @staticmethod
    def _zero_page_pdf(tmp_path):
        """Write a valid zero-page PDF that pymupdf opens but pdfium cannot."""
        import io

        import pypdfium2 as pdfium

        doc = pdfium.PdfDocument.new()
        buf = io.BytesIO()
        doc.save(buf)
        path = tmp_path / "zero_page.pdf"
        path.write_bytes(buf.getvalue())
        return path

    def test_zero_page_pdf_parses_cleanly(self, tmp_path):
        """A zero-page PDF must parse on the pymupdf engine, not raise PdfiumError.

        pymupdf opens zero-page PDFs (page_count == 0), but pdfium refuses to
        load them at all. Counting pages with the engine's own backend keeps the
        pymupdf path off pdfium so it returns an empty document instead of
        inheriting pdfium's stricter failure surface (see issue #95).
        """
        pdf = self._zero_page_pdf(tmp_path)
        doc = PDFReaderPyMuPDFEngine(pdf).to_dicts()
        assert doc["document"]["total_pages"] == 0
        assert doc["document"]["pages"] == []


class TestPDFWriterEngine:
    """Test suite for the PyMuPDF writer engine."""

    def test_write_json_default_name(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json()
        assert os.path.basename(path) == "output.json"
        with open(path) as f:
            data = json.load(f)
        assert data["document"]["total_pages"] == 1

    def test_write_json_custom_name(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json(export_filename="custom.json")
        assert os.path.basename(path) == "custom.json"
        assert os.path.isfile(path)

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json_newline_delimited()
        assert os.path.basename(path) == "output.jsonl"
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        assert len(lines) >= 1
        json.loads(lines[0])  # each line is valid JSON

    def test_extract_images(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        paths = engine.extract_images(output_dir=str(out))
        assert len(paths) >= 1
        assert all(os.path.isfile(p) for p in paths)


class TestPDFWriterDedupe:
    """Test suite for image de-duplication in the writer engine."""

    @staticmethod
    def _two_page_dupe_pdf(tmp_path):
        """Build a 2-page PDF with the same image embedded on each page."""
        import pymupdf

        doc = pymupdf.open()
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 120, 120))
        pix.set_rect(pix.irect, (0, 128, 255))
        img = pix.tobytes("png")
        for _ in range(2):
            page = doc.new_page(width=300, height=300)
            page.insert_image(pymupdf.Rect(50, 50, 170, 170), stream=img)
        path = tmp_path / "dupe.pdf"
        doc.save(str(path))
        doc.close()
        return str(path)

    def test_extract_images_dedupes_by_default(self, tmp_path):
        pdf = self._two_page_dupe_pdf(tmp_path)
        out = tmp_path / "imgs"
        engine = PDFWriterPyMuPDFEngine(pdf)
        paths = engine.extract_images(output_dir=str(out))
        # The two byte-identical images collapse to a single file.
        assert len(paths) == 1
        assert os.path.isfile(paths[0])
        assert len([f for f in os.listdir(out) if f.endswith(".png")]) == 1

    def test_extract_images_dedupe_disabled(self, tmp_path):
        pdf = self._two_page_dupe_pdf(tmp_path)
        out = tmp_path / "imgs2"
        engine = PDFWriterPyMuPDFEngine(pdf)
        paths = engine.extract_images(output_dir=str(out), dedupe=False)
        assert len(paths) == 2

    def test_write_json_dedupes_image_references(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pdf = self._two_page_dupe_pdf(tmp_path)
        engine = PDFWriterPyMuPDFEngine(pdf)
        jpath = engine.write_json(image_output_dir=str(tmp_path / "imgs3"))
        with open(jpath) as f:
            doc = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for pg in doc["document"]["pages"]
            for e in pg["elements"]
            if e["type"] == "image"
        ]
        # Both image elements reference the same (single) on-disk file.
        assert len(img_paths) == 2
        assert len(set(img_paths)) == 1
        assert all(os.path.isfile(p) for p in img_paths)


class TestDropLayoutTablesThreading:
    """Verify the drop_layout_tables flag is plumbed through the engines."""

    def _spy(self, monkeypatch):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        calls = []
        original = ParsedDocument.drop_layout_tables

        def spy(self, *args, **kwargs):
            calls.append(True)
            return original(self, *args, **kwargs)

        monkeypatch.setattr(ParsedDocument, "drop_layout_tables", spy)
        return calls

    def test_reader_to_dicts_invokes_filter_when_true(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReaderPyMuPDFEngine(sample_pdf).to_dicts(drop_layout_tables=True)
        assert calls == [True]

    def test_reader_to_dicts_default_does_not_filter(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReaderPyMuPDFEngine(sample_pdf).to_dicts()
        assert calls == []

    def test_reader_to_dataframe_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReaderPyMuPDFEngine(sample_pdf).to_dataframe(drop_layout_tables=True)
        assert calls == [True]

    def test_writer_write_json_threads_flag(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        calls = self._spy(monkeypatch)
        PDFWriterPyMuPDFEngine(sample_pdf).write_json(drop_layout_tables=True)
        assert calls == [True]


class TestPDFReaderPyMuPDFSequential:
    """PyMuPDF engine must parse pages sequentially (MuPDF is not thread-safe).

    These are correctness/regression guards rather than red-green tests: a
    thread-safety race is non-deterministic, so we cannot reliably reproduce a
    crash. Instead we pin the contract -- no ThreadPoolExecutor is used, multi
    page output is complete and correctly ordered, and a warning fires when a
    caller requests workers > 1 (which is now ignored).
    """

    @staticmethod
    def _four_page_pdf(tmp_path):
        """Build a 4-page PDF with a distinct marker on each page."""
        import pymupdf

        doc = pymupdf.open()
        for n in range(1, 5):
            page = doc.new_page(width=612, height=792)
            page.insert_text((72, 72), f"Sequential Marker {n}", fontsize=18)
            page.insert_text((72, 110), f"Body content for page {n}.", fontsize=11)
        path = tmp_path / "seq.pdf"
        doc.save(str(path))
        doc.close()
        return str(path)

    def test_multipage_parses_all_pages_in_order(self, tmp_path):
        pdf = self._four_page_pdf(tmp_path)
        doc = PDFReaderPyMuPDFEngine(pdf, workers=4).to_dicts()

        pages = doc["document"]["pages"]
        assert doc["document"]["total_pages"] == 4
        assert [p["page_number"] for p in pages] == [1, 2, 3, 4]
        for n, page in enumerate(pages, start=1):
            text = " ".join(
                e.get("content", "") for e in page["elements"] if e.get("type") in ("header", "body_text")
            )
            assert f"Sequential Marker {n}" in text

    def test_does_not_use_thread_pool_executor(self, tmp_path, monkeypatch):
        """The engine must never dispatch pages onto a ThreadPoolExecutor."""
        import concurrent.futures

        calls = []
        original_init = concurrent.futures.ThreadPoolExecutor.__init__

        def spy_init(self, *args, **kwargs):
            calls.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(concurrent.futures.ThreadPoolExecutor, "__init__", spy_init)

        pdf = self._four_page_pdf(tmp_path)
        PDFReaderPyMuPDFEngine(pdf, workers=4).to_dicts()
        assert calls == []

    def test_warns_once_when_workers_gt_one(self, tmp_path, caplog):
        import logging

        pdf = self._four_page_pdf(tmp_path)
        with caplog.at_level(logging.WARNING, logger="datagrunt.core.pdf_io.engines"):
            PDFReaderPyMuPDFEngine(pdf, workers=4).to_dicts()

        warnings = [r for r in caplog.records if "not thread-safe" in r.getMessage()]
        assert len(warnings) == 1

    def test_no_warning_when_workers_is_one(self, tmp_path, caplog):
        import logging

        pdf = self._four_page_pdf(tmp_path)
        with caplog.at_level(logging.WARNING, logger="datagrunt.core.pdf_io.engines"):
            PDFReaderPyMuPDFEngine(pdf, workers=1).to_dicts()

        warnings = [r for r in caplog.records if "not thread-safe" in r.getMessage()]
        assert warnings == []


class TestPDFReaderPdfiumEngine:
    """Test suite for the PDFium reader engine."""

    def test_to_dicts_native_schema(self, sample_pdf):
        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        doc = PDFReaderPdfiumEngine(sample_pdf).to_dicts()
        assert doc["document"]["page_count"] == 1
        page = doc["document"]["pages"][0]
        assert set(page.keys()) == {
            "page_number", "width", "height", "text",
            "text_objects", "images", "ocr",
        }
        assert "Quarterly Report" in page["text"]

    def test_get_sample_returns_first_page(self, sample_pdf):
        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        page = PDFReaderPdfiumEngine(sample_pdf).get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe_has_rows(self, sample_pdf):
        import polars as pl

        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        df = PDFReaderPdfiumEngine(sample_pdf).to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert "type" in df.columns

    def test_to_arrow_table_has_rows(self, sample_pdf):
        import pyarrow as pa

        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        table = PDFReaderPdfiumEngine(sample_pdf).to_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows > 0

    def test_missing_file_raises(self):
        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        with pytest.raises(FileNotFoundError):
            PDFReaderPdfiumEngine("nope.pdf")

    def test_to_dicts_multipage_ordered(self, multipage_pdf):
        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        doc = PDFReaderPdfiumEngine(multipage_pdf).to_dicts()
        pages = doc["document"]["pages"]
        assert doc["document"]["page_count"] == 3
        assert [p["page_number"] for p in pages] == [1, 2, 3]
        for n, page in enumerate(pages, start=1):
            assert f"Page Marker {n}" in page["text"]


class TestPDFWriterPdfiumEngine:
    """Test suite for the PDFium writer engine."""

    def test_write_json(self, sample_pdf, tmp_path):
        import json

        from datagrunt.core.pdf_io.engines import PDFWriterPdfiumEngine

        out = tmp_path / "doc.json"
        result = PDFWriterPdfiumEngine(sample_pdf).write_json(export_filename=str(out))
        assert result == str(out)
        data = json.loads(out.read_text())
        assert data["document"]["page_count"] == 1

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path):
        from datagrunt.core.pdf_io.engines import PDFWriterPdfiumEngine

        out = tmp_path / "doc.jsonl"
        result = PDFWriterPdfiumEngine(sample_pdf).write_json_newline_delimited(export_filename=str(out))
        assert result == str(out)
        lines = [ln for ln in out.read_text().splitlines() if ln.strip()]
        assert len(lines) > 0

    def test_extract_images_returns_paths(self, sample_pdf, tmp_path):
        import os

        from datagrunt.core.pdf_io.engines import PDFWriterPdfiumEngine

        out = tmp_path / "imgs"
        paths = PDFWriterPdfiumEngine(sample_pdf).extract_images(output_dir=str(out))
        assert len(paths) > 0
        assert all(os.path.isfile(p) for p in paths)


class TestPDFReaderPdfiumStructured:
    """pdfium engine in structured mode emits the unified element schema."""

    def test_structured_to_dicts_unified_schema(self, sample_pdf):
        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        doc = PDFReaderPdfiumEngine(sample_pdf, structured=True).to_dicts()
        assert "total_pages" in doc["document"]  # unified envelope key
        page = doc["document"]["pages"][0]
        assert set(page.keys()) == {"page_number", "width", "height", "classification", "elements"}

    def test_default_is_native_schema(self, sample_pdf):
        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        page = PDFReaderPdfiumEngine(sample_pdf).to_dicts()["document"]["pages"][0]
        assert "text_objects" in page  # native schema unchanged when structured=False

    def test_structured_dataframe(self, sample_pdf):
        import polars as pl

        from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine

        df = PDFReaderPdfiumEngine(sample_pdf, structured=True).to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert "type" in df.columns and "content" in df.columns  # unified flatten columns


class TestPDFWriterPdfiumStructured:
    def test_structured_write_json_unified(self, sample_pdf, tmp_path):
        import json

        from datagrunt.core.pdf_io.engines import PDFWriterPdfiumEngine

        out = tmp_path / "doc.json"
        PDFWriterPdfiumEngine(sample_pdf, structured=True).write_json(export_filename=str(out))
        data = json.loads(out.read_text())
        assert "total_pages" in data["document"]
        assert "elements" in data["document"]["pages"][0]

    def test_structured_extract_images(self, sample_pdf, tmp_path):
        import os

        from datagrunt.core.pdf_io.engines import PDFWriterPdfiumEngine

        paths = PDFWriterPdfiumEngine(sample_pdf, structured=True).extract_images(output_dir=str(tmp_path))
        assert len(paths) >= 1
        assert all(os.path.isfile(p) for p in paths)
