"""Tests for the public PDFReader API."""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.pdf_api.pdfreader import PDFReader


class TestPDFReader:
    """Test suite for PDFReader."""

    def test_init_normalizes_engine(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="Py Mu PDF")
        assert reader.engine == "pymupdf"
        assert reader.is_pdf

    def test_to_dicts(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_get_sample(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        page = reader.get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 2

    def test_to_arrow_table(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_empty_pdf_returns_empty_objects(self, empty_pdf):
        reader = PDFReader(empty_pdf)
        assert reader.to_dicts() == {}
        assert reader.to_dataframe().is_empty()

    def test_invalid_engine_raises(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="ghostscript")
        with pytest.raises(ValueError):
            reader.to_dicts()


class TestDropLayoutTablesPassthrough:
    """Verify PDFReader threads drop_layout_tables to the filter."""

    def _spy(self, monkeypatch):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        calls = []
        original = ParsedDocument.drop_layout_tables
        monkeypatch.setattr(
            ParsedDocument,
            "drop_layout_tables",
            lambda self, *a, **k: (calls.append(True), original(self, *a, **k))[1],
        )
        return calls

    def test_to_dicts_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_dicts(drop_layout_tables=True)
        assert calls == [True]

    def test_to_dataframe_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_dataframe(drop_layout_tables=True)
        assert calls == [True]

    def test_to_arrow_table_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_arrow_table(drop_layout_tables=True)
        assert calls == [True]

    def test_default_off(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_dicts()
        assert calls == []


class TestTopLevelExports:
    """Verify PDF classes are importable from the package root."""

    def test_top_level_imports(self):
        from datagrunt import PDFReader, PDFWriter
        assert PDFReader.__name__ == "PDFReader"
        assert PDFWriter.__name__ == "PDFWriter"


class TestPDFReaderPdfiumEngine:
    """End-to-end PDFReader tests using the pdfium engine."""

    def test_reader_pdfium_to_dicts(self, sample_pdf):
        doc = PDFReader(sample_pdf, engine="pdfium", native=True).to_dicts()
        page = doc["document"]["pages"][0]
        assert "text" in page
        assert "text_objects" in page

    def test_reader_pdfium_normalized_engine_name(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="PDF ium")
        assert reader.engine == "pdfium"


class TestPDFReaderStructured:
    def test_reader_structured_unified_schema(self, sample_pdf):
        from datagrunt import PDFReader

        doc = PDFReader(sample_pdf, engine="pdfium").to_dicts()
        page = doc["document"]["pages"][0]
        assert "elements" in page and "classification" in page


class TestPDFReaderMultiprocessing:
    """Test multiprocessing execution and Spark/Beam environment fallback in PDFReader."""

    def test_multiprocessing_structured_success(self, sample_pdf):
        # Run structured parsing with workers=2
        reader = PDFReader(sample_pdf, engine="pdfium", workers=2, native=False)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        page = doc["document"]["pages"][0]
        assert "elements" in page
        assert page["page_number"] == 1

    def test_multiprocessing_native_success(self, sample_pdf):
        # Run native parsing with workers=2
        reader = PDFReader(sample_pdf, engine="pdfium", workers=2, native=True)
        doc = reader.to_dicts()
        assert doc["document"]["page_count"] == 1
        page = doc["document"]["pages"][0]
        assert "text_objects" in page
        assert page["page_number"] == 1

    def test_spark_env_fallback_to_sequential_structured(self, sample_pdf, monkeypatch):
        from concurrent.futures import ProcessPoolExecutor
        calls = []
        original_init = ProcessPoolExecutor.__init__

        def mocked_init(self, *args, **kwargs):
            calls.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(ProcessPoolExecutor, "__init__", mocked_init)
        monkeypatch.setenv("SPARK_ENV_LOADED", "1")

        # Run structured parsing; should NOT invoke ProcessPoolExecutor
        reader = PDFReader(sample_pdf, engine="pdfium", workers=2, native=False)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(calls) == 0

    def test_beam_env_fallback_to_sequential_native(self, sample_pdf, monkeypatch):
        from concurrent.futures import ProcessPoolExecutor
        calls = []
        original_init = ProcessPoolExecutor.__init__

        def mocked_init(self, *args, **kwargs):
            calls.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(ProcessPoolExecutor, "__init__", mocked_init)
        monkeypatch.setenv("BEAM_WORKER_ID", "worker_123")

        # Run native parsing; should NOT invoke ProcessPoolExecutor
        reader = PDFReader(sample_pdf, engine="pdfium", workers=2, native=True)
        doc = reader.to_dicts()
        assert doc["document"]["page_count"] == 1
        assert len(calls) == 0

    def test_no_multiprocessing_when_workers_is_one(self, sample_pdf, monkeypatch):
        from concurrent.futures import ProcessPoolExecutor
        calls = []
        original_init = ProcessPoolExecutor.__init__

        def mocked_init(self, *args, **kwargs):
            calls.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(ProcessPoolExecutor, "__init__", mocked_init)

        # Run structured parsing with workers=1; should NOT invoke ProcessPoolExecutor
        reader = PDFReader(sample_pdf, engine="pdfium", workers=1, native=False)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(calls) == 0

    def test_no_multiprocessing_when_single_page(self, sample_pdf, monkeypatch):
        from concurrent.futures import ProcessPoolExecutor
        calls = []
        original_init = ProcessPoolExecutor.__init__

        def mocked_init(self, *args, **kwargs):
            calls.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(ProcessPoolExecutor, "__init__", mocked_init)

        # Run structured parsing with workers=2 on 1-page PDF; should NOT invoke ProcessPoolExecutor
        reader = PDFReader(sample_pdf, engine="pdfium", workers=2, native=False)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(calls) == 0

    def test_multiprocessing_when_multi_page(self, multipage_pdf, monkeypatch):
        from concurrent.futures import ProcessPoolExecutor
        calls = []
        original_init = ProcessPoolExecutor.__init__

        def mocked_init(self, *args, **kwargs):
            calls.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(ProcessPoolExecutor, "__init__", mocked_init)

        # Run structured parsing with workers=2 on 3-page PDF; should invoke ProcessPoolExecutor
        reader = PDFReader(multipage_pdf, engine="pdfium", workers=2, native=False)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 3
        assert len(calls) > 0


class TestPDFReaderPyMuPDFSequential:
    """PyMuPDF parses sequentially because MuPDF is not thread-safe.

    Regression guard (not red-green): a thread-safety race is non-deterministic,
    so we cannot reliably reproduce a crash. We instead pin that parsing a
    multi-page PDF with the public API and workers > 1 still returns every page
    in the correct order with the expected content -- the ``workers`` setting is
    accepted (API compatibility) but does not enable threading.
    """

    @staticmethod
    def _four_page_pdf(tmp_path):
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

    def test_to_dicts_all_pages_present_and_ordered(self, tmp_path):
        pdf = self._four_page_pdf(tmp_path)
        doc = PDFReader(pdf, engine="pymupdf", workers=4).to_dicts()

        pages = doc["document"]["pages"]
        assert doc["document"]["total_pages"] == 4
        assert [p["page_number"] for p in pages] == [1, 2, 3, 4]
        for n, page in enumerate(pages, start=1):
            text = " ".join(
                e.get("content", "") for e in page["elements"] if e.get("type") in ("header", "body_text")
            )
            assert f"Sequential Marker {n}" in text


class TestPDFReaderEngineCache:
    """Engine lifecycle: PDFReader builds its engine once per instance."""

    def test_pdfreader_caches_engine_across_calls(self, sample_pdf, monkeypatch):
        """PDFReader builds its engine once and reuses it across calls (one parse)."""
        from datagrunt.core import PDFEngineFactory

        calls = {"n": 0}
        original = PDFEngineFactory.create_reader

        def spy(self):
            calls["n"] += 1
            return original(self)

        monkeypatch.setattr(PDFEngineFactory, "create_reader", spy)

        reader = PDFReader(sample_pdf, engine="pymupdf")
        reader.to_dataframe()
        reader.to_arrow_table()
        reader.to_dataframe()

        assert calls["n"] == 1  # engine cached: one create_reader, not three
        assert "_engine" in reader.__dict__

    def test_engine_backed_requires_engine_role(self, sample_pdf):
        """The shared base raises if a subclass omits _engine_role (no silent fallback)."""
        from datagrunt.pdf_api._engine_backed import _PDFEngineBacked

        obj = _PDFEngineBacked(sample_pdf)  # _engine_role defaults to None
        with pytest.raises(NotImplementedError):
            _ = obj._engine


class TestPDFReaderJsonAndDictInputs:
    """Tests for initializing PDFReader with JSON files or dictionaries."""

    @pytest.fixture
    def dummy_structured_dict(self):
        return {
            "document": {
                "source": "dummy.pdf",
                "total_pages": 1,
                "pages": [
                    {
                        "page_number": 1,
                        "width": 100.0,
                        "height": 100.0,
                        "elements": [
                            {"id": "el1", "type": "header", "content": "Header Text", "position": {"x": 10, "y": 10, "w": 80, "h": 10}},
                            {"id": "el2", "type": "body_text", "content": "Hello body", "position": {"x": 10, "y": 30, "w": 80, "h": 10}},
                        ]
                    }
                ]
            }
        }

    def test_reader_with_dict(self, dummy_structured_dict):
        reader = PDFReader(dummy_structured_dict)
        assert reader._parsed_dict == dummy_structured_dict
        assert reader.total_pages == 1
        
        # Test to_dicts
        assert reader.to_dicts() == dummy_structured_dict
        
        # Test get_sample
        sample = reader.get_sample()
        assert sample["page_number"] == 1
        assert "elements" in sample

        # Test to_dataframe
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert list(df["content"]) == ["Header Text", "Hello body"]

        # Test to_arrow_table
        table = reader.to_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2

    def test_reader_with_json_file(self, tmp_path, dummy_structured_dict):
        import json
        json_file = tmp_path / "doc.json"
        with open(json_file, "w") as f:
            json.dump(dummy_structured_dict, f)

        reader = PDFReader(str(json_file))
        assert reader._parsed_dict == dummy_structured_dict
        assert reader.total_pages == 1
        assert reader.to_dicts() == dummy_structured_dict

        # Test to_dataframe
        df = reader.to_dataframe()
        assert df.height == 2
        assert list(df["content"]) == ["Header Text", "Hello body"]


