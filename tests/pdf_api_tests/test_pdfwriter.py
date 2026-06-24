"""Tests for the public PDFWriter API."""

import json
import os

import pytest

from datagrunt.pdf_api.pdfwriter import PDFWriter


class TestPDFWriter:
    """Test suite for PDFWriter."""

    def test_write_json_default(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_json()
        assert os.path.basename(path) == "output.json"
        with open(path) as f:
            data = json.load(f)
        assert data["document"]["total_pages"] == 1

    def test_write_json_custom_name_and_images(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        img_dir = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        path = writer.write_json(export_filename="report.json", image_output_dir=str(img_dir))
        assert os.path.basename(path) == "report.json"
        with open(path) as f:
            data = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for page in data["document"]["pages"]
            for e in page["elements"]
            if e["type"] == "image"
        ]
        assert img_paths and all(os.path.isfile(p) for p in img_paths)

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_json_newline_delimited()
        assert os.path.basename(path) == "output.jsonl"
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        assert len(lines) >= 1

    def test_extract_images(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        paths = writer.extract_images(output_dir=str(out))
        assert paths and all(os.path.isfile(p) for p in paths)

    def test_engine_normalized(self, sample_pdf):
        writer = PDFWriter(sample_pdf, engine="Py Mu PDF")
        assert writer.engine == "pymupdf"

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
        writer = PDFWriter(pdf)
        paths = writer.extract_images(output_dir=str(tmp_path / "imgs"))
        assert len(paths) == 1

    def test_extract_images_dedupe_can_be_disabled(self, tmp_path):
        pdf = self._two_page_dupe_pdf(tmp_path)
        writer = PDFWriter(pdf)
        paths = writer.extract_images(output_dir=str(tmp_path / "imgs2"), dedupe=False)
        assert len(paths) == 2

    def test_write_json_dedupes_by_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pdf = self._two_page_dupe_pdf(tmp_path)
        writer = PDFWriter(pdf)
        jpath = writer.write_json(image_output_dir=str(tmp_path / "imgs3"))
        with open(jpath) as f:
            doc = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for pg in doc["document"]["pages"]
            for e in pg["elements"]
            if e["type"] == "image"
        ]
        assert len(img_paths) == 2 and len(set(img_paths)) == 1

    def test_write_json_threads_drop_layout_tables(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        calls = []
        original = ParsedDocument.drop_layout_tables
        monkeypatch.setattr(
            ParsedDocument,
            "drop_layout_tables",
            lambda self, *a, **k: (calls.append(True), original(self, *a, **k))[1],
        )
        PDFWriter(sample_pdf).write_json(drop_layout_tables=True)
        assert calls == [True]

    def test_write_markdown_default(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_markdown()
        assert os.path.basename(path) == "output.md"
        with open(path) as f:
            content = f.read()
        assert len(content) > 0

    def test_write_markdown_custom_name_and_images(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        img_dir = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        path = writer.write_markdown(export_filename="report.md", image_output_dir=str(img_dir))
        assert os.path.basename(path) == "report.md"
        with open(path) as f:
            content = f.read()
        assert len(content) > 0
        assert "report_page" in content

    def test_multi_format_writer_parses_pdf_once(self, sample_pdf, tmp_path, monkeypatch):
        """write_json + write_jsonl + write_markdown must share one parse."""
        monkeypatch.chdir(tmp_path)
        parse_calls = 0

        from datagrunt.core.pdf_io.engines import PDFReaderPyMuPDFEngine

        original_to_dicts = PDFReaderPyMuPDFEngine.to_dicts

        def counting_to_dicts(self, image_output_dir=None, drop_layout_tables=False):
            nonlocal parse_calls
            parse_calls += 1
            return original_to_dicts(self, image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)

        monkeypatch.setattr(PDFReaderPyMuPDFEngine, "to_dicts", counting_to_dicts)

        img_dir = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf, engine="pymupdf")
        writer.write_json(image_output_dir=str(img_dir))
        writer.write_json_newline_delimited(image_output_dir=str(img_dir))
        writer.write_markdown(image_output_dir=str(img_dir))

        assert parse_calls == 1

    def test_write_markdown_pdfium_native(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf, engine="pdfium", native=True)
        path = writer.write_markdown()
        assert os.path.basename(path) == "output.md"
        with open(path) as f:
            content = f.read()
        assert len(content) > 0

    def test_pdfwriter_caches_engine_across_calls(self, sample_pdf, tmp_path, monkeypatch):
        """PDFWriter builds its engine once and reuses it across write_* calls."""
        from datagrunt.core import PDFEngineFactory

        calls = {"n": 0}
        original = PDFEngineFactory.create_writer

        def spy(self):
            calls["n"] += 1
            return original(self)

        monkeypatch.setattr(PDFEngineFactory, "create_writer", spy)

        writer = PDFWriter(sample_pdf, engine="pymupdf")
        writer.write_json(str(tmp_path / "a.json"))
        writer.write_markdown(str(tmp_path / "b.md"))

        assert calls["n"] == 1  # engine cached under the standardized _engine name
        assert "_engine" in writer.__dict__
        assert (tmp_path / "a.json").exists() and (tmp_path / "b.md").exists()


class TestPDFWriterUTF8Encoding:
    """Issue #218: text-mode file writes must always use UTF-8, not platform default.

    Each test drives the public PDFWriter API with a pre-parsed dict so no real
    PDF or OCR is needed, and spies on ``builtins.open`` to confirm the
    ``encoding="utf-8"`` kwarg is passed.  This deterministically fails before
    the fix even on UTF-8 developer machines.
    """

    @staticmethod
    def _minimal_parsed_dict_with_unicode():
        """Return a minimal parsed-document dict whose content includes non-ASCII."""
        return {
            "document": {
                "total_pages": 1,
                "pages": [
                    {
                        "page_number": 1,
                        "elements": [
                            {
                                "id": "e1",
                                "type": "text",
                                "page": 1,
                                "x": 0,
                                "y": 0,
                                "content": "Héllo wörld — copyright © 2024",
                                "metadata": {},
                            }
                        ],
                    }
                ],
            }
        }

    def _make_writer(self):
        return PDFWriter(self._minimal_parsed_dict_with_unicode())

    @staticmethod
    def _spy_builtins_open(monkeypatch, watch_paths):
        """Patch ``builtins.open`` to record the ``encoding`` kwarg for watched paths.

        Returns a list that is populated with one encoding value per write-mode
        ``open()`` call whose ``file`` argument is in *watch_paths*.  Using
        ``builtins.open`` is required because pdfwriter.py uses the builtin
        directly (no explicit ``import open``).
        """
        import builtins

        real_open = builtins.open
        recorded = []

        def spy_open(file, mode="r", **kwargs):
            if "w" in str(mode) and str(file) in {str(p) for p in watch_paths}:
                recorded.append(kwargs.get("encoding"))
            return real_open(file, mode, **kwargs)

        monkeypatch.setattr(builtins, "open", spy_open)
        return recorded

    def test_write_markdown_uses_utf8(self, tmp_path, monkeypatch):
        """write_markdown must pass encoding='utf-8' to open()."""
        out = str(tmp_path / "out.md")
        recorded = self._spy_builtins_open(monkeypatch, [out])
        self._make_writer().write_markdown(export_filename=out)

        assert recorded, "write_markdown did not call open() in write mode"
        assert all(enc == "utf-8" for enc in recorded), (
            f"expected all write-mode open() calls to use encoding='utf-8', got {recorded}"
        )

    def test_write_json_uses_utf8(self, tmp_path, monkeypatch):
        """write_json must pass encoding='utf-8' to open()."""
        out = str(tmp_path / "out.json")
        recorded = self._spy_builtins_open(monkeypatch, [out])
        self._make_writer().write_json(export_filename=out)

        assert recorded, "write_json did not call open() in write mode"
        assert all(enc == "utf-8" for enc in recorded), f"expected encoding='utf-8', got {recorded}"

    def test_write_jsonl_uses_utf8(self, tmp_path, monkeypatch):
        """write_json_newline_delimited must pass encoding='utf-8' to open()."""
        out = str(tmp_path / "out.jsonl")
        recorded = self._spy_builtins_open(monkeypatch, [out])
        self._make_writer().write_json_newline_delimited(export_filename=out)

        assert recorded, "write_json_newline_delimited did not call open() in write mode"
        assert all(enc == "utf-8" for enc in recorded), f"expected encoding='utf-8', got {recorded}"

    def test_write_empty_file_uses_utf8(self, tmp_path, monkeypatch):
        """_write_empty_file must pass encoding='utf-8' to open()."""
        out = str(tmp_path / "empty.md")
        recorded = self._spy_builtins_open(monkeypatch, [out])
        PDFWriter._write_empty_file(out)

        assert recorded, "_write_empty_file did not call open() in write mode"
        assert all(enc == "utf-8" for enc in recorded), f"expected encoding='utf-8', got {recorded}"


class TestPDFWriterEmptyPdf:
    """Empty (0-byte) PDFs must not surface a raw PdfiumError.

    The reader (PDFReader) guards 0-byte files with ``is_empty`` and returns
    empty objects rather than raising; the writer must behave consistently so
    callers get an empty output file (or a clear ValueError), never a confusing
    ``pypdfium2.PdfiumError`` leaking from deep inside the engine.
    """

    def _assert_not_pdfium_error(self, callable_):
        """Run ``callable_``; fail if it raises a raw PdfiumError."""
        try:
            return callable_()
        except Exception as exc:  # noqa: BLE001 - we are asserting the *kind* of error
            module = type(exc).__module__
            assert "pdfium" not in module.lower() and "PdfiumError" not in type(exc).__name__, (
                f"empty PDF leaked a raw PdfiumError: {type(exc).__module__}.{type(exc).__name__}: {exc}"
            )
            raise

    def test_write_json_empty_pdf(self, empty_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(empty_pdf)
        path = self._assert_not_pdfium_error(lambda: writer.write_json("out.json"))
        assert os.path.isfile(path)
        with open(path) as f:
            assert json.load(f) == {}

    def test_write_json_newline_delimited_empty_pdf(self, empty_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(empty_pdf)
        path = self._assert_not_pdfium_error(lambda: writer.write_json_newline_delimited("out.jsonl"))
        assert os.path.isfile(path)
        with open(path) as f:
            assert [line for line in f if line.strip()] == []

    def test_write_markdown_empty_pdf(self, empty_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(empty_pdf)
        path = self._assert_not_pdfium_error(lambda: writer.write_markdown("out.md"))
        assert os.path.isfile(path)

    def test_extract_images_empty_pdf(self, empty_pdf, tmp_path):
        writer = PDFWriter(empty_pdf)
        paths = self._assert_not_pdfium_error(lambda: writer.extract_images(output_dir=str(tmp_path / "imgs")))
        assert paths == []


class TestPDFWriterJsonAndDictInputs:
    """Tests for initializing PDFWriter with JSON files or dictionaries."""

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
                            {
                                "id": "el1",
                                "type": "header",
                                "content": "Header Text",
                                "position": {"x": 10, "y": 10, "w": 80, "h": 10},
                            },
                            {
                                "id": "el2",
                                "type": "body_text",
                                "content": "Hello body",
                                "position": {"x": 10, "y": 30, "w": 80, "h": 10},
                            },
                            {
                                "id": "el3",
                                "type": "image",
                                "content": None,
                                "position": {"x": 10, "y": 50, "w": 80, "h": 10},
                                "metadata": {"file_path": "/dummy/img.png"},
                            },
                        ],
                    }
                ],
            }
        }

    def test_writer_with_dict(self, dummy_structured_dict, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(dummy_structured_dict)
        assert writer._parsed_dict == dummy_structured_dict
        assert writer.total_pages == 1

        # Test write_json
        jpath = writer.write_json("test_out.json")
        assert os.path.exists(jpath)
        with open(jpath) as f:
            data = json.load(f)
        assert data["document"]["source"] == "dummy.pdf"

        # Test write_json_newline_delimited
        jlpath = writer.write_json_newline_delimited("test_out.jsonl")
        assert os.path.exists(jlpath)
        with open(jlpath) as f:
            lines = f.readlines()
        assert len(lines) == 3

        # Test write_markdown
        mdpath = writer.write_markdown("test_out.md")
        assert os.path.exists(mdpath)
        with open(mdpath) as f:
            content = f.read()
        assert "# Header Text" in content
        assert "Hello body" in content
        assert "![img.png]" in content
        assert "dummy/img.png" in content

        # Test extract_images
        img_paths = writer.extract_images()
        assert img_paths == ["/dummy/img.png"]

    def test_writer_with_json_file(self, tmp_path, dummy_structured_dict, monkeypatch):
        monkeypatch.chdir(tmp_path)
        json_file = tmp_path / "doc.json"
        with open(json_file, "w") as f:
            json.dump(dummy_structured_dict, f)

        writer = PDFWriter(str(json_file))
        assert writer._parsed_dict == dummy_structured_dict
        assert writer.total_pages == 1

        # Test write_markdown
        mdpath = writer.write_markdown("test_out_json.md")
        assert os.path.exists(mdpath)
        with open(mdpath) as f:
            content = f.read()
        assert "# Header Text" in content
        assert "Hello body" in content

    @pytest.fixture
    def dummy_native_dict(self):
        """A native-schema (non-"elements") document, exercising is_structured=False."""
        return {
            "document": {
                "source": "dummy.pdf",
                "total_pages": 1,
                "pages": [
                    {
                        "page_number": 1,
                        "text": "Hello native body",
                        "text_objects": [
                            {
                                "text": "Hello native body",
                                "font_size": 12,
                                "position": {"x": 1, "y": 1, "w": 1, "h": 1},
                                "bbox": [0, 0, 1, 1],
                            }
                        ],
                        "images": [],
                    }
                ],
            }
        }

    def test_writer_with_native_dict(self, dummy_native_dict, tmp_path, monkeypatch):
        """Native-schema dict must flatten/render via the non-structured branch."""
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(dummy_native_dict)

        jlpath = writer.write_json_newline_delimited("native_out.jsonl")
        with open(jlpath) as f:
            lines = [json.loads(line) for line in f]
        assert len(lines) == 1
        assert lines[0]["type"] == "text"
        assert lines[0]["text"] == "Hello native body"

        mdpath = writer.write_markdown("native_out.md")
        with open(mdpath) as f:
            assert "Hello native body" in f.read()


class TestPDFWriterMinImageDimension:
    """Tests for the keyword-only min_image_dimension parameter on PDFWriter."""

    def test_writer_min_image_dimension_keeps_small_image(self, small_image_pdf, tmp_path):
        out = tmp_path / "out.json"
        images_dir = tmp_path / "imgs"
        PDFWriter(small_image_pdf, min_image_dimension=10).write_json(
            export_filename=str(out), image_output_dir=str(images_dir)
        )
        assert out.exists()
        # the 20px image is written because the threshold was lowered
        assert any(images_dir.glob("*")) if images_dir.exists() else False
