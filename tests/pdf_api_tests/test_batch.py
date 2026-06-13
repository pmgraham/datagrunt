"""Tests for document-level batch PDF processing."""

import json
import os

import pymupdf
import pytest

from datagrunt.pdf_api.batch import PDFBatchWriter


def _make_pdf(path, text="Hello batch"):
    """Write a tiny one-page PDF with text and one embedded image."""
    doc = pymupdf.open()
    page = doc.new_page(width=300, height=300)
    page.insert_text((50, 50), text, fontsize=18)
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 80, 80))
    pix.set_rect(pix.irect, (10, 200, 100))
    page.insert_image(pymupdf.Rect(50, 100, 130, 180), stream=pix.tobytes("png"))
    doc.save(str(path))
    doc.close()
    return str(path)


class TestPDFBatchWriter:
    """Test suite for PDFBatchWriter.process."""

    def test_writes_one_json_per_document(self, tmp_path):
        pdf_a = _make_pdf(tmp_path / "alpha.pdf")
        pdf_b = _make_pdf(tmp_path / "beta.pdf")
        out = tmp_path / "out"

        results = PDFBatchWriter().process([pdf_a, pdf_b], str(out))

        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)
        # JSON lands in its own json/ subdirectory, one file per document.
        assert {os.path.basename(r["json_path"]) for r in results} == {
            "alpha.json",
            "beta.json",
        }
        for r in results:
            assert os.path.dirname(r["json_path"]) == str(out / "json")
            assert os.path.isfile(r["json_path"])
            with open(r["json_path"]) as f:
                doc = json.load(f)
            assert doc["document"]["total_pages"] == 1

    def test_text_and_images_go_to_separate_directories(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        (result,) = PDFBatchWriter(markdown=True, jsonl=True).process([pdf], str(out))

        assert result["json_path"] == str(out / "json" / "doc.json")
        assert result["jsonl_path"] == str(out / "jsonl" / "doc.jsonl")
        assert result["markdown_path"] == str(out / "markdown" / "doc.md")
        assert result["images_dir"] == str(out / "images" / "doc")
        for path in (result["json_path"], result["jsonl_path"], result["markdown_path"]):
            assert os.path.isfile(path)
        # Images live under images/<stem>/, not alongside the text outputs.
        assert os.path.isdir(result["images_dir"])
        assert len(os.listdir(result["images_dir"])) >= 1

    def test_markdown_written_only_when_enabled(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        (result,) = PDFBatchWriter().process([pdf], str(out))

        assert "markdown_path" not in result
        assert not (out / "markdown").exists()

    def test_images_disabled(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        (result,) = PDFBatchWriter(images=False).process([pdf], str(out))

        assert "images_dir" not in result
        assert not (out / "images").exists()

    def test_images_only_when_no_text_format(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        (result,) = PDFBatchWriter(json=False, images=True).process([pdf], str(out))

        assert "json_path" not in result
        assert not (out / "json").exists()
        assert os.path.isdir(result["images_dir"])
        assert len(os.listdir(result["images_dir"])) >= 1

    def test_bad_path_isolated_from_good_ones(self, tmp_path):
        good = _make_pdf(tmp_path / "good.pdf")
        bad = str(tmp_path / "nope.pdf")  # does not exist
        out = tmp_path / "out"

        results = PDFBatchWriter().process([bad, good], str(out))

        by_source = {r["source"]: r for r in results}
        assert by_source[bad]["status"] == "error"
        assert "error" in by_source[bad]
        assert by_source[good]["status"] == "success"
        assert os.path.isfile(by_source[good]["json_path"])

    def test_results_preserve_input_order(self, tmp_path):
        pdfs = [_make_pdf(tmp_path / f"doc{i}.pdf") for i in range(3)]
        out = tmp_path / "out"
        results = PDFBatchWriter().process(pdfs, str(out))
        assert [r["source"] for r in results] == pdfs

    def test_empty_input_returns_empty_list(self, tmp_path):
        out = tmp_path / "out"
        assert PDFBatchWriter().process([], str(out)) == []

    def test_no_output_enabled_raises(self):
        with pytest.raises(ValueError, match="nothing to write"):
            PDFBatchWriter(json=False, jsonl=False, markdown=False, images=False)


def _make_two_page_dupe_image_pdf(path):
    """Two pages, the same image on each -> a byte-duplicate pair."""
    doc = pymupdf.open()
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 90, 90))
    pix.set_rect(pix.irect, (200, 30, 30))
    img = pix.tobytes("png")
    for _ in range(2):
        page = doc.new_page(width=300, height=300)
        page.insert_image(pymupdf.Rect(40, 40, 130, 130), stream=img)
    doc.save(str(path))
    doc.close()
    return str(path)


class TestPDFBatchWriterFlags:
    """Verify per-document option flags reach the written output."""

    def test_dedupe_images_default_collapses_duplicates(self, tmp_path):
        pdf = _make_two_page_dupe_image_pdf(tmp_path / "dup.pdf")
        out = tmp_path / "out"

        PDFBatchWriter().process([pdf], str(out))

        # Two identical images across pages collapse to a single file on disk.
        files = os.listdir(out / "images" / "dup")
        assert len(files) == 1

    def test_dedupe_images_disabled_keeps_both(self, tmp_path):
        pdf = _make_two_page_dupe_image_pdf(tmp_path / "dup.pdf")
        out = tmp_path / "out"

        PDFBatchWriter(dedupe_images=False).process([pdf], str(out))

        files = os.listdir(out / "images" / "dup")
        assert len(files) == 2

    def test_drop_layout_tables_flag_reaches_output(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        results = PDFBatchWriter(drop_layout_tables=True).process([pdf], str(out))

        with open(results[0]["json_path"]) as f:
            doc = json.load(f)
        tables = [
            e
            for pg in doc["document"]["pages"]
            for e in pg["elements"]
            if e["type"] == "table"
        ]
        # With the filter on, no surviving table may be 1xN or Nx1.
        assert all(t["metadata"]["rows"] >= 2 and t["metadata"]["columns"] >= 2 for t in tables)


class TestBatchExports:
    """PDFBatchWriter should be importable from the package roots."""

    def test_pdf_api_export(self):
        from datagrunt.pdf_api import PDFBatchWriter as p

        assert p is PDFBatchWriter

    def test_top_level_export(self):
        from datagrunt import PDFBatchWriter as p

        assert p is PDFBatchWriter
