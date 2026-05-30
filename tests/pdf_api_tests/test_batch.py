"""Tests for document-level batch PDF processing."""

import json
import os

import pymupdf

from datagrunt.pdf_api.batch import process_pdfs


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


class TestProcessPdfs:
    """Test suite for process_pdfs."""

    def test_writes_one_json_per_document(self, tmp_path):
        pdf_a = _make_pdf(tmp_path / "alpha.pdf")
        pdf_b = _make_pdf(tmp_path / "beta.pdf")
        out = tmp_path / "out"

        results = process_pdfs([pdf_a, pdf_b], str(out))

        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)
        assert {os.path.basename(r["json_path"]) for r in results} == {
            "alpha.json",
            "beta.json",
        }
        for r in results:
            assert os.path.isfile(r["json_path"])
            with open(r["json_path"]) as f:
                doc = json.load(f)
            assert doc["document"]["total_pages"] == 1

    def test_images_written_by_default(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        process_pdfs([pdf], str(out))

        img_dir = out / "doc_images"
        assert img_dir.is_dir()
        assert len(os.listdir(img_dir)) >= 1

    def test_images_disabled(self, tmp_path):
        pdf = _make_pdf(tmp_path / "doc.pdf")
        out = tmp_path / "out"

        process_pdfs([pdf], str(out), images=False)

        assert not (out / "doc_images").exists()

    def test_bad_path_isolated_from_good_ones(self, tmp_path):
        good = _make_pdf(tmp_path / "good.pdf")
        bad = str(tmp_path / "nope.pdf")  # does not exist
        out = tmp_path / "out"

        results = process_pdfs([bad, good], str(out))

        by_source = {r["source"]: r for r in results}
        assert by_source[bad]["status"] == "error"
        assert "error" in by_source[bad]
        assert by_source[good]["status"] == "success"
        assert os.path.isfile(by_source[good]["json_path"])
