"""Tests for PDF component assembly."""


from datagrunt.core.pdf_io import pdfcomponents


class TestParsePage:
    """Test suite for parse_page."""

    def test_assembles_elements(self, sample_pdf):
        page = pdfcomponents.parse_page(sample_pdf, 0)
        assert page["page_number"] == 1
        assert page["classification"] in {"text_only", "scanned", "mixed"}
        types = {e["type"] for e in page["elements"]}
        assert "header" in types
        assert "image" in types
        # Every element has the unified schema keys.
        for e in page["elements"]:
            assert set(e) >= {"id", "type", "content", "page", "position",
                              "confidence", "metadata"}
            assert e["id"].startswith("elem_01_")


class TestParseDocument:
    """Test suite for parse_document."""

    def test_combined_structure(self, sample_pdf):
        doc = pdfcomponents.parse_document(sample_pdf, total_pages=1)
        assert "document" in doc
        d = doc["document"]
        assert d["total_pages"] == 1
        assert d["pipeline_type"] == "pure_python_local_v1"
        assert d["processing_id"].startswith("proc_py_")
        assert len(d["pages"]) == 1


class TestFlatten:
    """Test suite for flatten_document_elements."""

    def test_flattens_to_records(self, sample_pdf):
        doc = pdfcomponents.parse_document(sample_pdf, total_pages=1)
        records = pdfcomponents.flatten_document_elements(doc)
        assert len(records) >= 2
        rec = records[0]
        # Scalar position columns + JSON-encoded complex fields.
        assert {"id", "type", "page", "x", "y", "w", "h", "confidence",
                "content", "metadata"} <= set(rec)
        assert isinstance(rec["x"], float)
        assert isinstance(rec["metadata"], str)  # JSON-encoded


class TestPDFComponents:
    """Test suite for the PDFComponents base class."""

    def test_total_pages(self, sample_pdf):
        comp = pdfcomponents.PDFComponents(sample_pdf)
        assert comp.is_pdf
        assert comp.total_pages == 1
