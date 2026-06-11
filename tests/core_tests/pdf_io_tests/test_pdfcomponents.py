"""Tests for PDF component assembly."""


from datagrunt.core.pdf_io import pdfcomponents


class TestParsePage:
    """Test suite for DocumentAssembler.parse_page."""

    def test_assembles_elements(self, sample_pdf):
        page = pdfcomponents.DocumentAssembler(sample_pdf).parse_page(0)
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
    """Test suite for DocumentAssembler.parse_document."""

    def test_combined_structure(self, sample_pdf):
        doc = pdfcomponents.DocumentAssembler(sample_pdf).parse_document(1)
        assert "document" in doc
        d = doc["document"]
        assert d["total_pages"] == 1
        assert d["pipeline_type"] == "pure_python_local_v1"
        assert d["processing_id"].startswith("proc_py_")
        assert len(d["pages"]) == 1


class TestFlatten:
    """Test suite for ParsedDocument.flatten."""

    def test_flattens_to_records(self, sample_pdf):
        doc = pdfcomponents.DocumentAssembler(sample_pdf).parse_document(1)
        records = pdfcomponents.ParsedDocument(doc).flatten()
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


class TestDedupeImages:
    """Test suite for ParsedDocument.dedupe_images."""

    @staticmethod
    def _img_element(file_path):
        return {
            "id": "elem",
            "type": "image",
            "content": None,
            "page": 1,
            "position": {"x": 0, "y": 0, "w": 0, "h": 0},
            "confidence": 1.0,
            "metadata": {
                "file_path": file_path,
                "format": "png",
                "width_px": 100,
                "height_px": 100,
            },
        }

    def test_dedupes_identical_images(self, tmp_path):
        a = tmp_path / "a.png"
        a.write_bytes(b"IMG-DATA-1")
        b = tmp_path / "b.png"
        b.write_bytes(b"IMG-DATA-1")  # byte-identical to a
        c = tmp_path / "c.png"
        c.write_bytes(b"IMG-DATA-2")  # unique
        document = {
            "document": {
                "pages": [
                    {
                        "page_number": 1,
                        "elements": [
                            self._img_element(str(a)),
                            self._img_element(str(b)),
                            self._img_element(str(c)),
                        ],
                    }
                ]
            }
        }

        removed = pdfcomponents.ParsedDocument(document).dedupe_images()

        assert removed == 1
        # The redundant duplicate file is deleted; the first + unique remain.
        assert a.exists()
        assert not b.exists()
        assert c.exists()
        # The duplicate element is repointed at the first occurrence.
        elems = document["document"]["pages"][0]["elements"]
        assert elems[1]["metadata"]["file_path"] == str(a)
        assert elems[0]["metadata"]["file_path"] == str(a)
        assert elems[2]["metadata"]["file_path"] == str(c)

    def test_skips_none_and_missing_paths(self, tmp_path):
        a = tmp_path / "a.png"
        a.write_bytes(b"ONLY")
        document = {
            "document": {
                "pages": [
                    {
                        "page_number": 1,
                        "elements": [
                            self._img_element(None),
                            self._img_element(str(tmp_path / "gone.png")),
                            self._img_element(str(a)),
                        ],
                    }
                ]
            }
        }

        removed = pdfcomponents.ParsedDocument(document).dedupe_images()

        assert removed == 0
        assert a.exists()


class TestDropLayoutTables:
    """Test suite for ParsedDocument.drop_layout_tables."""

    @staticmethod
    def _table(rows, cols):
        return {
            "id": "t",
            "type": "table",
            "content": [[None] * cols] * rows,
            "page": 1,
            "position": {"x": 0, "y": 0, "w": 10, "h": 10},
            "confidence": 1.0,
            "metadata": {"rows": rows, "columns": cols, "has_header_row": False},
        }

    @staticmethod
    def _header():
        return {
            "id": "h",
            "type": "header",
            "content": "Title",
            "page": 1,
            "position": {"x": 0, "y": 0, "w": 10, "h": 10},
            "confidence": 1.0,
            "metadata": {},
        }

    def test_drops_one_dimensional_tables_keeps_real_ones(self):
        document = {
            "document": {
                "pages": [
                    {
                        "page_number": 1,
                        "elements": [
                            self._header(),
                            self._table(2, 2),  # real table -> keep
                            self._table(1, 3),  # single row -> drop
                            self._table(3, 1),  # single column -> drop
                            self._table(2, 1),  # single column -> drop
                        ],
                    }
                ]
            }
        }

        removed = pdfcomponents.ParsedDocument(document).drop_layout_tables()

        assert removed == 3
        kept = document["document"]["pages"][0]["elements"]
        # header preserved, only the 2x2 table remains among tables
        assert [e["type"] for e in kept] == ["header", "table"]
        tbl = next(e for e in kept if e["type"] == "table")
        assert (tbl["metadata"]["rows"], tbl["metadata"]["columns"]) == (2, 2)

    def test_non_table_elements_never_dropped(self):
        document = {
            "document": {"pages": [{"page_number": 1, "elements": [self._header()]}]}
        }
        removed = pdfcomponents.ParsedDocument(document).drop_layout_tables()
        assert removed == 0
        assert len(document["document"]["pages"][0]["elements"]) == 1


class TestParsePageBackend:
    """DocumentAssembler.parse_page consumes an ExtractionBackend + table extractor."""

    def test_default_backend_pymupdf(self, sample_pdf):
        from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

        page = DocumentAssembler(sample_pdf).parse_page(0)
        assert page["page_number"] == 1
        assert "elements" in page
        types = {el["type"] for el in page["elements"]}
        assert types & {"header", "subheader", "body_text", "caption"}

    def test_explicit_pdfium_backend(self, sample_pdf):
        from datagrunt.core.pdf_io.extraction import PdfiumBackend
        from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

        page = DocumentAssembler(sample_pdf, backend=PdfiumBackend(sample_pdf)).parse_page(0)
        assert set(page.keys()) == {"page_number", "width", "height", "classification", "elements"}
        types = {el["type"] for el in page["elements"]}
        assert types & {"header", "subheader", "body_text", "caption"}


class TestDocumentAssembler:
    """DocumentAssembler builds the unified-schema document via a backend."""

    def test_parse_page(self, sample_pdf):
        from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

        page = DocumentAssembler(sample_pdf).parse_page(0)
        assert set(page.keys()) == {"page_number", "width", "height", "classification", "elements"}
        assert page["page_number"] == 1
        assert {el["type"] for el in page["elements"]} & {"header", "subheader", "body_text", "caption"}

    def test_parse_document_envelope(self, sample_pdf):
        from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

        doc = DocumentAssembler(sample_pdf).parse_document(1)
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_explicit_pdfium_backend(self, sample_pdf):
        from datagrunt.core.pdf_io.extraction import PdfiumBackend
        from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

        page = DocumentAssembler(sample_pdf, backend=PdfiumBackend(sample_pdf)).parse_page(0)
        assert page["page_number"] == 1


class TestParsedDocument:
    """ParsedDocument exposes flatten/dedupe/drop operations over a document."""

    def test_flatten(self, sample_pdf):
        from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler, ParsedDocument

        doc = DocumentAssembler(sample_pdf).parse_document(1)
        records = ParsedDocument(doc).flatten()
        assert records and {"type", "page", "content"} <= set(records[0].keys())

    def test_dedupe_images_counts(self, tmp_path):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        a, b = tmp_path / "a.png", tmp_path / "b.png"
        a.write_bytes(b"X")
        b.write_bytes(b"X")
        document = {"document": {"pages": [{"elements": [
            {"type": "image", "metadata": {"file_path": str(a)}},
            {"type": "image", "metadata": {"file_path": str(b)}},
        ]}]}}
        removed = ParsedDocument(document).dedupe_images()
        assert removed == 1 and not b.exists()

    def test_drop_layout_tables(self):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        document = {"document": {"pages": [{"elements": [
            {"type": "table", "metadata": {"rows": 1, "columns": 5}},
            {"type": "table", "metadata": {"rows": 4, "columns": 3}},
            {"type": "body_text"},
        ]}]}}
        pd = ParsedDocument(document)
        assert pd.drop_layout_tables() == 1
        kept = document["document"]["pages"][0]["elements"]
        assert [e["type"] for e in kept] == ["table", "body_text"]


class TestMarkdownMetacharacterEscaping:
    """Body/caption text starting with markdown metacharacters must be escaped."""

    @staticmethod
    def _element(etype, content):
        return {"type": etype, "content": content, "metadata": {}}

    @staticmethod
    def _markdown(elements):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        document = {"document": {"pages": [{"page_number": 1, "elements": elements}]}}
        return ParsedDocument(document).to_markdown()

    def test_body_text_leading_hash_is_escaped(self):
        md = self._markdown([self._element("body_text", "# rm -rf is not a heading")])
        assert "\\# rm -rf is not a heading" in md
        # The line must not begin with a bare H1 marker.
        assert not md.lstrip().startswith("# ")

    def test_body_text_leading_dash_is_escaped(self):
        md = self._markdown([self._element("body_text", "- not a list item")])
        assert "\\- not a list item" in md

    def test_body_text_leading_blockquote_is_escaped(self):
        md = self._markdown([self._element("body_text", "> not a quote")])
        assert "\\> not a quote" in md

    def test_body_text_leading_ordered_list_is_escaped(self):
        md = self._markdown([self._element("body_text", "1. not a list")])
        assert "1\\. not a list" in md

    def test_caption_leading_metacharacter_is_escaped(self):
        md = self._markdown([self._element("caption", "# caption text")])
        assert "*\\# caption text*" in md

    def test_intentional_heading_element_still_renders(self):
        md = self._markdown([self._element("header", "Quarterly Report")])
        assert md.lstrip().startswith("# Quarterly Report")

    def test_heading_content_with_metacharacter_does_not_inject_structure(self):
        # A genuine heading element keeps its '# ' marker, but its own content
        # must not introduce a second heading level.
        md = self._markdown([self._element("header", "# extra hash")])
        assert "# \\# extra hash" in md

    def test_table_cell_escaping_unchanged(self):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        rendered = ParsedDocument._render_table([["a|b", "c"]], has_header=False)
        assert "a\\|b" in rendered
