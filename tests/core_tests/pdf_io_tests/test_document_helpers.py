"""Tests for the schema-dispatching document helpers in pdfcomponents."""

from datagrunt.core.pdf_io.pdfcomponents import (
    collect_image_paths,
    document_is_structured,
    flatten_document,
)

_STRUCTURED = {
    "document": {
        "pages": [
            {
                "page_number": 1,
                "elements": [
                    {"type": "body_text", "content": "hello", "metadata": {}},
                    {"type": "image", "content": None, "metadata": {"file_path": "/img/a.png"}},
                    {"type": "image", "content": None, "metadata": {"file_path": "/img/a.png"}},
                    {"type": "image", "content": None, "metadata": {"file_path": "/img/b.png"}},
                ],
            }
        ]
    }
}

_NATIVE = {
    "document": {
        "pages": [
            {
                "page_number": 1,
                "text": "hello native",
                "text_objects": [{"text": "hello native", "position": {}, "bbox": [0, 0, 1, 1]}],
                "images": [{"file": "/img/a.png"}, {"file": "/img/a.png"}, {"file": "/img/c.png"}],
            }
        ]
    }
}


class TestDocumentIsStructured:
    def test_structured_document(self):
        assert document_is_structured(_STRUCTURED) is True

    def test_native_document(self):
        assert document_is_structured(_NATIVE) is False

    def test_empty_document(self):
        assert document_is_structured({}) is False
        assert document_is_structured({"document": {"pages": []}}) is False


class TestFlattenDocument:
    def test_structured_flattens_via_parsed_document(self):
        records = flatten_document(_STRUCTURED)
        assert [r["type"] for r in records].count("image") == 3
        assert any(r.get("type") == "body_text" or r.get("content") == "hello" for r in records)

    def test_native_flattens_via_native_reader(self):
        records = flatten_document(_NATIVE)
        types = {r["type"] for r in records}
        assert types == {"text", "image"}

    def test_empty_document_yields_no_records(self):
        assert flatten_document({}) == []


class TestCollectImagePaths:
    def test_structured_dedupes_in_order(self):
        assert collect_image_paths(_STRUCTURED) == ["/img/a.png", "/img/b.png"]

    def test_native_dedupes_in_order(self):
        assert collect_image_paths(_NATIVE) == ["/img/a.png", "/img/c.png"]

    def test_empty_document(self):
        assert collect_image_paths({}) == []
