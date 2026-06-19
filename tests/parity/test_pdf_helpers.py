"""Unit tests for PDF parity helper functions."""

from pdf_helpers import (
    ELEMENT_SCHEMA_KEYS,
    collect_schema_violations,
    elements_by_type,
    table_cells,
    text_chars,
    text_within_floor,
)


def test_elements_by_type_counts_each_type():
    doc = {
        "document": {
            "pages": [
                {
                    "elements": [
                        {"type": "text", "content": "a"},
                        {"type": "image", "content": None},
                        {"type": "text", "content": "b"},
                    ]
                }
            ]
        }
    }
    assert elements_by_type(doc) == {"text": 2, "image": 1}


def test_table_cells_collects_only_tables():
    doc = {
        "document": {
            "pages": [
                {
                    "elements": [
                        {"type": "text", "content": "skip"},
                        {"type": "table", "content": [["a", "b"]]},
                    ]
                }
            ]
        }
    }
    assert table_cells(doc) == [[["a", "b"]]]


def test_text_chars_sums_string_content_only():
    doc = {
        "document": {
            "pages": [
                {
                    "elements": [
                        {"type": "text", "content": "hello"},
                        {"type": "image", "content": None},
                        {"type": "text", "content": "world"},
                    ]
                }
            ]
        }
    }
    assert text_chars(doc) == 10


def test_collect_schema_violations_flags_missing_keys():
    doc = {
        "document": {
            "pages": [
                {
                    "page_number": 1,
                    "elements": [{"type": "text", "content": "only partial schema"}],
                }
            ]
        }
    }
    violations = collect_schema_violations(doc)
    assert len(violations) == 1
    for key in ELEMENT_SCHEMA_KEYS - {"type", "content"}:
        assert key in violations[0]


def test_collect_schema_violations_empty_for_valid_element():
    doc = {
        "document": {
            "pages": [
                {
                    "page_number": 1,
                    "elements": [
                        {
                            "id": "elem_01_001",
                            "type": "text",
                            "content": "ok",
                            "position": {"x": 0, "y": 0, "w": 1, "h": 1},
                            "metadata": {},
                        }
                    ],
                }
            ]
        }
    }
    assert collect_schema_violations(doc) == []


def test_text_within_floor_accepts_similar_volumes():
    assert text_within_floor(100, 120) is True
    assert text_within_floor(10, 100) is False
