"""Tests for extraction dataclasses."""

from datagrunt.core.pdf_io.extraction import shapes


def test_bbox_to_dict():
    b = shapes.BBox(x=1.0, y=2.0, w=3.0, h=4.0)
    assert b.to_dict() == {"x": 1.0, "y": 2.0, "w": 3.0, "h": 4.0}


def test_bbox_from_pdfium_bounds():
    # page 792 tall; bounds left=100 bottom=692 right=200 top=742
    b = shapes.BBox.from_pdfium_bounds(100, 692, 200, 742, 792)
    assert b.to_dict() == {"x": 100.0, "y": 50.0, "w": 100.0, "h": 50.0}


def test_text_block_fields():
    tb = shapes.TextBlock(
        text="Hi", bbox=shapes.BBox(0, 0, 1, 1), font="Arial", font_size=11.0,
        is_bold=False, is_italic=False, classification="body_text", reading_order=0,
    )
    assert tb.text == "Hi" and tb.classification == "body_text"
