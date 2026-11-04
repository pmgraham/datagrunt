"""Tests for the v5 layout of hidden and recovered text (#368, A4b group 3): apart lines and where they read.

Unit tests build SpanPiece values directly: boxes are displayed, top-left
``(x0, y0, x1, y1)`` points. End-to-end tests write PDFs by hand
(``hand_written_pdf``: ReportLab cannot draw ``Tf 0``) and read them through
``extract_page``, ``recover_dropped_text``, ``assess_page`` and
``find_lattice_tables``.
"""

import dataclasses
import pickle

import pytest

from datagrunt.pdf import layout
from datagrunt.pdf.layout import (
    ApartLine,
    LatticeResult,
    PageLayout,
    StreamHeading,
    StreamParagraph,
    classify_blocks,
    find_lattice_tables,
    page_layout,
)
from datagrunt.pdf.pdfium_engine import extract_page, open_document, recover_dropped_text
from datagrunt.pdf.quality import SpanPiece, assess_page
from pdf_tests.test_layout_classify import sized
from pdf_tests.test_layout_lattice import box, chars, falling
from pdf_tests.test_layout_stream import cell
from pdf_tests.test_pdfium_recover import pdf
from pdf_tests.test_quality import raw_span

NO_TABLES = LatticeResult(tables=(), used_pieces=())
W, H = 400.0, 300.0


def hidden(text: str, x: float, y: float, reasons=("invisible",), **overrides) -> SpanPiece:
    """A hidden piece of ``text`` in 6 pt chars from ``(x, y)``, 12 pt tall, flagged with ``reasons``."""
    return dataclasses.replace(chars(text, x, y, **overrides), hidden_reasons=reasons)


def at_a_point(text: str, x: float, y: float, size: float, reasons=("tiny_font",)) -> SpanPiece:
    """``text`` whose every char box is the point ``(x, y)``: how recovery places ``Tf 0`` text, at ``size``."""
    point = (x, y, x, y)
    span = raw_span(text, (point,) * len(text), font_size_pt=size, em_width_pt=size)
    return SpanPiece(span=span, start=0, end=len(text), hidden_reasons=reasons)


def laid_out(*pieces: SpanPiece, recovered=(), width: float = W, height: float = H) -> tuple:
    """page_layout's items for ``pieces`` (and ``recovered``) on a ``width`` x ``height`` pt page, no tables."""
    found = page_layout(pieces, recovered, NO_TABLES, width_pt=width, height_pt=height)
    assert found.warnings == ()
    return found.items


def described(items) -> list[tuple]:
    """Each item as ``(kind, text, reasons)``: apart lines with their reasons, stream text with ``()``."""
    found = []
    for item in items:
        if isinstance(item, ApartLine):
            found.append(("apart", item.text, item.hidden_reasons))
        elif isinstance(item, (StreamParagraph, StreamHeading)):
            found.append((type(item).__name__, item.text, ()))
        else:
            found.append((type(item).__name__, None, ()))
    return found


# Two columns of 4 lines, 14 pt apart from y 10: each a paragraph of prose (4 words a line, so no table).
LEFT, RIGHT = "left column of prose", "right column of prose"


def column_of(text: str, x: float, count: int = 4) -> tuple[SpanPiece, ...]:
    """``count`` visible lines of ``text`` from ``x``, 14 pt apart from y 10: one paragraph."""
    return tuple(chars(text, x, 10.0 + 14.0 * index) for index in range(count))


def columns_named(items) -> list[str]:
    """Each item's text, a column paragraph's as "left" or "right"."""
    names = {" ".join([LEFT] * 4): "left", " ".join([RIGHT] * 4): "right"}
    return [names.get(text, text) for _, text, _ in described(items)]


class TestClassifyNeverDividesByZero:
    """classify_blocks reads the heading ratio as a ratio: never a division by a body size of 0 tenths."""

    def test_text_under_0_05_pt_throughout_is_paragraphs(self):
        # 0.04 pt is 0 tenths: the body size is 0, and so is every line's.
        items = classify_blocks((sized("Small print", 0.04), sized("More small print", 0.04, y=40.0)))

        assert [type(item) for item in items] == [StreamParagraph, StreamParagraph]

    def test_a_line_over_a_body_of_0_tenths_is_a_heading(self):
        items = classify_blocks((sized("Title", 10.0), sized("x" * 50, 0.04, y=40.0)))

        assert [(type(item), getattr(item, "level", None)) for item in items] == [
            (StreamHeading, 1),
            (StreamParagraph, None),
        ]


class TestApartLine:
    """ApartLine is a frozen, slotted, picklable, validated record of a hidden or recovered line."""

    def apart(self, **overrides) -> ApartLine:
        piece = hidden("SECRET", 10.0, 10.0)
        fields = {
            "cells": (dataclasses.replace(cell("SECRET"), pieces=(piece,)),),
            "bbox": box(10.0, 10.0, 46.0, 22.0),
            "turn": 0,
            "hidden_reasons": ("invisible",),
        }
        return ApartLine(**(fields | overrides))

    def test_pickle_round_trip_frozen_and_slotted(self):
        original = self.apart()

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.turn = 90
        assert not hasattr(original, "__dict__")

    def test_text_and_box(self):
        assert (self.apart().text, self.apart().bbox) == ("SECRET", box(10.0, 10.0, 46.0, 22.0))

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"cells": ()}, ValueError, "cells must hold at least one cell", id="no-cells"),
            pytest.param({"bbox": box(10.0, 10.0, 20.0, 22.0)}, ValueError, "must lie within the bbox", id="outside"),
            pytest.param({"turn": 180}, ValueError, "turn must be 0, 90 or 270", id="turn-180"),
            pytest.param({"hidden_reasons": ("tiny_font",)}, ValueError, "hidden_reasons", id="not-its-pieces"),
            pytest.param({"hidden_reasons": ("tiny_font", "off_page")}, ValueError, "canonical", id="order"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=message):
            self.apart(**overrides)


class TestApartLines:
    """Hidden and recovered pieces lay out as lines of their own: merged per line, never mixed with visible text."""

    @pytest.mark.parametrize("size", [0.0, 0.01, 0.04], ids=["0", "0.01", "0.04"])
    def test_text_at_a_tiny_size_lays_out_as_a_hidden_line(self, size):
        items = laid_out(chars("Visible body text", 10.0, 10.0), at_a_point("HIDDEN", 72.0, 100.0, size))

        assert described(items) == [
            ("StreamParagraph", "Visible body text", ()),
            ("apart", "HIDDEN", ("tiny_font",)),
        ]

    def test_hidden_text_on_a_visible_line_is_never_mixed_with_it(self):
        items = laid_out(chars("Visible", 10.0, 10.0), hidden("SECRET", 52.0, 10.0))

        assert described(items) == [("StreamParagraph", "Visible", ()), ("apart", "SECRET", ("invisible",))]

    def test_pieces_on_one_line_with_the_same_reasons_merge(self):
        items = laid_out(hidden("IGNORE", 10.0, 10.0), hidden("ALL", 52.0, 10.0))

        assert described(items) == [("apart", "IGNORE ALL", ("invisible",))]

    def test_pieces_with_other_reasons_are_other_lines(self):
        items = laid_out(hidden("IGNORE", 10.0, 10.0), hidden("ALL", 52.0, 10.0, reasons=("transparent",)))

        assert described(items) == [("apart", "IGNORE", ("invisible",)), ("apart", "ALL", ("transparent",))]

    def test_a_wide_gap_splits_the_line_into_cells(self):
        (item,) = laid_out(hidden("Name", 10.0, 10.0), hidden("Value", 200.0, 10.0))

        assert [found.text for found in item.cells] == ["Name", "Value"]

    def test_a_recovered_copy_is_apart_even_with_no_reason(self):
        drawn = chars("Total", 10.0, 50.0)

        items = laid_out(chars("Total", 10.0, 10.0), recovered=(drawn,))

        assert described(items) == [("StreamParagraph", "Total", ()), ("apart", "Total", ())]

    def test_recovered_and_hidden_pieces_with_the_same_reasons_share_a_line(self):
        items = laid_out(at_a_point("HID", 10.0, 10.0, 0.0), recovered=(at_a_point("DEN", 10.0, 10.0, 0.0),))

        assert described(items) == [("apart", "HIDDEN", ("tiny_font",))]

    def test_whitespace_only_hidden_text_lays_out_nothing(self):
        assert laid_out(hidden("   ", 10.0, 10.0)) == ()

    def test_an_off_page_line_stays_off_the_page(self):
        (item,) = laid_out(hidden("AWAY", -200.0, 10.0, reasons=("off_page",)))

        assert item.hidden_reasons == ("off_page",)
        assert not item.bbox.within(W, H)

    def test_every_char_is_in_a_line_once(self):
        pieces = (hidden("IGNORE", 10.0, 10.0), hidden("PREVIOUS", 10.0, 40.0), chars("Body", 10.0, 70.0))

        items = laid_out(*pieces, recovered=(at_a_point("RULES", 200.0, 200.0, 0.0),))

        apart = [
            piece.text
            for item in items
            if isinstance(item, ApartLine)
            for found in item.cells
            for piece in found.pieces
        ]
        assert sorted(apart) == ["IGNORE", "PREVIOUS", "RULES"]


class TestApartPlacement:
    """Each apart line reads next to the visible item nearest it: before it if it starts higher, else after it."""

    def test_a_line_between_two_paragraphs_reads_between_them(self):
        items = laid_out(chars("First", 10.0, 10.0), chars("Second", 10.0, 200.0), hidden("SECRET", 10.0, 50.0))

        assert [text for _, text, _ in described(items)] == ["First", "SECRET", "Second"]

    def test_a_line_above_everything_reads_first(self):
        items = laid_out(chars("First", 10.0, 100.0), chars("Second", 10.0, 200.0), hidden("SECRET", 10.0, 10.0))

        assert [text for _, text, _ in described(items)] == ["SECRET", "First", "Second"]

    def test_a_line_below_everything_reads_last(self):
        items = laid_out(chars("First", 10.0, 10.0), chars("Second", 10.0, 50.0), hidden("SECRET", 10.0, 250.0))

        assert [text for _, text, _ in described(items)] == ["First", "Second", "SECRET"]

    @pytest.mark.parametrize(
        "x,y,order",
        [
            pytest.param(200.0, 30.0, ["left", "right", "SECRET"], id="inside-the-right-column"),
            pytest.param(200.0, 0.0, ["left", "SECRET", "right"], id="atop-the-right-column"),
            pytest.param(10.0, 30.0, ["left", "SECRET", "right"], id="inside-the-left-column"),
        ],
    )
    def test_a_line_in_a_column_reads_with_that_column(self, x, y, order):
        items = laid_out(*column_of(LEFT, 10.0), *column_of(RIGHT, 200.0), hidden("SECRET", x, y))

        assert columns_named(items) == order

    def test_lines_by_one_item_read_top_to_bottom(self):
        items = laid_out(chars("Body", 10.0, 10.0), hidden("TWO", 10.0, 60.0), hidden("ONE", 10.0, 40.0))

        assert [text for _, text, _ in described(items)] == ["Body", "ONE", "TWO"]

    def test_with_no_visible_text_lines_read_top_to_bottom_then_left_to_right(self):
        items = laid_out(
            hidden("C", 10.0, 90.0), hidden("B", 200.0, 10.0), hidden("A", 10.0, 10.0, reasons=("tiny_font",))
        )

        assert [text for _, text, _ in described(items)] == ["A", "B", "C"]

    def test_an_off_page_line_reads_by_its_nearest_item(self):
        items = laid_out(
            chars("First", 10.0, 10.0),
            chars("Second", 10.0, 200.0),
            hidden("AWAY", -200.0, 210.0, reasons=("off_page",)),
        )

        assert [text for _, text, _ in described(items)] == ["First", "Second", "AWAY"]

    def test_hidden_text_never_changes_the_visible_order(self):
        left, right = column_of(LEFT, 10.0), column_of(RIGHT, 200.0)
        # A hidden line across the gutter, and one far off the page, would each change the XY-cut if it saw them.
        noise = (hidden("x" * 60, 10.0, 30.0), hidden("FAR", -5000.0, 10.0, reasons=("off_page",)))

        with_noise = [item for item in laid_out(*left, *right, *noise) if not isinstance(item, ApartLine)]

        assert columns_named(with_noise) == columns_named(laid_out(*left, *right)) == ["left", "right"]

    def test_the_nearest_item_is_by_the_gap_between_boxes_not_their_centres(self):
        # A tall paragraph on the left, a note at the top right. The line 9 pt right of the paragraph is 62 pt from
        # the note, though its centre is nearer the note's (94 pt) than the paragraph's (158 pt).
        tall = tuple(chars("this is a line of running text", 10.0, 10.0 + 14.0 * index) for index in range(20))

        items = laid_out(*tall, chars("Note", 300.0, 10.0), hidden("SECRET", 205.0, 40.0))

        assert [text[:4] for _, text, _ in described(items)] == ["this", "SECR", "Note"]

    def test_a_line_touching_two_items_reads_by_the_nearer_centre(self):
        # The line touches First's corner and overlaps Second: both gaps are 0, and Second's centre is nearer.
        items = laid_out(chars("First", 10.0, 10.0), chars("Second", 100.0, 22.0), hidden("SECRETSECRET", 40.0, 22.0))

        assert [text for _, text, _ in described(items)] == ["First", "Second", "SECRETSECRET"]

    def test_lines_read_in_the_frame_of_the_pages_turn(self):
        # Text falling down the page: lines read right to left, so "before" means further right, whatever its y.
        first, second = falling("First falling line", 300.0, 10.0), falling("Second falling line", 100.0, 10.0)
        secret = dataclasses.replace(falling("SECRET", 250.0, 0.0), hidden_reasons=("invisible",))

        items = laid_out(first, second, secret)

        assert [text for _, text, _ in described(items)] == ["First falling line", "SECRET", "Second falling line"]
        assert items[1].turn == 90

    def test_placement_at_its_bound_still_places_by_position(self, monkeypatch):
        monkeypatch.setattr(layout, "MAX_APART_PAIRS", 2)
        pieces = (chars("First", 10.0, 10.0), chars("Second", 10.0, 200.0), hidden("TWO", 10.0, 220.0))

        found = page_layout(pieces, (), NO_TABLES, width_pt=W, height_pt=H)

        assert ([text for _, text, _ in described(found.items)], found.warnings) == (["First", "Second", "TWO"], ())

    def test_placement_past_its_bound_puts_the_lines_last_with_a_warning(self, monkeypatch):
        monkeypatch.setattr(layout, "MAX_APART_PAIRS", 1)
        pieces = (
            chars("First", 10.0, 10.0),
            chars("Second", 10.0, 200.0),
            hidden("TWO", 10.0, 60.0),
            hidden("ONE", 10.0, 30.0),
        )

        found = page_layout(pieces, (), NO_TABLES, width_pt=W, height_pt=H)

        assert [text for _, text, _ in described(found.items)] == ["First", "Second", "ONE", "TWO"]
        assert found.warnings == (
            "2 hidden or recovered line(s) read after the page's visible text: placing them by position "
            "would compare 4 pairs, over the bound of 1",
        )


class TestPageLayoutArguments:
    """page_layout checks its arguments; its result is a validated record."""

    def test_recovered_must_be_a_tuple_of_pieces(self):
        with pytest.raises(TypeError, match="recovered"):
            page_layout((), [chars("x", 1.0, 1.0)], NO_TABLES, width_pt=W, height_pt=H)

    def test_a_piece_cannot_be_both_page_text_and_recovered(self):
        piece = chars("Twice", 10.0, 10.0)

        with pytest.raises(ValueError, match="recovered"):
            page_layout((piece,), (piece,), NO_TABLES, width_pt=W, height_pt=H)

    def test_a_recovered_piece_cannot_be_passed_twice(self):
        piece = chars("Twice", 10.0, 10.0)

        with pytest.raises(ValueError, match=r"^recovered\[1\] is recovered\[0\] again"):
            page_layout((), (piece, piece), NO_TABLES, width_pt=W, height_pt=H)

    def test_page_sides_must_be_positive(self):
        with pytest.raises(ValueError, match="width_pt"):
            page_layout((), (), NO_TABLES, width_pt=0.0, height_pt=H)

    def test_page_layout_is_validated(self):
        with pytest.raises(TypeError, match="items"):
            PageLayout(items=[], warnings=())
        with pytest.raises(TypeError, match="warnings"):
            PageLayout(items=(), warnings=["x"])


def tiny_text_page(tmp_path):
    """A 400 x 300 pt page of visible text and the same word hidden three ways: Tf 0, 0.01 pt and 0.04 pt."""
    content = (
        b"BT /F1 12 Tf 20 270 Td (Visible body text on this page) Tj ET "
        b"BT /F1 0 Tf 72 200 Td (ZERO) Tj ET "
        b"BT /F1 0.01 Tf 72 150 Td (CENTI) Tj ET "
        b"BT /F1 0.04 Tf 72 100 Td (FOUR) Tj ET"
    )
    return pdf(tmp_path, content)


class TestApartLinesEndToEnd:
    """The injection vector: text at 0, 0.01 and 0.04 pt lays out as hidden lines, through recovery and A3."""

    def test_tiny_text_lays_out_as_hidden_lines(self, tmp_path):
        handle = open_document(tiny_text_page(tmp_path))
        page = extract_page(handle, 0)
        assessment = assess_page(page, recovered=recover_dropped_text(handle, 0))
        lattice = find_lattice_tables(page.segments.segments, assessment.pieces)

        found = page_layout(assessment.pieces, assessment.recovered_pieces, lattice, width_pt=W, height_pt=H)

        assert found.warnings == ()
        assert described(found.items) == [
            ("StreamParagraph", "Visible body text on this page", ()),
            ("apart", "ZERO", ("tiny_font",)),
            ("apart", "CENTI", ("tiny_font",)),
            ("apart", "FOUR", ("tiny_font",)),
        ]
