"""Tests for the v5 PDF element types (``datagrunt.pdf.elements``)."""

import dataclasses
import hashlib
import os
import re
import time
from pathlib import Path
from typing import get_args

import polars as pl
import pyarrow as pa
import pytest

import datagrunt.pdf.elements as elements_module
from datagrunt.pdf import (
    HIDDEN_REASONS,
    BBox,
    BulletList,
    DatagruntPDFError,
    DocumentElement,
    Figure,
    Heading,
    HiddenReason,
    KeyValue,
    Paragraph,
    Table,
)
from datagrunt.pdf.elements import ExtractionMethod, PageRoute, PageStatus, make_element_id


def _bbox():
    return BBox(x0=0, y0=0, x1=100, y1=20)


def _base(**overrides):
    # element_id must equal make_element_id(page_number, reading_order).
    kwargs = {"element_id": "p0001_e001", "page_number": 1, "reading_order": 1, "bbox": _bbox()}
    kwargs.update(overrides)
    return kwargs


# Minimal own-field kwargs per element class; base-invariant tests run against
# every class to prove each one inherits the shared validation.
ELEMENT_FIELDS = {
    # A heading's own scope_path ends with its text.
    Heading: {"level": 2, "text": "Overview", "scope_path": ("Overview",)},
    Paragraph: {"text": "Body text."},
    KeyValue: {"key": "Invoice", "value": "42"},
    BulletList: {"items": ("one", "two"), "ordered": False},
    Table: {"headers": ("a", "b"), "rows": (("1", "2"),), "extraction_method": "lattice"},
    Figure: {"sha256": hashlib.sha256(b"png-bytes").hexdigest(), "width_px": 64, "height_px": 32},
}
ELEMENT_CLASSES = list(ELEMENT_FIELDS)


def _element(cls, **overrides):
    kwargs = {**_base(), **ELEMENT_FIELDS[cls], **overrides}
    text = overrides.get("text")
    if cls is Heading and "scope_path" not in overrides and isinstance(text, str) and text.strip():
        kwargs["scope_path"] = (text,)  # keep the heading's scope ending with its (overridden) text
    return cls(**kwargs)


class TestTypeAliases:
    def test_hidden_reasons_canonical_order(self):
        assert HIDDEN_REASONS == (
            "off_page",
            "invisible",
            "tiny_font",
            "transparent",
            "matches_background",
            "zero_scale",
        )

    def test_hidden_reason_literal_members(self):
        assert get_args(HiddenReason) == (
            "off_page",
            "invisible",
            "tiny_font",
            "transparent",
            "matches_background",
            "zero_scale",
        )

    def test_page_status_values(self):
        assert get_args(PageStatus) == ("ok", "ocr_fallback", "ocr_unavailable", "error")

    def test_page_route_values(self):
        assert get_args(PageRoute) == ("digital", "hybrid", "vision")

    def test_extraction_method_values(self):
        assert get_args(ExtractionMethod) == ("lattice", "stream")

    def test_document_element_is_union_of_the_six_element_types(self):
        assert set(get_args(DocumentElement)) == {Heading, Paragraph, KeyValue, Table, BulletList, Figure}


class TestMakeElementId:
    @pytest.mark.parametrize(
        ("page", "index", "expected"),
        [(1, 3, "p0001_e003"), (1, 0, "p0001_e000"), (9999, 999, "p9999_e999"), (12345, 1000, "p12345_e1000")],
    )
    def test_zero_padded_format(self, page, index, expected):
        assert make_element_id(page, index) == expected

    def test_page_zero_rejected(self):
        with pytest.raises(ValueError, match="page_number"):
            make_element_id(0, 1)

    def test_negative_index_rejected(self):
        with pytest.raises(ValueError, match="index"):
            make_element_id(1, -1)

    @pytest.mark.parametrize(("page", "index"), [(True, 1), (1, True), (1.0, 1), (1, "1")])
    def test_wrong_types_rejected(self, page, index):
        with pytest.raises(TypeError):
            make_element_id(page, index)


@pytest.mark.parametrize("cls", ELEMENT_CLASSES)
class TestElementBase:
    def test_valid_minimal_element(self, cls):
        element = _element(cls)
        assert element.element_id == "p0001_e001"
        assert element.scope_path == ELEMENT_FIELDS[cls].get("scope_path", ())
        assert element.hidden_reasons == ()
        assert element.is_hidden is False

    def test_large_page_number_id(self, cls):
        assert _element(cls, element_id="p12345_e001", page_number=12345).page_number == 12345

    def test_element_id_page_mismatch_rejected(self, cls):
        with pytest.raises(ValueError, match="element_id"):
            _element(cls, element_id="p0002_e001")

    @pytest.mark.parametrize("order", [0, 2, 10])
    def test_element_id_index_must_equal_reading_order(self, cls, order):
        with pytest.raises(ValueError, match=rf"element_id 'p0001_e001' encodes index 1, not reading_order {order}"):
            _element(cls, reading_order=order)

    @pytest.mark.parametrize("order", [0, 1, 999, 1000])
    def test_element_id_is_derived_from_page_and_reading_order(self, cls, order):
        element = _element(cls, element_id=make_element_id(1, order), reading_order=order)
        assert element.element_id == make_element_id(element.page_number, element.reading_order)

    @pytest.mark.parametrize(
        "bad_id",
        [
            "",
            "p001_e001",
            "p0001_e01",
            "P0001_e001",
            "p0001-e001",
            "p0001_e001\n",
            " p0001_e001",
            "p0001_e001x",
            "p١٢٣٤_e001",
            "p00001_e001",
            "p0001_e0001",
        ],
    )
    def test_malformed_element_id_rejected(self, cls, bad_id):
        with pytest.raises(ValueError, match="element_id"):
            _element(cls, element_id=bad_id)

    def test_element_id_wrong_type_rejected(self, cls):
        with pytest.raises(TypeError, match="element_id"):
            _element(cls, element_id=1)

    @pytest.mark.parametrize(("field", "bad"), [("page_number", 0), ("reading_order", -1)])
    def test_int_fields_below_minimum_rejected(self, cls, field, bad):
        with pytest.raises(ValueError, match=field):
            _element(cls, **{field: bad})

    @pytest.mark.parametrize("field", ["page_number", "reading_order"])
    @pytest.mark.parametrize("bad", [True, 1.0])
    def test_int_fields_wrong_type_rejected(self, cls, field, bad):
        with pytest.raises(TypeError, match=field):
            _element(cls, **{field: bad})

    def test_reading_order_zero_and_up(self, cls):
        assert _element(cls, element_id="p0001_e000", reading_order=0).reading_order == 0
        assert _element(cls, element_id="p0001_e007", reading_order=7).reading_order == 7

    def test_bbox_must_be_a_bbox(self, cls):
        with pytest.raises(TypeError, match="bbox"):
            _element(cls, bbox=(0, 0, 100, 20))

    def test_scope_path_accepts_tuple_of_headings(self, cls):
        # Ends with "Overview", so it is also a valid scope for the Heading fixture.
        assert _element(cls, scope_path=("Report", "Overview")).scope_path == ("Report", "Overview")

    @pytest.mark.parametrize(
        ("bad", "exc"), [(("Report", " "), ValueError), (["Report"], TypeError), ((1,), TypeError)]
    )
    def test_bad_scope_path_rejected(self, cls, bad, exc):
        with pytest.raises(exc, match="scope_path"):
            _element(cls, scope_path=bad)

    @pytest.mark.parametrize(
        "reasons", [("off_page",), ("invisible", "transparent"), ("matches_background",), HIDDEN_REASONS]
    )
    def test_hidden_reasons_flag_element_as_hidden(self, cls, reasons):
        element = _element(cls, hidden_reasons=reasons)
        assert element.hidden_reasons == reasons
        assert element.is_hidden is True

    @pytest.mark.parametrize(
        ("bad", "match"),
        [
            (("invisible", "invisible"), "duplicate"),
            (("blurry",), "unknown"),
            (("tiny_font", "off_page"), "canonical order"),
            (("transparent", "invisible"), "canonical order"),
            (("matches_background", "transparent"), "canonical order"),
        ],
    )
    def test_bad_hidden_reasons_rejected(self, cls, bad, match):
        with pytest.raises(ValueError, match=match):
            _element(cls, hidden_reasons=bad)

    @pytest.mark.parametrize("bad", [["off_page"], (1,), "off_page"])
    def test_hidden_reasons_wrong_type_rejected(self, cls, bad):
        with pytest.raises(TypeError, match="hidden_reasons"):
            _element(cls, hidden_reasons=bad)

    def test_is_frozen_slotted_and_keyword_only(self, cls):
        element = _element(cls)
        with pytest.raises(dataclasses.FrozenInstanceError):
            element.reading_order = 5
        assert not hasattr(element, "__dict__")
        # Every field supplied positionally: only kw_only can make this a TypeError.
        positional = [getattr(element, f.name) for f in dataclasses.fields(element)]
        with pytest.raises(TypeError, match="positional argument"):
            cls(*positional)

    def test_replace_revalidates(self, cls):
        with pytest.raises(ValueError, match="element_id"):
            dataclasses.replace(_element(cls), page_number=2)

    def test_replace_with_valid_values_succeeds(self, cls):
        moved = dataclasses.replace(_element(cls), element_id="p0002_e001", page_number=2)
        assert moved.page_number == 2

    def test_equal_and_hashable(self, cls):
        assert _element(cls) == _element(cls)
        assert hash(_element(cls)) == hash(_element(cls))


class TestHeading:
    @pytest.mark.parametrize("level", [1, 6])
    def test_level_bounds(self, level):
        assert _element(Heading, level=level).level == level

    def test_replace_revalidates_own_fields(self):
        with pytest.raises(ValueError, match="level"):
            dataclasses.replace(_element(Heading), level=7)

    @pytest.mark.parametrize("bad", [0, 7, -1])
    def test_level_out_of_range_rejected(self, bad):
        with pytest.raises(ValueError, match="level"):
            _element(Heading, level=bad)

    @pytest.mark.parametrize("bad", [True, 2.0, "2"])
    def test_level_wrong_type_rejected(self, bad):
        with pytest.raises(TypeError, match="level"):
            _element(Heading, level=bad)

    @pytest.mark.parametrize(("bad", "exc"), [("", ValueError), (" \n", ValueError), (None, TypeError)])
    def test_bad_text_rejected(self, bad, exc):
        with pytest.raises(exc, match="text"):
            _element(Heading, text=bad)

    @pytest.mark.parametrize("scope_path", [("Overview",), ("Report", "Overview"), ("A", "B", "Overview")])
    def test_scope_path_ends_with_own_text(self, scope_path):
        assert _element(Heading, scope_path=scope_path).scope_path == scope_path

    @pytest.mark.parametrize("scope_path", [(), ("Report",), ("Overview", "Report"), ("Overview ",), ("overview",)])
    def test_scope_path_not_ending_with_own_text_rejected(self, scope_path):
        # A heading opens its own scope, so its scope_path includes its text (verbatim, last).
        with pytest.raises(ValueError, match=r"scope_path must end with the heading's own text 'Overview'"):
            _element(Heading, scope_path=scope_path)

    @pytest.mark.parametrize(("level", "expected"), [(1, "# Overview"), (3, "### Overview"), (6, "###### Overview")])
    def test_to_markdown(self, level, expected):
        assert _element(Heading, level=level).to_markdown() == expected

    @pytest.mark.parametrize("text", ["Line one\nLine two", "Line one\r\nLine two", "Line one\rLine two"])
    def test_to_markdown_renders_line_breaks_as_spaces(self, text):
        heading = _element(Heading, text=text)
        assert heading.to_markdown() == "## Line one Line two"
        assert heading.text == text  # data stays verbatim

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("Rank #", "## Rank \\#"),
            ("#", "## \\#"),
            ("Issue ##  ", "## Issue \\#\\#"),  # every # escaped; trailing spaces stripped
            ("Tabbed\t#", "## Tabbed\t\\#"),
            ("Line\n#", "## Line \\#"),  # the break becomes a space, exposing the run
            ("C#", "## C\\#"),  # every # is escaped, closing sequence or not
            ("C# and F#", "## C\\# and F\\#"),
            ("# leading", "## \\# leading"),
        ],
    )
    def test_to_markdown_trailing_hash_run_not_eaten_as_closing_sequence(self, text, expected):
        assert _element(Heading, text=text).to_markdown() == expected


class TestParagraph:
    @pytest.mark.parametrize(("bad", "exc"), [("", ValueError), ("\t", ValueError), (b"x", TypeError)])
    def test_bad_text_rejected(self, bad, exc):
        with pytest.raises(exc, match="text"):
            _element(Paragraph, text=bad)

    def test_plain_text_unchanged(self):
        assert _element(Paragraph, text="Plain body text.").to_markdown() == "Plain body text."

    def test_leading_metachar_escaped(self):
        assert _element(Paragraph, text="# not a heading").to_markdown() == "\\# not a heading"

    def test_every_line_escaped(self):
        paragraph = _element(Paragraph, text="intro\n- not a list\n1. not ordered\n> not a quote")
        assert paragraph.to_markdown() == "intro\n\\- not a list\n1\\. not ordered\n\\> not a quote"

    @pytest.mark.parametrize("sep", ["\r\n", "\r", "\n"])
    def test_line_endings_rendered_as_newline_while_escaping(self, sep):
        paragraph = _element(Paragraph, text=f"a{sep}# b")
        assert paragraph.to_markdown() == "a\n\\# b"
        assert paragraph.text == f"a{sep}# b"

    def test_text_field_stays_verbatim(self):
        assert _element(Paragraph, text="# raw").text == "# raw"


class TestKeyValue:
    def test_empty_value_allowed(self):
        assert _element(KeyValue, value="").value == ""

    @pytest.mark.parametrize(("bad", "exc"), [("", ValueError), (" ", ValueError), (3, TypeError)])
    def test_bad_key_rejected(self, bad, exc):
        with pytest.raises(exc, match="key"):
            _element(KeyValue, key=bad)

    @pytest.mark.parametrize("bad", [None, 42, ("v",)])
    def test_value_wrong_type_rejected(self, bad):
        with pytest.raises(TypeError, match="value"):
            _element(KeyValue, value=bad)

    def test_to_markdown(self):
        assert _element(KeyValue).to_markdown() == "**Invoice:** 42"

    def test_to_markdown_empty_value(self):
        assert _element(KeyValue, value="").to_markdown() == "**Invoice:** "

    def test_to_markdown_escapes_asterisks_in_key(self):
        kv = _element(KeyValue, key="Total *net*", value="5 * 3")
        assert kv.to_markdown() == "**Total \\*net\\*:** 5 \\* 3"
        assert kv.key == "Total *net*"

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("C:\\", "**C:\\\\:** v"),  # trailing backslash would otherwise escape the colon
            ("a\\*b", "**a\\\\\\*b:** v"),  # backslash escaped before the star
            ("k\n# H", "**k\n\\# H:** v"),  # line break in key cannot start a heading
            ("k\r\nl", "**k\nl:** v"),
            ("  padded  ", "**padded:** v"),  # leading space would stop ** opening emphasis
        ],
    )
    def test_to_markdown_key_escaping(self, key, expected):
        kv = _element(KeyValue, key=key, value="v")
        assert kv.to_markdown() == expected
        assert kv.key == key

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("v\n# H", "**K:** v\n\\# H"),
            ("v\n===", "**K:** v\n\\==="),
            ("v\r\n- item", "**K:** v\n\\- item"),
        ],
    )
    def test_to_markdown_value_lines_escaped(self, value, expected):
        kv = _element(KeyValue, key="K", value=value)
        assert kv.to_markdown() == expected
        assert kv.value == value


class TestBulletList:
    def test_single_item(self):
        assert _element(BulletList, items=("only",)).to_markdown() == "- only"

    def test_unordered_markdown(self):
        assert _element(BulletList, items=("a", "b", "c")).to_markdown() == "- a\n- b\n- c"

    def test_ordered_markdown_numbers_sequentially(self):
        bullets = _element(BulletList, items=("a", "b", "c"), ordered=True)
        assert bullets.to_markdown() == "1. a\n2. b\n3. c"

    def test_items_escaped(self):
        bullets = _element(BulletList, items=("- nested?", "2. numbered", "# hash"), ordered=True)
        assert bullets.to_markdown() == "1. \\- nested?\n2. 2\\. numbered\n3. \\# hash"
        assert bullets.items == ("- nested?", "2. numbered", "# hash")

    @pytest.mark.parametrize(
        ("ordered", "expected"),
        [
            (False, "- first\n  \\# second\n  third"),
            (True, "1. first\n   \\# second\n   third"),
        ],
    )
    def test_multi_line_item_stays_inside_the_list(self, ordered, expected):
        bullets = _element(BulletList, items=("first\n# second\r\nthird",), ordered=ordered)
        assert bullets.to_markdown() == expected

    def test_continuation_indent_tracks_marker_width(self):
        items = tuple(f"item{n}" for n in range(1, 10)) + ("ten\n- not a sub-list",)
        lines = _element(BulletList, items=items, ordered=True).to_markdown().split("\n")
        assert lines[-2:] == ["10. ten", "    \\- not a sub-list"]

    def test_empty_items_rejected(self):
        with pytest.raises(ValueError, match="items"):
            _element(BulletList, items=())

    @pytest.mark.parametrize(("bad", "exc"), [(("a", ""), ValueError), (["a"], TypeError), (("a", None), TypeError)])
    def test_bad_items_rejected(self, bad, exc):
        with pytest.raises(exc, match="items"):
            _element(BulletList, items=bad)

    @pytest.mark.parametrize("bad", [1, 0, "yes", None])
    def test_ordered_must_be_bool(self, bad):
        with pytest.raises(TypeError, match="ordered"):
            _element(BulletList, ordered=bad)


def _table(**overrides):
    return _element(Table, **overrides)


class TestTableShape:
    def test_headers_and_rows(self):
        table = _table(headers=("Name", "Qty"), rows=(("bolt", "4"), ("nut", "9")))
        assert table.headers == ("Name", "Qty")
        assert table.rows == (("bolt", "4"), ("nut", "9"))
        assert table.continued_from_id is None

    def test_headers_with_zero_rows(self):
        assert _table(headers=("a",), rows=()).rows == ()

    def test_rows_without_headers(self):
        assert _table(headers=(), rows=(("1", "2", "3"),)).headers == ()

    def test_single_cell_table(self):
        assert _table(headers=(), rows=(("x",),)).rows == (("x",),)

    def test_blank_headers_and_cells_allowed(self):
        table = _table(headers=("", " "), rows=(("", "\n"),))
        assert table.headers == ("", " ")

    def test_no_headers_and_no_rows_rejected(self):
        with pytest.raises(ValueError, match="headers or at least one row"):
            _table(headers=(), rows=())

    def test_zero_width_rows_rejected(self):
        with pytest.raises(ValueError, match="at least one column"):
            _table(headers=(), rows=((),))

    @pytest.mark.parametrize("bad_row", [("1",), ("1", "2", "3")])
    def test_row_width_must_match_headers(self, bad_row):
        with pytest.raises(ValueError, match=r"rows\[1\]"):
            _table(headers=("a", "b"), rows=(("1", "2"), bad_row))

    def test_ragged_headerless_rows_rejected(self):
        with pytest.raises(ValueError, match=r"rows\[2\]"):
            _table(headers=(), rows=(("1", "2"), ("3", "4"), ("5",)))

    @pytest.mark.parametrize(
        ("field", "bad"),
        [
            ("headers", ["a", "b"]),
            ("headers", ("a", 2)),
            ("rows", [("1", "2")]),
            ("rows", (["1", "2"],)),
            ("rows", (("1", 2),)),
            ("rows", (("1", None),)),
        ],
    )
    def test_wrong_types_rejected(self, field, bad):
        with pytest.raises(TypeError, match=field):
            _table(**{field: bad})

    @pytest.mark.parametrize("method", ["lattice", "stream"])
    def test_extraction_methods(self, method):
        assert _table(extraction_method=method).extraction_method == method

    @pytest.mark.parametrize(("bad", "exc"), [("hybrid", ValueError), ("", ValueError), (None, TypeError)])
    def test_bad_extraction_method_rejected(self, bad, exc):
        with pytest.raises(exc, match="extraction_method"):
            _table(extraction_method=bad)


class TestTableContinuation:
    def test_valid_continuation_id(self):
        assert _table(continued_from_id="p0001_e000").continued_from_id == "p0001_e000"

    def test_continuation_from_earlier_page_id_format_allowed(self):
        table = _table(element_id="p0002_e001", page_number=2, continued_from_id="p0001_e004")
        assert table.continued_from_id == "p0001_e004"

    @pytest.mark.parametrize("bad", ["p1_e1", "p00001_e001", "p0000_e001", "table-1", ""])
    def test_malformed_continuation_id_rejected(self, bad):
        with pytest.raises(ValueError, match="continued_from_id"):
            _table(continued_from_id=bad)

    def test_self_continuation_rejected(self):
        with pytest.raises(ValueError, match="continued_from_id"):
            _table(continued_from_id="p0001_e001")

    def test_continuation_wrong_type_rejected(self):
        with pytest.raises(TypeError, match="continued_from_id"):
            _table(continued_from_id=1)


class TestTableColumnCentres:
    """``column_centres``: each column's display-space x centre, which the scope pass links split tables by."""

    def test_defaults_to_empty(self):
        assert _table().column_centres == ()

    def test_one_centre_per_column(self):
        assert _table(column_centres=(10.5, 80.0)).column_centres == (10.5, 80.0)

    def test_ints_are_stored_as_floats(self):
        centres = _table(column_centres=(10, 80)).column_centres

        assert centres == (10.0, 80.0)
        assert [type(centre) for centre in centres] == [float, float]

    def test_a_table_without_headers_counts_its_columns_in_the_first_row(self):
        table = _table(headers=(), rows=(("1", "2", "3"),), column_centres=(1.0, 2.0, 3.0))

        assert table.column_centres == (1.0, 2.0, 3.0)

    def test_off_page_centres_keep_their_raw_coordinates(self):
        assert _table(column_centres=(-40.0, -10.0)).column_centres == (-40.0, -10.0)

    @pytest.mark.parametrize("bad", [(10.0,), (10.0, 20.0, 30.0)], ids=["one", "three"])
    def test_count_must_match_the_width(self, bad):
        message = f"column_centres must have 2 values (one per column), got {len(bad)}"
        with pytest.raises(ValueError, match=re.escape(message)):
            _table(column_centres=bad)

    @pytest.mark.parametrize("bad", [(20.0, 10.0), (10.0, 10.0)], ids=["decreasing", "equal"])
    def test_must_strictly_increase(self, bad):
        with pytest.raises(ValueError, match=re.escape(f"column_centres must strictly increase, got {bad!r}")):
            _table(column_centres=bad)

    @pytest.mark.parametrize(
        ("bad", "exc", "match"),
        [
            ([10.0, 20.0], TypeError, "column_centres must be a tuple, got list"),
            ((10.0, "20"), TypeError, "column_centres[1] must be an int or float"),
            ((True, 20.0), TypeError, "column_centres[0] must be an int or float"),
            ((10.0, float("nan")), ValueError, "column_centres[1] must be finite"),
            ((10.0, float("inf")), ValueError, "column_centres[1] must be finite"),
        ],
        ids=["list", "str", "bool", "nan", "inf"],
    )
    def test_bad_values_rejected(self, bad, exc, match):
        with pytest.raises(exc, match=re.escape(match)):
            _table(column_centres=bad)

    def test_part_of_equality(self):
        assert _table(column_centres=(1.0, 2.0)) != _table()


class TestTableColumnNames:
    def test_headers_used_verbatim(self):
        assert _table(headers=("Name", "Qty"), rows=()).column_names() == ("Name", "Qty")

    def test_absent_headers_numbered_from_one(self):
        assert _table(headers=(), rows=(("1", "2", "3"),)).column_names() == ("column_1", "column_2", "column_3")

    def test_blank_headers_numbered_by_position(self):
        table = _table(headers=("", "Qty", "  "), rows=())
        assert table.column_names() == ("column_1", "Qty", "column_3")
        assert table.headers == ("", "Qty", "  ")

    def test_duplicates_suffixed_in_order(self):
        assert _table(headers=("a", "a", "a"), rows=()).column_names() == ("a", "a_2", "a_3")

    def test_suffix_skips_names_taken_by_real_headers(self):
        assert _table(headers=("a", "a", "a_2"), rows=()).column_names() == ("a", "a_3", "a_2")

    def test_generated_name_colliding_with_header_is_suffixed(self):
        assert _table(headers=("column_2", ""), rows=()).column_names() == ("column_2", "column_2_2")

    def test_real_header_beats_earlier_generated_name(self):
        table = _table(headers=("x", "y", "", "z", "column_3"), rows=())
        assert table.column_names() == ("x", "y", "column_3_2", "z", "column_3")

    def test_real_header_beats_earlier_duplicate_suffix(self):
        assert _table(headers=("a", "a", "a_2"), rows=()).column_names() == ("a", "a_3", "a_2")

    def test_names_always_unique(self):
        headers = ("x", "", "x", "column_2", "x_2", "x", "")
        names = _table(headers=headers, rows=()).column_names()
        assert len(set(names)) == len(names) == len(headers)

    def test_deterministic(self):
        table = _table(headers=("a", "", "a"), rows=())
        assert table.column_names() == table.column_names() == ("a", "column_2", "a_2")

    @pytest.mark.parametrize(
        ("headers", "names"),
        [
            (("a", "A"), ("a", "A_2")),  # DuckDB treats column names case-insensitively
            (("Name", "NAME", "name_2"), ("Name", "NAME_3", "name_2")),
            (("", "COLUMN_1"), ("column_1_2", "COLUMN_1")),
            (("Straße", "STRASSE"), ("Straße", "STRASSE_2")),  # casefold, not lower: ß folds to ss
            (("x", "X_2", "x"), ("x", "X_2", "x_3")),
        ],
    )
    def test_duplicates_detected_case_insensitively(self, headers, names):
        assert _table(headers=headers, rows=()).column_names() == names

    def test_names_unique_case_insensitively(self):
        headers = ("a", "A", "", "COLUMN_3", "a_2", "A_2", "column_3_2", "")
        names = _table(headers=headers, rows=()).column_names()
        folded = [name.casefold() for name in names]
        assert len(set(folded)) == len(folded) == len(headers)

    def test_dataframe_accepts_case_variant_headers(self):
        df = _table(headers=("id", "ID"), rows=(("1", "2"),)).to_dataframe()
        assert df.columns == ["id", "ID_2"]

    def test_many_duplicates_named_in_linear_time(self):
        # A quadratic suffix search takes seconds here; a linear one takes milliseconds.
        count = 10_000
        table = _table(headers=("a",) * count, rows=())
        started = time.perf_counter()
        names = table.column_names()
        elapsed = time.perf_counter() - started
        assert names[:3] == ("a", "a_2", "a_3") and names[-1] == f"a_{count}"
        assert elapsed < 1.0, f"column_names took {elapsed:.2f}s for {count} duplicate headers"


class TestTableExports:
    def test_to_dataframe(self):
        df = _table(headers=("Name", "Qty"), rows=(("bolt", "4"), ("nut", "9"))).to_dataframe()
        assert df.columns == ["Name", "Qty"]
        assert df.schema == {"Name": pl.String, "Qty": pl.String}
        assert df.rows() == [("bolt", "4"), ("nut", "9")]

    def test_zero_row_table_gives_empty_typed_dataframe(self):
        df = _table(headers=("a", "b"), rows=()).to_dataframe()
        assert df.shape == (0, 2)
        assert df.schema == {"a": pl.String, "b": pl.String}

    def test_dataframe_uses_deduplicated_column_names(self):
        df = _table(headers=("a", "a", ""), rows=(("1", "2", "3"),)).to_dataframe()
        assert df.columns == ["a", "a_2", "column_3"]

    def test_numeric_looking_cells_stay_strings(self):
        df = _table(headers=("n",), rows=(("007",), ("1e3",))).to_dataframe()
        assert df["n"].to_list() == ["007", "1e3"]

    def test_to_arrow_table(self):
        arrow = _table(headers=("Name", "Qty"), rows=(("bolt", "4"),)).to_arrow_table()
        assert isinstance(arrow, pa.Table)
        assert arrow.column_names == ["Name", "Qty"]
        assert arrow.to_pylist() == [{"Name": "bolt", "Qty": "4"}]
        assert arrow.schema == pa.schema([("Name", pa.large_string()), ("Qty", pa.large_string())])

    def test_zero_row_arrow_table(self):
        arrow = _table(headers=("a",), rows=()).to_arrow_table()
        assert (arrow.num_rows, arrow.column_names) == (0, ["a"])
        assert arrow.schema == pa.schema([("a", pa.large_string())])

    def test_arrow_schema_pinned_whatever_string_type_polars_exports(self, monkeypatch):
        # Newer polars exports string_view by default; the schema must not follow it.
        export = pl.DataFrame.to_arrow

        def newest(self, *args, **kwargs):
            return export(self, compat_level=pl.CompatLevel.newest())

        monkeypatch.setattr(pl.DataFrame, "to_arrow", newest)
        arrow = _table(headers=("a", "b"), rows=(("1", "2"),)).to_arrow_table()
        assert arrow.schema.types == [pa.large_string(), pa.large_string()]
        assert arrow.to_pylist() == [{"a": "1", "b": "2"}]

    def test_to_dicts(self):
        table = _table(headers=("a", "a"), rows=(("1", "2"), ("3", "4")))
        assert table.to_dicts() == [{"a": "1", "a_2": "2"}, {"a": "3", "a_2": "4"}]

    def test_zero_row_to_dicts(self):
        assert _table(headers=("a",), rows=()).to_dicts() == []


class TestTableMarkdown:
    def test_gfm_table(self):
        table = _table(headers=("Name", "Qty"), rows=(("bolt", "4"), ("nut", "9")))
        assert table.to_markdown() == "| Name | Qty |\n| --- | --- |\n| bolt | 4 |\n| nut | 9 |"

    def test_zero_row_table_is_header_and_separator(self):
        assert _table(headers=("a",), rows=()).to_markdown() == "| a |\n| --- |"

    def test_headerless_table_has_an_empty_header_row(self):
        # GFM needs a header row; generated column names would invent header text.
        table = _table(headers=(), rows=(("1", "2"),))
        assert table.to_markdown() == "|  |  |\n| --- | --- |\n| 1 | 2 |"

    def test_blank_headers_render_as_empty_cells(self):
        table = _table(headers=("", "Qty", " \n "), rows=(("a", "1", "b"),))
        assert table.to_markdown() == "|  | Qty |  |\n| --- | --- | --- |\n| a | 1 | b |"

    def test_duplicate_headers_render_verbatim(self):
        table = _table(headers=("a", "A", "a"), rows=())
        assert table.to_markdown() == "| a | A | a |\n| --- | --- | --- |"

    def test_pipes_and_line_breaks_escaped_in_cells(self):
        table = _table(headers=("a|b", "c\nd"), rows=(("x | y", "line1\r\nline2\rline3"),))
        assert table.to_markdown() == "| a\\|b | c<br>d |\n| --- | --- |\n| x \\| y | line1<br>line2<br>line3 |"
        assert table.rows == (("x | y", "line1\r\nline2\rline3"),)

    @pytest.mark.parametrize(
        ("cell", "rendered"),
        [
            ("a|b", "a\\|b"),
            ("a\\|b", "a\\\\\\|b"),  # backslash run before a pipe is doubled, then the pipe escaped
            ("a\\\\|b", "a\\\\\\\\\\|b"),
            ("C:\\dir", "C:\\\\dir"),  # every literal backslash is doubled, before a pipe or not
            ("end\\\nnext", "end\\\\<br>next"),  # backslash before <br> would escape its "<"
        ],
    )
    def test_backslashes_before_structural_chars_doubled(self, cell, rendered):
        table = _table(headers=("h",), rows=((cell,),))
        assert table.to_markdown() == f"| h |\n| --- |\n| {rendered} |"

    def test_empty_cells_render(self):
        assert _table(headers=("a", "b"), rows=(("", "1"),)).to_markdown() == "| a | b |\n| --- | --- |\n|  | 1 |"


IMAGE = b"png-bytes"
IMAGE_SHA = hashlib.sha256(IMAGE).hexdigest()


def _figure(**overrides):
    return _element(Figure, **overrides)


class TestFigureFields:
    def test_defaults(self):
        figure = _figure()
        assert (figure.image_path, figure.caption) == (None, None)

    @pytest.mark.parametrize("bad_sha", [IMAGE_SHA.upper(), IMAGE_SHA[:63], IMAGE_SHA + "0", "z" * 64])
    def test_bad_sha_rejected(self, bad_sha):
        with pytest.raises(ValueError, match="sha256"):
            _figure(sha256=bad_sha)

    def test_sha_wrong_type_rejected(self):
        with pytest.raises(TypeError, match="sha256"):
            _figure(sha256=None)

    @pytest.mark.parametrize("field", ["width_px", "height_px"])
    def test_pixel_dimensions_at_least_one(self, field):
        assert getattr(_figure(**{field: 1}), field) == 1
        with pytest.raises(ValueError, match=field):
            _figure(**{field: 0})

    @pytest.mark.parametrize("field", ["width_px", "height_px"])
    @pytest.mark.parametrize("bad", [True, 64.0])
    def test_pixel_dimensions_wrong_type_rejected(self, field, bad):
        with pytest.raises(TypeError, match=field):
            _figure(**{field: bad})

    def test_image_path_must_be_a_path(self):
        with pytest.raises(TypeError, match="image_path"):
            _figure(image_path="fig.png")

    def test_caption_must_be_str_or_none(self):
        with pytest.raises(TypeError, match="caption"):
            _figure(caption=3)

    def test_loader_must_be_callable(self):
        with pytest.raises(TypeError, match="_loader"):
            _figure(_loader=IMAGE)

    def test_image_path_excluded_from_equality_and_hash(self):
        # Where the image was written is storage, not identity.
        stored = _figure(image_path=Path("out/a/fig.png"))
        moved = _figure(image_path=Path("out/b/fig.png"))
        assert stored == moved == _figure()
        assert hash(stored) == hash(moved) == hash(_figure())
        assert "image_path=PosixPath" in repr(stored) or "image_path=WindowsPath" in repr(stored)

    def test_loader_excluded_from_equality_hash_and_repr(self):
        with_loader = _figure(_loader=lambda: IMAGE)
        assert with_loader == _figure()
        assert hash(with_loader) == hash(_figure())
        assert "_loader" not in repr(with_loader)

    def test_raw_loader_must_be_callable(self):
        with pytest.raises(TypeError, match="_raw_loader must be callable or None, got bytes"):
            _figure(_raw_loader=IMAGE)

    def test_raw_loader_excluded_from_equality_hash_and_repr(self):
        with_loader = _figure(_raw_loader=lambda: IMAGE)
        assert with_loader == _figure()
        assert hash(with_loader) == hash(_figure())
        assert "_raw_loader" not in repr(with_loader)


class TestFigureReadBytes:
    def test_reads_file(self, tmp_path):
        path = tmp_path / "fig.png"
        path.write_bytes(IMAGE)
        assert _figure(image_path=path).read_bytes() == IMAGE

    def test_file_takes_precedence_over_loader(self, tmp_path):
        path = tmp_path / "fig.png"
        path.write_bytes(IMAGE)

        def loader():
            raise AssertionError("loader must not be called when image_path is set")

        assert _figure(image_path=path, _loader=loader).read_bytes() == IMAGE

    def test_reads_loader_when_no_path(self):
        assert _figure(_loader=lambda: IMAGE).read_bytes() == IMAGE

    def test_file_bytes_are_derived_and_not_checked_against_sha256(self, tmp_path):
        # sha256 hashes the raw embedded stream (D1); an exported PNG is derived from it.
        path = tmp_path / "fig.png"
        path.write_bytes(b"exported png")
        assert _figure(image_path=path).read_bytes() == b"exported png"

    def test_loader_bytes_are_derived_and_not_checked_against_sha256(self):
        assert _figure(_loader=lambda: b"decoded png").read_bytes() == b"decoded png"

    def test_never_reads_the_raw_loader(self):
        def raw_loader():
            raise AssertionError("read_bytes must not read the raw stream")

        assert _figure(_loader=lambda: IMAGE, _raw_loader=raw_loader).read_bytes() == IMAGE

    def test_no_byte_source_raises(self):
        with pytest.raises(DatagruntPDFError, match="no image bytes"):
            _figure().read_bytes()

    def test_missing_file_fails_loud(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _figure(image_path=tmp_path / "gone.png").read_bytes()

    @pytest.mark.parametrize("bad", [None, "png-bytes", bytearray(IMAGE)])
    def test_loader_returning_non_bytes_raises(self, bad):
        with pytest.raises(TypeError, match="_loader"):
            _figure(_loader=lambda: bad).read_bytes()

    def test_directory_path_rejected_before_opening(self, tmp_path):
        with pytest.raises(DatagruntPDFError, match="is not a regular file"):
            _figure(image_path=tmp_path).read_bytes()

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs POSIX named pipes")
    def test_named_pipe_rejected_without_blocking(self, tmp_path):
        # Opening a FIFO for reading blocks until a writer appears: never open one.
        fifo = tmp_path / "fig.png"
        os.mkfifo(fifo)
        with pytest.raises(DatagruntPDFError, match="is not a regular file"):
            _figure(image_path=fifo).read_bytes()


class TestFigureReadBytesBounded:
    """Asset reads are capped at ``_MAX_ASSET_BYTES`` (patched small here, never a real 1 GiB file)."""

    def test_limit_is_one_gibibyte(self):
        assert elements_module._MAX_ASSET_BYTES == 1 << 30

    def test_file_at_limit_is_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE))
        path = tmp_path / "fig.png"
        path.write_bytes(IMAGE)
        assert _figure(image_path=path).read_bytes() == IMAGE

    def test_file_over_limit_rejected_naming_size(self, tmp_path, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE) - 1)
        path = tmp_path / "fig.png"
        path.write_bytes(IMAGE)
        with pytest.raises(DatagruntPDFError, match=f"{len(IMAGE)} bytes"):
            _figure(image_path=path).read_bytes()

    def test_read_stays_bounded_when_stat_understates_size(self, tmp_path, monkeypatch):
        # A file that grows after stat() must still be rejected, not read whole.
        class _StaleStatPath(type(tmp_path)):
            def stat(self, *args, **kwargs):
                return os.stat_result((0o100644, 0, 0, 1, 0, 0, 0, 0, 0, 0))

        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE) - 1)
        path = tmp_path / "fig.png"
        path.write_bytes(IMAGE)
        with pytest.raises(DatagruntPDFError, match="asset limit"):
            _figure(image_path=_StaleStatPath(path)).read_bytes()

    def test_loader_result_at_limit_is_returned(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE))
        assert _figure(_loader=lambda: IMAGE).read_bytes() == IMAGE

    def test_loader_result_over_limit_rejected_naming_size(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE) - 1)
        with pytest.raises(DatagruntPDFError, match=f"{len(IMAGE)} bytes"):
            _figure(_loader=lambda: IMAGE).read_bytes()


class TestFigureReadRawBytes:
    """read_raw_bytes() returns the raw embedded stream, verified against sha256 (D1)."""

    def test_reads_the_raw_loader_verified(self):
        assert _figure(_raw_loader=lambda: IMAGE).read_raw_bytes() == IMAGE

    def test_sha_mismatch_raises(self):
        with pytest.raises(DatagruntPDFError, match=f"sha256 mismatch \\(expected {IMAGE_SHA}, got "):
            _figure(_raw_loader=lambda: b"tampered").read_raw_bytes()

    def test_no_raw_source_raises_even_with_png_sources(self, tmp_path):
        # image_path and _loader hold derived PNG bytes, never the raw stream.
        path = tmp_path / "fig.png"
        path.write_bytes(IMAGE)
        with pytest.raises(DatagruntPDFError, match="figure p0001_e001 has no raw image bytes: no raw loader"):
            _figure(image_path=path, _loader=lambda: IMAGE).read_raw_bytes()

    @pytest.mark.parametrize("bad", [None, "png-bytes", bytearray(IMAGE)])
    def test_raw_loader_returning_non_bytes_raises(self, bad):
        with pytest.raises(TypeError, match="_raw_loader result must be bytes"):
            _figure(_raw_loader=lambda: bad).read_raw_bytes()

    def test_raw_loader_result_at_limit_is_returned(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE))
        assert _figure(_raw_loader=lambda: IMAGE).read_raw_bytes() == IMAGE

    def test_raw_loader_result_over_limit_rejected_naming_size(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(IMAGE) - 1)
        with pytest.raises(DatagruntPDFError, match=f"_raw_loader result is {len(IMAGE)} bytes, over the"):
            _figure(_raw_loader=lambda: IMAGE).read_raw_bytes()


class TestFigureMarkdown:
    def test_image_link_with_escaped_caption(self):
        figure = _figure(image_path=Path("figures/fig1.png"), caption="Revenue [2024]")
        assert figure.to_markdown() == "![Revenue \\[2024\\]](figures/fig1.png)"

    @pytest.mark.parametrize("caption", ["Rev\n# 2024", "Rev\r\n# 2024", "Rev\r# 2024"])
    def test_image_alt_text_kept_on_one_line(self, caption):
        figure = _figure(image_path=Path("figures/fig1.png"), caption=caption)
        assert figure.to_markdown() == "![Rev \\# 2024](figures/fig1.png)"

    def test_placeholder_caption_escapes_every_line(self):
        figure = _figure(caption="Rev\n# 2024")
        assert figure.to_markdown() == f"<!-- figure p0001_e001 sha256={IMAGE_SHA[:8]} -->\nRev\n\\# 2024"

    def test_image_link_without_caption(self):
        assert _figure(image_path=Path("figures/fig1.png")).to_markdown() == "![](figures/fig1.png)"

    def test_image_target_escaped(self):
        figure = _figure(image_path=Path("my figures/fig (1).png"))
        assert figure.to_markdown() == "![](<my figures/fig (1).png>)"

    def test_placeholder_comment_without_path(self):
        assert _figure().to_markdown() == f"<!-- figure p0001_e001 sha256={IMAGE_SHA[:8]} -->"

    def test_placeholder_comment_followed_by_caption(self):
        figure = _figure(caption="# Figure 1: revenue")
        assert figure.to_markdown() == f"<!-- figure p0001_e001 sha256={IMAGE_SHA[:8]} -->\n\\# Figure 1: revenue"
        assert figure.caption == "# Figure 1: revenue"

    @pytest.mark.parametrize("caption", ["", " ", "\n\t"])
    def test_blank_caption_adds_no_line(self, caption):
        figure = _figure(caption=caption)
        assert figure.to_markdown() == f"<!-- figure p0001_e001 sha256={IMAGE_SHA[:8]} -->"
        assert figure.caption == caption


class TestRawHtmlEscaping:
    """Element text can never open raw HTML, a comment, or an entity: every ``<`` / ``&`` is escaped."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("<!-- draft", "\\<!-- draft"),
            ("<!-- page 2 route=digital status=ok -->", "\\<!-- page 2 route=digital status=ok --\\>"),
            ("<img src=x onerror=alert(1)>", "\\<img src=x onerror=alert(1)\\>"),
            ("a\\<b", "a\\\\\\<b"),  # the backslash run before "<" is doubled, then "<" escaped
            ("a\\\\<b", "a\\\\\\\\\\<b"),
            ("&amp;", "\\&amp;"),
            ("x & y\n<b>", "x \\& y\n\\<b\\>"),
        ],
    )
    def test_paragraph(self, text, expected):
        paragraph = _element(Paragraph, text=text)
        assert paragraph.to_markdown() == expected
        assert paragraph.text == text  # data stays verbatim

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("<!-- draft", "## \\<!-- draft"),
            ("a\\<b", "## a\\\\\\<b"),
            ("Fish & <Chips>", "## Fish \\& \\<Chips\\>"),
        ],
    )
    def test_heading(self, text, expected):
        assert _element(Heading, text=text).to_markdown() == expected

    @pytest.mark.parametrize(
        ("key", "value", "expected"),
        [
            ("<k&>", "<!-- v\n&amp;", "**\\<k\\&\\>:** \\<!-- v\n\\&amp;"),
            ("a\\<b", "v", "**a\\\\\\<b:** v"),  # key backslashes are all escaped first: no double escape
        ],
    )
    def test_key_value(self, key, value, expected):
        assert _element(KeyValue, key=key, value=value).to_markdown() == expected

    def test_bullet_items(self):
        bullets = _element(BulletList, items=("<!-- a", "a\\<b & c"))
        assert bullets.to_markdown() == "- \\<!-- a\n- a\\\\\\<b \\& c"

    def test_table_cells_and_headers(self):
        table = _element(Table, headers=("<h>",), rows=(("<!-- x",), ("a\\<b",), ("&amp;",)))
        assert table.to_markdown() == "| \\<h\\> |\n| --- |\n| \\<!-- x |\n| a\\\\\\<b |\n| \\&amp; |"

    def test_figure_placeholder_caption_line(self):
        figure = _element(Figure, caption="<!-- c\n&amp;")
        assert figure.to_markdown() == f"<!-- figure p0001_e001 sha256={IMAGE_SHA[:8]} -->\n\\<!-- c\n\\&amp;"

    @pytest.mark.parametrize(
        ("caption", "alt"),
        [
            # Every backslash is doubled once and "<" escaped once: no double escaping.
            ("a\\<b & [c]", "a\\\\\\<b \\& \\[c\\]"),
            ("a\\\\<b", "a\\\\\\\\\\<b"),
            ("&amp;", "\\&amp;"),
            ("<img src=x onerror=alert(1)>", "\\<img src=x onerror=alert(1)\\>"),
        ],
    )
    def test_figure_alt_text_composes_with_link_text_escaping(self, caption, alt):
        figure = _element(Figure, caption=caption, image_path=Path("figures/fig1.png"))
        assert figure.to_markdown() == f"![{alt}](figures/fig1.png)"


PROBE = "x\n<!-- page 2 route=digital status=ok -->\r\n  <!-- draft\r<img src=x onerror=alert(1)> a\\<b &amp; \\\\<i>"

# The probe in every text field each element class renders.
PROBED_FIELDS = [
    (Heading, {"text": PROBE}),
    (Paragraph, {"text": PROBE}),
    (KeyValue, {"key": PROBE, "value": PROBE}),
    (BulletList, {"items": (PROBE, PROBE)}),
    (Table, {"headers": (PROBE,), "rows": ((PROBE,),)}),
    (Figure, {"caption": PROBE}),
    (Figure, {"caption": PROBE, "image_path": Path("figures/fig1.png")}),
]

# A "<" or "&" with an even (possibly empty) backslash run before it is live HTML.
_LIVE_HTML_OPENER = re.compile(r"(?<!\\)(?:\\\\)*[<&]")
# datagrunt's own <br> (a GFM cell line break), possibly after escaped backslashes.
_OWN_BR = re.compile(r"(?<!\\)((?:\\\\)*)<br>")


@pytest.mark.parametrize(("cls", "fields"), PROBED_FIELDS, ids=lambda value: getattr(value, "__name__", ""))
def test_no_element_text_opens_raw_html(cls, fields):
    lines = re.split(r"\r\n|\r|\n", _OWN_BR.sub(r"\1", _element(cls, **fields).to_markdown()))
    own_comment = f"<!-- figure p0001_e001 sha256={IMAGE_SHA[:8]} -->"
    text_lines = [line for line in lines if line != own_comment]
    assert [line for line in text_lines if line.lstrip().startswith("<")] == []
    assert [line for line in text_lines if _LIVE_HTML_OPENER.search(line)] == []


VISIBLE_MARKDOWN = {
    Heading: "## Overview",
    Paragraph: "Body text.",
    KeyValue: "**Invoice:** 42",
    BulletList: "- one\n- two",
    Table: "| a | b |\n| --- | --- |\n| 1 | 2 |",
    Figure: f"<!-- figure p0001_e001 sha256={hashlib.sha256(b'png-bytes').hexdigest()[:8]} -->",
}


@pytest.mark.parametrize("cls", ELEMENT_CLASSES)
class TestHiddenMarker:
    def test_visible_element_has_no_marker(self, cls):
        assert _element(cls).to_markdown() == VISIBLE_MARKDOWN[cls]

    def test_hidden_element_starts_with_hidden_marker(self, cls):
        hidden = _element(cls, hidden_reasons=("invisible", "tiny_font"))
        assert hidden.to_markdown() == "<!-- hidden: invisible,tiny_font -->\n" + VISIBLE_MARKDOWN[cls]

    def test_single_reason_marker(self, cls):
        hidden = _element(cls, hidden_reasons=("transparent",))
        assert hidden.to_markdown().split("\n", 1)[0] == "<!-- hidden: transparent -->"


def test_base_element_without_renderer_fails_loud():
    with pytest.raises(NotImplementedError, match="_Element must implement _render_markdown"):
        elements_module._Element(**_base()).to_markdown()


ALT_PATH = Path("figures/fig1.png")
FIGURE_COMMENT = f"<!-- figure p0001_e001 sha256={hashlib.sha256(b'png-bytes').hexdigest()[:8]} -->"


class TestMarkdownLeadingWhitespace:
    """Class A: Markdown cannot display line-edge whitespace; leading spaces would indent into code or a list."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("  leading two spaces", "leading two spaces"),
            ("    four spaces code", "four spaces code"),
            ("\tTab start", "Tab start"),
            ("line1\n    line2 indented", "line1\nline2 indented"),
            ("hard  \nbreak", "hard\nbreak"),  # two trailing spaces would be a hard line break
            ("a\n\n  \nb", "a\nb"),  # a blank line would split the paragraph
        ],
    )
    def test_paragraph(self, text, expected):
        paragraph = _element(Paragraph, text=text)
        assert paragraph.to_markdown() == expected
        assert paragraph.text == text  # stored data is unchanged

    def test_list_item_continuation_not_indented_into_code(self):
        bullets = _element(BulletList, items=("    code\n      more",), ordered=False)
        assert bullets.to_markdown() == "- code\n  more"

    def test_figure_caption(self):
        assert _element(Figure, caption="\tTab start").to_markdown() == f"{FIGURE_COMMENT}\nTab start"

    def test_key_value_value_lines(self):
        assert _element(KeyValue, key="  K ", value="  v\n    w").to_markdown() == "**K:** v\nw"

    def test_heading_and_table_cell(self):
        assert _element(Heading, text="  a\n   b  ").to_markdown() == "## a b"
        table = _element(Table, headers=("h",), rows=(("  a\n\n   b  ",),))
        assert table.to_markdown() == "| h |\n| --- |\n| a<br>b |"


class TestMarkdownLineStartMarkers:
    """Class B: list, thematic-break, and setext markers at a line start are escaped."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("2) paren", "2\\) paren"),
            ("10. ten", "10\\. ten"),
            ("___", "\\_\\_\\_"),
            ("***", "\\*\\*\\*"),
            ("+ plus", "\\+ plus"),
            ("= =", "\\= ="),
            ("line1\n---", "line1\n\\---"),
            ("line1\n===", "line1\n\\==="),
            ("1999 was a year", "1999 was a year"),  # digits alone are not a list marker
        ],
    )
    def test_paragraph(self, text, expected):
        assert _element(Paragraph, text=text).to_markdown() == expected

    def test_list_item_and_figure_caption(self):
        assert _element(BulletList, items=("2) x", "___"), ordered=False).to_markdown() == "- 2\\) x\n- \\_\\_\\_"
        assert _element(Figure, caption="2) x\n___").to_markdown() == f"{FIGURE_COMMENT}\n2\\) x\n\\_\\_\\_"


class TestMarkdownInlineSyntax:
    """Class C: emphasis, links, code spans, strikethrough, and fences are shown as literal text."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("*emph* _u_", "\\*emph\\* \\_u\\_"),
            ("[link](http://x)", "\\[link\\](http://x)"),
            ("`code`", "\\`code\\`"),
            ("~~gone~~", "\\~\\~gone\\~\\~"),
            ("a > b", "a \\> b"),
            ("C# and F#", "C\\# and F\\#"),
        ],
    )
    def test_paragraph(self, text, expected):
        assert _element(Paragraph, text=text).to_markdown() == expected

    def test_heading(self):
        assert _element(Heading, text="*emph* [l](u) `c`").to_markdown() == "## \\*emph\\* \\[l\\](u) \\`c\\`"

    def test_key_value_fence_key(self):
        kv = _element(KeyValue, key="```fence", value="_v_")
        assert kv.to_markdown() == "**\\`\\`\\`fence:** \\_v\\_"

    def test_list_item(self):
        assert _element(BulletList, items=("`code` *x*",), ordered=False).to_markdown() == "- \\`code\\` \\*x\\*"

    def test_table_cell(self):
        table = _element(Table, headers=("h",), rows=(("*emph* [l](u) `c`",),))
        assert table.to_markdown() == "| h |\n| --- |\n| \\*emph\\* \\[l\\](u) \\`c\\` |"

    def test_figure_alt_text(self):
        figure = _element(Figure, caption="*emph* `code`", image_path=ALT_PATH)
        assert figure.to_markdown() == "![\\*emph\\* \\`code\\`](figures/fig1.png)"


class TestMarkdownBackslashes:
    """Class D: every literal backslash is doubled, so none escapes the next character or makes a hard break."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("a\\|b", "a\\\\\\|b"),
            ("end\\\nnext", "end\\\\\nnext"),
            ("trailing backslash\\", "trailing backslash\\\\"),
            ("\\", "\\\\"),
            ("C:\\dir\\file", "C:\\\\dir\\\\file"),
        ],
    )
    def test_paragraph(self, text, expected):
        assert _element(Paragraph, text=text).to_markdown() == expected

    def test_heading_list_value_and_caption(self):
        assert _element(Heading, text="a\\|b").to_markdown() == "## a\\\\\\|b"
        assert _element(BulletList, items=("end\\\nnext",), ordered=False).to_markdown() == "- end\\\\\n  next"
        assert _element(KeyValue, key="k", value="C:\\dir").to_markdown() == "**k:** C:\\\\dir"
        assert _element(Figure, caption="end\\").to_markdown() == f"{FIGURE_COMMENT}\nend\\\\"

    def test_table_cell_backslash_not_before_a_pipe(self):
        table = _element(Table, headers=("h",), rows=(("C:\\dir",),))
        assert table.to_markdown() == "| h |\n| --- |\n| C:\\\\dir |"
