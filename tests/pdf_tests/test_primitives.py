"""Tests for the v5 PDF primitives: boundary validators, BBox, PageQuality, DocumentMetadata."""

import dataclasses
import hashlib
import math
import re
import unicodedata

import pytest

from datagrunt.pdf import BBox, DocumentMetadata, PageQuality
from datagrunt.pdf._validate import (
    choice_str,
    finite_float,
    finite_tuple,
    int_at_least,
    non_blank_str,
    optional_str,
    ordered_box,
    ordered_interval,
    positive_float,
    rgba_tuple,
    sha256_hex,
    str_tuple,
    strict_bool,
    typed_tuple,
)

NON_FINITE = [math.nan, math.inf, -math.inf]
BBOX_FIELDS = ["x0", "y0", "x1", "y1"]


class TestIntAtLeast:
    @pytest.mark.parametrize("value", [0, 1, 10**30])
    def test_returns_value_at_or_above_minimum(self, value):
        assert int_at_least("count", value, 0) == value

    def test_minimum_minus_one_rejected(self):
        with pytest.raises(ValueError, match="count must be >= 1, got 0"):
            int_at_least("count", 0, 1)

    @pytest.mark.parametrize("bad", [True, False])
    def test_rejects_bool(self, bad):
        with pytest.raises(TypeError, match="count"):
            int_at_least("count", bad, 0)

    @pytest.mark.parametrize("bad", [1.0, "1", None, [1]])
    def test_rejects_non_int(self, bad):
        with pytest.raises(TypeError, match="count"):
            int_at_least("count", bad, 0)


class TestFiniteFloat:
    def test_int_is_coerced_to_float(self):
        result = finite_float("x", 3)
        assert result == 3.0
        assert type(result) is float

    def test_float_returned_unchanged(self):
        assert finite_float("x", -2.5) == -2.5

    @pytest.mark.parametrize("bad", [True, False])
    def test_rejects_bool(self, bad):
        with pytest.raises(TypeError, match="x"):
            finite_float("x", bad)

    @pytest.mark.parametrize("bad", ["1.0", None, [1.0], complex(1, 0)])
    def test_rejects_non_number(self, bad):
        with pytest.raises(TypeError, match="x"):
            finite_float("x", bad)

    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_rejects_non_finite(self, bad):
        with pytest.raises(ValueError, match="x must be finite"):
            finite_float("x", bad)

    def test_rejects_int_too_large_for_float(self):
        with pytest.raises(ValueError, match="x must be finite"):
            finite_float("x", 10**400)

    @pytest.mark.parametrize("value", [0.0, 0.5, 1.0])
    def test_bounds_are_inclusive(self, value):
        assert finite_float("ratio", value, lo=0.0, hi=1.0) == value

    def test_below_lo_rejected(self):
        with pytest.raises(ValueError, match="ratio must be >= 0.0"):
            finite_float("ratio", -0.0001, lo=0.0)

    def test_above_hi_rejected(self):
        with pytest.raises(ValueError, match="ratio must be <= 1.0"):
            finite_float("ratio", 1.0001, hi=1.0)


class TestNonBlankStr:
    def test_returns_value_verbatim(self):
        assert non_blank_str("text", "  padded  ") == "  padded  "

    @pytest.mark.parametrize("bad", ["", " ", "\n\t", "　"])
    def test_rejects_blank(self, bad):
        with pytest.raises(ValueError, match="text must be a non-blank str"):
            non_blank_str("text", bad)

    @pytest.mark.parametrize("bad", [None, 1, b"bytes"])
    def test_rejects_non_str(self, bad):
        with pytest.raises(TypeError, match="text"):
            non_blank_str("text", bad)


class TestChoiceStr:
    @pytest.mark.parametrize("value", ["h", "v"])
    def test_returns_a_listed_choice(self, value):
        assert choice_str("orientation", value, ("h", "v")) == value

    def test_rejects_an_unlisted_str_naming_every_choice(self):
        with pytest.raises(ValueError, match="orientation must be 'h' or 'v', got 'x'"):
            choice_str("orientation", "x", ("h", "v"))

    @pytest.mark.parametrize("bad", [None, 1, b"h"])
    def test_rejects_non_str(self, bad):
        with pytest.raises(TypeError, match="orientation must be a str"):
            choice_str("orientation", bad, ("h", "v"))


class TestFiniteTuple:
    def test_returns_floats(self):
        assert finite_tuple("point", (1, 2.5), 2) == (1.0, 2.5)

    def test_rejects_non_tuple(self):
        with pytest.raises(TypeError, match="point must be a tuple, got list"):
            finite_tuple("point", [1.0, 2.0], 2)

    def test_rejects_wrong_size(self):
        with pytest.raises(ValueError, match="point must have 2 values, got 3"):
            finite_tuple("point", (1.0, 2.0, 3.0), 2)

    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_rejects_non_finite_entry_naming_index(self, bad):
        with pytest.raises(ValueError, match=r"point\[1\] must be finite"):
            finite_tuple("point", (1.0, bad), 2)


class TestStrictBool:
    @pytest.mark.parametrize("value", [True, False])
    def test_returns_bool(self, value):
        assert strict_bool("flag", value) is value

    @pytest.mark.parametrize("bad", [0, 1, None, "true"])
    def test_rejects_non_bool(self, bad):
        with pytest.raises(TypeError, match="flag must be a bool"):
            strict_bool("flag", bad)


class TestRgbaTuple:
    @pytest.mark.parametrize("value", [(0, 0, 0, 0), (255, 255, 255, 255), (1, 2, 3, 4)])
    def test_returns_channels(self, value):
        assert rgba_tuple("fill", value) == value

    def test_rejects_non_tuple(self):
        with pytest.raises(TypeError, match="fill must be a tuple, got list"):
            rgba_tuple("fill", [0, 0, 0, 0])

    def test_rejects_wrong_channel_count(self):
        with pytest.raises(ValueError, match="fill must have 4 channels, got 3"):
            rgba_tuple("fill", (0, 0, 0))

    @pytest.mark.parametrize("bad", [True, 1.0, "1"])
    def test_rejects_non_int_channel_naming_index(self, bad):
        with pytest.raises(TypeError, match=r"fill\[2\] must be an int"):
            rgba_tuple("fill", (0, 0, bad, 0))

    @pytest.mark.parametrize("bad", [-1, 256])
    def test_rejects_channel_out_of_range(self, bad):
        with pytest.raises(ValueError, match=r"fill\[3\] must be in 0..255"):
            rgba_tuple("fill", (0, 0, 0, bad))


class TestSha256Hex:
    def test_returns_digest(self):
        digest = hashlib.sha256(b"x").hexdigest()

        assert sha256_hex("sha256", digest) == digest

    @pytest.mark.parametrize(
        "bad",
        [
            pytest.param(hashlib.sha256(b"x").hexdigest().upper(), id="uppercase"),
            pytest.param("a" * 63, id="short"),
            pytest.param("a" * 65, id="long"),
            pytest.param("g" * 64, id="not-hex"),
            pytest.param("a" * 64 + "\n", id="trailing-newline"),
        ],
    )
    def test_rejects_malformed_digest(self, bad):
        with pytest.raises(ValueError, match="sha256 must be 64 lowercase hex characters"):
            sha256_hex("sha256", bad)

    @pytest.mark.parametrize("bad", [None, b"a" * 64])
    def test_rejects_non_str(self, bad):
        with pytest.raises(TypeError, match="sha256 must be a str"):
            sha256_hex("sha256", bad)


class TestOrderedBox:
    @pytest.mark.parametrize("value", [(0, 0, 10, 20), (5.0, 5.0, 5.0, 5.0), (-3.0, -2.0, -1.0, 0.0)])
    def test_returns_floats_edges_in_order(self, value):
        assert ordered_box("bbox", value) == tuple(float(v) for v in value)

    @pytest.mark.parametrize("bad", [(10.0, 0.0, 0.0, 20.0), (0.0, 20.0, 10.0, 0.0)], ids=["x-reversed", "y-reversed"])
    def test_rejects_reversed_edges(self, bad):
        with pytest.raises(ValueError, match="bbox must have x0 <= x1 and y0 <= y1"):
            ordered_box("bbox", bad)

    def test_rejects_non_finite(self):
        with pytest.raises(ValueError, match=r"bbox\[0\] must be finite"):
            ordered_box("bbox", (math.nan, 0.0, 1.0, 1.0))


class TestOrderedInterval:
    @pytest.mark.parametrize("value", [(0, 10), (5.0, 5.0), (-3.0, -1.0)])
    def test_returns_floats_start_then_end(self, value):
        assert ordered_interval("span", value) == tuple(float(v) for v in value)

    def test_rejects_a_start_past_its_end(self):
        with pytest.raises(ValueError, match=re.escape("span must have start <= end, got (10.0, 0.0)")):
            ordered_interval("span", (10, 0))

    @pytest.mark.parametrize(
        ("bad", "error", "message"),
        [
            ([0.0, 1.0], TypeError, "span must be a tuple, got list"),
            ((0.0, 1.0, 2.0), ValueError, "span must have 2 values, got 3"),
            ((0.0, math.inf), ValueError, r"span[1] must be finite"),
            ((True, 1.0), TypeError, r"span[0] must be an int or float"),
        ],
        ids=["list", "three-values", "inf", "bool"],
    )
    def test_rejects_what_finite_tuple_rejects(self, bad, error, message):
        with pytest.raises(error, match=re.escape(message)):
            ordered_interval("span", bad)


class TestPositiveFloat:
    @pytest.mark.parametrize("value", [1, 0.001, 1e300])
    def test_returns_a_float_above_zero(self, value):
        assert positive_float("width_pt", value) == float(value)

    @pytest.mark.parametrize("bad", [0, 0.0, -1.0])
    def test_rejects_zero_and_negative(self, bad):
        with pytest.raises(ValueError, match="width_pt must be > 0"):
            positive_float("width_pt", bad)

    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_rejects_non_finite(self, bad):
        with pytest.raises(ValueError, match="width_pt must be finite"):
            positive_float("width_pt", bad)

    @pytest.mark.parametrize("bad", [True, "1", None])
    def test_rejects_non_number(self, bad):
        with pytest.raises(TypeError, match="width_pt"):
            positive_float("width_pt", bad)


class TestTypedTuple:
    def test_returns_the_tuple(self):
        value = (1, 2)

        assert typed_tuple("items", value, int) is value

    def test_rejects_non_tuple(self):
        with pytest.raises(TypeError, match="items must be a tuple, got list"):
            typed_tuple("items", [1], int)

    def test_rejects_wrong_item_type_naming_index(self):
        with pytest.raises(TypeError, match=re.escape("items[1] must be a int, got str")):
            typed_tuple("items", (1, "2"), int)


class TestStrTuple:
    @pytest.mark.parametrize("value", [(), ("a",), ("a", "b", "c")])
    def test_accepts_tuple_of_str(self, value):
        assert str_tuple("items", value) == value

    @pytest.mark.parametrize("bad", [["a"], "a", None, {"a"}])
    def test_rejects_non_tuple(self, bad):
        with pytest.raises(TypeError, match="items must be a tuple"):
            str_tuple("items", bad)

    def test_rejects_non_str_entry_naming_index(self):
        with pytest.raises(TypeError, match=r"items\[1\]"):
            str_tuple("items", ("a", 2))

    def test_rejects_blank_entry_by_default(self):
        with pytest.raises(ValueError, match=r"items\[0\]"):
            str_tuple("items", (" ",))

    def test_allow_blank_accepts_blank_entries(self):
        assert str_tuple("cells", ("", " "), allow_blank=True) == ("", " ")


class TestOptionalStr:
    @pytest.mark.parametrize("value", [None, "", "title"])
    def test_accepts_none_or_str(self, value):
        assert optional_str("title", value) == value

    @pytest.mark.parametrize("bad", [1, b"t", ["t"]])
    def test_rejects_other_types(self, bad):
        with pytest.raises(TypeError, match="title"):
            optional_str("title", bad)


def _bbox(**overrides):
    kwargs = {"x0": 10, "y0": 20, "x1": 110, "y1": 70}
    kwargs.update(overrides)
    return BBox(**kwargs)


class TestBBox:
    def test_coerces_ints_to_float(self):
        box = _bbox()
        assert (box.x0, box.y0, box.x1, box.y1) == (10.0, 20.0, 110.0, 70.0)
        assert all(type(getattr(box, name)) is float for name in BBOX_FIELDS)

    def test_width_height_area(self):
        box = _bbox()
        assert box.width == 100.0
        assert box.height == 50.0
        assert box.area == 5000.0

    def test_zero_size_box_allowed(self):
        box = BBox(x0=5, y0=5, x1=5, y1=5)
        assert box.width == box.height == box.area == 0.0

    def test_negative_coordinates_allowed_for_off_page_spans(self):
        box = BBox(x0=-500, y0=-20, x1=-400, y1=-10)
        assert box.width == 100.0

    def test_inverted_x_rejected(self):
        with pytest.raises(ValueError, match="x0"):
            _bbox(x0=111)

    def test_inverted_y_rejected(self):
        with pytest.raises(ValueError, match="y0"):
            _bbox(y0=70.0001)

    @pytest.mark.parametrize("field", BBOX_FIELDS)
    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_non_finite_rejected(self, field, bad):
        with pytest.raises(ValueError, match=field):
            _bbox(**{field: bad})

    @pytest.mark.parametrize("field", BBOX_FIELDS)
    @pytest.mark.parametrize("bad", [True, "1", None])
    def test_wrong_type_rejected(self, field, bad):
        with pytest.raises(TypeError, match=field):
            _bbox(**{field: bad})

    def test_is_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            _bbox().x0 = 0.0

    def test_is_keyword_only(self):
        with pytest.raises(TypeError, match="positional argument"):
            BBox(0, 0, 1, 1)

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="x0 must be <= x1"):
            dataclasses.replace(_bbox(), x0=200)

    def test_is_slotted(self):
        assert not hasattr(_bbox(), "__dict__")

    def test_equal_and_hashable(self):
        assert _bbox() == _bbox(x0=10.0)
        assert hash(_bbox()) == hash(_bbox(x0=10.0))


class TestBBoxWithin:
    def test_inside_page(self):
        assert _bbox().within(612, 792)

    def test_exactly_on_page_edges(self):
        assert BBox(x0=0, y0=0, x1=612, y1=792).within(612, 792)

    def test_default_tolerance_is_half_a_point(self):
        assert BBox(x0=-0.5, y0=-0.5, x1=612.5, y1=792.5).within(612, 792)

    @pytest.mark.parametrize(
        "box",
        [
            BBox(x0=-0.51, y0=0, x1=10, y1=10),
            BBox(x0=0, y0=-0.51, x1=10, y1=10),
            BBox(x0=0, y0=0, x1=612.51, y1=10),
            BBox(x0=0, y0=0, x1=10, y1=792.51),
        ],
    )
    def test_just_beyond_tolerance_is_outside(self, box):
        assert not box.within(612, 792)

    def test_zero_tolerance_is_strict(self):
        box = BBox(x0=0, y0=0, x1=612.01, y1=10)
        assert box.within(612, 792)
        assert not box.within(612, 792, tolerance=0)

    def test_off_page_box_is_outside(self):
        assert not BBox(x0=-500, y0=-20, x1=-400, y1=-10).within(612, 792)

    @pytest.mark.parametrize(
        ("kwargs", "exc", "match"),
        [
            ({"width_pt": math.nan, "height_pt": 792}, ValueError, "width_pt must be finite"),
            ({"width_pt": 612, "height_pt": -1}, ValueError, "height_pt must be >= 0.0"),
            ({"width_pt": True, "height_pt": 792}, TypeError, "width_pt must be an int or float"),
            ({"width_pt": 612, "height_pt": 792, "tolerance": -0.1}, ValueError, "tolerance must be >= 0.0"),
            ({"width_pt": 612, "height_pt": 792, "tolerance": math.inf}, ValueError, "tolerance must be finite"),
        ],
    )
    def test_validates_arguments(self, kwargs, exc, match):
        with pytest.raises(exc, match=match):
            _bbox().within(**kwargs)


def _quality(**overrides):
    kwargs = {
        "valid_char_ratio": 0.98,
        "char_count": 1200,
        "vector_line_count": 14,
        "hidden_spans_flagged": 0,
        "rotation_deg": 0,
        "deskew_angle_deg": 0.0,
        "ocr_mean_confidence": None,
    }
    kwargs.update(overrides)
    return PageQuality(**kwargs)


COUNT_FIELDS = ["char_count", "vector_line_count", "hidden_spans_flagged"]


class TestPageQuality:
    def test_valid_construction(self):
        quality = _quality(ocr_mean_confidence=87)
        assert quality.char_count == 1200
        assert quality.ocr_mean_confidence == 87.0
        assert type(quality.ocr_mean_confidence) is float

    @pytest.mark.parametrize("ratio", [0, 1, 0.5])
    def test_ratio_bounds_inclusive_and_coerced(self, ratio):
        quality = _quality(valid_char_ratio=ratio)
        assert quality.valid_char_ratio == float(ratio)
        assert type(quality.valid_char_ratio) is float

    @pytest.mark.parametrize("bad", [1.0001, -0.0001, *NON_FINITE])
    def test_ratio_out_of_range_rejected(self, bad):
        with pytest.raises(ValueError, match="valid_char_ratio"):
            _quality(valid_char_ratio=bad)

    def test_ratio_bool_rejected(self):
        with pytest.raises(TypeError, match="valid_char_ratio"):
            _quality(valid_char_ratio=True)

    @pytest.mark.parametrize("field", COUNT_FIELDS)
    @pytest.mark.parametrize("value", [0, 1, 50_000])
    def test_counts_accept_zero_and_up(self, field, value):
        # A page with no characters has no valid-character ratio to report.
        ratio = None if (field, value) == ("char_count", 0) else 0.98
        assert getattr(_quality(**{field: value, "valid_char_ratio": ratio}), field) == value

    def test_ratio_is_none_for_a_page_without_characters(self):
        quality = _quality(char_count=0, valid_char_ratio=None)
        assert (quality.char_count, quality.valid_char_ratio) == (0, None)

    @pytest.mark.parametrize("ratio", [0, 0.0, 1.0])
    def test_ratio_without_characters_rejected(self, ratio):
        # 0 of 0 characters is not a ratio: reporting one would invent a value.
        with pytest.raises(ValueError, match="valid_char_ratio must be None when char_count is 0"):
            _quality(char_count=0, valid_char_ratio=ratio)

    @pytest.mark.parametrize("count", [1, 1200])
    def test_ratio_required_when_characters_exist(self, count):
        with pytest.raises(ValueError, match=f"valid_char_ratio is required when char_count is {count}"):
            _quality(char_count=count, valid_char_ratio=None)

    def test_deskew_none_when_not_measured(self):
        assert _quality(deskew_angle_deg=None).deskew_angle_deg is None

    @pytest.mark.parametrize("field", COUNT_FIELDS)
    def test_negative_count_rejected(self, field):
        with pytest.raises(ValueError, match=field):
            _quality(**{field: -1})

    @pytest.mark.parametrize("field", COUNT_FIELDS)
    @pytest.mark.parametrize("bad", [True, False, 1.0, "1"])
    def test_count_wrong_type_rejected(self, field, bad):
        with pytest.raises(TypeError, match=field):
            _quality(**{field: bad})

    @pytest.mark.parametrize("rotation", [0, 90, 180, 270])
    def test_right_angle_rotations_accepted(self, rotation):
        assert _quality(rotation_deg=rotation).rotation_deg == rotation

    @pytest.mark.parametrize("bad", [45, 1, 360, -90, 271])
    def test_other_rotations_rejected(self, bad):
        with pytest.raises(ValueError, match="rotation_deg"):
            _quality(rotation_deg=bad)

    @pytest.mark.parametrize("bad", [True, 90.0, "90"])
    def test_rotation_wrong_type_rejected(self, bad):
        with pytest.raises(TypeError, match="rotation_deg"):
            _quality(rotation_deg=bad)

    @pytest.mark.parametrize("angle", [-45, 45, 0, 1.25])
    def test_deskew_bounds_inclusive(self, angle):
        assert _quality(deskew_angle_deg=angle).deskew_angle_deg == float(angle)

    @pytest.mark.parametrize("bad", [-45.0001, 45.0001, *NON_FINITE])
    def test_deskew_out_of_range_rejected(self, bad):
        with pytest.raises(ValueError, match="deskew_angle_deg"):
            _quality(deskew_angle_deg=bad)

    def test_deskew_bool_rejected(self):
        with pytest.raises(TypeError, match="deskew_angle_deg"):
            _quality(deskew_angle_deg=False)

    @pytest.mark.parametrize("confidence", [None, 0, 100, 63.5])
    def test_ocr_confidence_none_or_in_range(self, confidence):
        expected = None if confidence is None else float(confidence)
        assert _quality(ocr_mean_confidence=confidence).ocr_mean_confidence == expected

    @pytest.mark.parametrize("bad", [-0.1, 100.0001, *NON_FINITE])
    def test_ocr_confidence_out_of_range_rejected(self, bad):
        with pytest.raises(ValueError, match="ocr_mean_confidence"):
            _quality(ocr_mean_confidence=bad)

    def test_ocr_confidence_bool_rejected(self):
        with pytest.raises(TypeError, match="ocr_mean_confidence"):
            _quality(ocr_mean_confidence=True)

    def test_is_frozen_slotted_and_keyword_only(self):
        quality = _quality()
        with pytest.raises(dataclasses.FrozenInstanceError):
            quality.char_count = 0
        assert not hasattr(quality, "__dict__")
        with pytest.raises(TypeError, match="positional argument"):
            PageQuality(0.9, 1, 1, 0, 0, 0.0, None)

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="rotation_deg"):
            dataclasses.replace(_quality(), rotation_deg=45)


SHA = hashlib.sha256(b"datagrunt").hexdigest()


def _metadata(**overrides):
    kwargs = {
        "doc_id": f"report__{SHA[:16]}",
        "source_name": "report.pdf",
        "sha256": SHA,
        "page_count": 3,
    }
    kwargs.update(overrides)
    return DocumentMetadata(**kwargs)


class TestMakeDocId:
    @pytest.mark.parametrize(
        ("source_name", "stem"),
        [
            ("My Report (v2).final.pdf", "My_Report_v2_final"),
            ("__.pdf", "document"),
            ("日本語.pdf", "document"),
            ("report.pdf", "report"),
            ("ABC-def.PDF", "ABC-def"),
            ("/tmp/scans/invoice 42.pdf", "invoice_42"),
            ("README", "README"),
            ("a__b___c.pdf", "a_b_c"),
            ("--x--.pdf", "--x--"),
            ("café.pdf", "cafe"),  # NFKD: the accent is a combining mark, dropped
            (".", "document"),
        ],
    )
    def test_sanitized_stem_plus_sha_prefix(self, source_name, stem):
        assert DocumentMetadata.make_doc_id(source_name, SHA) == f"{stem}__{SHA[:16]}"

    @pytest.mark.parametrize("source_name", ["My Report (v2).final.pdf", "__.pdf", "a__b.pdf", "日本語.pdf"])
    def test_separator_is_unambiguous(self, source_name):
        doc_id = DocumentMetadata.make_doc_id(source_name, SHA)
        stem, sha_prefix = doc_id.split("__")
        assert stem and "_" not in (stem[0], stem[-1])
        assert sha_prefix == SHA[:16]

    def test_concrete_doc_id(self):
        # Pinned literal: a change to the derivation must show up here, not just stay self-consistent.
        assert DocumentMetadata.make_doc_id("x.pdf", SHA) == "x__34a61eb5cc80969e"

    def test_suffix_is_sixteen_hex_characters(self):
        # 8 hex characters collide ~69% across 100k files sharing a stem (e.g. every
        # non-Latin name becomes "document"); 16 makes that negligible.
        assert DocumentMetadata.make_doc_id("日本語.pdf", SHA).split("__")[1] == SHA[:16]

    def test_nfc_and_nfd_names_give_the_same_id(self):
        nfc = unicodedata.normalize("NFC", "Résumé Café.pdf")
        nfd = unicodedata.normalize("NFD", "Résumé Café.pdf")
        assert nfc != nfd
        assert DocumentMetadata.make_doc_id(nfc, SHA) == DocumentMetadata.make_doc_id(nfd, SHA)
        assert DocumentMetadata.make_doc_id(nfd, SHA) == f"Resume_Cafe__{SHA[:16]}"

    @pytest.mark.parametrize(
        ("source_name", "stem"),
        [
            ("ﬁnal ＲＥＰＯＲＴ.pdf", "final_REPORT"),  # NFKD folds the ligature and full-width letters
            ("naïve-Ångström.pdf", "naive-Angstrom"),
        ],
    )
    def test_compatibility_forms_folded_to_ascii(self, source_name, stem):
        assert DocumentMetadata.make_doc_id(source_name, SHA) == f"{stem}__{SHA[:16]}"

    @pytest.mark.parametrize(("length", "kept"), [(63, 63), (64, 64), (65, 64), (500, 64)])
    def test_stem_capped_at_sixty_four_characters(self, length, kept):
        assert DocumentMetadata.make_doc_id("a" * length + ".pdf", SHA) == f"{'a' * kept}__{SHA[:16]}"

    def test_cap_never_leaves_a_trailing_underscore(self):
        # The cut lands right after a "_": stripping it keeps the "__" separator unambiguous.
        doc_id = DocumentMetadata.make_doc_id("a" * 63 + " b" * 10 + ".pdf", SHA)
        assert doc_id == f"{'a' * 63}__{SHA[:16]}"

    @pytest.mark.parametrize(
        "source_name",
        [
            "report.pdf",
            "/scans/report.pdf",
            "scans/report.pdf",
            "C:\\scans\\report.pdf",
            "\\\\server\\share\\report.pdf",
            "C:\\scans/mixed\\report.pdf",
        ],
    )
    def test_same_doc_id_for_posix_and_windows_paths(self, source_name):
        # OS-independent: the doc_id feeds cache keys and bundle names.
        assert DocumentMetadata.make_doc_id(source_name, SHA) == f"report__{SHA[:16]}"

    @pytest.mark.parametrize(
        ("source_name", "stem"),
        [
            ("report.", "report"),  # trailing dot: the empty extension is dropped
            (".hidden", "hidden"),  # a leading dot is not an extension separator
            (".hidden.pdf", "hidden"),
            ("scans/", "document"),  # nothing after the last separator
        ],
    )
    def test_dot_and_separator_edge_names(self, source_name, stem):
        assert DocumentMetadata.make_doc_id(source_name, SHA) == f"{stem}__{SHA[:16]}"

    @pytest.mark.parametrize("bad_sha", [SHA.upper(), SHA[:63], SHA + "0", "g" * 64])
    def test_rejects_bad_sha(self, bad_sha):
        with pytest.raises(ValueError, match="sha256"):
            DocumentMetadata.make_doc_id("x.pdf", bad_sha)

    @pytest.mark.parametrize(("source_name", "exc"), [("", ValueError), ("  ", ValueError), (None, TypeError)])
    def test_rejects_bad_source_name(self, source_name, exc):
        with pytest.raises(exc, match="source_name"):
            DocumentMetadata.make_doc_id(source_name, SHA)


class TestDocumentMetadata:
    def test_valid_construction_with_defaults(self):
        meta = _metadata()
        assert meta.doc_id == f"report__{SHA[:16]}"
        assert meta.title is None
        assert meta.author is None

    def test_title_and_author(self):
        meta = _metadata(title="Annual Report", author="")
        assert (meta.title, meta.author) == ("Annual Report", "")

    @pytest.mark.parametrize("pages", [0, 1, 5000])
    def test_page_count_zero_and_up(self, pages):
        assert _metadata(page_count=pages).page_count == pages

    def test_negative_page_count_rejected(self):
        with pytest.raises(ValueError, match="page_count"):
            _metadata(page_count=-1)

    @pytest.mark.parametrize("bad", [True, 3.0, "3"])
    def test_page_count_wrong_type_rejected(self, bad):
        with pytest.raises(TypeError, match="page_count"):
            _metadata(page_count=bad)

    @pytest.mark.parametrize("bad_sha", [SHA.upper(), SHA[:63], SHA + "a", SHA[:63] + "z", SHA + "\n"])
    def test_bad_sha_rejected(self, bad_sha):
        with pytest.raises(ValueError, match="sha256"):
            _metadata(sha256=bad_sha)

    def test_sha_wrong_type_rejected(self):
        with pytest.raises(TypeError, match="sha256"):
            _metadata(sha256=SHA.encode())

    @pytest.mark.parametrize("bad_id", [f"other__{SHA[:16]}", f"Report__{SHA[:16]}", "report__0000000000000000", ""])
    def test_doc_id_mismatch_rejected(self, bad_id):
        with pytest.raises(ValueError, match="doc_id"):
            _metadata(doc_id=bad_id)

    def test_doc_id_wrong_type_rejected(self):
        with pytest.raises(TypeError, match="doc_id"):
            _metadata(doc_id=None)

    @pytest.mark.parametrize(("bad", "exc"), [("", ValueError), (" ", ValueError), (7, TypeError)])
    def test_bad_source_name_rejected(self, bad, exc):
        with pytest.raises(exc, match="source_name"):
            _metadata(source_name=bad)

    @pytest.mark.parametrize("field", ["title", "author"])
    def test_title_author_wrong_type_rejected(self, field):
        with pytest.raises(TypeError, match=field):
            _metadata(**{field: 1})

    def test_is_frozen_slotted_and_keyword_only(self):
        meta = _metadata()
        with pytest.raises(dataclasses.FrozenInstanceError):
            meta.page_count = 9
        assert not hasattr(meta, "__dict__")
        with pytest.raises(TypeError, match="positional argument"):
            DocumentMetadata(f"report__{SHA[:16]}", "report.pdf", SHA, 3, None, None)

    def test_replace_revalidates(self):
        # Renaming the source without re-deriving doc_id must be caught.
        with pytest.raises(ValueError, match="doc_id"):
            dataclasses.replace(_metadata(), source_name="other.pdf")
