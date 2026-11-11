"""TDD regression and adversarial suite for Carbon 5.3.5 Section 3.2 CAD 3 Tr residual polish."""

from __future__ import annotations

import ast
import inspect
import json
import sys
from pathlib import Path
from types import FrameType
from typing import Any

import pytest
from reportlab.pdfgen import canvas

from datagrunt.pdf import quality
from datagrunt.pdf.corpus import extract_corpus
from datagrunt.pdf.pdfium_engine import RawSpan
from datagrunt.pdf.quality import classify_spans

PAGE_W, PAGE_H = 842.0, 595.0
BLOCKS = (ast.For, ast.AsyncFor, ast.While, ast.If, ast.With, ast.AsyncWith, ast.Try, ast.Match)


def _union(boxes: tuple[tuple[float, float, float, float], ...]) -> tuple[float, float, float, float]:
    return (
        min(b[0] for b in boxes),
        min(b[1] for b in boxes),
        max(b[2] for b in boxes),
        max(b[3] for b in boxes),
    )


def _row_boxes(
    count: int,
    x0: float = 60.0,
    y0: float = 100.0,
    *,
    cw: float = 6.0,
    ch_h: float = 10.0,
) -> tuple[tuple[float, float, float, float], ...]:
    return tuple((x0 + i * cw, y0, x0 + (i + 1) * cw, y0 + ch_h) for i in range(count))


def _tr3_span(
    text: str,
    x0: float = 60.0,
    y0: float = 100.0,
    *,
    cw: float = 6.0,
    ch_h: float = 10.0,
    object_index: int = 0,
) -> RawSpan:
    boxes = _row_boxes(len(text), x0, y0, cw=cw, ch_h=ch_h)
    return RawSpan(
        text=text,
        bbox=_union(boxes),
        char_boxes=boxes,
        tight_boxes=boxes,
        font_size_pt=ch_h,
        em_width_pt=ch_h,
        base_font_name="Courier",
        is_bold=False,
        is_italic=False,
        render_mode=3,
        fill_rgba=(0, 0, 0, 255),
        stroke_rgba=(0, 0, 0, 255),
        object_index=object_index,
        generated_breaks=(),
        map_errors=(),
    )


def _shifted_strokes(
    text: str,
    dx: float,
    x0: float = 60.0,
    y0: float = 100.0,
    *,
    cw: float = 6.0,
    sw: float = 3.2,
    sh: float = 7.0,
) -> tuple[tuple[float, float, float, float], ...]:
    """Generate shifted SHX glyph stroke boxes for non-space chars of ``text``."""
    strokes: list[tuple[float, float, float, float]] = []
    for idx, ch in enumerate(text):
        if ch.isspace():
            continue
        gx = x0 + idx * cw + dx
        strokes.append((gx, y0 + 1.0, gx + sw, y0 + 1.0 + sh))
    return tuple(strokes)


def _visible_texts(
    *spans: RawSpan,
    glyph_boxes: tuple[tuple[float, float, float, float], ...],
) -> list[str]:
    pieces = classify_spans(spans, width_pt=PAGE_W, height_pt=PAGE_H, images=(), glyph_boxes=glyph_boxes)
    return [p.text for p in pieces if not p.hidden_reasons]


def _nesting(node: ast.AST, level: int = 0) -> int:
    """Deepest control-flow block nesting under ``node`` (``elif`` stays at its ``if`` level)."""
    deepest = level
    for field, value in ast.iter_fields(node):
        children = value if isinstance(value, list) else [value]
        for child in children:
            if not isinstance(child, ast.AST) or isinstance(child, (ast.FunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            elif_ = isinstance(node, ast.If) and field == "orelse" and len(children) == 1 and isinstance(child, ast.If)
            deepest = max(deepest, _nesting(child, level + (isinstance(child, BLOCKS) and not elif_)))
    return deepest


class TestCaseAPunctuatedSpecCodeEdgeDigits:
    """Case A: 1-char edge digit bridges ONLY on leading YYYY/NN/XX directives with SHX overhang."""

    @pytest.mark.parametrize(
        "code",
        [
            "2002/95/CE",
            "2006/42/EC",
            "1999/44/EC",
            "IEC-60204-1",
        ],
    )
    def test_leading_directive_digit_or_code_letter_bridged_with_rightward_overhang(self, code: str) -> None:
        span = _tr3_span(code)
        strokes = _shifted_strokes(code, dx=5.6)

        assert _visible_texts(span, glyph_boxes=strokes) == [code]

    @pytest.mark.parametrize(
        "code",
        [
            "2002/95/CE",
            "REV_02_A",
        ],
    )
    def test_trailing_code_letter_bridged_with_leftward_overhang(self, code: str) -> None:
        span = _tr3_span(code)
        strokes = _shifted_strokes(code, dx=-4.8)

        assert _visible_texts(span, glyph_boxes=strokes) == [code]

    def test_punctuated_code_without_overhang_does_not_bridge_leading_or_trailing_char(self) -> None:
        lead_span = _tr3_span("2002/95/CE")
        lead_strokes = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in range(1, 10))
        assert _visible_texts(lead_span, glyph_boxes=lead_strokes) == ["002/95/CE"]

        trail_span = _tr3_span("2002/95/CE")
        trail_strokes = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in range(0, 9))
        assert _visible_texts(trail_span, glyph_boxes=trail_strokes) == ["2002/95/C"]

    @pytest.mark.parametrize(
        ("token", "backed_indices", "overhang_box", "forbidden"),
        [
            ("-100", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "-100"),
            ("+100", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "+100"),
            ("\u2013100", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "\u2013100"),
            ("\u2212100", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "\u2212100"),
            ("\u00b1100", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "\u00b1100"),
            ("1000", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "1000"),
            ("1000", (0, 1, 2), (55.2, 101.0, 58.4, 108.0), "1000"),
            ("100k", (0, 1, 2), (55.2, 101.0, 58.4, 108.0), "100k"),
            ("100k", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "100k"),
            ("1100k", (1, 2, 3, 4), (90.8, 101.0, 94.0, 108.0), "1100k"),
            ("100mm", (1, 2, 3, 4), (90.8, 101.0, 94.0, 108.0), "100mm"),
            ("100mm", (0, 1, 2, 3), (55.2, 101.0, 58.4, 108.0), "100mm"),
            ("100.5", (1, 2, 3, 4), (90.8, 101.0, 94.0, 108.0), "100.5"),
            ("1,000", (1, 2, 3, 4), (90.8, 101.0, 94.0, 108.0), "1,000"),
            ("10:00", (1, 2, 3, 4), (90.8, 101.0, 94.0, 108.0), "10:00"),
            ("1:1000", (0, 1, 2, 3, 4), (55.2, 101.0, 58.4, 108.0), "1:1000"),
            ("12:30:05", (1, 2, 3, 4, 5, 6, 7), (108.8, 101.0, 112.0, 108.0), "12:30:05"),
            ("11/4", (1, 2, 3), (84.8, 101.0, 88.0, 108.0), "11/4"),
            ("11/05/2021", tuple(range(1, 10)), (120.8, 101.0, 124.0, 108.0), "11/05/2021"),
            ("10/200/500", tuple(range(0, 9)), (55.2, 101.0, 58.4, 108.0), "10/200/500"),
            ("CE/95/2002", tuple(range(0, 9)), (55.2, 101.0, 58.4, 108.0), "CE/95/2002"),
            ("1.25/CE", (2, 3, 4, 5, 6), (102.8, 101.0, 106.0, 108.0), "1.25/CE"),
            ("AB-1000", (0, 1, 2, 3, 4, 5), (55.2, 101.0, 58.4, 108.0), "AB-1000"),
            ("AB-1000", (1, 2, 3, 4, 5, 6), (102.8, 101.0, 106.0, 108.0), "AB-1000"),
            ("1000-AB", (0, 1, 2, 3, 4, 5), (55.2, 101.0, 58.4, 108.0), "1000-AB"),
            ("EN-60204-1", tuple(range(0, 9)), (55.2, 101.0, 58.4, 108.0), "EN-60204-1"),
            ("1_500_AB", tuple(range(1, 8)), (108.8, 101.0, 112.0, 108.0), "1_500_AB"),
            ("-2002/95/CE", tuple(range(2, 11)), (126.8, 101.0, 130.0, 108.0), "2002/95/CE"),
            ("+2002/95/CE", tuple(range(2, 11)), (126.8, 101.0, 130.0, 108.0), "2002/95/CE"),
            ("\u20132002/95/CE", tuple(range(2, 11)), (126.8, 101.0, 130.0, 108.0), "2002/95/CE"),
            ("\ufe632002/95/CE", tuple(range(2, 11)), (126.8, 101.0, 130.0, 108.0), "2002/95/CE"),
            ("\ufe622002/95/CE", tuple(range(2, 11)), (126.8, 101.0, 130.0, 108.0), "2002/95/CE"),
            ("(-2002/95/CE)", tuple(range(3, 13)), (138.8, 101.0, 142.0, 108.0), "2002/95/CE"),
            ("2002/-95/CE", (1, 2, 3, 4, 6, 7, 8, 9, 10), (126.8, 101.0, 130.0, 108.0), "2002"),
        ],
    )
    def test_numeric_scale_fraction_decimal_and_signed_tokens_blocked_even_with_overhang(
        self,
        token: str,
        backed_indices: tuple[int, ...],
        overhang_box: tuple[float, float, float, float],
        forbidden: str,
    ) -> None:
        span = _tr3_span(token)
        strokes = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in backed_indices)
        got = "".join(_visible_texts(span, glyph_boxes=(*strokes, overhang_box)))

        assert forbidden not in got, f"{forbidden!r} leaked in {got!r}"

    @pytest.mark.parametrize(
        "minus_ch", ["-", "+", "\u2013", "\u2212", "\u00b1", "\uff0d", "\uff0b", "\ufe63", "\u207b", "\u2213", "\u2796"]
    )
    def test_leading_ascii_and_unicode_sign_strokes_never_bracket_unbacked_digit(self, minus_ch: str) -> None:
        span = _tr3_span(f"{minus_ch}100")
        sign_stroke = (61.0, 102.0, 64.5, 106.5)
        zeros = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in (2, 3))

        got = "".join(_visible_texts(span, glyph_boxes=(sign_stroke, *zeros)))
        assert f"{minus_ch}100" not in got
        assert "100" not in got

    def test_interior_digit_unit_exponent_fraction_sign_and_chaining_stay_blocked(self) -> None:
        # 1) Missing interior digit, unit prefix, scientific exponent, or fraction slash
        blocked_interiors = (
            ("1000", (0, 1, 3), "1000"),
            ("AB-1000", (0, 1, 2, 3, 4, 6), "AB-1000"),
            ("10km", (0, 1, 3), "10km"),
            ("1e6", (0, 2), "1e6"),
            ("10MPa", (0, 1, 3, 4), "10MPa"),
            ("2x10", (0, 2, 3), "2x10"),
            ("11/4", (0, 1, 3), "11/4"),
            ("100.5", (0, 1, 2, 4), "100.5"),
            ("(-100)", (0, 2, 3, 4, 5), "(-100)"),
        )
        for token, idx_set, forbidden in blocked_interiors:
            s_tok = _tr3_span(token)
            b_tok = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in idx_set)
            got = "".join(_visible_texts(s_tok, glyph_boxes=b_tok))
            assert forbidden not in got, f"{forbidden!r} leaked in {got!r}"

        # 2) Synthetic edge-anchor chaining: "(+-2002/95/CE" with '(' (0) and "002/95/CE" (4..12) + overhang
        s_chain = _tr3_span("(+-2002/95/CE")
        b_chain = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in (0, *range(4, 13)))
        over = (138.8, 101.0, 142.0, 108.0)
        got_chain = "".join(_visible_texts(s_chain, glyph_boxes=(*b_chain, over)))
        assert "+-" not in got_chain
        assert "2002/95/CE" not in got_chain


class TestCaseBMultiWordSpanPolish:
    """Case B: Short 2-char words (1/2 + overhang) and 2-char alphabetic edge gaps in corroborated multi-word spans."""

    @pytest.mark.parametrize("short_word", ["TO", "IN", "AT", "OF", "IS", "LE"])
    @pytest.mark.parametrize("dx", [5.6, -4.8], ids=["right-drift", "left-drift"])
    def test_short_two_letter_words_bridged_in_qualifying_multi_word_span(self, short_word: str, dx: float) -> None:
        phrase = f"CONFORMS {short_word} STANDARD"
        span = _tr3_span(phrase)
        strokes = _shifted_strokes(phrase, dx=dx)

        assert _visible_texts(span, glyph_boxes=strokes) == [phrase]

    @pytest.mark.parametrize("phrase", ["TO OPERATING PANEL", "CONNECTED TO:", "IN NOMINAL MODE"])
    def test_short_two_letter_word_at_span_start_or_end_or_punctuated(self, phrase: str) -> None:
        span = _tr3_span(phrase)
        strokes = _shifted_strokes(phrase, dx=5.6)

        assert _visible_texts(span, glyph_boxes=strokes) == [phrase]

    def test_two_char_alphabetic_edge_drift_restores_embedded_and_list(self) -> None:
        # 1) "EMBEDDED" -> "EMBEDD" under span-corroborated leftward SHX drift (-10.8 pt and -4.8 pt)
        p_emb = "EMBEDDED CONTROLLER"
        span_emb = _tr3_span(p_emb)
        strokes_emb = (
            *_shifted_strokes("EMBEDDED", dx=-10.8, x0=60.0),
            *_shifted_strokes("CONTROLLER", dx=-4.8, x0=60.0 + 9 * 6.0),
        )
        assert _visible_texts(span_emb, glyph_boxes=strokes_emb) == [p_emb]

        # 2) "LIST" -> "ST" under cumulative rightward SHX expansion (+1.0 pt -> +5.6 pt -> +11.0 pt)
        p_list_end = "COMPONENT PART LIST"
        span_list_end = _tr3_span(p_list_end)
        strokes_list_end = (
            *_shifted_strokes("COMPONENT", dx=1.0, x0=60.0),
            *_shifted_strokes("PART", dx=5.6, x0=60.0 + 10 * 6.0),
            *_shifted_strokes("LIST", dx=11.0, x0=60.0 + 15 * 6.0),
        )
        assert _visible_texts(span_list_end, glyph_boxes=strokes_list_end) == [p_list_end]

        # 3) "LIST" -> "ST" before wide multi-space column gap in rightward-drifted span
        p_list_mid = "PART   LIST   AVAILABLE"
        span_list_mid = _tr3_span(p_list_mid)
        strokes_list_mid = (
            *_shifted_strokes("PART", dx=5.6, x0=60.0),
            *_shifted_strokes("LIST", dx=11.0, x0=60.0 + 7 * 6.0),
            *_shifted_strokes("AVAILABLE", dx=5.6, x0=60.0 + 14 * 6.0),
        )
        assert _visible_texts(span_list_mid, glyph_boxes=strokes_list_mid) == [p_list_mid]

    def test_span_gate_rejects_single_word_short_span_low_ratio_and_one_word_hit(self) -> None:
        assert _visible_texts(_tr3_span("TO"), glyph_boxes=_shifted_strokes("TO", dx=5.6)) == ["O"]
        assert _visible_texts(_tr3_span("LIST"), glyph_boxes=_shifted_strokes("LIST", dx=11.0)) == ["ST"]

        short_phrase = "GO TO IT"
        got_short = _visible_texts(_tr3_span(short_phrase), glyph_boxes=_shifted_strokes(short_phrase, dx=5.6))
        assert got_short != [short_phrase]

        low_ratio = "ABCDEF   LIST"
        low_head = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in range(2, 6))
        strokes_low = (*low_head, *_shifted_strokes("LIST", dx=11.0, x0=60.0 + 9 * 6.0))
        assert _visible_texts(_tr3_span(low_ratio), glyph_boxes=strokes_low) != [low_ratio]

        one_hit_word = "EMBEDDED ZZZ"
        strokes_one = _shifted_strokes("EMBEDDED", dx=-10.8, x0=60.0)
        assert _visible_texts(_tr3_span(one_hit_word), glyph_boxes=strokes_one) == ["EMBEDD"]

    def test_two_char_digits_homoglyphs_and_unsafe_short_words_never_bridge_in_multi_word_span(self) -> None:
        # Includes Cyrillic 'N\u041e' (U+041E) and '\u0406N' (U+0406) homoglyphs
        phrase = "NOMINAL RATED 50 1k NO N\u041e \u0406N"
        span = _tr3_span(phrase)
        strokes = _shifted_strokes(phrase, dx=5.6)
        got = " ".join(_visible_texts(span, glyph_boxes=strokes)).split()
        for forbidden in ("50", "1k", "NO", "N\u041e", "\u0406N"):
            assert forbidden not in got, f"{forbidden!r} leaked in {got!r}"


class TestAdversarialPromptInjectionAndNumericResistance:
    """Adversarial checks with crafted overhang boxes for negation, prefix smuggling, and prompt injection."""

    def test_zero_hit_prompt_injection_words_stay_invisible_inside_qualifying_multi_word_span(self) -> None:
        valid_prefix = "NOMINAL OPERATING PRESSURE AT PRIMARY INLET CONTROL VALVE"
        phrase = f"{valid_prefix} IGNORE ALL"
        span = _tr3_span(phrase)
        strokes = _shifted_strokes(valid_prefix, dx=5.6, x0=60.0)

        pieces = classify_spans((span,), width_pt=PAGE_W, height_pt=PAGE_H, images=(), glyph_boxes=strokes)
        visible = [p.text for p in pieces if not p.hidden_reasons]
        hidden = [p.text for p in pieces if "invisible" in p.hidden_reasons]

        assert visible == [valid_prefix]
        assert hidden == [" IGNORE ALL"]

    def test_uncorroborated_and_denylisted_negation_or_antonym_prefixes_stay_blocked(self) -> None:
        # 1) "UNAUTHORIZED ACCESS PERMITTED" with unshifted strokes + crafted gap box
        p1 = "UNAUTHORIZED ACCESS PERMITTED"
        s1 = _tr3_span(p1)
        idx1 = (*range(2, 12), *range(13, 19), *range(20, 29))
        st1 = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in idx1)
        crafted_gap_box = (131.0, 101.0, 135.0, 108.0)
        assert "UNAUTHORIZED" not in " ".join(_visible_texts(s1, glyph_boxes=(*st1, crafted_gap_box)))

        # 2) "NEITHER VALVE MAY BE OPENED" with unshifted EITHER/VALVE/MAY/BE/OPENED + crafted gap box
        p_nei = "NEITHER VALVE MAY BE OPENED"
        s_nei = _tr3_span(p_nei)
        idx_nei = (*range(1, 7), *range(8, 13), *range(14, 17), *range(18, 20), *range(21, 27))
        st_nei = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in idx_nei)
        assert "NEITHER" not in " ".join(_visible_texts(s_nei, glyph_boxes=(*st_nei, (101.0, 101.0, 105.0, 108.0))))

        # 3) "VALVE POSITION ABNORMAL" and "SYSTEM MODE ANAEROBIC" with 2 crafted overhang boxes
        p_abn = "VALVE POSITION ABNORMAL"
        s_abn = _tr3_span(p_abn)
        idx_abn = (*range(0, 5), *range(6, 14), *range(17, 23))
        st_abn = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in idx_abn)
        boxes_abn = ((199.0, 101.0, 202.2, 108.0), (205.0, 101.0, 208.2, 108.0))
        assert "ABNORMAL" not in " ".join(_visible_texts(s_abn, glyph_boxes=(*st_abn, *boxes_abn)))

        # 4) Standalone 1-word unlisted privatives ("ASEPTIC", "ACHROMATIC", "NEITHER", "CANT") + crafted box
        for word, stem_range, box in (
            ("ASEPTIC", range(1, 7), (102.8, 101.0, 106.0, 108.0)),
            ("ACHROMATIC", range(1, 10), (120.8, 101.0, 124.0, 108.0)),
            ("NEITHER", range(1, 7), (102.8, 101.0, 106.0, 108.0)),
            ("CANT", range(0, 3), (55.2, 101.0, 58.4, 108.0)),
        ):
            sw = _tr3_span(word)
            stw = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in stem_range)
            assert word not in " ".join(_visible_texts(sw, glyph_boxes=(*stw, box)))

        # 5) Multi-word crafted overhang boxes across CANT, NONE, NEVER, NOT, DONT, ASYMMETRIC
        p3 = "CANT   NONE   NEVER   NOT   DONT   ASYMMETRIC   OPERATING   SYSTEM"
        s3 = _tr3_span(p3)
        idx3 = (
            *range(0, 3),
            *range(8, 11),
            *range(15, 19),
            *range(23, 25),
            *range(28, 31),
            *range(36, 45),
            *range(48, 57),
            *range(60, 66),
        )
        st3 = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in idx3)
        crafted3 = (
            (55.0, 101.0, 58.2, 108.0),
            (60.0 + 11 * 6.0 + 0.8, 101.0, 60.0 + 11 * 6.0 + 4.0, 108.0),
            (60.0 + 19 * 6.0 + 0.8, 101.0, 60.0 + 19 * 6.0 + 4.0, 108.0),
            (60.0 + 25 * 6.0 + 0.8, 101.0, 60.0 + 25 * 6.0 + 4.0, 108.0),
            (60.0 + 28 * 6.0 - 4.8, 101.0, 60.0 + 28 * 6.0 - 1.6, 108.0),
            (60.0 + 45 * 6.0 + 0.8, 101.0, 60.0 + 45 * 6.0 + 4.0, 108.0),
        )
        got3 = " ".join(_visible_texts(s3, glyph_boxes=(*st3, *crafted3))).split()
        for forbidden in ("CANT", "NONE", "NEVER", "NOT", "DONT", "ASYMMETRIC"):
            assert forbidden not in got3, f"{forbidden!r} leaked in {got3!r}"

        # 6) "NOK" and "BYPASS" stay blocked even when peer words in the span genuinely shift
        p_nok = "TEST RESULT NOK"
        s_nok = _tr3_span(p_nok)
        st_nok = (
            *_shifted_strokes("TEST RESULT", dx=5.6, x0=60.0),
            *(tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in (13, 14))),
            (60.0 + 15 * 6.0 + 0.8, 101.0, 60.0 + 15 * 6.0 + 4.0, 108.0),
        )
        assert "NOK" not in " ".join(_visible_texts(s_nok, glyph_boxes=st_nok)).split()

        # 7) Punctuation-adjacent gaps without overhang must NOT fabricate "(UNSAFE)", "(NOT)", "NOT.", "NONE.", "CANT*"
        for tok, backed_i, bad in (
            ("(UNSAFE)", (0, 3, 4, 5, 6, 7), "UNSAFE"),
            ("(NOT)", (0, 2, 3, 4), "NOT"),
            ("NOT.", (0, 1, 3), "NOT"),
            ("NONE.", (0, 1, 2, 4), "NONE"),
            ("NOT*", (0, 1, 3), "NOT"),
            ("CANT*", (0, 1, 2, 4), "CANT"),
        ):
            st_p = tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in backed_i)
            got_p = "".join(_visible_texts(_tr3_span(tok), glyph_boxes=st_p))
            assert bad not in got_p, f"{bad!r} leaked in {got_p!r}"

    def test_known_bbox_pitch_alias_limit_when_peer_words_also_shift(self) -> None:
        # Known pure-stage bbox limit (documented in quality.py module notes): stem strokes at 1-pitch offset
        # plus an extra mark at the overhanging pitch share identical bounding boxes with a genuine 1-pitch SHX
        # shift when peer words in the same span also shift in that direction.
        span = _tr3_span("ASEPTIC CHAMBER")
        strokes = (
            *(tuple((60.0 + i * 6.0 + 1.0, 101.0, 60.0 + i * 6.0 + 4.2, 108.0) for i in range(1, 7))),
            (102.8, 101.0, 106.0, 108.0),
            *_shifted_strokes("CHAMBER", dx=5.6, x0=60.0 + 8 * 6.0),
        )
        assert _visible_texts(span, glyph_boxes=strokes) == ["ASEPTIC CHAMBER"]

    def test_end_to_end_blueprint_corpus_restores_polish_cases_and_blocks_all_attacks(self, tmp_path: Path) -> None:
        pdf_path = tmp_path / "carbon_535_cad_polish.pdf"
        c = canvas.Canvas(str(pdf_path), pagesize=(PAGE_W, PAGE_H))
        c.setStrokeColorRGB(0.08, 0.08, 0.08)
        c.setLineWidth(0.7)

        def emit_shifted_line(text_val: str, x_pt: float, y_pt: float, dx: float) -> None:
            t_obj = c.beginText(x_pt, y_pt)
            t_obj.setFont("Courier", 10)
            t_obj.setTextRenderMode(3)
            t_obj.textOut(text_val)
            c.drawText(t_obj)
            for i, ch in enumerate(text_val):
                if not ch.isspace():
                    gx = x_pt + i * 6.0 + dx
                    c.line(gx, y_pt + 1.0, gx + 3.2, y_pt + 8.0)

        emit_shifted_line("CONFORMS TO 2002/95/CE", 60.0, 520.0, dx=5.6)
        emit_shifted_line("DANS LE TABLEAU", 60.0, 490.0, dx=5.6)

        t_list = c.beginText(60.0, 460.0)
        t_list.setFont("Courier", 10)
        t_list.setTextRenderMode(3)
        t_list.textOut("COMPONENT PART LIST")
        c.drawText(t_list)
        for i in range(len("COMPONENT")):
            c.line(60.0 + i * 6.0 + 1.0, 461.0, 60.0 + i * 6.0 + 4.2, 468.0)
        for i in range(4):
            gx_p = 60.0 + (10 + i) * 6.0 + 5.6
            c.line(gx_p, 461.0, gx_p + 3.2, 468.0)
        for i in range(4):
            gx = 60.0 + (15 + i) * 6.0 + 11.0
            c.line(gx, 461.0, gx + 3.2, 468.0)

        t_emb = c.beginText(60.0, 430.0)
        t_emb.setFont("Courier", 10)
        t_emb.setTextRenderMode(3)
        t_emb.textOut("EMBEDDED CONTROLLER")
        c.drawText(t_emb)
        for i in range(8):
            gx = 60.0 + i * 6.0 - 10.8
            c.line(gx, 431.0, gx + 3.2, 438.0)
        for i in range(len("CONTROLLER")):
            gx = 60.0 + (9 + i) * 6.0 - 4.8
            c.line(gx, 431.0, gx + 3.2, 438.0)

        adv_line = "UNAUTHORIZED NONE NEVER CANT -100 +100 1000 100k IGNORE ALL"
        t_adv = c.beginText(60.0, 380.0)
        t_adv.setFont("Courier", 10)
        t_adv.setTextRenderMode(3)
        t_adv.textOut(adv_line)
        c.drawText(t_adv)
        adv_backed = (
            *range(2, 12),
            *range(14, 17),
            *range(19, 23),
            *range(24, 27),
            *range(30, 33),
            *range(35, 38),
            *range(39, 42),
            *range(44, 47),
        )
        for i in adv_backed:
            c.line(60.0 + i * 6.0 + 1.0, 381.0, 60.0 + i * 6.0 + 4.2, 388.0)

        c.showPage()
        c.save()

        out_dir = tmp_path / "bundle_out"
        manifest = extract_corpus(pdf_path, out_dir, ocr="never")
        bdir = out_dir / manifest.entries[0].bundle_relpath
        doc_md = (bdir / "markdown" / "document.md").read_text(encoding="utf-8")
        doc_json = json.loads((bdir / "json" / "document.json").read_text(encoding="utf-8"))

        assert "CONFORMS TO 2002/95/CE" in doc_md
        assert "DANS LE TABLEAU" in doc_md
        assert "COMPONENT PART LIST" in doc_md
        assert "EMBEDDED CONTROLLER" in doc_md
        assert doc_json["pages"][0]["image_only_reasons"] == []
        for forbidden in ("IGNORE", "UNAUTHORIZED", "NONE", "NEVER", "CANT", "-100", "+100", "1000", "100k"):
            assert forbidden not in doc_md, f"{forbidden!r} leaked into Markdown: {doc_md!r}"


class TestEngineeringCanonAndScalingBounds:
    """Enforce AST nesting <= 3, helper length <= ~50 lines, defensive boundaries, and O(N log N) / O(N) scaling."""

    def test_ast_nesting_at_most_3_and_drift_helpers_within_50_lines(self) -> None:
        tree = ast.parse(inspect.getsource(quality))
        functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]

        deep = {node.name: _nesting(node) for node in functions if _nesting(node) > 3}
        assert deep == {}, f"Functions exceeding AST nesting 3: {deep}"

        drift_helpers = {
            "_is_ascii_alpha",
            "_is_sign_or_dash",
            "_gap_is_bridgeable",
            "_bridge_short_drift_gaps",
            "_adjacent_word_bound",
            "_stroke_matches_overhang",
            "_word_has_drift_overhang",
            "_overhangs_edge",
            "_has_sign_prefix",
            "_is_eu_directive_lead_digit",
            "_edge_char_ok",
            "_two_letter_gap_ready",
            "_bridge_two_char_word",
            "_bridge_two_char_edge_drift",
            "_span_word_slices",
            "_multi_word_span_qualifies",
            "_word_drifts_toward",
            "_with_span_corroboration",
            "_bridge_single_word_edges",
            "_mark_word_backing",
            "_propagate_internal_spaces",
            "_char_vector_flags",
        }
        found = {node.name: (node.end_lineno or node.lineno) - node.lineno + 1 for node in functions}
        assert drift_helpers <= set(found)
        over_50 = {name: found[name] for name in drift_helpers if found[name] > 50}
        assert over_50 == {}, f"Drift functions exceeding 50 lines: {over_50}"

    def test_defensive_boundaries_fail_closed_on_degenerate_inputs(self) -> None:
        span = _tr3_span("TO BE")
        ctx = quality._SpanDriftCtx(span=span, bands=(), tr3_bands=(), height=PAGE_H, budget=[10])
        backed = [False] * len(span.text)

        assert quality._is_ascii_alpha("") is False
        assert quality._is_sign_or_dash("") is False
        assert quality._gap_is_bridgeable("ABC", [0, 1, 2], 0, 1, 2) is False
        quality._bridge_short_drift_gaps("", [0, 1, 2], backed)
        quality._bridge_short_drift_gaps("AB", [0, 1], backed)
        assert quality._adjacent_word_bound((0.0, 0.0, 5.0, 5.0), (), PAGE_H, [10], right_side=True) is None
        got_over = quality._word_has_drift_overhang((0.0, 0.0, 5.0, 5.0), "", (), (), PAGE_H, [10], right_side=True)
        assert got_over is False
        assert quality._overhangs_edge(ctx, -1, right_side=True) is False
        assert quality._overhangs_edge(ctx, 99, right_side=True) is False
        assert quality._has_sign_prefix("", [0], [0]) is False
        assert quality._is_eu_directive_lead_digit("2", "12", [0, 1], [0, 1], 0) is False
        assert quality._edge_char_ok("", "ABC", [0, 1, 2], [0, 1, 2], 2) is False
        assert quality._edge_char_ok("!", "ABC!", [0, 1, 2, 3], [0, 1, 2], 3) is False
        assert quality._two_letter_gap_ready("", [False, False, True], 0, 1, 2) is False
        quality._bridge_two_char_word(ctx, [], [], [], backed)
        quality._bridge_two_char_edge_drift(ctx, [], [], [], backed)
        assert quality._span_word_slices(span, 0.0, PAGE_H) == ()
        assert quality._multi_word_span_qualifies("", ([0], [2]), [True, False, True]) is False
        assert quality._word_drifts_toward(ctx, [], [], right_side=True) is False
        assert quality._with_span_corroboration(ctx, ([0],), [True]) == ctx
        quality._bridge_single_word_edges(ctx, [], [], [], backed)
        quality._mark_word_backing(ctx, [], [], backed)
        assert quality._propagate_internal_spaces(span, PAGE_W, PAGE_H, backed) == (False,) * len(span.text)

        off_span = _tr3_span("CONFORMS TO 2002/95/CE", x0=-500.0, y0=-200.0)
        bands = quality._glyph_bands(((10.0, 10.0, 14.0, 18.0),), PAGE_H)
        assert quality._char_vector_flags(off_span, PAGE_W, PAGE_H, bands, [100]) == (False,) * len(off_span.text)

    @pytest.mark.no_cover
    def test_single_token_gap_bridging_and_multi_word_classification_scale_linearly(self) -> None:
        def count_gap_lines(m: int) -> int:
            text = "." * (2 * m) + "A." * m
            on_page = list(range(len(text)))
            backed = [False] * (2 * m) + [i % 2 == 0 for i in range(2 * m)]
            events = [0]

            def local(frame: FrameType, event: str, arg: Any) -> Any:
                events[0] += event == "line"
                return local

            def tracer(frame: FrameType, event: str, arg: Any) -> Any:
                return local if frame.f_code.co_filename == quality.__file__ else None

            prev = sys.gettrace()
            sys.settrace(tracer)
            try:
                quality._bridge_short_drift_gaps(text, on_page, backed, alpha_hits=m)
            finally:
                sys.settrace(prev)
            return events[0]

        g_small = count_gap_lines(400)
        g_large = count_gap_lines(1600)
        # 4x single-token length: O(N) grows ~4.0x; O(N^2) grew ~16x.
        assert g_large / g_small < 4.5, f"Expected O(N) single-token gap scaling, got {g_small} -> {g_large}"

        def count_span_lines(word_groups: int) -> int:
            phrase = "CONFORMS TO 2002/95/CE EMBEDDED LIST"
            spans = tuple(_tr3_span(phrase, y0=10.0 + (k % 50) * 11.0, object_index=k) for k in range(word_groups))
            rows = [_shifted_strokes(phrase, dx=5.6, y0=10.0 + (k % 50) * 11.0) for k in range(word_groups)]
            strokes = tuple(box for grp in rows for box in grp)
            events = [0]

            def local(frame: FrameType, event: str, arg: Any) -> Any:
                events[0] += event == "line"
                return local

            def tracer(frame: FrameType, event: str, arg: Any) -> Any:
                return local if frame.f_code.co_filename == quality.__file__ else None

            prev = sys.gettrace()
            sys.settrace(tracer)
            try:
                classify_spans(spans, width_pt=PAGE_W, height_pt=PAGE_H, images=(), glyph_boxes=strokes)
            finally:
                sys.settrace(prev)
            return events[0]

        s_small = count_span_lines(60)
        s_large = count_span_lines(240)
        assert s_large / s_small < 6.5, f"Expected sub-quadratic span scaling, got {s_small} -> {s_large}"
