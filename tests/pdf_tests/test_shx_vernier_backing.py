from __future__ import annotations

from datagrunt.pdf.quality import Box, RawSpan, classify_spans


def _make_tr3_span(text: str, cw: float = 10.0, x0: float = 100.0, y0: float = 100.0) -> RawSpan:
    cboxes = tuple((x0 + i * cw, y0, x0 + (i + 1) * cw, y0 + 15.0) for i in range(len(text)))
    return RawSpan(
        text=text,
        bbox=(cboxes[0][0], y0, cboxes[-1][2], y0 + 15.0),
        char_boxes=cboxes,
        tight_boxes=cboxes,
        base_font_name="Txt",
        font_size_pt=12.0,
        em_width_pt=12.0,
        is_bold=False,
        is_italic=False,
        render_mode=3,
        fill_rgba=(0, 0, 0, 255),
        stroke_rgba=(0, 0, 0, 255),
        object_index=0,
        generated_breaks=(),
        map_errors=(),
    )


def _visible_mask(text: str, strokes: list[Box], cw: float = 10.0) -> str:
    span = _make_tr3_span(text, cw=cw)
    pieces = classify_spans(
        (span,),
        width_pt=800.0,
        height_pt=500.0,
        images=(),
        glyph_boxes=tuple(strokes),
    )
    out = []
    for piece in pieces:
        out.append(piece.text if not piece.hidden_reasons else "_" * (piece.end - piece.start))
    return "".join(out)


def test_center_aligned_shx_part_number_vernier_crossover_recovered() -> None:
    # 12 glyphs stroked at 12.0pt and 12.5pt SHX pitch (with <30% Vernier straddle on indices 4 and 9)
    strokes_12 = [(88.0 + k * 12.0, 101.0, 95.5 + k * 12.0, 114.0) for k in range(12)]
    strokes_vernier = [(85.1 + k * 12.5, 101.0, 92.5 + k * 12.5, 114.0) for k in range(12)]
    assert _visible_mask("02XF44014601", strokes_12) == "02XF44014601"
    assert _visible_mask("02XF44014601", strokes_vernier) == "02XF44014601"


def test_shx_two_stroke_crossbar_t_recovered_without_accepting_table_rules() -> None:
    # Single-glyph cap-bar (vw=6.4pt, vh=0.8pt) backs 'T', whereas 120pt table border does not
    assert _visible_mask("T", [(101.5, 113.5, 107.9, 114.3)]) == "T"
    assert _visible_mask("T", [(40.0, 105.0, 160.0, 106.0)]) == "_"


def test_multi_word_partial_and_center_outward_drift_recovered() -> None:
    # 'MFG LOCATION': 'MFG' misses 'G' (left overhang), 'LOCATION' is 100% directly backed
    strokes = [
        (92.0, 101.0, 98.5, 114.0),  # M (overhangs left)
        (102.0, 101.0, 108.5, 114.0),  # F
        (112.0, 101.0, 118.5, 114.0),  # G (lands in F's cbox)
        *[(141.0 + k * 10.0, 101.0, 148.0 + k * 10.0, 114.0) for k in range(8)],
    ]
    assert _visible_mask("MFG LOCATION", strokes) == "MFG LOCATION"


def test_standalone_number_header_with_full_glyph_count_recovered() -> None:
    strokes = [(108.0 + k * 10.8, 101.0, 114.8 + k * 10.8, 114.0) for k in range(6)]
    assert _visible_mask("NUMBER", strokes) == "NUMBER"


def test_bracketed_and_cjk_bilingual_shx_tokens_recovered() -> None:
    # 1. '[INCHES]': 'I' straddled between '[' and 'NCHES]'
    bracket_strokes: list[Box] = [
        (101.0, 101.0, 108.0, 114.0),
        *[(121.0 + k * 10.0, 101.0, 128.0 + k * 10.0, 114.0) for k in range(6)],
    ]
    assert _visible_mask("[INCHES]", bracket_strokes) == "[INCHES]"

    # 2. CJK ideograph trailing edge drift ('关键功能')
    cjk_strokes: list[Box] = [
        (92.0, 101.0, 98.5, 114.0),
        (101.0, 101.0, 108.0, 114.0),
        (111.0, 101.0, 118.0, 114.0),
        (121.0, 101.0, 128.0, 114.0),
    ]
    assert _visible_mask("关键功能", cjk_strokes) == "关键功能"

    # 3. Unspaced bilingual note with Vernier crossover at index 6 ('F' in '原来为02XF45005701。')
    bilingual = "原来为02XF45005701。"
    bi_strokes: list[Box] = []
    for idx in range(len(bilingual)):
        if idx == 5:
            bi_strokes.append((151.0, 101.0, 162.0, 114.0))
        elif idx == 6:
            bi_strokes.append((168.0, 101.0, 178.0, 114.0))
        else:
            bi_strokes.append((101.0 + idx * 10.0, 101.0, 108.0 + idx * 10.0, 114.0))
    assert _visible_mask(bilingual, bi_strokes) == bilingual


def test_adversarial_hidden_digits_signs_negations_and_homoglyphs_stay_hidden() -> None:
    # 1. Omitted middle digit in "105" (with and without fake right overhang stroke)
    assert _visible_mask("105", [(101, 101, 108, 114), (122, 101, 129, 114)]) == "1_5"
    assert _visible_mask("105", [(101, 101, 108, 114), (122, 101, 129, 114), (133, 101, 139, 114)]) == "1_5"
    # 2. Omitted interior glyph in alphanumeric code "02XF04"
    assert (
        _visible_mask(
            "02XF04",
            [
                (101, 101, 108, 114),
                (111, 101, 118, 114),
                (121, 101, 128, 114),
                (142, 101, 148, 114),
                (151, 101, 158, 114),
            ],
        )
        == "02X_04"
    )
    # 3. Omitted digit inside multi-word span "PAYMENT IS 105000 USD"
    mw = "PAYMENT IS 105000 USD"
    mw_strokes: list[Box] = [
        (101 + i * 10.0, 101.0, 108 + i * 10.0, 114.0) for i, ch in enumerate(mw) if not ch.isspace() and i != 12
    ]
    assert _visible_mask(mw, mw_strokes) == "PAYMENT IS 1_5000 USD"
    # 4. Omitted minus sign, decimal point, negation words, polarity 'A' prefix, Cyrillic homoglyph
    assert _visible_mask("-100", [(112, 101, 119, 114), (122, 101, 129, 114), (132, 101, 143, 114)]) == "_100"
    assert _visible_mask("1.5", [(102, 101, 109, 114), (121, 101, 128, 114)]) == "1_5"
    assert _visible_mask("NOT", [(112, 101, 119, 114), (122, 101, 133, 114)]) == "_OT"
    assert _visible_mask("CANT", [(112, 101, 119, 114), (122, 101, 129, 114), (132, 101, 143, 114)]) == "_ANT"
    assert (
        _visible_mask(
            "IGNORE",
            [
                (101, 101, 108, 114),
                (111, 101, 118, 114),
                (132, 101, 139, 114),
                (142, 101, 149, 114),
                (152, 101, 159, 114),
            ],
        )
        == "IG_ORE"
    )
    assert _visible_mask("ATYPICAL", [(112 + k * 10.5, 101, 119 + k * 10.5, 114) for k in range(7)]) == "_TYPICAL"
    assert _visible_mask("N\u041eTE", [(101, 101, 108, 114), (122, 101, 129, 114), (132, 101, 139, 114)]) == "N_TE"
