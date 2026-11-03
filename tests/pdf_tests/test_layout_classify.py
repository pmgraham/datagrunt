"""Tests for the v5 stream pass's classification (#368, A4b): headings, key-values, lists and paragraphs.

Unit tests build TextBlock values directly: boxes are displayed, top-left
``(x0, y0, x1, y1)`` points. End-to-end tests build PDFs with the ReportLab
toolkit (``pdf_tests.builders``) and read them through ``extract_page``,
``assess_page``, ``find_lattice_tables``, ``text_blocks`` and
``classify_blocks``.
"""

import dataclasses
import pickle
import re
from collections import Counter

import pytest

from datagrunt.pdf.elements import KeyValue
from datagrunt.pdf.layout import (
    StreamHeading,
    StreamKeyValue,
    StreamList,
    StreamListItem,
    StreamParagraph,
    TextBlock,
    classify_blocks,
    find_lattice_tables,
    text_blocks,
)
from datagrunt.pdf.reader import PDFReader
from pdf_tests.builders import PageSpec, TextRun, build_pdf
from pdf_tests.test_layout_lattice import box, chars
from pdf_tests.test_layout_stream import block, cell, column, line, page_of

# Three lines of 10 pt body text, set 12 pt apart: one paragraph.
BODY = (
    "The woodland is vast and its creatures are many, each",
    "with its own purpose and its own ambitions for the",
    "forest and for the clearings that lie within it.",
)
BODY_TEXT = " ".join(BODY)


def body(top: float, **overrides) -> tuple[TextRun, ...]:
    """The BODY paragraph from x 72, its first baseline at ``top``."""
    return column(BODY, 72.0, top, pitch=12.0, size=10.0, **overrides)


def lines_of(texts, top: float = 100.0, **overrides) -> tuple[TextRun, ...]:
    """10 pt lines from x 72, 12 pt apart: close enough to merge into one paragraph."""
    return column(texts, 72.0, top, pitch=12.0, size=10.0, **overrides)


def read_items(tmp_path, *items):
    """classify_blocks of page 1's text_blocks, through extract_page, assess_page and find_lattice_tables."""
    page, pieces = page_of(tmp_path, *items)
    return classify_blocks(text_blocks(pieces, find_lattice_tables(page.segments.segments, pieces)))


def described(items) -> list[tuple]:
    """Each item as a plain tuple: its kind, then its fields."""
    found = []
    for item in items:
        if isinstance(item, StreamHeading):
            found.append(("heading", item.level, item.text))
        elif isinstance(item, StreamParagraph):
            found.append(("paragraph", item.text))
        elif isinstance(item, StreamKeyValue):
            found.append(("key_value", item.key, item.value))
        else:
            found.append(("list", item.ordered, [(entry.marker, entry.text) for entry in item.items]))
    return found


def sized(text: str, size: float, *, bold: bool = False, y: float = 10.0) -> TextBlock:
    """A one-line block of ``text`` at ``size`` (bold if asked), its chars 6 pt wide from x 10."""
    return block(line(cell(text, 10.0, y), font_size_pt=size, is_bold=bold))


class TestHeadings:
    """Heading: a size at least 1.15 times the body's, or a bold line on its own; short, never a sentence."""

    def test_a_line_larger_than_the_body_is_a_heading(self, tmp_path):
        items = read_items(tmp_path, TextRun("Overview", 72.0, 100.0, size=16.0), *body(130.0))

        assert described(items) == [("heading", 1, "Overview"), ("paragraph", BODY_TEXT)]

    @pytest.mark.parametrize(
        "size,kind",
        [
            # The body is 10 pt: 1.15 times it is 11.5 pt.
            pytest.param(11.5, "heading", id="1.15-times"),
            pytest.param(11.4, "paragraph", id="1.14-times"),
        ],
    )
    def test_a_heading_is_at_least_1_15_times_the_body_size(self, tmp_path, size, kind):
        items = read_items(tmp_path, TextRun("Overview", 72.0, 100.0, size=size), *body(130.0))

        assert described(items)[0][0] == kind

    @pytest.mark.parametrize(
        "font,expected",
        [
            pytest.param("Helvetica-Bold", [("heading", 1, "Overview"), ("paragraph", BODY_TEXT)], id="bold"),
            pytest.param("Helvetica", [("paragraph", f"Overview {BODY_TEXT}")], id="regular-merges-into-the-body"),
        ],
    )
    def test_a_bold_line_at_the_body_size_on_its_own_is_a_heading(self, tmp_path, font, expected):
        items = read_items(tmp_path, TextRun("Overview", 72.0, 100.0, font=font, size=10.0), *body(112.0))

        assert described(items) == expected

    def test_a_bold_line_inside_a_wrapped_paragraph_is_no_heading(self, tmp_path):
        # Review probe: the bold line is emphasis set mid-paragraph, not a heading (A6 would scope the page under it).
        texts = (
            "Operators must read the manual before use and",
            "must never exceed the rated load of the hoist",
            "under any circumstances whatsoever, as the",
            "chain may fail without warning in that case.",
        )
        fonts = ("Helvetica", "Helvetica-Bold", "Helvetica", "Helvetica")
        items = tuple(
            TextRun(text, 72.0, 100.0 + 12.0 * at, size=10.0, font=font)
            for at, (text, font) in enumerate(zip(texts, fonts))
        )

        assert described(read_items(tmp_path, *items)) == [("paragraph", " ".join(texts))]

    @pytest.mark.parametrize(
        "texts",
        [
            pytest.param(("Rated voltage and current for the drive", "Mechanical", "Weight 2 kg"), id="label-longest"),
            pytest.param(
                ("Install the unit on a flat surface", "Wiring", "Connect the supply first"), id="one-line-paragraph"
            ),
        ],
    )
    def test_a_bold_heading_under_the_longest_line_is_a_heading(self, tmp_path, texts):
        # The datasheet pattern: a regular line, a bold heading under it, then its body.
        fonts = ("Helvetica", "Helvetica-Bold", "Helvetica")
        items = tuple(
            TextRun(text, 72.0, 100.0 + 14.0 * at, font=font) for at, (text, font) in enumerate(zip(texts, fonts))
        )

        assert described(read_items(tmp_path, *items)) == [
            ("paragraph", texts[0]),
            ("heading", 1, texts[1]),
            ("paragraph", texts[2]),
        ]

    def test_a_bold_paragraph_of_two_lines_is_no_heading(self, tmp_path):
        items = read_items(tmp_path, *lines_of(("Bold words", "over two lines"), font="Helvetica-Bold"), *body(150.0))

        assert described(items) == [("paragraph", "Bold words over two lines"), ("paragraph", BODY_TEXT)]

    @pytest.mark.parametrize(
        "length,kind",
        [
            # Corpus headings run to 49 chars; text set as large but over 60 chars was always prose (68 to 581).
            pytest.param(60, "heading", id="60-chars"),
            pytest.param(61, "paragraph", id="61-chars"),
        ],
    )
    @pytest.mark.parametrize(
        "overrides",
        [pytest.param({"size": 12.0}, id="by-size"), pytest.param({"font": "Helvetica-Bold"}, id="by-bold")],
    )
    def test_a_heading_is_at_most_60_chars(self, tmp_path, overrides, length, kind):
        text = "Heading " * 7 + "H" * (length - 56)
        fields = {"size": 10.0} | overrides

        items = read_items(tmp_path, TextRun(text, 72.0, 100.0, **fields), *body(140.0))

        assert len(text) == length
        assert described(items)[0] == ((kind, 1, text) if kind == "heading" else (kind, text))

    @pytest.mark.parametrize(
        "overrides",
        [pytest.param({"size": 16.0}, id="by-size"), pytest.param({"font": "Helvetica-Bold"}, id="by-bold")],
    )
    def test_a_line_ending_with_a_full_stop_is_no_heading(self, tmp_path, overrides):
        # On the corpus, every short heading-sized line ending with one was a sentence (9 of 371).
        fields = {"size": 10.0} | overrides

        items = read_items(tmp_path, TextRun("Read this first.", 72.0, 100.0, **fields), *body(140.0))

        assert described(items)[0] == ("paragraph", "Read this first.")

    def test_a_line_ending_with_a_question_mark_can_be_a_heading(self, tmp_path):
        items = read_items(tmp_path, TextRun("Why a workshop?", 72.0, 100.0, font="Helvetica-Bold"), *body(140.0))

        assert described(items)[0] == ("heading", 1, "Why a workshop?")

    def test_a_numbered_line_can_be_a_heading_and_a_bulleted_one_never_is(self, tmp_path):
        items = read_items(
            tmp_path,
            TextRun("1. Golden Rules", 72.0, 100.0, size=21.0),
            *body(130.0),
            TextRun("• Warning", 72.0, 190.0, font="Helvetica-Bold", size=10.0),
            *body(220.0),
        )

        assert described(items) == [
            ("heading", 1, "1. Golden Rules"),
            ("paragraph", BODY_TEXT),
            ("list", False, [("•", "Warning")]),
            ("paragraph", BODY_TEXT),
        ]

    def test_levels_rank_the_page_heading_sizes_largest_first(self, tmp_path):
        items = read_items(
            tmp_path,
            TextRun("Chapter One", 72.0, 80.0, size=20.0),
            TextRun("Setting Up", 72.0, 120.0, size=15.0),
            *body(140.0),
            TextRun("Clearings", 72.0, 200.0, font="Helvetica-Bold", size=10.0),
            *body(212.0),
            TextRun("Playing the Game", 72.0, 270.0, size=15.0),
            *body(290.0),
        )

        assert described(items) == [
            ("heading", 1, "Chapter One"),
            ("heading", 2, "Setting Up"),
            ("paragraph", BODY_TEXT),
            ("heading", 3, "Clearings"),
            ("paragraph", BODY_TEXT),
            ("heading", 2, "Playing the Game"),
            ("paragraph", BODY_TEXT),
        ]

    def test_levels_stop_at_6(self):
        headings = tuple(
            sized(f"Size {size}", float(size), y=20.0 * index) for index, size in enumerate(range(20, 12, -1))
        )
        prose = block(line(cell("x" * 80, 10.0, 200.0), font_size_pt=10.0))

        items = classify_blocks((*headings, prose))

        assert [item.level for item in items[:-1]] == [1, 2, 3, 4, 5, 6, 6, 6]

    def test_the_body_size_weighs_each_line_by_its_chars(self):
        # Four 14 pt words (20 chars) and one 10 pt line (40 chars): the body is 10 pt, so the words are headings.
        # Weighed by lines (one each), the body would be 14 pt and nothing a heading.
        words = tuple(sized("Clear", 14.0, y=20.0 * index) for index in range(4))
        prose = block(line(cell("y" * 40, 10.0, 100.0), font_size_pt=10.0))

        assert [type(item) for item in classify_blocks((*words, prose))] == [StreamHeading] * 4 + [StreamParagraph]

    @pytest.mark.parametrize(
        ("size", "tier"), [(14.2, 14.0), (14.3, 14.5), (14.7, 14.5), (14.8, 15.0), (18.0, 18.0), (12.6, 12.5)]
    )
    def test_a_heading_carries_its_first_lines_size_to_half_a_point(self, size, tier):
        prose = block(line(cell("x" * 80, 10.0, 200.0), font_size_pt=10.0))

        (heading, _) = classify_blocks((sized("Overview", size), prose))

        assert (heading.font_size_pt, heading.is_bold) == (tier, False)

    def test_a_size_under_a_quarter_point_is_the_least_tier(self):
        # A1 needs a size over 0: 0.2 pt rounds to 0, so it is 0.5 pt, the least tier.
        prose = block(line(cell("x" * 80, 10.0, 200.0), font_size_pt=0.1))

        (heading, _) = classify_blocks((sized("Overview", 0.2), prose))

        assert heading.font_size_pt == 0.5

    @pytest.mark.parametrize(("first", "second"), [(True, False), (False, True)])
    def test_a_heading_carries_its_first_lines_boldness(self, first, second):
        two = block(
            line(cell("Bearing", 10.0, 10.0), font_size_pt=16.0, is_bold=first),
            line(cell("Assembly", 10.0, 30.0), font_size_pt=16.0, is_bold=second),
        )
        prose = block(line(cell("x" * 80, 10.0, 200.0), font_size_pt=10.0))

        (heading, _) = classify_blocks((two, prose))

        assert (heading.text, heading.font_size_pt, heading.is_bold) == ("Bearing Assembly", 16.0, first)

    def test_a_bold_line_carries_its_tier_end_to_end(self, tmp_path):
        items = read_items(tmp_path, TextRun("Clearings", 72.0, 80.0, font="Helvetica-Bold", size=10.0), *body(100.0))

        assert [(item.font_size_pt, item.is_bold) for item in items if isinstance(item, StreamHeading)] == [
            (10.0, True)
        ]

    def test_the_body_size_is_the_upper_median(self):
        # 20 chars at 10 pt and 20 at 12 pt: the upper median is 12 pt, so the 12 pt line is no heading.
        items = classify_blocks((sized("a" * 20, 10.0), sized("b" * 20, 12.0, y=30.0)))

        assert [type(item) for item in items] == [StreamParagraph, StreamParagraph]


class TestKeyValues:
    """KeyValue: ``Key: Value`` with a key of at most 40 chars and no sentence-ending punctuation."""

    def test_key_value_lines_are_one_pair_each(self, tmp_path):
        items = read_items(tmp_path, *lines_of(("Weight: 5 kg", "Colour: red", "Size: large")))

        assert described(items) == [
            ("key_value", "Weight", "5 kg"),
            ("key_value", "Colour", "red"),
            ("key_value", "Size", "large"),
        ]

    def test_a_line_without_a_key_continues_the_value_before_it(self, tmp_path):
        items = read_items(tmp_path, *lines_of(("Address: 1 Harbour Road", "Leith, Edinburgh", "Phone: 555 0100")))

        assert described(items) == [
            ("key_value", "Address", "1 Harbour Road Leith, Edinburgh"),
            ("key_value", "Phone", "555 0100"),
        ]

    def test_a_paragraph_set_close_under_a_pair_is_not_its_value(self, tmp_path):
        # The corpus datasheets: "Category: Fastener" ends 405 pt short of the column, so the line under it
        # (merged into its block by geometry) did not wrap from it.
        description = (
            "The hex bolt is a fastener engineered for industrial service, and this",
            "datasheet describes it.",
        )
        items = (
            TextRun("Category: Fastener", 72.0, 100.0, size=9.5),
            *column(description, 72.0, 122.0, pitch=12.0, size=9.5),
        )

        assert described(read_items(tmp_path, *items)) == [
            ("key_value", "Category", "Fastener"),
            ("paragraph", " ".join(description)),
        ]

    def test_a_value_that_wraps_continues_on_the_next_line(self, tmp_path):
        texts = ("Note: this note runs on past the end of its first line and so", "it wraps onto the next.")

        assert described(read_items(tmp_path, *lines_of(texts))) == [
            ("key_value", "Note", "this note runs on past the end of its first line and so it wraps onto the next.")
        ]

    def test_a_colon_in_the_paragraph_after_a_pair_makes_no_pair(self, tmp_path):
        texts = (
            "Category: Fastener",
            "The bolt is described here in full, and every value given",
            "is nominal: test it.",
        )

        assert described(read_items(tmp_path, *lines_of(texts))) == [
            ("key_value", "Category", "Fastener"),
            ("paragraph", " ".join(texts[1:])),
        ]

    @pytest.mark.parametrize(
        "tail,expected",
        [
            # 6 pt chars: the pair's line ends at x 46; the next line's first word "efgh" is 24 pt long; the
            # next line ends 18 pt (4 x's) or 24 pt (5 x's) past x 46. The word would have fitted in 24 pt.
            pytest.param(4, [("key_value", "Ab", "cd efgh xxxx")], id="no-room-for-the-word"),
            pytest.param(5, [("key_value", "Ab", "cd"), ("paragraph", "efgh xxxxx")], id="room-for-the-word"),
        ],
    )
    def test_a_value_continues_only_if_the_next_lines_first_word_did_not_fit_after_it(self, tail, expected):
        pair, after = line(cell("Ab: cd", 10.0, 10.0)), line(cell("efgh " + "x" * tail, 10.0, 24.0))

        assert described(classify_blocks((block(pair, after),))) == expected

    def test_a_line_of_one_word_too_long_for_the_room_continues_the_value(self):
        # The pair's line ends at x 46, the block at 82: 36 pt of room for a 72 pt word.
        pair, after = line(cell("Ab: cd", 10.0, 10.0)), line(cell("efghijklmnop", 10.0, 24.0))

        assert described(classify_blocks((block(pair, after),))) == [("key_value", "Ab", "cd efghijklmnop")]

    def test_a_key_with_nothing_after_it_has_an_empty_value(self, tmp_path):
        items = read_items(tmp_path, *lines_of(("Signature:", "Date:")))

        assert described(items) == [("key_value", "Signature", ""), ("key_value", "Date", "")]

    def test_a_key_and_value_cells_far_apart_are_one_pair(self, tmp_path):
        items = read_items(tmp_path, TextRun("Weight:", 72.0, 100.0), TextRun("5 kg", 200.0, 100.0))

        assert described(items) == [("key_value", "Weight", "5 kg")]

    @pytest.mark.parametrize(
        "length,kind",
        [pytest.param(40, "key_value", id="40-chars"), pytest.param(41, "paragraph", id="41-chars")],
    )
    def test_a_key_is_at_most_40_chars(self, tmp_path, length, kind):
        text = "K" * length + ": value"

        assert described(read_items(tmp_path, TextRun(text, 72.0, 100.0)))[0][0] == kind

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param("It rained. Then we said: go home", id="full-stop-in-the-key"),
            pytest.param("Why not? Because: reasons", id="question-mark-in-the-key"),
            pytest.param("Wow! Look: over there", id="exclamation-mark-in-the-key"),
            pytest.param("The meeting starts at 10:30 sharp", id="colon-in-a-time"),
            pytest.param("See https://example.com for more", id="colon-in-a-url"),
        ],
    )
    def test_a_sentence_with_a_colon_that_is_not_a_pair_is_a_paragraph(self, tmp_path, text):
        assert described(read_items(tmp_path, TextRun(text, 72.0, 100.0))) == [("paragraph", text)]

    def test_a_colon_after_the_first_line_of_a_paragraph_makes_no_pair(self, tmp_path):
        texts = ("The rules are short and the game", "is simple: each player draws a card.")

        assert described(read_items(tmp_path, *lines_of(texts))) == [("paragraph", " ".join(texts))]


# A lead-in line over two pairs, set as one block (#456).
INVOICE = ("Invoice Details", "Invoice Date: 2026-01-15", "Payment Terms: Net 30")
INVOICE_PAIRS = [("key_value", "Invoice Date", "2026-01-15"), ("key_value", "Payment Terms", "Net 30")]
# A line of body text set apart under the block.
PAGE_BODY = "Body text of the page that is long enough to read as a digital page here."


def stacked(*texts: str) -> TextBlock:
    """One block of ``texts``, a line each: 6 pt chars from x 10, the lines 14 pt apart."""
    return block(*(line(cell(text, 10.0, 10.0 + 14.0 * index)) for index, text in enumerate(texts)))


def element_fields(element) -> tuple:
    """An A1 element as a plain tuple: its kind, then its key and value or its text."""
    if isinstance(element, KeyValue):
        return ("KeyValue", element.key, element.value)
    return (type(element).__name__, element.text)


class TestKeyValueRuns:
    """A run of 2 or more whole-line pairs splits a paragraph; one pair-shaped line in prose stays prose (#456)."""

    def test_a_lead_in_over_two_pairs_reads_as_a_paragraph_then_pairs_through_the_reader(self, tmp_path):
        items = (*lines_of(INVOICE), TextRun(PAGE_BODY, 72.0, 300.0, size=10.0))
        reader = PDFReader(build_pdf(tmp_path / "invoice.pdf", [PageSpec(items=items)]), ocr="never")

        assert [element_fields(item) for item in reader.pages[0].elements] == [
            ("Paragraph", "Invoice Details"),
            ("KeyValue", "Invoice Date", "2026-01-15"),
            ("KeyValue", "Payment Terms", "Net 30"),
            ("Paragraph", PAGE_BODY),
        ]

    def test_a_lead_in_then_two_pairs_is_a_paragraph_then_two_pairs(self):
        assert described(classify_blocks((stacked(*INVOICE),))) == [("paragraph", "Invoice Details"), *INVOICE_PAIRS]

    def test_lines_before_the_run_are_one_paragraph(self):
        texts = ("Invoice Details for the order", "placed online", *INVOICE[1:])

        assert described(classify_blocks((stacked(*texts),))) == [
            ("paragraph", "Invoice Details for the order placed online"),
            *INVOICE_PAIRS,
        ]

    @pytest.mark.parametrize(
        "texts",
        [
            pytest.param(INVOICE[:2], id="a-lead-in-then-one-pair"),
            pytest.param(
                ("Each player draws a card and then", "Setup: deal five cards to each player", "before play begins."),
                id="one-pair-shaped-line-inside-prose",
            ),
        ],
    )
    def test_one_pair_shaped_line_after_a_lead_in_stays_in_its_paragraph(self, texts):
        assert described(classify_blocks((stacked(*texts),))) == [("paragraph", " ".join(texts))]

    def test_colons_late_in_lines_make_no_run(self):
        # Each colon follows more than 40 chars, so neither line is a pair (rule 3's key).
        texts = (
            "The rules of the game are short to read",
            "and when the round ends with every card played: score",
            "each clearing and when every player has passed: draw.",
        )

        assert described(classify_blocks((stacked(*texts),))) == [("paragraph", " ".join(texts))]

    @pytest.mark.parametrize(
        "after,expected",
        [
            # 6 pt chars: the block ends at x 184; "Payment Terms: Net 30" ends at x 136, room for "Thank".
            pytest.param(
                ("Thank you for your business.",),
                [("paragraph", "Thank you for your business.")],
                id="a-line-that-did-not-wrap-starts-a-paragraph",
            ),
            pytest.param(
                ("Thank you for your business.", "Account: 10442"),
                [("paragraph", "Thank you for your business. Account: 10442")],
                id="one-pair-line-after-it-stays-in-that-paragraph",
            ),
            pytest.param(
                ("Thank you for your business.", "Account: 10442", "Sort Code: 80-22-60"),
                [
                    ("paragraph", "Thank you for your business."),
                    ("key_value", "Account", "10442"),
                    ("key_value", "Sort Code", "80-22-60"),
                ],
                id="a-later-run-splits-that-paragraph-too",
            ),
        ],
    )
    def test_lines_after_the_run(self, after, expected):
        texts = ("Invoice Details for the order", *INVOICE[1:], *after)

        assert described(classify_blocks((stacked(*texts),))) == [
            ("paragraph", "Invoice Details for the order"),
            *INVOICE_PAIRS,
            *expected,
        ]

    def test_a_line_that_wrapped_from_the_runs_last_pair_continues_its_value(self):
        # "Ship To: 1 Harbour Road, Leith" ends where the block does, at x 190: "Edinburgh" did not fit after it.
        texts = ("Invoice Details for the order", INVOICE[1], "Ship To: 1 Harbour Road, Leith", "Edinburgh EH6")

        assert described(classify_blocks((stacked(*texts),))) == [
            ("paragraph", "Invoice Details for the order"),
            INVOICE_PAIRS[0],
            ("key_value", "Ship To", "1 Harbour Road, Leith Edinburgh EH6"),
        ]


# The 2026-11-02 audit's P2-C case: two lines of prose whose first starts "Operating envelope:", the second wrapped
# from it.
ENVELOPE = (
    "Operating envelope: -40°C to +185°C at rated speed and load. Lubrication conforms to",
    "ISO VG-32 turbine oil with an oil-mist feed at 2.5 bar.",
)
# The corpus datasheets' stream pairs, and the two longest values of their parameter tables (53 chars).
DATASHEET_PAIRS = (
    ("Document", "DS-GSK-PTFE-3IN-150"),
    ("Revision", "C.3"),
    ("Issue Date", "2025-03-27"),
    ("Category", "Gasket"),
    ("Certifications", "IEC 60584-1, ASTM E230, NIST traceable cert available"),
    ("Operating Medium", "Filtered compressed air, lubricated or non-lubricated"),
)


def one_line(text: str) -> list[tuple]:
    """``text`` as one line of 6 pt chars, classified."""
    return described(classify_blocks((block(line(cell(text, 10.0, 10.0))),)))


class TestProseIsNoPair:
    """A ``Key: Value`` whose value is prose, two sentences or past 80 chars, is a paragraph (P2-C)."""

    def test_the_audits_paragraph_is_a_paragraph(self, tmp_path):
        assert described(read_items(tmp_path, *lines_of(ENVELOPE))) == [("paragraph", " ".join(ENVELOPE))]

    def test_the_corpus_datasheet_pairs_stay_pairs(self, tmp_path):
        texts = tuple(f"{key}: {value}" for key, value in DATASHEET_PAIRS)

        assert described(read_items(tmp_path, *lines_of(texts))) == [
            ("key_value", key, value) for key, value in DATASHEET_PAIRS
        ]

    @pytest.mark.parametrize(
        "length,kind", [pytest.param(80, "key_value", id="80-chars"), pytest.param(81, "paragraph", id="81-chars")]
    )
    def test_a_value_is_at_most_80_chars(self, length, kind):
        assert one_line("Note: " + "v" * length)[0][0] == kind

    def test_the_cap_weighs_the_whole_value_with_its_wrapped_lines(self):
        # Each line's value is under 80 chars; wrapped together they are 87.
        pair, after = line(cell("Ab: " + "c" * 40, 10.0, 10.0)), line(cell("e" * 46, 10.0, 24.0))

        assert described(classify_blocks((block(pair, after),))) == [("paragraph", f"Ab: {'c' * 40} {'e' * 46}")]

    @pytest.mark.parametrize(
        "value,kind",
        [
            pytest.param("Fill the housing before use. Check the level every week.", "paragraph", id="two-sentences"),
            pytest.param("Fill the housing now! Check the level every week.", "paragraph", id="after-an-exclamation"),
            pytest.param("one two three four. five six seven eight", "paragraph", id="two-of-four-words"),
            pytest.param("one two three. five six seven eight", "key_value", id="three-words-then-four"),
            pytest.param("J. Smith, Acme Inc. Springfield", "key_value", id="initials-and-abbreviations"),
            pytest.param("approx. 25 mm by 40 mm", "key_value", id="an-abbreviation-first"),
            pytest.param("A.2", "key_value", id="a-revision"),
            pytest.param("Fill the housing with oil before use.", "key_value", id="one-sentence"),
        ],
    )
    def test_a_value_of_two_sentences_of_4_words_or_more_is_prose(self, value, kind):
        assert one_line(f"Note: {value}")[0][0] == kind

    def test_a_pair_after_a_prose_one_in_its_block_stays_a_pair(self, tmp_path):
        texts = ("Note: Fill the housing before use. Check the level every week.", "Weight: 5 kg")

        assert described(read_items(tmp_path, *lines_of(texts))) == [
            ("paragraph", texts[0]),
            ("key_value", "Weight", "5 kg"),
        ]


class TestLists:
    """BulletList: one item per marker, continuation lines joining their item, ordered when numbered."""

    @pytest.mark.parametrize(
        "markers,ordered",
        [
            pytest.param(("•", "•"), False, id="bullet"),
            pytest.param(("-", "-"), False, id="hyphen"),
            pytest.param(("*", "*"), False, id="asterisk"),
            pytest.param(("»", "»"), False, id="guillemet"),
            pytest.param(("1.", "2."), True, id="number-dot"),
            pytest.param(("1)", "2)"), True, id="number-bracket"),
            pytest.param(("a.", "b."), True, id="letter-dot"),
            pytest.param(("(a)", "(b)"), True, id="bracketed-letter"),
            pytest.param(("iv.", "v."), True, id="roman-dot"),
            pytest.param(("(iv)", "(v)"), True, id="bracketed-roman"),
            pytest.param(("I", "II"), True, id="bare-roman-before-a-capital"),
        ],
    )
    def test_each_marker_kind_makes_one_item_per_marker(self, tmp_path, markers, ordered):
        texts = ("Before the list", f"{markers[0]} First item", f"{markers[1]} Second item")

        assert described(read_items(tmp_path, *lines_of(texts))) == [
            ("paragraph", "Before the list"),
            ("list", ordered, [(markers[0], "First item"), (markers[1], "Second item")]),
        ]

    def test_a_continuation_line_joins_its_item(self, tmp_path):
        items = (
            TextRun("• First item that wraps", 72.0, 100.0, size=10.0),
            TextRun("onto a second line", 82.0, 112.0, size=10.0),
            TextRun("• Second item", 72.0, 124.0, size=10.0),
        )

        assert described(read_items(tmp_path, *items)) == [
            ("list", False, [("•", "First item that wraps onto a second line"), ("•", "Second item")])
        ]

    def test_bulleted_and_numbered_items_are_separate_lists(self, tmp_path):
        items = read_items(tmp_path, *lines_of(("• one", "• two", "1. three", "2. four")))

        assert described(items) == [
            ("list", False, [("•", "one"), ("•", "two")]),
            ("list", True, [("1.", "three"), ("2.", "four")]),
        ]

    def test_a_marker_without_an_item_is_a_paragraph(self, tmp_path):
        assert described(read_items(tmp_path, TextRun("•", 72.0, 100.0))) == [("paragraph", "•")]

    def test_a_list_item_wins_over_a_key_value(self, tmp_path):
        items = read_items(tmp_path, *lines_of(("» Battle: Initiate a battle.", "» March: Take up to two moves.")))

        assert described(items) == [
            ("list", False, [("»", "Battle: Initiate a battle."), ("»", "March: Take up to two moves.")])
        ]

    def test_a_section_numbered_paragraph_is_a_paragraph(self, tmp_path):
        texts = (
            "1.1.1 Precedence. If a card conflicts with the Law,",
            "follow the card.",
            "1.1.2 Cannot. It is absolute.",
        )

        assert described(read_items(tmp_path, *lines_of(texts))) == [
            ("paragraph", "1.1.1 Precedence. If a card conflicts with the Law, follow the card."),
            ("paragraph", "1.1.2 Cannot. It is absolute."),
        ]

    def test_a_bare_number_starting_a_line_is_no_marker(self, tmp_path):
        texts = ("Each player draws a hand of five cards and takes", "3 quest cards, placing them face up.")

        assert described(read_items(tmp_path, *lines_of(texts))) == [("paragraph", " ".join(texts))]

    @pytest.mark.parametrize("glyph", list("‣⁃◦∙▪●"))
    def test_unicode_bullet_glyphs_mark_unordered_items(self, glyph):
        items = classify_blocks((block(line(cell(f"{glyph} item", 10.0, 10.0))),))

        assert described(items) == [("list", False, [(glyph, "item")])]


class TestSources:
    """Every item keeps the char ranges it reads and its box, for the page's character check (Pass C)."""

    def test_the_items_read_every_char_of_the_blocks_once(self, tmp_path):
        page, pieces = page_of(
            tmp_path,
            TextRun("Overview", 72.0, 80.0, size=16.0),
            *lines_of(("Weight: 5 kg", "Colour: red"), top=110.0),
            *lines_of(("• one", "• two"), top=150.0),
            *body(200.0),
        )
        blocks = text_blocks(pieces, find_lattice_tables(page.segments.segments, pieces))

        items = classify_blocks(blocks)

        read = [piece for item in items for piece in item.pieces]
        assert Counter(char for piece in read for char in piece.text if not char.isspace()) == Counter(
            char for found in blocks for char in found.text if not char.isspace()
        )
        assert [type(item) for item in items] == [
            StreamHeading,
            StreamKeyValue,
            StreamKeyValue,
            StreamList,
            StreamParagraph,
        ]

    def test_an_item_is_boxed_by_its_lines(self):
        first, second = line(cell("Weight: 5 kg", 10.0, 10.0)), line(cell("Colour: red", 10.0, 30.0))

        pair = classify_blocks((block(first, second),))[1]

        assert pair.bbox == second.bbox
        assert pair.pieces == second.cells[0].pieces

    def test_a_list_item_keeps_its_markers_chars(self):
        (found,) = classify_blocks((block(line(cell("• item", 10.0, 10.0))),))

        assert "".join(piece.text for piece in found.items[0].pieces) == "• item"
        assert found.pieces == found.items[0].pieces
        assert found.bbox == found.items[0].bbox


class TestClassifyBlocksArguments:
    """The blocks are checked at the boundary."""

    @pytest.mark.parametrize(
        "blocks,message",
        [
            pytest.param([block()], "blocks must be a tuple, got list", id="list"),
            pytest.param(("text",), "blocks[0] must be a TextBlock, got str", id="str"),
        ],
    )
    def test_invalid_argument_raises(self, blocks, message):
        with pytest.raises(TypeError, match=f"^{re.escape(message)}$"):
            classify_blocks(blocks)

    def test_no_blocks_no_items(self):
        assert classify_blocks(()) == ()


PIECE = chars("Name", 10.0, 10.0)
INSIDE = box(10.0, 10.0, 34.0, 22.0)
OUTSIDE_MESSAGE = (
    "pieces[0] must lie within the bbox, got (10.0, 10.0, 34.0, 22.0) in BBox(x0=40.0, y0=10.0, x1=60.0, y1=22.0)"
)
SHARED_CASES = [
    pytest.param({"bbox": (0.0, 0.0, 1.0, 1.0)}, TypeError, "bbox must be a BBox, got tuple", id="bbox-tuple"),
    pytest.param({"pieces": [PIECE]}, TypeError, "pieces must be a tuple, got list", id="pieces-list"),
    pytest.param({"pieces": ("Name",)}, TypeError, "pieces[0] must be a SpanPiece, got str", id="pieces-str"),
    pytest.param({"pieces": ()}, ValueError, "pieces must hold at least one piece", id="pieces-empty"),
    pytest.param({"bbox": box(40.0, 10.0, 60.0, 22.0)}, ValueError, OUTSIDE_MESSAGE, id="piece-outside"),
]
RECORDS = {
    StreamHeading: {"text": "Name", "level": 1, "font_size_pt": 14.0, "is_bold": False},
    StreamParagraph: {"text": "Name"},
    StreamKeyValue: {"key": "Name", "value": ""},
    StreamListItem: {"marker": "•", "text": "Name"},
}
OWN_CASES = [
    pytest.param(StreamHeading, {"text": " "}, ValueError, "text must be a non-blank str, got ' '", id="heading-text"),
    pytest.param(StreamHeading, {"level": 0}, ValueError, "level must be >= 1, got 0", id="heading-level-0"),
    pytest.param(StreamHeading, {"level": 7}, ValueError, "level must be <= 6, got 7", id="heading-level-7"),
    pytest.param(StreamHeading, {"level": True}, TypeError, "level must be an int, got True (bool)", id="level-bool"),
    pytest.param(StreamHeading, {"font_size_pt": 0.0}, ValueError, "font_size_pt must be > 0, got 0.0", id="size-0"),
    pytest.param(
        StreamHeading,
        {"font_size_pt": "14"},
        TypeError,
        "font_size_pt must be an int or float, got '14' (str)",
        id="size-str",
    ),
    pytest.param(StreamHeading, {"is_bold": 1}, TypeError, "is_bold must be a bool, got 1 (int)", id="bold-int"),
    pytest.param(StreamParagraph, {"text": None}, TypeError, "text must be a str, got None (NoneType)", id="paragraph"),
    pytest.param(StreamKeyValue, {"key": ""}, ValueError, "key must be a non-blank str, got ''", id="key-blank"),
    pytest.param(
        StreamKeyValue, {"value": None}, TypeError, "value must be a str, got None (NoneType)", id="value-none"
    ),
    pytest.param(StreamListItem, {"marker": ""}, ValueError, "marker must be a non-blank str, got ''", id="marker"),
    pytest.param(StreamListItem, {"text": "\t"}, ValueError, "text must be a non-blank str, got '\\t'", id="item-text"),
]


def record(kind, **overrides):
    """A valid ``kind`` record over PIECE, boxed exactly, with ``overrides``."""
    return kind(**({"bbox": INSIDE, "pieces": (PIECE,)} | RECORDS[kind] | overrides))


class TestRecords:
    """The classified items are frozen, slotted, picklable, validated records."""

    @pytest.mark.parametrize("kind", list(RECORDS), ids=lambda kind: kind.__name__)
    def test_pickle_round_trip_frozen_and_slotted(self, kind):
        original = record(kind)

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.bbox = INSIDE
        assert not hasattr(original, "__dict__")

    @pytest.mark.parametrize("kind", list(RECORDS), ids=lambda kind: kind.__name__)
    @pytest.mark.parametrize("overrides,error,message", SHARED_CASES)
    def test_invalid_shared_field_raises(self, kind, overrides, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            record(kind, **overrides)

    @pytest.mark.parametrize("kind,overrides,error,message", OWN_CASES)
    def test_invalid_own_field_raises(self, kind, overrides, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            record(kind, **overrides)


class TestStreamList:
    """StreamList is a validated record of items; its pieces are its items' in order."""

    def item(self, x: float = 10.0) -> StreamListItem:
        piece = chars("Name", x, 10.0)
        return StreamListItem(marker="•", text="Name", bbox=box(x, 10.0, x + 24.0, 22.0), pieces=(piece,))

    def test_pickle_round_trip_and_pieces(self):
        items = (self.item(), self.item(40.0))
        original = StreamList(items=items, ordered=False, bbox=box(10.0, 10.0, 64.0, 22.0))

        assert pickle.loads(pickle.dumps(original)) == original
        assert original.pieces == items[0].pieces + items[1].pieces

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"items": []}, TypeError, "items must be a tuple, got list", id="items-list"),
            pytest.param({"items": ()}, ValueError, "items must hold at least one item", id="items-empty"),
            pytest.param({"items": ("x",)}, TypeError, "items[0] must be a StreamListItem, got str", id="items-str"),
            pytest.param({"ordered": 1}, TypeError, "ordered must be a bool, got 1 (int)", id="ordered-int"),
            pytest.param({"bbox": None}, TypeError, "bbox must be a BBox, got NoneType", id="bbox-none"),
            pytest.param(
                {"bbox": box(40.0, 10.0, 64.0, 22.0)},
                ValueError,
                "items[0].bbox must lie within the bbox, got BBox(x0=10.0, y0=10.0, x1=34.0, y1=22.0) in "
                "BBox(x0=40.0, y0=10.0, x1=64.0, y1=22.0)",
                id="item-outside",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        fields = {"items": (self.item(),), "ordered": False, "bbox": INSIDE}

        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            StreamList(**(fields | overrides))
