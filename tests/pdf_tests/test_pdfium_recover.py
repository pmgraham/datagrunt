"""Tests for the v5 engine's recovery of text pdfium's textpage drops (#366, A3 part 3).

Fixtures are written by hand (``hand_written_pdf``): ReportLab cannot draw
``Tf 0``, ``Tz 0`` or nested Form XObjects. Each page is 400 x 300 pt and
draws a visible control line (object 0), then the hidden text.
"""

import ctypes
import dataclasses
import itertools
import pickle
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import PDFCorruptError, pdfium_engine
from datagrunt.pdf._pdfium_common import object_address
from datagrunt.pdf.pdfium_engine import (
    PageFailure,
    RawSpan,
    RecoveredSpan,
    RecoveredText,
    RecoveryStats,
    extract_page,
    open_document,
    page_text,
    recover_dropped_text,
)
from pdf_tests.builders import FormXObject, PageSpec, TextRun, break_page, build_pdf, hand_written_pdf
from pdf_tests.test_pdfium_spans import (
    HEBREW_TWO_WORDS,
    OVERFLOW,
    bfchar_cmap,
    cid_font_page,
    cid_page,
    content_page,
    rtl_page,
)

HELVETICA = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
# Helvetica whose /Widths say every glyph is 5 em wide: 60 pt per char at 12 pt.
WIDE_GLYPHS = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /FirstChar 32 /LastChar 126 /Widths [%s] >>" % (
    b" ".join([b"5000"] * 95)
)
CONTROL = b"BT /F1 12 Tf 20 270 Td (Visible) Tj ET "
NO_LOSS = RecoveryStats(overprint_duplicates=0, whitespace_objects=0, empty_text_objects=0, unrecovered_text_objects=0)
# The techniques whose text object pdfium drops though it draws glyphs ("single-space" draws none).
DROPPING = ["tf-0", "tiny", "tz-0", "overprint"]


def show(technique: str, x: float, y: float) -> bytes:
    """The content drawing one hiding technique with its (first) text object's origin at ``(x, y)``."""
    at = b"%g %g Td" % (x, y)
    return {
        "tf-0": b"BT /F1 0 Tf %s (HIDDEN) Tj ET" % at,
        "tiny": b"BT /F1 0.0005 Tf %s (HIDDEN) Tj ET" % at,
        # Tz is graphics state: q / Q keeps it from squeezing later text.
        "tz-0": b"q BT /F1 12 Tf 0 Tz %s (HIDDEN) Tj ET Q" % at,
        "overprint": b"BT /F1 12 Tf %s (Total) Tj ET BT /F1 12 Tf %s (Total) Tj ET" % (at, at),
        # A copy 1 unit right and 1 down: a drop shadow, inside pdfium's duplicate window.
        "drop-shadow": b"BT /F1 12 Tf %s (Total) Tj ET BT /F1 12 Tf %g %g Td (Total) Tj ET" % (at, x + 1, y - 1),
        "single-space": b"BT /F1 12 Tf %s ( ) Tj ET" % at,
    }[technique]


def stream(data: bytes, entries: bytes = b"") -> bytes:
    """A stream object holding ``data``, with ``entries`` in its dictionary."""
    return b"<< %s /Length %d >>\nstream\n%s\nendstream" % (entries, len(data), data)


def form(body: bytes, matrix: bytes, resources: bytes) -> bytes:
    """A Form XObject drawing ``body`` under ``matrix``."""
    entries = b"/Type /XObject /Subtype /Form /BBox [-1000 -1000 1000 1000] /Matrix %s /Resources << %s >>"
    return stream(body, entries % (matrix, resources))


def pdf(
    tmp_path: Path,
    content: bytes,
    *,
    page_entries: bytes = b"",
    resources: bytes = b"",
    extra: tuple[bytes, ...] = (),
    font: bytes = HELVETICA,
) -> Path:
    """A one-page 400 x 300 pt PDF drawing ``content`` with font /F1 (object 5); ``extra`` objects from 6."""
    page = (
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] %s /Contents 4 0 R"
        b" /Resources << /Font << /F1 5 0 R >> %s >> >>" % (page_entries, resources)
    )
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", page]
    return hand_written_pdf(tmp_path / "recover.pdf", [*objects, stream(content), font, *extra])


def plain(tmp_path: Path, technique: str) -> Path:
    return pdf(tmp_path, CONTROL + show(technique, 72, 100))


def rotated(tmp_path: Path, technique: str) -> Path:
    return pdf(tmp_path, CONTROL + show(technique, 72, 100), page_entries=b"/Rotate 90")


def cropped(tmp_path: Path, technique: str) -> Path:
    return pdf(tmp_path, CONTROL + show(technique, 72, 100), page_entries=b"/CropBox [50 40 350 280]")


def nested(tmp_path: Path, technique: str) -> Path:
    """The technique at (10, 10) in form Y ([2 0 0 2 10 10]), drawn at (3, 4) in form X ([1.5 0 0 1.5 20 30]).

    The page draws X at (40, 0), so the text's origin is page (109.5, 81) at scale 3.
    """
    inner = form(show(technique, 10, 10), b"[2 0 0 2 10 10]", b"/Font << /F1 5 0 R >>")
    outer = form(b"q 1 0 0 1 3 4 cm /Y Do Q", b"[1.5 0 0 1.5 20 30]", b"/Font << /F1 5 0 R >> /XObject << /Y 7 0 R >>")
    content = CONTROL + b"q 1 0 0 1 40 0 cm /X Do Q"
    return pdf(tmp_path, content, resources=b"/XObject << /X 6 0 R >>", extra=(outer, inner))


@dataclass(frozen=True)
class Placement:
    """Where a technique is drawn: its builder, its text origin on the displayed page, and more.

    ``scale`` is the enclosing forms' scale (1 off a form); ``axis`` the
    displayed axis the text advances along (0 for x, 1 for y);
    ``first_index`` the walk index of the technique's first text object.
    """

    build: object
    point: tuple[float, float]
    scale: float
    axis: int
    first_index: int


PLACEMENTS = [
    pytest.param(Placement(plain, (72.0, 200.0), 1.0, 0, 1), id="plain"),
    # /Rotate 90 shows user (x, y) at display (y, x) on this page: the text runs down.
    pytest.param(Placement(rotated, (100.0, 72.0), 1.0, 1, 1), id="rotate-90"),
    # CropBox (50, 40)-(350, 280): display (72 - 50, 280 - 100).
    pytest.param(Placement(cropped, (22.0, 180.0), 1.0, 0, 1), id="crop-box"),
    # Objects 1 and 2 are forms X and Y; the text is object 3.
    pytest.param(Placement(nested, (109.5, 219.0), 3.0, 0, 3), id="nested-forms"),
]


def recovered(path: Path) -> RecoveredText:
    """recover_dropped_text of page 1 of ``path``."""
    return recover_dropped_text(open_document(path), 0)


def fails_on_call(name: str, call: int, value: int = 0):
    """A stand-in for pdfium function ``name`` that reports failure (``value``) on its ``call``-th call only."""
    real = getattr(pdfium_c, name)
    calls = itertools.count(1)
    return lambda *args: value if next(calls) == call else real(*args)


def dropped_count(path: Path) -> int:
    """TextStats.dropped_text_objects of page 1 of ``path``."""
    return page_text(open_document(path), 0).stats.dropped_text_objects


def ownerless_counts(path: Path) -> tuple[int, int]:
    """TextStats ``(dropped_text_objects, empty_text_objects)`` of page 1 of ``path``."""
    got = page_text(open_document(path), 0).stats
    return got.dropped_text_objects, got.empty_text_objects


def text_objects(page: pdfium.PdfPage) -> list:
    """The raw handles of the page's text objects, forms' included, in walk order.

    The handles live only as long as ``page``: callers keep a reference to it.
    """
    return [item.raw for item in page.get_objects(filter=(pdfium_c.FPDF_PAGEOBJ_TEXT,), max_depth=64)]


def owners(page: pdfium.PdfPage) -> set[int]:
    """The addresses of the text objects that own a char of a fresh textpage of ``page``."""
    textpage = page.get_textpage()
    try:
        return {object_address(pdfium_c.FPDFText_GetTextObject(textpage.raw, i)) for i in range(textpage.count_chars())}
    finally:
        textpage.close()


def copy_dropped(tmp_path: Path, text: bytes, dx: float, dy: float) -> bool:
    """Whether pdfium drops a 12 pt copy of ``text`` drawn ``(dx, dy)`` from the text at (72, 100)."""
    content = b"BT /F1 12 Tf 72 100 Td (%s) Tj ET BT /F1 12 Tf %g %g Td (%s) Tj ET" % (text, 72 + dx, 100 + dy, text)
    page = pdfium.PdfDocument(pdf(tmp_path, content))[0]
    return object_address(text_objects(page)[1]) not in owners(page)


def object_text(text_object, page: pdfium.PdfPage) -> str:
    """FPDFTextObj_GetText of ``text_object`` on a fresh textpage of ``page``."""
    textpage = page.get_textpage()
    try:
        size = pdfium_c.FPDFTextObj_GetText(text_object, textpage.raw, None, 0)
        buffer = ctypes.create_string_buffer(size)
        pdfium_c.FPDFTextObj_GetText(
            text_object, textpage.raw, ctypes.cast(buffer, ctypes.POINTER(pdfium_c.FPDF_WCHAR)), size
        )
        return buffer.raw[: max(size - 2, 0)].decode("utf-16-le")
    finally:
        textpage.close()


def matrix_of(text_object) -> tuple[float, ...]:
    matrix = pdfium_c.FS_MATRIX()
    assert pdfium_c.FPDFPageObj_GetMatrix(text_object, matrix)
    return matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f


def bounds_of(text_object) -> tuple[float, ...]:
    edges = [ctypes.c_float() for _ in range(4)]
    assert pdfium_c.FPDFPageObj_GetBounds(text_object, *edges)
    return tuple(edge.value for edge in edges)


def set_size(text_object, size: float) -> None:
    assert pdfium_c.FPDFTextObj_SetFontSize(text_object, ctypes.c_float(size))


def set_matrix(text_object, a: float, b: float, c: float, d: float, e: float, f: float) -> None:
    assert pdfium_c.FPDFPageObj_SetMatrix(text_object, pdfium_c.FS_MATRIX(a, b, c, d, e, f))


def raw_span(text: str = "Ab", box=(72.0, 200.0, 72.0, 200.0), **overrides) -> RawSpan:
    """A RawSpan of ``text`` whose every char box is ``box``, as recovery makes them."""
    fields = {
        "text": text,
        "bbox": box,
        "char_boxes": (box,) * len(text),
        "font_size_pt": 0.0,
        "em_width_pt": 0.0,
        "base_font_name": "Helvetica",
        "is_bold": False,
        "is_italic": False,
        "render_mode": 0,
        "fill_rgba": (0, 0, 0, 255),
        "stroke_rgba": (0, 0, 0, 255),
        "object_index": 1,
        "generated_breaks": (),
        "map_errors": (),
    }
    return RawSpan(**(fields | overrides))


def recovered_span(**overrides) -> RecoveredSpan:
    return RecoveredSpan(**({"span": raw_span(), "hidden_reasons": ("tiny_font",)} | overrides))


def stats(**overrides) -> RecoveryStats:
    return RecoveryStats(
        **(
            {"overprint_duplicates": 1, "whitespace_objects": 2, "empty_text_objects": 4, "unrecovered_text_objects": 3}
            | overrides
        )
    )


class TestRecoveredSpan:
    """RecoveredSpan is a validated, immutable, picklable value."""

    def test_pickle_round_trip(self):
        original = recovered_span(hidden_reasons=("tiny_font", "zero_scale"))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = recovered_span()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.hidden_reasons = ()
        assert not hasattr(value, "__dict__")

    def test_fields(self):
        assert [item.name for item in dataclasses.fields(RecoveredSpan)] == ["span", "hidden_reasons"]

    @pytest.mark.parametrize("reasons", [(), ("tiny_font",), ("zero_scale",), ("tiny_font", "zero_scale")])
    def test_reasons_are_any_subset_of_the_recovery_reasons(self, reasons):
        assert recovered_span(hidden_reasons=reasons).hidden_reasons == reasons

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"span": "Ab"}, TypeError, "span must be a RawSpan, got str", id="span"),
            pytest.param(
                {"hidden_reasons": ["tiny_font"]}, TypeError, "hidden_reasons must be a tuple of str", id="reasons-list"
            ),
            pytest.param(
                {"hidden_reasons": ("zero_scale", "tiny_font")},
                ValueError,
                "hidden_reasons must be in canonical order ('tiny_font', 'zero_scale')",
                id="order",
            ),
            pytest.param(
                {"hidden_reasons": ("off_page",)},
                ValueError,
                "hidden_reasons of a recovered span must be drawn from ('tiny_font', 'zero_scale'), got ('off_page',)",
                id="not-a-recovery-reason",
            ),
            pytest.param(
                {
                    "span": raw_span(
                        bbox=(72.0, 200.0, 80.0, 210.0),
                        char_boxes=((72.0, 200.0, 76.0, 210.0), (76.0, 200.0, 80.0, 210.0)),
                    )
                },
                ValueError,
                "char_boxes[0] of a recovered span must be its bbox (72.0, 200.0, 80.0, 210.0)"
                " (the text object's box), got (72.0, 200.0, 76.0, 210.0)",
                id="per-char-boxes",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            recovered_span(**overrides)


class TestRecoveryStats:
    """RecoveryStats counts the dropped objects that give no span."""

    def test_pickle_round_trip_and_frozen(self):
        original = stats()

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.overprint_duplicates = 0
        assert not hasattr(original, "__dict__")

    def test_fields(self):
        names = [item.name for item in dataclasses.fields(RecoveryStats)]

        assert names == ["overprint_duplicates", "whitespace_objects", "empty_text_objects", "unrecovered_text_objects"]

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param(
                {"overprint_duplicates": -1}, ValueError, "overprint_duplicates must be >= 0, got -1", id="overprint"
            ),
            pytest.param(
                {"whitespace_objects": True}, TypeError, "whitespace_objects must be an int, got True", id="whitespace"
            ),
            pytest.param({"empty_text_objects": -1}, ValueError, "empty_text_objects must be >= 0, got -1", id="empty"),
            pytest.param(
                {"unrecovered_text_objects": 1.0},
                TypeError,
                "unrecovered_text_objects must be an int, got 1.0",
                id="unrecovered",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            stats(**overrides)


class TestRecoveredText:
    """RecoveredText pairs the recovered spans with the counts."""

    def test_pickle_round_trip(self):
        original = RecoveredText(page_number=2, spans=(recovered_span(),), stats=stats())

        assert pickle.loads(pickle.dumps(original)) == original

    def test_fields(self):
        assert [item.name for item in dataclasses.fields(RecoveredText)] == ["page_number", "spans", "stats"]

    @pytest.mark.parametrize(
        "page_number,spans,counts,error,message",
        [
            pytest.param(1, [recovered_span()], stats(), TypeError, "spans must be a tuple, got list", id="list"),
            pytest.param(
                1, (raw_span(),), stats(), TypeError, "spans[0] must be a RecoveredSpan, got RawSpan", id="item"
            ),
            pytest.param(1, (), None, TypeError, "stats must be a RecoveryStats, got NoneType", id="stats"),
            pytest.param(0, (), stats(), ValueError, "page_number must be >= 1, got 0", id="page-0"),
            pytest.param(True, (), stats(), TypeError, "page_number must be an int, got True (bool)", id="page-bool"),
        ],
    )
    def test_invalid_field_raises(self, page_number, spans, counts, error, message):
        with pytest.raises(error, match=re.escape(message)):
            RecoveredText(page_number=page_number, spans=spans, stats=counts)


class TestDroppedTextObjects:
    """TextStats counts the text objects that own no textpage char but draw glyphs: the pages worth a recovery."""

    @pytest.mark.parametrize("technique", DROPPING)
    def test_each_technique_drops_one_object(self, tmp_path, technique):
        assert ownerless_counts(plain(tmp_path, technique)) == (1, 0)

    @pytest.mark.parametrize("technique", DROPPING)
    def test_dropped_objects_inside_forms_count(self, tmp_path, technique):
        assert ownerless_counts(nested(tmp_path, technique)) == (1, 0)

    @pytest.mark.parametrize(
        "make_page",
        [
            pytest.param(lambda tmp_path: pdf(tmp_path, CONTROL), id="one-line"),
            pytest.param(
                lambda tmp_path: build_pdf(
                    tmp_path / "form.pdf",
                    [
                        PageSpec(
                            items=(FormXObject(x=72.0, y=72.0, w=200.0, h=40.0, items=(TextRun("In", 5.0, 20.0),)),)
                        )
                    ],
                ),
                id="form",
            ),
            pytest.param(
                lambda tmp_path: content_page(tmp_path, b"BT /F1 12 Tf 72 250 Td [(A) -2000 (B)] TJ ET"), id="kerned"
            ),
            pytest.param(lambda tmp_path: cid_page(tmp_path, b"000200010003"), id="cid"),
            pytest.param(lambda tmp_path: rtl_page(tmp_path, HEBREW_TWO_WORDS), id="hebrew"),
            pytest.param(lambda tmp_path: pdf(tmp_path, b""), id="empty"),
            pytest.param(
                lambda tmp_path: pdf(tmp_path, CONTROL + b"BT /F1 12 Tf 72 100 Td (  ) Tj ET"), id="two-space-object"
            ),
        ],
    )
    def test_a_page_whose_text_objects_all_own_chars_has_none(self, tmp_path, make_page):
        # Callers recover only when the count is over 0: these pages never pay the second load.
        assert ownerless_counts(make_page(tmp_path)) == (0, 0)

    def test_an_object_pdfium_places_nowhere_is_skipped_not_dropped(self, tmp_path):
        inner = b"BT /F1 12 Tf 0 0 Td (Lost) Tj ET"
        lost = form(inner, b"[1 0 0 1 0 0]", b"/Font << /F1 5 0 R >>")
        path = pdf(
            tmp_path, CONTROL + b"q " + OVERFLOW + b"/X Do Q", resources=b"/XObject << /X 6 0 R >>", extra=(lost,)
        )

        got = page_text(open_document(path), 0).stats

        assert (got.skipped_nonfinite_objects, got.dropped_text_objects) == (1, 0)

    def test_extract_page_counts_the_same(self, tmp_path):
        content = CONTROL + show("tf-0", 72, 100) + b" " + show("single-space", 72, 150)
        got = extract_page(open_document(pdf(tmp_path, content)), 0).text.stats

        assert (got.dropped_text_objects, got.empty_text_objects) == (1, 1)

    @pytest.mark.parametrize(
        "function,message",
        [
            pytest.param("FPDFPageObj_GetMatrix", "page 1: pdfium cannot read the matrix of object 1", id="matrix"),
            pytest.param("FPDFTextObj_GetFontSize", "page 1: pdfium cannot read the font size of object 1", id="size"),
            pytest.param("FPDFPageObj_GetBounds", "page 1: pdfium cannot read the bounds of object 1", id="bounds"),
        ],
    )
    def test_a_failure_classifying_an_ownerless_object_fails_the_text_stage(
        self, tmp_path, monkeypatch, handle_closes, function, message
    ):
        # Only objects that own no char are read: the control line's size is read before the stand-in fails.
        # A single space: its em is 12 pt, so its bounds are read too (a Tf 0 object's are not).
        handle = open_document(plain(tmp_path, "single-space"))
        call = 2 if function == "FPDFTextObj_GetFontSize" else 1
        first, second = fails_on_call(function, call), fails_on_call(function, call)
        monkeypatch.setattr(pdfium_c, function, first)

        with pytest.raises(PDFCorruptError, match=re.escape(message) + "$"):
            page_text(handle, 0)
        assert handle_closes == ["PdfTextPage", "PdfPage"]
        monkeypatch.setattr(pdfium_c, function, second)
        assert extract_page(handle, 0) == PageFailure(page_number=1, stage="text", message=message)


class TestEmptyTextObjects:
    """A text object that owns no char and draws no glyph with extent is empty: counted, never recovered."""

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"BT /F1 12 Tf 72 100 Td () Tj ET", id="empty-string"),
            pytest.param(b"BT /F1 12 Tf 72 100 Td [] TJ ET", id="empty-array"),
            pytest.param(b"BT /F1 12 Tf 72 100 Td [-500] TJ ET", id="kerning-only"),
            pytest.param(b"BT /F1 12 Tf 72 100 Td ET", id="no-show"),
        ],
    )
    def test_a_show_of_nothing_makes_no_text_object(self, tmp_path, content):
        # Observed pdfium behaviour: there is no object to count at all.
        path = pdf(tmp_path, CONTROL + content)
        page = pdfium.PdfDocument(path)[0]

        assert len(text_objects(page)) == 1
        assert ownerless_counts(path) == (0, 0)

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_a_single_space_object_is_empty_not_dropped(self, tmp_path, where):
        path = where.build(tmp_path, "single-space")

        assert ownerless_counts(path) == (0, 1)
        # Nothing to recover: a caller never loads the page again for it.
        assert recovered(path) == RecoveredText(page_number=1, spans=(), stats=NO_LOSS)

    @pytest.mark.parametrize(
        "content,counts",
        [
            pytest.param(b"BT /F1 12 Tf 72 100 Td (\240) Tj ET", (0, 1), id="no-break-space"),
            # Tj then ' (next line, show): two objects of a single space.
            pytest.param(b"BT /F1 12 Tf 14 TL 72 100 Td ( ) Tj ( ) ' ET", (0, 2), id="two-shows"),
            # Two spaces drawn on one spot by char spacing (see TestPdfiumBehaviour).
            pytest.param(b"BT /F1 1 Tf -0.278 Tc 10 0 0 10 72 100 Tm (  ) Tj ET", (0, 1), id="spaces-on-one-spot"),
        ],
    )
    def test_other_glyph_less_objects_are_empty(self, tmp_path, content, counts):
        assert ownerless_counts(pdf(tmp_path, CONTROL + content)) == counts

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(show("tf-0", 72, 100), id="tf-0"),
            pytest.param(b"q 0 0 0 0 0 0 cm BT /F1 12 Tf 72 100 Td (FLAT) Tj ET Q", id="zero-matrix"),
            pytest.param(b"BT /F1 0.5 Tf 72 100 Td ( ) Tj ET", id="tiny-space"),
        ],
    )
    def test_a_point_box_from_a_tiny_or_flat_em_is_dropped_not_empty(self, tmp_path, content):
        # Each box is a point, as a space's is, but its em is under 1 pt: re-laid out, it may draw.
        assert ownerless_counts(pdf(tmp_path, CONTROL + content)) == (1, 0)

    def test_a_dropped_object_still_a_point_after_re_layout_is_empty(self, tmp_path):
        got = recovered(pdf(tmp_path, CONTROL + b"BT /F1 0 Tf 72 100 Td ( ) Tj ET"))

        assert got == RecoveredText(page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, empty_text_objects=1))

    def test_empty_objects_beside_dropped_ones_are_not_recovered(self, tmp_path):
        content = CONTROL + show("single-space", 72, 150) + b" " + show("tf-0", 72, 100)
        path = pdf(tmp_path, content)

        got = recovered(path)

        assert ownerless_counts(path) == (1, 1)
        assert [(item.span.text, item.span.object_index) for item in got.spans] == [("HIDDEN", 2)]
        assert got.stats == NO_LOSS


class TestPdfiumBehaviour:
    """The pdfium behaviour recovery relies on (pypdfium2 5.14.0 / pdfium 156.0.8076.0)."""

    @pytest.mark.parametrize("technique", ["tf-0", "tiny", "tz-0", "single-space"])
    def test_a_dropped_object_owns_no_char_and_reads_no_text(self, tmp_path, technique):
        # FPDFTextObj_GetText is served from the textpage, so it cannot read a dropped object.
        page = pdfium.PdfDocument(plain(tmp_path, technique))[0]
        _control, hidden = text_objects(page)

        assert object_address(hidden) not in owners(page)
        assert object_text(hidden, page) == ""

    def test_tf_0_bounds_are_the_origin(self, tmp_path):
        page = pdfium.PdfDocument(plain(tmp_path, "tf-0"))[0]
        tf_0 = text_objects(page)[1]

        assert matrix_of(tf_0) == (1.0, 0.0, 0.0, 1.0, 72.0, 100.0)
        assert bounds_of(tf_0) == (72.0, 100.0, 72.0, 100.0)

    @pytest.mark.parametrize("text", [b"( )", b"(\240)"], ids=["space", "no-break-space"])
    def test_a_single_glyph_less_char_object_is_the_point_at_its_origin(self, tmp_path, text):
        page = pdfium.PdfDocument(pdf(tmp_path, CONTROL + b"BT /F1 12 Tf 72 100 Td %s Tj ET" % text))[0]
        hidden = text_objects(page)[1]
        size = ctypes.c_float()
        assert pdfium_c.FPDFTextObj_GetFontSize(hidden, size)

        assert (size.value, matrix_of(hidden), bounds_of(hidden)) == (
            12.0,
            (1.0, 0.0, 0.0, 1.0, 72.0, 100.0),
            (72.0, 100.0, 72.0, 100.0),
        )
        assert object_address(hidden) not in owners(page)

    def test_spaces_char_spacing_draws_on_one_spot_spread_apart_when_laid_out_again(self, tmp_path):
        # As on page 12 of the corpus's Learn to Play: Tf 1 under a 10x Tm, char spacing -0.278 (one
        # space's advance), so both spaces sit at the origin. Tc is not scaled by Tf: at 12 pt they part.
        content = CONTROL + b"BT /F1 1 Tf -0.278 Tc 10 0 0 10 72 100 Tm (  ) Tj ET"
        page = pdfium.PdfDocument(pdf(tmp_path, content))[0]
        hidden = text_objects(page)[1]
        assert bounds_of(hidden) == (72.0, 100.0, 72.0, 100.0)

        set_size(hidden, 12.0)
        set_matrix(hidden, 1, 0, 0, 1, 0, -100)

        left, bottom, right, top = bounds_of(hidden)
        assert (left, bottom, top) == (0.0, -100.0, -100.0) and right == pytest.approx(12 * 0.278 - 0.278, abs=1e-4)
        assert object_text(hidden, page) == " "

    def test_tz_0_is_in_the_matrix_and_leaves_no_width(self, tmp_path):
        page = pdfium.PdfDocument(plain(tmp_path, "tz-0"))[0]
        tz_0 = text_objects(page)[1]

        assert matrix_of(tz_0) == (0.0, 0.0, 0.0, 1.0, 72.0, 100.0)
        # The box is the glyphs' ink: "HIDDEN" sits on its baseline.
        left, bottom, right, top = bounds_of(tz_0)
        assert (left, right, bottom) == (72.0, 72.0, 100.0) and top > 108.0

    @pytest.mark.parametrize(
        "content,dropped",
        [
            pytest.param(b"BT /F1 12 Tf 0.01 Tz 72 100 Td (HIDDEN) Tj ET", True, id="tz-0.01"),
            pytest.param(b"BT /F1 12 Tf 0.05 Tz 72 100 Td (HIDDEN) Tj ET", False, id="tz-0.05"),
            pytest.param(b"BT /F1 0.002 Tf 72 100 Td (HIDDEN) Tj ET", True, id="size-0.002"),
            pytest.param(b"BT /F1 0.004 Tf 72 100 Td (HIDDEN) Tj ET", False, id="size-0.004"),
            pytest.param(b"BT /F1 0.001 Tf 72 100 Td (WWWWWWWWWWWWWWWWWWWWWWWWWWWWWW) Tj ET", False, id="long-0.001"),
        ],
    )
    def test_pdfium_drops_a_text_object_under_a_hundredth_of_a_point_wide(self, tmp_path, content, dropped):
        # "HIDDEN" is 3.8 em wide: 0.0046 pt at Tz 0.01 and 12 pt, 0.023 at Tz 0.05, 0.0077 at 0.002 pt,
        # 0.015 at 0.004 pt; 30 W at 0.001 pt are 0.028 pt wide.
        page = pdfium.PdfDocument(pdf(tmp_path, content))[0]
        (text_object,) = text_objects(page)

        assert (object_address(text_object) not in owners(page)) == dropped

    @pytest.mark.parametrize("technique", ["tf-0", "tiny"])
    def test_a_new_font_size_alone_recovers_nothing(self, tmp_path, technique):
        page = pdfium.PdfDocument(plain(tmp_path, technique))[0]
        hidden = text_objects(page)[1]

        set_size(hidden, 12.0)

        assert object_address(hidden) not in owners(page)

    @pytest.mark.parametrize("technique", ["tf-0", "tiny"])
    def test_a_new_matrix_after_the_size_lays_the_object_out_again(self, tmp_path, technique):
        page = pdfium.PdfDocument(plain(tmp_path, technique))[0]
        hidden = text_objects(page)[1]

        set_size(hidden, 12.0)
        set_matrix(hidden, 1, 0, 0, 1, 72, 100)

        assert object_text(hidden, page) == "HIDDEN"

    def test_a_new_size_after_the_matrix_recovers_nothing(self, tmp_path):
        page = pdfium.PdfDocument(plain(tmp_path, "tf-0"))[0]
        hidden = text_objects(page)[1]

        set_matrix(hidden, 1, 0, 0, 1, 72, 100)
        set_size(hidden, 12.0)

        assert object_address(hidden) not in owners(page)

    def test_an_overprint_stays_dropped_in_place_and_reads_once_moved(self, tmp_path):
        page = pdfium.PdfDocument(plain(tmp_path, "overprint"))[0]
        _control, _first, copy = text_objects(page)

        set_size(copy, 12.0)
        set_matrix(copy, 1, 0, 0, 1, 72, 100)
        assert object_address(copy) not in owners(page)

        set_matrix(copy, 1, 0, 0, 1, 0, -100)
        assert object_text(copy, page) == "Total"

    @pytest.mark.parametrize(
        "text,reach",
        [
            # 0.9 of the last glyph's advance at 12 pt: Helvetica's l is 222 units wide, a 556, W 944.
            pytest.param(b"Total", 0.9 * 0.222 * 12, id="ends-in-l"),
            pytest.param(b"Tota", 0.9 * 0.556 * 12, id="ends-in-a"),
            pytest.param(b"TOTAL DUE MW", 0.9 * 0.944 * 12, id="ends-in-W"),
        ],
    )
    @pytest.mark.parametrize("sign", [1, -1], ids=["right", "left"])
    def test_pdfium_drops_a_copy_within_nine_tenths_of_its_last_glyphs_advance(self, tmp_path, text, reach, sign):
        # Not 2 pt: "TOTAL DUE MW" goes 10.18 pt away, "Total" stays 2.41 pt away.
        assert copy_dropped(tmp_path, text, sign * (reach - 0.01), 0.0)
        assert not copy_dropped(tmp_path, text, sign * (reach + 0.01), 0.0)

    @pytest.mark.parametrize(
        "text,dropped_at,kept_at",
        [
            # Up to about an eighth of the copy's width (26 pt of "Total")...
            pytest.param(b"Total", 3.0, 3.3, id="an-eighth-of-the-width"),
            # ... and only while the two boxes overlap (these capitals are 8.6 pt tall).
            pytest.param(b"TOTAL DUE MW", 8.0, 9.0, id="while-the-boxes-overlap"),
        ],
    )
    def test_pdfium_drops_a_copy_shifted_across_its_baseline_within_a_window(self, tmp_path, text, dropped_at, kept_at):
        assert copy_dropped(tmp_path, text, 0.0, dropped_at) and copy_dropped(tmp_path, text, 0.0, -dropped_at)
        assert not copy_dropped(tmp_path, text, 0.0, kept_at) and not copy_dropped(tmp_path, text, 0.0, -kept_at)

    def test_pdfium_drops_a_shrunk_copy_within_its_unscaled_window(self, tmp_path):
        # Tf 100 under a 0.1 cm: "Total" is 2.7 pt per em, but the window is still 0.9 of a 100 pt "l".
        shrunk = (
            b"q 0.1 0 0 0.1 0 0 cm BT /F1 100 Tf 720 1000 Td (Total) Tj ET BT /F1 100 Tf %g 1000 Td (Total) Tj ET Q"
        )

        page = pdfium.PdfDocument(pdf(tmp_path, shrunk % (720 + 100)))[0]

        assert object_address(text_objects(page)[1]) not in owners(page)

    def test_pdfium_drops_a_copy_as_far_as_the_fonts_glyph_widths_say(self, tmp_path):
        # Glyphs 5 em wide: the window is 0.9 of a 60 pt advance, so a copy 30 pt away goes.
        copy = b"BT /F1 12 Tf 72 100 Td (To) Tj ET BT /F1 12 Tf 102 100 Td (To) Tj ET"
        page = pdfium.PdfDocument(pdf(tmp_path, copy, font=WIDE_GLYPHS))[0]

        assert object_address(text_objects(page)[1]) not in owners(page)

    @pytest.mark.parametrize("between,dropped", [(4, True), (5, False)])
    def test_pdfium_looks_for_the_text_a_copy_copies_among_the_five_text_objects_before_it(
        self, tmp_path, between, dropped
    ):
        others = b"".join(b"BT /F1 12 Tf 72 %d Td (x%d) Tj ET " % (200 + 10 * i, i) for i in range(between))
        content = b"BT /F1 12 Tf 72 100 Td (Total) Tj ET " + others + b"BT /F1 12 Tf 72 100 Td (Total) Tj ET"
        page = pdfium.PdfDocument(pdf(tmp_path, content))[0]

        assert (object_address(text_objects(page)[-1]) not in owners(page)) == dropped

    @pytest.mark.parametrize("matrix", [(1, 0, 0, 1, 72, 100), (1, 0, 0, 1, 0, -100), (5, 0, 0, 5, 0, -100)])
    def test_a_single_space_object_stays_dropped_whatever_its_size_and_matrix(self, tmp_path, matrix):
        # Its one glyph has no outline, so its box has no width at any size.
        page = pdfium.PdfDocument(plain(tmp_path, "single-space"))[0]
        hidden = text_objects(page)[1]

        set_size(hidden, 12.0)
        set_matrix(hidden, *matrix)

        assert object_address(hidden) not in owners(page)
        assert bounds_of(hidden)[0] == bounds_of(hidden)[2]

    def test_object_edits_live_only_in_their_page_load(self, tmp_path):
        document = pdfium.PdfDocument(plain(tmp_path, "tf-0"))
        page = document[0]
        hidden = text_objects(page)[1]
        set_size(hidden, 12.0)
        set_matrix(hidden, 1, 0, 0, 1, 72, 100)
        assert object_address(hidden) in owners(page)
        page.close()

        fresh = document[0]

        assert object_address(text_objects(fresh)[1]) not in owners(fresh)


def assert_at_point(box, point, *, tolerance: float = 1e-6) -> None:
    assert box == pytest.approx((point[0], point[1], point[0], point[1]), abs=tolerance)


class TestRecovery:
    """recover_dropped_text reads back each dropped object's text, flagged, at the object's own place."""

    def test_tf_0_text_is_recovered_whole(self, tmp_path):
        box = (72.0, 200.0, 72.0, 200.0)
        expected = RecoveredSpan(span=raw_span("HIDDEN", box), hidden_reasons=("tiny_font",))

        assert recovered(plain(tmp_path, "tf-0")) == RecoveredText(page_number=1, spans=(expected,), stats=NO_LOSS)

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_tf_0(self, tmp_path, where):
        got = recovered(where.build(tmp_path, "tf-0"))

        (item,) = got.spans
        assert (item.span.text, item.hidden_reasons, item.span.object_index) == (
            "HIDDEN",
            ("tiny_font",),
            where.first_index,
        )
        assert item.span.font_size_pt == 0.0
        for box in item.span.char_boxes:
            assert_at_point(box, where.point)
        assert got.stats == NO_LOSS

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_tiny(self, tmp_path, where):
        got = recovered(where.build(tmp_path, "tiny"))

        (item,) = got.spans
        assert (item.span.text, item.hidden_reasons, item.span.object_index) == (
            "HIDDEN",
            ("tiny_font",),
            where.first_index,
        )
        assert item.span.font_size_pt == pytest.approx(0.0005 * where.scale, rel=1e-4)
        # "HIDDEN" at 0.0005 pt is 0.0032 pt wide.
        assert_at_point(item.span.bbox, where.point, tolerance=0.005 * where.scale)
        assert got.stats == NO_LOSS

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_tz_0(self, tmp_path, where):
        got = recovered(where.build(tmp_path, "tz-0"))

        (item,) = got.spans
        assert (item.span.text, item.hidden_reasons, item.span.object_index) == (
            "HIDDEN",
            ("zero_scale",),
            where.first_index,
        )
        assert item.span.font_size_pt == pytest.approx(12.0 * where.scale)
        box, along, across = item.span.bbox, where.axis, 1 - where.axis
        # No extent along the text; across it, the glyphs' ink from the baseline up (cap height 0.72 em).
        assert box[along] == pytest.approx(box[along + 2]) == pytest.approx(where.point[along])
        assert box[across] <= where.point[across] + 1e-6 and where.point[across] <= box[across + 2] + 1e-6
        assert box[across + 2] - box[across] == pytest.approx(0.716 * 12.0 * where.scale, rel=0.01)
        assert got.stats == NO_LOSS

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_overprint(self, tmp_path, where):
        got = recovered(where.build(tmp_path, "overprint"))

        assert got == RecoveredText(page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, overprint_duplicates=1))

    def test_type3_font(self, tmp_path):
        font = (
            b"<< /Type /Font /Subtype /Type3 /FontBBox [0 0 750 750] /FontMatrix [0.001 0 0 0.001 0 0]"
            b" /CharProcs << /a 6 0 R /b 6 0 R >> /Encoding << /Type /Encoding /Differences [97 /a /b] >>"
            b" /FirstChar 97 /LastChar 98 /Widths [1000 1000] /Resources << >> >>"
        )
        glyph = stream(b"1000 0 0 0 750 750 d1 0 0 750 750 re f")
        content = b"BT /F1 12 Tf 20 270 Td (abab) Tj ET BT /F1 0 Tf 72 100 Td (ab) Tj ET"

        (item,) = recovered(pdf(tmp_path, content, font=font, extra=(glyph,))).spans

        assert (item.span.text, item.span.base_font_name, item.hidden_reasons) == ("ab", "", ("tiny_font",))
        assert_at_point(item.span.bbox, (72.0, 200.0))

    def test_cid_font(self, tmp_path):
        content = b"BT /F1 20 Tf 20 250 Td <00010002> Tj ET BT /F1 0 Tf 72 100 Td <00020001> Tj ET"
        path = cid_font_page(tmp_path, content, bfchar_cmap({b"0001": b"4E2D", b"0002": b"6587"}))

        (item,) = recovered(path).spans

        assert (item.span.text, item.span.base_font_name, item.hidden_reasons) == ("文中", "TestCID", ("tiny_font",))
        assert_at_point(item.span.bbox, (72.0, 200.0))

    def test_whitespace_only_text_is_counted_not_emitted(self, tmp_path):
        got = recovered(pdf(tmp_path, CONTROL + b"BT /F1 0 Tf 72 100 Td (   ) Tj ET"))

        assert got == RecoveredText(page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, whitespace_objects=1))

    def test_a_hidden_copy_of_hidden_text_reads_once(self, tmp_path):
        content = CONTROL + b"BT /F1 0 Tf 72 100 Td (Dup) Tj ET BT /F1 0 Tf 72 100 Td (Dup) Tj ET"

        got = recovered(pdf(tmp_path, content))

        assert [(item.span.text, item.span.object_index) for item in got.spans] == [("Dup", 1)]
        assert got.stats == dataclasses.replace(NO_LOSS, overprint_duplicates=1)

    def test_a_copy_within_half_a_point_is_an_overprint(self, tmp_path):
        content = CONTROL + b"BT /F1 12 Tf 72 100 Td (Total) Tj ET BT /F1 12 Tf 72.4 99.6 Td (Total) Tj ET"

        got = recovered(pdf(tmp_path, content))

        assert got == RecoveredText(page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, overprint_duplicates=1))

    @pytest.mark.parametrize(
        "first,second,mode,reasons",
        [
            # pdfium keeps the first copy, which paints nothing, and drops the one a reader sees.
            pytest.param((b"", b"3 Tr"), (b"", b""), 0, (), id="invisible-then-filled"),
            pytest.param((b"", b""), (b"", b"1 Tr"), 1, (), id="filled-then-stroked"),
            pytest.param((b"/Clear gs", b""), (b"", b""), 0, (), id="transparent-then-opaque"),
            pytest.param((b"", b""), (b"/Faint gs", b""), 0, (), id="opaque-then-faint"),
            # Alike but for colour: a white copy on the white page, then the black one a reader sees.
            pytest.param((b"1 1 1 rg", b""), (b"", b""), 0, (), id="white-then-black"),
            pytest.param((b"", b""), (b"1 0 0 rg", b""), 0, (), id="black-then-red"),
            pytest.param((b"1 1 1 RG", b"1 Tr"), (b"", b"1 Tr"), 1, (), id="white-stroke-then-black"),
            # The copy pdfium drops for its width (Tz 0) is hidden though the text it copies is not.
            pytest.param((b"", b""), (b"", b"0 Tz"), 0, ("zero_scale",), id="drawn-then-squeezed"),
        ],
    )
    def test_a_copy_painted_unlike_the_text_it_copies_is_emitted(self, tmp_path, first, second, mode, reasons):
        show_copy = b"q %s BT /F1 12 Tf %s 72 100 Td (Total) Tj ET Q "
        content = CONTROL + show_copy % first + show_copy % second
        states = b"<< /Type /ExtGState /ca 0 /CA 0 >>", b"<< /Type /ExtGState /ca 0.5 /CA 0.5 >>"
        path = pdf(tmp_path, content, resources=b"/ExtGState << /Clear 6 0 R /Faint 7 0 R >>", extra=states)

        got = recovered(path)

        (item,) = got.spans
        assert (item.span.text, item.span.object_index, item.span.render_mode) == ("Total", 2, mode)
        assert (item.hidden_reasons, got.stats) == (reasons, NO_LOSS)

    @pytest.mark.parametrize(
        "text,dx,dy",
        [
            pytest.param(b"Total", 1.0, -1.0, id="drop-shadow"),
            pytest.param(b"Total", 2.0, 0.0, id="fake-bold"),
            pytest.param(b"Total", -2.3, 0.0, id="left"),
            pytest.param(b"Total", 0.0, 3.0, id="across"),
            pytest.param(b"TOTAL DUE MW", 10.1, 0.0, id="nine-tenths-of-a-W"),
            pytest.param(b"TOTAL DUE MW", 0.0, -8.0, id="across-while-overlapping"),
        ],
    )
    def test_a_copy_within_pdfiums_duplicate_window_is_an_overprint(self, tmp_path, text, dx, dy):
        # Drawn on the text it copies as pdfium's window allows (TestPdfiumBehaviour): counted, not emitted.
        copy = b"BT /F1 12 Tf 72 100 Td (%s) Tj ET BT /F1 12 Tf %g %g Td (%s) Tj ET" % (text, 72 + dx, 100 + dy, text)

        got = recovered(pdf(tmp_path, CONTROL + copy))

        assert got == RecoveredText(page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, overprint_duplicates=1))

    @pytest.mark.parametrize(
        "first_x,second_x",
        [
            # The first copy runs past the right edge; the copy 2.3 pt left of it lies on the page.
            pytest.param(375.0, 372.7, id="off-page-then-on"),
            pytest.param(372.7, 375.0, id="on-page-then-off"),
        ],
    )
    def test_a_copy_placed_unlike_the_text_it_copies_is_emitted(self, tmp_path, first_x, second_x):
        # pdfium drops the second copy (within 2.4 pt of a 12 pt "l"); one shows a char the other does not.
        copies = b"BT /F1 12 Tf %g 100 Td (Total) Tj ET BT /F1 12 Tf %g 100 Td (Total) Tj ET" % (first_x, second_x)

        got = recovered(pdf(tmp_path, CONTROL + copies))

        (item,) = got.spans
        assert (item.span.text, item.span.object_index, item.hidden_reasons, got.stats) == ("Total", 2, (), NO_LOSS)

    @pytest.mark.parametrize("dx,counted", [(30.0, False), (12.5, False), (11.5, True)])
    def test_the_duplicate_window_is_capped_at_12_pt_whatever_the_font_says(self, tmp_path, dx, counted):
        # A font whose glyphs claim 5 em each: pdfium drops a copy up to 54 pt away (TestPdfiumBehaviour),
        # which a reader sees beside the first. Recovery's window stops at 12 pt: further, the copy is emitted.
        copy = b"BT /F1 12 Tf 72 100 Td (To) Tj ET BT /F1 12 Tf %g 100 Td (To) Tj ET" % (72 + dx)

        got = recovered(pdf(tmp_path, CONTROL + copy, font=WIDE_GLYPHS))

        assert got.stats == dataclasses.replace(NO_LOSS, overprint_duplicates=int(counted))
        assert [(item.span.text, item.span.object_index, item.hidden_reasons) for item in got.spans] == (
            [] if counted else [("To", 2, ())]
        )

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_a_drop_shadow_is_an_overprint(self, tmp_path, where):
        got = recovered(where.build(tmp_path, "drop-shadow"))

        assert got == RecoveredText(page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, overprint_duplicates=1))

    def test_a_shrunk_copy_beyond_its_last_glyph_is_emitted_without_a_reason(self, tmp_path):
        # pdfium's window does not shrink with the text (TestPdfiumBehaviour); recovery's is the last
        # glyph's box (2.2 pt here), so a copy 10 pt away is emitted: it is drawn, so nothing hides it.
        shrunk = (
            b"q 0.1 0 0 0.1 0 0 cm BT /F1 100 Tf 720 1000 Td (Total) Tj ET BT /F1 100 Tf 820 1000 Td (Total) Tj ET Q"
        )

        (item,) = recovered(pdf(tmp_path, CONTROL + shrunk)).spans

        assert (item.span.text, item.span.object_index, item.hidden_reasons) == ("Total", 2, ())
        assert item.span.font_size_pt == pytest.approx(10.0)

    def test_several_dropped_objects_come_back_in_walk_order(self, tmp_path):
        content = CONTROL + b"BT /F1 0 Tf 72 100 Td (ONE) Tj ET q BT /F1 12 Tf 0 Tz 72 150 Td (TWO) Tj ET Q"

        got = recovered(pdf(tmp_path, content))

        summary = [(item.span.text, item.hidden_reasons, item.span.object_index) for item in got.spans]
        assert summary == [("ONE", ("tiny_font",), 1), ("TWO", ("zero_scale",), 2)]

    @pytest.mark.parametrize(
        "content,reasons,size",
        [
            # Squeezed to 0.12 pt per em: not 0, still flat.
            pytest.param(b"q BT /F1 12 Tf 0.001 Tz 72 100 Td (HIDDEN) Tj ET Q", ("zero_scale",), 12.0, id="tz-0.001"),
            # A quarter turn is neither squeezed nor small.
            pytest.param(b"BT /F1 0 Tf 0 1 -1 0 200 50 Tm (HIDDEN) Tj ET", ("tiny_font",), 0.0, id="turned-tf-0"),
            # A negative size draws mirrored glyphs at its magnitude.
            pytest.param(b"BT /F1 -0.0005 Tf 72 100 Td (HIDDEN) Tj ET", ("tiny_font",), -0.0005, id="negative-tiny"),
        ],
    )
    def test_reasons_and_size_come_from_the_original_object(self, tmp_path, content, reasons, size):
        (item,) = recovered(pdf(tmp_path, CONTROL + content)).spans

        assert (item.span.text, item.hidden_reasons) == ("HIDDEN", reasons)
        assert item.span.font_size_pt == pytest.approx(size, rel=1e-4)

    @pytest.mark.parametrize(
        "content,width",
        [
            pytest.param(show("tf-0", 72, 100), 0.0, id="tf-0"),
            pytest.param(show("tz-0", 72, 100), 0.0, id="tz-0"),
            pytest.param(show("tiny", 72, 100), 0.0005, id="tiny"),
            # 0.001 % of 12 pt.
            pytest.param(b"q BT /F1 12 Tf 0.001 Tz 72 100 Td (HIDDEN) Tj ET Q", 0.00012, id="tz-0.001"),
            pytest.param(b"BT /F1 0 Tf 0 1 -1 0 200 50 Tm (HIDDEN) Tj ET", 0.0, id="turned-tf-0"),
            # A drawn copy of invisible text (pdfium drops it as a duplicate) keeps its full 12 pt em.
            pytest.param(
                b"q BT /F1 12 Tf 3 Tr 72 100 Td (Total) Tj ET Q BT /F1 12 Tf 72 100 Td (Total) Tj ET",
                12.0,
                id="drawn-copy",
            ),
        ],
    )
    def test_em_width_comes_from_the_original_object(self, tmp_path, content, width):
        (item,) = recovered(pdf(tmp_path, CONTROL + content)).spans

        assert item.span.em_width_pt == pytest.approx(width, rel=1e-4)

    @pytest.mark.parametrize("where", PLACEMENTS)
    def test_em_width_takes_the_forms_scale(self, tmp_path, where):
        (item,) = recovered(where.build(tmp_path, "tiny")).spans

        assert item.span.em_width_pt == pytest.approx(0.0005 * where.scale, rel=1e-4)

    def test_both_reasons_when_the_text_is_tiny_and_squeezed(self, tmp_path):
        got = recovered(pdf(tmp_path, CONTROL + b"q BT /F1 0.5 Tf 0 Tz 72 100 Td (BOTH) Tj ET Q"))

        assert [item.hidden_reasons for item in got.spans] == [("tiny_font", "zero_scale")]

    def test_a_zero_scale_matrix_is_tiny_not_squeezed(self, tmp_path):
        # 0 0 0 0 cm leaves no extent either way: the text is 0 pt, not narrower than tall.
        (item,) = recovered(pdf(tmp_path, CONTROL + b"q 0 0 0 0 0 0 cm BT /F1 12 Tf 72 100 Td (FLAT) Tj ET Q")).spans

        assert (item.span.text, item.hidden_reasons, item.span.font_size_pt) == ("FLAT", ("tiny_font",), 0.0)

    @pytest.mark.parametrize("matrix", [b"0 1 0 1", b"0.00001 1 0 1"], ids=["rank-1", "sheared-flat"])
    def test_a_collapsed_matrix_is_tiny(self, tmp_path, matrix):
        # Both em sides are 12 pt long but (almost) on one line: pdfium drops the text, which draws a line.
        content = CONTROL + b"BT /F1 12 Tf %s 72 100 Tm (HIDDEN) Tj ET" % matrix

        (item,) = recovered(pdf(tmp_path, content)).spans

        assert (item.span.text, item.hidden_reasons) == ("HIDDEN", ("tiny_font",))
        assert (item.span.font_size_pt, item.span.em_width_pt) == pytest.approx((0.0, 0.0), abs=1e-3)

    def test_generated_breaks_are_kept(self, tmp_path):
        got = recovered(pdf(tmp_path, CONTROL + b"BT /F1 0 Tf 72 100 Td [(Name) -12000 (Value)] TJ ET"))

        assert [(item.span.text, item.span.generated_breaks) for item in got.spans] == [("NameValue", (4,))]

    def test_text_under_a_form_matrix_that_is_not_finite_is_unrecovered(self, tmp_path):
        hidden = form(b"BT /F1 0 Tf 0 0 Td (Lost) Tj ET", b"[1 0 0 1 0 0]", b"/Font << /F1 5 0 R >>")
        content = CONTROL + b"q " + OVERFLOW + b"/X Do Q"
        path = pdf(tmp_path, content, resources=b"/XObject << /X 6 0 R >>", extra=(hidden,))

        assert recovered(path) == RecoveredText(
            page_number=1, spans=(), stats=dataclasses.replace(NO_LOSS, unrecovered_text_objects=1)
        )

    def test_a_page_with_nothing_dropped_recovers_nothing(self, tmp_path):
        assert recovered(pdf(tmp_path, CONTROL)) == RecoveredText(page_number=1, spans=(), stats=NO_LOSS)

    def test_every_dropped_object_is_accounted_for(self, tmp_path):
        # Operators need whitespace between them: "ETBT" would be one unknown operator.
        shows = (
            show("tf-0", 72, 100),
            show("overprint", 72, 150),
            show("single-space", 72, 200),  # empty: never dropped
            b"BT /F1 0 Tf 72 50 Td (  ) Tj ET",  # whitespace once read
            b"BT /F1 0 Tf 300 50 Td ( ) Tj ET",  # still a point once laid out again
        )
        path = pdf(tmp_path, CONTROL + b" ".join(shows))

        got = recovered(path)

        assert got.stats == RecoveryStats(
            overprint_duplicates=1, whitespace_objects=1, empty_text_objects=1, unrecovered_text_objects=0
        )
        assert ownerless_counts(path) == (4, 1)
        assert len(got.spans) + sum(dataclasses.astuple(got.stats)) == 4

    def test_the_document_keeps_no_edit(self, tmp_path):
        handle = open_document(plain(tmp_path, "tf-0"))
        before = extract_page(handle, 0)

        recover_dropped_text(handle, 0)

        assert extract_page(handle, 0) == before
        assert page_text(handle, 0).spans == before.text.spans

    def test_the_recovery_names_its_page(self, tmp_path):
        pages = [PageSpec(items=(TextRun(f"Page {n}", x=72.0, y=100.0),)) for n in (1, 2, 3)]
        handle = open_document(build_pdf(tmp_path / "three.pdf", pages))

        assert [recover_dropped_text(handle, index).page_number for index in range(3)] == [1, 2, 3]

    def test_pickle_round_trip_of_a_recovery(self, tmp_path):
        original = recovered(plain(tmp_path, "tz-0"))

        assert pickle.loads(pickle.dumps(original)) == original


@pytest.fixture
def handle_closes(monkeypatch):
    """Record each explicit PdfPage / PdfTextPage close() call (not a parent's cascade), by type name."""
    calls = []
    for cls in (pdfium.PdfPage, pdfium.PdfTextPage):

        def counting_close(self, *args, _original=cls.close, **kwargs):
            if not kwargs.get("_by_parent", False):
                calls.append(type(self).__name__)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(cls, "close", counting_close)
    return calls


def failing(value):
    """A pdfium stand-in that reports failure by returning ``value``."""
    return lambda *args: value


TEXTPAGES_THEN_PAGE = ["PdfTextPage", "PdfTextPage", "PdfPage"]

# (pdfium function, stand-in maker, message, closes): each stand-in makes recovery of a page
# holding only a Tf 0 object (object 0) raise PDFCorruptError for page 1. The text stage reads
# the object's matrix and size (is it empty? Its em is 0, so its bounds are not read), then
# recovery reads its size and bounds (its original place), and its bounds again once it is
# moved (is it still a point?).
RECOVERY_FAILURES = [
    ("FPDFPageObj_GetMatrix", lambda: failing(0), "page 1: pdfium cannot read the matrix of object 0", ["PdfPage"]),
    (
        "FPDFText_CountChars",
        lambda: failing(-1),
        "page 1: pdfium cannot count the text page's chars",
        ["PdfTextPage", "PdfPage"],
    ),
    *(
        (function, maker, f"page 1: pdfium cannot read the {what} of object 0", ["PdfTextPage", "PdfPage"])
        for function, what in (("FPDFTextObj_GetFontSize", "font size"), ("FPDFPageObj_GetBounds", "bounds"))
        for maker in (lambda: failing(0), partial(fails_on_call, function, 2))
    ),
    (
        "FPDFTextObj_SetFontSize",
        lambda: failing(0),
        "page 1: pdfium cannot set the font size of object 0",
        ["PdfTextPage", "PdfPage"],
    ),
    (
        "FPDFPageObj_SetMatrix",
        lambda: failing(0),
        "page 1: pdfium cannot set the matrix of object 0",
        ["PdfTextPage", "PdfPage"],
    ),
]


def raises_bug(*args, **kwargs):
    raise ZeroDivisionError("bug")


class TestRecoveryErrors:
    """Recovery closes its textpages and page on every path, and raises PDFCorruptError for the page."""

    def test_closes_both_textpages_then_the_page(self, tmp_path, handle_closes):
        recovered(plain(tmp_path, "tf-0"))

        assert handle_closes == TEXTPAGES_THEN_PAGE

    @pytest.mark.parametrize("function,make_stand_in,message,closes", RECOVERY_FAILURES)
    def test_pdfium_failure_raises_for_the_page_and_closes_handles(
        self, tmp_path, monkeypatch, handle_closes, function, make_stand_in, message, closes
    ):
        handle = open_document(pdf(tmp_path, show("tf-0", 72, 100)))
        monkeypatch.setattr(pdfium_c, function, make_stand_in())

        with pytest.raises(PDFCorruptError, match=re.escape(message) + "$") as caught:
            recover_dropped_text(handle, 0)

        assert caught.value.page_number == 1
        assert handle_closes == closes

    def test_textpage_load_failure_raises_and_closes_the_page(self, tmp_path, monkeypatch, handle_closes):
        def failing_textpage(self):
            raise pdfium.PdfiumError("Failed to load text page.")

        handle = open_document(plain(tmp_path, "tf-0"))
        monkeypatch.setattr(pdfium.PdfPage, "get_textpage", failing_textpage)

        with pytest.raises(PDFCorruptError, match="page 1: pdfium cannot load the text page") as caught:
            recover_dropped_text(handle, 0)

        assert caught.value.page_number == 1
        assert handle_closes == ["PdfPage"]

    def test_a_bug_raises_and_closes_the_page(self, tmp_path, monkeypatch, handle_closes):
        handle = open_document(plain(tmp_path, "tf-0"))
        monkeypatch.setattr(pdfium_engine, "collect_text", raises_bug)

        with pytest.raises(ZeroDivisionError, match="^bug$"):
            recover_dropped_text(handle, 0)

        assert handle_closes == ["PdfTextPage", "PdfPage"]

    def test_unloadable_page_raises_for_that_page(self, tmp_path):
        pages = [PageSpec(items=(TextRun(f"Page {n}", x=72.0, y=100.0),)) for n in (1, 2, 3)]
        handle = open_document(break_page(build_pdf(tmp_path / "three.pdf", pages), 2))

        with pytest.raises(PDFCorruptError, match="page 2 is unreadable") as caught:
            recover_dropped_text(handle, 1)

        assert caught.value.page_number == 2
        assert recover_dropped_text(handle, 2) == RecoveredText(page_number=3, spans=(), stats=NO_LOSS)

    @pytest.mark.parametrize(
        "index,error,message",
        [
            pytest.param(1, IndexError, "index must be in 0..0, got 1", id="past-end"),
            pytest.param(True, TypeError, "index must be an int, got True (bool)", id="bool"),
        ],
    )
    def test_invalid_index_raises(self, tmp_path, index, error, message):
        with pytest.raises(error, match=re.escape(message) + "$"):
            recover_dropped_text(open_document(plain(tmp_path, "tf-0")), index)

    def test_non_handle_and_closed_handle_raise(self, tmp_path):
        handle = open_document(plain(tmp_path, "tf-0"))
        handle.close()

        with pytest.raises(TypeError, match="handle must be a DocumentHandle, got str"):
            recover_dropped_text("doc.pdf", 0)
        with pytest.raises(ValueError, match="document handle is closed"):
            recover_dropped_text(handle, 0)
