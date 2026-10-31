"""Text spans for the v5 pdfium engine (#365 T2).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf.pdfium_engine` re-exports :class:`RawSpan`,
:class:`TextStats` and :class:`PageText`, and calls :func:`collect_text` with
a loaded textpage. This module never imports the engine.

Text (#365 T2), pinned by ``tests/pdf_tests/test_pdfium_spans.py``:

- pdfium makes one text object per text-showing operator (``Tj``, ``TJ``,
  ``'``, ``"``), so one object's chars share one baseline. Non-generated
  textpage chars name their owner (``FPDFText_GetTextObject``), except the
  space pdfium inserts between two right-to-left words drawn by separate
  operators: it is not flagged generated and has no owner. A generated
  space inside one object (a wide ``TJ`` gap) names that object.
- Right-to-left text is in logical order: a Hebrew word drawn left to right
  in visual order reads from its rightmost glyph.
- Textpage char boxes and ``FPDFText_GetMatrix`` are page space with every
  enclosing form's matrix applied; ``get_bounds()`` and
  ``FPDFPageObj_GetMatrix`` of an object inside a form are in the form's
  space. pdfium parses forms at most 40 levels deep.
- A non-BMP char is two textpage chars, a UTF-16 surrogate pair with equal
  boxes. An unmapped char code reads as itself, flagged by
  ``FPDFText_HasUnicodeMapError`` (its offset is in ``RawSpan.map_errors``);
  without a ToUnicode map a 3- or 4-byte CMap code can read beyond U+10FFFF
  (0x110000, 0xFFFFFFFF).
- The textpage is not every drawn char: pdfium drops a text object whose
  box is under 0.01 pt wide (``Tf 0``, ``Tz 0``, a tiny size, an object of a
  single space) and a copy of identical text (the same codes at the same
  ``Tf``) drawn on one of the five text objects before it (fake-bold and
  drop-shadow removal): along x within 0.9 of its last glyph's advance at
  that ``Tf``, whatever matrix scales the text (2.4 pt for 12 pt "Total",
  10.2 pt for "TOTAL DUE MW"), and across within about an eighth of its
  width while the two boxes overlap. It collapses runs of spaces to one,
  decomposes ligatures (U+FB01 reads "fi") and mirrors "(" in right-to-left
  text (it reads ")"). The engine keeps every char the textpage reports and counts
  the text objects that own none: ``TextStats.empty_text_objects`` when the
  object draws no glyph with extent (its box is the point at its origin
  though its em is 1 unit or more each way in its form's space: a single
  space), else
  ``TextStats.dropped_text_objects``, which
  :mod:`datagrunt.pdf._pdfium_recover` reads back.
- Standard-14 fonts have no descriptor: their flags lack ForceBold and their
  weight reads 0, so only the base name says bold. A non-embedded alias
  reads as the standard-14 font pdfium substitutes (``Arial,BoldItalic``
  reads ``Helvetica-BoldOblique``). A pattern fill reads as a colour pdfium
  picks (opaque 75% grey for a tiling pattern).
"""

import ctypes
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import NamedTuple

import pypdfium2.raw as pdfium_c

from datagrunt.pdf._pdfium_common import Box, PdfiumFailure, object_address, read_rgba, union_box
from datagrunt.pdf._validate import finite_float, int_at_least, ordered_box, rgba_tuple, strict_bool, typed_tuple


@dataclass(frozen=True, slots=True, kw_only=True)
class RawSpan:
    """A maximal run of consecutive textpage chars owned by one text object.

    pdfium makes one text object per text-showing operator (``Tj``, ``TJ``,
    ``'``, ``"``), so a span never crosses a line. Boxes are displayed,
    top-left ``(x0, y0, x1, y1)`` points, as :func:`datagrunt.pdf.pdfium_engine.to_display_box` returns.

    Attributes:
        text: The chars pdfium's textpage reports for the object, each
            kept: control chars, U+FFFD and lone surrogates included (A3
            classifies them). A surrogate pair pdfium reports as two chars
            is one char here; a raw code beyond U+10FFFF is U+FFFD. The
            textpage itself drops or rewrites some drawn chars (see the
            module notes: ``Tf 0`` text, space runs, ligatures).
        bbox: The union of ``char_boxes``.
        char_boxes: One box per char of ``text``: pdfium's loose char box
            (advance width by the font's ascent-to-descent height).
        font_size_pt: The height of the text's em under its page-space
            matrix (form matrices included), measured across its baseline
            (:func:`em_size`): the ``Tf`` size for upright, turned or
            oblique text, near 0 for text sheared flat; negative for a
            negative ``Tf`` (mirrored glyphs).
        em_width_pt: The width of the text's em under the same matrix
            (form matrices and ``Tz`` included), measured across its height
            side (:func:`em_size`), so a mirrored scale counts at its
            magnitude, turned text in full and sheared-flat text as flat;
            never negative. An em under 1 pt wide and narrower than it is
            tall is text squeezed flat: ``Tz 0.05`` leaves 12 pt "HIDDEN"
            in the textpage at 0.006 pt per em.
        base_font_name: The font's /BaseFont without a subset tag
            (``ABCDEF+``); empty for a font without one (Type3). For a
            non-embedded Type1 font pdfium aliases to a standard-14 font,
            this is pdfium's alias, not the PDF's name: ``Arial-BoldMT``
            reads ``Helvetica-Bold`` and ``TimesNewRomanPS-BoldItalicMT``
            reads ``Times-BoldItalic``.
        is_bold: From a whole style word of the font name (Bold, Black,
            Heavy, Semibold, Bd, Blk, Hv, ...), the ForceBold flag, or
            weight >= 600.
        is_italic: From a whole style word of the font name (Italic,
            Oblique, It, Obl, Slanted) or the Italic flag.
        render_mode: The text render mode, 0 (fill) to 7 (clip).
        fill_rgba: Fill colour and alpha as 0-255 ints, as pdfium reports
            them (a pattern fill reads as a colour pdfium picks).
        stroke_rgba: Stroke colour and alpha as 0-255 ints.
        object_index: The owning text object's position in the page's
            depth-first, paint-order object walk (forms count, then their
            contents), so it also gives the span's z-order.
        generated_breaks: Offsets into ``text``, strictly increasing and
            each in ``1..len(text) - 1``, before which pdfium generated a
            char (a space or line break) inside this span, as at a wide
            ``TJ`` gap: ``[(Name) -12000 (Value)] TJ`` is text "NameValue"
            with a break at 4. The generated chars are not inserted into
            ``text``.
        map_errors: Offsets into ``text``, strictly increasing and each in
            ``0..len(text) - 1``, of the chars pdfium could not map to
            Unicode (``FPDFText_HasUnicodeMapError``, or a raw code beyond
            U+10FFFF, which reads as U+FFFD): each reads as its raw char
            code, so its text says nothing (A3 counts it invalid). A
            surrogate pair is one offset if either half is unmapped.
            ``TextStats.unicode_map_error_count`` counts the same chars in
            textpage chars.
        painted_over: For clip-only text (render mode 7, which paints
            nothing and adds its glyphs to the clip): whether a later object
            in paint order (a path, image or shading, forms' contents
            included; not text) overlaps the text object's box, so its
            paint shows through the glyphs (gradient-filled text). Boxes are
            page-space bounds, compared with positive overlap; the clip's
            scope (``q`` ... ``Q``) is not checked, so a later paint after
            the clip is restored counts too. False for every other render
            mode (not read).
    """

    text: str
    bbox: Box
    char_boxes: tuple[Box, ...]
    font_size_pt: float
    em_width_pt: float
    base_font_name: str
    is_bold: bool
    is_italic: bool
    render_mode: int
    fill_rgba: tuple[int, int, int, int]
    stroke_rgba: tuple[int, int, int, int]
    object_index: int
    generated_breaks: tuple[int, ...]
    map_errors: tuple[int, ...]
    painted_over: bool

    def __post_init__(self) -> None:
        self._check_text_and_boxes()
        last = len(self.text) - 1
        _check_offsets("generated_breaks", self.generated_breaks, 1, last)
        _check_offsets("map_errors", self.map_errors, 0, last)
        object.__setattr__(self, "font_size_pt", finite_float("font_size_pt", self.font_size_pt))
        object.__setattr__(self, "em_width_pt", finite_float("em_width_pt", self.em_width_pt, lo=0.0))
        if not isinstance(self.base_font_name, str):
            raise TypeError(f"base_font_name must be a str, got {self.base_font_name!r}")
        strict_bool("is_bold", self.is_bold)
        strict_bool("is_italic", self.is_italic)
        if isinstance(self.render_mode, bool) or not isinstance(self.render_mode, int):
            raise TypeError(f"render_mode must be an int, got {self.render_mode!r}")
        if self.render_mode not in range(8):
            raise ValueError(f"render_mode must be in 0..7, got {self.render_mode}")
        rgba_tuple("fill_rgba", self.fill_rgba)
        rgba_tuple("stroke_rgba", self.stroke_rgba)
        int_at_least("object_index", self.object_index, 0)
        if strict_bool("painted_over", self.painted_over) and self.render_mode != CLIP_ONLY_MODE:
            only = f"read for render mode {CLIP_ONLY_MODE} (clip only) text only"
            raise ValueError(f"painted_over is {only}, got render mode {self.render_mode}")

    def _check_text_and_boxes(self) -> None:
        if not isinstance(self.text, str):
            raise TypeError(f"text must be a str, got {type(self.text).__name__}")
        if not self.text:
            raise ValueError("text must not be empty")
        if not isinstance(self.char_boxes, tuple):
            raise TypeError(f"char_boxes must be a tuple, got {type(self.char_boxes).__name__}")
        if len(self.char_boxes) != len(self.text):
            counts = f"{len(self.char_boxes)} boxes for {len(self.text)} chars"
            raise ValueError(f"char_boxes must hold one box per char of text: {counts}")
        boxes = tuple(ordered_box(f"char_boxes[{i}]", box) for i, box in enumerate(self.char_boxes))
        bbox = ordered_box("bbox", self.bbox)
        if bbox != union_box(boxes):
            raise ValueError(f"bbox must be the union of char_boxes, {union_box(boxes)!r}, got {self.bbox!r}")
        object.__setattr__(self, "char_boxes", boxes)
        object.__setattr__(self, "bbox", bbox)


def _check_offsets(name: str, offsets: object, low: int, high: int) -> None:
    """Require ``offsets`` to be a tuple of strictly increasing ints in ``low..high`` (offsets into a span's text)."""
    if not isinstance(offsets, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(offsets).__name__}")
    for index, offset in enumerate(offsets):
        int_at_least(f"{name}[{index}]", offset, 0)
        if not low <= offset <= high:
            raise ValueError(f"{name}[{index}] must be inside text, in {low}..{high}, got {offset}")
    if any(later <= earlier for earlier, later in zip(offsets, offsets[1:])):
        raise ValueError(f"{name} must be strictly increasing, got {offsets!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class TextStats:
    """Counts of one page's textpage chars, for A3's text-quality checks.

    The three char counts are disjoint; with the chars of skipped objects
    they sum to the textpage's char count.

    Attributes:
        char_count: Non-generated textpage chars a text object owns: the
            chars of the page's spans (a non-BMP char counts twice, as a
            surrogate pair).
        generated_count: Chars pdfium flags as generated (spaces and line
            breaks it inserts); they are in no span.
        ownerless_count: Non-generated chars no text object owns
            (``FPDFText_GetTextObject`` is NULL): pdfium inserts them, as
            the space between two right-to-left words drawn by separate
            operators; they are in no span.
        unicode_map_error_count: Owned chars pdfium could not map to
            Unicode (``FPDFText_HasUnicodeMapError``); each reads as its raw
            char code. A raw code beyond U+10FFFF (a 3- or 4-byte CMap code
            with no ToUnicode entry) reads as U+FFFD and counts here once,
            whether pdfium flags it or not. The span chars they make are
            each span's ``map_errors`` (one per surrogate pair).
        skipped_nonfinite_objects: Text objects whose chars are in no span
            (and no other count) because pdfium places them at a non-finite
            position or size: a char box, the font size or the em width is
            NaN or infinite, as under a ``cm`` that overflows pdfium's
            32-bit CTM.
            pdfium renders the rest of such a page.
        dropped_text_objects: Text objects of the page (forms' included)
            that own no textpage char but draw glyphs: pdfium dropped them
            (see the module notes), so their text is in no span. Over 0, the
            page is worth
            :func:`~datagrunt.pdf.pdfium_engine.recover_dropped_text`; at 0
            there is nothing to recover. Disjoint from
            ``skipped_nonfinite_objects``, whose objects own chars.
        empty_text_objects: Text objects that own no textpage char and draw
            no glyph with extent: their box is the point at their origin
            though their em is 1 unit or more each way in their form's
            space (an object of a single space or no-break space). Nothing
            to recover; disjoint from ``dropped_text_objects``.
    """

    char_count: int
    generated_count: int
    ownerless_count: int
    unicode_map_error_count: int
    skipped_nonfinite_objects: int
    dropped_text_objects: int
    empty_text_objects: int

    def __post_init__(self) -> None:
        int_at_least("char_count", self.char_count, 0)
        int_at_least("generated_count", self.generated_count, 0)
        int_at_least("ownerless_count", self.ownerless_count, 0)
        int_at_least("unicode_map_error_count", self.unicode_map_error_count, 0)
        int_at_least("skipped_nonfinite_objects", self.skipped_nonfinite_objects, 0)
        int_at_least("dropped_text_objects", self.dropped_text_objects, 0)
        int_at_least("empty_text_objects", self.empty_text_objects, 0)
        if self.unicode_map_error_count > self.char_count:
            counts = f"unicode_map_error_count ({self.unicode_map_error_count}) cannot exceed"
            raise ValueError(f"{counts} char_count ({self.char_count})")


@dataclass(frozen=True, slots=True, kw_only=True)
class PageText:
    """One page's spans in textpage order, with its textpage counts.

    Every owned, non-generated textpage char is in exactly one span (one
    span char per textpage char, or one per surrogate pair), except the
    chars of objects counted in ``stats.skipped_nonfinite_objects``.
    """

    spans: tuple[RawSpan, ...]
    stats: TextStats

    def __post_init__(self) -> None:
        typed_tuple("spans", self.spans, RawSpan)
        if not isinstance(self.stats, TextStats):
            raise TypeError(f"stats must be a TextStats, got {type(self.stats).__name__}")
        _check_made_of("chars", sum(len(item.text) for item in self.spans), self.stats.char_count, "chars")
        unmapped = sum(len(item.map_errors) for item in self.spans)
        _check_made_of("map errors (map_errors)", unmapped, self.stats.unicode_map_error_count, "map errors")


def _check_made_of(what: str, span_count: int, textpage_count: int, unit: str) -> None:
    """Require ``textpage_count`` textpage items to make ``span_count`` span items (1 each, or 2 for a pair)."""
    if not span_count <= textpage_count <= 2 * span_count:
        message = f"spans hold {span_count} {what}, which {textpage_count} textpage {unit} cannot make"
        raise ValueError(f"{message} (each is 1 textpage char, or 2 for a surrogate pair)")


class ObjectWalk(NamedTuple):
    """What :func:`collect_text` needs of the page's object walk.

    Attributes:
        indices: Each walked object's address (``object_address``) to its
            index in the page's depth-first, paint-order object walk.
        text_objects: The addresses of the walk's text objects.
        glyphless: Whether the text object at an address draws no glyph
            with extent; asked only of text objects that own no textpage
            char, in walk order. It may raise PdfiumFailure.
        painted_over: Whether a later path, image or shading overlaps the
            text object at an address (``RawSpan.painted_over``); asked only
            of clip-only (render mode 7) owners. It may raise PdfiumFailure.
    """

    indices: dict[int, int]
    text_objects: frozenset[int]
    glyphless: Callable[[int], bool]
    painted_over: Callable[[int], bool]


class CollectedText(NamedTuple):
    """What :func:`collect_text` reads from a textpage.

    Attributes:
        page_text: The spans and counts.
        dropped: The addresses (``object_address``) of the text objects that
            own no textpage char but draw glyphs;
            ``page_text.stats.dropped_text_objects`` counts them.
    """

    page_text: PageText
    dropped: frozenset[int]


class _Style(NamedTuple):
    """The RawSpan fields read from a span's text object."""

    font_size_pt: float
    em_width_pt: float
    base_font_name: str
    is_bold: bool
    is_italic: bool
    render_mode: int
    fill_rgba: tuple[int, int, int, int]
    stroke_rgba: tuple[int, int, int, int]
    painted_over: bool


# The text render mode (PDF 32000-1, Table 106) that paints nothing and adds the glyphs to the clip.
CLIP_ONLY_MODE = 7
# PDF 32000-1 font descriptor flags (Table 121) and weights (Table 122).
_FLAG_ITALIC = 0x40
_FLAG_FORCE_BOLD = 0x40000
_BOLD_WEIGHT = 600
# Whole style words, lowercased: SemiBold is "semi" + "bold", BdIt "bd" + "it".
_BOLD_NAME_TOKENS = frozenset(
    {"bold", "black", "heavy", "semibold", "demibold", "extrabold", "ultrabold", "bd", "blk", "hv"}
)
_ITALIC_NAME_TOKENS = frozenset({"italic", "oblique", "it", "obl", "slanted"})
# A font name's words: capitals before a capitalised word (LT in LTStd), a
# capitalised or lowercase word, the rest of a run of capitals, a digit run.
_NAME_WORD = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
# A font subset's /BaseFont starts with exactly six capitals and "+" (PDF 32000-1, 9.9.2).
_SUBSET_TAG = re.compile(r"[A-Z]{6}\+")
_STYLE_SEPARATOR = re.compile(r"[-,]")
# PDF names are at most 127 bytes in practice (PDF 32000-1, Annex C); this
# bound, NUL included, only stops a hostile length from sizing the buffer.
_MAX_FONT_NAME_BYTES = 4096


def _base_font_name(font: pdfium_c.FPDF_FONT, label: str) -> str:
    """Read a font's /BaseFont without its subset tag.

    pdfium returns the name as UTF-8; bytes that are not UTF-8 are kept as
    visible ``\\xNN`` escapes rather than replaced.
    """
    size = pdfium_c.FPDFFont_GetBaseFontName(font, None, 0)
    if size < 1:
        raise PdfiumFailure(f"read the font name of {label}")
    if size > _MAX_FONT_NAME_BYTES:
        raise PdfiumFailure(
            f"read the font name of {label}: {size} bytes is over the {_MAX_FONT_NAME_BYTES}-byte bound"
        )
    buffer = ctypes.create_string_buffer(size)
    if pdfium_c.FPDFFont_GetBaseFontName(font, buffer, size) != size:
        raise PdfiumFailure(f"read the font name of {label}")
    name = buffer.raw[: size - 1].decode("utf-8", errors="backslashreplace")
    return name[7:] if _SUBSET_TAG.match(name) else name


def _style_words(name: str) -> frozenset[str]:
    """The lowercased words of a font name's style part, split on case and digit boundaries.

    The style part follows the first "-" or "," (Arial-BoldMT: "bold",
    "mt"; Arial,Bold: "bold"). A name without one starts with its family
    word, which is skipped (ArialBlack: "black"; BlackadderITC: "itc").
    Matching whole style words keeps family names such as BlackChancery and
    BlackadderITC-Regular from reading bold.
    """
    separator = _STYLE_SEPARATOR.search(name)
    if separator:
        return frozenset(word.lower() for word in _NAME_WORD.findall(name[separator.end() :]))
    return frozenset(word.lower() for word in _NAME_WORD.findall(name)[1:])


def _bold_and_italic(font: pdfium_c.FPDF_FONT, name: str, label: str) -> tuple[bool, bool]:
    """Read bold and italic from the font name, its descriptor flags and its weight.

    Each source alone suffices: standard-14 fonts have no descriptor (flags
    and weight read 0), so their name is the only one that says bold.
    pdfium keeps the low 19 bits of /Flags, so only a NULL font reads
    negative (-1, every bit set): that is a failure, not bold italic. The
    weight is not a failure signal: pdfium derives it from /StemV (114
    reads 570, so CMBX10 is not bold unless its name says so), and a
    negative /StemV reads negative (-5 reads -25).
    """
    style = _style_words(name)
    flags = pdfium_c.FPDFFont_GetFlags(font)
    if flags < 0:
        raise PdfiumFailure(f"read the font flags of {label}")
    is_bold = (
        not style.isdisjoint(_BOLD_NAME_TOKENS)
        or bool(flags & _FLAG_FORCE_BOLD)
        or pdfium_c.FPDFFont_GetWeight(font) >= _BOLD_WEIGHT
    )
    is_italic = not style.isdisjoint(_ITALIC_NAME_TOKENS) or bool(flags & _FLAG_ITALIC)
    return is_bold, is_italic


def em_size(size: float, a: float, b: float, c: float, d: float) -> tuple[float, float]:
    """The width and height in points of an em of ``Tf`` ``size`` under a text matrix ``[a b c d]`` to page space.

    The em's sides map to ``(a, b)`` (along the baseline) and ``(c, d)``
    (up the glyphs), and each is measured perpendicular to the other: the
    em is ``|size| x |ad - bc| / hypot(c, d)`` wide and ``|size| x |ad - bc|
    / hypot(a, b)`` tall, the two heights of the parallelogram it maps to.
    Perpendicular sides (any turn, scale or mirror) read their own lengths;
    a shear that lays both sides along almost one line (``1 0 1 0.0001``)
    or a singular matrix (``1 0 1 0``) leaves the em little or no extent
    either way, however long its sides. A side of no length leaves the
    other its own length (``Tz 0``: no width, full height). A mirrored
    (negative) size or scale counts at its magnitude. The one measure for
    textpage spans (:class:`RawSpan`) and for the text objects recovery
    reads back.
    """
    area = abs(a * d - b * c)
    along, up = math.hypot(a, b), math.hypot(c, d)
    width = area / up if up else along
    height = area / along if along else up
    return abs(size) * width, abs(size) * height


def _em(
    textpage: pdfium_c.FPDF_TEXTPAGE, text_object: pdfium_c.FPDF_PAGEOBJECT, char_index: int, label: str
) -> tuple[float, float]:
    """The char's ``(font_size_pt, em_width_pt)``: its em's height, signed by ``Tf``, and its width (:func:`em_size`).

    Measured on the char's page-space matrix: the textpage matrix holds
    ``Tz`` and every enclosing form's matrix (not ``Tf``); the object's own
    matrix (``FPDFPageObj_GetMatrix``) is in its form's space. It may not
    be finite (a ``cm`` that overflows pdfium's 32-bit CTM): the
    caller then skips the object.
    """
    nominal = ctypes.c_float()
    if not pdfium_c.FPDFTextObj_GetFontSize(text_object, nominal):
        raise PdfiumFailure(f"read the font size of {label}")
    matrix = pdfium_c.FS_MATRIX()
    if not pdfium_c.FPDFText_GetMatrix(textpage, char_index, matrix):
        raise PdfiumFailure(f"read the matrix of char {char_index}")
    width, height = em_size(nominal.value, matrix.a, matrix.b, matrix.c, matrix.d)
    return math.copysign(height, nominal.value), width


def render_mode(text_object: pdfium_c.FPDF_PAGEOBJECT, label: str) -> int:
    """The text object's render mode, 0 to 7."""
    mode = pdfium_c.FPDFTextObj_GetTextRenderMode(text_object)
    if mode not in range(8):  # FPDF_TEXTRENDERMODE_UNKNOWN (-1) on failure
        raise PdfiumFailure(f"read the render mode of {label}")
    return mode


def _read_style(
    textpage: pdfium_c.FPDF_TEXTPAGE, text_object: pdfium_c.FPDF_PAGEOBJECT, char_index: int, walk: ObjectWalk
) -> _Style:
    """Read the style of the walked text object ``text_object``, which owns textpage char ``char_index``."""
    address = object_address(text_object)
    label = f"object {walk.indices[address]}"
    font = pdfium_c.FPDFTextObj_GetFont(text_object)
    if not font:
        raise PdfiumFailure(f"read the font of {label}")
    name = _base_font_name(font, label)
    is_bold, is_italic = _bold_and_italic(font, name, label)
    mode = render_mode(text_object, label)
    font_size_pt, em_width_pt = _em(textpage, text_object, char_index, label)
    return _Style(
        font_size_pt=font_size_pt,
        em_width_pt=em_width_pt,
        base_font_name=name,
        is_bold=is_bold,
        is_italic=is_italic,
        render_mode=mode,
        fill_rgba=read_rgba(pdfium_c.FPDFPageObj_GetFillColor, text_object, f"fill colour of {label}"),
        stroke_rgba=read_rgba(pdfium_c.FPDFPageObj_GetStrokeColor, text_object, f"stroke colour of {label}"),
        painted_over=mode == CLIP_ONLY_MODE and walk.painted_over(address),
    )


def _loose_box(textpage: pdfium_c.FPDF_TEXTPAGE, char_index: int) -> Box:
    """Read a char's loose box as ``(left, bottom, right, top)`` in PDF user space.

    A box that is not finite is returned as pdfium reports it (the caller
    then skips the char's object); a finite one has its edges in order, so a
    mirrored (negative-size) char still reads as a box.
    """
    rect = pdfium_c.FS_RECTF()
    if not pdfium_c.FPDFText_GetLooseCharBox(textpage, char_index, rect):
        raise PdfiumFailure(f"read the box of char {char_index}")
    left, bottom, right, top = rect.left, rect.bottom, rect.right, rect.top
    if not all(math.isfinite(edge) for edge in (left, bottom, right, top)):
        return left, bottom, right, top
    return min(left, right), min(bottom, top), max(left, right), max(bottom, top)


class _OwnedChar(NamedTuple):
    """One owned, non-generated textpage char, as :func:`_read_textpage` reads it."""

    owner: int
    code: int
    map_error: bool
    box: Box


class _Textpage(NamedTuple):
    """A textpage read once: its chars in order, ``None`` where pdfium inserted one."""

    chars: list[_OwnedChar | None]
    generated_count: int
    ownerless_count: int
    styles: dict[int, _Style]
    # Owners with a char box, font size or em width that is not finite: no span takes their chars.
    skipped: frozenset[int]


_HIGH_SURROGATES = range(0xD800, 0xDC00)
_LOW_SURROGATES = range(0xDC00, 0xE000)
_MAX_CODE_POINT = 0x10FFFF
_REPLACEMENT_CHAR = "\ufffd"


class _SpanBuilder:
    """Groups a page's kept textpage chars into RawSpans, in order."""

    def __init__(
        self, to_display: Callable[[Box], Box], object_indices: dict[int, int], styles: dict[int, _Style]
    ) -> None:
        self._to_display = to_display
        self._object_indices = object_indices
        self._styles = styles
        self._spans: list[RawSpan] = []
        self._owner = 0
        self._chars: list[str] = []
        self._boxes: list[Box] = []
        self._breaks: list[int] = []
        self._errors: list[int] = []
        self._break_pending = False

    def mark_inserted(self) -> None:
        """Note that pdfium inserted a char (generated or ownerless) after the last one added."""
        self._break_pending = True

    def mark_skipped(self) -> None:
        """Note a char of a skipped object: the span it interrupts ends before it."""
        self._flush()
        self._break_pending = False

    def add(self, char: _OwnedChar) -> None:
        """Append ``char`` to the current span, or to a new one if another object owns it."""
        if char.owner != self._owner:
            self._flush()
            self._owner = char.owner
        # A char pdfium inserted inside this span becomes a break before this
        # char; one inserted between spans is only counted.
        breaking = self._break_pending and bool(self._chars)
        self._break_pending = False
        code = char.code
        if breaking:
            self._breaks.append(len(self._chars))
        elif code in _LOW_SURROGATES and self._chars and ord(self._chars[-1]) in _HIGH_SURROGATES:
            # pdfium reports a non-BMP char as a UTF-16 pair of textpage chars.
            high = ord(self._chars[-1])
            self._chars[-1] = chr(0x10000 + ((high - _HIGH_SURROGATES.start) << 10) + (code - _LOW_SURROGATES.start))
            self._boxes[-1] = union_box((self._boxes[-1], char.box))
            self._mark_unmapped(char, len(self._chars) - 1)
            return
        self._mark_unmapped(char, len(self._chars))
        # A code beyond Unicode is a raw char code pdfium could not map; U+FFFD
        # keeps its place (and is no surrogate, so it never pairs).
        self._chars.append(chr(code) if code <= _MAX_CODE_POINT else _REPLACEMENT_CHAR)
        self._boxes.append(char.box)

    def _mark_unmapped(self, char: _OwnedChar, offset: int) -> None:
        """Record span char ``offset`` as a map error if ``char`` (one of its textpage chars) is one; once each."""
        if _unmapped(char) and (not self._errors or self._errors[-1] != offset):
            self._errors.append(offset)

    def finish(self) -> tuple[RawSpan, ...]:
        """Close the current span and return every span."""
        self._flush()
        return tuple(self._spans)

    def _flush(self) -> None:
        if not self._chars:
            return
        boxes = tuple(self._to_display(box) for box in self._boxes)
        self._spans.append(
            RawSpan(
                text="".join(self._chars),
                bbox=union_box(boxes),
                char_boxes=boxes,
                object_index=self._object_indices[self._owner],
                generated_breaks=tuple(self._breaks),
                map_errors=tuple(self._errors),
                **self._styles[self._owner]._asdict(),
            )
        )
        self._chars, self._boxes, self._breaks, self._errors = [], [], [], []


def _unmapped(char: _OwnedChar) -> bool:
    """True if pdfium could not map ``char`` to Unicode: flagged, or a code beyond U+10FFFF (it reads as U+FFFD)."""
    return char.map_error or char.code > _MAX_CODE_POINT


def _textpage_flag(result: int, char_index: int, question: str) -> bool:
    """Return a 0/1 FPDFText_* answer about a char as a bool; anything else is a pdfium failure."""
    if result not in (0, 1):
        raise PdfiumFailure(f"read whether char {char_index} {question}")
    return result == 1


def _read_textpage(textpage: pdfium_c.FPDF_TEXTPAGE, walk: ObjectWalk) -> _Textpage:
    """Read every textpage char once, each owner's style once, and which owners pdfium cannot place."""
    count = pdfium_c.FPDFText_CountChars(textpage)
    if count < 0:
        raise PdfiumFailure("count the text page's chars")
    chars: list[_OwnedChar | None] = []
    styles: dict[int, _Style] = {}
    skipped: set[int] = set()
    generated = ownerless = 0
    for char_index in range(count):
        if _textpage_flag(pdfium_c.FPDFText_IsGenerated(textpage, char_index), char_index, "is generated"):
            generated += 1
            chars.append(None)
            continue
        text_object = pdfium_c.FPDFText_GetTextObject(textpage, char_index)
        if not text_object:
            # pdfium inserted it, though not flagged generated: the space
            # between two right-to-left words drawn by separate operators.
            ownerless += 1
            chars.append(None)
            continue
        map_error = _textpage_flag(
            pdfium_c.FPDFText_HasUnicodeMapError(textpage, char_index), char_index, "maps to Unicode"
        )
        code = pdfium_c.FPDFText_GetUnicode(textpage, char_index)
        owner = object_address(text_object)
        if owner not in styles:
            if owner not in walk.indices:
                raise PdfiumFailure(f"read the text object of char {char_index}: it is not in the page's object tree")
            styles[owner] = _read_style(textpage, text_object, char_index, walk)
        box = _loose_box(textpage, char_index)
        style = styles[owner]
        if not all(math.isfinite(value) for value in (*box, style.font_size_pt, style.em_width_pt)):
            skipped.add(owner)
        chars.append(_OwnedChar(owner, code, map_error, box))
    return _Textpage(chars, generated, ownerless, styles, frozenset(skipped))


def collect_text(textpage: pdfium_c.FPDF_TEXTPAGE, to_display: Callable[[Box], Box], walk: ObjectWalk) -> CollectedText:
    """Read every textpage char once: count them and group the owned, non-generated ones.

    A text object with a char box, font size or em width that is not finite
    (pdfium places it nowhere) is skipped whole and counted; a skipped char
    ends the span it interrupts. A text object that owns no char is empty if
    it draws no glyph with extent (``walk.glyphless``), else dropped.

    Args:
        textpage: The page's loaded textpage.
        to_display: Maps a user-space ``(left, bottom, right, top)`` box to a
            displayed, top-left ``(x0, y0, x1, y1)`` box.
        walk: The page's object walk.

    Raises:
        PdfiumFailure: If a pdfium call fails.
    """
    read = _read_textpage(textpage, walk)
    ownerless = walk.text_objects - {char.owner for char in read.chars if char is not None}
    # In walk order, so a pdfium failure always names the same object.
    empty = frozenset(address for address in sorted(ownerless, key=walk.indices.__getitem__) if walk.glyphless(address))
    dropped = ownerless - empty
    builder = _SpanBuilder(to_display, walk.indices, read.styles)
    kept = map_errors = 0
    for char in read.chars:
        if char is None:
            builder.mark_inserted()
        elif char.owner in read.skipped:
            builder.mark_skipped()
        else:
            kept += 1
            map_errors += _unmapped(char)
            builder.add(char)
    stats = TextStats(
        char_count=kept,
        generated_count=read.generated_count,
        ownerless_count=read.ownerless_count,
        unicode_map_error_count=map_errors,
        skipped_nonfinite_objects=len(read.skipped),
        dropped_text_objects=len(dropped),
        empty_text_objects=len(empty),
    )
    return CollectedText(page_text=PageText(spans=builder.finish(), stats=stats), dropped=dropped)
