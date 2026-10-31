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
  ``FPDFText_HasUnicodeMapError``; without a ToUnicode map a 3- or 4-byte
  CMap code can read beyond U+10FFFF (0x110000, 0xFFFFFFFF).
- The textpage is not every drawn char: pdfium drops a text object whose
  box is under 0.01 pt wide (``Tf 0``, ``Tz 0``, a tiny size, an object of a
  single space) and a copy drawn on identical text up to ~2 pt away
  (fake-bold removal); it collapses runs of spaces to one, decomposes
  ligatures (U+FB01 reads "fi") and mirrors "(" in right-to-left text (it
  reads ")"). The engine keeps every char the textpage reports and counts
  the text objects that own none (``TextStats.dropped_text_objects``):
  :mod:`datagrunt.pdf._pdfium_recover` reads them back.
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
        font_size_pt: The ``Tf`` size times the vertical scale of the text's
            page-space matrix (form matrices included); negative for a
            negative ``Tf`` (mirrored glyphs).
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
    """

    text: str
    bbox: Box
    char_boxes: tuple[Box, ...]
    font_size_pt: float
    base_font_name: str
    is_bold: bool
    is_italic: bool
    render_mode: int
    fill_rgba: tuple[int, int, int, int]
    stroke_rgba: tuple[int, int, int, int]
    object_index: int
    generated_breaks: tuple[int, ...]

    def __post_init__(self) -> None:
        self._check_text_and_boxes()
        self._check_generated_breaks()
        object.__setattr__(self, "font_size_pt", finite_float("font_size_pt", self.font_size_pt))
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

    def _check_generated_breaks(self) -> None:
        breaks = self.generated_breaks
        if not isinstance(breaks, tuple):
            raise TypeError(f"generated_breaks must be a tuple, got {type(breaks).__name__}")
        last = len(self.text) - 1
        for index, offset in enumerate(breaks):
            int_at_least(f"generated_breaks[{index}]", offset, 0)
            if not 1 <= offset <= last:
                raise ValueError(f"generated_breaks[{index}] must be inside text, in 1..{last}, got {offset}")
        if any(later <= earlier for earlier, later in zip(breaks, breaks[1:])):
            raise ValueError(f"generated_breaks must be strictly increasing, got {breaks!r}")


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
            whether pdfium flags it or not.
        skipped_nonfinite_objects: Text objects whose chars are in no span
            (and no other count) because pdfium places them at a non-finite
            position or size: a char box or the font size is NaN or
            infinite, as under a ``cm`` that overflows pdfium's 32-bit CTM.
            pdfium renders the rest of such a page.
        dropped_text_objects: Text objects of the page (forms' included)
            that own no textpage char: pdfium dropped them (see the module
            notes), so their text is in no span. Over 0, the page is worth
            :func:`~datagrunt.pdf.pdfium_engine.recover_dropped_text`; at 0
            there is nothing to recover. Disjoint from
            ``skipped_nonfinite_objects``, whose objects own chars.
    """

    char_count: int
    generated_count: int
    ownerless_count: int
    unicode_map_error_count: int
    skipped_nonfinite_objects: int
    dropped_text_objects: int

    def __post_init__(self) -> None:
        int_at_least("char_count", self.char_count, 0)
        int_at_least("generated_count", self.generated_count, 0)
        int_at_least("ownerless_count", self.ownerless_count, 0)
        int_at_least("unicode_map_error_count", self.unicode_map_error_count, 0)
        int_at_least("skipped_nonfinite_objects", self.skipped_nonfinite_objects, 0)
        int_at_least("dropped_text_objects", self.dropped_text_objects, 0)
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
        span_chars = sum(len(item.text) for item in self.spans)
        if not span_chars <= self.stats.char_count <= 2 * span_chars:
            message = f"spans hold {span_chars} chars, which {self.stats.char_count} textpage chars cannot make"
            raise ValueError(f"{message} (each is 1 textpage char, or 2 for a surrogate pair)")


class CollectedText(NamedTuple):
    """What :func:`collect_text` reads from a textpage.

    Attributes:
        page_text: The spans and counts.
        dropped: The addresses (``object_address``) of the text objects that
            own no textpage char; ``page_text.stats.dropped_text_objects``
            counts them.
    """

    page_text: PageText
    dropped: frozenset[int]


class _Style(NamedTuple):
    """The RawSpan fields read from a span's text object."""

    font_size_pt: float
    base_font_name: str
    is_bold: bool
    is_italic: bool
    render_mode: int
    fill_rgba: tuple[int, int, int, int]
    stroke_rgba: tuple[int, int, int, int]


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


def _font_size(
    textpage: pdfium_c.FPDF_TEXTPAGE, text_object: pdfium_c.FPDF_PAGEOBJECT, char_index: int, label: str
) -> float:
    """``Tf`` size times the vertical scale of the char's page-space matrix.

    The textpage matrix includes every enclosing form's matrix; the object's
    own matrix (``FPDFPageObj_GetMatrix``) is in its form's space. It may not
    be finite (a ``cm`` that overflows pdfium's 32-bit CTM): the
    caller then skips the object.
    """
    nominal = ctypes.c_float()
    if not pdfium_c.FPDFTextObj_GetFontSize(text_object, nominal):
        raise PdfiumFailure(f"read the font size of {label}")
    matrix = pdfium_c.FS_MATRIX()
    if not pdfium_c.FPDFText_GetMatrix(textpage, char_index, matrix):
        raise PdfiumFailure(f"read the matrix of char {char_index}")
    return nominal.value * math.hypot(matrix.c, matrix.d)


def _read_style(
    textpage: pdfium_c.FPDF_TEXTPAGE, text_object: pdfium_c.FPDF_PAGEOBJECT, char_index: int, object_index: int
) -> _Style:
    """Read the style of text object ``object_index``, which owns textpage char ``char_index``."""
    label = f"object {object_index}"
    font = pdfium_c.FPDFTextObj_GetFont(text_object)
    if not font:
        raise PdfiumFailure(f"read the font of {label}")
    name = _base_font_name(font, label)
    is_bold, is_italic = _bold_and_italic(font, name, label)
    render_mode = pdfium_c.FPDFTextObj_GetTextRenderMode(text_object)
    if render_mode not in range(8):  # FPDF_TEXTRENDERMODE_UNKNOWN (-1) on failure
        raise PdfiumFailure(f"read the render mode of {label}")
    return _Style(
        font_size_pt=_font_size(textpage, text_object, char_index, label),
        base_font_name=name,
        is_bold=is_bold,
        is_italic=is_italic,
        render_mode=render_mode,
        fill_rgba=read_rgba(pdfium_c.FPDFPageObj_GetFillColor, text_object, f"fill colour of {label}"),
        stroke_rgba=read_rgba(pdfium_c.FPDFPageObj_GetStrokeColor, text_object, f"stroke colour of {label}"),
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
    # Owners with a char box or font size that is not finite: no span takes their chars.
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
            return
        # A code beyond Unicode is a raw char code pdfium could not map; U+FFFD
        # keeps its place (and is no surrogate, so it never pairs).
        self._chars.append(chr(code) if code <= _MAX_CODE_POINT else _REPLACEMENT_CHAR)
        self._boxes.append(char.box)

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
                **self._styles[self._owner]._asdict(),
            )
        )
        self._chars, self._boxes, self._breaks = [], [], []


def _textpage_flag(result: int, char_index: int, question: str) -> bool:
    """Return a 0/1 FPDFText_* answer about a char as a bool; anything else is a pdfium failure."""
    if result not in (0, 1):
        raise PdfiumFailure(f"read whether char {char_index} {question}")
    return result == 1


def _read_textpage(textpage: pdfium_c.FPDF_TEXTPAGE, object_indices: dict[int, int]) -> _Textpage:
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
            if owner not in object_indices:
                raise PdfiumFailure(f"read the text object of char {char_index}: it is not in the page's object tree")
            styles[owner] = _read_style(textpage, text_object, char_index, object_indices[owner])
        box = _loose_box(textpage, char_index)
        if not all(math.isfinite(value) for value in (*box, styles[owner].font_size_pt)):
            skipped.add(owner)
        chars.append(_OwnedChar(owner, code, map_error, box))
    return _Textpage(chars, generated, ownerless, styles, frozenset(skipped))


def collect_text(
    textpage: pdfium_c.FPDF_TEXTPAGE,
    to_display: Callable[[Box], Box],
    object_indices: dict[int, int],
    text_objects: frozenset[int],
) -> CollectedText:
    """Read every textpage char once: count them and group the owned, non-generated ones.

    A text object with a char box or font size that is not finite (pdfium
    places it nowhere) is skipped whole and counted; a skipped char ends the
    span it interrupts. A text object that owns no char is dropped.

    Args:
        textpage: The page's loaded textpage.
        to_display: Maps a user-space ``(left, bottom, right, top)`` box to a
            displayed, top-left ``(x0, y0, x1, y1)`` box.
        object_indices: Each page object's address (``object_address``) to
            its index in the page's depth-first, paint-order object walk.
        text_objects: The addresses of the walk's text objects.

    Raises:
        PdfiumFailure: If a pdfium call fails.
    """
    read = _read_textpage(textpage, object_indices)
    dropped = text_objects - {char.owner for char in read.chars if char is not None}
    builder = _SpanBuilder(to_display, object_indices, read.styles)
    kept = map_errors = 0
    for char in read.chars:
        if char is None:
            builder.mark_inserted()
        elif char.owner in read.skipped:
            builder.mark_skipped()
        else:
            kept += 1
            # A code beyond U+10FFFF reads as U+FFFD: a map error, counted once.
            map_errors += char.map_error or char.code > _MAX_CODE_POINT
            builder.add(char)
    stats = TextStats(
        char_count=kept,
        generated_count=read.generated_count,
        ownerless_count=read.ownerless_count,
        unicode_map_error_count=map_errors,
        skipped_nonfinite_objects=len(read.skipped),
        dropped_text_objects=len(dropped),
    )
    return CollectedText(page_text=PageText(spans=builder.finish(), stats=stats), dropped=dropped)
