"""The v5 vision route (#369, A5b): a vision page's elements from OCR of its render.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. It runs no
child process of its own: renders go through
:func:`~datagrunt.pdf.render.render_page` (the guarded, capped worker) and
tesseract through :mod:`datagrunt.pdf.ocr` (its bounded CLI calls).

OCR words become elements through A4b's layout (spec §3.3 step 4: "populate
word boxes into layout.py"). Each :class:`~datagrunt.pdf.ocr.OCRWord` is one
visible piece of a synthetic span of its own (:func:`ocr_pieces`):

- its text, and its box on the displayed page;
- its chars splitting the box evenly along the word, each as tall as it
  (its loose and tight boxes alike: OCR reads no glyph bounds);
- a font size of the box's height in points (at least :data:`LEAST_SIZE_PT`,
  the least size a layout line holds), its em as wide; not bold, not
  italic, painted (render mode 0, opaque black), with no hidden reason.

:func:`vision_elements` lays the pieces out with
:func:`~datagrunt.pdf.layout.page_layout`, with no lattice (a scan has no
vector rules), so a scan yields headings, paragraphs, key-values, lists
and stream tables; each figure is a slot in the reading order, as on any
page. Digital text kept apart (hidden pieces, every one carrying a reason)
reads as hidden paragraphs placed among them. Pass C
(:func:`~datagrunt.pdf.assembly.conservation`) checks every piece's chars
against the elements: a mismatch is a warning.

Known limitation: a word's box runs from its highest to its lowest ink, so
its height is the font's x-height for a word such as "over", and its
ascent to descent for "jumps". Lines whose words mostly reach both read
larger than the page's body, so the layout's heading rule (1.15 times the
body size) takes some body lines for headings: on Root's Law (pages 5, 10
and 20) OCR finds 18 to 22 headings where the digital text has 5 to 7.
Sizing each word by its tesseract line's height instead finds as many (14
to 20) and joins words across their spaces (A5b evidence).
"""

from typing import NamedTuple

from datagrunt.pdf._validate import typed_tuple
from datagrunt.pdf.assembly import FigureDraft, conservation, page_elements
from datagrunt.pdf.elements import DocumentElement
from datagrunt.pdf.layout import LatticeResult, page_layout
from datagrunt.pdf.ocr import OCRWord
from datagrunt.pdf.pdfium_engine import PageGeometry, RawSpan
from datagrunt.pdf.quality import SpanPiece

__all__ = ["LEAST_SIZE_PT", "VisionElements", "ocr_pieces", "vision_elements"]

# The least font size an OCR piece is given: layout reads line sizes to 0.1 pt, and a line's size is over 0.
LEAST_SIZE_PT = 0.1

# OCR reads ink as it is painted: opaque black, filled (render mode 0).
_PAINTED = (0, 0, 0, 255)
_FILL = 0

# A scan has no vector rules: no lattice tables, no text used by them.
_NO_LATTICE = LatticeResult(tables=(), used_pieces=())


class VisionElements(NamedTuple):
    """A vision page's elements in reading order, and the warnings laying them out raised.

    Attributes:
        elements: The A1 elements, ids canonical on the page.
        warnings: The layout's warnings, then Pass C's mismatch, if any.
    """

    elements: tuple[DocumentElement, ...]
    warnings: tuple[str, ...]


def ocr_pieces(words: tuple[OCRWord, ...]) -> tuple[SpanPiece, ...]:
    """Each OCR word as one visible piece of a span of its own, in order (see the module notes). O(chars).

    A span's ``object_index`` is its word's position in ``words``.

    Raises:
        TypeError: If ``words`` is not a tuple of OCRWord.
    """
    return tuple(_word_piece(item, index) for index, item in enumerate(typed_tuple("words", words, OCRWord)))


def _word_piece(word: OCRWord, index: int) -> SpanPiece:
    """``word`` as the whole of a visible span: its chars split along its box, its size the box's height."""
    box = word.bbox
    count = len(word.text)
    # The last edge is the word's own, so the chars tile its box exactly whatever the float division.
    edges = [box.x0 + (box.x1 - box.x0) * at / count for at in range(count)] + [box.x1]
    chars = tuple((edges[at], box.y0, edges[at + 1], box.y1) for at in range(count))
    size = max(box.y1 - box.y0, LEAST_SIZE_PT)
    span = RawSpan(
        text=word.text,
        bbox=(box.x0, box.y0, box.x1, box.y1),
        char_boxes=chars,
        tight_boxes=chars,
        font_size_pt=size,
        em_width_pt=size,
        base_font_name="",
        is_bold=False,
        is_italic=False,
        render_mode=_FILL,
        fill_rgba=_PAINTED,
        stroke_rgba=_PAINTED,
        object_index=index,
        generated_breaks=(),
        map_errors=(),
    )
    return SpanPiece(span=span, start=0, end=count, hidden_reasons=())


def vision_elements(
    words: tuple[OCRWord, ...],
    *,
    geometry: PageGeometry,
    apart: tuple[SpanPiece, ...] = (),
    recovered: tuple[SpanPiece, ...] = (),
    figures: tuple[FigureDraft, ...] = (),
) -> VisionElements:
    """The page's elements from its OCR ``words`` (see the module notes), with its digital text kept apart.

    Args:
        words: The page's OCR words (``OCRResult.words``), on the displayed page.
        geometry: The page's geometry: its number and displayed size.
        apart: Digital pieces of the page laid out apart from the words:
            each must carry a hidden reason (the vision page supersedes its
            visible ones first).
        recovered: Recovered digital pieces, likewise all hidden.
        figures: The page's figure drafts; each reads as a figure slot.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: If a piece of ``apart`` or ``recovered`` is visible, or
            as :func:`~datagrunt.pdf.layout.page_layout` raises.
    """
    read = ocr_pieces(words)
    if not isinstance(geometry, PageGeometry):
        raise TypeError(f"geometry must be a PageGeometry, got {type(geometry).__name__}")
    _check_hidden("apart", apart)
    _check_hidden("recovered", recovered)
    typed_tuple("figures", figures, FigureDraft)
    layout = page_layout(
        (*read, *apart),
        recovered,
        _NO_LATTICE,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        figures=tuple(draft.bbox for draft in figures),
    )
    elements = page_elements(layout.items, figures, geometry)
    _, mismatch = conservation((*read, *apart, *recovered), elements)
    return VisionElements(elements=elements, warnings=(*layout.warnings, *mismatch))


def _check_hidden(name: str, pieces: object) -> None:
    """Require a tuple of SpanPiece, each with a hidden reason: a vision page's digital text never mixes with OCR's."""
    for index, piece in enumerate(typed_tuple(name, pieces, SpanPiece)):
        if not piece.hidden_reasons:
            raise ValueError(f"{name}[{index}] is visible ({piece.text!r}): a vision page's digital text reads apart")
