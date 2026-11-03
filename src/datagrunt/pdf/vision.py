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
- a font size of its tesseract line's (#416; at least
  :data:`LEAST_SIZE_PT`, the least size a layout line holds), its em as
  wide; not bold, not italic, painted (render mode 0, opaque black), with
  no hidden reason.

A word tesseract read with a confidence of 0 is, as a rule, art read as
letters: on A5b's 10 sample pages, 22 of 3,595 words, such as a column of
page numbers read as "PPWWNHD" in a 48 pt box, or bracketed icons read as
"[@],"; three are a digital word ("a", "of", "Omega"). It is kept and
visible, but laid out apart (#416): :func:`vision_elements` passes it to
the layout with the recovered text, so it reads as a line of its own (an
``ApartLine`` with no hidden reason: a visible paragraph) placed among
the page's items by position, and :func:`ocr_pieces` sizes it apart from
its line's other words. So it never joins a line of the page's text: laid
out with it, "PPWWNHD" ran Root's Law p. 1's two contents columns into
one line, glued at its em ("VictoryGoldenKeyKey..."), and the page's
elements recalled 31.0 % of its digital words; apart, 85.1 % (its OCR
words 86.2 %). Capping its box's height to the page's median size
instead recalls as much (80.7 % pooled for both) but still joins it to a
line, and boxes it smaller than its ink.

A word's box runs from its highest ink to its lowest, so its height is
not its font's: a word of x-height letters ("over") is shorter than one
reaching the ascender or cap height ("the", "FACTION") or the descender
("gun"), and one reaching both ("jumps") taller. So (#416):

1. Each word's height is scaled to a one-sided word's by what its glyphs
   reach (:func:`_reach_scale`): an x-height word's by 3/2, a both-sided
   word's by 3/4. On A5b's 10 sample pages those words measure 0.656 and
   1.323 times the one-sided words of their line (medians).
2. Each tesseract line is parted at its gutters (:func:`_line_parts`):
   tesseract can read lines of neighbouring columns as one.
3. Every word of a part is sized by the median of its words' scaled
   heights.

So a line's words share one size, and lines compare by the size of their
type, not by their words' mix of ascenders and descenders. On Root's Law
pages 5, 10 and 20, OCR finds 6, 5 and 8 headings where the digital text
has 6, 5 and 7: every digital heading, and on page 20 art read as "wx".
Sized by each word's own box, it found 17, 16 and 18. Each rule below
was measured with the seams' fragments dropped, before words of no
confidence were laid apart (the scaled median parted, so: 7, 5 and 8, a
misread of art on pages 5 and 20). Other rules tried, on the same pages:

- a line's own height (tesseract's line box, its tallest word, or its
  words' span) finds 8 or 9, 0, and 2 to 5: capitals set larger than the
  body ("9.4 BIRDSONG") reach no lower than the baseline, so they are no
  taller than a body line with a descender, and page 10's headings are lost;
- the unscaled median finds 9, 6 and 12;
- the scaled median of unparted lines finds 7, 5 and 8, but page 5's
  "6.2" heading shares a line with the body across the gutter and is lost.

:func:`vision_elements` lays the pieces out with
:func:`~datagrunt.pdf.layout.page_layout`, with no lattice (a scan has no
vector rules), so a scan yields headings, paragraphs, key-values, lists
and stream tables; each figure is a slot in the reading order, as on any
page. The layout runs on the page as OCR read it, righted, where its words
are upright: a scan turned 180 degrees reads in order (A4b reads
upside-down text as displayed, its lines and words reversed), and one
turned 90 or 270 reads as upright text. Each element's box is then mapped
back to the displayed page (exactly on an upright page; to within float
rounding on a turned one). Digital text kept apart (hidden pieces, every one carrying a reason)
reads as hidden paragraphs placed among them. Pass C
(:func:`~datagrunt.pdf.assembly.conservation`) checks every piece's chars
against the elements: a mismatch is a warning.

:func:`vision_page` makes the A1 page of a page A3 routed to vision (spec
§3.3 steps 2 and 4), with one render and one OCR pass (and a sample read
each way when a turn is checked):

1. Render: at :func:`~datagrunt.pdf.render.effective_dpi` of the vision
   DPI and the pixel bound, in the guarded worker.
2. Orientation: tesseract's OSD on the normalized render (grey, 1 %
   autocontrast). Its suggestion alone never turns the page (#456): OSD is
   unreliable on sparse text. A turn it suggests at a confidence of
   :data:`ROTATION_CONFIDENCE` or more is checked by reading (:func:`_turn`):
   a sample of the page is read as displayed and turned
   (:func:`~datagrunt.pdf.ocr.read_sample`), and, for a quarter turn, turned
   the other way too (half a turn from it: OSD can pick the wrong side of a
   sideways page). A reading weighs as its mean word confidence, each word
   weighted by its chars (a reading of no words: 0), and decides only when it
   is of text: of :data:`LEAST_TEXT_CHARS` letters or digits or more
   (``SampleReading.text_chars``). The turn is kept once the sample turned is
   of text and reads :data:`TURN_MARGIN` or more over each other reading; it
   is refused once another reading is of text and reads the margin or more
   over it turned. The sample is the page's bands, the one holding the most
   ink first (:func:`~datagrunt.pdf.ocr.inked_bands`): each band read joins
   its words to those before, until the sample decides or every band is read.
   A band of text decides alone. A band of art, often the one holding the most
   ink, reads as a few letters or none, at any confidence: it decides nothing,
   and the next band is read. A sample that decides neither way once every
   band is read leaves the page as displayed. Under the threshold, when OSD
   cannot tell, or when the check does not bear the turn out, the page reads
   as displayed, with a warning (the check's naming each of the sample's
   confidences, and the letters or digits it read turned when they are too
   few).

   Evidence, tesseract 5.5.3, on the corpus (95 pages, each rendered upright
   and turned 90, 180 and 270 degrees, as their scans display):

   - OSD gave the right turn at a confidence of 3.59 or more on 282 of the 285
     turned renders (on the other 3, a cover with almost no text, it could not
     tell, or suggested a wrong turn at 0.02).
   - The check kept 280 of those 282 turns: turned, the sample read 16.9 or
     more over as displayed (21.7 or more on a quarter turn), and 24.9 or more
     over the other way, on 108 letters or digits or more; 273 were decided by
     their first band, none took more than 3. It refused 2: a cover of art and
     a few words of title, turned 90 and 270, whose whole page reads only 4.1
     and 5.1 better turned. They read as displayed, with the warning.
   - Each upright page's 3 wrong turns, checked as if OSD suggested them
     (285): all refused, 272 on their first band, none past the third. A band
     read alone, its turned reading of text, read 5.3 better at a wrong turn
     at most (the margin is over it), the whole page 0.9. A band whose turned
     reading is not of text (art read as marks, or as a letter or two) read up
     to 57.5 over as displayed: such a reading decides nothing.
   - Sparse capitals (#456: 8 upright pages, two lines of capitals each): OSD
     suggests turning each 180 degrees, at 1.83 to 2.04 (3 pages over the
     threshold, read upside down before the check). Their sample reads 95.5 to
     95.8 as displayed and 45.2 to 57.3 turned: refused on the first band.
     Displayed sideways, OSD suggests the wrong side (the turn they are
     displayed at, at 1.78 to 2.05): turned so, the sample reads 47.3 or 55.6
     (upside down), as displayed no words, and the other way 93.9 or 94.4:
     refused on the first band.

   Art first (#456 review): pages whose band holding the most ink is art, each
   rendered upright and turned 90, 180 and 270 degrees:

   - A drawing of random shapes over two lines of text (12 pages, 48 renders):
     OSD gives the right turn on the 36 turned, at 5.26 to 6.11, and the check
     keeps all 36; upside down, on the second or third band (the drawing's
     bands read 7 letters or digits or fewer either way). Without the minimum,
     5 of the 12 upside down were refused on their drawing's band alone, 2 of
     them for reading no words turned. Three of those 5, as raster scans
     displayed upside down (``/Rotate 180``), were refused so too; they are
     righted.
   - Sparse capitals over a blotch of art (8 pages), or over a dark bar (8):
     upright or sideways (48 renders), OSD suggests a wrong turn on 47, at
     2.05 to 2.48; the check refuses each, an upright one on its third band
     (two of art, then the capitals: 78.1 to 96.3 as displayed, 41.4 to 57.7
     turned). Without the minimum, page 2 over the blotch was turned on its
     two art bands alone (51.8 turned on 5 letters or digits, 34.0 as
     displayed on 3), and read as gibberish.
   - A photo-like blotch over four lines of text (9 pages, 36 renders): OSD
     gives the right turn on the 27 turned, at 9.33 to 10.63, and the check
     keeps all 27. Over two short lines of capitals (1 page), OSD cannot tell,
     at any turn.
   - Every right turn of these pages and the sparse capitals (141), checked as
     if OSD suggested it, is kept; every wrong one (420) is refused.

   :data:`LEAST_TEXT_CHARS` is 12. Where a reading of art decided a wrong
   turn, it held 6 letters or digits at most (at a least of 6, a turn of 180
   degrees checked on 2 sideways pages of capitals over the blotch was kept on
   art read as 6); the sparsest text measured, the two short lines over a
   blotch, reads 22 turned right from sideways. Any least from 7 to 22 keeps
   every right turn above and refuses every wrong one, and keeps the corpus's
   280; 12 is about mid-way (their geometric mean is 12.4). The turned reading
   must be of text itself, not only some reading of the sample: weighed so, a
   wrong quarter turn checked on 4 of the drawings displayed upside down (and
   one of their scans) was kept on a glyph or two turned (62.5 on 2 letters or
   digits, against 41.1 on 96 as displayed).

   A page whose sample reads under 12 letters or digits every way reads as
   displayed. No page measured reached the check so: OSD could not tell on all
   224 renders of 56 pages of a word or two ("Page 3", "INVOICE", alone at 12
   to 72 pt or under a drawing), nor on 71 of the 96 renders of 24 pages of
   art alone, and read the other 25 under the threshold. As displayed is the
   safer: OSD is least reliable on the sparsest text (#456: wrong at 2.04 on
   the capitals' 45 letters and digits), a turn is never made unchecked, and
   such a page has under 12 letters or digits of text to read the wrong way.

   The sample is read as sparse text (``--psm 11``), not as the page's bands
   are (``--psm 3``): psm 3 rights sideways text itself, so an upright page's
   sample turned 90 degrees read as well as displayed (on the corpus, a median
   of 0.0 apart, up to 8.2 better turned): it cannot tell a page from its
   quarter turn. psm 11 reads sideways text as no words or poor ones (a median
   51.2 to 54.1 under the upright reading).

   Cost: a page whose turn is checked is OCR'd one extra sample: the band
   holding the most ink, read as displayed and turned (two tesseract calls of
   one band; three for a quarter turn), and the next bands by ink only while
   that does not decide (1.04 bands on average on the corpus's turned renders;
   2 or 3 on a page whose most ink is art). Here that is 0.55 to 1.39 s a
   corpus page turned 180 degrees and 1.07 to 2.42 s one turned 90, where its
   OCR takes 0.55 to 2.67 s; 1.6 s a page of art and text (3 bands), where its
   OCR takes 1.0 s. A page OSD reads upright, or under the threshold, is not
   checked. The sample's calls are within the page's budget and OCR timeout,
   as its bands' are: a check the budget runs out in leaves the page unread
   (``OCR stopped while checking orientation detection's turn of 180 degrees:
   the page's 600 s budget is spent``), as any page whose budget runs out; a
   sample tesseract cannot read (it failed, or timed out) leaves the page as
   displayed, with a warning saying why. A turn is never made unchecked, and
   the displayed page is the PDF's own orientation.
3. OCR: the righted page's bands (900 px, 60 px shared), each word mapped
   back to the displayed page.
4. Elements: :func:`vision_elements` of the words, the page's figures and
   its digital text kept apart, :func:`superseded`: each visible piece is
   flagged ``"superseded"`` (A1), each hidden one keeps its own reasons, so
   nothing is superseded twice and nothing is dropped.
5. The page: ``status="ok"``, ``route="vision"``, never
   ``integrity_verified`` (A1); A3's quality with the words' mean
   confidence (``ocr_mean_confidence``, each word counting once); a lazy
   :class:`~datagrunt.pdf.elements.PageImage` whose loaders
   (:class:`PageRender`) draw the page again on demand, so no pixels are
   held between pages. A page whose ``ocr_mean_confidence`` is under
   :data:`LOW_OCR_CONFIDENCE` has a warning naming it: the page may be
   turned the wrong way or unreadable, so its text should not be trusted
   as is (#456). Its status and its text stay as they are (extraction never
   drops text), and no more OCR is run: the mean is the one the page
   carries. A page of no words has no mean, and no such warning.

   It is how a page read the wrong way up shows: step 2 checks only a turn
   OSD suggests, and OSD reads the sparse capitals displayed upside down as
   upright (at 1.86 to 2.09), so they are read upside down, as gibberish.
   Evidence, tesseract 5.5.3, each page read as this route reads it:

   - The corpus (95 pages, ``ocr="always"``), right way up: 94 pages of
     words (a cover reads none), at 56.3 at least, 87.5 at the 5th
     percentile, 94.3 the median and 96.5 at most. Only 3 are under 87.4:
     79.7, and two pages of art: 58.3, a picture of the board with cards of
     small, slanted text, and 56.3, a cover with a contents list whose dot
     leaders read as words of no confidence. With ``ocr="auto"`` no corpus
     page is read by OCR (76 hybrid, 19 digital).
   - The corpus upside down (each page's render turned 180 degrees and
     read as displayed, unturned: what a turn OSD misses reads): 7.3 at
     least, 37.2 the median, 41.0 at the 95th percentile and 43.9 at most.
     As this route reads them, step 2 rights 94 of the 95; the other, the
     cover OSD cannot orient, reads 2 words, at 20.1.
   - Sparse capitals (#456, 8 pages): upright, 95.1 to 95.9; displayed
     upside down (``/Rotate 180``), 50.9 to 59.3.

   :data:`LOW_OCR_CONFIDENCE` is 70, about mid-way between the highest
   reading upside down (59.3) and the third lowest right way up (79.7). It
   flags all 103 pages read upside down and 2 of the 102 read right way up:
   the corpus's two pages of art, whose text OCR reads in part from the
   art. The mean weighted by chars (step 2's) flags the same pages at 70
   (upside down 57.4 at most; right way up 20.8 and 59.8, then 83.0 and
   over), so the page's own ``ocr_mean_confidence`` is used: the warning
   names the number the page carries.

The page's render and OCR share one time budget (#418),
:attr:`VisionOptions.page_budget_s` (600 s; a page takes 2-4 s here),
counted from the page's start: each call (the render, OSD, a sample's, a
band) is given at most its own timeout and the time left, and none starts
once it is spent. So a page's render and OCR end within its budget (and the last
call's reap) however large the page is: its bands are many on a tall page.
A page whose budget runs out is not read (the warning says how many of its
bands were: ``OCR stopped after 1 of 4 bands: the page's 600 s budget is
spent``).

When the page is not rendered (``RenderRefused``, ``RenderFailed``) or
tesseract cannot read it (missing, failed, timed out, its budget spent), it is
``"ocr_unavailable"``: :func:`~datagrunt.pdf.assembly.assemble_page`'s
page of its digital text, the page image when it was rendered, and a
warning naming why. Words read before a band failed are not used: the page
is the digital text's, never half of each. assemble_page is given the
vision DPI and pixel bound the page was rendered within, so its warning
that the page has no page image agrees with the image (#456).

:func:`fallback_page` makes the A1 page of a page A2 could not read (its
``PageFailure``; spec §3.3 step 2, per-page fault isolation): only its
geometry is read (``page_geometry``), then it is rendered and read as a
vision page is, with no digital text and no figures (none was read). Read,
it is ``"ocr_fallback"`` (route ``"vision"``, never verified): its quality
is the OCR's (the words' chars by A3's rule, their mean confidence; no
vector line, hidden span or skew was measured: 0, 0 and None), its first
warning says extraction failed and what was not read, and a mean word
confidence under :data:`LOW_OCR_CONFIDENCE` is flagged as on a vision page
(step 5). Only a page whose text is OCR's is so flagged: never an
``"ocr_unavailable"`` page (its text is the digital text's). When its geometry,
render or OCR fails too, or ``ocr="never"``, it is an ``"error"`` page (A1:
no elements, no quality) whose ``error`` names the extraction failure and
then why it was not read; a page whose geometry cannot be read is pdfium's
default 612 x 792 pt, with a warning.

Known limitations (A5b evidence: 10 corpus pages forced to OCR):

- What a word's glyphs reach is read from its plain Latin letters, digits
  and brackets only: other chars (another script, accents, cedillas)
  count for nothing, so a word of them alone is taken as one-sided, and
  "ça" as x-height only.
- OCR's misreads reach the layout: only words of no confidence are laid
  out apart. Every char is still read once (Pass C), but the elements'
  words recall 80.7 % of the digital words where the OCR words themselves
  recall 84.4 % (#416; 75.2 % and 84.5 % before it dropped the band seams'
  fragments, sized words by their lines and laid art apart).
- Only a turn OSD suggests is checked (step 2): a turned page OSD reads as
  upright is read as displayed. The sparse capitals displayed upside down
  are so: OSD reads each as upright (at 1.86 to 2.09), and its OCR is
  gibberish, at 50.9 to 59.3. Such a page is flagged (step 5), not
  righted: righting it would need more OCR of every page OSD reads as
  upright. One read the wrong way at :data:`LOW_OCR_CONFIDENCE` or more
  would not be flagged; none was measured (59.3 at most).
- The flag cannot tell a page read the wrong way from one hard to read:
  two corpus pages of art read right way up (56.3 and 58.3) are flagged
  too.
- A wrong turn OSD suggests on a page displayed turned can be kept (step
  2): checked as if OSD suggested them, 17 of the 570 wrong turns of the
  corpus's turned renders were (text read turned the wrong way is junk,
  and junk can read the margin over junk). OSD suggested none of them: its
  282 suggestions on those renders were all right.
"""

import dataclasses
import math
import os
import statistics
import time
from pathlib import Path
from typing import NamedTuple

from datagrunt.pdf._loaders import PageRender, _checked_turn
from datagrunt.pdf._options import VisionOptions
from datagrunt.pdf._pdfium_common import Box, union_box
from datagrunt.pdf._source import SourceIdentity, check_binding
from datagrunt.pdf._validate import choice_str, typed_tuple
from datagrunt.pdf.assembly import (
    MIN_IMAGE_DIMENSION,
    FigureDraft,
    assemble_page,
    check_assessment,
    conservation,
    page_elements,
    page_figures,
    raw_page_warnings,
)
from datagrunt.pdf.elements import BBox, DocumentElement, Figure, HiddenReason, Page, PageImage, PageQuality, Table
from datagrunt.pdf.exceptions import PDFCorruptError
from datagrunt.pdf.layout import LatticeResult, find_lattice_tables, page_layout
from datagrunt.pdf.ocr import (
    OCRWord,
    Orientation,
    SampleReading,
    bands,
    inked_bands,
    normalized,
    ocr_words,
    orientation,
    read_sample,
    rotated,
)
from datagrunt.pdf.pdfium_engine import (
    DocumentHandle,
    PageFailure,
    PageGeometry,
    RawPage,
    RawSpan,
    page_geometry,
)
from datagrunt.pdf.quality import OcrMode, PageAssessment, SpanPiece, valid_char
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page

__all__ = [
    "LEAST_SIZE_PT",
    "LEAST_TEXT_CHARS",
    "LOW_OCR_CONFIDENCE",
    "ROTATION_CONFIDENCE",
    "TURN_MARGIN",
    "PageRender",
    "VisionElements",
    "VisionOptions",
    "fallback_page",
    "ocr_pieces",
    "superseded",
    "vision_elements",
    "vision_page",
]

# The least font size an OCR piece is given: layout reads line sizes to 0.1 pt, and a line's size is over 0.
LEAST_SIZE_PT = 0.1

# What a word's glyphs reach (module notes): x-height letters only; above it (capitals, digits, ascenders, the dots
# of i and j); below the baseline (descenders). Brackets and bars reach both sides.
_X_HEIGHT = frozenset("acemnorsuvwxz")
_ABOVE = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789bdfhijklt()[]{}|")
_BELOW = frozenset("gjpqy()[]{}|")
# A word's box height times this is a one-sided word's on its line (module notes: 1 / 0.656 and 1 / 1.323 measured).
_X_HEIGHT_SCALE = 1.5
_BOTH_SIDES_SCALE = 0.75
# A tesseract line is parted where its words are over this many ems apart (the layout's cell gap; module notes).
_PART_GAP_EM = 1.8

# OCR reads ink as it is painted: opaque black, filled (render mode 0).
_PAINTED = (0, 0, 0, 255)
_FILL = 0

# A scan has no vector rules: no lattice tables, no text used by them.
_NO_LATTICE = LatticeResult(tables=(), used_pieces=())

# OSD's turn is checked at this confidence or more (see the module notes: right turns 3.59 and up, the wrong one 0.02).
ROTATION_CONFIDENCE = 2.0
# A turn OSD suggests is kept when its sample's mean word confidence turned is this much over it as displayed and, for a
# quarter turn, over it turned the other way, or more (module notes, step 2: right turns 16.9 and up over as displayed,
# 24.9 over the other way; a band of a wrong turn read as text, 5.3 over at most on the corpus, 11.3 on art pages).
TURN_MARGIN = 15.0
# A sample reading decides a turn only when it holds this many letters or digits, or more: it is of text (module notes,
# step 2: art that decided a wrong turn read 6 at most; the sparsest text measured, 22).
LEAST_TEXT_CHARS = 12
# A page OCR read at a mean word confidence (its ocr_mean_confidence) under this is flagged in its warnings (module
# notes, step 5: pages read upside down 59.3 at most; right way up 79.7 and over, but for 2 pages of art).
LOW_OCR_CONFIDENCE = 70.0

_SUPERSEDED: tuple[HiddenReason, ...] = ("superseded",)
_OCR_MODES: tuple[OcrMode, ...] = ("auto", "always", "never")
# What pdfium reads for a page with no MediaBox: an error page whose own size cannot be read has this one.
_DEFAULT_PAGE_SIZE = (612.0, 792.0)
_NOT_READ = "the page was read by OCR of its render: its text layer, vector rules and images were not read"


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

    A span's ``object_index`` is its word's position in ``words``; its size
    is its line's (:func:`_word_sizes`).

    Raises:
        TypeError: If ``words`` is not a tuple of OCRWord.
    """
    checked = typed_tuple("words", words, OCRWord)
    sizes = _word_sizes(checked)
    return tuple(_word_piece(item, index, sizes[index]) for index, item in enumerate(checked))


def _word_sizes(words: tuple[OCRWord, ...]) -> dict[int, float]:
    """Each word's font size, by its position: its line part's (:func:`_line_parts`). O(w log w) for w words.

    A part's size is the median of its words' heights, each scaled by
    :func:`_reach_scale`, and at least :data:`LEAST_SIZE_PT`.
    """
    scaled = [(item.bbox.y1 - item.bbox.y0) * _reach_scale(item.text) for item in words]
    # A line's words of no confidence are sized apart from its others, as they are laid out apart.
    lines: dict[tuple[int, bool], list[int]] = {}
    for index, item in enumerate(words):
        lines.setdefault((item.line, _no_confidence(item)), []).append(index)
    sizes: dict[int, float] = {}
    for members in lines.values():
        for part in _line_parts(words, scaled, members):
            sizes.update(dict.fromkeys(part, max(statistics.median(scaled[index] for index in part), LEAST_SIZE_PT)))
    return sizes


def _no_confidence(word: OCRWord) -> bool:
    """True if tesseract read ``word`` with a confidence of 0: art read as letters, as a rule (module notes)."""
    return word.confidence == 0.0


def _line_parts(words: tuple[OCRWord, ...], scaled: list[float], members: list[int]) -> list[list[int]]:
    """One tesseract line's words (``members``, by position) in parts along it, cut at its gutters.

    tesseract can read lines of neighbouring columns as one (Root's Law p.
    5: a body line and the heading "6.2 FACTION RULES AND ABILITIES"
    across the gutter). A part ends where the next word starts over
    :data:`_PART_GAP_EM` ems past all before it, the em the larger scaled
    height of it and the word before: the layout's cell gap.
    """
    ordered = sorted(members, key=lambda index: words[index].bbox.x0)
    parts = [[ordered[0]]]
    reach = words[ordered[0]].bbox.x1
    for before, index in zip(ordered, ordered[1:]):
        box = words[index].bbox
        if box.x0 - reach > _PART_GAP_EM * max(scaled[before], scaled[index]):
            parts.append([index])
        else:
            parts[-1].append(index)
        reach = max(reach, box.x1)
    return parts


def _reach_scale(text: str) -> float:
    """What scales a word's box height to a one-sided word's on its line (module notes), by what its glyphs reach.

    Both sides of the x-height (an ascender or capital and a descender, or a
    bracket): 3/4. One side: 1. Neither (x-height letters, with any other
    chars): 3/2. No letter, digit or bracket read (a dash, another script): 1.
    """
    above = any(char in _ABOVE for char in text)
    below = any(char in _BELOW for char in text)
    if above and below:
        return _BOTH_SIDES_SCALE
    if above or below or not any(char in _X_HEIGHT for char in text):
        return 1.0
    return _X_HEIGHT_SCALE


def _word_piece(word: OCRWord, index: int, size: float) -> SpanPiece:
    """``word`` as the whole of a visible span of font ``size``: its chars split along its box."""
    box = word.bbox
    count = len(word.text)
    # The last edge is the word's own, so the chars tile its box exactly whatever the float division.
    edges = [box.x0 + (box.x1 - box.x0) * at / count for at in range(count)] + [box.x1]
    chars = tuple((edges[at], box.y0, edges[at + 1], box.y1) for at in range(count))
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
    rotation: int = 0,
    apart: tuple[SpanPiece, ...] = (),
    recovered: tuple[SpanPiece, ...] = (),
    figures: tuple[FigureDraft, ...] = (),
) -> VisionElements:
    """The page's elements from its OCR ``words`` (see the module notes), with its digital text kept apart.

    The layout runs on the page as OCR read it, turned clockwise by
    ``rotation`` (its words upright, so their lines read in order whatever
    the page's turn); each element's box is then mapped back to the page as
    displayed, and a turned page's table has no column centres (A6 links
    only columns along the displayed x). A figure keeps its own box. A word
    of no confidence reads apart from the others, as visible text of its
    own (see the module notes).

    Args:
        words: The page's OCR words (``OCRResult.words``), on the displayed page.
        geometry: The page's geometry: its number and displayed size.
        rotation: The clockwise turn, 0, 90, 180 or 270, OCR read the page at.
        apart: Digital pieces of the page laid out apart from the words:
            each must carry a hidden reason (the vision page supersedes its
            visible ones first).
        recovered: Recovered digital pieces, likewise all hidden.
        figures: The page's figure drafts; each reads as a figure slot.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: If ``rotation`` is not a right-angle turn, a piece of
            ``apart`` or ``recovered`` is visible, or as
            :func:`~datagrunt.pdf.layout.page_layout` raises.
    """
    typed_tuple("words", words, OCRWord)
    if not isinstance(geometry, PageGeometry):
        raise TypeError(f"geometry must be a PageGeometry, got {type(geometry).__name__}")
    frame = _Frame(_checked_turn(rotation), geometry.width_pt, geometry.height_pt)
    _check_hidden("apart", apart)
    _check_hidden("recovered", recovered)
    typed_tuple("figures", figures, FigureDraft)
    righted = tuple(_righted_word(item, frame) for item in words)
    read = ocr_pieces(righted)
    art = tuple(piece for piece, item in zip(read, righted) if _no_confidence(item))
    text = tuple(piece for piece, item in zip(read, righted) if not _no_confidence(item))
    spans: dict[int, RawSpan] = {}
    kept = tuple(_righted_piece(piece, frame, spans) for piece in apart)
    copies = tuple(_righted_piece(piece, frame, spans) for piece in recovered)
    width, height = frame.righted_size
    slots = tuple(_bbox(frame.righted(_box(draft.bbox))) for draft in figures)
    # Words of no confidence go with the recovered text: laid out apart, as lines of their own (visible: no reason).
    layout = page_layout((*text, *kept), (*copies, *art), _NO_LATTICE, width_pt=width, height_pt=height, figures=slots)
    elements = tuple(_displayed(element, frame) for element in page_elements(layout.items, figures, geometry))
    _, mismatch = conservation((*read, *kept, *copies), elements)
    return VisionElements(elements=elements, warnings=(*layout.warnings, *mismatch))


class _Frame(NamedTuple):
    """The page as OCR read it: displayed ``width`` x ``height`` points, turned clockwise by ``rotation``."""

    rotation: int
    width: float
    height: float

    @property
    def righted_size(self) -> tuple[float, float]:
        """The turned page's width and height."""
        return (self.height, self.width) if self.rotation in (90, 270) else (self.width, self.height)

    def righted(self, box: Box) -> Box:
        """A box on the displayed page, on the turned page (the turn OCR made)."""
        x0, y0, x1, y1 = box
        if self.rotation == 90:
            return self.height - y1, x0, self.height - y0, x1
        if self.rotation == 180:
            return self.width - x1, self.height - y1, self.width - x0, self.height - y0
        if self.rotation == 270:
            return y0, self.width - x1, y1, self.width - x0
        return box

    def displayed(self, box: Box) -> Box:
        """A box on the turned page, on the displayed page (ocr.py's mapping of a word's box, in points)."""
        x0, y0, x1, y1 = box
        if self.rotation == 90:
            return y0, self.height - x1, y1, self.height - x0
        if self.rotation == 180:
            return self.width - x1, self.height - y1, self.width - x0, self.height - y0
        if self.rotation == 270:
            return self.width - y1, x0, self.width - y0, x1
        return box


def _box(bbox: BBox) -> Box:
    """A BBox as a plain ``(x0, y0, x1, y1)``."""
    return bbox.x0, bbox.y0, bbox.x1, bbox.y1


def _bbox(box: Box) -> BBox:
    """A plain ``(x0, y0, x1, y1)`` as a BBox."""
    x0, y0, x1, y1 = box
    return BBox(x0=x0, y0=y0, x1=x1, y1=y1)


def _righted_word(word: OCRWord, frame: _Frame) -> OCRWord:
    """``word`` on the turned page."""
    if frame.rotation == 0:
        return word
    return dataclasses.replace(word, bbox=_bbox(frame.righted(_box(word.bbox))))


def _righted_piece(piece: SpanPiece, frame: _Frame, spans: dict[int, RawSpan]) -> SpanPiece:
    """``piece`` of its span on the turned page; pieces of one span share one turned span (``spans``, by id)."""
    if frame.rotation == 0:
        return piece
    span = spans.get(id(piece.span))
    if span is None:
        chars = tuple(frame.righted(box) for box in piece.span.char_boxes)
        tight = tuple(frame.righted(box) for box in piece.span.tight_boxes)
        span = dataclasses.replace(piece.span, bbox=union_box(chars), char_boxes=chars, tight_boxes=tight)
        spans[id(piece.span)] = span
    return SpanPiece(span=span, start=piece.start, end=piece.end, hidden_reasons=piece.hidden_reasons)


def _displayed(element: DocumentElement, frame: _Frame) -> DocumentElement:
    """An element laid out on the turned page, boxed on the displayed page (the same box when upright).

    A figure keeps its own box; a turned page's table has no column centres or intervals.
    """
    if isinstance(element, Figure):
        return element
    bbox = _bbox(frame.displayed(_box(element.bbox)))
    if isinstance(element, Table) and frame.rotation != 0:
        return dataclasses.replace(element, bbox=bbox, column_centres=(), column_intervals=())
    return dataclasses.replace(element, bbox=bbox)


def _check_hidden(name: str, pieces: object) -> None:
    """Require a tuple of SpanPiece, each with a hidden reason: a vision page's digital text never mixes with OCR's."""
    for index, piece in enumerate(typed_tuple(name, pieces, SpanPiece)):
        if not piece.hidden_reasons:
            raise ValueError(f"{name}[{index}] is visible ({piece.text!r}): a vision page's digital text reads apart")


def superseded(pieces: tuple[SpanPiece, ...]) -> tuple[SpanPiece, ...]:
    """``pieces``, each visible one (no hidden reason) flagged ``"superseded"``; a hidden one as it is. O(pieces).

    Raises:
        TypeError: If ``pieces`` is not a tuple of SpanPiece.
    """
    return tuple(
        piece
        if piece.hidden_reasons
        else SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=_SUPERSEDED)
        for piece in typed_tuple("pieces", pieces, SpanPiece)
    )


def vision_page(
    raw_page: RawPage,
    assessment: PageAssessment,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    identity: SourceIdentity | None = None,
    sha256: str | None = None,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
    options: VisionOptions = VisionOptions(),
) -> Page:
    """The A1 page of a page A3 routed to vision: OCR's elements, or its digital text's (see the module notes).

    A page OCR read at a mean word confidence under
    :data:`LOW_OCR_CONFIDENCE` keeps its status and text, with a warning
    that its text should not be trusted as is (module notes, step 5).

    Args:
        raw_page: The page's raw facts (A2's ``RawPage``).
        assessment: A3's assessment of it, routed ``"vision"``.
        source: The PDF the page was read from: it is rendered again from it.
        password: The PDF's password, or None.
        identity: The identity of the file at ``source`` the document was
            extracted from (the pipeline's); given with ``sha256`` or not at
            all.
        sha256: The document's sha256: the page image's draws refuse a file
            at ``source`` whose bytes are not the document's (both None
            check nothing).
        min_image_dimension: The least pixel side of a figure (A4b's).
        options: How the page is rendered and read.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: If ``assessment`` is not of ``raw_page``, or does not
            route it to vision, or only one of ``identity`` and ``sha256``
            is given.
    """
    check_assessment(raw_page, assessment)
    check_binding(identity, sha256)
    if assessment.route != "vision":
        raise ValueError(f"assessment routes the page {assessment.route!r}, not 'vision': assemble_page lays it out")
    if not isinstance(options, VisionOptions):
        raise TypeError(f"options must be a VisionOptions, got {type(options).__name__}")
    geometry = raw_page.geometry
    deadline = time.monotonic() + options.page_budget_s
    drawn = _drawn(source, geometry, password, options, deadline)
    where = _Where(source, password, identity, sha256)
    if drawn.page is None:
        return _unread(raw_page, assessment, drawn.failure, None, where, min_image_dimension, options)
    reading = _read(drawn.page, drawn.dpi, geometry, options, deadline)
    image = _page_image(drawn.page, drawn.dpi, reading.rotation, geometry, where, options)
    if reading.failure:
        return _unread(raw_page, assessment, reading.failure, image, where, min_image_dimension, options)
    figures = page_figures(
        raw_page.images, geometry, source=source, password=password, min_image_dimension=min_image_dimension
    )
    laid = vision_elements(
        reading.words,
        geometry=geometry,
        rotation=reading.rotation,
        apart=superseded(assessment.pieces),
        recovered=superseded(assessment.recovered_pieces),
        figures=figures.drafts,
    )
    confidence = _mean_confidence(reading.words)
    notes = (*reading.notes, *_low_confidence(confidence))
    warnings = (*raw_page_warnings(raw_page), *figures.warnings, *assessment.warnings, *notes, *laid.warnings)
    return Page(
        page_number=geometry.page_number,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        status="ok",
        route="vision",
        quality=dataclasses.replace(assessment.quality, ocr_mean_confidence=confidence),
        elements=laid.elements,
        page_image=image,
        warnings=warnings,
    )


class _Where(NamedTuple):
    """The PDF a page image is drawn from: its path, its password, and the identity and sha256 of the file read."""

    source: str | os.PathLike[str]
    password: str | None
    identity: SourceIdentity | None
    sha256: str | None


class _Drawn(NamedTuple):
    """A page's render at its DPI, or why it was not rendered (``page`` None)."""

    page: RenderedPage | None
    dpi: int
    failure: str


def _drawn(
    source: str | os.PathLike[str],
    geometry: PageGeometry,
    password: str | None,
    options: VisionOptions,
    deadline: float,
) -> _Drawn:
    """The page rendered at its effective DPI in the guarded worker by ``deadline``; a refusal or failure is why not."""
    try:
        dpi = effective_dpi(options.vision_dpi, options.max_render_pixels, geometry.width_pt, geometry.height_pt)
        left = deadline - time.monotonic()
        if left <= 0.0:
            return _Drawn(None, 0, f"the page was not rendered: {_spent(options)}")
        page = render_page(
            source,
            geometry.page_number - 1,
            dpi=dpi,
            password=password,
            memory_cap_bytes=options.memory_cap_bytes,
            timeout_s=min(options.render_timeout_s, left),
        )
    except (RenderRefused, RenderFailed) as exc:
        return _Drawn(None, 0, f"the page was not rendered: {exc}")
    return _Drawn(page, dpi, "")


class _Reading(NamedTuple):
    """What OCR read on a page: its words, the turn it read at, notes on the turn, and why it failed ("" if not)."""

    words: tuple[OCRWord, ...]
    rotation: int
    notes: tuple[str, ...]
    failure: str


def _read(page: RenderedPage, dpi: int, geometry: PageGeometry, options: VisionOptions, deadline: float) -> _Reading:
    """The render normalized, oriented, turned, banded and read by tesseract.

    One OSD call; when it suggests a turn to check, two or three calls a band
    of its sample (:func:`_turn`); then one call a band. Every call ends by
    ``deadline`` (the page's budget): none starts after it.
    """
    grey = normalized(page)
    left = deadline - time.monotonic()
    if left <= 0.0:
        return _Reading((), 0, (), f"OCR stopped before orientation detection: {_spent(options)}")
    found = orientation(grey, timeout_s=min(options.ocr_timeout_s, left))
    if found.status == "unavailable":
        return _Reading((), 0, (), f"tesseract is unavailable: {found.reason}")
    turn = _turn(grey, found, options, deadline)
    if turn.failure:
        return _Reading((), 0, turn.notes, turn.failure)
    result = ocr_words(
        bands(rotated(grey, turn.degrees)),
        dpi=dpi,
        rotation=turn.degrees,
        page_size_pt=(geometry.width_pt, geometry.height_pt),
        timeout_s=options.ocr_timeout_s,
        language=options.language,
        deadline=deadline,
    )
    if result.status == "ok":
        return _Reading(result.words, turn.degrees, turn.notes, "")
    unused = f"; the {len(result.words)} word(s) it read before that are not used" if result.words else ""
    if result.status == "spent":
        return _Reading((), turn.degrees, turn.notes, f"{result.reason}: {_spent(options)}{unused}")
    return _Reading((), turn.degrees, turn.notes, f"{_tesseract(result.status, result.reason)}{unused}")


def _spent(options: VisionOptions) -> str:
    """Why a page's render or OCR stopped: its budget ran out."""
    return f"the page's {options.page_budget_s:g} s budget is spent"


def _tesseract(status: str, reason: str) -> str:
    """Why tesseract read nothing: its call's status and reason."""
    return f"tesseract {'is unavailable' if status == 'unavailable' else status}: {reason}"


class _Turn(NamedTuple):
    """The turn OCR reads the page at, notes on it, and why the page is not read ("" when it is)."""

    degrees: int
    notes: tuple[str, ...]
    failure: str = ""


def _turn(grey: RenderedPage, found: Orientation, options: VisionOptions, deadline: float) -> _Turn:
    """The turn OCR reads ``grey`` at: OSD's ``found``, made only when its sample reads better so (module notes, 2).

    A turn suggested under :data:`ROTATION_CONFIDENCE` is not checked. The
    check's calls end by ``deadline``: one cut at it, or not started, leaves
    the page unread (its ``failure``).
    """
    confidence = found.confidence
    if found.status != "ok" or confidence is None:
        return _Turn(0, (f"orientation detection {found.status}: {found.reason}; OCR read the page as displayed",))
    if found.degrees == 0:
        return _Turn(0, ())
    turning = f"turning the page {found.degrees} degrees clockwise at a confidence of {confidence:g}"
    suggests = f"orientation detection suggests {turning}"
    if confidence < ROTATION_CONFIDENCE:
        return _Turn(0, (f"{suggests}, under {ROTATION_CONFIDENCE:g}; OCR read it as displayed",))
    sample = _sampled(grey, found.degrees, options, deadline)
    if isinstance(sample, SampleReading):
        if sample.status == "spent":
            return _Turn(0, (), f"OCR stopped while checking {_suggested(found)}: {_spent(options)}")
        unread = f"its sample could not be read ({_tesseract(sample.status, sample.reason)})"
        return _Turn(0, (f"{suggests}, but {unread}, so the turn is not checked; OCR read it as displayed",))
    reads = sample.reads(found.degrees)
    verdict = sample.verdict()
    if verdict:
        righted = f"OCR righted it by {found.degrees} degrees clockwise"
        return _Turn(
            found.degrees, (f"the page reads turned: {righted} (orientation confidence {confidence:g}; {reads})",)
        )
    return _Turn(0, (f"{suggests}, but {reads} ({_not_borne_out(sample, verdict)}); OCR read it as displayed",))


def _suggested(found: Orientation) -> str:
    """The turn OSD suggests, named."""
    return f"orientation detection's turn of {found.degrees} degrees"


class _Sample(NamedTuple):
    """A page's sample read as displayed, turned as OSD suggests, and (a quarter turn) turned the other way."""

    displayed: SampleReading
    turned: SampleReading
    opposite: SampleReading | None

    def others(self) -> tuple[SampleReading, ...]:
        """The readings the turned one is weighed against: as displayed, and turned the other way if read."""
        return (self.displayed,) if self.opposite is None else (self.displayed, self.opposite)

    def gaps(self) -> tuple[float, ...]:
        """How much better the sample reads turned than each other way it was read (:func:`_gap`)."""
        return tuple(_gap(other, self.turned) for other in self.others())

    def verdict(self) -> bool | None:
        """True, the turn is borne out; False, refused; None while the sample decides neither (notes, step 2).

        A gap decides only for a reading of text: of :data:`LEAST_TEXT_CHARS`
        letters or digits or more. The turn is borne out once the sample
        turned is one, and reads better than every other way by
        :data:`TURN_MARGIN`; refused once another way is one, and reads
        better than it turned by the margin.
        """
        gaps = self.gaps()
        if self.turned.text_chars >= LEAST_TEXT_CHARS and all(gap >= TURN_MARGIN for gap in gaps):
            return True
        texts = (other.text_chars >= LEAST_TEXT_CHARS for other in self.others())
        return False if any(gap <= -TURN_MARGIN and text for gap, text in zip(gaps, texts)) else None

    def reads(self, degrees: int) -> str:
        """How the sample reads each way, as a warning names it (``degrees``: the turn OSD suggests)."""
        turned, displayed = _reads(self.turned), _reads(self.displayed)
        if self.opposite is None:
            return f"its sample reads {turned} turned and {displayed} as displayed"
        other = f"{_reads(self.opposite)} turned {_opposite(degrees)} degrees"
        return f"its sample reads {turned} turned, {displayed} as displayed and {other}"


def _opposite(degrees: int) -> int:
    """The turn half a turn from ``degrees``."""
    return (degrees + 180) % 360


def _not_borne_out(sample: _Sample, verdict: bool | None) -> str:
    """Why the sample does not bear its turn out: another way read better, or turned it read too little text."""
    chars = sample.turned.text_chars
    if verdict is None and chars < LEAST_TEXT_CHARS:
        under = f"under {LEAST_TEXT_CHARS}: too little text to bear a turn out"
        return f"turned, it reads {chars} letters or digits, {under}"
    return f"mean word confidence; a turn needs {TURN_MARGIN:g} more"


def _sampled(grey: RenderedPage, degrees: int, options: VisionOptions, deadline: float) -> _Sample | SampleReading:
    """The page's sample read as displayed and turned by ``degrees``, and the other way too for a quarter turn.

    Its bands are read most ink first (:func:`~datagrunt.pdf.ocr.inked_bands`),
    each every way, joining the words read before, until the joined
    readings decide (:meth:`_Sample.verdict`; readings of too few letters
    and digits decide nothing) or every band is read: two calls a band
    (three for a quarter turn), each within its timeout and ``deadline``.
    The first reading that fails ends it, and is returned.
    """
    turns = (0, degrees) if degrees == 180 else (0, degrees, _opposite(degrees))
    words: tuple[list[tuple[str, float]], ...] = tuple([] for _ in turns)
    for band in inked_bands(grey):
        for side, turn in enumerate(turns):
            reading = read_sample(
                rotated(band, turn), timeout_s=options.ocr_timeout_s, language=options.language, deadline=deadline
            )
            if reading.status != "ok":
                return reading
            words[side].extend(reading.words)
        if _joined(words).verdict() is not None:
            break
    return _joined(words)


def _joined(words: tuple[list[tuple[str, float]], ...]) -> _Sample:
    """The words of a sample's bands read so far each way (displayed, turned, other way if read), as one sample."""
    readings = [SampleReading(words=tuple(read), status="ok", reason="") for read in words]
    return _Sample(displayed=readings[0], turned=readings[1], opposite=readings[2] if len(readings) > 2 else None)


def _gap(other: SampleReading, turned: SampleReading) -> float:
    """How much better the sample reads turned than ``other``: a reading of no words counts as a confidence of 0."""
    return _confidence_or_0(turned) - _confidence_or_0(other)


def _confidence_or_0(reading: SampleReading) -> float:
    """A sample reading's mean word confidence; 0 for no words (no evidence either way, #456)."""
    return reading.confidence if reading.confidence is not None else 0.0


def _reads(reading: SampleReading) -> str:
    """A sample reading's mean word confidence, as a warning names it."""
    return "no words" if reading.confidence is None else f"at {reading.confidence:.1f}"


def _page_image(
    page: RenderedPage,
    dpi: int,
    rotation: int,
    geometry: PageGeometry,
    where: _Where,
    options: VisionOptions,
) -> PageImage:
    """The page's lazy image: the render's size as displayed, its loaders a :class:`PageRender` (no pixels held)."""
    loader = PageRender(
        source=Path(where.source),
        page_index=geometry.page_number - 1,
        dpi=dpi,
        rotation=rotation,
        password=where.password,
        memory_cap_bytes=options.memory_cap_bytes,
        timeout_s=options.render_timeout_s,
        identity=where.identity,
        sha256=where.sha256,
    )
    return PageImage(
        page_number=geometry.page_number,
        dpi=float(dpi),
        width_px=page.px_width,
        height_px=page.px_height,
        contrast_normalized=True,
        _loader=loader.png_bytes,
        _band_loader=loader.band_png_bytes,
    )


def _mean_confidence(words: tuple[OCRWord, ...]) -> float | None:
    """The words' mean tesseract confidence, each word counting once; None for no words (no mean to take)."""
    return math.fsum(word.confidence for word in words) / len(words) if words else None


def _low_confidence(confidence: float | None) -> tuple[str, ...]:
    """The warning of a page OCR read at a mean word ``confidence`` under :data:`LOW_OCR_CONFIDENCE`, if it was.

    See the module notes, step 5. No words read (None) warns of nothing: no
    text of OCR's is on the page.
    """
    if confidence is None or confidence >= LOW_OCR_CONFIDENCE:
        return ()
    read = f"OCR read the page at a mean word confidence of {confidence:.1f}, under {LOW_OCR_CONFIDENCE:g}"
    return (f"{read}: it may be turned the wrong way or unreadable, so its text should not be trusted as is",)


def _unread(
    raw_page: RawPage,
    assessment: PageAssessment,
    reason: str,
    image: PageImage | None,
    where: _Where,
    min_image_dimension: int,
    options: VisionOptions,
) -> Page:
    """The ``"ocr_unavailable"`` page: assemble_page's of the digital text, the image if rendered, and why.

    assemble_page checks the page image at the vision DPI and pixel bound
    the render used, so its warning that the page has none is given exactly
    when ``image`` is None for that reason (#456), never by its defaults.
    """
    lattice = find_lattice_tables(raw_page.segments.segments, assessment.pieces)
    page = assemble_page(
        raw_page,
        assessment,
        lattice,
        source=where.source,
        password=where.password,
        identity=where.identity,
        sha256=where.sha256,
        min_image_dimension=min_image_dimension,
        dpi=options.vision_dpi,
        max_render_pixels=options.max_render_pixels,
    )
    return dataclasses.replace(
        page, page_image=image, warnings=(*page.warnings, f"OCR could not read the page: {reason}")
    )


def fallback_page(
    handle: DocumentHandle,
    failure: PageFailure,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    identity: SourceIdentity | None = None,
    sha256: str | None = None,
    ocr: OcrMode = "auto",
    options: VisionOptions = VisionOptions(),
) -> Page:
    """The A1 page of a page whose extraction failed: read by OCR of its render, or an error page (module notes).

    Read at a mean word confidence under :data:`LOW_OCR_CONFIDENCE`, it is
    flagged as a vision page is (module notes, step 5).

    Args:
        handle: The open document the page failed in; its geometry is read again from it.
        failure: A2's ``PageFailure`` of the page.
        source: The PDF's path: the page is rendered from it.
        password: The PDF's password, or None.
        identity: The identity of the file at ``source`` the document was
            extracted from (the pipeline's); given with ``sha256`` or not at
            all.
        sha256: The document's sha256: the page image's draws refuse a file
            at ``source`` whose bytes are not the document's (both None
            check nothing).
        ocr: The reader's OCR option: ``"never"`` reads nothing by OCR.
        options: How the page is rendered and read.

    Raises:
        TypeError, ValueError: If an argument is of the wrong type or value
            (only one of ``identity`` and ``sha256`` given, say).
        IndexError: If ``failure`` names a page the document does not have.
    """
    _check_fallback(handle, failure, ocr, options)
    check_binding(identity, sha256)
    failed = f"extraction failed at stage {failure.stage!r}: {failure.message}"
    try:
        geometry = page_geometry(handle, failure.page_number - 1)
    except PDFCorruptError as exc:
        return _error_page(failure.page_number, None, _unsized(failed, exc))
    if ocr == "never":
        never = f"{failed}; ocr='never', so the page is not read by OCR"
        return _error_page(failure.page_number, geometry, _Unread(never))
    deadline = time.monotonic() + options.page_budget_s
    drawn = _drawn(source, geometry, password, options, deadline)
    if drawn.page is None:
        return _error_page(failure.page_number, geometry, _Unread(f"{failed}; {drawn.failure}"))
    reading = _read(drawn.page, drawn.dpi, geometry, options, deadline)
    where = _Where(source, password, identity, sha256)
    image = _page_image(drawn.page, drawn.dpi, reading.rotation, geometry, where, options)
    if reading.failure:
        why = f"{failed}; OCR could not read the page: {reading.failure}"
        return _error_page(failure.page_number, geometry, _Unread(why, reading.notes, image))
    laid = vision_elements(reading.words, geometry=geometry, rotation=reading.rotation)
    quality = _ocr_quality(reading.words, geometry)
    notes = (*reading.notes, *_low_confidence(quality.ocr_mean_confidence))
    return Page(
        page_number=geometry.page_number,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        status="ocr_fallback",
        route="vision",
        quality=quality,
        elements=laid.elements,
        page_image=image,
        warnings=(f"{failed}; {_NOT_READ}", *notes, *laid.warnings),
    )


def _check_fallback(handle: object, failure: object, ocr: object, options: object) -> None:
    """Require an open document's handle, a PageFailure, an OCR mode and VisionOptions."""
    if not isinstance(handle, DocumentHandle):
        raise TypeError(f"handle must be a DocumentHandle, got {type(handle).__name__}")
    if not isinstance(failure, PageFailure):
        raise TypeError(f"failure must be a PageFailure, got {type(failure).__name__}")
    choice_str("ocr", ocr, _OCR_MODES)
    if not isinstance(options, VisionOptions):
        raise TypeError(f"options must be a VisionOptions, got {type(options).__name__}")


class _Unread(NamedTuple):
    """Why a failed page was not read, its warnings, and its image if it was rendered."""

    error: str
    warnings: tuple[str, ...] = ()
    image: PageImage | None = None


def _unsized(failed: str, exc: PDFCorruptError) -> _Unread:
    """Why a failed page whose geometry cannot be read either is not read, and a warning that its size is a default."""
    size = f"the page's size could not be read ({type(exc).__name__}: {exc})"
    width, height = _DEFAULT_PAGE_SIZE
    default = f"{width:g} x {height:g} pt is pdfium's default page size, not this page's"
    return _Unread(f"{failed}; its geometry cannot be read either: {exc}", (f"{size}; {default}",))


def _error_page(page_number: int, geometry: PageGeometry | None, unread: _Unread) -> Page:
    """The A1 error page: no elements, no quality, routed vision; its size the page's, or pdfium's default."""
    width, height = (geometry.width_pt, geometry.height_pt) if geometry is not None else _DEFAULT_PAGE_SIZE
    return Page(
        page_number=page_number,
        width_pt=width,
        height_pt=height,
        status="error",
        route="vision",
        quality=None,
        page_image=unread.image,
        error=unread.error,
        warnings=unread.warnings,
    )


def _ocr_quality(words: tuple[OCRWord, ...], geometry: PageGeometry) -> PageQuality:
    """A fallback page's quality: its words' chars by A3's rule and their mean confidence; nothing else was read."""
    chars = [char for item in words for char in item.text]
    valid = sum(1 for char in chars if valid_char(char))
    return PageQuality(
        valid_char_ratio=valid / len(chars) if chars else None,
        char_count=len(chars),
        vector_line_count=0,
        hidden_spans_flagged=0,
        rotation_deg=geometry.rotation_deg,
        deskew_angle_deg=None,
        ocr_mean_confidence=_mean_confidence(words),
    )
