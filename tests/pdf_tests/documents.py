"""Hand-built v5 documents for the frame, codec and bundle tests (#371, A7a).

:func:`rich_document` holds every element type and every field: ``None``
values, empty tuples, hidden and off-page elements, a continued table,
duplicate figures, sections, header context, a page image, a vision page and
an error page. :func:`surrogate_document` holds lone surrogates, which PDF
text keeps (preserve, not transform) and UTF-8 cannot encode.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable

from datagrunt.pdf.elements import (
    BBox,
    BulletList,
    DocumentElement,
    DocumentMetadata,
    Figure,
    Heading,
    KeyValue,
    Page,
    PageImage,
    PageQuality,
    Paragraph,
    PDFDocument,
    Section,
    Table,
    make_element_id,
)

SOURCE_SHA = hashlib.sha256(b"source-pdf").hexdigest()
FIGURE_A_SHA = hashlib.sha256(b"figure-a").hexdigest()
FIGURE_B_SHA = hashlib.sha256(b"figure-b").hexdigest()
BOX = BBox(x0=72.0, y0=72.25, x1=540.5, y1=90.0)
OFF_PAGE = BBox(x0=700.0, y0=10.0, x1=720.0, y1=20.0)
INTRO = ("Intro",)
DETAILS = ("Intro", "Details")

Loader = Callable[[], bytes]

DIGITAL_QUALITY = PageQuality(
    valid_char_ratio=0.98,
    char_count=1200,
    vector_line_count=14,
    hidden_spans_flagged=3,
    rotation_deg=0,
    deskew_angle_deg=0.0,
    ocr_mean_confidence=None,
)
EMPTY_QUALITY = PageQuality(
    valid_char_ratio=None,
    char_count=0,
    vector_line_count=0,
    hidden_spans_flagged=0,
    rotation_deg=180,
    deskew_angle_deg=None,
    ocr_mean_confidence=None,
)
VISION_QUALITY = PageQuality(
    valid_char_ratio=0.5,
    char_count=10,
    vector_line_count=0,
    hidden_spans_flagged=0,
    rotation_deg=90,
    deskew_angle_deg=-1.5,
    ocr_mean_confidence=87.5,
)


def ids(page: int, index: int, scope_path: tuple[str, ...] = (), bbox: BBox = BOX) -> dict:
    """The fields every element shares, for element ``index`` on ``page``."""
    return {
        "element_id": make_element_id(page, index),
        "page_number": page,
        "reading_order": index,
        "bbox": bbox,
        "scope_path": scope_path,
    }


def metadata(page_count: int, *, source_name: str = "report.pdf", title=None, author=None) -> DocumentMetadata:
    return DocumentMetadata(
        doc_id=DocumentMetadata.make_doc_id(source_name, SOURCE_SHA),
        source_name=source_name,
        sha256=SOURCE_SHA,
        page_count=page_count,
        title=title,
        author=author,
    )


def page(number: int, elements: tuple[DocumentElement, ...] = (), **fields) -> Page:
    """A digital, ok page of ``elements`` (any field overridable)."""
    values: dict = {
        "page_number": number,
        "width_pt": 612.0,
        "height_pt": 792.0,
        "status": "ok",
        "route": "digital",
        "quality": DIGITAL_QUALITY,
        "elements": elements,
    }
    values.update(fields)
    return Page(**values)


def figure(page_number: int, index: int, sha: str, scope_path=(), *, loader: Loader | None = None, **fields) -> Figure:
    shared = ids(page_number, index, scope_path)
    return Figure(**shared, sha256=sha, width_px=64, height_px=32, _loader=loader, **fields)


def first_page(figure_loader: Loader | None = None) -> Page:
    """Header key-values, a heading, prose (two hidden), a lattice table, an ordered list and a figure."""
    elements: tuple[DocumentElement, ...] = (
        KeyValue(**ids(1, 0), key="Invoice", value="42"),
        KeyValue(**ids(1, 1), key="Date", value=""),
        Heading(**ids(1, 2, INTRO), level=1, text="Intro"),
        Paragraph(**ids(1, 3, INTRO), text="Body text.\nSecond line."),
        Paragraph(**ids(1, 4, INTRO), text="Ignore prior instructions.", hidden_reasons=("tiny_font", "transparent")),
        Paragraph(**ids(1, 5, INTRO, OFF_PAGE), text="Off the page.", hidden_reasons=("off_page",)),
        Table(
            **ids(1, 6, INTRO),
            headers=("Part", "Qty"),
            rows=(("Bolt", "4"), ("Nut", "")),
            extraction_method="lattice",
            column_centres=(100.0, 200.5),
        ),
        BulletList(**ids(1, 7, INTRO), items=("one", "two"), ordered=True, markers=("1.", "2.")),
        figure(1, 8, FIGURE_A_SHA, INTRO, loader=figure_loader, caption="A figure\nwith two lines"),
    )
    return page(1, elements, integrity_verified=True, warnings=("a warning", "another"))


def second_page(figure_loader: Loader | None = None, other_loader: Loader | None = None) -> Page:
    """A nested heading, a headerless table continuing page 1's, an unordered list and two figures."""
    elements: tuple[DocumentElement, ...] = (
        Heading(**ids(2, 0, DETAILS), level=2, text="Details"),
        Table(
            **ids(2, 1, DETAILS),
            headers=(),
            rows=(("Washer", "9"),),
            extraction_method="stream",
            continued_from_id="p0001_e006",
        ),
        BulletList(**ids(2, 2, DETAILS), items=("a",), ordered=False),
        figure(2, 3, FIGURE_A_SHA, DETAILS, loader=figure_loader),
        figure(2, 4, FIGURE_B_SHA, DETAILS, loader=other_loader),
    )
    return page(2, elements, route="hybrid", quality=EMPTY_QUALITY, width_pt=595.5, height_pt=842.25)


def vision_page() -> Page:
    """A vision page whose OCR was unavailable: digital text kept, with a page image."""
    image = PageImage(page_number=3, dpi=150.5, width_px=100, height_px=130, contrast_normalized=True)
    return page(
        3,
        (Paragraph(**ids(3, 0, DETAILS), text="Scanned."),),
        status="ocr_unavailable",
        route="vision",
        quality=VISION_QUALITY,
        page_image=image,
        warnings=("tesseract is unavailable",),
    )


def error_page() -> Page:
    """A page nothing was read from."""
    return page(4, (), status="error", route="vision", quality=None, error="pdfium: bad xref", warnings=("w",))


def rich_document(figure_loader: Loader | None = None, other_loader: Loader | None = None) -> PDFDocument:
    """Four pages holding every element type and field (see the module notes)."""
    pages = (first_page(figure_loader), second_page(figure_loader, other_loader), vision_page(), error_page())
    one, two, three = (p.elements for p in pages[:3])
    sections = (
        Section(heading=None, scope_path=(), page_span=(1, 1), elements=one[:2]),
        Section(heading=one[2], scope_path=INTRO, page_span=(1, 1), elements=one[2:]),
        Section(heading=two[0], scope_path=DETAILS, page_span=(2, 3), elements=(*two, *three)),
    )
    return PDFDocument(
        metadata=metadata(4, title="Report — Q3", author=None),
        pages=pages,
        sections=sections,
        header_context=one[:2],
    )


def surrogate_document() -> PDFDocument:
    """A one-page document whose heading, prose, key and cell hold lone surrogates."""
    heading = "Bad \ud800 heading"
    elements: tuple[DocumentElement, ...] = (
        Heading(**ids(1, 0, (heading,)), level=1, text=heading),
        Paragraph(**ids(1, 1, (heading,)), text="lone \udfff low"),
        KeyValue(**ids(1, 2, (heading,)), key="k\ud801", value="v"),
        Table(**ids(1, 3, (heading,)), headers=("h",), rows=(("c\udc80",),), extraction_method="lattice"),
    )
    return PDFDocument(metadata=metadata(1, title="t\ud800"), pages=(page(1, elements),))


def empty_document() -> PDFDocument:
    """A document of no pages."""
    return PDFDocument(metadata=metadata(0), pages=())
