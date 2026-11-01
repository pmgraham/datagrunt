"""The element frame (#371, A7a): one row per element of a PDFDocument.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A7b's
``PDFWriter`` (Parquet, CSV) and A8b's ``PDFReader`` (DataFrame, Arrow,
DuckDB) stand on it.

:func:`element_frame` is the spec's unified schema (:data:`FRAME_SCHEMA`),
in document order (page, then reading order), plus ``hidden_reasons``, a v5
column the spec's frame lacks: hidden text is flagged, never dropped, so a
frame consumer filters it out by this column (an empty list is visible).

Each column's source:

- ``doc_id``: the document's ``metadata.doc_id``.
- ``element_id``, ``page_number``, ``reading_order``, ``scope_path``,
  ``bbox``, ``hidden_reasons``: the element's own fields.
- ``kind``: the element's type (``BulletList`` is ``"list"``).
- ``text``: :func:`element_text`.
- ``markdown``: the element's ``to_markdown()``.
- ``route``, ``integrity_verified``: the element's page's.

A lone surrogate, which PDF text keeps (preserve, not transform), cannot be
held by a polars or Arrow string (both are UTF-8). Every string the frame
holds goes through :func:`utf8_text`, which writes each lone surrogate as
its Python escape (``\\ud800``): the frame is a view of the document, and
the document (and its JSON) keeps the exact text.

:func:`element_arrow` is the frame as a ``pyarrow.Table`` of the fixed
:data:`ARROW_SCHEMA`: polars' Arrow types for strings and enums vary by
version, and callers need one stable type.
"""

import polars as pl
import pyarrow as pa

from datagrunt.pdf.elements import (
    PAGE_ROUTES,
    BulletList,
    DocumentElement,
    Figure,
    Heading,
    KeyValue,
    Page,
    Paragraph,
    PDFDocument,
    Table,
)

__all__ = ["ARROW_SCHEMA", "FRAME_SCHEMA", "KINDS", "element_arrow", "element_frame", "element_text", "utf8_text"]

KINDS = ("heading", "paragraph", "key_value", "table", "list", "figure")

# Each element type's kind (DocumentElement's six concrete types).
_KIND_OF: dict[type, str] = {
    Heading: "heading",
    Paragraph: "paragraph",
    KeyValue: "key_value",
    Table: "table",
    BulletList: "list",
    Figure: "figure",
}

_BBOX_FIELDS = ("x0", "y0", "x1", "y1")

_COLUMNS: dict[str, pl.DataType] = {
    "doc_id": pl.String(),
    "element_id": pl.String(),
    "page_number": pl.Int32(),
    "reading_order": pl.Int32(),
    "kind": pl.Enum(list(KINDS)),
    "scope_path": pl.List(pl.String),
    "text": pl.String(),
    "markdown": pl.String(),
    "bbox": pl.Struct({name: pl.Float64 for name in _BBOX_FIELDS}),
    "route": pl.Enum(list(PAGE_ROUTES)),
    "integrity_verified": pl.Boolean(),
    "hidden_reasons": pl.List(pl.String),
}
FRAME_SCHEMA = pl.Schema(_COLUMNS)

_ENUM = pa.dictionary(pa.int32(), pa.large_string())

ARROW_SCHEMA = pa.schema(
    [
        ("doc_id", pa.large_string()),
        ("element_id", pa.large_string()),
        ("page_number", pa.int32()),
        ("reading_order", pa.int32()),
        ("kind", _ENUM),
        ("scope_path", pa.large_list(pa.large_string())),
        ("text", pa.large_string()),
        ("markdown", pa.large_string()),
        ("bbox", pa.struct([(name, pa.float64()) for name in _BBOX_FIELDS])),
        ("route", _ENUM),
        ("integrity_verified", pa.bool_()),
        ("hidden_reasons", pa.large_list(pa.large_string())),
    ]
)


def utf8_text(text: str) -> str:
    """``text``, with each lone surrogate written as its Python escape (``\\ud800``) so it encodes as UTF-8.

    Text without one (all but a rare PDF's) is returned as it is.
    """
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return text.encode("utf-8", "backslashreplace").decode("utf-8")
    return text


def element_text(element: DocumentElement) -> str:
    """The element's plain text (see the module notes): its words, without Markdown.

    - Heading, Paragraph: its text.
    - KeyValue: ``"key: value"``.
    - Table: its header row (when it has one), then each row; cells joined
      by tabs, rows by newlines.
    - BulletList: its items, one a line.
    - Figure: its caption, or ``""``.

    Raises:
        TypeError: If ``element`` is not one of the six element types.
    """
    if isinstance(element, (Heading, Paragraph)):
        return element.text
    if isinstance(element, KeyValue):
        return f"{element.key}: {element.value}"
    if isinstance(element, Table):
        rows = (element.headers, *element.rows) if element.headers else element.rows
        return "\n".join("\t".join(row) for row in rows)
    if isinstance(element, BulletList):
        return "\n".join(element.items)
    if isinstance(element, Figure):
        return element.caption or ""
    raise TypeError(f"element must be a document element, got {type(element).__name__}")


def element_frame(document: PDFDocument) -> pl.DataFrame:
    """One row per element of ``document``, in document order, of :data:`FRAME_SCHEMA` (see the module notes).

    A document without elements gives an empty frame of the schema.

    Raises:
        TypeError: If ``document`` is not a PDFDocument.
    """
    _check_document(document)
    columns: dict[str, list[object]] = {name: [] for name in FRAME_SCHEMA}
    pages = document.pages
    for element in document.elements:
        _append_row(columns, element, pages[element.page_number - 1])
    columns["doc_id"] = [document.metadata.doc_id] * len(document.elements)
    return pl.DataFrame(columns, schema=FRAME_SCHEMA)


def element_arrow(document: PDFDocument) -> pa.Table:
    """:func:`element_frame` as a ``pyarrow.Table`` of :data:`ARROW_SCHEMA`.

    Raises:
        TypeError: If ``document`` is not a PDFDocument.
    """
    return element_frame(document).to_arrow().cast(ARROW_SCHEMA)


def _check_document(document: object) -> None:
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")


def _append_row(columns: dict[str, list[object]], element: DocumentElement, page: Page) -> None:
    """Append ``element``'s values (all but ``doc_id``) to ``columns``; ``page`` is the page it is on."""
    box = element.bbox
    columns["element_id"].append(element.element_id)
    columns["page_number"].append(element.page_number)
    columns["reading_order"].append(element.reading_order)
    columns["kind"].append(_KIND_OF[type(element)])
    columns["scope_path"].append([utf8_text(entry) for entry in element.scope_path])
    columns["text"].append(utf8_text(element_text(element)))
    columns["markdown"].append(utf8_text(element.to_markdown()))
    columns["bbox"].append({name: getattr(box, name) for name in _BBOX_FIELDS})
    columns["route"].append(page.route)
    columns["integrity_verified"].append(page.integrity_verified)
    columns["hidden_reasons"].append(list(element.hidden_reasons))
