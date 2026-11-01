"""The v5 document JSON codec (#371, A7a): a PDFDocument as JSON and back, losslessly.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. The bundle
(:mod:`datagrunt.pdf.bundle`) and A7b's ``write_json`` stand on it.

:func:`document_to_json` writes every A1 type: metadata, pages, quality,
every element kind, page images, sections and header context. Each object
holds exactly its type's data fields; an element adds its ``kind`` (the
frame's, :data:`~datagrunt.pdf.frame.ELEMENT_KINDS`). A section's heading
and elements, and the header context, are element ids: A1 requires each to
equal the document's element with its id, so the reference loses nothing.
The top level names the ``"format"`` (:data:`FORMAT`) and its ``"version"``
(:data:`VERSION`).

Bytes are not in the JSON. A figure keeps its ``sha256``, its pixel size
and its ``image_path``; a page image its fields and paths. Paths are written
with ``/`` (``as_posix``). Loaders are not data: a decoded figure or page
image reads its bytes from its paths only.

:func:`document_from_json` is strict:

- an unknown format or version is refused;
- every object must hold exactly its keys: an unknown key or a missing one
  is refused;
- containers are checked, never coerced (a string is not split into a tuple);
- values are checked by A1's own validation, as the types are built.

With ``base_dir``, each path is joined to it by :func:`contained_path`,
which refuses a path that is not plain relative names or that resolves
out of ``base_dir``.

Decoding is iterative: it walks the schema's fixed depth (document, pages,
elements, fields) with loops. :func:`document_from_json_bytes` reads at most
:data:`_MAX_JSON_BYTES` (A1's 1 GiB asset limit) of UTF-8; the parser's own
recursion is bounded by the interpreter, and a value nested past it is
refused, as are ``NaN``, ``Infinity`` and a key repeated in an object.

:func:`document_to_json_bytes` writes ASCII (``ensure_ascii``): a lone
surrogate, which PDF text keeps (preserve, not transform) and UTF-8 cannot
encode, is written as its ``\\u`` escape and read back exactly.

Errors: ``TypeError`` for a value of the wrong type, ``ValueError`` for a
wrong value, A1's or the codec's, each naming where in the document it is.
"""

import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

from datagrunt.pdf.elements import (
    _MAX_ASSET_BYTES,
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
)
from datagrunt.pdf.frame import ELEMENT_KINDS, KINDS

__all__ = [
    "FORMAT",
    "VERSION",
    "document_from_json",
    "document_from_json_bytes",
    "document_to_json",
    "document_to_json_bytes",
    "contained_path",
    "parse_json",
]

FORMAT = "datagrunt.pdf.document"
VERSION = 1

# The most JSON bytes read: A1's asset limit.
_MAX_JSON_BYTES = _MAX_ASSET_BYTES

# A path part may not hold these (or a control character): ":" is a drive or an NTFS alternate data stream.
_FORBIDDEN_CHARS = frozenset(":\\")
# Device names Windows reserves in every directory, whatever the case and with or without an extension.
_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL", *(f"COM{n}" for n in range(1, 10)), *(f"LPT{n}" for n in range(1, 10))}
)

_DOCUMENT_KEYS = ("format", "version", "metadata", "pages", "sections", "header_context")
_METADATA_KEYS = ("doc_id", "source_name", "sha256", "page_count", "title", "author")
_PAGE_KEYS = (
    "page_number",
    "width_pt",
    "height_pt",
    "status",
    "route",
    "quality",
    "elements",
    "page_image",
    "integrity_verified",
    "error",
    "warnings",
)
_QUALITY_KEYS = (
    "valid_char_ratio",
    "char_count",
    "vector_line_count",
    "hidden_spans_flagged",
    "rotation_deg",
    "deskew_angle_deg",
    "ocr_mean_confidence",
)
_PAGE_IMAGE_KEYS = ("page_number", "dpi", "width_px", "height_px", "image_path", "band_paths", "contrast_normalized")
_BBOX_KEYS = ("x0", "y0", "x1", "y1")
_ELEMENT_KEYS = ("kind", "element_id", "page_number", "reading_order", "bbox", "scope_path", "hidden_reasons")
_SECTION_KEYS = ("heading", "scope_path", "page_span", "elements")

# Each kind's own keys, after the shared _ELEMENT_KEYS.
_KIND_KEYS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "heading": ("level", "text"),
        "paragraph": ("text",),
        "key_value": ("key", "value"),
        "table": ("headers", "rows", "extraction_method", "continued_from_id", "column_centres"),
        "list": ("items", "ordered", "markers"),
        "figure": ("sha256", "width_px", "height_px", "image_path", "caption"),
    }
)

Json = dict[str, Any]
_T = TypeVar("_T")


def document_to_json(document: PDFDocument) -> Json:
    """``document`` as a JSON value (dicts, lists, str, int, float, bool, None); see the module notes.

    Raises:
        TypeError: If ``document`` is not a PDFDocument.
    """
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    return {
        "format": FORMAT,
        "version": VERSION,
        "metadata": {key: getattr(document.metadata, key) for key in _METADATA_KEYS},
        "pages": [_page_json(page) for page in document.pages],
        "sections": [_section_json(section) for section in document.sections],
        "header_context": [item.element_id for item in document.header_context],
    }


def document_to_json_bytes(document: PDFDocument) -> bytes:
    """:func:`document_to_json` as compact ASCII JSON (lone surrogates escaped; no NaN).

    Raises:
        TypeError: If ``document`` is not a PDFDocument.
    """
    text = json.dumps(document_to_json(document), ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    return text.encode("ascii")


def _page_json(page: Page) -> Json:
    quality = page.quality
    return {
        "page_number": page.page_number,
        "width_pt": page.width_pt,
        "height_pt": page.height_pt,
        "status": page.status,
        "route": page.route,
        "quality": None if quality is None else {key: getattr(quality, key) for key in _QUALITY_KEYS},
        "elements": [_element_json(element) for element in page.elements],
        "page_image": _page_image_json(page.page_image),
        "integrity_verified": page.integrity_verified,
        "error": page.error,
        "warnings": list(page.warnings),
    }


def _page_image_json(image: PageImage | None) -> Json | None:
    if image is None:
        return None
    return {
        "page_number": image.page_number,
        "dpi": image.dpi,
        "width_px": image.width_px,
        "height_px": image.height_px,
        "image_path": _plain(image.image_path),
        "band_paths": [path.as_posix() for path in image.band_paths],
        "contrast_normalized": image.contrast_normalized,
    }


def _element_json(element: DocumentElement) -> Json:
    kind = ELEMENT_KINDS[type(element)]
    box = element.bbox
    data: Json = {
        "kind": kind,
        "element_id": element.element_id,
        "page_number": element.page_number,
        "reading_order": element.reading_order,
        "bbox": {key: getattr(box, key) for key in _BBOX_KEYS},
        "scope_path": list(element.scope_path),
        "hidden_reasons": list(element.hidden_reasons),
    }
    for key in _KIND_KEYS[kind]:
        data[key] = _plain(getattr(element, key))
    return data


def _plain(value: object) -> object:
    """A field's JSON value: a path as a ``/`` string, a tuple (of tuples: a table's rows) as a list."""
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, tuple):
        return [list(item) if isinstance(item, tuple) else item for item in value]
    return value


def _section_json(section: Section) -> Json:
    heading = section.heading
    return {
        "heading": None if heading is None else heading.element_id,
        "scope_path": list(section.scope_path),
        "page_span": list(section.page_span),
        "elements": [element.element_id for element in section.elements],
    }


def document_from_json_bytes(raw: bytes, *, base_dir: str | os.PathLike[str] | None = None) -> PDFDocument:
    """The document in ``raw``, UTF-8 JSON of at most :data:`_MAX_JSON_BYTES` (see the module notes).

    Raises:
        TypeError: If ``raw`` is not bytes, or as :func:`document_from_json` raises it.
        ValueError: If ``raw`` is over the bound (refused unparsed), not
            UTF-8 JSON (see :func:`parse_json`), or as
            :func:`document_from_json` raises it.
    """
    if not isinstance(raw, bytes):
        raise TypeError(f"raw must be bytes, got {type(raw).__name__}")
    if len(raw) > _MAX_JSON_BYTES:
        raise ValueError(f"document JSON is {len(raw)} bytes, over the {_MAX_JSON_BYTES}-byte bound")
    return document_from_json(parse_json(raw, "document JSON"), base_dir=base_dir)


def parse_json(raw: bytes, what: str) -> object:
    """``raw`` parsed as strict UTF-8 JSON; ``what`` names it in errors.

    Raises:
        ValueError: If ``raw`` is not UTF-8, not JSON, holds ``NaN`` or an
            infinity, repeats a key in an object, or nests past the parser's
            recursion bound.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{what} is not valid JSON: it is not UTF-8 ({exc})") from exc
    try:
        return json.loads(text, object_pairs_hook=_unique_keys, parse_constant=_no_constant)
    except RecursionError as exc:
        raise ValueError(f"{what} is nested too deeply to parse") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{what} is not valid JSON: {exc}") from exc


def _unique_keys(pairs: list[tuple[str, Any]]) -> Json:
    """A JSON object's dict, refusing a key it repeats (``json`` would keep the last silently)."""
    data: Json = {}
    for key, value in pairs:
        if key in data:
            raise ValueError(f"JSON object has duplicate key {key!r}")
        data[key] = value
    return data


def _no_constant(name: str) -> object:
    raise ValueError(f"{name} is not JSON (no NaN or infinity is allowed)")


def contained_path(name: str, value: object, root: Path) -> Path:
    """``root`` joined with ``value``, a relative path whose every part is checked, if it stays in ``root``.

    The one check of every path a bundle names: the document JSON's (figure,
    page image and band paths, here) and the manifest's (the bundle loader).
    ``value`` is split on ``/`` only, and each part must be a plain name
    (:func:`_part_problem`): not empty, ``.`` or ``..``; no ``:`` (a drive,
    or an NTFS alternate data stream), ``\\``, NUL or other control
    character; no trailing ``.`` or space (Windows strips them); and not a
    device name Windows reserves (:data:`_RESERVED_NAMES`), with or without
    an extension. The joined path, its symlinks resolved, must then lie in
    ``root``, resolved: a symlink out of it is refused. Nothing is sanitized.

    Raises:
        TypeError: If ``value`` is not a str.
        ValueError: If a part is refused (naming the path and the part), or
            the joined path resolves outside ``root``.
        FileNotFoundError: If ``root`` does not exist.
    """
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    parts = value.split("/")
    for part in parts:
        problem = _part_problem(part)
        if problem:
            raise ValueError(f"{name} {value!r}: part {part!r} {problem}")
    joined = root.joinpath(*parts)
    base = root.resolve(strict=True)
    if not joined.resolve(strict=False).is_relative_to(base):
        raise ValueError(f"{name} {value!r} resolves outside {base}")
    return joined


def _part_problem(part: str) -> str:
    """Why ``part`` cannot be a name in a bundle path, or ``""`` if it can."""
    if part in ("", ".", ".."):
        return "is empty, '.' or '..'"
    if any(char in _FORBIDDEN_CHARS or ord(char) < 32 for char in part):
        return "holds ':', '\\', NUL or a control character"
    if part.endswith((".", " ")):
        return "ends in '.' or a space, which Windows strips"
    if part.split(".", 1)[0].rstrip(" ").upper() in _RESERVED_NAMES:
        return "is a device name Windows reserves"
    return ""


def document_from_json(data: object, *, base_dir: str | os.PathLike[str] | None = None) -> PDFDocument:
    """The document :func:`document_to_json` wrote as ``data`` (see the module notes); ``data`` is not changed.

    Raises:
        TypeError, ValueError: If ``data`` is not such a document, naming where.
    """
    root = _checked_base_dir(base_dir)
    top = _object("document", data, _DOCUMENT_KEYS)
    _check_format(top)
    metadata = _located("metadata", _metadata, top["metadata"])
    pages = tuple(_located(f"pages[{i}]", _page, value, root) for i, value in enumerate(_array("pages", top["pages"])))
    by_id = {element.element_id: element for page in pages for element in page.elements}
    raw_sections = enumerate(_array("sections", top["sections"]))
    sections = tuple(_located(f"sections[{i}]", _section, value, by_id) for i, value in raw_sections)
    raw_header = enumerate(_array("header_context", top["header_context"]))
    header_context = tuple(_reference(f"header_context[{i}]", value, by_id) for i, value in raw_header)
    return _located("document", _document, metadata, pages, sections, header_context)


def _located(name: str, build: Callable[..., _T], *args: Any) -> _T:
    """``build(name, *args)``, naming ``name`` in a TypeError or ValueError raised inside it.

    A1's messages name only the field; an error that already starts with
    ``name`` (the codec's own, or one located deeper) is raised as it is.
    """
    try:
        return build(name, *args)
    except (TypeError, ValueError) as exc:
        if str(exc).startswith(name):
            raise
        kind = TypeError if isinstance(exc, TypeError) else ValueError
        raise kind(f"{name}: {exc}") from exc


def _metadata(name: str, value: object) -> DocumentMetadata:
    return DocumentMetadata(**_object(name, value, _METADATA_KEYS))


def _document(
    name: str,
    metadata: DocumentMetadata,
    pages: tuple[Page, ...],
    sections: tuple[Section, ...],
    header_context: tuple[KeyValue, ...],
) -> PDFDocument:
    return PDFDocument(metadata=metadata, pages=pages, sections=sections, header_context=header_context)


def _checked_base_dir(base_dir: object) -> Path | None:
    if base_dir is None:
        return None
    if not isinstance(base_dir, (str, os.PathLike)):
        raise TypeError(f"base_dir must be a str, os.PathLike or None, got {type(base_dir).__name__}")
    return Path(base_dir)


def _check_format(top: Json) -> None:
    """Refuse a JSON value that is not this format, or of a version this codec does not read."""
    if top["format"] != FORMAT:
        raise ValueError(f"format must be {FORMAT!r}, got {top['format']!r}")
    version = top["version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise TypeError(f"version must be an int, got {version!r} ({type(version).__name__})")
    if version != VERSION:
        raise ValueError(f"version {version} is not supported: this datagrunt reads version {VERSION}")


def _object(name: str, value: object, keys: tuple[str, ...]) -> Json:
    """``value`` if it is a JSON object holding exactly ``keys``."""
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a JSON object, got {type(value).__name__}")
    unknown = sorted((key for key in value if key not in keys), key=str)
    if unknown:
        raise ValueError(f"{name} has unknown key(s) {unknown}")
    missing = [key for key in keys if key not in value]
    if missing:
        raise ValueError(f"{name} has missing key(s) {missing}")
    return value


def _optional_object(name: str, value: object, keys: tuple[str, ...]) -> Json | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a JSON object or null, got {type(value).__name__}")
    return _object(name, value, keys)


def _array(name: str, value: object) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a JSON array, got {type(value).__name__}")
    return value


def _tuple(name: str, value: object) -> tuple[Any, ...]:
    return tuple(_array(name, value))


def _rows(name: str, value: object) -> tuple[tuple[Any, ...], ...]:
    return tuple(_tuple(f"{name}[{i}]", row) for i, row in enumerate(_array(name, value)))


def _path(name: str, value: object, root: Path | None) -> Path | None:
    """A JSON path or null: None, or :func:`_band_path`'s path."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str or null, got {type(value).__name__}")
    return _band_path(name, value, root)


def _band_path(name: str, value: object, root: Path | None) -> Path:
    """A JSON path: as written (no ``root``), or :func:`contained_path` in ``root``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if root is None:
        return Path(value)
    return contained_path(name, value, root)


def _page(name: str, value: object, root: Path | None) -> Page:
    data = _object(name, value, _PAGE_KEYS)
    raw_elements = enumerate(_array(f"{name}.elements", data["elements"]))
    elements = tuple(_located(f"{name}.elements[{i}]", _element, item, root) for i, item in raw_elements)
    quality = _optional_object(f"{name}.quality", data["quality"], _QUALITY_KEYS)
    return Page(
        page_number=data["page_number"],
        width_pt=data["width_pt"],
        height_pt=data["height_pt"],
        status=data["status"],
        route=data["route"],
        quality=None if quality is None else PageQuality(**quality),
        elements=elements,
        page_image=_page_image(f"{name}.page_image", data["page_image"], root),
        integrity_verified=data["integrity_verified"],
        error=data["error"],
        warnings=_tuple(f"{name}.warnings", data["warnings"]),
    )


def _page_image(name: str, value: object, root: Path | None) -> PageImage | None:
    data = _optional_object(name, value, _PAGE_IMAGE_KEYS)
    if data is None:
        return None
    bands = enumerate(_array(f"{name}.band_paths", data["band_paths"]))
    return PageImage(
        page_number=data["page_number"],
        dpi=data["dpi"],
        width_px=data["width_px"],
        height_px=data["height_px"],
        image_path=_path(f"{name}.image_path", data["image_path"], root),
        band_paths=tuple(_band_path(f"{name}.band_paths[{i}]", path, root) for i, path in bands),
        contrast_normalized=data["contrast_normalized"],
    )


def _element(name: str, value: object, root: Path | None) -> DocumentElement:
    """The element in ``value``: its kind picks its keys and its type."""
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a JSON object, got {type(value).__name__}")
    if "kind" not in value:
        raise ValueError(f"{name} has missing key(s) ['kind']")
    kind = value["kind"]
    if not isinstance(kind, str) or kind not in KINDS:
        raise ValueError(f"{name}.kind must be one of {KINDS}, got {kind!r}")
    data = _object(name, value, _ELEMENT_KEYS + _KIND_KEYS[kind])
    shared = {
        "element_id": data["element_id"],
        "page_number": data["page_number"],
        "reading_order": data["reading_order"],
        "bbox": BBox(**_object(f"{name}.bbox", data["bbox"], _BBOX_KEYS)),
        "scope_path": _tuple(f"{name}.scope_path", data["scope_path"]),
        "hidden_reasons": _tuple(f"{name}.hidden_reasons", data["hidden_reasons"]),
    }
    return _BUILDERS[kind](name, data, shared, root)


def _heading(name: str, data: Json, shared: Json, root: Path | None) -> Heading:
    return Heading(**shared, level=data["level"], text=data["text"])


def _paragraph(name: str, data: Json, shared: Json, root: Path | None) -> Paragraph:
    return Paragraph(**shared, text=data["text"])


def _key_value(name: str, data: Json, shared: Json, root: Path | None) -> KeyValue:
    return KeyValue(**shared, key=data["key"], value=data["value"])


def _table(name: str, data: Json, shared: Json, root: Path | None) -> Table:
    return Table(
        **shared,
        headers=_tuple(f"{name}.headers", data["headers"]),
        rows=_rows(f"{name}.rows", data["rows"]),
        extraction_method=data["extraction_method"],
        continued_from_id=data["continued_from_id"],
        column_centres=_tuple(f"{name}.column_centres", data["column_centres"]),
    )


def _bullet_list(name: str, data: Json, shared: Json, root: Path | None) -> BulletList:
    return BulletList(
        **shared,
        items=_tuple(f"{name}.items", data["items"]),
        ordered=data["ordered"],
        markers=_tuple(f"{name}.markers", data["markers"]),
    )


def _figure(name: str, data: Json, shared: Json, root: Path | None) -> Figure:
    return Figure(
        **shared,
        sha256=data["sha256"],
        width_px=data["width_px"],
        height_px=data["height_px"],
        image_path=_path(f"{name}.image_path", data["image_path"], root),
        caption=data["caption"],
    )


_BUILDERS: Mapping[str, Callable[[str, Json, Json, Path | None], DocumentElement]] = MappingProxyType(
    {
        "heading": _heading,
        "paragraph": _paragraph,
        "key_value": _key_value,
        "table": _table,
        "list": _bullet_list,
        "figure": _figure,
    }
)


def _section(name: str, value: object, by_id: Mapping[str, DocumentElement]) -> Section:
    data = _object(name, value, _SECTION_KEYS)
    heading = None if data["heading"] is None else _reference(f"{name}.heading", data["heading"], by_id)
    members = enumerate(_array(f"{name}.elements", data["elements"]))
    return Section(
        heading=heading,
        scope_path=_tuple(f"{name}.scope_path", data["scope_path"]),
        page_span=_tuple(f"{name}.page_span", data["page_span"]),
        elements=tuple(_reference(f"{name}.elements[{i}]", item, by_id) for i, item in members),
    )


def _reference(name: str, value: object, by_id: Mapping[str, DocumentElement]) -> Any:
    """The document's element whose id ``value`` is; A1 checks it is of the type its place needs."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an element id (a str), got {type(value).__name__}")
    element = by_id.get(value)
    if element is None:
        raise ValueError(f"{name}: {value!r} names no element of the document")
    return element
