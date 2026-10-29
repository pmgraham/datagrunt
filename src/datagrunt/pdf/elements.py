"""Immutable element types for the v5 PDF document model.

Every class is a frozen, slotted, keyword-only dataclass that validates its
fields in ``__post_init__`` and rejects bad input at construction. Fields hold
extracted data verbatim (preserve, not transform); Markdown escaping happens
only when an element is rendered with ``to_markdown()``.

Coordinates are PDF points with a top-left origin.
"""

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, TypeAlias, Union, get_args

import polars as pl
import pyarrow as pa

from datagrunt.core.pdf_io.extraction.markdown_escape import (
    escape_leading_markdown,
    escape_markdown_link_target,
    escape_markdown_link_text,
)
from datagrunt.pdf._validate import finite_float, int_at_least, non_blank_str, optional_str, str_tuple
from datagrunt.pdf.exceptions import DatagruntPDFError

HiddenReason: TypeAlias = Literal["off_page", "invisible", "tiny_font", "transparent"]
"""Why a text span is hidden from a human reader (it is flagged, never dropped)."""

# Canonical order, derived from the Literal so the two can never drift apart.
HIDDEN_REASONS: tuple[HiddenReason, ...] = get_args(HiddenReason)

PageStatus: TypeAlias = Literal["ok", "ocr_fallback", "error"]
PageRoute: TypeAlias = Literal["digital", "hybrid", "vision"]
ExtractionMethod: TypeAlias = Literal["lattice", "stream"]
_EXTRACTION_METHODS: tuple[ExtractionMethod, ...] = get_args(ExtractionMethod)

# Element ids look like ``p0001_e003``. [0-9] rather than \d: \d also matches
# non-ASCII Unicode digits, which int() would happily parse.
_ELEMENT_ID = re.compile(r"p([0-9]{4,})_e([0-9]{3,})")

# A Markdown line ending: CommonMark treats a lone CR as one too. The group
# makes split() keep the separators; sub() is unaffected by it.
_LINE_BREAK = re.compile(r"(\r\n|\r|\n)")

# A trailing run of ``#`` preceded by a space/tab (or alone): CommonMark reads it
# as an ATX heading's optional closing sequence and drops it from the text.
_ATX_CLOSING_RUN = re.compile(r"(^|[ \t])(#+)([ \t]*)\Z")

# A GFM table cell's structural characters (pipe or line break) with the run
# of backslashes immediately before them.
_GFM_STRUCTURAL = re.compile(r"(\\*)(\||\r\n|\r|\n)")

# Upper bound on one asset's bytes (an image file or loader result): every
# read is bounded, so a corrupt or hostile file cannot exhaust memory.
_MAX_ASSET_BYTES = 1 << 30  # 1 GiB

# Page rotations a PDF can declare (``/Rotate`` is a multiple of 90).
_RIGHT_ANGLE_ROTATIONS = (0, 90, 180, 270)

# A SHA-256 hex digest exactly as hashlib's hexdigest() produces it. Matched
# with fullmatch() so a trailing newline cannot slip past a `$` anchor.
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")

# Either path separator, so a doc_id never depends on the OS that built it.
_PATH_SEPARATOR = re.compile(r"[\\/]")

# Runs of characters a doc_id stem may not contain. ASCII-only on purpose: the
# id must be safe as a file name and URL segment on every platform.
_DOC_ID_UNSAFE_RUN = re.compile(r"[^A-Za-z0-9-]+")


def _check_sha256(name: str, value: object) -> str:
    """Return ``value`` if it is a 64-char lowercase hex SHA-256 digest."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    if _SHA256_HEX.fullmatch(value) is None:
        raise ValueError(f"{name} must be 64 lowercase hex characters, got {value!r}")
    return value


def make_element_id(page_number: int, index: int) -> str:
    """Return the canonical element id for ``index`` on ``page_number``, e.g. ``p0001_e003``."""
    int_at_least("page_number", page_number, 1)
    int_at_least("index", index, 0)
    return f"p{page_number:04d}_e{index:03d}"


def _parse_element_id(name: str, value: object) -> int:
    """Validate a canonical element id and return the page number it encodes.

    Canonical means it round-trips through :func:`make_element_id`: extra zero
    padding (``p00001_e001``) is rejected so two spellings can never name the
    same element and slip past an id-uniqueness check.
    """
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    match = _ELEMENT_ID.fullmatch(value)
    if match is None:
        raise ValueError(f"{name} must look like 'p0001_e003', got {value!r}")
    page, index = int(match.group(1)), int(match.group(2))
    if page < 1:
        raise ValueError(f"{name} must encode a page number >= 1, got {value!r}")
    if value != make_element_id(page, index):
        raise ValueError(f"{name} must be canonical (no extra zero padding), got {value!r}")
    return page


def _check_hidden_reasons(value: object) -> None:
    """Require unique, known hidden reasons in ``HIDDEN_REASONS`` order."""
    reasons = str_tuple("hidden_reasons", value)
    unknown = [reason for reason in reasons if reason not in HIDDEN_REASONS]
    if unknown:
        raise ValueError(f"hidden_reasons has unknown reason(s) {unknown}; allowed: {HIDDEN_REASONS}")
    if len(set(reasons)) != len(reasons):
        raise ValueError(f"hidden_reasons has duplicate entries: {reasons}")
    canonical = tuple(reason for reason in HIDDEN_REASONS if reason in reasons)
    if reasons != canonical:
        raise ValueError(f"hidden_reasons must be in canonical order {canonical}, got {reasons}")


def _check_choice(name: str, value: object, choices: tuple[str, ...]) -> str:
    """Return ``value`` if it is one of the ``choices`` strings."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    if value not in choices:
        raise ValueError(f"{name} must be one of {choices}, got {value!r}")
    return value


def _free_suffix(base: str, taken: set[str]) -> str:
    """Return the first ``base_<k>`` (k >= 2) not in ``taken``."""
    # At most len(taken) candidates can be taken, so len(taken) + 1 distinct
    # candidates always include a free one: the loop is bounded and total.
    for k in range(2, len(taken) + 3):
        candidate = f"{base}_{k}"
        if candidate not in taken:
            return candidate
    raise RuntimeError(f"no free column-name suffix for {base!r}")  # unreachable by the bound above


def _gfm_cell(match: re.Match[str]) -> str:
    """Double the backslash run before a ``|`` or line break, then emit ``\\|`` or ``<br>``."""
    backslashes, char = match.group(1), match.group(2)
    return backslashes * 2 + ("\\|" if char == "|" else "<br>")


def _gfm_row(cells: tuple[str, ...]) -> str:
    """Render one GFM table row: ``|`` escaped, line breaks as ``<br>``.

    A backslash run right before either would otherwise escape the emitted
    ``\\|`` (re-opening the cell boundary) or the ``<`` of ``<br>``.
    """
    escaped = (_GFM_STRUCTURAL.sub(_gfm_cell, cell) for cell in cells)
    return "| " + " | ".join(escaped) + " |"


def _read_asset_file(path: Path) -> bytes:
    """Read an asset file, refusing anything over ``_MAX_ASSET_BYTES``.

    ``stat()`` rejects an oversized file before reading; the capped read keeps
    the bound even if the file grows between the two calls.
    """
    limit = _MAX_ASSET_BYTES
    size = path.stat().st_size
    if size > limit:
        raise DatagruntPDFError(f"{path} is {size} bytes, over the {limit}-byte asset limit")
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise DatagruntPDFError(f"{path} grew past the {limit}-byte asset limit while being read")
    return data


def _escape_lines(text: str) -> str:
    """Escape a leading Markdown metacharacter on every line, keeping line endings verbatim."""
    # Splitting on a capturing group keeps the separators at the odd indices.
    parts = _LINE_BREAK.split(text)
    return "".join(part if i % 2 else escape_leading_markdown(part) for i, part in enumerate(parts))


@dataclass(frozen=True, slots=True, kw_only=True)
class BBox:
    """An axis-aligned bounding box in PDF points, top-left origin.

    Coordinates must be finite with ``x0 <= x1`` and ``y0 <= y1``. There is no
    page-bounds check: an off-page span keeps its raw (possibly negative)
    coordinates so hidden content is flagged, never clipped.
    """

    x0: float
    y0: float
    x1: float
    y1: float

    def __post_init__(self) -> None:
        for name in ("x0", "y0", "x1", "y1"):
            object.__setattr__(self, name, finite_float(name, getattr(self, name)))
        if self.x0 > self.x1:
            raise ValueError(f"x0 must be <= x1, got x0={self.x0!r}, x1={self.x1!r}")
        if self.y0 > self.y1:
            raise ValueError(f"y0 must be <= y1, got y0={self.y0!r}, y1={self.y1!r}")

    @property
    def width(self) -> float:
        """Horizontal extent in points."""
        return self.x1 - self.x0

    @property
    def height(self) -> float:
        """Vertical extent in points."""
        return self.y1 - self.y0

    @property
    def area(self) -> float:
        """Area in square points."""
        return self.width * self.height

    def within(self, width_pt: float, height_pt: float, *, tolerance: float = 0.5) -> bool:
        """Return True if the box lies inside a ``width_pt`` x ``height_pt`` page.

        ``tolerance`` (points) absorbs the sub-point rounding PDF producers
        leave on glyphs that touch the page edge.
        """
        width = finite_float("width_pt", width_pt, lo=0.0)
        height = finite_float("height_pt", height_pt, lo=0.0)
        slack = finite_float("tolerance", tolerance, lo=0.0)
        return self.x0 >= -slack and self.y0 >= -slack and self.x1 <= width + slack and self.y1 <= height + slack


@dataclass(frozen=True, slots=True, kw_only=True)
class PageQuality:
    """Extraction-quality signals measured for one page.

    Attributes:
        valid_char_ratio: Share of extracted characters that are valid text, in [0, 1].
        char_count: Extracted character count (>= 0).
        vector_line_count: Vector line segments on the page (>= 0).
        hidden_spans_flagged: Text spans flagged as hidden (>= 0).
        rotation_deg: Declared page rotation: 0, 90, 180, or 270.
        deskew_angle_deg: Measured skew angle in degrees, in [-45, 45].
        ocr_mean_confidence: Mean OCR confidence in [0, 100], or None when no OCR ran.
    """

    valid_char_ratio: float
    char_count: int
    vector_line_count: int
    hidden_spans_flagged: int
    rotation_deg: int
    deskew_angle_deg: float
    ocr_mean_confidence: float | None

    def __post_init__(self) -> None:
        ratio = finite_float("valid_char_ratio", self.valid_char_ratio, lo=0.0, hi=1.0)
        object.__setattr__(self, "valid_char_ratio", ratio)
        int_at_least("char_count", self.char_count, 0)
        int_at_least("vector_line_count", self.vector_line_count, 0)
        int_at_least("hidden_spans_flagged", self.hidden_spans_flagged, 0)
        int_at_least("rotation_deg", self.rotation_deg, 0)
        if self.rotation_deg not in _RIGHT_ANGLE_ROTATIONS:
            raise ValueError(f"rotation_deg must be one of {_RIGHT_ANGLE_ROTATIONS}, got {self.rotation_deg}")
        deskew = finite_float("deskew_angle_deg", self.deskew_angle_deg, lo=-45.0, hi=45.0)
        object.__setattr__(self, "deskew_angle_deg", deskew)
        if self.ocr_mean_confidence is not None:
            confidence = finite_float("ocr_mean_confidence", self.ocr_mean_confidence, lo=0.0, hi=100.0)
            object.__setattr__(self, "ocr_mean_confidence", confidence)


@dataclass(frozen=True, slots=True, kw_only=True)
class DocumentMetadata:
    """Identity and descriptive metadata for one source PDF.

    ``doc_id`` is derived, not chosen: it must equal
    ``DocumentMetadata.make_doc_id(source_name, sha256)``, so the same bytes
    under the same name always get the same id. Integrity and degraded-page
    facts are derived on ``PDFDocument`` (single source of truth), not stored here.

    Attributes:
        doc_id: Stable document id (see :meth:`make_doc_id`).
        source_name: Name the PDF was read from (non-blank).
        sha256: SHA-256 of the source bytes, 64 lowercase hex characters.
        page_count: Number of pages (>= 0).
        title: Document title from the PDF metadata, if any.
        author: Document author from the PDF metadata, if any.
    """

    doc_id: str
    source_name: str
    sha256: str
    page_count: int
    title: str | None = None
    author: str | None = None

    def __post_init__(self) -> None:
        non_blank_str("source_name", self.source_name)
        _check_sha256("sha256", self.sha256)
        int_at_least("page_count", self.page_count, 0)
        optional_str("title", self.title)
        optional_str("author", self.author)
        if not isinstance(self.doc_id, str):
            raise TypeError(f"doc_id must be a str, got {self.doc_id!r} ({type(self.doc_id).__name__})")
        expected = self.make_doc_id(self.source_name, self.sha256)
        if self.doc_id != expected:
            raise ValueError(f"doc_id must be {expected!r} for this source_name and sha256, got {self.doc_id!r}")

    @staticmethod
    def make_doc_id(source_name: str, sha256: str) -> str:
        """Build the deterministic doc id ``<stem>__<first 8 hex of sha256>``.

        The stem is the text after the last ``/`` or ``\\`` (either separator,
        on every OS), minus its last extension (a leading dot is not one), with
        every run of characters outside ``[A-Za-z0-9-]`` collapsed to one ``_``
        and outer ``_`` stripped; an empty result becomes ``"document"``. Case
        is preserved. The stem can never contain ``__``, so the separator is
        unambiguous. No pathlib: its ``stem`` differs across OSes and versions.
        """
        non_blank_str("source_name", source_name)
        _check_sha256("sha256", sha256)
        name = _PATH_SEPARATOR.split(source_name)[-1]
        dot = name.rfind(".")
        raw_stem = name[:dot] if dot > 0 else name
        stem = _DOC_ID_UNSAFE_RUN.sub("_", raw_stem).strip("_")
        return f"{stem or 'document'}__{sha256[:8]}"


@dataclass(frozen=True, slots=True, kw_only=True)
class _Element:
    """Fields and invariants shared by every document element.

    Attributes:
        element_id: Canonical id ``p<page:04d>_e<index:03d>``; its page part
            must equal ``page_number``.
        page_number: 1-based page the element was extracted from.
        reading_order: Position in the page's reading order (>= 0).
        bbox: Location on the page, in points.
        scope_path: Enclosing heading texts, outermost first (non-blank entries).
        hidden_reasons: Why the element is hidden from a human reader, in
            ``HIDDEN_REASONS`` order; empty for visible content.
    """

    element_id: str
    page_number: int
    reading_order: int
    bbox: BBox
    scope_path: tuple[str, ...] = ()
    hidden_reasons: tuple[HiddenReason, ...] = ()

    def __post_init__(self) -> None:
        int_at_least("page_number", self.page_number, 1)
        int_at_least("reading_order", self.reading_order, 0)
        id_page = _parse_element_id("element_id", self.element_id)
        if id_page != self.page_number:
            raise ValueError(
                f"element_id {self.element_id!r} encodes page {id_page}, not page_number {self.page_number}"
            )
        if not isinstance(self.bbox, BBox):
            raise TypeError(f"bbox must be a BBox, got {type(self.bbox).__name__}")
        str_tuple("scope_path", self.scope_path)
        _check_hidden_reasons(self.hidden_reasons)
        self._validate_fields()

    def _validate_fields(self) -> None:
        """Validate the subclass's own fields; called after the shared checks.

        A template-method hook rather than ``super().__post_init__()``: zero-arg
        ``super()`` is unreliable in slotted dataclasses on older Pythons.
        """

    @property
    def is_hidden(self) -> bool:
        """True if the element carries at least one hidden reason."""
        return bool(self.hidden_reasons)


@dataclass(frozen=True, slots=True, kw_only=True)
class Heading(_Element):
    """A section heading of ``level`` 1-6 with non-blank ``text``."""

    level: int
    text: str

    def _validate_fields(self) -> None:
        int_at_least("level", self.level, 1)
        if self.level > 6:
            raise ValueError(f"level must be <= 6, got {self.level}")
        non_blank_str("text", self.text)

    def to_markdown(self) -> str:
        """Render as an ATX heading on one line (line breaks become spaces).

        A trailing ``#`` run gets its first ``#`` escaped so it is kept as text
        rather than eaten as the closing sequence.
        """
        text = _ATX_CLOSING_RUN.sub(r"\1\\\2\3", _LINE_BREAK.sub(" ", self.text))
        return "#" * self.level + " " + text


@dataclass(frozen=True, slots=True, kw_only=True)
class Paragraph(_Element):
    """A block of body text (non-blank)."""

    text: str

    def _validate_fields(self) -> None:
        non_blank_str("text", self.text)

    def to_markdown(self) -> str:
        """Render the text with every line's leading metacharacter escaped."""
        return _escape_lines(self.text)


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyValue(_Element):
    """A labelled value such as ``Invoice: 42``; ``value`` may be empty."""

    key: str
    value: str

    def _validate_fields(self) -> None:
        non_blank_str("key", self.key)
        if not isinstance(self.value, str):
            raise TypeError(f"value must be a str, got {self.value!r} ({type(self.value).__name__})")

    def to_markdown(self) -> str:
        """Render as ``**key:** value``.

        The key is kept on one line and stripped (a space inside ``**`` stops
        it opening emphasis), with ``\\`` escaped before ``*`` so an existing
        backslash cannot un-escape a star. Every value line is escaped so a
        multi-line value cannot start a heading, list, or setext underline.
        """
        key = _LINE_BREAK.sub(" ", self.key).strip().replace("\\", "\\\\").replace("*", "\\*")
        return f"**{key}:** {_escape_lines(self.value)}"


@dataclass(frozen=True, slots=True, kw_only=True)
class BulletList(_Element):
    """A non-empty list of non-blank items, numbered when ``ordered``."""

    items: tuple[str, ...]
    ordered: bool

    def _validate_fields(self) -> None:
        str_tuple("items", self.items)
        if not self.items:
            raise ValueError("items must contain at least one item")
        if not isinstance(self.ordered, bool):
            raise TypeError(f"ordered must be a bool, got {self.ordered!r} ({type(self.ordered).__name__})")

    def to_markdown(self) -> str:
        """Render one ``- item`` (or ``N. item``) entry per item.

        Every line of an item is escaped, and continuation lines are indented
        to the item's content column so they stay inside the list item.
        """
        entries = []
        for number, item in enumerate(self.items, start=1):
            marker = f"{number}." if self.ordered else "-"
            # Odd split() indices are the captured line breaks; keep the lines only.
            lines = [escape_leading_markdown(line) for line in _LINE_BREAK.split(item)[::2]]
            continuation = "\n" + " " * (len(marker) + 1)
            entries.append(f"{marker} {continuation.join(lines)}")
        return "\n".join(entries)


@dataclass(frozen=True, slots=True, kw_only=True)
class Table(_Element):
    """A table of string cells, held verbatim.

    The width is ``len(headers)`` when headers exist, else ``len(rows[0])``;
    it must be >= 1 and every row must match it. Blank or duplicate headers
    stay untouched in ``headers``; :meth:`column_names` derives unique names.

    Attributes:
        headers: Header cells; may be empty when the table has no header row.
        rows: Body rows, each a tuple of ``str`` cells.
        extraction_method: ``"lattice"`` (ruled lines) or ``"stream"`` (whitespace).
        continued_from_id: Element id of the table this one continues (a table
            split across pages), or None.
    """

    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    extraction_method: ExtractionMethod
    continued_from_id: str | None = None

    def _validate_fields(self) -> None:
        str_tuple("headers", self.headers, allow_blank=True)
        if not isinstance(self.rows, tuple):
            raise TypeError(f"rows must be a tuple of tuples, got {type(self.rows).__name__}")
        for index, row in enumerate(self.rows):
            str_tuple(f"rows[{index}]", row, allow_blank=True)
        self._validate_width()
        _check_choice("extraction_method", self.extraction_method, _EXTRACTION_METHODS)
        if self.continued_from_id is not None:
            _parse_element_id("continued_from_id", self.continued_from_id)
            if self.continued_from_id == self.element_id:
                raise ValueError(f"continued_from_id must differ from element_id, got {self.continued_from_id!r}")

    def _validate_width(self) -> None:
        if not self.headers and not self.rows:
            raise ValueError("table must have headers or at least one row")
        width = self._width()
        if width < 1:
            raise ValueError("table must have at least one column")
        for index, row in enumerate(self.rows):
            if len(row) != width:
                raise ValueError(f"rows[{index}] has {len(row)} cells, expected {width}")

    def _width(self) -> int:
        return len(self.headers) if self.headers else len(self.rows[0])

    def column_names(self) -> tuple[str, ...]:
        """Return unique, non-blank column names derived from ``headers``.

        The first occurrence of each non-blank header keeps its text; real
        headers are reserved up front, so a generated name never takes one. A
        blank or absent header becomes ``column_<i>`` (1-based); a repeat, or a
        generated name already in use, gets ``_2``, ``_3``, ... in column order.
        The result is deterministic and unique.
        """
        cells = self.headers if self.headers else ("",) * self._width()
        real = [cell if cell.strip() else None for cell in cells]
        taken = {cell for cell in real if cell is not None}
        claimed: set[str] = set()
        names = []
        for position, cell in enumerate(real, start=1):
            if cell is not None and cell not in claimed:
                claimed.add(cell)
                names.append(cell)
                continue
            base = f"column_{position}" if cell is None else cell
            name = _free_suffix(base, taken) if base in taken else base
            taken.add(name)
            names.append(name)
        return tuple(names)

    def to_dataframe(self) -> pl.DataFrame:
        """Return the rows as a DataFrame of ``pl.String`` columns named by :meth:`column_names`."""
        names = self.column_names()
        columns = {name: [row[i] for row in self.rows] for i, name in enumerate(names)}
        # Explicit schema: a zero-row table still yields correctly typed columns.
        return pl.DataFrame(columns, schema={name: pl.String for name in names})

    def to_arrow_table(self) -> pa.Table:
        """Return the rows as a ``pyarrow.Table`` (converted via :meth:`to_dataframe`)."""
        return self.to_dataframe().to_arrow()

    def to_dicts(self) -> list[dict[str, str]]:
        """Return one ``{column_name: cell}`` dict per row."""
        names = self.column_names()
        return [dict(zip(names, row, strict=True)) for row in self.rows]

    def to_markdown(self) -> str:
        """Render as a GFM table headed by :meth:`column_names`."""
        names = self.column_names()
        lines = [_gfm_row(names), "| " + " | ".join("---" for _ in names) + " |"]
        lines.extend(_gfm_row(row) for row in self.rows)
        return "\n".join(lines)


@dataclass(frozen=True, slots=True, kw_only=True)
class Figure(_Element):
    """An embedded image, identified by the SHA-256 of its bytes.

    Bytes come from ``image_path`` when set, else from ``_loader`` (an internal
    lazy source, excluded from equality and repr). :meth:`read_bytes` verifies
    them against ``sha256`` and fails loud on a mismatch.

    Attributes:
        sha256: SHA-256 of the image bytes, 64 lowercase hex characters.
        width_px: Image width in pixels (>= 1).
        height_px: Image height in pixels (>= 1).
        image_path: Where the image was written, if it was.
        caption: Caption text, if one was found.
    """

    sha256: str
    width_px: int
    height_px: int
    image_path: Path | None = None
    caption: str | None = None
    _loader: Callable[[], bytes] | None = field(default=None, compare=False, repr=False)

    def _validate_fields(self) -> None:
        _check_sha256("sha256", self.sha256)
        int_at_least("width_px", self.width_px, 1)
        int_at_least("height_px", self.height_px, 1)
        if self.image_path is not None and not isinstance(self.image_path, Path):
            raise TypeError(f"image_path must be a pathlib.Path or None, got {type(self.image_path).__name__}")
        optional_str("caption", self.caption)
        if self._loader is not None and not callable(self._loader):
            raise TypeError(f"_loader must be callable or None, got {type(self._loader).__name__}")

    def read_bytes(self) -> bytes:
        """Return the image bytes after verifying them against ``sha256``.

        Raises:
            DatagruntPDFError: If there is no byte source or the bytes' SHA-256
                does not match ``sha256``.
        """
        data = self._load_bytes()
        actual = hashlib.sha256(data).hexdigest()
        if actual != self.sha256:
            raise DatagruntPDFError(f"figure {self.element_id}: sha256 mismatch (expected {self.sha256}, got {actual})")
        return data

    def _load_bytes(self) -> bytes:
        if self.image_path is not None:
            return _read_asset_file(self.image_path)
        if self._loader is None:
            raise DatagruntPDFError(f"figure {self.element_id} has no image bytes: no image_path and no loader")
        data = self._loader()
        if not isinstance(data, bytes):
            raise TypeError(f"_loader must return bytes, got {type(data).__name__}")
        if len(data) > _MAX_ASSET_BYTES:
            raise DatagruntPDFError(
                f"figure {self.element_id}: loader returned {len(data)} bytes, "
                f"over the {_MAX_ASSET_BYTES}-byte asset limit"
            )
        return data

    def to_markdown(self) -> str:
        """Render an image link when ``image_path`` is set, else a placeholder comment."""
        if self.image_path is not None:
            # Alt text must stay on one line or the image link breaks apart.
            alt = escape_markdown_link_text(_LINE_BREAK.sub(" ", self.caption or ""))
            target = escape_markdown_link_target(self.image_path.as_posix())
            return f"![{alt}]({target})"
        comment = f"<!-- figure {self.element_id} sha256={self.sha256[:8]} -->"
        if self.caption is None or not self.caption.strip():
            return comment
        return f"{comment}\n{_escape_lines(self.caption)}"


# Union (not ``|``) so the alias is a runtime object on every supported Python.
DocumentElement: TypeAlias = Union[Heading, Paragraph, KeyValue, Table, BulletList, Figure]
