"""Immutable element types for the v5 PDF document model.

Every class is a frozen, slotted, keyword-only dataclass that validates its
fields in ``__post_init__`` and rejects bad input at construction. Fields hold
extracted data verbatim (preserve, not transform); Markdown escaping happens
only when an element is rendered with ``to_markdown()``.

Coordinates are PDF points with a top-left origin.
"""

import hashlib
import re
import stat
import unicodedata
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, TypeAlias, Union, get_args

import polars as pl
import pyarrow as pa

from datagrunt.core.pdf_io.extraction.markdown_escape import escape_markdown_link_target
from datagrunt.pdf._validate import (
    finite_float,
    int_at_least,
    non_blank_str,
    optional_str,
    positive_float,
    sha256_hex,
    str_tuple,
)
from datagrunt.pdf.exceptions import DatagruntPDFError

HiddenReason: TypeAlias = Literal["off_page", "invisible", "tiny_font", "transparent", "matches_background"]
"""Why a text span is hidden from a human reader (it is flagged, never dropped).

``matches_background`` means the text colour is indistinguishable from the
background it is drawn on.
"""

# Canonical order, derived from the Literal so the two can never drift apart.
HIDDEN_REASONS: tuple[HiddenReason, ...] = get_args(HiddenReason)

PageStatus: TypeAlias = Literal["ok", "ocr_fallback", "ocr_unavailable", "error"]
PageRoute: TypeAlias = Literal["digital", "hybrid", "vision"]
ExtractionMethod: TypeAlias = Literal["lattice", "stream"]
_EXTRACTION_METHODS: tuple[ExtractionMethod, ...] = get_args(ExtractionMethod)

# Element ids look like ``p0001_e003``. [0-9] rather than \d: \d also matches
# non-ASCII Unicode digits, which int() would happily parse.
_ELEMENT_ID = re.compile(r"p([0-9]{4,})_e([0-9]{3,})")

# A Markdown line ending: CommonMark treats a lone CR as one too.
_LINE_BREAK = re.compile(r"\r\n|\r|\n")

# Characters that can open Markdown syntax anywhere in a line: the backslash
# itself, code spans/fences, emphasis, links, raw HTML/autolinks, entities,
# blockquotes, table cells, strikethrough/fences, and headings. Every one is
# backslash-escaped wherever it appears (CommonMark allows escaping any ASCII
# punctuation), so a literal backslash can never escape what follows it.
_MARKDOWN_SYNTAX_CHAR = re.compile(r"[\\`*_\[\]<>&|~#]")

# What can still open a block at the start of a line once every
# _MARKDOWN_SYNTAX_CHAR is escaped: a list, thematic-break, or setext marker
# (``-``, ``+``, ``=``), or an ordered-list number (digits before ``.``/``)``).
# [0-9], not \d: list numbers are ASCII digits only.
_LINE_START_MARKER = re.compile(r"\A(?:[-+=]|[0-9]+(?=[.)]))")

# The Markdown line datagrunt puts between two consecutive lists: without it a
# blank line alone would merge them into one loose list (and renumber it).
_LIST_SEPARATOR = "<!-- -->"

# Upper bound on one asset's bytes (an image file or loader result): every
# read is bounded, so a corrupt or hostile file cannot exhaust memory.
_MAX_ASSET_BYTES = 1 << 30  # 1 GiB

# Page rotations a PDF can declare (``/Rotate`` is a multiple of 90).
_RIGHT_ANGLE_ROTATIONS = (0, 90, 180, 270)

# PageQuality's optional float signals and their inclusive (lo, hi) ranges.
_QUALITY_RANGES = (
    ("valid_char_ratio", 0.0, 1.0),
    ("deskew_angle_deg", -45.0, 45.0),
    ("ocr_mean_confidence", 0.0, 100.0),
)

# Either path separator, so a doc_id never depends on the OS that built it.
_PATH_SEPARATOR = re.compile(r"[\\/]")

# Runs of characters a doc_id stem may not contain. ASCII-only on purpose: the
# id must be safe as a file name and URL segment on every platform.
_DOC_ID_UNSAFE_RUN = re.compile(r"[^A-Za-z0-9-]+")

# Longest doc_id stem kept: bounds file-name and cache-key length.
_DOC_ID_STEM_MAX = 64

# Hex characters of the source SHA-256 in a doc_id. 8 collide ~69% across 100k
# files sharing a stem (every non-Latin name becomes "document"); 16 do not.
_DOC_ID_SHA_CHARS = 16


def _ascii_folded(text: str) -> str:
    """Return ``text`` NFKD-normalised with combining marks dropped (``"Café"`` -> ``"Cafe"``).

    NFC and NFD spellings of a name normalise to the same string, so they get
    the same doc_id; compatibility forms (ligatures, full-width letters) fold
    to their ASCII equivalents where one exists.
    """
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(char for char in decomposed if not unicodedata.category(char).startswith("M"))


def make_element_id(page_number: int, index: int) -> str:
    """Return the canonical element id for ``index`` on ``page_number``, e.g. ``p0001_e003``."""
    int_at_least("page_number", page_number, 1)
    int_at_least("index", index, 0)
    return f"p{page_number:04d}_e{index:03d}"


def _parse_element_id(name: str, value: object) -> tuple[int, int]:
    """Validate a canonical element id and return the ``(page_number, index)`` it encodes.

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
    return page, index


def check_hidden_reasons(value: object) -> None:
    """Require unique, known hidden reasons in ``HIDDEN_REASONS`` order.

    The one rule for every ``hidden_reasons`` field: elements here, and the
    v5 quality stage's span pieces (:mod:`datagrunt.pdf.quality`).

    Raises:
        TypeError: If ``value`` is not a tuple of str.
        ValueError: If a reason is unknown, repeated, or out of canonical order.
    """
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


def _free_suffix(base: str, taken: set[str], next_k: dict[str, int]) -> str:
    """Return the first ``base_<k>`` (k >= 2) whose casefold is not in ``taken``.

    ``next_k`` records, per casefolded base, where the previous search for it
    stopped; the search resumes there, because ``taken`` only grows. A
    candidate ``<base>_<k>`` belongs to one base only (``k`` holds no ``_``),
    so each taken name is skipped at most once over all searches: naming n
    columns is O(n) in total, not O(n^2).
    """
    key = base.casefold()
    start = next_k.get(key, 2)
    # At most len(taken) candidates can be taken, so len(taken) + 1 distinct
    # candidates always include a free one: the loop is bounded and total.
    for k in range(start, start + len(taken) + 1):
        candidate = f"{base}_{k}"
        if candidate.casefold() not in taken:
            next_k[key] = k + 1
            return candidate
    raise RuntimeError(f"no free column-name suffix for {base!r}")  # unreachable by the bound above


def _escape_line_start(match: re.Match[str]) -> str:
    """Escape a line-start block marker: ``\\-`` for a marker character, ``2\\)`` for a number."""
    token = match.group(0)
    return token + "\\" if token[0].isdigit() else "\\" + token


def _markdown_literal(text: str) -> list[str]:
    """Return ``text`` as Markdown lines that render back to its visible text, with no syntax.

    The text is split on line breaks (``\\r\\n``, ``\\r``, ``\\n``). Each line is
    stripped of leading and trailing whitespace and blank lines are dropped:
    Markdown cannot display them (leading spaces indent a line into a list
    item or code block, two trailing spaces make a hard break, a blank line
    splits the block). This affects rendering only; stored fields are unchanged.

    Every ``_MARKDOWN_SYNTAX_CHAR`` is then backslash-escaped (so each literal
    backslash is doubled), and a block marker left at the line start is
    escaped too. The caller joins the lines as its context requires.
    """
    lines = []
    for raw_line in _LINE_BREAK.split(text):
        line = raw_line.strip()
        if line:
            escaped = _MARKDOWN_SYNTAX_CHAR.sub(r"\\\g<0>", line)
            lines.append(_LINE_START_MARKER.sub(_escape_line_start, escaped, count=1))
    return lines


def _gfm_row(cells: tuple[str, ...]) -> str:
    """Render one GFM table row; a cell's lines are joined with ``<br>``."""
    return "| " + " | ".join("<br>".join(_markdown_literal(cell)) for cell in cells) + " |"


def _read_capped(path: Path, cap: int) -> bytes:
    """Read at most ``cap + 1`` bytes of ``path``: one byte over ``cap`` proves it is too big."""
    with path.open("rb") as handle:
        return handle.read(cap + 1)


def _regular_file_size(path: Path, owner: str) -> int:
    """Return the size of ``path`` after checking, before any open, that it is a regular file.

    Opening a named pipe blocks until a writer appears, and a device or
    directory is not an asset, so anything but a regular file (symlinks are
    followed) is refused without being opened.
    """
    status = path.stat()
    if not stat.S_ISREG(status.st_mode):
        raise DatagruntPDFError(f"{owner}{path} is not a regular file")
    return status.st_size


def _read_asset_file(path: Path) -> bytes:
    """Read a regular asset file, refusing anything over ``_MAX_ASSET_BYTES``.

    ``stat()`` rejects a non-regular or oversized file before reading; the
    capped read keeps the bound even if the file grows between the two calls.
    """
    limit = _MAX_ASSET_BYTES
    size = _regular_file_size(path, "")
    if size > limit:
        raise DatagruntPDFError(f"{path} is {size} bytes, over the {limit}-byte asset limit")
    data = _read_capped(path, limit)
    if len(data) > limit:
        raise DatagruntPDFError(f"{path} grew past the {limit}-byte asset limit while being read")
    return data


def _read_band_files(owner: str, paths: tuple[Path, ...]) -> tuple[bytes, ...]:
    """Read band files whose combined size stays within ``_MAX_ASSET_BYTES``.

    The bands together are one page image, so the limit bounds their total:
    every path is checked to be a regular file and its ``stat()`` size summed
    before any is opened, and each read is capped at the budget left, so the
    bound holds even if a file grows in between.
    """
    limit = _MAX_ASSET_BYTES
    total = sum(_regular_file_size(path, f"{owner}: band file ") for path in paths)
    if total > limit:
        raise DatagruntPDFError(f"{owner}: band files total {total} bytes, over the {limit}-byte asset limit")
    bands: list[bytes] = []
    remaining = limit
    for path in paths:
        band = _read_capped(path, remaining)
        if len(band) > remaining:
            raise DatagruntPDFError(f"{owner}: band files grew past the {limit}-byte asset limit while being read")
        remaining -= len(band)
        bands.append(band)
    return tuple(bands)


def _check_loaded_bands(owner: str, bands: object) -> tuple[bytes, ...]:
    """Return a band loader's result if it is a non-empty tuple of bytes within ``_MAX_ASSET_BYTES`` in total."""
    if not isinstance(bands, tuple):
        raise TypeError(f"_band_loader must return a tuple of bytes, got {type(bands).__name__}")
    if not bands:
        raise DatagruntPDFError(f"{owner}: _band_loader returned no bands")
    for index, band in enumerate(bands):
        if not isinstance(band, bytes):
            raise TypeError(f"_band_loader result[{index}] must be bytes, got {type(band).__name__}")
    total = sum(len(band) for band in bands)
    if total > _MAX_ASSET_BYTES:
        raise DatagruntPDFError(
            f"{owner}: _band_loader results total {total} bytes, over the {_MAX_ASSET_BYTES}-byte asset limit"
        )
    return bands


def _check_loaded_asset(owner: str, what: str, data: object) -> bytes:
    """Return loader-supplied ``data`` if it is ``bytes`` within ``_MAX_ASSET_BYTES``.

    ``owner`` names the object (``figure p0001_e001``) and ``what`` the value
    (``_loader result``) in the error messages.
    """
    if not isinstance(data, bytes):
        raise TypeError(f"{what} must be bytes, got {type(data).__name__}")
    if len(data) > _MAX_ASSET_BYTES:
        raise DatagruntPDFError(f"{owner}: {what} is {len(data)} bytes, over the {_MAX_ASSET_BYTES}-byte asset limit")
    return data


def _check_optional_path(name: str, value: object) -> None:
    """Require ``value`` to be a ``pathlib.Path`` or None."""
    if value is not None and not isinstance(value, Path):
        raise TypeError(f"{name} must be a pathlib.Path or None, got {type(value).__name__}")


def _check_optional_callable(name: str, value: object) -> None:
    """Require ``value`` to be callable or None."""
    if value is not None and not callable(value):
        raise TypeError(f"{name} must be callable or None, got {type(value).__name__}")


def _check_bool(name: str, value: object) -> None:
    """Require ``value`` to be exactly a bool (not an int standing in for one)."""
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, got {value!r} ({type(value).__name__})")


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

    A signal that was not measured is None, never a stand-in value.

    Attributes:
        valid_char_ratio: Share of extracted characters that are valid text, in
            [0, 1]; None exactly when ``char_count`` is 0 (no characters, no ratio).
        char_count: Extracted character count (>= 0).
        vector_line_count: Vector line segments on the page (>= 0).
        hidden_spans_flagged: Text spans flagged as hidden (>= 0).
        rotation_deg: Declared page rotation: 0, 90, 180, or 270.
        deskew_angle_deg: Measured skew angle in degrees, in [-45, 45], or None
            when skew was not measured.
        ocr_mean_confidence: Mean OCR confidence in [0, 100], or None when no OCR ran.
    """

    valid_char_ratio: float | None
    char_count: int
    vector_line_count: int
    hidden_spans_flagged: int
    rotation_deg: int
    deskew_angle_deg: float | None
    ocr_mean_confidence: float | None

    def __post_init__(self) -> None:
        int_at_least("char_count", self.char_count, 0)
        int_at_least("vector_line_count", self.vector_line_count, 0)
        int_at_least("hidden_spans_flagged", self.hidden_spans_flagged, 0)
        int_at_least("rotation_deg", self.rotation_deg, 0)
        if self.rotation_deg not in _RIGHT_ANGLE_ROTATIONS:
            raise ValueError(f"rotation_deg must be one of {_RIGHT_ANGLE_ROTATIONS}, got {self.rotation_deg}")
        for name, lo, hi in _QUALITY_RANGES:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, finite_float(name, value, lo=lo, hi=hi))
        self._check_ratio_presence()

    def _check_ratio_presence(self) -> None:
        """Require ``valid_char_ratio`` to be None exactly when ``char_count`` is 0."""
        if self.char_count == 0 and self.valid_char_ratio is not None:
            raise ValueError(
                f"valid_char_ratio must be None when char_count is 0 (no characters, no ratio), "
                f"got {self.valid_char_ratio!r}"
            )
        if self.char_count > 0 and self.valid_char_ratio is None:
            raise ValueError(f"valid_char_ratio is required when char_count is {self.char_count}, got None")


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
        sha256_hex("sha256", self.sha256)
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
        """Build the deterministic doc id ``<stem>__<first 16 hex of sha256>``.

        The stem is the text after the last ``/`` or ``\\`` (either separator,
        on every OS), minus its last extension (a leading dot is not one),
        NFKD-normalised with combining marks dropped (so NFC and NFD names
        match), with every run of characters outside ``[A-Za-z0-9-]`` collapsed
        to one ``_``, outer ``_`` stripped, and capped at 64 characters (a
        ``_`` left at the cut is stripped too); an empty result becomes
        ``"document"``. Case is preserved. The stem can never contain ``__``
        or end in ``_``, so the separator is unambiguous. No pathlib: its
        ``stem`` differs across OSes and versions.
        """
        non_blank_str("source_name", source_name)
        sha256_hex("sha256", sha256)
        name = _PATH_SEPARATOR.split(source_name)[-1]
        dot = name.rfind(".")
        raw_stem = _ascii_folded(name[:dot] if dot > 0 else name)
        stem = _DOC_ID_UNSAFE_RUN.sub("_", raw_stem).strip("_")[:_DOC_ID_STEM_MAX].rstrip("_")
        return f"{stem or 'document'}__{sha256[:_DOC_ID_SHA_CHARS]}"


@dataclass(frozen=True, slots=True, kw_only=True)
class _Element:
    """Fields and invariants shared by every document element.

    Attributes:
        element_id: Canonical id ``p<page:04d>_e<index:03d>``; it must equal
            ``make_element_id(page_number, reading_order)``, so the id names
            exactly one (page, reading-order) position.
        page_number: 1-based page the element was extracted from.
        reading_order: Position in the page's reading order (>= 0); the
            element id's index.
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
        id_page, id_index = _parse_element_id("element_id", self.element_id)
        if id_page != self.page_number:
            raise ValueError(
                f"element_id {self.element_id!r} encodes page {id_page}, not page_number {self.page_number}"
            )
        if id_index != self.reading_order:
            raise ValueError(
                f"element_id {self.element_id!r} encodes index {id_index}, not reading_order {self.reading_order}"
            )
        if not isinstance(self.bbox, BBox):
            raise TypeError(f"bbox must be a BBox, got {type(self.bbox).__name__}")
        str_tuple("scope_path", self.scope_path)
        check_hidden_reasons(self.hidden_reasons)
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

    def to_markdown(self) -> str:
        """Render the element as Markdown; all element text is escaped, never raw HTML.

        A hidden element is rendered in full after a ``<!-- hidden: reasons -->``
        line (reasons comma-joined, canonical order), so Markdown consumers can
        see and filter hidden text instead of having it silently dropped.
        """
        body = self._render_markdown()
        if not self.hidden_reasons:
            return body
        return f"<!-- hidden: {','.join(self.hidden_reasons)} -->\n{body}"

    def _render_markdown(self) -> str:
        """Render the subclass's own Markdown (the hidden marker is added by :meth:`to_markdown`)."""
        raise NotImplementedError(f"{type(self).__name__} must implement _render_markdown")


@dataclass(frozen=True, slots=True, kw_only=True)
class Heading(_Element):
    """A section heading of ``level`` 1-6 with non-blank ``text``.

    A heading opens its own scope, so its ``scope_path`` includes its text:
    it is non-empty and ends with ``text`` (verbatim), after the texts of the
    headings enclosing it.
    """

    level: int
    text: str

    def _validate_fields(self) -> None:
        int_at_least("level", self.level, 1)
        if self.level > 6:
            raise ValueError(f"level must be <= 6, got {self.level}")
        non_blank_str("text", self.text)
        if not self.scope_path or self.scope_path[-1] != self.text:
            raise ValueError(f"scope_path must end with the heading's own text {self.text!r}, got {self.scope_path!r}")

    def _render_markdown(self) -> str:
        """Render as an ATX heading on one line: the text's lines joined with spaces.

        Every ``#`` is escaped, so a trailing run is never eaten as the
        heading's closing sequence.
        """
        return "#" * self.level + " " + " ".join(_markdown_literal(self.text))


@dataclass(frozen=True, slots=True, kw_only=True)
class Paragraph(_Element):
    """A block of body text (non-blank)."""

    text: str

    def _validate_fields(self) -> None:
        non_blank_str("text", self.text)

    def _render_markdown(self) -> str:
        """Render the text as literal Markdown lines (see :func:`_markdown_literal`)."""
        return "\n".join(_markdown_literal(self.text))


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyValue(_Element):
    """A labelled value such as ``Invoice: 42``; ``value`` may be empty."""

    key: str
    value: str

    def _validate_fields(self) -> None:
        non_blank_str("key", self.key)
        if not isinstance(self.value, str):
            raise TypeError(f"value must be a str, got {self.value!r} ({type(self.value).__name__})")

    def _render_markdown(self) -> str:
        """Render as ``**key:** value``, key and value as literal Markdown lines.

        Stripped key lines keep ``**`` opening emphasis (a space after it would
        not); an empty value leaves ``**key:** `` with nothing after the space.
        """
        key = "\n".join(_markdown_literal(self.key))
        return f"**{key}:** " + "\n".join(_markdown_literal(self.value))


@dataclass(frozen=True, slots=True, kw_only=True)
class BulletList(_Element):
    """A non-empty list of non-blank items, numbered when ``ordered``."""

    items: tuple[str, ...]
    ordered: bool

    def _validate_fields(self) -> None:
        str_tuple("items", self.items)
        if not self.items:
            raise ValueError("items must contain at least one item")
        _check_bool("ordered", self.ordered)

    def _render_markdown(self) -> str:
        """Render one ``- item`` (or ``N. item``) entry per item.

        Each item becomes literal Markdown lines, and continuation lines are
        indented to the item's content column so they stay inside the list item.
        """
        entries = []
        for number, item in enumerate(self.items, start=1):
            marker = f"{number}." if self.ordered else "-"
            lines = _markdown_literal(item)
            continuation = "\n" + " " * (len(marker) + 1)
            entries.append(f"{marker} {continuation.join(lines)}")
        return "\n".join(entries)


@dataclass(frozen=True, slots=True, kw_only=True)
class Table(_Element):
    """A table of string cells, held verbatim.

    The width is ``len(headers)`` when headers exist, else ``len(rows[0])``;
    it must be >= 1 and every row must match it. Blank or duplicate headers
    stay untouched in ``headers`` and render verbatim in Markdown;
    :meth:`column_names` derives unique names for the DataFrame exports.

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
        """Return unique, non-blank column names derived from ``headers``, for DataFrames.

        Names are compared case-insensitively (``str.casefold``), as DuckDB
        compares column names. The first occurrence of each non-blank header
        keeps its text; real headers are reserved up front, so a generated
        name never takes one. A blank or absent header becomes ``column_<i>``
        (1-based); a repeat, or a generated name already in use, gets ``_2``,
        ``_3``, ... in column order. The result is deterministic, unique even
        ignoring case, and computed in O(n) for n columns.
        """
        cells = self.headers if self.headers else ("",) * self._width()
        real = [cell if cell.strip() else None for cell in cells]
        taken = {cell.casefold() for cell in real if cell is not None}
        claimed: set[str] = set()
        next_k: dict[str, int] = {}
        names = []
        for position, cell in enumerate(real, start=1):
            if cell is not None and cell.casefold() not in claimed:
                claimed.add(cell.casefold())
                names.append(cell)
                continue
            base = f"column_{position}" if cell is None else cell
            name = _free_suffix(base, taken, next_k) if base.casefold() in taken else base
            taken.add(name.casefold())
            names.append(name)
        return tuple(names)

    def to_dataframe(self) -> pl.DataFrame:
        """Return the rows as a DataFrame of ``pl.String`` columns named by :meth:`column_names`."""
        names = self.column_names()
        columns = {name: [row[i] for row in self.rows] for i, name in enumerate(names)}
        # Explicit schema: a zero-row table still yields correctly typed columns.
        return pl.DataFrame(columns, schema={name: pl.String for name in names})

    def to_arrow_table(self) -> pa.Table:
        """Return the rows as a ``pyarrow.Table`` of ``pa.large_string()`` columns.

        Converted via :meth:`to_dataframe`, then cast to an explicit schema:
        polars' default Arrow string type varies by version (``large_string``,
        ``string_view``), and callers need one stable type.
        """
        frame = self.to_dataframe()
        schema = pa.schema([(name, pa.large_string()) for name in frame.columns])
        return frame.to_arrow().cast(schema)

    def to_dicts(self) -> list[dict[str, str]]:
        """Return one ``{column_name: cell}`` dict per row."""
        names = self.column_names()
        return [dict(zip(names, row, strict=True)) for row in self.rows]

    def _render_markdown(self) -> str:
        """Render as a GFM table headed by the raw ``headers``, escaped.

        A blank header is an empty cell and a table without headers gets an
        empty header row (GFM requires one): Markdown shows the table as
        extracted, while :meth:`column_names` exists for DataFrame column names.
        """
        width = self._width()
        header = self.headers if self.headers else ("",) * width
        lines = [_gfm_row(header), "| " + " | ".join("---" for _ in range(width)) + " |"]
        lines.extend(_gfm_row(row) for row in self.rows)
        return "\n".join(lines)


@dataclass(frozen=True, slots=True, kw_only=True)
class Figure(_Element):
    """An embedded image, identified by the SHA-256 of its raw embedded stream.

    Two byte sources, each excluded from equality and repr:

    - :meth:`read_raw_bytes` returns the raw stream (as stored in the PDF,
      still encoded by its filters) from ``_raw_loader``, verified against
      ``sha256``: a mismatch fails loud.
    - :meth:`read_bytes` returns the image as PNG, a derived artifact: from
      ``image_path`` when set, else from ``_loader``. It is not checked
      against ``sha256``, which hashes the raw stream, not the PNG.

    The engine sets both loaders to an ``ImageLoader``'s bound methods,
    which reopen the PDF on demand. For an encrypted PDF that loader holds
    the password, so a pickled Figure (one sent across a process pool)
    carries the password inside its loaders' pickle: never store or send
    such a pickle anywhere the PDF's readers could not already go. The
    password stays out of the Figure's repr and equality.

    Attributes:
        sha256: SHA-256 of the image's raw embedded stream, 64 lowercase hex
            characters. Unlike a hash of exported bytes, it survives encoder
            upgrades, so image dedupe and cache keys stay stable.
        width_px: Image width in pixels (>= 1).
        height_px: Image height in pixels (>= 1).
        image_path: Where the PNG was written, if it was. Not part of
            equality: the same image stored elsewhere is the same figure.
        caption: Caption text, if one was found.
    """

    sha256: str
    width_px: int
    height_px: int
    # Storage location, not identity: excluded from equality and hashing.
    image_path: Path | None = field(default=None, compare=False)
    caption: str | None = None
    # Internal lazy sources: PNG bytes, and the raw stream sha256 hashes.
    _loader: Callable[[], bytes] | None = field(default=None, compare=False, repr=False)
    _raw_loader: Callable[[], bytes] | None = field(default=None, compare=False, repr=False)

    def _validate_fields(self) -> None:
        sha256_hex("sha256", self.sha256)
        int_at_least("width_px", self.width_px, 1)
        int_at_least("height_px", self.height_px, 1)
        _check_optional_path("image_path", self.image_path)
        optional_str("caption", self.caption)
        _check_optional_callable("_loader", self._loader)
        _check_optional_callable("_raw_loader", self._raw_loader)

    def read_bytes(self) -> bytes:
        """Return the image as PNG bytes: ``image_path`` first, then the loader.

        Derived bytes, so they are not checked against ``sha256`` (use
        :meth:`read_raw_bytes` for verified bytes); every read is bounded by
        the asset limit.

        Raises:
            DatagruntPDFError: If there is no byte source, ``image_path`` is not
                a regular file, or the bytes exceed the asset limit.
            OSError: If ``image_path`` cannot be stat'ed or read (e.g.
                ``FileNotFoundError``, ``PermissionError``).
            TypeError: If the loader returns something other than ``bytes``.
        """
        if self.image_path is not None:
            return _read_asset_file(self.image_path)
        if self._loader is None:
            raise DatagruntPDFError(f"figure {self.element_id} has no image bytes: no image_path and no loader")
        return _check_loaded_asset(f"figure {self.element_id}", "_loader result", self._loader())

    def read_raw_bytes(self) -> bytes:
        """Return the raw embedded stream after verifying it against ``sha256``.

        Raises:
            DatagruntPDFError: If there is no raw loader, its bytes exceed the
                asset limit, or their SHA-256 does not match ``sha256``.
            TypeError: If the raw loader returns something other than ``bytes``.
        """
        owner = f"figure {self.element_id}"
        if self._raw_loader is None:
            raise DatagruntPDFError(f"{owner} has no raw image bytes: no raw loader")
        data = _check_loaded_asset(owner, "_raw_loader result", self._raw_loader())
        actual = hashlib.sha256(data).hexdigest()
        if actual != self.sha256:
            raise DatagruntPDFError(f"{owner}: sha256 mismatch (expected {self.sha256}, got {actual})")
        return data

    def _render_markdown(self) -> str:
        """Render an image link when ``image_path`` is set, else a placeholder comment.

        The caption is literal Markdown: as alt text its lines are joined with
        spaces (a line break would split the link; ``[``, ``]`` and ``\\`` are
        already escaped), under the placeholder comment with line breaks.
        """
        lines = _markdown_literal(self.caption or "")
        if self.image_path is not None:
            target = escape_markdown_link_target(self.image_path.as_posix())
            return f"![{' '.join(lines)}]({target})"
        comment = f"<!-- figure {self.element_id} sha256={self.sha256[:8]} -->"
        return "\n".join([comment, *lines])


# Union (not ``|``) so the alias is a runtime object on every supported Python.
DocumentElement: TypeAlias = Union[Heading, Paragraph, KeyValue, Table, BulletList, Figure]


@dataclass(frozen=True, slots=True, kw_only=True)
class PageImage:
    """A rendered raster of one page, plus optional horizontal bands of it.

    Bytes come from files when set (``image_path``, ``band_paths``), else from
    the internal lazy loaders (``_loader``, ``_band_loader``, excluded from
    equality and repr). The full image and the bands are separate sources:
    neither stands in for the other. Every read is bounded by the asset limit.

    Attributes:
        page_number: 1-based page this image renders.
        dpi: Render resolution in dots per inch (finite, > 0). A float: a
            render bounded by pixel size has a fractional dpi, below 36 for a
            very large page.
        width_px: Image width in pixels (>= 1).
        height_px: Image height in pixels (>= 1).
        image_path: Where the full-page image was written, if it was.
        band_paths: Where the bands were written, top to bottom; empty if none.
            Neither path is part of equality: storage location is not identity.
        contrast_normalized: True if autocontrast was applied before saving.
    """

    page_number: int
    dpi: float
    width_px: int
    height_px: int
    # Storage locations, not identity: excluded from equality and hashing.
    image_path: Path | None = field(default=None, compare=False)
    band_paths: tuple[Path, ...] = field(default=(), compare=False)
    contrast_normalized: bool = False
    _loader: Callable[[], bytes] | None = field(default=None, compare=False, repr=False)
    _band_loader: Callable[[], tuple[bytes, ...]] | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        int_at_least("page_number", self.page_number, 1)
        object.__setattr__(self, "dpi", positive_float("dpi", self.dpi))
        int_at_least("width_px", self.width_px, 1)
        int_at_least("height_px", self.height_px, 1)
        _check_optional_path("image_path", self.image_path)
        if not isinstance(self.band_paths, tuple):
            raise TypeError(f"band_paths must be a tuple of pathlib.Path, got {type(self.band_paths).__name__}")
        for index, path in enumerate(self.band_paths):
            if not isinstance(path, Path):
                raise TypeError(f"band_paths[{index}] must be a pathlib.Path, got {type(path).__name__}")
        _check_bool("contrast_normalized", self.contrast_normalized)
        _check_optional_callable("_loader", self._loader)
        _check_optional_callable("_band_loader", self._band_loader)

    def read_bytes(self) -> bytes:
        """Return the full-page image bytes: ``image_path`` first, then the loader.

        Raises:
            DatagruntPDFError: If there is no byte source, ``image_path`` is not
                a regular file, or the bytes exceed the asset limit.
            OSError: If ``image_path`` cannot be stat'ed or read (e.g.
                ``FileNotFoundError``, ``PermissionError``).
            TypeError: If the loader returns something other than ``bytes``.
        """
        if self.image_path is not None:
            return _read_asset_file(self.image_path)
        if self._loader is None:
            raise DatagruntPDFError(f"page image {self.page_number} has no image bytes: no image_path and no loader")
        return _check_loaded_asset(f"page image {self.page_number}", "_loader result", self._loader())

    def read_bands(self) -> tuple[bytes, ...]:
        """Return the band bytes top to bottom: ``band_paths`` first, then the band loader.

        The bands together are one page image, so the asset limit bounds their
        total, not each band.

        Raises:
            DatagruntPDFError: If there is no band source (a loader returning no
                bands counts as none), a band path is not a regular file, or the
                bands exceed the asset limit in total.
            OSError: If a band path cannot be stat'ed or read (e.g.
                ``FileNotFoundError``, ``PermissionError``).
            TypeError: If the band loader returns something other than a tuple
                of ``bytes``.
        """
        owner = f"page image {self.page_number}"
        if self.band_paths:
            return _read_band_files(owner, self.band_paths)
        if self._band_loader is None:
            raise DatagruntPDFError(f"{owner} has no bands: no band_paths and no band loader")
        return _check_loaded_bands(owner, self._band_loader())


_PAGE_STATUSES: tuple[PageStatus, ...] = get_args(PageStatus)
# Statuses that only a vision-route page can have.
_VISION_ONLY_STATUSES: tuple[PageStatus, ...] = ("ocr_fallback", "ocr_unavailable")
# Public, like HIDDEN_REASONS: the v5 quality stage (datagrunt.pdf.quality) checks routes with it.
PAGE_ROUTES: tuple[PageRoute, ...] = get_args(PageRoute)
_ELEMENT_TYPES: tuple[type, ...] = get_args(DocumentElement)


def _check_instances(name: str, value: object, types: tuple[type, ...], label: str) -> None:
    """Require ``value`` to be a tuple whose every entry is an instance of ``types``."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(value).__name__}")
    for index, item in enumerate(value):
        if not isinstance(item, types):
            raise TypeError(f"{name}[{index}] must be a {label}, got {type(item).__name__}")


def _check_document_elements(name: str, value: object) -> None:
    """Require ``value`` to be a tuple of the six concrete element types."""
    names = ", ".join(cls.__name__ for cls in _ELEMENT_TYPES)
    _check_instances(name, value, _ELEMENT_TYPES, f"document element ({names})")


def _render_elements(elements: Iterable[DocumentElement]) -> list[str]:
    """Render each element's Markdown, with ``_LIST_SEPARATOR`` between two consecutive lists.

    Two lists separated only by a blank line merge into one loose list (an
    ordered pair is renumbered); an HTML comment line between them ends the first.
    """
    parts: list[str] = []
    previous: DocumentElement | None = None
    for element in elements:
        if isinstance(element, BulletList) and isinstance(previous, BulletList):
            parts.append(_LIST_SEPARATOR)
        parts.append(element.to_markdown())
        previous = element
    return parts


@dataclass(frozen=True, slots=True, kw_only=True)
class Page:
    """One extracted page: its elements in reading order plus how they were obtained.

    ``status`` says how extraction went:

    - ``"ok"``: extracted on its ``route``.
    - ``"ocr_fallback"``: digital extraction raised and the page was recovered
      by render + OCR, so its route is ``"vision"``.
    - ``"ocr_unavailable"``: a ``"vision"`` page whose OCR could not run; the
      digital spans it had are kept as its elements.
    - ``"error"``: nothing was recovered; ``error`` says why and the page has
      no elements and no quality signals.

    Every status but ``"ok"`` marks a degraded page (``PDFDocument.degraded_pages``).

    ``integrity_verified`` is True when the reconstructed elements were checked
    against the page's text layer; a vision page has no text layer to check
    against, so it is never verified.

    Attributes:
        page_number: 1-based page number.
        width_pt: Page width in points (finite, > 0).
        height_pt: Page height in points (finite, > 0).
        status: ``"ok"``, ``"ocr_fallback"``, ``"ocr_unavailable"``, or ``"error"`` (see above).
        route: ``"digital"``, ``"hybrid"``, or ``"vision"``.
        quality: Extraction-quality signals for the page; None exactly when
            ``status == "error"`` (nothing was measured).
        elements: The page's elements; ``reading_order`` strictly increases in
            tuple order and ``element_id`` is unique. An element is flagged
            ``"off_page"`` exactly when its bbox lies outside the page.
        page_image: The rendered page, if one was made; same ``page_number``.
        integrity_verified: True if the elements passed the integrity check.
        error: Why extraction failed; set exactly when ``status == "error"``.
        warnings: Non-fatal problems met while extracting the page.
    """

    page_number: int
    width_pt: float
    height_pt: float
    status: PageStatus
    route: PageRoute
    quality: PageQuality | None
    elements: tuple[DocumentElement, ...] = ()
    page_image: PageImage | None = None
    integrity_verified: bool = False
    error: str | None = None
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self._validate_fields()
        self._check_status()
        if self.page_image is not None and self.page_image.page_number != self.page_number:
            raise ValueError(
                f"page_image.page_number {self.page_image.page_number} must equal page_number {self.page_number}"
            )
        self._check_elements()

    def _validate_fields(self) -> None:
        """Check each field's own type and range (coercing the dimensions to float)."""
        int_at_least("page_number", self.page_number, 1)
        object.__setattr__(self, "width_pt", positive_float("width_pt", self.width_pt))
        object.__setattr__(self, "height_pt", positive_float("height_pt", self.height_pt))
        _check_choice("status", self.status, _PAGE_STATUSES)
        _check_choice("route", self.route, PAGE_ROUTES)
        if self.quality is not None and not isinstance(self.quality, PageQuality):
            raise TypeError(f"quality must be a PageQuality or None, got {type(self.quality).__name__}")
        _check_document_elements("elements", self.elements)
        if self.page_image is not None and not isinstance(self.page_image, PageImage):
            raise TypeError(f"page_image must be a PageImage or None, got {type(self.page_image).__name__}")
        _check_bool("integrity_verified", self.integrity_verified)
        optional_str("error", self.error)
        str_tuple("warnings", self.warnings)

    def _check_status(self) -> None:
        """Enforce the status / route / error / quality / integrity_verified invariants."""
        if self.status == "error":
            self._check_error_page()
        elif self.error is not None:
            raise ValueError(
                f"error must be None unless status is 'error', got status={self.status!r}, error={self.error!r}"
            )
        elif self.quality is None:
            raise ValueError(f"status {self.status!r} requires a PageQuality, got quality=None")
        if self.status in _VISION_ONLY_STATUSES and self.route != "vision":
            raise ValueError(f"status {self.status!r} requires route 'vision', got route={self.route!r}")
        if self.route == "vision" and self.integrity_verified:
            raise ValueError("a vision page cannot be integrity_verified: it has no text layer to verify against")

    def _check_error_page(self) -> None:
        """An error page explains itself and holds nothing: no elements, no quality, no verification."""
        if self.error is None or not self.error.strip():
            raise ValueError(f"status 'error' requires a non-blank error message, got error={self.error!r}")
        if self.elements:
            raise ValueError(f"an error page must have no elements, got {len(self.elements)}")
        if self.quality is not None:
            raise ValueError(f"an error page must have quality None, got a {type(self.quality).__name__}")
        if self.integrity_verified:
            raise ValueError("an error page cannot be integrity_verified")

    def _check_elements(self) -> None:
        """Enforce page membership, unique ids, strictly increasing reading order, and page bounds."""
        seen: set[str] = set()
        previous_order = -1
        for index, element in enumerate(self.elements):
            label = f"elements[{index}] ({element.element_id})"
            if element.page_number != self.page_number:
                raise ValueError(f"{label} has page_number {element.page_number}, expected {self.page_number}")
            if element.element_id in seen:
                raise ValueError(f"elements has duplicate element_id {element.element_id!r}")
            seen.add(element.element_id)
            if element.reading_order <= previous_order:
                raise ValueError(
                    f"{label} reading_order {element.reading_order} must be greater than "
                    f"the previous element's {previous_order}"
                )
            previous_order = element.reading_order
            self._check_bounds(label, element)

    def _check_bounds(self, label: str, element: DocumentElement) -> None:
        """Require the bbox to lie off the page exactly when the element is flagged ``"off_page"``."""
        on_page = element.bbox.within(self.width_pt, self.height_pt)
        flagged = "off_page" in element.hidden_reasons
        if flagged and on_page:
            raise ValueError(
                f"{label} is flagged 'off_page' but its bbox lies within the {self.width_pt} x {self.height_pt} pt page"
            )
        if not flagged and not on_page:
            raise ValueError(
                f"{label} bbox lies outside the {self.width_pt} x {self.height_pt} pt page; "
                "an element off the page must carry the 'off_page' hidden reason"
            )

    def to_markdown(self) -> str:
        """Render a ``<!-- page N route=R status=S -->`` comment, then each element, blank-line separated.

        Consecutive lists get a ``<!-- -->`` line between them so they stay separate lists.
        """
        header = f"<!-- page {self.page_number} route={self.route} status={self.status} -->"
        return "\n\n".join([header, *_render_elements(self.elements)])


def _check_page_span(value: object) -> tuple[int, int]:
    """Return ``value`` if it is a ``(start, end)`` pair of page numbers with ``1 <= start <= end``."""
    if not isinstance(value, tuple):
        raise TypeError(f"page_span must be a tuple of two ints, got {type(value).__name__}")
    if len(value) != 2:
        raise ValueError(f"page_span must be a (start, end) pair, got {len(value)} item(s)")
    start = int_at_least("page_span[0]", value[0], 1)
    end = int_at_least("page_span[1]", value[1], 1)
    if start > end:
        raise ValueError(f"page_span start {start} must be <= end {end}")
    return start, end


def _position(element: DocumentElement) -> tuple[int, int]:
    """Return the element's place in document order: ``(page_number, reading_order)``."""
    return element.page_number, element.reading_order


@dataclass(frozen=True, slots=True, kw_only=True)
class Section:
    """A flat run of elements under one heading (or before the first heading).

    Sections do not nest: a subsection is its own ``Section`` whose
    ``scope_path`` extends its parent's, so nesting is read from
    ``scope_path`` prefixes. ``tables`` and ``figures`` are derived from
    ``elements`` on each access (O(len(elements))), in document order.

    Attributes:
        heading: The heading that opens the section, or None for content
            before the first heading. It lies on a page in ``page_span``, its
            ``scope_path`` equals the section's, and it may also appear in
            ``elements``, as ``elements[0]`` (an element with its id must equal it).
        scope_path: Enclosing heading texts, outermost first; when ``heading``
            is set it is non-empty and ends with ``heading.text`` (verbatim).
        page_span: Inclusive ``(start, end)`` pages, ``1 <= start <= end``;
            every element lies on a page in this span.
        elements: The section's elements in document order: ``(page_number,
            reading_order)`` strictly increases, so no element repeats. Every
            element's ``scope_path`` equals the section's.
    """

    heading: Heading | None
    scope_path: tuple[str, ...]
    page_span: tuple[int, int]
    elements: tuple[DocumentElement, ...]

    def __post_init__(self) -> None:
        if self.heading is not None and not isinstance(self.heading, Heading):
            raise TypeError(f"heading must be a Heading or None, got {type(self.heading).__name__}")
        str_tuple("scope_path", self.scope_path)
        start, end = _check_page_span(self.page_span)
        if self.heading is not None:
            self._check_heading(self.heading, start, end)
        _check_document_elements("elements", self.elements)
        self._check_elements(start, end)

    def _check_heading(self, heading: Heading, start: int, end: int) -> None:
        """Require the heading to open this scope and lie within ``[start, end]``."""
        if not self.scope_path:
            raise ValueError("scope_path must be non-empty when heading is set")
        if self.scope_path[-1] != heading.text:
            raise ValueError(f"scope_path[-1] {self.scope_path[-1]!r} must equal heading.text {heading.text!r}")
        if heading.scope_path != self.scope_path:
            raise ValueError(
                f"heading ({heading.element_id}) has scope_path {heading.scope_path!r}, "
                f"but the section's scope_path is {self.scope_path!r}"
            )
        if not start <= heading.page_number <= end:
            raise ValueError(
                f"heading ({heading.element_id}) is on page {heading.page_number}, outside page_span {(start, end)}"
            )

    def _check_elements(self, start: int, end: int) -> None:
        """Require every element in ``[start, end]``, in strictly increasing document order, ids unique."""
        seen: set[str] = set()
        previous: DocumentElement | None = None
        for index, element in enumerate(self.elements):
            label = f"elements[{index}] ({element.element_id})"
            if not start <= element.page_number <= end:
                raise ValueError(f"{label} is on page {element.page_number}, outside page_span {(start, end)}")
            if previous is not None and _position(element) <= _position(previous):
                raise ValueError(
                    f"{label} is not after elements[{index - 1}] ({previous.element_id}) "
                    "in document order (page_number, reading_order)"
                )
            if element.element_id in seen:
                raise ValueError(f"elements has duplicate element_id {element.element_id!r}")
            seen.add(element.element_id)
            self._check_member(index, label, element)
            previous = element

    def _check_member(self, index: int, label: str, element: DocumentElement) -> None:
        """Require the heading, if listed, to be itself and first, and every element to share the scope.

        An element with the heading's id must equal the heading, or
        :meth:`to_markdown` would render it in the heading's place.
        """
        heading = self.heading
        if heading is not None and element.element_id == heading.element_id:
            if element != heading:
                raise ValueError(f"{label} has the heading's element_id but differs from heading")
            if index != 0:
                raise ValueError(f"{label} is the section heading, so it must be elements[0]")
        if element.scope_path != self.scope_path:
            raise ValueError(
                f"{label} has scope_path {element.scope_path!r}, but the section's scope_path is {self.scope_path!r}"
            )

    @property
    def tables(self) -> tuple[Table, ...]:
        """The section's tables, in document order."""
        return tuple(element for element in self.elements if isinstance(element, Table))

    @property
    def figures(self) -> tuple[Figure, ...]:
        """The section's figures, in document order."""
        return tuple(element for element in self.elements if isinstance(element, Figure))

    def to_markdown(self) -> str:
        """Render the heading (unless it is already in ``elements``), then each element, blank-line separated.

        Consecutive lists get a ``<!-- -->`` line between them so they stay separate lists.
        """
        elements: tuple[DocumentElement, ...] = self.elements
        heading = self.heading
        if heading is not None and all(element.element_id != heading.element_id for element in elements):
            elements = (heading, *elements)
        return "\n\n".join(_render_elements(elements))


def _index_by_id(elements: tuple[DocumentElement, ...]) -> dict[str, DocumentElement]:
    """Map each element id to its element, rejecting an id used twice in the document.

    A validated ``Page`` cannot hold another page's id (an id encodes its page),
    so this is defence in depth against a page whose validation was bypassed.
    """
    by_id: dict[str, DocumentElement] = {}
    for element in elements:
        if element.element_id in by_id:
            raise ValueError(f"duplicate element_id {element.element_id!r} across pages")
        by_id[element.element_id] = element
    return by_id


def _describe(element: DocumentElement | None) -> str:
    """Name what an element-id lookup found, for error messages."""
    if element is None:
        return "no such element"
    return f"a {type(element).__name__} on page {element.page_number}"


def _check_continuations(elements: tuple[DocumentElement, ...], by_id: dict[str, DocumentElement]) -> None:
    """Require every ``Table.continued_from_id`` to name a table on an earlier page, at most once.

    A split table has a single next part, so two tables continuing the same
    table would fork it; the first in document order keeps the link.
    """
    continued_by: dict[str, str] = {}
    for element in elements:
        if not isinstance(element, Table) or element.continued_from_id is None:
            continue
        source = element.continued_from_id
        target = by_id.get(source)
        if not isinstance(target, Table) or target.page_number >= element.page_number:
            raise ValueError(
                f"table {element.element_id}: continued_from_id {source!r} "
                f"must name a table on an earlier page, got {_describe(target)}"
            )
        if source in continued_by:
            raise ValueError(
                f"table {element.element_id}: table {source!r} is already continued by {continued_by[source]}"
            )
        continued_by[source] = element.element_id


def _section_members(section: Section) -> tuple[DocumentElement, ...]:
    """Return what belongs to ``section``: its elements, plus its heading when it is not among them."""
    heading = section.heading
    if heading is None or any(element.element_id == heading.element_id for element in section.elements):
        return section.elements
    return (heading, *section.elements)


def _where(position: tuple[int, int]) -> str:
    """Describe a ``(page_number, reading_order)`` position for error messages."""
    return f"page {position[0]} reading_order {position[1]}"


def _check_section_layout(sections: tuple[Section, ...]) -> None:
    """Require flat sections in document order: none shares an element (heading included) or interleaves.

    A section spans from its first to its last member in document order;
    each must start after the previous non-empty section ends. O(N) overall.
    """
    owner: dict[str, int] = {}
    previous: tuple[int, tuple[int, int]] | None = None  # (index, end position) of the last non-empty section
    for index, section in enumerate(sections):
        members = _section_members(section)
        for element in members:
            first = owner.setdefault(element.element_id, index)
            if first != index:
                raise ValueError(
                    f"{element.element_id!r} is in sections[{first}] and sections[{index}]: "
                    "an element belongs to at most one section"
                )
        if not members:
            continue
        positions = [_position(element) for element in members]
        start = min(positions)
        if previous is not None and start <= previous[1]:
            raise ValueError(
                f"sections[{index}] starts at {_where(start)}, not after sections[{previous[0]}] ends at "
                f"{_where(previous[1])}: sections must be in document order and must not interleave"
            )
        previous = (index, max(positions))


def _check_member(where: str, element: DocumentElement, by_id: dict[str, DocumentElement]) -> None:
    """Require ``element`` to equal the document's element with its id (``where`` names it in errors)."""
    own = by_id.get(element.element_id)
    if own is None:
        raise ValueError(f"{where} {element.element_id!r} is not in the document")
    if own != element:
        raise ValueError(f"{where} {element.element_id!r} differs from the document's element with that id")


@dataclass(frozen=True, slots=True, kw_only=True)
class _DocumentIndex:
    """The values ``PDFDocument`` derives from its pages, computed once (see its properties)."""

    elements: tuple[DocumentElement, ...]
    headings: tuple[Heading, ...]
    paragraphs: tuple[Paragraph, ...]
    key_values: tuple[KeyValue, ...]
    tables: tuple[Table, ...]
    lists: tuple[BulletList, ...]
    figures: tuple[Figure, ...]
    page_images: tuple[PageImage, ...]
    degraded_pages: tuple[int, ...]
    integrity_verified: bool


def _build_index(pages: tuple[Page, ...], elements: tuple[DocumentElement, ...]) -> _DocumentIndex:
    """Derive the document's collections and page facts (one pass per collection)."""
    # An error page yielded nothing, so nothing on it was verified: the document is not.
    has_error_page = any(page.status == "error" for page in pages)
    verifiable = [page for page in pages if page.route != "vision"]
    return _DocumentIndex(
        elements=elements,
        headings=tuple(element for element in elements if isinstance(element, Heading)),
        paragraphs=tuple(element for element in elements if isinstance(element, Paragraph)),
        key_values=tuple(element for element in elements if isinstance(element, KeyValue)),
        tables=tuple(element for element in elements if isinstance(element, Table)),
        lists=tuple(element for element in elements if isinstance(element, BulletList)),
        figures=tuple(element for element in elements if isinstance(element, Figure)),
        page_images=tuple(page.page_image for page in pages if page.page_image is not None),
        degraded_pages=tuple(page.page_number for page in pages if page.status != "ok"),
        integrity_verified=(
            not has_error_page and bool(verifiable) and all(page.integrity_verified for page in verifiable)
        ),
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class PDFDocument:
    """A whole extracted PDF: metadata, pages in order, and optional sections.

    ``elements``, ``tables``, ``degraded_pages`` and the other derived values
    are read-only properties, not fields: they are computed once at
    construction into a private index (excluded from equality and repr;
    ``dataclasses.replace`` re-derives it), so each access is O(1).
    ``dataclasses.asdict`` is not a serialization format for this class.
    Validation and derivation are O(N) in time and memory, N being the total
    number of elements (plus the size of each section element compared
    against the document's element with its id).

    Attributes:
        metadata: Identity of the source PDF; ``page_count == len(pages)``.
        pages: The pages, numbered 1..N in order.
        sections: Flat, heading-scoped runs of the document's own elements, in
            document order, no element in two sections, each ending by the
            last page (nesting is expressed by ``scope_path`` prefixes).
    """

    metadata: DocumentMetadata
    pages: tuple[Page, ...]
    sections: tuple[Section, ...] = ()
    _index: _DocumentIndex = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.metadata, DocumentMetadata):
            raise TypeError(f"metadata must be a DocumentMetadata, got {type(self.metadata).__name__}")
        _check_instances("pages", self.pages, (Page,), "Page")
        _check_instances("sections", self.sections, (Section,), "Section")
        self._check_page_numbers()
        elements = tuple(element for page in self.pages for element in page.elements)
        by_id = _index_by_id(elements)
        _check_continuations(elements, by_id)
        self._check_sections(by_id)
        object.__setattr__(self, "_index", _build_index(self.pages, elements))

    def _check_page_numbers(self) -> None:
        """Require ``len(pages) == metadata.page_count`` and pages numbered 1..N in order."""
        if len(self.pages) != self.metadata.page_count:
            raise ValueError(
                f"pages has {len(self.pages)} page(s), but metadata.page_count is {self.metadata.page_count}"
            )
        for index, page in enumerate(self.pages):
            if page.page_number != index + 1:
                raise ValueError(f"pages[{index}] has page_number {page.page_number}, expected {index + 1}")

    def _check_sections(self, by_id: dict[str, DocumentElement]) -> None:
        """Require each section to end by the last page, hold only (equals of) the document's elements, and lie flat."""
        last_page = self.metadata.page_count
        for index, section in enumerate(self.sections):
            if section.page_span[1] > last_page:
                raise ValueError(
                    f"sections[{index}] page_span {section.page_span} ends after the document's last page {last_page}"
                )
            if section.heading is not None:
                _check_member(f"sections[{index}] heading", section.heading, by_id)
            for element in section.elements:
                _check_member(f"sections[{index}] element", element, by_id)
        _check_section_layout(self.sections)

    @property
    def elements(self) -> tuple[DocumentElement, ...]:
        """Every page's elements, in document order."""
        return self._index.elements

    @property
    def headings(self) -> tuple[Heading, ...]:
        """The ``Heading`` elements, in document order."""
        return self._index.headings

    @property
    def paragraphs(self) -> tuple[Paragraph, ...]:
        """The ``Paragraph`` elements, in document order."""
        return self._index.paragraphs

    @property
    def key_values(self) -> tuple[KeyValue, ...]:
        """The ``KeyValue`` elements, in document order."""
        return self._index.key_values

    @property
    def tables(self) -> tuple[Table, ...]:
        """The ``Table`` elements, in document order."""
        return self._index.tables

    @property
    def lists(self) -> tuple[BulletList, ...]:
        """The ``BulletList`` elements, in document order."""
        return self._index.lists

    @property
    def figures(self) -> tuple[Figure, ...]:
        """The ``Figure`` elements, in document order."""
        return self._index.figures

    @property
    def page_images(self) -> tuple[PageImage, ...]:
        """Every page's ``page_image`` that is not None, in page order."""
        return self._index.page_images

    @property
    def degraded_pages(self) -> tuple[int, ...]:
        """1-based numbers of pages whose status is not ``"ok"`` (error, ocr_fallback, ocr_unavailable)."""
        return self._index.degraded_pages

    @property
    def integrity_verified(self) -> bool:
        """False if any page is an error page; else True iff >= 1 non-vision page exists and all are verified."""
        return self._index.integrity_verified

    def to_markdown(self) -> str:
        """Render every page's Markdown, blank-line separated (empty for a zero-page document)."""
        return "\n\n".join(page.to_markdown() for page in self.pages)
