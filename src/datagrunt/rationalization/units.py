"""The units rationalize() sends to models: a document's visible elements, grouped, rendered and packed (plan §4.2).

:func:`build_units` turns a v5 :class:`~datagrunt.pdf.PDFDocument` into a :class:`UnitPlan`. The renderer it is
given turns one element into the block a model reads (T3's envelope passes the real one), so the size limit
applies to exactly what is sent; units carry their blocks, so T3 renders nothing again. The renderer must be pure:
it is called more than once for the same rows, for a whole element and then for candidate parts of its split. The
document is never changed (invariant 2): a unit holds the document's own element objects, and a split element's
parts are copies made by :func:`dataclasses.replace`. The rules, in order:

1. **Visible filter** (R7, invariant 1). An element with any ``hidden_reasons``, whichever of the nine, is left
   out and never passed to ``render``. It counts once under each of its reasons, in one unit (or skipped unit) and
   in the plan's totals: in the unit holding the visible element before it in its group, or in its group's first
   unit when none comes before it.
2. **Scope paths** (invariant 1). A unit's scope path is its group's, keeping, in order, only the entries whose
   opening heading is visible. A section's last entry is opened by its own ``heading`` when it has one, by
   identity, wherever that heading stands (the document records it: its path is the section's). Any other entry
   ``k`` (an ancestor, or the last entry of a section without a heading) is opened by the last heading, in document
   order at or before the section's first member, whose own ``scope_path`` is the section's first ``k`` entries,
   found by recorded path and position; a heading's path ends with its text, so that text is the entry. In the
   pipeline these are the headings the scope pass pushed (``scope.py`` gives each element the texts of the open
   visible headings: a visible heading records the path it opens, a hidden one, which opens nothing, the current
   path and its text), and each section's heading is visible and its first member. No entry is matched by its text
   alone: a visible heading with the same text on another branch, or later in the document, opens nothing here.
   Every other entry is dropped and counted in the unit's ``scope_entries_excluded``: one a hidden heading opened
   (a hidden section heading, or a hidden heading recording an ancestor's path last), which a hand-built or loaded
   document can put in a scope path, one a heading on an image-only page opened (rule 4: such a heading is read as
   a hidden one is, so it opens nothing, its text unsent), or one no heading opened (a path no heading records by
   then). A hidden or image-only ancestor drops only its own entry. The pipeline opens sections at visible headings
   only, so its paths pass whole. :func:`group_scopes` is the rule, read once per section, and T3's envelope reads it
   too. A heading's own path serves only to find openers: no element's ``scope_path`` reaches a unit, and T3 must
   render a unit's ``scope_path`` only, never an element's.
3. **Groups** (R5). One group per :class:`~datagrunt.pdf.elements.Section`, whose members are its elements and its
   heading (as the document counts them). An element in no section joins its page's group, whose scope path is
   ``()``. The pipeline leaves only hidden headings in no section (``scope.py``): a page group of them is an
   all-hidden skipped unit whose id range can lie inside another unit's span, and each of its counts still lands
   once. Units come in the document order of their first elements, ``(page_number, reading_order)``, then part: a
   section's units and a page group's interleave only where the groups do, in a hand-built document.
4. **Skipped units**, reported rather than dropped (invariant 5), in the document order of their pages and first
   elements. An error page (``status == "error"``, no elements) is a :class:`SkippedUnit` of reason
   ``"page_error"``, and a group whose members are all hidden one of reason ``"all_hidden"``. An image-only page
   (non-empty ``Page.image_only_reasons``: its text could not stand and nothing read its image, so its content
   lives only in its page image) that is not an error page is one of reason ``"image_only"``, at its place as an
   error page's: none of its elements, visible or hidden, joins a group, is rendered or reaches a unit, its hidden
   ones count on it alone, and with ``coalesce_sections`` it closes the open unit as an error page does. An error
   page is marked ``"extraction_failed"`` too and stays ``"page_error"``; a page whose reasons are ``()`` (its text
   stands, or OCR read it) or ``None`` (not recorded, a document of datagrunt 5.0-5.2) is read as before. Reading
   an image-only page's image is deferred (#473): the skipped unit names the page to read. Text is what a heading,
   paragraph, key-value or list always carries, a figure's non-blank caption, or a table's non-blank header or
   cell. A group whose visible members carry none (only figures without a caption or with a blank one, or tables
   of blank cells) is one of reason ``"no_text"``, never rendered: an ``ocr_unavailable`` scan page of a document
   that does not record ``image_only_reasons``, in a section of its own, say. So is a unit packed from a group with
   text whose own elements carry none (such a scan page's figure that packs alone, the blank parts of a split blank
   table): it is cut from what is sent, named and counted as a unit would be. Each carries its hidden counts (a
   group's, the unit's, or an image-only page's). An empty group yields nothing.
5. **Packing.** A group's visible elements are rendered in order, one block each (``render`` must return a
   non-empty ``str``: T3's real blocks carry the ``<el>`` framing), and the blocks are packed greedily: a unit's
   body is its blocks joined by :data:`BLOCK_SEPARATOR`, and a block starts a new unit when adding it would take
   the body past ``max_unit_chars``.
6. **Splitting.** A :class:`~datagrunt.pdf.elements.Table` or :class:`~datagrunt.pdf.elements.BulletList` whose
   block alone is past the limit splits into parts of consecutive rows (each part keeping every header) or items.
   A list part keeps its items' markers: the recorded ones, or, for an ordered list without any, the ones the whole
   list renders for those positions (``3.``, ``4.``), so a part shows its items' true numbers; an unordered list
   without markers stays without. A part's Markdown form can still differ from the whole list's, because whether
   its markers show as list numbers (``_shown_as_written``) is judged per part: the markers and items it shows are
   the whole list's, so nothing a model reads changes beyond the form. Each part is the largest that renders within
   the limit, found by doubling then halving its size, which assumes a block grows with its rows or items (the real
   renderer's does); every part is rendered and checked, so none is past the limit unless it holds one row or
   item. A part is a block like any other: a short last part shares its unit with what follows it. Headings,
   paragraphs, key-values and figures are never split: a split would put one element id in two units and break
   quote grounding at the cut.
7. **Oversize.** A block past the limit on its own (a text element, a figure, a table that cannot split, one row,
   one item) goes whole into its own unit, flagged ``oversize``. Nothing is ever truncated.
8. **Ids.** ``<doc_id>:<first>-<last>``: the ids of the unit's first and last elements, each followed by
   ``.<part>`` (1-based) when that end is a part of a split element (``doc:p0001_e003.2-p0001_e004``). A page
   error's or an image-only page's is ``<doc_id>:p<page>``, the page number in 4 or more digits as in element ids;
   an all-hidden or no-text group's names its first and last members. Ids are deterministic and unique within a
   document, and depend on the renderer only through the sizes of its blocks.

Every loop is linear in the document's elements, rows and items (a split renders O(log n) candidate parts for each
part it makes) and in its headings' and sections' scope paths (a trie of the headings' paths, never a copy of a
prefix), and nothing recurses. The module imports only the standard library and the element model: no
pydantic, no httpx, nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

import dataclasses
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeAlias, TypeGuard, get_args

from datagrunt.pdf.elements import (
    HIDDEN_REASONS,
    BBox,
    BulletList,
    DocumentElement,
    Figure,
    Heading,
    HiddenReason,
    Page,
    PDFDocument,
    Section,
    Table,
    _markdown_literal,
    _shown_as_written,
)

BLOCK_SEPARATOR = "\n"
"""What joins a unit's blocks into its body: one separator between two blocks, counted in ``body_chars``."""

HiddenCounts: TypeAlias = tuple[tuple[HiddenReason, int], ...]
"""Excluded elements per hidden reason: ``(reason, count)`` pairs in ``HIDDEN_REASONS`` order, zeros omitted."""

PartOf: TypeAlias = tuple[int, int] | None
"""``(part_index, part_count)`` of a split element's part (1-based, at least two parts), or ``None`` if whole."""

SkipReason: TypeAlias = Literal["all_hidden", "no_text", "page_error", "image_only"]
"""Why a skipped unit has nothing to send: every member hidden, no visible text, a page nothing was read from, or a
page whose content lives only in its page image (``Page.image_only_reasons``), which no model reads yet."""

Renderer: TypeAlias = Callable[[DocumentElement], str]
"""Turns one visible element (or a part of one) into the non-empty block a model reads.

It must be pure, its block a function of the element alone: it is called more than once for the same rows (the
whole element, then candidate parts while it splits), and the plan keeps the blocks it returned last for each part.
"""

_SKIP_REASONS: tuple[SkipReason, ...] = get_args(SkipReason)
_ELEMENT_TYPES: tuple[type[DocumentElement], ...] = get_args(DocumentElement)
_SPLITTABLE: tuple[type, ...] = (Table, BulletList)
_CM_NUMBER = re.compile(r"[0-9]{1,9}[.)]")
_ITEM_BREAK = re.compile(r"\r\n|\r|\n")


def _check_text(name: str, value: object) -> None:
    """Require a non-empty ``str``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    if not value:
        raise ValueError(f"{name} must be non-empty")


def _is_int(value: object) -> TypeGuard[int]:
    """True for an ``int`` that is not a ``bool``."""
    return isinstance(value, int) and not isinstance(value, bool)


def _check_count(name: str, value: object) -> None:
    """Require an ``int`` (not a ``bool``) >= 0."""
    if not _is_int(value):
        raise TypeError(f"{name} must be an int, got {value!r} ({type(value).__name__})")
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def _check_bool(name: str, value: object) -> None:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, got {value!r} ({type(value).__name__})")


def _check_tuple_of(name: str, value: object, kinds: type | tuple[type, ...]) -> None:
    """Require a tuple whose every entry is an instance of ``kinds``."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(value).__name__}")
    for index, item in enumerate(value):
        if not isinstance(item, kinds):
            raise TypeError(f"{name}[{index}] has the wrong type: {type(item).__name__}")


def _check_pages(name: str, value: object) -> None:
    """Require a non-empty tuple of page numbers (ints >= 1) in strictly increasing order."""
    if not isinstance(value, tuple) or not all(_is_int(number) for number in value):
        raise TypeError(f"{name} must be a tuple of int page numbers, got {value!r}")
    if not value or value[0] < 1:
        raise ValueError(f"{name} must be non-empty page numbers >= 1, got {value!r}")
    if any(later <= earlier for earlier, later in zip(value, value[1:])):
        raise ValueError(f"{name} must strictly increase, got {value!r}")


def _check_counts(name: str, value: object) -> None:
    """Require ``(reason, count)`` pairs: known reasons, each once and in ``HIDDEN_REASONS`` order, counts >= 1."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple of (reason, count) pairs, got {type(value).__name__}")
    previous = -1
    for pair in value:
        if not isinstance(pair, tuple) or len(pair) != 2 or not _is_int(pair[1]):
            raise TypeError(f"{name} must hold (reason, int count) pairs, got {pair!r}")
        reason, count = pair
        if reason not in HIDDEN_REASONS:
            raise ValueError(f"{name} names an unknown reason {reason!r}; known: {HIDDEN_REASONS}")
        if count < 1:
            raise ValueError(f"{name} omits zero counts, got {pair!r}")
        position = HIDDEN_REASONS.index(reason)
        if position <= previous:
            raise ValueError(f"{name} must name each reason once, in HIDDEN_REASONS order, got {value!r}")
        previous = position


def _check_parts(elements: tuple[DocumentElement, ...], parts: object) -> None:
    """Require one :data:`PartOf` per element (see :func:`_check_part`)."""
    if not isinstance(parts, tuple):
        raise TypeError(f"part_of must be a tuple, got {type(parts).__name__}")
    if len(parts) != len(elements):
        raise ValueError(f"part_of must hold one entry per element ({len(elements)}), got {len(parts)}")
    for element, part in zip(elements, parts):
        _check_part(element, part)


def _check_part(element: DocumentElement, part: object) -> None:
    """Require ``None``, or ``(index, count)`` with ``1 <= index <= count`` and ``count >= 2`` on a table or list.

    Only a table or a list splits, so only one of theirs can be a part.
    """
    if part is None:
        return
    if not isinstance(part, tuple) or len(part) != 2 or not all(_is_int(number) for number in part):
        raise TypeError(f"part_of entries must be None or (part_index, part_count) int pairs, got {part!r}")
    index, count = part
    if not 1 <= index <= count or count < 2:
        raise ValueError(f"part_of entry {part!r} must satisfy 1 <= part_index <= part_count, part_count >= 2")
    if not isinstance(element, _SPLITTABLE):
        raise ValueError(f"only a Table or BulletList has parts; {element.element_id} has part_of {part!r}")


def _check_elements(elements: object) -> None:
    """Require a non-empty tuple of document elements, none of them hidden (invariant 1)."""
    if not isinstance(elements, tuple):
        raise TypeError(f"elements must be a tuple, got {type(elements).__name__}")
    if not elements:
        raise ValueError("a unit holds at least one element")
    for index, element in enumerate(elements):
        if not isinstance(element, _ELEMENT_TYPES):
            raise TypeError(f"elements[{index}] must be a document element, got {type(element).__name__}")
        if element.hidden_reasons:
            raise ValueError(f"a unit holds visible elements only; {element.element_id} is {element.hidden_reasons}")


def _end_id(element: DocumentElement, part: PartOf) -> str:
    """One end of a unit id: the element id, with ``.<part>`` when the element is a part of a split one."""
    return element.element_id if part is None else f"{element.element_id}.{part[0]}"


def _unit_id(doc_id: str, elements: Sequence[DocumentElement], parts: Sequence[PartOf]) -> str:
    """``<doc_id>:<first>-<last>`` (module notes, 8)."""
    return f"{doc_id}:{_end_id(elements[0], parts[0])}-{_end_id(elements[-1], parts[-1])}"


def _pages(elements: Sequence[DocumentElement]) -> tuple[int, ...]:
    """The sorted, distinct page numbers of ``elements``."""
    return tuple(sorted({element.page_number for element in elements}))


def _body_chars(blocks: Sequence[str]) -> int:
    """The length of the blocks joined by :data:`BLOCK_SEPARATOR`."""
    return sum(len(block) for block in blocks) + len(BLOCK_SEPARATOR) * max(len(blocks) - 1, 0)


def _counts(counter: Mapping[str, int]) -> HiddenCounts:
    """``counter``'s non-zero counts as ``(reason, count)`` pairs, in ``HIDDEN_REASONS`` order."""
    return tuple((reason, counter[reason]) for reason in HIDDEN_REASONS if counter.get(reason, 0))


def _count(counter: dict[str, int], reasons: Sequence[str]) -> None:
    """Count one element under each of its ``reasons``."""
    for reason in reasons:
        counter[reason] = counter.get(reason, 0) + 1


@dataclass(frozen=True, slots=True, kw_only=True)
class Unit:
    """A run of one group's visible elements, rendered, to send to a model as one request.

    Args:
        unit_id: ``<doc_id>:<first>-<last>`` (module notes, 8); it must be the id ``elements`` and ``part_of`` give.
        doc_id: The document's id (``metadata.doc_id``).
        elements: The visible elements, in group order: the document's own objects, a split element's parts as
            their copies. A part keeps its whole table's or list's ``bbox``: its box is the element's (T12's
            provenance says so), not the box of its own rows.
        blocks: Each element's rendered block, in the same order.
        part_of: For each element, ``None``, or ``(part_index, part_count)`` of the split it is a part of.
        scope_path: The group's scope path, its section's (``()`` for a page group), keeping only the entries a
            visible heading opened (module notes, 2). T3 renders this, never an element's own ``scope_path``.
        scope_entries_excluded: How many entries of the group's scope path were dropped from ``scope_path``.
        page_numbers: The distinct pages of ``elements``, sorted.
        vision: Whether any of those pages has route ``"vision"`` (it selects the grounding mode, plan §4.8).
        oversize: Whether the unit is one block past ``max_unit_chars`` (module notes, 7).
        hidden_excluded: The hidden elements counted in this unit, per reason (module notes, 1).

    Attributes:
        body_chars: The body's length: the blocks plus one :data:`BLOCK_SEPARATOR` between each two (derived).

    Raises:
        TypeError: If a field has the wrong type (an element that is not a document element, a block that is not
            a ``str``, a list for a tuple, a non-``bool`` flag, ...).
        ValueError: If ``elements`` is empty or holds a hidden element, ``blocks`` or ``part_of`` does not match
            it, no element carries text, a block is empty, a part is out of range or not a table's or list's,
            ``unit_id`` or
            ``page_numbers`` is not the one the elements give, ``scope_entries_excluded`` is negative, ``oversize``
            is set on more than one block, or ``hidden_excluded`` is malformed.
    """

    unit_id: str
    doc_id: str
    elements: tuple[DocumentElement, ...]
    blocks: tuple[str, ...]
    part_of: tuple[PartOf, ...]
    scope_path: tuple[str, ...]
    scope_entries_excluded: int
    page_numbers: tuple[int, ...]
    vision: bool
    oversize: bool
    hidden_excluded: HiddenCounts
    body_chars: int = field(init=False)

    def __post_init__(self) -> None:
        self._check_content()
        self._check_facts()
        object.__setattr__(self, "body_chars", _body_chars(self.blocks))

    def _check_content(self) -> None:
        """Check the ids, the elements, their blocks and their parts."""
        _check_text("unit_id", self.unit_id)
        _check_text("doc_id", self.doc_id)
        _check_elements(self.elements)
        _check_tuple_of("blocks", self.blocks, str)
        if len(self.blocks) != len(self.elements):
            raise ValueError(f"blocks must hold one block per element ({len(self.elements)}), got {len(self.blocks)}")
        if not all(self.blocks):
            raise ValueError("a unit's blocks are non-empty: each carries an element to send")
        _check_parts(self.elements, self.part_of)
        if not any(_has_text(element) for element in self.elements):
            raise ValueError("a unit carries text: one with none is a no_text SkippedUnit")
        expected = _unit_id(self.doc_id, self.elements, self.part_of)
        if self.unit_id != expected:
            raise ValueError(f"unit_id must be {expected!r} for these elements, got {self.unit_id!r}")

    def _check_facts(self) -> None:
        """Check the scope path, pages, flags and hidden counts."""
        _check_tuple_of("scope_path", self.scope_path, str)
        _check_count("scope_entries_excluded", self.scope_entries_excluded)
        _check_pages("page_numbers", self.page_numbers)
        if self.page_numbers != _pages(self.elements):
            raise ValueError(
                f"page_numbers must be {_pages(self.elements)} for these elements, got {self.page_numbers}"
            )
        _check_bool("vision", self.vision)
        _check_bool("oversize", self.oversize)
        if self.oversize and len(self.blocks) != 1:
            raise ValueError(f"an oversize unit holds exactly one block, got {len(self.blocks)}")
        _check_counts("hidden_excluded", self.hidden_excluded)


@dataclass(frozen=True, slots=True, kw_only=True)
class SkippedUnit:
    """A group or page with nothing to send, reported rather than dropped (invariant 5).

    Args:
        unit_id: ``<doc_id>:p<page>`` for a page error or an image-only page, ``<doc_id>:<first>-<last>`` for a
            group (its first and last members) or for a unit cut for having no text (as the unit's id would be).
        reason: ``"page_error"`` (a page nothing was read from), ``"all_hidden"`` (every member hidden),
            ``"no_text"`` (no visible member carries text) or ``"image_only"`` (a page whose content lives only in
            its page image, ``Page.image_only_reasons``: none of its text is sent, and no model reads its image
            yet, so a caller reads ``page_numbers`` to find the page; module notes, 4).
        page_numbers: The pages it covers, sorted and distinct: exactly one for a page error or an image-only page;
            a group's members' pages, hidden ones included; a cut unit's elements' pages, as a unit's.
        hidden_excluded: The hidden members counted here, per reason: empty for a page error, non-empty for an
            all-hidden group, either for a no-text group or an image-only page (that page's hidden elements).

    Raises:
        TypeError: If ``unit_id`` is not a ``str``, or ``page_numbers`` or ``hidden_excluded`` is malformed in type.
        ValueError: If ``unit_id`` is empty, ``reason`` unknown, ``page_numbers`` empty, unsorted or below 1, or
            the pages or counts do not fit ``reason``.
    """

    unit_id: str
    reason: SkipReason
    page_numbers: tuple[int, ...]
    hidden_excluded: HiddenCounts = ()

    def __post_init__(self) -> None:
        _check_text("unit_id", self.unit_id)
        if self.reason not in _SKIP_REASONS:
            raise ValueError(f"reason must be one of {_SKIP_REASONS}, got {self.reason!r}")
        _check_pages("page_numbers", self.page_numbers)
        _check_counts("hidden_excluded", self.hidden_excluded)
        if self.reason == "page_error" and (len(self.page_numbers) != 1 or self.hidden_excluded):
            raise ValueError(f"a page_error unit covers one page and no hidden element, got {self!r}")
        if self.reason == "image_only" and len(self.page_numbers) != 1:
            raise ValueError(f"an image_only unit covers one page, got page_numbers={self.page_numbers}")
        if self.reason == "all_hidden" and not self.hidden_excluded:
            raise ValueError(
                f"an all_hidden unit counts its hidden members, got hidden_excluded={self.hidden_excluded}"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class UnitPlan:
    """What :func:`build_units` makes of one document: its units, its skipped units and its hidden totals.

    Args:
        units: The units, in document order of their first elements (module notes, 3).
        skipped: The skipped units, in document order.
        hidden_excluded_total: Every excluded element counted per reason: the sum of the units' and skipped units'
            counts.
        hidden_elements_total: How many elements were excluded (an element with two reasons counts once here).
        scope_entries_excluded_total: The units' ``scope_entries_excluded``, summed: a group's dropped entry counts
            once for each unit it would have reached.

    Raises:
        TypeError: If ``units`` or ``skipped`` is not a tuple of its type, or a total not an int.
        ValueError: If an id repeats, the units name two documents, or the totals disagree with the counts.
    """

    units: tuple[Unit, ...]
    skipped: tuple[SkippedUnit, ...]
    hidden_excluded_total: HiddenCounts
    hidden_elements_total: int
    scope_entries_excluded_total: int

    def __post_init__(self) -> None:
        _check_tuple_of("units", self.units, Unit)
        _check_tuple_of("skipped", self.skipped, SkippedUnit)
        _check_counts("hidden_excluded_total", self.hidden_excluded_total)
        if not _is_int(self.hidden_elements_total):
            raise TypeError(f"hidden_elements_total must be an int, got {self.hidden_elements_total!r}")
        self._check_ids()
        self._check_totals()

    def _check_ids(self) -> None:
        """Require unique ids across units and skipped units, and one document across the units."""
        seen: set[str] = set()
        for unit_id in [*(unit.unit_id for unit in self.units), *(item.unit_id for item in self.skipped)]:
            if unit_id in seen:
                raise ValueError(f"unit id {unit_id!r} is repeated")
            seen.add(unit_id)
        doc_ids = {unit.doc_id for unit in self.units}
        if len(doc_ids) > 1:
            raise ValueError(f"the units of one plan share one doc_id, got {sorted(doc_ids)}")

    def _check_totals(self) -> None:
        """Require the totals to be the sum of the counts, and the element total to fit them."""
        summed: dict[str, int] = {}
        every = [*(unit.hidden_excluded for unit in self.units), *(item.hidden_excluded for item in self.skipped)]
        for pairs in every:
            for reason, count in pairs:
                summed[reason] = summed.get(reason, 0) + count
        if _counts(summed) != self.hidden_excluded_total:
            raise ValueError(f"hidden_excluded_total must be {_counts(summed)}, got {self.hidden_excluded_total}")
        counts = [count for _, count in self.hidden_excluded_total]
        low, high = max(counts, default=0), sum(counts)
        if not low <= self.hidden_elements_total <= high:
            raise ValueError(f"hidden_elements_total must lie in [{low}, {high}], got {self.hidden_elements_total}")
        _check_count("scope_entries_excluded_total", self.scope_entries_excluded_total)
        dropped = sum(unit.scope_entries_excluded for unit in self.units)
        if self.scope_entries_excluded_total != dropped:
            raise ValueError(f"scope_entries_excluded_total must be {dropped}, got {self.scope_entries_excluded_total}")


@dataclass(slots=True)
class _Draft:
    """A unit while it is packed: its group's scope path (kept entries, dropped count), its elements, blocks and
    parts so far, its body size and its hidden counts."""

    scope_path: tuple[str, ...]
    scope_excluded: int
    oversize: bool
    hidden: dict[str, int]
    elements: list[DocumentElement] = field(default_factory=list)
    blocks: list[str] = field(default_factory=list)
    parts: list[PartOf] = field(default_factory=list)
    size: int = 0

    def fits(self, block: str, limit: int) -> bool:
        """Whether ``block`` joins this draft within ``limit``: never an oversize one, already past it."""
        return self.size + len(BLOCK_SEPARATOR) + len(block) <= limit

    def add(self, element: DocumentElement, block: str, part: PartOf) -> None:
        self.size += len(block) + (len(BLOCK_SEPARATOR) if self.blocks else 0)
        self.elements.append(element)
        self.blocks.append(block)
        self.parts.append(part)


def _render(render: Renderer, element: DocumentElement) -> str:
    """``render(element)``, which must be a non-empty ``str``.

    Raises:
        TypeError: If it is not a ``str``, naming the element.
        ValueError: If it is empty, naming the element.
    """
    block: object = render(element)
    if not isinstance(block, str):
        raise TypeError(f"render must return a str, got {type(block).__name__} for element {element.element_id}")
    if not block:
        raise ValueError(f"render returned an empty block for element {element.element_id}: a block carries text")
    return block


def _has_text(element: DocumentElement) -> bool:
    """Whether a visible element carries text: a figure a non-blank caption, a table a non-blank cell.

    Headings, paragraphs, key-values and lists always do: the element model refuses them blank.
    """
    if isinstance(element, Figure):
        return element.caption is not None and bool(element.caption.strip())
    if isinstance(element, Table):
        cells = (cell for row in (element.headers, *element.rows) for cell in row)
        return any(cell.strip() for cell in cells)
    return True


def _list_markers(listing: BulletList, start: int, stop: int) -> tuple[str, ...]:
    """The markers of items ``[start, stop)``: as recorded, else an ordered list's positions, else none.

    An ordered list without markers renders its items ``1.``, ``2.``, ... (``BulletList.to_markdown``), so a part
    gets the markers its items have in the whole list, and shows their true numbers rather than restarting at 1.
    """
    if listing.markers or not listing.ordered:
        return listing.markers[start:stop]
    return tuple(f"{position}." for position in range(start + 1, stop + 1))


def _part(element: Table | BulletList, start: int, stop: int) -> Table | BulletList:
    """A copy of ``element`` with rows (or items and their markers) ``[start, stop)``, every other field kept."""
    if isinstance(element, Table):
        return dataclasses.replace(element, rows=element.rows[start:stop])
    return dataclasses.replace(element, items=element.items[start:stop], markers=_list_markers(element, start, stop))


def _rendered_part(
    element: Table | BulletList, start: int, stop: int, render: Renderer
) -> tuple[Table | BulletList, str]:
    piece = _part(element, start, stop)
    return piece, _render(render, piece)


def _largest_part(
    element: Table | BulletList, start: int, length: int, limit: int, render: Renderer
) -> tuple[int, Table | BulletList, str]:
    """The largest part from ``start`` whose block fits, as ``(stop, part, block)``; one row or item at least.

    The size doubles until a block does not fit, then halves between ``fit`` (known to fit) and ``over`` (known
    not to; ``length + 1`` while none has failed): O(log n) renders.

    Raises:
        RuntimeError: If the search outruns its bound (unreachable: each step narrows ``over - fit``).
    """
    first = _rendered_part(element, start, start + 1, render)
    if len(first[1]) > limit:
        return start + 1, first[0], first[1]
    fit, best, over = start + 1, first, length + 1
    for _ in range(2 * length.bit_length() + 2):
        if over - fit <= 1:
            return fit, best[0], best[1]
        probe = min(2 * fit - start, length) if over > length else (fit + over) // 2
        candidate = _rendered_part(element, start, probe, render)
        if len(candidate[1]) > limit:
            over = probe
        else:
            fit, best = probe, candidate
    raise RuntimeError(f"splitting {element.element_id} outran its bound at {start}")


def _split(element: Table | BulletList, limit: int, render: Renderer) -> list[tuple[Table | BulletList, str]]:
    """Cut a table's rows or a list's items into consecutive parts, each the largest that fits (module notes, 6)."""
    length = len(element.rows) if isinstance(element, Table) else len(element.items)
    parts: list[tuple[Table | BulletList, str]] = []
    start = 0
    for _ in range(length):  # each part holds at least one row or item
        if start >= length:
            break
        start, piece, block = _largest_part(element, start, length, limit, render)
        parts.append((piece, block))
    return parts


def _pack_spans(entry_lens: Sequence[int], base_len: int, sep_len: int, limit: int) -> list[tuple[int, int]]:
    """Greedily partition ``0..len(entry_lens)`` into maximal ``(start, stop)`` slices within ``limit``."""
    if not entry_lens or limit < 1:
        raise ValueError("entry_lens must be non-empty and limit >= 1")
    spans: list[tuple[int, int]] = []
    start = 0
    current = base_len + entry_lens[0]
    for idx in range(1, len(entry_lens)):
        grown = current + sep_len + entry_lens[idx]
        if current <= limit and grown <= limit:
            current = grown
            continue
        spans.append((start, idx))
        start = idx
        current = base_len + entry_lens[idx]
    spans.append((start, len(entry_lens)))
    return spans


def _split_table_monotone(
    table: Table,
    block: str,
    limit: int,
    render: Renderer,
) -> list[tuple[Table | BulletList, str]]:
    """Split an oversize ``Table`` in ``O(R)`` under a monotone GFM element renderer, verifying each part."""
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")
    if len(table.rows) < 2:
        return []
    lines = block.split("\n")
    if len(lines) != len(table.rows) + 4:
        return _split(table, limit, render)
    open_tag, header_line, divider_line, close_tag = lines[0], lines[1], lines[2], lines[-1]
    row_lines = lines[3:-1]
    base_len = len(open_tag) + len(header_line) + len(divider_line) + len(close_tag) + 4
    spans = _pack_spans([len(line) for line in row_lines], base_len, 1, limit)
    if len(spans) < 2:
        return []
    parts: list[tuple[Table | BulletList, str]] = []
    for start, stop in spans:
        piece = dataclasses.replace(table, rows=table.rows[start:stop])
        expected = "\n".join((open_tag, header_line, divider_line, *row_lines[start:stop], close_tag))
        rendered = _render(render, piece)
        if rendered != expected:
            raise RuntimeError(f"monotone table split mismatch on {table.element_id}[{start}:{stop}]")
        parts.append((piece, rendered))
    return parts


def _slice_marker_text_items(listing: BulletList, body_lines: list[str]) -> list[str] | None:
    """Extract per-item escaped strings from a ``_marker_text_entries`` body (without inter-item ``\\``)."""
    count = len(listing.items)
    if len(body_lines) == count:
        if any(not line.endswith("\\") for line in body_lines[:-1]):
            return None
        return [*[line[:-1] for line in body_lines[:-1]], body_lines[-1]]
    entries: list[str] = []
    pos = 0
    for idx, (marker, item) in enumerate(zip(listing.markers, listing.items, strict=True)):
        span = len(_markdown_literal(f"{marker} {item}"))
        chunk = body_lines[pos : pos + span]
        if len(chunk) != span or (idx < count - 1 and not chunk[-1].endswith("\\")):
            return None
        if idx < count - 1:
            chunk = [*chunk[:-1], chunk[-1][:-1]]
        entries.append("\n".join(chunk))
        pos += span
    return entries if pos == len(body_lines) else None


def _slice_list_items(listing: BulletList, body_lines: list[str]) -> list[str] | None:
    """Extract per-item escaped strings from a ``_list_entries`` body."""
    count = len(listing.items)
    if len(body_lines) == count:
        return body_lines
    if listing.ordered and listing.markers:
        markers: Sequence[str] = listing.markers
    elif listing.ordered:
        markers = [f"{num}." for num in range(1, count + 1)]
    else:
        markers = ("-",) * count
    entries: list[str] = []
    pos = 0
    for marker, item in zip(markers, listing.items, strict=True):
        span = len(_markdown_literal(item)) + len(_ITEM_BREAK.findall(marker))
        chunk = body_lines[pos : pos + span]
        if len(chunk) != span:
            return None
        entries.append("\n".join(chunk))
        pos += span
    return entries if pos == len(body_lines) else None


def _list_item_entries(listing: BulletList, body_lines: list[str]) -> tuple[list[str], str] | None:
    """Return ``(escaped_entries, item_separator)`` when ``listing`` renders uniformly across all slices."""
    if listing.ordered and listing.markers and not _shown_as_written(listing.markers):
        if any(_CM_NUMBER.fullmatch(marker) is not None for marker in listing.markers):
            return None
        sliced = _slice_marker_text_items(listing, body_lines)
        return None if sliced is None else (sliced, "\\\n")
    sliced = _slice_list_items(listing, body_lines)
    return None if sliced is None else (sliced, "\n")


def _split_list_monotone(
    listing: BulletList,
    block: str,
    limit: int,
    render: Renderer,
) -> list[tuple[Table | BulletList, str]]:
    """Split an oversize ``BulletList`` in ``O(R)`` under a monotone GFM element renderer, verifying each part."""
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")
    if len(listing.items) < 2:
        return []
    lines = block.split("\n")
    if len(lines) < 3:
        return _split(listing, limit, render)
    open_tag, close_tag = lines[0], lines[-1]
    extracted = _list_item_entries(listing, lines[1:-1])
    if extracted is None:
        return _split(listing, limit, render)
    entries, sep = extracted
    base_len = len(open_tag) + len(close_tag) + 2
    spans = _pack_spans([len(entry) for entry in entries], base_len, len(sep), limit)
    if len(spans) < 2:
        return []
    parts: list[tuple[Table | BulletList, str]] = []
    for start, stop in spans:
        piece = _part(listing, start, stop)
        expected = f"{open_tag}\n{sep.join(entries[start:stop])}\n{close_tag}"
        rendered = _render(render, piece)
        if rendered != expected:
            raise RuntimeError(f"monotone list split mismatch on {listing.element_id}[{start}:{stop}]")
        parts.append((piece, rendered))
    return parts


def _is_builtin_element_renderer(render: Renderer) -> bool:
    """Return ``True`` when ``render`` is ``datagrunt.rationalization.envelope.render_element``."""
    return (
        callable(render)
        and getattr(render, "__name__", None) == "render_element"
        and getattr(render, "__module__", None) == "datagrunt.rationalization.envelope"
    )


def _pieces(element: DocumentElement, limit: int, render: Renderer) -> list[tuple[PartOf, DocumentElement, str]]:
    """The element's blocks: the element whole, or its parts when it is a table or list past the limit."""
    block = _render(render, element)
    if len(block) <= limit or not isinstance(element, (Table, BulletList)):
        return [(None, element, block)]
    if _is_builtin_element_renderer(render):
        parts = (
            _split_table_monotone(element, block, limit, render)
            if isinstance(element, Table)
            else _split_list_monotone(element, block, limit, render)
        )
    else:
        parts = _split(element, limit, render)
    if len(parts) < 2:  # one row or item, or a table of headers only: it cannot split
        return [(None, element, block)]
    count = len(parts)
    return [((index, count), piece, piece_block) for index, (piece, piece_block) in enumerate(parts, start=1)]


class _Group:
    """One group: its members, hidden ones included, read in a first pass, then packed into drafts in a second.

    The first pass (:meth:`take`) counts the hidden members and learns whether any visible one carries text; only a
    group with text is packed (:meth:`pack`), greedily and in order (module notes, 1 and 5-7). There a hidden member
    counts in the newest draft, the one holding the visible member before it; those met before the group's first
    visible member wait in ``leading`` and go to its first draft.
    """

    __slots__ = ("scope_path", "scope_excluded", "members", "hidden", "visible", "has_text", "drafts", "leading")

    def __init__(self, scope_path: tuple[str, ...], scope_excluded: int) -> None:
        self.scope_path = scope_path
        self.scope_excluded = scope_excluded
        self.members: list[DocumentElement] = []
        self.hidden: dict[str, int] = {}
        self.visible = False
        self.has_text = False
        self.drafts: list[_Draft] = []
        self.leading: dict[str, int] = {}

    def take(self, element: DocumentElement) -> None:
        """First pass: record the next member, counting it if hidden, noting whether it carries text if not."""
        self.members.append(element)
        if element.hidden_reasons:
            _count(self.hidden, element.hidden_reasons)
            return
        self.visible = True
        self.has_text = self.has_text or _has_text(element)

    def pack(self, element: DocumentElement, limit: int, render: Renderer) -> list[_Draft]:
        """Second pass: place the next member; return the drafts it opened, in order."""
        if element.hidden_reasons:
            _count(self.drafts[-1].hidden if self.drafts else self.leading, element.hidden_reasons)
            return []
        opened: list[_Draft] = []
        for part, piece, block in _pieces(element, limit, render):
            oversize = len(block) > limit
            if oversize or not self.drafts or not self.drafts[-1].fits(block, limit):
                hidden = {} if self.drafts else dict(self.leading)
                draft = _Draft(self.scope_path, self.scope_excluded, oversize=oversize, hidden=hidden)
                self.drafts.append(draft)
                opened.append(draft)
            self.drafts[-1].add(piece, block, part)
        return opened


def has_page_visual(page: Page) -> bool:
    """Whether ``page`` carries a readable ``PageImage`` (band loader/paths or full-page loader/path)."""
    if not isinstance(page, Page):
        raise TypeError(f"page must be a Page, got {type(page).__name__}")
    img = page.page_image
    if img is None:
        return False
    return img._band_loader is not None or bool(img.band_paths) or img._loader is not None or img.image_path is not None


def is_multimodal_page(page: Page, *, multimodal: bool = False) -> bool:
    """Whether ``page`` should be sent as multimodal 300-DPI PNG bands rather than text elements (#473)."""
    _check_bool("multimodal", multimodal)
    if not isinstance(page, Page):
        raise TypeError(f"page must be a Page, got {type(page).__name__}")
    return (
        multimodal
        and page.status != "error"
        and has_page_visual(page)
        and (page.route == "vision" or bool(page.image_only_reasons))
    )


def vision_page_anchor(page: Page) -> Figure:
    """Return the synthetic full-page :class:`Figure` anchor placed in ``<elements>`` for a multimodal page."""
    if not isinstance(page, Page) or page.page_image is None:
        raise ValueError("vision_page_anchor requires a Page with a non-None page_image")
    img = page.page_image
    return Figure(
        element_id=f"p{page.page_number:04d}_e000",
        page_number=page.page_number,
        reading_order=0,
        bbox=BBox(x0=0.0, y0=0.0, x1=page.width_pt, y1=page.height_pt),
        sha256="0" * 64,
        width_px=img.width_px,
        height_px=img.height_px,
        caption=f"[page {page.page_number} image attached]",
    )


def _is_image_only(page: Page) -> bool:
    """Whether ``page`` is skipped as image-only: marked (non-empty ``image_only_reasons``) and not an error page,
    which stays a page error (module notes, 4)."""
    return page.status != "error" and bool(page.image_only_reasons)


def _image_only_pages(document: PDFDocument) -> frozenset[int]:
    """The numbers of the pages :func:`build_units` skips as image-only, whose elements no unit holds (module notes,
    4): the rule :func:`group_scopes` and :mod:`~datagrunt.rationalization.envelope` read too."""
    return frozenset(page.page_number for page in document.pages if _is_image_only(page))


def _unsent_text_pages(document: PDFDocument, *, multimodal: bool = False) -> frozenset[int]:
    """Page numbers whose raw ``page.elements`` are withheld from prompts (image-only skips and multimodal pages)."""
    return frozenset(
        page.page_number
        for page in document.pages
        if _is_image_only(page) or is_multimodal_page(page, multimodal=multimodal)
    )


def _section_owners(sections: tuple[Section, ...]) -> dict[str, int]:
    """Map each section member's element id, its heading included (as the document counts it), to its section."""
    owners: dict[str, int] = {}
    for index, section in enumerate(sections):
        if section.heading is not None:
            owners[section.heading.element_id] = index
        for element in section.elements:
            owners[element.element_id] = index
    return owners


_NO_NODE = -1
"""A trie node no key names: once an entry leads off every recorded path, every later entry has no opener."""


@dataclass(frozen=True, slots=True, kw_only=True)
class GroupScope:
    """A section's scope path as its units show it: the entries a visible heading opened (module notes, 2).

    Args:
        section: The section's index in ``document.sections``.
        openers: The visible heading that opened each kept entry, outermost first: the document's own objects.
        excluded: How many entries of the section's ``scope_path`` were dropped.

    Raises:
        TypeError: If ``section`` or ``excluded`` is not an ``int``, or ``openers`` not a tuple of ``Heading``.
        ValueError: If ``section`` or ``excluded`` is negative, or an opener is hidden (invariant 1).
    """

    section: int
    openers: tuple[Heading, ...]
    excluded: int

    def __post_init__(self) -> None:
        _check_count("section", self.section)
        _check_count("excluded", self.excluded)
        _check_tuple_of("openers", self.openers, Heading)
        if any(opener.hidden_reasons for opener in self.openers):
            raise ValueError("a scope's openers are visible headings only (invariant 1)")

    @property
    def path(self) -> tuple[str, ...]:
        """The kept entries, outermost first: each its opener's text."""
        return tuple(opener.text for opener in self.openers)


class _Openers:
    """The headings read so far, each the last to record its own scope path (module notes, 2).

    The recorded paths form a trie keyed by ``(parent node, entry)`` (the root is node 0), so recording a heading and
    reading a section's path each take one step per entry, never a copy of a prefix. A heading on one of the
    ``closed`` pages (the image-only ones, module notes, 4) is recorded like any other but opens nothing.
    """

    __slots__ = ("nodes", "last", "closed")

    def __init__(self, closed: frozenset[int]) -> None:
        self.nodes: dict[tuple[int, str], int] = {}
        self.last: dict[int, Heading] = {}
        self.closed = closed

    def record(self, heading: Heading) -> None:
        """Record ``heading``, hidden or not, as the last heading read with its scope path: that entry's opener."""
        node = 0
        for entry in heading.scope_path:
            node = self.nodes.setdefault((node, entry), len(self.nodes) + 1)
        self.last[node] = heading

    def scope(self, index: int, section: Section) -> GroupScope:
        """The section's path keeping each entry whose opener is visible and on a page that is sent: the section's
        own heading for its last entry, when it has one, and for any other entry the last heading read whose own path
        is the entry's prefix."""
        node = 0
        kept: list[Heading] = []
        path = section.scope_path
        for position, entry in enumerate(path, start=1):
            node = self.nodes.get((node, entry), _NO_NODE)
            last = position == len(path)
            opener = section.heading if last and section.heading is not None else self.last.get(node)
            if opener is not None and not opener.hidden_reasons and opener.page_number not in self.closed:
                kept.append(opener)
        return GroupScope(section=index, openers=tuple(kept), excluded=len(path) - len(kept))


def group_scopes(document: PDFDocument, *, multimodal: bool = False) -> dict[str, GroupScope]:
    """Map each section member's element id, its heading included, to its section's scope (module notes, 2).

    One pass over the document's elements in document order: each heading is recorded where it stands, and a
    section's scope is read once, at its first member, so only the headings at or before that member can open its
    ancestor entries; its own heading opens its last entry wherever it stands.
    :mod:`~datagrunt.rationalization.envelope` reads scopes from here too, so both apply one rule.

    Args:
        document: The document; it is never changed.

    Returns:
        A section member's id to its section's scope, one object per section; an element in no section is absent
        (its page group's scope path is ``()``).

    Raises:
        TypeError: If ``document`` is not a ``PDFDocument``.
    """
    _check_bool("multimodal", multimodal)
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    owners = _section_owners(document.sections)
    openers = _Openers(_unsent_text_pages(document, multimodal=multimodal))
    scopes: dict[int, GroupScope] = {}
    found: dict[str, GroupScope] = {}
    for element in document.elements:
        if isinstance(element, Heading):
            openers.record(element)
        index = owners.get(element.element_id)
        if index is None:
            continue
        if index not in scopes:
            scopes[index] = openers.scope(index, document.sections[index])
        found[element.element_id] = scopes[index]
    return found


@dataclass(slots=True)
class _SkippedPage:
    """A page skipped whole, at its place: an error page, or an image-only one with its hidden elements counted
    per reason (module notes, 1 and 4)."""

    page_number: int
    reason: SkipReason
    hidden: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class _MultimodalPage:
    """A vision or image-only page with a page image, packed as one synthetic anchor unit (#473)."""

    page_number: int
    anchor: Figure
    hidden: dict[str, int] = field(default_factory=dict)


_Step: TypeAlias = tuple[DocumentElement, _Group] | _SkippedPage | _MultimodalPage
"""What the first pass yields, in document order: an element with its group, a skipped page, or a multimodal page."""


def _groups(
    document: PDFDocument,
    scopes: Mapping[str, GroupScope] | None = None,
    *,
    multimodal: bool = False,
) -> list[_Step]:
    """First pass: every element with its group, and every page skipped or multimodal whole, in document order.

    Groups, their members and their scope paths are as module notes 2-4 say.
    """
    resolved_scopes = group_scopes(document, multimodal=multimodal) if scopes is None else scopes
    steps: list[_Step] = []
    groups: dict[tuple[str, int], _Group] = {}
    for page in document.pages:
        if page.status == "error":
            steps.append(_SkippedPage(page.page_number, "page_error"))
            continue
        if is_multimodal_page(page, multimodal=multimodal):
            mm_page = _MultimodalPage(page.page_number, vision_page_anchor(page))
            for element in page.elements:
                _count(mm_page.hidden, element.hidden_reasons)
            steps.append(mm_page)
            continue
        if _is_image_only(page):
            skipped = _SkippedPage(page.page_number, "image_only")
            for element in page.elements:
                _count(skipped.hidden, element.hidden_reasons)
            steps.append(skipped)
            continue
        for element in page.elements:
            scope = resolved_scopes.get(element.element_id)
            key = ("page", page.page_number) if scope is None else ("section", scope.section)
            if key not in groups:
                groups[key] = _Group((), 0) if scope is None else _Group(scope.path, scope.excluded)
            groups[key].take(element)
            steps.append((element, groups[key]))
    return steps


def _common_prefix(left: tuple[Heading, ...], right: tuple[Heading, ...]) -> tuple[Heading, ...]:
    """The longest shared prefix of two visible opener sequences."""
    matched = 0
    for one, two in zip(left, right):
        if one != two:
            break
        matched += 1
    return left[:matched]


def _shared_openers(scopes: Mapping[str, GroupScope], elements: Sequence[DocumentElement]) -> tuple[Heading, ...]:
    """The longest common visible opener prefix across the groups of ``elements``."""
    if not elements:
        return ()
    first_scope = scopes.get(elements[0].element_id)
    if first_scope is None:
        return ()
    shared = first_scope.openers
    for element in elements[1:]:
        if not shared:
            return ()
        scope = scopes.get(element.element_id)
        if scope is None:
            return ()
        if scope is not first_scope:
            shared = _common_prefix(shared, scope.openers)
    return shared


def common_openers(document: PDFDocument, elements: Sequence[DocumentElement]) -> tuple[Heading, ...]:
    """Return the longest common visible opener prefix across the groups of ``elements``."""
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    if not isinstance(elements, Sequence) or isinstance(elements, (str, bytes)):
        raise TypeError(f"elements must be a Sequence of DocumentElement, got {type(elements).__name__}")
    return _shared_openers(group_scopes(document), elements)


def _draft_scope(scopes: Mapping[str, GroupScope], elements: Sequence[DocumentElement]) -> tuple[tuple[str, ...], int]:
    """The common scope_path and summed excluded count across the distinct groups of ``elements``."""
    openers = _shared_openers(scopes, elements)
    seen_sections: set[int] = set()
    excluded = 0
    for element in elements:
        scope = scopes.get(element.element_id)
        if scope is not None and scope.section not in seen_sections:
            seen_sections.add(scope.section)
            excluded += scope.excluded
    return tuple(opener.text for opener in openers), excluded


class _Coalescer:
    """Packs elements across consecutive text-bearing groups into coalesced drafts, flushing at pages skipped whole."""

    __slots__ = ("drafts", "current", "leading")

    def __init__(self) -> None:
        self.drafts: list[_Draft] = []
        self.current: _Draft | None = None
        self.leading: dict[str, int] = {}

    def flush(self) -> None:
        """Close the active draft at a page skipped whole (error or image-only) so later elements start a new unit."""
        self.current = None

    def pack(self, element: DocumentElement, limit: int, render: Renderer) -> list[_Draft]:
        """Place ``element`` into the active coalesced draft or open new draft(s); return any opened drafts."""
        if element.hidden_reasons:
            target = self.current.hidden if self.current is not None else self.leading
            _count(target, element.hidden_reasons)
            return []
        opened: list[_Draft] = []
        for part, piece, block in _pieces(element, limit, render):
            oversize = len(block) > limit
            if oversize or self.current is None or not self.current.fits(block, limit):
                draft = _Draft((), 0, oversize=oversize, hidden=dict(self.leading))
                self.leading.clear()
                self.drafts.append(draft)
                self.current = draft
                opened.append(draft)
            self.current.add(piece, block, part)
        return opened

    def finish(self, scopes: Mapping[str, GroupScope]) -> None:
        """Assign each coalesced draft its common scope path and excluded count, and attach trailing hidden counts."""
        if self.leading and self.drafts:
            for reason, count in self.leading.items():
                self.drafts[-1].hidden[reason] = self.drafts[-1].hidden.get(reason, 0) + count
            self.leading.clear()
        for draft in self.drafts:
            draft.scope_path, draft.scope_excluded = _draft_scope(scopes, draft.elements)


def _multimodal_draft(step: _MultimodalPage, limit: int, render: Renderer, extra_hidden: dict[str, int]) -> _Draft:
    """Build a single-page draft from ``step``, absorbing any pending ``extra_hidden`` counts."""
    block = _render(render, step.anchor)
    merged = dict(step.hidden)
    for reason, count in extra_hidden.items():
        merged[reason] = merged.get(reason, 0) + count
    extra_hidden.clear()
    draft = _Draft((), 0, oversize=len(block) > limit, hidden=merged)
    draft.add(step.anchor, block, None)
    return draft


def _pack(steps: list[_Step], limit: int, render: Renderer) -> list[_Draft | _Group | _SkippedPage]:
    """Second pass: everything a unit or skipped unit comes from, in document order of where it starts.

    A group with text is packed, its drafts listed as they open (at their first elements); a group without text
    is listed at its first member, unpacked; a page skipped whole at its place.
    """
    stream: list[_Draft | _Group | _SkippedPage] = []
    empty_hidden: dict[str, int] = {}
    for step in steps:
        if isinstance(step, _SkippedPage):
            stream.append(step)
            continue
        if isinstance(step, _MultimodalPage):
            stream.append(_multimodal_draft(step, limit, render, empty_hidden))
            continue
        element, group = step
        if group.has_text:
            stream.extend(group.pack(element, limit, render))
        elif element is group.members[0]:
            stream.append(group)
    return stream


def _pack_coalesced(
    steps: list[_Step],
    scopes: Mapping[str, GroupScope],
    limit: int,
    render: Renderer,
) -> list[_Draft | _Group | _SkippedPage]:
    """Second pass when ``coalesce_sections=True``: pack across text-bearing groups, flushing at a page skipped
    or sent multimodally whole."""
    stream: list[_Draft | _Group | _SkippedPage] = []
    coalescer = _Coalescer()
    mm_drafts: list[_Draft] = []
    for step in steps:
        if isinstance(step, _SkippedPage):
            coalescer.flush()
            stream.append(step)
            continue
        if isinstance(step, _MultimodalPage):
            coalescer.flush()
            mm = _multimodal_draft(step, limit, render, coalescer.leading)
            mm_drafts.append(mm)
            stream.append(mm)
            continue
        element, group = step
        if group.has_text:
            stream.extend(coalescer.pack(element, limit, render))
        elif element is group.members[0]:
            stream.append(group)
    if coalescer.leading and not coalescer.drafts and mm_drafts:
        for reason, count in coalescer.leading.items():
            mm_drafts[-1].hidden[reason] = mm_drafts[-1].hidden.get(reason, 0) + count
        coalescer.leading.clear()
    coalescer.finish(scopes)
    return stream


def _unit(doc_id: str, vision_pages: Mapping[int, bool], draft: _Draft) -> Unit:
    pages = _pages(draft.elements)
    return Unit(
        unit_id=_unit_id(doc_id, draft.elements, draft.parts),
        doc_id=doc_id,
        elements=tuple(draft.elements),
        blocks=tuple(draft.blocks),
        part_of=tuple(draft.parts),
        scope_path=draft.scope_path,
        scope_entries_excluded=draft.scope_excluded,
        page_numbers=pages,
        vision=any(vision_pages[number] for number in pages),
        oversize=draft.oversize,
        hidden_excluded=_counts(draft.hidden),
    )


def _skipped(doc_id: str, item: _Draft | _Group | _SkippedPage) -> SkippedUnit:
    """The skipped unit of a page skipped whole, a group without text, or a packed unit without text."""
    if isinstance(item, _SkippedPage):
        number = item.page_number
        return SkippedUnit(
            unit_id=f"{doc_id}:p{number:04d}",
            reason=item.reason,
            page_numbers=(number,),
            hidden_excluded=_counts(item.hidden),
        )
    if isinstance(item, _Draft):
        return SkippedUnit(
            unit_id=_unit_id(doc_id, item.elements, item.parts),
            reason="no_text",
            page_numbers=_pages(item.elements),
            hidden_excluded=_counts(item.hidden),
        )
    return SkippedUnit(
        unit_id=f"{doc_id}:{item.members[0].element_id}-{item.members[-1].element_id}",
        reason="no_text" if item.visible else "all_hidden",
        page_numbers=_pages(item.members),
        hidden_excluded=_counts(item.hidden),
    )


def _totals(document: PDFDocument) -> tuple[HiddenCounts, int]:
    """The document's hidden elements counted per reason, and how many there are (read from the pages)."""
    counter: dict[str, int] = {}
    hidden = [element for element in document.elements if element.hidden_reasons]
    for element in hidden:
        _count(counter, element.hidden_reasons)
    return _counts(counter), len(hidden)


def _check_inputs(
    document: object,
    max_unit_chars: object,
    render: object,
    coalesce_sections: object = False,
    multimodal: object = False,
) -> None:
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    if not _is_int(max_unit_chars):
        raise TypeError(f"max_unit_chars must be an int, got {max_unit_chars!r} ({type(max_unit_chars).__name__})")
    if max_unit_chars < 1:
        raise ValueError(f"max_unit_chars must be >= 1, got {max_unit_chars}")
    if not callable(render):
        raise TypeError(f"render must be callable, got {type(render).__name__}")
    _check_bool("coalesce_sections", coalesce_sections)
    _check_bool("multimodal", multimodal)


def build_units(
    document: PDFDocument,
    *,
    max_unit_chars: int,
    render: Renderer,
    coalesce_sections: bool = False,
    multimodal: bool = False,
) -> UnitPlan:
    """Group, render and pack a document's visible elements into units (module notes).

    Args:
        document: The document; it is never changed.
        max_unit_chars: The most characters a unit's body holds, unless it is one oversize block (>= 1).
        render: Turns one visible element, or a part of a split one, into its non-empty block; it never sees a
            hidden one. It must be pure: it may be called more than once for the same rows (:data:`Renderer`).
        coalesce_sections: When ``True``, pack consecutive text-bearing groups greedily across section boundaries
            up to ``max_unit_chars``, using the longest common visible opener prefix as ``scope_path``.

    Returns:
        The units and skipped units in document order, with the hidden counts and the dropped scope entries.

    Raises:
        TypeError: If ``document`` is not a ``PDFDocument``, ``max_unit_chars`` not an ``int`` (a ``bool`` is not
            one), ``render`` not callable, ``coalesce_sections`` not a ``bool``, or ``render`` returns something
            other than a ``str`` (naming the element).
        ValueError: If ``max_unit_chars`` is below 1, or ``render`` returns an empty block (naming the element).
    """
    _check_inputs(document, max_unit_chars, render, coalesce_sections, multimodal)
    doc_id = document.metadata.doc_id
    vision_pages = {
        page.page_number: (page.route == "vision" or is_multimodal_page(page, multimodal=multimodal))
        for page in document.pages
    }
    scopes = group_scopes(document, multimodal=multimodal)
    steps = _groups(document, scopes, multimodal=multimodal)
    packed = (
        _pack_coalesced(steps, scopes, max_unit_chars, render)
        if coalesce_sections
        else _pack(steps, max_unit_chars, render)
    )
    units: list[Unit] = []
    skipped: list[SkippedUnit] = []
    for item in packed:
        if isinstance(item, _Draft) and any(_has_text(element) for element in item.elements):
            units.append(_unit(doc_id, vision_pages, item))
        else:
            skipped.append(_skipped(doc_id, item))
    total, hidden_elements = _totals(document)
    return UnitPlan(
        units=tuple(units),
        skipped=tuple(skipped),
        hidden_excluded_total=total,
        hidden_elements_total=hidden_elements,
        scope_entries_excluded_total=sum(unit.scope_entries_excluded for unit in units),
    )
