"""Cross-unit entity folding and element provenance per document (plan §3, §4.9).

:func:`fold_documents` takes a :class:`~datagrunt.rationalization.grounding.GroundingResult` and the run's units,
envelopes, documents and prepared schema, and returns a :class:`FoldingResult` whose items are
:class:`GroundedRecord` instances in document order. Nothing ever folds across distinct ``doc_id`` values, and
records of an unkeyed root schema (``item.key == ()``) never fold across units. The rules, in order:

1. **Grouping.** Each :class:`~datagrunt.rationalization.grounding.GroundedItem` belongs to its unit's document
   (``units[item.unit_id].doc_id``). Within one document, when the root schema has keys (``item.key != ()``), items
   sharing ``(doc_id, item.key)`` across units fold in unit order into a single :class:`GroundedRecord`. When the
   root schema has no keys (``item.key == ()``), each item stays a single-unit :class:`GroundedRecord` in unit and
   item order.
2. **Single-unit pass-through.** A group with one contributing unit keeps its consensus record, status, disputed
   fields, notes and grounding reasons unchanged, and attaches its resolved :class:`ElementSource` provenance and
   combined grounding quotes.
3. **Cross-unit field folding.** When two or more units of a document share a root key:

   - **Scalar and unkeyed list fields** (``scalar``, ``scalar_list``, or ``model_list`` whose child model has no
     keys): a unit contributes an explicit value when the field was set on its record (and is non-``None`` for an
     Optional field, or non-empty for a non-required list field). If all explicit values across units share one
     normalized form (:func:`~datagrunt.rationalization.consensus._normalized_field`), the first explicit value
     wins; if no unit provided an explicit value, the field's default stands; if two units provided explicit values
     with distinct normalized forms, the field's dotted attribute path is recorded in ``disputed_fields`` and the
     folded record becomes ``"cross_page_conflict"``.
   - **Nested model fields** (``model``): units where an Optional model field is ``None`` yield to units where a
     model instance is present; when two or more units provide an instance, they fold recursively field by field.
   - **Keyed child lists** (``model_list`` whose child model declares keys): children across units are united by
     their normalized child key in first-seen order; children sharing a key across units fold recursively field by
     field, recording any conflict under ``<list_field>[<key>].<child_field>``.
   - **Single rebuild.** When every contributing unit has a record and no cross-unit conflict occurs, the folded
     wire representation is validated once via ``model_validate`` and checked with
     :func:`~datagrunt.rationalization.schema.instance_problems`. If a cross-field validator rejects the folded
     record, the record becomes ``None`` with status ``"cross_page_conflict"`` and a note naming the validation
     failure (field paths and error kinds only, never input values).
4. **Status ranking.** The seven :data:`RECORD_STATUSES` form a severity order from cleanest to worst:
   ``consensus < single_model < missing_in_some_models < field_disagreement < unmatched < ungrounded_value <
   cross_page_conflict``. A folded record's status is the worst status among its contributing units, promoted to
   ``"ungrounded_value"`` whenever any contributing unit has grounding reasons, and to ``"cross_page_conflict"``
   whenever cross-unit folding finds a conflict or the folded rebuild fails validation.
5. **Element provenance** (:class:`ElementSource`). Each cited element id that is citable in its unit (one of the
   unit's elements or its envelope's ``header_element_ids``) resolves to an :class:`ElementSource` carrying the
   whole unsplit element's ``bbox`` from ``documents[doc_id]`` and ``scope_path = unit.scope_path`` (never the
   element's own ``scope_path``), deduplicated by ``(unit_id, element_id)`` in first-seen citation order.

Bounds: every walk is bounded by :data:`~datagrunt.rationalization.schema.MAX_DEPTH` and linear in the contributing
records, child items and cited element ids. The module imports only the standard library, pydantic, the PDF element
model and its sibling modules: nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeAlias, TypeVar, cast, get_args

from pydantic import BaseModel

from datagrunt.pdf.elements import BBox, DocumentElement, PDFDocument, _parse_element_id
from datagrunt.rationalization.consensus import (
    _check_choice,
    _check_family,
    _check_strings,
    _check_text,
    _check_tuple_of,
    _check_votes,
    _dumped,
    _key_of,
    _key_text,
    _normalized_field,
    _read_only,
    _united,
    _validated,
    key_order,
)
from datagrunt.rationalization.envelope import Envelope
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.grounding import GroundedItem, GroundingReason, GroundingResult
from datagrunt.rationalization.schema import MAX_DEPTH, FieldSpec, ModelSpec, PreparedSchema
from datagrunt.rationalization.units import GroupScope, Unit, group_scopes

RecordStatus: TypeAlias = Literal[
    "consensus",
    "single_model",
    "missing_in_some_models",
    "field_disagreement",
    "unmatched",
    "ungrounded_value",
    "cross_page_conflict",
]
"""A record's status after cross-unit folding (module notes, 4)."""

RECORD_STATUSES: tuple[RecordStatus, ...] = get_args(RecordStatus)

T = TypeVar("T", bound=BaseModel)

__all__ = [
    "RECORD_STATUSES",
    "ElementSource",
    "FoldingResult",
    "GroundedRecord",
    "RecordStatus",
    "fold_documents",
]

_CLEAN_STATUSES: frozenset[str] = frozenset({"consensus", "single_model"})
_STATUS_RANK: Mapping[str, int] = {status: index for index, status in enumerate(RECORD_STATUSES)}


# ---------------------------------------------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ElementSource:
    """One cited document element backing a :class:`GroundedRecord` (module notes, 5).

    Args:
        doc_id: The document's id.
        unit_id: The unit in which the element was cited.
        element_id: The canonical element id (e.g. ``"p0001_e003"``).
        page_number: The 1-based page number encoded by ``element_id``.
        bbox: The whole element's bounding box on its page, from the document.
        scope_path: The unit's sanitized scope path (``Unit.scope_path``, never ``DocumentElement.scope_path``).
            Left out of ``repr`` because section headings are document text.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If an id or scope entry is blank, ``element_id`` is not canonical, or ``page_number`` does not
            match ``element_id``.
    """

    doc_id: str
    unit_id: str
    element_id: str
    page_number: int
    bbox: BBox
    scope_path: tuple[str, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        _check_text("doc_id", self.doc_id)
        _check_text("unit_id", self.unit_id)
        parsed_page, _ = _parse_element_id("element_id", self.element_id)
        if type(self.page_number) is not int:
            raise TypeError(f"page_number must be an int, got {type(self.page_number).__name__}")
        if self.page_number < 1:
            raise ValueError(f"page_number must be >= 1, got {self.page_number}")
        if self.page_number != parsed_page:
            raise ValueError(
                f"page_number ({self.page_number}) does not match element_id {self.element_id!r} (page {parsed_page})"
            )
        if not isinstance(self.bbox, BBox):
            raise TypeError(f"bbox must be a BBox, got {type(self.bbox).__name__}")
        if not isinstance(self.scope_path, tuple):
            raise TypeError(f"scope_path must be a tuple of str, got {type(self.scope_path).__name__}")
        for entry in self.scope_path:
            _check_text("scope_path entry", entry)


@dataclass(frozen=True, slots=True)
class GroundedRecord(Generic[T]):
    """One entity of one document after consensus, grounding and cross-unit folding (module notes, 1-5).

    Every field that can hold document or model-chosen text is omitted from ``repr`` (invariant 7).

    Args:
        doc_id: The document's id.
        unit_ids: The contributing unit ids in document order, non-empty and distinct.
        status: One of :data:`RECORD_STATUSES`.
        key: The normalized root key tuple, ``()`` for an unkeyed schema.
        record: The validated record instance, or ``None`` when disputed, unmatched, or in cross-page conflict.
        disputed_fields: The dotted attribute paths of disputed or conflicting fields, each once.
        notes: Diagnostic notes for review (validation failures, repeated keys), never holding input values.
        grounding_reasons: Why supporting models' evidence did not ground the record, ``()`` when grounded.
        sources: Resolved :class:`ElementSource` entries in first-seen citation order.
        grounding_quotes: Union of supporting models' quotes across contributing units, in first-seen order.
        model_votes: Each requested model's validated records across the contributing units, as JSON-mode dicts.
        quotes_by_model: Each requested model's grounding quotes united across the contributing units.
        element_ids_by_model: Each requested model's cited element ids united across the contributing units.
        families: Each requested model's family.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If an invariant of the status, unit ids, sources or per-model mappings is violated.
    """

    doc_id: str
    unit_ids: tuple[str, ...]
    status: RecordStatus
    key: tuple[object, ...] = field(repr=False)
    record: T | None = field(repr=False)
    disputed_fields: tuple[str, ...] = field(repr=False)
    notes: tuple[str, ...] = field(repr=False)
    grounding_reasons: tuple[GroundingReason, ...] = field(repr=False)
    sources: tuple[ElementSource, ...]
    grounding_quotes: tuple[str, ...] = field(repr=False)
    model_votes: Mapping[str, tuple[Mapping[str, Any], ...]] = field(repr=False)
    quotes_by_model: Mapping[str, tuple[str, ...]] = field(repr=False)
    element_ids_by_model: Mapping[str, tuple[str, ...]] = field(repr=False)
    families: Mapping[str, str]

    def __post_init__(self) -> None:
        _check_text("doc_id", self.doc_id)
        _check_unit_ids(self.unit_ids)
        _check_choice("status", self.status, RECORD_STATUSES)
        if not isinstance(self.key, tuple):
            raise TypeError(f"key must be a tuple, got {type(self.key).__name__}")
        if self.record is not None and not isinstance(self.record, BaseModel):
            raise TypeError(f"record must be a pydantic model instance or None, got {type(self.record).__name__}")
        _check_strings("disputed_fields", self.disputed_fields)
        if not all(path.strip() for path in self.disputed_fields):
            raise ValueError("disputed_fields must hold non-blank paths")
        _check_strings("notes", self.notes)
        _check_tuple_of("grounding_reasons", self.grounding_reasons, GroundingReason)
        _check_tuple_of("sources", self.sources, ElementSource)
        _check_strings("grounding_quotes", self.grounding_quotes)
        object.__setattr__(self, "model_votes", _read_only("model_votes", self.model_votes, _check_votes))
        object.__setattr__(self, "quotes_by_model", _read_only("quotes_by_model", self.quotes_by_model, _check_strings))
        ids = _read_only("element_ids_by_model", self.element_ids_by_model, _check_strings)
        object.__setattr__(self, "element_ids_by_model", ids)
        object.__setattr__(self, "families", _read_only("families", self.families, _check_family))
        self._check_shape()

    def _check_shape(self) -> None:
        """Verify model mapping keys, source membership, and status invariants."""
        models = list(self.model_votes)
        others = (self.quotes_by_model, self.element_ids_by_model, self.families)
        if not models or any(list(mapping) != models for mapping in others):
            raise ValueError(
                "model_votes, quotes_by_model, element_ids_by_model and families must name the same models"
            )
        if not any(self.model_votes.values()):
            raise ValueError("a GroundedRecord needs a vote from at least one model")
        unit_set = set(self.unit_ids)
        for source in self.sources:
            if source.doc_id != self.doc_id or source.unit_id not in unit_set:
                raise ValueError("every ElementSource must belong to the record's doc_id and unit_ids")
        if any(reason.model_id not in self.model_votes for reason in self.grounding_reasons):
            raise ValueError("grounding_reasons name a model not in model_votes")
        if self.status in _CLEAN_STATUSES and (
            self.record is None or self.disputed_fields or self.notes or self.grounding_reasons
        ):
            raise ValueError(f"a {self.status} record requires a record and no disputed_fields, notes or reasons")
        if self.status == "single_model" and len(models) != 1:
            raise ValueError("a single_model record must have exactly one model")
        if self.status in ("field_disagreement", "cross_page_conflict") and not (self.disputed_fields or self.notes):
            raise ValueError(f"a {self.status} record must name a disputed field or a note")
        if self.status == "cross_page_conflict" and self.record is not None:
            raise ValueError("a cross_page_conflict record must have record=None")
        if self.status == "ungrounded_value" and not self.grounding_reasons:
            raise ValueError("an ungrounded_value record must have at least one GroundingReason")


@dataclass(frozen=True, slots=True)
class FoldingResult(Generic[T]):
    """The folded :class:`GroundedRecord` items of a run, in document order.

    Raises:
        TypeError: If ``items`` is not a tuple of :class:`GroundedRecord`.
    """

    items: tuple[GroundedRecord[T], ...]

    def __post_init__(self) -> None:
        _check_tuple_of("items", self.items, GroundedRecord)


# ---------------------------------------------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------------------------------------------


def fold_documents(
    grounding: GroundingResult,
    *,
    units: Mapping[str, Unit],
    envelopes: Mapping[str, Envelope],
    documents: Mapping[str, PDFDocument],
    schema: PreparedSchema,
) -> FoldingResult[Any]:
    """Fold grounded items across units of the same document when they share a non-empty root key (module notes).

    Args:
        grounding: The grounding result of the run.
        units: Every unit of the run by its ``unit_id``, in document order.
        envelopes: Every unit's envelope by its ``unit_id`` (same keys as ``units``).
        documents: Every document of the run by its ``doc_id``.
        schema: The prepared schema the run used.

    Returns:
        The :class:`FoldingResult`.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If ``envelopes`` and ``units`` do not name the same unit ids, a unit's document is missing from
            ``documents``, or an item or empty unit in ``grounding`` names an unknown unit or wrong model/key shape.
    """
    _check_arguments(grounding, units, envelopes, documents, schema)
    doc_elements = _index_documents(documents)
    doc_scopes = {doc_id: group_scopes(document) for doc_id, document in documents.items()}
    unit_citable = _index_citable(units, envelopes)
    unit_rank = {unit_id: index for index, unit_id in enumerate(units)}
    groups = _group_items(grounding.items, units, unit_rank, bool(schema.spec.keys.fields))
    folded = [
        _fold_group(
            group,
            units=units,
            doc_elements=doc_elements,
            doc_scopes=doc_scopes,
            unit_citable=unit_citable,
            schema=schema,
        )
        for group in groups
    ]
    return FoldingResult(items=tuple(folded))


# ---------------------------------------------------------------------------------------------------------------
# Argument validation & indexing
# ---------------------------------------------------------------------------------------------------------------


def _check_unit_ids(unit_ids: object) -> None:
    """Require a non-empty tuple of distinct, non-blank unit id strings."""
    if not isinstance(unit_ids, tuple):
        raise TypeError(f"unit_ids must be a tuple of str, got {type(unit_ids).__name__}")
    for unit_id in unit_ids:
        _check_text("unit_ids entry", unit_id)
    if not unit_ids:
        raise ValueError("unit_ids must be non-empty")
    if len(set(unit_ids)) != len(unit_ids):
        raise ValueError("unit_ids must not repeat a unit id")


def _check_mapping(name: str, value: object, kind: type, what: str) -> None:
    """Require a mapping of unit ids to ``kind`` instances, each under its own ``unit_id``."""
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a Mapping of unit ids to {what}s, got {type(value).__name__}")
    for unit_id, entry in value.items():
        if not isinstance(unit_id, str):
            raise TypeError(f"{name} keys must be unit ids (str), got {type(unit_id).__name__}")
        if not isinstance(entry, kind) or not isinstance(entry, (Unit, Envelope)):
            raise TypeError(f"{name}[{unit_id!r}] must be a {kind.__name__}, got {type(entry).__name__}")
        if entry.unit_id != unit_id:
            raise ValueError(f"{name} key {unit_id!r} is not its {what}'s id ({entry.unit_id!r})")


def _check_arguments(
    grounding: object,
    units: object,
    envelopes: object,
    documents: object,
    schema: object,
) -> None:
    """Validate all inputs to :func:`fold_documents` before folding."""
    if not isinstance(grounding, GroundingResult):
        raise TypeError(f"grounding must be a GroundingResult, got {type(grounding).__name__}")
    if not isinstance(schema, PreparedSchema):
        raise TypeError(f"schema must be a PreparedSchema, got {type(schema).__name__}")
    _check_mapping("units", units, Unit, "unit")
    _check_mapping("envelopes", envelopes, Envelope, "envelope")
    typed_units = cast(Mapping[str, Unit], units)
    typed_envelopes = cast(Mapping[str, Envelope], envelopes)
    if set(typed_envelopes) != set(typed_units):
        raise ValueError("envelopes must name the same unit ids as units")
    if not isinstance(documents, Mapping):
        raise TypeError(f"documents must be a Mapping of doc ids to PDFDocuments, got {type(documents).__name__}")
    for doc_id, document in documents.items():
        if not isinstance(doc_id, str) or not isinstance(document, PDFDocument):
            raise TypeError(f"documents must map doc ids (str) to PDFDocuments, got {type(document).__name__}")
        if document.metadata.doc_id != doc_id:
            raise ValueError(f"documents key {doc_id!r} is not its document's id ({document.metadata.doc_id!r})")
    for unit in typed_units.values():
        if unit.doc_id not in documents:
            raise ValueError(f"unit {unit.unit_id!r} references doc_id {unit.doc_id!r}, which is not in documents")
    for empty_id in grounding.empty_units:
        if empty_id not in typed_units:
            raise ValueError(f"empty_units names unknown unit {empty_id!r}")
    key_count = len(schema.spec.keys.fields)
    for grounded in grounding.items:
        item = grounded.item
        if item.unit_id not in typed_units:
            raise ValueError(f"grounded item names unknown unit {item.unit_id!r}")
        if item.record is not None and not isinstance(item.record, schema.model):
            raise ValueError(f"grounded item record is a {type(item.record).__name__}, not {schema.model.__name__}")
        if len(item.key) != key_count:
            raise ValueError(f"grounded item key has {len(item.key)} part(s), expected {key_count}")


def _index_documents(documents: Mapping[str, PDFDocument]) -> dict[str, dict[str, DocumentElement]]:
    """Map each ``doc_id`` to its whole ``DocumentElement`` instances by ``element_id``."""
    by_doc: dict[str, dict[str, DocumentElement]] = {}
    for doc_id, document in documents.items():
        elements: dict[str, DocumentElement] = {}
        for header_kv in document.header_context:
            elements[header_kv.element_id] = header_kv
        for page in document.pages:
            for element in page.elements:
                elements[element.element_id] = element
        by_doc[doc_id] = elements
    return by_doc


def _index_citable(
    units: Mapping[str, Unit], envelopes: Mapping[str, Envelope]
) -> dict[str, dict[str, DocumentElement]]:
    """Map each ``unit_id`` to the citable elements in that unit (own elements plus ``header_element_ids``)."""
    by_doc_unit_elements: dict[tuple[str, str], DocumentElement] = {}
    for unit in units.values():
        for element in unit.elements:
            by_doc_unit_elements.setdefault((unit.doc_id, element.element_id), element)
    citable: dict[str, dict[str, DocumentElement]] = {}
    for unit_id, unit in units.items():
        allowed: dict[str, DocumentElement] = {element.element_id: element for element in unit.elements}
        for header_id in envelopes[unit_id].header_element_ids:
            header_el = by_doc_unit_elements.get((unit.doc_id, header_id))
            if header_el is not None:
                allowed[header_id] = header_el
        citable[unit_id] = allowed
    return citable


# ---------------------------------------------------------------------------------------------------------------
# Grouping & Provenance
# ---------------------------------------------------------------------------------------------------------------


def _group_items(
    items: Sequence[GroundedItem],
    units: Mapping[str, Unit],
    unit_rank: Mapping[str, int],
    has_keys: bool,
) -> list[list[GroundedItem]]:
    """Partition ``items`` by document in unit order; fold within a document only when ``has_keys`` is True."""
    if not has_keys:
        ordered = sorted(enumerate(items), key=lambda pair: (unit_rank[pair[1].item.unit_id], pair[0]))
        return [[grounded] for _, grounded in ordered]
    doc_order: list[str] = []
    by_doc: dict[str, dict[tuple[object, ...], list[GroundedItem]]] = {}
    for grounded in items:
        doc_id = units[grounded.item.unit_id].doc_id
        if doc_id not in by_doc:
            doc_order.append(doc_id)
            by_doc[doc_id] = {}
        by_doc[doc_id].setdefault(grounded.item.key, []).append(grounded)
    groups: list[list[GroundedItem]] = []
    for doc_id in doc_order:
        keyed_groups = by_doc[doc_id]
        for _, group in sorted(keyed_groups.items(), key=lambda pair: key_order(pair[0])):
            group.sort(key=lambda grounded: unit_rank[grounded.item.unit_id])
            groups.append(group)
    return groups


def _element_scope(
    element_id: str,
    unit: Unit,
    body_ids: frozenset[str],
    scopes: Mapping[str, GroupScope],
) -> tuple[str, ...]:
    """Resolve the sanitized section ``scope_path`` for a cited element in ``unit``."""
    if element_id not in body_ids:
        return unit.scope_path
    group_scope = scopes.get(element_id)
    return () if group_scope is None else group_scope.path


def _build_sources(
    group: Sequence[GroundedItem],
    *,
    units: Mapping[str, Unit],
    doc_elements: Mapping[str, Mapping[str, DocumentElement]],
    doc_scopes: Mapping[str, Mapping[str, GroupScope]],
    unit_citable: Mapping[str, Mapping[str, DocumentElement]],
) -> tuple[ElementSource, ...]:
    """Resolve cited element ids across ``group`` into deduplicated :class:`ElementSource` instances."""
    seen: set[tuple[str, str]] = set()
    sources: list[ElementSource] = []
    for grounded in group:
        unit_id = grounded.item.unit_id
        unit = units[unit_id]
        citable = unit_citable[unit_id]
        whole_by_id = doc_elements[unit.doc_id]
        scopes = doc_scopes[unit.doc_id]
        body_ids = frozenset(element.element_id for element in unit.elements)
        for element_ids in grounded.item.element_ids_by_model.values():
            for element_id in element_ids:
                pair = (unit_id, element_id)
                if pair in seen or element_id not in citable:
                    continue
                seen.add(pair)
                element = whole_by_id.get(element_id, citable[element_id])
                sources.append(
                    ElementSource(
                        doc_id=unit.doc_id,
                        unit_id=unit_id,
                        element_id=element_id,
                        page_number=element.page_number,
                        bbox=element.bbox,
                        scope_path=_element_scope(element_id, unit, body_ids, scopes),
                    )
                )
    return tuple(sources)


# ---------------------------------------------------------------------------------------------------------------
# Folding a group of GroundedItems
# ---------------------------------------------------------------------------------------------------------------


def _fold_group(
    group: Sequence[GroundedItem],
    *,
    units: Mapping[str, Unit],
    doc_elements: Mapping[str, Mapping[str, DocumentElement]],
    doc_scopes: Mapping[str, Mapping[str, GroupScope]],
    unit_citable: Mapping[str, Mapping[str, DocumentElement]],
    schema: PreparedSchema,
) -> GroundedRecord[Any]:
    """Fold one group of :class:`GroundedItem` instances into a :class:`GroundedRecord`."""
    first = group[0]
    doc_id = units[first.item.unit_id].doc_id
    sources = _build_sources(
        group,
        units=units,
        doc_elements=doc_elements,
        doc_scopes=doc_scopes,
        unit_citable=unit_citable,
    )
    quotes = _united(
        quotes_for_model for grounded in group for quotes_for_model in grounded.item.quotes_by_model.values()
    )
    if len(group) == 1:
        item = first.item
        return GroundedRecord(
            doc_id=doc_id,
            unit_ids=(item.unit_id,),
            status=first.status,
            key=item.key,
            record=item.record,
            disputed_fields=item.disputed_fields,
            notes=item.notes,
            grounding_reasons=first.reasons,
            sources=sources,
            grounding_quotes=quotes,
            model_votes=item.model_votes,
            quotes_by_model=item.quotes_by_model,
            element_ids_by_model=item.element_ids_by_model,
            families=item.families,
        )

    unit_ids = tuple(dict.fromkeys(grounded.item.unit_id for grounded in group))
    model_ids = tuple(first.item.model_votes)
    model_votes = {
        model_id: tuple(vote for grounded in group for vote in grounded.item.model_votes[model_id])
        for model_id in model_ids
    }
    quotes_by_model = {
        model_id: _united(grounded.item.quotes_by_model[model_id] for grounded in group) for model_id in model_ids
    }
    element_ids_by_model = {
        model_id: _united(grounded.item.element_ids_by_model[model_id] for grounded in group) for model_id in model_ids
    }
    reasons = tuple(dict.fromkeys(reason for grounded in group for reason in grounded.reasons))
    disputed: list[str] = [path for grounded in group for path in grounded.item.disputed_fields]
    notes: list[str] = [note for grounded in group for note in grounded.item.notes]

    records = [grounded.item.record for grounded in group if grounded.item.record is not None]
    all_have_records = len(records) == len(group)
    folder = _Folder()
    wire = folder.fold_model(schema.spec, "", records, 1) if len(records) >= 2 else None
    disputed.extend(folder.disputed)
    has_conflict = bool(folder.disputed)

    folded_record: BaseModel | None = None
    if all_have_records and not has_conflict and wire is not None:
        built = _validated(wire, schema)
        if isinstance(built, str):
            has_conflict = True
            notes.append(f"the folded record failed validation: {built}")
        else:
            folded_record = built

    status = _folded_status(group, has_conflict=has_conflict, has_reasons=bool(reasons))
    if status in ("field_disagreement", "unmatched", "cross_page_conflict"):
        folded_record = None

    return GroundedRecord(
        doc_id=doc_id,
        unit_ids=unit_ids,
        status=status,
        key=first.item.key,
        record=folded_record,
        disputed_fields=tuple(dict.fromkeys(disputed)),
        notes=tuple(dict.fromkeys(notes)),
        grounding_reasons=reasons,
        sources=sources,
        grounding_quotes=quotes,
        model_votes=model_votes,
        quotes_by_model=quotes_by_model,
        element_ids_by_model=element_ids_by_model,
        families=first.item.families,
    )


def _folded_status(group: Sequence[GroundedItem], *, has_conflict: bool, has_reasons: bool) -> RecordStatus:
    """Rank the status of a multi-unit group by worst-status precedence (module notes, 4)."""
    if has_conflict:
        return "cross_page_conflict"
    worst_rank = max(_STATUS_RANK[grounded.status] for grounded in group)
    if has_reasons:
        worst_rank = max(worst_rank, _STATUS_RANK["ungrounded_value"])
    return RECORD_STATUSES[worst_rank]


class _Folder:
    """Cross-unit recursive model folder (module notes, 3)."""

    __slots__ = ("disputed",)

    def __init__(self) -> None:
        self.disputed: list[str] = []

    def fold_model(
        self,
        spec: ModelSpec,
        path: str,
        instances: Sequence[BaseModel],
        depth: int,
    ) -> dict[str, Any] | None:
        """Fold ``instances`` of ``spec`` across units into a wire dict, or ``None`` on conflict."""
        if depth > MAX_DEPTH:
            raise RationalizationError(f"{path or spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
        wire: dict[str, Any] = {}
        complete = True
        for field_spec in spec.fields:
            field_path = f"{path}.{field_spec.name}" if path else field_spec.name
            ok, has_value, value = self._fold_field(field_spec, field_path, instances, depth)
            if not ok:
                complete = False
            elif has_value:
                wire[field_spec.wire_name] = value
        return wire if complete else None

    def _fold_field(
        self,
        field_spec: FieldSpec,
        field_path: str,
        instances: Sequence[BaseModel],
        depth: int,
    ) -> tuple[bool, bool, Any]:
        """Fold one field across ``instances``: returns ``(ok, include_in_wire, wire_value)``."""
        child = field_spec.child
        if field_spec.kind == "model" and child is not None:
            return self._fold_nested_model(field_spec, child, field_path, instances, depth)
        if field_spec.kind == "model_list" and child is not None and child.keys.fields:
            return self._fold_keyed_list(field_spec, child, field_path, instances, depth)
        return self._fold_leaf(field_spec, field_path, instances, depth)

    def _fold_nested_model(
        self,
        field_spec: FieldSpec,
        child: ModelSpec,
        field_path: str,
        instances: Sequence[BaseModel],
        depth: int,
    ) -> tuple[bool, bool, Any]:
        """Fold an Optional or required nested ``BaseModel`` field across units."""
        present = [getattr(inst, field_spec.name) for inst in instances if getattr(inst, field_spec.name) is not None]
        if not present:
            include = field_spec.required or any(field_spec.name in inst.model_fields_set for inst in instances)
            return True, include, None
        if len(present) == 1:
            return True, True, _instance_wire(present[0], child, depth + 1)
        folded = self.fold_model(child, field_path, present, depth + 1)
        return (folded is not None), (folded is not None), folded

    def _fold_keyed_list(
        self,
        field_spec: FieldSpec,
        child: ModelSpec,
        field_path: str,
        instances: Sequence[BaseModel],
        depth: int,
    ) -> tuple[bool, bool, Any]:
        """Union and fold a keyed ``list[ChildModel]`` across units in first-seen child key order."""
        groups: dict[tuple[object, ...], list[BaseModel]] = {}
        for inst in instances:
            for child_inst in getattr(inst, field_spec.name):
                groups.setdefault(_key_of(child_inst, child), []).append(child_inst)
        if not groups:
            include = field_spec.required or any(field_spec.name in inst.model_fields_set for inst in instances)
            return True, include, []
        merged_list: list[dict[str, Any]] = []
        list_ok = True
        for child_instances in groups.values():
            child_path = f"{field_path}[{_key_text(child_instances[0], child)}]"
            if len(child_instances) == 1:
                merged_list.append(_instance_wire(child_instances[0], child, depth + 1))
            else:
                folded_child = self.fold_model(child, child_path, child_instances, depth + 1)
                if folded_child is None:
                    list_ok = False
                else:
                    merged_list.append(folded_child)
        return list_ok, list_ok, merged_list

    def _fold_leaf(
        self,
        field_spec: FieldSpec,
        field_path: str,
        instances: Sequence[BaseModel],
        depth: int,
    ) -> tuple[bool, bool, Any]:
        """Fold a scalar, ``scalar_list`` or unkeyed ``model_list`` field across units."""
        explicit = [inst for inst in instances if _is_explicit(inst, field_spec)]
        if not explicit:
            include = field_spec.required or any(field_spec.name in inst.model_fields_set for inst in instances)
            if not include:
                return True, False, None
            source = next((inst for inst in instances if field_spec.name in inst.model_fields_set), instances[0])
            return True, True, _dump_leaf(source, field_spec, depth)
        norms = {_normalized_field(field_spec, getattr(inst, field_spec.name)) for inst in explicit}
        if len(norms) > 1:
            self.disputed.append(field_path)
            return False, False, None
        return True, True, _dump_leaf(explicit[0], field_spec, depth)


def _is_explicit(instance: BaseModel, field_spec: FieldSpec) -> bool:
    """Whether ``instance`` provides a non-vacuous explicit value for ``field_spec``."""
    value = getattr(instance, field_spec.name)
    if field_spec.optional:
        return value is not None
    if field_spec.kind in ("scalar_list", "model_list"):
        return (field_spec.required or field_spec.name in instance.model_fields_set) and (
            bool(value) or field_spec.required
        )
    return field_spec.required or field_spec.name in instance.model_fields_set


def _instance_wire(instance: BaseModel, spec: ModelSpec, depth: int) -> dict[str, Any]:
    """Serialize ``instance`` to a wire dict for ``model_validate``, omitting unset defaulted fields."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    dumped = _dumped(instance)
    wire: dict[str, Any] = {}
    for field_spec in spec.fields:
        if not field_spec.required and field_spec.name not in instance.model_fields_set:
            continue
        if field_spec.kind == "model" and field_spec.child is not None:
            sub = getattr(instance, field_spec.name)
            wire[field_spec.wire_name] = None if sub is None else _instance_wire(sub, field_spec.child, depth + 1)
        elif field_spec.kind == "model_list" and field_spec.child is not None:
            items = getattr(instance, field_spec.name)
            wire[field_spec.wire_name] = [_instance_wire(item, field_spec.child, depth + 1) for item in items]
        else:
            wire[field_spec.wire_name] = dumped[field_spec.name]
    return wire


def _dump_leaf(instance: BaseModel, field_spec: FieldSpec, depth: int) -> Any:
    """Serialize a scalar, ``scalar_list``, or unkeyed ``model_list`` field of ``instance`` for ``model_validate``."""
    if field_spec.kind == "model_list" and field_spec.child is not None:
        return [_instance_wire(item, field_spec.child, depth + 1) for item in getattr(instance, field_spec.name)]
    return _dumped(instance)[field_spec.name]
