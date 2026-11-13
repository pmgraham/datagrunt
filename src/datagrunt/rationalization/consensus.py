"""Validation, quarantine and cross-family consensus over a run's responses (plan §4.7, the 2026-11-05 addenda).

:func:`build_consensus` takes a :class:`~datagrunt.rationalization.runner.RunOutcome`, the prepared schema and the
run's unit ids in document order, and returns a :class:`ConsensusResult`: every record any model returned, as a
:class:`ConsensusItem` with a status, or as a :class:`QuarantinedItem` saying why it could not be used. Nothing is
dropped silently. Grounding (T11) and folding (T12) come after it; each item carries what they read: its unit id,
its normalized key, and each model's records, quotes, element ids and family. The rules, in order:

1. **Quarantine** (:func:`validate_response`, per (unit, model) response). A ``truncated`` or ``refused`` response is
   one item of that reason, never parsed. A response whose ``content`` is ``None``, or not exactly the wrapper
   ``{"records": [...]}``, is one ``"unparseable"`` item; an entry of ``records`` that is not exactly ``{"record":
   {...}, "grounding_quotes": [str, ...], "source_element_ids": [str, ...]}`` is one ``"unparseable"`` item of its
   own. A record ``model_validate`` refuses is a ``"validation"`` item: its message names each error's field path
   (wire names) and type, never an input value, in at most :data:`ERROR_MESSAGE_MAX_CHARS` characters; its
   ``raw_json`` keeps the record as the model sent it, in at most :data:`RAW_JSON_MAX_CHARS`. A validator of the
   schema that raises what pydantic does not wrap into a ``ValidationError`` (a ``KeyError``, say) quarantines the
   record too, its message naming the exception's class only, since its text can quote the input; a
   ``MemoryError``, a ``RecursionError`` and an exception that is not an ``Exception`` (``KeyboardInterrupt``)
   propagate. The same holds for the merged record's rebuild (rule 8), as a note. ``model_validate`` is given a
   copy of the record (pydantic hands a ``mode="before"`` validator the very dict, and it may change it in place),
   so the record as sent is kept, unchanged, for rule 8.
2. **Null = not shown** (maintainer, 2026-11-05). Before validation, in each record and recursively in its child
   records (following the spec by wire name, and by attribute name too in a model that validates by name:
   ``validate_by_name`` or ``populate_by_name``), a key whose value is ``null`` is dropped when its field is neither
   Optional nor required (``FieldSpec.optional`` and ``FieldSpec.required`` both false), so the field's pydantic
   default applies: OpenAI strict mode sends such a field required and nullable, and a null there means the model
   showed no value. A null in an Optional field stays ``None``; one in a required field is left for
   ``model_validate`` to refuse. The response itself is never changed.
3. **Defaults are never validated by pydantic.** After validation every value of the instance is checked against
   its spec, so a value that came from a default or a ``default_factory`` is held to the field's type
   (:func:`~datagrunt.rationalization.schema.admits_scalar`). ``prepare`` refuses a bad static default before any
   request; a factory is never called there, so this is where what it gave is checked. A scalar's exact type must
   be its ``ScalarSpec.base`` (a ``bool`` is no ``int`` or ``float``, a ``date`` no ``datetime``, a ``str`` no Enum
   member), but for two lossless widenings: an ``int`` (never a ``bool``) in a ``float`` field whose magnitude is at
   most 2**53 (:data:`~datagrunt.rationalization.schema.FLOAT_INT_LIMIT`; 2**53 + 1 is the first int with no exact
   float, so a larger one is refused as lossy), and any ``int`` in a ``Decimal`` field. Comparison treats a widened
   int as the field's own type, so ``0`` and ``0.0`` match (rule 6), and the record keeps the int as pydantic left
   it. A non-Optional field
   holding ``None``, any other scalar, an Enum field holding no member of its Enum, a Literal field a value it does
   not list, a datetime of the other kind (aware or naive), a list field holding no ``list``, or a model field an
   instance not exactly of its class quarantines the record (``"validation"``; the message names the field by
   attribute path, and says a default gave the value only when one did, its field never set by the model). A
   non-finite float or ``Decimal`` (lax validation takes ``"Infinity"`` and ``"NaN"``) is quarantined the same way.
4. **One model.** With one requested model (``min_families`` 1), each record is compared as the one voter (a keyed
   root's copies of one key first with each other, rule 9; a root without keys, each record on its own), so rule 9
   holds inside it at every depth: its item is ``single_model``, or ``field_disagreement`` when it repeats a
   child's key with differing values. Its record is the model's own, or rebuilt when a repeated child merged
   (rule 8).
5. **Grouping** (several models). A keyed root's records are grouped by their normalized key tuple
   (:func:`normalized_key`). Values normalize by kind (maintainer, 2026-11-05):

   - a string to its NFC form, stripped, every run of Unicode whitespace made one space. Nothing else folds: no
     compatibility mapping (NFKC) and no casefolding, so ``10⁶`` and ``106``, ``A-1¹`` and ``A-11``, or a
     fullwidth digit and its ASCII digit stay different (and go to review, the safe direction);
   - a float to itself rounded to :data:`FLOAT_SIGNIFICANT_DIGITS` (15) significant digits, so binary noise agrees
     (``0.1 + 0.2`` and ``0.3``) and any real decimal difference disputes (``987654.32`` and ``987654.23``);
   - an ``int`` or ``Decimal`` to itself (equal by numeric value), a date to itself, an aware datetime to its
     instant (in UTC), an Enum member to its value, a bool to itself.

   Key fields never hold floats (a schema rule). A key some requested models returned and others did not (a model
   that failed on the unit, whose records were all quarantined, or that left it out) is an item of status
   ``missing_in_some_models``. A root without keys is paired only when every requested model returned exactly one
   valid record for the unit; otherwise each of its records is an item of status ``unmatched``, never an error
   (the requests are already paid).
6. **Field agreement** (several models; ``N`` requested, ``k`` of them returned the record). Each field's values are
   clustered by their normalized form (rule 5; a list's as the tuple of its items'), which is exact and hashable,
   so matching is transitive and grouping a dictionary lookup; clusters keep model order, and a cluster's first
   value (its representative) is the value it stands for. The merged value is that representative: the first
   backing model's own value (as it sent it, rule 8), which every backer matched under rule 5's normalization and
   nothing looser. A
   field's winning value is the representative of the cluster backed by more than half of the
   ``k`` models; with none (a tie) the field has no winner, never a pick. When ``k == N``, a field is agreed when
   its winning value is backed by more than half of the requested models **and** by at least ``min_families``
   distinct model families (the cross-family rule, for defense in depth), and a ``must_agree`` field (a dotted
   attribute path, :func:`check_must_agree`, and everything under it) only when every requested model backs it.
   When ``k < N``, a field is agreed when every model that has it backs its value. Families count as the distinct
   known families among a value's backers: ``"unknown"`` adds no diversity, so backers of unknown family alone
   count as one.
7. **Nesting.** A nested model compares field by field, an Optional one first as present or ``None`` (a value like
   any other). A ``list[Child]`` whose child has keys matches its children by normalized child key, recursively; a
   child some of the models with its parent left out is disputed at its path, and is kept when more than half of
   the ``k`` models have it, left out when more than half do not, and has no winner otherwise. A list whose child
   has no keys, and a list of scalars, compares as one normalized sequence, in order. Comparison follows the spec,
   so its recursion is bounded by the spec's depth (at most :data:`~datagrunt.rationalization.schema.MAX_DEPTH`).
8. **Statuses and the merged record.** A field not agreed makes the item ``field_disagreement``, naming each
   disputed field's path relative to the record, by attribute name, in field order (``title``,
   ``components['PN-104'].quantity``, ``steps[2]``: a keyed child by its key, a sequence by its differing positions
   when its versions have one length); otherwise a key missing in some models makes it ``missing_in_some_models``,
   and else it is ``consensus`` (``single_model`` with one model). When every field has a winning value the item has
   a merged record; else it is ``None``. Values are compared validated, but the merged record is validated exactly
   once, from JSON as the models sent it, never from validated values (a validator would run twice), so one model
   and several give the same record:

   - when every winning value is one model's own (its representative everywhere) and no child of its lists was
     left out or merged, the record is that model's validated instance, unchanged;
   - else it is rebuilt from the representatives' own JSON: each record as rule 2 left it, a value found under its
     wire name (or its attribute name, in a model that validates by name), a child by its position in the list
     sent, and a part one model gave whole taken whole, so a default stays a default. It is validated once, on a
     copy (rule 1), checked like any record (rule 3), then compared with the records it came from: it must hold
     every winning value and every kept child, and nothing else. A validator that reorders or reshapes what it is
     given can set a value apart from the JSON it was found by; this check sends that record to review.

   A rebuild ``model_validate`` refuses (a cross-field validator), that rule 3 refuses, or that does not hold the
   agreed values makes the item ``field_disagreement`` with the note ``the merged record failed validation: ...``
   (field paths and kinds, never a value) and no record, never an exception.
9. **Repeated keys.** Records of one key from one model in one unit merge when they are identical (rule 6 finds
   no dispute between them), their quotes and element ids united; when they differ, the item is
   ``field_disagreement`` with the note ``<model> returned this key N times, with differing values``, the paths
   where they differ disputed, its record ``None``, and every copy kept. A child key repeated in one model's list,
   at any depth and with one model or several, is likewise one child when identical (the merged record keeps the
   first), else disputed at its path without a winner, with the note ``<model> repeated a child key within a list,
   with differing values``; the model's votes keep every version. Such a child is disputed whole and compared no
   further, so copies of a key count as differing (the first note) only where they differ outside it, or where one
   repeats it and another does not; both notes come when both happen. Notes never hold a value.
10. **Output.** Units in the given order; within a unit, keyed items by key tuple, compared part by part in a
    total order over the kinds of normalized values: ``bool`` < numbers (``int``, ``Decimal``, by value) < ``str``
    < ``date`` < naive ``datetime`` < aware ``datetime`` (by instant); items without keys in model order, then
    response order. Quarantined items come in unit order, then model order, then response order. Every item maps
    each requested model, in the outcome's order, to its validated records (``model_votes``, JSON-mode dicts, ``()``
    for a model without one), quotes, element ids and family.

Bounds: every walk is linear in a response's records and values, and follows the spec, never deeper than its depth;
clustering is a dictionary lookup per value, so it is linear too, across models and across one model's repeated
copies of a key (rule 9) alike. Each record is copied once to be validated, and a rebuilt record is compared once
more with the records it came from (rule 8), both linear. The module imports only the standard
library, pydantic and its sibling modules: nothing from ``datagrunt.core``, and no import inside a function
(invariant 9).
"""

import re
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Literal, TypeAlias, TypeVar, cast, get_args

from pydantic import BaseModel, ValidationError

from datagrunt.rationalization._canonical import canonical_json
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.base import UNKNOWN_FAMILY, family_problem
from datagrunt.rationalization.runner import RunOutcome
from datagrunt.rationalization.schema import (
    MAX_DEPTH,
    FieldSpec,
    ModelSpec,
    PreparedSchema,
    instance_problems,
)
from datagrunt.rationalization.wire import ProviderResponse

QuarantineReason: TypeAlias = Literal["validation", "unparseable", "truncated", "refused"]
"""Why a record or a response could not be used."""

QUARANTINE_REASONS: tuple[QuarantineReason, ...] = get_args(QuarantineReason)

ConsensusStatus: TypeAlias = Literal[
    "consensus", "single_model", "missing_in_some_models", "field_disagreement", "unmatched"
]
"""What consensus concluded about an item (module notes, 4-9)."""

CONSENSUS_STATUSES: tuple[ConsensusStatus, ...] = get_args(ConsensusStatus)

ERROR_MESSAGE_MAX_CHARS = 2_000
"""The longest :attr:`QuarantinedItem.error_message`."""

RAW_JSON_MAX_CHARS = 65_536
"""The longest :attr:`QuarantinedItem.raw_json`: a longer text is cut, and says so."""

QUOTES_MAX = 256
"""The most grounding quotes a :class:`ValidRecord` or wrapper entry may hold."""

QUOTE_MAX_CHARS = 16_384
"""The longest grounding quote a :class:`ValidRecord` or wrapper entry may hold."""

SOURCE_IDS_MAX = 256
"""The most element ids a :class:`QuarantinedItem` keeps (the entry's first ones)."""

SOURCE_ID_MAX_CHARS = 256
"""The longest element id a :class:`QuarantinedItem` keeps: a longer one is cut, and says so."""

FLOAT_SIGNIFICANT_DIGITS = 15
"""Two floats match when they are equal once each is rounded to this many significant digits (maintainer, 2026-11-05):
this absorbs binary noise (``0.1 + 0.2`` is ``0.3``), and any real decimal difference disputes."""

__all__ = [
    "CONSENSUS_STATUSES",
    "ConsensusItem",
    "ConsensusResult",
    "ConsensusStatus",
    "ERROR_MESSAGE_MAX_CHARS",
    "FLOAT_SIGNIFICANT_DIGITS",
    "QUARANTINE_REASONS",
    "QUOTES_MAX",
    "QUOTE_MAX_CHARS",
    "QuarantineReason",
    "QuarantinedItem",
    "RAW_JSON_MAX_CHARS",
    "SOURCE_IDS_MAX",
    "SOURCE_ID_MAX_CHARS",
    "ValidRecord",
    "ValidatedResponse",
    "build_consensus",
    "check_must_agree",
    "key_order",
    "normalized_key",
    "normalized_text",
    "validate_response",
]

_BRIEF_CHARS = 80
_ABSENT = object()
_REBUILT = -1  # the entry index of a rebuilt record, compared with the records it was merged from
_LIST_KINDS = frozenset({"scalar_list", "model_list"})
_CLEAN_STATUSES = ("consensus", "single_model")
_WRAPPER_SHAPE = '{"records": [...]}'
_WRAPPER_KEYS_SET = frozenset({"records"})
_ENTRY_KEYS = ("grounding_quotes", "record", "source_element_ids")
_ENTRY_KEYS_SET = frozenset(_ENTRY_KEYS)
_ENTRY_SHAPE = '{"record": {...}, "grounding_quotes": [str, ...], "source_element_ids": [str, ...]}'
_FLOAT_FMT = f".{FLOAT_SIGNIFICANT_DIGITS}g"
_V = TypeVar("_V")


# ---------------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------------


def _brief(value: object) -> str:
    """``repr(value)``, cut to :data:`_BRIEF_CHARS` characters, for a message."""
    text = repr(value)
    return text if len(text) <= _BRIEF_CHARS else f"{text[: _BRIEF_CHARS - 3]}..."


def _check_text(name: str, value: object) -> str:
    """Require a non-blank ``str``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"{name} must be non-blank")
    return value


def _check_bounded(name: str, value: object, limit: int) -> str:
    """Require a ``str`` of at most ``limit`` characters."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if len(value) > limit:
        raise ValueError(f"{name} must be at most {limit} characters, got {len(value)}")
    return value


def _check_choice(name: str, value: object, choices: tuple[str, ...]) -> None:
    """Require one of ``choices``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if value not in choices:
        raise ValueError(f"{name} must be one of {choices}, got {_brief(value)}")


def _check_strings(name: str, value: object) -> tuple[str, ...]:
    """Require a tuple of ``str``."""
    if not isinstance(value, tuple) or not all(isinstance(item, str) for item in value):
        raise TypeError(f"{name} must be a tuple of str, got {_brief(value)}")
    return value


def _check_tuple_of(name: str, value: object, kind: type) -> None:
    """Require a tuple of ``kind`` instances."""
    if not isinstance(value, tuple) or not all(isinstance(item, kind) for item in value):
        raise TypeError(f"{name} must be a tuple of {kind.__name__}")


def _check_votes(name: str, value: object) -> tuple[Mapping[str, Any], ...]:
    """Require a tuple of mappings (JSON-mode records)."""
    if not isinstance(value, tuple) or not all(isinstance(item, Mapping) for item in value):
        raise TypeError(f"{name} must be a tuple of mappings, got {_brief(value)}")
    return value


def _check_family(name: str, value: object) -> str:
    """Require a ``str`` (a model family)."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    return value


def _read_only(name: str, value: object, check: Callable[[str, object], _V]) -> Mapping[str, _V]:
    """A read-only copy of a mapping of model ids (``str``) to values that pass ``check``."""
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a Mapping, got {type(value).__name__}")
    copy: dict[str, _V] = {}
    for model_id, item in value.items():
        if not isinstance(model_id, str):
            raise TypeError(f"{name} keys must be model ids (str), got {_brief(model_id)}")
        copy[model_id] = check(f"{name}[{model_id!r}]", item)
    return MappingProxyType(copy)


# ---------------------------------------------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QuarantinedItem:
    """A response, or one record of it, that consensus could not use (module notes, 1-3).

    Args:
        model_id: The model that returned it.
        unit_id: The unit it answers.
        reason: One of :data:`QUARANTINE_REASONS`.
        raw_json: What the model returned, cut to :data:`RAW_JSON_MAX_CHARS`: the canonical JSON of the record (a
            ``"validation"`` item) or of the entry (an ``"unparseable"`` entry), else the response's text, or its
            content's canonical JSON when it has no text; possibly empty. It can hold document text, so the
            ``repr`` leaves it out.
        error_message: Why, in at most :data:`ERROR_MESSAGE_MAX_CHARS` characters: field paths and error types,
            never an input value. A path can still be model-chosen text (an extra key a model sent, a key of the
            response), so the ``repr`` leaves it out too; it stays in the data, for review.
        source_element_ids: The element ids the entry cited, for audit: its first :data:`SOURCE_IDS_MAX`, each cut
            to :data:`SOURCE_ID_MAX_CHARS` characters; ``()`` for an item of a whole response, or an entry whose ids
            are not a list of strings. Model-chosen text, so left out of the ``repr``.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If an id or the message is blank, ``reason`` is unknown, or a text is past its bound.
    """

    model_id: str
    unit_id: str
    reason: QuarantineReason
    raw_json: str = field(repr=False)
    error_message: str = field(repr=False)
    source_element_ids: tuple[str, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        _check_text("model_id", self.model_id)
        _check_text("unit_id", self.unit_id)
        _check_choice("reason", self.reason, QUARANTINE_REASONS)
        _check_bounded("raw_json", self.raw_json, RAW_JSON_MAX_CHARS)
        _check_text("error_message", self.error_message)
        _check_bounded("error_message", self.error_message, ERROR_MESSAGE_MAX_CHARS)
        ids = _check_strings("source_element_ids", self.source_element_ids)
        if len(ids) > SOURCE_IDS_MAX or any(len(element_id) > SOURCE_ID_MAX_CHARS for element_id in ids):
            raise ValueError(
                f"source_element_ids holds at most {SOURCE_IDS_MAX} ids of at most {SOURCE_ID_MAX_CHARS} characters"
            )


@dataclass(frozen=True, slots=True)
class ValidRecord:
    """One record of a response that passed validation (module notes, 1-3), with its grounding.

    Args:
        record: The validated instance of the schema's model.
        grounding_quotes: The quotes the model gave for it, in its order.
        source_element_ids: The element ids the model gave for it, in its order.
        position: Its index among the response's entries (0-based).
        wire: Keyword-only. The record as the model sent it, less the nulls that mean "not shown" (module notes,
            2): a copy of exactly what ``record`` was validated from, by wire name, which a merged record is
            rebuilt from (module notes, 8). It holds document text, so the ``repr`` leaves it out.

    Raises:
        TypeError: If a field has the wrong type (``wire`` is a ``dict`` with ``str`` keys).
        ValueError: If ``position`` is negative, or a quote or element id is past its bound.
    """

    record: BaseModel = field(repr=False)
    grounding_quotes: tuple[str, ...] = field(repr=False)
    source_element_ids: tuple[str, ...]
    position: int
    wire: dict[str, Any] = field(kw_only=True, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.record, BaseModel):
            raise TypeError(f"record must be a pydantic model instance, got {type(self.record).__name__}")
        if not isinstance(self.wire, dict) or not all(isinstance(name, str) for name in self.wire):
            raise TypeError(f"wire must be a dict with str keys (a JSON object), got {type(self.wire).__name__}")
        _check_strings("grounding_quotes", self.grounding_quotes)
        _check_strings("source_element_ids", self.source_element_ids)
        if len(self.grounding_quotes) > QUOTES_MAX or any(len(q) > QUOTE_MAX_CHARS for q in self.grounding_quotes):
            raise ValueError(
                f"grounding_quotes holds at most {QUOTES_MAX} quotes of at most {QUOTE_MAX_CHARS} characters"
            )
        if len(self.source_element_ids) > SOURCE_IDS_MAX or any(
            len(element_id) > SOURCE_ID_MAX_CHARS for element_id in self.source_element_ids
        ):
            raise ValueError(
                f"source_element_ids holds at most {SOURCE_IDS_MAX} ids of at most {SOURCE_ID_MAX_CHARS} characters"
            )
        if type(self.position) is not int:
            raise TypeError(f"position must be an int, got {type(self.position).__name__}")
        if self.position < 0:
            raise ValueError(f"position must be >= 0, got {self.position}")

    @classmethod
    def _trusted(
        cls,
        *,
        record: BaseModel,
        grounding_quotes: tuple[str, ...],
        source_element_ids: tuple[str, ...],
        position: int,
        wire: dict[str, Any],
    ) -> "ValidRecord":
        """Construct an internal :class:`ValidRecord` from pre-checked wrapper parts."""
        if not isinstance(record, BaseModel) or not isinstance(wire, dict):
            raise TypeError("ValidRecord._trusted requires a BaseModel record and dict wire")
        self = object.__new__(cls)
        object.__setattr__(self, "record", record)
        object.__setattr__(self, "grounding_quotes", grounding_quotes)
        object.__setattr__(self, "source_element_ids", source_element_ids)
        object.__setattr__(self, "position", position)
        object.__setattr__(self, "wire", wire)
        return self


@dataclass(frozen=True, slots=True)
class ValidatedResponse:
    """What :func:`validate_response` made of one response: its valid records and its quarantined items, in order.

    Raises:
        TypeError: If a field is not a tuple of its type.
    """

    records: tuple[ValidRecord, ...]
    quarantined: tuple[QuarantinedItem, ...]

    def __post_init__(self) -> None:
        _check_tuple_of("records", self.records, ValidRecord)
        _check_tuple_of("quarantined", self.quarantined, QuarantinedItem)


@dataclass(frozen=True, slots=True)
class ConsensusItem:
    """One entity of one unit, as the requested models described it (module notes, 4-10).

    Args:
        unit_id: The unit it was found in.
        status: One of :data:`CONSENSUS_STATUSES`.
        key: Its normalized root key (:func:`normalized_key`), ``()`` for a root without keys: what T12 folds on.
        record: The merged record (module notes, 8: a model's own validated record when it is entirely that
            model's, else rebuilt once from what the models sent), or ``None``: for ``unmatched``, when a field has
            no winning value, when one model's copies differ or a child key repeats with differing values, or when
            the rebuild failed.
        disputed_fields: The paths of the fields not agreed, in field order, each once (module notes, 8); ``()``
            for ``consensus`` and ``single_model``.
        notes: What else needs review (a failed rebuild, a model's differing copies, a child key it repeated with
            differing values), else ``()``. A path or a
            note can hold model-chosen text (a child's key, an extra key a rebuild refused), so the ``repr`` leaves
            both out, like every field holding document text; they stay in the data, for review.
        model_votes: Each requested model's validated records for this item, as JSON-mode dicts (``()`` for a model
            without one), in the outcome's model order; held as a read-only mapping.
        quotes_by_model: Each requested model's grounding quotes for this item (its copies' united), same order.
        element_ids_by_model: Each requested model's source element ids for this item, same order.
        families: Each requested model's family, same order.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If ``status`` is unknown or a path blank, the mappings name no model or not the same ones, no
            model has a record, a ``consensus`` or ``single_model`` item has no record or names a disputed field
            or note, a ``field_disagreement`` names neither, a ``single_model`` item has several models, or an
            ``unmatched`` one more than one record.
    """

    unit_id: str
    status: ConsensusStatus
    key: tuple[object, ...] = field(repr=False)
    record: BaseModel | None = field(repr=False)
    disputed_fields: tuple[str, ...] = field(repr=False)
    notes: tuple[str, ...] = field(repr=False)
    model_votes: Mapping[str, tuple[Mapping[str, Any], ...]] = field(repr=False)
    quotes_by_model: Mapping[str, tuple[str, ...]] = field(repr=False)
    element_ids_by_model: Mapping[str, tuple[str, ...]] = field(repr=False)
    families: Mapping[str, str]

    def __post_init__(self) -> None:
        _check_text("unit_id", self.unit_id)
        _check_choice("status", self.status, CONSENSUS_STATUSES)
        if not isinstance(self.key, tuple):
            raise TypeError(f"key must be a tuple, got {type(self.key).__name__}")
        if self.record is not None and not isinstance(self.record, BaseModel):
            raise TypeError(f"record must be a pydantic model instance or None, got {type(self.record).__name__}")
        _check_strings("disputed_fields", self.disputed_fields)
        if not all(path.strip() for path in self.disputed_fields):
            raise ValueError("disputed_fields must hold non-blank paths")
        _check_strings("notes", self.notes)
        object.__setattr__(self, "model_votes", _read_only("model_votes", self.model_votes, _check_votes))
        object.__setattr__(self, "quotes_by_model", _read_only("quotes_by_model", self.quotes_by_model, _check_strings))
        ids = _read_only("element_ids_by_model", self.element_ids_by_model, _check_strings)
        object.__setattr__(self, "element_ids_by_model", ids)
        object.__setattr__(self, "families", _read_only("families", self.families, _check_family))
        self._check_shape()

    def _check_shape(self) -> None:
        """Check that the mappings name the same models, and that the status fits the record and the votes."""
        models = list(self.model_votes)
        others = (self.quotes_by_model, self.element_ids_by_model, self.families)
        if not models or any(list(mapping) != models for mapping in others):
            raise ValueError(
                "model_votes, quotes_by_model, element_ids_by_model and families must name the same models"
            )
        counts = sorted(len(votes) for votes in self.model_votes.values())
        if not counts[-1]:
            raise ValueError("an item needs a record from at least one model")
        if self.status in _CLEAN_STATUSES and (self.record is None or self.disputed_fields or self.notes):
            raise ValueError(f"a {self.status} item has a record, and no disputed field or note")
        if self.status == "field_disagreement" and not (self.disputed_fields or self.notes):
            raise ValueError("a field_disagreement item names a disputed field or a note")
        if self.status == "single_model" and len(models) != 1:
            raise ValueError("a single_model item has one model")
        if self.status == "unmatched" and counts != [0] * (len(counts) - 1) + [1]:
            raise ValueError("an unmatched item holds exactly one record")

    @classmethod
    def _trusted(
        cls,
        *,
        unit_id: str,
        status: ConsensusStatus,
        key: tuple[object, ...],
        record: BaseModel | None,
        disputed_fields: tuple[str, ...],
        notes: tuple[str, ...],
        model_votes: Mapping[str, tuple[Mapping[str, Any], ...]],
        quotes_by_model: Mapping[str, tuple[str, ...]],
        element_ids_by_model: Mapping[str, tuple[str, ...]],
        families: Mapping[str, str],
    ) -> "ConsensusItem":
        """Construct an internal :class:`ConsensusItem` from read-only per-model mappings."""
        if not unit_id or not isinstance(key, tuple):
            raise ValueError("ConsensusItem._trusted requires non-empty unit_id and tuple key")
        self = object.__new__(cls)
        object.__setattr__(self, "unit_id", unit_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "record", record)
        object.__setattr__(self, "disputed_fields", disputed_fields)
        object.__setattr__(self, "notes", notes)
        object.__setattr__(self, "model_votes", model_votes)
        object.__setattr__(self, "quotes_by_model", quotes_by_model)
        object.__setattr__(self, "element_ids_by_model", element_ids_by_model)
        object.__setattr__(self, "families", families)
        return self


@dataclass(frozen=True, slots=True)
class ConsensusResult:
    """Every item and every quarantined record of a run, in the order of module notes, 10.

    Raises:
        TypeError: If a field is not a tuple of its type.
    """

    items: tuple[ConsensusItem, ...]
    quarantined: tuple[QuarantinedItem, ...]

    def __post_init__(self) -> None:
        _check_tuple_of("items", self.items, ConsensusItem)
        _check_tuple_of("quarantined", self.quarantined, QuarantinedItem)

    @classmethod
    def _trusted(cls, items: tuple[ConsensusItem, ...], quarantined: tuple[QuarantinedItem, ...]) -> "ConsensusResult":
        """Construct an internal :class:`ConsensusResult` from verified item and quarantine tuples."""
        if not isinstance(items, tuple) or not isinstance(quarantined, tuple):
            raise TypeError("ConsensusResult._trusted requires item and quarantine tuples")
        self = object.__new__(cls)
        object.__setattr__(self, "items", items)
        object.__setattr__(self, "quarantined", quarantined)
        return self


# ---------------------------------------------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------------------------------------------


def check_must_agree(spec: ModelSpec, paths: object) -> tuple[str, ...]:
    """``paths`` as the dotted field paths ``must_agree`` names, each checked against ``spec``.

    A path names attributes (not aliases) from the root, through model and list-of-model fields, without list
    indices or keys (``"components.quantity"`` names every component's quantity). It may name a field of any kind; a
    model or list field makes everything under it need unanimity. The API calls it before any request is sent.

    Args:
        spec: The schema's root spec.
        paths: A sequence of dotted paths (not a ``str``), possibly empty.

    Returns:
        The paths as a tuple, in their order.

    Raises:
        RationalizationError: If ``paths`` is not such a sequence, a path is not a non-empty ``str``, has an empty
            part, is repeated, or names no field (the message names the path and the fields there).
    """
    if isinstance(paths, str):
        raise RationalizationError("must_agree must be a sequence of dotted field paths, not a str: wrap it, as ('x',)")
    if not isinstance(paths, Sequence) or isinstance(paths, (bytes, bytearray)):
        found = "None" if paths is None else type(paths).__name__
        raise RationalizationError(f"must_agree must be a sequence of dotted field paths, got {found}")
    checked: list[str] = []
    for path in paths:
        if not isinstance(path, str) or not path:
            raise RationalizationError(f"must_agree entries must be non-empty str paths, got {_brief(path)}")
        if path in checked:
            raise RationalizationError(f"must_agree names {path!r} more than once")
        _check_path(spec, path)
        checked.append(path)
    return tuple(checked)


_ASCII_PUNCT_OR_SPACE = re.compile(r"[\s!-/:-@\[-`{-~]+")
_WORD_TOKENS = re.compile(r"\w+")


def normalized_text(value: str) -> str:
    """``value`` in consensus's canonical string form (module notes, 5): NFC, whitespace runs made one space.

    Raises:
        TypeError: If ``value`` is not a ``str``.
    """
    if not isinstance(value, str):
        raise TypeError(f"value must be a str, got {type(value).__name__}")
    if value.isascii():
        if value.isprintable() and (not value or (value[0] != " " and value[-1] != " " and "  " not in value)):
            return value
        return " ".join(value.split())
    if unicodedata.is_normalized("NFC", value):
        return " ".join(value.split())
    return " ".join(unicodedata.normalize("NFC", value).split())


def alnum_upper_text(value: str) -> str:
    """Return ``value`` NFC-normalized with ASCII punctuation/whitespace stripped and uppercased."""
    return _ASCII_PUNCT_OR_SPACE.sub("", normalized_text(value)).upper()


def normalized_key(record: BaseModel, spec: ModelSpec) -> tuple[object, ...]:
    """The normalized key tuple of ``record`` (module notes, 5): hashable, and equal for records of one entity.

    Args:
        record: A validated instance of ``spec.model``.
        spec: Its spec.

    Returns:
        One normalized value per key field, in key order; ``()`` when the model has no keys.

    Raises:
        TypeError: If ``record`` is not an instance of ``spec.model``.
    """
    if not isinstance(record, spec.model):
        raise TypeError(f"record must be a {spec.model.__name__}, got {type(record).__name__}")
    return _key_of(record, spec)


def key_order(key: tuple[object, ...]) -> tuple[tuple[int, Any], ...]:
    """A sort key that places a normalized key (:func:`normalized_key`) in the total order of module notes, 10.

    Parts compare in turn: ``bool`` < numbers (``int``, ``Decimal``, by value) < ``str`` < ``date`` < naive
    ``datetime`` < aware ``datetime`` (by instant); kinds that do not compare with each other get ranks of their
    own, so any two keys of one model order without error. Items of a unit come in this order; T12 can order folded
    records the same way.

    Args:
        key: A normalized key tuple.

    Returns:
        A tuple of ``(rank, value)`` pairs, one per part.
    """
    return tuple(_order_part(value) for value in key)


def validate_response(
    response: ProviderResponse, schema: PreparedSchema, *, unit_id: str, model_id: str
) -> ValidatedResponse:
    """Validate one (unit, model) response (module notes, 1-3).

    Args:
        response: The response; it is never changed.
        schema: The prepared schema.
        unit_id: The unit it answers.
        model_id: The model that returned it.

    Returns:
        Its valid records and its quarantined items, each in response order.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If an id is blank.
    """
    if not isinstance(response, ProviderResponse):
        raise TypeError(f"response must be a ProviderResponse, got {type(response).__name__}")
    if not isinstance(schema, PreparedSchema):
        raise TypeError(f"schema must be a PreparedSchema, got {type(schema).__name__}")
    origin = _Origin(model_id=_check_text("model_id", model_id), unit_id=_check_text("unit_id", unit_id))
    quarantine: list[QuarantinedItem] = []
    records = _validate_response_records(response, schema, origin, quarantine, isolate_wire=True)
    return ValidatedResponse(records=records, quarantined=tuple(quarantine))


def _validate_response_records(
    response: ProviderResponse,
    schema: PreparedSchema,
    origin: "_Origin",
    quarantine: list[QuarantinedItem],
    *,
    isolate_wire: bool,
) -> tuple[ValidRecord, ...]:
    """Validate ``response`` into valid records and append quarantined items to ``quarantine``."""
    if not isinstance(response, ProviderResponse) or not isinstance(schema, PreparedSchema):
        raise TypeError("response and schema must be ProviderResponse and PreparedSchema")
    if response.finish != "complete":
        quarantine.append(_finish_item(origin, response))
        return ()
    entries = _wrapper_entries(response)
    if isinstance(entries, str):
        quarantine.append(origin.item("unparseable", _response_text(response), entries))
        return ()
    records: list[ValidRecord] = []
    for position, entry in enumerate(entries):
        parts = _entry_parts(entry)
        if isinstance(parts, str):
            message = f"records[{position}] is not {_ENTRY_SHAPE}: {parts}"
            quarantine.append(origin.item("unparseable", _json_text(entry), message, _cited_ids(entry)))
            continue
        record, quotes, ids = parts
        checked = _validate_record(record, schema, isolate_wire=isolate_wire)
        if isinstance(checked, str):
            quarantine.append(origin.item("validation", _json_text(record), f"records[{position}]: {checked}", ids))
            continue
        instance, wire = checked
        records.append(
            ValidRecord._trusted(
                record=instance, grounding_quotes=quotes, source_element_ids=ids, position=position, wire=wire
            )
        )
    return tuple(records)


def build_consensus(
    outcome: RunOutcome,
    schema: PreparedSchema,
    unit_ids: Sequence[str],
    *,
    min_families: int,
    must_agree: Sequence[str] = (),
) -> ConsensusResult:
    """Validate every response of ``outcome`` and form each unit's consensus (module notes).

    Args:
        outcome: The run's outcome; its ``models`` give the requested models, their order and their families.
        schema: The prepared schema the run used.
        unit_ids: Every unit id of the run, in document order; each response and failed unit must name one.
        min_families: The distinct model families an agreed value needs (module notes, 6), 1 to the number of
            models.
        must_agree: Dotted field paths that need every requested model (:func:`check_must_agree`).

    Returns:
        The :class:`ConsensusResult`.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If ``unit_ids`` is empty or repeats or blanks an id, a response or failed unit names a unit or
            model the run does not, the outcome names no model, repeats a model id or holds a bad family,
            ``min_families`` is out of range, or the outcome's models span fewer distinct known families than
            ``min_families`` (``"unknown"`` adds none; models of unknown family alone count as one), so no value
            could be agreed.
        RationalizationError: If ``must_agree`` is malformed or names no field (:func:`check_must_agree`).
    """
    context = _check_run(outcome, schema, min_families, must_agree)
    units = _check_units(unit_ids, outcome, context)
    items: list[ConsensusItem] = []
    quarantine: list[QuarantinedItem] = []
    for unit_id in units:
        votes = _unit_votes(outcome, context, unit_id, quarantine)
        items.extend(_unit_items(unit_id, votes, context))
    return ConsensusResult._trusted(items=tuple(items), quarantined=tuple(quarantine))


# ---------------------------------------------------------------------------------------------------------------
# Quarantine (validate_response)
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Origin:
    """The (model, unit) a response answers, for its quarantined items."""

    model_id: str
    unit_id: str

    def item(self, reason: QuarantineReason, raw_json: str, message: str, ids: Sequence[str] = ()) -> QuarantinedItem:
        """A quarantined item of this origin, its texts and element ids cut to their bounds."""
        return QuarantinedItem(
            model_id=self.model_id,
            unit_id=self.unit_id,
            reason=reason,
            raw_json=_cut(raw_json, RAW_JSON_MAX_CHARS),
            error_message=_cut(message, ERROR_MESSAGE_MAX_CHARS),
            source_element_ids=tuple(_cut(element_id, SOURCE_ID_MAX_CHARS) for element_id in ids[:SOURCE_IDS_MAX]),
        )


def _cut(text: str, limit: int) -> str:
    """``text``, or its first characters and a note of its length when it is longer than ``limit``."""
    if len(text) <= limit:
        return text
    note = f" ... [cut: {len(text)} characters]"
    return f"{text[: limit - len(note)]}{note}"


def _finish_item(origin: _Origin, response: ProviderResponse) -> QuarantinedItem:
    """The one item of a ``truncated`` or ``refused`` response."""
    reason: QuarantineReason = "truncated" if response.finish == "truncated" else "refused"
    why = "the output limit cut it" if reason == "truncated" else "the model or its safety layer declined"
    return origin.item(reason, _response_text(response), f"the response was {reason} ({why}); nothing was parsed")


def _response_text(response: ProviderResponse) -> str:
    """What a response said: its text, else its content's canonical JSON, else ``""``."""
    if response.raw_text:
        return response.raw_text
    return "" if response.content is None else _json_text(response.content)


def _json_text(value: object) -> str:
    """``value``'s canonical JSON (a response's content is exactly JSON, so every part of it has one)."""
    return canonical_json(value, subject="the response content")


def _wrapper_entries(response: ProviderResponse) -> list[object] | str:
    """The entries of a complete response's ``records``, or why its content is not the wrapper."""
    content = response.content
    if content is None:
        return "the response holds no JSON object (it did not parse)"
    if content.keys() != _WRAPPER_KEYS_SET:
        return f"the response is not the wrapper {_WRAPPER_SHAPE}: its keys are {_brief(sorted(content))}"
    entries = content["records"]
    if not isinstance(entries, list):
        return f'the response is not the wrapper {_WRAPPER_SHAPE}: "records" is a {_json_kind(entries)}'
    return entries


def _entry_parts(entry: object) -> tuple[dict[str, Any], tuple[str, ...], tuple[str, ...]] | str:
    """A wrapper entry's record, quotes and element ids, or why it is not an entry."""
    if not isinstance(entry, dict):
        return f"it is a {_json_kind(entry)}"
    if entry.keys() != _ENTRY_KEYS_SET:
        return f"its keys are {_brief(sorted(entry))}"
    record = entry["record"]
    if not isinstance(record, dict):
        return f'"record" is a {_json_kind(record)}'
    quotes = entry["grounding_quotes"]
    ids = entry["source_element_ids"]
    for name, value in (("grounding_quotes", quotes), ("source_element_ids", ids)):
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            return f'"{name}" is not a list of strings'
    if len(quotes) > QUOTES_MAX or any(len(q) > QUOTE_MAX_CHARS for q in quotes):
        return (
            f'"grounding_quotes" holds more than {QUOTES_MAX} quotes or a quote longer than '
            f"{QUOTE_MAX_CHARS} characters"
        )
    if len(ids) > SOURCE_IDS_MAX or any(len(element_id) > SOURCE_ID_MAX_CHARS for element_id in ids):
        return (
            f'"source_element_ids" holds more than {SOURCE_IDS_MAX} ids or an id longer than '
            f"{SOURCE_ID_MAX_CHARS} characters"
        )
    return record, tuple(quotes), tuple(ids)


def _cited_ids(entry: object) -> tuple[str, ...]:
    """The element ids a malformed entry cites, when they are a list of strings, else ``()``."""
    ids = entry.get("source_element_ids") if isinstance(entry, dict) else None
    if isinstance(ids, list) and all(isinstance(element_id, str) for element_id in ids):
        return tuple(ids)
    return ()


def _json_kind(value: object) -> str:
    """The JSON kind of a parsed value, for a message (never the value itself)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    return "array" if isinstance(value, list) else "object"


def _validate_record(
    record: dict[str, Any],
    schema: PreparedSchema,
    *,
    isolate_wire: bool = True,
) -> tuple[BaseModel, dict[str, Any]] | str:
    """The validated instance of ``record`` and what it was validated from, or why it is not one (module notes, 2-3)."""
    wire = _drop_unshown(record, schema.spec) if isolate_wire else _drop_unshown_cow(record, schema.spec)
    checked = _validated(wire, schema)
    return checked if isinstance(checked, str) else (checked, wire)


def _validated(wire: object, schema: PreparedSchema) -> BaseModel | str:
    """``model_validate`` of ``wire``, checked like any record (module notes, 1 and 3), or why it is not one.

    Every record goes through it once: each model's as it sent it, and a merged one rebuilt (module notes, 8). A
    validator may change what it is given in place (pydantic hands a ``mode="before"`` validator the very dict),
    so non-pure schemas get a deep copy and ``wire`` stays what the models sent.
    """
    given = wire if schema.is_pure_pydantic else _json_copy(wire)
    try:
        instance = schema.model.model_validate(given)
    except ValidationError as error:
        return _error_text(error)
    except (MemoryError, RecursionError):
        raise
    except Exception as error:  # a validator of the caller's schema raised what pydantic does not wrap
        return _raised_text(error)
    problems = instance_problems(instance, schema.spec)
    return "; ".join(problems) if problems else instance


def _json_copy(value: object) -> Any:
    """A deep copy of ``value``, exactly JSON (a response's content is, and so is every part of it)."""
    if type(value) is dict:
        return {key: _json_copy(item) for key, item in value.items()}
    if type(value) is list:
        return [_json_copy(item) for item in value]
    return value


def _error_text(error: ValidationError) -> str:
    """A validation error as its count and each error's field path and type, never an input value."""
    found = error.errors(include_url=False, include_input=False, include_context=False)
    listed = "; ".join(f"{_loc_text(detail['loc'])}: {detail['type']}" for detail in found)
    return f"{len(found)} validation error{'' if len(found) == 1 else 's'}: {listed}"


def _raised_text(error: Exception) -> str:
    """An exception a validator raised, by its class only: its message can quote what the model sent."""
    return f"model_validate raised {type(error).__name__} (from a validator of the schema; its message is not kept)"


def _loc_text(loc: tuple[int | str, ...]) -> str:
    """A pydantic error location as a dotted path with list indices (``components[0].quantity``)."""
    text = ""
    for part in loc:
        text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else str(part))
    return text or "(the record)"


def _drop_unshown(record: dict[str, Any], spec: ModelSpec) -> dict[str, Any]:
    """An isolated copy of ``record`` without the nulls that mean "not shown" (module notes, 2)."""
    cleaned = _drop_unshown_cow(record, spec)
    copied: dict[str, Any] = _json_copy(cleaned)
    return copied


def _drop_unshown_cow(record: dict[str, Any], spec: ModelSpec, depth: int = 1) -> dict[str, Any]:
    """Return ``record`` without unshown nulls, copying containers only along mutated paths."""
    if not isinstance(record, dict):
        raise TypeError(f"record must be a dict, got {type(record).__name__}")
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    if not spec.has_unshown_droppable:
        return record
    copied: dict[str, Any] | None = None
    by_name = spec.reads_by_name
    for field_spec in spec.fields:
        copied = _drop_key_cow(record, copied, field_spec, field_spec.wire_name, depth)
        if by_name and field_spec.name != field_spec.wire_name:
            copied = _drop_key_cow(record, copied, field_spec, field_spec.name, depth)
    return record if copied is None else copied


def _drop_key_cow(
    record: dict[str, Any],
    copied: dict[str, Any] | None,
    field_spec: FieldSpec,
    name: str,
    depth: int,
) -> dict[str, Any] | None:
    """Apply null-not-shown CoW rule for ``name`` on ``record``, returning ``copied`` when mutated."""
    if not name or depth < 1:
        raise ValueError("_drop_key_cow requires non-empty name and depth >= 1")
    value = record.get(name, _ABSENT)
    if value is _ABSENT:
        return copied
    if value is None and not field_spec.optional and not field_spec.required:
        target = dict(record) if copied is None else copied
        del target[name]
        return target
    updated = _drop_unshown_child(field_spec, value, depth)
    if updated is not value:
        target = dict(record) if copied is None else copied
        target[name] = updated
        return target
    return copied


def _drop_unshown_child(field_spec: FieldSpec, value: object, depth: int) -> object:
    """Return ``value`` with unshown nulls dropped in child records (copy-on-write)."""
    if depth < 1 or not isinstance(field_spec, FieldSpec):
        raise ValueError("_drop_unshown_child requires FieldSpec and depth >= 1")
    child = field_spec.child
    if child is None or not child.has_unshown_droppable:
        return value
    if field_spec.kind == "model":
        return _drop_unshown_cow(value, child, depth + 1) if isinstance(value, dict) else value
    if field_spec.kind != "model_list" or not isinstance(value, list):
        return value
    copied_list: list[Any] | None = None
    for idx, item in enumerate(value):
        updated = _drop_unshown_cow(item, child, depth + 1) if isinstance(item, dict) else item
        if updated is not item:
            copied_list = list(value) if copied_list is None else copied_list
            copied_list[idx] = updated
    return value if copied_list is None else copied_list


# ---------------------------------------------------------------------------------------------------------------
# The run's checks (build_consensus)
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Context:
    """What every unit's consensus reads: the schema, the requested models in order, and the rules' parameters."""

    schema: PreparedSchema
    model_ids: tuple[str, ...]
    families: tuple[str, ...]
    families_map: Mapping[str, str]
    min_families: int
    must_agree: frozenset[str]


def _check_path(spec: ModelSpec, path: str) -> None:
    """Raise unless the dotted ``path`` names a field of ``spec``, through model fields only."""
    parts = path.split(".")
    if not all(parts):
        raise RationalizationError(f"must_agree path {path!r} has an empty part")
    node: ModelSpec | None = spec
    previous = ""
    for part in parts:
        if node is None:
            raise RationalizationError(f"must_agree path {path!r}: {previous!r} holds no model, so nothing is under it")
        found = node.fields_by_name.get(part)
        if found is None:
            names = ", ".join(field_spec.name for field_spec in node.fields)
            raise RationalizationError(
                f"must_agree path {path!r}: {node.model.__name__} has no field {part!r} (its fields: {names}); "
                f"paths name attributes, not aliases"
            )
        node, previous = found.child, part


def _check_run(outcome: object, schema: object, min_families: object, must_agree: object) -> _Context:
    """The run's context, every argument checked but the unit ids."""
    if not isinstance(outcome, RunOutcome):
        raise TypeError(f"outcome must be a RunOutcome, got {type(outcome).__name__}")
    if not isinstance(schema, PreparedSchema):
        raise TypeError(f"schema must be a PreparedSchema, got {type(schema).__name__}")
    model_ids = tuple(model[0] for model in outcome.models)
    families = tuple(model[1] for model in outcome.models)
    if not model_ids:
        raise ValueError("the outcome names no model")
    if len(set(model_ids)) != len(model_ids):
        raise ValueError("the outcome names a model id more than once")
    for family in families:
        problem = family_problem(family)
        if problem is not None:
            raise ValueError(f"the outcome's model family {problem}")
    if type(min_families) is not int:
        raise TypeError(f"min_families must be an int, got {type(min_families).__name__}")
    if not 1 <= min_families <= len(model_ids):
        raise ValueError(f"min_families must be in 1-{len(model_ids)} (the number of models), got {min_families}")
    spanned = _family_count(range(len(model_ids)), families)
    if spanned < min_families:
        raise ValueError(
            f"the outcome's models span {spanned} distinct known famil{'y' if spanned == 1 else 'ies'} (unknown "
            f"adds none) but min_families is {min_families}: no value could be agreed"
        )
    paths = frozenset(check_must_agree(schema.spec, must_agree))
    families_map = MappingProxyType(dict(zip(model_ids, families, strict=True)))
    return _Context(
        schema=schema,
        model_ids=model_ids,
        families=families,
        families_map=families_map,
        min_families=min_families,
        must_agree=paths,
    )


def _check_units(unit_ids: object, outcome: RunOutcome, context: _Context) -> tuple[str, ...]:
    """The unit ids, checked, and checked to name every unit the outcome's responses and failed units name."""
    if isinstance(unit_ids, str) or not isinstance(unit_ids, Sequence):
        raise TypeError(f"unit_ids must be a sequence of unit ids, got {type(unit_ids).__name__}")
    units = tuple(_check_text("unit_ids entry", unit_id) for unit_id in unit_ids)
    if not units:
        raise ValueError("unit_ids must name at least one unit")
    known = set(units)
    if len(known) != len(units):
        raise ValueError("unit_ids must not repeat a unit id")
    models = set(context.model_ids)
    named = [*outcome.responses, *((failure.unit_id, failure.model_id) for failure in outcome.failed_units)]
    for unit_id, model_id in named:
        if unit_id not in known:
            raise ValueError(f"the outcome answers the unit {unit_id!r}, which unit_ids does not name")
        if model_id not in models:
            raise ValueError(f"the outcome answers for the model {model_id!r}, which its models do not name")
    return units


def _unit_votes(
    outcome: RunOutcome, context: _Context, unit_id: str, quarantine: list[QuarantinedItem]
) -> list[tuple[ValidRecord, ...]]:
    """Each requested model's valid records for the unit, in model order; its quarantined items join ``quarantine``.

    A model without a response for the unit failed on it (a ``FailedUnit``) and has no record.
    """
    votes: list[tuple[ValidRecord, ...]] = []
    for model_id in context.model_ids:
        response = outcome.responses.get((unit_id, model_id))
        if response is None:
            votes.append(())
            continue
        origin = _Origin(model_id=model_id, unit_id=unit_id)
        votes.append(_validate_response_records(response, context.schema, origin, quarantine, isolate_wire=False))
    return votes


# ---------------------------------------------------------------------------------------------------------------
# Items (one unit)
# ---------------------------------------------------------------------------------------------------------------

_PerModel: TypeAlias = list[list[ValidRecord]]
"""One entity's records from each requested model, in model order: ``[]`` for a model without one."""

_Entry: TypeAlias = tuple[int, Any, object]
"""One entry at a node of a comparison: its index (a model's, or a copy's), its validated value, and that value's
JSON as the model sent it (``_ABSENT`` where the record holds none: a default)."""


class _Merged:
    """A merged value as the models sent it (module notes, 8): its JSON (``_ABSENT`` for a default), and the entry
    whose own JSON it is, unchanged (every value under it that entry's, no child of its lists left out or merged),
    else ``None``."""

    __slots__ = ("source", "wire")

    def __init__(self, wire: object, source: int | None) -> None:
        self.wire = wire
        self.source = source


def _unit_items(unit_id: str, votes: Sequence[tuple[ValidRecord, ...]], context: _Context) -> list[ConsensusItem]:
    """One unit's items, in output order (module notes, 10), from each requested model's valid records."""
    spec = context.schema.spec
    if not spec.keys.fields:
        return _unkeyed_items(unit_id, votes, context)
    groups: dict[tuple[object, ...], _PerModel] = {}
    for index, records in enumerate(votes):
        for valid in records:
            groups.setdefault(_key_of(valid.record, spec), [[] for _ in votes])[index].append(valid)
    ordered = sorted(groups.items(), key=lambda group: key_order(group[0]))
    return [_keyed_item(unit_id, key, per_model, context) for key, per_model in ordered]


@dataclass(frozen=True, slots=True)
class _UnkeyedFeatures:
    """Pre-normalized active field values, word-token sets, and posting keys for one unkeyed record."""

    norms: tuple[object, ...]
    empty: tuple[bool, ...]
    tokens: tuple[frozenset[str] | None, ...]
    keys: tuple[tuple[object, ...], ...]


def _extract_unkeyed_features(instance: BaseModel, active: Sequence[FieldSpec]) -> _UnkeyedFeatures:
    """Precompute normalized field values, word tokens, and inverted-index keys for ``instance``."""
    if not isinstance(instance, BaseModel) or not active:
        raise ValueError("_extract_unkeyed_features requires a BaseModel instance and non-empty active fields")
    norms: list[object] = []
    empty: list[bool] = []
    tokens: list[frozenset[str] | None] = []
    keys: list[tuple[object, ...]] = []
    for f_idx, field_spec in enumerate(active):
        norm = _normalized_field(field_spec, getattr(instance, field_spec.name))
        is_empty = norm is None or norm == ()
        norms.append(norm)
        empty.append(is_empty)
        if not is_empty:
            keys.append((f_idx, "eq", norm))
        if isinstance(norm, str):
            word_set = frozenset(_WORD_TOKENS.findall(norm.casefold()))
            tokens.append(word_set)
            keys.extend((f_idx, "tok", tok) for tok in word_set)
        else:
            tokens.append(None)
    if all(empty):
        keys.append(("__empty__",))
    return _UnkeyedFeatures(
        norms=tuple(norms),
        empty=tuple(empty),
        tokens=tuple(tokens),
        keys=tuple(dict.fromkeys(keys)),
    )


def _features_similarity(left: _UnkeyedFeatures, right: _UnkeyedFeatures) -> float:
    """Mean similarity across populated active fields from precomputed :class:`_UnkeyedFeatures`."""
    if not isinstance(left, _UnkeyedFeatures) or not isinstance(right, _UnkeyedFeatures):
        raise TypeError("left and right must be _UnkeyedFeatures")
    if len(left.norms) != len(right.norms):
        raise ValueError("feature vectors must share the same field count")
    total = 0.0
    count = 0
    for f_idx in range(len(left.norms)):
        if left.empty[f_idx] and right.empty[f_idx]:
            continue
        count += 1
        norm_l, norm_r = left.norms[f_idx], right.norms[f_idx]
        if norm_l == norm_r:
            total += 1.0
            continue
        toks_l, toks_r = left.tokens[f_idx], right.tokens[f_idx]
        if toks_l and toks_r:
            total += len(toks_l & toks_r) / len(toks_l | toks_r)
    return (total / count) if count else 1.0


def _match_model_unkeyed(
    feats0: Sequence[_UnkeyedFeatures],
    featsm: Sequence[_UnkeyedFeatures],
) -> dict[int, int]:
    """Greedily pair ``feats0`` to ``featsm`` via an inverted index over positive-similarity features."""
    if not isinstance(feats0, Sequence) or not isinstance(featsm, Sequence):
        raise TypeError("feats0 and featsm must be Sequences of _UnkeyedFeatures")
    postings: dict[tuple[object, ...], list[int]] = {}
    for posm, recm in enumerate(featsm):
        for key in recm.keys:
            postings.setdefault(key, []).append(posm)
    candidates: list[tuple[float, int, int]] = []
    for pos0, rec0 in enumerate(feats0):
        seen_m = {posm for key in rec0.keys for posm in postings.get(key, ())}
        for posm in seen_m:
            score = _features_similarity(rec0, featsm[posm])
            if score > 0.5:
                candidates.append((score, pos0, posm))
    candidates.sort(key=lambda entry: (-entry[0], entry[1], entry[2]))
    chosen: dict[int, int] = {}
    used_m: set[int] = set()
    for _, pos0, posm in candidates:
        if pos0 not in chosen and posm not in used_m:
            chosen[pos0] = posm
            used_m.add(posm)
    return chosen


def _pair_unkeyed_multi(
    unit_id: str, votes: Sequence[tuple[ValidRecord, ...]], context: _Context
) -> list[ConsensusItem]:
    """Greedily pair multi-record unkeyed responses across all models when similarity strictly exceeds 0.5."""
    if not unit_id or len(votes) < 2:
        raise ValueError("_pair_unkeyed_multi requires non-empty unit_id and at least two models")
    spec = context.schema.spec
    active = [f for f in spec.fields if not f.informational] or list(spec.fields)
    model_count = len(votes)
    feats_by_model = [[_extract_unkeyed_features(rec.record, active) for rec in records] for records in votes]
    chosen_by_model: list[dict[int, int]] = [{}]
    for m_idx in range(1, model_count):
        chosen_by_model.append(_match_model_unkeyed(feats_by_model[0], feats_by_model[m_idx]))

    items: list[ConsensusItem] = []
    consumed: list[set[int]] = [set() for _ in range(model_count)]
    for pos0, rec0 in enumerate(votes[0]):
        if all(pos0 in chosen_by_model[m_idx] for m_idx in range(1, model_count)):
            group: _PerModel = [[rec0]]
            consumed[0].add(pos0)
            for m_idx in range(1, model_count):
                posm = chosen_by_model[m_idx][pos0]
                consumed[m_idx].add(posm)
                group.append([votes[m_idx][posm]])
            items.append(_compared_item(unit_id, (), group, context))
    for m_idx, records in enumerate(votes):
        for pos, valid in enumerate(records):
            if pos not in consumed[m_idx]:
                single: _PerModel = [[] for _ in votes]
                single[m_idx].append(valid)
                items.append(_item(unit_id, "unmatched", (), None, single, context))
    return items


def _unkeyed_items(unit_id: str, votes: Sequence[tuple[ValidRecord, ...]], context: _Context) -> list[ConsensusItem]:
    """The items of a root without keys: paired by position (1 each) or similarity (> 0.5), else unmatched."""
    if len(votes) == 1:
        return [_compared_item(unit_id, (), [[valid]], context) for valid in votes[0]]
    if all(len(records) == 1 for records in votes):
        return [_compared_item(unit_id, (), [list(records) for records in votes], context)]
    if all(votes):
        return _pair_unkeyed_multi(unit_id, votes, context)
    items: list[ConsensusItem] = []
    for index, records in enumerate(votes):
        for valid in records:
            per_model: _PerModel = [[] for _ in votes]
            per_model[index].append(valid)
            items.append(_item(unit_id, "unmatched", (), None, per_model, context))
    return items


def _keyed_item(unit_id: str, key: tuple[object, ...], per_model: _PerModel, context: _Context) -> ConsensusItem:
    """The item of one key of a unit: one model's differing copies first (module notes, 9), then agreement."""
    disputed: list[str] = []
    notes: list[str] = []
    for model_id, copies in zip(context.model_ids, per_model, strict=True):
        if len(copies) < 2:
            continue
        comparison = _copies_compared(copies, context.schema.spec)
        disputed.extend(comparison.disputed)
        if comparison.split:
            notes.append(f"{model_id} returned this key {len(copies)} times, with differing values")
        if comparison.repeated:
            notes.append(_repeated_note(model_id))
    if notes:
        return _item(unit_id, "field_disagreement", key, None, per_model, context, disputed=disputed, notes=notes)
    return _compared_item(unit_id, key, per_model, context)


def _copies_compared(copies: Sequence[ValidRecord], spec: ModelSpec) -> "_Comparison":
    """One model's copies of one key compared with each other (module notes, 9), each copy a voter."""
    comparison = _Comparison(voters=len(copies), quorum=None)
    comparison.compare_model(spec, "", "", [(index, valid.record, valid.wire) for index, valid in enumerate(copies)], 1)
    return comparison


def _compared_item(unit_id: str, key: tuple[object, ...], per_model: _PerModel, context: _Context) -> ConsensusItem:
    """The item of one entity the requested models described, each by one record (or identical copies): rules 4, 6-9.

    With one requested model its one record is the one voter, so its repeated child keys are found (rule 9).
    """
    voters: list[_Entry] = [
        (index, copies[0].record, copies[0].wire) for index, copies in enumerate(per_model) if copies
    ]
    everyone = len(voters) == len(per_model)
    quorum = _Quorum(len(per_model), context.families, context.min_families, context.must_agree) if everyone else None
    comparison = _Comparison(voters=len(voters), quorum=quorum)
    merged = comparison.compare_model(context.schema.spec, "", "", voters, 1)
    record: BaseModel | None = None
    notes = [_repeated_note(context.model_ids[index]) for index in sorted(comparison.repeated)]
    if not isinstance(merged, _NoWinner):
        built = _merged_record(merged, voters, context.schema)
        if isinstance(built, str):
            notes.append(f"the merged record failed validation: {built}")
        else:
            record = built
    status: ConsensusStatus = "consensus" if everyone else "missing_in_some_models"
    if len(per_model) == 1:
        status = "single_model"
    if comparison.disputed or notes:
        status = "field_disagreement"
    disputed = comparison.disputed
    return _item(unit_id, status, key, record, per_model, context, disputed=disputed, notes=notes, merged=True)


def _repeated_note(model_id: str) -> str:
    """The note of a model whose record repeats a child's key with differing values (module notes, 9)."""
    return f"{model_id} repeated a child key within a list, with differing values"


def _merged_record(merged: _Merged, voters: Sequence[_Entry], schema: PreparedSchema) -> BaseModel | str:
    """The merged record (module notes, 8), or why it is none.

    A merge that is one model's own record unchanged is that model's validated instance; any other is rebuilt.
    """
    if merged.source is not None:
        record: BaseModel = next(instance for index, instance, _ in voters if index == merged.source)
        return record
    rebuilt = _validated(merged.wire, schema)
    if isinstance(rebuilt, str):
        return rebuilt
    if not _holds_agreed(rebuilt, voters, schema.spec):
        return "rebuilt from what the models sent, it does not hold the values they agreed on"
    return rebuilt


def _holds_agreed(rebuilt: BaseModel, voters: Sequence[_Entry], spec: ModelSpec) -> bool:
    """Whether ``rebuilt`` holds the values its voters agreed on, and no others (module notes, 8).

    Placed first among them and compared, it must be the source of their merge: the representative of every winning
    value, holding every child that wins and no other. A value no majority backs, or a child it lacks, gives the
    merge another source, or none. A validator that reorders or reshapes what it is given can set a value apart
    from the JSON it was found by; this check sends such a record to review.
    """
    entries = [(_REBUILT, rebuilt, _ABSENT), *((index, instance, _ABSENT) for index, instance, _ in voters)]
    check = _Comparison(voters=len(entries), quorum=None).compare_model(spec, "", "", entries, 1)
    return not isinstance(check, _NoWinner) and check.source == _REBUILT


def _item(
    unit_id: str,
    status: ConsensusStatus,
    key: tuple[object, ...],
    record: BaseModel | None,
    per_model: _PerModel,
    context: _Context,
    *,
    disputed: Sequence[str] = (),
    notes: Sequence[str] = (),
    merged: bool = False,
) -> ConsensusItem:
    """An item, with each requested model's records, quotes and element ids in model order.

    With ``merged``, each model's copies are identical: its first stands for them in ``model_votes``, and the
    grounding of all of them is united.
    """
    votes: dict[str, tuple[Mapping[str, Any], ...]] = {}
    quotes: dict[str, tuple[str, ...]] = {}
    element_ids: dict[str, tuple[str, ...]] = {}
    for model_id, copies in zip(context.model_ids, per_model, strict=True):
        shown = copies[:1] if merged else copies
        votes[model_id] = tuple(_dumped(valid.record) for valid in shown)
        quotes[model_id] = _united(valid.grounding_quotes for valid in copies)
        element_ids[model_id] = _united(valid.source_element_ids for valid in copies)
    return ConsensusItem._trusted(
        unit_id=unit_id,
        status=status,
        key=key,
        record=record,
        disputed_fields=tuple(dict.fromkeys(disputed)),
        notes=tuple(notes),
        model_votes=MappingProxyType(votes),
        quotes_by_model=MappingProxyType(quotes),
        element_ids_by_model=MappingProxyType(element_ids),
        families=context.families_map,
    )


def _dumped(record: BaseModel) -> dict[str, Any]:
    """A validated record as a JSON-mode dict keyed by Python attribute names.

    Pydantic's serializer warns about an ``int`` in a ``Decimal`` field (and writes it as a number); the default
    check admitted every value a record holds, widened ints included (module notes, 3), so the warning carries
    nothing and is left out. ``by_alias=False`` prevents ``serialize_by_alias=True`` from rewriting attribute keys.
    """
    return record.model_dump(mode="json", by_alias=False, warnings=False)


def _united(groups: Iterable[tuple[str, ...]]) -> tuple[str, ...]:
    """The strings of ``groups``, each once, in first-seen order."""
    return tuple(dict.fromkeys(text for group in groups for text in group))


# ---------------------------------------------------------------------------------------------------------------
# Field agreement
# ---------------------------------------------------------------------------------------------------------------


class _NoWinner:
    """The value of a field no cluster won (a tie, or a child kept by no majority): never a pick."""

    __slots__ = ()


_NO_WINNER = _NoWinner()


@dataclass(frozen=True, slots=True)
class _Quorum:
    """What a winning value needs to be agreed when every requested model returned the record (module notes, 6).

    It is used only then (``k == N``), so a winning value is already backed by more than half of the requested
    models; what it adds is the cross-family rule and ``must_agree``'s unanimity.
    """

    requested: int
    families: tuple[str, ...]
    min_families: int
    must_agree: frozenset[str]
    all_families_ok: bool = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "all_families_ok", _family_count(range(self.requested), self.families) >= self.min_families
        )

    def agrees(self, supporters: Sequence[int], spec_path: str) -> bool:
        """Whether a winning value's ``supporters`` (model indices) span enough families, and all models if it must."""
        if len(supporters) == self.requested:
            return self.all_families_ok
        if _family_count(supporters, self.families) < self.min_families:
            return False
        return not self._must(spec_path)

    def _must(self, spec_path: str) -> bool:
        """Whether ``spec_path`` is a ``must_agree`` path or lies under one."""
        if not self.must_agree:
            return False
        parts = spec_path.split(".")
        return any(".".join(parts[:end]) in self.must_agree for end in range(1, len(parts) + 1))


def _family_count(supporters: Sequence[int], families: Sequence[str]) -> int:
    """The distinct known families among ``supporters``; backers of unknown family alone count as one."""
    known = {families[index] for index in supporters if families[index] != UNKNOWN_FAMILY}
    return len(known) if known else min(1, len(supporters))


@dataclass(slots=True)
class _Cluster:
    """Values matching one representative (the first of them, in model order): its JSON, and the entries backing it."""

    normalized: object
    wire: object
    source: int
    supporters: list[int]


class _Comparison:
    """One item's field-by-field comparison: the merged values, and the paths not agreed (module notes, 6-9).

    ``voters`` is the number of entries at the root (``k``): a value wins with more than half of them. With a
    ``quorum`` (every requested model returned the record), a winning value is agreed when it meets the quorum;
    without one (the record missing in some models, or one model's copies), when every entry at its node backs it.
    Values are compared validated; the merged value is the JSON its representative sent (module notes, 8).
    ``repeated`` collects the entries that repeat a child's key with differing values, and ``split`` says whether
    the entries differ anywhere (a repeated child's versions aside).
    """

    __slots__ = ("disputed", "quorum", "repeated", "split", "voters")

    def __init__(self, voters: int, quorum: _Quorum | None) -> None:
        self.voters = voters
        self.quorum = quorum
        self.disputed: list[str] = []
        self.repeated: set[int] = set()
        self.split = False

    def agreed(self, supporters: Sequence[int], at_node: int, spec_path: str) -> bool:
        """Whether a winning value backed by ``supporters`` (of the ``at_node`` entries there) is agreed."""
        if self.quorum is None:
            return len(supporters) == at_node
        return self.quorum.agrees(supporters, spec_path)

    def wins(self, supporters: Sequence[int]) -> bool:
        """Whether ``supporters`` are more than half of the root's entries."""
        return len(supporters) * 2 > self.voters

    def compare_model(
        self, spec: ModelSpec, path: str, spec_path: str, entries: list[_Entry], depth: int
    ) -> _Merged | _NoWinner:
        """Compare instances of one model field by field: the merged object (JSON, by wire name), or no winner."""
        if depth > MAX_DEPTH:
            raise RationalizationError(f"{path or 'the record'}: nesting is deeper than {MAX_DEPTH} levels")
        if len(entries) == 1 and self.voters == 1 and self.quorum is None and not spec.has_keyed_list:
            return _Merged(entries[0][2], entries[0][0])
        by_name = spec.reads_by_name
        has_must = self.quorum is not None and bool(self.quorum.must_agree)
        parts: list[tuple[FieldSpec, _Merged]] = []
        complete = True
        for field_spec in spec.fields:
            field_path = _join_path(path, field_spec.name)
            field_spec_path = _join_path(spec_path, field_spec.name) if has_must else ""
            values = [
                (index, getattr(instance, field_spec.name), _wire_value(wire, field_spec, by_name))
                for index, instance, wire in entries
            ]
            result = self.compare_field(field_spec, field_path, field_spec_path, values, depth)
            if isinstance(result, _NoWinner):
                complete = False
            else:
                parts.append((field_spec, result))
        return _assembled(parts, entries) if complete else _NO_WINNER

    def compare_field(
        self, field_spec: FieldSpec, path: str, spec_path: str, values: list[_Entry], depth: int
    ) -> _Merged | _NoWinner:
        """Compare one field's values: its merged value, or no winner."""
        child = field_spec.child
        if field_spec.kind == "model" and child is not None:
            return self.compare_child(child, path, spec_path, values, depth)
        if field_spec.kind == "model_list" and child is not None and child.keys.fields:
            return self.compare_keyed_list(child, path, spec_path, values, depth)
        return self.compare_value(field_spec, path, spec_path, values)

    def _compare_submodel(
        self,
        child: ModelSpec,
        parent_path: str,
        key_instance: BaseModel | None,
        spec_path: str,
        entries: list[_Entry],
        next_depth: int,
        cached_path: str | None = None,
    ) -> _Merged | _NoWinner:
        """Compare ``child`` with lazy dispute-path formatting and prefixing."""
        if not entries:
            raise ValueError("_compare_submodel requires non-empty entries")
        if next_depth > MAX_DEPTH:
            resolved = _submodel_path(child, parent_path, key_instance, cached_path)
            raise RationalizationError(f"{resolved or 'the record'}: nesting is deeper than {MAX_DEPTH} levels")
        mark = len(self.disputed)
        try:
            result = self.compare_model(child, "", spec_path, entries, next_depth)
        except RationalizationError as exc:
            resolved = _submodel_path(child, parent_path, key_instance, cached_path)
            raise RationalizationError(_prefixed_depth_error(resolved, str(exc))) from exc
        if len(self.disputed) > mark:
            resolved = _submodel_path(child, parent_path, key_instance, cached_path)
            _prefix_disputed(self.disputed, mark, resolved)
        return result

    def compare_child(
        self, child: ModelSpec, path: str, spec_path: str, values: list[_Entry], depth: int
    ) -> _Merged | _NoWinner:
        """Compare a nested model: when some hold ``None`` (it is Optional), first as present or ``None``."""
        present = [(index, value, wire) for index, value, wire in values if value is not None]
        if len(present) < len(values):
            shown = [index for index, _, _ in present]
            absent = [index for index, value, _ in values if value is None]
            self.split = self.split or bool(shown)
            winner = shown if self.wins(shown) else absent if self.wins(absent) else None
            if winner is None or not self.agreed(winner, len(values), spec_path):
                self.disputed.append(path)
            if winner is None:
                return _NO_WINNER
            if winner is absent:
                return _Merged(_wire_of(values, absent[0]), absent[0])
        return self._compare_submodel(child, path, None, spec_path, present, depth + 1)

    def compare_keyed_list(
        self, child: ModelSpec, path: str, spec_path: str, values: list[_Entry], depth: int
    ) -> _Merged | _NoWinner:
        """Match a keyed list's children by key across the entries (module notes, 7): the merged list, or none.

        Children are agreed field by field. Lists empty in every entry have no child to carry the quorum, so their
        emptiness is a value of its own: agreed only as any other value is (enough families, under a quorum).
        """
        members, conflicts = _children_by_key(child, values)
        if not members:
            if not self.agreed([index for index, _, _ in values], len(values), spec_path):
                self.disputed.append(path)
            return _Merged(values[0][2], values[0][0])
        merged: list[_Merged] = []
        complete = True
        for key, found in members.items():
            clashing = conflicts.get(key, ())
            self.repeated.update(clashing)
            self.split = self.split or len(found) < len(values) or 0 < len(clashing) < len(found)
            child_path: str | None = None
            if clashing or len(found) < len(values):
                child_path = f"{path}[{_key_text(found[0][1], child)}]"
                self.disputed.append(child_path)
            if clashing or len(found) * 2 <= self.voters:
                left_out = (len(values) - len(found)) * 2 > self.voters
                complete = complete and not clashing and left_out
                continue
            result = self._compare_submodel(child, path, found[0][1], spec_path, found, depth + 1, child_path)
            if isinstance(result, _NoWinner):
                complete = False
            else:
                merged.append(result)
        return _listed(merged, values) if complete else _NO_WINNER

    def _compare_informational(self, field_spec: FieldSpec, values: list[_Entry], *, prefer_wire: bool) -> _Merged:
        """Select a representative value for a non-disputing ``Informational`` field without recording disputes."""
        if _same_raw_values(field_spec, values):
            wire, source = _single_wire(values, prefer_wire=prefer_wire)
            return _Merged(wire, source)
        clusters = _clusters(
            [(index, wire, _normalized_field(field_spec, value)) for index, value, wire in values],
            prefer_wire=prefer_wire,
        )
        winner = next((cluster for cluster in clusters if self.wins(cluster.supporters)), None)
        if winner is None:
            winner = next((c for c in clusters if c.normalized is not None and c.normalized != ()), clusters[0])
        return _Merged(winner.wire, winner.source)

    def _same_raw_merged(
        self, path: str, spec_path: str, values: list[_Entry], *, prefer_wire: bool
    ) -> _Merged | _NoWinner:
        """Merge ``values`` when all entries already hold the same raw scalar or scalar list."""
        if not values or self.voters < 1:
            raise ValueError("_same_raw_merged requires non-empty values and voters >= 1")
        if len(values) == self.voters:
            if self.quorum is not None and not self.quorum.all_families_ok:
                self.disputed.append(path)
            wire, source = _single_wire(values, prefer_wire=prefer_wire)
            return _Merged(wire, source)
        supporters = [index for index, _, _ in values]
        wins = self.wins(supporters)
        if not wins or not self.agreed(supporters, len(values), spec_path):
            self.disputed.append(path)
        if not wins:
            return _NO_WINNER
        wire, source = _single_wire(values, prefer_wire=prefer_wire)
        return _Merged(wire, source)

    def compare_value(
        self, field_spec: FieldSpec, path: str, spec_path: str, values: list[_Entry]
    ) -> _Merged | _NoWinner:
        """Compare a scalar's values, or a list's as normalized sequences: the winner, or no winner."""
        prefer_wire = not field_spec.required
        quorum = self.quorum
        if field_spec.informational and not (quorum is not None and quorum._must(spec_path)):
            return self._compare_informational(field_spec, values, prefer_wire=prefer_wire)
        if _same_raw_values(field_spec, values):
            return self._same_raw_merged(path, spec_path, values, prefer_wire=prefer_wire)
        use_quorum_norm = field_spec.child is not None and quorum is not None and bool(quorum.must_agree)
        triples = [
            (
                index,
                wire,
                _normalized_field_quorum(field_spec, value, spec_path, quorum)
                if use_quorum_norm
                else _normalized_field(field_spec, value),
            )
            for index, value, wire in values
        ]
        clusters = _clusters(triples, prefer_wire=prefer_wire)
        self.split = self.split or len(clusters) > 1
        winner = next((cluster for cluster in clusters if self.wins(cluster.supporters)), None)
        disagreed = (
            winner is None
            or not self.agreed(winner.supporters, len(values), spec_path)
            or (
                quorum is not None
                and len(winner.supporters) < quorum.requested
                and _unkeyed_list_must_disagrees(field_spec, spec_path, values, quorum)
            )
        )
        if disagreed:
            self.disputed.extend(_value_paths(field_spec, path, clusters))
        return _NO_WINNER if winner is None else _Merged(winner.wire, winner.source)


def _join_path(prefix: str, name: str) -> str:
    """Return ``name`` when ``prefix`` is empty, else ``f'{prefix}.{name}'``."""
    if not isinstance(prefix, str) or not isinstance(name, str) or not name:
        raise ValueError("_join_path requires str prefix and non-empty str name")
    return f"{prefix}.{name}" if prefix else name


def _submodel_path(
    child: ModelSpec,
    parent_path: str,
    key_instance: BaseModel | None,
    cached_path: str | None,
) -> str:
    """Format the attribute/key path of a nested model or keyed-list child on demand."""
    if not isinstance(child, ModelSpec) or not isinstance(parent_path, str):
        raise TypeError("_submodel_path requires ModelSpec and str parent_path")
    if cached_path is not None:
        return cached_path
    return parent_path if key_instance is None else f"{parent_path}[{_key_text(key_instance, child)}]"


def _prefixed_depth_error(prefix: str, message: str) -> str:
    """Prepend ancestor ``prefix`` to a nested ``MAX_DEPTH`` :class:`RationalizationError` message."""
    if not isinstance(prefix, str) or not isinstance(message, str) or not message:
        raise ValueError("_prefixed_depth_error requires str prefix and non-empty message")
    if not prefix:
        return message
    if message.startswith("the record:"):
        return f"{prefix}{message[len('the record') :]}"
    return f"{prefix}.{message}"


def _prefix_disputed(disputed: list[str], start: int, prefix: str) -> None:
    """Prepend ``prefix`` to every disputed field path recorded since ``start``."""
    if start < 0 or start > len(disputed):
        raise ValueError(f"start index {start} out of bounds for disputed length {len(disputed)}")
    if not isinstance(prefix, str) or not prefix:
        raise ValueError("prefix must be a non-empty str")
    for idx in range(start, len(disputed)):
        disputed[idx] = f"{prefix}.{disputed[idx]}"


def _assembled(parts: Sequence[tuple[FieldSpec, _Merged]], entries: Sequence[_Entry]) -> _Merged:
    """A model's merged object: one entry's own JSON when every field is that entry's own, else its fields' JSON by
    wire name (a default left out, so it applies again)."""
    first_source = parts[0][1].source if parts else None
    if first_source is not None and all(merged.source == first_source for _, merged in parts[1:]):
        return _Merged(_wire_of(entries, first_source), first_source)
    wire = {field_spec.wire_name: merged.wire for field_spec, merged in parts if merged.wire is not _ABSENT}
    return _Merged(wire, None)


def _listed(children: Sequence[_Merged], values: Sequence[_Entry]) -> _Merged:
    """A keyed list's merged children: one entry's own list when they are all of it, each its own, else their JSON."""
    first_source = children[0].source if children else None
    if first_source is not None and all(merged.source == first_source for merged in children[1:]):
        _, own, wire = next(entry for entry in values if entry[0] == first_source)
        if len(own) == len(children):
            return _Merged(wire, first_source)
    return _Merged([merged.wire for merged in children if merged.wire is not _ABSENT], None)


def _wire_value(node: object, field_spec: FieldSpec, by_name: bool) -> object:
    """A field's JSON in a record object as its model sent it: under its wire name, else (``by_name``) its attribute
    name; ``_ABSENT`` when the object holds neither (the field took its default) or is no object."""
    if type(node) is dict:
        found = node.get(field_spec.wire_name, _ABSENT)
        if found is not _ABSENT or not by_name:
            return found
        return node.get(field_spec.name, _ABSENT)
    if not isinstance(node, dict):
        return _ABSENT
    if field_spec.wire_name in node:
        return node[field_spec.wire_name]
    if by_name and field_spec.name in node:
        return node[field_spec.name]
    return _ABSENT


def _wire_of(entries: Sequence[_Entry], index: int) -> object:
    """The JSON of the entry ``index`` among ``entries``."""
    for at, _, wire in entries:
        if at == index:
            return wire
    raise LookupError(f"entry index {index} not found")


def _children_by_key(
    child: ModelSpec, values: list[_Entry]
) -> tuple[dict[tuple[object, ...], list[_Entry]], dict[tuple[object, ...], set[int]]]:
    """Each child key's first instance per entry with its JSON, in first-seen order; and per key, the entries
    repeating it with differing values.

    A child's JSON is the item at its position in the list the model sent, when that list has one item per child.
    """
    members: dict[tuple[object, ...], list[_Entry]] = {}
    conflicts: dict[tuple[object, ...], set[int]] = {}
    for index, children, wire in values:
        items = wire if isinstance(wire, list) and len(wire) == len(children) else None
        own: dict[tuple[object, ...], BaseModel] = {}
        for position, instance in enumerate(children):
            key = _key_of(instance, child)
            if key not in own:
                own[key] = instance
                members.setdefault(key, []).append((index, instance, _ABSENT if items is None else items[position]))
                continue
            if _differ(own[key], instance, child):
                conflicts.setdefault(key, set()).add(index)
    return members, conflicts


def _differ(first: BaseModel, second: BaseModel, spec: ModelSpec) -> bool:
    """Whether two instances of ``spec`` differ anywhere (compared as one model's copies)."""
    comparison = _Comparison(voters=2, quorum=None)
    comparison.compare_model(spec, "", "", [(0, first, _ABSENT), (1, second, _ABSENT)], 1)
    return bool(comparison.disputed)


def _same_scalar(first: object, second: object) -> bool:
    """Whether two validated scalars have the exact same type and value (and timezone, for datetime)."""
    return (
        type(first) is type(second)
        and first == second
        and (not isinstance(first, datetime) or first.tzinfo == cast(datetime, second).tzinfo)
    )


def _same_raw_values(field_spec: FieldSpec, values: list[_Entry]) -> bool:
    """Whether every entry in ``values`` already holds the same raw scalar or scalar list value."""
    if field_spec.child is not None:
        return False
    n_vals = len(values)
    if n_vals <= 1:
        return True
    if n_vals == 2 and field_spec.kind == "scalar":
        return _same_scalar(values[0][1], values[1][1])
    first = values[0][1]
    if field_spec.kind == "scalar":
        return all(_same_scalar(first, value) for _, value, _ in values[1:])
    if field_spec.kind == "scalar_list":
        return all(
            len(value) == len(first) and all(_same_scalar(a, b) for a, b in zip(first, value))
            for _, value, _ in values[1:]
        )
    return False


def _single_wire(values: list[_Entry], *, prefer_wire: bool) -> tuple[object, int]:
    """The wire value and source index when every entry in ``values`` holds the same value."""
    first_index, _, first_wire = values[0]
    wire, source = first_wire, first_index
    if prefer_wire and wire is _ABSENT:
        for index, _, entry_wire in values[1:]:
            if entry_wire is not _ABSENT:
                return entry_wire, index
    return wire, source


def _clusters(values: Iterable[tuple[int, object, object]], *, prefer_wire: bool = True) -> list[_Cluster]:
    """Group ``(index, wire, normalized)`` triples by their normalized value, clusters in first-seen order.

    A normalized value is a hashable canonical form (:func:`_normalized_field`) and the values of one field share
    one exact type (validation and the widening of ints see to it), so grouping is a dictionary lookup, linear in
    the values; the first value of a cluster is its representative, and the cluster keeps its JSON (or, when
    ``prefer_wire`` is true, the first supporter's explicit wire value when earlier supporters left the field to its
    default).
    """
    clusters: dict[object, _Cluster] = {}
    for index, wire, normalized in values:
        cluster = clusters.get(normalized)
        if cluster is None:
            clusters[normalized] = _Cluster(normalized=normalized, wire=wire, source=index, supporters=[index])
        else:
            if prefer_wire and cluster.wire is _ABSENT and wire is not _ABSENT:
                cluster.wire = wire
                cluster.source = index
            cluster.supporters.append(index)
    return list(clusters.values())


def _value_paths(field_spec: FieldSpec, path: str, clusters: Sequence[_Cluster]) -> list[str]:
    """The paths a disputed value names: a list's differing positions when all its versions have one length."""
    if field_spec.kind not in _LIST_KINDS or len(clusters) < 2:
        return [path]
    versions = [cast(tuple[object, ...], cluster.normalized) for cluster in clusters]  # a list's are tuples
    if len({len(version) for version in versions}) != 1:
        return [path]
    first, *others = versions
    differing = [
        f"{path}[{position}]"
        for position, item in enumerate(first)
        if not all(item == other[position] for other in others)
    ]
    return differing or [path]


# ---------------------------------------------------------------------------------------------------------------
# Normalization and order
# ---------------------------------------------------------------------------------------------------------------


def _normalized(value: object) -> object:
    """A scalar as consensus compares it (module notes, 5): equal for values that describe the same thing."""
    if value is None or type(value) is bool:
        return value
    if isinstance(value, Enum):
        return value.value
    if type(value) is float:
        return float(format(value, _FLOAT_FMT))
    if type(value) is str:
        return normalized_text(value)
    if type(value) is datetime and value.utcoffset() is not None:
        return value.astimezone(timezone.utc)
    return value


def _key_of(record: BaseModel, spec: ModelSpec) -> tuple[object, ...]:
    """The normalized key tuple of ``record``, an instance of ``spec``'s model (:func:`normalized_key`)."""
    fields = spec.keys.fields
    norms = spec.key_normalizers
    if len(fields) == 1:
        raw = getattr(record, fields[0])
        if norms[0] == "alnum_upper" and type(raw) is str:
            return (alnum_upper_text(raw),)
        return (_normalized(raw),)
    if not any(norms):
        return tuple(_normalized(getattr(record, name)) for name in fields)
    return tuple(
        alnum_upper_text(raw)
        if norm == "alnum_upper" and type(raw := getattr(record, name)) is str
        else _normalized(getattr(record, name))
        for name, norm in zip(fields, norms, strict=True)
    )


def _normalized_field(field_spec: FieldSpec, value: Any) -> object:
    """A field's value as it is compared: a scalar normalized, a model or list as normalized tuple(s).

    An ``int`` a ``float`` field holds (a widened default, module notes, 3) is compared as a float, rounded like
    one, so ``0`` and ``0.0`` match; an ``int`` in a ``Decimal`` field compares by value as it is.
    """
    if value is None:
        return None
    scalar = field_spec.scalar
    if field_spec.kind == "scalar" and scalar is not None:
        if field_spec.key_normalizer == "alnum_upper" and type(value) is str:
            return alnum_upper_text(value)
        return _normalized(_widened(value, scalar.base))
    if field_spec.kind == "scalar_list" and scalar is not None:
        return tuple(_normalized(_widened(item, scalar.base)) for item in value)
    child = field_spec.child
    if child is None:
        return value
    if field_spec.kind == "model":
        return _normalized_model(value, child, 1)
    return tuple(_normalized_model(item, child, 1) for item in value)


def _normalized_field_quorum(
    field_spec: FieldSpec,
    value: Any,
    spec_path: str,
    quorum: _Quorum | None,
) -> object:
    """Normalize ``value`` for ``field_spec`` while honoring ``quorum`` ``must_agree`` on ``Informational`` fields."""
    if not isinstance(field_spec, FieldSpec) or not isinstance(spec_path, str):
        raise TypeError("_normalized_field_quorum requires FieldSpec and str spec_path")
    if value is None or field_spec.child is None:
        return _normalized_field(field_spec, value)
    child = field_spec.child
    if field_spec.kind == "model":
        return _normalized_model(value, child, 1, spec_path, quorum)
    return tuple(_normalized_model(item, child, 1, spec_path, quorum) for item in value)


def _widened(value: object, base: type) -> object:
    """``value`` as a ``float`` when it is an ``int`` (never a ``bool``) of a ``float`` field, so it rounds as one.

    An ``int`` in a ``Decimal`` field needs nothing: an int and an equal Decimal compare and hash alike.
    """
    if type(value) is int and base is float:
        return float(value)
    return value


def _normalized_model(
    instance: BaseModel | None,
    spec: ModelSpec,
    depth: int,
    spec_path: str = "",
    quorum: _Quorum | None = None,
) -> object:
    """An instance as the tuple of its fields' normalized values, its nested models and lists in order."""
    if instance is None:
        return None
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    has_must = quorum is not None and bool(quorum.must_agree)
    parts: list[object] = []
    for field_spec in spec.fields:
        f_path = _join_path(spec_path, field_spec.name) if has_must else ""
        if field_spec.informational and not (has_must and quorum is not None and quorum._must(f_path)):
            continue
        value = getattr(instance, field_spec.name)
        child = field_spec.child
        if child is None:
            parts.append(_normalized_field(field_spec, value))
        elif field_spec.kind == "model":
            parts.append(_normalized_model(value, child, depth + 1, f_path, quorum))
        else:
            parts.append(tuple(_normalized_model(item, child, depth + 1, f_path, quorum) for item in value))
    return tuple(parts)


def _models_differ_on_must(
    instances: Sequence[BaseModel],
    spec: ModelSpec,
    spec_path: str,
    quorum: _Quorum,
    depth: int = 1,
) -> bool:
    """Whether ``instances`` disagree on any ``must_agree`` field at or below ``spec_path``."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    if not isinstance(spec_path, str):
        raise TypeError("spec_path must be a str")
    if len(instances) <= 1:
        return False
    for cf in spec.fields:
        cf_path = _join_path(spec_path, cf.name)
        if quorum._must(cf_path):
            norms = {_normalized_field_quorum(cf, getattr(inst, cf.name), cf_path, quorum) for inst in instances}
            if len(norms) > 1:
                return True
            continue
        has_descendant = cf.child is not None and any(p.startswith(f"{cf_path}.") for p in quorum.must_agree)
        if has_descendant and cf.child is not None:
            if _child_must_differs(instances, cf, cf.child, cf_path, quorum, depth + 1):
                return True
    return False


def _child_must_differs(
    instances: Sequence[BaseModel],
    field_spec: FieldSpec,
    child: ModelSpec,
    spec_path: str,
    quorum: _Quorum,
    depth: int,
) -> bool:
    """Whether nested ``model`` or ``model_list`` field ``field_spec`` differs on a descendant ``must_agree`` path."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{child.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    if not spec_path:
        raise ValueError("spec_path must be non-empty")
    if field_spec.kind == "model":
        present = [sub for inst in instances if (sub := getattr(inst, field_spec.name)) is not None]
        if 0 < len(present) < len(instances):
            return True
        return _models_differ_on_must(present, child, spec_path, quorum, depth)
    lists = [getattr(inst, field_spec.name) for inst in instances]
    if len({len(items) for items in lists}) > 1:
        return True
    if not lists or not lists[0]:
        return False
    return any(
        _models_differ_on_must([items[idx] for items in lists], child, spec_path, quorum, depth)
        for idx in range(len(lists[0]))
    )


def _unkeyed_list_must_disagrees(
    field_spec: FieldSpec,
    spec_path: str,
    values: Sequence[_Entry],
    quorum: _Quorum | None,
) -> bool:
    """Whether an unkeyed ``model_list`` violates a descendant ``must_agree`` path across ``values``."""
    if not isinstance(field_spec, FieldSpec) or not isinstance(spec_path, str):
        raise TypeError("_unkeyed_list_must_disagrees requires FieldSpec and str spec_path")
    child = field_spec.child
    if child is None or field_spec.kind != "model_list" or quorum is None or not quorum.must_agree:
        return False
    prefix = f"{spec_path}."
    if not any(path.startswith(prefix) for path in quorum.must_agree):
        return False
    if len(values) < quorum.requested:
        return True
    lists = [entry[1] for entry in values]
    if len({len(items) for items in lists}) > 1:
        return True
    if not lists or not lists[0]:
        return False
    return any(
        _models_differ_on_must([items[idx] for items in lists], child, spec_path, quorum, 1)
        for idx in range(len(lists[0]))
    )


def _order_part(value: object) -> tuple[int, Any]:
    """One normalized key part's rank and comparable value: kinds that do not compare get ranks of their own."""
    if type(value) is bool:
        return (0, value)
    if type(value) in (int, float, Decimal):
        return (1, value)
    if type(value) is str:
        return (2, value)
    if type(value) is date:
        return (3, value)
    if isinstance(value, datetime):
        return (5, value) if value.utcoffset() is not None else (4, value)
    return (6, repr(value))


def _key_text(instance: BaseModel, spec: ModelSpec) -> str:
    """A child's key as its path shows it (``'PN-104'``, ``3, 'PN-1'``), from the first model's own values."""
    return ", ".join(_key_part_text(getattr(instance, name)) for name in spec.keys.fields)


def _key_part_text(value: object) -> str:
    """One key value as a path shows it: a string or date quoted, a number as written, an Enum by its value."""
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, (date, datetime)):
        return repr(value.isoformat())
    return repr(value) if isinstance(value, str) else str(value)
