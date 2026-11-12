"""The caller's schema: what rationalize() accepts, and what it derives from it (spec §4.1, plan §4.1).

A schema is a pydantic ``BaseModel`` subclass, checked at four levels before any work starts, so that no request is
paid for whose answers the schema would then reject. Each rejection raises
:class:`~datagrunt.rationalization.RationalizationError` naming what is at fault.

1. **Classes** (:func:`check_schema`). The schema, and every model its fields name, is a built ``BaseModel``
   subclass with at least one field: not a ``RootModel`` (a record is an object with named fields), not
   ``extra="allow"`` (unbounded keys), not ``use_enum_values=True`` (its records would hold raw values where its
   spec says Enum members), with no ``json_schema_mode_override`` but ``"validation"`` (the JSON schema must
   describe what ``model_validate`` accepts), and not waiting on ``defer_build=True`` (call ``model_rebuild()``).
2. **Annotations and keys** (:func:`describe`, naming the dotted field path, as ``Order.lines[].part.number``).
   It checks each model as in 1 when it reaches it, after the annotation that names it. A field is one of:

   - a scalar: ``str``, ``int``, ``float``, ``bool``, ``Decimal``, ``date``, ``pydantic.AwareDatetime``,
     ``pydantic.NaiveDatetime``, an ``Enum`` (not a ``Flag``) whose values are all ``str`` or all ``int``, or a
     ``Literal`` whose values are all ``str``, all ``int`` or all ``bool``;
   - a nested model, or ``X | None`` of a scalar or a model;
   - ``list[X]`` of a scalar or a model.

   Nothing else is: no plain ``datetime`` (one field would hold naive and aware values alike), ``dict``,
   ``tuple``, ``set``, ``bytes``, ``Any``, ``NewType`` or ``type`` alias, union of two or more types (a
   discriminated one included), list of lists, list of Optional, Optional list, or recursive model. A field whose
   type does not admit ``None`` may not default to ``None`` (``SkipJsonSchema[None]`` counts as ``None``).
   Pydantic does not validate defaults, so a record that leaves a field out holds its default unchecked; a static
   default must therefore be a value of its field (:func:`value_problems`): for a scalar, a finite one
   :func:`admits_scalar` admits (its exact type, or an ``int`` widening losslessly to a ``float`` within
   :data:`FLOAT_INT_LIMIT` or to a ``Decimal``); for a list, a ``list`` of such items; for a model, an instance of
   exactly its class whose own fields hold such values, checked in turn. A default pydantic validates
   (``validate_default=True`` on the field, or on its model unless the field sets it false) must instead pass
   that validation: the model's own, as assigning the default to the field would run it (inconclusive outcomes,
   such as a model validator meeting the half-built instance, pass).
   A ``default_factory`` is never called here, so what it gives is checked after validation, by consensus, which
   quarantines a record holding a value its field does not admit (``None`` in a non-Optional field above all).
   A field's value travels under one plain name, its
   wire name (an ``AliasPath`` is refused), and no two fields of a model share one. A path holds at most 32
   models and lists: the root is level 1, a nested model adds one, ``list[Child]`` adds two (the list and the
   model in it). The expanded field tree, where a model reached by two paths counts twice, holds at most 10,000
   fields. Each scalar or scalar-list field's :class:`ScalarSpec` records the exact type of its values, its Enum
   or Literal, and whether a datetime is aware or naive, and each :class:`FieldSpec` whether ``model_validate``
   requires the field (``required``, from ``FieldInfo.is_required()``), so no later step reads an annotation again.

   A field is a key when ``Key`` marks it (``Annotated[str, Key]``, ``Key("part")``). The root may instead name
   its keys with ``key=`` (attribute names, not aliases); when both are given, they must name the same fields. A
   key field is required and not Optional, and its type is ``str``, ``int``, ``Decimal``, ``date``, an aware or
   naive datetime, an ``Enum``, or a ``Literal`` of ``str`` or ``int`` values. The keys of one model share one
   namespace, or none. A model without keys gets ``KeySpec((), None)``: whether that is allowed is decided by
   consensus, not here.
3. **Agreement** (part of :func:`prepare`, naming the field path). The JSON schema a model would answer must be
   what ``model_validate`` accepts: for each model of the spec, its JSON object (through ``$ref`` and Optional's
   ``anyOf``) has exactly its fields' wire names as properties, requires exactly the fields ``model_validate``
   requires, allows ``null`` only for Optional fields, and gives each field its kind (a model an object, a list an
   array, a scalar neither). Each scalar, and each scalar list's ``items``, has the JSON pydantic emits for its
   type: ``string``, ``integer``, ``number`` or ``boolean``; a ``date`` a string of format ``date``, a datetime
   one of format ``date-time``; a ``Decimal`` the anyOf of a ``number`` and a ``string``; an Enum an ``enum``
   equal to its values in order, a Literal an ``enum`` (or, for one value, a ``const``) equal to its values, each
   with its type. Constraints may narrow it. No keyword that shapes a value may sit beside a ``$ref`` or
   Optional's ``anyOf``. A default must be JSON (no ``nan`` or ``inf``).
4. **JSON schema** (:func:`check_json_schema`, naming the JSON pointer). Pydantic's validation schema must stay
   in the subset every provider adapter expresses: objects with properties, arrays with one ``items`` schema,
   scalars, ``enum`` and ``const``, ``anyOf`` only as ``[X, {"type": "null"}]`` (Optional) or as pydantic's
   ``Decimal`` (a ``number`` then a ``string``, then ``null`` when Optional), and local ``#/$defs/<name>``
   references without cycles, each entry referenced (the name one RFC 6901 token, read by
   :func:`~datagrunt.rationalization._refs.def_name` as every adapter reads it). Keywords that combine or
   condition schemas, or hold subschemas the walk does not read, are refused (``oneOf``, ``allOf``, ``not``,
   ``if`` / ``then`` / ``else``, ``patternProperties``, ``prefixItems``, ``contains``, ``$id``, ...), as are
   ``additionalProperties`` other than ``false``, an unconstrained schema, an object without properties and an
   array without ``items``.
   Constraints (``pattern``, ``maximum``, ``format``, ...) and annotations (``title``, ``description``,
   ``default``, ...) pass: adapters handle them. Pydantic 2.14 emits a model field with a description as ``$ref``
   with a sibling ``description``, never ``allOf``. A path holds at most 32 objects and arrays, counted as
   :func:`describe` counts models and lists, and a walk visits at most 100,000 nodes.

:func:`response_schema` builds the response wrapper (plan §0 correction 2): ``{"records": [{"record": <schema>,
"grounding_quotes": [str], "source_element_ids": [str]}]}``, with the schema's ``$defs`` hoisted to its root so
every ``$ref`` still resolves. :func:`schema_sha256` hashes its canonical JSON. :func:`prepare` runs every step,
in order, and is what the API calls.

Pydantic is imported with this module. ``datagrunt.rationalization`` does not import it, so the package's light
names (``Key``, ``RationalizationError``) never load pydantic.
"""

import copy
import json
import math
import re
import typing
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum, Flag
from types import MappingProxyType, UnionType
from typing import Annotated, Any, Literal, NewType, TypeAlias, TypeGuard, Union, get_args, get_origin

from pydantic import (
    AliasChoices,
    AliasPath,
    AwareDatetime,
    BaseModel,
    Discriminator,
    NaiveDatetime,
    PydanticUserError,
    RootModel,
    ValidationError,
)
from pydantic.fields import FieldInfo

from datagrunt.rationalization._canonical import canonical_json, sha256_hex
from datagrunt.rationalization._refs import DEFS_POINTER, DEFS_PREFIX, UNREAD_KEYWORDS, def_name, json_pointer
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.markers import (
    KeyNormalizer,
    _check_normalized_namespace,
    _check_normalizer,
    _InformationalMarker,
    _KeyMarker,
)

MAX_DEPTH = 32
"""Levels on one path: models and lists for :func:`describe`, objects and arrays for :func:`check_json_schema`."""
MAX_FIELDS = 10_000
"""Fields in a schema's expanded tree: a model reached by two paths counts twice."""
MAX_WALK_STEPS = 100_000
"""Nodes one walk visits: annotations for :func:`check_schema`, JSON-schema nodes for :func:`check_json_schema`."""
DEFAULT_HINT = " (a default: pydantic does not validate defaults, so fix the field's default)"
"""What :func:`instance_problems` adds to a problem whose value a default gave (one the input never set)."""
FLOAT_INT_LIMIT = 2**53
"""The largest magnitude of an ``int`` a ``float`` field admits (:func:`admits_scalar`): every int up to it is exactly
a float, and 2**53 + 1 is the first int with no exact float."""

FieldKind: TypeAlias = Literal["scalar", "scalar_list", "model", "model_list"]
"""What a field holds: a scalar, a list of scalars, a model, or a list of models."""

_FIELD_KINDS = frozenset(get_args(FieldKind))
_MODEL_KINDS = frozenset({"model", "model_list"})
_LIST_KINDS = frozenset({"scalar_list", "model_list"})
_SCALAR_TYPES: tuple[type, ...] = (str, int, float, bool, Decimal, date, datetime)
_PLAIN_SCALARS: tuple[type, ...] = (str, int, float, bool, Decimal, date)
_DATETIME_ZONES: tuple[tuple[object, Literal["aware", "naive"]], ...] = (
    (AwareDatetime, "aware"),
    (NaiveDatetime, "naive"),
)
_TIME_ZONES = ("aware", "naive")
_ENUM_BASES: tuple[type, ...] = (str, int)
_LITERAL_BASES: tuple[type, ...] = (str, int, bool)
_KEY_TYPES: tuple[type, ...] = (str, int, Decimal, date, datetime)
_UNION_ORIGINS: tuple[object, ...] = (Union, UnionType)
_NONE_TYPE = type(None)
_TYPE_ALIAS_TYPES: tuple[type, ...] = tuple(
    kind for kind in (getattr(typing, "TypeAliasType", None),) if isinstance(kind, type)
)
_SUPPORTED = (
    "supported: str, int, float, bool, Decimal, date, pydantic.AwareDatetime, pydantic.NaiveDatetime, an Enum of "
    "str or int values, a Literal of str, int or bool values, a BaseModel, list[X] of one of those, and X | None "
    "of a scalar or a model"
)
_KEY_RULE = (
    "a Key field must be str, int, Decimal, date, an AwareDatetime or NaiveDatetime, an Enum, or a Literal of str "
    "or int values"
)
_DISCRIMINATED = "discriminated unions are not supported (Field(discriminator=...) or Discriminator(...))"
_BRIEF_CHARS = 80
_NAMES_SHOWN = 10

_JSON_TYPES = frozenset({"array", "boolean", "integer", "null", "number", "object", "string"})
_CONTAINER_TYPES = frozenset({"array", "object"})
_TYPED_KEYWORDS = ("type", "$ref", "anyOf", "enum", "const")
# What model_json_schema() raises on a schema it cannot build: its own error, and what escapes it on a tampered
# WithJsonSchema (a $ref to a missing $defs entry, a $ref that is not a str, nesting deeper than the stack).
_PYDANTIC_SCHEMA_ERRORS: tuple[type[BaseException], ...] = (PydanticUserError, KeyError, AttributeError, RecursionError)
_KIND_WORDS = MappingProxyType(
    {"object": "an object", "array": "an array", "scalar": "a scalar value", "union": "a union of shapes"}
)
# What pydantic 2.14 emits for a scalar of each base, which the agreement step requires: its JSON type, the format
# of a date or datetime, and a Decimal's anyOf pair (pinned by the tests' PINNED_SCALAR_JSON).
_JSON_TYPE_OF: Mapping[type, str] = MappingProxyType(
    {str: "string", int: "integer", float: "number", bool: "boolean", date: "string", datetime: "string"}
)
_JSON_FORMAT_OF: Mapping[type, str] = MappingProxyType({date: "date", datetime: "date-time"})
_DECIMAL_TYPES = ("number", "string")
# Keywords that shape a value. Beside a $ref or Optional's anyOf (where json_schema_extra puts an override on an
# Enum, model or Optional field), the agreement step cannot weigh them against what the $ref or anyOf describes.
_VALUE_KEYWORDS = ("type", "enum", "const", "format", "items", "properties", "anyOf", "$ref")
# Keywords that describe a value without shaping it: left out of the JSON a message shows.
_ANNOTATION_KEYWORDS = frozenset({"title", "description", "default", "examples", "deprecated"})
_R8_ESCAPED = re.compile(
    r"\r\n?|[\u0000-\u0008\u000b-\u000c\u000e-\u001f\u007f-\u009f"
    r"\u202a-\u202e\u2066-\u2069\ud800-\udfff\U000e0000-\U000e007f]"
)
_JSON_SHOWN = 200
_FROZEN_ERRORS = frozenset({"frozen_field", "frozen_instance"})
_MISSING = object()  # what getattr gives for a field an instance lacks (a model_construct() instance)
_FAST_EXACT_BASES: frozenset[type] = frozenset({str, int, bool, date})
_FUNCTION_SCHEMA_TYPES: frozenset[str] = frozenset(
    {"function-before", "function-wrap", "function-plain", "function-after"}
)
_VALIDATE_METHODS: tuple[str, ...] = ("model_validate", "model_validate_json", "model_validate_strings")


@dataclass(frozen=True, slots=True)
class KeySpec:
    """The key fields of one model: records whose key values are equal describe the same entity.

    Args:
        fields: The key field names, in declaration order; empty when the model has no key.
        namespace: The namespace every key field's ``Key`` names (normalized), or ``None``.

    Raises:
        TypeError: If ``fields`` is not a tuple of ``str``, or ``namespace`` is neither ``None`` nor a ``str``.
        ValueError: If a field name is empty or repeated, or a namespace is named without key fields or is not
            normalized (``Key(namespace)`` normalizes one).
    """

    fields: tuple[str, ...]
    namespace: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.fields, tuple) or not all(isinstance(name, str) for name in self.fields):
            raise TypeError(f"KeySpec fields must be a tuple of str, got {_brief(self.fields)}")
        if not all(self.fields) or len(set(self.fields)) != len(self.fields):
            raise ValueError(f"KeySpec fields must be non-empty and distinct, got {self.fields!r}")
        if self.namespace is not None and not self.fields:
            raise ValueError(f"KeySpec names the namespace {self.namespace!r} for no key field")
        _check_normalized_namespace(self.namespace)


@dataclass(frozen=True, slots=True)
class ScalarSpec:
    """The Python type of the values a scalar field holds, and the Enum, Literal or time zone kind that bounds them.

    ``base`` is exact, never a base class: a ``bool`` field's is ``bool``, not ``int``, and a datetime field's is
    ``datetime``, not ``date``. An Enum field's base is the type of its values (``str`` or ``int``), a Literal
    field's the type of its values (``str``, ``int`` or ``bool``). A datetime field is aware or naive, never both.

    Args:
        base: ``str``, ``int``, ``float``, ``bool``, ``Decimal``, ``date`` or ``datetime``.
        enum: The field's Enum class, or ``None``.
        literal: The field's Literal values in declaration order, or ``None``.
        tz: ``"aware"`` (``pydantic.AwareDatetime``) or ``"naive"`` (``pydantic.NaiveDatetime``) when ``base`` is
            ``datetime``, else ``None``.

    Raises:
        TypeError: If ``base`` is not a class, ``enum`` neither ``None`` nor an Enum class, or ``literal`` neither
            ``None`` nor a tuple.
        ValueError: If ``base`` is another class, both ``enum`` and ``literal`` are given, ``tz`` is missing for a
            datetime, given for another base, or neither ``"aware"`` nor ``"naive"``, the Enum is a ``Flag``, has
            no member, or has a value whose exact type is not ``base`` (``str`` or ``int``), or ``literal`` is empty
            or holds a value whose exact type is not ``base`` (``str``, ``int`` or ``bool``).
    """

    base: type
    enum: type[Enum] | None = None
    literal: tuple[object, ...] | None = None
    tz: Literal["aware", "naive"] | None = None
    _literal_set: frozenset[tuple[type, object]] | None = dc_field(init=False, repr=False, compare=False, default=None)

    def __post_init__(self) -> None:
        if not isinstance(self.base, type):
            raise TypeError(f"ScalarSpec base must be a class, got {_brief(self.base)}")
        if not any(self.base is scalar for scalar in _SCALAR_TYPES):
            raise ValueError(
                f"ScalarSpec base must be one of {_names(kind.__name__ for kind in _SCALAR_TYPES)}, got {self.base!r}"
            )
        if self.enum is not None and self.literal is not None:
            raise ValueError("ScalarSpec takes an enum or a literal, not both")
        if (self.base is datetime) != (self.tz is not None) or (self.tz is not None and self.tz not in _TIME_ZONES):
            raise ValueError(
                f"ScalarSpec: a datetime takes tz 'aware' or 'naive' and no other base takes one; "
                f"got base {self.base.__name__}, tz {_brief(self.tz)}"
            )
        if self.enum is not None:
            _check_enum_values(self.enum, self.base)
        if self.literal is not None:
            _check_literal_values(self.literal, self.base)
            object.__setattr__(self, "_literal_set", frozenset((type(item), item) for item in self.literal))


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One field of a model, as :func:`describe` read its annotation.

    Args:
        name: The field's attribute name.
        wire_name: The name its value travels under in a record: the one the JSON schema gives it and
            ``model_validate`` reads (its validation alias or alias, else ``name``).
        kind: ``"scalar"``, ``"scalar_list"``, ``"model"`` or ``"model_list"``.
        optional: Whether the field's annotation admits ``None`` (``X | None``). A list is never Optional. A
            record may still hold ``None`` in a non-Optional field: a ``default_factory`` returning it is not
            validated (module notes, 2).
        required: Whether ``model_validate`` requires the field (``FieldInfo.is_required()``: it has neither a
            default nor a ``default_factory``). Independent of ``optional``: ``x: int | None`` is required, and
            ``n: int = 1`` is not. Consensus reads it for "null = not shown": a null in a field that is neither
            Optional nor required is dropped, so its default applies.
        child: The spec of the model a ``"model"`` or ``"model_list"`` field holds, else ``None``.
        scalar: The type of the values a ``"scalar"`` or ``"scalar_list"`` field holds, else ``None``.

    Raises:
        TypeError: If ``name`` or ``wire_name`` is not a ``str``, ``optional`` or ``required`` not a ``bool``,
            ``child`` neither
            ``None`` nor a :class:`ModelSpec`, or ``scalar`` neither ``None`` nor a :class:`ScalarSpec`.
        ValueError: If ``name`` is empty, ``wire_name`` blank, ``kind`` unknown, ``child`` given exactly for a
            scalar kind, ``scalar`` given exactly for a model kind, or a list is Optional.
    """

    name: str
    wire_name: str
    kind: FieldKind
    optional: bool
    required: bool
    child: "ModelSpec | None"
    scalar: ScalarSpec | None
    informational: bool = False
    key_normalizer: KeyNormalizer | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not isinstance(self.wire_name, str):
            raise TypeError(
                f"FieldSpec name and wire_name must be str, got {_brief(self.name)}, {_brief(self.wire_name)}"
            )
        if not self.name or not self.wire_name.strip() or self.kind not in _FIELD_KINDS:
            raise ValueError(
                f"FieldSpec needs a non-empty name and wire_name and one of {sorted(_FIELD_KINDS)}, got {self!r}"
            )
        if not isinstance(self.optional, bool):
            raise TypeError(f"FieldSpec {self.name!r}: optional must be a bool, got {_brief(self.optional)}")
        if not isinstance(self.required, bool):
            raise TypeError(f"FieldSpec {self.name!r}: required must be a bool, got {_brief(self.required)}")
        if self.child is not None and not isinstance(self.child, ModelSpec):
            raise TypeError(f"FieldSpec {self.name!r}: child must be a ModelSpec or None, got {_brief(self.child)}")
        if self.scalar is not None and not isinstance(self.scalar, ScalarSpec):
            raise TypeError(f"FieldSpec {self.name!r}: scalar must be a ScalarSpec or None, got {_brief(self.scalar)}")
        holds_models = self.kind in _MODEL_KINDS
        if (self.child is not None) != holds_models or (self.scalar is not None) == holds_models:
            raise ValueError(
                f"FieldSpec {self.name!r}: a {self.kind} field needs a child spec when it holds models and a scalar "
                f"spec when it holds scalars, never both"
            )
        if self.optional and self.kind in _LIST_KINDS:
            raise ValueError(f"FieldSpec {self.name!r}: a list field cannot be Optional")
        if not isinstance(self.informational, bool):
            raise TypeError(f"FieldSpec {self.name!r}: informational must be a bool, got {_brief(self.informational)}")
        if self.informational and self.kind in _MODEL_KINDS:
            raise ValueError(
                f"FieldSpec {self.name!r}: Informational supports scalar and scalar_list fields, not {self.kind}"
            )
        _check_normalizer(self.key_normalizer)
        if self.key_normalizer is not None:
            if self.informational:
                raise ValueError(f"FieldSpec {self.name!r}: an Informational field cannot have a key_normalizer")
            if self.kind != "scalar" or self.optional or self.scalar is None or self.scalar.base is not str:
                raise ValueError(
                    f"FieldSpec {self.name!r}: key_normalizer={self.key_normalizer!r} requires a non-Optional str key"
                )


@dataclass(frozen=True, slots=True)
class ModelSpec:
    """One model of a schema: its keys and fields, nested models described in the same way.

    Args:
        model: The model class.
        keys: Its key fields.
        fields: Its fields in declaration order: at least one, names and wire names distinct.

    Raises:
        TypeError: If ``model`` is not a ``BaseModel`` subclass, ``keys`` not a :class:`KeySpec`, or ``fields``
            not a tuple of :class:`FieldSpec`.
        ValueError: If ``fields`` is empty or repeats a name or a wire name, or a key names no non-Optional
            scalar field.
    """

    model: type[BaseModel]
    keys: KeySpec
    fields: tuple[FieldSpec, ...]
    key_normalizers: tuple[KeyNormalizer | None, ...] = dc_field(init=False, compare=False, repr=False, default=())
    fields_by_name: Mapping[str, FieldSpec] = dc_field(init=False, compare=False, repr=False)
    reads_by_name: bool = dc_field(init=False, compare=False, repr=False, default=False)
    has_unshown_droppable: bool = dc_field(init=False, compare=False, repr=False, default=False)
    has_keyed_list: bool = dc_field(init=False, compare=False, repr=False, default=False)

    def __post_init__(self) -> None:
        if not _is_model_class(self.model):
            raise TypeError(f"ModelSpec model must be a BaseModel subclass, got {_brief(self.model)}")
        if not isinstance(self.keys, KeySpec):
            raise TypeError(f"ModelSpec keys must be a KeySpec, got {_brief(self.keys)}")
        if not isinstance(self.fields, tuple) or not all(isinstance(field, FieldSpec) for field in self.fields):
            raise TypeError(f"ModelSpec fields must be a tuple of FieldSpec, got {_brief(self.fields)}")
        by_name = {field.name: field for field in self.fields}
        wire_names = {field.wire_name for field in self.fields}
        if not self.fields or len(by_name) != len(self.fields) or len(wire_names) != len(self.fields):
            raise ValueError(
                f"ModelSpec {self.model.__name__}: fields must be non-empty, names and wire names distinct"
            )
        for name in self.keys.fields:
            field = by_name.get(name)
            if field is None or field.kind != "scalar" or field.optional:
                raise ValueError(f"ModelSpec {self.model.__name__}: key {name!r} names no non-Optional scalar field")
            if field.informational:
                raise ValueError(f"ModelSpec {self.model.__name__}: key {name!r} cannot be Informational")
        for field in self.fields:
            if field.key_normalizer is not None and field.name not in self.keys.fields:
                raise ValueError(f"ModelSpec {self.model.__name__}: non-key {field.name!r} has key_normalizer")
        _compile_model_spec(self, by_name)


def _compile_model_spec(spec: ModelSpec, by_name: dict[str, FieldSpec]) -> None:
    """Populate derived non-compared dispatch slots on ``spec`` from ``by_name`` and ``spec.fields``."""
    object.__setattr__(spec, "key_normalizers", tuple(by_name[name].key_normalizer for name in spec.keys.fields))
    object.__setattr__(spec, "fields_by_name", MappingProxyType(by_name))
    config = spec.model.model_config
    object.__setattr__(spec, "reads_by_name", bool(config.get("validate_by_name") or config.get("populate_by_name")))
    has_unshown = any(
        (not field.optional and not field.required) or (field.child is not None and field.child.has_unshown_droppable)
        for field in spec.fields
    )
    object.__setattr__(spec, "has_unshown_droppable", has_unshown)
    has_keyed = any(
        field.child is not None
        and ((field.kind == "model_list" and bool(field.child.keys.fields)) or field.child.has_keyed_list)
        for field in spec.fields
    )
    object.__setattr__(spec, "has_keyed_list", has_keyed)


@dataclass(frozen=True, slots=True)
class PreparedSchema:
    """A checked schema with everything rationalize() derives from it (built by :func:`prepare`).

    The wrapper is kept as its canonical JSON, the exact text :attr:`sha256` hashes; :attr:`wrapper` parses it
    afresh on each access, so no caller can change what another one sees, or what the hash stands for.

    Args:
        model: The schema's model class.
        spec: Its spec (:func:`describe`).
        wrapper_json: The response wrapper (:func:`response_schema`) as canonical JSON.
        sha256: The SHA-256 hex digest of ``wrapper_json`` in UTF-8 (:func:`schema_sha256`).

    Raises:
        TypeError: If ``model`` is not a ``BaseModel`` subclass, ``spec`` not a :class:`ModelSpec`, or
            ``wrapper_json`` not a ``str``.
        ValueError: If ``spec`` describes another model, or ``sha256`` is not the digest of ``wrapper_json``.
    """

    model: type[BaseModel]
    spec: ModelSpec
    wrapper_json: str
    sha256: str
    is_pure_pydantic: bool = dc_field(init=False, compare=False, repr=False, default=False)

    def __post_init__(self) -> None:
        if not _is_model_class(self.model) or not isinstance(self.spec, ModelSpec):
            raise TypeError(f"PreparedSchema needs a model class and a ModelSpec, got {_brief(self.model)}")
        if self.spec.model is not self.model:
            raise ValueError(f"PreparedSchema spec describes {self.spec.model.__name__}, not {self.model.__name__}")
        if not isinstance(self.wrapper_json, str):
            raise TypeError(f"PreparedSchema wrapper_json must be a str, got {type(self.wrapper_json).__name__}")
        try:
            digest = sha256_hex(self.wrapper_json)
        except RationalizationError as error:  # a lone surrogate: no digest can match it
            raise ValueError("PreparedSchema wrapper_json is not valid Unicode") from error
        if self.sha256 != digest:
            raise ValueError("PreparedSchema sha256 does not match wrapper_json")
        object.__setattr__(self, "is_pure_pydantic", _spec_is_pure_pydantic(self.spec))

    @property
    def wrapper(self) -> dict[str, Any]:
        """The response wrapper, parsed afresh on each access: a caller may transform it freely."""
        parsed: dict[str, Any] = json.loads(self.wrapper_json)
        return parsed


def _trusted_prepared_schema(
    model: type[BaseModel],
    spec: ModelSpec,
    wrapper_json: str,
    sha256: str,
) -> PreparedSchema:
    """Build a :class:`PreparedSchema` from verified ``prepare()`` outputs without hashing ``wrapper_json`` twice."""
    prepared = object.__new__(PreparedSchema)
    object.__setattr__(prepared, "model", model)
    object.__setattr__(prepared, "spec", spec)
    object.__setattr__(prepared, "wrapper_json", wrapper_json)
    object.__setattr__(prepared, "sha256", sha256)
    object.__setattr__(prepared, "is_pure_pydantic", _spec_is_pure_pydantic(spec))
    return prepared


def _push_core_children(stack: list[object], node: object) -> None:
    """Push child schema containers of ``node`` (excluding opaque ``metadata``) onto ``stack``."""
    if isinstance(node, Mapping):
        for key, value in node.items():
            if key != "metadata" and isinstance(value, (Mapping, list, tuple)):
                stack.append(value)
    elif isinstance(node, (list, tuple)):
        for item in node:
            if isinstance(item, (Mapping, list, tuple)):
                stack.append(item)


def _has_default_model_hooks(model: type[BaseModel]) -> bool:
    """Whether ``model`` leaves ``BaseModel``'s init, post-init, and ``model_validate*`` classmethods untouched."""
    if model.__init__ is not BaseModel.__init__ or model.model_post_init is not BaseModel.model_post_init:
        return False
    for method_name in _VALIDATE_METHODS:
        own_func = getattr(getattr(model, method_name, None), "__func__", object())
        base_func = getattr(getattr(BaseModel, method_name, None), "__func__", None)
        if own_func is not base_func:
            return False
    return True


def _model_is_pure_pydantic(model: type[BaseModel]) -> bool:
    """Whether ``model`` keeps default hooks/classmethods and has zero custom function validators."""
    if not _has_default_model_hooks(model):
        return False
    core_schema = getattr(model, "__pydantic_core_schema__", None)
    if not isinstance(core_schema, Mapping):
        return False
    stack: list[object] = [core_schema]
    for _ in range(MAX_WALK_STEPS):
        if not stack:
            return True
        node = stack.pop()
        if isinstance(node, Mapping) and node.get("type") in _FUNCTION_SCHEMA_TYPES:
            return False
        _push_core_children(stack, node)
    return False


def _spec_is_pure_pydantic(spec: ModelSpec) -> bool:
    """Whether every reachable model in ``spec`` is pure Pydantic without custom hooks or validators."""
    checked_models: set[type[BaseModel]] = set()
    seen_specs: set[int] = {id(spec)}
    stack: list[ModelSpec] = [spec]
    for _ in range(MAX_FIELDS + 1):
        if not stack:
            return True
        current = stack.pop()
        if current.model not in checked_models:
            if not _model_is_pure_pydantic(current.model):
                return False
            checked_models.add(current.model)
        for field in current.fields:
            if field.child is not None and id(field.child) not in seen_specs:
                seen_specs.add(id(field.child))
                stack.append(field.child)
    return False


@dataclass(frozen=True, slots=True)
class _Shape:
    """What one annotation holds: the model class for a model kind, the scalar spec for a scalar kind."""

    kind: FieldKind
    optional: bool
    model: type[BaseModel] | None
    scalar: ScalarSpec | None


@dataclass(frozen=True, slots=True)
class _Node:
    """A JSON-schema node to visit: where it is, the objects and arrays above it, the $defs entered to reach it."""

    schema: object
    pointer: str
    depth: int
    refs: frozenset[str]


class _FieldBudget:
    """The fields one :func:`describe` call may still expand, so a schema doubling per level stops early."""

    __slots__ = ("left",)

    def __init__(self) -> None:
        self.left = MAX_FIELDS

    def spend(self, path: str) -> None:
        """Count the field at ``path``; raise once more than :data:`MAX_FIELDS` were counted."""
        self.left -= 1
        if self.left < 0:
            raise RationalizationError(f"{path}: the schema expands to more than {MAX_FIELDS} fields")


# ---------------------------------------------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------------------------------------------


def check_schema(schema: object) -> type[BaseModel]:
    """``schema`` if it and every model its fields name is a class rationalize() accepts (module notes, 1).

    Args:
        schema: What the caller passed as the schema.

    Returns:
        ``schema``, the model class.

    Raises:
        RationalizationError: If ``schema`` is not a ``BaseModel`` subclass (an instance, ``BaseModel`` itself,
            anything else), or it or a model its fields name is a ``RootModel``, not built (not fully defined, or
            ``defer_build=True``), without fields, ``extra="allow"``, ``use_enum_values=True``, or a
            ``json_schema_mode_override`` other than ``"validation"``. The message names the value or the field
            path.
    """
    model = _model_class(schema)
    for path, found in _reachable_models(model):
        _check_model_class(found, path)
    return model


def describe(model: object, key: str | Sequence[str] | None = None) -> ModelSpec:
    """The spec of ``model``: each field's kind, wire name and nesting, and each model's keys (module notes, 2).

    Each model is checked as :func:`check_schema` checks it when the walk reaches it, after the annotation that
    names it, so an unsupported annotation is named before anything about a model inside it.

    Args:
        model: The schema.
        key: The root's key field names (attribute names), as an alternative to marking them with ``Key``;
            ``None`` reads the marks alone. Child models always declare their keys with ``Key``.

    Returns:
        The root :class:`ModelSpec`, its fields in declaration order.

    Raises:
        RationalizationError: If ``model`` is not a model class, a model it reaches breaks a class rule, ``key`` is
            not a non-empty field name or sequence of distinct ones, or a field or key breaks a rule; the message
            names the dotted field path.
    """
    return _describe(_model_class(model), _key_names(key))


def check_json_schema(model: object) -> None:
    """Raise unless pydantic's JSON schema for ``model`` stays in the supported subset (module notes, 4).

    Args:
        model: The schema (checked by :func:`check_schema` first).

    Raises:
        RationalizationError: If :func:`check_schema` does, pydantic cannot build the JSON schema (its own error,
            or a ``KeyError``, ``AttributeError`` or ``RecursionError`` escaping it, chained), the schema is nested
            too deeply to copy, or it leaves the subset; the message names the JSON pointer.
    """
    checked = check_schema(model)
    _check_document(_json_schema(checked), checked.__name__)


def response_schema(model: object) -> dict[str, Any]:
    """The JSON schema a model's response must match: records of ``model``, each with its grounding.

    Args:
        model: The schema (checked by :func:`check_schema` first).

    Returns:
        A fresh structure on each call (nothing shared with pydantic or another call): ``records``, an array of
        objects holding ``record`` (the model's schema without ``$defs``), ``grounding_quotes`` and
        ``source_element_ids`` (arrays of strings), all required, no other keys; the model's ``$defs``, if any,
        at the root.

    Raises:
        RationalizationError: If :func:`check_schema` does, or the JSON schema cannot be built or copied (as for
            :func:`check_json_schema`).
    """
    checked = check_schema(model)
    return _wrap(_json_schema(checked))


def schema_sha256(wrapper: Mapping[str, Any]) -> str:
    """The SHA-256 hex digest of ``wrapper``'s canonical JSON: sorted keys, compact separators, UTF-8.

    Args:
        wrapper: A response wrapper (:func:`response_schema`).

    Returns:
        64 lower-case hex characters, the same for equal structures whatever their insertion order.

    Raises:
        RationalizationError: If ``wrapper`` is not a mapping, or holds what canonical JSON cannot: a value that
            is not JSON, a non-finite float, keys that do not sort, a string that is not UTF-8, or nesting deeper
            than the stack (the ``RecursionError`` chained).
    """
    return sha256_hex(_canonical_json(wrapper))


def admits_scalar(scalar: ScalarSpec, value: object) -> bool:
    """Whether a field of ``scalar`` admits ``value`` (not ``None``), as a validated record holds it.

    The value's exact type must be the field's base (a ``bool`` is not an ``int``, a ``datetime`` not a ``date``),
    with two lossless widenings: an ``int`` (never a ``bool``) in a ``float`` field when its magnitude is at most
    :data:`FLOAT_INT_LIMIT`, and any ``int`` in a ``Decimal`` field. An Enum field admits only its own members, a
    Literal field only its values (each of its exact type), a datetime field only datetimes of its kind (aware or
    naive). Pydantic does not validate defaults, so :func:`describe` checks static defaults with this, and consensus
    every value a record holds after validation (what a ``default_factory`` gave included).

    Args:
        scalar: The field's scalar spec.
        value: The value.

    Returns:
        Whether the field admits it.
    """
    if not isinstance(scalar, ScalarSpec):
        raise TypeError(f"scalar must be a ScalarSpec, got {_brief(scalar)}")
    if scalar.enum is not None:
        return type(value) is scalar.enum
    kind = type(value)
    if isinstance(value, int) and kind is int and scalar.base is float:
        return abs(value) <= FLOAT_INT_LIMIT
    if kind is int and scalar.base is Decimal:
        return True
    if kind is not scalar.base:
        return False
    if scalar._literal_set is not None:
        return (kind, value) in scalar._literal_set
    if isinstance(value, datetime):
        return (value.utcoffset() is not None) == (scalar.tz == "aware")
    return True


def value_problems(field: FieldSpec, value: object, path: str = "") -> list[str]:
    """Each way ``value`` is not a value of ``field``, as a ``"<path>: <problem>"`` line (``path`` leads each).

    The check pydantic leaves undone for defaults (module notes, 2): a value whose type the field does not admit
    (:func:`admits_scalar` for a scalar, a ``list`` of admitted items for a list, an instance of exactly its class
    for a model), ``None`` in a non-Optional field, and, inside a model instance, a field it lacks (an instance
    built by ``model_construct()`` can). A model instance's fields are checked in turn, by its spec. The walk follows
    the spec, so it goes no deeper than the spec's depth, even through an instance that contains itself, and checks
    each value it reaches once. A problem never shows a value, only its type.

    Args:
        field: The field's spec.
        value: The value it holds.
        path: The field's attribute path, prefixed to each problem; ``""`` for none.

    Returns:
        The problems in field order; ``[]`` when the field admits ``value``.
    """
    return _walk_values([_Visit(field, value, path, None)])


def instance_problems(instance: BaseModel, spec: ModelSpec) -> list[str]:
    """Each way the fields of ``instance`` are not values of ``spec`` (:func:`value_problems`, by attribute path).

    A problem whose value a default gave (its field, or a model holding it, was not set by the input: not in the
    instance's ``model_fields_set``) ends with :data:`DEFAULT_HINT`; one the input gave (a non-finite float that
    lax validation took, say) does not, so the message never blames a default for what the model sent.

    Args:
        instance: An instance of ``spec.model`` (a validated record, say).
        spec: Its spec.

    Returns:
        The problems in field order; ``[]`` when every field holds a value the spec admits.
    """
    if not isinstance(spec, ModelSpec):
        raise TypeError(f"spec must be a ModelSpec, got {_brief(spec)}")
    if _instance_ok(instance, spec):
        return []
    return _walk_values(list(reversed(_child_visits(spec, instance, "", False))))


def prepare(schema: object, key: str | Sequence[str] | None = None) -> PreparedSchema:
    """Check ``schema`` and derive everything rationalize() needs from it, before any work starts.

    Runs :func:`describe` (with the class checks of :func:`check_schema`), the agreement of the spec with the JSON
    schema (module notes, 3), :func:`check_json_schema`'s walk, then builds the wrapper (:func:`response_schema`)
    and its hash (:func:`schema_sha256`), in that order: the first failure raises.

    Args:
        schema: What the caller passed as the schema.
        key: The root's key field names, or ``None`` (:func:`describe`).

    Returns:
        The :class:`PreparedSchema`.

    Raises:
        RationalizationError: If any step does.
    """
    model = _model_class(schema)
    spec = _describe(model, _key_names(key))
    document = _json_schema(model)
    _check_agreement(spec, document)
    _check_document(document, model.__name__)
    text = _canonical_json(_wrap(document))
    return _trusted_prepared_schema(model=model, spec=spec, wrapper_json=text, sha256=sha256_hex(text))


# ---------------------------------------------------------------------------------------------------------------
# Classes (check_schema, and describe as it reaches each model)
# ---------------------------------------------------------------------------------------------------------------


def _is_model_class(value: object) -> TypeGuard[type[BaseModel]]:
    """Whether ``value`` is a proper ``BaseModel`` subclass (``BaseModel`` itself is not)."""
    return isinstance(value, type) and issubclass(value, BaseModel) and value is not BaseModel


def _model_class(schema: object) -> type[BaseModel]:
    """``schema`` as a model class, or a RationalizationError naming what it is instead."""
    if isinstance(schema, BaseModel):
        raise RationalizationError(
            f"schema must be a pydantic BaseModel subclass, not an instance of {type(schema).__name__}: pass the class"
        )
    if schema is BaseModel:
        raise RationalizationError("schema must be a subclass of pydantic's BaseModel, not BaseModel itself")
    if not _is_model_class(schema):
        raise RationalizationError(
            f"schema must be a pydantic BaseModel subclass, got {_brief(schema)} ({type(schema).__name__})"
        )
    return schema


def _reachable_models(model: type[BaseModel]) -> list[tuple[str, type[BaseModel]]]:
    """``model`` and each model class its field annotations name, once each, with the first field path to it.

    Literal values and ``Annotated`` metadata are values, not annotations, so the walk does not enter them.
    """
    found: dict[type[BaseModel], str] = {model: model.__name__}
    stack = _field_annotations(model, model.__name__)
    for _ in range(MAX_WALK_STEPS):
        if not stack:
            break
        path, annotation = stack.pop()
        if _is_model_class(annotation):
            if annotation not in found:
                found[annotation] = path
                stack.extend(_field_annotations(annotation, path))
            continue
        origin = get_origin(annotation)
        if origin is Literal:
            continue
        arguments = get_args(annotation)[:1] if origin is Annotated else get_args(annotation)
        inner_path = f"{path}[]" if origin is list else path
        stack.extend((inner_path, argument) for argument in reversed(arguments))
    if stack:
        raise RationalizationError(f"{model.__name__}: more than {MAX_WALK_STEPS} annotations to walk")
    return [(path, found_model) for found_model, path in found.items()]


def _field_annotations(model: type[BaseModel], path: str) -> list[tuple[str, object]]:
    """Each field's path and annotation, last field first: popped from a stack, they come in declaration order."""
    return [(f"{path}.{name}", info.annotation) for name, info in reversed(model.model_fields.items())]


def _check_model_class(model: type[BaseModel], path: str) -> None:
    """Raise unless ``model``, found at ``path``, is a built model of named fields with bounded keys."""
    name = model.__name__
    if issubclass(model, RootModel):
        raise RationalizationError(f"{path}: {name} is a RootModel; a record must be a model with named fields")
    if not model.__pydantic_complete__ and model.model_config.get("defer_build"):
        raise RationalizationError(
            f"{path}: {name} sets model_config defer_build=True and is not built yet; call {name}.model_rebuild() "
            f"before passing it"
        )
    if not model.__pydantic_complete__:
        raise RationalizationError(
            f"{path}: {name} is not fully defined: define the types it refers to, then call {name}.model_rebuild()"
        )
    _check_model_config(model, path)
    if not model.model_fields:
        raise RationalizationError(f"{path}: {name} has no fields")


def _check_model_config(model: type[BaseModel], path: str) -> None:
    """Raise if the ``model_config`` of ``model`` would let its records or JSON schema differ from its spec."""
    name = model.__name__
    config = model.model_config
    if config.get("extra") == "allow":
        raise RationalizationError(
            f"{path}: {name} sets model_config extra='allow', which admits unbounded keys; use 'ignore' or 'forbid'"
        )
    mode = config.get("json_schema_mode_override")
    if mode is not None and mode != "validation":
        raise RationalizationError(
            f"{path}: {name} sets model_config json_schema_mode_override={mode!r}; the JSON schema a model answers "
            f"must be the one model_validate accepts, so leave it unset or 'validation'"
        )
    if config.get("use_enum_values"):
        raise RationalizationError(
            f"{path}: {name} sets model_config use_enum_values=True, so its records would hold raw values where its "
            f"fields hold Enum members; leave it unset"
        )


# ---------------------------------------------------------------------------------------------------------------
# Annotations and nesting (describe)
# ---------------------------------------------------------------------------------------------------------------


def _describe(model: type[BaseModel], explicit: tuple[str, ...] | None) -> ModelSpec:
    """The spec of the model class ``model``, its root keys named by ``explicit`` when given."""
    return _describe_model(model, model.__name__, 1, (), explicit, _FieldBudget())


def _describe_model(
    model: type[BaseModel],
    path: str,
    depth: int,
    active: tuple[type[BaseModel], ...],
    explicit: tuple[str, ...] | None,
    budget: _FieldBudget,
) -> ModelSpec:
    """The spec of ``model`` at ``path``, level ``depth``, inside the models ``active`` on the way here."""
    if model in active:
        raise RationalizationError(
            f"{path}: {model.__name__} is recursive (it contains itself); recursive models are not supported"
        )
    _check_depth(depth, path)
    _check_model_class(model, path)
    inner = (*active, model)
    fields = tuple(
        _describe_field(model, name, info, f"{path}.{name}", depth, inner, budget)
        for name, info in model.model_fields.items()
    )
    _check_wire_names(path, fields)
    return ModelSpec(model=model, keys=_model_keys(model, path, explicit, fields), fields=fields)


def _field_markers(info: FieldInfo, shape: "_Shape", path: str) -> tuple[KeyNormalizer | None, bool]:
    """Extract ``(key_normalizer, informational)`` from ``info.metadata`` and reject illegal combinations."""
    key_markers = [item for item in info.metadata if isinstance(item, _KeyMarker)]
    informational = any(isinstance(item, _InformationalMarker) for item in info.metadata)
    if key_markers and informational:
        raise RationalizationError(f"{path}: a field cannot be both Key and Informational")
    if informational and shape.model is not None:
        raise RationalizationError(f"{path}: Informational supports scalar and scalar-list fields, not model fields")
    norm = key_markers[0].normalizer if len(key_markers) == 1 else None
    if norm is not None:
        _check_key_field(info, path)
        if shape.scalar is None or shape.scalar.base is not str:
            raise RationalizationError(
                f"{path}: Key(normalizer={norm!r}) requires a str key field, not {_type_name(info.annotation)}"
            )
    return norm, informational


def _describe_field(
    model: type[BaseModel],
    name: str,
    info: FieldInfo,
    path: str,
    depth: int,
    active: tuple[type[BaseModel], ...],
    budget: _FieldBudget,
) -> FieldSpec:
    """The spec of the field ``name`` of ``model`` at ``path``, in a model at level ``depth``."""
    budget.spend(path)
    if info.discriminator is not None or _has_discriminator(info.metadata):
        raise RationalizationError(f"{path}: {_DISCRIMINATED}")
    shape = _shape(info.annotation, path)
    key_normalizer, informational = _field_markers(info, shape, path)
    if isinstance(info.description, str) and (found := _R8_ESCAPED.search(info.description)) is not None:
        raise RationalizationError(
            f"{path}: its description holds U+{ord(found.group(0)[0]):04X}, a code point R8 escapes: "
            "remove it from the description"
        )
    _check_none_default(info, shape, path)
    wire_name = _wire_name(model, name, info, path)
    if shape.kind != "scalar":
        _check_depth(depth + 1, path)  # a list, or a nested model
    child: ModelSpec | None = None
    if shape.model is not None and shape.kind == "model":
        child = _describe_model(shape.model, path, depth + 1, active, None, budget)
    elif shape.model is not None:
        child = _describe_model(shape.model, f"{path}[]", depth + 2, active, None, budget)
    spec = FieldSpec(
        name=name,
        wire_name=wire_name,
        kind=shape.kind,
        optional=shape.optional,
        required=info.is_required(),
        child=child,
        scalar=shape.scalar if child is None else None,
        informational=informational,
        key_normalizer=key_normalizer,
    )
    _check_static_default(model, info, spec, path)
    return spec


def _check_depth(depth: int, path: str) -> None:
    """Raise if level ``depth`` at ``path`` is deeper than :data:`MAX_DEPTH`."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{path}: nesting is deeper than {MAX_DEPTH} models and lists")


def _check_none_default(info: FieldInfo, shape: _Shape, path: str) -> None:
    """Raise if the field at ``path`` defaults to ``None`` while its type does not admit ``None``."""
    if shape.optional or info.default is not None:
        return
    if shape.kind in _LIST_KINDS:
        raise RationalizationError(
            f"{path}: its default is None, which a list does not admit; give it the default [] "
            f"(Field(default_factory=list)), since a list cannot be Optional"
        )
    named = _type_name(_strip_annotated(info.annotation, path))
    raise RationalizationError(
        f"{path}: its default is None, which {named} does not admit: annotate {named} | None, or drop the default"
    )


def _check_static_default(model: type[BaseModel], info: FieldInfo, spec: FieldSpec, path: str) -> None:
    """Raise if the field at ``path`` has a static default that is no value of the field (module notes, 2).

    Pydantic does not validate defaults, so every record that left the field out would hold it unchecked, and
    consensus would quarantine each one, after its request was paid. The default is checked as consensus checks a
    record (:func:`value_problems`): a model instance field by field, by the field's spec. A ``None`` default was
    checked before (only an Optional field takes one), a ``default_factory`` is never called here (consensus checks
    what it gives), and a default pydantic validates (``validate_default``, on the field or, unless the field says
    otherwise, its model) must pass that validation instead (:func:`_check_validated_default`).
    """
    if info.is_required() or info.default_factory is not None or info.default is None:
        return
    validated = info.validate_default
    if validated is None:
        validated = bool(model.model_config.get("validate_default", False))
    if validated:
        _check_validated_default(model, spec.name, info, path)
        return
    problems = value_problems(spec, info.default)
    if problems:
        raise RationalizationError(
            f"{path}: its default {_brief(info.default)} is not a value of the field ({'; '.join(problems)}): "
            f"pydantic does not validate defaults, so every record that leaves the field out would hold it and be "
            f"quarantined; give a default of the field's own type, or set Field(validate_default=True)"
        )


def _check_validated_default(model: type[BaseModel], name: str, info: FieldInfo, path: str) -> None:
    """Raise if a static default pydantic validates (``validate_default``) fails the model's own validation of it.

    Pydantic would refuse it for every record that leaves the field out. The check is the model's own validator
    (``validate_assignment`` of the default to the field), so the model's field validators, its ``model_config``
    (``strict``, ``coerce_numbers_to_str``, ``str_max_length``, ...) and the field's constraints all apply. It runs
    against an instance built with every field set to ``None`` (``model_construct``), so no ``default_factory`` is
    called. Only a ``ValidationError`` whose every error lies at this field refuses the default; anything else is
    inconclusive and the default passes (consensus quarantines a record its validation refuses): an error elsewhere
    (a model validator meeting the half-built instance), a frozen field, which takes no assignment, or another
    exception. A ``MemoryError`` or ``RecursionError`` propagates.
    """
    names = {found for found in (name, info.alias, info.validation_alias) if isinstance(found, str)}
    try:
        instance = model.model_construct(**dict.fromkeys(model.model_fields))
        model.__pydantic_validator__.validate_assignment(instance, name, info.default)
    except ValidationError as error:
        details = error.errors(include_url=False)
        if not details or not all(_refuses(detail, names) for detail in details):
            return
        kinds = ", ".join(sorted({str(detail["type"]) for detail in details}))
        raise RationalizationError(
            f"{path}: its default {_brief(info.default)} fails the field's own validation ({kinds}), and "
            f"validate_default makes pydantic validate it for every record that leaves the field out; give a default "
            f"that validates"
        ) from error
    except (MemoryError, RecursionError):
        raise
    except Exception:  # inconclusive: a validator meeting the half-built instance, model_post_init, ...
        return


def _refuses(detail: Mapping[str, Any], names: set[str]) -> bool:
    """Whether a validation error refuses the default itself: it lies at the field, and it is not a frozen field's."""
    loc = detail.get("loc", ())
    return bool(loc) and loc[0] in names and detail.get("type") not in _FROZEN_ERRORS


def _wire_name(model: type[BaseModel], name: str, info: FieldInfo, path: str) -> str:
    """The name the field ``name`` travels under: what the JSON schema calls it and ``model_validate`` reads."""
    alias = info.validation_alias
    if isinstance(alias, AliasPath):
        raise RationalizationError(
            f"{path}: validation_alias=AliasPath(...) is not supported: a record carries each field under one name"
        )
    if isinstance(alias, AliasChoices):
        first = alias.choices[0] if alias.choices else None
        if not isinstance(first, str):
            raise RationalizationError(
                f"{path}: AliasChoices led by an AliasPath are not supported: put a plain name first"
            )
        alias = first
    if model.model_config.get("validate_by_alias", True) is False:
        return name
    if isinstance(alias, str):
        return alias
    return info.alias if isinstance(info.alias, str) else name


def _check_wire_names(path: str, fields: Sequence[FieldSpec]) -> None:
    """Raise if two fields of the model at ``path`` travel under the same wire name."""
    owners: dict[str, str] = {}
    for field in fields:
        owner = owners.setdefault(field.wire_name, field.name)
        if owner != field.name:
            raise RationalizationError(
                f"{path}.{field.name}: its wire name {field.wire_name!r} is also the wire name of {owner!r}"
            )


def _has_discriminator(metadata: Iterable[object]) -> bool:
    """Whether ``metadata`` asks for a discriminated union: a ``Discriminator``, or a ``Field(discriminator=)``."""
    return any(
        isinstance(item, Discriminator) or (isinstance(item, FieldInfo) and item.discriminator is not None)
        for item in metadata
    )


def _shape(annotation: object, path: str) -> _Shape:
    """What the annotation of the field at ``path`` holds, or a RationalizationError naming the field."""
    inner = _strip_annotated(annotation, path)
    if get_origin(inner) in _UNION_ORIGINS:
        return _optional_shape(inner, path)
    if inner is list or get_origin(inner) is list:
        return _list_shape(inner, path)
    return _element_shape(inner, False, path)


def _optional_shape(union: object, path: str) -> _Shape:
    """The shape of ``X | None`` (``None`` may be annotated, as ``SkipJsonSchema[None]``); any other union raises."""
    arguments = get_args(union)
    members = [argument for argument in arguments if _strip_annotated(argument, path) is not _NONE_TYPE]
    if len(members) != 1 or len(members) == len(arguments):
        raise RationalizationError(
            f"{path}: unions of two or more types ({_type_name(union)}) are not supported; only X | None is"
        )
    inner = _strip_annotated(members[0], path)
    if inner is list or get_origin(inner) is list:
        raise RationalizationError(
            f"{path}: a list cannot be Optional ({_type_name(union)}); use list[X] = [], the empty list for none"
        )
    return _element_shape(inner, True, path)


def _list_shape(list_type: object, path: str) -> _Shape:
    """The shape of ``list[X]``, X a scalar or a model."""
    arguments = get_args(list_type)
    if not arguments:
        raise RationalizationError(f"{path}: a list needs an item type, as list[str]")
    item = _strip_annotated(arguments[0], path)
    if get_origin(item) in _UNION_ORIGINS:
        raise RationalizationError(f"{path}: list items cannot be Optional or a union ({_type_name(list_type)})")
    if item is list or get_origin(item) is list:
        raise RationalizationError(f"{path}: lists cannot hold lists ({_type_name(list_type)})")
    if _is_model_class(item):
        return _Shape("model_list", False, model=item, scalar=None)
    scalar = _scalar_spec(item)
    if scalar is not None:
        return _Shape("scalar_list", False, model=None, scalar=scalar)
    raise RationalizationError(f"{path}: {_unsupported(item, 'list item type')}")


def _element_shape(inner: object, optional: bool, path: str) -> _Shape:
    """The shape of a model or a scalar; anything else raises."""
    if _is_model_class(inner):
        return _Shape("model", optional, model=inner, scalar=None)
    scalar = _scalar_spec(inner)
    if scalar is not None:
        return _Shape("scalar", optional, model=None, scalar=scalar)
    raise RationalizationError(f"{path}: {_unsupported(inner, 'type')}")


def _unsupported(annotation: object, what: str) -> str:
    """Why ``annotation`` is refused: a reason of its own for a known near miss, else the supported forms."""
    if annotation is datetime:
        return (
            "a plain datetime holds naive and aware values alike: annotate pydantic.AwareDatetime or "
            "pydantic.NaiveDatetime"
        )
    if isinstance(annotation, type) and issubclass(annotation, Flag):
        return (
            f"Flag enums are not supported ({annotation.__name__}): validation yields combinations of members the "
            f"enum does not list"
        )
    if isinstance(annotation, NewType):
        return (
            f"NewType aliases are not supported ({_type_name(annotation)}): annotate the type it names, "
            f"{_type_name(annotation.__supertype__)}"
        )
    if _TYPE_ALIAS_TYPES and isinstance(annotation, _TYPE_ALIAS_TYPES):
        return f"type aliases are not supported ({_type_name(annotation)}): annotate the type it stands for"
    return f"unsupported {what} {_type_name(annotation)}; {_SUPPORTED}"


def _strip_annotated(annotation: object, path: str) -> object:
    """``annotation`` without its ``Annotated`` metadata, which must hold neither ``Key`` nor a discriminator."""
    if get_origin(annotation) is not Annotated:
        return annotation
    inner, *metadata = get_args(annotation)
    if any(isinstance(item, _KeyMarker) for item in metadata):
        raise RationalizationError(
            f"{path}: Key must mark the field itself, as Annotated[str, Key], not a type inside it"
        )
    if any(isinstance(item, _InformationalMarker) for item in metadata):
        raise RationalizationError(
            f"{path}: Informational must mark the field itself, as Annotated[str | None, Informational], "
            "not a type inside it"
        )
    if _has_discriminator(metadata):
        raise RationalizationError(f"{path}: {_DISCRIMINATED}")
    return inner


def _scalar_spec(annotation: object) -> ScalarSpec | None:
    """The spec of a supported scalar ``annotation`` (module notes, 2), or ``None`` for anything else.

    Types compare by identity, so ``bool`` is never ``int``, and a plain ``datetime`` is refused: only
    ``AwareDatetime`` and ``NaiveDatetime`` say what its values are.
    """
    for scalar in _PLAIN_SCALARS:
        if annotation is scalar:
            return ScalarSpec(base=scalar)
    for zone_type, zone in _DATETIME_ZONES:
        if annotation is zone_type:
            return ScalarSpec(base=datetime, tz=zone)
    if isinstance(annotation, type) and issubclass(annotation, Enum) and not issubclass(annotation, Flag):
        base = _one_kind([member.value for member in annotation], _ENUM_BASES)
        return None if base is None else ScalarSpec(base=base, enum=annotation)
    if get_origin(annotation) is Literal:
        values = get_args(annotation)
        base = _one_kind(values, _LITERAL_BASES)
        return None if base is None else ScalarSpec(base=base, literal=values)
    return None


def _check_enum_values(enum: object, base: type) -> None:
    """Raise unless ``enum`` is a non-Flag Enum class with members whose values are all exactly ``base``."""
    if not isinstance(enum, type) or not issubclass(enum, Enum):
        raise TypeError(f"ScalarSpec enum must be an Enum class, got {_brief(enum)}")
    if issubclass(enum, Flag):
        raise ValueError(f"ScalarSpec enum {enum.__name__} is a Flag, whose values may combine its members")
    if _one_kind([member.value for member in enum], _ENUM_BASES) is not base:
        raise ValueError(
            f"ScalarSpec enum {enum.__name__} needs members whose values are all exactly {base.__name__}, str or int"
        )


def _check_literal_values(values: object, base: type) -> None:
    """Raise unless ``values`` is a non-empty tuple whose values are all exactly ``base`` (str, int or bool)."""
    if not isinstance(values, tuple):
        raise TypeError(f"ScalarSpec literal must be a tuple of values, got {_brief(values)}")
    if _one_kind(values, _LITERAL_BASES) is not base:
        raise ValueError(
            f"ScalarSpec literal {_brief(values)} needs values that are all exactly {base.__name__}, str, int or bool"
        )


def _one_kind(values: Sequence[object], kinds: tuple[type, ...]) -> type | None:
    """The exact type all of ``values`` share when there is a value and that type is one of ``kinds``, else None."""
    found = {type(value) for value in values}
    if len(found) != 1:
        return None
    (kind,) = found
    return kind if any(kind is allowed for allowed in kinds) else None


# ---------------------------------------------------------------------------------------------------------------
# Keys (describe)
# ---------------------------------------------------------------------------------------------------------------


def _key_names(key: object) -> tuple[str, ...] | None:
    """The ``key=`` parameter as distinct field names, or ``None``; anything else raises."""
    if key is None:
        return None
    if isinstance(key, str):
        entries: tuple[object, ...] = (key,)
    elif isinstance(key, Sequence) and not isinstance(key, (bytes, bytearray)):
        entries = tuple(key)
    else:
        raise RationalizationError(
            f"key= must be a field name, a sequence of field names, or None; got {_brief(key)} ({type(key).__name__})"
        )
    if not entries:
        raise RationalizationError("key= names no field: pass a field name, a sequence of field names, or None")
    names = tuple(_key_name(entry) for entry in entries)
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise RationalizationError(f"key= names {name!r} more than once")
        seen.add(name)
    return names


def _key_name(entry: object) -> str:
    """One ``key=`` entry, which must be a non-empty ``str``."""
    if not isinstance(entry, str) or not entry:
        raise RationalizationError(f"key= entries must be non-empty str field names, got {_brief(entry)}")
    return entry


def _model_keys(
    model: type[BaseModel], path: str, explicit: tuple[str, ...] | None, fields: Sequence[FieldSpec]
) -> KeySpec:
    """The keys of ``model`` at ``path``: its ``Key`` marks, or ``explicit`` (the root's ``key=``) agreeing."""
    marked = _marked_keys(model, path)
    namespace = _namespace(path, marked)
    names = _key_fields(model, path, marked, explicit, fields)
    by_name = {field.name: field for field in fields}
    for name in names:
        if by_name[name].informational:
            raise RationalizationError(f"{path}.{name}: a field cannot be both Key and Informational")
        _check_key_field(model.model_fields[name], f"{path}.{name}")
    return KeySpec(fields=names, namespace=namespace)


def _marked_keys(model: type[BaseModel], path: str) -> dict[str, _KeyMarker]:
    """Each field ``Key`` marks, in declaration order, with its marker."""
    marked: dict[str, _KeyMarker] = {}
    for name, info in model.model_fields.items():
        markers = {item for item in info.metadata if isinstance(item, _KeyMarker)}
        if len(markers) > 1:
            named = ", ".join(sorted(repr(marker) for marker in markers))
            raise RationalizationError(f"{path}.{name}: Key names more than one namespace ({named}); name one")
        if markers:
            marked[name] = markers.pop()
    return marked


def _namespace(path: str, marked: Mapping[str, _KeyMarker]) -> str | None:
    """The one namespace the ``marked`` keys of the model at ``path`` share, or ``None``."""
    namespaces = {marker.namespace for marker in marked.values()}
    if len(namespaces) > 1:
        detail = ", ".join(f"{name}: {marker!r}" for name, marker in marked.items())
        raise RationalizationError(
            f"{path}: Key fields mix namespaces ({detail}); mark every key of one model with the same namespace"
        )
    return next(iter(namespaces), None)


def _key_fields(
    model: type[BaseModel],
    path: str,
    marked: Mapping[str, _KeyMarker],
    explicit: tuple[str, ...] | None,
    fields: Sequence[FieldSpec],
) -> tuple[str, ...]:
    """The key field names of ``model`` in declaration order: ``marked``, or ``explicit`` when given."""
    if explicit is None:
        return tuple(marked)
    unknown = [name for name in explicit if name not in model.model_fields]
    if unknown:
        raise RationalizationError(_unknown_key(model, path, unknown[0], fields))
    if marked and set(marked) != set(explicit):
        raise RationalizationError(
            f"{path}: key= names {sorted(explicit)} but Key marks {sorted(marked)}; when both are given they must "
            f"name the same fields"
        )
    return tuple(name for name in model.model_fields if name in explicit)


def _unknown_key(model: type[BaseModel], path: str, name: str, fields: Sequence[FieldSpec]) -> str:
    """The message for a ``key=`` entry ``name`` that is no attribute of ``model``: it may be a field's alias."""
    owner = next((field.name for field in fields if field.wire_name == name), None)
    if owner is not None:
        return (
            f"{path}: key= names {name!r}, the alias of the field {owner!r}; key= takes attribute names, "
            f"as key={owner!r}"
        )
    return (
        f"{path}: key= names {name!r}, which is not a field of {model.__name__} "
        f"(its fields: {_names(model.model_fields)})"
    )


def _check_key_field(info: FieldInfo, path: str) -> None:
    """Raise unless the key field at ``path`` is required, not Optional, and of a key type."""
    if not info.is_required():
        raise RationalizationError(f"{path}: a Key field must be required, but it has a default")
    shape = _shape(info.annotation, path)
    if shape.optional:
        raise RationalizationError(f"{path}: a Key field must not be Optional")
    if shape.kind != "scalar" or shape.scalar is None or not _is_key_type(shape.scalar):
        raise RationalizationError(f"{path}: {_KEY_RULE}, not {_type_name(info.annotation)}")


def _is_key_type(scalar: ScalarSpec) -> bool:
    """Whether ``scalar`` may type a key: any base but ``float`` and ``bool``.

    An Enum (its values str or int) may, and so may a Literal of str or int values; a Literal of bools may not.
    """
    return any(scalar.base is key_type for key_type in _KEY_TYPES)


# ---------------------------------------------------------------------------------------------------------------
# Agreement of the spec and the JSON schema (prepare)
# ---------------------------------------------------------------------------------------------------------------


def _check_agreement(spec: ModelSpec, document: Mapping[str, Any]) -> None:
    """Raise unless every record the JSON schema ``document`` admits is one ``model_validate`` would accept.

    Walks the spec, not the document: every model of the spec is matched with its JSON object, at most one per
    field of the expanded tree, so the walk is bounded by :data:`MAX_FIELDS`.
    """
    defs = document.get("$defs", {})
    if not isinstance(defs, Mapping):
        raise _json_error(spec.model.__name__, DEFS_POINTER, "$defs must map names to schemas")
    stack: list[tuple[ModelSpec, object, str]] = [(spec, document, spec.model.__name__)]
    for _ in range(MAX_FIELDS + 1):
        if not stack:
            return
        model_spec, node, path = stack.pop()
        stack.extend(_agree_model(model_spec, node, path, defs))
    raise RationalizationError(f"{spec.model.__name__}: more than {MAX_FIELDS + 1} models to match")


def _agree_model(
    spec: ModelSpec, node: object, path: str, defs: Mapping[str, Any]
) -> list[tuple[ModelSpec, object, str]]:
    """Match the model at ``path`` with its JSON ``node``; the child models still to match."""
    resolved, _ = _resolve(node, defs, path)
    properties = resolved.get("properties")
    if resolved.get("type") != "object" or not isinstance(properties, Mapping):
        raise RationalizationError(
            f"{path}: its JSON schema is not an object with properties, but {spec.model.__name__} is a model"
        )
    _agree_properties(spec, properties, path)
    _agree_required(spec, resolved.get("required", []), path)
    children: list[tuple[ModelSpec, object, str]] = []
    for field in spec.fields:
        children.extend(_agree_field(field, properties[field.wire_name], f"{path}.{field.name}", defs))
    return children


def _agree_properties(spec: ModelSpec, properties: Mapping[str, Any], path: str) -> None:
    """Raise unless ``properties`` names exactly the wire names of the fields of the model at ``path``."""
    for field in spec.fields:
        if field.wire_name not in properties:
            raise RationalizationError(
                f"{path}.{field.name}: its wire name {field.wire_name!r} is not among the JSON schema's properties "
                f"(SkipJsonSchema, WithJsonSchema or json_schema_extra hides it), so no response could carry it"
            )
    wire_names = {field.wire_name for field in spec.fields}
    for name in properties:
        if name not in wire_names:
            raise RationalizationError(
                f"{path}: the JSON schema has the property {name!r}, which no field of {spec.model.__name__} reads "
                f"(json_schema_extra or WithJsonSchema adds it)"
            )


def _agree_required(spec: ModelSpec, required: object, path: str) -> None:
    """Raise unless ``required`` lists every field ``model_validate`` requires, and names only fields.

    It may require a defaulted field too (as a strict provider style does): that narrows what a response may omit.
    """
    if not isinstance(required, list) or not all(isinstance(name, str) for name in required):
        raise RationalizationError(f"{path}: the JSON schema's 'required' must be a list of property names")
    listed = set(required)
    for field in spec.fields:
        if spec.model.model_fields[field.name].is_required() and field.wire_name not in listed:
            raise RationalizationError(
                f"{path}.{field.name}: the JSON schema does not require it, but model_validate does"
            )
    wire_names = {field.wire_name for field in spec.fields}
    for name in required:
        if name not in wire_names:
            raise RationalizationError(f"{path}: the JSON schema requires {name!r}, which no field reads")


def _agree_field(
    field: FieldSpec, node: object, path: str, defs: Mapping[str, Any]
) -> list[tuple[ModelSpec, object, str]]:
    """Match the field at ``path`` with its JSON property ``node``; the child model still to match, if any."""
    _check_json_values(node, path)
    resolved, nullable = _resolve(node, defs, path)
    if nullable and not field.optional:
        raise RationalizationError(f"{path}: its JSON schema allows null, but the field is not Optional")
    expected = "object" if field.kind == "model" else "array" if field.kind in _LIST_KINDS else "scalar"
    _agree_kind(resolved, expected, path)
    if field.kind == "model" and field.child is not None:
        return [(field.child, resolved, path)]
    if field.kind == "scalar" and field.scalar is not None:
        _agree_scalar(field.scalar, resolved, path)
        return []
    items, item_nullable = _resolve(resolved.get("items"), defs, f"{path}[]")
    if item_nullable:
        raise RationalizationError(f"{path}[]: its JSON schema allows null items, but list items are never None")
    _agree_kind(items, "object" if field.kind == "model_list" else "scalar", f"{path}[]")
    if field.scalar is not None:
        _agree_scalar(field.scalar, items, f"{path}[]")
    return [(field.child, items, f"{path}[]")] if field.child is not None else []


def _agree_scalar(scalar: ScalarSpec, node: Mapping[str, Any], path: str) -> None:
    """Raise unless the resolved JSON ``node`` of the scalar at ``path`` is what pydantic emits for ``scalar``.

    Constraint keywords (``pattern``, ``minimum``, ...) may narrow it; its type, format, and Enum or Literal values
    may not differ.
    """
    if _scalar_agrees(scalar, node):
        return
    found = {keyword: value for keyword, value in node.items() if keyword not in _ANNOTATION_KEYWORDS}
    raise RationalizationError(
        f"{path}: its JSON schema {_json_text(found)} does not match the field's type ({_scalar_name(scalar)}); "
        f"expected {_json_text(_expected_scalar_json(scalar))}"
    )


def _scalar_agrees(scalar: ScalarSpec, node: Mapping[str, Any]) -> bool:
    """Whether ``node`` has the JSON type, format and values pydantic emits for ``scalar``."""
    if scalar.base is Decimal:
        return _decimal_agrees(node)
    expected = _expected_scalar_json(scalar)
    if node.get("type") != expected["type"]:
        return False
    if "format" in expected and node.get("format") != expected["format"]:
        return False
    values = _bounded_values(scalar)
    return values is None or _values_agree(node, values)


def _decimal_agrees(node: Mapping[str, Any]) -> bool:
    """Whether ``node`` is pydantic's Decimal or one of its branches.

    That is an anyOf of a number then a string (and null), nothing beside it; or a lone ``number`` or ``string``
    node, a narrowing ``model_validate`` still accepts.
    """
    if "anyOf" not in node:
        return node.get("type") in _DECIMAL_TYPES
    members = node.get("anyOf")
    if not isinstance(members, list) or any(keyword in node for keyword in _VALUE_KEYWORDS if keyword != "anyOf"):
        return False
    types = [member.get("type") if isinstance(member, Mapping) else None for member in members if not _is_null(member)]
    return types == list(_DECIMAL_TYPES)


def _values_agree(node: Mapping[str, Any], values: Sequence[object]) -> bool:
    """Whether ``node`` bounds a value to exactly ``values``: by ``enum``, or by ``const`` for one value."""
    if "enum" not in node and "const" not in node:
        return False
    if "enum" in node and not _same_values(node["enum"], values):
        return False
    return "const" not in node or _same_values([node["const"]], values)


def _same_values(found: object, expected: Sequence[object]) -> bool:
    """Whether ``found`` is a list of ``expected``'s values in order, each of the same exact type (1 is not true)."""
    return (
        isinstance(found, list)
        and len(found) == len(expected)
        and all(type(one) is type(other) and one == other for one, other in zip(found, expected, strict=True))
    )


def _expected_scalar_json(scalar: ScalarSpec) -> dict[str, Any]:
    """The JSON pydantic 2.14 emits for a scalar of ``scalar``, without annotations or constraints."""
    if scalar.base is Decimal:
        return {"anyOf": [{"type": name} for name in _DECIMAL_TYPES]}
    expected: dict[str, Any] = {"type": _JSON_TYPE_OF[scalar.base]}
    if scalar.base in _JSON_FORMAT_OF:
        expected["format"] = _JSON_FORMAT_OF[scalar.base]
    values = _bounded_values(scalar)
    if values is not None and scalar.literal is not None and len(values) == 1:
        expected["const"] = values[0]
    elif values is not None:
        expected["enum"] = values
    return expected


def _bounded_values(scalar: ScalarSpec) -> list[object] | None:
    """The values an Enum or Literal scalar is bounded to, as pydantic lists them; ``None`` for an unbounded one.

    An Enum's are its ``__members__`` values in order, an alias's value repeated (pydantic 2.14 emits
    ``{"enum": [1, 2, 1]}`` for an alias of the first member).
    """
    if scalar.enum is not None:
        return [member.value for member in scalar.enum.__members__.values()]  # aliases too, as pydantic lists them
    return None if scalar.literal is None else list(scalar.literal)


def _scalar_name(scalar: ScalarSpec) -> str:
    """A readable name for the type ``scalar`` describes, for a message."""
    if scalar.enum is not None:
        return f"Enum {scalar.enum.__name__}"
    if scalar.literal is not None:
        return f"Literal[{', '.join(repr(value) for value in scalar.literal)}]"
    return scalar.base.__name__ if scalar.tz is None else f"{scalar.tz} datetime"


def _agree_kind(node: Mapping[str, Any], expected: str, path: str) -> None:
    """Raise unless the resolved JSON ``node`` describes the ``expected`` kind of value."""
    found = _json_kind(node)
    if found != expected:
        raise RationalizationError(
            f"{path}: its JSON schema describes {_KIND_WORDS[found]}, but the field holds {_KIND_WORDS[expected]}"
        )


def _json_kind(node: Mapping[str, Any]) -> str:
    """What a resolved JSON node describes: ``object``, ``array``, ``scalar``, or ``union`` (an anyOf of shapes)."""
    kind = node.get("type")
    if kind in _CONTAINER_TYPES:
        return str(kind)
    members = node.get("anyOf")
    if isinstance(members, list) and any(
        not isinstance(member, Mapping) or "$ref" in member or member.get("type") in _CONTAINER_TYPES
        for member in members
    ):
        return "union"
    return "scalar"


def _resolve(node: object, defs: Mapping[str, Any], path: str) -> tuple[Mapping[str, Any], bool]:
    """The JSON node that describes a value, through ``$ref`` and Optional's ``anyOf``; and whether null passes."""
    nullable = False
    for _ in range(MAX_DEPTH):
        if not isinstance(node, Mapping):
            raise RationalizationError(f"{path}: its JSON schema is not a schema object")
        if "$ref" in node:
            _check_siblings(node, "$ref", path)
            entry = def_name(node["$ref"])
            if entry is None or entry not in defs:
                raise RationalizationError(f"{path}: its JSON schema's $ref {node['$ref']!r} names no $defs entry")
            node = defs[entry]
            continue
        members = node.get("anyOf")
        if not isinstance(members, list):
            return node, nullable
        others = [member for member in members if not _is_null(member)]
        nullable = nullable or len(others) < len(members)
        if len(others) != 1 or len(members) == 1:
            return node, nullable
        _check_siblings(node, "anyOf", path)
        node = others[0]
    raise RationalizationError(f"{path}: its JSON schema's references run more than {MAX_DEPTH} deep")


def _check_siblings(node: Mapping[str, Any], own: str, path: str) -> None:
    """Raise if ``node`` sets a keyword that shapes a value beside its ``own`` (``$ref``, or Optional's anyOf)."""
    beside = [keyword for keyword in _VALUE_KEYWORDS if keyword != own and keyword in node]
    if beside:
        raise RationalizationError(
            f"{path}: its JSON schema sets {beside} beside {own}, where this check cannot weigh it against what {own} "
            f"describes (json_schema_extra on an Enum, model or Optional field?); drop the override"
        )


def _check_json_values(node: object, path: str) -> None:
    """Raise if the JSON property ``node`` of the field at ``path`` has a default or examples that are not JSON."""
    if not isinstance(node, Mapping):
        return
    for keyword in ("default", "examples"):
        if keyword not in node:
            continue
        try:
            json.dumps(node[keyword], allow_nan=False)
        except (TypeError, ValueError, RecursionError) as error:
            raise RationalizationError(
                f"{path}: its {keyword} {_brief(node[keyword])} cannot be sent as JSON (nan and inf have no JSON form)"
            ) from error


def _is_null(member: object) -> bool:
    """Whether the anyOf ``member`` is exactly ``{"type": "null"}``."""
    return isinstance(member, Mapping) and dict(member) == {"type": "null"}


# ---------------------------------------------------------------------------------------------------------------
# JSON schema (check_json_schema)
# ---------------------------------------------------------------------------------------------------------------


def _json_schema(model: type[BaseModel]) -> dict[str, Any]:
    """A deep copy of pydantic's validation JSON schema for ``model``, sharing nothing with pydantic or the model.

    Pydantic 2.14 builds a fresh structure per call, its ``json_schema_extra`` values copied; the deep copy keeps
    that true whatever a pydantic release does, since callers may transform what they are given.
    """
    try:
        document = model.model_json_schema()
    except _PYDANTIC_SCHEMA_ERRORS as error:
        raise RationalizationError(
            f"{model.__name__}: pydantic cannot build its JSON schema: {type(error).__name__}: {error}"
        ) from error
    try:
        return copy.deepcopy(document)
    except RecursionError as error:
        raise RationalizationError(f"{model.__name__}: its JSON schema is nested too deeply to copy") from error


def _check_document(document: Mapping[str, Any], name: str) -> None:
    """Raise unless the JSON schema ``document`` of the model ``name`` stays in the supported subset."""
    defs = document.get("$defs", {})
    if not isinstance(defs, Mapping):
        raise _json_error(name, DEFS_POINTER, "$defs must map names to schemas")
    reached = _walk(document, defs, name)
    for entry in defs:
        if entry not in reached:
            raise _json_error(name, json_pointer(DEFS_POINTER, entry), "this $defs entry is not referenced")
    if document.get("type") != "object":
        raise _json_error(name, "#", "the root schema must be an object")


def _walk(document: Mapping[str, Any], defs: Mapping[str, Any], name: str) -> set[str]:
    """Visit every node of ``document``, following each ``$ref``; the ``$defs`` entries reached."""
    reached: set[str] = set()
    stack = [_Node(schema=document, pointer="#", depth=0, refs=frozenset())]
    for _ in range(MAX_WALK_STEPS):
        if not stack:
            break
        children = _visit(stack.pop(), defs, name)
        for child in children:
            reached |= child.refs
        stack.extend(reversed(children))
    if stack:
        raise RationalizationError(f"{name} JSON schema: more than {MAX_WALK_STEPS} nodes to walk")
    return reached


def _visit(node: _Node, defs: Mapping[str, Any], name: str) -> list[_Node]:
    """Check one node; the nodes under it, its ``$ref`` target last."""
    schema = node.schema
    if not isinstance(schema, Mapping):
        raise _json_error(name, node.pointer, "not a schema object (a boolean schema or a bare value)")
    _check_keywords(schema, node.pointer, name)
    depth = node.depth + (1 if schema.get("type") in _CONTAINER_TYPES else 0)
    if depth > MAX_DEPTH:
        raise _json_error(name, node.pointer, f"nesting is deeper than {MAX_DEPTH} objects and arrays")
    children = _subschemas(schema, node.pointer, depth, node.refs, name)
    if "$ref" in schema:
        children.append(_follow(schema["$ref"], node, depth, defs, name))
    return children


def _check_keywords(schema: Mapping[str, Any], pointer: str, name: str) -> None:
    """Raise if the node at ``pointer`` uses a refused keyword or form, or constrains nothing."""
    for keyword in schema:
        if keyword in UNREAD_KEYWORDS:  # they combine or condition schemas, or hide subschemas or $ref targets
            raise _json_error(name, pointer, f"{keyword!r} is not supported")
    if "$defs" in schema and pointer != "#":
        raise _json_error(name, pointer, "$defs is only supported at the root")
    if schema.get("additionalProperties", False) is not False:
        raise _json_error(
            name, pointer, "additionalProperties must be absent or false: a dict field or extra='allow' is unbounded"
        )
    description = schema.get("description")
    if isinstance(description, str) and (found := _R8_ESCAPED.search(description)) is not None:
        raise _json_error(
            name,
            pointer,
            f"description holds U+{ord(found.group(0)[0]):04X}, a code point R8 escapes: "
            "remove it from the description",
        )
    _check_type(schema, pointer, name)
    if "anyOf" in schema:
        _check_any_of(schema["anyOf"], pointer, name)
    if "enum" in schema and (not isinstance(schema["enum"], list) or not schema["enum"]):
        raise _json_error(name, pointer, "enum must be a non-empty list")
    if not any(keyword in schema for keyword in _TYPED_KEYWORDS):
        raise _json_error(name, pointer, "an unconstrained schema: it has no type, $ref, anyOf, enum or const")


def _check_any_of(members: object, pointer: str, name: str) -> None:
    """Raise unless ``members`` is Optional's ``[X, null]`` or pydantic's Decimal (``number``, ``string``[, null])."""
    if not isinstance(members, list) or not members:
        raise _json_error(name, pointer, "anyOf must be a non-empty list of schemas")
    nulls = [_is_null(member) for member in members]
    if nulls == [False, True]:
        return
    types = [member.get("type") if isinstance(member, Mapping) else None for member in members]
    if types[:2] == ["number", "string"] and nulls[:2] == [False, False] and nulls[2:] in ([], [True]):
        return
    raise _json_error(
        name,
        pointer,
        "anyOf must be X | None ([X, {'type': 'null'}]) or pydantic's Decimal (a number, then a string, then null "
        "when Optional)",
    )


def _check_type(schema: Mapping[str, Any], pointer: str, name: str) -> None:
    """Raise if the node's ``type`` is not one JSON type name, or names an object or array that is unconstrained."""
    if "type" not in schema:
        return
    kind = schema["type"]
    if not isinstance(kind, str) or kind not in _JSON_TYPES:
        raise _json_error(name, pointer, f"'type' must be one of {', '.join(sorted(_JSON_TYPES))}, got {kind!r}")
    if kind == "object" and not schema.get("properties"):
        raise _json_error(name, pointer, "an object without properties is unconstrained")
    if kind == "array" and "items" not in schema:
        raise _json_error(name, pointer, "an array without items is unconstrained")


def _subschemas(schema: Mapping[str, Any], pointer: str, depth: int, refs: frozenset[str], name: str) -> list[_Node]:
    """The nodes under ``schema``: its properties, its items, then its anyOf members."""
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        raise _json_error(name, pointer, "'properties' must map names to schemas")
    children = [
        _Node(schema=value, pointer=json_pointer(f"{pointer}/properties", str(key)), depth=depth, refs=refs)
        for key, value in properties.items()
    ]
    if "items" in schema:
        if not isinstance(schema["items"], Mapping):
            raise _json_error(name, pointer, "'items' must be one schema")
        children.append(_Node(schema=schema["items"], pointer=f"{pointer}/items", depth=depth, refs=refs))
    children.extend(
        _Node(schema=member, pointer=f"{pointer}/anyOf/{index}", depth=depth, refs=refs)
        for index, member in enumerate(schema.get("anyOf", ()))
    )
    return children


def _follow(ref: object, node: _Node, depth: int, defs: Mapping[str, Any], name: str) -> _Node:
    """The ``$defs`` entry the ``ref`` of ``node`` names; any other reference, or a cycle, raises."""
    entry = def_name(ref)
    if entry is None:
        raise _json_error(
            name, node.pointer, f"$ref {ref!r} is not supported: only local {DEFS_PREFIX}<name> references are"
        )
    if entry not in defs:
        raise _json_error(name, node.pointer, f"$ref {ref!r} names no $defs entry")
    if entry in node.refs:
        raise _json_error(
            name, node.pointer, f"$ref cycle: {ref!r} refers back to a schema that contains it (a recursive model)"
        )
    return _Node(schema=defs[entry], pointer=json_pointer(DEFS_POINTER, entry), depth=depth, refs=node.refs | {entry})


def _json_error(name: str, pointer: str, reason: str) -> RationalizationError:
    """The error for the JSON schema of the model ``name`` at ``pointer``."""
    return RationalizationError(f"{name} JSON schema at {pointer}: {reason}")


# ---------------------------------------------------------------------------------------------------------------
# Wrapper and hash
# ---------------------------------------------------------------------------------------------------------------


def _wrap(document: Mapping[str, Any]) -> dict[str, Any]:
    """The response wrapper around a fresh ``document`` (plan §0 correction 2), its ``$defs`` at the root."""
    item = {
        "type": "object",
        "properties": {
            "record": {keyword: value for keyword, value in document.items() if keyword != "$defs"},
            "grounding_quotes": {"type": "array", "items": {"type": "string"}},
            "source_element_ids": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["record", "grounding_quotes", "source_element_ids"],
        "additionalProperties": False,
    }
    wrapper: dict[str, Any] = {
        "type": "object",
        "properties": {"records": {"type": "array", "items": item}},
        "required": ["records"],
        "additionalProperties": False,
    }
    if "$defs" in document:
        wrapper["$defs"] = document["$defs"]
    return wrapper


def _canonical_json(wrapper: object) -> str:
    """``wrapper``, which must be a mapping, as canonical JSON text (``_canonical.canonical_json``)."""
    if not isinstance(wrapper, Mapping):
        raise RationalizationError(f"the response schema must be a mapping, got {type(wrapper).__name__}")
    return canonical_json(wrapper, subject="the response schema")


# ---------------------------------------------------------------------------------------------------------------
# Values (value_problems)
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Visit:
    """One value to check: its field, its path, and whether a default gave it (``None``: not told)."""

    field: FieldSpec
    value: object
    path: str
    defaulted: bool | None


def _scalar_ok(scalar: ScalarSpec, value: object) -> bool:
    """Whether ``value`` is a finite value admitted by ``scalar`` (fast zero-allocation check)."""
    if scalar.enum is not None:
        return type(value) is scalar.enum
    kind = type(value)
    base = scalar.base
    if kind is base:
        if scalar._literal_set is not None:
            return (kind, value) in scalar._literal_set
        if kind in _FAST_EXACT_BASES:
            return True
        if isinstance(value, float):
            return math.isfinite(value)
        if isinstance(value, Decimal):
            return value.is_finite()
        if isinstance(value, datetime):
            return (value.utcoffset() is not None) == (scalar.tz == "aware")
        return False
    if kind is int and isinstance(value, int):
        if base is float:
            return abs(value) <= FLOAT_INT_LIMIT
        return base is Decimal
    return False


def _scalar_list_ok(scalar: ScalarSpec, value: object, steps: int) -> int:
    """Verify ``value`` is a ``list`` of admitted scalars within ``MAX_WALK_STEPS``; return updated ``steps`` or -1."""
    if not isinstance(value, list) or type(value) is not list:
        return -1
    steps += len(value)
    if steps > MAX_WALK_STEPS:
        return -1
    for item in value:
        if item is None or not _scalar_ok(scalar, item):
            return -1
    return steps


def _model_list_ok(
    child_spec: ModelSpec,
    value: object,
    child_depth: int,
    steps: int,
    stack: list[tuple[BaseModel, ModelSpec, int]],
) -> int:
    """Verify ``value`` is a ``list`` of exact ``child_spec.model`` instances and push them onto ``stack``."""
    if child_depth > MAX_DEPTH or not isinstance(value, list) or type(value) is not list:
        return -1
    steps += len(value)
    if steps > MAX_WALK_STEPS:
        return -1
    expected_model = child_spec.model
    for item in reversed(value):
        if not isinstance(item, BaseModel) or type(item) is not expected_model:
            return -1
        stack.append((item, child_spec, child_depth))
    return steps


def _field_ok(
    node: BaseModel,
    field_spec: FieldSpec,
    depth: int,
    steps: int,
    stack: list[tuple[BaseModel, ModelSpec, int]],
) -> int:
    """Check one field of ``node`` without path allocations; return updated ``steps`` or -1 on any problem."""
    value = getattr(node, field_spec.name, _MISSING)
    if value is _MISSING:
        return -1
    kind = field_spec.kind
    if kind == "scalar":
        if value is None:
            return steps if field_spec.optional else -1
        return steps if field_spec.scalar is not None and _scalar_ok(field_spec.scalar, value) else -1
    if kind == "scalar_list":
        if depth + 1 > MAX_DEPTH or field_spec.scalar is None:
            return -1
        return _scalar_list_ok(field_spec.scalar, value, steps)
    if kind == "model":
        if value is None:
            return steps if field_spec.optional else -1
        child = field_spec.child
        if depth + 1 > MAX_DEPTH or child is None:
            return -1
        if not isinstance(value, BaseModel) or type(value) is not child.model or steps + 1 > MAX_WALK_STEPS:
            return -1
        stack.append((value, child, depth + 1))
        return steps + 1
    if field_spec.child is None:
        return -1
    return _model_list_ok(field_spec.child, value, depth + 2, steps, stack)


def _instance_ok(instance: object, spec: ModelSpec) -> bool:
    """Fast zero-path-allocation check whether ``instance`` satisfies ``spec`` within depth and step bounds."""
    if not isinstance(spec, ModelSpec) or not isinstance(instance, BaseModel) or type(instance) is not spec.model:
        return False
    stack: list[tuple[BaseModel, ModelSpec, int]] = [(instance, spec, 1)]
    steps = 1
    for _ in range(MAX_WALK_STEPS + 1):
        if not stack:
            return True
        node, node_spec, depth = stack.pop()
        if depth > MAX_DEPTH or getattr(node, "__pydantic_extra__", None):
            return False
        steps += len(node_spec.fields)
        if steps > MAX_WALK_STEPS:
            return False
        for field_spec in node_spec.fields:
            steps = _field_ok(node, field_spec, depth, steps, stack)
            if steps < 0:
                return False
    return False


def _walk_values(stack: list[_Visit]) -> list[str]:
    """The problems of every value on ``stack`` (popped last first), and of what they hold."""
    problems: list[str] = []
    while stack:
        visit = stack.pop()
        problems.extend(_marked(problem, visit.defaulted) for problem in _value_problems(visit, stack))
    return problems


def _marked(problem: str, defaulted: bool | None) -> str:
    """``problem``, with :data:`DEFAULT_HINT` when a default gave the value (so the fix is the field's default)."""
    return f"{problem}{DEFAULT_HINT}" if defaulted else problem


def _value_problems(visit: _Visit, stack: list[_Visit]) -> list[str]:
    """What is wrong with one field's value; the fields of each model instance it holds go on ``stack``."""
    field_spec, value, path = visit.field, visit.value, visit.path
    if value is _MISSING:
        return [_at(path, "is missing (the instance was built without it, as model_construct() can)")]
    if field_spec.child is None and field_spec.kind not in _LIST_KINDS:
        if value is None:
            return [] if field_spec.optional else [_at(path, "holds None, but the field is not Optional")]
        problem = _item_problem(field_spec, value)
        return [] if problem is None else [_at(path, problem)]
    if field_spec.child is None:
        if type(value) is not list:
            return [_at(path, f"holds {_type_of(value)}, but the field holds a list")]
        return [
            _at(f"{path}[{index}]", problem)
            for index, item in enumerate(value)
            if (problem := _item_problem(field_spec, item)) is not None
        ]
    if field_spec.kind in _LIST_KINDS:
        if type(value) is not list:
            return [_at(path, f"holds {_type_of(value)}, but the field holds a list")]
        items = [(f"{path}[{index}]", item) for index, item in enumerate(value)]
    elif value is None:
        return [] if field_spec.optional else [_at(path, "holds None, but the field is not Optional")]
    else:
        items = [(path, value)]
    problems: list[str] = []
    children: list[_Visit] = []
    for item_path, item in items:
        problem = _item_problem(field_spec, item)
        if problem is not None:
            problems.append(_at(item_path, problem))
        elif field_spec.child is not None:
            children.extend(_child_visits(field_spec.child, item, item_path, visit.defaulted))
    stack.extend(reversed(children))  # popped in field order
    return problems


def _child_visits(spec: ModelSpec, item: object, path: str, defaulted: bool | None) -> list[_Visit]:
    """The fields of a model instance to check, in field order.

    A field the input did not set holds its default; a field the instance lacks holds nothing, so only the default
    that gave the instance (``defaulted``) can be at fault for it.
    """
    given: set[str] | frozenset[str] | None = None
    visits: list[_Visit] = []
    for child in spec.fields:
        value = getattr(item, child.name, _MISSING)
        if child.child is None and _scalar_field_ok(child, value):
            continue
        if given is None and defaulted is not None:
            given = _fields_set(item)
        own_default = value is not _MISSING and given is not None and child.name not in given
        child_path = f"{path}.{child.name}" if path else child.name
        visits.append(_Visit(child, value, child_path, None if defaulted is None else defaulted or own_default))
    return visits


def _scalar_field_ok(field_spec: FieldSpec, value: object) -> bool:
    """Whether ``value`` is valid for a scalar or scalar_list field (``field_spec.child is None``)."""
    if value is _MISSING:
        return False
    if field_spec.kind not in _LIST_KINDS:
        return field_spec.optional if value is None else _item_problem(field_spec, value) is None
    return type(value) is list and all(_item_problem(field_spec, each) is None for each in value)


_EMPTY_FIELDS: frozenset[str] = frozenset()


def _fields_set(instance: object) -> set[str] | frozenset[str]:
    """The fields validation (or ``model_construct``) set on ``instance``: the others hold their defaults."""
    found = getattr(instance, "__pydantic_fields_set__", None)
    return found if isinstance(found, set) else _EMPTY_FIELDS


def _item_problem(field_spec: FieldSpec, value: object) -> str | None:
    """What is wrong with one value (not ``None``, not a list) of a field, else ``None``."""
    if field_spec.child is not None:
        model = field_spec.child.model
        return None if type(value) is model else f"holds {_type_of(value)}, but the field holds {model.__name__}"
    scalar = field_spec.scalar
    if scalar is None:
        return None
    if (
        type(value) is scalar.base
        and scalar.enum is None
        and scalar.literal is None
        and type(value) in (str, int, bool, date)
    ):
        return None
    if not _finite(value):
        return f"holds a non-finite {_type_of(value)}"
    if admits_scalar(scalar, value):
        return None
    return f"holds {_type_of(value)}, but the field holds {_held_text(scalar)}"


def _finite(value: object) -> bool:
    """Whether ``value`` is not a non-finite ``float`` or ``Decimal`` (NaN, an infinity): lax validation takes
    ``"Infinity"`` and ``"NaN"`` for a float, and neither has a JSON form."""
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is Decimal:
        return value.is_finite()
    return True


def _held_text(scalar: ScalarSpec) -> str:
    """What a scalar field holds, for a message: an int widening to it named too."""
    if scalar.enum is not None:
        return scalar.enum.__name__
    if scalar.base is float:
        return "float (or an int within 2**53 of 0)"
    if scalar.base is Decimal:
        return "Decimal (or an int)"
    if scalar.literal is not None:
        return f"Literal[{', '.join(repr(item) for item in scalar.literal)}]"
    if scalar.tz == "aware":
        return "an aware datetime"
    if scalar.tz == "naive":
        return "a naive datetime"
    return scalar.base.__name__


def _type_of(value: object) -> str:
    """The name of ``value``'s type (``None`` for ``None``), for a message that never shows the value."""
    return "None" if value is None else type(value).__name__


def _at(path: str, problem: str) -> str:
    """``problem`` at ``path`` (``"path: problem"``), or alone for the empty path."""
    return f"{path}: {problem}" if path else problem


# ---------------------------------------------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------------------------------------------


def _brief(value: object) -> str:
    """``repr(value)``, cut to :data:`_BRIEF_CHARS` characters, for a message."""
    text = repr(value)
    return text if len(text) <= _BRIEF_CHARS else f"{text[: _BRIEF_CHARS - 3]}..."


def _json_text(value: object) -> str:
    """``value`` as JSON with sorted keys, cut to :data:`_JSON_SHOWN` characters, for a message (else its repr)."""
    try:
        text = json.dumps(value, sort_keys=True)
    except (TypeError, ValueError, RecursionError):
        return _brief(value)
    return text if len(text) <= _JSON_SHOWN else f"{text[: _JSON_SHOWN - 3]}..."


def _type_name(annotation: object) -> str:
    """A readable name for ``annotation``: a plain class's name, else its repr without ``typing.``."""
    if isinstance(annotation, type) and not get_args(annotation):
        return annotation.__name__
    return repr(annotation).replace("typing.", "")


def _names(names: Iterable[str]) -> str:
    """The first :data:`_NAMES_SHOWN` of ``names``, comma-separated, with an ellipsis when there are more."""
    listed = list(names)
    shown = ", ".join(listed[:_NAMES_SHOWN])
    return shown if len(listed) <= _NAMES_SHOWN else f"{shown}, ..."
