"""Tests for ``datagrunt.rationalization.schema``: which caller schemas rationalize() accepts, and what it derives.

A schema is checked at three levels, and each rejection must name what is at fault: the model classes
(``check_schema``), the annotations and keys (``describe``, naming the dotted field path), and pydantic's JSON
schema (``check_json_schema``, naming the JSON pointer). ``prepare`` runs them in that order, then builds the
response wrapper and its hash.
"""

import ast
import dataclasses
import hashlib
import json
import os
import re
import subprocess
import sys
import textwrap
import time
import typing
from collections.abc import Callable
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum, Flag, IntEnum, IntFlag, StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal, NewType

import pytest
from pydantic import (
    AfterValidator,
    AliasChoices,
    AliasPath,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    NaiveDatetime,
    NonNegativeInt,
    PositiveInt,
    RootModel,
    StringConstraints,
    WithJsonSchema,
    create_model,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel
from pydantic.json_schema import SkipJsonSchema

from datagrunt.rationalization import Key, RationalizationError
from datagrunt.rationalization import schema as schema_module
from datagrunt.rationalization.schema import (
    MAX_DEPTH,
    MAX_FIELDS,
    MAX_WALK_STEPS,
    FieldSpec,
    KeySpec,
    ModelSpec,
    PreparedSchema,
    ScalarSpec,
    check_json_schema,
    check_schema,
    describe,
    instance_problems,
    prepare,
    response_schema,
    schema_sha256,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TIMEOUT_S = 120
# A rejection walks a handful of classes or schema nodes; a hang or an exponential walk takes far longer.
FAST_S = 5.0


def raises_at(path: str):
    """``pytest.raises`` for a RationalizationError whose message starts with the dotted field ``path``."""
    return pytest.raises(RationalizationError, match=rf"^{re.escape(path)}: ")


def raises_json(model: str, pointer: str):
    """``pytest.raises`` for a RationalizationError naming ``model`` and the JSON ``pointer`` at fault."""
    return pytest.raises(RationalizationError, match=rf"^{re.escape(model)} JSON schema at {re.escape(pointer)}: ")


# ---------------------------------------------------------------------------------------------------------------
# Models (module level, so a fresh process can import them by name)
# ---------------------------------------------------------------------------------------------------------------


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Level(IntEnum):
    LOW = 1
    HIGH = 2


class MixedValues(Enum):
    TEXT = "a"
    NUMBER = 1


class NoMembers(Enum):
    pass


class Shade(StrEnum):
    LIGHT = "light"
    DARK = "dark"


class Answer(Enum):
    YES = True
    NO = False


class Ratio(Enum):
    HALF = 0.5
    WHOLE = 1.0


class Legacy(IntEnum):
    NEW = 1
    OLD = 2
    ORIGINAL = 1  # an alias of NEW: pydantic lists its value again


class Bit(IntEnum):
    ON = 1
    OFF = 0


class Perm(Flag):
    READ = 1
    WRITE = 2


class IPerm(IntFlag):
    READ = 1
    WRITE = 2


class Part(BaseModel):
    number: Annotated[str, Key("Part")]
    name: str | None = None


class Line(BaseModel):
    line_no: Annotated[int, Key]
    part: Part
    quantity: Decimal


class Order(BaseModel):
    order_id: Annotated[str, Key]
    placed: date
    lines: list[Line]
    tags: list[str] = []


PART_SPEC = ModelSpec(
    model=Part,
    keys=KeySpec(fields=("number",), namespace="part"),
    fields=(
        FieldSpec(
            name="number",
            wire_name="number",
            kind="scalar",
            optional=False,
            required=True,
            child=None,
            scalar=ScalarSpec(base=str),
        ),
        FieldSpec(
            name="name",
            wire_name="name",
            kind="scalar",
            optional=True,
            required=False,
            child=None,
            scalar=ScalarSpec(base=str),
        ),
    ),
)
LINE_SPEC = ModelSpec(
    model=Line,
    keys=KeySpec(fields=("line_no",), namespace=None),
    fields=(
        FieldSpec(
            name="line_no",
            wire_name="line_no",
            kind="scalar",
            optional=False,
            required=True,
            child=None,
            scalar=ScalarSpec(base=int),
        ),
        FieldSpec(
            name="part", wire_name="part", kind="model", optional=False, required=True, child=PART_SPEC, scalar=None
        ),
        FieldSpec(
            name="quantity",
            wire_name="quantity",
            kind="scalar",
            optional=False,
            required=True,
            child=None,
            scalar=ScalarSpec(base=Decimal),
        ),
    ),
)
ORDER_SPEC = ModelSpec(
    model=Order,
    keys=KeySpec(fields=("order_id",), namespace=None),
    fields=(
        FieldSpec(
            name="order_id",
            wire_name="order_id",
            kind="scalar",
            optional=False,
            required=True,
            child=None,
            scalar=ScalarSpec(base=str),
        ),
        FieldSpec(
            name="placed",
            wire_name="placed",
            kind="scalar",
            optional=False,
            required=True,
            child=None,
            scalar=ScalarSpec(base=date),
        ),
        FieldSpec(
            name="lines",
            wire_name="lines",
            kind="model_list",
            optional=False,
            required=True,
            child=LINE_SPEC,
            scalar=None,
        ),
        FieldSpec(
            name="tags",
            wire_name="tags",
            kind="scalar_list",
            optional=False,
            required=False,
            child=None,
            scalar=ScalarSpec(base=str),
        ),
    ),
)


class EveryForm(BaseModel):
    text: str
    count: int
    ratio: float
    flag: bool
    amount: Decimal
    day: date
    at: AwareDatetime
    color: Color
    level: Level
    letter: Literal["a", "b"]
    number: Literal[1, 2]
    truth: Literal[True]
    constrained: Annotated[str, Field(min_length=1, max_length=8, pattern=r"^\w+$")]
    maybe_text: str | None = None
    maybe_amount: Decimal | None = None
    maybe_color: Color | None = None
    maybe_letter: Literal["a"] | None = None
    maybe_part: Part | None = None
    texts: list[str] = []
    amounts: list[Decimal] = []
    colors: list[Color] = []
    letters: list[Literal["x", "y"]] = []
    parts: list[Part] = []
    part: Part
    described_part: Part = Field(description="the part this form describes")


EVERY_FORM_SHAPES = {
    "text": ("scalar", False),
    "count": ("scalar", False),
    "ratio": ("scalar", False),
    "flag": ("scalar", False),
    "amount": ("scalar", False),
    "day": ("scalar", False),
    "at": ("scalar", False),
    "color": ("scalar", False),
    "level": ("scalar", False),
    "letter": ("scalar", False),
    "number": ("scalar", False),
    "truth": ("scalar", False),
    "constrained": ("scalar", False),
    "maybe_text": ("scalar", True),
    "maybe_amount": ("scalar", True),
    "maybe_color": ("scalar", True),
    "maybe_letter": ("scalar", True),
    "maybe_part": ("model", True),
    "texts": ("scalar_list", False),
    "amounts": ("scalar_list", False),
    "colors": ("scalar_list", False),
    "letters": ("scalar_list", False),
    "parts": ("model_list", False),
    "part": ("model", False),
    "described_part": ("model", False),
}

# Each accepted scalar annotation and the ScalarSpec describe() gives it: bool is not int, datetime is not date,
# an Enum's base is its values' type, a Literal's base is its values' type and its values keep their order.
SCALAR_FORMS = {
    "str": (str, ScalarSpec(base=str)),
    "int": (int, ScalarSpec(base=int)),
    "float": (float, ScalarSpec(base=float)),
    "bool": (bool, ScalarSpec(base=bool)),
    "Decimal": (Decimal, ScalarSpec(base=Decimal)),
    "date": (date, ScalarSpec(base=date)),
    "AwareDatetime": (AwareDatetime, ScalarSpec(base=datetime, tz="aware")),
    "NaiveDatetime": (NaiveDatetime, ScalarSpec(base=datetime, tz="naive")),
    "str-Enum": (Color, ScalarSpec(base=str, enum=Color)),
    "StrEnum": (Shade, ScalarSpec(base=str, enum=Shade)),
    "IntEnum": (Level, ScalarSpec(base=int, enum=Level)),
    "aliased-IntEnum": (Legacy, ScalarSpec(base=int, enum=Legacy)),
    "str-Literal": (Literal["b", "a"], ScalarSpec(base=str, literal=("b", "a"))),
    "int-Literal": (Literal[2, 1], ScalarSpec(base=int, literal=(2, 1))),
    "bool-Literal": (Literal[False, True], ScalarSpec(base=bool, literal=(False, True))),
    "constrained-str": (Annotated[str, Field(min_length=1)], ScalarSpec(base=str)),
    "constrained-int": (Annotated[int, Field(ge=0)], ScalarSpec(base=int)),
}
# Each way a field holds a scalar: its pydantic field definition for an annotation, and the FieldSpec kind and
# optional flag describe() gives it.
SCALAR_HOLDERS = {
    "plain": (lambda annotation: (annotation, ...), "scalar", False, True),
    "optional": (lambda annotation: (annotation | None, None), "scalar", True, False),
    "list": (lambda annotation: (list[annotation], []), "scalar_list", False, False),
}

# What pydantic 2.14 emits for each scalar form, its $ref followed and its title dropped: what the agreement step
# requires of each scalar field's JSON (probed 2026-11-04). A one-member Enum still emits an enum list, a
# one-value Literal a const; aware and naive datetimes emit the same format.
PINNED_SCALAR_JSON = {
    "str": (str, {"type": "string"}),
    "int": (int, {"type": "integer"}),
    "float": (float, {"type": "number"}),
    "bool": (bool, {"type": "boolean"}),
    "Decimal": (Decimal, {"anyOf": [{"type": "number"}, {"type": "string"}]}),
    "date": (date, {"format": "date", "type": "string"}),
    "AwareDatetime": (AwareDatetime, {"format": "date-time", "type": "string"}),
    "NaiveDatetime": (NaiveDatetime, {"format": "date-time", "type": "string"}),
    "str-Enum": (Color, {"enum": ["red", "blue"], "type": "string"}),
    "StrEnum": (Shade, {"enum": ["light", "dark"], "type": "string"}),
    "IntEnum": (Level, {"enum": [1, 2], "type": "integer"}),
    "aliased-IntEnum": (Legacy, {"enum": [1, 2, 1], "type": "integer"}),
    "str-Literal": (Literal["b", "a"], {"enum": ["b", "a"], "type": "string"}),
    "one-str-Literal": (Literal["a"], {"const": "a", "type": "string"}),
    "int-Literal": (Literal[2, 1], {"enum": [2, 1], "type": "integer"}),
    "one-bool-Literal": (Literal[True], {"const": True, "type": "boolean"}),
    "bool-Literal": (Literal[False, True], {"enum": [False, True], "type": "boolean"}),
}


def overridden(annotation: object, schema: dict) -> tuple[object, object]:
    """A required field definition whose JSON schema WithJsonSchema replaces with ``schema``."""
    return (Annotated[annotation, WithJsonSchema(schema)], ...)


# Scalar fields whose JSON schema an override makes disagree with the field's type: each field definition, the
# path the message names, the JSON the field's type needs, and the JSON found (annotations dropped).
SCALAR_OVERRIDES = {
    "int-as-string-enum": (
        overridden(int, {"type": "string", "enum": ["a"]}),
        "Overridden.value",
        {"type": "integer"},
        {"enum": ["a"], "type": "string"},
    ),
    "int-as-number": (overridden(int, {"type": "number"}), "Overridden.value", {"type": "integer"}, {"type": "number"}),
    "str-as-integer": (
        overridden(str, {"type": "integer"}),
        "Overridden.value",
        {"type": "string"},
        {"type": "integer"},
    ),
    "float-as-integer": (
        overridden(float, {"type": "integer"}),
        "Overridden.value",
        {"type": "number"},
        {"type": "integer"},
    ),
    "bool-as-string": (
        overridden(bool, {"type": "string"}),
        "Overridden.value",
        {"type": "boolean"},
        {"type": "string"},
    ),
    "Decimal-as-boolean": (
        overridden(Decimal, {"type": "boolean"}),
        "Overridden.value",
        {"anyOf": [{"type": "number"}, {"type": "string"}]},
        {"type": "boolean"},
    ),
    "Decimal-pair-reversed": (
        overridden(Decimal, {"anyOf": [{"type": "string"}, {"type": "number"}]}),
        "Overridden.value",
        {"anyOf": [{"type": "number"}, {"type": "string"}]},
        {"anyOf": [{"type": "string"}, {"type": "number"}]},
    ),
    "Decimal-pair-as-integer": (
        overridden(Decimal, {"anyOf": [{"type": "integer"}, {"type": "string"}]}),
        "Overridden.value",
        {"anyOf": [{"type": "number"}, {"type": "string"}]},
        {"anyOf": [{"type": "integer"}, {"type": "string"}]},
    ),
    "Decimal-pair-typed-string": (
        overridden(Decimal, {"anyOf": [{"type": "number"}, {"type": "string"}], "type": "string"}),
        "Overridden.value",
        {"anyOf": [{"type": "number"}, {"type": "string"}]},
        {"anyOf": [{"type": "number"}, {"type": "string"}], "type": "string"},
    ),
    "date-without-format": (
        overridden(date, {"type": "string"}),
        "Overridden.value",
        {"format": "date", "type": "string"},
        {"type": "string"},
    ),
    "date-as-date-time": (
        overridden(date, {"type": "string", "format": "date-time"}),
        "Overridden.value",
        {"format": "date", "type": "string"},
        {"format": "date-time", "type": "string"},
    ),
    "aware-as-date": (
        overridden(AwareDatetime, {"type": "string", "format": "date"}),
        "Overridden.value",
        {"format": "date-time", "type": "string"},
        {"format": "date", "type": "string"},
    ),
    "naive-without-format": (
        overridden(NaiveDatetime, {"type": "string"}),
        "Overridden.value",
        {"format": "date-time", "type": "string"},
        {"type": "string"},
    ),
    "Enum-missing-a-member": (
        overridden(Color, {"type": "string", "enum": ["red"]}),
        "Overridden.value",
        {"enum": ["red", "blue"], "type": "string"},
        {"enum": ["red"], "type": "string"},
    ),
    "Enum-reordered": (
        overridden(Color, {"type": "string", "enum": ["blue", "red"]}),
        "Overridden.value",
        {"enum": ["red", "blue"], "type": "string"},
        {"enum": ["blue", "red"], "type": "string"},
    ),
    "Enum-unbounded": (
        overridden(Color, {"type": "string"}),
        "Overridden.value",
        {"enum": ["red", "blue"], "type": "string"},
        {"type": "string"},
    ),
    "IntEnum-as-string": (
        overridden(Level, {"type": "string", "enum": [1, 2]}),
        "Overridden.value",
        {"enum": [1, 2], "type": "integer"},
        {"enum": [1, 2], "type": "string"},
    ),
    "IntEnum-as-bools": (
        overridden(Bit, {"type": "integer", "enum": [True, False]}),
        "Overridden.value",
        {"enum": [1, 0], "type": "integer"},
        {"enum": [True, False], "type": "integer"},
    ),
    "Literal-extra-value": (
        overridden(Literal["a", "b"], {"type": "string", "enum": ["a", "b", "c"]}),
        "Overridden.value",
        {"enum": ["a", "b"], "type": "string"},
        {"enum": ["a", "b", "c"], "type": "string"},
    ),
    "Literal-other-const": (
        overridden(Literal["a"], {"type": "string", "const": "b"}),
        "Overridden.value",
        {"const": "a", "type": "string"},
        {"const": "b", "type": "string"},
    ),
    "Literal-unbounded": (
        overridden(Literal[1, 2], {"type": "integer"}),
        "Overridden.value",
        {"enum": [1, 2], "type": "integer"},
        {"type": "integer"},
    ),
    "bool-Literal-as-int": (
        overridden(Literal[True], {"type": "boolean", "const": 1}),
        "Overridden.value",
        {"const": True, "type": "boolean"},
        {"const": 1, "type": "boolean"},
    ),
    "list-items": (
        (Annotated[list[int], WithJsonSchema({"type": "array", "items": {"type": "string"}})], []),
        "Overridden.value[]",
        {"type": "integer"},
        {"type": "string"},
    ),
    "extra-int-as-string": (
        (int, Field(json_schema_extra={"type": "string"})),
        "Overridden.value",
        {"type": "integer"},
        {"type": "string"},
    ),
    "extra-str-as-integer": (
        (str, Field(json_schema_extra={"type": "integer"})),
        "Overridden.value",
        {"type": "string"},
        {"type": "integer"},
    ),
    "extra-Literal-enum": (
        (Literal["a", "b"], Field(json_schema_extra={"enum": ["a", "z"]})),
        "Overridden.value",
        {"enum": ["a", "b"], "type": "string"},
        {"enum": ["a", "z"], "type": "string"},
    ),
    "extra-date-as-date-time": (
        (date, Field(json_schema_extra={"format": "date-time"})),
        "Overridden.value",
        {"format": "date", "type": "string"},
        {"format": "date-time", "type": "string"},
    ),
    "extra-aware-as-date": (
        (AwareDatetime, Field(json_schema_extra={"format": "date"})),
        "Overridden.value",
        {"format": "date-time", "type": "string"},
        {"format": "date", "type": "string"},
    ),
}
# Overrides that only narrow what model_validate accepts, so a response that matches them still validates.
NARROWING_OVERRIDES = {
    "Decimal-as-number": overridden(Decimal, {"type": "number"}),
    "Decimal-as-bounded-number": overridden(Decimal, {"type": "number", "minimum": 0}),
    "Decimal-as-numeric-string": overridden(Decimal, {"type": "string", "pattern": r"^-?\d+(\.\d+)?$"}),
    "str-narrowed-by-enum": (str, Field(json_schema_extra={"enum": ["a", "b"]})),
    "int-narrowed-by-minimum": overridden(int, {"type": "integer", "minimum": 3}),
}
# Overrides json_schema_extra places beside a $ref (an Enum field) or beside Optional's anyOf, where the check
# cannot weigh them against the schema they sit by: each field definition and the keyword's place.
SIBLING_OVERRIDES = {
    "extra-Enum-enum": ((Color, Field(json_schema_extra={"enum": ["x"]})), "['enum'] beside $ref"),
    "extra-Optional-type": ((int | None, Field(None, json_schema_extra={"type": "string"})), "['type'] beside anyOf"),
}


# Annotations describe() rejects: each model, the field path the message names, and a phrase of its reason.
class DictField(BaseModel):
    counts: dict[str, int]


class TupleField(BaseModel):
    pair: tuple[int, int]


class SetField(BaseModel):
    ids: set[int]


class FrozensetField(BaseModel):
    ids: frozenset[str]


class BytesField(BaseModel):
    blob: bytes


class AnyField(BaseModel):
    value: Any


class ObjectField(BaseModel):
    value: object


class ScalarUnion(BaseModel):
    value: int | str


class ModelUnion(BaseModel):
    item: Part | Line


class OptionalUnion(BaseModel):
    value: int | str | None = None


class ListOfLists(BaseModel):
    grid: list[list[int]]


class ListOfOptional(BaseModel):
    values: list[int | None]


class ListOfUnion(BaseModel):
    values: list[int | str]


class ListOfDicts(BaseModel):
    rows: list[dict[str, int]]


class BareList(BaseModel):
    values: list


class OptionalList(BaseModel):
    values: list[str] | None = None


class MixedEnumField(BaseModel):
    value: MixedValues


class EmptyEnumField(BaseModel):
    value: NoMembers


class MixedLiteral(BaseModel):
    value: Literal["a", 1]


class NoneLiteral(BaseModel):
    value: Literal[None]


class IntBoolLiteral(BaseModel):
    value: Literal[1, True]


class BoolEnumField(BaseModel):
    value: Answer


class FloatEnumField(BaseModel):
    value: Ratio


class PlainDatetime(BaseModel):
    at: datetime


class OptionalPlainDatetime(BaseModel):
    at: datetime | None = None


class PlainDatetimes(BaseModel):
    ats: list[datetime] = []


class PlainDatetimeKey(BaseModel):
    at: Annotated[datetime, Key]


class FlagField(BaseModel):
    perm: Perm


class IntFlagField(BaseModel):
    perm: IPerm


class Cat(BaseModel):
    kind: Literal["cat"]
    lives: int


class DiscriminatedField(BaseModel):
    pet: Annotated[Cat, Field(discriminator="kind")]


class DiscriminatedList(BaseModel):
    pets: list[Annotated[Cat, Field(discriminator="kind")]] = []


class DiscriminatedOptional(BaseModel):
    pet: Annotated[Cat, Field(discriminator="kind")] | None = None


class DiscriminatorObject(BaseModel):
    pet: Annotated[Cat, Discriminator("kind")]


PartNo = NewType("PartNo", str)


class NewTypeField(BaseModel):
    code: PartNo


class DeepPart(BaseModel):
    number: dict[str, int]


class DeepLine(BaseModel):
    part: DeepPart


class DeepOrder(BaseModel):
    lines: list[DeepLine]


REJECTED_ANNOTATIONS = [
    (DictField, "DictField.counts", "unsupported type"),
    (TupleField, "TupleField.pair", "unsupported type"),
    (SetField, "SetField.ids", "unsupported type"),
    (FrozensetField, "FrozensetField.ids", "unsupported type"),
    (BytesField, "BytesField.blob", "unsupported type"),
    (AnyField, "AnyField.value", "unsupported type"),
    (ObjectField, "ObjectField.value", "unsupported type"),
    (ScalarUnion, "ScalarUnion.value", "two or more types"),
    (ModelUnion, "ModelUnion.item", "two or more types"),
    (OptionalUnion, "OptionalUnion.value", "two or more types"),
    (ListOfLists, "ListOfLists.grid", "lists cannot hold lists"),
    (ListOfOptional, "ListOfOptional.values", "list items cannot be Optional or a union"),
    (ListOfUnion, "ListOfUnion.values", "list items cannot be Optional or a union"),
    (ListOfDicts, "ListOfDicts.rows", "unsupported list item type"),
    (BareList, "BareList.values", "needs an item type"),
    (OptionalList, "OptionalList.values", "a list cannot be Optional"),
    (MixedEnumField, "MixedEnumField.value", "unsupported type"),
    (EmptyEnumField, "EmptyEnumField.value", "unsupported type"),
    (MixedLiteral, "MixedLiteral.value", "unsupported type"),
    (NoneLiteral, "NoneLiteral.value", "unsupported type"),
    (IntBoolLiteral, "IntBoolLiteral.value", "unsupported type"),
    (BoolEnumField, "BoolEnumField.value", "unsupported type"),
    (FloatEnumField, "FloatEnumField.value", "unsupported type"),
    (DeepOrder, "DeepOrder.lines[].part.number", "unsupported type"),
    (PlainDatetime, "PlainDatetime.at", "annotate pydantic.AwareDatetime or pydantic.NaiveDatetime"),
    (OptionalPlainDatetime, "OptionalPlainDatetime.at", "annotate pydantic.AwareDatetime or pydantic.NaiveDatetime"),
    (PlainDatetimes, "PlainDatetimes.ats", "annotate pydantic.AwareDatetime or pydantic.NaiveDatetime"),
    (PlainDatetimeKey, "PlainDatetimeKey.at", "annotate pydantic.AwareDatetime or pydantic.NaiveDatetime"),
    (FlagField, "FlagField.perm", "Flag enums are not supported"),
    (IntFlagField, "IntFlagField.perm", "Flag enums are not supported"),
    (DiscriminatedField, "DiscriminatedField.pet", "discriminated unions are not supported"),
    (DiscriminatedList, "DiscriminatedList.pets", "discriminated unions are not supported"),
    (DiscriminatedOptional, "DiscriminatedOptional.pet", "discriminated unions are not supported"),
    (DiscriminatorObject, "DiscriminatorObject.pet", "discriminated unions are not supported"),
    (NewTypeField, "NewTypeField.code", "NewType aliases are not supported"),
]


class Node(BaseModel):
    label: str
    children: list["Node"] = []


class Ping(BaseModel):
    pong: "Pong | None" = None


class Pong(BaseModel):
    ping: Ping


Ping.model_rebuild()


# Key declarations.
class Marked(BaseModel):
    code: Annotated[str, Key]
    name: str


class Namespaced(BaseModel):
    code: Annotated[str, Key("Part")]
    revision: Annotated[int, Key(" PART ")]
    name: str


class Unkeyed(BaseModel):
    code: str
    revision: int
    name: str


class EveryKeyType(BaseModel):
    text: Annotated[str, Key]
    count: Annotated[int, Key]
    amount: Annotated[Decimal, Key]
    day: Annotated[date, Key]
    at: Annotated[AwareDatetime, Key]
    local: Annotated[NaiveDatetime, Key]
    color: Annotated[Color, Key]
    level: Annotated[Level, Key]
    legacy: Annotated[Legacy, Key]
    letter: Annotated[Literal["a", "b"], Key]
    number: Annotated[Literal[1, 2], Key]
    constrained: Annotated[str, Key, Field(min_length=1, description="a described key")]


class MixedNamespaces(BaseModel):
    code: Annotated[str, Key("part")]
    revision: Annotated[int, Key]


class TwoNamespaces(BaseModel):
    code: Annotated[str, Key("part")]
    revision: Annotated[int, Key("assembly")]


class OneFieldTwoNamespaces(BaseModel):
    code: Annotated[str, Key("part"), Key("assembly")]


class OptionalKey(BaseModel):
    code: Annotated[str | None, Key]


class DefaultedKey(BaseModel):
    code: Annotated[str, Key] = "PN-1"


class FactoryKey(BaseModel):
    code: Annotated[str, Key] = Field(default_factory=lambda: "PN-1")


class FloatKey(BaseModel):
    weight: Annotated[float, Key]


class BoolKey(BaseModel):
    flag: Annotated[bool, Key]


class BoolLiteralKey(BaseModel):
    flag: Annotated[Literal[True, False], Key]


class ModelKey(BaseModel):
    part: Annotated[Part, Key]


class ListKey(BaseModel):
    codes: Annotated[list[str], Key]


class KeyInsideOptional(BaseModel):
    code: Annotated[str, Key] | None = None


class KeyInsideList(BaseModel):
    codes: list[Annotated[str, Key]]


class ChildWithBadKey(BaseModel):
    weight: Annotated[float, Key]


class HoldsBadKey(BaseModel):
    code: Annotated[str, Key]
    children: list[ChildWithBadKey]


REJECTED_KEYS = [
    (MixedNamespaces, "MixedNamespaces", "mix namespaces"),
    (TwoNamespaces, "TwoNamespaces", "mix namespaces"),
    (OneFieldTwoNamespaces, "OneFieldTwoNamespaces.code", "more than one namespace"),
    (OptionalKey, "OptionalKey.code", "must not be Optional"),
    (DefaultedKey, "DefaultedKey.code", "must be required"),
    (FactoryKey, "FactoryKey.code", "must be required"),
    (FloatKey, "FloatKey.weight", "not float"),
    (BoolKey, "BoolKey.flag", "not bool"),
    (BoolLiteralKey, "BoolLiteralKey.flag", "a Key field must be"),
    (ModelKey, "ModelKey.part", "a Key field must be"),
    (ListKey, "ListKey.codes", "a Key field must be"),
    (KeyInsideOptional, "KeyInsideOptional.code", "Key must mark the field itself"),
    (KeyInsideList, "KeyInsideList.codes", "Key must mark the field itself"),
    (HoldsBadKey, "HoldsBadKey.children[].weight", "not float"),
]


# Model classes check_schema() rejects.
class NoFields(BaseModel):
    pass


class Permissive(BaseModel):
    model_config = ConfigDict(extra="allow")
    code: str


class HoldsPermissive(BaseModel):
    children: list[Permissive]


class Forbidding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str


class Ignoring(BaseModel):
    model_config = ConfigDict(extra="ignore")
    code: str


class Codes(RootModel[list[str]]):
    pass


class HoldsRootModel(BaseModel):
    codes: Codes


class HoldsNoFields(BaseModel):
    empty: NoFields | None = None


class NotFullyDefined(BaseModel):
    later: "NotDefinedAnywhere"  # noqa: F821


# Class rules (review I1, Minor 1, Minor 4).
class SerializationMode(BaseModel):
    model_config = ConfigDict(json_schema_mode_override="serialization")
    code: Annotated[str, Key]
    quantity: int = Field(serialization_alias="Quantity")


class ValidationMode(BaseModel):
    model_config = ConfigDict(json_schema_mode_override="validation")
    code: Annotated[str, Key]


class EnumValues(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    color: Color


class Deferred(BaseModel):
    model_config = ConfigDict(defer_build=True)
    code: str


class DeferredRebuilt(BaseModel):
    model_config = ConfigDict(defer_build=True)
    code: str


# Wire names: the key each field's value travels under, the key model_validate reads it from (review I1, Minor 7).
class Aliases(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    code: Annotated[str, Key] = Field(alias="Code")
    name: str = Field(validation_alias="Name")
    note: str = Field(validation_alias=AliasChoices("Note", "remark", AliasPath("notes", 0)))
    quantity: int = Field(serialization_alias="Quantity")
    plain: str


class Camel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    part_number: Annotated[str, Key]


class ByName(BaseModel):
    model_config = ConfigDict(validate_by_alias=False, validate_by_name=True)
    code: str = Field(alias="Code")


class Pathed(BaseModel):
    code: Annotated[str, Key] = Field(validation_alias=AliasPath("ids", 0))


class ChoicePathFirst(BaseModel):
    code: Annotated[str, Key] = Field(validation_alias=AliasChoices(AliasPath("ids", 0), "code"))


class SharedWireName(BaseModel):
    first: int = Field(alias="x")
    second: int = Field(alias="x")


# Schemas whose JSON schema disagrees with what model_validate accepts (review I1): each passed prepare() before.
class SkippedField(BaseModel):
    code: Annotated[str, Key]
    secret: SkipJsonSchema[int]


class SkippedKey(BaseModel):
    code: Annotated[SkipJsonSchema[str], Key]
    name: str


class AllSkipped(BaseModel):
    secret: SkipJsonSchema[int] = 0


class Retyped(BaseModel):
    code: Annotated[str, Key]
    part: Annotated[Part, WithJsonSchema({"type": "string"})]


class Rewritten(BaseModel):
    model_config = ConfigDict(json_schema_extra={"properties": {"other": {"type": "string"}}, "required": ["other"]})
    code: Annotated[str, Key]


def add_other_property(schema: dict) -> None:
    """A json_schema_extra that adds a property no field has."""
    schema["properties"]["other"] = {"type": "string"}


class Extended(BaseModel):
    model_config = ConfigDict(json_schema_extra=add_other_property)
    code: str


class RequiredDropped(BaseModel):
    model_config = ConfigDict(json_schema_extra={"required": []})
    code: str


class RequiredAdded(BaseModel):
    # The OpenAI-strict style: every property required, a defaulted one too. It narrows; it does not disagree.
    model_config = ConfigDict(json_schema_extra={"required": ["code", "note"]})
    code: str
    note: str = ""


class NullItems(BaseModel):
    values: Annotated[
        list[int], WithJsonSchema({"type": "array", "items": {"anyOf": [{"type": "integer"}, {"type": "null"}]}})
    ]


class UnionShape(BaseModel):
    value: Annotated[
        int,
        WithJsonSchema({"anyOf": [{"type": "object", "properties": {"a": {"type": "integer"}}}, {"type": "integer"}]}),
    ]


class GhostRequired(BaseModel):
    model_config = ConfigDict(json_schema_extra={"required": ["code", "ghost"]})
    code: str


class NullableWire(BaseModel):
    value: Annotated[int, WithJsonSchema({"anyOf": [{"type": "integer"}, {"type": "null"}]})]


class ListAsString(BaseModel):
    tags: Annotated[list[str], WithJsonSchema({"type": "string"})]


class ItemsAsStrings(BaseModel):
    parts: Annotated[list[Part], WithJsonSchema({"type": "array", "items": {"type": "string"}})]


class ScalarAsObject(BaseModel):
    value: Annotated[int, WithJsonSchema({"type": "object", "properties": {"a": {"type": "integer"}}})]


class HiddenChild(BaseModel):
    code: SkipJsonSchema[str]
    name: str


class HoldsHiddenChild(BaseModel):
    lines: list[HiddenChild]


DISAGREEING = [
    (SkippedField, "SkippedField.secret", "is not among the JSON schema's properties"),
    (SkippedKey, "SkippedKey.code", "is not among the JSON schema's properties"),
    (AllSkipped, "AllSkipped.secret", "is not among the JSON schema's properties"),
    (Retyped, "Retyped.part", "describes a scalar value, but the field holds an object"),
    (Rewritten, "Rewritten.code", "is not among the JSON schema's properties"),
    (Extended, "Extended", "the JSON schema has the property 'other'"),
    (RequiredDropped, "RequiredDropped.code", "the JSON schema does not require it, but model_validate does"),
    (GhostRequired, "GhostRequired", "the JSON schema requires 'ghost', which no field reads"),
    (NullableWire, "NullableWire.value", "allows null, but the field is not Optional"),
    (NullItems, "NullItems.values[]", "allows null items, but list items are never None"),
    (UnionShape, "UnionShape.value", "describes a union of shapes, but the field holds a scalar value"),
    (ListAsString, "ListAsString.tags", "describes a scalar value, but the field holds an array"),
    (ItemsAsStrings, "ItemsAsStrings.parts[]", "describes a scalar value, but the field holds an object"),
    (ScalarAsObject, "ScalarAsObject.value", "describes an object, but the field holds a scalar value"),
    (HoldsHiddenChild, "HoldsHiddenChild.lines[].code", "is not among the JSON schema's properties"),
]


# Defaults (review I2, Minor 4).
class NoneDefaultScalar(BaseModel):
    count: int = None


class NoneDefaultList(BaseModel):
    tags: list[str] = None


class NoneDefaultModel(BaseModel):
    part: Part = None


class NanDefault(BaseModel):
    ratio: float = float("nan")


class InfDefault(BaseModel):
    ratio: float | None = float("inf")


class NanExamples(BaseModel):
    ratio: float = Field(examples=[1.5, float("nan")])


class InfExamplesOnEnum(BaseModel):
    color: Color = Field(examples=[float("inf")])


class FactoryNone(BaseModel):
    count: int = Field(default_factory=lambda: None)


# Realistic schemas (the review's over-strictness battery, trimmed): each must pass prepare(), and a record
# shaped like its JSON schema must validate.
class Currency(StrEnum):
    USD = "USD"
    EUR = "EUR"


class InvoiceStatus(str, Enum):
    DRAFT = "draft"
    ISSUED = "issued"


class Address(BaseModel):
    """A postal address."""

    street: str = Field(description="Street and number", examples=["1 Main St"])
    city: str = Field(min_length=1, max_length=100)
    postal_code: str | None = Field(default=None, pattern=r"^[A-Z0-9 -]{3,10}$")
    country: str = Field(default="US", min_length=2, max_length=2)


class LineItem(BaseModel):
    line_no: Annotated[PositiveInt, Key, Field(description="1-based line number")]
    description: str
    quantity: Decimal = Field(gt=0, max_digits=12, decimal_places=3)
    unit_price: Decimal = Field(ge=0, max_digits=12, decimal_places=2, examples=[Decimal("9.99")])
    discount_pct: float | None = Field(default=None, ge=0, le=100)
    unit: Literal["ea", "kg", "h"] = "ea"


class Invoice(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    invoice_number: Annotated[str, Key("invoice"), Field(examples=["INV-001"])]
    issue_date: date
    due_date: date | None = None
    status: InvoiceStatus = InvoiceStatus.ISSUED
    currency: Currency = Currency.USD
    bill_to: Address = Field(description="Billing address")
    ship_to: Address | None = None
    line_items: list[LineItem] = Field(default_factory=list, max_length=500)
    total: Decimal = Field(max_digits=14, decimal_places=2)


class Material(IntEnum):
    STEEL = 1
    ALUMINIUM = 2


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class Supplier(CamelModel):
    supplier_name: str
    supplier_code: str | None = None


class Component(CamelModel):
    part_number: Annotated[str, Key("part"), StringConstraints(strip_whitespace=True, min_length=1, max_length=32)]
    quantity_per: int = Field(default=1, ge=1)
    material: Material | None = None
    suppliers: list[Supplier] = Field(default_factory=list)


class Assembly(CamelModel):
    assembly_number: Annotated[str, Key("part")]
    revision: Annotated[str, Key("part")] = Field(pattern=r"^[A-Z]{1,2}$")
    components: list[Component] = Field(default_factory=list)


class BillOfMaterials(CamelModel):
    document_number: Annotated[str, Key]
    top_level: Assembly
    assemblies: list[Assembly] = []


class PartyRole(Enum):
    BUYER = "buyer"
    SELLER = "seller"


class Party(BaseModel):
    name: Annotated[str, Key("party")]
    role: PartyRole
    registration_number: str | None = Field(default=None, alias="regNo")
    signatory: str | None = Field(default=None, validation_alias=AliasChoices("signatory", "signedBy"))


class Contract(BaseModel):
    model_config = ConfigDict(frozen=True, title="Commercial contract")

    contract_id: Annotated[str, Key]
    effective_date: date
    signed_at: AwareDatetime | None = None
    governing_law: Literal["England and Wales", "New York", "Other"] = "Other"
    value: Decimal | None = Field(default=None, ge=0, max_digits=16, decimal_places=2)
    auto_renew: bool = False
    notice_period_days: NonNegativeInt | None = None
    parties: list[Party] = Field(min_length=2)


class ResultFlag(StrEnum):
    LOW = "low"
    HIGH = "high"


class Priority(IntEnum):
    ROUTINE = 0
    URGENT = 1


class Specimen(BaseModel):
    specimen_id: Annotated[str, Key("specimen")]
    kind: Literal["blood", "urine"]
    collected_at: NaiveDatetime


class Analyte(BaseModel):
    code: Annotated[str, Key, Field(max_length=20)]
    value: float | None = None
    flag: ResultFlag | None = None


class LabReport(BaseModel):
    model_config = ConfigDict(use_attribute_docstrings=True)

    accession_number: Annotated[str, Key("accession")]
    """The lab's accession number."""
    report_date: date
    priority: Priority = Priority.ROUTINE
    specimens: list[Specimen] = Field(default_factory=list)
    results: list[Analyte] = Field(default_factory=list, min_length=1)
    comments: list[Annotated[str, StringConstraints(max_length=500)]] = []


# Each realistic schema, its root keys, and a record shaped like its JSON schema (wire names, JSON values).
REALISTIC = {
    "invoice": (
        Invoice,
        KeySpec(fields=("invoice_number",), namespace="invoice"),
        {
            "invoice_number": "INV-001",
            "issue_date": "2026-11-04",
            "bill_to": {"street": "1 Main St", "city": "Springfield"},
            "line_items": [{"line_no": 1, "description": "Bolt", "quantity": "2", "unit_price": 1.5}],
            "total": "3.00",
        },
    ),
    "camelCase BOM": (
        BillOfMaterials,
        KeySpec(fields=("document_number",), namespace=None),
        {
            "documentNumber": "BOM-7",
            "topLevel": {
                "assemblyNumber": "A-1",
                "revision": "B",
                "components": [{"partNumber": "PN-104", "material": 2, "suppliers": [{"supplierName": "Acme"}]}],
            },
        },
    ),
    "contract": (
        Contract,
        KeySpec(fields=("contract_id",), namespace=None),
        {
            "contract_id": "C-9",
            "effective_date": "2026-01-01",
            "signed_at": "2026-01-02T10:00:00+00:00",
            "parties": [{"name": "Acme", "role": "buyer", "regNo": "123"}, {"name": "Globex", "role": "seller"}],
        },
    ),
    "lab report": (
        LabReport,
        KeySpec(fields=("accession_number",), namespace="accession"),
        {
            "accession_number": "ACC-1",
            "report_date": "2026-11-04",
            "priority": 1,
            "specimens": [{"specimen_id": "S1", "kind": "blood", "collected_at": "2026-11-03T08:30:00"}],
            "results": [{"code": "GLU", "value": 5.4, "flag": "high"}],
        },
    ),
}


# Message order (review Minor 4): the annotation at fault, not a model it names.
class HoldsPermissiveDict(BaseModel):
    counts: dict[str, Permissive]


class HoldsPermissiveUnion(BaseModel):
    item: Permissive | Part


# JSON-schema shapes check_json_schema() accepts and rejects.
class DescribedChild(BaseModel):
    child: Part = Field(description="the part")


class Prices(BaseModel):
    price: Decimal
    discount: Decimal | None = None


class ConstrainedPrices(BaseModel):
    price: Decimal = Field(ge=0, max_digits=7, decimal_places=2)
    discount: Decimal | None = Field(default=None, ge=0)


class Constrained(BaseModel):
    code: str = Field(min_length=1, max_length=10, pattern=r"^[A-Z]+$", description="a code", examples=["AB"])
    count: int = Field(ge=0, le=100, multiple_of=5)
    ratio: float = Field(gt=0, lt=1)
    codes: list[str] = Field(default_factory=list, max_length=5, json_schema_extra={"uniqueItems": True})
    day: date
    old: int | None = Field(default=None, deprecated=True)


class Orphaned(BaseModel):
    model_config = ConfigDict(json_schema_extra={"$defs": {"Orphan": {"type": "string"}}})
    code: str


class OpenRoot(BaseModel):
    model_config = ConfigDict(json_schema_extra={"additionalProperties": True})
    code: str


class StringRoot(BaseModel):
    model_config = ConfigDict(json_schema_extra={"type": "string"})
    code: str


class SlashAlias(BaseModel):
    value: int = Field(alias="a/b~c", json_schema_extra={"oneOf": [{"type": "integer"}]})


class NoJsonSchema(BaseModel):
    callback: Callable[[], int]


SHARED_EXAMPLES = [{"code": "AB"}]


class SharedExtras(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": SHARED_EXAMPLES})
    code: str = Field(json_schema_extra={"x-meta": {"tags": ["a"]}})


def tampered(schema: dict) -> type[BaseModel]:
    """A model whose ``value`` field (an ``int``) has the JSON schema ``schema``."""
    return create_model("Tampered", value=(Annotated[int, WithJsonSchema(schema)], ...))


# Each tampered field schema, the pointer at fault (under #/properties/value) and a phrase of the reason.
REJECTED_JSON = [
    ({"oneOf": [{"type": "integer"}]}, "", "'oneOf'"),
    ({"allOf": [{"type": "integer"}]}, "", "'allOf'"),
    ({"type": "integer", "not": {"type": "string"}}, "", "'not'"),
    ({"type": "integer", "if": {"minimum": 1}}, "", "'if'"),
    ({"type": "integer", "then": {"minimum": 1}}, "", "'then'"),
    ({"type": "integer", "else": {"minimum": 1}}, "", "'else'"),
    ({"type": "object", "properties": {"a": {"type": "integer"}}, "patternProperties": {"^x": {}}}, "", "'pattern"),
    ({"type": "array", "items": {"type": "integer"}, "prefixItems": [{"type": "integer"}]}, "", "'prefixItems'"),
    ({"type": "array", "items": {"type": "integer"}, "additionalItems": False}, "", "'additionalItems'"),
    ({"type": "array", "items": {"type": "integer"}, "contains": {"type": "integer"}}, "", "'contains'"),
    ({"type": "array", "items": {"type": "integer"}, "unevaluatedItems": False}, "", "'unevaluatedItems'"),
    ({"type": "object", "properties": {"a": {"type": "integer"}}, "propertyNames": {}}, "", "'propertyNames'"),
    ({"type": "object", "properties": {"a": {"type": "integer"}}, "dependentSchemas": {}}, "", "'dependentSchemas'"),
    ({"type": "object", "properties": {"a": {"type": "integer"}}, "dependencies": {}}, "", "'dependencies'"),
    ({"type": "object", "properties": {"a": {"type": "integer"}}, "unevaluatedProperties": False}, "", "'unevaluated"),
    ({"type": "integer", "$dynamicRef": "#x"}, "", "'$dynamicRef'"),
    ({"type": "integer", "$recursiveRef": "#"}, "", "'$recursiveRef'"),
    ({"type": "integer", "definitions": {}}, "", "'definitions'"),
    ({"type": "string", "contentSchema": {"type": "integer"}}, "", "'contentSchema'"),
    ({"type": "integer", "$id": "https://example.com/other"}, "", "'$id'"),
    ({"type": "integer", "$anchor": "here"}, "", "'$anchor'"),
    ({"type": "integer", "$dynamicAnchor": "here"}, "", "'$dynamicAnchor'"),
    ({"type": "integer", "$recursiveAnchor": True}, "", "'$recursiveAnchor'"),
    ({"type": "string", "$defs": {"X": {"type": "string"}}}, "", "$defs"),
    (
        {"type": "object", "properties": {"a": {"type": "integer"}}, "additionalProperties": {"type": "integer"}},
        "",
        "additionalProperties",
    ),
    ({"type": "object", "properties": {"a": {"type": "integer"}}, "additionalProperties": True}, "", "additional"),
    ({}, "", "unconstrained"),
    ({"description": "anything at all"}, "", "unconstrained"),
    ({"type": "object"}, "", "without properties"),
    ({"type": "object", "properties": {}}, "", "without properties"),
    ({"type": "array"}, "", "without items"),
    ({"type": "array", "items": [{"type": "integer"}]}, "", "'items' must be one schema"),
    ({"type": ["integer", "null"]}, "", "'type'"),
    ({"type": "date"}, "", "'type'"),
    ({"$ref": "https://example.com/schema.json"}, "", "$ref"),
    ({"anyOf": []}, "", "anyOf"),
    ({"anyOf": [{"type": "integer"}, {"type": "string"}]}, "", "anyOf must be"),
    ({"anyOf": [{"type": "null"}, {"type": "integer"}]}, "", "anyOf must be"),
    ({"anyOf": [{"type": "integer"}]}, "", "anyOf must be"),
    ({"anyOf": [{"type": "integer"}, {"type": "null"}, {"type": "null"}]}, "", "anyOf must be"),
    ({"anyOf": [{"type": "string"}, {"type": "number"}]}, "", "anyOf must be"),
    ({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]}, "", "anyOf must be"),
    ({"type": "object", "properties": [{"type": "integer"}]}, "", "'properties' must map names to schemas"),
    ({"enum": []}, "", "enum"),
    ({"anyOf": [{}, {"type": "null"}]}, "/anyOf/0", "unconstrained"),
    ({"anyOf": [True, {"type": "null"}]}, "/anyOf/0", "not a schema object"),
    ({"type": "array", "items": {"oneOf": [{"type": "integer"}]}}, "/items", "'oneOf'"),
    ({"type": "object", "properties": {"a": {"type": "object"}}}, "/properties/a", "without properties"),
]


def chain(models: int, leaf: object = str) -> type[BaseModel]:
    """``M1.next.next…``: ``models`` models nested one in the next; the innermost has ``value: leaf``."""
    model = create_model(f"M{models}", value=(leaf, ...))
    for number in range(models - 1, 0, -1):
        model = create_model(f"M{number}", next=(model, ...))
    return model


def list_chain(lists: int) -> type[BaseModel]:
    """``L0.items[].items[]…``: ``lists`` lists of models nested one in the next."""
    model = create_model(f"L{lists}", value=(str, ...))
    for number in range(lists - 1, -1, -1):
        model = create_model(f"L{number}", items=(list[model], ...))
    return model


def doubling(levels: int) -> type[BaseModel]:
    """``D<levels>``: each level holds the one below twice, so its expanded field tree doubles per level."""
    model = create_model("D0", value=(str, ...))
    for level in range(1, levels + 1):
        model = create_model(f"D{level}", left=(model, ...), right=(model, ...))
    return model


def expanded_fields(levels: int) -> int:
    """How many fields describe() expands ``doubling(levels)`` to: 1 at level 0, then 2 + twice the level below."""
    fields = 1
    for _ in range(levels):
        fields = 2 + 2 * fields
    return fields


def refs_in(document: object) -> list[str]:
    """Every ``$ref`` string anywhere in ``document``."""
    found: list[str] = []
    stack = [document]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if isinstance(node.get("$ref"), str):
                found.append(node["$ref"])
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return found


def resolve(document: dict, ref: str) -> object:
    """The node a local JSON-pointer ``ref`` (``#/…``) names in ``document``; KeyError when it names none."""
    assert ref.startswith("#/"), ref
    node: object = document
    for token in ref[2:].split("/"):
        assert isinstance(node, dict), ref
        node = node[token.replace("~1", "/").replace("~0", "~")]
    return node


def expected_wrapper(model: type[BaseModel]) -> dict:
    """The response wrapper of plan §0 correction 2, built here from pydantic's schema for ``model``."""
    document = model.model_json_schema()
    wrapper: dict = {
        "type": "object",
        "properties": {
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "record": {key: value for key, value in document.items() if key != "$defs"},
                        "grounding_quotes": {"type": "array", "items": {"type": "string"}},
                        "source_element_ids": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["record", "grounding_quotes", "source_element_ids"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["records"],
        "additionalProperties": False,
    }
    if "$defs" in document:
        wrapper["$defs"] = document["$defs"]
    return wrapper


def run_python(code: str, *, isolated: bool = True, env: dict | None = None, cwd: Path | None = None) -> str:
    """Run ``code`` in a fresh interpreter (``-I`` unless not ``isolated``); the last line it prints."""
    command = [sys.executable, "-I", "-c", code] if isolated else [sys.executable, "-c", code]
    completed = subprocess.run(
        command, cwd=cwd, env=env, capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip().splitlines()[-1]


# ---------------------------------------------------------------------------------------------------------------
# check_schema: the model classes
# ---------------------------------------------------------------------------------------------------------------


class TestCheckSchema:
    @pytest.mark.parametrize("model", [Order, Marked, Unkeyed, EveryForm, Forbidding, Ignoring])
    def test_returns_an_accepted_model(self, model):
        assert check_schema(model) is model

    def test_an_instance_is_rejected_naming_its_class(self):
        with pytest.raises(RationalizationError, match=r"not an instance of Unkeyed: pass the class"):
            check_schema(Unkeyed(code="PN-1", revision=1, name="bolt"))

    def test_base_model_itself_is_rejected(self):
        with pytest.raises(RationalizationError, match=r"not BaseModel itself"):
            check_schema(BaseModel)

    @pytest.mark.parametrize(
        ("value", "named"),
        [(42, "42 (int)"), ("Order", "'Order' (str)"), (dict, "dict"), (None, "None"), (list[int], "list[int]")],
    )
    def test_anything_but_a_model_class_is_rejected_naming_it(self, value, named):
        with pytest.raises(RationalizationError, match=r"^schema must be a pydantic BaseModel subclass, got ") as error:
            check_schema(value)

        assert named in str(error.value)

    def test_a_long_value_is_named_briefly(self):
        with pytest.raises(RationalizationError) as error:
            check_schema("x" * 10_000)

        assert len(str(error.value)) < 300

    def test_a_model_without_fields_is_rejected(self):
        with raises_at("NoFields"):
            check_schema(NoFields)

    def test_a_nested_model_without_fields_is_rejected_naming_the_field(self):
        with raises_at("HoldsNoFields.empty"):
            check_schema(HoldsNoFields)

    def test_extra_allow_is_rejected_at_the_root(self):
        with raises_at("Permissive") as error:
            check_schema(Permissive)

        assert "extra='allow'" in str(error.value)

    def test_extra_allow_is_rejected_anywhere_in_the_tree_naming_the_field(self):
        with raises_at("HoldsPermissive.children[]") as error:
            check_schema(HoldsPermissive)

        assert "Permissive" in str(error.value)
        assert "extra='allow'" in str(error.value)

    def test_a_root_model_is_rejected(self):
        with raises_at("Codes") as error:
            check_schema(Codes)

        assert "RootModel" in str(error.value)

    def test_a_nested_root_model_is_rejected_naming_the_field(self):
        with raises_at("HoldsRootModel.codes") as error:
            check_schema(HoldsRootModel)

        assert "RootModel" in str(error.value)

    def test_a_model_that_is_not_fully_defined_is_rejected(self):
        with raises_at("NotFullyDefined") as error:
            check_schema(NotFullyDefined)

        assert "model_rebuild()" in str(error.value)

    def test_literal_values_are_not_walked(self):
        values = tuple(f"v{index}" for index in range(MAX_WALK_STEPS + 1))
        model = create_model("BigLiteral", value=(Literal[values], ...))

        started = time.monotonic()
        assert check_schema(model) is model
        assert time.monotonic() - started < FAST_S

    def test_annotated_metadata_is_not_walked(self):
        # Walked, list[Permissive] in the metadata would name Permissive, whose extra='allow' check_schema rejects.
        model = create_model("Documented", value=(list[Annotated[str, list[Permissive]]], ...))

        assert check_schema(model) is model

    def test_a_json_schema_mode_override_other_than_validation_is_rejected(self):
        with raises_at("SerializationMode") as error:
            check_schema(SerializationMode)

        assert "json_schema_mode_override='serialization'" in str(error.value)
        assert check_schema(ValidationMode) is ValidationMode

    def test_use_enum_values_is_rejected(self):
        with raises_at("EnumValues") as error:
            check_schema(EnumValues)

        assert "use_enum_values=True" in str(error.value)

    def test_defer_build_is_named_with_its_remedy(self):
        with raises_at("Deferred") as error:
            check_schema(Deferred)

        assert "defer_build=True" in str(error.value)
        assert "Deferred.model_rebuild()" in str(error.value)

    def test_a_deferred_model_once_built_is_accepted(self):
        DeferredRebuilt.model_rebuild()

        assert prepare(DeferredRebuilt).model is DeferredRebuilt

    def test_a_recursive_model_is_accepted_here_since_describe_rejects_it(self):
        # Classes are visited once each, so a cycle ends the class walk without a loop.
        assert check_schema(Node) is Node
        assert check_schema(Ping) is Ping


# ---------------------------------------------------------------------------------------------------------------
# describe: annotations, nesting and keys
# ---------------------------------------------------------------------------------------------------------------


class TestDescribeAnnotations:
    def test_order_spec_with_its_child_keys(self):
        assert describe(Order) == ORDER_SPEC

    def test_child_keys_under_a_list_and_a_nested_model(self):
        spec = describe(Order)
        line = spec.fields[2].child
        assert line is not None
        part = line.fields[1].child
        assert part is not None

        assert line.keys == KeySpec(fields=("line_no",), namespace=None)
        assert part.keys == KeySpec(fields=("number",), namespace="part")

    def test_every_supported_form(self):
        spec = describe(EveryForm)

        assert {field.name: (field.kind, field.optional) for field in spec.fields} == EVERY_FORM_SHAPES
        assert [field.name for field in spec.fields] == list(EveryForm.model_fields)
        assert spec.keys == KeySpec(fields=(), namespace=None)
        for field in spec.fields:
            assert (field.child is not None) == (field.kind in ("model", "model_list"))
            if field.child is not None:
                assert field.child == PART_SPEC

    @pytest.mark.parametrize("holder", list(SCALAR_HOLDERS))
    @pytest.mark.parametrize("form", list(SCALAR_FORMS))
    def test_each_scalar_form_has_its_scalar_spec(self, form, holder):
        annotation, scalar = SCALAR_FORMS[form]
        definition, kind, optional, required = SCALAR_HOLDERS[holder]

        spec = describe(create_model("Holder", value=definition(annotation)))

        assert spec.fields == (
            FieldSpec(
                name="value",
                wire_name="value",
                kind=kind,
                optional=optional,
                required=required,
                child=None,
                scalar=scalar,
            ),
        )

    def test_required_is_whether_model_validate_requires_the_field(self):
        spec = describe(Order)
        line = spec.fields[2].child
        assert line is not None
        part = line.fields[1].child
        assert part is not None

        assert [(field.name, field.required) for field in spec.fields] == [
            ("order_id", True),
            ("placed", True),
            ("lines", True),
            ("tags", False),
        ]
        assert [field.required for field in line.fields] == [True, True, True]
        assert [(field.name, field.required) for field in part.fields] == [("number", True), ("name", False)]

    def test_required_follows_is_required_for_every_form(self):
        spec = describe(EveryForm)

        assert {field.name: field.required for field in spec.fields} == {
            name: info.is_required() for name, info in EveryForm.model_fields.items()
        }
        assert {field.name for field in spec.fields if not field.required} == {
            "maybe_text",
            "maybe_amount",
            "maybe_color",
            "maybe_letter",
            "maybe_part",
            "texts",
            "amounts",
            "colors",
            "letters",
            "parts",
        }

    def test_required_is_independent_of_optional(self):
        class Defaults(BaseModel):
            maybe: int | None
            counted: int = Field(default_factory=lambda: 0)
            plain: int = 1
            maybe_defaulted: int | None = None

        spec = describe(Defaults)

        assert [(field.name, field.optional, field.required) for field in spec.fields] == [
            ("maybe", True, True),
            ("counted", False, False),
            ("plain", False, False),
            ("maybe_defaulted", True, False),
        ]

    def test_a_model_field_has_no_scalar_spec(self):
        spec = describe(EveryForm)

        for field in spec.fields:
            assert (field.scalar is None) == (field.kind in ("model", "model_list")), field.name

    def test_every_supported_form_passes_every_check(self):
        prepared = prepare(EveryForm)

        assert prepared.spec == describe(EveryForm)

    @pytest.mark.parametrize(
        ("model", "path", "reason"), REJECTED_ANNOTATIONS, ids=lambda v: getattr(v, "__name__", None)
    )
    def test_an_unsupported_annotation_is_rejected_naming_the_field(self, model, path, reason):
        with raises_at(path) as error:
            describe(model)

        assert reason in str(error.value)[len(path) :]

    def test_the_spec_is_frozen_slotted_and_hashable(self):
        spec = describe(Order)

        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.keys = KeySpec(fields=(), namespace=None)
        assert not hasattr(spec, "__dict__")
        assert hash(spec) == hash(describe(Order))


class SubPart(Part):
    """A subclass: an instance of it is not exactly a Part."""


UTC_ZONE = timezone.utc
NAIVE_AT = datetime(2026, 1, 2, 3, 4, 5)
AWARE_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC_ZONE)

# A static default pydantic would leave unchecked in every record that omits the field, and what the field holds.
BAD_STATIC_DEFAULTS = {
    "str-for-int": (int, "1", "holds str, but the field holds int"),
    "int-for-bool": (bool, 0, "holds int, but the field holds bool"),
    "bool-for-int": (int, True, "holds bool, but the field holds int"),
    "bool-for-float": (float, True, "holds bool, but the field holds float (or an int within 2**53 of 0)"),
    "int-past-the-float-bound": (
        float,
        2**53 + 1,
        "holds int, but the field holds float (or an int within 2**53 of 0)",
    ),
    "float-for-decimal": (Decimal, 1.5, "holds float, but the field holds Decimal (or an int)"),
    "datetime-for-date": (date, NAIVE_AT, "holds datetime, but the field holds date"),
    "date-for-aware": (AwareDatetime, date(2026, 1, 2), "holds date, but the field holds an aware datetime"),
    "naive-for-aware": (AwareDatetime, NAIVE_AT, "holds datetime, but the field holds an aware datetime"),
    "aware-for-naive": (NaiveDatetime, AWARE_AT, "holds datetime, but the field holds a naive datetime"),
    "str-for-enum": (Color, "red", "holds str, but the field holds Color"),
    "int-for-int-enum": (Level, 1, "holds int, but the field holds Level"),
    "unlisted-literal": (Literal["a", "b"], "c", "holds str, but the field holds Literal['a', 'b']"),
    "bool-for-int-literal": (Literal[1, 2], True, "holds bool, but the field holds Literal[1, 2]"),
    "str-for-optional-int": (int | None, "1", "holds str, but the field holds int"),
    "bad-list-item": (list[str], ["a", 1], "[1]: holds int, but the field holds str"),
    "tuple-for-list": (list[str], ("a",), "holds tuple, but the field holds a list"),
    "dict-for-model": (Part, {"number": "PN-1"}, "holds dict, but the field holds Part"),
    "subclass-for-model": (Part, SubPart(number="PN-1"), "holds SubPart, but the field holds Part"),
    "dicts-for-models": (list[Part], [{"number": "PN-1"}], "[0]: holds dict, but the field holds Part"),
    "constructed-model-of-bad-values": (
        Part,
        Part.model_construct(number=1, name=2),
        "number: holds int, but the field holds str; name: holds int, but the field holds str",
    ),
    "constructed-model-without-a-field": (
        Part,
        Part.model_construct(),
        "number: is missing (the instance was built without it, as model_construct() can)",
    ),
    "constructed-model-in-a-list": (list[Part], [Part(number="PN-1"), Part.model_construct(number=1)], "[1].number: "),
    "constructed-line-of-a-bad-part": (
        Line,
        Line.model_construct(line_no=1, part=Part.model_construct(number=2), quantity=Decimal(1)),
        "part.number: holds int, but the field holds str",
    ),
}

GOOD_STATIC_DEFAULTS = {
    "int-for-float": (float, 0),
    "int-at-the-float-bound": (float, -(2**53)),
    "int-for-decimal": (Decimal, 2),
    "ints-for-float-list": (list[float], [0, 1.5]),
    "int": (int, 1),
    "bool": (bool, False),
    "str": (str, "x"),
    "date": (date, date(2026, 1, 2)),
    "aware": (AwareDatetime, AWARE_AT),
    "naive": (NaiveDatetime, NAIVE_AT),
    "enum-member": (Color, Color.RED),
    "listed-literal": (Literal["a", "b"], "a"),
    "optional-none": (int | None, None),
    "optional-value": (int | None, 3),
    "empty-list": (list[str], []),
    "model": (Part, Part(number="PN-1")),
    "models": (list[Part], [Part(number="PN-1")]),
    "constructed-model-of-good-values": (Part, Part.model_construct(number="PN-1", name=None)),
}


FACTORY_CALLS: list[int] = []


def counted_factory() -> int:
    FACTORY_CALLS.append(1)
    return 1


class SplitTags(BaseModel):
    """A before-validator of the model's own turns the default into the field's type."""

    model_config = ConfigDict(validate_default=True)
    k: Annotated[str, Key]
    tags: list[str] = "a,b"  # type: ignore[assignment]

    @field_validator("tags", mode="before")
    @classmethod
    def split(cls, value: object) -> object:
        return value.split(",") if isinstance(value, str) else value


class Coerced(BaseModel):
    model_config = ConfigDict(validate_default=True, coerce_numbers_to_str=True)
    k: Annotated[str, Key]
    code: str = 5  # type: ignore[assignment]


class AfterOnHalfBuilt(BaseModel):
    """A model validator that fails on the half-built instance the check validates against: inconclusive."""

    model_config = ConfigDict(validate_default=True)
    k: Annotated[str, Key]
    code: int = "5"  # type: ignore[assignment]

    @model_validator(mode="after")
    def check(self) -> "AfterOnHalfBuilt":
        if self.k.startswith("x"):
            raise ValueError("no")
        return self


class ErrorElsewhere(BaseModel):
    """A model validator refusing the half-built instance with a ValidationError located at the model: inconclusive."""

    model_config = ConfigDict(validate_default=True)
    k: Annotated[str, Key]
    code: int = "5"  # type: ignore[assignment]

    @model_validator(mode="after")
    def needs_k(self) -> "ErrorElsewhere":
        if self.k is None:
            raise ValueError("k is required")
        return self


class FrozenDefault(BaseModel):
    model_config = ConfigDict(validate_default=True)
    k: Annotated[str, Key]
    code: int = Field("5", frozen=True)  # type: ignore[assignment]


class FactoryAlongside(BaseModel):
    """A factory beside a validated default: the check must not call it."""

    model_config = ConfigDict(validate_default=True)
    k: Annotated[str, Key]
    code: int = "5"  # type: ignore[assignment]
    counted: int = Field(default_factory=counted_factory)


class StrictDate(BaseModel):
    model_config = ConfigDict(validate_default=True, strict=True)
    k: Annotated[str, Key]
    when: date = "2026-01-01"  # type: ignore[assignment]


class BadInt(BaseModel):
    model_config = ConfigDict(validate_default=True)
    k: Annotated[str, Key]
    code: int = "oops"  # type: ignore[assignment]


class ShortStrings(BaseModel):
    model_config = ConfigDict(validate_default=True, str_max_length=3)
    k: Annotated[str, Key]
    code: str = "toolong"


def _brief(value: object) -> str:
    """The default as the message shows it: its repr, cut to 80 characters."""
    text = repr(value)
    return text if len(text) <= 80 else f"{text[:77]}..."


class TestStaticDefaults:
    @pytest.mark.parametrize("case", list(BAD_STATIC_DEFAULTS))
    def test_a_static_default_that_is_no_value_of_its_field_is_refused(self, case):
        annotation, default, holds = BAD_STATIC_DEFAULTS[case]

        with raises_at("Holder.value") as error:
            prepare(create_model("Holder", value=(annotation, default)))

        message = str(error.value)
        assert message.startswith(f"Holder.value: its default {_brief(default)} is not a value of the field ({holds}")
        assert "pydantic does not validate defaults" in message
        assert "Field(validate_default=True)" in message

    @pytest.mark.parametrize("case", list(GOOD_STATIC_DEFAULTS))
    def test_a_static_default_of_the_fields_type_passes(self, case):
        annotation, default = GOOD_STATIC_DEFAULTS[case]

        assert prepare(create_model("Holder", value=(annotation, default))).spec.fields[0].name == "value"

    def test_a_bad_static_default_in_a_child_model_names_its_path(self):
        class Inner(BaseModel):
            count: int = "none"  # type: ignore[assignment]

        class Outer(BaseModel):
            inner: list[Inner] = []

        with raises_at("Outer.inner[].count"):
            describe(Outer)

    def test_instance_problems_names_a_field_a_constructed_root_lacks(self):
        problems = instance_problems(Part.model_construct(name=1), describe(Part))

        assert problems == [
            "number: is missing (the instance was built without it, as model_construct() can)",
            "name: holds int, but the field holds str",
        ]

    def test_a_default_factory_is_not_called_so_it_passes(self):
        model = create_model("Holder", value=(int, Field(default_factory=lambda: "not an int")))

        assert prepare(model).spec.fields[0].required is False

    def test_a_default_pydantic_validates_passes(self):
        field_level = create_model("Holder", value=(int, Field(default="1", validate_default=True)))
        model_level = create_model("Holder", __config__=ConfigDict(validate_default=True), value=(int, "1"))

        assert prepare(field_level).model is field_level
        assert prepare(model_level).model is model_level

    @pytest.mark.parametrize(
        ("annotation", "field"),
        [
            (int, Field("oops", validate_default=True)),
            (Annotated[int, Field(gt=0)], Field(0, validate_default=True)),
            (list[int], Field(["1", "x"], validate_default=True)),
        ],
    )
    def test_a_default_pydantic_validates_must_validate(self, annotation, field):
        with raises_at("Holder.value") as error:
            prepare(create_model("Holder", value=(annotation, field)))

        assert "fails the field's own validation" in str(error.value)
        assert "validate_default" in str(error.value)

    def test_a_models_validate_default_validates_its_static_defaults(self):
        model = create_model("Holder", __config__=ConfigDict(validate_default=True), value=(int, "oops"))

        with raises_at(
            "Holder.value",
        ):
            prepare(model)

    def test_a_validator_raising_another_exception_on_the_default_is_inconclusive(self):
        annotation = Annotated[int, AfterValidator(lambda value: {}[value])]
        model = create_model("Holder", value=(annotation, Field(1, validate_default=True)))

        assert prepare(model).model is model

    @pytest.mark.parametrize(
        "model", [SplitTags, Coerced, AfterOnHalfBuilt, ErrorElsewhere, FrozenDefault, FactoryAlongside]
    )
    def test_a_default_the_models_own_validation_accepts_passes(self, model):
        FACTORY_CALLS.clear()

        assert prepare(model).model is model
        assert FACTORY_CALLS == []

    @pytest.mark.parametrize(
        ("model", "path", "kind"),
        [
            (StrictDate, "StrictDate.when", "date_type"),
            (BadInt, "BadInt.code", "int_parsing"),
            (ShortStrings, "ShortStrings.code", "string_too_long"),
        ],
    )
    def test_a_default_the_models_own_validation_refuses_is_refused(self, model, path, kind):
        with raises_at(path) as error:
            prepare(model)

        assert f"fails the field's own validation ({kind})" in str(error.value)

    @pytest.mark.parametrize("error", [MemoryError, RecursionError])
    def test_memory_and_recursion_errors_propagate(self, error):
        def explode(value: int) -> int:
            raise error

        model = create_model("Holder", value=(Annotated[int, AfterValidator(explode)], Field(1, validate_default=True)))

        with pytest.raises(error):
            prepare(model)

    def test_a_field_that_opts_out_of_its_models_validate_default_is_checked(self):
        model = create_model(
            "Holder",
            __config__=ConfigDict(validate_default=True),
            value=(int, Field(default="1", validate_default=False)),
        )

        with raises_at("Holder.value"):
            prepare(model)


class TestDescribeRecursionAndDepth:
    def test_a_recursive_model_is_rejected_in_a_fresh_process_within_the_timeout(self):
        # The hang guard: a walk that loops on a cycle times this subprocess out instead of hanging the suite.
        code = textwrap.dedent(
            """
            import json
            from pydantic import BaseModel
            from datagrunt.rationalization import RationalizationError
            from datagrunt.rationalization.schema import prepare

            class Node(BaseModel):
                label: str
                children: list["Node"] = []

            try:
                prepare(Node)
            except RationalizationError as error:
                print(json.dumps(str(error)))
            """
        )

        message = json.loads(run_python(code))

        assert message.startswith("Node.children[]: ")
        assert "recursive" in message

    @pytest.mark.parametrize(
        ("model", "path"), [(Node, "Node.children[]"), (Ping, "Ping.pong.ping"), (Pong, "Pong.ping.pong")]
    )
    def test_a_recursive_model_is_rejected_quickly_naming_where_it_recurs(self, model, path):
        started = time.monotonic()
        with raises_at(path) as error:
            describe(model)

        assert time.monotonic() - started < FAST_S
        assert "recursive" in str(error.value)

    def test_the_bound_is_32(self):
        assert MAX_DEPTH == 32

    def test_32_nested_models_are_accepted(self):
        model = chain(MAX_DEPTH)

        spec = prepare(model).spec

        depth = 1
        while spec.fields[0].child is not None:
            spec = spec.fields[0].child
            depth += 1
        assert depth == MAX_DEPTH

    def test_33_nested_models_are_rejected_naming_the_deepest(self):
        with raises_at("M1" + ".next" * MAX_DEPTH) as error:
            describe(chain(MAX_DEPTH + 1))

        assert "deeper than 32" in str(error.value)

    def test_a_list_counts_as_a_level(self):
        # L0 is level 1; each list[L] adds two levels: the list and the model in it.
        assert prepare(list_chain(15)).spec.model.__name__ == "L0"  # 31 levels

        with raises_at("L0" + ".items[]" * 16):
            describe(list_chain(16))  # 33 levels

    def test_a_scalar_list_counts_as_a_level(self):
        assert prepare(chain(MAX_DEPTH - 1, leaf=list[str])).spec is not None  # 31 models and a list: 32

        with raises_at("M1" + ".next" * (MAX_DEPTH - 1) + ".value"):
            describe(chain(MAX_DEPTH, leaf=list[str]))  # 32 models and a list: 33

    def test_the_field_bound_is_10000_exactly(self):
        child = create_model("Child", **{f"f{index}": (int, ...) for index in range(99)})
        exact = create_model("Exact", **{f"c{index}": (child, ...) for index in range(100)})  # 100 + 100 * 99
        over = create_model("Over", **{f"c{index}": (child, ...) for index in range(100)}, extra=(int, ...))

        assert MAX_FIELDS == 10_000
        assert len(describe(exact).fields) == 100
        with raises_at("Over.extra") as error:
            describe(over)

        assert f"more than {MAX_FIELDS} fields" in str(error.value)

    def test_an_expanding_schema_is_rejected_quickly_past_the_field_bound(self):
        assert expanded_fields(11) <= MAX_FIELDS < expanded_fields(12)
        assert describe(doubling(11)).model.__name__ == "D11"

        started = time.monotonic()
        with pytest.raises(RationalizationError, match=rf"^D12[.\w]*: .*more than {MAX_FIELDS} fields"):
            describe(doubling(12))

        assert time.monotonic() - started < FAST_S


class TestDescribeKeys:
    def test_annotated_key(self):
        assert describe(Marked).keys == KeySpec(fields=("code",), namespace=None)

    def test_a_namespace_is_normalized_and_shared(self):
        assert describe(Namespaced).keys == KeySpec(fields=("code", "revision"), namespace="part")

    def test_every_key_type(self):
        assert describe(EveryKeyType).keys.fields == tuple(EveryKeyType.model_fields)

    def test_no_key_fields(self):
        assert describe(Unkeyed).keys == KeySpec(fields=(), namespace=None)

    @pytest.mark.parametrize(("model", "path", "reason"), REJECTED_KEYS, ids=lambda v: getattr(v, "__name__", None))
    def test_an_invalid_key_is_rejected_naming_the_field(self, model, path, reason):
        with raises_at(path) as error:
            describe(model)

        assert reason in str(error.value)[len(path) :]

    @pytest.mark.parametrize(
        ("key", "fields"),
        [("code", ("code",)), (["revision", "code"], ("code", "revision")), (("name",), ("name",))],
    )
    def test_key_parameter_names_root_fields_in_declaration_order(self, key, fields):
        assert describe(Unkeyed, key=key).keys == KeySpec(fields=fields, namespace=None)

    @pytest.mark.parametrize("key", ["code", ["code"], ("code",)])
    def test_key_parameter_agreeing_with_annotations(self, key):
        assert describe(Marked, key=key) == describe(Marked)

    def test_key_parameter_agreeing_keeps_the_annotated_namespace(self):
        assert describe(Namespaced, key=["revision", "code"]).keys == KeySpec(("code", "revision"), "part")

    @pytest.mark.parametrize("key", ["name", ["code", "name"]])
    def test_key_parameter_disagreeing_with_annotations_is_rejected(self, key):
        with raises_at("Marked") as error:
            describe(Marked, key=key)

        assert "must name the same fields" in str(error.value)
        assert "'code'" in str(error.value)

    @pytest.mark.parametrize("key", ["code", ["revision"], ["code", "revision", "name"]])
    def test_key_parameter_naming_a_subset_or_superset_of_the_marks_is_rejected(self, key):
        with raises_at("Namespaced") as error:
            describe(Namespaced, key=key)

        assert "must name the same fields" in str(error.value)

    def test_key_parameter_naming_an_unknown_field_is_rejected(self):
        with raises_at("Unkeyed") as error:
            describe(Unkeyed, key=["code", "missing"])

        assert "'missing'" in str(error.value)
        assert "not a field of Unkeyed" in str(error.value)

    def test_key_parameter_applies_to_the_root_only(self):
        # Line has a field named line_no, but key= only names Order's fields; Line keeps its Annotated key.
        with raises_at("Order") as error:
            describe(Order, key="line_no")

        assert "'line_no'" in str(error.value)

    def test_key_parameter_children_keep_their_annotations(self):
        spec = describe(Order, key="order_id")

        assert spec == ORDER_SPEC

    @pytest.mark.parametrize(
        ("key", "reason"),
        [
            ([], "names no field"),
            ((), "names no field"),
            ([1], "must be non-empty str field names"),
            ([""], "must be non-empty str field names"),
            ("", "must be non-empty str field names"),
            (["code", "code"], "more than once"),
            (b"code", "a field name, a sequence of field names, or None"),
            (5, "a field name, a sequence of field names, or None"),
            ({"code"}, "a field name, a sequence of field names, or None"),
        ],
    )
    def test_an_invalid_key_parameter_is_rejected(self, key, reason):
        with pytest.raises(RationalizationError, match=r"^key= ") as error:
            describe(Unkeyed, key=key)

        assert reason in str(error.value)

    @pytest.mark.parametrize(
        ("key", "path", "reason"),
        [
            ("ratio", "KeyedByParameter.ratio", "not float"),
            ("note", "KeyedByParameter.note", "must be required"),
            ("maybe", "KeyedByParameter.maybe", "must not be Optional"),
        ],
    )
    def test_key_parameter_fields_follow_the_key_rules(self, key, path, reason):
        model = create_model("KeyedByParameter", ratio=(float, ...), note=(str, "n"), maybe=(str | None, ...))

        with raises_at(path) as error:
            describe(model, key=key)

        assert reason in str(error.value)[len(path) :]


# ---------------------------------------------------------------------------------------------------------------
# check_json_schema: the supported JSON-schema subset
# ---------------------------------------------------------------------------------------------------------------


class TestPydanticShapes:
    """What pydantic 2.14 emits for the shapes the walk must accept: pinned so a change in pydantic shows up here."""

    @pytest.mark.parametrize("form", list(PINNED_SCALAR_JSON))
    def test_each_scalar_form_emits_its_pinned_json(self, form):
        annotation, pinned = PINNED_SCALAR_JSON[form]
        document = create_model("Pinned", value=(annotation, ...)).model_json_schema()
        node = document["properties"]["value"]
        if "$ref" in node:
            node = resolve(document, node["$ref"])

        assert {keyword: value for keyword, value in node.items() if keyword != "title"} == pinned

    def test_a_described_model_field_is_a_ref_with_a_sibling_description_not_all_of(self):
        document = DescribedChild.model_json_schema()

        assert document["properties"]["child"] == {"$ref": "#/$defs/Part", "description": "the part"}
        assert "allOf" not in json.dumps(document)

    def test_a_decimal_is_any_of_number_and_string(self):
        properties = Prices.model_json_schema()["properties"]

        assert properties["price"] == {"anyOf": [{"type": "number"}, {"type": "string"}], "title": "Price"}
        assert properties["discount"]["anyOf"] == [{"type": "number"}, {"type": "string"}, {"type": "null"}]

    def test_a_constrained_decimal_carries_its_constraints_in_the_number_branch(self):
        properties = ConstrainedPrices.model_json_schema()["properties"]

        assert properties["price"]["anyOf"] == [{"minimum": 0.0, "type": "number"}, {"type": "string"}]
        assert [member.get("type") for member in properties["discount"]["anyOf"]] == ["number", "string", "null"]

    @pytest.mark.parametrize(
        "model", [DescribedChild, Prices, ConstrainedPrices, Constrained, Order, EveryForm, EveryKeyType]
    )
    def test_the_walk_accepts_them(self, model):
        check_json_schema(model)


class TestCheckJsonSchema:
    @pytest.mark.parametrize(("schema", "under", "reason"), REJECTED_JSON, ids=lambda v: json.dumps(v)[:60])
    def test_an_unsupported_shape_is_rejected_naming_its_pointer(self, schema, under, reason):
        with raises_json("Tampered", f"#/properties/value{under}") as error:
            check_json_schema(tampered(schema))

        assert reason in str(error.value).partition(": ")[2]

    def test_a_dict_field(self):
        with raises_json("DictField", "#/properties/counts") as error:
            check_json_schema(DictField)

        assert "additionalProperties" in str(error.value)

    def test_a_tuple_field(self):
        with raises_json("TupleField", "#/properties/pair") as error:
            check_json_schema(TupleField)

        assert "'prefixItems'" in str(error.value)

    def test_an_any_field(self):
        with raises_json("AnyField", "#/properties/value") as error:
            check_json_schema(AnyField)

        assert "unconstrained" in str(error.value)

    def test_additional_properties_true_at_the_root(self):
        with raises_json("OpenRoot", "#"):
            check_json_schema(OpenRoot)

    def test_a_root_that_is_not_an_object(self):
        with raises_json("StringRoot", "#") as error:
            check_json_schema(StringRoot)

        assert "must be an object" in str(error.value)

    def test_an_unreferenced_def(self):
        with raises_json("Orphaned", "#/$defs/Orphan") as error:
            check_json_schema(Orphaned)

        assert "not referenced" in str(error.value)

    def test_a_pointer_escapes_slash_and_tilde(self):
        with raises_json("SlashAlias", "#/properties/a~1b~0c"):
            check_json_schema(SlashAlias)

    @pytest.mark.parametrize(
        ("model", "pointer"),
        [(Node, "#/$defs/Node/properties/children/items"), (Ping, "#/$defs/Pong/properties/ping")],
    )
    def test_a_ref_cycle_is_rejected_quickly(self, model, pointer):
        started = time.monotonic()
        with raises_json(model.__name__, pointer) as error:
            check_json_schema(model)

        assert time.monotonic() - started < FAST_S
        assert "$ref cycle" in str(error.value)

    def test_33_nested_objects_are_rejected_and_32_accepted(self):
        check_json_schema(chain(MAX_DEPTH))

        with raises_json("M1", f"#/$defs/M{MAX_DEPTH + 1}") as error:
            check_json_schema(chain(MAX_DEPTH + 1))

        assert "deeper than 32" in str(error.value)

    def test_an_array_counts_as_a_level(self):
        check_json_schema(chain(MAX_DEPTH - 1, leaf=list[str]))

        with raises_json("M1", f"#/$defs/M{MAX_DEPTH}/properties/value"):
            check_json_schema(chain(MAX_DEPTH, leaf=list[str]))

    def test_a_walk_past_the_node_bound_is_rejected_quickly(self):
        started = time.monotonic()
        with pytest.raises(RationalizationError, match=rf"^D16 JSON schema: more than {MAX_WALK_STEPS} nodes"):
            check_json_schema(doubling(16))

        assert time.monotonic() - started < FAST_S

    def test_a_model_pydantic_cannot_render_is_rejected_naming_it(self):
        with pytest.raises(RationalizationError, match=r"^NoJsonSchema: pydantic cannot build its JSON schema"):
            check_json_schema(NoJsonSchema)

    def test_the_model_is_checked_first(self):
        with pytest.raises(RationalizationError, match=r"^schema must be a pydantic BaseModel subclass"):
            check_json_schema({"type": "object"})


# ---------------------------------------------------------------------------------------------------------------
# response_schema: the wrapper
# ---------------------------------------------------------------------------------------------------------------


class TestResponseSchema:
    @pytest.mark.parametrize("model", [Order, Unkeyed, EveryForm, DescribedChild])
    def test_wraps_the_record_and_hoists_its_defs(self, model):
        assert response_schema(model) == expected_wrapper(model)

    def test_the_defs_are_hoisted_to_the_root(self):
        wrapper = response_schema(Order)
        record = wrapper["properties"]["records"]["items"]["properties"]["record"]

        assert set(wrapper["$defs"]) == {"Line", "Part"}
        assert "$defs" not in record
        assert record["title"] == "Order"

    def test_a_model_without_defs_has_none(self):
        assert "$defs" not in response_schema(Unkeyed)

    @pytest.mark.parametrize("model", [Order, EveryForm, DescribedChild])
    def test_every_ref_resolves_from_the_wrapper_root(self, model):
        wrapper = response_schema(model)
        refs = refs_in(wrapper)

        assert refs, "vacuous: the model has no $ref"
        for ref in refs:
            target = resolve(wrapper, ref)
            assert isinstance(target, dict), ref
            assert "type" in target or "enum" in target, ref

    def test_each_call_returns_a_fresh_structure(self):
        first = response_schema(Order)
        first["$defs"]["Part"]["properties"]["number"]["type"] = "integer"
        first["properties"]["records"]["items"]["properties"]["record"]["properties"].clear()

        second = response_schema(Order)

        assert second == expected_wrapper(Order)
        assert Order.model_json_schema()["$defs"]["Part"]["properties"]["number"]["type"] == "string"

    def test_shares_nothing_with_the_models_json_schema_extra(self):
        wrapper = response_schema(SharedExtras)
        record = wrapper["properties"]["records"]["items"]["properties"]["record"]
        record["examples"][0]["code"] = "changed"
        record["properties"]["code"]["x-meta"]["tags"].append("b")

        assert SHARED_EXAMPLES == [{"code": "AB"}]
        assert SharedExtras.model_fields["code"].json_schema_extra == {"x-meta": {"tags": ["a"]}}
        assert response_schema(SharedExtras) == expected_wrapper(SharedExtras)

    def test_the_model_is_checked_first(self):
        with pytest.raises(RationalizationError, match=r"^schema must be a pydantic BaseModel subclass"):
            response_schema(42)


# ---------------------------------------------------------------------------------------------------------------
# schema_sha256: the canonical hash
# ---------------------------------------------------------------------------------------------------------------

ITEM_FIELDS: dict[str, Any] = {
    "code": (Annotated[str, Key], ...),
    "quantity": (int, Field(description="how many")),
}


def item_hash(**fields: Any) -> str:
    """The wrapper hash of a model named ``Item`` with ``fields``, so the title never differs."""
    return schema_sha256(response_schema(create_model("Item", **fields)))


class TestSchemaSha256:
    def test_canonical_bytes(self):
        expected = hashlib.sha256('{"a":"é","b":[1,{"c":null,"d":true}]}'.encode()).hexdigest()

        assert schema_sha256({"b": [1, {"d": True, "c": None}], "a": "é"}) == expected

    def test_unchanged_by_insertion_order(self):
        assert schema_sha256({"a": 1, "b": {"c": 2, "d": 3}}) == schema_sha256({"b": {"d": 3, "c": 2}, "a": 1})

    def test_stable_across_calls_and_equal_models(self):
        assert schema_sha256(response_schema(Order)) == schema_sha256(response_schema(Order))
        assert item_hash(**ITEM_FIELDS) == item_hash(**ITEM_FIELDS)

    @pytest.mark.parametrize("seed", ["0", "1", "4242"])
    def test_stable_across_processes(self, seed, tmp_path):
        # Not -I, so the interpreter honours PYTHONHASHSEED (set and dict orders change with it); run from an
        # empty directory, so nothing local shadows an import. The models are this module's, written out again.
        code = textwrap.dedent(
            """
            from datetime import date
            from decimal import Decimal
            from typing import Annotated
            from pydantic import BaseModel
            from datagrunt.rationalization import Key
            from datagrunt.rationalization.schema import response_schema, schema_sha256

            class Part(BaseModel):
                number: Annotated[str, Key("Part")]
                name: str | None = None

            class Line(BaseModel):
                line_no: Annotated[int, Key]
                part: Part
                quantity: Decimal

            class Order(BaseModel):
                order_id: Annotated[str, Key]
                placed: date
                lines: list[Line]
                tags: list[str] = []

            print(schema_sha256(response_schema(Order)))
            """
        )
        env = {**os.environ, "PYTHONHASHSEED": seed}

        assert run_python(code, isolated=False, env=env, cwd=tmp_path) == schema_sha256(response_schema(Order))

    def test_changes_with_each_schema_edit(self):
        base = item_hash(**ITEM_FIELDS)
        added = item_hash(**ITEM_FIELDS, note=(str | None, None))
        renamed = item_hash(part_code=ITEM_FIELDS["code"], quantity=ITEM_FIELDS["quantity"])
        redescribed = item_hash(code=ITEM_FIELDS["code"], quantity=(int, Field(description="how many units")))
        retyped = item_hash(code=ITEM_FIELDS["code"], quantity=(Decimal, Field(description="how many")))

        assert len({base, added, renamed, redescribed, retyped}) == 5

    @pytest.mark.parametrize(
        "wrapper",
        [{"a": float("nan")}, {"a": float("inf")}, {"a": object()}, {"a": "\ud800"}, {1: "a", "b": 2}],
        ids=["nan", "inf", "object", "lone-surrogate", "mixed-keys"],
    )
    def test_a_value_that_is_not_canonical_json_is_rejected(self, wrapper):
        with pytest.raises(RationalizationError, match=r"^the response schema is not canonical JSON"):
            schema_sha256(wrapper)

    @pytest.mark.parametrize("wrapper", [[1, 2], "{}", None])
    def test_a_value_that_is_not_a_mapping_is_rejected(self, wrapper):
        with pytest.raises(RationalizationError, match=r"^the response schema must be a mapping"):
            schema_sha256(wrapper)


# ---------------------------------------------------------------------------------------------------------------
# prepare: the entry point, and the spec types' own checks
# ---------------------------------------------------------------------------------------------------------------


class TestPrepare:
    def test_prepares_every_part(self):
        prepared = prepare(Order)

        assert isinstance(prepared, PreparedSchema)
        assert prepared.model is Order
        assert prepared.spec == ORDER_SPEC
        assert prepared.wrapper == response_schema(Order)
        assert prepared.sha256 == schema_sha256(response_schema(Order))
        assert prepared.wrapper_json == json.dumps(
            response_schema(Order), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )

    def test_passes_key_to_describe(self):
        assert prepare(Unkeyed, key="code").spec == describe(Unkeyed, key="code")

    def test_the_wrapper_is_fresh_on_every_access(self):
        prepared = prepare(Order)
        first = prepared.wrapper
        first["$defs"].clear()

        assert prepared.wrapper == response_schema(Order)
        assert prepared.wrapper is not prepared.wrapper

    def test_is_frozen_and_slotted(self):
        prepared = prepare(Order)

        with pytest.raises(dataclasses.FrozenInstanceError):
            prepared.sha256 = "0" * 64
        assert not hasattr(prepared, "__dict__")

    def test_checks_run_in_order(self):
        # A class error before an annotation error, an annotation error before an agreement error, and that
        # before a JSON-schema error: an int narrowed by 'not' agrees with its type, then the walk refuses 'not'.
        with pytest.raises(RationalizationError, match=r"^schema must be a pydantic BaseModel subclass"):
            prepare(object())
        with raises_at("AnyField.value"):
            prepare(AnyField)
        with raises_at("Tampered.value"):
            prepare(tampered({"oneOf": [{"type": "integer"}]}))
        with raises_json("Tampered", "#/properties/value") as error:
            prepare(tampered({"type": "integer", "not": {"const": 3}}))

        assert "'not' is not supported" in str(error.value)

    def test_rejects_a_hash_that_does_not_match(self):
        prepared = prepare(Order)

        with pytest.raises(ValueError, match="sha256 does not match"):
            PreparedSchema(model=Order, spec=prepared.spec, wrapper_json=prepared.wrapper_json, sha256="0" * 64)

    def test_rejects_a_spec_of_another_model(self):
        prepared = prepare(Order)

        with pytest.raises(ValueError, match="spec describes Unkeyed, not Order"):
            PreparedSchema(
                model=Order, spec=describe(Unkeyed), wrapper_json=prepared.wrapper_json, sha256=prepared.sha256
            )


SCALAR = FieldSpec(
    name="code", wire_name="code", kind="scalar", optional=False, required=True, child=None, scalar=ScalarSpec(base=str)
)


class TestSpecTypes:
    @pytest.mark.parametrize(
        ("fields", "namespace", "error"),
        [
            (["code"], None, TypeError),
            (("code", 1), None, TypeError),
            (("code", ""), None, ValueError),
            (("code", "code"), None, ValueError),
            ((), "part", ValueError),
            (("code",), "Part", ValueError),
            (("code",), 1, TypeError),
        ],
    )
    def test_key_spec_rejects(self, fields, namespace, error):
        with pytest.raises(error):
            KeySpec(fields=fields, namespace=namespace)

    @pytest.mark.parametrize(
        ("kwargs", "error"),
        [
            ({"name": ""}, ValueError),
            ({"name": 1}, TypeError),
            ({"kind": "mapping"}, ValueError),
            ({"optional": 1}, TypeError),
            ({"kind": "model", "scalar": None}, ValueError),
            ({"kind": "model_list", "scalar": None}, ValueError),
            ({"child": PART_SPEC}, ValueError),
            ({"kind": "scalar_list", "optional": True}, ValueError),
            ({"kind": "model_list", "optional": True, "child": PART_SPEC, "scalar": None}, ValueError),
            ({"kind": "model", "child": "Part", "scalar": None}, TypeError),
            ({"scalar": None}, ValueError),
            ({"kind": "scalar_list", "scalar": None}, ValueError),
            ({"kind": "model", "child": PART_SPEC}, ValueError),
            ({"kind": "model_list", "child": PART_SPEC}, ValueError),
            ({"scalar": str}, TypeError),
            ({"scalar": "str"}, TypeError),
            ({"wire_name": 1}, TypeError),
            ({"wire_name": None}, TypeError),
            ({"wire_name": ""}, ValueError),
            ({"wire_name": " \t"}, ValueError),
        ],
    )
    def test_field_spec_rejects(self, kwargs, error):
        with pytest.raises(error):
            FieldSpec(
                **{
                    "name": "code",
                    "wire_name": "code",
                    "kind": "scalar",
                    "optional": False,
                    "required": True,
                    "child": None,
                    "scalar": ScalarSpec(str),
                    **kwargs,
                }
            )

    @pytest.mark.parametrize("required", [1, 0, None, "yes"])
    def test_field_spec_requires_a_bool_required(self, required):
        with pytest.raises(TypeError, match=r"^FieldSpec 'code': required must be a bool"):
            FieldSpec(
                name="code",
                wire_name="code",
                kind="scalar",
                optional=False,
                required=required,
                child=None,
                scalar=ScalarSpec(str),
            )

    @pytest.mark.parametrize("required", [True, False])
    def test_field_spec_keeps_required(self, required):
        field = FieldSpec(
            name="code",
            wire_name="code",
            kind="scalar",
            optional=True,
            required=required,
            child=None,
            scalar=ScalarSpec(str),
        )

        assert field.required is required

    @pytest.mark.parametrize(
        ("kind", "child", "scalar"),
        [
            ("scalar", None, ScalarSpec(base=int)),
            ("scalar_list", None, ScalarSpec(base=int)),
            ("model", PART_SPEC, None),
        ],
    )
    def test_field_spec_accepts(self, kind, child, scalar):
        field = FieldSpec(
            name="code", wire_name="code", kind=kind, optional=False, required=True, child=child, scalar=scalar
        )

        assert field.scalar == scalar

    @pytest.mark.parametrize(
        ("kwargs", "error"),
        [
            ({"model": int}, TypeError),
            ({"model": "Part"}, TypeError),
            ({"model": Part(number="PN-1")}, TypeError),
            ({"fields": ()}, ValueError),
            ({"fields": [SCALAR]}, TypeError),
            ({"fields": (SCALAR, "name")}, TypeError),
            ({"fields": (SCALAR, SCALAR)}, ValueError),
            ({"fields": (SCALAR, dataclasses.replace(SCALAR, name="other"))}, ValueError),
            ({"keys": KeySpec(fields=("missing",), namespace=None)}, ValueError),
            ({"keys": ("code",)}, TypeError),
            (
                {
                    "fields": (
                        FieldSpec(
                            "code", "code", "scalar", optional=True, required=True, child=None, scalar=ScalarSpec(str)
                        ),
                    )
                },
                ValueError,
            ),
            (
                {
                    "fields": (
                        FieldSpec(
                            "code",
                            "code",
                            "scalar_list",
                            optional=False,
                            required=True,
                            child=None,
                            scalar=ScalarSpec(str),
                        ),
                    )
                },
                ValueError,
            ),
        ],
    )
    def test_model_spec_rejects(self, kwargs, error):
        with pytest.raises(error):
            ModelSpec(
                **{"model": Part, "keys": KeySpec(fields=("code",), namespace=None), "fields": (SCALAR,), **kwargs}
            )


class TestScalarSpec:
    @pytest.mark.parametrize("form", list(SCALAR_FORMS))
    def test_accepts_what_describe_builds(self, form):
        scalar = SCALAR_FORMS[form][1]

        assert ScalarSpec(base=scalar.base, enum=scalar.enum, literal=scalar.literal, tz=scalar.tz) == scalar

    def test_defaults_to_neither_enum_nor_literal(self):
        scalar = ScalarSpec(base=str)

        assert (scalar.enum, scalar.literal, scalar.tz) == (None, None, None)

    def test_is_frozen_slotted_and_hashable(self):
        scalar = ScalarSpec(base=str, literal=("a", "b"))

        with pytest.raises(dataclasses.FrozenInstanceError):
            scalar.base = int
        assert not hasattr(scalar, "__dict__")
        assert hash(scalar) == hash(ScalarSpec(base=str, literal=("a", "b")))

    @pytest.mark.parametrize(
        ("kwargs", "error"),
        [
            ({"base": "str"}, TypeError),
            ({"base": Color.RED}, TypeError),
            ({"base": list}, ValueError),
            ({"base": object}, ValueError),
            ({"base": type(None)}, ValueError),
            ({"base": Color}, ValueError),
            ({"base": str, "enum": Color, "literal": ("red",)}, ValueError),
            ({"base": str, "enum": str}, TypeError),
            ({"base": str, "enum": Color.RED}, TypeError),
            ({"base": int, "enum": Color}, ValueError),
            ({"base": str, "enum": Level}, ValueError),
            ({"base": str, "enum": NoMembers}, ValueError),
            ({"base": bool, "enum": Answer}, ValueError),
            ({"base": float, "enum": Ratio}, ValueError),
            ({"base": str, "literal": ["a"]}, TypeError),
            ({"base": str, "literal": ()}, ValueError),
            ({"base": str, "literal": ("a", 1)}, ValueError),
            ({"base": int, "literal": (1, True)}, ValueError),
            ({"base": bool, "literal": (True, 1)}, ValueError),
            ({"base": date, "literal": (datetime(2026, 1, 1),)}, ValueError),
            ({"base": float, "literal": (1.5,)}, ValueError),
            ({"base": int, "enum": Perm}, ValueError),
            ({"base": int, "enum": IPerm}, ValueError),
            ({"base": datetime}, ValueError),
            ({"base": datetime, "tz": "utc"}, ValueError),
            ({"base": datetime, "tz": None}, ValueError),
            ({"base": str, "tz": "aware"}, ValueError),
            ({"base": date, "tz": "naive"}, ValueError),
        ],
        ids=lambda v: repr(v)[:50],
    )
    def test_rejects(self, kwargs, error):
        with pytest.raises(error):
            ScalarSpec(**kwargs)


# ---------------------------------------------------------------------------------------------------------------
# The review's cases: wire names, agreement with model_validate, defaults, datetimes, escapes, messages
# ---------------------------------------------------------------------------------------------------------------


def json_text(value: object) -> str:
    """``value`` as the agreement step's messages show JSON: sorted keys, default separators."""
    return json.dumps(value, sort_keys=True)


class TestScalarAgreement:
    """Each scalar's JSON must be what pydantic emits for its type: an override that differs is refused."""

    @pytest.mark.parametrize("holder", list(SCALAR_HOLDERS))
    @pytest.mark.parametrize("form", list(SCALAR_FORMS))
    def test_every_default_form_passes(self, form, holder):
        annotation = SCALAR_FORMS[form][0]
        definition = SCALAR_HOLDERS[holder][0]

        assert prepare(create_model("Holder", value=definition(annotation))).spec.fields[0].name == "value"

    @pytest.mark.parametrize("case", list(SCALAR_OVERRIDES))
    def test_an_overridden_scalar_json_is_rejected_naming_both(self, case):
        definition, path, expected, found = SCALAR_OVERRIDES[case]

        with raises_at(path) as error:
            prepare(create_model("Overridden", value=definition))

        message = str(error.value)[len(path) :]
        assert f"its JSON schema {json_text(found)} does not match" in message
        assert f"expected {json_text(expected)}" in message

    @pytest.mark.parametrize("case", list(NARROWING_OVERRIDES))
    def test_an_override_that_only_narrows_passes(self, case):
        assert prepare(create_model("Narrowed", value=NARROWING_OVERRIDES[case])).spec.fields[0].name == "value"

    def test_an_aliased_enum_matches_the_values_pydantic_lists(self):
        prepared = prepare(create_model("LegacyKeyed", code=(Annotated[Legacy, Key], ...), name=(str, ...)))

        assert prepared.spec.keys == KeySpec(fields=("code",), namespace=None)
        assert prepared.spec.fields[0].scalar == ScalarSpec(base=int, enum=Legacy)

    @pytest.mark.parametrize(
        ("definition", "kind"),
        [((int | SkipJsonSchema[None], None), "scalar"), ((Part | SkipJsonSchema[None], None), "model")],
        ids=["scalar", "model"],
    )
    def test_skip_json_schema_none_is_optional(self, definition, kind):
        # Pydantic's strict-mode idiom: None validates, but the JSON schema leaves the null branch out.
        prepared = prepare(create_model("StrictIdiom", value=definition))

        field = prepared.spec.fields[0]
        assert (field.kind, field.optional) == (kind, True)
        assert (
            "anyOf"
            not in prepared.wrapper["properties"]["records"]["items"]["properties"]["record"]["properties"]["value"]
        )

    @pytest.mark.parametrize("case", list(SIBLING_OVERRIDES))
    def test_a_value_keyword_beside_a_ref_or_an_optional_any_of_is_rejected(self, case):
        definition, placed = SIBLING_OVERRIDES[case]

        with raises_at("Overridden.value") as error:
            prepare(create_model("Overridden", value=definition))

        assert placed in str(error.value)


class TestRealisticSchemas:
    """Ordinary schemas a caller would write pass every check: over-strictness shows up here first."""

    @pytest.mark.parametrize("case", list(REALISTIC))
    def test_passes_and_a_record_shaped_like_its_json_schema_validates(self, case):
        model, keys, record = REALISTIC[case]

        started = time.monotonic()
        prepared = prepare(model)

        assert time.monotonic() - started < FAST_S
        assert prepared.spec.keys == keys
        assert model.model_validate(record) is not None


class TestClassRulesInDescribeAndPrepare:
    """describe() and prepare() check each model they reach as check_schema() does, before reading its fields."""

    @pytest.mark.parametrize(
        ("model", "path", "reason"),
        [
            (NoFields, "NoFields", "has no fields"),
            (HoldsNoFields, "HoldsNoFields.empty", "has no fields"),
            (Permissive, "Permissive", "extra='allow'"),
            (HoldsPermissive, "HoldsPermissive.children[]", "extra='allow'"),
            (Codes, "Codes", "is a RootModel"),
            (HoldsRootModel, "HoldsRootModel.codes", "is a RootModel"),
            (NotFullyDefined, "NotFullyDefined", "NotFullyDefined.model_rebuild()"),
            (Deferred, "Deferred", "defer_build=True"),
            (SerializationMode, "SerializationMode", "json_schema_mode_override='serialization'"),
            (EnumValues, "EnumValues", "use_enum_values=True"),
        ],
        ids=lambda v: getattr(v, "__name__", None),
    )
    def test_a_class_rule_is_enforced_naming_the_model(self, model, path, reason):
        for check in (describe, prepare):
            with raises_at(path) as error:
                check(model)

            assert reason in str(error.value)[len(path) :]


class TestWireNames:
    @pytest.mark.parametrize(
        ("model", "wire_names"),
        [
            (Aliases, ["Code", "Name", "Note", "quantity", "plain"]),
            (Camel, ["partNumber"]),
            (ByName, ["code"]),
            (Order, ["order_id", "placed", "lines", "tags"]),
        ],
    )
    def test_each_field_carries_the_name_model_validate_reads(self, model, wire_names):
        spec = prepare(model).spec

        assert [field.wire_name for field in spec.fields] == wire_names
        assert list(model.model_json_schema()["properties"]) == wire_names

    def test_an_alias_path_is_rejected(self):
        with raises_at("Pathed.code") as error:
            describe(Pathed)

        assert "AliasPath" in str(error.value)

    def test_alias_choices_led_by_a_path_are_rejected(self):
        with raises_at("ChoicePathFirst.code") as error:
            describe(ChoicePathFirst)

        assert "AliasChoices" in str(error.value)

    def test_two_fields_sharing_a_wire_name_are_rejected(self):
        with raises_at("SharedWireName.second") as error:
            describe(SharedWireName)

        assert "'x'" in str(error.value)
        assert "'first'" in str(error.value)

    def test_key_parameter_naming_an_alias_says_to_name_the_attribute(self):
        with raises_at("Aliases") as error:
            describe(Aliases, key="Code")

        assert "the alias of the field 'code'" in str(error.value)
        assert "key='code'" in str(error.value)


class TestAgreement:
    @pytest.mark.parametrize(("model", "path", "reason"), DISAGREEING, ids=lambda v: getattr(v, "__name__", None))
    def test_a_json_schema_that_disagrees_with_the_validator_is_rejected_naming_the_field(self, model, path, reason):
        with raises_at(path) as error:
            prepare(model)

        assert reason in str(error.value)[len(path) :]

    def test_a_json_schema_that_requires_a_defaulted_field_passes(self):
        # Requiring more than model_validate does narrows the wire; the other direction (RequiredDropped) is refused.
        prepared = prepare(RequiredAdded)

        assert prepared.wrapper["properties"]["records"]["items"]["properties"]["record"]["required"] == [
            "code",
            "note",
        ]

    def test_a_record_that_matches_an_agreeing_schema_validates(self):
        # The other side of the check: what the wire schema asks for is what model_validate accepts.
        record = {"Code": "PN-1", "Name": "bolt", "Note": "zinc", "quantity": 2, "plain": "x"}

        prepare(Aliases)

        assert Aliases.model_validate(record).code == "PN-1"


class TestDefaults:
    @pytest.mark.parametrize(
        ("model", "path", "reason"),
        [
            (NoneDefaultScalar, "NoneDefaultScalar.count", "annotate int | None"),
            (NoneDefaultList, "NoneDefaultList.tags", "give it the default []"),
            (NoneDefaultModel, "NoneDefaultModel.part", "annotate Part | None"),
        ],
    )
    def test_a_none_default_on_a_type_without_none_is_rejected(self, model, path, reason):
        with raises_at(path) as error:
            describe(model)

        assert "its default is None" in str(error.value)
        assert reason in str(error.value)[len(path) :]

    @pytest.mark.parametrize(("model", "path"), [(NanDefault, "NanDefault.ratio"), (InfDefault, "InfDefault.ratio")])
    def test_a_non_finite_default_names_its_field(self, model, path):
        with raises_at(path) as error:
            prepare(model)

        assert "default" in str(error.value)

    @pytest.mark.parametrize(
        ("model", "path"), [(NanExamples, "NanExamples.ratio"), (InfExamplesOnEnum, "InfExamplesOnEnum.color")]
    )
    def test_non_finite_examples_name_their_field(self, model, path):
        with raises_at(path) as error:
            prepare(model)

        assert "its examples" in str(error.value)

    def test_a_default_factory_is_never_called_so_its_value_is_unchecked(self):
        # The known limit the module notes document for consensus: pydantic does not validate defaults, so a
        # factory returning None (or a default of another type) reaches a validated record as is.
        assert describe(FactoryNone).fields[0].optional is False
        assert prepare(FactoryNone).model.model_validate({}).count is None


class TestDatetimes:
    def test_aware_and_naive_keys_are_accepted(self):
        fields = {field.name: field for field in describe(EveryKeyType).fields}

        assert fields["at"].scalar == ScalarSpec(base=datetime, tz="aware")
        assert fields["local"].scalar == ScalarSpec(base=datetime, tz="naive")
        assert {"at", "local"} <= set(describe(EveryKeyType).keys.fields)


class TestEscapingErrorsAreWrapped:
    @pytest.mark.parametrize(
        ("schema", "cause"),
        [({"$ref": "#/$defs/Missing"}, KeyError), ({"$ref": 5}, AttributeError)],
        ids=["missing-def", "non-str-ref"],
    )
    def test_pydantic_failures_building_the_schema(self, schema, cause):
        with pytest.raises(RationalizationError, match=r"^Tampered: pydantic cannot build its JSON schema") as error:
            check_json_schema(tampered(schema))

        assert isinstance(error.value.__cause__, cause)

    def test_a_schema_too_deep_for_pydantic(self):
        deep: dict = {"type": "string"}
        for _ in range(5_000):
            deep = {"anyOf": [deep, {"type": "null"}]}

        with pytest.raises(RationalizationError, match=r"^Tampered: pydantic cannot build its JSON schema") as error:
            check_json_schema(tampered(deep))

        assert isinstance(error.value.__cause__, RecursionError)

    def test_a_schema_too_deep_to_copy(self):
        class TooDeep(BaseModel):
            value: str

            @classmethod
            def model_json_schema(cls, *args: Any, **kwargs: Any) -> dict[str, Any]:
                nested: dict[str, Any] = {"type": "string"}
                for _ in range(100_000):
                    nested = {"anyOf": [nested, {"type": "null"}]}
                return nested

        with pytest.raises(RationalizationError, match=r"^TooDeep: its JSON schema is nested too deeply") as error:
            check_json_schema(TooDeep)

        assert isinstance(error.value.__cause__, RecursionError)

    def test_a_wrapper_too_deep_for_canonical_json(self):
        nested: dict = {}
        for _ in range(100_000):
            nested = {"a": nested}

        with pytest.raises(RationalizationError, match=r"^the response schema is not canonical JSON") as error:
            schema_sha256(nested)

        assert isinstance(error.value.__cause__, RecursionError)


class TestMessageOrder:
    @pytest.mark.parametrize(
        ("model", "path", "reason"),
        [
            (HoldsPermissiveDict, "HoldsPermissiveDict.counts", "unsupported type"),
            (HoldsPermissiveUnion, "HoldsPermissiveUnion.item", "two or more types"),
        ],
    )
    def test_the_annotation_is_named_before_a_model_it_names(self, model, path, reason):
        for check in (describe, prepare):
            with raises_at(path) as error:
                check(model)

            assert reason in str(error.value)[len(path) :]

    @pytest.mark.skipif(not hasattr(typing, "TypeAliasType"), reason="typing.TypeAliasType is Python 3.12+")
    def test_a_type_alias_is_named_as_one(self):
        alias = typing.TypeAliasType("PartNumber", str)  # what `type PartNumber = str` builds

        with raises_at("Aliased.value") as error:
            describe(create_model("Aliased", value=(alias, ...)))

        assert "type alias" in str(error.value)


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_nothing_inside_functions_and_nothing_from_core():
    tree = ast.parse(Path(schema_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert [module for module in modules if module.startswith("datagrunt.core")] == []
