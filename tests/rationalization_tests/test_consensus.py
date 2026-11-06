"""Tests for ``datagrunt.rationalization.consensus``: validation, quarantine and cross-family consensus.

Responses are built by hand as :class:`~datagrunt.rationalization.wire.ProviderResponse` objects and gathered into a
:class:`~datagrunt.rationalization.runner.RunOutcome`, exactly what the runner hands consensus; no provider runs.
"""

import dataclasses
import gc
import json
import math
import time
import warnings
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal

import pytest
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NaiveDatetime,
    create_model,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

from datagrunt.rationalization import Key, RationalizationError
from datagrunt.rationalization.consensus import (
    CONSENSUS_STATUSES,
    ERROR_MESSAGE_MAX_CHARS,
    QUARANTINE_REASONS,
    RAW_JSON_MAX_CHARS,
    SOURCE_ID_MAX_CHARS,
    SOURCE_IDS_MAX,
    ConsensusItem,
    ConsensusResult,
    QuarantinedItem,
    ValidatedResponse,
    ValidRecord,
    build_consensus,
    check_must_agree,
    key_order,
    normalized_key,
    validate_response,
)
from datagrunt.rationalization.runner import FailedUnit, RunOutcome
from datagrunt.rationalization.schema import prepare
from datagrunt.rationalization.wire import ProviderResponse

# ---------------------------------------------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------------------------------------------


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Component(BaseModel):
    part_number: Annotated[str, Key]
    quantity: int
    unit_price: float | None = None


class Assembly(BaseModel):
    assembly_id: Annotated[str, Key]
    title: str
    revision: int = 1
    components: list[Component] = []
    notes: list[str] = []


class Note(BaseModel):
    text: str
    page: int


class Summary(BaseModel):
    """A root without keys."""

    headline: str
    count: int


ASSEMBLY = prepare(Assembly)
SUMMARY = prepare(Summary)

GOOGLE = ("gemini:gemini-3", "google", "gemini")
ANTHROPIC = ("anthropic:claude", "anthropic", "anthropic")
OPENAI = ("openai:gpt", "openai", "openai-compatible")
GOOGLE_2 = ("google-cloud:p/l/google/gemma", "google", "gemini")
UNKNOWN = ("callable:tests.fake", "unknown", "callable")
UNKNOWN_2 = ("callable:tests.other", "unknown", "callable")


def entry(record: Any, quotes: Any = ("a quote",), ids: Any = ("p0001_e001",)) -> dict[str, Any]:
    """One wrapper entry around ``record``."""
    return {"record": record, "grounding_quotes": list(quotes), "source_element_ids": list(ids)}


def answer(*records: Any, finish: str = "complete", raw_text: str = "{}") -> ProviderResponse:
    """A complete response whose wrapper holds ``records``, each wrapped by :func:`entry`."""
    return ProviderResponse(
        content={"records": [entry(record) for record in records]},
        raw_text=raw_text,
        finish=finish,  # type: ignore[arg-type]
        input_tokens=None,
        output_tokens=None,
    )


def raw(content: Any, *, finish: str = "complete", raw_text: str = "") -> ProviderResponse:
    """A response holding ``content`` as given."""
    return ProviderResponse(
        content=content,
        raw_text=raw_text,
        finish=finish,  # type: ignore[arg-type]
        input_tokens=None,
        output_tokens=None,
    )


def outcome(
    responses: dict[tuple[str, str], ProviderResponse],
    models: tuple[tuple[str, str, str], ...],
    failed: tuple[FailedUnit, ...] = (),
) -> RunOutcome:
    """A run outcome of ``responses`` for ``models`` (ids, families, kinds)."""
    return RunOutcome(
        responses=responses,
        failed_units=failed,
        cache_hits=0,
        cache_misses=len(responses),
        cache_corrupt=0,
        cache_write_failures=0,
        usage={},
        models=models,
    )


def assembly(assembly_id: str = "A-1", title: str = "Pump", **extra: Any) -> dict[str, Any]:
    """An Assembly record as a model would send it."""
    return {"assembly_id": assembly_id, "title": title, **extra}


def quarantined(**overrides: Any) -> QuarantinedItem:
    fields: dict[str, Any] = {
        "model_id": "openai:gpt",
        "unit_id": "doc:p0001_e001-p0001_e004",
        "reason": "validation",
        "raw_json": "{}",
        "error_message": "title: missing",
        **overrides,
    }
    return QuarantinedItem(**fields)


# ---------------------------------------------------------------------------------------------------------------
# QuarantinedItem
# ---------------------------------------------------------------------------------------------------------------


class TestQuarantinedItem:
    def test_holds_its_fields_frozen_and_slotted(self):
        item = quarantined()

        assert (item.model_id, item.unit_id, item.reason, item.raw_json, item.error_message) == (
            "openai:gpt",
            "doc:p0001_e001-p0001_e004",
            "validation",
            "{}",
            "title: missing",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            item.reason = "refused"  # type: ignore[misc]
        assert not hasattr(item, "__dict__")

    def test_reasons_are_the_four(self):
        assert QUARANTINE_REASONS == ("validation", "unparseable", "truncated", "refused")
        for reason in QUARANTINE_REASONS:
            assert quarantined(reason=reason).reason == reason

    def test_raw_json_is_left_out_of_the_repr(self):
        assert "SECRET-DOCUMENT-TEXT" not in repr(quarantined(raw_json='{"title": "SECRET-DOCUMENT-TEXT"}'))

    def test_the_message_is_left_out_of_the_repr(self):
        message = "records[0]: 1 validation error: Patient SECRET-NAME: extra_forbidden"

        assert "SECRET" not in repr(quarantined(error_message=message))

    @pytest.mark.parametrize(
        ("overrides", "error"),
        [
            ({"model_id": 1}, TypeError),
            ({"model_id": " "}, ValueError),
            ({"unit_id": None}, TypeError),
            ({"unit_id": ""}, ValueError),
            ({"reason": "bad"}, ValueError),
            ({"reason": 1}, TypeError),
            ({"raw_json": None}, TypeError),
            ({"raw_json": "x" * (RAW_JSON_MAX_CHARS + 1)}, ValueError),
            ({"error_message": ""}, ValueError),
            ({"error_message": b"x"}, TypeError),
            ({"error_message": "x" * (ERROR_MESSAGE_MAX_CHARS + 1)}, ValueError),
        ],
    )
    def test_rejects(self, overrides, error):
        with pytest.raises(error):
            quarantined(**overrides)

    def test_source_element_ids_default_to_none_and_stay_out_of_the_repr(self):
        assert quarantined().source_element_ids == ()
        assert "SECRET" not in repr(quarantined(source_element_ids=("SECRET-ID",)))

    @pytest.mark.parametrize(
        ("ids", "error"),
        [
            (["e1"], TypeError),
            (("e1", 2), TypeError),
            (tuple(f"e{index}" for index in range(SOURCE_IDS_MAX + 1)), ValueError),
            (("e" * (SOURCE_ID_MAX_CHARS + 1),), ValueError),
        ],
    )
    def test_source_element_ids_are_bounded(self, ids, error):
        with pytest.raises(error):
            quarantined(source_element_ids=ids)

    def test_takes_an_empty_raw_json_and_texts_at_their_bounds(self):
        item = quarantined(raw_json="", error_message="x" * ERROR_MESSAGE_MAX_CHARS)

        assert item.raw_json == ""
        assert len(quarantined(raw_json="x" * RAW_JSON_MAX_CHARS).raw_json) == RAW_JSON_MAX_CHARS


# ---------------------------------------------------------------------------------------------------------------
# ValidRecord, ValidatedResponse
# ---------------------------------------------------------------------------------------------------------------


class TestValidRecordTypes:
    def test_valid_record_holds_its_fields(self):
        record = Assembly(assembly_id="A-1", title="Pump")

        valid = ValidRecord(record=record, grounding_quotes=("q",), source_element_ids=("e1",), position=0)

        assert (valid.record, valid.grounding_quotes, valid.source_element_ids, valid.position) == (
            record,
            ("q",),
            ("e1",),
            0,
        )

    @pytest.mark.parametrize(
        ("overrides", "error"),
        [
            ({"record": {"assembly_id": "A-1"}}, TypeError),
            ({"grounding_quotes": ["q"]}, TypeError),
            ({"grounding_quotes": ("q", 1)}, TypeError),
            ({"source_element_ids": ("e1", None)}, TypeError),
            ({"position": -1}, ValueError),
            ({"position": True}, TypeError),
            ({"position": 1.0}, TypeError),
        ],
    )
    def test_valid_record_rejects(self, overrides, error):
        fields: dict[str, Any] = {
            "record": Assembly(assembly_id="A-1", title="Pump"),
            "grounding_quotes": (),
            "source_element_ids": (),
            "position": 0,
            **overrides,
        }
        with pytest.raises(error):
            ValidRecord(**fields)

    @pytest.mark.parametrize(
        "fields",
        [
            {"records": [], "quarantined": ()},
            {"records": (), "quarantined": []},
            {"records": ("x",), "quarantined": ()},
            {"records": (), "quarantined": ("x",)},
        ],
    )
    def test_validated_response_rejects(self, fields):
        with pytest.raises(TypeError):
            ValidatedResponse(**fields)

    def test_validated_response_holds_tuples(self):
        assert ValidatedResponse(records=(), quarantined=(quarantined(),)).quarantined == (quarantined(),)


# ---------------------------------------------------------------------------------------------------------------
# validate_response: quarantine
# ---------------------------------------------------------------------------------------------------------------

UNIT = "doc:p0001_e001-p0001_e009"
MODEL = "openai:gpt"


def validate(response: ProviderResponse, schema=ASSEMBLY) -> ValidatedResponse:
    return validate_response(response, schema, unit_id=UNIT, model_id=MODEL)


class TestFinishQuarantine:
    @pytest.mark.parametrize("finish", ["truncated", "refused"])
    def test_a_cut_or_refused_response_is_one_item_never_parsed(self, finish):
        response = answer(assembly(), assembly("A-2"), finish=finish, raw_text='{"records": [{"record": ')

        validated = validate(response)

        assert validated.records == ()
        assert validated.quarantined == (
            QuarantinedItem(
                model_id=MODEL,
                unit_id=UNIT,
                reason=finish,
                raw_json='{"records": [{"record": ',
                error_message=validated.quarantined[0].error_message,
            ),
        )
        assert finish in validated.quarantined[0].error_message

    def test_without_text_raw_json_is_the_contents_json(self):
        response = raw({"records": []}, finish="refused", raw_text="")

        (item,) = validate(response).quarantined

        assert item.raw_json == '{"records":[]}'

    def test_without_text_or_content_raw_json_is_empty(self):
        (item,) = validate(raw(None, finish="truncated")).quarantined

        assert (item.reason, item.raw_json) == ("truncated", "")

    def test_raw_text_past_the_bound_is_cut_and_says_so(self):
        text = "x" * (RAW_JSON_MAX_CHARS * 2)

        (item,) = validate(raw(None, finish="truncated", raw_text=text)).quarantined

        assert len(item.raw_json) <= RAW_JSON_MAX_CHARS
        assert item.raw_json.startswith("x" * 1000)
        assert item.raw_json.endswith(f"[cut: {len(text)} characters]")


class TestValidateResponseArguments:
    @pytest.mark.parametrize(
        ("call", "error"),
        [
            (lambda: validate_response({"records": []}, ASSEMBLY, unit_id=UNIT, model_id=MODEL), TypeError),
            (lambda: validate_response(answer(), Assembly, unit_id=UNIT, model_id=MODEL), TypeError),
            (lambda: validate_response(answer(), ASSEMBLY, unit_id=1, model_id=MODEL), TypeError),
            (lambda: validate_response(answer(), ASSEMBLY, unit_id=" ", model_id=MODEL), ValueError),
            (lambda: validate_response(answer(), ASSEMBLY, unit_id=UNIT, model_id=""), ValueError),
        ],
    )
    def test_rejects(self, call, error):
        with pytest.raises(error):
            call()


class TestUnparseable:
    def test_no_content_is_one_unparseable_item(self):
        (item,) = validate(raw(None, raw_text="not json")).quarantined

        assert (item.reason, item.raw_json) == ("unparseable", "not json")
        assert "no JSON object" in item.error_message

    @pytest.mark.parametrize(
        "content",
        [
            {},
            {"items": []},
            {"records": {"record": {}}},
            {"records": "none"},
            {"records": [], "extra": 1},
        ],
    )
    def test_a_content_that_is_not_the_wrapper_is_one_unparseable_item(self, content):
        validated = validate(raw(content, raw_text="the text"))

        assert validated.records == ()
        (item,) = validated.quarantined
        assert (item.reason, item.raw_json) == ("unparseable", "the text")
        assert item.error_message.startswith('the response is not the wrapper {"records": [...]}: ')

    @pytest.mark.parametrize(
        "bad",
        [
            "a string",
            [assembly()],
            {"record": assembly(), "grounding_quotes": []},
            {"record": assembly(), "grounding_quotes": [], "source_element_ids": [], "extra": 1},
            {"record": [assembly()], "grounding_quotes": [], "source_element_ids": []},
            {"record": None, "grounding_quotes": [], "source_element_ids": []},
            {"record": assembly(), "grounding_quotes": "q", "source_element_ids": []},
            {"record": assembly(), "grounding_quotes": ["q", 1], "source_element_ids": []},
            {"record": assembly(), "grounding_quotes": [], "source_element_ids": [None]},
        ],
    )
    def test_a_bad_entry_is_an_unparseable_item_of_its_own(self, bad):
        content = {"records": [bad, bad]}

        validated = validate(raw(content, raw_text="the text"))

        assert [item.reason for item in validated.quarantined] == ["unparseable", "unparseable"]
        first, second = validated.quarantined
        assert first.raw_json == canonical(bad)
        assert first.error_message.startswith(
            'records[0] is not {"record": {...}, "grounding_quotes": [str, ...], "source_element_ids": [str, ...]}: '
        )
        assert second.error_message.startswith("records[1] is not ")


def canonical(value: Any) -> str:
    """The canonical JSON consensus keeps in raw_json."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class TestQuarantinedSources:
    def test_a_validation_item_keeps_the_entrys_element_ids(self):
        (item,) = validate(
            raw({"records": [entry(assembly(title=None), ids=["p0001_e002", "p0001_e003"])]})
        ).quarantined

        assert item.source_element_ids == ("p0001_e002", "p0001_e003")

    def test_a_bad_entry_keeps_its_element_ids_when_they_are_strings(self):
        good_ids = {"record": "not an object", "grounding_quotes": [], "source_element_ids": ["p0001_e004"]}
        bad_ids = {"record": assembly(), "grounding_quotes": [], "source_element_ids": [1]}

        first, second = validate(raw({"records": [good_ids, bad_ids]})).quarantined

        assert (first.source_element_ids, second.source_element_ids) == (("p0001_e004",), ())

    def test_a_whole_response_item_names_no_element(self):
        (item,) = validate(raw(None, finish="truncated")).quarantined

        assert item.source_element_ids == ()

    def test_the_ids_kept_are_bounded(self):
        ids = [f"p0001_e{index:04d}" for index in range(SOURCE_IDS_MAX + 44)] + ["x" * 1_000]

        (item,) = validate(raw({"records": [entry(assembly(title=None), ids=ids)]})).quarantined

        assert item.source_element_ids == tuple(ids[:SOURCE_IDS_MAX])
        (cut,) = validate(raw({"records": [entry(assembly(title=None), ids=["x" * 1_000])]})).quarantined
        assert len(cut.source_element_ids[0]) <= SOURCE_ID_MAX_CHARS
        assert cut.source_element_ids[0].endswith("[cut: 1000 characters]")


# ---------------------------------------------------------------------------------------------------------------
# validate_response: valid records and validation
# ---------------------------------------------------------------------------------------------------------------


class TestValidRecords:
    def test_a_valid_entry_is_a_valid_record_with_its_grounding_and_position(self):
        content = {
            "records": [
                "bad entry",
                entry(assembly(components=[{"part_number": "PN-1", "quantity": 2}]), ["q1", "q2"], ["e1"]),
            ]
        }

        validated = validate(raw(content))

        (valid,) = validated.records
        assert valid.record == Assembly(
            assembly_id="A-1", title="Pump", components=[Component(part_number="PN-1", quantity=2)]
        )
        assert (valid.grounding_quotes, valid.source_element_ids, valid.position) == (("q1", "q2"), ("e1",), 1)
        assert [item.reason for item in validated.quarantined] == ["unparseable"]

    def test_the_response_is_not_changed(self):
        response = answer(assembly(revision=None, components=[{"part_number": "PN-1", "quantity": 2}]))
        before = response.to_json()

        validate(response)

        assert response.to_json() == before

    def test_an_empty_records_list_yields_nothing(self):
        assert validate(raw({"records": []})) == ValidatedResponse(records=(), quarantined=())


class TestValidationQuarantine:
    def test_a_record_model_validate_refuses_is_a_validation_item(self):
        bad = assembly(title=None, components=[{"part_number": "PN-1", "quantity": "two"}])

        validated = validate(answer(assembly("A-0"), bad))

        assert [valid.record.assembly_id for valid in validated.records] == ["A-0"]
        (item,) = validated.quarantined
        assert (item.reason, item.raw_json) == ("validation", canonical(bad))
        assert (
            item.error_message
            == "records[1]: 2 validation errors: title: string_type; components[0].quantity: int_parsing"
        )

    def test_the_message_never_quotes_an_input_value(self):
        bad = assembly(title=["SECRET-VALUE"], revision="SECRET-NUMBER")

        (item,) = validate(answer(bad)).quarantined

        assert "SECRET" not in item.error_message
        assert "SECRET" in item.raw_json

    def test_a_long_message_and_a_long_record_are_bounded(self):
        bad = assembly(components=[{"part_number": "x" * 100, "quantity": "q"} for _ in range(2_000)])

        (item,) = validate(answer(bad)).quarantined

        assert len(item.error_message) <= ERROR_MESSAGE_MAX_CHARS
        assert item.error_message.endswith("characters]")
        assert len(item.raw_json) <= RAW_JSON_MAX_CHARS
        assert item.raw_json.endswith("characters]")


# ---------------------------------------------------------------------------------------------------------------
# validate_response: null = not shown
# ---------------------------------------------------------------------------------------------------------------


class Bin(BaseModel):
    code: Annotated[str, Key]
    capacity: int = 10
    label: str | None = "none given"


class Shelf(BaseModel):
    shelf_id: Annotated[str, Key]
    rows: int = Field(default=3, alias="rowCount")
    bins: list[Bin] = []
    primary: Bin | None = None
    spare: Bin = Field(default_factory=lambda: Bin(code="SPARE"))
    tags: list[str] = []
    maybe: int | None
    must: str


SHELF = prepare(Shelf)


def shelf(**fields: Any) -> dict[str, Any]:
    return {"shelf_id": "S-1", "maybe": 1, "must": "yes", **fields}


def validated_shelf(record: dict[str, Any]) -> Shelf:
    (valid,) = validate(answer(record), SHELF).records
    assert isinstance(valid.record, Shelf)
    return valid.record


class CamelLine(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True)
    part_number: Annotated[str, Key]
    unit_count: int = 3


class CamelOrder(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True)
    order_id: Annotated[str, Key]
    line_items: list[CamelLine] = []
    max_qty: int = 7


class PopulatedOrder(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    order_id: Annotated[str, Key]
    max_qty: int = 7


class AliasOnlyOrder(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    order_id: Annotated[str, Key]
    max_qty: int = 7


class TestNullIsNotShown:
    def test_a_null_in_a_defaulted_non_optional_field_takes_the_default(self):
        record = validated_shelf(shelf(rowCount=None, bins=None, tags=None, spare=None))

        assert (record.rows, record.bins, record.tags, record.spare) == (3, [], [], Bin(code="SPARE"))

    def test_a_null_in_an_optional_field_stays_none(self):
        record = validated_shelf(shelf(maybe=None, primary=None, bins=[{"code": "B", "label": None}]))

        assert (record.maybe, record.primary, record.bins[0].label) == (None, None, None)

    def test_a_null_in_a_required_non_optional_field_is_refused(self):
        (item,) = validate(answer(shelf(must=None)), SHELF).quarantined

        assert item.error_message == "records[0]: 1 validation error: must: string_type"

    def test_child_records_drop_their_unshown_nulls_by_wire_name(self):
        record = validated_shelf(
            shelf(
                bins=[{"code": "B1", "capacity": None, "label": None}, {"code": "B2", "capacity": 4}],
                primary={"code": "P", "capacity": None},
                spare={"code": "S", "capacity": None},
            )
        )

        assert [(item.code, item.capacity, item.label) for item in record.bins] == [
            ("B1", 10, None),
            ("B2", 4, "none given"),
        ]
        assert (record.primary, record.spare) == (Bin(code="P"), Bin(code="S"))

    def test_the_attribute_name_is_not_the_wire_name(self):
        (item,) = validate(answer(shelf(rows=None, rowCount="three")), SHELF).quarantined

        assert item.error_message == "records[0]: 1 validation error: rowCount: int_parsing"

    def test_a_null_list_item_is_left_for_validation(self):
        (item,) = validate(answer(shelf(tags=["a", None], bins=[None])), SHELF).quarantined

        assert item.error_message == "records[0]: 2 validation errors: bins[0]: model_type; tags[1]: string_type"

    def test_a_child_that_is_not_an_object_is_left_for_validation(self):
        (item,) = validate(answer(shelf(spare="S", bins="B")), SHELF).quarantined

        assert item.error_message == "records[0]: 2 validation errors: bins: list_type; spare: model_type"

    def test_a_model_that_validates_by_name_drops_a_null_under_the_attribute_name(self):
        record = {"orderId": "O", "max_qty": None, "line_items": [{"part_number": "P", "unit_count": None}]}

        (valid,) = validate(answer(record), prepare(CamelOrder)).records

        assert valid.record == CamelOrder(order_id="O", max_qty=7, line_items=[CamelLine(part_number="P")])

    def test_populate_by_name_counts_as_validating_by_name(self):
        (valid,) = validate(answer({"orderId": "O", "max_qty": None}), prepare(PopulatedOrder)).records

        assert valid.record.max_qty == 7

    def test_a_model_that_reads_aliases_only_ignores_the_attribute_name(self):
        (valid,) = validate(answer({"orderId": "O", "max_qty": "not read"}), prepare(AliasOnlyOrder)).records

        assert valid.record.max_qty == 7

    def test_raw_json_keeps_the_nulls_the_model_sent(self):
        bad = shelf(must=None, rowCount=None)

        (item,) = validate(answer(bad), SHELF).quarantined

        assert item.raw_json == canonical(bad)


# ---------------------------------------------------------------------------------------------------------------
# validate_response: defaults pydantic never validated
# ---------------------------------------------------------------------------------------------------------------


class Level(Enum):
    LOW = 1
    HIGH = 2


class Spare(Bin):
    """A subclass: not exactly the field's class."""


UTC = timezone.utc
NAIVE = datetime(2026, 1, 2, 3, 4, 5)
AWARE = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def made(value: Any) -> Any:
    """A ``default_factory`` returning ``value``: prepare() cannot check it (it is never called there), so its value
    reaches a validated record unchecked, the case consensus's post-validation check exists for."""
    return Field(default_factory=lambda: value)


# Each case: a field whose default pydantic never validates, and the problem consensus names.
BAD_DEFAULTS = {
    "factory-none": ((int, made(None)), "value: holds None, but the field is not Optional"),
    "str-for-int": ((int, made("oops")), "value: holds str, but the field holds int"),
    "bool-for-int": ((int, made(True)), "value: holds bool, but the field holds int"),
    "int-for-bool": ((bool, made(1)), "value: holds int, but the field holds bool"),
    "float-for-decimal": ((Decimal, made(1.5)), "value: holds float, but the field holds Decimal"),
    "datetime-for-date": ((date, made(NAIVE)), "value: holds datetime, but the field holds date"),
    "naive-for-aware": ((AwareDatetime, made(NAIVE)), "value: holds datetime, but the field holds an aware datetime"),
    "aware-for-naive": ((NaiveDatetime, made(AWARE)), "value: holds datetime, but the field holds a naive datetime"),
    "value-for-enum": ((Color, made("red")), "value: holds str, but the field holds Color"),
    "int-for-int-enum": ((Level, made(1)), "value: holds int, but the field holds Level"),
    "unlisted-literal": ((Literal["a", "b"], made("c")), "value: holds str, but the field holds Literal['a', 'b']"),
    "bool-for-int-literal": ((Literal[1, 2], made(True)), "value: holds bool, but the field holds Literal[1, 2]"),
    "optional-wrong-type": ((int | None, made("x")), "value: holds str, but the field holds int"),
    "list-factory-none": (
        (list[str], Field(default_factory=lambda: None)),
        "value: holds None, but the field holds a list",
    ),
    "list-item": ((list[str], made(["a", 1])), "value[1]: holds int, but the field holds str"),
    "model-factory-none": (
        (Bin, Field(default_factory=lambda: None)),
        "value: holds None, but the field is not Optional",
    ),
    "model-subclass": (
        (Bin, Field(default_factory=lambda: Spare(code="S"))),
        "value: holds Spare, but the field holds Bin",
    ),
    "model-list-factory-none": (
        (list[Bin], Field(default_factory=lambda: None)),
        "value: holds None, but the field holds a list",
    ),
    "model-list-item": (
        (list[Bin], Field(default_factory=lambda: [Bin(code="B"), Spare(code="S")])),
        "value[1]: holds Spare, but the field holds Bin",
    ),
}


def holder(annotation: Any, default: Any) -> type[BaseModel]:
    """A keyed model whose ``value`` field has ``annotation`` and ``default``."""
    namespace = {"__annotations__": {"code": Annotated[str, Key], "value": annotation}, "value": default}
    return type("Holder", (BaseModel,), namespace)


# Defaults an int widens to losslessly: an int in a float field within 2**53 (every such int is exactly a float),
# any int in a Decimal field.
WIDENED_DEFAULTS = {
    "int-for-float": (float, 0),
    "int-at-the-float-bound": (float, 2**53),
    "negative-int-at-the-float-bound": (float, -(2**53)),
    "int-for-optional-float": (float | None, 3),
    "int-for-decimal": (Decimal, 2),
    "huge-int-for-decimal": (Decimal, 10**40),
    "ints-for-float-list": (list[float], [0, 1.5, -2]),
    "ints-for-decimal-list": (list[Decimal], [1, 2]),
}

# Ints that do not widen losslessly, and bools, which are never numbers here.
UNWIDENED_DEFAULTS = {
    "int-past-the-float-bound": (
        (float, made(2**53 + 1)),
        "value: holds int, but the field holds float (or an int within 2**53 of 0)",
    ),
    "negative-int-past-the-float-bound": (
        (float, made(-(2**53) - 1)),
        "value: holds int, but the field holds float (or an int within 2**53 of 0)",
    ),
    "bool-for-float": ((float, made(True)), "value: holds bool, but the field holds float"),
    "bool-for-decimal": ((Decimal, made(False)), "value: holds bool, but the field holds Decimal"),
    "float-for-int": ((int, made(1.0)), "value: holds float, but the field holds int"),
    "decimal-for-float": ((float, made(Decimal("1"))), "value: holds Decimal, but the field holds float"),
    "big-int-in-float-list": (
        (list[float], made([1, 2**53 + 1])),
        "value[1]: holds int, but the field holds float (or an int within 2**53 of 0)",
    ),
}


class Packed(BaseModel):
    code: Annotated[str, Key]
    qty: int = 0


class Crate(BaseModel):
    crate_id: Annotated[str, Key]
    packed: Packed = Field(default_factory=Packed.model_construct)


LOOKUP = {"a": "Alpha"}


class Coded(BaseModel):
    coded_id: Annotated[str, Key]
    code: str

    @field_validator("code")
    @classmethod
    def look_up(cls, value: str) -> str:
        return LOOKUP[value]  # a KeyError, which pydantic does not wrap


class Halting(BaseModel):
    halting_id: Annotated[str, Key]
    code: str

    @field_validator("code")
    @classmethod
    def halt(cls, value: str) -> str:
        raise KeyboardInterrupt


class Combined(BaseModel):
    combined_id: Annotated[str, Key]
    a: int
    b: int
    c: int

    @model_validator(mode="after")
    def not_all_ones(self) -> "Combined":
        if self.a == self.b == self.c == 1:
            raise LookupError("SECRET-COMBINATION")
        return self


class TestValidatorExceptions:
    def test_a_validator_raising_another_exception_quarantines_naming_its_class_only(self):
        validated = validate(
            answer({"coded_id": "C", "code": "a"}, {"coded_id": "D", "code": "SECRET-zzz"}), prepare(Coded)
        )

        assert [valid.record.code for valid in validated.records] == ["Alpha"]
        (item,) = validated.quarantined
        assert item.reason == "validation"
        assert item.error_message == (
            "records[1]: model_validate raised KeyError (from a validator of the schema; its message is not kept)"
        )

    def test_a_rebuild_raising_another_exception_is_a_note_naming_its_class_only(self):
        records = ({"a": 1, "b": 1, "c": 0}, {"a": 1, "b": 0, "c": 1}, {"a": 0, "b": 1, "c": 1})
        responses = {
            (U1, model): answer({"combined_id": "X", **record}) for model, record in zip(THREE, records, strict=True)
        }

        found = only(blend(responses, THREE, schema=prepare(Combined)))

        assert (found.status, found.record) == ("field_disagreement", None)
        assert found.notes == (
            "the merged record failed validation: model_validate raised LookupError (from a validator of the schema; "
            "its message is not kept)",
        )

    @pytest.mark.parametrize("error", [MemoryError, RecursionError])
    def test_memory_and_recursion_errors_from_a_validator_propagate(self, error):
        def explode(value: str) -> str:
            raise error

        model = create_model(
            "Exploding", exploding_id=(Annotated[str, Key], ...), code=(Annotated[str, AfterValidator(explode)], ...)
        )

        with pytest.raises(error):
            validate(answer({"exploding_id": "E", "code": "x"}), prepare(model))

    @pytest.mark.parametrize("error", [MemoryError, RecursionError])
    def test_memory_and_recursion_errors_from_the_rebuild_propagate(self, error):
        class Rebuilt(BaseModel):
            rebuilt_id: Annotated[str, Key]
            a: int
            b: int
            c: int

            @model_validator(mode="after")
            def not_all_ones(self) -> "Rebuilt":
                if self.a == self.b == self.c == 1:
                    raise error
                return self

        records = ({"a": 1, "b": 1, "c": 0}, {"a": 1, "b": 0, "c": 1}, {"a": 0, "b": 1, "c": 1})
        responses = {
            (U1, model): answer({"rebuilt_id": "X", **record}) for model, record in zip(THREE, records, strict=True)
        }

        with pytest.raises(error):
            blend(responses, THREE, schema=prepare(Rebuilt))

    def test_an_interrupt_is_never_caught(self):
        with pytest.raises(KeyboardInterrupt):
            validate(answer({"halting_id": "H", "code": "x"}), prepare(Halting))


DEFAULT_HINT = " (a default: pydantic does not validate defaults, so fix the field's default)"


class TestNonFiniteValues:
    @pytest.mark.parametrize("sent", ["Infinity", "-Infinity", "inf", "NaN"])
    def test_a_non_finite_float_the_model_sent_is_quarantined(self, sent):
        (item,) = validate(answer(reading(value=sent)), READING).quarantined

        assert item.error_message.startswith("records[0]: value: holds a non-finite float")

    def test_a_non_finite_float_in_a_list_names_its_position(self):
        model = holder(list[float], [])

        (item,) = validate(answer({"code": "C", "value": [1.5, "inf"]}), prepare(model)).quarantined

        assert item.error_message.startswith("records[0]: value[1]: holds a non-finite float")

    def test_a_non_finite_decimal_a_factory_gave_is_quarantined(self):
        model = holder(Decimal, made(Decimal("NaN")))

        (item,) = validate(answer({"code": "C"}), prepare(model)).quarantined

        assert item.error_message.startswith("records[0]: value: holds a non-finite Decimal")

    def test_a_value_the_model_sent_is_not_blamed_on_a_default(self):
        (item,) = validate(answer(reading(value="Infinity")), READING).quarantined

        assert item.error_message == "records[0]: value: holds a non-finite float"

    def test_a_value_a_default_gave_says_so(self):
        (item,) = validate(answer({"code": "C"}), prepare(holder(Decimal, made(Decimal("NaN"))))).quarantined

        assert item.error_message == f"records[0]: value: holds a non-finite Decimal{DEFAULT_HINT}"

    def test_each_problem_says_whether_a_default_gave_it(self):
        class Mixed(BaseModel):
            mixed_id: Annotated[str, Key]
            sent: float
            count: int = made("x")

        (item,) = validate(answer({"mixed_id": "M", "sent": "NaN"}), prepare(Mixed)).quarantined

        assert item.error_message == (
            f"records[0]: sent: holds a non-finite float; count: holds str, but the field holds int{DEFAULT_HINT}"
        )

    def test_a_default_inside_a_child_the_model_sent_says_so(self):
        class Inner(BaseModel):
            code: Annotated[str, Key]
            count: int = made("none")

        class Outer(BaseModel):
            outer_id: Annotated[str, Key]
            inner: list[Inner] = []

        record = {"outer_id": "O", "inner": [{"code": "I-1"}]}
        (item,) = validate(answer(record), prepare(Outer)).quarantined

        assert item.error_message == f"records[0]: inner[0].count: holds str, but the field holds int{DEFAULT_HINT}"

    def test_finite_values_pass(self):
        assert len(validate(answer(reading(value=-1.5e300)), READING).records) == 1


class TestMissingFields:
    def test_a_default_built_without_a_field_is_quarantined_not_a_crash(self):
        responses = {(U1, GOOGLE): answer({"crate_id": "C"}), (U1, ANTHROPIC): answer({"crate_id": "C"})}

        result = blend(responses, schema=prepare(Crate))

        assert result.items == ()
        assert [found.reason for found in result.quarantined] == ["validation", "validation"]
        assert result.quarantined[0].error_message.startswith(
            "records[0]: packed.code: is missing (the instance was built without it, as model_construct() can)"
        )

    def test_a_record_that_sends_the_child_is_valid(self):
        (valid,) = validate(answer({"crate_id": "C", "packed": {"code": "P"}}), prepare(Crate)).records

        assert valid.record == Crate(crate_id="C", packed=Packed(code="P"))


class TestWidenedDefaults:
    @pytest.mark.parametrize("case", list(WIDENED_DEFAULTS))
    def test_an_int_that_widens_losslessly_is_valid(self, case):
        annotation, default = WIDENED_DEFAULTS[case]

        validated = validate(answer({"code": "C-1"}), prepare(holder(annotation, default)))

        assert validated.quarantined == ()
        (valid,) = validated.records
        assert valid.record.value == default

    @pytest.mark.parametrize("case", list(UNWIDENED_DEFAULTS))
    def test_an_int_that_does_not_widen_losslessly_or_a_bool_is_refused(self, case):
        (annotation, default), problem = UNWIDENED_DEFAULTS[case]

        (item,) = validate(answer({"code": "C-1"}), prepare(holder(annotation, default))).quarantined

        assert item.error_message.startswith(f"records[0]: {problem}")

    def test_a_widened_decimal_dumps_its_vote_without_a_warning(self):
        model = prepare(holder(Decimal, 2))

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            found = only(blend({(U1, OPENAI): answer({"code": "C-1"})}, (OPENAI,), schema=model))

        assert dict(found.model_votes) == {OPENAI[0]: ({"code": "C-1", "value": 2},)}


class TestUnvalidatedDefaults:
    @pytest.mark.parametrize("case", list(BAD_DEFAULTS))
    def test_a_default_of_the_wrong_kind_is_quarantined(self, case):
        (annotation, default), problem = BAD_DEFAULTS[case]
        model = holder(annotation, default)

        validated = validate(answer({"code": "C-1"}), prepare(model))

        assert validated.records == ()
        (item,) = validated.quarantined
        assert item.reason == "validation"
        assert item.error_message.startswith(f"records[0]: {problem}")
        assert item.raw_json == '{"code":"C-1"}'

    def test_a_bad_default_in_a_child_record_names_its_path(self):
        class Inner(BaseModel):
            code: Annotated[str, Key]
            count: int = made("none")

        class Outer(BaseModel):
            outer_id: Annotated[str, Key]
            inner: list[Inner] = []

        (item,) = validate(
            answer({"outer_id": "O", "inner": [{"code": "I-1", "count": 2}, {"code": "I-2"}]}), prepare(Outer)
        ).quarantined

        assert item.error_message.startswith("records[0]: inner[1].count: holds str, but the field holds int")

    def test_every_value_validation_gives_passes(self):
        class Every(BaseModel):
            code: Annotated[str, Key]
            text: str
            count: int
            ratio: float
            flag: bool
            amount: Decimal
            day: date
            at: AwareDatetime
            local: NaiveDatetime
            color: Color
            level: Level
            letter: Literal["a", "b"]
            number: Literal[1, 2]
            truth: Literal[True]
            maybe: int | None = None
            maybe_bin: Bin | None = None
            texts: list[str] = []
            bins: list[Bin] = []
            spare: Bin = Field(default_factory=lambda: Bin(code="S"))
            ratio_default: float = 0.5

        record = {
            "code": "C",
            "text": "t",
            "count": 2.0,
            "ratio": 1,
            "flag": True,
            "amount": 1.5,
            "day": "2026-01-02",
            "at": "2026-01-02T03:04:05+01:00",
            "local": "2026-01-02T03:04:05",
            "color": "red",
            "level": 2,
            "letter": "b",
            "number": 1,
            "truth": True,
            "maybe": None,
            "maybe_bin": {"code": "M"},
            "texts": ["x"],
            "bins": [{"code": "B"}],
        }

        validated = validate(answer(record), prepare(Every))

        assert validated.quarantined == ()
        assert len(validated.records) == 1


# ---------------------------------------------------------------------------------------------------------------
# ConsensusItem, ConsensusResult
# ---------------------------------------------------------------------------------------------------------------

VOTE = {"assembly_id": "A-1", "title": "Pump", "revision": 1, "components": [], "notes": []}


def item(**overrides: Any) -> ConsensusItem:
    fields: dict[str, Any] = {
        "unit_id": UNIT,
        "status": "consensus",
        "key": ("A-1",),
        "record": Assembly(assembly_id="A-1", title="Pump"),
        "disputed_fields": (),
        "notes": (),
        "model_votes": {"m1": (VOTE,), "m2": (VOTE,)},
        "quotes_by_model": {"m1": ("q",), "m2": ()},
        "element_ids_by_model": {"m1": ("e1",), "m2": ("e1",)},
        "families": {"m1": "google", "m2": "anthropic"},
        **overrides,
    }
    return ConsensusItem(**fields)


class TestConsensusItem:
    def test_holds_its_fields_and_read_only_mappings(self):
        votes = {"m1": (VOTE,), "m2": (VOTE,)}
        built = item(model_votes=votes)

        assert built.status == "consensus"
        assert dict(built.model_votes) == votes
        votes["m3"] = ()
        assert list(built.model_votes) == ["m1", "m2"]
        with pytest.raises(TypeError):
            built.model_votes["m3"] = ()  # type: ignore[index]
        with pytest.raises(dataclasses.FrozenInstanceError):
            built.status = "unmatched"  # type: ignore[misc]

    def test_statuses_are_the_five(self):
        assert CONSENSUS_STATUSES == (
            "consensus",
            "single_model",
            "missing_in_some_models",
            "field_disagreement",
            "unmatched",
        )

    def test_model_chosen_paths_and_notes_are_left_out_of_the_repr(self):
        disputed = item(
            status="field_disagreement",
            record=None,
            disputed_fields=("components['SECRET-KEY'].quantity",),
            notes=("the merged record failed validation: 1 validation error: SECRET-EXTRA: extra_forbidden",),
        )

        assert "SECRET" not in repr(disputed)
        assert disputed.disputed_fields == ("components['SECRET-KEY'].quantity",)

    def test_document_text_is_left_out_of_the_repr(self):
        text = repr(
            item(record=Assembly(assembly_id="A-1", title="SECRET"), quotes_by_model={"m1": ("SECRET",), "m2": ()})
        )

        assert "SECRET" not in text

    @pytest.mark.parametrize(
        ("overrides", "error"),
        [
            ({"unit_id": ""}, ValueError),
            ({"status": "maybe"}, ValueError),
            ({"key": ["A-1"]}, TypeError),
            ({"record": VOTE}, TypeError),
            ({"disputed_fields": ["title"]}, TypeError),
            ({"disputed_fields": ("",), "status": "field_disagreement"}, ValueError),
            ({"notes": ("ok", 1)}, TypeError),
            ({"model_votes": [("m1", (VOTE,))]}, TypeError),
            ({"model_votes": {"m1": [VOTE], "m2": (VOTE,)}}, TypeError),
            ({"model_votes": {"m1": ("x",), "m2": (VOTE,)}}, TypeError),
            ({"model_votes": {1: (VOTE,), "m2": (VOTE,)}}, TypeError),
            ({"quotes_by_model": {"m1": ["q"], "m2": ()}}, TypeError),
            ({"element_ids_by_model": {"m1": (1,), "m2": ()}}, TypeError),
            ({"families": {"m1": "google", "m2": 2}}, TypeError),
            ({"families": {"m1": "google"}}, ValueError),
            ({"quotes_by_model": {"m2": (), "m1": ()}}, ValueError),
            ({"model_votes": {"m1": (), "m2": ()}}, ValueError),
            ({"model_votes": {}, "quotes_by_model": {}, "element_ids_by_model": {}, "families": {}}, ValueError),
            ({"record": None}, ValueError),
            ({"disputed_fields": ("title",)}, ValueError),
            ({"notes": ("a note",)}, ValueError),
            ({"status": "field_disagreement"}, ValueError),
            ({"status": "single_model"}, ValueError),
            ({"status": "unmatched"}, ValueError),
        ],
    )
    def test_rejects(self, overrides, error):
        with pytest.raises(error):
            item(**overrides)

    def test_takes_each_status_in_its_shape(self):
        one = {"m1": (VOTE,)}
        single = item(
            status="single_model",
            model_votes=one,
            quotes_by_model={"m1": ()},
            element_ids_by_model={"m1": ()},
            families={"m1": "google"},
        )
        unmatched = item(status="unmatched", record=None, key=(), model_votes={"m1": (VOTE,), "m2": ()})
        disputed = item(status="field_disagreement", record=None, disputed_fields=("title",))
        noted = item(status="field_disagreement", notes=("the merged record failed validation",))
        missing = item(status="missing_in_some_models", model_votes={"m1": (VOTE,), "m2": ()})

        assert [found.status for found in (single, unmatched, disputed, noted, missing)] == [
            "single_model",
            "unmatched",
            "field_disagreement",
            "field_disagreement",
            "missing_in_some_models",
        ]


class TestConsensusResult:
    def test_holds_tuples(self):
        result = ConsensusResult(items=(item(),), quarantined=(quarantined(),))

        assert (result.items, result.quarantined) == ((item(),), (quarantined(),))

    @pytest.mark.parametrize(
        "fields",
        [
            lambda: {"items": [item()], "quarantined": ()},
            lambda: {"items": (), "quarantined": [quarantined()]},
            lambda: {"items": (quarantined(),), "quarantined": ()},
            lambda: {"items": (), "quarantined": (item(),)},
        ],
    )
    def test_rejects(self, fields):
        built = fields()
        with pytest.raises(TypeError):
            ConsensusResult(**built)


# ---------------------------------------------------------------------------------------------------------------
# check_must_agree, normalized_key
# ---------------------------------------------------------------------------------------------------------------


class TestCheckMustAgree:
    def test_returns_the_paths_it_checked(self):
        paths = ["title", "components", "components.quantity", "notes"]

        assert check_must_agree(ASSEMBLY.spec, paths) == tuple(paths)
        assert check_must_agree(ASSEMBLY.spec, ()) == ()

    def test_paths_are_attribute_names_through_models(self):
        assert check_must_agree(SHELF.spec, ("rows", "primary.capacity", "bins.label", "spare")) == (
            "rows",
            "primary.capacity",
            "bins.label",
            "spare",
        )

    @pytest.mark.parametrize(
        ("paths", "message"),
        [
            ("title", "must_agree must be a sequence of dotted field paths, not a str"),
            (None, "must_agree must be a sequence of dotted field paths, got None"),
            ({"title"}, "must_agree must be a sequence of dotted field paths, got set"),
            ((1,), "must_agree entries must be non-empty str paths, got 1"),
            (("",), "must_agree entries must be non-empty str paths, got ''"),
            (("components..quantity",), "must_agree path 'components..quantity' has an empty part"),
            (("title", "title"), "must_agree names 'title' more than once"),
            (("nope",), "must_agree path 'nope': Assembly has no field 'nope' (its fields: assembly_id, title,"),
            (("components.nope",), "must_agree path 'components.nope': Component has no field 'nope'"),
            (("title.length",), "must_agree path 'title.length': 'title' holds no model, so nothing is under it"),
        ],
    )
    def test_rejects(self, paths, message):
        with pytest.raises(RationalizationError) as error:
            check_must_agree(ASSEMBLY.spec, paths)

        assert str(error.value).startswith(message)

    def test_a_wire_name_is_not_a_path(self):
        with pytest.raises(RationalizationError, match="Shelf has no field 'rowCount'"):
            check_must_agree(SHELF.spec, ("rowCount",))


class TestKeyOrder:
    def test_kinds_that_do_not_compare_take_the_documented_ranks(self):
        east = timezone(timedelta(hours=2))
        keys = [
            (datetime(2026, 1, 1, 9, tzinfo=east),),
            (NAIVE,),
            (date(2026, 1, 1),),
            ("a",),
            (2,),
            (Decimal("1.5"),),
            (True,),
        ]

        assert sorted(keys, key=key_order) == list(reversed(keys))

    def test_numbers_sort_by_value_and_aware_datetimes_by_instant(self):
        east = timezone(timedelta(hours=2))
        later = datetime(2026, 1, 1, 9, tzinfo=UTC)
        earlier = datetime(2026, 1, 1, 10, tzinfo=east)  # 08:00 UTC

        assert sorted([(10,), (Decimal("9.5"),), (2,)], key=key_order) == [(2,), (Decimal("9.5"),), (10,)]
        assert sorted([(later,), (earlier,)], key=key_order) == [(earlier,), (later,)]


class TestNormalizedKey:
    def test_strings_are_nfc_stripped_and_collapsed(self):
        first = Assembly(assembly_id="  Cafe\u0301\u00a0-  104 \t", title="t")
        second = Assembly(assembly_id="Caf\u00e9 -\n104", title="t")

        assert normalized_key(first, ASSEMBLY.spec) == normalized_key(second, ASSEMBLY.spec) == ("Caf\u00e9 - 104",)

    @pytest.mark.parametrize(("first", "second"), [("A-1\u00b9", "A-11"), ("\uff30\uff2e-1", "PN-1"), ("pn-1", "PN-1")])
    def test_compatibility_forms_and_case_stay_distinct(self, first, second):
        one = normalized_key(Assembly(assembly_id=first, title="t"), ASSEMBLY.spec)

        assert one != normalized_key(Assembly(assembly_id=second, title="t"), ASSEMBLY.spec)

    def test_a_model_without_keys_has_the_empty_key(self):
        assert normalized_key(Summary(headline="h", count=1), SUMMARY.spec) == ()

    def test_values_normalize_by_kind(self):
        class Keyed(BaseModel):
            number: Annotated[int, Key]
            amount: Annotated[Decimal, Key]
            day: Annotated[date, Key]
            at: Annotated[AwareDatetime, Key]
            local: Annotated[NaiveDatetime, Key]
            color: Annotated[Color, Key]
            letter: Annotated[Literal["a", "b"], Key]

        spec = prepare(Keyed).spec
        east = timezone(timedelta(hours=2))
        first = Keyed(
            number=3,
            amount=Decimal("1.50"),
            day=date(2026, 1, 2),
            at=datetime(2026, 1, 2, 12, tzinfo=east),
            local=NAIVE,
            color=Color.RED,
            letter="a",
        )
        second = first.model_copy(update={"amount": Decimal("1.5"), "at": datetime(2026, 1, 2, 10, tzinfo=UTC)})

        key = normalized_key(first, spec)
        assert key == normalized_key(second, spec)
        assert hash(key) == hash(normalized_key(second, spec))
        assert key == (3, Decimal("1.5"), date(2026, 1, 2), datetime(2026, 1, 2, 10, tzinfo=UTC), NAIVE, "red", "a")
        assert key[3].tzinfo is UTC

    def test_rejects_a_record_of_another_model(self):
        with pytest.raises(TypeError):
            normalized_key(Summary(headline="h", count=1), ASSEMBLY.spec)


# ---------------------------------------------------------------------------------------------------------------
# build_consensus: arguments
# ---------------------------------------------------------------------------------------------------------------

U1 = "doc:p0001_e001-p0001_e005"
U2 = "doc:p0002_e001-p0002_e007"
U3 = "doc:p0003_e001-p0003_e002"
UNITS = (U1, U2, U3)
PAIR = (GOOGLE, ANTHROPIC)


def failed(unit_id: str, model: tuple[str, str, str]) -> FailedUnit:
    return FailedUnit(unit_id=unit_id, model_id=model[0], status_code=503, error_message="retries spent", attempts=3)


def blend(
    responses: dict[tuple[str, tuple[str, str, str]], ProviderResponse],
    models: tuple[tuple[str, str, str], ...] = PAIR,
    *,
    schema=ASSEMBLY,
    units: tuple[str, ...] = UNITS,
    min_families: int | None = None,
    must_agree: tuple[str, ...] = (),
    failures: tuple[FailedUnit, ...] = (),
) -> ConsensusResult:
    """Consensus over ``responses`` keyed by (unit id, model triple)."""
    run = outcome({(unit, model[0]): response for (unit, model), response in responses.items()}, models, failures)
    families = min(2, len(models)) if min_families is None else min_families  # rationalize's default, 2
    return build_consensus(run, schema, units, min_families=families, must_agree=must_agree)


class TestBuildConsensusArguments:
    def run(self, **overrides: Any) -> ConsensusResult:
        fields: dict[str, Any] = {
            "outcome": outcome({(U1, GOOGLE[0]): answer(assembly())}, PAIR),
            "schema": ASSEMBLY,
            "unit_ids": UNITS,
            "min_families": 2,
            "must_agree": (),
            **overrides,
        }
        return build_consensus(
            fields["outcome"],
            fields["schema"],
            fields["unit_ids"],
            min_families=fields["min_families"],
            must_agree=fields["must_agree"],
        )

    def test_min_families_past_the_number_of_models_names_the_range(self):
        with pytest.raises(ValueError, match=r"^min_families must be in 1-2 \(the number of models\), got 3$"):
            self.run(min_families=3)

    def test_accepts_a_list_of_unit_ids(self):
        assert len(self.run(unit_ids=list(UNITS)).items) == 1

    @pytest.mark.parametrize(
        ("overrides", "error"),
        [
            ({"outcome": {}}, TypeError),
            ({"schema": Assembly}, TypeError),
            ({"unit_ids": U1}, TypeError),
            ({"unit_ids": (U1, 2)}, TypeError),
            ({"unit_ids": ()}, ValueError),
            ({"unit_ids": (U1, U1)}, ValueError),
            ({"unit_ids": (U1, " ")}, ValueError),
            ({"unit_ids": (U2,)}, ValueError),
            ({"outcome": outcome({(U1, "other:model"): answer()}, PAIR)}, ValueError),
            ({"outcome": outcome({}, PAIR, (failed("doc:elsewhere", GOOGLE),))}, ValueError),
            ({"outcome": outcome({}, PAIR, (failed(U1, OPENAI),))}, ValueError),
            ({"outcome": outcome({}, ())}, ValueError),
            ({"outcome": outcome({}, (GOOGLE, GOOGLE))}, ValueError),
            ({"outcome": outcome({}, (GOOGLE, ("m", "Google", "x")))}, ValueError),
            ({"min_families": True}, TypeError),
            ({"min_families": 2.0}, TypeError),
            ({"min_families": 0}, ValueError),
            ({"min_families": 3}, ValueError),
            ({"must_agree": ("nope",)}, RationalizationError),
            ({"must_agree": "title"}, RationalizationError),
        ],
    )
    def test_rejects(self, overrides, error):
        with pytest.raises(error):
            self.run(**overrides)


# ---------------------------------------------------------------------------------------------------------------
# build_consensus: one model
# ---------------------------------------------------------------------------------------------------------------


def vote(record: dict[str, Any], schema=ASSEMBLY) -> dict[str, Any]:
    """A record as model_votes holds it: validated, then dumped in JSON mode."""
    return schema.model.model_validate(record).model_dump(mode="json")


class TestSingleModel:
    def test_every_valid_record_is_a_single_model_item(self):
        result = blend({(U1, OPENAI): answer(assembly("A-2", "Valve"), assembly("A-1"))}, (OPENAI,))

        assert [found.status for found in result.items] == ["single_model", "single_model"]
        first = result.items[0]
        assert first.record == Assembly(assembly_id="A-1", title="Pump")
        assert (first.unit_id, first.key, first.disputed_fields, first.notes) == (U1, ("A-1",), (), ())
        assert dict(first.model_votes) == {OPENAI[0]: (vote(assembly("A-1")),)}
        assert dict(first.quotes_by_model) == {OPENAI[0]: ("a quote",)}
        assert dict(first.element_ids_by_model) == {OPENAI[0]: ("p0001_e001",)}
        assert dict(first.families) == {OPENAI[0]: "openai"}

    def test_quarantined_records_are_listed_and_the_rest_kept(self):
        result = blend({(U1, OPENAI): answer(assembly("A-1"), assembly("A-2", title=None))}, (OPENAI,))

        assert [found.key for found in result.items] == [("A-1",)]
        assert [(found.reason, found.unit_id, found.model_id) for found in result.quarantined] == [
            ("validation", U1, OPENAI[0])
        ]

    def test_a_failed_or_silent_unit_has_no_item(self):
        result = blend({(U2, OPENAI): answer(assembly())}, (OPENAI,), failures=(failed(U1, OPENAI),))

        assert [found.unit_id for found in result.items] == [U2]
        assert result.quarantined == ()

    def test_a_root_without_keys_gives_each_record_an_item(self):
        records = ({"headline": "b", "count": 2}, {"headline": "a", "count": 1})

        result = blend({(U1, OPENAI): answer(*records)}, (OPENAI,), schema=SUMMARY)

        assert [(found.status, found.key, found.record) for found in result.items] == [
            ("single_model", (), Summary(headline="b", count=2)),
            ("single_model", (), Summary(headline="a", count=1)),
        ]


def entries(*pairs: tuple[dict[str, Any], tuple[str, ...]]) -> ProviderResponse:
    """A response of (record, quotes) pairs, each entry citing the element ``e`` + its index."""
    return raw({"records": [entry(record, quotes, [f"e{index}"]) for index, (record, quotes) in enumerate(pairs)]})


class TestDuplicateKeys:
    def test_identical_copies_from_one_model_merge_and_unite_their_grounding(self):
        response = entries((assembly(title="Pump"), ("q1",)), (assembly(title="  Pump "), ("q2", "q1")))

        (found,) = blend({(U1, OPENAI): response}, (OPENAI,)).items

        assert found.status == "single_model"
        assert found.record == Assembly(assembly_id="A-1", title="Pump")
        assert dict(found.model_votes) == {OPENAI[0]: (vote(assembly(title="Pump")),)}
        assert dict(found.quotes_by_model) == {OPENAI[0]: ("q1", "q2")}
        assert dict(found.element_ids_by_model) == {OPENAI[0]: ("e0", "e1")}

    def test_differing_copies_from_one_model_are_a_disagreement_keeping_every_copy(self):
        copies = (assembly(title="Pump"), assembly(title="Valve"), assembly(title="Pump", revision=2))

        (found,) = blend({(U1, OPENAI): answer(*copies)}, (OPENAI,)).items

        assert found.status == "field_disagreement"
        assert found.record is None
        assert found.disputed_fields == ("title", "revision")
        assert found.notes == (f"{OPENAI[0]} returned this key 3 times, with differing values",)
        assert dict(found.model_votes) == {OPENAI[0]: tuple(vote(copy) for copy in copies)}


# ---------------------------------------------------------------------------------------------------------------
# build_consensus: several models
# ---------------------------------------------------------------------------------------------------------------


def only(result: ConsensusResult) -> ConsensusItem:
    (found,) = result.items
    return found


class TestTwoFamilies:
    def test_agreement_across_two_families_is_consensus(self):
        result = blend({(U1, GOOGLE): answer(assembly()), (U1, ANTHROPIC): answer(assembly())})

        found = only(result)
        assert found.status == "consensus"
        assert found.record == Assembly(assembly_id="A-1", title="Pump")
        assert (found.disputed_fields, found.notes, found.key) == ((), (), ("A-1",))
        assert dict(found.model_votes) == {GOOGLE[0]: (vote(assembly()),), ANTHROPIC[0]: (vote(assembly()),)}
        assert dict(found.families) == {GOOGLE[0]: "google", ANTHROPIC[0]: "anthropic"}

    def test_the_merged_value_is_the_first_models_own_text(self):
        result = blend(
            {
                (U1, GOOGLE): answer(assembly(title="Pump  Housing")),
                (U1, ANTHROPIC): answer(assembly(title="Pump Housing")),
            }
        )

        assert only(result).record == Assembly(assembly_id="A-1", title="Pump  Housing")

    def test_a_field_two_models_differ_on_is_a_disagreement_without_a_pick(self):
        result = blend({(U1, GOOGLE): answer(assembly(title="Pump")), (U1, ANTHROPIC): answer(assembly(title="Valve"))})

        found = only(result)
        assert (found.status, found.disputed_fields, found.record) == ("field_disagreement", ("title",), None)
        assert [cast["title"] for casts in found.model_votes.values() for cast in casts] == ["Pump", "Valve"]

    def test_a_key_one_model_left_out_is_missing_in_some_models(self):
        result = blend(
            {(U1, GOOGLE): answer(assembly("A-1"), assembly("A-2", "Valve")), (U1, ANTHROPIC): answer(assembly("A-1"))}
        )

        first, second = result.items
        assert first.status == "consensus"
        assert (second.status, second.key, second.disputed_fields) == ("missing_in_some_models", ("A-2",), ())
        assert second.record == Assembly(assembly_id="A-2", title="Valve")
        assert dict(second.model_votes) == {GOOGLE[0]: (vote(assembly("A-2", "Valve")),), ANTHROPIC[0]: ()}

    def test_a_model_that_failed_on_the_unit_leaves_every_key_missing(self):
        result = blend({(U1, GOOGLE): answer(assembly("A-1"), assembly("A-2"))}, failures=(failed(U1, ANTHROPIC),))

        assert [found.status for found in result.items] == ["missing_in_some_models", "missing_in_some_models"]

    def test_a_model_whose_records_were_all_quarantined_leaves_every_key_missing(self):
        result = blend({(U1, GOOGLE): answer(assembly()), (U1, ANTHROPIC): answer(assembly(title=None))})

        assert only(result).status == "missing_in_some_models"
        assert [found.model_id for found in result.quarantined] == [ANTHROPIC[0]]


def titled(models: tuple[tuple[str, str, str], ...], titles: tuple[str, ...], **options: Any) -> ConsensusItem:
    """The one item of models answering A-1 with one title each."""
    responses = {(U1, model): answer(assembly(title=title)) for model, title in zip(models, titles, strict=True)}
    return only(blend(responses, models, **options))


class TestQuorum:
    def test_a_majority_backed_by_two_families_is_consensus(self):
        found = titled((GOOGLE, ANTHROPIC, OPENAI), ("Pump", "Valve", "Pump"), min_families=2)

        assert (found.status, found.record) == ("consensus", Assembly(assembly_id="A-1", title="Pump"))

    def test_a_majority_from_one_family_is_not_agreed(self):
        found = titled((GOOGLE, GOOGLE_2, ANTHROPIC), ("Pump", "Pump", "Valve"), min_families=2)

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("title",))
        assert found.record == Assembly(assembly_id="A-1", title="Pump")

    def test_with_min_families_one_a_one_family_majority_is_consensus(self):
        found = titled((GOOGLE, GOOGLE_2, ANTHROPIC), ("Pump", "Pump", "Valve"), min_families=1)

        assert found.status == "consensus"

    @pytest.mark.parametrize(
        ("models", "titles", "status"),
        [
            ((UNKNOWN, UNKNOWN_2, GOOGLE, ANTHROPIC), ("Pump", "Pump", "Pump", "Valve"), "field_disagreement"),
            ((UNKNOWN, GOOGLE, ANTHROPIC), ("Pump", "Pump", "Valve"), "field_disagreement"),
            ((UNKNOWN, GOOGLE, ANTHROPIC), ("Valve", "Pump", "Pump"), "consensus"),
        ],
    )
    def test_the_unknown_family_adds_no_diversity(self, models, titles, status):
        assert titled(models, titles, min_families=2).status == status

    @pytest.mark.parametrize(
        ("models", "min_families"),
        [
            ((GOOGLE, GOOGLE_2), 2),
            ((UNKNOWN, UNKNOWN_2), 2),
            ((GOOGLE, UNKNOWN, UNKNOWN_2), 3),
            ((GOOGLE, UNKNOWN), 2),
            ((GOOGLE, GOOGLE_2, ANTHROPIC), 3),
        ],
    )
    def test_a_run_whose_models_span_too_few_known_families_is_refused(self, models, min_families):
        responses = {(U1, model): answer(assembly()) for model in models}

        with pytest.raises(ValueError, match=r"^the outcome's models span \d+ distinct known famil"):
            blend(responses, models, min_families=min_families)

    @pytest.mark.parametrize("models", [(UNKNOWN, UNKNOWN_2), (GOOGLE, UNKNOWN), (OPENAI,)])
    def test_min_families_one_takes_any_models(self, models):
        responses = {(U1, model): answer(assembly()) for model in models}

        assert only(blend(responses, models, min_families=1)).record == Assembly(assembly_id="A-1", title="Pump")

    def test_unknown_families_alone_count_as_one(self):
        found = titled((UNKNOWN, UNKNOWN_2), ("Pump", "Pump"), min_families=1)

        assert found.status == "consensus"

    def test_no_majority_is_a_tie_without_a_record(self):
        found = titled((GOOGLE, ANTHROPIC, OPENAI), ("Pump", "Valve", "Fan"), min_families=2)

        assert (found.status, found.disputed_fields, found.record) == ("field_disagreement", ("title",), None)

    def test_an_even_split_is_a_tie(self):
        found = titled((GOOGLE, ANTHROPIC, OPENAI, GOOGLE_2), ("Pump", "Valve", "Valve", "Pump"), min_families=2)

        assert (found.status, found.record) == ("field_disagreement", None)

    def test_a_must_agree_field_needs_every_model(self):
        found = titled((GOOGLE, ANTHROPIC, OPENAI), ("Pump", "Valve", "Pump"), min_families=2, must_agree=("title",))

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("title",))
        assert found.record == Assembly(assembly_id="A-1", title="Pump")

    def test_must_agree_leaves_other_fields_to_the_majority(self):
        found = titled((GOOGLE, ANTHROPIC, OPENAI), ("Pump", "Valve", "Pump"), min_families=2, must_agree=("revision",))

        assert found.status == "consensus"


class Reading(BaseModel):
    sensor: Annotated[str, Key]
    value: float
    amount: Decimal
    at: AwareDatetime
    color: Color
    ok: bool


READING = prepare(Reading)
THREE = (GOOGLE, ANTHROPIC, OPENAI)


def reading(**fields: Any) -> dict[str, Any]:
    return {
        "sensor": "S-1",
        "value": 1.0,
        "amount": "1.5",
        "at": "2026-01-02T10:00:00Z",
        "color": "red",
        "ok": True,
        **fields,
    }


def readings(*records: dict[str, Any], models=PAIR, **options: Any) -> ConsensusItem:
    responses = {(U1, model): answer(record) for model, record in zip(models, records, strict=True)}
    return only(blend(responses, models, schema=READING, **options))


class TestNormalizedComparison:
    @pytest.mark.parametrize(
        ("first", "second"),
        [
            (987654.32, 987654.23),
            (12345687.0, 12345678.0),
            (1234567.8, 1234568.7),
            (1.0, 1.00001),
            (0.0, 5e-10),
            (1.23456789012345, 1.23456789012346),
        ],
    )
    def test_floats_with_any_real_decimal_difference_dispute(self, first, second):
        found = readings(reading(value=first), reading(value=second))

        assert (found.status, found.disputed_fields, found.record) == ("field_disagreement", ("value",), None)

    @pytest.mark.parametrize(
        ("first", "second"),
        [(0.1 + 0.2, 0.3), (1.0, 1.0000000000000002), (1e300 * 3, 3e300), (-0.0, 0.0), (0, 0.0)],
    )
    def test_floats_equal_at_fifteen_significant_digits_agree(self, first, second):
        found = readings(reading(value=first), reading(value=second))

        assert (found.status, found.record.value) == ("consensus", first)

    def test_float_agreement_is_transitive_so_one_cluster_holds_every_spelling(self):
        records = [reading(value=value) for value in (0.30000000000000004, 0.3, 0.1 + 0.2)]

        assert readings(*records, models=THREE, must_agree=("value",)).status == "consensus"

    @pytest.mark.parametrize(
        ("first", "second"),
        [("10\u2076", "106"), ("1,200\u00b9", "1,2001"), ("\uff11\uff12", "12"), ("Pump", "pump")],
    )
    def test_strings_that_only_compatibility_folding_or_case_would_match_dispute(self, first, second):
        found = titled(PAIR, (first, second))

        assert (found.status, found.disputed_fields, found.record) == ("field_disagreement", ("title",), None)

    @pytest.mark.parametrize(
        ("first", "second"), [("Cafe\u0301 Pump", "Caf\u00e9 Pump"), ("Pump\u00a0 \t Housing ", "Pump Housing")]
    )
    def test_canonically_equal_strings_agree_and_keep_the_first_models_text(self, first, second):
        found = titled(PAIR, (first, second))

        assert (found.status, found.record.title) == ("consensus", first)

    def test_keys_that_differ_by_a_compatibility_form_are_separate_entities(self):
        responses = {(U1, GOOGLE): answer(assembly("A-1\u00b9")), (U1, ANTHROPIC): answer(assembly("A-11"))}

        result = blend(responses)

        assert sorted(found.key for found in result.items) == [("A-11",), ("A-1\u00b9",)]
        assert [found.status for found in result.items] == ["missing_in_some_models"] * 2

    def test_decimals_agree_by_numeric_value(self):
        found = readings(reading(amount="1.50"), reading(amount=1.5))

        assert (found.status, found.record.amount) == ("consensus", Decimal("1.50"))
        assert readings(reading(amount="1.5"), reading(amount="1.5000001")).disputed_fields == ("amount",)

    def test_aware_datetimes_agree_by_instant(self):
        found = readings(reading(at="2026-01-02T12:00:00+02:00"), reading(at="2026-01-02T10:00:00Z"))

        assert found.status == "consensus"
        assert found.record.at.utcoffset() == timedelta(hours=2)

    def test_enums_and_bools_compare_exactly(self):
        assert readings(reading(color="red", ok=True), reading(color="blue", ok=False)).disputed_fields == (
            "color",
            "ok",
        )


# ---------------------------------------------------------------------------------------------------------------
# build_consensus: nested records
# ---------------------------------------------------------------------------------------------------------------


def parts(*components: tuple[str, int]) -> list[dict[str, Any]]:
    return [{"part_number": number, "quantity": quantity} for number, quantity in components]


def built(*component_lists: list[dict[str, Any]], models=PAIR, **options: Any) -> ConsensusItem:
    responses = {
        (U1, model): answer(assembly(components=components))
        for model, components in zip(models, component_lists, strict=True)
    }
    return only(blend(responses, models, **options))


class TestKeyedChildLists:
    def test_children_match_by_key_in_any_order(self):
        found = built(parts(("PN-2", 1), ("PN-1", 4)), parts(("PN-1 ", 4), ("PN-2", 1)))

        assert found.status == "consensus"
        assert [(part.part_number, part.quantity) for part in found.record.components] == [("PN-2", 1), ("PN-1", 4)]

    def test_a_child_field_that_differs_is_disputed_at_its_keyed_path(self):
        found = built(parts(("PN-104", 2), ("PN-105", 1)), parts(("PN-104", 3), ("PN-105", 1)))

        assert (found.status, found.disputed_fields, found.record) == (
            "field_disagreement",
            ("components['PN-104'].quantity",),
            None,
        )

    def test_a_child_one_of_two_models_left_out_is_disputed_and_has_no_winner(self):
        found = built(parts(("PN-1", 1), ("PN-2", 1)), parts(("PN-1", 1)))

        assert (found.status, found.disputed_fields, found.record) == (
            "field_disagreement",
            ("components['PN-2']",),
            None,
        )

    def test_a_child_two_of_three_models_have_is_kept_but_disputed(self):
        found = built(
            parts(("PN-1", 1), ("PN-2", 1)), parts(("PN-1", 1)), parts(("PN-1", 1), ("PN-2", 1)), models=THREE
        )

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("components['PN-2']",))
        assert [part.part_number for part in found.record.components] == ["PN-1", "PN-2"]

    def test_a_child_one_of_three_models_has_is_left_out_but_disputed(self):
        found = built(parts(("PN-1", 1)), parts(("PN-1", 1), ("PN-9", 1)), parts(("PN-1", 1)), models=THREE)

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("components['PN-9']",))
        assert [part.part_number for part in found.record.components] == ["PN-1"]

    def test_a_child_key_repeated_identically_in_one_list_merges(self):
        found = built(parts(("PN-1", 1), ("PN-1", 1)), parts(("PN-1", 1)))

        assert found.status == "consensus"
        assert [part.part_number for part in found.record.components] == ["PN-1"]

    def test_a_child_key_repeated_with_differing_values_is_disputed(self):
        found = built(parts(("PN-1", 1), ("PN-1", 2)), parts(("PN-1", 1)))

        assert (found.status, found.disputed_fields, found.record) == (
            "field_disagreement",
            ("components['PN-1']",),
            None,
        )

    def test_must_agree_reaches_child_fields(self):
        lists = (parts(("PN-1", 1)), parts(("PN-1", 2)), parts(("PN-1", 1)))

        assert built(*lists, models=THREE).status == "consensus"
        found = built(*lists, models=THREE, must_agree=("components.quantity",))
        assert (found.status, found.disputed_fields) == ("field_disagreement", ("components['PN-1'].quantity",))

    def test_must_agree_on_a_list_covers_everything_under_it(self):
        lists = (parts(("PN-1", 1)), parts(("PN-1", 2)), parts(("PN-1", 1)))

        assert built(*lists, models=THREE, must_agree=("components",)).disputed_fields == (
            "components['PN-1'].quantity",
        )

    def test_a_child_with_several_keys_names_them_all(self):
        class Line(BaseModel):
            line_no: Annotated[int, Key]
            part: Annotated[str, Key]
            quantity: int

        class Order(BaseModel):
            order_id: Annotated[str, Key]
            lines: list[Line]

        responses = {
            (U1, GOOGLE): answer({"order_id": "O-1", "lines": [{"line_no": 3, "part": "PN-1", "quantity": 1}]}),
            (U1, ANTHROPIC): answer({"order_id": "O-1", "lines": [{"line_no": 3, "part": "PN-1", "quantity": 2}]}),
        }

        found = only(blend(responses, schema=prepare(Order)))

        assert found.disputed_fields == ("lines[3, 'PN-1'].quantity",)


class Step(BaseModel):
    text: str
    minutes: int


class Recipe(BaseModel):
    recipe_id: Annotated[str, Key]
    main: Note
    steps: list[Step] = []
    tags: list[str] = []
    origin: Bin | None = None


RECIPE = prepare(Recipe)


def recipe(**fields: Any) -> dict[str, Any]:
    return {"recipe_id": "R-1", "main": {"text": "Soup", "page": 1}, **fields}


def recipes(*records: dict[str, Any], models=PAIR, **options: Any) -> ConsensusItem:
    responses = {(U1, model): answer(record) for model, record in zip(models, records, strict=True)}
    return only(blend(responses, models, schema=RECIPE, **options))


def steps(*items: tuple[str, int]) -> list[dict[str, Any]]:
    return [{"text": text, "minutes": minutes} for text, minutes in items]


class TestSequencesAndNestedModels:
    def test_unkeyed_children_compare_as_a_normalized_sequence(self):
        found = recipes(
            recipe(steps=steps(("Boil  water", 5), ("Add salt", 1))),
            recipe(steps=steps(("Boil water", 5), ("Add salt", 1))),
        )

        assert found.status == "consensus"
        assert [step.text for step in found.record.steps] == ["Boil  water", "Add salt"]

    def test_unkeyed_children_that_differ_are_disputed_by_position(self):
        first = steps(("Boil", 5), ("Salt", 1), ("Stir", 2))
        second = steps(("Boil", 5), ("Salt", 1), ("Stir", 3))

        assert recipes(recipe(steps=first), recipe(steps=second)).disputed_fields == ("steps[2]",)

    def test_sequences_of_different_lengths_are_disputed_whole(self):
        found = recipes(recipe(steps=steps(("Boil", 5))), recipe(steps=steps(("Boil", 5), ("Salt", 1))))

        assert (found.disputed_fields, found.record) == (("steps",), None)

    def test_scalar_lists_compare_in_order(self):
        assert recipes(recipe(tags=["hot ", "soup"]), recipe(tags=["hot", "soup"])).status == "consensus"
        assert recipes(recipe(tags=["soup", "hot"]), recipe(tags=["hot", "soup"])).disputed_fields == (
            "tags[0]",
            "tags[1]",
        )

    def test_a_nested_model_compares_field_by_field(self):
        found = recipes(recipe(main={"text": "Soup", "page": 1}), recipe(main={"text": "Stew", "page": 1}))

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("main.text",))

    def test_an_optional_model_none_everywhere_agrees(self):
        found = recipes(recipe(origin=None), recipe())

        assert (found.status, found.record.origin) == ("consensus", None)

    def test_an_optional_model_present_in_one_of_two_is_disputed_without_a_winner(self):
        found = recipes(recipe(origin={"code": "B-1"}), recipe(origin=None))

        assert (found.disputed_fields, found.record) == (("origin",), None)

    def test_an_optional_model_present_in_a_cross_family_majority_is_agreed(self):
        found = recipes(
            recipe(origin={"code": "B-1"}), recipe(origin=None), recipe(origin={"code": "B-1"}), models=THREE
        )

        assert (found.status, found.record.origin) == ("consensus", Bin(code="B-1"))

    def test_an_optional_model_absent_in_a_majority_is_none(self):
        found = recipes(recipe(origin={"code": "B-1"}), recipe(), recipe(), models=THREE)

        assert (found.status, found.record.origin) == ("consensus", None)

    def test_an_optional_models_fields_compare_among_the_models_that_have_it(self):
        found = recipes(
            recipe(origin={"code": "B-1", "capacity": 4}),
            recipe(),
            recipe(origin={"code": "B-1", "capacity": 5}),
            models=THREE,
        )

        assert (found.disputed_fields, found.record) == (("origin.capacity",), None)


class Item(BaseModel):
    sku: Annotated[str, Key]
    count: int


class Section(BaseModel):
    title: str
    items: list[Item] = []


class Catalog(BaseModel):
    catalog_id: Annotated[str, Key]
    section: Section | None = None


MISTRAL = ("mistral:large", "mistral", "openai-compatible")
FIVE = (GOOGLE, ANTHROPIC, OPENAI, GOOGLE_2, MISTRAL)


class TestMajorityUnderAPartialParent:
    def test_a_childs_majority_counts_every_model_that_returned_the_record(self):
        sections = [
            {"title": "S", "items": [{"sku": "X", "count": 1}]},
            {"title": "S", "items": [{"sku": "X", "count": 1}]},
            {"title": "S", "items": []},
            None,
            None,
        ]
        responses = {
            (U1, model): answer({"catalog_id": "C", "section": section})
            for model, section in zip(FIVE, sections, strict=True)
        }

        found = only(blend(responses, FIVE, schema=prepare(Catalog)))

        # The section wins (3 of 5); X is in 2 of those 3 but not in more than half of the 5 models: no winner.
        assert (found.status, found.disputed_fields, found.record) == (
            "field_disagreement",
            ("section.items['X']",),
            None,
        )


ONE_FAMILY_MAJORITY = (GOOGLE, GOOGLE_2, ANTHROPIC)


class TestFamilyRuleBelowTheRoot:
    def test_a_one_family_majority_on_a_keyed_childs_field_is_disputed(self):
        lists = (parts(("PN-1", 1)), parts(("PN-1", 1)), parts(("PN-1", 2)))

        found = built(*lists, models=ONE_FAMILY_MAJORITY)

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("components['PN-1'].quantity",))

    def test_a_one_family_majority_on_a_nested_models_field_is_disputed(self):
        records = [recipe(main={"text": "Soup", "page": page}) for page in (1, 1, 2)]

        found = recipes(*records, models=ONE_FAMILY_MAJORITY)

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("main.page",))

    def test_a_one_family_majority_on_an_optional_models_field_is_disputed(self):
        records = [recipe(origin={"code": "B-1", "capacity": capacity}) for capacity in (4, 4, 5)]

        found = recipes(*records, models=ONE_FAMILY_MAJORITY)

        assert (found.status, found.disputed_fields) == ("field_disagreement", ("origin.capacity",))

    def test_a_one_family_majority_for_an_optional_models_presence_is_disputed(self):
        present = recipes(
            recipe(origin={"code": "B-1"}), recipe(origin={"code": "B-1"}), recipe(), models=ONE_FAMILY_MAJORITY
        )
        absent = recipes(recipe(), recipe(), recipe(origin={"code": "B-1"}), models=ONE_FAMILY_MAJORITY)

        assert present.status == "field_disagreement"
        assert present.disputed_fields[0] == "origin"
        assert (absent.status, absent.disputed_fields, absent.record.origin) == (
            "field_disagreement",
            ("origin",),
            None,
        )

    def test_an_empty_keyed_list_meets_the_quorum_too(self):
        sections = [{"title": "S", "items": []}, {"title": "S", "items": []}, None]
        responses = {
            (U1, model): answer({"catalog_id": "C", "section": section})
            for model, section in zip(ONE_FAMILY_MAJORITY, sections, strict=True)
        }

        found = only(blend(responses, ONE_FAMILY_MAJORITY, schema=prepare(Catalog)))

        assert found.disputed_fields == ("section", "section.title", "section.items")

    @pytest.mark.parametrize(
        ("models", "min_families"),
        [((GOOGLE, GOOGLE_2), 2), ((UNKNOWN, UNKNOWN_2), 2), ((GOOGLE, UNKNOWN, UNKNOWN_2), 3)],
    )
    def test_the_reviews_empty_children_cases_never_reach_consensus(self, models, min_families):
        class Child(BaseModel):
            code: Annotated[str, Key]

        class Root(BaseModel):
            children: list[Child]

        responses = {(U1, model): answer({"children": []}) for model in models}

        with pytest.raises(ValueError, match="distinct known famil"):
            blend(responses, models, schema=prepare(Root), min_families=min_families)


# ---------------------------------------------------------------------------------------------------------------
# build_consensus: roots without keys
# ---------------------------------------------------------------------------------------------------------------


def summary(headline: str = "Q3", count: int = 1) -> dict[str, Any]:
    return {"headline": headline, "count": count}


class TestUnkeyedRoots:
    def test_one_record_from_every_model_pairs_by_position(self):
        result = blend({(U1, GOOGLE): answer(summary()), (U1, ANTHROPIC): answer(summary())}, schema=SUMMARY)

        found = only(result)
        assert (found.status, found.key, found.record) == ("consensus", (), Summary(headline="Q3", count=1))

    def test_a_paired_field_can_disagree(self):
        result = blend(
            {(U1, GOOGLE): answer(summary(count=1)), (U1, ANTHROPIC): answer(summary(count=2))}, schema=SUMMARY
        )

        assert only(result).disputed_fields == ("count",)

    def test_several_records_are_each_unmatched_in_model_then_response_order(self):
        result = blend(
            {
                (U1, GOOGLE): answer(summary("g1"), summary("g2")),
                (U1, ANTHROPIC): answer(summary("a1"), summary("a2")),
            },
            schema=SUMMARY,
        )

        assert [found.status for found in result.items] == ["unmatched"] * 4
        assert [found.record for found in result.items] == [None] * 4
        assert [
            [cast["headline"] for casts in found.model_votes.values() for cast in casts] for found in result.items
        ] == [["g1"], ["g2"], ["a1"], ["a2"]]

    def test_a_record_only_one_model_returned_is_unmatched(self):
        result = blend({(U1, GOOGLE): answer(summary())}, schema=SUMMARY, failures=(failed(U1, ANTHROPIC),))

        assert [(found.status, dict(found.model_votes)) for found in result.items] == [
            ("unmatched", {GOOGLE[0]: (vote(summary(), SUMMARY),), ANTHROPIC[0]: ()})
        ]

    def test_no_records_no_items(self):
        result = blend({(U1, GOOGLE): answer(), (U1, ANTHROPIC): answer()}, schema=SUMMARY)

        assert result.items == ()


class Flags(BaseModel):
    flag_id: Annotated[str, Key]
    a: int
    b: int
    c: int

    @model_validator(mode="after")
    def not_all_ones(self) -> "Flags":
        if self.a == self.b == self.c == 1:
            raise ValueError("a, b and c cannot all be 1")
        return self


class TestRebuild:
    def test_a_merged_record_model_validate_refuses_is_a_disagreement_with_a_note(self):
        records = (
            {"flag_id": "F", "a": 1, "b": 1, "c": 0},
            {"flag_id": "F", "a": 1, "b": 0, "c": 1},
            {"flag_id": "F", "a": 0, "b": 1, "c": 1},
        )
        responses = {(U1, model): answer(record) for model, record in zip(THREE, records, strict=True)}

        found = only(blend(responses, THREE, schema=prepare(Flags)))

        assert (found.status, found.record, found.disputed_fields) == ("field_disagreement", None, ())
        assert found.notes == ("the merged record failed validation: 1 validation error: (the record): value_error",)


class Priced(BaseModel):
    priced_id: Annotated[str, Key]
    price: float | None = None


def prices(*values: float | None, models: tuple[tuple[str, str, str], ...]) -> ConsensusItem:
    responses = {
        (U1, model): answer({"priced_id": "P", "price": value}) for model, value in zip(models, values, strict=True)
    }
    return only(blend(responses, models, schema=prepare(Priced)))


class TestReviewedEdges:
    def test_an_unkeyed_root_with_one_model_silent_of_three_is_unmatched(self):
        responses = {(U1, GOOGLE): answer(summary()), (U1, ANTHROPIC): answer(summary()), (U1, OPENAI): answer()}

        result = blend(responses, THREE, schema=SUMMARY)

        assert [found.status for found in result.items] == ["unmatched", "unmatched"]

    def test_none_backed_by_two_of_three_wins_an_optional_scalar(self):
        found = prices(None, None, 1.5, models=THREE)

        assert (found.status, found.record.price) == ("consensus", None)

    def test_a_number_before_none_is_a_tie_of_two(self):
        found = prices(1.5, None, models=PAIR)

        assert (found.status, found.disputed_fields, found.record) == ("field_disagreement", ("price",), None)

    def test_none_after_a_number_still_wins_with_a_majority(self):
        found = prices(1.5, None, None, models=THREE)

        assert (found.status, found.record.price) == ("consensus", None)


# ---------------------------------------------------------------------------------------------------------------
# build_consensus: order
# ---------------------------------------------------------------------------------------------------------------


class Numbered(BaseModel):
    number: Annotated[int, Key]
    title: str


class Dated(BaseModel):
    day: Annotated[date, Key]
    amount: Annotated[Decimal, Key]
    title: str


class TestOrder:
    def test_units_come_in_the_given_order_whatever_the_outcomes(self):
        responses = {
            (U3, OPENAI): answer(assembly("A-3")),
            (U1, OPENAI): answer(assembly("A-1")),
            (U2, OPENAI): answer(assembly("A-2")),
        }

        result = blend(responses, (OPENAI,), units=(U2, U3, U1))

        assert [found.unit_id for found in result.items] == [U2, U3, U1]

    def test_keyed_items_sort_by_their_normalized_key(self):
        records = [assembly(name) for name in ("b", " a", "B", "a2", "A")]

        result = blend({(U1, OPENAI): answer(*records)}, (OPENAI,))

        assert [found.key for found in result.items] == [("A",), ("B",), ("a",), ("a2",), ("b",)]

    def test_numbers_sort_by_value(self):
        records = [{"number": number, "title": "t"} for number in (10, 2, 1)]

        result = blend({(U1, OPENAI): answer(*records)}, (OPENAI,), schema=prepare(Numbered))

        assert [found.key for found in result.items] == [(1,), (2,), (10,)]

    def test_keys_compare_part_by_part(self):
        records = [
            {"day": "2026-02-01", "amount": "10", "title": "t"},
            {"day": "2026-01-31", "amount": "9", "title": "t"},
            {"day": "2026-02-01", "amount": "9.5", "title": "t"},
        ]

        result = blend({(U1, OPENAI): answer(*records)}, (OPENAI,), schema=prepare(Dated))

        assert [found.key for found in result.items] == [
            (date(2026, 1, 31), Decimal("9")),
            (date(2026, 2, 1), Decimal("9.5")),
            (date(2026, 2, 1), Decimal("10")),
        ]

    def test_the_order_is_the_same_whatever_the_models_order_of_records(self):
        first = blend(
            {
                (U1, GOOGLE): answer(assembly("A-2"), assembly("A-1")),
                (U1, ANTHROPIC): answer(assembly("A-1"), assembly("A-2")),
            }
        )
        second = blend(
            {
                (U1, GOOGLE): answer(assembly("A-1"), assembly("A-2")),
                (U1, ANTHROPIC): answer(assembly("A-2"), assembly("A-1")),
            }
        )

        assert [found.key for found in first.items] == [found.key for found in second.items] == [("A-1",), ("A-2",)]

    def test_quarantined_items_come_in_unit_then_model_then_response_order(self):
        bad = assembly(title=None)
        responses = {
            (U2, GOOGLE): answer(bad),
            (U1, ANTHROPIC): answer(bad, bad),
            (U1, GOOGLE): raw(None, raw_text="x"),
        }

        result = blend(responses)

        assert [(found.unit_id, found.model_id, found.error_message[:10]) for found in result.quarantined] == [
            (U1, GOOGLE[0], "the respon"),
            (U1, ANTHROPIC[0], "records[0]"),
            (U1, ANTHROPIC[0], "records[1]"),
            (U2, GOOGLE[0], "records[0]"),
        ]


class TestMissingWithDisputes:
    def test_present_models_that_disagree_make_a_missing_key_a_disagreement(self):
        responses = {(U1, GOOGLE): answer(assembly(title="Pump")), (U1, ANTHROPIC): answer(assembly(title="Valve"))}

        found = only(blend(responses, THREE, failures=(failed(U1, OPENAI),)))

        assert (found.status, found.disputed_fields, found.record) == ("field_disagreement", ("title",), None)

    def test_present_models_that_agree_keep_their_record(self):
        responses = {(U1, GOOGLE): answer(assembly()), (U1, OPENAI): answer(assembly())}

        found = only(blend(responses, THREE))

        assert (found.status, found.record) == ("missing_in_some_models", Assembly(assembly_id="A-1", title="Pump"))
        assert dict(found.model_votes)[ANTHROPIC[0]] == ()

    def test_a_child_among_present_models_is_disputed_when_one_of_them_lacks_it(self):
        responses = {
            (U1, GOOGLE): answer(assembly(components=parts(("PN-1", 1), ("PN-2", 1)))),
            (U1, OPENAI): answer(assembly(components=parts(("PN-1", 1)))),
        }

        found = only(blend(responses, THREE))

        assert (found.status, found.disputed_fields, found.record) == (
            "field_disagreement",
            ("components['PN-2']",),
            None,
        )

    def test_must_agree_does_not_dispute_what_the_present_models_agree_on(self):
        responses = {(U1, GOOGLE): answer(assembly()), (U1, OPENAI): answer(assembly())}

        assert only(blend(responses, THREE, must_agree=("title",))).status == "missing_in_some_models"


class TestDuplicatesAcrossModels:
    def test_identical_copies_count_as_one_vote(self):
        responses = {(U1, GOOGLE): answer(assembly(), assembly(title=" Pump")), (U1, ANTHROPIC): answer(assembly())}

        found = only(blend(responses))

        assert found.status == "consensus"
        assert [len(votes) for votes in found.model_votes.values()] == [1, 1]

    def test_differing_copies_from_one_model_are_a_disagreement_naming_the_model(self):
        responses = {(U1, GOOGLE): answer(assembly()), (U1, ANTHROPIC): answer(assembly(), assembly(revision=2))}

        found = only(blend(responses))

        assert (found.status, found.record, found.disputed_fields) == ("field_disagreement", None, ("revision",))
        assert found.notes == (f"{ANTHROPIC[0]} returned this key 2 times, with differing values",)
        assert [len(votes) for votes in found.model_votes.values()] == [1, 2]


# ---------------------------------------------------------------------------------------------------------------
# Benchmark gate (maintainer-approved 2026-11-05)
# ---------------------------------------------------------------------------------------------------------------

MEDIAN_LATENCY_S = 2.0
"""A realistic median latency of one structured-output request."""
CONCURRENCY = 8
"""rationalize()'s default max_concurrency."""
BLEND_SHARE = 0.01
"""The share of a run's wall time validation and consensus may take."""
RECORDS_PER_UNIT = 10
CHILDREN_PER_RECORD = 5
TIMING_RUNS = 3


def synthetic_run(units: int) -> tuple[RunOutcome, tuple[str, ...]]:
    """A two-family run: per unit and model ~10 Assembly records of 5 components each.

    The second model disputes one component quantity per unit and leaves out a record in every seventh unit, so
    the run exercises agreement, disputes and missing keys, not only the happy path.
    """
    responses: dict[tuple[str, str], ProviderResponse] = {}
    unit_ids = tuple(f"doc:p{unit:04d}_e001-p{unit:04d}_e050" for unit in range(units))
    for unit, unit_id in enumerate(unit_ids):
        for second, model in enumerate(PAIR):
            records = [
                assembly(
                    f"A-{unit}-{index}",
                    f"Assembly {index}",
                    revision=2,
                    notes=["n1", "n2"],
                    components=[
                        {
                            "part_number": f"PN-{unit}-{index}-{child}",
                            "quantity": child + (1 if second and index == 3 and child == 2 else 0),
                            "unit_price": 1.5 * child,
                        }
                        for child in range(CHILDREN_PER_RECORD)
                    ],
                )
                for index in range(RECORDS_PER_UNIT - (1 if second and unit % 7 == 0 else 0))
            ]
            responses[(unit_id, model[0])] = raw({"records": [entry(record) for record in records]})
    return outcome(responses, PAIR), unit_ids


def light_run(units: int) -> tuple[RunOutcome, tuple[str, ...]]:
    """A two-family run of 2 one-component records per unit and model: little work per unit, so a cost that grows
    with the square of the units (a scan of every response per unit, say) dominates its time."""
    responses: dict[tuple[str, str], ProviderResponse] = {}
    unit_ids = tuple(f"doc:p{unit:04d}_e001-p{unit:04d}_e050" for unit in range(units))
    for unit, unit_id in enumerate(unit_ids):
        records = [assembly(f"A-{unit}-{index}", components=parts(("PN-1", 1))) for index in range(2)]
        for model in PAIR:
            responses[(unit_id, model[0])] = raw({"records": [entry(record) for record in records]})
    return outcome(responses, PAIR), unit_ids


def best_time(
    run: RunOutcome, unit_ids: tuple[str, ...], runs: int = TIMING_RUNS, *, collect: bool = True
) -> tuple[float, ConsensusResult]:
    """The fastest of ``runs`` runs of validation and consensus, and the result.

    With ``collect`` false the garbage collector is paused while timing, so its full collections, whose cost
    grows with every live object of the test, do not blur a scaling comparison.
    """
    best = math.inf
    result = None
    for _ in range(runs):
        gc.collect()
        if not collect:
            gc.disable()
        try:
            started = time.perf_counter()
            result = build_consensus(run, ASSEMBLY, unit_ids, min_families=2)
            best = min(best, time.perf_counter() - started)
        finally:
            gc.enable()
    assert result is not None
    return best, result


class TestBenchmarkGate:
    def test_blending_takes_at_most_one_percent_of_the_runs_wall_time(self):
        run, unit_ids = synthetic_run(1_000)
        requests = len(run.responses)
        budget_s = requests * MEDIAN_LATENCY_S / CONCURRENCY * BLEND_SHARE  # 2,000 x 2.0 s / 8 x 1% = 5 s

        elapsed, result = best_time(run, unit_ids)

        assert requests == 2_000 and budget_s == pytest.approx(5.0)
        statuses = [found.status for found in result.items]
        assert len(statuses) == 10_000
        assert statuses.count("field_disagreement") == 1_000
        assert statuses.count("missing_in_some_models") == 143
        print(f"\nvalidation + consensus, 1,000 units x 2 families x 10 records: {elapsed:.3f} s (budget {budget_s} s)")
        assert elapsed <= budget_s

    def test_twice_the_repeated_copies_of_a_key_take_about_twice_the_time(self):
        def copies_run(count: int) -> tuple[RunOutcome, tuple[str, ...]]:
            records = [assembly("A-1", f"Title {index}", revision=index) for index in range(count)]
            return outcome({(U1, OPENAI[0]): answer(*records)}, (OPENAI,)), (U1,)

        def best(run: RunOutcome, unit_ids: tuple[str, ...]) -> float:
            fastest = math.inf
            for _ in range(3):
                gc.collect()
                gc.disable()
                try:
                    started = time.perf_counter()
                    (found,) = build_consensus(run, ASSEMBLY, unit_ids, min_families=1).items
                    fastest = min(fastest, time.perf_counter() - started)
                finally:
                    gc.enable()
                assert found.disputed_fields == ("title", "revision")
            return fastest

        small_s, large_s = best(*copies_run(1_000)), best(*copies_run(2_000))

        print(f"\n1,000 copies: {small_s:.3f} s; 2,000 copies: {large_s:.3f} s; ratio {large_s / small_s:.2f}")
        assert large_s <= 2.2 * small_s

    def test_twice_the_units_take_about_twice_the_time(self):
        small, large = light_run(1_000), light_run(2_000)

        small_s, _ = best_time(*small, runs=5, collect=False)
        large_s, _ = best_time(*large, runs=5, collect=False)

        print(f"\n1,000 units: {small_s:.3f} s; 2,000 units: {large_s:.3f} s; ratio {large_s / small_s:.2f}")
        assert large_s <= 2.2 * small_s


class Gauge(BaseModel):
    gauge_id: Annotated[str, Key]
    ratio: float = 0
    amount: Decimal = 2
    ratios: list[float] = [0, 1]


GAUGE = prepare(Gauge)


class TestWidenedComparison:
    @pytest.mark.parametrize(
        "sent",
        [
            {"ratio": 0.0},
            {"amount": "2.0"},
            {"ratios": [0.0, 1.0]},
            {"ratio": 0.0, "amount": 2, "ratios": [0.0, 1]},
        ],
    )
    def test_a_widened_default_agrees_with_the_value_another_model_sent(self, sent):
        responses = {(U1, GOOGLE): answer({"gauge_id": "G"}), (U1, ANTHROPIC): answer({"gauge_id": "G", **sent})}

        found = only(blend(responses, schema=GAUGE))

        assert (found.status, found.disputed_fields) == ("consensus", ())
        assert found.record == Gauge(gauge_id="G", ratio=0.0, amount=Decimal(2), ratios=[0.0, 1.0])

    def test_a_widened_sixteen_digit_int_rounds_as_its_float_would(self):
        model = prepare(holder(float, 2**53 - 1))
        responses = {
            (U1, GOOGLE): answer({"code": "C"}),
            (U1, ANTHROPIC): answer({"code": "C", "value": 9007199254740990.0}),
        }

        assert only(blend(responses, schema=model)).status == "consensus"

    def test_a_widened_default_still_differs_from_another_number(self):
        responses = {(U1, GOOGLE): answer({"gauge_id": "G"}), (U1, ANTHROPIC): answer({"gauge_id": "G", "ratio": 0.5})}

        assert only(blend(responses, schema=GAUGE)).disputed_fields == ("ratio",)
