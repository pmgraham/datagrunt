"""Tests for ``datagrunt.rationalization.result`` (plan §3, §4.10)."""

import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Literal

import polars as pl
import pyarrow as pa
import pytest
from pdf_tests.documents import BOX
from pydantic import AwareDatetime, BaseModel, ConfigDict, NaiveDatetime, field_serializer
from pydantic import Field as PydanticField

from datagrunt.rationalization import Key, RationalizationError
from datagrunt.rationalization.consensus import QuarantinedItem
from datagrunt.rationalization.folding import ElementSource, GroundedRecord, RecordStatus
from datagrunt.rationalization.grounding import GroundingReason
from datagrunt.rationalization.result import (
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    INT64_MAX,
    INT64_MIN,
    RationalizedResult,
    RationalizeTelemetry,
    polars_schema,
)
from datagrunt.rationalization.runner import FailedUnit, ModelUsage
from datagrunt.rationalization.schema import describe, prepare
from datagrunt.rationalization.units import SkippedUnit

GOOGLE = ("gemini:gemini-3", "google", "gemini")
ANTHROPIC = ("anthropic:claude", "anthropic", "anthropic")
PAIR = (GOOGLE, ANTHROPIC)
G, A = GOOGLE[0], ANTHROPIC[0]


# ---------------------------------------------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------------------------------------------


class StatusEnum(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class PriorityEnum(int, Enum):
    LOW = 1
    HIGH = 2


class SubDetail(BaseModel):
    label: str
    weight: float | None = None


class LineItem(BaseModel):
    sku: Annotated[str, Key]
    qty: int
    unit_price: Decimal


class AllTypesRecord(BaseModel):
    record_id: Annotated[str, Key]
    count: int
    ratio: float
    flag: bool
    amount: Decimal
    issued_on: date
    recorded_at: AwareDatetime
    local_time: NaiveDatetime
    status: StatusEnum
    priority: PriorityEnum
    kind: Literal["alpha", "beta"]
    code: Literal[10, 20]
    truth: Literal[True]
    tags: list[str] = []
    detail: SubDetail | None = None
    items: list[LineItem] = []


class Employee(BaseModel):
    emp_id: Annotated[str, Key]
    name: str
    skills: list[str] = []


class Department(BaseModel):
    dept_id: Annotated[str, Key]
    dept_name: str
    codes: list[int] = []
    employees: list[Employee] = []


class Company(BaseModel):
    company_id: Annotated[str, Key]
    company_name: str
    hq: SubDetail | None = None
    aliases: list[str] = []
    departments: list[Department] = []
    line_items: list[LineItem] = []


ALL_TYPES_SCHEMA = prepare(AllTypesRecord)
COMPANY_SCHEMA = prepare(Company)


# ---------------------------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------------------------


def make_telemetry(
    *,
    document_count: int = 1,
    unit_count: int = 2,
    skipped_unit_count: int = 0,
    empty_unit_count: int = 0,
    failed_unit_count: int = 0,
    oversize_unit_count: int = 0,
    cache_hits: int = 1,
    cache_misses: int = 3,
    cache_corrupt: int = 0,
    cache_write_failures: int = 0,
    quarantined_count: int = 0,
    hidden_excluded_by_reason: dict[str, int] | None = None,
    escaped_by_category: dict[str, int] | None = None,
    usage_by_model: dict[str, ModelUsage] | None = None,
    models: tuple[tuple[str, str, str], ...] = PAIR,
) -> RationalizeTelemetry:
    usage = (
        {
            G: ModelUsage(requests=2, retries=0, input_tokens=100, output_tokens=50, latency_ms=12.5),
            A: ModelUsage(requests=2, retries=1, input_tokens=110, output_tokens=55, latency_ms=18.0),
        }
        if usage_by_model is None
        else usage_by_model
    )
    return RationalizeTelemetry(
        document_count=document_count,
        unit_count=unit_count,
        skipped_unit_count=skipped_unit_count,
        empty_unit_count=empty_unit_count,
        failed_unit_count=failed_unit_count,
        oversize_unit_count=oversize_unit_count,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        cache_corrupt=cache_corrupt,
        cache_write_failures=cache_write_failures,
        quarantined_count=quarantined_count,
        hidden_excluded_by_reason={"off_page": 2} if hidden_excluded_by_reason is None else hidden_excluded_by_reason,
        escaped_by_category={"control": 1} if escaped_by_category is None else escaped_by_category,
        usage_by_model=usage,
        models=models,
    )


def make_grounded(
    record: Any,
    *,
    doc_id: str = "doc_1",
    unit_id: str = "doc_1:p0001_e001-p0001_e001",
    status: RecordStatus = "consensus",
    key: tuple[object, ...] = ("R-1",),
    disputed_fields: tuple[str, ...] = (),
    notes: tuple[str, ...] = (),
    grounding_reasons: tuple[GroundingReason, ...] = (),
) -> GroundedRecord[Any]:
    source = ElementSource(
        doc_id=doc_id,
        unit_id=unit_id,
        element_id="p0001_e001",
        page_number=1,
        bbox=BOX,
        scope_path=("Section 1",),
    )
    vote = record.model_dump(mode="json") if record is not None else {"record_id": "R-1"}
    model_ids = (G,) if status == "single_model" else (G, A)
    family_map = {G: "google", A: "anthropic"}
    return GroundedRecord(
        doc_id=doc_id,
        unit_ids=(unit_id,),
        status=status,
        key=key,
        record=record,
        disputed_fields=disputed_fields,
        notes=notes,
        grounding_reasons=grounding_reasons,
        sources=(source,),
        grounding_quotes=("quote one two three",),
        model_votes={m: (vote,) for m in model_ids},
        quotes_by_model={m: ("quote one two three",) for m in model_ids},
        element_ids_by_model={m: ("p0001_e001",) for m in model_ids},
        families={m: family_map[m] for m in model_ids},
    )


def sample_all_types(
    record_id: str = "R-1",
    *,
    count: int = 42,
    amount: Decimal = Decimal("1234.560000000"),
    recorded_at: datetime = datetime(2026, 5, 10, 14, 30, tzinfo=timezone(timedelta(hours=2))),
    tags: list[str] | None = None,
    detail: SubDetail | None = None,
    items: list[LineItem] | None = None,
) -> AllTypesRecord:
    return AllTypesRecord(
        record_id=record_id,
        count=count,
        ratio=3.5,
        flag=True,
        amount=amount,
        issued_on=date(2026, 5, 10),
        recorded_at=recorded_at,
        local_time=datetime(2026, 5, 10, 9, 15, 0),
        status=StatusEnum.ACTIVE,
        priority=PriorityEnum.HIGH,
        kind="alpha",
        code=10,
        truth=True,
        tags=["t1", "t2"] if tags is None else tags,
        detail=SubDetail(label="primary", weight=1.25) if detail is None else detail,
        items=[LineItem(sku="SKU-1", qty=3, unit_price=Decimal("9.99"))] if items is None else items,
    )


# ---------------------------------------------------------------------------------------------------------------
# Constants & RationalizeTelemetry
# ---------------------------------------------------------------------------------------------------------------


def test_constants() -> None:
    assert DECIMAL_PRECISION == 38
    assert DECIMAL_SCALE == 9
    assert INT64_MIN == -(1 << 63)
    assert INT64_MAX == (1 << 63) - 1


def test_telemetry_valid_and_read_only_mappings() -> None:
    telemetry = make_telemetry()
    assert telemetry.document_count == 1
    assert telemetry.unit_count == 2
    assert isinstance(telemetry.hidden_excluded_by_reason, MappingProxyType)
    assert isinstance(telemetry.escaped_by_category, MappingProxyType)
    assert isinstance(telemetry.usage_by_model, MappingProxyType)
    with pytest.raises(TypeError):
        telemetry.hidden_excluded_by_reason["off_page"] = 99  # type: ignore[index]


@pytest.mark.parametrize(
    "counter_name",
    [
        "document_count",
        "unit_count",
        "skipped_unit_count",
        "empty_unit_count",
        "failed_unit_count",
        "oversize_unit_count",
        "cache_hits",
        "cache_misses",
        "cache_corrupt",
        "cache_write_failures",
        "quarantined_count",
    ],
)
def test_telemetry_counter_validation(counter_name: str) -> None:
    with pytest.raises(TypeError):
        make_telemetry(**{counter_name: True})  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        make_telemetry(**{counter_name: -1})


def test_telemetry_mapping_and_models_validation() -> None:
    with pytest.raises((TypeError, ValueError)):
        make_telemetry(hidden_excluded_by_reason={"not_a_reason": 1})
    with pytest.raises((TypeError, ValueError)):
        make_telemetry(hidden_excluded_by_reason={"off_page": -1})
    with pytest.raises(TypeError):
        make_telemetry(hidden_excluded_by_reason={"off_page": True})  # type: ignore[dict-item]
    with pytest.raises((TypeError, ValueError)):
        make_telemetry(escaped_by_category={"unknown_cat": 1})
    with pytest.raises((TypeError, ValueError)):
        make_telemetry(escaped_by_category={"control": -1})
    with pytest.raises(TypeError):
        make_telemetry(usage_by_model={G: "not_usage"})  # type: ignore[dict-item]
    with pytest.raises((TypeError, ValueError)):
        make_telemetry(models=(GOOGLE,))
    with pytest.raises((TypeError, ValueError)):
        make_telemetry(models=((" ", "google", "gemini"), ANTHROPIC))


# ---------------------------------------------------------------------------------------------------------------
# polars_schema
# ---------------------------------------------------------------------------------------------------------------


def test_polars_schema_all_supported_types() -> None:
    expected: dict[str, pl.DataType] = {
        "record_id": pl.String(),
        "count": pl.Int64(),
        "ratio": pl.Float64(),
        "flag": pl.Boolean(),
        "amount": pl.Decimal(precision=38, scale=9),
        "issued_on": pl.Date(),
        "recorded_at": pl.Datetime("us", "UTC"),
        "local_time": pl.Datetime("us"),
        "status": pl.String(),
        "priority": pl.Int64(),
        "kind": pl.String(),
        "code": pl.Int64(),
        "truth": pl.Boolean(),
        "tags": pl.List(pl.String()),
        "detail": pl.Struct({"label": pl.String(), "weight": pl.Float64()}),
        "items": pl.List(
            pl.Struct(
                {
                    "sku": pl.String(),
                    "qty": pl.Int64(),
                    "unit_price": pl.Decimal(precision=38, scale=9),
                }
            )
        ),
    }
    assert polars_schema(ALL_TYPES_SCHEMA.spec) == expected
    assert polars_schema(ALL_TYPES_SCHEMA) == expected
    assert polars_schema(AllTypesRecord) == expected


def test_polars_schema_rejects_invalid_input() -> None:
    with pytest.raises(TypeError):
        polars_schema("not a schema")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# RationalizedResult validation
# ---------------------------------------------------------------------------------------------------------------


def test_rationalized_result_invariants() -> None:
    rec1 = sample_all_types("R-1")
    rec2 = sample_all_types("R-2")
    item_clean = make_grounded(rec1, status="consensus", key=("R-1",))
    item_disputed = make_grounded(rec2, status="missing_in_some_models", key=("R-2",))
    item_conflict = make_grounded(
        None,
        status="field_disagreement",
        key=("R-3",),
        disputed_fields=("count",),
    )
    quarantined = (
        QuarantinedItem(
            model_id=G,
            unit_id="doc_1:p0001_e001-p0001_e001",
            reason="truncated",
            raw_json="{}",
            error_message="response truncated",
        ),
    )
    failed = (
        FailedUnit(
            unit_id="doc_1:p0001_e001-p0001_e001",
            model_id=A,
            status_code=503,
            error_message="service unavailable",
            attempts=2,
        ),
    )
    skipped = (SkippedUnit(unit_id="doc_1:p0002", reason="page_error", page_numbers=(2,)),)
    telemetry = make_telemetry(quarantined_count=1, failed_unit_count=1, skipped_unit_count=1)

    result = RationalizedResult(
        items=(item_clean, item_disputed, item_conflict),
        records=(rec1,),
        needs_review=(item_disputed, item_conflict),
        quarantined=quarantined,
        failed_units=failed,
        empty_units=("doc_1:p0003_e001-p0003_e001",),
        skipped_units=skipped,
        telemetry=telemetry,
        schema=ALL_TYPES_SCHEMA,
    )
    assert result.records == (rec1,)
    assert result.needs_review == (item_disputed, item_conflict)

    # Mismatched records tuple raises ValueError
    with pytest.raises(ValueError, match="records"):
        RationalizedResult(
            items=(item_clean, item_disputed),
            records=(rec1, rec2),
            needs_review=(item_disputed,),
            quarantined=(),
            failed_units=(),
            empty_units=(),
            skipped_units=(),
            telemetry=telemetry,
        )

    # Mismatched needs_review tuple raises ValueError
    with pytest.raises(ValueError, match="needs_review"):
        RationalizedResult(
            items=(item_clean, item_disputed),
            records=(rec1,),
            needs_review=(),
            quarantined=(),
            failed_units=(),
            empty_units=(),
            skipped_units=(),
            telemetry=telemetry,
        )


# ---------------------------------------------------------------------------------------------------------------
# to_dataframe, to_dicts, to_arrow_table
# ---------------------------------------------------------------------------------------------------------------


def test_to_dataframe_dtypes_and_utc_conversion_without_mutating_records() -> None:
    tz_plus_2 = timezone(timedelta(hours=2))
    orig_dt = datetime(2026, 5, 10, 14, 30, 0, tzinfo=tz_plus_2)
    rec1 = sample_all_types("R-1", recorded_at=orig_dt)
    rec2 = sample_all_types("R-2", count=7)
    item1 = make_grounded(rec1, status="consensus", key=("R-1",))
    item2 = make_grounded(rec2, status="missing_in_some_models", key=("R-2",))
    item3 = make_grounded(None, status="field_disagreement", key=("R-3",), disputed_fields=("count",))

    result = RationalizedResult(
        items=(item1, item2, item3),
        records=(rec1,),
        needs_review=(item2, item3),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=ALL_TYPES_SCHEMA,
    )

    df = result.to_dataframe()
    assert df.height == 1
    assert dict(df.schema) == polars_schema(ALL_TYPES_SCHEMA)
    assert df["record_id"].to_list() == ["R-1"]
    assert df["status"].to_list() == ["active"]
    assert df["priority"].to_list() == [2]
    assert df["amount"].to_list() == [Decimal("1234.560000000")]
    assert df["recorded_at"].to_list() == [datetime(2026, 5, 10, 12, 30, 0, tzinfo=timezone.utc)]

    # Original record retains its +02:00 offset unchanged
    assert result.records[0].recorded_at == orig_dt
    assert result.records[0].recorded_at.utcoffset() == timedelta(hours=2)

    # include_disputed=True includes non-None disputed items and adds _status and _doc_id
    df_disputed = result.to_dataframe(include_disputed=True)
    assert df_disputed.height == 2
    assert df_disputed["record_id"].to_list() == ["R-1", "R-2"]
    assert df_disputed["_status"].to_list() == ["consensus", "missing_in_some_models"]
    assert df_disputed["_doc_id"].to_list() == ["doc_1", "doc_1"]
    assert df_disputed.schema["_status"] == pl.String()
    assert df_disputed.schema["_doc_id"] == pl.String()


def test_to_dataframe_empty_result_preserves_schema() -> None:
    empty_result: RationalizedResult[Company] = RationalizedResult(
        items=(),
        records=(),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=Company,
    )
    df = empty_result.to_dataframe()
    assert df.height == 0
    assert dict(df.schema) == polars_schema(COMPANY_SCHEMA)

    df_disp = empty_result.to_dataframe(include_disputed=True)
    assert df_disp.height == 0
    expected_disp = {**polars_schema(COMPANY_SCHEMA), "_status": pl.String(), "_doc_id": pl.String()}
    assert dict(df_disp.schema) == expected_disp

    df_flat = empty_result.to_dataframe(flatten="departments.employees")
    assert df_flat.height == 0
    assert "departments.dept_id" in df_flat.schema
    assert "departments.employees.emp_id" in df_flat.schema
    assert "aliases" not in df_flat.schema
    assert "line_items" not in df_flat.schema
    assert "departments.codes" not in df_flat.schema


def test_to_dataframe_int64_and_decimal_bounds() -> None:
    for bad_int in (INT64_MAX + 1, INT64_MIN - 1):
        rec = sample_all_types("R-1", count=bad_int)
        item = make_grounded(rec, status="consensus", key=("R-1",))
        res = RationalizedResult(
            items=(item,),
            records=(rec,),
            needs_review=(),
            quarantined=(),
            failed_units=(),
            empty_units=(),
            skipped_units=(),
            telemetry=make_telemetry(),
            schema=ALL_TYPES_SCHEMA,
        )
        with pytest.raises(RationalizationError, match="count"):
            res.to_dataframe()

    # Nested int out of range
    rec_nested_int = sample_all_types(
        "R-1",
        items=[LineItem(sku="S-1", qty=INT64_MAX + 1, unit_price=Decimal("1.00"))],
    )
    item_nested = make_grounded(rec_nested_int, status="consensus", key=("R-1",))
    res_nested = RationalizedResult(
        items=(item_nested,),
        records=(rec_nested_int,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=ALL_TYPES_SCHEMA,
    )
    with pytest.raises(RationalizationError, match=r"items.*qty"):
        res_nested.to_dataframe()

    # Decimal scale > 9 or integer digits > 29
    for bad_dec in (Decimal("1.0000000001"), Decimal("1" + "0" * 29)):
        rec_dec = sample_all_types("R-1", amount=bad_dec)
        item_dec = make_grounded(rec_dec, status="consensus", key=("R-1",))
        res_dec = RationalizedResult(
            items=(item_dec,),
            records=(rec_dec,),
            needs_review=(),
            quarantined=(),
            failed_units=(),
            empty_units=(),
            skipped_units=(),
            telemetry=make_telemetry(),
            schema=ALL_TYPES_SCHEMA,
        )
        with pytest.raises(RationalizationError, match="amount"):
            res_dec.to_dataframe()


def test_to_dataframe_flatten_one_level_two_level_and_scalar_list() -> None:
    c1 = Company(
        company_id="C-1",
        company_name="Acme",
        hq=SubDetail(label="HQ-1", weight=10.0),
        aliases=["Acme Corp", "ACME"],
        departments=[
            Department(
                dept_id="D-1",
                dept_name="Engineering",
                codes=[100, 200],
                employees=[
                    Employee(emp_id="E-1", name="Ada", skills=["python", "rust"]),
                    Employee(emp_id="E-2", name="Grace", skills=["cobol"]),
                ],
            ),
            Department(
                dept_id="D-2",
                dept_name="EmptyDept",
                codes=[300],
                employees=[],
            ),
        ],
        line_items=[LineItem(sku="L-1", qty=1, unit_price=Decimal("5.00"))],
    )
    c2 = Company(
        company_id="C-2",
        company_name="Solo",
        hq=None,
        aliases=[],
        departments=[],
        line_items=[],
    )
    item1 = make_grounded(c1, status="consensus", key=("C-1",))
    item2 = make_grounded(c2, status="single_model", key=("C-2",))
    result = RationalizedResult(
        items=(item1, item2),
        records=(c1, c2),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=COMPANY_SCHEMA,
    )

    # 1-level model_list flatten="line_items": drops sibling lists (aliases, departments), left-outer explodes
    df_lines = result.to_dataframe(flatten="line_items")
    assert "aliases" not in df_lines.columns
    assert "departments" not in df_lines.columns
    assert df_lines["company_id"].to_list() == ["C-1", "C-2"]
    assert df_lines["line_items.sku"].to_list() == ["L-1", None]
    assert df_lines["line_items.qty"].to_list() == [1, None]

    # 2-level nested model_list flatten="departments.employees": drops sibling lists at both levels
    df_emp = result.to_dataframe(flatten="departments.employees")
    assert "aliases" not in df_emp.columns
    assert "line_items" not in df_emp.columns
    assert "departments.codes" not in df_emp.columns
    assert df_emp["company_id"].to_list() == ["C-1", "C-1", "C-1", "C-2"]
    assert df_emp["departments.dept_id"].to_list() == ["D-1", "D-1", "D-2", None]
    assert df_emp["departments.employees.emp_id"].to_list() == ["E-1", "E-2", None, None]
    assert df_emp["departments.employees.name"].to_list() == ["Ada", "Grace", None, None]
    assert df_emp["departments.employees.skills"].to_list() == [["python", "rust"], ["cobol"], None, None]

    # scalar_list flatten="aliases"
    df_aliases = result.to_dataframe(flatten="aliases")
    assert "departments" not in df_aliases.columns
    assert "line_items" not in df_aliases.columns
    assert df_aliases["company_id"].to_list() == ["C-1", "C-1", "C-2"]
    assert df_aliases["aliases"].to_list() == ["Acme Corp", "ACME", None]


@pytest.mark.parametrize(
    "bad_path",
    [
        "",
        "   ",
        "unknown_field",
        "company_name",
        "hq",
        "hq.label",
        "aliases.sub",
        "departments.unknown",
        "departments.dept_name",
    ],
)
def test_to_dataframe_rejects_invalid_flatten_path(bad_path: str) -> None:
    c1 = Company(company_id="C-1", company_name="Acme")
    item1 = make_grounded(c1, status="consensus", key=("C-1",))
    result = RationalizedResult(
        items=(item1,),
        records=(c1,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=describe(Company),
    )
    with pytest.raises(RationalizationError):
        result.to_dataframe(flatten=bad_path)


def test_to_dicts_and_to_arrow_table() -> None:
    rec1 = sample_all_types("R-1")
    rec2 = sample_all_types("R-2", count=99)
    item1 = make_grounded(rec1, status="consensus", key=("R-1",))
    item2 = make_grounded(
        rec2,
        status="ungrounded_value",
        key=("R-2",),
        grounding_reasons=(GroundingReason(reason="no_quotes", model_id=G),),
    )
    item3 = make_grounded(None, status="cross_page_conflict", key=("R-3",), disputed_fields=("count",))

    result = RationalizedResult(
        items=(item1, item2, item3),
        records=(rec1,),
        needs_review=(item2, item3),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=ALL_TYPES_SCHEMA,
    )

    dicts_clean = result.to_dicts()
    assert dicts_clean == [rec1.model_dump(mode="python")]

    dicts_disputed = result.to_dicts(include_disputed=True)
    assert len(dicts_disputed) == 2
    assert dicts_disputed[0] == {
        **rec1.model_dump(mode="python"),
        "_status": "consensus",
        "_doc_id": "doc_1",
    }
    assert dicts_disputed[1] == {
        **rec2.model_dump(mode="python"),
        "_status": "ungrounded_value",
        "_doc_id": "doc_1",
    }

    arrow_tbl = result.to_arrow_table(flatten="items", include_disputed=True)
    assert isinstance(arrow_tbl, pa.Table)
    expected_tbl = result.to_dataframe(flatten="items", include_disputed=True).to_arrow()
    assert arrow_tbl.equals(expected_tbl)


def test_spec_cached_and_flatten_skips_dropped_sibling_lists() -> None:
    bad_sibling_company = Company(
        company_id="C-9",
        company_name="SkipCo",
        departments=[
            Department(
                dept_id="D-9",
                dept_name="R&D",
                codes=[INT64_MAX + 1],
                employees=[Employee(emp_id="E-9", name="Alan", skills=["math"])],
            ),
            Department(dept_id="D-10", dept_name="Ops", codes=[], employees=[]),
        ],
        line_items=[LineItem(sku="BAD", qty=INT64_MAX + 1, unit_price=Decimal("1.00"))],
    )
    item_clean = make_grounded(bad_sibling_company, status="consensus", key=("C-9",))
    item_disp = make_grounded(
        Company(company_id="C-10", company_name="EmptyCo"),
        doc_id="doc_2",
        status="missing_in_some_models",
        key=("C-10",),
    )
    res = RationalizedResult(
        items=(item_clean, item_disp),
        records=(bad_sibling_company,),
        needs_review=(item_disp,),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
    )
    assert res._optional_spec() is res._optional_spec()
    df = res.to_dataframe(flatten="departments.employees", include_disputed=True)
    assert df["company_id"].to_list() == ["C-9", "C-9", "C-10"]
    assert df["departments.dept_id"].to_list() == ["D-9", "D-10", None]
    assert df["departments.employees.emp_id"].to_list() == ["E-9", None, None]
    assert df["_status"].to_list() == ["consensus", "consensus", "missing_in_some_models"]
    assert df["_doc_id"].to_list() == ["doc_1", "doc_1", "doc_2"]


def test_lazy_error_paths_across_unflattened_and_flattened_levels() -> None:
    bad_co = Company(
        company_id="C-9",
        company_name="SkipCo",
        departments=[Department(dept_id="D-9", dept_name="R&D", codes=[INT64_MAX + 1], employees=[])],
        line_items=[LineItem(sku="BAD", qty=INT64_MAX + 1, unit_price=Decimal("1.00"))],
    )
    item = make_grounded(bad_co, status="consensus", key=("C-9",))
    res = RationalizedResult(
        items=(item,),
        records=(bad_co,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
    )
    with pytest.raises(RationalizationError, match=r"^departments\[0\]\.codes\[0\]: int value is outside Int64"):
        res.to_dataframe(flatten="departments.codes")
    with pytest.raises(RationalizationError, match=r"^line_items\[0\]\.qty: int value is outside Int64"):
        res.to_dataframe(flatten="line_items")
    with pytest.raises(RationalizationError, match=r"^departments\[0\]\.codes\[0\]: int value is outside Int64"):
        res.to_dataframe()


def test_grounded_union_properties_and_exports_exclude_ungrounded_and_disputed() -> None:
    c_consensus = Company(
        company_id="C-1",
        company_name="ConsensusCo",
        departments=[
            Department(
                dept_id="D-1",
                dept_name="Core",
                employees=[Employee(emp_id="E-1", name="Ada")],
            )
        ],
    )
    c_grounded_partial = Company(
        company_id="SMBJ18A",
        company_name="SingleModelGrounded",
        departments=[
            Department(
                dept_id="D-2",
                dept_name="Analog",
                employees=[Employee(emp_id="E-2", name="Grace")],
            )
        ],
    )
    c_ungrounded_partial = Company(company_id="FAKE-1", company_name="HallucinatedKey")
    c_ungrounded_val = Company(company_id="C-4", company_name="BadQuote")
    c_disputed = Company(company_id="C-5", company_name="Disputed")

    it_consensus = make_grounded(c_consensus, status="consensus", key=("C-1",))
    it_grounded_partial = make_grounded(
        c_grounded_partial,
        doc_id="doc_2",
        unit_id="doc_2:p0001_e001-p0001_e001",
        status="missing_in_some_models",
        key=("SMBJ18A",),
    )
    it_ungrounded_partial = make_grounded(
        c_ungrounded_partial,
        status="missing_in_some_models",
        key=("FAKE-1",),
        grounding_reasons=(GroundingReason(reason="key_not_found", model_id=G, key_field="company_id"),),
    )
    it_ungrounded_val = make_grounded(
        c_ungrounded_val,
        status="ungrounded_value",
        key=("C-4",),
        grounding_reasons=(GroundingReason(reason="no_quotes", model_id=G),),
    )
    it_disputed = make_grounded(
        c_disputed,
        status="field_disagreement",
        key=("C-5",),
        disputed_fields=("company_name",),
    )

    result = RationalizedResult(
        items=(it_consensus, it_grounded_partial, it_ungrounded_partial, it_ungrounded_val, it_disputed),
        records=(c_consensus,),
        needs_review=(it_grounded_partial, it_ungrounded_partial, it_ungrounded_val, it_disputed),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=COMPANY_SCHEMA,
    )

    assert result.grounded_items == (it_consensus, it_grounded_partial)
    assert [item.status for item in result.grounded_items] == ["consensus", "missing_in_some_models"]
    assert [item.sources[0].element_id for item in result.grounded_items] == ["p0001_e001", "p0001_e001"]
    assert result.grounded_records == (c_consensus, c_grounded_partial)

    df_gu = result.to_dataframe(include_grounded_union=True)
    assert df_gu["company_id"].to_list() == ["C-1", "SMBJ18A"]
    assert df_gu["_status"].to_list() == ["consensus", "missing_in_some_models"]
    assert df_gu["_doc_id"].to_list() == ["doc_1", "doc_2"]

    df_gu_flat = result.to_dataframe(flatten="departments.employees", include_grounded_union=True)
    assert df_gu_flat["company_id"].to_list() == ["C-1", "SMBJ18A"]
    assert df_gu_flat["departments.employees.emp_id"].to_list() == ["E-1", "E-2"]
    assert df_gu_flat["_status"].to_list() == ["consensus", "missing_in_some_models"]
    assert df_gu_flat["_doc_id"].to_list() == ["doc_1", "doc_2"]

    dicts_gu = result.to_dicts(include_grounded_union=True)
    assert [row["company_id"] for row in dicts_gu] == ["C-1", "SMBJ18A"]
    assert [row["_status"] for row in dicts_gu] == ["consensus", "missing_in_some_models"]
    assert [row["_doc_id"] for row in dicts_gu] == ["doc_1", "doc_2"]

    arrow_gu = result.to_arrow_table(include_grounded_union=True)
    assert arrow_gu.equals(df_gu.to_arrow())

    empty_res: RationalizedResult[Company] = RationalizedResult(
        items=(),
        records=(),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=COMPANY_SCHEMA,
    )
    empty_gu = empty_res.to_dataframe(include_grounded_union=True)
    assert empty_gu.height == 0
    assert "_status" in empty_gu.schema and "_doc_id" in empty_gu.schema

    for bad_flag in (1, "yes", None):
        with pytest.raises(TypeError, match="include_grounded_union"):
            result.to_dataframe(include_grounded_union=bad_flag)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="include_grounded_union"):
            result.to_dicts(include_grounded_union=bad_flag)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="include_grounded_union"):
            result.to_arrow_table(include_grounded_union=bad_flag)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# RationalizeTelemetry.to_dict() & RationalizedResult.to_jsonl()
# ---------------------------------------------------------------------------------------------------------------


def test_telemetry_to_dict_json_roundtrip_and_isolation() -> None:
    telemetry = make_telemetry(
        document_count=3,
        unit_count=7,
        skipped_unit_count=1,
        empty_unit_count=2,
        failed_unit_count=1,
        oversize_unit_count=1,
        cache_hits=4,
        cache_misses=10,
        cache_corrupt=1,
        cache_write_failures=2,
        quarantined_count=3,
        hidden_excluded_by_reason={"off_page": 2, "tiny_font": 5},
        escaped_by_category={"control": 1, "bidi": 4},
    )
    as_dict = telemetry.to_dict()
    expected = {
        "document_count": 3,
        "unit_count": 7,
        "skipped_unit_count": 1,
        "empty_unit_count": 2,
        "failed_unit_count": 1,
        "oversize_unit_count": 1,
        "cache_hits": 4,
        "cache_misses": 10,
        "cache_corrupt": 1,
        "cache_write_failures": 2,
        "quarantined_count": 3,
        "hidden_excluded_by_reason": {"off_page": 2, "tiny_font": 5},
        "escaped_by_category": {"control": 1, "bidi": 4},
        "models": [
            {"model_id": G, "model_family": "google", "provider_kind": "gemini"},
            {"model_id": A, "model_family": "anthropic", "provider_kind": "anthropic"},
        ],
        "usage_by_model": {
            G: {"requests": 2, "retries": 0, "input_tokens": 100, "output_tokens": 50, "latency_ms": 12.5},
            A: {"requests": 2, "retries": 1, "input_tokens": 110, "output_tokens": 55, "latency_ms": 18.0},
        },
    }
    assert as_dict == expected
    assert json.loads(json.dumps(as_dict, allow_nan=False)) == expected

    # Mutating returned containers never mutates the frozen telemetry instance.
    as_dict["hidden_excluded_by_reason"]["off_page"] = 999
    as_dict["escaped_by_category"]["control"] = 999
    as_dict["models"].clear()
    as_dict["usage_by_model"][G]["requests"] = 999
    assert telemetry.to_dict() == expected


def _make_named_company_item(
    company: Company,
    *,
    doc_id: str,
    status: RecordStatus,
    source_name: str,
    quote: str,
    sources: tuple[ElementSource, ...],
    both_models: bool,
) -> GroundedRecord[Company]:
    dump = company.model_dump(mode="json")
    a_votes = (dump,) if both_models else ()
    a_quotes = (quote,) if both_models else ()
    a_eids = ("p0001_e001",) if both_models else ()
    return GroundedRecord(
        doc_id=doc_id,
        unit_ids=(f"{doc_id}:u1",),
        status=status,
        key=(company.company_id,),
        record=company,
        disputed_fields=(),
        notes=(),
        grounding_reasons=(),
        sources=sources,
        grounding_quotes=(quote,),
        model_votes={G: (dump,), A: a_votes},
        quotes_by_model={G: (quote,), A: a_quotes},
        element_ids_by_model={G: ("p0001_e001",), A: a_eids},
        families={G: "google", A: "anthropic"},
        source_name=source_name,
    )


def _build_jsonl_fixture() -> RationalizedResult[Company]:
    c_clean = Company(company_id="C-1", company_name="Café Ω", aliases=["A1"])
    c_partial = Company(company_id="C-2", company_name="GroundedUnionCo")
    c_ungrounded = Company(company_id="C-3", company_name="UngroundedCo")
    src1 = ElementSource(
        doc_id="doc_1",
        unit_id="doc_1:u1",
        element_id="p0001_e001",
        page_number=1,
        bbox=BOX,
        scope_path=("Sec 1",),
    )
    it_clean = _make_named_company_item(
        c_clean,
        doc_id="doc_1",
        status="consensus",
        source_name="alpha_bom.pdf",
        quote="Café Ω",
        sources=(src1,),
        both_models=True,
    )
    it_partial = _make_named_company_item(
        c_partial,
        doc_id="doc_2",
        status="missing_in_some_models",
        source_name="beta_scan.pdf",
        quote="C-2 GroundedUnionCo",
        sources=(),
        both_models=False,
    )
    reason = GroundingReason(reason="quote_not_found", model_id=G, index=0, text="hallucinated")
    it_ungrounded = make_grounded(c_ungrounded, status="ungrounded_value", key=("C-3",), grounding_reasons=(reason,))
    it_conflict = make_grounded(
        None,
        status="cross_page_conflict",
        key=("C-4",),
        disputed_fields=("company_name",),
        notes=("conflict across pages",),
    )
    return RationalizedResult(
        items=(it_clean, it_partial, it_ungrounded, it_conflict),
        records=(c_clean,),
        needs_review=(it_partial, it_ungrounded, it_conflict),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=COMPANY_SCHEMA,
    )


def test_to_jsonl_default_provenance_and_audit_rows(tmp_path: Path) -> None:
    result = _build_jsonl_fixture()
    out_file = tmp_path / "audit.jsonl"
    written = result.to_jsonl(out_file)
    assert written == 4

    raw_text = out_file.read_text(encoding="utf-8")
    assert raw_text.endswith("\n")
    assert "Café Ω" in raw_text  # ensure_ascii=False preserves UTF-8 text verbatim
    rows = [json.loads(line) for line in raw_text.splitlines()]
    assert len(rows) == 4

    assert rows[0]["company_id"] == "C-1"
    assert rows[0]["_doc_id"] == "doc_1"
    assert rows[0]["_source_name"] == "alpha_bom.pdf"
    assert rows[0]["_status"] == "consensus"
    assert rows[0]["_is_grounded"] is True
    assert rows[0]["_is_grounded_union"] is True
    assert rows[0]["_unit_ids"] == ["doc_1:u1"]
    assert rows[0]["_supporting_models"] == [G, A]
    assert rows[0]["_grounding_quotes"] == ["Café Ω"]
    assert rows[0]["_disputed_fields"] == []
    assert rows[0]["_notes"] == []
    assert rows[0]["_grounding_reasons"] == []
    assert rows[0]["_sources"] == [
        {
            "doc_id": "doc_1",
            "unit_id": "doc_1:u1",
            "element_id": "p0001_e001",
            "page_number": 1,
            "bbox": {"x0": BOX.x0, "y0": BOX.y0, "x1": BOX.x1, "y1": BOX.y1},
            "scope_path": ["Sec 1"],
        }
    ]

    assert rows[1]["_supporting_models"] == [G]
    assert rows[1]["_source_name"] == "beta_scan.pdf"
    assert rows[1]["_is_grounded_union"] is True

    assert rows[2]["_status"] == "ungrounded_value"
    assert rows[2]["_is_grounded"] is False
    assert rows[2]["_grounding_reasons"] == [
        {"reason": "quote_not_found", "model_id": G, "index": 0, "key_field": None, "text": "hallucinated"}
    ]

    # record=None audit row starts from {} and preserves full provenance
    assert "company_id" not in rows[3]
    assert rows[3]["_status"] == "cross_page_conflict"
    assert rows[3]["_disputed_fields"] == ["company_name"]
    assert rows[3]["_notes"] == ["conflict across pages"]


def test_to_jsonl_disputed_and_grounded_union_tiers(tmp_path: Path) -> None:
    result = _build_jsonl_fixture()

    p_disp_noprov = tmp_path / "disp_noprov.jsonl"
    assert result.to_jsonl(str(p_disp_noprov), include_provenance=False, include_disputed=True) == 3
    rows_disp = [json.loads(ln) for ln in p_disp_noprov.read_text(encoding="utf-8").splitlines()]
    assert [r["company_id"] for r in rows_disp] == ["C-1", "C-2", "C-3"]
    assert [r["_status"] for r in rows_disp] == ["consensus", "missing_in_some_models", "ungrounded_value"]
    assert [r["_doc_id"] for r in rows_disp] == ["doc_1", "doc_2", "doc_1"]
    assert all("_sources" not in r and "_source_name" not in r for r in rows_disp)

    p_gu_prov = tmp_path / "gu_prov.jsonl"
    assert result.to_jsonl(p_gu_prov, include_disputed=False, include_grounded_union=True) == 2
    rows_gu_prov = [json.loads(ln) for ln in p_gu_prov.read_text(encoding="utf-8").splitlines()]
    assert [r["company_id"] for r in rows_gu_prov] == ["C-1", "C-2"]
    assert [r["_source_name"] for r in rows_gu_prov] == ["alpha_bom.pdf", "beta_scan.pdf"]

    p_gu_noprov = tmp_path / "gu_noprov.jsonl"
    assert (
        result.to_jsonl(p_gu_noprov, include_provenance=False, include_disputed=False, include_grounded_union=True) == 2
    )
    rows_gu_noprov = [json.loads(ln) for ln in p_gu_noprov.read_text(encoding="utf-8").splitlines()]
    assert [(r["company_id"], r["_status"], r["_doc_id"]) for r in rows_gu_noprov] == [
        ("C-1", "consensus", "doc_1"),
        ("C-2", "missing_in_some_models", "doc_2"),
    ]
    assert all("_sources" not in r for r in rows_gu_noprov)


def test_to_jsonl_clean_tier_and_empty_result(tmp_path: Path) -> None:
    result = _build_jsonl_fixture()

    p_clean_prov = tmp_path / "clean_prov.jsonl"
    assert result.to_jsonl(p_clean_prov, include_disputed=False) == 1
    (row_clean_prov,) = [json.loads(ln) for ln in p_clean_prov.read_text(encoding="utf-8").splitlines()]
    assert row_clean_prov["company_id"] == "C-1" and row_clean_prov["_source_name"] == "alpha_bom.pdf"

    p_clean_bare = tmp_path / "clean_bare.jsonl"
    assert result.to_jsonl(p_clean_bare, include_provenance=False, include_disputed=False) == 1
    (row_clean_bare,) = [json.loads(ln) for ln in p_clean_bare.read_text(encoding="utf-8").splitlines()]
    assert row_clean_bare == result.records[0].model_dump(mode="json")

    empty_res: RationalizedResult[Any] = RationalizedResult(
        items=(),
        records=(),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
    )
    p_empty = tmp_path / "empty.jsonl"
    assert empty_res.to_jsonl(p_empty) == 0
    assert p_empty.read_text(encoding="utf-8") == ""


def test_to_jsonl_validation_and_atomic_replace_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _build_jsonl_fixture()
    valid_target = tmp_path / "out.jsonl"

    for bad_path in (None, 123, b"out.jsonl"):
        with pytest.raises(TypeError, match="path"):
            result.to_jsonl(bad_path)  # type: ignore[arg-type]
    for blank_path in ("", "   ", "bad\x00name.jsonl"):
        with pytest.raises(ValueError, match="path"):
            result.to_jsonl(blank_path)

    for bad_bool in (1, 0, "true", None):
        with pytest.raises(TypeError, match="include_provenance"):
            result.to_jsonl(valid_target, include_provenance=bad_bool)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="include_disputed"):
            result.to_jsonl(valid_target, include_disputed=bad_bool)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="include_grounded_union"):
            result.to_jsonl(valid_target, include_grounded_union=bad_bool)  # type: ignore[arg-type]

    # Missing parent directory and directory target raise RationalizationError and leave no staging files
    with pytest.raises(RationalizationError):
        result.to_jsonl(tmp_path / "missing_parent" / "out.jsonl")
    with pytest.raises(RationalizationError):
        result.to_jsonl(tmp_path)
    assert list(tmp_path.iterdir()) == []

    # Simulated os.replace OSError preserves pre-existing target and unlinks staging file
    existing = tmp_path / "existing.jsonl"
    existing.write_text("KEEP_ME\n", encoding="utf-8")

    def boom_replace(src: object, dst: object) -> None:
        raise OSError("simulated disk error on replace")

    monkeypatch.setattr(os, "replace", boom_replace)
    with pytest.raises(RationalizationError):
        result.to_jsonl(existing)
    assert existing.read_text(encoding="utf-8") == "KEEP_ME\n"
    assert [p.name for p in tmp_path.iterdir()] == ["existing.jsonl"]


# ---------------------------------------------------------------------------------------------------------------
# Per-model audit exports: nested _model_outputs on union/provenance rows + flat by_model=True views
# ---------------------------------------------------------------------------------------------------------------


class StrictAliasPart(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    part_id: Annotated[str, Key] = PydanticField(alias="partId")
    unit_cost: Decimal = PydanticField(alias="unitCost")
    checked_at: AwareDatetime = PydanticField(alias="checkedAt")


def _build_per_model_audit_fixture() -> RationalizedResult[Company]:
    src_p1 = ElementSource(
        doc_id="doc_1",
        unit_id="doc_1:u1",
        element_id="p0001_e001",
        page_number=1,
        bbox=BOX,
        scope_path=("BOM",),
    )
    src_p2 = ElementSource(
        doc_id="doc_2",
        unit_id="doc_2:u1",
        element_id="p0002_e004",
        page_number=2,
        bbox=BOX,
        scope_path=("Schematics",),
    )

    c_consensus = Company(
        company_id="AW504ET-089-A4C",
        company_name="Tecumseh Compressor",
        aliases=["AW504ET-089-A4C", "AW504ET - 089 - A4C"],
        departments=[
            Department(
                dept_id="D-1",
                dept_name="Assembly",
                employees=[Employee(emp_id="E-1", name="Ada")],
            )
        ],
    )
    g_vote_consensus = {
        "company_id": "AW504ET - 089 - A4C",
        "company_name": "Tecumseh",
        "hq": None,
        "aliases": ["AW504ET - 089 - A4C"],
        "departments": [
            {
                "dept_id": "D-1",
                "dept_name": "Assembly",
                "codes": [],
                "employees": [{"emp_id": "E-1", "name": "Ada", "skills": ["cad"]}],
            }
        ],
        "line_items": [],
    }
    a_vote_consensus = {
        "company_id": "AW504ET-089-A4C",
        "company_name": "Tecumseh Compressor",
        "hq": {"label": "Plant-2", "weight": 4.5},
        "aliases": ["AW504ET-089-A4C"],
        "departments": [
            {
                "dept_id": "D-1",
                "dept_name": "Assembly",
                "codes": [],
                "employees": [{"emp_id": "E-1", "name": "Ada", "skills": ["cam"]}],
            }
        ],
        "line_items": [],
    }
    it_consensus = GroundedRecord(
        doc_id="doc_1",
        unit_ids=("doc_1:u1",),
        status="consensus",
        key=("AW504ET089A4C",),
        record=c_consensus,
        disputed_fields=(),
        notes=(),
        grounding_reasons=(),
        sources=(src_p1,),
        grounding_quotes=("AW504ET - 089 - A4C", "AW504ET-089-A4C"),
        model_votes={G: (g_vote_consensus,), A: (a_vote_consensus,)},
        quotes_by_model={G: ("AW504ET - 089 - A4C",), A: ("AW504ET-089-A4C",)},
        element_ids_by_model={G: ("p0001_e001",), A: ("p0001_e001",)},
        families={G: "google", A: "anthropic"},
        source_name="compressor_bom.pdf",
    )

    c_single_catch = Company(company_id="SMBJ18A", company_name="TVS Diode")
    g_vote_smbj = c_single_catch.model_dump(mode="json")
    it_single_catch = GroundedRecord(
        doc_id="doc_2",
        unit_ids=("doc_2:u1",),
        status="missing_in_some_models",
        key=("SMBJ18A",),
        record=c_single_catch,
        disputed_fields=(),
        notes=(),
        grounding_reasons=(),
        sources=(src_p2,),
        grounding_quotes=("ZD601: SMBJ18A",),
        model_votes={G: (g_vote_smbj,), A: ()},
        quotes_by_model={G: ("ZD601: SMBJ18A",), A: ()},
        element_ids_by_model={G: ("p0002_e004",), A: ()},
        families={G: "google", A: "anthropic"},
        source_name="pcb_schematic.pdf",
    )

    # Disputed tie with record=None + per-model grounding failure on A only
    it_tie_conflict = GroundedRecord(
        doc_id="doc_2",
        unit_ids=("doc_2:u1",),
        status="field_disagreement",
        key=("TIE-9",),
        record=None,
        disputed_fields=("company_name",),
        notes=(),
        grounding_reasons=(GroundingReason(reason="no_quotes", model_id=A),),
        sources=(src_p2,),
        grounding_quotes=("TIE-9 Alpha",),
        model_votes={
            G: ({"company_id": "TIE-9", "company_name": "Alpha"},),
            A: ({"company_id": "TIE-9", "company_name": "Beta"},),
        },
        quotes_by_model={G: ("TIE-9 Alpha",), A: ()},
        element_ids_by_model={G: ("p0002_e004",), A: ("p0002_e004",)},
        families={G: "google", A: "anthropic"},
        source_name="pcb_schematic.pdf",
    )

    return RationalizedResult(
        items=(it_consensus, it_single_catch, it_tie_conflict),
        records=(c_consensus,),
        needs_review=(it_single_catch, it_tie_conflict),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=COMPANY_SCHEMA,
    )


def test_grounded_union_exports_surface_nested_model_outputs_by_default_and_support_opt_out(tmp_path: Path) -> None:
    result = _build_per_model_audit_fixture()

    # 1. Union DataFrame includes _supporting_models and typed _model_outputs by default
    df_union = result.to_dataframe(include_grounded_union=True)
    assert df_union.height == 2
    assert df_union["_supporting_models"].to_list() == [[G, A], [G]]
    model_outs = df_union["_model_outputs"].to_list()
    assert len(model_outs[0]) == 2
    assert model_outs[0][0]["model_id"] == G
    assert model_outs[0][0]["model_family"] == "google"
    assert model_outs[0][0]["supported"] is True
    assert model_outs[0][0]["is_grounded"] is True
    assert model_outs[0][0]["votes"][0]["company_name"] == "Tecumseh"
    assert model_outs[0][1]["model_id"] == A
    assert model_outs[0][1]["votes"][0]["company_name"] == "Tecumseh Compressor"
    assert model_outs[0][1]["votes"][0]["hq"] == {"label": "Plant-2", "weight": 4.5}

    # Row 1 (SMBJ18A): G supported=True, A supported=False with votes=[]
    assert model_outs[1][0]["model_id"] == G and model_outs[1][0]["supported"] is True
    assert model_outs[1][0]["votes"][0]["company_id"] == "SMBJ18A"
    assert model_outs[1][1]["model_id"] == A and model_outs[1][1]["supported"] is False
    assert model_outs[1][1]["votes"] == []

    # 2. Opt-out via include_model_outputs=False keeps only _status and _doc_id
    df_union_bare = result.to_dataframe(include_grounded_union=True, include_model_outputs=False)
    assert "_model_outputs" not in df_union_bare.columns
    assert "_supporting_models" not in df_union_bare.columns
    assert "_status" in df_union_bare.columns and "_doc_id" in df_union_bare.columns

    # 3. Opt-in via include_model_outputs=True works on clean and include_disputed exports (+ flatten)
    df_clean_mo = result.to_dataframe(include_model_outputs=True)
    assert df_clean_mo.height == 1
    assert df_clean_mo["_supporting_models"].to_list() == [[G, A]]
    assert len(df_clean_mo["_model_outputs"].to_list()[0]) == 2

    df_flat_mo = result.to_dataframe(flatten="departments.employees", include_grounded_union=True)
    assert df_flat_mo["company_id"].to_list() == ["AW504ET-089-A4C", "SMBJ18A"]
    assert df_flat_mo["_supporting_models"].to_list() == [[G, A], [G]]
    assert len(df_flat_mo["_model_outputs"].to_list()[0]) == 2

    # 4. to_dicts, to_arrow_table, and to_jsonl include _model_outputs
    dicts_union = result.to_dicts(include_grounded_union=True)
    assert dicts_union[0]["_supporting_models"] == [G, A]
    assert dicts_union[0]["_model_outputs"][0]["votes"][0]["company_name"] == "Tecumseh"
    assert dicts_union[1]["_supporting_models"] == [G]
    assert dicts_union[1]["_model_outputs"][1]["supported"] is False

    dicts_union_bare = result.to_dicts(include_grounded_union=True, include_model_outputs=False)
    assert "_model_outputs" not in dicts_union_bare[0]

    arrow_union = result.to_arrow_table(include_grounded_union=True)
    assert arrow_union.equals(df_union.to_arrow())

    jsonl_path = tmp_path / "union_prov.jsonl"
    assert result.to_jsonl(jsonl_path, include_disputed=False, include_grounded_union=True) == 2
    j_rows = [json.loads(ln) for ln in jsonl_path.read_text(encoding="utf-8").splitlines()]
    assert j_rows[0]["_model_outputs"][0]["votes"][0]["company_name"] == "Tecumseh"
    assert j_rows[1]["_model_outputs"][1]["supported"] is False


def test_by_model_flat_exports_across_tiers_disagreements_flatten_aliases_and_bounds(tmp_path: Path) -> None:
    result = _build_per_model_audit_fixture()

    # 1. Grounded-union by_model=True: 2 votes on AW504ET + 1 vote on SMBJ18A = 3 rows
    df_bm_gu = result.to_dataframe(include_grounded_union=True, by_model=True)
    assert df_bm_gu.height == 3
    assert df_bm_gu["_model_id"].to_list() == [G, A, G]
    assert df_bm_gu["_model_family"].to_list() == ["google", "anthropic", "google"]
    assert df_bm_gu["_vote_index"].to_list() == [0, 0, 0]
    assert df_bm_gu["company_id"].to_list() == ["AW504ET - 089 - A4C", "AW504ET-089-A4C", "SMBJ18A"]
    assert df_bm_gu["company_name"].to_list() == ["Tecumseh", "Tecumseh Compressor", "TVS Diode"]
    assert df_bm_gu["_status"].to_list() == ["consensus", "consensus", "missing_in_some_models"]
    assert df_bm_gu["_doc_id"].to_list() == ["doc_1", "doc_1", "doc_2"]
    assert df_bm_gu["_is_grounded"].to_list() == [True, True, True]
    assert df_bm_gu["_grounding_quotes"].to_list() == [
        ["AW504ET - 089 - A4C"],
        ["AW504ET-089-A4C"],
        ["ZD601: SMBJ18A"],
    ]
    assert df_bm_gu["_element_ids"].to_list() == [["p0001_e001"], ["p0001_e001"], ["p0002_e004"]]

    # 2. include_disputed=True, by_model=True retains individual model votes even when merged item.record is None!
    df_bm_disp = result.to_dataframe(include_disputed=True, by_model=True)
    assert df_bm_disp.height == 5
    assert df_bm_disp["company_id"].to_list()[3:] == ["TIE-9", "TIE-9"]
    assert df_bm_disp["company_name"].to_list()[3:] == ["Alpha", "Beta"]
    assert df_bm_disp["_model_id"].to_list()[3:] == [G, A]
    assert df_bm_disp["_is_grounded"].to_list()[3:] == [True, False]

    # 3. Flattened by_model=True left-outer explodes each model's vote nested list
    df_bm_flat = result.to_dataframe(flatten="departments.employees", include_grounded_union=True, by_model=True)
    assert df_bm_flat.height == 3
    assert df_bm_flat["departments.employees.emp_id"].to_list() == ["E-1", "E-1", None]
    assert df_bm_flat["departments.employees.skills"].to_list() == [["cad"], ["cam"], None]
    assert df_bm_flat["_model_id"].to_list() == [G, A, G]

    # 4. to_dicts, to_arrow_table, and to_jsonl with by_model=True
    dicts_bm = result.to_dicts(include_disputed=True, by_model=True)
    assert len(dicts_bm) == 5
    assert [(r["_model_id"], r["company_name"], r["_is_grounded"]) for r in dicts_bm[3:]] == [
        (G, "Alpha", True),
        (A, "Beta", False),
    ]
    assert result.to_arrow_table(include_grounded_union=True, by_model=True).equals(df_bm_gu.to_arrow())

    jsonl_bm = tmp_path / "by_model.jsonl"
    assert result.to_jsonl(jsonl_bm, include_disputed=True, by_model=True) == 5
    j_bm_rows = [json.loads(ln) for ln in jsonl_bm.read_text(encoding="utf-8").splitlines()]
    assert [(r["_model_id"], r["company_name"], r["_is_grounded"]) for r in j_bm_rows[3:]] == [
        (G, "Alpha", True),
        (A, "Beta", False),
    ]

    # 5. StrictAliasPart (populate_by_name=False) + Decimal + AwareDatetime UTC conversion + bounds check
    alias_schema = prepare(StrictAliasPart)
    rec_alias = StrictAliasPart.model_validate(
        {
            "partId": "P-100",
            "unitCost": "12.50",
            "checkedAt": "2026-05-10T14:30:00+02:00",
        }
    )
    it_alias = make_grounded(rec_alias, status="consensus", key=("P-100",))
    res_alias = RationalizedResult(
        items=(it_alias,),
        records=(rec_alias,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=alias_schema,
    )
    df_alias_bm = res_alias.to_dataframe(by_model=True)
    assert df_alias_bm.height == 2
    assert df_alias_bm["part_id"].to_list() == ["P-100", "P-100"]
    assert df_alias_bm["unit_cost"].to_list() == [Decimal("12.500000000"), Decimal("12.500000000")]
    assert df_alias_bm["checked_at"].to_list() == [
        datetime(2026, 5, 10, 12, 30, tzinfo=timezone.utc),
        datetime(2026, 5, 10, 12, 30, tzinfo=timezone.utc),
    ]

    # Out-of-bounds Decimal inside model_votes raises RationalizationError naming field path
    bad_vote_item = GroundedRecord(
        doc_id="doc_1",
        unit_ids=("doc_1:u1",),
        status="field_disagreement",
        key=("P-BAD",),
        record=None,
        disputed_fields=("unit_cost",),
        notes=(),
        grounding_reasons=(),
        sources=(),
        grounding_quotes=("P-BAD",),
        model_votes={
            G: ({"part_id": "P-BAD", "unit_cost": "1.0000000009", "checked_at": "2026-05-10T12:00:00Z"},),
            A: (),
        },
        quotes_by_model={G: ("P-BAD",), A: ()},
        element_ids_by_model={G: ("p0001_e001",), A: ()},
        families={G: "google", A: "anthropic"},
    )
    res_bad = RationalizedResult(
        items=(bad_vote_item,),
        records=(),
        needs_review=(bad_vote_item,),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=alias_schema,
    )
    with pytest.raises(RationalizationError, match=r"model\[gemini:gemini-3\]\[0\]\.unit_cost"):
        res_bad.to_dataframe(include_disputed=True, by_model=True)

    # Empty result preserves schema for both by_model=True and include_grounded_union=True
    empty_res: RationalizedResult[Company] = RationalizedResult(
        items=(),
        records=(),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=COMPANY_SCHEMA,
    )
    empty_bm = empty_res.to_dataframe(by_model=True)
    assert empty_bm.height == 0
    assert "_model_id" in empty_bm.schema and "_is_grounded" in empty_bm.schema
    empty_gu = empty_res.to_dataframe(include_grounded_union=True)
    assert empty_gu.height == 0
    assert "_model_outputs" in empty_gu.schema and "_supporting_models" in empty_gu.schema

    # Strict bool validation on by_model and include_model_outputs
    for bad_flag in (1, 0, "true"):
        with pytest.raises(TypeError, match="by_model"):
            result.to_dataframe(by_model=bad_flag)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="by_model"):
            result.to_dicts(by_model=bad_flag)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="by_model"):
            result.to_jsonl(tmp_path / "x.jsonl", by_model=bad_flag)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="include_model_outputs"):
            result.to_dataframe(include_model_outputs=bad_flag)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="include_model_outputs"):
            result.to_dicts(include_model_outputs=bad_flag)  # type: ignore[arg-type]

    # by_model=True with explicit include_model_outputs=True attaches peer context too; to_jsonl bare by_model
    df_bm_peer = result.to_dataframe(include_grounded_union=True, by_model=True, include_model_outputs=True)
    assert "_model_outputs" in df_bm_peer.columns and "_supporting_models" in df_bm_peer.columns
    dicts_bm_peer = result.to_dicts(include_grounded_union=True, by_model=True, include_model_outputs=True)
    assert len(dicts_bm_peer[0]["_model_outputs"]) == 2
    jsonl_bm_bare = tmp_path / "by_model_bare.jsonl"
    assert result.to_jsonl(jsonl_bm_bare, include_provenance=False, include_disputed=False, by_model=True) == 2
    bare_rows = [json.loads(ln) for ln in jsonl_bm_bare.read_text(encoding="utf-8").splitlines()]
    assert all("_sources" not in r and "_model_id" in r for r in bare_rows)

    # AllTypesRecord via by_model=True exercises Enum, date, NaiveDatetime, AwareDatetime, Decimal, int, float, bool
    rec_all = sample_all_types("R-ALL")
    it_all = make_grounded(rec_all, status="consensus", key=("R-ALL",))
    res_all = RationalizedResult(
        items=(it_all,),
        records=(rec_all,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=ALL_TYPES_SCHEMA,
    )
    df_all_bm = res_all.to_dataframe(by_model=True)
    assert df_all_bm.height == 2
    assert df_all_bm["issued_on"].to_list() == [date(2026, 5, 10), date(2026, 5, 10)]
    assert df_all_bm["local_time"].to_list() == [datetime(2026, 5, 10, 9, 15, 0), datetime(2026, 5, 10, 9, 15, 0)]
    assert df_all_bm["status"].to_list() == ["active", "active"]
    assert df_all_bm["priority"].to_list() == [2, 2]
    assert df_all_bm["amount"].to_list() == [Decimal("1234.560000000"), Decimal("1234.560000000")]

    # Out-of-bounds int inside _model_outputs raises RationalizationError with full _model_outputs path
    bad_int_vote = dict(rec_all.model_dump(mode="json"))
    bad_int_vote["count"] = INT64_MAX + 1
    it_bad_int = GroundedRecord(
        doc_id="doc_1",
        unit_ids=("doc_1:u1",),
        status="consensus",
        key=("R-ALL",),
        record=rec_all,
        disputed_fields=(),
        notes=(),
        grounding_reasons=(),
        sources=(),
        grounding_quotes=("R-ALL",),
        model_votes={G: (bad_int_vote,), A: (rec_all.model_dump(mode="json"),)},
        quotes_by_model={G: ("R-ALL",), A: ("R-ALL",)},
        element_ids_by_model={G: ("p0001_e001",), A: ("p0001_e001",)},
        families={G: "google", A: "anthropic"},
    )
    res_bad_int = RationalizedResult(
        items=(it_bad_int,),
        records=(rec_all,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=ALL_TYPES_SCHEMA,
    )
    with pytest.raises(RationalizationError, match=r"_model_outputs\[0\]\.votes\[0\]\.count"):
        res_bad_int.to_dataframe(include_model_outputs=True)

    # Nested container mutation in to_dicts(include_grounded_union=True) must never mutate result.items
    dicts_mut = result.to_dicts(include_grounded_union=True)
    dicts_mut[0]["_model_outputs"][0]["votes"][0]["aliases"].append("INJECTED")
    assert result.items[0].model_votes[G][0]["aliases"] == ["AW504ET - 089 - A4C"]
    assert result.to_dataframe(include_grounded_union=True, by_model=True)["aliases"].to_list()[0] == [
        "AW504ET - 089 - A4C"
    ]

    # Empty result to_jsonl(by_model=True) writes 0 rows
    p_empty_bm = tmp_path / "empty_bm.jsonl"
    assert empty_res.to_jsonl(p_empty_bm, by_model=True) == 0
    assert p_empty_bm.read_text(encoding="utf-8") == ""

    # Custom serializers / ser_json_temporal fail closed with RationalizationError (never AttributeError)
    class EpochTemporalModel(BaseModel):
        model_config = ConfigDict(ser_json_temporal="milliseconds")
        pid: Annotated[str, Key]
        at: AwareDatetime

    rec_epoch = EpochTemporalModel(pid="E-1", at=datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc))
    res_epoch = RationalizedResult(
        items=(make_grounded(rec_epoch, status="consensus", key=("E-1",)),),
        records=(rec_epoch,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=prepare(EpochTemporalModel),
    )
    with pytest.raises(RationalizationError, match="cannot serialize float vote value as Datetime"):
        res_epoch.to_dataframe(include_grounded_union=True)
    with pytest.raises(RationalizationError, match=r"model\[gemini:gemini-3\]\[0\]\.at"):
        res_epoch.to_dataframe(by_model=True)
    assert res_epoch.to_dataframe(include_grounded_union=True, include_model_outputs=False).height == 1

    class CustomIntSerModel(BaseModel):
        pid: Annotated[str, Key]
        n: int

        @field_serializer("n", mode="plain", when_used="json")
        def _ser_n(self, v: int) -> str:
            return f"#{v}"

    rec_cint = CustomIntSerModel(pid="C-1", n=5)
    res_cint = RationalizedResult(
        items=(make_grounded(rec_cint, status="consensus", key=("C-1",)),),
        records=(rec_cint,),
        needs_review=(),
        quarantined=(),
        failed_units=(),
        empty_units=(),
        skipped_units=(),
        telemetry=make_telemetry(),
        schema=prepare(CustomIntSerModel),
    )
    with pytest.raises(RationalizationError, match="cannot serialize str vote value as Int64"):
        res_cint.to_dataframe(include_model_outputs=True)
