"""Tests for ``datagrunt.rationalization.result`` (plan §3, §4.10)."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Any, Literal

import polars as pl
import pyarrow as pa
import pytest
from pdf_tests.documents import BOX
from pydantic import AwareDatetime, BaseModel, NaiveDatetime

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
