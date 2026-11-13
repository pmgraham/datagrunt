"""Tests for ``datagrunt.rationalization.folding``: cross-unit entity folding and element provenance (plan §3, §4.9).

Documents hold real elements (built with ``pdf_tests.documents``), units and envelopes come from the real builders,
and grounded items come from ``build_consensus`` + ``ground`` over hand-built responses.
"""

import dataclasses
import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Any

import pytest
from pdf_tests.documents import BOX, VISION_QUALITY, ids, metadata, page
from pydantic import AwareDatetime, BaseModel, ConfigDict, field_validator, model_validator
from pydantic.alias_generators import to_camel

import datagrunt.rationalization.folding as folding_module
from datagrunt.pdf.elements import (
    BBox,
    Heading,
    KeyValue,
    PageImage,
    Paragraph,
    PDFDocument,
    Section,
    Table,
)
from datagrunt.pdf.render import RenderRefused
from datagrunt.rationalization import Informational, Key, rationalize
from datagrunt.rationalization.consensus import build_consensus
from datagrunt.rationalization.envelope import Envelope, build_envelopes, build_envelopes_isolated, render_element
from datagrunt.rationalization.folding import (
    RECORD_STATUSES,
    ElementSource,
    FoldingResult,
    GroundedRecord,
    RecordStatus,
    fold_documents,
)
from datagrunt.rationalization.grounding import GroundingReason, GroundingResult, ground
from datagrunt.rationalization.providers.callable import CallableProvider
from datagrunt.rationalization.runner import FailedUnit, RunOutcome
from datagrunt.rationalization.schema import PreparedSchema, prepare
from datagrunt.rationalization.units import Unit, build_units
from datagrunt.rationalization.wire import ProviderResponse

BIG = 1_000_000
GOOGLE = ("gemini:gemini-3", "google", "gemini")
ANTHROPIC = ("anthropic:claude", "anthropic", "anthropic")
PAIR = (GOOGLE, ANTHROPIC)
G, A = GOOGLE[0], ANTHROPIC[0]


# ---------------------------------------------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------------------------------------------


class Component(BaseModel):
    part_no: Annotated[str, Key]
    qty: int | None = None
    material: str | None = None
    finish: str = "plain"


class SubSpec(BaseModel):
    pressure_bar: int | None = None
    voltage_v: int | None = None
    grade: str = "standard"


class Assembly(BaseModel):
    assembly_id: Annotated[str, Key]
    title: str | None = None
    notes: str | None = None
    revision: str = "A"
    tags: list[str] = []
    spec: SubSpec | None = None
    components: list[Component] = []


class CheckedAssembly(BaseModel):
    assembly_id: Annotated[str, Key]
    min_temp: int | None = None
    max_temp: int | None = None

    @model_validator(mode="after")
    def _check_range(self) -> "CheckedAssembly":
        if self.min_temp is not None and self.max_temp is not None and self.min_temp > self.max_temp:
            raise ValueError("min_temp exceeds max_temp")
        return self


class KeylessNote(BaseModel):
    headline: str
    body: str | None = None


ASSEMBLY = prepare(Assembly)
CHECKED_ASSEMBLY = prepare(CheckedAssembly)
KEYLESS_NOTE = prepare(KeylessNote)


# ---------------------------------------------------------------------------------------------------------------
# Document, unit, consensus and grounding helpers
# ---------------------------------------------------------------------------------------------------------------


def para(page_number: int, index: int, text: str, *, scope_path: tuple[str, ...] = (), bbox: BBox = BOX) -> Paragraph:
    return Paragraph(**ids(page_number, index, scope_path, bbox), text=text)


def pair(page_number: int, index: int, key: str, value: str, *, bbox: BBox = BOX) -> KeyValue:
    return KeyValue(**ids(page_number, index, (), bbox), key=key, value=value)


def grid(
    page_number: int,
    index: int,
    headers: tuple[str, ...],
    *rows: tuple[str, ...],
    scope_path: tuple[str, ...] = (),
    bbox: BBox = BOX,
) -> Table:
    return Table(**ids(page_number, index, scope_path, bbox), headers=headers, rows=rows, extraction_method="lattice")


def titled(
    page_number: int,
    index: int,
    text: str,
    *,
    scope_path: tuple[str, ...] | None = None,
    hidden_reasons: tuple[str, ...] = (),
) -> Heading:
    path = (text,) if scope_path is None else scope_path
    return Heading(**ids(page_number, index, path), level=1, text=text, hidden_reasons=hidden_reasons)  # type: ignore[arg-type]


def make_doc(
    *pages: Any,
    source_name: str = "report.pdf",
    sections: tuple[Section, ...] = (),
    header_context: tuple[KeyValue, ...] = (),
) -> PDFDocument:
    return PDFDocument(
        metadata=metadata(len(pages), source_name=source_name),
        pages=pages,
        sections=sections,
        header_context=header_context,
    )


@dataclasses.dataclass(frozen=True)
class PreparedDoc:
    document: PDFDocument
    units: dict[str, Unit]
    envelopes: dict[str, Envelope]

    @property
    def doc_id(self) -> str:
        return self.document.metadata.doc_id

    @property
    def unit_ids(self) -> tuple[str, ...]:
        return tuple(self.units)


def prepare_doc(doc: PDFDocument, *, schema: PreparedSchema = ASSEMBLY, limit: int = BIG) -> PreparedDoc:
    plan = build_units(doc, max_unit_chars=limit, render=render_element)
    envelopes = build_envelopes(plan.units, doc, prompt="Extract.", context=None, schema_json=schema.wrapper_json)
    return PreparedDoc(
        document=doc,
        units={unit.unit_id: unit for unit in plan.units},
        envelopes={envelope.unit_id: envelope for envelope in envelopes},
    )


def entry(record: dict[str, Any], quotes: tuple[str, ...], element_ids: tuple[str, ...]) -> dict[str, Any]:
    return {"record": record, "grounding_quotes": list(quotes), "source_element_ids": list(element_ids)}


def response(*entries: dict[str, Any], finish: str = "complete") -> ProviderResponse:
    return ProviderResponse(
        content={"records": list(entries)},
        raw_text="",
        finish=finish,  # type: ignore[arg-type]
        input_tokens=None,
        output_tokens=None,
    )


def run_and_fold(
    responses: dict[tuple[str, str], ProviderResponse],
    *runs: PreparedDoc,
    models: tuple[tuple[str, str, str], ...] = PAIR,
    schema: PreparedSchema = ASSEMBLY,
    failed: tuple[FailedUnit, ...] = (),
) -> FoldingResult[Any]:
    units: dict[str, Unit] = {}
    envelopes: dict[str, Envelope] = {}
    documents: dict[str, PDFDocument] = {}
    for run in runs:
        units.update(run.units)
        envelopes.update(run.envelopes)
        documents[run.doc_id] = run.document
    outcome = RunOutcome(
        responses=responses,
        failed_units=failed,
        cache_hits=0,
        cache_misses=len(responses),
        cache_corrupt=0,
        cache_write_failures=0,
        usage={},
        models=models,
    )
    families = len({model[1] for model in models})
    consensus = build_consensus(outcome, schema, tuple(units), min_families=families)
    grounding = ground(consensus, outcome=outcome, units=units, envelopes=envelopes, documents=documents, schema=schema)
    return fold_documents(grounding, units=units, envelopes=envelopes, documents=documents, schema=schema)


# Three-page BOM document for cross-unit folding tests.
BBOX_H = BBox(x0=10.0, y0=10.0, x1=200.0, y1=25.0)
BBOX_P1 = BBox(x0=20.0, y0=40.0, x1=400.0, y1=120.0)
BBOX_P2 = BBox(x0=25.0, y0=45.0, x1=410.0, y1=130.0)
BBOX_P3 = BBox(x0=30.0, y0=50.0, x1=420.0, y1=140.0)

HEADER_KV = pair(1, 0, "Program", "A-100", bbox=BBOX_H)
P1_TEXT = para(
    1,
    1,
    "Assembly A-100 Main Pump rev A. Part PN-1 qty 2 steel. Tags: alpha, beta. Pressure 10 bar. Min 10.",
    bbox=BBOX_P1,
)
P2_TEXT = para(
    2,
    0,
    "Assembly A-100 Spare rev B. Part PN-1 qty 2. Part PN-2 qty 4 brass. Tags: gamma. Voltage 24 V. Max 5.",
    bbox=BBOX_P2,
)
P3_TEXT = para(
    3,
    0,
    "Assembly A-100 continued. Part PN-3 qty 1 copper. Part PN-2 brass anodized.",
    bbox=BBOX_P3,
)

BOM_DOC = make_doc(
    page(1, (HEADER_KV, P1_TEXT)),
    page(2, (P2_TEXT,)),
    page(3, (P3_TEXT,)),
    header_context=(HEADER_KV,),
)
BOM_RUN = prepare_doc(BOM_DOC)
U1, U2, U3 = BOM_RUN.unit_ids
DOC_ID = BOM_RUN.doc_id
E_HEADER = HEADER_KV.element_id
E_P1 = P1_TEXT.element_id
E_P2 = P2_TEXT.element_id
E_P3 = P3_TEXT.element_id


# ---------------------------------------------------------------------------------------------------------------
# 1. Constants & Status Order
# ---------------------------------------------------------------------------------------------------------------


class TestConstantsAndExports:
    def test_record_statuses_order(self) -> None:
        assert RECORD_STATUSES == (
            "consensus",
            "single_model",
            "missing_in_some_models",
            "field_disagreement",
            "unmatched",
            "ungrounded_value",
            "cross_page_conflict",
        )

    def test_public_exports_in_all(self) -> None:
        expected = {
            "RECORD_STATUSES",
            "RecordStatus",
            "ElementSource",
            "GroundedRecord",
            "FoldingResult",
            "fold_documents",
        }
        assert expected.issubset(set(folding_module.__all__))
        assert RecordStatus is not None


# ---------------------------------------------------------------------------------------------------------------
# 2. ElementSource
# ---------------------------------------------------------------------------------------------------------------


class TestElementSource:
    def test_frozen_slotted_and_scope_path_omitted_from_repr(self) -> None:
        source = ElementSource(
            doc_id=DOC_ID,
            unit_id=U1,
            element_id="p0001_e001",
            page_number=1,
            bbox=BBOX_P1,
            scope_path=("SECRET_HEADING",),
        )
        assert (
            source.doc_id,
            source.unit_id,
            source.element_id,
            source.page_number,
            source.bbox,
            source.scope_path,
        ) == (DOC_ID, U1, "p0001_e001", 1, BBOX_P1, ("SECRET_HEADING",))
        assert "SECRET_HEADING" not in repr(source)
        assert not hasattr(source, "__dict__")
        with pytest.raises(dataclasses.FrozenInstanceError):
            source.page_number = 2  # type: ignore[misc]

    def test_default_scope_path_is_empty_tuple(self) -> None:
        source = ElementSource(doc_id=DOC_ID, unit_id=U1, element_id="p0001_e001", page_number=1, bbox=BBOX_P1)
        assert source.scope_path == ()

    @pytest.mark.parametrize(
        ("overrides", "error"),
        [
            ({"doc_id": 123}, TypeError),
            ({"doc_id": ""}, ValueError),
            ({"doc_id": "   "}, ValueError),
            ({"unit_id": None}, TypeError),
            ({"unit_id": ""}, ValueError),
            ({"unit_id": "\t"}, ValueError),
            ({"element_id": 1}, TypeError),
            ({"element_id": "bad"}, ValueError),
            ({"element_id": "p0000_e001"}, ValueError),
            ({"element_id": "p00001_e001"}, ValueError),
            ({"element_id": "p0001_e0001"}, ValueError),
            ({"page_number": True}, TypeError),
            ({"page_number": 1.0}, TypeError),
            ({"page_number": "1"}, TypeError),
            ({"page_number": 0}, ValueError),
            ({"page_number": 2}, ValueError),  # mismatches p0001_e001
            ({"bbox": (0.0, 0.0, 10.0, 10.0)}, TypeError),
            ({"scope_path": ["Section"]}, TypeError),
            ({"scope_path": (123,)}, TypeError),
            ({"scope_path": ("",)}, ValueError),
            ({"scope_path": ("  ",)}, ValueError),
        ],
    )
    def test_validation_rejects_bad_inputs(self, overrides: dict[str, Any], error: type[Exception]) -> None:
        valid: dict[str, Any] = {
            "doc_id": DOC_ID,
            "unit_id": U1,
            "element_id": "p0001_e001",
            "page_number": 1,
            "bbox": BBOX_P1,
            "scope_path": ("Intro",),
        }
        valid.update(overrides)
        with pytest.raises(error):
            ElementSource(**valid)


# ---------------------------------------------------------------------------------------------------------------
# 3. GroundedRecord & 4. FoldingResult
# ---------------------------------------------------------------------------------------------------------------


def make_grounded_record(**overrides: Any) -> GroundedRecord[Assembly]:
    source = ElementSource(
        doc_id=DOC_ID,
        unit_id=U1,
        element_id=E_P1,
        page_number=1,
        bbox=BBOX_P1,
        scope_path=("SECRET_SCOPE",),
    )
    record = Assembly(assembly_id="A-100", title="SECRET_TITLE")
    defaults: dict[str, Any] = {
        "doc_id": DOC_ID,
        "unit_ids": (U1,),
        "status": "consensus",
        "key": ("A-100",),
        "record": record,
        "disputed_fields": (),
        "notes": (),
        "grounding_reasons": (),
        "sources": (source,),
        "grounding_quotes": ("Assembly A-100 Main Pump",),
        "model_votes": {
            G: (record.model_dump(mode="json"),),
            A: (record.model_dump(mode="json"),),
        },
        "quotes_by_model": {
            G: ("Assembly A-100 Main Pump",),
            A: ("Assembly A-100 Main Pump",),
        },
        "element_ids_by_model": {
            G: (E_P1,),
            A: (E_P1,),
        },
        "families": {G: "google", A: "anthropic"},
    }
    defaults.update(overrides)
    return GroundedRecord(**defaults)


class TestGroundedRecordAndFoldingResult:
    def test_frozen_slotted_and_read_only_mappings(self) -> None:
        item = make_grounded_record()
        assert isinstance(item.model_votes, MappingProxyType)
        assert isinstance(item.quotes_by_model, MappingProxyType)
        assert isinstance(item.element_ids_by_model, MappingProxyType)
        assert isinstance(item.families, MappingProxyType)
        assert not hasattr(item, "__dict__")
        with pytest.raises(dataclasses.FrozenInstanceError):
            item.status = "unmatched"  # type: ignore[misc]

    def test_repr_never_leaks_document_or_model_text(self) -> None:
        reason = GroundingReason("quote_not_found", G, index=0, text="SECRET_REASON_TEXT")
        item = make_grounded_record(
            status="cross_page_conflict",
            key=("SECRET_KEY",),
            record=None,
            disputed_fields=("SECRET_FIELD",),
            notes=("SECRET_NOTE",),
            grounding_reasons=(reason,),
            grounding_quotes=("SECRET_QUOTE",),
            quotes_by_model={G: ("SECRET_QUOTE",), A: ("SECRET_QUOTE",)},
            element_ids_by_model={G: ("p0001_e999",), A: ("p0001_e999",)},
        )
        rendered = repr(item)
        for secret in (
            "SECRET_KEY",
            "SECRET_TITLE",
            "SECRET_FIELD",
            "SECRET_NOTE",
            "SECRET_REASON_TEXT",
            "SECRET_QUOTE",
            "SECRET_SCOPE",
            "p0001_e999",
        ):
            assert secret not in rendered
        assert DOC_ID in rendered
        assert "cross_page_conflict" in rendered

    @pytest.mark.parametrize(
        ("overrides", "error"),
        [
            ({"doc_id": ""}, ValueError),
            ({"doc_id": 123}, TypeError),
            ({"unit_ids": ()}, ValueError),
            ({"unit_ids": [U1]}, TypeError),
            ({"unit_ids": ("",)}, ValueError),
            ({"unit_ids": (U1, U1)}, ValueError),
            ({"status": "not_a_status"}, ValueError),
            ({"status": 1}, TypeError),
            ({"key": ["A-100"]}, TypeError),
            ({"record": {"assembly_id": "A-100"}}, TypeError),
            ({"disputed_fields": ["title"]}, TypeError),
            ({"disputed_fields": ("",), "status": "field_disagreement", "record": None}, ValueError),
            ({"notes": ["note"], "status": "field_disagreement", "record": None}, TypeError),
            ({"grounding_reasons": ["bad"]}, TypeError),
            ({"sources": ["bad"]}, TypeError),
            ({"grounding_quotes": ["bad"]}, TypeError),
            # Clean status invariants:
            ({"status": "consensus", "record": None}, ValueError),
            ({"status": "single_model", "record": None}, ValueError),
            ({"status": "consensus", "disputed_fields": ("title",)}, ValueError),
            ({"status": "consensus", "notes": ("a note",)}, ValueError),
            (
                {
                    "status": "consensus",
                    "grounding_reasons": (GroundingReason("no_quotes", G),),
                },
                ValueError,
            ),
            (
                {
                    "status": "single_model",
                    "grounding_reasons": (GroundingReason("no_quotes", G),),
                    "model_votes": {G: ({"assembly_id": "A-100"},)},
                    "quotes_by_model": {G: ()},
                    "element_ids_by_model": {G: (E_P1,)},
                    "families": {G: "google"},
                },
                ValueError,
            ),
        ],
    )
    def test_grounded_record_rejects_invalid_fields(self, overrides: dict[str, Any], error: type[Exception]) -> None:
        with pytest.raises(error):
            make_grounded_record(**overrides)

    def test_folding_result_validates_items_tuple(self) -> None:
        item = make_grounded_record()
        result = FoldingResult(items=(item,))
        assert result.items == (item,)
        with pytest.raises(TypeError):
            FoldingResult(items=[item])  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            FoldingResult(items=("not_a_record",))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# 5. fold_documents: Single-unit pass-through & Provenance
# ---------------------------------------------------------------------------------------------------------------


class TestSingleUnitAndProvenance:
    def test_single_unit_pass_through_with_header_and_element_sources_in_citation_order(self) -> None:
        rec = {"assembly_id": "A-100", "title": "Main Pump"}
        # Cite header first on P2, then P2's own element: sources must preserve citation order.
        responses = {
            (U2, G): response(entry(rec, ("A-100", "Assembly A-100 Spare"), (E_HEADER, E_P2))),
            (U2, A): response(entry(rec, ("Assembly A-100 Spare", "A-100"), (E_P2, E_HEADER))),
        }
        result = run_and_fold(responses, BOM_RUN)
        assert len(result.items) == 1
        item = result.items[0]
        assert item.doc_id == DOC_ID
        assert item.unit_ids == (U2,)
        assert item.status == "consensus"
        assert item.key == ("A-100",)
        assert item.record == Assembly(assembly_id="A-100", title="Main Pump")
        assert item.disputed_fields == ()
        assert item.notes == ()
        assert item.grounding_reasons == ()
        assert item.grounding_quotes == ("A-100", "Assembly A-100 Spare")
        assert [(s.unit_id, s.element_id, s.page_number, s.bbox) for s in item.sources] == [
            (U2, E_HEADER, 1, BBOX_H),
            (U2, E_P2, 2, BBOX_P2),
        ]

    def test_scope_path_comes_from_unit_never_element_with_hidden_heading(self) -> None:
        """ElementSource.scope_path must always be Unit.scope_path, never DocumentElement.scope_path."""
        hidden_h = titled(
            1, 0, "HIDDEN_INJECTION_HEADING", scope_path=("HIDDEN_INJECTION_HEADING",), hidden_reasons=("tiny_font",)
        )
        body = para(1, 1, "Assembly A-100 Main Pump.", scope_path=("HIDDEN_INJECTION_HEADING",), bbox=BBOX_P1)
        sec = Section(
            heading=hidden_h,
            scope_path=("HIDDEN_INJECTION_HEADING",),
            page_span=(1, 1),
            elements=(body,),
        )
        doc = make_doc(page(1, (hidden_h, body)), sections=(sec,))
        run = prepare_doc(doc)
        (u_id,) = run.unit_ids
        assert run.units[u_id].scope_path == ()
        assert body.scope_path == ("HIDDEN_INJECTION_HEADING",)

        rec = {"assembly_id": "A-100", "title": "Main Pump"}
        ans = response(entry(rec, ("Assembly A-100 Main Pump.",), (body.element_id,)))
        result = run_and_fold({(u_id, G): ans, (u_id, A): ans}, run)
        (item,) = result.items
        assert len(item.sources) == 1
        assert item.sources[0].scope_path == ()
        assert "HIDDEN_INJECTION_HEADING" not in repr(item.sources[0])

    def test_split_table_part_uses_whole_table_bbox_from_document(self) -> None:
        whole_bbox = BBox(x0=15.0, y0=30.0, x1=550.0, y1=700.0)
        part_bbox = BBox(x0=15.0, y0=30.0, x1=550.0, y1=200.0)
        big_table = grid(
            1,
            0,
            ("Assembly", "Part", "Notes"),
            ("A-100", "PN-1", "Row one " * 20),
            ("A-100", "PN-2", "Row two " * 20),
            bbox=whole_bbox,
        )
        doc = make_doc(page(1, (big_table,)))
        run = prepare_doc(doc, limit=180)
        assert len(run.unit_ids) == 2
        u_first = run.unit_ids[0]
        assert run.units[u_first].part_of == ((1, 2),)
        # Even if the unit's part copy had a different bbox, documents[doc_id] has the whole table's BBox.
        tampered_part = dataclasses.replace(run.units[u_first].elements[0], bbox=part_bbox)
        tampered_unit = dataclasses.replace(run.units[u_first], elements=(tampered_part,))
        run.units[u_first] = tampered_unit

        rec = {"assembly_id": "A-100", "components": [{"part_no": "PN-1"}]}
        ans = response(entry(rec, ("A-100", "PN-1"), (big_table.element_id,)))
        result = run_and_fold({(u_first, G): ans, (u_first, A): ans}, run)
        (item,) = result.items
        assert len(item.sources) == 1
        assert item.sources[0].bbox == whole_bbox

    def test_unknown_element_id_skipped_in_sources_without_raising(self) -> None:
        rec = {"assembly_id": "A-100", "title": "Main Pump"}
        ans = response(entry(rec, ("Assembly A-100 Main Pump",), ("p0099_e999", E_P1)))
        result = run_and_fold({(U1, G): ans, (U1, A): ans}, BOM_RUN)
        (item,) = result.items
        assert item.status == "ungrounded_value"
        assert any(r.reason == "unknown_element" for r in item.grounding_reasons)
        assert [(s.unit_id, s.element_id) for s in item.sources] == [(U1, E_P1)]


# ---------------------------------------------------------------------------------------------------------------
# 6. Cross-Unit BOM Folding & Complementing Fields
# ---------------------------------------------------------------------------------------------------------------


class TestCrossUnitBomFolding:
    def test_three_unit_bom_folds_children_in_first_seen_order_and_merges_child_fields(self) -> None:
        """3 units sharing root key 'A-100' fold into one GroundedRecord with unioned & merged children."""
        r1 = {
            "assembly_id": "A-100",
            "title": "Main Pump",
            "components": [{"part_no": "PN-1", "qty": 2, "material": "steel"}],
        }
        r2 = {
            "assembly_id": "A-100",
            "notes": "Spare",
            "components": [
                {"part_no": "PN-1", "qty": 2},
                {"part_no": "PN-2", "qty": 4, "material": "brass"},
            ],
        }
        r3 = {
            "assembly_id": "A-100",
            "components": [
                {"part_no": "PN-3", "qty": 1, "material": "copper"},
                {"part_no": "PN-2", "material": "brass", "finish": "anodized"},
            ],
        }
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump", "Part PN-1 qty 2 steel"), (E_P1, E_HEADER))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare", "Part PN-2 qty 4 brass"), (E_P2, E_HEADER))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U3, G): response(entry(r3, ("Assembly A-100 continued", "Part PN-3 qty 1 copper"), (E_P3,))),
            (U3, A): response(entry(r3, ("Assembly A-100 continued",), (E_P3,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        assert len(result.items) == 1
        item = result.items[0]
        assert item.status == "consensus"
        assert item.unit_ids == (U1, U2, U3)
        assert item.key == ("A-100",)
        assert item.record == Assembly(
            assembly_id="A-100",
            title="Main Pump",
            notes="Spare",
            revision="A",
            tags=[],
            spec=None,
            components=[
                Component(part_no="PN-1", qty=2, material="steel", finish="plain"),
                Component(part_no="PN-2", qty=4, material="brass", finish="anodized"),
                Component(part_no="PN-3", qty=1, material="copper", finish="plain"),
            ],
        )
        # Sources deduplicated by (unit_id, element_id) in citation order across units.
        assert [(s.unit_id, s.element_id) for s in item.sources] == [
            (U1, E_P1),
            (U1, E_HEADER),
            (U2, E_P2),
            (U2, E_HEADER),
            (U3, E_P3),
        ]
        assert item.grounding_quotes == (
            "Assembly A-100 Main Pump",
            "Part PN-1 qty 2 steel",
            "Assembly A-100 Spare",
            "Part PN-2 qty 4 brass",
            "Assembly A-100 continued",
            "Part PN-3 qty 1 copper",
        )
        assert len(item.model_votes[G]) == 3
        assert len(item.model_votes[A]) == 3

    def test_complements_optional_and_defaulted_parent_and_nested_model_fields(self) -> None:
        """Unit 1 sets spec.pressure_bar=10; Unit 2 sets revision='B' and spec.voltage_v=24."""
        r1 = {
            "assembly_id": "A-100",
            "title": "Main Pump",
            "notes": None,
            "spec": {"pressure_bar": 10},
        }
        r2 = {
            "assembly_id": "A-100",
            "title": None,
            "notes": "Spare",
            "revision": "B",
            "tags": ["gamma"],
            "spec": {"voltage_v": 24, "grade": "marine"},
        }
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare rev B",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare rev B",), (E_P2,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "consensus"
        assert item.record is not None
        assert item.record.title == "Main Pump"
        assert item.record.notes == "Spare"
        assert item.record.revision == "B"
        assert "revision" in item.record.model_fields_set
        assert item.record.tags == ["gamma"]
        assert item.record.spec == SubSpec(pressure_bar=10, voltage_v=24, grade="marine")
        assert "grade" in item.record.spec.model_fields_set


# ---------------------------------------------------------------------------------------------------------------
# 7. Cross-Page Conflicts & Status Ranking
# ---------------------------------------------------------------------------------------------------------------


class TestCrossPageConflictsAndStatusRanking:
    def test_conflicting_parent_scalar_produces_cross_page_conflict(self) -> None:
        r1 = {"assembly_id": "A-100", "title": "Main Pump", "revision": "A"}
        r2 = {"assembly_id": "A-100", "title": "Spare", "revision": "B"}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump rev A",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump rev A",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare rev B",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare rev B",), (E_P2,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "cross_page_conflict"
        assert item.record is None
        assert item.disputed_fields == ("title", "revision")

    def test_conflicting_child_scalar_produces_cross_page_conflict(self) -> None:
        r1 = {"assembly_id": "A-100", "components": [{"part_no": "PN-1", "qty": 2}]}
        r2 = {"assembly_id": "A-100", "components": [{"part_no": "PN-1", "qty": 99}]}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "cross_page_conflict"
        assert item.record is None
        assert item.disputed_fields == ("components['PN-1'].qty",)

    def test_conflicting_unkeyed_list_produces_cross_page_conflict(self) -> None:
        r1 = {"assembly_id": "A-100", "tags": ["alpha", "beta"]}
        r2 = {"assembly_id": "A-100", "tags": ["gamma"]}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "cross_page_conflict"
        assert item.record is None
        assert item.disputed_fields == ("tags",)

    def test_cross_field_validator_failure_on_folded_record_produces_cross_page_conflict(self) -> None:
        doc = prepare_doc(BOM_DOC, schema=CHECKED_ASSEMBLY)
        r1 = {"assembly_id": "A-100", "min_temp": 10}
        r2 = {"assembly_id": "A-100", "max_temp": 5}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        result = run_and_fold(responses, doc, schema=CHECKED_ASSEMBLY)
        (item,) = result.items
        assert item.status == "cross_page_conflict"
        assert item.record is None
        assert any("the folded record failed validation" in note for note in item.notes)

    def test_worst_status_ranking_preserves_missing_in_some_models(self) -> None:
        r1 = {"assembly_id": "A-100", "title": "Main Pump"}
        r2 = {"assembly_id": "A-100", "notes": "Spare"}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(),  # missing in A on U2
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "missing_in_some_models"
        assert item.record == Assembly(assembly_id="A-100", title="Main Pump", notes="Spare")

    def test_worst_status_ranking_never_hides_ungrounded_value_under_clean_unit(self) -> None:
        r1 = {"assembly_id": "A-100", "title": "Main Pump"}
        r2 = {"assembly_id": "A-100", "notes": "Spare"}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            # U2 has a fabricated quote -> ungrounded_value
            (U2, G): response(entry(r2, ("FABRICATED QUOTE A-100",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "ungrounded_value"
        assert len(item.grounding_reasons) == 1
        assert item.grounding_reasons[0].reason == "quote_not_found"
        assert item.record == Assembly(assembly_id="A-100", title="Main Pump", notes="Spare")

    def test_ungrounded_unit_plus_cross_page_conflict_yields_cross_page_conflict_and_keeps_reasons(self) -> None:
        r1 = {"assembly_id": "A-100", "title": "Main Pump"}
        r2 = {"assembly_id": "A-100", "title": "Spare"}
        responses = {
            (U1, G): response(entry(r1, ("FABRICATED QUOTE A-100",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        result = run_and_fold(responses, BOM_RUN)
        (item,) = result.items
        assert item.status == "cross_page_conflict"
        assert item.record is None
        assert item.disputed_fields == ("title",)
        assert len(item.grounding_reasons) == 1
        assert item.grounding_reasons[0].reason == "quote_not_found"


# ---------------------------------------------------------------------------------------------------------------
# 8. Keyless Roots & Multi-Document Isolation
# ---------------------------------------------------------------------------------------------------------------


class TestKeylessAndMultiDocumentIsolation:
    def test_keyless_root_items_never_fold_across_units(self) -> None:
        run = prepare_doc(BOM_DOC, schema=KEYLESS_NOTE)
        n1 = {"headline": "Assembly A-100 Main Pump"}
        n2 = {"headline": "Assembly A-100 Spare"}
        responses = {
            (U1, G): response(entry(n1, ("Assembly A-100 Main Pump rev A.",), (E_P1,))),
            (U1, A): response(entry(n1, ("Assembly A-100 Main Pump rev A.",), (E_P1,))),
            (U2, G): response(entry(n2, ("Assembly A-100 Spare rev B.",), (E_P2,))),
            (U2, A): response(entry(n2, ("Assembly A-100 Spare rev B.",), (E_P2,))),
        }
        result = run_and_fold(responses, run, schema=KEYLESS_NOTE)
        assert len(result.items) == 2
        assert result.items[0].unit_ids == (U1,)
        assert result.items[1].unit_ids == (U2,)
        assert result.items[0].key == ()
        assert result.items[1].key == ()

    def test_records_with_same_key_in_different_documents_never_fold_together(self) -> None:
        doc_a = make_doc(page(1, (para(1, 0, "Assembly A-100 Main Pump."),)), source_name="doc_a.pdf")
        doc_b = make_doc(page(1, (para(1, 0, "Assembly A-100 Spare."),)), source_name="doc_b.pdf")
        run_a = prepare_doc(doc_a)
        run_b = prepare_doc(doc_b)
        (ua,) = run_a.unit_ids
        (ub,) = run_b.unit_ids

        ra = {"assembly_id": "A-100", "title": "Main Pump"}
        rb = {"assembly_id": "A-100", "title": "Spare"}
        responses = {
            (ua, G): response(entry(ra, ("Assembly A-100 Main Pump.",), ("p0001_e000",))),
            (ua, A): response(entry(ra, ("Assembly A-100 Main Pump.",), ("p0001_e000",))),
            (ub, G): response(entry(rb, ("Assembly A-100 Spare.",), ("p0001_e000",))),
            (ub, A): response(entry(rb, ("Assembly A-100 Spare.",), ("p0001_e000",))),
        }
        result = run_and_fold(responses, run_a, run_b)
        assert len(result.items) == 2
        assert result.items[0].doc_id == run_a.doc_id
        assert result.items[0].record == Assembly(assembly_id="A-100", title="Main Pump")
        assert result.items[1].doc_id == run_b.doc_id
        assert result.items[1].record == Assembly(assembly_id="A-100", title="Spare")


# ---------------------------------------------------------------------------------------------------------------
# 9. Argument Validation on fold_documents
# ---------------------------------------------------------------------------------------------------------------


class TestFoldDocumentsArgumentValidation:
    def test_rejects_bad_arguments(self) -> None:
        empty_grounding = GroundingResult(items=(), empty_units=())
        units = BOM_RUN.units
        envs = BOM_RUN.envelopes
        docs = {DOC_ID: BOM_DOC}
        with pytest.raises(TypeError):
            fold_documents("not_grounding", units=units, envelopes=envs, documents=docs, schema=ASSEMBLY)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            fold_documents(empty_grounding, units="bad", envelopes=envs, documents=docs, schema=ASSEMBLY)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            fold_documents(empty_grounding, units=units, envelopes="bad", documents=docs, schema=ASSEMBLY)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            fold_documents(empty_grounding, units=units, envelopes=envs, documents="bad", schema=ASSEMBLY)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            fold_documents(empty_grounding, units=units, envelopes=envs, documents=docs, schema="bad")  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            fold_documents(empty_grounding, units=units, envelopes={}, documents=docs, schema=ASSEMBLY)
        with pytest.raises(ValueError):
            fold_documents(empty_grounding, units=units, envelopes=envs, documents={}, schema=ASSEMBLY)


# ---------------------------------------------------------------------------------------------------------------
# 10. Section Coalescing Provenance (coalesce_sections=True)
# ---------------------------------------------------------------------------------------------------------------


class TestCoalesceSectionsFolding:
    def test_coalesced_unit_resolves_each_element_source_to_its_own_sanitized_section_scope_path(self) -> None:
        h1 = titled(1, 0, "1. Electrical")
        p1 = para(1, 1, "Assembly A-100 Main Pump.", scope_path=("1. Electrical",), bbox=BBOX_P1)
        h2 = titled(1, 2, "2. Mechanical")
        p2 = para(1, 3, "Part PN-1 qty 2 steel.", scope_path=("2. Mechanical",), bbox=BBOX_P2)
        h3_hidden = titled(
            1,
            4,
            "SECRET_HIDDEN_HEADING",
            scope_path=("2. Mechanical", "SECRET_HIDDEN_HEADING"),
            hidden_reasons=("tiny_font",),
        )
        p3 = para(
            1,
            5,
            "Pressure 10 bar.",
            scope_path=("2. Mechanical", "SECRET_HIDDEN_HEADING"),
            bbox=BBOX_P3,
        )
        doc = make_doc(
            page(1, (h1, p1, h2, p2, h3_hidden, p3)),
            sections=(
                Section(heading=h1, scope_path=("1. Electrical",), page_span=(1, 1), elements=(h1, p1)),
                Section(heading=h2, scope_path=("2. Mechanical",), page_span=(1, 1), elements=(h2, p2)),
                Section(
                    heading=h3_hidden,
                    scope_path=("2. Mechanical", "SECRET_HIDDEN_HEADING"),
                    page_span=(1, 1),
                    elements=(p3,),
                ),
            ),
        )
        visible_els = (h1, p1, h2, p2, p3)
        blocks = tuple(render_element(el) for el in visible_els)
        u_id = f"{doc.metadata.doc_id}:{visible_els[0].element_id}-{visible_els[-1].element_id}"
        coalesced_unit = Unit(
            unit_id=u_id,
            doc_id=doc.metadata.doc_id,
            elements=visible_els,
            blocks=blocks,
            part_of=(None,) * len(visible_els),
            scope_path=(),
            scope_entries_excluded=1,
            page_numbers=(1,),
            vision=False,
            oversize=False,
            hidden_excluded=(("tiny_font", 1),),
        )
        payload = "\n".join(
            [
                f"<document>{doc.metadata.doc_id.replace('_', chr(92) + '_')}</document>",
                "<scope></scope>",
                "<elements>",
                "\n".join(blocks),
                "</elements>",
            ]
        )
        env = Envelope(
            unit_id=u_id,
            prompt="Extract.",
            context_json="null",
            payload=payload,
            schema_json=ASSEMBLY.wrapper_json,
        )
        run = PreparedDoc(document=doc, units={u_id: coalesced_unit}, envelopes={u_id: env})
        rec = {
            "assembly_id": "A-100",
            "title": "Main Pump",
            "spec": {"pressure_bar": 10},
            "components": [{"part_no": "PN-1", "qty": 2, "material": "steel"}],
        }
        ans = response(
            entry(
                rec,
                ("Assembly A-100 Main Pump.", "Part PN-1 qty 2 steel.", "Pressure 10 bar."),
                (p1.element_id, p2.element_id, p3.element_id),
            )
        )
        result = run_and_fold({(u_id, G): ans, (u_id, A): ans}, run)
        (item,) = result.items
        assert [(s.element_id, s.scope_path) for s in item.sources] == [
            (p1.element_id, ("1. Electrical",)),
            (p2.element_id, ("2. Mechanical",)),
            (p3.element_id, ("2. Mechanical",)),
        ]
        assert all("SECRET_HIDDEN_HEADING" not in s.scope_path for s in item.sources)

    def test_end_to_end_coalesced_build_units_and_envelopes_preserve_per_element_section_scope(self) -> None:
        h_spec = titled(1, 0, "Spec")
        h_elec = titled(1, 1, "1. Electrical", scope_path=("Spec", "1. Electrical"))
        p1 = para(1, 2, "Assembly A-100 Main Pump.", scope_path=("Spec", "1. Electrical"), bbox=BBOX_P1)
        h_mech = titled(1, 3, "2. Mechanical", scope_path=("Spec", "2. Mechanical"))
        p2 = para(1, 4, "Part PN-1 qty 2 steel.", scope_path=("Spec", "2. Mechanical"), bbox=BBOX_P2)
        doc = make_doc(
            page(1, (h_spec, h_elec, p1, h_mech, p2)),
            sections=(
                Section(heading=h_spec, scope_path=("Spec",), page_span=(1, 1), elements=(h_spec,)),
                Section(
                    heading=h_elec,
                    scope_path=("Spec", "1. Electrical"),
                    page_span=(1, 1),
                    elements=(h_elec, p1),
                ),
                Section(
                    heading=h_mech,
                    scope_path=("Spec", "2. Mechanical"),
                    page_span=(1, 1),
                    elements=(h_mech, p2),
                ),
            ),
        )
        plan = build_units(doc, max_unit_chars=BIG, render=render_element, coalesce_sections=True)
        envelopes = build_envelopes(
            plan.units,
            doc,
            prompt="Extract.",
            context=None,
            schema_json=ASSEMBLY.wrapper_json,
            coalesce_sections=True,
        )
        (unit,) = plan.units
        assert unit.scope_path == ("Spec",)
        run = PreparedDoc(
            document=doc,
            units={u.unit_id: u for u in plan.units},
            envelopes={e.unit_id: e for e in envelopes},
        )
        rec = {
            "assembly_id": "A-100",
            "title": "Main Pump",
            "components": [{"part_no": "PN-1", "qty": 2, "material": "steel"}],
        }
        ans = response(
            entry(
                rec,
                ("Assembly A-100 Main Pump.", "Part PN-1 qty 2 steel."),
                (p1.element_id, p2.element_id),
            )
        )
        result = run_and_fold({(unit.unit_id, G): ans, (unit.unit_id, A): ans}, run)
        (item,) = result.items
        assert [(s.element_id, s.scope_path) for s in item.sources] == [
            (p1.element_id, ("Spec", "1. Electrical")),
            (p2.element_id, ("Spec", "2. Mechanical")),
        ]


class TestNode4Folding:
    def test_fold_across_units_false_preserves_multi_customer_prices_and_informational_folds_cleanly(self):
        class CustomerPrice(BaseModel):
            sku: Annotated[str, Key]
            description: Annotated[str | None, Informational] = None
            price: Decimal

        h1 = Heading(**ids(1, 0, ("Customer A",)), level=1, text="Customer A")
        p1 = Paragraph(**ids(1, 1, ("Customer A",)), text="SKU-100 Valve 10.00")
        h2 = Heading(**ids(2, 0, ("Customer B",)), level=1, text="Customer B")
        p2 = Paragraph(**ids(2, 1, ("Customer B",)), text="SKU-100 Valve 12.50")
        doc = PDFDocument(
            metadata=metadata(2, source_name="multi_tier.pdf", sha=hashlib.sha256(b"multi_tier").hexdigest()),
            pages=(page(1, (h1, p1)), page(2, (h2, p2))),
            sections=(
                Section(heading=h1, scope_path=("Customer A",), page_span=(1, 1), elements=(h1, p1)),
                Section(heading=h2, scope_path=("Customer B",), page_span=(2, 2), elements=(h2, p2)),
            ),
        )

        def answer_tier(req):
            if "p0001_e001" in req.user_envelope:
                rec = {"sku": "SKU-100", "description": "Standard Valve", "price": "10.00"}
                item_1 = {
                    "record": rec,
                    "grounding_quotes": ["SKU-100 Valve 10.00"],
                    "source_element_ids": ["p0001_e001"],
                }
                return {"records": [item_1]}
            rec = {"sku": "SKU-100", "description": "Valve - Tier B", "price": "12.50"}
            item_2 = {"record": rec, "grounding_quotes": ["SKU-100 Valve 12.50"], "source_element_ids": ["p0002_e001"]}
            return {"records": [item_2]}

        models = (
            CallableProvider("callable:g-tier", answer_tier, family="google"),
            CallableProvider("callable:a-tier", answer_tier, family="anthropic"),
        )
        unfolded = rationalize(
            doc,
            schema=CustomerPrice,
            prompt="Extract prices.",
            models=models,
            coalesce_sections=False,
            fold_across_units=False,
        )
        assert [(r.sku, r.price) for r in unfolded.records] == [
            ("SKU-100", Decimal("10.00")),
            ("SKU-100", Decimal("12.50")),
        ]
        assert [item.sources[0].scope_path for item in unfolded.items] == [("Customer A",), ("Customer B",)]

        # Matching cross-page prices with differing Informational descriptions fold cleanly!
        def answer_same_price(req):
            if "p0001_e001" in req.user_envelope:
                rec = {"sku": "SKU-100", "description": "Valve Page 1", "price": "10.00"}
                item_1 = {
                    "record": rec,
                    "grounding_quotes": ["SKU-100 Valve 10.00"],
                    "source_element_ids": ["p0001_e001"],
                }
                return {"records": [item_1]}
            rec = {"sku": "SKU-100", "description": "Valve Page 2 wording", "price": "10.00"}
            item_2 = {"record": rec, "grounding_quotes": ["SKU-100 Valve 12.50"], "source_element_ids": ["p0002_e001"]}
            return {"records": [item_2]}

        folded = rationalize(
            doc,
            schema=CustomerPrice,
            prompt="Extract prices.",
            models=(
                CallableProvider("callable:g-fold", answer_same_price, family="google"),
                CallableProvider("callable:a-fold", answer_same_price, family="anthropic"),
            ),
            coalesce_sections=False,
            fold_across_units=True,
        )
        assert len(folded.records) == 1
        assert folded.records[0].price == Decimal("10.00")
        assert folded.needs_review == ()

    def test_strict_false_isolates_page_render_refusal_on_image_only_page(self):
        class SimpleRow(BaseModel):
            sku: Annotated[str, Key]
            price: Decimal

        def refuse_png() -> bytes:
            raise RenderRefused("synthetic decompression bomb on page 2")

        p1 = page(1, (Paragraph(**ids(1, 0), text="SKU-100 10.00"),))
        bad_img = PageImage(page_number=2, dpi=150, width_px=100, height_px=100, _loader=refuse_png)
        p2 = dataclasses.replace(
            page(2, (), route="vision"),
            status="ocr_unavailable",
            image_only_reasons=("too_little_text",),
            page_image=bad_img,
        )
        doc = PDFDocument(
            metadata=metadata(2, source_name="mixed_refusal.pdf", sha=hashlib.sha256(b"mixed_ref").hexdigest()),
            pages=(p1, p2),
        )

        def answer_ok(req):
            return {
                "records": [
                    {
                        "record": {"sku": "SKU-100", "price": "10.00"},
                        "grounding_quotes": ["SKU-100 10.00"],
                        "source_element_ids": ["p0001_e000"],
                    }
                ]
            }

        res = rationalize(
            doc,
            schema=SimpleRow,
            prompt="Extract.",
            models=(
                CallableProvider("callable:g-ref", answer_ok, family="google"),
                CallableProvider("callable:a-ref", answer_ok, family="anthropic"),
            ),
            strict=False,
        )
        assert [r.sku for r in res.records] == ["SKU-100"]
        assert any(s.page_numbers == (2,) for s in res.skipped_units)


class FinishKind(Enum):
    PLAIN = "plain"
    COATED = "coated"


class AliasedFoldLine(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True, serialize_by_alias=True)
    part_no: Annotated[str, Key]
    unit_cost: Decimal | None = None


class AliasedFoldOrder(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True, serialize_by_alias=True)
    assembly_id: Annotated[str, Key]
    finish_kind: FinishKind | None = None
    ship_date: date | None = None
    verified_at: AwareDatetime | None = None
    amounts: list[Decimal] = []
    lines: list[AliasedFoldLine] = []


class PrefixedFold(BaseModel):
    assembly_id: Annotated[str, Key]
    code: str
    label: str | None = None

    @field_validator("code")
    @classmethod
    def _prefix(cls, value: str) -> str:
        return f"PFX_{value}"


class TextualFold(BaseModel):
    assembly_id: Annotated[str, Key]
    amount: int

    @field_validator("amount", mode="before")
    @classmethod
    def _parse_text(cls, value: object) -> int:
        if not isinstance(value, str):
            raise ValueError("amount must arrive as str")
        return int(value.replace(",", ""))


class UnkeyedFoldStep(BaseModel):
    step_no: int
    note: Annotated[str, Informational] = ""


class UnkeyedStepsAssembly(BaseModel):
    assembly_id: Annotated[str, Key]
    steps: list[UnkeyedFoldStep] = []


def _skipped_vision_heading_doc() -> tuple[PDFDocument, Paragraph]:
    def refuse_png() -> bytes:
        raise RenderRefused("page 1 render refused")

    bad_img = PageImage(page_number=1, dpi=150, width_px=80, height_px=80, _loader=refuse_png)
    h_vis = Heading(**ids(1, 0, ("UNSENT_VISION_HEADING",)), level=1, text="UNSENT_VISION_HEADING")
    p_dig = para(2, 0, "Assembly A-100 Main Pump.", scope_path=("UNSENT_VISION_HEADING",), bbox=BBOX_P2)
    sec = Section(heading=h_vis, scope_path=("UNSENT_VISION_HEADING",), page_span=(1, 2), elements=(h_vis, p_dig))
    p1 = dataclasses.replace(page(1, (h_vis,), route="vision", quality=VISION_QUALITY), page_image=bad_img)
    return make_doc(p1, page(2, (p_dig,)), sections=(sec,)), p_dig


class TestWave2AFoldingRegressions:
    def test_serialize_by_alias_and_scalar_to_wire_fold_cleanly_across_units(self) -> None:
        schema = prepare(AliasedFoldOrder)
        run = prepare_doc(BOM_DOC, schema=schema)
        r1 = {
            "assemblyId": "A-100",
            "finishKind": "coated",
            "amounts": ["12.50"],
            "lines": [{"partNo": "PN-1", "unitCost": "4.25"}],
        }
        r2 = {
            "assemblyId": "A-100",
            "shipDate": "2026-11-05",
            "verifiedAt": "2026-11-05T10:30:00Z",
            "lines": [{"partNo": "PN-2", "unitCost": "8.00"}],
        }
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump", "Part PN-1 qty 2 steel"), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump", "Part PN-1 qty 2 steel"), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare", "Part PN-2 qty 4 brass"), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare", "Part PN-2 qty 4 brass"), (E_P2,))),
        }
        (item,) = run_and_fold(responses, run, schema=schema).items
        assert item.status == "consensus"
        assert item.record is not None
        assert item.record.finish_kind is FinishKind.COATED
        assert item.record.ship_date == date(2026, 11, 5)
        assert item.record.verified_at == datetime(2026, 11, 5, 10, 30, tzinfo=timezone.utc)
        assert [amt.as_tuple() for amt in item.record.amounts] == [Decimal("12.50").as_tuple()]
        assert [(ln.part_no, ln.unit_cost) for ln in item.record.lines] == [
            ("PN-1", Decimal("4.25")),
            ("PN-2", Decimal("8.00")),
        ]

    def test_source_skip_rebuild_prevents_double_running_validators_and_preserves_fields_set(self) -> None:
        pfx_schema = prepare(PrefixedFold)
        pfx_run = prepare_doc(BOM_DOC, schema=pfx_schema)
        p1 = {"assembly_id": "A-100", "code": "123"}
        p2 = {"assembly_id": "A-100", "code": "123", "label": "Spare"}
        responses = {
            (U1, G): response(entry(p1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(p1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(p2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(p2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        (item,) = run_and_fold(responses, pfx_run, schema=pfx_schema).items
        assert item.record is not None
        assert (item.status, item.record.code, item.record.label) == ("consensus", "PFX_123", "Spare")

        txt_schema = prepare(TextualFold)
        txt_run = prepare_doc(BOM_DOC, schema=txt_schema)
        t_rec = {"assembly_id": "A-100", "amount": "1,234"}
        t_resp = {
            (U1, G): response(entry(t_rec, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(t_rec, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(t_rec, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(t_rec, ("Assembly A-100 Spare",), (E_P2,))),
        }
        (txt_item,) = run_and_fold(t_resp, txt_run, schema=txt_schema).items
        assert txt_item.record is not None
        assert (txt_item.status, txt_item.record.amount) == ("consensus", 1234)

        f1 = {"assembly_id": "A-100", "title": "Main Pump"}
        f2 = {"assembly_id": "A-100", "notes": None}
        f_resp = {
            (U1, G): response(entry(f1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(f1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(f2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(f2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        (f_item,) = run_and_fold(f_resp, BOM_RUN).items
        assert f_item.record is not None
        assert {"assembly_id", "title", "notes"} <= f_item.record.model_fields_set

    def test_multimodal_flag_prevents_unsent_vision_heading_scope_leak_when_vision_unit_skipped(self) -> None:
        doc, p_dig = _skipped_vision_heading_doc()
        plan = build_units(doc, max_unit_chars=BIG, render=render_element, multimodal=True)
        kept_units, envs, skipped = build_envelopes_isolated(
            plan.units,
            doc,
            prompt="Extract.",
            context=None,
            schema_json=ASSEMBLY.wrapper_json,
            multimodal=True,
        )
        assert len(skipped) == 1 and len(kept_units) == 1
        assert all(not env.images for env in envs)
        u_dig = kept_units[0].unit_id
        units_map = {u.unit_id: u for u in kept_units}
        envs_map = {e.unit_id: e for e in envs}
        docs_map = {doc.metadata.doc_id: doc}
        ans = response(entry({"assembly_id": "A-100"}, ("Assembly A-100 Main Pump.",), (p_dig.element_id,)))
        out = RunOutcome(
            responses={(u_dig, G): ans, (u_dig, A): ans},
            failed_units=(),
            cache_hits=0,
            cache_misses=2,
            cache_corrupt=0,
            cache_write_failures=0,
            usage={},
            models=PAIR,
        )
        cons = build_consensus(out, ASSEMBLY, (u_dig,), min_families=2)
        g_res = ground(cons, outcome=out, units=units_map, envelopes=envs_map, documents=docs_map, schema=ASSEMBLY)
        folded = fold_documents(
            g_res,
            units=units_map,
            envelopes=envs_map,
            documents=docs_map,
            schema=ASSEMBLY,
            multimodal=True,
        )
        assert folded.items[0].sources[0].scope_path == ()

    def test_fold_documents_rejects_non_bool_multimodal_flag(self) -> None:
        g_res = GroundingResult(items=(), empty_units=())
        bad_mm: Any = "yes"
        with pytest.raises(TypeError, match="multimodal"):
            fold_documents(
                g_res,
                units=BOM_RUN.units,
                envelopes=BOM_RUN.envelopes,
                documents={BOM_RUN.document.metadata.doc_id: BOM_RUN.document},
                schema=ASSEMBLY,
                multimodal=bad_mm,
            )

    def test_informational_field_in_unkeyed_child_model_folds_across_units_without_conflict(self) -> None:
        schema = prepare(UnkeyedStepsAssembly)
        run = prepare_doc(BOM_DOC, schema=schema)
        r1 = {"assembly_id": "A-100", "steps": [{"step_no": 1, "note": "Page 1 wording"}]}
        r2 = {"assembly_id": "A-100", "steps": [{"step_no": 1, "note": "Page 2 wording"}]}
        responses = {
            (U1, G): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U1, A): response(entry(r1, ("Assembly A-100 Main Pump",), (E_P1,))),
            (U2, G): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
            (U2, A): response(entry(r2, ("Assembly A-100 Spare",), (E_P2,))),
        }
        (item,) = run_and_fold(responses, run, schema=schema).items
        assert item.status == "consensus"
        assert item.record == UnkeyedStepsAssembly(
            assembly_id="A-100",
            steps=[UnkeyedFoldStep(step_no=1, note="Page 1 wording")],
        )
