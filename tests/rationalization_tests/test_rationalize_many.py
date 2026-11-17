"""Tests for multi-schema ``PassSpec`` and ``rationalize_many()`` (5.4 roadmap)."""

import hashlib
import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import Annotated, Any

import pytest
from pdf_tests.documents import BOX, ids, metadata, page
from pydantic import BaseModel

from datagrunt.pdf import _bundle_load, bundle
from datagrunt.pdf.bundle import write_bundle
from datagrunt.pdf.corpus import CORPUS_FORMAT_VERSION, CorpusEntry, CorpusManifest, _write_corpus_manifest
from datagrunt.pdf.elements import Heading, KeyValue, Paragraph, PDFDocument, Section, Table
from datagrunt.pdf.render import RenderedPage
from datagrunt.rationalization import Key, PassSpec, RationalizationError, rationalize_many
from datagrunt.rationalization import api as api_module
from datagrunt.rationalization.api import MAX_PASSES
from datagrunt.rationalization.cache import FileCache
from datagrunt.rationalization.providers.base import ModelProvider
from datagrunt.rationalization.providers.callable import CallableProvider
from datagrunt.rationalization.runner import TelemetryEvent
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

NEVER_OPENED = "/nonexistent/never_opened_many.pdf"


class DocHeader(BaseModel):
    drawing_no: Annotated[str, Key]
    title: str


class PartRow(BaseModel):
    part_no: Annotated[str, Key]
    qty: int


def _fake_render(source: Path, page_index: int, *, dpi: int, password: str | None = None, **_: Any) -> RenderedPage:
    width, height = max(1, dpi // 10), max(1, dpi // 5)
    return RenderedPage(pixels=bytes([200]) * (width * height), px_width=width, px_height=height)


def _sample_doc(source_name: str = "sheet.pdf", sha: str | None = None) -> PDFDocument:
    doc_sha = hashlib.sha256(source_name.encode("utf-8")).hexdigest() if sha is None else sha
    kv = KeyValue(**ids(1, 1, (), BOX), key="Drawing", value="DWG-42")
    h1 = Heading(**ids(1, 2, ("Parts",), BOX), level=1, text="Parts")
    p1 = Paragraph(**ids(1, 3, ("Parts",), BOX), text="Drawing DWG-42 Main Valve")
    tbl = Table(
        **ids(1, 4, ("Parts",), BOX),
        headers=("Part", "Qty"),
        rows=(("PN-10", "3"), ("PN-20", "5")),
        extraction_method="lattice",
    )
    sections = (
        Section(heading=None, scope_path=(), page_span=(1, 1), elements=(kv,)),
        Section(heading=h1, scope_path=("Parts",), page_span=(1, 1), elements=(h1, p1, tbl)),
    )
    return PDFDocument(
        metadata=metadata(1, source_name=source_name, sha=doc_sha),
        pages=(page(1, (kv, h1, p1, tbl)),),
        sections=sections,
        header_context=(kv,),
    )


SAMPLE_DOC = _sample_doc()


def _multi_pass_answer(req: ModelRequest) -> ProviderResponse:
    is_header = "drawing_no" in json.dumps(req.json_schema)
    if is_header:
        records: list[dict[str, Any]] = [
            {
                "record": {"drawing_no": "DWG-42", "title": "Main Valve"},
                "grounding_quotes": ["Drawing DWG-42 Main Valve"],
                "source_element_ids": ["p0001_e003"],
            }
        ]
    else:
        records = [
            {
                "record": {"part_no": "PN-10", "qty": 3},
                "grounding_quotes": ["| PN-10 | 3 |"],
                "source_element_ids": ["p0001_e004"],
            },
            {
                "record": {"part_no": "PN-20", "qty": 5},
                "grounding_quotes": ["| PN-20 | 5 |"],
                "source_element_ids": ["p0001_e004"],
            },
        ]
    return ProviderResponse(
        content={"records": records},
        raw_text="{}",
        finish="complete",
        input_tokens=80,
        output_tokens=30,
    )


def _two_family_models() -> tuple[CallableProvider, CallableProvider]:
    return (
        CallableProvider("test:google-mp", _multi_pass_answer, family="google"),
        CallableProvider("test:anthropic-mp", _multi_pass_answer, family="anthropic"),
    )


class TestPassSpecValidation:
    def test_valid_pass_spec_defaults_and_sequence_freeze(self) -> None:
        spec = PassSpec(schema=PartRow, prompt="Extract parts.", must_agree=["qty"], key=["part_no"])
        assert spec.name is None
        assert spec.schema_version == "1.0"
        assert spec.must_agree == ("qty",)
        assert spec.key == ("part_no",)
        assert spec.max_unit_chars == 120_000
        assert spec.max_output_tokens == 8_192
        assert spec.min_families is None
        assert spec.split_on_truncation is None

    @pytest.mark.parametrize(
        ("kwargs", "exc"),
        [
            ({"schema": str, "prompt": "ok"}, TypeError),
            ({"schema": PartRow, "prompt": 123}, TypeError),
            ({"schema": PartRow, "prompt": "   "}, (ValueError, RationalizationError)),
            ({"schema": PartRow, "prompt": "ok", "name": 123}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "name": "   "}, ValueError),
            ({"schema": PartRow, "prompt": "ok", "schema_version": ""}, ValueError),
            ({"schema": PartRow, "prompt": "ok", "schema_version": 9}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "max_unit_chars": 0}, ValueError),
            ({"schema": PartRow, "prompt": "ok", "max_unit_chars": True}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "max_output_tokens": 0}, ValueError),
            ({"schema": PartRow, "prompt": "ok", "max_output_tokens": False}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "min_families": 0}, ValueError),
            ({"schema": PartRow, "prompt": "ok", "min_families": True}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "expect_non_empty": 1}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "coalesce_sections": "yes"}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "include_schema_in_prompt": 0}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "multimodal": "no"}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "fold_across_units": 1}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "split_on_truncation": "yes"}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "must_agree": "qty"}, TypeError),
            ({"schema": PartRow, "prompt": "ok", "key": 123}, TypeError),
        ],
    )
    def test_eager_field_validation(self, kwargs: dict[str, Any], exc: Any) -> None:
        with pytest.raises(exc):
            PassSpec(**kwargs)


class TestRationalizeManyPreflightAndShapes:
    def test_accepts_mapping_and_sequence_preserving_pass_order(self) -> None:
        models = _two_family_models()
        by_seq = rationalize_many(
            SAMPLE_DOC,
            passes=(
                PassSpec(DocHeader, "Extract header."),
                PassSpec(PartRow, "Extract parts.", name="bom_rows"),
            ),
            models=models,
        )
        assert list(by_seq) == ["DocHeader", "bom_rows"]
        assert [r.drawing_no for r in by_seq["DocHeader"].records] == ["DWG-42"]
        assert [r.part_no for r in by_seq["bom_rows"].records] == ["PN-10", "PN-20"]

        by_map = rationalize_many(
            SAMPLE_DOC,
            passes={
                "parts": PassSpec(PartRow, "Extract parts."),
                "header": PassSpec(DocHeader, "Extract header.", name="header"),
            },
            models=models,
        )
        assert list(by_map) == ["parts", "header"]
        assert by_map["parts"].records == by_seq["bom_rows"].records
        assert by_map["header"].records == by_seq["DocHeader"].records

    def test_rejects_invalid_passes_before_opening_target(self) -> None:
        models = _two_family_models()
        with pytest.raises((ValueError, RationalizationError)):
            rationalize_many(NEVER_OPENED, passes=(), models=models)
        with pytest.raises((ValueError, RationalizationError)):
            rationalize_many(NEVER_OPENED, passes={}, models=models)
        with pytest.raises(TypeError):
            rationalize_many(NEVER_OPENED, passes="bad", models=models)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="duplicate"):
            rationalize_many(
                NEVER_OPENED,
                passes=[PassSpec(PartRow, "p1"), PassSpec(PartRow, "p2")],
                models=models,
            )
        with pytest.raises(ValueError, match="conflicts"):
            rationalize_many(
                NEVER_OPENED,
                passes={"wrong_key": PassSpec(PartRow, "p1", name="actual_name")},
                models=models,
            )
        too_many = [PassSpec(PartRow, "p", name=f"p_{i}") for i in range(MAX_PASSES + 1)]
        with pytest.raises(ValueError, match="MAX_PASSES"):
            rationalize_many(NEVER_OPENED, passes=too_many, models=models)


class _PoolTrackingProvider(ModelProvider):
    """Fake provider tracking open() and close() calls across multiple passes."""

    def __init__(self, model_id: str, family: str) -> None:
        self._model_id = model_id
        self._family = family
        self.opened = 0
        self.closed = 0

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def provider_kind(self) -> str:
        return "callable"

    @property
    def rate_key(self) -> str:
        return self._model_id

    @property
    def model_family(self) -> str:
        return self._family

    @property
    def generation_params(self) -> dict[str, Any]:
        return {}

    def open(self) -> None:
        self.opened += 1

    def close(self) -> None:
        self.closed += 1

    def transform_schema(self, schema: Any) -> dict[str, Any]:
        return dict(schema)

    def generate(self, request: ModelRequest) -> ProviderResponse:
        return _multi_pass_answer(request)


class TestSharedPoolCorpusAndCache:
    def test_single_provider_resolution_open_and_close_across_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ephemeral = _PoolTrackingProvider("gemini:gemini-2.5-flash", "google")
        caller_owned = _PoolTrackingProvider("anthropic:caller-owned", "anthropic")
        resolve_calls: list[object] = []
        real_resolve = api_module._resolve_provider

        def spy_resolve(entry: object, *, persistent_cache: bool) -> ModelProvider:
            resolve_calls.append(entry)
            if entry == "gemini:gemini-2.5-flash":
                return ephemeral
            return real_resolve(entry, persistent_cache=persistent_cache)

        monkeypatch.setattr(api_module, "_resolve_provider", spy_resolve)
        out = rationalize_many(
            SAMPLE_DOC,
            passes=[PassSpec(DocHeader, "Header."), PassSpec(PartRow, "Parts.")],
            models=("gemini:gemini-2.5-flash", caller_owned),
        )
        assert len(out) == 2
        assert resolve_calls.count("gemini:gemini-2.5-flash") == 1
        assert ephemeral.opened == 1
        assert caller_owned.opened == 1
        assert ephemeral.closed == 1
        assert caller_owned.closed == 0

    def test_corpus_bundles_are_loaded_once_across_all_passes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        corpus_dir = (tmp_path / "corpus").resolve()
        (corpus_dir / "bundles").mkdir(parents=True, exist_ok=True)
        src_pdf = tmp_path / "sheet.pdf"
        src_pdf.write_bytes(b"%PDF-1.4\n%sheet\n%%EOF\n")
        sha = hashlib.sha256(src_pdf.read_bytes()).hexdigest()
        doc = _sample_doc("sheet.pdf", sha=sha)
        rel = f"bundles/{doc.metadata.doc_id}"
        with monkeypatch.context() as m:
            m.setattr(bundle, "render_page", _fake_render)
            written = write_bundle(doc, corpus_dir / rel, source=src_pdf, dpi=72)
        entry = CorpusEntry(
            doc_id=written.metadata.doc_id,
            source_name=written.metadata.source_name,
            source_sha256=written.metadata.sha256,
            bundle_relpath=rel,
            page_count=written.metadata.page_count,
            integrity_verified=written.integrity_verified,
            degraded_pages=written.degraded_pages,
            reused=False,
        )
        manifest = CorpusManifest(
            corpus_format_version=CORPUS_FORMAT_VERSION,
            output_dir=corpus_dir,
            entries=(entry,),
            failures=(),
        )
        _write_corpus_manifest(corpus_dir, manifest)

        loaded_reads = 0
        real_loaded = _bundle_load._loaded

        def spy_loaded(*args: Any, **kwargs: Any) -> Any:
            nonlocal loaded_reads
            loaded_reads += 1
            return real_loaded(*args, **kwargs)

        monkeypatch.setattr(_bundle_load, "_loaded", spy_loaded)
        out = rationalize_many(
            corpus_dir,
            passes=[PassSpec(DocHeader, "Header."), PassSpec(PartRow, "Parts.")],
            models=_two_family_models(),
        )
        assert len(out["DocHeader"].records) == 1
        assert len(out["PartRow"].records) == 2
        assert loaded_reads == 1

    def test_passes_execute_concurrently_and_write_per_pass_cache_meta(self, tmp_path: Path) -> None:
        both_in_flight = threading.Barrier(2, timeout=5.0)

        def concurrent_google(req: ModelRequest) -> ProviderResponse:
            both_in_flight.wait()
            return _multi_pass_answer(req)

        cache_dir = tmp_path / "mp_cache"
        events: list[TelemetryEvent] = []
        p_g = CallableProvider("test:g-conc", concurrent_google, family="google")
        p_a = CallableProvider("test:a-conc", _multi_pass_answer, family="anthropic")

        out = rationalize_many(
            SAMPLE_DOC,
            passes={
                "hdr": PassSpec(DocHeader, "Header.", schema_version="2.1"),
                "rows": PassSpec(PartRow, "Parts.", schema_version="3.4"),
            },
            models=(p_g, p_a),
            cache_dir=cache_dir,
            max_concurrency=4,
            event_delivery="live",
            on_event=events.append,
        )
        assert len(out["hdr"].records) == 1
        assert len(out["rows"].records) == 2
        assert len(events) == 4

        meta_entries = [json.loads(p.read_text(encoding="utf-8"))["meta"] for p in cache_dir.rglob("*.json")]
        assert {(m["schema_name"], m["schema_version"]) for m in meta_entries} == {
            ("DocHeader", "2.1"),
            ("PartRow", "3.4"),
        }

        out_cached = rationalize_many(
            SAMPLE_DOC,
            passes={
                "hdr": PassSpec(DocHeader, "Header.", schema_version="2.1"),
                "rows": PassSpec(PartRow, "Parts.", schema_version="3.4"),
            },
            models=_two_family_models()[0:0] + (p_g, p_a),
            cache_dir=cache_dir,
        )
        assert out_cached["hdr"].telemetry.cache_hits == 2
        assert out_cached["hdr"].telemetry.cache_misses == 0
        assert out_cached["rows"].telemetry.cache_hits == 2
        assert out_cached["rows"].telemetry.cache_misses == 0


class TestMultiPassBisectionAndOverrides:
    def test_per_pass_truncation_bisection_min_families_and_expect_non_empty(self) -> None:
        def selective_trunc(req: ModelRequest) -> ProviderResponse:
            is_header = "drawing_no" in json.dumps(req.json_schema)
            if not is_header and req.unit_id.endswith(":p0001_e001-p0001_e004"):
                return ProviderResponse(
                    content=None, raw_text="{", finish="truncated", input_tokens=50, output_tokens=90
                )
            if not is_header and not req.unit_id.endswith("p0001_e004"):
                return ProviderResponse(
                    content={"records": []}, raw_text="{}", finish="complete", input_tokens=20, output_tokens=5
                )
            return _multi_pass_answer(req)

        p_g = CallableProvider("test:g-sel", selective_trunc, family="google")
        p_a = CallableProvider("test:a-sel", selective_trunc, family="anthropic")

        out = rationalize_many(
            SAMPLE_DOC,
            passes={
                "hdr_no_split": PassSpec(DocHeader, "Header.", split_on_truncation=False, expect_non_empty=True),
                "rows_split": PassSpec(PartRow, "Parts.", split_on_truncation=True, expect_non_empty=True),
            },
            models=(p_g, p_a),
            split_on_truncation=False,
        )
        assert [r.drawing_no for r in out["hdr_no_split"].records] == ["DWG-42"]
        assert out["hdr_no_split"].telemetry.unit_count == 1
        assert [r.part_no for r in out["rows_split"].records] == ["PN-10", "PN-20"]
        assert out["rows_split"].telemetry.unit_count == 2
        assert out["rows_split"].telemetry.cache_misses == 6

    def test_per_pass_min_families_and_expect_non_empty_failure(self) -> None:
        g1 = CallableProvider("test:g1-same-fam", _multi_pass_answer, family="google")
        g2 = CallableProvider("test:g2-same-fam", _multi_pass_answer, family="google")

        # Pass-level min_families=2 rejects 1-family model set at preflight even when call min_families=1.
        with pytest.raises(RationalizationError, match="min_families"):
            rationalize_many(
                NEVER_OPENED,
                passes={
                    "hdr": PassSpec(DocHeader, "Header.", min_families=1),
                    "rows": PassSpec(PartRow, "Parts.", min_families=2),
                },
                models=(g1, g2),
                min_families=1,
            )

        out = rationalize_many(
            SAMPLE_DOC,
            passes={"hdr": PassSpec(DocHeader, "Header.", min_families=1)},
            models=(g1, g2),
            min_families=1,
        )
        assert [r.drawing_no for r in out["hdr"].records] == ["DWG-42"]

        def header_only(req: ModelRequest) -> ProviderResponse:
            if "drawing_no" in json.dumps(req.json_schema):
                return _multi_pass_answer(req)
            return ProviderResponse(content={"records": []}, raw_text="{}", finish="complete")

        with pytest.raises(RationalizationError, match="rows"):
            rationalize_many(
                SAMPLE_DOC,
                passes={
                    "hdr": PassSpec(DocHeader, "Header."),
                    "rows": PassSpec(PartRow, "Parts.", expect_non_empty=True),
                },
                models=(
                    CallableProvider("test:g-empty-rows", header_only, family="google"),
                    CallableProvider("test:a-empty-rows", header_only, family="anthropic"),
                ),
            )

    def test_cumulative_max_requests_enforced_across_passes(self) -> None:
        calls = 0

        def counting(req: ModelRequest) -> ProviderResponse:
            nonlocal calls
            calls += 1
            return _multi_pass_answer(req)

        p_g = CallableProvider("test:g-lim", counting, family="google")
        p_a = CallableProvider("test:a-lim", counting, family="anthropic")
        # 2 passes × 1 unit × 2 models = 4 misses > max_requests=3 -> fails preflight with 0 calls!
        with pytest.raises(RationalizationError, match="max_requests"):
            rationalize_many(
                SAMPLE_DOC,
                passes=[PassSpec(DocHeader, "Header."), PassSpec(PartRow, "Parts.")],
                models=(p_g, p_a),
                max_requests=3,
            )
        assert calls == 0

    def test_import_datagrunt_loads_zero_heavy_dependencies(self) -> None:
        code = (
            "import json, sys; import datagrunt, datagrunt.rationalization; "
            "heavy = ('pydantic', 'httpx', 'polars', 'pyarrow'); "
            "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in heavy)))"
        )
        proc = subprocess.run(
            [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=60, check=False
        )
        assert proc.returncode == 0, proc.stderr
        assert json.loads(proc.stdout.strip().splitlines()[-1]) == []

    def test_parallel_file_cache_prewarm_preserves_corrupt_order_and_lowest_index_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_dir = tmp_path / "warm_cache"
        passes = [
            PassSpec(DocHeader, "Header.", schema_version="1.0", coalesce_sections=False),
            PassSpec(PartRow, "Parts 1.", name="rows_1", schema_version="2.0", coalesce_sections=False),
            PassSpec(PartRow, "Parts 2.", name="rows_2", schema_version="3.0", coalesce_sections=False),
        ]
        # Cold run: 3 passes × 2 units × 2 models = 12 distinct request keys (> 4 -> parallel FileCache pre-warm).
        rationalize_many(SAMPLE_DOC, passes=passes, models=_two_family_models(), cache_dir=cache_dir)
        by_ver: dict[str, list[str]] = {"1.0": [], "2.0": [], "3.0": []}
        for p in sorted(cache_dir.rglob("*.json")):
            entry = json.loads(p.read_text(encoding="utf-8"))
            by_ver[entry["meta"]["schema_version"]].append(entry["key"])
        assert sum(len(v) for v in by_ver.values()) == 12

        barrier = threading.Barrier(12, timeout=3.0)
        seen_keys: list[str] = []
        seen_lock = threading.Lock()
        real_get = FileCache.get

        def concurrent_get(self: FileCache, key: str):
            with seen_lock:
                seen_keys.append(key)
            barrier.wait()
            return real_get(self, key)

        monkeypatch.setattr(FileCache, "get", concurrent_get)
        hit_out = rationalize_many(SAMPLE_DOC, passes=passes, models=_two_family_models(), cache_dir=cache_dir)
        assert len(seen_keys) == 12
        assert len(set(seen_keys)) == 12
        assert sum(r.telemetry.cache_hits for r in hit_out.values()) == 12

        # Corrupt a Pass 0 ("1.0") key and a Pass 2 ("3.0") key; delay Pass 0 until Pass 2 finishes -> ordered!
        key_early, key_late = by_ver["1.0"][0], by_ver["3.0"][0]
        for k in (key_early, key_late):
            (cache_dir / k[:2] / f"{k}.json").write_text("{corrupt", encoding="utf-8")
        late_done = threading.Event()

        def delayed_corrupt_get(self: FileCache, key: str):
            if key == key_early:
                late_done.wait(timeout=3.0)
            res = real_get(self, key)
            if key == key_late:
                late_done.set()
            return res

        monkeypatch.setattr(FileCache, "get", delayed_corrupt_get)
        events: list[TelemetryEvent] = []
        corrupt_out = rationalize_many(
            SAMPLE_DOC, passes=passes, models=_two_family_models(), cache_dir=cache_dir, on_event=events.append
        )
        corrupt_events = [ev for ev in events if ev.kind == "cache_corrupt"]
        assert len(corrupt_events) == 2
        assert sum(r.telemetry.cache_corrupt for r in corrupt_out.values()) == 2
        # Pass 0 ("DocHeader", index 1) corrupt event arrives before Pass 2 ("rows_2", index 8) corrupt event.
        assert corrupt_out["DocHeader"].telemetry.cache_corrupt == 1
        assert corrupt_out["rows_2"].telemetry.cache_corrupt == 1
        assert events.index(corrupt_events[0]) < events.index(corrupt_events[1])

        # Lowest-index exception when multiple FileCache.get calls raise concurrently!
        late_raised = threading.Event()

        def ordered_error_get(self: FileCache, key: str):
            if key == key_late:
                late_raised.set()
                raise RationalizationError("late_shard_failure")
            if key == key_early:
                late_raised.wait(timeout=3.0)
                raise RationalizationError("early_shard_failure")
            return real_get(self, key)

        monkeypatch.setattr(FileCache, "get", ordered_error_get)
        with pytest.raises(RationalizationError, match="early_shard_failure"):
            rationalize_many(SAMPLE_DOC, passes=passes, models=_two_family_models(), cache_dir=cache_dir)
