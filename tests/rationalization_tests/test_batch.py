"""Tests for per-provider concurrency caps, prompt schema trimming, and offline cloud batch mode (#464)."""

import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from datagrunt.rationalization import (
    AnthropicProvider,
    BatchItem,
    BatchPlan,
    CallableProvider,
    GeminiProvider,
    GoogleCloudAuth,
    OpenAICompatibleProvider,
    ProviderResponse,
    RationalizationError,
    ingest_batch,
    prepare_batch,
    rationalize,
)
from datagrunt.rationalization.runner import RunLimits, _slot_counts, snapshot_providers
from datagrunt.rationalization.schema import prepare
from datagrunt.rationalization.wire import ModelRequest
from rationalization_tests.test_api import (
    BOM_DOC,
    Assembly,
    _bom_response_for,
    make_bom_providers,
)


class TestPerProviderConcurrencyCap:
    """Tests for Track C.1: per-provider max_concurrency and with_max_concurrency()."""

    def test_providers_expose_with_max_concurrency_and_validate_bounds(self) -> None:
        gem = GeminiProvider("gemini-2.5-flash", api_key="AIza-test-key")
        ant = AnthropicProvider("claude-opus-4-7", api_key="sk-ant-test-key")
        oa = OpenAICompatibleProvider("llama3", base_url="http://127.0.0.1:8000/v1")
        call_p = CallableProvider("test:callable", _bom_response_for, family="google")

        for provider in (gem, ant, oa, call_p):
            assert provider.max_concurrency is None
            returned = provider.with_max_concurrency(2)
            assert returned is provider
            assert provider.max_concurrency == 2
            provider.with_max_concurrency(None)
            assert provider.max_concurrency is None

            for bad in (0, -1, 65, True, "2"):
                with pytest.raises(RationalizationError, match="max_concurrency"):
                    provider.with_max_concurrency(bad)  # type: ignore[arg-type]

    def test_slot_counts_respects_per_provider_max_concurrency(self) -> None:
        prepared = prepare(Assembly)
        p_slow = CallableProvider("test:dsq-gemini", _bom_response_for, family="google").with_max_concurrency(1)
        p_fast = CallableProvider("test:fast-claude", _bom_response_for, family="anthropic").with_max_concurrency(6)
        snapshots = snapshot_providers((p_slow, p_fast), prepared)
        assert snapshots[0].max_concurrency == 1
        assert snapshots[1].max_concurrency == 6

        limits = RunLimits(max_concurrency=8, max_concurrency_per_provider=4)
        slots = _slot_counts(snapshots, limits)
        assert slots[snapshots[0].rate_key] == 1
        assert slots[snapshots[1].rate_key] == 4

    def test_runner_caps_in_flight_requests_per_provider_max_concurrency(self) -> None:
        lock = threading.Lock()
        in_flight = {"slow": 0, "max_slow": 0, "fast": 0, "max_fast": 0}

        def slow_fn(req: ModelRequest) -> ProviderResponse:
            with lock:
                in_flight["slow"] += 1
                in_flight["max_slow"] = max(in_flight["max_slow"], in_flight["slow"])
            time.sleep(0.02)
            with lock:
                in_flight["slow"] -= 1
            return _bom_response_for(req)

        def fast_fn(req: ModelRequest) -> ProviderResponse:
            with lock:
                in_flight["fast"] += 1
                in_flight["max_fast"] = max(in_flight["max_fast"], in_flight["fast"])
            time.sleep(0.02)
            with lock:
                in_flight["fast"] -= 1
            return _bom_response_for(req)

        p_slow = CallableProvider("test:slow-g", slow_fn, family="google").with_max_concurrency(1)
        p_fast = CallableProvider("test:fast-a", fast_fn, family="anthropic").with_max_concurrency(4)

        res = rationalize(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_slow, p_fast),
            coalesce_sections=False,
            max_concurrency=8,
            max_concurrency_per_provider=4,
        )
        assert len(res.records) == 2
        assert in_flight["max_slow"] == 1
        assert in_flight["max_fast"] >= 1


class TestPromptSchemaTrimming:
    """Tests for Track C.2: include_schema_in_prompt=False trims inline schema from system_prompt."""

    def test_include_schema_in_prompt_false_omits_inline_schema_and_preserves_wire_schema(self) -> None:
        seen_with_schema: list[ModelRequest] = []
        seen_without_schema: list[ModelRequest] = []

        p_g1 = CallableProvider(
            "test:g1",
            lambda r: (seen_with_schema.append(r), _bom_response_for(r))[1],
            family="google",
        )
        p_a1 = CallableProvider("test:a1", _bom_response_for, family="anthropic")

        res_with = rationalize(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_g1, p_a1),
            include_schema_in_prompt=True,
        )

        p_g2 = CallableProvider(
            "test:g1",
            lambda r: (seen_without_schema.append(r), _bom_response_for(r))[1],
            family="google",
        )
        p_a2 = CallableProvider("test:a1", _bom_response_for, family="anthropic")

        res_without = rationalize(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_g2, p_a2),
            include_schema_in_prompt=False,
        )

        assert res_without.records == res_with.records
        assert len(seen_with_schema) == 1
        assert len(seen_without_schema) == 1

        sys_with = seen_with_schema[0].system_prompt
        sys_without = seen_without_schema[0].system_prompt
        assert "The response schema, as JSON:" in sys_with
        assert "The response schema, as JSON:" not in sys_without
        assert len(sys_without) < len(sys_with) - 500
        # Structured output schema on ModelRequest is still populated identically.
        assert seen_without_schema[0].json_schema == seen_with_schema[0].json_schema


class TestOfflineCloudBatchMode:
    """Tests for Track C.3: prepare_batch() and ingest_batch() (#464)."""

    def test_prepare_and_ingest_batch_matches_rationalize_and_populates_cache(self, tmp_path: Path) -> None:
        cache_dir = tmp_path / "batch_cache"
        p_google, p_anthropic, counts = make_bom_providers()

        plan = prepare_batch(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_google, p_anthropic),
            cache_dir=cache_dir,
            include_schema_in_prompt=False,
        )
        assert isinstance(plan, BatchPlan)
        assert counts == {"google": 0, "anthropic": 0}
        assert len(plan.items) == 2
        assert len(plan.cached_hits) == 0
        assert all(isinstance(item, BatchItem) for item in plan.items)
        assert all("The response schema, as JSON:" not in item.request.system_prompt for item in plan.items)

        responses = {item.custom_id: _bom_response_for(item.request) for item in plan.items}
        result = ingest_batch(plan, responses)

        online_g, online_a, _ = make_bom_providers()
        expected = rationalize(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(online_g, online_a),
            include_schema_in_prompt=False,
        )
        assert result.records == expected.records
        assert result.telemetry.cache_misses == 2
        assert result.telemetry.cache_hits == 0

        # A second prepare_batch against the same cache_dir hits the persistent cache for all items!
        plan_cached = prepare_batch(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_google, p_anthropic),
            cache_dir=cache_dir,
            include_schema_in_prompt=False,
        )
        assert len(plan_cached.items) == 0
        assert len(plan_cached.cached_hits) == 2
        result_cached = ingest_batch(plan_cached, {})
        assert result_cached.records == expected.records
        assert result_cached.telemetry.cache_hits == 2
        assert result_cached.telemetry.cache_misses == 0

    def test_vertex_and_anthropic_jsonl_export_and_ingestion(self, tmp_path: Path) -> None:
        gem = GeminiProvider.google_cloud(
            "gemini-2.5-pro",
            project="demo-proj",
            location="global",
            auth=GoogleCloudAuth(token=lambda: "ya29.unused"),
        )
        ant = AnthropicProvider("claude-opus-4-7", api_key="sk-ant-test-key")

        plan = prepare_batch(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(gem, ant),
            include_schema_in_prompt=False,
        )
        assert len(plan.items) == 2
        assert len(plan.items_for(provider_kind="google-cloud-gemini")) == 1
        assert len(plan.items_for(provider_kind="anthropic")) == 1

        vertex_req_path = tmp_path / "vertex_requests.jsonl"
        anthropic_req_path = tmp_path / "anthropic_requests.jsonl"
        assert plan.to_vertex_jsonl(vertex_req_path) == 1
        assert plan.to_anthropic_jsonl(anthropic_req_path) == 1

        vertex_req_line = json.loads(vertex_req_path.read_text(encoding="utf-8").strip())
        anthropic_req_line = json.loads(anthropic_req_path.read_text(encoding="utf-8").strip())
        assert "custom_id" in vertex_req_line
        assert "generationConfig" in vertex_req_line["request"]
        assert "custom_id" in anthropic_req_line
        assert anthropic_req_line["params"]["model"] == "claude-opus-4-7"
        assert anthropic_req_line["params"]["output_config"]["format"]["type"] == "json_schema"

        # Simulate Vertex AI and Anthropic batch output JSONL files
        gem_item = plan.items_for(provider_kind="google-cloud-gemini")[0]
        ant_item = plan.items_for(provider_kind="anthropic")[0]
        gem_payload = _bom_response_for(gem_item.request).content
        ant_payload = _bom_response_for(ant_item.request).content

        vertex_out_path = tmp_path / "vertex_out.jsonl"
        vertex_out_path.write_text(
            json.dumps(
                {
                    "custom_id": gem_item.custom_id,
                    "response": {
                        "candidates": [
                            {
                                "content": {"parts": [{"text": json.dumps(gem_payload)}], "role": "model"},
                                "finishReason": "STOP",
                            }
                        ],
                        "usageMetadata": {"promptTokenCount": 500, "candidatesTokenCount": 120},
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )

        anthropic_out_path = tmp_path / "anthropic_out.jsonl"
        anthropic_out_path.write_text(
            json.dumps(
                {
                    "custom_id": ant_item.custom_id,
                    "result": {
                        "type": "succeeded",
                        "message": {
                            "id": "msg_batch_1",
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "text", "text": json.dumps(ant_payload)}],
                            "stop_reason": "end_turn",
                            "usage": {"input_tokens": 480, "output_tokens": 115},
                        },
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )

        result = ingest_batch(plan, [vertex_out_path, anthropic_out_path])
        assert len(result.records) == 2
        assert result.records[0].assembly_id == "A-100"
        assert result.records[1].assembly_id == "A-200"
        assert result.telemetry.usage_by_model[gem.model_id].input_tokens == 500
        assert result.telemetry.usage_by_model[ant.model_id].input_tokens == 480

    def test_ingest_batch_handles_missing_and_errored_items(self) -> None:
        p_google, p_anthropic, _ = make_bom_providers()
        plan = prepare_batch(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_google, p_anthropic),
            strict=False,
        )
        g_item = plan.items[0]
        a_item = plan.items[1]
        responses: dict[str, Any] = {
            g_item.custom_id: _bom_response_for(g_item.request),
            a_item.custom_id: {"custom_id": a_item.custom_id, "result": {"type": "errored"}},
        }
        res = ingest_batch(plan, responses)
        assert len(res.failed_units) == 1
        assert res.failed_units[0].model_id == a_item.model_id
        assert len(res.needs_review) == 2

        plan_strict = prepare_batch(
            BOM_DOC,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(p_google, p_anthropic),
            strict=True,
        )
        with pytest.raises(RationalizationError, match="strict"):
            ingest_batch(plan_strict, responses)


MM_BATCH_PNG = b"\x89PNG\r\n\x1a\nBATCH_SCAN_PNG"


class TestMultimodalBatch:
    def test_prepare_batch_serializes_multimodal_png_bands_for_vertex_and_anthropic(self, tmp_path) -> None:
        import base64
        import dataclasses

        from datagrunt.pdf.elements import PageImage, PDFDocument

        scan_img = PageImage(
            page_number=1,
            dpi=300.0,
            width_px=1200,
            height_px=1600,
            _band_loader=lambda: (MM_BATCH_PNG,),
        )
        scan_page = dataclasses.replace(BOM_DOC.pages[0], route="vision", page_image=scan_img)
        scan_doc = PDFDocument(
            metadata=dataclasses.replace(BOM_DOC.metadata, page_count=1),
            pages=(scan_page,),
        )
        gem = GeminiProvider("gemini-2.5-flash", api_key="AIzaSyDummyKeyForOfflineBatch12345")
        ant = AnthropicProvider("claude-sonnet-4-6", api_key="sk-ant-dummy-key-offline-batch-12345")

        plan = prepare_batch(
            scan_doc,
            schema=Assembly,
            prompt="Extract BOM.",
            models=(gem, ant),
            multimodal=True,
        )
        assert len(plan.items) == 2
        assert all(item.request.images == (MM_BATCH_PNG,) for item in plan.items)

        v_path = tmp_path / "mm_vertex.jsonl"
        a_path = tmp_path / "mm_anthropic.jsonl"
        assert plan.to_vertex_jsonl(v_path) == 1
        assert plan.to_anthropic_jsonl(a_path) == 1

        b64 = base64.b64encode(MM_BATCH_PNG).decode("ascii")
        v_obj = json.loads(v_path.read_text(encoding="utf-8").strip())
        assert v_obj["request"]["contents"][0]["parts"][0] == {"inlineData": {"mimeType": "image/png", "data": b64}}
        a_obj = json.loads(a_path.read_text(encoding="utf-8").strip())
        assert a_obj["params"]["messages"][0]["content"][0] == {
            "type": "image",
            "source": {"type": "base64", "media_type": "image/png", "data": b64},
        }
