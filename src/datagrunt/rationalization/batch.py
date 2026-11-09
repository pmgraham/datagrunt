"""Offline cloud batch preparation and ingestion for ``rationalize()`` (#464).

:func:`prepare_batch` runs the exact preflight, unit packing, envelope rendering, and cache probing of
:func:`~datagrunt.rationalization.api.rationalize`, returning a :class:`BatchPlan` of uncached
:class:`BatchItem` requests without making any network calls. Callers can serialize items to Vertex AI
(:meth:`BatchPlan.to_vertex_jsonl`) or Anthropic Message Batches (:meth:`BatchPlan.to_anthropic_jsonl`)
JSONL files, submit them to cloud batch endpoints (for 50% token discount and zero online DSQ contention),
and pass the completed JSONL paths or response mappings to :func:`ingest_batch` to run the identical
consensus, grounding, folding, cache-persistence, and :class:`~datagrunt.rationalization.result.RationalizedResult`
assembly pipeline.

The module imports only the standard library, pydantic, the PDF model and its sibling modules: nothing from
``datagrunt.core``, and no import inside a function (invariant 9).
"""

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from os import PathLike
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeAlias, TypeVar, cast

from pydantic import BaseModel

from datagrunt.rationalization._canonical import canonical_copy, canonical_json, sha256_hex, strict_loads
from datagrunt.rationalization.api import (
    ModelInput,
    TargetInput,
    _assemble_pipeline_result,
    _build_target_work,
    _check_scalar_params,
    _normalize_targets,
    _prepare_snapshots,
    _read_prompt,
    _TargetWork,
)
from datagrunt.rationalization.cache import CacheMeta, FileCache
from datagrunt.rationalization.consensus import check_must_agree
from datagrunt.rationalization.envelope import Envelope, _check_schema, _context_json, new_nonce
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.anthropic import (
    _FINISH_BY_STOP_REASON as _ANTHROPIC_FINISH_BY_STOP,
)
from datagrunt.rationalization.providers.anthropic import (
    FALLBACK_MECHANISM as _ANTHROPIC_FALLBACK_MECHANISM,
)
from datagrunt.rationalization.providers.anthropic import (
    MECHANISM as _ANTHROPIC_MECHANISM,
)
from datagrunt.rationalization.providers.anthropic import (
    PROMPT_MECHANISM as _ANTHROPIC_PROMPT_MECHANISM,
)
from datagrunt.rationalization.providers.anthropic import (
    _answer_text as _anthropic_answer_text,
)
from datagrunt.rationalization.providers.anthropic import (
    _exceeds_limits as _anthropic_exceeds_limits,
)
from datagrunt.rationalization.providers.anthropic import (
    _input_tokens as _anthropic_input_tokens,
)
from datagrunt.rationalization.providers.anthropic import (
    _prompt_system as _anthropic_prompt_system,
)
from datagrunt.rationalization.providers.anthropic import (
    _strip_json_fence as _anthropic_strip_fence,
)
from datagrunt.rationalization.providers.base import bounded_detail
from datagrunt.rationalization.providers.gemini import (
    _candidate_text as _gemini_candidate_text,
)
from datagrunt.rationalization.providers.gemini import (
    _finish as _gemini_finish,
)
from datagrunt.rationalization.providers.gemini import (
    _usage as _gemini_usage,
)
from datagrunt.rationalization.providers.responses import parse_json_object
from datagrunt.rationalization.providers.responses import response as _build_provider_response
from datagrunt.rationalization.result import RationalizedResult
from datagrunt.rationalization.runner import (
    NONCE_DRAWS,
    FailedUnit,
    ModelUsage,
    ProviderSnapshot,
    RunOutcome,
    _file_put,
    _plan,
)
from datagrunt.rationalization.schema import PreparedSchema, prepare
from datagrunt.rationalization.wire import FinishReason, ModelRequest, ProviderResponse

T = TypeVar("T", bound=BaseModel)
BatchResponseInput: TypeAlias = (
    Mapping[Any, ProviderResponse | Mapping[str, Any]] | str | PathLike[str] | Sequence[str | PathLike[str]]
)

_GEMINI_KINDS = frozenset({"gemini", "google-cloud-gemini"})
_ANTHROPIC_KINDS = frozenset({"anthropic", "google-cloud-anthropic"})

__all__ = ["BatchItem", "BatchPlan", "ingest_batch", "prepare_batch"]


def _draw_envelope_messages(envelope: Envelope) -> tuple[str, str]:
    """Draw a fresh collision-free nonce and render ``(system_prompt, user_envelope)``."""
    texts = (envelope.prompt, envelope.schema_json, envelope.context_json, envelope.payload)
    for _ in range(NONCE_DRAWS):
        candidate = new_nonce()
        if all(candidate not in text for text in texts):
            return envelope.render(candidate)
    raise RationalizationError(f"failed to draw a collision-free nonce in {NONCE_DRAWS} attempts")


def _vertex_request_body(item: "BatchItem", snapshot: ProviderSnapshot) -> dict[str, Any]:
    """Construct the Vertex AI / Gemini ``generateContent`` request dict for ``item``."""
    params = snapshot.generation_params
    schema_field = str(params.get("schema_field", "responseSchema"))
    config: dict[str, Any] = {
        "responseMimeType": "application/json",
        schema_field: dict(item.request.json_schema),
        "maxOutputTokens": item.request.max_output_tokens,
    }
    if "temperature" in params and params["temperature"] is not None:
        config["temperature"] = params["temperature"]
    if "seed" in params and params["seed"] is not None:
        config["seed"] = params["seed"]
    if "thinking_level" in params and params["thinking_level"] is not None:
        config["thinkingConfig"] = {"thinkingLevel": params["thinking_level"]}
    parts: list[dict[str, Any]] = [
        {"inlineData": {"mimeType": "image/png", "data": base64.b64encode(img).decode("ascii")}}
        for img in item.request.images
    ]
    parts.append({"text": item.request.user_envelope})
    return {
        "systemInstruction": {"parts": [{"text": item.request.system_prompt}]},
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": config,
    }


def _anthropic_model_name(model_id: str) -> str:
    """Extract the bare Anthropic model name from a ``model_id``."""
    if model_id.startswith("anthropic:"):
        bare = model_id[len("anthropic:") :]
        return bare.split("@", 1)[0]
    if "/models/" in model_id:
        return model_id.rsplit("/models/", 1)[1]
    return model_id


def _anthropic_request_params(item: "BatchItem", snapshot: ProviderSnapshot) -> dict[str, Any]:
    """Construct the Anthropic Message Batches ``params`` dict for ``item``."""
    gen_params = snapshot.generation_params
    user_content: str | list[dict[str, Any]] = (
        [
            *[
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": base64.b64encode(img).decode("ascii"),
                    },
                }
                for img in item.request.images
            ],
            {"type": "text", "text": item.request.user_envelope},
        ]
        if item.request.images
        else item.request.user_envelope
    )
    mechanism = gen_params.get("mechanism", _ANTHROPIC_MECHANISM)
    omit_output_config = mechanism == _ANTHROPIC_PROMPT_MECHANISM or (
        mechanism == _ANTHROPIC_FALLBACK_MECHANISM and _anthropic_exceeds_limits(item.request.json_schema)
    )
    params: dict[str, Any] = {
        "model": _anthropic_model_name(snapshot.model_id),
        "max_tokens": item.request.max_output_tokens,
        "system": _anthropic_prompt_system(item.request) if omit_output_config else item.request.system_prompt,
        "messages": [{"role": "user", "content": user_content}],
    }
    if not omit_output_config:
        params["output_config"] = {
            "format": {
                "type": _ANTHROPIC_MECHANISM,
                "schema": dict(item.request.json_schema),
            }
        }
    if "temperature" in gen_params and gen_params["temperature"] is not None:
        params["temperature"] = gen_params["temperature"]
    return params


@dataclass(frozen=True, slots=True)
class BatchItem:
    """One uncached ``(unit, model)`` request prepared for offline cloud batch execution (#464).

    Attributes:
        custom_id: The 64-character SHA-256 request key used as the batch line correlate id.
        key: The cache/request key (identical to ``custom_id``).
        unit_id: The packed unit's id.
        model_id: The target provider's ``model_id``.
        provider_kind: The target provider's ``provider_kind``.
        request: The rendered :class:`~datagrunt.rationalization.wire.ModelRequest`.
    """

    custom_id: str
    key: str
    unit_id: str
    model_id: str
    provider_kind: str
    request: ModelRequest


@dataclass(frozen=True, slots=True)
class BatchPlan:
    """Prepared offline batch plan returned by :func:`prepare_batch` (#464).

    Holds the uncached :attr:`items` to execute offline, any :attr:`cached_hits` already found in ``cache_dir``,
    and the validated pipeline state required by :func:`ingest_batch`.
    """

    items: tuple[BatchItem, ...]
    cached_hits: Mapping[str, ProviderResponse]
    schema_version: str
    min_families: int
    must_agree: tuple[str, ...]
    expect_non_empty: bool
    strict: bool
    _target_work: _TargetWork = field(repr=False)
    _snapshots: tuple[ProviderSnapshot, ...] = field(repr=False)
    _prepared: PreparedSchema = field(repr=False)
    _file_cache: FileCache | None = field(repr=False)
    _slots: Mapping[tuple[str, str], str] = field(repr=False)
    _cache_corrupt: int = field(default=0, repr=False)
    fold_across_units: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "cached_hits", MappingProxyType(dict(self.cached_hits)))
        object.__setattr__(self, "_slots", MappingProxyType(dict(self._slots)))

    def items_for(
        self,
        model_id: str | None = None,
        *,
        provider_kind: str | None = None,
    ) -> tuple[BatchItem, ...]:
        """Return the subset of :attr:`items` matching ``model_id`` and/or ``provider_kind``."""
        return tuple(
            item
            for item in self.items
            if (model_id is None or item.model_id == model_id)
            and (provider_kind is None or item.provider_kind == provider_kind)
        )

    def to_vertex_jsonl(
        self,
        path: str | PathLike[str],
        *,
        model_id: str | None = None,
    ) -> int:
        """Write Vertex AI (``generateContent``) batch prediction JSONL lines to ``path``.

        Each line is ``{"custom_id": item.custom_id, "request": {...}}``. Returns the number of lines written.
        """
        out_path = Path(path)
        snapshots_by_id = {snap.model_id: snap for snap in self._snapshots}
        selected = [
            item
            for item in self.items
            if (model_id is not None and item.model_id == model_id)
            or (model_id is None and item.provider_kind in _GEMINI_KINDS)
        ]
        lines: list[str] = []
        for item in selected:
            snap = snapshots_by_id[item.model_id]
            record = {
                "custom_id": item.custom_id,
                "request": _vertex_request_body(item, snap),
            }
            lines.append(canonical_json(record, subject="vertex batch item"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = ("\n".join(lines) + "\n") if lines else ""
        out_path.write_text(payload, encoding="utf-8")
        return len(lines)

    def to_anthropic_jsonl(
        self,
        path: str | PathLike[str],
        *,
        model_id: str | None = None,
    ) -> int:
        """Write Anthropic Message Batches JSONL lines to ``path``.

        Each line is ``{"custom_id": item.custom_id, "params": {...}}``. Returns the number of lines written.
        """
        out_path = Path(path)
        snapshots_by_id = {snap.model_id: snap for snap in self._snapshots}
        selected = [
            item
            for item in self.items
            if (model_id is not None and item.model_id == model_id)
            or (model_id is None and item.provider_kind in _ANTHROPIC_KINDS)
        ]
        lines: list[str] = []
        for item in selected:
            snap = snapshots_by_id[item.model_id]
            record = {
                "custom_id": item.custom_id,
                "params": _anthropic_request_params(item, snap),
            }
            lines.append(canonical_json(record, subject="anthropic batch item"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = ("\n".join(lines) + "\n") if lines else ""
        out_path.write_text(payload, encoding="utf-8")
        return len(lines)


def prepare_batch(
    target: TargetInput | Sequence[TargetInput],
    *,
    schema: type[T],
    prompt: str | Path,
    models: Sequence[ModelInput],
    min_families: int = 2,
    schema_version: str = "1.0",
    key: str | Sequence[str] | None = None,
    must_agree: Sequence[str] = (),
    context: object = None,
    cache_dir: str | PathLike[str] | None = None,
    max_unit_chars: int = 120_000,
    max_requests: int = 1_000,
    max_output_tokens: int = 8_192,
    expect_non_empty: bool = False,
    strict: bool = False,
    coalesce_sections: bool = True,
    include_schema_in_prompt: bool = True,
    multimodal: bool = True,
    fold_across_units: bool = True,
    timeout_s: float = 60.0,
) -> BatchPlan:
    """Prepare all uncached ``(unit, model)`` requests for offline cloud batch execution (#464).

    Runs the same zero-I/O preflight, target resolution, unit packing, envelope construction, and persistent
    ``FileCache`` probing as :func:`~datagrunt.rationalization.api.rationalize`, returning a :class:`BatchPlan`
    without making any network calls.
    """
    limits = _check_scalar_params(
        max_concurrency=8,
        max_concurrency_per_provider=4,
        rpm_limit=None,
        timeout_s=timeout_s,
        max_retries=0,
        max_unit_chars=max_unit_chars,
        max_requests=max_requests,
        max_output_tokens=max_output_tokens,
        schema_version=schema_version,
        expect_non_empty=expect_non_empty,
        strict=strict,
        coalesce_sections=coalesce_sections,
        include_schema_in_prompt=include_schema_in_prompt,
        multimodal=multimodal,
        fold_across_units=fold_across_units,
        on_event=None,
        min_families=min_families,
    )
    prepared = prepare(schema, key=key)
    checked_must_agree = check_must_agree(prepared.spec, must_agree)
    prompt_text = _read_prompt(prompt)
    _context_json(context)
    _check_schema(prepared.wrapper_json)
    snapshots = _prepare_snapshots(
        models,
        prepared,
        min_families=min_families,
        persistent_cache=cache_dir is not None,
    )
    file_cache = FileCache(cache_dir) if cache_dir is not None else None
    raw_targets = _normalize_targets(target)
    target_work = _build_target_work(
        raw_targets,
        max_unit_chars=max_unit_chars,
        coalesce_sections=coalesce_sections,
        include_schema_in_prompt=include_schema_in_prompt,
        multimodal=multimodal,
        prompt_text=prompt_text,
        context=context,
        prepared=prepared,
        snapshots=snapshots,
        schema_version=schema_version,
        file_cache=file_cache,
        limits=limits,
    )
    slots, distinct_requests = _plan(target_work.work, snapshots, limits, schema_version)
    cached_hits: dict[str, ProviderResponse] = {}
    items: list[BatchItem] = []
    corrupt_count = 0
    schemas_by_model = {snap.model_id: snap.provider_schema for snap in snapshots}

    for req in distinct_requests:
        if file_cache is not None:
            lookup = file_cache.get(req.key)
            if lookup.response is not None:
                cached_hits[req.key] = lookup.response
                continue
            if lookup.corrupt:
                corrupt_count += 1
        system_prompt, user_envelope = _draw_envelope_messages(req.envelope)
        model_req = ModelRequest(
            unit_id=req.unit.unit_id,
            model_id=req.provider.model_id,
            system_prompt=system_prompt,
            user_envelope=user_envelope,
            json_schema=canonical_copy(schemas_by_model[req.provider.model_id], subject="provider_schema"),
            max_output_tokens=limits.max_output_tokens,
            timeout_s=limits.timeout_s,
            images=req.envelope.images,
        )
        items.append(
            BatchItem(
                custom_id=req.key,
                key=req.key,
                unit_id=req.unit.unit_id,
                model_id=req.provider.model_id,
                provider_kind=req.provider.provider_kind,
                request=model_req,
            )
        )

    return BatchPlan(
        items=tuple(items),
        cached_hits=cached_hits,
        schema_version=schema_version,
        min_families=min_families,
        must_agree=checked_must_agree,
        expect_non_empty=expect_non_empty,
        strict=strict,
        _target_work=target_work,
        _snapshots=snapshots,
        _prepared=prepared,
        _file_cache=file_cache,
        _slots=slots,
        _cache_corrupt=corrupt_count,
        fold_across_units=fold_across_units,
    )


def _parse_vertex_batch_body(body: Mapping[str, Any]) -> ProviderResponse:
    """Convert a Vertex AI / Gemini ``generateContent`` response object into a :class:`ProviderResponse`."""
    finish = _gemini_finish(body)
    text = _gemini_candidate_text(body)
    inputs, outputs = _gemini_usage(body.get("usageMetadata"))
    raw_text = canonical_json(dict(body), subject="vertex batch response") if text is None else text
    content = None if text is None or finish == "refused" else parse_json_object(text)
    return _build_provider_response(
        content=content,
        raw_text=raw_text,
        finish=finish,
        input_tokens=inputs,
        output_tokens=outputs,
    )


def _parse_anthropic_batch_message(message: Mapping[str, Any]) -> ProviderResponse:
    """Convert an Anthropic Messages response object into a :class:`ProviderResponse`."""
    usage = message.get("usage")
    output_tokens = usage.get("output_tokens") if isinstance(usage, Mapping) else None
    stop_reason = message.get("stop_reason")
    finish = _ANTHROPIC_FINISH_BY_STOP.get(stop_reason) if isinstance(stop_reason, str) else None
    text = _anthropic_answer_text(message)
    raw_text = canonical_json(dict(message), subject="anthropic batch message") if text is None else text
    cleaned = _anthropic_strip_fence(text) if text is not None else None
    content = parse_json_object(cleaned) if cleaned is not None and finish is not None else None
    return _build_provider_response(
        content=content,
        raw_text=raw_text,
        finish=finish or "complete",
        input_tokens=_anthropic_input_tokens(usage),
        output_tokens=output_tokens,
    )


def _coerce_provider_response(entry: object) -> tuple[ProviderResponse | None, str | None]:
    """Convert a mapping or :class:`ProviderResponse` into ``(response, error_reason)``."""
    if isinstance(entry, ProviderResponse):
        return entry, None
    if not isinstance(entry, Mapping):
        raise TypeError(f"batch response entry must be a ProviderResponse or Mapping, got {type(entry).__name__}")

    # 1. Anthropic Message Batches line: {"custom_id": "...", "result": {"type": "succeeded" | ..., ...}}
    if isinstance(entry.get("result"), Mapping):
        result_obj = cast(Mapping[str, Any], entry["result"])
        res_type = result_obj.get("type")
        if res_type == "succeeded" and isinstance(result_obj.get("message"), Mapping):
            return _parse_anthropic_batch_message(cast(Mapping[str, Any], result_obj["message"])), None
        return None, f"anthropic batch result type={res_type!r}"

    # 2. Vertex AI batch prediction line: {"custom_id": "...", "response": {"candidates": ...}, "status": ...}
    if "status" in entry and entry["status"] and not isinstance(entry.get("response"), Mapping):
        return None, f"vertex batch error status: {bounded_detail(str(entry['status']))}"
    if isinstance(entry.get("response"), Mapping):
        return _parse_vertex_batch_body(cast(Mapping[str, Any], entry["response"])), None

    # 3. Direct Gemini / Vertex response dict: {"candidates": [...]}
    if "candidates" in entry:
        return _parse_vertex_batch_body(entry), None

    # 4. Direct Anthropic message dict: {"content": [...], "stop_reason": ...}
    if isinstance(entry.get("content"), list) and "stop_reason" in entry:
        return _parse_anthropic_batch_message(entry), None

    # 5. Serialized ProviderResponse dict: {"content": {...}, "raw_text": "...", "finish": "..."}
    if "raw_text" in entry and "finish" in entry:
        content_val = entry.get("content")
        parsed_content = None if content_val is None else canonical_copy(dict(content_val), subject="content")
        return (
            ProviderResponse(
                content=parsed_content,
                raw_text=str(entry["raw_text"]),
                finish=cast(FinishReason, entry["finish"]),
                input_tokens=cast(int | None, entry.get("input_tokens")),
                output_tokens=cast(int | None, entry.get("output_tokens")),
            ),
            None,
        )

    # 6. Plain record wrapper dict: {"records": [...]}
    if "records" in entry:
        copied: dict[str, Any] = canonical_copy(dict(entry), subject="batch response mapping")
        return (
            ProviderResponse(
                content=copied,
                raw_text=canonical_json(copied, subject="batch response mapping"),
                finish="complete",
            ),
            None,
        )

    raise RationalizationError("unrecognized batch response mapping structure")


def _load_jsonl_file(path: Path, out: dict[Any, object]) -> None:
    """Read a JSONL batch output file and populate ``out`` keyed by ``custom_id`` (or ``key``)."""
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RationalizationError(f"batch response file could not be read: {type(exc).__name__}") from exc
    for line_no, raw_line in enumerate(raw_lines, start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        try:
            parsed = strict_loads(stripped)
        except ValueError as exc:
            raise RationalizationError(f"invalid JSON on line {line_no} of {path.name}") from exc
        if not isinstance(parsed, Mapping):
            raise RationalizationError(f"line {line_no} of {path.name} must be a JSON object")
        cid = parsed.get("custom_id") or parsed.get("key") or parsed.get("id")
        if not isinstance(cid, str) or not cid.strip():
            raise RationalizationError(f"line {line_no} of {path.name} is missing a non-blank 'custom_id'")
        out[cid] = parsed


def _normalize_batch_responses(responses: BatchResponseInput) -> dict[Any, object]:
    """Normalize ``responses`` from a mapping, a JSONL path, or a sequence of JSONL paths."""
    if isinstance(responses, Mapping):
        return dict(responses)
    if isinstance(responses, (str, PathLike)):
        loaded: dict[Any, object] = {}
        _load_jsonl_file(Path(responses), loaded)
        return loaded
    if isinstance(responses, Sequence) and not isinstance(responses, (bytes, bytearray)):
        loaded = {}
        for entry in responses:
            if not isinstance(entry, (str, PathLike)):
                raise TypeError(f"each batch response path must be a str or PathLike, got {type(entry).__name__}")
            _load_jsonl_file(Path(entry), loaded)
        return loaded
    raise TypeError(
        "responses must be a Mapping, a JSONL path (str or PathLike), or a Sequence of JSONL paths, "
        f"got {type(responses).__name__}"
    )


def ingest_batch(
    plan: BatchPlan,
    responses: BatchResponseInput,
) -> RationalizedResult[Any]:
    """Ingest completed offline batch responses for ``plan`` and assemble :class:`RationalizedResult` (#464).

    Args:
        plan: The :class:`BatchPlan` returned by :func:`prepare_batch`.
        responses: A mapping keyed by ``custom_id`` (or ``(unit_id, model_id)``), a path to a completed Vertex AI
            or Anthropic batch JSONL file, or a sequence of JSONL file paths.

    Returns:
        A :class:`~datagrunt.rationalization.result.RationalizedResult` identical to what :func:`rationalize`
        produces.
    """
    if not isinstance(plan, BatchPlan):
        raise TypeError(f"plan must be a BatchPlan, got {type(plan).__name__}")
    raw_map = _normalize_batch_responses(responses)

    responses_by_key: dict[str, ProviderResponse] = dict(plan.cached_hits)
    failed_by_key: dict[str, tuple[int | None, str]] = {}
    usage_by_model: dict[str, tuple[int, int, int, int, float]] = {
        snap.model_id: (0, 0, 0, 0, 0.0) for snap in plan._snapshots
    }
    write_failures = 0
    envelopes_by_unit = plan._target_work.envelopes_by_id

    for item in plan.items:
        raw_entry = raw_map.get(item.custom_id)
        if raw_entry is None:
            raw_entry = raw_map.get((item.unit_id, item.model_id))
        if raw_entry is None:
            reason = "missing from batch responses"
            if plan.strict:
                raise RationalizationError(f"provider {item.model_id} failed unit {item.unit_id} (strict): {reason}")
            failed_by_key[item.key] = (None, reason)
            continue

        parsed_resp, err_reason = _coerce_provider_response(raw_entry)
        if parsed_resp is None:
            reason = err_reason or "batch request failed"
            if plan.strict:
                raise RationalizationError(f"provider {item.model_id} failed unit {item.unit_id} (strict): {reason}")
            reqs, retries, in_tok, out_tok, lat = usage_by_model[item.model_id]
            usage_by_model[item.model_id] = (reqs + 1, retries, in_tok, out_tok, lat)
            failed_by_key[item.key] = (None, bounded_detail(reason))
            continue

        responses_by_key[item.key] = parsed_resp
        reqs, retries, in_tok, out_tok, lat = usage_by_model[item.model_id]
        usage_by_model[item.model_id] = (
            reqs + 1,
            retries,
            in_tok + (parsed_resp.input_tokens or 0),
            out_tok + (parsed_resp.output_tokens or 0),
            lat,
        )
        if plan._file_cache is not None:
            env = envelopes_by_unit[item.unit_id]
            unit = plan._target_work.units_by_id[item.unit_id]
            meta = CacheMeta(
                model_id=item.model_id,
                unit_id=item.unit_id,
                doc_id=unit.doc_id,
                schema_name=plan._prepared.spec.model.__name__,
                schema_version=plan.schema_version,
                schema_sha256=plan._prepared.sha256,
                prompt_sha256=sha256_hex(env.prompt),
                latency_ms=0.0,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            failures = _file_put(plan._file_cache, item.key, parsed_resp, meta)
            write_failures += len(failures)

    slot_responses: dict[tuple[str, str], ProviderResponse] = {}
    failed_units: list[FailedUnit] = []
    for (unit_id, model_id), key in plan._slots.items():
        if key in responses_by_key:
            slot_responses[(unit_id, model_id)] = responses_by_key[key]
        else:
            status_code, reason = failed_by_key.get(key, (None, "missing from batch responses"))
            attempts = 1 if (unit_id, model_id) in raw_map or key in raw_map else 0
            failed_units.append(
                FailedUnit(
                    unit_id=unit_id,
                    model_id=model_id,
                    status_code=status_code,
                    error_message=bounded_detail(reason),
                    attempts=attempts,
                )
            )

    usage = {
        snap.model_id: ModelUsage(
            requests=usage_by_model[snap.model_id][0],
            retries=usage_by_model[snap.model_id][1],
            input_tokens=usage_by_model[snap.model_id][2],
            output_tokens=usage_by_model[snap.model_id][3],
            latency_ms=usage_by_model[snap.model_id][4],
        )
        for snap in plan._snapshots
    }
    models_meta = tuple((snap.model_id, snap.model_family, snap.provider_kind) for snap in plan._snapshots)
    outcome = RunOutcome(
        responses=slot_responses,
        failed_units=tuple(failed_units),
        cache_hits=len(plan.cached_hits),
        cache_misses=len(plan.items),
        cache_corrupt=plan._cache_corrupt,
        cache_write_failures=write_failures,
        usage=usage,
        models=models_meta,
    )
    return _assemble_pipeline_result(
        plan._target_work,
        outcome,
        plan._prepared,
        min_families=plan.min_families,
        must_agree=plan.must_agree,
        expect_non_empty=plan.expect_non_empty,
        fold_across_units=plan.fold_across_units,
    )
