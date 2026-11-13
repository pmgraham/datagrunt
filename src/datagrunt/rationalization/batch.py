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

import stat
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
    _close_ephemeral,
    _normalize_targets,
    _prepare_snapshots_with_closers,
    _read_prompt,
    _TargetWork,
)
from datagrunt.rationalization.cache import CacheMeta, FileCache
from datagrunt.rationalization.consensus import check_must_agree
from datagrunt.rationalization.envelope import Envelope, _check_schema, _context_json, new_nonce
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.anthropic import _FINISH_BY_STOP_REASON as _ANTHROPIC_FINISH_BY_STOP
from datagrunt.rationalization.providers.anthropic import FALLBACK_MECHANISM as _ANTHROPIC_FALLBACK_MECHANISM
from datagrunt.rationalization.providers.anthropic import MECHANISM as _ANTHROPIC_MECHANISM
from datagrunt.rationalization.providers.anthropic import PROMPT_MECHANISM as _ANTHROPIC_PROMPT_MECHANISM
from datagrunt.rationalization.providers.anthropic import _answer_text as _anthropic_answer_text
from datagrunt.rationalization.providers.anthropic import _exceeds_limits as _anthropic_exceeds_limits
from datagrunt.rationalization.providers.anthropic import _input_tokens as _anthropic_input_tokens
from datagrunt.rationalization.providers.anthropic import _prompt_system as _anthropic_prompt_system
from datagrunt.rationalization.providers.anthropic import _strip_json_fence as _anthropic_strip_fence
from datagrunt.rationalization.providers.base import ImageB64Cache, bounded_detail, encode_images_b64
from datagrunt.rationalization.providers.gemini import _candidate_text as _gemini_candidate_text
from datagrunt.rationalization.providers.gemini import _finish as _gemini_finish
from datagrunt.rationalization.providers.gemini import _usage as _gemini_usage
from datagrunt.rationalization.providers.responses import parse_json_object
from datagrunt.rationalization.providers.responses import response as _build_provider_response
from datagrunt.rationalization.result import RationalizedResult
from datagrunt.rationalization.runner import (
    NONCE_DRAWS,
    FailedUnit,
    ModelUsage,
    ProviderSnapshot,
    RunLimits,
    RunOutcome,
)
from datagrunt.rationalization.schema import PreparedSchema, prepare
from datagrunt.rationalization.wire import (
    FinishReason,
    ModelRequest,
    ProviderResponse,
    _trusted_model_request,
)

T = TypeVar("T", bound=BaseModel)
BatchResponseInput: TypeAlias = (
    Mapping[Any, ProviderResponse | Mapping[str, Any]] | str | PathLike[str] | Sequence[str | PathLike[str]]
)

BATCH_LINE_MAX_BYTES = 16 * 1024 * 1024
"""Maximum UTF-8 byte length of a single batch JSONL output line."""

BATCH_MAX_LINES = 1_000_000
"""Maximum physical lines read from a single batch JSONL output file."""

_GEMINI_KINDS = frozenset({"gemini", "google-cloud-gemini"})
_ANTHROPIC_KINDS = frozenset({"anthropic", "google-cloud-anthropic"})

__all__ = ["BatchItem", "BatchPlan", "ingest_batch", "prepare_batch"]


def _draw_envelope_messages(envelope: Envelope) -> tuple[str, str]:
    """Draw a fresh collision-free nonce and render ``(system_prompt, user_envelope)``."""
    for _ in range(NONCE_DRAWS):
        try:
            return envelope.render(new_nonce())
        except ValueError as exc:
            if "already holds this nonce" in str(exc):
                continue
            raise
    raise RationalizationError(f"failed to draw a collision-free nonce in {NONCE_DRAWS} attempts")


def _vertex_request_body(
    item: "BatchItem",
    snapshot: ProviderSnapshot,
    b64_cache: ImageB64Cache | None = None,
) -> tuple[dict[str, Any], ImageB64Cache | None]:
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
    parts: list[dict[str, Any]] = []
    if item.request.images:
        encoded, b64_cache = encode_images_b64(item.request.images, b64_cache)
        parts.extend({"inlineData": {"mimeType": "image/png", "data": data}} for data in encoded)
    parts.append({"text": item.request.user_envelope})
    body = {
        "systemInstruction": {"parts": [{"text": item.request.system_prompt}]},
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": config,
    }
    return body, b64_cache


def _anthropic_model_name(model_id: str) -> str:
    """Extract the bare Anthropic model name from a ``model_id``."""
    if model_id.startswith("anthropic:"):
        bare = model_id[len("anthropic:") :]
        return bare.split("@", 1)[0]
    if "/models/" in model_id:
        return model_id.rsplit("/models/", 1)[1]
    return model_id


def _anthropic_request_params(
    item: "BatchItem",
    snapshot: ProviderSnapshot,
    b64_cache: ImageB64Cache | None = None,
) -> tuple[dict[str, Any], ImageB64Cache | None]:
    """Construct the Anthropic Message Batches ``params`` dict for ``item``."""
    gen_params = snapshot.generation_params
    if item.request.images:
        encoded, b64_cache = encode_images_b64(item.request.images, b64_cache)
        user_content: str | list[dict[str, Any]] = [
            *(
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": data}}
                for data in encoded
            ),
            {"type": "text", "text": item.request.user_envelope},
        ]
    else:
        user_content = item.request.user_envelope
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
        params["output_config"] = {"format": {"type": _ANTHROPIC_MECHANISM, "schema": dict(item.request.json_schema)}}
    if "temperature" in gen_params and gen_params["temperature"] is not None:
        params["temperature"] = gen_params["temperature"]
    return params, b64_cache


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


def _matches_export(item: BatchItem, model_id: str | None, kinds: frozenset[str]) -> bool:
    """Whether ``item`` belongs in a provider-specific JSONL export."""
    if not isinstance(item, BatchItem):
        raise TypeError(f"item must be a BatchItem, got {type(item).__name__}")
    if model_id is not None and not isinstance(model_id, str):
        raise TypeError(f"model_id must be a str or None, got {type(model_id).__name__}")
    if model_id is not None:
        return item.model_id == model_id
    return item.provider_kind in kinds


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
    multimodal: bool = True

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
        """Stream Vertex AI (``generateContent``) batch prediction JSONL lines to ``path``."""
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        snapshots_by_id = {snap.model_id: snap for snap in self._snapshots}
        b64_cache: ImageB64Cache | None = None
        written = 0
        with out_path.open("w", encoding="utf-8") as handle:
            for item in self.items:
                if not _matches_export(item, model_id, _GEMINI_KINDS):
                    continue
                body, b64_cache = _vertex_request_body(item, snapshots_by_id[item.model_id], b64_cache)
                record = {"custom_id": item.custom_id, "request": body}
                handle.write(canonical_json(record, subject="vertex batch item"))
                handle.write("\n")
                written += 1
        return written

    def to_anthropic_jsonl(
        self,
        path: str | PathLike[str],
        *,
        model_id: str | None = None,
    ) -> int:
        """Stream Anthropic Message Batches JSONL lines to ``path``."""
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        snapshots_by_id = {snap.model_id: snap for snap in self._snapshots}
        b64_cache: ImageB64Cache | None = None
        written = 0
        with out_path.open("w", encoding="utf-8") as handle:
            for item in self.items:
                if not _matches_export(item, model_id, _ANTHROPIC_KINDS):
                    continue
                params, b64_cache = _anthropic_request_params(item, snapshots_by_id[item.model_id], b64_cache)
                record = {"custom_id": item.custom_id, "params": params}
                handle.write(canonical_json(record, subject="anthropic batch item"))
                handle.write("\n")
                written += 1
        return written


def _build_batch_items(
    target_work: _TargetWork,
    snapshots: Sequence[ProviderSnapshot],
    limits: RunLimits,
) -> tuple[dict[str, ProviderResponse], tuple[BatchItem, ...], int]:
    """Partition preflight requests into cached hits and rendered :class:`BatchItem` misses."""
    if not snapshots:
        raise ValueError("snapshots must not be empty")
    if not isinstance(limits, RunLimits):
        raise TypeError(f"limits must be a RunLimits, got {type(limits).__name__}")
    preflight = target_work.preflight
    cached_hits: dict[str, ProviderResponse] = {}
    items: list[BatchItem] = []
    corrupt_count = 0
    schemas_by_model = {snap.model_id: snap.provider_schema for snap in snapshots}
    for req in preflight.requests:
        lookup = preflight.file_lookups.get(req.key)
        if lookup is not None:
            if lookup.response is not None:
                cached_hits[req.key] = lookup.response
                continue
            if lookup.corrupt:
                corrupt_count += 1
        system_prompt, user_envelope = _draw_envelope_messages(req.envelope)
        model_req = _trusted_model_request(
            unit_id=req.unit.unit_id,
            model_id=req.provider.model_id,
            system_prompt=system_prompt,
            user_envelope=user_envelope,
            json_schema=schemas_by_model[req.provider.model_id],
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
    return cached_hits, tuple(items), corrupt_count


def _collect_batch_work(
    targets: tuple[TargetInput, ...],
    prepared: PreparedSchema,
    snapshots: tuple[ProviderSnapshot, ...],
    file_cache: FileCache | None,
    limits: RunLimits,
    prompt_ctx: tuple[str, object, str],
    u_opts: tuple[int, bool, bool, bool],
) -> tuple[_TargetWork, dict[str, ProviderResponse], tuple[BatchItem, ...], int]:
    """Build target work and partition batch items for validated snapshots."""
    if not snapshots:
        raise ValueError("snapshots must not be empty")
    if not isinstance(limits, RunLimits):
        raise TypeError(f"limits must be a RunLimits, got {type(limits).__name__}")
    prompt_text, context, schema_version = prompt_ctx
    max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal = u_opts
    target_work = _build_target_work(
        targets,
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
    cached_hits, items, corrupt_count = _build_batch_items(target_work, snapshots, limits)
    return target_work, cached_hits, items, corrupt_count


def _execute_prepare_batch(
    target: TargetInput | Sequence[TargetInput],
    pre: tuple[type[Any], str | Sequence[str] | None, Sequence[str], str | Path, object, Sequence[ModelInput], Any],
    limits: RunLimits,
    u_opts: tuple[int, bool, bool, bool],
    b_opts: tuple[bool, bool, int, str],
) -> BatchPlan:
    """Snapshot providers, build target work, and construct :class:`BatchPlan`, closing ephemeral ``str`` models."""
    if not isinstance(limits, RunLimits):
        raise TypeError(f"limits must be a RunLimits, got {type(limits).__name__}")
    if len(pre) != 7:
        raise ValueError("pre must be a 7-tuple of batch preflight inputs")
    schema, key, must_agree, prompt, context, models, cache_dir = pre
    fold_across_units, expect_non_empty, min_families, schema_version = b_opts
    prepared = prepare(schema, key=key)
    checked_must_agree, prompt_text = check_must_agree(prepared.spec, must_agree), _read_prompt(prompt)
    _context_json(context)
    _check_schema(prepared.wrapper_json)
    snapshots, closers = _prepare_snapshots_with_closers(
        models,
        prepared,
        min_families=min_families,
        persistent_cache=cache_dir is not None,
    )
    try:
        file_cache = FileCache(cache_dir) if cache_dir is not None else None
        targets, prompt_ctx = _normalize_targets(target), (prompt_text, context, schema_version)
        work = _collect_batch_work(targets, prepared, snapshots, file_cache, limits, prompt_ctx, u_opts)
    finally:
        _close_ephemeral(closers)
    target_work, cached_hits, items, corrupt_count = work
    return BatchPlan(
        items=items,
        cached_hits=cached_hits,
        schema_version=schema_version,
        min_families=min_families,
        must_agree=checked_must_agree,
        expect_non_empty=expect_non_empty,
        strict=limits.strict,
        _target_work=target_work,
        _snapshots=snapshots,
        _prepared=prepared,
        _file_cache=file_cache,
        _slots=target_work.preflight.slots,
        _cache_corrupt=corrupt_count,
        fold_across_units=fold_across_units,
        multimodal=u_opts[3],
    )


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
    """Prepare all uncached ``(unit, model)`` requests for offline cloud batch execution (#464)."""
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
    pre = (schema, key, must_agree, prompt, context, models, cache_dir)
    u_opts = (max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal)
    b_opts = (fold_across_units, expect_non_empty, min_families, schema_version)
    return _execute_prepare_batch(target, pre, limits, u_opts, b_opts)


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


def _coerce_serialized_or_wrapper(entry: Mapping[str, Any]) -> ProviderResponse:
    """Coerce a serialized :class:`ProviderResponse` dict or plain ``{"records": [...]}`` wrapper."""
    if not isinstance(entry, Mapping):
        raise TypeError(f"entry must be a Mapping, got {type(entry).__name__}")
    if "raw_text" in entry and "finish" in entry:
        content_val = entry.get("content")
        parsed_content = None if content_val is None else canonical_copy(dict(content_val), subject="content")
        return ProviderResponse(
            content=parsed_content,
            raw_text=str(entry["raw_text"]),
            finish=cast(FinishReason, entry["finish"]),
            input_tokens=cast(int | None, entry.get("input_tokens")),
            output_tokens=cast(int | None, entry.get("output_tokens")),
        )
    if "records" in entry:
        copied: dict[str, Any] = canonical_copy(dict(entry), subject="batch response mapping")
        return ProviderResponse(
            content=copied,
            raw_text=canonical_json(copied, subject="batch response mapping"),
            finish="complete",
        )
    raise RationalizationError("unrecognized batch response mapping structure")


def _coerce_provider_response(entry: object) -> tuple[ProviderResponse | None, str | None]:
    """Convert a mapping or :class:`ProviderResponse` into ``(response, error_reason)``."""
    if isinstance(entry, ProviderResponse):
        return entry, None
    if not isinstance(entry, Mapping):
        raise TypeError(f"batch response entry must be a ProviderResponse or Mapping, got {type(entry).__name__}")
    if isinstance(entry.get("result"), Mapping):
        result_obj = cast(Mapping[str, Any], entry["result"])
        res_type = result_obj.get("type")
        if res_type == "succeeded" and isinstance(result_obj.get("message"), Mapping):
            return _parse_anthropic_batch_message(cast(Mapping[str, Any], result_obj["message"])), None
        return None, f"anthropic batch result type={res_type!r}"
    if "status" in entry and entry["status"] and not isinstance(entry.get("response"), Mapping):
        return None, f"vertex batch error status: {bounded_detail(str(entry['status']))}"
    if isinstance(entry.get("response"), Mapping):
        return _parse_vertex_batch_body(cast(Mapping[str, Any], entry["response"])), None
    if "candidates" in entry:
        return _parse_vertex_batch_body(entry), None
    if isinstance(entry.get("content"), list) and "stop_reason" in entry:
        return _parse_anthropic_batch_message(entry), None
    return _coerce_serialized_or_wrapper(entry), None


def _parse_jsonl_line(
    raw_bytes: bytes,
    line_no: int,
    filename: str,
    out: dict[Any, object],
    seen_ids: set[str],
    wanted_ids: frozenset[str] | None,
) -> None:
    """Validate and decode one JSONL line, enforcing UTF-8, object shape, and unique ``custom_id``."""
    try:
        stripped = raw_bytes.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise RationalizationError(f"invalid UTF-8 on line {line_no} of {filename}") from exc
    if not stripped:
        return
    try:
        parsed = strict_loads(stripped)
    except (ValueError, RecursionError) as exc:
        raise RationalizationError(f"invalid JSON on line {line_no} of {filename}") from exc
    if not isinstance(parsed, Mapping):
        raise RationalizationError(f"line {line_no} of {filename} must be a JSON object")
    cid = parsed.get("custom_id") or parsed.get("key") or parsed.get("id")
    if not isinstance(cid, str) or not cid.strip():
        raise RationalizationError(f"line {line_no} of {filename} is missing a non-blank 'custom_id'")
    if cid in seen_ids or cid in out:
        raise RationalizationError(f"duplicate custom_id {cid!r} on line {line_no} of {filename}")
    seen_ids.add(cid)
    if wanted_ids is None or cid in wanted_ids:
        out[cid] = parsed


def _strip_line_ending(raw_bytes: bytes) -> bytes:
    """Strip at most one trailing LF and preceding CR from ``raw_bytes``."""
    if type(raw_bytes) is not bytes:
        raise TypeError(f"raw_bytes must be bytes, got {type(raw_bytes).__name__}")
    if len(raw_bytes) > BATCH_LINE_MAX_BYTES + 2:
        raise ValueError("raw_bytes exceeds bounded readline buffer")
    if not raw_bytes:
        return b""
    trimmed = raw_bytes[:-1] if raw_bytes.endswith(b"\n") else raw_bytes
    return trimmed[:-1] if trimmed.endswith(b"\r") else trimmed


def _read_bounded_jsonl(
    handle: Any,
    filename: str,
    out: dict[Any, object],
    seen: set[str],
    wanted_ids: frozenset[str] | None,
) -> None:
    """Read bounded binary JSONL lines from ``handle``, stripping CR/LF before the byte cap check."""
    if not filename:
        raise ValueError("filename must be non-empty")
    if not isinstance(out, dict) or not isinstance(seen, set):
        raise TypeError("out must be a dict and seen must be a set")
    for line_no in range(1, BATCH_MAX_LINES + 2):
        raw_bytes = handle.readline(BATCH_LINE_MAX_BYTES + 2)
        if not raw_bytes:
            return
        if line_no > BATCH_MAX_LINES:
            raise RationalizationError(f"{filename} exceeds {BATCH_MAX_LINES} lines")
        line_bytes = _strip_line_ending(raw_bytes)
        if len(line_bytes) > BATCH_LINE_MAX_BYTES:
            raise RationalizationError(f"line {line_no} of {filename} exceeds {BATCH_LINE_MAX_BYTES} bytes")
        _parse_jsonl_line(line_bytes, line_no, filename, out, seen, wanted_ids)


def _load_jsonl_file(
    path: Path,
    out: dict[Any, object],
    *,
    seen_ids: set[str] | None = None,
    wanted_ids: frozenset[str] | None = None,
) -> None:
    """Stream a JSONL batch output file line-by-line into ``out`` with bounded line size and count."""
    if not isinstance(path, Path):
        raise TypeError(f"path must be a Path, got {type(path).__name__}")
    if not isinstance(out, dict):
        raise TypeError(f"out must be a dict, got {type(out).__name__}")
    seen = seen_ids if seen_ids is not None else set()
    try:
        st = path.stat()
        if not stat.S_ISREG(st.st_mode):
            raise RationalizationError(f"batch response path {path.name} must be a regular file")
        with path.open("rb") as handle:
            _read_bounded_jsonl(handle, path.name, out, seen, wanted_ids)
    except OSError as exc:
        raise RationalizationError(f"batch response file could not be read: {type(exc).__name__}") from exc


def _normalize_batch_responses(
    responses: BatchResponseInput,
    wanted_ids: frozenset[str] | None = None,
) -> dict[Any, object]:
    """Normalize ``responses`` from a mapping, a JSONL path, or a sequence of JSONL paths."""
    if isinstance(responses, Mapping):
        return dict(responses)
    if isinstance(responses, (str, PathLike)):
        loaded: dict[Any, object] = {}
        _load_jsonl_file(Path(responses), loaded, seen_ids=set(), wanted_ids=wanted_ids)
        return loaded
    if isinstance(responses, Sequence) and not isinstance(responses, (bytes, bytearray)):
        loaded = {}
        seen_ids: set[str] = set()
        for entry in responses:
            if not isinstance(entry, (str, PathLike)):
                raise TypeError(f"each batch response path must be a str or PathLike, got {type(entry).__name__}")
            _load_jsonl_file(Path(entry), loaded, seen_ids=seen_ids, wanted_ids=wanted_ids)
        return loaded
    raise TypeError(
        "responses must be a Mapping, a JSONL path (str or PathLike), or a Sequence of JSONL paths, "
        f"got {type(responses).__name__}"
    )


def _put_single_safe(file_cache: FileCache, key: str, resp: ProviderResponse, meta: CacheMeta) -> bool:
    """Write one fallback entry to ``file_cache``, returning ``False`` on >8 MiB or ``RationalizationError``."""
    if not isinstance(file_cache, FileCache):
        raise TypeError(f"file_cache must be a FileCache, got {type(file_cache).__name__}")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty str")
    try:
        return file_cache.put(key, resp, meta)
    except RationalizationError:
        return False


def _flush_batch_cache(
    file_cache: FileCache,
    pending_puts: Sequence[tuple[str, ProviderResponse, CacheMeta]],
) -> int:
    """Persist ``pending_puts`` via shard-batched ``put_many``, falling back to per-entry ``put`` on error."""
    if not isinstance(file_cache, FileCache):
        raise TypeError(f"file_cache must be a FileCache, got {type(file_cache).__name__}")
    if not isinstance(pending_puts, Sequence):
        raise TypeError(f"pending_puts must be a Sequence, got {type(pending_puts).__name__}")
    if not pending_puts:
        return 0
    try:
        flags = file_cache.put_many(pending_puts)
        return sum(1 for ok in flags if not ok)
    except RationalizationError:
        return sum(1 for key, resp, meta in pending_puts if not _put_single_safe(file_cache, key, resp, meta))


def _build_batch_meta(
    plan: BatchPlan,
    item: BatchItem,
    prompt_hashes: Mapping[str, str],
    created_at: str,
) -> CacheMeta:
    """Construct audit :class:`CacheMeta` for a freshly ingested batch response."""
    if not isinstance(plan, BatchPlan) or not isinstance(item, BatchItem):
        raise TypeError("plan must be a BatchPlan and item a BatchItem")
    if not created_at:
        raise ValueError("created_at must be non-empty")
    env = plan._target_work.envelopes_by_id[item.unit_id]
    unit = plan._target_work.units_by_id[item.unit_id]
    return CacheMeta(
        model_id=item.model_id,
        unit_id=item.unit_id,
        doc_id=unit.doc_id,
        schema_name=plan._prepared.spec.model.__name__,
        schema_version=plan.schema_version,
        schema_sha256=plan._prepared.sha256,
        prompt_sha256=prompt_hashes[env.prompt],
        latency_ms=0.0,
        created_at=created_at,
    )


def _ingest_plan_items(
    plan: BatchPlan,
    raw_map: Mapping[Any, object],
) -> tuple[dict[str, ProviderResponse], dict[str, tuple[int | None, str]], dict[str, list[float]], int]:
    """Coerce all ``plan.items`` from ``raw_map``, tally model usage, and batch-write to ``FileCache``."""
    if not isinstance(plan, BatchPlan):
        raise TypeError(f"plan must be a BatchPlan, got {type(plan).__name__}")
    if not isinstance(raw_map, Mapping):
        raise TypeError(f"raw_map must be a Mapping, got {type(raw_map).__name__}")
    responses_by_key: dict[str, ProviderResponse] = dict(plan.cached_hits)
    failed_by_key: dict[str, tuple[int | None, str]] = {}
    usage_by_model: dict[str, list[float]] = {snap.model_id: [0, 0, 0, 0, 0.0] for snap in plan._snapshots}
    pending_puts: list[tuple[str, ProviderResponse, CacheMeta]] = []
    prompt_hashes = {env.prompt: sha256_hex(env.prompt) for env in plan._target_work.envelopes_by_id.values()}
    now_iso = datetime.now(timezone.utc).isoformat()

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
        counts = usage_by_model[item.model_id]
        if parsed_resp is None:
            reason = err_reason or "batch request failed"
            if plan.strict:
                raise RationalizationError(f"provider {item.model_id} failed unit {item.unit_id} (strict): {reason}")
            counts[0] += 1
            failed_by_key[item.key] = (None, bounded_detail(reason))
            continue
        responses_by_key[item.key] = parsed_resp
        counts[0] += 1
        counts[2] += parsed_resp.input_tokens or 0
        counts[3] += parsed_resp.output_tokens or 0
        if plan._file_cache is not None:
            pending_puts.append((item.key, parsed_resp, _build_batch_meta(plan, item, prompt_hashes, now_iso)))

    write_failures = _flush_batch_cache(plan._file_cache, pending_puts) if plan._file_cache is not None else 0
    return responses_by_key, failed_by_key, usage_by_model, write_failures


def _build_batch_usage(
    snapshots: Sequence[ProviderSnapshot],
    usage_by_model: Mapping[str, list[float]],
) -> dict[str, ModelUsage]:
    """Convert raw per-model tally lists into :class:`ModelUsage` instances."""
    if not snapshots:
        raise ValueError("snapshots must not be empty")
    if not isinstance(usage_by_model, Mapping):
        raise TypeError(f"usage_by_model must be a Mapping, got {type(usage_by_model).__name__}")
    return {
        snap.model_id: ModelUsage(
            requests=int(usage_by_model[snap.model_id][0]),
            retries=int(usage_by_model[snap.model_id][1]),
            input_tokens=int(usage_by_model[snap.model_id][2]),
            output_tokens=int(usage_by_model[snap.model_id][3]),
            latency_ms=float(usage_by_model[snap.model_id][4]),
        )
        for snap in snapshots
    }


def ingest_batch(
    plan: BatchPlan,
    responses: BatchResponseInput,
) -> RationalizedResult[Any]:
    """Ingest completed offline batch responses for ``plan`` and assemble :class:`RationalizedResult` (#464)."""
    if not isinstance(plan, BatchPlan):
        raise TypeError(f"plan must be a BatchPlan, got {type(plan).__name__}")
    wanted = frozenset(item.custom_id for item in plan.items)
    raw_map = _normalize_batch_responses(responses, wanted_ids=wanted)
    responses_by_key, failed_by_key, usage_by_model, write_failures = _ingest_plan_items(plan, raw_map)

    slot_responses: dict[tuple[str, str], ProviderResponse] = {}
    failed_units: list[FailedUnit] = []
    for (unit_id, model_id), key in plan._slots.items():
        if key in responses_by_key:
            slot_responses[(unit_id, model_id)] = responses_by_key[key]
        else:
            status_code, reason = failed_by_key.get(key, (None, "missing from batch responses"))
            attempts = 1 if (unit_id, model_id) in raw_map or key in raw_map else 0
            failed_units.append(FailedUnit(unit_id, model_id, status_code, bounded_detail(reason), attempts))
    outcome = RunOutcome(
        responses=slot_responses,
        failed_units=tuple(failed_units),
        cache_hits=len(plan.cached_hits),
        cache_misses=len(plan.items),
        cache_corrupt=plan._cache_corrupt,
        cache_write_failures=write_failures,
        usage=_build_batch_usage(plan._snapshots, usage_by_model),
        models=tuple((snap.model_id, snap.model_family, snap.provider_kind) for snap in plan._snapshots),
    )
    return _assemble_pipeline_result(
        plan._target_work,
        outcome,
        plan._prepared,
        min_families=plan.min_families,
        must_agree=plan.must_agree,
        expect_non_empty=plan.expect_non_empty,
        fold_across_units=plan.fold_across_units,
        multimodal=plan.multimodal,
    )
