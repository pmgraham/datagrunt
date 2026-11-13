"""Public entry point ``rationalize()``: zero-I/O preflight, target resolution and pipeline wiring (plan §4.11).

:func:`rationalize` orchestrates the 5.1 extraction pipeline over one or more :class:`~datagrunt.pdf.PDFDocument`
instances (or PDF / bundle paths resolved through :class:`~datagrunt.pdf.PDFReader`):

1. **Zero-I/O preflight** (before opening any PDF or making any network call):
   - Validate all numeric, boolean, string and hook parameters (:class:`~datagrunt.rationalization.runner.RunLimits`,
     ``max_unit_chars``, ``schema_version``, ``expect_non_empty``, ``on_event``, ``min_families``).
   - Prepare the caller's Pydantic schema (:func:`~datagrunt.rationalization.schema.prepare`) and validate
     ``must_agree`` (:func:`~datagrunt.rationalization.consensus.check_must_agree`).
   - Read and validate ``prompt`` (if a :class:`~pathlib.Path`, require a regular file ``<= PROMPT_MAX_BYTES`` of
     UTF-8) and validate ``context`` and the wrapper schema JSON.
   - Resolve ``models`` into :class:`~datagrunt.rationalization.providers.base.ModelProvider` instances, refuse bare
     callables when ``cache_dir`` is set, snapshot providers
     (:func:`~datagrunt.rationalization.runner.snapshot_providers`), and verify ``1 <= min_families <= len(snapshots)``
     and that distinct known model families among ``snapshots`` satisfy ``min_families``.
   - Open :class:`~datagrunt.rationalization.cache.FileCache` (when ``cache_dir`` is given) and
     :class:`~datagrunt.rationalization.cache.MemoryCache`.
2. **Target resolution and cumulative request preflight**:
   - Accept a ``PDFDocument``, ``str``, ``PathLike``, or a sequence of ``1..MAX_TARGETS`` targets; resolve paths via
     ``datagrunt.pdf._extra.load("PDFReader")(path).document``.
   - Refuse duplicate ``doc_id`` values across targets with :class:`~datagrunt.rationalization.RationalizationError`.
   - Build each document's units (:func:`~datagrunt.rationalization.units.build_units`) and envelopes
     (:func:`~datagrunt.rationalization.envelope.build_envelopes`), stopping immediately if cumulative distinct cache
     misses exceed ``max_requests``. What has nothing to send (an error page, an image-only page, an all-hidden or
     no-text group) becomes a :class:`~datagrunt.rationalization.units.SkippedUnit` of the result.
3. **Pipeline execution**:
   - Run requests (:func:`~datagrunt.rationalization.runner.run_requests`), form cross-family consensus
     (:func:`~datagrunt.rationalization.consensus.build_consensus`), ground evidence against cited elements
     (:func:`~datagrunt.rationalization.grounding.ground`), fold across units per document
     (:func:`~datagrunt.rationalization.folding.fold_documents`), and assemble
     :class:`~datagrunt.rationalization.result.RationalizedResult`.

Bounds: targets at most :data:`MAX_TARGETS`; all downstream stages bounded by :class:`RunLimits` and schema depth.
The module imports only the standard library, pydantic, the PDF model and its sibling modules: nothing from
``datagrunt.core``, and no import inside a function (invariant 9).
"""

import contextlib
import hashlib
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Any, TypeAlias, TypeVar, cast

from pydantic import BaseModel

from datagrunt.pdf._bundle_load import MANIFEST, load_bundle
from datagrunt.pdf._extra import load as _pdf_load
from datagrunt.pdf.corpus import CORPUS_MANIFEST, CorpusManifest, load_corpus
from datagrunt.pdf.elements import PDFDocument
from datagrunt.pdf.exceptions import (
    BundleCorruptError,
    DatagruntPDFError,
    PDFCorruptError,
    PDFEncryptedError,
    PDFSourceChangedError,
)
from datagrunt.rationalization.cache import (
    CacheLookup,
    FileCache,
    MemoryCache,
    _envelope_key_prefix,
    _fast_request_key,
    _provider_key_suffix,
)
from datagrunt.rationalization.consensus import ConsensusResult, build_consensus, check_must_agree
from datagrunt.rationalization.envelope import (
    PROMPT_MAX_BYTES,
    Envelope,
    _check_prompt,
    _check_schema,
    _context_json,
    build_envelopes,
    build_envelopes_isolated,
    element_escape_counts,
    render_element,
)
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.folding import _CLEAN_STATUSES, FoldingResult, GroundedRecord, fold_documents
from datagrunt.rationalization.grounding import GroundingResult, ground
from datagrunt.rationalization.providers.anthropic import AnthropicProvider
from datagrunt.rationalization.providers.base import (
    UNKNOWN_FAMILY,
    ModelProvider,
    identity_problem,
    parse_model_string,
)
from datagrunt.rationalization.providers.callable import as_provider
from datagrunt.rationalization.providers.gemini import GeminiProvider
from datagrunt.rationalization.providers.openai_compatible import OpenAICompatibleProvider
from datagrunt.rationalization.result import RationalizedResult, RationalizeTelemetry
from datagrunt.rationalization.runner import (
    EventHook,
    ProviderSnapshot,
    RunLimits,
    RunOutcome,
    _PreflightPlan,
    _Request,
    run_requests,
    snapshot_providers,
)
from datagrunt.rationalization.schema import PreparedSchema, prepare
from datagrunt.rationalization.units import SkippedUnit, Unit, build_units
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

MAX_TARGETS = 10_000
"""The most targets a single :func:`rationalize` call accepts (plan §4.11)."""

T = TypeVar("T", bound=BaseModel)
TargetInput: TypeAlias = PDFDocument | CorpusManifest | str | PathLike[str]
ModelInput: TypeAlias = str | ModelProvider | Callable[[ModelRequest], ProviderResponse]
_TARGET_TYPES = (PDFDocument, CorpusManifest, str, PathLike)

__all__ = ["MAX_TARGETS", "rationalize"]


# ---------------------------------------------------------------------------------------------------------------
# Step 1-4: Zero-I/O preflight helpers
# ---------------------------------------------------------------------------------------------------------------


def _require_bool(name: str, value: object) -> None:
    if type(value) is not bool:
        raise TypeError(f"{name} must be a bool, got {type(value).__name__}")


def _check_bool_flags(flags: Sequence[tuple[str, object]]) -> None:
    """Validate that every ``(name, value)`` pair in ``flags`` is a strict ``bool``."""
    if not flags:
        raise ValueError("flags must not be empty")
    for flag_name, flag_val in flags:
        _require_bool(flag_name, flag_val)


def _check_extra_scalars(
    max_unit_chars: object,
    schema_version: object,
    on_event: object,
    min_families: object,
    flags: Sequence[tuple[str, object]],
) -> None:
    """Validate non-RunLimits scalar, string, hook, and boolean flags."""
    if type(max_unit_chars) is not int:
        raise TypeError(f"max_unit_chars must be an int, got {type(max_unit_chars).__name__}")
    if max_unit_chars < 1:
        raise ValueError(f"max_unit_chars must be >= 1, got {max_unit_chars}")
    if (version_problem := identity_problem(schema_version)) is not None:
        raise (TypeError if not isinstance(schema_version, str) else ValueError)(f"schema_version {version_problem}")
    _check_bool_flags(flags)
    if on_event is not None and not callable(on_event):
        raise TypeError(f"on_event must be callable or None, got {type(on_event).__name__}")
    if type(min_families) is not int:
        raise TypeError(f"min_families must be an int, got {type(min_families).__name__}")
    if min_families < 1:
        raise RationalizationError(f"min_families must be >= 1, got {min_families}")


def _check_scalar_params(
    *,
    max_concurrency: int,
    max_concurrency_per_provider: int,
    rpm_limit: int | None,
    timeout_s: float,
    max_retries: int,
    max_unit_chars: int,
    max_requests: int,
    max_output_tokens: int,
    schema_version: str,
    expect_non_empty: bool,
    strict: bool,
    coalesce_sections: bool,
    include_schema_in_prompt: bool = True,
    multimodal: bool = True,
    fold_across_units: bool = True,
    on_event: EventHook | None,
    min_families: int,
) -> RunLimits:
    """Validate all scalar, boolean, string and hook parameters before any schema, file or target work."""
    limits = RunLimits(
        max_concurrency=max_concurrency,
        max_concurrency_per_provider=max_concurrency_per_provider,
        rpm_limit=rpm_limit,
        timeout_s=timeout_s,
        max_retries=max_retries,
        max_output_tokens=max_output_tokens,
        max_requests=max_requests,
        strict=strict,
    )
    flags = (
        ("expect_non_empty", expect_non_empty),
        ("coalesce_sections", coalesce_sections),
        ("include_schema_in_prompt", include_schema_in_prompt),
        ("multimodal", multimodal),
        ("fold_across_units", fold_across_units),
    )
    _check_extra_scalars(max_unit_chars, schema_version, on_event, min_families, flags)
    return limits


def _read_prompt(prompt: object) -> str:
    """Resolve ``prompt`` from a ``str`` or regular UTF-8 :class:`~pathlib.Path` within :data:`PROMPT_MAX_BYTES`."""
    if isinstance(prompt, str):
        return _check_prompt(prompt)
    if not isinstance(prompt, Path):
        raise TypeError(f"prompt must be a str or Path, got {type(prompt).__name__}")
    try:
        st = prompt.stat()
    except OSError as exc:
        raise RationalizationError(f"prompt file could not be read: {type(exc).__name__}") from exc
    if not stat.S_ISREG(st.st_mode):
        raise RationalizationError("prompt Path must be a regular file, not a directory or special file")
    if st.st_size > PROMPT_MAX_BYTES:
        raise RationalizationError(f"prompt file passes {PROMPT_MAX_BYTES} bytes of UTF-8")
    try:
        raw = prompt.read_bytes()
    except OSError as exc:
        raise RationalizationError(f"prompt file could not be read: {type(exc).__name__}") from exc
    if len(raw) > PROMPT_MAX_BYTES:
        raise RationalizationError(f"prompt file passes {PROMPT_MAX_BYTES} bytes of UTF-8")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RationalizationError("prompt file must be valid UTF-8 text") from exc
    return _check_prompt(text)


def _resolve_provider(entry: object, *, persistent_cache: bool) -> ModelProvider:
    """Resolve one ``models`` entry from a model string, :class:`ModelProvider`, or callable."""
    if isinstance(entry, str):
        parsed = parse_model_string(entry)
        if parsed.kind == "anthropic":
            return AnthropicProvider.from_model_string(parsed)
        if parsed.kind == "gemini":
            return GeminiProvider.from_model_string(parsed)
        return OpenAICompatibleProvider.from_model_string(parsed)
    return as_provider(entry, persistent_cache=persistent_cache)


def _close_ephemeral(closers: Sequence[Callable[[], object]]) -> None:
    """Close idle ephemeral string-resolved providers without masking active exceptions."""
    if not isinstance(closers, Sequence):
        raise TypeError(f"closers must be a Sequence, got {type(closers).__name__}")
    for closer in closers:
        if callable(closer):
            with contextlib.suppress(Exception):
                closer()


def _check_snapshot_families(snapshots: tuple[ProviderSnapshot, ...], min_families: int) -> None:
    """Verify ``snapshots`` satisfy ``min_families`` across distinct known model families."""
    if min_families > len(snapshots):
        raise RationalizationError(
            f"min_families ({min_families}) cannot exceed the number of models ({len(snapshots)})"
        )
    known = {snap.model_family for snap in snapshots if snap.model_family != UNKNOWN_FAMILY}
    spanned = len(known) if known else min(1, len(snapshots))
    if spanned < min_families:
        raise RationalizationError(
            f"models span {spanned} distinct known model famil{'y' if spanned == 1 else 'ies'} "
            f"(unknown adds none), fewer than min_families={min_families}"
        )


def _prepare_snapshots_with_closers(
    models: object,
    prepared: PreparedSchema,
    *,
    min_families: int,
    persistent_cache: bool,
) -> tuple[tuple[ProviderSnapshot, ...], tuple[Callable[[], object], ...]]:
    """Resolve ``models``, snapshot each provider, and collect ``close`` hooks for ``str`` entries only."""
    if isinstance(models, (str, bytes, bytearray)) or not isinstance(models, Sequence):
        raise TypeError(
            f"models must be a sequence of model strings, providers or callables, got {type(models).__name__}"
        )
    if not models:
        raise RationalizationError("models must not be empty")
    providers: list[ModelProvider] = []
    closers: list[Callable[[], object]] = []
    try:
        for entry in models:
            prov = _resolve_provider(entry, persistent_cache=persistent_cache)
            providers.append(prov)
            if isinstance(entry, str) and callable(closer := getattr(prov, "close", None)):
                closers.append(closer)
        snapshots = snapshot_providers(providers, prepared)
        _check_snapshot_families(snapshots, min_families)
    except BaseException:
        _close_ephemeral(closers)
        raise
    return snapshots, tuple(closers)


def _prepare_snapshots(
    models: object,
    prepared: PreparedSchema,
    *,
    min_families: int,
    persistent_cache: bool,
) -> tuple[ProviderSnapshot, ...]:
    """Resolve ``models``, snapshot each provider, and check ``min_families`` against known model families."""
    snapshots, _ = _prepare_snapshots_with_closers(
        models,
        prepared,
        min_families=min_families,
        persistent_cache=persistent_cache,
    )
    return snapshots


# ---------------------------------------------------------------------------------------------------------------
# Step 5: Target resolution and cumulative request preflight
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _TargetWork:
    """Accumulated documents, units, envelopes, skipped units, hidden counts, and preflight plan."""

    docs_by_id: Mapping[str, PDFDocument]
    units_by_id: Mapping[str, Unit]
    envelopes_by_id: Mapping[str, Envelope]
    work: tuple[tuple[Unit, Envelope], ...]
    skipped_units: tuple[SkippedUnit, ...]
    hidden_excluded_by_reason: Mapping[str, int]
    preflight: _PreflightPlan


def _normalize_targets(target: object) -> tuple[TargetInput, ...]:
    """Validate and normalize ``target`` into a non-empty tuple of at most :data:`MAX_TARGETS` items."""
    if isinstance(target, _TARGET_TYPES):
        return (target,)
    if isinstance(target, Sequence) and not isinstance(target, (bytes, bytearray)):
        items = tuple(target)
        if not items:
            raise RationalizationError("target sequence must not be empty")
        if len(items) > MAX_TARGETS:
            raise RationalizationError(f"target sequence holds {len(items)} items, past MAX_TARGETS ({MAX_TARGETS})")
        for idx, item in enumerate(items):
            if not isinstance(item, _TARGET_TYPES):
                raise TypeError(
                    f"target[{idx}] must be a PDFDocument, CorpusManifest, str, or PathLike, got {type(item).__name__}"
                )
        return items
    raise TypeError(
        f"target must be a PDFDocument, CorpusManifest, str, PathLike, or Sequence thereof, got {type(target).__name__}"
    )


def _is_corpus_target(path: Path) -> bool:
    """True when ``path`` is ``corpus_manifest.json`` or a directory containing ``corpus_manifest.json``."""
    try:
        return path.name == CORPUS_MANIFEST or (path.is_dir() and (path / CORPUS_MANIFEST).exists())
    except (OSError, ValueError):
        return False


def _is_bundle_target(path: Path) -> bool:
    """True when ``path`` is a directory containing ``manifest.json``."""
    try:
        return path.is_dir() and (path / MANIFEST).exists()
    except (OSError, ValueError):
        return False


def _resolve_documents(item: TargetInput) -> tuple[PDFDocument, ...]:
    """Resolve one target item into one or more :class:`PDFDocument` instances."""
    if isinstance(item, PDFDocument):
        return (item,)
    if isinstance(item, CorpusManifest):
        if not item.entries:
            raise RationalizationError("corpus manifest has no extracted document entries")
        return item.load_documents()
    path = Path(item)
    if _is_corpus_target(path):
        manifest = load_corpus(path)
        if not manifest.entries:
            raise RationalizationError("corpus manifest has no extracted document entries")
        return manifest.load_documents()
    if _is_bundle_target(path):
        return (load_bundle(path),)
    reader_cls = _pdf_load("PDFReader")
    reader = reader_cls(item)
    return (cast(PDFDocument, getattr(reader, "document")),)


@dataclass(slots=True)
class _WorkAccumulator:
    """Mutable preflight accumulator building :class:`_TargetWork` in a single pass."""

    docs_by_id: dict[str, PDFDocument]
    units_by_id: dict[str, Unit]
    envelopes_by_id: dict[str, Envelope]
    work: list[tuple[Unit, Envelope]]
    skipped: list[SkippedUnit]
    hidden_counts: dict[str, int]
    slots: dict[tuple[str, str], str]
    requests: dict[str, _Request]
    file_lookups: dict[str, CacheLookup]
    miss_count: int = 0

    @classmethod
    def empty(cls) -> "_WorkAccumulator":
        return cls({}, {}, {}, [], [], {}, {}, {}, {}, 0)

    def freeze(self) -> _TargetWork:
        preflight = _PreflightPlan(
            slots=self.slots,
            requests=tuple(self.requests.values()),
            file_lookups=self.file_lookups,
        )
        return _TargetWork(
            docs_by_id=self.docs_by_id,
            units_by_id=self.units_by_id,
            envelopes_by_id=self.envelopes_by_id,
            work=tuple(self.work),
            skipped_units=tuple(self.skipped),
            hidden_excluded_by_reason=self.hidden_counts,
            preflight=preflight,
        )


def _probe_new_request(
    req: _Request,
    file_cache: FileCache | None,
    acc: _WorkAccumulator,
    max_requests: int,
) -> None:
    """Record distinct ``req``, probe ``file_cache`` at most once, and enforce ``max_requests``."""
    acc.requests[req.key] = req
    if file_cache is not None:
        lookup = file_cache.get(req.key)
        acc.file_lookups[req.key] = lookup
        if lookup.hit:
            return
    acc.miss_count += 1
    if acc.miss_count > max_requests:
        raise RationalizationError(
            f"this run needs at least {acc.miss_count} requests (distinct cache misses), past max_requests "
            f"{max_requests}: raise max_requests, or send fewer units or models"
        )


def _record_doc_requests(
    kept_units: Sequence[Unit],
    envelopes: Sequence[Envelope],
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    file_cache: FileCache | None,
    acc: _WorkAccumulator,
    max_requests: int,
) -> None:
    """Key and probe ``kept_units`` × ``snapshots`` once, failing fast past ``max_requests``."""
    if len(kept_units) != len(envelopes):
        raise ValueError("kept_units and envelopes must have equal length")
    if max_requests < 1:
        raise ValueError(f"max_requests must be >= 1, got {max_requests}")
    for unit, envelope in zip(kept_units, envelopes, strict=True):
        env_hasher = hashlib.sha256(_envelope_key_prefix(envelope))
        for snapshot in snapshots:
            key = _fast_request_key(envelope, suffixes[snapshot.model_id], envelope_hasher=env_hasher)
            slot = (unit.unit_id, snapshot.model_id)
            if acc.slots.setdefault(slot, key) != key:
                raise ValueError(f"unit {unit.unit_id} appears twice with different envelopes")
            if key not in acc.requests:
                _probe_new_request(_Request(key, unit, envelope, snapshot), file_cache, acc, max_requests)
        acc.units_by_id[unit.unit_id] = unit
        acc.envelopes_by_id[unit.unit_id] = envelope
        acc.work.append((unit, envelope))


def _suffixes_by_model(
    snapshots: Sequence[ProviderSnapshot],
    max_output_tokens: int,
    schema_version: str,
) -> dict[str, bytes]:
    """Precompute canonical non-envelope key suffixes once per provider snapshot."""
    if not snapshots:
        raise ValueError("snapshots must not be empty")
    if max_output_tokens < 1:
        raise ValueError(f"max_output_tokens must be >= 1, got {max_output_tokens}")
    return {
        s.model_id: _provider_key_suffix(
            provider_kind=s.provider_kind,
            model_id=s.model_id,
            generation_params={**s.generation_params, "max_output_tokens": max_output_tokens},
            provider_schema_json=s.provider_schema_json,
            schema_version=schema_version,
        )
        for s in snapshots
    }


def _add_doc_to_work(
    doc: PDFDocument,
    acc: _WorkAccumulator,
    *,
    unit_opts: tuple[int, bool, bool, bool],
    prompt_text: str,
    context: object,
    wrapper_json: str,
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    file_cache: FileCache | None,
    limits: RunLimits,
) -> None:
    """Validate ``doc_id``, pack units, build envelopes, and record requests for one document."""
    max_unit_chars, coalesce_sections, include_schema, multimodal = unit_opts
    doc_id = doc.metadata.doc_id
    if doc_id in acc.docs_by_id:
        raise RationalizationError(f"duplicate doc_id {doc_id!r} across targets: each target must be distinct")
    if len(acc.docs_by_id) >= MAX_TARGETS:
        raise RationalizationError(f"resolved targets exceed MAX_TARGETS ({MAX_TARGETS})")
    acc.docs_by_id[doc_id] = doc
    plan = build_units(
        doc,
        max_unit_chars=max_unit_chars,
        render=render_element,
        coalesce_sections=coalesce_sections,
        multimodal=multimodal,
    )
    kept_units, envelopes, render_skipped = _safe_doc_envelopes(
        plan.units,
        doc,
        prompt_text=prompt_text,
        context=context,
        wrapper_json=wrapper_json,
        coalesce_sections=coalesce_sections,
        include_schema=include_schema,
        multimodal=multimodal,
        strict=limits.strict,
    )
    acc.skipped.extend(render_skipped)
    _record_doc_requests(kept_units, envelopes, snapshots, suffixes, file_cache, acc, limits.max_requests)
    acc.skipped.extend(plan.skipped)
    for reason, count in plan.hidden_excluded_total:
        acc.hidden_counts[reason] = acc.hidden_counts.get(reason, 0) + count


def _build_target_work(
    raw_targets: Sequence[TargetInput],
    *,
    max_unit_chars: int,
    coalesce_sections: bool,
    include_schema_in_prompt: bool = True,
    multimodal: bool = True,
    prompt_text: str,
    context: object,
    prepared: PreparedSchema,
    snapshots: Sequence[ProviderSnapshot],
    schema_version: str,
    file_cache: FileCache | None,
    limits: RunLimits,
) -> _TargetWork:
    """Resolve targets in order, building units and envelopes while checking ``doc_id`` and ``max_requests``."""
    acc = _WorkAccumulator.empty()
    suffixes = _suffixes_by_model(snapshots, limits.max_output_tokens, schema_version)
    unit_opts = (max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal)
    for doc in (doc for item in raw_targets for doc in _resolve_documents(item)):
        _add_doc_to_work(
            doc,
            acc,
            unit_opts=unit_opts,
            prompt_text=prompt_text,
            context=context,
            wrapper_json=prepared.wrapper_json,
            snapshots=snapshots,
            suffixes=suffixes,
            file_cache=file_cache,
            limits=limits,
        )
    return acc.freeze()


_FATAL_PDF_ERRORS = (PDFSourceChangedError, BundleCorruptError, PDFCorruptError, PDFEncryptedError)


def _safe_doc_envelopes(
    units: tuple[Unit, ...],
    doc: PDFDocument,
    *,
    prompt_text: str,
    context: object,
    wrapper_json: str,
    coalesce_sections: bool,
    include_schema: bool,
    multimodal: bool,
    strict: bool,
) -> tuple[tuple[Unit, ...], tuple[Envelope, ...], tuple[SkippedUnit, ...]]:
    """Build envelopes for ``units``; isolate per-page render errors into ``SkippedUnit`` when ``not strict``."""
    if not strict:
        return build_envelopes_isolated(
            units,
            doc,
            prompt=prompt_text,
            context=context,
            schema_json=wrapper_json,
            coalesce_sections=coalesce_sections,
            include_schema=include_schema,
            multimodal=multimodal,
            fatal_errors=_FATAL_PDF_ERRORS,
            _trusted_blocks=True,
        )
    try:
        envs = build_envelopes(
            units,
            doc,
            prompt=prompt_text,
            context=context,
            schema_json=wrapper_json,
            coalesce_sections=coalesce_sections,
            include_schema=include_schema,
            multimodal=multimodal,
            _trusted_blocks=True,
        )
    except _FATAL_PDF_ERRORS:
        raise
    except DatagruntPDFError as exc:
        raise RationalizationError(f"multimodal page render failed on {doc.metadata.doc_id!r}: {exc}") from exc
    return units, envs, ()


# ---------------------------------------------------------------------------------------------------------------
# Step 6: Pipeline execution and result assembly
# ---------------------------------------------------------------------------------------------------------------


def _escaped_totals(units: Mapping[str, Unit]) -> dict[str, int]:
    """Sum R8 escaped character counts per category across all packed unit elements."""
    totals: dict[str, int] = {}
    for unit in units.values():
        for element in unit.elements:
            for category, count in element_escape_counts(element):
                totals[category] = totals.get(category, 0) + count
    return totals


def _run_post_stages(
    target_work: _TargetWork,
    outcome: RunOutcome,
    prepared: PreparedSchema,
    *,
    min_families: int,
    must_agree: Sequence[str],
    fold_across_units: bool,
    multimodal: bool,
) -> tuple[ConsensusResult, GroundingResult, FoldingResult[Any]]:
    """Run consensus, grounding, and document folding over ``outcome`` when units are present."""
    if not isinstance(target_work, _TargetWork) or not isinstance(outcome, RunOutcome):
        raise TypeError("target_work and outcome must be validated pipeline structures")
    if not target_work.units_by_id:
        return (
            ConsensusResult(items=(), quarantined=()),
            GroundingResult(items=(), empty_units=()),
            FoldingResult(items=()),
        )
    consensus_res = build_consensus(
        outcome,
        prepared,
        tuple(target_work.units_by_id),
        min_families=min_families,
        must_agree=must_agree,
    )
    grounding_res = ground(
        consensus_res,
        outcome=outcome,
        units=target_work.units_by_id,
        envelopes=target_work.envelopes_by_id,
        documents=target_work.docs_by_id,
        schema=prepared,
    )
    folding_res: FoldingResult[Any] = fold_documents(
        grounding_res,
        units=target_work.units_by_id,
        envelopes=target_work.envelopes_by_id,
        documents=target_work.docs_by_id,
        schema=prepared,
        fold_across_units=fold_across_units,
        multimodal=multimodal,
    )
    return consensus_res, grounding_res, folding_res


def _build_telemetry(
    target_work: _TargetWork,
    outcome: RunOutcome,
    consensus_res: ConsensusResult,
    grounding_res: GroundingResult,
) -> RationalizeTelemetry:
    """Assemble :class:`RationalizeTelemetry` from target, runner, consensus, and grounding counts."""
    if not isinstance(target_work, _TargetWork) or not isinstance(outcome, RunOutcome):
        raise TypeError("target_work and outcome must be validated pipeline structures")
    return RationalizeTelemetry(
        document_count=len(target_work.docs_by_id),
        unit_count=len(target_work.units_by_id),
        skipped_unit_count=len(target_work.skipped_units),
        empty_unit_count=len(grounding_res.empty_units),
        failed_unit_count=len(outcome.failed_units),
        oversize_unit_count=sum(1 for u in target_work.units_by_id.values() if u.oversize),
        cache_hits=outcome.cache_hits,
        cache_misses=outcome.cache_misses,
        cache_corrupt=outcome.cache_corrupt,
        cache_write_failures=outcome.cache_write_failures,
        quarantined_count=len(consensus_res.quarantined),
        hidden_excluded_by_reason=target_work.hidden_excluded_by_reason,
        escaped_by_category=_escaped_totals(target_work.units_by_id),
        usage_by_model=outcome.usage,
        models=outcome.models,
    )


def _assemble_pipeline_result(
    target_work: _TargetWork,
    outcome: RunOutcome,
    prepared: PreparedSchema,
    *,
    min_families: int,
    must_agree: Sequence[str],
    expect_non_empty: bool,
    fold_across_units: bool = True,
    multimodal: bool = True,
) -> RationalizedResult[Any]:
    """Execute consensus, grounding and folding over ``outcome`` and assemble :class:`RationalizedResult`."""
    consensus_res, grounding_res, folding_res = _run_post_stages(
        target_work,
        outcome,
        prepared,
        min_families=min_families,
        must_agree=must_agree,
        fold_across_units=fold_across_units,
        multimodal=multimodal,
    )
    telemetry = _build_telemetry(target_work, outcome, consensus_res, grounding_res)
    items: tuple[GroundedRecord[Any], ...] = folding_res.items
    records = tuple(item.record for item in items if item.status in _CLEAN_STATUSES and item.record is not None)
    needs_review = tuple(item for item in items if item.status not in _CLEAN_STATUSES)
    result: RationalizedResult[Any] = RationalizedResult(
        items=items,
        records=records,
        needs_review=needs_review,
        quarantined=consensus_res.quarantined,
        failed_units=outcome.failed_units,
        empty_units=grounding_res.empty_units,
        skipped_units=target_work.skipped_units,
        telemetry=telemetry,
        schema=prepared.spec,
    )
    if expect_non_empty and not result.records:
        raise RationalizationError("expect_non_empty=True, but rationalize() produced no records")
    return result


def _run_pipeline(
    target_work: _TargetWork,
    snapshots: Sequence[ProviderSnapshot],
    prepared: PreparedSchema,
    *,
    schema_version: str,
    memory_cache: MemoryCache,
    file_cache: FileCache | None,
    limits: RunLimits,
    min_families: int,
    must_agree: Sequence[str],
    expect_non_empty: bool,
    fold_across_units: bool = True,
    multimodal: bool = True,
    on_event: EventHook | None,
) -> RationalizedResult[Any]:
    """Execute runner, consensus, grounding and folding, then assemble :class:`RationalizedResult`."""
    outcome = run_requests(
        target_work.work,
        snapshots,
        prepared,
        schema_version=schema_version,
        memory_cache=memory_cache,
        file_cache=file_cache,
        limits=limits,
        on_event=on_event,
        _preflight=target_work.preflight,
    )
    return _assemble_pipeline_result(
        target_work,
        outcome,
        prepared,
        min_families=min_families,
        must_agree=must_agree,
        expect_non_empty=expect_non_empty,
        fold_across_units=fold_across_units,
        multimodal=multimodal,
    )


# ---------------------------------------------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------------------------------------------


def _preflight_schema_and_snapshots(
    pre: tuple[type[Any], str | Sequence[str] | None, Sequence[str], str | Path, object, Sequence[ModelInput], Any],
    min_families: int,
) -> tuple[PreparedSchema, tuple[str, ...], str, tuple[ProviderSnapshot, ...], tuple[Callable[[], object], ...]]:
    """Prepare ``schema``, validate ``prompt`` and ``context``, and snapshot ``models``."""
    if len(pre) != 7 or min_families < 1:
        raise ValueError("pre must be a 7-tuple and min_families >= 1")
    schema, key, must_agree, prompt, context, models, cache_dir = pre
    prepared = prepare(schema, key=key)
    checked_must_agree = check_must_agree(prepared.spec, must_agree)
    prompt_text = _read_prompt(prompt)
    _context_json(context)
    _check_schema(prepared.wrapper_json)
    snapshots, closers = _prepare_snapshots_with_closers(
        models,
        prepared,
        min_families=min_families,
        persistent_cache=cache_dir is not None,
    )
    return prepared, checked_must_agree, prompt_text, snapshots, closers


def _execute_with_snapshots(
    target: TargetInput | Sequence[TargetInput],
    pre: tuple[type[Any], str | Sequence[str] | None, Sequence[str], str | Path, object, Sequence[ModelInput], Any],
    limits: RunLimits,
    u_opts: tuple[int, bool, bool, bool],
    r_opts: tuple[bool, bool, int, str, EventHook | None],
) -> RationalizedResult[Any]:
    """Snapshot ``models``, resolve targets, run the pipeline, and close ephemeral ``str`` models."""
    if not isinstance(limits, RunLimits) or len(pre) != 7:
        raise TypeError("limits and pre must be validated preflight inputs")
    max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal = u_opts
    fold_across_units, expect_non_empty, min_families, schema_version, on_event = r_opts
    _, _, _, _, context, _, cache_dir = pre
    prepared, checked_must_agree, prompt_text, snapshots, closers = _preflight_schema_and_snapshots(pre, min_families)
    try:
        file_cache = FileCache(cache_dir) if cache_dir is not None else None
        target_work = _build_target_work(
            _normalize_targets(target),
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
        return _run_pipeline(
            target_work,
            snapshots,
            prepared,
            schema_version=schema_version,
            memory_cache=MemoryCache(),
            file_cache=file_cache,
            limits=limits,
            min_families=min_families,
            must_agree=checked_must_agree,
            expect_non_empty=expect_non_empty,
            fold_across_units=fold_across_units,
            multimodal=multimodal,
            on_event=on_event,
        )
    finally:
        _close_ephemeral(closers)


def rationalize(
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
    max_concurrency: int = 8,
    max_concurrency_per_provider: int = 4,
    rpm_limit: int | None = None,
    timeout_s: float = 60.0,
    max_retries: int = 2,
    max_unit_chars: int = 120_000,
    max_requests: int = 1_000,
    max_output_tokens: int = 8_192,
    expect_non_empty: bool = False,
    strict: bool = False,
    coalesce_sections: bool = True,
    include_schema_in_prompt: bool = True,
    multimodal: bool = True,
    fold_across_units: bool = True,
    on_event: EventHook | None = None,
) -> RationalizedResult[T]:
    """Extract, ground, reach cross-family consensus on, and fold schema records over v5 PDF documents (plan §4.11).

    Args:
        target: A :class:`~datagrunt.pdf.PDFDocument`, a :class:`~datagrunt.pdf.CorpusManifest`, a PDF, bundle, or
            corpus path (``str`` or ``PathLike``), or a sequence of 1 to :data:`MAX_TARGETS` targets.
        schema: The caller's Pydantic ``BaseModel`` subclass describing one entity record.
        prompt: Extraction instructions as a non-blank ``str`` or a regular UTF-8 file :class:`~pathlib.Path`.
        models: Non-empty sequence of model strings, :class:`ModelProvider` objects, or callables.
        min_families: Minimum distinct known model families required for field consensus (default 2).
        schema_version: Caller-controlled version string included in cache keys (default ``"1.0"``).
        key: Optional root key field name(s) when not annotated with :data:`~datagrunt.rationalization.Key` on the
            schema class.
        must_agree: Dotted field attribute paths requiring unanimous agreement across all requested models.
        context: Optional JSON-serializable caller context included in every user envelope.
        cache_dir: Optional directory path for persistent atomic JSON response caching.
        max_concurrency: Maximum total worker threads across all providers (1..64, default 8).
        max_concurrency_per_provider: Maximum concurrent requests per rate key (1..64, default 4).
        rpm_limit: Optional requests-per-minute limit per rate key (1..100,000).
        timeout_s: Per-attempt timeout in seconds (1.0..3600.0, default 60.0).
        max_retries: Retries per transient failure (0..10, default 2).
        max_unit_chars: Maximum rendered characters per packed unit body (>= 1, default 120,000).
        max_requests: Maximum distinct cache misses allowed in this call (1..100,000, default 1,000).
        max_output_tokens: Output token cap sent on each request (1..131,072, default 8,192).
        expect_non_empty: If ``True``, raise :class:`RationalizationError` when ``result.records`` is empty.
        strict: If ``True``, raise :class:`RationalizationError` on any unit request failure.
        coalesce_sections: If ``True`` (default), pack consecutive text-bearing groups greedily across section
            boundaries up to ``max_unit_chars``.
        include_schema_in_prompt: If ``True`` (default), inline the JSON schema in the system prompt in addition
            to provider-enforced structured outputs; set ``False`` to trim system prompt token overhead.
        on_event: Optional callback invoked on the caller's thread with each :class:`TelemetryEvent`.

    Returns:
        A :class:`~datagrunt.rationalization.result.RationalizedResult` parameterized by ``schema``. What was not
        sent is reported, never dropped: ``result.skipped_units`` names each page error, all-hidden or no-text
        group, and each ``"image_only"`` page (``Page.image_only_reasons``: its content lives only in its page
        image, so none of its text, visible or hidden, is sent, and no model reads the image yet), with its pages.

    Raises:
        TypeError: If any parameter has the wrong type.
        ValueError: If any numeric or string bound is out of range.
        RationalizationError: If the schema, ``must_agree``, ``prompt``, ``context``, ``models``, ``min_families``,
            ``cache_dir``, target ``doc_id`` uniqueness, ``max_requests`` preflight, a permanent/strict provider
            error, or ``expect_non_empty`` check fails.
    """
    limits = RunLimits(
        max_concurrency=max_concurrency,
        max_concurrency_per_provider=max_concurrency_per_provider,
        rpm_limit=rpm_limit,
        timeout_s=timeout_s,
        max_retries=max_retries,
        max_output_tokens=max_output_tokens,
        max_requests=max_requests,
        strict=strict,
    )
    f1 = (("expect_non_empty", expect_non_empty), ("coalesce_sections", coalesce_sections))
    f2 = (("include_schema_in_prompt", include_schema_in_prompt), ("multimodal", multimodal))
    f3 = (*f1, *f2, ("fold_across_units", fold_across_units))
    _check_extra_scalars(max_unit_chars, schema_version, on_event, min_families, f3)
    u_opts = (max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal)
    r_opts = (fold_across_units, expect_non_empty, min_families, schema_version, on_event)
    pre = (schema, key, must_agree, prompt, context, models, cache_dir)
    return cast(RationalizedResult[T], _execute_with_snapshots(target, pre, limits, u_opts, r_opts))
