"""Public entry points ``rationalize()`` and ``rationalize_many()``: preflight, bisection, and pipeline wiring.

:func:`rationalize` and :func:`rationalize_many` orchestrate extraction over one or more
:class:`~datagrunt.pdf.PDFDocument` instances (or PDF, bundle, or corpus paths):

1. **Zero-I/O preflight**: validate all bounds, boolean flags, prompts, contexts, schemas, ``must_agree``,
   and provider model-family diversity before opening any target document or making any network call.
2. **Single-pass target & provider resolution**: resolve ``models`` once (sharing HTTP connection pools across
   passes and bisection rounds) and resolve targets once (reading corpus bundles in a single pass).
3. **Execution & truncation bisection**: execute passes concurrently via :func:`run_multi_requests`, iteratively
   bisecting any splittable unit whose response finishes ``"truncated"`` (up to :data:`MAX_TRUNCATION_ROUNDS`
   within ``max_requests``), then run consensus, grounding, and document folding per pass.

The module imports only the standard library, pydantic, the PDF model and its sibling modules: nothing from
``datagrunt.core``, and no import inside a function (invariant 9).
"""

import contextlib
import dataclasses
import hashlib
import stat
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from typing import Any, Generic, TypeAlias, TypeVar, cast

from pydantic import BaseModel

from datagrunt.pdf._bundle_load import MANIFEST, load_bundle
from datagrunt.pdf._extra import load as _pdf_load
from datagrunt.pdf.corpus import CORPUS_MANIFEST, CorpusManifest, load_corpus_documents
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
    EventDelivery,
    EventHook,
    FailedUnit,
    ModelUsage,
    PassRunSpec,
    ProviderSnapshot,
    RunLimits,
    RunOutcome,
    _PreflightPlan,
    _Request,
    run_multi_requests,
    run_requests,
    snapshot_providers,
)
from datagrunt.rationalization.schema import PreparedSchema, prepare
from datagrunt.rationalization.units import SkippedUnit, Unit, bisect_unit, build_units
from datagrunt.rationalization.wire import MAX_OUTPUT_TOKENS, ModelRequest, ProviderResponse

MAX_TARGETS = 10_000
"""The most targets a single :func:`rationalize` or :func:`rationalize_many` call accepts (plan §4.11)."""

MAX_PASSES = 64
"""The most schema passes a single :func:`rationalize_many` call accepts."""

MAX_TRUNCATION_ROUNDS = 16
"""Maximum iterative midpoint-bisection rounds when ``split_on_truncation=True``."""

T = TypeVar("T", bound=BaseModel)
S = TypeVar("S")
R = TypeVar("R")
TargetInput: TypeAlias = PDFDocument | CorpusManifest | str | PathLike[str]
ModelInput: TypeAlias = str | ModelProvider | Callable[[ModelRequest], ProviderResponse]
_TARGET_TYPES = (PDFDocument, CorpusManifest, str, PathLike)
_FATAL_PDF_ERRORS = (PDFSourceChangedError, BundleCorruptError, PDFCorruptError, PDFEncryptedError)
_PREFLIGHT_WORKERS = 16
_MIN_PARALLEL_CACHE_KEYS = 4

__all__ = ["MAX_PASSES", "MAX_TARGETS", "MAX_TRUNCATION_ROUNDS", "PassSpec", "rationalize", "rationalize_many"]


# ---------------------------------------------------------------------------------------------------------------
# Step 1: Scalar, PassSpec, and provider preflight helpers
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


def _check_str_seq(name: str, value: object) -> tuple[str, ...]:
    """Validate and freeze a sequence of ``str`` values (rejecting bare ``str``/``bytes``)."""
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence of str, got {type(value).__name__}")
    items = tuple(value)
    for idx, entry in enumerate(items):
        if not isinstance(entry, str):
            raise TypeError(f"{name}[{idx}] must be a str, got {type(entry).__name__}")
    return items


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
    split_on_truncation: bool = True,
    event_delivery: EventDelivery = "ordered",
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
        event_delivery=event_delivery,
    )
    flags = (
        ("expect_non_empty", expect_non_empty),
        ("coalesce_sections", coalesce_sections),
        ("include_schema_in_prompt", include_schema_in_prompt),
        ("multimodal", multimodal),
        ("fold_across_units", fold_across_units),
        ("split_on_truncation", split_on_truncation),
    )
    _check_extra_scalars(max_unit_chars, schema_version, on_event, min_families, flags)
    return limits


@dataclass(frozen=True, slots=True)
class PassSpec(Generic[T]):
    """Per-pass schema, prompt, and packing/consensus overrides for :func:`rationalize_many`."""

    schema: type[T]
    prompt: str | Path = field(repr=False)
    name: str | None = None
    schema_version: str = "1.0"
    key: str | Sequence[str] | None = None
    must_agree: Sequence[str] = ()
    context: object = field(default=None, repr=False)
    max_unit_chars: int = 120_000
    max_output_tokens: int = 8_192
    min_families: int | None = None
    expect_non_empty: bool = False
    coalesce_sections: bool = True
    include_schema_in_prompt: bool = True
    multimodal: bool = True
    fold_across_units: bool = True
    split_on_truncation: bool | None = None

    def __post_init__(self) -> None:
        _check_pass_spec_identity(self)
        _check_pass_spec_options(self)


def _check_pass_spec_identity(spec: PassSpec[Any]) -> None:
    """Validate ``schema``, ``prompt``, ``name``, ``schema_version``, ``key``, and ``must_agree`` on ``spec``."""
    if not isinstance(spec.schema, type) or not issubclass(spec.schema, BaseModel):
        raise TypeError(f"schema must be a pydantic BaseModel subclass, got {spec.schema!r}")
    if isinstance(spec.prompt, str):
        if not spec.prompt.strip():
            raise ValueError("prompt must be non-blank")
        _check_prompt(spec.prompt)
    elif not isinstance(spec.prompt, Path):
        raise TypeError(f"prompt must be a str or Path, got {type(spec.prompt).__name__}")
    if spec.name is not None:
        if not isinstance(spec.name, str):
            raise TypeError(f"name must be a str or None, got {type(spec.name).__name__}")
        if not spec.name.strip():
            raise ValueError("name must be non-blank when set")
    if (ver_prob := identity_problem(spec.schema_version)) is not None:
        raise (TypeError if not isinstance(spec.schema_version, str) else ValueError)(f"schema_version {ver_prob}")
    if spec.key is not None and not isinstance(spec.key, str):
        object.__setattr__(spec, "key", _check_str_seq("key", spec.key))
    object.__setattr__(spec, "must_agree", _check_str_seq("must_agree", spec.must_agree))


def _check_pass_spec_options(spec: PassSpec[Any]) -> None:
    """Validate numeric bounds and strict boolean flags on ``spec``."""
    if type(spec.max_unit_chars) is not int:
        raise TypeError(f"max_unit_chars must be an int, got {type(spec.max_unit_chars).__name__}")
    if spec.max_unit_chars < 1:
        raise ValueError(f"max_unit_chars must be >= 1, got {spec.max_unit_chars}")
    if type(spec.max_output_tokens) is not int:
        raise TypeError(f"max_output_tokens must be an int, got {type(spec.max_output_tokens).__name__}")
    if not 1 <= spec.max_output_tokens <= MAX_OUTPUT_TOKENS:
        raise ValueError(f"max_output_tokens must be in 1-{MAX_OUTPUT_TOKENS}, got {spec.max_output_tokens}")
    if spec.min_families is not None:
        if type(spec.min_families) is not int:
            raise TypeError(f"min_families must be an int or None, got {type(spec.min_families).__name__}")
        if spec.min_families < 1:
            raise ValueError(f"min_families must be >= 1, got {spec.min_families}")
    flags = (
        ("expect_non_empty", spec.expect_non_empty),
        ("coalesce_sections", spec.coalesce_sections),
        ("include_schema_in_prompt", spec.include_schema_in_prompt),
        ("multimodal", spec.multimodal),
        ("fold_across_units", spec.fold_across_units),
    )
    _check_bool_flags(flags)
    if spec.split_on_truncation is not None:
        _require_bool("split_on_truncation", spec.split_on_truncation)


def _normalize_mapping_passes(passes: Mapping[object, object]) -> tuple[tuple[str, PassSpec[Any]], ...]:
    """Validate a ``Mapping[str, PassSpec]`` into ordered ``(pass_name, spec)`` pairs."""
    if not passes:
        raise ValueError("passes must not be empty")
    if len(passes) > MAX_PASSES:
        raise ValueError(f"passes holds {len(passes)} entries, past MAX_PASSES ({MAX_PASSES})")
    resolved: list[tuple[str, PassSpec[Any]]] = []
    for raw_name, raw_spec in passes.items():
        if not isinstance(raw_name, str):
            raise TypeError(f"passes keys must be str, got {type(raw_name).__name__}")
        if not raw_name.strip():
            raise ValueError("pass name must be non-blank")
        if not isinstance(raw_spec, PassSpec):
            raise TypeError(f"passes[{raw_name!r}] must be a PassSpec, got {type(raw_spec).__name__}")
        if raw_spec.name is not None and raw_spec.name != raw_name:
            raise ValueError(f"pass key {raw_name!r} conflicts with PassSpec.name {raw_spec.name!r}")
        resolved.append((raw_name, raw_spec))
    return tuple(resolved)


def _normalize_passes(passes: object) -> tuple[tuple[str, PassSpec[Any]], ...]:
    """Validate ``passes`` as a non-empty ``Mapping`` or ``Sequence`` of at most :data:`MAX_PASSES` items."""
    if isinstance(passes, Mapping):
        return _normalize_mapping_passes(passes)
    if isinstance(passes, (str, bytes, bytearray)) or not isinstance(passes, Sequence):
        raise TypeError(f"passes must be a Mapping or Sequence of PassSpec, got {type(passes).__name__}")
    items = tuple(passes)
    if not items:
        raise ValueError("passes must not be empty")
    if len(items) > MAX_PASSES:
        raise ValueError(f"passes holds {len(items)} items, past MAX_PASSES ({MAX_PASSES})")
    seen: set[str] = set()
    resolved: list[tuple[str, PassSpec[Any]]] = []
    for idx, raw_spec in enumerate(items):
        if not isinstance(raw_spec, PassSpec):
            raise TypeError(f"passes[{idx}] must be a PassSpec, got {type(raw_spec).__name__}")
        pass_name = raw_spec.name if raw_spec.name is not None else raw_spec.schema.__name__
        if not isinstance(pass_name, str) or not pass_name.strip():
            raise ValueError("pass name must be non-blank")
        if pass_name in seen:
            raise ValueError(f"duplicate pass name {pass_name!r} in passes")
        seen.add(pass_name)
        resolved.append((pass_name, raw_spec))
    return tuple(resolved)


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


def _resolve_providers_with_closers(
    models: object,
    *,
    persistent_cache: bool,
) -> tuple[tuple[ModelProvider, ...], tuple[Callable[[], object], ...]]:
    """Resolve ``models`` once into :class:`ModelProvider` instances and collect ``close`` hooks for ``str`` entries."""
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
    except BaseException:
        _close_ephemeral(closers)
        raise
    return tuple(providers), tuple(closers)


def _prepare_snapshots_with_closers(
    models: object,
    prepared: PreparedSchema,
    *,
    min_families: int,
    persistent_cache: bool,
) -> tuple[tuple[ProviderSnapshot, ...], tuple[Callable[[], object], ...]]:
    """Resolve ``models``, snapshot each provider, and collect ``close`` hooks for ``str`` entries only."""
    providers, closers = _resolve_providers_with_closers(models, persistent_cache=persistent_cache)
    try:
        snapshots = snapshot_providers(providers, prepared)
        _check_snapshot_families(snapshots, min_families)
    except BaseException:
        _close_ephemeral(closers)
        raise
    return snapshots, closers


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
# Step 2: Target resolution and cumulative request preflight
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


def _resolve_corpus_path(path: Path) -> tuple[PDFDocument, ...]:
    """Load verified corpus documents in a single pass via :func:`load_corpus_documents`."""
    manifest, docs = load_corpus_documents(path)
    if not manifest.entries:
        raise RationalizationError("corpus manifest has no extracted document entries")
    return docs


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
        return _resolve_corpus_path(path)
    if _is_bundle_target(path):
        return (load_bundle(path),)
    reader_cls = _pdf_load("PDFReader")
    reader = reader_cls(item)
    return (cast(PDFDocument, getattr(reader, "document")),)


def _resolve_all_documents(raw_targets: Sequence[TargetInput]) -> dict[str, PDFDocument]:
    """Resolve ``raw_targets`` once into an ordered ``{doc_id: PDFDocument}`` mapping."""
    if not raw_targets:
        raise RationalizationError("target sequence must not be empty")
    docs_by_id: dict[str, PDFDocument] = {}
    for item in raw_targets:
        for doc in _resolve_documents(item):
            doc_id = doc.metadata.doc_id
            if doc_id in docs_by_id:
                raise RationalizationError(f"duplicate doc_id {doc_id!r} across targets: each target must be distinct")
            if len(docs_by_id) >= MAX_TARGETS:
                raise RationalizationError(f"resolved targets exceed MAX_TARGETS ({MAX_TARGETS})")
            docs_by_id[doc_id] = doc
    return docs_by_id


def _collect_done_futures(
    done: set[Future[R]],
    running: dict[Future[R], int],
    results: dict[int, R],
    errors: dict[int, Exception],
    stop_at: int,
) -> int:
    """Record completed futures into ``results`` or ``errors`` and return updated ``stop_at``."""
    for fut in done:
        idx = running.pop(fut)
        exc = fut.exception()
        if exc is None:
            results[idx] = fut.result()
            continue
        if not isinstance(exc, Exception):
            raise exc
        errors[idx] = exc
        stop_at = min(stop_at, idx)
    return stop_at


def _parallel_map(items: Sequence[S], fn: Callable[[S], R], *, min_parallel: int) -> tuple[R, ...]:
    """Map ``fn`` across ``items`` on a bounded pool, preserving order and lowest-index exception."""
    n = len(items)
    if n <= min_parallel:
        return tuple(fn(item) for item in items)
    workers = min(_PREFLIGHT_WORKERS, n)
    results: dict[int, R] = {}
    errors: dict[int, Exception] = {}
    running: dict[Future[R], int] = {}
    next_idx = 0
    stop_at = n
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="datagrunt-preflight")
    try:
        for _ in range(n + 1):
            while next_idx < stop_at and len(running) < workers:
                running[pool.submit(fn, items[next_idx])] = next_idx
                next_idx += 1
            if not running:
                break
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            stop_at = _collect_done_futures(done, running, results, errors, stop_at)
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    if errors:
        raise errors[min(errors)]
    return tuple(results[idx] for idx in range(n))


@dataclass(slots=True)
class _CacheProbeTracker:
    """Shared cache-probe state deduplicating ``FileCache.get`` and counting misses across passes and rounds."""

    file_cache: FileCache | None
    memory_cache: MemoryCache | None
    shared_lookups: dict[str, CacheLookup]
    miss_keys: set[str]
    max_requests: int

    def prewarm(self, keys: Sequence[str]) -> None:
        """Pre-fetch unprobed ``keys`` in bounded parallel when ``file_cache`` is set and ``> 4`` remain."""
        if self.file_cache is None or not keys:
            return
        todo = tuple(
            k
            for k in dict.fromkeys(keys)
            if k not in self.shared_lookups and not (self.memory_cache is not None and self.memory_cache.get(k).hit)
        )
        if not todo:
            return
        fetched = _parallel_map(todo, self.file_cache.get, min_parallel=_MIN_PARALLEL_CACHE_KEYS)
        for key, lookup in zip(todo, fetched, strict=True):
            self.shared_lookups[key] = lookup

    def probe(self, key: str, pass_lookups: dict[str, CacheLookup]) -> None:
        """Probe ``key`` at most once against memory and file caches and enforce ``max_requests``."""
        if self.memory_cache is not None and self.memory_cache.get(key).hit:
            return
        if self.file_cache is not None:
            lookup = self.shared_lookups.get(key)
            if lookup is None:
                lookup = self.file_cache.get(key)
                self.shared_lookups[key] = lookup
            pass_lookups[key] = lookup
            if lookup.hit:
                return
        self.miss_keys.add(key)
        if len(self.miss_keys) > self.max_requests:
            raise RationalizationError(
                f"this run needs at least {len(self.miss_keys)} requests (distinct cache misses), past max_requests "
                f"{self.max_requests}: raise max_requests, or send fewer units or models"
            )


@dataclass(slots=True)
class _WorkAccumulator:
    """Mutable preflight accumulator building :class:`_TargetWork` for one schema pass."""

    docs_by_id: dict[str, PDFDocument]
    units_by_id: dict[str, Unit]
    envelopes_by_id: dict[str, Envelope]
    work: list[tuple[Unit, Envelope]]
    skipped: list[SkippedUnit]
    hidden_counts: dict[str, int]
    slots: dict[tuple[str, str], str]
    requests: dict[str, _Request]
    file_lookups: dict[str, CacheLookup]

    @classmethod
    def empty(cls) -> "_WorkAccumulator":
        return cls({}, {}, {}, [], [], {}, {}, {}, {})

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


def _key_unit_requests(
    kept_units: Sequence[Unit],
    envelopes: Sequence[Envelope],
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    req_meta: tuple[int, str, str, str],
) -> tuple[dict[tuple[str, str], str], dict[str, _Request]]:
    """Compute request keys and :class:`_Request` entries for ``kept_units`` × ``snapshots`` without cache I/O."""
    if len(kept_units) != len(envelopes):
        raise ValueError("kept_units and envelopes must have equal length")
    max_tokens, schema_name, schema_version, schema_sha256 = req_meta
    slots: dict[tuple[str, str], str] = {}
    requests: dict[str, _Request] = {}
    for unit, envelope in zip(kept_units, envelopes, strict=True):
        env_hasher = hashlib.sha256(_envelope_key_prefix(envelope))
        for snapshot in snapshots:
            key = _fast_request_key(envelope, suffixes[snapshot.model_id], envelope_hasher=env_hasher)
            slot = (unit.unit_id, snapshot.model_id)
            if slots.setdefault(slot, key) != key:
                raise ValueError(f"unit {unit.unit_id} appears twice with different envelopes")
            if key not in requests:
                requests[key] = _Request(
                    key, unit, envelope, snapshot, max_tokens, schema_name, schema_version, schema_sha256
                )
    return slots, requests


def _plan_unit_requests(
    kept_units: Sequence[Unit],
    envelopes: Sequence[Envelope],
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    req_meta: tuple[int, str, str, str],
    tracker: _CacheProbeTracker,
) -> _PreflightPlan:
    """Build a :class:`_PreflightPlan` for ``kept_units`` × ``snapshots`` while enforcing ``tracker.max_requests``."""
    slots, requests = _key_unit_requests(kept_units, envelopes, snapshots, suffixes, req_meta)
    tracker.prewarm(tuple(requests))
    lookups: dict[str, CacheLookup] = {}
    for key in requests:
        tracker.probe(key, lookups)
    return _PreflightPlan(slots=slots, requests=tuple(requests.values()), file_lookups=lookups)


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


@dataclass(frozen=True, slots=True)
class _PackedDoc:
    """Packed units, envelopes, skipped units, and hidden counts for one :class:`PDFDocument`."""

    doc: PDFDocument
    kept_units: tuple[Unit, ...]
    envelopes: tuple[Envelope, ...]
    render_skipped: tuple[SkippedUnit, ...]
    plan_skipped: tuple[SkippedUnit, ...]
    hidden_total: tuple[tuple[Any, int], ...]


def _pack_one_document(
    doc: PDFDocument,
    unit_opts: tuple[int, bool, bool, bool],
    prompt_text: str,
    context: object,
    wrapper_json: str,
    strict: bool,
) -> _PackedDoc:
    """Pack units and build envelopes for ``doc`` without mutating shared accumulator state."""
    max_unit_chars, coalesce_sections, include_schema, multimodal = unit_opts
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
        strict=strict,
    )
    return _PackedDoc(doc, kept_units, envelopes, render_skipped, plan.skipped, plan.hidden_excluded_total)


def _stage_packed_doc(
    acc: _WorkAccumulator,
    packed: _PackedDoc,
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    req_meta: tuple[int, str, str, str],
) -> None:
    """Stage one :class:`_PackedDoc` and its request keys into ``acc`` prior to cache probing."""
    acc.docs_by_id[packed.doc.metadata.doc_id] = packed.doc
    acc.skipped.extend(packed.render_skipped)
    slots, reqs = _key_unit_requests(packed.kept_units, packed.envelopes, snapshots, suffixes, req_meta)
    for slot, key in slots.items():
        if acc.slots.setdefault(slot, key) != key:
            raise ValueError(f"unit {slot[0]} appears twice with different envelopes")
    for key, req in reqs.items():
        acc.requests.setdefault(key, req)
    for unit, env in zip(packed.kept_units, packed.envelopes, strict=True):
        acc.units_by_id[unit.unit_id] = unit
        acc.envelopes_by_id[unit.unit_id] = env
        acc.work.append((unit, env))
    acc.skipped.extend(packed.plan_skipped)
    for reason, count in packed.hidden_total:
        acc.hidden_counts[reason] = acc.hidden_counts.get(reason, 0) + count


def _stage_docs_into_accumulator(
    docs_by_id: Mapping[str, PDFDocument],
    acc: _WorkAccumulator,
    *,
    unit_opts: tuple[int, bool, bool, bool],
    prompt_text: str,
    context: object,
    prepared: PreparedSchema,
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    req_meta: tuple[int, str, str, str],
    strict: bool,
) -> None:
    """Pack ``docs_by_id`` (in bounded parallel when ``len > 1``) and stage keyed requests into ``acc``."""
    docs = tuple(docs_by_id.values())
    packed_docs = _parallel_map(
        docs,
        lambda d: _pack_one_document(d, unit_opts, prompt_text, context, prepared.wrapper_json, strict),
        min_parallel=1,
    )
    for packed in packed_docs:
        _stage_packed_doc(acc, packed, snapshots, suffixes, req_meta)


def _pack_docs_into_accumulator(
    docs_by_id: Mapping[str, PDFDocument],
    acc: _WorkAccumulator,
    *,
    unit_opts: tuple[int, bool, bool, bool],
    prompt_text: str,
    context: object,
    prepared: PreparedSchema,
    snapshots: Sequence[ProviderSnapshot],
    suffixes: Mapping[str, bytes],
    req_meta: tuple[int, str, str, str],
    strict: bool,
    tracker: _CacheProbeTracker,
) -> None:
    """Pack units, build envelopes, pre-warm ``FileCache``, and probe requests across ``docs_by_id``."""
    _stage_docs_into_accumulator(
        docs_by_id,
        acc,
        unit_opts=unit_opts,
        prompt_text=prompt_text,
        context=context,
        prepared=prepared,
        snapshots=snapshots,
        suffixes=suffixes,
        req_meta=req_meta,
        strict=strict,
    )
    keys = tuple(acc.requests)
    tracker.prewarm(keys)
    for key in keys:
        tracker.probe(key, acc.file_lookups)


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
    docs_by_id = _resolve_all_documents(raw_targets)
    acc = _WorkAccumulator.empty()
    suffixes = _suffixes_by_model(snapshots, limits.max_output_tokens, schema_version)
    req_meta = (limits.max_output_tokens, prepared.model.__name__, schema_version, prepared.sha256)
    tracker = _CacheProbeTracker(file_cache, None, {}, set(), limits.max_requests)
    _pack_docs_into_accumulator(
        docs_by_id,
        acc,
        unit_opts=(max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal),
        prompt_text=prompt_text,
        context=context,
        prepared=prepared,
        snapshots=snapshots,
        suffixes=suffixes,
        req_meta=req_meta,
        strict=limits.strict,
        tracker=tracker,
    )
    return acc.freeze()


# ---------------------------------------------------------------------------------------------------------------
# Step 3: Truncation bisection & multi-pass execution
# ---------------------------------------------------------------------------------------------------------------


def _sum_usage(
    snapshots: Sequence[ProviderSnapshot],
    parent_usage: Mapping[str, ModelUsage],
    child_usage: Mapping[str, ModelUsage],
) -> dict[str, ModelUsage]:
    """Sum parent and child :class:`ModelUsage` counters in provider order."""
    merged: dict[str, ModelUsage] = {}
    for snap in snapshots:
        mid = snap.model_id
        p = parent_usage[mid]
        c = child_usage[mid]
        merged[mid] = ModelUsage(
            requests=p.requests + c.requests,
            retries=p.retries + c.retries,
            input_tokens=p.input_tokens + c.input_tokens,
            output_tokens=p.output_tokens + c.output_tokens,
            latency_ms=p.latency_ms + c.latency_ms,
        )
    return merged


def _merge_bisected_outcome(
    parent_outcome: RunOutcome,
    child_outcome: RunOutcome,
    new_units: Mapping[str, Unit],
    replaced_uids: set[str],
    snapshots: Sequence[ProviderSnapshot],
) -> RunOutcome:
    """Drop replaced parent slots while ordering child+surviving slots and summing telemetry counters."""
    responses: dict[tuple[str, str], ProviderResponse] = {}
    failed_by_slot: dict[tuple[str, str], FailedUnit] = {
        (f.unit_id, f.model_id): f for f in parent_outcome.failed_units if f.unit_id not in replaced_uids
    }
    for f in child_outcome.failed_units:
        failed_by_slot[(f.unit_id, f.model_id)] = f
    failed_ordered: list[FailedUnit] = []
    for uid in new_units:
        for snap in snapshots:
            slot = (uid, snap.model_id)
            resp = child_outcome.responses.get(slot) or parent_outcome.responses.get(slot)
            if resp is not None:
                responses[slot] = resp
            if slot in failed_by_slot:
                failed_ordered.append(failed_by_slot[slot])
    return RunOutcome(
        responses=responses,
        failed_units=tuple(failed_ordered),
        cache_hits=parent_outcome.cache_hits + child_outcome.cache_hits,
        cache_misses=parent_outcome.cache_misses + child_outcome.cache_misses,
        cache_corrupt=parent_outcome.cache_corrupt + child_outcome.cache_corrupt,
        cache_write_failures=parent_outcome.cache_write_failures + child_outcome.cache_write_failures,
        usage=_sum_usage(snapshots, parent_outcome.usage, child_outcome.usage),
        models=parent_outcome.models,
    )


def _splice_units(
    units_by_id: Mapping[str, Unit],
    envelopes_by_id: Mapping[str, Envelope],
    replacements: Mapping[str, Sequence[tuple[Unit, Envelope]]],
) -> tuple[dict[str, Unit], dict[str, Envelope]]:
    """Splice bisected child ``(Unit, Envelope)`` pairs in place of their parent units in document order."""
    new_units: dict[str, Unit] = {}
    new_envs: dict[str, Envelope] = {}
    for uid, unit in units_by_id.items():
        if uid not in replacements:
            new_units[uid] = unit
            new_envs[uid] = envelopes_by_id[uid]
            continue
        for c_unit, c_env in replacements[uid]:
            new_units[c_unit.unit_id] = c_unit
            new_envs[c_unit.unit_id] = c_env
    return new_units, new_envs


@dataclass(slots=True)
class _PassRuntime:
    """Mutable state for one pass across initial execution and truncation-bisection rounds."""

    pass_name: str
    spec: PassSpec[Any]
    prepared: PreparedSchema
    checked_must_agree: tuple[str, ...]
    prompt_text: str
    snapshots: tuple[ProviderSnapshot, ...]
    suffixes: dict[str, bytes]
    eff_min_families: int
    eff_split: bool
    docs_by_id: Mapping[str, PDFDocument]
    units_by_id: dict[str, Unit]
    envelopes_by_id: dict[str, Envelope]
    skipped_units: list[SkippedUnit]
    hidden_counts: dict[str, int]
    seen_unit_ids: set[str]
    active_unit_ids: tuple[str, ...]
    preflight: _PreflightPlan
    outcome: RunOutcome | None = None

    def to_run_spec(self) -> PassRunSpec:
        work = tuple((u, self.envelopes_by_id[uid]) for uid, u in self.units_by_id.items())
        return PassRunSpec(
            pass_name=self.pass_name,
            work=work,
            providers=self.snapshots,
            schema=self.prepared,
            schema_version=self.spec.schema_version,
            max_output_tokens=self.spec.max_output_tokens,
            preflight=self.preflight,
        )

    def to_target_work(self) -> _TargetWork:
        work = tuple((u, self.envelopes_by_id[uid]) for uid, u in self.units_by_id.items())
        return _TargetWork(
            docs_by_id=self.docs_by_id,
            units_by_id=self.units_by_id,
            envelopes_by_id=self.envelopes_by_id,
            work=work,
            skipped_units=tuple(self.skipped_units),
            hidden_excluded_by_reason=self.hidden_counts,
            preflight=self.preflight,
        )


def _find_pass_replacements(
    rt: _PassRuntime,
    strict: bool,
) -> dict[str, list[tuple[Unit, Envelope]]]:
    """Bisect truncated active units of ``rt`` and build their child envelopes."""
    if not rt.eff_split or not rt.active_unit_ids or rt.outcome is None:
        return {}
    replacements: dict[str, list[tuple[Unit, Envelope]]] = {}
    for uid in rt.active_unit_ids:
        truncated = any(
            (resp := rt.outcome.responses.get((uid, s.model_id))) is not None and resp.finish == "truncated"
            for s in rt.snapshots
        )
        if not truncated:
            continue
        unit = rt.units_by_id[uid]
        doc = rt.docs_by_id[unit.doc_id]
        split = bisect_unit(
            unit, doc, render=render_element, multimodal=rt.spec.multimodal, existing_unit_ids=rt.seen_unit_ids
        )
        if split is None:
            continue
        rt.seen_unit_ids.update((split[0].unit_id, split[1].unit_id))
        kept, envs, r_skipped = _safe_doc_envelopes(
            split,
            doc,
            prompt_text=rt.prompt_text,
            context=rt.spec.context,
            wrapper_json=rt.prepared.wrapper_json,
            coalesce_sections=rt.spec.coalesce_sections,
            include_schema=rt.spec.include_schema_in_prompt,
            multimodal=rt.spec.multimodal,
            strict=strict,
        )
        rt.skipped_units.extend(r_skipped)
        replacements[uid] = list(zip(kept, envs, strict=True))
    return replacements


def _build_round_spec(
    rt: _PassRuntime,
    replacements: Mapping[str, Sequence[tuple[Unit, Envelope]]],
    tracker: _CacheProbeTracker,
) -> PassRunSpec | None:
    """Create a :class:`PassRunSpec` for ``rt``'s bisected child units, or ``None`` if empty."""
    child_units = tuple(u for pairs in replacements.values() for u, _ in pairs)
    child_envs = tuple(e for pairs in replacements.values() for _, e in pairs)
    if not child_units:
        return None
    req_meta = (
        rt.spec.max_output_tokens,
        rt.prepared.model.__name__,
        rt.spec.schema_version,
        rt.prepared.sha256,
    )
    child_preflight = _plan_unit_requests(child_units, child_envs, rt.snapshots, rt.suffixes, req_meta, tracker)
    return PassRunSpec(
        pass_name=rt.pass_name,
        work=tuple(zip(child_units, child_envs, strict=True)),
        providers=rt.snapshots,
        schema=rt.prepared,
        schema_version=rt.spec.schema_version,
        max_output_tokens=rt.spec.max_output_tokens,
        preflight=child_preflight,
    )


def _run_truncation_rounds(
    runtimes: Sequence[_PassRuntime],
    tracker: _CacheProbeTracker,
    *,
    memory_cache: MemoryCache,
    file_cache: FileCache | None,
    limits: RunLimits,
    on_event: EventHook | None,
    spent_misses: int,
) -> None:
    """Iteratively bisect truncated units across ``runtimes`` up to :data:`MAX_TRUNCATION_ROUNDS`."""
    tracker.memory_cache = memory_cache
    for _ in range(MAX_TRUNCATION_ROUNDS):
        by_pass: dict[str, dict[str, list[tuple[Unit, Envelope]]]] = {}
        round_specs: list[PassRunSpec] = []
        for rt in runtimes:
            repl = _find_pass_replacements(rt, limits.strict)
            if not repl:
                rt.active_unit_ids = ()
                continue
            by_pass[rt.pass_name] = repl
            spec = _build_round_spec(rt, repl, tracker)
            if spec is not None:
                round_specs.append(spec)
        if not by_pass:
            return
        round_limits = dataclasses.replace(limits, max_requests=max(1, limits.max_requests - spent_misses))
        outcomes = (
            run_multi_requests(
                round_specs,
                memory_cache=memory_cache,
                file_cache=file_cache,
                limits=round_limits,
                on_event=on_event,
            )
            if round_specs
            else {}
        )
        spent_misses += sum(o.cache_misses for o in outcomes.values())
        for rt in runtimes:
            if rt.pass_name not in by_pass or rt.outcome is None:
                continue
            repl = by_pass[rt.pass_name]
            rt.units_by_id, rt.envelopes_by_id = _splice_units(rt.units_by_id, rt.envelopes_by_id, repl)
            empty_out = RunOutcome(
                {}, (), 0, 0, 0, 0, {s.model_id: ModelUsage(0, 0, 0, 0, 0.0) for s in rt.snapshots}, rt.outcome.models
            )
            child_out = outcomes.get(rt.pass_name, empty_out)
            rt.outcome = _merge_bisected_outcome(rt.outcome, child_out, rt.units_by_id, set(repl), rt.snapshots)
            rt.active_unit_ids = tuple(u.unit_id for pairs in repl.values() for u, _ in pairs)


# ---------------------------------------------------------------------------------------------------------------
# Step 4: Post-stages & result assembly
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
        min_families=min_families,
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
    pass_label: str | None = None,
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
        if pass_label is not None:
            raise RationalizationError(
                f"expect_non_empty=True on pass {pass_label!r}, but rationalize_many() produced no records"
            )
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
# Step 5: Shared multi-pass orchestrator & public entry points
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _PreparedPass:
    """Zero-I/O validated pass specification ready for provider snapshotting and document packing."""

    pass_name: str
    spec: PassSpec[Any]
    prepared: PreparedSchema
    checked_must_agree: tuple[str, ...]
    prompt_text: str
    eff_min_families: int
    eff_split: bool


def _preflight_all_passes(
    normalized_passes: Sequence[tuple[str, PassSpec[Any]]],
    *,
    default_min_families: int,
    default_split: bool,
) -> tuple[_PreparedPass, ...]:
    """Validate schemas, ``must_agree``, prompts, and contexts across all passes before I/O."""
    out: list[_PreparedPass] = []
    for pass_name, spec in normalized_passes:
        eff_min = default_min_families if spec.min_families is None else spec.min_families
        eff_split = default_split if spec.split_on_truncation is None else spec.split_on_truncation
        prepared = prepare(spec.schema, key=spec.key)
        checked_must_agree = check_must_agree(prepared.spec, spec.must_agree)
        prompt_text = _read_prompt(spec.prompt)
        _context_json(spec.context)
        _check_schema(prepared.wrapper_json)
        out.append(_PreparedPass(pass_name, spec, prepared, checked_must_agree, prompt_text, eff_min, eff_split))
    return tuple(out)


def _stage_pass_accumulator(
    prep: _PreparedPass,
    snapshots: tuple[ProviderSnapshot, ...],
    docs_by_id: Mapping[str, PDFDocument],
    *,
    strict: bool,
) -> tuple[_WorkAccumulator, dict[str, bytes]]:
    """Stage packed documents and request keys for ``prep`` prior to cross-pass ``FileCache`` pre-warming."""
    spec = prep.spec
    suffixes = _suffixes_by_model(snapshots, spec.max_output_tokens, spec.schema_version)
    req_meta = (spec.max_output_tokens, prep.prepared.model.__name__, spec.schema_version, prep.prepared.sha256)
    acc = _WorkAccumulator.empty()
    _stage_docs_into_accumulator(
        docs_by_id,
        acc,
        unit_opts=(spec.max_unit_chars, spec.coalesce_sections, spec.include_schema_in_prompt, spec.multimodal),
        prompt_text=prep.prompt_text,
        context=spec.context,
        prepared=prep.prepared,
        snapshots=snapshots,
        suffixes=suffixes,
        req_meta=req_meta,
        strict=strict,
    )
    return acc, suffixes


def _finalize_pass_runtime(
    prep: _PreparedPass,
    snapshots: tuple[ProviderSnapshot, ...],
    docs_by_id: Mapping[str, PDFDocument],
    acc: _WorkAccumulator,
    suffixes: dict[str, bytes],
    tracker: _CacheProbeTracker,
) -> _PassRuntime:
    """Probe staged requests in deterministic order and freeze into a :class:`_PassRuntime`."""
    for key in acc.requests:
        tracker.probe(key, acc.file_lookups)
    tw = acc.freeze()
    seen_ids = set(acc.units_by_id) | {s.unit_id for s in acc.skipped}
    return _PassRuntime(
        pass_name=prep.pass_name,
        spec=prep.spec,
        prepared=prep.prepared,
        checked_must_agree=prep.checked_must_agree,
        prompt_text=prep.prompt_text,
        snapshots=snapshots,
        suffixes=suffixes,
        eff_min_families=prep.eff_min_families,
        eff_split=prep.eff_split,
        docs_by_id=docs_by_id,
        units_by_id=acc.units_by_id,
        envelopes_by_id=acc.envelopes_by_id,
        skipped_units=acc.skipped,
        hidden_counts=acc.hidden_counts,
        seen_unit_ids=seen_ids,
        active_unit_ids=tuple(acc.units_by_id),
        preflight=tw.preflight,
    )


def _pack_all_pass_runtimes(
    prepared_passes: Sequence[_PreparedPass],
    snapshots_by_pass: Sequence[tuple[ProviderSnapshot, ...]],
    docs_by_id: Mapping[str, PDFDocument],
    tracker: _CacheProbeTracker,
    *,
    strict: bool,
) -> list[_PassRuntime]:
    """Stage all passes, pre-warm ``FileCache`` once across all unique keys, and finalize runtimes."""
    staged = [
        _stage_pass_accumulator(p, snaps, docs_by_id, strict=strict)
        for p, snaps in zip(prepared_passes, snapshots_by_pass, strict=True)
    ]
    tracker.prewarm([key for acc, _ in staged for key in acc.requests])
    return [
        _finalize_pass_runtime(p, snaps, docs_by_id, acc, suffixes, tracker)
        for (p, snaps), (acc, suffixes) in zip(
            zip(prepared_passes, snapshots_by_pass, strict=True), staged, strict=True
        )
    ]


def _run_and_assemble_runtimes(
    runtimes: Sequence[_PassRuntime],
    tracker: _CacheProbeTracker,
    *,
    memory_cache: MemoryCache,
    file_cache: FileCache | None,
    limits: RunLimits,
    on_event: EventHook | None,
    is_many: bool,
) -> dict[str, RationalizedResult[Any]]:
    """Execute initial requests, run bisection rounds, and assemble per-pass results."""
    initial_specs = [rt.to_run_spec() for rt in runtimes]
    outcomes = run_multi_requests(
        initial_specs, memory_cache=memory_cache, file_cache=file_cache, limits=limits, on_event=on_event
    )
    for rt in runtimes:
        rt.outcome = outcomes[rt.pass_name]
    spent = sum(o.cache_misses for o in outcomes.values())
    _run_truncation_rounds(
        runtimes,
        tracker,
        memory_cache=memory_cache,
        file_cache=file_cache,
        limits=limits,
        on_event=on_event,
        spent_misses=spent,
    )
    return {
        rt.pass_name: _assemble_pipeline_result(
            rt.to_target_work(),
            cast(RunOutcome, rt.outcome),
            rt.prepared,
            min_families=rt.eff_min_families,
            must_agree=rt.checked_must_agree,
            expect_non_empty=rt.spec.expect_non_empty,
            fold_across_units=rt.spec.fold_across_units,
            multimodal=rt.spec.multimodal,
            pass_label=rt.pass_name if is_many else None,
        )
        for rt in runtimes
    }


def _execute_prepared_passes(
    target: TargetInput | Sequence[TargetInput],
    prepared_passes: Sequence[_PreparedPass],
    models: Sequence[ModelInput],
    *,
    cache_dir: str | PathLike[str] | None,
    limits: RunLimits,
    on_event: EventHook | None,
    is_many: bool,
) -> dict[str, RationalizedResult[Any]]:
    """Resolve providers and documents once, execute all passes + bisection, and assemble results."""
    providers, closers = _resolve_providers_with_closers(models, persistent_cache=cache_dir is not None)
    try:
        snapshots_by_pass = [snapshot_providers(providers, p.prepared) for p in prepared_passes]
        for p, snaps in zip(prepared_passes, snapshots_by_pass, strict=True):
            _check_snapshot_families(snaps, p.eff_min_families)
        file_cache = FileCache(cache_dir) if cache_dir is not None else None
        docs_by_id = _resolve_all_documents(_normalize_targets(target))
        tracker = _CacheProbeTracker(file_cache, None, {}, set(), limits.max_requests)
        runtimes = _pack_all_pass_runtimes(
            prepared_passes, snapshots_by_pass, docs_by_id, tracker, strict=limits.strict
        )
        return _run_and_assemble_runtimes(
            runtimes,
            tracker,
            memory_cache=MemoryCache(),
            file_cache=file_cache,
            limits=limits,
            on_event=on_event,
            is_many=is_many,
        )
    finally:
        _close_ephemeral(closers)


def rationalize_many(
    target: TargetInput | Sequence[TargetInput],
    *,
    passes: Mapping[str, PassSpec[Any]] | Sequence[PassSpec[Any]],
    models: Sequence[ModelInput],
    min_families: int = 2,
    cache_dir: str | PathLike[str] | None = None,
    max_concurrency: int = 8,
    max_concurrency_per_provider: int = 4,
    rpm_limit: int | None = None,
    timeout_s: float = 60.0,
    max_retries: int = 2,
    max_requests: int = 1_000,
    strict: bool = False,
    split_on_truncation: bool = True,
    event_delivery: EventDelivery = "ordered",
    on_event: EventHook | None = None,
) -> dict[str, RationalizedResult[Any]]:
    """Execute multiple schema extraction passes over one target load and one shared provider pool."""
    limits = _check_scalar_params(
        max_concurrency=max_concurrency,
        max_concurrency_per_provider=max_concurrency_per_provider,
        rpm_limit=rpm_limit,
        timeout_s=timeout_s,
        max_retries=max_retries,
        max_unit_chars=1,
        max_requests=max_requests,
        max_output_tokens=8_192,
        schema_version="1.0",
        expect_non_empty=False,
        strict=strict,
        coalesce_sections=True,
        split_on_truncation=split_on_truncation,
        event_delivery=event_delivery,
        on_event=on_event,
        min_families=min_families,
    )
    normalized = _normalize_passes(passes)
    prepared_passes = _preflight_all_passes(
        normalized, default_min_families=min_families, default_split=split_on_truncation
    )
    return _execute_prepared_passes(
        target,
        prepared_passes,
        models,
        cache_dir=cache_dir,
        limits=limits,
        on_event=on_event,
        is_many=True,
    )


def _prepare_single_pass(
    schema: type[Any],
    prompt: str | Path,
    key: str | Sequence[str] | None,
    must_agree: Sequence[str],
    context: object,
    u_opts: tuple[int, bool, bool, bool],
    r_opts: tuple[int, int, str, bool, bool, bool],
) -> _PreparedPass:
    """Validate and construct a :class:`_PreparedPass` for :func:`rationalize`."""
    max_unit_chars, coalesce_sections, include_schema, multimodal = u_opts
    max_output_tokens, min_families, schema_version, expect_non_empty, fold_across_units, split_on_trunc = r_opts
    prepared = prepare(schema, key=key)
    checked_must_agree = check_must_agree(prepared.spec, must_agree)
    prompt_text = _read_prompt(prompt)
    _context_json(context)
    _check_schema(prepared.wrapper_json)
    spec = PassSpec(
        schema=schema,
        prompt=prompt,
        schema_version=schema_version,
        key=key,
        must_agree=checked_must_agree,
        context=context,
        max_unit_chars=max_unit_chars,
        max_output_tokens=max_output_tokens,
        min_families=min_families,
        expect_non_empty=expect_non_empty,
        coalesce_sections=coalesce_sections,
        include_schema_in_prompt=include_schema,
        multimodal=multimodal,
        fold_across_units=fold_across_units,
        split_on_truncation=split_on_trunc,
    )
    return _PreparedPass("_single", spec, prepared, checked_must_agree, prompt_text, min_families, split_on_trunc)


_SINGLE_BOOL_NAMES = (
    "expect_non_empty",
    "coalesce_sections",
    "include_schema_in_prompt",
    "multimodal",
    "fold_across_units",
    "split_on_truncation",
)


def _single_limits(
    lim_args: tuple[int, int, int | None, float, int, int, int, bool, EventDelivery],
    max_unit_chars: int,
    schema_version: str,
    on_event: EventHook | None,
    min_families: int,
    bool_vals: tuple[object, ...],
) -> RunLimits:
    """Validate scalar bounds and boolean flags for :func:`rationalize`."""
    c_max, c_prov, rpm, timeout_s, retries, out_toks, max_reqs, strict, delivery = lim_args
    limits = RunLimits(
        max_concurrency=c_max,
        max_concurrency_per_provider=c_prov,
        rpm_limit=rpm,
        timeout_s=timeout_s,
        max_retries=retries,
        max_output_tokens=out_toks,
        max_requests=max_reqs,
        strict=strict,
        event_delivery=delivery,
    )
    flags = tuple(zip(_SINGLE_BOOL_NAMES, bool_vals, strict=True))
    _check_extra_scalars(max_unit_chars, schema_version, on_event, min_families, flags)
    return limits


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
    split_on_truncation: bool = True,
    event_delivery: EventDelivery = "ordered",
    on_event: EventHook | None = None,
) -> RationalizedResult[T]:
    """Extract, ground, reach cross-family consensus on, and fold schema records over v5 PDF documents (plan §4.11)."""
    lim_a = (max_concurrency, max_concurrency_per_provider, rpm_limit, timeout_s, max_retries)
    lim_b = (max_output_tokens, max_requests, strict, event_delivery)
    bool_a = (expect_non_empty, coalesce_sections, include_schema_in_prompt)
    bool_b = (multimodal, fold_across_units, split_on_truncation)
    limits = _single_limits(
        (*lim_a, *lim_b), max_unit_chars, schema_version, on_event, min_families, (*bool_a, *bool_b)
    )
    u_opts = (max_unit_chars, coalesce_sections, include_schema_in_prompt, multimodal)
    r_opts = (max_output_tokens, min_families, schema_version, expect_non_empty, fold_across_units, split_on_truncation)
    prep = _prepare_single_pass(schema, prompt, key, must_agree, context, u_opts, r_opts)
    res = _execute_prepared_passes(
        target, (prep,), models, cache_dir=cache_dir, limits=limits, on_event=on_event, is_many=False
    )
    return cast(RationalizedResult[T], res["_single"])
