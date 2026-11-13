"""The runner: turns units × providers into model requests and returns their responses (plan §4.6, R11).

A run has two steps. :func:`snapshot_providers` reads each provider once into a :class:`ProviderSnapshot`, so the
caller can check its families (``min_families``) on the snapshots before anything is sent. :func:`run_requests`
takes the run's ``(Unit, Envelope)`` pairs in document order, the snapshots in the caller's order, the prepared
schema and a cache configuration. It returns a :class:`RunOutcome`: each (unit, model) response, from a cache or a
provider, the requests that failed, and the run's counters. The bounds are a :class:`RunLimits`. The rules, in order:

1. **Snapshots** (:func:`snapshot_providers`, the caller's thread). Each provider passes
   :func:`~datagrunt.rationalization.providers.base.check_provider` and is then read once: ``model_id``,
   ``provider_kind``, ``rate_key``, ``model_family``, ``generation_params``, its bound ``generate``, its optional
   ``open`` and ``max_connections``, and ``transform_schema`` of a fresh copy of the response wrapper (each provider
   gets its own, so a transform that changes its input changes no other's). The values read are checked themselves,
   and nothing of the provider is read again; the snapshot keeps its JSON as canonical text and hands out fresh
   copies, so nothing changes it afterwards. Build snapshots only this way: one built by hand is checked field by
   field, but nothing can check that its identity is its ``generate``'s. A provider whose own private state changes
   after its snapshot (a new temperature, say) sends with the new state under the old cache key: keeping its state
   still is the caller's responsibility. Two providers with one model id, and ``generation_params`` holding
   ``max_output_tokens``, are refused.
2. **Preflight** (:func:`run_requests`, the caller's thread, before any request). Every argument is checked (type and
   range): each envelope must be its unit's and carry the run's schema, each snapshot must be made for that schema.
   Each (unit, provider) gets its key (:func:`~datagrunt.rationalization.cache.request_key`, the request's
   ``max_output_tokens`` merged into ``generation_params``); identical keys make one request; one (unit id, model
   id) with two keys (a unit twice with different envelopes) is refused. The caches are probed, the memory cache
   first: a hit is a ``cache_hit`` event, a corrupt file entry a ``cache_corrupt`` event and a miss. More distinct
   misses than ``max_requests`` raise :class:`~datagrunt.rationalization.RationalizationError` before any request
   and before any ``open()``; then ``open()`` runs for each provider with a miss to send.
3. **Scheduling.** A pool of ``min(max_concurrency, misses)`` threads. The caller submits a miss only into a free slot
   of its rate key, a :class:`threading.BoundedSemaphore` of ``max_concurrency_per_provider`` cut to the smallest
   ``max_connections`` among the providers sharing the key (each has its own HTTP pool), taken on the caller's
   thread and released by the worker; with ``rpm_limit``, only with the key's token taken too, due now: a key's
   bucket has capacity one, one token per ``60 / rpm_limit`` s, and the caller waits for the earliest due token
   while nothing it could send has one. The earliest miss of any key with a free slot and a token goes first, so a
   busy key never holds back another, and at most ``max_concurrency`` run at once. A submitted request starts at
   once, at a real send, and its unit deadline with it (:meth:`RunLimits.unit_deadline_s`):
   ``(max_retries + 1) * (timeout_s + ATTEMPT_SLACK_S) + max_retries * RETRY_AFTER_CAP_S``, plus, with
   ``rpm_limit``, ``max_retries * slots * 60 / rpm_limit`` for its retries' tokens (a retry's token waits behind at
   most one token per slot of its key). :data:`ATTEMPT_SLACK_S` (2 s) covers what an attempt spends outside its HTTP
   deadline: rendering and building the request (a canonical copy of a schema up to 1 MiB), parsing a reply of up to
   8 MiB after the HTTP helper's cut, and thread scheduling; an adapter's token fetch spends the attempt's own time.
   Token spacing holds when a token is taken, not at the send itself, so a minute can see ``rpm_limit + 1`` sends.
4. **Attempts**, at most ``max_retries + 1``. A retry first reserves its key's next token, whose wait never passes
   the deadline. The attempt's ``ModelRequest.timeout_s`` is ``min(timeout_s, time left)``; with less than
   :data:`MIN_ATTEMPT_S` left, or a retry's token past it, nothing is sent and the request fails, ``"deadline
   passed"``. Each attempt draws a fresh nonce, redrawn while any component it sends (the prompt, the schema, the
   context, the payload) holds it, at most :data:`NONCE_DRAWS` times.
5. **Errors.** A :class:`~datagrunt.rationalization.providers.base.TransientProviderError` is retried after
   ``min(retry_after_s, 30)`` s when the provider gave one, else full jitter ``uniform(0, min(20, 0.75 * 2**n))`` for
   attempt ``n``'s failure; a wait that would leave the next attempt less than the floor is not waited (``"deadline
   passed"``). A :class:`~datagrunt.rationalization.providers.base.PermanentProviderError` raises
   ``RationalizationError``, naming the provider, the unit and the status, with the bounded detail, chained. An
   answer that is no ``ProviderResponse`` is a ``TypeError``; any other exception propagates unchanged.
6. **Stopping.** Any of these, a strict failure (7), an exception from ``on_event`` (a ``KeyboardInterrupt``
   included) and a request past its bound (11) stop the run. Its stop flag is set, which ends every wait at once (the
   default ``sleep`` waits on it), and no attempt or request starts after it; queued futures are cancelled; the
   requests still in flight are waited for, each only until its current attempt ends (its send, ``timeout_s`` and
   :data:`ATTEMPT_SLACK_S`, which each worker records as it sends), or its unit deadline when that comes first or it
   has sent nothing yet, plus :data:`STOP_GRACE_S`; the pool is joined when they are all done, else shut down without
   joining the call still running. A second ``KeyboardInterrupt`` during that wait abandons the wait, and the
   original failure with it: the interrupt propagates, and the pool is left to the interpreter. Then the run raises:
   the caller's own exception as it is, else the failure of the earliest request (unit order, then provider order)
   among those that raised by then, so racing failures surface deterministically given which finished. Events not
   yet delivered are dropped.
7. **Failure.** A request whose retries are spent or whose deadline passed raises ``RationalizationError`` under
   ``strict``; otherwise it is a :class:`FailedUnit` (its message: the reason, the attempts, the last provider error
   with its bounded detail) and a ``unit_failed`` event, and the run goes on.
8. **Success.** Every response, ``truncated`` and ``refused`` ones included, goes into the memory cache, then the
   file cache when there is one. A store that keeps nothing (the memory cache full, an entry past 8 MiB,
   ``FileCache.put`` raising ``RationalizationError``) is a ``cache_write_failed`` event and counts; the run keeps
   the response.
9. **Events** (:class:`TelemetryEvent`) reach ``on_event`` on the caller's thread only: preflight's as they are found,
   the workers' through a queue the caller drains between bounded ``concurrent.futures.wait`` calls (at most
   :data:`POLL_S`), in the misses' order (every event of one request, as it comes, before any of the next one's), so
   their order never depends on which worker finishes first. Delivery is ordered, not live: a long request holds back
   the events of every request after it. ``on_event`` is the caller's code and runs on the caller's thread, so one
   that blocks stalls the run (no request is submitted or collected meanwhile). A detail is a fixed word or phrase, a
   finish reason, an error's class, a wait, a store, a corrupt entry's reason or a cache error's message (paths and
   the OS error): never document text, a provider's error text or a secret.
10. **Assembly**, on the caller's thread: the responses keyed by ``(unit_id, model_id)`` in unit order then provider
    order, the failed units in the same order, the cache counters, each model's :class:`ModelUsage` in provider order
    (attempts, retries, the tokens fresh responses report, summed latency), and each model's id, family and kind.
    Worker completion order never reaches it.
11. **Each request's own bound.** A request still in flight past its submission plus its unit deadline plus
    :data:`RUN_GRACE_S` (a call no deadline can cut: a custom provider's, a token source's, a hung file system's)
    makes the run raise ``RationalizationError`` there (6), abandoning only that worker. The whole unit deadline
    applies here, not the current attempt's, since a live run may start more attempts; a request that finished since
    the caller last looked (while ``on_event`` held the caller's thread, say) is never counted. Its thread is a
    ``concurrent.futures`` worker, which is not a daemon: it ends when its call returns, and until then it can delay
    the interpreter's exit. The caller's loop also takes at most :func:`_wait_bound` waits. That count assumes each
    wait really waits: an injected ``sleep`` must sleep (a test pairs a fake ``sleep`` with a fake ``clock``); a
    ``sleep`` that returns at once, with the real clock and ``rpm_limit``, makes the caller spin through its waits
    until the count ends the run.

Bounds: attempts ``max_retries + 1`` (at most 11); each wait at most 30 s and never past the unit deadline; a first
attempt's token wait at most one token interval per request queued ahead of it on its key; nonce draws
:data:`NONCE_DRAWS`; requests ``max_requests`` (at most :data:`REQUESTS_LIMIT`); threads and requests in flight per
rate key at most :data:`CONCURRENCY_LIMIT`; each request its own bound (11); a stopping run's wait one pass per request
in flight; the caller's loop :func:`_wait_bound` waits. Every other loop is linear in units × providers, and nothing
recurses. The module imports only the standard library and its sibling modules (the schema's for
:class:`~datagrunt.rationalization.schema.PreparedSchema`, so it needs the [rationalize] extra): no httpx, which the
providers own, nothing from ``datagrunt.core``, and no import inside a function, so a worker thread never imports
(invariant 9).
"""

import hashlib
import heapq
import math
import queue
import random
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Literal, NoReturn, Protocol, TypeAlias, cast, get_args

from datagrunt.rationalization._canonical import canonical_copy, canonical_json, sha256_hex, strict_loads
from datagrunt.rationalization.cache import (
    CacheLookup,
    CacheMeta,
    FileCache,
    MemoryCache,
    _envelope_key_prefix,
    _fast_request_key,
    _provider_key_suffix,
)
from datagrunt.rationalization.envelope import Envelope, new_nonce
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.base import (
    DETAIL_MAX_CHARS,
    PermanentProviderError,
    TransientProviderError,
    bounded_detail,
    check_provider,
    family_problem,
    has_real_member,
    identity_problem,
)
from datagrunt.rationalization.schema import PreparedSchema
from datagrunt.rationalization.units import Unit
from datagrunt.rationalization.wire import (
    MAX_OUTPUT_TOKENS,
    ModelRequest,
    ProviderResponse,
    _to_trusted_json,
    _trusted_model_request,
)

EventKind: TypeAlias = Literal["cache_hit", "cache_corrupt", "request", "retry", "unit_failed", "cache_write_failed"]
"""What a :class:`TelemetryEvent` reports."""

EVENT_KINDS: tuple[EventKind, ...] = get_args(EventKind)

MAX_RETRIES_LIMIT = 10
"""The most retries a request may take (plan §5): at most 11 attempts."""

CONCURRENCY_LIMIT = 64
"""The most worker threads (``max_concurrency``) and the most requests in flight per rate key."""

RPM_LIMIT_MAX = 100_000
"""The largest ``rpm_limit``."""

TIMEOUT_MAX_S = 3_600.0
"""The longest ``timeout_s`` one attempt may take."""

REQUESTS_LIMIT = 100_000
"""The largest ``max_requests``: what one :class:`~datagrunt.rationalization.cache.MemoryCache` holds."""

MAX_CONNECTIONS_LIMIT = 1_024
"""The largest ``max_connections`` a provider may expose (the HTTP helper's pool bound)."""

MIN_ATTEMPT_S = 1.0
"""The least time an attempt is sent with: with less left before the unit deadline, the unit fails instead."""

ATTEMPT_SLACK_S = 2.0
"""The time an attempt may take besides its ``timeout_s`` in the unit deadline."""

RETRY_AFTER_CAP_S = 30.0
"""The longest wait a provider's ``retry_after_s`` buys."""

JITTER_CAP_S = 20.0
"""The longest full-jitter wait."""

JITTER_BASE_S = 0.75
"""The full-jitter scale: attempt ``n``'s failure waits ``uniform(0, min(20, 0.75 * 2**n))``."""

NONCE_DRAWS = 8
"""The most nonces an attempt draws before one is absent from what it sends."""

POLL_S = 0.1
"""The caller's longest wait between event deliveries."""

MIN_WAIT_S = 0.01
"""The caller's shortest wait for a token while requests are in flight."""

RUN_GRACE_S = 60.0
"""The run bound's margin past its requests' deadlines."""

STOP_GRACE_S = 1.0
"""How long past its own unit deadline a stopping run waits for a request still in flight."""

_ATTEMPTS_MAX = MAX_RETRIES_LIMIT + 1
_COUNT_MAX = 2**63 - 1  # what an Int64 telemetry column holds
_THREADS = "datagrunt-rationalize"


# ---------------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------------


def _check_id(name: str, value: object) -> str:
    """Require a non-blank ``str``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"{name} must be non-blank")
    return value


def _check_int(name: str, value: object, low: int, high: int) -> int:
    """Require an ``int`` (not a ``bool``) in ``low``-``high``."""
    if type(value) is not int:
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if not low <= value <= high:
        raise ValueError(f"{name} must be in {low}-{high}, got {value}")
    return value


def _check_status(value: object) -> int | None:
    """Require ``None`` or an HTTP status (an ``int`` in 100-599)."""
    return None if value is None else _check_int("status_code", value, 100, 599)


def _check_float(name: str, value: object, low: float, high: float) -> float:
    """Require an ``int`` or ``float`` (not a ``bool``) that is finite and in ``low``-``high``; return it as a float."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be an int or float, got {type(value).__name__}")
    try:
        number = float(value)
    except OverflowError as error:  # an int past the float range
        raise ValueError(f"{name} must be finite") from error
    if not math.isfinite(number) or not low <= number <= high:
        raise ValueError(f"{name} must be finite and in {low}-{high}, got {number}")
    return number


def _check_identity(name: str, value: object) -> str:
    """Require a provider identity (``identity_problem``): a ``TypeError`` for a non-``str``, else a ``ValueError``."""
    problem = identity_problem(value)
    if problem is not None:
        raise (TypeError if not isinstance(value, str) else ValueError)(f"{name} {problem}")
    return cast(str, value)


def _check_digest(name: str, value: object) -> str:
    """Require a SHA-256 hex digest (64 lowercase hex characters)."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be 64 lowercase hex characters")
    return value


def _parse_object(name: str, text: object) -> dict[str, Any]:
    """Parse ``text``, which must be exactly the canonical JSON of an object.

    Raises:
        TypeError: If ``text`` is not a ``str``.
        ValueError: If it is not JSON, not an object, or not canonical (sorted keys, compact, unescaped).
    """
    if not isinstance(text, str):
        raise TypeError(f"{name} must be a str of canonical JSON, got {type(text).__name__}")
    try:
        parsed = strict_loads(text)
        exact = isinstance(parsed, dict) and canonical_json(parsed, subject=name) == text
    except (ValueError, RecursionError, RationalizationError) as error:
        raise ValueError(f"{name} is not JSON: {type(error).__name__}") from error
    if not exact:
        raise ValueError(f"{name} must be the canonical JSON of an object")
    return cast(dict[str, Any], parsed)


def _check_callable(name: str, value: object, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if not callable(value):
        raise TypeError(f"{name} must be callable, got {type(value).__name__}")


def _check_line(name: str, value: object) -> str:
    """Require one bounded line: a ``str`` that ``bounded_detail`` leaves as it is."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if bounded_detail(value) != value:
        raise ValueError(f"{name} must be one line of at most {DETAIL_MAX_CHARS} characters, without padding")
    return value


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """One thing that happened in a run, for ``on_event`` (plan §3): ids, numbers and a fixed detail, no text.

    Args:
        kind: One of :data:`EVENT_KINDS`.
        unit_id: The unit's id.
        model_id: The provider's model id.
        attempt: The attempt it concerns, 0 for a cache event, at most ``MAX_RETRIES_LIMIT + 1``.
        status_code: The HTTP status, or ``None``.
        latency_ms: The attempt's latency in milliseconds, finite and >= 0 (held as a float), or ``None``.
        detail: One line of at most 300 characters (``bounded_detail``-ed): never document text or a secret.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a value is out of range, or ``detail`` is not one bounded line.
    """

    kind: EventKind
    unit_id: str
    model_id: str
    attempt: int
    status_code: int | None
    latency_ms: float | None
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str):
            raise TypeError(f"kind must be a str, got {type(self.kind).__name__}")
        if self.kind not in EVENT_KINDS:
            raise ValueError(f"kind must be one of {EVENT_KINDS}")
        _check_id("unit_id", self.unit_id)
        _check_id("model_id", self.model_id)
        _check_int("attempt", self.attempt, 0, _ATTEMPTS_MAX)
        _check_status(self.status_code)
        if self.latency_ms is not None:
            object.__setattr__(self, "latency_ms", _check_float("latency_ms", self.latency_ms, 0.0, math.inf))
        _check_line("detail", self.detail)


@dataclass(frozen=True, slots=True)
class FailedUnit:
    """A (unit, model) request that ended without a response (retries exhausted, or its deadline passed).

    Args:
        unit_id: The unit's id.
        model_id: The provider's model id.
        status_code: The last attempt's HTTP status, or ``None``.
        error_message: Why it failed: one line of at most 300 characters. It can hold the provider's error text,
            which can echo document text (a provider's error message may quote the request): never a secret.
        attempts: The attempts sent, 0 to ``MAX_RETRIES_LIMIT + 1``.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a value is out of range, or ``error_message`` is blank or not one bounded line.
    """

    unit_id: str
    model_id: str
    status_code: int | None
    error_message: str
    attempts: int

    def __post_init__(self) -> None:
        _check_id("unit_id", self.unit_id)
        _check_id("model_id", self.model_id)
        _check_status(self.status_code)
        _check_id("error_message", self.error_message)
        _check_line("error_message", self.error_message)
        _check_int("attempts", self.attempts, 0, _ATTEMPTS_MAX)


@dataclass(frozen=True, slots=True)
class ModelUsage:
    """One model's counters in a run: the attempts it was sent, their retries, tokens and summed latency.

    Args:
        requests: Attempts sent.
        retries: Retries waited for.
        input_tokens: Input tokens its fresh responses reported (``None`` counts 0).
        output_tokens: Output tokens its fresh responses reported (``None`` counts 0).
        latency_ms: The summed latency of every attempt, in milliseconds, finite and >= 0 (held as a float).

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a value is negative or not finite.
    """

    requests: int
    retries: int
    input_tokens: int
    output_tokens: int
    latency_ms: float

    def __post_init__(self) -> None:
        for name in ("requests", "retries", "input_tokens", "output_tokens"):
            _check_int(name, getattr(self, name), 0, _COUNT_MAX)
        object.__setattr__(self, "latency_ms", _check_float("latency_ms", self.latency_ms, 0.0, math.inf))


def _read_only_responses(value: object) -> Mapping[tuple[str, str], ProviderResponse]:
    """A read-only copy of a ``{(unit_id, model_id): ProviderResponse}`` mapping, every item checked."""
    if not isinstance(value, Mapping):
        raise TypeError(f"responses must be a Mapping, got {type(value).__name__}")
    copy: dict[tuple[str, str], ProviderResponse] = {}
    for slot, response in value.items():
        if not (isinstance(slot, tuple) and len(slot) == 2 and all(isinstance(part, str) for part in slot)):
            raise TypeError("each responses key must be a (unit_id, model_id) tuple of str")
        if not isinstance(response, ProviderResponse):
            raise TypeError(f"each response must be a ProviderResponse, got {type(response).__name__}")
        copy[slot] = response
    return MappingProxyType(copy)


def _read_only_usage(value: object) -> Mapping[str, ModelUsage]:
    """A read-only copy of a ``{model_id: ModelUsage}`` mapping, every item checked."""
    if not isinstance(value, Mapping):
        raise TypeError(f"usage must be a Mapping, got {type(value).__name__}")
    copy: dict[str, ModelUsage] = {}
    for model_id, usage in value.items():
        if not isinstance(model_id, str) or not isinstance(usage, ModelUsage):
            raise TypeError("usage must map each model id (a str) to a ModelUsage")
        copy[model_id] = usage
    return MappingProxyType(copy)


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """What a run returns, assembled in unit order then provider order (never completion order).

    Args:
        responses: Each (unit id, model id)'s response, from a cache or a provider; held as a read-only copy.
        failed_units: The (unit, model) requests that ended without one, in the same order.
        cache_hits: Distinct requests a cache answered.
        cache_misses: Distinct requests sent to a provider (a corrupt entry's included).
        cache_corrupt: Distinct requests whose file-cache entry was corrupt.
        cache_write_failures: Responses a cache did not store.
        usage: Each model's :class:`ModelUsage`, in provider order; held as a read-only copy.
        models: Each provider's ``(model_id, model_family, provider_kind)``, in provider order.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a count is negative.
    """

    responses: Mapping[tuple[str, str], ProviderResponse]
    failed_units: tuple[FailedUnit, ...]
    cache_hits: int
    cache_misses: int
    cache_corrupt: int
    cache_write_failures: int
    usage: Mapping[str, ModelUsage]
    models: tuple[tuple[str, str, str], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "responses", _read_only_responses(self.responses))
        if not isinstance(self.failed_units, tuple) or not all(
            isinstance(item, FailedUnit) for item in self.failed_units
        ):
            raise TypeError("failed_units must be a tuple of FailedUnit")
        for name in ("cache_hits", "cache_misses", "cache_corrupt", "cache_write_failures"):
            _check_int(name, getattr(self, name), 0, _COUNT_MAX)
        object.__setattr__(self, "usage", _read_only_usage(self.usage))
        if not isinstance(self.models, tuple) or not all(
            isinstance(model, tuple) and len(model) == 3 and all(isinstance(part, str) for part in model)
            for model in self.models
        ):
            raise TypeError("models must be a tuple of (model_id, model_family, provider_kind) tuples of str")


@dataclass(frozen=True, slots=True, kw_only=True)
class RunLimits:
    """A run's bounds (plan §5), each checked for its type and range.

    Args:
        max_concurrency: Worker threads, 1-:data:`CONCURRENCY_LIMIT`.
        max_concurrency_per_provider: Requests in flight per rate key, 1-:data:`CONCURRENCY_LIMIT`.
        rpm_limit: Requests per minute per rate key, 1-:data:`RPM_LIMIT_MAX`, or ``None`` for no limit.
        timeout_s: One attempt's timeout, :data:`MIN_ATTEMPT_S`-:data:`TIMEOUT_MAX_S` (held as a float).
        max_retries: Retries per request, 0-:data:`MAX_RETRIES_LIMIT`.
        max_output_tokens: The output limit sent, 1-131,072.
        max_requests: The most distinct cache misses a run may send, 1-:data:`REQUESTS_LIMIT`.
        strict: Whether a request that fails stops the run (else it is a :class:`FailedUnit`).

    Raises:
        TypeError: If a bound has the wrong type (a ``bool`` for a number included).
        ValueError: If it is out of range.
    """

    max_concurrency: int = 8
    max_concurrency_per_provider: int = 4
    rpm_limit: int | None = None
    timeout_s: float = 60.0
    max_retries: int = 2
    max_output_tokens: int = 8_192
    max_requests: int = 1_000
    strict: bool = False

    def __post_init__(self) -> None:
        _check_int("max_concurrency", self.max_concurrency, 1, CONCURRENCY_LIMIT)
        _check_int("max_concurrency_per_provider", self.max_concurrency_per_provider, 1, CONCURRENCY_LIMIT)
        if self.rpm_limit is not None:
            _check_int("rpm_limit", self.rpm_limit, 1, RPM_LIMIT_MAX)
        object.__setattr__(self, "timeout_s", _check_float("timeout_s", self.timeout_s, MIN_ATTEMPT_S, TIMEOUT_MAX_S))
        _check_int("max_retries", self.max_retries, 0, MAX_RETRIES_LIMIT)
        _check_int("max_output_tokens", self.max_output_tokens, 1, MAX_OUTPUT_TOKENS)
        _check_int("max_requests", self.max_requests, 1, REQUESTS_LIMIT)
        if not isinstance(self.strict, bool):
            raise TypeError(f"strict must be a bool, got {type(self.strict).__name__}")

    def unit_deadline_s(self, slots: int) -> float:
        """The wall time one request may take on a rate key of ``slots`` slots (module notes, 3).

        ``(max_retries + 1) * (timeout_s + ATTEMPT_SLACK_S) + max_retries * RETRY_AFTER_CAP_S``: each attempt's
        timeout and slack, and the longest wait before each retry (a capped ``retry_after_s``, which also covers the
        jitter's 20 s); with ``rpm_limit``, plus ``max_retries * slots * 60 / rpm_limit``: a retry's token waits
        behind at most one token per slot of its key. A first attempt's token is waited for before it starts.

        Raises:
            TypeError: If ``slots`` is not an ``int``.
            ValueError: If it is out of 1-:data:`CONCURRENCY_LIMIT`.
        """
        _check_int("slots", slots, 1, CONCURRENCY_LIMIT)
        attempts = self.max_retries + 1
        deadline = attempts * (self.timeout_s + ATTEMPT_SLACK_S) + self.max_retries * RETRY_AFTER_CAP_S
        if self.rpm_limit is not None:
            deadline += self.max_retries * slots * 60.0 / self.rpm_limit
        return deadline


EventHook: TypeAlias = Callable[[TelemetryEvent], object]
"""What ``on_event`` is: called with each event, on the caller's thread."""


class JitterSource(Protocol):
    """What ``rng`` is: a ``uniform(low, high)`` method, as :class:`random.Random` has."""

    def uniform(self, a: float, b: float, /) -> float:
        """A number in ``a``-``b``."""
        ...


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class ProviderSnapshot:
    """One provider as :func:`snapshot_providers` read it, once: everything a run uses of it afterwards.

    Build one only with :func:`snapshot_providers`: a snapshot built by hand is checked field by field, but nothing
    can check that its identity is its ``generate``'s (a snapshot may claim one model and call another). Its JSON is
    held as canonical text, and :attr:`generation_params` and :attr:`provider_schema` parse a fresh copy on each
    access (as ``PreparedSchema.wrapper`` does), so nothing changes a snapshot after it is made. Snapshots compare and
    hash by identity.

    Args:
        provider: The provider object (never read again: the members below are).
        model_id: Its model id.
        provider_kind: Its cache-key kind.
        rate_key: The endpoint identity its slots and token bucket share.
        model_family: Its vendor lineage (``"unknown"`` adds no family diversity).
        generation_params_json: What it sends that shapes the output, as the canonical JSON of an object without
            ``max_output_tokens``.
        provider_schema_json: ``transform_schema`` of the response wrapper, as the canonical JSON of an object.
        max_connections: Its HTTP pool size, or ``None`` when it exposes none.
        schema_sha256: The prepared schema it was made for (``PreparedSchema.sha256``).
        generate: Its ``generate`` method, read once.
        open: Its ``open`` method, read once, or ``None``.
        max_concurrency: Its optional per-provider concurrency cap (1-:data:`CONCURRENCY_LIMIT`), or ``None``.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a value breaks its rule (a JSON text that is not an object's canonical JSON included).
    """

    provider: object
    model_id: str
    provider_kind: str
    rate_key: str
    model_family: str
    generation_params_json: str = field(repr=False)
    provider_schema_json: str = field(repr=False)
    max_connections: int | None
    schema_sha256: str
    generate: Callable[[ModelRequest], object] = field(repr=False)
    open: Callable[[], object] | None = field(repr=False)
    max_concurrency: int | None = None

    def __post_init__(self) -> None:
        for name in ("model_id", "provider_kind", "rate_key"):
            _check_identity(name, getattr(self, name))
        problem = family_problem(self.model_family)
        if problem is not None:
            raise (TypeError if type(self.model_family) is not str else ValueError)(f"model_family {problem}")
        if "max_output_tokens" in _parse_object("generation_params_json", self.generation_params_json):
            raise ValueError("generation_params must not hold max_output_tokens: the run sets it and keys it")
        _parse_object("provider_schema_json", self.provider_schema_json)
        if self.max_connections is not None:
            _check_int("max_connections", self.max_connections, 1, MAX_CONNECTIONS_LIMIT)
        if self.max_concurrency is not None:
            _check_int("max_concurrency", self.max_concurrency, 1, CONCURRENCY_LIMIT)
        _check_digest("schema_sha256", self.schema_sha256)
        _check_callable("generate", self.generate)
        _check_callable("open", self.open, optional=True)

    @property
    def generation_params(self) -> dict[str, Any]:
        """The generation params, parsed afresh on each access."""
        return cast(dict[str, Any], strict_loads(self.generation_params_json))

    @property
    def provider_schema(self) -> dict[str, Any]:
        """The schema this provider sends, parsed afresh on each access."""
        return cast(dict[str, Any], strict_loads(self.provider_schema_json))


def snapshot_providers(providers: Sequence[object], schema: PreparedSchema) -> tuple[ProviderSnapshot, ...]:
    """Check each provider and read it once, in order (module notes, 1): the only read of any provider.

    Args:
        providers: The providers, in the caller's order (a non-empty tuple or list).
        schema: The prepared schema; each provider transforms its own fresh copy of the wrapper.

    Returns:
        One :class:`ProviderSnapshot` per provider, in order.

    Raises:
        TypeError: If ``providers`` is not a tuple or list, or ``schema`` not a ``PreparedSchema``.
        ValueError: If ``providers`` is empty.
        RationalizationError: If a provider is refused: not a provider, a member that raises or breaks its rule,
            ``generation_params`` holding ``max_output_tokens``, a transformed schema that is not exactly JSON, or two
            providers with one model id.
    """
    candidates = _check_sequence("providers", providers)
    if not isinstance(schema, PreparedSchema):
        raise TypeError(f"schema must be a PreparedSchema, got {type(schema).__name__}")
    snapshots = tuple(_snapshot(candidate, schema) for candidate in candidates)
    _refuse_shared_ids(snapshots)
    return snapshots


# ---------------------------------------------------------------------------------------------------------------
# Preflight: arguments
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Hooks:
    """The run's injected callables, checked: the event hook, the clock, the wait, the jitter and the nonce."""

    on_event: EventHook | None
    clock: Callable[[], float]
    sleep: Callable[[float], object] | None
    uniform: Callable[[float, float], float]
    nonce: Callable[[], str]


def _check_work(work: object) -> tuple[tuple[Unit, Envelope], ...]:
    """Require a tuple or list of ``(Unit, Envelope)`` pairs, each envelope its unit's."""
    if not isinstance(work, (tuple, list)):
        raise TypeError(f"work must be a tuple or list of (Unit, Envelope) pairs, got {type(work).__name__}")
    pairs: list[tuple[Unit, Envelope]] = []
    for item in work:
        if not (isinstance(item, tuple) and len(item) == 2):
            raise TypeError("each work item must be a (Unit, Envelope) tuple")
        unit, envelope = item
        if not isinstance(unit, Unit) or not isinstance(envelope, Envelope):
            raise TypeError(
                f"each work item must be a (Unit, Envelope), got ({type(unit).__name__}, {type(envelope).__name__})"
            )
        if envelope.unit_id != unit.unit_id:
            raise ValueError(f"the envelope of unit {unit.unit_id} is unit {envelope.unit_id}'s")
        pairs.append((unit, envelope))
    return tuple(pairs)


def _check_sequence(name: str, value: object) -> tuple[object, ...]:
    """Require a non-empty tuple or list."""
    if not isinstance(value, (tuple, list)):
        raise TypeError(f"{name} must be a tuple or list, got {type(value).__name__}")
    if not value:
        raise ValueError(f"{name} must not be empty")
    return tuple(value)


def _check_stores(schema: object, schema_version: object, memory_cache: object, file_cache: object) -> None:
    """Require the prepared schema, a schema version, a ``MemoryCache`` and a ``FileCache`` or ``None``."""
    if not isinstance(schema, PreparedSchema):
        raise TypeError(f"schema must be a PreparedSchema, got {type(schema).__name__}")
    _check_id("schema_version", schema_version)
    if not isinstance(memory_cache, MemoryCache):
        raise TypeError(f"memory_cache must be a MemoryCache, got {type(memory_cache).__name__}")
    if file_cache is not None and not isinstance(file_cache, FileCache):
        raise TypeError(f"file_cache must be a FileCache or None, got {type(file_cache).__name__}")


def _check_limits(limits: object) -> RunLimits:
    if limits is None:
        return RunLimits()
    if not isinstance(limits, RunLimits):
        raise TypeError(f"limits must be a RunLimits or None, got {type(limits).__name__}")
    return limits


def _check_hooks(on_event: object, clock: object, sleep: object, rng: object, nonce: object) -> _Hooks:
    """Require each hook callable (``on_event`` and ``sleep`` may be ``None``; ``rng`` needs ``uniform``)."""
    _check_callable("on_event", on_event, optional=True)
    _check_callable("clock", clock)
    _check_callable("sleep", sleep, optional=True)
    _check_callable("nonce", nonce)
    source = random.Random() if rng is None else rng
    uniform = getattr(source, "uniform", None)
    if not callable(uniform):
        raise TypeError(f"rng must have a uniform(low, high) method, got {type(rng).__name__}")
    return _Hooks(
        on_event=cast(EventHook | None, on_event),
        clock=cast(Callable[[], float], clock),
        sleep=cast(Callable[[float], object] | None, sleep),
        uniform=cast(Callable[[float, float], float], uniform),
        nonce=cast(Callable[[], str], nonce),
    )


# ---------------------------------------------------------------------------------------------------------------
# Preflight: provider snapshots and the request plan
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Request:
    """One distinct request: its key, its unit and envelope, and the provider snapshot it goes to."""

    key: str
    unit: Unit
    envelope: Envelope
    provider: ProviderSnapshot


def _refuse(owner: str, message: str) -> NoReturn:
    raise RationalizationError(f"provider {owner}: {message}")


def _read(obj: object, owner: str, member: str) -> object:
    """Read one member for the snapshot, refusing (outside the ``except``, so nothing chains) one that raises."""
    failure = ""
    try:
        return getattr(obj, member)
    except Exception as error:  # a provider's property is the provider's code: whatever it raises is refused
        failure = type(error).__name__
    _refuse(owner, f"reading {member} raised {failure}")


def _read_identity(obj: object, owner: str, member: str) -> str:
    value = _read(obj, owner, member)
    problem = identity_problem(value)
    if problem is not None:
        _refuse(owner, f"{member} {problem}")
    return cast(str, value)


def _read_callable(obj: object, owner: str, member: str) -> object:
    value = _read(obj, owner, member)
    if not callable(value):
        _refuse(owner, f"{member} must be callable")
    return value


def _read_params(obj: object, owner: str) -> dict[str, Any]:
    """The provider's generation params, exactly JSON, without ``max_output_tokens`` (the run sets it)."""
    params = _read(obj, owner, "generation_params")
    if not isinstance(params, Mapping):
        _refuse(owner, f"generation_params must be a JSON object (a Mapping), got {type(params).__name__}")
    copy: dict[str, Any] = canonical_copy(dict(params), subject=f"the generation_params of provider {owner}")
    if "max_output_tokens" in copy:
        _refuse(owner, "generation_params must not hold max_output_tokens: the run sets it on each request and keys it")
    return copy


def _read_optional(obj: object, owner: str) -> tuple[Callable[[], object] | None, int | None, int | None]:
    """The provider's ``open`` method, ``max_connections`` and ``max_concurrency``, each ``None`` when absent."""
    opener = _read(obj, owner, "open") if has_real_member(obj, "open") else None
    if opener is not None and not callable(opener):
        _refuse(owner, "open must be callable")
    connections = _read(obj, owner, "max_connections") if has_real_member(obj, "max_connections") else None
    if connections is not None and (type(connections) is not int or not 1 <= connections <= MAX_CONNECTIONS_LIMIT):
        _refuse(owner, f"max_connections must be an int in 1-{MAX_CONNECTIONS_LIMIT}")
    concurrency = _read(obj, owner, "max_concurrency") if has_real_member(obj, "max_concurrency") else None
    if concurrency is not None and (type(concurrency) is not int or not 1 <= concurrency <= CONCURRENCY_LIMIT):
        _refuse(owner, f"max_concurrency must be an int in 1-{CONCURRENCY_LIMIT}")
    return cast(Callable[[], object] | None, opener), connections, concurrency


def _snapshot(obj: object, schema: PreparedSchema) -> ProviderSnapshot:
    """Check one provider and read what a run uses of it, once; the values read are checked themselves. Its
    ``transform_schema`` gets a fresh copy of the wrapper (``PreparedSchema.wrapper`` parses one on each access)."""
    check_provider(obj)
    owner = type(obj).__name__
    model_id = _read_identity(obj, owner, "model_id")
    provider_kind = _read_identity(obj, owner, "provider_kind")
    rate_key = _read_identity(obj, owner, "rate_key")
    family = _read(obj, owner, "model_family")
    problem = family_problem(family)
    if problem is not None:
        _refuse(owner, f"model_family {problem}")
    params = _read_params(obj, owner)
    transform = cast(Callable[[Mapping[str, Any]], object], _read_callable(obj, owner, "transform_schema"))
    generate = cast(Callable[[ModelRequest], object], _read_callable(obj, owner, "generate"))
    opener, connections, concurrency = _read_optional(obj, owner)
    transformed = transform(schema.wrapper)
    if not isinstance(transformed, Mapping):
        _refuse(owner, f"transform_schema must return a JSON object (a Mapping), got {type(transformed).__name__}")
    return ProviderSnapshot(
        provider=obj,
        model_id=model_id,
        provider_kind=provider_kind,
        rate_key=rate_key,
        model_family=cast(str, family),
        generation_params_json=canonical_json(params, subject=f"the generation_params of provider {owner}"),
        provider_schema_json=canonical_json(
            canonical_copy(dict(transformed), subject=f"the transformed schema of provider {owner}"),
            subject=f"the transformed schema of provider {owner}",
        ),
        max_connections=connections,
        schema_sha256=schema.sha256,
        generate=generate,
        open=opener,
        max_concurrency=concurrency,
    )


def _refuse_shared_ids(snapshots: Sequence[ProviderSnapshot]) -> None:
    """Refuse two providers with one model id: the outcome is keyed by it."""
    seen: set[str] = set()
    for snapshot in snapshots:
        if snapshot.model_id in seen:
            raise RationalizationError(f"two providers share the model id {snapshot.model_id}: each needs its own")
        seen.add(snapshot.model_id)


def _check_snapshots(value: object, schema: PreparedSchema) -> tuple[ProviderSnapshot, ...]:
    """Require a non-empty tuple or list of snapshots made for ``schema``, with distinct model ids."""
    snapshots = _check_sequence("providers", value)
    for snapshot in snapshots:
        if not isinstance(snapshot, ProviderSnapshot):
            raise TypeError(f"providers must be ProviderSnapshot (snapshot_providers()), got {type(snapshot).__name__}")
        if snapshot.schema_sha256 != schema.sha256:
            raise ValueError(f"the snapshot of provider {snapshot.model_id} was made for another schema")
    checked = cast(tuple[ProviderSnapshot, ...], snapshots)
    _refuse_shared_ids(checked)
    return checked


@dataclass(frozen=True, slots=True)
class _PreflightPlan:
    """Precomputed request slots, distinct requests, and FileCache lookups from single-pass preflight."""

    slots: Mapping[tuple[str, str], str]
    requests: tuple[_Request, ...]
    file_lookups: Mapping[str, CacheLookup]


def _plan(
    work: Sequence[tuple[Unit, Envelope]], providers: Sequence[ProviderSnapshot], limits: RunLimits, schema_version: str
) -> tuple[dict[tuple[str, str], str], list[_Request]]:
    """Each (unit, model) slot's key, in unit then provider order, and the distinct requests in first-seen order.

    The key's ``generation_params`` are the provider's with the request's ``max_output_tokens``.
    """
    slots: dict[tuple[str, str], str] = {}
    requests: dict[str, _Request] = {}
    suffixes = {
        provider.model_id: _provider_key_suffix(
            provider_kind=provider.provider_kind,
            model_id=provider.model_id,
            generation_params={**provider.generation_params, "max_output_tokens": limits.max_output_tokens},
            provider_schema_json=provider.provider_schema_json,
            schema_version=schema_version,
        )
        for provider in providers
    }
    for unit, envelope in work:
        env_hasher = hashlib.sha256(_envelope_key_prefix(envelope))
        for provider in providers:
            key = _fast_request_key(envelope, suffixes[provider.model_id], envelope_hasher=env_hasher)
            slot = (unit.unit_id, provider.model_id)
            if slots.setdefault(slot, key) != key:
                raise ValueError(f"unit {unit.unit_id} appears twice with different envelopes")
            requests.setdefault(key, _Request(key, unit, envelope, provider))
    return slots, list(requests.values())


@dataclass(frozen=True, slots=True)
class _Probe:
    """What the caches answered at preflight: the hits by key, the misses in order, the corrupt entries."""

    hits: Mapping[str, ProviderResponse]
    misses: tuple[_Request, ...]
    corrupt: int


def _event(
    request: _Request, kind: EventKind, attempt: int, status: int | None, latency: float | None, detail: str
) -> TelemetryEvent:
    """An event about ``request``, its detail one bounded line."""
    return TelemetryEvent(
        kind=kind,
        unit_id=request.unit.unit_id,
        model_id=request.provider.model_id,
        attempt=attempt,
        status_code=status,
        latency_ms=latency,
        detail=bounded_detail(detail),
    )


def _lookup(
    key: str,
    memory: MemoryCache,
    file_cache: FileCache | None,
    file_lookups: Mapping[str, CacheLookup] | None = None,
) -> tuple[CacheLookup, str]:
    """The memory cache's answer, else the file cache's when there is one, and which store gave it."""
    lookup = memory.get(key)
    if lookup.hit or file_cache is None:
        return lookup, "memory"
    if file_lookups is not None and key in file_lookups:
        return file_lookups[key], "file"
    return file_cache.get(key), "file"


def _probe(
    requests: Sequence[_Request],
    memory: MemoryCache,
    file_cache: FileCache | None,
    deliver: Callable[[TelemetryEvent], None],
    file_lookups: Mapping[str, CacheLookup] | None = None,
) -> _Probe:
    """Look each distinct request up, the memory cache first; each hit and corrupt entry is an event, delivered here."""
    hits: dict[str, ProviderResponse] = {}
    misses: list[_Request] = []
    corrupt = 0
    for request in requests:
        lookup, store = _lookup(request.key, memory, file_cache, file_lookups)
        if lookup.response is not None:
            hits[request.key] = lookup.response
            deliver(_event(request, "cache_hit", 0, None, None, store))
            continue
        if lookup.corrupt:
            corrupt += 1
            deliver(_event(request, "cache_corrupt", 0, None, None, str(lookup.reason)))
        misses.append(request)
    return _Probe(hits, tuple(misses), corrupt)


# ---------------------------------------------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Stores:
    """Where fresh responses go, and the metadata every entry shares."""

    memory: MemoryCache
    file: FileCache | None
    schema_name: str
    schema_version: str
    schema_sha256: str
    prompt_hashes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Job:
    """A request as one worker runs it: its position among the misses (the order its events reach the caller)."""

    position: int
    request: _Request


@dataclass(slots=True)
class _Tally:
    """One request's counters, kept by the one worker that runs it."""

    attempts: int = 0
    retries: int = 0
    latency_ms: float = 0.0


@dataclass(frozen=True, slots=True)
class _Result:
    """How one request ended: its response, and its counters."""

    response: ProviderResponse | None
    attempts: int
    retries: int
    latency_ms: float
    write_failures: int
    failed: FailedUnit | None


class _TokenBucket:
    """One rate key's token bucket of capacity one: one attempt per ``interval_s``, reserved in order under a lock
    (a reservation that would start past its latest instant reserves nothing)."""

    def __init__(self, interval_s: float, clock: Callable[[], float]) -> None:
        self._interval_s = interval_s
        self._clock = clock
        self._next: float | None = None
        self._lock = threading.Lock()

    def take_if_due(self) -> float:
        """Take the next token if it is due now (then ``0.0``); else take nothing and return the time until it."""
        with self._lock:
            now = self._clock()
            if self._next is not None and self._next > now:
                return self._next - now
            self._next = now + self._interval_s
            return 0.0

    def reserve(self, latest: float) -> float | None:
        """Reserve the next token; return the wait until it, or ``None`` when it comes after ``latest``."""
        with self._lock:
            now = self._clock()
            start = now if self._next is None else max(now, self._next)
            if start > latest:
                return None
            self._next = start + self._interval_s
            return start - now


def _slot_counts(providers: Sequence[ProviderSnapshot], limits: RunLimits) -> dict[str, int]:
    """Each rate key's slots: ``max_concurrency_per_provider``, cut to the smallest ``max_connections`` and
    ``max_concurrency`` among the providers sharing the key (each has its own pool), so no request waits on a
    connection pool or exceeds a provider's concurrency cap."""
    counts: dict[str, int] = {}
    for provider in providers:
        size = counts.get(provider.rate_key, limits.max_concurrency_per_provider)
        if provider.max_connections is not None:
            size = min(size, provider.max_connections)
        if provider.max_concurrency is not None:
            size = min(size, provider.max_concurrency)
        counts[provider.rate_key] = size
    return counts


def _file_put(
    file_cache: FileCache,
    key: str,
    response: ProviderResponse,
    meta: CacheMeta,
    *,
    resp_json: dict[str, Any] | None = None,
) -> list[str]:
    """Write one entry; what went wrong (an entry past 8 MiB, a write that failed), as event details."""
    try:
        written = (
            file_cache._put_trusted_json(key, resp_json, meta)
            if resp_json is not None and type(file_cache) is FileCache
            else file_cache.put(key, response, meta)
        )
    except RationalizationError as error:  # the run keeps the response it has (module notes)
        return [f"file cache write failed: {error}"]
    return [] if written else ["file cache entry past 8 MiB: not written"]


class _Run:
    """One run's shared state: limits, hooks and per-rate-key semaphores (module notes)."""

    def __init__(
        self,
        limits: RunLimits,
        providers: Sequence[ProviderSnapshot],
        hooks: _Hooks,
        stores: _Stores,
    ) -> None:
        self.limits = limits
        self.clock = hooks.clock
        self.stop = threading.Event()
        self._sleep = hooks.sleep if hooks.sleep is not None else self.stop.wait
        self._uniform = hooks.uniform
        self._nonce = hooks.nonce
        self._jitter_lock = threading.Lock()
        self._stores = stores
        slots = _slot_counts(providers, limits)
        self.semaphores = {key: threading.BoundedSemaphore(size) for key, size in slots.items()}
        self.deadlines = {key: limits.unit_deadline_s(size) for key, size in slots.items()}
        self.events: queue.SimpleQueue[tuple[int, TelemetryEvent]] = queue.SimpleQueue()
        self._schemas = {provider.model_id: provider.provider_schema for provider in providers}  # parsed once
        self._attempt_ends: dict[int, float] = {}
        self._attempt_lock = threading.Lock()
        rpm = limits.rpm_limit
        self.buckets = {} if rpm is None else {key: _TokenBucket(60.0 / rpm, self.clock) for key in slots}

    def attempt_end(self, position: int) -> float | None:
        """When the request's current attempt ends at the latest (its send, ``timeout_s``, the slack), or ``None``
        before its first send."""
        with self._attempt_lock:
            return self._attempt_ends.get(position)

    def wait_for_token(self, due_in: float | None) -> None:
        """Wait, on the caller's thread with nothing in flight, until the earliest token a free slot waits for.

        Raises:
            RuntimeError: If no token is awaited (nothing in flight, so every slot is free): the scheduler stalled.
        """
        if due_in is None:
            raise RuntimeError("requests are queued, none is in flight and none waits for a token")
        self._sleep(due_in)

    def execute(self, position: int, request: _Request) -> _Result:
        """Run one request on a worker thread; its rate key's slot, taken by the caller, is released here."""
        try:
            return self._attempts(_Job(position, request))
        finally:
            self.semaphores[request.provider.rate_key].release()

    def _emit(
        self, job: _Job, kind: EventKind, attempt: int, status: int | None, latency: float | None, detail: str
    ) -> None:
        """Queue an event for the caller's thread, tagged with the request's position (its delivery order)."""
        self.events.put((job.position, _event(job.request, kind, attempt, status, latency, detail)))

    def _attempts(self, job: _Job) -> _Result:
        """Send the request until it succeeds, its attempts are spent (``max_retries + 1``) or its deadline passes."""
        tally = _Tally()
        rate_key = job.request.provider.rate_key
        deadline = self.clock() + self.deadlines[rate_key]
        last: TransientProviderError | None = None
        for attempt in range(1, self.limits.max_retries + 2):
            # the first attempt's token was taken when the request was submitted; a retry reserves its own
            timeout = self._time_left(deadline) if attempt == 1 else self._ready(rate_key, deadline)
            if self.stop.is_set():  # the run is stopping: nothing more is sent
                return _Result(None, tally.attempts, tally.retries, tally.latency_ms, 0, None)
            if timeout is None:
                return self._fail(job, tally, None if last is None else last.status_code, "deadline passed", last)
            started = self.clock()
            try:
                response = self._send(job, tally, timeout)
            except TransientProviderError as error:
                last = error
                if attempt > self.limits.max_retries:
                    return self._fail(job, tally, error.status_code, "retries exhausted", error)
                if not self._back_off(job, attempt, error, tally, deadline):
                    return self._fail(job, tally, error.status_code, "deadline passed", error)
                continue
            failures = self._store(job, response, max(0.0, self.clock() - started) * 1000.0, tally.attempts)
            return _Result(response, tally.attempts, tally.retries, tally.latency_ms, failures, None)
        raise RuntimeError("the attempt loop ended without a result")  # unreachable: the last attempt returns

    def _ready(self, rate_key: str, deadline: float) -> float | None:
        """Wait for the rate key's token (with ``rpm_limit``), then the attempt's timeout (:meth:`_time_left`);
        ``None`` when the token or the floor would come past the deadline (then nothing is waited for)."""
        bucket = self.buckets.get(rate_key)
        if bucket is not None:
            wait = bucket.reserve(deadline - MIN_ATTEMPT_S)
            if wait is None:
                return None
            if wait > 0:
                self._sleep(wait)
        return self._time_left(deadline)

    def _time_left(self, deadline: float) -> float | None:
        """The next attempt's timeout: ``timeout_s`` cut to the time left, ``None`` with less than the floor left."""
        remaining = deadline - self.clock()
        if remaining < MIN_ATTEMPT_S:
            return None
        return min(self.limits.timeout_s, remaining)

    def _fail(
        self, job: _Job, tally: _Tally, status: int | None, reason: str, error: TransientProviderError | None
    ) -> _Result:
        """End a request without a response: raise under ``strict``, else a :class:`FailedUnit` and its event.

        Raises:
            RationalizationError: Under ``strict``, naming the provider, the unit and the attempts (chained).
        """
        request = job.request
        why = f"{reason} after {tally.attempts} attempts" + ("" if error is None else f": {error}")
        if self.limits.strict:
            raise RationalizationError(
                f"provider {request.provider.model_id} failed unit {request.unit.unit_id} (strict): {why}"
            ) from error
        self._emit(job, "unit_failed", tally.attempts, status, None, reason)
        failed = FailedUnit(
            request.unit.unit_id, request.provider.model_id, status, bounded_detail(why), tally.attempts
        )
        return _Result(None, tally.attempts, tally.retries, tally.latency_ms, 0, failed)

    def _retry_wait(self, attempt: int, error: TransientProviderError) -> float:
        """The wait before the next attempt: the provider's ``retry_after_s`` capped at 30 s, else full jitter."""
        if error.retry_after_s is not None:
            return min(error.retry_after_s, RETRY_AFTER_CAP_S)
        window = min(JITTER_CAP_S, JITTER_BASE_S * 2**attempt)
        with self._jitter_lock:
            wait = self._uniform(0.0, window)
        if isinstance(wait, bool) or not isinstance(wait, (int, float)) or not 0.0 <= wait <= window:
            raise ValueError(f"rng.uniform(0, {window}) answered outside its window")
        return float(wait)

    def _back_off(self, job: _Job, attempt: int, error: TransientProviderError, tally: _Tally, deadline: float) -> bool:
        """Wait before the next attempt, as an event; ``False`` (no wait) when the wait would leave the next attempt
        less than the floor before the deadline."""
        wait = self._retry_wait(attempt, error)
        if self.clock() + wait > deadline - MIN_ATTEMPT_S:
            return False
        tally.retries += 1
        self._emit(job, "retry", attempt, error.status_code, None, f"attempt {attempt + 1} after a {wait:.3f} s wait")
        if wait > 0:
            self._sleep(wait)
        return True

    def _store(self, job: _Job, response: ProviderResponse, latency_ms: float, attempt: int) -> int:
        """Put a fresh response into the memory cache and the file cache; each store that keeps nothing is an
        event, and counts. Returns how many did not."""
        request, stores = job.request, self._stores
        prompt_hash = stores.prompt_hashes.get(request.envelope.prompt) or sha256_hex(request.envelope.prompt)
        meta = CacheMeta(
            model_id=request.provider.model_id,
            unit_id=request.unit.unit_id,
            doc_id=request.unit.doc_id,
            schema_name=stores.schema_name,
            schema_version=stores.schema_version,
            schema_sha256=stores.schema_sha256,
            prompt_sha256=prompt_hash,
            latency_ms=latency_ms,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        resp_json = _to_trusted_json(response)
        mem_ok = (
            stores.memory._put_trusted_json(request.key, resp_json, meta)
            if type(stores.memory) is MemoryCache
            else stores.memory.put(request.key, response, meta)
        )
        problems = [] if mem_ok else ["memory cache full"]
        if stores.file is not None:
            problems.extend(_file_put(stores.file, request.key, response, meta, resp_json=resp_json))
        for problem in problems:
            self._emit(job, "cache_write_failed", attempt, None, None, problem)
        return len(problems)

    def _render(self, request: _Request) -> tuple[str, str]:
        """The request's messages under a fresh nonce: one that appears in none of what it sends, drawn at most
        :data:`NONCE_DRAWS` times.

        Raises:
            RationalizationError: If every draw is already in the content (the nonce source is not random).
        """
        envelope = request.envelope
        for _ in range(NONCE_DRAWS):
            try:
                return envelope.render(self._nonce())
            except ValueError as error:
                if "already holds this nonce" in str(error):
                    continue
                raise
        raise RationalizationError(
            f"no nonce absent from unit {request.unit.unit_id}'s content in {NONCE_DRAWS} draws: the nonce source is "
            "not random"
        )

    def _send(self, job: _Job, tally: _Tally, timeout_s: float) -> ProviderResponse:
        request = job.request
        system, user = self._render(request)
        model_request = _trusted_model_request(
            unit_id=request.unit.unit_id,
            model_id=request.provider.model_id,
            system_prompt=system,
            user_envelope=user,
            json_schema=self._schemas[request.provider.model_id],
            max_output_tokens=self.limits.max_output_tokens,
            timeout_s=timeout_s,
            images=request.envelope.images,
        )
        started = self.clock()
        tally.attempts += 1
        with self._attempt_lock:  # a stopping run waits for this attempt only until then (module notes, 6)
            self._attempt_ends[job.position] = started + timeout_s + ATTEMPT_SLACK_S
        try:
            answer = request.provider.generate(model_request)
        except (TransientProviderError, PermanentProviderError) as error:
            latency = max(0.0, self.clock() - started) * 1000.0
            tally.latency_ms += latency
            self._emit(job, "request", tally.attempts, error.status_code, latency, type(error).__name__)
            if isinstance(error, PermanentProviderError):
                raise RationalizationError(
                    f"provider {request.provider.model_id} failed for good on unit {request.unit.unit_id}: {error}"
                ) from error
            raise
        latency = max(0.0, self.clock() - started) * 1000.0
        tally.latency_ms += latency
        if not isinstance(answer, ProviderResponse):
            raise TypeError(
                f"provider {request.provider.model_id} returned {type(answer).__name__}, not a ProviderResponse"
            )
        self._emit(job, "request", tally.attempts, None, latency, answer.finish)
        return answer


class _Scheduler:
    """The caller's queue of misses, per rate key in order: a request is released only into a free slot of its key
    and, with ``rpm_limit``, with its key's token taken (due now), so its unit deadline starts at a real send."""

    def __init__(self, misses: Sequence[_Request], run: "_Run") -> None:
        self._queues: dict[str, deque[tuple[int, _Request]]] = {}
        for position, request in enumerate(misses):
            self._queues.setdefault(request.provider.rate_key, deque()).append((position, request))
        self._semaphores = run.semaphores
        self._buckets = run.buckets
        self.due_in: float | None = None

    @property
    def empty(self) -> bool:
        """Whether every request has been released."""
        return not any(self._queues.values())

    def take(self, room: int) -> list[tuple[int, _Request]]:
        """Up to ``room`` requests whose key has a free slot and a due token (both taken here), earliest first.

        Afterwards :attr:`due_in` is the time until the earliest token a free slot waits for, or ``None``.
        """
        taken: list[tuple[int, _Request]] = []
        self.due_in = None
        if room <= 0:
            return taken
        heap = [(line[0][0], rate_key) for rate_key, line in self._queues.items() if line]
        heapq.heapify(heap)
        while heap and len(taken) < room:
            _, rate_key = heapq.heappop(heap)
            line = self._queues[rate_key]
            if self._release(line):
                taken.append(line.popleft())
                if line:
                    heapq.heappush(heap, (line[0][0], rate_key))
        return taken

    def _release(self, line: deque[tuple[int, _Request]]) -> bool:
        """Take a slot and a token for the line's first request: ``False`` (nothing taken) without both."""
        key = line[0][1].provider.rate_key
        if not self._semaphores[key].acquire(blocking=False):
            return False
        bucket = self._buckets.get(key)
        wait = 0.0 if bucket is None else bucket.take_if_due()
        if wait > 0:
            self._semaphores[key].release()
            self.due_in = wait if self.due_in is None else min(self.due_in, wait)
            return False
        return True


class _EventOrder:
    """Delivers the workers' events on the caller's thread in the misses' order: every event of one request, as
    they come, before any of the next one's, so the order never depends on which worker finishes first."""

    def __init__(
        self,
        events: "queue.SimpleQueue[tuple[int, TelemetryEvent]]",
        count: int,
        deliver: Callable[[TelemetryEvent], None],
    ) -> None:
        self._events = events
        self._buffers: list[list[TelemetryEvent]] = [[] for _ in range(count)]
        self._next = 0
        self._deliver = deliver

    def flush(self, finished: Mapping[int, _Result]) -> None:
        """Drain the queue, then deliver what is due: the current request's events, then the next one's once the
        current one has finished (its events are all queued before its result is set)."""
        while True:
            try:
                position, event = self._events.get_nowait()
            except queue.Empty:
                break
            self._buffers[position].append(event)
        while self._next < len(self._buffers):
            pending, self._buffers[self._next] = self._buffers[self._next], []
            for event in pending:
                self._deliver(event)
            if self._next not in finished:
                return
            self._next += 1

    def drain(self) -> None:
        """Drain the queue and deliver every remaining buffered event in request order, when a failing run stops."""
        while True:
            try:
                position, event = self._events.get_nowait()
            except queue.Empty:
                break
            self._buffers[position].append(event)
        while self._next < len(self._buffers):
            pending, self._buffers[self._next] = self._buffers[self._next], []
            for event in pending:
                self._deliver(event)
            self._next += 1


class _Overrun(Exception):
    """A request passed its own bound, or the caller's loop its: a call outlived every deadline."""


class _WorkerFailed(Exception):
    """A request's worker raised: the run stops, then raises the earliest such failure (:func:`_stop`)."""


@dataclass(frozen=True, slots=True)
class _Flight:
    """A submitted request: its position among the misses, when it was submitted, and its key's unit deadline."""

    position: int
    submitted: float
    deadline_s: float


def _wait_bound(run: "_Run", count: int) -> int:
    """The most waits the caller's loop takes for ``count`` requests, so it ends even if a provider call never does
    (or the clock never moves). It assumes each wait really waits (module notes, 11).

    Each wait ends with a request done (at most ``count`` times), a token due (at most ``count`` times), or after at
    least :data:`MIN_WAIT_S`; the requests, run one at a time, end within ``count`` unit deadlines and token
    intervals, plus :data:`RUN_GRACE_S`.
    """
    interval = 0.0 if run.limits.rpm_limit is None else 60.0 / run.limits.rpm_limit
    longest = max(run.deadlines.values()) + interval
    return 2 * count + math.ceil((count * longest + RUN_GRACE_S) / MIN_WAIT_S) + 1


def _drive(
    run: _Run,
    pool: ThreadPoolExecutor,
    misses: Sequence[_Request],
    order: _EventOrder,
    flights: dict["Future[_Result]", _Flight],
) -> dict[int, _Result]:
    """Submit the misses into free slots, collect their results and deliver their events until every one is done.

    Every submitted future is recorded in ``flights`` (the stop path reads it).

    Raises:
        _WorkerFailed: If a request's worker raised.
        _Overrun: If a request passes its own bound (:func:`_check_overrun`), or the loop its (:func:`_wait_bound`).
    """
    scheduler = _Scheduler(misses, run)
    active: dict[Future[_Result], int] = {}
    results: dict[int, _Result] = {}
    for _ in range(_wait_bound(run, len(misses))):
        for position, request in scheduler.take(run.limits.max_concurrency - len(active)):
            future = pool.submit(run.execute, position, request)
            flights[future] = _Flight(position, run.clock(), run.deadlines[request.provider.rate_key])
            active[future] = position
        if not active:
            if scheduler.empty:
                order.flush(results)
                return results
            run.wait_for_token(scheduler.due_in)
            continue
        due = scheduler.due_in
        timeout = POLL_S if due is None else max(MIN_WAIT_S, min(POLL_S, due))
        done, _ = wait(active, timeout=timeout, return_when=FIRST_COMPLETED)
        for future in sorted(done, key=active.__getitem__):
            if future.exception() is not None:
                raise _WorkerFailed
            results[active.pop(future)] = future.result()
        order.flush(results)
        _check_overrun(run, active, flights)
    raise _Overrun


def _check_overrun(
    run: _Run, active: Mapping["Future[_Result]", int], flights: Mapping["Future[_Result]", _Flight]
) -> None:
    """Raise once any request still in flight passes its own bound: its submission, its unit deadline,
    :data:`RUN_GRACE_S`. One that finished since the last wait (while ``on_event`` held the caller's thread, say) is
    no longer in flight, whatever the time.

    Raises:
        _Overrun: If one has.
    """
    now = run.clock()
    for future in active:
        if future.done():
            continue
        flight = flights[future]
        if now > flight.submitted + flight.deadline_s + RUN_GRACE_S:
            raise _Overrun


def _stop_bound(run: _Run, flight: _Flight) -> float:
    """How long a stopping run waits for one request: until its current attempt ends (no attempt starts after a
    stop), or its unit deadline when that comes first or it has sent nothing yet, plus :data:`STOP_GRACE_S`."""
    unit_end = flight.submitted + flight.deadline_s
    attempt_end = run.attempt_end(flight.position)
    return (unit_end if attempt_end is None else min(unit_end, attempt_end)) + STOP_GRACE_S


def _join_in_flight(run: _Run, flights: Mapping["Future[_Result]", _Flight]) -> None:
    """Wait for the requests still in flight, each only until its bound (:func:`_stop_bound`, read on each pass).

    Each pass ends with a request done or the earliest bound passed, so at most one pass per request.
    """
    running = [future for future in flights if not future.done()]
    for _ in range(len(running) + 1):
        now = run.clock()
        bounds = {future: _stop_bound(run, flights[future]) for future in running if not future.done()}
        live = [future for future, bound in bounds.items() if bound > now]
        if not live:
            return
        wait(live, timeout=min(bounds[future] for future in live) - now, return_when=FIRST_COMPLETED)


def _earliest_failure(flights: Mapping["Future[_Result]", _Flight]) -> BaseException:
    """The exception of the earliest request (unit order, then provider order) among those that raised."""
    failed = [
        (flight.position, future)
        for future, flight in flights.items()
        if future.done() and not future.cancelled() and future.exception() is not None
    ]
    earliest = min(failed, key=lambda item: item[0])[1]
    return cast(BaseException, earliest.exception())


def _stop(
    run: _Run,
    pool: ThreadPoolExecutor,
    flights: Mapping["Future[_Result]", _Flight],
    order: _EventOrder,
    failure: BaseException,
) -> BaseException:
    """Stop a failing run (module notes, 6) and return what to raise.

    The stop flag ends every wait; queued requests are cancelled; the ones in flight are waited for only until their
    own deadlines (:func:`_join_in_flight`); the pool is joined when they are all done, else shut down without
    joining the call still running.
    """
    run.stop.set()
    for future in flights:
        future.cancel()  # a request not yet started never starts; one running or done is unaffected
    _join_in_flight(run, flights)
    # with every request done the threads are idle and exit at once, so join them; else leave the hung one running
    pool.shutdown(wait=all(future.done() for future in flights), cancel_futures=True)
    if isinstance(failure, (_WorkerFailed, _Overrun)):
        order.drain()
    if isinstance(failure, _WorkerFailed):
        return _earliest_failure(flights)
    if isinstance(failure, _Overrun):
        return RationalizationError(
            "a request passed its time bound: a call no deadline can cut did not return (a custom provider, a token "
            "source or a file system under the file cache must bound its own calls; a custom provider by "
            "request.timeout_s); its worker thread is left to end when the call returns"
        )
    return failure


def _execute(run: _Run, misses: Sequence[_Request], deliver: Callable[[TelemetryEvent], None]) -> dict[int, _Result]:
    """Run the misses on a pool of at most ``max_concurrency`` threads (module notes, 3-6).

    A run that ends well joins its idle pool; a failing one stops (:func:`_stop`) and raises.

    Raises:
        RationalizationError: If a provider fails for good, a strict request fails, or a request passes its bound.
        BaseException: Whatever else a worker or ``on_event`` raised (the earliest worker's, unchanged).
    """
    if not misses:
        return {}
    pool = ThreadPoolExecutor(max_workers=min(run.limits.max_concurrency, len(misses)), thread_name_prefix=_THREADS)
    order = _EventOrder(run.events, len(misses), deliver)
    flights: dict[Future[_Result], _Flight] = {}
    try:
        results = _drive(run, pool, misses, order, flights)
    except BaseException as error:  # every way out stops the run within its bounds, then raises
        failure: BaseException = error
    else:
        run.stop.set()
        pool.shutdown(wait=True)  # every request is done: this joins idle threads
        return results
    raise _stop(run, pool, flights, order, failure)


# ---------------------------------------------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------------------------------------------


def _usage(
    providers: Sequence[ProviderSnapshot], misses: Sequence[_Request], results: Mapping[int, _Result]
) -> dict[str, ModelUsage]:
    """Each model's counters, in provider order, summed over its requests' results."""
    totals = {provider.model_id: [0, 0, 0, 0, 0.0] for provider in providers}
    for position, request in enumerate(misses):
        result = results[position]
        total = totals[request.provider.model_id]
        total[0] += result.attempts
        total[1] += result.retries
        if result.response is not None:
            total[2] += result.response.input_tokens or 0
            total[3] += result.response.output_tokens or 0
        total[4] += result.latency_ms
    return {
        model_id: ModelUsage(int(a), int(b), int(c), int(d), float(e)) for model_id, (a, b, c, d, e) in totals.items()
    }


def _assemble(
    slots: Mapping[tuple[str, str], str],
    probe: _Probe,
    results: Mapping[int, _Result],
    providers: Sequence[ProviderSnapshot],
) -> RunOutcome:
    """The outcome, slot by slot in unit then provider order: each slot's response or failure, and the counters.

    Raises:
        RuntimeError: If a request ended with neither (only a stopping run cancels one, and a stopping run raises).
    """
    keyed: dict[str, ProviderResponse | FailedUnit] = dict(probe.hits)
    for position, request in enumerate(probe.misses):
        result = results[position]
        found = result.response if result.response is not None else result.failed
        if found is None:
            raise RuntimeError(f"request for unit {request.unit.unit_id} ended with neither a response nor a failure")
        keyed[request.key] = found
    return RunOutcome(
        responses={slot: found for slot, key in slots.items() if isinstance(found := keyed[key], ProviderResponse)},
        failed_units=tuple(found for key in slots.values() if isinstance(found := keyed[key], FailedUnit)),
        cache_hits=len(probe.hits),
        cache_misses=len(probe.misses),
        cache_corrupt=probe.corrupt,
        cache_write_failures=sum(result.write_failures for result in results.values()),
        usage=_usage(providers, probe.misses, results),
        models=tuple((provider.model_id, provider.model_family, provider.provider_kind) for provider in providers),
    )


def _deliverer(on_event: EventHook | None) -> Callable[[TelemetryEvent], None]:
    """What delivers one event on the caller's thread: ``on_event``, or nothing without one."""

    def deliver(event: TelemetryEvent) -> None:
        if on_event is not None:
            on_event(event)

    return deliver


def _resolve_plan(
    pairs: tuple[tuple[Unit, Envelope], ...],
    snapshots: tuple[ProviderSnapshot, ...],
    limits: RunLimits,
    schema_version: str,
    preflight: _PreflightPlan | None,
) -> tuple[Mapping[tuple[str, str], str], Sequence[_Request], Mapping[str, CacheLookup] | None]:
    """Return ``(slots, requests, file_lookups)`` from ``preflight`` or compute them via :func:`_plan`."""
    if preflight is None:
        slots, requests = _plan(pairs, snapshots, limits, schema_version)
        return slots, requests, None
    if not isinstance(preflight, _PreflightPlan):
        raise TypeError(f"_preflight must be a _PreflightPlan or None, got {type(preflight).__name__}")
    return preflight.slots, preflight.requests, preflight.file_lookups


def run_requests(
    work: Sequence[tuple[Unit, Envelope]],
    providers: Sequence[ProviderSnapshot],
    schema: PreparedSchema,
    *,
    schema_version: str,
    memory_cache: MemoryCache,
    file_cache: FileCache | None = None,
    limits: RunLimits | None = None,
    on_event: EventHook | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], object] | None = None,
    rng: JitterSource | None = None,
    nonce: Callable[[], str] = new_nonce,
    _preflight: _PreflightPlan | None = None,
) -> RunOutcome:
    """Send every (unit, provider) request the caches do not answer, and return the outcome (module notes).

    Args:
        work: The ``(Unit, Envelope)`` pairs, in document order (a tuple or list).
        providers: The providers' snapshots (:func:`snapshot_providers`, made for ``schema``), in the caller's
            order (a non-empty tuple or list).
        schema: The prepared schema: its hash and model name go in each cache entry's metadata, and every
            envelope must carry its wrapper.
        schema_version: The caller's schema version, a cache-key field.
        memory_cache: The in-run store, probed first and written on every fresh response.
        file_cache: The persistent store, probed second and written too, or ``None``.
        limits: The bounds (:class:`RunLimits`; its defaults when ``None``).
        on_event: Called with each :class:`TelemetryEvent` on the caller's thread, or ``None``.
        clock: A monotonic clock in seconds (tests inject one).
        sleep: The waits (before a retry, for a token); ``None`` waits on the run's stop flag, so a stopping run
            ends it at once. One passed in must really sleep, or be paired with a fake ``clock`` it advances: a
            ``sleep`` that returns at once with the real clock and ``rpm_limit`` spins the caller until its count of
            waits ends the run (module notes, 11).
        rng: The jitter's source (``uniform``); a fresh ``random.Random`` when ``None``.
        nonce: The nonce source (:func:`~datagrunt.rationalization.envelope.new_nonce`).

    Returns:
        The :class:`RunOutcome`.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If an argument is out of range, an envelope carries another schema or a snapshot was made for
            one, or one unit appears twice with different envelopes.
        RationalizationError: If the distinct misses pass ``max_requests``, a provider fails permanently,
            (``strict``) a request fails, or a request passes its own bound.
        BaseException: Whatever ``on_event`` (a ``KeyboardInterrupt`` included), a provider or a hook raises
            otherwise, unchanged.

    A run that stops waits for each request still in flight only until its own deadline (module notes, 6), and a
    request past its own bound abandons its worker (module notes, 11). That worker is a non-daemon
    ``concurrent.futures`` thread: a call no deadline can cut (a custom provider, a token source, a hung file system)
    keeps it alive, and can delay the interpreter's exit, until the call returns.
    """
    pairs = _check_work(work)
    _check_stores(schema, schema_version, memory_cache, file_cache)
    for unit, envelope in pairs:
        if envelope.schema_json != schema.wrapper_json:
            raise ValueError(f"the envelope of unit {unit.unit_id} carries another schema than the run's")
    snapshots = _check_snapshots(providers, schema)
    checked = _check_limits(limits)
    hooks = _check_hooks(on_event, clock, sleep, rng, nonce)
    slots, requests, file_lookups = _resolve_plan(pairs, snapshots, checked, schema_version, _preflight)
    deliver = _deliverer(hooks.on_event)
    probe = _probe(requests, memory_cache, file_cache, deliver, file_lookups)
    if len(probe.misses) > checked.max_requests:
        raise RationalizationError(
            f"this run needs {len(probe.misses)} requests (distinct cache misses), past max_requests "
            f"{checked.max_requests}: raise max_requests, or send fewer units or models"
        )
    used = {request.provider.model_id for request in probe.misses}
    for snapshot in snapshots:
        if snapshot.open is not None and snapshot.model_id in used:
            snapshot.open()
    prompt_hashes = {envelope.prompt: sha256_hex(envelope.prompt) for _, envelope in pairs}
    stores = _Stores(memory_cache, file_cache, schema.model.__name__, schema_version, schema.sha256, prompt_hashes)
    results = _execute(_Run(checked, snapshots, hooks, stores), probe.misses, deliver)
    return _assemble(slots, probe, results, snapshots)
