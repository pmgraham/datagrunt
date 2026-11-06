"""Tests for ``datagrunt.rationalization.runner``: units × providers into requests and responses (plan §4.6, R11).

Every provider here is a fake (a ``CallableProvider`` or a small provider class): no test reaches a network. Time,
waits and jitter are injected where a test pins them; the concurrency tests use real threads and short real sleeps.
"""

import json
import random
import sys
import threading
import time
from typing import Annotated

import pytest
from pdf_tests.documents import ids, metadata, page
from pydantic import BaseModel, Field

from datagrunt.pdf.elements import Paragraph, PDFDocument
from datagrunt.rationalization import Key, RationalizationError
from datagrunt.rationalization import runner as runner_module
from datagrunt.rationalization.cache import FileCache, MemoryCache, request_key
from datagrunt.rationalization.envelope import build_envelopes, render_element
from datagrunt.rationalization.providers.base import PermanentProviderError, ProviderError, TransientProviderError
from datagrunt.rationalization.providers.callable import CallableProvider
from datagrunt.rationalization.runner import (
    EVENT_KINDS,
    FailedUnit,
    ModelUsage,
    ProviderSnapshot,
    RunLimits,
    RunOutcome,
    TelemetryEvent,
    run_requests,
    snapshot_providers,
)
from datagrunt.rationalization.schema import prepare
from datagrunt.rationalization.units import build_units
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse


class Part(BaseModel):
    code: Annotated[str, Key]


SCHEMA = prepare(Part)
PROMPT = "Extract parts."
EVENT = {
    "kind": "request",
    "unit_id": "doc:p0001_e000-p0001_e000",
    "model_id": "callable:a",
    "attempt": 1,
    "status_code": None,
    "latency_ms": 1.5,
    "detail": "complete",
}


def make_document(texts: tuple[str, ...], source_name: str = "report.pdf") -> PDFDocument:
    """One page per text, each holding one paragraph of it."""
    pages = tuple(page(number, (Paragraph(**ids(number, 0), text=text),)) for number, text in enumerate(texts, 1))
    return PDFDocument(metadata=metadata(len(pages), source_name=source_name), pages=pages)


def make_work(
    count: int = 1,
    *,
    texts: tuple[str, ...] | None = None,
    source_name: str = "report.pdf",
    prompt: str = PROMPT,
    context: object = None,
    schema=SCHEMA,
):
    """``count`` one-paragraph pages, as the runner's ``(Unit, Envelope)`` pairs (one unit per page)."""
    shown = texts if texts is not None else tuple(f"Part PN-{number:03d}" for number in range(count))
    document = make_document(shown, source_name)
    units = build_units(document, max_unit_chars=10_000, render=render_element).units
    envelopes = build_envelopes(units, document, prompt=prompt, context=context, schema_json=schema.wrapper_json)
    return list(zip(units, envelopes, strict=True))


def answer(request) -> ProviderResponse:
    """A complete response naming the request's unit, so each (unit, model) answer is told apart."""
    content = {"records": [{"record": {"code": request.unit_id}, "grounding_quotes": [], "source_element_ids": []}]}
    return ProviderResponse(content=content, raw_text="{}", finish="complete", input_tokens=7, output_tokens=3)


# ---------------------------------------------------------------------------------------------------------------
# TelemetryEvent, FailedUnit, ModelUsage, RunOutcome, RunLimits
# ---------------------------------------------------------------------------------------------------------------


def test_event_kinds_are_the_six_named():
    assert EVENT_KINDS == ("cache_hit", "cache_corrupt", "request", "retry", "unit_failed", "cache_write_failed")


def test_event_holds_its_fields_and_is_frozen():
    event = TelemetryEvent(**EVENT)
    assert (event.kind, event.attempt, event.latency_ms, event.detail) == ("request", 1, 1.5, "complete")
    assert type(TelemetryEvent(**{**EVENT, "latency_ms": 2}).latency_ms) is float
    with pytest.raises(AttributeError):
        event.kind = "retry"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("kind", "requests", ValueError),
        ("kind", 1, TypeError),
        ("unit_id", "", ValueError),
        ("unit_id", None, TypeError),
        ("model_id", " ", ValueError),
        ("attempt", -1, ValueError),
        ("attempt", 12, ValueError),
        ("attempt", True, TypeError),
        ("attempt", 1.0, TypeError),
        ("status_code", 99, ValueError),
        ("status_code", 600, ValueError),
        ("status_code", "500", TypeError),
        ("latency_ms", -0.1, ValueError),
        ("latency_ms", float("nan"), ValueError),
        ("latency_ms", float("inf"), ValueError),
        ("latency_ms", True, TypeError),
        ("latency_ms", "1", TypeError),
        ("detail", "two\nlines", ValueError),
        ("detail", "x" * 301, ValueError),
        ("detail", " padded", ValueError),
        ("detail", b"bytes", TypeError),
    ],
)
def test_event_refuses_a_bad_field(field, value, error):
    with pytest.raises(error):
        TelemetryEvent(**{**EVENT, field: value})


FAILED = {
    "unit_id": "doc:p0001_e000-p0001_e000",
    "model_id": "callable:a",
    "status_code": 503,
    "error_message": "retries exhausted after 3 attempts",
    "attempts": 3,
}


def test_failed_unit_holds_its_fields():
    failed = FailedUnit(**FAILED)
    assert (failed.status_code, failed.attempts) == (503, 3)
    assert FailedUnit(**{**FAILED, "status_code": None, "attempts": 0}).attempts == 0


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("unit_id", "", ValueError),
        ("model_id", 3, TypeError),
        ("status_code", 700, ValueError),
        ("status_code", True, TypeError),
        ("error_message", "", ValueError),
        ("error_message", "a\nb", ValueError),
        ("error_message", None, TypeError),
        ("attempts", -1, ValueError),
        ("attempts", 12, ValueError),
        ("attempts", 2.0, TypeError),
    ],
)
def test_failed_unit_refuses_a_bad_field(field, value, error):
    with pytest.raises(error):
        FailedUnit(**{**FAILED, field: value})


USAGE = {"requests": 2, "retries": 1, "input_tokens": 10, "output_tokens": 4, "latency_ms": 3}


def test_model_usage_holds_its_fields():
    usage = ModelUsage(**USAGE)
    assert (usage.requests, usage.retries, usage.input_tokens, usage.output_tokens) == (2, 1, 10, 4)
    assert usage.latency_ms == 3.0 and type(usage.latency_ms) is float


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("requests", -1, ValueError),
        ("retries", True, TypeError),
        ("input_tokens", -5, ValueError),
        ("output_tokens", 1.5, TypeError),
        ("latency_ms", -1.0, ValueError),
        ("latency_ms", float("nan"), ValueError),
        ("latency_ms", None, TypeError),
    ],
)
def test_model_usage_refuses_a_bad_field(field, value, error):
    with pytest.raises(error):
        ModelUsage(**{**USAGE, field: value})


def outcome_fields(**changes):
    """RunOutcome's fields; a ``FAILED`` or ``USAGE`` dict in ``changes`` stands for the object built from it."""
    response = ProviderResponse(content=None, raw_text="", finish="refused", input_tokens=None, output_tokens=None)
    for name, value in changes.items():
        if name == "failed_units" and isinstance(value, list):
            changes[name] = [FailedUnit(**item) for item in value]
        if name == "usage" and isinstance(value, dict) and 1 in value:
            changes[name] = {1: ModelUsage(**value[1])}
    fields = {
        "responses": {("u", "m"): response},
        "failed_units": (FailedUnit(**FAILED),),
        "cache_hits": 1,
        "cache_misses": 2,
        "cache_corrupt": 0,
        "cache_write_failures": 0,
        "usage": {"m": ModelUsage(**USAGE)},
        "models": (("m", "meta", "callable"),),
    }
    return {**fields, **changes}


def test_outcome_holds_read_only_copies():
    given = outcome_fields()
    outcome = RunOutcome(**given)
    given["responses"][("v", "m")] = given["responses"][("u", "m")]
    given["usage"]["n"] = ModelUsage(**USAGE)
    assert list(outcome.responses) == [("u", "m")] and list(outcome.usage) == ["m"]
    with pytest.raises(TypeError):
        outcome.responses[("w", "m")] = None  # type: ignore[index]
    with pytest.raises(TypeError):
        outcome.usage["n"] = None  # type: ignore[index]


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"responses": [("u", "m")]}, TypeError),
        ({"responses": {"u": None}}, TypeError),
        ({"responses": {("u",): None}}, TypeError),
        ({"responses": {("u", 1): None}}, TypeError),
        ({"responses": {("u", "m"): {"content": None}}}, TypeError),
        ({"failed_units": [FAILED]}, TypeError),
        ({"failed_units": ("x",)}, TypeError),
        ({"cache_hits": -1}, ValueError),
        ({"cache_misses": "2"}, TypeError),
        ({"cache_corrupt": -1}, ValueError),
        ({"cache_write_failures": False}, TypeError),
        ({"usage": {"m": USAGE}}, TypeError),
        ({"usage": {1: USAGE}}, TypeError),
        ({"usage": ()}, TypeError),
        ({"models": [("m", "meta", "callable")]}, TypeError),
        ({"models": (("m", "meta"),)}, TypeError),
        ({"models": (("m", "meta", 3),)}, TypeError),
    ],
)
def test_outcome_refuses_a_bad_field(changes, error):
    with pytest.raises(error):
        RunOutcome(**outcome_fields(**changes))


def test_limits_defaults_are_the_plan_defaults():
    limits = RunLimits()
    assert (limits.max_concurrency, limits.max_concurrency_per_provider, limits.rpm_limit) == (8, 4, None)
    assert (limits.timeout_s, limits.max_retries, limits.max_output_tokens) == (60.0, 2, 8_192)
    assert (limits.max_requests, limits.strict) == (1_000, False)
    assert type(RunLimits(timeout_s=5).timeout_s) is float


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("max_concurrency", 0, ValueError),
        ("max_concurrency", 65, ValueError),
        ("max_concurrency", 2.0, TypeError),
        ("max_concurrency_per_provider", 0, ValueError),
        ("max_concurrency_per_provider", 65, ValueError),
        ("max_concurrency_per_provider", True, TypeError),
        ("rpm_limit", 0, ValueError),
        ("rpm_limit", 100_001, ValueError),
        ("rpm_limit", 60.0, TypeError),
        ("timeout_s", 0.999, ValueError),
        ("timeout_s", 3_600.5, ValueError),
        ("timeout_s", float("inf"), ValueError),
        ("timeout_s", float("nan"), ValueError),
        ("timeout_s", 10**400, ValueError),
        ("timeout_s", True, TypeError),
        ("timeout_s", "60", TypeError),
        ("max_retries", -1, ValueError),
        ("max_retries", 11, ValueError),
        ("max_retries", None, TypeError),
        ("max_output_tokens", 0, ValueError),
        ("max_output_tokens", 131_073, ValueError),
        ("max_output_tokens", 8192.0, TypeError),
        ("max_requests", 0, ValueError),
        ("max_requests", 100_001, ValueError),
        ("max_requests", "1000", TypeError),
        ("strict", 1, TypeError),
        ("strict", None, TypeError),
    ],
)
def test_limits_refuse_a_bad_bound(field, value, error):
    with pytest.raises(error):
        RunLimits(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_concurrency", 1),
        ("max_concurrency", 64),
        ("max_concurrency_per_provider", 64),
        ("rpm_limit", 1),
        ("rpm_limit", 100_000),
        ("timeout_s", 1.0),
        ("timeout_s", 3_600),
        ("max_retries", 0),
        ("max_retries", 10),
        ("max_output_tokens", 1),
        ("max_output_tokens", 131_072),
        ("max_requests", 1),
        ("max_requests", 100_000),
        ("strict", True),
    ],
)
def test_limits_take_each_bound_at_its_edge(field, value):
    assert getattr(RunLimits(**{field: value}), field) == value


@pytest.mark.parametrize(
    ("timeout_s", "max_retries", "expected"),
    [(60.0, 2, 3 * (60.0 + 2.0) + 2 * 30.0), (1.0, 0, 3.0), (10.0, 10, 11 * 12.0 + 10 * 30.0)],
)
def test_unit_deadline_is_attempts_with_slack_plus_capped_waits(timeout_s, max_retries, expected):
    limits = RunLimits(timeout_s=timeout_s, max_retries=max_retries)
    assert limits.unit_deadline_s(slots=4) == pytest.approx(expected)
    assert runner_module.ATTEMPT_SLACK_S == 2.0 and runner_module.RETRY_AFTER_CAP_S == 30.0
    assert runner_module.JITTER_CAP_S < runner_module.RETRY_AFTER_CAP_S  # 30 s per wait covers jitter too


def test_with_rpm_limit_the_unit_deadline_adds_each_retrys_token_wait():
    limits = RunLimits(timeout_s=10, max_retries=2, rpm_limit=30)  # a token every 2 s
    # each retry's token waits behind at most one token per slot of its rate key
    assert limits.unit_deadline_s(slots=4) == pytest.approx(3 * 12.0 + 2 * 30.0 + 2 * 4 * 2.0)
    assert RunLimits(timeout_s=10, max_retries=0, rpm_limit=30).unit_deadline_s(slots=4) == pytest.approx(12.0)
    with pytest.raises(ValueError):
        limits.unit_deadline_s(slots=0)
    with pytest.raises(TypeError):
        limits.unit_deadline_s(slots=2.0)


# ---------------------------------------------------------------------------------------------------------------
# The happy path
# ---------------------------------------------------------------------------------------------------------------


class Recorder:
    """A provider function that records each request (thread-safe) and answers it with :func:`answer`."""

    def __init__(self, respond=answer):
        self.requests = []
        self.threads = []
        self._lock = threading.Lock()
        self._respond = respond

    def __call__(self, request):
        with self._lock:
            self.requests.append(request)
            self.threads.append(threading.get_ident())
        return self._respond(request)


def run(work, providers, *, schema=SCHEMA, **options):
    """Snapshot ``providers``, then ``run_requests`` with a fresh memory cache and schema version 1.0 unless
    ``options`` say otherwise."""
    options.setdefault("memory_cache", MemoryCache())
    options.setdefault("schema_version", "1.0")
    return run_requests(work, snapshot_providers(providers, schema), schema, **options)


def nonce_of(user_envelope: str) -> str:
    return user_envelope.split(">", 1)[0].removeprefix("<caller_context_")


def test_each_unit_and_provider_gets_one_request_and_its_response():
    work = make_work(2)
    first, second = Recorder(), Recorder()
    providers = [CallableProvider("callable:a", first), CallableProvider("callable:b", second)]
    outcome = run(work, providers)
    unit_ids = [unit.unit_id for unit, _ in work]
    assert list(outcome.responses) == [(u, m) for u in unit_ids for m in ("callable:a", "callable:b")]
    for (unit_id, _), response in outcome.responses.items():
        assert response.content["records"][0]["record"]["code"] == unit_id
    assert outcome.failed_units == ()
    assert (outcome.cache_hits, outcome.cache_misses, outcome.cache_corrupt, outcome.cache_write_failures) == (
        0,
        4,
        0,
        0,
    )
    assert sorted(request.unit_id for request in first.requests) == unit_ids
    assert {request.model_id for request in second.requests} == {"callable:b"}


def test_a_request_carries_the_rendered_envelope_the_schema_and_the_limits():
    ((unit, envelope),) = make_work(1)
    recorder = Recorder()
    run(
        [(unit, envelope)],
        [CallableProvider("callable:a", recorder)],
        limits=RunLimits(max_output_tokens=512, timeout_s=9),
    )
    (request,) = recorder.requests
    assert (request.unit_id, request.model_id, request.max_output_tokens, request.timeout_s) == (
        unit.unit_id,
        "callable:a",
        512,
        9.0,
    )
    assert (request.system_prompt, request.user_envelope) == envelope.render(nonce_of(request.user_envelope))
    assert request.json_schema == SCHEMA.wrapper  # a callable's transform is the identity


def test_usage_counts_requests_tokens_and_latency_per_model():
    work = make_work(3)
    outcome = run(work, [CallableProvider("callable:a", Recorder()), CallableProvider("callable:b", Recorder())])
    assert list(outcome.usage) == ["callable:a", "callable:b"]
    usage = outcome.usage["callable:a"]
    assert (usage.requests, usage.retries, usage.input_tokens, usage.output_tokens) == (3, 0, 21, 9)
    assert usage.latency_ms >= 0.0


def test_no_work_sends_nothing():
    recorder = Recorder()
    outcome = run([], [CallableProvider("callable:a", recorder)])
    assert recorder.requests == [] and dict(outcome.responses) == {} and outcome.cache_misses == 0
    assert outcome.usage["callable:a"] == ModelUsage(
        requests=0, retries=0, input_tokens=0, output_tokens=0, latency_ms=0
    )


# ---------------------------------------------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------------------------------------------


def valid_arguments():
    work = make_work(1)
    recorder = Recorder()
    return recorder, {
        "work": work,
        "providers": snapshot_providers([CallableProvider("callable:a", recorder)], SCHEMA),
        "schema": SCHEMA,
        "schema_version": "1.0",
        "memory_cache": MemoryCache(),
    }


def other_document_envelope():
    return make_work(1, source_name="other.pdf")[0][1]


@pytest.mark.parametrize(
    ("name", "make", "error"),
    [
        ("work", lambda work: iter(work), TypeError),
        ("work", lambda work: "work", TypeError),
        ("work", lambda work: [work[0][0]], TypeError),
        ("work", lambda work: [(work[0][0],)], TypeError),
        ("work", lambda work: [(work[0][1], work[0][0])], TypeError),
        ("work", lambda work: [(work[0][0], "envelope")], TypeError),
        ("work", lambda work: [(work[0][0], other_document_envelope())], ValueError),
        ("providers", lambda work: (), ValueError),
        ("providers", lambda work: iter(()), TypeError),
        ("providers", lambda work: "callable:a", TypeError),
        ("providers", lambda work: [CallableProvider("callable:a", answer)], TypeError),
        ("schema", lambda work: SCHEMA.wrapper, TypeError),
        ("schema_version", lambda work: "", ValueError),
        ("schema_version", lambda work: 1, TypeError),
        ("memory_cache", lambda work: None, TypeError),
        ("memory_cache", lambda work: {}, TypeError),
        ("file_cache", lambda work: "cache_dir", TypeError),
        ("limits", lambda work: {"max_retries": 1}, TypeError),
        ("on_event", lambda work: "print", TypeError),
        ("clock", lambda work: 0.0, TypeError),
        ("clock", lambda work: None, TypeError),
        ("sleep", lambda work: 1.0, TypeError),
        ("rng", lambda work: 42, TypeError),
        ("rng", lambda work: object(), TypeError),
        ("nonce", lambda work: "0" * 32, TypeError),
    ],
)
def test_run_refuses_a_bad_argument_before_any_request(name, make, error):
    recorder, arguments = valid_arguments()
    arguments[name] = make(arguments["work"])
    with pytest.raises(error):
        run_requests(**arguments)
    assert recorder.requests == []


def test_run_takes_an_rng_with_a_uniform_method_and_a_tuple_of_work():
    recorder, arguments = valid_arguments()

    class Uniform:
        def uniform(self, low, high):
            return low

    arguments.update(work=tuple(arguments["work"]), providers=tuple(arguments["providers"]), rng=Uniform())
    assert len(run_requests(**arguments).responses) == 1


# ---------------------------------------------------------------------------------------------------------------
# Provider snapshots
# ---------------------------------------------------------------------------------------------------------------


class FakeProvider:
    """A provider with plain attributes behind its properties, counting every read of them and of its schema."""

    def __init__(self, model_id="fake:a", *, respond=answer, rate_key=None, params=None, family="meta"):
        self._values = {
            "model_id": model_id,
            "provider_kind": "fake",
            "rate_key": rate_key if rate_key is not None else f"rate:{model_id}",
            "model_family": family,
            "generation_params": params if params is not None else {"temperature": 0.0},
        }
        self.reads = 0
        self.transforms = 0
        self.requests = []
        self._respond = respond
        self._lock = threading.Lock()

    def _read(self, name):
        with self._lock:
            self.reads += 1
            return self._values[name]

    model_id = property(lambda self: self._read("model_id"))
    provider_kind = property(lambda self: self._read("provider_kind"))
    rate_key = property(lambda self: self._read("rate_key"))
    model_family = property(lambda self: self._read("model_family"))
    generation_params = property(lambda self: dict(self._read("generation_params")))

    def transform_schema(self, schema):
        self.transforms += 1
        return {"wrapped": dict(schema)}

    def generate(self, request):
        with self._lock:
            self.requests.append(request)
        return self._respond(request)


def test_a_provider_is_read_once_at_preflight_and_never_after():
    provider = FakeProvider()
    reads_at_first_request = []

    def respond(request):
        reads_at_first_request.append(provider.__dict__["reads"])
        provider._values.update(model_id="fake:changed", rate_key="rate:changed", generation_params={"temperature": 9})
        provider.generate = lambda request: pytest.fail("the generate bound at preflight is the one called")
        return answer(request)

    provider._respond = respond
    work = make_work(3)
    outcome = run(work, [provider], limits=RunLimits(max_concurrency=1))
    assert provider.reads == reads_at_first_request[0]  # nothing read after preflight
    assert provider.transforms == 1  # once per provider, whatever the units
    assert len(provider.requests) == 3
    assert {request.model_id for request in provider.requests} == {"fake:a"}
    assert [request.json_schema for request in provider.requests] == [{"wrapped": SCHEMA.wrapper}] * 3
    assert list(outcome.responses) == [(unit.unit_id, "fake:a") for unit, _ in work]


@pytest.mark.parametrize(
    ("member", "second_read"),
    [
        ("model_id", "two\nlines"),
        ("provider_kind", ""),
        ("rate_key", "x" * 513),
        ("model_family", "Not A Family"),
        ("generation_params", {"seed": (1, 2)}),
    ],
)
def test_the_snapshot_itself_is_checked(member, second_read):
    provider = FakeProvider()
    reads = {"count": 0}
    first = provider._values[member]

    def changing(self):
        reads["count"] += 1
        return first if reads["count"] == 1 else second_read

    shifty = type("Shifty", (FakeProvider,), {member: property(changing)})()
    with pytest.raises(RationalizationError, match=member):
        run(make_work(1), [shifty])
    assert shifty.requests == []


def test_a_transformed_schema_that_is_not_exactly_json_is_refused():
    provider = FakeProvider()
    provider.transform_schema = lambda schema: {"items": (1, 2)}
    with pytest.raises(RationalizationError):
        run(make_work(1), [provider])
    assert provider.requests == []


def test_a_provider_whose_params_hold_max_output_tokens_is_refused():
    provider = FakeProvider(params={"temperature": 0.0, "max_output_tokens": 100})
    with pytest.raises(RationalizationError, match="max_output_tokens"):
        run(make_work(1), [provider])
    assert provider.requests == []


def test_two_providers_with_one_model_id_are_refused():
    first, second = FakeProvider("fake:a"), FakeProvider("fake:a", rate_key="other")
    with pytest.raises(RationalizationError, match="fake:a"):
        run(make_work(1), [first, second])
    assert first.requests == second.requests == []


def test_an_object_that_is_no_provider_is_refused():
    with pytest.raises(RationalizationError, match="not a ModelProvider"):
        run(make_work(1), [object()])


class OpeningProvider(FakeProvider):
    """A provider with ``open()``, recording the thread it ran on and whether a request came first."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.opened = []

    def open(self):
        self.opened.append((threading.get_ident(), len(self.requests)))


def test_open_is_called_once_at_preflight_on_the_callers_thread():
    provider, plain = OpeningProvider(), FakeProvider("fake:b")
    run(make_work(3), [provider, plain])
    assert provider.opened == [(threading.get_ident(), 0)]
    assert len(provider.requests) == len(plain.requests) == 3


def test_open_that_raises_stops_the_run_before_any_request():
    provider = OpeningProvider()
    provider.open = lambda: (_ for _ in ()).throw(RationalizationError("SSL_CERT_FILE names no file"))
    with pytest.raises(RationalizationError, match="SSL_CERT_FILE"):
        run(make_work(1), [provider])
    assert provider.requests == []


@pytest.mark.parametrize(
    ("member", "value"),
    [("open", "not callable"), ("max_connections", 0), ("max_connections", 1_025), ("max_connections", True)],
)
def test_an_optional_member_that_is_not_usable_is_refused(member, value):
    provider = type("Extra", (FakeProvider,), {member: value})()
    with pytest.raises(RationalizationError, match=member):
        run(make_work(1), [provider])
    assert provider.requests == []


# ---------------------------------------------------------------------------------------------------------------
# Keys, deduplication and the caches
# ---------------------------------------------------------------------------------------------------------------


def expected_key(envelope, provider: CallableProvider, *, max_output_tokens=8_192, schema_version="1.0"):
    return request_key(
        provider_kind=provider.provider_kind,
        model_id=provider.model_id,
        generation_params={**provider.generation_params, "max_output_tokens": max_output_tokens},
        envelope=envelope,
        provider_schema=provider.transform_schema(SCHEMA.wrapper),
        schema_version=schema_version,
    )


def test_a_response_is_stored_under_its_key_and_a_second_run_replays_it_without_a_call():
    work = make_work(2)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    memory = MemoryCache()
    first = run(work, [provider], memory_cache=memory)
    assert [memory.get(expected_key(envelope, provider)).hit for _, envelope in work] == [True, True]
    second = run(work, [provider], memory_cache=memory)
    assert len(recorder.requests) == 2  # the second run made none
    assert dict(second.responses) == dict(first.responses)
    assert (second.cache_hits, second.cache_misses) == (2, 0)
    assert second.usage["callable:a"].requests == 0


def test_runs_differing_only_in_max_output_tokens_never_share_an_entry():
    work = make_work(1)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    memory = MemoryCache()
    run(work, [provider], memory_cache=memory, limits=RunLimits(max_output_tokens=100))
    outcome = run(work, [provider], memory_cache=memory, limits=RunLimits(max_output_tokens=200))
    assert [request.max_output_tokens for request in recorder.requests] == [100, 200]
    assert outcome.cache_hits == 0
    envelope = work[0][1]
    assert expected_key(envelope, provider, max_output_tokens=100) != expected_key(
        envelope, provider, max_output_tokens=200
    )


def test_the_schema_version_keys_the_entry():
    work = make_work(1)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    memory = MemoryCache()
    run(work, [provider], memory_cache=memory, schema_version="1.0")
    run(work, [provider], memory_cache=memory, schema_version="2.0")
    assert len(recorder.requests) == 2


def test_identical_requests_are_sent_once_and_answer_every_slot():
    (pair,) = make_work(1)
    recorder = Recorder()
    outcome = run([pair, pair], [CallableProvider("callable:a", recorder)])
    assert len(recorder.requests) == 1
    assert list(outcome.responses) == [(pair[0].unit_id, "callable:a")]
    assert (outcome.cache_misses, outcome.usage["callable:a"].requests) == (1, 1)


def test_one_unit_with_two_different_envelopes_is_refused():
    ((unit, envelope),) = make_work(1)
    other = build_envelopes(
        [unit], make_document(("Part PN-000",)), prompt="Another prompt.", context=None, schema_json=SCHEMA.wrapper_json
    )[0]
    recorder = Recorder()
    with pytest.raises(ValueError, match=unit.unit_id):
        run([(unit, envelope), (unit, other)], [CallableProvider("callable:a", recorder)])
    assert recorder.requests == []


def test_preflight_refuses_more_misses_than_max_requests_before_any_call_or_open():
    work = make_work(3)
    first, second = OpeningProvider("fake:a"), OpeningProvider("fake:b")
    with pytest.raises(RationalizationError, match=r"6 requests.*max_requests 5"):
        run(work, [first, second], limits=RunLimits(max_requests=5))
    assert first.requests == second.requests == []
    assert first.opened == second.opened == []


def test_cache_hits_do_not_count_against_max_requests():
    work = make_work(3)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    memory = MemoryCache()
    run(work[:2], [provider], memory_cache=memory)
    outcome = run(work, [provider], memory_cache=memory, limits=RunLimits(max_requests=1))
    assert len(recorder.requests) == 3 and (outcome.cache_hits, outcome.cache_misses) == (2, 1)


# ---------------------------------------------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------------------------------------------


class Events:
    """An ``on_event`` hook recording each event and the thread it was delivered on."""

    def __init__(self):
        self.events = []
        self.threads = set()

    def __call__(self, event):
        self.events.append(event)
        self.threads.add(threading.get_ident())

    def brief(self):
        return [(event.kind, event.unit_id, event.model_id, event.attempt) for event in self.events]


def test_each_attempt_and_each_cache_hit_is_an_event_on_the_callers_thread():
    work = make_work(2)
    provider = CallableProvider("callable:a", Recorder())
    memory = MemoryCache()
    sent = Events()
    run(work, [provider], memory_cache=memory, on_event=sent)
    unit_ids = [unit.unit_id for unit, _ in work]
    assert sent.brief() == [("request", unit_id, "callable:a", 1) for unit_id in unit_ids]
    assert {(event.status_code, event.detail) for event in sent.events} == {(None, "complete")}
    assert all(event.latency_ms is not None and event.latency_ms >= 0 for event in sent.events)
    assert sent.threads == {threading.get_ident()}
    replayed = Events()
    run(work, [provider], memory_cache=memory, on_event=replayed)
    assert replayed.brief() == [("cache_hit", unit_id, "callable:a", 0) for unit_id in unit_ids]
    assert {(event.detail, event.latency_ms) for event in replayed.events} == {("memory", None)}


def jittery(seed):
    """A provider function that sleeps a random few milliseconds before answering, so workers finish out of order."""
    source = random.Random(seed)
    lock = threading.Lock()

    def respond(request):
        with lock:
            delay = source.uniform(0.0, 0.01)
        time.sleep(delay)
        return answer(request)

    return respond


def test_events_arrive_in_request_order_whatever_order_the_workers_finish_in():
    work = make_work(8)
    expected = None
    for seed in range(5):
        providers = [CallableProvider("callable:a", jittery(seed)), CallableProvider("callable:b", jittery(seed + 99))]
        sent = Events()
        run(work, providers, on_event=sent, limits=RunLimits(max_concurrency=8))
        assert sent.threads == {threading.get_ident()}
        if expected is None:
            expected = sent.brief()
        assert sent.brief() == expected
    assert expected == [
        ("request", unit.unit_id, model, 1) for unit, _ in work for model in ("callable:a", "callable:b")
    ]


def test_a_file_cache_hit_replays_the_response_byte_for_byte_without_a_call(tmp_path):
    work = make_work(2)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    first = run(work, [provider], file_cache=FileCache(tmp_path))
    keys = [expected_key(envelope, provider) for _, envelope in work]
    assert [FileCache(tmp_path).get(key).hit for key in keys] == [True, True]
    sent = Events()
    second = run(work, [provider], file_cache=FileCache(tmp_path), on_event=sent)  # a fresh memory cache
    assert len(recorder.requests) == 2
    assert [response.to_json() for response in second.responses.values()] == [
        response.to_json() for response in first.responses.values()
    ]
    assert [(event.kind, event.detail) for event in sent.events] == [("cache_hit", "file")] * 2
    assert (second.cache_hits, second.cache_misses) == (2, 0)


def test_the_memory_cache_answers_before_the_file_cache(tmp_path):
    work = make_work(1)
    provider = CallableProvider("callable:a", Recorder())
    memory = MemoryCache()
    run(work, [provider], memory_cache=memory)  # only the memory cache holds it
    sent = Events()
    run(work, [provider], memory_cache=memory, file_cache=FileCache(tmp_path), on_event=sent)
    assert [event.detail for event in sent.events] == ["memory"]


def test_a_corrupt_file_entry_is_an_event_a_miss_and_is_rewritten(tmp_path):
    work = make_work(2)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    run(work, [provider], file_cache=FileCache(tmp_path))
    key = expected_key(work[1][1], provider)
    (tmp_path / key[:2] / f"{key}.json").write_text("{not json", encoding="utf-8")
    sent = Events()
    outcome = run(work, [provider], file_cache=FileCache(tmp_path), on_event=sent)
    assert len(recorder.requests) == 3  # the corrupt one was asked again
    assert [(event.kind, event.unit_id, event.detail) for event in sent.events] == [
        ("cache_hit", work[0][0].unit_id, "file"),
        ("cache_corrupt", work[1][0].unit_id, "not_json"),
        ("request", work[1][0].unit_id, "complete"),
    ]
    assert (outcome.cache_hits, outcome.cache_misses, outcome.cache_corrupt) == (1, 1, 1)
    assert FileCache(tmp_path).get(key).hit


class FailingFileCache(FileCache):
    """A file cache whose writes raise, or report the entry too large."""

    __slots__ = ("raises",)

    def __init__(self, cache_dir, *, raises):
        super().__init__(cache_dir)
        self.raises = raises

    def put(self, key, response, meta):
        if self.raises:
            raise RationalizationError(f"cannot write cache entry {key} under cache_dir {self.cache_dir}: disk full")
        return False


@pytest.mark.parametrize(("raises", "detail"), [(True, "file cache write failed"), (False, "past 8 MiB")])
def test_a_file_cache_write_failure_is_counted_and_the_run_goes_on(tmp_path, raises, detail):
    work = make_work(2)
    sent = Events()
    outcome = run(
        work,
        [CallableProvider("callable:a", Recorder())],
        file_cache=FailingFileCache(tmp_path, raises=raises),
        on_event=sent,
    )
    assert len(outcome.responses) == 2 and outcome.failed_units == ()
    assert outcome.cache_write_failures == 2
    failed = [event for event in sent.events if event.kind == "cache_write_failed"]
    assert [event.unit_id for event in failed] == [unit.unit_id for unit, _ in work]
    assert all(detail in event.detail and event.attempt == 1 for event in failed)
    assert [event.kind for event in sent.events] == ["request", "cache_write_failed"] * 2


def test_a_full_memory_cache_is_counted_and_the_run_goes_on():
    work = make_work(2)
    sent = Events()
    outcome = run(
        work,
        [CallableProvider("callable:a", Recorder())],
        memory_cache=MemoryCache(max_entries=1),
        on_event=sent,
        limits=RunLimits(max_concurrency=1),
    )
    assert len(outcome.responses) == 2 and outcome.cache_write_failures == 1
    assert [(event.kind, event.unit_id, event.detail) for event in sent.events if event.kind != "request"] == [
        ("cache_write_failed", work[1][0].unit_id, "memory cache full")
    ]


# ---------------------------------------------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------------------------------------------


class FakeTime:
    """An injected clock and sleep: time moves only when a test sleeps or advances it (thread-safe)."""

    def __init__(self):
        self.now = 0.0
        self.waits = []
        self._lock = threading.Lock()

    def clock(self):
        with self._lock:
            return self.now

    def sleep(self, seconds):
        with self._lock:
            self.waits.append(seconds)
            self.now += seconds

    def advance(self, seconds):
        with self._lock:
            self.now += seconds


class HalfJitter:
    """An injected rng: records each ``uniform`` range and returns its middle."""

    def __init__(self):
        self.ranges = []

    def uniform(self, low, high):
        self.ranges.append((low, high))
        return (low + high) / 2


def failing(times, error=lambda: TransientProviderError(503, None, "busy")):
    """A provider function raising ``error()`` on its first ``times`` calls, then answering."""
    calls = {"count": 0}

    def respond(request):
        calls["count"] += 1
        if calls["count"] <= times:
            raise error()
        return answer(request)

    return respond


def test_a_transient_failure_is_retried_with_full_jitter_until_it_succeeds():
    recorder = Recorder(failing(2))
    fake, jitter, sent = FakeTime(), HalfJitter(), Events()
    outcome = run(
        make_work(1),
        [CallableProvider("callable:a", recorder)],
        limits=RunLimits(max_retries=2),
        clock=fake.clock,
        sleep=fake.sleep,
        rng=jitter,
        on_event=sent,
    )
    assert len(recorder.requests) == 3 and len(outcome.responses) == 1 and outcome.failed_units == ()
    assert jitter.ranges == [(0.0, 1.5), (0.0, 3.0)]  # uniform(0, min(20, 0.75 * 2**attempt))
    assert fake.waits == [0.75, 1.5]
    assert [(event.kind, event.attempt, event.status_code) for event in sent.events] == [
        ("request", 1, 503),
        ("retry", 1, 503),
        ("request", 2, 503),
        ("retry", 2, 503),
        ("request", 3, None),
    ]
    assert sent.events[0].detail == "TransientProviderError" and "1.500 s" not in sent.events[1].detail
    usage = outcome.usage["callable:a"]
    assert (usage.requests, usage.retries) == (3, 2)
    assert len({nonce_of(request.user_envelope) for request in recorder.requests}) == 3  # a fresh nonce each


def test_the_jitter_window_doubles_to_its_20_second_cap():
    jitter, fake = HalfJitter(), FakeTime()
    run(
        make_work(1),
        [CallableProvider("callable:a", Recorder(failing(10)))],
        limits=RunLimits(max_retries=10),
        clock=fake.clock,
        sleep=fake.sleep,
        rng=jitter,
    )
    assert [high for _, high in jitter.ranges] == [1.5, 3.0, 6.0, 12.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0]
    assert all(low == 0.0 for low, _ in jitter.ranges)


@pytest.mark.parametrize(("retry_after_s", "wait"), [(100.0, 30.0), (30.0, 30.0), (5.0, 5.0), (0.0, 0.0)])
def test_retry_after_is_honoured_up_to_30_seconds_without_jitter(retry_after_s, wait):
    jitter, fake = HalfJitter(), FakeTime()
    error = lambda: TransientProviderError(429, retry_after_s, "slow down")  # noqa: E731
    run(
        make_work(1),
        [CallableProvider("callable:a", Recorder(failing(1, error)))],
        clock=fake.clock,
        sleep=fake.sleep,
        rng=jitter,
    )
    assert jitter.ranges == [] and fake.waits == ([wait] if wait else [])


def test_an_rng_answer_outside_its_window_is_refused():
    class Wild:
        def uniform(self, low, high):
            return high + 1

    fake = FakeTime()
    recorder = Recorder(failing(1))
    with pytest.raises(ValueError, match="uniform"):
        run(make_work(1), [CallableProvider("callable:a", recorder)], clock=fake.clock, sleep=fake.sleep, rng=Wild())
    assert len(recorder.requests) == 1  # the jitter came after a real attempt failed


def always_busy(request):
    raise TransientProviderError(503, None, "overloaded")


def test_exhausted_retries_record_a_failed_unit_and_the_run_goes_on():
    work = make_work(2)
    fake, sent = FakeTime(), Events()
    good, busy = Recorder(), Recorder(always_busy)
    outcome = run(
        work,
        [CallableProvider("callable:good", good), CallableProvider("callable:busy", busy)],
        limits=RunLimits(max_retries=1),
        clock=fake.clock,
        sleep=fake.sleep,
        rng=HalfJitter(),
        on_event=sent,
    )
    assert list(outcome.responses) == [(unit.unit_id, "callable:good") for unit, _ in work]
    assert [
        (failed.unit_id, failed.model_id, failed.status_code, failed.attempts) for failed in outcome.failed_units
    ] == [(unit.unit_id, "callable:busy", 503, 2) for unit, _ in work]
    assert "retries exhausted" in outcome.failed_units[0].error_message
    assert "overloaded" in outcome.failed_units[0].error_message  # the provider's bounded detail
    failures = [event for event in sent.events if event.kind == "unit_failed"]
    assert [(event.unit_id, event.attempt, event.status_code) for event in failures] == [
        (unit.unit_id, 2, 503) for unit, _ in work
    ]
    assert all("overloaded" not in event.detail for event in sent.events)  # no provider text in an event
    assert len(busy.requests) == 4 and outcome.usage["callable:busy"].retries == 2


def test_with_strict_exhausted_retries_stop_the_run():
    fake = FakeTime()
    with pytest.raises(RationalizationError, match=r"callable:busy.*2 attempts") as caught:
        run(
            make_work(1),
            [CallableProvider("callable:busy", always_busy)],
            limits=RunLimits(max_retries=1, strict=True),
            clock=fake.clock,
            sleep=fake.sleep,
            rng=HalfJitter(),
        )
    assert isinstance(caught.value.__cause__, TransientProviderError)


# ---------------------------------------------------------------------------------------------------------------
# Errors that stop the run
# ---------------------------------------------------------------------------------------------------------------


def refusing(request):
    raise PermanentProviderError(400, "temperature is not supported for this model")


def test_a_permanent_failure_raises_at_once_and_the_remaining_requests_never_start():
    recorder = Recorder(refusing)
    with pytest.raises(RationalizationError, match=r"callable:a.*HTTP 400.*temperature is not supported") as caught:
        run(make_work(4), [CallableProvider("callable:a", recorder)], limits=RunLimits(max_concurrency=1))
    assert isinstance(caught.value.__cause__, PermanentProviderError)
    assert len(recorder.requests) == 1


def test_a_permanent_failure_ends_another_requests_retry_wait_at_once():
    waiting = threading.Event()
    busy_calls = []

    def busy(request):
        busy_calls.append(request)
        waiting.set()
        raise TransientProviderError(429, 30.0, "slow down")  # the default sleep would wait 30 s

    def refuse_after_busy(request):
        waiting.wait(5)
        time.sleep(0.05)  # the busy request is in its wait by now
        refusing(request)

    threads_before = threading.active_count()
    started = time.monotonic()
    with pytest.raises(RationalizationError, match="callable:refuse"):
        run(
            make_work(1),
            [CallableProvider("callable:busy", busy), CallableProvider("callable:refuse", refuse_after_busy)],
        )
    assert time.monotonic() - started < 5
    assert len(busy_calls) == 1  # its retry never ran
    assert threading.active_count() == threads_before


@pytest.mark.parametrize("error", [KeyError("bug"), ProviderError(500, "neither transient nor permanent")])
def test_any_other_exception_from_a_provider_propagates_unchanged(error):
    def broken(request):
        raise error

    recorder = Recorder(broken)
    with pytest.raises(type(error)) as caught:
        run(make_work(3), [CallableProvider("callable:a", recorder)], limits=RunLimits(max_concurrency=1))
    assert caught.value is error
    assert len(recorder.requests) == 1


def test_a_provider_answering_something_else_than_a_response_is_a_type_error():
    provider = FakeProvider(respond=lambda request: {"records": []})
    with pytest.raises(TypeError, match="fake:a returned dict"):
        run(make_work(1), [provider])


# ---------------------------------------------------------------------------------------------------------------
# Deadlines
# ---------------------------------------------------------------------------------------------------------------


def slow_then_busy(fake, spent, retry_after_s=0.0):
    """A provider function: each call spends ``spent[i]`` seconds of fake time, then fails transiently."""
    calls = {"count": 0}

    def respond(request):
        fake.advance(spent[calls["count"]])
        calls["count"] += 1
        raise TransientProviderError(503, retry_after_s, "busy")

    return respond


def test_an_attempt_is_sent_with_the_time_left_before_the_unit_deadline():
    fake = FakeTime()
    limits = RunLimits(timeout_s=10, max_retries=1)  # unit deadline: 2 * (10 + 2) + 30 = 54 s
    recorder = Recorder(slow_then_busy(fake, [46.0, 0.0]))
    outcome = run(
        make_work(1), [CallableProvider("callable:a", recorder)], limits=limits, clock=fake.clock, sleep=fake.sleep
    )
    assert [request.timeout_s for request in recorder.requests] == [10.0, 8.0]
    assert "retries exhausted" in outcome.failed_units[0].error_message


def test_with_less_than_the_floor_left_the_unit_fails_without_sending():
    fake, sent = FakeTime(), Events()
    limits = RunLimits(timeout_s=10, max_retries=1)
    recorder = Recorder(slow_then_busy(fake, [53.5]))
    outcome = run(
        make_work(1),
        [CallableProvider("callable:a", recorder)],
        limits=limits,
        clock=fake.clock,
        sleep=fake.sleep,
        on_event=sent,
    )
    assert len(recorder.requests) == 1
    (failed,) = outcome.failed_units
    assert (failed.attempts, failed.status_code) == (1, 503) and "deadline passed" in failed.error_message
    assert [(event.kind, event.detail) for event in sent.events][-1] == ("unit_failed", "deadline passed")


def test_a_retry_wait_that_would_pass_the_deadline_is_not_waited():
    fake = FakeTime()
    limits = RunLimits(timeout_s=10, max_retries=1)
    recorder = Recorder(slow_then_busy(fake, [40.0], retry_after_s=30.0))
    outcome = run(
        make_work(1), [CallableProvider("callable:a", recorder)], limits=limits, clock=fake.clock, sleep=fake.sleep
    )
    assert fake.waits == [] and len(recorder.requests) == 1
    assert "deadline passed" in outcome.failed_units[0].error_message
    assert outcome.usage["callable:a"].retries == 0


def test_with_strict_a_passed_deadline_stops_the_run():
    fake = FakeTime()
    limits = RunLimits(timeout_s=10, max_retries=1, strict=True)
    recorder = Recorder(slow_then_busy(fake, [53.5]))
    with pytest.raises(RationalizationError, match="deadline passed") as caught:
        run(make_work(1), [CallableProvider("callable:a", recorder)], limits=limits, clock=fake.clock, sleep=fake.sleep)
    assert len(recorder.requests) == 1 and isinstance(caught.value.__cause__, TransientProviderError)


# ---------------------------------------------------------------------------------------------------------------
# Nonces
# ---------------------------------------------------------------------------------------------------------------

IN_TEXT = "0123456789abcdef0123456789abcdef"
FRESH = "fedcba9876543210fedcba9876543210"


def nonces(*values):
    """An injected nonce source answering ``values`` in turn, recording how many it gave."""
    drawn = []

    def draw():
        drawn.append(values[min(len(drawn), len(values) - 1)])
        return drawn[-1]

    return draw, drawn


def test_a_nonce_found_in_the_content_is_redrawn():
    work = make_work(texts=(f"Code {IN_TEXT}",))
    assert IN_TEXT in work[0][1].payload
    draw, drawn = nonces(IN_TEXT, IN_TEXT, IN_TEXT, FRESH)
    recorder = Recorder()
    run(work, [CallableProvider("callable:a", recorder)], nonce=draw)
    assert len(drawn) == 4
    assert nonce_of(recorder.requests[0].user_envelope) == FRESH


def test_nonce_draws_are_bounded():
    work = make_work(texts=(f"Code {IN_TEXT}",))
    draw, drawn = nonces(IN_TEXT)
    recorder = Recorder()
    with pytest.raises(RationalizationError, match="8"):
        run(work, [CallableProvider("callable:a", recorder)], nonce=draw)
    assert len(drawn) == runner_module.NONCE_DRAWS == 8 and recorder.requests == []


# ---------------------------------------------------------------------------------------------------------------
# Rate limits and concurrency
# ---------------------------------------------------------------------------------------------------------------


def stamping(fake, respond=answer):
    """A provider function recording the fake time of each call."""
    times = []

    def call(request):
        times.append(fake.clock())
        return respond(request)

    return call, times


def test_rpm_limit_spaces_requests_on_one_rate_key():
    fake = FakeTime()
    call, times = stamping(fake)
    run(
        make_work(4),
        [CallableProvider("callable:a", call)],
        limits=RunLimits(rpm_limit=30, max_concurrency=1),
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert times == [0.0, 2.0, 4.0, 6.0]  # 60 / 30 = 2 s apart
    assert fake.waits == [2.0, 2.0, 2.0]


def test_each_rate_key_has_its_own_bucket():
    fake = FakeTime()
    first, first_times = stamping(fake)
    second, second_times = stamping(fake)
    run(
        make_work(2),
        [CallableProvider("callable:a", first), CallableProvider("callable:b", second)],
        limits=RunLimits(rpm_limit=60, max_concurrency=1),
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert first_times == [0.0, 1.0] and second_times == [0.0, 1.0]


def test_a_retry_spends_a_token_too():
    fake = FakeTime()
    call, times = stamping(fake, failing(1, lambda: TransientProviderError(503, 0.0, "busy")))
    run(
        make_work(1),
        [CallableProvider("callable:a", call)],
        limits=RunLimits(rpm_limit=60),
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert times == [0.0, 1.0]


def test_a_first_attempt_waits_for_its_token_before_its_deadline_starts():
    fake = FakeTime()
    call, times = stamping(fake)
    outcome = run(
        make_work(2),
        [CallableProvider("callable:a", call)],
        limits=RunLimits(rpm_limit=1, timeout_s=1, max_retries=0),  # deadline 3 s, tokens 60 s apart
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert times == [0.0, 60.0] and outcome.failed_units == ()


def test_a_low_rpm_limit_sends_every_request_on_a_fake_clock():
    fake = FakeTime()
    call, times = stamping(fake)
    outcome = run(
        make_work(30),
        [CallableProvider("callable:a", call)],
        limits=RunLimits(rpm_limit=5, timeout_s=30, max_retries=0),  # a free-tier quota; 4 slots x 12 s > 32 s
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert outcome.failed_units == () and times == [12.0 * index for index in range(30)]


def test_a_low_rpm_limit_sends_every_request_in_real_time(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.5)  # unit deadline 1.5 s: a token must come by 0.5 s
    sent = []

    def call(request):
        sent.append(time.monotonic())
        return answer(request)

    outcome = run(
        make_work(20),
        [CallableProvider("callable:a", call)],
        limits=RunLimits(
            rpm_limit=1_200, timeout_s=1, max_retries=0, max_concurrency=32, max_concurrency_per_provider=32
        ),
    )  # 32 slots x 0.05 s per token > 1.5 s
    assert len(sent) == 20 and outcome.failed_units == ()
    gaps = [later - earlier for earlier, later in zip(sent, sent[1:], strict=False)]
    assert min(gaps) >= 0.05 * 0.9  # still spaced


class InFlight:
    """Counts requests in flight (real threads, real short sleeps), overall and per name, keeping the maxima."""

    def __init__(self, hold_s=0.02):
        self.hold_s = hold_s
        self.now = {}
        self.peak = {}
        self._lock = threading.Lock()

    def respond(self, *names):
        def call(request):
            with self._lock:
                for name in names:
                    self.now[name] = self.now.get(name, 0) + 1
                    self.peak[name] = max(self.peak.get(name, 0), self.now[name])
            time.sleep(self.hold_s)
            with self._lock:
                for name in names:
                    self.now[name] -= 1
            return answer(request)

        return call


def test_requests_in_flight_per_rate_key_never_pass_the_cap():
    flight = InFlight()
    run(
        make_work(12),
        [CallableProvider("callable:a", flight.respond("a"))],
        limits=RunLimits(max_concurrency=8, max_concurrency_per_provider=3),
    )
    assert flight.peak["a"] == 3


def test_a_providers_max_connections_lowers_its_cap():
    flight = InFlight()
    provider = type("Pooled", (FakeProvider,), {"max_connections": 2})(respond=flight.respond("a"))
    run(make_work(10), [provider], limits=RunLimits(max_concurrency=8, max_concurrency_per_provider=4))
    assert flight.peak["a"] == 2


def test_providers_sharing_a_rate_key_share_its_slots():
    flight = InFlight()
    providers = [
        FakeProvider("fake:a", rate_key="endpoint", respond=flight.respond("endpoint")),
        FakeProvider("fake:b", rate_key="endpoint", respond=flight.respond("endpoint")),
    ]
    run(make_work(8), providers, limits=RunLimits(max_concurrency=8, max_concurrency_per_provider=2))
    assert flight.peak["endpoint"] == 2


def test_requests_in_flight_overall_never_pass_max_concurrency():
    flight = InFlight()
    providers = [CallableProvider(f"callable:{name}", flight.respond("all")) for name in "abc"]
    run(make_work(8), providers, limits=RunLimits(max_concurrency=5, max_concurrency_per_provider=4))
    assert flight.peak["all"] == 5


def test_a_slow_rate_key_never_holds_back_another():
    flight = InFlight(hold_s=0.05)
    slow = FakeProvider("fake:slow", respond=flight.respond("slow"))
    fast_times = []

    def fast(request):
        fast_times.append(time.monotonic())
        return answer(request)

    started = time.monotonic()
    run(
        make_work(6),
        [slow, CallableProvider("callable:fast", fast)],
        limits=RunLimits(max_concurrency=2, max_concurrency_per_provider=1),
    )
    # the slow key holds one worker at a time; the fast key's six requests run on the other meanwhile, all within
    # the first slow request (a first-in-first-out queue would hold each fast one behind a slow one: 5 holds)
    assert max(fast_times) - started < 2 * flight.hold_s


# ---------------------------------------------------------------------------------------------------------------
# Cancellation, ordering and threads
# ---------------------------------------------------------------------------------------------------------------


class Stop(Exception):
    pass


def test_on_event_raising_cancels_the_run_and_propagates_unchanged():
    raised = Stop("enough")

    def on_event(event):
        raise raised

    recorder = Recorder()
    threads_before = threading.active_count()
    with pytest.raises(Stop) as caught:
        run(
            make_work(4),
            [CallableProvider("callable:a", recorder)],
            limits=RunLimits(max_concurrency=1),
            on_event=on_event,
        )
    assert caught.value is raised
    assert len(recorder.requests) == 1
    assert threading.active_count() == threads_before


def test_on_event_raising_at_preflight_stops_before_any_request():
    work = make_work(2)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    memory = MemoryCache()
    run(work[:1], [provider], memory_cache=memory)

    def on_event(event):
        raise Stop(event.kind)

    with pytest.raises(Stop, match="cache_hit"):
        run(work, [provider], memory_cache=memory, on_event=on_event)
    assert len(recorder.requests) == 1  # the first run's only


def test_the_outcome_is_in_unit_then_provider_order_whatever_the_completion_order():
    work = make_work(10)
    unit_ids = [unit.unit_id for unit, _ in work]
    odd = set(unit_ids[1::2])

    def jittery_failing(seed):
        respond = jittery(seed)

        def call(request):
            if request.unit_id in odd:
                time.sleep(random.Random(seed).uniform(0.0, 0.005))
                raise TransientProviderError(503, 0.0, "busy")
            return respond(request)

        return call

    for seed in range(5):
        threads_before = threading.active_count()
        providers = [
            CallableProvider("callable:a", jittery(seed)),
            CallableProvider("callable:b", jittery_failing(seed)),
            CallableProvider("callable:c", jittery(seed + 7)),
        ]
        outcome = run(work, providers, limits=RunLimits(max_concurrency=8, max_retries=0))
        expected = [
            (u, m)
            for u in unit_ids
            for m in ("callable:a", "callable:b", "callable:c")
            if not (m == "callable:b" and u in odd)
        ]
        assert list(outcome.responses) == expected
        assert [(failed.unit_id, failed.model_id) for failed in outcome.failed_units] == [
            (u, "callable:b") for u in unit_ids if u in odd
        ]
        assert threading.active_count() == threads_before


def test_one_call_overrunning_its_own_bound_ends_the_run_there(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.5)  # unit deadline 1.5 s: past the 1 s floor
    monkeypatch.setattr(runner_module, "RUN_GRACE_S", 0.0)
    monkeypatch.setattr(runner_module, "STOP_GRACE_S", 0.1)
    release = threading.Event()
    hung_calls, other_calls = [], []
    work = make_work(3)

    def provider(request):
        if request.unit_id == work[0][0].unit_id:
            hung_calls.append(request)
            release.wait(10)  # a call no deadline can cut
        else:
            other_calls.append(request)
        return answer(request)

    threads_before = threading.active_count()
    started = time.monotonic()
    try:
        with pytest.raises(RationalizationError, match="time bound") as caught:
            run(work, [CallableProvider("callable:hung", provider)], limits=RunLimits(timeout_s=1, max_retries=0))
        elapsed = time.monotonic() - started
    finally:
        release.set()
    assert len(hung_calls) == 1 and len(other_calls) == 2
    assert elapsed < 1.5 + 1.0  # its own bound, not the 3 x 1.5 s of three units, nor its 10 s
    for named in ("custom provider", "token source", "file system"):
        assert named in str(caught.value)
    deadline = time.monotonic() + 5
    while threading.active_count() > threads_before and time.monotonic() < deadline:
        time.sleep(0.01)
    assert threading.active_count() == threads_before  # the abandoned worker ends once its call returns


def test_a_long_run_of_requests_each_within_its_own_bound_never_trips_the_run_bound(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.5)  # unit deadline 1.5 s
    monkeypatch.setattr(runner_module, "RUN_GRACE_S", 0.0)

    def slow(request):
        time.sleep(0.6)  # longer than a poll, well within its own deadline
        return answer(request)

    outcome = run(
        make_work(4),
        [CallableProvider("callable:a", slow)],
        limits=RunLimits(timeout_s=1, max_retries=0, max_concurrency=1),
    )  # 2.4 s in all: past one unit deadline, within each request's own
    assert len(outcome.responses) == 4 and outcome.failed_units == ()


def test_latency_sums_every_attempt_and_unreported_tokens_count_zero():
    fake = FakeTime()
    calls = {"count": 0}

    def respond(request):
        fake.advance(0.25)
        calls["count"] += 1
        if calls["count"] == 1:
            raise TransientProviderError(503, 0.0, "busy")
        return ProviderResponse(content=None, raw_text="", finish="truncated", input_tokens=None, output_tokens=11)

    outcome = run(make_work(1), [CallableProvider("callable:a", respond)], clock=fake.clock, sleep=fake.sleep)
    usage = outcome.usage["callable:a"]
    assert (usage.requests, usage.retries, usage.input_tokens, usage.output_tokens) == (2, 1, 0, 11)
    assert usage.latency_ms == pytest.approx(500.0)


@pytest.mark.skipif(sys.platform == "win32", reason="symlinks")
def test_a_tampered_file_cache_fails_loud_before_any_request(tmp_path):
    work = make_work(1)
    recorder = Recorder()
    provider = CallableProvider("callable:a", recorder)
    key = expected_key(work[0][1], provider)
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / key[:2]).symlink_to(tmp_path / "elsewhere")
    with pytest.raises(RationalizationError, match="symlink"):
        run(work, [provider], file_cache=FileCache(tmp_path / "cache"))
    assert recorder.requests == []


def test_without_an_rng_the_jitter_comes_from_a_fresh_random_source():
    fake = FakeTime()
    run(
        make_work(1),
        [CallableProvider("callable:a", Recorder(failing(2)))],
        clock=fake.clock,
        sleep=fake.sleep,
    )
    first, second = fake.waits
    assert 0.0 <= first <= 1.5 and 0.0 <= second <= 3.0


# ---------------------------------------------------------------------------------------------------------------
# Tests added for mutants that survived
# ---------------------------------------------------------------------------------------------------------------


def test_a_wait_that_overshoots_still_never_sends_with_less_than_the_floor():
    fake = FakeTime()

    def overshooting(seconds):
        fake.sleep(seconds + 1.5)  # a loaded machine wakes late

    limits = RunLimits(timeout_s=10, max_retries=1)  # unit deadline 54 s
    recorder = Recorder(slow_then_busy(fake, [40.0], retry_after_s=12.0))  # 40 + 12 = 52 s leaves the floor...
    outcome = run(
        make_work(1), [CallableProvider("callable:a", recorder)], limits=limits, clock=fake.clock, sleep=overshooting
    )
    assert len(recorder.requests) == 1  # ...but the wake at 53.5 s does not
    assert "deadline passed after 1 attempts" in outcome.failed_units[0].error_message


def test_a_transform_that_returns_no_mapping_is_refused():
    provider = FakeProvider()
    provider.transform_schema = lambda schema: ["records"]
    with pytest.raises(RationalizationError, match="transform_schema must return a JSON object"):
        run(make_work(1), [provider])


@pytest.mark.parametrize("member", ["generate", "transform_schema"])
def test_a_method_that_stops_being_callable_after_check_provider_is_refused(member):
    reads = {"count": 0}
    real = getattr(FakeProvider, member)

    def changing(self):
        reads["count"] += 1
        return real.__get__(self) if reads["count"] == 1 else "not callable"

    provider = type("Shifty", (FakeProvider,), {member: property(changing)})()
    with pytest.raises(RationalizationError, match=f"{member} must be callable"):
        run(make_work(1), [provider])
    assert provider.requests == []


# ---------------------------------------------------------------------------------------------------------------
# Provider snapshots as their own step (snapshot_providers)
# ---------------------------------------------------------------------------------------------------------------


def test_snapshot_providers_reads_each_provider_into_a_snapshot():
    callable_provider = CallableProvider("callable:a", answer, family="meta")
    pooled = type("Pooled", (OpeningProvider,), {"max_connections": 3})("fake:b", family="google")
    first, second = snapshot_providers([callable_provider, pooled], SCHEMA)
    assert isinstance(first, ProviderSnapshot) and first.provider is callable_provider
    assert (first.model_id, first.provider_kind, first.rate_key, first.model_family) == (
        "callable:a",
        "callable",
        "callable:callable:a",
        "meta",
    )
    assert (dict(first.generation_params), first.max_connections, first.open) == ({}, None, None)
    assert first.provider_schema == SCHEMA.wrapper and first.schema_sha256 == SCHEMA.sha256
    assert (second.model_family, dict(second.generation_params), second.max_connections) == (
        "google",
        {"temperature": 0.0},
        3,
    )
    assert second.provider_schema == {"wrapped": SCHEMA.wrapper} and callable(second.open)
    assert pooled.opened == []  # snapshotting opens nothing


def test_each_provider_transforms_its_own_fresh_wrapper():
    seen = []

    class Mutating(FakeProvider):
        def transform_schema(self, schema):
            seen.append(json.dumps(schema, sort_keys=True))
            schema.clear()  # a careless transform
            return {"type": "object"}

    snapshot_providers([Mutating("fake:a"), Mutating("fake:b")], SCHEMA)
    assert seen == [json.dumps(SCHEMA.wrapper, sort_keys=True)] * 2


@pytest.mark.parametrize(
    ("providers", "schema", "error"),
    [
        ((), SCHEMA, ValueError),
        (iter([CallableProvider("callable:a", answer)]), SCHEMA, TypeError),
        ([CallableProvider("callable:a", answer)], SCHEMA.wrapper, TypeError),
    ],
)
def test_snapshot_providers_refuses_a_bad_argument(providers, schema, error):
    with pytest.raises(error):
        snapshot_providers(providers, schema)


def test_the_outcome_names_each_models_family_and_kind_in_provider_order():
    providers = [CallableProvider("callable:a", answer, family="meta"), FakeProvider("fake:b", family="google")]
    outcome = run(make_work(1), providers)
    assert outcome.models == (("callable:a", "meta", "callable"), ("fake:b", "google", "fake"))


def test_a_snapshot_made_for_another_schema_is_refused():
    class Other(BaseModel):
        code: Annotated[str, Key]
        quantity: int

    recorder = Recorder()
    snapshots = snapshot_providers([CallableProvider("callable:a", recorder)], prepare(Other))
    with pytest.raises(ValueError, match="another schema"):
        run_requests(make_work(1), snapshots, SCHEMA, schema_version="1.0", memory_cache=MemoryCache())
    assert recorder.requests == []


def snapshot_fields(**changes):
    provider = CallableProvider("callable:a", answer)
    fields = {
        "provider": provider,
        "model_id": "callable:a",
        "provider_kind": "callable",
        "rate_key": "callable:callable:a",
        "model_family": "unknown",
        "generation_params_json": "{}",
        "provider_schema_json": SCHEMA.wrapper_json,
        "max_connections": None,
        "schema_sha256": SCHEMA.sha256,
        "generate": provider.generate,
        "open": None,
    }
    return {**fields, **changes}


def test_a_snapshot_keeps_its_json_as_text_and_hands_out_fresh_copies():
    snapshot = ProviderSnapshot(**snapshot_fields(generation_params_json='{"seed":1}'))
    assert snapshot.generation_params_json == '{"seed":1}' and snapshot.provider_schema_json == SCHEMA.wrapper_json
    params, schema = snapshot.generation_params, snapshot.provider_schema
    params["seed"] = 2
    schema["required"] = []
    schema["properties"]["records"]["items"]["properties"].clear()  # a nested change, too
    assert snapshot.generation_params == {"seed": 1}
    assert snapshot.provider_schema == SCHEMA.wrapper
    assert snapshot.generation_params is not snapshot.generation_params


def test_snapshots_compare_by_identity():
    fields = snapshot_fields()
    first, second = ProviderSnapshot(**fields), ProviderSnapshot(**fields)  # equal field by field
    assert first == first and first != second
    assert len({first, second}) == 2  # hashable, by identity


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"model_id": "two\nlines"}, ValueError),
        ({"provider_kind": 3}, TypeError),
        ({"rate_key": ""}, ValueError),
        ({"model_family": "Not A Family"}, ValueError),
        ({"generation_params_json": '{"max_output_tokens":5}'}, ValueError),
        ({"generation_params_json": '{"seed": 1}'}, ValueError),  # not canonical
        ({"generation_params_json": "[1]"}, ValueError),
        ({"generation_params_json": "{"}, ValueError),
        ({"generation_params_json": {"seed": 1}}, TypeError),
        ({"provider_schema_json": '{"b":1,"a":2}'}, ValueError),
        ({"provider_schema_json": "null"}, ValueError),
        ({"max_connections": 0}, ValueError),
        ({"max_connections": True}, TypeError),
        ({"schema_sha256": "abc"}, ValueError),
        ({"generate": "generate"}, TypeError),
        ({"open": "open"}, TypeError),
    ],
)
def test_a_snapshot_refuses_a_bad_field(changes, error):
    with pytest.raises(error):
        ProviderSnapshot(**snapshot_fields(**changes))


def test_a_snapshot_is_frozen():
    snapshot = ProviderSnapshot(**snapshot_fields())
    with pytest.raises(AttributeError):
        snapshot.model_id = "callable:b"  # type: ignore[misc]


def test_an_envelope_carrying_another_schema_is_refused():
    class Other(BaseModel):
        code: Annotated[str, Key]
        quantity: int

    document = make_document(("Part PN-000",))
    (unit,) = build_units(document, max_unit_chars=10_000, render=render_element).units
    (envelope,) = build_envelopes(
        [unit], document, prompt=PROMPT, context=None, schema_json=prepare(Other).wrapper_json
    )
    recorder = Recorder()
    with pytest.raises(ValueError, match="schema"):
        run([(unit, envelope)], [CallableProvider("callable:a", recorder)])
    assert recorder.requests == []


def test_open_is_called_only_for_providers_with_a_request_to_send():
    work = make_work(2)
    cached, fresh = OpeningProvider("fake:a"), OpeningProvider("fake:b")
    memory = MemoryCache()
    run(work, [cached], memory_cache=memory)
    assert len(cached.opened) == 1
    run(work, [cached, fresh], memory_cache=memory)
    assert len(cached.opened) == 1  # every answer it owes is cached: nothing to open
    assert len(fresh.opened) == 1 and len(fresh.requests) == 2


# ---------------------------------------------------------------------------------------------------------------
# Stopping never waits on a hung call past its bound
# ---------------------------------------------------------------------------------------------------------------


class Interrupt(KeyboardInterrupt):
    pass


@pytest.mark.parametrize("mode", ["permanent", "strict", "unknown", "on_event", "interrupt"])
def test_a_stopping_run_never_waits_on_a_hung_call_past_its_deadline(monkeypatch, mode):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.1)  # unit deadline 1.1 s: past the 1 s floor
    monkeypatch.setattr(runner_module, "STOP_GRACE_S", 0.1)
    release, hung_started = threading.Event(), threading.Event()
    failing_calls = []

    def hung(request):
        hung_started.set()
        release.wait(10)  # a call no deadline can cut
        return answer(request)

    def failing(request):
        failing_calls.append(request)
        hung_started.wait(5)
        if mode == "permanent":
            raise PermanentProviderError(400, "bad request")
        if mode == "strict":
            raise TransientProviderError(503, 0.0, "busy")
        if mode == "unknown":
            raise KeyError("bug")
        return answer(request)

    def on_event(event):
        if event.model_id == "callable:fail" and mode in ("on_event", "interrupt"):
            raise Interrupt() if mode == "interrupt" else Stop("from on_event")

    expected = {"permanent": RationalizationError, "strict": RationalizationError, "unknown": KeyError}
    threads_before = threading.active_count()
    started = time.monotonic()
    try:
        with pytest.raises(expected.get(mode, Interrupt if mode == "interrupt" else Stop)):
            run(
                make_work(1),
                # the failing request comes first: events reach on_event in request order (module notes, 7)
                [CallableProvider("callable:fail", failing), CallableProvider("callable:hung", hung)],
                limits=RunLimits(timeout_s=1, max_retries=0, strict=mode == "strict"),
                on_event=on_event,
            )
        elapsed = time.monotonic() - started
    finally:
        release.set()
    assert failing_calls and hung_started.is_set()
    assert elapsed < 1.1 + 0.1 + 1.0  # the hung call's deadline and grace, not its 10 s
    deadline = time.monotonic() + 5
    while threading.active_count() > threads_before and time.monotonic() < deadline:
        time.sleep(0.01)
    assert threading.active_count() == threads_before


def test_racing_permanent_failures_raise_the_earliest_requests():
    def late_refusal(request):
        time.sleep(0.2)  # fails after the other one has stopped the run
        raise PermanentProviderError(400, "first in provider order")

    def early_refusal(request):
        raise PermanentProviderError(403, "second in provider order")

    with pytest.raises(RationalizationError, match="callable:a.*first in provider order"):
        run(make_work(1), [CallableProvider("callable:a", late_refusal), CallableProvider("callable:b", early_refusal)])


# ---------------------------------------------------------------------------------------------------------------
# Requirements the review found unpinned
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("finish", ["truncated", "refused"])
def test_a_truncated_or_refused_response_is_cached_and_replayed(finish):
    work = make_work(1)
    calls = []

    def respond(request):
        calls.append(request)
        return ProviderResponse(content=None, raw_text="cut", finish=finish, input_tokens=1, output_tokens=2)

    provider = CallableProvider("callable:a", respond)
    memory = MemoryCache()
    run(work, [provider], memory_cache=memory)
    outcome = run(work, [provider], memory_cache=memory)
    assert len(calls) == 1 and outcome.cache_hits == 1
    assert [response.finish for response in outcome.responses.values()] == [finish]


def test_providers_sharing_a_rate_key_share_its_token_bucket():
    fake = FakeTime()
    times = []

    def stamp(request):
        times.append(fake.clock())
        return answer(request)

    providers = [
        FakeProvider("fake:a", rate_key="endpoint", respond=stamp),
        FakeProvider("fake:b", rate_key="endpoint", respond=stamp),
    ]
    run(make_work(2), providers, limits=RunLimits(rpm_limit=60, max_concurrency=1), clock=fake.clock, sleep=fake.sleep)
    assert times == [0.0, 1.0, 2.0, 3.0]  # one token a second for the endpoint, whichever model asks


@pytest.mark.parametrize("connections", [(1, 3), (3, 1)])
def test_a_shared_rate_key_gets_the_smallest_max_connections(connections):
    flight = InFlight()
    providers = [
        type("Pooled", (FakeProvider,), {"max_connections": count})(
            f"fake:{index}", rate_key="endpoint", respond=flight.respond("endpoint")
        )
        for index, count in enumerate(connections)
    ]
    run(make_work(6), providers, limits=RunLimits(max_concurrency=8, max_concurrency_per_provider=4))
    assert flight.peak["endpoint"] == 1


def test_each_request_has_its_own_deadline_not_one_for_the_run():
    fake = FakeTime()

    def slow(request):
        fake.advance(10.0)
        return answer(request)

    outcome = run(
        make_work(10),
        [CallableProvider("callable:a", slow)],
        limits=RunLimits(timeout_s=60, max_retries=0, max_concurrency=1),  # each 62 s; 100 s in all
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert outcome.failed_units == () and len(outcome.responses) == 10


def test_the_memory_cache_wins_when_both_caches_hold_an_entry(tmp_path):
    work = make_work(1)
    file_provider = CallableProvider("callable:a", lambda request: answer(request))
    run(work, [file_provider], file_cache=FileCache(tmp_path))
    key = expected_key(work[0][1], file_provider)
    held = ProviderResponse(
        content={"records": []}, raw_text="memory", finish="complete", input_tokens=None, output_tokens=None
    )
    memory = MemoryCache()
    meta = runner_module.CacheMeta(
        model_id="callable:a",
        unit_id=work[0][0].unit_id,
        doc_id=work[0][0].doc_id,
        schema_name="Part",
        schema_version="1.0",
        schema_sha256=SCHEMA.sha256,
        prompt_sha256=SCHEMA.sha256,
        latency_ms=0.0,
        created_at="2026-11-05T10:00:00+00:00",
    )
    assert memory.put(key, held, meta)
    sent = Events()
    outcome = run(work, [file_provider], memory_cache=memory, file_cache=FileCache(tmp_path), on_event=sent)
    assert list(outcome.responses.values()) == [held]
    assert [event.detail for event in sent.events] == ["memory"]


def test_a_fresh_response_goes_into_the_memory_cache_too_when_a_file_cache_is_set(tmp_path):
    work = make_work(1)
    provider = CallableProvider("callable:a", answer)
    memory = MemoryCache()
    run(work, [provider], memory_cache=memory, file_cache=FileCache(tmp_path))
    assert memory.get(expected_key(work[0][1], provider)).hit


class Hexed(BaseModel):
    code: Annotated[str, Key] = Field(description=f"A code such as {IN_TEXT}.")


@pytest.mark.parametrize("component", ["prompt", "context", "schema", "payload"])
def test_a_nonce_in_any_component_is_redrawn(component):
    options = {
        "prompt": {"prompt": f"Extract parts like {IN_TEXT}."},
        "context": {"context": {"example": IN_TEXT}},
        "schema": {"schema": prepare(Hexed)},
        "payload": {"texts": (f"Code {IN_TEXT}",)},
    }[component]
    work = make_work(1, **options)
    draw, drawn = nonces(IN_TEXT, FRESH)
    recorder = Recorder()
    run(work, [CallableProvider("callable:a", recorder)], schema=options.get("schema", SCHEMA), nonce=draw)
    assert len(drawn) == 2 and nonce_of(recorder.requests[0].user_envelope) == FRESH


def test_a_retry_wait_leaving_less_than_the_floor_is_not_waited():
    fake = FakeTime()
    limits = RunLimits(timeout_s=10, max_retries=1)  # unit deadline 54 s
    recorder = Recorder(slow_then_busy(fake, [40.0], retry_after_s=13.5))  # 53.5 s: past 54 - 1, short of 54
    outcome = run(
        make_work(1), [CallableProvider("callable:a", recorder)], limits=limits, clock=fake.clock, sleep=fake.sleep
    )
    assert fake.waits == [] and len(recorder.requests) == 1 and outcome.usage["callable:a"].retries == 0
    assert "deadline passed" in outcome.failed_units[0].error_message


def test_a_retry_token_leaving_less_than_the_floor_is_not_taken():
    fake = FakeTime()
    work = make_work(2)
    times = []

    def overshooting(seconds):
        fake.sleep(seconds + 1.5)  # a loaded machine wakes late

    def respond(request):
        times.append(fake.clock())
        if request.unit_id == work[0][0].unit_id:
            fake.advance(40.0)
            raise TransientProviderError(503, 13.0, "busy")
        return answer(request)

    outcome = run(
        work,
        [CallableProvider("callable:a", respond)],
        # unit deadline 2 * 12 + 30 + 1 * 1 * 1 = 55 s; a token a second
        limits=RunLimits(timeout_s=10, max_retries=1, rpm_limit=60, max_concurrency=1, max_concurrency_per_provider=1),
        clock=fake.clock,
        sleep=overshooting,
    )
    # the retry wakes at 54.5 s, past 55 - 1: no token is taken for it, so the next unit's comes due at once
    assert "deadline passed" in outcome.failed_units[0].error_message
    assert times == [0.0, 54.5]


def test_run_requests_refuses_two_snapshots_with_one_model_id():
    recorder = Recorder()
    first = snapshot_providers([CallableProvider("callable:a", recorder)], SCHEMA)
    second = snapshot_providers([CallableProvider("callable:a", recorder)], SCHEMA)
    with pytest.raises(RationalizationError, match="callable:a"):
        run_requests(make_work(1), first + second, SCHEMA, schema_version="1.0", memory_cache=MemoryCache())
    assert recorder.requests == []


def test_a_clock_that_never_moves_cannot_keep_the_run_waiting(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.5)  # unit deadline 1.5 s
    monkeypatch.setattr(runner_module, "RUN_GRACE_S", 0.0)
    monkeypatch.setattr(runner_module, "MIN_WAIT_S", runner_module.POLL_S)  # a wait counts as long as it takes
    monkeypatch.setattr(runner_module, "STOP_GRACE_S", 0.1)
    release = threading.Event()
    calls = []

    def hung(request):
        calls.append(request)
        release.wait(10)
        return answer(request)

    started = time.monotonic()
    try:
        with pytest.raises(RationalizationError, match="time bound"):
            run(
                make_work(1),
                [CallableProvider("callable:hung", hung)],
                limits=RunLimits(timeout_s=1, max_retries=0),
                clock=lambda: 0.0,  # no deadline ever passes on it: the count of waits still ends the loop
            )
        elapsed = time.monotonic() - started
    finally:
        release.set()
    # 18 waits of 0.1 s, then the stop's two passes of 1.6 s each (the clock never says a bound passed): not 10 s
    assert len(calls) == 1 and elapsed < 1.8 + 2 * 1.6 + 1.5


def test_the_count_of_waits_allows_every_request_its_own_deadline(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.5)  # unit deadline 1.5 s
    monkeypatch.setattr(runner_module, "RUN_GRACE_S", 0.0)
    monkeypatch.setattr(runner_module, "MIN_WAIT_S", runner_module.POLL_S)

    def slow(request):
        time.sleep(0.6)  # six polls each, within its own deadline
        return answer(request)

    outcome = run(
        make_work(6),
        [CallableProvider("callable:a", slow)],
        limits=RunLimits(timeout_s=1, max_retries=0, max_concurrency=1),
    )  # 3.6 s of waits: past one unit deadline's worth of them, within six
    assert len(outcome.responses) == 6 and outcome.failed_units == ()


def test_a_token_is_used_as_soon_as_it_is_due_while_requests_are_in_flight():
    sent = []

    def held(request):
        sent.append(time.monotonic())
        time.sleep(0.5)  # every earlier request is still in flight when the next token comes due
        return answer(request)

    run(
        make_work(10),
        [CallableProvider("callable:a", held)],
        limits=RunLimits(rpm_limit=1_200, max_concurrency=32, max_concurrency_per_provider=32),  # one per 0.05 s
    )
    assert len(sent) == 10
    assert sent[-1] - sent[0] < 0.45 + 0.25  # nine tokens 0.05 s apart, not one per 0.1 s poll


# ---------------------------------------------------------------------------------------------------------------
# Third review
# ---------------------------------------------------------------------------------------------------------------


def test_a_request_that_finished_while_on_event_blocked_is_no_overrun(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.1)  # unit deadline 1.1 s
    monkeypatch.setattr(runner_module, "RUN_GRACE_S", 0.0)
    calls = []

    def quick(request):
        calls.append("a")
        return answer(request)

    def healthy(request):
        time.sleep(0.3)  # done 0.3 s after it starts, well within its bound
        calls.append("b")
        return answer(request)

    blocked = []

    def on_event(event):
        if not blocked:
            blocked.append(event)
            time.sleep(1.5)  # a slow log sink: the caller's thread is held past b's bound while b finishes

    outcome = run(
        make_work(1),
        [CallableProvider("callable:a", quick), CallableProvider("callable:b", healthy)],
        limits=RunLimits(timeout_s=1, max_retries=0),
        on_event=on_event,
    )
    assert sorted(calls) == ["a", "b"] and len(outcome.responses) == 2 and outcome.failed_units == ()


def test_a_stop_waits_for_a_hung_neighbour_only_until_its_current_attempt_ends(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.1)  # an attempt's bound: 1 + 0.1 s
    monkeypatch.setattr(runner_module, "STOP_GRACE_S", 0.1)
    release, hung_started = threading.Event(), threading.Event()
    hung_calls = []

    def hung(request):
        hung_calls.append(request)
        hung_started.set()
        release.wait(10)
        return answer(request)

    def refusing_once_hung(request):
        hung_started.wait(5)
        refusing(request)

    limits = RunLimits(timeout_s=1, max_retries=2)  # unit deadline 3 * 1.1 + 2 * 30 = 63.3 s: no attempt follows a stop
    started = time.monotonic()
    try:
        with pytest.raises(RationalizationError, match="callable:refuse"):
            run(
                make_work(1),
                [CallableProvider("callable:refuse", refusing_once_hung), CallableProvider("callable:hung", hung)],
                limits=limits,
            )
        elapsed = time.monotonic() - started
    finally:
        release.set()
    assert len(hung_calls) == 1
    assert elapsed < 1.1 + 0.1 + 1.0  # its first attempt's bound, not its 63.3 s unit deadline (nor its 10 s)


def test_a_call_slightly_past_its_deadline_is_within_the_run_grace(monkeypatch):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", 0.1)  # unit deadline 1.1 s
    monkeypatch.setattr(runner_module, "RUN_GRACE_S", 0.6)

    def late(request):
        time.sleep(1.3)  # past its deadline, within the grace: slow, not hung
        return answer(request)

    outcome = run(make_work(1), [CallableProvider("callable:a", late)], limits=RunLimits(timeout_s=1, max_retries=0))
    assert len(outcome.responses) == 1


@pytest.mark.parametrize(("slack", "grace"), [(0.1, 0.6), (0.6, 0.1)])
def test_a_stop_waits_out_the_slack_and_grace_for_a_request_finishing_just_past_its_timeout(monkeypatch, slack, grace):
    monkeypatch.setattr(runner_module, "ATTEMPT_SLACK_S", slack)  # its attempt ends by 1 + slack s
    monkeypatch.setattr(runner_module, "STOP_GRACE_S", grace)  # the stop waits for it until 1.7 s

    def late_refusal(request):
        time.sleep(1.3)  # past its timeout, within the slack and the grace
        raise PermanentProviderError(400, "first in provider order")

    def early_refusal(request):
        raise PermanentProviderError(403, "second in provider order")

    with pytest.raises(RationalizationError, match="callable:a.*first in provider order"):
        run(
            make_work(1),
            [CallableProvider("callable:a", late_refusal), CallableProvider("callable:b", early_refusal)],
            limits=RunLimits(timeout_s=1, max_retries=0),
        )


def test_a_stop_waits_for_every_request_in_flight_not_just_the_first_to_finish():
    def refusal(name, delay):
        def call(request):
            time.sleep(delay)
            raise PermanentProviderError(400, f"from {name}")

        return call

    providers = [
        CallableProvider("callable:a", refusal("a", 0.6)),  # the earliest request finishes last
        CallableProvider("callable:b", refusal("b", 0.3)),
        CallableProvider("callable:c", refusal("c", 0.0)),  # stops the run
    ]
    threads_before = threading.active_count()
    with pytest.raises(RationalizationError, match="callable:a.*from a"):
        run(make_work(1), providers)
    assert threading.active_count() == threads_before


def test_with_nothing_in_flight_the_caller_waits_for_the_earliest_keys_token():
    fake = FakeTime()
    work = make_work(2)
    stamps = []

    def stamped(name, spends):
        def call(request):
            stamps.append((name, request.unit_id, fake.clock()))
            fake.advance(spends)
            return answer(request)

        return call

    providers = [
        FakeProvider("fake:a", rate_key="key-a", respond=stamped("a", 0.5)),  # key b's first token comes 0.5 s later
        FakeProvider("fake:b", rate_key="key-b", respond=stamped("b", 0.0)),
    ]
    run(work, providers, limits=RunLimits(rpm_limit=60, max_concurrency=1), clock=fake.clock, sleep=fake.sleep)
    second = work[1][0].unit_id
    # key a's next token is due at 1.0 s and key b's at 1.5 s: the caller wakes for the earlier one
    assert [(name, at) for name, unit_id, at in stamps if unit_id == second] == [("a", 1.0), ("b", 1.5)]


def test_a_keys_unit_deadline_counts_only_its_own_slots():
    fake = FakeTime()
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            fake.advance(110.0)
            raise TransientProviderError(503, 0.0, "busy")
        return answer(request)

    pooled = type("Pooled", (FakeProvider,), {"max_connections": 1})("fake:a", respond=respond)
    run(
        make_work(1),
        [pooled],
        # one slot (max_connections): deadline 2 * 12 + 30 + 1 * 1 * 60 = 114 s, not the 4-slot 294 s
        limits=RunLimits(rpm_limit=1, timeout_s=10, max_retries=1, max_concurrency_per_provider=4),
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert [request.timeout_s for request in calls] == [10.0, 4.0]


# ---------------------------------------------------------------------------------------------------------------
# Tier 1 — Unit 1D (M3, N2, M6)
# ---------------------------------------------------------------------------------------------------------------


def test_stop_drains_buffered_events_in_request_order_on_worker_failure():
    work = make_work(2)
    unit_0, unit_1 = work[0][0].unit_id, work[1][0].unit_id

    # 1. Strict failure on unit 0 after unit 1 already finished and buffered its event in _EventOrder.
    unit_1_done = threading.Event()
    strict_events = Events()

    def strict_provider(request):
        if request.unit_id == unit_1:
            unit_1_done.set()
            return answer(request)
        unit_1_done.wait(5)
        time.sleep(0.05)
        raise TransientProviderError(503, 0.0, "busy")

    with pytest.raises(RationalizationError, match="strict"):
        run(
            work,
            [CallableProvider("callable:a", strict_provider)],
            limits=RunLimits(max_concurrency=2, max_concurrency_per_provider=2, max_retries=0, strict=True),
            on_event=strict_events,
        )
    assert [(e.kind, e.unit_id, e.attempt, e.status_code, e.detail) for e in strict_events.events] == [
        ("request", unit_0, 1, 503, "TransientProviderError"),
        ("request", unit_1, 1, None, "complete"),
    ]

    # 2. Unhandled exception on unit 0 (after a retry and on first attempt) drains unit 0 and unit 1 events in order.
    retry_unit_1_done = threading.Event()
    retry_events = Events()
    attempts = {"unit_0": 0}

    def unhandled_after_retry(request):
        if request.unit_id == unit_1:
            retry_unit_1_done.set()
            return answer(request)
        retry_unit_1_done.wait(5)
        time.sleep(0.05)
        attempts["unit_0"] += 1
        if attempts["unit_0"] == 1:
            raise TransientProviderError(503, 0.0, "busy")
        raise RuntimeError("boom after retry")

    with pytest.raises(RuntimeError, match="boom after retry"):
        run(
            work,
            [CallableProvider("callable:a", unhandled_after_retry)],
            limits=RunLimits(max_concurrency=2, max_concurrency_per_provider=2, max_retries=1),
            on_event=retry_events,
        )
    assert [(e.kind, e.unit_id, e.attempt) for e in retry_events.events] == [
        ("request", unit_0, 1),
        ("retry", unit_0, 1),
        ("request", unit_1, 1),
    ]

    direct_unit_1_done = threading.Event()
    direct_events = Events()

    def unhandled_direct(request):
        if request.unit_id == unit_1:
            direct_unit_1_done.set()
            return answer(request)
        direct_unit_1_done.wait(5)
        time.sleep(0.05)
        raise RuntimeError("boom direct")

    with pytest.raises(RuntimeError, match="boom direct"):
        run(
            work,
            [CallableProvider("callable:a", unhandled_direct)],
            limits=RunLimits(max_concurrency=2, max_concurrency_per_provider=2, max_retries=0),
            on_event=direct_events,
        )
    assert [(e.kind, e.unit_id, e.attempt, e.detail) for e in direct_events.events] == [
        ("request", unit_1, 1, "complete"),
    ]

    # 3. If on_event itself raises on unit 0's event while unit 1's event is buffered, _stop does not call on_event.
    hook_unit_1_done = threading.Event()
    seen = []

    def both_succeed(request):
        if request.unit_id == unit_1:
            hook_unit_1_done.set()
            return answer(request)
        hook_unit_1_done.wait(5)
        time.sleep(0.05)
        return answer(request)

    def raising_hook(event):
        seen.append(event)
        raise Stop("from on_event")

    with pytest.raises(Stop, match="from on_event"):
        run(
            work,
            [CallableProvider("callable:a", both_succeed)],
            limits=RunLimits(max_concurrency=2, max_concurrency_per_provider=2, max_retries=0),
            on_event=raising_hook,
        )
    assert [e.unit_id for e in seen] == [unit_0]


def test_snapshot_property_reads_do_not_call_canonical_json(monkeypatch):
    snapshot = ProviderSnapshot(**snapshot_fields(generation_params_json='{"seed":1}'))
    monkeypatch.setattr(
        runner_module,
        "canonical_json",
        lambda *args, **kwargs: pytest.fail("canonical_json must not be called when reading snapshot properties"),
    )
    params = snapshot.generation_params
    schema = snapshot.provider_schema
    assert params == {"seed": 1}
    assert schema == SCHEMA.wrapper
    params["seed"] = 99
    schema.clear()
    assert snapshot.generation_params == {"seed": 1}
    assert snapshot.provider_schema == SCHEMA.wrapper


def test_model_request_docstring_describes_provider_transformed_wire_schema():
    doc = ModelRequest.__doc__
    assert doc is not None
    assert "The neutral response schema, which each adapter transforms" not in doc
    schema_lines = [line.strip() for line in doc.splitlines() if "json_schema:" in line]
    assert len(schema_lines) == 1
    assert "transform" in doc.lower() and "provider" in doc.lower()
