"""Tests for ``datagrunt.rationalization.providers.anthropic``: the Anthropic Messages API adapter (plan §4.5, R3).

The wire format is pinned from Anthropic's official documentation, read on 2026-11-04 (docs.claude.com redirects to
platform.claude.com, Anthropic's documentation host):

- https://platform.claude.com/docs/en/build-with-claude/structured-outputs (status "stable"; ``output_config.format``
  with ``type: "json_schema"``; no beta header; the JSON Schema subset; the JSON in the response's ``text`` block)
- https://platform.claude.com/docs/en/agents-and-tools/tool-use/strict-tool-use (status "stable")
- https://platform.claude.com/docs/en/api/errors ("Forced tool use not supported": ``tool_choice`` ``any`` / ``tool``
  is a 400 on Claude Opus 5.5, Sonnet 5.5, Fable 5.1 and Mythos 5.1; the error body shape; 529 ``overloaded_error``)
- https://platform.claude.com/docs/en/api/messages/create (the ``stop_reason`` values, ``usage``, content blocks)
- https://platform.claude.com/docs/en/api/versioning and .../api/migration-2026-10-21 (``anthropic-version``: the
  documented versions are 2026-10-21 and 2023-06-01, "Anthropic recommends using the latest API version whenever
  possible"; ``temperature`` / ``top_p`` / ``top_k`` removed in 2026-10-21, a 400 on every model)
- https://platform.claude.com/docs/en/models/opus-5-5/migration-guide (sampling parameters and forced tool use
  rejected on current models)

Re-checked on 2026-11-05 by the T7 review (the wire shape matches the current docs). Every exchange runs over
``httpx.MockTransport``; nothing reaches a network.
"""

import ast
import copy
import json
import math
import pickle
import re
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType

import httpx
import pytest
from pydantic import AwareDatetime, BaseModel, Field, NaiveDatetime

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization._canonical import canonical_copy, canonical_json
from datagrunt.rationalization.providers import anthropic as anthropic_module
from datagrunt.rationalization.providers.anthropic import (
    API_KEY_VARIABLE,
    API_VERSIONS,
    DEFAULT_API_VERSION,
    DEFAULT_BASE_URL,
    ENFORCED_FORMATS,
    MAX_OPTIONAL_PARAMETERS,
    MAX_UNION_PARAMETERS,
    MECHANISM,
    SCHEMA_MAX_DEPTH,
    SCHEMA_MAX_NODES,
    SUPPORTED_FORMATS,
    WIRE,
    AnthropicProvider,
    parameter_counts,
)
from datagrunt.rationalization.providers.base import (
    PermanentProviderError,
    TransientProviderError,
    _Secret,
    check_endpoint,
    check_provider,
    parse_model_string,
)
from datagrunt.rationalization.providers.google_cloud import (
    ANTHROPIC_VERSION,
    GoogleCloudAuth,
    GoogleCloudTarget,
    family_for_publisher,
    model_id_for,
    rate_key_for,
)
from datagrunt.rationalization.providers.http import HttpClient
from datagrunt.rationalization.schema import response_schema
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse
from rationalization_tests.test_schema import REALISTIC

KEY = "sk-ant-api03-hunter2-KEY"
MODEL = "claude-opus-5-5"
URL = "https://api.anthropic.com/v1/messages"

# A transformed wrapper, as the runner puts it in ModelRequest.json_schema.
SENT_SCHEMA = {
    "type": "object",
    "properties": {
        "records": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"part": {"type": "string"}},
                "required": ["part"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["records"],
    "additionalProperties": False,
}
REQUEST = ModelRequest(
    unit_id="doc:p0001_e000-p0001_e002",
    model_id=f"anthropic:{MODEL}",
    system_prompt="SYSTEM RULES",
    user_envelope="<document_payload_ab>PN-1</document_payload_ab>",
    json_schema=SENT_SCHEMA,
    max_output_tokens=1024,
    timeout_s=30.0,
)
ANSWER = {"records": [{"part": "PN-1"}]}

# Wire shapes (Messages API, 2026-11-04): a reply with an adaptive-thinking block before the JSON text block.
MESSAGE = {
    "id": "msg_013Zva2CMHLNnXjNJJKqJ2EF",
    "type": "message",
    "role": "assistant",
    "model": MODEL,
    "content": [
        {"type": "thinking", "thinking": "", "signature": "EqQBCkYIBhgC"},
        {"type": "text", "text": json.dumps(ANSWER)},
    ],
    "stop_reason": "end_turn",
    "stop_sequence": None,
    "stop_details": None,
    "usage": {
        "input_tokens": 2095,
        "output_tokens": 503,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "output_tokens_details": {"thinking_tokens": 120},
        "service_tier": "standard",
    },
}


def error_body(kind, message):
    """An error reply's body, as the Claude API documents it."""
    return {"type": "error", "error": {"type": kind, "message": message}, "request_id": "req_011CSHoEeqs5C35K2UUqR7Fy"}


def message_with(**fields):
    reply = copy.deepcopy(MESSAGE)
    reply.update(fields)
    return reply


def streamed(status, *, json_body=None, content=b"", headers=None):
    """A streaming ``httpx.Response`` (a ``content=`` bytes response is pre-read, which http.py refuses)."""
    headers = dict(headers or {})
    if json_body is not None:
        content = json.dumps(json_body).encode("utf-8")
        headers.setdefault("content-type", "application/json")
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(content))


def reply_with(status=200, **kwargs):
    """A handler answering ``streamed(status, **kwargs)``, recording each request it gets."""
    seen = []

    def handler(request):
        seen.append(request)
        return streamed(status, **kwargs)

    handler.seen = seen
    return handler


def make_provider(handler=None, **kwargs):
    kwargs.setdefault("api_key", KEY)
    transport = httpx.MockTransport(handler or reply_with(json_body=MESSAGE))
    return AnthropicProvider(kwargs.pop("model", MODEL), transport=transport, **kwargs)


def answer(json_body=None, *, content=None, status=200, headers=None):
    """The provider's response to REQUEST when the server sends this reply."""
    if content is not None:
        handler = reply_with(status, content=content, headers=headers)
    else:
        handler = reply_with(status, json_body=json_body, headers=headers)
    return make_provider(handler).generate(REQUEST)


def traceback_text(error):
    """The traceback with the locals of datagrunt's frames, as a log with ``capture_locals`` prints them."""
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def assert_secret_free(error):
    """No key text in the error's traceback locals, text or repr, and no chained exception that could hold it."""
    for text in (traceback_text(error), str(error), repr(error)):
        assert "hunter2" not in text
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.fixture(autouse=True)
def no_ambient_key(monkeypatch):
    """No key from the developer's shell reaches a test."""
    monkeypatch.delenv(API_KEY_VARIABLE, raising=False)


# ---------------------------------------------------------------------------------------------------------------
# The request: body, URL, headers, bounds
# ---------------------------------------------------------------------------------------------------------------


def test_the_request_body_is_the_documented_structured_outputs_request():
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler).generate(REQUEST)

    (sent,) = handler.seen
    assert sent.method == "POST"
    assert str(sent.url) == URL
    assert json.loads(sent.content) == {
        "model": MODEL,
        "max_tokens": 1024,
        "system": "SYSTEM RULES",
        "messages": [{"role": "user", "content": "<document_payload_ab>PN-1</document_payload_ab>"}],
        "output_config": {"format": {"type": "json_schema", "schema": SENT_SCHEMA}},
    }
    assert sent.headers["anthropic-version"] == "2026-10-21"


def test_the_defaults_are_the_latest_api_version_and_no_temperature():
    assert DEFAULT_API_VERSION == "2026-10-21"
    assert API_VERSIONS == ("2026-10-21", "2023-06-01")
    assert canonical_json(make_provider().generation_params) == (
        '{"api_version":"2026-10-21","mechanism":"json_schema","wire":"messages/1"}'
    )


def test_api_version_2023_06_01_can_send_a_temperature_for_older_models():
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler, api_version="2023-06-01", temperature=0.0).generate(REQUEST)

    body = json.loads(handler.seen[0].content)
    assert body["temperature"] == 0.0
    assert sorted(body) == ["max_tokens", "messages", "model", "output_config", "system", "temperature"]
    assert handler.seen[0].headers["anthropic-version"] == "2023-06-01"


def test_the_body_never_forces_a_tool_or_sets_thinking_effort_or_sampling():
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler).generate(REQUEST)

    body = json.loads(handler.seen[0].content)
    for absent in ("tools", "tool_choice", "thinking", "top_p", "top_k", "stop_sequences", "stream", "metadata"):
        assert absent not in body
    assert body["output_config"] == {"format": {"type": "json_schema", "schema": SENT_SCHEMA}}


def test_the_headers_carry_the_key_and_the_version_and_no_beta_flag():
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler).generate(REQUEST)

    headers = handler.seen[0].headers
    assert headers["x-api-key"] == KEY
    assert headers["anthropic-version"] == "2026-10-21"
    assert headers["content-type"] == "application/json"
    assert "anthropic-beta" not in headers
    assert "authorization" not in headers
    assert KEY not in str(handler.seen[0].url)
    assert KEY.encode() not in handler.seen[0].content


def test_a_gateway_base_url_gets_the_messages_path():
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler, base_url="https://GW.example/anthropic/").generate(REQUEST)

    assert str(handler.seen[0].url) == "https://gw.example/anthropic/v1/messages"


def test_generate_passes_the_key_as_a_secret_and_bounds_the_request_by_its_timeout(monkeypatch):
    calls = []
    real = HttpClient.post_json

    def spy(self, url, **kwargs):
        calls.append((url, kwargs))
        return real(self, url, **kwargs)

    monkeypatch.setattr(HttpClient, "post_json", spy)
    provider = make_provider()

    before = time.monotonic()
    provider.generate(REQUEST)
    after = time.monotonic()

    ((url, kwargs),) = calls
    assert url == URL
    assert isinstance(kwargs["headers"]["x-api-key"], _Secret)
    assert kwargs["headers"]["anthropic-version"] == DEFAULT_API_VERSION
    assert kwargs["timeout_s"] == REQUEST.timeout_s
    assert before + REQUEST.timeout_s <= kwargs["deadline"] <= after + REQUEST.timeout_s


def test_generate_needs_a_model_request():
    with pytest.raises(TypeError, match="ModelRequest"):
        make_provider().generate({"system_prompt": "x"})


def test_open_builds_the_http_clients_on_the_callers_thread(monkeypatch):
    opened = []
    monkeypatch.setattr(HttpClient, "open", lambda self: opened.append(self))
    provider = make_provider()

    assert provider.open() is None
    assert len(opened) == 1


# ---------------------------------------------------------------------------------------------------------------
# The reply: content, finish, raw text, tokens
# ---------------------------------------------------------------------------------------------------------------


def test_a_complete_reply_parses_the_text_block_as_the_answer():
    response = answer(MESSAGE)

    assert isinstance(response, ProviderResponse)
    assert response.content == ANSWER
    assert response.finish == "complete"
    assert response.raw_text == json.dumps(ANSWER)
    assert (response.input_tokens, response.output_tokens) == (2095, 503)


@pytest.mark.parametrize(
    ("stop_reason", "finish"),
    [
        ("end_turn", "complete"),
        ("stop_sequence", "complete"),
        ("tool_use", "complete"),
        ("max_tokens", "truncated"),
        ("model_context_window_exceeded", "truncated"),
        ("pause_turn", "truncated"),
        ("refusal", "refused"),
    ],
)
def test_each_documented_stop_reason_maps_to_a_finish(stop_reason, finish):
    response = answer(message_with(stop_reason=stop_reason))

    assert response.finish == finish
    assert response.content == ANSWER


@pytest.mark.parametrize("stop_reason", ["brand_new_reason", "", 5, None, ["end_turn"]])
def test_an_unknown_or_missing_stop_reason_is_an_unexpected_reply(stop_reason):
    response = answer(message_with(stop_reason=stop_reason))

    assert response.content is None
    assert response.finish == "complete"
    assert response.raw_text == json.dumps(ANSWER)


def test_a_reply_without_a_stop_reason_is_an_unexpected_reply():
    reply = message_with()
    del reply["stop_reason"]

    response = answer(reply)

    assert response.content is None


def test_a_refusal_in_prose_is_refused_with_its_text_kept():
    reply = message_with(stop_reason="refusal", content=[{"type": "text", "text": "I can't help with that."}])

    response = answer(reply)

    assert response.finish == "refused"
    assert response.content is None
    assert response.raw_text == "I can't help with that."


def test_a_reply_without_a_text_block_has_no_content_and_keeps_the_reply_text():
    reply = message_with(stop_reason="max_tokens", content=[{"type": "thinking", "thinking": "", "signature": "s"}])

    response = answer(reply)

    assert response.content is None
    assert response.finish == "truncated"
    assert json.loads(response.raw_text) == reply


def test_a_reply_with_no_content_blocks_keeps_the_reply_text():
    response = answer(message_with(content=[]))

    assert response.content is None
    assert json.loads(response.raw_text)["content"] == []


@pytest.mark.parametrize(
    "text",
    [
        "Here are the records: " + json.dumps(ANSWER),
        "```json\n" + json.dumps(ANSWER) + "\n```",
        json.dumps(ANSWER) + json.dumps(ANSWER),
        "[]",
        '{"records": [], "records": []}',
        "",
    ],
    ids=["prose", "fence", "two-objects", "array", "repeated-key", "empty"],
)
def test_text_that_is_not_exactly_one_json_object_is_never_repaired(text):
    response = answer(message_with(content=[{"type": "text", "text": text}]))

    assert response.content is None
    assert response.finish == "complete"
    assert response.raw_text == text


def test_text_blocks_are_joined_in_order_and_other_blocks_skipped():
    halves = json.dumps(ANSWER)
    blocks = [
        {"type": "text", "text": halves[:10]},
        {"type": "redacted_thinking", "data": "opaque"},
        {"type": "text", "text": halves[10:]},
    ]

    response = answer(message_with(content=blocks))

    assert response.content == ANSWER
    assert response.raw_text == halves


def test_a_lone_surrogate_in_the_answer_is_escaped_in_content_and_raw_text():
    escaped_in_text = '{"records": [], "note": "\\ud800"}'
    reply = message_with(content=[{"type": "text", "text": escaped_in_text}])

    response = answer(reply)

    assert response.content == {"note": "[U+D800]", "records": []}
    response.raw_text.encode("utf-8")


def test_a_lone_surrogate_in_the_raw_text_is_escaped():
    body = b'{"content": [{"type": "text", "text": "not json \\udfff"}], "stop_reason": "end_turn"}'

    response = answer(content=body, headers={"content-type": "application/json"})

    assert response.content is None
    assert response.raw_text == "not json [U+DFFF]"


@pytest.mark.parametrize(
    ("usage", "tokens"),
    [
        ({"input_tokens": 10, "output_tokens": 5}, (10, 5)),
        (
            {"input_tokens": 10, "output_tokens": 5, "cache_creation_input_tokens": 3, "cache_read_input_tokens": 2},
            (15, 5),
        ),
        ({"input_tokens": 10, "output_tokens": 5, "cache_creation_input_tokens": None}, (10, 5)),
        ({"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": "2"}, (10, 5)),
        ({"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": -2}, (10, 5)),
        ({"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": True}, (10, 5)),
        ({"input_tokens": 10, "output_tokens": 5, "cache_creation_input_tokens": False}, (10, 5)),
        ({"input_tokens": True, "output_tokens": 5}, (None, 5)),
        ({"input_tokens": False, "output_tokens": 5, "cache_read_input_tokens": 2}, (None, 5)),
        ({"input_tokens": "10", "output_tokens": True}, (None, None)),
        ({"input_tokens": -1, "output_tokens": 2**63}, (None, None)),
        ({"input_tokens": 1.5, "output_tokens": None}, (None, None)),
        ({"cache_read_input_tokens": 4, "output_tokens": 5}, (None, 5)),
        ({}, (None, None)),
        ([10, 5], (None, None)),
        ("10", (None, None)),
        (None, (None, None)),
    ],
)
def test_usage_counts_input_with_its_cache_parts_and_ignores_junk(usage, tokens):
    response = answer(message_with(usage=usage))

    assert (response.input_tokens, response.output_tokens) == tokens
    assert response.content == ANSWER


def test_a_reply_without_usage_has_no_token_counts():
    reply = message_with()
    del reply["usage"]

    response = answer(reply)

    assert (response.input_tokens, response.output_tokens) == (None, None)


@pytest.mark.parametrize(
    "reply",
    [
        message_with(content={"type": "text", "text": "{}"}),
        message_with(content="{}"),
        message_with(content=["{}"]),
        message_with(content=[{"text": json.dumps(ANSWER)}]),
        message_with(content=[{"type": 3, "text": json.dumps(ANSWER)}]),
        message_with(content=[{"type": "text", "text": 5}]),
        message_with(content=[{"type": "text"}]),
        message_with(content=[{"type": "text", "text": json.dumps(ANSWER)}, None]),
        message_with(content=[{"type": "text", "text": json.dumps(ANSWER)}, {"type": "text", "text": 5}]),
        message_with(content=[{"type": "text", "text": json.dumps(ANSWER)}, {"type": "text"}]),
        {k: v for k, v in MESSAGE.items() if k != "content"},
        {"type": "error", "error": {"type": "api_error", "message": "boom"}},
    ],
    ids=[
        "content-object",
        "content-string",
        "block-string",
        "block-untyped",
        "block-type-not-str",
        "text-not-str",
        "text-missing",
        "block-null",
        "good-text-then-text-not-str",
        "good-text-then-text-missing",
        "no-content",
        "error-shape",
    ],
)
def test_an_unexpected_reply_shape_has_no_content_and_never_raises(reply):
    response = answer(reply)

    assert response.content is None
    assert json.loads(response.raw_text) == reply


def test_an_unexpected_shape_keeps_its_stop_reason_mapping():
    response = answer(message_with(stop_reason="max_tokens", content="{}"))

    assert response.content is None
    assert response.finish == "truncated"


@pytest.mark.parametrize(
    "body",
    [b"[1, 2]", b"<html>gateway</html>", b"", b'{"content": NaN}', b'"a string"'],
    ids=["array", "html", "empty", "nan", "string"],
)
def test_a_body_that_is_not_a_json_object_has_no_content_and_keeps_its_text(body):
    response = answer(content=body)

    assert response.content is None
    assert response.finish == "complete"
    assert response.raw_text == body.decode()
    assert (response.input_tokens, response.output_tokens) == (None, None)


# ---------------------------------------------------------------------------------------------------------------
# HTTP failures propagate as the HTTP helper raises them
# ---------------------------------------------------------------------------------------------------------------


def test_a_401_is_permanent_with_the_providers_message_and_the_key_redacted():
    handler = reply_with(401, json_body=error_body("authentication_error", f"invalid x-api-key {KEY}"))

    with pytest.raises(PermanentProviderError) as caught:
        make_provider(handler).generate(REQUEST)

    assert caught.value.status_code == 401
    assert "invalid x-api-key" in caught.value.detail
    assert KEY not in caught.value.detail
    assert len(handler.seen) == 1


def test_a_429_is_transient_with_its_retry_after():
    handler = reply_with(429, json_body=error_body("rate_limit_error", "slow down"), headers={"retry-after": "7"})

    with pytest.raises(TransientProviderError) as caught:
        make_provider(handler).generate(REQUEST)

    assert caught.value.status_code == 429
    assert caught.value.retry_after_s == 7.0
    assert caught.value.detail == "slow down"


@pytest.mark.parametrize(("status", "kind"), [(500, "api_error"), (503, "api_error"), (504, "timeout_error")])
def test_a_retryable_5xx_is_transient(status, kind):
    with pytest.raises(TransientProviderError) as caught:
        answer(error_body(kind, "try again"), status=status)

    assert caught.value.status_code == status


def test_a_529_overloaded_is_transient():
    handler = reply_with(529, json_body=error_body("overloaded_error", "Overloaded"), headers={"retry-after": "3"})

    with pytest.raises(TransientProviderError) as caught:
        make_provider(handler).generate(REQUEST)

    assert type(caught.value) is TransientProviderError
    assert caught.value.status_code == 529
    assert caught.value.retry_after_s == 3.0
    assert caught.value.detail == "Overloaded"


@pytest.mark.parametrize("status", [520, 521, 522, 523, 524])
def test_a_cloudflare_origin_failure_is_transient(status):
    with pytest.raises(TransientProviderError) as caught:
        answer(content=b"<html>origin error</html>", status=status)

    assert type(caught.value) is TransientProviderError
    assert caught.value.status_code == status


@pytest.mark.parametrize("status", [501, 505, 511])
def test_a_5xx_outside_the_retryable_set_is_permanent(status):
    with pytest.raises(PermanentProviderError) as caught:
        answer(error_body("api_error", "no"), status=status)

    assert caught.value.status_code == status


@pytest.mark.parametrize(
    ("status", "kind"), [(400, "invalid_request_error"), (403, "permission_error"), (413, "request_too_large")]
)
def test_other_4xx_are_permanent(status, kind):
    with pytest.raises(PermanentProviderError) as caught:
        answer(error_body(kind, "no"), status=status)

    assert caught.value.status_code == status


def test_a_redirect_is_never_followed():
    handler = reply_with(307, headers={"location": "https://evil.example/v1/messages"})

    with pytest.raises(PermanentProviderError, match="redirect"):
        make_provider(handler).generate(REQUEST)

    assert len(handler.seen) == 1


def test_a_401_traceback_with_its_locals_holds_no_key():
    handler = reply_with(401, json_body=error_body("authentication_error", f"bad key {KEY}"))

    with pytest.raises(PermanentProviderError) as caught:
        make_provider(handler).generate(REQUEST)

    text = traceback_text(caught.value)
    assert "generate" in text
    assert "hunter2" not in text
    assert "hunter2" not in repr(caught.value)


# ---------------------------------------------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "base_url",
    [DEFAULT_BASE_URL, "https://api.anthropic.com/", "https://API.Anthropic.com:443", "https://api.anthropic.com/./"],
)
def test_the_default_endpoint_however_written_gets_the_plain_model_id(base_url):
    provider = make_provider(base_url=base_url)

    assert provider.model_id == f"anthropic:{MODEL}"
    assert provider.rate_key == "https://api.anthropic.com"


def test_the_default_base_url_is_already_normalized():
    assert check_endpoint(DEFAULT_BASE_URL) == DEFAULT_BASE_URL


def test_a_gateway_gets_a_model_id_naming_its_endpoint():
    provider = make_provider(base_url="https://GW.example/anthropic/")
    default = make_provider()

    assert provider.model_id == f"anthropic:{MODEL}@https://gw.example/anthropic"
    assert provider.rate_key == "https://gw.example/anthropic"
    assert provider.model_id != default.model_id


def test_a_loopback_http_gateway_is_allowed_and_named():
    provider = make_provider(base_url="http://localhost:8080")

    assert provider.model_id == f"anthropic:{MODEL}@http://localhost:8080"


def test_the_provider_kind_is_anthropic():
    assert make_provider().provider_kind == "anthropic"


@pytest.mark.parametrize("base_url", [DEFAULT_BASE_URL, "https://gw.example/anthropic", "http://localhost:8080"])
def test_the_model_family_is_anthropic_whatever_the_endpoint(base_url):
    assert make_provider(base_url=base_url).model_family == "anthropic"


def test_generation_params_are_the_exact_json_of_what_shapes_the_output():
    provider = make_provider()

    assert MECHANISM == "json_schema"
    assert WIRE == "messages/1"
    assert canonical_json(provider.generation_params) == (
        '{"api_version":"2026-10-21","mechanism":"json_schema","wire":"messages/1"}'
    )


OLD = '{"api_version":"2023-06-01","mechanism":"json_schema",'


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({"api_version": "2023-06-01"}, OLD + '"wire":"messages/1"}'),
        ({"api_version": "2023-06-01", "temperature": 0}, OLD + '"temperature":0.0,"wire":"messages/1"}'),
        ({"api_version": "2023-06-01", "temperature": -0.0}, OLD + '"temperature":0.0,"wire":"messages/1"}'),
        ({"api_version": "2023-06-01", "temperature": 1}, OLD + '"temperature":1.0,"wire":"messages/1"}'),
        ({"api_version": "2023-06-01", "temperature": 0.25}, OLD + '"temperature":0.25,"wire":"messages/1"}'),
        ({"temperature": None}, '{"api_version":"2026-10-21","mechanism":"json_schema","wire":"messages/1"}'),
    ],
)
def test_generation_params_follow_the_options(options, expected):
    assert canonical_json(make_provider(**options).generation_params) == expected


def test_generation_params_are_a_fresh_mapping_each_time():
    provider = make_provider()
    first = provider.generation_params
    first["api_version"] = "2023-06-01"

    assert provider.generation_params["api_version"] == "2026-10-21"
    assert provider.generation_params is not provider.generation_params


def test_max_connections_is_exposed_read_only():
    provider = make_provider(max_connections=3)

    assert provider.max_connections == 3
    assert make_provider().max_connections == 8
    with pytest.raises(AttributeError):
        provider.max_connections = 4


def test_the_sent_temperature_is_the_normalized_one():
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler, api_version="2023-06-01", temperature=-0.0).generate(REQUEST)

    assert b'"temperature":0.0' in handler.seen[0].content


def test_the_provider_passes_the_preflight_check():
    assert check_provider(make_provider()) is not None


def test_the_repr_names_the_model_and_hides_the_key():
    provider = make_provider()

    assert f"anthropic:{MODEL}" in repr(provider)
    assert "hunter2" not in repr(provider)
    assert "hunter2" not in str(provider)


@pytest.mark.parametrize("build", [lambda: make_provider(), lambda: gc_provider()], ids=["direct", "google-cloud"])
def test_a_provider_never_pickles_or_copies_its_credential(build):
    provider = build()

    for attempt in (pickle.dumps, copy.copy, copy.deepcopy):
        with pytest.raises(TypeError, match="never pickles or copies"):
            attempt(provider)


# ---------------------------------------------------------------------------------------------------------------
# Construction: checks and the API key
# ---------------------------------------------------------------------------------------------------------------


VERSIONS_RULE = "api_version must be one of the documented anthropic-version values: 2026-10-21, 2023-06-01"


@pytest.mark.parametrize(
    ("options", "phrase"),
    [
        ({"model": ""}, "model"),
        ({"model": "claude opus"}, "model"),
        ({"model": "https://evil.example/claude"}, "URL"),
        ({"model": 5}, "model"),
        ({"temperature": True}, "temperature must be None or a number"),
        ({"temperature": "0"}, "temperature must be None or a number"),
        ({"temperature": math.nan}, "temperature must be None or a finite number in 0-1"),
        ({"temperature": math.inf}, "temperature must be None or a finite number in 0-1"),
        ({"temperature": -0.1}, "temperature must be None or a finite number in 0-1"),
        ({"temperature": 1.01}, "temperature must be None or a finite number in 0-1"),
        ({"temperature": 10**400}, "temperature must be None or a finite number in 0-1"),
        ({"api_version": ""}, VERSIONS_RULE),
        ({"api_version": "latest"}, VERSIONS_RULE),
        ({"api_version": "2023-6-1"}, VERSIONS_RULE),
        ({"api_version": "20230601"}, VERSIONS_RULE),
        ({"api_version": " 2023-06-01"}, VERSIONS_RULE),
        ({"api_version": "2023-06-01\n"}, VERSIONS_RULE),
        ({"api_version": "\uff12\uff10\uff12\uff13-06-01"}, VERSIONS_RULE),
        ({"api_version": 20230601}, VERSIONS_RULE),
        ({"api_version": None}, VERSIONS_RULE),
        ({"api_version": "2026-02-30"}, VERSIONS_RULE),
        ({"api_version": "2023-01-01"}, VERSIONS_RULE),
        ({"api_version": "2027-01-01"}, VERSIONS_RULE),
        ({"api_version": "2026-10-22"}, VERSIONS_RULE),
        ({"temperature": 0.0}, 'api_version="2023-06-01"'),
        ({"api_version": "2026-10-21", "temperature": 0.5}, 'api_version="2023-06-01"'),
        ({"base_url": "http://10.0.0.5"}, "loopback"),
        ({"base_url": "https://user@gw.example"}, "credentials"),
        ({"base_url": "https://gw.example/?key=1"}, "query"),
        ({"base_url": "ftp://gw.example"}, "scheme"),
        ({"base_url": None}, "str"),
        ({"allow_insecure_http": "yes"}, "allow_insecure_http"),
        ({"max_connections": 0}, "max_connections must be an int in 1-1024"),
        ({"max_connections": 1025}, "max_connections must be an int in 1-1024"),
        ({"max_connections": True}, "max_connections must be an int in 1-1024"),
        ({"max_connections": "8"}, "max_connections must be an int in 1-1024"),
        ({"max_connections": 8.0}, "max_connections must be an int in 1-1024"),
    ],
)
def test_a_bad_option_is_refused_naming_it(options, phrase):
    with pytest.raises(RationalizationError) as caught:
        make_provider(**options)

    assert phrase in str(caught.value)
    assert_secret_free(caught.value)


def test_the_temperature_refusal_explains_the_remedy():
    with pytest.raises(RationalizationError) as caught:
        make_provider(temperature=0.0)

    message = str(caught.value)
    assert "2026-10-21" in message
    assert "temperature=None" in message
    assert 'api_version="2023-06-01"' in message


def test_insecure_http_to_another_host_needs_the_opt_in():
    handler = reply_with(json_body=MESSAGE)
    provider = make_provider(handler, base_url="http://10.0.0.5", allow_insecure_http=True)

    provider.generate(REQUEST)

    assert provider.model_id == f"anthropic:{MODEL}@http://10.0.0.5"
    assert str(handler.seen[0].url) == "http://10.0.0.5/v1/messages"


@pytest.mark.parametrize("bounds", [{"max_connections": 1}, {"max_connections": 1024}])
def test_the_pool_bounds_are_inclusive(bounds):
    assert make_provider(**bounds).max_connections == bounds["max_connections"]


def test_a_transport_that_is_not_an_httpx_transport_is_refused():
    with pytest.raises(RationalizationError, match="transport") as caught:
        AnthropicProvider(MODEL, api_key=KEY, transport=lambda request: None)

    assert_secret_free(caught.value)


@pytest.mark.parametrize(
    "key", ["", "   ", f"{KEY}\n", f" {KEY}", "sk-ant-h\u00fcnter2", "sk-ant\x00hunter2", 12345, b"sk-ant-hunter2"]
)
def test_a_bad_api_key_is_refused_without_echoing_it(key):
    with pytest.raises(RationalizationError, match="api_key") as caught:
        make_provider(api_key=key)

    assert_secret_free(caught.value)


def test_the_key_comes_from_the_environment_when_not_given(monkeypatch):
    monkeypatch.setenv(API_KEY_VARIABLE, KEY)
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler, api_key=None).generate(REQUEST)

    assert API_KEY_VARIABLE == "ANTHROPIC_API_KEY"
    assert handler.seen[0].headers["x-api-key"] == KEY


# The environment key goes only to Anthropic: a gateway never receives ANTHROPIC_API_KEY unless it is passed.

GATEWAYS = ["https://gw.example/anthropic", "https://GW.example:8443/v1/", "http://localhost:8080"]


@pytest.mark.parametrize("base_url", GATEWAYS)
def test_a_gateway_never_gets_the_environment_key_and_nothing_is_sent(monkeypatch, base_url):
    monkeypatch.setenv(API_KEY_VARIABLE, KEY)
    handler = reply_with(json_body=MESSAGE)

    with pytest.raises(RationalizationError) as caught:
        make_provider(handler, api_key=None, base_url=base_url)

    message = str(caught.value)
    assert "ANTHROPIC_API_KEY is read only for https://api.anthropic.com" in message
    assert "pass api_key=" in message
    assert check_endpoint(base_url).split("://", 1)[1].split("/", 1)[0] in message
    assert handler.seen == []
    assert_secret_free(caught.value)


@pytest.mark.parametrize("base_url", GATEWAYS)
def test_a_gateway_from_a_model_string_never_gets_the_environment_key(monkeypatch, base_url):
    monkeypatch.setenv(API_KEY_VARIABLE, KEY)
    handler = reply_with(json_body=MESSAGE)

    with pytest.raises(RationalizationError, match="ANTHROPIC_API_KEY is read only for") as caught:
        AnthropicProvider.from_model_string(PARSED, base_url=base_url, transport=httpx.MockTransport(handler))

    assert handler.seen == []
    assert_secret_free(caught.value)


@pytest.mark.parametrize("base_url", GATEWAYS)
def test_a_gateway_with_an_explicit_key_sends_that_key_only(monkeypatch, base_url):
    monkeypatch.setenv(API_KEY_VARIABLE, KEY)
    explicit = "sk-gateway-explicit-KEY"
    handler = reply_with(json_body=MESSAGE)

    make_provider(handler, api_key=explicit, base_url=base_url).generate(REQUEST)
    AnthropicProvider.from_model_string(
        PARSED, api_key=explicit, base_url=base_url, transport=httpx.MockTransport(handler)
    ).generate(REQUEST)

    assert len(handler.seen) == 2
    for sent in handler.seen:
        assert sent.headers["x-api-key"] == explicit
        assert "hunter2" not in str(sent.headers)
        assert b"hunter2" not in sent.content
        assert "hunter2" not in str(sent.url)


@pytest.mark.parametrize(
    "base_url", [DEFAULT_BASE_URL, "https://api.anthropic.com/", "https://API.Anthropic.com:443", None]
)
def test_the_default_endpoint_however_written_still_reads_the_environment_key(monkeypatch, base_url):
    monkeypatch.setenv(API_KEY_VARIABLE, KEY)
    handler = reply_with(json_body=MESSAGE)
    options = {} if base_url is None else {"base_url": base_url}

    make_provider(handler, api_key=None, **options).generate(REQUEST)
    AnthropicProvider.from_model_string(PARSED, transport=httpx.MockTransport(handler), **options).generate(REQUEST)

    assert [sent.headers["x-api-key"] for sent in handler.seen] == [KEY, KEY]


def test_a_gateway_without_any_key_is_refused_naming_the_rule(monkeypatch):
    handler = reply_with(json_body=MESSAGE)

    with pytest.raises(RationalizationError, match="pass api_key= for gw.example explicitly") as caught:
        make_provider(handler, api_key=None, base_url="https://gw.example/anthropic")

    assert handler.seen == []
    assert_secret_free(caught.value)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_a_missing_key_is_refused_naming_the_variable(monkeypatch, value):
    if value is not None:
        monkeypatch.setenv(API_KEY_VARIABLE, value)

    with pytest.raises(RationalizationError, match="ANTHROPIC_API_KEY") as caught:
        make_provider(api_key=None)

    assert_secret_free(caught.value)


@pytest.mark.parametrize("value", [f"{KEY}\n", f" {KEY}", f"{KEY}\u00e9", f"{KEY}\x7f"])
def test_a_bad_key_in_the_environment_is_refused_naming_the_variable_only(monkeypatch, value):
    monkeypatch.setenv(API_KEY_VARIABLE, value)

    with pytest.raises(RationalizationError, match="ANTHROPIC_API_KEY") as caught:
        make_provider(api_key=None)

    assert str(caught.value).startswith("AnthropicProvider: the ANTHROPIC_API_KEY environment variable")
    assert_secret_free(caught.value)


@pytest.mark.parametrize(
    "options",
    [
        {"temperature": 5},
        {"temperature": 0.5},
        {"api_version": "nope"},
        {"base_url": "http://10.0.0.5"},
        {"model": "bad model"},
        {"max_connections": 0},
        {"transport": object()},
        {"api_key": f"{KEY}\n"},
    ],
    ids=["temperature", "temperature-on-2026", "api_version", "base_url", "model", "pool", "transport", "bad-key"],
)
def test_a_construction_traceback_with_its_locals_holds_no_key(options):
    options.setdefault("api_key", KEY)
    model = options.pop("model", MODEL)

    with pytest.raises(RationalizationError) as caught:
        AnthropicProvider(model, **options)

    assert_secret_free(caught.value)


# ---------------------------------------------------------------------------------------------------------------
# from_model_string
# ---------------------------------------------------------------------------------------------------------------

PARSED = parse_model_string(f"anthropic:{MODEL}")


def test_from_model_string_builds_the_provider_with_its_options():
    handler = reply_with(json_body=MESSAGE)

    provider = AnthropicProvider.from_model_string(
        PARSED,
        api_key=KEY,
        api_version="2023-06-01",
        temperature=0.5,
        max_connections=2,
        transport=httpx.MockTransport(handler),
    )
    provider.generate(REQUEST)

    assert provider.model_id == f"anthropic:{MODEL}"
    assert provider.max_connections == 2
    assert json.loads(handler.seen[0].content)["temperature"] == 0.5
    assert handler.seen[0].headers["x-api-key"] == KEY
    assert handler.seen[0].headers["anthropic-version"] == "2023-06-01"


def test_from_model_string_reads_the_environment_key(monkeypatch):
    monkeypatch.setenv(API_KEY_VARIABLE, KEY)

    provider = AnthropicProvider.from_model_string(PARSED)

    assert provider.model_id == f"anthropic:{MODEL}"


@pytest.mark.parametrize("spec", ["openai:gpt-4o", "gemini:gemini-2.5-pro", "ollama:llama3"])
def test_from_model_string_refuses_another_kind(spec):
    with pytest.raises(RationalizationError, match="anthropic") as caught:
        AnthropicProvider.from_model_string(parse_model_string(spec), api_key=KEY)

    assert_secret_free(caught.value)


def test_from_model_string_needs_a_parsed_model_string():
    with pytest.raises(RationalizationError, match="ModelString") as caught:
        AnthropicProvider.from_model_string(f"anthropic:{MODEL}", api_key=KEY)

    assert_secret_free(caught.value)


def test_from_model_string_refuses_unknown_options_naming_only_them():
    with pytest.raises(RationalizationError) as caught:
        AnthropicProvider.from_model_string(PARSED, api_key=KEY, zeta=0.25, alpha=0.75, mid="x-secret-value")

    message = str(caught.value)
    assert "takes no option alpha, mid, zeta:" in message
    assert "0.25" not in message
    assert "x-secret-value" not in message
    assert_secret_free(caught.value)


def test_from_model_string_names_a_bad_key_as_the_key_problem():
    with pytest.raises(RationalizationError, match="api_key must be non-blank printable ASCII") as caught:
        AnthropicProvider.from_model_string(PARSED, api_key=f"{KEY}\n")

    assert_secret_free(caught.value)


@pytest.mark.parametrize(
    "options",
    [
        {"temperature": 5},
        {"temperature": 0.5},
        {"api_version": "nope"},
        {"base_url": "http://10.0.0.5"},
        {"transport": object()},
        {"api_key": f"{KEY}\n"},
        {"api_key": 5},
        {"unknown": True},
    ],
    ids=[
        "temperature",
        "temperature-on-2026",
        "api_version",
        "base_url",
        "transport",
        "bad-key",
        "key-type",
        "unknown",
    ],
)
def test_a_from_model_string_traceback_with_its_locals_holds_no_key(options):
    options.setdefault("api_key", KEY)

    with pytest.raises(RationalizationError) as caught:
        AnthropicProvider.from_model_string(PARSED, **options)

    assert_secret_free(caught.value)


def test_a_from_model_string_traceback_holds_no_environment_key(monkeypatch):
    monkeypatch.setenv(API_KEY_VARIABLE, f"{KEY}\n")

    with pytest.raises(RationalizationError, match="ANTHROPIC_API_KEY") as caught:
        AnthropicProvider.from_model_string(PARSED)

    assert_secret_free(caught.value)


# ---------------------------------------------------------------------------------------------------------------
# transform_schema: the subset structured outputs accepts
# ---------------------------------------------------------------------------------------------------------------

ALLOWED_KEYWORDS = frozenset(
    {
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "anyOf",
        "enum",
        "const",
        "$ref",
        "description",
        "default",
        "format",
        "minItems",
    }
)


def schema_nodes(schema):
    """Every schema node of a transformed document, with its JSON pointer: the root body, then each $defs entry."""
    stack = [("#", schema)] + [(f"#/$defs/{name}", node) for name, node in schema.get("$defs", {}).items()]
    while stack:
        pointer, node = stack.pop()
        yield pointer, node
        for name, child in node.get("properties", {}).items():
            stack.append((f"{pointer}/properties/{name}", child))
        if isinstance(node.get("items"), dict):
            stack.append((f"{pointer}/items", node["items"]))
        for index, member in enumerate(node.get("anyOf", [])):
            stack.append((f"{pointer}/anyOf/{index}", member))


RFC3339_DATE_TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})"
)


def _is_date(text):
    try:
        return re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", text) is not None and bool(date.fromisoformat(text))
    except ValueError:
        return False


def _is_date_time(text):
    try:
        return RFC3339_DATE_TIME.fullmatch(text) is not None and bool(datetime.fromisoformat(text.replace("z", "Z")))
    except ValueError:
        return False


FORMAT_CHECKS = {"date": _is_date, "date-time": _is_date_time}


def conforms(value, node, defs):
    """Whether ``value`` matches ``node`` in the transformed subset (a test-only checker of what the grammar
    enforces, ``format`` included: ``date-time`` is RFC 3339, which requires an offset)."""
    if "$ref" in node:
        return conforms(value, defs[node["$ref"].removeprefix("#/$defs/")], defs)
    if "anyOf" in node:
        return any(conforms(value, member, defs) for member in node["anyOf"])
    if "enum" in node and value not in node["enum"]:
        return False
    checks = {
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: type(v) is int,
        "number": lambda v: type(v) in (int, float),
        "boolean": lambda v: type(v) is bool,
        "null": lambda v: v is None,
    }
    kind = node.get("type")
    if kind is not None and not checks[kind](value):
        return False
    if isinstance(value, str) and node.get("format") in FORMAT_CHECKS and not FORMAT_CHECKS[node["format"]](value):
        return False
    if kind == "object":
        properties = node.get("properties", {})
        if not set(node.get("required", [])) <= set(value) or not set(value) <= set(properties):
            return False
        return all(conforms(item, properties[name], defs) for name, item in value.items())
    if kind == "array":
        return len(value) >= node.get("minItems", 0) and all(conforms(item, node["items"], defs) for item in value)
    return True


def wrapped(record):
    return {"records": [{"record": record, "grounding_quotes": ["INV-001"], "source_element_ids": ["p0001_e000"]}]}


@pytest.mark.parametrize(
    ("value", "fmt", "expected"),
    [
        ("2026-11-03T08:30:00+00:00", "date-time", True),
        ("2026-11-03T08:30:00.5Z", "date-time", True),
        ("2026-11-03T08:30:00", "date-time", False),
        ("2026-13-03T08:30:00Z", "date-time", False),
        ("2026-11-04", "date", True),
        ("2026-02-30", "date", False),
        ("20261104", "date", False),
    ],
)
def test_the_conformance_checker_honours_format(value, fmt, expected):
    assert conforms(value, {"type": "string", "format": fmt}, {}) is expected


@pytest.mark.parametrize("case", list(REALISTIC))
def test_the_transform_of_each_realistic_schema_keeps_to_the_documented_subset(case):
    model, _, _ = REALISTIC[case]

    sent = make_provider().transform_schema(response_schema(model))

    nodes = list(schema_nodes(sent))
    assert len(nodes) > 10, "vacuous: the walk found almost no nodes"
    assert sent["type"] == "object"
    for pointer, node in nodes:
        root_extra = {"$defs"} if pointer == "#" else set()
        assert set(node) <= ALLOWED_KEYWORDS | root_extra, pointer
        if "$ref" in node:
            assert set(node) == {"$ref"}, pointer
            assert node["$ref"].removeprefix("#/$defs/") in sent["$defs"], pointer
        if node.get("type") == "object":
            assert node["additionalProperties"] is False, pointer
        if "format" in node:
            assert node["format"] in ENFORCED_FORMATS, pointer
        if "minItems" in node:
            assert node["minItems"] in (0, 1), pointer
    assert ENFORCED_FORMATS == SUPPORTED_FORMATS - {"date-time"}
    optional, union = parameter_counts(sent)
    assert optional <= MAX_OPTIONAL_PARAMETERS
    assert union <= MAX_UNION_PARAMETERS


# Counted by hand from test_schema's models (optional = not in required; union = anyOf or a type array, a $ref never):
# invoice: Invoice 5/3 + Address 2/1 + LineItem 2/3; BOM: BillOfMaterials 1/0 + Assembly 1/0 + Component 3/1 +
# Supplier 1/1; contract: Contract 5/3 + Party 2/2; lab report: LabReport 4/0 + Specimen 0/0 + Analyte 2/2.
REALISTIC_COUNTS = {"invoice": (9, 7), "camelCase BOM": (6, 2), "contract": (7, 5), "lab report": (6, 2)}


@pytest.mark.parametrize("case", list(REALISTIC))
def test_the_realistic_schemas_optional_and_union_counts(case):
    model, _, _ = REALISTIC[case]

    sent = make_provider().transform_schema(response_schema(model))

    assert parameter_counts(sent) == REALISTIC_COUNTS[case]


def test_datetimes_naive_or_aware_are_hinted_never_enforced():
    class Times(BaseModel):
        naive: NaiveDatetime
        aware: AwareDatetime
        described: NaiveDatetime = Field(description="When it was collected")

    sent = make_provider().transform_schema(response_schema(Times))
    record = sent["properties"]["records"]["items"]["properties"]["record"]
    values = {"naive": "2026-11-03T08:30:00", "aware": "2026-11-03T08:30:00+01:00", "described": "2026-11-03T08:30:00"}

    assert record["properties"]["naive"] == {"description": '{format: "date-time"}', "type": "string"}
    assert record["properties"]["aware"] == {"description": '{format: "date-time"}', "type": "string"}
    assert record["properties"]["described"]["description"] == 'When it was collected\n\n{format: "date-time"}'
    assert conforms(values, record, {})
    assert Times.model_validate(values).naive.tzinfo is None


@pytest.mark.parametrize("case", list(REALISTIC))
def test_a_record_shaped_like_the_transform_still_validates_in_pydantic(case):
    model, _, record = REALISTIC[case]

    sent = make_provider().transform_schema(response_schema(model))

    assert conforms(wrapped(record), sent, sent.get("$defs", {}))
    assert not conforms(wrapped({**record, "unexpected": 1}), sent, sent.get("$defs", {}))
    assert model.model_validate(record) is not None


def test_dropped_constraints_become_a_hint_in_the_description():
    invoice, _, _ = REALISTIC["invoice"]
    contract, _, _ = REALISTIC["contract"]
    lab, _, _ = REALISTIC["lab report"]
    provider = make_provider()

    sent = provider.transform_schema(response_schema(invoice))
    address = sent["$defs"]["Address"]["properties"]
    line = sent["$defs"]["LineItem"]["properties"]
    invoice_record = sent["properties"]["records"]["items"]["properties"]["record"]["properties"]
    parties = provider.transform_schema(response_schema(contract))["properties"]["records"]["items"]["properties"]
    lab_record = provider.transform_schema(response_schema(lab))["properties"]["records"]["items"]["properties"]

    assert address["country"] == {"default": "US", "description": "{maxLength: 2, minLength: 2}", "type": "string"}
    assert address["street"] == {"description": "Street and number", "type": "string"}
    assert line["line_no"] == {"description": "1-based line number\n\n{exclusiveMinimum: 0}", "type": "integer"}
    assert address["postal_code"]["anyOf"][0] == {"description": '{pattern: "^[A-Z0-9 -]{3,10}$"}', "type": "string"}
    assert invoice_record["line_items"]["description"] == "{maxItems: 500}"
    assert "maxItems" not in invoice_record["line_items"]
    assert parties["record"]["properties"]["parties"]["description"] == "{minItems: 2}"
    assert "minItems" not in parties["record"]["properties"]["parties"]
    assert lab_record["record"]["properties"]["results"]["minItems"] == 1
    assert lab_record["record"]["properties"]["comments"]["items"] == {
        "description": "{maxLength: 500}",
        "type": "string",
    }


def test_a_ref_keeps_only_its_reference():
    invoice, _, _ = REALISTIC["invoice"]

    sent = make_provider().transform_schema(response_schema(invoice))
    record = sent["properties"]["records"]["items"]["properties"]["record"]["properties"]

    assert record["bill_to"] == {"$ref": "#/$defs/Address"}
    assert record["status"] == {"$ref": "#/$defs/InvoiceStatus"}
    assert sent["$defs"]["InvoiceStatus"] == {"enum": ["draft", "issued"], "type": "string"}


def test_titles_examples_and_unknown_annotations_are_dropped():
    schema = {
        "type": "object",
        "title": "Root",
        "properties": {
            "a": {"type": "string", "title": "A", "examples": ["x"], "deprecated": True, "x-unit": "kg"},
            "b": {"type": "string", "format": "email", "readOnly": True, "$comment": "c"},
        },
        "required": ["a"],
    }

    sent = make_provider().transform_schema(schema)

    assert sent == {
        "type": "object",
        "properties": {"a": {"type": "string"}, "b": {"type": "string", "format": "email"}},
        "required": ["a"],
        "additionalProperties": False,
    }


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        ({"type": "string", "format": "date"}, {"type": "string", "format": "date"}),
        ({"type": "string", "format": "date-time"}, {"type": "string", "description": '{format: "date-time"}'}),
        ({"type": "string", "format": "uuid"}, {"type": "string", "format": "uuid"}),
        (
            {"type": ["object", "null"], "properties": {"x": {"type": "string"}}},
            {"type": ["object", "null"], "properties": {"x": {"type": "string"}}, "additionalProperties": False},
        ),
        ({"enum": ["a", "b"]}, {"enum": ["a", "b"]}),
        ({"const": 3}, {"const": 3}),
        ({"type": ["object", "null"]}, {"type": ["object", "null"], "additionalProperties": False}),
        (
            {"anyOf": [{"type": "null"}, {"type": "string"}], "properties": {"x": {"type": "string"}}},
            {
                "anyOf": [{"type": "null"}, {"type": "string"}],
                "properties": {"x": {"type": "string"}},
                "additionalProperties": False,
            },
        ),
        (
            {"enum": ["a"], "properties": {"x": {"type": "string"}}},
            {"enum": ["a"], "properties": {"x": {"type": "string"}}, "additionalProperties": False},
        ),
        ({"type": "string", "format": "binary"}, {"type": "string", "description": '{format: "binary"}'}),
        (
            {"type": "array", "items": {"type": "string"}, "minItems": 0},
            {"type": "array", "items": {"type": "string"}, "minItems": 0},
        ),
        (
            {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
            {"type": "array", "items": {"type": "string"}, "description": "{uniqueItems: true}"},
        ),
        (
            {"type": "number", "multipleOf": 5, "maximum": 100},
            {"type": "number", "description": "{maximum: 100, multipleOf: 5}"},
        ),
        ({"type": "integer", "exclusiveMaximum": 9}, {"type": "integer", "description": "{exclusiveMaximum: 9}"}),
        ({"const": "x", "type": "string"}, {"const": "x", "type": "string"}),
        ({"enum": [1, "a", True, None, 2.5]}, {"enum": [1, "a", True, None, 2.5]}),
        ({"type": ["string", "null"]}, {"type": ["string", "null"]}),
        ({"type": "string", "default": "d"}, {"type": "string", "default": "d"}),
    ],
)
def test_each_keyword_rule(node, expected):
    sent = make_provider().transform_schema({"type": "object", "properties": {"f": node}})

    assert sent["properties"]["f"] == expected


def test_an_object_without_properties_still_gets_additional_properties_false():
    sent = make_provider().transform_schema({"type": "object", "properties": {"o": {"type": "object"}}})

    assert sent["properties"]["o"] == {"type": "object", "additionalProperties": False}


def test_the_transform_is_pure_and_fresh():
    invoice, _, _ = REALISTIC["invoice"]
    neutral = response_schema(invoice)
    before = copy.deepcopy(neutral)
    provider = make_provider()

    first = provider.transform_schema(neutral)
    first["properties"]["records"]["type"] = "string"
    second = provider.transform_schema(neutral)

    assert neutral == before
    assert second["properties"]["records"]["type"] == "array"
    assert second == provider.transform_schema(copy.deepcopy(before))
    assert canonical_copy(second) == second
    assert list(second) == sorted(second)
    assert list(second["properties"]["records"]) == sorted(second["properties"]["records"])


def test_the_transform_is_a_fixed_point():
    invoice, _, _ = REALISTIC["invoice"]
    provider = make_provider()

    once = provider.transform_schema(response_schema(invoice))

    assert provider.transform_schema(once) == once


def test_the_transformed_schema_fits_a_model_request():
    invoice, _, _ = REALISTIC["invoice"]
    sent = make_provider().transform_schema(response_schema(invoice))

    request = ModelRequest(
        unit_id="u",
        model_id="m",
        system_prompt="s",
        user_envelope="e",
        json_schema=sent,
        max_output_tokens=1,
        timeout_s=1,
    )

    assert request.json_schema == sent


@pytest.mark.parametrize("schema", [[{"type": "object"}], "{}", None])
def test_the_transform_needs_a_mapping(schema):
    with pytest.raises(TypeError, match="Mapping"):
        make_provider().transform_schema(schema)


def test_the_transform_takes_any_mapping():
    invoice, _, _ = REALISTIC["invoice"]
    neutral = response_schema(invoice)

    sent = make_provider().transform_schema(MappingProxyType(neutral))

    assert sent == make_provider().transform_schema(neutral)


def obj(**properties):
    return {"type": "object", "properties": properties}


@pytest.mark.parametrize(
    ("schema", "phrase"),
    [
        ({"type": "string"}, "object"),
        ({"properties": {}}, "object"),
        (obj(a={"oneOf": [{"type": "string"}]}), "#/properties/a"),
        (obj(a={"allOf": [{"type": "string"}]}), "allOf"),
        (obj(a={"not": {"type": "string"}}), "not"),
        (obj(a={"if": {"type": "string"}, "then": {}}), "if"),
        (obj(a={"type": "object", "patternProperties": {"x": {}}}), "patternProperties"),
        (obj(a={"type": "array", "prefixItems": [{}]}), "prefixItems"),
        (obj(a={"type": "array", "items": [{"type": "string"}]}), "tuple items"),
        (obj(a={"type": "object", "additionalProperties": True}), "additionalProperties"),
        (obj(a={"type": "object", "additionalProperties": {"type": "string"}}), "additionalProperties"),
        (obj(a={"$ref": "https://example.com/schema.json"}), "external"),
        (obj(a={"$ref": "#/definitions/A"}), "#/$defs/"),
        (obj(a={"$ref": "#/$defs/Missing"}), "Missing"),
        (obj(a={"$ref": 5}), "$ref"),
        ({**obj(a={"$ref": "#/$defs/A"}), "$defs": {"A": {"type": "string", "$defs": {}}}}, "$defs"),
        ({**obj(), "$defs": []}, "$defs"),
        (obj(a={"type": "strng"}), "type"),
        (obj(a={"type": []}), "type"),
        (obj(a={"type": "object", "properties": []}), "properties"),
        (obj(a={"type": "object", "required": "a"}), "required"),
        (obj(a={"enum": [{"x": 1}]}), "enum"),
        (obj(a={"enum": []}), "enum"),
        (obj(a={"const": [1]}), "const"),
        (obj(a={"anyOf": []}), "anyOf"),
        (obj(a={"anyOf": {"type": "string"}}), "anyOf"),
        (obj(a={"type": "string", "description": 5}), "description"),
        (obj(a=True), "#/properties/a"),
        ({"type": "object", "properties": {"a": {"type": "string"}}, "$id": "x"}, "$id"),
        (obj(a={"type": "string", "$anchor": "here"}), "$anchor"),
        (obj(a={"type": "object", "properties": {}, "dependentRequired": {"a": ["b"]}}), "dependentRequired"),
        (obj(a={"type": ["string", "string"]}), "type"),
        (obj(a={}), "no type"),
        (obj(a={"description": "anything at all", "default": 1}), "no type"),
        (obj(a={"properties": {"x": {"type": "string"}}}), "no type"),
        (obj(a={"type": "array", "items": {}}), "#/properties/a/items"),
    ],
)
def test_a_shape_structured_outputs_cannot_take_is_refused_naming_where(schema, phrase):
    with pytest.raises(RationalizationError) as caught:
        make_provider().transform_schema(schema)

    assert phrase in str(caught.value)


@pytest.mark.parametrize(
    "defs",
    [
        {"A": obj(a={"$ref": "#/$defs/A"})},
        {"A": obj(b={"$ref": "#/$defs/B"}), "B": {"type": "array", "items": {"$ref": "#/$defs/A"}}},
        {
            "A": obj(b={"anyOf": [{"$ref": "#/$defs/B"}, {"type": "null"}]}),
            "B": obj(c={"$ref": "#/$defs/C"}),
            "C": obj(a={"$ref": "#/$defs/A"}),
        },
    ],
    ids=["self", "pair", "triangle"],
)
def test_a_recursive_schema_is_refused(defs):
    schema = {**obj(root={"$ref": "#/$defs/A"}), "$defs": defs}

    with pytest.raises(RationalizationError, match="recursive"):
        make_provider().transform_schema(schema)


def test_shared_definitions_without_a_cycle_pass():
    defs = {"A": obj(b={"$ref": "#/$defs/B"}, c={"$ref": "#/$defs/C"}), "B": obj(c={"$ref": "#/$defs/C"}), "C": obj()}
    schema = {**obj(root={"$ref": "#/$defs/A"}, also={"$ref": "#/$defs/C"}), "$defs": defs}

    sent = make_provider().transform_schema(schema)

    assert set(sent["$defs"]) == {"A", "B", "C"}


def nested(levels):
    """``obj(deep=...)`` whose innermost node sits at depth ``levels + 2`` (the root 1, ``deep`` 2)."""
    node = {"type": "string"}
    for _ in range(levels):
        node = {"type": "array", "items": node}
    return {**obj(deep=node), "required": ["deep"]}


def test_nesting_one_past_the_depth_bound_is_refused():
    with pytest.raises(RationalizationError, match=f"more than {SCHEMA_MAX_DEPTH} levels"):
        make_provider().transform_schema(nested(SCHEMA_MAX_DEPTH - 1))


def test_nesting_exactly_at_the_depth_bound_passes():
    sent = make_provider().transform_schema(nested(SCHEMA_MAX_DEPTH - 2))

    assert sent["properties"]["deep"]["type"] == "array"


def required_strings(count):
    names = [f"p{index}" for index in range(count)]
    return {"type": "object", "properties": {name: {"type": "string"} for name in names}, "required": names}


def test_a_schema_one_past_the_node_bound_is_refused():
    with pytest.raises(RationalizationError, match=f"more than {SCHEMA_MAX_NODES:,} schema nodes"):
        make_provider().transform_schema(required_strings(SCHEMA_MAX_NODES))


def test_a_schema_exactly_at_the_node_bound_passes():
    sent = make_provider().transform_schema(required_strings(SCHEMA_MAX_NODES - 1))

    assert len(sent["properties"]) == SCHEMA_MAX_NODES - 1


# The documented limits: 24 optional parameters and 16 union-typed ones per request, counted per definition here.

OPTIONAL = {"type": "string"}
UNION = {"anyOf": [{"type": "string"}, {"type": "null"}]}


def fields(count, node, *, required):
    names = [f"f{index:02d}" for index in range(count)]
    schema = {"type": "object", "properties": {name: copy.deepcopy(node) for name in names}}
    if required:
        schema["required"] = names
    return schema


def test_the_limits_are_the_documented_ones():
    assert (MAX_OPTIONAL_PARAMETERS, MAX_UNION_PARAMETERS) == (24, 16)


def test_optional_parameters_at_the_limit_pass():
    sent = make_provider().transform_schema(fields(MAX_OPTIONAL_PARAMETERS, OPTIONAL, required=False))

    assert parameter_counts(sent) == (24, 0)


def test_one_optional_parameter_past_the_limit_is_refused_naming_the_counts():
    with pytest.raises(RationalizationError) as caught:
        make_provider().transform_schema(fields(MAX_OPTIONAL_PARAMETERS + 1, OPTIONAL, required=False))

    assert "25 optional parameters" in str(caught.value)
    assert "at most 24" in str(caught.value)


def test_union_parameters_at_the_limit_pass():
    sent = make_provider().transform_schema(fields(MAX_UNION_PARAMETERS, UNION, required=True))

    assert parameter_counts(sent) == (0, 16)


@pytest.mark.parametrize("node", [UNION, {"type": ["string", "null"]}], ids=["anyOf", "type-array"])
def test_one_union_parameter_past_the_limit_is_refused_naming_the_counts(node):
    with pytest.raises(RationalizationError) as caught:
        make_provider().transform_schema(fields(MAX_UNION_PARAMETERS + 1, node, required=True))

    assert "17 union-typed" in str(caught.value)
    assert "at most 16" in str(caught.value)


def test_an_optional_union_counts_toward_both_limits():
    sent = make_provider().transform_schema(fields(3, UNION, required=False))

    assert parameter_counts(sent) == (3, 3)


def test_a_definition_is_counted_once_however_often_it_is_used():
    shared = fields(13, OPTIONAL, required=False)
    schema = {
        "type": "object",
        "properties": {"a": {"$ref": "#/$defs/S"}, "b": {"$ref": "#/$defs/S"}, "c": {"$ref": "#/$defs/S"}},
        "required": ["a", "b", "c"],
        "$defs": {"S": shared},
    }

    sent = make_provider().transform_schema(schema)

    assert parameter_counts(sent) == (13, 0)


def test_counts_cover_nested_objects_and_reachable_definitions_only():
    inner = {"type": "array", "items": fields(20, OPTIONAL, required=False)}
    schema = {
        "type": "object",
        "properties": {"list": inner, "child": {"$ref": "#/$defs/Reached"}},
        "required": ["list", "child"],
        "$defs": {"Reached": fields(5, UNION, required=False), "Unreached": fields(30, UNION, required=False)},
    }

    with pytest.raises(RationalizationError) as caught:
        make_provider().transform_schema(schema)

    assert "25 optional parameters" in str(caught.value)
    assert "5 union-typed" in str(caught.value)


def test_a_definition_reached_from_two_definitions_is_counted_once():
    schema = {
        "type": "object",
        "properties": {"a": {"$ref": "#/$defs/A"}, "b": {"$ref": "#/$defs/B"}},
        "required": ["a", "b"],
        "$defs": {
            "A": {"type": "object", "properties": {"s": {"$ref": "#/$defs/S"}}, "required": ["s"]},
            "B": {"type": "object", "properties": {"s": {"$ref": "#/$defs/S"}}, "required": ["s"]},
            "S": fields(13, OPTIONAL, required=False),
        },
    }

    assert parameter_counts(make_provider().transform_schema(schema)) == (13, 0)


@pytest.mark.parametrize(("name", "ref"), [("a/b~c", "#/$defs/a~1b~0c"), ("a~1", "#/$defs/a~01")])
def test_a_definition_named_by_an_escaped_ref_is_counted(name, ref):
    schema = {**obj(child={"$ref": ref}), "required": ["child"], "$defs": {name: fields(25, OPTIONAL, required=False)}}

    with pytest.raises(RationalizationError) as caught:
        make_provider().transform_schema(schema)

    assert "25 optional parameters" in str(caught.value)
    assert parameter_counts(schema) == (25, 0)


def test_an_object_inside_an_any_of_is_counted():
    inline = {"anyOf": [fields(25, OPTIONAL, required=False), {"type": "null"}]}

    with pytest.raises(RationalizationError) as caught:
        make_provider().transform_schema({**obj(maybe=inline), "required": ["maybe"]})

    assert "25 optional parameters" in str(caught.value)
    assert "1 union-typed" in str(caught.value)


def test_a_ref_property_is_never_a_union():
    schema = {
        **fields(20, {"$ref": "#/$defs/E"}, required=True),
        "$defs": {"E": {"type": "string", "enum": ["a", "b"]}},
    }

    assert parameter_counts(make_provider().transform_schema(schema)) == (0, 0)


def test_a_schema_that_is_not_exactly_json_is_refused():
    with pytest.raises(RationalizationError):
        make_provider().transform_schema(obj(a={"enum": {1, 2}}))


# ---------------------------------------------------------------------------------------------------------------
# Claude on Google Cloud (Vertex AI / Agent Platform): AnthropicProvider.google_cloud
# ---------------------------------------------------------------------------------------------------------------
# Wire shapes from Google's pages, read on 2026-11-05: rawPredict on publishers/anthropic, Bearer token, the body's
# anthropic_version "vertex-2023-10-16" and no model, output_config.format json_schema (Claude 4.5 and later, gated by
# the constraints/vertexai.allowedPartnerModelFeatures policy), and the native Messages reply:
# https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/claude/structured-outputs
# https://docs.cloud.google.com/vertex-ai/generative-ai/docs/partner-models/claude/use-claude

TOKEN = "ya29.hunter2-TOKEN"
GC_MODEL = "claude-opus-4-7"
REGIONAL_MODEL = "claude-sonnet-4-6"
PROJECT = "my-project"
# Anthropic: "Regional endpoints serve Claude Sonnet 4.6 and earlier models only"; newer models need global, us or eu.
NEWER_MODEL_LOCATIONS = ("global", "us", "eu")


def model_for(location):
    """A Claude model the location serves: a newer one on global, us and eu; Sonnet 4.6 on a regional endpoint."""
    return GC_MODEL if location in NEWER_MODEL_LOCATIONS else REGIONAL_MODEL


class TokenSource:
    """A token source counting its calls; raises ``value`` when it is an exception."""

    def __init__(self, value=TOKEN, events=None):
        self.value = value
        self.calls = 0
        self.events = events

    def __call__(self):
        self.calls += 1
        if self.events is not None:
            self.events.append("token")
        if isinstance(self.value, BaseException):
            raise self.value
        return self.value


def gc_provider(handler=None, *, location="global", source=None, **kwargs):
    auth = GoogleCloudAuth(token=source or TokenSource())
    transport = httpx.MockTransport(handler or reply_with(json_body=MESSAGE))
    model = kwargs.pop("model", model_for(location))
    return AnthropicProvider.google_cloud(
        model, project=PROJECT, location=location, auth=auth, transport=transport, **kwargs
    )


@pytest.mark.parametrize(
    ("location", "host"),
    [
        ("us-east5", "us-east5-aiplatform.googleapis.com"),
        ("europe-west1", "europe-west1-aiplatform.googleapis.com"),
        ("global", "aiplatform.googleapis.com"),
        ("us", "aiplatform.us.rep.googleapis.com"),
        ("eu", "aiplatform.eu.rep.googleapis.com"),
    ],
)
def test_the_google_cloud_request_is_the_documented_raw_predict_request(location, host):
    handler = reply_with(json_body=MESSAGE)

    gc_provider(handler, location=location).generate(REQUEST)

    (sent,) = handler.seen
    assert sent.method == "POST"
    assert str(sent.url) == (
        f"https://{host}/v1/projects/my-project/locations/{location}/publishers/anthropic/models/"
        f"{model_for(location)}:rawPredict"
    )
    assert json.loads(sent.content) == {
        "anthropic_version": "vertex-2023-10-16",
        "max_tokens": 1024,
        "system": "SYSTEM RULES",
        "messages": [{"role": "user", "content": "<document_payload_ab>PN-1</document_payload_ab>"}],
        "output_config": {"format": {"type": "json_schema", "schema": SENT_SCHEMA}},
    }
    assert ANTHROPIC_VERSION == "vertex-2023-10-16"


def test_the_google_cloud_headers_carry_the_bearer_token_and_no_anthropic_header():
    handler = reply_with(json_body=MESSAGE)

    gc_provider(handler).generate(REQUEST)

    headers = handler.seen[0].headers
    assert headers["authorization"] == f"Bearer {TOKEN}"
    assert headers["content-type"] == "application/json"
    for absent in ("x-api-key", "anthropic-version", "anthropic-beta", "x-goog-api-key"):
        assert absent not in headers
    assert TOKEN not in str(handler.seen[0].url)
    assert TOKEN.encode() not in handler.seen[0].content


def test_the_google_cloud_body_sends_a_temperature_only_when_set():
    handler = reply_with(json_body=MESSAGE)

    gc_provider(handler, temperature=0.5).generate(REQUEST)
    gc_provider(handler).generate(REQUEST)

    assert json.loads(handler.seen[0].content)["temperature"] == 0.5
    assert "temperature" not in json.loads(handler.seen[1].content)
    assert all("model" not in json.loads(request.content) for request in handler.seen)


def test_the_token_is_fetched_once_per_request_and_never_at_construction():
    source = TokenSource()
    provider = gc_provider(source=source)

    assert source.calls == 0
    provider.generate(REQUEST)
    provider.generate(REQUEST)
    assert source.calls == 2


def test_the_token_is_fetched_after_the_deadline_is_set(monkeypatch):
    events = []
    provider = gc_provider(source=TokenSource(events=events))
    real = time.monotonic

    def clock():
        events.append("clock")
        return real()

    monkeypatch.setattr(time, "monotonic", clock)
    provider.generate(REQUEST)

    assert "token" in events
    assert events.index("clock") < events.index("token")


@pytest.mark.parametrize(
    ("value", "named"),
    [
        (RuntimeError(f"boom {TOKEN}"), "RuntimeError"),
        (5, "int"),
        (f"Bearer {TOKEN}", "Bearer"),
        ("", "no valid token"),
        ("ya29 hunter2 spaced", "no valid token"),
        (None, "NoneType"),
    ],
    ids=["raises", "int", "bearer", "empty", "whitespace", "none"],
)
def test_a_token_failure_is_permanent_without_its_message_and_sends_nothing(value, named):
    handler = reply_with(json_body=MESSAGE)

    with pytest.raises(PermanentProviderError) as caught:
        gc_provider(handler, source=TokenSource(value)).generate(REQUEST)

    assert caught.value.status_code is None
    assert "token provider failed" in caught.value.detail
    assert named in caught.value.detail
    assert "boom" not in caught.value.detail
    assert handler.seen == []
    assert_secret_free(caught.value)


def test_a_google_cloud_401_echoing_the_token_is_permanent_and_redacted():
    handler = reply_with(401, json_body={"error": {"code": 401, "message": f"bad credentials Bearer {TOKEN}"}})

    with pytest.raises(PermanentProviderError) as caught:
        gc_provider(handler).generate(REQUEST)

    assert caught.value.status_code == 401
    assert "bad credentials" in caught.value.detail
    assert_secret_free(caught.value)


def build_gc(**options):
    """``AnthropicProvider.google_cloud`` with the given options over the defaults (a token auth, global)."""
    arguments = {
        "model": GC_MODEL,
        "project": PROJECT,
        "location": "global",
        "auth": GoogleCloudAuth(token=TokenSource()),
        "transport": httpx.MockTransport(reply_with(json_body=MESSAGE)),
    }
    arguments.update(options)
    return AnthropicProvider.google_cloud(arguments.pop("model"), **arguments)


@pytest.mark.parametrize(
    ("options", "phrase"),
    [
        ({"auth": GoogleCloudAuth(api_key="AIza-hunter2-KEY")}, "not rawPredict"),
        ({"project": None, "location": "global"}, "express mode"),
        ({"project": None, "location": "us-east5"}, "express mode"),
        ({"auth": "AIza-hunter2-raw-KEY"}, "auth must be a GoogleCloudAuth, got str"),
        ({"auth": None}, "auth must be a GoogleCloudAuth, got NoneType"),
        ({"auth": TokenSource()}, "auth must be a GoogleCloudAuth, got TokenSource"),
        ({"project": "Bad_Project"}, "project"),
        ({"project": 7}, "project"),
        ({"location": "US-East5"}, "location"),
        ({"model": "claude opus"}, "path segment"),
        ({"model": "../claude"}, "path segment"),
        ({"model": "anthropic/claude"}, "path segment"),
        ({"model": ""}, "path segment"),
        ({"temperature": 2}, "temperature must be None or a finite number in 0-1"),
        ({"temperature": True}, "temperature must be None or a number"),
        ({"max_connections": 0}, "max_connections must be an int in 1-1024"),
        ({"transport": object()}, "transport"),
    ],
)
def test_a_bad_google_cloud_option_is_refused_at_construction_holding_no_secret(options, phrase):
    with pytest.raises(RationalizationError) as caught:
        build_gc(**options)

    assert phrase in str(caught.value)
    assert_secret_free(caught.value)


def test_a_google_cloud_model_id_with_an_at_version_reaches_the_wire():
    handler = reply_with(json_body=MESSAGE)
    provider = gc_provider(handler, model="claude-opus-4-5@20251101")

    provider.generate(REQUEST)

    assert str(handler.seen[0].url) == (
        "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/publishers/anthropic/"
        "models/claude-opus-4-5@20251101:rawPredict"
    )
    assert "model" not in json.loads(handler.seen[0].content)
    assert provider.model_id == (
        "google-cloud:projects/my-project/locations/global/publishers/anthropic/models/claude-opus-4-5@20251101"
    )
    assert provider.model_id != gc_provider(model="claude-opus-4-5").model_id


@pytest.mark.parametrize("model", ["claude@1@2", "claude@", "@20251101", "claude@..", "claude@v 1"])
def test_a_malformed_at_version_is_refused_on_google_cloud(model):
    with pytest.raises(RationalizationError, match="path segment") as caught:
        build_gc(model=model)

    assert_secret_free(caught.value)


def test_the_direct_route_refuses_a_google_cloud_at_version_model():
    with pytest.raises(RationalizationError, match="@") as caught:
        make_provider(model="claude-opus-4-5@20251101")

    assert_secret_free(caught.value)


def test_the_google_cloud_identity():
    provider = gc_provider()
    target = GoogleCloudTarget(PROJECT, "global")

    assert provider.model_id == (
        "google-cloud:projects/my-project/locations/global/publishers/anthropic/models/claude-opus-4-7"
    )
    assert provider.model_id == model_id_for(target, "anthropic", GC_MODEL)
    assert provider.provider_kind == "google-cloud-anthropic"
    assert provider.rate_key == provider.model_id  # Google Cloud quotas are per model: the resource name
    regional = gc_provider(location="us-east5")
    assert regional.model_id == (
        "google-cloud:projects/my-project/locations/us-east5/publishers/anthropic/models/claude-sonnet-4-6"
    )
    assert regional.rate_key == regional.model_id
    assert provider.rate_key != rate_key_for(target)
    assert provider.model_family == "anthropic" == family_for_publisher("anthropic")
    assert provider.max_connections == 8
    assert canonical_json(provider.generation_params) == (
        '{"anthropic_version":"vertex-2023-10-16","mechanism":"json_schema","wire":"messages/1"}'
    )
    assert canonical_json(gc_provider(temperature=0).generation_params) == (
        '{"anthropic_version":"vertex-2023-10-16","mechanism":"json_schema","temperature":0.0,"wire":"messages/1"}'
    )
    assert check_provider(provider) is provider


def test_the_google_cloud_identity_differs_from_the_direct_provider_for_the_same_model():
    cloud = gc_provider()
    direct = make_provider(model=GC_MODEL)

    assert cloud.model_id != direct.model_id
    assert cloud.provider_kind != direct.provider_kind
    assert cloud.rate_key != direct.rate_key
    assert cloud.generation_params != direct.generation_params
    assert cloud.model_family == direct.model_family


def test_two_models_in_one_project_and_location_never_share_a_rate_key():
    opus, sonnet = gc_provider(model="claude-opus-4-7"), gc_provider(model="claude-sonnet-4-6")
    versioned = gc_provider(model="claude-opus-4-5@20251101")

    assert len({opus.rate_key, sonnet.rate_key, versioned.rate_key}) == 3
    assert opus.rate_key == gc_provider(model="claude-opus-4-7").rate_key


@pytest.mark.parametrize(("first", "second"), [("us-east5", "europe-west1"), ("us", "eu"), ("global", "us")])
def test_two_locations_never_share_an_identity(first, second):
    one, other = gc_provider(location=first), gc_provider(location=second)

    assert one.model_id != other.model_id
    assert one.rate_key != other.rate_key


def test_the_google_cloud_repr_and_pickle_hold_no_token():
    provider = gc_provider()

    assert "hunter2" not in repr(provider)
    assert "google-cloud-anthropic" in repr(provider)
    with pytest.raises(TypeError):
        pickle.dumps(provider)


def test_the_google_cloud_provider_opens_its_clients(monkeypatch):
    opened = []
    monkeypatch.setattr(HttpClient, "open", lambda self: opened.append(self))

    assert gc_provider().open() is None
    assert len(opened) == 1


def test_both_routes_share_the_transform():
    invoice, _, _ = REALISTIC["invoice"]
    neutral = response_schema(invoice)

    assert gc_provider().transform_schema(neutral) == make_provider().transform_schema(neutral)


ROUTES = pytest.mark.parametrize("build", [make_provider, gc_provider], ids=["direct", "google-cloud"])
PROSE = [{"type": "text", "text": "I can't help with that."}]
THINKING_ONLY = [{"type": "thinking", "thinking": "", "signature": "s"}]
CACHED = {"input_tokens": 10, "output_tokens": 5, "cache_creation_input_tokens": 3, "cache_read_input_tokens": True}


@ROUTES
@pytest.mark.parametrize(
    ("reply", "finish", "content", "tokens"),
    [
        (MESSAGE, "complete", ANSWER, (2095, 503)),
        (message_with(stop_reason="max_tokens"), "truncated", ANSWER, (2095, 503)),
        (message_with(stop_reason="model_context_window_exceeded"), "truncated", ANSWER, (2095, 503)),
        (message_with(stop_reason="refusal", content=PROSE), "refused", None, (2095, 503)),
        (message_with(stop_reason="brand_new"), "complete", None, (2095, 503)),
        (message_with(content=THINKING_ONLY), "complete", None, (2095, 503)),
        (message_with(content=[{"type": "text", "text": "```json\n{}\n```"}]), "complete", None, (2095, 503)),
        (message_with(usage=CACHED), "complete", ANSWER, (13, 5)),
        (message_with(content="{}"), "complete", None, (2095, 503)),
    ],
    ids=["complete", "max-tokens", "context", "refusal", "unknown-stop", "no-text", "fence", "usage", "bad-shape"],
)
def test_both_routes_map_a_reply_alike(build, reply, finish, content, tokens):
    response = build(reply_with(json_body=reply)).generate(REQUEST)

    assert (response.finish, response.content, (response.input_tokens, response.output_tokens)) == (
        finish,
        content,
        tokens,
    )


@ROUTES
@pytest.mark.parametrize(
    ("status", "error"),
    [
        (400, PermanentProviderError),
        (401, PermanentProviderError),
        (403, PermanentProviderError),
        (429, TransientProviderError),
        (500, TransientProviderError),
        (503, TransientProviderError),
        (529, TransientProviderError),
    ],
)
def test_both_routes_raise_http_failures_alike(build, status, error):
    with pytest.raises(error) as caught:
        build(reply_with(status, json_body=error_body("api_error", "no"))).generate(REQUEST)

    assert type(caught.value) is error
    assert caught.value.status_code == status


# ---------------------------------------------------------------------------------------------------------------
# The module
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_the_package_and_nothing_inside_functions():
    tree = ast.parse(Path(anthropic_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    runtime = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    modules = [node.module or "" for node in runtime if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in runtime if isinstance(node, ast.Import) for alias in node.names]
    foreign = [
        m
        for m in modules
        if not m.startswith("datagrunt.rationalization.") and m.split(".")[0] not in sys.stdlib_module_names
    ]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert foreign == []
    assert not any(m.startswith("datagrunt.core") for m in modules)


MM_ANTHROPIC_PNG = b"\x89PNG\r\n\x1a\nCLAUDE_BAND"


class TestMultimodalImages:
    def test_anthropic_provider_sends_base64_image_blocks_before_text(self):
        import base64
        import dataclasses

        handler = reply_with(json_body=MESSAGE)
        provider = make_provider(handler)
        req = dataclasses.replace(REQUEST, images=(MM_ANTHROPIC_PNG,))

        provider.generate(req)

        body = json.loads(handler.seen[0].content)
        assert body["messages"] == [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": base64.b64encode(MM_ANTHROPIC_PNG).decode("ascii"),
                        },
                    },
                    {"type": "text", "text": REQUEST.user_envelope},
                ],
            }
        ]
