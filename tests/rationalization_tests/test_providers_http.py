"""Tests for ``datagrunt.rationalization.providers.http``: the bounded HTTP helper the wire adapters share (plan §4.5).

``HttpClient`` posts JSON over two lazily created ``httpx.Client``s (built eagerly by ``open()``): one for loopback
hosts, which never uses an environment proxy, and one for every other host, which trusts the environment. No redirect
is followed, keep-alive is off, and a finalizer releases both clients. Each reply is read raw, capped at 8 MiB, with
at most one gzip or deflate layer decoded under that cap. 2xx is a reply; 3xx, and 4xx/5xx other than 408, 429, 500,
502, 503, 504, 520-524 (Cloudflare) and 529 (overloaded), are permanent errors; those twelve, timeouts and transport
failures are transient (named from a fixed vocabulary), with the wait from ``Retry-After`` or
``x-ratelimit-reset-requests``. Keys travel as ``_Secret`` values
and appear in no exception, repr, traceback local or log. These tests run over ``httpx.MockTransport``; the
real-socket tests are in ``test_providers_http_sockets.py``.
"""

import ast
import email.utils
import gc
import gzip
import json
import logging
import pickle
import socket
import ssl
import subprocess
import sys
import threading
import time
import traceback
import tracemalloc
import zlib
from pathlib import Path

import h11
import httpx
import pytest

import datagrunt
from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization.providers import http as http_module
from datagrunt.rationalization.providers.base import (
    DETAIL_CUT_MARKER,
    DETAIL_MAX_CHARS,
    PermanentProviderError,
    ProviderError,
    TransientProviderError,
    _Secret,
)
from datagrunt.rationalization.providers.http import (
    CONNECT_TIMEOUT_S,
    FRAMING_HEADERS,
    KEY_HEADERS,
    POOL_TIMEOUT_S,
    RESPONSE_MAX_BYTES,
    TRANSIENT_STATUSES,
    USER_AGENT,
    WRITE_TIMEOUT_S,
    HttpClient,
    HttpReply,
    parse_reset_duration,
    parse_retry_after,
    retry_after_seconds,
)

TIMEOUT_S = 120
MIB = 1024 * 1024
URL = "https://api.example.com/v1/chat/completions"
BODY = {"model": "m", "messages": [{"role": "user", "content": "héllo"}]}
SECRETS = {
    "Authorization": _Secret("sk-hunter2-AUTH").prefixed("Bearer "),
    "x-api-key": _Secret("sk-hunter2-ANTHROPIC"),
    "x-goog-api-key": _Secret("AIza-hunter2-GOOGLE"),
}
SECRET_VALUES = ("sk-hunter2-AUTH", "sk-hunter2-ANTHROPIC", "AIza-hunter2-GOOGLE", "hunter2")
ENVIRONMENT = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "SSLKEYLOGFILE",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """No proxy or certificate variable from the developer's shell reaches a test."""
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


def streamed(status, *, json_body=None, content=b"", headers=None):
    """A streaming ``httpx.Response``, as from the network (a ``content=`` bytes response is read when built)."""
    headers = dict(headers or {})
    if json_body is not None:
        content = json.dumps(json_body).encode("utf-8")
        headers.setdefault("content-type", "application/json")
    if isinstance(content, bytes):
        return httpx.Response(status, headers=headers, stream=httpx.ByteStream(content))
    return httpx.Response(status, headers=headers, content=content)


def reply_with(status, **kwargs):
    """A handler answering ``streamed(status, **kwargs)``, recording each request it gets."""
    seen = []

    def handler(request):
        seen.append(request)
        return streamed(status, **kwargs)

    handler.seen = seen
    return handler


def make_client(handler, **kwargs):
    """An ``HttpClient`` over a mock transport calling ``handler``."""
    return HttpClient(transport=httpx.MockTransport(handler), **kwargs)


def post(client, url=URL, headers=None, body=None, timeout_s=30.0, deadline=None):
    return client.post_json(
        url,
        headers=headers or {},
        body=BODY if body is None else body,
        timeout_s=timeout_s,
        deadline=time.monotonic() + 60 if deadline is None else deadline,
    )


def traceback_text(error):
    """The error's traceback with the locals of datagrunt's frames, as a log with ``capture_locals`` prints them.

    The test's own frames are left out: their locals hold the test's fixtures, keys included.
    """
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def assert_no_secret(error):
    """No secret value in the error's text, repr, args, detail, notes, pickle or traceback locals; no chain."""
    texts = [str(error), repr(error), repr(error.args), error.detail, repr(getattr(error, "__notes__", None))]
    texts += [pickle.dumps(error).decode("latin-1"), traceback_text(error)]
    for text in texts:
        for secret in SECRET_VALUES:
            assert secret not in text
    assert error.__cause__ is None
    assert error.__context__ is None


def failing_with(error_type, *causes):
    """A handler raising ``error_type`` whose cause chain is ``causes`` (outermost first), as httpcore builds it."""

    def handler(request):
        error = error_type(f"boom at {request.url} with {request.headers.get('authorization')}", request=request)
        inner = error
        for cause in causes:
            inner.__cause__ = cause
            inner = cause
        raise error

    return handler


@pytest.fixture
def created(monkeypatch):
    """Every ``httpx.Client`` the module creates, with the keyword arguments it got."""
    made = []
    real = httpx.Client

    class Recording(real):
        def __init__(self, **kwargs):
            made.append((self, kwargs))
            super().__init__(**kwargs)

    monkeypatch.setattr(http_module.httpx, "Client", Recording)
    return made


# ---------------------------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------------------------


def test_the_bounds():
    assert RESPONSE_MAX_BYTES == 8 * MIB
    assert TRANSIENT_STATUSES == frozenset({408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 529})
    assert (CONNECT_TIMEOUT_S, WRITE_TIMEOUT_S, POOL_TIMEOUT_S) == (10.0, 15.0, 10.0)
    assert {"authorization", "x-api-key", "x-goog-api-key", "api-key"} <= KEY_HEADERS
    assert {"host", "content-length", "transfer-encoding", "connection", "upgrade", "te", "trailer"} <= FRAMING_HEADERS
    assert {"keep-alive", "expect"} <= FRAMING_HEADERS
    assert USER_AGENT == f"datagrunt/{datagrunt.__version__}"


# ---------------------------------------------------------------------------------------------------------------
# 2xx: the reply
# ---------------------------------------------------------------------------------------------------------------


def test_a_json_object_reply_is_parsed():
    reply = post(make_client(reply_with(200, json_body={"id": "x", "choices": []})))

    assert isinstance(reply, HttpReply)
    assert reply.status == 200
    assert reply.json == {"id": "x", "choices": []}
    assert json.loads(reply.text) == {"id": "x", "choices": []}
    assert reply.retry_after_s is None


@pytest.mark.parametrize("status", [200, 201, 204, 299])
def test_every_2xx_is_a_reply(status):
    reply = post(make_client(reply_with(status)))

    assert reply.status == status
    assert reply.json is None
    assert reply.text == ""


@pytest.mark.parametrize(
    "content",
    [
        b"[1, 2]",
        b'"text"',
        b"42",
        b"null",
        b"not json",
        b'{"a": NaN}',
        b'{"a": Infinity}',
        b'{"a": -Infinity}',
        b'{"a": 1e999}',
        b'{"a": -1e400}',
        b'{"a": 1, "a": 2}',
        b'{"a": 1',
    ],
    ids=[
        "array",
        "string",
        "number",
        "null",
        "text",
        "nan",
        "infinity",
        "minus-infinity",
        "overflow",
        "minus-overflow",
        "duplicate-key",
        "truncated",
    ],
)
def test_a_body_that_is_not_a_strict_finite_json_object_has_no_json(content):
    reply = post(make_client(reply_with(200, content=content)))

    assert reply.json is None
    assert reply.text == content.decode()


def test_invalid_utf8_has_no_json_and_its_text_is_replaced():
    reply = post(make_client(reply_with(200, content=b'{"a": "\xff"}')))

    assert reply.json is None
    assert reply.text == '{"a": "\ufffd"}'


def test_a_nested_or_huge_number_body_has_no_json():
    deep = b"[" * 100_000 + b"]" * 100_000
    huge = b'{"n": ' + b"9" * 10_000 + b"}"

    assert post(make_client(reply_with(200, content=deep))).json is None
    assert post(make_client(reply_with(200, content=huge))).json is None


def test_a_reply_repr_shows_neither_the_body_nor_its_text():
    reply = post(make_client(reply_with(200, json_body={"secret": "document text"})))

    assert "document text" not in repr(reply)


# ---------------------------------------------------------------------------------------------------------------
# The request
# ---------------------------------------------------------------------------------------------------------------


def test_the_request_is_a_json_post_with_the_callers_headers():
    handler = reply_with(200, json_body={})

    post(make_client(handler), headers={"anthropic-version": "2023-06-01", **SECRETS})

    (request,) = handler.seen
    assert request.method == "POST"
    assert str(request.url) == URL
    assert request.content == json.dumps(BODY, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    assert "héllo".encode() in request.content
    assert request.headers["content-type"] == "application/json"
    assert request.headers["accept"] == "application/json"
    assert request.headers["accept-encoding"] == "identity"
    assert request.headers["user-agent"] == USER_AGENT
    assert request.headers["anthropic-version"] == "2023-06-01"
    assert request.headers["authorization"] == "Bearer sk-hunter2-AUTH"
    assert request.headers["x-api-key"] == "sk-hunter2-ANTHROPIC"
    assert "trace" in request.extensions


def test_the_normalized_url_is_what_is_sent():
    handler = reply_with(200, json_body={})

    post(make_client(handler), url="HTTPS://API.Example.COM:443/v1/x/../chat/./completions/")

    assert str(handler.seen[0].url) == URL


def test_the_timeouts_are_connect_10_read_timeout_write_15_pool_10_when_the_deadline_is_far():
    handler = reply_with(200, json_body={})

    post(make_client(handler), timeout_s=42.5)

    assert handler.seen[0].extensions["timeout"] == {"connect": 10.0, "read": 42.5, "write": 15.0, "pool": 10.0}


def test_no_phase_timeout_outlasts_a_near_deadline():
    handler = reply_with(200, json_body={})

    post(make_client(handler), timeout_s=42.5, deadline=time.monotonic() + 2.0)

    timeouts = handler.seen[0].extensions["timeout"]
    assert set(timeouts) == {"connect", "read", "write", "pool"}
    assert all(0 < value <= 2.0 for value in timeouts.values())


@pytest.mark.parametrize("timeout_s", [0, -1, float("nan"), float("inf")])
def test_a_bad_timeout_is_refused(timeout_s):
    handler = reply_with(200, json_body={})

    with pytest.raises(ValueError, match="timeout_s"):
        post(make_client(handler), timeout_s=timeout_s)
    assert handler.seen == []


@pytest.mark.parametrize("timeout_s", [True, "30", None])
def test_a_timeout_of_the_wrong_type_is_refused(timeout_s):
    with pytest.raises(TypeError, match="timeout_s"):
        post(make_client(reply_with(200)), timeout_s=timeout_s)


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_deadline_is_refused(deadline):
    with pytest.raises(ValueError, match="deadline"):
        post(make_client(reply_with(200)), deadline=deadline)


@pytest.mark.parametrize("deadline", [True, "1", [1]])
def test_a_deadline_of_the_wrong_type_is_refused(deadline):
    with pytest.raises(TypeError, match="deadline"):
        post(make_client(reply_with(200)), deadline=deadline)


def test_a_deadline_more_than_a_day_away_is_a_caller_bug():
    handler = reply_with(200)

    with pytest.raises(ValueError, match="day"):
        post(make_client(handler), deadline=time.monotonic() + 86_401)
    with pytest.raises(ValueError, match="day"):
        post(make_client(handler), deadline=time.time())  # a wall-clock time passed by mistake
    assert handler.seen == []


def test_a_timer_that_cannot_start_leaves_the_request_unsent_and_transient(monkeypatch):
    handler = reply_with(200, json_body={})

    def refuse(self):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Timer, "start", refuse)
    with pytest.raises(TransientProviderError, match="could not be bounded"):
        post(make_client(handler))
    assert handler.seen == []


def test_a_deadline_already_past_sends_nothing_and_is_transient():
    handler = reply_with(200, json_body={})

    with pytest.raises(TransientProviderError, match="deadline passed"):
        post(make_client(handler), deadline=time.monotonic() - 1)
    assert handler.seen == []


@pytest.mark.parametrize("body", [[1, 2], "text", None])
def test_a_body_that_is_not_a_mapping_is_refused(body):
    with pytest.raises(TypeError, match="body"):
        make_client(reply_with(200)).post_json(URL, headers={}, body=body, timeout_s=1.0, deadline=time.monotonic() + 9)


@pytest.mark.parametrize("body", [{"a": float("nan")}, {"a": {1, 2}}, {"a": "\ud800"}])
def test_a_body_that_is_not_json_is_refused_before_sending(body):
    handler = reply_with(200)

    with pytest.raises(ValueError, match="body"):
        post(make_client(handler), body=body)
    assert handler.seen == []


@pytest.mark.parametrize("headers", [[("a", "b")], {"a": 1}, {1: "a"}])
def test_headers_must_be_a_str_mapping(headers):
    with pytest.raises(TypeError, match="header"):
        post(make_client(reply_with(200)), headers=headers)


@pytest.mark.parametrize(
    "url",
    [
        "http://10.0.0.5/v1/chat/completions",
        "https://user:pw@api.example.com/v1",
        "https://api.example.com/v1?key=x",
        "ftp://x",
        "https://999.1.1.1/v1",
        "https://xn--zz/v1",
    ],
)
def test_a_refused_url_is_a_permanent_error_and_nothing_is_sent(url):
    handler = reply_with(200)

    with pytest.raises(PermanentProviderError, match="endpoint URL refused"):
        post(make_client(handler), url=url)
    assert handler.seen == []


def test_insecure_http_is_sent_only_when_allowed():
    handler = reply_with(200, json_body={})

    reply = post(make_client(handler, allow_insecure_http=True), url="http://10.0.0.5/v1/chat/completions")

    assert reply.status == 200
    assert str(handler.seen[0].url) == "http://10.0.0.5/v1/chat/completions"


def test_loopback_http_is_sent():
    handler = reply_with(200, json_body={})

    post(make_client(handler), url="http://localhost:11434/v1/chat/completions")

    assert str(handler.seen[0].url) == "http://localhost:11434/v1/chat/completions"


# ---------------------------------------------------------------------------------------------------------------
# Headers: no injection, no framing, keys only as _Secret
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "hunter2\r\nX-Evil: 1",
        "hunter2\nb",
        "hunter2\rb",
        "hunter2\x00b",
        "hunter2é",
        "hunter2\u2028",
        " hunter2",
        "hunter2 ",
        "\thunter2",
        "hunter2\t",
    ],
)
def test_a_header_value_that_could_inject_or_break_framing_is_refused_before_sending(value):
    handler = reply_with(200)

    with pytest.raises(PermanentProviderError, match="anthropic-version") as caught:
        post(make_client(handler), headers={"anthropic-version": value})

    assert handler.seen == []
    assert "hunter2" not in str(caught.value)
    assert caught.value.status_code is None


@pytest.mark.parametrize("value", ["", "a\tb", "a b"])
def test_an_empty_or_inner_whitespace_header_value_is_sent(value):
    handler = reply_with(200, json_body={})

    post(make_client(handler), headers={"x-a": value})

    assert handler.seen[0].headers["x-a"] == value


@pytest.mark.parametrize("name", ["bad header", "x-a\r\nb", "", "é"])
def test_a_header_name_that_is_not_a_token_is_refused(name):
    handler = reply_with(200)

    with pytest.raises(PermanentProviderError, match="header name"):
        post(make_client(handler), headers={name: "v"})
    assert handler.seen == []


@pytest.mark.parametrize(
    "name",
    [
        "Host",
        "Content-Length",
        "Transfer-Encoding",
        "Connection",
        "Upgrade",
        "TE",
        "Trailer",
        "Keep-Alive",
        "Expect",
        "Proxy-Authorization",
        "Proxy-Connection",
        "proxy-anything",
    ],
)
def test_a_framing_or_routing_header_is_refused(name):
    handler = reply_with(200)

    with pytest.raises(PermanentProviderError, match=name):
        post(make_client(handler), headers={name: "x"})
    assert handler.seen == []


@pytest.mark.parametrize("name", ["Authorization", "x-api-key", "X-Goog-Api-Key", "api-key"])
def test_a_key_header_with_a_plain_str_value_is_refused(name):
    handler = reply_with(200)

    with pytest.raises(TypeError, match="_Secret") as caught:
        post(make_client(handler), headers={name: "sk-hunter2"})

    assert "hunter2" not in str(caught.value)
    assert handler.seen == []


def test_a_header_given_twice_is_refused_before_sending():
    handler = reply_with(200)

    with pytest.raises(ValueError, match="twice"):
        post(make_client(handler), headers={"x-a": "1", "X-A": "2"})
    assert handler.seen == []


def test_a_callers_header_replaces_a_default_of_any_case():
    handler = reply_with(200, json_body={})

    post(make_client(handler), headers={"Accept": "application/x-ndjson"})

    assert handler.seen[0].headers.get_list("accept") == ["application/x-ndjson"]


# ---------------------------------------------------------------------------------------------------------------
# Secrets: redacted by type, never in a traceback local, an exception, a repr or a log
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 401, 403, 429, 500])
def test_an_error_body_echoing_a_key_is_redacted(status):
    body = {"error": {"message": "Incorrect API key provided: sk-hunter2-AUTH and AIza-hunter2-GOOGLE"}}

    with pytest.raises((TransientProviderError, PermanentProviderError)) as caught:
        post(make_client(reply_with(status, json_body=body)), headers=SECRETS)

    assert_no_secret(caught.value)
    assert "<secret>" in caught.value.detail


@pytest.mark.parametrize("name", ["api-key", "x-custom-token", "x-api-key"])
def test_a_secret_under_any_header_name_is_redacted(name):
    body = {"error": {"message": "bad token sk-hunter2-ANYNAME"}}

    with pytest.raises(PermanentProviderError) as caught:
        post(make_client(reply_with(401, json_body=body)), headers={name: _Secret("sk-hunter2-ANYNAME")})

    assert "hunter2" not in caught.value.detail
    assert "<secret>" in caught.value.detail


@pytest.mark.parametrize(
    "handler",
    [
        reply_with(401, json_body={"error": {"message": "bad key sk-hunter2-AUTH"}}),
        reply_with(500, json_body={"error": {"message": "oops"}}),
        reply_with(302, headers={"location": "https://evil.example/"}),
        reply_with(200, content=b"sk-hunter2-AUTH " * (RESPONSE_MAX_BYTES // 16 + 1)),
        failing_with(httpx.ConnectError, ConnectionRefusedError(61, "refused")),
        failing_with(httpx.ReadTimeout),
        failing_with(httpx.ConnectError, ssl.SSLCertVerificationError(1, "certificate verify failed")),
    ],
    ids=["401", "500", "302", "cap", "connect-error", "timeout", "tls"],
)
def test_no_error_path_carries_the_key_in_its_text_chain_or_traceback_locals(handler):
    with pytest.raises(ProviderError) as caught:
        post(make_client(handler), headers=SECRETS)

    assert_no_secret(caught.value)


def test_the_client_repr_shows_no_header():
    client = make_client(reply_with(200, json_body={}))
    post(client, headers=SECRETS)

    for secret in SECRET_VALUES:
        assert secret not in repr(client)


def test_no_secret_reaches_the_httpx_or_httpcore_logs_over_a_mock_transport(caplog):
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    post(make_client(reply_with(200, json_body={})), headers=SECRETS)
    with pytest.raises(PermanentProviderError):
        post(make_client(reply_with(401, json_body={"error": {"message": "bad"}})), headers=SECRETS)

    assert caplog.records, "vacuous: httpx logged nothing"
    for secret in SECRET_VALUES:
        assert secret not in caplog.text


# ---------------------------------------------------------------------------------------------------------------
# 3xx: never followed
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("status", [300, 301, 302, 303, 307, 308])
def test_a_redirect_is_permanent_and_never_followed(status):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return streamed(status, headers={"location": "https://elsewhere.example/v1"}, content=b"moved")

    with pytest.raises(PermanentProviderError, match="redirects are not followed") as caught:
        post(make_client(handler))

    assert caught.value.status_code == status
    assert seen == [URL]


# ---------------------------------------------------------------------------------------------------------------
# 4xx / 5xx: transient or permanent
# ---------------------------------------------------------------------------------------------------------------


RETRYABLE_STATUSES = [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 529]
"""Timeout, rate limit, server errors, Cloudflare's origin errors (520-524) and Anthropic's overloaded (529)."""


@pytest.mark.parametrize("status", RETRYABLE_STATUSES)
def test_each_retryable_status_is_transient_with_its_wait(status):
    reply = reply_with(status, headers={"retry-after": "3"}, json_body={"error": {"message": "try later"}})

    with pytest.raises(TransientProviderError) as caught:
        post(make_client(reply))

    assert caught.value.status_code == status
    assert caught.value.retry_after_s == 3.0
    assert caught.value.detail == "try later"


@pytest.mark.parametrize(
    "status",
    [400, 401, 402, 403, 404, 409, 413, 418, 422, 451, 499, 501, 505, 506, 507, 508, 510, 511, 519, 525, 526, 527]
    + [528, 530, 598, 599, 199],
)
def test_every_other_status_is_permanent(status):
    with pytest.raises(PermanentProviderError) as caught:
        post(make_client(reply_with(status, json_body={"error": {"message": "no"}})))

    assert caught.value.status_code == status
    assert caught.value.detail == "no"


@pytest.mark.parametrize(
    ("content", "detail"),
    [
        (b'{"error": {"message": "openai style", "type": "invalid_request_error"}}', "openai style"),
        (b'{"type": "error", "error": {"type": "x", "message": "anthropic style"}}', "anthropic style"),
        (b'{"error": {"code": 400, "message": "gemini style", "status": "INVALID_ARGUMENT"}}', "gemini style"),
        (b'{"error": "ollama style"}', "ollama style"),
        (b'{"message": "plain message"}', "plain message"),
        (b'{"error": {"code": 400}}', '{"error": {"code": 400}}'),
        (b'{"detail": "other"}', '{"detail": "other"}'),
        (b"<html>\n<body>Bad Gateway</body>\n</html>", "<html> <body>Bad Gateway</body> </html>"),
        (b"", ""),
    ],
)
def test_the_detail_comes_from_the_error_message_else_the_text(content, detail):
    with pytest.raises(PermanentProviderError) as caught:
        post(make_client(reply_with(400, content=content)))

    assert caught.value.detail == detail


def test_a_long_or_multiline_error_body_is_bounded():
    message = "line\n" * 200
    with pytest.raises(PermanentProviderError) as caught:
        post(make_client(reply_with(400, json_body={"error": {"message": message}})))

    assert len(caught.value.detail) == DETAIL_MAX_CHARS
    assert caught.value.detail.endswith(DETAIL_CUT_MARKER)
    assert "\n" not in caught.value.detail


# ---------------------------------------------------------------------------------------------------------------
# Retry-After and x-ratelimit-reset-requests
# ---------------------------------------------------------------------------------------------------------------

NOW = 1_800_000_000.0


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("7", 7.0),
        ("0", 0.0),
        ("1.5", 1.5),
        (" 12 ", 12.0),
        ("86400", 86400.0),
        ("-5", None),
        ("soon", None),
        ("", None),
        ("1e3", None),
        ("nan", None),
        ("+5", None),
        ("0x10", None),
        ("١٢", None),
    ],
)
def test_retry_after_in_seconds(value, seconds):
    assert parse_retry_after(value, now=NOW) == seconds


def test_retry_after_as_an_http_date_is_relative_to_now():
    future = email.utils.formatdate(NOW + 30, usegmt=True)
    past = email.utils.formatdate(NOW - 30, usegmt=True)

    assert parse_retry_after(future, now=NOW) == 30.0
    assert parse_retry_after(past, now=NOW) == 0.0


def test_retry_after_as_a_date_without_a_zone_is_utc():
    date = email.utils.formatdate(NOW + 5, usegmt=True).replace("GMT", "-0000")

    assert parse_retry_after(date, now=NOW) == 5.0


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("1s", 1.0),
        ("250ms", 0.25),
        ("6m0s", 360.0),
        ("1m30s", 90.0),
        ("1h2m3s", 3723.0),
        ("1.5s", 1.5),
        ("2m59.56s", 179.56),
        ("0s", 0.0),
        ("1h", 3600.0),
        ("5m", 300.0),
        ("", None),
        ("s", None),
        ("1", None),
        ("-1s", None),
        ("1d", None),
        ("1s1h", None),
        ("soon", None),
    ],
)
def test_reset_durations(value, seconds):
    found = parse_reset_duration(value)

    assert found == pytest.approx(seconds) if seconds is not None else found is None


def test_retry_after_comes_first_then_the_reset_header():
    assert (
        retry_after_seconds(httpx.Headers({"retry-after": "3", "x-ratelimit-reset-requests": "6m0s"}), now=NOW) == 3.0
    )
    assert retry_after_seconds(httpx.Headers({"x-ratelimit-reset-requests": "6m0s"}), now=NOW) == 360.0
    assert (
        retry_after_seconds(httpx.Headers({"retry-after": "soon", "x-ratelimit-reset-requests": "250ms"}), now=NOW)
        == 0.25
    )
    assert retry_after_seconds(httpx.Headers({}), now=NOW) is None
    assert retry_after_seconds(httpx.Headers({"retry-after": "soon"}), now=NOW) is None


@pytest.mark.parametrize(
    ("headers", "seconds"),
    [
        ({"retry-after": "12"}, 12.0),
        ({"Retry-After": "3600"}, 3600.0),
        ({"x-ratelimit-reset-requests": "1s"}, 1.0),
        ({}, None),
    ],
)
def test_a_429_carries_the_wait_uncapped(headers, seconds):
    with pytest.raises(TransientProviderError) as caught:
        post(make_client(reply_with(429, headers=headers, json_body={"error": {"message": "slow down"}})))

    assert caught.value.retry_after_s == seconds


def test_a_429_with_a_date_uses_the_wall_clock():
    date = email.utils.formatdate(NOW + 45, usegmt=True)

    with pytest.raises(TransientProviderError) as caught:
        post(make_client(reply_with(429, headers={"retry-after": date}), wall_clock=lambda: NOW))

    assert caught.value.retry_after_s == 45.0


# ---------------------------------------------------------------------------------------------------------------
# The 8 MiB cap and Content-Encoding
# ---------------------------------------------------------------------------------------------------------------


def test_a_body_of_exactly_8_mib_is_read():
    reply = post(make_client(reply_with(200, content=b"a" * RESPONSE_MAX_BYTES)))

    assert len(reply.text) == RESPONSE_MAX_BYTES


@pytest.mark.parametrize("status", [200, 400, 401])
def test_a_body_past_8_mib_is_permanent_for_a_status_no_retry_fixes(status):
    with pytest.raises(PermanentProviderError, match="exceeds 8 MiB") as caught:
        post(make_client(reply_with(status, content=b"a" * (RESPONSE_MAX_BYTES + 1))))

    assert caught.value.status_code == status


@pytest.mark.parametrize("status", [429, 503, 520, 529])
def test_a_retryable_status_stays_transient_whatever_its_body(status):
    over = reply_with(status, content=b"a" * (RESPONSE_MAX_BYTES + 1), headers={"retry-after": "2"})
    undecodable = reply_with(status, content=b"not gzip", headers={"content-encoding": "gzip", "retry-after": "2"})
    unsupported = reply_with(status, content=b"x", headers={"content-encoding": "br", "retry-after": "2"})

    for handler, detail in ((over, "exceeds 8 MiB"), (undecodable, "could not be decoded"), (unsupported, "br")):
        with pytest.raises(TransientProviderError, match=detail) as caught:
            post(make_client(handler))
        assert (caught.value.status_code, caught.value.retry_after_s) == (status, 2.0)


def test_a_streamed_body_is_read_only_to_the_cap_with_memory_bounded():
    chunk = b"a" * (64 * 1024)
    pulled = []

    def endless():
        for _ in range(4096):  # 256 MiB if read whole
            pulled.append(len(chunk))
            yield chunk

    client = make_client(lambda request: streamed(200, content=endless()))
    tracemalloc.start()
    try:
        with pytest.raises(PermanentProviderError, match="exceeds 8 MiB"):
            post(client)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert sum(pulled) <= RESPONSE_MAX_BYTES + len(chunk)
    assert peak < 3 * RESPONSE_MAX_BYTES


@pytest.mark.parametrize("encoding", ["gzip", "x-gzip", "GZIP", "gzip, identity", "identity, gzip"])
def test_one_gzip_layer_is_decoded(encoding):
    body = gzip.compress(b'{"ok": true}')

    reply = post(make_client(reply_with(200, content=body, headers={"content-encoding": encoding})))

    assert reply.json == {"ok": True}


def test_one_deflate_layer_is_decoded():
    body = zlib.compress(b'{"ok": true}')

    reply = post(make_client(reply_with(200, content=body, headers={"content-encoding": "deflate"})))

    assert reply.json == {"ok": True}


@pytest.mark.parametrize("encoding", ["identity", ""])
def test_identity_is_read_as_is(encoding):
    reply = post(make_client(reply_with(200, content=b'{"ok": 1}', headers={"content-encoding": encoding})))

    assert reply.json == {"ok": 1}


@pytest.mark.parametrize("encoding", ["gzip, gzip", "gzip, deflate", "br", "zstd", "compress", "gzip;q=1", "chunked"])
def test_a_chain_or_another_encoding_is_refused(encoding):
    body = gzip.compress(gzip.compress(b"{}"))

    with pytest.raises(PermanentProviderError, match="unsupported Content-Encoding"):
        post(make_client(reply_with(200, content=body, headers={"content-encoding": encoding})))


def test_a_huge_gzip_layer_is_refused_with_memory_bounded():
    bomb = gzip.compress(b"\0" * (64 * MIB), compresslevel=9)  # about 64 KiB on the wire
    client = make_client(lambda request: streamed(200, content=bomb, headers={"content-encoding": "gzip"}))

    tracemalloc.start()
    try:
        with pytest.raises(PermanentProviderError, match="exceeds 8 MiB"):
            post(client)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 3 * RESPONSE_MAX_BYTES


def test_a_gzip_layer_decoding_to_exactly_the_cap_is_read():
    body = gzip.compress(b"a" * RESPONSE_MAX_BYTES)

    reply = post(make_client(reply_with(200, content=body, headers={"content-encoding": "gzip"})))

    assert len(reply.text) == RESPONSE_MAX_BYTES


@pytest.mark.parametrize(
    "body",
    [b"not gzip", gzip.compress(b'{"a": 1}')[:-6], gzip.compress(b"{}") + b"trailing", gzip.compress(b"{}") * 2],
    ids=["garbage", "truncated", "trailing-bytes", "two-members"],
)
def test_a_gzip_body_that_does_not_decode_exactly_is_permanent(body):
    with pytest.raises(PermanentProviderError, match="could not be decoded") as caught:
        post(make_client(reply_with(200, content=body, headers={"content-encoding": "gzip"})))

    assert caught.value.status_code == 200


def test_an_empty_body_with_an_encoding_is_empty():
    reply = post(make_client(reply_with(204, content=b"", headers={"content-encoding": "gzip"})))

    assert reply.text == ""


def test_a_compressed_body_past_8_mib_on_the_wire_is_refused_even_when_it_decodes_small():
    whole = zlib.compress(b'{"ok": true}')
    empty_block = b"\x00\x00\x00\xff\xff"  # a non-final stored block of no bytes, at a byte boundary
    padded = whole[:2] + empty_block * (RESPONSE_MAX_BYTES // len(empty_block) + 1) + whole[2:]
    assert zlib.decompress(padded) == b'{"ok": true}'

    with pytest.raises(PermanentProviderError, match="exceeds 8 MiB"):
        post(make_client(reply_with(200, content=padded, headers={"content-encoding": "deflate"})))


def test_a_reply_read_before_the_bounds_apply_is_refused():
    client = make_client(lambda request: httpx.Response(200, content=b"{}"))  # bytes content: read when built

    with pytest.raises(PermanentProviderError, match="read before"):
        post(client)


# ---------------------------------------------------------------------------------------------------------------
# Transport failures: a fixed vocabulary, never the exception's message
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("handler", "kind", "detail"),
    [
        (failing_with(httpx.ConnectTimeout), TransientProviderError, "connect timeout"),
        (failing_with(httpx.ReadTimeout), TransientProviderError, "read timeout"),
        (failing_with(httpx.WriteTimeout), TransientProviderError, "write timeout"),
        (failing_with(httpx.PoolTimeout), TransientProviderError, "pool timeout"),
        (failing_with(httpx.ConnectError), TransientProviderError, "connection failed"),
        (
            failing_with(httpx.ConnectError, OSError("x"), socket.gaierror(8, "nodename")),
            TransientProviderError,
            "DNS resolution failed",
        ),
        (
            failing_with(httpx.ConnectError, ConnectionRefusedError(61, "x")),
            TransientProviderError,
            "connection refused",
        ),
        (failing_with(httpx.ReadError, ConnectionResetError(54, "x")), TransientProviderError, "connection reset"),
        (failing_with(httpx.ReadError), TransientProviderError, "connection lost"),
        (failing_with(httpx.WriteError), TransientProviderError, "connection lost"),
        (failing_with(httpx.RemoteProtocolError), TransientProviderError, "the server broke the HTTP protocol"),
        (failing_with(httpx.ProxyError), TransientProviderError, "proxy error"),
        (
            failing_with(httpx.ConnectError, ssl.SSLCertVerificationError(1, "certificate verify failed")),
            PermanentProviderError,
            "TLS certificate verification failed",
        ),
        (failing_with(httpx.ConnectError, ssl.SSLError(1, "wrong version")), TransientProviderError, "TLS failed"),
        (failing_with(httpx.LocalProtocolError), PermanentProviderError, "the request broke the HTTP protocol"),
        (failing_with(httpx.UnsupportedProtocol), PermanentProviderError, "unsupported protocol"),
        (failing_with(httpx.DecodingError), PermanentProviderError, "the HTTP client failed"),
        (failing_with(httpx.TooManyRedirects), PermanentProviderError, "the HTTP client failed"),
    ],
)
def test_a_transport_failure_is_named_from_a_fixed_vocabulary(handler, kind, detail):
    with pytest.raises(kind) as caught:
        post(make_client(handler), headers=SECRETS)

    assert type(caught.value) is kind
    assert caught.value.status_code is None
    assert caught.value.detail == detail
    assert "boom" not in str(caught.value)
    assert_no_secret(caught.value)


def test_a_cause_reached_through_the_context_is_named():
    def handler(request):
        error = httpx.ConnectError("x", request=request)
        error.__context__ = OSError("wrapper")
        error.__context__.__context__ = socket.gaierror(8, "nodename")
        raise error

    with pytest.raises(TransientProviderError, match="DNS resolution failed"):
        post(make_client(handler))


@pytest.mark.parametrize(
    ("raised", "detail"),
    [
        (h11.LocalProtocolError("Too much data for declared Content-Length"), "the request broke the HTTP protocol"),
        (h11.RemoteProtocolError("bad reply"), "the server broke the HTTP protocol"),
        (httpx.StreamClosed(), "the HTTP client failed"),
    ],
)
def test_an_h11_or_stream_error_is_a_provider_error_never_raw(raised, detail):
    def handler(request):
        raise raised

    with pytest.raises(ProviderError, match=detail):
        post(make_client(handler), headers=SECRETS)


@pytest.mark.parametrize("raised", [httpx.InvalidURL("bad"), UnicodeError("IDNA")])
def test_a_url_httpx_refuses_when_building_the_request_is_permanent(monkeypatch, raised):
    def build_request(*args, **kwargs):
        raise raised

    client = make_client(reply_with(200, json_body={}))
    client.open()
    monkeypatch.setattr(httpx.Client, "build_request", build_request)

    with pytest.raises(PermanentProviderError, match="refused by the HTTP client"):
        post(client)


def test_the_cutter_shuts_a_socket_captured_after_the_deadline_fired():
    left, right = socket.socketpair()
    cutter = http_module._Cutter(0.01)
    assert cutter.arm()
    try:
        time.sleep(0.1)
        assert cutter.fired

        class Stream:
            def get_extra_info(self, name):
                return left if name == "socket" else None

        cutter.trace("connection.connect_tcp.complete", {"return_value": Stream()})
        right.settimeout(5)
        assert right.recv(1) == b""  # the connection was shut down
    finally:
        cutter.close()
        left.close()
        right.close()


def test_a_cutter_without_a_socket_to_capture_stops_the_request():
    cutter = http_module._Cutter(30.0)
    try:

        class Stream:
            def get_extra_info(self, name):
                return None

        with pytest.raises(http_module._CutUnavailable):
            cutter.trace("connection.connect_tcp.complete", {"return_value": Stream()})
        cutter.trace("http11.send_request_headers.complete", {})  # other events are ignored
    finally:
        cutter.close()


def test_a_cause_chain_that_loops_is_walked_a_bounded_number_of_times():
    def handler(request):
        error = httpx.ConnectError("x", request=request)
        first, second = OSError("a"), OSError("b")
        first.__cause__, second.__cause__ = second, first
        error.__cause__ = first
        raise error

    with pytest.raises(TransientProviderError, match="connection failed"):
        post(make_client(handler))


def test_a_timeout_while_the_body_streams_is_transient():
    def body():
        yield b'{"partial": '
        raise httpx.ReadTimeout("stalled")

    with pytest.raises(TransientProviderError, match="read timeout"):
        post(make_client(lambda request: streamed(200, content=body())))


# ---------------------------------------------------------------------------------------------------------------
# The caller's deadline (the socket cut is tested over real sockets)
# ---------------------------------------------------------------------------------------------------------------


def test_a_body_still_arriving_at_the_deadline_is_transient():
    clock = [100.0]

    def body():
        yield b'{"a": '
        clock[0] += 6.0
        yield b"1}"

    client = make_client(lambda request: streamed(200, content=body()), monotonic=lambda: clock[0])

    with pytest.raises(TransientProviderError, match="did not finish within 5 s") as caught:
        post(client, deadline=105.0)

    assert caught.value.status_code is None


def test_a_body_finishing_before_the_deadline_is_read():
    clock = [100.0]

    def body():
        yield b'{"a": '
        clock[0] += 4.0
        yield b"1}"

    client = make_client(lambda request: streamed(200, content=body()), monotonic=lambda: clock[0])

    assert post(client, deadline=105.0).json == {"a": 1}


# ---------------------------------------------------------------------------------------------------------------
# The clients' configuration and lifecycle
# ---------------------------------------------------------------------------------------------------------------


def test_the_remote_client_trusts_the_environment_and_never_follows_redirects(created):
    post(make_client(reply_with(200, json_body={}), max_connections=3))

    ((client, kwargs),) = created
    assert kwargs["follow_redirects"] is False
    assert kwargs["trust_env"] is True
    assert kwargs["limits"] == httpx.Limits(max_connections=3, max_keepalive_connections=0)
    assert client.headers["user-agent"] == USER_AGENT
    assert client.headers["accept-encoding"] == "identity"
    assert client.follow_redirects is False


@pytest.mark.parametrize("url", ["http://localhost:11434/v1", "http://127.0.0.1:8000/v1", "https://[::1]/v1"])
def test_a_loopback_request_uses_a_client_that_ignores_the_environment(created, url):
    post(make_client(reply_with(200, json_body={})), url=url)

    ((_, kwargs),) = created
    assert kwargs["trust_env"] is False
    assert kwargs["follow_redirects"] is False
    assert kwargs["limits"] == httpx.Limits(max_connections=4, max_keepalive_connections=0)


def test_open_builds_both_clients_once_on_the_callers_thread(created):
    client = make_client(reply_with(200, json_body={}))

    client.open()
    client.open()
    post(client)
    post(client, url="http://localhost:11434/v1")

    assert sorted(kwargs["trust_env"] for _, kwargs in created) == [False, True]


def test_a_client_without_a_transport_gets_the_default_one(created):
    client = HttpClient(max_connections=2)
    client.open()

    assert len(created) == 2
    assert all(kwargs.get("transport") is None for _, kwargs in created)
    client.close()


@pytest.mark.parametrize("value", [0, -1, 1025])
def test_max_connections_out_of_range_is_refused(value):
    with pytest.raises(ValueError, match="max_connections"):
        HttpClient(max_connections=value)


@pytest.mark.parametrize("value", [True, 1.5, "4"])
def test_max_connections_of_the_wrong_type_is_refused(value):
    with pytest.raises(TypeError, match="max_connections"):
        HttpClient(max_connections=value)


def test_allow_insecure_http_must_be_a_bool():
    with pytest.raises(TypeError, match="allow_insecure_http"):
        HttpClient(allow_insecure_http=1)


def test_the_clients_are_created_lazily_and_reused(created):
    client = make_client(reply_with(200, json_body={}))

    assert created == []
    post(client)
    post(client)
    assert len(created) == 1


def test_the_finalizer_closes_the_clients_when_the_helper_is_collected(created):
    client = make_client(reply_with(200, json_body={}))
    client.open()
    inner = [made for made, _ in created]

    assert not any(made.is_closed for made in inner)
    del client
    gc.collect()
    assert all(made.is_closed for made in inner)


def test_an_unused_helper_is_collected_without_creating_a_client(created):
    client = make_client(reply_with(200, json_body={}))
    del client
    gc.collect()

    assert created == []


def test_close_is_optional_idempotent_and_the_helper_stays_usable(created):
    client = make_client(reply_with(200, json_body={}))
    client.close()  # nothing created yet: nothing to close
    assert created == []

    post(client)
    first = created[0][0]
    client.close()
    client.close()
    assert first.is_closed

    assert post(client).status == 200
    assert len(created) == 2
    assert not created[1][0].is_closed


def test_a_close_during_a_request_lets_it_finish_and_closes_the_client_after(created):
    holder = {}

    def handler(request):
        holder["client"].close()  # another thread's close() lands while this request is in flight
        return streamed(200, json_body={"ok": True})

    client = holder["client"] = make_client(handler)

    assert post(client).json == {"ok": True}
    assert created[0][0].is_closed
    assert post(client).json == {"ok": True}  # a fresh client
    assert len(created) == 2


def test_a_close_while_the_body_streams_lets_it_finish(created):
    holder = {}

    def body():
        yield b'{"a": '
        holder["client"].close()
        yield b"1}"

    client = holder["client"] = make_client(lambda request: streamed(200, content=body()))

    assert post(client).json == {"a": 1}
    assert created[0][0].is_closed


def test_the_helper_is_a_context_manager_that_opens_and_closes(created):
    with make_client(reply_with(200, json_body={})) as client:
        assert len(created) == 2
        post(client)

    assert all(made.is_closed for made, _ in created)


def test_32_threads_creating_the_client_at_once_share_one(monkeypatch):
    made = []
    real = httpx.Client

    class Slow(real):
        def __init__(self, **kwargs):
            made.append(self)
            time.sleep(0.05)  # widen the race window
            super().__init__(**kwargs)

    monkeypatch.setattr(http_module.httpx, "Client", Slow)
    client = make_client(reply_with(200, json_body={"ok": True}))
    barrier = threading.Barrier(32, timeout=30)
    replies, errors = [], []

    def first_use():
        try:
            barrier.wait()
            replies.append(post(client).status)
        except BaseException as error:
            errors.append(repr(error))

    threads = [threading.Thread(target=first_use, daemon=True) for _ in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert errors == []
    assert not any(thread.is_alive() for thread in threads)
    assert len(made) == 1
    assert replies == [200] * 32


# ---------------------------------------------------------------------------------------------------------------
# open(): configuration failures named, never their values
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("HTTPS_PROXY", "socks5://127.0.0.1:1080"),
        ("https_proxy", "socks5h://user:hunter2@127.0.0.1:1080"),
        ("HTTP_PROXY", "ftp://127.0.0.1:21"),
        ("ALL_PROXY", "http://[::1"),
        ("HTTPS_PROXY", "http://user:hunter2@[::1"),
        ("HTTPS_PROXY", "http://proxy.example:99999"),
        ("HTTP_PROXY", "http://"),
    ],
)
def test_a_bad_proxy_variable_fails_open_naming_it_never_its_value(monkeypatch, name, value):
    monkeypatch.setenv(name, value)

    with pytest.raises(RationalizationError, match=name) as caught:
        HttpClient().open()

    assert value not in str(caught.value)
    assert "hunter2" not in str(caught.value)


def test_the_lower_case_proxy_variable_is_the_one_named_as_urllib_reads_it(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:3128")
    monkeypatch.setenv("https_proxy", "socks5://127.0.0.1:1080")

    with pytest.raises(RationalizationError, match="https_proxy"):
        HttpClient().open()


@pytest.mark.parametrize("name", ["SSL_CERT_FILE", "SSL_CERT_DIR"])
def test_a_missing_certificate_location_fails_open_naming_the_variable(monkeypatch, tmp_path, name):
    monkeypatch.setenv(name, str(tmp_path / "missing"))

    with pytest.raises(RationalizationError, match=f"{name} names no (file|directory)"):
        HttpClient().open()


def test_a_bad_environment_makes_post_json_permanent_when_open_was_skipped(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "socks5://user:hunter2@127.0.0.1:1080")

    with pytest.raises(PermanentProviderError, match="HTTPS_PROXY") as caught:
        post(HttpClient(), url="https://api.example.com/v1")

    assert "hunter2" not in str(caught.value)


def test_a_bad_environment_does_not_stop_a_loopback_request(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "socks5://127.0.0.1:1080")
    client = make_client(reply_with(200, json_body={}))

    assert post(client, url="http://localhost:11434/v1").status == 200


def test_with_a_transport_the_environment_is_not_consulted(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "socks5://127.0.0.1:1080")
    client = make_client(reply_with(200, json_body={}))

    client.open()
    assert post(client).status == 200


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9) and the extra
# ---------------------------------------------------------------------------------------------------------------

PROVIDERS = Path(http_module.__file__).parent
HTTP_STACK = ("httpx", "httpcore", "h11", "certifi")


def _imports(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    return functions, nested, modules


def test_http_imports_only_the_stdlib_the_http_stack_and_the_package_and_nothing_inside_functions():
    functions, nested, modules = _imports(Path(http_module.__file__))
    allowed_roots = {*HTTP_STACK, "datagrunt"}
    foreign = [
        m for m in modules if m.split(".")[0] not in sys.stdlib_module_names and m.split(".")[0] not in allowed_roots
    ]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert foreign == []
    assert [m for m in modules if m.startswith("datagrunt.") and not m.startswith("datagrunt.rationalization.")] == []
    assert set(HTTP_STACK) <= {m.split(".")[0] for m in modules}


def test_http_is_the_only_provider_module_that_imports_the_http_stack():
    importing = sorted(
        path.name for path in PROVIDERS.glob("*.py") if any(m.split(".")[0] in HTTP_STACK for m in _imports(path)[2])
    )

    assert importing == ["http.py"]


def test_importing_http_loads_the_http_stack():
    code = (
        "import json, sys; import datagrunt.rationalization.providers.http; "
        "print(json.dumps(sorted(m for m in ('httpx', 'httpcore', 'h11', 'certifi') if m in sys.modules)))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == sorted(HTTP_STACK)
