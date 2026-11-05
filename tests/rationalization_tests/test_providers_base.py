"""Tests for ``datagrunt.rationalization.providers.base``: the protocol and what every adapter shares (plan §4.5).

A provider is any object with the seven ``ModelProvider`` members (``model_family`` its vendor lineage, or
``"unknown"``); ``check_provider`` checks one at preflight. Provider
failures are ``TransientProviderError`` (worth a retry) or ``PermanentProviderError``, both ``ProviderError``s with a
bounded, one-line ``detail``, and both picklable. A model string must name its provider (R3: a guess decides which
vendor receives the documents); ``check_endpoint`` allows ``https`` anywhere and ``http`` only on loopback unless
asked, and never a URL with credentials, a query or a fragment; ``endpoint_model_id`` gives every endpoint its own
model id, since the cache key holds no URL. An API key lives in a ``_Secret``, which never shows or pickles.
"""

import ast
import copy
import json
import pickle
import subprocess
import sys
import traceback
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization.providers import base as base_module
from datagrunt.rationalization.providers.base import (
    DETAIL_CUT_MARKER,
    DETAIL_MAX_CHARS,
    IDENTITY_MAX_CHARS,
    MODEL_NAME_MAX_CHARS,
    OLLAMA_BASE_URL,
    OPENAI_BASE_URL,
    PROVIDER_KINDS,
    PROVIDER_MEMBERS,
    UNKNOWN_FAMILY,
    URL_MAX_CHARS,
    ModelProvider,
    ModelString,
    PermanentProviderError,
    ProviderError,
    TransientProviderError,
    _Secret,
    api_key_from_env,
    bounded_detail,
    check_endpoint,
    check_gcp_label,
    check_path_segment,
    check_provider,
    endpoint_model_id,
    family_problem,
    identity_problem,
    is_loopback_endpoint,
    parse_model_string,
)
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

TIMEOUT_S = 120


# ---------------------------------------------------------------------------------------------------------------
# bounded_detail
# ---------------------------------------------------------------------------------------------------------------


def test_bounded_detail_keeps_plain_text_as_is():
    assert bounded_detail("Invalid value for 'temperature'.") == "Invalid value for 'temperature'."


def test_bounded_detail_keeps_exactly_the_limit_and_cuts_one_more_with_the_marker():
    exact = "x" * DETAIL_MAX_CHARS
    over = "y" * (DETAIL_MAX_CHARS + 1)

    assert bounded_detail(exact) == exact
    cut = bounded_detail(over)
    assert len(cut) == DETAIL_MAX_CHARS
    assert cut.endswith(DETAIL_CUT_MARKER)
    assert cut == "y" * (DETAIL_MAX_CHARS - len(DETAIL_CUT_MARKER)) + DETAIL_CUT_MARKER


def test_the_limit_is_300_characters():
    assert DETAIL_MAX_CHARS == 300


@pytest.mark.parametrize(
    "character",
    ["\x00", "\x07", "\x1b", "\x7f", "\x85", "\x9b", "\r", "\n", "\t", "\x0b", "\x0c", " ", " "],
)
def test_bounded_detail_turns_controls_and_line_breaks_into_spaces(character):
    assert bounded_detail(f"a{character}b") == "a b"


@pytest.mark.parametrize("character", ["‮", "⁦", "\U000e0041", "\ud800", "\udfff"])
def test_bounded_detail_turns_bidi_tag_and_surrogate_code_points_into_spaces(character):
    detail = bounded_detail(f"a{character}b")

    assert detail == "a b"
    detail.encode("utf-8")


def test_bounded_detail_is_one_line_with_runs_collapsed_and_ends_stripped():
    assert bounded_detail('  {\n    "error":\r\n\t"bad"\n}\n  ') == '{ "error": "bad" }'


def test_bounded_detail_cuts_after_cleaning():
    detail = bounded_detail("\n" * 1000 + "z" * 10)

    assert detail == "z" * 10


def test_bounded_detail_of_nothing_is_empty():
    assert bounded_detail("") == ""
    assert bounded_detail(" \n\x00 ") == ""


def test_bounded_detail_refuses_a_non_str():
    with pytest.raises(TypeError, match="str"):
        bounded_detail(b"bytes")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# Provider errors
# ---------------------------------------------------------------------------------------------------------------


def test_both_provider_errors_are_provider_errors_and_not_rationalization_errors():
    assert issubclass(ProviderError, Exception)
    assert issubclass(TransientProviderError, ProviderError)
    assert issubclass(PermanentProviderError, ProviderError)
    assert not issubclass(ProviderError, RationalizationError)
    assert not issubclass(TransientProviderError, PermanentProviderError)
    assert not issubclass(PermanentProviderError, TransientProviderError)


def test_a_transient_error_holds_its_fields_and_says_them():
    error = TransientProviderError(429, 1.5, "Rate limit reached")

    assert (error.status_code, error.retry_after_s, error.detail) == (429, 1.5, "Rate limit reached")
    assert "429" in str(error)
    assert "Rate limit reached" in str(error)
    assert "1.5" in str(error)


def test_a_permanent_error_holds_its_fields_and_says_them():
    error = PermanentProviderError(401, "Invalid API key")

    assert (error.status_code, error.detail) == (401, "Invalid API key")
    assert "401" in str(error)
    assert "Invalid API key" in str(error)


def test_errors_without_a_status_or_wait_say_so():
    transient = TransientProviderError(None, None, "ReadTimeout")
    permanent = PermanentProviderError(None, "")

    assert transient.status_code is None
    assert transient.retry_after_s is None
    assert "ReadTimeout" in str(transient)
    assert permanent.detail == ""
    assert str(permanent)


def test_an_int_wait_is_held_as_a_float():
    error = TransientProviderError(503, 2, "busy")

    assert error.retry_after_s == 2.0
    assert type(error.retry_after_s) is float


def test_a_zero_wait_is_allowed():
    assert TransientProviderError(429, 0, "now").retry_after_s == 0.0


@pytest.mark.parametrize("wait", [-0.001, -1, float("nan"), float("inf"), float("-inf"), 10**400])
def test_a_negative_or_non_finite_wait_is_refused(wait):
    with pytest.raises(ValueError, match="retry_after_s"):
        TransientProviderError(429, wait, "x")


@pytest.mark.parametrize("wait", [True, "1", [1]])
def test_a_wait_of_the_wrong_type_is_refused(wait):
    with pytest.raises(TypeError, match="retry_after_s"):
        TransientProviderError(429, wait, "x")


@pytest.mark.parametrize(
    "make", [lambda s: TransientProviderError(s, None, "x"), lambda s: PermanentProviderError(s, "x")]
)
@pytest.mark.parametrize("status", [True, "429", 429.0])
def test_a_status_of_the_wrong_type_is_refused(make, status):
    with pytest.raises(TypeError, match="status_code"):
        make(status)


@pytest.mark.parametrize(
    "make", [lambda s: TransientProviderError(s, None, "x"), lambda s: PermanentProviderError(s, "x")]
)
@pytest.mark.parametrize("status", [99, 600, -1, 0])
def test_a_status_out_of_range_is_refused(make, status):
    with pytest.raises(ValueError, match="status_code"):
        make(status)


@pytest.mark.parametrize(
    "make", [lambda d: TransientProviderError(500, None, d), lambda d: PermanentProviderError(400, d)]
)
def test_a_detail_that_is_not_a_str_is_refused(make):
    with pytest.raises(TypeError, match="detail"):
        make(None)


@pytest.mark.parametrize(
    "make", [lambda d: TransientProviderError(500, None, d), lambda d: PermanentProviderError(400, d)]
)
def test_the_detail_is_bounded_and_one_line(make):
    error = make("line one\nline two\x1b[31m" + "x" * 1000)

    assert len(error.detail) == DETAIL_MAX_CHARS
    assert error.detail.endswith(DETAIL_CUT_MARKER)
    assert "\n" not in error.detail
    assert "\x1b" not in error.detail
    assert "\n" not in str(error)


@pytest.mark.parametrize(
    "make",
    [
        lambda: TransientProviderError(429, 1.25, "slow down"),
        lambda: TransientProviderError(None, None, "ConnectError"),
        lambda: PermanentProviderError(401, "bad key"),
        lambda: PermanentProviderError(None, ""),
    ],
    ids=["transient", "transient-bare", "permanent", "permanent-bare"],
)
@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
def test_provider_errors_survive_pickling(make, protocol):
    error = make()
    error.add_note("a note")
    copy_ = pickle.loads(pickle.dumps(error, protocol=protocol))

    assert type(copy_) is type(error)
    assert str(copy_) == str(error)
    assert copy_.status_code == error.status_code
    assert copy_.detail == error.detail
    assert getattr(copy_, "retry_after_s", None) == getattr(error, "retry_after_s", None)
    assert copy_.__notes__ == ["a note"]


# ---------------------------------------------------------------------------------------------------------------
# Model strings
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("openai:gpt-4o", ("openai", "gpt-4o", None)),
        ("anthropic:claude-sonnet-4-5", ("anthropic", "claude-sonnet-4-5", None)),
        ("gemini:gemini-2.5-pro", ("gemini", "gemini-2.5-pro", None)),
        ("ollama:llama3.1:8b", ("ollama", "llama3.1:8b", "http://localhost:11434/v1")),
        ("ollama:qwen2.5", ("ollama", "qwen2.5", "http://localhost:11434/v1")),
        ("openai-compatible:llama3@https://gw.example/v1", ("openai-compatible", "llama3", "https://gw.example/v1")),
        (
            "openai-compatible:claude-3@20240620@HTTPS://GW.Example/v1/",
            ("openai-compatible", "claude-3@20240620", "https://gw.example/v1"),
        ),
        ("openai-compatible:m@http://localhost:8000/v1", ("openai-compatible", "m", "http://localhost:8000/v1")),
        ("openai-compatible:m@http://127.0.0.1:8000", ("openai-compatible", "m", "http://127.0.0.1:8000")),
        (
            "openai-compatible:org/model:tag@https://gw.example",
            ("openai-compatible", "org/model:tag", "https://gw.example"),
        ),
    ],
)
def test_a_prefixed_model_string_parses(spec, expected):
    parsed = parse_model_string(spec)

    assert isinstance(parsed, ModelString)
    assert (parsed.kind, parsed.model, parsed.base_url) == expected


def test_the_kinds_are_the_five_prefixes():
    assert set(PROVIDER_KINDS) == {"openai", "anthropic", "gemini", "ollama", "openai-compatible"}


def test_the_default_endpoints():
    assert OLLAMA_BASE_URL == "http://localhost:11434/v1"
    assert OPENAI_BASE_URL == "https://api.openai.com/v1"


def test_a_model_string_is_frozen():
    parsed = parse_model_string("openai:gpt-4o")

    with pytest.raises(AttributeError):
        parsed.model = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("spec", "suggestion"),
    [
        ("gpt-4o", "openai:gpt-4o"),
        ("o1", "openai:o1"),
        ("claude-sonnet-4-5", "anthropic:claude-sonnet-4-5"),
        ("gemini-2.5-pro", "gemini:gemini-2.5-pro"),
        ("llama3", "openai:gpt-4o"),
    ],
)
def test_a_bare_model_name_is_refused_with_the_prefixed_form(spec, suggestion):
    with pytest.raises(RationalizationError, match="name the provider") as caught:
        parse_model_string(spec)

    assert suggestion in str(caught.value)


@pytest.mark.parametrize("spec", ["foo:bar", "llama3.1:8b", "OpenAI:gpt-4o", "gpt-4o:latest", "https://gw.example/v1"])
def test_an_unknown_prefix_is_refused_listing_the_prefixes(spec):
    with pytest.raises(RationalizationError, match="name the provider") as caught:
        parse_model_string(spec)

    for kind in PROVIDER_KINDS:
        assert kind in str(caught.value)


@pytest.mark.parametrize(
    "spec", ["foo:bar", "hunter2:gpt-4o", "sk-proj-hunter2hunter2:gpt", "token=hunter2@gw.example:m"]
)
def test_an_unknown_prefix_is_never_echoed(spec):
    prefix = spec.split(":", 1)[0]

    with pytest.raises(RationalizationError, match="unknown provider prefix") as caught:
        parse_model_string(spec)

    assert prefix not in str(caught.value)


@pytest.mark.parametrize(
    "spec",
    [
        "token=hunter2@gw.example:m",  # a prefix that is no plain word is never echoed
        "claude-x@hunter2.example",  # a bare name holding "@" is never echoed
        "claude sk-hunter2",  # nor one holding whitespace
        "gpt-4o://hunter2",  # nor one holding a URL
    ],
)
def test_a_refused_model_string_never_echoes_what_may_be_a_credential(spec):
    with pytest.raises(RationalizationError, match="name the provider") as caught:
        parse_model_string(spec)

    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize("spec", ["gemini:gemini-2.5-pro", "gemini:gemini-1.5-flash-001", "gemini:gemini_x.y-1"])
def test_a_gemini_model_that_is_one_path_segment_parses(spec):
    assert parse_model_string(spec).model == spec.split(":", 1)[1]


@pytest.mark.parametrize(
    "spec",
    [
        "gemini:../../v1beta/files",
        "gemini:gemini-2.5-pro/../../../other",
        "gemini:a?key=x",
        "gemini:a#frag",
        "gemini:a%2F..%2Fb",
        "gemini:a@b",
        "gemini:a/b",
        "gemini:.",
        "gemini:..",
        "gemini:a:b",
    ],
)
def test_a_gemini_model_that_is_not_one_path_segment_is_refused(spec):
    with pytest.raises(RationalizationError, match="path segment"):
        parse_model_string(spec)


def test_a_gemini_model_with_the_models_prefix_is_refused_with_the_remedy():
    with pytest.raises(RationalizationError, match="gemini:gemini-2.5-pro") as caught:
        parse_model_string("gemini:models/gemini-2.5-pro")

    assert "models/" in str(caught.value)


@pytest.mark.parametrize(
    "spec",
    ["openai-compatible:meta-llama/Llama-3-8b@https://gw.example/v1", "openai:ft:gpt-4o:org::id", "ollama:a/b:c"],
)
def test_a_body_borne_model_name_may_hold_slashes_and_colons(spec):
    assert parse_model_string(spec).model


def test_vertex_is_refused_pointing_at_the_provider_object():
    with pytest.raises(RationalizationError, match=r"GeminiProvider\.vertex"):
        parse_model_string("vertex:gemini-2.5-pro")


@pytest.mark.parametrize("spec", ["openai:", "anthropic:", "ollama:", "openai-compatible:@https://gw.example/v1"])
def test_an_empty_model_is_refused(spec):
    with pytest.raises(RationalizationError, match="no model"):
        parse_model_string(spec)


@pytest.mark.parametrize("spec", ["openai-compatible:llama3", "openai-compatible:"])
def test_openai_compatible_without_an_endpoint_is_refused(spec):
    with pytest.raises(RationalizationError, match="@https://"):
        parse_model_string(spec)


def test_openai_compatible_with_an_empty_endpoint_is_refused():
    with pytest.raises(RationalizationError):
        parse_model_string("openai-compatible:llama3@")


def test_openai_compatible_over_insecure_http_is_refused_naming_the_switch():
    with pytest.raises(RationalizationError, match="allow_insecure_http"):
        parse_model_string("openai-compatible:m@http://10.0.0.5")


@pytest.mark.parametrize(
    "model",
    ["gpt 4o", "gpt\t4o", "gpt\n4o", "gpt\x004o", "gpt\x7f", "gpt 4o", "gpt‮4o", "gpt​", "gpt\ud800"],
)
def test_a_model_name_with_whitespace_or_control_characters_is_refused(model):
    with pytest.raises(RationalizationError, match="whitespace or control"):
        parse_model_string(f"openai:{model}")


def test_a_model_name_of_the_limit_parses_and_one_more_is_refused():
    assert MODEL_NAME_MAX_CHARS == 256
    assert parse_model_string("openai:" + "m" * MODEL_NAME_MAX_CHARS).model == "m" * MODEL_NAME_MAX_CHARS

    with pytest.raises(RationalizationError, match="256"):
        parse_model_string("openai:" + "m" * (MODEL_NAME_MAX_CHARS + 1))


@pytest.mark.parametrize("spec", ["openai:gpt-4o@https://proxy.example/v1", "anthropic:claude@https://gw.example"])
def test_a_model_name_holding_a_url_is_refused_pointing_at_openai_compatible(spec):
    with pytest.raises(RationalizationError, match="openai-compatible:<model>@<url>"):
        parse_model_string(spec)


@pytest.mark.parametrize("spec", [None, 1, b"openai:gpt-4o", ["openai:gpt-4o"]])
def test_a_model_string_that_is_not_a_str_is_refused(spec):
    with pytest.raises(RationalizationError, match="str"):
        parse_model_string(spec)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "spec",
    [
        "openai-compatible:m@https://user:hunter2@gw.example/v1",
        "openai-compatible:m@https://gw.example/v1?key=hunter2",
        "openai-compatible:m@https://gw.example/v1#hunter2",
        "openai-compatible:m@https://hunter2@gw.example/v1",
    ],
)
def test_a_refused_endpoint_never_echoes_its_credentials(spec):
    with pytest.raises(RationalizationError) as caught:
        parse_model_string(spec)

    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize(
    ("kind", "model", "base_url"),
    [
        ("openai", "", None),
        ("openai", "m", "https://gw.example/v1/"),
        ("openai-compatible", "m", None),
        ("vertex", "m", None),
        ("openai", "a b", None),
        ("openai", 1, None),
        ("openai", "m", 1),
    ],
)
def test_a_model_string_built_by_hand_is_checked(kind, model, base_url):
    with pytest.raises(RationalizationError):
        ModelString(kind, model, base_url)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# check_endpoint
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://api.example.com/v1", "https://api.example.com/v1"),
        ("HTTPS://API.Example.COM/V1/", "https://api.example.com/V1"),
        ("https://api.example.com", "https://api.example.com"),
        ("https://api.example.com/", "https://api.example.com"),
        ("https://api.example.com///", "https://api.example.com"),
        ("https://api.example.com:8443/v1", "https://api.example.com:8443/v1"),
        ("https://10.0.0.5/v1", "https://10.0.0.5/v1"),
        ("https://[2001:db8::1]:8443/v1", "https://[2001:db8::1]:8443/v1"),
        ("https://[0:0:0:0:0:0:0:1]/v1", "https://[::1]/v1"),
        ("https://xn--exmple-cua.com/v1", "https://xn--exmple-cua.com/v1"),
        ("https://my_gateway.internal/v1", "https://my_gateway.internal/v1"),
        (
            "https://api.example.com/models/gemini-2.5-pro:generateContent",
            "https://api.example.com/models/gemini-2.5-pro:generateContent",
        ),
    ],
)
def test_https_is_allowed_and_normalized(url, expected):
    assert check_endpoint(url) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://localhost:11434/v1", "http://localhost:11434/v1"),
        ("http://LOCALHOST:11434/v1/", "http://localhost:11434/v1"),
        ("http://127.0.0.1:8000", "http://127.0.0.1:8000"),
        ("http://127.255.0.9/v1", "http://127.255.0.9/v1"),
        ("http://[::1]:8080/v1", "http://[::1]:8080/v1"),
    ],
)
def test_http_is_allowed_on_loopback(url, expected):
    assert check_endpoint(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "http://10.0.0.5",
        "http://example.com/v1",
        "http://localhost.example.com/v1",
        "http://128.0.0.1/v1",
        "http://0.0.0.0:8000",
        "http://[::2]/v1",
    ],
)
def test_http_elsewhere_is_refused_unless_allowed(url):
    with pytest.raises(RationalizationError, match="allow_insecure_http"):
        check_endpoint(url)

    assert check_endpoint(url, allow_insecure_http=True).startswith("http://")


@pytest.mark.parametrize("url", ["ftp://gw.example/v1", "file:///etc/passwd", "ws://localhost/v1", "gw.example/v1"])
@pytest.mark.parametrize("allow", [False, True])
def test_other_schemes_are_refused_even_when_http_is_allowed(url, allow):
    with pytest.raises(RationalizationError, match="https"):
        check_endpoint(url, allow_insecure_http=allow)


@pytest.mark.parametrize(
    "url",
    [
        "https://user:hunter2@gw.example/v1",
        "https://hunter2@gw.example",
        "https://@gw.example",
        "https://gw.example/a@hunter2",
    ],
)
def test_a_url_with_an_at_sign_is_refused_without_echoing_it(url):
    with pytest.raises(RationalizationError, match="credentials") as caught:
        check_endpoint(url)

    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize(
    "url", ["https://gw.example/v1?key=hunter2", "https://gw.example/v1?", "https://gw.example?hunter2"]
)
def test_a_url_with_a_query_is_refused_without_echoing_it(url):
    with pytest.raises(RationalizationError, match="query") as caught:
        check_endpoint(url)

    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize("url", ["https://gw.example/v1#hunter2", "https://gw.example#"])
def test_a_url_with_a_fragment_is_refused_without_echoing_it(url):
    with pytest.raises(RationalizationError, match="fragment") as caught:
        check_endpoint(url)

    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize("url", ["https://", "https:///v1", "https:gw.example", "https://:443/v1", "http://"])
def test_a_url_without_a_host_is_refused(url):
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(url)


@pytest.mark.parametrize("url", ["https://exämple.com/v1", "https://gw.example/vé", "https://гугл.рф"])
def test_a_non_ascii_url_is_refused_asking_for_punycode(url):
    with pytest.raises(RationalizationError, match="punycode"):
        check_endpoint(url)


@pytest.mark.parametrize(
    "url",
    [
        " https://gw.example",
        "https://gw.example /v1",
        "https://gw.example\t",
        "https://gw.example/v1\n",
        "https://gw\x00.example",
        "https://gw.example/\x7f",
    ],
)
def test_a_url_with_whitespace_or_control_characters_is_refused(url):
    with pytest.raises(RationalizationError, match="whitespace or control"):
        check_endpoint(url)


@pytest.mark.parametrize("url", ["https://good.example\\.evil.example/v1", "https://good.example/v1\\..\\x"])
def test_a_url_with_a_backslash_is_refused(url):
    with pytest.raises(RationalizationError, match="backslash"):
        check_endpoint(url)


@pytest.mark.parametrize("url", ["https://[fe80::1%25eth0]/v1", "http://[::1%25lo0]:8080/v1"])
def test_an_ipv6_host_with_a_zone_is_refused(url):
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(url, allow_insecure_http=True)


@pytest.mark.parametrize(
    "url",
    [
        "https://%65vil.example/v1",
        "https://gw..example/v1",
        "https://gw.example./v1",
        "https://-gw.example",
        "https://[zz::1]/v1",
    ],
)
def test_a_malformed_host_is_refused(url):
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://2130706433/v1",
        "https://0x7f.1/v1",
        "https://0x7f000001/v1",
        "https://a.0x/v1",
        "https://a.0XFF/v1",
        "https://1/v1",
        "http://127.1/v1",
        "https://127.000.000.001/v1",
        "https://999.1.1.1/v1",
        "https://1.2.3.04/v1",
        "https://1.2.3.256/v1",
        "https://017700000001/v1",
    ],
)
@pytest.mark.parametrize("allow", [False, True])
def test_a_host_that_looks_numeric_but_is_no_canonical_ip_address_is_refused(url, allow):
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(url, allow_insecure_http=allow)


@pytest.mark.parametrize(
    "url", ["https://1.example/v1", "https://0x7f.example/v1", "https://a.1b/v1", "https://a.b1/v1"]
)
def test_a_numeric_label_other_than_the_last_is_a_host_name(url):
    assert check_endpoint(url) == url


@pytest.mark.parametrize(
    "url", ["https://xn--zz/v1", "https://xn--/v1", "https://xn--a.example/v1", "https://xn--abc-.example"]
)
def test_a_punycode_label_that_does_not_decode_is_refused(url):
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(url)


@pytest.mark.parametrize("url", ["https://xn--n3h.example/v1", "https://xn--ls8h.example/v1"])
def test_a_punycode_label_decoding_to_a_symbol_is_refused_as_httpx_refuses_it(url):
    with pytest.raises(RationalizationError, match="punycode"):
        check_endpoint(url)


def test_a_punycode_label_that_decodes_passes():
    assert check_endpoint("https://xn--80ak6aa92e.com/v1") == "https://xn--80ak6aa92e.com/v1"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://gw.example:443/v1", "https://gw.example/v1"),
        ("https://gw.example:0443/v1", "https://gw.example/v1"),
        ("http://localhost:80/v1", "http://localhost/v1"),
        ("https://gw.example:80/v1", "https://gw.example:80/v1"),
        ("http://localhost:443/v1", "http://localhost:443/v1"),
        ("https://gw.example/v1/./", "https://gw.example/v1"),
        ("https://gw.example/x/../v1", "https://gw.example/v1"),
        ("https://gw.example/v1/../../admin", "https://gw.example/admin"),
        ("https://gw.example/%2e%2e/admin", "https://gw.example/admin"),
        ("https://gw.example/%2E/v1", "https://gw.example/v1"),
        ("https://gw.example/%76%31", "https://gw.example/v1"),
        ("https://gw.example/a%2fb", "https://gw.example/a%2Fb"),
        ("https://gw.example/%0d%0aX", "https://gw.example/%0D%0AX"),
        ("https://gw.example/%7e%5F", "https://gw.example/~_"),
        ("https://gw.example//evil.example/v1", "https://gw.example//evil.example/v1"),
        ("https://gw.example/v1;x=1", "https://gw.example/v1;x=1"),
    ],
)
def test_default_ports_dot_segments_and_percent_encodings_are_normalized(url, expected):
    assert check_endpoint(url) == expected


@pytest.mark.parametrize("url", ["https://gw.example/%zz", "https://gw.example/%2", "https://gw.example/a%"])
def test_a_malformed_percent_encoding_is_refused(url):
    with pytest.raises(RationalizationError, match="percent"):
        check_endpoint(url)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("https://gw.example/v1", "https://gw.example:443/v1"),
        ("http://localhost/v1", "http://localhost:80/v1"),
        ("https://gw.example/v1", "https://gw.example/v1/./"),
        ("https://gw.example/v1", "https://gw.example/x/../v1"),
        ("https://gw.example/v1", "https://gw.example/%76%31"),
    ],
)
def test_one_endpoint_written_two_ways_gets_one_model_id(first, second):
    assert endpoint_model_id("openai-compatible", "m", first) == endpoint_model_id("openai-compatible", "m", second)


_LABELS = st.text(alphabet="ax0123456789-_", min_size=1, max_size=6) | st.sampled_from(
    ["xn--80ak6aa92e", "xn--zz", "0x7f", "0X1", "255", "256", "001", "localhost", "a-", "-a"]
)
_HOSTS = (
    st.lists(_LABELS, min_size=1, max_size=4).map(".".join)
    | st.tuples(*[st.integers(0, 300).map(str) | st.sampled_from(["00", "010", "0x1"])] * 4).map(".".join)
    | st.sampled_from(["[::1]", "[2001:db8::1]", "[0:0:0:0:0:0:0:1]", "[::ffff:127.0.0.1]", "[zz::1]"])
)
_PATHS = st.lists(
    st.text(alphabet="az09._~-;=:", max_size=4) | st.sampled_from([".", "..", "%2e", "%2E%2e", "%41", "%2F", "%zz"]),
    max_size=4,
).map(lambda segments: "".join("/" + segment for segment in segments))


@settings(max_examples=800, deadline=None)
@given(
    scheme=st.sampled_from(["https", "http", "HTTPS"]),
    host=_HOSTS,
    port=st.none() | st.sampled_from([80, 443, 8443, 1, 65535]),
    path=_PATHS,
)
def test_every_endpoint_check_endpoint_accepts_builds_an_httpx_url_with_the_same_host(scheme, host, port, path):
    url = f"{scheme}://{host}{'' if port is None else f':{port}'}{path}"
    try:
        normalized = check_endpoint(url, allow_insecure_http=True)
    except RationalizationError:
        return
    parts = urlsplit(normalized)
    built = httpx.URL(normalized)

    assert built.raw_host.decode("ascii") == parts.hostname
    assert built.port == parts.port
    assert built.raw_path.decode("ascii") == (parts.path or "/")
    assert check_endpoint(normalized, allow_insecure_http=True) == normalized  # normalizing twice changes nothing


@pytest.mark.parametrize(
    ("url", "loopback"),
    [
        ("http://localhost:11434/v1", True),
        ("https://127.0.0.1/v1", True),
        ("http://127.9.9.9", True),
        ("http://[::1]:8080", True),
        ("https://gw.example/v1", False),
        ("http://10.0.0.5/v1", False),
        ("https://localhost.example/v1", False),
    ],
)
def test_is_loopback_endpoint(url, loopback):
    assert is_loopback_endpoint(check_endpoint(url, allow_insecure_http=True)) is loopback


@pytest.mark.parametrize(
    "url", ["http://[::ffff:127.0.0.1]/v1", "https://[::ffff:7f00:1]/v1", "https://[::ffff:10.0.0.5]/v1"]
)
@pytest.mark.parametrize("allow", [False, True])
def test_an_ipv4_mapped_ipv6_literal_is_refused(url, allow):
    with pytest.raises(RationalizationError, match="IPv4-mapped"):
        check_endpoint(url, allow_insecure_http=allow)


def test_a_host_of_253_characters_passes_and_one_more_is_refused():
    host = ".".join(["a" * 63] * 3 + ["b" * 61])  # 253 characters
    longer = host + "c"

    assert len(host) == 253
    assert check_endpoint(f"https://{host}/v1") == f"https://{host}/v1"
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(f"https://{longer}/v1")


def test_a_label_of_63_characters_passes_and_one_more_is_refused():
    assert check_endpoint(f"https://{'a' * 63}.example") == f"https://{'a' * 63}.example"
    with pytest.raises(RationalizationError, match="host"):
        check_endpoint(f"https://{'a' * 64}.example")


@pytest.mark.parametrize("url", ["https://gw.example:99999/v1", "https://gw.example:abc/v1", "https://gw.example:0/v1"])
def test_a_bad_port_is_refused(url):
    with pytest.raises(RationalizationError, match="port"):
        check_endpoint(url)


def test_a_url_of_the_limit_passes_and_one_more_is_refused():
    head = "https://gw.example/"
    exact = head + "a" * (URL_MAX_CHARS - len(head))

    assert check_endpoint(exact) == exact
    with pytest.raises(RationalizationError, match=str(URL_MAX_CHARS)):
        check_endpoint(exact + "a")


@pytest.mark.parametrize("url", [None, 1, b"https://gw.example"])
def test_a_url_that_is_not_a_str_is_refused(url):
    with pytest.raises(RationalizationError, match="str"):
        check_endpoint(url)  # type: ignore[arg-type]


@pytest.mark.parametrize("allow", [1, "yes", None])
def test_the_switch_must_be_a_bool(allow):
    with pytest.raises(RationalizationError, match="allow_insecure_http"):
        check_endpoint("https://gw.example", allow_insecure_http=allow)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# endpoint_model_id
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "model", "base_url", "expected"),
    [
        ("openai", "gpt-4o", None, "openai:gpt-4o"),
        ("openai", "gpt-4o", "https://api.openai.com/v1", "openai:gpt-4o"),
        ("openai", "gpt-4o", "https://API.openai.com/v1/", "openai:gpt-4o"),
        ("openai", "gpt-4o", "https://proxy.example/v1", "openai:gpt-4o@https://proxy.example/v1"),
        ("anthropic", "claude-sonnet-4-5", None, "anthropic:claude-sonnet-4-5"),
        ("gemini", "gemini-2.5-pro", None, "gemini:gemini-2.5-pro"),
        ("ollama", "llama3.1:8b", None, "ollama:llama3.1:8b"),
        ("ollama", "llama3.1:8b", "http://localhost:11434/v1", "ollama:llama3.1:8b"),
        ("ollama", "llama3.1:8b", "http://127.0.0.1:11434/v1", "ollama:llama3.1:8b@http://127.0.0.1:11434/v1"),
        ("openai-compatible", "llama3", "https://gw.example/v1", "openai-compatible:llama3@https://gw.example/v1"),
        ("openai-compatible", "llama3", "HTTPS://GW.EXAMPLE/v1/", "openai-compatible:llama3@https://gw.example/v1"),
        ("openai-compatible", "m", "http://10.0.0.5", "openai-compatible:m@http://10.0.0.5"),
        ("openai", "gpt-4o", "https://api.openai.com/v1/proxy/x", "openai:gpt-4o@https://api.openai.com/v1/proxy/x"),
        ("openai", "gpt-4o", "https://api.openai.com/v1extra", "openai:gpt-4o@https://api.openai.com/v1extra"),
        ("openai", "gpt-4o", "https://api.openai.com", "openai:gpt-4o@https://api.openai.com"),
        ("openai", "gpt-4o", "https://api.openai.com:443/v1", "openai:gpt-4o"),
        ("ollama", "llama3", "http://localhost:11434/v1/x", "ollama:llama3@http://localhost:11434/v1/x"),
        ("ollama", "llama3", "http://localhost:11434/v1/", "ollama:llama3"),
    ],
)
def test_the_model_id_names_the_endpoint(kind, model, base_url, expected):
    assert endpoint_model_id(kind, model, base_url) == expected


def test_openai_compatible_needs_its_endpoint():
    with pytest.raises(RationalizationError, match="endpoint"):
        endpoint_model_id("openai-compatible", "m", None)


@pytest.mark.parametrize(
    ("kind", "model", "base_url"),
    [
        ("callable", "m", None),
        ("vertex", "m", None),
        ("openai", "", None),
        ("openai", "a b", None),
        ("openai", "a@https://x.example", None),
        ("openai", None, None),
        (None, "m", None),
        ("gemini", "a/b", None),
        ("gemini", "models/gemini-2.5-pro", None),
        ("openai", "m", "https://gw.example/v1?key=1"),
        ("openai", "m", "ftp://gw.example"),
    ],
)
def test_a_bad_kind_model_or_endpoint_is_refused(kind, model, base_url):
    with pytest.raises(RationalizationError):
        endpoint_model_id(kind, model, base_url)


@pytest.mark.parametrize(
    "spec",
    [
        "openai:gpt-4o",
        "ollama:llama3.1:8b",
        "openai-compatible:llama3@https://gw.example/v1",
        "openai-compatible:claude-3@20240620@https://gw.example/v1",
    ],
)
def test_a_parsed_model_string_gives_back_its_own_model_id(spec):
    parsed = parse_model_string(spec)

    assert endpoint_model_id(parsed.kind, parsed.model, parsed.base_url) == spec


ENDPOINTS = [
    None,
    "https://gw.example/v1",
    "https://gw.example/v2",
    "https://gw.example:8443/v1",
    "https://other.example/v1",
    "http://localhost:11434/v1",
    "http://127.0.0.1:11434/v1",
    "https://api.openai.com/v1",
    "https://gw.example/a:b",
]
DEFAULTS = {"openai": "https://api.openai.com/v1", "ollama": "http://localhost:11434/v1"}


def _endpoint_identity(kind, base_url):
    """The endpoint a (kind, base_url) pair names: its default's URL when None or the default, else the URL."""
    return DEFAULTS.get(kind) if base_url is None else base_url


# A model name may hold "@" and ":", and may try to end in what looks like an endpoint.
MODELS = st.builds(
    lambda head, tail: head + tail,
    st.text(alphabet="ab@:/.-", min_size=1, max_size=6),
    st.sampled_from(["", "@https://gw.example/v1", "@https://api.openai.com/v1", "@gw.example", "@", ":8b"]),
)


@settings(max_examples=500, deadline=None)
@given(
    kind=st.sampled_from(PROVIDER_KINDS),
    first=st.tuples(MODELS, st.sampled_from(ENDPOINTS)),
    second=st.tuples(MODELS, st.sampled_from(ENDPOINTS)),
)
def test_two_endpoints_or_two_models_never_share_a_model_id(kind, first, second):
    ids = []
    for model, base_url in (first, second):
        try:
            ids.append(endpoint_model_id(kind, model, base_url))
        except RationalizationError:
            return  # "://" in the model, or openai-compatible without an endpoint
    if ids[0] == ids[1]:
        assert first[0] == second[0]
        assert _endpoint_identity(kind, first[1]) == _endpoint_identity(kind, second[1])


# ---------------------------------------------------------------------------------------------------------------
# Path segments and GCP labels (what a URL-path adapter puts in a URL)
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["gemini-2.5-pro", "a", "A.b_c-9", "..a", "a..", "x" * 256])
def test_a_path_segment_of_safe_characters_passes(value):
    assert check_path_segment(value, name="model") == value


@pytest.mark.parametrize(
    "value", ["", ".", "..", "a/b", "a?b", "a#b", "a%2Fb", "a@b", "a b", "a:b", "é", "x" * 257, "a\\b", None, 1]
)
def test_a_path_segment_that_could_change_the_url_is_refused_naming_it(value):
    with pytest.raises(RationalizationError, match="the_name"):
        check_path_segment(value, name="the_name")


@pytest.mark.parametrize("value", ["us-central1", "europe-west4", "global", "a", "asia-northeast1", "a" * 52])
def test_a_location_label_passes(value):
    assert check_gcp_label(value, field="location") == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "evil.example/x",
        "us-central1.evil",
        "US-CENTRAL1",
        "-us",
        "us-",
        "a" * 53,  # "{location}-aiplatform" must fit one 63-character DNS label
        "us central",
        "us_central1",
        None,
    ],
)
def test_a_bad_location_label_is_refused(value):
    with pytest.raises(RationalizationError, match="location"):
        check_gcp_label(value, field="location")


@pytest.mark.parametrize(
    "value", ["my-project", "abcdef", "a" + "b" * 28 + "c", "project-123", "123456789012", "example.com:my-project"]
)
def test_a_project_id_number_or_domain_scoped_id_passes(value):
    assert check_gcp_label(value, field="project") == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "abcde",
        "a" * 31,
        "1project",
        "my-project-",
        "My-Project",
        "my_project",
        "evil.example/x",
        "projects/x",
        "0123",
        "example.com:",
        ":my-project",
        "a..b:my-project",
        None,
    ],
)
def test_a_bad_project_is_refused(value):
    with pytest.raises(RationalizationError, match="project"):
        check_gcp_label(value, field="project")


def test_a_domain_scoped_project_past_256_characters_is_refused():
    value = ".".join(["a" * 60] * 4) + ".example:my-project"

    assert len(value) > 256
    with pytest.raises(RationalizationError, match="project"):
        check_gcp_label(value, field="project")


def test_the_gcp_field_must_be_project_or_location():
    with pytest.raises(ValueError, match="field"):
        check_gcp_label("x", field="zone")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# _Secret and api_key_from_env
# ---------------------------------------------------------------------------------------------------------------


def test_a_secret_never_shows():
    secret = _Secret("sk-hunter2")

    assert repr(secret) == "<secret>"
    assert str(secret) == "<secret>"
    assert f"{secret}" == "<secret>"
    assert "hunter2" not in repr([secret, {"key": secret}])


def test_a_secret_reveals_its_value():
    assert _Secret("sk-hunter2").reveal() == "sk-hunter2"


def test_a_secret_has_no_instance_dict():
    assert not hasattr(_Secret("sk-hunter2"), "__dict__")


@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
def test_a_secret_never_pickles(protocol):
    with pytest.raises(TypeError, match="pickle"):
        pickle.dumps(_Secret("sk-hunter2"), protocol=protocol)
    with pytest.raises(TypeError, match="pickle"):
        pickle.dumps({"key": _Secret("sk-hunter2")}, protocol=protocol)


@pytest.mark.parametrize("copier", [copy.copy, copy.deepcopy])
def test_a_secret_never_copies(copier):
    with pytest.raises(TypeError):
        copier(_Secret("sk-hunter2"))


def test_a_secret_must_be_a_non_blank_str():
    with pytest.raises(TypeError, match="str"):
        _Secret(b"sk")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="blank"):
        _Secret("  ")


@pytest.mark.parametrize(
    "value", [" sk", "sk ", "\tsk", "sk\t", "sk\n", "s\rk", "s\x00k", "sk\x7f", "sk-é", "sk\u2028"]
)
def test_a_secret_must_be_printable_ascii_without_edge_whitespace(value):
    with pytest.raises(ValueError, match="printable ASCII") as caught:
        _Secret(value)

    assert value.strip() not in str(caught.value)


def test_a_secret_may_hold_an_inner_space():
    assert _Secret("Bearer sk-x").reveal() == "Bearer sk-x"


def test_a_secret_never_gives_its_state():
    with pytest.raises(TypeError):
        _Secret("sk-hunter2").__getstate__()


def test_json_with_a_str_default_shows_only_the_mask():
    assert json.dumps({"key": _Secret("sk-hunter2")}, default=str) == '{"key": "<secret>"}'


def test_a_secret_redacts_its_value_and_its_words_but_the_scheme():
    secret = _Secret("sk-hunter2").prefixed("Bearer ")

    assert secret.redact("got Bearer sk-hunter2, then sk-hunter2 alone") == "got <secret>, then <secret> alone"
    assert secret.redact("a Bearer token") == "a Bearer token"
    assert _Secret("sk-hunter2").redact("x sk-hunter2 y") == "x <secret> y"
    with pytest.raises(TypeError):
        secret.redact(b"x")  # type: ignore[arg-type]


def test_a_prefixed_secret_holds_the_prefix_and_never_shows():
    secret = _Secret("sk-hunter2").prefixed("Bearer ")

    assert isinstance(secret, _Secret)
    assert secret.reveal() == "Bearer sk-hunter2"
    assert repr(secret) == "<secret>"


@pytest.mark.parametrize("prefix", ["Bearer\r\n", "Béarer ", " Bearer ", 1])
def test_a_bad_prefix_is_refused(prefix):
    with pytest.raises((TypeError, ValueError)):
        _Secret("sk").prefixed(prefix)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["sk-hunter2\n", " sk-hunter2", "sk-hunter2\u00e9"])
def test_an_invalid_key_in_the_environment_fails_at_preflight_naming_the_variable(monkeypatch, value):
    monkeypatch.setenv("DATAGRUNT_TEST_KEY", value)

    with pytest.raises(RationalizationError, match="DATAGRUNT_TEST_KEY") as caught:
        api_key_from_env("DATAGRUNT_TEST_KEY")

    assert "hunter2" not in str(caught.value)


def test_an_api_key_comes_from_its_variable(monkeypatch):
    monkeypatch.setenv("DATAGRUNT_TEST_KEY", "sk-hunter2")

    key = api_key_from_env("DATAGRUNT_TEST_KEY")

    assert isinstance(key, _Secret)
    assert key.reveal() == "sk-hunter2"


@pytest.mark.parametrize("value", [None, "", "   ", "\t\n"])
def test_an_unset_or_blank_variable_gives_no_key(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("DATAGRUNT_TEST_KEY", raising=False)
    else:
        monkeypatch.setenv("DATAGRUNT_TEST_KEY", value)

    assert api_key_from_env("DATAGRUNT_TEST_KEY") is None


@pytest.mark.parametrize("var", [None, "", " ", 1])
def test_the_variable_name_must_be_a_non_blank_str(var):
    with pytest.raises((TypeError, ValueError)):
        api_key_from_env(var)  # type: ignore[arg-type]


PROBE = "probe-KEY-7f3a"
"""The text of a key that must never reach a traceback local."""


def traceback_text(error):
    """The error's traceback with the locals of datagrunt's frames, as a log with ``capture_locals`` prints them.

    The test's own frame is left out: it is the caller, which holds what it passed.
    """
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def assert_secret_free(error):
    """No key text in the error's traceback locals, text or repr, and no chained exception that could hold it."""
    for text in (traceback_text(error), str(error), repr(error)):
        assert PROBE not in text
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    "value", [f"AIza-{PROBE}\n", f" sk-{PROBE}", f"sk-{PROBE}é", f"sk-{PROBE}\x7f", f"sk-{PROBE}\r\n"]
)
def test_an_invalid_key_in_the_environment_never_reaches_a_traceback_local(monkeypatch, value):
    monkeypatch.setenv("DATAGRUNT_TEST_KEY", value)

    with pytest.raises(RationalizationError, match="DATAGRUNT_TEST_KEY") as caught:
        api_key_from_env("DATAGRUNT_TEST_KEY")

    assert_secret_free(caught.value)


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (f"sk-{PROBE}\n", ValueError),
        (f" sk-{PROBE}", ValueError),
        (f"sk-{PROBE}é", ValueError),
        (f"sk-{PROBE}\x7f", ValueError),
        (f"sk-{PROBE}".encode(), TypeError),
        (bytearray(f"sk-{PROBE}".encode()), TypeError),
    ],
)
def test_a_refused_secret_never_reaches_a_traceback_local(value, error):
    with pytest.raises(error) as caught:
        _Secret(value)

    assert_secret_free(caught.value)


@pytest.mark.parametrize("prefix", ["Bearer\r\n", "Béarer ", " Bearer ", "Bearer\x00"])
def test_a_refused_prefixed_secret_never_reaches_a_traceback_local(prefix):
    with pytest.raises(ValueError) as caught:
        _Secret(f"sk-{PROBE}").prefixed(prefix)

    assert_secret_free(caught.value)


def test_redacting_a_non_str_never_puts_what_it_holds_in_a_traceback_local():
    with pytest.raises(TypeError) as caught:
        _Secret("sk-x").redact(f"echo sk-{PROBE}".encode())  # type: ignore[arg-type]

    assert_secret_free(caught.value)


@pytest.mark.parametrize(
    "act",
    [
        lambda secret: pickle.dumps(secret),
        copy.copy,
        copy.deepcopy,
        lambda secret: secret.__getstate__(),
        lambda secret: secret.prefixed(1),
    ],
    ids=["pickle", "copy", "deepcopy", "getstate", "non-str-prefix"],
)
def test_every_other_refusal_of_a_secret_holds_no_key_text(act):
    with pytest.raises(TypeError) as caught:
        act(_Secret(f"sk-{PROBE}"))

    assert_secret_free(caught.value)


# ---------------------------------------------------------------------------------------------------------------
# ModelProvider and check_provider
# ---------------------------------------------------------------------------------------------------------------

RESPONSE = ProviderResponse(
    content={"records": []}, raw_text='{"records":[]}', finish="complete", input_tokens=1, output_tokens=1
)


class AttributeProvider:
    """A provider whose members are plain attributes."""

    def __init__(self, **overrides):
        self.model_id = "fake:model@https://gw.example/v1"
        self.provider_kind = "fake"
        self.rate_key = "fake:https://gw.example/v1"
        self.model_family = "google"
        self.generation_params = {"temperature": 0.0, "seed": 0}
        for name, value in overrides.items():
            setattr(self, name, value)

    def transform_schema(self, schema):
        return dict(schema)

    def generate(self, request):
        return RESPONSE


class PropertyProvider:
    """A provider whose members are read-only properties."""

    @property
    def model_id(self):
        return "fake:model"

    @property
    def provider_kind(self):
        return "fake"

    @property
    def rate_key(self):
        return "fake"

    @property
    def model_family(self):
        return "unknown"

    @property
    def generation_params(self):
        return {}

    def transform_schema(self, schema):
        return schema

    def generate(self, request):
        return RESPONSE


def test_the_members_are_the_seven_of_the_protocol():
    assert PROVIDER_MEMBERS == (
        "model_id",
        "provider_kind",
        "rate_key",
        "model_family",
        "generation_params",
        "transform_schema",
        "generate",
    )


@pytest.mark.parametrize("cls", [AttributeProvider, PropertyProvider])
def test_a_complete_provider_is_a_model_provider_and_passes(cls):
    provider = cls()

    assert isinstance(provider, ModelProvider)
    assert check_provider(provider) is provider


def _without(member):
    """A provider class lacking one member."""
    members = {
        name: value for name, value in vars(PropertyProvider).items() if name in PROVIDER_MEMBERS and name != member
    }
    return type(f"Without_{member}", (), members)


@pytest.mark.parametrize("member", PROVIDER_MEMBERS)
def test_a_provider_lacking_a_member_is_refused_naming_it(member):
    provider = _without(member)()

    assert not isinstance(provider, ModelProvider)
    with pytest.raises(RationalizationError, match=member):
        check_provider(provider)


@pytest.mark.parametrize("member", ["model_id", "provider_kind", "rate_key"])
@pytest.mark.parametrize(
    "value",
    [1, None, b"x", "", "   ", "a\nb", "a\rb", "a b", "a\x00b", "a\tb", "a\ud800", "x" * (IDENTITY_MAX_CHARS + 1)],
)
def test_a_bad_identity_is_refused_naming_it(member, value):
    with pytest.raises(RationalizationError, match=member):
        check_provider(AttributeProvider(**{member: value}))


@pytest.mark.parametrize("member", ["model_id", "provider_kind", "rate_key"])
def test_an_identity_of_the_limit_passes(member):
    provider = AttributeProvider(**{member: "x" * IDENTITY_MAX_CHARS})

    assert check_provider(provider) is provider


class Lineage(str):
    """A str subclass: a family must be exactly a str."""


def test_the_unknown_family_is_unknown_and_valid():
    assert UNKNOWN_FAMILY == "unknown"
    assert family_problem(UNKNOWN_FAMILY) is None


@pytest.mark.parametrize(
    "value", ["google", "anthropic", "openai", "meta", "mistral", "deepseek", "qwen", "x" * 32, "a-1", "-", "0"]
)
def test_a_family_of_lower_case_letters_digits_and_hyphens_passes(value):
    assert family_problem(value) is None
    provider = AttributeProvider(model_family=value)

    assert check_provider(provider) is provider


BAD_FAMILIES = [
    1,
    None,
    b"zorgleprobe",
    Lineage("zorgleprobe"),
    "",
    "x" * 33,
    "Zorgleprobe",
    "zorgle probe",
    "zorgleprobe\n",
    "zorgle_probe",
    "zorgle.probe",
    "zorgle/probe",
    "zörgleprobe",
    "zorgleprobe\u200b",
    "ｚorgleprobe",
]


@pytest.mark.parametrize("value", BAD_FAMILIES)
def test_family_problem_refuses_anything_but_1_to_32_of_a_z_0_9_and_hyphen_never_quoting_it(value):
    problem = family_problem(value)

    assert isinstance(problem, str)
    assert "orgle" not in problem


@pytest.mark.parametrize("value", BAD_FAMILIES)
def test_a_bad_family_is_refused_naming_the_member_never_its_value(value):
    with pytest.raises(RationalizationError, match="model_family") as caught:
        check_provider(AttributeProvider(model_family=value))

    assert "orgle" not in str(caught.value)


def test_family_problem_names_what_a_family_must_be():
    assert family_problem(1) == "must be exactly a str, got int"
    assert family_problem(Lineage("google")) == "must be exactly a str, got Lineage"
    assert family_problem("Google") == (
        "must be 1-32 of a-z, 0-9 and '-': the model's vendor lineage (google, anthropic, openai, meta, ...) or unknown"
    )


@pytest.mark.parametrize(
    "params", [[], "temperature=0", None, {1: "x"}, {"t": float("nan")}, {"t": (1, 2)}, {"t": {1, 2}}, {"t": object()}]
)
def test_generation_params_that_are_not_exactly_json_are_refused(params):
    with pytest.raises(RationalizationError, match="generation_params"):
        check_provider(AttributeProvider(generation_params=params))


@pytest.mark.parametrize("member", ["transform_schema", "generate"])
def test_a_method_that_is_not_callable_is_refused_naming_it(member):
    with pytest.raises(RationalizationError, match=member):
        check_provider(AttributeProvider(**{member: "not callable"}))


class DynamicProvider:
    """Answers every member through __getattr__."""

    def __getattr__(self, name):
        if name in ("model_id", "provider_kind", "rate_key", "model_family"):
            return "x"
        if name == "generation_params":
            return {}
        if name in ("transform_schema", "generate"):
            return lambda *args: None
        raise AttributeError(name)


@pytest.mark.parametrize(
    "obj", [DynamicProvider(), mock.Mock(), mock.MagicMock()], ids=["getattr", "mock", "magicmock"]
)
def test_members_answered_by_getattr_are_refused_as_not_real_attributes(obj):
    with pytest.raises(RationalizationError, match="not real attributes") as caught:
        check_provider(obj)

    assert "model_id" in str(caught.value)


class RaisingProvider(PropertyProvider):
    @property
    def model_id(self):
        raise RuntimeError("model_id lookup failed: sk-hunter2")


class RaisingParams(PropertyProvider):
    @property
    def generation_params(self):
        raise KeyError("sk-hunter2")


class RaisingFamily(PropertyProvider):
    @property
    def model_family(self):
        raise LookupError("family lookup failed: sk-hunter2")


@pytest.mark.parametrize(
    ("cls", "member"),
    [(RaisingProvider, "model_id"), (RaisingParams, "generation_params"), (RaisingFamily, "model_family")],
)
def test_a_property_that_raises_is_refused_naming_it_without_its_message(cls, member):
    with pytest.raises(RationalizationError, match=member) as caught:
        check_provider(cls())

    assert "hunter2" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_a_provider_error_carries_a_status_and_a_detail():
    bare = ProviderError()
    full = ProviderError(418, "line\none")

    assert (bare.status_code, bare.detail) == (None, "")
    assert (full.status_code, full.detail) == (418, "line one")
    assert "418" in str(full)
    copy_ = pickle.loads(pickle.dumps(full))
    assert (type(copy_), copy_.status_code, copy_.detail) == (ProviderError, 418, "line one")


@pytest.mark.parametrize("obj", ["openai:gpt-4o", 1, None, object()])
def test_something_that_is_no_provider_is_refused(obj):
    with pytest.raises(RationalizationError, match="ModelProvider"):
        check_provider(obj)


def test_a_bad_identity_is_never_echoed():
    with pytest.raises(RationalizationError) as caught:
        check_provider(AttributeProvider(model_id="sk-hunter2\nsecond line"))

    assert "hunter2" not in str(caught.value)


@pytest.mark.parametrize(
    ("value", "problem"),
    [
        ("ok", None),
        ("", "blank"),
        ("a\nb", "line"),
        (1, "str"),
        ("x" * (IDENTITY_MAX_CHARS + 1), str(IDENTITY_MAX_CHARS)),
    ],
)
def test_identity_problem_says_what_is_wrong(value, problem):
    found = identity_problem(value)

    assert found is None if problem is None else problem in found


def test_a_provider_is_called_with_a_model_request():
    request = ModelRequest(
        unit_id="doc:p0001_e000-p0001_e001",
        model_id="fake:model",
        system_prompt="rules",
        user_envelope="payload",
        json_schema={"type": "object"},
        max_output_tokens=16,
        timeout_s=1.0,
    )

    assert check_provider(PropertyProvider()).generate(request) is RESPONSE


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_the_package_and_nothing_inside_functions():
    tree = ast.parse(Path(base_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    foreign = [
        m
        for m in modules
        if not m.startswith("datagrunt.rationalization.") and m.split(".")[0] not in sys.stdlib_module_names
    ]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert foreign == []
    assert [m for m in modules if m.startswith("datagrunt.rationalization.providers.http")] == []


@pytest.mark.parametrize(
    "module",
    [
        "datagrunt.rationalization.providers",
        "datagrunt.rationalization.providers.base",
        "datagrunt.rationalization.providers.callable",
    ],
)
def test_importing_the_providers_loads_neither_pydantic_nor_httpx(module):
    code = (
        f"import json, sys; import {module}; "
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('pydantic', 'httpx') "
        "or m == 'datagrunt.rationalization.providers.http')))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []


def test_the_providers_package_imports_only_base_and_callable():
    init = Path(base_module.__file__).with_name("__init__.py")
    tree = ast.parse(init.read_text(encoding="utf-8"))
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    names = [alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names]

    assert sorted(set(modules)) == [
        "datagrunt.rationalization.providers.base",
        "datagrunt.rationalization.providers.callable",
    ]
    assert "http" not in names


def test_the_providers_package_exports_the_public_names():
    code = (
        "import json; import datagrunt.rationalization.providers as p; "
        "from datagrunt.rationalization.providers import base, callable as c; "
        "print(json.dumps({'all': sorted(p.__all__), 'same': [p.ModelProvider is base.ModelProvider, "
        "p.CallableProvider is c.CallableProvider, p.ProviderError is base.ProviderError, "
        "p.TransientProviderError is base.TransientProviderError, "
        "p.PermanentProviderError is base.PermanentProviderError]}))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    assert result["all"] == sorted(
        ["CallableProvider", "ModelProvider", "PermanentProviderError", "ProviderError", "TransientProviderError"]
    )
    assert result["same"] == [True] * 5


def test_importing_the_rationalization_package_does_not_import_the_providers():
    code = (
        "import json, sys; import datagrunt.rationalization; "
        "print(json.dumps(sorted(m for m in sys.modules if '.providers' in m)))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []
