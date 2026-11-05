"""Tests for ``datagrunt.rationalization._canonical.strict_loads`` and its hooks: the one strict JSON parser.

``strict_loads`` is ``json.loads`` with NaN and Infinity refused, a number that overflows a float refused and a
repeated key refused, each a ``ValueError``; it raises what ``json.loads`` raises otherwise, and the caller decides
what a failure means. It is the only definition: the HTTP helper reads reply bodies and ``parse_json_object`` reads a
model's text with it, and the cache reads its entries with its NaN and repeated-key hooks (the cache leaves an
overflowing number to its field checks, so such an entry stays ``invalid_meta`` / ``invalid_response``).
"""

import ast
import json
from pathlib import Path

import pytest

from datagrunt.rationalization import _canonical as canonical_module
from datagrunt.rationalization import cache as cache_module
from datagrunt.rationalization._canonical import (
    canonical_json,
    finite_float,
    refuse_constant,
    sha256_hex,
    strict_loads,
    unique_keys,
)
from datagrunt.rationalization.cache import CACHE_FORMAT, CacheMeta
from datagrunt.rationalization.providers import http as http_module
from datagrunt.rationalization.providers import responses as responses_module
from datagrunt.rationalization.providers.responses import parse_json_object
from datagrunt.rationalization.wire import ProviderResponse

STRICT_REFUSALS = [
    '{"a": NaN}',
    '{"a": Infinity}',
    '{"a": -Infinity}',
    '{"a": 1e999}',
    '{"a": [-1e999]}',
    '{"a": 1, "a": 2}',
    '{"x": {"b": 0, "b": 0}}',
]
HOOK_NAMES = {"refuse_constant", "finite_float", "unique_keys", "strict_loads"}
CACHE_KEY = "a" * 64


def cache_entry(**response_changes: object) -> str:
    """A valid cache entry's text for :data:`CACHE_KEY`, its response fields changed."""
    response = {"content": {"x": 1}, "raw_text": "raw", "finish": "complete", "input_tokens": 10, "output_tokens": 5}
    meta = CacheMeta(
        model_id="anthropic:claude-x",
        unit_id="doc:p0001_e000-p0001_e000",
        doc_id="doc",
        schema_name="Order",
        schema_version="1.0",
        schema_sha256=sha256_hex("schema"),
        prompt_sha256=sha256_hex("prompt"),
        latency_ms=12.5,
        created_at="2026-11-04T10:00:00.123456+00:00",
    )
    entry = {
        "format": CACHE_FORMAT,
        "key": CACHE_KEY,
        "response": ProviderResponse(**{**response, **response_changes}).to_json(),
        "meta": meta.to_json(),
    }
    return canonical_json(entry)


# ---------------------------------------------------------------------------------------------------------------
# strict_loads and its hooks
# ---------------------------------------------------------------------------------------------------------------


class TestStrictLoads:
    @pytest.mark.parametrize(
        "text",
        ['{"a": [1, 2.5, "x", true, null], "b": {}}', '{"small": 1e-999, "neg": -0}', "[1]", '"s"', "3", "null"],
    )
    def test_gives_what_json_loads_gives(self, text):
        assert strict_loads(text) == json.loads(text)

    @pytest.mark.parametrize("text", ['{"a": NaN}', "Infinity", "[-Infinity]"])
    def test_refuses_nan_and_infinity(self, text):
        with pytest.raises(ValueError, match="is not JSON"):
            strict_loads(text)

    @pytest.mark.parametrize("text", ["1e999", "[-1e999]", '{"a": 1E400}', "1" + "0" * 400 + ".0"])
    def test_refuses_a_number_that_overflows_a_float(self, text):
        with pytest.raises(ValueError, match="overflows a float") as raised:
            strict_loads(text)
        assert "999" not in str(raised.value) and "000" not in str(raised.value)  # the text is never echoed

    @pytest.mark.parametrize("text", ['{"a": 1, "a": 2}', '{"a": 1, "a": 1}', '[{"x": {"b": 0, "b": 0}}]'])
    def test_refuses_a_repeated_key(self, text):
        with pytest.raises(ValueError, match="repeats a key"):
            strict_loads(text)

    @pytest.mark.parametrize(
        "text", ["", "{", '{"a": }', "{'a': 1}", '{"a": 1} x', '\N{ZERO WIDTH NO-BREAK SPACE}{"a": 1}']
    )
    def test_raises_what_json_loads_raises_for_invalid_json(self, text):
        with pytest.raises(json.JSONDecodeError):
            json.loads(text)
        with pytest.raises(json.JSONDecodeError):
            strict_loads(text)

    def test_raises_what_json_loads_raises_for_deep_nesting_and_huge_integers(self):
        for text in ["[" * 200_000 + "]" * 200_000, "1" * 5000]:
            with pytest.raises((ValueError, RecursionError)) as plain:
                json.loads(text)
            with pytest.raises((ValueError, RecursionError)) as strict:
                strict_loads(text)
            assert type(strict.value) is type(plain.value)

    @pytest.mark.parametrize("value", [None, b'{"a": 1}', 1])
    def test_refuses_a_non_str(self, value):
        with pytest.raises(TypeError, match="text must be a str"):
            strict_loads(value)

    def test_the_hooks_alone(self):
        assert finite_float("1.5") == 1.5
        assert finite_float("1e-999") == 0.0
        assert unique_keys([("a", 1), ("b", 2)]) == {"a": 1, "b": 2}
        with pytest.raises(ValueError, match="overflows a float"):
            finite_float("-1e999")
        with pytest.raises(ValueError, match="repeats a key"):
            unique_keys([("a", 1), ("a", 1)])
        with pytest.raises(ValueError, match="NaN is not JSON"):
            refuse_constant("NaN")


# ---------------------------------------------------------------------------------------------------------------
# One definition, used by every caller
# ---------------------------------------------------------------------------------------------------------------


def test_every_caller_uses_the_one_definition():
    assert responses_module.strict_loads is canonical_module.strict_loads
    assert http_module.strict_loads is canonical_module.strict_loads
    assert cache_module.refuse_constant is canonical_module.refuse_constant
    assert cache_module.unique_keys is canonical_module.unique_keys


@pytest.mark.parametrize("module", [responses_module, http_module, cache_module], ids=["responses", "http", "cache"])
def test_no_caller_keeps_a_copy_of_a_hook(module):
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    defined = {node.name.lstrip("_") for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}

    assert defined & HOOK_NAMES == set()


@pytest.mark.parametrize("text", STRICT_REFUSALS)
def test_the_http_reply_parser_and_parse_json_object_refuse_the_same_inputs(text):
    assert http_module._json_object(text.encode("utf-8")) is None
    assert parse_json_object(text) is None
    with pytest.raises(ValueError):
        strict_loads(text)


@pytest.mark.parametrize("text", ['{"a": 1.5, "b": [true, null, "x"]}', '{"small": 1e-999, "neg": -0}', "{}"])
def test_the_http_reply_parser_and_parse_json_object_accept_the_same_object(text):
    assert http_module._json_object(text.encode("utf-8")) == parse_json_object(text) == strict_loads(text)


@pytest.mark.parametrize(
    ("old", "new", "reason"),
    [
        ('"input_tokens":10', '"input_tokens":NaN', "not_json"),
        ('"input_tokens":10', '"input_tokens":Infinity', "not_json"),
        ('"input_tokens":10', '"input_tokens":1e999', "invalid_response"),
        ('"latency_ms":12.5', '"latency_ms":1e999', "invalid_meta"),
        ('"content":{"x":1}', '"content":{"x":1e999}', "invalid_response"),
        ('"content":{"x":1}', '"content":{"x":1,"x":2}', "not_json"),
    ],
    ids=["nan", "infinity", "overflow-tokens", "overflow-latency", "overflow-content", "repeated-key"],
)
def test_the_cache_refuses_a_constant_or_a_repeated_key_as_not_json_and_an_overflow_by_field(old, new, reason):
    text = cache_entry()
    assert text.count(old) == 1

    assert cache_module._parse_entry(text.encode("utf-8"), CACHE_KEY).hit
    lookup = cache_module._parse_entry(text.replace(old, new).encode("utf-8"), CACHE_KEY)
    assert (lookup.hit, lookup.reason) == (False, reason)
