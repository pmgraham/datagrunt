"""Tests for ``datagrunt.rationalization.wire``: the request a provider gets and the response it returns (plan §3).

Both are frozen, slotted and checked as they are built: strings non-blank (``raw_text`` may be empty) and valid
Unicode, ``finish`` one of three, token counts ``None`` or non-negative ints, ``raw_text`` at most 8 MiB of UTF-8,
the JSON objects JSON (each held as its own canonical copy), ``max_output_tokens`` in 1-131,072 and ``timeout_s``
finite and positive. A response survives ``to_json`` / ``from_json`` exactly, through JSON text too.
"""

import ast
import dataclasses
import json
import math
import subprocess
import sys
from enum import IntEnum
from pathlib import Path
from types import MappingProxyType

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization import _canonical as canonical_module
from datagrunt.rationalization import wire as wire_module
from datagrunt.rationalization._canonical import canonical_json
from datagrunt.rationalization.wire import (
    FINISH_REASONS,
    MAX_OUTPUT_TOKENS,
    RAW_TEXT_MAX_BYTES,
    TOKEN_COUNT_MAX,
    ModelRequest,
    ProviderResponse,
)

TIMEOUT_S = 120
MIB = 1024 * 1024
SCHEMA = {"type": "object", "properties": {"records": {"type": "array", "items": {"type": "string"}}}}
REQUEST = {
    "unit_id": "doc:p0001_e000-p0001_e002",
    "model_id": "anthropic:claude-x",
    "system_prompt": "SYSTEM rules",
    "user_envelope": "<caller_context_x>null</caller_context_x> DOCUMENT TEXT",
    "json_schema": SCHEMA,
    "max_output_tokens": 8_192,
    "timeout_s": 60.0,
}
RESPONSE = {
    "content": {"records": [{"record": {"b": 1, "a": "é"}, "grounding_quotes": ["q"], "source_element_ids": []}]},
    "raw_text": '{"records": [...]} DOCUMENT ECHO',
    "finish": "complete",
    "input_tokens": 120,
    "output_tokens": 40,
}


class Small(IntEnum):
    ONE = 1


def request(**changes) -> ModelRequest:
    return ModelRequest(**{**REQUEST, **changes})


def response(**changes) -> ProviderResponse:
    return ProviderResponse(**{**RESPONSE, **changes})


# ---------------------------------------------------------------------------------------------------------------
# ModelRequest
# ---------------------------------------------------------------------------------------------------------------


class TestModelRequest:
    def test_fields_in_plan_order(self):
        assert [field.name for field in dataclasses.fields(ModelRequest)] == [*REQUEST, "images"]

    def test_frozen_and_slotted(self):
        built = request()

        with pytest.raises(dataclasses.FrozenInstanceError):
            built.model_id = "other"
        assert not hasattr(built, "__dict__")

    def test_holds_what_it_was_given(self):
        built = request()

        assert [getattr(built, name) for name in REQUEST] == list(REQUEST.values())
        assert built == request()

    @pytest.mark.parametrize("name", ["unit_id", "model_id", "system_prompt", "user_envelope"])
    def test_each_text_is_a_non_blank_encodable_str(self, name):
        with pytest.raises(TypeError, match=name):
            request(**{name: None})
        with pytest.raises(TypeError, match=name):
            request(**{name: b"bytes"})
        with pytest.raises(ValueError, match=name):
            request(**{name: " \n "})
        with pytest.raises(ValueError, match=name):
            request(**{name: "a\ud800"})

    @pytest.mark.parametrize("value", [None, [1], "{}", 3])
    def test_the_schema_must_be_a_mapping(self, value):
        with pytest.raises(TypeError, match="json_schema"):
            request(json_schema=value)

    @pytest.mark.parametrize(
        "value",
        [{"a": float("nan")}, {"a": {1, 2}}, {"a": "\ud800"}, {1: "a"}, {"a": (1, 2)}],
        ids=["nan", "set", "surrogate", "int-key", "tuple"],
    )
    def test_the_schema_must_be_a_json_object(self, value):
        with pytest.raises(ValueError, match="json_schema"):
            request(json_schema=value)

    def test_the_schema_is_its_own_copy(self):
        source = {"type": "object", "properties": {"a": {"type": "string"}}}
        built = request(json_schema=source)
        source["properties"]["a"]["type"] = "integer"

        assert built.json_schema == {"type": "object", "properties": {"a": {"type": "string"}}}
        assert request(json_schema=MappingProxyType({"type": "object"})).json_schema == {"type": "object"}

    @pytest.mark.parametrize("value", [True, 8.0, "8", None])
    def test_max_output_tokens_is_an_int(self, value):
        with pytest.raises(TypeError, match="max_output_tokens"):
            request(max_output_tokens=value)

    @pytest.mark.parametrize("value", [0, -1, MAX_OUTPUT_TOKENS + 1])
    def test_max_output_tokens_is_in_range(self, value):
        with pytest.raises(ValueError, match="max_output_tokens"):
            request(max_output_tokens=value)

    def test_max_output_tokens_bounds_are_inclusive(self):
        assert MAX_OUTPUT_TOKENS == 131_072
        assert request(max_output_tokens=1).max_output_tokens == 1
        assert request(max_output_tokens=MAX_OUTPUT_TOKENS).max_output_tokens == MAX_OUTPUT_TOKENS

    @pytest.mark.parametrize("value", [True, "60", None, 1j])
    def test_timeout_is_a_number(self, value):
        with pytest.raises(TypeError, match="timeout_s"):
            request(timeout_s=value)

    @pytest.mark.parametrize("value", [0, 0.0, -1.0, math.nan, math.inf, -math.inf, 10**400, -(10**400)])
    def test_timeout_is_finite_and_positive(self, value):
        with pytest.raises(ValueError, match="timeout_s"):
            request(timeout_s=value)

    def test_an_int_timeout_is_held_as_a_float(self):
        built = request(timeout_s=5)

        assert built.timeout_s == 5.0 and type(built.timeout_s) is float
        assert request(timeout_s=1e-3).timeout_s == 1e-3

    def test_the_repr_holds_no_prompt_payload_or_schema(self):
        text = repr(request())

        assert "SYSTEM" not in text and "DOCUMENT" not in text and "records" not in text
        assert REQUEST["unit_id"] in text and REQUEST["model_id"] in text


# ---------------------------------------------------------------------------------------------------------------
# ProviderResponse
# ---------------------------------------------------------------------------------------------------------------


class TestProviderResponse:
    def test_fields_in_plan_order(self):
        assert [field.name for field in dataclasses.fields(ProviderResponse)] == list(RESPONSE)

    def test_frozen_and_slotted(self):
        built = response()

        with pytest.raises(dataclasses.FrozenInstanceError):
            built.finish = "refused"
        assert not hasattr(built, "__dict__")

    @pytest.mark.parametrize("finish", ["complete", "truncated", "refused"])
    def test_each_finish(self, finish):
        assert FINISH_REASONS == ("complete", "truncated", "refused")
        assert response(finish=finish).finish == finish

    @pytest.mark.parametrize(("finish", "error"), [("length", ValueError), ("", ValueError), (None, TypeError)])
    def test_another_finish_is_refused(self, finish, error):
        with pytest.raises(error, match="finish"):
            response(finish=finish)

    @pytest.mark.parametrize("name", ["input_tokens", "output_tokens"])
    def test_token_counts(self, name):
        assert getattr(response(**{name: None}), name) is None
        assert getattr(response(**{name: 0}), name) == 0
        assert getattr(response(**{name: 10**12}), name) == 10**12
        assert getattr(response(**{name: TOKEN_COUNT_MAX}), name) == TOKEN_COUNT_MAX == 2**63 - 1
        for value in (True, False, 1.0, "1"):
            with pytest.raises(TypeError, match=name):
                response(**{name: value})
        for value in (-1, TOKEN_COUNT_MAX + 1, 10**400):  # an Int64 column holds every count
            with pytest.raises(ValueError, match=name):
                response(**{name: value})

    def test_raw_text_is_a_str(self):
        assert response(raw_text="").raw_text == ""
        for value in (None, b"x", 3):
            with pytest.raises(TypeError, match="raw_text"):
                response(raw_text=value)

    def test_raw_text_is_bounded_in_utf8_bytes(self):
        assert RAW_TEXT_MAX_BYTES == 8 * MIB
        assert len(response(raw_text="a" * RAW_TEXT_MAX_BYTES).raw_text) == RAW_TEXT_MAX_BYTES
        assert response(raw_text="é" * (RAW_TEXT_MAX_BYTES // 2)).raw_text[0] == "é"
        for value in ("a" * (RAW_TEXT_MAX_BYTES + 1), "é" * (RAW_TEXT_MAX_BYTES // 2) + "a"):
            with pytest.raises(ValueError, match="raw_text"):
                response(raw_text=value)

    def test_raw_text_must_be_valid_unicode(self):
        with pytest.raises(ValueError, match="raw_text"):
            response(raw_text="a\ud800b")

    @pytest.mark.parametrize("value", [[1], "{}", 3, ("a", 1)])
    def test_content_is_none_or_a_mapping(self, value):
        assert response(content=None).content is None
        with pytest.raises(TypeError, match="content"):
            response(content=value)

    @pytest.mark.parametrize(
        "value",
        [
            {"a": float("nan")},
            {"a": float("inf")},
            {"a": {1, 2}},
            {"a": object()},
            {"a": "\ud800"},
            {1: "a"},
            {"a": {2: "b"}},
            {"a": (1, 2)},
            {"a": [{"b": (1,)}]},
        ],
        ids=["nan", "inf", "set", "object", "surrogate", "int-key", "nested-int-key", "tuple", "nested-tuple"],
    )
    def test_content_must_be_exactly_a_json_object(self, value):
        with pytest.raises(ValueError, match="content"):
            response(content=value)

    def test_content_too_deep_is_a_value_error(self):
        nested: dict = {}
        for _ in range(100_000):
            nested = {"a": nested}

        with pytest.raises(ValueError, match="content"):
            response(content=nested)

    def test_a_recursion_error_while_copying_is_a_value_error(self, monkeypatch):
        # Where the parse or the comparison recurses deeper than the encoder (interpreters differ), it still
        # fails as a ValueError, never a bare RecursionError.
        monkeypatch.setattr(canonical_module, "canonical_json", lambda value, subject: "[" * 200_000 + "]" * 200_000)

        with pytest.raises(ValueError, match="content") as error:
            response(content={"a": 1})

        assert isinstance(error.value.__cause__, RationalizationError)
        assert isinstance(error.value.__cause__.__cause__, RecursionError)

    def test_content_is_its_own_canonical_copy(self):
        source = {"b": [1, {"d": 2, "c": 3}], "a": Small.ONE}
        built = response(content=source)
        source["b"].append(4)

        assert built.content == {"a": 1, "b": [1, {"c": 3, "d": 2}]}
        assert type(built.content["a"]) is int
        assert list(built.content) == ["a", "b"] and list(built.content["b"][1]) == ["c", "d"]
        assert response(content=MappingProxyType({"a": 1})).content == {"a": 1}

    def test_the_repr_holds_no_content_or_raw_text(self):
        text = repr(response())

        assert "DOCUMENT" not in text and "records" not in text and "é" not in text
        assert "complete" in text


class TestRoundTrip:
    def test_to_json_is_the_fields_in_order(self):
        built = response()
        data = built.to_json()

        assert list(data) == list(RESPONSE)
        assert data == {**RESPONSE, "content": built.content}
        assert canonical_json(data)  # JSON as it is

    def test_to_json_is_a_fresh_copy(self):
        built = response()
        data = built.to_json()
        data["content"]["records"].clear()

        assert built.content == RESPONSE["content"]
        assert built.to_json()["content"] == RESPONSE["content"]

    @pytest.mark.parametrize(
        "changes",
        [
            {},
            {"content": None, "finish": "truncated", "input_tokens": None, "output_tokens": None, "raw_text": ""},
            {"content": {"x": [0.1, -0.0, 1e300, 10**40, None, True, "é‮ "]}, "finish": "refused"},
            {"content": {}},
        ],
        ids=["full", "empty", "edge-values", "empty-object"],
    )
    def test_round_trip_is_lossless(self, changes):
        built = response(**changes)
        through_text = json.loads(canonical_json(built.to_json()))

        assert ProviderResponse.from_json(built.to_json()) == built
        assert ProviderResponse.from_json(through_text) == built
        assert ProviderResponse.from_json(through_text).to_json() == built.to_json()
        assert canonical_json(ProviderResponse.from_json(through_text).to_json()) == canonical_json(built.to_json())

    @settings(max_examples=200, deadline=None)
    @given(
        content=st.none()
        | st.dictionaries(
            st.text(max_size=8),
            st.recursive(
                st.none()
                | st.booleans()
                | st.integers()
                | st.floats(allow_nan=False, allow_infinity=False)
                | st.text(),
                lambda inner: st.lists(inner, max_size=4) | st.dictionaries(st.text(max_size=8), inner, max_size=4),
                max_leaves=20,
            ),
            max_size=5,
        ),
        raw_text=st.text(max_size=50),
        finish=st.sampled_from(["complete", "truncated", "refused"]),
        tokens=st.none() | st.integers(min_value=0, max_value=10**9),
    )
    def test_round_trip_property(self, content, raw_text, finish, tokens):
        built = ProviderResponse(
            content=content, raw_text=raw_text, finish=finish, input_tokens=tokens, output_tokens=tokens
        )
        again = ProviderResponse.from_json(json.loads(canonical_json(built.to_json())))

        assert again == built
        assert canonical_json(again.to_json()) == canonical_json(built.to_json())

    @pytest.mark.parametrize("data", [None, [1], "{}"])
    def test_from_json_needs_a_mapping(self, data):
        with pytest.raises(TypeError, match="ProviderResponse"):
            ProviderResponse.from_json(data)

    def test_from_json_needs_exactly_the_fields(self):
        data = response().to_json()
        missing = {name: value for name, value in data.items() if name != "finish"}

        with pytest.raises(ValueError, match="finish"):
            ProviderResponse.from_json(missing)
        with pytest.raises(ValueError, match="extra"):
            ProviderResponse.from_json({**data, "extra": 1})

    def test_from_json_checks_every_value(self):
        data = response().to_json()

        with pytest.raises(ValueError, match="finish"):
            ProviderResponse.from_json({**data, "finish": "length"})
        with pytest.raises(TypeError, match="input_tokens"):
            ProviderResponse.from_json({**data, "input_tokens": "1"})
        with pytest.raises(TypeError, match="content"):
            ProviderResponse.from_json({**data, "content": [1]})


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_its_siblings_and_nothing_inside_functions():
    tree = ast.parse(Path(wire_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    allowed = {"datagrunt.rationalization._canonical", "datagrunt.rationalization.exceptions"}

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert [m for m in modules if m not in allowed and m.split(".")[0] not in sys.stdlib_module_names] == []


def test_importing_the_module_loads_neither_pydantic_nor_httpx():
    code = (
        "import json, sys; import datagrunt.rationalization.wire; "
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('pydantic', 'httpx'))))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []


VALID_PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"


class TestModelRequestImages:
    def test_images_defaults_to_empty_tuple_and_accepts_valid_pngs(self):
        default_req = request()
        assert default_req.images == ()

        img_req = request(images=(VALID_PNG, VALID_PNG + b"1"))
        assert img_req.images == (VALID_PNG, VALID_PNG + b"1")
        assert "IHDR" not in repr(img_req)

    @pytest.mark.parametrize("bad", [[VALID_PNG], VALID_PNG, None, (123,)])
    def test_images_rejects_non_tuple_or_non_bytes(self, bad):
        with pytest.raises(TypeError, match="images"):
            request(images=bad)

    @pytest.mark.parametrize(
        "bad",
        [
            (b"",),
            (b"not-a-png",),
            (VALID_PNG,) * 65,
            (VALID_PNG + b"x" * (32 * MIB),),
        ],
    )
    def test_images_rejects_non_png_or_excess_count_or_bytes(self, bad):
        with pytest.raises(ValueError, match="images"):
            request(images=bad)
