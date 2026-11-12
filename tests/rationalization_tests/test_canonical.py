"""Tests for ``datagrunt.rationalization._canonical``: the canonical JSON and SHA-256 the schema hash and cache share.

Canonical JSON is ``json.dumps`` with sorted keys, compact separators, no ASCII escaping and no NaN, and it must
encode in UTF-8; anything else raises :class:`RationalizationError` chained from its cause. ``schema_sha256`` is
this module's hash of the wrapper's canonical JSON, byte for byte.
"""

import ast
import hashlib
import json
import subprocess
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import pytest
from pydantic import BaseModel

from datagrunt.rationalization import Key, RationalizationError
from datagrunt.rationalization import _canonical as canonical_module
from datagrunt.rationalization._canonical import canonical_copy, canonical_json, sha256_hex
from datagrunt.rationalization.schema import PreparedSchema, prepare, response_schema, schema_sha256

TIMEOUT_S = 120


class Part(BaseModel):
    number: Annotated[str, Key("Part")]
    name: str | None = None


class Line(BaseModel):
    line_no: Annotated[int, Key]
    part: Part
    quantity: Decimal


class Order(BaseModel):
    order_id: Annotated[str, Key]
    placed: date
    lines: list[Line]


def circular() -> dict:
    value: dict = {}
    value["self"] = value
    return value


def too_deep() -> dict:
    nested: dict = {}
    for _ in range(100_000):
        nested = {"a": nested}
    return nested


class TestCanonicalJson:
    def test_sorted_compact_unescaped(self):
        assert canonical_json({"b": [1, {"d": True, "c": None}], "a": "é"}) == '{"a":"é","b":[1,{"c":null,"d":true}]}'

    def test_insertion_order_is_irrelevant(self):
        assert canonical_json({"a": 1, "b": {"c": 2, "d": 3}}) == canonical_json({"b": {"d": 3, "c": 2}, "a": 1})

    @pytest.mark.parametrize(
        ("value", "text"),
        [([1, "x"], '[1,"x"]'), (None, "null"), ("é<", '"é<"'), (0.1, "0.1"), (10**30, str(10**30)), (True, "true")],
        ids=["list", "null", "string", "float", "big-int", "bool"],
    )
    def test_any_json_value(self, value, text):
        assert canonical_json(value) == text

    @pytest.mark.parametrize(
        ("value", "cause"),
        [
            ({"a": float("nan")}, ValueError),
            ({"a": float("inf")}, ValueError),
            ({"a": float("-inf")}, ValueError),
            ({"a": object()}, TypeError),
            ({"a": {1, 2}}, TypeError),
            ({"a": Decimal("1")}, TypeError),
            ({"a": "\ud800"}, UnicodeEncodeError),
            ({"\udfff": 1}, UnicodeEncodeError),
            ({1: "a", "b": 2}, TypeError),
            ({(1, 2): "a"}, TypeError),
            (circular(), ValueError),
            (too_deep(), RecursionError),
        ],
        ids=[
            "nan",
            "inf",
            "-inf",
            "object",
            "set",
            "decimal",
            "lone-surrogate-value",
            "lone-surrogate-key",
            "unsortable-keys",
            "tuple-key",
            "circular",
            "too-deep",
        ],
    )
    def test_what_is_not_canonical_json_raises_chained(self, value, cause):
        with pytest.raises(RationalizationError, match=r"^the value is not canonical JSON: ") as error:
            canonical_json(value)

        assert isinstance(error.value.__cause__, cause)

    def test_the_subject_names_what_failed(self):
        with pytest.raises(RationalizationError, match=r"^the response schema is not canonical JSON: "):
            canonical_json({"a": float("nan")}, subject="the response schema")

    @pytest.mark.parametrize(("subject", "error"), [(None, TypeError), (3, TypeError), ("  ", ValueError)])
    def test_the_subject_is_checked(self, subject, error):
        with pytest.raises(error, match="subject"):
            canonical_json({}, subject=subject)

    def test_ascii_fast_path_and_nested_surrogate_rejection(self):
        ascii_doc = {"z": [1, 2, 3], "a": {"flag": True, "empty": None, "text": "plain-ascii_123"}}
        assert canonical_json(ascii_doc) == '{"a":{"empty":null,"flag":true,"text":"plain-ascii_123"},"z":[1,2,3]}'

        with pytest.raises(RationalizationError, match=r"^the value is not canonical JSON: ") as val_err:
            canonical_json({"ascii_key": ["ok", {"nested": "prefix-\ud83d-suffix"}]})
        assert isinstance(val_err.value.__cause__, UnicodeEncodeError)


class TestCanonicalCopy:
    def test_the_parse_of_the_canonical_json(self):
        source = {"b": [1, {"d": 2.5, "c": None}], "a": "é"}
        copy = canonical_copy(source)

        assert copy == source and copy is not source
        assert list(copy) == ["a", "b"] and list(copy["b"][1]) == ["c", "d"]
        copy["b"].append(3)
        assert source["b"] == [1, {"d": 2.5, "c": None}]

    @pytest.mark.parametrize(
        "value",
        [{1: "x"}, {"a": {2: "y"}}, {"a": (1, 2)}, {True: "z"}, {None: 1}, {1.5: 1}, [(1,)], (1,)],
        ids=["int-key", "nested-int-key", "tuple", "bool-key", "none-key", "float-key", "nested-tuple", "top-tuple"],
    )
    def test_what_canonical_json_would_fold_is_refused(self, value):
        with pytest.raises(RationalizationError, match=r"^the value is not exactly JSON: "):
            canonical_copy(value)

    def test_what_is_not_canonical_json_is_refused(self):
        with pytest.raises(RationalizationError, match=r"^the schema is not canonical JSON: "):
            canonical_copy({"a": float("nan")}, subject="the schema")

    def test_a_recursion_error_while_parsing_is_chained(self, monkeypatch):
        monkeypatch.setattr(canonical_module, "canonical_json", lambda value, subject: "[" * 200_000 + "]" * 200_000)

        with pytest.raises(RationalizationError, match="not canonical JSON") as error:
            canonical_copy([1])

        assert isinstance(error.value.__cause__, RecursionError)


class TestSha256Hex:
    def test_the_digest_of_the_utf8_bytes(self):
        assert sha256_hex("abc") == hashlib.sha256(b"abc").hexdigest()
        assert sha256_hex("é") == hashlib.sha256("é".encode()).hexdigest()
        assert sha256_hex("") == hashlib.sha256(b"").hexdigest()

    @pytest.mark.parametrize("text", [None, b"abc", 3])
    def test_a_non_str_is_refused(self, text):
        with pytest.raises(TypeError, match="text must be a str"):
            sha256_hex(text)

    def test_a_lone_surrogate_raises_chained(self):
        with pytest.raises(RationalizationError, match="not valid Unicode") as error:
            sha256_hex("a\ud800")

        assert isinstance(error.value.__cause__, UnicodeEncodeError)


class TestSchemaUsesIt:
    def test_schema_sha256_is_the_digest_of_the_canonical_json(self):
        wrapper = response_schema(Order)

        assert schema_sha256(wrapper) == sha256_hex(canonical_json(wrapper))
        assert (
            schema_sha256(wrapper)
            == hashlib.sha256(
                json.dumps(wrapper, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            ).hexdigest()
        )

    def test_prepare_keeps_the_canonical_json_it_hashes(self):
        prepared = prepare(Order)

        assert prepared.wrapper_json == canonical_json(response_schema(Order))
        assert prepared.sha256 == sha256_hex(prepared.wrapper_json)

    def test_a_prepared_schema_of_invalid_unicode_is_still_a_value_error(self):
        # Before the move, hashing a lone surrogate raised UnicodeEncodeError, a ValueError: it still is one.
        prepared = prepare(Order)

        with pytest.raises(ValueError, match="not valid Unicode"):
            PreparedSchema(model=Order, spec=prepared.spec, wrapper_json="\ud800", sha256=prepared.sha256)


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_the_error_and_nothing_inside_functions():
    tree = ast.parse(Path(canonical_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    allowed = {"datagrunt.rationalization.exceptions"}
    assert [m for m in modules if m not in allowed and m.split(".")[0] not in sys.stdlib_module_names] == []


def test_importing_the_module_loads_neither_pydantic_nor_httpx():
    code = (
        "import json, sys; import datagrunt.rationalization._canonical; "
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('pydantic', 'httpx'))))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []
