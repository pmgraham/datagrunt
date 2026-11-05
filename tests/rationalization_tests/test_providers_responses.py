"""Tests for ``datagrunt.rationalization.providers.responses``: what every adapter does to a model's answer.

Model output is untrusted data, never repaired: lone surrogates become R8 ``[U+XXXX]`` tokens, ``raw_text`` is cut
to 8 MiB of UTF-8 at a character boundary with a `` [truncated]`` marker, a parsed JSON value becomes an exact,
escaped copy or ``None`` (non-finite floats, non-``str`` keys, tuples, depth past 64, more than 1,000,000 nodes, a
key collision created by escaping), a model's JSON text is parsed strictly (no NaN, no overflow, no repeated key, no
fence, no prose), and :func:`response` builds a :class:`ProviderResponse` that never fails on junk usage counts.
"""

import ast
import json
import math
import re
import subprocess
import sys
import threading
from decimal import Decimal
from enum import IntEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datagrunt.rationalization._canonical import canonical_copy, canonical_json
from datagrunt.rationalization.providers import responses as responses_module
from datagrunt.rationalization.providers.responses import (
    JSON_MAX_DEPTH,
    JSON_MAX_NODES,
    TRUNCATION_MARKER,
    bounded_raw_text,
    parse_json_object,
    response,
    safe_json,
    safe_text,
)
from datagrunt.rationalization.wire import FINISH_REASONS, RAW_TEXT_MAX_BYTES, TOKEN_COUNT_MAX, ProviderResponse

TIMEOUT_S = 120
MARKER_BYTES = len(TRUNCATION_MARKER.encode("utf-8"))
CUT = RAW_TEXT_MAX_BYTES - MARKER_BYTES
TOKEN = re.compile(r"\[U\+D[89A-F][0-9A-F]{2}\]")


class Small(IntEnum):
    ONE = 1


class NotJson(Exception):
    """The reference copy's refusal."""


def escaped(text: str) -> str:
    """The reference escape: each surrogate code point as its own token, everything else verbatim."""
    return "".join(f"[U+{ord(char):04X}]" if 0xD800 <= ord(char) <= 0xDFFF else char for char in text)


def reference_copy(value: Any, depth: int = 1) -> object:
    """The reference (recursive) form of ``safe_json``'s copy, for small trees; raises :class:`NotJson`."""
    kind = type(value)
    if kind is str:
        return escaped(value)
    if value is None or kind is bool or kind is int:
        return value
    if kind is float:
        if not math.isfinite(value):
            raise NotJson
        return value
    if kind not in (dict, list) or depth > JSON_MAX_DEPTH:
        raise NotJson
    if kind is list:
        return [reference_copy(item, depth + 1) for item in value]
    copy: dict[str, object] = {}
    for key, item in value.items():
        if type(key) is not str or escaped(key) in copy:
            raise NotJson
        copy[escaped(key)] = reference_copy(item, depth + 1)
    return copy


def reference(value: object) -> object:
    """What ``safe_json(value)`` must equal (``None`` when refused)."""
    if type(value) is not dict:
        return None
    try:
        return reference_copy(value)
    except NotJson:
        return None


def nested(containers: int, leaf: object = 0) -> dict[str, object]:
    """A JSON object chaining ``containers`` containers (the root counted), alternately objects and arrays."""
    value: object = leaf
    for level in range(containers - 1):
        value = [value] if level % 2 else {"k": value}
    return {"k": value}


def accepted(content: dict[str, object]) -> ProviderResponse:
    """Build a response around ``content``, which must be exactly JSON and kept equal."""
    assert canonical_copy(content) == content
    built = ProviderResponse(content=content, raw_text="", finish="complete", input_tokens=None, output_tokens=None)
    assert built.content == content
    assert canonical_json(built.content) == canonical_json(content)
    return built


SURROGATES = st.characters(min_codepoint=0xD800, max_codepoint=0xDFFF, exclude_categories=())
CHARS = st.characters(exclude_categories=()) | SURROGATES | st.sampled_from(["😀", "é", "[", "]", "U", "+", "D", "8"])
TEXT = st.text(CHARS, max_size=10) | st.sampled_from(["", "\ud800", "[U+D800]", "\ud83d" + "\ude00", "\ude00\ud83d"])
FLOATS = st.floats() | st.sampled_from([math.nan, math.inf, -math.inf, -0.0, 1e308, 5e-324])
SCALARS = st.none() | st.booleans() | st.integers() | FLOATS | TEXT
KEYS = TEXT | st.integers() | st.none() | st.booleans() | st.floats()
TREES = st.recursive(
    SCALARS,
    lambda inner: (
        st.lists(inner, max_size=4)
        | st.dictionaries(TEXT, inner, max_size=4)
        | st.dictionaries(KEYS, inner, max_size=3)
        | st.tuples(inner)
    ),
    max_leaves=24,
)
ROOTS = st.dictionaries(TEXT, TREES, max_size=5) | TREES
JUNK = st.none() | st.booleans() | st.integers() | FLOATS | TEXT | st.lists(st.integers(), max_size=2) | st.just(b"x")
JSON_PIECES = ["{", "}", "[", "]", '"', ":", ",", "0", "1.5", "e9", "-", "NaN", "Infinity", "true", "null", " "]
JSON_PIECES += ['"a"', '"\\ud800"', '"\\udc00\\ud800"', "1e999", "```", "\n"]


# ---------------------------------------------------------------------------------------------------------------
# safe_text
# ---------------------------------------------------------------------------------------------------------------


class TestSafeText:
    @pytest.mark.parametrize(
        "text",
        ["", "plain", "é😀", "\u202ebidi\u0007control\u200dzwj\ufeff", "[U+D800] written out", "a\r\nb", "\ufffd"],
    )
    def test_text_without_a_surrogate_is_verbatim(self, text):
        assert safe_text(text) == text

    def test_the_code_points_beside_the_surrogate_range_are_verbatim(self):
        beside = chr(0xD7FF) + chr(0xE000) + chr(0xFFFF) + chr(0x10000)

        assert safe_text(beside) == beside

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("\ud800abc", "[U+D800]abc"),
            ("a\udfffb", "a[U+DFFF]b"),
            ("abc\udbff", "abc[U+DBFF]"),
            ("\udc00", "[U+DC00]"),
            ("\udabc", "[U+DABC]"),
            ("\ud800\ud800", "[U+D800][U+D800]"),
        ],
        ids=["start", "middle", "end", "alone", "uppercase-hex", "repeated"],
    )
    def test_a_lone_surrogate_becomes_an_r8_token(self, text, expected):
        assert safe_text(text) == expected
        safe_text(text).encode("utf-8")

    def test_a_json_escape_pair_parses_to_one_code_point_and_stays(self):
        emoji = json.loads('"\\ud83d\\ude00"')

        assert emoji == "😀" and len(emoji) == 1
        assert safe_text(emoji) == "😀"

    def test_a_lone_escape_parses_to_a_lone_surrogate_and_is_escaped(self):
        lone = json.loads('"\\ud83d"')

        assert len(lone) == 1 and ord(lone) == 0xD83D
        assert safe_text(lone) == "[U+D83D]"

    def test_adjacent_surrogates_in_a_str_are_two_lone_surrogates(self):
        # A str holds code points: a high then a low surrogate is never one character, so each is escaped alone.
        pair = "\ud83d" + "\ude00"

        assert len(pair) == 2
        assert safe_text(pair) == "[U+D83D][U+DE00]"
        assert safe_text(json.loads('"\\ude00\\ud83d"')) == "[U+DE00][U+D83D]"
        assert safe_text(json.loads('"\\ud83d\\ud83d\\ude00"')) == "[U+D83D]😀"

    @pytest.mark.parametrize("value", [None, b"\xed\xa0\x80", 1, ["a"]])
    def test_refuses_a_non_str(self, value):
        with pytest.raises(TypeError, match="text must be a str"):
            safe_text(value)

    @settings(max_examples=300, deadline=None)
    @given(text=st.text(CHARS, max_size=40))
    def test_matches_the_reference_and_always_encodes(self, text):
        result = safe_text(text)

        assert result == escaped(text)
        result.encode("utf-8")


# ---------------------------------------------------------------------------------------------------------------
# bounded_raw_text
# ---------------------------------------------------------------------------------------------------------------


class TestBoundedRawText:
    def test_the_marker_is_fixed(self):
        assert TRUNCATION_MARKER == " [truncated]"

    @pytest.mark.parametrize("text", ["", "abc", "é😀"])
    def test_a_text_within_the_bound_is_unchanged(self, text):
        assert bounded_raw_text(text) == text

    def test_a_text_within_the_bound_is_escaped(self):
        assert bounded_raw_text("a\ud800b") == "a[U+D800]b"

    def test_exactly_at_the_bound_is_unchanged(self):
        ascii_text = "a" * RAW_TEXT_MAX_BYTES
        two_byte = "é" * (RAW_TEXT_MAX_BYTES // 2)
        tokens = "\ud800" * (RAW_TEXT_MAX_BYTES // 8)

        assert bounded_raw_text(ascii_text) == ascii_text
        assert bounded_raw_text(two_byte) == two_byte
        assert bounded_raw_text(tokens) == "[U+D800]" * (RAW_TEXT_MAX_BYTES // 8)

    def test_one_byte_over_is_cut_with_the_marker(self):
        result = bounded_raw_text("a" * (RAW_TEXT_MAX_BYTES + 1))

        assert result == "a" * CUT + TRUNCATION_MARKER
        assert len(result.encode("utf-8")) == RAW_TEXT_MAX_BYTES

    def test_one_byte_over_through_escaping_is_cut(self):
        # Seven ASCII bytes short of the bound, then one surrogate: its token makes the text one byte too long.
        result = bounded_raw_text("a" * (RAW_TEXT_MAX_BYTES - 7) + "\ud800")

        assert result == "a" * CUT + TRUNCATION_MARKER

    @pytest.mark.parametrize("before_cut", [1, 2, 3])
    def test_a_four_byte_character_straddling_the_cut_is_left_out(self, before_cut):
        text = "a" * (CUT - before_cut) + "😀" + "b" * 100

        assert bounded_raw_text(text) == "a" * (CUT - before_cut) + TRUNCATION_MARKER

    def test_a_four_byte_character_ending_at_the_cut_is_kept(self):
        text = "a" * (CUT - 4) + "😀" + "b" * 100

        assert bounded_raw_text(text) == "a" * (CUT - 4) + "😀" + TRUNCATION_MARKER

    def test_a_four_byte_character_straddling_the_bound_is_cut(self):
        result = bounded_raw_text("a" * (RAW_TEXT_MAX_BYTES - 2) + "😀")

        assert result == "a" * CUT + TRUNCATION_MARKER

    @pytest.mark.parametrize("before_cut", range(1, 8))
    def test_an_escape_token_straddling_the_cut_is_left_out_whole(self, before_cut):
        text = "a" * (CUT - before_cut) + "\ud800" + "b" * 100

        assert bounded_raw_text(text) == "a" * (CUT - before_cut) + TRUNCATION_MARKER

    @pytest.mark.parametrize("before_cut", [0, 8])
    def test_an_escape_token_beside_the_cut_is_kept_or_dropped_whole(self, before_cut):
        text = "a" * (CUT - before_cut) + "\ud800" + "b" * 100
        kept = "[U+D800]" if before_cut == 8 else ""

        assert bounded_raw_text(text) == "a" * (CUT - before_cut) + kept + TRUNCATION_MARKER

    def test_a_token_written_out_by_the_model_is_never_cut_either(self):
        text = "a" * (CUT - 3) + "[U+DABC]" + "b" * 100

        assert bounded_raw_text(text) == "a" * (CUT - 3) + TRUNCATION_MARKER

    @pytest.mark.parametrize(
        ("unit", "copies", "kept", "kept_bytes"),
        [
            ("\N{REPLACEMENT CHARACTER}", RAW_TEXT_MAX_BYTES, "\N{REPLACEMENT CHARACTER}", 3),
            ("\ud800", RAW_TEXT_MAX_BYTES, "[U+D800]", 8),
            ("é", 3 * RAW_TEXT_MAX_BYTES, "é", 2),
        ],
        ids=["replacement-chars", "surrogates", "two-byte"],
    )
    def test_a_text_far_past_the_bound_is_cut_to_the_bound(self, unit, copies, kept, kept_bytes):
        # An HttpReply's text, decoded with errors replaced, can take about three times the body in UTF-8.
        result = bounded_raw_text(unit * copies)

        assert result == kept * (CUT // kept_bytes) + TRUNCATION_MARKER
        assert len(result.encode("utf-8")) <= RAW_TEXT_MAX_BYTES
        ProviderResponse(content=None, raw_text=result, finish="complete", input_tokens=None, output_tokens=None)

    @pytest.mark.parametrize("value", [None, b"abc", 3])
    def test_refuses_a_non_str(self, value):
        with pytest.raises(TypeError, match="text must be a str"):
            bounded_raw_text(value)

    @settings(max_examples=400, deadline=None)
    @given(text=st.text(CHARS, max_size=60) | st.lists(TEXT, max_size=12).map("".join), bound=st.integers(12, 64))
    def test_cuts_at_a_character_and_token_boundary_as_late_as_fits(self, text, bound):
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(responses_module, "RAW_TEXT_MAX_BYTES", bound)
            result = bounded_raw_text(text)
        safe = escaped(text)

        assert len(result.encode("utf-8")) <= bound
        if len(safe.encode("utf-8")) <= bound:
            assert result == safe
            return
        assert result.endswith(TRUNCATION_MARKER)
        head = result[: -len(TRUNCATION_MARKER)]
        end = len(head)
        assert safe.startswith(head)
        assert all(not match.start() < end < match.end() for match in TOKEN.finditer(safe))
        following = TOKEN.match(safe, end)
        piece = following.group(0) if following else safe[end]
        assert len((head + piece).encode("utf-8")) > bound - MARKER_BYTES


# ---------------------------------------------------------------------------------------------------------------
# safe_json
# ---------------------------------------------------------------------------------------------------------------


class TestSafeJson:
    def test_returns_an_equal_deep_copy(self):
        value = {"b": [1, 2.5, None, True, {"c": "é"}], "a": {"d": []}, "e": -0.0, "f": 10**40}
        copy = safe_json(value)

        assert copy == value and copy is not value
        assert copy["b"] is not value["b"] and copy["b"][4] is not value["b"][4]
        copy["b"].append(9)
        copy["a"]["d"].append(1)
        assert value == {"b": [1, 2.5, None, True, {"c": "é"}], "a": {"d": []}, "e": -0.0, "f": 10**40}
        accepted(copy)

    def test_escapes_every_string_and_every_key(self):
        value = {"\ud800k": ["\udfff", {"x\ud83d": "y\ude00"}], "plain": "😀"}

        assert safe_json(value) == {"[U+D800]k": ["[U+DFFF]", {"x[U+D83D]": "y[U+DE00]"}], "plain": "😀"}
        accepted(safe_json(value))

    def test_keeps_booleans_apart_from_numbers(self):
        copy = safe_json({"t": True, "one": 1, "f": False, "zero": 0, "n": None})

        assert canonical_json(copy) == '{"f":false,"n":null,"one":1,"t":true,"zero":0}'

    @pytest.mark.parametrize("value", [[], [{"a": 1}], "{}", 1, 1.5, True, None, ({"a": 1},), b"{}"])
    def test_a_value_that_is_not_an_object_is_none(self, value):
        assert safe_json(value) is None

    @pytest.mark.parametrize(
        "value",
        [
            {"a": math.nan},
            {"a": math.inf},
            {"a": [-math.inf]},
            {1: "x"},
            {None: "x"},
            {True: "x"},
            {(1,): "x"},
            {"a": (1,)},
            {"a": {1, 2}},
            {"a": b"x"},
            {"a": Decimal("1.5")},
            {"a": Small.ONE},
            {"a": object()},
            {"a": MappingProxyType({"b": 1})},
            {"a": 10**5000},
        ],
        ids=[
            "nan",
            "inf",
            "nested-minus-inf",
            "int-key",
            "none-key",
            "bool-key",
            "tuple-key",
            "tuple",
            "set",
            "bytes",
            "decimal",
            "int-enum",
            "object",
            "mapping-proxy",
            "int-past-the-digit-limit",
        ],
    )
    def test_a_value_that_cannot_be_made_exactly_json_is_none(self, value):
        assert safe_json(value) is None

    @pytest.mark.parametrize("root", [MappingProxyType({"a": 1}), type("Sub", (dict,), {})(a=1)])
    def test_a_root_that_is_not_exactly_a_dict_is_none(self, root):
        assert safe_json(root) is None

    def test_a_str_subclass_is_none_as_a_value_or_a_key(self):
        text = type("Text", (str,), {})

        assert safe_json({"a": text("x")}) is None
        assert safe_json({"a": [text("x")]}) is None
        assert safe_json({text("a"): 1}) is None

    @pytest.mark.parametrize(
        "value",
        [
            {"\ud800": 1, "[U+D800]": 2},
            {"x": [{"a\udfff": 1, "a[U+DFFF]": 2}]},
            {"[U+D83D][U+DE00]": 1, "\ud83d" + "\ude00": 2},
        ],
        ids=["root", "nested", "pair"],
    )
    def test_a_key_collision_created_by_escaping_is_none(self, value):
        assert safe_json(value) is None

    def test_distinct_escaped_keys_are_kept(self):
        value = {chr(0xD800): 1, chr(0xD801): 2, "[U+D802]": 3}  # chr(): ruff reads two lone-surrogate keys as one

        assert safe_json(value) == {"[U+D800]": 1, "[U+D801]": 2, "[U+D802]": 3}

    def test_depth_64_is_kept_and_65_is_none(self):
        assert JSON_MAX_DEPTH == 64
        assert safe_json(nested(64)) == nested(64)
        assert safe_json(nested(63, leaf=[])) == nested(63, leaf=[])
        assert safe_json(nested(65)) is None
        assert safe_json(nested(64, leaf=[])) is None
        assert safe_json(nested(64, leaf={})) is None
        accepted(safe_json(nested(64)))

    def test_a_deep_chain_is_none_without_recursing(self):
        assert safe_json(nested(100_000)) is None

    def test_a_circular_value_is_none(self):
        circular: dict[str, object] = {}
        circular["self"] = circular
        looped: list[object] = []
        looped.append(looped)

        assert safe_json(circular) is None
        assert safe_json({"a": looped}) is None

    def test_one_million_nodes_are_kept_and_one_more_is_none(self):
        assert JSON_MAX_NODES == 1_000_000
        # The root, the array and its items are each a node.
        assert safe_json({"a": [0] * (JSON_MAX_NODES - 2)}) == {"a": [0] * (JSON_MAX_NODES - 2)}
        assert safe_json({"a": [0] * (JSON_MAX_NODES - 1)}) is None
        assert safe_json({str(index): None for index in range(JSON_MAX_NODES)}) is None
        assert safe_json({str(index): None for index in range(JSON_MAX_NODES - 1)}) is not None

    def test_nested_containers_count_as_nodes(self):
        rows = JSON_MAX_NODES // 2 - 1  # the root and the array, then each row's object and its value
        many = {"a": [{"k": 0} for _ in range(rows)]}

        assert safe_json(many) is not None
        assert safe_json({"a": [*many["a"], {"k": 0}]}) is None

    def test_a_shared_value_counts_each_time_it_is_reached(self):
        shared: list[object] = [0]
        for _ in range(25):
            shared = [shared, shared]  # 2**26 nodes reached from a few dozen objects

        assert safe_json({"a": shared}) is None

    @settings(max_examples=400, deadline=None)
    @given(value=ROOTS)
    def test_never_raises_and_matches_the_reference(self, value):
        copy = safe_json(value)
        expected = reference(value)

        if expected is None:
            assert copy is None
            return
        assert copy is not None
        assert canonical_json(copy) == canonical_json(expected)
        accepted(copy)


# ---------------------------------------------------------------------------------------------------------------
# parse_json_object
# ---------------------------------------------------------------------------------------------------------------


class TestParseJsonObject:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ('{"a": 1, "b": [true, null, 2.5, "x"]}', {"a": 1, "b": [True, None, 2.5, "x"]}),
            (' \t\r\n{"a": {}}\r\n\t ', {"a": {}}),
            ("{}", {}),
            ('{"small": 1e-999, "neg": -0}', {"small": 0.0, "neg": 0}),
        ],
    )
    def test_a_json_object_parses(self, text, expected):
        parsed = parse_json_object(text)

        assert parsed == expected
        accepted(parsed)

    def test_lone_surrogate_escapes_are_escaped_and_pairs_are_decoded(self):
        parsed = parse_json_object('{"k\\ud800": "\\udfff", "e": "\\ud83d\\ude00", "r": "\\ude00\\ud83d"}')

        assert parsed == {"k[U+D800]": "[U+DFFF]", "e": "😀", "r": "[U+DE00][U+D83D]"}
        accepted(parsed)

    def test_a_raw_lone_surrogate_in_the_text_is_escaped(self):
        assert parse_json_object('{"a": "x\ud800"}') == {"a": "x[U+D800]"}

    @pytest.mark.parametrize(
        "text",
        ['{"a": NaN}', '{"a": Infinity}', '{"a": -Infinity}', '{"a": 1e999}', '{"a": [-1e999]}', '{"a": 1E400}'],
    )
    def test_non_finite_numbers_are_none(self, text):
        assert parse_json_object(text) is None

    @pytest.mark.parametrize("text", ['{"a": 1, "a": 2}', '{"x": {"a": 1, "a": 1}}', '{"x": [{"b": 0, "b": 0}]}'])
    def test_a_repeated_key_is_none(self, text):
        assert parse_json_object(text) is None

    def test_a_key_collision_created_by_escaping_is_none(self):
        assert parse_json_object('{"\\ud800": 1, "[U+D800]": 2}') is None

    @pytest.mark.parametrize(
        "text",
        [
            '```json\n{"a": 1}\n```',
            '```\n{"a": 1}\n```',
            'Here is the JSON:\n{"a": 1}',
            '{"a": 1}\nI hope this helps.',
            '{"a": 1}{"b": 2}',
            '\ufeff{"a": 1}',
            '\u00a0{"a": 1}',
            '\x0c{"a": 1}',
            '{"a": 1}\u3000',
        ],
        ids=["fence", "bare-fence", "prose-before", "prose-after", "two-objects", "bom", "nbsp", "form-feed", "ideo"],
    )
    def test_anything_around_the_object_but_json_whitespace_is_none(self, text):
        assert parse_json_object(text) is None

    @pytest.mark.parametrize("text", ["[]", '[{"a": 1}]', '"{}"', "1", "1.5", "true", "null"])
    def test_a_value_that_is_not_an_object_is_none(self, text):
        assert parse_json_object(text) is None

    @pytest.mark.parametrize("text", ["", " ", "{", '{"a": }', "{'a': 1}", '{"a": 1,}', '{"a": 01}', "{a: 1}"])
    def test_invalid_json_is_none(self, text):
        assert parse_json_object(text) is None

    @pytest.mark.parametrize("value", [None, b'{"a": 1}', bytearray(b"{}"), 1, {"a": 1}])
    def test_a_non_str_is_none(self, value):
        assert parse_json_object(value) is None

    def test_a_str_subclass_is_none(self):
        assert parse_json_object(type("Text", (str,), {})('{"a": 1}')) is None

    def test_an_integer_past_the_digit_limit_is_none(self):
        assert parse_json_object('{"a": ' + "1" * 5000 + "}") is None

    def test_depth_past_the_bound_is_none(self):
        assert parse_json_object(json.dumps(nested(64))) == nested(64)
        assert parse_json_object(json.dumps(nested(65))) is None

    @pytest.mark.parametrize("on_thread", [False, True], ids=["caller-thread", "worker-thread"])
    def test_nesting_past_the_recursion_limit_is_none(self, on_thread):
        texts = ["[" * 200_000 + "]" * 200_000, '{"a": ' + "[" * 200_000 + "]" * 200_000 + "}"]
        results: list[object] = []

        def parse() -> None:
            results.extend(parse_json_object(text) for text in texts)

        if on_thread:
            worker = threading.Thread(target=parse)
            worker.start()
            worker.join(TIMEOUT_S)
        else:
            parse()
        assert results == [None, None]

    def test_exactly_at_the_bound_parses_and_one_byte_over_is_none(self):
        body = '{"a": 1}'
        at_bound = body + " " * (RAW_TEXT_MAX_BYTES - len(body))

        assert parse_json_object(at_bound) == {"a": 1}
        assert parse_json_object(at_bound + " ") is None

    def test_the_bound_is_in_utf8_bytes(self):
        frame = len('{"a": ""}')
        fits = '{"a": "' + "é" * ((RAW_TEXT_MAX_BYTES - frame) // 2) + '"}'
        over = '{"a": "' + "é" * ((RAW_TEXT_MAX_BYTES - frame) // 2 + 1) + '"}'

        assert len(over) <= RAW_TEXT_MAX_BYTES
        assert parse_json_object(fits) is not None
        assert parse_json_object(over) is None

    def test_a_lone_surrogate_counts_three_bytes(self):
        frame = len('{"a": ""}')
        over = '{"a": "' + "\ud800" * ((RAW_TEXT_MAX_BYTES - frame) // 3 + 1) + '"}'

        assert len(over) <= RAW_TEXT_MAX_BYTES
        assert parse_json_object(over) is None

    def test_a_text_over_the_bound_is_never_parsed(self, monkeypatch):
        calls: list[str] = []

        def spy(text: str) -> object:
            calls.append(text)
            return {}

        monkeypatch.setattr(responses_module, "strict_loads", spy)

        assert parse_json_object('{"a": 1}' + " " * RAW_TEXT_MAX_BYTES) is None
        assert parse_json_object("é" * (RAW_TEXT_MAX_BYTES // 2 + 1)) is None
        assert calls == []
        assert parse_json_object('{"a": 1}') == {}  # vacuous otherwise: the spy is the parser
        assert calls == ['{"a": 1}']

    @settings(max_examples=400, deadline=None)
    @given(value=ROOTS, ensure_ascii=st.booleans(), indent=st.none() | st.integers(0, 2))
    def test_never_raises_and_agrees_with_safe_json(self, value, ensure_ascii, indent):
        text = json.dumps(value, ensure_ascii=ensure_ascii, allow_nan=True, indent=indent)
        parsed = parse_json_object(text)
        exact = safe_json(value)

        if parsed is not None:
            accepted(parsed)
        if exact is None:
            return
        # JSON text cannot keep a high surrogate escape before a low one apart: json.loads joins the two.
        assert parsed is not None
        assert canonical_json(parsed) == canonical_json(safe_json(json.loads(text)))
        if not ensure_ascii:
            assert canonical_json(parsed) == canonical_json(exact)

    def test_json_text_joins_a_high_and_a_low_surrogate_escape(self):
        two = "\ud83d" + "\ude00"

        assert safe_json({"k": two}) == {"k": "[U+D83D][U+DE00]"}
        assert parse_json_object(json.dumps({"k": two}, ensure_ascii=False)) == {"k": "[U+D83D][U+DE00]"}
        assert parse_json_object(json.dumps({"k": two})) == {"k": json.loads('"\\ud83d\\ude00"')}

    @settings(max_examples=300, deadline=None)
    @given(text=st.text(CHARS, max_size=40) | st.lists(st.sampled_from(JSON_PIECES), max_size=30).map("".join))
    def test_never_raises_on_any_text(self, text):
        parsed = parse_json_object(text)

        if parsed is not None:
            accepted(parsed)


# ---------------------------------------------------------------------------------------------------------------
# response
# ---------------------------------------------------------------------------------------------------------------


class TestResponse:
    def test_builds_a_provider_response(self):
        built = response(
            content={"records": ["\ud800"]}, raw_text="raw\udfff", finish="truncated", input_tokens=12, output_tokens=0
        )

        assert built == ProviderResponse(
            content={"records": ["[U+D800]"]},
            raw_text="raw[U+DFFF]",
            finish="truncated",
            input_tokens=12,
            output_tokens=0,
        )

    def test_every_argument_is_keyword_only(self):
        with pytest.raises(TypeError):
            response({}, "", "complete", None, None)  # type: ignore[misc]

    @pytest.mark.parametrize("finish", FINISH_REASONS)
    def test_each_finish_reason_is_kept(self, finish):
        assert (
            response(content=None, raw_text="", finish=finish, input_tokens=None, output_tokens=None).finish == finish
        )

    @pytest.mark.parametrize("finish", ["length", "", "Complete", None, 1, ["complete"]])
    def test_a_bad_finish_is_a_value_error(self, finish):
        with pytest.raises(ValueError, match="finish"):
            response(content=None, raw_text="", finish=finish, input_tokens=None, output_tokens=None)

    def test_a_finish_that_only_compares_equal_is_a_value_error(self):
        lookalike = type("Reason", (str,), {})("complete")

        with pytest.raises(ValueError, match="finish"):
            response(content=None, raw_text="", finish=lookalike, input_tokens=None, output_tokens=None)

    @pytest.mark.parametrize(
        "content",
        [None, {"a": math.nan}, [{"a": 1}], "{}", {1: 2}, {"\ud800": 1, "[U+D800]": 2}, nested(65), {"a": (1,)}],
    )
    def test_content_that_cannot_be_made_exact_is_none(self, content):
        built = response(content=content, raw_text="text", finish="complete", input_tokens=1, output_tokens=2)

        assert built.content is None
        assert (built.raw_text, built.input_tokens, built.output_tokens) == ("text", 1, 2)

    def test_raw_text_is_bounded(self):
        built = response(
            content=None,
            raw_text="a" * (RAW_TEXT_MAX_BYTES + 5),
            finish="complete",
            input_tokens=None,
            output_tokens=None,
        )

        assert built.raw_text == "a" * CUT + TRUNCATION_MARKER

    @pytest.mark.parametrize("raw_text", [None, b"bytes", 7, ["a"]])
    def test_raw_text_that_is_not_a_str_is_empty(self, raw_text):
        built = response(content={"a": 1}, raw_text=raw_text, finish="complete", input_tokens=None, output_tokens=None)

        assert built.raw_text == ""
        assert built.content == {"a": 1}

    @pytest.mark.parametrize("count", [0, 1, 4096, TOKEN_COUNT_MAX])
    def test_a_valid_token_count_is_kept(self, count):
        built = response(content=None, raw_text="", finish="complete", input_tokens=count, output_tokens=count)

        assert (built.input_tokens, built.output_tokens) == (count, count)

    @pytest.mark.parametrize(
        "count",
        [None, -1, TOKEN_COUNT_MAX + 1, 10**30, True, False, 1.0, math.nan, "12", [3], Small.ONE, Decimal(3)],
    )
    def test_a_junk_token_count_is_none(self, count):
        built = response(content=None, raw_text="", finish="complete", input_tokens=count, output_tokens=7)
        swapped = response(content=None, raw_text="", finish="complete", input_tokens=7, output_tokens=count)

        assert (built.input_tokens, built.output_tokens) == (None, 7)
        assert (swapped.input_tokens, swapped.output_tokens) == (7, None)

    @settings(max_examples=400, deadline=None)
    @given(
        content=ROOTS | JUNK,
        raw_text=TEXT | JUNK,
        finish=st.sampled_from(FINISH_REASONS),
        tokens=JUNK | st.integers(-3, 3) | st.integers(TOKEN_COUNT_MAX - 2, TOKEN_COUNT_MAX + 2),
    )
    def test_never_raises_on_any_adapter_value(self, content, raw_text, finish, tokens):
        built = response(content=content, raw_text=raw_text, finish=finish, input_tokens=tokens, output_tokens=tokens)

        assert isinstance(built, ProviderResponse)
        expected = None if content is None else reference(content)
        assert (built.content is None) == (expected is None)
        if expected is not None:
            assert canonical_json(built.content) == canonical_json(expected)
        assert built.raw_text == (escaped(raw_text) if isinstance(raw_text, str) else "")
        kept = tokens if type(tokens) is int and 0 <= tokens <= TOKEN_COUNT_MAX else None
        assert (built.input_tokens, built.output_tokens) == (kept, kept)
        assert ProviderResponse.from_json(json.loads(canonical_json(built.to_json()))) == built


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_the_package_and_nothing_inside_functions():
    tree = ast.parse(Path(responses_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested_imports = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    allowed = {
        "datagrunt.rationalization._canonical",
        "datagrunt.rationalization.exceptions",
        "datagrunt.rationalization.wire",
    }

    assert functions, "vacuous: the module defines no function"
    assert nested_imports == []
    assert [m for m in modules if m not in allowed and m.split(".")[0] not in sys.stdlib_module_names] == []


def test_importing_the_module_loads_neither_pydantic_nor_httpx():
    code = (
        "import json, sys; import datagrunt.rationalization.providers.responses; "
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('pydantic', 'httpx'))))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []
