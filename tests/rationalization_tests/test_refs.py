"""Tests for ``datagrunt.rationalization._refs``: the local ``$ref`` rules every schema transform shares (issue #360).

Every transform reads a ``#/$defs/<name>`` reference as RFC 6901 says (the name one JSON-pointer token, ``~1`` for
``/`` and ``~0`` for ``~``, unescaped in that order), writes every pointer escaped the same way, and refuses a
``$ref`` cycle among the ``$defs`` entries whatever the nodes on it, naming its path. The route tests run each rule
on every route, built through the providers' public constructors; the unit tests pin the shared functions.
"""

import ast
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest
from pydantic import create_model

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization import _refs as refs_module
from datagrunt.rationalization._refs import NodeBoundError, cycle_text, def_name, json_pointer, ref_cycle
from datagrunt.rationalization.providers.anthropic import AnthropicProvider
from datagrunt.rationalization.providers.gemini import GeminiProvider
from datagrunt.rationalization.providers.google_cloud import GoogleCloudAuth
from datagrunt.rationalization.providers.openai_compatible import OpenAICompatibleProvider
from datagrunt.rationalization.schema import prepare
from rationalization_tests.test_schema import REJECTED_JSON

KEY = "test-KEY-hunter2"
PROJECT = "my-project"
TIMEOUT_S = 120
FAST_S = 5.0  # a linear search takes milliseconds; an exponential one takes far longer


def token():
    return "ya29.test-TOKEN-hunter2"


ROUTES = {
    "anthropic": lambda: AnthropicProvider("claude-opus-5-5", api_key=KEY),
    "anthropic-google-cloud": lambda: AnthropicProvider.google_cloud(
        "claude-opus-4-7", project=PROJECT, location="global", auth=GoogleCloudAuth(token=token)
    ),
    "openai": lambda: OpenAICompatibleProvider.openai("gpt-5", api_key=KEY),
    "openai-google-cloud": lambda: OpenAICompatibleProvider.google_cloud(
        "meta/llama-4-maverick-17b-128e-instruct-maas",
        project=PROJECT,
        location="us-east5",
        auth=GoogleCloudAuth(token=token),
    ),
    "gemini": lambda: GeminiProvider("gemini-2.5-flash", api_key=KEY),
    "gemini-google-cloud": lambda: GeminiProvider.google_cloud(
        "gemini-2.5-pro", project=PROJECT, location="global", auth=GoogleCloudAuth(token=token)
    ),
    "gemini-express": lambda: GeminiProvider.google_cloud_express("gemini-2.5-pro", auth=GoogleCloudAuth(api_key=KEY)),
}

# The Google Cloud Gemini routes read a definition only where a $ref inlines it, so they name the $ref's pointer.
DEFS_WALKING_ROUTES = sorted(set(ROUTES) - {"gemini-google-cloud", "gemini-express"})


@pytest.fixture(params=sorted(ROUTES))
def route(request):
    """Each route's provider, whose ``transform_schema`` is that route's transform."""
    return ROUTES[request.param]()


def closed(**properties):
    """An object schema that requires each of its properties and allows no other."""
    return {"type": "object", "properties": properties, "required": list(properties), "additionalProperties": False}


LEAF = closed(v={"type": "integer"})


def refusal(provider, schema):
    """The message of the transform's refusal of ``schema``, or ``None`` when it accepts it."""
    try:
        provider.transform_schema(schema)
    except RationalizationError as error:
        return str(error)
    return None


def referring(ref, defs):
    """A root whose one property is ``{"$ref": ref}``, with these ``$defs``."""
    return {**closed(x={"$ref": ref}), "$defs": defs}


# ---------------------------------------------------------------------------------------------------------------
# RFC 6901 names, on every route
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "ref"),
    [("a/b~c", "#/$defs/a~1b~0c"), ("a~1", "#/$defs/a~01"), ("~", "#/$defs/~0"), ("/", "#/$defs/~1")],
    ids=["slash-and-tilde", "unescape-order", "tilde", "slash"],
)
def test_an_escaped_ref_names_its_definition_on_every_route(route, name, ref):
    assert refusal(route, referring(ref, {name: LEAF})) is None


@pytest.mark.parametrize("name", ["a/b", "/"])
def test_an_unescaped_slash_in_a_ref_name_is_refused_on_every_route(route, name):
    message = refusal(route, referring(f"#/$defs/{name}", {name: LEAF}))

    assert message is not None
    assert "$ref" in message


@pytest.mark.parametrize("token", ["a~2b", "a~", "~~0", "~a"])
def test_a_tilde_escaping_neither_tilde_nor_slash_is_refused_on_every_route(route, token):
    # Both readings are defined: the token as written, and as a lax replace of ~1 and ~0 reads it.
    defs = {token: LEAF, token.replace("~1", "/").replace("~0", "~"): LEAF}

    message = refusal(route, referring(f"#/$defs/{token}", defs))

    assert message is not None
    assert "$ref" in message


@pytest.mark.parametrize("name", ["a%20b", "%", "a%2Fb", "100%"])
def test_a_percent_in_a_ref_name_is_refused_on_every_route(route, name):
    # RFC 6901 section 6 percent-encodes a pointer in a URI fragment; a literal reading of "%" could name another entry.
    message = refusal(route, referring(f"#/$defs/{name}", {name: LEAF}))

    assert message is not None
    assert "$ref" in message


def test_a_refusal_under_a_property_names_its_rfc_6901_pointer_on_every_route(route):
    message = refusal(route, closed(**{"a/b~c": 5}))

    assert message is not None
    assert "#/properties/a~1b~0c" in message


@pytest.mark.parametrize("name", DEFS_WALKING_ROUTES)
def test_a_refusal_in_a_definition_names_its_rfc_6901_pointer(name):
    message = refusal(ROUTES[name](), referring("#/$defs/a~1b~0c", {"a/b~c": 5}))

    assert message is not None
    assert "#/$defs/a~1b~0c" in message


# Keywords that hold subschemas no transform reads, or change what a $ref resolves to: schema.prepare() refuses each,
# and dropping one would loosen the schema silently (a $ref under one would also hide from the cycle check).
UNREAD = [
    "oneOf",
    "allOf",
    "not",
    "if",
    "then",
    "else",
    "patternProperties",
    "prefixItems",
    "additionalItems",
    "contains",
    "propertyNames",
    "dependentSchemas",
    "dependencies",
    "unevaluatedProperties",
    "unevaluatedItems",
    "contentSchema",
    "definitions",
    "$dynamicRef",
    "$recursiveRef",
    "$id",
    "$anchor",
    "$dynamicAnchor",
    "$recursiveAnchor",
]


@pytest.mark.parametrize("keyword", UNREAD)
def test_a_keyword_holding_unread_subschemas_is_refused_on_every_route_naming_where(route, keyword):
    message = refusal(route, closed(a={"type": "string", keyword: [{"type": "string"}]}))

    assert message is not None
    assert keyword in message
    assert "#/properties/a" in message


def test_schema_prepare_refuses_every_such_keyword_so_no_prepared_schema_newly_fails():
    exercised = {keyword for schema, _, _ in REJECTED_JSON for keyword in schema}  # each case must be refused there

    assert set(UNREAD) <= exercised


# ---------------------------------------------------------------------------------------------------------------
# def_name and json_pointer
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ref", "name"),
    [
        ("#/$defs/Part", "Part"),
        ("#/$defs/a~1b~0c", "a/b~c"),
        ("#/$defs/a~01", "a~1"),
        ("#/$defs/~0~1", "~/"),
        ("#/$defs/", ""),
    ],
)
def test_def_name_unescapes_a_local_reference(ref, name):
    assert def_name(ref) == name


@pytest.mark.parametrize(
    "ref", [None, 5, ["#/$defs/A"], "#", "#/definitions/A", "#/$defs", "https://x.example/s#/$defs/A"]
)
def test_def_name_refuses_what_is_not_a_local_reference(ref):
    assert def_name(ref) is None


@pytest.mark.parametrize("ref", ["#/$defs/a/b", "#/$defs//", "#/$defs/A/properties/x"])
def test_def_name_refuses_an_unescaped_slash(ref):
    assert def_name(ref) is None


@pytest.mark.parametrize("ref", ["#/$defs/a~2b", "#/$defs/a~", "#/$defs/~~0", "#/$defs/~a", "#/$defs/~"])
def test_def_name_refuses_a_tilde_that_escapes_neither_tilde_nor_slash(ref):
    assert def_name(ref) is None


@pytest.mark.parametrize("ref", ["#/$defs/a%20b", "#/$defs/%", "#/$defs/%7E0"])
def test_def_name_refuses_a_percent(ref):
    assert def_name(ref) is None


def test_no_prepared_schema_names_a_definition_with_a_percent():
    # Pydantic writes each character of a model name outside [A-Za-z0-9._-] as "_", so def_name never meets "%".
    part = create_model("Part%20No", code=(str, ...))
    order = create_model("Order%Root", part=(part, ...), parts=(list[part], ...))

    names = list(prepare(order).wrapper.get("$defs", {}))

    assert len(names) == 1
    assert not any("%" in name for name in names)


@pytest.mark.parametrize(
    ("token", "pointer"),
    [("Part", "#/$defs/Part"), ("a/b~c", "#/$defs/a~1b~0c"), ("~1", "#/$defs/~01"), ("", "#/$defs/")],
)
def test_json_pointer_escapes_tilde_then_slash(token, pointer):
    assert json_pointer("#/$defs", token) == pointer


@pytest.mark.parametrize("name", ["Part", "a/b~c", "~1", "~0", "/~/", "", "Größe"])
def test_a_name_written_by_json_pointer_reads_back_by_def_name(name):
    assert def_name(json_pointer("#/$defs", name)) == name


def test_json_pointer_rejects_non_str_arguments():
    with pytest.raises(TypeError, match="str"):
        json_pointer(None, "Part")
    with pytest.raises(TypeError, match="str"):
        json_pointer("#/$defs", 123)


def test_edges_direct_push_counts_all_supported_subschema_containers():
    body = {
        "type": "object",
        "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
        "items": {"type": "string"},
        "additionalProperties": {"$ref": "#/$defs/A"},
        "anyOf": [{"type": "string"}, {"type": "null"}],
    }
    # 1 root body + 2 properties + 1 items + 1 additionalProperties + 2 anyOf = 7 nodes
    assert ref_cycle({"A": body}, max_nodes=7) == ("A", "A")
    with pytest.raises(NodeBoundError, match="more than 6 schema nodes"):
        ref_cycle({"A": body}, max_nodes=6)


# ---------------------------------------------------------------------------------------------------------------
# $ref cycles, on every route
# ---------------------------------------------------------------------------------------------------------------

CYCLES = {
    # A $ref alone: no object or array on the cycle, so no nesting count ever grows.
    "ref-chain": (
        referring("#/$defs/A", {"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}}),
        "#/$defs/A -> #/$defs/B -> #/$defs/A",
    ),
    "any-of-self": (
        referring("#/$defs/A", {"A": {"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}}),
        "#/$defs/A -> #/$defs/A",
    ),
    "object-self": (
        referring("#/$defs/Node", {"Node": closed(next={"$ref": "#/$defs/Node"})}),
        "#/$defs/Node -> #/$defs/Node",
    ),
    "array-pair": (
        referring(
            "#/$defs/A", {"A": closed(b={"$ref": "#/$defs/B"}), "B": {"type": "array", "items": {"$ref": "#/$defs/A"}}}
        ),
        "#/$defs/A -> #/$defs/B -> #/$defs/A",
    ),
    "unreferenced": (
        referring("#/$defs/L", {"L": LEAF, "A": closed(a={"$ref": "#/$defs/A"})}),
        "#/$defs/A -> #/$defs/A",
    ),
    "escaped-name": (
        referring("#/$defs/a~1b", {"a/b": closed(a={"$ref": "#/$defs/a~1b"})}),
        "#/$defs/a~1b -> #/$defs/a~1b",
    ),
}


@pytest.mark.parametrize("shape", sorted(CYCLES))
def test_a_ref_cycle_is_refused_on_every_route_naming_its_path(route, shape):
    schema, path = CYCLES[shape]

    message = refusal(route, schema)

    assert message is not None
    assert "recursive" in message
    assert path in message


@pytest.mark.parametrize("name", ["gemini", "gemini-google-cloud"])
def test_a_ref_cycle_through_additional_properties_is_refused(name):
    looping = {"type": "object", "properties": {}, "additionalProperties": {"$ref": "#/$defs/A"}}

    message = refusal(ROUTES[name](), referring("#/$defs/A", {"A": looping}))

    assert message is not None
    assert "#/$defs/A -> #/$defs/A" in message


@pytest.mark.parametrize("name", ["gemini", "gemini-google-cloud"])
def test_a_ref_cycle_through_a_refs_siblings_is_refused(name):
    # responseJsonSchema keeps a $ref's siblings, and responseSchema merges them into the entry it inlines.
    defs = {"A": {"$ref": "#/$defs/B", "properties": {"a": {"$ref": "#/$defs/A"}}}, "B": LEAF}

    message = refusal(ROUTES[name](), referring("#/$defs/A", defs))

    assert message is not None
    assert "#/$defs/A -> #/$defs/A" in message


@pytest.mark.parametrize("name", ["gemini", "gemini-google-cloud"])
def test_a_ref_whose_sibling_names_the_same_entry_is_no_cycle(name):
    # Not recursive: the entry is used twice on one path, never inside itself.
    schema = {**closed(f={"$ref": "#/$defs/Y", "properties": {"p": {"$ref": "#/$defs/Y"}}}), "$defs": {"Y": LEAF}}

    assert refusal(ROUTES[name](), schema) is None


def test_shared_definitions_without_a_cycle_pass_on_every_route(route):
    defs = {
        "A": closed(b={"$ref": "#/$defs/B"}, c={"$ref": "#/$defs/C"}),
        "B": closed(c={"$ref": "#/$defs/C"}),
        "C": LEAF,
    }

    assert refusal(route, {**closed(a={"$ref": "#/$defs/A"}, c={"$ref": "#/$defs/C"}), "$defs": defs}) is None


# Each adapter's own refusal of $defs bodies past the cycle check's node bound, by route family.
BOUND_REFUSALS = {
    "anthropic": "the response schema at #/$defs cannot go to Anthropic structured outputs: it has more than 100,000 "
    "schema nodes",
    "openai": "the response schema at #/$defs cannot be sent in OpenAI's strict mode: it has more than 100000 nodes",
    "gemini": "the response schema at #/$defs cannot be sent to Gemini: it has more than 100000 schema nodes",
}


@pytest.mark.parametrize("name", sorted(ROUTES))
def test_defs_past_the_node_bound_are_refused_in_each_adapters_words(name):
    big = {"type": "object", "properties": {f"p{index}": {"type": "string"} for index in range(100_001)}}

    message = refusal(ROUTES[name](), referring("#/$defs/A", {"A": big}))

    assert message == BOUND_REFUSALS[name.split("-")[0]]


# ---------------------------------------------------------------------------------------------------------------
# ref_cycle and cycle_text
# ---------------------------------------------------------------------------------------------------------------


def test_ref_cycle_finds_none_in_a_diamond():
    defs = {"A": closed(b={"$ref": "#/$defs/B"}, c={"$ref": "#/$defs/C"}), "B": closed(d={"$ref": "#/$defs/D"})}
    defs |= {"C": closed(d={"$ref": "#/$defs/D"}), "D": LEAF}

    assert ref_cycle(defs, max_nodes=100) is None
    assert ref_cycle({}, max_nodes=1) is None


def test_ref_cycle_returns_the_first_cycle_in_name_order():
    defs = {"B": {"$ref": "#/$defs/B"}, "A": {"$ref": "#/$defs/C"}, "C": {"anyOf": [{"$ref": "#/$defs/A"}]}}

    assert ref_cycle(defs, max_nodes=100) == ("A", "C", "A")


def test_ref_cycle_returns_only_the_cycle_not_the_path_into_it():
    defs = {
        "A": closed(b={"$ref": "#/$defs/B"}),
        "B": closed(c={"$ref": "#/$defs/C"}),
        "C": closed(b={"$ref": "#/$defs/B"}),
    }

    assert ref_cycle(defs, max_nodes=100) == ("B", "C", "B")


@pytest.mark.parametrize(
    "body",
    [
        closed(a={"$ref": "#/$defs/Missing"}),
        closed(a={"$ref": "#/$defs/a/b"}),
        closed(a={"$ref": 5}),
        {"type": "object", "properties": {}, "$defs": {"inner": {"$ref": "#/$defs/A"}}},
        {"type": "string", "default": {"$ref": "#/$defs/A"}, "enum": [{"$ref": "#/$defs/A"}]},
        {"type": "array", "items": [{"$ref": "#/$defs/A"}]},
    ],
    ids=["missing", "malformed", "not-text", "nested-defs", "values", "tuple-items"],
)
def test_ref_cycle_reads_only_references_a_transform_follows(body):
    assert ref_cycle({"A": body}, max_nodes=100) is None


def test_ref_cycle_reads_at_most_max_nodes():
    defs = {"A": closed(a={"type": "string"}, b={"type": "string"})}  # three nodes: A and its two properties

    assert ref_cycle(defs, max_nodes=3) is None
    with pytest.raises(RationalizationError, match="more than 2 schema nodes"):
        ref_cycle(defs, max_nodes=2)


def test_ref_cycles_bound_refusal_is_its_own_rationalization_error_and_counts_one_node_singular():
    with pytest.raises(NodeBoundError) as caught:
        ref_cycle({"A": closed(a={"type": "string"})}, max_nodes=1)

    assert isinstance(caught.value, RationalizationError)
    assert str(caught.value) == "the response schema's $defs hold more than 1 schema node"


def ladder(layers):
    """Two entries per layer, each referring to both of the next layer's: no cycle, but 2 ** (layers - 1) paths from
    the top, so a search that forgets the entries it finished walks every one of them."""
    defs = {}
    for index in range(layers):
        last = index == layers - 1
        refs = {} if last else {"x": {"$ref": f"#/$defs/a{index + 1}"}, "y": {"$ref": f"#/$defs/b{index + 1}"}}
        defs |= {f"{side}{index}": {"type": "object", "properties": refs} for side in "ab"}
    return defs


def test_ref_cycle_is_linear_on_a_ladder_of_shared_entries():
    defs = ladder(60)  # 2 ** 59 paths from a0: hours for a search without its memory of finished entries

    started = time.monotonic()
    assert ref_cycle(defs, max_nodes=100_000) is None
    assert time.monotonic() - started < FAST_S


def test_the_cycle_search_raises_once_past_its_step_budget():
    # ref_cycle gives each search 2 * (entries + references) + 1 steps, more than a correct one ever takes.
    edges = {"a": ["b"], "b": ["c"], "c": []}

    assert refs_module._cycle_from("a", edges, set(), 6) is None  # a, b, c pushed and finished: 5 steps
    with pytest.raises(RuntimeError, match="passed its bound of 4 steps"):
        refs_module._cycle_from("a", edges, set(), 4)


@pytest.mark.parametrize(
    ("definitions", "max_nodes", "error"),
    [([], 10, TypeError), ({}, True, TypeError), ({}, 1.0, TypeError), ({}, 0, ValueError)],
)
def test_ref_cycle_refuses_bad_arguments(definitions, max_nodes, error):
    with pytest.raises(error):
        ref_cycle(definitions, max_nodes=max_nodes)


def test_cycle_text_writes_each_name_as_its_pointer():
    assert cycle_text(("a/b", "C", "a/b")) == "#/$defs/a~1b -> #/$defs/C -> #/$defs/a~1b"


def test_cycle_text_shows_a_cycle_of_ten_hops_whole():
    cycle = (*(f"d{index}" for index in range(10)), "d0")

    assert cycle_text(cycle) == " -> ".join(f"#/$defs/{name}" for name in cycle)


def test_cycle_text_shows_ten_hops_of_a_longer_cycle_then_its_size():
    cycle = (*(f"d{index}" for index in range(1_234)), "d0")

    assert cycle_text(cycle) == " -> ".join(f"#/$defs/d{index}" for index in range(11)) + " -> … (1,234 entries)"


def test_cycle_text_cuts_a_long_name():
    assert cycle_text(("L" * 5_000, "L" * 5_000)) == " -> ".join(["#/$defs/" + "L" * 120 + "…"] * 2)


# The longest cycle text: eleven names cut at 120 characters, each doubled by escaping, plus the arrows and the size.
LONGEST_CYCLE_TEXT = 11 * len("#/$defs/" + "~0" * 120 + "…") + 11 * len(" -> ") + len("… (100,000 entries)")


@pytest.mark.parametrize(
    "defs",
    [
        {f"d{index}": {"$ref": f"#/$defs/d{(index + 1) % 100_000}"} for index in range(100_000)},
        {"~" * 5_000: closed(n={"$ref": "#/$defs/" + "~0" * 5_000})},
    ],
    ids=["ring-of-100000", "name-of-5000-tildes"],
)
def test_a_cycle_refusal_stays_short_on_every_route(route, defs):
    message = refusal(route, {**closed(x={"$ref": f"#/$defs/{json_pointer('', min(defs))[1:]}"}), "$defs": defs})

    assert message is not None
    assert "recursive" in message
    assert len(message) < LONGEST_CYCLE_TEXT + 200  # the adapter's own words take under 200 characters


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_the_error_and_nothing_inside_functions():
    tree = ast.parse(Path(refs_module.__file__).read_text(encoding="utf-8"))
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
    source_root = str(Path(refs_module.__file__).resolve().parents[2])  # the tree these tests import, in -I mode too
    code = (
        f"import json, sys; sys.path.insert(0, {source_root!r}); import datagrunt.rationalization._refs; "
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('pydantic', 'httpx'))))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []
