"""Tests for ``datagrunt.rationalization.providers.openai_compatible``: the OpenAI-compatible wire adapter (plan §4.5).

``OpenAICompatibleProvider`` sends one Chat Completions request per ``generate``: ``POST {base_url}/chat/completions``
with a system and a user message, ``response_format`` of type ``json_schema`` (name ``records``, the request's schema,
``strict``), the output limit under ``max_tokens`` or ``max_completion_tokens``, ``seed`` unless ``None`` and
``temperature`` only when one is set (the default is ``None``). The key travels only as ``Authorization: Bearer
<key>``. Every test runs over ``httpx.MockTransport`` through ``HttpClient(transport=...)``; nothing reaches a network.

Wire format, checked against OpenAI's documentation on 2026-11-04 (``platform.openai.com/docs/...`` now redirects to
``developers.openai.com``):

- Chat Completions create, request and response object:
  https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
- Structured Outputs, the strict JSON Schema subset:
  https://developers.openai.com/api/docs/guides/structured-outputs

What those pages state, and these fixtures pin:

- ``response_format`` ``{"type": "json_schema", "json_schema": {"name", "schema", "strict", "description"?}}``; the
  name is ``a-z A-Z 0-9 _ -``, at most 64 characters (``records`` fits).
- ``max_tokens`` is deprecated in favour of ``max_completion_tokens`` and is not compatible with o-series models;
  ``seed`` is Beta, best effort; ``temperature`` is 0-2.
- The reply: ``object`` ``"chat.completion"``; ``choices[].message.content`` is a string or null,
  ``choices[].message.refusal`` an optional string or null; ``finish_reason`` is ``stop``, ``length`` (the token
  limit was reached), ``content_filter`` (content omitted by the content filters), ``tool_calls`` or the deprecated
  ``function_call``; ``usage`` has ``prompt_tokens``, ``completion_tokens`` and ``total_tokens``.
- Strict mode: the root is an object, never ``anyOf``; ``additionalProperties: false`` is set on every object; every
  field is required, an optional one emulated as a union with ``null``; ``anyOf`` members are each in the subset;
  definitions (``$defs``) and ``$ref`` are supported. Supported keywords: ``pattern`` and ``format`` (``date-time``,
  ``time``, ``date``, ``duration``, ``email``, ``hostname``, ``ipv4``, ``ipv6``, ``uuid``) on strings;
  ``multipleOf``, ``maximum``, ``exclusiveMaximum``, ``minimum`` and ``exclusiveMinimum`` on numbers; ``minItems``
  and ``maxItems`` on arrays. Unsupported: ``allOf``, ``not``, ``dependentRequired``, ``dependentSchemas``, ``if``,
  ``then``, ``else``. Fine-tuned models additionally refuse ``minLength``, ``maxLength``, ``pattern`` and ``format``
  on strings, ``minimum``, ``maximum`` and ``multipleOf`` on numbers, ``patternProperties``, and ``minItems`` and
  ``maxItems`` on arrays.
- Limits: up to 5,000 object properties in all, with up to 10 levels of nesting; up to 1,000 enum values across all
  enum properties; the property names, definition names, enum values and const values together at most 120,000
  characters; one string enum of more than 250 values at most 15,000 characters.

Unverified (the pages do not say): whether ``title``, ``default`` and ``examples`` are accepted (they are dropped, as
the supported list omits them; ``description`` is kept, as every example uses it), whether ``minLength`` /
``maxLength`` are (dropped), whether ``const`` is (written as a one-value ``enum``), whether a ``$ref`` may carry
sibling keywords (a description is kept by wrapping the ``$ref`` in a one-member ``anyOf``), and how the limits count
a definition used several times (counted once; nesting is followed through every reference, objects and arrays both
counted, the conservative reading). The adapter sends the ``system`` role, which the reference says ``developer``
replaces for o1 and newer models.

Google Cloud (``OpenAICompatibleProvider.google_cloud``), read on 2026-11-05 beside the research table in
``providers/google_cloud.py``:

- Open and partner models on the shared endpoint: ``POST …/endpoints/openapi/chat/completions``, ``model``
  ``{publisher}/{model}``; the supported parameters include ``max_tokens`` (``max_completion_tokens`` is its alias),
  ``seed``, ``temperature`` and ``response_format`` (``json_schema``; "Fully recursive schemas are not supported"), and
  an unsupported parameter "is ignored".
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/migrate/openai/overview,
  https://docs.cloud.google.com/vertex-ai/generative-ai/docs/migrate/openai/examples,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/maas/capabilities/structured-output (all MaaS
  open models "support structured outputs"; its REST example shows only JSON mode, its Python example a schema).
- Grok: schema-enforced output is documented for the Responses API; on Chat Completions the REST body shown is JSON
  mode ``{"type": "json_object"}``, and the Python example uses ``beta.chat.completions.parse`` with a schema ("The
  model output will adhere to the following JSON schema"), so ``strict`` defaults on there too.
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/grok/capabilities/structured-output
- Mistral: ``POST …/publishers/mistralai/models/{model}:rawPredict``; the body holds ``model`` ("exclude the ``@``
  model version number"), ``messages`` whose roles are ``user`` or ``assistant`` ("The first message must use the
  ``user`` role"; alternating turns; no ``system`` role is mentioned), ``max_tokens`` and ``stream``; structured output,
  temperature and a seed are not mentioned. Mistral's own API reference adds a ``system`` role, ``temperature``,
  ``random_seed`` and ``response_format`` (``json_schema``) on its own platform. Google's pages show ``user`` turns
  only for the shared endpoint too. The adapter still sends a ``system`` turn first on every route: if a route
  refuses it, the first request fails with a permanent 4xx rather than folding the rules into the data's turn
  (T16's live smoke test settles it).
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/mistral,
  https://docs.mistral.ai/api/endpoint/chat
- Dedicated endpoints: ``POST …/endpoints/{ENDPOINT_ID}/chat/completions``; the REST example's body has no ``model``.
"""

import copy
import inspect
import json
import logging
import math
import time
import traceback

import httpx
import pytest
from pydantic import BaseModel, NaiveDatetime, create_model

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization.providers import openai_compatible as module
from datagrunt.rationalization.providers.base import (
    OLLAMA_BASE_URL,
    OPENAI_BASE_URL,
    UNKNOWN_FAMILY,
    ModelProvider,
    ModelString,
    PermanentProviderError,
    ProviderError,
    TransientProviderError,
    check_provider,
    parse_model_string,
)
from datagrunt.rationalization.providers.google_cloud import GoogleCloudAuth
from datagrunt.rationalization.providers.http import HttpClient
from datagrunt.rationalization.providers.openai_compatible import OpenAICompatibleProvider
from datagrunt.rationalization.schema import response_schema
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse
from rationalization_tests.test_schema import REALISTIC

KEY = "sk-test-hunter2-KEY"
GATEWAY = "https://gw.example.com/v1"
SYSTEM = "SYSTEM-PROMPT-TEXT"
ENVELOPE = "USER-ENVELOPE-TEXT Größe"
WIRE = "chat-completions/1"
SMALL_SCHEMA = {
    "type": "object",
    "properties": {"records": {"type": "array", "items": {"type": "string"}}},
    "required": ["records"],
    "additionalProperties": False,
}
# The documented reply's usage block (the reference's example: 19 + 10 = 29 tokens, zeroed details).
USAGE = {
    "prompt_tokens": 19,
    "completion_tokens": 10,
    "total_tokens": 29,
    "prompt_tokens_details": {"cached_tokens": 0, "audio_tokens": 0},
    "completion_tokens_details": {
        "reasoning_tokens": 0,
        "audio_tokens": 0,
        "accepted_prediction_tokens": 0,
        "rejected_prediction_tokens": 0,
    },
}
RECORDS = '{"records": [{"record": {"code": "PN-1"}, "grounding_quotes": ["PN-1"], "source_element_ids": ["e1"]}]}'
# OpenAI's strict subset as the documentation states it (module notes), written independently of the module. The
# formats date-time and time are left out: a naive datetime has no offset, which the RFC 3339 form requires.
DOC_COMMON = frozenset({"type", "description", "enum", "anyOf"})
DOC_NUMBER = frozenset({"multipleOf", "maximum", "exclusiveMaximum", "minimum", "exclusiveMinimum"})
DOC_BY_TYPE = {
    "object": frozenset({"properties", "required", "additionalProperties"}),
    "array": frozenset({"items", "minItems", "maxItems"}),
    "string": frozenset({"pattern", "format"}),
    "number": DOC_NUMBER,
    "integer": DOC_NUMBER,
    "boolean": frozenset(),
    "null": frozenset(),
}
DOC_FORMATS = frozenset({"date", "duration", "email", "hostname", "ipv4", "ipv6", "uuid"})
DOC_MAX_PROPERTIES = 5_000
DOC_MAX_NESTING = 10
MISSING = object()
ENVIRONMENT = (
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
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
)
# The fields of each realistic schema that are not required and not Optional (a default, a default factory, a list
# defaulting to []), as (owner, field): the owner is a $defs entry, or "record" for the root model. Strict mode sends
# each one required and nullable.
REALISTIC_DEFAULTED = {
    "invoice": [
        ("Address", "country"),
        ("record", "currency"),
        ("record", "line_items"),
        ("record", "status"),
        ("LineItem", "unit"),
    ],
    "camelCase BOM": [
        ("Assembly", "components"),
        ("Component", "quantityPer"),
        ("Component", "suppliers"),
        ("record", "assemblies"),
    ],
    "contract": [("record", "auto_renew"), ("record", "governing_law")],
    "lab report": [("record", "comments"), ("record", "priority"), ("record", "results"), ("record", "specimens")],
}


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """No key, endpoint, proxy or certificate variable from the developer's shell reaches a test."""
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------------------------


def completion(content=RECORDS, *, finish_reason="stop", refusal=None, usage=USAGE):
    """A chat completion shaped as the reference's example reply."""
    message = {"role": "assistant", "content": content, "refusal": refusal, "annotations": []}
    return {
        "id": "chatcmpl-B9MBs8CjcvOU2jLn4n570S5qMJKcT",
        "object": "chat.completion",
        "created": 1762214400,
        "model": "gpt-4o-2024-08-06",
        "choices": [{"index": 0, "message": message, "logprobs": None, "finish_reason": finish_reason}],
        "usage": usage,
        "service_tier": "default",
    }


class Server:
    """A mock transport handler: records each request and answers one streaming reply."""

    def __init__(self, status=200, body=MISSING, *, raw=None, headers=None):
        self.status = status
        self.data = raw if raw is not None else json.dumps(completion() if body is MISSING else body).encode("utf-8")
        self.headers = {"content-type": "application/json", **(headers or {})}
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        return httpx.Response(self.status, headers=self.headers, stream=httpx.ByteStream(self.data))

    @property
    def only(self):
        assert len(self.requests) == 1
        return self.requests[0]


def transport(server):
    return httpx.MockTransport(server)


def gateway(server, **kwargs):
    """A provider for a gateway endpoint over a mock transport."""
    return OpenAICompatibleProvider("llama3", base_url=GATEWAY, transport=transport(server), **kwargs)


def make_request(**overrides):
    fields = {
        "unit_id": "doc:p0001_e001-p0001_e003",
        "model_id": "openai-compatible:llama3@https://gw.example.com/v1",
        "system_prompt": SYSTEM,
        "user_envelope": ENVELOPE,
        "json_schema": SMALL_SCHEMA,
        "max_output_tokens": 1024,
        "timeout_s": 30.0,
    }
    fields.update(overrides)
    return ModelRequest(**fields)


def answer(body=MISSING, *, raw=None, **kwargs):
    """The response a gateway provider builds for one reply."""
    return gateway(Server(200, body, raw=raw), **kwargs).generate(make_request())


def obj(**properties):
    """An object schema requiring every one of its properties."""
    return {"type": "object", "properties": properties, "required": list(properties)}


def traceback_text(error):
    """The error's traceback with the locals of datagrunt's frames, as a log with ``capture_locals`` prints them.

    The test's own frame is left out: it is the caller, which holds what it passed.
    """
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def assert_key_free(error, key=KEY):
    """No key text in the error's traceback locals, text or repr, and no chained exception that could hold it."""
    for text in (traceback_text(error), str(error), repr(error)):
        assert key not in text
        assert "hunter2" not in text
    assert error.__cause__ is None
    assert error.__context__ is None


def deref(node, defs):
    while "$ref" in node:
        node = defs[node["$ref"].removeprefix("#/$defs/")]
    return node


def strict_violations(schema):
    """Every way ``schema`` leaves OpenAI's documented strict subset (DOC_* above), as messages."""
    problems = []
    defs = schema.get("$defs", {})
    if schema.get("type") != "object" or "anyOf" in schema:
        problems.append("#: the root must be an object, not anyOf")
    stack = [("#", schema)] + [(f"#/$defs/{name}", node) for name, node in defs.items()]
    while stack:
        pointer, node = stack.pop()
        problems += node_violations(pointer, node, defs, root=pointer == "#")
        if not isinstance(node, dict):
            continue
        stack += [(f"{pointer}/properties/{name}", child) for name, child in node.get("properties", {}).items()]
        stack += [(f"{pointer}/items", node["items"])] if "items" in node else []
        stack += [(f"{pointer}/anyOf/{index}", child) for index, child in enumerate(node.get("anyOf", []))]
    problems += size_violations(schema, defs)
    return problems


def node_violations(pointer, node, defs, *, root):
    if not isinstance(node, dict):
        return [f"{pointer}: not a schema object"]
    if "$ref" in node:
        fine = set(node) == {"$ref"} and node["$ref"].removeprefix("#/$defs/") in defs
        return [] if fine else [f"{pointer}: a $ref must stand alone and resolve"]
    kind = node.get("type")
    typed = DOC_BY_TYPE.get(kind, frozenset()) if isinstance(kind, str) else frozenset()
    allowed = DOC_COMMON | typed | ({"$defs"} if root else set())
    problems = [f"{pointer}: {keyword!r} is not in the subset" for keyword in node if keyword not in allowed]
    if "format" in node and node["format"] not in DOC_FORMATS:
        problems.append(f"{pointer}: format {node['format']!r} is not sent")
    if node.get("type") == "object":
        if node.get("additionalProperties") is not False:
            problems.append(f"{pointer}: additionalProperties must be false")
        if node.get("required") != list(node.get("properties", {})):
            problems.append(f"{pointer}: every property must be required")
    return problems


def size_violations(schema, defs):
    counts = {"properties": 0}

    def depth(node, level):
        node = deref(node, defs)
        level += node.get("type") in ("object", "array")
        counts["properties"] += len(node.get("properties", {}))
        children = [*node.get("properties", {}).values(), *node.get("anyOf", [])]
        children += [node["items"]] if "items" in node else []
        return max([level, *(depth(child, level) for child in children)])

    deepest = depth(schema, 0)
    problems = [f"nesting {deepest} passes {DOC_MAX_NESTING}"] if deepest > DOC_MAX_NESTING else []
    if counts["properties"] > DOC_MAX_PROPERTIES:
        problems.append(f"{counts['properties']} properties pass {DOC_MAX_PROPERTIES}")
    return problems


def conforms(value, node, defs):
    """Whether ``value`` matches ``node`` of the strict subset (structure only: types, enums, required, items)."""
    node = deref(node, defs)
    if "anyOf" in node:
        return any(conforms(value, member, defs) for member in node["anyOf"])
    if "enum" in node and not any(type(value) is type(item) and value == item for item in node["enum"]):
        return False
    kind = node.get("type")
    if kind == "object":
        properties = node["properties"]
        return (
            isinstance(value, dict)
            and list(value) == node["required"] == list(properties)
            and all(conforms(value[name], properties[name], defs) for name in properties)
        )
    if kind == "array":
        return isinstance(value, list) and all(conforms(item, node["items"], defs) for item in value)
    checks = {
        "null": lambda v: v is None,
        "string": lambda v: type(v) is str,
        "integer": lambda v: type(v) is int,
        "number": lambda v: type(v) in (int, float),
        "boolean": lambda v: type(v) is bool,
    }
    return kind in checks and checks[kind](value)


def nullable(node):
    return any(isinstance(member, dict) and member.get("type") == "null" for member in node.get("anyOf", []))


def filled(value, node, defs, defaulted=frozenset(), owner="record"):
    """``value`` with every field named in ``defaulted`` ((owner, field) pairs) null, the model's "not shown", and
    every other property its (original) schema leaves out filled: its default, else null when Optional.

    The keys come out in the order of the schema's ``properties``, sorted, as the transformed schema lists them.
    """
    name_of = node["$ref"].removeprefix("#/$defs/") if "$ref" in node else owner
    resolved = deref(node, defs)
    if "anyOf" in resolved:
        if value is None:
            return None
        members = [member for member in resolved["anyOf"] if member != {"type": "null"}]
        wanted = "object" if isinstance(value, dict) else "array" if isinstance(value, list) else None
        chosen = next((member for member in members if deref(member, defs).get("type") == wanted), None)
        return value if chosen is None else filled(value, chosen, defs, defaulted, owner)
    if resolved.get("type") == "object":
        out = {}
        for name in sorted(resolved["properties"]):
            prop = resolved["properties"][name]
            if (name_of, name) in defaulted:
                out[name] = None
            elif name in value:
                out[name] = filled(value[name], prop, defs, defaulted, name_of)
            elif "default" in prop:
                out[name] = prop["default"]
            else:
                assert nullable(prop) or deref(prop, defs).get("type") == "array", f"nothing fills {name}"
                out[name] = None if nullable(prop) else []
        return out
    if resolved.get("type") == "array":
        return [filled(item, resolved["items"], defs, defaulted, owner) for item in value]
    return value


def without_nulls(record, defaulted, wrapper):
    """What consensus (T10) hands pydantic: ``record`` with every null of a defaulted field dropped."""
    defs = wrapper.get("$defs", {})

    def walk(value, node, owner):
        name_of = node["$ref"].removeprefix("#/$defs/") if "$ref" in node else owner
        resolved = deref(node, defs)
        members = [m for m in resolved.get("anyOf", []) if m != {"type": "null"}]
        if members and isinstance(value, (dict, list)):
            kind = "object" if isinstance(value, dict) else "array"
            resolved_member = next(m for m in members if deref(m, defs).get("type") == kind)
            return walk(value, resolved_member, owner)
        if isinstance(value, dict):
            props = resolved["properties"]
            return {
                k: walk(v, props[k], name_of) for k, v in value.items() if not (v is None and (name_of, k) in defaulted)
            }
        if isinstance(value, list):
            return [walk(item, resolved["items"], owner) for item in value]
        return value

    return walk(record, wrapper["properties"]["records"]["items"]["properties"]["record"], "record")


def remedied(schema):
    """``schema`` as if every defaulted field that is neither Optional nor a list had its default dropped: required."""
    result = copy.deepcopy(schema)
    stack = [result]
    while stack:
        node = stack.pop()
        if node.get("type") == "object":
            node["required"] = [name for name, prop in node["properties"].items() if not nullable(prop)]
        stack += [*node.get("properties", {}).values(), *node.get("$defs", {}).values()]
        stack += [node["items"]] if "items" in node else []
    return result


# ---------------------------------------------------------------------------------------------------------------
# The request: goldens
# ---------------------------------------------------------------------------------------------------------------


class TestRequestBody:
    def test_openai_sends_the_golden_body_with_the_key_as_a_bearer_header(self):
        server = Server()
        provider = OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY, transport=transport(server))

        provider.generate(make_request(model_id="openai:gpt-4o"))

        sent = server.only
        assert sent.method == "POST"
        assert str(sent.url) == "https://api.openai.com/v1/chat/completions"
        assert sent.headers["authorization"] == f"Bearer {KEY}"
        assert sent.content.decode("utf-8") == (
            '{"model":"gpt-4o","messages":[{"role":"system","content":"SYSTEM-PROMPT-TEXT"},{"role":"user",'
            '"content":"USER-ENVELOPE-TEXT Größe"}],"response_format":{"type":"json_schema","json_schema":{"name":'
            '"records","schema":{"additionalProperties":false,"properties":{"records":{"items":{"type":"string"},'
            '"type":"array"}},"required":["records"],"type":"object"},"strict":true}},"max_completion_tokens":1024,'
            '"seed":0}'
        )

    def test_ollama_sends_the_golden_body_without_any_authorization(self):
        server = Server()
        provider = OpenAICompatibleProvider.ollama("llama3.1:8b", transport=transport(server))

        provider.generate(make_request(model_id="ollama:llama3.1:8b", max_output_tokens=77))

        sent = server.only
        assert str(sent.url) == "http://localhost:11434/v1/chat/completions"
        assert "authorization" not in sent.headers
        assert sent.content.decode("utf-8") == (
            '{"model":"llama3.1:8b","messages":[{"role":"system","content":"SYSTEM-PROMPT-TEXT"},{"role":"user",'
            '"content":"USER-ENVELOPE-TEXT Größe"}],"response_format":{"type":"json_schema","json_schema":{"name":'
            '"records","schema":{"additionalProperties":false,"properties":{"records":{"items":{"type":"string"},'
            '"type":"array"}},"required":["records"],"type":"object"},"strict":true}},"max_tokens":77,'
            '"temperature":0.0,"seed":0}'
        )

    def test_a_gateway_sends_its_own_options_and_omits_none_valued_parameters(self):
        server = Server()
        provider = gateway(
            server,
            api_key=KEY,
            temperature=None,
            seed=None,
            max_tokens_field="max_completion_tokens",
            strict=False,
        )

        provider.generate(make_request(timeout_s=5))

        sent = server.only
        assert str(sent.url) == "https://gw.example.com/v1/chat/completions"
        assert sent.headers["authorization"] == f"Bearer {KEY}"
        assert sent.content.decode("utf-8") == (
            '{"model":"llama3","messages":[{"role":"system","content":"SYSTEM-PROMPT-TEXT"},{"role":"user",'
            '"content":"USER-ENVELOPE-TEXT Größe"}],"response_format":{"type":"json_schema","json_schema":{"name":'
            '"records","schema":{"additionalProperties":false,"properties":{"records":{"items":{"type":"string"},'
            '"type":"array"}},"required":["records"],"type":"object"},"strict":false}},"max_completion_tokens":1024}'
        )

    @pytest.mark.parametrize(
        ("build", "temperature"),
        [
            (lambda server: gateway(server), 0.0),
            (lambda server: OpenAICompatibleProvider.openai("o3", api_key=KEY, transport=transport(server)), None),
            (lambda server: OpenAICompatibleProvider.ollama("llama3", transport=transport(server)), 0.0),
            (
                lambda server: OpenAICompatibleProvider.from_model_string(
                    parse_model_string("ollama:llama3"), transport=transport(server)
                ),
                0.0,
            ),
            (
                lambda server: OpenAICompatibleProvider.from_model_string(
                    parse_model_string("openai-compatible:m@https://gw.example.com/v1"), transport=transport(server)
                ),
                0.0,
            ),
            (
                lambda server: OpenAICompatibleProvider.from_model_string(
                    parse_model_string("openai:o3"), api_key=KEY, transport=transport(server)
                ),
                None,
            ),
        ],
        ids=["constructor", "openai", "ollama", "ollama-string", "compatible-string", "openai-string"],
    )
    def test_the_default_temperature_is_0_except_for_openai_which_sends_none(self, build, temperature):
        # OpenAI's reasoning models refuse a temperature; every other route takes 0, and cache replay stays the
        # determinism guarantee (R10).
        server = Server()

        build(server).generate(make_request())

        body = json.loads(server.only.content)
        assert body.get("temperature", "absent") == ("absent" if temperature is None else temperature)
        assert body["seed"] == 0

    def test_a_gateway_sends_a_set_temperature_and_seed(self):
        server = Server()

        gateway(server, temperature=1.25, seed=-7).generate(make_request())

        body = json.loads(server.only.content)
        assert (body["temperature"], body["seed"], body["max_tokens"]) == (1.25, -7, 1024)

    @pytest.mark.parametrize("temperature", [0, 0.0])
    def test_a_zero_temperature_is_sent_when_set(self, temperature):
        server = Server()

        gateway(server, temperature=temperature).generate(make_request())

        assert b'"temperature":0.0' in server.only.content

    def test_an_int_temperature_is_sent_as_a_float(self):
        server = Server()

        gateway(server, temperature=1).generate(make_request())

        assert b'"temperature":1.0' in server.only.content

    def test_a_gateway_without_a_key_sends_no_authorization(self):
        server = Server()

        gateway(server).generate(make_request())

        assert "authorization" not in server.only.headers

    def test_the_key_is_only_in_the_authorization_header(self):
        server = Server()

        gateway(server, api_key=KEY).generate(make_request())

        sent = server.only
        assert KEY not in str(sent.url)
        assert KEY.encode() not in sent.content
        assert [name for name, value in sent.headers.items() if KEY in value] == ["authorization"]

    def test_the_schema_sent_is_the_requests_own(self):
        server = Server()
        wrapper = response_schema(REALISTIC["invoice"][0])
        schema = OpenAICompatibleProvider("m", base_url=GATEWAY).transform_schema(wrapper)

        gateway(server).generate(make_request(json_schema=schema))

        assert json.loads(server.only.content)["response_format"]["json_schema"]["schema"] == schema

    def test_generate_needs_a_model_request(self):
        with pytest.raises(TypeError, match="ModelRequest"):
            gateway(Server()).generate({"system_prompt": SYSTEM})


class TestDeadline:
    def test_post_json_gets_the_timeout_and_a_deadline_of_monotonic_plus_the_timeout(self, monkeypatch):
        seen = []
        original = HttpClient.post_json

        def spy(self, url, **kwargs):
            seen.append(kwargs)
            return original(self, url, **kwargs)

        monkeypatch.setattr(HttpClient, "post_json", spy)
        provider = gateway(Server())

        before = time.monotonic()
        provider.generate(make_request(timeout_s=42.5))
        after = time.monotonic()

        (call,) = seen
        assert call["timeout_s"] == 42.5
        assert before + 42.5 <= call["deadline"] <= after + 42.5


# ---------------------------------------------------------------------------------------------------------------
# The reply
# ---------------------------------------------------------------------------------------------------------------


class TestFinish:
    @pytest.mark.parametrize(
        ("finish_reason", "finish"),
        [
            ("stop", "complete"),
            ("length", "truncated"),
            ("content_filter", "refused"),
            ("tool_calls", "complete"),
            ("function_call", "complete"),
            ("something-new", "complete"),
            (None, "complete"),
            (["stop"], "complete"),
            (7, "complete"),
        ],
    )
    def test_each_finish_reason_maps_and_the_content_is_still_parsed(self, finish_reason, finish):
        result = answer(completion(finish_reason=finish_reason))

        assert isinstance(result, ProviderResponse)
        assert result.finish == finish
        assert result.content == json.loads(RECORDS)
        assert result.raw_text == RECORDS

    def test_a_missing_finish_reason_is_complete(self):
        body = completion()
        del body["choices"][0]["finish_reason"]

        assert answer(body).finish == "complete"

    def test_a_refusal_is_refused_and_keeps_the_refusal_text(self):
        result = answer(completion(content=None, refusal="I can't help with that."))

        assert result.finish == "refused"
        assert result.content is None
        assert result.raw_text == "I can't help with that."

    def test_a_refusal_wins_over_a_stop_and_over_content(self):
        result = answer(completion(content=RECORDS, refusal="no", finish_reason="stop"))

        assert result.finish == "refused"
        assert result.raw_text == RECORDS

    @pytest.mark.parametrize("refusal", [None, ""])
    def test_a_null_or_empty_refusal_is_no_refusal(self, refusal):
        assert answer(completion(refusal=refusal)).finish == "complete"

    @pytest.mark.parametrize("refusal", [" ", True, 0, ["no"], {"reason": "x"}])
    def test_any_other_refusal_value_fails_closed_as_refused(self, refusal):
        assert answer(completion(refusal=refusal)).finish == "refused"

    @pytest.mark.parametrize("refusal", [True, ["no"], {"reason": "x"}, 7])
    def test_a_refusal_that_is_not_text_keeps_the_reply_text(self, refusal):
        raw = json.dumps(completion(content=None, refusal=refusal)).encode("utf-8")

        result = answer(raw=raw)

        assert result.finish == "refused"
        assert result.raw_text == raw.decode("utf-8")

    def test_a_missing_refusal_field_is_no_refusal(self):
        body = completion()
        del body["choices"][0]["message"]["refusal"]

        assert answer(body).finish == "complete"


class TestContent:
    @pytest.mark.parametrize(
        "text",
        [
            f"```json\n{RECORDS}\n```",
            f"Here are the records: {RECORDS}",
            f"{RECORDS} I hope this helps.",
            "﻿" + RECORDS,
            "[1, 2]",
            "not json at all",
            "",
            '{"a": NaN}',
            '{"a": 1, "a": 2}',
        ],
        ids=["fence", "prose-before", "prose-after", "bom", "array", "text", "empty", "nan", "repeated-key"],
    )
    def test_content_that_is_not_exactly_one_json_object_is_none_and_kept_as_raw_text(self, text):
        result = answer(completion(content=text))

        assert result.content is None
        assert result.raw_text == text
        assert result.finish == "complete"

    def test_null_content_without_a_refusal_keeps_the_reply_text(self):
        body = completion(content=None, finish_reason="tool_calls")
        raw = json.dumps(body).encode("utf-8")

        result = answer(raw=raw)

        assert result.content is None
        assert result.raw_text == raw.decode("utf-8")

    def test_content_as_an_array_of_parts_is_none_and_keeps_the_reply_text(self):
        body = completion(content=[{"type": "text", "text": RECORDS}])
        raw = json.dumps(body).encode("utf-8")

        result = answer(raw=raw)

        assert result.content is None
        assert result.raw_text == raw.decode("utf-8")

    def test_a_lone_surrogate_in_the_content_text_is_escaped_in_content_and_raw_text(self):
        # The reply's own \ud800 escape puts a lone surrogate in the content string itself.
        inner = '{\\"records\\": [], \\"note\\": \\"\\ud800\\"}'
        raw = json.dumps(completion(content="PLACEHOLDER")).replace("PLACEHOLDER", inner).encode("utf-8")

        result = answer(raw=raw)

        assert result.content == {"note": "[U+D800]", "records": []}
        assert result.raw_text == '{"records": [], "note": "[U+D800]"}'

    def test_a_lone_surrogate_escape_inside_the_content_json_is_escaped_in_content_only(self):
        # The content's own JSON holds the six characters \ud800: raw_text keeps them, the parse escapes the result.
        result = answer(completion(content='{"records": [], "note": "\\ud800"}'))

        assert result.content == {"note": "[U+D800]", "records": []}
        assert result.raw_text == '{"records": [], "note": "\\ud800"}'

    def test_only_the_first_choice_is_read(self):
        body = completion()
        body["choices"].append({"index": 1, "message": {"content": "{}", "refusal": "no"}, "finish_reason": "length"})

        result = answer(body)

        assert (result.finish, result.content) == ("complete", json.loads(RECORDS))


class TestUsage:
    def test_usage_is_parsed(self):
        result = answer()

        assert (result.input_tokens, result.output_tokens) == (19, 10)

    @pytest.mark.parametrize(
        "usage",
        [
            None,
            [],
            "19",
            {},
            {"prompt_tokens": "19", "completion_tokens": "10"},
            {"prompt_tokens": True, "completion_tokens": False},
            {"prompt_tokens": -1, "completion_tokens": 1.5},
            {"prompt_tokens": 2**63, "completion_tokens": None},
            {"input_tokens": 19, "output_tokens": 10},
        ],
    )
    def test_junk_usage_is_ignored_without_failing_the_response(self, usage):
        result = answer(completion(usage=usage))

        assert (result.input_tokens, result.output_tokens) == (None, None)
        assert result.content == json.loads(RECORDS)

    def test_one_good_count_is_kept_beside_a_junk_one(self):
        result = answer(completion(usage={"prompt_tokens": 5, "completion_tokens": "x"}))

        assert (result.input_tokens, result.output_tokens) == (5, None)

    def test_usage_is_read_even_when_the_reply_is_no_chat_completion(self):
        result = answer({"usage": {"prompt_tokens": 3, "completion_tokens": 4}})

        assert (result.content, result.input_tokens, result.output_tokens) == (None, 3, 4)


class TestUnexpectedReplies:
    @pytest.mark.parametrize(
        "raw",
        [
            b"{}",
            b'{"choices": []}',
            b'{"choices": "x"}',
            b'{"choices": {"0": {}}}',
            b'{"choices": [1]}',
            b'{"choices": [{"message": "x"}]}',
            b'{"choices": [{"finish_reason": "stop"}]}',
            b"[1, 2, 3]",
            b"<html>gateway says hi</html>",
            b"",
            b'{"choices": [], "choices": []}',
        ],
    )
    def test_a_200_that_is_no_chat_completion_is_unparseable_never_an_exception(self, raw):
        result = answer(raw=raw)

        assert result.content is None
        assert result.finish == "complete"
        assert result.raw_text == raw.decode("utf-8")

    def test_an_undecodable_200_body_keeps_its_replaced_text(self):
        result = answer(raw=b"\xff\xfe junk")

        assert (result.content, result.raw_text) == (None, "�� junk")


class TestHttpErrors:
    def test_a_401_is_permanent_with_its_status(self):
        server = Server(401, {"error": {"message": "Incorrect API key provided", "type": "invalid_request_error"}})

        with pytest.raises(PermanentProviderError) as caught:
            gateway(server, api_key=KEY).generate(make_request())

        assert caught.value.status_code == 401
        assert "Incorrect API key provided" in caught.value.detail

    def test_a_429_is_transient_with_its_retry_after(self):
        server = Server(429, {"error": {"message": "Rate limit reached"}}, headers={"retry-after": "7"})

        with pytest.raises(TransientProviderError) as caught:
            gateway(server).generate(make_request())

        assert (caught.value.status_code, caught.value.retry_after_s) == (429, 7.0)

    def test_a_429_reads_openais_reset_header_when_there_is_no_retry_after(self):
        server = Server(429, {"error": {"message": "slow down"}}, headers={"x-ratelimit-reset-requests": "6m0s"})

        with pytest.raises(TransientProviderError) as caught:
            gateway(server).generate(make_request())

        assert caught.value.retry_after_s == 360.0

    @pytest.mark.parametrize("status", [500, 503, 520, 529])
    def test_a_retryable_5xx_is_transient_without_a_wait(self, status):
        with pytest.raises(TransientProviderError) as caught:
            gateway(Server(status, {"error": {"message": "boom"}})).generate(make_request())

        assert (caught.value.status_code, caught.value.retry_after_s) == (status, None)

    def test_a_400_is_permanent(self):
        with pytest.raises(PermanentProviderError) as caught:
            gateway(Server(400, {"error": {"message": "Invalid schema for response_format"}})).generate(make_request())

        assert caught.value.status_code == 400

    def test_a_redirect_is_permanent_and_not_followed(self):
        server = Server(307, {}, headers={"location": "https://elsewhere.example.com/v1/chat/completions"})

        with pytest.raises(PermanentProviderError):
            gateway(server).generate(make_request())

        assert len(server.requests) == 1

    def test_a_401_carries_no_key_in_its_text_detail_or_traceback_locals(self):
        server = Server(401, {"error": {"message": f"Incorrect API key provided: {KEY}"}})
        provider = gateway(server, api_key=KEY)

        with pytest.raises(ProviderError) as caught:
            provider.generate(make_request())

        assert KEY not in caught.value.detail
        assert_key_free(caught.value)

    def test_nothing_logged_holds_the_key_the_system_prompt_or_the_envelope(self, caplog):
        caplog.set_level(logging.DEBUG)

        gateway(Server(), api_key=KEY).generate(make_request())
        with pytest.raises(PermanentProviderError):
            gateway(Server(401, {"error": {"message": "bad"}}), api_key=KEY).generate(make_request())

        assert caplog.records, "vacuous: nothing was logged"
        for secret in (KEY, SYSTEM, ENVELOPE):
            assert secret not in caplog.text


# ---------------------------------------------------------------------------------------------------------------
# The strict schema transform
# ---------------------------------------------------------------------------------------------------------------

PYDANTIC_LIKE = {
    "$defs": {
        "Part": {
            "title": "Part",
            "description": "A part.",
            "type": "object",
            "properties": {
                "number": {"title": "Number", "type": "string", "minLength": 1, "maxLength": 32, "pattern": "^[A-Z]"},
                "qty": {"title": "Qty", "type": "integer", "minimum": 1, "examples": [3]},
                "kind": {"const": "bolt", "type": "string", "title": "Kind"},
            },
            "required": ["number", "qty", "kind"],
        },
    },
    "title": "Wrapper",
    "type": "object",
    "properties": {
        "records": {
            "type": "array",
            "minItems": 0,
            "maxItems": 10,
            "uniqueItems": False,
            "items": {
                "type": "object",
                "properties": {
                    "record": {"$ref": "#/$defs/Part", "description": "the part"},
                    "spare": {"$ref": "#/$defs/Part", "title": "Spare"},
                    "note": {"anyOf": [{"type": "string", "format": "uri"}, {"type": "null"}], "default": None},
                    "when": {"type": "string", "format": "date-time", "deprecated": True, "description": "When"},
                    "at": {"type": "string", "format": "time"},
                    "on": {"type": "string", "format": "date"},
                    "price": {
                        "anyOf": [{"type": "number", "exclusiveMinimum": 0, "multipleOf": 0.01}, {"type": "string"}]
                    },
                    "title": {"type": "string", "title": "Title"},
                    "description": {"type": "boolean"},
                    "level": {"type": "integer", "enum": [1, 2], "maximum": 2, "exclusiveMaximum": 3},
                    "tags": {"type": "array", "items": {"type": "string", "maxLength": 5}, "default": []},
                },
                "required": ["record", "spare", "when", "at", "on", "price", "title", "description", "level"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["records"],
    "additionalProperties": False,
}

PYDANTIC_LIKE_STRICT = {
    "$defs": {
        "Part": {
            "description": "A part.",
            "type": "object",
            "properties": {
                "kind": {"enum": ["bolt"], "type": "string"},
                "number": {"pattern": "^[A-Z]", "type": "string"},
                "qty": {"minimum": 1, "type": "integer"},
            },
            "required": ["kind", "number", "qty"],
            "additionalProperties": False,
        },
    },
    "type": "object",
    "properties": {
        "records": {
            "type": "array",
            "minItems": 0,
            "maxItems": 10,
            "items": {
                "type": "object",
                "properties": {
                    "at": {"description": "an ISO 8601 time", "type": "string"},
                    "description": {"type": "boolean"},
                    "level": {"enum": [1, 2], "exclusiveMaximum": 3, "maximum": 2, "type": "integer"},
                    "note": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    "on": {"format": "date", "type": "string"},
                    "price": {
                        "anyOf": [{"exclusiveMinimum": 0, "multipleOf": 0.01, "type": "number"}, {"type": "string"}]
                    },
                    "record": {"anyOf": [{"$ref": "#/$defs/Part"}], "description": "the part"},
                    "spare": {"$ref": "#/$defs/Part"},
                    "tags": {"anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}]},
                    "title": {"type": "string"},
                    "when": {"description": "When (an ISO 8601 date and time)", "type": "string"},
                },
                "required": [
                    "at",
                    "description",
                    "level",
                    "note",
                    "on",
                    "price",
                    "record",
                    "spare",
                    "tags",
                    "title",
                    "when",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["records"],
    "additionalProperties": False,
}


def strict_provider(model="m", **kwargs):
    return OpenAICompatibleProvider(model, base_url=GATEWAY, **kwargs)


def refusal_of(schema, **kwargs):
    with pytest.raises(RationalizationError) as caught:
        strict_provider(**kwargs).transform_schema(schema)
    return str(caught.value)


class TestStrictTransform:
    def test_the_golden(self):
        assert strict_provider().transform_schema(PYDANTIC_LIKE) == PYDANTIC_LIKE_STRICT

    def test_the_golden_is_within_the_documented_subset(self):
        assert strict_violations(PYDANTIC_LIKE_STRICT) == []
        assert strict_violations(PYDANTIC_LIKE) != [], "vacuous: the validator accepts the untransformed schema"

    def test_every_object_is_closed_and_requires_every_property_in_properties_order(self):
        result = strict_provider().transform_schema(PYDANTIC_LIKE)

        objects = [result, result["$defs"]["Part"], result["properties"]["records"]["items"]]
        for node in objects:
            assert node["additionalProperties"] is False
            assert node["required"] == list(node["properties"])

    def test_a_list_that_is_not_required_is_sent_required_and_nullable(self):
        items = strict_provider().transform_schema(PYDANTIC_LIKE)["properties"]["records"]["items"]

        assert "tags" in items["required"]
        assert items["properties"]["tags"] == {
            "anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}]
        }

    def test_a_ref_with_a_description_keeps_it_beside_a_one_member_anyof(self):
        properties = strict_provider().transform_schema(PYDANTIC_LIKE)["properties"]["records"]["items"]["properties"]

        assert properties["record"] == {"anyOf": [{"$ref": "#/$defs/Part"}], "description": "the part"}

    @pytest.mark.parametrize("sibling", [{}, {"title": "T", "default": None}, {"description": 5}])
    def test_a_ref_without_a_text_description_stands_alone(self, sibling):
        schema = obj(a={"$ref": "#/$defs/X", **sibling}) | {"$defs": {"X": obj(b={"type": "string"})}}

        assert strict_provider().transform_schema(schema)["properties"]["a"] == {"$ref": "#/$defs/X"}

    def test_a_const_becomes_a_one_value_enum(self):
        part = strict_provider().transform_schema(PYDANTIC_LIKE)["$defs"]["Part"]

        assert part["properties"]["kind"] == {"enum": ["bolt"], "type": "string"}

    def test_a_const_beside_an_enum_is_dropped(self):
        schema = obj(a={"type": "string", "enum": ["x", "y"], "const": "x"})

        assert strict_provider().transform_schema(schema)["properties"]["a"] == {"enum": ["x", "y"], "type": "string"}

    @pytest.mark.parametrize("fmt", sorted(DOC_FORMATS))
    def test_a_documented_unambiguous_format_is_kept(self, fmt):
        schema = obj(a={"type": "string", "format": fmt})

        assert strict_provider().transform_schema(schema)["properties"]["a"]["format"] == fmt

    @pytest.mark.parametrize(
        ("fmt", "hint"), [("date-time", "an ISO 8601 date and time"), ("time", "an ISO 8601 time")]
    )
    def test_a_format_a_naive_value_breaks_is_sent_as_a_description_hint(self, fmt, hint):
        # RFC 3339 needs an offset, which a NaiveDatetime refuses: the format is a hint, pydantic checks the value.
        bare = strict_provider().transform_schema(obj(a={"type": "string", "format": fmt}))
        described = strict_provider().transform_schema(obj(a={"type": "string", "format": fmt, "description": "D"}))

        assert bare["properties"]["a"] == {"description": hint, "type": "string"}
        assert described["properties"]["a"] == {"description": f"D ({hint})", "type": "string"}

    @pytest.mark.parametrize("fmt", ["uri", "regex", "binary", "", 5, ["date"]])
    def test_any_other_format_is_dropped(self, fmt):
        schema = obj(a={"type": "string", "format": fmt})

        assert strict_provider().transform_schema(schema)["properties"]["a"] == {"type": "string"}

    def test_a_description_that_is_not_text_is_dropped(self):
        schema = obj(a={"type": "string", "description": ["x"]}) | {"description": 5}

        result = strict_provider().transform_schema(schema)

        assert "description" not in result
        assert result["properties"]["a"] == {"type": "string"}

    def test_type_keywords_are_kept_only_on_their_type(self):
        schema = obj(
            s={"type": "string", "minimum": 1, "minItems": 1, "pattern": "x"},
            n={"type": "number", "pattern": "x", "maxItems": 2, "maximum": 3},
            a={"type": "array", "items": {"type": "string"}, "pattern": "x", "minimum": 1, "minItems": 1},
            b={"type": "boolean", "pattern": "x", "minimum": 1, "minItems": 1},
        )

        properties = strict_provider().transform_schema(schema)["properties"]

        assert properties["s"] == {"pattern": "x", "type": "string"}
        assert properties["n"] == {"maximum": 3, "type": "number"}
        assert properties["a"] == {"items": {"type": "string"}, "minItems": 1, "type": "array"}
        assert properties["b"] == {"type": "boolean"}

    def test_a_fine_tuned_model_gets_no_constraint_keywords(self):
        schema = obj(
            s={"type": "string", "pattern": "x", "format": "email"},
            w={"type": "string", "format": "date-time"},
            n={
                "type": "number",
                "minimum": 1,
                "maximum": 2,
                "exclusiveMinimum": 0,
                "exclusiveMaximum": 3,
                "multipleOf": 1,
            },
            a={"type": "array", "items": {"type": "integer", "minimum": 0}, "minItems": 1, "maxItems": 2},
            e={"type": "string", "enum": ["x"], "description": "kept"},
        )

        properties = strict_provider("ft:gpt-4o-mini-2024-07-18:acme::abc123").transform_schema(schema)["properties"]

        assert properties == {
            "a": {"items": {"type": "integer"}, "type": "array"},
            "e": {"description": "kept", "enum": ["x"], "type": "string"},
            "n": {"type": "number"},
            "s": {"type": "string"},
            "w": {"description": "an ISO 8601 date and time", "type": "string"},
        }

    def test_only_a_model_name_starting_ft_is_fine_tuned(self):
        schema = obj(s={"type": "string", "pattern": "x"})

        assert strict_provider("gpt-4o-ft:x").transform_schema(schema)["properties"]["s"]["pattern"] == "x"

    def test_a_node_whose_type_is_a_list_keeps_only_the_common_keywords(self):
        schema = obj(a={"type": ["string", "null"], "pattern": "x", "title": "A"})

        assert strict_provider().transform_schema(schema)["properties"]["a"] == {"type": ["string", "null"]}

    def test_property_names_are_never_taken_for_keywords(self):
        names = ["title", "default", "examples", "$ref", "format", "const", "required", "additionalProperties"]
        schema = obj(**{name: {"type": "string"} for name in names})

        result = strict_provider().transform_schema(schema)

        assert sorted(result["properties"]) == sorted(names)
        assert result["required"] == sorted(names)

    def test_defs_names_are_never_taken_for_keywords(self):
        schema = obj(x={"$ref": "#/$defs/title"}) | {"$defs": {"title": obj(a={"type": "string"})}}

        result = strict_provider().transform_schema(schema)

        assert result["$defs"]["title"] == {
            "additionalProperties": False,
            "properties": {"a": {"type": "string"}},
            "required": ["a"],
            "type": "object",
        }

    def test_the_input_is_never_changed_and_each_result_is_fresh(self):
        before = json.dumps(PYDANTIC_LIKE, sort_keys=True)
        provider = strict_provider()

        first = provider.transform_schema(PYDANTIC_LIKE)
        first["$defs"]["Part"]["properties"].clear()
        second = provider.transform_schema(PYDANTIC_LIKE)

        assert json.dumps(PYDANTIC_LIKE, sort_keys=True) == before
        assert second == PYDANTIC_LIKE_STRICT

    def test_without_strict_the_schema_is_an_equal_fresh_copy(self):
        provider = strict_provider(strict=False)

        result = provider.transform_schema(PYDANTIC_LIKE)

        assert result == PYDANTIC_LIKE
        assert result is not PYDANTIC_LIKE
        assert result["$defs"] is not PYDANTIC_LIKE["$defs"]

    def test_without_strict_a_non_object_root_is_still_sent_as_is(self):
        assert strict_provider(strict=False).transform_schema({"anyOf": [{"type": "string"}]}) == {
            "anyOf": [{"type": "string"}]
        }

    @pytest.mark.parametrize(
        ("schema", "where"),
        [
            ({"type": "array", "items": {"type": "string"}}, "#"),
            ({"anyOf": [{"type": "object", "properties": {}}]}, "#"),
            ({"type": "object", "properties": {}, "anyOf": [{"type": "null"}]}, "#"),
            ({"type": "object", "properties": {"a": True}}, "#/properties/a"),
            ({"type": "object", "properties": {"a/b~": "x"}}, "#/properties/a~1b~0"),
            ({"type": "object", "properties": ["a"]}, "#"),
            ({"type": "object"}, "#"),
            (obj(a={"type": "object"}), "#/properties/a"),
            (obj(a={"type": "array", "items": [{"type": "string"}]}), "#/properties/a/items"),
            (obj(a={"anyOf": {"type": "null"}}), "#/properties/a"),
            ({"type": "object", "properties": {"a": {"anyOf": [False]}}}, "#/properties/a/anyOf/0"),
            ({"type": "object", "properties": {"a": {"type": "string"}}, "$defs": []}, "#"),
            ({"type": "object", "properties": {"a": {"type": "string"}}, "$defs": {"X": 1}}, "#/$defs/X"),
            (obj(a={"type": "object", "properties": {}, "$defs": {}}), "#/properties/a"),
            ({"type": "object", "properties": {"a": {"type": "object"}}}, "#/properties/a/anyOf/0"),
            (obj(a={"$ref": "#/$defs/Missing"}), "#/properties/a"),
            (obj(a={"$ref": "https://example.com/schema"}), "#/properties/a"),
            (obj(a={"$ref": 5}), "#/properties/a"),
        ],
    )
    def test_a_schema_outside_what_the_transform_can_express_is_refused_naming_where(self, schema, where):
        assert f"the response schema at {where} cannot be sent" in refusal_of(schema)

    def test_a_schema_that_is_not_a_mapping_is_a_type_error(self):
        with pytest.raises(TypeError, match="Mapping"):
            strict_provider().transform_schema([SMALL_SCHEMA])

    @pytest.mark.parametrize("strict", [True, False])
    def test_a_schema_that_is_not_exactly_json_is_refused(self, strict):
        with pytest.raises(RationalizationError):
            strict_provider(strict=strict).transform_schema(obj(a={1, 2}))
        with pytest.raises(RationalizationError):
            strict_provider(strict=strict).transform_schema({"type": "object", "minimum": math.nan})

    def test_the_walk_is_bounded(self, monkeypatch):
        monkeypatch.setattr(module, "SCHEMA_MAX_NODES", 3)
        schema = obj(**{name: {"type": "string"} for name in "abc"})

        with pytest.raises(RationalizationError, match="more than 3"):
            strict_provider().transform_schema(schema)

        monkeypatch.setattr(module, "SCHEMA_MAX_NODES", 4)
        assert strict_provider().transform_schema(schema)["required"] == ["a", "b", "c"]


def owners(schema):
    """Each object of a response wrapper a model's fields live in: the record, then each $defs entry."""
    found = {"record": schema["properties"]["records"]["items"]["properties"]["record"]}
    return found | {name: node for name, node in schema.get("$defs", {}).items() if node.get("type") == "object"}


def null_wrapped(prop):
    """``prop`` (a transformed property) as strict mode sends a defaulted field: required and nullable."""
    if "anyOf" in prop:
        return prop | {"anyOf": [*prop["anyOf"], {"type": "null"}]}
    inner = {key: value for key, value in prop.items() if key != "description"}
    return {"anyOf": [inner, {"type": "null"}]} | (
        {"description": prop["description"]} if "description" in prop else {}
    )


def part_schema(prop):
    """A schema whose Part definition holds a required ``number`` and the field ``qty`` (not required)."""
    part = {"type": "object", "properties": {"number": {"type": "string"}, "qty": prop}, "required": ["number"]}
    return obj(p={"$ref": "#/$defs/Part"}) | {"$defs": {"Part": part, "Color": {"type": "string", "enum": ["red"]}}}


class TestDefaultedFields:
    """Strict mode requires every field, so every field that is not required (a scalar, a model or a list, whatever
    its default) is sent required and nullable: null means "not shown", and consensus drops it so the caller's default
    applies, a factory's included (maintainer decision 2026-11-05, "null = not shown"). Optional fields are sent as
    they are."""

    @pytest.mark.parametrize(
        ("prop", "sent"),
        [
            ({"type": "integer", "default": 1, "title": "Qty"}, {"anyOf": [{"type": "integer"}, {"type": "null"}]}),
            (
                {"type": "integer", "default": 1, "description": "How many"},
                {"anyOf": [{"type": "integer"}, {"type": "null"}], "description": "How many"},
            ),
            ({"type": "integer", "minimum": 1}, {"anyOf": [{"minimum": 1, "type": "integer"}, {"type": "null"}]}),
            ({"$ref": "#/$defs/Color", "default": "red"}, {"anyOf": [{"$ref": "#/$defs/Color"}, {"type": "null"}]}),
            (
                {"$ref": "#/$defs/Color", "default": "red", "description": "The colour"},
                {"anyOf": [{"$ref": "#/$defs/Color"}, {"type": "null"}], "description": "The colour"},
            ),
            (
                {"anyOf": [{"type": "number"}, {"type": "string"}], "default": "1", "description": "Price"},
                {"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "null"}], "description": "Price"},
            ),
            (
                {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]},
                {
                    "anyOf": [
                        {
                            "additionalProperties": False,
                            "properties": {"x": {"type": "string"}},
                            "required": ["x"],
                            "type": "object",
                        },
                        {"type": "null"},
                    ]
                },
            ),
            (
                {"type": "string", "format": "date-time", "default": "2026-01-01T00:00:00"},
                {"anyOf": [{"description": "an ISO 8601 date and time", "type": "string"}, {"type": "null"}]},
            ),
            (
                {"type": "array", "items": {"type": "string"}, "default": ["x"]},
                {"anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}]},
            ),
            (
                {"type": "array", "items": {"type": "string"}, "default": []},
                {"anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}]},
            ),
            (
                {"type": "array", "items": {"type": "string"}, "description": "Tags"},
                {"anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}], "description": "Tags"},
            ),
        ],
        ids=[
            "int",
            "int-described",
            "no-default",
            "enum-ref",
            "enum-ref-described",
            "decimal",
            "inline-model",
            "datetime",
            "non-empty-list",
            "empty-list",
            "list-factory",
        ],
    )
    def test_a_defaulted_field_is_sent_required_and_nullable_with_its_description(self, prop, sent):
        part = strict_provider().transform_schema(part_schema(prop))["$defs"]["Part"]

        assert part["required"] == ["number", "qty"]
        assert part["properties"]["qty"] == sent
        assert part["properties"]["number"] == {"type": "string"}

    @pytest.mark.parametrize(
        ("prop", "sent"),
        [
            (
                {"anyOf": [{"type": "integer"}, {"type": "null"}], "default": None, "description": "D"},
                {"anyOf": [{"type": "integer"}, {"type": "null"}], "description": "D"},
            ),
            (
                {"anyOf": [{"$ref": "#/$defs/Color"}, {"type": "null"}], "default": None},
                {"anyOf": [{"$ref": "#/$defs/Color"}, {"type": "null"}]},
            ),
            ({"type": ["integer", "null"], "default": None}, {"type": ["integer", "null"]}),
            (
                {"anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}], "default": None},
                {"anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}]},
            ),
        ],
        ids=["optional-int", "optional-ref", "type-list-null", "optional-list"],
    )
    def test_an_optional_field_is_sent_unchanged(self, prop, sent):
        part = strict_provider().transform_schema(part_schema(prop))["$defs"]["Part"]

        assert part["required"] == ["number", "qty"]
        assert part["properties"]["qty"] == sent

    def test_a_required_field_is_never_made_nullable(self):
        schema = {"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]}

        assert strict_provider().transform_schema(schema)["properties"]["a"] == {"type": "integer"}

    def test_without_strict_a_defaulted_field_is_sent_as_given(self):
        schema = {"type": "object", "properties": {"qty": {"type": "integer", "default": 1}}}

        assert strict_provider(strict=False).transform_schema(schema) == schema

    def test_a_real_model_with_a_defaulted_int_gets_null_for_not_shown_and_its_default_after(self):
        class Line(BaseModel):
            code: str
            qty: int = 1
            tags: list[str] = []

        result = strict_provider().transform_schema(response_schema(Line))
        record = result["properties"]["records"]["items"]["properties"]["record"]
        answer_value = {"code": "PN-1", "qty": None, "tags": None}

        assert record["required"] == ["code", "qty", "tags"]
        assert record["properties"]["qty"] == {"anyOf": [{"type": "integer"}, {"type": "null"}]}
        assert record["properties"]["tags"] == {
            "anyOf": [{"items": {"type": "string"}, "type": "array"}, {"type": "null"}]
        }
        assert strict_violations(result) == []
        assert conforms(answer_value, record, result.get("$defs", {}))
        # What consensus (T10) does with null in a field that is not required: drops it, so the default applies.
        assert Line.model_validate({"code": "PN-1"}).model_dump() == {"code": "PN-1", "qty": 1, "tags": []}


class TestRealisticSchemas:
    """The strict transform of every realistic schema (test_schema.REALISTIC) passes and stays in the documented
    subset: each field that is not required and not Optional is required and nullable, every other field as it would
    be sent required."""

    @pytest.mark.parametrize("case", list(REALISTIC))
    def test_the_transform_passes_and_is_within_the_documented_subset(self, case):
        wrapper = response_schema(REALISTIC[case][0])

        result = strict_provider().transform_schema(wrapper)

        assert strict_violations(result) == []
        assert strict_violations(wrapper) != [], "vacuous: the wrapper already passes"

    @pytest.mark.parametrize("case", list(REALISTIC))
    def test_only_the_defaulted_fields_become_nullable(self, case):
        wrapper = response_schema(REALISTIC[case][0])
        sent = owners(strict_provider().transform_schema(wrapper))
        # The same schema with every default dropped: what each field is sent as when required.
        plain = owners(strict_provider().transform_schema(remedied(wrapper)))
        defaulted = set(REALISTIC_DEFAULTED[case])

        assert set(sent) == set(plain)
        for owner, node in sent.items():
            assert node["required"] == list(node["properties"])
            for name, prop in node["properties"].items():
                expected = plain[owner]["properties"][name]
                assert prop == (null_wrapped(expected) if (owner, name) in defaulted else expected), (owner, name)
        assert all(name in sent[owner]["properties"] for owner, name in defaulted), "vacuous: a field is missing"

    @pytest.mark.parametrize("case", list(REALISTIC))
    def test_a_record_with_null_in_its_defaulted_fields_conforms_and_validates_once_they_are_dropped(self, case):
        model, _, record = REALISTIC[case]
        wrapper = response_schema(model)
        result = strict_provider().transform_schema(wrapper)
        defs = wrapper.get("$defs", {})
        item = {"record": record, "grounding_quotes": ["q"], "source_element_ids": ["p0001_e001"]}

        with_nulls = filled({"records": [item]}, wrapper, defs, defaulted=set(REALISTIC_DEFAULTED[case]))

        assert conforms(with_nulls, result, result.get("$defs", {}))
        assert "None" in repr(with_nulls), "vacuous: no null was sent"
        dropped = without_nulls(with_nulls["records"][0]["record"], set(REALISTIC_DEFAULTED[case]), wrapper)
        assert model.model_validate(dropped) is not None
        assert not conforms({"records": [item]} | {"extra": 1}, result, result.get("$defs", {}))

    def test_a_naive_datetime_is_sent_without_a_format_and_a_naive_value_validates(self):
        wrapper = response_schema(REALISTIC["lab report"][0])

        specimen = strict_provider().transform_schema(wrapper)["$defs"]["Specimen"]["properties"]["collected_at"]

        assert specimen == {"description": "an ISO 8601 date and time", "type": "string"}

        class Reading(BaseModel):
            at: NaiveDatetime

        assert Reading.model_validate({"at": "2026-11-03T08:30:00"}).at.tzinfo is None


class TestLimits:
    """OpenAI's strict-mode limits, checked at preflight (review I1), only with strict=True."""

    def test_5000_properties_pass_and_5001_are_refused(self):
        assert strict_provider().transform_schema(obj(**{f"p{i}": {"type": "string"} for i in range(5000)}))

        message = refusal_of(
            obj(
                **{f"p{i}": {"type": "string"} for i in range(4000)},
                n=obj(**{f"q{i}": {"type": "string"} for i in range(1000)}),
            )
        )

        assert "5000" in message
        assert "5001" in message

    def test_a_definition_is_counted_once_however_often_it_is_used(self):
        part = obj(**{f"q{i}": {"type": "string"} for i in range(3000)})
        schema = obj(a={"$ref": "#/$defs/P"}, b={"$ref": "#/$defs/P"}) | {"$defs": {"P": part}}

        assert strict_provider().transform_schema(schema)["required"] == ["a", "b"]

    @staticmethod
    def nested(levels):
        """An object nesting ``levels`` objects and arrays in all, the root counted."""
        node = {"type": "string"}
        for index in range(levels - 1):
            node = obj(x=node) if index % 2 else {"type": "array", "items": node}
        return obj(x=node)

    def test_10_levels_pass_and_11_are_refused_objects_and_arrays_counted(self):
        assert strict_provider().transform_schema(self.nested(10))

        message = refusal_of(self.nested(11))

        assert "10 levels" in message
        assert "11" in message

    def test_nesting_is_followed_through_references(self):
        defs = {"D0": obj(x={"type": "string"})}
        for index in range(1, 10):
            defs[f"D{index}"] = obj(x={"$ref": f"#/$defs/D{index - 1}"})
        within = obj(top={"$ref": "#/$defs/D8"}) | {"$defs": defs}
        past = obj(top={"$ref": "#/$defs/D9"}) | {"$defs": defs}

        assert strict_provider().transform_schema(within)
        assert "11" in refusal_of(past)

    def test_nesting_counts_the_wrapper_levels(self):
        class Leaf(BaseModel):
            v: str

        models = [Leaf]
        for index in range(4):
            models.append(create_model(f"L{index}", children=(list[models[-1]], ...)))

        # root, records, item, record, then three list-of-model pairs: 4 + 6 = 10 levels; a fourth pair makes 12.
        assert strict_provider().transform_schema(response_schema(models[3]))
        assert "12" in refusal_of(response_schema(models[4]))

    @pytest.mark.parametrize("deep_first", [True, False])
    def test_a_definition_reached_at_two_depths_counts_the_deeper(self, deep_first):
        defs = {"D0": obj(x={"type": "string"})}
        for index in range(1, 8):
            defs[f"D{index}"] = obj(x={"$ref": f"#/$defs/D{index - 1}"})
        shallow, deep = {"$ref": "#/$defs/D7"}, obj(y=obj(z={"$ref": "#/$defs/D7"}))
        # root (1), then shallow: D7..D0 (8 more) = 9; deep: y (2), z (3), then D7..D0 = 11.
        schema = (obj(a=deep, b=shallow) if deep_first else obj(a=shallow, b=deep)) | {"$defs": defs}

        assert "11" in refusal_of(schema)

    def test_nesting_past_the_walks_cap_is_refused_without_suggesting_a_cycle(self):
        # Every cycle is refused before nesting is counted, so a count at the cap is a deep schema, never a cycle.
        node = {"type": "string"}
        for _ in range(45):
            node = {"type": "array", "items": node}

        message = refusal_of(obj(deep=node))

        assert "it nests 40 or more;" in message
        assert "cycle" not in message

    def test_a_reference_cycle_through_an_object_is_refused_naming_its_path(self):
        schema = obj(a={"$ref": "#/$defs/A"}) | {
            "$defs": {"A": obj(b={"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]})}
        }

        assert "recursive (a $ref cycle: #/$defs/A -> #/$defs/A)" in refusal_of(schema)

    def test_a_reference_cycle_through_anyof_alone_is_refused(self):
        # No object or array on the cycle: the nesting count never grows, so only the cycle check refuses it.
        schema = obj(a={"$ref": "#/$defs/A"}) | {"$defs": {"A": {"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}}}

        assert "recursive (a $ref cycle: #/$defs/A -> #/$defs/A)" in refusal_of(schema)

    def test_1000_enum_values_pass_and_1001_are_refused(self):
        within = obj(a={"type": "integer", "enum": list(range(600))}, b={"type": "integer", "enum": list(range(400))})
        past = obj(a={"type": "integer", "enum": list(range(600))}, b={"type": "integer", "enum": list(range(401))})

        assert strict_provider().transform_schema(within)
        message = refusal_of(past)
        assert "1000 enum values" in message
        assert "1001" in message

    def test_a_const_counts_as_an_enum_value(self):
        past = obj(a={"type": "integer", "enum": list(range(1000))}, b={"type": "string", "const": "x"})

        assert "1001" in refusal_of(past)

    def test_the_total_string_length_is_bounded(self):
        # property names + definition names + enum values: 2 + 3 + 119995 = 120000 passes; one character more fails.
        def schema(extra):
            return obj(a={"$ref": "#/$defs/Def"}, b={"type": "string", "enum": ["x" * (119_995 + extra)]}) | {
                "$defs": {"Def": {"type": "string", "enum": [""]}}
            }

        assert strict_provider().transform_schema(schema(0))
        message = refusal_of(schema(1))
        assert "120000" in message
        assert "120001" in message

    def test_a_non_string_enum_value_counts_its_json_text_in_the_total(self):
        # The limit names "enum values" without a type: an integer counts as its JSON text, the conservative reading.
        within = obj(a={"type": "integer", "enum": [10**120 + i for i in range(990)]})
        past = obj(a={"type": "integer", "enum": [10**120 + i for i in range(1000)]})

        assert strict_provider().transform_schema(within)
        assert "121001" in refusal_of(past)

    def test_the_long_enum_rule_counts_string_values_only(self):
        # "For a single enum property with string values": 300 integers of 60 digits (18,000 characters) pass it.
        schema = obj(a={"type": "integer", "enum": [10**59 + i for i in range(300)]})

        assert strict_provider().transform_schema(schema)["properties"]["a"]["enum"][0] == 10**59

    def test_a_long_enum_of_more_than_250_values_is_bounded_to_15000_characters(self):
        within = obj(a={"type": "string", "enum": [f"{i:03d}" + "x" * 56 for i in range(251)]})
        past = obj(a={"type": "string", "enum": [f"{i:03d}" + "x" * 57 for i in range(251)]})
        short = obj(a={"type": "string", "enum": [f"{i:03d}" + "x" * 97 for i in range(250)]})

        assert strict_provider().transform_schema(within)
        assert strict_provider().transform_schema(short)
        message = refusal_of(past)
        assert "15000" in message
        assert "15060" in message
        assert "#/properties/a" in message

    def test_the_limits_apply_only_in_strict_mode(self):
        schema = obj(**{f"p{i}": {"type": "string"} for i in range(5001)})

        assert strict_provider(strict=False).transform_schema(schema) == schema


# ---------------------------------------------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------------------------------------------


class TestIdentity:
    def test_openai(self):
        provider = OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY)

        assert provider.model_id == "openai:gpt-4o"
        assert provider.provider_kind == "openai-compatible"
        assert provider.rate_key == OPENAI_BASE_URL
        assert provider.model_family == "openai"

    def test_ollama_at_its_default_and_elsewhere(self):
        local = OpenAICompatibleProvider.ollama("llama3.1:8b")
        other = OpenAICompatibleProvider.ollama("llama3.1:8b", base_url="http://127.0.0.1:11500/v1")

        assert (local.model_id, local.rate_key) == ("ollama:llama3.1:8b", OLLAMA_BASE_URL)
        assert (other.model_id, other.rate_key) == (
            "ollama:llama3.1:8b@http://127.0.0.1:11500/v1",
            "http://127.0.0.1:11500/v1",
        )
        assert local.provider_kind == other.provider_kind == "openai-compatible"

    def test_a_gateway_names_its_endpoint(self):
        provider = OpenAICompatibleProvider("llama3", base_url=GATEWAY)

        assert provider.model_id == "openai-compatible:llama3@https://gw.example.com/v1"
        assert provider.rate_key == GATEWAY
        assert provider.provider_kind == "openai-compatible"

    def test_two_endpoints_never_share_a_model_id_or_a_rate_key(self):
        one = OpenAICompatibleProvider("llama3", base_url="https://gw.example.com/v1")
        two = OpenAICompatibleProvider("llama3", base_url="https://gw.example.com/v2")

        assert one.model_id != two.model_id
        assert one.rate_key != two.rate_key

    def test_one_endpoint_written_two_ways_has_one_model_id_and_rate_key(self):
        one = OpenAICompatibleProvider("llama3", base_url="https://gw.example.com/v1")
        two = OpenAICompatibleProvider("llama3", base_url="HTTPS://GW.Example.com:443/v1/")

        assert (one.model_id, one.rate_key) == (two.model_id, two.rate_key)

    def test_two_models_at_one_endpoint_share_the_rate_key_only(self):
        one = OpenAICompatibleProvider("llama3", base_url=GATEWAY)
        two = OpenAICompatibleProvider("mistral", base_url=GATEWAY)

        assert one.rate_key == two.rate_key
        assert one.model_id != two.model_id

    def test_the_key_never_changes_the_identity(self):
        with_key = OpenAICompatibleProvider("llama3", base_url=GATEWAY, api_key=KEY)
        without = OpenAICompatibleProvider("llama3", base_url=GATEWAY)

        assert (with_key.model_id, with_key.rate_key, with_key.generation_params) == (
            without.model_id,
            without.rate_key,
            without.generation_params,
        )

    def test_generation_params_are_exact_json_of_what_shapes_the_output(self):
        assert json.dumps(OpenAICompatibleProvider("m", base_url=GATEWAY).generation_params) == (
            '{"wire": "chat-completions/1", "temperature": 0.0, "seed": 0, "max_tokens_field": "max_tokens", '
            '"strict": true}'
        )
        params = OpenAICompatibleProvider.openai("m", api_key=KEY, temperature=1, seed=9).generation_params
        assert json.dumps(params) == (
            '{"wire": "chat-completions/1", "temperature": 1.0, "seed": 9, "max_tokens_field": '
            '"max_completion_tokens", "strict": true}'
        )

    def test_none_valued_parameters_are_left_out_of_generation_params(self):
        provider = OpenAICompatibleProvider("m", base_url=GATEWAY, temperature=None, seed=None, strict=False)

        assert provider.generation_params == {"wire": WIRE, "max_tokens_field": "max_tokens", "strict": False}

    def test_generation_params_is_a_fresh_mapping_each_time(self):
        provider = OpenAICompatibleProvider("m", base_url=GATEWAY)

        provider.generation_params["seed"] = 5

        assert provider.generation_params["seed"] == 0

    @pytest.mark.parametrize(
        "provider",
        [
            OpenAICompatibleProvider("m", base_url=GATEWAY),
            OpenAICompatibleProvider.ollama("m"),
            OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY),
        ],
        ids=["gateway", "ollama", "openai"],
    )
    def test_each_passes_the_preflight_check(self, provider):
        assert isinstance(provider, ModelProvider)
        assert check_provider(provider) is provider

    def test_an_identity_past_512_characters_is_refused_at_construction(self):
        url = "https://gw.example.com/" + "a" * 300

        with pytest.raises(RationalizationError, match="512"):
            OpenAICompatibleProvider("m" * 256, base_url=url)


class TestFamily:
    def test_openai_is_openai(self):
        assert OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY).model_family == "openai"

    @pytest.mark.parametrize(
        "provider",
        [OpenAICompatibleProvider("m", base_url=GATEWAY), OpenAICompatibleProvider.ollama("m")],
        ids=["gateway", "ollama"],
    )
    def test_a_gateway_or_ollama_is_unknown_unless_declared(self, provider):
        assert provider.model_family == UNKNOWN_FAMILY == "unknown"

    def test_a_declared_family_is_exposed(self):
        assert OpenAICompatibleProvider("llama3", base_url=GATEWAY, family="meta").model_family == "meta"
        assert OpenAICompatibleProvider.ollama("qwen3", family="qwen").model_family == "qwen"

    def test_the_family_is_neither_in_the_model_id_nor_in_generation_params(self):
        meta = OpenAICompatibleProvider("llama3", base_url=GATEWAY, family="meta")
        unknown = OpenAICompatibleProvider("llama3", base_url=GATEWAY)

        assert (meta.model_id, meta.rate_key, meta.generation_params) == (
            unknown.model_id,
            unknown.rate_key,
            unknown.generation_params,
        )

    @pytest.mark.parametrize("family", ["", "Meta", "me ta", "a" * 33, 5, None, "meta\n"])
    def test_a_bad_family_is_refused_without_echoing_it(self, family):
        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider("llama3", base_url=GATEWAY, family=family)

        assert "family" in str(caught.value)
        if isinstance(family, str) and family:
            assert repr(family) not in str(caught.value)

    def test_from_model_string_passes_the_family_through(self):
        compatible = parse_model_string("openai-compatible:llama3@https://gw.example.com/v1")

        assert OpenAICompatibleProvider.from_model_string(compatible, family="meta").model_family == "meta"
        assert (
            OpenAICompatibleProvider.from_model_string(parse_model_string("ollama:m"), family="qwen").model_family
            == "qwen"
        )

    def test_an_openai_string_takes_no_family(self):
        with pytest.raises(RationalizationError, match="family"):
            OpenAICompatibleProvider.from_model_string(parse_model_string("openai:gpt-4o"), api_key=KEY, family="meta")


class TestReprAndSecrets:
    def test_the_repr_names_the_model_and_hides_the_key(self):
        provider = OpenAICompatibleProvider("llama3", base_url=GATEWAY, api_key=KEY)

        for text in (repr(provider), str(provider), f"{provider}"):
            assert KEY not in text
            assert "hunter2" not in text
        assert "openai-compatible:llama3@https://gw.example.com/v1" in repr(provider)

    def test_the_provider_has_no_dict_to_leak(self):
        provider = OpenAICompatibleProvider("llama3", base_url=GATEWAY, api_key=KEY)

        assert not hasattr(provider, "__dict__")

    def test_a_half_built_provider_still_has_a_safe_repr(self):
        half = OpenAICompatibleProvider.__new__(OpenAICompatibleProvider)
        assert repr(half) == "OpenAICompatibleProvider(<unset>)"

    def test_a_constructor_error_after_the_key_holds_no_key_in_its_traceback_locals(self):
        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider("llama3", base_url=GATEWAY, api_key=KEY, temperature=9.0)

        assert_key_free(caught.value)

    def test_a_factory_error_after_the_key_holds_no_key_in_its_traceback_locals(self):
        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY, seed=1.5)

        assert_key_free(caught.value)

    @pytest.mark.parametrize("bad", [f"{KEY}\n", f" {KEY}", f"{KEY}é", f"{KEY}\x00"])
    def test_a_bad_key_is_refused_with_no_key_in_its_text_or_traceback_locals(self, bad):
        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider("llama3", base_url=GATEWAY, api_key=bad)

        assert "api_key" in str(caught.value)
        assert_key_free(caught.value)

    @pytest.mark.parametrize("bad", [f"{KEY}\n", f" {KEY}", ""])
    def test_a_bad_openai_key_is_refused_with_no_key_in_its_traceback_locals(self, bad):
        with pytest.raises(RationalizationError, match="api_key") as caught:
            OpenAICompatibleProvider.openai("gpt-4o", api_key=bad)

        assert_key_free(caught.value)

    def test_a_bad_environment_key_holds_no_key_in_its_traceback_locals(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", f"{KEY}\n")

        with pytest.raises(RationalizationError, match="OPENAI_API_KEY") as caught:
            OpenAICompatibleProvider.openai("gpt-4o")

        assert_key_free(caught.value)

    def test_a_bad_environment_key_via_a_model_string_holds_no_key_in_its_traceback_locals(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", f"{KEY}\n")

        with pytest.raises(RationalizationError, match="OPENAI_API_KEY") as caught:
            OpenAICompatibleProvider.from_model_string(parse_model_string("openai:gpt-4o"))

        assert_key_free(caught.value)

    @pytest.mark.parametrize(
        ("text", "options"),
        [
            ("openai:gpt-4o", {"api_key": f"{KEY}\n"}),
            ("openai:gpt-4o", {"api_key": KEY, "seed": 1.5}),
            ("openai:gpt-4o", {"api_key": KEY, "colour": "VALUE-crimson"}),
            ("openai:gpt-4o", {"api_kye": KEY}),
            ("ollama:llama3", {"api_key": KEY}),
            ("openai-compatible:m@https://gw.example.com/v1", {"api_key": KEY, "temperature": 3}),
            ("anthropic:claude-sonnet-4-5", {"api_key": KEY}),
        ],
        ids=["bad-key", "bad-seed", "unknown-option", "misspelt-key", "ollama-key", "bad-temperature", "other-vendor"],
    )
    def test_every_from_model_string_refusal_holds_no_key_in_its_traceback_locals(self, text, options):
        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider.from_model_string(parse_model_string(text), **options)

        assert_key_free(caught.value)

    def test_a_type_error_from_from_model_string_holds_no_key_in_its_traceback_locals(self):
        with pytest.raises(TypeError, match="ModelString") as caught:
            OpenAICompatibleProvider.from_model_string("openai:gpt-4o", api_key=KEY, api_kye=KEY)

        assert_key_free(caught.value)


# ---------------------------------------------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------------------------------------------


class TestConstructor:
    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            ({"model": ""}, "model"),
            ({"model": " llama3"}, "model"),
            ({"model": 5}, "model"),
            ({"model": "a" * 257}, "model"),
            ({"model": "https://evil.example.com/m"}, "model"),
            ({"base_url": "ftp://gw.example.com/v1"}, "endpoint URL"),
            ({"base_url": "http://10.0.0.5/v1"}, "endpoint URL"),
            ({"base_url": "https://user:pw@gw.example.com/v1"}, "endpoint URL"),
            ({"base_url": 5}, "endpoint URL"),
            ({"api_key": ""}, "api_key"),
            ({"api_key": "   "}, "api_key"),
            ({"api_key": 5}, "api_key"),
            ({"api_key": b"sk-bytes"}, "api_key"),
            ({"family": "Meta"}, "family"),
            ({"temperature": -0.1}, "temperature"),
            ({"temperature": 2.000001}, "temperature"),
            ({"temperature": math.nan}, "temperature"),
            ({"temperature": math.inf}, "temperature"),
            ({"temperature": "0"}, "temperature"),
            ({"temperature": True}, "temperature"),
            ({"temperature": 10**400}, "temperature"),
            ({"seed": 1.5}, "seed"),
            ({"seed": True}, "seed"),
            ({"seed": "1"}, "seed"),
            ({"seed": 2**63}, "seed"),
            ({"seed": -(2**63) - 1}, "seed"),
            ({"max_tokens_field": "max_output_tokens"}, "max_tokens_field"),
            ({"max_tokens_field": None}, "max_tokens_field"),
            ({"strict": 1}, "strict"),
            ({"strict": None}, "strict"),
            ({"allow_insecure_http": "yes"}, "allow_insecure_http"),
            ({"allow_insecure_http": 1}, "allow_insecure_http"),
            ({"max_connections": 0}, "max_connections"),
            ({"max_connections": 1025}, "max_connections"),
            ({"max_connections": True}, "max_connections"),
            ({"max_connections": 2.0}, "max_connections"),
            ({"transport": object()}, "transport"),
        ],
    )
    def test_each_bad_argument_is_refused_naming_it(self, kwargs, fragment):
        arguments = {"model": "llama3", "base_url": GATEWAY} | kwargs

        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider(arguments.pop("model"), **arguments)

        assert fragment in str(caught.value)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"temperature": 0},
            {"temperature": 2},
            {"temperature": 0.7},
            {"temperature": None},
            {"seed": None},
            {"seed": 2**63 - 1},
            {"seed": -(2**63)},
            {"max_tokens_field": "max_completion_tokens"},
            {"strict": False},
            {"max_connections": 1},
            {"max_connections": 1024},
            {"family": "a" * 32},
            {"family": "mistral-ai"},
            {"base_url": "http://10.0.0.5/v1", "allow_insecure_http": True},
            {"base_url": "http://127.0.0.1:8000/v1"},
            {"api_key": "x"},
        ],
    )
    def test_each_edge_value_is_accepted(self, kwargs):
        arguments = {"base_url": GATEWAY} | kwargs

        assert OpenAICompatibleProvider("llama3", **arguments).model_id.startswith("openai-compatible:llama3@")

    def test_the_options_are_keyword_only(self):
        with pytest.raises(TypeError):
            OpenAICompatibleProvider("llama3", GATEWAY)

    def test_insecure_http_reaches_the_http_helper(self):
        server = Server()
        provider = OpenAICompatibleProvider(
            "llama3", base_url="http://10.0.0.5/v1", allow_insecure_http=True, transport=transport(server)
        )

        provider.generate(make_request())

        assert str(server.only.url) == "http://10.0.0.5/v1/chat/completions"

    def test_the_http_helper_gets_the_pool_size_and_the_insecure_http_setting(self, monkeypatch):
        seen = []
        original = HttpClient.__init__

        def spy(self, **kwargs):
            seen.append((kwargs["max_connections"], kwargs["allow_insecure_http"]))
            original(self, **kwargs)

        monkeypatch.setattr(HttpClient, "__init__", spy)

        OpenAICompatibleProvider("llama3", base_url=GATEWAY, max_connections=3, allow_insecure_http=True)
        OpenAICompatibleProvider("llama3", base_url=GATEWAY)
        OpenAICompatibleProvider.ollama("llama3", base_url="http://10.0.0.5:11434/v1", allow_insecure_http=True)

        assert seen == [(3, True), (8, False), (8, True)]

    def test_max_connections_is_exposed_read_only(self):
        provider = OpenAICompatibleProvider("llama3", base_url=GATEWAY, max_connections=3)

        assert provider.max_connections == 3
        assert OpenAICompatibleProvider.ollama("llama3").max_connections == 8
        with pytest.raises(AttributeError):
            provider.max_connections = 4

    def test_open_opens_the_http_helper(self, monkeypatch):
        opened = []
        monkeypatch.setattr(HttpClient, "open", lambda self: opened.append(self))
        provider = OpenAICompatibleProvider("llama3", base_url=GATEWAY)

        assert provider.open() is None
        assert len(opened) == 1
        assert isinstance(opened[0], HttpClient)

    def test_open_raises_for_a_bad_environment(self, monkeypatch):
        monkeypatch.setenv("SSL_CERT_FILE", "/no/such/bundle.pem")

        with pytest.raises(RationalizationError, match="SSL_CERT_FILE"):
            OpenAICompatibleProvider("llama3", base_url=GATEWAY).open()


class TestFactories:
    def test_openai_reads_its_key_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", KEY)
        server = Server()

        OpenAICompatibleProvider.openai("gpt-4o", transport=transport(server)).generate(make_request())

        assert server.only.headers["authorization"] == f"Bearer {KEY}"

    def test_an_openai_model_string_prefers_the_key_it_is_given(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-from-the-environment")
        server = Server()

        provider = OpenAICompatibleProvider.from_model_string(
            parse_model_string("openai:gpt-4o"), api_key=KEY, transport=transport(server)
        )
        provider.generate(make_request())

        assert server.only.headers["authorization"] == f"Bearer {KEY}"

    def test_openai_prefers_the_key_it_is_given(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-from-the-environment")
        server = Server()

        OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY, transport=transport(server)).generate(make_request())

        assert server.only.headers["authorization"] == f"Bearer {KEY}"

    def test_an_empty_key_is_refused_never_replaced_by_the_environment(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", KEY)

        with pytest.raises(RationalizationError, match="api_key"):
            OpenAICompatibleProvider.openai("gpt-4o", api_key="")
        with pytest.raises(RationalizationError, match="api_key"):
            OpenAICompatibleProvider.from_model_string(parse_model_string("openai:gpt-4o"), api_key="")

    def test_openai_base_url_in_the_environment_is_ignored(self, monkeypatch):
        monkeypatch.setenv("OPENAI_BASE_URL", "https://elsewhere.example.com/v1")
        server = Server()

        provider = OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY, transport=transport(server))
        provider.generate(make_request())
        parsed = OpenAICompatibleProvider.from_model_string(parse_model_string("openai:gpt-4o"), api_key=KEY)

        assert str(server.only.url) == "https://api.openai.com/v1/chat/completions"
        assert (provider.rate_key, parsed.rate_key, parsed.model_id) == (
            OPENAI_BASE_URL,
            OPENAI_BASE_URL,
            "openai:gpt-4o",
        )

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_openai_without_a_key_is_refused_naming_the_variable(self, monkeypatch, value):
        if value is not None:
            monkeypatch.setenv("OPENAI_API_KEY", value)

        with pytest.raises(RationalizationError, match="OPENAI_API_KEY"):
            OpenAICompatibleProvider.openai("gpt-4o")

    def test_openai_with_a_bad_environment_key_names_the_variable_never_the_value(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", f"{KEY}\n")

        with pytest.raises(RationalizationError, match="OPENAI_API_KEY") as caught:
            OpenAICompatibleProvider.openai("gpt-4o")

        assert KEY not in str(caught.value)

    def test_openai_limits_output_with_max_completion_tokens_by_default(self):
        provider = OpenAICompatibleProvider.openai("gpt-4o", api_key=KEY)

        assert provider.generation_params["max_tokens_field"] == "max_completion_tokens"

    def test_openai_passes_its_options_on(self):
        provider = OpenAICompatibleProvider.openai(
            "o3", api_key=KEY, temperature=None, seed=None, max_tokens_field="max_tokens", strict=False
        )

        assert provider.generation_params == {"wire": WIRE, "max_tokens_field": "max_tokens", "strict": False}

    def test_a_fine_tuned_openai_model_is_accepted(self):
        provider = OpenAICompatibleProvider.openai("ft:gpt-4o-mini-2024-07-18:acme::abc123", api_key=KEY)

        assert provider.model_id == "openai:ft:gpt-4o-mini-2024-07-18:acme::abc123"

    def test_ollama_never_sends_a_key_even_with_one_in_the_environment(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", KEY)
        server = Server()

        OpenAICompatibleProvider.ollama("llama3", transport=transport(server)).generate(make_request())

        assert "authorization" not in server.only.headers

    def test_ollama_limits_output_with_max_tokens_by_default(self):
        assert OpenAICompatibleProvider.ollama("llama3").generation_params["max_tokens_field"] == "max_tokens"

    def test_ollama_on_another_host_needs_insecure_http(self):
        with pytest.raises(RationalizationError, match="endpoint URL"):
            OpenAICompatibleProvider.ollama("llama3", base_url="http://10.0.0.5:11434/v1")

        provider = OpenAICompatibleProvider.ollama(
            "llama3", base_url="http://10.0.0.5:11434/v1", allow_insecure_http=True
        )
        assert provider.rate_key == "http://10.0.0.5:11434/v1"

    def test_the_constructor_never_reads_the_environment(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", KEY)
        server = Server()

        OpenAICompatibleProvider("gpt-4o", base_url=OPENAI_BASE_URL, transport=transport(server)).generate(
            make_request()
        )

        assert "authorization" not in server.only.headers


def factory_options(factory, *skipped):
    """The keyword options of a factory (or the constructor) and their defaults, skipping the named ones."""
    parameters = inspect.signature(factory).parameters.values()
    return {
        parameter.name: parameter.default
        for parameter in parameters
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.name not in skipped
    }


class TestFromModelString:
    def test_openai(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", KEY)

        provider = OpenAICompatibleProvider.from_model_string(parse_model_string("openai:gpt-4o"))

        assert provider.model_id == "openai:gpt-4o"
        assert provider.rate_key == OPENAI_BASE_URL
        assert provider.model_family == "openai"
        assert provider.generation_params["max_tokens_field"] == "max_completion_tokens"

    def test_openai_without_a_key_is_refused(self):
        with pytest.raises(RationalizationError, match="OPENAI_API_KEY"):
            OpenAICompatibleProvider.from_model_string(parse_model_string("openai:gpt-4o"))

    def test_openai_naming_another_endpoint_is_refused(self):
        string = ModelString("openai", "gpt-4o", GATEWAY)

        with pytest.raises(RationalizationError, match="openai-compatible"):
            OpenAICompatibleProvider.from_model_string(string, api_key=KEY)

    def test_openai_naming_its_own_endpoint_is_accepted(self):
        string = ModelString("openai", "gpt-4o", OPENAI_BASE_URL)

        assert OpenAICompatibleProvider.from_model_string(string, api_key=KEY).model_id == "openai:gpt-4o"

    def test_ollama(self):
        server = Server()

        provider = OpenAICompatibleProvider.from_model_string(
            parse_model_string("ollama:llama3.1:8b"), transport=transport(server)
        )
        provider.generate(make_request())

        assert provider.model_id == "ollama:llama3.1:8b"
        assert str(server.only.url) == "http://localhost:11434/v1/chat/completions"
        assert "authorization" not in server.only.headers

    def test_ollama_without_a_base_url_uses_the_default(self):
        provider = OpenAICompatibleProvider.from_model_string(ModelString("ollama", "llama3", None))

        assert (provider.model_id, provider.rate_key) == ("ollama:llama3", OLLAMA_BASE_URL)

    def test_ollama_at_another_endpoint(self):
        string = ModelString("ollama", "llama3", "http://127.0.0.1:11500/v1")

        provider = OpenAICompatibleProvider.from_model_string(string)

        assert provider.model_id == "ollama:llama3@http://127.0.0.1:11500/v1"

    def test_openai_compatible(self):
        string = parse_model_string("openai-compatible:meta-llama/Llama-3-8b@https://GW.example.com/v1/")

        provider = OpenAICompatibleProvider.from_model_string(string, api_key=KEY, temperature=0.5)

        assert provider.model_id == "openai-compatible:meta-llama/Llama-3-8b@https://gw.example.com/v1"
        assert provider.rate_key == GATEWAY
        assert provider.generation_params["temperature"] == 0.5
        assert provider.generation_params["max_tokens_field"] == "max_tokens"

    def test_the_model_id_of_a_parsed_string_parses_back_to_it(self):
        for text in ("openai-compatible:llama3@https://gw.example.com/v1", "ollama:llama3.1:8b"):
            provider = OpenAICompatibleProvider.from_model_string(parse_model_string(text))

            assert provider.model_id == text

    @pytest.mark.parametrize(
        ("text", "factory", "skipped"),
        [
            ("openai:gpt-4o", OpenAICompatibleProvider.openai, ()),
            ("ollama:llama3", OpenAICompatibleProvider.ollama, ("base_url",)),
            ("openai-compatible:m@https://gw.example.com/v1", OpenAICompatibleProvider, ("base_url",)),
        ],
        ids=["openai", "ollama", "openai-compatible"],
    )
    def test_each_kind_takes_exactly_its_factorys_options_with_its_defaults(self, text, factory, skipped):
        key = {"api_key": KEY} if "api_key" in factory_options(factory) else {}
        options = factory_options(factory, *skipped) | key
        string = parse_model_string(text)
        endpoint = {"base_url": string.base_url} if factory is OpenAICompatibleProvider else {}

        explicit = OpenAICompatibleProvider.from_model_string(string, **options)
        implicit = OpenAICompatibleProvider.from_model_string(string, **key)
        direct = factory(string.model, **endpoint, **options)

        for provider in (implicit, direct):
            assert provider.model_id == explicit.model_id
            assert provider.generation_params == explicit.generation_params
            assert (provider.model_family, provider.max_connections) == (
                explicit.model_family,
                explicit.max_connections,
            )

    @pytest.mark.parametrize(
        ("text", "options", "named"),
        [
            (
                "openai:gpt-4o",
                {"api_key": KEY, "colour": "VALUE-crimson", "allow_insecure_http": True},
                "allow_insecure_http, colour",
            ),
            ("openai:gpt-4o", {"api_key": KEY, "base_url": GATEWAY}, "base_url"),
            ("ollama:llama3", {"api_key": KEY}, "api_key"),
            ("ollama:llama3", {"base_url": GATEWAY}, "base_url"),
            ("openai-compatible:m@https://gw.example.com/v1", {"Temperature": 0.5}, "Temperature"),
        ],
    )
    def test_an_unknown_option_is_refused_naming_only_its_name(self, text, options, named):
        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider.from_model_string(parse_model_string(text), **options)

        message = str(caught.value)
        assert named in message
        assert KEY not in message
        assert "VALUE-crimson" not in message
        assert GATEWAY not in message

    @pytest.mark.parametrize("text", ["anthropic:claude-sonnet-4-5", "gemini:gemini-2.5-pro"])
    def test_another_vendors_string_is_refused(self, text):
        with pytest.raises(RationalizationError, match="openai, ollama"):
            OpenAICompatibleProvider.from_model_string(parse_model_string(text))

    @pytest.mark.parametrize("value", ["openai:gpt-4o", None, {"kind": "openai"}])
    def test_anything_but_a_model_string_is_a_type_error(self, value):
        with pytest.raises(TypeError, match="ModelString"):
            OpenAICompatibleProvider.from_model_string(value)

    def test_a_bad_option_is_refused(self):
        with pytest.raises(RationalizationError, match="temperature"):
            OpenAICompatibleProvider.from_model_string(parse_model_string("ollama:llama3"), temperature=3)


# ---------------------------------------------------------------------------------------------------------------
# Google Cloud: open and partner models, Mistral, dedicated endpoints
# ---------------------------------------------------------------------------------------------------------------

PROJECT = "my-project"
TOKEN = "ya29.test-TOKEN-secret"
LLAMA = "meta/llama-4-maverick-17b-128e-instruct-maas"
CHAT_BODY_HEAD = (
    '"messages":[{"role":"system","content":"SYSTEM-PROMPT-TEXT"},'
    '{"role":"user","content":"USER-ENVELOPE-TEXT Größe"}],'
    '"response_format":{"type":"json_schema","json_schema":{"name":"records","schema":{"additionalProperties":false,'
    '"properties":{"records":{"items":{"type":"string"},"type":"array"}},"required":["records"],"type":"object"},'
)


class Tokens:
    """A token source: a new token on each call, counted."""

    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return f"{TOKEN}-{self.calls}"


def cloud(model, server=None, *, location="us-central1", tokens=None, **kwargs):
    """A Google Cloud provider with a token source, over a mock transport when a server is given."""
    extra = {} if server is None else {"transport": transport(server)}
    auth = GoogleCloudAuth(token=Tokens() if tokens is None else tokens)
    return OpenAICompatibleProvider.google_cloud(
        model, project=PROJECT, location=location, auth=auth, **extra, **kwargs
    )


class TestGoogleCloudRequests:
    @pytest.mark.parametrize(
        ("model", "location", "url", "strict"),
        [
            (
                LLAMA,
                "us-central1",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/endpoints/"
                "openapi/chat/completions",
                "true",
            ),
            (
                "google/gemini-2.5-flash",
                "global",
                "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/endpoints/openapi/"
                "chat/completions",
                "true",
            ),
            (
                "deepseek-ai/deepseek-v3.2-maas",
                "us",
                "https://aiplatform.us.rep.googleapis.com/v1/projects/my-project/locations/us/endpoints/openapi/"
                "chat/completions",
                "true",
            ),
            (
                "qwen/qwen3-next-80b-a3b-instruct-maas",
                "eu",
                "https://aiplatform.eu.rep.googleapis.com/v1/projects/my-project/locations/eu/endpoints/openapi/"
                "chat/completions",
                "true",
            ),
            (
                "xai/grok-4.6",
                "global",
                "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/endpoints/openapi/"
                "chat/completions",
                "true",
            ),
            (
                "openai/gpt-oss-120b-maas@001",
                "us-central1",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/endpoints/"
                "openapi/chat/completions",
                "true",
            ),
        ],
        ids=["meta-regional", "gemini-global", "deepseek-us", "qwen-eu", "xai-global", "gpt-oss-versioned"],
    )
    def test_the_shared_endpoint_golden(self, model, location, url, strict):
        server = Server()
        tokens = Tokens()

        cloud(model, server, location=location, tokens=tokens).generate(make_request())

        sent = server.only
        assert (sent.method, str(sent.url)) == ("POST", url)
        assert sent.headers["authorization"] == f"Bearer {TOKEN}-1"
        assert sent.content.decode("utf-8") == (
            f'{{"model":"{model}",{CHAT_BODY_HEAD}"strict":{strict}}}}},"max_tokens":1024,"temperature":0.0,"seed":0}}'
        )

    @pytest.mark.parametrize(
        ("model", "location", "url", "body_model"),
        [
            (
                "mistralai/mistral-small-2503@001",
                "us-central1",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/publishers/"
                "mistralai/models/mistral-small-2503@001:rawPredict",
                "mistral-small-2503",
            ),
            (
                "mistralai/codestral-2",
                "global",
                "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/publishers/mistralai/models/"
                "codestral-2:rawPredict",
                "codestral-2",
            ),
            (
                "mistralai/mistral-medium-3@001",
                "eu",
                "https://aiplatform.eu.rep.googleapis.com/v1/projects/my-project/locations/eu/publishers/mistralai/"
                "models/mistral-medium-3@001:rawPredict",
                "mistral-medium-3",
            ),
        ],
        ids=["versioned-regional", "bare-global", "versioned-eu"],
    )
    def test_the_mistral_golden_keeps_the_version_in_the_url_only(self, model, location, url, body_model):
        # Google documents user and assistant turns only; a system turn is sent anyway (module notes, 7).
        server = Server()

        cloud(model, server, location=location).generate(make_request())

        sent = server.only
        assert str(sent.url) == url
        assert sent.headers["authorization"] == f"Bearer {TOKEN}-1"
        assert sent.content.decode("utf-8") == (
            f'{{"model":"{body_model}",{CHAT_BODY_HEAD}"strict":false}}}},"max_tokens":1024,"stream":false,'
            '"temperature":0.0,"random_seed":0}'
        )

    def test_mistral_sends_a_set_temperature_and_leaves_out_a_none_seed(self):
        server = Server()

        cloud("mistralai/codestral-2", server, temperature=0.3, seed=None).generate(make_request())

        body = json.loads(server.only.content)
        assert body["temperature"] == 0.3
        assert "random_seed" not in body
        assert "seed" not in body

    @pytest.mark.parametrize(
        ("location", "url"),
        [
            (
                "us-central1",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/endpoints/"
                "1234567890/chat/completions",
            ),
            (
                "global",
                "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/endpoints/1234567890/"
                "chat/completions",
            ),
        ],
        ids=["regional", "global"],
    )
    def test_the_dedicated_endpoint_golden_sends_no_model(self, location, url):
        server = Server()

        cloud(None, server, location=location, endpoint_id="1234567890").generate(make_request())

        sent = server.only
        assert str(sent.url) == url
        assert sent.headers["authorization"] == f"Bearer {TOKEN}-1"
        assert sent.content.decode("utf-8") == (
            f'{{{CHAT_BODY_HEAD}"strict":false}}}},"max_tokens":1024,"temperature":0.0,"seed":0}}'
        )

    @pytest.mark.parametrize("model", [LLAMA, "mistralai/codestral-2", None], ids=["openapi", "mistral", "endpoint"])
    def test_the_temperature_is_0_by_default_a_set_one_is_sent_and_none_leaves_it_out(self, model):
        endpoint = {"endpoint_id": "1234567890"} if model is None else {}
        default, warm, none = Server(), Server(), Server()

        cloud(model, default, **endpoint).generate(make_request())
        cloud(model, warm, temperature=0.5, **endpoint).generate(make_request())
        cloud(model, none, temperature=None, **endpoint).generate(make_request())

        assert json.loads(default.only.content)["temperature"] == 0.0
        assert json.loads(warm.only.content)["temperature"] == 0.5
        assert "temperature" not in json.loads(none.only.content)

    def test_the_token_is_fetched_once_per_request_never_at_construction(self):
        server, tokens = Server(), Tokens()
        provider = cloud(LLAMA, server, tokens=tokens)

        assert tokens.calls == 0
        provider.generate(make_request())
        provider.generate(make_request())

        assert tokens.calls == 2
        assert [request.headers["authorization"] for request in server.requests] == [
            f"Bearer {TOKEN}-1",
            f"Bearer {TOKEN}-2",
        ]

    def test_the_token_is_fetched_after_the_deadline_is_set(self, monkeypatch):
        # A slow token source spends the request's own time: the deadline is set first.
        real = time.monotonic
        lag = {"s": 0.0}
        seen = []
        original = HttpClient.post_json

        def slow_token():
            lag["s"] += 20.0
            return TOKEN

        def spy(self, url, **kwargs):
            seen.append(kwargs["deadline"])
            return original(self, url, **kwargs)

        monkeypatch.setattr(module.time, "monotonic", lambda: real() + lag["s"])
        monkeypatch.setattr(HttpClient, "post_json", spy)
        provider = cloud(LLAMA, Server(), tokens=slow_token)

        before = real()
        provider.generate(make_request(timeout_s=30.0))

        (deadline,) = seen
        assert deadline < before + 30.0 + 10.0

    @pytest.mark.parametrize(
        "source",
        [
            lambda: (_ for _ in ()).throw(RuntimeError(f"refresh failed for {TOKEN}")),
            lambda: None,
            lambda: f"Bearer {TOKEN}",
            lambda: f"{TOKEN} with space",
        ],
        ids=["raises", "none", "bearer", "space"],
    )
    def test_a_failing_token_source_is_permanent_sends_nothing_and_leaks_nothing(self, source):
        server = Server()

        with pytest.raises(PermanentProviderError) as caught:
            cloud(LLAMA, server, tokens=source).generate(make_request())

        assert server.requests == []
        assert_key_free(caught.value, key=TOKEN)

    def test_a_401_from_google_carries_no_token(self):
        server = Server(401, {"error": {"message": f"Request had invalid authentication credentials: {TOKEN}-1"}})

        with pytest.raises(PermanentProviderError) as caught:
            cloud(LLAMA, server).generate(make_request())

        assert caught.value.status_code == 401
        assert_key_free(caught.value, key=TOKEN)

    @pytest.mark.parametrize(
        ("body", "finish", "content"),
        [
            (completion(), "complete", json.loads(RECORDS)),
            (completion(finish_reason="length"), "truncated", json.loads(RECORDS)),
            (completion(content=None, refusal="no"), "refused", None),
            (completion(content=f"```json\n{RECORDS}\n```"), "complete", None),
        ],
        ids=["complete", "length", "refusal", "fenced"],
    )
    @pytest.mark.parametrize("model", [LLAMA, "mistralai/codestral-2", None], ids=["openapi", "mistral", "endpoint"])
    def test_the_reply_is_mapped_as_on_the_direct_path(self, body, finish, content, model):
        endpoint = {"endpoint_id": "1234567890"} if model is None else {}
        direct = gateway(Server(200, body)).generate(make_request())

        result = cloud(model, Server(200, body), **endpoint).generate(make_request())

        assert result == direct
        assert (result.finish, result.content, result.input_tokens) == (finish, content, 19)


class TestGoogleCloudIdentity:
    @pytest.mark.parametrize(
        ("model", "endpoint_id", "model_id", "kind", "params"),
        [
            (
                LLAMA,
                None,
                "google-cloud:projects/my-project/locations/us-central1/publishers/meta/models/"
                "llama-4-maverick-17b-128e-instruct-maas",
                "google-cloud-openai",
                {
                    "wire": "chat-completions/1",
                    "temperature": 0.0,
                    "seed": 0,
                    "max_tokens_field": "max_tokens",
                    "strict": True,
                },
            ),
            (
                "mistralai/mistral-small-2503@001",
                None,
                "google-cloud:projects/my-project/locations/us-central1/publishers/mistralai/models/"
                "mistral-small-2503@001",
                "google-cloud-mistral",
                {
                    "wire": "mistral-raw-predict/2",
                    "temperature": 0.0,
                    "seed": 0,
                    "max_tokens_field": "max_tokens",
                    "strict": False,
                },
            ),
            (
                None,
                "1234567890",
                "google-cloud:projects/my-project/locations/us-central1/endpoints/1234567890",
                "google-cloud-endpoint",
                {
                    "wire": "chat-completions/1",
                    "temperature": 0.0,
                    "seed": 0,
                    "max_tokens_field": "max_tokens",
                    "strict": False,
                },
            ),
        ],
        ids=["openapi", "mistral", "endpoint"],
    )
    def test_identity_per_path(self, model, endpoint_id, model_id, kind, params):
        provider = cloud(model, endpoint_id=endpoint_id)

        assert provider.model_id == model_id
        assert provider.provider_kind == kind
        assert provider.rate_key == model_id  # Vertex quotas are per model
        assert provider.generation_params == params
        assert isinstance(provider, ModelProvider)
        assert check_provider(provider) is provider

    def test_each_location_has_its_own_identity(self):
        providers = [
            cloud(LLAMA, location=location) for location in ("us-central1", "europe-west4", "us", "eu", "global")
        ]

        assert len({provider.model_id for provider in providers}) == 5
        assert [provider.rate_key for provider in providers] == [provider.model_id for provider in providers]
        assert providers[2].rate_key == (
            "google-cloud:projects/my-project/locations/us/publishers/meta/models/llama-4-maverick-17b-128e-instruct-maas"
        )

    def test_two_models_of_one_project_and_location_get_their_own_rate_keys(self):
        assert cloud(LLAMA).rate_key != cloud("qwen/qwen3-next-80b-a3b-instruct-maas").rate_key

    def test_a_deployed_model_label_rekeys_a_dedicated_endpoint_but_shares_its_rate_key(self):
        bare = cloud(None, endpoint_id="1234567890")
        first = cloud(None, endpoint_id="1234567890", deployed_model="gemma-2-9b-it")
        second = cloud(None, endpoint_id="1234567890", deployed_model="gemma-2-9b-it@v2")

        assert first.model_id == f"{bare.model_id}#model=gemma-2-9b-it"
        assert len({bare.model_id, first.model_id, second.model_id}) == 3
        assert bare.rate_key == first.rate_key == second.rate_key == bare.model_id
        assert check_provider(second) is second

    def test_a_deployed_model_label_is_never_sent(self):
        server = Server()

        cloud(None, server, endpoint_id="1234567890", deployed_model="gemma-2-9b-it").generate(make_request())

        assert "model" not in json.loads(server.only.content)
        assert b"gemma" not in server.only.content

    def test_the_identity_is_distinct_from_the_direct_providers(self):
        on_google = cloud("openai/gpt-oss-120b-maas")
        direct = [
            OpenAICompatibleProvider.openai("gpt-oss-120b-maas", api_key=KEY),
            OpenAICompatibleProvider.ollama("gpt-oss-120b-maas"),
            OpenAICompatibleProvider("openai/gpt-oss-120b-maas", base_url=GATEWAY),
        ]

        for other in direct:
            assert other.model_id != on_google.model_id
            assert other.rate_key != on_google.rate_key
            assert other.provider_kind != on_google.provider_kind

    @pytest.mark.parametrize(
        ("model", "family"),
        [
            (LLAMA, "meta"),
            ("deepseek-ai/deepseek-v3.2-maas", "deepseek"),
            ("qwen/qwen3-next-80b-a3b-instruct-maas", "qwen"),
            ("openai/gpt-oss-120b-maas", "openai"),
            ("moonshotai/kimi-k2-thinking-maas", "moonshot"),
            ("minimaxai/minimax-m2-maas", "minimax"),
            ("zai-org/glm-5.2-maas", "zai"),
            ("google/gemma-3-27b-it-maas", "google"),
            ("google/gemini-2.5-flash", "google"),
            ("xai/grok-4.6", "xai"),
            ("mistralai/codestral-2", "mistral"),
            ("acme/model-1", "unknown"),
        ],
    )
    def test_the_family_comes_from_the_publisher(self, model, family):
        assert cloud(model).model_family == family

    def test_a_family_that_agrees_with_the_publisher_is_accepted(self):
        assert cloud(LLAMA, family="meta").model_family == "meta"

    def test_a_family_that_disagrees_with_the_publisher_is_refused(self):
        with pytest.raises(RationalizationError) as caught:
            cloud(LLAMA, family="qwen")

        assert "meta" in str(caught.value)
        assert "family" in str(caught.value)

    def test_an_unmapped_publisher_or_a_dedicated_endpoint_takes_the_declared_family(self):
        assert cloud("acme/model-1", family="acme-labs").model_family == "acme-labs"
        assert cloud(None, endpoint_id="1234567890").model_family == "unknown"
        assert cloud(None, endpoint_id="1234567890", family="meta").model_family == "meta"

    @pytest.mark.parametrize("family", ["Meta", "", 5])
    def test_a_bad_family_is_refused(self, family):
        with pytest.raises(RationalizationError, match="family"):
            cloud("acme/model-1", family=family)

    @pytest.mark.parametrize(
        ("model", "endpoint_id", "strict"),
        [
            (LLAMA, None, True),
            ("google/gemini-2.5-flash", None, True),
            ("deepseek-ai/deepseek-v3.2-maas", None, True),
            ("xai/grok-4.6", None, True),
            ("mistralai/codestral-2", None, False),
            (None, "1234567890", False),
        ],
        ids=["meta", "gemini", "deepseek", "xai", "mistral", "endpoint"],
    )
    def test_strict_defaults_per_path_and_the_caller_overrides_them(self, model, endpoint_id, strict):
        assert cloud(model, endpoint_id=endpoint_id).generation_params["strict"] is strict
        assert cloud(model, endpoint_id=endpoint_id, strict=not strict).generation_params["strict"] is (not strict)

    def test_strict_applies_the_strict_transform_and_its_rules(self):
        schema = obj(a={"type": "string", "title": "A"})
        defaulted = {"type": "object", "title": "Line", "properties": {"qty": {"type": "integer", "default": 1}}}
        nullable = {
            "additionalProperties": False,
            "properties": {"qty": {"anyOf": [{"type": "integer"}, {"type": "null"}]}},
            "required": ["qty"],
            "type": "object",
        }

        assert cloud(LLAMA).transform_schema(schema) == {
            "additionalProperties": False,
            "properties": {"a": {"type": "string"}},
            "required": ["a"],
            "type": "object",
        }
        assert cloud("mistralai/codestral-2").transform_schema(schema) == schema
        patterned = cloud(LLAMA).transform_schema(obj(a={"type": "string", "pattern": "^x"}))
        assert patterned["properties"]["a"] == {"pattern": "^x", "type": "string"}
        assert cloud(LLAMA).transform_schema(defaulted) == nullable

    def test_max_connections_and_repr(self):
        provider = cloud(LLAMA, max_connections=3)

        assert provider.max_connections == 3
        assert "google-cloud:projects/my-project" in repr(provider)
        assert "GoogleCloudAuth(kind='token')" in repr(provider)
        assert TOKEN not in repr(provider)

    def test_the_options_are_keyword_only(self):
        parameters = inspect.signature(OpenAICompatibleProvider.google_cloud).parameters

        assert list(parameters) == [
            "model",
            "project",
            "location",
            "auth",
            "endpoint_id",
            "deployed_model",
            "family",
            "strict",
            "temperature",
            "seed",
            "max_connections",
            "transport",
        ]
        assert [parameters[name].default for name in list(parameters)[4:]] == [None, None, None, None, 0.0, 0, 8, None]
        assert all(parameters[name].kind is inspect.Parameter.KEYWORD_ONLY for name in list(parameters)[1:])


class TestGoogleCloudRefusals:
    @pytest.mark.parametrize("model", [LLAMA, "mistralai/codestral-2", None], ids=["openapi", "mistral", "endpoint"])
    def test_an_api_key_is_refused_at_construction(self, model):
        endpoint = {"endpoint_id": "1234567890"} if model is None else {}

        with pytest.raises(RationalizationError, match="token") as caught:
            OpenAICompatibleProvider.google_cloud(
                model, project=PROJECT, location="us-central1", auth=GoogleCloudAuth(api_key=KEY), **endpoint
            )

        assert_key_free(caught.value)

    @pytest.mark.parametrize(
        ("arguments", "fragment"),
        [
            ({"model": LLAMA, "auth": KEY}, "GoogleCloudAuth"),
            ({"model": LLAMA, "project": "Bad_Project"}, "project"),
            ({"model": LLAMA, "location": "US-Central1"}, "location"),
            ({"model": LLAMA, "project": None, "location": "global"}, "express"),
            ({"model": "llama-4-maverick"}, "publisher/model"),
            ({"model": "meta/llama/4"}, "publisher/model"),
            ({"model": "meta/llama 4"}, "model"),
            ({"model": None}, "publisher/model"),
            ({"model": "anthropic/claude-opus-4-8"}, "AnthropicProvider.google_cloud"),
            ({"model": "ai21/jamba-1.5-large@001"}, "ai21"),
            ({"model": LLAMA, "family": "qwen"}, "family"),
            ({"model": LLAMA, "family": "Meta"}, "family"),
            ({"model": LLAMA, "strict": "yes"}, "strict"),
            ({"model": LLAMA, "temperature": 3.0}, "temperature"),
            ({"model": LLAMA, "seed": 1.5}, "seed"),
            ({"model": LLAMA, "max_connections": 0}, "max_connections"),
            ({"model": LLAMA, "transport": object()}, "transport"),
            ({"model": LLAMA, "endpoint_id": "1234567890"}, "model=None"),
            ({"model": None, "endpoint_id": "openapi"}, "openapi"),
            ({"model": None, "endpoint_id": "12/34"}, "endpoint ID"),
            ({"model": LLAMA, "deployed_model": "gemma-2-9b-it"}, "endpoint_id"),
            ({"model": None, "endpoint_id": "1234567890", "deployed_model": "gemma/2"}, "deployed_model"),
            ({"model": None, "endpoint_id": "1234567890", "deployed_model": ""}, "deployed_model"),
            ({"model": None, "endpoint_id": "1" * 200, "deployed_model": "m" * 256}, "512"),
        ],
    )
    def test_each_refusal_names_its_cause_and_holds_no_secret_in_its_traceback_locals(self, arguments, fragment):
        given = {"project": PROJECT, "location": "us-central1", "auth": GoogleCloudAuth(token=lambda: TOKEN)}
        given |= arguments
        if given["auth"] is KEY:
            pass
        elif "project" in arguments and arguments["project"] is None:
            given["auth"] = GoogleCloudAuth(api_key=KEY)

        with pytest.raises(RationalizationError) as caught:
            OpenAICompatibleProvider.google_cloud(given.pop("model"), **given)

        assert fragment in str(caught.value)
        assert_key_free(caught.value)
        assert_key_free(caught.value, key=TOKEN)


class TestRoles:
    def test_every_route_sends_a_system_turn_then_a_user_turn(self):
        messages = module._messages(make_request(), fold=False)

        assert messages == [{"role": "system", "content": SYSTEM}, {"role": "user", "content": ENVELOPE}]

    def test_the_private_fold_puts_the_system_prompt_first_in_the_one_user_turn(self):
        # Not exposed: kept so a fold can become an explicit opt-in if a live test shows a route refuses system.
        assert module._messages(make_request(), fold=True) == [{"role": "user", "content": f"{SYSTEM}\n\n{ENVELOPE}"}]

    @pytest.mark.parametrize("model", [LLAMA, "mistralai/codestral-2", None], ids=["openapi", "mistral", "endpoint"])
    def test_no_google_route_folds(self, model):
        server = Server()
        endpoint = {"endpoint_id": "1234567890"} if model is None else {}

        cloud(model, server, **endpoint).generate(make_request())

        roles = [message["role"] for message in json.loads(server.only.content)["messages"]]
        assert roles == ["system", "user"]
