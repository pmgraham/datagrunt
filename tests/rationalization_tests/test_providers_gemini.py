"""Tests for ``datagrunt.rationalization.providers.gemini``: Gemini on the Gemini API and on Google Cloud (T8).

Wire shapes pinned here were read on 2026-11-04 and 2026-11-05 from Google's official documentation:

- Gemini API ``models.generateContent``: https://ai.google.dev/api/generate-content (301 to
  https://aistudio.google.com/api-reference/generate-content): ``POST /v1beta/models/{model}:generateContent``,
  the ``FinishReason`` and ``BlockReason`` values, ``PromptFeedback.blockReason`` (set: no candidates),
  ``UsageMetadata`` (``promptTokenCount``, ``candidatesTokenCount``, ``thoughtsTokenCount``), ``Part.thought``.
- Gemini API structured output: https://ai.google.dev/gemini-api/docs/structured-output (301 to
  https://aistudio.google.com/docs/structured-output): host ``generativelanguage.googleapis.com``, the
  ``x-goog-api-key`` header, and the JSON Schema subset (types, ``title``/``description``, ``properties``/
  ``required``/``additionalProperties``, ``enum``, ``format`` date-time/date/time, ``minimum``/``maximum``,
  ``items``/``prefixItems``/``minItems``/``maxItems``, ``anyOf``, ``$ref``/``$defs``; ``minLength``, ``maxLength``,
  ``minProperties``, ``maxProperties`` and others stripped by the server).
- Google Cloud (Agent Platform, formerly Vertex AI): hosts, auth, URLs and identity come from
  :mod:`~datagrunt.rationalization.providers.google_cloud` (its research table cites the pages: ``global``,
  ``us``/``eu`` multi-region ``.rep.`` and regional hosts; API keys for ``generateContent``; express mode). Its
  structured output:
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/capabilities/control-generated-output:
  ``responseSchema`` supports ``anyOf``, ``enum`` (string enums only), ``format``, ``items``, ``maximum``,
  ``maxItems``, ``minimum``, ``minItems``, ``nullable``, ``properties``, ``description``, ``propertyOrdering``,
  ``required``; other fields are ignored.
- ``Schema``: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/Schema (``Type`` values
  ``STRING``, ``NUMBER``, ``INTEGER``, ``BOOLEAN``, ``ARRAY``, ``OBJECT``, ``NULL``).
- Finish reasons: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference (spelled
  ``FINISH_REASON_STOP``, ``FINISH_REASON_MAX_TOKENS``, ...) and
  https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/GenerateContentResponse
  (``MODEL_ARMOR``).

Every request goes to an ``httpx.MockTransport``; no test reaches a network host.
"""

import ast
import copy
import inspect
import json
import logging
import math
import pickle
import time
import traceback
from pathlib import Path

import httpx
import pytest

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization.providers import gemini as gemini_module
from datagrunt.rationalization.providers.base import (
    ModelProvider,
    ModelString,
    PermanentProviderError,
    TransientProviderError,
    _Secret,
    check_provider,
    family_problem,
    parse_model_string,
)
from datagrunt.rationalization.providers.gemini import (
    FROM_MODEL_STRING_OPTIONS,
    GEMINI_API_KEY_ENV,
    SCHEMA_MAX_DEPTH,
    SCHEMA_MAX_NODES,
    GeminiProvider,
)
from datagrunt.rationalization.providers.google_cloud import GoogleCloudAuth
from datagrunt.rationalization.providers.http import HttpClient, HttpReply
from datagrunt.rationalization.schema import response_schema
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse
from rationalization_tests.test_schema import REALISTIC

KEY = "AIza-test-KEY-hunter2"
TOKEN = "ya29.test-TOKEN-hunter2"
SECRET_VALUES = (KEY, TOKEN, "hunter2")
MODEL = "gemini-2.5-flash"
CLOUD_MODEL = "gemini-2.5-pro"
PROJECT = "my-project"
SYSTEM = "SYSTEM-PROMPT-TEXT: extract parts"
ENVELOPE = "ENVELOPE-TEXT: <document_payload_x>Bolt PN-104</document_payload_x>"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"
CLOUD_RESOURCE = f"projects/{PROJECT}/locations/us-central1/publishers/google/models/{CLOUD_MODEL}"
CLOUD_URL = f"https://us-central1-aiplatform.googleapis.com/v1/{CLOUD_RESOURCE}:generateContent"
EXPRESS_URL = f"https://aiplatform.googleapis.com/v1/publishers/google/models/{CLOUD_MODEL}:generateContent"
CLOUD_HOSTS = {
    "us-central1": "us-central1-aiplatform.googleapis.com",
    "europe-west4": "europe-west4-aiplatform.googleapis.com",
    "global": "aiplatform.googleapis.com",
    "us": "aiplatform.us.rep.googleapis.com",
    "eu": "aiplatform.eu.rep.googleapis.com",
}
ANSWER = '{"records": []}'

# A pinned 200 reply of models.generateContent (both APIs share the shape).
GEMINI_REPLY = {
    "candidates": [
        {
            "content": {"parts": [{"text": ANSWER}], "role": "model"},
            "finishReason": "STOP",
            "index": 0,
        }
    ],
    "usageMetadata": {"promptTokenCount": 120, "candidatesTokenCount": 7, "totalTokenCount": 127},
    "modelVersion": MODEL,
    "responseId": "resp-1",
}
# A pinned error body: the Google API error object.
ERROR_BODY = {"error": {"code": 400, "message": "Invalid JSON payload received.", "status": "INVALID_ARGUMENT"}}

# A small neutral wrapper (the shape schema.response_schema builds), with the keywords each dialect must handle.
SMALL = {
    "type": "object",
    "properties": {
        "records": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "record": {"$ref": "#/$defs/Part"},
                    "grounding_quotes": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["record", "grounding_quotes"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["records"],
    "additionalProperties": False,
    "$defs": {
        "Part": {
            "type": "object",
            "title": "Part",
            "properties": {
                "number": {"type": "string", "title": "Number", "pattern": "^P", "examples": ["P1"]},
                "qty": {
                    "anyOf": [{"type": "integer", "minimum": 1}, {"type": "null"}],
                    "default": None,
                    "description": "how many",
                },
                "kind": {"const": "bolt"},
                "weight": {"anyOf": [{"type": "number"}, {"type": "string"}]},
            },
            "required": ["number", "kind"],
        }
    },
}
SMALL_FOR_GEMINI = {
    "type": "object",
    "properties": {
        "records": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "record": {"$ref": "#/$defs/Part"},
                    "grounding_quotes": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["record", "grounding_quotes"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["records"],
    "additionalProperties": False,
    "$defs": {
        "Part": {
            "type": "object",
            "title": "Part",
            "properties": {
                "number": {"type": "string", "title": "Number", "description": '{pattern: "^P"}'},
                "qty": {"anyOf": [{"type": "integer", "minimum": 1}, {"type": "null"}], "description": "how many"},
                "kind": {"enum": ["bolt"]},
                "weight": {"anyOf": [{"type": "number"}, {"type": "string"}]},
            },
            "required": ["number", "kind"],
        }
    },
}
SMALL_FOR_CLOUD = {
    "type": "OBJECT",
    "properties": {
        "records": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "record": {
                        "type": "OBJECT",
                        "properties": {
                            "number": {"type": "STRING", "description": '{pattern: "^P"}'},
                            "qty": {"type": "INTEGER", "minimum": 1, "nullable": True, "description": "how many"},
                            "kind": {"type": "STRING", "enum": ["bolt"]},
                            "weight": {"anyOf": [{"type": "NUMBER"}, {"type": "STRING"}]},
                        },
                        "required": ["number", "kind"],
                    },
                    "grounding_quotes": {"type": "ARRAY", "items": {"type": "STRING"}},
                },
                "required": ["record", "grounding_quotes"],
                "description": "{additionalProperties: false}",
            },
        }
    },
    "required": ["records"],
    "description": "{additionalProperties: false}",
}

GEMINI_KEYWORDS = frozenset(
    {
        "type",
        "title",
        "description",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "enum",
        "format",
        "minimum",
        "maximum",
        "minItems",
        "maxItems",
        "anyOf",
        "$ref",
        "$defs",
    }
)
CLOUD_KEYWORDS = frozenset(
    {
        "type",
        "description",
        "nullable",
        "properties",
        "required",
        "items",
        "enum",
        "format",
        "minimum",
        "maximum",
        "minItems",
        "maxItems",
        "anyOf",
    }
)
JSON_TYPES = frozenset({"string", "number", "integer", "boolean", "object", "array", "null"})
OPENAPI_TYPES = frozenset({"STRING", "NUMBER", "INTEGER", "BOOLEAN", "OBJECT", "ARRAY", "NULL"})
DATETIME_STYLE = {"lab report": "2026-11-04T10:00:00"}  # its datetimes are NaiveDatetime; the rest are aware
STRING_SAMPLES = {"postal_code": "AB1 2CD"}  # Invoice's Address.postal_code pattern, dropped on the wire


@pytest.fixture(autouse=True)
def no_key_in_environment(monkeypatch):
    """No GEMINI_API_KEY (or proxy setting) from the developer's shell reaches a test."""
    for name in (GEMINI_API_KEY_ENV, "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------------------------


def streamed(status, *, json_body=None, content=b"", headers=None):
    """A streaming ``httpx.Response``, as from the network (http.py refuses a pre-read ``content=`` response)."""
    headers = dict(headers or {})
    if json_body is not None:
        content = json.dumps(json_body).encode("utf-8")
        headers.setdefault("content-type", "application/json")
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(content))


class Recorder:
    """A MockTransport handler answering with one fixed reply, recording each request."""

    def __init__(self, status=200, *, json_body=GEMINI_REPLY, content=b"", headers=None):
        self.status, self.json_body, self.content, self.headers = status, json_body, content, headers
        self.seen = []

    def __call__(self, request):
        self.seen.append(request)
        return streamed(self.status, json_body=self.json_body, content=self.content, headers=self.headers)


class TokenSource:
    """A token callable that counts its calls; its repr would show a token if anything printed it."""

    def __init__(self, *values):
        self.values = list(values) or [TOKEN]
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.values[min(self.calls, len(self.values)) - 1]

    def __repr__(self):
        return f"TokenSource({TOKEN})"


def gemini(handler=None, **options):
    handler = Recorder() if handler is None else handler
    options.setdefault("api_key", KEY)
    return GeminiProvider(MODEL, transport=httpx.MockTransport(handler), **options)


def cloud(handler=None, *, location="us-central1", project=PROJECT, token=None, auth=None, **options):
    """Gemini on Google Cloud, by default with a token source; ``auth`` overrides it."""
    handler = Recorder() if handler is None else handler
    auth = GoogleCloudAuth(token=TokenSource() if token is None else token) if auth is None else auth
    return GeminiProvider.google_cloud(
        CLOUD_MODEL, project=project, location=location, auth=auth, transport=httpx.MockTransport(handler), **options
    )


def express(handler=None, *, auth=None, **options):
    """Gemini on Google Cloud in express mode, asked for explicitly: an API key, no project, the global host."""
    handler = Recorder() if handler is None else handler
    return GeminiProvider.google_cloud_express(
        CLOUD_MODEL,
        auth=GoogleCloudAuth(api_key=KEY) if auth is None else auth,
        transport=httpx.MockTransport(handler),
        **options,
    )


def full_traceback_text(error):
    """Every frame of the error's traceback with its locals, the formatted exception included."""
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in summary.stack) + "".join(summary.format())


def request_for(provider, schema=SMALL, *, timeout_s=30.0, max_output_tokens=1024):
    return ModelRequest(
        unit_id="doc:p0001_e001-p0001_e003",
        model_id=provider.model_id,
        system_prompt=SYSTEM,
        user_envelope=ENVELOPE,
        json_schema=provider.transform_schema(schema),
        max_output_tokens=max_output_tokens,
        timeout_s=timeout_s,
    )


@pytest.fixture(params=["gemini-api", "google-cloud"])
def answer(request):
    """Generate once over a reply ``body`` (JSON) or raw ``content`` bytes, through each API in turn: the reply
    mapping is shared."""
    make = gemini if request.param == "gemini-api" else cloud

    def run(body=None, *, content=None):
        handler = Recorder(json_body=body) if content is None else Recorder(json_body=None, content=content)
        provider = make(handler)
        return provider.generate(request_for(provider))

    return run


def reply_with_candidate(candidate, **extra):
    return {"candidates": [candidate], **extra}


def text_candidate(*texts, finish="STOP"):
    return {"content": {"parts": [{"text": text} for text in texts], "role": "model"}, "finishReason": finish}


def sent_json(request):
    return json.loads(request.content.decode("utf-8"))


def traceback_text(error):
    """The error's traceback with the locals of datagrunt's frames, as a log with ``capture_locals`` prints them."""
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def assert_secret_free(text):
    for secret in SECRET_VALUES:
        assert secret not in text


def nodes(schema):
    """Every schema object in ``schema``, iteratively (the root, properties, items, anyOf members, $defs)."""
    found, stack = [], [schema]
    while stack:
        node = stack.pop()
        assert isinstance(node, dict)
        found.append(node)
        stack.extend(node.get("properties", {}).values())
        stack.extend(node.get("$defs", {}).values())
        stack.extend(node.get("anyOf", []))
        if isinstance(node.get("items"), dict):
            stack.append(node["items"])
    return found


def resolve(node, defs):
    for _ in range(64):
        if "$ref" not in node:
            return node
        node = defs[node["$ref"].removeprefix("#/$defs/")]
    raise AssertionError("a $ref chain that does not end")


def is_null(member):
    return member.get("type") in ("null", "NULL")


def instance(node, defs, *, nulls, datetime_value, sample="AB"):
    """A value shaped like ``node`` (either dialect): every property present, null where null passes and ``nulls``.

    A string is ``sample``: :data:`STRING_SAMPLES` by property name, for a constraint a transform drops (a pattern)
    that the local ``model_validate`` still enforces.
    """
    node = resolve(node, defs)
    again = {"nulls": nulls, "datetime_value": datetime_value}
    if node.get("nullable") and nulls:
        return None
    if "anyOf" in node:
        members = [member for member in node["anyOf"] if not is_null(member)]
        if nulls and len(members) < len(node["anyOf"]):
            return None
        return instance(members[0], defs, sample=sample, **again)
    if "enum" in node:
        return node["enum"][0]
    kind = node["type"].lower()
    if kind == "object":
        return {
            name: instance(child, defs, sample=STRING_SAMPLES.get(name, "AB"), **again)
            for name, child in node["properties"].items()
        }
    if kind == "array":
        return [instance(node["items"], defs, sample=sample, **again)] * max(1, node.get("minItems", 1))
    formats = {"date": "2026-11-04", "date-time": datetime_value, "time": "10:00:00"}
    form = "date-time" if 'format: "date-time"' in node.get("description", "") else node.get("format")
    scalars = {"string": formats.get(form, sample), "integer": 1, "number": 1.5, "boolean": True}
    return scalars[kind]


def conforms(value, node, defs):
    """Whether ``value`` matches ``node`` (either dialect) in type, enum, nullability, required and closed objects."""
    node = resolve(node, defs)
    if value is None:
        return bool(node.get("nullable")) or any(is_null(m) for m in node.get("anyOf", [])) or is_null(node)
    if "anyOf" in node:
        return any(conforms(value, member, defs) for member in node["anyOf"] if not is_null(member))
    if "enum" in node and value not in node["enum"]:
        return False
    kind = node.get("type", "string").lower()
    checks = {
        "string": lambda: isinstance(value, str),
        "integer": lambda: type(value) is int,
        "number": lambda: type(value) in (int, float),
        "boolean": lambda: type(value) is bool,
        "array": lambda: isinstance(value, list) and all(conforms(item, node["items"], defs) for item in value),
        "object": lambda: isinstance(value, dict) and object_conforms(value, node, defs),
    }
    return checks[kind]()


def object_conforms(value, node, defs):
    properties = node["properties"]
    if any(name not in value for name in node.get("required", [])):
        return False
    if node.get("additionalProperties") is False and any(name not in properties for name in value):
        return False
    return all(conforms(item, properties[name], defs) for name, item in value.items() if name in properties)


def wrapped(record):
    return {"records": [{"record": record, "grounding_quotes": ["Bolt"], "source_element_ids": ["p0001_e001"]}]}


# ---------------------------------------------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------------------------------------------


class TestGeminiApiRequest:
    def test_the_request_is_the_pinned_golden(self):
        handler = Recorder()
        provider = gemini(handler)

        provider.generate(request_for(provider))

        (sent,) = handler.seen
        assert sent.method == "POST"
        assert str(sent.url) == GEMINI_URL
        assert sent.headers["x-goog-api-key"] == KEY
        assert "authorization" not in sent.headers
        assert sent_json(sent) == {
            "systemInstruction": {"parts": [{"text": SYSTEM}]},
            "contents": [{"role": "user", "parts": [{"text": ENVELOPE}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseJsonSchema": SMALL_FOR_GEMINI,
                "maxOutputTokens": 1024,
                "seed": 0,
            },
        }

    def test_the_key_goes_only_in_the_header_never_in_the_url(self):
        handler = Recorder()
        provider = gemini(handler)

        provider.generate(request_for(provider))

        (sent,) = handler.seen
        assert sent.url.query == b""
        assert KEY not in str(sent.url)
        assert "key=" not in str(sent.url)
        assert KEY not in sent.content.decode("utf-8")

    def test_none_temperature_and_seed_are_omitted(self):
        handler = Recorder()
        provider = gemini(handler, temperature=None, seed=None)

        provider.generate(request_for(provider))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert "temperature" not in config
        assert "seed" not in config

    def test_given_temperature_and_seed_are_sent(self):
        handler = Recorder()
        provider = gemini(handler, temperature=0.7, seed=42)

        provider.generate(request_for(provider, max_output_tokens=77))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert (config["temperature"], config["seed"], config["maxOutputTokens"]) == (0.7, 42, 77)

    def test_the_request_schema_is_sent_as_given(self):
        handler = Recorder()
        provider = gemini(handler)
        request = request_for(provider)

        provider.generate(request)

        assert sent_json(handler.seen[0])["generationConfig"]["responseJsonSchema"] == dict(request.json_schema)

    def test_a_key_from_the_environment_is_sent(self, monkeypatch):
        monkeypatch.setenv(GEMINI_API_KEY_ENV, KEY)
        handler = Recorder()
        provider = GeminiProvider(MODEL, transport=httpx.MockTransport(handler))

        provider.generate(request_for(provider))

        assert handler.seen[0].headers["x-goog-api-key"] == KEY

    def test_a_given_key_wins_over_the_environment(self, monkeypatch):
        monkeypatch.setenv(GEMINI_API_KEY_ENV, "AIza-from-the-environment")
        handler = Recorder()
        provider = gemini(handler)

        provider.generate(request_for(provider))

        assert handler.seen[0].headers["x-goog-api-key"] == KEY

    def test_a_secret_key_is_accepted(self):
        handler = Recorder()
        provider = gemini(handler, api_key=_Secret(KEY))

        provider.generate(request_for(provider))

        assert handler.seen[0].headers["x-goog-api-key"] == KEY


class TestGoogleCloudRequest:
    def test_the_regional_token_request_is_the_pinned_golden(self):
        handler = Recorder()
        provider = cloud(handler)

        provider.generate(request_for(provider))

        (sent,) = handler.seen
        assert sent.method == "POST"
        assert str(sent.url) == CLOUD_URL
        assert sent.headers["authorization"] == f"Bearer {TOKEN}"
        assert "x-goog-api-key" not in sent.headers
        assert sent.url.query == b""
        assert sent_json(sent) == {
            "systemInstruction": {"parts": [{"text": SYSTEM}]},
            "contents": [{"role": "user", "parts": [{"text": ENVELOPE}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": SMALL_FOR_CLOUD,
                "maxOutputTokens": 1024,
                "seed": 0,
            },
        }

    def test_express_mode_is_the_pinned_golden(self):
        handler = Recorder()
        provider = express(handler)

        provider.generate(request_for(provider))

        (sent,) = handler.seen
        assert str(sent.url) == EXPRESS_URL
        assert sent.headers["x-goog-api-key"] == KEY
        assert "authorization" not in sent.headers
        assert sent.url.query == b""
        assert KEY not in str(sent.url)
        assert KEY not in sent.content.decode("utf-8")
        assert sent_json(sent)["generationConfig"]["responseSchema"] == SMALL_FOR_CLOUD

    @pytest.mark.parametrize(("location", "host"), list(CLOUD_HOSTS.items()))
    def test_the_request_goes_to_its_locations_host(self, location, host):
        handler = Recorder()
        provider = cloud(handler, location=location)

        provider.generate(request_for(provider))

        assert str(handler.seen[0].url) == (
            f"https://{host}/v1/projects/{PROJECT}/locations/{location}/publishers/google/models/{CLOUD_MODEL}"
            ":generateContent"
        )

    def test_an_api_key_with_a_project_goes_only_in_its_header(self):
        handler = Recorder()
        provider = cloud(handler, auth=GoogleCloudAuth(api_key=KEY))

        provider.generate(request_for(provider))

        (sent,) = handler.seen
        assert str(sent.url) == CLOUD_URL
        assert sent.headers["x-goog-api-key"] == KEY
        assert "authorization" not in sent.headers
        assert sent.url.query == b""
        assert KEY not in str(sent.url)

    def test_a_domain_scoped_project_goes_in_the_path(self):
        handler = Recorder()
        provider = cloud(handler, project="example.com:my-project", location="europe-west4")

        provider.generate(request_for(provider))

        assert handler.seen[0].url.host == "europe-west4-aiplatform.googleapis.com"
        assert handler.seen[0].url.path.startswith("/v1/projects/example.com:my-project/locations/europe-west4/")

    def test_the_token_is_called_once_per_request_never_at_construction(self):
        handler = Recorder()
        source = TokenSource("ya29.first-hunter2", "ya29.second-hunter2")
        provider = cloud(handler, token=source)

        assert source.calls == 0
        provider.generate(request_for(provider))
        provider.generate(request_for(provider))

        assert source.calls == 2
        assert [sent.headers["authorization"] for sent in handler.seen] == [
            "Bearer ya29.first-hunter2",
            "Bearer ya29.second-hunter2",
        ]

    @pytest.mark.parametrize("make", [cloud, express])
    def test_temperature_none_and_seed_none_are_omitted(self, make):
        handler = Recorder()
        provider = make(handler, temperature=None, seed=None)

        provider.generate(request_for(provider))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert "temperature" not in config
        assert "seed" not in config

    def test_a_versioned_model_reaches_its_resource(self):
        handler = Recorder()
        provider = GeminiProvider.google_cloud(
            "gemini-2.0-flash@001",
            project=PROJECT,
            location="us-central1",
            auth=GoogleCloudAuth(token=TokenSource()),
            transport=httpx.MockTransport(handler),
        )

        provider.generate(request_for(provider))

        resource = f"projects/{PROJECT}/locations/us-central1/publishers/google/models/gemini-2.0-flash@001"
        assert (
            str(handler.seen[0].url) == f"https://us-central1-aiplatform.googleapis.com/v1/{resource}:generateContent"
        )
        assert provider.model_id == f"google-cloud:{resource}"

    @pytest.mark.parametrize("model", ["a@b@c", "gemini-2.0-flash@", "@001", "a@.", "a@..", "a@b/c", "a@b c"])
    def test_a_malformed_version_is_refused(self, model):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider.google_cloud(model, project=PROJECT, location="us", auth=GoogleCloudAuth(api_key=KEY))

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_the_gemini_api_refuses_a_versioned_model_naming_google_cloud(self):
        with pytest.raises(RationalizationError, match="google_cloud") as raised:
            GeminiProvider("gemini-2.0-flash@001", api_key=KEY)

        assert_secret_free(traceback_text(raised.value))

    def test_the_old_vertex_factory_is_gone(self):
        assert not hasattr(GeminiProvider, "vertex")
        assert not hasattr(gemini_module, "VERTEX_GLOBAL_HOST")


class TestGoogleCloudAuth:
    @pytest.mark.parametrize("error", [RuntimeError(f"refresh failed for {TOKEN}"), ValueError(TOKEN), OSError()])
    def test_a_raising_token_source_is_permanent_without_its_message(self, error):
        def token():
            raise error

        handler = Recorder()
        provider = cloud(handler, token=token)

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert raised.value.status_code is None
        assert raised.value.detail == f"the Google Cloud token provider failed ({type(error).__name__})"
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert_secret_free(traceback_text(raised.value))
        assert handler.seen == []

    @pytest.mark.parametrize(("value", "named"), [(None, "NoneType"), (5, "int"), (TOKEN.encode(), "bytes")])
    def test_a_token_that_is_not_a_str_is_permanent(self, value, named):
        handler = Recorder()
        provider = cloud(handler, token=lambda: value)

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert raised.value.detail.startswith("the Google Cloud token provider failed")
        assert named in raised.value.detail
        assert handler.seen == []

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "   ",
            f"{TOKEN}\n",
            f" {TOKEN}",
            "ya29.tök-hunter2",
            "ya29\x00hunter2",
            "ya29 inner-hunter2",
            "ya29\tinner-hunter2",
            f"Bearer {TOKEN}",
            f"bearer {TOKEN}",
            "hunter2" * 3000,
        ],
    )
    def test_an_invalid_token_is_permanent_and_never_shown(self, value):
        handler = Recorder()
        provider = cloud(handler, token=lambda: value)

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert raised.value.detail.startswith("the Google Cloud token provider failed")
        assert_secret_free(str(raised.value) + repr(raised.value) + traceback_text(raised.value))
        assert handler.seen == []

    def test_a_str_subclass_token_is_refused_by_its_class(self):
        class SneakyToken(str):
            pass

        handler = Recorder()
        provider = cloud(handler, token=lambda: SneakyToken(TOKEN))

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert "SneakyToken" in raised.value.detail
        assert_secret_free(raised.value.detail + traceback_text(raised.value))
        assert handler.seen == []

    def test_a_token_object_that_prints_its_value_is_never_echoed(self):
        class Printed:
            def __str__(self):
                return TOKEN

            __repr__ = __str__

        provider = cloud(token=Printed)

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert "Printed" in raised.value.detail
        assert_secret_free(str(raised.value) + repr(raised.value) + traceback_text(raised.value))

    def test_a_keyboard_interrupt_in_the_token_source_propagates(self):
        def token():
            raise KeyboardInterrupt

        provider = cloud(token=token)

        with pytest.raises(KeyboardInterrupt):
            provider.generate(request_for(provider))

    @pytest.mark.parametrize("auth", [KEY, _Secret(KEY), TokenSource(), lambda: TOKEN, None, {"api_key": KEY}])
    def test_auth_that_is_not_a_google_cloud_auth_is_refused_without_its_value(self, auth):
        with pytest.raises(RationalizationError, match="GoogleCloudAuth") as raised:
            GeminiProvider.google_cloud(CLOUD_MODEL, project=PROJECT, location="global", auth=auth)

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_bare_auth_is_refused_first_naming_the_remedy(self):
        with pytest.raises(
            RationalizationError, match=r"auth must be a GoogleCloudAuth \(GoogleCloudAuth\(api_key="
        ) as raised:
            GeminiProvider.google_cloud("a/b", project="a/b", location="-x", auth=KEY)

        assert_secret_free(traceback_text(raised.value))

    def test_express_bare_auth_is_refused_first_naming_the_remedy(self):
        with pytest.raises(
            RationalizationError, match=r"auth must be a GoogleCloudAuth \(GoogleCloudAuth\(api_key="
        ) as raised:
            GeminiProvider.google_cloud_express("a/b", auth=KEY)

        assert_secret_free(traceback_text(raised.value))

    def test_express_mode_with_a_token_is_refused_at_construction(self):
        source = TokenSource()

        with pytest.raises(RationalizationError, match="express mode takes an API key") as raised:
            GeminiProvider.google_cloud_express(CLOUD_MODEL, auth=GoogleCloudAuth(token=source))

        assert source.calls == 0
        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize("auth", [KEY, _Secret(KEY), TokenSource(), None])
    def test_express_mode_with_bare_auth_is_refused_without_its_value(self, auth):
        with pytest.raises(RationalizationError, match="GoogleCloudAuth") as raised:
            GeminiProvider.google_cloud_express(CLOUD_MODEL, auth=auth)

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    @pytest.mark.parametrize("location", ["global", "us-central1", "us", "eu"])
    def test_a_missing_project_is_refused_naming_express_mode(self, location):
        with pytest.raises(RationalizationError, match="google_cloud_express") as raised:
            GeminiProvider.google_cloud(CLOUD_MODEL, project=None, location=location, auth=GoogleCloudAuth(api_key=KEY))

        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize(
        "given",
        [{"location": "global"}, {"project": PROJECT}, {}],
        ids=["no project", "no location", "neither"],
    )
    def test_project_and_location_are_required_keywords(self, given):
        with pytest.raises(TypeError, match="required keyword-only argument") as raised:
            GeminiProvider.google_cloud(CLOUD_MODEL, auth=GoogleCloudAuth(api_key=KEY), **given)

        assert_secret_free(str(raised.value) + full_traceback_text(raised.value))

    def test_the_factories_signatures_make_express_an_explicit_choice(self):
        regular = inspect.signature(GeminiProvider.google_cloud).parameters
        explicit = inspect.signature(GeminiProvider.google_cloud_express).parameters

        for name in ("project", "location", "auth"):
            assert regular[name].kind is inspect.Parameter.KEYWORD_ONLY
            assert regular[name].default is inspect.Parameter.empty
        assert list(explicit) == [
            "model",
            "auth",
            "temperature",
            "seed",
            "thinking_level",
            "max_connections",
            "transport",
        ]
        assert explicit["auth"].default is inspect.Parameter.empty
        assert (
            explicit["temperature"].default,
            explicit["seed"].default,
            explicit["thinking_level"].default,
        ) == (None, 0, None)

    def test_the_docstrings_say_what_express_mode_and_global_give_up(self):
        express_doc = " ".join(GeminiProvider.google_cloud_express.__doc__.split())
        regular_doc = " ".join(GeminiProvider.google_cloud.__doc__.split())

        assert "Preview" in express_doc
        assert "Pre-GA" in express_doc
        assert "@gmail.com" in express_doc
        assert "no data-residency guarantee" in express_doc
        assert "no data-residency guarantee" in regular_doc

    @pytest.mark.parametrize("make", [cloud, express])
    def test_a_refused_construction_holds_no_secret_in_its_traceback(self, make):
        with pytest.raises(RationalizationError) as raised:
            make(temperature=9)

        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize("factory", ["google_cloud", "google_cloud_express"])
    def test_auth_is_required(self, factory):
        place = {"project": PROJECT, "location": "global"} if factory == "google_cloud" else {}
        with pytest.raises(TypeError, match="auth") as raised:
            getattr(GeminiProvider, factory)(CLOUD_MODEL, **place)

        assert_secret_free(full_traceback_text(raised.value))

    def test_express_takes_no_project_or_location(self):
        with pytest.raises(TypeError, match="location"):
            GeminiProvider.google_cloud_express(CLOUD_MODEL, location="us", auth=GoogleCloudAuth(api_key=KEY))


# ---------------------------------------------------------------------------------------------------------------
# Replies
# ---------------------------------------------------------------------------------------------------------------


class TestFinishMapping:
    @pytest.mark.parametrize(
        ("reason", "finish"),
        [
            ("STOP", "complete"),
            ("MAX_TOKENS", "truncated"),
            ("CONTINUATION", "truncated"),
            ("SAFETY", "refused"),
            ("RECITATION", "refused"),
            ("LANGUAGE", "refused"),
            ("BLOCKLIST", "refused"),
            ("PROHIBITED_CONTENT", "refused"),
            ("SPII", "refused"),
            ("IMAGE_SAFETY", "refused"),
            ("IMAGE_PROHIBITED_CONTENT", "refused"),
            ("IMAGE_RECITATION", "refused"),
            ("ESCALATION", "refused"),
            ("MODEL_ARMOR", "refused"),
            ("FINISH_REASON_MODEL_ARMOR", "refused"),
            ("OTHER", "complete"),
            ("MALFORMED_FUNCTION_CALL", "complete"),
            ("MALFORMED_RESPONSE", "complete"),
            ("FINISH_REASON_UNSPECIFIED", "complete"),
            ("UNEXPECTED_TOOL_CALL", "complete"),
            ("A_REASON_FROM_THE_FUTURE", "complete"),
            ("FINISH_REASON_STOP", "complete"),
            ("FINISH_REASON_MAX_TOKENS", "truncated"),
            ("FINISH_REASON_CONTINUATION", "truncated"),
            ("FINISH_REASON_SAFETY", "refused"),
            ("FINISH_REASON_SPII", "refused"),
            ("max_tokens", "complete"),
            (5, "complete"),
            (None, "complete"),
        ],
    )
    def test_each_finish_reason_maps(self, answer, reason, finish):
        result = answer(reply_with_candidate(text_candidate(ANSWER, finish=reason)))

        assert result.finish == finish

    @pytest.mark.parametrize("make", [gemini, cloud, express])
    @pytest.mark.parametrize("reason", ["PUP_LIMITED_DISABLED", "FINISH_REASON_PUP_LIMITED_DISABLED"])
    def test_a_limited_or_disabled_account_stops_the_run(self, reason, make):
        handler = Recorder(json_body=reply_with_candidate(text_candidate(ANSWER, finish=reason)))
        provider = make(handler)

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert raised.value.status_code is None
        assert raised.value.detail == "the Gemini account is limited or disabled (PUP_LIMITED_DISABLED)"
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert_secret_free(traceback_text(raised.value))

    def test_a_missing_finish_reason_is_complete(self, answer):
        result = answer(reply_with_candidate({"content": {"parts": [{"text": ANSWER}]}}))

        assert (result.finish, result.content) == ("complete", {"records": []})

    def test_a_complete_answer_is_parsed(self, answer):
        result = answer(GEMINI_REPLY)

        assert isinstance(result, ProviderResponse)
        assert result == ProviderResponse(
            content={"records": []}, raw_text=ANSWER, finish="complete", input_tokens=120, output_tokens=7
        )

    def test_a_truncated_answer_keeps_its_text_and_parses_what_parses(self, answer):
        cut = answer(reply_with_candidate(text_candidate('{"records": [{"rec', finish="MAX_TOKENS")))
        whole = answer(reply_with_candidate(text_candidate(ANSWER, finish="MAX_TOKENS")))

        assert (cut.finish, cut.content, cut.raw_text) == ("truncated", None, '{"records": [{"rec')
        assert (whole.finish, whole.content) == ("truncated", {"records": []})

    @pytest.mark.parametrize("reason", ["SAFETY", "RECITATION", "PROHIBITED_CONTENT"])
    def test_a_refused_answer_has_no_content_even_when_its_text_parses(self, answer, reason):
        result = answer(reply_with_candidate(text_candidate(ANSWER, finish=reason)))

        assert (result.finish, result.content, result.raw_text) == ("refused", None, ANSWER)

    def test_a_refused_candidate_without_content_keeps_the_reply_text(self, answer):
        body = reply_with_candidate({"finishReason": "SAFETY", "safetyRatings": []})

        result = answer(body)

        assert (result.finish, result.content) == ("refused", None)
        assert json.loads(result.raw_text) == body


class TestBlockedPrompt:
    @pytest.mark.parametrize(
        "reason", ["SAFETY", "OTHER", "BLOCKLIST", "PROHIBITED_CONTENT", "IMAGE_SAFETY", "MODEL_ARMOR", ""]
    )
    def test_a_blocked_prompt_is_refused_with_no_content(self, answer, reason):
        body = {
            "promptFeedback": {"blockReason": reason, "safetyRatings": []},
            "usageMetadata": {"promptTokenCount": 9},
        }

        result = answer(body)

        assert (result.finish, result.content, result.input_tokens) == ("refused", None, 9)
        assert json.loads(result.raw_text) == body

    def test_a_block_reason_wins_over_a_parsable_candidate(self, answer):
        body = {**GEMINI_REPLY, "promptFeedback": {"blockReason": "SAFETY"}}

        result = answer(body)

        assert (result.finish, result.content) == ("refused", None)

    def test_prompt_feedback_without_a_block_reason_changes_nothing(self, answer):
        result = answer({**GEMINI_REPLY, "promptFeedback": {"safetyRatings": [{"category": "HARM_CATEGORY_X"}]}})

        assert (result.finish, result.content) == ("complete", {"records": []})

    @pytest.mark.parametrize("feedback", ["SAFETY", ["SAFETY"], {"blockReason": None}])
    def test_prompt_feedback_that_blocks_nothing_is_ignored(self, answer, feedback):
        result = answer({**GEMINI_REPLY, "promptFeedback": feedback})

        assert (result.finish, result.content) == ("complete", {"records": []})


class TestText:
    def test_multi_part_text_is_joined(self, answer):
        result = answer(reply_with_candidate(text_candidate('{"records": ', "[]", "}")))

        assert (result.content, result.raw_text) == ({"records": []}, ANSWER)

    def test_thought_parts_are_left_out(self, answer):
        candidate = {
            "content": {"parts": [{"text": "Let me think: PN-104", "thought": True}, {"text": ANSWER}]},
            "finishReason": "STOP",
        }

        result = answer(reply_with_candidate(candidate))

        assert (result.content, result.raw_text) == ({"records": []}, ANSWER)

    def test_parts_without_text_are_skipped(self, answer):
        candidate = {"content": {"parts": [{"thoughtSignature": "c2ln"}, {"text": ANSWER}]}, "finishReason": "STOP"}

        result = answer(reply_with_candidate(candidate))

        assert result.content == {"records": []}

    def test_only_the_first_candidate_is_read(self, answer):
        body = {"candidates": [text_candidate(ANSWER), text_candidate('{"records": [{"x": 1}]}')]}

        result = answer(body)

        assert result.content == {"records": []}

    @pytest.mark.parametrize("text", ["not json", '```json\n{"records": []}\n```', '{"records": []} trailing', "[]"])
    def test_text_that_is_not_one_json_object_has_no_content(self, answer, text):
        result = answer(reply_with_candidate(text_candidate(text)))

        assert (result.finish, result.content, result.raw_text) == ("complete", None, text)

    def test_a_lone_surrogate_in_the_answer_is_escaped(self, answer):
        result = answer(reply_with_candidate(text_candidate('{"records": [], "note": "\\ud800"}')))

        assert result.content == {"note": "[U+D800]", "records": []}


class TestUsage:
    def test_prompt_and_candidate_tokens_are_read(self, answer):
        result = answer(GEMINI_REPLY)

        assert (result.input_tokens, result.output_tokens) == (120, 7)

    def test_thinking_tokens_count_as_output(self, answer):
        usage = {"promptTokenCount": 10, "candidatesTokenCount": 7, "thoughtsTokenCount": 30}

        result = answer({**GEMINI_REPLY, "usageMetadata": usage})

        assert (result.input_tokens, result.output_tokens) == (10, 37)

    def test_thinking_tokens_alone_count(self, answer):
        result = answer({**GEMINI_REPLY, "usageMetadata": {"thoughtsTokenCount": 30}})

        assert (result.input_tokens, result.output_tokens) == (None, 30)

    @pytest.mark.parametrize("junk", ["12", True, -1, 1.5, None, [3], 2**63])
    def test_junk_counts_are_ignored(self, answer, junk):
        usage = {"promptTokenCount": junk, "candidatesTokenCount": junk, "thoughtsTokenCount": junk}

        result = answer({**GEMINI_REPLY, "usageMetadata": usage})

        assert (result.input_tokens, result.output_tokens) == (None, None)
        assert result.content == {"records": []}

    def test_an_out_of_range_candidate_count_leaves_the_thinking_count(self, answer):
        usage = {"promptTokenCount": 3, "candidatesTokenCount": 2**63, "thoughtsTokenCount": 5}

        result = answer({**GEMINI_REPLY, "usageMetadata": usage})

        assert (result.input_tokens, result.output_tokens) == (3, 5)

    def test_the_largest_counts_are_kept(self, answer):
        usage = {"promptTokenCount": 2**63 - 1, "candidatesTokenCount": 2**63 - 1}

        result = answer({**GEMINI_REPLY, "usageMetadata": usage})

        assert (result.input_tokens, result.output_tokens) == (2**63 - 1, 2**63 - 1)

    @pytest.mark.parametrize("junk", ["12", True, -1, 1.5, 2**63])
    def test_junk_thinking_tokens_leave_the_candidate_count(self, answer, junk):
        usage = {"promptTokenCount": 3, "candidatesTokenCount": 7, "thoughtsTokenCount": junk}

        result = answer({**GEMINI_REPLY, "usageMetadata": usage})

        assert (result.input_tokens, result.output_tokens) == (3, 7)

    @pytest.mark.parametrize("usage", ["x", [1, 2], 5, None])
    def test_usage_that_is_not_an_object_is_ignored(self, answer, usage):
        result = answer({**GEMINI_REPLY, "usageMetadata": usage})

        assert (result.input_tokens, result.output_tokens) == (None, None)

    def test_missing_usage_is_none(self, answer):
        result = answer({"candidates": GEMINI_REPLY["candidates"]})

        assert (result.input_tokens, result.output_tokens) == (None, None)


class TestUnexpectedShapes:
    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"candidates": []},
            {"candidates": "x"},
            {"candidates": [5]},
            {"candidates": [{"finishReason": "STOP"}]},
            {"candidates": [{"content": "x", "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": "x"}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"role": "model"}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": []}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": [5]}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": [5, {"text": ANSWER}]}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": [{"text": ANSWER}, "x"]}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": [{"text": 5}]}, "finishReason": "STOP"}]},
            {"candidates": [{"content": {"parts": [{"text": ANSWER}, {"text": None}]}}]},
            {"candidates": [{"content": {"parts": [{"thought": True, "text": ANSWER}]}}]},
        ],
    )
    def test_an_unexpected_json_shape_has_no_content_and_keeps_the_reply(self, answer, body):
        result = answer(body)

        assert (result.finish, result.content) == ("complete", None)
        assert json.loads(result.raw_text) == body

    @pytest.mark.parametrize("content", [b"not json", b"[1, 2]", b'"text"', b"", b'{"a": NaN}', b"\xff\xfe"])
    def test_a_body_that_is_not_a_json_object_has_no_content(self, answer, content):
        result = answer(content=content)

        assert (result.finish, result.content) == ("complete", None)
        assert result.raw_text == content.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------------------------------------------
# HTTP failures and the deadline
# ---------------------------------------------------------------------------------------------------------------


class TestHttpFailures:
    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_a_client_error_is_permanent(self, status, make):
        provider = make(Recorder(status, json_body=ERROR_BODY))

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert raised.value.status_code == status
        assert raised.value.detail == "Invalid JSON payload received."

    @pytest.mark.parametrize("status", [429, 500, 503])
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_a_retryable_status_is_transient(self, status, make):
        provider = make(Recorder(status, json_body=ERROR_BODY))

        with pytest.raises(TransientProviderError) as raised:
            provider.generate(request_for(provider))

        assert (raised.value.status_code, raised.value.retry_after_s) == (status, None)

    def test_a_429_carries_its_retry_after(self):
        provider = gemini(Recorder(429, json_body=ERROR_BODY, headers={"retry-after": "7"}))

        with pytest.raises(TransientProviderError) as raised:
            provider.generate(request_for(provider))

        assert (raised.value.status_code, raised.value.retry_after_s) == (429, 7.0)

    def test_a_redirect_is_not_followed(self):
        handler = Recorder(302, json_body=None, headers={"location": "https://evil.example/x"})
        provider = gemini(handler)

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        assert raised.value.status_code == 302
        assert len(handler.seen) == 1

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_an_echoed_secret_is_redacted(self, make):
        echo = {"error": {"code": 401, "message": f"API key {KEY} / token {TOKEN} not valid"}}
        provider = make(Recorder(401, json_body=echo))

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        sent_secret = KEY if make is gemini else TOKEN  # each API sends only its own secret, and redacts it
        assert "<secret>" in raised.value.detail
        assert sent_secret not in raised.value.detail

    def test_a_transport_failure_is_transient(self):
        def handler(request):
            raise httpx.ConnectError("boom", request=request)

        provider = gemini(handler)

        with pytest.raises(TransientProviderError):
            provider.generate(request_for(provider))


class TestDeadline:
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_post_json_gets_the_timeout_and_a_deadline_timeout_s_away(self, make, monkeypatch):
        calls = []

        def spy(self, url, *, headers, body, timeout_s, deadline):
            calls.append((url, headers, body, timeout_s, deadline))
            return HttpReply(200, GEMINI_REPLY, json.dumps(GEMINI_REPLY), None)

        provider = make()
        monkeypatch.setattr(HttpClient, "post_json", spy)
        request = request_for(provider, timeout_s=12.5)

        before = time.monotonic()
        result = provider.generate(request)
        after = time.monotonic()

        ((url, headers, body, timeout_s, deadline),) = calls
        assert url == (GEMINI_URL if make is gemini else CLOUD_URL)
        assert timeout_s == 12.5
        assert before + 12.5 <= deadline <= after + 12.5
        assert all(isinstance(value, _Secret) for value in headers.values())
        assert body["contents"][0]["parts"][0]["text"] == ENVELOPE
        assert result.content == {"records": []}

    def test_the_token_is_fetched_after_the_deadline_is_set(self, monkeypatch):
        deadlines = []

        def spy(self, url, *, headers, body, timeout_s, deadline):
            deadlines.append(deadline)
            return HttpReply(200, GEMINI_REPLY, json.dumps(GEMINI_REPLY), None)

        def slow_token():
            time.sleep(0.05)
            return TOKEN

        provider = cloud(token=slow_token)
        monkeypatch.setattr(HttpClient, "post_json", spy)

        before = time.monotonic()
        provider.generate(request_for(provider, timeout_s=10.0))

        assert deadlines[0] < before + 10.0 + 0.05


# ---------------------------------------------------------------------------------------------------------------
# Schema transforms
# ---------------------------------------------------------------------------------------------------------------


def transformed(make, case):
    model = REALISTIC[case][0]
    return make().transform_schema(response_schema(model))


class TestGeminiSchema:
    def test_the_small_wrapper_is_the_pinned_golden(self):
        assert gemini().transform_schema(SMALL) == SMALL_FOR_GEMINI

    def test_it_is_pure_and_fresh(self):
        provider = gemini()
        before = copy.deepcopy(SMALL)

        first = provider.transform_schema(SMALL)
        second = provider.transform_schema(SMALL)

        assert SMALL == before
        assert first == second
        assert first is not second
        first["$defs"]["Part"]["required"].append("weight")
        first["properties"]["records"]["items"]["required"].clear()
        assert SMALL == before
        assert provider.transform_schema(SMALL) == second

    @pytest.mark.parametrize("case", list(REALISTIC))
    def test_a_realistic_schema_keeps_only_the_documented_subset(self, case):
        schema = transformed(gemini, case)

        defs = schema.get("$defs", {})
        for node in nodes(schema):
            assert set(node) <= GEMINI_KEYWORDS, sorted(set(node) - GEMINI_KEYWORDS)
            if "type" in node:
                assert node["type"] in JSON_TYPES
            if "format" in node:
                assert node["format"] in ("date", "time")
            if "$ref" in node:
                assert node["$ref"].removeprefix("#/$defs/") in defs
        assert ('format: \\"date-time\\"' in json.dumps(schema)) == (
            "date-time" in json.dumps(response_schema(REALISTIC[case][0]))
        )

    @pytest.mark.parametrize("case", list(REALISTIC))
    @pytest.mark.parametrize("nulls", [True, False])
    def test_a_record_shaped_like_it_validates(self, case, nulls):
        model, _, record = REALISTIC[case]
        schema = transformed(gemini, case)
        stamp = DATETIME_STYLE.get(case, "2026-11-04T10:00:00+00:00")

        value = instance(schema, schema.get("$defs", {}), nulls=nulls, datetime_value=stamp)

        assert model.model_validate(value["records"][0]["record"]) is not None
        assert conforms(wrapped(record), schema, schema.get("$defs", {}))

    def test_const_becomes_a_one_value_enum(self):
        schema = {"type": "string", "const": "bolt"}

        assert gemini().transform_schema(schema) == {"type": "string", "enum": ["bolt"]}

    def test_an_enum_beside_a_const_is_kept(self):
        schema = {"type": "string", "const": "a", "enum": ["a", "b"]}

        assert gemini().transform_schema(schema) == {"type": "string", "enum": ["a", "b"]}

    def test_integer_enums_and_type_lists_are_kept(self):
        schema = {
            "type": "object",
            "properties": {"m": {"type": "integer", "enum": [1, 2]}, "n": {"type": ["string", "null"]}},
        }

        assert gemini().transform_schema(schema) == schema

    @pytest.mark.parametrize(
        "keyword", ["default", "examples", "deprecated", "x-unit", "nullable", "readOnly", "$comment"]
    )
    def test_an_unlisted_annotation_is_dropped(self, keyword):
        schema = {"type": "object", "properties": {"a": {"type": "string", keyword: 3}}, "required": ["a"]}

        assert gemini().transform_schema(schema) == {
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "required": ["a"],
        }

    @pytest.mark.parametrize(
        ("value", "shown"),
        [
            ("date-time", '"date-time"'),
            ("uuid", '"uuid"'),
            ("email", '"email"'),
            (5, "5"),
            (["date"], '["date"]'),
            ({"a": 1}, '{"a":1}'),
        ],
    )
    def test_an_unlisted_format_becomes_a_hint(self, value, shown):
        assert gemini().transform_schema({"type": "string", "format": value}) == {
            "type": "string",
            "description": f"{{format: {shown}}}",
        }

    @pytest.mark.parametrize("value", ["date", "time"])
    def test_a_listed_format_is_kept(self, value):
        assert gemini().transform_schema({"type": "string", "format": value}) == {"type": "string", "format": value}

    @pytest.mark.parametrize(
        ("node", "expected"),
        [
            ({"type": "integer", "exclusiveMinimum": 0}, {"type": "integer", "minimum": 1}),
            ({"type": "integer", "exclusiveMaximum": 10}, {"type": "integer", "maximum": 9}),
            ({"type": "integer", "exclusiveMinimum": 0.5}, {"type": "integer", "minimum": 1}),
            ({"type": "integer", "exclusiveMaximum": 9.5}, {"type": "integer", "maximum": 9}),
            ({"type": "integer", "exclusiveMinimum": -3}, {"type": "integer", "minimum": -2}),
            ({"type": "integer", "exclusiveMinimum": -2.5}, {"type": "integer", "minimum": -2}),
            ({"type": "integer", "exclusiveMaximum": -2.5}, {"type": "integer", "maximum": -3}),
            ({"type": "integer", "exclusiveMinimum": 2.0}, {"type": "integer", "minimum": 3}),
            ({"type": "integer", "minimum": 5, "exclusiveMinimum": 0}, {"type": "integer", "minimum": 5}),
            ({"type": "integer", "minimum": 0, "exclusiveMinimum": 3}, {"type": "integer", "minimum": 4}),
            ({"type": "integer", "maximum": 5, "exclusiveMaximum": 10}, {"type": "integer", "maximum": 5}),
            ({"type": "integer", "maximum": 50, "exclusiveMaximum": 10}, {"type": "integer", "maximum": 9}),
            ({"type": ["integer", "null"], "exclusiveMinimum": 0}, {"type": ["integer", "null"], "minimum": 1}),
        ],
    )
    def test_an_integer_exclusive_bound_becomes_inclusive(self, node, expected):
        result = gemini().transform_schema(node)

        assert result == expected
        assert all(type(result[bound]) is int for bound in ("minimum", "maximum") if bound in result)

    @pytest.mark.parametrize(
        ("node", "hint"),
        [
            ({"type": "number", "exclusiveMinimum": 0}, "{exclusiveMinimum: 0}"),
            ({"type": "number", "exclusiveMaximum": 1.5}, "{exclusiveMaximum: 1.5}"),
            ({"type": "integer", "exclusiveMinimum": True}, "{exclusiveMinimum: true}"),
            ({"type": ["integer", "number"], "exclusiveMinimum": 0}, "{exclusiveMinimum: 0}"),
            ({"type": "string", "pattern": "^P"}, '{pattern: "^P"}'),
            ({"type": "string", "minLength": 2, "maxLength": 3}, "{maxLength: 3, minLength: 2}"),
            ({"type": "number", "multipleOf": 0.5}, "{multipleOf: 0.5}"),
            ({"type": "array", "items": {"type": "string"}, "uniqueItems": True}, "{uniqueItems: true}"),
            ({"type": "object", "properties": {"a": {"type": "string"}}, "minProperties": 1}, "{minProperties: 1}"),
        ],
    )
    def test_a_constraint_outside_the_subset_becomes_a_description_hint(self, node, hint):
        result = gemini().transform_schema(node)

        assert result["description"] == hint
        assert not {"pattern", "minLength", "maxLength", "multipleOf", "uniqueItems", "minProperties"} & set(result)
        assert not {"exclusiveMinimum", "exclusiveMaximum"} & set(result)

    @pytest.mark.parametrize("description", ["", 5, None])
    def test_a_hint_replaces_an_empty_or_non_text_description(self, description):
        node = {"type": "string", "description": description, "pattern": "^P"}

        assert gemini().transform_schema(node)["description"] == '{pattern: "^P"}'

    def test_a_hint_follows_the_fields_own_description(self):
        node = {"type": "string", "description": "the part number", "pattern": "^P", "format": "date-time"}

        assert gemini().transform_schema(node) == {
            "type": "string",
            "description": 'the part number\n\n{format: "date-time", pattern: "^P"}',
        }

    def test_a_schema_valued_additional_properties_is_transformed(self):
        schema = {
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "additionalProperties": {"type": "integer", "default": 1},
        }

        assert gemini().transform_schema(schema)["additionalProperties"] == {"type": "integer"}


class TestGoogleCloudSchema:
    def test_the_small_wrapper_is_the_pinned_golden(self):
        assert cloud().transform_schema(SMALL) == SMALL_FOR_CLOUD

    def test_it_is_pure_and_fresh_even_where_one_def_is_inlined_twice(self):
        schema = {
            "type": "object",
            "properties": {"a": {"$ref": "#/$defs/P"}, "b": {"$ref": "#/$defs/P"}},
            "$defs": {"P": {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}},
        }
        before = copy.deepcopy(schema)
        provider = cloud()

        result = provider.transform_schema(schema)
        result["properties"]["a"]["required"].append("y")
        result["properties"]["a"]["properties"]["x"]["type"] = "NUMBER"

        assert schema == before
        assert result["properties"]["b"] == {
            "type": "OBJECT",
            "properties": {"x": {"type": "STRING"}},
            "required": ["x"],
        }
        assert provider.transform_schema(schema)["properties"]["a"] == result["properties"]["b"]

    @pytest.mark.parametrize("case", list(REALISTIC))
    def test_a_realistic_schema_obeys_the_openapi_subset(self, case):
        schema = transformed(cloud, case)

        for node in nodes(schema):
            assert set(node) <= CLOUD_KEYWORDS, sorted(set(node) - CLOUD_KEYWORDS)
            if "type" in node:
                assert node["type"] in OPENAPI_TYPES
            if "enum" in node:
                assert node["type"] == "STRING"
                assert all(isinstance(value, str) for value in node["enum"])
            if "format" in node:
                assert node["type"] == "STRING"
                assert node["format"] in ("date", "time", "duration")
            if "nullable" in node:
                assert node["nullable"] is True
            if "anyOf" in node:
                assert len(node["anyOf"]) >= 2
                assert not any(is_null(member) for member in node["anyOf"])
        assert "$ref" not in json.dumps(schema)
        assert "$defs" not in json.dumps(schema)

    @pytest.mark.parametrize("case", list(REALISTIC))
    @pytest.mark.parametrize("nulls", [True, False])
    def test_a_record_shaped_like_it_validates(self, case, nulls):
        model, _, record = REALISTIC[case]
        schema = transformed(cloud, case)
        stamp = DATETIME_STYLE.get(case, "2026-11-04T10:00:00+00:00")

        value = instance(schema, {}, nulls=nulls, datetime_value=stamp)

        assert model.model_validate(value["records"][0]["record"]) is not None
        assert conforms(wrapped(record), schema, {})

    def test_optional_becomes_nullable_and_the_outer_description_wins(self):
        schema = {
            "type": "object",
            "properties": {"p": {"anyOf": [{"$ref": "#/$defs/P"}, {"type": "null"}], "description": "outer"}},
            "$defs": {"P": {"type": "object", "description": "inner", "properties": {"x": {"type": "string"}}}},
        }

        assert cloud().transform_schema(schema)["properties"]["p"] == {
            "type": "OBJECT",
            "description": "outer",
            "nullable": True,
            "properties": {"x": {"type": "STRING"}},
        }

    def test_a_ref_sibling_description_wins_over_the_defs(self):
        schema = {
            "type": "object",
            "properties": {"p": {"$ref": "#/$defs/E", "description": "the status", "default": "a"}},
            "$defs": {"E": {"type": "string", "enum": ["a", "b"], "title": "E", "description": "an enum"}},
        }

        assert cloud().transform_schema(schema)["properties"]["p"] == {
            "type": "STRING",
            "enum": ["a", "b"],
            "description": "the status",
        }

    def test_an_optional_decimal_is_a_nullable_any_of(self):
        schema = {"anyOf": [{"type": "number", "minimum": 0}, {"type": "string", "pattern": "x"}, {"type": "null"}]}

        assert cloud().transform_schema(schema) == {
            "anyOf": [{"type": "NUMBER", "minimum": 0}, {"type": "STRING", "description": '{pattern: "x"}'}],
            "nullable": True,
        }

    @pytest.mark.parametrize(
        ("values", "shown"), [([1, 2], "[1,2]"), ([True, False], "[true,false]"), ([1, "a"], '[1,"a"]')]
    )
    def test_an_enum_that_is_not_all_strings_becomes_a_hint(self, values, shown):
        assert cloud().transform_schema({"type": "integer", "enum": values}) == {
            "type": "INTEGER",
            "description": f"{{enum: {shown}}}",
        }

    @pytest.mark.parametrize(("case", "hint"), [("camelCase BOM", "{enum: [1,2]}"), ("lab report", "{enum: [0,1]}")])
    def test_a_realistic_int_enum_reaches_the_model_as_a_hint(self, case, hint):
        assert hint in json.dumps(transformed(cloud, case))

    def test_a_hint_follows_an_outer_description_over_an_inlined_enum(self):
        schema = {
            "type": "object",
            "properties": {"p": {"$ref": "#/$defs/E", "description": "the priority"}},
            "$defs": {"E": {"type": "integer", "enum": [0, 1], "title": "E"}},
        }

        assert cloud().transform_schema(schema)["properties"]["p"] == {
            "type": "INTEGER",
            "description": "the priority\n\n{enum: [0,1]}",
        }

    @pytest.mark.parametrize(
        ("schema", "expected"),
        [
            ({"const": "a", "type": "string"}, {"type": "STRING", "enum": ["a"]}),
            ({"const": 1, "type": "integer"}, {"type": "INTEGER", "description": "{const: 1}"}),
            ({"const": True, "type": "boolean"}, {"type": "BOOLEAN", "description": "{const: true}"}),
            ({"const": "a", "enum": ["a", "b"], "type": "string"}, {"type": "STRING", "enum": ["a", "b"]}),
        ],
    )
    def test_a_const_becomes_a_string_enum_or_a_hint(self, schema, expected):
        assert cloud().transform_schema(schema) == expected

    @pytest.mark.parametrize(
        ("node", "expected"),
        [
            ({"type": "integer", "exclusiveMinimum": 0}, {"type": "INTEGER", "minimum": 1}),
            ({"type": "integer", "exclusiveMaximum": 9.5}, {"type": "INTEGER", "maximum": 9}),
            ({"type": "integer", "minimum": 5, "exclusiveMinimum": 0}, {"type": "INTEGER", "minimum": 5}),
            (
                {"anyOf": [{"type": "integer", "exclusiveMinimum": 0}, {"type": "null"}]},
                {"type": "INTEGER", "minimum": 1, "nullable": True},
            ),
            ({"type": "number", "exclusiveMinimum": 0}, {"type": "NUMBER", "description": "{exclusiveMinimum: 0}"}),
            (
                {"type": "integer", "exclusiveMinimum": True},
                {"type": "INTEGER", "description": "{exclusiveMinimum: true}"},
            ),
        ],
    )
    def test_an_exclusive_bound_becomes_inclusive_on_an_integer_else_a_hint(self, node, expected):
        assert cloud().transform_schema(node) == expected

    @pytest.mark.parametrize(
        ("node", "hint"),
        [
            ({"type": "string", "pattern": "^P"}, '{pattern: "^P"}'),
            ({"type": "string", "minLength": 2, "maxLength": 3}, "{maxLength: 3, minLength: 2}"),
            ({"type": "number", "multipleOf": 0.5}, "{multipleOf: 0.5}"),
            ({"type": "object", "properties": {"a": {"type": "string"}}, "minProperties": 1}, "{minProperties: 1}"),
        ],
    )
    def test_a_constraint_outside_the_subset_becomes_a_description_hint(self, node, hint):
        assert cloud().transform_schema(node)["description"] == hint

    def test_a_hint_follows_the_fields_own_description(self):
        node = {"type": "string", "description": "the code", "pattern": "^P", "format": "date-time"}

        assert cloud().transform_schema(node) == {
            "type": "STRING",
            "description": 'the code\n\n{format: "date-time", pattern: "^P"}',
        }

    def test_a_string_enum_without_a_type_gets_one(self):
        assert cloud().transform_schema({"enum": ["a", "b"]}) == {"type": "STRING", "enum": ["a", "b"]}

    @pytest.mark.parametrize(("given", "kept"), [(True, {"nullable": True}), (False, {}), ("yes", {})])
    def test_a_given_nullable_true_is_kept(self, given, kept):
        assert cloud().transform_schema({"type": "string", "nullable": given}) == {"type": "STRING", **kept}

    def test_a_type_list_with_null_is_nullable(self):
        assert cloud().transform_schema({"type": ["string", "null"]}) == {"type": "STRING", "nullable": True}

    def test_a_null_only_any_of_is_the_null_type(self):
        assert cloud().transform_schema({"anyOf": [{"type": "null"}]}) == {"type": "NULL", "nullable": True}

    def test_a_ref_chain_is_inlined(self):
        schema = {"$ref": "#/$defs/A", "$defs": {"A": {"$ref": "#/$defs/B"}, "B": {"type": "boolean", "title": "B"}}}

        assert cloud().transform_schema(schema) == {"type": "BOOLEAN"}

    @pytest.mark.parametrize(
        ("node", "kept"),
        [
            ({"type": "integer", "format": "int64"}, {"type": "INTEGER", "description": '{format: "int64"}'}),
            ({"type": "string", "format": "date-time"}, {"type": "STRING", "description": '{format: "date-time"}'}),
            ({"type": "string", "format": "date"}, {"type": "STRING", "format": "date"}),
            ({"type": "string", "format": "time"}, {"type": "STRING", "format": "time"}),
            ({"type": "string", "format": "duration"}, {"type": "STRING", "format": "duration"}),
            ({"type": "string", "format": "email"}, {"type": "STRING", "description": '{format: "email"}'}),
            ({"type": "string", "format": ["date"]}, {"type": "STRING", "description": '{format: ["date"]}'}),
            ({"type": "integer", "format": "date"}, {"type": "INTEGER", "description": '{format: "date"}'}),
            ({"type": "string", "format": {"a": 1}}, {"type": "STRING", "description": '{format: {"a":1}}'}),
            (
                {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3, "uniqueItems": True},
                {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "minItems": 1,
                    "maxItems": 3,
                    "description": "{uniqueItems: true}",
                },
            ),
            (
                {
                    "type": "object",
                    "properties": {"a": {"type": "string"}},
                    "additionalProperties": False,
                    "title": "T",
                },
                {
                    "type": "OBJECT",
                    "properties": {"a": {"type": "STRING"}},
                    "description": "{additionalProperties: false}",
                },
            ),
            (
                {"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": True},
                {"type": "OBJECT", "properties": {"a": {"type": "STRING"}}},
            ),
        ],
    )
    def test_keywords_outside_the_subset_are_dropped_or_hinted(self, node, kept):
        assert cloud().transform_schema(node) == kept

    def test_a_cycle_is_refused(self):
        schema = {"$ref": "#/$defs/A", "$defs": {"A": {"type": "object", "properties": {"a": {"$ref": "#/$defs/A"}}}}}

        with pytest.raises(RationalizationError, match="cycle"):
            cloud().transform_schema(schema)

    def test_an_expansion_past_the_bound_is_refused_quickly(self):
        defs = {
            f"D{i}": {
                "type": "object",
                "properties": {"a": {"$ref": f"#/$defs/D{i + 1}"}, "b": {"$ref": f"#/$defs/D{i + 1}"}},
            }
            for i in range(24)
        }
        defs["D24"] = {"type": "string"}
        schema = {"$ref": "#/$defs/D0", "$defs": defs}

        started = time.monotonic()
        with pytest.raises(RationalizationError, match="nodes"):
            cloud().transform_schema(schema)

        assert time.monotonic() - started < 10
        assert gemini().transform_schema(schema)["$defs"]["D0"]["properties"]["a"] == {"$ref": "#/$defs/D1"}

    def test_a_schema_valued_additional_properties_is_refused(self):
        schema = {
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "additionalProperties": {"type": "integer"},
        }

        with pytest.raises(RationalizationError, match="additionalProperties"):
            cloud().transform_schema(schema)

    @pytest.mark.parametrize("kind", [["string", "integer"], ["string", "integer", "null"], "date", 5, ["text"]])
    def test_a_type_responseschema_cannot_express_is_refused(self, kind):
        with pytest.raises(RationalizationError, match="type"):
            cloud().transform_schema({"type": kind})


class TestSchemaRefusals:
    @pytest.mark.parametrize("make", [gemini, cloud])
    @pytest.mark.parametrize("keyword", ["oneOf", "allOf", "not", "if", "patternProperties", "prefixItems", "$id"])
    def test_a_keyword_that_holds_unread_subschemas_is_refused_by_name(self, make, keyword):
        schema = {"type": "object", "properties": {"a": {"type": "string", keyword: [{"type": "string"}]}}}

        with pytest.raises(RationalizationError) as raised:
            make().transform_schema(schema)

        assert keyword in str(raised.value)
        assert "#/properties/a" in str(raised.value)

    @pytest.mark.parametrize("make", [gemini, cloud])
    @pytest.mark.parametrize("ref", ["#/$defs/Missing", "#/definitions/P", "https://evil.example/s.json", "#", 5])
    def test_a_ref_that_names_no_local_def_is_refused(self, make, ref):
        schema = {"type": "object", "properties": {"a": {"$ref": ref}}, "$defs": {"P": {"type": "string"}}}

        with pytest.raises(RationalizationError, match=r"\$ref"):
            make().transform_schema(schema)

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_a_def_name_is_a_json_pointer_token(self, make):
        defs = {"a/b~c": {"type": "string"}}
        escaped = {"type": "object", "properties": {"x": {"$ref": "#/$defs/a~1b~0c"}}, "$defs": defs}
        raw = {"type": "object", "properties": {"x": {"$ref": "#/$defs/a/b~c"}}, "$defs": defs}

        assert make().transform_schema(escaped)["properties"]["x"] in ({"$ref": "#/$defs/a~1b~0c"}, {"type": "STRING"})
        with pytest.raises(RationalizationError, match=r"\$ref must be a local"):
            make().transform_schema(raw)

    @pytest.mark.parametrize("make", [gemini, cloud])
    @pytest.mark.parametrize("defs", [[{"type": "string"}], "x", 5])
    def test_defs_that_are_not_a_mapping_are_refused(self, make, defs):
        with pytest.raises(RationalizationError, match="'\\$defs' must map names to schemas"):
            make().transform_schema({"type": "string", "$defs": defs})

    def test_a_ref_chain_past_the_depth_bound_is_refused_by_google_cloud(self):
        def chain(length):
            defs = {f"D{index}": {"$ref": f"#/$defs/D{index + 1}"} for index in range(length)}
            defs[f"D{length}"] = {"type": "string"}
            return {"$ref": "#/$defs/D0", "$defs": defs}

        # chain(n) is n + 1 hops (the root's $ref, then D0 ... D(n-1)); the last of the SCHEMA_MAX_DEPTH steps reads
        # the schema the chain ends at.
        assert cloud().transform_schema(chain(SCHEMA_MAX_DEPTH - 2)) == {"type": "STRING"}
        with pytest.raises(RationalizationError, match="chain runs more than"):
            cloud().transform_schema(chain(SCHEMA_MAX_DEPTH - 1))
        assert gemini().transform_schema(chain(SCHEMA_MAX_DEPTH))["$ref"] == "#/$defs/D0"

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_an_empty_type_list_is_refused(self, make):
        with pytest.raises(RationalizationError, match="'type' must be"):
            make().transform_schema({"type": []})

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_the_node_bound_is_exact(self, make):
        def wide(count):
            return {"type": "object", "properties": {f"p{index}": {"type": "string"} for index in range(count)}}

        assert len(make().transform_schema(wide(SCHEMA_MAX_NODES - 1))["properties"]) == SCHEMA_MAX_NODES - 1
        with pytest.raises(RationalizationError, match=f"{SCHEMA_MAX_NODES} schema nodes"):
            make().transform_schema(wide(SCHEMA_MAX_NODES))

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_nesting_past_the_depth_bound_is_refused(self, make):
        schema = {"type": "string"}
        for _ in range(SCHEMA_MAX_DEPTH + 1):
            schema = {"type": "array", "items": schema}

        with pytest.raises(RationalizationError, match="deep"):
            make().transform_schema(schema)

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_nesting_at_the_depth_bound_passes(self, make):
        schema = {"type": "string"}
        for _ in range(SCHEMA_MAX_DEPTH - 1):
            schema = {"type": "array", "items": schema}

        assert make().transform_schema(schema)["type"] in ("array", "ARRAY")

    @pytest.mark.parametrize("make", [gemini, cloud])
    @pytest.mark.parametrize("schema", [{"type": "number", "minimum": math.nan}, {"a": {1, 2}}, {1: "x"}])
    def test_a_schema_that_is_not_exactly_json_is_refused(self, make, schema):
        with pytest.raises(RationalizationError):
            make().transform_schema(schema)

    @pytest.mark.parametrize("make", [gemini, cloud])
    @pytest.mark.parametrize("schema", [None, [], "schema"])
    def test_a_schema_that_is_not_a_mapping_is_a_type_error(self, make, schema):
        with pytest.raises(TypeError):
            make().transform_schema(schema)

    @pytest.mark.parametrize("make", [gemini, cloud])
    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "object", "properties": [1]},
            {"type": "array", "items": [{"type": "string"}]},
            {"anyOf": {"type": "string"}},
            {"type": "object", "properties": {"a": 5}},
            {"anyOf": [5, {"type": "null"}]},
        ],
    )
    def test_a_malformed_subschema_is_refused(self, make, schema):
        with pytest.raises(RationalizationError):
            make().transform_schema(schema)


# ---------------------------------------------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------------------------------------------


class TestIdentity:
    def test_the_gemini_api_identity(self):
        provider = gemini()

        assert provider.model_id == f"gemini:{MODEL}"
        assert provider.provider_kind == "gemini"
        assert provider.rate_key == "generativelanguage.googleapis.com"
        assert provider.model_family == "google"
        assert json.dumps(provider.generation_params, sort_keys=True) == (
            '{"api": "v1beta", "schema_field": "responseJsonSchema", "seed": 0, "wire": "generate-content/1"}'
        )

    def test_the_google_cloud_identity_names_the_resource(self):
        provider = cloud()

        assert provider.model_id == f"google-cloud:{CLOUD_RESOURCE}"
        assert provider.provider_kind == "google-cloud-gemini"
        assert provider.rate_key == provider.model_id  # quotas are per model: one bucket per resource
        assert provider.model_family == "google"
        assert json.dumps(provider.generation_params, sort_keys=True) == (
            '{"api": "v1", "schema_field": "responseSchema", "seed": 0, "wire": "generate-content/1"}'
        )

    def test_the_express_identity(self):
        provider = express()

        assert provider.model_id == f"google-cloud:publishers/google/models/{CLOUD_MODEL}"
        assert provider.provider_kind == "google-cloud-gemini"
        assert provider.rate_key == provider.model_id
        assert provider.generation_params == cloud().generation_params

    @pytest.mark.parametrize(("location", "host"), list(CLOUD_HOSTS.items()))
    def test_each_location_has_its_own_model_id_and_rate_key(self, location, host):
        provider = cloud(location=location)

        assert provider.model_id == (
            f"google-cloud:projects/{PROJECT}/locations/{location}/publishers/google/models/{CLOUD_MODEL}"
        )
        assert provider.rate_key == provider.model_id
        assert provider.max_connections == 8

    def test_two_models_in_one_project_never_share_a_rate_key(self):
        auth = GoogleCloudAuth(token=TokenSource())
        first, second = (
            GeminiProvider.google_cloud(model, project=PROJECT, location="us-central1", auth=auth)
            for model in ("gemini-2.5-pro", "gemini-2.5-flash")
        )
        quick, slow = (
            GeminiProvider.google_cloud_express(model, auth=GoogleCloudAuth(api_key=KEY))
            for model in ("gemini-2.5-pro", "gemini-2.5-flash")
        )

        assert first.rate_key != second.rate_key
        assert quick.rate_key != slow.rate_key

    def test_locations_projects_auth_and_express_differ(self):
        providers = [cloud(location=place) for place in CLOUD_HOSTS]
        providers += [cloud(project="other-project"), express()]
        same = [cloud(auth=GoogleCloudAuth(api_key=KEY)), cloud()]

        assert len({provider.model_id for provider in providers}) == len(providers)
        assert len({provider.rate_key for provider in providers}) == len(providers)
        assert len({provider.model_id for provider in same}) == 1  # the credential is not the model

    def test_it_differs_from_the_gemini_api_provider_of_the_same_model(self):
        direct = GeminiProvider(CLOUD_MODEL, api_key=KEY, transport=httpx.MockTransport(Recorder()))

        for provider in (cloud(), express()):
            assert provider.model_id != direct.model_id
            assert provider.provider_kind != direct.provider_kind
            assert provider.rate_key != direct.rate_key
            assert provider.generation_params != direct.generation_params
            assert provider.model_family == direct.model_family

    def test_none_valued_params_are_omitted(self):
        assert gemini(temperature=None, seed=None).generation_params == {
            "api": "v1beta",
            "schema_field": "responseJsonSchema",
            "wire": "generate-content/1",
        }
        assert cloud(temperature=None).generation_params == {
            "api": "v1",
            "schema_field": "responseSchema",
            "seed": 0,
            "wire": "generate-content/1",
        }

    def test_an_int_temperature_is_held_as_a_float(self):
        params = gemini(temperature=1, seed=7).generation_params

        assert json.dumps(params, sort_keys=True) == (
            '{"api": "v1beta", "schema_field": "responseJsonSchema", "seed": 7, "temperature": 1.0, '
            '"wire": "generate-content/1"}'
        )

    def test_generation_params_is_a_fresh_mapping(self):
        provider = gemini(temperature=0.5)

        provider.generation_params["temperature"] = 2.0

        assert provider.generation_params["temperature"] == 0.5

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_the_defaults_send_no_temperature_and_seed_zero(self, make):
        handler = Recorder()
        provider = make(handler)

        provider.generate(request_for(provider))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert "temperature" not in config
        assert config["seed"] == 0
        assert "temperature" not in provider.generation_params

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_the_family_is_google_and_read_only(self, make):
        provider = make()

        assert family_problem(provider.model_family) is None
        with pytest.raises(AttributeError):
            provider.model_family = "anthropic"
        assert provider.model_family == "google"

    @pytest.mark.parametrize(
        ("given", "held"), [({}, 8), ({"max_connections": 3}, 3), ({"max_connections": 1024}, 1024)]
    )
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_max_connections_is_exposed_read_only(self, make, given, held):
        provider = make(**given)

        assert provider.max_connections == held
        with pytest.raises(AttributeError):
            provider.max_connections = 99
        assert provider.max_connections == held

    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_it_is_a_model_provider(self, make):
        provider = make()

        assert isinstance(provider, ModelProvider)
        assert check_provider(provider) is provider

    def test_a_google_cloud_model_id_past_the_identity_bound_is_refused(self):
        project = "a" * 63 + "." + "b" * 63 + "." + "c" * 63 + ".example:my-project"

        with pytest.raises(RationalizationError, match="model id"):
            GeminiProvider.google_cloud(
                "m" * 256, project=project, location="l" * 52, auth=GoogleCloudAuth(token=TokenSource())
            )


# ---------------------------------------------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------------------------------------------

BAD_SEGMENTS = ["", "a/b", "..", ".", "evil.example/x", "gemini 2", "models/gemini-2.5-pro", "a?b", "a%2Fb", 5, None]


class TestConstruction:
    @pytest.mark.parametrize("model", BAD_SEGMENTS)
    def test_a_bad_gemini_model_is_refused(self, model):
        with pytest.raises(RationalizationError):
            GeminiProvider(model, api_key=KEY)

    @pytest.mark.parametrize("model", BAD_SEGMENTS)
    @pytest.mark.parametrize("auth", [GoogleCloudAuth(token=TokenSource()), GoogleCloudAuth(api_key=KEY)])
    def test_a_bad_google_cloud_model_is_refused(self, model, auth):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider.google_cloud(model, project=PROJECT, location="us-central1", auth=auth)

        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize("project", ["", "a/b", "..", ".", "evil.example/x", "My-Project", "short", 5])
    def test_a_bad_project_is_refused(self, project):
        with pytest.raises(RationalizationError, match="project") as raised:
            GeminiProvider.google_cloud(
                CLOUD_MODEL, project=project, location="us-central1", auth=GoogleCloudAuth(api_key=KEY)
            )

        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize("location", ["", "a/b", "..", ".", "l" * 53, "evil.example/x", "US-CENTRAL1", "-us", 5])
    def test_a_bad_location_is_refused(self, location):
        with pytest.raises(RationalizationError, match="location") as raised:
            GeminiProvider.google_cloud(
                CLOUD_MODEL, project=PROJECT, location=location, auth=GoogleCloudAuth(api_key=KEY)
            )

        assert_secret_free(traceback_text(raised.value))

    def test_a_52_character_location_is_accepted(self):
        provider = cloud(location="l" * 52)

        assert f"/locations/{'l' * 52}/" in provider.rate_key

    @pytest.mark.parametrize("temperature", [-0.1, 2.1, math.nan, math.inf, -math.inf, True, "0.5", 10**400, [0.5]])
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_a_bad_temperature_is_refused(self, temperature, make):
        with pytest.raises(RationalizationError, match="temperature"):
            make(temperature=temperature)

    @pytest.mark.parametrize("temperature", [0, 2, 1.5, 0.0, None])
    def test_a_temperature_in_range_is_accepted(self, temperature):
        assert gemini(temperature=temperature).generation_params.get("temperature") == (
            None if temperature is None else float(temperature)
        )

    @pytest.mark.parametrize("seed", [True, 1.0, "1", 2**31, -(2**31) - 1])
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_a_bad_seed_is_refused(self, seed, make):
        with pytest.raises(RationalizationError, match="seed"):
            make(seed=seed)

    @pytest.mark.parametrize("seed", [None, 0, -1, 2**31 - 1, -(2**31)])
    def test_a_seed_in_range_is_accepted(self, seed):
        assert gemini(seed=seed).generation_params.get("seed") == seed

    @pytest.mark.parametrize("count", [0, 1025, True, 1.5, "8", None])
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_a_bad_max_connections_is_refused(self, count, make):
        with pytest.raises(RationalizationError, match="max_connections"):
            make(max_connections=count)

    @pytest.mark.parametrize("count", [1, 1024])
    def test_max_connections_in_range_is_accepted(self, count):
        assert gemini(max_connections=count).model_id == f"gemini:{MODEL}"

    @pytest.mark.parametrize("transport", [object(), "https://evil.example", 5])
    def test_a_transport_that_is_not_one_is_refused_naming_it(self, transport):
        with pytest.raises(RationalizationError, match="transport") as raised:
            GeminiProvider(MODEL, api_key=KEY, transport=transport)

        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    @pytest.mark.parametrize("make", [cloud, express])
    def test_a_google_cloud_transport_that_is_not_one_is_refused(self, make):
        with pytest.raises(RationalizationError, match="transport"):
            GeminiProvider.google_cloud(
                CLOUD_MODEL, project=PROJECT, location="global", auth=GoogleCloudAuth(api_key=KEY), transport=5
            )

    @pytest.mark.parametrize("key", ["", "   ", f"{KEY}\n", f" {KEY}", "AIza-kéy-hunter2", 5, b"AIza-hunter2"])
    def test_a_bad_api_key_is_refused_without_its_value(self, key):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider(MODEL, api_key=key)

        assert "API key" in str(raised.value)
        assert_secret_free(str(raised.value))
        assert_secret_free(traceback_text(raised.value))

    def test_a_str_subclass_key_is_refused_by_its_class(self):
        class SneakyKey(str):
            pass

        with pytest.raises(RationalizationError, match="must be a str, got SneakyKey") as raised:
            GeminiProvider(MODEL, api_key=SneakyKey(KEY))

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_a_missing_key_names_the_variable(self):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider(MODEL)

        assert GEMINI_API_KEY_ENV in str(raised.value)

    def test_a_blank_environment_key_is_missing(self, monkeypatch):
        monkeypatch.setenv(GEMINI_API_KEY_ENV, "  ")

        with pytest.raises(RationalizationError, match=GEMINI_API_KEY_ENV):
            GeminiProvider(MODEL)

    def test_an_invalid_environment_key_names_the_variable_never_its_value(self, monkeypatch):
        monkeypatch.setenv(GEMINI_API_KEY_ENV, f"{KEY}\n")

        with pytest.raises(RationalizationError) as raised:
            GeminiProvider(MODEL)

        assert GEMINI_API_KEY_ENV in str(raised.value)
        assert_secret_free(str(raised.value))
        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize("value", [f"{KEY}\n", f" {KEY}", "AIza-kéy-hunter2"])
    def test_an_invalid_environment_key_through_a_model_string_is_never_shown(self, monkeypatch, value):
        monkeypatch.setenv(GEMINI_API_KEY_ENV, value)

        with pytest.raises(RationalizationError) as raised:
            GeminiProvider.from_model_string(parse_model_string(f"gemini:{MODEL}"))

        assert GEMINI_API_KEY_ENV in str(raised.value)
        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_a_missing_environment_key_traceback_is_clean(self):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider(MODEL)

        assert_secret_free(traceback_text(raised.value))

    def test_a_later_refusal_holds_no_key_in_its_traceback(self):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider(MODEL, api_key=KEY, temperature=9)

        assert_secret_free(traceback_text(raised.value))


class TestFromModelString:
    def test_a_gemini_model_string_builds_the_provider(self):
        handler = Recorder()
        provider = GeminiProvider.from_model_string(
            parse_model_string(f"gemini:{MODEL}"), api_key=KEY, temperature=None, transport=httpx.MockTransport(handler)
        )

        provider.generate(request_for(provider))

        assert provider.model_id == f"gemini:{MODEL}"
        assert "temperature" not in sent_json(handler.seen[0])["generationConfig"]
        assert handler.seen[0].headers["x-goog-api-key"] == KEY

    def test_the_key_comes_from_the_environment(self, monkeypatch):
        monkeypatch.setenv(GEMINI_API_KEY_ENV, KEY)

        assert GeminiProvider.from_model_string(parse_model_string(f"gemini:{MODEL}")).provider_kind == "gemini"

    @pytest.mark.parametrize(
        "spec", ["openai:gpt-4o", "anthropic:claude-x", "ollama:llama3", "openai-compatible:m@https://h.example/v1"]
    )
    def test_another_kind_is_refused(self, spec):
        with pytest.raises(RationalizationError, match="gemini"):
            GeminiProvider.from_model_string(parse_model_string(spec), api_key=KEY)

    @pytest.mark.parametrize("spec", [f"gemini:{MODEL}", None, ("gemini", MODEL)])
    def test_something_other_than_a_model_string_is_refused(self, spec):
        with pytest.raises(RationalizationError, match="ModelString"):
            GeminiProvider.from_model_string(spec, api_key=KEY)

    @pytest.mark.parametrize("key", [KEY, f"{KEY}\n", _Secret(KEY)])
    def test_a_refused_model_string_holds_no_key_in_its_traceback(self, key):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider.from_model_string(parse_model_string("openai:gpt-4o"), api_key=key)

        assert_secret_free(traceback_text(raised.value))

    @pytest.mark.parametrize(
        "spec",
        [
            parse_model_string(f"gemini:{MODEL}"),
            parse_model_string("openai:gpt-4o"),
            f"gemini:{MODEL}",
            ModelString("gemini", MODEL, "https://proxy.example/v1beta"),
        ],
        ids=["unknown option", "another kind", "not parsed", "an endpoint"],
    )
    @pytest.mark.parametrize("option", ["key", "apikey", "API_KEY", "token"])
    def test_a_misspelt_key_option_never_sits_in_a_raising_frame(self, spec, option):
        with pytest.raises(RationalizationError) as raised:
            GeminiProvider.from_model_string(spec, api_key=KEY, **{option: KEY})

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_an_unknown_option_is_refused_by_name_never_a_type_error(self):
        with pytest.raises(RationalizationError, match="base_url, project") as raised:
            GeminiProvider.from_model_string(
                parse_model_string(f"gemini:{MODEL}"), api_key=KEY, project="p", base_url="https://evil.example"
            )

        assert_secret_free(traceback_text(raised.value))

    def test_an_invalid_key_through_a_model_string_is_never_shown(self):
        with pytest.raises(RationalizationError, match="API key") as raised:
            GeminiProvider.from_model_string(parse_model_string(f"gemini:{MODEL}"), api_key=f"{KEY}\n")

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_a_gemini_model_string_with_an_endpoint_is_refused(self):
        spec = ModelString("gemini", MODEL, "https://proxy.example/v1beta")

        with pytest.raises(RationalizationError, match="endpoint"):
            GeminiProvider.from_model_string(spec, api_key=KEY)


# ---------------------------------------------------------------------------------------------------------------
# Secrets, logs and lifecycle
# ---------------------------------------------------------------------------------------------------------------


class TestSecrets:
    @pytest.mark.parametrize("make", [gemini, cloud, express])
    def test_the_repr_hides_key_and_token(self, make):
        provider = make()

        text = repr(provider) + str(provider)

        assert_secret_free(text)
        assert repr(provider) == f"GeminiProvider(model_id={provider.model_id!r})"

    @pytest.mark.parametrize("make", [gemini, cloud, express])
    def test_a_401_traceback_with_locals_holds_neither_key_nor_token(self, make):
        provider = make(Recorder(401, json_body=ERROR_BODY))

        with pytest.raises(PermanentProviderError) as raised:
            provider.generate(request_for(provider))

        text = traceback_text(raised.value)
        assert "generate" in text
        assert_secret_free(text)
        assert_secret_free(str(raised.value) + repr(raised.value))

    @pytest.mark.parametrize("make", [gemini, cloud, express])
    def test_it_never_pickles(self, make):
        with pytest.raises(TypeError, match="GeminiProvider never pickles"):
            pickle.dumps(make())

    @pytest.mark.parametrize("make", [gemini, cloud, express])
    @pytest.mark.parametrize("status", [200, 401])
    def test_logs_hold_no_secret_prompt_or_envelope(self, make, status, caplog):
        provider = make(Recorder(status, json_body=GEMINI_REPLY if status == 200 else ERROR_BODY))

        with caplog.at_level(logging.DEBUG):
            try:
                provider.generate(request_for(provider))
            except PermanentProviderError:
                pass

        logged = caplog.text + "".join(str(record.args) for record in caplog.records)
        assert_secret_free(logged)
        assert SYSTEM not in logged
        assert ENVELOPE not in logged


class TestLifecycle:
    @pytest.mark.parametrize("make", [gemini, cloud])
    def test_open_opens_the_http_client(self, make, monkeypatch):
        opened = []
        monkeypatch.setattr(HttpClient, "open", lambda self: opened.append(self))
        provider = make()

        provider.open()

        assert len(opened) == 1

    def test_a_request_that_is_not_a_model_request_is_a_type_error(self):
        with pytest.raises(TypeError, match="ModelRequest"):
            gemini().generate({"system_prompt": SYSTEM})

    def test_no_request_is_sent_until_generate(self):
        handler = Recorder()

        provider = gemini(handler)
        provider.transform_schema(SMALL)
        provider.open()

        assert handler.seen == []


def from_model_string_gemini(handler=None, **options):
    handler = Recorder() if handler is None else handler
    options.setdefault("api_key", KEY)
    return GeminiProvider.from_model_string(
        parse_model_string(f"gemini:{MODEL}"), transport=httpx.MockTransport(handler), **options
    )


class TestThinkingLevel:
    def test_constants_expose_thinking_levels_and_from_model_string_option(self):
        assert gemini_module.THINKING_LEVELS == frozenset({"LOW", "MEDIUM", "HIGH"})
        assert type(gemini_module.THINKING_LEVELS) is frozenset
        assert "thinking_level" in FROM_MODEL_STRING_OPTIONS

    def test_signatures_expose_thinking_level_keyword_defaulting_to_none(self):
        init_params = inspect.signature(GeminiProvider.__init__).parameters
        cloud_params = inspect.signature(GeminiProvider.google_cloud).parameters
        express_params = inspect.signature(GeminiProvider.google_cloud_express).parameters

        assert list(init_params) == [
            "self",
            "model",
            "api_key",
            "temperature",
            "seed",
            "thinking_level",
            "max_connections",
            "transport",
        ]
        assert list(cloud_params) == [
            "model",
            "project",
            "location",
            "auth",
            "temperature",
            "seed",
            "thinking_level",
            "max_connections",
            "transport",
        ]
        assert list(express_params) == [
            "model",
            "auth",
            "temperature",
            "seed",
            "thinking_level",
            "max_connections",
            "transport",
        ]
        for params in (init_params, cloud_params, express_params):
            assert params["thinking_level"].kind is inspect.Parameter.KEYWORD_ONLY
            assert params["thinking_level"].default is None

    @pytest.mark.parametrize("make", [gemini, cloud, express, from_model_string_gemini])
    @pytest.mark.parametrize("given", [{}, {"thinking_level": None}], ids=["default", "none"])
    def test_none_thinking_level_is_omitted_from_generation_params_and_wire(self, make, given):
        handler = Recorder()
        provider = make(handler, **given)

        provider.generate(request_for(provider))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert "thinking_level" not in provider.generation_params
        assert "thinkingConfig" not in config

    @pytest.mark.parametrize("make", [gemini, cloud, express, from_model_string_gemini])
    @pytest.mark.parametrize("level", ["LOW", "MEDIUM", "HIGH"])
    def test_thinking_level_is_in_generation_params_and_sent_as_thinking_config(self, make, level):
        handler = Recorder()
        provider = make(handler, thinking_level=level)

        provider.generate(request_for(provider))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert provider.generation_params["thinking_level"] == level
        assert config["thinkingConfig"] == {"thinkingLevel": level}

    @pytest.mark.parametrize("make", [gemini, cloud, express, from_model_string_gemini])
    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("low", "LOW"),
            ("medium", "MEDIUM"),
            ("high", "HIGH"),
            ("Low", "LOW"),
            ("MeDiUm", "MEDIUM"),
            ("hIgH", "HIGH"),
            (" low ", "LOW"),
            ("  MEDIUM\n", "MEDIUM"),
            ("\tHigh\t", "HIGH"),
        ],
    )
    def test_lowercase_and_mixed_case_thinking_level_is_normalized_to_uppercase(self, make, given, expected):
        handler = Recorder()
        provider = make(handler, thinking_level=given)

        provider.generate(request_for(provider))

        config = sent_json(handler.seen[0])["generationConfig"]
        assert provider.generation_params["thinking_level"] == expected
        assert config["thinkingConfig"] == {"thinkingLevel": expected}
        assert provider.generation_params == make(thinking_level=expected).generation_params
        assert gemini(thinking_level="low").generation_params == gemini(thinking_level="LOW").generation_params

    def test_each_thinking_level_produces_distinct_generation_params(self):
        seen = [gemini(thinking_level=level).generation_params for level in (None, "LOW", "MEDIUM", "HIGH")]

        assert len({json.dumps(params, sort_keys=True) for params in seen}) == 4

    @pytest.mark.parametrize("make", [gemini, cloud, express, from_model_string_gemini])
    @pytest.mark.parametrize("bad", [123, True, False, 1.5, ["LOW"], b"LOW", ("LOW",)])
    def test_a_non_str_thinking_level_is_refused(self, make, bad):
        with pytest.raises(RationalizationError, match="thinking_level must be None or a str") as raised:
            make(thinking_level=bad)

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    def test_a_str_subclass_thinking_level_is_refused_without_its_value(self):
        class SneakyLevel(str):
            pass

        with pytest.raises(RationalizationError, match="thinking_level must be None or a str") as raised:
            gemini(thinking_level=SneakyLevel(KEY))

        assert_secret_free(str(raised.value) + traceback_text(raised.value))

    @pytest.mark.parametrize("make", [gemini, cloud, express, from_model_string_gemini])
    @pytest.mark.parametrize(
        "bad",
        ["", "   ", "MINIMAL", "NONE", KEY, "canary-secret-hunter2"],
    )
    def test_an_invalid_str_thinking_level_is_refused_without_echoing_value(self, make, bad):
        with pytest.raises(
            RationalizationError, match='thinking_level must be None, "LOW", "MEDIUM" or "HIGH"'
        ) as raised:
            make(thinking_level=bad)

        text = str(raised.value) + repr(raised.value) + traceback_text(raised.value)
        assert_secret_free(text)
        if bad.strip():
            assert bad not in text


def test_the_module_imports_nothing_inside_functions_and_nothing_from_core():
    tree = ast.parse(Path(gemini_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    inner = [
        node for function in functions for node in ast.walk(function) if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]

    assert inner == []
    assert not [name for name in modules if name.startswith("datagrunt.core")]
    assert "pydantic" not in modules
