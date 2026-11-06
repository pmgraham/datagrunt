"""The Gemini wire adapter: the Gemini API and Google Cloud (Agent Platform), by API key or OAuth token (R3, T8).

:class:`GeminiProvider` sends one ``generateContent`` request per :class:`~datagrunt.rationalization.wire.ModelRequest`
through the bounded HTTP helper (:class:`~datagrunt.rationalization.providers.http.HttpClient`). The rules:

1. **Where it goes.** The Gemini API: ``POST https://generativelanguage.googleapis.com/v1beta/models/{model}:
   generateContent``, the key in the ``x-goog-api-key`` header. Google Cloud (:meth:`GeminiProvider.google_cloud`;
   Agent Platform, formerly Vertex AI): the shared :mod:`~datagrunt.rationalization.providers.google_cloud` builds
   everything: ``POST https://{host}/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:
   generateContent`` (``global`` on ``aiplatform.googleapis.com``, ``us`` and ``eu`` on
   ``aiplatform.{us|eu}.rep.googleapis.com``, any other region on ``{location}-aiplatform.googleapis.com``); the
   project and the location are required, since where documents go is never a default, and ``global`` gives no
   data-residency guarantee. Express mode (:meth:`GeminiProvider.google_cloud_express`: no project, an API key; Preview
   under the Pre-GA Offerings Terms, for ``@gmail.com`` accounts only, per Google's docs) is only ever asked for by
   name: ``POST https://aiplatform.googleapis.com/v1/publishers/google/models/{model}:generateContent``;
   :class:`~datagrunt.rationalization.providers.google_cloud.GoogleCloudAuth` sends the key as
   ``x-goog-api-key`` or a token as ``Authorization: Bearer``, and
   :func:`~datagrunt.rationalization.providers.google_cloud.check_access` refuses, at construction, a pair Google
   does not document (express mode with a token). Nothing else is ever contacted: every path segment and label is
   checked by the shared builders, and no redirect is followed.
2. **What it sends.** ``systemInstruction`` (the system prompt), one ``user`` turn (the envelope), and
   ``generationConfig``: ``responseMimeType`` ``application/json``, the request's schema under the API's schema
   field (rule 4), ``maxOutputTokens``, then ``temperature`` and ``seed`` unless they are ``None``. By default no
   temperature is sent (Google recommends keeping Gemini 3's default 1.0, Gemini 3.6 Flash and newer ignore it, and
   later models will refuse it with HTTP 400) and ``seed`` is 0.
3. **What it never sends or shows.** Keys and tokens travel only as
   :class:`~datagrunt.rationalization.providers.base._Secret` headers, never in a URL (no ``?key=``), a ``repr``, an
   error, a log or a traceback local: key text is checked without an exception and deleted before anything here
   raises, :meth:`GeminiProvider.from_model_string` takes the key out of its options and empties them first (a
   misspelt ``key=...`` included), and a key or token
   passed to :meth:`GeminiProvider.google_cloud` bare (not in a ``GoogleCloudAuth``) leaves its frame at once. The
   provider logs nothing and never pickles. A Google Cloud token source is called once per request, on the worker
   thread, after the request's deadline is set (``GoogleCloudAuth.headers``): it must be thread-safe and return
   promptly (a cached token). If it fails (raises, or returns anything but a printable-ASCII ``str`` without
   whitespace or a ``Bearer`` scheme of its own), the request fails with a
   :class:`~datagrunt.rationalization.providers.base.PermanentProviderError` that names the exception's class or the
   value's type, never a message or a value, so a misconfigured token source stops the run at once.
4. **The schema field, per API** (``generation_params["schema_field"]``). The Gemini API takes
   ``responseJsonSchema``, a JSON Schema subset; :meth:`GeminiProvider.transform_schema` keeps the documented
   keywords (``type``, ``title``, ``description``, ``properties``, ``required``, ``additionalProperties``,
   ``items``, ``enum``, ``format`` (date and time), ``minimum``, ``maximum``, ``minItems``, ``maxItems``, ``anyOf``,
   ``$ref``, ``$defs``) and turns ``const`` into a one-value ``enum``. On either API a ``$ref`` is a local
   ``#/$defs/<name>``, the name one RFC 6901 token (``~1`` for ``/``, ``~0`` for ``~``) read by
   :func:`~datagrunt.rationalization._refs.def_name` as every transform reads it, and a recursive schema, a ``$ref``
   cycle among the ``$defs`` entries through a ``properties`` value, ``items``, an ``anyOf`` member, a
   schema-valued ``additionalProperties`` or a ``$ref``'s siblings (and an unreferenced entry's), is refused at
   ``#/$defs`` before anything is converted, naming its path (``#/$defs/A -> #/$defs/B -> #/$defs/A``, ten hops at
   most), by the check every transform shares (:func:`~datagrunt.rationalization._refs.ref_cycle`, which reads at
   most :data:`SCHEMA_MAX_NODES` nodes of the entries and refuses more at ``#/$defs``; the other keywords holding
   subschemas are refused by name, below); ``schema.prepare()`` already refuses a recursive model, so this is defense
   in depth for a direct caller of the transform. Google Cloud takes ``responseSchema``, the field its
   structured-output guide documents, an OpenAPI subset: its ``Schema`` object has root-level ``ref`` and ``defs``,
   but the guide's list of supported fields names neither, so every local ``$ref`` is inlined (at most
   :data:`SCHEMA_MAX_NODES` nodes; the ``$defs`` are acyclic by then, so an entry used twice on one path, as a
   ``$ref`` beside a sibling naming the same entry, is inlined twice), ``anyOf [X, null]`` becomes ``X`` with
   ``nullable: true`` (the outer keywords, such as a field's ``description``, win over the inlined ones), types
   become ``STRING``, ``NUMBER``, ``INTEGER``, ``BOOLEAN``, ``ARRAY``, ``OBJECT`` or ``NULL``, an ``enum`` (or
   ``const``) stays only when all its values are strings (string enums only), ``format`` stays only on a string and
   only for date, time and duration, and only the documented keywords (``type``, ``description``, ``nullable``,
   ``properties``, ``required``, ``items``, ``enum``, ``format``, ``minimum``, ``maximum``, ``minItems``,
   ``maxItems``, ``anyOf``) are kept. On either API, an integer's ``exclusiveMinimum: n`` becomes
   ``minimum: floor(n) + 1`` and ``exclusiveMaximum: n`` becomes ``maximum: ceil(n) - 1`` (the stricter of it and a
   given inclusive bound), and every other constraint the field cannot hold (``pattern``, ``minLength``, ``maxLength``,
   a number's exclusive bounds, ``multipleOf``, ``uniqueItems``, ``minProperties``, ``maxProperties``, ``minContains``,
   ``maxContains``, any other ``format``, ``date-time`` included, and on Google Cloud an integer or boolean ``enum`` or
   ``const``, an int or bool ``Literal``, and ``additionalProperties: false``, which ``responseSchema`` cannot state) is
   removed and appended to the field's description as ``{enum: [0,1], pattern: "^P"}``
   (canonical JSON values, keys sorted; the Anthropic adapter's form), so the model still reads it and no paid answer
   is always quarantined for a rule the model never saw. ``date-time`` is only a hint because pydantic writes it for
   an aware and a naive datetime alike, and RFC 3339, which the grammar would enforce, requires the offset a
   ``NaiveDatetime`` refuses. Annotations (``title`` on Google Cloud, ``default``, ``examples``, ``deprecated``,
   ``x-...``) are dropped. A constraint hinted or dropped loosens only the wire: the local ``model_validate`` still
   enforces it, and a failure is quarantined. A keyword that holds subschemas the transform does not read
   (``oneOf``, ``allOf``, ``not``, ``if``, ``prefixItems``, ...) is refused by name rather than dropped, since
   dropping it would loosen the schema silently; so is nesting deeper than :data:`SCHEMA_MAX_DEPTH`. The transform is
   iterative and pure: a fresh structure on every call. Candidates for the T16 live tests: ``responseJsonSchema`` on
   Agent Platform (its ``GenerationConfig`` reference documents it; it would keep integer enums and ``$ref``), and
   ``responseFormat``, which that reference marks as the replacement for both schema fields.
5. **What it reads back** (one mapping for both APIs). ``promptFeedback.blockReason`` set: ``"refused"``. Else the
   first candidate's ``finishReason``, read with or without the ``FINISH_REASON_`` spelling: ``STOP`` is
   ``"complete"``, ``MAX_TOKENS`` and ``CONTINUATION`` (the output limit cut it) ``"truncated"``, the blocking
   reasons (:data:`REFUSED_REASONS`: ``SAFETY``, ``RECITATION``, ``LANGUAGE``, ``BLOCKLIST``,
   ``PROHIBITED_CONTENT``, ``SPII``, the ``IMAGE_*`` blocks, ``ESCALATION``, ``MODEL_ARMOR``) ``"refused"``, and
   anything else (``OTHER``, ``MALFORMED_FUNCTION_CALL``, an unknown or missing reason) ``"complete"`` with its
   content parsed, for validation to judge. ``PUP_LIMITED_DISABLED`` (:data:`ACCOUNT_REASONS`: the account is
   limited or disabled) is no answer at all: it raises a
   :class:`~datagrunt.rationalization.providers.base.PermanentProviderError`, which stops the run and is never
   cached. The text is the first candidate's text parts joined, thought parts (``thought: true``) left out.
   ``content`` is that text parsed strictly as one JSON object
   (:func:`~datagrunt.rationalization.providers.responses.parse_json_object`), and ``None`` for a refused answer;
   ``raw_text`` is that text, or the whole reply body when no text could be read (an unexpected shape, a blocked
   prompt). Tokens: ``promptTokenCount`` in, ``candidatesTokenCount`` plus ``thoughtsTokenCount`` out (thinking is
   billed as output); a count that is not an ``int`` in range is ignored. A model's output never raises; an HTTP
   failure propagates as the helper raises it: the statuses of its ``TRANSIENT_STATUSES`` (408, 429, 500, 502-504,
   520-524, 529) transient with ``Retry-After``, every other status permanent.
6. **Identity.** The Gemini API: ``model_id`` ``gemini:<model>``, ``provider_kind`` ``gemini``, ``rate_key`` its
   host, ``api`` ``v1beta``. Google Cloud: ``model_id`` ``google-cloud:`` + the model's resource name
   (:func:`~datagrunt.rationalization.providers.google_cloud.model_id_for`: project, location, publisher and model;
   express mode ``google-cloud:publishers/google/models/{model}``; the cache key holds no URL, so these are part of
   it), ``provider_kind`` ``google-cloud-gemini``, ``rate_key`` the model id itself (Google Cloud quotas are per
   model, so two models in one project never share a semaphore or a token bucket), ``api`` ``v1``. The credential is not
   part of the identity. Both: ``model_family`` ``google`` (:data:`MODEL_FAMILY`), ``max_connections`` as given, and
   ``generation_params`` ``{"api", "schema_field", "wire", "temperature", "seed"}``, ``None`` values left out,
   ``wire`` the format marker :data:`WIRE`.

Docs read on 2026-11-04 and 2026-11-05 (ai.google.dev answers with a 301 to aistudio.google.com, followed; the Google
Cloud hosts, auth and names are cited in :mod:`~datagrunt.rationalization.providers.google_cloud`):
https://ai.google.dev/api/generate-content, https://ai.google.dev/gemini-api/docs/structured-output,
https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/start/express-mode/overview (Pre-GA, @gmail.com),
https://aistudio.google.com/docs/whats-new-gemini-3.6 (temperature deprecated and ignored),
https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/capabilities/control-generated-output,
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/GenerationConfig,
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/Schema,
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/GenerateContentResponse (``MODEL_ARMOR``)
and https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference. The Gemini API reference
documents ``responseJsonSchema`` on ``v1beta``.

Bounds: the schema walk visits at most :data:`SCHEMA_MAX_NODES` nodes at most :data:`SCHEMA_MAX_DEPTH` deep, a
``$ref`` chain and an ``anyOf`` collapse each end within :data:`SCHEMA_MAX_DEPTH` steps, a reply is at most 8 MiB
(the helper's cap) and every other pass is linear in it. Nothing recurses but the JSON coder in
:func:`~datagrunt.rationalization._canonical.canonical_copy`. The module imports the standard library and the
package's own modules (the HTTP stack only through the helper): no pydantic, nothing from ``datagrunt.core``, and
no import inside a function (invariant 9).
"""

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final, Literal, NoReturn, TypeAlias, cast

from datagrunt.rationalization._canonical import canonical_copy, canonical_json
from datagrunt.rationalization._refs import (
    DEFS_POINTER,
    DEFS_PREFIX,
    UNREAD_KEYWORDS,
    NodeBoundError,
    cycle_text,
    def_name,
    json_pointer,
    ref_cycle,
)
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.base import (
    ModelString,
    PermanentProviderError,
    _Secret,
    _secret_problem,
    api_key_from_env,
    endpoint_model_id,
)
from datagrunt.rationalization.providers.google_cloud import API_VERSION as GOOGLE_CLOUD_API_VERSION
from datagrunt.rationalization.providers.google_cloud import (
    GoogleCloudAuth,
    GoogleCloudTarget,
    check_access,
    model_id_for,
)
from datagrunt.rationalization.providers.http import MAX_CONNECTIONS_LIMIT, HeaderValue, HttpClient, HttpReply
from datagrunt.rationalization.providers.responses import parse_json_object, response
from datagrunt.rationalization.wire import TOKEN_COUNT_MAX, FinishReason, ModelRequest, ProviderResponse

GEMINI_API_KEY_ENV = "GEMINI_API_KEY"
"""The variable a Gemini API key is read from when none is given."""

GEMINI_API_VERSION = "v1beta"
"""The Gemini API version: the one whose reference documents ``responseJsonSchema``."""

GEMINI_HOST = "generativelanguage.googleapis.com"

GOOGLE_CLOUD_KIND: Final = "google-cloud-gemini"
"""The ``provider_kind`` of Gemini on Google Cloud (Agent Platform): ``generateContent`` on a publisher model."""

GOOGLE_CLOUD_PUBLISHER = "google"
"""The publisher of Gemini models on Google Cloud."""

JSON_SCHEMA_FIELD = "responseJsonSchema"
"""The Gemini API's schema field: a JSON Schema subset (module notes, 4)."""

OPENAPI_SCHEMA_FIELD = "responseSchema"
"""Google Cloud's schema field: an OpenAPI 3.0 subset (module notes, 4)."""

TEMPERATURE_MAX = 2.0
SEED_MIN = -(2**31)
SEED_MAX = 2**31 - 1
"""A seed is an ``integer`` in Google's REST reference: 32 bits (64-bit fields are written as strings)."""

SCHEMA_MAX_NODES = 100_000
"""The most schema objects one transform builds (Google Cloud's ``responseSchema`` inlines every ``$ref``)."""

SCHEMA_MAX_DEPTH = 64
"""The deepest schema object a transform builds, the root at depth 1."""

TRUNCATED_REASONS = frozenset({"MAX_TOKENS", "CONTINUATION"})
"""The finish reasons of an answer the output limit cut."""

ACCOUNT_REASONS = frozenset({"PUP_LIMITED_DISABLED"})
"""The finish reasons of an account the provider limited or disabled: the run stops (a permanent error)."""

REFUSED_REASONS = frozenset(
    {
        "SAFETY",
        "RECITATION",
        "LANGUAGE",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "IMAGE_SAFETY",
        "IMAGE_PROHIBITED_CONTENT",
        "IMAGE_RECITATION",
        "ESCALATION",
        "MODEL_ARMOR",
    }
)
"""The finish reasons of an answer a filter or policy blocked."""

_FINISH_PREFIX = "FINISH_REASON_"  # Google Cloud's inference reference spells the reasons FINISH_REASON_STOP, ...
_JSON_TYPES = frozenset({"string", "number", "integer", "boolean", "object", "array", "null"})
_OPENAPI_TYPES: Mapping[str, str] = MappingProxyType({name: name.upper() for name in _JSON_TYPES})
# date-time is only hinted: pydantic writes it for an aware and a naive datetime alike, and RFC 3339 (which the
# grammar would enforce) requires the offset a NaiveDatetime refuses.
_JSON_FORMATS = frozenset({"date", "time"})
_OPENAPI_FORMATS = frozenset({"date", "time", "duration"})  # date-time is hinted, as on the Gemini API
# Copied as they are (a list copied): the documented keywords that hold no subschema.
_JSON_VALUES = frozenset({"title", "description", "required", "minimum", "maximum", "minItems", "maxItems", "enum"})
# Constraints responseJsonSchema does not take: removed, and appended to the description as a hint (rule 4).
_JSON_HINTED = frozenset(
    {
        "pattern",
        "minLength",
        "maxLength",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "uniqueItems",
        "minProperties",
        "maxProperties",
        "minContains",
        "maxContains",
    }
)
# An integer's exclusive bound and the inclusive one it means: x > n is x >= floor(n) + 1, x < n is x <= ceil(n) - 1.
_INTEGER_BOUNDS: tuple[tuple[str, str, Callable[[float], int], Callable[..., Any]], ...] = (
    ("exclusiveMinimum", "minimum", lambda bound: math.floor(bound) + 1, max),
    ("exclusiveMaximum", "maximum", lambda bound: math.ceil(bound) - 1, min),
)
_OPENAPI_VALUES = frozenset({"description", "required", "minimum", "maximum", "minItems", "maxItems"})

MODEL_FAMILY = "google"
"""The vendor lineage of every model this adapter reaches (Gemini, Gemma): Google's."""

WIRE = "generate-content/1"
"""The wire format marker in ``generation_params``: a change to what this adapter sends bumps it (a new cache key)."""

FROM_MODEL_STRING_OPTIONS = frozenset({"api_key", "temperature", "seed", "max_connections", "transport"})
"""The keywords :meth:`GeminiProvider.from_model_string` passes on."""

ApiKind: TypeAlias = Literal["gemini", "google-cloud-gemini"]


# ---------------------------------------------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------------------------------------------


def _check_temperature(value: object) -> float | None:
    """``None``, or a number (not a ``bool``) that is finite and in 0-2, held as a ``float``."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RationalizationError(f"temperature must be None or a number, got {type(value).__name__}")
    try:
        number = float(value)
    except OverflowError:
        number = math.inf
    if not 0.0 <= number <= TEMPERATURE_MAX:  # NaN and the infinities fail the comparison too
        raise RationalizationError(f"temperature must be finite and in 0-{TEMPERATURE_MAX:g}")
    return number


def _check_seed(value: object) -> int | None:
    """``None``, or an ``int`` (not a ``bool``) in the API's 32-bit range."""
    if value is None:
        return None
    if type(value) is not int:
        raise RationalizationError(f"seed must be None or an int, got {type(value).__name__}")
    if not SEED_MIN <= value <= SEED_MAX:
        raise RationalizationError(f"seed must be in {SEED_MIN}-{SEED_MAX} (a 32-bit integer)")
    return value


def _check_max_connections(value: object) -> int:
    if type(value) is not int:
        raise RationalizationError(f"max_connections must be an int, got {type(value).__name__}")
    if not 1 <= value <= MAX_CONNECTIONS_LIMIT:
        raise RationalizationError(f"max_connections must be in 1-{MAX_CONNECTIONS_LIMIT}, got {value}")
    return value


def _http_client(max_connections: object, transport: object) -> HttpClient:
    """The provider's HTTP helper; a bad pool size or a transport that is not an ``httpx.BaseTransport`` is refused."""
    count = _check_max_connections(max_connections)
    problem = ""
    try:  # the helper checks the transport's type at run time
        return HttpClient(max_connections=count, transport=cast(Any, transport))
    except TypeError as error:  # its message names the transport's type, never a value
        problem = str(error)
    raise RationalizationError(problem)


def _api_key(given: object) -> _Secret | str:
    """The Gemini API key as a ``_Secret``, from ``given`` or else the environment; or the problem, never the key.

    Nothing here raises while it holds key text: the text is checked without an exception and deleted first.

    Raises:
        RationalizationError: If the variable holds no valid key (``api_key_from_env``, naming the variable).
    """
    if isinstance(given, _Secret):
        return given
    if given is None:
        found = api_key_from_env(GEMINI_API_KEY_ENV)
        return f"no Gemini API key: set {GEMINI_API_KEY_ENV}, or pass api_key=..." if found is None else found
    kind = type(given).__name__
    key = _Secret(given) if type(given) is str and _secret_problem(given, what="an API key") is None else None
    del given  # this frame holds no key text from here on
    if key is not None:
        return key
    if kind != "str":
        return f"the Gemini API key must be a str, got {kind}"
    return "the Gemini API key must be printable ASCII without leading or trailing whitespace"


def _google_cloud_auth(auth: object) -> tuple[GoogleCloudAuth | None, str]:
    """The ``GoogleCloudAuth``, or ``None`` and the problem: never the value (a key passed bare is a secret)."""
    if isinstance(auth, GoogleCloudAuth):
        return auth, ""
    return None, (
        f"auth must be a GoogleCloudAuth (GoogleCloudAuth(api_key=...) or GoogleCloudAuth(token=...)), got "
        f"{type(auth).__name__}"
    )


# ---------------------------------------------------------------------------------------------------------------
# The endpoint
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Endpoint:
    """Where one provider's requests go, and the identity that names it (module notes, 1, 4, 6)."""

    kind: ApiKind
    model_id: str
    rate_key: str
    url: str
    api: str
    schema_field: str


def _gemini_endpoint(model: str) -> _Endpoint:
    model_id = endpoint_model_id("gemini", model, None)  # checks the model: a str, one path segment, no models/
    url = f"https://{GEMINI_HOST}/{GEMINI_API_VERSION}/models/{model}:generateContent"
    return _Endpoint("gemini", model_id, GEMINI_HOST, url, GEMINI_API_VERSION, JSON_SCHEMA_FIELD)


def _google_cloud_endpoint(model: str, target: GoogleCloudTarget) -> _Endpoint:
    """``generateContent`` on Google's publisher model at ``target`` (module notes, 1, 6), by the shared builders."""
    url = target.publisher_model_url(GOOGLE_CLOUD_PUBLISHER, model, "generateContent")
    model_id = model_id_for(target, GOOGLE_CLOUD_PUBLISHER, model)  # the rate key too: quotas are per model
    return _Endpoint(GOOGLE_CLOUD_KIND, model_id, model_id, url, GOOGLE_CLOUD_API_VERSION, OPENAPI_SCHEMA_FIELD)


# ---------------------------------------------------------------------------------------------------------------
# Schema transforms (module notes, 4)
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Task:
    """One schema object to convert into ``target``: where it is, and how deep."""

    source: Mapping[str, Any]
    target: dict[str, Any]
    pointer: str
    depth: int


_Convert: TypeAlias = Callable[[_Task, Mapping[str, Any]], list[_Task]]


def _schema_error(pointer: str, reason: str) -> RationalizationError:
    return RationalizationError(f"the response schema at {pointer} cannot be sent to Gemini: {reason}")


def _fresh_schema(schema: object) -> dict[str, Any]:
    """A private, exactly-JSON copy of ``schema``.

    Raises:
        TypeError: If ``schema`` is not a ``Mapping``.
        RationalizationError: If it is not exactly JSON.
    """
    if not isinstance(schema, Mapping):
        raise TypeError(f"schema must be a Mapping, got {type(schema).__name__}")
    copy: dict[str, Any] = canonical_copy(dict(schema), subject="the response schema")
    return copy


def _root_defs(root: Mapping[str, Any]) -> Mapping[str, Any]:
    defs = root.get("$defs", {})
    if not isinstance(defs, Mapping):
        raise _schema_error(DEFS_POINTER, "'$defs' must map names to schemas")
    return defs


def _check_acyclic(defs: Mapping[str, Any]) -> None:
    """Refuse a ``$ref`` cycle among the ``$defs`` entries, naming its path, on either API (module notes, 4)."""
    try:
        cycle = ref_cycle(defs, max_nodes=SCHEMA_MAX_NODES)
    except NodeBoundError as error:
        raise _schema_error(DEFS_POINTER, f"it has more than {SCHEMA_MAX_NODES} schema nodes") from error
    if cycle is not None:
        raise _schema_error(DEFS_POINTER, f"it is recursive (a $ref cycle: {cycle_text(cycle)})")


def _entry_name(ref: object, defs: Mapping[str, Any], pointer: str) -> str:
    """The ``$defs`` entry a local ``#/$defs/<name>`` reference names (RFC 6901); any other reference raises."""
    name = def_name(ref)
    if name is None:
        raise _schema_error(pointer, f"$ref must be a local {DEFS_PREFIX}<name> reference")
    if name not in defs:
        raise _schema_error(pointer, "$ref names no $defs entry")
    return name


def _walk(root: Mapping[str, Any], convert: _Convert) -> dict[str, Any]:
    """Convert ``root`` object by object with ``convert``, iteratively, under both bounds, once its ``$defs`` are
    known to hold no ``$ref`` cycle."""
    defs = _root_defs(root)
    _check_acyclic(defs)
    out: dict[str, Any] = {}
    stack = [_Task(root, out, "#", 1)]
    built = 0
    while stack:  # bounded: each task is a schema object counted against SCHEMA_MAX_NODES
        task = stack.pop()
        built += 1
        if built > SCHEMA_MAX_NODES:
            raise _schema_error("#", f"it expands past {SCHEMA_MAX_NODES} schema nodes")
        if task.depth > SCHEMA_MAX_DEPTH:
            raise _schema_error(task.pointer, f"it nests more than {SCHEMA_MAX_DEPTH} schema objects deep")
        stack.extend(convert(task, defs))
    return out


def _as_schema(value: object, pointer: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _schema_error(pointer, "a subschema must be a schema object")
    return value


def _child(task: _Task, source: object, pointer: str) -> tuple[dict[str, Any], _Task]:
    """A new empty target for the subschema ``source`` and the task that fills it."""
    target: dict[str, Any] = {}
    return target, _Task(_as_schema(source, pointer), target, pointer, task.depth + 1)


def _subschemas(task: _Task, source: Mapping[str, Any], keyword: str) -> list[_Task]:
    """The tasks for one container keyword: ``properties`` / ``$defs`` (named), ``items`` (one), ``anyOf`` (a list)."""
    value, base = source[keyword], f"{task.pointer}/{keyword}"
    if keyword in ("properties", "$defs"):
        if not isinstance(value, Mapping):
            raise _schema_error(base, f"'{keyword}' must map names to schemas")
        named: dict[str, Any] = {}
        tasks = []
        for name, child in value.items():
            named[name], made = _child(task, child, json_pointer(base, name))
            tasks.append(made)
        task.target[keyword] = named
        return tasks
    if keyword == "anyOf":
        if not isinstance(value, list):
            raise _schema_error(base, "'anyOf' must be a list of schemas")
        pairs = [_child(task, member, f"{base}/{index}") for index, member in enumerate(value)]
        task.target[keyword] = [target for target, _ in pairs]
        return [made for _, made in pairs]
    target, made = _child(task, value, base)
    task.target[keyword] = target
    return [made]


def _refuse_unread(source: Mapping[str, Any], pointer: str) -> None:
    for keyword in source:
        if keyword in UNREAD_KEYWORDS:  # dropping one would loosen the schema silently
            raise _schema_error(pointer, f"{keyword!r} holds subschemas the Gemini adapter cannot express")


def _check_type(kind: object, pointer: str) -> list[str]:
    """The JSON type names of a ``type`` value (one name or a list of them); anything else raises."""
    given = kind if isinstance(kind, list) else [kind]
    names = [name for name in given if isinstance(name, str) and name in _JSON_TYPES]
    if not names or len(names) != len(given):
        raise _schema_error(pointer, "'type' must be a JSON type name or a list of them")
    return names


def _json_node(task: _Task, defs: Mapping[str, Any]) -> list[_Task]:
    """Convert one object for ``responseJsonSchema``: the documented keywords kept, ``const`` an enum, an integer's
    exclusive bounds made inclusive, other constraints a description hint (rule 4)."""
    source, target = task.source, task.target
    _refuse_unread(source, task.pointer)
    hints: dict[str, Any] = {}
    tasks: list[_Task] = []
    for keyword, value in source.items():
        if keyword in _JSON_VALUES:
            target[keyword] = list(value) if isinstance(value, list) else value
        elif keyword == "type":
            names = _check_type(value, task.pointer)
            target[keyword] = names if isinstance(value, list) else value
        elif keyword == "format":
            (target if isinstance(value, str) and value in _JSON_FORMATS else hints)[keyword] = value
        elif keyword in _JSON_HINTED:
            hints[keyword] = value
        elif keyword == "$ref":
            _entry_name(value, defs, task.pointer)
            target[keyword] = value
        elif keyword == "additionalProperties" and isinstance(value, bool):
            target[keyword] = value
        elif keyword in ("properties", "$defs", "items", "anyOf", "additionalProperties"):
            tasks.extend(_subschemas(task, source, keyword))
    if "const" in source and "enum" not in source:
        target["enum"] = [source["const"]]
    _integer_bounds(target, hints, "integer")
    _add_hints(target, hints)
    return tasks


def _is_bound(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value)  # type: ignore[arg-type]  # an int or a float


def _integer_bounds(target: dict[str, Any], hints: dict[str, Any], integer: str) -> None:
    """On an integer-only schema (its type ``integer``, the dialect's name for it), turn each hinted exclusive bound
    into the inclusive bound it means."""
    kind = target.get("type")
    names = [
        name for name in (kind if isinstance(kind, list) else [kind]) if name != "null"
    ]  # responseSchema: nullable
    if names != [integer]:
        return
    for exclusive, inclusive, convert, stricter in _INTEGER_BOUNDS:
        if not _is_bound(hints.get(exclusive)):
            continue
        bound = convert(hints.pop(exclusive))
        given = target.get(inclusive)
        target[inclusive] = stricter(given, bound) if _is_bound(given) else bound


def _add_hints(target: dict[str, Any], hints: Mapping[str, Any]) -> None:
    """Append the removed constraints to the description, as ``{key: <canonical JSON>, ...}`` in key order (the form
    the Anthropic adapter writes), after a blank line when there is a description."""
    if not hints:
        return
    text = "{" + ", ".join(f"{key}: {canonical_json(hints[key])}" for key in sorted(hints)) + "}"
    description = target.get("description")
    target["description"] = f"{description}\n\n{text}" if isinstance(description, str) and description else text


def _merged(inner: Mapping[str, Any], outer: Mapping[str, Any], dropped: str) -> dict[str, Any]:
    """``inner`` with ``outer``'s keywords (but ``dropped``) laid over it: the outer ones win."""
    merged = dict(inner)
    merged.update((keyword, value) for keyword, value in outer.items() if keyword != dropped)
    return merged


def _is_null(member: object) -> bool:
    return isinstance(member, Mapping) and member.get("type") == "null"


def _flatten(task: _Task, defs: Mapping[str, Any]) -> tuple[Mapping[str, Any], bool]:
    """``task``'s object through ``$ref`` (inlined) and Optional's ``anyOf`` (collapsed); and whether null passes.

    The ``$defs`` hold no ``$ref`` cycle (``_walk`` refused one), so each ``$ref`` it inlines follows an edge of an
    acyclic graph: an entry used twice on one path (a ``$ref`` beside a sibling naming the same entry) is inlined
    twice, never forever; the loop's bound and the walk's ``SCHEMA_MAX_NODES`` and ``SCHEMA_MAX_DEPTH`` bound it
    anyway.
    """
    node, nullable = task.source, False
    for _ in range(SCHEMA_MAX_DEPTH):  # bounded: each step inlines one $defs entry or removes one anyOf level
        if "$ref" in node:
            name = _entry_name(node["$ref"], defs, task.pointer)
            node = _merged(_as_schema(defs[name], task.pointer), node, "$ref")
            continue
        members = node.get("anyOf")
        if not isinstance(members, list) or not any(_is_null(member) for member in members):
            return node, nullable
        others = [member for member in members if not _is_null(member)]
        nullable = True
        if len(others) == 1:
            node = _merged(_as_schema(others[0], task.pointer), node, "anyOf")
            continue
        rest = {keyword: value for keyword, value in node.items() if keyword != "anyOf"}
        return ({**rest, "anyOf": others} if others else {**rest, "type": "null"}), nullable
    raise _schema_error(task.pointer, f"its $ref and anyOf chain runs more than {SCHEMA_MAX_DEPTH} deep")


def _openapi_type(source: Mapping[str, Any], pointer: str) -> tuple[str | None, bool]:
    """The OpenAPI ``type`` of ``source`` (``None`` when it has none), and whether its type list admits null."""
    if "type" not in source:
        return None, False
    names = _check_type(source["type"], pointer)
    others = [name for name in names if name != "null"]
    if len(others) > 1:
        raise _schema_error(pointer, "a 'type' of several types has no responseSchema form")
    return _OPENAPI_TYPES[others[0] if others else "null"], len(others) < len(names) and bool(others)


def _openapi_enum(
    source: Mapping[str, Any], target: dict[str, Any], hints: dict[str, Any], kind: str | None
) -> str | None:
    """Keep a string ``enum`` (or ``const``) and return the type it needs; hint any other (Google Cloud supports string
    enums only), so the model still reads the allowed values."""
    keyword = "enum" if "enum" in source else "const" if "const" in source else None
    if keyword is None:
        return kind
    values = source[keyword] if keyword == "enum" else [source[keyword]]
    if isinstance(values, list) and values and all(isinstance(value, str) for value in values):
        target["enum"] = list(values)
        return kind or "STRING"
    hints[keyword] = source[keyword]
    return kind


def _openapi_format(source: Mapping[str, Any], target: dict[str, Any], hints: dict[str, Any], kind: str | None) -> None:
    """Keep a documented string ``format``; hint any other."""
    if "format" not in source:
        return
    form = source["format"]
    supported = kind == "STRING" and isinstance(form, str) and form in _OPENAPI_FORMATS
    (target if supported else hints)["format"] = form


def _openapi_node(task: _Task, defs: Mapping[str, Any]) -> list[_Task]:
    """Convert one object for ``responseSchema``: ``$ref`` inlined, Optional nullable, OpenAPI types, an integer's
    exclusive bounds made inclusive, the other constraints a description hint (rule 4)."""
    source, nullable = _flatten(task, defs)
    _refuse_unread(source, task.pointer)
    target = task.target
    hints = {keyword: value for keyword, value in source.items() if keyword in _JSON_HINTED}
    kind, listed_null = _openapi_type(source, task.pointer)
    kind = _openapi_enum(source, target, hints, kind)
    if kind is not None:
        target["type"] = kind
    _openapi_format(source, target, hints, kind)
    if nullable or listed_null or source.get("nullable") is True:
        target["nullable"] = True
    for keyword in _OPENAPI_VALUES.intersection(source):
        value = source[keyword]
        target[keyword] = list(value) if isinstance(value, list) else value
    if isinstance(source.get("additionalProperties"), Mapping):
        raise _schema_error(
            task.pointer, "a schema-valued additionalProperties (a dict field) has no responseSchema form"
        )
    if source.get("additionalProperties") is False:
        hints["additionalProperties"] = False  # responseSchema cannot close an object: the model reads it
    _integer_bounds(target, hints, "INTEGER")
    _add_hints(target, hints)
    tasks: list[_Task] = []
    for keyword in ("properties", "items", "anyOf"):
        if keyword in source:
            tasks.extend(_subschemas(task, source, keyword))
    return tasks


def _json_schema_for(field: str, schema: object) -> dict[str, Any]:
    root = _fresh_schema(schema)
    return _walk(root, _json_node if field == JSON_SCHEMA_FIELD else _openapi_node)


# ---------------------------------------------------------------------------------------------------------------
# Replies (module notes, 5)
# ---------------------------------------------------------------------------------------------------------------


def _reason(body: Mapping[str, Any]) -> str | None:
    """The first candidate's finish reason, without the ``FINISH_REASON_`` prefix, or ``None``."""
    candidates = body.get("candidates")
    first = candidates[0] if isinstance(candidates, list) and candidates else None
    reason = first.get("finishReason") if isinstance(first, Mapping) else None
    return reason.removeprefix(_FINISH_PREFIX) if isinstance(reason, str) else None


def _finish(body: Mapping[str, Any]) -> FinishReason:
    """``"refused"`` for a blocked prompt, else the first candidate's finish reason mapped."""
    feedback = body.get("promptFeedback")
    if isinstance(feedback, Mapping) and feedback.get("blockReason") is not None:
        return "refused"
    name = _reason(body)
    if name in TRUNCATED_REASONS:
        return "truncated"
    if name in REFUSED_REASONS:
        return "refused"
    return "complete"


def _candidate_text(body: Mapping[str, Any]) -> str | None:
    """The first candidate's text parts joined, thought parts left out; ``None`` when there is no such text."""
    candidates = body.get("candidates")
    if not isinstance(candidates, list) or not candidates or not isinstance(candidates[0], Mapping):
        return None
    content = candidates[0].get("content")
    parts = content.get("parts") if isinstance(content, Mapping) else None
    if not isinstance(parts, list):
        return None
    texts: list[str] = []
    for part in parts:
        if not isinstance(part, Mapping):
            return None
        if part.get("thought") is True or "text" not in part:
            continue
        if not isinstance(part["text"], str):
            return None
        texts.append(part["text"])
    return "".join(texts) if texts else None


def _count(value: object) -> int | None:
    return value if type(value) is int and 0 <= value <= TOKEN_COUNT_MAX else None


def _usage(metadata: object) -> tuple[int | None, int | None]:
    """The input and output token counts: the prompt's, and the candidates' plus the thoughts'; junk ignored."""
    if not isinstance(metadata, Mapping):
        return None, None
    outputs = [
        count
        for count in (_count(metadata.get("candidatesTokenCount")), _count(metadata.get("thoughtsTokenCount")))
        if count is not None
    ]
    return _count(metadata.get("promptTokenCount")), sum(outputs) if outputs else None


def _parse_reply(reply: HttpReply) -> ProviderResponse:
    """Build the response for a 200 ``generateContent`` reply (module notes, 5).

    Raises:
        PermanentProviderError: If the account is limited or disabled (:data:`ACCOUNT_REASONS`); a model's output
            never raises.
    """
    body = reply.json
    if body is None:
        return response(content=None, raw_text=reply.text, finish="complete", input_tokens=None, output_tokens=None)
    reason = _reason(body)
    if reason in ACCOUNT_REASONS:
        raise PermanentProviderError(None, f"the Gemini account is limited or disabled ({reason})")
    finish = _finish(body)
    text = _candidate_text(body)
    inputs, outputs = _usage(body.get("usageMetadata"))
    content = None if text is None or finish == "refused" else parse_json_object(text)
    return response(
        content=content,
        raw_text=reply.text if text is None else text,
        finish=finish,
        input_tokens=inputs,
        output_tokens=outputs,
    )


# ---------------------------------------------------------------------------------------------------------------
# GeminiProvider
# ---------------------------------------------------------------------------------------------------------------


class GeminiProvider:
    """Gemini through the Gemini API (an API key) or Google Cloud (:meth:`google_cloud`) (module notes).

    Args:
        model: The Gemini model (``gemini-2.5-flash``), one URL path segment, without a ``models/`` prefix.
        api_key: The Gemini API key; ``None`` reads :data:`GEMINI_API_KEY_ENV`.
        temperature: Sent when not ``None`` (the default, module notes 2): finite, 0-2. Gemini 3.6 Flash and newer
            ignore it, and Google says later models will refuse it with HTTP 400 (a permanent error).
        seed: Sent when not ``None``: a 32-bit ``int``.
        max_connections: The HTTP pool size, 1-1,024.
        transport: An ``httpx.BaseTransport`` instead of the network (tests); the helper checks its type.

    Raises:
        RationalizationError: If an argument breaks its rule (``transport`` included), or no key is given or set;
            never quoting a key.
    """

    __slots__ = ("_auth", "_client", "_endpoint", "_max_connections", "_seed", "_temperature")

    def __init__(
        self,
        model: str,
        *,
        api_key: "str | _Secret | None" = None,
        temperature: float | None = None,
        seed: int | None = 0,
        max_connections: int = 8,
        transport: object = None,
    ) -> None:
        key = _api_key(api_key)
        del api_key  # the raw key leaves this frame before anything here can raise (a traceback shows its locals)
        if isinstance(key, str):
            raise RationalizationError(key)
        self._configure(_gemini_endpoint(model), key, temperature, seed, max_connections, transport)

    @classmethod
    def google_cloud(
        cls,
        model: str,
        *,
        project: str,
        location: str,
        auth: GoogleCloudAuth,
        temperature: float | None = None,
        seed: int | None = 0,
        max_connections: int = 8,
        transport: object = None,
    ) -> "GeminiProvider":
        """Gemini on Google Cloud (Agent Platform, formerly Vertex AI) in a named project and location, with an API
        key or an OAuth token source (module notes, 1, 3, 6). Both are required: where the documents go is never a
        default. ``global`` gives no data-residency guarantee (Google may serve the request from any region); a
        region (``us-central1``) or a multi-region (``us``, ``eu``) keeps the processing there.

        Args:
            model: The publisher model (``gemini-2.5-pro``, or ``gemini-2.0-flash@001``), one URL path segment.
            project: The project ID, number or ``domain:project-id``. Express mode, which has none, is
                :meth:`google_cloud_express`.
            location: A region (``us-central1``), a multi-region (``us``, ``eu``) or ``global``.
            auth: ``GoogleCloudAuth(api_key=...)`` (the ``x-goog-api-key`` header) or ``GoogleCloudAuth(token=...)``
                (a callable returning the current OAuth access token, called once per request on a worker thread,
                after the request's deadline is set).
            temperature: Sent when not ``None`` (the default, module notes 2): finite, 0-2.
            seed: Sent when not ``None``: a 32-bit ``int``.
            max_connections: The HTTP pool size, 1-1,024.
            transport: An ``httpx.BaseTransport`` instead of the network (tests); the helper checks its type.

        Raises:
            TypeError: If ``project``, ``location`` or ``auth`` is not given (Python's own, naming the argument).
            RationalizationError: If ``auth`` is not a ``GoogleCloudAuth`` (never quoting it), ``project`` is
                ``None`` (naming :meth:`google_cloud_express`), the project, location or model breaks its rule, or
                another argument breaks its rule (``transport`` included).
        """
        credential, problem = _google_cloud_auth(auth)
        del auth  # a key or token passed bare leaves this frame before anything here can raise (a traceback's locals)
        if credential is None:
            raise RationalizationError(problem)
        if project is None:
            raise RationalizationError(
                "project is required: name the Google Cloud project and location (express mode, which has no "
                "project, is GeminiProvider.google_cloud_express(model, auth=...))"
            )
        target = GoogleCloudTarget(project, location)
        return cls._on_google_cloud(model, target, credential, temperature, seed, max_connections, transport)

    @classmethod
    def google_cloud_express(
        cls,
        model: str,
        *,
        auth: GoogleCloudAuth,
        temperature: float | None = None,
        seed: int | None = 0,
        max_connections: int = 8,
        transport: object = None,
    ) -> "GeminiProvider":
        """Gemini on Google Cloud in express mode: an API key, no project, the global host (module notes, 1, 6).

        Express mode is Preview: Google offers it under the Pre-GA Offerings Terms ("as is", with limited support),
        only to developers with a ``@gmail.com`` Google Account, for Gemini models only. It uses the ``global``
        endpoint, which gives no data-residency guarantee. Ask for it explicitly; :meth:`google_cloud` with a
        project and a location is the default for anything else.

        Args:
            model: The Gemini model (``gemini-2.5-flash``), one URL path segment.
            auth: ``GoogleCloudAuth(api_key=...)``: express mode takes an API key, never a token.
            temperature: Sent when not ``None`` (the default, module notes 2): finite, 0-2.
            seed: Sent when not ``None``: a 32-bit ``int``.
            max_connections: The HTTP pool size, 1-1,024.
            transport: An ``httpx.BaseTransport`` instead of the network (tests); the helper checks its type.

        Raises:
            TypeError: If ``auth`` is not given (Python's own).
            RationalizationError: If ``auth`` is not a ``GoogleCloudAuth`` holding an API key (never quoting it),
                the model breaks its rule, or another argument breaks its rule (``transport`` included).
        """
        credential, problem = _google_cloud_auth(auth)
        del auth  # a key or token passed bare leaves this frame before anything here can raise (a traceback's locals)
        if credential is None:
            raise RationalizationError(problem)
        target = GoogleCloudTarget.express()
        return cls._on_google_cloud(model, target, credential, temperature, seed, max_connections, transport)

    @classmethod
    def _on_google_cloud(
        cls,
        model: str,
        target: GoogleCloudTarget,
        credential: GoogleCloudAuth,
        temperature: object,
        seed: object,
        max_connections: object,
        transport: object,
    ) -> "GeminiProvider":
        """The provider of ``model`` at ``target``, once ``check_access`` allows ``credential`` there."""
        endpoint = _google_cloud_endpoint(model, target)
        check_access(target, credential, interface="generateContent")
        provider = cls.__new__(cls)
        provider._configure(endpoint, credential, temperature, seed, max_connections, transport)
        return provider

    @classmethod
    def from_model_string(cls, spec: ModelString, **options: Any) -> "GeminiProvider":
        """The provider of a parsed ``gemini:<model>`` string; ``options`` are the constructor's keywords
        (:data:`FROM_MODEL_STRING_OPTIONS`).

        The key is taken out of ``options`` and wrapped, and the other options sorted into known and unknown names
        with ``options`` emptied, before anything is checked: no frame that raises holds a key, a misspelt one
        (``key=...``) included.

        Raises:
            RationalizationError: If ``spec`` is not a ``ModelString`` of kind ``gemini`` without an endpoint, an
                option is unknown (named, never its value), or the constructor refuses.
        """
        given = options.pop("api_key", None)
        key = _api_key(given)
        del given  # this frame holds no key text from here on
        known = {name: value for name, value in options.items() if name in FROM_MODEL_STRING_OPTIONS}
        unknown = sorted(str(name) for name in options if name not in FROM_MODEL_STRING_OPTIONS)
        options.clear()  # an unknown option's value (a misspelt key=...) leaves this frame before anything can raise
        if not isinstance(spec, ModelString):
            raise RationalizationError(f"from_model_string takes a ModelString, got {type(spec).__name__}")
        if spec.kind != "gemini":
            raise RationalizationError("GeminiProvider takes only gemini:<model> strings")
        if spec.base_url is not None:
            raise RationalizationError("a gemini model string names no endpoint: the Gemini API's is fixed")
        if unknown:
            names = ", ".join(unknown)
            raise RationalizationError(
                f"unknown GeminiProvider option(s): {names[:200]}; the options are "
                f"{', '.join(sorted(FROM_MODEL_STRING_OPTIONS))}"
            )
        if isinstance(key, str):
            raise RationalizationError(key)
        return cls(spec.model, api_key=key, **known)

    def _configure(
        self,
        endpoint: _Endpoint,
        auth: _Secret | GoogleCloudAuth,
        temperature: object,
        seed: object,
        max_connections: object,
        transport: object,
    ) -> None:
        self._endpoint = endpoint
        self._auth = auth
        self._temperature = _check_temperature(temperature)
        self._seed = _check_seed(seed)
        self._client = _http_client(max_connections, transport)
        self._max_connections = _check_max_connections(max_connections)

    @property
    def model_id(self) -> str:
        """``gemini:<model>``, or ``google-cloud:`` + the model's resource name."""
        return self._endpoint.model_id

    @property
    def provider_kind(self) -> str:
        """``"gemini"`` or ``"google-cloud-gemini"``."""
        return self._endpoint.kind

    @property
    def model_family(self) -> str:
        """Always ``"google"`` (:data:`MODEL_FAMILY`): Gemini and Gemma are Google's models."""
        return MODEL_FAMILY

    @property
    def max_connections(self) -> int:
        """The HTTP pool size given (1-1,024)."""
        return self._max_connections

    @property
    def rate_key(self) -> str:
        """The Gemini API host; on Google Cloud the model id (its resource name), since quotas are per model."""
        return self._endpoint.rate_key

    @property
    def generation_params(self) -> dict[str, Any]:
        """A new mapping: the API version, the schema field, the wire marker, then temperature and seed unless
        ``None`` (``maxOutputTokens``, sent per request, is the runner's cache-key field)."""
        params: dict[str, Any] = {"api": self._endpoint.api, "schema_field": self._endpoint.schema_field, "wire": WIRE}
        if self._temperature is not None:
            params["temperature"] = self._temperature
        if self._seed is not None:
            params["seed"] = self._seed
        return params

    def transform_schema(self, schema: Mapping[str, Any]) -> dict[str, Any]:
        """The schema this provider sends for the neutral ``schema`` (module notes, 4); pure, fresh on each call.

        Raises:
            TypeError: If ``schema`` is not a ``Mapping``.
            RationalizationError: If it is not exactly JSON, or holds what the API's schema field cannot express.
        """
        return _json_schema_for(self._endpoint.schema_field, schema)

    def open(self) -> None:
        """Build the HTTP clients now, on the caller's thread (the runner calls it at preflight). Optional.

        Raises:
            RationalizationError: If the environment prevents a client (a proxy or certificate setting).
        """
        self._client.open()

    def generate(self, request: ModelRequest) -> ProviderResponse:
        """Send ``request`` and read the answer (module notes, 1-5).

        Raises:
            TypeError: If ``request`` is not a ``ModelRequest``.
            PermanentProviderError: If the Google Cloud token source fails, the account is limited or disabled, or
                on a permanent HTTP failure.
            TransientProviderError: On a retryable HTTP failure, a timeout or the deadline.
        """
        if not isinstance(request, ModelRequest):
            raise TypeError(f"generate needs a ModelRequest, got {type(request).__name__}")
        deadline = time.monotonic() + request.timeout_s
        reply = self._client.post_json(
            self._endpoint.url,
            headers=self._headers(),
            body=self._body(request),
            timeout_s=request.timeout_s,
            deadline=deadline,
        )
        return _parse_reply(reply)

    def _headers(self) -> dict[str, HeaderValue]:
        """The Gemini API key header, or Google Cloud's headers built now: its key, or a token fetched now (rule 3).

        Raises:
            PermanentProviderError: If the Google Cloud token source fails (``GoogleCloudAuth.headers``).
        """
        if isinstance(self._auth, _Secret):
            return {"x-goog-api-key": self._auth}
        return dict(self._auth.headers())

    def _body(self, request: ModelRequest) -> dict[str, Any]:
        config: dict[str, Any] = {
            "responseMimeType": "application/json",
            self._endpoint.schema_field: dict(request.json_schema),
            "maxOutputTokens": request.max_output_tokens,
        }
        if self._temperature is not None:
            config["temperature"] = self._temperature
        if self._seed is not None:
            config["seed"] = self._seed
        return {
            "systemInstruction": {"parts": [{"text": request.system_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": request.user_envelope}]}],
            "generationConfig": config,
        }

    def __reduce__(self) -> NoReturn:
        raise TypeError("a GeminiProvider never pickles: it holds an API key or a token source")

    def __reduce_ex__(self, protocol: object) -> NoReturn:
        raise TypeError("a GeminiProvider never pickles: it holds an API key or a token source")

    def __repr__(self) -> str:
        endpoint = getattr(self, "_endpoint", None)
        shown = endpoint.model_id if isinstance(endpoint, _Endpoint) else "?"
        return f"GeminiProvider(model_id={shown!r})"
