"""The Anthropic Messages API adapter: one structured-outputs request per unit (plan §4.5, R3).

**What is sent, and where.** :meth:`AnthropicProvider.generate` POSTs to ``{base_url}/v1/messages`` (default
``https://api.anthropic.com``) through the bounded HTTP helper
(:class:`~datagrunt.rationalization.providers.http.HttpClient`), with two headers: ``x-api-key`` (the key, as a
:class:`~datagrunt.rationalization.providers.base._Secret`) and ``anthropic-version``. The body holds ``model``,
``max_tokens`` (the request's ``max_output_tokens``), ``system`` (the system prompt), one ``user`` message (the
envelope: document text), ``output_config.format`` (``{"type": "json_schema", "schema": <the transformed schema>}``)
and, only when it is set (API version 2023-06-01), ``temperature``. Nothing else: no beta header, no tools, no
``tool_choice``, no ``thinking`` or effort setting (each model's default applies), and the key never goes in a URL,
a body, a ``repr``, an exception or a log. :meth:`AnthropicProvider.google_cloud` sends the same body, without
``model`` and with ``anthropic_version``, to Claude on Google Cloud (rule 7). The schema is what the caller's
pydantic model says (field names, descriptions, enum values, patterns): Anthropic caches compiled schemas for up to
24 hours, apart from prompts and responses, so a schema must hold no confidential data.

**Why structured outputs.** Anthropic's documentation, read on 2026-11-04 and re-checked on 2026-11-05, gives the
mechanism and the wire shapes:

- https://platform.claude.com/docs/en/build-with-claude/structured-outputs (status "stable", no beta header, "available
  on every model"; the request shape; the JSON Schema subset and its limits; the answer "in the response's text
  content block");
- https://platform.claude.com/docs/en/api/errors (forced tool use, ``tool_choice`` ``any`` or ``tool``, is a 400 on
  Claude Opus 5.5, Sonnet 5.5, Fable 5.1 and Mythos 5.1; the error body; 529 ``overloaded_error``);
- https://platform.claude.com/docs/en/api/messages/create (``stop_reason`` values, ``usage``, content blocks);
- https://platform.claude.com/docs/en/api/versioning and .../api/migration-2026-10-21 (the documented
  ``anthropic-version`` values are 2026-10-21 and 2023-06-01; "Anthropic recommends using the latest API version
  whenever possible"; 2026-10-21 removes ``temperature``, ``top_p`` and ``top_k``: sending one is a 400 on every
  model);
- https://platform.claude.com/docs/en/models/opus-5-5/migration-guide (current models reject sampling parameters).

(docs.claude.com redirects to platform.claude.com, Anthropic's documentation host.) Forced tool use, the long-stable
alternative, fails on the current flagship models, so this adapter uses ``output_config.format`` and never sends a
tool. Grammar-constrained decoding makes the answer match the schema it was sent in most cases (the documentation's
words; see the known limits); datagrunt still validates every record locally, which is the authority (R4).

**The rules.**

1. **Construction** checks everything before any request, and every rejection is a
   :class:`~datagrunt.rationalization.RationalizationError` naming the option (never a key): the model name (as
   :func:`~datagrunt.rationalization.providers.base.endpoint_model_id` checks it); the key, given, or read from
   ``ANTHROPIC_API_KEY`` only when the normalized ``base_url`` is Anthropic's own (:data:`DEFAULT_BASE_URL`, however
   written): any other endpoint, a gateway, needs an explicit ``api_key=``, so the caller's Anthropic key never
   reaches a third party unasked (a missing key names the variable, never a value); held as a ``_Secret``.
   ``base_url`` goes through :func:`~datagrunt.rationalization.providers.base.check_endpoint`; ``api_version`` is one
   of :data:`API_VERSIONS` (default the latest, 2026-10-21); ``temperature`` ``None`` (the default) or, with
   ``api_version="2023-06-01"`` only, a finite number in 0-1 (held as a float, ``-0.0`` as ``0.0``);
   ``max_connections`` 1-1,024; ``transport`` an httpx transport. ``from_model_string`` takes the constructor's
   keyword options and refuses any other, naming only the option names. A key passed as text never sits in a local
   of a frame that raises: it is wrapped (or its problem found) before any check and its text deleted, so a
   traceback printed with its locals shows no key.
2. **Identity.** ``model_id`` is ``anthropic:<model>`` for the default endpoint however written, else
   ``anthropic:<model>@<normalized base_url>``, so a gateway never shares a cache entry with Anthropic.
   ``rate_key`` is the normalized base URL; ``model_family`` is ``"anthropic"`` (Claude, whatever the endpoint).
   On Google Cloud the identity is rule 7's.
   ``generation_params`` is ``{"api_version", "mechanism": "json_schema", "temperature", "wire": "messages/1"}``,
   the temperature left out when it is ``None``; ``wire`` names this adapter's request and reply format, so a change
   to either re-keys the cache.
3. **The schema** (:meth:`AnthropicProvider.transform_schema`, pure: a fresh object each call). The documented
   subset: an object root; ``type``, ``properties``, ``required``, ``items``, ``anyOf``, ``enum`` and ``const`` of
   scalars, ``description`` and ``default`` are kept; every object gets ``additionalProperties: false`` (a ``true`` or
   a schema is refused); ``format`` is kept when it is one of :data:`ENFORCED_FORMATS` and ``minItems`` when it is 0
   or 1. The constraints the grammar does not take (``minimum``, ``maximum``, ``exclusiveMinimum``,
   ``exclusiveMaximum``, ``multipleOf``, ``minLength``, ``maxLength``, ``pattern``, ``maxItems``, ``uniqueItems``, an
   unsupported ``format``, a ``minItems`` past 1, ...) are removed and appended to the field's description as
   ``{maxLength: 2, minLength: 2}`` (canonical JSON values, sorted), as Anthropic's SDKs do: the model still sees
   them, and local validation enforces them. ``pattern`` is among them because the grammar refuses some regular
   expressions (lookaround, backreferences, large ``{n,m}``) with a 400 that would stop the run. ``date-time`` is
   among them too: pydantic gives an aware and a naive datetime the same ``format: date-time``, and RFC 3339 requires
   an offset, so enforcing it would force one onto a naive value that pydantic then rejects. Other annotations
   (``title``, ``examples``, ``deprecated``, ``x-...``) are dropped. A ``$ref`` must be a local ``#/$defs/<name>``
   naming a root ``$defs`` entry, the name one RFC 6901 token (``~1`` for ``/``, ``~0`` for ``~``), read by
   :func:`~datagrunt.rationalization._refs.def_name` as every transform reads it, and is kept alone: its siblings
   are dropped, as the SDKs drop them. Refused, naming the JSON pointer (RFC 6901 escaped): a keyword that combines
   or conditions schemas or holds subschemas the walk does not read (``oneOf``, ``allOf``, ``not``, ``if``,
   ``patternProperties``, ``prefixItems``, ``dependentRequired``, ``$id``, ``$anchor``, ...), ``$defs`` anywhere but
   the root, a node with no ``type``, ``$ref``, ``anyOf``, ``enum`` or ``const`` (``{}``, pydantic's ``Any``:
   nothing to enforce), a tuple ``items``, an enum or const holding an object or array, and a malformed keyword (a
   type list naming a type twice included). A recursive schema, a ``$ref`` cycle among the ``$defs`` entries (an
   unreferenced entry's included) through a ``properties`` value, ``items``, an ``anyOf`` member, a schema-valued
   ``additionalProperties`` or a ``$ref``'s siblings, is refused at ``#/$defs`` before anything is converted,
   naming its path (``#/$defs/A -> #/$defs/B -> #/$defs/A``, ten hops at most), by the check every transform
   shares (:func:`~datagrunt.rationalization._refs.ref_cycle`; the other keywords holding subschemas are refused
   above); ``schema.prepare()`` already refuses a recursive model, so this is defense in depth for a direct caller
   of the transform. The walk is iterative: at most :data:`SCHEMA_MAX_NODES` nodes, at most
   :data:`SCHEMA_MAX_DEPTH` levels deep; the cycle check reads at most :data:`SCHEMA_MAX_NODES` nodes of the
   ``$defs`` entries, a ``$ref``'s dropped siblings included, and refuses more at ``#/$defs``.
4. **The limits.** Structured outputs accept at most :data:`MAX_OPTIONAL_PARAMETERS` (24) optional parameters
   ("each parameter not listed in ``required``") and :data:`MAX_UNION_PARAMETERS` (16) union-typed ones ("parameters
   that use ``anyOf`` or type arrays") per request. :func:`parameter_counts` counts both once per definition: the
   root body and each ``$defs`` entry reachable from it, each property once (a ``$ref`` property is never a union).
   That is a lower bound of the API's own count, so a schema past either limit is refused before any request, naming
   both counts; one within them can still be refused by the API if it counts a definition each time it is used.
5. **The reply.** ``content`` is the parse of the ``text`` blocks joined in order (thinking blocks skipped) by
   :func:`~datagrunt.rationalization.providers.responses.parse_json_object`: exactly one JSON object, never repaired.
   ``stop_reason`` ``max_tokens``, ``model_context_window_exceeded`` and ``pause_turn`` (an unfinished turn) are
   ``"truncated"``, ``refusal`` is ``"refused"``, ``end_turn``, ``stop_sequence`` and ``tool_use`` are
   ``"complete"``. An unexpected reply (a body that is not a JSON object, ``content`` that is not a list of typed
   blocks, a text block without text, an unknown or missing ``stop_reason``) has ``content`` ``None``, for the runner
   to quarantine, and never raises. ``raw_text`` is the joined text, or the whole reply body when there is no usable
   text. ``input_tokens`` is ``usage.input_tokens`` plus the cache-creation and cache-read counts (the documented
   total), ``output_tokens`` is ``usage.output_tokens``; junk counts (a ``bool`` included) are ``None``, and a junk
   cache part counts 0. The response is built by
   :func:`~datagrunt.rationalization.providers.responses.response`, which escapes lone surrogates and bounds the text.
6. **Failures.** Every HTTP failure propagates as the HTTP helper raises it (401 permanent; 429, 529 and the other
   retryable statuses transient with their ``Retry-After``; ...). A request is bounded by
   ``deadline = time.monotonic() + request.timeout_s``, set before anything else the request does. A provider never
   pickles or copies (it holds a credential).
7. **Claude on Google Cloud, Google managed** (Vertex AI, now Agent Platform: :meth:`AnthropicProvider.google_cloud`).
   Anthropic also documents an Anthropic-managed Google Cloud offering on ``claude.googleapis.com``; it is not
   supported here. Google's pages, read on 2026-11-05, give the request and the reply:
   https://docs.cloud.google.com/vertex-ai/generative-ai/docs/partner-models/claude/use-claude and
   https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/claude/structured-outputs.
   Hosts, URLs, auth and identities come from :mod:`~datagrunt.rationalization.providers.google_cloud`, never built
   here:

   - **Where.** ``POST https://{host}/v1/projects/{p}/locations/{l}/publishers/anthropic/models/{model}:rawPredict``,
     the host the global one (``aiplatform.googleapis.com``, which gives no data residency), a multi-region's
     (``aiplatform.us.rep.googleapis.com``, ``aiplatform.eu.rep.googleapis.com``) or a region's
     (``us-east5-aiplatform.googleapis.com``). Newer Claude models need ``global``, ``us`` or ``eu``: Anthropic writes
     that "Regional endpoints serve Claude Sonnet 4.6 and earlier models only", while Google's Claude pages show
     regional hosts; a model a location does not serve is the API's 4xx, a permanent error.
   - **Auth.** A ``GoogleCloudAuth`` with a token source, checked by ``check_access(..., interface="rawPredict")``
     when the provider is built: Google documents no API key and no express mode for Claude, so both are refused
     then. The source is called once per request, on the worker, after the deadline is set; its header is
     ``Authorization: Bearer <token>``, and a source that fails ends the request with the auth's
     ``PermanentProviderError`` (its class named, never its message) before anything is sent. No ``x-api-key`` and
     no ``anthropic-version`` header is sent. The deadline bounds the HTTP exchange only: a token source that hangs
     cannot be cut by it, and holds its worker until it returns, so the source must return promptly (a cached
     credential refreshed in the background, not a blocking interactive login).
   - **Body.** Rule 1's Messages body without ``model`` (the URL names it) and with ``"anthropic_version":
     "vertex-2023-10-16"``; ``output_config.format`` json_schema, which Google documents for Claude 4.5 and later.
     Structured outputs are off by default on Google Cloud: the organization policy
     ``constraints/vertexai.allowedPartnerModelFeatures`` must allow ``structured_outputs``. Google does not name the
     error a refused feature returns; a 4xx stops the run as a permanent error. ``temperature`` is sent only when set;
     current Claude models refuse sampling parameters whatever the transport, so leave it ``None``.
   - **Reply.** The native Messages reply (``content`` blocks, the JSON in a ``text`` block, ``stop_reason``,
     ``usage``), read by rule 5 exactly as on the direct route: one transform, one limit check, one parser.
   - **Identity.** ``model_id`` is ``google-cloud:projects/{p}/locations/{l}/publishers/anthropic/models/{model}``,
     ``provider_kind`` ``"google-cloud-anthropic"`` (so no cache entry is shared with the direct ``"anthropic"``),
     ``rate_key`` the model id itself (Google Cloud quotas are per model, so two models in one project never share
     a semaphore or token bucket), ``model_family`` ``"anthropic"``, and ``generation_params``
     ``{"anthropic_version", "mechanism", "temperature", "wire"}`` (the temperature only when set).

**Known limits, from the same documentation.**

- The grammar holds the answer to the schema in most cases, not always: an enum value can come back in another
  capitalization ("Claude might return a value that differs from your schema only in capitalization"), a
  ``refusal`` or a ``max_tokens`` stop can end before the schema is met. Local validation quarantines any of these.
- Thinking tokens count toward ``max_tokens``, and some models always think, so a small ``max_output_tokens`` can
  end in ``"truncated"``.
- A schema the API cannot compile is a 400 ("Schema is too complex for compilation"), which stops the run at its
  first request, and compiling a new schema can take up to 180 seconds, longer than a short ``timeout_s``.
- Claude 4.7 and later models accept only the default sampling, so they answer a ``temperature`` with a 400 under
  either API version: set one only for an older model, with ``api_version="2023-06-01"``.
- On Google Cloud, a token source that hangs is not bounded by the request's deadline (rule 7): it must return
  promptly.

The module imports the standard library and the package's own modules (the HTTP stack only through
:mod:`~datagrunt.rationalization.providers.http`, which checks ``transport``): nothing from ``datagrunt.core``, and no
import inside a function (invariant 9).
"""

import math
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final, NoReturn, cast
from urllib.parse import urlsplit

from datagrunt.rationalization._canonical import canonical_copy, canonical_json
from datagrunt.rationalization._refs import (
    DEFS_POINTER,
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
    _Secret,
    _secret_problem,
    api_key_from_env,
    check_endpoint,
    endpoint_model_id,
)
from datagrunt.rationalization.providers.google_cloud import (
    ANTHROPIC_VERSION,
    GoogleCloudAuth,
    GoogleCloudTarget,
    check_access,
    model_id_for,
)
from datagrunt.rationalization.providers.http import MAX_CONNECTIONS_LIMIT, HeaderValue, HttpClient, HttpReply
from datagrunt.rationalization.providers.responses import parse_json_object, response
from datagrunt.rationalization.wire import FinishReason, ModelRequest, ProviderResponse

DEFAULT_BASE_URL = "https://api.anthropic.com"
"""Anthropic's endpoint (already normalized)."""

API_VERSIONS = ("2026-10-21", "2023-06-01")
"""The documented ``anthropic-version`` values, the latest first."""

DEFAULT_API_VERSION = API_VERSIONS[0]
"""The ``anthropic-version`` sent by default: the latest, as Anthropic recommends."""

SAMPLING_API_VERSION = "2023-06-01"
"""The one documented API version that still takes ``temperature`` (2026-10-21 refuses it on every model)."""

MODEL_FAMILY = "anthropic"
"""The vendor lineage of every model this adapter reaches: Claude."""

WIRE = "messages/1"
"""This adapter's request and reply format, in ``generation_params`` so that a change to it re-keys the cache."""

API_KEY_VARIABLE = "ANTHROPIC_API_KEY"
"""The environment variable the key is read from when none is given."""

MESSAGES_PATH = "/v1/messages"

GOOGLE_CLOUD_KIND = "google-cloud-anthropic"
"""The ``provider_kind`` of Claude on Google Cloud: its own cache-key kind, apart from the direct ``"anthropic"``."""

GOOGLE_CLOUD_PUBLISHER = "anthropic"
GOOGLE_CLOUD_METHOD: Final = "rawPredict"
"""The Google Cloud method Claude is called through (its streaming twin is never used)."""

MECHANISM = "json_schema"
"""How the answer's shape is enforced: ``output_config.format`` of type ``json_schema`` (structured outputs)."""

SUPPORTED_FORMATS = frozenset(
    {"date-time", "time", "date", "duration", "email", "hostname", "uri", "ipv4", "ipv6", "uuid"}
)
"""The string formats structured outputs enforce, as documented."""

ENFORCED_FORMATS = SUPPORTED_FORMATS - {"date-time"}
"""The formats the transform keeps (module notes, 3): ``date-time`` becomes a hint, since a naive datetime has none."""

MAX_OPTIONAL_PARAMETERS = 24
"""The most optional parameters (properties not in ``required``) structured outputs accept per request."""

MAX_UNION_PARAMETERS = 16
"""The most union-typed parameters (``anyOf`` or a type array) structured outputs accept per request."""

SCHEMA_MAX_NODES = 100_000
"""The most schema nodes :meth:`AnthropicProvider.transform_schema` visits."""

SCHEMA_MAX_DEPTH = 64
"""The deepest a schema node may sit (the root is level 1)."""

_FINISH_BY_STOP_REASON: Mapping[str, FinishReason] = MappingProxyType(
    {
        "end_turn": "complete",
        "stop_sequence": "complete",
        "tool_use": "complete",
        "max_tokens": "truncated",
        "model_context_window_exceeded": "truncated",
        "pause_turn": "truncated",
        "refusal": "refused",
    }
)
_CACHE_TOKEN_FIELDS = ("cache_creation_input_tokens", "cache_read_input_tokens")
_OPTIONS = frozenset({"base_url", "api_version", "temperature", "allow_insecure_http", "max_connections", "transport"})
_TYPED_KEYWORDS = ("type", "anyOf", "enum", "const")
_OPTIONS_SHOWN = 10
_JSON_TYPES = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
# Constraints the grammar does not take: removed, and appended to the description as a hint.
_HINTED = frozenset(
    {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "maxItems",
        "uniqueItems",
        "minProperties",
        "maxProperties",
        "minContains",
        "maxContains",
    }
)
# Keywords that combine, condition or hold schemas the walk does not read, or change what a $ref resolves to; and
# dependentRequired, which the grammar does not take.
_REFUSED = UNREAD_KEYWORDS | frozenset({"dependentRequired"})


# ---------------------------------------------------------------------------------------------------------------
# Construction checks
# ---------------------------------------------------------------------------------------------------------------


def _refuse(message: str) -> NoReturn:
    raise RationalizationError(f"AnthropicProvider: {message}")


def _wrap_key(value: object) -> tuple[_Secret | None, str | None]:
    """A given key as a ``_Secret`` (``None`` when none is given), or the problem with it; it reads no environment
    variable and never raises, so no frame holding the key is on a traceback (rule 1)."""
    if isinstance(value, _Secret):
        return value, None
    if value is None:
        return None, None
    if not isinstance(value, str):
        return None, f"api_key must be a str, got {type(value).__name__}"
    if _secret_problem(value, what="api_key") is not None:  # checked without raising, so no frame holds the text
        return None, "api_key must be non-blank printable ASCII without leading or trailing whitespace"
    return _Secret(value), None


def _env_key(base: str) -> tuple[_Secret | None, str]:
    """The ``ANTHROPIC_API_KEY`` key for the normalized endpoint ``base``, or the problem (rule 1).

    The variable is read only for :data:`DEFAULT_BASE_URL`: any other endpoint (a gateway) needs an explicit
    ``api_key=``, so the caller's Anthropic key never reaches a third party unasked. Never raises, never quotes a key.
    """
    if base != DEFAULT_BASE_URL:
        host = urlsplit(base).netloc
        return None, f"{API_KEY_VARIABLE} is read only for {DEFAULT_BASE_URL}; pass api_key= for {host} explicitly"
    try:
        found = api_key_from_env(API_KEY_VARIABLE)
    except RationalizationError as error:  # its message names the variable, never the value
        return None, str(error)
    if found is None:
        return None, f"no API key: set {API_KEY_VARIABLE} or pass api_key="
    return found, ""


def _check_temperature(value: object) -> float | None:
    """``None``, or a finite number in 0-1 as a float (``-0.0`` becomes ``0.0``)."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _refuse(f"temperature must be None or a number in 0-1, got {type(value).__name__}")
    try:
        number = float(value)
    except OverflowError:
        number = math.inf
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        _refuse("temperature must be None or a finite number in 0-1")
    return number + 0.0


def _check_api_version(value: object, temperature: float | None) -> str:
    """One of :data:`API_VERSIONS`; with a temperature, only :data:`SAMPLING_API_VERSION`."""
    if type(value) is not str or value not in API_VERSIONS:
        _refuse(f"api_version must be one of the documented anthropic-version values: {', '.join(API_VERSIONS)}")
    if temperature is not None and value != SAMPLING_API_VERSION:
        _refuse(
            f"API version {value} removed temperature (a 400 on every model): pass temperature=None, or "
            f'api_version="{SAMPLING_API_VERSION}" for an older model that still takes one'
        )
    return value


def _check_max_connections(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_CONNECTIONS_LIMIT:
        _refuse(f"max_connections must be an int in 1-{MAX_CONNECTIONS_LIMIT}")
    return value


def _http_client(max_connections: int, allow_insecure_http: bool, transport: object) -> HttpClient:
    """The provider's HTTP helper; a transport that is not an ``httpx.BaseTransport`` is refused naming it."""
    problem = ""
    try:  # the helper checks the transport's type at run time
        return HttpClient(
            max_connections=max_connections, allow_insecure_http=allow_insecure_http, transport=cast(Any, transport)
        )
    except TypeError as error:  # its message names the transport's type, never a key
        problem = str(error)
    _refuse(problem)


# ---------------------------------------------------------------------------------------------------------------
# The schema transform
# ---------------------------------------------------------------------------------------------------------------


def _refuse_at(pointer: str, message: str) -> NoReturn:
    raise RationalizationError(f"the response schema at {pointer} cannot go to Anthropic structured outputs: {message}")


def _is_scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


@dataclass(frozen=True, slots=True)
class _Node:
    """A schema node still to convert: its source, its (empty) copy, where it is, and how deep."""

    source: object
    target: dict[str, Any] = field(repr=False)
    pointer: str
    depth: int


class _Walk:
    """One transform: an iterative walk over the root body and each ``$defs`` entry (module notes, 3)."""

    def __init__(self, definitions: Mapping[str, Any]) -> None:
        self.definitions = definitions
        self.stack: list[_Node] = []
        self.visited = 0

    def run(self) -> None:
        while self.stack:  # bounded: every node is counted, and the count stops at SCHEMA_MAX_NODES
            node = self.stack.pop()
            self.visited += 1
            if self.visited > SCHEMA_MAX_NODES:
                _refuse_at("#", f"it has more than {SCHEMA_MAX_NODES:,} schema nodes")
            if node.depth > SCHEMA_MAX_DEPTH:
                _refuse_at(node.pointer, f"it nests more than {SCHEMA_MAX_DEPTH} levels")
            self.convert(node)

    def push(self, source: object, pointer: str, parent: _Node) -> dict[str, Any]:
        target: dict[str, Any] = {}
        self.stack.append(_Node(source, target, pointer, parent.depth + 1))
        return target

    def convert(self, node: _Node) -> None:
        source, pointer = node.source, node.pointer
        if not isinstance(source, dict):
            _refuse_at(pointer, "a schema must be a JSON object")
        if "$ref" in source:
            self.convert_ref(node, source["$ref"])
            return
        refused = sorted(keyword for keyword in source if keyword in _REFUSED)
        if refused or ("$defs" in source and pointer != "#"):
            _refuse_at(pointer, f"{(refused or ['$defs'])[0]} is not supported (only a root $defs is)")
        if not any(keyword in source for keyword in _TYPED_KEYWORDS):
            _refuse_at(
                pointer,
                "it has no type, $ref, anyOf, enum or const: an unconstrained schema (such as pydantic's Any) cannot "
                "be enforced",
            )
        hints = {keyword: value for keyword, value in source.items() if keyword in _HINTED}
        self.copy_values(node, source)
        self.copy_children(node, source)
        self.copy_limits(node, source, hints)
        _add_hints(node.target, hints)

    def convert_ref(self, node: _Node, ref: object) -> None:
        """A ``$ref`` is kept alone (its siblings dropped), and must name a root ``$defs`` entry."""
        name = def_name(ref)
        if name is None:
            _refuse_at(
                node.pointer, "a $ref must be a local #/$defs/<name> reference (an external $ref is not supported)"
            )
        if name not in self.definitions:
            _refuse_at(node.pointer, f"the $ref names no $defs entry {name}")
        node.target["$ref"] = ref

    def copy_values(self, node: _Node, source: Mapping[str, Any]) -> None:
        """``type``, ``enum``, ``const``, ``description`` and ``default``, each checked."""
        target, pointer = node.target, node.pointer
        if "type" in source:
            target["type"] = _checked_type(source["type"], pointer)
        if "enum" in source:
            values = source["enum"]
            if not isinstance(values, list) or not values or not all(_is_scalar(value) for value in values):
                _refuse_at(pointer, "an enum must be a non-empty list of strings, numbers, booleans or nulls")
            target["enum"] = values
        if "const" in source:
            if not _is_scalar(source["const"]):
                _refuse_at(pointer, "a const must be a string, number, boolean or null")
            target["const"] = source["const"]
        if "description" in source:
            if not isinstance(source["description"], str):
                _refuse_at(pointer, "a description must be a string")
            target["description"] = source["description"]
        if "default" in source:
            target["default"] = source["default"]

    def copy_children(self, node: _Node, source: Mapping[str, Any]) -> None:
        """``properties``, ``items`` and ``anyOf``: each child queued to convert."""
        target, pointer = node.target, node.pointer
        if "properties" in source:
            properties = source["properties"]
            if not isinstance(properties, dict):
                _refuse_at(pointer, "properties must be an object")
            target["properties"] = {
                name: self.push(child, json_pointer(f"{pointer}/properties", name), node)
                for name, child in properties.items()
            }
        if "items" in source:
            if not isinstance(source["items"], dict):
                _refuse_at(pointer, "items must be one schema (tuple items are not supported)")
            target["items"] = self.push(source["items"], f"{pointer}/items", node)
        if "anyOf" in source:
            members = source["anyOf"]
            if not isinstance(members, list) or not members:
                _refuse_at(pointer, "anyOf must be a non-empty list of schemas")
            target["anyOf"] = [self.push(member, f"{pointer}/anyOf/{i}", node) for i, member in enumerate(members)]

    def copy_limits(self, node: _Node, source: Mapping[str, Any], hints: dict[str, Any]) -> None:
        """``required``, ``additionalProperties`` (``false`` on every object), ``format`` and ``minItems``."""
        target, pointer = node.target, node.pointer
        if "required" in source:
            required = source["required"]
            if not isinstance(required, list) or not all(isinstance(name, str) for name in required):
                _refuse_at(pointer, "required must be a list of property names")
            target["required"] = required
        if source.get("additionalProperties", False) is not False:
            _refuse_at(pointer, "additionalProperties must be false (open objects are not supported)")
        kinds = target.get("type")
        if kinds == "object" or (isinstance(kinds, list) and "object" in kinds) or "properties" in target:
            target["additionalProperties"] = False
        if "format" in source:
            supported = isinstance(source["format"], str) and source["format"] in ENFORCED_FORMATS
            (target if supported else hints)["format"] = source["format"]
        if "minItems" in source:
            kept = type(source["minItems"]) is int and source["minItems"] in (0, 1)
            (target if kept else hints)["minItems"] = source["minItems"]


def _checked_type(value: object, pointer: str) -> object:
    """A JSON type name, or a non-empty list of distinct ones."""
    if isinstance(value, str) and value in _JSON_TYPES:
        return value
    if (
        isinstance(value, list)
        and value
        and all(isinstance(kind, str) and kind in _JSON_TYPES for kind in value)
        and len(set(value)) == len(value)
    ):
        return value
    _refuse_at(pointer, "type must be a JSON type name or a list of distinct ones")


def _add_hints(target: dict[str, Any], hints: Mapping[str, Any]) -> None:
    """Append the removed constraints to the description, as ``{key: <canonical JSON>, ...}`` in key order."""
    if not hints:
        return
    text = "{" + ", ".join(f"{key}: {canonical_json(hints[key])}" for key in sorted(hints)) + "}"
    description = target.get("description")
    target["description"] = f"{description}\n\n{text}" if description else text


def _check_acyclic(definitions: Mapping[str, Any]) -> None:
    """Refuse a ``$ref`` cycle among the ``$defs`` entries, naming its path (module notes, 3): structured outputs
    refuse recursive schemas."""
    try:
        cycle = ref_cycle(definitions, max_nodes=SCHEMA_MAX_NODES)
    except NodeBoundError:
        _refuse_at(DEFS_POINTER, f"it has more than {SCHEMA_MAX_NODES:,} schema nodes")
    if cycle is not None:
        _refuse_at(DEFS_POINTER, f"it is recursive (a $ref cycle: {cycle_text(cycle)})")


def _is_union(node: object) -> bool:
    """Whether a property uses ``anyOf`` or a type array (a ``$ref`` property never does)."""
    return isinstance(node, Mapping) and "$ref" not in node and ("anyOf" in node or isinstance(node.get("type"), list))


def _count_body(body: object) -> tuple[set[str], int, int]:
    """The ``$defs`` names one definition's body refers to, and its optional and union-typed properties.

    Iterative; ``$ref`` is not followed and ``$defs`` not entered, so each node of the body is visited once.
    """
    refs: set[str] = set()
    optional = union = 0
    stack: list[object] = [body]
    while stack:  # bounded: every node of the body is pushed once
        node = stack.pop()
        if not isinstance(node, Mapping):
            continue
        ref = node.get("$ref")
        if isinstance(ref, str):
            name = def_name(ref)
            if name is not None:
                refs.add(name)
            continue
        properties = node.get("properties")
        if isinstance(properties, Mapping):
            listed = node.get("required")
            required = {name for name in listed if isinstance(name, str)} if isinstance(listed, list) else set()
            optional += sum(1 for name in properties if name not in required)
            union += sum(1 for child in properties.values() if _is_union(child))
            stack.extend(properties.values())
        if isinstance(node.get("items"), Mapping):
            stack.append(node["items"])
        if isinstance(node.get("anyOf"), list):
            stack.extend(node["anyOf"])
    return refs, optional, union


def parameter_counts(schema: Mapping[str, Any]) -> tuple[int, int]:
    """The optional and union-typed parameters of ``schema``, counted once per definition (module notes, 4).

    The root body and each ``$defs`` entry reachable from it, each property once: optional when it is not in its
    object's ``required``, union-typed when it uses ``anyOf`` or a type array. A lower bound of the API's count.

    Returns:
        ``(optional, union)``.
    """
    found = schema.get("$defs")
    definitions: Mapping[str, Any] = found if isinstance(found, Mapping) else {}
    optional = union = 0
    queued: set[str] = set()
    bodies: list[object] = [schema]
    while bodies:  # bounded: the root and each definition are counted once
        refs, body_optional, body_union = _count_body(bodies.pop())
        optional += body_optional
        union += body_union
        for name in sorted(refs - queued):
            if name in definitions:
                queued.add(name)
                bodies.append(definitions[name])
    return optional, union


def _check_limits(optional: int, union: int) -> None:
    """Refuse a schema past the documented optional or union limit (module notes, 4), naming both counts."""
    if optional <= MAX_OPTIONAL_PARAMETERS and union <= MAX_UNION_PARAMETERS:
        return
    raise RationalizationError(
        f"the response schema has {optional} optional parameters (properties not in required; at most "
        f"{MAX_OPTIONAL_PARAMETERS}) and {union} union-typed ones (anyOf or a type array; at most "
        f"{MAX_UNION_PARAMETERS}), counted once per definition: Anthropic structured outputs refuse more per request; "
        "require more fields, or use fewer Optional, Decimal or union-typed ones"
    )


def transform_schema(schema: Mapping[str, Any]) -> dict[str, Any]:
    """Return the schema structured outputs accept for the neutral ``schema`` (module notes, 3): a fresh object.

    Raises:
        TypeError: If ``schema`` is not a ``Mapping``.
        RationalizationError: If it is not exactly JSON, its root is not an object schema, it uses a shape the
            subset refuses, it is recursive, it passes a bound (the message names the JSON pointer), or it passes the
            optional or union limit (the message names both counts).
    """
    if not isinstance(schema, Mapping):
        raise TypeError(f"schema must be a Mapping, got {type(schema).__name__}")
    document = canonical_copy(dict(schema), subject="the response schema")
    if document.get("type") != "object":
        _refuse_at("#", "the root must be an object schema (type object)")
    definitions = document.get("$defs", {})
    if not isinstance(definitions, dict):
        _refuse_at(DEFS_POINTER, "$defs must be an object")
    _check_acyclic(definitions)
    walk = _Walk(definitions)
    result: dict[str, Any] = {}
    walk.stack.append(_Node(document, result, "#", 1))
    if definitions:
        result["$defs"] = {name: {} for name in definitions}
        for name, definition in definitions.items():
            walk.stack.append(_Node(definition, result["$defs"][name], json_pointer(DEFS_POINTER, name), 1))
    walk.run()
    _check_limits(*parameter_counts(result))
    copy: dict[str, Any] = canonical_copy(result, subject="the transformed response schema")
    return copy


# ---------------------------------------------------------------------------------------------------------------
# The reply
# ---------------------------------------------------------------------------------------------------------------


def _count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _input_tokens(usage: object) -> int | None:
    """``input_tokens`` plus the cache-creation and cache-read counts; ``None`` when ``input_tokens`` is junk."""
    if not isinstance(usage, Mapping):
        return None
    base = _count(usage.get("input_tokens"))
    if base is None:
        return None
    return base + sum(_count(usage.get(name)) or 0 for name in _CACHE_TOKEN_FIELDS)


def _answer_text(message: Mapping[str, Any]) -> str | None:
    """The ``text`` blocks joined in order; ``None`` when ``content`` is malformed or holds no text block."""
    blocks = message.get("content")
    if not isinstance(blocks, list):
        return None
    texts: list[str] = []
    for block in blocks:
        if not isinstance(block, Mapping) or not isinstance(block.get("type"), str):
            return None
        if block["type"] != "text":
            continue
        if not isinstance(block.get("text"), str):
            return None
        texts.append(block["text"])
    return "".join(texts) if texts else None


def _parse_reply(reply: HttpReply) -> ProviderResponse:
    """The response for a 2xx reply (module notes, 4); never raises for what the model or the server sent."""
    message = reply.json
    if message is None:
        return response(content=None, raw_text=reply.text, finish="complete", input_tokens=None, output_tokens=None)
    usage = message.get("usage")
    output_tokens = usage.get("output_tokens") if isinstance(usage, Mapping) else None
    stop_reason = message.get("stop_reason")
    finish = _FINISH_BY_STOP_REASON.get(stop_reason) if isinstance(stop_reason, str) else None
    text = _answer_text(message)
    content = parse_json_object(text) if text is not None and finish is not None else None
    return response(
        content=content,
        raw_text=reply.text if text is None else text,
        finish=finish or "complete",
        input_tokens=_input_tokens(usage),
        output_tokens=output_tokens,
    )


# ---------------------------------------------------------------------------------------------------------------
# AnthropicProvider
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Route:
    """Where one provider sends and who it is: the direct Messages API or Claude on Google Cloud (module notes, 2, 7).

    ``body_fields`` join the shared Messages body (``model`` directly; ``anthropic_version`` on Google Cloud),
    ``params`` join the shared ``generation_params``, and ``plain_headers`` join the credential's header.
    """

    url: str
    provider_kind: str
    model_id: str
    rate_key: str
    body_fields: Mapping[str, str]
    params: Mapping[str, str]
    plain_headers: Mapping[str, str]
    credential: _Secret | GoogleCloudAuth = field(repr=False)

    def headers(self) -> dict[str, HeaderValue]:
        """A new mapping of one request's headers: the key, or the Bearer token fetched now (on the worker).

        Raises:
            PermanentProviderError: If the Google Cloud token source fails (naming its class, never its message).
        """
        sent: dict[str, HeaderValue] = dict(self.plain_headers)
        if isinstance(self.credential, GoogleCloudAuth):
            sent.update(self.credential.headers())
        else:
            sent["x-api-key"] = self.credential
        return sent


def _auth_of(auth: object) -> tuple[GoogleCloudAuth | None, str]:
    """``auth`` when it is a :class:`GoogleCloudAuth`, else ``None`` and the problem; it never raises, so the caller
    deletes whatever was passed (a key by mistake) before it raises."""
    if isinstance(auth, GoogleCloudAuth):
        return auth, ""
    kind = type(auth).__name__
    return None, f"auth must be a GoogleCloudAuth, got {kind}: GoogleCloudAuth(token=...) with an OAuth token source"


class AnthropicProvider:
    """Claude through the Anthropic Messages API, with structured outputs (module notes).

    Args:
        model: The model name (``"claude-opus-5-5"``).
        api_key: The API key; ``None`` reads ``ANTHROPIC_API_KEY``, for Anthropic's own endpoint only (a gateway
            needs an explicit key).
        base_url: The endpoint: Anthropic's, or a gateway that speaks the Messages API.
        api_version: The ``anthropic-version`` header: one of :data:`API_VERSIONS`, the latest by default.
        temperature: ``None`` (the default) leaves it out. A number in 0-1 needs ``api_version="2023-06-01"``, and
            only an older model takes it (Claude 4.7 and later models refuse it).
        allow_insecure_http: Allow ``http`` to a host that is not loopback: documents and the API key then travel
            unencrypted, to the host and to any proxy on the way.
        max_connections: The HTTP connection pool size, 1-1,024.
        transport: An ``httpx.BaseTransport`` to use instead of the network (tests), checked by the HTTP helper.

    Raises:
        RationalizationError: If an option breaks its rule (``transport`` included), no key is found, or a gateway
            has no explicit key; the message names the option or the variable and never shows a key.
    """

    __slots__ = ("_http", "_max_concurrency", "_max_connections", "_model", "_route", "_temperature")

    def __init__(
        self,
        model: str,
        *,
        api_key: str | _Secret | None = None,
        base_url: str = DEFAULT_BASE_URL,
        api_version: str = DEFAULT_API_VERSION,
        temperature: float | None = None,
        allow_insecure_http: bool = False,
        max_connections: int = 8,
        transport: object = None,
    ) -> None:
        secret, problem = _wrap_key(api_key)
        del api_key  # a key given as text never sits in this frame's locals past this line
        if problem is not None:
            _refuse(problem)
        base = check_endpoint(base_url, allow_insecure_http=allow_insecure_http)
        if secret is None:  # no key given: the environment's, for Anthropic's own endpoint only
            secret, missing = _env_key(base)
            if secret is None:
                _refuse(missing)
        checked_temperature = _check_temperature(temperature)
        version = _check_api_version(api_version, checked_temperature)
        route = _Route(
            url=base + MESSAGES_PATH,
            provider_kind="anthropic",
            model_id=endpoint_model_id("anthropic", model, None if base == DEFAULT_BASE_URL else base),
            rate_key=base,
            body_fields=MappingProxyType({"model": model}),
            params=MappingProxyType({"api_version": version}),
            plain_headers=MappingProxyType({"anthropic-version": version}),
            credential=secret,
        )
        self._setup(route, model, checked_temperature, max_connections, allow_insecure_http, transport)

    def _setup(
        self,
        route: _Route,
        model: str,
        temperature: float | None,
        max_connections: object,
        allow_insecure_http: bool,
        transport: object,
    ) -> None:
        """Set the checked state shared by both routes (one code path for the body, the reply and the bounds)."""
        connections = _check_max_connections(max_connections)
        self._http = _http_client(connections, allow_insecure_http, transport)
        self._max_connections = connections
        self._max_concurrency: int | None = None
        self._route = route
        self._model = model
        self._temperature = temperature

    @classmethod
    def google_cloud(
        cls,
        model: str,
        *,
        project: str,
        location: str,
        auth: GoogleCloudAuth,
        temperature: float | None = None,
        max_connections: int = 8,
        transport: object = None,
    ) -> "AnthropicProvider":
        """Claude on Google Cloud, Google managed (Vertex AI, now Agent Platform), via ``rawPredict`` (module notes, 7).

        The Anthropic-managed Google Cloud offering (``claude.googleapis.com``) is not supported. The token source is
        called once per request and must return promptly: the request's deadline cannot cut a source that hangs.

        Args:
            model: The Claude model ID on Google Cloud (``"claude-opus-4-7"``, or an older ``@``-versioned ID such as
                ``"claude-opus-4-5@20251101"``): one URL path segment.
            project: The Google Cloud project ID, number or ``domain:project-id``.
            location: ``global``, a multi-region (``us``, ``eu``) or a region (``us-east5``). Newer Claude models need
                ``global``, ``us`` or ``eu``: regional endpoints serve Claude Sonnet 4.6 and earlier models only.
            auth: A :class:`~datagrunt.rationalization.providers.google_cloud.GoogleCloudAuth` with a token source:
                Google documents no API key and no express mode for Claude.
            temperature: ``None`` (the default) leaves it out; current Claude models refuse any other value.
            max_connections: The HTTP connection pool size, 1-1,024.
            transport: An ``httpx.BaseTransport`` to use instead of the network (tests).

        Raises:
            RationalizationError: If an option breaks its rule: ``auth`` not a ``GoogleCloudAuth``, an API key or
                express mode (``project=None``), a bad project, location or model; never showing a credential.
        """
        checked, problem = _auth_of(auth)
        del auth  # whatever was passed as auth leaves this frame before anything can raise
        if checked is None:
            _refuse(problem)
        target = GoogleCloudTarget(project, location)
        check_access(target, checked, interface=GOOGLE_CLOUD_METHOD)
        model_id = model_id_for(target, GOOGLE_CLOUD_PUBLISHER, model)
        route = _Route(
            url=target.publisher_model_url(GOOGLE_CLOUD_PUBLISHER, model, GOOGLE_CLOUD_METHOD),
            provider_kind=GOOGLE_CLOUD_KIND,
            model_id=model_id,
            rate_key=model_id,  # quotas are per model: the resource name, never the project's shared key
            body_fields=MappingProxyType({"anthropic_version": ANTHROPIC_VERSION}),
            params=MappingProxyType({"anthropic_version": ANTHROPIC_VERSION}),
            plain_headers=MappingProxyType({}),
            credential=checked,
        )
        provider = cls.__new__(cls)
        provider._setup(route, model, _check_temperature(temperature), max_connections, False, transport)
        return provider

    @classmethod
    def from_model_string(cls, model_string: ModelString, **options: Any) -> "AnthropicProvider":
        """The provider for a parsed ``anthropic:<model>`` string, with the constructor's keyword options.

        The key is taken out of ``options`` and wrapped before anything is checked, so no frame that raises holds
        its text (module notes, 1).

        Raises:
            RationalizationError: If ``model_string`` is not an anthropic ``ModelString``, an option is not one of the
                constructor's (naming only the option names), no valid key is found, or the constructor refuses an
                option.
        """
        secret, problem = _wrap_key(options.pop("api_key", None))
        if not isinstance(model_string, ModelString):
            _refuse(f"from_model_string needs a ModelString, got {type(model_string).__name__}")
        if model_string.kind != "anthropic":
            _refuse(f"from_model_string needs an anthropic: model string, got kind {model_string.kind}")
        unknown = sorted(name for name in options if name not in _OPTIONS)
        if unknown:
            _refuse(
                f"from_model_string takes no option {', '.join(unknown[:_OPTIONS_SHOWN])}: the options are api_key, "
                f"{', '.join(sorted(_OPTIONS))}"
            )
        if problem is not None:
            _refuse(problem)
        if model_string.base_url is not None:
            options.setdefault("base_url", model_string.base_url)
        return cls(model_string.model, api_key=secret, **options)

    @property
    def model_id(self) -> str:
        """``anthropic:<model>`` (``@<base_url>`` for any endpoint but Anthropic's), or on Google Cloud
        ``google-cloud:projects/{p}/locations/{l}/publishers/anthropic/models/{model}``."""
        return self._route.model_id

    @property
    def provider_kind(self) -> str:
        """``"anthropic"``, or ``"google-cloud-anthropic"`` on Google Cloud."""
        return self._route.provider_kind

    @property
    def rate_key(self) -> str:
        """The normalized base URL, or on Google Cloud the model id (the resource name: quotas are per model)."""
        return self._route.rate_key

    @property
    def model_family(self) -> str:
        """Always ``"anthropic"``: Claude, whatever the endpoint."""
        return MODEL_FAMILY

    @property
    def max_connections(self) -> int:
        """The HTTP connection pool size (read-only); the runner sizes its per-provider concurrency within it."""
        return self._max_connections

    @property
    def max_concurrency(self) -> int | None:
        """Optional per-provider concurrency cap (1-64), or ``None``."""
        return self._max_concurrency

    def with_max_concurrency(self, max_concurrency: int | None) -> "AnthropicProvider":
        """Configure a per-provider concurrency cap (1-64, or ``None``) and return ``self``."""
        if max_concurrency is not None and (type(max_concurrency) is not int or not 1 <= max_concurrency <= 64):
            _refuse("max_concurrency must be None or an int in 1-64")
        self._max_concurrency = max_concurrency
        return self

    @property
    def generation_params(self) -> dict[str, Any]:
        """A new mapping of what shapes the output: the API version (on Google Cloud, the body's
        ``anthropic_version``), the mechanism, the temperature and the wire."""
        params: dict[str, Any] = {**self._route.params, "mechanism": MECHANISM, "wire": WIRE}
        if self._temperature is not None:
            params["temperature"] = self._temperature
        return params

    def transform_schema(self, schema: Mapping[str, Any]) -> dict[str, Any]:
        """The schema structured outputs accept for ``schema`` (module notes, 3); pure.

        Raises:
            TypeError: If ``schema`` is not a ``Mapping``.
            RationalizationError: If the schema cannot be expressed.
        """
        return transform_schema(schema)

    def open(self) -> None:
        """Build the HTTP clients now, on the caller's thread (optional; the runner calls it at preflight).

        Raises:
            RationalizationError: If the environment prevents a client (a bad proxy or certificate setting).
        """
        self._http.open()

    def generate(self, request: ModelRequest) -> ProviderResponse:
        """Send one request and read its reply (module notes, 5-6).

        Raises:
            TypeError: If ``request`` is not a ``ModelRequest``.
            TransientProviderError: On a retryable status, a timeout or a transport failure.
            PermanentProviderError: On any other HTTP failure, or (Google Cloud) a token source that fails.
        """
        if not isinstance(request, ModelRequest):
            raise TypeError(f"generate needs a ModelRequest, got {type(request).__name__}")
        body: dict[str, Any] = {
            **self._route.body_fields,
            "max_tokens": request.max_output_tokens,
            "system": request.system_prompt,
            "messages": [{"role": "user", "content": request.user_envelope}],
            "output_config": {"format": {"type": MECHANISM, "schema": dict(request.json_schema)}},
        }
        if self._temperature is not None:
            body["temperature"] = self._temperature
        deadline = time.monotonic() + request.timeout_s  # set before the token is fetched, which counts against it
        reply = self._http.post_json(
            self._route.url,
            headers=self._route.headers(),
            body=body,
            timeout_s=request.timeout_s,
            deadline=deadline,
        )
        return _parse_reply(reply)

    def __reduce_ex__(self, protocol: object) -> NoReturn:
        raise TypeError("an AnthropicProvider holds a credential: it never pickles or copies")

    def __reduce__(self) -> NoReturn:
        raise TypeError("an AnthropicProvider holds a credential: it never pickles or copies")

    def __repr__(self) -> str:
        route = getattr(self, "_route", None)
        if route is None:  # not built (__new__ alone, or a refused construction)
            return "AnthropicProvider(<unset>)"
        return f"AnthropicProvider(model_id={route.model_id!r}, provider_kind={route.provider_kind!r})"
