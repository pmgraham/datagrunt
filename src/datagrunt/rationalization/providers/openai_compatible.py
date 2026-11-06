"""The OpenAI-compatible wire adapter: Chat Completions with strict structured outputs (plan §4.5, R3).

:class:`OpenAICompatibleProvider` serves OpenAI (:meth:`~OpenAICompatibleProvider.openai`), a local Ollama server
(:meth:`~OpenAICompatibleProvider.ollama`), any other server speaking OpenAI's Chat Completions (vLLM, a gateway:
the constructor), and open and partner models on Google Cloud (:meth:`~OpenAICompatibleProvider.google_cloud`). The
rules:

1. **What is sent, and where.** Each :meth:`~OpenAICompatibleProvider.generate` is one ``POST
   {base_url}/chat/completions`` through the bounded :class:`~datagrunt.rationalization.providers.http.HttpClient`
   (no redirect followed, ``https`` or loopback ``http`` only unless ``allow_insecure_http=True``), whose body is::

       {"model": <model>,
        "messages": [{"role": "system", "content": <system_prompt>}, {"role": "user", "content": <user_envelope>}],
        "response_format": {"type": "json_schema",
                            "json_schema": {"name": "records", "schema": <request.json_schema>, "strict": <strict>}},
        <max_tokens_field>: <max_output_tokens>, "temperature": <temperature>, "seed": <seed>}

   ``temperature`` defaults to 0 (the most repeatable sampling compatible servers, Ollama and Google Cloud's models
   offer) and to ``None``, left out, for :meth:`openai`, since OpenAI's reasoning models refuse a temperature;
   ``None`` leaves it out anywhere, and determinism stays cache replay's (R10). ``seed`` (default 0) is left out when
   ``None``. A ``system`` turn always comes first (OpenAI's reference says ``developer`` replaces it for o1 and newer
   models), never folded into the data's turn: the instructions and the untrusted document stay in separate turns.
   The read timeout is the request's ``timeout_s`` and the deadline ``time.monotonic() + timeout_s``, so the runner
   bounds each attempt.
2. **What is never sent, read or shown.** The key goes only in ``Authorization: Bearer <key>``, built by
   :meth:`~datagrunt.rationalization.providers.base._Secret.prefixed` and passed as a ``_Secret``: never in the URL
   or the body, a ``repr`` or an error. No frame of this module that raises holds it as text (a log with
   ``capture_locals`` prints traceback locals): the constructor, :meth:`openai` and :meth:`from_model_string` turn it
   into a ``_Secret`` and delete the text before any check can raise; only the caller's own frame, which passed it,
   still holds it. Ollama gets no key. The constructor reads no environment variable; only :meth:`openai` (and an
   ``openai`` model string) reads ``OPENAI_API_KEY``, and only when no ``api_key`` is passed (an empty one is
   refused, never replaced). ``OPENAI_BASE_URL`` is never read (plan correction 5). Nothing is logged.
3. **Identity.** ``model_id`` is :func:`~datagrunt.rationalization.providers.base.endpoint_model_id` of the kind
   (``openai`` for :meth:`openai`, ``ollama`` for :meth:`ollama`, ``openai-compatible`` for the constructor), the
   model and the normalized endpoint, so two endpoints never share one. ``provider_kind`` is ``"openai-compatible"``
   for every direct endpoint (one wire format). ``rate_key`` is the normalized endpoint: one rate limit per endpoint,
   shared by its models and keys (Google Cloud's identities: rule 7). ``model_family`` is ``"openai"`` for
   :meth:`openai`, else the ``family=`` the caller declares (``"meta"`` for a Llama behind a gateway), ``"unknown"`` by
   default; it is in neither the model id nor ``generation_params``, since it shapes no output. ``generation_params``
   is ``{"wire": "chat-completions/1", "temperature", "seed", "max_tokens_field", "strict"}`` with each
   ``None``-valued key left out: exactly what the body carries beyond the request, and the wire format's version, so a
   change of format re-keys the cache.
4. **The strict transform** (:meth:`~OpenAICompatibleProvider.transform_schema`, ``strict=True``), a fresh copy in
   OpenAI's documented strict subset:

   - an object root (never ``anyOf``); on every object ``additionalProperties: false`` and ``required`` = every
     property, in ``properties`` order;
   - **every field that is not required is sent required and nullable** (maintainer decision 2026-11-05, "null =
     not shown"): strict mode makes every field required, so a field the caller's schema leaves optional (a
     non-Optional scalar, model or list with a default, an empty one or a default factory) would otherwise force the
     model to invent a value. It is sent as Optional's form: ``null`` appended to its own ``anyOf``, else
     ``{"anyOf": [<schema>, {"type": "null"}]}`` with its description on the outer node. A null there means "not
     shown": consensus (T10) drops the key before validation, so the caller's pydantic default applies (a factory's
     included); the adapter never changes a reply. Optional fields are sent as they are;
   - a ``$ref`` must name an entry of the root's ``$defs`` (``#/$defs/<name>``, the name one RFC 6901 token, read by
     :func:`~datagrunt.rationalization._refs.def_name` as every transform reads it), and stands alone, but for a
     text ``description``, kept as ``{"anyOf": [{"$ref": ...}], "description": ...}`` so the field's instruction
     survives; a ``const`` becomes a one-value ``enum``;
   - a recursive schema, a ``$ref`` cycle among the ``$defs`` entries through a ``properties`` value, ``items``, an
     ``anyOf`` member, a schema-valued ``additionalProperties`` or a ``$ref``'s siblings (a ``$ref`` or an
     ``anyOf`` alone on it included, which no nesting count would ever catch, and an unreferenced entry's), is
     refused at ``#/$defs`` before anything is transformed, naming its path (``#/$defs/A -> #/$defs/B ->
     #/$defs/A``, ten hops at most), by the check every transform shares
     (:func:`~datagrunt.rationalization._refs.ref_cycle`, which reads at most :data:`SCHEMA_MAX_NODES` nodes of the
     entries and refuses more at ``#/$defs``); ``schema.prepare()`` already refuses a recursive model, so this is
     defense in depth for a direct caller of the transform;
   - a keyword that holds subschemas the strict subset does not read, or changes what a ``$ref`` resolves to
     (:data:`~datagrunt.rationalization._refs.UNREAD_KEYWORDS`: ``oneOf``, ``allOf``, ``not``, ``if`` / ``then`` /
     ``else``, ``prefixItems``, ``patternProperties``, ``$id``, ...), is refused naming the JSON pointer, as the
     Anthropic and Gemini adapters refuse it: dropping it would loosen the schema silently, and a ``$ref`` under it
     would escape the cycle check. ``schema.prepare()`` refuses every one of them, so no prepared schema meets this;
     beside a ``$ref``, which stands alone, it is dropped with the ``$ref``'s other siblings;
   - every node keeps ``type``, ``description`` (text only), ``enum`` and ``anyOf``, plus its type's own keywords:
     ``pattern`` and ``format`` (one of :data:`SENT_FORMATS`) on a string; ``multipleOf``, ``maximum``,
     ``exclusiveMaximum``, ``minimum`` and ``exclusiveMinimum`` on a number or integer; ``items``, ``minItems`` and
     ``maxItems`` on an array. ``date-time`` and ``time`` are never sent as a format, whose RFC 3339 form needs an
     offset a ``NaiveDatetime`` refuses: they become a hint in the description (``an ISO 8601 date and time``).
     A fine-tuned model (a name starting ``ft:``) gets no ``pattern``, ``format``, number bound, ``multipleOf``,
     ``minItems`` or ``maxItems``, which OpenAI's fine-tuned models refuse. Everything else (``title``,
     ``default``, ``examples``, ``minLength``, ``maxLength``, ...) is dropped: the local ``model_validate`` enforces
     every constraint anyway (plan §4.1);
   - **OpenAI's limits**, checked here, before any request: at most :data:`OPENAI_MAX_PROPERTIES` object properties;
     at most :data:`OPENAI_MAX_NESTING` levels of nesting, counting every object and array on a path (the response
     wrapper's root, ``records`` and item included), every ``$ref`` followed (no cycle is left to follow);
     at most :data:`OPENAI_MAX_ENUM_VALUES` enum values; at most :data:`OPENAI_MAX_TEXT_CHARS` characters of property
     names, definition names and enum values together (a value that is not a string counted as its JSON text, since
     the limit names "enum values" of any type); and at most :data:`OPENAI_MAX_LARGE_ENUM_CHARS` characters in the
     string values of one enum of more than :data:`OPENAI_LARGE_ENUM_VALUES` values (that rule names "string
     values"). Counts are taken as written: a
     definition used twice counts once (if OpenAI counts each use, such a schema fails at its first request with a
     400, a permanent error). Each breach is named with its limit and its count.

   ``strict=False`` sends the schema as given, a fresh equal copy, with none of these checks (the ``$ref`` reading,
   the cycle check and the keyword refusals included): a gateway's own limits may differ.
5. **The reply.** ``choices[0]`` is read: a ``message.refusal`` other than ``null`` or ``""`` is ``"refused"`` (fail
   closed); else ``finish_reason`` ``"length"`` is ``"truncated"``, ``"content_filter"`` ``"refused"``, and
   ``"stop"`` or anything else (``tool_calls``, a missing or new reason) ``"complete"``. ``content`` is
   :func:`~datagrunt.rationalization.providers.responses.parse_json_object` of ``message.content`` (``None`` when it
   is not exactly one JSON object: the runner quarantines it as unparseable). ``raw_text`` is ``message.content``
   when it is a string, else a non-empty refusal text, else the whole reply text. A 200 that is no chat completion
   (no ``choices[0].message`` object) is a complete response with ``content`` ``None`` and the reply text: a model
   or output problem never raises. Token counts are ``usage.prompt_tokens`` and ``usage.completion_tokens`` when
   they are counts (junk is ``None``). Every response is built by
   :func:`~datagrunt.rationalization.providers.responses.response`, which escapes and bounds it. Every HTTP failure
   propagates as the HTTP helper raises it (``TransientProviderError`` or ``PermanentProviderError``).
6. **Options.** :meth:`from_model_string` takes exactly the options of the factory its kind names (``api_key`` for
   ``openai`` and ``openai-compatible``; ``family`` and ``allow_insecure_http`` for ``ollama`` and
   ``openai-compatible``; the generation and pool options for all), with the same defaults; any other option is
   refused naming the option, never its value.
7. **Google Cloud** (:meth:`google_cloud`, built on :mod:`~datagrunt.rationalization.providers.google_cloud`, whose
   module notes hold the research). A project, a location (a region, ``us`` / ``eu``, or ``global``, which picks the
   host) and a :class:`~datagrunt.rationalization.providers.google_cloud.GoogleCloudAuth` holding a token source:
   none of these interfaces takes an API key (``check_access`` refuses one at construction). The token is fetched
   once per request, on the worker, after the request's deadline is set, so a slow token source spends the request's
   own time; a failing one is a ``PermanentProviderError`` and nothing is sent. The paths:

   ==========  ============================================  ======================  ========================  ======
   Path        POST (``https://{host}/v1/projects/{p}/        Body ``model``           ``provider_kind``          strict
               locations/{l}/…``)
   ==========  ============================================  ======================  ========================  ======
   shared      ``endpoints/openapi/chat/completions``        ``{publisher}/{model}``  ``google-cloud-openai``    on
   Mistral     ``publishers/mistralai/models/{model}:``       the model without its    ``google-cloud-mistral``   off
               ``rawPredict``                                ``@version``
   dedicated   ``endpoints/{endpoint_id}/chat/completions``  none                    ``google-cloud-endpoint``  off
   ==========  ============================================  ======================  ========================  ======

   ``strict`` defaults on where Google documents schema-enforced structured output (every MaaS open model; Gemini
   through the OpenAI-compatible endpoint; Grok, whose page parses Chat Completions output against a schema: "The
   model output will adhere to the following JSON schema"), off where it does not document structured output
   (Mistral) or leaves it to the container (dedicated endpoints); a caller's ``strict=`` overrides it. The strict
   transform is OpenAI's subset with OpenAI's limits (rule 4), a conservative common subset: Google states only that
   fully recursive schemas are not supported. ``temperature`` defaults to 0 on every route (each takes one).

   Every route sends the Chat Completions turns of rule 1, a ``system`` turn first. Google's pages show only ``user``
   (and ``assistant``) turns, for the shared endpoint and for Mistral ("The first message must use the ``user``
   role"), while Mistral's own API has a ``system`` role. A route that refuses the ``system`` turn fails its first
   request with a permanent 4xx, preferred to weakening the boundary between the instructions and the untrusted data
   by folding them into one turn; T16's live smoke test settles it. (A private hook, ``_Route.fold_system``, can fold
   them; nothing sets it, and an opt-in would be explicit.) The shared endpoint and a dedicated endpoint take the body
   of rule 1 (``max_tokens``; the dedicated endpoint's REST example names no ``model``). Mistral takes its own body
   (wire ``mistral-raw-predict/2``): ``model``, the turns, ``response_format`` as in rule 1, ``max_tokens``,
   ``stream: false``, ``temperature`` and the seed as ``random_seed`` (Mistral's own API names; Google's page shows
   only ``model``, ``messages``, ``max_tokens`` and ``stream``, so a field it refuses fails the first request with a
   4xx). Every route's reply is mapped by rule 5.

   ``model_id`` is the model's resource name (``google-cloud:projects/{p}/locations/{l}/publishers/{publisher}/
   models/{model}``, the ``@version`` kept; a dedicated endpoint's ``…/endpoints/{id}``, then ``#model={label}``
   when ``deployed_model`` names the model deployed there), distinct from every direct provider's. Without a
   ``deployed_model`` label, a new model deployed behind the same endpoint id keeps the model id, so it replays the
   old model's cached answers: label each deployment. ``rate_key`` is the model's resource name (Vertex quotas are
   per model, project and region), a dedicated endpoint's without its label. ``model_family`` is the publisher's
   (``meta``, ``mistral``, ``qwen``, ...): a declared ``family=`` must match it, and supplies the family only for a
   publisher with none, or a dedicated endpoint (``"unknown"`` without one). Claude (publisher ``anthropic``) is
   refused with the remedy ``AnthropicProvider.google_cloud``, and AI21 (no documented request body) as unsupported.

Wire format read from OpenAI's documentation on 2026-11-04 (the URLs and the facts pinned are in
``tests/rationalization_tests/test_providers_openai.py``):
https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create and
https://developers.openai.com/api/docs/guides/structured-outputs. Not stated there, and so chosen conservatively:
whether ``title``, ``default``, ``examples``, ``minLength`` and ``maxLength`` are accepted (dropped), whether
``const`` is (rewritten as ``enum``), whether a ``$ref`` may carry siblings (only a description, inside an
``anyOf``), and how the limits count (as above).

Every rejection a caller can cause at construction is a :class:`~datagrunt.rationalization.RationalizationError`
naming the argument, never echoing a key. Bounds: the transform visits at most :data:`SCHEMA_MAX_NODES` nodes,
iteratively, and the nesting count enters each definition at most once per level, up to a cap; a reply is bounded by
the HTTP helper (8 MiB) and :func:`response`. The module imports the standard library and the package's own modules,
never the HTTP stack itself (only the HTTP helper imports httpx): nothing from ``datagrunt.core``, and no import
inside a function (invariant 9).
"""

import math
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, NoReturn, Protocol, TypeAlias, cast, get_args

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
    OLLAMA_BASE_URL,
    OPENAI_BASE_URL,
    UNKNOWN_FAMILY,
    ModelString,
    _Secret,
    _secret_problem,
    api_key_from_env,
    check_endpoint,
    endpoint_model_id,
    family_problem,
    identity_problem,
)
from datagrunt.rationalization.providers.google_cloud import (
    GoogleCloudAuth,
    GoogleCloudTarget,
    Interface,
    check_access,
    check_model_segment,
    dedicated_model_id,
    family_for_publisher,
    model_id_for,
    split_publisher_model,
)
from datagrunt.rationalization.providers.http import MAX_CONNECTIONS_LIMIT, HttpClient, HttpReply
from datagrunt.rationalization.providers.responses import parse_json_object, response
from datagrunt.rationalization.wire import FinishReason, ModelRequest, ProviderResponse

MaxTokensField: TypeAlias = Literal["max_tokens", "max_completion_tokens"]
"""The body field that carries the output limit: OpenAI reads ``max_completion_tokens``, compatible servers
``max_tokens``."""

MAX_TOKENS_FIELDS: tuple[MaxTokensField, ...] = get_args(MaxTokensField)

WIRE_FORMAT = "chat-completions/1"
"""The version of the Chat Completions request, in ``generation_params``: a change of format re-keys the cache."""

MISTRAL_WIRE_FORMAT = "mistral-raw-predict/2"
"""The version of the Mistral ``rawPredict`` request on Google Cloud (module notes, 7)."""

MISTRAL_PUBLISHER = "mistralai"
"""The Google Cloud publisher whose models take the Mistral body through ``rawPredict``."""

GOOGLE_MAX_TOKENS_FIELD: MaxTokensField = "max_tokens"
"""The output-limit field on Google Cloud (``max_completion_tokens`` is its documented alias there)."""

OPENAI_FAMILY = "openai"
"""The ``model_family`` of :meth:`OpenAICompatibleProvider.openai`."""

OPENAI_KEY_VARIABLE = "OPENAI_API_KEY"
"""The one environment variable this module reads, for OpenAI only."""

FINE_TUNED_PREFIX = "ft:"
"""How an OpenAI fine-tuned model's name starts."""

SCHEMA_NAME = "records"
"""The ``response_format.json_schema.name`` sent (``a-z A-Z 0-9 _ -``, at most 64 characters)."""

TEMPERATURE_MAX = 2.0
"""The largest temperature OpenAI documents."""

SEED_MIN = -(2**63)
SEED_MAX = 2**63 - 1
"""The seed range: a signed 64-bit integer."""

SCHEMA_MAX_NODES = 100_000
"""The most schema nodes the strict transform visits."""

STRICT_FORMATS = frozenset({"date-time", "time", "date", "duration", "email", "hostname", "ipv4", "ipv6", "uuid"})
"""The string formats OpenAI's strict mode supports."""

FORMAT_HINTS: Mapping[str, str] = MappingProxyType(
    {"date-time": "an ISO 8601 date and time", "time": "an ISO 8601 time"}
)
"""The formats sent as a description hint instead (a naive value has no offset), and their hints."""

SENT_FORMATS = STRICT_FORMATS - FORMAT_HINTS.keys()
"""The string formats sent as ``format``; any other is dropped."""

OPENAI_MAX_PROPERTIES = 5_000
OPENAI_MAX_NESTING = 10
OPENAI_MAX_ENUM_VALUES = 1_000
OPENAI_MAX_TEXT_CHARS = 120_000
OPENAI_LARGE_ENUM_VALUES = 250
OPENAI_MAX_LARGE_ENUM_CHARS = 15_000
"""OpenAI's strict-mode limits (module notes, 4)."""

DEFAULT_MAX_CONNECTIONS = 8

_ROOT = "#"
_WIRE_FORMATS: Mapping[str, str] = MappingProxyType({"chat": WIRE_FORMAT, "mistral": MISTRAL_WIRE_FORMAT})
# Publishers whose models no Chat Completions interface serves on Google Cloud, and why.
_OTHER_INTERFACES: Mapping[str, str] = MappingProxyType(
    {
        "anthropic": (
            "Claude on Google Cloud takes the Anthropic Messages body through rawPredict: use "
            "AnthropicProvider.google_cloud"
        ),
        "ai21": "ai21 models on Google Cloud use rawPredict with no documented request body: they are not supported",
    }
)
_CONTAINER_TYPES = ("object", "array")
_DEPTH_CAP = 4 * OPENAI_MAX_NESTING  # the nesting count stops descending here: past the limit, deeper adds nothing
_SHOWN = 10
_NAME_CHARS = 120
_COMMON_KEYWORDS = frozenset({"type", "description", "enum", "anyOf"})
_NUMBER_KEYWORDS = frozenset({"multipleOf", "maximum", "exclusiveMaximum", "minimum", "exclusiveMinimum"})
# required and additionalProperties are written on every object, so they are not kept from the input.
_TYPE_KEYWORDS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "object": frozenset({"properties"}),
        "array": frozenset({"items", "minItems", "maxItems"}),
        "string": frozenset({"pattern", "format"}),
        "number": _NUMBER_KEYWORDS,
        "integer": _NUMBER_KEYWORDS,
    }
)
_FINE_TUNED_KEYWORDS: Mapping[str, frozenset[str]] = MappingProxyType(
    {"object": frozenset({"properties"}), "array": frozenset({"items"})}
)
_FINISH: Mapping[str, FinishReason] = MappingProxyType(
    {"stop": "complete", "length": "truncated", "content_filter": "refused"}
)
_NO_REFUSAL: tuple[object, ...] = (None, "")
_DEFAULTS: Mapping[str, object] = MappingProxyType(
    {
        "family": UNKNOWN_FAMILY,
        "temperature": 0.0,
        "seed": 0,
        "max_tokens_field": "max_tokens",
        "strict": True,
        "allow_insecure_http": False,
        "max_connections": DEFAULT_MAX_CONNECTIONS,
        "transport": None,
    }
)
_KIND_DEFAULTS: Mapping[str, Mapping[str, object]] = MappingProxyType(
    {
        "openai": MappingProxyType(
            {**_DEFAULTS, "family": OPENAI_FAMILY, "temperature": None, "max_tokens_field": "max_completion_tokens"}
        ),
        "ollama": _DEFAULTS,
        "openai-compatible": _DEFAULTS,
    }
)
_SHARED_OPTIONS = frozenset({"temperature", "seed", "max_tokens_field", "strict", "max_connections", "transport"})
_KIND_OPTIONS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "openai": _SHARED_OPTIONS | {"api_key"},
        "ollama": _SHARED_OPTIONS | {"family", "allow_insecure_http"},
        "openai-compatible": _SHARED_OPTIONS | {"api_key", "family", "allow_insecure_http"},
    }
)

# A schema node still to transform, and its JSON pointer.
_Pending: TypeAlias = tuple[object, str]
_Kind: TypeAlias = Literal["openai", "ollama", "openai-compatible"]
_Wire: TypeAlias = Literal["chat", "mistral"]


# ---------------------------------------------------------------------------------------------------------------
# The strict transform
# ---------------------------------------------------------------------------------------------------------------


@dataclass(slots=True)
class _Walk:
    """What one strict transform gathers as it walks (one per call, never shared)."""

    defs: dict[str, Any]
    fine_tuned: bool
    properties: int = 0
    enum_values: int = 0
    text_chars: int = 0
    long_enums: list[str] = field(default_factory=list)


def _refuse_schema(pointer: str, reason: str) -> NoReturn:
    raise RationalizationError(f"the response schema at {pointer} cannot be sent in OpenAI's strict mode: {reason}")


def strict_schema(schema: Mapping[str, Any], *, fine_tuned: bool = False) -> dict[str, Any]:
    """Return a fresh copy of ``schema`` in OpenAI's strict subset, within its limits (module notes, 4).

    Args:
        schema: The neutral response schema.
        fine_tuned: Leave out the constraint keywords fine-tuned models refuse.

    Raises:
        TypeError: If ``schema`` is not a ``Mapping``.
        RationalizationError: If it is not exactly JSON; its root is not an object or uses ``anyOf``; a node is not
            a schema object; an object has no ``properties`` map; ``anyOf`` is not a list; ``$defs`` is not a map or
            sits below the root; a ``$ref`` names no ``$defs`` entry; the ``$defs`` entries refer to one another in a
            cycle; it holds more than :data:`SCHEMA_MAX_NODES` nodes; or it passes one of OpenAI's limits. Each
            message names where (the JSON pointer, the cycle's path, or the limit and the count).
    """
    root = _json_copy(schema)
    if root.get("type") != "object" or "anyOf" in root:
        _refuse_schema(_ROOT, "the root must be an object schema, not anyOf")
    defs = root.get("$defs", {})
    if not isinstance(defs, dict):
        _refuse_schema(_ROOT, "$defs must map names to schemas")
    _check_acyclic(defs)
    walk = _Walk(defs=defs, fine_tuned=fine_tuned, text_chars=sum(len(name) for name in defs))
    _transform(root, walk)
    _check_sizes(walk)
    _check_nesting(root, defs)
    return root


def _check_acyclic(defs: Mapping[str, Any]) -> None:
    """Refuse a ``$ref`` cycle among the ``$defs`` entries, naming its path (module notes, 4)."""
    try:
        cycle = ref_cycle(defs, max_nodes=SCHEMA_MAX_NODES)
    except NodeBoundError:
        _refuse_schema(DEFS_POINTER, f"it has more than {SCHEMA_MAX_NODES} nodes")
    if cycle is not None:
        _refuse_schema(DEFS_POINTER, f"it is recursive (a $ref cycle: {cycle_text(cycle)})")


def _json_copy(schema: object) -> dict[str, Any]:
    """A fresh copy of ``schema``, which must be a JSON object exactly (its keys sorted at every level)."""
    if not isinstance(schema, Mapping):
        raise TypeError(f"schema must be a Mapping, got {type(schema).__name__}")
    copy: dict[str, Any] = canonical_copy(dict(schema), subject="the response schema")
    return copy


def _transform(root: dict[str, Any], walk: _Walk) -> None:
    """Rewrite every node of ``root`` in place, iteratively, within the node budget."""
    stack: list[_Pending] = [(root, _ROOT)]
    for _ in range(SCHEMA_MAX_NODES):  # each pass transforms one node: bounded by the node budget
        node, pointer = stack.pop()
        stack.extend(_strict_node(node, pointer, walk))
        if not stack:
            return
    raise RationalizationError(f"the response schema has more than {SCHEMA_MAX_NODES} nodes")


def _strict_node(node: object, pointer: str, walk: _Walk) -> list[_Pending]:
    """Rewrite one node in place (a fresh copy's own) and tally it; return the nodes under it."""
    if not isinstance(node, dict):
        _refuse_schema(pointer, "this is not a schema object")
    if "$ref" in node:
        _strict_ref(node, pointer, walk.defs)
        return []
    if "$defs" in node and pointer != _ROOT:
        _refuse_schema(pointer, "$defs may only sit at the root")
    unread = sorted(keyword for keyword in node if keyword in UNREAD_KEYWORDS)
    if unread:
        _refuse_schema(pointer, f"{unread[0]!r} holds subschemas strict mode would drop, loosening the schema silently")
    is_object = node.get("type") == "object"
    if is_object:
        _null_defaults(node)
    _keep_supported(node, root=pointer == _ROOT, fine_tuned=walk.fine_tuned)
    if is_object:
        _close_object(node, pointer)
        walk.properties += len(node["properties"])
        walk.text_chars += sum(len(name) for name in node["properties"])
    _tally_enum(node, pointer, walk)
    return _children(node, pointer)


def _strict_ref(node: dict[str, Any], pointer: str, defs: Mapping[str, Any]) -> None:
    """A ``$ref`` alone, or beside its text description inside a one-member ``anyOf``; it must resolve."""
    ref, description = node.get("$ref"), node.get("description")
    name = def_name(ref)
    if name is None or name not in defs:
        _refuse_schema(pointer, "a $ref must name an entry of the root's $defs (#/$defs/<name>)")
    node.clear()
    if isinstance(description, str):
        node["anyOf"] = [{"$ref": ref}]
        node["description"] = description
    else:
        node["$ref"] = ref


def _nullable(prop: Mapping[str, Any]) -> bool:
    """Whether a property's schema admits ``null``: a null type, or Optional's ``anyOf`` with a null member."""
    kind = prop.get("type")
    if kind == "null" or (isinstance(kind, list) and "null" in kind):
        return True
    members = prop.get("anyOf")
    return isinstance(members, list) and any(isinstance(m, Mapping) and m.get("type") == "null" for m in members)


def _needs_null(prop: object) -> bool:
    """Whether a property that is not required must admit null once strict mode requires it (module notes, 4): any
    that is not nullable already, a list included, whatever its default."""
    return isinstance(prop, dict) and not _nullable(prop)


def _with_null(prop: dict[str, Any]) -> dict[str, Any]:
    """``prop`` admitting null as Optional's form does: null appended to its own ``anyOf``, else beside it in a new
    one, its description moved to the outer node."""
    members = prop.get("anyOf")
    if isinstance(members, list):
        prop["anyOf"] = [*members, {"type": "null"}]
        return prop
    wrapped: dict[str, Any] = {"anyOf": [prop, {"type": "null"}]}
    if "description" in prop:
        wrapped["description"] = prop.pop("description")
    return wrapped


def _null_defaults(node: dict[str, Any]) -> None:
    """Make each property of an object that is not required, and would force the model to invent a value once it is,
    nullable: null means "not shown" (module notes, 4). Runs before ``required`` is rewritten."""
    properties, required = node.get("properties"), node.get("required")
    if not isinstance(properties, dict):
        return  # _close_object refuses it
    named = required if isinstance(required, list) else []
    for name, prop in list(properties.items()):
        if name not in named and _needs_null(prop):
            properties[name] = _with_null(prop)


def _move_format(node: dict[str, Any]) -> None:
    """Send a hinted format as a description hint, keep a sent one, drop any other."""
    form = node.get("format")
    hint = FORMAT_HINTS.get(form) if isinstance(form, str) else None
    if hint is not None:
        description = node.get("description")
        node["description"] = f"{description} ({hint})" if isinstance(description, str) and description else hint
        del node["format"]
    elif not (isinstance(form, str) and form in SENT_FORMATS):
        node.pop("format", None)


def _keep_supported(node: dict[str, Any], *, root: bool, fine_tuned: bool) -> None:
    """Drop every keyword the strict subset does not take for the node's type; a ``const`` becomes an ``enum``."""
    kind = node.get("type")
    by_type = _FINE_TUNED_KEYWORDS if fine_tuned else _TYPE_KEYWORDS
    allowed = _COMMON_KEYWORDS | (by_type.get(kind, frozenset()) if isinstance(kind, str) else frozenset())
    if "const" in node and "enum" not in node:
        node["enum"] = [node["const"]]
    if not isinstance(node.get("description"), str):
        node.pop("description", None)
    _move_format(node)
    for keyword in [keyword for keyword in node if keyword not in allowed and not (root and keyword == "$defs")]:
        del node[keyword]


def _close_object(node: dict[str, Any], pointer: str) -> None:
    """Forbid extra keys and require every property, in ``properties`` order."""
    properties = node.get("properties")
    if not isinstance(properties, dict):
        _refuse_schema(pointer, "an object schema needs a properties map")
    node["additionalProperties"] = False
    node["required"] = list(properties)


def _tally_enum(node: Mapping[str, Any], pointer: str, walk: _Walk) -> None:
    """Count an enum's values and their characters (a non-string value's JSON text; only string values for the
    long-enum rule); note an enum of too many characters for its size."""
    values = node.get("enum")
    if not isinstance(values, list):
        return
    chars = sum(len(value) for value in values if isinstance(value, str))
    walk.enum_values += len(values)
    walk.text_chars += chars + sum(len(canonical_json(value)) for value in values if not isinstance(value, str))
    if len(values) > OPENAI_LARGE_ENUM_VALUES and chars > OPENAI_MAX_LARGE_ENUM_CHARS:
        walk.long_enums.append(f"{pointer} ({chars} characters)")


def _children(node: dict[str, Any], pointer: str) -> list[_Pending]:
    """The schemas under ``node``: its properties, its items, its anyOf members, then (at the root) its $defs."""
    children: list[_Pending] = [
        (child, json_pointer(f"{pointer}/properties", name)) for name, child in node.get("properties", {}).items()
    ]
    if "items" in node:
        children.append((node["items"], f"{pointer}/items"))
    members = node.get("anyOf", [])
    if not isinstance(members, list):
        _refuse_schema(pointer, "anyOf must be a list of schemas")
    children.extend((member, f"{pointer}/anyOf/{index}") for index, member in enumerate(members))
    defs = node.get("$defs", {}) if pointer == _ROOT else {}
    children.extend((child, json_pointer(f"{pointer}/$defs", name)) for name, child in defs.items())
    return children


def _check_sizes(walk: _Walk) -> None:
    """Refuse a schema past OpenAI's counted limits, naming each limit and its count (module notes, 4)."""
    breaches = []
    if walk.properties > OPENAI_MAX_PROPERTIES:
        breaches.append(f"at most {OPENAI_MAX_PROPERTIES} object properties, and it has {walk.properties}")
    if walk.enum_values > OPENAI_MAX_ENUM_VALUES:
        breaches.append(f"at most {OPENAI_MAX_ENUM_VALUES} enum values, and it has {walk.enum_values}")
    if walk.text_chars > OPENAI_MAX_TEXT_CHARS:
        breaches.append(
            f"at most {OPENAI_MAX_TEXT_CHARS} characters of property names, definition names and enum values, and "
            f"it has {walk.text_chars}"
        )
    if walk.long_enums:
        breaches.append(
            f"at most {OPENAI_MAX_LARGE_ENUM_CHARS} characters in an enum of more than {OPENAI_LARGE_ENUM_VALUES} "
            f"values, and these have more: {', '.join(walk.long_enums[:_SHOWN])}"
        )
    if breaches:
        raise RationalizationError(
            f"the response schema passes OpenAI's strict-mode limits ({'; '.join(breaches)}): simplify the schema, "
            "or pass strict=False"
        )


def _subschemas(node: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The schemas directly under a transformed node: its properties, its items and its anyOf members."""
    return [*node.get("properties", {}).values(), *([node["items"]] if "items" in node else []), *node.get("anyOf", [])]


def _deepest(root: dict[str, Any], defs: Mapping[str, Any]) -> int:
    """The most objects and arrays on one path of a transformed schema, every ``$ref`` followed, at most
    :data:`_DEPTH_CAP`. A definition is entered again only at a deeper level than before, so each is entered at most
    once per level. :func:`_check_acyclic` has refused every ``$ref`` cycle already; the cap bounds the walk anyway."""
    entered: dict[str, int] = {}
    stack: list[tuple[dict[str, Any], int]] = [(root, 0)]
    deepest = 0
    for _ in range((_DEPTH_CAP + 2) * SCHEMA_MAX_NODES):  # each node is popped at most once per level
        if not stack:
            return deepest
        node, level = stack.pop()
        name = def_name(node.get("$ref"))
        if name is not None:
            if entered.get(name, -1) < level:
                entered[name] = level
                stack.append((defs[name], level))
            continue
        level += node.get("type") in _CONTAINER_TYPES
        deepest = max(deepest, level)
        if level < _DEPTH_CAP:
            stack.extend((child, level) for child in _subschemas(node))
    raise RationalizationError(f"the response schema has more than {SCHEMA_MAX_NODES} nodes")


def _check_nesting(root: dict[str, Any], defs: Mapping[str, Any]) -> None:
    deepest = _deepest(root, defs)
    if deepest > OPENAI_MAX_NESTING:
        more = " or more" if deepest >= _DEPTH_CAP else ""
        raise RationalizationError(
            f"the response schema passes OpenAI's strict-mode limit of {OPENAI_MAX_NESTING} levels of nesting "
            f"(objects and arrays on one path, every $ref followed, the response wrapper's three included): it nests "
            f"{deepest}{more}; flatten the schema, or pass strict=False"
        )


# ---------------------------------------------------------------------------------------------------------------
# The reply
# ---------------------------------------------------------------------------------------------------------------


def _first_message(document: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], object] | None:
    """The first choice's message and finish reason, or ``None`` when the reply is not a chat completion."""
    if document is None:
        return None
    choices = document.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], Mapping):
        return None
    message = choices[0].get("message")
    if not isinstance(message, Mapping):
        return None
    return message, choices[0].get("finish_reason")


def _finish(message: Mapping[str, Any], finish_reason: object) -> FinishReason:
    """A refusal first (any value but null or ""), then the finish reason; an unknown one is complete."""
    if message.get("refusal") not in _NO_REFUSAL:
        return "refused"
    if isinstance(finish_reason, str):
        return _FINISH.get(finish_reason, "complete")
    return "complete"


def _raw_text(message: Mapping[str, Any], reply_text: str) -> str:
    """The message content when it is text, else a refusal's text, else the whole reply."""
    content = message.get("content")
    if isinstance(content, str):
        return content
    refusal = message.get("refusal")
    if isinstance(refusal, str) and refusal:
        return refusal
    return reply_text


def _usage(document: Mapping[str, Any] | None) -> tuple[object, object]:
    """The reported prompt and completion token counts, as reported (``response`` keeps only real counts)."""
    usage = None if document is None else document.get("usage")
    if not isinstance(usage, Mapping):
        return None, None
    return usage.get("prompt_tokens"), usage.get("completion_tokens")


def reply_response(reply: HttpReply) -> ProviderResponse:
    """The :class:`~datagrunt.rationalization.wire.ProviderResponse` of one 2xx chat completion (module notes, 5).

    It never raises for what the reply holds.
    """
    input_tokens, output_tokens = _usage(reply.json)
    found = _first_message(reply.json)
    if found is None:
        return response(
            content=None,
            raw_text=reply.text,
            finish="complete",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
    message, finish_reason = found
    return response(
        content=parse_json_object(message.get("content")),
        raw_text=_raw_text(message, reply.text),
        finish=_finish(message, finish_reason),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


# ---------------------------------------------------------------------------------------------------------------
# Construction checks
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Generation:
    """What the provider sends that shapes the output, beside the request (module notes, 3)."""

    temperature: float | None
    seed: int | None
    max_tokens_field: MaxTokensField
    strict: bool

    def params(self) -> dict[str, Any]:
        """Each setting of ``generation_params`` (the wire format is the route's), ``None``-valued ones left out."""
        found = {
            "temperature": self.temperature,
            "seed": self.seed,
            "max_tokens_field": self.max_tokens_field,
            "strict": self.strict,
        }
        return {name: value for name, value in found.items() if value is not None}


@dataclass(frozen=True, slots=True)
class _Settings:
    """Every checked option but the key, the model and the endpoint."""

    family: str
    generation: _Generation
    allow_insecure_http: bool
    max_connections: int
    transport: object


def _check_temperature(value: object) -> float | None:
    """``None``, or an ``int`` or ``float`` (not a ``bool``) in 0-2, held as a ``float``."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RationalizationError(f"temperature must be None or a number, got {type(value).__name__}")
    try:
        number = float(value)
    except OverflowError:  # an int past the float range
        number = math.inf
    if not math.isfinite(number) or not 0 <= number <= TEMPERATURE_MAX:
        raise RationalizationError(f"temperature must be finite and in 0-{TEMPERATURE_MAX:g}")
    return number


def _check_seed(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise RationalizationError(f"seed must be None or an int, got {type(value).__name__}")
    if not SEED_MIN <= value <= SEED_MAX:
        raise RationalizationError("seed must fit a signed 64-bit integer")
    return value


def _generation(temperature: object, seed: object, max_tokens_field: object, strict: object) -> _Generation:
    """The checked generation settings; each refusal names its argument."""
    if max_tokens_field not in MAX_TOKENS_FIELDS:
        raise RationalizationError(f"max_tokens_field must be one of {', '.join(MAX_TOKENS_FIELDS)}")
    if not isinstance(strict, bool):
        raise RationalizationError(f"strict must be a bool, got {type(strict).__name__}")
    return _Generation(_check_temperature(temperature), _check_seed(seed), max_tokens_field, strict)


def _settings(
    *,
    family: object,
    temperature: object,
    seed: object,
    max_tokens_field: object,
    strict: object,
    allow_insecure_http: object,
    max_connections: object,
    transport: object,
) -> _Settings:
    """Every option checked; each refusal names its argument, never its value."""
    problem = family_problem(family)
    if problem is not None:
        raise RationalizationError(f"family {problem}")
    if type(max_connections) is not int or not 1 <= max_connections <= MAX_CONNECTIONS_LIMIT:
        raise RationalizationError(f"max_connections must be an int in 1-{MAX_CONNECTIONS_LIMIT}")
    generation = _generation(temperature, seed, max_tokens_field, strict)
    # check_endpoint checks allow_insecure_http, before the endpoint it governs
    return _Settings(cast(str, family), generation, cast(bool, allow_insecure_http), max_connections, transport)


def _wrap_key(api_key: object) -> tuple[_Secret | None, str | None]:
    """The key a caller passed as a ``_Secret`` (``None`` for none), or the problem with it; it never raises, so no
    frame that raises holds the text."""
    if api_key is None:
        return None, None
    problem = _secret_problem(api_key, what="api_key")
    if problem is not None:
        return None, f"api_key refused: {problem}"
    return _Secret(cast(str, api_key)), None


def _option_problem(kind: str, names: Iterable[str], has_key: bool) -> str | None:
    """What is wrong with a model string's kind or options (module notes, 6), naming options, never values."""
    allowed = _KIND_OPTIONS.get(kind)
    if allowed is None:
        return f"OpenAICompatibleProvider serves the openai, ollama and openai-compatible kinds, not {kind}"
    unknown = {name for name in names if name not in allowed} | (
        {"api_key"} if has_key and "api_key" not in allowed else set()
    )
    if not unknown:
        return None
    shown = ", ".join(name[:_NAME_CHARS] for name in sorted(unknown)[:_SHOWN])
    return f"the {kind} provider takes no option {shown}: it takes {', '.join(sorted(allowed))}"


def _http_client(settings: _Settings) -> HttpClient:
    """The provider's HTTP helper; a transport that is not an ``httpx.BaseTransport`` is refused."""
    problem = ""
    try:  # the helper checks the transport's type at run time
        return HttpClient(
            max_connections=settings.max_connections,
            allow_insecure_http=settings.allow_insecure_http,
            transport=cast(Any, settings.transport),
        )
    except TypeError as error:
        problem = str(error)
    raise RationalizationError(problem)


# ---------------------------------------------------------------------------------------------------------------
# Routes and headers: where a request goes, how it is shaped, and what authorizes it
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Route:
    """Where a provider's requests go and how they are shaped (module notes, 3 and 7)."""

    url: str
    model_id: str
    rate_key: str
    provider_kind: str
    body_model: str | None  # the body's "model"; None leaves it out
    wire: _Wire
    fine_tuned: bool
    fold_system: bool = False  # a private hook: no route folds the system prompt into the user turn (module notes, 7)


@dataclass(frozen=True, slots=True)
class _GoogleRoute:
    """A Google Cloud route, with the interface it calls, its default ``strict`` and its publisher's family."""

    route: _Route
    interface: Interface
    strict: bool
    family: str | None  # None when the publisher maps to no family


class _HeaderSource(Protocol):
    def headers(self) -> Mapping[str, _Secret]:
        """The headers that authorize one request, fetched for it."""
        ...


class _KeyHeaders:
    """A static key's headers, ``Authorization: Bearer <key>``, or none: the same for every request."""

    __slots__ = ("_bearer",)

    def __init__(self, key: _Secret | None) -> None:
        self._bearer = None if key is None else key.prefixed("Bearer ")

    def headers(self) -> dict[str, _Secret]:
        """A new mapping of the key's header, empty without a key."""
        return {} if self._bearer is None else {"authorization": self._bearer}

    def __repr__(self) -> str:
        return "None" if self._bearer is None else "<secret>"


def _direct_route(kind: _Kind, model: str, base_url: str, allow_insecure_http: bool) -> _Route:
    """The route to ``{base_url}/chat/completions``: the endpoint checked, the model id bounded (module notes, 3)."""
    endpoint = check_endpoint(base_url, allow_insecure_http=allow_insecure_http)
    model_id = endpoint_model_id(kind, model, endpoint)
    problem = identity_problem(model_id)  # it holds any non-default endpoint: never shorter than rate_key
    if problem is not None:
        raise RationalizationError(f"the provider's model_id {problem}: shorten the model name or the endpoint URL")
    return _Route(
        url=f"{endpoint}/chat/completions",
        model_id=model_id,
        rate_key=endpoint,
        provider_kind="openai-compatible",
        body_model=model,
        wire="chat",
        fine_tuned=model.startswith(FINE_TUNED_PREFIX),
    )


def _dedicated_route(
    target: GoogleCloudTarget, model: object, endpoint_id: str, deployed_model: str | None
) -> _GoogleRoute:
    """A dedicated endpoint's route: its deployed model is named by the endpoint, so the body names none; a
    ``deployed_model`` label joins the model id (never the rate key, which is the endpoint's)."""
    if model is not None:
        raise RationalizationError(
            "a dedicated endpoint serves the model deployed on it, named by endpoint_id: pass model=None"
        )
    endpoint = dedicated_model_id(target, endpoint_id)
    model_id = endpoint
    if deployed_model is not None:
        model_id = f"{endpoint}#model={check_model_segment(deployed_model, name='deployed_model')}"
        problem = identity_problem(model_id)
        if problem is not None:
            raise RationalizationError(f"the provider's model_id {problem}: shorten deployed_model")
    route = _Route(
        url=f"{target.endpoint_base_url(endpoint_id)}/chat/completions",
        model_id=model_id,
        rate_key=endpoint,
        provider_kind="google-cloud-endpoint",
        body_model=None,
        wire="chat",
        fine_tuned=False,
    )
    return _GoogleRoute(route, "chatCompletions", False, None)


def _google_route(
    target: GoogleCloudTarget, model: object, endpoint_id: str | None, deployed_model: str | None
) -> _GoogleRoute:
    """The route of a Google Cloud model (module notes, 7): a dedicated endpoint, Mistral's rawPredict, or the
    shared OpenAI-compatible endpoint. The rate key is the model id: Vertex quotas are per model."""
    if endpoint_id is not None:
        return _dedicated_route(target, model, endpoint_id, deployed_model)
    if deployed_model is not None:
        raise RationalizationError("deployed_model labels the model on a dedicated endpoint: pass endpoint_id too")
    publisher, name = split_publisher_model(model)
    other = _OTHER_INTERFACES.get(publisher)
    if other is not None:
        raise RationalizationError(other)
    mapped = family_for_publisher(publisher)
    family = None if mapped == UNKNOWN_FAMILY else mapped
    model_id = model_id_for(target, publisher, name)
    if publisher == MISTRAL_PUBLISHER:
        url = target.publisher_model_url(publisher, name, "rawPredict")
        bare = name.partition("@")[0]  # the URL keeps the @version; the body's model leaves it out
        route = _Route(url, model_id, model_id, "google-cloud-mistral", bare, "mistral", False)
        return _GoogleRoute(route, "rawPredict", False, family)
    url = f"{target.openai_base_url()}/chat/completions"
    route = _Route(url, model_id, model_id, "google-cloud-openai", f"{publisher}/{name}", "chat", False)
    return _GoogleRoute(route, "chatCompletions", True, family)


def _google_family(google: _GoogleRoute, family: object) -> object:
    """The family of a Google Cloud model: its publisher's, which a declared family must match, else the declared
    one, else ``"unknown"`` (``_settings`` checks it)."""
    if family is None:
        return UNKNOWN_FAMILY if google.family is None else google.family
    if google.family is not None and family != google.family:
        raise RationalizationError(
            f"family disagrees with the model's publisher, whose models are of family {google.family}: leave it out"
        )
    return family


def _messages(request: ModelRequest, *, fold: bool) -> list[dict[str, str]]:
    """A ``system`` turn, then the ``user`` turn (module notes, 1 and 7); ``fold`` (a private hook, never set) puts
    the system prompt first in the one user turn instead."""
    if fold:
        return [{"role": "user", "content": f"{request.system_prompt}\n\n{request.user_envelope}"}]
    return [
        {"role": "system", "content": request.system_prompt},
        {"role": "user", "content": request.user_envelope},
    ]


def _response_format(request: ModelRequest, strict: bool) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {"name": SCHEMA_NAME, "schema": dict(request.json_schema), "strict": strict},
    }


# ---------------------------------------------------------------------------------------------------------------
# The provider
# ---------------------------------------------------------------------------------------------------------------


class OpenAICompatibleProvider:
    """A Chat Completions endpoint as a model provider (module notes).

    Args:
        model: The model name sent in the body (``meta-llama/Llama-3-8b`` may hold ``/`` and ``:``).
        base_url: The endpoint, ending before ``/chat/completions`` (``https://host/v1``); checked and normalized.
        api_key: The key sent as ``Authorization: Bearer <key>``, or ``None`` to send none; never read from the
            environment here.
        family: The model's vendor lineage (``"meta"``, ``"mistral"``, ``"qwen"``, ...): 1-32 of ``a-z``, ``0-9``
            and ``-``; ``"unknown"`` adds no family diversity to consensus.
        temperature: A number in 0-2 (0 by default), or ``None`` to leave it out of the body.
        seed: ``None`` (left out) or a signed 64-bit integer.
        max_tokens_field: The body field for the output limit: ``"max_tokens"`` (compatible servers) or
            ``"max_completion_tokens"`` (OpenAI).
        strict: Send ``strict: true`` and transform the schema to OpenAI's strict subset, within its limits.
        allow_insecure_http: Allow ``http`` to a host that is not loopback: documents and the key then travel
            unencrypted.
        max_connections: The HTTP helper's pool size, 1-1,024.
        transport: An ``httpx.BaseTransport`` to use instead of the network (tests), checked by the HTTP helper.

    Raises:
        RationalizationError: If an argument breaks its rule (the message names it and never echoes a key), or the
            model id would pass 512 characters.
    """

    __slots__ = ("_auth", "_family", "_generation", "_http", "_max_connections", "_route")

    def __init__(
        self,
        model: str,
        *,
        base_url: str,
        api_key: str | None = None,
        family: str = UNKNOWN_FAMILY,
        temperature: float | None = 0.0,
        seed: int | None = 0,
        max_tokens_field: MaxTokensField = "max_tokens",
        strict: bool = True,
        allow_insecure_http: bool = False,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        transport: object = None,
    ) -> None:
        key, problem = _wrap_key(api_key)
        del api_key  # no frame that raises below holds the key as text
        if problem is not None:
            raise RationalizationError(problem)
        settings = _settings(
            family=family,
            temperature=temperature,
            seed=seed,
            max_tokens_field=max_tokens_field,
            strict=strict,
            allow_insecure_http=allow_insecure_http,
            max_connections=max_connections,
            transport=transport,
        )
        route = _direct_route("openai-compatible", model, base_url, settings.allow_insecure_http)
        self._configure(route, settings, _KeyHeaders(key))

    def _configure(self, route: _Route, settings: _Settings, auth: _HeaderSource) -> None:
        """Set every field (the constructor and the factories), the HTTP helper last checked."""
        self._http = _http_client(settings)
        self._route = route
        self._family = settings.family
        self._generation = settings.generation
        self._max_connections = settings.max_connections
        self._auth = auth

    @classmethod
    def _build(
        cls, kind: _Kind, model: str, base_url: str, key: _Secret | None, settings: _Settings
    ) -> "OpenAICompatibleProvider":
        """A provider for a direct endpoint (the factories)."""
        provider = cls.__new__(cls)
        provider._configure(
            _direct_route(kind, model, base_url, settings.allow_insecure_http), settings, _KeyHeaders(key)
        )
        return provider

    @classmethod
    def _openai(cls, model: str, key: _Secret | None, settings: _Settings) -> "OpenAICompatibleProvider":
        if key is None:
            raise RationalizationError(
                f"OpenAI needs an API key: set the {OPENAI_KEY_VARIABLE} environment variable or pass api_key="
            )
        return cls._build("openai", model, OPENAI_BASE_URL, key, settings)

    @classmethod
    def openai(
        cls,
        model: str,
        *,
        api_key: str | None = None,
        temperature: float | None = None,
        seed: int | None = 0,
        max_tokens_field: MaxTokensField = "max_completion_tokens",
        strict: bool = True,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        transport: object = None,
    ) -> "OpenAICompatibleProvider":
        """OpenAI itself: :data:`~datagrunt.rationalization.providers.base.OPENAI_BASE_URL`, model id
        ``openai:<model>``, family ``"openai"``.

        Args:
            model: The OpenAI model (``gpt-4o``, or a fine-tuned ``ft:...``).
            api_key: The key; when ``None``, the ``OPENAI_API_KEY`` environment variable (an empty key is refused,
                never replaced by the variable).
            temperature, seed, max_tokens_field, strict, max_connections, transport: As for the constructor;
                ``max_tokens_field`` defaults to ``"max_completion_tokens"`` (OpenAI deprecates ``max_tokens``,
                which o-series models refuse).

        Raises:
            RationalizationError: If there is no key (naming ``OPENAI_API_KEY``, never a value), or an argument
                breaks its rule.
        """
        explicit = api_key is not None
        key, problem = _wrap_key(api_key)
        del api_key  # no frame that raises below holds the key as text
        if problem is not None:
            raise RationalizationError(problem)
        settings = _settings(
            family=OPENAI_FAMILY,
            temperature=temperature,
            seed=seed,
            max_tokens_field=max_tokens_field,
            strict=strict,
            allow_insecure_http=False,
            max_connections=max_connections,
            transport=transport,
        )
        return cls._openai(model, key if explicit else api_key_from_env(OPENAI_KEY_VARIABLE), settings)

    @classmethod
    def ollama(
        cls,
        model: str,
        *,
        base_url: str = OLLAMA_BASE_URL,
        family: str = UNKNOWN_FAMILY,
        temperature: float | None = 0.0,
        seed: int | None = 0,
        max_tokens_field: MaxTokensField = "max_tokens",
        strict: bool = True,
        allow_insecure_http: bool = False,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        transport: object = None,
    ) -> "OpenAICompatibleProvider":
        """An Ollama server's OpenAI-compatible endpoint, sent no key: model id ``ollama:<model>`` at the default
        :data:`~datagrunt.rationalization.providers.base.OLLAMA_BASE_URL`, ``ollama:<model>@<url>`` elsewhere.

        Raises:
            RationalizationError: If an argument breaks its rule (``http`` to a host that is not loopback needs
                ``allow_insecure_http=True``).
        """
        settings = _settings(
            family=family,
            temperature=temperature,
            seed=seed,
            max_tokens_field=max_tokens_field,
            strict=strict,
            allow_insecure_http=allow_insecure_http,
            max_connections=max_connections,
            transport=transport,
        )
        return cls._build("ollama", model, base_url, None, settings)

    @classmethod
    def google_cloud(
        cls,
        model: str | None,
        *,
        project: str,
        location: str,
        auth: GoogleCloudAuth,
        endpoint_id: str | None = None,
        deployed_model: str | None = None,
        family: str | None = None,
        strict: bool | None = None,
        temperature: float | None = 0.0,
        seed: int | None = 0,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        transport: object = None,
    ) -> "OpenAICompatibleProvider":
        """An open or partner model on Google Cloud (Vertex AI, now Agent Platform), or a dedicated endpoint
        (module notes, 7).

        Args:
            model: ``publisher/model`` (``meta/llama-4-maverick-17b-128e-instruct-maas``,
                ``mistralai/mistral-small-2503@001``); ``None`` with ``endpoint_id``.
            project: The Google Cloud project.
            location: The region (``us-central1``), a multi-region (``us``, ``eu``) or ``global``.
            auth: A :class:`~datagrunt.rationalization.providers.google_cloud.GoogleCloudAuth` with a token source:
                these interfaces take no API key. The token is fetched once per request.
            endpoint_id: A dedicated endpoint serving a self-deployed model, instead of a publisher model.
            deployed_model: A label for the model deployed on ``endpoint_id`` (one path segment, ``@version``
                allowed), joined to the model id so that a redeploy re-keys the cache; never sent. Without it, a
                new model deployed behind the same endpoint id replays the old model's cached answers.
            family: The model's family: a publisher's own, which must match it, or one to declare for a publisher
                with none (and a dedicated endpoint); ``"unknown"`` by default.
            strict: ``None`` for the path's default (module notes, 7), else as for the constructor.
            temperature, seed, max_connections, transport: As for the constructor (temperature 0 by default;
                ``seed`` goes to Mistral as ``random_seed``).

        Raises:
            RationalizationError: If ``auth`` is not a ``GoogleCloudAuth`` or holds an API key, the project, the
                location, the model or the endpoint ID breaks its rule, the publisher is served by another adapter
                or none, a model is given with ``endpoint_id``, or another argument breaks its rule; no message
                quotes a credential.
        """
        if not isinstance(auth, GoogleCloudAuth):
            kind = type(auth).__name__
            del auth  # a key passed here by mistake leaves this frame before it can raise
            raise RationalizationError(f"auth must be a GoogleCloudAuth (token=...), got {kind}")
        target = GoogleCloudTarget(project, location)
        google = _google_route(target, model, endpoint_id, deployed_model)
        check_access(target, auth, interface=google.interface)
        settings = _settings(
            family=_google_family(google, family),
            temperature=temperature,
            seed=seed,
            max_tokens_field=GOOGLE_MAX_TOKENS_FIELD,
            strict=google.strict if strict is None else strict,
            allow_insecure_http=False,
            max_connections=max_connections,
            transport=transport,
        )
        provider = cls.__new__(cls)
        provider._configure(google.route, settings, auth)
        return provider

    @classmethod
    def from_model_string(cls, model_string: ModelString, **options: Any) -> "OpenAICompatibleProvider":
        """The provider a parsed model string names: ``openai`` → :meth:`openai`, ``ollama`` → :meth:`ollama` at the
        string's endpoint, ``openai-compatible`` → the constructor at its endpoint (module notes, 6).

        Args:
            model_string: A :class:`~datagrunt.rationalization.providers.base.ModelString`.
            options: Exactly the options of the factory the kind names, with its defaults.

        Raises:
            TypeError: If ``model_string`` is not a ``ModelString``.
            RationalizationError: If its kind is another vendor's, an option is unknown to its factory (naming the
                option, never its value), an ``openai`` string names an endpoint other than OpenAI's, or the factory
                refuses an argument.
        """
        has_key = "api_key" in options
        key, key_problem = _wrap_key(options.pop("api_key", None))
        if not isinstance(model_string, ModelString):
            options.clear()  # an unknown option may hold a key: no frame that raises keeps it
            raise TypeError(f"from_model_string needs a ModelString, got {type(model_string).__name__}")
        problem = _option_problem(model_string.kind, options, has_key) or key_problem
        if problem is not None:
            options.clear()
            raise RationalizationError(problem)
        settings = _settings(**{**_KIND_DEFAULTS[model_string.kind], **options})
        return cls._from_parsed(model_string, key, has_key, settings)

    @classmethod
    def _from_parsed(
        cls, model_string: ModelString, key: _Secret | None, has_key: bool, settings: _Settings
    ) -> "OpenAICompatibleProvider":
        """The provider of a model string whose options are checked."""
        kind, model, base_url = model_string.kind, model_string.model, model_string.base_url
        if kind == "openai":
            if base_url not in (None, OPENAI_BASE_URL):
                raise RationalizationError(
                    "an openai model names OpenAI's endpoint: use openai-compatible:<model>@<url>"
                )
            return cls._openai(model, key if has_key else api_key_from_env(OPENAI_KEY_VARIABLE), settings)
        if kind == "ollama":
            return cls._build("ollama", model, base_url or OLLAMA_BASE_URL, None, settings)
        return cls._build("openai-compatible", model, cast(str, base_url), key, settings)  # ModelString requires it

    @property
    def model_id(self) -> str:
        """The provider-qualified model id, naming the endpoint (module notes, 3)."""
        return self._route.model_id

    @property
    def provider_kind(self) -> str:
        """``"openai-compatible"`` for a direct endpoint, else the Google Cloud interface (module notes, 3 and 7)."""
        return self._route.provider_kind

    @property
    def rate_key(self) -> str:
        """The normalized endpoint (on Google Cloud, the host and project): one rate limit per endpoint."""
        return self._route.rate_key

    @property
    def model_family(self) -> str:
        """The model's vendor lineage: ``"openai"`` for OpenAI, else the declared family (``"unknown"``)."""
        return self._family

    @property
    def max_connections(self) -> int:
        """The HTTP helper's pool size: the runner sizes its per-provider concurrency to fit it."""
        return self._max_connections

    @property
    def generation_params(self) -> dict[str, Any]:
        """A new mapping of the wire format, ``temperature``, ``seed``, ``max_tokens_field`` and ``strict``,
        ``None`` values left out."""
        return {"wire": _WIRE_FORMATS[self._route.wire], **self._generation.params()}

    def transform_schema(self, schema: Mapping[str, Any]) -> dict[str, Any]:
        """Return the schema to send: :func:`strict_schema` of it with ``strict=True``, else a fresh equal copy.

        Raises:
            TypeError: If ``schema`` is not a ``Mapping``.
            RationalizationError: If it is not exactly JSON, or (strict) cannot be expressed in the strict subset or
                passes OpenAI's limits.
        """
        if self._generation.strict:
            return strict_schema(schema, fine_tuned=self._route.fine_tuned)
        return _json_copy(schema)

    def open(self) -> None:
        """Build the HTTP clients now, on the caller's thread (the runner calls it at preflight).

        Raises:
            RationalizationError: If the environment prevents a client (a bad proxy or certificate variable).
        """
        self._http.open()

    def generate(self, request: ModelRequest) -> ProviderResponse:
        """Send one request and map the reply (module notes, 1 and 5).

        Raises:
            TransientProviderError: On a retryable status, a timeout, a transport failure or the deadline.
            PermanentProviderError: On any other HTTP failure.
            TypeError: If ``request`` is not a ``ModelRequest``.
        """
        if not isinstance(request, ModelRequest):
            raise TypeError(f"generate needs a ModelRequest, got {type(request).__name__}")
        deadline = time.monotonic() + request.timeout_s  # set first: a token fetched below spends the request's time
        reply = self._http.post_json(
            self._route.url,
            headers=self._auth.headers(),
            body=self._body(request),
            timeout_s=request.timeout_s,
            deadline=deadline,
        )
        return reply_response(reply)

    def _body(self, request: ModelRequest) -> dict[str, Any]:
        """The request body for ``request`` (module notes, 1 and 7)."""
        if self._route.wire == "mistral":
            return self._mistral_body(request)
        return self._chat_body(request)

    def _chat_body(self, request: ModelRequest) -> dict[str, Any]:
        """The Chat Completions body (module notes, 1); a dedicated endpoint's names no model."""
        generation = self._generation
        body: dict[str, Any] = {} if self._route.body_model is None else {"model": self._route.body_model}
        body["messages"] = _messages(request, fold=self._route.fold_system)
        body["response_format"] = _response_format(request, generation.strict)
        body[generation.max_tokens_field] = request.max_output_tokens
        if generation.temperature is not None:
            body["temperature"] = generation.temperature
        if generation.seed is not None:
            body["seed"] = generation.seed
        return body

    def _mistral_body(self, request: ModelRequest) -> dict[str, Any]:
        """The Mistral ``rawPredict`` body (module notes, 7)."""
        generation = self._generation
        body: dict[str, Any] = {
            "model": self._route.body_model,
            "messages": _messages(request, fold=self._route.fold_system),
            "response_format": _response_format(request, generation.strict),
            generation.max_tokens_field: request.max_output_tokens,
            "stream": False,
        }
        if generation.temperature is not None:
            body["temperature"] = generation.temperature
        if generation.seed is not None:
            body["random_seed"] = generation.seed
        return body

    def __repr__(self) -> str:
        params = ", ".join(f"{name}={value!r}" for name, value in self.generation_params.items())
        identity = f"model_id={self.model_id!r}, family={self._family!r}"
        return f"OpenAICompatibleProvider({identity}, {params}, auth={self._auth!r})"
