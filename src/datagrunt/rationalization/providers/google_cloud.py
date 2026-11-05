"""Google Cloud for every adapter: hosts, auth, URLs, identity and model families (plan §4.5, R3, T8a).

The Gemini, Anthropic and OpenAI-compatible adapters reach any model Google Cloud serves, first party (Gemini, Gemma)
or third party (Model Garden partner and open models), through this module. It builds text only: it sends nothing,
imports no HTTP stack and holds no state but a credential. The rules:

1. **Where requests go.** A :class:`GoogleCloudTarget` is a project and a location (both
   :func:`~datagrunt.rationalization.providers.base.check_gcp_label`-checked), whose host :func:`host_for` derives:
   ``global`` → ``aiplatform.googleapis.com``; the multi-regions ``us`` and ``eu`` →
   ``aiplatform.{us|eu}.rep.googleapis.com``; any other location → ``{location}-aiplatform.googleapis.com``. Express
   mode (:meth:`GoogleCloudTarget.express`) has no project and the global host. Its URL builders put the publisher,
   the method and an endpoint ID through :func:`~datagrunt.rationalization.providers.base.check_path_segment`, and
   the model through :func:`check_model_segment` (one path segment that may end in one ``@version``, as older
   Claude IDs, ``claude-opus-4-5@20251101``, and ``@001`` versions do), and allow only the documented methods
   (:data:`PUBLISHER_METHODS`, :data:`EXPRESS_METHODS`), so nothing a caller passes moves a request to another
   host or another resource. The API version is :data:`API_VERSION` everywhere.
2. **Auth.** :class:`GoogleCloudAuth` holds exactly one credential: an API key (a
   :class:`~datagrunt.rationalization.providers.base._Secret`, sent as the ``x-goog-api-key`` header, never in a
   URL) or a token source (a zero-argument callable, called once per :meth:`GoogleCloudAuth.headers` call and never
   at construction, sent as ``Authorization: Bearer``). A token must be an exact ``str`` of 1 to
   :data:`TOKEN_MAX_CHARS` printable ASCII characters without whitespace and without its own ``Bearer`` scheme. A
   token source signals a failure worth a retry (a credential refresh that may work next time) by raising
   :class:`~datagrunt.rationalization.providers.base.TransientProviderError`: the request then fails with a fresh
   ``TransientProviderError(None, None, "the Google Cloud token provider failed transiently")``, which the runner
   retries. A token source that raises anything else (``Exception``; anything else propagates), or returns anything
   but a token, fails the request with a :class:`~datagrunt.rationalization.providers.base.PermanentProviderError`
   naming the exception's class or the value's type. Neither carries the source's message, detail or value, and both
   are raised outside the ``except`` so nothing chains. A token source must return promptly (a cached token, refreshed
   ahead of time): it runs on a worker thread before the request is sent, and the request's deadline bounds the HTTP
   exchange, not a Python call, so a hung source holds its worker until it returns. The auth object never
   pickles or copies, its ``repr`` shows only its kind, and no frame of this module that raises holds a key or a
   token (a log with ``capture_locals`` prints them). :func:`check_access` refuses, before any request, the pairs
   Google does not document: an API key anywhere but ``generateContent`` / ``streamGenerateContent``, and express
   mode with a token or with any other interface.
3. **Identity.** :func:`model_id_for` is ``google-cloud:`` + the model's resource name, exactly the path the URL
   calls (``google-cloud:projects/{p}/locations/{l}/publishers/{publisher}/models/{model}``; express
   ``google-cloud:publishers/google/models/{model}``; a dedicated endpoint, :func:`dedicated_model_id`,
   ``google-cloud:projects/{p}/locations/{l}/endpoints/{id}``). Resource names are unique and no segment holds a
   ``/``, so two models, projects, locations or publishers never share a model id. The model id names the model, not
   the interface: an adapter's ``provider_kind`` tells a model's interfaces apart. :func:`rate_key_for` is the host
   and ``/projects/{p}`` (``/express`` in express mode). Each identity passes
   :func:`~datagrunt.rationalization.providers.base.identity_problem`.
4. **Families.** :func:`family_for_publisher` maps a publisher to its model's vendor lineage
   (:data:`PUBLISHER_FAMILIES`), ``"unknown"`` for any publisher not in the table.

Research, read on 2026-11-05 from Google's documentation (every claim below cites a page listed after the table):

==============  ====================================================  ============  ====================  ==========
Publisher       Interface (``…/projects/{p}/locations/{l}/…``)         Auth          Structured output     Family
==============  ====================================================  ============  ====================  ==========
google          ``publishers/google/models/{m}:generateContent``      key or token  ``responseSchema``    google
                (also express: ``/v1/publishers/google/models/…``)    (express:                           (Gemma too)
                                                                      key only)
anthropic       ``publishers/anthropic/models/{m}:rawPredict``,       token         ``output_config.``    anthropic
                Anthropic Messages body, ``anthropic_version``                      ``format``
                ``vertex-2023-10-16``, no ``model`` in the body                     json_schema (4.5+;
                                                                                    org policy gated)
mistralai       ``publishers/mistralai/models/{m}:rawPredict``,       token         not documented        mistral
                Mistral body (``model`` in it, no ``@`` version)
ai21            ``publishers/ai21/models/{m}:rawPredict`` (assumed;   token         not documented        ai21
                no request page found)
xai             ``endpoints/openapi/chat/completions`` (and           token         Responses API         xai
                ``…/responses``), model ``xai/{m}``                                 ``text.format``
                                                                                    json_schema (chat:
                                                                                    JSON mode only)
meta, deepseek  ``endpoints/openapi/chat/completions``, model         token         ``response_format``   meta,
-ai, qwen,      ``{publisher}/{m}`` (``meta/llama-4-…-maas``,         (OpenAI       (all MaaS open        deepseek,
openai,         ``deepseek-ai/deepseek-v3.2-maas``,                   library:      models; json_schema   qwen,
moonshotai,     ``qwen/qwen3-next-…-maas``,                           Google        enforcement per       openai,
minimaxai,      ``openai/gpt-oss-120b-maas``,                         Cloud auth    model not stated)     moonshot,
zai-org,        ``minimaxai/minimax-m2-maas``,                        only)                               minimax,
google (gemma)  ``zai-org/glm-5.2-maas``, ``google/gemma-…-maas``)                                       zai, google
google (gemini  ``endpoints/openapi/chat/completions``, model         token         ``response_format``   google
via OpenAI)     ``google/gemini-…``                                                 json_schema
self-deployed   ``endpoints/{id}/chat/completions`` (vLLM, HF TGI     token         per container         declared
                containers; not every model)                                                              by caller
==============  ====================================================  ============  ====================  ==========

- Name: Vertex AI is now Agent Platform, part of Gemini Enterprise Agent Platform; its API is the "Gemini Enterprise
  Agent Platform API". The REST service is still ``aiplatform.googleapis.com`` (the reference and the release notes,
  which still send requests "directly to ``aiplatform.googleapis.com``" on 2026-11-02).
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/vertex-ai-name-changes,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/release-notes,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rest
- Hosts: the global, multi-region (``https://aiplatform.us.rep.googleapis.com``,
  ``https://aiplatform.eu.rep.googleapis.com``; locations ``us`` / ``eu``; "Explicitly use the .rep. hostname for
  multi-region endpoints") and regional forms; no other form is documented. Claude, Mistral, Grok and the open models
  are listed on the global, multi-region and regional endpoints; the global endpoint gives no data residency.
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/resources/locations,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/resources/data-residency
- API keys: Agent Platform supports API keys for ``generateContent`` and ``streamGenerateContent`` only; keys bound
  to a service account ("authorization keys") are supported by ``aiplatform.googleapis.com`` (blocked by a default
  organization policy). A key goes in the ``x-goog-api-key`` header (the ``key`` query parameter is the fallback
  Google warns exposes the key; datagrunt never uses it). The suggested variable is ``GOOGLE_API_KEY``
  (:data:`API_KEY_ENV`). Express mode: Gemini models only, ``/v1/publishers/google/models/{m}:{method}`` on the global
  host with no project or location, methods ``generateContent``, ``streamGenerateContent``, ``countTokens``
  (Preview). https://docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/authentication,
  https://docs.cloud.google.com/docs/authentication/api-keys,
  https://docs.cloud.google.com/docs/authentication/api-keys-use,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/start/api-keys,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/express-mode/api-reference,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/start/express-mode/overview
- Tokens: every REST example sends ``Authorization: Bearer $(gcloud auth print-access-token)``; access tokens last
  an hour by default and must be refreshed (so the caller passes a source, not a token). The OpenAI-compatible page:
  "Only Google Cloud Auth is supported using the OpenAI library".
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/start/openai,
  https://docs.cloud.google.com/vertex-ai/generative-ai/docs/migrate/openai/auth-and-credentials
- Claude: ``rawPredict`` / ``streamRawPredict`` on ``publishers/anthropic``, ``anthropic_version`` must be
  ``vertex-2023-10-16`` (:data:`ANTHROPIC_VERSION`) and the model goes in the URL, not the body; structured outputs
  for Claude 4.5 and later, off by default until the ``constraints/vertexai.allowedPartnerModelFeatures`` policy
  allows ``structured_outputs``. Older Claude IDs carry an ``@`` version (``claude-opus-4-5@20251101``); current
  ones are bare (``claude-opus-4-8``); the examples' bodies carry no ``model``.
  https://docs.cloud.google.com/vertex-ai/generative-ai/docs/partner-models/claude/use-claude,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/claude/structured-outputs
- Partners: Anthropic, xAI (Grok: OpenAI-compatible chat completions and Responses API; schema-enforced output is
  documented for the Responses API only), AI21 (Jamba, Preview), Mistral (``rawPredict``; body holds ``model``
  without the ``@`` version), Meta.
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/use-partner-models,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/grok/capabilities/structured-output,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/mistral
- Open models (MaaS): ``POST …/endpoints/openapi/chat/completions`` with ``model`` ``{publisher}/{model}``; the
  thinking guide's requests name ``deepseek-ai/``, ``moonshot-ai/`` (its reply says ``moonshotai/``), ``qwen/``,
  ``openai/``, ``minimaxai/``, ``zai-org/`` and ``google/`` models; all MaaS open models support structured output
  (the page shows ``response_format``; JSON mode alone enforces no schema). Llama's page uses ``v1beta1``, the MaaS
  and OpenAI pages ``v1``; the REST reference lists chat completions under ``v1beta1`` only, the guides call ``v1``.
  https://docs.cloud.google.com/vertex-ai/generative-ai/docs/maas/call-open-model-apis,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/maas/capabilities/thinking,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/maas/capabilities/structured-output,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/maas/use-open-models,
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/llama/use-llama
- OpenAI-compatible endpoint for Gemini and dedicated endpoints: base ``…/endpoints/openapi`` (Gemini as
  ``google/gemini-…``) or ``…/endpoints/{ENDPOINT_ID}`` (self-deployed vLLM / HF TGI models; not every model);
  ``response_format`` ``json_schema`` is supported for Google models ("Fully recursive schemas are not supported").
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/migrate/openai/overview,
  https://docs.cloud.google.com/vertex-ai/generative-ai/docs/migrate/openai/examples
- Gemini structured output: ``generationConfig.responseSchema`` with ``responseMimeType`` ``application/json``.
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/capabilities/control-generated-output

A model ID's ``@version`` stays in the URL path, where the endpoint check allows ``@`` (only the authority refuses
it, so the host cannot move); an ``openai/…`` style body name may carry it too (:func:`split_publisher_model`).

Bounds: every check is linear in its input, itself bounded (a label, a path segment, a token of at most
:data:`TOKEN_MAX_CHARS` characters); nothing loops but over fixed tables, nothing recurses. The module imports the
standard library, :mod:`~datagrunt.rationalization.providers.base` and the package's exception only: no httpx,
nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, NoReturn, TypeAlias, TypeGuard

from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.base import (
    MODEL_NAME_MAX_CHARS,
    UNKNOWN_FAMILY,
    PermanentProviderError,
    TransientProviderError,
    _Secret,
    api_key_from_env,
    check_gcp_label,
    check_path_segment,
    identity_problem,
)

API_VERSION = "v1"
"""The API version every URL names (the guides call ``v1`` for every interface used here)."""

GLOBAL_LOCATION = "global"
GLOBAL_HOST = "aiplatform.googleapis.com"
"""The host of the ``global`` location and of express mode."""

MULTI_REGION_HOSTS: Mapping[str, str] = MappingProxyType(
    {"us": "aiplatform.us.rep.googleapis.com", "eu": "aiplatform.eu.rep.googleapis.com"}
)
"""The multi-region locations and their hosts."""

REGIONAL_HOST_SUFFIX = "-aiplatform.googleapis.com"
"""What follows the location in a regional host."""

API_KEY_ENV = "GOOGLE_API_KEY"
"""The variable :meth:`GoogleCloudAuth.from_env` reads by default: the one Google's API-key page suggests."""

API_KEY_HEADER = "x-goog-api-key"
"""The header an API key travels in."""

OPENAI_ENDPOINT_ID = "openapi"
"""The shared endpoint that serves the OpenAI-compatible Chat Completions API."""

ANTHROPIC_VERSION = "vertex-2023-10-16"
"""The ``anthropic_version`` Google requires in the body of a Claude request."""

TOKEN_MAX_CHARS = 16_384
"""The longest access token sent (a datagrunt bound on one header, not a Google limit)."""

MODEL_ID_PREFIX = "google-cloud:"
"""What every Google Cloud model id starts with."""

PUBLISHER_METHODS = frozenset(
    {
        "computeTokens",
        "countTokens",
        "embedContent",
        "generateContent",
        "predict",
        "rawPredict",
        "serverStreamingPredict",
        "streamGenerateContent",
        "streamRawPredict",
    }
)
"""The methods the v1 REST reference documents on ``projects.locations.publishers.models``."""

EXPRESS_METHODS = frozenset({"countTokens", "generateContent", "streamGenerateContent"})
"""The methods express mode serves."""

Interface: TypeAlias = Literal[
    "generateContent", "streamGenerateContent", "rawPredict", "streamRawPredict", "chatCompletions"
]
"""How an adapter talks to a model: a publisher-model method, or the OpenAI-compatible Chat Completions API."""

INTERFACES: frozenset[str] = frozenset(
    {"generateContent", "streamGenerateContent", "rawPredict", "streamRawPredict", "chatCompletions"}
)

API_KEY_INTERFACES: frozenset[str] = frozenset({"generateContent", "streamGenerateContent"})
"""The interfaces Google documents an API key for."""

PUBLISHER_FAMILIES: Mapping[str, str] = MappingProxyType(
    {
        "google": "google",
        "anthropic": "anthropic",
        "meta": "meta",
        "mistralai": "mistral",
        "ai21": "ai21",
        "xai": "xai",
        "deepseek-ai": "deepseek",
        "qwen": "qwen",
        "openai": "openai",
        "moonshotai": "moonshot",
        "moonshot-ai": "moonshot",  # the thinking guide's requests spell it so; its replies say moonshotai
        "minimaxai": "minimax",
        "zai-org": "zai",
    }
)
"""A publisher's model family (its vendor lineage); any other publisher is ``"unknown"``."""

AuthKind: TypeAlias = Literal["api_key", "token"]
TokenSource: TypeAlias = Callable[[], object]
"""A token source: no arguments, the current OAuth access token (a ``str``) back."""

_MODEL_SEGMENT = re.compile(r"[A-Za-z0-9._-]+(?:@[A-Za-z0-9._-]+)?")
_DOT_SEGMENTS = frozenset({".", ".."})
_BEARER = "bearer "
_TOKEN_TEXT = re.compile(r"[\x21-\x7e]+")
_TOKEN_FAILED = "the Google Cloud token provider failed"
_TOKEN_TRANSIENT = "the Google Cloud token provider failed transiently"
_KEY_RULE = "the Google Cloud API key must be non-blank printable ASCII without leading or trailing whitespace"


# ---------------------------------------------------------------------------------------------------------------
# Hosts and targets (module notes, 1)
# ---------------------------------------------------------------------------------------------------------------


def host_for(location: str) -> str:
    """The host that serves ``location`` (module notes, 1).

    Raises:
        RationalizationError: If ``location`` is not a Google Cloud location (a lower-case label of at most 52
            characters); the message names the field, never the value.
    """
    place = check_gcp_label(location, field="location")
    if place == GLOBAL_LOCATION:
        return GLOBAL_HOST
    multi = MULTI_REGION_HOSTS.get(place)
    if multi is not None:
        return multi
    return f"{place}{REGIONAL_HOST_SUFFIX}"


def _segment(value: object, name: str) -> str:
    return check_path_segment(value, name=f"{name} (it goes in the URL path)")


def _is_model_segment(value: object) -> TypeGuard[str]:
    if not isinstance(value, str) or len(value) > MODEL_NAME_MAX_CHARS or not _MODEL_SEGMENT.fullmatch(value):
        return False
    model, _, version = value.partition("@")
    return model not in _DOT_SEGMENTS and version not in _DOT_SEGMENTS


def check_model_segment(value: object, *, name: str) -> str:
    """Require a Google Cloud model ID as one URL path segment that may end in one ``@version`` (module notes, 1).

    ``[A-Za-z0-9._-]+``, optionally ``@`` and a version of the same characters, at most
    :data:`~datagrunt.rationalization.providers.base.MODEL_NAME_MAX_CHARS` characters in all; neither the name nor the
    version is ``.`` or ``..``.

    Raises:
        RationalizationError: If ``value`` is no such segment; the message names ``name``, never the value.
    """
    if not _is_model_segment(value):
        raise RationalizationError(
            f"{name} must be one URL path segment: 1-{MODEL_NAME_MAX_CHARS} letters, digits, '.', '_' or '-', "
            "optionally ending in one '@' and a version of the same characters, and neither part '.' nor '..'"
        )
    return value


def _check_method(method: object, *, express: bool) -> str:
    """A documented method name: one of :data:`PUBLISHER_METHODS`, and of :data:`EXPRESS_METHODS` in express mode."""
    verb = _segment(method, "a Google Cloud method")
    if verb not in PUBLISHER_METHODS:
        raise RationalizationError(
            "the method must be one the REST reference documents for publisher models: "
            + ", ".join(sorted(PUBLISHER_METHODS))
        )
    if express and verb not in EXPRESS_METHODS:
        raise RationalizationError(f"express mode serves only {', '.join(sorted(EXPRESS_METHODS))}")
    return verb


@dataclass(frozen=True, slots=True)
class GoogleCloudTarget:
    """Where Google Cloud requests go: a project, a location and the host they derive (module notes, 1).

    Args:
        project: The project ID, number or ``domain:project-id``; ``None`` for express mode (:meth:`express`).
        location: The region (``us-central1``), a multi-region (``us``, ``eu``) or ``global``; express mode takes
            ``global`` only.

    Raises:
        RationalizationError: If the project or the location breaks its rule (the project is checked first), or
            express mode is asked for another location; no message quotes a value.
    """

    project: str | None
    location: str
    host: str = field(init=False)

    def __post_init__(self) -> None:
        if self.project is None:
            if self.location != GLOBAL_LOCATION:
                raise RationalizationError(
                    "express mode (project=None) uses the global endpoint: location must be 'global'"
                )
        else:
            check_gcp_label(self.project, field="project")
        object.__setattr__(self, "host", host_for(self.location))

    @classmethod
    def express(cls) -> "GoogleCloudTarget":
        """Express mode: an API key, no project, the global host (Gemini models only)."""
        return cls(None, GLOBAL_LOCATION)

    @property
    def is_express(self) -> bool:
        """Whether this is express mode (no project)."""
        return self.project is None

    def _parent(self, what: str) -> str:
        """``projects/{p}/locations/{l}``; express mode has neither, so ``what`` is refused there."""
        if self.project is None:
            raise RationalizationError(
                f"express mode has no {what}: it serves only Google's models through generateContent"
            )
        return f"projects/{self.project}/locations/{self.location}"

    def publisher_model_path(self, publisher: str, model: str) -> str:
        """The model's resource name: ``projects/{p}/locations/{l}/publishers/{publisher}/models/{model}``.

        Express mode: ``publishers/google/models/{model}``.

        Raises:
            RationalizationError: If the publisher or the model is not one path segment, or express mode is asked
                for a publisher other than ``google``.
        """
        source = _segment(publisher, "a Google Cloud publisher")
        name = check_model_segment(model, name="a Google Cloud model name (it goes in the URL path)")
        path = f"publishers/{source}/models/{name}"
        if self.project is None:
            if source != "google":
                raise RationalizationError("express mode serves only Google's models (publisher google)")
            return path
        return f"{self._parent('partner or open models')}/{path}"

    def publisher_model_url(self, publisher: str, model: str, method: str) -> str:
        """``https://{host}/v1/{resource name}:{method}`` (module notes, 1).

        Raises:
            RationalizationError: If a segment is not one path segment, the method is not documented (in express
                mode, not served there), or express mode is asked for another publisher.
        """
        path = self.publisher_model_path(publisher, model)
        verb = _check_method(method, express=self.project is None)
        return f"https://{self.host}/{API_VERSION}/{path}:{verb}"

    def openai_base_url(self) -> str:
        """The OpenAI-compatible base: ``https://{host}/v1/projects/{p}/locations/{l}/endpoints/openapi``.

        Raises:
            RationalizationError: In express mode, which has no such endpoint.
        """
        parent = self._parent("OpenAI-compatible endpoint")
        return f"https://{self.host}/{API_VERSION}/{parent}/endpoints/{OPENAI_ENDPOINT_ID}"

    def endpoint_path(self, endpoint_id: str) -> str:
        """A dedicated endpoint's resource name: ``projects/{p}/locations/{l}/endpoints/{endpoint_id}``.

        Raises:
            RationalizationError: If the ID is not one path segment or is ``openapi`` (the shared endpoint:
                :meth:`openai_base_url`), or in express mode.
        """
        name = _segment(endpoint_id, "a Google Cloud endpoint ID")
        if name == OPENAI_ENDPOINT_ID:
            raise RationalizationError(
                "the openapi endpoint is the shared one, not a dedicated endpoint: use openai_base_url()"
            )
        return f"{self._parent('dedicated endpoints')}/endpoints/{name}"

    def endpoint_base_url(self, endpoint_id: str) -> str:
        """A dedicated endpoint's OpenAI-compatible base: ``https://{host}/v1/projects/{p}/locations/{l}/endpoints/{id}``.

        Raises:
            RationalizationError: As :meth:`endpoint_path`.
        """
        return f"https://{self.host}/{API_VERSION}/{self.endpoint_path(endpoint_id)}"


def split_publisher_model(name: object) -> tuple[str, str]:
    """The publisher and the model of an OpenAI-compatible model name, ``publisher/model`` (module notes, 1).

    Raises:
        RationalizationError: If ``name`` is not a ``str`` with exactly one ``/`` between a publisher (one path
            segment) and a model (:func:`check_model_segment`: it may carry an ``@version``).
    """
    if not isinstance(name, str) or name.count("/") != 1:
        raise RationalizationError(
            "a model on the Google Cloud OpenAI-compatible endpoint is named publisher/model, with one '/' "
            "(meta/llama-4-maverick-17b-128e-instruct-maas, google/gemini-2.5-flash)"
        )
    publisher, model = name.split("/")
    return (
        _segment(publisher, "the publisher of a publisher/model name"),
        check_model_segment(model, name="the model of a publisher/model name"),
    )


# ---------------------------------------------------------------------------------------------------------------
# Auth (module notes, 2)
# ---------------------------------------------------------------------------------------------------------------


def _api_key(given: object) -> tuple[_Secret | None, str]:
    """The key as a ``_Secret``, or ``None`` and the problem: never the key."""
    if isinstance(given, _Secret):
        return given, ""
    if type(given) is not str:
        return None, f"the Google Cloud API key must be a str, got {type(given).__name__}"
    problem = ""
    try:
        return _Secret(given), ""
    except ValueError:  # _Secret's rule: its message holds no key text
        problem = _KEY_RULE
    return None, problem


def _credential(api_key: object, token: object) -> tuple[_Secret | TokenSource | None, str]:
    """Exactly one credential, or ``None`` and the problem; it never raises and never quotes a value."""
    if api_key is not None and token is not None:
        return None, "pass exactly one of api_key=... or token=..., not both"
    if api_key is None and token is None:
        return None, (
            "no Google Cloud credential: pass api_key=... (an API key) or token=... (a callable returning an OAuth "
            "access token)"
        )
    if api_key is not None:
        return _api_key(api_key)
    if not callable(token):
        return None, f"token must be a callable returning the OAuth access token (a str), got {type(token).__name__}"
    return token, ""


def _token_problem(value: object) -> str:
    """Why ``value`` is no token to send (module notes, 2), or ``""``; never quoting it."""
    if type(value) is not str:
        return f"it returned {type(value).__name__}, not a str"
    if value[: len(_BEARER)].lower() == _BEARER:
        return "it returned a value starting with 'Bearer ': return the token alone"
    if len(value) > TOKEN_MAX_CHARS:
        return f"it returned more than {TOKEN_MAX_CHARS} characters"
    if _TOKEN_TEXT.fullmatch(value) is None:
        return "it returned no valid token: 1 or more printable ASCII characters without whitespace"
    return ""


@dataclass(frozen=True, slots=True)
class _TokenFailure:
    """Why no token was fetched: worth a retry (the source raised ``TransientProviderError``) or not."""

    transient: bool
    detail: str


def _token_secret(source: TokenSource) -> _Secret | _TokenFailure:
    """The token ``source()`` returns as a ``_Secret``, or the failure; it never raises for the source's sake."""
    value: object = None
    failure = ""
    transient = False
    try:
        value = source()
    except TransientProviderError:  # the source's own signal of a failure worth a retry; its detail is never kept
        transient = True
    except Exception as error:  # the caller's code: whatever else it raises fails the request, named by class only
        failure = type(error).__name__
    if transient:
        return _TokenFailure(True, _TOKEN_TRANSIENT)
    if not failure:
        failure = _token_problem(value)
    if failure:
        return _TokenFailure(False, f"{_TOKEN_FAILED} ({failure})")
    try:
        return _Secret(str(value))
    except ValueError:  # unreachable: _token_problem's rule is stricter than _Secret's
        return _TokenFailure(False, f"{_TOKEN_FAILED} (it returned no valid token)")


class GoogleCloudAuth:
    """A Google Cloud credential: an API key or an OAuth token source, exactly one (module notes, 2).

    Args:
        api_key: The API key (a standard key, a key bound to a service account, or an express-mode key), or a
            ``_Secret`` holding one. Sent as the ``x-goog-api-key`` header.
        token: Called with no arguments once per :meth:`headers` call, on a worker thread: returns the current OAuth
            access token (``credentials.token`` after a refresh). It must be thread-safe and return promptly (no
            deadline can cut a hung Python call); it raises ``TransientProviderError`` for a failure worth a
            retry (module notes, 2).

    Raises:
        RationalizationError: If neither or both are given, the key is not non-blank printable ASCII without
            leading or trailing whitespace (an exact ``str``), or ``token`` is not callable; never quoting either.
    """

    __slots__ = ("_credential",)

    _credential: "_Secret | TokenSource"

    def __init__(self, *, api_key: "str | _Secret | None" = None, token: TokenSource | None = None) -> None:
        credential, problem = _credential(api_key, token)
        del api_key, token  # a key, or a token passed by mistake, leaves this frame before it can raise
        if credential is None:
            raise RationalizationError(problem)
        object.__setattr__(self, "_credential", credential)

    @classmethod
    def from_env(cls, var: str = API_KEY_ENV) -> "GoogleCloudAuth":
        """The API key in the environment variable ``var`` (default :data:`API_KEY_ENV`).

        Raises:
            TypeError: If ``var`` is not a ``str``.
            ValueError: If ``var`` is blank.
            RationalizationError: If the variable is unset or blank, or holds no valid key; naming the variable,
                never its value.
        """
        key = api_key_from_env(var)
        if key is None:
            raise RationalizationError(f"no Google Cloud API key: set {var}, or pass api_key=... or token=...")
        return cls(api_key=key)

    @property
    def kind(self) -> AuthKind:
        """``"api_key"`` or ``"token"``."""
        return "api_key" if isinstance(self._credential, _Secret) else "token"

    def headers(self) -> dict[str, _Secret]:
        """A new mapping: ``{"x-goog-api-key": key}``, or ``{"authorization": "Bearer <token>"}`` fetched now.

        Raises:
            TransientProviderError: If the token source raises ``TransientProviderError``: a fresh one, without
                the source's status, wait or detail, and not chained.
            PermanentProviderError: If the token source raises anything else (``Exception``; anything else
                propagates) or returns no valid token; the detail names the exception's class or the value's type,
                never a message or a value, and nothing chains.
        """
        if isinstance(self._credential, _Secret):
            return {API_KEY_HEADER: self._credential}
        secret = _token_secret(self._credential)  # no local here holds the source (its repr) or the raw token
        if isinstance(secret, _TokenFailure):
            if secret.transient:
                raise TransientProviderError(None, None, secret.detail)
            raise PermanentProviderError(None, secret.detail)
        return {"authorization": secret.prefixed("Bearer ")}

    def __setattr__(self, name: str, value: object) -> NoReturn:
        raise AttributeError("a GoogleCloudAuth is immutable")

    def __delattr__(self, name: str) -> NoReturn:
        raise AttributeError("a GoogleCloudAuth is immutable")

    def __repr__(self) -> str:  # str() and format() use it too
        try:
            kind: str = self.kind
        except AttributeError:  # not built (__new__ alone)
            kind = "?"
        return f"GoogleCloudAuth(kind={kind!r})"

    def __reduce_ex__(self, protocol: object) -> NoReturn:
        raise TypeError("Google Cloud credentials never pickle or copy")

    def __reduce__(self) -> NoReturn:
        raise TypeError("Google Cloud credentials never pickle or copy")

    def __getstate__(self) -> NoReturn:
        raise TypeError("Google Cloud credentials never give their state")


def _access_problem(target: object, auth: object, interface: object) -> str | None:
    """What Google does not document for this target, auth and interface (module notes, 2), or ``None``."""
    if not isinstance(target, GoogleCloudTarget):
        return f"a Google Cloud target must be a GoogleCloudTarget, got {type(target).__name__}"
    if not isinstance(auth, GoogleCloudAuth):
        return f"Google Cloud auth must be a GoogleCloudAuth, got {type(auth).__name__}"
    if target.is_express:
        if interface not in API_KEY_INTERFACES:
            return f"express mode serves only {' and '.join(sorted(API_KEY_INTERFACES))}, not {interface}"
        if auth.kind != "api_key":
            return "express mode takes an API key (api_key=...): with a token, name the project and location"
        return None
    if auth.kind == "api_key" and interface not in API_KEY_INTERFACES:
        return (
            f"Google Cloud documents API keys only for {' and '.join(sorted(API_KEY_INTERFACES))}, not {interface}: "
            "pass token=... (a callable returning an OAuth access token)"
        )
    return None


def check_access(target: object, auth: object, *, interface: Interface) -> None:
    """Refuse, before any request, a target, auth and interface Google does not document together (module notes, 2).

    Raises:
        ValueError: If ``interface`` is not one of :data:`INTERFACES` (the adapter's mistake).
        RationalizationError: If ``target`` or ``auth`` has the wrong type, an API key meets an interface other
            than :data:`API_KEY_INTERFACES`, or express mode meets a token or another interface.
    """
    problem = _access_problem(target, auth, interface)
    del auth  # a key or token source passed here by mistake leaves this frame before it can raise
    if interface not in INTERFACES:
        raise ValueError(f"interface must be one of {', '.join(sorted(INTERFACES))}")
    if problem is not None:
        raise RationalizationError(problem)


# ---------------------------------------------------------------------------------------------------------------
# Identity (module notes, 3) and families (module notes, 4)
# ---------------------------------------------------------------------------------------------------------------


def _check_target(target: object) -> GoogleCloudTarget:
    if not isinstance(target, GoogleCloudTarget):
        raise RationalizationError(f"a Google Cloud target must be a GoogleCloudTarget, got {type(target).__name__}")
    return target


def _identity(value: str, what: str) -> str:
    problem = identity_problem(value)
    if problem is not None:
        raise RationalizationError(f"the Google Cloud {what} {problem}")
    return value


def model_id_for(target: GoogleCloudTarget, publisher: str, model: str) -> str:
    """``google-cloud:`` + the model's resource name (module notes, 3).

    Raises:
        RationalizationError: If ``target`` is not a :class:`GoogleCloudTarget`, a segment is not one path segment,
            express mode is asked for another publisher, or the id passes the identity bound.
    """
    path = _check_target(target).publisher_model_path(publisher, model)
    return _identity(f"{MODEL_ID_PREFIX}{path}", "model id")


def dedicated_model_id(target: GoogleCloudTarget, endpoint_id: str) -> str:
    """``google-cloud:projects/{p}/locations/{l}/endpoints/{endpoint_id}`` (module notes, 3).

    Raises:
        RationalizationError: As :meth:`GoogleCloudTarget.endpoint_path`, or if ``target`` is not a target or the
            id passes the identity bound.
    """
    path = _check_target(target).endpoint_path(endpoint_id)
    return _identity(f"{MODEL_ID_PREFIX}{path}", "model id")


def rate_key_for(target: GoogleCloudTarget) -> str:
    """The host and ``/projects/{project}`` (``/express`` in express mode) (module notes, 3).

    Raises:
        RationalizationError: If ``target`` is not a :class:`GoogleCloudTarget`, or the key passes the identity
            bound.
    """
    checked = _check_target(target)
    scope = "express" if checked.project is None else f"projects/{checked.project}"
    return _identity(f"{checked.host}/{scope}", "rate key")


def family_for_publisher(publisher: str) -> str:
    """The publisher's model family (:data:`PUBLISHER_FAMILIES`), ``"unknown"`` when it is not in the table.

    Raises:
        RationalizationError: If ``publisher`` is not one path segment.
    """
    name = _segment(publisher, "a Google Cloud publisher")
    return PUBLISHER_FAMILIES.get(name, UNKNOWN_FAMILY)
