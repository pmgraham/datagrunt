"""What every model provider shares: its protocol and errors, model strings, endpoints, API keys (plan §3, §4.5, R3).

The rules and guarantees:

1. **The protocol.** A provider is any object with the seven members of :class:`ModelProvider`: ``model_id`` (it
   names the endpoint, since the cache key holds no URL), ``provider_kind`` (a cache-key field), ``rate_key`` (the
   endpoint identity the runner's semaphores and token bucket share), ``model_family`` (the model's vendor lineage,
   not its transport: ``google``, ``anthropic``, ``openai``, ``meta``, ... or :data:`UNKNOWN_FAMILY`, which adds no
   family diversity to consensus; not a cache-key field, since it shapes no output), ``generation_params`` (exactly
   JSON: what it sends that shapes the output), ``transform_schema`` (pure) and ``generate``. :func:`check_provider`
   checks one at preflight, before any request: each member a real attribute (found without calling
   ``__getattr__``, so a ``Mock`` is refused by name), each identity a non-blank single-line ``str`` of at most
   :data:`IDENTITY_MAX_CHARS` characters (:func:`identity_problem`), the family exactly a ``str`` of 1-32 of
   ``a-z``, ``0-9`` and ``-`` (:func:`family_problem`), ``generation_params`` exactly JSON, both methods callable. A
   property that raises is refused naming the attribute and the exception's class. Its errors never quote a value
   or a message.
2. **Errors.** A provider raises :class:`TransientProviderError` for a failure worth a retry (HTTP 408, 429, 500,
   502-504, 520-524 and 529, the HTTP helper's ``TRANSIENT_STATUSES``; timeouts; transport failures) and
   :class:`PermanentProviderError` for every other failure; both are :class:`ProviderError`, which itself carries
   ``status_code`` and ``detail``. Any other exception from a provider is a bug and propagates. ``detail`` is always
   :func:`bounded_detail` of what it is given: one line of at most :data:`DETAIL_MAX_CHARS` characters, without
   control, bidi, tag or surrogate code points. It can echo document text (a provider's error message may quote the
   request), so it is bounded but never a place for secrets. Every provider error pickles (they cross threads).
3. **Model strings** (R3: explicit prefix only, since a guess decides which vendor receives the documents).
   :func:`parse_model_string` takes ``<kind>:<model>`` for the five :data:`PROVIDER_KINDS`. The prefix runs to the
   first ``:``, so an Ollama name keeps its own colons (``ollama:llama3.1:8b``); ``openai-compatible:<model>@<url>``
   takes its endpoint after the last ``@``. A bare name (``gpt-4o``), an unknown prefix (never echoed: it could be a
   mistyped key), ``vertex:`` (a string cannot carry the OAuth token Vertex needs), an empty model and
   ``openai-compatible`` without its endpoint are refused, each with its remedy. A model name is non-blank, at most
   :data:`MODEL_NAME_MAX_CHARS` characters, holds no whitespace and no control or format character (Unicode
   categories ``Z*`` and ``C*``), and no ``://``: a model name never holds a URL, so a model id can always tell its
   model from its endpoint. A model sent in a request body may hold ``/`` and ``:`` (``meta-llama/Llama-3-8b``); a
   Gemini model goes in the URL path, so it must be one :func:`check_path_segment`.
4. **Endpoints.** :func:`check_endpoint` allows ``https`` always, and ``http`` only for a loopback host
   (``localhost``, ``127.0.0.0/8``, ``::1``) unless ``allow_insecure_http=True``. It refuses an ``@`` anywhere
   (credentials never go in a URL), a query, a fragment, a backslash, whitespace and control characters, non-ASCII
   text (a host goes in punycode), more than :data:`URL_MAX_CHARS` characters, a bad port, and a host that is
   missing or malformed: one whose last label is all digits or ``0x``-hex unless it is a canonical IP address (no
   leading zeros, no octet past 255), an IPv4-mapped IPv6 literal (``[::ffff:127.0.0.1]``, which a proxy would not
   treat as loopback), and one with a punycode label that does not decode to letters, digits and ``-``. It returns
   the normalized base: lower-case scheme and host, the default port (443, 80) dropped, percent-encoded unreserved
   characters decoded and other escapes in upper case, dot segments removed, no trailing slash; normalizing twice
   changes nothing. Its errors never echo the URL, which may hold a credential. :func:`endpoint_model_id` composes a
   ``model_id`` naming the endpoint (``openai-compatible:llama3@https://gw.example/v1``; plain ``openai:gpt-4o`` for
   a kind's default endpoint exactly), so two endpoints never share a model id and one endpoint written two ways
   gets one. :func:`check_path_segment` and :func:`check_gcp_label` check what a URL-path adapter (Gemini, Vertex)
   puts in a URL.
5. **API keys.** :class:`_Secret` holds a key: printable ASCII without leading or trailing whitespace (so a bad key
   fails where it is read, not in a header). Its ``repr`` and ``str`` are ``<secret>``, it has no ``__dict__``, and
   it refuses to pickle, copy or give its state, so a key reaches neither a pickle nor a log by accident.
   :meth:`_Secret.prefixed` builds ``Bearer <key>`` without the key passing through the caller's code. Adapters send
   a key in a header only, as a ``_Secret`` (the HTTP helper reveals it only to build the request), never in a URL.
   :func:`api_key_from_env` reads one variable. A refused key never sits in a traceback local of this module (a log
   with ``capture_locals`` prints them): the check returns its error instead of raising it, and the function that
   raises deletes the text first. Only the caller's own frame, which passed the text, still holds it.

Every rejection a caller can cause is a :class:`~datagrunt.rationalization.RationalizationError`; building an error
or a secret wrongly is a ``TypeError`` or ``ValueError`` at the call. Bounds: every check is linear in its input,
which is itself bounded (a model name, a URL, an identity, a cause chain). Nothing recurses but the JSON coder in
:func:`~datagrunt.rationalization._canonical.canonical_copy`, bounded by the recursion limit. The module imports only
the standard library and the package's own modules: no pydantic, no httpx, nothing from ``datagrunt.core``, and no
import inside a function (invariant 9).
"""

import encodings.idna  # noqa: F401  # the "idna" codec check_endpoint uses, loaded here rather than on a worker thread
import inspect
import ipaddress
import math
import os
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, NoReturn, Protocol, TypeAlias, cast, get_args, runtime_checkable
from urllib.parse import SplitResult, urlsplit

from datagrunt.rationalization._canonical import canonical_copy
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

ProviderKind: TypeAlias = Literal["openai", "anthropic", "gemini", "ollama", "openai-compatible"]
"""The prefix of a model string: the provider that receives the documents."""

PROVIDER_KINDS: tuple[ProviderKind, ...] = get_args(ProviderKind)

GcpField: TypeAlias = Literal["project", "location"]
"""What :func:`check_gcp_label` checks."""

PROVIDER_MEMBERS = (
    "model_id",
    "provider_kind",
    "rate_key",
    "model_family",
    "generation_params",
    "transform_schema",
    "generate",
)
"""The members every :class:`ModelProvider` has."""

IDENTITY_MAX_CHARS = 512
"""The most characters a provider's ``model_id``, ``provider_kind`` or ``rate_key`` may take."""

UNKNOWN_FAMILY = "unknown"
"""The ``model_family`` of a model whose vendor lineage is not known: it adds no family diversity."""

MODEL_NAME_MAX_CHARS = 256
"""The most characters a model name, or a path segment, may take."""

URL_MAX_CHARS = 2_048
"""The most characters an endpoint URL may take."""

DETAIL_MAX_CHARS = 300
"""The most characters a provider error's ``detail`` takes, the cut marker included."""

DETAIL_CUT_MARKER = " [truncated]"
"""What ends a ``detail`` that was cut."""

OPENAI_BASE_URL = "https://api.openai.com/v1"
"""OpenAI's endpoint, the default of the ``openai`` kind."""

OLLAMA_BASE_URL = "http://localhost:11434/v1"
"""A local Ollama server's OpenAI-compatible endpoint, the default of the ``ollama`` kind."""

_DEFAULT_ENDPOINTS: Mapping[str, str] = MappingProxyType({"openai": OPENAI_BASE_URL, "ollama": OLLAMA_BASE_URL})
_PARSED_BASE_URLS: Mapping[str, str] = MappingProxyType({"ollama": OLLAMA_BASE_URL})
_DEFAULT_PORTS: Mapping[str, int] = MappingProxyType({"https": 443, "http": 80})
_IDENTITIES = ("model_id", "provider_kind", "rate_key")
_OPENAI_FAMILY = re.compile(r"(?:gpt|chatgpt|o[0-9])(?:[-.:].*)?")
_LABEL = re.compile(r"[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?")
_NUMERIC_LABEL = re.compile(r"[0-9]+|0x[0-9a-f]*")
_HOST_MAX_CHARS = 253
_LOOPBACK_V6 = ipaddress.IPv6Address("::1")
_PERCENT = re.compile(r"%(.{0,2})", re.DOTALL)
_HEX_PAIR = re.compile(r"[0-9A-Fa-f]{2}")
_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9._-]{1,256}")
_LOCATION = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,50}[a-z0-9])?")  # "{location}-aiplatform" fits a 63-char label
_PROJECT_ID = r"[a-z][a-z0-9-]{4,28}[a-z0-9]"
_DOMAIN = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*"
_PROJECT = re.compile(rf"{_PROJECT_ID}|[1-9][0-9]{{0,18}}|{_DOMAIN}:{_PROJECT_ID}")
_GCP_LABEL_MAX_CHARS = 256
_KINDS_TEXT = ", ".join(sorted(PROVIDER_KINDS))
# What bounded_detail turns into spaces: whitespace (line breaks included), C0, DEL and C1 controls, bidi controls
# (the R8 set), tag characters and lone surrogates.
_DETAIL_BLANKS = re.compile(r"[\s\x00-\x1f\x7f-\x9f‪-‮⁦-⁩\U000e0000-\U000e007f\ud800-\udfff]+")
_IDENTITY_FORBIDDEN = frozenset({"Cc", "Cs", "Zl", "Zp"})
_SECRET_TEXT = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]*[\x21-\x7e])?")
_FAMILY = re.compile(r"[a-z0-9-]{1,32}")


# ---------------------------------------------------------------------------------------------------------------
# Error details and provider errors
# ---------------------------------------------------------------------------------------------------------------


def bounded_detail(text: str) -> str:
    """Return ``text`` as one line of at most :data:`DETAIL_MAX_CHARS` characters (module notes, 2).

    Each run of whitespace, control (C0, DEL, C1), bidi, tag or lone-surrogate code points becomes one space, the
    ends are stripped, and a longer text is cut to end with :data:`DETAIL_CUT_MARKER`.

    Raises:
        TypeError: If ``text`` is not a ``str``.
    """
    if not isinstance(text, str):
        raise TypeError(f"detail must be a str, got {type(text).__name__}")
    line = _DETAIL_BLANKS.sub(" ", text).strip()
    if len(line) <= DETAIL_MAX_CHARS:
        return line
    return line[: DETAIL_MAX_CHARS - len(DETAIL_CUT_MARKER)] + DETAIL_CUT_MARKER


def _check_status(value: object) -> int | None:
    """Require ``None`` or an ``int`` (not a ``bool``) HTTP status in 100-599."""
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"status_code must be None or an int, got {type(value).__name__}")
    if not 100 <= value <= 599:
        raise ValueError(f"status_code must be in 100-599, got {value}")
    return value


def _check_wait(value: object) -> float | None:
    """Require ``None`` or an ``int`` or ``float`` (not a ``bool``) that is finite and >= 0; return it as a float."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"retry_after_s must be None or a number, got {type(value).__name__}")
    try:
        seconds = float(value)
    except OverflowError as error:  # an int past the float range
        raise ValueError("retry_after_s must be finite and >= 0") from error
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(f"retry_after_s must be finite and >= 0, got {seconds}")
    return seconds


def _status_text(status_code: int | None) -> str:
    return "no HTTP status" if status_code is None else f"HTTP {status_code}"


class ProviderError(Exception):
    """A model provider failed one request: the base of the transient and the permanent provider errors.

    Args:
        status_code: The HTTP status (100-599), or ``None`` when there was none (a transport failure, a custom
            provider).
        detail: What went wrong; held :func:`bounded_detail`-ed: one line of at most 300 characters, which can echo
            document text.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If ``status_code`` is out of range.
    """

    _KIND = "provider error"

    def __init__(self, status_code: int | None = None, detail: str = "") -> None:
        self.status_code = _check_status(status_code)
        self.detail = bounded_detail(detail)
        super().__init__(self._message(""))

    def _message(self, extra: str) -> str:
        tail = f": {self.detail}" if self.detail else ""
        return f"{self._KIND} ({_status_text(self.status_code)}{extra}){tail}"

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), self._arguments(), dict(self.__dict__))

    def _arguments(self) -> tuple[Any, ...]:
        """The constructor arguments a pickle rebuilds the error from."""
        return (self.status_code, self.detail)


class TransientProviderError(ProviderError):
    """A failure worth a retry: a retryable HTTP status, a timeout or a transport failure (module notes, 2).

    Args:
        status_code: The HTTP status (100-599), or ``None``.
        retry_after_s: The wait the provider asked for, finite and >= 0 (held as a float), or ``None``.
        detail: What went wrong; held :func:`bounded_detail`-ed.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If ``status_code`` is out of range, or ``retry_after_s`` negative or not finite.
    """

    _KIND = "transient provider error"

    def __init__(self, status_code: int | None, retry_after_s: float | None, detail: str) -> None:
        self.retry_after_s = _check_wait(retry_after_s)
        super().__init__(status_code, detail)
        wait = "" if self.retry_after_s is None else f", retry after {self.retry_after_s} s"
        self.args = (self._message(wait),)

    def _arguments(self) -> tuple[Any, ...]:
        return (self.status_code, self.retry_after_s, self.detail)


class PermanentProviderError(ProviderError):
    """A failure no retry fixes: a 3xx, a 4xx or 5xx outside the retryable set, a refused request (module notes, 2).

    Args:
        status_code: The HTTP status (100-599), or ``None``.
        detail: What went wrong; held :func:`bounded_detail`-ed.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If ``status_code`` is out of range.
    """

    _KIND = "permanent provider error"

    def __init__(self, status_code: int | None, detail: str) -> None:
        super().__init__(status_code, detail)


# ---------------------------------------------------------------------------------------------------------------
# The protocol
# ---------------------------------------------------------------------------------------------------------------


@runtime_checkable
class ModelProvider(Protocol):
    """A model provider: what rationalize() calls for each (unit, model) request (plan §3; module notes, 1)."""

    @property
    def model_id(self) -> str:
        """The provider-qualified model id, naming the endpoint (``"openai-compatible:llama3@https://..."``)."""
        ...

    @property
    def provider_kind(self) -> str:
        """The cache-key kind: ``"openai-compatible"``, ``"anthropic"``, ``"gemini"``, ``"callable"``, ..."""
        ...

    @property
    def rate_key(self) -> str:
        """The endpoint identity the runner's per-provider semaphore and token bucket share."""
        ...

    @property
    def model_family(self) -> str:
        """The model's vendor lineage (``"google"``, ``"anthropic"``, ``"openai"``, ...) or ``"unknown"``."""
        ...

    @property
    def generation_params(self) -> Mapping[str, Any]:
        """What the provider sends that shapes the output (temperature, seed, ...), exactly JSON."""
        ...

    def transform_schema(self, schema: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return the JSON schema this provider sends for the neutral ``schema``; pure."""
        ...

    def generate(self, request: ModelRequest) -> ProviderResponse:
        """Answer one request.

        Raises:
            TransientProviderError: On a failure worth a retry.
            PermanentProviderError: On any other provider failure.
        """
        ...


def identity_problem(value: object) -> str | None:
    """What is wrong with ``value`` as a provider identity (module notes, 1), or ``None`` when nothing is.

    An identity is a non-blank ``str`` of at most :data:`IDENTITY_MAX_CHARS` characters on one line: no control
    character (TAB included), line or paragraph separator, or lone surrogate. The answer never quotes ``value``.
    """
    if not isinstance(value, str):
        return f"must be a str, got {type(value).__name__}"
    if not value.strip():
        return "must be non-blank"
    if len(value) > IDENTITY_MAX_CHARS:
        return f"must be at most {IDENTITY_MAX_CHARS} characters, got {len(value)}"
    if any(unicodedata.category(character) in _IDENTITY_FORBIDDEN for character in value):
        return "must be a single line without control characters, line separators or lone surrogates"
    return None


def family_problem(value: object) -> str | None:
    """What is wrong with ``value`` as a ``model_family`` (module notes, 1), or ``None`` when nothing is.

    A family is exactly a ``str`` (no subclass) of 1-32 of ``a-z``, ``0-9`` and ``-``: the model's vendor lineage
    (``google``, ``anthropic``, ``openai``, ``meta``, ...) or :data:`UNKNOWN_FAMILY`. The answer never quotes ``value``.
    """
    if type(value) is not str:
        return f"must be exactly a str, got {type(value).__name__}"
    if not _FAMILY.fullmatch(value):
        return (
            "must be 1-32 of a-z, 0-9 and '-': the model's vendor lineage (google, anthropic, openai, meta, ...) or "
            "unknown"
        )
    return None


_ABSENT = object()


def has_real_member(obj: object, member: str) -> bool:
    """Whether ``obj`` has ``member`` without calling ``__getattr__`` (an instance, class or slot attribute)."""
    return inspect.getattr_static(obj, member, _ABSENT) is not _ABSENT


def _refuse_provider(owner: str, message: str) -> NoReturn:
    raise RationalizationError(f"provider {owner}: {message}")


def _check_members(obj: object, owner: str) -> None:
    missing = [member for member in PROVIDER_MEMBERS if not has_real_member(obj, member)]
    if not missing:
        return
    if has_real_member(type(obj), "__getattr__"):
        _refuse_provider(
            owner,
            f"{', '.join(missing)} are not real attributes (a __getattr__ answers them, as a Mock does): a "
            "ModelProvider needs real attributes or properties",
        )
    _refuse_provider(owner, f"is not a ModelProvider: it lacks {', '.join(missing)}")


def _read_member(obj: object, owner: str, member: str) -> object:
    """Read one member, refusing (outside the ``except``, so nothing chains) a property that raises."""
    failure = ""
    try:
        return getattr(obj, member)
    except Exception as error:  # a provider's property is the provider's code: whatever it raises is refused
        failure = type(error).__name__
    _refuse_provider(owner, f"reading {member} raised {failure}")


def check_provider(obj: object) -> ModelProvider:
    """Check a provider object at preflight (module notes, 1) and return it.

    Raises:
        RationalizationError: If a member is missing or answered only by ``__getattr__``, a property raises, an
            identity is not a non-blank single-line ``str`` of at most :data:`IDENTITY_MAX_CHARS` characters,
            ``model_family`` is not a family (:func:`family_problem`), ``generation_params`` is not exactly JSON, or
            a method is not callable; the message names the attribute, never its value.
    """
    owner = type(obj).__name__
    _check_members(obj, owner)
    for member in _IDENTITIES:
        problem = identity_problem(_read_member(obj, owner, member))
        if problem is not None:
            _refuse_provider(owner, f"{member} {problem}")
    problem = family_problem(_read_member(obj, owner, "model_family"))
    if problem is not None:
        _refuse_provider(owner, f"model_family {problem}")
    params = _read_member(obj, owner, "generation_params")
    if not isinstance(params, Mapping):
        _refuse_provider(owner, f"generation_params must be a JSON object (a Mapping), got {type(params).__name__}")
    canonical_copy(dict(params), subject=f"the generation_params of provider {owner}")
    for member in ("transform_schema", "generate"):
        if not callable(_read_member(obj, owner, member)):
            _refuse_provider(owner, f"{member} must be callable")
    return cast(ModelProvider, obj)


# ---------------------------------------------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------------------------------------------


def _refuse_url(message: str) -> NoReturn:
    raise RationalizationError(f"endpoint URL refused: {message}")


def _check_url_text(url: object) -> str:
    """The text rules of an endpoint URL, checked before any parse; the errors never echo the URL."""
    if not isinstance(url, str):
        _refuse_url(f"it must be a str, got {type(url).__name__}")
    if len(url) > URL_MAX_CHARS:
        _refuse_url(f"it passes {URL_MAX_CHARS} characters")
    if not url.isascii():
        _refuse_url("it must be ASCII: write the host in punycode (xn--...) and percent-encode the path")
    if any(character <= " " or character == "\x7f" for character in url):
        _refuse_url("it holds whitespace or control characters")
    if "@" in url:
        _refuse_url("it holds '@': credentials never go in a URL (pass the key to the provider object)")
    if "?" in url:
        _refuse_url("it holds a query ('?'): keys go in headers, and the endpoint takes none")
    if "#" in url:
        _refuse_url("it holds a fragment ('#')")
    if "\\" in url:
        _refuse_url("it holds a backslash")
    return url


def _split(url: str) -> tuple[SplitResult, str, int | None]:
    """Parse a checked URL into its parts, its lower-case host and its port."""
    try:
        parts = urlsplit(url)
        host, port = parts.hostname, parts.port
    except ValueError:
        _refuse_url("its host or port is malformed")
    if parts.scheme.lower() not in ("https", "http"):
        _refuse_url("its scheme must be https:// (or http:// for a loopback host)")
    if not host:
        _refuse_url("it names no host")
    if port == 0:
        _refuse_url("its port must be in 1-65535")
    return parts, host, port


def _check_punycode(labels: list[str]) -> None:
    """Refuse an ``xn--`` label that does not decode, or decodes to a symbol or punctuation (IDNA 2008, which httpx
    applies, allows only letters, marks, digits and '-')."""
    for label in labels:
        if not label.startswith("xn--"):
            continue
        try:
            decoded = label.encode("ascii").decode("idna")
        except UnicodeError:
            decoded = ""
        if not decoded or any(unicodedata.category(c)[0] not in "LMN" and c != "-" for c in decoded):
            _refuse_url("its host holds a punycode (xn--) label that does not decode to letters, digits and '-'")


def _ip_address(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """The host as an IP address, ``None`` when it is none; an IPv4-mapped IPv6 literal is refused."""
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return None
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        _refuse_url("its host is an IPv4-mapped IPv6 literal (::ffff:...): write the IPv4 address itself")
    return address


def _host_address(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """The host's IP address, ``None`` for a well-formed host name; a malformed host is refused (module notes, 4)."""
    if "%" in host or len(host) > _HOST_MAX_CHARS:
        _refuse_url("its host is malformed")
    address = _ip_address(host)
    if address is not None:
        return address
    labels = host.split(".")
    if ":" in host or not all(_LABEL.fullmatch(label) for label in labels):
        _refuse_url("its host is malformed (letters, digits, '-' and '_' in dot-separated labels)")
    if _NUMERIC_LABEL.fullmatch(labels[-1]):
        _refuse_url("its host looks like an IPv4 address but is none: write it in dotted decimal without leading zeros")
    _check_punycode(labels)
    return None


def _is_loopback(host: str, address: ipaddress.IPv4Address | ipaddress.IPv6Address | None) -> bool:
    if address is None:
        return host == "localhost"
    if isinstance(address, ipaddress.IPv4Address):
        return address.is_loopback
    return address == _LOOPBACK_V6


def _percent(match: re.Match[str]) -> str:
    pair = match.group(1)
    if not _HEX_PAIR.fullmatch(pair):
        _refuse_url("its path holds a malformed percent-encoding (a '%' not followed by two hex digits)")
    character = chr(int(pair, 16))
    return character if character in _UNRESERVED else f"%{pair.upper()}"


def _normalize_path(path: str) -> str:
    """Decode unreserved escapes, upper-case the rest, remove dot segments (RFC 3986 §6.2.2) and trailing slashes."""
    decoded = _PERCENT.sub(_percent, path)
    kept: list[str] = []
    for segment in decoded.split("/")[1:]:
        if segment == "..":
            if kept:
                kept.pop()
        elif segment != ".":
            kept.append(segment)
    return ("/" + "/".join(kept)).rstrip("/") if decoded else ""


def check_endpoint(url: str, *, allow_insecure_http: bool = False) -> str:
    """Check an endpoint URL (module notes, 4) and return its normalized base.

    Args:
        url: The endpoint, ``https://host[:port][/path]``.
        allow_insecure_http: Allow ``http`` to a host that is not loopback: documents then travel unencrypted, to
            the host and to any proxy on the way.

    Returns:
        The URL normalized: scheme and host in lower case, no default port, escapes normalized, no dot segments,
        no trailing slash.

    Raises:
        RationalizationError: If the URL breaks a rule; the message never echoes the URL.
    """
    if not isinstance(allow_insecure_http, bool):
        raise RationalizationError(f"allow_insecure_http must be a bool, got {type(allow_insecure_http).__name__}")
    parts, host, port = _split(_check_url_text(url))
    address = _host_address(host)
    scheme = parts.scheme.lower()
    if scheme == "http" and not allow_insecure_http and not _is_loopback(host, address):
        _refuse_url(
            "http is allowed only for a loopback host (localhost, 127.0.0.0/8, ::1): use https://, or pass "
            "allow_insecure_http=True to a provider object to send documents unencrypted"
        )
    shown = f"[{address.compressed}]" if isinstance(address, ipaddress.IPv6Address) else host
    port_text = "" if port is None or port == _DEFAULT_PORTS[scheme] else f":{port}"
    return f"{scheme}://{shown}{port_text}{_normalize_path(parts.path)}"


def is_loopback_endpoint(url: str) -> bool:
    """Whether a normalized endpoint (:func:`check_endpoint`'s result) names a loopback host.

    Raises:
        RationalizationError: If ``url`` is not a well-formed endpoint.
    """
    _, host, _ = _split(_check_url_text(url))
    return _is_loopback(host, _host_address(host))


def check_path_segment(value: object, *, name: str) -> str:
    """Require ``value`` to be one URL path segment: 1-256 of ``A-Z a-z 0-9 . _ -``, and neither ``.`` nor ``..``.

    Args:
        value: What goes in the path (a Gemini model name).
        name: What it is, for the message.

    Raises:
        RationalizationError: If it is not such a segment; the message names ``name``, never the value.
    """
    if not isinstance(value, str) or not _PATH_SEGMENT.fullmatch(value) or value in (".", ".."):
        raise RationalizationError(
            f"{name} must be one URL path segment: 1-{MODEL_NAME_MAX_CHARS} letters, digits, '.', '_' or '-', and "
            "neither '.' nor '..'"
        )
    return value


def check_gcp_label(value: object, *, field: GcpField) -> str:
    """Require a Google Cloud ``project`` or ``location`` as Google documents them, safe in a host and a path.

    A location (``us-central1``, ``global``) is a lower-case DNS label: 1-52 of ``a-z 0-9 -`` (so that
    ``{location}-aiplatform`` still fits one 63-character label), not starting or ending with ``-``. A project is a
    project ID (6-30 of ``a-z 0-9 -``, starting with a letter, not ending with ``-``), a project number (digits,
    no leading zero) or a domain-scoped project ID (``example.com:my-project``).

    Raises:
        ValueError: If ``field`` is neither ``"project"`` nor ``"location"``.
        RationalizationError: If ``value`` breaks its rule; the message names the field, never the value.
    """
    if field not in ("project", "location"):
        raise ValueError(f"field must be 'project' or 'location', got {field!r}")
    pattern = _PROJECT if field == "project" else _LOCATION
    if not isinstance(value, str) or len(value) > _GCP_LABEL_MAX_CHARS or not pattern.fullmatch(value):
        rule = (
            "a project ID (6-30 of a-z, 0-9 and '-', starting with a letter), a project number or domain:project-id"
            if field == "project"
            else "a lower-case region such as us-central1 or global (1-52 of a-z, 0-9 and '-')"
        )
        raise RationalizationError(f"the {field} must be {rule}")
    return value


# ---------------------------------------------------------------------------------------------------------------
# Model strings and model ids
# ---------------------------------------------------------------------------------------------------------------


def _model_problem(model: object) -> str | None:
    """What is wrong with ``model`` as a model name (module notes, 3), or ``None``; never quotes it."""
    if not isinstance(model, str):
        return f"has a model name that is not a str ({type(model).__name__})"
    if not model:
        return "names no model"
    if len(model) > MODEL_NAME_MAX_CHARS:
        return f"has a model name longer than {MODEL_NAME_MAX_CHARS} characters"
    if any(unicodedata.category(character)[0] in "CZ" for character in model):
        return "has a model name holding whitespace or control characters"
    if "://" in model:
        return (
            "has a model name holding a URL: only openai-compatible:<model>@<url> (or a provider object) names "
            "an endpoint"
        )
    return None


def _check_model(kind: str, model: object) -> str:
    problem = _model_problem(model)
    if problem is not None or not isinstance(model, str):
        raise RationalizationError(f"the {kind} model string {problem}: write {kind}:<model>")
    if kind == "gemini":
        if model.startswith("models/"):
            raise RationalizationError("write the Gemini model without its models/ prefix, as in gemini:gemini-2.5-pro")
        check_path_segment(model, name="a Gemini model name (it goes in the URL path)")
    return model


def _check_kind(kind: object) -> ProviderKind:
    if kind not in PROVIDER_KINDS:
        raise RationalizationError(f"the provider kind must be one of {_KINDS_TEXT}")
    return kind


@dataclass(frozen=True, slots=True)
class ModelString:
    """A parsed model string: its provider kind, its model name and its endpoint (module notes, 3).

    Args:
        kind: One of :data:`PROVIDER_KINDS`.
        model: The model name.
        base_url: The endpoint as :func:`check_endpoint` normalizes it, or ``None`` for the adapter's own default;
            required for ``openai-compatible``.

    Raises:
        RationalizationError: If a field breaks its rule.
    """

    kind: ProviderKind
    model: str
    base_url: str | None

    def __post_init__(self) -> None:
        _check_kind(self.kind)
        _check_model(self.kind, self.model)
        if self.base_url is None:
            if self.kind == "openai-compatible":
                raise RationalizationError("openai-compatible needs its endpoint: base_url is required")
            return
        if check_endpoint(self.base_url, allow_insecure_http=True) != self.base_url:
            raise RationalizationError("base_url must be an endpoint as check_endpoint() normalizes it")


def _bare_suggestion(spec: str) -> str:
    """The prefixed form to suggest for a bare model name, quoting it only when it is a plain model name."""
    if _model_problem(spec) is not None or "@" in spec:
        return "openai:gpt-4o"
    if spec.startswith("claude"):
        return f"anthropic:{spec}"
    if spec.startswith("gemini"):
        return f"gemini:{spec}"
    if _OPENAI_FAMILY.fullmatch(spec):
        return f"openai:{spec}"
    return "openai:gpt-4o"


def _refuse_prefix(spec: str, *, bare: bool) -> NoReturn:
    found = "names no provider" if bare else "names an unknown provider prefix"
    suggestion = _bare_suggestion(spec) if bare else "openai:gpt-4o"
    raise RationalizationError(
        f"the model string {found}: name the provider, as in {suggestion} (the prefixes: {_KINDS_TEXT}; "
        "openai-compatible:<model>@<url> for any other endpoint)"
    )


def _split_compatible(rest: str) -> tuple[str, str]:
    """The model and the endpoint of ``openai-compatible:<model>@<url>``: the URL after the last ``@``."""
    if "@" not in rest:
        raise RationalizationError(
            "the openai-compatible model string needs its endpoint: write openai-compatible:<model>@https://host/v1"
        )
    model, url = rest.rsplit("@", 1)
    return _check_model("openai-compatible", model), check_endpoint(url)


def parse_model_string(spec: str) -> ModelString:
    """Parse a model string (module notes, 3).

    Args:
        spec: ``<kind>:<model>``, or ``openai-compatible:<model>@<url>``.

    Returns:
        The kind, the model and the endpoint: :data:`OLLAMA_BASE_URL` for ``ollama``, the checked URL for
        ``openai-compatible``, else ``None`` (the adapter knows its endpoint).

    Raises:
        RationalizationError: If ``spec`` is not a ``str``, names no provider or an unknown one, is ``vertex:``,
            names no model or a bad one, or is ``openai-compatible`` without a checked endpoint; each message gives
            the remedy, and none echoes an unknown prefix.
    """
    if not isinstance(spec, str):
        raise RationalizationError(f"a model string must be a str, got {type(spec).__name__}")
    if ":" not in spec:
        _refuse_prefix(spec, bare=True)
    prefix, rest = spec.split(":", 1)
    if prefix == "vertex":
        raise RationalizationError(
            "Vertex needs a token, which a model string cannot carry: use "
            "GeminiProvider.vertex(model, project=..., location=..., token=...)"
        )
    if prefix not in PROVIDER_KINDS:
        _refuse_prefix(spec, bare=False)
    kind = _check_kind(prefix)
    if kind == "openai-compatible":
        model, base_url = _split_compatible(rest)
        return ModelString(kind, model, base_url)
    return ModelString(kind, _check_model(kind, rest), _PARSED_BASE_URLS.get(kind))


def endpoint_model_id(kind: str, model: str, base_url: str | None) -> str:
    """Return the ``model_id`` that names a model at its endpoint (module notes, 4).

    ``<kind>:<model>`` when ``base_url`` is ``None`` or exactly the kind's default endpoint (:data:`OPENAI_BASE_URL`,
    :data:`OLLAMA_BASE_URL`) once normalized, else ``<kind>:<model>@<normalized base_url>`` (an endpoint extending a
    default keeps its URL). A model name never holds ``://`` and a URL never holds ``@``, so two endpoints, or two
    models, never share a model id.

    Raises:
        RationalizationError: If ``kind`` is not a provider kind, ``model`` is not a model name, ``base_url`` is not
            a well-formed endpoint (whether ``http`` may go to its host is the caller's check, made before), or
            ``kind`` is ``openai-compatible`` without a ``base_url``.
    """
    found = _check_kind(kind)
    _check_model(found, model)
    if base_url is None:
        if found == "openai-compatible":
            raise RationalizationError("openai-compatible needs its endpoint: base_url is required")
        return f"{found}:{model}"
    endpoint = check_endpoint(base_url, allow_insecure_http=True)
    if endpoint == _DEFAULT_ENDPOINTS.get(found):
        return f"{found}:{model}"
    return f"{found}:{model}@{endpoint}"


# ---------------------------------------------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------------------------------------------


def _secret_problem(value: object, *, what: str) -> TypeError | ValueError | None:
    """The error ``value`` earns as key text (module notes, 5), or ``None`` for a valid key.

    It returns the error rather than raising it, so the caller deletes the text before it raises: no frame that
    raises holds a key.
    """
    if not isinstance(value, str):
        return TypeError(f"{what} must be a str, got {type(value).__name__}")
    if not value.strip():
        return ValueError(f"{what} must be non-blank")
    if not _SECRET_TEXT.fullmatch(value):
        return ValueError(f"{what} must be printable ASCII without leading or trailing whitespace")
    return None


class _Secret:
    """An API key that never shows: ``repr`` and ``str`` are ``<secret>``; it never pickles, copies or gives state.

    Args:
        value: The key: non-blank printable ASCII without leading or trailing whitespace (module notes, 5).

    Raises:
        TypeError: If ``value`` is not a ``str``.
        ValueError: If it is blank, not printable ASCII, or has leading or trailing whitespace.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        problem = _secret_problem(value, what="a secret")
        if problem is not None:
            del value  # the frame that raises holds no key text (module notes, 5)
            raise problem
        self._value = value

    def reveal(self) -> str:
        """Return the key, for the one header that sends it."""
        return self._value

    def prefixed(self, prefix: str) -> "_Secret":
        """A new secret holding ``prefix + key`` (``"Bearer "``), built without the key leaving this object.

        Raises:
            TypeError: If ``prefix`` is not a ``str``.
            ValueError: If the result is not printable ASCII, or starts with whitespace.
        """
        if not isinstance(prefix, str):
            raise TypeError(f"a secret's prefix must be a str, got {type(prefix).__name__}")
        return _Secret(prefix + self._value)  # the whole is checked as any secret is

    def redact(self, text: str) -> str:
        """Return ``text`` with the key, and each of its words but a leading scheme (``Bearer``), as ``<secret>``.

        Raises:
            TypeError: If ``text`` is not a ``str``.
        """
        if not isinstance(text, str):
            kind = type(text).__name__
            del text  # it may hold the key (an echo): the frame that raises holds none of it
            raise TypeError(f"text must be a str, got {kind}")
        words = self._value.split()
        for word in sorted({self._value, *(words[1:] if len(words) > 1 else words)}, key=len, reverse=True):
            text = text.replace(word, "<secret>")
        return text

    def __repr__(self) -> str:  # str() and format() use it too
        return "<secret>"

    def __reduce_ex__(self, protocol: object) -> NoReturn:
        raise TypeError("an API key never pickles or copies")

    def __reduce__(self) -> NoReturn:
        raise TypeError("an API key never pickles or copies")

    def __getstate__(self) -> NoReturn:
        raise TypeError("an API key never gives its state")


def api_key_from_env(var: str) -> _Secret | None:
    """Return the API key in the environment variable ``var``, or ``None`` when it is unset or blank.

    Raises:
        TypeError: If ``var`` is not a ``str``.
        ValueError: If ``var`` is blank.
        RationalizationError: If the variable holds no valid key (not printable ASCII, or leading or trailing
            whitespace such as a newline); the message names the variable, never its value.
    """
    if not isinstance(var, str):
        raise TypeError(f"the variable name must be a str, got {type(var).__name__}")
    if not var.strip():
        raise ValueError("the variable name must be non-blank")
    value = os.environ.get(var)
    if value is None or not value.strip():
        return None
    key = _Secret(value) if _secret_problem(value, what="an API key") is None else None
    del value  # the frame that raises holds no key text (module notes, 5)
    if key is None:
        raise RationalizationError(
            f"the {var} environment variable holds no valid API key: it must be printable ASCII without leading or "
            "trailing whitespace (a newline included)"
        )
    return key
