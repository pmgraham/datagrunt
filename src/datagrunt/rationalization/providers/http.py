"""The bounded HTTP helper the wire adapters share: one JSON POST, read safely, mapped to provider errors (plan §4.5).

:class:`HttpClient` owns two ``httpx.Client``\\ s, each created on first use under a lock (so concurrent first uses
share one), or at once by :meth:`HttpClient.open` on the caller's thread (the runner calls it at preflight, so no
module is first imported on a worker thread, #458). Both are closed by a ``weakref.finalize`` when the helper is
garbage-collected: ``close()`` (or ``with``) only releases them early, a request in flight finishes first, and the
helper stays usable after it. Neither follows a redirect; neither keeps a connection alive (every request opens its
own, so its socket can be cut at the deadline); both send ``User-Agent: datagrunt/<version>`` and
``Accept-Encoding: identity`` and hold at most ``max_connections`` connections. After a transport failure (a network
error, a cut at the deadline, a TLS failure) the client that failed is retired: the next request gets a fresh one,
and the old one closes when its last request ends. This works around a defect of httpcore 1.0.9: when the TLS
handshake of a tunnelled (``CONNECT``) connection fails or is cut, the connection stays ACTIVE in the pool, so
``max_connections`` such failures would hang every later request until ``close()``. Keep-alive is off, so a fresh
client loses nothing.

- **Loopback** (``localhost``, ``127.0.0.0/8``, ``::1``: Ollama, vLLM on this machine) uses a client with
  ``trust_env=False``: it never goes through a proxy and ignores the environment's certificate settings
  (``SSL_CERT_FILE``, ``SSL_CERT_DIR``) and ``SSLKEYLOGFILE``, so a document sent to this machine never leaves it.
- **Every other host** uses a client with ``trust_env=True``: ``HTTPS_PROXY`` / ``HTTP_PROXY`` / ``ALL_PROXY`` /
  ``NO_PROXY`` and the system proxy settings route it through a proxy (``https`` stays TLS end to end through a
  ``CONNECT`` tunnel; ``http`` allowed by ``allow_insecure_http=True`` is plaintext to the proxy too),
  ``SSL_CERT_FILE`` / ``SSL_CERT_DIR`` name the CA bundle, and ``SSLKEYLOGFILE``, when set, makes the TLS layer write
  session keys there (whoever reads that file can decrypt the traffic). ``.netrc`` is never read.

:meth:`HttpClient.post_json` makes one request. The rules, in order:

1. **Egress.** The URL passes :func:`~datagrunt.rationalization.providers.base.check_endpoint` and is sent in its
   normalized form: ``https``, or ``http`` to a loopback host (another host only with ``allow_insecure_http=True``),
   never credentials, a query or a fragment in it (an ``@`` only in the path, never in the authority). A redirect
   (any 3xx) is never followed, so no document reaches a host the caller did not name.
2. **Headers.** Every header name must be an HTTP token. The framing and routing headers (``Host``,
   ``Content-Length``, ``Transfer-Encoding``, ``Connection``, ``Upgrade``, ``TE``, ``Trailer``, ``Keep-Alive``,
   ``Expect`` and every ``Proxy-*``) belong to the client and are refused. A plain value must be printable ASCII
   (an inner TAB allowed) without leading or trailing whitespace. A key travels as a
   :class:`~datagrunt.rationalization.providers.base._Secret` (a key header, :data:`KEY_HEADERS`, refuses a plain
   ``str``): it is revealed only inside the one function that builds the httpx request and returns, so no key sits in
   a traceback local, an exception, a ``repr`` or a log (httpx and httpcore never log request headers either).
3. **Body.** The JSON object is sent as compact JSON (``ensure_ascii=False``) in UTF-8.
4. **Time.** The caller passes ``deadline``, an absolute ``time.monotonic()`` instant at most a day away (a
   wall-clock time passed by mistake is refused). Each phase timeout is ``httpx.Timeout(connect=10, read=timeout_s,
   write=15, pool=10)``, each cut to the time left, and the request's socket (the proxy's, behind a proxy) is shut
   down at the deadline by a timer, whatever phase it is in: a server trickling headers or body, one that stops
   reading the request, or a stalled TLS handshake, never holds a worker past it. Only a DNS lookup cannot be cut;
   the socket is cut the moment it connects. A request is never sent unbounded: when no timer thread can be started
   it is not sent, and over the network a reply whose socket was not captured fails (both transient, "could not be
   bounded").
5. **The reply** is read raw, at most :data:`RESPONSE_MAX_BYTES` (8 MiB) on the wire and decoded: ``identity``, or
   exactly one ``gzip`` or ``deflate`` layer, decoded with ``zlib`` so its output never passes the cap (a
   decompression bomb costs at most 8 MiB). A chain or any other encoding is refused unread. Conservatively, a raw
   deflate stream (no zlib header) and a gzip body of several members do not decode, and are refused as such.
6. **Statuses.** 2xx is an :class:`HttpReply`: ``json`` the body when it is a strict JSON object (UTF-8, no NaN, no
   infinity or overflowing number, no repeated key), else ``None``; ``text`` the body decoded as UTF-8 with errors
   replaced. The statuses of :data:`TRANSIENT_STATUSES`, an explicit allow-list, are a
   :class:`~datagrunt.rationalization.providers.base.TransientProviderError` with the wait from ``Retry-After``
   (seconds, or an HTTP date relative to now, never negative) or else ``x-ratelimit-reset-requests`` (``1s``,
   ``6m0s``, ``250ms``), not capped here (the runner caps it at 30 s), and stay transient even when their body is
   too long or does not decode: 408 (request timeout), 429 (rate limited), 500, 502, 503 and 504 (a server or
   gateway that may answer next time), 520-524 (Cloudflare, which fronts many endpoints, reporting an origin that
   failed, refused, timed out or is unreachable) and 529 (Anthropic's "overloaded", sent under load, so one
   overload never stops a whole run). Every other status is a
   :class:`~datagrunt.rationalization.providers.base.PermanentProviderError`, since a retry would only repeat it:
   every other 4xx, and 501 (not implemented), 505-511 (version, negotiation, storage, loop, extension and network
   authentication failures) and the rest of 5xx.
7. **Transport failures** are named from a fixed vocabulary, never the exception's message (which can hold the URL
   or a header): "TLS certificate verification failed" is permanent; "DNS resolution failed", "connection
   refused", "connection reset", the four timeouts, "proxy error" and the like are transient. Provider errors are
   raised outside any ``except`` block, so no httpx exception (which holds the request) rides along.
8. **Error text.** An error's detail is the body's JSON ``error.message``, ``message`` or ``error`` string when
   there is one, else its text, with every key of the request (and each of its words) replaced by ``<secret>``,
   then bounded to one line of 300 characters. That text comes from the provider and can echo the request, document
   text included. Redaction matches a key's own text: a key the server echoes JSON-escaped (``\\u0073k-...``)
   inside a body that does not parse as JSON is not recognized (a parsed body's escapes are decoded first).

**What it raises (a closed set).** :meth:`HttpClient.post_json` raises ``TransientProviderError`` or
``PermanentProviderError`` for anything about the endpoint, the environment, the network or the reply (a refused URL
and a bad proxy or certificate setting included), and ``TypeError`` or ``ValueError`` only for a caller's bug in its
arguments (a wrong type, a body that is not JSON, a header given twice, a plain ``str`` key). :meth:`HttpClient.open`
raises :class:`~datagrunt.rationalization.RationalizationError` for a bad environment, naming the variable and never
its value. Nothing else is caught broadly, and nothing else escapes but a bug.

Bounds: the 8 MiB caps, the deadline, the pool size, a 16-link cause chain, and linear passes over at most 8 MiB.
Nothing recurses but the JSON parser, bounded by the recursion limit. The module imports the standard library, the
HTTP stack (httpx, httpcore, h11, certifi: the [rationalize] extra, all imported here so a worker thread imports none),
the package's version and the package's own modules: nothing from ``datagrunt.core``, and no import inside a
function (invariant 9). Only this module of the providers imports the HTTP stack.
"""

import email.utils
import json
import math
import os
import re
import select
import socket
import ssl
import threading
import time
import urllib.request
import weakref
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timezone
from types import MappingProxyType
from typing import Any, NoReturn, TypeAlias
from urllib.parse import urlsplit

import certifi
import h11
import httpcore
import httpx

from datagrunt import __version__
from datagrunt.rationalization._canonical import strict_loads
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.base import (
    PermanentProviderError,
    TransientProviderError,
    _Secret,
    check_endpoint,
    is_loopback_endpoint,
)

RESPONSE_MAX_BYTES = 8 * 1024 * 1024
"""The most bytes of a response body that are read, on the wire and decoded (plan §5)."""

TRANSIENT_STATUSES = frozenset({408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 529})
"""The HTTP statuses worth a retry, an explicit allow-list (module notes, 6); every other 4xx and 5xx is permanent."""

CONNECT_TIMEOUT_S = 10.0
WRITE_TIMEOUT_S = 15.0
POOL_TIMEOUT_S = 10.0

MAX_CONNECTIONS_LIMIT = 1_024
"""The largest pool a helper may ask for."""

KEY_HEADERS = frozenset({"authorization", "x-api-key", "x-goog-api-key", "api-key"})
"""The headers that carry a key, which must be a ``_Secret`` (lower case)."""

FRAMING_HEADERS = frozenset(
    {"host", "content-length", "transfer-encoding", "connection", "upgrade", "te", "trailer", "keep-alive", "expect"}
)
"""The headers the HTTP client sets itself, refused from a caller (with every ``proxy-*`` header)."""

USER_AGENT = f"datagrunt/{__version__}"

HeaderValue: TypeAlias = str | _Secret
"""A header's value: plain text, or a key."""

# Imported at the top so that building a client imports none of them on a worker thread (#458); certifi and
# httpcore are otherwise only imported by httpx when the first client is built.
_PRELOADED = (httpcore, select)
_CA_BUNDLE = certifi.where()  # resolved here (it imports importlib.resources and zipfile), never first on a worker
_CLIENT_HEADERS: Mapping[str, str] = MappingProxyType({"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
_REQUEST_HEADERS: Mapping[str, str] = MappingProxyType(
    {"content-type": "application/json", "accept": "application/json"}
)
_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
_HEADER_VALUE = re.compile(r"(?:[\x21-\x7e](?:[\t\x20-\x7e]*[\x21-\x7e])?)?")
_SECONDS = re.compile(r"[0-9]{1,10}(?:\.[0-9]{1,6})?")
_AMOUNT = r"([0-9]{1,10}(?:\.[0-9]{1,6})?)"
_DURATION = re.compile(rf"(?:{_AMOUNT}h)?(?:{_AMOUNT}m(?!s))?(?:{_AMOUNT}s)?(?:{_AMOUNT}ms)?")
_DURATION_SCALES = (3600.0, 60.0, 1.0, 0.001)
_WBITS: Mapping[str, int] = MappingProxyType(
    {"gzip": 16 + zlib.MAX_WBITS, "x-gzip": 16 + zlib.MAX_WBITS, "deflate": zlib.MAX_WBITS}
)
_OVER_CAP = "response body exceeds 8 MiB"
_UNDECODABLE = "the response body could not be decoded"
_CAUSE_DEPTH = 16
_DEADLINE_MAX_S = 86_400.0
_UNCAPTURED = "the request could not be bounded by its deadline (its socket was not captured)"
_UNTIMED = "the request could not be bounded by its deadline (no thread could be started to time it)"
_PROXY_SCHEMES = ("http", "https", "all")
_BUILD_ERRORS = (ValueError, ImportError, OSError, httpx.InvalidURL)
_CLIENT_ERRORS = (httpx.HTTPError, httpx.StreamError, httpx.InvalidURL, h11.ProtocolError)
_TIMEOUT_NAMES: Mapping[type[BaseException], str] = MappingProxyType(
    {
        httpx.ConnectTimeout: "connect timeout",
        httpx.ReadTimeout: "read timeout",
        httpx.WriteTimeout: "write timeout",
        httpx.PoolTimeout: "pool timeout",
    }
)
_CAUSE_NAMES: tuple[tuple[type[BaseException], str], ...] = (
    (socket.gaierror, "DNS resolution failed"),
    (ConnectionRefusedError, "connection refused"),
    (ConnectionResetError, "connection reset"),
    (ssl.SSLError, "TLS failed"),
)
_ERROR_NAMES: tuple[tuple[type[BaseException], bool, str], ...] = (
    (httpx.ProxyError, True, "proxy error"),
    (httpx.UnsupportedProtocol, False, "unsupported protocol"),
    (httpx.LocalProtocolError, False, "the request broke the HTTP protocol"),
    (h11.LocalProtocolError, False, "the request broke the HTTP protocol"),
    (httpx.RemoteProtocolError, True, "the server broke the HTTP protocol"),
    (h11.RemoteProtocolError, True, "the server broke the HTTP protocol"),
    (httpx.ConnectError, True, "connection failed"),
    (httpx.NetworkError, True, "connection lost"),
    (httpx.TransportError, True, "connection failed"),
)


# ---------------------------------------------------------------------------------------------------------------
# Waits
# ---------------------------------------------------------------------------------------------------------------


def parse_retry_after(value: str, *, now: float) -> float | None:
    """The seconds a ``Retry-After`` value asks to wait: delta seconds, or an HTTP date less ``now``.

    Args:
        value: The header value.
        now: The current wall-clock time, in seconds since the epoch.

    Returns:
        The wait (a date in the past waits 0), or ``None`` when the value is neither form.
    """
    text = value.strip()
    if _SECONDS.fullmatch(text):
        return float(text)
    try:
        when = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if when.tzinfo is None:  # an RFC 5322 "-0000" zone: UTC
        when = when.replace(tzinfo=timezone.utc)
    try:
        return max(0.0, when.timestamp() - now)
    except (OverflowError, ValueError, OSError):
        return None


def parse_reset_duration(value: str) -> float | None:
    """The seconds of an ``x-ratelimit-reset-requests`` duration (``1s``, ``6m0s``, ``250ms``, ``1h2m3s``).

    Returns:
        The duration, or ``None`` when the value is not hours, minutes, seconds and milliseconds in that order.
    """
    match = _DURATION.fullmatch(value.strip())
    if match is None or not any(match.groups()):
        return None
    return sum(float(amount) * scale for amount, scale in zip(match.groups(), _DURATION_SCALES) if amount)


def retry_after_seconds(headers: Mapping[str, str], *, now: float) -> float | None:
    """The wait a reply asks for: ``Retry-After`` when it parses, else ``x-ratelimit-reset-requests``, else ``None``.

    Args:
        headers: The reply's headers, a case-insensitive mapping (``httpx.Headers``).
        now: The current wall-clock time, in seconds since the epoch.
    """
    retry_after = headers.get("retry-after")
    if retry_after is not None:
        seconds = parse_retry_after(retry_after, now=now)
        if seconds is not None:
            return seconds
    reset = headers.get("x-ratelimit-reset-requests")
    return None if reset is None else parse_reset_duration(reset)


# ---------------------------------------------------------------------------------------------------------------
# Request checks
# ---------------------------------------------------------------------------------------------------------------


def _check_number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be an int or float, got {type(value).__name__}")
    try:
        number = float(value)
    except OverflowError as error:
        raise ValueError(f"{name} must be finite") from error
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite, got {number}")
    return number


def _check_timeout(value: object) -> float:
    """Require an ``int`` or ``float`` (not a ``bool``) that is finite and > 0; return it as a float."""
    seconds = _check_number("timeout_s", value)
    if seconds <= 0:
        raise ValueError(f"timeout_s must be > 0, got {seconds}")
    return seconds


def _check_deadline(value: object, now: float) -> float:
    """Require a finite ``monotonic()`` instant at most a day away (a wall-clock time passed by mistake is refused)."""
    deadline = _check_number("deadline", value)
    if deadline - now > _DEADLINE_MAX_S:
        raise ValueError("deadline is more than a day away: pass a time.monotonic() instant, not a wall-clock time")
    return deadline


def _target(url: str, allow_insecure_http: bool) -> str:
    """The normalized endpoint, or a permanent error (raised outside the ``except``) when it is refused."""
    problem = ""
    try:
        return check_endpoint(url, allow_insecure_http=allow_insecure_http)
    except RationalizationError as error:  # its message never echoes the URL
        problem = str(error)
    raise PermanentProviderError(None, problem)


def _check_header(name: str, value: HeaderValue) -> None:
    """Refuse a header that could inject another, frame the request or route it (rule 2), never quoting a value."""
    if _TOKEN.fullmatch(name) is None:
        raise PermanentProviderError(None, "a header name is not an HTTP token")
    lower = name.lower()
    if lower in FRAMING_HEADERS or lower.startswith("proxy-"):
        raise PermanentProviderError(None, f"the {name} header is set by the HTTP client, never by an adapter")
    if isinstance(value, _Secret):
        return  # printable ASCII without edge whitespace, checked when it was made
    if lower in KEY_HEADERS:
        raise TypeError(f"the {name} header must carry its key as a _Secret, never a plain str")
    if _HEADER_VALUE.fullmatch(value) is None:
        raise PermanentProviderError(
            None,
            f"the {name} header value must be printable ASCII without CR, LF, other control characters, or leading "
            "or trailing whitespace",
        )


def _request_headers(headers: object) -> dict[str, HeaderValue]:
    """The headers to send: the JSON defaults, then the caller's, each checked, all names in lower case."""
    if not isinstance(headers, Mapping):
        raise TypeError(f"headers must be a Mapping of str to str or _Secret, got {type(headers).__name__}")
    sent: dict[str, HeaderValue] = dict(_REQUEST_HEADERS)
    given: set[str] = set()
    for name, value in headers.items():
        if not isinstance(name, str) or not isinstance(value, (str, _Secret)):
            raise TypeError("header names must be str, and values str or _Secret")
        _check_header(name, value)
        if name.lower() in given:
            raise ValueError(f"the {name} header is given twice")
        given.add(name.lower())
        sent[name.lower()] = value
    return sent


def _encode_body(body: object) -> bytes:
    if not isinstance(body, Mapping):
        raise TypeError(f"body must be a JSON object (a Mapping), got {type(body).__name__}")
    try:
        text = json.dumps(dict(body), ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        return text.encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:  # UnicodeEncodeError (a lone surrogate) is a ValueError
        raise ValueError(f"the request body is not JSON: {error}") from error


# ---------------------------------------------------------------------------------------------------------------
# Reply parsing
# ---------------------------------------------------------------------------------------------------------------


def _json_object(data: bytes) -> dict[str, Any] | None:
    """The body as a strict JSON object (rule 6), or ``None``: parsed by
    :func:`~datagrunt.rationalization._canonical.strict_loads`, as a model's JSON text is."""
    try:
        value = strict_loads(data.decode("utf-8"))
    except (ValueError, RecursionError):  # a UnicodeDecodeError or a JSONDecodeError is a ValueError
        return None
    return value if isinstance(value, dict) else None


def _error_text(parsed: Mapping[str, Any] | None, text: str) -> str:
    """The provider's own error message (rule 8), else the whole text."""
    if parsed is None:
        return text
    error = parsed.get("error")
    if isinstance(error, Mapping) and isinstance(error.get("message"), str):
        return str(error["message"])
    if isinstance(parsed.get("message"), str):
        return str(parsed["message"])
    if isinstance(error, str):
        return error
    return text


def _error_detail(data: bytes, secrets: tuple[_Secret, ...]) -> str:
    """An error reply's detail (rule 8), every key of the request redacted; it returns, so no raw text is left
    in the locals of the frame that raises."""
    text = data.decode("utf-8", errors="replace")
    detail = _error_text(_json_object(data), text)
    for secret in secrets:
        detail = secret.redact(detail)
    return detail


@dataclass(frozen=True, slots=True)
class HttpReply:
    """A 2xx reply (rule 6). ``json`` and ``text`` can hold document text, so the ``repr`` leaves them out.

    Args:
        status: The HTTP status, 200-299.
        json: The body as a strict JSON object, or ``None``. A ``\\uD800`` escape parses to a lone surrogate:
            an adapter escapes those before building a response.
        text: The body decoded as UTF-8, errors replaced.
        retry_after_s: The wait the reply asked for, or ``None``.
    """

    status: int
    json: Mapping[str, Any] | None = field(repr=False)
    text: str = field(repr=False)
    retry_after_s: float | None


class _Body:
    """A reply body read under the cap: raw bytes at most 8 MiB, decoded output at most 8 MiB (rule 5)."""

    def __init__(self, wbits: int | None) -> None:
        self._inflater = None if wbits is None else zlib.decompressobj(wbits=wbits)
        self._buffer = bytearray()
        self._raw = 0

    @classmethod
    def for_encoding(cls, header: str | None) -> "_Body | str":
        """The body for a ``Content-Encoding``, or the problem with it: identity, or one gzip or deflate layer."""
        tokens = [token.strip().lower() for token in (header or "").split(",")]
        codings = [token for token in tokens if token not in ("", "identity")]
        if not codings:
            return cls(None)
        if len(codings) == 1 and codings[0] in _WBITS:
            return cls(_WBITS[codings[0]])
        return (
            f"unsupported Content-Encoding {', '.join(codings)}: datagrunt decodes identity or one gzip or deflate "
            "layer"
        )

    def feed(self, chunk: bytes) -> str | None:
        """Take one raw chunk; the problem when the body passes a cap or does not decode, else ``None``."""
        self._raw += len(chunk)
        if self._raw > RESPONSE_MAX_BYTES:
            return _OVER_CAP
        if self._inflater is None:
            self._buffer += chunk
            return None
        try:  # at most one byte past the room: zlib never outputs more than max_length
            output = self._inflater.decompress(chunk, RESPONSE_MAX_BYTES - len(self._buffer) + 1)
        except zlib.error:
            return _UNDECODABLE
        self._buffer += output
        if len(self._buffer) > RESPONSE_MAX_BYTES:
            return _OVER_CAP
        return _UNDECODABLE if self._inflater.unused_data else None

    def finish(self) -> str | None:
        """The problem when an encoded body ended before its stream did, else ``None``."""
        if self._inflater is None or self._raw == 0 or self._inflater.eof:
            return None
        return _UNDECODABLE

    @property
    def data(self) -> bytes:
        return bytes(self._buffer)


@dataclass(frozen=True, slots=True)
class _Exchange:
    """A reply that came back: its status and headers, and its body or the problem reading it.

    The body can echo the request (a key a server repeats in its error included), so the ``repr`` leaves it and the
    headers out: a traceback printed with its locals shows neither.
    """

    status: int
    headers: httpx.Headers = field(repr=False)
    data: bytes = field(repr=False)
    problem: str | None = None


@dataclass(frozen=True, slots=True)
class _Failure:
    """A request that failed before a whole reply came back: the provider error to raise, without the exception."""

    transient: bool
    status: int | None
    detail: str
    network: bool = False  # True when the transport was used: the client that failed is then retired


def _raise_failure(failure: _Failure) -> NoReturn:
    if failure.transient:
        raise TransientProviderError(failure.status, None, failure.detail)
    raise PermanentProviderError(failure.status, failure.detail)


def _reply(exchange: _Exchange, secrets: tuple[_Secret, ...], now: float) -> HttpReply:
    """Map a whole exchange to a reply or a provider error, by status first (rules 1, 5, 6, 8)."""
    status = exchange.status
    if 300 <= status < 400:
        raise PermanentProviderError(status, "redirects are not followed")
    wait = retry_after_seconds(exchange.headers, now=now)
    if exchange.problem is not None:
        if status in TRANSIENT_STATUSES:
            raise TransientProviderError(status, wait, exchange.problem)
        raise PermanentProviderError(status, exchange.problem)
    if 200 <= status < 300:
        return HttpReply(status, _json_object(exchange.data), exchange.data.decode("utf-8", errors="replace"), wait)
    detail = _error_detail(exchange.data, secrets)
    if status in TRANSIENT_STATUSES:
        raise TransientProviderError(status, wait, detail)
    raise PermanentProviderError(status, detail)


# ---------------------------------------------------------------------------------------------------------------
# Transport failures and the deadline
# ---------------------------------------------------------------------------------------------------------------


def _causes(error: BaseException) -> list[BaseException]:
    """The error and its causes (``__cause__``, else ``__context__``), at most 16, never one twice."""
    found: list[BaseException] = []
    current: BaseException | None = error
    for _ in range(_CAUSE_DEPTH):
        if current is None or any(current is seen for seen in found):
            break
        found.append(current)
        current = current.__cause__ or current.__context__
    return found


def _vocabulary(error: BaseException) -> _Failure:
    """A transport failure named from the fixed vocabulary (rule 7), never from the exception's message."""
    chain = _causes(error)
    if any(isinstance(cause, ssl.SSLCertVerificationError) for cause in chain):
        return _Failure(False, None, "TLS certificate verification failed")
    for kind, name in _TIMEOUT_NAMES.items():
        if isinstance(error, kind):
            return _Failure(True, None, name)
    for cause_kind, name in _CAUSE_NAMES:
        if any(isinstance(cause, cause_kind) for cause in chain):
            return _Failure(True, None, name)
    for error_kind, transient, name in _ERROR_NAMES:
        if isinstance(error, error_kind):
            return _Failure(transient, None, name)
    return _Failure(False, None, "the HTTP client failed")


class _CutUnavailable(Exception):
    """The request's socket could not be duplicated, so the deadline could not be enforced: the request stops."""


class _Cutter:
    """Shuts the request's socket down at the deadline (rule 4), from a timer thread.

    The socket is captured from httpcore's ``connection.connect_tcp.complete`` trace event (the proxy's socket
    behind a proxy) and duplicated, so a later TLS wrap, which detaches the original object, does not lose it;
    shutting the duplicate down ends the connection for every descriptor of it.
    """

    def __init__(self, budget: float) -> None:
        self.budget = budget
        self.fired = False
        self.captured = False
        self.require_socket = False
        self._lock = threading.Lock()
        self._socket: socket.socket | None = None
        self._timer = threading.Timer(budget, self._fire)
        self._timer.daemon = True

    def arm(self) -> bool:
        """Start the timer; ``False`` when no thread can be started (the request must then not be sent)."""
        try:
            self._timer.start()
        except RuntimeError:  # "can't start new thread"
            return False
        return True

    def trace(self, event: str, info: Mapping[str, Any]) -> None:
        """httpcore's trace callback: capture the socket when a connection is made."""
        if not event.endswith(".connect_tcp.complete"):
            return
        stream = info.get("return_value")
        original = None if stream is None else stream.get_extra_info("socket")
        if not isinstance(original, socket.socket):
            raise _CutUnavailable("no socket")
        try:
            duplicate = original.dup()
        except OSError as error:
            raise _CutUnavailable("no file descriptor to spare") from error
        with self._lock:  # shutdown never blocks; holding the lock, no closed descriptor number is ever shut
            previous, self._socket, self.captured = self._socket, duplicate, True
            if previous is not None:
                previous.close()
            if self.fired:
                _shut(duplicate)

    def _fire(self) -> None:
        with self._lock:
            self.fired = True
            if self._socket is not None:
                _shut(self._socket)

    def close(self) -> None:
        self._timer.cancel()
        with self._lock:
            if self._socket is not None:
                self._socket.close()
                self._socket = None


def _shut(target: socket.socket) -> None:
    try:
        target.shutdown(socket.SHUT_RDWR)
    except OSError:  # the connection already ended (the reply completed as the timer fired): nothing to cut
        return


def _deadline_failure(budget: float) -> _Failure:
    return _Failure(True, None, f"the request did not finish within {budget:.3g} s", network=True)


def _network_failure(error: BaseException, cutter: _Cutter, *, past_deadline: bool) -> _Failure:
    """The failure to report: the deadline when it has passed (a phase timeout cut to the time left ends there too,
    and may beat the timer), else the error's name from the vocabulary."""
    if cutter.fired or past_deadline:
        return _deadline_failure(cutter.budget)
    if isinstance(error, _CutUnavailable):
        return _Failure(True, None, _UNCAPTURED, network=True)
    failure = _vocabulary(error)
    return _Failure(failure.transient, failure.status, failure.detail, network=True)


# ---------------------------------------------------------------------------------------------------------------
# One exchange: the only place a key is revealed
# ---------------------------------------------------------------------------------------------------------------


def _wire(value: HeaderValue) -> str:
    return value.reveal() if isinstance(value, _Secret) else value


def _build_request(
    client: httpx.Client,
    target: str,
    headers: Mapping[str, HeaderValue],
    content: bytes,
    timeout: httpx.Timeout,
    cutter: _Cutter,
) -> httpx.Request | _Failure:
    """Build the httpx request, revealing each key into it and into nothing else (rule 2); never raises."""
    try:
        return client.build_request(
            "POST",
            target,
            content=content,
            headers={name: _wire(value) for name, value in headers.items()},
            timeout=timeout,
            extensions={"trace": cutter.trace},
        )
    except (httpx.InvalidURL, UnicodeError):  # a URL httpx parses more strictly than check_endpoint
        return _Failure(False, None, "endpoint URL refused by the HTTP client")


def _read_reply(
    reply: httpx.Response, cutter: _Cutter, deadline: float, monotonic: Callable[[], float]
) -> _Exchange | _Failure:
    """Read the raw body under the caps and the deadline (rules 4, 5)."""
    status, headers = reply.status_code, reply.headers
    if 300 <= status < 400:
        return _Exchange(status, headers, b"")
    if reply.is_stream_consumed:  # an injected transport's pre-read reply: no bound applied to its reading
        return _Exchange(status, headers, b"", "the reply was read before datagrunt could bound it")
    body = _Body.for_encoding(headers.get("content-encoding"))
    if isinstance(body, str):
        return _Exchange(status, headers, b"", body)
    try:
        for chunk in reply.iter_raw():
            problem = body.feed(chunk)
            if problem is not None:
                return _Exchange(status, headers, b"", problem)
            if monotonic() > deadline:
                return _deadline_failure(cutter.budget)
    except (*_CLIENT_ERRORS, _CutUnavailable) as error:
        return _network_failure(error, cutter, past_deadline=monotonic() >= deadline)
    problem = body.finish()
    return _Exchange(status, headers, b"" if problem else body.data, problem)


def _send(
    client: httpx.Client, request: httpx.Request, cutter: _Cutter, deadline: float, monotonic: Callable[[], float]
) -> _Exchange | _Failure:
    """Send the request and read the reply; every httpx failure becomes a ``_Failure`` (rule 7), never an exception.

    Over the network (``cutter`` requiring a socket), a reply that came back without its socket captured fails loud:
    the deadline could not have been enforced.
    """
    try:
        reply = client.send(request, stream=True)
    except (*_CLIENT_ERRORS, _CutUnavailable) as error:
        return _network_failure(error, cutter, past_deadline=monotonic() >= deadline)
    try:
        if cutter.require_socket and not cutter.captured:
            return _Failure(True, None, _UNCAPTURED, network=True)
        return _read_reply(reply, cutter, deadline, monotonic)
    finally:
        reply.close()


def _exchange(
    client: httpx.Client,
    target: str,
    headers: Mapping[str, HeaderValue],
    content: bytes,
    seconds: float,
    deadline: float,
    monotonic: Callable[[], float],
    *,
    require_socket: bool,
) -> _Exchange | _Failure:
    """One request under the deadline (rule 4): every phase timeout cut to the time left, the socket cut at the end.

    ``require_socket``: the client goes over the network (no injected transport), so its socket must be captured.
    """
    budget = deadline - monotonic()
    if budget <= 0:
        return _Failure(True, None, "the deadline passed before the request was sent")
    timeout = httpx.Timeout(
        connect=min(CONNECT_TIMEOUT_S, budget),
        read=min(seconds, budget),
        write=min(WRITE_TIMEOUT_S, budget),
        pool=min(POOL_TIMEOUT_S, budget),
    )
    cutter = _Cutter(budget)
    cutter.require_socket = require_socket
    if not cutter.arm():
        return _Failure(True, None, _UNTIMED)
    try:
        request = _build_request(client, target, headers, content, timeout, cutter)
        if isinstance(request, _Failure):
            return request
        return _send(client, request, cutter, deadline, monotonic)
    finally:
        cutter.close()


# ---------------------------------------------------------------------------------------------------------------
# The environment
# ---------------------------------------------------------------------------------------------------------------


def _proxy_source(scheme: str) -> str:
    """The variable a proxy setting came from (lower case wins, as in urllib), or the system settings."""
    for name in (f"{scheme}_proxy", f"{scheme.upper()}_PROXY"):
        if os.environ.get(name):
            return name
    return "the system proxy settings"


def _proxy_problem(value: str) -> str | None:
    """What makes a proxy setting unusable, never quoting it (it can hold credentials), or ``None``."""
    url = value if "://" in value else f"http://{value}"
    try:
        parts = urlsplit(url)
        host, port = parts.hostname, parts.port  # a malformed port raises
        httpx.URL(url)
    except (ValueError, httpx.InvalidURL):
        return "holds no valid proxy URL"
    if port == 0:
        return "names port 0"
    if parts.scheme.lower() not in ("http", "https"):
        return "names a proxy datagrunt cannot use: only http:// and https:// proxies are supported"
    if not host:
        return "names no proxy host"
    return None


def _environment_problem() -> str | None:
    """What in the environment would break the remote client (proxies, CA bundle), naming it, or ``None``."""
    for scheme, value in urllib.request.getproxies().items():
        if scheme in _PROXY_SCHEMES and value:
            problem = _proxy_problem(value)
            if problem is not None:
                return f"{_proxy_source(scheme)} {problem}"
    certificate_file, certificate_dir = os.environ.get("SSL_CERT_FILE"), os.environ.get("SSL_CERT_DIR")
    if certificate_file and not os.path.isfile(certificate_file):
        return "SSL_CERT_FILE names no file: set it to a CA bundle, or unset it"
    if certificate_dir and not os.path.isdir(certificate_dir):
        return "SSL_CERT_DIR names no directory: set it to a CA directory, or unset it"
    return None


# ---------------------------------------------------------------------------------------------------------------
# HttpClient
# ---------------------------------------------------------------------------------------------------------------


def _check_options(max_connections: object, allow_insecure_http: object, transport: object) -> None:
    if type(max_connections) is not int:
        raise TypeError(f"max_connections must be an int, got {type(max_connections).__name__}")
    if not 1 <= max_connections <= MAX_CONNECTIONS_LIMIT:
        raise ValueError(f"max_connections must be in 1-{MAX_CONNECTIONS_LIMIT}, got {max_connections}")
    if not isinstance(allow_insecure_http, bool):
        raise TypeError(f"allow_insecure_http must be a bool, got {type(allow_insecure_http).__name__}")
    if transport is not None and not isinstance(transport, httpx.BaseTransport):
        raise TypeError(f"transport must be an httpx.BaseTransport, got {type(transport).__name__}")


class _Slot:
    """One client and the requests using it; a retired client closes when its last request ends."""

    __slots__ = ("client", "finalizer", "loopback", "retired", "users")

    def __init__(self, client: httpx.Client, finalizer: "weakref.finalize[[], HttpClient]", loopback: bool) -> None:
        self.client = client
        self.loopback = loopback
        self.finalizer = finalizer
        self.retired = False
        self.users = 0


class HttpClient:
    """One provider's bounded HTTP helper (module notes). Thread-safe; ``open()`` and ``close()`` are optional.

    Args:
        max_connections: Each client's connection pool size, 1-1,024.
        allow_insecure_http: Allow ``http`` to a host that is not loopback (plaintext to it and to any proxy).
        transport: An httpx transport to use instead of the network (tests use ``httpx.MockTransport``, whose handlers
            must return streaming responses); the environment is then not consulted. It is closed with the clients.
        wall_clock: The wall clock, for an HTTP-date ``Retry-After``.
        monotonic: The monotonic clock the ``deadline`` is on.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If ``max_connections`` is out of range.
    """

    def __init__(
        self,
        *,
        max_connections: int = 4,
        allow_insecure_http: bool = False,
        transport: httpx.BaseTransport | None = None,
        wall_clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        _check_options(max_connections, allow_insecure_http, transport)
        if not callable(wall_clock) or not callable(monotonic):
            raise TypeError("wall_clock and monotonic must be callable")
        self._max_connections = max_connections
        self._allow_insecure_http = allow_insecure_http
        self._transport = transport
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._slots: dict[bool, _Slot] = {}

    def _build(self, loopback: bool) -> httpx.Client | str:
        """A new client, or the configuration problem that prevents it (named, never its value)."""
        if self._transport is None and not loopback:
            problem = _environment_problem()
            if problem is not None:
                return problem
        failure = ""
        try:
            return httpx.Client(
                follow_redirects=False,
                trust_env=not loopback,
                limits=httpx.Limits(max_connections=self._max_connections, max_keepalive_connections=0),
                headers=dict(_CLIENT_HEADERS),
                transport=self._transport,
            )
        except _BUILD_ERRORS as error:
            failure = type(error).__name__
        return (
            f"the HTTP client could not be built from the environment ({failure}): check HTTP_PROXY, HTTPS_PROXY, "
            "ALL_PROXY, SSL_CERT_FILE and SSL_CERT_DIR"
        )

    def _slot(self, loopback: bool) -> _Slot | str:
        """The current slot, built if missing; call with the lock held."""
        slot = self._slots.get(loopback)
        if slot is not None:
            return slot
        client = self._build(loopback)
        if isinstance(client, str):
            return client
        slot = self._slots[loopback] = _Slot(client, weakref.finalize(self, client.close), loopback)
        return slot

    def _acquire(self, loopback: bool) -> _Slot | str:
        with self._lock:
            slot = self._slot(loopback)
            if isinstance(slot, _Slot):
                slot.users += 1
            return slot

    def _release(self, slot: _Slot, *, retire: bool) -> None:
        """End one request's lease; ``retire`` (after a transport failure) replaces the client for later requests."""
        with self._lock:
            slot.users -= 1
            if retire and self._slots.get(slot.loopback) is slot:
                del self._slots[slot.loopback]
                slot.retired = True
            idle = slot.retired and slot.users == 0
        if idle:
            slot.finalizer()

    def open(self) -> None:
        """Build both clients now, on the caller's thread. Idempotent and optional.

        Raises:
            RationalizationError: If the environment prevents a client (a proxy variable that is malformed or names
                a SOCKS or other unsupported proxy, an ``SSL_CERT_FILE`` or ``SSL_CERT_DIR`` that does not exist),
                naming the variable and never its value.
        """
        for loopback in (False, True):
            with self._lock:
                slot = self._slot(loopback)
            if isinstance(slot, str):
                raise RationalizationError(slot)

    def close(self) -> None:
        """Close the clients now (a request in flight first finishes). Idempotent and optional; usable after."""
        with self._lock:
            retiring, self._slots = list(self._slots.values()), {}
            for slot in retiring:
                slot.retired = True
            idle = [slot for slot in retiring if slot.users == 0]
        for slot in idle:
            slot.finalizer()

    def __enter__(self) -> "HttpClient":
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"HttpClient(max_connections={self._max_connections}, allow_insecure_http={self._allow_insecure_http})"

    def post_json(
        self,
        url: str,
        *,
        headers: Mapping[str, HeaderValue],
        body: Mapping[str, Any],
        timeout_s: float,
        deadline: float,
    ) -> HttpReply:
        """POST ``body`` as JSON to ``url`` and return the 2xx reply (module notes, 1-8).

        Args:
            url: The endpoint, checked as :func:`~datagrunt.rationalization.providers.base.check_endpoint` checks
                it and sent normalized.
            headers: Extra headers; a key as a ``_Secret``.
            body: The JSON object to send.
            timeout_s: The read timeout, finite and > 0.
            deadline: The absolute ``monotonic()`` instant the whole request must end by.

        Raises:
            TransientProviderError: On a retryable status, a timeout, a transport failure or the deadline.
            PermanentProviderError: On a refused URL or environment, any other status, a redirect, a body past
                8 MiB or one that does not decode, or a header that could inject another or frame the request.
            TypeError: If an argument has the wrong type, or a key header is a plain ``str``.
            ValueError: If ``timeout_s`` or ``deadline`` is out of range, ``body`` is not JSON or a header is given
                twice.
        """
        target = _target(url, self._allow_insecure_http)
        seconds = _check_timeout(timeout_s)
        finish = _check_deadline(deadline, self._monotonic())
        sent = _request_headers(headers)
        content = _encode_body(body)
        slot = self._acquire(is_loopback_endpoint(target))
        if isinstance(slot, str):
            raise PermanentProviderError(None, slot)
        outcome: _Exchange | _Failure | None = None
        try:
            outcome = _exchange(
                slot.client,
                target,
                sent,
                content,
                seconds,
                finish,
                self._monotonic,
                require_socket=self._transport is None,
            )
        finally:
            self._release(slot, retire=isinstance(outcome, _Failure) and outcome.network)
        if isinstance(outcome, _Failure):
            _raise_failure(outcome)
        secrets = tuple(value for value in sent.values() if isinstance(value, _Secret))
        return _reply(outcome, secrets, self._wall_clock())
