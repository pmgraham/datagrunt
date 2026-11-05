"""What every wire adapter does to a model's answer before it becomes a ``ProviderResponse`` (plan §1, §3, §4.5).

A model's output is untrusted data. It is never repaired: these helpers only escape it and bound it, and what cannot
be made exact becomes ``None`` for the runner to quarantine, never a guess. The wire adapters (OpenAI-compatible,
Anthropic, Gemini) build their :class:`~datagrunt.rationalization.wire.ProviderResponse` through :func:`response`.
The rules:

1. **Escaping** (:func:`safe_text`). Every surrogate code point (U+D800-U+DFFF) becomes a visible R8 token,
   ``[U+D800]`` (uppercase hex, four digits), the form :mod:`~datagrunt.rationalization.envelope` gives document
   text, so UTF-8 can always encode the result. A ``str`` holds code points, so every surrogate in one is lone:
   JSON's escape pair ``"\\uD83D\\uDE00"`` parses to the one character U+1F600 and stays as it is, while a ``str``
   holding U+D83D then U+DE00 holds two lone surrogates and gets two tokens. Everything else stays verbatim, bidi
   and control characters included. A model can write a token's text itself, so the text alone does not prove an
   escape.
2. **Raw text** (:func:`bounded_raw_text`). The escaped text, when its UTF-8 passes :data:`RAW_TEXT_MAX_BYTES`
   (8 MiB), is cut and ends with :data:`TRUNCATION_MARKER`, the whole within the bound. The cut falls on a character
   boundary, never inside a multi-byte sequence, and never inside a token's eight characters (one the model wrote
   included), as late as that allows. A text within the bound comes back escaped and otherwise unchanged. Only the
   first :data:`RAW_TEXT_MAX_BYTES` + 1 characters are read, so the work is bounded whatever the text's length (an
   ``HttpReply.text``, decoded with errors replaced, can take about three times its body in UTF-8).
3. **JSON values** (:func:`safe_json`). A parsed JSON value becomes its own copy, every string and every key
   escaped, which is exactly JSON (:func:`~datagrunt.rationalization._canonical.canonical_copy`) and so is accepted
   as ``ProviderResponse.content``. It is ``None`` instead when the value is not a JSON object (a ``dict``), or:

   - holds anything but the exact types ``json.loads`` gives (``dict``, ``list``, ``str``, ``int``, ``float``,
     ``bool``, ``None``), so a tuple, a ``Mapping``, a ``Decimal`` or a subclass such as an ``IntEnum`` is refused;
   - holds a NaN or an infinity, a key that is not a ``str``, or an ``int`` canonical JSON cannot write (past the
     interpreter's digit limit);
   - nests more than :data:`JSON_MAX_DEPTH` (64) containers, the root counted;
   - holds more than :data:`JSON_MAX_NODES` (1,000,000) nodes: the root, each array item and each member's value,
     a shared value counted each time it is reached;
   - has two keys in one object that escaping makes equal (``"\\uD800"`` and the text ``"[U+D800]"``).

   The walk is iterative and stops at the first refusal, so a cycle or a deep chain ends in ``None``, never in a
   ``RecursionError``.
4. **JSON text** (:func:`parse_json_object`). A model's structured output, the string it returns, is parsed
   strictly: exactly one JSON object with nothing around it but JSON's whitespace (space, TAB, LF, CR); no ``NaN``
   or ``Infinity``, no number that overflows a float (``1e999``), no key repeated in an object; then
   :func:`safe_json`. Anything else is ``None``, never an exception: a parse error, nesting past the recursion
   limit, a text that is not exactly a ``str``, and a text past :data:`RAW_TEXT_MAX_BYTES` in UTF-8 (a lone
   surrogate counted as its three bytes), which is refused before it is parsed. A Markdown code fence around the
   object (three backticks and ``json``), prose before or after it, a BOM or a second value are refused as well:
   the adapter quarantines that answer as unparseable rather than repair it. The parse is
   :func:`~datagrunt.rationalization._canonical.strict_loads`, the one the HTTP helper reads reply bodies with, so
   both refuse alike.
5. **Responses** (:func:`response`). A ``ProviderResponse`` built from what an adapter extracted: ``content``
   through :func:`safe_json` when it is not ``None`` (``None`` when it cannot be made exact); ``raw_text`` through
   :func:`bounded_raw_text` (one that is not a ``str``, where the provider sent no text, is kept as ``""``); each
   token count kept when it is an ``int`` (not a ``bool``) in 0-:data:`TOKEN_COUNT_MAX`, else ``None``, so a provider
   reporting junk usage never fails the response. ``finish`` must be one of :data:`FINISH_REASONS`: the adapter maps
   the provider's reason to one, so anything else is the adapter's bug, a ``ValueError``. Nothing else raises.

Bounds: the copy visits at most :data:`JSON_MAX_NODES` nodes, the cut backs off at most three bytes of a character
and one token, and every other pass is linear in an input that is itself bounded. Nothing recurses but the JSON
coder: ``json.loads`` (bounded by the recursion limit; a ``RecursionError`` is ``None``) and ``canonical_copy``
(over at most :data:`JSON_MAX_DEPTH` levels). The module imports only the standard library and the package's own
modules: no pydantic, no httpx, nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

import math
import re
from typing import Any, TypeAlias

from datagrunt.rationalization._canonical import canonical_copy, strict_loads
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.wire import (
    FINISH_REASONS,
    RAW_TEXT_MAX_BYTES,
    TOKEN_COUNT_MAX,
    FinishReason,
    ProviderResponse,
)

JSON_MAX_DEPTH = 64
"""The most containers (objects and arrays) a JSON value may nest, the root counted."""

JSON_MAX_NODES = 1_000_000
"""The most nodes a JSON value may hold: the root, each array item and each object member's value."""

TRUNCATION_MARKER = " [truncated]"
"""What ends a raw text cut to :data:`RAW_TEXT_MAX_BYTES`."""

_SURROGATE = re.compile(r"[\uD800-\uDFFF]")
# The token safe_text writes for a surrogate: eight ASCII characters.
_TOKEN = re.compile(r"\[U\+D[89A-F][0-9A-F]{2}\]")
_TOKEN_CHARS = 8
# The most UTF-8 continuation bytes that follow a lead byte.
_CONTINUATION_BYTES = 3

# A container still to copy: its source, its (empty) copy, and its depth (the root is 1).
_Frame: TypeAlias = tuple[Any, dict[str, object] | list[object], int]


class _NotJsonError(Exception):
    """A value :func:`safe_json` cannot make exactly JSON; caught there, never raised out of this module."""


# ---------------------------------------------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------------------------------------------


def _token(match: re.Match[str]) -> str:
    return f"[U+{ord(match.group(0)):04X}]"


def safe_text(text: str) -> str:
    """Return ``text`` with every surrogate code point as an R8 ``[U+XXXX]`` token, everything else verbatim (rule 1).

    Raises:
        TypeError: If ``text`` is not a ``str``.
    """
    if not isinstance(text, str):
        raise TypeError(f"text must be a str, got {type(text).__name__}")
    return _SURROGATE.sub(_token, text)


def bounded_raw_text(text: str) -> str:
    """Return :func:`safe_text` of ``text``, cut to :data:`RAW_TEXT_MAX_BYTES` of UTF-8 with the marker (rule 2).

    Raises:
        TypeError: If ``text`` is not a ``str``.
    """
    if not isinstance(text, str):
        raise TypeError(f"text must be a str, got {type(text).__name__}")
    escaped = safe_text(text[: RAW_TEXT_MAX_BYTES + 1])  # every character takes a byte or more: the rest is cut
    data = escaped.encode("utf-8")
    if len(data) <= RAW_TEXT_MAX_BYTES:
        return escaped
    return escaped[: _cut_point(escaped, data)] + TRUNCATION_MARKER


def _cut_point(escaped: str, data: bytes) -> int:
    """The length in characters of the head of ``escaped`` kept before the marker: as long as fits, ending on a
    character boundary and outside every token. ``data`` is ``escaped`` in UTF-8, longer than the bound."""
    limit = RAW_TEXT_MAX_BYTES - len(TRUNCATION_MARKER.encode("utf-8"))
    for _ in range(_CONTINUATION_BYTES):  # data[limit] is the first byte left out: never a continuation byte
        if data[limit] & 0xC0 != 0x80:
            break
        limit -= 1
    end = len(data[:limit].decode("utf-8"))
    # Any token found in this window starts before end and finishes after it: the cut would split it.
    spanning = _TOKEN.search(escaped, max(0, end - _TOKEN_CHARS + 1), end + _TOKEN_CHARS - 1)
    return end if spanning is None else spanning.start()


# ---------------------------------------------------------------------------------------------------------------
# JSON values
# ---------------------------------------------------------------------------------------------------------------


def safe_json(value: object) -> dict[str, object] | None:
    """Return an escaped, exactly-JSON copy of the JSON object ``value``, or ``None`` when there is none (rule 3).

    Args:
        value: A parsed JSON value, as ``json.loads`` gives it; anything else is ``None``.

    Returns:
        A new ``dict`` that :func:`~datagrunt.rationalization._canonical.canonical_copy` returns unchanged (its keys
        sorted at every level), or ``None``. It never raises.
    """
    if type(value) is not dict:
        return None
    try:
        copy: dict[str, object] = canonical_copy(_escaped_copy(value), subject="the model's JSON object")
    except (_NotJsonError, RationalizationError):  # canonical JSON refuses an int past the digit limit
        return None
    return copy


def _escaped_copy(root: dict[Any, Any]) -> dict[str, object]:
    """Copy ``root`` iteratively, every string and key escaped, under both bounds.

    Raises:
        _NotJsonError: At the first value, key or bound that refuses it.
    """
    copy: dict[str, object] = {}
    stack: list[_Frame] = [(root, copy, 1)]
    nodes = 1
    while stack:  # bounded: each frame is a node already counted, and the count stops at JSON_MAX_NODES
        source, target, depth = stack.pop()
        nodes += len(source)
        if nodes > JSON_MAX_NODES:
            raise _NotJsonError(f"the value holds more than {JSON_MAX_NODES} nodes")
        if isinstance(target, dict):
            _copy_members(source, target, depth, stack)
        else:
            target.extend(_copy_value(item, depth, stack) for item in source)
    return copy


def _copy_members(source: dict[Any, Any], target: dict[str, object], depth: int, stack: list[_Frame]) -> None:
    """Copy an object's members into ``target``, refusing a key that is not a ``str`` or collides once escaped."""
    for key, item in source.items():
        if type(key) is not str:
            raise _NotJsonError(f"a key is a {type(key).__name__}, not a str")
        name = safe_text(key)
        if name in target:
            raise _NotJsonError("two keys of one object are equal once escaped")
        target[name] = _copy_value(item, depth, stack)


def _copy_value(item: object, depth: int, stack: list[_Frame]) -> object:
    """A scalar's copy (a string escaped), or a new empty container queued on ``stack`` to be filled.

    ``depth`` is the depth of ``item``'s container.
    """
    if item is None or type(item) is bool or type(item) is int:
        return item
    if type(item) is str:
        return safe_text(item)
    if type(item) is float:
        if not math.isfinite(item):
            raise _NotJsonError("a number is not finite")
        return item
    if type(item) is not dict and type(item) is not list:
        raise _NotJsonError(f"a {type(item).__name__} is not JSON")
    if depth >= JSON_MAX_DEPTH:
        raise _NotJsonError(f"the value nests more than {JSON_MAX_DEPTH} containers")
    child: dict[str, object] | list[object] = {} if type(item) is dict else []
    stack.append((item, child, depth + 1))
    return child


# ---------------------------------------------------------------------------------------------------------------
# JSON text
# ---------------------------------------------------------------------------------------------------------------


def parse_json_object(text: object) -> dict[str, object] | None:
    """Parse a model's JSON text strictly into :func:`safe_json`'s copy, or ``None`` (rule 4); it never raises.

    The text must be one JSON object with only JSON whitespace around it. A Markdown fence (```json ... ```) or any
    prose around the object makes it ``None``, which the adapter quarantines as unparseable: model output is never
    repaired.

    Args:
        text: The model's text; anything but a ``str`` is ``None``.
    """
    if type(text) is not str:
        return None
    if len(text) > RAW_TEXT_MAX_BYTES or len(text.encode("utf-8", "surrogatepass")) > RAW_TEXT_MAX_BYTES:
        return None
    try:
        value = strict_loads(text)
    except (ValueError, RecursionError):  # a JSONDecodeError, a refused value or an int past the digit limit
        return None
    return safe_json(value)


# ---------------------------------------------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------------------------------------------


def _token_count(value: object) -> int | None:
    """``value`` when it is an ``int`` (not a ``bool``) in 0-:data:`TOKEN_COUNT_MAX`, else ``None``."""
    if type(value) is int and 0 <= value <= TOKEN_COUNT_MAX:
        return value
    return None


def response(
    *,
    content: object,
    raw_text: object,
    finish: FinishReason,
    input_tokens: object,
    output_tokens: object,
) -> ProviderResponse:
    """Build a :class:`~datagrunt.rationalization.wire.ProviderResponse` from what an adapter extracted (rule 5).

    Args:
        content: The parsed answer (a JSON value, or ``None`` when the answer did not parse).
        raw_text: The model's text, kept for quarantine.
        finish: The adapter's mapping of the provider's finish reason, one of :data:`FINISH_REASONS`.
        input_tokens: The provider's input-token count, whatever it reported.
        output_tokens: The provider's output-token count, whatever it reported.

    Raises:
        ValueError: If ``finish`` is not one of :data:`FINISH_REASONS` (the adapter's bug); nothing else raises.
    """
    if type(finish) is not str or finish not in FINISH_REASONS:
        raise ValueError(f"finish must be one of {FINISH_REASONS}: the adapter maps the provider's reason to one")
    return ProviderResponse(
        content=None if content is None else safe_json(content),
        raw_text=bounded_raw_text(raw_text) if isinstance(raw_text, str) else "",
        finish=finish,
        input_tokens=_token_count(input_tokens),
        output_tokens=_token_count(output_tokens),
    )
