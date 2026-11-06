"""What rationalize() hands a model provider, and what the provider hands back (plan §3).

:class:`ModelRequest` is one unit's request to one model; :class:`ProviderResponse` is what an adapter (or a
caller's own provider) returns for it, the type the cache (:mod:`datagrunt.rationalization.cache`) stores and
replays. Both are frozen and slotted, and check every field as they are built, so a malformed value fails where it
is made, never later in consensus or in a cache file. The rules:

1. **Text** (``unit_id``, ``model_id``, ``system_prompt``, ``user_envelope``) is a non-blank ``str`` that UTF-8
   can encode (no lone surrogate), since it is sent. ``raw_text``, the model's own text kept for quarantine, is a
   ``str`` too, possibly empty, valid Unicode, and at most :data:`RAW_TEXT_MAX_BYTES` (8 MiB) in UTF-8. An
   adapter that meets a lone surrogate in a model's output must escape it before it builds the response.
2. **JSON objects** (``ModelRequest.json_schema``, ``ProviderResponse.content``) are a ``Mapping`` that is exactly
   JSON (:func:`~datagrunt.rationalization._canonical.canonical_copy`): its canonical JSON parses back to an equal
   value, so every key is a ``str`` and every value a ``dict``, ``list``, ``str``, finite number, ``bool`` or
   ``None`` (a tuple, a set, an ``int`` key or a NaN is refused). Each is held as its own copy, the parse of that
   canonical JSON: its keys sorted at every level, its numbers and strings plain ``int``, ``float`` and ``str``. No
   caller can change it afterwards through the mapping it passed, and a response read back from the cache equals
   the one first returned and iterates in the same order (a hit is byte-identical).
3. **Numbers.** ``max_output_tokens`` is an ``int`` (not a ``bool``) in 1-:data:`MAX_OUTPUT_TOKENS`;
   ``timeout_s`` an ``int`` or ``float`` (not a ``bool``), finite and positive as a float (an ``int`` past the float
   range is refused, never an ``OverflowError``), held as a ``float``; token counts ``None`` or an ``int`` (not a
   ``bool``) in 0-:data:`TOKEN_COUNT_MAX`, so every count fits an ``Int64`` column.
4. **Finish.** ``finish`` is one of :data:`FINISH_REASONS`: ``"complete"``, ``"truncated"`` (the output limit
   cut the answer) or ``"refused"`` (the model or its safety layer declined).
5. **Round trip.** :meth:`ProviderResponse.to_json` returns a new JSON object of the fields in declaration order,
   ``content`` as a JSON object (not a string); :meth:`ProviderResponse.from_json` takes exactly those fields back
   and checks them as the constructor does, so ``from_json(to_json())`` equals the response, through JSON text
   too.

A ``repr`` never shows document or caller text: the prompts, the envelope, the schema, ``content`` and ``raw_text``
are left out of it. Bounds: ``raw_text`` as above; checking a JSON object serializes and parses it once (linear in
its size, nesting bounded by the recursion limit). The module imports only the standard library and its sibling
modules: no pydantic, no httpx, nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any, Literal, TypeAlias, get_args

from datagrunt.rationalization._canonical import canonical_copy
from datagrunt.rationalization.exceptions import RationalizationError

FinishReason: TypeAlias = Literal["complete", "truncated", "refused"]
"""How a model's answer ended: whole, cut by the output limit, or declined."""

FINISH_REASONS: tuple[FinishReason, ...] = get_args(FinishReason)

MAX_OUTPUT_TOKENS = 131_072
"""The largest ``max_output_tokens`` a request may ask for (plan §5)."""

RAW_TEXT_MAX_BYTES = 8 * 1024 * 1024
"""The most UTF-8 bytes a response's ``raw_text`` may take (the response-body bound, plan §5)."""

TOKEN_COUNT_MAX = 2**63 - 1
"""The largest token count a response may report: what an ``Int64`` column holds."""

_BRIEF_CHARS = 80


def _brief(value: object) -> str:
    """``repr(value)``, cut to :data:`_BRIEF_CHARS` characters, for a message."""
    text = repr(value)
    return text if len(text) <= _BRIEF_CHARS else f"{text[: _BRIEF_CHARS - 3]}..."


def _check_encodable(name: str, value: str) -> None:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"{name} must be valid Unicode text: it holds a lone surrogate at {error.start}") from error


def _check_text(name: str, value: object) -> str:
    """Require a non-blank ``str`` that UTF-8 can encode."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"{name} must be non-blank")
    _check_encodable(name, value)
    return value


def _json_object(name: str, value: object) -> dict[str, Any]:
    """Require a mapping that is exactly JSON (module notes, 2); return its own canonical copy.

    Raises:
        TypeError: If ``value`` is not a ``Mapping``.
        ValueError: If it is not exactly JSON, chained from the cause.
    """
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a JSON object (a Mapping), got {type(value).__name__}")
    try:
        copy: dict[str, Any] = canonical_copy(dict(value), subject=name)
    except RationalizationError as error:
        raise ValueError(f"{name} must be a JSON object: {error}") from error
    return copy


def _check_count(name: str, value: object) -> int | None:
    """Require ``None`` or an ``int`` (not a ``bool``) in 0-:data:`TOKEN_COUNT_MAX`."""
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{name} must be None or an int, got {type(value).__name__}")
    if not 0 <= value <= TOKEN_COUNT_MAX:
        raise ValueError(f"{name} must be in 0-{TOKEN_COUNT_MAX}")
    return value


def _check_timeout(value: object) -> float:
    """Require an ``int`` or ``float`` (not a ``bool``) that is a finite, positive float; return it as one."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"timeout_s must be an int or float, got {type(value).__name__}")
    try:
        seconds = float(value)
    except OverflowError as error:  # an int past the float range
        raise ValueError("timeout_s must be finite and > 0") from error
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"timeout_s must be finite and > 0, got {seconds}")
    return seconds


@dataclass(frozen=True, slots=True)
class ModelRequest:
    """One unit's request to one model (plan §3); every field is checked (module notes).

    Args:
        unit_id: The unit's id.
        model_id: The provider-qualified model id (``"anthropic:claude-..."``).
        system_prompt: The system message.
        user_envelope: The user message (the nonce'd envelope).
        json_schema: The provider-transformed wire schema produced at preflight; held as its own canonical copy.
        max_output_tokens: The output limit, 1-:data:`MAX_OUTPUT_TOKENS`.
        timeout_s: The read timeout in seconds, finite and positive (held as a ``float``).

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a text is blank or not valid Unicode, ``json_schema`` is not exactly JSON, or a number is
            out of range.
    """

    unit_id: str
    model_id: str
    system_prompt: str = field(repr=False)
    user_envelope: str = field(repr=False)
    json_schema: Mapping[str, Any] = field(repr=False)
    max_output_tokens: int
    timeout_s: float

    def __post_init__(self) -> None:
        for name in ("unit_id", "model_id", "system_prompt", "user_envelope"):
            _check_text(name, getattr(self, name))
        object.__setattr__(self, "json_schema", _json_object("json_schema", self.json_schema))
        tokens: object = self.max_output_tokens
        if type(tokens) is not int:
            raise TypeError(f"max_output_tokens must be an int, got {type(tokens).__name__}")
        if not 1 <= tokens <= MAX_OUTPUT_TOKENS:
            raise ValueError(f"max_output_tokens must be in 1-{MAX_OUTPUT_TOKENS}, got {tokens}")
        object.__setattr__(self, "timeout_s", _check_timeout(self.timeout_s))


@dataclass(frozen=True, slots=True)
class ProviderResponse:
    """What a provider returns for one request (plan §3); every field is checked (module notes).

    Args:
        content: The parsed JSON object of the answer, or ``None`` when it did not parse; held as its own
            canonical copy.
        raw_text: The model's text, kept for quarantine (at most :data:`RAW_TEXT_MAX_BYTES` in UTF-8).
        finish: One of :data:`FINISH_REASONS`.
        input_tokens: The input tokens billed, or ``None`` when the provider did not say.
        output_tokens: The output tokens billed, or ``None`` when the provider did not say.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If ``content`` is not exactly JSON, ``raw_text`` is too long or not valid Unicode, ``finish``
            is not a finish reason, or a token count is out of range.
    """

    content: Mapping[str, Any] | None = field(repr=False)
    raw_text: str = field(repr=False)
    finish: FinishReason
    input_tokens: int | None
    output_tokens: int | None

    def __post_init__(self) -> None:
        if self.content is not None:
            object.__setattr__(self, "content", _json_object("content", self.content))
        raw: object = self.raw_text
        if not isinstance(raw, str):
            raise TypeError(f"raw_text must be a str, got {type(raw).__name__}")
        _check_encodable("raw_text", raw)
        if len(raw) > RAW_TEXT_MAX_BYTES or len(raw.encode("utf-8")) > RAW_TEXT_MAX_BYTES:
            raise ValueError(f"raw_text passes {RAW_TEXT_MAX_BYTES} bytes of UTF-8")
        finish: object = self.finish
        if not isinstance(finish, str):
            raise TypeError(f"finish must be a str, got {type(finish).__name__}")
        if finish not in FINISH_REASONS:
            raise ValueError(f"finish must be one of {FINISH_REASONS}, got {_brief(finish)}")
        _check_count("input_tokens", self.input_tokens)
        _check_count("output_tokens", self.output_tokens)

    def to_json(self) -> dict[str, Any]:
        """Return the response as a new JSON object: its fields in declaration order, ``content`` a JSON object.

        Raises:
            ValueError: If ``content`` was changed in place into something that is not exactly JSON.
        """
        content = None if self.content is None else _json_object("content", self.content)
        return {
            "content": content,
            "raw_text": self.raw_text,
            "finish": self.finish,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
        }

    @classmethod
    def from_json(cls, data: object) -> "ProviderResponse":
        """Build a response from :meth:`to_json`'s object: exactly its fields, each checked as the constructor does.

        Raises:
            TypeError: If ``data`` is not a ``Mapping``, or a field has the wrong type.
            ValueError: If a field is missing or extra, or a value is out of range.
        """
        if not isinstance(data, Mapping):
            raise TypeError(f"ProviderResponse.from_json needs a JSON object, got {type(data).__name__}")
        names = [item.name for item in fields(cls)]
        missing = [name for name in names if name not in data]
        extra = sorted(_brief(name) for name in data if name not in names)
        if missing or extra:
            raise ValueError(f"ProviderResponse fields must be exactly {names}: missing {missing}, extra {extra[:5]}")
        return cls(**{name: data[name] for name in names})
