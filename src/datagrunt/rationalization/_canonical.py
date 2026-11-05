"""Canonical JSON and its SHA-256: the one text the schema hash and the cache key are computed over (plan §4.1, §4.4).

:func:`canonical_json` is ``json.dumps`` with sorted keys, compact separators (``","`` and ``":"``), no ASCII
escaping and no NaN or infinity, and the text must encode in UTF-8. Equal structures give equal text, whatever
their insertion order, the process or its ``PYTHONHASHSEED``. A value it cannot write raises
:class:`~datagrunt.rationalization.RationalizationError` chained from the cause (``from``), never a partial text:

- a value that is not JSON (a ``set``, a ``Decimal``, an object, ...) or a key that is not a ``str``, ``int``,
  ``float``, ``bool`` or ``None`` (``TypeError``);
- keys of one object that do not sort together, such as ``1`` and ``"a"`` (``TypeError``);
- NaN or an infinity, or a circular value (``ValueError``);
- a lone surrogate in a string or a key, which UTF-8 cannot encode (``UnicodeEncodeError``);
- nesting deeper than the interpreter's recursion limit (``RecursionError``).

Keys that are ``int``, ``float``, ``bool`` or ``None`` are written as JSON writes them (``1`` as ``"1"``), and a
tuple as an array, so ``{1: "x"}`` and ``{"1": "x"}`` canonicalize alike. :func:`canonical_copy` is the strict form
for what must be exactly JSON (a cache key's parts, a provider's answer): it parses the canonical JSON back and
requires the result to equal the value, so every key is a ``str`` and every array a ``list`` at every level, and
nothing folds. :func:`sha256_hex` is the SHA-256 hex digest of a text's UTF-8 bytes.

Nothing here recurses but the JSON coder and the comparison, bounded by the recursion limit. The module imports
only the standard library and the package's error: no pydantic, no httpx, nothing from ``datagrunt.core``, and no
import inside a function (invariant 9).
"""

import hashlib
import json
from typing import Any

from datagrunt.rationalization.exceptions import RationalizationError


def canonical_json(value: object, *, subject: str = "the value") -> str:
    """Return ``value`` as canonical JSON text: sorted keys, compact separators, unescaped, encodable in UTF-8.

    Args:
        value: Any JSON value (an object, an array, a string, a number, a boolean or ``None``).
        subject: What ``value`` is, for the error message (``"the response schema"``).

    Returns:
        The text; equal values give equal text.

    Raises:
        TypeError: If ``subject`` is not a ``str``.
        ValueError: If ``subject`` is blank.
        RationalizationError: If ``value`` is not canonical JSON (module notes), chained from the cause.
    """
    if not isinstance(subject, str):
        raise TypeError(f"subject must be a str, got {type(subject).__name__}")
    if not subject.strip():
        raise ValueError("subject must be non-blank")
    try:
        text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        text.encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise RationalizationError(f"{subject} is not canonical JSON: {error}") from error
    return text


def canonical_copy(value: object, *, subject: str = "the value") -> Any:
    """Return the parse of ``value``'s canonical JSON, which must equal ``value``: ``value`` is exactly JSON.

    The copy is the value's own: its keys sorted at every level, its numbers and strings plain ``int``, ``float``
    and ``str``.

    Args:
        value: Any JSON value whose keys are all ``str`` and whose arrays are all ``list``.
        subject: What ``value`` is, for the error message.

    Raises:
        TypeError: If ``subject`` is not a ``str``.
        ValueError: If ``subject`` is blank.
        RationalizationError: If ``value`` is not canonical JSON, or canonical JSON would fold it (a key that is
            not a ``str``, a tuple), chained from the cause where there is one.
    """
    text = canonical_json(value, subject=subject)
    try:
        copy = json.loads(text)
        exact = copy == value
    except (ValueError, RecursionError) as error:  # the parse or the comparison nesting deeper than the encoder
        raise RationalizationError(f"{subject} is not canonical JSON: {error}") from error
    if not exact:
        raise RationalizationError(
            f"{subject} is not exactly JSON: every key must be a str and every array a list, at every level"
        )
    return copy


def sha256_hex(text: str) -> str:
    """Return the SHA-256 hex digest (64 lowercase characters) of ``text``'s UTF-8 bytes.

    Raises:
        TypeError: If ``text`` is not a ``str``.
        RationalizationError: If ``text`` holds a lone surrogate (it is not valid Unicode), chained from the
            ``UnicodeEncodeError``.
    """
    if not isinstance(text, str):
        raise TypeError(f"text must be a str, got {type(text).__name__}")
    try:
        data = text.encode("utf-8")
    except UnicodeEncodeError as error:
        raise RationalizationError(f"text is not valid Unicode: a lone surrogate at {error.start}") from error
    return hashlib.sha256(data).hexdigest()
