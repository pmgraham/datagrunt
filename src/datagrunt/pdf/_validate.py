"""Private boundary validators for the v5 PDF document model.

Each helper checks one field and either returns the (possibly coerced) value
or raises: ``TypeError`` for a wrong type, ``ValueError`` for a bad value.
Every message names the offending field so construction errors are actionable.
"""

import math
import re

# A SHA-256 hex digest exactly as hashlib's hexdigest() produces it. Matched
# with fullmatch() so a trailing newline cannot slip past a `$` anchor.
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")


def int_at_least(name: str, value: object, minimum: int) -> int:
    """Return ``value`` if it is an int >= ``minimum``; bool is rejected."""
    # bool is a subclass of int; reject it explicitly so True/False cannot
    # masquerade as 1/0.
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {value!r} ({type(value).__name__})")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def finite_float(name: str, value: object, *, lo: float | None = None, hi: float | None = None) -> float:
    """Return ``value`` as a finite float within the inclusive ``[lo, hi]`` bounds.

    Accepts int or float; rejects bool, NaN, and +/-inf.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be an int or float, got {value!r} ({type(value).__name__})")
    try:
        result = float(value)
    except OverflowError as exc:
        # An int beyond float range has no finite float representation.
        raise ValueError(f"{name} must be finite, got an int too large for a float") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite, got {result!r}")
    if lo is not None and result < lo:
        raise ValueError(f"{name} must be >= {lo}, got {result!r}")
    if hi is not None and result > hi:
        raise ValueError(f"{name} must be <= {hi}, got {result!r}")
    return result


def non_blank_str(name: str, value: object) -> str:
    """Return ``value`` verbatim if it is a str with at least one non-whitespace char."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    if not value.strip():
        raise ValueError(f"{name} must be a non-blank str, got {value!r}")
    return value


def str_tuple(name: str, value: object, *, allow_blank: bool = False) -> tuple[str, ...]:
    """Return ``value`` if it is a tuple of str (entries non-blank unless ``allow_blank``)."""
    # A tuple, never a list: element fields must stay immutable and hashable.
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple of str, got {type(value).__name__}")
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise TypeError(f"{name}[{index}] must be a str, got {item!r} ({type(item).__name__})")
        if not allow_blank and not item.strip():
            raise ValueError(f"{name}[{index}] must be a non-blank str, got {item!r}")
    return value


def optional_str(name: str, value: object) -> str | None:
    """Return ``value`` if it is ``None`` or a str."""
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a str or None, got {value!r} ({type(value).__name__})")
    return value


def finite_tuple(name: str, value: object, size: int) -> tuple[float, ...]:
    """Return ``value`` as a tuple of ``size`` finite floats."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(value).__name__}")
    if len(value) != size:
        raise ValueError(f"{name} must have {size} values, got {len(value)}")
    return tuple(finite_float(f"{name}[{index}]", item) for index, item in enumerate(value))


def strict_bool(name: str, value: object) -> bool:
    """Return ``value`` if it is exactly a bool (not an int standing in for one)."""
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, got {value!r} ({type(value).__name__})")
    return value


def rgba_tuple(name: str, value: object) -> tuple[int, int, int, int]:
    """Return ``value`` if it is a tuple of 4 ints in 0..255 (bool rejected)."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(value).__name__}")
    if len(value) != 4:
        raise ValueError(f"{name} must have 4 channels, got {len(value)}")
    for index, channel in enumerate(value):
        if isinstance(channel, bool) or not isinstance(channel, int):
            raise TypeError(f"{name}[{index}] must be an int, got {channel!r} ({type(channel).__name__})")
        if not 0 <= channel <= 255:
            raise ValueError(f"{name}[{index}] must be in 0..255, got {channel}")
    red, green, blue, alpha = value
    return red, green, blue, alpha


def sha256_hex(name: str, value: object) -> str:
    """Return ``value`` if it is a 64-char lowercase hex SHA-256 digest."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    if _SHA256_HEX.fullmatch(value) is None:
        raise ValueError(f"{name} must be 64 lowercase hex characters, got {value!r}")
    return value


def ordered_box(name: str, value: object) -> tuple[float, float, float, float]:
    """Return ``value`` as 4 finite floats ``(x0, y0, x1, y1)`` with ``x0 <= x1`` and ``y0 <= y1``."""
    x0, y0, x1, y1 = finite_tuple(name, value, 4)
    if x0 > x1 or y0 > y1:
        raise ValueError(f"{name} must have x0 <= x1 and y0 <= y1, got {value!r}")
    return x0, y0, x1, y1
