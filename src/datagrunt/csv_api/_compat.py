"""Compatibility helpers for deprecated csv_api parameters."""

# standard library
import warnings
from typing import Optional


def warn_per_call_normalize(normalize_columns: Optional[bool]) -> Optional[bool]:
    """Warn when the deprecated per-call ``normalize_columns`` argument is used.

    Args:
        normalize_columns (bool or None): The per-call value. ``None`` means
        "inherit the instance-level setting" and is the non-deprecated path.

    Returns:
        The value unchanged, so callers can pass through in one expression.
    """
    if normalize_columns is not None:
        warnings.warn(
            "Passing normalize_columns per call is deprecated and will be removed in a "
            "future release. Set normalize_columns on the CSVReader/CSVWriter "
            "constructor instead.",
            DeprecationWarning,
            stacklevel=3,
        )
    return normalize_columns
