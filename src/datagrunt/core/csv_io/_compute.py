"""Dispatch between the Rust extension and the pure-Python reference.

The CSV-compute sites in ``csvcomponents`` call ``backend().<fn>(...)``.
``backend()`` is read at call time, so toggling takes effect immediately.

HIDDEN toggle — for testing/diagnostics only; intentionally undocumented and not
exported in any ``__all__``:
- env var ``DATAGRUNT_DISABLE_RUST`` (set to a non-empty, non-"0"/"false" value)
- ``set_disable_rust(bool)``
- ``rust_disabled()`` context manager (scoped, auto-resets)

Default is Rust (``disable_rust=False``); the binary always ships and is the
default path.

NOT thread-safe: ``set_disable_rust`` / ``rust_disabled`` flip a shared module
global with no lock, so toggling from one thread changes the backend for all
threads mid-flight. Use the toggle only in single-threaded test/diagnostic
contexts, never to switch backends per-thread at runtime.
"""

import contextlib
import os
from typing import TYPE_CHECKING

from datagrunt import _native
from datagrunt.core.csv_io import _compute_python
from datagrunt.core.csv_io._compute_protocol import ComputeBackendProtocol

if TYPE_CHECKING:
    # Assigning each backend module to a Protocol-typed name makes mypy verify
    # both satisfy the contract. Drift in either is reported here, at edit time,
    # naming the offending member — rather than as a parity-suite failure later.
    _python_backend: ComputeBackendProtocol = _compute_python
    _rust_backend: ComputeBackendProtocol = _native

_DISABLE_RUST = os.environ.get("DATAGRUNT_DISABLE_RUST", "") not in ("", "0", "false", "False")


def set_disable_rust(value):
    """Force the pure-Python path (True) or Rust (False). Hidden test hook."""
    global _DISABLE_RUST
    _DISABLE_RUST = bool(value)


@contextlib.contextmanager
def rust_disabled():
    """Temporarily force the pure-Python path; restore the prior state on exit."""
    global _DISABLE_RUST
    previous = _DISABLE_RUST
    _DISABLE_RUST = True
    try:
        yield
    finally:
        _DISABLE_RUST = previous


def backend() -> ComputeBackendProtocol:
    """Return the active compute backend module (read at call time)."""
    return _compute_python if _DISABLE_RUST else _native
