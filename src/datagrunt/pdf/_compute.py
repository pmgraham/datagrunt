"""Dispatch between the Rust extension and the pure-Python PDF compute reference.

Follows the exact pattern of ``datagrunt.core.csv_io._compute``:
- ``DATAGRUNT_DISABLE_RUST=1`` env var
- ``set_disable_rust(bool)``
- ``rust_disabled()`` context manager
- ``backend() -> PDFComputeBackendProtocol``
"""

import contextlib
import os
from collections.abc import Iterator
from typing import TYPE_CHECKING

from datagrunt import _native
from datagrunt.pdf import _compute_python
from datagrunt.pdf._compute_protocol import PDFComputeBackendProtocol

if TYPE_CHECKING:
    _python_backend: PDFComputeBackendProtocol = _compute_python
    _rust_backend: PDFComputeBackendProtocol = _native

_DISABLE_RUST = os.environ.get("DATAGRUNT_DISABLE_RUST", "") not in ("", "0", "false", "False")


def set_disable_rust(value: bool) -> None:
    """Force the pure-Python path (True) or Rust (False). Hidden test hook."""
    global _DISABLE_RUST
    _DISABLE_RUST = bool(value)


@contextlib.contextmanager
def rust_disabled() -> Iterator[None]:
    """Temporarily force the pure-Python path; restore the prior state on exit."""
    global _DISABLE_RUST
    previous = _DISABLE_RUST
    _DISABLE_RUST = True
    try:
        yield
    finally:
        _DISABLE_RUST = previous


def backend() -> PDFComputeBackendProtocol:
    """Return the active PDF compute backend module (read at call time)."""
    return _compute_python if _DISABLE_RUST else _native
