"""The contract every CSV compute backend must satisfy.

``_compute.backend()`` returns either ``datagrunt._native`` (Rust) or
``_compute_python`` (the parity oracle). Both must expose the same surface.
This module states that surface once so mypy enforces it at edit time,
rather than leaving it to the runtime parity suite to discover.

Parameters are positional-only throughout. This is not stylistic: the Rust
bindings name the first parameter ``path`` while the oracle names it
``filepath``, and neither accepts the other's keyword, so no keyword call is
portable across backends.
"""

from __future__ import annotations

import os
from typing import Protocol, TypedDict

StrPath = str | os.PathLike[str]


class HeaderProbe(TypedDict):
    """Single-pass header probe result shared by delimiter and dialect inference."""

    empty: bool
    blank: bool
    first_row: str
    sample_rows: list[str]
    sample_lines: list[str]


class SniffedDialect(TypedDict):
    """Sniffed CSV dialect. Constant fields mirror ``csv.Sniffer``."""

    delimiter: str
    quotechar: str
    escapechar: None
    doublequote: bool
    lineterminator: str
    skipinitialspace: bool
    quoting: int


# Deliberately not @runtime_checkable, unlike protocol.py's Protocols: that
# decorator only enables isinstance() checks, which do not apply here since
# a backend is a module, not an instance — and isinstance() against a
# Protocol checks member presence, not signatures, which is what this
# contract actually needs mypy to verify structurally.
class ComputeBackendProtocol(Protocol):
    """Structural interface every CSV compute backend must satisfy."""

    def is_legacy_mac_newlines(self, path: StrPath, /) -> bool:
        """Whether the file uses bare-CR line endings."""
        ...

    def leading_rows(self, path: StrPath, limit: int, /) -> list[str]:
        """Up to ``limit`` stripped, non-blank, non-comment rows."""
        ...

    def first_row(self, path: StrPath, /) -> str:
        """First stripped, non-blank, non-comment row ("" if none)."""
        ...

    def count_leading_comments(self, path: StrPath, /) -> int:
        """Number of leading comment rows before the header."""
        ...

    def count_leading_physical_lines_before_header(self, path: StrPath, /) -> int:
        """Number of physical lines preceding the header row."""
        ...

    def normalize_columns(self, names: list[str], /) -> list[str]:
        """Normalize column names to unique lowercase identifiers."""
        ...

    def infer_delimiter(self, path: StrPath, /) -> str:
        """Infer the field delimiter."""
        ...

    def row_count_with_header(self, path: StrPath, delimiter: str, /) -> int:
        """Total row count including the header row."""
        ...

    def check_ragged(self, path: StrPath, delimiter: str, /) -> bool:
        """Whether any row's field count differs from the header's."""
        ...

    def sniff_dialect(self, path: StrPath, delimiter: str | None = None, /) -> SniffedDialect | None:
        """Sniff the dialect; ``None`` for empty, blank, or undeterminable input."""
        ...

    def probe_csv_header(self, path: StrPath, /) -> HeaderProbe:
        """Single-pass probe feeding both delimiter and dialect inference."""
        ...
