"""Type stub for the compiled Rust extension.

Hand-written to mirror the ``#[pymodule]`` exports in
``rust/datagrunt-python/src/lib.rs``. Without it mypy sees ``_native`` as
``Any``, and the ComputeBackendProtocol conformance check on the Rust side
would pass vacuously.

Guarded by ``test_stub_matches_the_compiled_module`` — adding a
``#[pyfunction]`` without updating this file fails that test.

Parameters are positional-only: PyO3 exposes them under the Rust names, which
differ from the oracle's, so no keyword call is portable across backends.
"""

from collections.abc import Sequence

from datagrunt.core.csv_io._compute_protocol import HeaderProbe, SniffedDialect, StrPath

__all__ = [
    "is_legacy_mac_newlines",
    "leading_rows",
    "first_row",
    "count_leading_comments",
    "count_leading_physical_lines_before_header",
    "normalize_columns",
    "infer_delimiter",
    "row_count_with_header",
    "check_ragged",
    "sniff_dialect",
    "probe_csv_header",
    "pdf_char_bgr_medians",
    "pdf_union_area",
    "pdf_char_reason_runs",
    "pdf_snap_and_merge_segments",
    "pdf_lzw_decode",
]

def is_legacy_mac_newlines(path: StrPath, /) -> bool: ...
def leading_rows(path: StrPath, limit: int, /) -> list[str]: ...
def first_row(path: StrPath, /) -> str: ...
def count_leading_comments(path: StrPath, /) -> int: ...
def count_leading_physical_lines_before_header(path: StrPath, /) -> int: ...
def normalize_columns(names: list[str], /) -> list[str]: ...
def infer_delimiter(path: StrPath, /) -> str: ...
def row_count_with_header(path: StrPath, delimiter: str, /) -> int: ...
def check_ragged(path: StrPath, delimiter: str, /) -> bool: ...
def sniff_dialect(path: StrPath, delimiter: str | None = None, /) -> SniffedDialect | None: ...
def probe_csv_header(path: StrPath, /) -> HeaderProbe: ...
def pdf_char_bgr_medians(
    pixels: bytes,
    px_width: int,
    px_height: int,
    scale_x: float,
    scale_y: float,
    boxes: Sequence[tuple[float, float, float, float]],
    /,
) -> list[tuple[int, int, int]]: ...
def pdf_union_area(
    boxes: Sequence[tuple[float, float, float, float]],
    /,
) -> float: ...
def pdf_char_reason_runs(
    char_boxes: Sequence[tuple[float, float, float, float]],
    width: float,
    height: float,
    no_paint: bool,
    under: Sequence[tuple[float, float, float, float]],
    /,
) -> list[tuple[int, int, bool, bool]]: ...
def pdf_snap_and_merge_segments(
    segments: Sequence[tuple[float, float, float]],
    snap_pt: float,
    /,
) -> list[tuple[float, float, float]]: ...
def pdf_lzw_decode(
    data: bytes,
    early: int,
    max_output: int,
    /,
) -> list[bytes]: ...
