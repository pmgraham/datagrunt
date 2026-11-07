"""Structural contract that both PDF compute backends (Rust `_native` and `_compute_python`) must satisfy."""

from collections.abc import Sequence
from typing import Protocol


class PDFComputeBackendProtocol(Protocol):
    """Contract enforced by mypy on both `datagrunt._native` and `datagrunt.pdf._compute_python`."""

    def pdf_char_bgr_medians(
        self,
        pixels: bytes,
        px_width: int,
        px_height: int,
        scale_x: float,
        scale_y: float,
        boxes: Sequence[tuple[float, float, float, float]],
        /,
    ) -> list[tuple[int, int, int]]: ...

    def pdf_union_area(
        self,
        boxes: Sequence[tuple[float, float, float, float]],
        /,
    ) -> float: ...

    def pdf_char_reason_runs(
        self,
        char_boxes: Sequence[tuple[float, float, float, float]],
        width: float,
        height: float,
        no_paint: bool,
        under: Sequence[tuple[float, float, float, float]],
        /,
    ) -> list[tuple[int, int, bool, bool]]: ...

    def pdf_snap_and_merge_segments(
        self,
        segments: Sequence[tuple[float, float, float]],
        snap_pt: float,
        /,
    ) -> list[tuple[float, float, float]]: ...

    def pdf_lzw_decode(
        self,
        data: bytes,
        early: int,
        max_output: int,
        /,
    ) -> list[bytes]: ...
