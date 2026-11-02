"""Boxes the v5 engine and its caller share: no pdfium.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf._pdfium_common` re-exports both names.
"""

__all__ = ["Box", "union_box"]

Box = tuple[float, float, float, float]


def union_box(boxes: tuple[Box, ...]) -> Box:
    """The smallest box holding every one of ``boxes`` (at least one)."""
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )
