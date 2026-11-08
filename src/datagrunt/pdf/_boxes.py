"""Boxes the v5 engine and its caller share: no pdfium.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf._pdfium_common` re-exports both names.
"""

__all__ = ["Box", "union_box"]

Box = tuple[float, float, float, float]


def union_box(boxes: tuple[Box, ...]) -> Box:
    """The smallest box holding every one of ``boxes`` (at least one)."""
    if len(boxes) == 1:
        return boxes[0]
    if not boxes:
        raise ValueError("union_box() arg is an empty sequence")
    x0, y0, x1, y1 = boxes[0]
    for b in boxes[1:]:
        if b[0] < x0:
            x0 = b[0]
        if b[1] < y0:
            y0 = b[1]
        if b[2] > x1:
            x1 = b[2]
        if b[3] > y1:
            y1 = b[3]
    return x0, y0, x1, y1
