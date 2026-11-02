"""pdfium helpers shared by the v5 engine's modules (#365).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Imports
neither :mod:`datagrunt.pdf.pdfium_engine` nor :mod:`datagrunt.pdf._pdfium_text`,
so both import from here without a cycle.
"""

import ctypes
from collections.abc import Callable

import pypdfium2.raw as pdfium_c

from datagrunt.pdf._boxes import Box, union_box

__all__ = ["Box", "PdfiumFailure", "object_address", "read_rgba", "union_box"]


class PdfiumFailure(Exception):
    """A pdfium call failed; the engine reports it as PDFCorruptError for the page.

    The message completes "pdfium cannot ...".
    """


def object_address(pointer: pdfium_c.FPDF_PAGEOBJECT) -> int:
    """The address a pdfium object pointer holds (0 for NULL), to compare objects."""
    return ctypes.cast(pointer, ctypes.c_void_p).value or 0


def read_rgba(
    getter: Callable[..., int], page_object: pdfium_c.FPDF_PAGEOBJECT, what: str
) -> tuple[int, int, int, int]:
    """Read a page object's fill or stroke colour (``what``) as 0-255 (R, G, B, A)."""
    red, green, blue, alpha = (ctypes.c_uint() for _ in range(4))
    if not getter(page_object, red, green, blue, alpha):
        raise PdfiumFailure(f"read the {what}")
    return red.value, green.value, blue.value, alpha.value
