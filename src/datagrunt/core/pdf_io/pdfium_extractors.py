"""Pure-Python PDF extractor functions backed by PDFium (pypdfium2).

Ported from a standalone ``pdf_deconstruct.py`` script. This engine emits a
*native* schema (full page text, positioned text objects, and embedded image
files) that is distinct from the pymupdf unified element schema. Third-party
imports are performed lazily so that ``import datagrunt`` works on a base
install without the optional ``[pdf]`` extra.
"""

# standard library
import glob
import hashlib
import os
from pathlib import Path

# local libraries
from datagrunt.core.pdf_io import extractors
from datagrunt.core.pdf_io.extractors import PDF_EXTRA_HINT

# OCR fallback DPI scaling (mirrors pdfcomponents thresholds).
LARGE_FORMAT_DIMENSION = 1500
LARGE_FORMAT_DPI = 75
STANDARD_DPI = 150


def _import_pdfium():
    """Import pypdfium2 lazily with a helpful error if the extra is missing."""
    try:
        import pypdfium2 as pdfium
        import pypdfium2.raw as raw
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pdfium, raw


def _topleft_xywh(bbox: list, page_height: float) -> dict:
    """Convert PDFium's [left, bottom, right, top] (bottom-left origin) into a
    top-left {x, y, w, h} box, matching the convention datagrunt uses."""
    left, bottom, right, top = bbox
    return {
        "x": round(left, 2),
        "y": round(page_height - top, 2),
        "w": round(right - left, 2),
        "h": round(top - bottom, 2),
    }
