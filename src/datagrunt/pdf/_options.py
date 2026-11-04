"""Extraction options the caller and the workers share (#369, #372): no pdfium.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. The caller
checks its options before any work and sends them to its workers, so these
live apart from the modules only a worker imports: :data:`OcrMode` (A3's,
re-exported by :mod:`datagrunt.pdf.quality`), :data:`MIN_IMAGE_DIMENSION`
(A4b's, re-exported by :mod:`datagrunt.pdf.assembly`), :data:`DPI` and
:data:`MAX_RENDER_PIXELS` (the reader's defaults, and A4b's for a page's
image) and :class:`VisionOptions` (A5b's, re-exported by
:mod:`datagrunt.pdf.vision`).
"""

from dataclasses import dataclass
from typing import Literal, TypeAlias

from datagrunt.pdf._validate import int_at_least, non_blank_str, positive_float

__all__ = ["DPI", "MAX_RENDER_PIXELS", "MEMORY_CAP_BYTES", "MIN_IMAGE_DIMENSION", "OcrMode", "VisionOptions"]

OcrMode: TypeAlias = Literal["auto", "always", "never"]
"""The reader's OCR option: ``"auto"`` routes by the page's text,
``"always"`` sends every page to vision, ``"never"`` routes no page to OCR:
a page without enough valid text, or whose text is mostly invalid, stays
digital (with a warning) instead of going to vision, and one that would be
hybrid stays digital."""

# The default minimum image side in pixels: v4's, kept.
MIN_IMAGE_DIMENSION = 40

# The spec's construction checks (PDFReader / PDFWriter) and defaults: dpi >= 36, 200; vision_dpi >= 36, 300;
# max_render_pixels >= 512 x 512, 16,000,000. A page's image (P2-A) is drawn at dpi.
_MIN_DPI = 36
_MIN_RENDER_PIXELS = 262_144
DPI = 200
MAX_RENDER_PIXELS = 16_000_000
# Every pdfium worker's memory past its start (render, page and figure workers): pdfium can spend ~1.7 GiB decoding an
# image within A2's bounds (_pdfium_images).
MEMORY_CAP_BYTES = 2 << 30
# A render takes ~0.5 s here (a worker's start included) and a tesseract call ~1 s: each bound is over 100 times that.
_RENDER_TIMEOUT_S = 120.0
_OCR_TIMEOUT_S = 120.0
# A vision page's render and OCR together (#418): a page takes 2-4 s here (a render, OSD and 4 bands), so this is over
# 100 times that, and below what the calls' own timeouts allow a tall page (120 s each, a band each 840 rows).
_PAGE_BUDGET_S = 600.0


@dataclass(frozen=True, slots=True, kw_only=True)
class VisionOptions:
    """How a vision page is rendered and read. Picklable, so it crosses the process pool.

    Attributes:
        vision_dpi: The render's target DPI (>= 36).
        max_render_pixels: The render's pixel bound (>= 262,144, 512 x 512).
        language: tesseract's language(s), as ``eng`` or ``deu+eng``.
        memory_cap_bytes: The memory cap past its start (>= 1) of the render
            worker, and of the page worker the caller starts.
        render_timeout_s: Seconds a render may take, its worker's start included.
        ocr_timeout_s: Seconds each tesseract call may take.
        page_budget_s: Seconds a page's render and OCR may take in all (see
            :mod:`datagrunt.pdf.vision`): each call within its own timeout
            and the time left.

    Raises:
        TypeError, ValueError: If a value is of the wrong type or out of range.
    """

    vision_dpi: int = 300
    max_render_pixels: int = MAX_RENDER_PIXELS
    language: str = "eng"
    memory_cap_bytes: int = MEMORY_CAP_BYTES
    render_timeout_s: float = _RENDER_TIMEOUT_S
    ocr_timeout_s: float = _OCR_TIMEOUT_S
    page_budget_s: float = _PAGE_BUDGET_S

    def __post_init__(self) -> None:
        int_at_least("vision_dpi", self.vision_dpi, _MIN_DPI)
        int_at_least("max_render_pixels", self.max_render_pixels, _MIN_RENDER_PIXELS)
        non_blank_str("language", self.language)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)
        object.__setattr__(self, "render_timeout_s", positive_float("render_timeout_s", self.render_timeout_s))
        object.__setattr__(self, "ocr_timeout_s", positive_float("ocr_timeout_s", self.ocr_timeout_s))
        object.__setattr__(self, "page_budget_s", positive_float("page_budget_s", self.page_budget_s))
