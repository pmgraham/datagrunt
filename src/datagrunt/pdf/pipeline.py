"""The v5 PDF document pipeline (#372, A8a): spec §3.3 steps 1-5, page by page, then the document.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A7's
``PDFWriter`` and A8b's ``PDFReader`` stand on it.

:class:`PipelineOptions` holds the reader's and writer's extraction options
and runs the spec's construction checks (invariant 2) when it is built, so
a bad option fails before any work. :class:`PDFSource` is where a document
is opened from (its path and password), in this process or in a worker.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import get_args

from datagrunt.pdf._validate import choice_str, int_at_least, strict_bool
from datagrunt.pdf.assembly import MIN_IMAGE_DIMENSION
from datagrunt.pdf.pdfium_engine import _checked_password, _checked_path
from datagrunt.pdf.quality import OcrMode
from datagrunt.pdf.render import _MIN_RENDER_PIXELS, _MIN_TARGET_DPI

__all__ = ["PDFSource", "PipelineOptions"]

_OCR_MODES: tuple[OcrMode, ...] = get_args(OcrMode)


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineOptions:
    """The extraction options of ``PDFReader`` and ``PDFWriter``, checked at construction (spec invariant 2).

    Picklable, so it crosses the process pool.

    Attributes:
        workers: Processes to extract pages with (>= 1); 1 extracts in this one.
        ocr: ``"auto"``, ``"always"`` or ``"never"`` (A3's routing option).
        dpi: The page render's target DPI (>= 36). A5b renders with it.
        vision_dpi: A vision page's render DPI (>= ``dpi``). A5b renders with it.
        max_render_pixels: The pixel bound of any render (>= 262,144, 512 x
            512). A5b renders within it.
        min_image_dimension: The smallest image side, in pixels, that is a
            figure (>= 1).
        strict: Raise :class:`~datagrunt.pdf.exceptions.PDFCorruptError` on a
            bad page instead of recording it as an error page.

    Raises:
        TypeError: If a value is of the wrong type (bool is no int).
        ValueError: If ``workers < 1``, ``dpi < 36``, ``vision_dpi < dpi``,
            ``max_render_pixels < 262_144``, ``min_image_dimension < 1``, or
            ``ocr`` is not one of the three modes.
    """

    workers: int = 1
    ocr: OcrMode = "auto"
    dpi: int = 200
    vision_dpi: int = 300
    max_render_pixels: int = 16_000_000
    min_image_dimension: int = MIN_IMAGE_DIMENSION
    strict: bool = False

    def __post_init__(self) -> None:
        int_at_least("workers", self.workers, 1)
        int_at_least("dpi", self.dpi, _MIN_TARGET_DPI)
        int_at_least("vision_dpi", self.vision_dpi, _MIN_TARGET_DPI)
        if self.vision_dpi < self.dpi:
            raise ValueError(f"vision_dpi must be >= dpi ({self.dpi}), got {self.vision_dpi}")
        int_at_least("max_render_pixels", self.max_render_pixels, _MIN_RENDER_PIXELS)
        int_at_least("min_image_dimension", self.min_image_dimension, 1)
        choice_str("ocr", self.ocr, _OCR_MODES)
        strict_bool("strict", self.strict)


@dataclass(frozen=True, slots=True, kw_only=True)
class PDFSource:
    """Where the document is opened from: in this process, or reopened in a worker.

    Picklable, so it crosses the process pool (a pdfium handle cannot).

    Attributes:
        path: The PDF's path, made absolute at construction (a worker's
            ``chdir`` cannot redirect it).
        password: The encrypted PDF's password, or None. Excluded from repr
            and equality, and never put in an error message; a pickle of the
            source carries it (the worker needs it).

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike of str, or
            ``password`` is not a str or None.
        ValueError: If ``password`` contains NUL.
    """

    path: Path
    password: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _checked_path(self.path, "source").absolute())
        _checked_password(self.password)
