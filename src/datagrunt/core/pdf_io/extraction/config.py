"""Private, picklable PDF-extraction tunables threaded to the backends.

Not public API: the leading underscore and absence from every ``__all__`` keep
``_PDFExtractionConfig`` an internal detail. The public surface is the
``min_image_dimension``, ``ocr_standard_dpi``, ``ocr_large_format_dpi``,
``ocr_large_format_dimension``, and ``render_dpi`` keyword arguments on
``PDFReader``/``PDFWriter``.
"""

from dataclasses import dataclass

# Single source of truth for the default minimum kept image dimension (px).
# Images smaller than this on either side are dropped as layout artifacts.
_DEFAULT_MIN_IMAGE_DIMENSION = 40

# Single source of truth for the OCR dynamic-DPI scaling thresholds and the
# default render DPI. These used to live in ocr.py; they moved here so that
# ocr.py can import both them and `_PDFExtractionConfig` from one place.
# ocr.py's `dpi_for_page` takes a `_PDFExtractionConfig`, so the dependency
# must run config -> ocr, never the reverse, or the two modules would import
# each other.
STANDARD_DPI = 150
LARGE_FORMAT_DPI = 75
LARGE_FORMAT_DIMENSION = 1500
_DEFAULT_RENDER_DPI = 300


@dataclass(frozen=True)
class _PDFExtractionConfig:
    """Frozen, picklable tunables for PDF extraction.

    Frozen so it is hashable and safe to share, and picklable (primitives only)
    so it can cross the pdfium process-pool worker boundary unchanged.
    """

    min_image_dimension: int = _DEFAULT_MIN_IMAGE_DIMENSION
    ocr_standard_dpi: int = STANDARD_DPI
    ocr_large_format_dpi: int = LARGE_FORMAT_DPI
    ocr_large_format_dimension: int = LARGE_FORMAT_DIMENSION
    render_dpi: int = _DEFAULT_RENDER_DPI

    def __post_init__(self) -> None:
        # min_image_dimension alone allows 0 (a documented way to disable the
        # small-image filter); every DPI/threshold field must be >= 1 since a
        # zero or negative DPI or dimension threshold is nonsense.
        self._validate_int_field("min_image_dimension", min_value=0)
        self._validate_int_field("ocr_standard_dpi", min_value=1)
        self._validate_int_field("ocr_large_format_dpi", min_value=1)
        self._validate_int_field("ocr_large_format_dimension", min_value=1)
        self._validate_int_field("render_dpi", min_value=1)

    def _validate_int_field(self, name: str, *, min_value: int) -> None:
        """Reject bool, non-int, and below-``min_value`` values for field ``name``."""
        value = getattr(self, name)
        # bool is a subclass of int; reject it explicitly so True/False cannot
        # masquerade as 1/0.
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an int, got {value!r} ({type(value).__name__})")
        if value < min_value:
            raise ValueError(f"{name} must be >= {min_value}, got {value}")
