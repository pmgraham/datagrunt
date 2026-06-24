"""Private, picklable PDF-extraction tunables threaded to the backends.

Not public API: the leading underscore and absence from every ``__all__`` keep
``_PDFExtractionConfig`` an internal detail. The public surface is the
``min_image_dimension`` keyword argument on ``PDFReader``/``PDFWriter``.
"""

from dataclasses import dataclass

# Single source of truth for the default minimum kept image dimension (px).
# Images smaller than this on either side are dropped as layout artifacts.
_DEFAULT_MIN_IMAGE_DIMENSION = 40


@dataclass(frozen=True)
class _PDFExtractionConfig:
    """Frozen, picklable tunables for PDF extraction.

    Frozen so it is hashable and safe to share, and picklable (primitives only)
    so it can cross the pdfium process-pool worker boundary unchanged.
    """

    min_image_dimension: int = _DEFAULT_MIN_IMAGE_DIMENSION

    def __post_init__(self):
        value = self.min_image_dimension
        # bool is a subclass of int; reject it explicitly so True/False cannot
        # masquerade as 1/0.
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(
                f"min_image_dimension must be an int, got {value!r} "
                f"({type(value).__name__})"
            )
        if value < 0:
            raise ValueError(f"min_image_dimension must be >= 0, got {value}")
