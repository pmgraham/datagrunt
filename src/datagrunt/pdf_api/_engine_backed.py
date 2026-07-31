"""Shared engine lifecycle for the PDF public API (PDFReader/PDFWriter)."""

from functools import cached_property
from pathlib import Path
from typing import ClassVar

from datagrunt.core import PDFComponents, PDFEngineFactory
from datagrunt.core.pdf_io.extraction.config import _PDFExtractionConfig


class _PDFEngineBacked(PDFComponents):
    """Engine ownership shared by PDFReader and PDFWriter.

    Holds the identical PDF constructor and the single cached engine (built via
    ``PDFEngineFactory`` by role), so both classes parse the source once per
    instance and neither duplicates the factory wiring. PDF engines hold no
    external connection (only in-memory parse caches), so there is no
    ``close()``/context-manager here.
    """

    _engine_role: ClassVar[str | None] = None  # "reader" or "writer"

    def __init__(
        self,
        filepath,
        engine="pdfium",
        workers=1,
        native=False,
        *,
        min_image_dimension=None,
        ocr_standard_dpi=None,
        ocr_large_format_dpi=None,
        ocr_large_format_dimension=None,
        render_dpi=None,
    ):
        """Initialize a PDF reader/writer.

        Args:
            filepath (str, Path, or dict): Path to the PDF/JSON file, or a
                parsed document dict.
            engine (str, default 'pdfium'): Parsing engine to instantiate. One of
                'pdfium' (default -- permissive license; emits the unified element
                schema by default, or the lean native schema when ``native=True``)
                or 'pymupdf' (unified element schema, tables + OCR).
            workers (int, default 1): Number of concurrent per-page workers.
            native (bool, default False): pdfium only -- when True, emit the lean
                native schema instead of the unified element schema. Ignored by
                the pymupdf engine.
            min_image_dimension (int, optional, keyword-only): Minimum embedded-image
                pixel size (either side) to keep; smaller images are dropped as
                layout artifacts. Defaults to 40. Use 0 to keep every image.
            ocr_standard_dpi (int, optional, keyword-only): OCR render DPI used for
                normal-sized pages. Defaults to 150.
            ocr_large_format_dpi (int, optional, keyword-only): OCR render DPI used
                for large-format pages (see ``ocr_large_format_dimension``).
                Defaults to 75.
            ocr_large_format_dimension (int, optional, keyword-only): Page width/height
                threshold (points); a page with either side above this is treated as
                large-format and rendered at ``ocr_large_format_dpi`` instead of
                ``ocr_standard_dpi``. Defaults to 1500.
            render_dpi (int, optional, keyword-only): Default DPI for rendering pages
                to images. Defaults to 300.
        """
        if not isinstance(filepath, dict):
            filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers
        self.native = native
        overrides = {
            k: v
            for k, v in {
                "min_image_dimension": min_image_dimension,
                "ocr_standard_dpi": ocr_standard_dpi,
                "ocr_large_format_dpi": ocr_large_format_dpi,
                "ocr_large_format_dimension": ocr_large_format_dimension,
                "render_dpi": render_dpi,
            }.items()
            if v is not None
        }
        # Validates immediately (fail-fast at construction).
        self._extraction_config = _PDFExtractionConfig(**overrides)

    @cached_property
    def _engine(self):
        """Build this object's engine once and reuse it across calls.

        Caching means repeated/mixed conversions reuse a single parse (the engine
        memoizes internally) instead of re-parsing the PDF on every call.
        Released when this object goes out of scope.
        """
        factory = PDFEngineFactory(
            self.filepath,
            self.engine,
            self.workers,
            structured=not self.native,
            extraction_config=self._extraction_config,
        )
        if self._engine_role == "reader":
            return factory.create_reader()
        if self._engine_role == "writer":
            return factory.create_writer()
        raise NotImplementedError(f"Subclass must set _engine_role to 'reader' or 'writer', got {self._engine_role!r}")
