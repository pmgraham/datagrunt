"""Shared engine lifecycle for the PDF public API (PDFReader/PDFWriter)."""

from functools import cached_property
from pathlib import Path

from datagrunt.core import PDFComponents, PDFEngineFactory


class _PDFEngineBacked(PDFComponents):
    """Engine ownership shared by PDFReader and PDFWriter.

    Holds the identical PDF constructor and the single cached engine (built via
    ``PDFEngineFactory`` by role), so both classes parse the source once per
    instance and neither duplicates the factory wiring. PDF engines hold no
    external connection (only in-memory parse caches), so there is no
    ``close()``/context-manager here.
    """

    _engine_role = None  # "reader" or "writer"

    def __init__(self, filepath, engine="pdfium", workers=1, native=False):
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
        """
        if not isinstance(filepath, dict):
            filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers
        self.native = native

    @cached_property
    def _engine(self):
        """Build this object's engine once and reuse it across calls.

        Caching means repeated/mixed conversions reuse a single parse (the engine
        memoizes internally) instead of re-parsing the PDF on every call.
        Released when this object goes out of scope.
        """
        factory = PDFEngineFactory(
            self.filepath, self.engine, self.workers, structured=not self.native
        )
        if self._engine_role == "reader":
            return factory.create_reader()
        return factory.create_writer()
