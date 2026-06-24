"""Factory module for creating PDF engine instances."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core.pdf_io.engines import (
    PDFEngineProperties,
    PDFReaderPdfiumEngine,
    PDFReaderPyMuPDFEngine,
    PDFWriterPdfiumEngine,
    PDFWriterPyMuPDFEngine,
)


class PDFEngineFactory:
    """Factory class for creating PDF reader and writer engine instances."""

    READER_ENGINES = {
        "pymupdf": PDFReaderPyMuPDFEngine,
        "pdfium": PDFReaderPdfiumEngine,
    }

    WRITER_ENGINES = {
        "pymupdf": PDFWriterPyMuPDFEngine,
        "pdfium": PDFWriterPdfiumEngine,
    }

    def __init__(self, filepath, engine, workers: int = 1, structured: bool = False, extraction_config=None):
        """Initialize the PDF Engine Factory class.

        Args:
            filepath (str or Path): Path to the PDF file.
            engine (str): Engine type to create.
            workers (int): Number of concurrent per-page workers.
            structured (bool): Emit unified element schema when True (pdfium only).
            extraction_config: Optional extraction config forwarded to the created engine.
        """
        self.filepath = Path(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers
        self.structured = structured
        self.extraction_config = extraction_config
        if not self.filepath.exists():
            raise FileNotFoundError
        if self.engine not in PDFEngineProperties.valid_engines:
            raise ValueError(
                PDFEngineProperties.value_error_message.format(
                    engine=self.engine,
                    valid=", ".join(PDFEngineProperties.valid_engines),
                )
            )

    def create_reader(self):
        """Create a PDF reader engine instance."""
        engine_class = self.READER_ENGINES.get(self.engine)
        if engine_class:
            if self.engine == "pdfium":
                return engine_class(
                    self.filepath, workers=self.workers, structured=self.structured,
                    extraction_config=self.extraction_config,
                )
            return engine_class(self.filepath, workers=self.workers, extraction_config=self.extraction_config)
        raise ValueError(f"Unsupported reader engine: {self.engine}")

    def create_writer(self):
        """Create a PDF writer engine instance."""
        engine_class = self.WRITER_ENGINES.get(self.engine)
        if engine_class:
            if self.engine == "pdfium":
                return engine_class(
                    self.filepath, workers=self.workers, structured=self.structured,
                    extraction_config=self.extraction_config,
                )
            return engine_class(self.filepath, workers=self.workers, extraction_config=self.extraction_config)
        raise ValueError(f"Unsupported writer engine: {self.engine}")
