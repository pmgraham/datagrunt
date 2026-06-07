"""Abstract interface for page-level PDF extraction backends."""

from abc import ABC, abstractmethod
from pathlib import Path

from datagrunt.core.pdf_io.extraction.shapes import ImageBlock, OcrBlock, PageAnalysis, TextBlock


class ExtractionBackend(ABC):
    """Page-level extraction primitives for one PDF using one engine.

    Concrete backends return datagrunt dataclasses with identical shapes so the
    shared assembly pipeline (``pdfcomponents.parse_page``) is engine-agnostic.
    Hard failures (missing file, page out of range) raise; soft per-category
    failures should return empty lists so a page still parses.
    """

    def __init__(self, filepath):
        """Store the path and verify it exists.

        Args:
            filepath (str or Path): Path to the PDF file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        self.filepath = Path(filepath)
        if not self.filepath.exists():
            raise FileNotFoundError(self.filepath)

    def __enter__(self):
        """Enter a context session (reusing open resources)."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context session (cleaning up resources)."""
        pass

    @abstractmethod
    def analyze_page(self, page_number: int) -> PageAnalysis:
        """Return summary metadata for ``page_number`` (0-indexed)."""

    @abstractmethod
    def extract_text_blocks(self, page_number: int) -> list[TextBlock]:
        """Return classified text blocks for the page."""

    @abstractmethod
    def extract_images(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> list[ImageBlock]:
        """Return embedded images; write files when ``output_dir`` is given."""

    @abstractmethod
    def ocr_page(self, page_number: int, dpi: int = 300) -> list[OcrBlock]:
        """Return OCR-recovered text lines for the page."""
