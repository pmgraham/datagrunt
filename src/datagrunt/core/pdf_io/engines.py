"""Module to create engines for PDF processing."""

# standard library
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.pdf_io import extractors, pdfcomponents


def set_export_filename(default_filename, export_filename=None):
    """Return the export filename if provided, otherwise the default.

    Mirrors the CSV writer's path-resolution semantics without coupling PDF to
    DuckDB.
    """
    return export_filename if export_filename else default_filename


@dataclass
class PDFEngineProperties:
    """Base properties for PDF operations."""

    filepath: Path
    default_workers: int = 4
    json_export_filename: str = "output.json"
    json_newline_export_filename: str = "output.jsonl"
    images_export_dir: str = "output_images"
    valid_engines: tuple = ("pymupdf",)
    value_error_message: str = (
        "Engine '{engine}' is not 'pymupdf'. Pass 'pymupdf' as a valid engine param."
    )


class PDFBaseReaderEngine(ABC):
    """Abstract base class defining the interface for PDF reader engines."""

    def __init__(self, filepath, workers: int = 4):
        """Initialize the PDF reader engine.

        Args:
            filepath (str or Path): Path to the PDF file.
            workers (int): Number of concurrent per-page workers.
        """
        self.filepath = Path(filepath)
        self.workers = workers
        if not self.filepath.exists():
            raise FileNotFoundError

    @abstractmethod
    def get_sample(self) -> dict:
        """Return the parsed first page."""
        pass

    @abstractmethod
    def to_dicts(self, image_output_dir: Optional[str] = None) -> dict:
        """Return the unified parsed document dict."""
        pass

    @abstractmethod
    def to_dataframe(self) -> pl.DataFrame:
        """Return parsed elements as a Polars DataFrame."""
        pass

    @abstractmethod
    def to_arrow_table(self) -> pa.Table:
        """Return parsed elements as a PyArrow table."""
        pass


class PDFReaderPyMuPDFEngine(PDFBaseReaderEngine):
    """Read and parse PDF files using PyMuPDF / pdfplumber / Tesseract."""

    def _total_pages(self) -> int:
        pymupdf = extractors._import_pymupdf()
        doc = pymupdf.open(self.filepath)
        try:
            return doc.page_count
        finally:
            doc.close()

    def to_dicts(self, image_output_dir: Optional[str] = None) -> dict:
        """Parse all pages concurrently into the unified document dict."""
        total_pages = self._total_pages()
        page_results = {}
        errors = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(
                    pdfcomponents.parse_page, str(self.filepath), idx, image_output_dir
                ): idx
                for idx in range(total_pages)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    page = future.result()
                    page_results[page["page_number"]] = page
                except Exception as e:  # noqa: BLE001 - per-page isolation
                    errors.append(f"Page {idx + 1}: {e}")

        ordered = [page_results[p] for p in sorted(page_results.keys())]
        return pdfcomponents.combine_pages(self.filepath, total_pages, ordered, errors)

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        return pdfcomponents.parse_page(str(self.filepath), 0)

    def to_dataframe(self) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element)."""
        records = pdfcomponents.flatten_document_elements(self.to_dicts())
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def to_arrow_table(self) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        records = pdfcomponents.flatten_document_elements(self.to_dicts())
        if not records:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(records)
