"""Module to create engines for PDF processing."""

# standard library
import json
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
    value_error_message: str = "Engine '{engine}' is not 'pymupdf'. Pass 'pymupdf' as a valid engine param."


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
                executor.submit(pdfcomponents.parse_page, str(self.filepath), idx, image_output_dir): idx
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


class PDFBaseWriterEngine(ABC):
    """Abstract base class defining the interface for PDF writer engines."""

    def __init__(self, filepath, workers: int = 4):
        """Initialize the PDF writer engine.

        Args:
            filepath (str or Path): Path to the PDF file.
            workers (int): Number of concurrent per-page workers.
        """
        self.filepath = Path(filepath)
        self.workers = workers
        self.properties = PDFEngineProperties(filepath=self.filepath)
        if not self.filepath.exists():
            raise FileNotFoundError

    @abstractmethod
    def write_json(self, export_filename=None, image_output_dir=None):
        """Write the unified document JSON to disk."""
        pass

    @abstractmethod
    def write_json_newline_delimited(self, export_filename=None, image_output_dir=None):
        """Write one element per line as JSON Lines."""
        pass

    @abstractmethod
    def extract_images(self, output_dir=None):
        """Write embedded images to disk; return their paths."""
        pass


class PDFWriterPyMuPDFEngine(PDFBaseWriterEngine):
    """Write parsed PDF output (JSON + image files) using PyMuPDF."""

    def _reader(self):
        return PDFReaderPyMuPDFEngine(self.filepath, workers=self.workers)

    def write_json(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write the unified document JSON.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the JSON; otherwise image
                ``file_path`` values are null.
        """
        filename = set_export_filename(self.properties.json_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir)
        with open(filename, "w") as f:
            json.dump(document, f, indent=2)
        return filename

    def write_json_newline_delimited(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(self.properties.json_newline_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir)
        records = pdfcomponents.flatten_document_elements(document)
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def extract_images(self, output_dir=None):
        """Parse the PDF, write embedded images to disk, return their paths."""
        directory = output_dir if output_dir else self.properties.images_export_dir
        document = self._reader().to_dicts(image_output_dir=directory)
        paths = []
        for page in document.get("document", {}).get("pages", []):
            for elem in page.get("elements", []):
                if elem.get("type") == "image":
                    fp = (elem.get("metadata") or {}).get("file_path")
                    if fp:
                        paths.append(fp)
        return paths
