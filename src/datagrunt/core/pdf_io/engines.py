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
from datagrunt.core.pdf_io import extractors, pdfcomponents, pdfium_extractors


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
    valid_engines: tuple = ("pymupdf", "pdfium")
    value_error_message: str = "Engine '{engine}' is not supported. Valid engines: {valid}."


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
    def to_dicts(self, image_output_dir: Optional[str] = None, drop_layout_tables: bool = False) -> dict:
        """Return the unified parsed document dict."""
        pass

    @abstractmethod
    def to_dataframe(self, drop_layout_tables: bool = False) -> pl.DataFrame:
        """Return parsed elements as a Polars DataFrame."""
        pass

    @abstractmethod
    def to_arrow_table(self, drop_layout_tables: bool = False) -> pa.Table:
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

    def to_dicts(self, image_output_dir: Optional[str] = None, drop_layout_tables: bool = False) -> dict:
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
        document = pdfcomponents.combine_pages(self.filepath, total_pages, ordered, errors)
        if drop_layout_tables:
            pdfcomponents.drop_layout_tables(document)
        return document

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        return pdfcomponents.parse_page(str(self.filepath), 0)

    def to_dataframe(self, drop_layout_tables: bool = False) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element)."""
        records = pdfcomponents.flatten_document_elements(self.to_dicts(drop_layout_tables=drop_layout_tables))
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def to_arrow_table(self, drop_layout_tables: bool = False) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        records = pdfcomponents.flatten_document_elements(self.to_dicts(drop_layout_tables=drop_layout_tables))
        if not records:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(records)


class PDFReaderPdfiumEngine(PDFBaseReaderEngine):
    """Read and parse PDF files using PDFium (pypdfium2).

    Emits the native PDFium schema (full page text, positioned text objects,
    and embedded image files). Image-only pages fall back to OCR so extraction
    stays complete. ``drop_layout_tables`` is accepted for interface parity but
    is a no-op (PDFium has no table detection).
    """

    def _total_pages(self) -> int:
        pdfium, _ = pdfium_extractors._import_pdfium()
        pdf = pdfium.PdfDocument(str(self.filepath))
        try:
            return len(pdf)
        finally:
            pdf.close()

    def to_dicts(self, image_output_dir: Optional[str] = None, drop_layout_tables: bool = False) -> dict:
        """Parse all pages concurrently into the native document dict."""
        total_pages = self._total_pages()
        page_results = {}
        errors = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(pdfium_extractors.parse_pdfium_page, str(self.filepath), idx, image_output_dir): idx
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
        return pdfium_extractors.combine_pdfium_pages(self.filepath, total_pages, ordered, errors)

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        return pdfium_extractors.parse_pdfium_page(str(self.filepath), 0)

    def to_dataframe(self, drop_layout_tables: bool = False) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element)."""
        records = pdfium_extractors.flatten_pdfium_document(self.to_dicts())
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def to_arrow_table(self, drop_layout_tables: bool = False) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        records = pdfium_extractors.flatten_pdfium_document(self.to_dicts())
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
    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Write the unified document JSON to disk."""
        pass

    @abstractmethod
    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Write one element per line as JSON Lines."""
        pass

    @abstractmethod
    def extract_images(self, output_dir=None, dedupe=True):
        """Write embedded images to disk; return their paths."""
        pass


class PDFWriterPyMuPDFEngine(PDFBaseWriterEngine):
    """Write parsed PDF output (JSON + image files) using PyMuPDF."""

    def _reader(self):
        return PDFReaderPyMuPDFEngine(self.filepath, workers=self.workers)

    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the unified document JSON.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the JSON; otherwise image
                ``file_path`` values are null.
            dedupe_images (bool, default True): When images are written, collapse
                byte-identical duplicates to a single file and repoint references.
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.
        """
        filename = set_export_filename(self.properties.json_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document)
        with open(filename, "w") as f:
            json.dump(document, f, indent=2)
        return filename

    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(self.properties.json_newline_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document)
        records = pdfcomponents.flatten_document_elements(document)
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def extract_images(self, output_dir=None, dedupe=True):
        """Parse the PDF, write embedded images to disk, return their paths.

        Args:
            output_dir (optional, str): Output directory; defaults to output_images.
            dedupe (bool, default True): Collapse byte-identical duplicate images
                to a single file before returning paths.
        """
        directory = output_dir if output_dir else self.properties.images_export_dir
        document = self._reader().to_dicts(image_output_dir=directory)
        if dedupe:
            pdfcomponents.dedupe_document_images(document)
        paths = []
        seen = set()
        for page in document.get("document", {}).get("pages", []):
            for elem in page.get("elements", []):
                if elem.get("type") == "image":
                    fp = (elem.get("metadata") or {}).get("file_path")
                    if fp and fp not in seen:
                        seen.add(fp)
                        paths.append(fp)
        return paths


class PDFWriterPdfiumEngine(PDFBaseWriterEngine):
    """Write parsed PDFium output (native-schema JSON + image files)."""

    def _reader(self):
        return PDFReaderPdfiumEngine(self.filepath, workers=self.workers)

    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the native document JSON.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the JSON; otherwise image
                ``file`` values are null.
            dedupe_images (bool, default True): When images are written, collapse
                byte-identical duplicates to a single file and repoint references.
            drop_layout_tables (bool, default False): No-op for PDFium (no table
                detection); accepted for interface parity.
        """
        filename = set_export_filename(self.properties.json_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir)
        if image_output_dir and dedupe_images:
            pdfium_extractors.dedupe_pdfium_images(document)
        with open(filename, "w") as f:
            json.dump(document, f, indent=2)
        return filename

    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(self.properties.json_newline_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir)
        if image_output_dir and dedupe_images:
            pdfium_extractors.dedupe_pdfium_images(document)
        records = pdfium_extractors.flatten_pdfium_document(document)
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def extract_images(self, output_dir=None, dedupe=True):
        """Parse the PDF, write embedded images to disk, return their paths.

        Args:
            output_dir (optional, str): Output directory; defaults to output_images.
            dedupe (bool, default True): Collapse byte-identical duplicate images
                to a single file before returning paths.
        """
        directory = output_dir if output_dir else self.properties.images_export_dir
        document = self._reader().to_dicts(image_output_dir=directory)
        if dedupe:
            pdfium_extractors.dedupe_pdfium_images(document)
        paths = []
        seen = set()
        for page in document.get("document", {}).get("pages", []):
            for img in page.get("images", []):
                fp = img.get("file")
                if fp and fp not in seen:
                    seen.add(fp)
                    paths.append(fp)
        return paths
