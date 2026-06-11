"""Module to create engines for PDF processing."""

# standard library
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.core.pdf_io.extraction import PdfiumBackend, PdfiumNativeReader
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument

logger = logging.getLogger(__name__)


def _is_distributed_env() -> bool:
    """Return True if running inside a distributed execution environment like Spark or Beam."""
    distributed_keys = {
        "SPARK_ENV_LOADED",
        "SPARK_HOME",
        "BEAM_WORKER_ID",
        "FLINK_CONF_DIR",
        "CELERY_BROKER_URL",
    }
    return any(k in os.environ for k in distributed_keys)


def _parse_page_structured_worker(filepath_str: str, page_index: int, image_output_dir: Optional[str]) -> dict:
    """Process worker function to parse a single page using PDFium in structured mode."""
    from pathlib import Path
    from datagrunt.core.pdf_io import pdfcomponents
    from datagrunt.core.pdf_io.extraction import PdfiumBackend

    filepath = Path(filepath_str)
    assembler = pdfcomponents.DocumentAssembler(filepath, backend=PdfiumBackend(filepath))
    with assembler.backend, assembler.table_extractor:
        return assembler.parse_page(page_index, image_output_dir)


def _parse_page_native_worker(filepath_str: str, page_index: int, image_output_dir: Optional[str]) -> dict:
    """Process worker function to parse a single page using PDFium in native mode."""
    from pathlib import Path
    from datagrunt.core.pdf_io.extraction import PdfiumNativeReader

    filepath = Path(filepath_str)
    reader = PdfiumNativeReader(filepath)
    with reader:
        return reader.parse_page(page_index, image_output_dir)


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
    default_workers: int = 1
    json_export_filename: str = "output.json"
    json_newline_export_filename: str = "output.jsonl"
    markdown_export_filename: str = "output.md"
    images_export_dir: str = "output_images"
    valid_engines: tuple = ("pymupdf", "pdfium")
    value_error_message: str = "Engine '{engine}' is not supported. Valid engines: {valid}."


class PDFBaseReaderEngine(ABC):
    """Abstract base class defining the interface for PDF reader engines."""

    def __init__(self, filepath, workers: int = 1):
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
        with PdfiumDocument(self.filepath) as doc:
            return len(doc)

    def to_dicts(self, image_output_dir: Optional[str] = None, drop_layout_tables: bool = False) -> dict:
        """Parse all pages sequentially into the unified document dict.

        Pages are parsed one at a time on purpose: PyMuPDF/MuPDF shares a global
        context and is not thread-safe, so dispatching pages across threads risks
        garbled output or an interpreter crash. The ``workers`` argument is kept
        for API compatibility but does not enable threading here; callers needing
        parallel parsing should use the process-based pdfium engine.

        The document and table-extractor contexts are held open once for the
        whole parse via ``DocumentAssembler.parse_document`` instead of being
        reopened per page (issue #102).
        """
        if self.workers > 1:
            logger.warning(
                "PyMuPDF parses pages sequentially because MuPDF is not thread-safe; "
                "the 'workers=%d' setting is ignored. Use the pdfium engine for parallel parsing.",
                self.workers,
            )
        assembler = pdfcomponents.DocumentAssembler(self.filepath)
        total_pages = self._total_pages()
        document = assembler.parse_document(total_pages, image_output_dir)
        if drop_layout_tables:
            pdfcomponents.ParsedDocument(document).drop_layout_tables()
        return document

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        return pdfcomponents.DocumentAssembler(self.filepath).parse_page(0)

    def to_dataframe(self, drop_layout_tables: bool = False) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element)."""
        records = pdfcomponents.ParsedDocument(self.to_dicts(drop_layout_tables=drop_layout_tables)).flatten()
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def to_arrow_table(self, drop_layout_tables: bool = False) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        records = pdfcomponents.ParsedDocument(self.to_dicts(drop_layout_tables=drop_layout_tables)).flatten()
        if not records:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(records)


class PDFReaderPdfiumEngine(PDFBaseReaderEngine):
    """Read and parse PDF files using PDFium (pypdfium2).

    Default (``structured=False``) emits the native PDFium schema. With
    ``structured=True`` it emits the same unified element schema as the pymupdf
    engine, via the shared ``pdfcomponents`` pipeline driven by ``PdfiumBackend``
    (text, images, OCR) plus shared pdfplumber tables. Pages are parsed
    concurrently using a process pool (or sequentially when workers=1 or in a
    distributed environment) because pdfium is not thread-safe within the same
    process.
    """

    def __init__(self, filepath, workers: int = 1, structured: bool = False):
        super().__init__(filepath, workers=workers)
        self.structured = structured

    def _total_pages(self) -> int:
        with PdfiumDocument(self.filepath) as doc:
            return len(doc)

    def _to_dicts_structured(self, image_output_dir, drop_layout_tables) -> dict:
        total_pages = self._total_pages()
        pages = []
        errors = []

        if self.workers <= 1 or total_pages <= 1 or _is_distributed_env():
            if self.workers > 1 and _is_distributed_env():
                logger.warning(
                    "Distributed environment detected. Defaulting to sequential PDFium execution to prevent multiprocessing overhead."
                )
            assembler = pdfcomponents.DocumentAssembler(self.filepath, backend=PdfiumBackend(self.filepath))
            with assembler.backend, assembler.table_extractor:
                for idx in range(total_pages):
                    try:
                        pages.append(assembler.parse_page(idx, image_output_dir))
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"Page {idx + 1}: {e}")
        else:
            from concurrent.futures import ProcessPoolExecutor, as_completed
            pages_map = {}
            with ProcessPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(_parse_page_structured_worker, str(self.filepath), idx, image_output_dir): idx
                    for idx in range(total_pages)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        page = future.result()
                        pages_map[idx] = page
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"Page {idx + 1}: {e}")
            for idx in range(total_pages):
                if idx in pages_map:
                    pages.append(pages_map[idx])

        assembler = pdfcomponents.DocumentAssembler(self.filepath, backend=PdfiumBackend(self.filepath))
        document = assembler.combine(total_pages, pages, errors)
        if drop_layout_tables:
            pdfcomponents.ParsedDocument(document).drop_layout_tables()
        return document

    def _to_dicts_native(self, image_output_dir) -> dict:
        total_pages = self._total_pages()
        page_results = {}
        errors = []

        if self.workers <= 1 or total_pages <= 1 or _is_distributed_env():
            if self.workers > 1 and _is_distributed_env():
                logger.warning(
                    "Distributed environment detected. Defaulting to sequential PDFium execution to prevent multiprocessing overhead."
                )
            reader = PdfiumNativeReader(self.filepath)
            with reader:
                for idx in range(total_pages):
                    try:
                        page = reader.parse_page(idx, image_output_dir)
                        page_results[page["page_number"]] = page
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"Page {idx + 1}: {e}")
        else:
            from concurrent.futures import ProcessPoolExecutor, as_completed
            with ProcessPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(_parse_page_native_worker, str(self.filepath), idx, image_output_dir): idx
                    for idx in range(total_pages)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        page = future.result()
                        page_results[page["page_number"]] = page
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"Page {idx + 1}: {e}")

        ordered = [page_results[p] for p in sorted(page_results.keys())]
        reader = PdfiumNativeReader(self.filepath)
        return reader.combine(total_pages, ordered, errors)

    def to_dicts(self, image_output_dir: Optional[str] = None, drop_layout_tables: bool = False) -> dict:
        """Parse all pages concurrently using a process pool (or sequentially)."""
        if self.structured:
            return self._to_dicts_structured(image_output_dir, drop_layout_tables)
        return self._to_dicts_native(image_output_dir)

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        if self.structured:
            return pdfcomponents.DocumentAssembler(
                self.filepath, backend=PdfiumBackend(self.filepath)
            ).parse_page(0)
        return PdfiumNativeReader(self.filepath).parse_page(0)

    def to_dataframe(self, drop_layout_tables: bool = False) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element)."""
        if self.structured:
            records = pdfcomponents.ParsedDocument(self.to_dicts(drop_layout_tables=drop_layout_tables)).flatten()
        else:
            records = PdfiumNativeReader.flatten(self.to_dicts())
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def to_arrow_table(self, drop_layout_tables: bool = False) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        if self.structured:
            records = pdfcomponents.ParsedDocument(self.to_dicts(drop_layout_tables=drop_layout_tables)).flatten()
        else:
            records = PdfiumNativeReader.flatten(self.to_dicts())
        if not records:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(records)


class PDFBaseWriterEngine(ABC):
    """Abstract base class defining the interface for PDF writer engines."""

    def __init__(self, filepath, workers: int = 1):
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
    def write_markdown(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Write the document Markdown representation to disk."""
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
            pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
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
            pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
        records = pdfcomponents.ParsedDocument(document).flatten()
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def write_markdown(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the document Markdown.

        Args:
            export_filename (optional, str): Output path; defaults to output.md.
            image_output_dir (optional, str): If provided, embedded images are
                written there and referenced in the Markdown.
            dedupe_images (bool, default True): When images are written, collapse
                byte-identical duplicates to a single file and repoint references.
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.
        """
        filename = set_export_filename(self.properties.markdown_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
        markdown_text = pdfcomponents.ParsedDocument(document).to_markdown(export_filename=filename)
        with open(filename, "w") as f:
            f.write(markdown_text)
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
            pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=directory)
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
    """Write parsed PDFium output. Native schema by default; unified when structured."""

    def __init__(self, filepath, workers: int = 1, structured: bool = False):
        super().__init__(filepath, workers=workers)
        self.structured = structured

    def _reader(self):
        return PDFReaderPdfiumEngine(self.filepath, workers=self.workers, structured=self.structured)

    def _dedupe(self, document, image_output_dir):
        if self.structured:
            pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
        else:
            PdfiumNativeReader.dedupe_images(document, image_output_dir=image_output_dir)

    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the document JSON (native or unified schema)."""
        filename = set_export_filename(self.properties.json_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)
        if image_output_dir and dedupe_images:
            self._dedupe(document, image_output_dir)
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
            self._dedupe(document, image_output_dir)
        if self.structured:
            records = pdfcomponents.ParsedDocument(document).flatten()
        else:
            records = PdfiumNativeReader.flatten(document)
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def write_markdown(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the document Markdown (native or structured schema)."""
        filename = set_export_filename(self.properties.markdown_export_filename, export_filename)
        document = self._reader().to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)
        if image_output_dir and dedupe_images:
            self._dedupe(document, image_output_dir)
        if self.structured:
            markdown_text = pdfcomponents.ParsedDocument(document).to_markdown(export_filename=filename)
        else:
            markdown_text = PdfiumNativeReader.to_markdown(document)
        with open(filename, "w") as f:
            f.write(markdown_text)
        return filename

    def extract_images(self, output_dir=None, dedupe=True):
        """Parse the PDF, write embedded images to disk, return their paths."""
        directory = output_dir if output_dir else self.properties.images_export_dir
        document = self._reader().to_dicts(image_output_dir=directory)
        if dedupe:
            self._dedupe(document, directory)
        paths = []
        seen = set()
        for page in document.get("document", {}).get("pages", []):
            if self.structured:
                candidates = [
                    (el.get("metadata") or {}).get("file_path")
                    for el in page.get("elements", [])
                    if el.get("type") == "image"
                ]
            else:
                candidates = [img.get("file") for img in page.get("images", [])]
            for fp in candidates:
                if fp and fp not in seen:
                    seen.add(fp)
                    paths.append(fp)
        return paths
