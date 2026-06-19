"""Module to create engines for PDF processing."""

# standard library
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.core.pdf_io.extraction import PdfiumBackend, PdfiumNativeReader, PyMuPDFBackend
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
    """Return the export filename if explicitly provided, otherwise the default.

    Mirrors ``DuckDBQueries.set_export_filename``: ``None`` means "use the
    default"; a provided-but-empty/whitespace-only string raises ``ValueError``
    rather than silently writing to the default name (almost always a caller bug).
    """
    if export_filename is not None:
        if not export_filename.strip():
            raise ValueError(
                f"export_filename must not be empty or whitespace-only; "
                f"got {export_filename!r}. Pass None to use the default."
            )
        return export_filename
    return default_filename


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
        self._parsed_cache = {}

    def _parse_to_dicts(self, drop_layout_tables: bool = False) -> dict:
        """Parse once per drop_layout_tables value, shared by the conversions.

        ``to_dataframe`` and ``to_arrow_table`` both need the parsed document
        and only read it (via ``flatten``), so they reuse a single parse on the
        same instance. The public ``to_dicts`` stays uncached so its return
        value is never an unexpectedly shared/mutable object.
        """
        if drop_layout_tables not in self._parsed_cache:
            self._parsed_cache[drop_layout_tables] = self.to_dicts(drop_layout_tables=drop_layout_tables)
        return self._parsed_cache[drop_layout_tables]

    @abstractmethod
    def get_sample(self) -> dict:
        """Return the parsed first page."""
        pass

    @abstractmethod
    def to_dicts(self, image_output_dir: Optional[str] = None, drop_layout_tables: bool = False) -> dict:
        """Return the unified parsed document dict."""
        pass

    def to_dataframe(self, drop_layout_tables: bool = False) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element).

        Shared by every engine: the parse is memoized and ``flatten_document``
        dispatches on the document's schema, so the unified and native paths
        need no per-engine override.
        """
        records = pdfcomponents.flatten_document(self._parse_to_dicts(drop_layout_tables=drop_layout_tables))
        return pl.DataFrame(records) if records else pl.DataFrame()

    def to_arrow_table(self, drop_layout_tables: bool = False) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        records = pdfcomponents.flatten_document(self._parse_to_dicts(drop_layout_tables=drop_layout_tables))
        return pa.Table.from_pylist(records) if records else pa.Table.from_pydict({})


class PDFReaderPyMuPDFEngine(PDFBaseReaderEngine):
    """Read and parse PDF files using PyMuPDF / pdfplumber / Tesseract."""

    def _total_pages(self) -> int:
        # Count pages with the engine's own backend so the pymupdf path has no
        # pdfium dependency; pdfium cannot load zero-page PDFs that pymupdf
        # handles fine (see issue #95).
        return PyMuPDFBackend(self.filepath).page_count()

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
        # Count pages inside the held-open backend context so the count reuses
        # the same pymupdf document handle as the parse — one open per parse
        # (issue #102) with no pdfium dependency for the count (issue #95).
        with assembler.backend:
            total_pages = assembler.backend.page_count()
            document = assembler.parse_document(total_pages, image_output_dir)
        if drop_layout_tables:
            pdfcomponents.ParsedDocument(document).drop_layout_tables()
        return document

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        return pdfcomponents.DocumentAssembler(self.filepath).parse_page(0)


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
                    "Distributed environment detected. Defaulting to sequential "
                    "PDFium execution to prevent multiprocessing overhead."
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
                    "Distributed environment detected. Defaulting to sequential "
                    "PDFium execution to prevent multiprocessing overhead."
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
            return pdfcomponents.DocumentAssembler(self.filepath, backend=PdfiumBackend(self.filepath)).parse_page(0)
        return PdfiumNativeReader(self.filepath).parse_page(0)


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
        self._reader_engine = None
        self._cached_document = None
        self._cached_parse_key = None

    def _parse_document(self, image_output_dir, drop_layout_tables):
        """Parse once per (image_output_dir, drop_layout_tables) and reuse the dict."""
        key = (image_output_dir, drop_layout_tables)
        if self._cached_parse_key != key:
            self._cached_document = self._reader().to_dicts(
                image_output_dir=image_output_dir,
                drop_layout_tables=drop_layout_tables,
            )
            self._cached_parse_key = key
        return self._cached_document

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
    def write_markdown(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Write the document Markdown representation to disk."""
        pass

    def extract_images(self, output_dir=None, dedupe=True):
        """Parse the PDF, write embedded images to disk, return their paths.

        Shared by every engine: the parse is memoized and both the dedupe and
        the path collection dispatch on the document's schema, so no per-engine
        override is needed.

        Args:
            output_dir (optional, str): Output directory; defaults to output_images.
            dedupe (bool, default True): Collapse byte-identical duplicate images
                to a single file before returning paths.
        """
        directory = output_dir if output_dir else self.properties.images_export_dir
        document = self._parse_document(directory, False)
        if dedupe:
            pdfcomponents.dedupe_document_images(document, directory)
        return pdfcomponents.collect_image_paths(document)


class PDFWriterPyMuPDFEngine(PDFBaseWriterEngine):
    """Write parsed PDF output (JSON + image files) using PyMuPDF."""

    def _reader(self):
        if self._reader_engine is None:
            self._reader_engine = PDFReaderPyMuPDFEngine(self.filepath, workers=self.workers)
        return self._reader_engine

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
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(document, f, indent=2)
        return filename

    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(self.properties.json_newline_export_filename, export_filename)
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        records = pdfcomponents.flatten_document(document)
        with open(filename, "w", encoding="utf-8") as f:
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
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        markdown_text = pdfcomponents.ParsedDocument(document).to_markdown(export_filename=filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(markdown_text)
        return filename


class PDFWriterPdfiumEngine(PDFBaseWriterEngine):
    """Write parsed PDFium output. Native schema by default; unified when structured."""

    def __init__(self, filepath, workers: int = 1, structured: bool = False):
        super().__init__(filepath, workers=workers)
        self.structured = structured

    def _reader(self):
        if self._reader_engine is None:
            self._reader_engine = PDFReaderPdfiumEngine(self.filepath, workers=self.workers, structured=self.structured)
        return self._reader_engine

    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the document JSON (native or unified schema)."""
        filename = set_export_filename(self.properties.json_export_filename, export_filename)
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(document, f, indent=2)
        return filename

    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(self.properties.json_newline_export_filename, export_filename)
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        records = pdfcomponents.flatten_document(document)
        with open(filename, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def write_markdown(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the document Markdown (native or structured schema)."""
        filename = set_export_filename(self.properties.markdown_export_filename, export_filename)
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        if self.structured:
            markdown_text = pdfcomponents.ParsedDocument(document).to_markdown(export_filename=filename)
        else:
            markdown_text = PdfiumNativeReader.to_markdown(document)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(markdown_text)
        return filename
