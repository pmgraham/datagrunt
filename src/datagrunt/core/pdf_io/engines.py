"""Module to create engines for PDF processing."""

# standard library
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
from datagrunt.core.pdf_io.extraction.config import _PDFExtractionConfig
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


def _parse_page_structured_worker(
    filepath_str: str, page_index: int, image_output_dir: Optional[str], extraction_config=None
) -> dict:
    """Process worker function to parse a single page using PDFium in structured mode."""
    from pathlib import Path

    from datagrunt.core.pdf_io import pdfcomponents
    from datagrunt.core.pdf_io.extraction import PdfiumBackend

    filepath = Path(filepath_str)
    assembler = pdfcomponents.DocumentAssembler(
        filepath,
        backend=PdfiumBackend(filepath, extraction_config=extraction_config),
        extraction_config=extraction_config,
    )
    with assembler.backend, assembler.table_extractor:
        return assembler.parse_page(page_index, image_output_dir)


def _parse_page_native_worker(
    filepath_str: str, page_index: int, image_output_dir: Optional[str], extraction_config=None
) -> dict:
    """Process worker function to parse a single page using PDFium in native mode."""
    from pathlib import Path

    from datagrunt.core.pdf_io.extraction import PdfiumNativeReader

    filepath = Path(filepath_str)
    reader = PdfiumNativeReader(filepath, extraction_config=extraction_config)
    with reader:
        return reader.parse_page(page_index, image_output_dir)


# Image formats both writer engines (pdfium via PIL, pymupdf via Pixmap) can
# emit. Restricting to this shared set keeps the two engines byte-compatible in
# behavior and lets an unsupported request fail fast (see _normalize_image_format).
_SUPPORTED_IMAGE_FORMATS = ("png", "jpg", "jpeg")
_PIL_FORMAT_BY_EXT = {"png": "PNG", "jpg": "JPEG", "jpeg": "JPEG"}


def _normalize_image_format(image_format: str) -> tuple:
    """Return ``(file_extension, pil_format)`` for a requested image format.

    PIL registers JPEG under ``"JPEG"`` (not ``"JPG"``), so ``"jpg"`` maps to the
    ``"JPEG"`` save handler while keeping the caller's ``.jpg`` file extension on
    disk. Only formats both engines can write are accepted; an unsupported
    format is a caller bug and raises ``ValueError`` up front rather than
    silently yielding an empty result (mirrors ``set_export_filename``).
    """
    ext = image_format.lower().lstrip(".")
    if ext not in _SUPPORTED_IMAGE_FORMATS:
        raise ValueError(
            f"Unsupported image_format {image_format!r}. Supported formats: {', '.join(_SUPPORTED_IMAGE_FORMATS)}."
        )
    return ext, _PIL_FORMAT_BY_EXT[ext]


def _render_pdfium_page_to_file(
    page, directory: Path, pdf_name: str, page_index: int, dpi: int, ext: str, pil_format: str
) -> str:
    """Render one already-opened PDFium page to ``directory``; return its path.

    Shared by the in-process sequential loop and the process-pool worker so the
    render-and-save step lives in exactly one place (DRY within the PDF domain).
    """
    img = page.render_pil(dpi=dpi)
    # Zero-padded so directory listings sort in page order (page_02 < page_10).
    out_path = directory / f"{pdf_name}_page_{page_index + 1:02d}.{ext}"
    img.save(out_path, format=pil_format)
    return str(out_path)


def _render_page_worker(filepath_str: str, page_index: int, output_dir_str: str, dpi: int, image_format: str) -> str:
    """Process worker function to render a single page to an image using PDFium."""
    from pathlib import Path

    from datagrunt.core.pdf_io.engines import _normalize_image_format, _render_pdfium_page_to_file
    from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument

    filepath = Path(filepath_str)
    directory = Path(output_dir_str)
    pdf_name = filepath.stem
    ext, pil_format = _normalize_image_format(image_format)
    with PdfiumDocument(filepath) as doc:
        with doc.page(page_index) as page:
            return _render_pdfium_page_to_file(page, directory, pdf_name, page_index, dpi, ext, pil_format)


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

    def __init__(self, filepath, workers: int = 1, extraction_config=None):
        """Initialize the PDF reader engine.

        Args:
            filepath (str or Path): Path to the PDF file.
            workers (int): Number of concurrent per-page workers.
            extraction_config: Optional extraction config; defaults to _PDFExtractionConfig().
        """
        self.filepath = Path(filepath)
        self.workers = workers
        self.extraction_config = extraction_config or _PDFExtractionConfig()
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
        assembler = pdfcomponents.DocumentAssembler(self.filepath, extraction_config=self.extraction_config)
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
        return pdfcomponents.DocumentAssembler(self.filepath, extraction_config=self.extraction_config).parse_page(0)


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

    def __init__(self, filepath, workers: int = 1, structured: bool = False, extraction_config=None):
        super().__init__(filepath, workers=workers, extraction_config=extraction_config)
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
            assembler = pdfcomponents.DocumentAssembler(
                self.filepath,
                backend=PdfiumBackend(self.filepath, extraction_config=self.extraction_config),
                extraction_config=self.extraction_config,
            )
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
                    executor.submit(
                        _parse_page_structured_worker,
                        str(self.filepath),
                        idx,
                        image_output_dir,
                        self.extraction_config,
                    ): idx
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

        assembler = pdfcomponents.DocumentAssembler(
            self.filepath,
            backend=PdfiumBackend(self.filepath, extraction_config=self.extraction_config),
            extraction_config=self.extraction_config,
        )
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
            reader = PdfiumNativeReader(self.filepath, extraction_config=self.extraction_config)
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
                    executor.submit(
                        _parse_page_native_worker,
                        str(self.filepath),
                        idx,
                        image_output_dir,
                        self.extraction_config,
                    ): idx
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
        reader = PdfiumNativeReader(self.filepath, extraction_config=self.extraction_config)
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
                self.filepath,
                backend=PdfiumBackend(self.filepath, extraction_config=self.extraction_config),
                extraction_config=self.extraction_config,
            ).parse_page(0)
        return PdfiumNativeReader(self.filepath, extraction_config=self.extraction_config).parse_page(0)


class PDFBaseWriterEngine(ABC):
    """Abstract base class defining the interface for PDF writer engines."""

    def __init__(self, filepath, workers: int = 1, extraction_config=None):
        """Initialize the PDF writer engine.

        Args:
            filepath (str or Path): Path to the PDF file.
            workers (int): Number of concurrent per-page workers.
            extraction_config: Optional extraction config; defaults to _PDFExtractionConfig().
        """
        self.filepath = Path(filepath)
        self.workers = workers
        self.extraction_config = extraction_config or _PDFExtractionConfig()
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

    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the unified document JSON to disk.

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
        return pdfcomponents.write_document_json(document, filename)

    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(self.properties.json_newline_export_filename, export_filename)
        document = self._parse_document(image_output_dir, drop_layout_tables)
        if image_output_dir and dedupe_images:
            pdfcomponents.dedupe_document_images(document, image_output_dir)
        return pdfcomponents.write_document_jsonl(document, filename)

    def write_markdown(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the document Markdown to disk.

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
        return pdfcomponents.write_document_markdown(document, filename)

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

    @abstractmethod
    def render_pages_as_images(self, output_dir=None, dpi=300, image_format="png") -> list:
        """Render each page of the PDF as an image and save to disk."""
        pass


class PDFWriterPyMuPDFEngine(PDFBaseWriterEngine):
    """Write parsed PDF output (JSON + image files) using PyMuPDF."""

    def _reader(self):
        if self._reader_engine is None:
            self._reader_engine = PDFReaderPyMuPDFEngine(
                self.filepath, workers=self.workers, extraction_config=self.extraction_config
            )
        return self._reader_engine

    def render_pages_as_images(self, output_dir=None, dpi=300, image_format="png") -> list:
        from datagrunt.core.pdf_io.extraction.pymupdf_backend import _import_pymupdf

        if self.workers > 1:
            logger.warning(
                "PyMuPDF renders pages sequentially; the 'workers=%d' setting is ignored. "
                "Use the pdfium engine for parallel rendering.",
                self.workers,
            )

        # Validate the format before any filesystem side effects (fail fast).
        ext, _ = _normalize_image_format(image_format)
        pymupdf = _import_pymupdf()
        directory = Path(output_dir if output_dir else "page_images")
        directory.mkdir(parents=True, exist_ok=True)

        written_paths = []
        doc = pymupdf.open(self.filepath)
        pdf_name = self.filepath.stem
        try:
            for idx in range(doc.page_count):
                # Per-page error isolation: a page that fails to render is logged
                # and skipped, never aborting the whole batch (project invariant).
                try:
                    page = doc[idx]
                    zoom = dpi / 72.0
                    mat = pymupdf.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat)
                    img_data = pix.tobytes(ext)
                    # Zero-padded to match the pdfium engine's filenames.
                    out_path = directory / f"{pdf_name}_page_{idx + 1:02d}.{ext}"
                    with open(out_path, "wb") as f:
                        f.write(img_data)
                    written_paths.append(str(out_path))
                except Exception as e:  # noqa: BLE001
                    logger.warning("Page %d could not be rendered and was skipped: %s", idx + 1, e)
        finally:
            doc.close()
        return written_paths


class PDFWriterPdfiumEngine(PDFBaseWriterEngine):
    """Write parsed PDFium output. Native schema by default; unified when structured."""

    def __init__(self, filepath, workers: int = 1, structured: bool = False, extraction_config=None):
        super().__init__(filepath, workers=workers, extraction_config=extraction_config)
        self.structured = structured

    def _reader(self):
        if self._reader_engine is None:
            self._reader_engine = PDFReaderPdfiumEngine(
                self.filepath,
                workers=self.workers,
                structured=self.structured,
                extraction_config=self.extraction_config,
            )
        return self._reader_engine

    def render_pages_as_images(self, output_dir=None, dpi=300, image_format="png") -> list:
        # Validate the format before any filesystem side effects (fail fast).
        ext, pil_format = _normalize_image_format(image_format)
        directory = Path(output_dir if output_dir else "page_images")
        directory.mkdir(parents=True, exist_ok=True)

        with PdfiumDocument(self.filepath) as doc:
            total_pages = len(doc)

        if self.workers <= 1 or total_pages <= 1 or _is_distributed_env():
            if self.workers > 1 and _is_distributed_env():
                logger.warning(
                    "Distributed environment detected. Defaulting to sequential "
                    "PDFium execution to prevent multiprocessing overhead."
                )
            written_paths = []
            pdf_name = self.filepath.stem
            with PdfiumDocument(self.filepath) as doc:
                for idx in range(total_pages):
                    # Per-page error isolation: a page that fails to render is
                    # logged and skipped, never aborting the whole batch.
                    try:
                        with doc.page(idx) as page:
                            written_paths.append(
                                _render_pdfium_page_to_file(page, directory, pdf_name, idx, dpi, ext, pil_format)
                            )
                    except Exception as e:  # noqa: BLE001
                        logger.warning("Page %d could not be rendered and was skipped: %s", idx + 1, e)
            return written_paths
        else:
            from concurrent.futures import ProcessPoolExecutor, as_completed

            paths_map = {}
            with ProcessPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(
                        _render_page_worker,
                        str(self.filepath),
                        idx,
                        str(directory),
                        dpi,
                        image_format,
                    ): idx
                    for idx in range(total_pages)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    # Per-page error isolation applies to the parallel path too:
                    # a worker that raises is logged and its page skipped, while
                    # the successfully rendered pages are still returned in order.
                    try:
                        paths_map[idx] = future.result()
                    except Exception as e:  # noqa: BLE001
                        logger.warning("Page %d could not be rendered and was skipped: %s", idx + 1, e)

            return [paths_map[idx] for idx in sorted(paths_map.keys())]
