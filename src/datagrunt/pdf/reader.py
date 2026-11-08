"""The v5 PDFReader (#372, A8b): a PDF's document, read in memory, with zero filesystem writes.

Public since 5.0 (#373): ``datagrunt.PDFReader`` and ``datagrunt.pdf.PDFReader``,
both imported on first access, so ``import datagrunt`` needs no [pdf] extra
(:mod:`datagrunt.pdf._extra`).

:class:`PDFReader` takes one of three sources:

- a PDF's path (a str or os.PathLike of str): A8a's
  :func:`~datagrunt.pdf.pipeline.extract_document`, with the reader's
  options and password;
- a bundle directory, or its ``manifest.json``: A7a's
  :func:`~datagrunt.pdf.bundle.load_bundle`, which verifies every artifact;
- a Mapping: A7a's codec's document JSON
  (:func:`~datagrunt.pdf.codec.document_from_json`), decoded without a
  base directory: it names no file, so a path in it is refused, and its
  figures and page images have no bytes to read.

Construction checks (spec invariants 1-2) run before any work, in order,
as ``PDFWriter`` (A7b) runs its own:

1. The options, as :class:`~datagrunt.pdf.pipeline.PipelineOptions`
   checks them (``ValueError`` out of bounds, ``TypeError`` of the wrong
   type), whatever the source.
2. The source's type, then the password (a str without NUL, or None), as
   :class:`~datagrunt.pdf.pipeline.PDFSource` checks it. The options and
   the password apply to a PDF; a bundle or a Mapping does not use them.
3. The source itself. A directory is a bundle: it must hold a
   ``manifest.json``, read and checked as ``load_bundle`` checks it (its
   artifacts are read on first use). A file named ``manifest.json`` is its
   directory's bundle. Any other path is a PDF, checked by the check
   ``PDFWriter`` shares (:func:`~datagrunt.pdf._source.checked_pdf_file`):
   a path that does not exist raises ``FileNotFoundError``; one that is not
   a regular file, or has no PDF header where pdfium looks for one,
   ``ValueError`` (it reads at most the file's first 1028 bytes, and never
   blocks on a named pipe). A bundle that is not valid is a ``ValueError``
   chained from its ``BundleCorruptError``. A Mapping is decoded at
   construction: decoding is its check, and the reader keeps the document,
   so the Mapping changed later does not change the reader.

A path is made absolute at construction: a later ``chdir`` cannot
redirect it.

When the work happens: a PDF is extracted, and a bundle loaded, on first
use (the first property or method that needs the document), then cached
for the reader's life. Text and layout are read then, eagerly; a figure's
and a page image's bytes stay lazy, read only through their A1 loaders
(``Figure.read_bytes``, ``PageImage.read_bytes``: a bundle's verifying
reader, a capped figure worker, or A5a's guarded, capped render). A page
image of a PDF is drawn from the file at the source path when it is read:
that read raises ``PDFSourceChangedError`` when the file there no longer
holds the document's bytes (its ``metadata.sha256``), or changes while it
is drawn; a file only touched, or rewritten with the same bytes, is drawn
(:mod:`datagrunt.pdf._loaders`, #453). A failed extraction or load is
not cached: the next use tries again. A lock makes the first use
thread-safe: two threads never extract the same reader's document twice.

The document's collections (``pages``, ``headings``, ``paragraphs``,
``key_values``, ``tables``, ``lists``, ``figures``, ``page_images``,
``sections``, ``header_context``) and its ``metadata`` are the document's
own (A1's immutable elements). Its views:

- :meth:`PDFReader.to_dataframe`: A7a's element frame
  (:func:`~datagrunt.pdf.frame.element_frame`, ``hidden_reasons``
  included: hidden text is flagged, never dropped, so filter it out by that
  column), one row per element in document order. It is built once, on
  first use, and each call returns its own clone (cheap: the buffers are
  shared, and a change to one does not reach the reader's).
- :meth:`PDFReader.to_arrow_table`: that frame as Arrow, of A7a's fixed
  :data:`~datagrunt.pdf.frame.ARROW_SCHEMA` (immutable, so shared).
- :meth:`PDFReader.to_dicts`: the frame's rows as new dicts each call.
- :meth:`PDFReader.get_sample`: its first ``n_rows`` rows (20 by
  default); ``n_rows`` is checked before any work.
- :meth:`PDFReader.to_markdown`: the document's Markdown (A1). Hidden
  elements are omitted by default, as a reader sees the page; with
  ``include_hidden=True`` each is rendered behind its
  ``<!-- hidden: ... -->`` marker. The frame and the document keep them
  either way. The text is the document's exactly: a lone surrogate is kept
  (the frame's strings hold its escape, as UTF-8 needs).

DuckDB:

- :attr:`PDFReader.db_table` names the element frame in SQL: ``tbl_`` and
  the document's ``doc_id`` with each character outside ``[A-Za-z0-9_]``
  (a ``-``) as ``_``. It is stable (the same bytes under the same name
  always get it, whatever the source) and a plain identifier: it needs no
  quoting, and DuckDB matches it in any case.
- :meth:`PDFReader.query_data` runs SQL on the reader's own in-memory
  connection (:func:`~datagrunt.pdf._database.private_connection`:
  spilling to disk and extension installs off), where the frame's Arrow
  table is registered as ``db_table``, and returns its rows read whole
  (:class:`~datagrunt.pdf._database.QueryResult`: ``.pl()``, ``.pa()``,
  ``.dicts()``). The connection persists between queries (a table a query
  creates is there for the next), and no other reader shares it.
- Extensions: a query that needs one DuckDB does not link in (``read_xlsx``,
  say) raises ``duckdb.Error`` rather than install it: the caller installs
  it, by an explicit ``INSTALL`` in their own SQL or in their environment.
- Security: ``sql`` runs as it is written. Building it from untrusted
  input is an injection vector against that connection (which can read and
  write files, as any DuckDB connection can): validating, parameterizing
  and sandboxing such SQL is the application's concern, as for the CSV
  readers (preserve, not transform: datagrunt never rewrites SQL).

Lifecycle: nothing is required of the caller. The connection opens on the
first query and is released automatically: a ``weakref.finalize`` closes
it when the reader is collected (or at exit), and it holds no reference to
the reader. :meth:`PDFReader.close` and ``with`` release it early, for a
caller who wants that; the reader stays usable, and a later query opens a
new connection. pdfium handles and worker processes are already per call:
the reader holds none between calls.

Zero writes: no method writes to the filesystem. Extraction's workers,
renders and tesseract calls drain their stderr in memory
(:class:`~datagrunt.pdf._process.StderrTail`), PNGs are encoded in memory,
tesseract reads stdin and writes stdout, a bundle is read through
descriptors opened read-only, and DuckDB neither spills nor installs an
extension. What a caller's SQL writes explicitly (``COPY ... TO``,
``INSTALL``) is the caller's.

Workers: ``workers`` goes to ``extract_document``, whose pool starts plain
``python -I`` processes: a caller's script needs no ``__main__`` guard.
pdfium reads the PDF only in those workers, each capped in memory: this
process never imports pypdfium2, and where no worker can start (a frozen
application) reading a PDF fails, saying why.
"""

import os
import re
import threading
import weakref
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa

from datagrunt.pdf._database import QueryResult, private_connection, query_result
from datagrunt.pdf._options import MIN_IMAGE_DIMENSION, WORKERS, OcrMode
from datagrunt.pdf._source import checked_pdf_file
from datagrunt.pdf._validate import checked_password as _checked_password
from datagrunt.pdf._validate import checked_path as _checked_path
from datagrunt.pdf._validate import int_at_least, non_blank_str, strict_bool
from datagrunt.pdf.bundle import MANIFEST, _read_manifest, load_bundle
from datagrunt.pdf.codec import document_from_json
from datagrunt.pdf.elements import (
    BulletList,
    DocumentMetadata,
    Figure,
    Heading,
    KeyValue,
    Page,
    PageImage,
    Paragraph,
    PDFDocument,
    Section,
    Table,
)
from datagrunt.pdf.exceptions import BundleCorruptError
from datagrunt.pdf.frame import ARROW_SCHEMA, element_frame
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_document

__all__ = ["PDFReader"]

_SAMPLE_ROWS = 20  # get_sample's default, the CSV readers' sample size
_TABLE_PREFIX = "tbl_"  # a table name starts with a letter, as the CSV readers' do
_NOT_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]")


@dataclass(frozen=True, slots=True)
class _Bundle:
    """A bundle source: its directory, made absolute, whose manifest was checked at construction."""

    root: Path


_Source = PDFSource | _Bundle | PDFDocument


class _Connection:
    """The reader's DuckDB connection: opened on its first query, closed by ``close`` (the reader's finalizer's).

    It holds no reference to its reader, so the reader's finalizer can close it.
    """

    __slots__ = ("_connection",)

    def __init__(self) -> None:
        self._connection: duckdb.DuckDBPyConnection | None = None

    def opened(self, table_name: str, table: pa.Table) -> duckdb.DuckDBPyConnection:
        """The connection, opened now with ``table`` registered as ``table_name`` if it is not open."""
        if self._connection is None:
            connection = private_connection()
            try:
                connection.register(table_name, table)
            except BaseException:
                connection.close()
                raise
            self._connection = connection
        return self._connection

    def close(self) -> None:
        """Close the connection if it is open (idempotent)."""
        connection, self._connection = self._connection, None
        if connection is not None:
            connection.close()


class PDFReader:
    """Reads a PDF, a bundle or a document's JSON into a PDFDocument and its views (see the module notes).

    Args:
        source: A PDF's path, a bundle directory or its ``manifest.json``
            (a str or os.PathLike of str), or a Mapping of the codec's
            document JSON.
        password: The encrypted PDF's password, or None. Never in a repr or
            an error message.
        workers, ocr, dpi, vision_dpi, max_render_pixels,
        min_image_dimension, strict: The extraction options, as
            :class:`~datagrunt.pdf.pipeline.PipelineOptions` holds them.

    Raises:
        TypeError: If ``source`` is not a path or a Mapping, or an option or
            ``password`` is of the wrong type.
        ValueError: If an option is out of range (spec invariant 2),
            ``password`` holds NUL, the path is not a PDF or a valid bundle,
            or the Mapping is not a document's JSON (one naming a file path
            included).
        FileNotFoundError: If the path does not exist.
    """

    def __init__(
        self,
        source: str | os.PathLike[str] | Mapping[str, Any],
        *,
        password: str | None = None,
        workers: int = WORKERS,
        ocr: OcrMode = "never",
        dpi: int = 200,
        vision_dpi: int = 300,
        max_render_pixels: int = 16_000_000,
        min_image_dimension: int = MIN_IMAGE_DIMENSION,
        strict: bool = False,
    ) -> None:
        self._options = PipelineOptions(
            workers=workers,
            ocr=ocr,
            dpi=dpi,
            vision_dpi=vision_dpi,
            max_render_pixels=max_render_pixels,
            min_image_dimension=min_image_dimension,
            strict=strict,
        )
        self._source = _checked_source(source, password)
        self._document: PDFDocument | None = None
        self._frame: pl.DataFrame | None = None
        self._arrow: pa.Table | None = None
        self._lock = threading.RLock()
        self._connection = _Connection()
        weakref.finalize(self, self._connection.close)

    @property
    def document(self) -> PDFDocument:
        """The document: extracted or loaded on first use, then cached (see the module notes).

        Every property and method that needs the document raises as this does, on first use; a failure is
        not cached, so the next use tries again.

        Raises:
            BundleCorruptError: If the source is a bundle and an artifact is not what its manifest says
                (:func:`~datagrunt.pdf.bundle.load_bundle`).
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, when the source is a PDF.
        """
        with self._lock:
            if self._document is None:
                self._document = _read(self._source, self._options)
            return self._document

    @property
    def metadata(self) -> DocumentMetadata:
        """The document's metadata."""
        return self.document.metadata

    @property
    def pages(self) -> tuple[Page, ...]:
        """The document's pages, in order."""
        return self.document.pages

    @property
    def headings(self) -> tuple[Heading, ...]:
        """The document's ``Heading`` elements, in document order."""
        return self.document.headings

    @property
    def paragraphs(self) -> tuple[Paragraph, ...]:
        """The document's ``Paragraph`` elements, in document order."""
        return self.document.paragraphs

    @property
    def key_values(self) -> tuple[KeyValue, ...]:
        """The document's ``KeyValue`` elements, in document order."""
        return self.document.key_values

    @property
    def tables(self) -> tuple[Table, ...]:
        """The document's ``Table`` elements, in document order."""
        return self.document.tables

    @property
    def lists(self) -> tuple[BulletList, ...]:
        """The document's ``BulletList`` elements, in document order."""
        return self.document.lists

    @property
    def figures(self) -> tuple[Figure, ...]:
        """The document's ``Figure`` elements, in document order: their bytes read lazily, by their loaders."""
        return self.document.figures

    @property
    def page_images(self) -> tuple[PageImage, ...]:
        """The document's page images, in page order: their bytes read lazily, by their loaders."""
        return self.document.page_images

    @property
    def sections(self) -> tuple[Section, ...]:
        """The document's sections, in document order."""
        return self.document.sections

    @property
    def header_context(self) -> tuple[KeyValue, ...]:
        """The visible ``KeyValue`` elements that head page 1, in reading order."""
        return self.document.header_context

    def to_dataframe(self) -> pl.DataFrame:
        """The element frame, one row per element (see the module notes): the caller's own clone."""
        return self._element_frame().clone()

    def to_arrow_table(self) -> pa.Table:
        """The element frame as a ``pyarrow.Table`` of A7a's fixed ``ARROW_SCHEMA``."""
        with self._lock:
            if self._arrow is None:
                self._arrow = self._element_frame().to_arrow().cast(ARROW_SCHEMA)
            return self._arrow

    def to_dicts(self) -> list[dict[str, Any]]:
        """The element frame's rows, as new dicts."""
        return self._element_frame().to_dicts()

    def get_sample(self, n_rows: int = _SAMPLE_ROWS) -> pl.DataFrame:
        """The element frame's first ``n_rows`` rows (all of them if it has fewer).

        Raises:
            TypeError: If ``n_rows`` is not an int (bool is not), before any work.
            ValueError: If ``n_rows`` is under 1, before any work.
        """
        rows = int_at_least("n_rows", n_rows, 1)
        return self._element_frame().head(rows)

    def to_markdown(self, *, include_hidden: bool = False) -> str:
        """The document's Markdown (A1's ``PDFDocument.to_markdown``): hidden elements only with ``include_hidden``.

        Raises:
            TypeError: If ``include_hidden`` is not a bool, before any work.
        """
        shown = strict_bool("include_hidden", include_hidden)
        return self.document.to_markdown(include_hidden=shown)

    @property
    def db_table(self) -> str:
        """The element frame's table name in :meth:`query_data`'s SQL (see the module notes)."""
        return _TABLE_PREFIX + _NOT_IDENTIFIER.sub("_", self.metadata.doc_id)

    def query_data(self, sql: str) -> QueryResult:
        """Run ``sql`` on the reader's private DuckDB, where :attr:`db_table` is the element frame (module notes).

        ``sql`` runs as it is written: SQL built from untrusted input is the
        application's to guard. DuckDB installs no extension a query needs:
        the caller installs it, by an explicit ``INSTALL`` in their own SQL
        or in their environment.

        Returns:
            The rows, read whole: ``.pl()``, ``.pa()``, ``.dicts()``.

        Raises:
            TypeError: If ``sql`` is not a str, before any work.
            ValueError: If ``sql`` is blank, before any work.
            duckdb.Error: As DuckDB raises it; for a query that needs an extension not installed, too.
        """
        statement = non_blank_str("sql", sql)
        with self._lock:
            connection = self._connection.opened(self.db_table, self.to_arrow_table())
            return query_result(connection, statement)

    def close(self) -> None:
        """Release the DuckDB connection now: optional, never required (see the module notes); idempotent."""
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "PDFReader":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Release the DuckDB connection at the block's end; an error raised in it propagates."""
        self.close()

    def _element_frame(self) -> pl.DataFrame:
        """The element frame, built on first use, then cached: never handed out (callers get clones)."""
        with self._lock:
            if self._frame is None:
                self._frame = element_frame(self.document)
            return self._frame


def _checked_source(source: object, password: object) -> _Source:
    """The reader's source, checked in the writer's order (see the module notes): a PDF, a bundle, or a document."""
    if isinstance(source, Mapping):
        _checked_password(password)
        return document_from_json(dict(source))
    if not isinstance(source, (str, os.PathLike)):
        raise TypeError(f"source must be a str, os.PathLike or Mapping, got {type(source).__name__}")
    path = _checked_path(source, "source").absolute()
    checked = _checked_password(password)
    if path.is_dir():
        return _checked_bundle(path, path)
    if path.name == MANIFEST and path.is_file():
        return _checked_bundle(path, path.parent)
    return PDFSource(path=checked_pdf_file(path, "source"), password=checked)


def _checked_bundle(path: Path, root: Path) -> _Bundle:
    """The bundle at ``root`` (``path`` names it: the directory, or its manifest), if its manifest is valid."""
    if not os.path.lexists(root / MANIFEST):
        raise ValueError(f"source {path} is a directory without a {MANIFEST}: not a bundle")
    try:
        _read_manifest(root)
    except BundleCorruptError as exc:
        raise ValueError(f"source {root} is not a valid bundle: {exc}") from exc
    return _Bundle(root)


def _read(source: _Source, options: PipelineOptions) -> PDFDocument:
    """The source's document: the Mapping's, the bundle's (loaded and verified), or the PDF's (extracted)."""
    if isinstance(source, PDFDocument):
        return source
    if isinstance(source, _Bundle):
        return load_bundle(source.root)
    return extract_document(
        source.path,
        password=source.password,
        workers=options.workers,
        ocr=options.ocr,
        dpi=options.dpi,
        vision_dpi=options.vision_dpi,
        max_render_pixels=options.max_render_pixels,
        min_image_dimension=options.min_image_dimension,
        strict=options.strict,
    )
