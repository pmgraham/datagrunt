"""The v5 PDFWriter (#371, A7b): a PDF's document, written to disk.

Public since 5.0 (#373): ``datagrunt.PDFWriter`` and ``datagrunt.pdf.PDFWriter``,
both imported on first access, so ``import datagrunt`` needs no [pdf] extra
(:mod:`datagrunt.pdf._extra`).

:class:`PDFWriter` takes a source PDF's path or a
:class:`~datagrunt.pdf.elements.PDFDocument`. Its construction checks run
before any work, in order: A8a's (:class:`~datagrunt.pdf.pipeline.PipelineOptions`;
the source's type and the password, as
:class:`~datagrunt.pdf.pipeline.PDFSource` checks them), then, for a path,
the file itself (spec invariant 1), by the check A8b's ``PDFReader`` shares
(:func:`~datagrunt.pdf._source.checked_pdf_file`): a missing path raises
``FileNotFoundError``; one that is not a regular file, or has no PDF header
where pdfium looks for one, ``ValueError``. That check reads at most the
file's first 1028 bytes and closes it. A path is made absolute at
construction (a later ``chdir`` cannot redirect it). The document stays
lazy: it is extracted when a method first needs it (A8a's
:func:`~datagrunt.pdf.pipeline.extract_document`, with the writer's
options) and cached for the writer's life. A failed extraction is not
cached: the next call extracts again. A lock makes the first use
thread-safe: two threads never extract the same writer's document twice. A
document given as the source is the document, never extracted, and no file
is checked.

The writer holds no open file, handle or process between calls (extraction
and every render close theirs before returning), so there is nothing to
close.

Each method writes the document in one form. A path returned is absolute
(``Path.absolute()``: symlinks are not resolved):

- :meth:`PDFWriter.write_json`: the codec's document JSON
  (:func:`~datagrunt.pdf.codec.document_to_json_bytes`: ASCII, lossless,
  read back by :func:`~datagrunt.pdf.codec.document_from_json_bytes`).
- :meth:`PDFWriter.write_jsonl`: one JSON object a line, each element's row
  of A7a's frame (:func:`~datagrunt.pdf.frame.element_frame`), in document
  order: its columns in order, ``bbox`` an object, ``scope_path``,
  ``hidden_reasons`` and ``image_only_reasons`` arrays (the last null for
  a page whose reasons were not recorded). Compact UTF-8 JSON, each line
  ending in a newline; a document without elements is an empty file. The
  frame's strings hold each lone surrogate as its escape (``\\ud800``): the
  JSON is the lossless form.
- :meth:`PDFWriter.write_markdown`: the document's ``to_markdown()``, UTF-8,
  each lone surrogate written as its escape (as the bundle's Markdown).
  Hidden elements are omitted by default; with ``include_hidden=True`` each
  is written behind its ``<!-- hidden: ... -->`` marker (A1).
- :meth:`PDFWriter.write_parquet`: A7a's frame as it is, by polars (its
  keyword arguments passed on): List and Struct columns kept.
- :meth:`PDFWriter.write_csv`: the frame flattened (:func:`_csv_frame`),
  by polars (its keyword arguments passed on), because CSV cannot hold a
  List or Struct column: ``bbox`` becomes ``bbox_x0``, ``bbox_y0``,
  ``bbox_x1`` and ``bbox_y1`` in its place, and ``scope_path``,
  ``hidden_reasons`` and ``image_only_reasons`` compact JSON-array strings
  (``["Intro","Details"]``, ``[]`` for none). A null ``image_only_reasons``
  (not recorded) stays null, which polars writes as its ``null_value``: an
  empty cell by default, never ``[]`` (none) or a string a reader would
  take for a list. Every value is written as it is (preserve, not
  transform): a cell that starts with ``=``, ``+``, ``-`` or ``@`` is not
  escaped against spreadsheet formula injection. Guarding a spreadsheet
  that opens the CSV is the application's concern.
- :meth:`PDFWriter.extract_images`: each figure's PNG, through its A1
  loader, into ``output_dir``: an extracted figure's PNGs are read in
  batches, up to 64 a capped figure worker
  (:func:`~datagrunt.pdf._loaders.figure_pngs`), as ``Figure.read_bytes``
  reads one; any other figure by its own ``read_bytes``. Every figure is
  read: its ``sha256`` hashes the raw stream, not the image dictionary that
  decodes it, so only its PNG shows whether it is a picture already written
  (one read a figure: correctness over speed, as the bundle's notes say).
  With ``dedupe`` (the default) each distinct picture is written
  once, as the bundle names it: ``<sha256>.png`` (the raw stream's sha256,
  a figure's identity), and another picture of the same stream (the stream
  under another image dictionary: a colour space, a palette, a
  ``/Decode``) as ``<sha256>-<its PNG's sha256>.png``; without it, each
  figure is written as ``<element_id>.png`` (``p0001_e003.png``). A figure
  whose bytes cannot be read (a ``DatagruntPDFError``, an ``OSError``) or
  are not a PNG is skipped, never silently: a warning on the module logger
  names the figure, its sha256 and why; a later figure of its stream is
  still read. A figure is read only when its loader is bound to the
  writer's source as the bundle binds one
  (:func:`~datagrunt.pdf.bundle._figure_unbound`): it reads from the
  source PDF (A5a's ``FigureRead``) or is a bundle's verifying reader. A
  writer built from a PDFDocument has no source to compare: its figures'
  loaders must still name the PDF they read, or be a bundle's reader. A
  figure with no loader (a bare ``image_path``, which would be read
  unverified, whatever file it names) or another loader is skipped unread,
  with a warning. Any other error fails the call. The paths returned are
  the files written, in document order.
- :meth:`PDFWriter.render_pages_as_images`: every page, rendered in A5a's
  guarded, capped worker (:func:`~datagrunt.pdf.render.render_page`, as
  the bundle renders: its memory cap and timeout) at
  :func:`~datagrunt.pdf.render.effective_dpi` of ``dpi`` (the writer's when
  None) and ``max_render_pixels``, each page's size the document's, as 8-bit
  grey ``page_NNNN.png`` in ``output_dir``. A vision page is rendered too:
  its own page image is the bundle's to keep. Pages render on ``workers``
  threads in the bundle's sliding window: at most ``workers`` in flight,
  the next starting as soon as one ends; each page's pixels are written in
  its own thread and do not outlive it. A page whose render is refused or fails
  (``RenderRefused``, ``RenderFailed``: its effective DPI's refusal too) is
  skipped, never silently: a warning on the module logger names the page
  and why. The image-only pages skipped (``Page.image_only_reasons``,
  #470), whose content lives only in their page images, are also named in
  one ``UserWarning`` a call, with the document and ``output_dir``: the
  source PDF is their only full copy (#471, as the bundle warns). Any other error fails the call. The paths returned are
  the files written, in page order. The PDF rendered is the file at the
  source path when the call runs.
- :meth:`PDFWriter.write_bundle`: A7a's
  :func:`~datagrunt.pdf.bundle.write_bundle` of the document, with the
  writer's source, password and options (``ocr``,
  ``min_image_dimension`` and ``strict`` recorded as its
  ``extraction_params``) and ``overwrite``: a bundle already there that is
  this write's is reused, any other replaced only with ``overwrite=True``.
  It warns as the bundle's write does, an image-only page whose page image
  it does not write included (#471).

A writer built from a PDFDocument has no PDF to render:
:meth:`PDFWriter.render_pages_as_images` and :meth:`PDFWriter.write_bundle`
raise ``ValueError`` (as v4's writer of a parsed document did), before any
work.

Every file is written atomically (:func:`_write_atomically`): a new file,
``.tmp_<uuid4 hex>`` beside the target (the same directory, so the same
filesystem), created exclusively with the process's usual permissions,
filled, then renamed onto the target (``os.replace``). On any failure, an
interrupt included, the temporary file is removed and the error raised; an
existing target is then as it was. A writer killed outright can leave its
temporary file, the only trace: never a partial target. A target that
exists is replaced (v4 overwrote too); a symlink there is itself replaced,
and what it pointed to is untouched. Missing directories above the target
are created. Nothing outside them is written.

Every path is checked before any work: a str or os.PathLike of str (bytes
are refused); a file target that is a directory raises
``IsADirectoryError`` (checked again as the file is written), and an
``output_dir`` that exists and is not a directory raises
``NotADirectoryError``. No ``fsync`` before the rename (as the bundle): a
write survives a killed process, not a power loss.
"""

import errno
import hashlib
import json
import logging
import os
import threading
import uuid
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO

import polars as pl
from PIL import Image

from datagrunt.pdf._loaders import figure_pngs
from datagrunt.pdf._options import MIN_IMAGE_DIMENSION, OcrMode, RenderPagesMode, checked_render_pages
from datagrunt.pdf._source import checked_pdf_file
from datagrunt.pdf._validate import checked_password as _checked_password
from datagrunt.pdf._validate import checked_path as _checked_path
from datagrunt.pdf._validate import int_at_least, strict_bool
from datagrunt.pdf.bundle import (
    _checked_target,
    _drawn,
    _figure_unbound,
    _in_page_order,
    _only_copy_notice,
    _Pictures,
    _png,
    _write_rows,
    write_bundle,
)
from datagrunt.pdf.codec import document_to_json_bytes
from datagrunt.pdf.elements import Figure, Page, PDFDocument
from datagrunt.pdf.frame import _BBOX_FIELDS, element_frame, utf8_text
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_document
from datagrunt.pdf.render import _MIN_TARGET_DPI, RenderedPage, RenderFailed, RenderRefused

__all__ = ["PDFWriter"]

_LOGGER = logging.getLogger(__name__)

# A temporary file's name: this, then a uuid4's hex (the bundle's staging prefix).
_TEMPORARY_PREFIX = ".tmp_"
# The frame's List columns, which a CSV holds as JSON-array strings (a null as polars writes one).
_LIST_COLUMNS = ("scope_path", "hidden_reasons", "image_only_reasons")


class PDFWriter:
    """Writes a PDF's document to disk (see the module notes).

    Args:
        source: The PDF's path (a str or os.PathLike of str), or a
            PDFDocument to write as it is.
        password: The encrypted PDF's password, or None. Never in a repr or
            an error message.
        workers, ocr, dpi, vision_dpi, max_render_pixels,
        min_image_dimension, strict: The extraction options, as
            :class:`~datagrunt.pdf.pipeline.PipelineOptions` holds them.

    Raises:
        TypeError: If ``source`` is not a path or a PDFDocument, or an option
            or ``password`` is of the wrong type.
        ValueError: If an option is out of range (spec invariant 2),
            ``password`` holds NUL, or the source path is not a regular file
            or not a PDF (spec invariant 1).
        FileNotFoundError: If the source path does not exist (spec invariant 1).
    """

    def __init__(
        self,
        source: str | os.PathLike[str] | PDFDocument,
        *,
        password: str | None = None,
        workers: int = 1,
        ocr: OcrMode = "auto",
        dpi: int = 200,
        vision_dpi: int = 300,
        max_render_pixels: int = 16_000_000,
        min_image_dimension: int = MIN_IMAGE_DIMENSION,
        strict: bool = False,
        sandbox: bool = False,
    ) -> None:
        self._options = PipelineOptions(
            workers=workers,
            ocr=ocr,
            dpi=dpi,
            vision_dpi=vision_dpi,
            max_render_pixels=max_render_pixels,
            min_image_dimension=min_image_dimension,
            strict=strict,
            sandbox=sandbox,
        )
        self._source, self._document = _checked_source(source, password)
        self._lock = threading.RLock()

    def write_json(self, out_filepath: str | os.PathLike[str]) -> Path:
        """Write the document as the codec's JSON at ``out_filepath`` (see the module notes).

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work).
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.
        """
        target = _file_target(out_filepath)
        return _write_bytes(target, document_to_json_bytes(self._extracted()))

    def write_jsonl(self, out_filepath: str | os.PathLike[str]) -> Path:
        """Write the document's frame as JSON Lines at ``out_filepath``, one element a line (see the module notes).

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work).
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.
        """
        target = _file_target(out_filepath)
        frame = element_frame(self._extracted())
        return _write_atomically(target, lambda handle: _write_rows(frame, handle.write))

    def write_json_newline_delimited(self, out_filepath: str | os.PathLike[str]) -> Path:
        """:meth:`write_jsonl` under its v4 name."""
        return self.write_jsonl(out_filepath)

    def write_markdown(self, out_filepath: str | os.PathLike[str], *, include_hidden: bool = False) -> Path:
        """Write the document's Markdown at ``out_filepath``, UTF-8: hidden elements only with ``include_hidden``.

        See the module notes.

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path, or ``include_hidden`` not a bool (before any work).
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.
        """
        target = _file_target(out_filepath)
        shown = strict_bool("include_hidden", include_hidden)
        markdown = self._extracted().to_markdown(include_hidden=shown)
        return _write_bytes(target, utf8_text(markdown).encode("utf-8"))

    def write_parquet(self, out_filepath: str | os.PathLike[str], **polars_kwargs: Any) -> Path:
        """Write the document's frame as Parquet at ``out_filepath``, nested types kept (see the module notes).

        ``polars_kwargs`` go to ``polars.DataFrame.write_parquet`` (``compression``, say).

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work); as polars raises it.
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.
        """
        target = _file_target(out_filepath)
        frame = element_frame(self._extracted())
        return _write_atomically(target, lambda handle: frame.write_parquet(handle, **polars_kwargs))

    def write_csv(self, out_filepath: str | os.PathLike[str], **polars_kwargs: Any) -> Path:
        """Write the document's frame, flattened, as CSV at ``out_filepath`` (see the module notes).

        ``polars_kwargs`` go to ``polars.DataFrame.write_csv`` (``separator``, say).
        Values are never sanitized: formula injection is the application's concern.

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work); as polars raises it.
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.
        """
        target = _file_target(out_filepath)
        frame = _csv_frame(element_frame(self._extracted()))
        return _write_atomically(target, lambda handle: frame.write_csv(handle, **polars_kwargs))

    def extract_images(self, output_dir: str | os.PathLike[str], *, dedupe: bool = True) -> list[Path]:
        """Write each figure's PNG into ``output_dir``, made when missing (see the module notes).

        Returns:
            The absolute paths written, in document order: a figure skipped is not among them.

        Raises:
            TypeError: If ``output_dir`` is not a path, or ``dedupe`` not a bool (before any work);
                if a figure's loader returns something other than bytes.
            NotADirectoryError: If ``output_dir`` exists and is not a directory (before any work).
            OSError: If a file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.
        """
        directory = _directory_target(output_dir)
        strict_bool("dedupe", dedupe)
        figures = self._extracted().figures
        directory.mkdir(parents=True, exist_ok=True)
        source = None if self._source is None else self._source.path
        return _write_figures(_bound(figures, source), directory, dedupe=dedupe)

    def render_pages_as_images(self, output_dir: str | os.PathLike[str], *, dpi: int | None = None) -> list[Path]:
        """Render every page into ``output_dir``, made when missing, as ``page_NNNN.png`` (see the module notes).

        Returns:
            The absolute paths written, in page order: a page skipped is not among them.

        Raises:
            ValueError: If the writer was built from a PDFDocument, or ``dpi`` is under 36 (before any work).
            TypeError: If ``output_dir`` is not a path, or ``dpi`` not an int or None (before any work).
            NotADirectoryError: If ``output_dir`` exists and is not a directory (before any work).
            OSError: If a file cannot be written; as extraction raises it.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.

        Warns:
            UserWarning: Once, naming the image-only pages skipped: the source PDF is their only full copy (#471).
        """
        source = self._required_source("render_pages_as_images")
        directory = _directory_target(output_dir)
        target_dpi = self._options.dpi if dpi is None else int_at_least("dpi", dpi, _MIN_TARGET_DPI)
        document = self._extracted()
        directory.mkdir(parents=True, exist_ok=True)
        return _write_renders(document, directory, source, target_dpi, self._options)

    def write_bundle(
        self,
        output_dir: str | os.PathLike[str],
        *,
        overwrite: bool = False,
        include_images: bool = True,
        render_pages: RenderPagesMode = True,
    ) -> PDFDocument:
        """Write the document's bundle at ``output_dir``, atomically (A7a; see the module notes).

        Returns:
            The document with its bundle paths set, as :func:`~datagrunt.pdf.bundle.write_bundle` returns it
            (a reused bundle's document, as ``load_bundle`` returns it).

        Raises:
            ValueError: If the writer was built from a PDFDocument (before any work), or the source no
                longer hashes to the document's sha256 (it changed since it was extracted).
            TypeError: If ``output_dir`` is not a path, or ``overwrite`` not a bool (before any work).
            FileExistsError: If ``output_dir`` is a file, a symlink or a non-empty directory holding no bundle
                (before any work), or holds a bundle that is not this write's and ``overwrite`` is False.
            FileNotFoundError: If ``output_dir``'s parent is missing (before any work).
            PDFSourceChangedError: If the source changes while it is bundled: nothing is published.
            PDFEncryptedError, PDFCorruptError, PDFSourceChangedError: As extraction
                (:func:`~datagrunt.pdf.pipeline.extract_document`) raises them, on the call that first
                extracts the document.

        Warns:
            UserWarning: As :func:`~datagrunt.pdf.bundle.write_bundle` warns, an image-only page whose page image
                is not written included (#471).
        """
        source = self._required_source("write_bundle")
        _checked_target(output_dir)
        replace = strict_bool("overwrite", overwrite)
        include_imgs = strict_bool("include_images", include_images)
        render_pgs = checked_render_pages(render_pages)
        options = self._options
        sandbox_kw = {"sandbox": True} if options.sandbox else {}
        return write_bundle(
            self._extracted(),
            output_dir,
            source=source.path,
            password=source.password,
            dpi=options.dpi,
            vision_dpi=options.vision_dpi,
            max_render_pixels=options.max_render_pixels,
            workers=options.workers,
            extraction_params={
                "ocr": options.ocr,
                "min_image_dimension": options.min_image_dimension,
                "strict": options.strict,
            },
            overwrite=replace,
            include_images=include_imgs,
            render_pages=render_pgs,
            **sandbox_kw,
        )

    def _extracted(self) -> PDFDocument:
        """The document: the one given, or the source's, extracted on first use and cached."""
        with self._lock:
            if self._document is None:
                self._document = _extract(self._required_source("extraction"), self._options)
            return self._document

    def _required_source(self, method: str) -> PDFSource:
        """The source PDF, which ``method`` reads; a writer built from a PDFDocument has none."""
        if self._source is None:
            raise ValueError(
                f"{method} needs the source PDF: this writer was built from a PDFDocument, "
                "which holds no PDF to read; build it from the .pdf path"
            )
        return self._source


def _checked_source(source: object, password: object) -> tuple[PDFSource | None, PDFDocument | None]:
    """The writer's source PDF (path made absolute, its file checked) or its document: exactly one is None."""
    if isinstance(source, PDFDocument):
        _checked_password(password)
        return None, source
    if not isinstance(source, (str, os.PathLike)):
        raise TypeError(f"source must be a str, os.PathLike or PDFDocument, got {type(source).__name__}")
    path = _checked_path(source, "source").absolute()
    checked = _checked_password(password)
    return PDFSource(path=checked_pdf_file(path, "source"), password=checked), None


def _extract(source: PDFSource, options: PipelineOptions) -> PDFDocument:
    """The document of ``source``, extracted with ``options``."""
    if options.sandbox:
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
            sandbox=True,
        )
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


def _file_target(out_filepath: object) -> Path:
    """``out_filepath`` made absolute, if a file may be written there: it is not a directory."""
    target = _checked_path(out_filepath, "out_filepath").absolute()
    _refuse_directory(target, "out_filepath")
    return target


def _refuse_directory(target: Path, name: str = "the target") -> None:
    if target.is_dir():
        raise IsADirectoryError(errno.EISDIR, f"{name} is a directory, not a file", str(target))


def _directory_target(output_dir: object) -> Path:
    """``output_dir`` made absolute, if files may be written into it: it is a directory, or is missing."""
    directory = _checked_path(output_dir, "output_dir").absolute()
    if directory.exists() and not directory.is_dir():
        raise NotADirectoryError(errno.ENOTDIR, "output_dir exists and is not a directory", str(directory))
    return directory


def _write_atomically(target: Path, fill: Callable[[BinaryIO], object]) -> Path:
    """Write ``target`` through ``fill`` into a new temporary file beside it, then rename it on (see the module notes).

    Returns:
        ``target``.

    Raises:
        IsADirectoryError: If ``target`` is a directory, or becomes one.
        OSError: If a directory cannot be made, or the file written or renamed.
    """
    _refuse_directory(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{_TEMPORARY_PREFIX}{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as handle:
            fill(handle)
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return target


def _write_bytes(target: Path, data: bytes) -> Path:
    return _write_atomically(target, lambda handle: handle.write(data))


def _csv_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """A7a's ``frame`` flattened for CSV: ``bbox`` as four columns in its place, the List columns JSON arrays.

    A null list stays null. See the module notes. The other columns are as they are.
    """
    columns: list[pl.Expr | pl.Series] = []
    for name in frame.columns:
        if name == "bbox":
            columns.extend(pl.col(name).struct.field(field).alias(f"bbox_{field}") for field in _BBOX_FIELDS)
        elif name in _LIST_COLUMNS:
            cells = [None if items is None else _json_array(items) for items in frame[name].to_list()]
            columns.append(pl.Series(name, cells, dtype=pl.String))
        else:
            columns.append(pl.col(name))
    return frame.select(columns)


def _json_array(items: list[str]) -> str:
    return json.dumps(items, ensure_ascii=False, separators=(",", ":"))


def _bound(figures: tuple[Figure, ...], source: Path | None) -> tuple[Figure, ...]:
    """The figures bound to ``source`` (as the bundle binds one); each other is skipped unread, with a warning."""
    bound: list[Figure] = []
    for figure in figures:
        unbound = _figure_unbound(figure, source)
        if unbound:
            why = f"it {unbound}: its bytes could not be bound to the source"
            _LOGGER.warning("figure %s (sha256 %s) was not written: %s", figure.element_id, figure.sha256, why)
            continue
        bound.append(figure)
    return tuple(bound)


def _write_figures(figures: tuple[Figure, ...], directory: Path, *, dedupe: bool) -> list[Path]:
    """Write each figure's PNG into ``directory`` (each distinct picture once, with ``dedupe``); the paths, in order.

    Every figure is read. A figure whose bytes cannot be read, or are not a PNG, is skipped with a warning (see
    the module notes).
    """
    written: list[Path] = []
    pictures = _Pictures()
    for figure, read in zip(figures, figure_pngs(figures), strict=True):  # read in batches, each when its turn comes
        data, why = _png(read)
        if data is None:
            _LOGGER.warning("figure %s (sha256 %s) was not written: %s", figure.element_id, figure.sha256, why)
            continue
        if not dedupe:
            written.append(_write_bytes(directory / f"{figure.element_id}.png", data))
            continue
        digest = hashlib.sha256(data).hexdigest()
        name, unwritten = pictures.name(figure.sha256, digest)
        if unwritten:
            written.append(_write_bytes(directory / name, data))
            pictures.record(figure.sha256, digest, name)
    return written


def _write_renders(
    document: PDFDocument, directory: Path, source: PDFSource, dpi: int, options: PipelineOptions
) -> list[Path]:
    """Render and write every page on ``workers`` threads (the bundle's window); the paths written, in page order.

    The image-only pages skipped are named in one warning, with the document and ``directory`` (#471).
    """
    pages = document.pages

    def task(page: Page) -> Path | None:
        return _rendered(page, directory, source, dpi, options.max_render_pixels, sandbox=options.sandbox)

    outcomes = _in_page_order(task, pages, options.workers, "datagrunt-writer")
    unrendered = [
        page.page_number for page, path in zip(pages, outcomes, strict=True) if path is None and page.image_only_reasons
    ]
    if unrendered:
        unsaved = f"render_pages_as_images could not render into {directory} (the {_LOGGER.name} log says why)"
        notice = _only_copy_notice(document.metadata.source_name, unrendered, unsaved)
        warnings.warn(notice, UserWarning, stacklevel=3)
    return [path for path in outcomes if path is not None]


def _rendered(
    page: Page, directory: Path, source: PDFSource, dpi: int, max_pixels: int, *, sandbox: bool = False
) -> Path | None:
    """Render ``page`` at the effective DPI of ``dpi`` and write its PNG; None, with a warning, if it is not."""
    try:
        _, drawn = _drawn(source, page, dpi, max_pixels, sandbox=sandbox)
    except (RenderRefused, RenderFailed) as exc:
        _LOGGER.warning("page %d was not rendered: %s: %s", page.page_number, type(exc).__name__, exc)
        return None
    return _write_png(directory / f"page_{page.page_number:04d}.png", drawn)


def _write_png(target: Path, image: RenderedPage) -> Path:
    """Write 8-bit grey pixels as a PNG, encoded by Pillow straight into the file."""
    picture = Image.frombytes("L", (image.px_width, image.px_height), image.pixels)
    return _write_atomically(target, lambda handle: picture.save(handle, format="PNG"))
