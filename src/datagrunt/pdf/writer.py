"""The v5 PDFWriter (#371, A7b): a PDF's document, written to disk.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf`` until the
5.0 flip (#373).

:class:`PDFWriter` takes a source PDF's path or a
:class:`~datagrunt.pdf.elements.PDFDocument`. Its construction checks are
A8a's (:class:`~datagrunt.pdf.pipeline.PipelineOptions`,
:class:`~datagrunt.pdf.pipeline.PDFSource`), and run before any work: a
bad option, password or source type raises, and no file is read. A path is
made absolute at construction (a later ``chdir`` cannot redirect it); the
file is first read when a method needs the document, which is then
extracted (A8a's :func:`~datagrunt.pdf.pipeline.extract_document`, with the
writer's options) and cached for the writer's life. A failed extraction is
not cached: the next call extracts again. A document given as the source is
the document, never extracted.

The writer holds no open file, handle or process between calls (extraction
and every render close theirs before returning), so there is nothing to
close.

Each method writes the document as one format and returns the absolute
path written (``Path.absolute()``: symlinks are not resolved):

- :meth:`PDFWriter.write_json`: the codec's document JSON
  (:func:`~datagrunt.pdf.codec.document_to_json_bytes`: ASCII, lossless,
  read back by :func:`~datagrunt.pdf.codec.document_from_json_bytes`).
- :meth:`PDFWriter.write_jsonl`: one JSON object a line, each element's row
  of A7a's frame (:func:`~datagrunt.pdf.frame.element_frame`), in document
  order: its columns in order, ``bbox`` an object, ``scope_path`` and
  ``hidden_reasons`` arrays. Compact UTF-8 JSON, each line ending in a
  newline; no element is an empty file. The frame's strings hold each lone
  surrogate as its escape (``\\ud800``): the JSON is the lossless form.
- :meth:`PDFWriter.write_markdown`: the document's ``to_markdown()``, UTF-8,
  each lone surrogate written as its escape (as the bundle's Markdown).
  Hidden elements stay behind their ``<!-- hidden: ... -->`` markers (A1).

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
are refused), and a target that is a directory raises
``IsADirectoryError`` (checked again as the file is written). No ``fsync``
before the rename (as the bundle): a write survives a killed process, not a
power loss.
"""

import errno
import json
import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

import polars as pl

from datagrunt.pdf.assembly import MIN_IMAGE_DIMENSION
from datagrunt.pdf.codec import document_to_json_bytes
from datagrunt.pdf.elements import PDFDocument
from datagrunt.pdf.frame import element_frame, utf8_text
from datagrunt.pdf.pdfium_engine import _checked_password, _checked_path
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_document
from datagrunt.pdf.quality import OcrMode

__all__ = ["PDFWriter"]

# A temporary file's name: this, then a uuid4's hex (the bundle's staging prefix).
_TEMPORARY_PREFIX = ".tmp_"


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
        ValueError: If an option is out of range (spec invariant 2), or
            ``password`` holds NUL.
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
        self._source, self._document = _checked_source(source, password)

    def write_json(self, out_filepath: str | os.PathLike[str]) -> Path:
        """Write the document as the codec's JSON at ``out_filepath`` (see the module notes).

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work).
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
        """
        target = _file_target(out_filepath)
        data = document_to_json_bytes(self._extracted())
        return _write_atomically(target, lambda handle: handle.write(data))

    def write_jsonl(self, out_filepath: str | os.PathLike[str]) -> Path:
        """Write the document's frame as JSON Lines at ``out_filepath``, one element a line (see the module notes).

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work).
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
        """
        target = _file_target(out_filepath)
        frame = element_frame(self._extracted())
        return _write_atomically(target, lambda handle: _write_rows(frame, handle))

    def write_json_newline_delimited(self, out_filepath: str | os.PathLike[str]) -> Path:
        """:meth:`write_jsonl` under its v4 name."""
        return self.write_jsonl(out_filepath)

    def write_markdown(self, out_filepath: str | os.PathLike[str]) -> Path:
        """Write the document's Markdown at ``out_filepath``, UTF-8 (see the module notes).

        Returns:
            The absolute path written.

        Raises:
            TypeError: If ``out_filepath`` is not a path (before any work).
            IsADirectoryError: If ``out_filepath`` is a directory (before any work).
            OSError: If the file cannot be written; as extraction raises it.
        """
        target = _file_target(out_filepath)
        data = utf8_text(self._extracted().to_markdown()).encode("utf-8")
        return _write_atomically(target, lambda handle: handle.write(data))

    def _extracted(self) -> PDFDocument:
        """The document: the one given, or the source's, extracted on first use and cached."""
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
    """The writer's source PDF (path made absolute) or its document: exactly one is None."""
    if isinstance(source, PDFDocument):
        _checked_password(password)
        return None, source
    if not isinstance(source, (str, os.PathLike)):
        raise TypeError(f"source must be a str, os.PathLike or PDFDocument, got {type(source).__name__}")
    return PDFSource(path=_checked_path(source, "source"), password=_checked_password(password)), None


def _extract(source: PDFSource, options: PipelineOptions) -> PDFDocument:
    """The document of ``source``, extracted with ``options``."""
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
    _refuse_directory(target)
    return target


def _refuse_directory(target: Path) -> None:
    if target.is_dir():
        raise IsADirectoryError(errno.EISDIR, "out_filepath is a directory, not a file", str(target))


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


def _write_rows(frame: pl.DataFrame, handle: BinaryIO) -> None:
    """Write each row of ``frame`` as a line of compact UTF-8 JSON, in order."""
    for row in frame.iter_rows(named=True):
        line = json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        handle.write(line.encode("utf-8") + b"\n")
