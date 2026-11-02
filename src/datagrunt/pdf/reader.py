"""The v5 PDFReader (#372, A8b): a PDF's document, read in memory, with zero filesystem writes.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf`` until the
5.0 flip (#373).

:class:`PDFReader` takes one of three sources:

- a PDF's path (a str or os.PathLike of str): A8a's
  :func:`~datagrunt.pdf.pipeline.extract_document`, with the reader's
  options and password;
- a bundle directory, or its ``manifest.json``: A7a's
  :func:`~datagrunt.pdf.bundle.load_bundle`, which verifies every artifact;
- a Mapping: A7a's codec's document JSON
  (:func:`~datagrunt.pdf.codec.document_from_json`).

Construction checks (spec invariants 1-2) run before any work, in order:

1. The options, as :class:`~datagrunt.pdf.pipeline.PipelineOptions`
   checks them (``ValueError`` out of bounds, ``TypeError`` of the wrong
   type), and the password (a str without NUL, or None), whatever the
   source. They apply to a PDF; a bundle or a Mapping does not use them.
2. The source. A path that does not exist is a ``FileNotFoundError``. A
   directory is a bundle: it must hold a ``manifest.json``, read and
   checked as ``load_bundle`` checks it (its artifacts are read on first
   use). A file named ``manifest.json`` is its directory's bundle. Any
   other path must be a regular file with ``%PDF`` starting at an offset
   from 0 to 1024, as pdfium finds it (:func:`_checked_pdf_file`: opened
   once, without blocking on a named pipe, checked as a regular file
   through ``fstat``, at most 1028 bytes read). Anything else is a ``ValueError``,
   chained from the bundle's error where there is one. A Mapping is decoded
   at construction: decoding is its check, and the reader keeps the
   document, so the Mapping changed later does not change the reader.

A path is made absolute at construction: a later ``chdir`` cannot
redirect it.

When the work happens: a PDF is extracted, and a bundle loaded, on first
use (the first property or method that needs the document), then cached
for the reader's life. Text and layout are read then, eagerly; a figure's
and a page image's bytes stay lazy, read only through their A1 loaders
(``Figure.read_bytes``, ``PageImage.read_bytes``: a bundle's verifying
reader, or A5a's guarded, capped render). A failed extraction or load is
not cached: the next use tries again. A lock makes the first use
thread-safe: two threads never extract the same reader's document twice.

Workers: ``workers`` goes to ``extract_document``, whose pool starts plain
``python -I`` processes: a caller's script needs no ``__main__`` guard.
"""

import os
import stat
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datagrunt.pdf.assembly import MIN_IMAGE_DIMENSION
from datagrunt.pdf.bundle import MANIFEST, _read_manifest, load_bundle
from datagrunt.pdf.codec import document_from_json
from datagrunt.pdf.elements import PDFDocument
from datagrunt.pdf.exceptions import BundleCorruptError
from datagrunt.pdf.pdfium_engine import _checked_password, _checked_path
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_document
from datagrunt.pdf.quality import OcrMode

__all__ = ["PDFReader"]

# A PDF's header may follow other bytes: pdfium finds "%PDF" (case-sensitive) starting at any offset from 0 to 1024,
# so at most 1028 bytes are read.
_PDF_MARKER = b"%PDF"
_PDF_MARKER_LAST_OFFSET = 1024
_PDF_HEADER_BYTES = _PDF_MARKER_LAST_OFFSET + len(_PDF_MARKER)
# How a source file's head is opened: never blocking on a named pipe (O_NONBLOCK, which a regular file ignores),
# as bytes (O_BINARY, Windows). Each flag is 0 where it does not exist.
_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)


@dataclass(frozen=True, slots=True)
class _Bundle:
    """A bundle source: its directory, made absolute, whose manifest was checked at construction."""

    root: Path


_Source = PDFSource | _Bundle | PDFDocument


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
            or the Mapping is not a document's JSON.
        FileNotFoundError: If the path does not exist.
    """

    def __init__(
        self,
        source: str | os.PathLike[str] | Mapping[str, Any],
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
        self._source = _checked_source(source, _checked_password(password))
        self._document: PDFDocument | None = None
        self._lock = threading.RLock()

    @property
    def document(self) -> PDFDocument:
        """The document: extracted or loaded on first use, then cached (see the module notes)."""
        with self._lock:
            if self._document is None:
                self._document = _read(self._source, self._options)
            return self._document


def _checked_source(source: object, password: str | None) -> _Source:
    """The reader's source, checked (see the module notes): a PDF, a bundle, or a Mapping's decoded document."""
    if isinstance(source, Mapping):
        return document_from_json(dict(source))
    if not isinstance(source, (str, os.PathLike)):
        raise TypeError(f"source must be a str, os.PathLike or Mapping, got {type(source).__name__}")
    path = _checked_path(source, "source").absolute()
    if path.is_dir():
        return _checked_bundle(path, path)
    if path.name == MANIFEST and path.is_file():
        return _checked_bundle(path, path.parent)
    return PDFSource(path=_checked_pdf_file(path, "source"), password=password)


def _checked_bundle(path: Path, root: Path) -> _Bundle:
    """The bundle at ``root`` (``path`` names it: the directory, or its manifest), if its manifest is valid."""
    if not os.path.lexists(root / MANIFEST):
        raise ValueError(f"source {path} is a directory without a {MANIFEST}: not a bundle")
    try:
        _read_manifest(root)
    except BundleCorruptError as exc:
        raise ValueError(f"source {root} is not a valid bundle: {exc}") from exc
    return _Bundle(root)


def _checked_pdf_file(path: Path, name: str) -> Path:
    """``path`` if it is a regular file with ``%PDF`` starting at an offset from 0 to 1024 (pdfium's rule).

    The file is opened once (``O_NONBLOCK``: a named pipe does not block),
    checked as a regular file through ``fstat`` of that descriptor, and at
    most 1028 bytes are read from it. A local stand-in for the writer
    branch's shared ``datagrunt.pdf._source.checked_pdf_file`` (#371), of
    the same contract: the two are one at its merge.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If it is not a regular file, or no ``%PDF`` starts at
            an offset from 0 to 1024.
    """
    descriptor = os.open(path, _OPEN_FLAGS)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{name} {path} is not a regular file")
        head = os.read(descriptor, _PDF_HEADER_BYTES)
    finally:
        os.close(descriptor)
    if _PDF_MARKER not in head:  # the head is 1028 bytes at most: a marker in it starts at offset 1024 at most
        raise ValueError(f"{name} {path} is not a PDF: no %PDF at any offset from 0 to {_PDF_MARKER_LAST_OFFSET}")
    return path


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
