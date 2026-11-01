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
"""

import os

from datagrunt.pdf.assembly import MIN_IMAGE_DIMENSION
from datagrunt.pdf.elements import PDFDocument
from datagrunt.pdf.pdfium_engine import _checked_password, _checked_path
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_document
from datagrunt.pdf.quality import OcrMode

__all__ = ["PDFWriter"]


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
