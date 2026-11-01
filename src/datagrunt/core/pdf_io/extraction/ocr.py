"""Shared OCR: render-agnostic Tesseract block construction."""

import os

from datagrunt.core.pdf_io.extraction.config import _PDFExtractionConfig
from datagrunt.core.pdf_io.extraction.shapes import BBox, OcrBlock
from datagrunt.pdf._executables import trusted_executable


def dpi_for_page(width: float, height: float, config: "_PDFExtractionConfig | None" = None) -> int:
    """Return the OCR render DPI for a page of the given size (points)."""
    # No config -> module defaults, which are the historical STANDARD_DPI /
    # LARGE_FORMAT_DPI / LARGE_FORMAT_DIMENSION constants (now defined once in
    # config.py) — so behavior is unchanged for existing no-config callers.
    cfg = config if config is not None else _PDFExtractionConfig()
    is_large = width > cfg.ocr_large_format_dimension or height > cfg.ocr_large_format_dimension
    return cfg.ocr_large_format_dpi if is_large else cfg.ocr_standard_dpi


def _import_ocr_deps():
    """Import OCR deps (pytesseract, Output) lazily with a helpful error."""
    try:
        import pytesseract
        from pytesseract import Output
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError("PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]") from exc
    return pytesseract, Output


def _data_to_blocks(data: dict, dpi: int) -> list:
    """Group a pytesseract image_to_data DICT into ``OcrBlock`` lines (point bboxes)."""
    scale = 72.0 / dpi
    lines: dict[tuple[int, int, int], list[dict]] = {}
    for i in range(len(data["text"])):
        conf = int(data["conf"][i])
        text = data["text"][i].strip()
        if conf < 0 or not text:
            continue
        key = (data["block_num"][i], data["par_num"][i], data["line_num"][i])
        lines.setdefault(key, []).append(
            {
                "text": text,
                "confidence": conf,
                "left": data["left"][i],
                "top": data["top"][i],
                "width": data["width"][i],
                "height": data["height"][i],
            }
        )
    blocks = []
    for words in lines.values():
        x0 = min(w["left"] for w in words)
        y0 = min(w["top"] for w in words)
        x1 = max(w["left"] + w["width"] for w in words)
        y1 = max(w["top"] + w["height"] for w in words)
        blocks.append(
            OcrBlock(
                text=" ".join(w["text"] for w in words),
                bbox=BBox(
                    x=round(x0 * scale, 2),
                    y=round(y0 * scale, 2),
                    w=round((x1 - x0) * scale, 2),
                    h=round((y1 - y0) * scale, 2),
                ),
                confidence=round(sum(w["confidence"] for w in words) / len(words), 1),
                word_count=len(words),
                per_word_confidence=[w["confidence"] for w in words],
            )
        )
    return blocks


def _use_trusted_tesseract(pytesseract) -> None:
    """Point pytesseract at an absolute tesseract before it runs one; raise TesseractNotFoundError if none.

    An absolute ``tesseract_cmd`` the caller set is kept. A bare name (the
    default, ``tesseract``) is found in an absolute ``PATH`` directory only
    (:func:`~datagrunt.pdf._executables.trusted_executable`) and set as the
    command, so pytesseract never starts a bare name. Any other command (a
    relative path) is refused. Setting it is pytesseract's own configuration,
    and idempotent.
    """
    configured = pytesseract.pytesseract.tesseract_cmd
    if os.path.isabs(configured):
        return
    try:
        found = trusted_executable(configured)
    except (TypeError, ValueError):
        found = None
    if found is None:
        raise pytesseract.TesseractNotFoundError()
    pytesseract.pytesseract.tesseract_cmd = found


def ocr_data_to_blocks(img, dpi: int) -> list:
    """Run Tesseract (an absolute, trusted one: :func:`_use_trusted_tesseract`) on a PIL image; ``OcrBlock`` lines."""
    pytesseract, Output = _import_ocr_deps()
    _use_trusted_tesseract(pytesseract)
    data = pytesseract.image_to_data(img, output_type=Output.DICT)
    return _data_to_blocks(data, dpi)
