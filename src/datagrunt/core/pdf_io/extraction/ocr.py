"""Shared OCR: render-agnostic Tesseract block construction."""

from datagrunt.core.pdf_io.extraction.shapes import BBox, OcrBlock

# Dynamic DPI scaling thresholds for scanned (OCR) pages.
LARGE_FORMAT_DIMENSION = 1500
LARGE_FORMAT_DPI = 75
STANDARD_DPI = 150


def dpi_for_page(width: float, height: float) -> int:
    """Return the OCR render DPI for a page of the given size (points)."""
    is_large = width > LARGE_FORMAT_DIMENSION or height > LARGE_FORMAT_DIMENSION
    return LARGE_FORMAT_DPI if is_large else STANDARD_DPI


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
    lines = {}
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


def ocr_data_to_blocks(img, dpi: int) -> list:
    """Run Tesseract on a PIL image and return ``OcrBlock`` lines."""
    pytesseract, Output = _import_ocr_deps()
    data = pytesseract.image_to_data(img, output_type=Output.DICT)
    return _data_to_blocks(data, dpi)
