"""Pure-Python PDF extractor functions (PyMuPDF / pdfplumber / Tesseract).

Ported from github.com/pmgraham/pdf-parser-python. Third-party imports are
performed lazily inside each function so that ``import datagrunt`` works on a
base install without the optional ``[pdf]`` extra.
"""

PDF_EXTRA_HINT = (
    "PDF parsing requires extra dependencies. "
    "Install with: pip install datagrunt[pdf]"
)


def _import_pymupdf():
    """Import pymupdf lazily with a helpful error if the extra is missing."""
    try:
        import pymupdf
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pymupdf


def analyze_page(pdf_path: str, page_number: int) -> dict:
    """Analyze a single PDF page and return metadata about its content.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number to analyze.

    Returns:
        Dict with page metadata including dimensions, text/image presence,
        and whether the page appears to be scanned.
    """
    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    page_count = doc.page_count
    if page_number < 0 or page_number >= page_count:
        doc.close()
        return {
            "status": "error",
            "message": f"Page {page_number} out of range (0-{page_count - 1})",
        }

    try:
        page = doc[page_number]
        text = page.get_text("text").strip()
        blocks = page.get_text("dict")["blocks"]
        images = page.get_images()

        text_blocks = [b for b in blocks if b.get("type") == 0]
        image_blocks = [b for b in blocks if b.get("type") == 1]

        has_text_layer = len(text) > 0
        is_scanned = not has_text_layer and len(images) > 0

        drawings = page.get_drawings()
        has_lines = any(
            item[0] in ("l", "re") for d in drawings for item in d.get("items", [])
        )

        result = {
            "status": "success",
            "page_number": page_number,
            "total_pages": page_count,
            "width": page.rect.width,
            "height": page.rect.height,
            "rotation": page.rotation,
            "has_text_layer": has_text_layer,
            "is_scanned": is_scanned,
            "text_block_count": len(text_blocks),
            "image_count": len(images),
            "image_block_count": len(image_blocks),
            "has_line_drawings": has_lines,
            "text_length": len(text),
        }
    finally:
        doc.close()

    return result
