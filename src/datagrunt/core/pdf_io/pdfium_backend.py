"""pdfium-backed extraction primitives matching the ``extractors`` interface.

These return the exact dict shapes of their ``extractors`` (pymupdf)
counterparts so that ``pdfcomponents.parse_page`` can run unchanged with either
backend. Tables are NOT provided here -- ``parse_page`` always sources tables
from ``extractors.extract_tables`` (pdfplumber), which is engine-independent.
"""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core.pdf_io import extractors, pdfium_extractors
from datagrunt.core.pdf_io.pdfium_extractors import MIN_IMAGE_DIMENSION


def analyze_page(pdf_path: str, page_number: int) -> dict:
    """pdfium equivalent of extractors.analyze_page (same return keys)."""
    pdfium, raw = pdfium_extractors._import_pdfium()
    try:
        pdf = pdfium.PdfDocument(str(pdf_path))
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "message": f"Failed to open PDF: {e}"}
    try:
        if page_number < 0 or page_number >= len(pdf):
            return {"status": "error", "message": f"Page {page_number} out of range"}
        page = pdf[page_number]
        width, height = page.get_size()
        textpage = page.get_textpage()
        text = textpage.get_text_bounded().strip()

        text_objs = image_objs = path_objs = 0
        for obj in page.get_objects(
            filter=(raw.FPDF_PAGEOBJ_TEXT, raw.FPDF_PAGEOBJ_IMAGE, raw.FPDF_PAGEOBJ_PATH),
            max_depth=15,
        ):
            if obj.type == raw.FPDF_PAGEOBJ_TEXT:
                text_objs += 1
            elif obj.type == raw.FPDF_PAGEOBJ_IMAGE:
                image_objs += 1
            elif obj.type == raw.FPDF_PAGEOBJ_PATH:
                path_objs += 1

        has_text = len(text) > 0
        return {
            "status": "success",
            "page_number": page_number,
            "total_pages": len(pdf),
            "width": float(width),
            "height": float(height),
            "rotation": page.get_rotation(),
            "has_text_layer": has_text,
            "is_scanned": (not has_text and image_objs > 0),
            "text_block_count": text_objs,
            "image_count": image_objs,
            "image_block_count": image_objs,
            "has_line_drawings": path_objs > 0,
            "text_length": len(text),
        }
    finally:
        pdf.close()
