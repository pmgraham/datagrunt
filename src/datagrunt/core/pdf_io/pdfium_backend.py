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


def extract_images(pdf_path: str, page_number: int, output_dir: str = None, name_prefix: str = "page") -> dict:
    """pdfium equivalent of extractors.extract_images (same return shape).

    Applies the shared MIN_IMAGE_DIMENSION filter so counts match the pymupdf
    engine. Writes files when output_dir is provided; otherwise metadata only.
    """
    pdfium, raw = pdfium_extractors._import_pdfium()
    try:
        pdf = pdfium.PdfDocument(str(pdf_path))
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "message": f"Failed to open PDF: {e}"}
    try:
        if page_number < 0 or page_number >= len(pdf):
            return {"status": "error", "message": f"Page {page_number} out of range"}
        page = pdf[page_number]
        _, height = page.get_size()

        images = []
        idx = 0
        for obj in page.get_objects(filter=(raw.FPDF_PAGEOBJ_IMAGE,), max_depth=15):
            px_w, px_h = obj.get_px_size()
            if px_w < MIN_IMAGE_DIMENSION or px_h < MIN_IMAGE_DIMENSION:
                continue
            left, bottom, right, top = obj.get_bounds()
            bbox = pdfium_extractors._topleft_xywh(
                [round(left, 2), round(bottom, 2), round(right, 2), round(top, 2)], height
            )
            file_path = None
            fmt = "png"
            if output_dir:
                base = Path(output_dir) / f"{name_prefix}_page{page_number}_img{idx}"
                written = pdfium_extractors._extract_image(obj, base)
                if written is not None:
                    file_path = str(written)
                    fmt = written.suffix.lstrip(".") or "png"
            images.append(
                {
                    "file_path": file_path,
                    "bbox": bbox,
                    "width_px": px_w,
                    "height_px": px_h,
                    "format": fmt,
                }
            )
            idx += 1
        return {"status": "success", "images": images}
    finally:
        pdf.close()


def ocr_page(pdf_path: str, page_number: int, dpi: int = 300) -> dict:
    """pdfium equivalent of extractors.ocr_page: render via pdfium, OCR via Tesseract.

    Reuses extractors._ocr_data_to_blocks for identical block construction, so
    only the page-rendering source differs from the pymupdf path.
    """
    pdfium, _ = pdfium_extractors._import_pdfium()
    _, pytesseract, Output = extractors._import_ocr_deps()

    try:
        pdf = pdfium.PdfDocument(str(pdf_path))
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "message": f"Failed to open PDF: {e}"}
    try:
        if page_number < 0 or page_number >= len(pdf):
            return {"status": "error", "message": f"Page {page_number} out of range"}
        page = pdf[page_number]
        img = page.render(scale=dpi / 72.0).to_pil().convert("RGB")
    finally:
        pdf.close()

    try:
        data = pytesseract.image_to_data(img, output_type=Output.DICT)
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "message": f"Tesseract OCR failed: {e}"}

    return {
        "status": "success",
        "blocks": extractors._ocr_data_to_blocks(data, dpi),
        "ocr_engine": "tesseract",
        "dpi": dpi,
    }
