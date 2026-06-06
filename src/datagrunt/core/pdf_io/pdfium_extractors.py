"""Pure-Python PDF extractor functions backed by PDFium (pypdfium2).

Ported from a standalone ``pdf_deconstruct.py`` script. This engine emits a
*native* schema (full page text, positioned text objects, and embedded image
files) that is distinct from the pymupdf unified element schema. Third-party
imports are performed lazily so that ``import datagrunt`` works on a base
install without the optional ``[pdf]`` extra.
"""

# standard library
import glob
import hashlib
import os
from pathlib import Path

# local libraries
from datagrunt.core.pdf_io import extractors
from datagrunt.core.pdf_io.extractors import PDF_EXTRA_HINT

# OCR fallback DPI scaling (mirrors pdfcomponents thresholds).
LARGE_FORMAT_DIMENSION = 1500
LARGE_FORMAT_DPI = 75
STANDARD_DPI = 150


def _import_pdfium():
    """Import pypdfium2 lazily with a helpful error if the extra is missing."""
    try:
        import pypdfium2 as pdfium
        import pypdfium2.raw as raw
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pdfium, raw


def _topleft_xywh(bbox: list, page_height: float) -> dict:
    """Convert PDFium's [left, bottom, right, top] (bottom-left origin) into a
    top-left {x, y, w, h} box, matching the convention datagrunt uses."""
    left, bottom, right, top = bbox
    return {
        "x": round(left, 2),
        "y": round(page_height - top, 2),
        "w": round(right - left, 2),
        "h": round(top - bottom, 2),
    }


def _extract_image(obj, base: Path):
    """Write an image object to disk, preserving original format when possible.

    pypdfium2's ``extract`` chooses the extension from the image filter and
    reuses the stored bytes (no re-encode) when it can; we fall back to render
    only if raw extraction fails. Returns the path written, or ``None``.
    """
    pdfium, _ = _import_pdfium()
    base.parent.mkdir(parents=True, exist_ok=True)
    try:
        obj.extract(str(base))
    except pdfium.PdfiumError:
        try:
            obj.extract(str(base), fb_render=True)
        except pdfium.PdfiumError:
            return None
    matches = glob.glob(str(base) + ".*")
    return Path(matches[0]) if matches else None


def parse_pdfium_page(pdf_path: str, page_index: int, image_output_dir: str = None) -> dict:
    """Parse a single PDF page into the native PDFium schema.

    Args:
        pdf_path: Path to the PDF file.
        page_index: Zero-indexed page number.
        image_output_dir: If provided, embedded images are written here and
            referenced via ``file``; otherwise images are metadata only.

    Returns:
        A page dict: page_number, width, height, text, text_objects, images, ocr.
    """
    pdfium, raw = _import_pdfium()
    stem = Path(pdf_path).stem

    pdf = pdfium.PdfDocument(str(pdf_path))
    try:
        page = pdf[page_index]
        width, height = page.get_size()
        textpage = page.get_textpage()
        full_text = textpage.get_text_bounded()

        text_objects = []
        images = []
        img_index = 0

        for obj in page.get_objects(
            filter=(raw.FPDF_PAGEOBJ_TEXT, raw.FPDF_PAGEOBJ_IMAGE),
            max_depth=15,
        ):
            left, bottom, right, top = obj.get_bounds()
            bbox = [round(left, 2), round(bottom, 2), round(right, 2), round(top, 2)]
            position = _topleft_xywh(bbox, height)

            if obj.type == raw.FPDF_PAGEOBJ_TEXT:
                text = textpage.get_text_bounded(
                    left=left, bottom=bottom, right=right, top=top
                ).strip()
                if text:
                    text_objects.append(
                        {
                            "text": text,
                            "bbox": bbox,
                            "position": position,
                            "font_size": round(obj.get_font_size(), 2),
                        }
                    )
            elif obj.type == raw.FPDF_PAGEOBJ_IMAGE:
                px_w, px_h = obj.get_px_size()
                written = None
                if image_output_dir:
                    base = Path(image_output_dir) / f"{stem}_p{page_index + 1}_img{img_index}"
                    written = _extract_image(obj, base)
                images.append(
                    {
                        "file": str(written) if written else None,
                        "bbox": bbox,
                        "position": position,
                        "px_width": px_w,
                        "px_height": px_h,
                        "extracted": written is not None,
                    }
                )
                img_index += 1

        ocr_used = False
        if not full_text.strip():
            page_dpi = (
                LARGE_FORMAT_DPI
                if (width > LARGE_FORMAT_DIMENSION or height > LARGE_FORMAT_DIMENSION)
                else STANDARD_DPI
            )
            ocr_result = extractors.ocr_page(pdf_path, page_index, dpi=page_dpi)
            if ocr_result.get("status") == "success":
                blocks = ocr_result.get("blocks", [])
                if blocks:
                    ocr_used = True
                    full_text = "\n".join(b["text"] for b in blocks)
                    for b in blocks:
                        bb = b["bbox"]
                        # OCR bbox is [x, y, x+w, y+h] in top-left origin (PDF pts);
                        # native text_objects use [left, bottom, right, top] (PDFium
                        # bottom-left origin). ``position`` is top-left {x,y,w,h} in both.
                        ocr_bbox = [
                            bb["x"],
                            bb["y"],
                            round(bb["x"] + bb["w"], 2),
                            round(bb["y"] + bb["h"], 2),
                        ]
                        text_objects.append(
                            {
                                "text": b["text"],
                                "bbox": ocr_bbox,
                                "position": {"x": bb["x"], "y": bb["y"], "w": bb["w"], "h": bb["h"]},
                                "font_size": None,
                            }
                        )

        return {
            "page_number": page_index + 1,
            "width": round(float(width), 2),
            "height": round(float(height), 2),
            "text": full_text,
            "text_objects": text_objects,
            "images": images,
            "ocr": ocr_used,
        }
    finally:
        pdf.close()


def combine_pdfium_pages(source: str, total_pages: int, pages: list, errors: list) -> dict:
    """Wrap parsed PDFium pages in the native document envelope."""
    return {
        "document": {
            "source": str(source),
            "page_count": total_pages,
            "errors": [e for e in errors] if errors else None,
            "pages": pages,
        }
    }


def flatten_pdfium_document(document: dict) -> list:
    """Flatten a native PDFium document into one record per text object/image.

    Scalar columns only (position split into x/y/w/h, bbox JSON-encoded) so the
    result loads cleanly into a columnar frame regardless of element type.
    """
    import json

    records = []
    pages = document.get("document", {}).get("pages", [])
    for page in pages:
        page_no = page.get("page_number")
        ocr = page.get("ocr", False)
        for obj in page.get("text_objects", []):
            pos = obj.get("position", {})
            records.append(
                {
                    "page": page_no,
                    "type": "text",
                    "text": obj.get("text"),
                    "font_size": obj.get("font_size"),
                    "x": float(pos.get("x", 0.0)),
                    "y": float(pos.get("y", 0.0)),
                    "w": float(pos.get("w", 0.0)),
                    "h": float(pos.get("h", 0.0)),
                    "bbox": json.dumps(obj.get("bbox")),
                    "file": None,
                    "px_width": None,
                    "px_height": None,
                    "ocr": ocr,
                }
            )
        for img in page.get("images", []):
            pos = img.get("position", {})
            records.append(
                {
                    "page": page_no,
                    "type": "image",
                    "text": None,
                    "font_size": None,
                    "x": float(pos.get("x", 0.0)),
                    "y": float(pos.get("y", 0.0)),
                    "w": float(pos.get("w", 0.0)),
                    "h": float(pos.get("h", 0.0)),
                    "bbox": json.dumps(img.get("bbox")),
                    "file": img.get("file"),
                    "px_width": img.get("px_width"),
                    "px_height": img.get("px_height"),
                    "ocr": ocr,
                }
            )
    return records
