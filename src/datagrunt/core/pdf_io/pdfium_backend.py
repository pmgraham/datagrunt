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


def _dominant(values, default=None):
    """Most common value in a list (ties broken by first occurrence)."""
    values = [v for v in values if v not in (None, "")]
    if not values:
        return default
    return max(set(values), key=values.count)


def _group_blocks(items: list) -> list:
    """Group per-object text items (top-left coords) into line-merged blocks.

    items: dicts with text, x0, x1, y_top, y_bot, size, font, bold, italic.
    Returns extractors.extract_text_blocks-shaped block dicts (minus
    classification, which the caller adds).
    """
    if not items:
        return []

    # 1. Cluster items into lines by y_top proximity.
    lines: list[dict] = []
    for it in sorted(items, key=lambda i: (round(i["y_top"], 0), i["x0"])):
        placed = False
        for ln in lines:
            if abs(ln["y"] - it["y_top"]) <= max(2.0, it["size"] * 0.4):
                ln["items"].append(it)
                placed = True
                break
        if not placed:
            lines.append({"y": it["y_top"], "items": [it]})

    # 2. Build per-line records.
    line_recs = []
    for ln in lines:
        its = sorted(ln["items"], key=lambda i: i["x0"])
        sizes = [i["size"] for i in its]
        line_recs.append(
            {
                "text": " ".join(i["text"] for i in its).strip(),
                "x0": min(i["x0"] for i in its),
                "x1": max(i["x1"] for i in its),
                "y_top": min(i["y_top"] for i in its),
                "y_bot": max(i["y_bot"] for i in its),
                "size": _dominant(sizes, default=0.0),
                "font": _dominant([i["font"] for i in its], default=""),
                "bold": any(i["bold"] for i in its),
                "italic": any(i["italic"] for i in its),
            }
        )
    line_recs.sort(key=lambda r: (r["y_top"], r["x0"]))

    # 3. Merge adjacent lines with similar size, small vertical gap, x-overlap.
    merged: list[dict] = []
    for ln in line_recs:
        if merged:
            prev = merged[-1]
            gap = ln["y_top"] - prev["y_bot"]
            same_size = abs(ln["size"] - prev["size"]) < 0.6
            close = 0 <= gap <= max(prev["size"], 1.0) * 1.6
            overlap = not (ln["x0"] > prev["x1"] or ln["x1"] < prev["x0"])
            if same_size and close and overlap:
                prev["text"] = (prev["text"] + " " + ln["text"]).strip()
                prev["x0"] = min(prev["x0"], ln["x0"])
                prev["x1"] = max(prev["x1"], ln["x1"])
                prev["y_bot"] = ln["y_bot"]
                prev["bold"] = prev["bold"] or ln["bold"]
                prev["italic"] = prev["italic"] or ln["italic"]
                continue
        merged.append(dict(ln))

    # 4. Finalize into extractor-shaped blocks (classification added by caller).
    blocks = []
    for order, b in enumerate(merged):
        if not b["text"]:
            continue
        blocks.append(
            {
                "text": b["text"],
                "bbox": {
                    "x": round(b["x0"], 2),
                    "y": round(b["y_top"], 2),
                    "w": round(b["x1"] - b["x0"], 2),
                    "h": round(b["y_bot"] - b["y_top"], 2),
                },
                "font": b["font"],
                "font_size": round(b["size"], 1),
                "is_bold": b["bold"],
                "is_italic": b["italic"],
                "reading_order": order,
            }
        )
    return blocks


def extract_text_blocks(pdf_path: str, page_number: int) -> dict:
    """pdfium equivalent of extractors.extract_text_blocks (same return shape).

    Sources text from pdfium text objects (preserving pdfium's completeness),
    groups them into line-merged blocks, and classifies via the shared
    extractors._classify_block using the page's font-size distribution.
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
        textpage = page.get_textpage()

        items = []
        for obj in page.get_objects(filter=(raw.FPDF_PAGEOBJ_TEXT,), max_depth=15):
            left, bottom, right, top = obj.get_bounds()
            text = textpage.get_text_bounded(left=left, bottom=bottom, right=right, top=top).strip()
            if not text:
                continue
            font_name = ""
            weight = 400
            try:
                font = obj.get_font()
                font_name = font.get_family_name() or ""
                weight = font.get_weight() or 400
            except Exception:  # noqa: BLE001 - font metadata is best-effort
                pass
            lower = font_name.lower()
            items.append(
                {
                    "text": text,
                    "x0": round(left, 2),
                    "x1": round(right, 2),
                    "y_top": round(height - top, 2),
                    "y_bot": round(height - bottom, 2),
                    "size": round(obj.get_font_size(), 1),
                    "font": font_name,
                    "bold": weight >= 600,
                    "italic": ("italic" in lower or "oblique" in lower),
                }
            )
    finally:
        pdf.close()

    blocks = _group_blocks(items)
    # Build the font-size distribution from the per-object items (mirrors the
    # pymupdf extractor's per-span basis). Using merged-block sizes would skew
    # the median when many same-size lines collapse into one block.
    all_sizes = [it["size"] for it in items if it["text"]]
    for b in blocks:
        b["classification"] = extractors._classify_block(b["font_size"], b["is_bold"], all_sizes)
    return {"status": "success", "blocks": blocks}
