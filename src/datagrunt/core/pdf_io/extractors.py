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


def _classify_block(font_size: float, is_bold: bool, all_sizes: list) -> str:
    """Classify a text block based on font size relative to the page."""
    if not all_sizes:
        return "body_text"
    median_size = sorted(all_sizes)[len(all_sizes) // 2]

    if font_size >= median_size * 1.6:
        return "header"
    elif font_size >= median_size * 1.2:
        return "subheader"
    elif font_size < median_size * 0.85:
        return "caption"
    return "body_text"


def extract_text_blocks(pdf_path: str, page_number: int) -> dict:
    """Extract text blocks from a PDF page with font and position metadata.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.

    Returns:
        Dict with status and list of text blocks, each containing text,
        bounding box, font info, and classification.
    """
    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= doc.page_count:
        doc.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    try:
        page = doc[page_number]
        data = page.get_text("dict", sort=True)
        raw_blocks = [b for b in data["blocks"] if b.get("type") == 0]

        all_sizes = []
        for block in raw_blocks:
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    if span["text"].strip():
                        all_sizes.append(span["size"])

        blocks = []
        for order, block in enumerate(raw_blocks):
            texts = []
            fonts = []
            sizes = []
            bold_flags = []

            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text = span["text"]
                    if text.strip():
                        texts.append(text)
                        fonts.append(span["font"])
                        sizes.append(span["size"])
                        bold_flags.append(bool(span["flags"] & 16))

            full_text = " ".join(texts).strip()
            if not full_text:
                continue

            dominant_font = max(set(fonts), key=fonts.count) if fonts else "unknown"
            dominant_size = max(set(sizes), key=sizes.count) if sizes else 0
            is_bold = any(bold_flags)
            is_italic = False
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    if span["text"].strip() and (span["flags"] & 2):
                        is_italic = True
                        break

            bbox = block["bbox"]
            classification = _classify_block(dominant_size, is_bold, all_sizes)

            blocks.append({
                "text": full_text,
                "bbox": {
                    "x": round(bbox[0], 2),
                    "y": round(bbox[1], 2),
                    "w": round(bbox[2] - bbox[0], 2),
                    "h": round(bbox[3] - bbox[1], 2),
                },
                "font": dominant_font,
                "font_size": round(dominant_size, 1),
                "is_bold": is_bold,
                "is_italic": is_italic,
                "classification": classification,
                "reading_order": order,
            })
    finally:
        doc.close()

    return {"status": "success", "blocks": blocks}


def _import_pdfplumber():
    """Import pdfplumber lazily with a helpful error if the extra is missing."""
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pdfplumber


def extract_tables(pdf_path: str, page_number: int) -> dict:
    """Extract tables from a PDF page with position and structure metadata.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.

    Returns:
        Dict with status and list of tables, each containing a 2D data array,
        bounding box, row/column counts, and header detection.
    """
    pdfplumber = _import_pdfplumber()
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= len(pdf.pages):
        pdf.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    try:
        page = pdf.pages[page_number]
        found_tables = page.find_tables()

        tables = []
        for table in found_tables:
            data = table.extract()
            if not data:
                continue

            bbox = table.bbox
            num_rows = len(data)
            num_cols = max(len(row) for row in data) if data else 0

            has_header = (
                num_rows > 1
                and all(cell is not None and cell.strip() for cell in data[0])
            )

            tables.append({
                "data": data,
                "bbox": {
                    "x": round(bbox[0], 2),
                    "y": round(bbox[1], 2),
                    "w": round(bbox[2] - bbox[0], 2),
                    "h": round(bbox[3] - bbox[1], 2),
                },
                "rows": num_rows,
                "columns": num_cols,
                "has_header_row": has_header,
            })
    finally:
        pdf.close()

    return {"status": "success", "tables": tables}


def extract_images(
    pdf_path: str,
    page_number: int,
    output_dir: str = None,
    name_prefix: str = "page",
) -> dict:
    """Extract embedded images from a PDF page.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.
        output_dir: Directory to write images to. When ``None``, no files are
            written and each image's ``file_path`` is ``None`` (metadata only).
        name_prefix: Filename prefix used to keep image names unique across
            documents: ``{name_prefix}_page{page_number}_img{idx}.{ext}``.

    Returns:
        Dict with status and list of image metadata (file path, pixel
        dimensions, format, and position on the page).
    """
    import os

    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= doc.page_count:
        doc.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        page = doc[page_number]
        image_list = page.get_images()

        blocks = page.get_text("dict")["blocks"]
        image_blocks = [b for b in blocks if b.get("type") == 1]

        images = []
        for idx, img_info in enumerate(image_list):
            xref = img_info[0]
            try:
                base_image = doc.extract_image(xref)
            except Exception:
                continue

            ext = base_image.get("ext", "png")
            width = base_image.get("width", 0)
            height = base_image.get("height", 0)
            image_bytes = base_image.get("image", b"")

            if not image_bytes:
                continue

            # Skip tiny layout artifacts, borders, and spacer pixels.
            if width < 40 or height < 40:
                continue

            file_path = None
            if output_dir:
                filename = f"{name_prefix}_page{page_number}_img{idx}.{ext}"
                file_path = os.path.join(output_dir, filename)
                with open(file_path, "wb") as f:
                    f.write(image_bytes)

            bbox = {"x": 0, "y": 0, "w": 0, "h": 0}
            if idx < len(image_blocks):
                b = image_blocks[idx]["bbox"]
                bbox = {
                    "x": round(b[0], 2),
                    "y": round(b[1], 2),
                    "w": round(b[2] - b[0], 2),
                    "h": round(b[3] - b[1], 2),
                }

            images.append({
                "file_path": file_path,
                "bbox": bbox,
                "width_px": width,
                "height_px": height,
                "format": ext,
            })
    finally:
        doc.close()

    return {"status": "success", "images": images}


def _import_ocr_deps():
    """Import OCR deps (PIL, pytesseract) lazily with a helpful error."""
    try:
        from PIL import Image
        import pytesseract
        from pytesseract import Output
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return Image, pytesseract, Output


def ocr_page(pdf_path: str, page_number: int, dpi: int = 300) -> dict:
    """OCR a PDF page by rendering it to an image and running Tesseract.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.
        dpi: Resolution for rendering the page. Higher = better OCR but slower.

    Returns:
        Dict with status and list of text blocks with confidence scores and
        bounding boxes (in PDF points).
    """
    pymupdf = _import_pymupdf()
    Image, pytesseract, Output = _import_ocr_deps()

    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= doc.page_count:
        doc.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    try:
        page = doc[page_number]
        pix = page.get_pixmap(dpi=dpi)
        mode = "RGBA" if pix.alpha else "RGB"
        img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
    finally:
        doc.close()

    try:
        data = pytesseract.image_to_data(img, output_type=Output.DICT)
    except Exception as e:
        return {"status": "error", "message": f"Tesseract OCR failed: {e}"}

    scale = 72.0 / dpi

    lines = {}
    n = len(data["text"])
    for i in range(n):
        conf = int(data["conf"][i])
        text = data["text"][i].strip()
        if conf < 0 or not text:
            continue

        key = (data["block_num"][i], data["par_num"][i], data["line_num"][i])
        word = {
            "text": text,
            "confidence": conf,
            "left": data["left"][i],
            "top": data["top"][i],
            "width": data["width"][i],
            "height": data["height"][i],
        }
        lines.setdefault(key, []).append(word)

    blocks = []
    for key, words in lines.items():
        line_text = " ".join(w["text"] for w in words)
        avg_conf = sum(w["confidence"] for w in words) / len(words)

        x0 = min(w["left"] for w in words)
        y0 = min(w["top"] for w in words)
        x1 = max(w["left"] + w["width"] for w in words)
        y1 = max(w["top"] + w["height"] for w in words)

        blocks.append({
            "text": line_text,
            "bbox": {
                "x": round(x0 * scale, 2),
                "y": round(y0 * scale, 2),
                "w": round((x1 - x0) * scale, 2),
                "h": round((y1 - y0) * scale, 2),
            },
            "confidence": round(avg_conf, 1),
            "word_count": len(words),
            "per_word_confidence": [w["confidence"] for w in words],
        })

    return {
        "status": "success",
        "blocks": blocks,
        "ocr_engine": "tesseract",
        "dpi": dpi,
    }
