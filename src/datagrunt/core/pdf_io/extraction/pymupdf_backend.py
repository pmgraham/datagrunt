"""pymupdf-backed extraction backend (the reference engine)."""

from datagrunt.core.pdf_io.extraction.base import ExtractionBackend
from datagrunt.core.pdf_io.extraction.ocr import ocr_data_to_blocks
from datagrunt.core.pdf_io.extraction.shapes import BBox, ImageBlock, PageAnalysis, TextBlock
from datagrunt.core.pdf_io.extraction.text_block_builder import classify_font_size


def _import_pymupdf():
    """Import pymupdf lazily with a helpful error if the extra is missing."""
    try:
        import pymupdf
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError("PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]") from exc
    return pymupdf


class PyMuPDFBackend(ExtractionBackend):
    """Extraction via PyMuPDF (text/dict spans, images) + Tesseract OCR."""

    def analyze_page(self, page_number: int) -> PageAnalysis:
        """Return page metadata; raises ValueError on failure/out-of-range."""
        pymupdf = _import_pymupdf()
        doc = pymupdf.open(self.filepath)
        try:
            if page_number < 0 or page_number >= doc.page_count:
                raise ValueError(f"Page {page_number} out of range")
            page = doc[page_number]
            text = page.get_text("text").strip()
            blocks = page.get_text("dict")["blocks"]
            images = page.get_images()
            text_blocks = [b for b in blocks if b.get("type") == 0]
            image_blocks = [b for b in blocks if b.get("type") == 1]
            has_text = len(text) > 0
            drawings = page.get_drawings()
            has_lines = any(item[0] in ("l", "re") for d in drawings for item in d.get("items", []))
            return PageAnalysis(
                width=page.rect.width,
                height=page.rect.height,
                rotation=page.rotation,
                has_text_layer=has_text,
                is_scanned=(not has_text and len(images) > 0),
                text_block_count=len(text_blocks),
                image_count=len(images),
                image_block_count=len(image_blocks),
                has_line_drawings=has_lines,
                text_length=len(text),
            )
        finally:
            doc.close()

    def extract_text_blocks(self, page_number: int) -> list:
        """Return classified text blocks (ported from extractors.extract_text_blocks)."""
        pymupdf = _import_pymupdf()
        doc = pymupdf.open(self.filepath)
        try:
            if page_number < 0 or page_number >= doc.page_count:
                return []
            page = doc[page_number]
            data = page.get_text("dict", sort=True)
            raw_blocks = [b for b in data["blocks"] if b.get("type") == 0]
            all_sizes = [
                span["size"]
                for block in raw_blocks
                for line in block.get("lines", [])
                for span in line.get("spans", [])
                if span["text"].strip()
            ]
            blocks = []
            for order, block in enumerate(raw_blocks):
                tb = self._build_block(block, all_sizes, order)
                if tb is not None:
                    blocks.append(tb)
            return blocks
        finally:
            doc.close()

    def _build_block(self, block: dict, all_sizes: list, order: int):
        """Reduce one pymupdf block dict to a TextBlock (or None if empty)."""
        texts, fonts, sizes, bolds, italics = [], [], [], [], []
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                if span["text"].strip():
                    texts.append(span["text"])
                    fonts.append(span["font"])
                    sizes.append(span["size"])
                    bolds.append(bool(span["flags"] & 16))
                    italics.append(bool(span["flags"] & 2))
        full_text = " ".join(texts).strip()
        if not full_text:
            return None
        dominant_font = max(set(fonts), key=fonts.count) if fonts else "unknown"
        dominant_size = max(set(sizes), key=sizes.count) if sizes else 0
        is_bold = any(bolds)
        x0, y0, x1, y1 = block["bbox"]
        return TextBlock(
            text=full_text,
            bbox=BBox(x=round(x0, 2), y=round(y0, 2), w=round(x1 - x0, 2), h=round(y1 - y0, 2)),
            font=dominant_font,
            font_size=round(dominant_size, 1),
            is_bold=is_bold,
            is_italic=any(italics),
            classification=classify_font_size(round(dominant_size, 1), is_bold, all_sizes),
            reading_order=order,
        )

    def extract_images(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> list:
        """Return embedded images (ported from extractors.extract_images)."""
        import os

        pymupdf = _import_pymupdf()
        doc = pymupdf.open(self.filepath)
        try:
            if page_number < 0 or page_number >= doc.page_count:
                return []
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            page = doc[page_number]
            image_list = page.get_images()
            image_blocks = [b for b in page.get_text("dict")["blocks"] if b.get("type") == 1]
            results = []
            for idx, img_info in enumerate(image_list):
                block = self._image_block(doc, img_info, idx, image_blocks, output_dir, name_prefix, page_number)
                if block is not None:
                    results.append(block)
            return results
        finally:
            doc.close()

    def _image_block(self, doc, img_info, idx, image_blocks, output_dir, name_prefix, page_number):
        """Extract one image to an ImageBlock (or None to skip), <40px filtered."""
        try:
            base_image = doc.extract_image(img_info[0])
        except Exception:  # noqa: BLE001
            return None
        ext = base_image.get("ext", "png")
        width, height = base_image.get("width", 0), base_image.get("height", 0)
        image_bytes = base_image.get("image", b"")
        if not image_bytes or width < 40 or height < 40:
            return None
        file_path = None
        if output_dir:
            import os

            file_path = os.path.join(output_dir, f"{name_prefix}_page{page_number}_img{idx}.{ext}")
            with open(file_path, "wb") as f:
                f.write(image_bytes)
        bbox = BBox(0, 0, 0, 0)
        if idx < len(image_blocks):
            b = image_blocks[idx]["bbox"]
            bbox = BBox(x=round(b[0], 2), y=round(b[1], 2), w=round(b[2] - b[0], 2), h=round(b[3] - b[1], 2))
        return ImageBlock(bbox=bbox, file_path=file_path, width_px=width, height_px=height, fmt=ext)

    def ocr_page(self, page_number: int, dpi: int = 300) -> list:
        """Render the page via pymupdf and OCR it with Tesseract."""
        pymupdf = _import_pymupdf()
        from PIL import Image

        doc = pymupdf.open(self.filepath)
        try:
            if page_number < 0 or page_number >= doc.page_count:
                return []
            page = doc[page_number]
            pix = page.get_pixmap(dpi=dpi)
            mode = "RGBA" if pix.alpha else "RGB"
            img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
        finally:
            doc.close()
        return ocr_data_to_blocks(img, dpi)
