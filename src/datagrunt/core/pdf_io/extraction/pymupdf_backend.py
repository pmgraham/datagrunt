"""pymupdf-backed extraction backend (the reference engine)."""

import logging

from datagrunt.core.pdf_io.extraction._doc_session import _ThreadLocalDocSession
from datagrunt.core.pdf_io.extraction.base import ExtractionBackend
from datagrunt.core.pdf_io.extraction.ocr import ocr_data_to_blocks
from datagrunt.core.pdf_io.extraction.pdfium_document import ENCRYPTED_PDF_MESSAGE
from datagrunt.core.pdf_io.extraction.shapes import BBox, ImageBlock, PageAnalysis, TextBlock
from datagrunt.core.pdf_io.extraction.text_block_builder import classify_by_median, page_median_size

logger = logging.getLogger(__name__)


def _import_pymupdf():
    """Import pymupdf lazily with a helpful error if the extra is missing."""
    try:
        import pymupdf
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError("PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]") from exc
    return pymupdf


class PyMuPDFBackend(_ThreadLocalDocSession, ExtractionBackend):
    """Extraction via PyMuPDF (text/dict spans, images) + Tesseract OCR."""

    def __init__(self, filepath):
        super().__init__(filepath)
        import threading

        self._local = threading.local()

    def _open_doc(self):
        """Open the document, translating the encrypted case to a clear error.

        pymupdf opens password-protected PDFs without raising (every page then
        reads as empty/garbage), so encryption must be detected explicitly via
        ``needs_pass`` to match the pdfium engines' behavior (issue #99).
        """
        pymupdf = _import_pymupdf()
        doc = pymupdf.open(self.filepath)
        if doc.needs_pass:
            doc.close()
            raise ValueError(ENCRYPTED_PDF_MESSAGE)
        return doc

    def _open_resource(self):
        return self._open_doc()

    def page_count(self) -> int:
        """Return the document's page count using pymupdf (no pdfium dependency)."""
        doc, should_close = self._get_doc()
        try:
            return doc.page_count
        finally:
            if should_close:
                doc.close()

    def analyze_page(self, page_number: int) -> PageAnalysis:
        """Return page metadata; raises ValueError on failure/out-of-range."""
        doc, should_close = self._get_doc()
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
            if should_close:
                doc.close()

    def extract_text_blocks(self, page_number: int) -> list:
        """Return classified text blocks (ported from extractors.extract_text_blocks)."""
        doc, should_close = self._get_doc()
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
            median_size = page_median_size(all_sizes)
            blocks = []
            for order, block in enumerate(raw_blocks):
                tb = self._build_block(block, median_size, order)
                if tb is not None:
                    blocks.append(tb)
            return blocks
        finally:
            if should_close:
                doc.close()

    def _build_block(self, block: dict, median_size, order: int):
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
            classification=classify_by_median(round(dominant_size, 1), median_size),
            reading_order=order,
        )

    def extract_page(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> tuple:
        """Parse the page once, returning analysis + text blocks + images.

        Shares a single ``get_text("dict", sort=True)`` and a single
        ``get_images()`` between analysis and extraction. Block/image counts are
        sort-order independent, so the sorted dict yields the same counts the
        unsorted ``analyze_page`` produced. ``get_text("text")`` is retained for
        ``has_text_layer``/``text_length`` to keep parity byte-identical.
        """
        doc, should_close = self._get_doc()
        try:
            if page_number < 0 or page_number >= doc.page_count:
                raise ValueError(f"Page {page_number} out of range")
            page = doc[page_number]
            data = page.get_text("dict", sort=True)
            raw_blocks = data["blocks"]
            text_block_dicts = [b for b in raw_blocks if b.get("type") == 0]
            image_block_dicts = [b for b in raw_blocks if b.get("type") == 1]
            text = page.get_text("text").strip()
            has_text = len(text) > 0
            image_list = page.get_images()
            drawings = page.get_drawings()
            has_lines = any(item[0] in ("l", "re") for d in drawings for item in d.get("items", []))
            analysis = PageAnalysis(
                width=page.rect.width,
                height=page.rect.height,
                rotation=page.rotation,
                has_text_layer=has_text,
                is_scanned=(not has_text and len(image_list) > 0),
                text_block_count=len(text_block_dicts),
                image_count=len(image_list),
                image_block_count=len(image_block_dicts),
                has_line_drawings=has_lines,
                text_length=len(text),
            )
            text_blocks = []
            if has_text:
                all_sizes = [
                    span["size"]
                    for block in text_block_dicts
                    for line in block.get("lines", [])
                    for span in line.get("spans", [])
                    if span["text"].strip()
                ]
                median_size = page_median_size(all_sizes)
                for order, block in enumerate(text_block_dicts):
                    tb = self._build_block(block, median_size, order)
                    if tb is not None:
                        text_blocks.append(tb)
            images = []
            if analysis.image_count > 0:
                images = self._images_from_page(doc, page, image_list, output_dir, name_prefix, page_number)
            return analysis, text_blocks, images
        finally:
            if should_close:
                doc.close()

    def extract_images(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> list:
        """Return embedded images (ported from extractors.extract_images)."""
        doc, should_close = self._get_doc()
        try:
            if page_number < 0 or page_number >= doc.page_count:
                return []
            page = doc[page_number]
            return self._images_from_page(doc, page, page.get_images(), output_dir, name_prefix, page_number)
        finally:
            if should_close:
                doc.close()

    def _images_from_page(self, doc, page, image_list, output_dir, name_prefix, page_number) -> list:
        """Build ImageBlocks from an already-fetched ``page.get_images()`` list.

        Shared by ``extract_images`` and ``extract_page`` so the image list is
        enumerated once per page.
        """
        import os

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        results = []
        # ``get_images()`` lists images in resource (xref) order, which can
        # differ from the content/display order. Resolve each image's bbox by
        # its xref so positions never get swapped; track per-xref occurrences
        # so an image drawn multiple times maps to the correct rect each time.
        xref_occurrence = {}
        for idx, img_info in enumerate(image_list):
            xref = img_info[0]
            occurrence = xref_occurrence.get(xref, 0)
            xref_occurrence[xref] = occurrence + 1
            bbox = self._resolve_image_bbox(page, xref, occurrence)
            block = self._image_block(doc, img_info, idx, bbox, output_dir, name_prefix, page_number)
            if block is not None:
                results.append(block)
        return results

    @staticmethod
    def _resolve_image_bbox(page, xref, occurrence) -> BBox:
        """Return the BBox for the ``occurrence``-th placement of image ``xref``.

        Uses ``page.get_image_rects(xref)`` so the bbox always corresponds to the
        specific image being emitted rather than a position paired by list index.
        Falls back to a zero box when the image has no resolvable rect (e.g. it is
        referenced but never drawn).
        """
        try:
            rects = page.get_image_rects(xref)
        except Exception:  # noqa: BLE001 - rect lookup is best-effort
            rects = []
        if not rects:
            return BBox(0, 0, 0, 0)
        # ``get_image_rects`` returns one rect per placement, in placement order,
        # so the k-th occurrence in ``get_images()`` maps to the k-th rect.
        rect = rects[occurrence] if occurrence < len(rects) else rects[0]
        return BBox(
            x=round(rect.x0, 2),
            y=round(rect.y0, 2),
            w=round(rect.x1 - rect.x0, 2),
            h=round(rect.y1 - rect.y0, 2),
        )

    def _image_block(self, doc, img_info, idx, bbox, output_dir, name_prefix, page_number):
        """Extract one image to an ImageBlock (or None to skip), <40px filtered.

        ``bbox`` is the already-resolved (by xref) position for this image.
        """
        try:
            base_image = doc.extract_image(img_info[0])
        except Exception:  # noqa: BLE001
            return None
        ext = base_image.get("ext", "png")
        width, height = base_image.get("width", 0), base_image.get("height", 0)
        image_bytes = base_image.get("image", b"")
        if not image_bytes or width < 40 or height < 40:
            return None

        # Convert non-web-friendly formats to PNG using Pixmap
        if ext.lower() not in ("png", "jpg", "jpeg", "gif"):
            try:
                import pymupdf

                pix = pymupdf.Pixmap(doc, img_info[0])
                if pix.colorspace and (pix.colorspace.n > 3 or pix.colorspace.name == "DeviceCMYK"):
                    pix = pymupdf.Pixmap(pymupdf.csRGB, pix)
                image_bytes = pix.tobytes("png")
                ext = "png"
            except Exception:  # noqa: BLE001 - Pixmap conversion is best-effort; keep original bytes
                logger.debug("Pixmap PNG conversion failed; keeping original image bytes", exc_info=True)

        file_path = None
        if output_dir:
            import os

            file_path = os.path.join(output_dir, f"{name_prefix}_page{page_number}_img{idx}.{ext}")
            with open(file_path, "wb") as f:
                f.write(image_bytes)
        return ImageBlock(bbox=bbox, file_path=file_path, width_px=width, height_px=height, fmt=ext)

    def ocr_page(self, page_number: int, dpi: int = 300) -> list:
        """Render the page via pymupdf and OCR it with Tesseract."""
        from PIL import Image

        doc, should_close = self._get_doc()
        try:
            if page_number < 0 or page_number >= doc.page_count:
                return []
            page = doc[page_number]
            pix = page.get_pixmap(dpi=dpi)
            mode = "RGBA" if pix.alpha else "RGB"
            img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
        finally:
            if should_close:
                doc.close()
        return ocr_data_to_blocks(img, dpi)
