"""Native-schema pdfium reader (text + positioned text objects + images)."""

import json
from pathlib import Path

from datagrunt.core.pdf_io.extraction.image_dedupe import dedupe_image_files
from datagrunt.core.pdf_io.extraction.markdown_escape import escape_leading_markdown
from datagrunt.core.pdf_io.extraction.ocr import dpi_for_page, ocr_data_to_blocks
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument


class PdfiumNativeReader:
    """Parse pages into the native pdfium schema and assemble documents."""

    def __init__(self, filepath):
        """Store the path.

        Args:
            filepath (str or Path): Path to the PDF file.
        """
        self.filepath = Path(filepath)
        import threading
        self._local = threading.local()

    def __enter__(self):
        if not hasattr(self._local, "depth"):
            self._local.depth = 0
        if self._local.depth == 0:
            self._local.doc = PdfiumDocument(self.filepath)
        self._local.depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._local.depth -= 1
        if self._local.depth == 0:
            if hasattr(self._local, "doc") and self._local.doc:
                self._local.doc.close()
                self._local.doc = None

    def _get_doc(self):
        if hasattr(self._local, "doc") and self._local.doc:
            return self._local.doc, False
        return PdfiumDocument(self.filepath), True

    def parse_page(self, page_index: int, image_output_dir: str = None) -> dict:
        """Parse one page into the native schema dict."""
        doc, should_close = self._get_doc()
        try:
            page = doc.page(page_index)
            width, height = page.size()
            full_text = page.full_text()
            text_objects = [self._text_object(it) for it in page.text_items()]
            images = [
                self._image(img)
                for img in page.image_items(
                    output_dir=image_output_dir, name_prefix=Path(self.filepath).stem, page_number=page_index
                )
            ]
            ocr_used = False
            if not full_text.strip():
                full_text, extra, ocr_used = self._ocr_fallback(doc, page_index, width, height)
                text_objects.extend(extra)
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
            if should_close:
                doc.close()

    def _text_object(self, item) -> dict:
        """Convert a TextItem to the native text-object dict."""
        bbox = [item.x0, round(item.y_bot, 2), item.x1, round(item.y_top, 2)]
        return {
            "text": item.text,
            "bbox": bbox,
            "position": {
                "x": item.x0,
                "y": item.y_top,
                "w": round(item.x1 - item.x0, 2),
                "h": round(item.y_bot - item.y_top, 2),
            },
            "font_size": item.size,
        }

    def _image(self, img) -> dict:
        """Convert an ImageBlock to the native image dict."""
        return {
            "file": img.file_path,
            "bbox": [img.bbox.x, img.bbox.y, round(img.bbox.x + img.bbox.w, 2), round(img.bbox.y + img.bbox.h, 2)],
            "position": img.bbox.to_dict(),
            "px_width": img.width_px,
            "px_height": img.height_px,
            "extracted": img.file_path is not None,
        }

    def _ocr_fallback(self, doc, page_index, width, height):
        """Run OCR on an image-only page; return (text, extra_objects, used)."""
        page_dpi = dpi_for_page(width, height)
        img = doc.page(page_index).render_pil(dpi=page_dpi)
        blocks = ocr_data_to_blocks(img, page_dpi)
        if not blocks:
            return "", [], False
        text = "\n".join(b.text for b in blocks)
        extra = [
            {
                "text": b.text,
                "bbox": [b.bbox.x, b.bbox.y, round(b.bbox.x + b.bbox.w, 2), round(b.bbox.y + b.bbox.h, 2)],
                "position": b.bbox.to_dict(),
                "font_size": None,
            }
            for b in blocks
        ]
        return text, extra, True

    def combine(self, total_pages: int, pages: list, errors: list) -> dict:
        """Wrap parsed pages in the native document envelope."""
        return {
            "document": {
                "source": str(self.filepath),
                "page_count": total_pages,
                "errors": list(errors) if errors else None,
                "pages": pages,
            }
        }

    @staticmethod
    def flatten(document: dict) -> list:
        """Flatten a native document into one record per text object/image."""
        records = []
        for page in document.get("document", {}).get("pages", []):
            page_no, ocr = page.get("page_number"), page.get("ocr", False)
            for obj in page.get("text_objects", []):
                records.append(PdfiumNativeReader._flat_text(obj, page_no, ocr))
            for img in page.get("images", []):
                records.append(PdfiumNativeReader._flat_image(img, page_no, ocr))
        return records

    @staticmethod
    def _flat_text(obj, page_no, ocr) -> dict:
        """Flatten one native text object to a scalar record."""
        pos = obj.get("position", {})
        return {
            "page": page_no, "type": "text", "text": obj.get("text"), "font_size": obj.get("font_size"),
            "x": float(pos.get("x", 0.0)), "y": float(pos.get("y", 0.0)), "w": float(pos.get("w", 0.0)),
            "h": float(pos.get("h", 0.0)), "bbox": json.dumps(obj.get("bbox")), "file": None,
            "px_width": None, "px_height": None, "ocr": ocr,
        }

    @staticmethod
    def _flat_image(img, page_no, ocr) -> dict:
        """Flatten one native image to a scalar record."""
        pos = img.get("position", {})
        return {
            "page": page_no, "type": "image", "text": None, "font_size": None,
            "x": float(pos.get("x", 0.0)), "y": float(pos.get("y", 0.0)), "w": float(pos.get("w", 0.0)),
            "h": float(pos.get("h", 0.0)), "bbox": json.dumps(img.get("bbox")), "file": img.get("file"),
            "px_width": img.get("px_width"), "px_height": img.get("px_height"), "ocr": ocr,
        }

    @staticmethod
    def dedupe_images(document: dict) -> int:
        """Collapse byte-identical extracted image files; return count removed."""
        images = [
            img
            for page in document.get("document", {}).get("pages", [])
            for img in page.get("images", [])
        ]
        return dedupe_image_files(
            images,
            lambda img: img.get("file"),
            lambda img, p: img.__setitem__("file", p),
        )

    @staticmethod
    def to_markdown(document: dict) -> str:
        """Render native schema document into Markdown paragraphs."""
        blocks = []
        for page in document.get("document", {}).get("pages", []):
            text = (page.get("text") or "").strip()
            if not text:
                continue
            for para in text.split("\n\n"):
                para = para.strip()
                if para:
                    # Native paragraphs are all body text; escape any leading
                    # markdown metacharacter so it is not rendered as structure.
                    blocks.append(escape_leading_markdown(para))
        return "\n\n".join(blocks) + "\n"
