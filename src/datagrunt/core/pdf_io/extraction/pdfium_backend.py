"""pdfium-backed extraction backend (structured/unified schema)."""

from datagrunt.core.pdf_io.extraction.base import ExtractionBackend
from datagrunt.core.pdf_io.extraction.ocr import ocr_data_to_blocks
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.core.pdf_io.extraction.shapes import PageAnalysis
from datagrunt.core.pdf_io.extraction.text_block_builder import TextBlockBuilder


class PdfiumBackend(ExtractionBackend):
    """Extraction via pypdfium2: text objects, images, and pdfium-rendered OCR."""

    def __init__(self, filepath):
        super().__init__(filepath)
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

    def analyze_page(self, page_number: int) -> PageAnalysis:
        """Return page metadata (raises if the page/file is invalid)."""
        doc, should_close = self._get_doc()
        try:
            with doc.page(page_number) as page:
                width, height = page.size()
                text = page.full_text().strip()
                text_objs, image_objs = page.count_objects()
                has_text = len(text) > 0
                return PageAnalysis(
                    width=float(width),
                    height=float(height),
                    rotation=page.rotation(),
                    has_text_layer=has_text,
                    is_scanned=(not has_text and image_objs > 0),
                    text_block_count=text_objs,
                    image_count=image_objs,
                    image_block_count=image_objs,
                    has_line_drawings=page.has_paths(),
                    text_length=len(text),
                )
        finally:
            if should_close:
                doc.close()

    def extract_page(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> tuple:
        """Parse the page once (single pdfium page/textpage open), returning
        analysis + text blocks + images, instead of opening the page three times.
        """
        doc, should_close = self._get_doc()
        try:
            with doc.page(page_number) as page:
                width, height = page.size()
                text = page.full_text().strip()
                text_objs, image_objs = page.count_objects()
                has_text = len(text) > 0
                analysis = PageAnalysis(
                    width=float(width),
                    height=float(height),
                    rotation=page.rotation(),
                    has_text_layer=has_text,
                    is_scanned=(not has_text and image_objs > 0),
                    text_block_count=text_objs,
                    image_count=image_objs,
                    image_block_count=image_objs,
                    has_line_drawings=page.has_paths(),
                    text_length=len(text),
                )
                text_blocks = []
                if has_text:
                    text_blocks = TextBlockBuilder().build(list(page.text_items()))
                images = []
                if image_objs > 0:
                    images = list(
                        page.image_items(
                            output_dir=output_dir, name_prefix=name_prefix, page_number=page_number
                        )
                    )
                return analysis, text_blocks, images
        finally:
            if should_close:
                doc.close()

    def extract_text_blocks(self, page_number: int) -> list:
        """Return classified text blocks built from pdfium text objects."""
        doc, should_close = self._get_doc()
        try:
            with doc.page(page_number) as page:
                items = list(page.text_items())
        finally:
            if should_close:
                doc.close()
        return TextBlockBuilder().build(items)

    def extract_images(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> list:
        """Return embedded images (>= MIN_IMAGE_DIMENSION); write when output_dir set."""
        doc, should_close = self._get_doc()
        try:
            with doc.page(page_number) as page:
                res = list(
                    page.image_items(
                        output_dir=output_dir, name_prefix=name_prefix, page_number=page_number
                    )
                )
        finally:
            if should_close:
                doc.close()
        return res

    def ocr_page(self, page_number: int, dpi: int = 300) -> list:
        """Render the page via pdfium and OCR it with Tesseract."""
        doc, should_close = self._get_doc()
        try:
            with doc.page(page_number) as page:
                img = page.render_pil(dpi=dpi)
        finally:
            if should_close:
                doc.close()
        return ocr_data_to_blocks(img, dpi)
