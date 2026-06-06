"""pdfium-backed extraction backend (structured/unified schema)."""

from datagrunt.core.pdf_io.extraction.base import ExtractionBackend
from datagrunt.core.pdf_io.extraction.ocr import ocr_data_to_blocks
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.core.pdf_io.extraction.shapes import PageAnalysis
from datagrunt.core.pdf_io.extraction.text_block_builder import TextBlockBuilder


class PdfiumBackend(ExtractionBackend):
    """Extraction via pypdfium2: text objects, images, and pdfium-rendered OCR."""

    def analyze_page(self, page_number: int) -> PageAnalysis:
        """Return page metadata (raises if the page/file is invalid)."""
        with PdfiumDocument(self.filepath) as doc:
            page = doc.page(page_number)
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

    def extract_text_blocks(self, page_number: int) -> list:
        """Return classified text blocks built from pdfium text objects."""
        with PdfiumDocument(self.filepath) as doc:
            items = list(doc.page(page_number).text_items())
        return TextBlockBuilder().build(items)

    def extract_images(self, page_number: int, output_dir: str = None, name_prefix: str = "page") -> list:
        """Return embedded images (>= MIN_IMAGE_DIMENSION); write when output_dir set."""
        with PdfiumDocument(self.filepath) as doc:
            return list(
                doc.page(page_number).image_items(
                    output_dir=output_dir, name_prefix=name_prefix, page_number=page_number
                )
            )

    def ocr_page(self, page_number: int, dpi: int = 300) -> list:
        """Render the page via pdfium and OCR it with Tesseract."""
        with PdfiumDocument(self.filepath) as doc:
            img = doc.page(page_number).render_pil(dpi=dpi)
        return ocr_data_to_blocks(img, dpi)
