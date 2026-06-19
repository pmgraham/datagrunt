"""Module for writing parsed PDF output (JSON + image files)."""

# standard library
import json

# local libraries
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.pdf_api._engine_backed import _PDFEngineBacked


class PDFWriter(_PDFEngineBacked):
    """Class to unify the interface for writing parsed PDF output."""

    _engine_role = "writer"

    @staticmethod
    def _write_empty_file(filename, content=""):
        """Write ``content`` (default empty) to ``filename`` and return the path.

        Used to mirror the reader's empty-document handling: a 0-byte PDF yields
        an empty/minimal output file instead of leaking a raw PdfiumError from
        the engine.
        """
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        return filename

    def write_json(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write the unified document JSON to disk.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written there and referenced in the JSON.
            dedupe_images (bool, default True): When images are written, collapse
                byte-identical duplicates to a single file and repoint references.
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            str: The path of the written JSON file.
        """
        if self._parsed_dict is not None:
            filename = export_filename if export_filename else "output.json"
            document = self._parsed_dict
            if image_output_dir and dedupe_images:
                pdfcomponents.dedupe_document_images(document, image_output_dir)
            return pdfcomponents.write_document_json(document, filename)
        # Mirror PDFReader's is_empty handling: a 0-byte PDF produces an empty
        # document ({}) rather than a raw PdfiumError from loading the file.
        if self.is_empty:
            return self._write_empty_file(export_filename or "output.json", json.dumps({}))
        return self._engine.write_json(export_filename, image_output_dir, dedupe_images, drop_layout_tables)

    def write_json_newline_delimited(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
        """Parse the PDF and write one flattened element per line (JSONL).

        Args:
            export_filename (optional, str): Output path; defaults to output.jsonl.
            image_output_dir (optional, str): If provided, embedded images are
                written there.
            dedupe_images (bool, default True): When images are written, collapse
                byte-identical duplicates to a single file and repoint references.
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            str: The path of the written JSONL file.
        """
        if self._parsed_dict is not None:
            filename = export_filename if export_filename else "output.jsonl"
            document = self._parsed_dict
            if image_output_dir and dedupe_images:
                pdfcomponents.dedupe_document_images(document, image_output_dir)
            return pdfcomponents.write_document_jsonl(document, filename)
        # Mirror PDFReader's is_empty handling: a 0-byte PDF has no elements, so
        # the JSONL output is an empty file rather than a raw PdfiumError.
        if self.is_empty:
            return self._write_empty_file(export_filename or "output.jsonl")
        return self._engine.write_json_newline_delimited(
            export_filename, image_output_dir, dedupe_images, drop_layout_tables
        )

    def write_markdown(self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False):
        """Parse the PDF and write a formatted Markdown file to disk.

        Args:
            export_filename (optional, str): Output path; defaults to output.md.
            image_output_dir (optional, str): If provided, embedded images are
                written there and referenced in the Markdown.
            dedupe_images (bool, default True): When images are written, collapse
                byte-identical duplicates to a single file and repoint references.
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            str: The path of the written Markdown file.
        """
        if self._parsed_dict is not None:
            filename = export_filename if export_filename else "output.md"
            document = self._parsed_dict
            if image_output_dir and dedupe_images:
                pdfcomponents.dedupe_document_images(document, image_output_dir)
            return pdfcomponents.write_document_markdown(document, filename)
        # Mirror PDFReader's is_empty handling: a 0-byte PDF has no content, so
        # the Markdown output is an empty file rather than a raw PdfiumError.
        if self.is_empty:
            return self._write_empty_file(export_filename or "output.md")
        return self._engine.write_markdown(export_filename, image_output_dir, dedupe_images, drop_layout_tables)

    def extract_images(self, output_dir=None, dedupe=True):
        """Parse the PDF and write embedded image files to disk.

        Args:
            output_dir (optional, str): Output directory; defaults to
                output_images.
            dedupe (bool, default True): Collapse byte-identical duplicate images
                to a single file before returning paths.

        Returns:
            list: Paths of the written image files.
        """
        if self._parsed_dict is not None:
            document = self._parsed_dict
            image_dir = output_dir if output_dir else "output_images"
            if dedupe:
                pdfcomponents.dedupe_document_images(document, image_dir)
            return pdfcomponents.collect_image_paths(document)
        # Mirror PDFReader's is_empty handling: a 0-byte PDF has no images.
        if self.is_empty:
            return []
        return self._engine.extract_images(output_dir, dedupe)
