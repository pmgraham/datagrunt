"""Module for writing parsed PDF output (JSON + image files)."""

# standard library
import json
from functools import cached_property
from pathlib import Path

# local libraries
from datagrunt.core import PDFComponents, PDFEngineFactory
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.core.pdf_io.extraction import PdfiumNativeReader


class PDFWriter(PDFComponents):
    """Class to unify the interface for writing parsed PDF output."""

    def __init__(self, filepath, engine="pdfium", workers=1, native=False):
        """Initialize the PDF Writer class.

        Args:
            filepath (str, Path, or dict): Path to the PDF/JSON file to parse, or parsed document dict.
            engine (str, default 'pdfium'): Parsing engine to instantiate.
                One of 'pdfium' (default -- permissive license; emits the unified
                element schema by default, or the lean native schema when
                ``native=True``) or 'pymupdf' (unified element schema, tables +
                OCR).
            workers (int, default 1): Number of concurrent per-page workers.
            native (bool, default False): pdfium only -- when True, emit the lean
                native schema (text, positioned text objects, images; no table
                detection) instead of the default unified element schema. Ignored
                by the pymupdf engine.
        """
        if not isinstance(filepath, dict):
            filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers
        self.native = native

    def _create_writer(self):
        """Create a writer engine instance."""
        return PDFEngineFactory(self.filepath, self.engine, self.workers, structured=not self.native).create_writer()

    @cached_property
    def _writer(self):
        """Cached writer engine; reused across write_* calls on this instance."""
        return self._create_writer()

    @staticmethod
    def _write_empty_file(filename, content=""):
        """Write ``content`` (default empty) to ``filename`` and return the path.

        Used to mirror the reader's empty-document handling: a 0-byte PDF yields
        an empty/minimal output file instead of leaking a raw PdfiumError from
        the engine.
        """
        with open(filename, "w") as f:
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
                is_structured = any("elements" in pg for pg in document.get("document", {}).get("pages", []))
                if is_structured:
                    pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
                else:
                    PdfiumNativeReader.dedupe_images(document, image_output_dir=image_output_dir)
            with open(filename, "w") as f:
                json.dump(document, f, indent=2)
            return filename
        # Mirror PDFReader's is_empty handling: a 0-byte PDF produces an empty
        # document ({}) rather than a raw PdfiumError from loading the file.
        if self.is_empty:
            return self._write_empty_file(export_filename or "output.json", json.dumps({}))
        return self._writer.write_json(export_filename, image_output_dir, dedupe_images, drop_layout_tables)

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
            is_structured = any("elements" in pg for pg in document.get("document", {}).get("pages", []))
            if image_output_dir and dedupe_images:
                if is_structured:
                    pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
                else:
                    PdfiumNativeReader.dedupe_images(document, image_output_dir=image_output_dir)
            if is_structured:
                records = pdfcomponents.ParsedDocument(document).flatten()
            else:
                records = PdfiumNativeReader.flatten(document)
            with open(filename, "w") as f:
                for record in records:
                    f.write(json.dumps(record) + "\n")
            return filename
        # Mirror PDFReader's is_empty handling: a 0-byte PDF has no elements, so
        # the JSONL output is an empty file rather than a raw PdfiumError.
        if self.is_empty:
            return self._write_empty_file(export_filename or "output.jsonl")
        return self._writer.write_json_newline_delimited(
            export_filename, image_output_dir, dedupe_images, drop_layout_tables
        )

    def write_markdown(
        self, export_filename=None, image_output_dir=None, dedupe_images=True, drop_layout_tables=False
    ):
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
            is_structured = any("elements" in pg for pg in document.get("document", {}).get("pages", []))
            if image_output_dir and dedupe_images:
                if is_structured:
                    pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
                else:
                    PdfiumNativeReader.dedupe_images(document, image_output_dir=image_output_dir)
            if is_structured:
                markdown_text = pdfcomponents.ParsedDocument(document).to_markdown(export_filename=filename)
            else:
                markdown_text = PdfiumNativeReader.to_markdown(document)
            with open(filename, "w") as f:
                f.write(markdown_text)
            return filename
        # Mirror PDFReader's is_empty handling: a 0-byte PDF has no content, so
        # the Markdown output is an empty file rather than a raw PdfiumError.
        if self.is_empty:
            return self._write_empty_file(export_filename or "output.md")
        return self._writer.write_markdown(
            export_filename, image_output_dir, dedupe_images, drop_layout_tables
        )

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
            is_structured = any("elements" in pg for pg in document.get("document", {}).get("pages", []))
            if dedupe:
                if is_structured:
                    pdfcomponents.ParsedDocument(document).dedupe_images(image_output_dir=image_dir)
                else:
                    PdfiumNativeReader.dedupe_images(document, image_output_dir=image_dir)
            paths = []
            seen = set()
            for page in document.get("document", {}).get("pages", []):
                if is_structured:
                    candidates = [
                        (el.get("metadata") or {}).get("file_path")
                        for el in page.get("elements", [])
                        if el.get("type") == "image"
                    ]
                else:
                    candidates = [img.get("file") for img in page.get("images", [])]
                for fp in candidates:
                    if fp and fp not in seen:
                        seen.add(fp)
                        paths.append(fp)
            return paths
        # Mirror PDFReader's is_empty handling: a 0-byte PDF has no images.
        if self.is_empty:
            return []
        return self._writer.extract_images(output_dir, dedupe)
