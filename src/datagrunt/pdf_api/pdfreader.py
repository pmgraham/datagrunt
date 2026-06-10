"""Module for reading PDF files and converting to in-memory Python objects."""

# standard library
from pathlib import Path

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core import PDFComponents, PDFEngineFactory
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.core.pdf_io.extraction import PdfiumNativeReader


class PDFReader(PDFComponents):
    """Class to unify the interface for reading and parsing PDF files."""

    def __init__(self, filepath, engine="pdfium", workers=1, native=False):
        """Initialize the PDF Reader class.

        Args:
            filepath (str, Path, or dict): Path to the PDF/JSON file to read, or parsed document dict.
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

    def _return_empty_file_object(self, object):
        """Return an empty object of the specified type."""
        return object

    def _create_reader(self):
        """Create a reader engine instance."""
        return PDFEngineFactory(self.filepath, self.engine, self.workers, structured=not self.native).create_reader()

    def get_sample(self):
        """Parse and return the first page of the PDF."""
        if self._parsed_dict is not None:
            pages = self._parsed_dict.get("document", {}).get("pages", [])
            return pages[0] if pages else {}
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._create_reader().get_sample()

    def to_dicts(self, image_output_dir=None, drop_layout_tables=False):
        """Parse the PDF into the unified document dict.

        Args:
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the result; otherwise image
                ``file_path`` values are null.
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            dict: ``{"document": {... "pages": [...]}}``.
        """
        if self._parsed_dict is not None:
            return self._parsed_dict
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._create_reader().to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)

    def to_dataframe(self, drop_layout_tables=False):
        """Parse the PDF and flatten elements into a Polars DataFrame.

        Args:
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            A Polars DataFrame with one row per extracted element.
        """
        if self._parsed_dict is not None:
            is_structured = any("elements" in pg for pg in self._parsed_dict.get("document", {}).get("pages", []))
            if is_structured:
                records = pdfcomponents.ParsedDocument(self._parsed_dict).flatten()
            else:
                records = PdfiumNativeReader.flatten(self._parsed_dict)
            if not records:
                return pl.DataFrame()
            return pl.DataFrame(records)
        if self.is_empty:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().to_dataframe(drop_layout_tables=drop_layout_tables)

    def to_arrow_table(self, drop_layout_tables=False):
        """Parse the PDF and flatten elements into a PyArrow table.

        Args:
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            A PyArrow table with one row per extracted element.
        """
        if self._parsed_dict is not None:
            is_structured = any("elements" in pg for pg in self._parsed_dict.get("document", {}).get("pages", []))
            if is_structured:
                records = pdfcomponents.ParsedDocument(self._parsed_dict).flatten()
            else:
                records = PdfiumNativeReader.flatten(self._parsed_dict)
            if not records:
                return pa.Table.from_pydict({})
            return pa.Table.from_pylist(records)
        if self.is_empty:
            return self._return_empty_file_object(pa.Table.from_pydict({}))
        return self._create_reader().to_arrow_table(drop_layout_tables=drop_layout_tables)
