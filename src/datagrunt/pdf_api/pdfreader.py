"""Module for reading PDF files and converting to in-memory Python objects."""

# standard library
from pathlib import Path

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core import PDFComponents, PDFEngineFactory


class PDFReader(PDFComponents):
    """Class to unify the interface for reading and parsing PDF files."""

    def __init__(self, filepath, engine="pymupdf", workers=4):
        """Initialize the PDF Reader class.

        Args:
            filepath (str or Path): Path to the PDF file to read.
            engine (str, default 'pymupdf'): Parsing engine to instantiate.
            workers (int, default 4): Number of concurrent per-page workers.
        """
        filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers

    def _return_empty_file_object(self, object):
        """Return an empty object of the specified type."""
        return object

    def _create_reader(self):
        """Create a reader engine instance."""
        return PDFEngineFactory(self.filepath, self.engine, self.workers).create_reader()

    def get_sample(self):
        """Parse and return the first page of the PDF."""
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._create_reader().get_sample()

    def to_dicts(self, image_output_dir=None):
        """Parse the PDF into the unified document dict.

        Args:
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the result; otherwise image
                ``file_path`` values are null.

        Returns:
            dict: ``{"document": {... "pages": [...]}}``.
        """
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._create_reader().to_dicts(image_output_dir=image_output_dir)

    def to_dataframe(self):
        """Parse the PDF and flatten elements into a Polars DataFrame.

        Returns:
            A Polars DataFrame with one row per extracted element.
        """
        if self.is_empty:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().to_dataframe()

    def to_arrow_table(self):
        """Parse the PDF and flatten elements into a PyArrow table.

        Returns:
            A PyArrow table with one row per extracted element.
        """
        if self.is_empty:
            return self._return_empty_file_object(pa.Table.from_pydict({}))
        return self._create_reader().to_arrow_table()
