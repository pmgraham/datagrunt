"""Module for reading PDF files and converting to in-memory Python objects."""

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.pdf_api._engine_backed import _PDFEngineBacked


class PDFReader(_PDFEngineBacked):
    """Class to unify the interface for reading and parsing PDF files.

    Pass ``min_image_dimension`` (keyword-only) to control the minimum embedded
    image pixel size kept during extraction (default 40; 0 keeps everything).
    """

    _engine_role = "reader"

    def _return_empty_file_object(self, object):
        """Return an empty object of the specified type."""
        return object

    def get_sample(self):
        """Parse and return the first page of the PDF."""
        if self._parsed_dict is not None:
            pages = self._parsed_dict.get("document", {}).get("pages", [])
            return pages[0] if pages else {}
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._engine.get_sample()

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
        return self._engine.to_dicts(image_output_dir=image_output_dir, drop_layout_tables=drop_layout_tables)

    def to_dataframe(self, drop_layout_tables=False):
        """Parse the PDF and flatten elements into a Polars DataFrame.

        Args:
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            A Polars DataFrame with one row per extracted element.
        """
        if self._parsed_dict is not None:
            records = pdfcomponents.flatten_document(self._parsed_dict)
            return pl.DataFrame(records) if records else pl.DataFrame()
        if self.is_empty:
            return self._return_empty_file_object(pl.DataFrame())
        return self._engine.to_dataframe(drop_layout_tables=drop_layout_tables)

    def to_arrow_table(self, drop_layout_tables=False):
        """Parse the PDF and flatten elements into a PyArrow table.

        Args:
            drop_layout_tables (bool, default False): Drop 1xN / Nx1 "tables"
                that are layout boxes rather than real tabular data.

        Returns:
            A PyArrow table with one row per extracted element.
        """
        if self._parsed_dict is not None:
            records = pdfcomponents.flatten_document(self._parsed_dict)
            return pa.Table.from_pylist(records) if records else pa.Table.from_pydict({})
        if self.is_empty:
            return self._return_empty_file_object(pa.Table.from_pydict({}))
        return self._engine.to_arrow_table(drop_layout_tables=drop_layout_tables)
