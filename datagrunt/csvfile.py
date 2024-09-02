"""Module for defining properties, reading, and writing CSV files."""

# standard library
from collections import Counter
import csv
import re

# third party libraries
import duckdb
import polars as pl
# from pyarrow import csv

# local libraries
from datagrunt.core.fileproperties import CSVProperties
from datagrunt.core.queries import DuckDBQueries
from datagrunt.core.logger import *

class CSVReader(CSVProperties):

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)

    def get_sample(self):
        """Return a sample of the CSV file."""
        duckdb.read_csv(self.filepath, delimiter=self.delimiter).show()

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return duckdb.read_csv(self.filepath,
                               delimiter=self.delimiter,
                               all_varchar=True
                                ).pl()

    def to_arrow_table(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A PyArrow table.
        """
        arrow_table = duckdb.read_csv(self.filepath,
                                      delimiter=self.delimiter,
                                      all_varchar=True
                                      ).arrow()
        return arrow_table

    def to_dicts(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe().to_dicts()
        return dicts


class CSVConverterDuckDBEngine(DuckDBQueries):
    """Class to convert CSV files to various other supported file types."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        super().__init__(filepath)
        self.delimiter = CSVReader(self.filepath).delimiter

    def write_csv(self, out_filename=None):
            """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
            duckdb.sql(self.import_csv_query(self.delimiter))
            duckdb.sql(self.export_csv_query(out_filename))

    def write_excel(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(self.delimiter))
        duckdb.sql(self.export_excel_query(out_filename))

    def write_json(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(self.delimiter))
        duckdb.sql(self.export_json_query(out_filename))

    def write_json_newline_delimited(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(self.delimiter))
        duckdb.sql(self.export_json_newline_delimited_query(out_filename))

    def write_parquet(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(self.delimiter))
        duckdb.sql(self.export_parquet_query(out_filename))
