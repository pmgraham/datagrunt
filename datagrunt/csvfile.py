"""Module for reading CSV files and converting CSV files to different standard file formats."""

# standard library

# third party libraries
import duckdb
import polars as pl

# local libraries
from datagrunt.core.fileproperties import CSVProperties
from datagrunt.core.queries import DuckDBQueries
from datagrunt.core.logger import *

class CSVReaderDuckDBEngine(CSVProperties):

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

class CSVReaderPolarsEngine(CSVProperties):

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)

    def get_sample(self):
        """Return a sample of the CSV file."""
        return pl.read_csv(self.filepath,
                           separator=self.delimiter,
                           n_rows=self.DATAFRAME_SAMPLE_ROWS
                           )

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return pl.read_csv(self.filepath, separator=self.delimiter)

    def to_arrow_table(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A PyArrow table.
        """
        df = self.to_dataframe().to_arrow()
        return df

    def to_dicts(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe().to_dicts()
        return dicts

class CSVWriterDuckDBEngine(CSVProperties):
    """Class to convert CSV files to various other supported file types."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        super().__init__(filepath)
        self.queries = DuckDBQueries(self.filepath)

    def write_csv(self, out_filename=None):
            """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
            duckdb.sql(self.queries.import_csv_query(self.delimiter))
            duckdb.sql(self.queries.export_csv_query(out_filename))

    def write_excel(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_excel_query(out_filename))

    def write_json(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_json_query(out_filename))

    def write_json_newline_delimited(self, out_filename=None):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_json_newline_delimited_query(out_filename))

    def write_parquet(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.execute(self.queries.import_csv_query(self.delimiter))
        duckdb.execute(self.queries.export_parquet_query(out_filename))

class CSVWriterPolarsEngine(CSVProperties):
    """Class to write CSVs to other file formats powered by Polars."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        super().__init__(filepath)

    def _set_out_filename(self, default_filename, out_filename=None):
        """Evaluate if a filename is passed in and if not, return default filename."""
        if out_filename:
            filename = out_filename
        else:
            filename = default_filename
        return filename

    def write_csv(self, out_filename=None):
        """Export a Polars dataframe to a CSV file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_csv(filename)

    def write_excel(self, out_filename=None):
        """Export a Polars dataframe to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_excel(filename)

    def write_json(self, out_filename=None):
        """Export a Polars dataframe to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_json(filename)

    def write_json_newline_delimited(self, out_filename=None):
        """Export a Polars dataframe to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_ndjson(filename)

    def write_parquet(self, out_filename=None):
        """Export a Polars dataframe to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_parquet(filename)
