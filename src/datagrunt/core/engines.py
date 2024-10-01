"""Module engines to enable data processing."""

# standard library

# third party libraries
import duckdb
import polars as pl

# local libraries
from .fileproperties import CSVProperties
from .queries import DuckDBQueries
from .logger import show_large_file_warning, show_dataframe_sample

class CSVReaderDuckDBEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name

    def _read_csv(self):
        """Reads a CSV using DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation.
        """
        return duckdb.read_csv(self.filepath,
                               delimiter=self.delimiter,
                               null_padding=True,
                               all_varchar=True
                            )

    def get_sample(self):
        """Return a sample of the CSV file."""
        self._read_csv().show()

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return self._read_csv().pl()

    def to_arrow_table(self):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        arrow_table = self._read_csv().arrow()
        return arrow_table

    def to_dicts(self):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe().to_dicts()
        return dicts

class CSVReaderPolarsEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by Polars."""

    def get_sample(self):
        """Return a sample of the CSV file."""
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         n_rows=self.DATAFRAME_SAMPLE_ROWS
                        )
        show_dataframe_sample(df)

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return pl.read_csv(self.filepath,
                           separator=self.delimiter,
                           truncate_ragged_lines=True
                           )

    def to_arrow_table(self):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        df = self.to_dataframe().to_arrow()
        return df

    def to_dicts(self):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe().to_dicts()
        return dicts

class CSVWriterDuckDBEngine(CSVProperties):
    """Class to convert CSV files to various other supported file types powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        super().__init__(filepath)
        self.queries = DuckDBQueries(self.filepath)
    
    def _set_out_filename(self, default_filename, out_filename=None):
        """Evaluate if a filename is passed in and if not, return default filename."""
        if out_filename:
            filename = out_filename
        else:
            filename = default_filename
        return filename

    def write_csv(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_csv_query(filename))

    def write_excel(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_excel_query(filename))

    def write_json(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_json_query(filename))

    def write_json_newline_delimited(self, out_filename=None):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_json_newline_delimited_query(filename))

    def write_parquet(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        duckdb.execute(self.queries.import_csv_query(self.delimiter))
        duckdb.execute(self.queries.export_parquet_query(filename))

class CSVWriterPolarsEngine(CSVProperties):
    """Class to write CSVs to other file formats powered by Polars."""

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
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_csv(filename)

    def write_excel(self, out_filename=None):
        """Export a Polars dataframe to an Excel file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_excel(filename)

    def write_json(self, out_filename=None):
        """Export a Polars dataframe to a JSON file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_json(filename)

    def write_json_newline_delimited(self, out_filename=None):
        """Export a Polars dataframe to a JSON newline delimited file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_ndjson(filename)

    def write_parquet(self, out_filename=None):
        """Export a Polars dataframe to a Parquet file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_parquet(filename)
