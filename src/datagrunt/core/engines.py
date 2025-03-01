"""Module engines to enable data processing."""

# standard library

# third party libraries
import duckdb
import polars as pl

# local libraries
from src.datagrunt.core.csvproperties import CSVProperties
from src.datagrunt.core.queries import DuckDBQueries

class CSVReaderDuckDBEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.queries = DuckDBQueries(self.filepath)

    @property
    def db_table(self):
        """Return the DuckDB table."""
        return self.queries.database_table_name

    def create_table(self, normalize_columns=False):
        """Create a DuckDB table from the CSV file."""
        if normalize_columns:
            duckdb.sql(self.queries.import_csv_query_normalize_columns())
        else:
            duckdb.sql(self.queries.import_csv_query())
        return duckdb.sql(self.queries.select_from_duckdb_table()).execute()

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        self.create_table(normalize_columns).show()

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return self.create_table(normalize_columns).pl()

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        return self.create_table(normalize_columns).arrow()

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe(normalize_columns).to_dicts()
        return dicts

class CSVReaderPolarsEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by Polars."""

    def _create_dataframe(self, normalize_columns=False):
        """Normalizes the column names of the dataframe."""
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         infer_schema=False
                        )
        if normalize_columns:
            df = df.rename(self.columns_to_normalized_mapping)
        return df

    def _create_dataframe_sample(self, normalize_columns=False):
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         infer_schema=False,
                         n_rows=self.DATAFRAME_SAMPLE_ROWS
                        )
        if normalize_columns:
            df = df.rename(self.columns_to_normalized_mapping)
        return df

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        # show_dataframe_sample(self._create_dataframe_sample(normalize_columns))
        df = self._create_dataframe_sample(normalize_columns)
        print(df)

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        return self._create_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        df = self._create_dataframe(normalize_columns).to_arrow()
        return df

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self._create_dataframe(normalize_columns).to_dicts()
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

    @property
    def db_table(self):
        """Return the DuckDB table."""
        return self.queries.database_table_name

    def _set_out_filename(self, default_filename, out_filename=None):
        """Evaluate if a filename is passed in and if not, return default filename

           Args:
               default_filename (str): The default filename.
               out_filename (str): The name of the output file.

            Returns:
                str: The output filename.
        """
        if out_filename:
            filename = out_filename
        else:
            filename = default_filename
        return filename

    def write_csv(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        CSVReaderDuckDBEngine(self.filepath).create_table(normalize_columns)
        duckdb.sql(self.queries.export_csv_query(filename))

    def write_excel(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        CSVReaderDuckDBEngine(self.filepath).create_table(normalize_columns)
        duckdb.sql(self.queries.export_excel_query(filename))

    def write_json(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        CSVReaderDuckDBEngine(self.filepath).create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_query(filename))

    def write_json_newline_delimited(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        CSVReaderDuckDBEngine(self.filepath).create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_newline_delimited_query(filename))

    def write_parquet(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        CSVReaderDuckDBEngine(self.filepath).create_table(normalize_columns)
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

    def write_csv(self, out_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a CSV file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_csv(filename)

    def write_excel(self, out_filename=None, normalize_columns=False):
        """Export a Polars dataframe to an Excel file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_excel(filename)

    def write_json(self, out_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a JSON file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_json(filename)

    def write_json_newline_delimited(self, out_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a JSON newline delimited file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_ndjson(filename)

    def write_parquet(self, out_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a Parquet file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_parquet(filename)
