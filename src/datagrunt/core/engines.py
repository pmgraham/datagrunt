"""Module engines to enable data processing."""

# standard library

# third party libraries
import duckdb
import polars as pl

# local libraries
from src.datagrunt.core.csvproperties import CSVProperties
from src.datagrunt.core.queries import DuckDBQueries
from src.datagrunt.core.logger import show_large_file_warning

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

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        self.queries.create_table(normalize_columns).show()

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return self.queries.create_table(normalize_columns).pl()

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        return self.queries.create_table(normalize_columns).arrow()

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe(normalize_columns).to_dicts()
        return dicts

    def query_data(self, sql_query, normalize_columns=False):
            """Queries as CSV file after importing into DuckDB.

            Args:
                sql_query (str): Query to run against DuckDB.

            Returns:
                A DuckDB DuckDBPyRelation with the query results.

            Example if DuckDB Engine:
                dg = CSVReader('myfile.csv')
                query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
                dg.query_csv_data(query)
            """
            self.queries.create_table(normalize_columns)
            return duckdb.sql(sql_query)

class CSVReaderPolarsEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by Polars."""
    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.queries = DuckDBQueries(self.filepath)
        self.db_engine = CSVReaderDuckDBEngine(self.filepath)

    @property
    def db_table(self):
        """Return the DuckDB table."""
        return self.queries.database_table_name

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

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)
        """
        return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)

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
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_csv_query(filename))

    def write_excel(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_excel_query(filename))

    def write_json(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_query(filename))

    def write_json_newline_delimited(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_newline_delimited_query(filename))

    def write_parquet(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename (optional, str): The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_parquet_query(filename))

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
