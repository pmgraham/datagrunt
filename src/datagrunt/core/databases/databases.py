"""Module for interfacing with databases."""

# standard library
import os
import re
from pathlib import Path

# third party libraries
import duckdb

# local imports
from datagrunt.core.csv_io import CSVColumnNameNormalizer, CSVDelimiter


class DuckDBDatabase:
    """Class to configure local database for file processing.
    Utilizes duckdb as the processing engine.
    """

    DEFAULT_ENCODING = "utf-8"
    DEFAULT_THREAD_COUNT = 16

    def __init__(self, filepath):
        """
        Initialize the FileDatabase class.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath
        self.database_filename = self._set_database_filename()
        self.database_table_name = self._set_database_table_name()
        self.database_connection = self._set_database_connection()

    def __del__(self):
        """
        Close the database connection and delete .db files after use.
        """
        self.database_connection.close()
        if os.path.exists(self.database_filename):
            os.remove(self.database_filename)

    def _format_filename_string(self):
        """Remove all non alphanumeric characters from filename."""
        return re.sub(r"[^a-zA-Z0-9]", "", Path(self.filepath).stem)

    def _set_database_filename(self):
        """Return name of duckdb file created at runtime."""
        return f"{self._format_filename_string()}.db"

    def _set_database_table_name(self):
        """
        Return name of duckdb import table created during file import.
        """
        return f"{self._format_filename_string()}"

    def _set_database_connection(self, threads=DEFAULT_THREAD_COUNT):
        """Establish a connection with duckdb.

        Args:
            threads (int): Number of threads to use for duckdb.
        """
        return duckdb.connect(self.database_filename, config={"threads": threads})


class DuckDBQueries:
    """Class to store DuckDB database queries and query strings."""

    def __init__(self, filepath):
        """
        Initialize the DuckDBQueries class.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath
        self.delimiter = CSVDelimiter(filepath).delimiter
        self.database_table_name = DuckDBDatabase(filepath).database_table_name

    def set_export_filename(self, default_filename, export_filename=None):
        """
        Return the export filename if provided, otherwise return the default.
        """
        # This method doesn't really fit the class but it's the only place it's
        # relevant.
        if export_filename:
            filename = export_filename
        else:
            filename = default_filename
        return filename

    def import_csv_query(self):
        """Query to import a CSV file into a DuckDB table.

        Returns:
            str: The query to import the CSV file.
        """
        return f"""
            CREATE OR REPLACE TABLE {self.database_table_name} AS
            SELECT *
            FROM read_csv('{self.filepath}',
                            auto_detect=true,
                            delim='{self.delimiter}',
                            header=true,
                            null_padding=true,
                            all_varchar=True,
                            strict_mode=false);
            """

    def import_csv_query_normalize_columns(self):
        """
        Query to import a CSV file into a DuckDB table and normalize column
        names.

        Returns:
            str: The query to import the CSV file and normalize column names.
        """
        return f"""
            CREATE OR REPLACE TABLE {self.database_table_name} AS
            SELECT *
            FROM read_csv('{self.filepath}',
                            auto_detect=true,
                            delim='{self.delimiter}',
                            header=true,
                            null_padding=true,
                            all_varchar=True,
                            strict_mode=false,
                            normalize_names=true);
            """

    def select_from_duckdb_table(self):
        """Query to select from a DuckDB table."""
        return f"SELECT * FROM {self.database_table_name}"

    def export_csv_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a CSV file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a CSV file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY {self.database_table_name} TO '{filename}' (HEADER, DELIMITER ',');"  # noqa: E501

    def export_excel_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to an Excel file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to an Excel file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"""
            INSTALL spatial;
            LOAD spatial;
            COPY (SELECT * FROM {self.database_table_name})
            TO '{filename}'(FORMAT GDAL, DRIVER 'xlsx')
        """

    def export_json_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a JSON file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a JSON file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}' (ARRAY true)"  # noqa: E501

    def export_json_newline_delimited_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a JSON file with newline delimited.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a JSON file with newline
            delimited.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'"  # noqa: E501

    def export_parquet_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a Parquet file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a Parquet file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT PARQUET)"  # noqa: E501

    def update_and_normalize_column_names(self):
        """Query to update column names in a DuckDB table.

        DuckDB has a built-in function to normalize column names. However,
        the format of the column names from the native DuckDB function often
        differs from the class CSVColumnNameNormalizer. Because other types
        of engines throughout the ecosystem may have different conventions,
        this method uses the CSVColumnNameNormalizer class to ensure
        consistent naming conventions across different processing engines.
        """
        duckdb.sql(self.import_csv_query())
        table_columns = duckdb.sql(f"SELECT * FROM {self.database_table_name} LIMIT 0").columns
        for old_name, new_name in zip(table_columns, CSVColumnNameNormalizer(self.filepath).columns_normalized):
            sql_string = f'ALTER TABLE {self.database_table_name} RENAME COLUMN "{old_name}" TO "{new_name}"'
            duckdb.sql(sql_string)

    def create_table(self, normalize_columns=False):
        """Create a DuckDB table from the CSV file.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        if normalize_columns:
            self.update_and_normalize_column_names()
        else:
            duckdb.sql(self.import_csv_query())
        return duckdb.sql(self.select_from_duckdb_table()).execute()

    def _normalize_dataframe_columns(self, dataframe):
        """Applies column name normalization to a Polars DataFrame.
        Args:
            dataframe (polars.DataFrame): The DataFrame to normalize.
        Returns:
            polars.DataFrame: The normalized DataFrame.
        """
        column_normalizer = CSVColumnNameNormalizer(self.filepath)
        normalized_mapping = {
            col: column_normalizer.columns_to_normalized_mapping.get(col, col) for col in dataframe.columns
        }
        return dataframe.rename(normalized_mapping)

    def sql_query_to_dataframe(self, sql_query, normalize_columns=False):
        """
        Query to convert a SQL query to a Polars DataFrame.

        Args:
            sql_query (str): The SQL query to execute.
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            polars.DataFrame: The resulting DataFrame.
        """
        # Ensure the table is created with original column names for querying
        duckdb.sql(self.import_csv_query())

        # Execute the user's query
        result_df = duckdb.sql(sql_query).pl()

        if normalize_columns:
            result_df = self._normalize_dataframe_columns(result_df)

        return result_df
