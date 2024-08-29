"""Module for interfacing with databases."""

# standard library
import os
from pathlib import Path

# third party libraries
import duckdb

class DuckDBDatabase:
    """Class to configure local database for file processing.
       Utilizes duckdb as the processing engine.
    """
    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_THREAD_COUNT = 16
    JSON_OUT_FILENAME = 'output.json'
    JSON_NEWLINE_OUT_FILENAME = 'output.jsonl'
    CSV_OUT_FILENAME = 'output.csv'
    EXCEL_OUT_FILENAME = 'output.xlsx'
    PARQUET_OUT_FILENAME = 'output.parquet'
    AVRO_OUT_FILENAME = 'output.avro'

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
        """Delete .db files when exiting the context manager or when connection closes."""
        self.database_connection.close()
        if os.path.exists(self.database_filename):
            os.remove(self.database_filename)

    def _set_database_filename(self):
        """Return name of duckdb file created at runtime."""
        return f'{Path(self.filepath).stem}.db'

    def _set_database_table_name(self):
        """Return name of duckdb import table created during file import."""
        return f'{Path(self.filepath).stem}'

    def _set_database_connection(self, threads=DEFAULT_THREAD_COUNT):
        """Establish a connection with duckdb.

        Args:
            threads (int): Number of threads to use for duckdb.
        """
        return duckdb.connect(self.database_filename, config = {'threads': threads})

    def write_to_file(self, sql_import_statement, sql_export_statement):
        """Write database content to file.

        Args:
            sql_import_statement (str): SQL statement to import data.
            sql_export_statement (str): SQL statement to export data.
        """
        con = self.database_connection
        con.sql(sql_import_statement)
        con.sql(sql_export_statement)

    def to_dataframe(self, sql_import_statement):
        """Export data as Polars a dataframe.

        Args:
            sql_import_statement (str): SQL statement to import data.

        Return:
            Polars dataframe.
        """
        con = self.database_connection
        con.sql(sql_import_statement)
        return con.query(self.select_from_table_statement()).pl()
