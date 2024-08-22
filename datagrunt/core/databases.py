"""Module for interfacing with databases."""

# standard library
from pathlib import Path

# third party libraries
import duckdb

# local libraries

class DuckDBDatabase:
    """Class to configure local database for file processing.
       Utilizes duckdb as the processing engine.
    """
    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_THREAD_COUNT = 16
    JSON_OUT_FILENAME = 'output.json'
    JSON_NEWLINE_OUT_FILENAME = 'output.jsonl'

    def __init__(self, filepath):
        """
        Initialize the FileDatabase class.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath
        self.database_filename = self._set_database_filename()
        self.database_table_name = self._set_database_table_name()

    def _set_database_filename(self):
        """Return name of duckdb file created at runtime."""
        return f'{Path(self.filepath).stem}.db'

    def _set_database_table_name(self):
        """Return name of duckdb import table created during file import."""
        return f'{Path(self.filepath).stem}'

    @property
    def set_database_connection(self, threads=DEFAULT_THREAD_COUNT):
        """Establish a connection with duckdb.

        Args:
            threads (int): Number of threads to use for duckdb.
        """
        return duckdb.connect(self.database_filename, config = {'threads': threads})

    def select_from_table_statement(self):
        """Select from table."""
        return f"SELECT * FROM {self.database_table_name}"

    def export_to_json_array_statement(self):
        """Export SQL statement to export data as a JSON file."""
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{self.JSON_OUT_FILENAME}' (ARRAY true) "

    def export_to_json_new_line_delimited_statement(self):
        """Export SQL statement to export data as a JSON newline delimited file."""
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{self.JSON_NEWLINE_OUT_FILENAME}'"

    def export_to_parquet_statement(self):
        """Export SQL statement to export data as a parquet file."""
        return f"COPY (SELECT * FROM {self.database_table_name}) TO 'output.parquet'(FORMAT PARQUET)"

    def export_to_excel_statement(self):
        """Export SQL statement to export data as an Excel file."""
        return f"""
        INSTALL spatial;
        LOAD spatial;
        COPY (SELECT * FROM {self.database_table_name}) TO 'output.xlsx'(FORMAT GDAL, DRIVER 'xlsx')"""

    def to_dataframe(self, sql_import_statement):
        """Export data as Polars a dataframe.

        Args:
            sql_import_statement (str): SQL statement to import data.

        Return:
            Polars dataframe.
        """
        with self.set_database_connection as con:
            con.sql(sql_import_statement)
            return con.query(self.select_from_table_statement()).pl()

    def to_json(self, sql_import_statement):
        """Export data as a JSON file array.

        Args:
            sql_import_statement (str): SQL statement to import data.
        """
        with self.set_database_connection as con:
            con.sql(sql_import_statement)
            con.sql(self.export_to_json_array_statement())

    def to_json_newline_delimited(self, sql_import_statement):
        """Export data as a JSON newline delimited file.

        Args:
            sql_import_statement (str): SQL statement to import data.
        """
        with self.set_database_connection as con:
            con.sql(sql_import_statement)
            con.sql(self.export_to_json_new_line_delimited_statement())

    def to_parquet(self, sql_import_statement):
        """Export data as a parquet file.

        Args:
            sql_import_statement (str): SQL statement to import data.
        """
        with self.set_database_connection as con:
            con.sql(sql_import_statement)
            con.sql(self.export_to_parquet_statement())

    def to_excel(self, sql_import_statement):
        """Export data as an Excel file.

        Args:
            sql_import_statement (str): SQL statement to import data.
        """
        with self.set_database_connection as con:
            con.sql(sql_import_statement)
            con.sql(self.export_to_excel_statement())
