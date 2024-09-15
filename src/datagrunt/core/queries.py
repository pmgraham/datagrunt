"""Module to store database queries and query strings."""

# standard library

# third party libraries

# local libraries
from .databases import DuckDBDatabase
from .fileproperties import FileProperties

class DuckDBQueries(DuckDBDatabase):
    """Class to store DuckDB database queries and query strings."""

    def __init__(self, filepath):
        """
        Initialize the DuckDBQueries class.

        Args:
            filepath (str): Path to the file.
        """
        super().__init__(filepath)
        self.export_properties = FileProperties(self.filepath)

    def _set_out_filename(self, default_filename, out_filename=None):
        """Evaluate if a filename is passed in and if not, return default filename."""
        if out_filename:
            filename = out_filename
        else:
            filename = default_filename
        return filename

    def import_csv_query(self, delimiter):
        """Query to import a CSV file into a DuckDB table.

        Args:
            filepath str: Path to the file.
            delimiter str: The delimiter to use.
        """
        return f"""
            CREATE OR REPLACE TABLE {self.database_table_name} AS
            SELECT *
            FROM read_csv('{self.filepath}',
                            auto_detect=true,
                            delim='{delimiter}',
                            header=true,
                            null_padding=true,
                            all_varchar=True);
            """

    def select_from_duckdb_table(self):
        """Query to select from a DuckDB table."""
        return f"SELECT * FROM {self.database_table_name}"

    def export_csv_query(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            out_filename (str, optional): The name of the output file.
        """
        filename = self._set_out_filename(self.export_properties.CSV_OUT_FILENAME, out_filename)
        return f"COPY {self.database_table_name} TO '{filename}' (HEADER, DELIMITER ',');"

    def export_excel_query(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename (str, optional): The name of the output file.
        """
        filename = self._set_out_filename(self.export_properties.EXCEL_OUT_FILENAME, out_filename)
        return f"""
            INSTALL spatial;
            LOAD spatial;
            COPY (SELECT * FROM {self.database_table_name})
            TO '{filename}'(FORMAT GDAL, DRIVER 'xlsx')
        """

    def export_json_query(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename (str, optional): The name of the output file.
        """
        filename = self._set_out_filename(self.export_properties.JSON_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}' (ARRAY true) "

    def export_json_newline_delimited_query(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file with newline delimited.

        Args:
            out_filename (str, optional): The name of the output file.
        """
        filename = self._set_out_filename(self.export_properties.JSON_NEWLINE_OUT_FILENAME,
                                          out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'"

    def export_parquet_query(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename (str, optional): The name of the output file.
        """
        filename = self._set_out_filename(self.export_properties.PARQUET_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT PARQUET)"
