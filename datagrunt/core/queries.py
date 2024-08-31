"""Module to store database queries and query strings."""

from datagrunt.core.databases import DuckDBDatabase

class DuckDBQueries(DuckDBDatabase):
    """Class to store DuckDB database queries and query strings."""

    JSON_OUT_FILENAME = 'output.json'
    JSON_NEWLINE_OUT_FILENAME = 'output.jsonl'
    CSV_OUT_FILENAME = 'output.csv'
    EXCEL_OUT_FILENAME = 'output.xlsx'
    PARQUET_OUT_FILENAME = 'output.parquet'
    AVRO_OUT_FILENAME = 'output.avro'

    def __init__(self, filepath):
        """
        Initialize the DuckDBQueries class.

        Args:
            filepath (str): Path to the file.
        """
        super().__init__(filepath)

    def _set_out_filename(self, default_filename, out_filename=None):
        """Evaluate if a filename is passed in and if not, return default filename."""
        if out_filename:
            filename = out_filename
        else:
            filename = default_filename
        return filename

    def export_csv_query(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file."""
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        return f"COPY {self.database_table_name} TO '{filename}' (HEADER, DELIMITER ',');"

    def export_json_query(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file."""
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}' (ARRAY true) "

    def export_json_newline_delimited_query(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file with newline delimited."""
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'"

    def export_parquet_query(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file."""
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT PARQUET)"

    def export_excel_query(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file."""
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        return f"""
        INSTALL spatial;
        LOAD spatial;
        COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT GDAL, DRIVER 'xlsx')"""
