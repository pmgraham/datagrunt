"""Module to store database queries and query strings."""

from datagrunt.core.databases import DuckDBDatabase
import duckdb

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
        return f"SELECT * FROM {self.database_table_name}"

    def export_csv_query(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.
        
        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        return f"COPY {self.database_table_name} TO '{filename}' (HEADER, DELIMITER ',');"

    def export_json_query(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.
        
        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}' (ARRAY true) "

    def export_json_newline_delimited_query(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file with newline delimited.
        
        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'"

    def export_parquet_query(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.
        
        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT PARQUET)"

    def export_excel_query(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.
        
        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        return f"""
        INSTALL spatial;
        LOAD spatial;
        COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT GDAL, DRIVER 'xlsx')"""
    
    def write_csv(self, input_filepath, delimiter, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            input_filepath str: The path to the input file.
            delimiter str: The delimiter to use.
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(input_filepath, delimiter))
        duckdb.sql(self.export_csv_query(out_filename))
    
    def write_excel(self, input_filepath, delimiter, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            input_filepath str: The path to the input file.
            delimiter str: The delimiter to use.
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(input_filepath, delimiter))
        duckdb.sql(self.export_excel_query(out_filename))
    
    def write_json(self, input_filepath, delimiter, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            input_filepath str: The path to the input file.
            delimiter str: The delimiter to use.
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(input_filepath, delimiter))
        duckdb.sql(self.export_json_query(out_filename))
    
    def write_json_newline_delimited(self, input_filepath, delimiter, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            input_filepath str: The path to the input file.
            delimiter str: The delimiter to use.
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(input_filepath, delimiter))
        duckdb.sql(self.export_json_newline_delimited_query(out_filename))
    
    def write_parquet(self, input_filepath, delimiter, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

        Args:
            input_filepath str: The path to the input file.
            delimiter str: The delimiter to use.
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.import_csv_query(input_filepath, delimiter))
        duckdb.sql(self.export_parquet_query(out_filename))
