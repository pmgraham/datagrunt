"""Module to store database queries and query strings."""

# standard library

# third party libraries
import duckdb

# local libraries
from datagrunt.core.databases import DuckDBDatabase
from datagrunt.core.csvcomponents import CSVDelimiter, CSVColumns, CSVColumnNameNormalizer

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
        """Evaluate if a filename is passed in and if not, return default filename."""
        # This method doesn't really fit the class but it's the only place it's relevant.
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
        """Query to import a CSV file into a DuckDB table and normalize column names.

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
        """Query to export a DuckDB table to a CSV file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a CSV file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY {self.database_table_name} TO '{filename}' (HEADER, DELIMITER ',');"

    def export_excel_query(self, default_filename, export_filename=None):
        """Query to export a DuckDB table to an Excel file.

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
        """Query to export a DuckDB table to a JSON file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a JSON file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}' (ARRAY true) "

    def export_json_newline_delimited_query(self, default_filename, export_filename=None):
        """Query to export a DuckDB table to a JSON file with newline delimited.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a JSON file with newline delimited.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'"

    def export_parquet_query(self, default_filename, export_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a Parquet file.
        """
        filename = self.set_export_filename(default_filename, export_filename)
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT PARQUET)"

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
        for old_name, new_name in zip(CSVColumns(self.filepath).columns,
                                      CSVColumnNameNormalizer(self.filepath).columns_normalized
                                      ):
            sql_string = f"ALTER TABLE {self.database_table_name} RENAME COLUMN '{old_name}' TO '{new_name}'"
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

    def sql_query_to_dataframe(self, sql_query, normalize_columns=False):
        """Query to convert a SQL query to a Polars DataFrame.

        Args:
            sql_query (str): The SQL query to execute.
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            polars.DataFrame: The resulting DataFrame.
        """
        self.create_table(normalize_columns)
        return duckdb.sql(sql_query).pl()
