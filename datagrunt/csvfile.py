"""Module for data processing from files. Uses duckdb for file processing
   and Polars for dataframe objects.
"""

# standard library
import csv

# third party libraries
from duckdb import read_csv, sql

# local libraries
from datagrunt.core.filehelpers import CSVParser
from datagrunt.core.queries import DuckDBQueries
from datagrunt.core.logger import show_warning, show_large_file_warning

class CSVFile(CSVParser):

    QUOTING_MAP = {
        0: 'no quoting',
        1: 'quote all',
        2: 'quote minimal',
        3: 'quote non-numeric'
    }

    def __init__(self, filepath):
        """
        Initialize the CSVFile class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.attributes = self._get_attributes()
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.duckdb_connection = DuckDBQueries(self.filepath).database_connection
        if not self.is_csv:
            raise ValueError(f"File extension '{self.extension_string}' is not a valid CSV file extension.")

    def _csv_import_table_statement(self):
        """Default CSV import table statement."""
        # all_varchar=True is set to preserve integrity of data by importing as strings.
        return f"""
            CREATE OR REPLACE TABLE {self.db_table} AS
            SELECT *
            FROM read_csv('{self.filepath}',
                            auto_detect=true,
                            delim='{self.delimiter}',
                            header=true,
                            null_padding=true,
                            all_varchar=True);
            """
    
    def query_csv_data(self, sql_statement, show_only=False):
        """Query a CSV file using DuckDB Python API.

        Args:
            sql_statement (str): The SQL statement to execute.
            show_only (bool): Only show results don't return a dataframe.
        
        Returns:
            Polars DataFrame or None
        """
        con = self.duckdb_connection
        con.sql(self._csv_import_table_statement())
        if show_only:
            con.sql(sql_statement).show()
        else:
            return con.sql(sql_statement).pl()


    @staticmethod
    def update_sql_output_file(sql, original_output_file, new_output_file):
        """Updates the output file path in a SQL export statement.

        Args:
            sql (str): The original SQL export statement.
            original_output_file (str): The original output file path in the SQL statement.
            new_output_file (str): The new output file path to replace the original.

        Returns:
            str: The updated SQL statement with the new output file path.
        """
        return sql.replace(original_output_file, new_output_file)

    def _read_csv_to_duckdb(self):
        """Read CSV file using DuckDB Python API."""
        return read_csv(self.filepath,
                        delimiter=self.delimiter,
                        null_padding=True,
                        all_varchar=True
                        )

    def get_row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    def get_row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.get_row_count_with_header() - 1

    def _get_attributes(self):
        """Generate CSV attributes."""
        columns_list = self.first_row.split(self.delimiter)
        columns = {c: 'VARCHAR' for c in columns_list}
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csvfile:
            # Sniff the file to detect parameters
            dialect = csv.Sniffer().sniff(csvfile.read(self.CSV_SNIFF_SAMPLE_ROWS))
            csvfile.seek(0)  # Reset file pointer to the beginning

            attributes = {
                'delimiter': self.delimiter,
                'quotechar': dialect.quotechar,
                'escapechar': dialect.escapechar,
                'doublequote': dialect.doublequote,
                'newline_delimiter': dialect.lineterminator,
                'skipinitialspace': dialect.skipinitialspace,
                'quoting': self.QUOTING_MAP.get(dialect.quoting),
                'row_count_with_header': self.get_row_count_with_header(),
                'row_count_without_header': self.get_row_count_without_header(),
                'columns_schema': columns,
                'columns_original_format': self.first_row,
                'columns_list': columns_list,
                'columns_string': ", ".join(columns_list),
                'columns_byte_string': ", ".join(columns_list).encode(),
                'column_count': len(columns_list)
            }

        return attributes

    def get_columns(self):
        """Return the schema of the columns in the CSV file."""
        return self.attributes['columns_list']

    def get_columns_string(self):
        """Return the first row of the CSV file."""
        return self.attributes['columns_string']

    def get_columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        return self.attributes['columns_byte_string']

    def get_column_count(self):
        """Return the number of columns in the CSV file."""
        return self.attributes['column_count']

    def get_quotechar(self):
        """Return the quote character used in the CSV file."""
        return self.attributes['quotechar']

    def get_escapechar(self):
        """Return the escape character used in the CSV file."""
        return self.attributes['escapechar']

    def get_newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self.attributes['newline_delimiter']

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        if self.is_large:
             show_large_file_warning()
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=self.delimiter)
            return list(csv_reader)

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe."""
        return self._read_csv_to_duckdb().pl()

    def to_json(self):
         """Converts CSV to a JSON string."""
         if self.is_large:
             show_large_file_warning()
         return self.to_dataframe().write_json()

    def to_json_newline_delimited(self):
        """Converts CSV to a JSON string with newline delimited."""
        if self.is_large:
             show_large_file_warning()
        return self.to_dataframe().write_ndjson()

    def write_avro(self, out_filename=None):
        """Writes data to an Avro file.

        Args:
            out_filename (str): The name of the output file.
        """
        if out_filename:
            filename = out_filename
        else:
            filename = DuckDBQueries(self.filepath).AVRO_OUT_FILENAME
        self.to_dataframe().write_avro(filename)

    def write_csv(self, out_filename=None):
        """Writes CSV to a file.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).write_csv_query(out_filename))

    def write_json(self, out_filename=None):
        """Writes JSON to a file.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).write_json_query(out_filename))

    def write_json_newline_delimited(self, out_filename=None):
        """Writes JSON to a file with newline delimited.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).write_json_newline_delimited_query(out_filename))

    def write_parquet(self, out_filename=None):
        """Writes data to a Parquet file.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).write_parquet_query(out_filename))

    def write_excel(self, out_filename=None):
        """Writes data to an Excel file.

        Args:
            out_filename (str): The name of the output file.
        """
        if self.get_row_count_with_header() > self.EXCEL_ROW_LIMIT:
            show_warning(f"Data will be lost: file contains {self.get_row_count_with_header()} rows and Excel supports a max of {self.EXCEL_ROW_LIMIT} rows.")
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).write_excel_query(out_filename))
