"""Module for data processing from files. Uses duckdb for file processing
   and Polars for dataframe objects.
"""

# standard library
import csv
from functools import lru_cache
import json
import logging

# third party libraries

# local libraries
from core.filehelpers import CSVParser
from core.databases import DuckDBDatabase

class CSVFile(CSVParser):

    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_DELIMITER = ','
    DEFAULT_SAMPLE_ROWS = 1
    CSV_SNIFF_SAMPLE_ROWS = 5
    DATAFRAME_SAMPLE_ROWS = 5

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
        self.duckdb_instance = DuckDBDatabase(self.filepath)
        if not self.is_csv:
            raise ValueError(f"File extension '{self.extension_string}' is not a valid CSV file extension.")

    @lru_cache
    def _csv_import_table_statement(self):
        """Default CSV import table statement."""
        # all_varchar=True is set to preserve integrity of data by importing as strings.
        return f"""CREATE OR REPLACE TABLE {self.duckdb_instance.database_table_name}
                 AS SELECT *
                 FROM read_csv('{self.filepath}',
                                auto_detect=true,
                                delim='{self.delimiter}',
                                header=true,
                                null_padding=true,
                                all_varchar=True)
                """

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

    @lru_cache
    def select_from_table(self, sql_statement):
        """Select from duckdb table. This method gives the user an option to
           write a data transformation as a SQL statement. Results returned
           as a Polars dataframe.

        Args:
            sql_statement (str): SQL statement to import data.

        Return:
            Polars dataframe.
        """
        with self.duckdb_instance.database_connection as con:
            con.sql(self._csv_import_table_statement())
            return con.query(sql_statement).pl()

    @lru_cache
    def get_row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    def get_row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.get_row_count_with_header() - 1

    def get_attributes(self):
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
        return self.get_attributes()['columns_list']

    def get_columns_string(self):
        """Return the first row of the CSV file."""
        return self.get_attributes()['columns_string']

    def get_columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        return self.get_attributes()['columns_byte_string']

    def get_column_count(self):
        """Return the number of columns in the CSV file."""
        return self.get_attributes()['column_count']

    def get_quotechar(self):
        """Return the quote character used in the CSV file."""
        return self.get_attributes()['quotechar']

    def get_escapechar(self):
        """Return the escape character used in the CSV file."""
        return self.get_attributes()['escapechar']

    def get_newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self.get_attributes()['newline_delimiter']

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        delimiter = self.infer_csv_file_delimiter()
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
            return list(csv_reader)

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe."""
        return self.duckdb_instance.to_dataframe(self._csv_import_table_statement())

    def to_json(self):
         """Converts CSV to a JSON string."""
         if self.is_large:
             print("Warning: File is large and may not load into memory properly.")
         return json.loads(self.to_dataframe().write_json())

    def to_json_newline_delimited(self):
        """Converts CSV to a JSON string with newline delimited."""
        return self.to_dataframe().write_ndjson()

    def write_avro(self, out_filename=None):
        """Writes data to an Avro file.

        Args:
            out_filename (str): The name of the output file.
        """
        if not out_filename:
            filename = self.duckdb_instance.AVRO_OUT_FILENAME
        self.to_dataframe().write_avro(filename)

    def write_csv(self, out_filename=None):
        """Writes CSV to a file.

        Args:
            out_filename (str): The name of the output file.
        """
        if out_filename:
            sql = self.update_sql_output_file(self.duckdb_instance.export_to_csv_statement(),
                                              self.duckdb_instance.CSV_OUT_FILENAME,
                                              out_filename)
        else:
            sql = self.duckdb_instance.export_to_csv_statement()
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(), sql)

    def write_json(self, out_filename=None):
        """Writes JSON to a file.

        Args:
            out_filename (str): The name of the output file.
        """
        if out_filename:
            sql = self.update_sql_output_file(self.duckdb_instance.export_to_json_array_statement(),
                                              self.duckdb_instance.JSON_OUT_FILENAME,
                                              out_filename)
        else:
            sql = self.duckdb_instance.export_to_json_array_statement()
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(), sql)

    def write_json_newline_delimited(self, out_filename=None):
        """Writes JSON to a file with newline delimited.

        Args:
            out_filename (str): The name of the output file.
        """
        if out_filename:
            sql = self.update_sql_output_file(self.duckdb_instance.export_to_json_newline_delimited_statement(),
                                              self.duckdb_instance.JSON_NEWLINE_OUT_FILENAME,
                                              out_filename)
        else:
            sql = self.duckdb_instance.export_to_json_newline_delimited_statement()
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(), sql)

    def write_parquet(self, out_filename=None):
        """Writes data to a Parquet file.

        Args:
            out_filename (str): The name of the output file.
        """
        if out_filename:
            sql = self.update_sql_output_file(self.duckdb_instance.export_to_parquet_statement(),
                                              self.duckdb_instance.PARQUET_OUT_FILENAME,
                                              out_filename)
        else:
            sql = self.duckdb_instance.export_to_parquet_statement()
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(), sql)

    def write_excel(self, out_filename=None):
        """Writes data to an Excel file.

        Args:
            out_filename (str): The name of the output file.
        """
        if self.get_row_count_without_header() > self.EXCEL_ROW_LIMIT:
            logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
            logging.warning(f"Row count {self.get_row_count_without_header()} is greater than Excel row limit of {self.EXCEL_ROW_LIMIT}. Data will be lost.")

        if out_filename:
            sql = self.update_sql_output_file(self.duckdb_instance.export_to_excel_statement(),
                                              self.duckdb_instance.EXCEL_OUT_FILENAME,
                                              out_filename)
        else:
            sql = self.duckdb_instance.export_to_excel_statement()
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(), sql)