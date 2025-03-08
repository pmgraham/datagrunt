"""Module to create engines for data processing."""

# standard library
from abc import ABC, abstractmethod
from dataclasses import dataclass
import os
from typing import Dict, List, Union

# third party libraries
import duckdb
from duckdb import DuckDBPyRelation
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.csvcomponents import CSVDelimiter, CSVColumnNameNormalizer
from datagrunt.core.queries import DuckDBQueries

@dataclass
class EngineProperties:
    """Base properties for CSV operations."""
    filepath: str
    dataframe_sample_rows: int = 20
    csv_export_filename: str = 'output.csv'
    excel_export_filename: str = 'output.xlsx'
    json_export_filename: str = 'output.json'
    json_newline_export_filename: str = 'output.jsonl'
    parquet_export_filename: str = 'output.parquet'
    valid_engines: tuple = ('duckdb', 'polars')
    value_error_message: str = """Reader engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""
    missing_file_message: str = """File '{filepath}'. No such file or directory."""

class CSVBaseReaderEngine(ABC):
    """Abstract base class defining the interface for reader engines."""

    def __init__(self, filepath):
        """Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        self.filepath = filepath
        self.queries = DuckDBQueries(self.filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.delimiter = CSVDelimiter(self.filepath).delimiter
        if not os.path.exists(self.filepath):
            raise FileNotFoundError

    @abstractmethod
    def get_sample(self, normalize_columns: bool = False) -> None:
        """Return a sample of the data.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def to_dataframe(self, normalize_columns: bool = False) -> pl.DataFrame:
        """Convert data to a dataframe.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def to_arrow_table(self, normalize_columns: bool = False) -> pa.Table:
        """Convert data to a PyArrow table.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def to_dicts(self, normalize_columns: bool = False) -> List[Dict]:
        """Convert data to a list of dictionaries.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def query_data(self, sql_query: str, normalize_columns: bool = False) -> Union[DuckDBPyRelation, pl.DataFrame]:
        """Query the data using SQL.

        Args:
            sql_query (str): SQL query to execute.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        pass

class CSVBaseWriterEngine(ABC):
    """Abstract base class defining the interface for writer engines."""

    def __init__(self, filepath):
        """
        Initialize the CSV Writer DuckDB Engine class.

        Args:
            filepath (str): Path to the file to write.
        """
        self.filepath = filepath
        self.queries = DuckDBQueries(self.filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        if not os.path.exists(self.filepath):
            raise FileNotFoundError

    @abstractmethod
    def write_csv(self, export_filename, normalize_columns=False):
        """Write data to CSV format."""
        pass

    @abstractmethod
    def write_excel(self, export_filename, normalize_columns=False):
        """Write data to Excel format.

        Args:
            export_filename (str): Path to the file to write.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def write_json(self, export_filename, normalize_columns=False):
        """Write data to JSON format."""
        pass

    @abstractmethod
    def write_json_newline_delimited(self, export_filename, normalize_columns=False):
        """Write data to JSON Lines format.

        Args:
            export_filename (str): Path to the file to write.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def write_parquet(self, export_filename, normalize_columns=False):
        """Write data to Parquet format.

        Args:
            export_filename (str): Path to the file to write.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        pass

class CSVReaderDuckDBEngine(CSVBaseReaderEngine):
    """Class to read CSV files and convert CSV files powered by DuckDB."""

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        self.queries.create_table(normalize_columns).show()

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A Polars dataframe.
        """
        return self.queries.create_table(normalize_columns).pl()

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A PyArrow table.
        """
        return self.queries.create_table(normalize_columns).arrow()

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A list of dictionaries.
        """
        return self.to_dataframe(normalize_columns).to_dicts()

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = f"SELECT col1, col2 FROM {dg.db_table}"
            dg.query_csv_data(query)
        """
        self.queries.create_table(normalize_columns)
        return duckdb.sql(sql_query)

class CSVReaderPolarsEngine(CSVBaseReaderEngine):
    """Class to read CSV files and convert CSV files powered by Polars."""

    def _create_dataframe(self, normalize_columns=False):
        """Normalizes the column names of the dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A Polars dataframe.
        """
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         infer_schema=False
                        )
        if normalize_columns:
            df = df.rename(CSVColumnNameNormalizer(self.filepath).columns_to_normalized_mapping)
        return df

    def _create_dataframe_sample(self, normalize_columns=False):
        """Create a sample of the CSV file as a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A Polars dataframe.
        """
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         infer_schema=False,
                         n_rows=EngineProperties.dataframe_sample_rows
                        )
        if normalize_columns:
            df = df.rename(CSVColumnNameNormalizer(self.filepath).columns_to_normalized_mapping)
        return df

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A Polars dataframe.
        """
        df = self._create_dataframe_sample(normalize_columns)
        print(df)

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A Polars dataframe.
        """
        return self._create_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A PyArrow table.
        """
        df = self._create_dataframe(normalize_columns).to_arrow()
        return df

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A list of dictionaries.
        """
        dicts = self._create_dataframe(normalize_columns).to_dicts()
        return dicts

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)
        """
        return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)

class CSVWriterDuckDBEngine(CSVBaseWriterEngine):
    """Class to convert CSV files to various other supported file types powered by DuckDB."""

    def write_csv(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a CSV file.

            Args:
                export_filename str: The name of the output file.
                normalize_columns bool: Whether to normalize column names.
            """
        filename = self.queries.set_export_filename(EngineProperties.csv_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_csv_query(filename))

    def write_excel(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.excel_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_excel_query(filename))

    def write_json(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.json_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_query(filename))

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.json_newline_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_newline_delimited_query(filename))

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.parquet_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_parquet_query(filename))

class CSVWriterPolarsEngine(CSVBaseWriterEngine):
    """Class to write CSVs to other file formats powered by Polars."""

    def write_csv(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a CSV file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.csv_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_csv(filename)

    def write_excel(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.excel_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_excel(filename)

    def write_json(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.json_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_json(filename)

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.json_newline_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_ndjson(filename)

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(EngineProperties.parquet_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_parquet(filename)
