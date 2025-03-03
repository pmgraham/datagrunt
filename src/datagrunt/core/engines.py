"""Module engines to enable data processing."""

# standard library
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Union

# third party libraries
import duckdb
from duckdb import DuckDBPyRelation
import polars as pl
import pyarrow as pa

# local libraries
from src.datagrunt.core.csvcomponents import CSVDelimiter, CSVColumnNameNormalizer
from src.datagrunt.core.queries import DuckDBQueries

@dataclass
class EngineProperties:
    """Base properties for CSV operations."""
    filepath: str
    DATAFRAME_SAMPLE_ROWS: int = 20
    CSV_export_filename: str = 'output.csv'
    EXCEL_export_filename: str = 'output.xlsx'
    JSON_export_filename: str = 'output.json'
    JSON_NEWLINE_export_filename: str = 'output.jsonl'
    PARQUET_export_filename: str = 'output.parquet'
    VALID_ENGINES: tuple = ('duckdb', 'polars')
    VALUE_ERROR_MESSAGE: str = """Reader engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

class BaseReaderEngine(ABC):
    """Abstract base class defining the interface for reader engines."""

    @abstractmethod
    def get_sample(self, normalize_columns: bool = False) -> None:
        """Return a sample of the data."""
        pass

    @abstractmethod
    def to_dataframe(self, normalize_columns: bool = False) -> pl.DataFrame:
        """Convert data to a dataframe."""
        pass

    @abstractmethod
    def to_arrow_table(self, normalize_columns: bool = False) -> pa.Table:
        """Convert data to a PyArrow table."""
        pass

    @abstractmethod
    def to_dicts(self, normalize_columns: bool = False) -> List[Dict]:
        """Convert data to a list of dictionaries."""
        pass

    @abstractmethod
    def query_data(self, sql_query: str, normalize_columns: bool = False) -> Union[DuckDBPyRelation, pl.DataFrame]:
        """Query the data using SQL."""
        pass

class BaseWriterEngine(ABC):
    """Abstract base class defining the interface for writer engines."""

    @abstractmethod
    def write_csv(self, export_filename, normalize_columns=False):
        """Write data to CSV format."""
        pass

    @abstractmethod
    def write_excel(self, export_filename, normalize_columns=False):
        """Write data to Excel format."""
        pass

    @abstractmethod
    def write_json(self, export_filename, normalize_columns=False):
        """Write data to JSON format."""
        pass

    @abstractmethod
    def write_json_newline_delimited(self, export_filename, normalize_columns=False):
        """Write data to JSON Lines format."""
        pass

    @abstractmethod
    def write_parquet(self, export_filename, normalize_columns=False):
        """Write data to Parquet format."""
        pass

class CSVReaderDuckDBEngine(BaseReaderEngine):
    """Class to read CSV files and convert CSV files powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        self.filepath = filepath
        self.queries = DuckDBQueries(self.filepath)

    @property
    def db_table(self):
        """Return the DuckDB table."""
        return self.queries.database_table_name

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        self.queries.create_table(normalize_columns).show()

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        return self.queries.create_table(normalize_columns).pl()

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        return self.queries.create_table(normalize_columns).arrow()

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe(normalize_columns).to_dicts()
        return dicts

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = f"SELECT col1, col2 FROM {dg.db_table}"
            dg.query_csv_data(query)
        """
        self.queries.create_table(normalize_columns)
        return duckdb.sql(sql_query)

class CSVReaderPolarsEngine(BaseReaderEngine):
    """Class to read CSV files and convert CSV files powered by Polars."""
    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        self.filepath = filepath
        self.queries = DuckDBQueries(self.filepath)
        self.delimiter = CSVDelimiter(self.filepath).delimiter

    @property
    def db_table(self):
        """Return the DuckDB table."""
        return self.queries.database_table_name

    def _create_dataframe(self, normalize_columns=False):
        """Normalizes the column names of the dataframe."""
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         infer_schema=False
                        )
        if normalize_columns:
            df = df.rename(CSVColumnNameNormalizer(self.filepath).columns_to_normalized_mapping)
        return df

    def _create_dataframe_sample(self, normalize_columns=False):
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         infer_schema=False,
                         n_rows=EngineProperties.DATAFRAME_SAMPLE_ROWS
                        )
        if normalize_columns:
            df = df.rename(CSVColumnNameNormalizer(self.filepath).columns_to_normalized_mapping)
        return df

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        df = self._create_dataframe_sample(normalize_columns)
        print(df)

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        return self._create_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        df = self._create_dataframe(normalize_columns).to_arrow()
        return df

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of Python dictionaries.

        Returns:
            A list of dictionaries.
        """
        dicts = self._create_dataframe(normalize_columns).to_dicts()
        return dicts

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)
        """
        return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)

class CSVWriterDuckDBEngine(BaseWriterEngine):
    """Class to convert CSV files to various other supported file types powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        self.filepath = filepath
        self.queries = DuckDBQueries(self.filepath)

    @property
    def db_table(self):
        """Return the DuckDB table."""
        return self.queries.database_table_name

    def write_csv(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a CSV file.

            Args:
                export_filename str: The name of the output file.
            """
        filename = self.queries.set_export_filename(EngineProperties.CSV_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_csv_query(filename))

    def write_excel(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.EXCEL_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_excel_query(filename))

    def write_json(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.JSON_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_query(filename))

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.JSON_NEWLINE_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_newline_delimited_query(filename))

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.PARQUET_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_parquet_query(filename))

class CSVWriterPolarsEngine(BaseWriterEngine):
    """Class to write CSVs to other file formats powered by Polars."""

    def __init__(self, filepath):
        self.filepath = filepath
        self.queries = DuckDBQueries(filepath)

    def write_csv(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a CSV file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.CSV_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_csv(filename)

    def write_excel(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.EXCEL_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_excel(filename)

    def write_json(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.JSON_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_json(filename)

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.JSON_NEWLINE_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_ndjson(filename)

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """Export a Polars dataframe to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
        """
        filename = self.queries.set_export_filename(EngineProperties.PARQUET_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_parquet(filename)

class EngineFactory:
    """Factory class for creating reader and writer engine instances."""

    def __init__(self, filepath, engine):
        self.filepath = filepath
        self.engine = engine.lower().replace(' ', '')
        if self.engine not in EngineProperties.VALID_ENGINES:
            raise ValueError(EngineProperties.VALUE_ERROR_MESSAGE.format(engine=self.engine))

    def create_reader(self):
        """Create a reader engine instance.

        Args:
            filepath: Path to the input file
            engine: Engine type ('duckdb' or 'polars')

        Returns:
            An instance of BaseReaderEngine
        """
        if self.engine == 'duckdb':
            return CSVReaderDuckDBEngine(self.filepath)
        else:
            return CSVReaderPolarsEngine(self.filepath)

    def create_writer(self):
        """Create a writer engine instance.

        Args:
            filepath: Path to the input file
            engine: Engine type ('duckdb' or 'polars')

        Returns:
            An instance of BaseWriterEngine
        """
        if self.engine == 'duckdb':
            return CSVWriterDuckDBEngine(self.filepath)
        else:
            return CSVWriterPolarsEngine(self.filepath)
