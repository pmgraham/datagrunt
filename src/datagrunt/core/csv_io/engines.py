"""Module to create engines for data processing."""

# standard library
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Union

# third party libraries
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from duckdb import DuckDBPyRelation

# local libraries
from datagrunt.core.csv_io.csvcomponents import CSVColumnNameNormalizer, CSVColumns, CSVDelimiter
from datagrunt.core.databases import DuckDBQueries


@dataclass
class CSVEngineProperties:
    """Base properties for CSV operations."""

    filepath: str
    dataframe_sample_rows: int = 20
    csv_export_filename: str = "output.csv"
    excel_export_filename: str = "output.xlsx"
    json_export_filename: str = "output.json"
    json_newline_export_filename: str = "output.jsonl"
    parquet_export_filename: str = "output.parquet"
    valid_engines: tuple = ("duckdb", "polars", "pyarrow")
    value_error_message: str = (
        "Reader engine '{engine}' is not 'duckdb', 'polars', or 'pyarrow'. "
        "Pass either 'duckdb', 'polars', or 'pyarrow' as valid engine params."
    )
    missing_file_message: str = "File '{filepath}'. No such file or directory."


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
        """
        Convert data to a PyArrow table.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def to_dicts(self, normalize_columns: bool = False) -> List[Dict]:
        """
        Convert data to a list of dictionaries.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        pass

    @abstractmethod
    def query_data(self, sql_query: str, normalize_columns: bool = False) -> Union[DuckDBPyRelation, pl.DataFrame]:
        """
        Query the data using SQL.

        Args:
            sql_query (str): SQL query to execute.
            normalize_columns (optional, bool): Whether to normalize column
            names.
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
        """
        Write data to Excel format.

        Args:
            export_filename (str): Path to the file to write.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        pass

    @abstractmethod
    def write_json(self, export_filename, normalize_columns=False):
        """Write data to JSON format."""
        pass

    @abstractmethod
    def write_json_newline_delimited(self, export_filename, normalize_columns=False):
        """
        Write data to JSON Lines format.

        Args:
            export_filename (str): Path to the file to write.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        pass

    @abstractmethod
    def write_parquet(self, export_filename, normalize_columns=False):
        """
        Write data to Parquet format.

        Args:
            export_filename (str): Path to the file to write.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        pass


class CSVReaderDuckDBEngine(CSVBaseReaderEngine):
    """
    Class to read CSV files and convert CSV files powered by DuckDB.
    """

    def get_sample(self, normalize_columns=False):
        """
        Return a sample of the CSV file.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        self.queries.create_table(normalize_columns).show()

    def to_dataframe(self, normalize_columns=False):
        """
        Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        return self.queries.create_table(normalize_columns).pl()

    def to_arrow_table(self, normalize_columns=False):
        """
        Converts CSV to a PyArrow table.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A PyArrow table.
        """
        return self.queries.create_table(normalize_columns).arrow()

    def to_dicts(self, normalize_columns=False):
        """
        Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A list of dictionaries.
        """
        return self.to_dataframe(normalize_columns).to_dicts()

    def _normalize_relation_columns(self, relation):
        """
        Applies column name normalization to a DuckDBPyRelation.
        Args:
            relation (duckdb.DuckDBPyRelation): The relation to normalize.
        Returns:
            duckdb.DuckDBPyRelation: The normalized relation.
        """
        current_columns = relation.columns
        column_normalizer = CSVColumnNameNormalizer(self.filepath)
        projections = []
        for col in current_columns:
            normalized_name = column_normalizer.columns_to_normalized_mapping.get(col, col)
            projections.append(f'"{col}" AS "{normalized_name}"')
        return relation.project(", ".join(projections))

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (optional, bool): Whether to normalize
                column names.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = f"SELECT col1, col2 FROM {dg.db_table}"
            dg.query_csv_data(query)
        """  # noqa: E501
        # Ensure the base table is created with original column names
        # so the user's query can reference them.
        self.queries.create_table(normalize_columns=False)

        # Execute the user's query
        result_relation = duckdb.sql(sql_query)

        if normalize_columns:
            result_relation = self._normalize_relation_columns(result_relation)

        return result_relation


class CSVReaderPolarsEngine(CSVBaseReaderEngine):
    """
    Class to read CSV files and convert CSV files powered by Polars.
    """

    def _create_dataframe(self, normalize_columns=False):
        """
        Normalizes the column names of the dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        df = pl.read_csv(self.filepath, separator=self.delimiter, truncate_ragged_lines=True, infer_schema=False)
        if normalize_columns:
            df = df.rename(CSVColumnNameNormalizer(self.filepath).columns_to_normalized_mapping)
        return df

    def _create_dataframe_sample(self, normalize_columns=False):
        """
        Create a sample of the CSV file as a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        df = pl.read_csv(
            self.filepath,
            separator=self.delimiter,
            truncate_ragged_lines=True,
            infer_schema=False,
            n_rows=CSVEngineProperties.dataframe_sample_rows,
        )
        if normalize_columns:
            df = df.rename(CSVColumnNameNormalizer(self.filepath).columns_to_normalized_mapping)
        return df

    def get_sample(self, normalize_columns=False):
        """
        Return a sample of the CSV file.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        df = self._create_dataframe_sample(normalize_columns)
        print(df)

    def to_dataframe(self, normalize_columns=False):
        """
        Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        return self._create_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """
        Converts CSV to a PyArrow table.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A PyArrow table.
        """
        df = self._create_dataframe(normalize_columns).to_arrow()
        return df

    def to_dicts(self, normalize_columns=False):
        """
        Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A list of dictionaries.
        """
        dicts = self._create_dataframe(normalize_columns).to_dicts()
        return dicts

    def query_data(self, sql_query, normalize_columns=False):
        """
        Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)
        """
        return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)


class CSVWriterDuckDBEngine(CSVBaseWriterEngine):
    """
    Class to convert CSV files to various other supported file types powered
    by DuckDB.
    """

    def __init__(self, filepath):
        """Initialize the CSVWriterDuckDBEngine class."""
        super().__init__(filepath)

    def write_csv(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a CSV file.

            Args:
                export_filename str: The name of the output file.
                normalize_columns bool: Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.csv_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_csv_query(filename))

    def write_excel(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.excel_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_excel_query(filename))

    def write_json(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_query(filename))

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_newline_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_json_newline_delimited_query(filename))

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.parquet_export_filename, export_filename)
        self.queries.create_table(normalize_columns)
        duckdb.sql(self.queries.export_parquet_query(filename))


class CSVWriterPolarsEngine(CSVBaseWriterEngine):
    """Class to write CSVs to other file formats powered by Polars."""

    def __init__(self, filepath):
        """Initialize the CSVWriterPolarsEngine class."""
        super().__init__(filepath)

    def write_csv(self, export_filename=None, normalize_columns=False):
        """
        Export a Polars dataframe to a CSV file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.csv_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_csv(filename)

    def write_excel(self, export_filename=None, normalize_columns=False):
        """
        Export a Polars dataframe to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.excel_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_excel(filename)

    def write_json(self, export_filename=None, normalize_columns=False):
        """
        Export a Polars dataframe to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_json(filename)

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """
        Export a Polars dataframe to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_newline_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_ndjson(filename)

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """
        Export a Polars dataframe to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.parquet_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe(normalize_columns)
        df.write_parquet(filename)


class CSVReaderPyArrowEngine(CSVBaseReaderEngine):
    """
    Class to read CSV files and convert CSV files powered by PyArrow.
    """

    def _create_table(self, normalize_columns=False):
        """
        Create a PyArrow table from the CSV file.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A PyArrow table.
        """
        # Use PyArrow's CSV reader with all columns as string to prevent data loss
        # Get column names efficiently using existing CSVColumns class
        columns = CSVColumns(self.filepath).columns

        # Create schema with all string types
        string_schema = pa.schema([(name, pa.string()) for name in columns])

        # Read with explicit string types
        table = pacsv.read_csv(
            self.filepath,
            parse_options=pacsv.ParseOptions(delimiter=self.delimiter),
            convert_options=pacsv.ConvertOptions(column_types=string_schema),
        )

        if normalize_columns:
            column_normalizer = CSVColumnNameNormalizer(self.filepath)
            old_names = table.column_names
            new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
            table = table.rename_columns(new_names)

        return table

    def _create_table_sample(self, normalize_columns=False):
        """
        Create a sample of the CSV file as a PyArrow table.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A PyArrow table.
        """
        # Read with PyArrow with all columns as string to prevent data loss
        # Get column names efficiently using existing CSVColumns class
        columns = CSVColumns(self.filepath).columns

        # Create schema with all string types
        string_schema = pa.schema([(name, pa.string()) for name in columns])

        # Read with explicit string types
        table = pacsv.read_csv(
            self.filepath,
            parse_options=pacsv.ParseOptions(delimiter=self.delimiter),
            convert_options=pacsv.ConvertOptions(column_types=string_schema),
        )

        # Take sample rows
        sample_table = table.slice(0, CSVEngineProperties.dataframe_sample_rows)

        if normalize_columns:
            column_normalizer = CSVColumnNameNormalizer(self.filepath)
            old_names = sample_table.column_names
            new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
            sample_table = sample_table.rename_columns(new_names)

        return sample_table

    def get_sample(self, normalize_columns=False):
        """
        Return a sample of the CSV file.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        table = self._create_table_sample(normalize_columns)
        # Convert to Polars DataFrame for display
        df = pl.from_arrow(table)
        if isinstance(df, pl.Series):
            df = df.to_frame()
        print(df)

    def to_dataframe(self, normalize_columns=False) -> pl.DataFrame:
        """
        Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        table = self._create_table(normalize_columns)
        df = pl.from_arrow(table)
        # Ensure we always return a DataFrame, not a Series
        if isinstance(df, pl.Series):
            df = df.to_frame()
        return df

    def to_arrow_table(self, normalize_columns=False):
        """
        Converts CSV to a PyArrow table.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A PyArrow table.
        """
        return self._create_table(normalize_columns)

    def to_dicts(self, normalize_columns=False):
        """
        Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A list of dictionaries.
        """
        table = self._create_table(normalize_columns)
        # Convert to Polars DataFrame and then to dicts
        df = pl.from_arrow(table)
        if isinstance(df, pl.Series):
            df = df.to_frame()
        return df.to_dicts()

    def query_data(self, sql_query, normalize_columns=False):
        """
        Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)
        """
        return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)


class CSVWriterPyArrowEngine(CSVBaseWriterEngine):
    """Class to write CSVs to other file formats powered by PyArrow."""

    def __init__(self, filepath):
        """Initialize the CSVWriterPyArrowEngine class."""
        super().__init__(filepath)

    def _create_table(self, normalize_columns=False):
        """Create a PyArrow table for writing operations."""
        # Read with PyArrow with all columns as string to prevent data loss
        # Get column names efficiently using existing CSVColumns class
        columns = CSVColumns(self.filepath).columns

        # Create schema with all string types
        string_schema = pa.schema([(name, pa.string()) for name in columns])

        # Read with explicit string types
        table = pacsv.read_csv(
            self.filepath,
            parse_options=pacsv.ParseOptions(delimiter=CSVDelimiter(self.filepath).delimiter),
            convert_options=pacsv.ConvertOptions(column_types=string_schema),
        )

        if normalize_columns:
            column_normalizer = CSVColumnNameNormalizer(self.filepath)
            old_names = table.column_names
            new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
            table = table.rename_columns(new_names)

        return table

    def write_csv(self, export_filename=None, normalize_columns=False):
        """
        Export a PyArrow table to a CSV file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.csv_export_filename, export_filename)
        table = self._create_table(normalize_columns)
        # Use native PyArrow CSV writer - no dataframe conversion needed
        pacsv.write_csv(table, filename)

    def write_excel(self, export_filename=None, normalize_columns=False):
        """
        Export a PyArrow table to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.excel_export_filename, export_filename)
        table = self._create_table(normalize_columns)
        # Convert to Polars DataFrame for Excel export
        df = pl.from_arrow(table)
        if isinstance(df, pl.Series):
            df = df.to_frame()
        df.write_excel(filename)

    def write_json(self, export_filename=None, normalize_columns=False):
        """
        Export a PyArrow table to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_export_filename, export_filename)
        table = self._create_table(normalize_columns)
        # Use native PyArrow iteration to avoid dataframe conversion
        records = []
        for i in range(table.num_rows):
            record = {col: table[col][i].as_py() for col in table.column_names}
            records.append(record)
        with open(filename, "w") as f:
            json.dump(records, f, indent=4)

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """
        Export a PyArrow table to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_newline_export_filename, export_filename)
        table = self._create_table(normalize_columns)
        # Use native PyArrow iteration to avoid dataframe conversion
        with open(filename, "w") as f:
            for i in range(table.num_rows):
                record = {col: table[col][i].as_py() for col in table.column_names}
                f.write(json.dumps(record) + "\n")

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """
        Export a PyArrow table to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.parquet_export_filename, export_filename)
        table = self._create_table(normalize_columns)
        # Use native PyArrow Parquet writer - no dataframe conversion needed
        pq.write_table(table, filename)
