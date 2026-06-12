"""Module to create engines for data processing."""

# standard library
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

# third party libraries
import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from duckdb import DuckDBPyRelation

# local libraries
from datagrunt.core.csv_io.csvcomponents import (
    CSVColumnNameNormalizer,
    CSVColumns,
    _check_csv_ragged_and_warn,
    _count_leading_comments,
    _count_leading_physical_lines_before_header,
)
from datagrunt.core.databases import DuckDBQueries


def _has_midfile_comments(filepath):
    """Return True if a ``#`` comment line appears after the leading comment block.

    PyArrow's CSV reader has no comment support and only the leading block is
    skipped via ``skip_rows``. A comment line further down the file (which the
    polars and duckdb engines tolerate) would otherwise crash the parser, so we
    detect it here to route around the native PyArrow reader. errors="ignore"
    so a non-UTF-8 byte can't crash this lightweight probe (issue #76).
    """
    in_leading_block = True
    with open(filepath, "r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            stripped = line.strip()
            is_comment_or_blank = stripped.startswith("#") or not stripped
            if in_leading_block:
                if is_comment_or_blank:
                    continue
                in_leading_block = False
            elif stripped.startswith("#"):
                return True
    return False


def _is_legacy_mac_newlines(filepath):
    """Check if the file uses legacy Mac OS carriage returns (\\r) as line endings."""
    try:
        with open(filepath, "rb") as f:
            chunk = f.read(4096)
        return b"\r" in chunk and b"\n" not in chunk
    except Exception:
        return False


def _resolve_normalize_columns(instance_default, per_call_value):
    """Resolve a per-call normalize flag against the engine's instance default.

    Args:
        instance_default (bool): The engine's constructor-level setting.
        per_call_value (bool or None): The per-call argument; ``None`` means
        "inherit the instance-level setting".

    Returns:
        bool: The effective normalization mode.
    """
    return instance_default if per_call_value is None else per_call_value


@dataclass
class CSVEngineProperties:
    """Base properties for CSV operations."""

    filepath: Path
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

    def __init__(self, filepath, lenient=False, normalize_columns=False):
        """Initialize the CSVReader class.

        Args:
            filepath (str or Path): Path to the file to read.
            lenient (bool): Whether to run in lenient mode.
            normalize_columns (bool): Whether to normalize column names for
            every operation on this engine. A per-call argument overrides it.
        """
        self.filepath = Path(filepath)
        self.lenient = lenient
        self.normalize_columns = normalize_columns
        self.queries = DuckDBQueries(self.filepath, lenient=self.lenient)
        self.db_table = self.queries.database_table_name
        self.delimiter = self.queries.delimiter
        if not self.filepath.exists():
            raise FileNotFoundError

    @abstractmethod
    def get_sample(self, normalize_columns: Optional[bool] = None) -> pl.DataFrame:
        """Return a sample of the data as a Polars DataFrame.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars DataFrame containing the sample rows.
        """
        pass

    @abstractmethod
    def to_dataframe(self, normalize_columns: Optional[bool] = None) -> pl.DataFrame:
        """Convert data to a dataframe.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.
        """
        pass

    @abstractmethod
    def to_arrow_table(self, normalize_columns: Optional[bool] = None) -> pa.Table:
        """
        Convert data to a PyArrow table.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.
        """
        pass

    @abstractmethod
    def to_dicts(self, normalize_columns: Optional[bool] = None) -> List[Dict]:
        """
        Convert data to a list of dictionaries.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.
        """
        pass

    @abstractmethod
    def query_data(self, sql_query: str, normalize_columns: Optional[bool] = None) -> Union[DuckDBPyRelation, pl.DataFrame]:
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

    def __init__(self, filepath, lenient=False, normalize_columns=False):
        """
        Initialize the CSV Writer DuckDB Engine class.

        Args:
            filepath (str or Path): Path to the file to write.
            lenient (bool): Whether to run in lenient mode.
            normalize_columns (bool): Whether to normalize column names for
            every operation on this engine. A per-call argument overrides it.
        """
        self.filepath = Path(filepath)
        self.lenient = lenient
        self.normalize_columns = normalize_columns
        self.queries = DuckDBQueries(self.filepath, lenient=self.lenient)
        self.db_table = self.queries.database_table_name
        if not self.filepath.exists():
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

    def get_sample(self, normalize_columns=None):
        """
        Return a sample of the CSV file.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars DataFrame containing the sample rows.
        """
        normalize_columns = _resolve_normalize_columns(self.normalize_columns, normalize_columns)
        # sample_dataframe streams the first rows (no full-table import) and
        # returns a materialized Polars frame, so the per-call connection can
        # be released deterministically rather than waiting on garbage
        # collection.
        try:
            return self.queries.sample_dataframe(
                CSVEngineProperties.dataframe_sample_rows,
                normalize_columns,
            )
        finally:
            self.queries.close()

    def to_dataframe(self, normalize_columns=None):
        """
        Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars dataframe.
        """
        normalize_columns = _resolve_normalize_columns(self.normalize_columns, normalize_columns)
        # Materialize fully (.pl()) before closing the per-call connection.
        try:
            return self.queries.create_table(normalize_columns).pl()
        finally:
            self.queries.close()

    def to_arrow_table(self, normalize_columns=None):
        """
        Converts CSV to a PyArrow table.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A PyArrow table.
        """
        normalize_columns = _resolve_normalize_columns(self.normalize_columns, normalize_columns)
        # Materialize into an in-memory pa.Table before closing the connection.
        try:
            result = self.queries.create_table(normalize_columns).arrow()
            if isinstance(result, pa.Table):
                return result
            return result.read_all()
        finally:
            self.queries.close()

    def to_dicts(self, normalize_columns=None):
        """
        Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A list of dictionaries.
        """
        # to_dataframe already materializes and closes; this is a thin wrapper.
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
            # Double-quote-escape both identifiers; a quote in a header-derived
            # column name would otherwise break the projection SQL.
            safe_col = col.replace('"', '""')
            safe_normalized = normalized_name.replace('"', '""')
            projections.append(f'"{safe_col}" AS "{safe_normalized}"')
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

        # Execute the user's query on the same per-instance connection that
        # holds the table. The returned relation keeps this connection alive,
        # so it remains valid after this engine instance is garbage collected.
        result_relation = self.queries.connection.sql(sql_query)

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
        if _is_legacy_mac_newlines(self.filepath):
            raise ValueError(
                "Polars engine does not support legacy Mac OS carriage return (\\r) newlines. "
                "Please use engine='duckdb' or convert the file to Unix/Windows newlines."
            )
        if self.lenient:
            _check_csv_ragged_and_warn(self.filepath, self.delimiter)
        try:
            # Skip only the LEADING comment block. Using comment_prefix here
            # would also drop any data row whose first field begins with "#"
            # (e.g. a hex color like "#FF0000"), silently losing rows.
            df = pl.read_csv(
                self.filepath,
                separator=self.delimiter,
                truncate_ragged_lines=self.lenient,
                infer_schema=False,
                skip_rows=_count_leading_comments(self.filepath),
            )
            if normalize_columns:
                df = df.rename(CSVColumnNameNormalizer(self.filepath, columns=df.columns).columns_to_normalized_mapping)
            return df
        except Exception as e:
            raise e

    def _create_dataframe_sample(self, normalize_columns=False):
        """
        Create a sample of the CSV file as a Polars dataframe.

        Args:
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            A Polars dataframe.
        """
        if _is_legacy_mac_newlines(self.filepath):
            raise ValueError(
                "Polars engine does not support legacy Mac OS carriage return (\\r) newlines. "
                "Please use engine='duckdb' or convert the file to Unix/Windows newlines."
            )
        if self.lenient:
            _check_csv_ragged_and_warn(self.filepath, self.delimiter)
        try:
            # Skip only the LEADING comment block so data rows whose first
            # field starts with "#" are not mistaken for comments.
            df = pl.read_csv(
                self.filepath,
                separator=self.delimiter,
                truncate_ragged_lines=self.lenient,
                infer_schema=False,
                n_rows=CSVEngineProperties.dataframe_sample_rows,
                skip_rows=_count_leading_comments(self.filepath),
            )
            if normalize_columns:
                df = df.rename(CSVColumnNameNormalizer(self.filepath, columns=df.columns).columns_to_normalized_mapping)
            return df
        except Exception as e:
            raise e

    def get_sample(self, normalize_columns=None):
        """
        Return a sample of the CSV file.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars dataframe.
        """
        return self._create_dataframe_sample(_resolve_normalize_columns(self.normalize_columns, normalize_columns))

    def to_dataframe(self, normalize_columns=None):
        """
        Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars dataframe.
        """
        return self._create_dataframe(_resolve_normalize_columns(self.normalize_columns, normalize_columns))

    def to_arrow_table(self, normalize_columns=None):
        """
        Converts CSV to a PyArrow table.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A PyArrow table.
        """
        return self._create_dataframe(_resolve_normalize_columns(self.normalize_columns, normalize_columns)).to_arrow()

    def to_dicts(self, normalize_columns=None):
        """
        Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A list of dictionaries.
        """
        return self._create_dataframe(_resolve_normalize_columns(self.normalize_columns, normalize_columns)).to_dicts()

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
        # The result is a fully-materialized polars DataFrame, so the
        # connection can be disposed deterministically (unlike the duckdb
        # engine, whose query_data returns a live relation).
        try:
            return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)
        finally:
            self.queries.close()


class CSVWriterDuckDBEngine(CSVBaseWriterEngine):
    """
    Class to convert CSV files to various other supported file types powered
    by DuckDB.
    """

    def write_csv(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a CSV file.

            Args:
                export_filename str: The name of the output file.
                normalize_columns bool: Whether to normalize column names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.csv_export_filename, export_filename)
        # COPY executes eagerly on .sql(), so the file is written before close()
        # releases the per-call connection.
        try:
            self.queries.create_table(normalize_columns)
            self.queries.connection.sql(self.queries.export_csv_query(filename))
        finally:
            self.queries.close()

    def write_excel(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to an Excel file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.excel_export_filename, export_filename)
        try:
            self.queries.create_table(normalize_columns)
            self.queries.connection.sql(self.queries.export_excel_query(filename))
        finally:
            self.queries.close()

    def write_json(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a JSON file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_export_filename, export_filename)
        try:
            self.queries.create_table(normalize_columns)
            self.queries.connection.sql(self.queries.export_json_query(filename))
        finally:
            self.queries.close()

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.json_newline_export_filename, export_filename)
        try:
            self.queries.create_table(normalize_columns)
            self.queries.connection.sql(self.queries.export_json_newline_delimited_query(filename))
        finally:
            self.queries.close()

    def write_parquet(self, export_filename=None, normalize_columns=False):
        """
        Query to export a DuckDB table to a Parquet file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.parquet_export_filename, export_filename)
        try:
            self.queries.create_table(normalize_columns)
            self.queries.connection.sql(self.queries.export_parquet_query(filename))
        finally:
            self.queries.close()


class CSVWriterPolarsEngine(CSVBaseWriterEngine):
    """Class to write CSVs to other file formats powered by Polars."""

    def write_csv(self, export_filename=None, normalize_columns=False):
        """
        Export a Polars dataframe to a CSV file.

        Args:
            export_filename (optional, str): The name of the output file.
            normalize_columns (optional, bool): Whether to normalize column
            names.
        """
        filename = self.queries.set_export_filename(CSVEngineProperties.csv_export_filename, export_filename)
        df = CSVReaderPolarsEngine(self.filepath, lenient=self.lenient).to_dataframe(normalize_columns)
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
        df = CSVReaderPolarsEngine(self.filepath, lenient=self.lenient).to_dataframe(normalize_columns)
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
        df = CSVReaderPolarsEngine(self.filepath, lenient=self.lenient).to_dataframe(normalize_columns)
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
        df = CSVReaderPolarsEngine(self.filepath, lenient=self.lenient).to_dataframe(normalize_columns)
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
        df = CSVReaderPolarsEngine(self.filepath, lenient=self.lenient).to_dataframe(normalize_columns)
        df.write_parquet(filename)


class CSVReaderPyArrowEngine(CSVBaseReaderEngine):
    """
    Class to read CSV files and convert CSV files powered by PyArrow.
    """

    def _polars_fallback_table(self, columns, normalize_columns, truncate_ragged_lines, n_rows=None):
        """Parse the CSV with Polars and convert it to an Arrow table.

        Used whenever the native PyArrow reader cannot handle the input: lenient
        (ragged) mode and files with mid-file ``#`` comment lines (which crash
        PyArrow's parser). Skips only the LEADING comment block — using
        ``comment_prefix`` would also drop any data row whose first field
        begins with ``#`` (e.g. a hex color), silently losing rows (issue #73)
        — so the result matches the polars reference engine row-for-row.

        Args:
            columns (list): Original column names, used for normalization lookup.
            normalize_columns (bool): Whether to normalize column names.
            truncate_ragged_lines (bool): Whether to truncate ragged rows.
            n_rows (int, optional): Limit the number of rows read (for samples).

        Returns:
            A PyArrow table with all columns cast to string.
        """
        df = pl.read_csv(
            self.filepath,
            separator=self.delimiter,
            truncate_ragged_lines=truncate_ragged_lines,
            infer_schema=False,
            skip_rows=_count_leading_comments(self.filepath),
            n_rows=n_rows,
        )
        table = df.to_arrow()
        string_schema = pa.schema([(name, pa.string()) for name in table.column_names])
        table = table.cast(string_schema)
        if normalize_columns:
            column_normalizer = CSVColumnNameNormalizer(self.filepath, columns=columns)
            old_names = table.column_names
            new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
            table = table.rename_columns(new_names)
        return table

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
        columns = CSVColumns(self.filepath, delimiter=self.delimiter).columns

        # Create schema with all string types
        string_schema = pa.schema([(name, pa.string()) for name in columns])

        # Read with explicit string types
        if _is_legacy_mac_newlines(self.filepath):
            raise ValueError(
                "PyArrow engine does not support legacy Mac OS carriage return (\\r) newlines. "
                "Please use engine='duckdb' or convert the file to Unix/Windows newlines."
            )
        if self.lenient:
            _check_csv_ragged_and_warn(self.filepath, self.delimiter)
            # Fallback to Polars to parse the ragged CSV, then convert to Arrow Table
            return self._polars_fallback_table(columns, normalize_columns, truncate_ragged_lines=True)
        if _has_midfile_comments(self.filepath):
            # PyArrow's native reader crashes on comment lines past the leading
            # block; defer to Polars so the rows match the polars reference
            # engine (issue #90).
            return self._polars_fallback_table(columns, normalize_columns, truncate_ragged_lines=False)
        try:
            skip_count = _count_leading_physical_lines_before_header(self.filepath)
            table = pacsv.read_csv(
                self.filepath,
                read_options=pacsv.ReadOptions(column_names=columns, skip_rows=skip_count),
                parse_options=pacsv.ParseOptions(delimiter=self.delimiter),
                convert_options=pacsv.ConvertOptions(column_types=string_schema),
            )

            if normalize_columns:
                column_normalizer = CSVColumnNameNormalizer(self.filepath, columns=columns)
                old_names = table.column_names
                new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
                table = table.rename_columns(new_names)

            return table
        except Exception as e:
            raise e

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
        columns = CSVColumns(self.filepath, delimiter=self.delimiter).columns

        # Create schema with all string types
        string_schema = pa.schema([(name, pa.string()) for name in columns])

        # Read with explicit string types
        if _is_legacy_mac_newlines(self.filepath):
            raise ValueError(
                "PyArrow engine does not support legacy Mac OS carriage return (\\r) newlines. "
                "Please use engine='duckdb' or convert the file to Unix/Windows newlines."
            )
        if self.lenient:
            _check_csv_ragged_and_warn(self.filepath, self.delimiter)
            # Fallback to Polars to parse the ragged CSV, then convert to Arrow Table
            return self._polars_fallback_table(
                columns,
                normalize_columns,
                truncate_ragged_lines=True,
                n_rows=CSVEngineProperties.dataframe_sample_rows,
            )
        if _has_midfile_comments(self.filepath):
            # PyArrow's native reader crashes on comment lines past the leading
            # block; defer to Polars so the rows match the polars reference
            # engine (issue #90).
            return self._polars_fallback_table(
                columns,
                normalize_columns,
                truncate_ragged_lines=False,
                n_rows=CSVEngineProperties.dataframe_sample_rows,
            )
        try:
            skip_count = _count_leading_physical_lines_before_header(self.filepath)
            sample_rows = CSVEngineProperties.dataframe_sample_rows
            # Stream the file in batches and stop once we have enough rows,
            # rather than materializing the whole file just to slice the head.
            reader = pacsv.open_csv(
                self.filepath,
                read_options=pacsv.ReadOptions(column_names=columns, skip_rows=skip_count),
                parse_options=pacsv.ParseOptions(delimiter=self.delimiter),
                convert_options=pacsv.ConvertOptions(column_types=string_schema),
            )
            batches = []
            collected = 0
            try:
                while collected < sample_rows:
                    batch = reader.read_next_batch()
                    if batch.num_rows == 0:
                        continue
                    batches.append(batch)
                    collected += batch.num_rows
            except StopIteration:
                # Fewer rows than the sample size; return whatever was read.
                pass
            finally:
                reader.close()

            sample_table = pa.Table.from_batches(batches, schema=string_schema).slice(0, sample_rows)

            if normalize_columns:
                column_normalizer = CSVColumnNameNormalizer(self.filepath, columns=columns)
                old_names = sample_table.column_names
                new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
                sample_table = sample_table.rename_columns(new_names)

            return sample_table
        except Exception as e:
            raise e

    def get_sample(self, normalize_columns=None):
        """
        Return a sample of the CSV file.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars DataFrame containing the sample rows.
        """
        normalize_columns = _resolve_normalize_columns(self.normalize_columns, normalize_columns)
        table = self._create_table_sample(normalize_columns)
        # Convert to a Polars DataFrame for a consistent return type
        df = pl.from_arrow(table)
        if isinstance(df, pl.Series):
            df = df.to_frame()
        return df

    def to_dataframe(self, normalize_columns=None) -> pl.DataFrame:
        """
        Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A Polars dataframe.
        """
        normalize_columns = _resolve_normalize_columns(self.normalize_columns, normalize_columns)
        table = self._create_table(normalize_columns)
        df = pl.from_arrow(table)
        # Ensure we always return a DataFrame, not a Series
        if isinstance(df, pl.Series):
            df = df.to_frame()
        return df

    def to_arrow_table(self, normalize_columns=None):
        """
        Converts CSV to a PyArrow table.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A PyArrow table.
        """
        return self._create_table(_resolve_normalize_columns(self.normalize_columns, normalize_columns))

    def to_dicts(self, normalize_columns=None):
        """
        Converts CSV to a list of Python dictionaries.

        Args:
            normalize_columns (bool or None): Whether to normalize column
            names. ``None`` (default) inherits the instance-level setting.

        Returns:
            A list of dictionaries.
        """
        normalize_columns = _resolve_normalize_columns(self.normalize_columns, normalize_columns)
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
        # The result is a fully-materialized polars DataFrame, so the
        # connection can be disposed deterministically (unlike the duckdb
        # engine, whose query_data returns a live relation).
        try:
            return self.queries.sql_query_to_dataframe(sql_query, normalize_columns)
        finally:
            self.queries.close()


class CSVWriterPyArrowEngine(CSVBaseWriterEngine):
    """Class to write CSVs to other file formats powered by PyArrow."""

    def _create_table(self, normalize_columns=False):
        """Create a PyArrow table for writing operations."""
        # Read with PyArrow with all columns as string to prevent data loss
        # Get column names efficiently using existing CSVColumns class
        columns = CSVColumns(self.filepath, delimiter=self.queries.delimiter).columns

        # Create schema with all string types
        string_schema = pa.schema([(name, pa.string()) for name in columns])

        # Read with explicit string types
        if _is_legacy_mac_newlines(self.filepath):
            raise ValueError(
                "PyArrow engine does not support legacy Mac OS carriage return (\\r) newlines. "
                "Please use engine='duckdb' or convert the file to Unix/Windows newlines."
            )
        if self.lenient or _has_midfile_comments(self.filepath):
            if self.lenient:
                _check_csv_ragged_and_warn(self.filepath, self.queries.delimiter)
            # Fallback to Polars: it parses ragged rows and skips mid-file ``#``
            # comment lines that the native PyArrow reader chokes on (issue #90).
            df = pl.read_csv(
                self.filepath,
                separator=self.queries.delimiter,
                truncate_ragged_lines=self.lenient,
                infer_schema=False,
                skip_rows=_count_leading_comments(self.filepath),
            )
            table = df.to_arrow()
            string_schema = pa.schema([(name, pa.string()) for name in table.column_names])
            table = table.cast(string_schema)
            if normalize_columns:
                column_normalizer = CSVColumnNameNormalizer(self.filepath, columns=columns)
                old_names = table.column_names
                new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
                table = table.rename_columns(new_names)
            return table
        try:
            skip_count = _count_leading_physical_lines_before_header(self.filepath)
            table = pacsv.read_csv(
                self.filepath,
                read_options=pacsv.ReadOptions(column_names=columns, skip_rows=skip_count),
                parse_options=pacsv.ParseOptions(delimiter=self.queries.delimiter),
                convert_options=pacsv.ConvertOptions(column_types=string_schema),
            )

            if normalize_columns:
                column_normalizer = CSVColumnNameNormalizer(self.filepath, columns=columns)
                old_names = table.column_names
                new_names = [column_normalizer.columns_to_normalized_mapping.get(name, name) for name in old_names]
                table = table.rename_columns(new_names)

            return table
        except Exception as e:
            raise e

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
