"""Module for interfacing with databases."""

# standard library
import hashlib
import re
from functools import cached_property
from pathlib import Path

# third party libraries
import duckdb

# local imports
from datagrunt.core.csv_io import (
    CSVColumnNameNormalizer,
    CSVDelimiter,
    CSVDialect,
    _check_csv_ragged_and_warn,
    _count_leading_physical_lines_before_header,
)


class DuckDBQueries:
    """Class to store DuckDB database queries and query strings."""

    def __init__(self, filepath, lenient=False):
        """
        Initialize the DuckDBQueries class.

        Each instance owns a private in-memory DuckDB connection so that
        concurrent readers/writers - or simply multiple instances in the
        same process - never share global state. Combined with a table name
        that is unique per file path, this prevents one file's table from
        silently overwriting another's.

        Args:
            filepath (str or Path): Path to the file.
            lenient (bool): Whether to run in lenient mode.
        """
        self.filepath = Path(filepath)
        self.lenient = lenient
        self.database_table_name = self._set_database_table_name()
        # Opened lazily on first access via the ``connection`` property. Many
        # construction paths (reading a table name, or any polars/pyarrow read)
        # never touch DuckDB, so eagerly opening a connection here would waste
        # one on every such instance.
        self._connection = None
        # DuckDB's read_csv ``skip`` operates on physical lines and, unlike
        # Polars/PyArrow, does not natively ignore leading blank lines. Skip
        # every leading physical line up to (but not including) the header,
        # which ``header=true`` then consumes. See issue #85. ``max(..., 0)``
        # guards files with no header line (empty/blank), where the helper
        # returns 0 and DuckDB rejects a negative ``skip``.
        self.skip_rows = max(_count_leading_physical_lines_before_header(self.filepath) - 1, 0)
        # Tracks how the cached table was imported (None until first import,
        # then True/False for normalize_columns) so create_table can reuse the
        # table for matching calls and re-import only when the mode changes.
        self._imported_normalize_columns = None

    @property
    def _escaped_filepath_literal(self):
        """Return the file path as a single-quote-escaped SQL string literal.

        DuckDB string literals are single-quoted, so a path containing an
        apostrophe (e.g. ``o'hara.csv``) must double the quote to avoid
        producing broken SQL. Built once and reused by every import/export
        query that interpolates the path.
        """
        return Path(self.filepath).as_posix().replace("'", "''")

    @property
    def connection(self):
        """Return this instance's DuckDB connection, opening it on first use.

        The connection is created lazily so that constructing a
        ``DuckDBQueries`` (e.g. just to read ``database_table_name``) does not
        open a connection. The public attribute name is unchanged, so existing
        callers that do ``self.connection.sql(...)`` continue to work exactly
        as they did with the previous eager attribute.
        """
        if self._connection is None:
            self._connection = duckdb.connect(":memory:")
        return self._connection

    @cached_property
    def delimiter(self):
        """Get the delimiter."""
        return CSVDelimiter(self.filepath).delimiter

    @cached_property
    def quotechar(self):
        """Get the quote character."""
        try:
            return CSVDialect(self.filepath).quotechar
        except Exception:
            return '"'

    @staticmethod
    def _escape_sql_literal(value):
        """Escape a value for use inside a single-quoted SQL string literal.

        Doubling embedded apostrophes prevents an export path containing a
        quote (e.g. ``my'data.csv``) from terminating the literal early and
        producing broken ``COPY ... TO '...'`` SQL.

        Args:
            value (str): The raw string to place inside a literal.

        Returns:
            str: The escaped string (without the surrounding quotes).
        """
        return str(value).replace("'", "''")

    @staticmethod
    def _escape_identifier(name):
        """Escape a column name for use inside a double-quoted SQL identifier.

        DuckDB identifiers are double-quoted, so a quote in a column name (which
        can come straight from a CSV header cell) must be doubled or it
        terminates the identifier early and produces broken SQL.

        Args:
            name (str): The raw column name.

        Returns:
            str: The escaped name (without the surrounding double quotes).
        """
        return str(name).replace('"', '""')

    @staticmethod
    def _build_lenient_columns_param(columns):
        """Build the ``columns={...}`` struct for a lenient ``read_csv`` call.

        Each column name becomes a single-quoted SQL string literal, so any
        apostrophe in a header cell must be doubled or it terminates the literal
        early and produces broken SQL.

        Args:
            columns (list[str]): Header-derived column names.

        Returns:
            str: A DuckDB struct literal mapping each column to ``'VARCHAR'``.
        """
        entries = ", ".join(f"'{name.replace(chr(39), chr(39) * 2)}': 'VARCHAR'" for name in columns)
        return "{" + entries + "}"

    def close(self):
        """Close this instance's DuckDB connection.

        Provides deterministic disposal of the per-instance in-memory
        connection. Safe to call multiple times. There is intentionally no
        ``__del__``: relying on the garbage collector to close connections is
        unreliable - timing is non-deterministic and module globals may already
        be torn down at interpreter shutdown.

        Note: any lazy ``DuckDBPyRelation`` still referencing this connection
        becomes invalid once it is closed, so only call ``close()`` once you are
        done with results derived from this instance.
        """
        # Only close if a connection was actually opened, and reset the backing
        # attribute so a later access transparently reopens one (preserving the
        # always-usable contract of the previous eager attribute).
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            # The imported table lived in the in-memory database that was just
            # destroyed; reset the import cache so a reopened connection
            # re-imports instead of reusing a table that no longer exists.
            self._imported_normalize_columns = None

    def __enter__(self):
        """Enter a ``with`` block, returning this instance."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the connection on leaving the ``with`` block (normal or error).

        ``close()`` is idempotent and resets the lazy-connection/import-cache
        state, so the instance stays usable after the block - a later access
        transparently reopens. Returns ``None`` so any in-flight exception
        propagates.
        """
        self.close()

    def _format_filename_string(self):
        """Remove all non alphanumeric characters from the file stem."""
        return re.sub(r"[^a-zA-Z0-9]", "", self.filepath.stem)

    def _set_database_table_name(self):
        """Return a unique, deterministic table name for this file.

        The name combines a constant ``tbl_`` prefix with the sanitized file
        stem and a short hash of the file's absolute path. The hash is
        deterministic, so every DuckDBQueries instance created for the same
        file agrees on the same table name (callers can therefore still
        reference ``db_table`` in their queries). At the same time, two
        different files that happen to share a stem (e.g. ``dirA/data.csv``
        and ``dirB/data.csv``) resolve to distinct tables and cannot overwrite
        each other.

        The ``tbl_`` prefix guarantees the name starts with a letter. The table
        name is interpolated unquoted into every query, and DuckDB rejects an
        unquoted identifier that begins with a digit, so a numeric-leading
        filename (e.g. ``2026_sales.csv``) would otherwise produce a
        ParserException (issue #149).

        Returns:
            str: The unique table name.
        """
        stem = self._format_filename_string()
        path_hash = hashlib.sha1(str(self.filepath.resolve()).encode()).hexdigest()[:8]  # noqa: E501
        return f"tbl_{stem}_{path_hash}"

    def set_export_filename(self, default_filename, export_filename=None):
        """
        Return the export filename if provided, otherwise return the default.
        """
        # This method doesn't really fit the class but it's the only place it's
        # relevant.
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
        if self.lenient:
            from datagrunt.core.csv_io import CSVColumns

            cols = CSVColumns(self.filepath, delimiter=self.delimiter).columns
            cols_param = self._build_lenient_columns_param(cols)
            return f"""
                CREATE OR REPLACE TABLE {self.database_table_name} AS
                SELECT *
                FROM read_csv('{self._escaped_filepath_literal}',
                                delim='{self._escape_sql_literal(self.delimiter)}',
                                header=true,
                                columns={cols_param},
                                quote='{self.quotechar.replace("'", "''")}',
                                null_padding=true,
                                all_varchar=True,
                                auto_detect=false,
                                strict_mode=false,
                                skip={self.skip_rows});
                """
        # comment='' pins comment handling OFF despite auto_detect=true. The
        # sniffer otherwise infers comment='#' for files mixing a leading '#'
        # block with '#'-prefixed data rows and swallows the data rows
        # (issue #141). Leading comment blocks are already handled via skip,
        # and mid-file '#' lines are data on the polars/pyarrow engines.
        else:
            return f"""
                CREATE OR REPLACE TABLE {self.database_table_name} AS
                SELECT *
                FROM read_csv('{self._escaped_filepath_literal}',
                                auto_detect=true,
                                comment='',
                                delim='{self._escape_sql_literal(self.delimiter)}',
                                header=true,
                                quote='{self.quotechar.replace("'", "''")}',
                                null_padding=true,
                                all_varchar=True,
                                strict_mode=true,
                                skip={self.skip_rows});
                """

    def import_csv_query_normalize_columns(self):
        """
        Query to import a CSV file into a DuckDB table and normalize column
        names.

        Returns:
            str: The query to import the CSV file and normalize column names.
        """
        if self.lenient:
            from datagrunt.core.csv_io import CSVColumns

            cols = CSVColumns(self.filepath, delimiter=self.delimiter).columns
            cols_param = self._build_lenient_columns_param(cols)
            return f"""
                CREATE OR REPLACE TABLE {self.database_table_name} AS
                SELECT *
                FROM read_csv('{self._escaped_filepath_literal}',
                                delim='{self._escape_sql_literal(self.delimiter)}',
                                header=true,
                                columns={cols_param},
                                quote='{self.quotechar.replace("'", "''")}',
                                null_padding=true,
                                all_varchar=True,
                                auto_detect=false,
                                strict_mode=false,
                                normalize_names=true,
                                skip={self.skip_rows});
                """
        else:
            return f"""
                CREATE OR REPLACE TABLE {self.database_table_name} AS
                SELECT *
                FROM read_csv('{self._escaped_filepath_literal}',
                                auto_detect=true,
                                comment='',
                                delim='{self._escape_sql_literal(self.delimiter)}',
                                header=true,
                                quote='{self.quotechar.replace("'", "''")}',
                                null_padding=true,
                                all_varchar=True,
                                strict_mode=true,
                                normalize_names=true,
                                skip={self.skip_rows});
                """

    def select_from_duckdb_table(self):
        """Query to select from a DuckDB table."""
        return f"SELECT * FROM {self.database_table_name}"

    def sample_csv_query(self, limit):
        """Query to stream the first ``limit`` rows directly from the CSV file.

        Unlike :meth:`import_csv_query`, this does not create a table; the
        ``LIMIT`` lets DuckDB stop reading once enough rows are produced, so the
        whole file is never materialized just to return a small sample. The
        ``read_csv`` options mirror :meth:`import_csv_query` so the sampled rows
        and column headers match a full import.

        Args:
            limit (int): The maximum number of rows to return.

        Returns:
            str: The SQL query to stream the sample rows.
        """
        if self.lenient:
            from datagrunt.core.csv_io import CSVColumns

            cols = CSVColumns(self.filepath, delimiter=self.delimiter).columns
            cols_param = self._build_lenient_columns_param(cols)
            read_csv = f"""read_csv('{self._escaped_filepath_literal}',
                                delim='{self._escape_sql_literal(self.delimiter)}',
                                header=true,
                                columns={cols_param},
                                quote='{self.quotechar.replace("'", "''")}',
                                null_padding=true,
                                all_varchar=True,
                                auto_detect=false,
                                strict_mode=false,
                                skip={self.skip_rows})"""
        else:
            read_csv = f"""read_csv('{self._escaped_filepath_literal}',
                                auto_detect=true,
                                comment='',
                                delim='{self._escape_sql_literal(self.delimiter)}',
                                header=true,
                                quote='{self.quotechar.replace("'", "''")}',
                                null_padding=true,
                                all_varchar=True,
                                strict_mode=true,
                                skip={self.skip_rows})"""
        return f"SELECT * FROM {read_csv} LIMIT {limit}"

    def export_csv_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a CSV file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a CSV file.
        """
        filename = self._escape_sql_literal(self.set_export_filename(default_filename, export_filename))
        return f"COPY {self.database_table_name} TO '{filename}' (HEADER, DELIMITER ',');"  # noqa: E501

    def export_excel_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to an Excel file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to an Excel file.
        """
        filename = self._escape_sql_literal(self.set_export_filename(default_filename, export_filename))
        return f"""
            INSTALL spatial;
            LOAD spatial;
            COPY (SELECT * FROM {self.database_table_name})
            TO '{filename}'(FORMAT GDAL, DRIVER 'xlsx')
        """

    def export_json_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a JSON file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a JSON file.
        """
        filename = self._escape_sql_literal(self.set_export_filename(default_filename, export_filename))
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}' (ARRAY true)"  # noqa: E501

    def export_json_newline_delimited_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a JSON file with newline delimited.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a JSON file with newline
            delimited.
        """
        filename = self._escape_sql_literal(self.set_export_filename(default_filename, export_filename))
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'"  # noqa: E501

    def export_parquet_query(self, default_filename, export_filename=None):
        """
        Query to export a DuckDB table to a Parquet file.

        Args:
            default_filename (str): The default name of the output file.
            export_filename (str, optional): The name of the output file.

        Returns:
            str: The SQL query to export the table to a Parquet file.
        """
        filename = self._escape_sql_literal(self.set_export_filename(default_filename, export_filename))
        return f"COPY (SELECT * FROM {self.database_table_name}) TO '{filename}'(FORMAT PARQUET)"  # noqa: E501

    def update_and_normalize_column_names(self):
        """Query to update column names in a DuckDB table.

        DuckDB has a built-in function to normalize column names. However,
        the format of the column names from the native DuckDB function often
        differs from the class CSVColumnNameNormalizer. Because other types
        of engines throughout the ecosystem may have different conventions,
        this method uses the CSVColumnNameNormalizer class to ensure
        consistent naming conventions across different processing engines.

        The rename is done as a single atomic rebuild rather than a sequence of
        ``ALTER TABLE ... RENAME COLUMN`` statements. Sequential renames can
        collide mid-flight: if two distinct headers normalize to the same name
        (e.g. ``Col A`` and ``col_a`` both -> ``col_a``), an intermediate rename
        would target a name a not-yet-renamed column still holds, raising a
        CatalogException. Projecting every column to its new name in one
        ``CREATE OR REPLACE TABLE ... AS SELECT`` avoids any intermediate state.
        """
        self.connection.sql(self.import_csv_query())
        table_columns = self.connection.sql(f"SELECT * FROM {self.database_table_name} LIMIT 0").columns
        normalizer = CSVColumnNameNormalizer(self.filepath, columns=table_columns)
        # The normalizer guarantees the new names are unique among themselves,
        # so the projection below cannot produce a duplicate output column.
        # Double-quote-escape both identifiers; a quote in a header cell would
        # otherwise break the projection SQL (#82).
        projections = ", ".join(
            f'"{self._escape_identifier(old_name)}" AS "{self._escape_identifier(new_name)}"'
            for old_name, new_name in zip(table_columns, normalizer.columns_normalized)
        )
        rebuild_sql = (
            f"CREATE OR REPLACE TABLE {self.database_table_name} AS "
            f"SELECT {projections} FROM {self.database_table_name}"
        )
        self.connection.sql(rebuild_sql)

    def _table_is_current(self, normalize_columns):
        """Return True if the cached table already matches the requested import.

        The table name is deterministic per file path, so once the import has
        run on this instance's connection the table can be reused - but only
        when the requested ``normalize_columns`` mode matches how the table was
        originally imported. A mode change requires a fresh import.

        Args:
            normalize_columns (bool): The requested normalization mode.

        Returns:
            bool: Whether the existing table can be reused as-is.
        """
        return self._imported_normalize_columns == normalize_columns

    def create_table(self, normalize_columns=False):
        """Create a DuckDB table from the CSV file.

        The import is skipped when the table already exists on this instance's
        connection with the same ``normalize_columns`` mode: the table name is
        deterministic per file path, so a single import serves every subsequent
        matching call. This keeps repeated reads - notably ``query_data`` - from
        paying a full file import each time (issue #104). A change in
        ``normalize_columns`` triggers a fresh import so column names stay
        correct.

        Args:
            normalize_columns (bool): Whether to normalize column names.
        """
        if not self._table_is_current(normalize_columns):
            if self.lenient:
                _check_csv_ragged_and_warn(self.filepath, self.delimiter)
            if normalize_columns:
                self.update_and_normalize_column_names()
            else:
                self.connection.sql(self.import_csv_query())
            self._imported_normalize_columns = normalize_columns
        return self.connection.sql(self.select_from_duckdb_table()).execute()

    def sample_dataframe(self, limit, normalize_columns=False):
        """Return the first ``limit`` rows of the CSV as a Polars DataFrame.

        Streams the rows via :meth:`sample_csv_query` rather than importing the
        full file into a table, so memory and time stay bounded by the sample
        size instead of the file size. Column normalization is applied to the
        small result frame, matching the headers a full import would produce.

        Args:
            limit (int): The maximum number of rows to return.
            normalize_columns (bool): Whether to normalize column names.

        Returns:
            polars.DataFrame: The sampled rows.
        """
        if self.lenient:
            _check_csv_ragged_and_warn(self.filepath, self.delimiter)
        dataframe = self.connection.sql(self.sample_csv_query(limit)).pl()
        if normalize_columns:
            dataframe = self._normalize_dataframe_columns(dataframe)
        return dataframe

    def _normalize_dataframe_columns(self, dataframe):
        """Applies column name normalization to a Polars DataFrame.
        Args:
            dataframe (polars.DataFrame): The DataFrame to normalize.
        Returns:
            polars.DataFrame: The normalized DataFrame.
        """
        column_normalizer = CSVColumnNameNormalizer(self.filepath)
        normalized_mapping = {
            col: column_normalizer.columns_to_normalized_mapping.get(col, col) for col in dataframe.columns
        }
        return dataframe.rename(normalized_mapping)

    def sql_query_to_dataframe(self, sql_query, normalize_columns=False):
        """
        Query to convert a SQL query to a Polars DataFrame.

        Args:
            sql_query (str): The SQL query to execute.
            normalize_columns (optional, bool): Whether to normalize column
            names.

        Returns:
            polars.DataFrame: The resulting DataFrame.
        """
        if self.lenient:
            _check_csv_ragged_and_warn(self.filepath, self.delimiter)
        # Ensure the table is created with original column names for querying
        self.connection.sql(self.import_csv_query())
        # The raw import above replaced the table with original column names;
        # record that so create_table cannot wrongly reuse a "normalized" table.
        self._imported_normalize_columns = False

        # Execute the user's query
        result_df = self.connection.sql(sql_query).pl()

        if normalize_columns:
            result_df = self._normalize_dataframe_columns(result_df)

        return result_df
