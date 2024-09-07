"""Module for reading CSV files and converting CSV files to different standard file formats."""

# standard library

# third party libraries
import duckdb
import polars as pl

# local libraries
from datagrunt.core.fileproperties import CSVProperties
from datagrunt.core.queries import DuckDBQueries
from datagrunt.core.logger import show_large_file_warning, show_dataframe_sample

class CSVReaderDuckDBEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name

    def _read_csv(self):
        """Reads a CSV using DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation.
        """
        return duckdb.read_csv(self.filepath,
                               delimiter=self.delimiter,
                               null_padding=True,
                               all_varchar=True
                            )

    def get_sample(self):
        """Return a sample of the CSV file."""
        self._read_csv().show()

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return self._read_csv().pl()

    def to_arrow_table(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A PyArrow table.
        """
        arrow_table = self._read_csv().arrow()
        return arrow_table

    def to_dicts(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe().to_dicts()
        return dicts

class CSVReaderPolarsEngine(CSVProperties):
    """Class to read CSV files and convert CSV files powered by Polars."""

    def get_sample(self):
        """Return a sample of the CSV file."""
        df = pl.read_csv(self.filepath,
                         separator=self.delimiter,
                         truncate_ragged_lines=True,
                         n_rows=self.DATAFRAME_SAMPLE_ROWS
                        )
        show_dataframe_sample(df)

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_large:
            show_large_file_warning()
        return pl.read_csv(self.filepath,
                           separator=self.delimiter)

    def to_arrow_table(self):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        df = self.to_dataframe().to_arrow()
        return df

    def to_dicts(self):
        """Converts CSV to a Python list of dictionaies.

        Returns:
            A list of dictionaries.
        """
        dicts = self.to_dataframe().to_dicts()
        return dicts

class CSVWriterDuckDBEngine(CSVProperties):
    """Class to convert CSV files to various other supported file types powered by DuckDB."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        super().__init__(filepath)
        self.queries = DuckDBQueries(self.filepath)

    def write_csv(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_csv_query(out_filename))

    def write_excel(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_excel_query(out_filename))

    def write_json(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_json_query(out_filename))

    def write_json_newline_delimited(self, out_filename=None):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.sql(self.queries.import_csv_query(self.delimiter))
        duckdb.sql(self.queries.export_json_newline_delimited_query(out_filename))

    def write_parquet(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        duckdb.execute(self.queries.import_csv_query(self.delimiter))
        duckdb.execute(self.queries.export_parquet_query(out_filename))

class CSVWriterPolarsEngine(CSVProperties):
    """Class to write CSVs to other file formats powered by Polars."""

    def _set_out_filename(self, default_filename, out_filename=None):
        """Evaluate if a filename is passed in and if not, return default filename."""
        if out_filename:
            filename = out_filename
        else:
            filename = default_filename
        return filename

    def write_csv(self, out_filename=None):
        """Export a Polars dataframe to a CSV file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.CSV_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_csv(filename)

    def write_excel(self, out_filename=None):
        """Export a Polars dataframe to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.EXCEL_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_excel(filename)

    def write_json(self, out_filename=None):
        """Export a Polars dataframe to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_json(filename)

    def write_json_newline_delimited(self, out_filename=None):
        """Export a Polars dataframe to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.JSON_NEWLINE_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_ndjson(filename)

    def write_parquet(self, out_filename=None):
        """Export a Polars dataframe to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        filename = self._set_out_filename(self.PARQUET_OUT_FILENAME, out_filename)
        df = CSVReaderPolarsEngine(self.filepath).to_dataframe()
        df.write_parquet(filename)

class CSVReader(CSVProperties):
    """Class to unify the interface for reading CSV files."""

    READER_ENGINES = ['duckdb', 'polars']
    VALUE_ERROR_MESSAGE = """Reader engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

    def __init__(self, filepath, engine='polars'):
        """Initialize the CSV Reader class.

        Args:
            filepath (str): Path to the file to read.
            engine (str, default 'polars'): Determines which reader engine class to instantiate.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')
        if self.engine not in self.READER_ENGINES:
            raise ValueError(self.VALUE_ERROR_MESSAGE.format(engine=self.engine))

    def _set_reader_engine(self):
        """Sets the CSV reader engine as either DuckDB or Polars.
           Default engine is Polars.
        """
        if self.engine != 'polars':
            return CSVReaderDuckDBEngine(self.filepath)
        else:
            return CSVReaderPolarsEngine(self.filepath)

    def get_sample(self):
        """Return a sample of the CSV file."""
        self._set_reader_engine().get_sample()

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        return self._set_reader_engine().to_dataframe()

    def to_arrow_table(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A PyArrow table.
        """
        return self._set_reader_engine().to_arrow_table()

    def to_dicts(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A list of dictionaries.
        """
        return self._set_reader_engine().to_dicts()

    def query_data(self, sql_query):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)

        Example if Polars Engine:
            dg = CSVReaderPolarsEngine('myfile.csv')
            df = dg.to_dataframe()
            query = "SELECT col1, col2 FROM df" # no f string required to query from dataframe
            dg.query_csv_data(query)
        """
        if self.engine == 'duckdb':
            queries = DuckDBQueries(self.filepath)
            duckdb.sql(queries.import_csv_query(self.delimiter))
            return duckdb.sql(sql_query)
        else:
            return duckdb.sql(sql_query)

class CSVWriter(CSVProperties):
    """Class to unify the interface for converting CSV files to various other supported file types."""

    WRITER_ENGINES = ['duckdb', 'polars']
    VALUE_ERROR_MESSAGE = """Reader engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

    def __init__(self, filepath, engine='duckdb'):
        """Initialize the CSV Writer class.

        Args:
            filepath (str): Path to the file to write.
            engine (str, default 'duckdb'): Determines which writer engine class to instantiate.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')
        if self.engine not in self.READER_ENGINES:
            raise ValueError(self.VALUE_ERROR_MESSAGE.format(engine=self.engine))

    def _set_writer_engine(self, engine):
        """Sets the CSV reader engine as either DuckDB or Polars.
           Default engine is Polars.
        """
        if engine != 'duckdb':
            return CSVWriterPolarsEngine(self.filepath)
        else:
            return CSVWriterDuckDBEngine(self.filepath)

    def write_csv(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
        return self._set_writer_engine(self.engine).write_csv(out_filename)

    def write_excel(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine(self.engine).write_excel(out_filename)

    def write_json(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine(self.engine).write_json(out_filename)

    def write_json_newline_delimited(self, out_filename=None):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine(self.engine).write_json_newline_delimited(out_filename)

    def write_parquet(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine(self.engine).write_parquet(out_filename)
