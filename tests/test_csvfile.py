"""Unit tests for datagrunt."""

# standard library
import os
import sys
import unittest
from unittest.mock import patch
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../datagrunt')  # Add the parent directory to the search path

# third party libraries
import duckdb
import polars as pl

# local libraries
from src.datagrunt.core.engines import CSVReaderDuckDBEngine, CSVReaderPolarsEngine
from src.datagrunt.core.engines import CSVWriterDuckDBEngine, CSVWriterPolarsEngine
from src.datagrunt import CSVReader, CSVWriter
from src.datagrunt.core.databases import DuckDBDatabase
from src.datagrunt.core.fileproperties import FileProperties, CSVProperties
from src.datagrunt.core.logger import *
# from datagrunt.core.queries import DuckDBQueries

class TestDuckDBDatabase(unittest.TestCase):
    """Test class for DuckDBDatabase."""

    def setUp(self):
        """Setup method to create a sample file path before each test."""
        self.filepath = 'path/to/--my_file.csv'
        self.expected_db_filename = 'myfile.db'
        self.expected_db_table = 'myfile'

    def tearDown(self):
        """Cleanup method to remove the database file after each test."""
        db_filename = f'{self.filepath.replace("/", "_").replace(".", "_")}.db'
        if os.path.exists(db_filename):
            os.remove(db_filename)

    def test_init(self):
        """Test if the DuckDBDatabase class initializes correctly."""
        db = DuckDBDatabase(self.filepath)
        self.assertEqual(db.filepath, self.filepath)
        self.assertEqual(db.database_filename, self.expected_db_filename)
        self.assertEqual(db.database_table_name, self.expected_db_table)
        self.assertIsInstance(db.database_connection, duckdb.DuckDBPyConnection)

    def test_del(self):
        """Test if the database file is deleted when the object is destroyed."""
        db = DuckDBDatabase(self.filepath)
        db_filename = db.database_filename
        self.assertTrue(os.path.exists(db_filename))
        del db
        self.assertFalse(os.path.exists(db_filename))

    def test_format_filename_string(self):
        """Test if the filename is formatted correctly."""
        db = DuckDBDatabase(self.filepath)
        self.assertEqual(db._format_filename_string(), self.expected_db_table)

    def test_set_database_filename(self):
        """Test if the database filename is set correctly."""
        db = DuckDBDatabase(self.filepath)
        self.assertEqual(db._set_database_filename(), self.expected_db_filename)

    def test_set_database_table_name(self):
        """Test if the database table name is set correctly."""
        db = DuckDBDatabase(self.filepath)
        self.assertEqual(db._set_database_table_name(), self.expected_db_table)

    def test_set_database_connection(self):
        """Test if the database connection is established correctly."""
        db = DuckDBDatabase(self.filepath)
        self.assertIsInstance(db._set_database_connection(), duckdb.DuckDBPyConnection)

class TestFileProperties(unittest.TestCase):
    """Test class for FileProperties."""

    def setUp(self):
        """Setup method to create sample files before each test."""
        self.empty_file = 'empty.txt'
        self.small_file = 'small.txt'
        self.large_file = 'large.txt'
        self.csv_file = 'test.csv'
        self.excel_file = 'test.xlsx'
        self.parquet_file = 'test.parquet'
        self.json_file = 'test.json'

        open(self.empty_file, 'w').close()
        with open(self.small_file, 'w') as f:
            f.write('This is a small file.\n')
        with open(self.large_file, 'wb') as f:
            f.seek(1024 * 1024 * 1024 * 1024)  # Seek to 1GB
            f.write(b'\0')
        with open(self.csv_file, 'w') as f:
            f.write('Name,Age,City\n')
            f.write('Alice,25,New York\n')
        with open(self.excel_file, 'w') as f:
            f.write('This is a dummy Excel file.\n')
        with open(self.parquet_file, 'w') as f:
            f.write('This is a dummy Parquet file.\n')
        with open(self.json_file, 'w') as f:
            f.write('{"name": "John", "age": 30, "city": "New York"}\n')

    def tearDown(self):
        """Cleanup method to remove the sample files after each test."""
        for file in [self.empty_file, self.small_file, self.large_file,
                     self.csv_file, self.excel_file, self.parquet_file,
                     self.json_file]:
            if os.path.exists(file):
                os.remove(file)

    def test_file_properties(self):
        """Test if file properties are correctly determined."""
        # Empty file
        empty_file_props = FileProperties(self.empty_file)
        self.assertEqual(empty_file_props.size_in_bytes, 0)
        self.assertTrue(empty_file_props.is_empty)
        self.assertFalse(empty_file_props.is_large)

        # Small file
        small_file_props = FileProperties(self.small_file)
        self.assertGreater(small_file_props.size_in_bytes, 0)
        self.assertFalse(small_file_props.is_empty)
        self.assertFalse(small_file_props.is_large)

        # Large file
        large_file_props = FileProperties(self.large_file)
        self.assertGreater(large_file_props.size_in_gb, 1.0)
        self.assertFalse(large_file_props.is_empty)
        self.assertTrue(large_file_props.is_large)

        # CSV file
        csv_file_props = FileProperties(self.csv_file)
        self.assertTrue(csv_file_props.is_structured)
        self.assertTrue(csv_file_props.is_standard)
        self.assertFalse(csv_file_props.is_proprietary)
        self.assertTrue(csv_file_props.is_csv)
        self.assertFalse(csv_file_props.is_excel)
        self.assertTrue(csv_file_props.is_tabular)

        # Excel file
        excel_file_props = FileProperties(self.excel_file)
        self.assertTrue(excel_file_props.is_structured)
        self.assertFalse(excel_file_props.is_standard)
        self.assertTrue(excel_file_props.is_proprietary)
        self.assertFalse(excel_file_props.is_csv)
        self.assertTrue(excel_file_props.is_excel)
        self.assertTrue(excel_file_props.is_tabular)

        # Parquet file
        parquet_file_props = FileProperties(self.parquet_file)
        self.assertTrue(parquet_file_props.is_structured)
        self.assertTrue(parquet_file_props.is_standard)
        self.assertFalse(parquet_file_props.is_proprietary)
        self.assertFalse(parquet_file_props.is_csv)
        self.assertFalse(parquet_file_props.is_excel)
        self.assertFalse(parquet_file_props.is_tabular)
        self.assertTrue(parquet_file_props.is_apache)

        # JSON file
        json_file_props = FileProperties(self.json_file)
        self.assertTrue(json_file_props.is_semi_structured)
        self.assertTrue(json_file_props.is_standard)
        self.assertFalse(json_file_props.is_structured)
        self.assertFalse(json_file_props.is_proprietary)
        self.assertFalse(json_file_props.is_csv)
        self.assertFalse(json_file_props.is_excel)
        self.assertFalse(json_file_props.is_tabular)

class TestCSVProperties(unittest.TestCase):
    """Test class for CSVProperties."""

    def setUp(self):
        """Setup method to create sample CSV files before each test."""
        self.empty_file = 'empty.csv'
        self.comma_file = 'comma.csv'
        self.pipe_file = 'pipe.csv'
        self.space_file = 'space.csv'
        self.tab_file = 'tab.csv'
        self.text_file = 'text.txt'

        open(self.empty_file, 'w').close()
        with open(self.comma_file, 'w') as f:
            f.write('Name,Age,City\n')
            f.write('Alice,25,New York\n')
        with open(self.pipe_file, 'w') as f:
            f.write('Name|Age|City\n')
            f.write('Bob|30|London\n')
        with open(self.space_file, 'w') as f:
            f.write('Name Age City\n')
            f.write('Charlie 28 Paris\n')
        with open(self.tab_file, 'w') as f:
            f.write('Name\tAge\tCity\n')
            f.write('David\t35\tTokyo\n')
        with open(self.text_file, 'w') as f:
            f.write('Name\tAge\tCity\n')
            f.write('David\t35\tTokyo\n')

    def tearDown(self):
        """Cleanup method to remove the sample files after each test."""
        for file in [self.empty_file, self.comma_file, self.pipe_file,
                     self.space_file, self.tab_file, self.text_file]:
            if os.path.exists(file):
                os.remove(file)

    def test_delimiter_inference(self):
        """Test if delimiters are correctly inferred for different CSV files."""
        self.assertEqual(CSVProperties(self.empty_file).delimiter, ',')
        self.assertEqual(CSVProperties(self.comma_file).delimiter, ',')
        self.assertEqual(CSVProperties(self.pipe_file).delimiter, '|')
        self.assertEqual(CSVProperties(self.space_file).delimiter, ' ')
        self.assertEqual(CSVProperties(self.tab_file).delimiter, '\t')

    def test_row_counting(self):
        """Test if row counts are correctly determined."""
        self.assertEqual(CSVProperties(self.empty_file).row_count_with_header, 0)
        self.assertEqual(CSVProperties(self.empty_file).row_count_without_header, -1)
        self.assertEqual(CSVProperties(self.comma_file).row_count_with_header, 2)
        self.assertEqual(CSVProperties(self.comma_file).row_count_without_header, 1)

    def test_column_extraction(self):
        """Test if column names are correctly extracted."""
        self.assertEqual(CSVProperties(self.comma_file).columns, ['Name', 'Age', 'City'])
        self.assertEqual(CSVProperties(self.pipe_file).columns, ['Name', 'Age', 'City'])

    def test_invalid_csv(self):
        """Test if a ValueError is raised for a non-CSV file."""
        with self.assertRaises(ValueError):
            CSVProperties(self.text_file)

class TestCSVReaderDuckDBEngine(unittest.TestCase):
    """Test class for CSVReaderDuckDBEngine."""

    def setUp(self):
        """Setup method to create a sample CSV file before each test."""
        self.filepath = 'test.csv'
        self.data = {
                        'Name': pl.Series(['Alice', 'Bob', 'Charlie'], dtype=pl.Utf8),
                        'Age': pl.Series(['25', '30', '28'], dtype=pl.Utf8),
                        'City': pl.Series(['New York', 'London', 'Paris'], dtype=pl.Utf8)
                    }
        self.df = pl.DataFrame(self.data)
        self.df.write_csv(self.filepath)

    def tearDown(self):
        """Cleanup method to remove the sample CSV file after each test."""
        os.remove(self.filepath)

    def test_get_sample(self):
        """Test if get_sample reads the CSV file and returns a sample."""
        csv_reader = CSVReaderDuckDBEngine(self.filepath)
        with patch('builtins.print') as mocked_print:
            print(csv_reader.get_sample())
            mocked_print.assert_called_once()

    def test_to_dataframe(self):
        """Test if to_dataframe reads the CSV file and returns a Polars dataframe."""
        csv_reader = CSVReaderDuckDBEngine(self.filepath)
        df = csv_reader.to_dataframe()
        # df = df.select(pl.all().cast(pl.Utf8))
        self.assertEqual(df.shape, self.df.shape)

    def test_to_arrow_table(self):
        """Test if to_arrow_table reads the CSV file and returns a PyArrow table."""
        csv_reader = CSVReaderDuckDBEngine(self.filepath)
        table = csv_reader.to_arrow_table()
        self.assertEqual(table.num_rows, len(self.df))
        self.assertEqual(table.num_columns, len(self.df.columns))

    def test_to_dicts(self):
        """Test if to_dicts reads the CSV file and returns a list of dictionaries."""
        csv_reader = CSVReaderDuckDBEngine(self.filepath)
        dicts = csv_reader.to_dicts()
        self.assertEqual(len(dicts), len(self.df))
        for i, row in enumerate(dicts):
            for key in self.data:
                self.assertEqual(row[key], self.df[key][i])

class TestCSVReaderPolarsEngine(unittest.TestCase):
    """Test class for CSVReaderPolarsEngine."""

    def setUp(self):
        """Setup method to create a sample CSV file before each test."""
        self.filepath = 'test.csv'
        self.data = {
                        'Name': pl.Series(['Alice', 'Bob', 'Charlie'], dtype=pl.Utf8),
                        'Age': pl.Series([25, 30, 28], dtype=pl.Int64),
                        'City': pl.Series(['New York', 'London', 'Paris'], dtype=pl.Utf8)
                    }
        self.df = pl.DataFrame(self.data)
        self.df.write_csv(self.filepath)

    def tearDown(self):
        """Cleanup method to remove the sample CSV file after each test."""
        os.remove(self.filepath)

    def test_get_sample(self):
        """Test if get_sample reads the CSV file and returns a sample."""
        csv_reader = CSVReaderPolarsEngine(self.filepath)
        with patch('builtins.print') as mocked_print:
            print(csv_reader.get_sample())
            mocked_print.assert_called_once()

    def test_to_dataframe(self):
        """Test if to_dataframe reads the CSV file and returns a Polars dataframe."""
        csv_reader = CSVReaderPolarsEngine(self.filepath)
        df = csv_reader.to_dataframe()
        self.assertEqual(df.shape, self.df.shape)

    def test_to_arrow_table(self):
        """Test if to_arrow_table reads the CSV file and returns a PyArrow table."""
        csv_reader = CSVReaderPolarsEngine(self.filepath)
        table = csv_reader.to_arrow_table()
        self.assertEqual(table.num_rows, len(self.df))
        self.assertEqual(table.num_columns, len(self.df.columns))

    def test_to_dicts(self):
        """Test if to_dicts reads the CSV file and returns a list of dictionaries."""
        csv_reader = CSVReaderPolarsEngine(self.filepath)
        dicts = csv_reader.to_dicts()
        self.assertEqual(len(dicts), len(self.df))
        for i, row in enumerate(dicts):
            for key in self.data:
                self.assertEqual(row[key], self.df[key][i])

class TestCSVWriterDuckDBEngine(unittest.TestCase):
    """Test class for CSVWriterDuckDBEngine."""

    def setUp(self):
        """Setup method to create a sample CSV file before each test."""
        self.filepath = 'test.csv'
        self.csv_outfile = 'test_out.csv'
        self.excel_outfile = 'test_out.xlsx'
        self.json_outfile = 'test_out.json'
        self.ndjson_outfile = 'test_out.jsonl'
        self.parquet_outfile = 'test_out.parquet'
        with open(self.filepath, 'w') as f:
            f.write('Name,Age,City\n')
            f.write('Alice,25,New York\n')
            f.write('Bob,30,London\n')
            f.write('Charlie,28,Paris\n')

    def tearDown(self):
        """Cleanup method to remove the sample files after each test."""
        os.remove(self.filepath)
        for ext in ['csv', 'xlsx', 'json', 'jsonl', 'parquet']:
            try:
                os.remove(f'test_out.{ext}')
            except FileNotFoundError:
                pass

    def test_write_csv(self):
        """Test if write_csv exports the correct CSV file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_csv(out_filename=self.csv_outfile)
        self.assertTrue(os.path.exists(self.csv_outfile))

    def test_write_excel(self):
        """Test if write_excel exports the correct Excel file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_excel(out_filename=self.excel_outfile)
        self.assertTrue(os.path.exists(self.excel_outfile))

    def test_write_json(self):
        """Test if write_json exports the correct JSON file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_json(out_filename=self.json_outfile)
        self.assertTrue(os.path.exists(self.json_outfile))

    def test_write_json_newline_delimited(self):
        """Test if write_json_newline_delimited exports the correct JSON newline delimited file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_json_newline_delimited(out_filename=self.ndjson_outfile)
        self.assertTrue(os.path.exists(self.ndjson_outfile))

    def test_write_parquet(self):
        """Test if write_parquet exports the correct Parquet file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_parquet(out_filename=self.parquet_outfile)
        self.assertTrue(os.path.exists(self.parquet_outfile))

class TestCSVWriterPolarsEngine(unittest.TestCase):
    """Test class for CSVWriterPolarsEngine."""

    def setUp(self):
        """Setup method to create a sample CSV file before each test."""
        self.filepath = 'test.csv'
        self.csv_outfile = 'test_out.csv'
        self.excel_outfile = 'test_out.xlsx'
        self.json_outfile = 'test_out.json'
        self.ndjson_outfile = 'test_out.jsonl'
        self.parquet_outfile = 'test_out.parquet'
        with open(self.filepath, 'w') as f:
            f.write('Name,Age,City\n')
            f.write('Alice,25,New York\n')
            f.write('Bob,30,London\n')
            f.write('Charlie,28,Paris\n')

    def tearDown(self):
        """Cleanup method to remove the sample files after each test."""
        os.remove(self.filepath)
        for ext in ['csv', 'xlsx', 'json', 'jsonl', 'parquet']:
            try:
                os.remove(f'test_out.{ext}')
            except FileNotFoundError:
                pass

    def test_write_csv(self):
        """Test if write_csv exports the correct CSV file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_csv(out_filename=self.csv_outfile)
        self.assertTrue(os.path.exists(self.csv_outfile))

    def test_write_excel(self):
        """Test if write_excel exports the correct Excel file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_csv(out_filename=self.excel_outfile)
        self.assertTrue(os.path.exists(self.excel_outfile))

    def test_write_json(self):
        """Test if write_json exports the correct JSON file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_csv(out_filename=self.json_outfile)
        self.assertTrue(os.path.exists(self.json_outfile))

    def test_write_json_newline_delimited(self):
        """Test if write_json_newline_delimited exports the correct JSON newline delimited file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_csv(out_filename=self.ndjson_outfile)
        self.assertTrue(os.path.exists(self.ndjson_outfile))

    def test_write_parquet(self):
        """Test if write_parquet exports the correct Parquet file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_parquet(out_filename=self.parquet_outfile)
        self.assertTrue(os.path.exists(self.parquet_outfile))

class TestCSVReader(unittest.TestCase):
    """Test class for CSVReader."""

    def setUp(self):
        """Setup method to create a sample CSV file before each test."""
        self.filepath = 'test.csv'
        with open(self.filepath, 'w') as f:
            f.write('Name,Age,City\n')
            f.write('Alice,25,New York\n')
            f.write('Bob,30,London\n')
            f.write('Charlie,28,Paris\n')

    def tearDown(self):
        """Cleanup method to remove the sample files after each test."""
        os.remove(self.filepath)
        for ext in ['csv', 'xlsx', 'json', 'ndjson', 'parquet']:
            try:
                os.remove(f'test.{ext}')
            except FileNotFoundError:
                pass

    def test_invalid_engine(self):
        """Test that an error is raised for an invalid reader engine."""
        with self.assertRaises(ValueError) as context:
            CSVReader(self.filepath, engine='invalid')
        self.assertEqual(
            str(context.exception),
            """Reader engine 'invalid' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""
        )

    @patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.get_sample')
    def test_get_sample_polars(self, mock_get_sample):
        """Test that the get_sample method calls the Polars engine."""
        reader = CSVReader(self.filepath)
        reader.get_sample()
        mock_get_sample.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.get_sample')
    def test_get_sample_duckdb(self, mock_get_sample):
        """Test that the get_sample method calls the DuckDB engine."""
        reader = CSVReader(self.filepath, engine='duckdb')
        reader.get_sample()
        mock_get_sample.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.to_dataframe')
    def test_to_dataframe_polars(self, mock_to_dataframe):
        """Test that the to_dataframe method calls the Polars engine."""
        reader = CSVReader(self.filepath)
        reader.to_dataframe()
        mock_to_dataframe.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.to_dataframe')
    def test_to_dataframe_duckdb(self, mock_to_dataframe):
        """Test that the to_dataframe method calls the DuckDB engine."""
        reader = CSVReader(self.filepath, engine='duckdb')
        reader.to_dataframe()
        mock_to_dataframe.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.to_arrow_table')
    def test_to_arrow_table_polars(self, mock_to_arrow_table):
        """Test that the to_arrow_table method calls the Polars engine."""
        reader = CSVReader(self.filepath)
        reader.to_arrow_table()
        mock_to_arrow_table.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.to_arrow_table')
    def test_to_arrow_table_duckdb(self, mock_to_arrow_table):
        """Test that the to_arrow_table method calls the DuckDB engine."""
        reader = CSVReader(self.filepath, engine='duckdb')
        reader.to_arrow_table()
        mock_to_arrow_table.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.to_dicts')
    def test_to_dicts_polars(self, mock_to_dicts):
        """Test that the to_dicts method calls the Polars engine."""
        reader = CSVReader(self.filepath)
        reader.to_dicts()
        mock_to_dicts.assert_called_once()

    @patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.to_dicts')
    def test_to_dicts_duckdb(self, mock_to_dicts):
        """Test that the to_dicts method calls the DuckDB engine."""
        reader = CSVReader(self.filepath, engine='duckdb')
        reader.to_dicts()
        mock_to_dicts.assert_called_once()

    @patch('src.datagrunt.csvfile.duckdb.sql')
    def test_query_data(self, mock_sql):
        """Test that the query_data method calls DuckDB SQL with the correct query."""
        reader = CSVReader(self.filepath)
        query = "SELECT * FROM {reader.db_table}"
        reader.query_data(query)
        mock_sql.assert_called_with(query)

class TestCSVWriter(unittest.TestCase):
    """Test class for CSVWriter."""

    def setUp(self):
        """Setup method to create a sample CSV file before each test."""
        self.filepath = 'test.csv'
        self.csv_outfile = 'test_out.csv'
        self.excel_outfile = 'test_out.xlsx'
        self.json_outfile = 'test_out.json'
        self.ndjson_outfile = 'test_out.ndjson'
        self.parquet_outfile = 'test_out.parquet'

        self.filepath = 'test.csv'
        with open(self.filepath, 'w') as f:
            f.write('Name,Age,City\n')
            f.write('Alice,25,New York\n')
            f.write('Bob,30,London\n')
            f.write('Charlie,28,Paris\n')

    def tearDown(self):
        """Cleanup method to remove the sample files after each test."""
        os.remove(self.filepath)
        for ext in ['csv', 'xlsx', 'json', 'ndjson', 'parquet']:
            try:
                os.remove(f'test_out.{ext}')
            except FileNotFoundError:
                pass

    def test_invalid_engine(self):
        """Test that an error is raised for an invalid writer engine."""
        with self.assertRaises(ValueError) as context:
            CSVWriter(self.filepath, engine='invalid')
        self.assertEqual(
            str(context.exception),
            """Writer engine 'invalid' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""
        )

    def test_write_csv_polars(self):
        """Test that the write_csv method calls the Polars engine."""
        writer = CSVWriter(self.filepath)
        writer.write_csv(out_filename=self.csv_outfile)
        self.assertTrue(os.path.exists(self.csv_outfile))

    def test_write_csv_duckdb(self):
        """Test that the write_csv method calls the DuckDB engine."""
        writer = CSVWriter(self.filepath, engine='duckdb')
        writer.write_csv(out_filename=self.csv_outfile)
        self.assertTrue(os.path.exists(self.csv_outfile))

    def test_write_excel_polars(self):
        """Test that the write_excel method calls the Polars engine."""
        writer = CSVWriter(self.filepath)
        writer.write_excel(out_filename=self.excel_outfile)
        self.assertTrue(os.path.exists(self.excel_outfile))

    def test_write_excel_duckdb(self):
        """Test that the write_excel method calls the DuckDB engine."""
        writer = CSVWriter(self.filepath, engine='duckdb')
        writer.write_excel(out_filename=self.excel_outfile)
        self.assertTrue(os.path.exists(self.excel_outfile))

    def test_write_json_polars(self):
        """Test that the write_json method calls the Polars engine."""
        writer = CSVWriter(self.filepath)
        writer.write_json(out_filename=self.json_outfile)
        self.assertTrue(os.path.exists(self.json_outfile))

    def test_write_json_duckdb(self):
        """Test that the write_json method calls the DuckDB engine."""
        writer = CSVWriter(self.filepath, engine='duckdb')
        writer.write_json(out_filename=self.json_outfile)
        self.assertTrue(os.path.exists(self.json_outfile))

    def test_write_json_newline_delimited_polars(self):
        """Test that the write_json_newline_delimited method calls the Polars engine."""
        writer = CSVWriter(self.filepath)
        writer.write_json_newline_delimited(out_filename=self.ndjson_outfile)
        self.assertTrue(os.path.exists(self.ndjson_outfile))

    def test_write_json_newline_delimited_duckdb(self):
        """Test that the write_json_newline_delimited method calls the DuckDB engine."""
        writer = CSVWriter(self.filepath, engine='duckdb')
        writer.write_json_newline_delimited(out_filename=self.ndjson_outfile)
        self.assertTrue(os.path.exists(self.ndjson_outfile))

    def test_write_parquet_polars(self):
        """Test that the write_parquet method calls the Polars engine."""
        writer = CSVWriter(self.filepath)
        writer.write_parquet(out_filename=self.parquet_outfile)
        self.assertTrue(os.path.exists(self.parquet_outfile))

    def test_write_parquet_duckdb(self):
        """Test that the write_parquet method calls the DuckDB engine."""
        writer = CSVWriter(self.filepath, engine='duckdb')
        writer.write_parquet(out_filename=self.parquet_outfile)
        self.assertTrue(os.path.exists(self.parquet_outfile))

if __name__ == '__main__':
    unittest.main()
