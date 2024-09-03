"""Unit tests for datagrunt."""

# standard library
import os
import sys
import unittest
from unittest.mock import patch
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../datagrunt')  # Add the parent directory to the search path

# third party libraries
import polars as pl

# local libraries
from datagrunt.csvfile import *
from datagrunt.core.queries import *
from datagrunt.core.logger import *
from datagrunt.core.fileproperties import *

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
        # df = df.select(pl.all().cast(pl.Utf8))
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

    def test_write_csv(self):
        """Test if write_csv exports the correct CSV file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_csv(out_filename='test.csv')
        self.assertTrue(os.path.exists('test.csv'))

    def test_write_excel(self):
        """Test if write_excel exports the correct Excel file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_excel(out_filename='test.xlsx')
        self.assertTrue(os.path.exists('test.xlsx'))

    def test_write_json(self):
        """Test if write_json exports the correct JSON file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_json(out_filename='test.json')
        self.assertTrue(os.path.exists('test.json'))

    def test_write_json_newline_delimited(self):
        """Test if write_json_newline_delimited exports the correct JSON newline delimited file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_json_newline_delimited(out_filename='test.ndjson')
        self.assertTrue(os.path.exists('test.ndjson'))

    def test_write_parquet(self):
        """Test if write_parquet exports the correct Parquet file."""
        csv_writer = CSVWriterDuckDBEngine(self.filepath)
        csv_writer.write_parquet(out_filename='test.parquet')
        self.assertTrue(os.path.exists('test.parquet'))

class TestCSVWriterPolarsEngine(unittest.TestCase):
    """Test class for CSVWriterPolarsEngine."""

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

    def test_write_csv(self):
        """Test if write_csv exports the correct CSV file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_csv(out_filename='test.csv')
        self.assertTrue(os.path.exists('test.csv'))

    def test_write_excel(self):
        """Test if write_excel exports the correct Excel file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_excel(out_filename='test.xlsx')
        self.assertTrue(os.path.exists('test.xlsx'))

    def test_write_json(self):
        """Test if write_json exports the correct JSON file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_json(out_filename='test.json')
        self.assertTrue(os.path.exists('test.json'))

    def test_write_json_newline_delimited(self):
        """Test if write_json_newline_delimited exports the correct JSON newline delimited file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_json_newline_delimited(out_filename='test.ndjson')
        self.assertTrue(os.path.exists('test.ndjson'))

    def test_write_parquet(self):
        """Test if write_parquet exports the correct Parquet file."""
        csv_writer = CSVWriterPolarsEngine(self.filepath)
        csv_writer.write_parquet(out_filename='test.parquet')
        self.assertTrue(os.path.exists('test.parquet'))

if __name__ == '__main__':
    unittest.main()
