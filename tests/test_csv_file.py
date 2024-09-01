import unittest
import os
from pathlib import Path
import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../datagrunt')  # Add the parent directory to the search path
from datagrunt.csvfile import CSVReader
from datagrunt.core.queries import DuckDBQueries

class TestCSVReader(unittest.TestCase):

    def setUp(self):
        """Setup for test cases."""
        self.test_csv_path = Path(__file__).parent / 'data' / 'test.csv'
        self.test_csv_file = CSVReader(self.test_csv_path)

    def test_init(self):
        """Test initialization of CSVReader class."""
        self.assertEqual(self.test_csv_file.filepath, self.test_csv_path)
        self.assertEqual(self.test_csv_file.filename, 'test.csv')
        self.assertEqual(self.test_csv_file.extension, '.csv')
        self.assertEqual(self.test_csv_file.extension_string, 'csv')

    def test_to_dataframe(self):
        """Test to_dataframe method."""
        df = self.test_csv_file.to_dataframe()
        self.assertEqual(df.shape, (2629, 5))
        self.assertEqual(df.columns, ['state', 'location', 'address', 'latitude', 'longitude'])

    def test_to_dicts(self):
        """Test to_dicts method."""
        dicts = self.test_csv_file.to_dicts()
        self.assertEqual(len(dicts), 2629)

    def test_to_json(self):
        """Test to_json method."""
        json_data = self.test_csv_file.to_json()
        self.assertEqual(len(json_data), 425898)

    def test_to_json_newline_delimited(self):
        """Test to_json_newline_delimited method."""
        jsonl_data = self.test_csv_file.to_json_newline_delimited()
        self.assertEqual(len(jsonl_data), 425897) # string object not a dict; hence the high number of chars

if __name__ == '__main__':
    unittest.main()
