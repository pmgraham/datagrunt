import unittest
import os
from pathlib import Path
import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../datagrunt')  # Add the parent directory to the search path
from datagrunt.csvfiles import CSVFile

class TestCSVFile(unittest.TestCase):

    def setUp(self):
        """Setup for test cases."""
        self.test_csv_path = Path(__file__).parent / 'test_data' / 'test.csv'
        self.test_empty_csv_path = Path(__file__).parent / 'test_data' / 'empty.csv'
        self.test_large_csv_path = Path(__file__).parent / 'test_data' / 'large.csv'
        self.test_csv_file = CSVFile(self.test_csv_path)
        self.test_empty_csv_file = CSVFile(self.test_empty_csv_path)
        self.test_large_csv_file = CSVFile(self.test_large_csv_path)

    def test_init(self):
        """Test initialization of CSVFile class."""
        self.assertEqual(self.test_csv_file.filepath, self.test_csv_path)
        self.assertEqual(self.test_csv_file.filename, 'test.csv')
        self.assertEqual(self.test_csv_file.extension, '.csv')
        self.assertEqual(self.test_csv_file.extension_string, 'csv')

    def test_select_from_table(self):
        """Test select_from_table method."""
        query = f"SELECT * FROM {self.test_csv_file.duckdb_instance.database_table_name} LIMIT 10"
        df = self.test_csv_file.select_from_table(query)
        self.assertEqual(len(df), 10)  # Check if 10 rows are returned

    def test_is_structured(self):
        """Test is_structured property."""
        self.assertTrue(self.test_csv_file.is_structured)

    def test_is_standard(self):
        """Test is_standard property."""
        self.assertTrue(self.test_csv_file.is_standard)

    def test_is_unstructured(self):
        """Test is_standard property."""
        self.assertFalse(self.test_csv_file.is_unstructured)

    def test_is_empty(self):
        """Test is_empty property."""
        self.assertFalse(self.test_csv_file.is_empty)
        self.assertTrue(self.test_empty_csv_file.is_empty)

    def test_is_large(self):
        """Test is_large property."""
        self.assertFalse(self.test_csv_file.is_large)
        self.assertTrue(self.test_large_csv_file.is_large)

    def test_get_attributes(self):
        """Test attributes method."""
        attributes = self.test_csv_file.get_attributes()
        self.assertIn('delimiter', attributes)
        self.assertIn('quotechar', attributes)
        self.assertIn('escapechar', attributes)
        self.assertIn('doublequote', attributes)
        self.assertIn('newline_delimiter', attributes)
        self.assertIn('skipinitialspace', attributes)
        self.assertIn('quoting', attributes)
        self.assertIn('columns', attributes)

    def test_get_row_count_with_header(self):
        """Test row_count_with_header method."""
        self.assertEqual(self.test_csv_file.get_row_count_with_header(), 2630)

    def test_get_row_count_without_header(self):
        """Test row_count_without_header method."""
        self.assertEqual(self.test_csv_file.get_row_count_without_header(), 2629)

    def test_get_columns(self):
        """Test columns_schema method."""
        schema = self.test_csv_file.get_columns()
        self.assertEqual(len(schema), 5)

    def test_get_columns_string(self):
        """Test columns_string method."""
        columns = self.test_csv_file.get_columns_string()
        self.assertEqual(columns, 'state,location,address,latitude,longitude')

    def test_get_columns_byte_string(self):
        """Test columns_byte_string method."""
        columns = self.test_csv_file.get_columns_byte_string()
        self.assertEqual(columns, b'state,location,address,latitude,longitude')

    def test_get_column_count(self):
        """Test column_count method."""
        self.assertEqual(self.test_csv_file.get_column_count(), 5)

    def test_delimiter(self):
        """Test delimiter method."""
        self.assertEqual(self.test_csv_file.delimiter, ',')

    def test_get_quotechar(self):
        """Test quotechar method."""
        self.assertEqual(self.test_csv_file.get_quotechar(), '"')

    def test_get_escapechar(self):
        """Test escapechar method."""
        self.assertEqual(self.test_csv_file.get_escapechar(), None)

    def test_get_newline_delimiter(self):
        """Test newline_delimiter method."""
        self.assertEqual(self.test_csv_file.get_newline_delimiter(), '\r\n')

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
        self.assertEqual(len(json_data), 2629)

    def test_to_json_newline_delimited(self):
        """Test to_json_new_line_delimited method."""
        jsonl_data = self.test_csv_file.to_json_newline_delimited()
        self.assertEqual(len(jsonl_data), 425897) # string object not a dict; hence the high number of chars

    def test_write_json(self):
        """Test write_json method."""
        os.chdir(Path(__file__).parent / 'test_data')
        output_path = 'output.json'
        self.test_csv_file.write_json()
        self.assertTrue(os.path.exists(output_path))
        os.remove(output_path)  # Clean up

    def test_write_json_newline_delimited(self):
        """Test write_json_newline_delimited method."""
        os.chdir(Path(__file__).parent / 'test_data')
        output_path = 'output.jsonl'
        self.test_csv_file.write_json_newline_delimited()
        self.assertTrue(os.path.exists(output_path))
        os.remove(output_path)  # Clean up

    def test_write_parquet(self):
        """Test write_parquet method."""
        os.chdir(Path(__file__).parent / 'test_data')
        output_path = 'output.parquet'
        self.test_csv_file.write_parquet()
        self.assertTrue(os.path.exists(output_path))
        os.remove(output_path)  # Clean up

    def test_write_excel(self):
        """Test write_excel method."""
        os.chdir(Path(__file__).parent / 'test_data')
        output_path = 'output.xlsx'
        self.test_csv_file.write_excel()
        self.assertTrue(os.path.exists(output_path))
        os.remove(output_path)  # Clean up

if __name__ == '__main__':
    unittest.main()
