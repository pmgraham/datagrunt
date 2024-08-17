import unittest
from pathlib import Path
import sys
sys.path.append('../')  # Add the parent directory to the search path
from libs.files import CSVFile

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

    def test_is_structured(self):
        """Test is_structured property."""
        self.assertTrue(self.test_csv_file.is_structured)

    def test_is_standard(self):
        """Test is_standard property."""
        self.assertTrue(self.test_csv_file.is_standard)

    def test_is_empty(self):
        """Test is_empty property."""
        self.assertFalse(self.test_csv_file.is_empty)
        self.assertTrue(self.test_empty_csv_file.is_empty)

    def test_is_large(self):
        """Test is_large property."""
        self.assertFalse(self.test_csv_file.is_large)
        self.assertTrue(self.test_large_csv_file.is_large)

    def test_attributes(self):
        """Test attributes method."""
        attributes = self.test_csv_file.attributes()
        self.assertIn('Columns', attributes)
        self.assertIn('Delimiter', attributes)
        self.assertIn('Quote', attributes)
        self.assertIn('Escape', attributes)
        self.assertIn('NewLineDelimiter', attributes)
        self.assertIn('HasHeader', attributes)

    def test_row_count_with_header(self):
        """Test row_count_with_header method."""
        self.assertEqual(self.test_csv_file.row_count_with_header(), 2630)

    def test_row_count_without_header(self):
        """Test row_count_without_header method."""
        self.assertEqual(self.test_csv_file.row_count_without_header(), 2629)

    def test_columns_schema(self):
        """Test columns_schema method."""
        schema = self.test_csv_file.columns_schema()
        self.assertEqual(len(schema), 5)

    def test_columns_string(self):
        """Test columns_string method."""
        columns = self.test_csv_file.columns_string()
        self.assertEqual(columns, 'state,location,address,latitude,longitude')

    def test_columns_byte_string(self):
        """Test columns_byte_string method."""
        columns = self.test_csv_file.columns_byte_string()
        self.assertEqual(columns, b'state,location,address,latitude,longitude')

    def test_column_count(self):
        """Test column_count method."""
        self.assertEqual(self.test_csv_file.column_count(), 5)

    def test_delimiter(self):
        """Test delimiter method."""
        self.assertEqual(self.test_csv_file.delimiter(), ',')

    def test_quotechar(self):
        """Test quotechar method."""
        self.assertEqual(self.test_csv_file.quotechar(), '"')

    def test_escapechar(self):
        """Test escapechar method."""
        self.assertEqual(self.test_csv_file.escapechar(), '"')

    def test_newline_delimiter(self):
        """Test newline_delimiter method."""
        self.assertEqual(self.test_csv_file.newline_delimiter(), '\\n')

    def test_has_header(self):
        """Test has_header method."""
        self.assertTrue(self.test_csv_file.has_header())

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
        jsonl_data = self.test_csv_file.to_json_new_line_delimited()
        self.assertEqual(len(jsonl_data), 425897) # string object not a dict; hence the high number of chars

if __name__ == '__main__':
    unittest.main()
