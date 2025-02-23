import os
import pytest
import polars as pl
from src.datagrunt import CSVReader, CSVWriter

class TestCSVReader:
    def test_initialization(self, sample_csv):
        """Test basic initialization of CSVReader."""
        reader = CSVReader(sample_csv)
        assert isinstance(reader, CSVReader)
        assert reader.filepath == sample_csv
        assert reader.delimiter == ','
        assert reader.engine == 'polars'

    def test_invalid_engine(self, sample_csv):
        """Test initialization with invalid engine."""
        with pytest.raises(ValueError):
            CSVReader(sample_csv, engine='invalid')

    def test_to_dataframe(self, sample_csv):
        """Test conversion to dataframe."""
        reader = CSVReader(sample_csv)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ['name', 'age', 'city']

    def test_empty_file(self, empty_csv):
        """Test handling of empty file."""
        reader = CSVReader(empty_csv)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_to_dicts(self, sample_csv):
        """Test conversion to list of dictionaries."""
        reader = CSVReader(sample_csv)
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 2
        assert all(isinstance(d, dict) for d in dicts)
        assert dicts[0]['name'] == 'John'

    def test_query_data(self, sample_csv):
        """Test query_data method."""
        reader = CSVReader(sample_csv, engine='duckdb')
        query = f"SELECT * FROM {reader.db_table} WHERE age > '25'"
        result = reader.query_data(query)
        assert len(result) == 1

class TestCSVWriter:
    def test_initialization(self, sample_csv):
        """Test basic initialization of CSVWriter."""
        writer = CSVWriter(sample_csv)
        assert isinstance(writer, CSVWriter)
        assert writer.filepath == sample_csv
        assert writer.engine == 'duckdb'

    def test_invalid_engine(self, sample_csv):
        """Test initialization with invalid engine."""
        with pytest.raises(ValueError):
            CSVWriter(sample_csv, engine='invalid')

    def test_write_csv(self, sample_csv):
        """Test writing to CSV."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.csv"
        writer.write_csv(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_json(self, sample_csv):
        """Test writing to JSON."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.json"
        writer.write_json(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_parquet(self, sample_csv):
        """Test writing to Parquet."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.parquet"
        writer.write_parquet(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_excel(self, sample_csv):
        """Test writing to Excel."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.xlsx"
        writer.write_excel(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_json_newline_delimited(self, sample_csv):
        """Test writing to newline-delimited JSON."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.jsonl"
        writer.write_json_newline_delimited(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)
