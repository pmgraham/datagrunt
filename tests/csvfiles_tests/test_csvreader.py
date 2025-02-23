import pytest
import polars as pl
from src.datagrunt import CSVReader

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
