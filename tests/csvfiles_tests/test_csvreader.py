import pytest
import polars as pl
from src.datagrunt import CSVReader

import duckdb
from unittest.mock import patch

from src.datagrunt.core import (
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
)

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

    def test_csv_reader_invalid_engine(self, temp_csv_file):
        """Test that an invalid engine raises a ValueError."""
        with pytest.raises(ValueError) as exc_info:
            CSVReader(temp_csv_file, engine="invalid")
        assert "Reader engine 'invalid' is not 'duckdb' or 'polars'." in str(exc_info.value)

    def test_csv_reader_default_engine(self, temp_csv_file):
        """Test that the default engine is 'polars'."""
        reader = CSVReader(temp_csv_file)
        assert reader.engine == "polars"

    def test_csv_reader_set_reader_engine_polars(self, temp_csv_file):
        """Test that _set_reader_engine returns a Polars engine."""
        reader = CSVReader(temp_csv_file, engine="polars")
        engine = reader._set_reader_engine()
        assert isinstance(engine, CSVReaderPolarsEngine)

    def test_csv_reader_set_reader_engine_duckdb(self, temp_csv_file):
        """Test that _set_reader_engine returns a DuckDB engine."""
        reader = CSVReader(temp_csv_file, engine="duckdb")
        engine = reader._set_reader_engine()
        assert isinstance(engine, CSVReaderDuckDBEngine)

    def test_csv_reader_set_reader_engine_with_spaces(self, temp_csv_file):
        """Test that _set_reader_engine returns a DuckDB engine when spaces are in the engine name."""
        reader = CSVReader(temp_csv_file, engine=" duckdb ")
        engine = reader._set_reader_engine()
        assert isinstance(engine, CSVReaderDuckDBEngine)
        assert reader.engine == "duckdb"

    def test_csv_reader_get_sample(self, temp_csv_file, monkeypatch):
        """Test the get_sample method calls the engine's get_sample."""
        reader = CSVReader(temp_csv_file)
        with patch.object(CSVReaderPolarsEngine, "get_sample") as mock_get_sample:
            reader.get_sample()
            mock_get_sample.assert_called_once()

    def test_csv_reader_to_dataframe(self, temp_csv_file, monkeypatch):
        """Test that to_dataframe returns a Polars dataframe."""
        reader = CSVReader(temp_csv_file)
        with patch.object(CSVReaderPolarsEngine, "to_dataframe") as mock_to_dataframe:
            mock_to_dataframe.return_value = pl.DataFrame({"col1": [1, 2, 3]})
            df = reader.to_dataframe()
            mock_to_dataframe.assert_called_once()
            assert isinstance(df, pl.DataFrame)
            assert df.shape == (3,1)

    def test_csv_reader_to_arrow_table(self, temp_csv_file, monkeypatch):
        """Test that to_arrow_table returns a pyarrow table."""
        reader = CSVReader(temp_csv_file)
        with patch.object(CSVReaderPolarsEngine, "to_arrow_table") as mock_to_arrow_table:
            mock_to_arrow_table.return_value = pl.DataFrame({"col1": [1, 2, 3]}).to_arrow()
            table = reader.to_arrow_table()
            mock_to_arrow_table.assert_called_once()
            assert table.num_rows == 3

    def test_csv_reader_to_dicts(self, temp_csv_file, monkeypatch):
        """Test that to_dicts returns a list of dictionaries."""
        reader = CSVReader(temp_csv_file)
        with patch.object(CSVReaderPolarsEngine, "to_dicts") as mock_to_dicts:
            mock_to_dicts.return_value = [{"col1": 1}, {"col1": 2}, {"col1": 3}]
            dicts = reader.to_dicts()
            mock_to_dicts.assert_called_once()
            assert isinstance(dicts, list)
            assert len(dicts) == 3

    def test_csv_reader_query_data(self, temp_csv_file, monkeypatch):
        """Test that query_data returns a DuckDBPyRelation."""
        reader = CSVReader(temp_csv_file, engine="duckdb")
        result = reader.query_data("SELECT * FROM test")
        assert isinstance(result, duckdb.DuckDBPyRelation)

    def test_csv_reader_to_dataframe_empty_file(self, temp_empty_csv_file):
        """Test that to_dataframe handles empty files."""
        reader = CSVReader(temp_empty_csv_file)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()

    def test_csv_reader_to_dataframe_blank_file(self, temp_blank_csv_file):
        """Test that to_dataframe handles blank files."""
        reader = CSVReader(temp_blank_csv_file)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()

    def test_csv_reader_to_arrow_table_empty_file(self, temp_empty_csv_file):
        """Test that to_arrow_table handles empty files."""
        reader = CSVReader(temp_empty_csv_file)
        arrow_table = reader.to_arrow_table()
        assert arrow_table.num_rows == 0

    def test_csv_reader_to_arrow_table_blank_file(self, temp_blank_csv_file):
        """Test that to_arrow_table handles blank files."""
        reader = CSVReader(temp_blank_csv_file)
        arrow_table = reader.to_arrow_table()
        assert arrow_table.num_rows == 0

    def test_csv_reader_to_dicts_empty_file(self, temp_empty_csv_file):
        """Test that to_dicts handles empty files."""
        reader = CSVReader(temp_empty_csv_file)
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 0

    def test_csv_reader_to_dicts_blank_file(self, temp_blank_csv_file):
        """Test that to_dicts handles blank files."""
        reader = CSVReader(temp_blank_csv_file)
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 0

    def test_csv_reader_query_data_empty_file(self, temp_empty_csv_file):
        """Test that query_data handles empty files."""
        reader = CSVReader(temp_empty_csv_file, engine="duckdb")
        result = reader.query_data("SELECT * FROM test")
        assert isinstance(result, list)
        assert len(result) == 0

    def test_csv_reader_query_data_blank_file(self, temp_blank_csv_file):
        """Test that query_data handles blank files."""
        reader = CSVReader(temp_blank_csv_file, engine="duckdb")
        result = reader.query_data("SELECT * FROM test")
        assert isinstance(result, list)
        assert len(result) == 0
