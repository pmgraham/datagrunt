import pytest
import polars as pl
from src.datagrunt.core.engines import (
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine
)

class TestCSVReaderDuckDBEngine:
    def test_get_sample(self, sample_csv_path):
        engine = CSVReaderDuckDBEngine(sample_csv_path)
        # Since get_sample prints to console, we'll just verify it doesn't raise an error
        engine.get_sample()

    def test_to_dataframe(self, sample_csv_path):
        engine = CSVReaderDuckDBEngine(sample_csv_path)
        df = engine.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df.columns) == 3
        assert len(df) == 2

    def test_to_arrow_table(self, sample_csv_path):
        engine = CSVReaderDuckDBEngine(sample_csv_path)
        table = engine.to_arrow_table()
        assert table.num_columns == 3
        assert table.num_rows == 2

    def test_to_dicts(self, sample_csv_path):
        engine = CSVReaderDuckDBEngine(sample_csv_path)
        dicts = engine.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 2
        assert all(isinstance(d, dict) for d in dicts)

class TestCSVReaderPolarsEngine:
    # Similar tests as above but for Polars engine
    pass

class TestCSVWriterDuckDBEngine:
    def test_write_csv(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        output_path = tmp_path / "output.csv"
        engine.write_csv(str(output_path))
        assert output_path.exists()

    # Add tests for other write methods...

class TestCSVWriterPolarsEngine:
    # Similar tests as above but for Polars engine
    pass
