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
    def test_get_sample(self, sample_csv_path):
        engine = CSVReaderPolarsEngine(sample_csv_path)
        # Since get_sample prints to console, we'll just verify it doesn't raise an error
        engine.get_sample()

    def test_to_dataframe(self, sample_csv_path):
        engine = CSVReaderPolarsEngine(sample_csv_path)
        df = engine.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df.columns) == 3
        assert len(df) == 2

    def test_to_arrow_table(self, sample_csv_path):
        engine = CSVReaderPolarsEngine(sample_csv_path)
        table = engine.to_arrow_table()
        assert table.num_columns == 3
        assert table.num_rows == 2

    def test_to_dicts(self, sample_csv_path):
        engine = CSVReaderPolarsEngine(sample_csv_path)
        dicts = engine.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 2
        assert all(isinstance(d, dict) for d in dicts)

class TestCSVWriterDuckDBEngine:

    def test_write_csv(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        output_path = tmp_path / "test.csv"
        engine.write_csv(str(output_path))
        assert output_path.exists()

    def test_write_csv_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_csv()

    def test_write_parquet(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.parquet"
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_parquet(str(output_path))

        # Verify the file was created
        df = pl.read_parquet(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_parquet_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_parquet()

    def test_write_json(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.json"
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_json(str(output_path))

        # Verify the file was created
        df = pl.read_json(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_json_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_json()

    def test_write_newline_delimited_json(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.jsonl"
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_json_newline_delimited(str(output_path))

        # Verify the file was created
        df = pl.read_ndjson(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_newline_delimited_json_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_json_newline_delimited()

    def test_write_excel(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.xlsx"
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_excel(str(output_path))

        # Verify the file was created
        df = pl.read_excel(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_excel_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterDuckDBEngine(sample_csv_path)
        engine.write_excel()

class TestCSVWriterPolarsEngine:

    def test_write_csv(self, sample_csv_path, tmp_path):
        engine = CSVWriterPolarsEngine(sample_csv_path)
        output_path = tmp_path / "test.csv"
        engine.write_csv(str(output_path))
        assert output_path.exists()

    def test_write_csv_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_csv()

    def test_write_parquet(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.parquet"
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_parquet(str(output_path))

        # Verify the file was created
        df = pl.read_parquet(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_parquet_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_parquet()

    def test_write_json(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.json"
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_json(str(output_path))

        # Verify the file was created
        df = pl.read_json(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_json_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_json()

    def test_write_newline_delimited_json(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.jsonl"
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_json_newline_delimited(str(output_path))

        # Verify the file was created
        df = pl.read_ndjson(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_newline_delimited_json_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_json_newline_delimited()

    def test_write_excel(self, sample_csv_path, tmp_path):
        output_path = tmp_path / "test.xlsx"
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_excel(str(output_path))

        # Verify the file was created
        df = pl.read_excel(output_path)
        assert len(df.columns) == 3
        assert output_path.exists()

    def test_write_excel_default(self, sample_csv_path, tmp_path):
        engine = CSVWriterPolarsEngine(sample_csv_path)
        engine.write_excel()
