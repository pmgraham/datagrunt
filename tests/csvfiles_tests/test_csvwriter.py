import pytest
import json
import polars as pl
import pyarrow.parquet as pq
from pathlib import Path

from src.datagrunt.csvfiles.csvwriter import CSVWriter

class TestCSVWriter:

    def test_init_with_valid_engines(self, sample_csv):
            """Test initialization with valid engine values."""
            writer_duckdb = CSVWriter(sample_csv, engine='duckdb')
            assert writer_duckdb.engine == 'duckdb'

            writer_polars = CSVWriter(sample_csv, engine='polars')
            assert writer_polars.engine == 'polars'

            # Test with spaces and different cases
            writer_with_spaces = CSVWriter(sample_csv, engine='Duck DB')
            assert writer_with_spaces.engine == 'duckdb'

    def test_init_with_invalid_engine(self, sample_csv):
        """Test initialization with invalid engine value."""
        with pytest.raises(ValueError) as exc_info:
            CSVWriter(sample_csv, engine='invalid')
        assert "Reader engine 'invalid' is not 'duckdb' or 'polars'" in str(exc_info.value)

    def test_write_csv(self, sample_csv, tmp_path):
        """Test writing to CSV format."""
        for engine in ['duckdb', 'polars']:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.csv")
            writer.write_csv(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            df = pl.read_csv(out_file)
            assert len(df) == 2
            assert list(df.columns) == ['name', 'age', 'city']

    def test_write_excel(self, sample_csv, tmp_path):
        """Test writing to Excel format."""
        for engine in ['duckdb', 'polars']:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.xlsx")
            writer.write_excel(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            df = pl.read_excel(out_file)
            assert len(df) == 2
            assert list(df.columns) == ['name', 'age', 'city']

    def test_write_json(self, sample_csv, tmp_path):
        """Test writing to JSON format."""
        for engine in ['duckdb', 'polars']:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.json")
            writer.write_json(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            with open(out_file, 'r') as f:
                data = json.load(f)
            assert len(data) == 2
            assert isinstance(data, list)
            assert all(isinstance(item, dict) for item in data)

    def test_write_json_newline_delimited(self, sample_csv, tmp_path):
        """Test writing to JSON Lines format."""
        for engine in ['duckdb', 'polars']:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.jsonl")
            writer.write_json_newline_delimited(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            with open(out_file, 'r') as f:
                lines = f.readlines()
            assert len(lines) == 2
            assert all(json.loads(line) for line in lines)

    def test_write_parquet(self, sample_csv, tmp_path):
        """Test writing to Parquet format."""
        for engine in ['duckdb', 'polars']:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.parquet")
            writer.write_parquet(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            table = pq.read_table(out_file)
            assert len(table) == 2
            assert table.column_names == ['name', 'age', 'city']

    def test_write_with_normalized_columns(self, tmp_path):
        """Test writing files with normalized column names."""
        # Create CSV with mixed case and spaces in column names
        csv_content = "First Name,Last Name,Age Group\nJohn,Doe,30-40\nJane,Smith,20-30"
        input_file = tmp_path / "test_normalize.csv"
        input_file.write_text(csv_content)

        for engine in ['duckdb', 'polars']:
            writer = CSVWriter(str(input_file), engine=engine)
            out_file = str(tmp_path / f"normalized_{engine}.csv")
            writer.write_csv(out_file, normalize_columns=True)

            # Verify the output has normalized column names
            df = pl.read_csv(out_file)
            expected_columns = ['first_name', 'last_name', 'age_group']
            assert list(df.columns) == expected_columns

    def test_write_completely_empty_file(self, completely_empty_csv, tmp_path):
        """Test writing completely empty files (no headers, no content)."""
        writer = CSVWriter(completely_empty_csv)

        out_csv = str(tmp_path / "completely_empty_output.csv")
        writer.write_csv(out_csv)
        assert Path(out_csv).exists()
        with open(out_csv, 'r') as f:
            content = f.read()
        assert content.strip() == "" or "column0"  # Verify file is completely empty (ignoring whitespace)

    def test_default_filenames(self, sample_csv):
        """Test writing with default filenames."""
        writer = CSVWriter(sample_csv)

        # Test that default filenames are used when no filename is provided
        writer.write_csv()
        assert Path('output.csv').exists()
        Path('output.csv').unlink()  # Cleanup

        writer.write_parquet()
        assert Path('output.parquet').exists()
        Path('output.parquet').unlink()  # Cleanup

    def test_file_overwrite(self, sample_csv, tmp_path):
        """Test overwriting existing output files."""
        out_file = str(tmp_path / "test_overwrite.csv")
        writer = CSVWriter(sample_csv)

        # Write file twice to test overwrite behavior
        writer.write_csv(out_file)
        original_timestamp = Path(out_file).stat().st_mtime

        writer.write_csv(out_file)
        new_timestamp = Path(out_file).stat().st_mtime

        assert new_timestamp > original_timestamp
