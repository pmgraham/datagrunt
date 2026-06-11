"""This module contains tests for the CSVWriter class."""

import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from datagrunt import CSVWriter

# All supported engines for CSVWriter
ALL_ENGINES = ["duckdb", "polars", "pyarrow"]


class TestCSVWriter:
    def test_write_csv(self, sample_csv, tmp_path):
        """Test writing to CSV format."""
        for engine in ALL_ENGINES:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.csv")
            writer.write_csv(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            df = pl.read_csv(out_file)
            assert len(df) == 2
            assert list(df.columns) == ["name", "age", "city"]

    def test_write_excel(self, sample_csv, tmp_path):
        """Test writing to Excel format."""
        for engine in ALL_ENGINES:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.xlsx")
            writer.write_excel(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            df = pl.read_excel(out_file)
            assert len(df) == 2
            assert list(df.columns) == ["name", "age", "city"]

    def test_write_json(self, sample_csv, tmp_path):
        """Test writing to JSON format."""
        for engine in ALL_ENGINES:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.json")
            writer.write_json(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            with open(out_file, "r") as f:
                data = json.load(f)
            assert len(data) == 2
            assert isinstance(data, list)
            assert all(isinstance(item, dict) for item in data)

    def test_write_json_newline_delimited(self, sample_csv, tmp_path):
        """Test writing to JSON Lines format."""
        for engine in ALL_ENGINES:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.jsonl")
            writer.write_json_newline_delimited(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            with open(out_file, "r") as f:
                lines = f.readlines()
            assert len(lines) == 2
            assert all(json.loads(line) for line in lines)

    def test_write_parquet(self, sample_csv, tmp_path):
        """Test writing to Parquet format."""
        for engine in ALL_ENGINES:
            writer = CSVWriter(sample_csv, engine=engine)
            out_file = str(tmp_path / f"output_{engine}.parquet")
            writer.write_parquet(out_file)

            # Verify the output file exists and contains data
            assert Path(out_file).exists()
            table = pq.read_table(out_file)
            assert len(table) == 2
            assert table.column_names == ["name", "age", "city"]

    def test_write_with_normalized_columns(self, tmp_path):
        """Test writing files with normalized column names."""
        # Create CSV with mixed case and spaces in column names
        csv_content = "First Name,Last Name,Age Group\nJohn,Doe,30-40\nJane,Smith,20-30"
        input_file = tmp_path / "test_normalize.csv"
        input_file.write_text(csv_content)

        for engine in ALL_ENGINES:
            writer = CSVWriter(str(input_file), engine=engine)
            out_file = str(tmp_path / f"normalized_{engine}.csv")
            writer.write_csv(out_file, normalize_columns=True)

            # Verify the output has normalized column names
            df = pl.read_csv(out_file)
            expected_columns = ["first_name", "last_name", "age_group"]
            assert list(df.columns) == expected_columns

    def test_write_empty_source_produces_empty_output(self, empty_csv, tmp_path):
        """Writing a zero-byte source yields a truly empty output on every engine.

        Mirrors CSVReader's empty/blank guard: no fabricated ``column0`` header
        (duckdb) and no engine-specific crash (polars NoDataError / pyarrow
        ArrowInvalid). The output file must exist and be empty (0 bytes).
        """
        for engine in ALL_ENGINES:
            writer = CSVWriter(empty_csv, engine=engine)
            out_file = Path(tmp_path / f"empty_source_{engine}.csv")
            writer.write_csv(str(out_file))

            assert out_file.exists()
            content = out_file.read_text()
            assert content == "", f"{engine} fabricated output: {content!r}"
            assert out_file.stat().st_size == 0

    def test_write_blank_source_produces_empty_output(self, blank_csv, tmp_path):
        """Writing a whitespace-only source yields empty output on every engine."""
        for engine in ALL_ENGINES:
            writer = CSVWriter(blank_csv, engine=engine)
            out_file = Path(tmp_path / f"blank_source_{engine}.csv")
            writer.write_csv(str(out_file))

            assert out_file.exists()
            assert out_file.read_text() == ""
            assert out_file.stat().st_size == 0

    def test_write_empty_source_all_formats(self, empty_csv, tmp_path):
        """All write_* methods produce empty output for an empty source."""
        methods_and_exts = [
            ("write_csv", "csv"),
            ("write_json", "json"),
            ("write_json_newline_delimited", "jsonl"),
            ("write_parquet", "parquet"),
            ("write_excel", "xlsx"),
        ]
        for engine in ALL_ENGINES:
            writer = CSVWriter(empty_csv, engine=engine)
            for method_name, ext in methods_and_exts:
                out_file = Path(tmp_path / f"empty_{engine}.{ext}")
                getattr(writer, method_name)(str(out_file))
                assert out_file.exists()
                assert out_file.stat().st_size == 0

    def test_default_filenames(self, sample_csv):
        """Test writing with default filenames."""
        writer = CSVWriter(sample_csv)

        # Test that default filenames are used when no filename is provided
        writer.write_csv()
        assert Path("output.csv").exists()
        Path("output.csv").unlink()  # Cleanup

        writer.write_parquet()
        assert Path("output.parquet").exists()
        Path("output.parquet").unlink()  # Cleanup

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
