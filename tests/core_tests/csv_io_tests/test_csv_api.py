"""Tests for the CSV API classes."""

import json
import tempfile
from pathlib import Path

import polars as pl
import pytest

from datagrunt import CSVReader, CSVSchemaReportAIGenerated, CSVWriter


class TestCSVReader:
    """Test suite for CSVReader class."""

    def test_csvreader_initialization(self, sample_csv):
        """Test CSVReader initialization with different engines."""
        # Test with default engine (polars)
        reader = CSVReader(sample_csv)
        assert reader.engine == 'polars'

        # Test with explicit engines
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(sample_csv, engine=engine)
            assert reader.engine == engine

    def test_csvreader_engine_normalization(self, sample_csv):
        """Test that CSVReader normalizes engine names."""
        test_cases = [
            ('POLARS', 'polars'),
            ('Polars', 'polars'),
            ('duck db', 'duckdb'),
            ('DuckDB', 'duckdb'),
            ('PyArrow', 'pyarrow'),
        ]

        for input_engine, expected_engine in test_cases:
            reader = CSVReader(sample_csv, engine=input_engine)
            assert reader.engine == expected_engine

    def test_csvreader_to_dataframe_all_engines(self, sample_csv):
        """Test to_dataframe method across all engines."""
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(sample_csv, engine=engine)
            df = reader.to_dataframe()
            assert isinstance(df, pl.DataFrame)
            assert len(df) > 0

    def test_csvreader_to_dataframe_with_normalization(self, tmp_path):
        """Test to_dataframe with column normalization."""
        csv_file = tmp_path / "test_norm.csv"
        csv_file.write_text("First Name,Last Name,E-mail\nJohn,Doe,john@test.com")

        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(csv_file), engine=engine)
            df = reader.to_dataframe(normalize_columns=True)
            assert 'first_name' in df.columns
            assert 'last_name' in df.columns
            assert 'e_mail' in df.columns

    def test_csvreader_to_arrow_table_all_engines(self, sample_csv):
        """Test to_arrow_table method across all engines."""
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(sample_csv, engine=engine)
            table = reader.to_arrow_table()
            assert table.num_rows > 0
            assert table.num_columns > 0

    def test_csvreader_to_dicts_all_engines(self, sample_csv):
        """Test to_dicts method across all engines."""
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(sample_csv, engine=engine)
            dicts = reader.to_dicts()
            assert isinstance(dicts, list)
            assert len(dicts) > 0
            assert all(isinstance(d, dict) for d in dicts)

    def test_csvreader_query_data_all_engines(self, sample_csv):
        """Test query_data method across all engines."""
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(sample_csv, engine=engine)
            result = reader.query_data(f"SELECT * FROM {reader.db_table} LIMIT 1")
            assert result is not None

    def test_csvreader_empty_file_handling(self, tmp_path):
        """Test CSVReader with empty files."""
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(empty_file), engine=engine)

            # Test that empty file returns empty objects
            df = reader.to_dataframe()
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 0

            dicts = reader.to_dicts()
            assert isinstance(dicts, list)
            assert len(dicts) == 0

    def test_csvreader_blank_file_handling(self, tmp_path):
        """Test CSVReader with blank files (only whitespace)."""
        blank_file = tmp_path / "blank.csv"
        blank_file.write_text("   \n  \n  ")

        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(blank_file), engine=engine)

            df = reader.to_dataframe()
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 0

    def test_csvreader_get_sample_all_engines(self, sample_csv, capsys):
        """Test get_sample method across all engines."""
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(sample_csv, engine=engine)
            reader.get_sample()

            captured = capsys.readouterr()
            assert len(captured.out) > 0  # Should print something

    def test_csvreader_invalid_file(self, tmp_path):
        """Test CSVReader with non-existent file."""
        non_existent = str(tmp_path / "does_not_exist.csv")

        for engine in ['polars', 'duckdb', 'pyarrow']:
            with pytest.raises(FileNotFoundError):
                CSVReader(non_existent, engine=engine)


class TestCSVWriter:
    """Test suite for CSVWriter class."""

    def test_csvwriter_initialization(self, sample_csv):
        """Test CSVWriter initialization with different engines."""
        # Test with default engine (duckdb)
        writer = CSVWriter(sample_csv)
        assert writer.engine == 'duckdb'

        # Test with explicit engines
        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(sample_csv, engine=engine)
            assert writer.engine == engine

    def test_csvwriter_write_csv_all_engines(self, sample_csv, tmp_path):
        """Test write_csv method across all engines."""
        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(sample_csv, engine=engine)
            output_file = str(tmp_path / f"output_{engine}.csv")

            writer.write_csv(output_file)
            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

    def test_csvwriter_write_csv_with_normalization(self, tmp_path):
        """Test write_csv with column normalization."""
        input_file = tmp_path / "input.csv"
        input_file.write_text("First Name,Last Name\nJohn,Doe")

        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(str(input_file), engine=engine)
            output_file = str(tmp_path / f"output_norm_{engine}.csv")

            writer.write_csv(output_file, normalize_columns=True)

            # Verify normalized column names in output
            with open(output_file, 'r') as f:
                header = f.readline().strip()
                assert 'first_name' in header.lower()
                assert 'last_name' in header.lower()

    def test_csvwriter_write_json_all_engines(self, sample_csv, tmp_path):
        """Test write_json method across all engines."""
        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(sample_csv, engine=engine)
            output_file = str(tmp_path / f"output_{engine}.json")

            writer.write_json(output_file)
            assert Path(output_file).exists()

            # Verify valid JSON
            with open(output_file, 'r') as f:
                data = json.load(f)
                assert isinstance(data, list)
                assert len(data) > 0

    def test_csvwriter_write_jsonl_all_engines(self, sample_csv, tmp_path):
        """Test write_json_newline_delimited method across all engines."""
        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(sample_csv, engine=engine)
            output_file = str(tmp_path / f"output_{engine}.jsonl")

            writer.write_json_newline_delimited(output_file)
            assert Path(output_file).exists()

            # Verify valid JSONL
            with open(output_file, 'r') as f:
                lines = f.readlines()
                assert len(lines) > 0
                for line in lines:
                    json.loads(line.strip())  # Should not raise

    def test_csvwriter_write_parquet_all_engines(self, sample_csv, tmp_path):
        """Test write_parquet method across all engines."""
        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(sample_csv, engine=engine)
            output_file = str(tmp_path / f"output_{engine}.parquet")

            writer.write_parquet(output_file)
            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

    def test_csvwriter_write_excel_all_engines(self, sample_csv, tmp_path):
        """Test write_excel method across all engines."""
        for engine in ['duckdb', 'polars', 'pyarrow']:
            writer = CSVWriter(sample_csv, engine=engine)
            output_file = str(tmp_path / f"output_{engine}.xlsx")

            try:
                writer.write_excel(output_file)
                assert Path(output_file).exists()
                assert Path(output_file).stat().st_size > 0
            except ImportError:
                # Excel export may require additional dependencies
                pass

    def test_csvwriter_default_filenames(self, sample_csv):
        """Test CSVWriter with default filenames."""
        writer = CSVWriter(sample_csv, engine='polars')

        # Test with default filenames - should create files in current directory
        writer.write_csv()
        assert Path('output.csv').exists()
        Path('output.csv').unlink()  # Clean up

        writer.write_json()
        assert Path('output.json').exists()
        Path('output.json').unlink()  # Clean up

    def test_csvwriter_invalid_file(self, tmp_path):
        """Test CSVWriter with non-existent file."""
        non_existent = str(tmp_path / "does_not_exist.csv")

        for engine in ['duckdb', 'polars', 'pyarrow']:
            with pytest.raises(FileNotFoundError):
                CSVWriter(non_existent, engine=engine)


class TestCSVSchemaReportAIGenerated:
    """Test suite for CSVSchemaReportAIGenerated class."""

    def test_csvschema_initialization(self, sample_csv):
        """Test CSVSchemaReportAIGenerated initialization."""
        # Test with valid engine
        schema_gen = CSVSchemaReportAIGenerated(
            sample_csv,
            engine='google',
            api_key='fake_key'
        )
        assert schema_gen.engine == 'google'
        assert schema_gen.api_key == 'fake_key'

    def test_csvschema_invalid_engine(self, sample_csv):
        """Test CSVSchemaReportAIGenerated with invalid engine."""
        with pytest.raises(ValueError) as exc_info:
            CSVSchemaReportAIGenerated(
                sample_csv,
                engine='invalid',
                api_key='fake_key'
            )
        assert "Unsupported AI engine: invalid" in str(exc_info.value)

    def test_csvschema_google_search_restriction(self, sample_csv):
        """Test that Google Search grounding is restricted."""
        with pytest.raises(ValueError) as exc_info:
            CSVSchemaReportAIGenerated(
                sample_csv,
                engine='google',
                api_key='fake_key',
                ground_google_search=True
            )
        assert "Grounding in Google Search is not supported" in str(exc_info.value)


class TestIntegrationCSVReaderWriter:
    """Integration tests between CSVReader and CSVWriter."""

    def test_reader_writer_round_trip(self, sample_csv, tmp_path):
        """Test round-trip: read with one engine, write with another."""
        # Read with one engine
        reader = CSVReader(sample_csv, engine='polars')
        original_data = reader.to_dicts()

        # Write with different engine
        writer = CSVWriter(sample_csv, engine='duckdb')
        output_file = str(tmp_path / "round_trip.csv")
        writer.write_csv(output_file)

        # Read back with third engine
        reader2 = CSVReader(output_file, engine='pyarrow')
        round_trip_data = reader2.to_dicts()

        # Compare data (allowing for string type differences)
        assert len(original_data) == len(round_trip_data)
        assert len(original_data[0]) == len(round_trip_data[0])

    def test_normalization_consistency(self, tmp_path):
        """Test that normalization is consistent across engines."""
        # Create test file with problematic column names
        test_file = tmp_path / "normalization_test.csv"
        test_file.write_text("First Name,2nd Column,E-mail Address\nJohn,A,john@test.com")

        normalized_columns = {}

        # Test normalization with each engine
        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(test_file), engine=engine)
            df = reader.to_dataframe(normalize_columns=True)
            normalized_columns[engine] = list(df.columns)

        # All engines should produce the same normalized column names
        assert normalized_columns['polars'] == normalized_columns['duckdb']
        assert normalized_columns['duckdb'] == normalized_columns['pyarrow']

    def test_data_preservation_across_engines(self, tmp_path):
        """Test that data with leading zeros is preserved across engines."""
        # Create CSV with data that could lose precision
        test_file = tmp_path / "preservation_test.csv"
        test_file.write_text("id,amount,code\n001,0500,ABC123\n002,1000.50,DEF456")

        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(test_file), engine=engine)
            dicts = reader.to_dicts()

            # Check that leading zeros are preserved
            assert dicts[0]['id'] == '001'
            assert dicts[0]['amount'] == '0500'
            assert dicts[1]['id'] == '002'

    def test_large_file_handling(self, tmp_path):
        """Test handling of moderately large files."""
        # Create a file with many rows
        large_file = tmp_path / "large_test.csv"
        with open(large_file, 'w') as f:
            f.write("col1,col2,col3\n")
            for i in range(1000):
                f.write(f"{i},{i*2},{i*3}\n")

        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(large_file), engine=engine)
            df = reader.to_dataframe()
            assert len(df) == 1000

            # Test sampling
            reader.get_sample()  # Should not fail

    def test_special_characters_handling(self, tmp_path):
        """Test handling of special characters in data."""
        special_file = tmp_path / "special_chars.csv"
        special_file.write_text('name,description\n"John, Jr.","Product with ""quotes"""\nJané,Café item')

        for engine in ['polars', 'duckdb', 'pyarrow']:
            reader = CSVReader(str(special_file), engine=engine)
            dicts = reader.to_dicts()

            assert len(dicts) == 2
            assert 'John, Jr.' in dicts[0]['name']
            assert 'Jané' in dicts[1]['name']
