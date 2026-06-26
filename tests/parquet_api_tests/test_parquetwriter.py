"""Tests for the ParquetWriter public class."""

# standard library
from pathlib import Path

# external libraries
import polars as pl

# local libraries
from datagrunt import CSVReader, ParquetReader, ParquetWriter


def test_write_csv_round_trips_through_csvreader(sample_parquet, tmp_path):
    out = str(tmp_path / "out.csv")
    ParquetWriter(sample_parquet).write_csv(out)
    assert CSVReader(out).to_dataframe().shape == (4, 3)


def test_write_all_formats(sample_parquet, tmp_path):
    writer = ParquetWriter(sample_parquet)
    json_out = str(tmp_path / "o.json")
    jsonl_out = str(tmp_path / "o.jsonl")
    parquet_out = str(tmp_path / "o.parquet")
    xlsx_out = str(tmp_path / "o.xlsx")
    writer.write_json(json_out)
    writer.write_json_newline_delimited(jsonl_out)
    writer.write_parquet(parquet_out)
    writer.write_excel(xlsx_out)
    assert Path(json_out).stat().st_size > 0
    assert Path(jsonl_out).stat().st_size > 0
    assert pl.read_parquet(parquet_out).shape == (4, 3)
    assert pl.read_excel(xlsx_out).shape == (4, 3)


def test_write_parquet_compression_passthrough(sample_parquet, tmp_path):
    out = str(tmp_path / "z.parquet")
    ParquetWriter(sample_parquet).write_parquet(out, compression="zstd")
    assert pl.read_parquet(out).shape == (4, 3)


def test_normalize_columns(messy_parquet, tmp_path):
    out = str(tmp_path / "n.csv")
    ParquetWriter(messy_parquet, normalize_columns=True).write_csv(out)
    assert pl.read_csv(out).columns == ["first_name", "age"]


def test_empty_writes_zero_byte_file(empty_parquet, tmp_path):
    out = str(tmp_path / "empty_out.csv")
    ParquetWriter(empty_parquet).write_csv(out)
    assert Path(out).stat().st_size == 0


def test_reader_and_writer_top_level_import():
    assert ParquetReader is not None
    assert ParquetWriter is not None
