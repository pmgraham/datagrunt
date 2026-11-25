"""Security regression tests for DuckDB hardening, spatial-free Excel export, and SAST/FIPS hygiene (DG-03/04/06)."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import polars as pl
import pytest

from datagrunt.core.csv_io.engines import CSVWriterDuckDBEngine
from datagrunt.core.databases.databases import DuckDBQueries
from datagrunt.core.excel_io.engines import ExcelReaderEngine
from datagrunt.core.parquet_io.engines import ParquetReaderEngine
from datagrunt.rationalization.providers.base import bounded_detail

ROOT = Path(__file__).resolve().parents[2]


class TestDuckDBLimitValidation:
    """DG-03: sample_csv_query must reject non-integer or non-positive limits before SQL interpolation."""

    @pytest.mark.parametrize(
        "bad_limit",
        [
            "1; DROP TABLE users; --",
            "10",
            True,
            False,
            0,
            -5,
            1.5,
            None,
        ],
    )
    def test_sample_csv_query_rejects_invalid_limit(self, tmp_path: Path, bad_limit: object) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
        queries = DuckDBQueries(csv_path)
        with pytest.raises(ValueError, match="limit"):
            queries.sample_csv_query(bad_limit)

    def test_sample_csv_query_accepts_positive_int(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
        queries = DuckDBQueries(csv_path)
        sql = queries.sample_csv_query(1)
        assert sql.endswith("LIMIT 1")
        df = queries.sample_dataframe(1)
        assert len(df) == 1
        queries.close()


class TestDuckDBConnectionHardening:
    """DG-03: In-memory DuckDB sessions disable temp spilling and extension auto-install."""

    def test_duckdb_queries_connection_disables_temp_spill_and_autoinstall(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
        with DuckDBQueries(csv_path) as queries:
            rows = queries.connection.sql(
                "SELECT current_setting('temp_directory'), current_setting('autoinstall_known_extensions')"
            ).fetchall()
            assert rows == [("", False)]

    def test_excel_reader_query_data_disables_temp_spill_and_autoinstall(self, tmp_path: Path) -> None:
        xlsx_path = tmp_path / "sample.xlsx"
        pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_excel(xlsx_path)
        engine = ExcelReaderEngine(xlsx_path)
        try:
            rows = engine.query_data(
                "SELECT current_setting('temp_directory'), current_setting('autoinstall_known_extensions')"
            ).fetchall()
            assert rows == [("", False)]
        finally:
            engine.close()

    def test_parquet_reader_query_data_disables_temp_spill_and_autoinstall(self, tmp_path: Path) -> None:
        pq_path = tmp_path / "sample.parquet"
        pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_parquet(pq_path)
        engine = ParquetReaderEngine(pq_path)
        try:
            rows = engine.query_data(
                "SELECT current_setting('temp_directory'), current_setting('autoinstall_known_extensions')"
            ).fetchall()
            assert rows == [("", False)]
        finally:
            engine.close()


class TestCSVWriterDuckDBExcelNoSpatialDownload:
    """DG-04: CSVWriterDuckDBEngine.write_excel writes .xlsx locally via Polars/XlsxWriter without spatial."""

    @pytest.mark.parametrize("normalize_columns", [False, True])
    def test_write_excel_does_not_call_load_spatial_extension(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, normalize_columns: bool
    ) -> None:
        def _forbid_spatial(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("write_excel must not download or load DuckDB's spatial extension")

        monkeypatch.setattr(DuckDBQueries, "load_spatial_extension", _forbid_spatial)

        csv_path = tmp_path / "input.csv"
        csv_path.write_text("First Name,Last Name\nAda,Lovelace\nGrace,Hopper\n", encoding="utf-8")
        out_path = tmp_path / "out.xlsx"

        writer = CSVWriterDuckDBEngine(csv_path)
        try:
            writer.write_excel(str(out_path), normalize_columns=normalize_columns)
        finally:
            writer.close()

        assert out_path.is_file()
        loaded = pl.read_excel(out_path)
        expected_cols = ["first_name", "last_name"] if normalize_columns else ["First Name", "Last Name"]
        assert loaded.columns == expected_cols
        assert loaded.height == 2


class TestFipsAndBidiSourceHygiene:
    """DG-06: Non-security SHA-1 passes usedforsecurity=False and source has no literal bidi controls."""

    def test_databases_sha1_sets_usedforsecurity_false_and_preserves_table_name(self, tmp_path: Path) -> None:
        src_path = ROOT / "src" / "datagrunt" / "core" / "databases" / "databases.py"
        tree = ast.parse(src_path.read_text(encoding="utf-8"))
        sha1_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "sha1"
        ]
        assert sha1_calls, "Expected at least one hashlib.sha1 call in databases.py"
        for call in sha1_calls:
            kw = {k.arg: k.value for k in call.keywords}
            assert "usedforsecurity" in kw
            assert isinstance(kw["usedforsecurity"], ast.Constant)
            assert kw["usedforsecurity"].value is False

        csv_path = tmp_path / "sales.csv"
        csv_path.write_text("a\n1\n", encoding="utf-8")
        expected_hash = hashlib.sha1(str(csv_path.resolve()).encode(), usedforsecurity=False).hexdigest()[:8]
        assert DuckDBQueries(csv_path).database_table_name == f"tbl_sales_{expected_hash}"

    def test_providers_base_has_no_literal_bidi_controls_and_still_strips_them(self) -> None:
        base_path = ROOT / "src" / "datagrunt" / "rationalization" / "providers" / "base.py"
        source = base_path.read_text(encoding="utf-8")
        bidi_codepoints = set(range(0x202A, 0x202F)) | set(range(0x2066, 0x206A))
        found = [(idx + 1, hex(ord(ch))) for idx, ch in enumerate(source) if ord(ch) in bidi_codepoints]
        assert found == [], f"Literal Unicode bidi characters found in base.py: {found}"

        sample = "alpha\u202abeta\u202egamma\u2066delta\u2069omega"
        assert bounded_detail(sample) == "alpha beta gamma delta omega"
