"""The PyArrow writer parses the source once across multiple exports."""

from datagrunt.core.csv_io.engines import CSVWriterPyArrowEngine


def test_pyarrow_writer_parses_once_across_formats(tmp_path, monkeypatch):
    src = tmp_path / "in.csv"
    src.write_text("a,b\n1,2\n3,4\n")

    engine = CSVWriterPyArrowEngine(str(src))

    calls = {"n": 0}
    original = engine._create_table

    def counting_create_table(normalize_columns=False):
        calls["n"] += 1
        return original(normalize_columns)

    monkeypatch.setattr(engine, "_create_table", counting_create_table)

    engine.write_csv(str(tmp_path / "out.csv"))
    engine.write_json(str(tmp_path / "out.json"))
    engine.write_parquet(str(tmp_path / "out.parquet"))

    assert calls["n"] == 1  # source parsed once, not once per format


def test_pyarrow_writer_caches_per_normalize_mode(tmp_path, monkeypatch):
    src = tmp_path / "in.csv"
    src.write_text("A B,C\n1,2\n")
    engine = CSVWriterPyArrowEngine(str(src))

    calls = {"n": 0}
    original = engine._create_table

    def counting_create_table(normalize_columns=False):
        calls["n"] += 1
        return original(normalize_columns)

    monkeypatch.setattr(engine, "_create_table", counting_create_table)

    engine.write_csv(str(tmp_path / "raw.csv"), normalize_columns=False)
    engine.write_csv(str(tmp_path / "norm.csv"), normalize_columns=True)
    engine.write_json(str(tmp_path / "norm.json"), normalize_columns=True)

    assert calls["n"] == 2  # one build per distinct normalize_columns value
