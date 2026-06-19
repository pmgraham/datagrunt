"""Per-line cap bounds pathological newline-free input (issue #222)."""
from datagrunt.core.csv_io import _compute_python as py


def test_probe_caps_giant_single_line(tmp_path):
    path = tmp_path / "huge.csv"
    # One physical line, no newline, far over the cap.
    path.write_bytes(b"a," * (py.MAX_LINE_CHARS))  # ~2x cap in chars
    probe = py.probe_csv_header(str(path))
    assert probe["sample_lines"], "should still yield the (capped) line"
    assert len(probe["sample_lines"][0]) <= py.MAX_LINE_CHARS
    assert len(probe["first_row"]) <= py.MAX_LINE_CHARS


def test_realistic_wide_line_is_unaffected(tmp_path):
    path = tmp_path / "wide.csv"
    header = ",".join(f"col{i}" for i in range(5000))  # wide but << cap
    path.write_text(header + "\n1,2\n")
    probe = py.probe_csv_header(str(path))
    assert probe["first_row"] == header  # not truncated
    assert py.infer_delimiter(str(path)) == ","


def test_leading_rows_caps_long_line(tmp_path):
    path = tmp_path / "long.csv"
    path.write_text(("x" * (py.MAX_LINE_CHARS + 10)) + "\nsecond\n")
    rows = py.leading_rows(str(path), 5)
    assert len(rows[0]) <= py.MAX_LINE_CHARS
    assert rows[1] == "second"  # line alignment preserved after a capped line
