"""Unit tests for the single-pass CSV header probe (Python reference)."""

from datagrunt.core.csv_io import _compute_python as py


def _write(tmp_path, name, content):
    p = tmp_path / name
    p.write_bytes(content)
    return str(p)


def test_probe_normal_csv(tmp_path):
    path = _write(tmp_path, "a.csv", b"a,b,c\n1,2,3\n4,5,6\n")
    probe = py.probe_csv_header(path)
    assert probe["empty"] is False
    assert probe["blank"] is False
    assert probe["first_row"] == "a,b,c"
    assert probe["sample_rows"][0] == "a,b,c"
    assert probe["sample_lines"][0] == "a,b,c\n"


def test_probe_empty(tmp_path):
    path = _write(tmp_path, "e.csv", b"")
    probe = py.probe_csv_header(path)
    assert probe["empty"] is True
    assert probe["first_row"] == "" and probe["sample_rows"] == [] and probe["sample_lines"] == []


def test_probe_blank(tmp_path):
    path = _write(tmp_path, "b.csv", b"   \n\n\t\n")
    probe = py.probe_csv_header(path)
    assert probe["empty"] is False
    assert probe["blank"] is True
    assert probe["first_row"] == ""


def test_probe_skips_comments_for_rows_but_keeps_blanks_in_lines(tmp_path):
    path = _write(tmp_path, "c.csv", b"# comment\n\na,b\n1,2\n")
    probe = py.probe_csv_header(path)
    # sample_rows: stripped, non-blank, non-comment
    assert probe["sample_rows"][0] == "a,b"
    # sample_lines: raw non-comment lines incl the blank line
    assert probe["sample_lines"][0] == "\n"
    assert probe["first_row"] == "a,b"


def test_probe_matches_legacy_helpers(tmp_path):
    path = _write(tmp_path, "d.csv", b"x;y;z\n1;2;3\n#c\n4;5;6\n")
    probe = py.probe_csv_header(path)
    assert probe["first_row"] == py.first_row(path)
    assert probe["sample_rows"] == py.leading_rows(path, py.CANDIDATE_SAMPLE_ROWS)


def test_probe_caps_sample_rows_and_lines_at_five(tmp_path):
    """Both sample windows cap at their 5-row limits regardless of file size."""
    rows = b"".join(f"r{i},v\n".encode() for i in range(8))  # 8 non-blank, non-comment rows
    path = _write(tmp_path, "big.csv", rows)
    probe = py.probe_csv_header(path)
    assert len(probe["sample_rows"]) == py.CANDIDATE_SAMPLE_ROWS == 5
    assert len(probe["sample_lines"]) == py.CSV_SNIFF_SAMPLE_ROWS == 5
    assert probe["sample_rows"] == py.leading_rows(path, py.CANDIDATE_SAMPLE_ROWS)
