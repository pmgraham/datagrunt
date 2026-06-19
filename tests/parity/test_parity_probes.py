from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py


def test_is_legacy_mac_newlines(corpus_file):
    assert datagrunt_rs.is_legacy_mac_newlines(str(corpus_file)) == py.is_legacy_mac_newlines(
        str(corpus_file)
    )


def test_count_leading_comments(corpus_file):
    assert datagrunt_rs.count_leading_comments(str(corpus_file)) == py.count_leading_comments(
        str(corpus_file)
    )


def test_count_leading_physical_lines_before_header(corpus_file):
    assert datagrunt_rs.count_leading_physical_lines_before_header(
        str(corpus_file)
    ) == py.count_leading_physical_lines_before_header(str(corpus_file))


def test_first_row(corpus_file):
    assert datagrunt_rs.first_row(str(corpus_file)) == py.first_row(str(corpus_file))


def test_leading_rows(corpus_file):
    for limit in (1, 2, 5, 100):
        assert datagrunt_rs.leading_rows(str(corpus_file), limit) == py.leading_rows(
            str(corpus_file), limit
        )


def test_probe_csv_header(corpus_file):
    rust = datagrunt_rs.probe_csv_header(str(corpus_file))
    python = py.probe_csv_header(str(corpus_file))
    assert rust == python, corpus_file.name
