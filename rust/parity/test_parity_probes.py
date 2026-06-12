import datagrunt_rs
from datagrunt.core.csv_io import csvcomponents as py


def test_is_legacy_mac_newlines(corpus_file):
    assert datagrunt_rs.is_legacy_mac_newlines(str(corpus_file)) == py._is_legacy_mac_newlines(
        corpus_file
    )


def test_count_leading_comments(corpus_file):
    assert datagrunt_rs.count_leading_comments(str(corpus_file)) == py._count_leading_comments(
        corpus_file
    )


def test_count_leading_physical_lines_before_header(corpus_file):
    assert datagrunt_rs.count_leading_physical_lines_before_header(
        str(corpus_file)
    ) == py._count_leading_physical_lines_before_header(corpus_file)


def test_first_row(corpus_file):
    assert datagrunt_rs.first_row(str(corpus_file)) == py.CSVRows(corpus_file).first_row


def test_leading_rows(corpus_file):
    for limit in (1, 2, 5, 100):
        assert datagrunt_rs.leading_rows(str(corpus_file), limit) == py.CSVRows(
            corpus_file
        ).leading_rows(limit)
