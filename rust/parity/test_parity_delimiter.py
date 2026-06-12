import datagrunt_rs
from datagrunt.core.csv_io.csvcomponents import CSVDelimiter


def test_infer_delimiter(corpus_file):
    assert datagrunt_rs.infer_delimiter(str(corpus_file)) == CSVDelimiter(corpus_file).delimiter
