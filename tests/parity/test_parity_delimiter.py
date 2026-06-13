from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py


def test_infer_delimiter(corpus_file):
    assert datagrunt_rs.infer_delimiter(str(corpus_file)) == py.infer_delimiter(str(corpus_file))
