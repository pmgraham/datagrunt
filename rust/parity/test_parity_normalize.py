import datagrunt_rs
from datagrunt.core.csv_io.csvcomponents import CSVColumnNameNormalizer

CASES = [
    ["name", "age", "city"],
    ["First Name", "LAST-NAME", "e-mail  address"],
    ["col_a", "col_a", "col_a_1"],            # uniquify must not collide
    ["dup", "dup", "dup", "dup_1", "dup_2"],
    ["%", "()", "!!!"],                        # all-special -> "column" placeholder
    ["123abc", "9lives", "_x_"],               # leading digits / underscores
    ["näme", "ÄGE", "城市"],                   # non-ASCII -> underscores
    ["", " ", "a"],                            # empty-ish headers
    ["a__b___c", "__d__"],
    ["UPPER", "upper", "Upper"],               # case-fold collisions
]


def normalize_py(columns):
    # filepath is never read when columns are supplied
    return CSVColumnNameNormalizer("unused.csv", columns=columns).columns_normalized


def test_normalize_columns_parity():
    for columns in CASES:
        assert datagrunt_rs.normalize_columns(columns) == normalize_py(columns), columns
