from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py
from datagrunt.core.csv_io.csvcomponents import CSVDialect

QUOTING_MAP = CSVDialect.QUOTING_MAP


def dialect_properties_from_rust(rust_dict):
    """Apply CSVDialect's property defaults to the raw Rust sniff result.

    ``delimiter`` is included deliberately (#319). It used to be omitted, which
    meant sniff_dialect's own delimiter output was never compared by any corpus
    test — infer_delimiter has its own test, but it is a different code path.
    That gap hid #318, a real cross-backend delimiter divergence, until the
    property suite compared the raw dicts and found it.
    """
    if rust_dict is None:
        return {
            "delimiter": None,
            "quotechar": '"',
            "escapechar": None,
            "doublequote": False,
            "newline_delimiter": "\r\n",
            "skipinitialspace": False,
            "quoting": "quote minimal",
        }
    return {
        "delimiter": rust_dict["delimiter"],
        "quotechar": rust_dict["quotechar"],
        "escapechar": rust_dict["escapechar"],
        "doublequote": rust_dict["doublequote"],
        "newline_delimiter": rust_dict["lineterminator"],
        "skipinitialspace": rust_dict["skipinitialspace"],
        "quoting": QUOTING_MAP.get(rust_dict["quoting"]),
    }


def test_sniff_dialect_unrestricted(corpus_file):
    rust = dialect_properties_from_rust(datagrunt_rs.sniff_dialect(str(corpus_file)))
    python = dialect_properties_from_rust(py.sniff_dialect(str(corpus_file)))
    assert rust == python, corpus_file.name


def test_sniff_dialect_with_delimiter_restriction(corpus_file):
    delimiter = py.infer_delimiter(str(corpus_file))
    rust = dialect_properties_from_rust(datagrunt_rs.sniff_dialect(str(corpus_file), delimiter))
    python = dialect_properties_from_rust(py.sniff_dialect(str(corpus_file), delimiter))
    assert rust == python, corpus_file.name
