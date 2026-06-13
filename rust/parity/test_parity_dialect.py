from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io.csvcomponents import CSVDelimiter, CSVDialect

QUOTING_MAP = CSVDialect.QUOTING_MAP


def dialect_properties_from_rust(rust_dict):
    """Apply CSVDialect's property defaults to the raw Rust sniff result."""
    if rust_dict is None:
        return {
            "quotechar": '"',
            "escapechar": None,
            "doublequote": False,
            "newline_delimiter": "\r\n",
            "skipinitialspace": False,
            "quoting": "quote minimal",
        }
    return {
        "quotechar": rust_dict["quotechar"],
        "escapechar": rust_dict["escapechar"],
        "doublequote": rust_dict["doublequote"],
        "newline_delimiter": rust_dict["lineterminator"],
        "skipinitialspace": rust_dict["skipinitialspace"],
        "quoting": QUOTING_MAP.get(rust_dict["quoting"]),
    }


def dialect_properties_from_python(dialect_obj):
    return {
        "quotechar": dialect_obj.quotechar,
        "escapechar": dialect_obj.escapechar,
        "doublequote": dialect_obj.doublequote,
        "newline_delimiter": dialect_obj.newline_delimiter,
        "skipinitialspace": dialect_obj.skipinitialspace,
        "quoting": dialect_obj.quoting,
    }


def test_sniff_dialect_unrestricted(corpus_file):
    rust = dialect_properties_from_rust(datagrunt_rs.sniff_dialect(str(corpus_file)))
    python = dialect_properties_from_python(CSVDialect(corpus_file))
    assert rust == python, corpus_file.name


def test_sniff_dialect_with_delimiter_restriction(corpus_file):
    delimiter = CSVDelimiter(corpus_file).delimiter
    rust = dialect_properties_from_rust(
        datagrunt_rs.sniff_dialect(str(corpus_file), delimiter)
    )
    python = dialect_properties_from_python(CSVDialect(corpus_file, delimiter=delimiter))
    assert rust == python, corpus_file.name
