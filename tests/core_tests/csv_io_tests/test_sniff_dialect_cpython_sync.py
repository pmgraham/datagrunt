"""The Python oracle's sniffer pins the FIXED CPython ``csv.Sniffer`` (#378).

``_compute_python.sniff_dialect`` used to delegate to the running interpreter's
``csv.Sniffer``, so the parity oracle changed whenever a CPython patch release
changed the Sniffer. Upstream fixed four things in 2026 (backported to 3.10+):

* gh-103925 — ``sniff()`` normalises ``\\r\\n`` / ``\\r`` to ``\\n`` first.
* gh-98820 — a quoted-field body stops at the first undoubled quote (the old
  lazy ``.*?`` could slide across doubled quotes and field boundaries).
* gh-109638 — doubled-quote detection matches whole fields, and only runs once
  a delimiter is known.
* gh-80678 — docs only: preferred delimiters win ties regardless of count.

Every expected value below was produced by the fixed reference ``Lib/csv.py``
(CPython v3.10.22, cross-checked against v3.14.10) on the exact sample the
oracle builds from the file. ``pre-fix`` notes give what CPython v3.14.3 /
v3.12.3 returned instead; cases without one are controls that did not change.
The oracle is called directly, so these hold whatever the backend toggle says.
"""

import csv

import pytest

from datagrunt.core.csv_io import _compute_python as py

SAY_HI = b'id,note\n1,"say ""hi"" now"\n2,ok\n'


def _dialect(delimiter, quotechar, doublequote, skipinitialspace):
    return {
        "delimiter": delimiter,
        "quotechar": quotechar,
        "escapechar": None,
        "doublequote": doublequote,
        "lineterminator": "\r\n",
        "skipinitialspace": skipinitialspace,
        "quoting": 0,
    }


# (file bytes, delimiters restriction, fixed result as (delim, quote, dq, sis) or None)
FIXED_CPYTHON_CASES = [
    # gh-98820: the lazy body slid across the doubled quotes to pair `" ""hi"" `.
    pytest.param(SAY_HI, None, (",", '"', True, False), id="corpus-embedded-quotes"),  # pre-fix ' '
    pytest.param(
        b'id,note\n1,"say ""hi"" now"\n2,"ok"\n', None, (",", '"', True, False), id="embedded-quotes-quoted-tail"
    ),  # pre-fix ' '
    pytest.param(
        b'id,note\r\n1,"say ""hi"" now"\r\n2,ok\r\n', None, (",", '"', True, False), id="embedded-quotes-crlf"
    ),  # pre-fix ' '
    pytest.param(
        b'id;note\n1;"say ""hi"" now"\n2;ok\n', None, (";", '"', True, False), id="embedded-quotes-semicolon"
    ),  # pre-fix ' '
    pytest.param(
        b'id\tnote\n1\t"say ""hi"" now"\n2\tok\n', None, ("\t", '"', True, False), id="embedded-quotes-tab"
    ),  # pre-fix ' '
    pytest.param(
        b'a,b\n"""quoted"" start",1\n"plain",2\n', None, (",", '"', True, False), id="doubled-quote-field-start"
    ),  # pre-fix ' '
    pytest.param(
        b'name\theight\n"x ""y"" z"\t5\'10"\n', None, ("\t", '"', True, False), id="inch-marks-tab"
    ),  # pre-fix ' '
    pytest.param(
        b'k\tv\n\'a\'\t"x ""y"" z"\n', None, ("\t", "'", False, False), id="apostrophe-then-quoted-tab"
    ),  # pre-fix (' ', '"', True, False)
    pytest.param(b'\'a\'|"a"\n"b"|12\n', None, ("|", "'", False, False), id="mixed-quotes-pipe"),  # pre-fix '"'
    pytest.param(b'a; b\n "x"; "y"\n', None, (";", '"', False, True), id="spaced-semicolons"),  # pre-fix ' ', sis F
    pytest.param(
        b'note;id\n"a; ""b""";1\n', None, (";", '"', True, False), id="semicolon-doubled-then-field"
    ),  # pre-fix sis True
    # gh-109638: doubled-quote detection on whole fields only.
    pytest.param(
        b'k,v\n"a,b","say ""x, y"""\n"c","d"\n', None, (",", '"', True, False), id="quoted-delims-and-doubled"
    ),  # pre-fix dq False
    pytest.param(
        b'"a""b"\n"c""d"\n"e""f"\n', None, ('"', '"', False, False), id="single-column-doubled"
    ),  # pre-fix dq True
    pytest.param(
        b'title\n"""Dune"" novel"\n', None, ("l", '"', False, False), id="single-column-title-doubled"
    ),  # pre-fix (' ', '"', True, False)
    pytest.param(b'note\n"a, ""b"""\n', None, None, id="comma-space-doubled"),  # pre-fix (',', '"', True, True)
    # `delimiters=` restrictions.
    pytest.param(
        b'a;b,c\n1;"x ""y"" z",3\n', ";", (";", '"', False, False), id="restricted-semicolon"
    ),  # pre-fix dq True
    pytest.param(SAY_HI, " ", None, id="restricted-space"),  # pre-fix (' ', '"', True, False)
    pytest.param(
        b' "a";a; "a"\n', ";", (";", '"', False, True), id="restricted-semicolon-spaced"
    ),  # pre-fix (';', '"', True, False)
    pytest.param(
        b'"a\tb"\n"a\t ""b"""\n', "\t", ("\t", '"', False, False), id="restricted-tab-doubled"
    ),  # pre-fix (dq True, sis True)
    # Controls: unchanged by the upstream fixes.
    pytest.param(SAY_HI, ",", (",", '"', True, False), id="control-restricted-comma"),
    pytest.param(b'a, "b", c\n1, "x", 3\n', None, (",", '"', False, True), id="control-skipinitialspace"),
    pytest.param(
        b'name,addr\n"Smith, J","1 Main St, Apt 2"\n"Doe, A","9 Elm"\n',
        None,
        (",", '"', False, False),
        id="control-quoted-delims-inside",
    ),
    pytest.param(b"a;b;c\n1;2;3\n4;5;6\n", None, (";", '"', False, False), id="control-frequency-single"),
    pytest.param(b"a,b\tc,d\n1,2\t3,4\n5,6\t7,8\n", None, (",", '"', False, False), id="control-preferred-tie-break"),
    pytest.param(b"ab\nab\n", None, ("b", '"', False, False), id="control-dominant-char-fallback"),
]


def _sniff_file(tmp_path, data, delimiters):
    path = tmp_path / "sample.csv"
    path.write_bytes(data)
    if delimiters is None:
        return py.sniff_dialect(str(path))
    return py.sniff_dialect(str(path), delimiters)


@pytest.mark.parametrize(("data", "delimiters", "expected"), FIXED_CPYTHON_CASES)
def test_oracle_matches_fixed_cpython_sniffer(tmp_path, data, delimiters, expected):
    want = None if expected is None else _dialect(*expected)
    assert _sniff_file(tmp_path, data, delimiters) == want


class _SnifferMustNotBeUsed:
    def __init__(self, *args, **kwargs):
        raise AssertionError("the oracle must not delegate to the interpreter's csv.Sniffer")


@pytest.mark.parametrize(("data", "delimiters", "expected"), FIXED_CPYTHON_CASES)
def test_oracle_is_independent_of_interpreter_sniffer(monkeypatch, tmp_path, data, delimiters, expected):
    """The interpreter's Sniffer changes across patch releases; the oracle must not."""
    monkeypatch.setattr(csv, "Sniffer", _SnifferMustNotBeUsed)
    want = None if expected is None else _dialect(*expected)
    assert _sniff_file(tmp_path, data, delimiters) == want


@pytest.mark.parametrize(
    "sample",
    [
        pytest.param('a,b\r\n1,"x ""y"""\r\n2,"z"\r\n', id="crlf"),
        pytest.param('a,b\r1,"x ""y"""\r2,"z"\r', id="cr"),
    ],
)
def test_sample_sniff_normalises_line_endings(sample):
    """gh-103925. Unreachable through a file (universal newlines already translate
    both), so this pins the mirror of ``Sniffer.sniff`` on the raw sample.
    Pre-fix CPython returned doublequote False for both samples.
    """
    assert py._sniff_sample(sample, None) == _dialect(",", '"', True, False)
