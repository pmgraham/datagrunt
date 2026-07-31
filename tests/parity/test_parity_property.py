"""Differential properties: the two compute backends must agree on generated input.

The fixed corpus in corpus.py proves the cases someone thought to write down.
These prove the cases nobody did. Every property compares *outcomes* — returned
value or raised exception — so a Rust panic crossing PyO3 is a parity failure
with a minimized repro attached, not a mysterious crash.

On divergence: minimize, add the bytes to corpus.py as a named case, and open an
issue. Then pin it with a per-example `@example(...).xfail(...)` carrying the
issue link — NOT a test-level `@pytest.mark.xfail`, which stops the property
searching entirely (the explicit phase raises before the generate phase runs, so
the test explores nothing and reports no statistics). Reach for a test-level
xfail only when the divergence rate is too high to enumerate, and say so in the
reason. Do not weaken a property to make it pass.
"""

from hypothesis import event, example, given
from hypothesis import strategies as st
from strategies import (
    PANIC_EXCEPTION_NAME,
    GeneratedCSV,
    column_names,
    csv_bytes,
    csv_file,
    outcome,
    raw_bytes,
)

from datagrunt import _native as rs
from datagrunt.core.csv_io import _compute_python as py

# Functions taking only a path.
PATH_ONLY = [
    "is_legacy_mac_newlines",
    "first_row",
    "count_leading_comments",
    "count_leading_physical_lines_before_header",
    "infer_delimiter",
    "probe_csv_header",
]

# #317 is fixed, so every path-only function is back in the value-parity search.
# The function was held out while it carried an open divergence: a generated hit
# does not merely fail once, it writes the falsifying example to
# .hypothesis/examples, the deep job's actions/cache carries that database
# forward, and Hypothesis's `reuse` phase then replays the stored failure and
# stops before `generate` ever runs — a property reporting activity while
# searching nothing. Keep that in mind before pinning any future divergence at
# the test level rather than fixing it.


def _record(gen):
    """Surface which seams this example hit, for --hypothesis-show-statistics."""
    for seam in gen.seams:
        event(seam)


@given(gen=csv_bytes(), fn_name=st.sampled_from(PATH_ONLY))
def test_path_only_functions_agree(gen, fn_name):
    _record(gen)
    event(f"fn:{fn_name}")
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(getattr(rs, fn_name), path) == outcome(getattr(py, fn_name), path), fn_name


@given(gen=csv_bytes(), limit=st.integers(min_value=0, max_value=200))
def test_leading_rows_agrees(gen, limit):
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(rs.leading_rows, path, limit) == outcome(py.leading_rows, path, limit)


@example(
    # #318's minimized falsifying example, kept as a permanent regression case
    # now that it is fixed. U+00B2 is a word character to CPython (category No)
    # but was not to fancy-regex, so the sniffer's [^\w\n"'] delimiter class
    # disagreed about whether it could be a delimiter at all.
    gen=GeneratedCSV(
        data=b'"\'"\'\n"\xc2\xb2\'"',
        suffix=".csv",
        seams=frozenset({"embedded-delimiter", "lf", "no-trailing-newline", "quoted", "ragged"}),
    ),
    delimiter=None,
)
@given(gen=csv_bytes(), delimiter=st.one_of(st.none(), st.sampled_from([",", ";", "|", "\t", ":", " "])))
def test_sniff_dialect_agrees(gen, delimiter):
    """Compares the RAW dicts, including `delimiter`.

    The corpus suite normalizes both sides through dialect_properties_from_rust,
    which drops `delimiter` — so sniff_dialect's own delimiter output has never
    been compared. This closes that gap.
    """
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        args = (path,) if delimiter is None else (path, delimiter)
        assert outcome(rs.sniff_dialect, *args) == outcome(py.sniff_dialect, *args)


@given(gen=csv_bytes(), delimiter=st.sampled_from([",", ";", "|", "\t", ":", " ", "x"]))
def test_row_count_with_header_agrees(gen, delimiter):
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(rs.row_count_with_header, path, delimiter) == outcome(py.row_count_with_header, path, delimiter)


@given(gen=csv_bytes(), delimiter=st.sampled_from([",", ";", "|", "\t", ":", " ", "x"]))
def test_check_ragged_agrees(gen, delimiter):
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(rs.check_ragged, path, delimiter) == outcome(py.check_ragged, path, delimiter)


@given(names=column_names())
def test_normalize_columns_agrees(names):
    """Takes a list, not a path — duplicates and post-normalization collisions
    are the interesting cases."""
    event(f"n_names:{len(names)}")
    assert outcome(rs.normalize_columns, names) == outcome(py.normalize_columns, names)


@example(
    # #317's minimized falsifying example, kept as a permanent regression case
    # now that it is fixed. Bytes that decode to "" under errors="ignore" with
    # no line terminator: Rust used to emit a phantom final line here because
    # its EOF-flush gate was byte-level while a line is decoded text.
    data=b"\x80",
    fn_name="count_leading_physical_lines_before_header",
)
@given(data=raw_bytes(), fn_name=st.sampled_from(PATH_ONLY))
def test_arbitrary_bytes_do_not_diverge(data, fn_name):
    """Crash hunt, value-parity half. Most examples are garbage both backends
    reject identically; the value is the one that does not.

    Measured at 80,000 raw_bytes calls: zero divergences.
    """
    with csv_file(data, ".csv") as path:
        assert outcome(getattr(rs, fn_name), path) == outcome(getattr(py, fn_name), path), fn_name


def _panicked(fn, path) -> bool:
    """True if the call died the one way only the Rust side can."""
    return outcome(fn, path) == ("raised", PANIC_EXCEPTION_NAME)


@given(data=raw_bytes(), fn_name=st.sampled_from(PATH_ONLY))
def test_arbitrary_bytes_never_panic(data, fn_name):
    """Crash hunt, panic-freedom half — over ALL of PATH_ONLY.

    A panic crossing PyO3 is a DoS, and this is the only place arbitrary bytes
    reach every path-only function, so the set here must stay complete.

    Asserting panic-freedom rather than equality is what keeps it complete:
    the property is immune to value divergences, so a future open bug could
    hold a function out of the value-parity property above (as #317 once did)
    without ever costing crash coverage here.
    """
    with csv_file(data, ".csv") as path:
        # Only the Rust side is asserted. PanicException comes from PyO3, so the
        # pure-Python oracle cannot raise it — asserting it there would be a
        # permanently-true check masquerading as coverage.
        assert not _panicked(getattr(rs, fn_name), path), f"{fn_name} panicked on {data!r}"
