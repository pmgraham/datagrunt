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

import pytest
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

# The one function carrying an open, known divergence (#317).
KNOWN_DIVERGENT_FN = "count_leading_physical_lines_before_header"

# What the VALUE-PARITY properties sample. KNOWN_DIVERGENT_FN is held out for as
# long as #317 is open, because a hit there does not merely fail once — it stops
# the search permanently. On the first hit Hypothesis writes the falsifying
# example to .hypothesis/examples, the deep job's actions/cache carries that
# database into every later run, and the `reuse` phase then replays the stored
# failure and stops before `generate` ever executes. The property keeps reporting
# activity while searching nothing.
#
# Both generators reach #317: csv_bytes at roughly 1-in-20,000 examples, and
# raw_bytes far faster — it rediscovers b"\x80" within ~75 examples, so the ci
# profile alone would trip it.
#
# Nothing is left uncovered meanwhile. The divergence stays strictly asserted by
# the pinned .xfail() example on test_arbitrary_bytes_do_not_diverge below and by
# test_parity_probes.py's per-case strict xfail over the corpus case
# invalid_utf8_only_no_newline.csv; panic-freedom for the held-out function stays
# covered by test_arbitrary_bytes_never_panic, which samples the full set.
#
# RESTORE KNOWN_DIVERGENT_FN TO THIS LIST WHEN #317 LANDS.
# Measured before holding it out: 60,000 csv_bytes and 80,000 raw_bytes calls
# against the other five functions produced zero divergences.
PATH_ONLY_AGREEING = [fn for fn in PATH_ONLY if fn != KNOWN_DIVERGENT_FN]


def _record(gen):
    """Surface which seams this example hit, for --hypothesis-show-statistics."""
    for seam in gen.seams:
        event(seam)


@given(gen=csv_bytes(), fn_name=st.sampled_from(PATH_ONLY_AGREEING))
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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "sniff_dialect's delimiter class is [^\\w\\n\"'], and CPython's \\w (str.isalnum()-based) "
        "disagrees with fancy-regex's \\w on category No (U+00B2) and on marks (U+0301). The blast "
        "radius is NOT delimiter-only: measured over 20,000 calls the divergences were delimiter 71, "
        "delimiter+doublequote 8, delimiter+skipinitialspace 4, doublequote alone 1, "
        "delimiter+quotechar 1, and 23 where Rust returns a dialect while CPython returns None. "
        "doublequote, quotechar and skipinitialspace ARE fields the corpus dialect tests compare, and "
        "a None-vs-dialect flip is a behavior change, reachable in production from "
        "src/datagrunt/core/databases/databases.py:125 (CSVDialect(self.filepath), no delimiter). "
        "The rate is 1 in 137 generated examples — too high to pin per-example the way #317 is — so "
        "this stays a TEST-LEVEL xfail, which means the property explores ZERO generated examples "
        "until #318 is fixed. Corpus case sniff_delimiter_word_class.csv. See #318"
    ),
)
@example(
    # The minimized falsifying example, pinned so this fails at every profile
    # rather than only where the search happens to reach it (it was found at the
    # deep profile, not at ci's 100 examples). Keep as a regression case once
    # #318 is fixed and the xfail comes off.
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
    # The minimized falsifying example for #317, pinned so the failure does not
    # depend on the search reaching it under a given profile or Hypothesis
    # version. Keep as a regression case once #317 is fixed and the .xfail()
    # comes off.
    #
    # PER-EXAMPLE .xfail(), not a test-level @pytest.mark.xfail: a test-level
    # marker made this property completely inert. The explicit phase runs before
    # the generate phase, so this example raised, pytest recorded the expected
    # failure, and generation never happened — the test emitted no statistics
    # block at all. .xfail() scopes the expectation to this one example and
    # still asserts it strictly (Hypothesis errors if it stops failing), so the
    # search runs and #317 stays pinned.
    #
    # KNOWN_DIVERGENT_FN is deliberately outside the sampled_from domain below.
    # Explicit examples bypass the strategy, so this still runs; generation must
    # not reach it, because raw_bytes() rediscovers b"\x80" within ~75 examples
    # and a generate-phase hit is NOT covered by this .xfail() — it is an
    # ordinary failure that would redden the PR gate and poison the example
    # database. Panic coverage for the held-out function is preserved by
    # test_arbitrary_bytes_never_panic below.
    data=b"\x80",
    fn_name=KNOWN_DIVERGENT_FN,
).xfail(
    raises=AssertionError,
    reason=(
        "count_leading_physical_lines_before_header diverges on bytes that decode to '' under "
        "errors='ignore' with no line terminator (b'\\x80'): Rust counts a final empty line (1), "
        "CPython iterates decoded text and yields none (0). Corpus case "
        "invalid_utf8_only_no_newline.csv. See #317"
    ),
)
@given(data=raw_bytes(), fn_name=st.sampled_from(PATH_ONLY_AGREEING))
def test_arbitrary_bytes_do_not_diverge(data, fn_name):
    """Crash hunt, value-parity half. Most examples are garbage both backends
    reject identically; the value is the one that does not.

    Measured at 80,000 raw_bytes calls against these five functions: zero
    divergences.
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
    reach every path-only function, so the set here must stay complete even
    while #317 holds KNOWN_DIVERGENT_FN out of the value-parity property above.
    Asserting panic-freedom rather than equality is what makes that possible:
    #317 is a value divergence, invisible to this assertion, so no known bug
    blocks the search.
    """
    with csv_file(data, ".csv") as path:
        assert not _panicked(getattr(rs, fn_name), path), f"{fn_name} panicked on {data!r}"
        assert not _panicked(getattr(py, fn_name), path), f"{fn_name} panicked on {data!r}"
