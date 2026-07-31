"""Meta-tests for the property-testing machinery itself.

A property suite that only ever generates one shape passes thousands of times
and proves nothing — and its exit code is identical to a working one. These
tests guard the machinery so the properties in test_parity_property.py mean
something.
"""

import os

import pytest
from hypothesis import given, settings
from strategies import (
    SEAM_LABELS,
    GeneratedCSV,
    column_names,
    csv_bytes,
    csv_file,
    outcome,
    raw_bytes,
)


def test_hypothesis_profiles_are_registered():
    """CI needs a derandomized profile; the scheduled job needs a deep one."""
    for name in ("ci", "dev", "deep"):
        assert settings.get_profile(name) is not None, name


def test_ci_profile_is_reproducible():
    """A random seed in the PR gate means unreproducible red builds."""
    ci = settings.get_profile("ci")
    assert ci.derandomize is True
    assert ci.deadline is None, "file I/O per example makes deadlines a flake source"


def test_deep_profile_searches_much_harder_than_ci():
    assert settings.get_profile("deep").max_examples >= 10 * settings.get_profile("ci").max_examples


def test_outcome_reports_a_returned_value():
    assert outcome(lambda: 42) == ("ok", 42)


def test_outcome_reports_the_exception_name():
    def boom():
        raise ValueError("nope")

    assert outcome(boom) == ("raised", "ValueError")


def test_outcome_normalizes_oserror_subclasses():
    """PyO3 maps io errors to bare OSError; CPython raises specific subclasses.

    Comparing exact names would flag that as a divergence on every missing-file
    example. Normalizing to the family keeps the comparison honest without
    hiding anything: PanicException is not an OSError, so panics still surface.
    """

    def missing():
        raise FileNotFoundError(2, "No such file")

    assert outcome(missing) == ("raised", "OSError")


def test_outcome_captures_baseexception_so_hypothesis_can_shrink_it():
    """The safety-critical path: a panic must be captured, not left to escape.

    PyO3 declares PanicException with PyBaseException as its base, so an
    `except Exception` clause misses it. Hypothesis's
    failure_exceptions_to_catch() is (Exception, SystemExit, GeneratorExit),
    which means an escaping panic reddens the suite with no minimized repro.
    Captured, it is an ordinary tuple mismatch that Hypothesis shrinks.
    """

    class FakePanic(BaseException):
        """Stand-in: PanicException itself needs a real panic to construct."""

    def panics():
        raise FakePanic("boom")

    assert outcome(panics) == ("raised", "FakePanic")


@pytest.mark.parametrize("control_flow", [KeyboardInterrupt, SystemExit])
def test_outcome_never_swallows_control_flow_exceptions(control_flow):
    """Broadening to BaseException must not make Ctrl-C uninterruptible."""

    def interrupted():
        raise control_flow

    with pytest.raises(control_flow):
        outcome(interrupted)


def test_csv_file_writes_bytes_and_cleans_up():
    with csv_file(b"a,b\n1,2\n", ".csv") as path:
        with open(path, "rb") as handle:
            assert handle.read() == b"a,b\n1,2\n"
        assert path.endswith(".csv")
    assert not os.path.exists(path)


@given(gen=csv_bytes())
def test_csv_bytes_produces_a_well_formed_example(gen):
    assert isinstance(gen, GeneratedCSV)
    assert isinstance(gen.data, bytes)
    assert gen.suffix.startswith(".")
    assert gen.seams <= SEAM_LABELS


@given(data=raw_bytes())
def test_raw_bytes_produces_bytes(data):
    assert isinstance(data, bytes)


@given(names=column_names())
def test_column_names_produces_a_list_of_str(names):
    assert all(isinstance(n, str) for n in names)


def test_every_seam_is_reachable():
    """THE VACUITY GUARD.

    If a seam becomes unreachable — a branch mis-weighted to near-zero, a
    condition that can never fire — the properties keep passing while silently
    testing less. This test fails loudly instead.
    """
    seen: set[str] = set()

    @given(gen=csv_bytes())
    @settings(max_examples=2000, deadline=None, derandomize=True)
    def collect(gen):
        seen.update(gen.seams)

    collect()
    missing = SEAM_LABELS - seen
    assert not missing, f"unreachable seams in csv_bytes(): {sorted(missing)}"
