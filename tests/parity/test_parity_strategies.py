"""Meta-tests for the property-testing machinery itself.

A property suite that only ever generates one shape passes thousands of times
and proves nothing — and its exit code is identical to a working one. These
tests guard the machinery so the properties in test_parity_property.py mean
something.
"""

from hypothesis import settings


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
