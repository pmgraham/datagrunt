"""Differential parity harness: every test compares datagrunt_rs (Rust)
against the Python implementation in datagrunt.core.csv_io.csvcomponents."""

import os

import pytest
from corpus import CORPUS
from hypothesis import HealthCheck, settings


@pytest.fixture(scope="session")
def corpus_dir(tmp_path_factory):
    """Write the seeded corpus once per session; yield its directory."""
    root = tmp_path_factory.mktemp("parity_corpus")
    for name, content in CORPUS.items():
        (root / name).write_bytes(content)
    return root


@pytest.fixture(params=sorted(CORPUS), ids=lambda n: n)
def corpus_file(request, corpus_dir):
    """One corpus file per parametrized test case."""
    return corpus_dir / request.param


# derandomize: a random seed in the PR gate produces failures that do not
# reproduce on re-run, which is worse than no test. The scheduled `deep` job is
# where genuinely random exploration happens.
# deadline: every example writes a temp file, so per-example timing is noisy
# and a deadline would be a pure flake source.
settings.register_profile("ci", max_examples=100, derandomize=True, deadline=None)
settings.register_profile("dev", max_examples=200, deadline=None)
settings.register_profile(
    "deep",
    max_examples=10_000,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))
