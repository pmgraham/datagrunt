"""Differential parity harness: every test compares datagrunt_rs (Rust)
against the Python implementation in datagrunt.core.csv_io.csvcomponents."""

import pytest

from corpus import CORPUS


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
