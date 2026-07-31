"""Regression tests for issue #325.

``leading_rows`` promises "up to ``limit``" rows but checked the cap *after*
appending, so ``limit=0`` returned one row. Both backends carried the defect
identically, which is exactly what differential parity cannot see — so these
tests assert the contract against each backend directly rather than against
their agreement.
"""

import pytest

from datagrunt import _native
from datagrunt.core.csv_io import _compute_python

backends = pytest.mark.parametrize("backend", (_native, _compute_python), ids=("rust", "python"))

ROWS = ["name,age", "Alice,30", "Bob,25"]


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / "sample.csv"
    path.write_text("\n".join(ROWS) + "\n")
    return str(path)


@backends
def test_limit_zero_returns_no_rows(backend, sample):
    assert backend.leading_rows(sample, 0) == []


@backends
@pytest.mark.parametrize("limit", (1, 2, 3, 4))
def test_never_returns_more_rows_than_the_limit(backend, sample, limit):
    rows = backend.leading_rows(sample, limit)
    assert len(rows) <= limit
    assert rows == ROWS[:limit]


@backends
def test_limit_zero_still_reports_an_unreadable_path(backend, tmp_path):
    """Asking for nothing must not turn a bad path into a silent empty result."""
    with pytest.raises(OSError):
        backend.leading_rows(str(tmp_path / "missing.csv"), 0)
