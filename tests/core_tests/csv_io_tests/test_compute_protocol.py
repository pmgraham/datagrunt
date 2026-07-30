"""Runtime guards for the compute-backend contract.

The contract itself is enforced statically by mypy (see ``_compute_protocol``).
These tests cover what mypy cannot see: that the compiled module and the oracle
actually expose the members at runtime, and that the hand-written stub has not
drifted from the extension it describes.
"""

import ast
from pathlib import Path

import datagrunt
from datagrunt import _native
from datagrunt.core.csv_io import _compute_python
from datagrunt.core.csv_io._compute_protocol import ComputeBackendProtocol

BACKEND_FUNCTIONS = (
    "is_legacy_mac_newlines",
    "leading_rows",
    "first_row",
    "count_leading_comments",
    "count_leading_physical_lines_before_header",
    "normalize_columns",
    "infer_delimiter",
    "row_count_with_header",
    "check_ragged",
    "sniff_dialect",
    "probe_csv_header",
)

STUB_PATH = Path(datagrunt.__file__).parent / "_native.pyi"


def test_protocol_declares_every_backend_function():
    """The Protocol must name exactly the functions both backends share."""
    declared = {name for name in dir(ComputeBackendProtocol) if not name.startswith("_")}
    assert declared == set(BACKEND_FUNCTIONS)


def _stub_function_names() -> set[str]:
    tree = ast.parse(STUB_PATH.read_text())
    return {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}


def test_stub_matches_the_compiled_module():
    """Adding a #[pyfunction] without updating the stub must fail here."""
    compiled = {n for n in dir(_native) if not n.startswith("_") and callable(getattr(_native, n))}
    assert _stub_function_names() == compiled


def test_both_backends_expose_every_protocol_function():
    """Neither backend may quietly drop a member of the shared surface."""
    for backend in (_native, _compute_python):
        missing = [fn for fn in BACKEND_FUNCTIONS if not callable(getattr(backend, fn, None))]
        assert not missing, f"{backend.__name__} is missing {missing}"
