"""Runtime guards for the compute-backend contract.

The contract itself is enforced statically by mypy (see ``_compute_protocol``).
These tests cover what mypy cannot see: that the compiled module and the oracle
actually expose the members at runtime, and that the hand-written stub has not
drifted from the extension it describes.
"""

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


def test_protocol_declares_every_backend_function():
    """The Protocol must name exactly the functions both backends share."""
    declared = {name for name in dir(ComputeBackendProtocol) if not name.startswith("_")}
    assert declared == set(BACKEND_FUNCTIONS)
