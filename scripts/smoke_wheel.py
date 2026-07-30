"""Smoke test for a built datagrunt wheel: the binary loads and the dispatch
toggle works on this platform. Run AFTER `pip install`-ing the wheel, from a
directory that does NOT contain a top-level `datagrunt/` package (so the import
resolves to the installed wheel)."""

import pathlib
import tempfile

import datagrunt
from datagrunt import _native  # the bundled Rust binary must import
from datagrunt.core.csv_io import _compute, _compute_python

stub_path = pathlib.Path(datagrunt.__file__).parent / "_native.pyi"
assert stub_path.is_file(), f"_native.pyi stub missing from installed package: {stub_path}"

csv_path = pathlib.Path(tempfile.mkdtemp()) / "smoke.csv"
csv_path.write_text("a,b,c\n1,2,3\n4,5,6\n")

reader = datagrunt.CSVReader(str(csv_path))
rust_result = (reader.delimiter, len(reader.columns), reader.row_count_with_header)
assert rust_result == (",", 3, 3), f"rust path wrong: {rust_result}"
assert _compute.backend() is _native, "default backend should be Rust"

with _compute.rust_disabled():
    assert _compute.backend() is _compute_python, "toggle did not switch to Python"
    reader2 = datagrunt.CSVReader(str(csv_path))
    py_result = (reader2.delimiter, len(reader2.columns), reader2.row_count_with_header)
    assert py_result == rust_result, f"python path != rust path: {py_result} vs {rust_result}"
assert _compute.backend() is _native, "context manager did not restore Rust"

print(f"smoke OK on {datagrunt.__version__}: {rust_result}; toggle verified; stub present")
