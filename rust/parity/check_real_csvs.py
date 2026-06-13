#!/usr/bin/env python
"""Differential parity checker for *real* CSV files.

Points the same Rust-vs-Python comparison the test suite uses at a directory
(or single file) of your own CSVs, instead of the synthetic fixture corpus.
For every file it runs each exposed ``datagrunt_rs`` function and its Python
``csvcomponents`` counterpart and asserts the outputs are identical. Any
disagreement is reported with the file, the function, and both values.

Usage (run with the project venv, after ``maturin develop``):

    .venv/bin/python rust/parity/check_real_csvs.py /path/to/csvs
    .venv/bin/python rust/parity/check_real_csvs.py /path/to/csvs --recursive
    .venv/bin/python rust/parity/check_real_csvs.py one_file.csv --verbose
    .venv/bin/python rust/parity/check_real_csvs.py /data --quick   # header-only

Exit code is 0 when every file matches, 1 when any divergence is found, so it
can gate a release.
"""

from __future__ import annotations

import argparse
import sys
import time
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import datagrunt_rs
from datagrunt.core.csv_io import csvcomponents as py
from datagrunt.core.csv_io.csvcomponents import (
    CSVColumnNameNormalizer,
    CSVComponents,
    CSVDelimiter,
    CSVDialect,
    CSVRows,
)

DEFAULT_EXTENSIONS = (".csv", ".tsv", ".txt")
LEADING_ROW_LIMITS = (1, 2, 5, 100)
QUOTING_MAP = CSVDialect.QUOTING_MAP


# --- Outcome model ----------------------------------------------------------


class Outcome:
    """The result of calling one side (Rust or Python) of a check.

    Captures either a returned value or a raised exception, so a check that
    raises on one side but not the other is itself a reportable divergence.
    """

    def __init__(self, value=None, error: Exception | None = None):
        self.value = value
        self.error = error

    @classmethod
    def run(cls, fn: Callable) -> "Outcome":
        try:
            return cls(value=fn())
        except Exception as exc:  # noqa: BLE001 - capturing both sides is the point
            return cls(error=exc)

    def describe(self) -> str:
        if self.error is not None:
            return f"{type(self.error).__name__}: {self.error}"
        return repr(self.value)


def outcomes_agree(rust: Outcome, python: Outcome) -> bool:
    """Two outcomes agree if both returned equal values, or both raised the
    same exception type. A value on one side and an error on the other is a
    divergence — exactly the class of bug this checker exists to catch."""
    if rust.error is not None or python.error is not None:
        return (
            rust.error is not None
            and python.error is not None
            and type(rust.error) is type(python.error)
        )
    return rust.value == python.value


# --- The checks (mirror the pytest parity suite exactly) --------------------


@dataclass
class Divergence:
    file: Path
    check: str
    rust: str
    python: str


def build_checks(path: Path) -> list[tuple[str, Callable[[], Outcome], Callable[[], Outcome]]]:
    """Return ``(name, rust_outcome, python_outcome)`` triples for one file.

    Delimiter-dependent checks pass the Python-inferred delimiter to both sides
    so they isolate their own behaviour from delimiter inference (which is
    compared on its own line), matching the test suite's methodology.
    """
    p = str(path)
    checks: list[tuple[str, Callable[[], Outcome], Callable[[], Outcome]]] = []

    def add(name: str, rust_fn: Callable, py_fn: Callable) -> None:
        checks.append((name, lambda: Outcome.run(rust_fn), lambda: Outcome.run(py_fn)))

    # Lightweight probes.
    add("is_legacy_mac_newlines",
        lambda: datagrunt_rs.is_legacy_mac_newlines(p),
        lambda: py._is_legacy_mac_newlines(path))
    add("count_leading_comments",
        lambda: datagrunt_rs.count_leading_comments(p),
        lambda: py._count_leading_comments(path))
    add("count_leading_physical_lines_before_header",
        lambda: datagrunt_rs.count_leading_physical_lines_before_header(p),
        lambda: py._count_leading_physical_lines_before_header(path))
    add("first_row",
        lambda: datagrunt_rs.first_row(p),
        lambda: CSVRows(path).first_row)
    for limit in LEADING_ROW_LIMITS:
        add(f"leading_rows({limit})",
            lambda limit=limit: datagrunt_rs.leading_rows(p, limit),
            lambda limit=limit: CSVRows(path).leading_rows(limit))

    # Delimiter inference.
    add("infer_delimiter",
        lambda: datagrunt_rs.infer_delimiter(p),
        lambda: CSVDelimiter(path).delimiter)

    # Dialect sniffing (unrestricted + delimiter-restricted), via the same
    # property-default mapping the suite uses.
    add("sniff_dialect",
        lambda: _rust_dialect(datagrunt_rs.sniff_dialect(p)),
        lambda: _python_dialect(CSVDialect(path)))
    add("sniff_dialect(restricted)",
        lambda: _rust_dialect(datagrunt_rs.sniff_dialect(p, CSVDelimiter(path).delimiter)),
        lambda: _python_dialect(CSVDialect(path, delimiter=CSVDelimiter(path).delimiter)))

    # Column-name normalization on the file's real header.
    add("normalize_columns",
        lambda: datagrunt_rs.normalize_columns(_file_columns(path)),
        lambda: CSVColumnNameNormalizer(path, columns=_file_columns(path)).columns_normalized)

    return checks


def build_fullscan_checks(path: Path) -> list[tuple[str, Callable, Callable]]:
    """Whole-file checks, separated so ``--quick`` can skip them on huge dirs."""
    p = str(path)
    checks: list[tuple[str, Callable, Callable]] = []

    def add(name: str, rust_fn: Callable, py_fn: Callable) -> None:
        checks.append((name, lambda: Outcome.run(rust_fn), lambda: Outcome.run(py_fn)))

    add("row_count_with_header",
        lambda: datagrunt_rs.row_count_with_header(p, CSVDelimiter(path).delimiter),
        lambda: CSVRows(path).row_count_with_header)
    add("check_ragged",
        lambda: datagrunt_rs.check_ragged(p, CSVDelimiter(path).delimiter),
        lambda: _python_ragged(path, CSVDelimiter(path).delimiter))
    return checks


# --- Helpers that reproduce the test harness's value shaping ----------------


def _rust_dialect(rust_dict) -> dict:
    if rust_dict is None:
        return {
            "quotechar": '"',
            "escapechar": None,
            "doublequote": False,
            "newline_delimiter": "\r\n",
            "skipinitialspace": False,
            "quoting": "quote minimal",
        }
    return {
        "quotechar": rust_dict["quotechar"],
        "escapechar": rust_dict["escapechar"],
        "doublequote": rust_dict["doublequote"],
        "newline_delimiter": rust_dict["lineterminator"],
        "skipinitialspace": rust_dict["skipinitialspace"],
        "quoting": QUOTING_MAP.get(rust_dict["quoting"]),
    }


def _python_dialect(dialect_obj: CSVDialect) -> dict:
    return {
        "quotechar": dialect_obj.quotechar,
        "escapechar": dialect_obj.escapechar,
        "doublequote": dialect_obj.doublequote,
        "newline_delimiter": dialect_obj.newline_delimiter,
        "skipinitialspace": dialect_obj.skipinitialspace,
        "quoting": dialect_obj.quoting,
    }


def _python_ragged(path: Path, delimiter: str) -> bool:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return py._check_csv_ragged_and_warn(path, delimiter)


def _file_columns(path: Path) -> list[str]:
    """The file's parsed header, used to feed both normalizers identically."""
    return CSVComponents(path).columns


# --- File discovery and the run loop ----------------------------------------


def discover_files(target: Path, extensions: tuple[str, ...], recursive: bool) -> list[Path]:
    if target.is_file():
        return [target]
    if not target.is_dir():
        raise SystemExit(f"error: {target} is neither a file nor a directory")
    walker = target.rglob("*") if recursive else target.glob("*")
    files = sorted(
        p for p in walker if p.is_file() and p.suffix.lower() in extensions
    )
    return files


@dataclass
class Report:
    files_checked: int = 0
    files_clean: int = 0
    checks_run: int = 0
    divergences: list[Divergence] = field(default_factory=list)


@dataclass
class CheckResult:
    """One Rust-vs-Python comparison for one file — pass or fail."""

    file: Path
    check: str
    rust: str
    python: str
    agree: bool


def run_file_checks(path: Path | str, quick: bool = False) -> list[CheckResult]:
    """Run every Rust-vs-Python check for one file and return ALL results.

    This is the reusable core used by both the CLI and interactive callers
    (e.g. a Jupyter notebook), which want the full table including the checks
    that passed, not just the divergences.
    """
    path = Path(path)
    checks = build_checks(path)
    if not quick:
        checks = checks + build_fullscan_checks(path)

    results: list[CheckResult] = []
    for name, rust_call, python_call in checks:
        rust = rust_call()
        python = python_call()
        results.append(
            CheckResult(path, name, rust.describe(), python.describe(), outcomes_agree(rust, python))
        )
    return results


def check_file(path: Path, quick: bool, verbose: bool) -> tuple[int, list[Divergence]]:
    results = run_file_checks(path, quick)
    if verbose:
        for r in results:
            print(f"    {'OK ' if r.agree else 'XX '}{r.check}")
    divergences = [Divergence(r.file, r.check, r.rust, r.python) for r in results if not r.agree]
    return len(results), divergences


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("target", type=Path, help="CSV file or directory of CSV files")
    parser.add_argument("--recursive", action="store_true", help="recurse into subdirectories")
    parser.add_argument("--quick", action="store_true",
                        help="skip whole-file scans (row_count, ragged); header checks only")
    parser.add_argument("--ext", default=",".join(DEFAULT_EXTENSIONS),
                        help=f"comma-separated extensions to include (default: {','.join(DEFAULT_EXTENSIONS)})")
    parser.add_argument("--verbose", action="store_true", help="print every check, not just divergences")
    args = parser.parse_args()

    extensions = tuple(e if e.startswith(".") else f".{e}" for e in args.ext.lower().split(","))
    files = discover_files(args.target, extensions, args.recursive)
    if not files:
        print(f"No files matching {extensions} found under {args.target}")
        return 0

    print(f"Checking {len(files)} file(s) — Rust (datagrunt_rs) vs Python (csvcomponents)"
          + (" [quick: header checks only]" if args.quick else ""))
    report = Report()
    started = time.perf_counter()

    for path in files:
        if args.verbose:
            print(f"  {path}")
        n_checks, divergences = check_file(path, args.quick, args.verbose)
        report.files_checked += 1
        report.checks_run += n_checks
        if divergences:
            report.divergences.extend(divergences)
            for d in divergences:
                print(f"  DIVERGENCE  {d.file}  [{d.check}]")
                print(f"              rust   = {d.rust}")
                print(f"              python = {d.python}")
        else:
            report.files_clean += 1

    elapsed = time.perf_counter() - started
    print()
    print("=" * 60)
    print(f"Files checked : {report.files_checked}")
    print(f"Files clean   : {report.files_clean}")
    print(f"Checks run    : {report.checks_run}")
    print(f"Divergences   : {len(report.divergences)}")
    print(f"Elapsed       : {elapsed:.2f}s")
    print("=" * 60)
    if report.divergences:
        print("RESULT: DIVERGENCES FOUND — Rust and Python disagree on the files above.")
        return 1
    print("RESULT: FULL PARITY — Rust matches Python on every file.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
