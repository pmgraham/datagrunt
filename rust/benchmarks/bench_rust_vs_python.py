"""Benchmark datagrunt._native (Rust) vs csvcomponents (Python), per component.

Usage:
    .venv/bin/python rust/benchmarks/bench_rust_vs_python.py --scales small medium
    .venv/bin/python rust/benchmarks/bench_rust_vs_python.py --scales small medium large

Generates CSVs under rust/benchmarks/data/ (gitignored), times each component
for both implementations (median of N runs, warm cache, same process), and
writes a markdown report to rust/benchmarks/results/.
"""

import argparse
import statistics
import time
from datetime import date
from pathlib import Path

import polars as pl

from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io.csvcomponents import (
    CSVColumnNameNormalizer,
    CSVDelimiter,
    CSVDialect,
    CSVRows,
    _check_csv_ragged_and_warn,
    _count_leading_comments,
)

DATA_DIR = Path(__file__).parent / "data"
RESULTS_DIR = Path(__file__).parent / "results"

# rows -> (approx size, repeats)
SCALES = {
    "small": (12_000, 15),       # ~1 MB
    "medium": (1_200_000, 5),    # ~100 MB
    "large": (24_000_000, 3),    # ~2 GB
}


def generate_csv(filepath: Path, num_rows: int) -> None:
    """Same shape as scripts/test_csv_scale.py: 6 mixed-type columns."""
    if filepath.exists():
        return
    df = pl.select(pl.int_range(1, num_rows + 1, dtype=pl.Int64).alias("id"))
    df = df.select(
        pl.col("id"),
        (pl.lit("User_") + pl.col("id").cast(pl.Utf8)).alias("name"),
        (((pl.col("id") * 1.1) % 100).round(2)).alias("score"),
        pl.lit("2026-06-12 00:00:00").alias("timestamp"),
        pl.when(pl.col("id") % 3 == 0).then(pl.lit("A"))
        .when(pl.col("id") % 3 == 1).then(pl.lit("B"))
        .otherwise(pl.lit("C")).alias("category"),
        pl.lit("This is a description for the benchmark row data.").alias("description"),
    )
    df.write_csv(filepath)


def timed(fn, repeats: int) -> float:
    """Median wall-clock seconds over `repeats` runs."""
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples)


def components(path: Path):
    """(name, python_callable, rust_callable) per benchmarked component.

    Fresh objects every call so cached_properties can't fake the numbers.
    Note: Python row_count_with_header internally re-infers the delimiter;
    the Rust pairing does the same (infer + count) to keep the comparison fair.
    """
    path_str = str(path)
    wide_columns = [f"Column Name #{i} (raw)" for i in range(5_000)]
    return [
        ("infer_delimiter",
         lambda: CSVDelimiter(path).delimiter,
         lambda: datagrunt_rs.infer_delimiter(path_str)),
        ("first_row",
         lambda: CSVRows(path).first_row,
         lambda: datagrunt_rs.first_row(path_str)),
        ("leading_rows(5)",
         lambda: CSVRows(path).leading_rows(5),
         lambda: datagrunt_rs.leading_rows(path_str, 5)),
        ("count_leading_comments",
         lambda: _count_leading_comments(path),
         lambda: datagrunt_rs.count_leading_comments(path_str)),
        ("sniff_dialect",
         lambda: CSVDialect(path).quotechar,
         lambda: datagrunt_rs.sniff_dialect(path_str)),
        ("row_count_with_header",
         lambda: CSVRows(path).row_count_with_header,
         lambda: datagrunt_rs.row_count_with_header(
             path_str, datagrunt_rs.infer_delimiter(path_str))),
        ("check_ragged",
         lambda: _check_csv_ragged_and_warn(path, ","),
         lambda: datagrunt_rs.check_ragged(path_str, ",")),
        ("normalize_columns(5000)",
         lambda: CSVColumnNameNormalizer("unused.csv", columns=wide_columns).columns_normalized,
         lambda: datagrunt_rs.normalize_columns(wide_columns)),
    ]


def fmt_seconds(s: float) -> str:
    return f"{s * 1000:.3f} ms" if s < 1 else f"{s:.3f} s"


def run_scale(scale: str, report_lines: list[str]) -> None:
    num_rows, repeats = SCALES[scale]
    path = DATA_DIR / f"bench_{scale}.csv"
    print(f"[{scale}] generating {num_rows:,} rows ...")
    generate_csv(path, num_rows)
    size_mb = path.stat().st_size / 1024 / 1024
    report_lines += [
        "",
        f"## Scale: {scale} ({num_rows:,} rows, {size_mb:,.1f} MB, median of {repeats})",
        "",
        "| Component | Python | Rust | Speedup |",
        "|---|---|---|---|",
    ]
    for name, py_fn, rust_fn in components(path):
        py_fn()   # correctness/warm-up call on both sides before timing
        rust_fn()
        py_time = timed(py_fn, repeats)
        rust_time = timed(rust_fn, repeats)
        speedup = py_time / rust_time if rust_time > 0 else float("inf")
        report_lines.append(
            f"| {name} | {fmt_seconds(py_time)} | {fmt_seconds(rust_time)} | {speedup:,.1f}x |"
        )
        print(f"  {name}: py={fmt_seconds(py_time)} rust={fmt_seconds(rust_time)} ({speedup:,.1f}x)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scales", nargs="+", choices=list(SCALES), default=["small", "medium"])
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    RESULTS_DIR.mkdir(exist_ok=True)
    report = [
        "# Rust vs Python: csvcomponents benchmark",
        "",
        f"Date: {date.today().isoformat()}. Warm cache, same process, fresh objects per call.",
        "Python row_count/dialect include their internal delimiter inference;",
        "Rust pairings replicate that for a like-for-like comparison.",
    ]
    for scale in args.scales:
        run_scale(scale, report)
    out = RESULTS_DIR / f"{date.today().isoformat()}-rust-vs-python.md"
    out.write_text("\n".join(report) + "\n")
    print(f"\nReport written to {out}")


if __name__ == "__main__":
    main()
