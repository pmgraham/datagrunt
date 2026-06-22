"""The Rust CSV-scan #[pyfunction]s release the GIL during heavy file I/O.

``row_count_with_header`` scans the entire file. Before issue #177 it held the
GIL for the whole scan, blocking every other Python thread until it finished.
After wrapping the heavy compute in ``py.detach`` (PyO3's GIL-release API) a
concurrent Python thread runs in parallel for the scan's duration.

Proving GIL release by an *absolute* iteration count is unreliable: even a
GIL-holding C call leaks the GIL in brief slices, so a background loop always
advances *somewhat*. The robust discriminator is the background loop's rate as
a fraction of its unobstructed (free) rate:

* GIL held for the scan  -> background runs at a small fraction of free (~7%).
* GIL released           -> background runs at nearly the free rate (~98%).

The assertion threshold sits in the wide gap between those regimes, so it holds
across machines and core counts without being timing-flaky. ``row_count`` is the
only one of the four wrapped functions that does a true full-file scan
(``check_ragged`` caps at 10k rows; ``infer_delimiter``/``sniff_dialect`` read
only a header sample), so it is the probe here; the others' wrappers are covered
by ``test_concurrent_scans_stay_correct``.
"""

import threading
import time

import pytest

from datagrunt import _native

# Wide rows so a modest row count still yields a multi-megabyte file whose scan
# takes long enough (tens of ms) to measure the background thread's rate.
_ROW = b"alpha,bravo,charlie,delta,echo,foxtrot,golf,hotel\n"
_HEADER = b"col1,col2,col3,col4,col5,col6,col7,col8\n"
_TARGET_BYTES = 48 * 1024 * 1024

# Background must reach at least this fraction of its free rate during the scan
# for the GIL to count as released. Measured: ~0.07 (held) vs ~0.98 (released);
# 0.4 clears the held regime by >5x while tolerating a single-core, time-sharing
# scheduler (which still yields ~0.5) and CI noise.
_MIN_RATE_FRACTION = 0.4


@pytest.fixture
def large_csv(tmp_path):
    """A multi-megabyte CSV whose full scan takes long enough to time."""
    path = tmp_path / "large.csv"
    path.write_bytes(_HEADER + _ROW * (_TARGET_BYTES // len(_ROW)))
    return str(path)


class _BackgroundCounter:
    """A daemon thread running a pure-Python loop that needs the GIL to advance."""

    def __init__(self):
        self._count = 0
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop:
            self._count += 1

    def __enter__(self):
        self._thread.start()
        time.sleep(0.05)  # let the loop reach steady state before sampling
        return self

    def __exit__(self, *exc):
        self._stop = True
        self._thread.join(timeout=1.0)

    def rate_during(self, action):
        """Iterations/sec the background loop achieved while ``action`` ran."""
        before = self._count
        start = time.perf_counter()
        action()
        elapsed = time.perf_counter() - start
        return (self._count - before) / elapsed, elapsed


def test_row_count_releases_gil_during_scan(large_csv):
    with _BackgroundCounter() as counter:
        # Free rate: main thread sleeps, so the background loop owns the GIL.
        free_rate, _ = counter.rate_during(lambda: time.sleep(0.08))
        # Scan rate: best of a few runs, so one scheduler hiccup can't fail it.
        # If the GIL is released the background loop keeps near its free rate.
        scan_rate = 0.0
        scan_elapsed = 0.0
        for _ in range(3):
            rate, elapsed = counter.rate_during(lambda: _native.row_count_with_header(large_csv, ","))
            scan_rate = max(scan_rate, rate)
            scan_elapsed = max(scan_elapsed, elapsed)

    if scan_elapsed < 0.02:
        pytest.skip(f"scan too fast ({scan_elapsed * 1000:.1f}ms) to measure reliably")

    fraction = scan_rate / free_rate
    assert fraction >= _MIN_RATE_FRACTION, (
        f"background thread ran at only {fraction:.0%} of its free rate during "
        f"the scan ({scan_rate / 1e6:.1f}M/s vs {free_rate / 1e6:.1f}M/s) — the "
        f"GIL was not released"
    )


def test_concurrent_scans_stay_correct(large_csv):
    """The GIL-releasing scans return consistent results when run concurrently.

    Guards against any data race or unsafety introduced by releasing the GIL:
    every worker thread must agree with a single-threaded baseline across all
    four wrapped functions.
    """

    def snapshot():
        return (
            _native.row_count_with_header(large_csv, ","),
            _native.check_ragged(large_csv, ","),
            _native.infer_delimiter(large_csv),
            _native.sniff_dialect(large_csv)["delimiter"],
        )

    baseline = snapshot()
    results = []
    results_lock = threading.Lock()

    def worker():
        result = snapshot()
        with results_lock:
            results.append(result)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30.0)

    assert results == [baseline] * 8
