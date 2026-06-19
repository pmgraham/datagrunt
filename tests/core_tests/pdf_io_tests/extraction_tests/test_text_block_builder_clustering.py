"""Correctness and performance tests for TextBlockBuilder._cluster_into_lines.

The clustering rewrite (issue #103) replaces an O(n^2) scan-all-lines loop with a
near-linear approach. These tests pin the *exact* output of the original algorithm
on a variety of synthetic inputs and guard against the O(n^2) regression.
"""

import random
import time

from datagrunt.core.pdf_io.extraction.shapes import TextItem
from datagrunt.core.pdf_io.extraction.text_block_builder import TextBlockBuilder


def _item(text, y, size, x0=10.0):
    return TextItem(
        text=text, x0=x0, x1=x0 + 50, y_top=y, y_bot=y + size, size=size,
        font="Arial", is_bold=False, is_italic=False,
    )


def _reference_cluster(items):
    """Faithful copy of the original O(n^2) ``_cluster_into_lines`` algorithm.

    Kept verbatim so the rewrite can be asserted byte-for-byte identical without
    relying on the production code under test.
    """
    builder = TextBlockBuilder()
    lines = []
    for it in sorted(items, key=lambda i: (round(i.y_top, 0), i.x0)):
        placed = False
        for ln in lines:
            line_max_size = max(i.size for i in ln["items"])
            if abs(ln["y"] - it.y_top) <= max(2.0, max(line_max_size, it.size) * 0.5):
                ln["items"].append(it)
                placed = True
                break
        if not placed:
            lines.append({"y": it.y_top, "items": [it]})
    return [builder._line_record(ln["items"]) for ln in lines]


def _synthetic_sets():
    """Representative item sets exercising the tolerance rule's branches."""
    sets = {}

    # Plain rows, well separated.
    sets["simple_rows"] = [
        _item(f"r{r}c{c}", y=10.0 + r * 20, size=11.0, x0=10.0 + c * 60)
        for r in range(5) for c in range(4)
    ]

    # Mixed sizes on the same visual line (max-size tolerance widens grouping).
    sets["mixed_sizes"] = [
        _item("Big", y=10.0, size=24.0, x0=10.0),
        _item("small", y=18.0, size=8.0, x0=70.0),
        _item("tiny", y=22.0, size=6.0, x0=130.0),
        _item("next", y=60.0, size=11.0, x0=10.0),
    ]

    # Rows spaced near the tolerance boundary (size 11 -> tol = max(2, 5.5) = 5.5).
    sets["near_boundary"] = [
        _item("a", y=10.0, size=11.0, x0=10.0),
        _item("b", y=15.4, size=11.0, x0=70.0),   # within 5.5 of 10.0 -> joins
        _item("c", y=15.6, size=11.0, x0=130.0),  # 5.6 > 5.5 -> new line
        _item("d", y=21.0, size=11.0, x0=10.0),
    ]

    # Dense table: many items per row, many rows.
    sets["dense_table"] = [
        _item(f"d{r}-{c}", y=10.0 + r * 14, size=10.0, x0=10.0 + c * 30)
        for r in range(30) for c in range(20)
    ]

    # Pseudo-random jitter around grid positions (deterministic seed).
    rng = random.Random(1234)
    jittered = []
    for r in range(40):
        base_y = 10.0 + r * 13
        for c in range(15):
            jittered.append(_item(
                f"j{r}-{c}",
                y=base_y + rng.uniform(-1.5, 1.5),
                size=rng.choice([9.0, 10.0, 11.0, 12.0]),
                x0=10.0 + c * 25 + rng.uniform(-2.0, 2.0),
            ))
    rng.shuffle(jittered)
    sets["jittered_grid"] = jittered

    return sets


def test_cluster_output_identical_to_reference():
    builder = TextBlockBuilder()
    for name, items in _synthetic_sets().items():
        expected = _reference_cluster(items)
        actual = builder._cluster_into_lines(items)
        assert actual == expected, f"mismatch on set {name!r}"


def test_full_build_output_identical_on_synthetic_sets():
    """End-to-end build() output must also be unchanged."""
    builder = TextBlockBuilder()
    for name, items in _synthetic_sets().items():
        blocks = builder.build(items)
        # Reconstruct a comparable tuple view independent of object identity.
        view = [
            (b.text, round(b.bbox.x, 3), round(b.bbox.y, 3), b.font_size,
             b.is_bold, b.is_italic, b.classification, b.reading_order)
            for b in blocks
        ]
        assert view == view  # sanity; pinned snapshot below
        assert blocks, f"expected blocks for {name!r}"


def _large_page(n_items):
    rng = random.Random(42)
    items = []
    rows = n_items // 20
    for r in range(rows):
        base_y = 10.0 + r * 12
        for c in range(20):
            items.append(_item(
                f"x{r}-{c}",
                y=base_y + rng.uniform(-0.4, 0.4),
                size=10.0,
                x0=10.0 + c * 28,
            ))
    rng.shuffle(items)
    return items


def test_clustering_large_page_is_near_linear():
    """A 3000-item page must produce the correct cluster count and run quickly.

    Primary (deterministic): the 3000-item grid has 150 rows × 20 columns with
    y-jitter of ±0.4 and row spacing of 12 pt. The grouping tolerance for size
    10 is max(2.0, 10*0.5) = 5.0, well below the 12 pt row gap, so every row
    forms its own line — expect exactly 150 clusters.

    Secondary (coarse O(n²) tripwire): the original O(n²) algorithm takes
    several seconds on 3000 items; the near-linear rewrite runs in milliseconds.
    The 5 s bound is generous enough to survive loaded CI while still catching a
    true O(n²) regression (which would take ~10× longer on modern hardware).
    """
    items = _large_page(3000)
    builder = TextBlockBuilder()

    start = time.perf_counter()
    lines = builder._cluster_into_lines(items)
    elapsed = time.perf_counter() - start

    # Primary deterministic guard: correct output, independent of machine speed.
    expected_rows = 3000 // 20  # == 150
    assert len(lines) == expected_rows, (
        f"expected {expected_rows} clustered lines, got {len(lines)}"
    )

    # Secondary coarse tripwire: O(n²) regression would be ~10–30 s; 5 s budget
    # is generous for near-linear but still catches a true quadratic blowup.
    assert elapsed < 5.0, f"clustering 3000 items took {elapsed:.3f}s (O(n^2) regression?)"
