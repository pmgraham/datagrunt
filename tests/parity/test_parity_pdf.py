"""Differential parity tests between `datagrunt._native` and `datagrunt.pdf._compute_python`."""

from __future__ import annotations

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pdf_tests.test_stream_decode import from_bits, lzw_chain, lzw_chain_bits, lzw_encode, lzw_two_readings, pack

from datagrunt import _native
from datagrunt.pdf import _compute, _compute_python
from datagrunt.pdf._compute_protocol import PDFComputeBackendProtocol

PDF_BACKEND_FUNCTIONS = (
    "pdf_char_bgr_medians",
    "pdf_union_area",
    "pdf_char_reason_runs",
    "pdf_snap_and_merge_segments",
    "pdf_lzw_decode",
)


def test_pdf_protocol_declares_every_pdf_backend_function() -> None:
    declared = {name for name in dir(PDFComputeBackendProtocol) if not name.startswith("_")}
    assert declared == set(PDF_BACKEND_FUNCTIONS)


def test_both_pdf_backends_expose_every_protocol_function() -> None:
    for mod in (_native, _compute_python):
        missing = [fn for fn in PDF_BACKEND_FUNCTIONS if not callable(getattr(mod, fn, None))]
        assert not missing, f"{mod.__name__} is missing {missing}"


def test_compute_backend_switch_and_env_toggle() -> None:
    _compute.set_disable_rust(False)
    assert _compute.backend() is _native

    with _compute.rust_disabled():
        assert _compute.backend() is _compute_python
    assert _compute.backend() is _native

    _compute.set_disable_rust(True)
    try:
        assert _compute.backend() is _compute_python
    finally:
        _compute.set_disable_rust(False)


@pytest.mark.parametrize("seed", range(25))
def test_parity_pdf_char_bgr_medians(seed: int) -> None:
    rng = random.Random(seed)
    px_width = rng.randint(1, 80)
    px_height = rng.randint(1, 80)
    pixels = bytes(rng.randrange(256) for _ in range(px_width * px_height * 3))
    scale_x = rng.uniform(0.5, 3.0)
    scale_y = rng.uniform(0.5, 3.0)
    boxes: list[tuple[float, float, float, float]] = []
    for _ in range(rng.randint(0, 40)):
        x0 = rng.uniform(-10.0, 60.0)
        y0 = rng.uniform(-10.0, 60.0)
        x1 = x0 + rng.uniform(0.0, 45.0)
        y1 = y0 + rng.uniform(0.0, 45.0)
        boxes.append((x0, y0, x1, y1))

    got_rust = _native.pdf_char_bgr_medians(pixels, px_width, px_height, scale_x, scale_y, boxes)
    got_py = _compute_python.pdf_char_bgr_medians(pixels, px_width, px_height, scale_x, scale_y, boxes)
    assert got_rust == got_py


@given(
    boxes=st.lists(
        st.tuples(
            st.floats(min_value=-100.0, max_value=500.0, allow_nan=False, allow_infinity=False),
            st.floats(min_value=-100.0, max_value=500.0, allow_nan=False, allow_infinity=False),
            st.floats(min_value=0.1, max_value=200.0, allow_nan=False, allow_infinity=False),
            st.floats(min_value=0.1, max_value=200.0, allow_nan=False, allow_infinity=False),
        ).map(lambda t: (t[0], t[1], t[0] + t[2], t[1] + t[3])),
        min_size=0,
        max_size=40,
    )
)
def test_parity_pdf_union_area(boxes: list[tuple[float, float, float, float]]) -> None:
    got_rust = _native.pdf_union_area(boxes)
    got_py = _compute_python.pdf_union_area(boxes)
    assert got_rust == pytest.approx(got_py, rel=1e-12, abs=1e-12)


@pytest.mark.parametrize("no_paint", [False, True])
@pytest.mark.parametrize("seed", range(20))
def test_parity_pdf_char_reason_runs(no_paint: bool, seed: int) -> None:
    rng = random.Random(seed)
    width, height = 400.0, 300.0
    under = [
        (rng.uniform(0.0, 200.0), rng.uniform(0.0, 150.0), rng.uniform(200.0, 400.0), rng.uniform(150.0, 300.0))
        for _ in range(rng.randint(0, 6))
    ]
    char_boxes: list[tuple[float, float, float, float]] = []
    for _ in range(rng.randint(0, 80)):
        x0 = rng.uniform(-5.0, 405.0)
        y0 = rng.uniform(-5.0, 305.0)
        char_boxes.append((x0, y0, x0 + rng.uniform(2.0, 10.0), y0 + rng.uniform(4.0, 14.0)))

    got_rust = _native.pdf_char_reason_runs(char_boxes, width, height, no_paint, under)
    got_py = _compute_python.pdf_char_reason_runs(char_boxes, width, height, no_paint, under)
    assert got_rust == got_py


@pytest.mark.parametrize("seed", range(30))
def test_parity_pdf_snap_and_merge_segments(seed: int) -> None:
    rng = random.Random(seed)
    segments: list[tuple[float, float, float]] = []
    for _ in range(rng.randint(0, 60)):
        pos = rng.choice([10.0, 10.5, 11.8, 50.0, 50.2, 100.0]) + rng.uniform(-0.4, 0.4)
        start = rng.uniform(0.0, 200.0)
        end = start + rng.uniform(1.0, 80.0)
        segments.append((pos, start, end))

    got_rust = _native.pdf_snap_and_merge_segments(segments, 2.0)
    got_py = _compute_python.pdf_snap_and_merge_segments(segments, 2.0)
    assert len(got_rust) == len(got_py)
    for r_seg, p_seg in zip(got_rust, got_py, strict=True):
        assert r_seg == pytest.approx(p_seg, rel=1e-12, abs=1e-12)


@pytest.mark.parametrize("early", [1, 0])
@pytest.mark.parametrize(
    "raw,max_output",
    [
        (lzw_encode(b"Hello World! " * 500, 1), 1 << 20),
        (lzw_encode(bytes(range(256)) * 120, 0, clear_when_full=False), 1 << 20),
        (lzw_chain(2000), 810),
        (lzw_two_readings(200), 810),
        (from_bits(lzw_chain_bits(3800) + "".join(format(c, "012b") for c in (4095,) * 3 + (300, 301))), 1 << 22),
        (pack([(65, 9), (300, 9), (259, 9), (66, 9)]), 1024),
        (pack([(258, 9), (65, 9)]), 1024),
        (b"", 1024),
    ],
)
def test_parity_pdf_lzw_decode(early: int, raw: bytes, max_output: int) -> None:
    got_rust = _native.pdf_lzw_decode(raw, early, max_output)
    got_py = _compute_python.pdf_lzw_decode(raw, early, max_output)
    assert got_rust == got_py
