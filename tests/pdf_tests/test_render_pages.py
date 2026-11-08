"""Tests for the v5 render worker's analysis modes and its multi-page batch (#400).

``render_page(..., colour=True, without_text=True)`` draws a page in 24-bit
BGR with its text objects removed from the worker's load of it, and
``render_pages`` draws several pages of one document in one capped worker.
Expected pixels come from pdfium itself in this process (the same render
call, size and flags), never pinned from one platform's fonts.
"""

import contextvars
import ctypes
import errno
import functools
import io
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import _process, _render_worker, render
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, render_page, render_pages, render_size
from pdf_tests.builders import FormXObject, PageSpec, Rect, TextRun, build_pdf, hand_written_pdf
from pdf_tests.test_pdfium_images import JPX_16, image_xobject
from pdf_tests.test_render import JPX_BOMB, JPX_BOMB_REFUSAL
from pdf_tests.test_render_worker import FakeProcess, Sink, bounded, frame, framed, slow_page

CAP = 1 << 30
TIMEOUT_S = 120.0
RED, BLUE = (1.0, 0.0, 0.0), (0.0, 0.0, 1.0)


def coloured(tmp_path: Path, *, name: str = "coloured.pdf") -> Path:
    """A 400 x 300 pt page: a red box (40, 40)-(240, 140) with red text on it, black text below, blue text in a form."""
    items = (
        Rect(40.0, 40.0, 200.0, 100.0, fill_rgb=RED, stroke=False),
        TextRun("Red on red", x=50.0, y=100.0, size=24.0, rgb=RED),
        TextRun("Black text", x=50.0, y=200.0, size=24.0),
        FormXObject(x=250.0, y=180.0, w=140.0, h=60.0, items=(TextRun("Blue", x=10.0, y=40.0, size=24.0, rgb=BLUE),)),
    )
    return build_pdf(tmp_path / name, [PageSpec(width=400.0, height=300.0, items=items)])


def text_only(tmp_path: Path) -> Path:
    """A 400 x 300 pt page of text only: a line on the page and a line inside a form."""
    items = (
        TextRun("Only text here", x=40.0, y=60.0, size=24.0),
        FormXObject(x=40.0, y=120.0, w=300.0, h=80.0, items=(TextRun("And in a form", x=0.0, y=40.0, size=24.0),)),
    )
    return build_pdf(tmp_path / "text.pdf", [PageSpec(width=400.0, height=300.0, items=items)])


def pdfium_bgr(path: Path, dpi: int, index: int = 0) -> tuple[int, int, bytes]:
    """``(width, height, pixels)`` of a page drawn by pdfium here in 24-bit BGR, with its text, at render_size."""
    page = pdfium.PdfDocument(str(path))[index]
    width, height = render_size(*page.get_size(), dpi)
    buffer = (ctypes.c_ubyte * (width * height * 3))()
    bitmap = pdfium_c.FPDFBitmap_CreateEx(width, height, pdfium_c.FPDFBitmap_BGR, buffer, width * 3)
    try:
        assert pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, width, height, 0xFFFFFFFF)
        pdfium_c.FPDF_RenderPageBitmap(bitmap, page.raw, 0, 0, width, height, 0, 0)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    return width, height, bytes(buffer)


def bgr_at(page: RenderedPage, x: int, y: int) -> tuple[int, ...]:
    at = (y * page.px_width + x) * 3
    return tuple(page.pixels[at : at + 3])


def gray_at(page: RenderedPage, x: int, y: int) -> int:
    return page.pixels[y * page.px_width + x]


def colours_in(page: RenderedPage, box: tuple[int, int, int, int]) -> set[tuple[int, ...]]:
    """The BGR colours of the pixels in ``box`` (x0, y0, x1, y1), ends excluded."""
    x0, y0, x1, y1 = box
    return {bgr_at(page, x, y) for x in range(x0, x1) for y in range(y0, y1)}


@pytest.fixture
def started(monkeypatch):
    """Every subprocess started during the test."""
    workers = []
    real = render.start_worker

    def recorded(command):
        worker, stderr = real(command)
        workers.append(worker)
        return worker, stderr

    monkeypatch.setattr(render, "start_worker", recorded)
    return workers


class TestRenderedPageChannels:
    """A RenderedPage is 8-bit gray (1 channel) by default, or 24-bit BGR (3 channels)."""

    def test_gray_by_default(self):
        assert RenderedPage(pixels=bytes(6), px_width=3, px_height=2).channels == 1

    def test_a_bgr_page_holds_three_bytes_a_pixel(self):
        page = RenderedPage(pixels=bytes(18), px_width=3, px_height=2, channels=3)

        assert (page.px_width, page.px_height, page.channels, len(page.pixels)) == (3, 2, 3, 18)

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"channels": 2}, ValueError, "channels must be 1 (gray) or 3 (BGR), got 2", id="two"),
            pytest.param({"channels": True}, TypeError, "channels must be an int", id="bool"),
            pytest.param({"channels": 3}, ValueError, "pixels must hold 3 x 2 x 3 = 18 bytes, got 6", id="bgr-length"),
        ],
    )
    def test_invalid_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            RenderedPage(**{"pixels": bytes(6), "px_width": 3, "px_height": 2, **fields})


class TestModes:
    """render_page draws a page in BGR, its text removed, on request; by default as before: gray, with its text."""

    def test_colour_draws_the_page_in_bgr_as_pdfium_does(self, tmp_path):
        path = coloured(tmp_path)

        page = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, colour=True)

        assert page.channels == 3
        assert (page.px_width, page.px_height, page.pixels) == pdfium_bgr(path, 72)
        # Blue, green, red: the red box is (0, 0, 255), the margin white.
        assert (bgr_at(page, 45, 45), bgr_at(page, 5, 5)) == ((0, 0, 255), (255, 255, 255))

    def test_bgr_rows_are_unpadded(self, tmp_path):
        # 401 px across is 1203 bytes a row: not a multiple of 4, which pdfium's own stride would pad to.
        box = Rect(100.0, 100.0, 50.0, 50.0, fill_rgb=RED, stroke=False)
        path = build_pdf(tmp_path / "odd.pdf", [PageSpec(width=401.0, height=301.0, items=(box,))])

        page = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, colour=True)

        assert (page.px_width, page.px_height, len(page.pixels)) == (401, 301, 401 * 301 * 3)
        assert bgr_at(page, 120, 120) == (0, 0, 255)
        assert {bgr_at(page, x, y) for x, y in ((99, 120), (151, 120), (120, 99), (120, 151))} == {(255, 255, 255)}

    def test_without_text_a_text_only_page_draws_blank(self, tmp_path):
        path = text_only(tmp_path)

        with_text = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)
        page = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, without_text=True)

        assert min(with_text.pixels) < 128  # the text is drawn by default
        assert (page.channels, set(page.pixels)) == (1, {255})

    def test_without_text_keeps_everything_but_text(self, tmp_path):
        path = coloured(tmp_path)

        page = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, colour=True, without_text=True)

        assert colours_in(page, (41, 41, 239, 139)) == {(0, 0, 255)}  # the box, its red text gone
        assert colours_in(page, (0, 150, 400, 300)) == {(255, 255, 255)}  # the black text and the form's blue gone

    def test_text_is_removed_from_the_workers_load_only(self, tmp_path):
        path = coloured(tmp_path)
        before = path.read_bytes()

        render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, without_text=True)
        again = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, colour=True)

        assert path.read_bytes() == before
        assert (0, 0, 0) in colours_in(again, (50, 180, 200, 205))  # the black text is drawn again

    def test_the_default_render_is_one_gray_page_of_the_single_page_worker(self, tmp_path, started):
        path = coloured(tmp_path)

        page = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert page.channels == 1 and len(page.pixels) == page.px_width * page.px_height
        assert min(gray_at(page, x, y) for x in range(50, 150) for y in range(182, 200)) < 128  # its text is drawn
        (worker,) = started
        assert worker.returncode == 0

    @pytest.mark.parametrize("mode", ["colour", "without_text"])
    def test_a_mode_must_be_a_bool(self, tmp_path, started, mode):
        with pytest.raises(TypeError, match=re.escape(f"{mode} must be a bool, got 1 (int)")):
            render_page(tmp_path / "x.pdf", 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, **{mode: 1})
        assert started == []

    def test_a_refused_page_in_a_mode_raises_render_refused(self, tmp_path):
        path = refused_middle(tmp_path)

        with pytest.raises(RenderRefused, match=re.escape(f"{path}: page 2: {JPX_BOMB_REFUSAL.format(index=0)}")):
            render_page(path, 1, dpi=72, memory_cap_bytes=64 << 20, timeout_s=TIMEOUT_S, colour=True)


def stream(content: bytes) -> bytes:
    return b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content)


def refused_middle(tmp_path: Path) -> Path:
    """Three 400 x 300 pt pages; the second draws a JPEG 2000 bomb the image guard refuses, the others a blue box."""
    box = stream(b"0 0 1 rg 40 160 200 100 re f")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R 4 0 R 5 0 R] /Count 3 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 6 0 R >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 7 0 R "
        b"/Resources << /XObject << /Im 8 0 R >> >> >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 6 0 R >>",
        box,
        stream(b"q 80 0 0 40 10 20 cm /Im Do Q"),
        image_xobject(JPX_BOMB, JPX_16),
    ]
    return hand_written_pdf(tmp_path / "middle.pdf", objects)


def batch(path: Path, pages, **options):
    """render_pages of ``pages`` with a generous cap and timeout, in BGR without text unless ``options`` say."""
    arguments = {"memory_cap_bytes": CAP, "timeout_s": TIMEOUT_S, "colour": True, "without_text": True} | options
    return render_pages(path, pages, **arguments)


class TestBatch:
    """render_pages draws several pages of one document in one capped worker; each page's outcome is its own."""

    def test_pages_render_in_order_in_one_worker(self, tmp_path, started):
        path = build_pdf(tmp_path / "three.pdf", [PageSpec(width=400.0, height=300.0)] * 2 + [PageSpec()])

        first, second, third = batch(path, ((2, 72), (0, 36), (1, 72)))

        assert [(page.px_width, page.px_height, page.channels) for page in (first, second, third)] == [
            (612, 792, 3),
            (200, 150, 3),
            (400, 300, 3),
        ]
        assert set(first.pixels) == set(second.pixels) == {255}
        (worker,) = started
        assert (worker.returncode, worker.stdin.closed, worker.stdout.closed) == (0, True, True)

    def test_each_page_is_drawn_as_render_page_draws_it(self, tmp_path):
        path = coloured(tmp_path)

        (page,) = batch(path, ((0, 72),), without_text=False)

        assert (page.px_width, page.px_height, page.pixels) == pdfium_bgr(path, 72)

    def test_a_gray_batch_with_its_text_is_render_pages_default(self, tmp_path):
        path = coloured(tmp_path)

        (page,) = render_pages(path, ((0, 72),), memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert page == render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

    def test_a_refused_page_does_not_end_the_batch(self, tmp_path):
        path = refused_middle(tmp_path)

        first, second, third = batch(path, ((0, 72), (1, 72), (2, 72)), memory_cap_bytes=64 << 20)

        assert isinstance(second, RenderRefused)
        assert str(second) == f"{path}: page 2: {JPX_BOMB_REFUSAL.format(index=0)}; the page is not rendered"
        assert first == third
        assert colours_in(first, (41, 41, 239, 139)) == {(255, 0, 0)}  # the blue box, drawn on pages 1 and 3

    def test_a_failed_page_does_not_end_the_batch(self, tmp_path):
        path = coloured(tmp_path)

        first, second, third = batch(path, ((0, 72), (3, 72), (0, 36)))

        assert isinstance(second, RenderFailed)
        assert str(second).startswith(f"{path}: page 4: the render worker failed: IndexError: index must")
        assert (first.px_width, third.px_width) == (400, 200)

    def test_a_killed_worker_fails_the_rest_of_its_batch(self, tmp_path):
        # 10,000,000 DPI is 8 PiB of pixels: the bitmap cannot be mapped, so the worker ends at its cap.
        path = coloured(tmp_path)

        first, second, third = batch(path, ((0, 72), (0, 10_000_000), (0, 72)), memory_cap_bytes=64 << 20)

        assert isinstance(first, RenderedPage)
        ending = "the render worker reached its 67108864-byte memory cap and was ended"
        assert [str(page) for page in (second, third)] == [f"{path}: page 1: {ending}"] * 2
        assert all(isinstance(page, RenderFailed) for page in (second, third))

    def test_a_document_that_cannot_be_opened_fails_every_page(self, tmp_path):
        missing = tmp_path / "missing.pdf"

        results = batch(missing, ((0, 72), (1, 72)))

        assert [type(result) for result in results] == [RenderFailed, RenderFailed]
        for number, result in enumerate(results, 1):
            assert str(result).startswith(f"{missing}: page {number}: the render worker failed: FileNotFoundError: ")

    @pytest.mark.parametrize(
        "attribute,value,why",
        [
            pytest.param("frozen", True, "this application is frozen (sys.frozen)", id="frozen"),
            pytest.param("executable", "", "sys.executable is empty", id="no-executable"),
        ],
    )
    def test_no_interpreter_fails_every_page_before_anything_starts(
        self, tmp_path, monkeypatch, started, attribute, value, why
    ):
        path = coloured(tmp_path)
        monkeypatch.setattr(sys, attribute, value, raising=False)

        results = batch(path, ((0, 72), (0, 36)))

        assert [type(result) for result in results] == [RenderFailed, RenderFailed]
        assert all(str(result).startswith(f"{path}: page 1: ") and why in str(result) for result in results)
        assert started == []

    def test_a_worker_that_cannot_start_fails_every_page(self, tmp_path, monkeypatch):
        def refused(*args, **kwargs):
            raise OSError(8, "Exec format error")

        monkeypatch.setattr(render, "start_worker", refused)

        results = batch(coloured(tmp_path), ((0, 72),))

        assert "cannot start a render worker: [Errno 8] Exec format error" in str(results[0])

    def test_a_worker_whose_stderr_cannot_be_drained_fails_every_page(self, tmp_path, monkeypatch):
        def refused(command):
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(render, "start_worker", refused)

        results = batch(coloured(tmp_path), ((0, 72), (0, 36)))

        why = "cannot start a render worker: can't start new thread"
        assert [str(result).endswith(why) for result in results] == [True, True]

    def test_a_request_over_its_bound_fails_every_page_before_a_worker_starts(self, tmp_path, monkeypatch, started):
        monkeypatch.setattr(render, "_MAX_REQUEST_BYTES", 10)

        (result,) = batch(coloured(tmp_path), ((0, 72),))

        assert re.search(r"page 1: the render request is \d+ bytes, over the 10-byte bound$", str(result))
        assert started == []

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_one_timeout_covers_the_whole_batch(self, tmp_path):
        # Each page takes seconds at 600 DPI (a 400,000-segment path); the batch's one deadline ends it.
        path = slow_page(tmp_path)

        began = time.monotonic()
        results = batch(path, ((0, 600), (0, 600), (0, 600)), timeout_s=1.0)
        elapsed = time.monotonic() - began

        assert elapsed < 4.0
        assert [str(result) for result in results] == [
            f"{path}: page 1: the render timed out after 1.0 s; its worker was killed"
        ] * 3

    @pytest.mark.parametrize(
        "pages,error,message",
        [
            pytest.param((), ValueError, "pages must hold 1 to 64 (page index, dpi) pairs, got 0", id="empty"),
            pytest.param([(0, 72)], TypeError, "pages must be a tuple, got list", id="list"),
            pytest.param(((0,),), ValueError, "pages[0] must be a (page index, dpi) pair, got (0,)", id="single"),
            pytest.param(((0, 72), [1, 72]), ValueError, "pages[1] must be a (page index, dpi) pair", id="pair-list"),
            pytest.param(((-1, 72),), ValueError, "pages[0] page index must be >= 0, got -1", id="index"),
            pytest.param(((0, 0),), ValueError, "pages[0] dpi must be >= 1, got 0", id="dpi"),
            pytest.param(((0, 72.0),), TypeError, "pages[0] dpi must be an int", id="dpi-float"),
            pytest.param(
                ((0, 72),) * 65, ValueError, "pages must hold 1 to 64 (page index, dpi) pairs, got 65", id="many"
            ),
        ],
    )
    def test_invalid_pages_raise_before_a_worker_starts(self, tmp_path, started, pages, error, message):
        with pytest.raises(error, match=re.escape(message)):
            batch(tmp_path / "x.pdf", pages)
        assert started == []

    @pytest.mark.parametrize(
        "change,error,message",
        [
            pytest.param({"memory_cap_bytes": 0}, ValueError, "memory_cap_bytes must be >= 1, got 0", id="cap"),
            pytest.param({"timeout_s": 0.0}, ValueError, "timeout_s must be > 0", id="timeout"),
            pytest.param({"password": 3}, TypeError, "password must be a str or None, got int", id="password"),
            pytest.param({"colour": None}, TypeError, "colour must be a bool, got None", id="colour"),
            pytest.param({"without_text": "yes"}, TypeError, "without_text must be a bool", id="without-text"),
        ],
    )
    def test_invalid_options_raise_before_a_worker_starts(self, tmp_path, started, change, error, message):
        with pytest.raises(error, match=re.escape(message)):
            batch(tmp_path / "x.pdf", ((0, 72),), **change)
        assert started == []


class TestBatchWorker:
    """The worker's side of a batch: one capped worker, one reply (or pair of replies) per page, in order."""

    def test_the_request_is_json_on_stdin_with_its_pages_and_modes(self):
        request = render._BatchRequest(
            source="/x.pdf",
            pages=((2, 72), (0, 36)),
            password="pw-6b0d",
            memory_cap_bytes=9,
            timeout_s=1.5,
            colour=True,
            without_text=True,
        )

        payload = json.loads(render._batch_payload(request))

        assert "pw-6b0d" not in repr(request)
        assert payload == {
            "sys_path": sys.path,
            "worker": "render",
            "request": {
                "source": "/x.pdf",
                "pages": [[2, 72], [0, 36]],
                "password": "pw-6b0d",
                "memory_cap_bytes": 9,
                "timeout_s": 1.5,
                "colour": True,
                "without_text": True,
            },
        }

    def test_the_cap_is_applied_once_for_the_whole_batch(self, tmp_path, monkeypatch):
        caps = []
        monkeypatch.setattr(_render_worker, "apply_memory_cap", lambda cap, timeout: caps.append((cap, timeout)))
        path = coloured(tmp_path)
        message = json.loads(render._batch_payload(batch_request(path, ((0, 72), (0, 36)))))["request"]
        replies = Sink()

        _render_worker._worker_main(message, replies)

        first, first_pixels, second, second_pixels = framed(replies.data)
        assert caps == [(CAP, TIMEOUT_S)]
        assert (first, len(first_pixels)) == (render._PIXELS + render._PIXEL_SIZE.pack(400, 300), 400 * 300 * 3)
        assert (second, len(second_pixels)) == (render._PIXELS + render._PIXEL_SIZE.pack(200, 150), 200 * 150 * 3)

    def test_a_refused_or_failed_page_is_its_reply_and_the_batch_goes_on(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_render_worker, "apply_memory_cap", lambda *args: None)
        path = refused_middle(tmp_path)
        message = json.loads(render._batch_payload(batch_request(path, ((1, 72), (5, 72), (0, 72)))))["request"]
        replies = Sink()

        _render_worker._worker_main(message, replies)

        refused, failed, size, pixels = framed(replies.data)
        assert (
            refused
            == render._REFUSED + f"page 2: {JPX_BOMB_REFUSAL.format(index=0)}; the page is not rendered".encode()
        )
        assert failed.startswith(render._FAILED + b"IndexError: index must")
        assert (size, len(pixels)) == (render._PIXELS + render._PIXEL_SIZE.pack(400, 300), 400 * 300 * 3)

    def test_a_pdf_it_cannot_open_fails_each_page_with_the_reason(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_render_worker, "apply_memory_cap", lambda *args: None)
        message = json.loads(render._batch_payload(batch_request(tmp_path / "missing.pdf", ((0, 72), (1, 72)))))
        replies = Sink()

        _render_worker._worker_main(message["request"], replies)

        first, second = framed(replies.data)
        assert first == second and first.startswith(render._FAILED + b"FileNotFoundError: ")

    @pytest.mark.parametrize(
        "message,expected",
        [
            pytest.param({"pages": "all"}, "ValueError: the batch request must hold exactly", id="keys"),
            pytest.param(
                {**dict.fromkeys(render._BATCH_FIELDS, 1), "source": "/x.pdf", "password": None, "pages": [[0]]},
                "ValueError: pages[0] must be a (page index, dpi) pair, got [0]",
                id="pair",
            ),
        ],
    )
    def test_a_batch_it_cannot_read_is_answered_with_a_failure(self, monkeypatch, message, expected):
        monkeypatch.setattr(
            _render_worker, "apply_memory_cap", lambda *args: pytest.fail("the cap reached the test process")
        )
        replies = Sink()

        _render_worker._worker_main(message, replies)

        (reply,) = framed(replies.data)
        assert reply.startswith(render._FAILED + expected.encode())


def batch_request(path: Path, pages, **options) -> "render._BatchRequest":
    fields = {"source": str(path), "pages": pages, "password": None, "memory_cap_bytes": CAP, "timeout_s": TIMEOUT_S}
    return render._BatchRequest(**(fields | {"colour": True, "without_text": True} | options))


SIZE_3_BY_2 = render._PIXEL_SIZE.pack(3, 2)


def exchanged_batch(process: FakeProcess, count: int, *, cap: int = 1 << 20, channels: int = 3):
    """_exchange_batch with ``process`` (its replies already written), bounded at 10 s."""
    return bounded(lambda: render._exchange_batch(process.stdin, process.stdout, b"{}", cap, channels, count))


class TestBatchReplies:
    """The parent reads each page's reply in order, each within its bound; one it cannot trust ends the reading."""

    def test_each_page_in_order(self):
        process = FakeProcess().send(
            frame(render._PIXELS + SIZE_3_BY_2, bytes(18), render._REFUSED + b"page 2: why", render._FAILED + b"no")
        )

        first, second, third = exchanged_batch(process, 3)

        assert first.page == RenderedPage(pixels=bytes(18), px_width=3, px_height=2, channels=3)
        assert [(reply.kind, reply.detail) for reply in (second, third)] == [
            (render._REFUSED, "page 2: why"),
            (render._FAILED, "no"),
        ]

    def test_replies_that_end_early_leave_the_rest_unanswered(self):
        process = FakeProcess().send(frame(render._FAILED + b"no")).die(1)

        assert [reply.detail for reply in exchanged_batch(process, 3)] == ["no"]

    def test_a_bgr_claim_is_bounded_by_its_three_bytes_a_pixel(self):
        # 3 x 2 x 3 = 18 bytes, over a 17-byte cap: refused before its pixels are read, and the reading ends.
        process = FakeProcess().send(frame(render._PIXELS + SIZE_3_BY_2, bytes(18), render._FAILED + b"no"))

        (reply,) = exchanged_batch(process, 2, cap=17)

        assert (reply.kind, reply.detail) == (
            render._FAILED,
            "it claimed a 3 x 2 px BGR render, over its 17-byte memory cap",
        )

    def test_short_pixels_end_the_reading(self):
        process = FakeProcess().send(frame(render._PIXELS + SIZE_3_BY_2, bytes(6), render._FAILED + b"no"))

        (reply,) = exchanged_batch(process, 2)

        assert reply.detail == "it sent 6 pixel bytes for a 3 x 2 px BGR render"

    def test_the_exchange_needs_both_pipes(self):
        with pytest.raises(ValueError, match="the worker's stdin and stdout must be pipes"):
            render._exchange_batch(None, None, b"{}", 1, 3, 1)

    def test_a_message_over_its_bound_is_that_pages_failure_and_ends_the_reading(self, monkeypatch):
        monkeypatch.setattr(render, "_MAX_STATUS_BYTES", 8)
        process = FakeProcess().send(frame(render._FAILED + b"ok", render._FAILED + bytes(8), render._FAILED + b"no"))

        first, second = exchanged_batch(process, 3)

        assert (first.detail, second.detail) == ("ok", "a 9-byte message, over the 8-byte bound")

    def test_a_worker_that_stops_reading_leaves_every_page_unanswered(self):
        process = FakeProcess()

        def broken(data):
            raise BrokenPipeError(32, "Broken pipe")

        process.stdin.write = broken

        assert exchanged_batch(process, 2) == []


class TestBatchOutcomes:
    """Each page's outcome: its own reply, or, past the replies, why the worker ended."""

    REQUEST = render._BatchRequest(
        source="/x.pdf",
        pages=((0, 72), (4, 72), (2, 72)),
        password=None,
        memory_cap_bytes=99,
        timeout_s=2.5,
        colour=True,
        without_text=True,
    )

    def test_each_reply_then_the_ending(self):
        page = RenderedPage(pixels=bytes(18), px_width=3, px_height=2, channels=3)
        replies = [render._Reply(render._PIXELS, "", page), render._Reply(render._REFUSED, "page 5: why", None)]

        first, second, third = render._batch_outcomes(replies, True, -9, self.REQUEST, "")

        assert first is page
        assert (type(second), str(second)) == (RenderRefused, "/x.pdf: page 5: why")
        assert (type(third), str(third)) == (
            RenderFailed,
            "/x.pdf: page 3: the render timed out after 2.5 s; its worker was killed",
        )

    def test_a_failure_names_its_page_and_an_ending_ends_with_the_stderr_tail(self):
        replies = [render._Reply(render._FAILED, "IndexError: no page", None)]

        first, second, _third = render._batch_outcomes(replies, False, 1, self.REQUEST, "Traceback: boom")

        assert str(first) == "/x.pdf: page 1: the render worker failed: IndexError: no page"
        expected = (
            "/x.pdf: page 5: the render worker exited with code 1 without a reply; its stderr ends: Traceback: boom"
        )
        assert str(second) == expected


class TestTextRemoval:
    """The worker removes every text object of its load, forms' included, before it draws: in memory only."""

    def test_page_and_form_text_objects_are_removed(self, tmp_path):
        path = text_only(tmp_path)
        document = pdfium.PdfDocument(str(path))
        page = document[0]
        before = len(list(page.get_objects(max_depth=2)))

        removed = _render_worker._remove_text(page, 1)

        types = [pdfium_c.FPDFPageObj_GetType(item.raw) for item in page.get_objects(max_depth=2)]
        assert (before, removed, types) == (3, 2, [pdfium_c.FPDF_PAGEOBJ_FORM])

    def test_a_removal_pdfium_refuses_fails_naming_the_object(self, tmp_path, monkeypatch):
        page = pdfium.PdfDocument(str(text_only(tmp_path)))[0]
        monkeypatch.setattr(pdfium_c, "FPDFPage_RemoveObject", lambda *args: 0)

        with pytest.raises(RenderFailed, match=re.escape("page 1: pdfium cannot remove text object 0")):
            _render_worker._remove_text(page, 1)


def test_the_process_allowlist_is_unchanged():
    # The batch is the render worker's: it needs no worker key of its own.
    assert dict(_process.WORKERS) == {
        "figures": ("datagrunt.pdf._figure_worker", "_worker_main"),
        "pages": ("datagrunt.pdf._page_worker", "_worker_main"),
        "render": ("datagrunt.pdf._render_worker", "_worker_main"),
    }
    assert os.path.basename(_render_worker.__file__) == "_render_worker.py"


def renders_at_once(path: Path, monkeypatch, count: int) -> None:
    """``count`` pooled render_page calls at once, each holding its worker until every one of them has one.

    The calls' exchanges wait for each other, so ``count`` workers are out of the pool together. The barrier is
    removed before this returns: the test's later renders run alone.
    """
    barrier = threading.Barrier(count, timeout=60)
    real = render._exchange_pool

    def held(*args, **kwargs):
        barrier.wait()
        return real(*args, **kwargs)

    monkeypatch.setattr(render, "_exchange_pool", held)
    try:
        with ThreadPoolExecutor(max_workers=count) as threads:
            calls = [
                threads.submit(
                    contextvars.copy_context().run,
                    render_page,
                    path,
                    0,
                    dpi=36,
                    memory_cap_bytes=CAP,
                    timeout_s=TIMEOUT_S,
                )
                for _ in range(count)
            ]
            assert [call.result().px_width for call in calls] == [200] * count
    finally:
        monkeypatch.setattr(render, "_exchange_pool", real)


def idle_spares(pool: "render.RenderWorkerPool", count: int, wait_s: float = 10.0) -> list[subprocess.Popen]:
    """``pool``'s idle spares, oldest first, once ``count`` are there (within ``wait_s``)."""
    give_up = time.monotonic() + wait_s
    while time.monotonic() < give_up:  # bounded: by give_up
        idle = [child for _cap, child, _stderr in list(pool._idle)]
        if len(idle) >= count:
            return idle
        time.sleep(0.01)
    pytest.fail(f"the pool holds {len(pool._idle)} idle spares after {wait_s} s, not {count}")


def idle_spare(pool: "render.RenderWorkerPool") -> subprocess.Popen:
    """The first of ``pool``'s idle spares, once one is there (within 10 s)."""
    return idle_spares(pool, 1)[0]


def open_files(pid: int) -> set[str]:
    """The paths process ``pid`` holds open: from /proc on Linux, from lsof elsewhere."""
    if sys.platform == "linux":
        names = set()
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                names.add(os.readlink(f"/proc/{pid}/fd/{fd}"))
            except OSError:  # closed since it was listed
                continue
        return names
    listed = subprocess.run(["lsof", "-n", "-P", "-Fn", "-p", str(pid)], capture_output=True, text=True, timeout=30)
    return {line[1:] for line in listed.stdout.splitlines() if line.startswith("n")}


def spare_stdin(monkeypatch, *messages: bytes) -> None:
    """A spare's stdin in this process: ``messages``, each after its length, then its end."""
    stream = io.BytesIO(b"".join(_process.LENGTH.pack(len(message)) + message for message in messages))
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=stream))


def as_json(request) -> bytes:
    return json.dumps(asdict(request)).encode("ascii")


class TestRenderWorkerPool:
    """render_worker_pool keeps pre-started spares; each serves exactly one request, then exits."""

    def test_the_pool_is_scoped_and_refuses_once_closed(self):
        with render.render_worker_pool() as pool:
            assert render.active_render_worker_pool() is pool

        assert render.active_render_worker_pool() is None
        with pytest.raises(ValueError, match="RenderWorkerPool is closed"):
            pool.acquire("", CAP)

    def test_each_pooled_render_has_a_worker_of_its_own_that_exits_after_its_reply(
        self, tmp_path, started, monkeypatch
    ):
        served = []
        real = render._exchange_pool

        def recorded(stdin, *args, **kwargs):
            served.append(stdin)
            return real(stdin, *args, **kwargs)

        monkeypatch.setattr(render, "_exchange_pool", recorded)
        path = coloured(tmp_path)

        with render.render_worker_pool():
            first = render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)
            second = render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)
            (third,) = batch(path, ((0, 36),))
            workers = [next(worker for worker in list(started) if worker.stdin is stdin) for stdin in served]
            codes = [worker.returncode for worker in workers]

        assert (first.px_width, second.px_width, third.px_width) == (400, 200, 200)
        assert len({id(worker) for worker in workers}) == len(workers) == 3  # no worker serves twice
        assert codes == [0, 0, 0]  # each exited by itself after its reply, and was reaped then
        assert all(worker.poll() is not None for worker in started)  # the spares are reaped on close

    def test_a_pooled_render_past_its_cap_says_so(self, tmp_path, monkeypatch):
        path = coloured(tmp_path)
        expected = f"{path}: page 1: the render worker reached its {CAP}-byte memory cap and was ended"

        with render.render_worker_pool():
            renders_at_once(path, monkeypatch, 2)  # two spares, or two warm workers, are then idle
            with pytest.raises(RenderFailed, match=re.escape(expected)):
                render_page(path, 0, dpi=10_000_000, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

    def test_a_pooled_batch_past_its_cap_says_so(self, tmp_path, monkeypatch):
        path = coloured(tmp_path)
        expected = f"{path}: page 1: the render worker reached its {CAP}-byte memory cap and was ended"

        with render.render_worker_pool():
            renders_at_once(path, monkeypatch, 2)
            (outcome,) = batch(path, ((0, 10_000_000),))

        assert isinstance(outcome, RenderFailed)
        assert str(outcome).startswith(expected), outcome

    def test_a_spare_that_died_idle_is_replaced_by_a_new_worker(self, tmp_path, started, monkeypatch):
        path = coloured(tmp_path)

        with render.render_worker_pool() as pool:
            pool.warm(CAP)
            spare = idle_spare(pool)
            spare.kill()
            spare.wait(10)
            monkeypatch.setattr(spare, "poll", lambda: None)  # it dies after acquire finds it alive
            page = render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert (page.px_width, page.px_height) == (200, 150)
        assert spare.returncode == -signal.SIGKILL

    @pytest.mark.skipif(
        sys.platform != "linux" and shutil.which("lsof") is None, reason="no way to list a process's open files"
    )
    def test_no_idle_spare_holds_a_pdf_open(self, tmp_path, started):
        path = coloured(tmp_path)
        real = os.path.realpath(path)

        with render.render_worker_pool() as pool:
            render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)
            batch(path, ((0, 36),))
            held = open_files(idle_spare(pool).pid)
            path.unlink()  # the source can go while the pool holds spares

        assert [name for name in held if name == real or name.endswith(os.sep + path.name)] == []

    def test_a_spare_that_cannot_start_is_logged_and_the_render_fails_saying_why(self, tmp_path, monkeypatch, caplog):
        refusal = OSError(errno.EAGAIN, "no processes left")

        def refused(command):
            raise refusal

        monkeypatch.setattr(render, "start_worker", refused)
        path = coloured(tmp_path)
        expected = f"{path}: page 1: cannot start a render worker: {refusal}"

        with caplog.at_level(logging.WARNING, logger=render.__name__), render.render_worker_pool() as pool:
            pool.warm(CAP)
            with pytest.raises(RenderFailed, match=re.escape(expected)):
                render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        said = [record.getMessage() for record in caplog.records if record.name == render.__name__]
        assert said and all("a spare render worker could not start" in line and str(refusal) in line for line in said)

    def test_spares_are_bounded_per_memory_cap(self, tmp_path, started, monkeypatch):
        path = coloured(tmp_path)
        count = 8

        with render.render_worker_pool() as pool:
            for _ in range(10):
                pool.warm(CAP)
            renders_at_once(path, monkeypatch, count)
            alive = [worker for worker in list(started) if worker.poll() is None]

        assert len(alive) <= render._MAX_SPARES
        assert len(started) <= count + render._MAX_SPARES  # one a render, and the spares left at close

    def test_a_pooled_render_keeps_two_spares_ahead(self, tmp_path):
        path = coloured(tmp_path)

        with render.render_worker_pool() as pool:
            render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)
            spares = idle_spares(pool, 2, wait_s=5.0)

        assert len(spares) == render._SPARES_AHEAD == 2

    def test_the_oldest_idle_spare_is_handed_out_first(self, tmp_path):
        path = coloured(tmp_path)

        with render.render_worker_pool() as pool:
            render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)
            oldest = idle_spares(pool, 2)[0]
            worker, stderr = pool.acquire("", CAP)
            render._retired(worker, stderr)

        assert worker is oldest  # its imports are the likeliest done

    def test_the_exchange_ends_when_the_spares_stdout_does(self):
        spare_stdin, stdin = (os.fdopen(fd, mode) for fd, mode in zip(os.pipe(), ("rb", "wb"), strict=True))
        stdout, spare_stdout = (os.fdopen(fd, mode) for fd, mode in zip(os.pipe(), ("rb", "wb"), strict=True))
        spare_stdout.write(frame(render._FAILED + b"it failed"))
        spare_stdout.flush()
        read = functools.partial(render._one_reply, memory_cap_bytes=CAP)
        results = []
        exchange = threading.Thread(
            target=lambda: results.append(render._exchange_pool(stdin, stdout, b"{}", read)), daemon=True
        )

        exchange.start()
        exchange.join(0.3)
        waiting = list(results)  # its reply is in, but the spare has not exited: it would be reaped by polling
        spare_stdout.close()  # it exits
        exchange.join(10.0)

        stdin.close()
        sent = spare_stdin.read()
        for stream in (spare_stdin, stdout):
            stream.close()
        assert waiting == []
        ((replies, delivered),) = results
        assert (delivered, [(reply.kind, reply.detail) for reply in replies]) == (True, [(render._FAILED, "it failed")])
        assert framed(sent) == [b"{}"]

    def test_a_spare_exits_at_once_once_it_has_replied(self, tmp_path):
        # A stand-in render worker on the spare's sys.path: its reply is a line to stdout, left unflushed, and an
        # atexit hook that interpreter teardown would run. The spare flushes, then exits without that teardown.
        package = tmp_path / "datagrunt" / "pdf"
        package.mkdir(parents=True)
        (tmp_path / "datagrunt" / "__init__.py").write_text("")
        (package / "__init__.py").write_text("")
        (package / "_render_worker.py").write_text(
            "import atexit, sys\n"
            "def _pool_worker_main(replies):\n"
            "    atexit.register(lambda: sys.stderr.write(' torn down'))\n"
            "    sys.stdout.write('replied')\n"
        )

        done = subprocess.run(
            [sys.executable, "-I", "-c", render._RENDER_POOL_BOOTSTRAP],
            input=(json.dumps([str(tmp_path)]) + "\n").encode(),
            capture_output=True,
            timeout=60,
        )

        assert (done.returncode, done.stdout, done.stderr) == (0, b"", b"replied")

    def test_a_spare_that_ends_before_its_sys_path_is_reaped_and_the_render_fails_saying_so(
        self, tmp_path, started, monkeypatch
    ):
        monkeypatch.setattr(render, "_written", lambda stdin, data: False)  # as if it had ended: its pipe unread
        path = coloured(tmp_path)
        expected = f"{path}: page 1: cannot start a render worker: it ended with exit code -{signal.SIGKILL}"

        with render.render_worker_pool(), pytest.raises(RenderFailed, match=re.escape(expected)):
            render_page(path, 0, dpi=36, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert [worker.returncode for worker in started] == [-signal.SIGKILL]


class TestSpareWorker:
    """A spare (_pool_worker_main) serves its one request as a fresh worker serves it, then returns."""

    @pytest.fixture
    def capped(self, monkeypatch):
        """Each cap the spare sets (none reaches this process), and each move of its deadline."""
        calls = []

        class Lifetime:
            def extend(self, seconds: float) -> None:
                calls.append(("extend", seconds))

        def apply(cap: int, timeout_s: float) -> Lifetime:
            calls.append(("cap", cap, timeout_s))
            return Lifetime()

        monkeypatch.setattr(_render_worker, "apply_memory_cap", apply)
        return calls

    def test_a_page_request_is_capped_by_its_own_cap_and_deadline_and_served_alone(self, tmp_path, monkeypatch, capped):
        path = coloured(tmp_path)
        request = render._Request(
            source=str(path), page_index=0, dpi=36, password=None, memory_cap_bytes=CAP, timeout_s=7.5
        )
        spare_stdin(monkeypatch, as_json(request), as_json(request))
        replies = Sink()

        _render_worker._pool_worker_main(replies)

        assert capped == [("cap", CAP, 7.5)]  # its deadline is the request's: never extended
        messages = framed(replies.data)
        assert len(messages) == 2
        assert messages[0] == render._PIXELS + render._PIXEL_SIZE.pack(200, 150)
        assert len(messages[1]) == 200 * 150

    def test_a_batch_request_is_capped_by_its_own_cap_and_deadline_and_served_alone(
        self, tmp_path, monkeypatch, capped
    ):
        path = coloured(tmp_path)
        request = batch_request(path, ((0, 36), (0, 72)), timeout_s=9.0)
        spare_stdin(monkeypatch, as_json(request), as_json(request))
        replies = Sink()

        _render_worker._pool_worker_main(replies)

        assert capped == [("cap", CAP, 9.0)]
        messages = framed(replies.data)
        assert [messages[0], messages[2]] == [
            render._PIXELS + render._PIXEL_SIZE.pack(200, 150),
            render._PIXELS + render._PIXEL_SIZE.pack(400, 300),
        ]
        assert len(messages) == 4

    @pytest.mark.parametrize(
        "message,expected",
        [
            pytest.param(b"{not json", b"JSONDecodeError: ", id="not-json"),
            pytest.param(b'{"bad": "request"}', b"ValueError: the worker's request must hold exactly", id="page"),
            pytest.param(b'{"pages": []}', b"ValueError: the batch request must hold exactly", id="batch"),
        ],
    )
    def test_a_request_it_cannot_read_gets_its_failure_then_it_returns(
        self, tmp_path, monkeypatch, capped, message, expected
    ):
        request = render._Request(
            source=str(coloured(tmp_path)), page_index=0, dpi=36, password=None, memory_cap_bytes=CAP, timeout_s=7.5
        )
        spare_stdin(monkeypatch, message, as_json(request))
        replies = Sink()

        _render_worker._pool_worker_main(replies)

        (reply,) = framed(replies.data)
        assert reply.startswith(render._FAILED + expected)
        assert capped == []

    def test_its_stdin_ending_unserved_ends_it_without_a_reply(self, monkeypatch, capped):
        spare_stdin(monkeypatch)
        replies = Sink()

        _render_worker._pool_worker_main(replies)

        assert (replies.data, capped) == (b"", [])
