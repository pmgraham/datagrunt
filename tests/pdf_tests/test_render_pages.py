"""Tests for the v5 render worker's analysis modes and its multi-page batch (#400).

``render_page(..., colour=True, without_text=True)`` draws a page in 24-bit
BGR with its text objects removed from the worker's load of it, and
``render_pages`` draws several pages of one document in one capped worker.
Expected pixels come from pdfium itself in this process (the same render
call, size and flags), never pinned from one platform's fonts.
"""

import ctypes
import json
import os
import re
import sys
import time
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import _process, render
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

    def recorded(command, errors):
        worker = real(command, errors)
        workers.append(worker)
        return worker

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
            pytest.param("frozen", True, "a frozen application has no Python interpreter", id="frozen"),
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

    def test_no_file_for_the_workers_stderr_fails_every_page(self, tmp_path, monkeypatch, started):
        def refused():
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(render.tempfile, "TemporaryFile", refused)

        results = batch(coloured(tmp_path), ((0, 72), (0, 36)))

        why = "cannot make a file for the render worker's stderr: [Errno 28] No space left on device"
        assert [str(result).endswith(why) for result in results] == [True, True]
        assert started == []

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
        monkeypatch.setattr(render, "_apply_memory_cap", lambda cap, timeout: caps.append((cap, timeout)))
        path = coloured(tmp_path)
        message = json.loads(render._batch_payload(batch_request(path, ((0, 72), (0, 36)))))["request"]
        replies = Sink()

        render._worker_main(message, replies)

        first, first_pixels, second, second_pixels = framed(replies.data)
        assert caps == [(CAP, TIMEOUT_S)]
        assert (first, len(first_pixels)) == (render._PIXELS + render._PIXEL_SIZE.pack(400, 300), 400 * 300 * 3)
        assert (second, len(second_pixels)) == (render._PIXELS + render._PIXEL_SIZE.pack(200, 150), 200 * 150 * 3)

    def test_a_refused_or_failed_page_is_its_reply_and_the_batch_goes_on(self, tmp_path, monkeypatch):
        monkeypatch.setattr(render, "_apply_memory_cap", lambda *args: None)
        path = refused_middle(tmp_path)
        message = json.loads(render._batch_payload(batch_request(path, ((1, 72), (5, 72), (0, 72)))))["request"]
        replies = Sink()

        render._worker_main(message, replies)

        refused, failed, size, pixels = framed(replies.data)
        assert (
            refused
            == render._REFUSED + f"page 2: {JPX_BOMB_REFUSAL.format(index=0)}; the page is not rendered".encode()
        )
        assert failed.startswith(render._FAILED + b"IndexError: index must")
        assert (size, len(pixels)) == (render._PIXELS + render._PIXEL_SIZE.pack(400, 300), 400 * 300 * 3)

    def test_a_pdf_it_cannot_open_fails_each_page_with_the_reason(self, tmp_path, monkeypatch):
        monkeypatch.setattr(render, "_apply_memory_cap", lambda *args: None)
        message = json.loads(render._batch_payload(batch_request(tmp_path / "missing.pdf", ((0, 72), (1, 72)))))
        replies = Sink()

        render._worker_main(message["request"], replies)

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
        monkeypatch.setattr(render, "_apply_memory_cap", lambda *args: pytest.fail("the cap reached the test process"))
        replies = Sink()

        render._worker_main(message, replies)

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

        removed = render._remove_text(page, 1)

        types = [pdfium_c.FPDFPageObj_GetType(item.raw) for item in page.get_objects(max_depth=2)]
        assert (before, removed, types) == (3, 2, [pdfium_c.FPDF_PAGEOBJ_FORM])

    def test_a_removal_pdfium_refuses_fails_naming_the_object(self, tmp_path, monkeypatch):
        page = pdfium.PdfDocument(str(text_only(tmp_path)))[0]
        monkeypatch.setattr(pdfium_c, "FPDFPage_RemoveObject", lambda *args: 0)

        with pytest.raises(RenderFailed, match=re.escape("page 1: pdfium cannot remove text object 0")):
            render._remove_text(page, 1)


def test_the_process_allowlist_is_unchanged():
    # The batch is the render worker's: it needs no worker key of its own.
    assert dict(_process.WORKERS) == {
        "render": ("datagrunt.pdf.render", "_worker_main"),
        "pages": ("datagrunt.pdf.pipeline", "_worker_main"),
    }
    assert os.path.basename(render.__file__) == "render.py"
