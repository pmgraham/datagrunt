"""Tests for the v5 capped render worker (#369, A5a): ``datagrunt.pdf.render.render_page``.

Each render runs in a worker process: a fresh ``python -I`` interpreter. Expected pixels come from
pdfium itself in this process (the same render call, size and flags), and
expected sizes from pdfium's page geometry: nothing is pinned from one
platform's fonts. Peak memory is read as ``test_pdfium_images.BOMB_SCRIPT``
reads it: VmHWM on Linux (``ru_maxrss`` survives fork + exec there),
``ru_maxrss`` in bytes on macOS.
"""

import ctypes
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import DatagruntPDFError, _process, render
from datagrunt.pdf.pdfium_engine import open_document, page_geometry
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page, render_size
from pdf_tests.builders import PageSpec, Rect, TextRun, build_pdf, hand_written_pdf
from pdf_tests.test_pdfium_images import JPX_16, xobject_page
from pdf_tests.test_render import JPX_BOMB, JPX_BOMB_REFUSAL

USER_PW = "user-pw-7f3a"
OWNER_PW = "owner-pw-91c2"
# Generous for a test page: a letter page at 300 DPI is 8.4 MB of pixels.
CAP = 1 << 30
TIMEOUT_S = 120.0


def letter(tmp_path: Path, *, name: str = "letter.pdf", rotate: int = 0, **encryption) -> Path:
    """A letter page with a black box at top-left (72, 72)-(272, 172) and a line of text."""
    items = (Rect(72.0, 72.0, 200.0, 100.0, fill_rgb=(0.0, 0.0, 0.0)), TextRun("Render me", x=72.0, y=300.0, size=24.0))
    return build_pdf(tmp_path / name, [PageSpec(width=612.0, height=792.0, items=items, rotate=rotate)], **encryption)


def pdfium_render(path: Path, dpi: int, *, flags: int = pdfium_c.FPDF_GRAYSCALE, password: str | None = None):
    """``(width, height, pixels)`` of page 1 drawn by pdfium here, at render_size and in 8-bit gray."""
    page = pdfium.PdfDocument(str(path), password=password)[0]
    width, height = render_size(*page.get_size(), dpi)
    buffer = (ctypes.c_ubyte * (width * height))()
    bitmap = pdfium_c.FPDFBitmap_CreateEx(width, height, pdfium_c.FPDFBitmap_Gray, buffer, width)
    try:
        assert pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, width, height, 0xFFFFFFFF)
        pdfium_c.FPDF_RenderPageBitmap(bitmap, page.raw, 0, 0, width, height, 0, flags)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    return width, height, bytes(buffer)


def rendered(path: Path, dpi: int, **options) -> RenderedPage:
    """``render_page`` of page 1 with a generous cap and timeout."""
    return render_page(path, 0, dpi=dpi, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S, **options)


def pixel(page: RenderedPage, x: int, y: int) -> int:
    return page.pixels[y * page.px_width + x]


@pytest.fixture
def started(monkeypatch):
    """Every subprocess started during the test, with the arguments it was started with."""
    workers = []

    class Recorded(subprocess.Popen):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            workers.append((self, args, kwargs))

    monkeypatch.setattr(subprocess, "Popen", Recorded)
    return workers


class TestRenderedPage:
    """RenderedPage is a validated, frozen record of 8-bit gray pixels and their size."""

    def test_valid(self):
        page = RenderedPage(pixels=bytes(6), px_width=3, px_height=2)

        assert (page.px_width, page.px_height, len(page.pixels)) == (3, 2, 6)
        assert "pixels" not in repr(page)

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"pixels": bytearray(6)}, TypeError, "pixels must be bytes, got bytearray", id="bytearray"),
            pytest.param({"pixels": bytes(5)}, ValueError, "pixels must hold 3 x 2 = 6 bytes, got 5", id="length"),
            pytest.param({"px_width": 0, "pixels": b""}, ValueError, "px_width must be >= 1, got 0", id="width"),
            pytest.param({"px_height": 2.0}, TypeError, "px_height must be an int", id="height"),
        ],
    )
    def test_invalid_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            RenderedPage(**{"pixels": bytes(6), "px_width": 3, "px_height": 2, **fields})


class TestArguments:
    """render_page validates its arguments before it starts a worker."""

    @pytest.mark.parametrize(
        "change,error,message",
        [
            pytest.param({"source": b"x.pdf"}, TypeError, "source must be a str or os.PathLike", id="source"),
            pytest.param({"page_index": -1}, ValueError, "page_index must be >= 0, got -1", id="index"),
            pytest.param({"dpi": 0}, ValueError, "dpi must be >= 1, got 0", id="dpi"),
            pytest.param({"dpi": 72.0}, TypeError, "dpi must be an int", id="dpi-float"),
            pytest.param({"password": 1234}, TypeError, "password must be a str or None, got int", id="password"),
            pytest.param({"password": "a\x00b"}, ValueError, "password must not contain NUL", id="password-nul"),
            pytest.param({"memory_cap_bytes": 0}, ValueError, "memory_cap_bytes must be >= 1, got 0", id="cap"),
            pytest.param({"timeout_s": 0.0}, ValueError, "timeout_s must be > 0", id="timeout"),
            pytest.param({"timeout_s": float("inf")}, ValueError, "timeout_s must be finite", id="timeout-inf"),
        ],
    )
    def test_invalid_arguments_raise_before_a_worker_starts(self, tmp_path, started, change, error, message):
        arguments = {"source": tmp_path / "x.pdf", "page_index": 0, "dpi": 72, "memory_cap_bytes": CAP}
        arguments |= {"timeout_s": 10.0, **change}
        source, index = arguments.pop("source"), arguments.pop("page_index")

        with pytest.raises(error, match=re.escape(message)):
            render_page(source, index, **arguments)
        assert started == []

    def test_render_failed_is_a_datagrunt_pdf_error(self):
        assert issubclass(RenderFailed, DatagruntPDFError)


class TestRender:
    """A page renders in a worker process, in 8-bit gray, as pdfium draws it, without annotations."""

    def test_a_page_renders_at_its_effective_dpi_as_pdfium_draws_it(self, tmp_path, started):
        path = letter(tmp_path)
        geometry = page_geometry(open_document(path), 0)
        dpi = effective_dpi(300, 16_000_000, geometry.width_pt, geometry.height_pt)

        page = rendered(path, dpi)

        assert dpi == 300
        assert (page.px_width, page.px_height) == render_size(geometry.width_pt, geometry.height_pt, dpi)
        assert (page.px_width, page.px_height, page.pixels) == pdfium_render(path, dpi)
        # The box (72, 72)-(272, 172) pt is black; the margin is white.
        assert (pixel(page, 600, 400), pixel(page, 100, 100)) == (0, 255)
        # It ran in one worker, which was reaped, its pipes closed.
        ((worker, _args, _kwargs),) = started
        assert (worker.returncode, worker.stdin.closed, worker.stdout.closed) == (0, True, True)

    def test_a_36_by_48_inch_sheet_renders_within_max_render_pixels(self, tmp_path):
        path = build_pdf(tmp_path / "sheet.pdf", [PageSpec(width=36 * 72.0, height=48 * 72.0)])
        geometry = page_geometry(open_document(path), 0)
        dpi = effective_dpi(300, 16_000_000, geometry.width_pt, geometry.height_pt)

        page = rendered(path, dpi)

        assert dpi == 96
        assert (page.px_width, page.px_height) == render_size(geometry.width_pt, geometry.height_pt, dpi)
        assert page.px_width * page.px_height <= 16_000_000

    def test_a_rotated_page_renders_as_displayed(self, tmp_path):
        path = letter(tmp_path, rotate=90)

        page = rendered(path, 72)

        # /Rotate 90 displays the letter page landscape.
        assert (page.px_width, page.px_height) == (792, 612)
        assert (page.px_width, page.px_height, page.pixels) == pdfium_render(path, 72)

    def test_annotations_are_not_drawn(self, tmp_path):
        # A square annotation whose appearance stream fills (100, 100)-(200, 200) black.
        appearance = b"0 g 0 0 100 100 re f"
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R /Annots [5 0 R] >>",
            b"<< /Length 0 >>\nstream\n\nendstream",
            b"<< /Type /Annot /Subtype /Square /Rect [100 100 200 200] /F 4 /AP << /N 6 0 R >> >>",
            b"<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Length %d >>\nstream\n%s\nendstream"
            % (len(appearance), appearance),
        ]
        path = hand_written_pdf(tmp_path / "annotated.pdf", objects)
        with_annotations = pdfium_render(path, 72, flags=pdfium_c.FPDF_GRAYSCALE | pdfium_c.FPDF_ANNOT)

        page = rendered(path, 72)

        # pdfium draws the annotation when asked (display y 150 is PDF y 150 on a 300 pt page).
        assert with_annotations[2][150 * 400 + 150] == 0
        assert set(page.pixels) == {255}

    def test_an_encrypted_page_renders_with_its_password(self, tmp_path):
        path = letter(tmp_path, user_password=USER_PW, owner_password=OWNER_PW)

        page = rendered(path, 100, password=USER_PW)

        assert (page.px_width, page.px_height, page.pixels) == pdfium_render(path, 100, password=USER_PW)
        assert pixel(page, 200, 150) == 0

    @pytest.mark.parametrize(
        "password,message",
        [
            pytest.param(None, "is encrypted; pass its password to open it", id="missing"),
            pytest.param("wrong-pw-5d1e", "is encrypted and the password is incorrect", id="wrong"),
        ],
    )
    def test_an_encrypted_page_without_its_password_fails_without_naming_one(self, tmp_path, password, message):
        path = letter(tmp_path, user_password=USER_PW, owner_password=OWNER_PW)

        with pytest.raises(RenderFailed, match=re.escape(message)) as caught:
            rendered(path, 72, password=password)
        assert not {USER_PW, OWNER_PW, "wrong-pw-5d1e"} & set(re.findall(r"[\w-]+", str(caught.value)))

    def test_the_password_reaches_the_worker_on_its_stdin_only(self, tmp_path, started):
        path = letter(tmp_path, user_password=USER_PW, owner_password=OWNER_PW)

        assert rendered(path, 72, password=USER_PW).px_width == 612
        ((_worker, args, kwargs),) = started
        assert not [arg for arg in args[0] if USER_PW in arg]
        assert kwargs.get("env") is None  # the caller's environment, which holds no password
        assert not [name for name, value in os.environ.items() if USER_PW in value or USER_PW in name]

    def test_a_missing_file_fails_in_the_worker(self, tmp_path):
        with pytest.raises(
            RenderFailed, match=r"page 1: the render worker failed: FileNotFoundError: .*not an existing"
        ):
            rendered(tmp_path / "missing.pdf", 72)

    def test_a_page_past_the_last_fails_in_the_worker(self, tmp_path):
        with pytest.raises(RenderFailed, match=re.escape("page 2: the render worker failed: IndexError: index must")):
            render_page(letter(tmp_path), 1, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

    def test_a_page_under_a_pixel_across_is_refused(self, tmp_path):
        path = build_pdf(tmp_path / "sliver.pdf", [PageSpec(width=0.5, height=200.0)])

        message = "page 1 renders 0 x 208 px at 75 DPI; the page is not rendered"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            rendered(path, 75)


class TestDrawFailures:
    """A bitmap pdfium cannot make or fill, or no memory can map, ends the render loudly (in-process: no worker)."""

    def test_a_bitmap_pdfium_cannot_make_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pdfium_c, "FPDFBitmap_CreateEx", lambda *args: None)

        with pytest.raises(RenderFailed, match=re.escape("page 1: pdfium cannot make a 612 x 792 gray bitmap")):
            render._render_guarded(letter(tmp_path), 0, 72, None)

    def test_a_bitmap_pdfium_cannot_fill_fails_and_is_destroyed(self, tmp_path, monkeypatch):
        destroyed = []
        real_destroy = pdfium_c.FPDFBitmap_Destroy
        monkeypatch.setattr(pdfium_c, "FPDFBitmap_FillRect", lambda *args: 0)
        monkeypatch.setattr(pdfium_c, "FPDFBitmap_Destroy", lambda bitmap: destroyed.append(real_destroy(bitmap)))

        with pytest.raises(RenderFailed, match=re.escape("page 1: pdfium cannot fill a 612 x 792 bitmap")):
            render._render_guarded(letter(tmp_path), 0, 72, None)
        assert len(destroyed) == 1

    def test_a_bitmap_no_memory_can_map_is_a_memory_error(self, tmp_path, monkeypatch):
        def refused(*args):
            raise OSError(12, "Cannot allocate memory")

        monkeypatch.setattr(render.mmap, "mmap", refused)

        with pytest.raises(MemoryError, match=re.escape("cannot map 484704 bytes for the bitmap")):
            render._render_guarded(letter(tmp_path), 0, 72, None)

    def test_a_bitmap_no_memory_can_map_ends_the_worker_at_its_cap(self, tmp_path):
        # 10,000,000 DPI on a letter page is 85 M x 110 M px: 8 PiB, past any address space, so mmap fails.
        message = "page 1: the render worker reached its 67108864-byte memory cap and was ended"
        with pytest.raises(RenderFailed, match=re.escape(message)):
            render_page(letter(tmp_path), 0, dpi=10_000_000, memory_cap_bytes=64 << 20, timeout_s=TIMEOUT_S)

    def test_the_cap_counts_from_the_workers_start(self, tmp_path):
        # The worker's interpreter and imports (~120 MiB) are not charged to its cap.
        page = render_page(letter(tmp_path), 0, dpi=72, memory_cap_bytes=64 << 20, timeout_s=TIMEOUT_S)

        assert (page.px_width, page.px_height) == (612, 792)

    def test_a_relative_source_is_made_absolute(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        with pytest.raises(RenderFailed, match=re.escape(f"{tmp_path / 'missing.pdf'}: page 1: the render worker")):
            render_page("missing.pdf", 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

    @pytest.mark.parametrize(
        "message,expected",
        [
            pytest.param("not a request", "TypeError: the worker's request must be a dict, got str", id="str"),
            pytest.param({"source": "/x.pdf"}, "ValueError: the worker's request must hold exactly", id="keys"),
            pytest.param(
                {**dict.fromkeys(render._REQUEST_FIELDS, 1), "source": "/x.pdf", "password": None, "dpi": 0},
                "ValueError: dpi must be >= 1, got 0",
                id="dpi",
            ),
        ],
    )
    def test_the_worker_answers_a_request_it_cannot_read_with_a_failure(self, monkeypatch, message, expected):
        # A tripwire: a request read wrongly must never cap this (the test) process's memory.
        monkeypatch.setattr(render, "_apply_memory_cap", lambda *args: pytest.fail("the cap reached the test process"))
        replies = Sink()

        render._worker_main(message, replies)

        (reply,) = framed(replies.data)
        assert reply.startswith(render._FAILED + expected.encode())


class TestGuardBeforeRender:
    """A page the image guard refuses is never drawn (#369 decision: the guard runs before the render)."""

    def test_a_refusal_raises_render_refused_naming_the_object(self, tmp_path):
        path = xobject_page(tmp_path, JPX_BOMB, JPX_16)

        # A 64 MiB cap: decoding the 256 M pixel bomb would end the worker (RenderFailed), not refuse it.
        message = f"{path}: page 1: {JPX_BOMB_REFUSAL.format(index=0)}; the page is not rendered"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            render_page(path, 0, dpi=72, memory_cap_bytes=64 << 20, timeout_s=TIMEOUT_S)

    def test_a_refused_page_starts_no_bitmap_and_no_render(self, tmp_path, monkeypatch):
        calls = []
        names = ("FPDFBitmap_CreateEx", "FPDF_RenderPageBitmap", "FPDF_RenderPageBitmapWithMatrix", "FPDF_FFLDraw")
        for name in names:
            monkeypatch.setattr(pdfium_c, name, lambda *args, _name=name: calls.append(_name))
        path = xobject_page(tmp_path, JPX_BOMB, JPX_16)

        with pytest.raises(RenderRefused, match=re.escape(JPX_BOMB_REFUSAL.format(index=0))):
            render._render_guarded(path, 0, 72, None)
        assert calls == []

    def test_a_page_that_passes_is_drawn_from_the_guarded_load(self, tmp_path, monkeypatch):
        loads = []
        real_load = render.loaded_page

        def counted(*args):
            loads.append(args[1])
            return real_load(*args)

        monkeypatch.setattr(render, "loaded_page", counted)
        path = letter(tmp_path)

        bitmap = render._render_guarded(path, 0, 72, None)

        assert loads == [0]
        assert (bitmap.width, bitmap.height, bytes(bitmap.pixels)) == pdfium_render(path, 72)


def slow_page(tmp_path: Path) -> Path:
    """A letter page of one 400,000-segment path: seconds to draw at 600 DPI, one object to guard."""
    body = b"0.1 w 0 0 m " + b"".join(b"%d %d l " % ((i * 37) % 612, (i * 91) % 792) for i in range(400_000)) + b"S"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(body), body),
    ]
    return hand_written_pdf(tmp_path / "slow.pdf", objects)


class TestTimeout:
    """Every wait is bounded: past its timeout the worker is killed and reaped, and the parent continues."""

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_timeout_kills_and_reaps_the_worker(self, tmp_path, started):
        path = slow_page(tmp_path)

        began = time.monotonic()
        with pytest.raises(RenderFailed, match=re.escape("page 1: the render timed out after 1.0 s")):
            render_page(path, 0, dpi=600, memory_cap_bytes=CAP, timeout_s=1.0)
        elapsed = time.monotonic() - began

        # Drawing the page takes seconds; the worker was killed at its 1 s deadline instead.
        assert elapsed < 4.0
        ((worker, _args, _kwargs),) = started
        assert worker.returncode == (-signal.SIGKILL if sys.platform != "win32" else 1)
        if sys.platform != "win32":
            with pytest.raises(ProcessLookupError):
                os.kill(worker.pid, 0)  # reaped: not even a zombie holds the pid

    def test_the_parent_renders_the_next_page_after_a_timeout(self, tmp_path):
        with pytest.raises(RenderFailed, match="timed out"):
            render_page(letter(tmp_path), 0, dpi=72, memory_cap_bytes=CAP, timeout_s=0.01)

        assert rendered(letter(tmp_path, name="next.pdf"), 72).px_width == 612


RENDER_SCRIPT = r"""
import os, resource, sys, threading, time

LIMIT = 1536 << 20  # bytes of peak RSS; past it this process ends at once


def peak() -> int:
    # Linux keeps ru_maxrss across fork + exec, so a child of a large process
    # (pytest) reports the parent's peak; VmHWM is this process's own, reset at exec.
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("no VmHWM line in /proc/self/status")
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # macOS reports bytes


def watch() -> None:
    while True:
        if peak() > LIMIT:
            sys.stdout.write(f"killed: peak RSS over {LIMIT >> 20} MiB\n")
            sys.stdout.flush()
            os._exit(99)
        time.sleep(0.005)


threading.Thread(target=watch, daemon=True).start()
from datagrunt.pdf.render import RenderFailed, render_page

try:
    page = render_page(sys.argv[1], 0, dpi=int(sys.argv[2]), memory_cap_bytes=int(sys.argv[3]), timeout_s=120.0)
    print(f"rendered {page.px_width} x {page.px_height}")
except RenderFailed as exc:
    print(f"failed: {exc}")
print(f"peak {peak() >> 20} MiB")
"""


def render_in_a_fresh_process(path: Path, dpi: int, cap: int) -> subprocess.CompletedProcess:
    """RENDER_SCRIPT: render_page in a new interpreter that exits (99) once its own peak RSS passes 1.5 GiB."""
    return subprocess.run(
        [sys.executable, "-c", RENDER_SCRIPT, str(path), str(dpi), str(cap)],
        capture_output=True,
        check=False,
        timeout=180,
        text=True,
    )


class TestMemoryCap:
    """A render past its memory cap ends its worker (RenderFailed); the parent's memory stays its own."""

    def test_a_render_past_the_cap_fails_and_the_parent_stays_small(self, tmp_path):
        # 2400 DPI on a letter page is 20400 x 26400 = 538.6 M pixels: a 514 MiB bitmap against a 256 MiB cap.
        path = letter(tmp_path)

        done = render_in_a_fresh_process(path, 2400, 256 << 20)

        assert done.returncode == 0, done.stdout + done.stderr
        expected = f"failed: {path}: page 1: the render worker reached its 268435456-byte memory cap and was ended"
        assert expected in done.stdout, done.stdout
        # The bitmap never reaches the parent: its peak is the interpreter's, far under the 514 MiB bitmap.
        assert int(re.search(r"peak (\d+) MiB", done.stdout).group(1)) < 400, done.stdout

    def test_the_same_page_renders_under_a_cap_it_fits(self, tmp_path):
        done = render_in_a_fresh_process(letter(tmp_path), 300, 256 << 20)

        assert done.returncode == 0, done.stdout + done.stderr
        assert "rendered 2550 x 3300" in done.stdout

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX resource limits")
    @pytest.mark.parametrize(
        "mapped,cap,soft,hard,expected",
        [
            pytest.param(100, 50, None, None, 150, id="unlimited"),
            # Past macOS's RLIM_INFINITY (2**63 - 1), and Linux's reads -1: neither may be taken as a limit.
            pytest.param(1 << 63, 50, None, None, (1 << 63) + 50, id="unlimited-huge"),
            pytest.param(100, 50, 120, None, 120, id="soft-lower"),
            pytest.param(100, 50, None, 130, 130, id="hard-lower"),
            pytest.param(100, 50, 400, 500, 150, id="limits-higher"),
        ],
    )
    def test_the_address_space_limit(self, mapped, cap, soft, hard, expected):
        import resource

        unlimited = resource.RLIM_INFINITY
        got = render._address_space_limit(mapped, cap, unlimited if soft is None else soft, hard or unlimited)

        assert got == expected

    @pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="peak RSS is read on Linux and macOS")
    def test_the_peak_rss_reader_sees_an_allocation(self):
        done = subprocess.run(
            [
                sys.executable,
                "-c",
                "from datagrunt.pdf import render; before = render._peak_rss_bytes(); "
                "block = bytearray(64 << 20); print(render._peak_rss_bytes() - before)",
            ],
            capture_output=True,
            check=True,
            timeout=60,
            text=True,
        )

        assert 60 << 20 < int(done.stdout) < 80 << 20

    @pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="peak RSS is read on Linux and macOS")
    @pytest.mark.parametrize(
        "limit,deadline_s,code",
        [
            pytest.param("0", "30", render._CAP_EXIT, id="past-the-limit"),
            pytest.param("1 << 40", "0.2", render._DEADLINE_EXIT, id="past-the-deadline"),
        ],
    )
    def test_the_watchdog_ends_its_process(self, limit, deadline_s, code):
        script = (
            "import time; from datagrunt.pdf import render; "
            f"render._watch_peak_rss({limit}, time.monotonic() + {deadline_s}); time.sleep(20); print('outlived')"
        )
        done = subprocess.run([sys.executable, "-c", script], capture_output=True, check=False, timeout=60, text=True)

        assert (done.returncode, done.stdout) == (code, "")

    def test_an_unsupported_platform_is_not_rendered_uncapped(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "sunos5")

        with pytest.raises(OSError, match=re.escape("no memory cap is implemented for platform 'sunos5'")):
            render._apply_memory_cap(1 << 20, 10.0)

    @pytest.mark.skipif(ctypes.sizeof(ctypes.c_void_p) != 8, reason="the 64-bit Windows layouts")
    def test_the_job_object_structures_match_the_windows_x64_layouts(self):
        # JOBOBJECT_EXTENDED_LIMIT_INFORMATION and PROCESS_MEMORY_COUNTERS_EX on x64 (winnt.h, psapi.h).
        assert ctypes.sizeof(render._ExtendedLimits) == 144
        assert render._ExtendedLimits.ProcessMemoryLimit.offset == 112
        assert ctypes.sizeof(render._MemoryCounters) == 80
        assert render._MemoryCounters.PrivateUsage.offset == 72


HUNG = "the exchange outlived its 10 s bound"


def bounded(call, limit_s: float = 10.0):
    """``call()`` in a daemon thread, waited on for ``limit_s``: its result, or HUNG (a regression must not hang)."""
    results = []
    runner = threading.Thread(target=lambda: results.append(call()), daemon=True)
    runner.start()
    runner.join(limit_s)
    return results[0] if results else HUNG


def frame(*messages: bytes) -> bytes:
    """``messages`` as a worker writes them: each after its 8-byte little-endian length."""
    return b"".join(_process.LENGTH.pack(len(message)) + message for message in messages)


def framed(data: bytes) -> list[bytes]:
    """The messages in ``data`` (the inverse of frame)."""
    messages = []
    while data:
        (length,) = _process.LENGTH.unpack_from(data)
        messages.append(data[8 : 8 + length])
        data = data[8 + length :]
    return messages


class Sink:
    """A worker's stdin, or a worker's replies, in this process: what is written, and whether it was closed."""

    def __init__(self):
        self.data, self.closed, self.flushed = b"", False, b""

    def write(self, data) -> int:
        self.data += bytes(data)
        return len(data)

    def flush(self) -> None:
        self.flushed = self.data

    def close(self) -> None:
        self.closed = True


class FakeProcess:
    """A worker stand-in: ``stdin`` records the request; a kill closes ``stdout``'s pipe, as a dying worker's is."""

    def __init__(self):
        read_end, write_end = os.pipe()
        self.stdin = Sink()
        self.stdout = os.fdopen(read_end, "rb")
        self.writer = os.fdopen(write_end, "wb")
        self.returncode = None
        self.waits = []
        self.kills = 0

    def send(self, data: bytes) -> "FakeProcess":
        self.writer.write(data)
        self.writer.flush()
        return self

    def die(self, code: int) -> "FakeProcess":
        self.returncode = code
        self.writer.close()
        return self

    def kill(self):
        self.kills += 1
        self.returncode = -9
        self.writer.close()

    def wait(self, timeout=None):
        self.waits.append(timeout)
        if self.returncode is None:
            raise subprocess.TimeoutExpired("fake worker", timeout)
        return self.returncode


PAYLOAD = b'{"request": "the request"}'


def exchanged(process: FakeProcess, cap: int = 1 << 20):
    """_exchange with ``process`` (its replies already written), bounded at 10 s."""
    return bounded(lambda: render._exchange(process.stdin, process.stdout, PAYLOAD, cap))


SIZE_3_BY_2 = render._PIXEL_SIZE.pack(3, 2)


class TestReplies:
    """The parent reads a worker's length-prefixed raw-bytes reply, each message within its bound."""

    def test_pixels(self):
        process = FakeProcess().send(frame(render._PIXELS + SIZE_3_BY_2, bytes(range(6))))

        assert exchanged(process).page == RenderedPage(pixels=bytes(range(6)), px_width=3, px_height=2)
        assert (process.stdin.data, process.stdin.closed) == (PAYLOAD, True)

    @pytest.mark.parametrize("kind", [render._REFUSED, render._FAILED])
    def test_a_message(self, kind):
        got = exchanged(FakeProcess().send(frame(kind + "page 1: why \u00e9".encode())))

        assert (got.kind, got.detail, got.page) == (kind, "page 1: why \u00e9", None)

    @pytest.mark.parametrize(
        "messages,detail",
        [
            pytest.param((b"X why",), "it sent a reply of unknown kind b'X'", id="kind"),
            pytest.param((b"",), "it sent a reply of unknown kind b''", id="empty"),
            pytest.param((render._PIXELS + b"\x03",), "it sent a 1-byte pixel header, not 16", id="header"),
            pytest.param(
                (render._PIXELS + render._PIXEL_SIZE.pack(0, 2),), "it claimed a 0 x 2 px render", id="zero-width"
            ),
            pytest.param(
                (render._PIXELS + render._PIXEL_SIZE.pack(1025, 1024),),
                "it claimed a 1025 x 1024 px render, over its 1048576-byte memory cap",
                id="over-cap",
            ),
            pytest.param(
                (render._PIXELS + SIZE_3_BY_2, bytes(5)), "it sent 5 pixel bytes for a 3 x 2 px render", id="short"
            ),
        ],
    )
    def test_a_malformed_reply_is_a_failure(self, messages, detail):
        got = exchanged(FakeProcess().send(frame(*messages)))

        assert (got.kind, got.detail, got.page) == (render._FAILED, detail, None)

    def test_a_claim_of_exactly_the_cap_is_read(self):
        got = exchanged(FakeProcess().send(frame(render._PIXELS + SIZE_3_BY_2, bytes(6))), cap=6)

        assert got.page == RenderedPage(pixels=bytes(6), px_width=3, px_height=2)

    def test_pixels_longer_than_claimed_end_the_exchange_unread(self):
        # The length is read first: a message over its bound is refused before it is read.
        assert exchanged(FakeProcess().send(frame(render._PIXELS + SIZE_3_BY_2) + _process.LENGTH.pack(7))) is None

    def test_a_message_over_its_bound_ends_the_exchange(self, monkeypatch):
        monkeypatch.setattr(render, "_MAX_STATUS_BYTES", 8)

        assert exchanged(FakeProcess().send(frame(render._FAILED + bytes(8)))) is None

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(b"", id="nothing"),
            pytest.param(b"\x05\x00\x00", id="part-of-a-length"),
            pytest.param(_process.LENGTH.pack(10) + b"F why", id="part-of-a-message"),
        ],
    )
    def test_a_worker_that_ends_mid_reply_ends_the_exchange(self, data):
        assert exchanged(FakeProcess().send(data).die(1)) is None

    def test_a_worker_that_stops_reading_ends_the_exchange(self):
        process = FakeProcess()

        def broken(data):
            raise BrokenPipeError(32, "Broken pipe")

        process.stdin.write = broken

        assert exchanged(process) is None

    def test_the_request_never_shows_its_password(self):
        request = render._Request(
            source="/x.pdf", page_index=0, dpi=72, password="pw-6b0d", memory_cap_bytes=1, timeout_s=1.0
        )

        assert "pw-6b0d" not in repr(request) and "pw-6b0d" not in str(request)

    def test_the_request_payload_is_json_with_the_callers_sys_path(self):
        request = render._Request(
            source="/x.pdf", page_index=2, dpi=72, password="pw-6b0d", memory_cap_bytes=9, timeout_s=1.5
        )

        payload = json.loads(render._request_payload(request))

        assert payload == {
            "sys_path": sys.path,
            "worker": "render",
            "request": {
                "source": "/x.pdf",
                "page_index": 2,
                "dpi": 72,
                "password": "pw-6b0d",
                "memory_cap_bytes": 9,
                "timeout_s": 1.5,
            },
        }


def supervised(process: FakeProcess, deadline_s: float):
    """_supervised over ``process``, bounded at 10 s: (reply, timed out, exit code), or HUNG."""
    return bounded(lambda: render._supervised(process, PAYLOAD, 99, time.monotonic() + deadline_s))


class TestSupervision:
    """The parent's waits: a timer ends the worker at the deadline, and the worker is reaped once."""

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_no_reply_by_the_deadline_kills_the_worker_and_reaps_it_at_once(self):
        process = FakeProcess()

        began = time.monotonic()
        assert supervised(process, 0.2) == (None, True, -9)
        assert 0.15 < time.monotonic() - began < 2.0
        assert process.waits[0] == 0.0

    def test_a_worker_that_stalls_mid_reply_is_killed_at_the_deadline(self):
        process = FakeProcess().send(frame(render._PIXELS + SIZE_3_BY_2))  # and never its pixels

        assert supervised(process, 0.2) == (None, True, -9)

    def test_a_worker_that_dies_without_a_reply_is_given_time_to_exit(self):
        process = FakeProcess().die(render._CAP_EXIT)

        assert supervised(process, 5.0) == (None, False, render._CAP_EXIT)
        assert (process.waits, process.kills) == ([render._EXIT_GRACE_S], 0)

    def test_a_worker_that_replied_is_reaped_and_never_killed_late(self):
        process = FakeProcess().send(frame(render._FAILED + b"page 1: why"))

        reply, timed_out, _exitcode = supervised(process, 0.2)
        time.sleep(0.4)  # past the deadline: a timer left running would kill again

        assert (reply.kind, reply.detail, timed_out) == (render._FAILED, "page 1: why", False)
        assert process.waits[0] == _process.REAP_TIMEOUT_S
        assert process.kills == 1  # reaped()'s own: this stand-in never exits by itself

    def test_a_worker_that_will_not_exit_is_killed_then_reaped(self):
        process = FakeProcess()

        assert _process.reaped(process, 0.0) == -9
        assert (process.waits, process.kills) == ([0.0, _process.REAP_TIMEOUT_S], 1)

    def test_a_worker_that_outlives_its_kill_is_reported_unreaped(self):
        process = FakeProcess()
        process.kill = lambda: setattr(process, "kills", process.kills + 1)  # a kill that never lands

        assert _process.reaped(process, 0.0) is None
        assert (process.waits, process.kills) == ([0.0, _process.REAP_TIMEOUT_S], 1)


NO_GUARD_SCRIPT = """
import sys

from datagrunt.pdf.render import render_page

with open(sys.argv[2], "a") as marker:
    marker.write("top level ran\\n")
page = render_page(sys.argv[1], 0, dpi=72, memory_cap_bytes=1 << 30, timeout_s=120.0)
print(f"rendered {page.px_width} x {page.px_height}")
"""

POISON = """
with open({marker!r}, "a") as marker:
    marker.write("poisoned json imported\\n")
raise ImportError("a json module from the caller's cwd or PYTHONPATH reached the render worker")
"""


FAKE_RENDER = """
def _worker_main(request, replies):
    print("noise on stdout")
    replies.write(repr(request).encode())
    replies.flush()
"""


def run_bootstrap(stdin: bytes) -> subprocess.CompletedProcess:
    """_process.BOOTSTRAP in ``python -I``, fed ``stdin``."""
    return subprocess.run(
        [sys.executable, "-I", "-c", _process.BOOTSTRAP], input=stdin, capture_output=True, check=False, timeout=60
    )


class TestBootstrap:
    """The worker's bootstrap: a bounded JSON request in, the caller's sys.path installed, stdout kept for replies."""

    def test_it_installs_the_sent_sys_path_and_keeps_stdout_for_replies(self, tmp_path):
        package = tmp_path / "datagrunt" / "pdf"
        package.mkdir(parents=True)
        (tmp_path / "datagrunt" / "__init__.py").write_text('print("noise at import")\n', encoding="utf-8")
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "render.py").write_text(FAKE_RENDER, encoding="utf-8")

        request = {"sys_path": [str(tmp_path)], "worker": "render", "request": {"page_index": 0}}
        done = run_bootstrap(json.dumps(request).encode())

        assert (done.returncode, done.stdout) == (0, b"{'page_index': 0}"), done.stderr
        assert b"noise at import" in done.stderr and b"noise on stdout" in done.stderr

    def test_a_request_over_its_bound_is_refused_unread(self):
        done = run_bootstrap(b" " * (render._MAX_REQUEST_BYTES + 1))

        assert done.returncode == 1
        assert b"the worker request is over 1048576 bytes" in done.stderr

    @pytest.mark.parametrize("path", ["/a/path", ["/a/path", 3]], ids=["str", "not-all-str"])
    def test_a_sys_path_that_is_not_a_list_of_str_is_refused(self, path):
        done = run_bootstrap(json.dumps({"sys_path": path, "request": {}}).encode())

        assert done.returncode == 1
        assert b"the worker request's sys_path is not a list of str" in done.stderr

    def test_a_message_is_sent_whole_after_its_length_and_flushed(self):
        replies = Sink()

        render._send(replies, b"F why")

        assert replies.flushed == replies.data == _process.LENGTH.pack(5) + b"F why"

    def test_a_request_over_its_bound_is_refused_before_a_worker_starts(self, monkeypatch):
        request = render._Request(
            source="/x.pdf", page_index=0, dpi=72, password=None, memory_cap_bytes=9, timeout_s=1.0
        )
        monkeypatch.setattr(render, "_MAX_REQUEST_BYTES", 10)

        with pytest.raises(RenderFailed, match=r"the render request is \d+ bytes, over the 10-byte bound"):
            render._request_payload(request)

    def test_the_exchange_needs_both_pipes(self):
        with pytest.raises(ValueError, match="the worker's stdin and stdout must be pipes"):
            render._exchange(None, None, PAYLOAD, 1)


class TestWorkerProcess:
    """The worker is a plain ``python -I`` subprocess: it never imports the caller's main module."""

    def test_a_script_without_a_main_guard_renders_and_runs_its_top_level_once(self, tmp_path):
        script = tmp_path / "no_guard.py"
        script.write_text(NO_GUARD_SCRIPT, encoding="utf-8")
        marker = tmp_path / "marker.txt"

        done = subprocess.run(
            [sys.executable, str(script), str(letter(tmp_path)), str(marker)],
            capture_output=True,
            check=False,
            timeout=180,
            text=True,
        )

        assert (done.returncode, done.stdout) == (0, "rendered 612 x 792\n"), done.stderr
        assert marker.read_text(encoding="utf-8").splitlines() == ["top level ran"]

    def test_the_worker_is_python_isolated_running_the_bootstrap(self, tmp_path, started):
        rendered(letter(tmp_path), 72)

        ((_worker, args, kwargs),) = started
        assert args[0] == [sys.executable, "-I", "-c", _process.BOOTSTRAP]
        assert (kwargs["stdin"], kwargs["stdout"]) == (subprocess.PIPE, subprocess.PIPE)

    def test_the_worker_ignores_the_callers_cwd_and_python_environment(self, tmp_path, monkeypatch):
        poisoned = tmp_path / "poisoned"
        poisoned.mkdir()
        marker = tmp_path / "poison.txt"
        (poisoned / "json.py").write_text(POISON.format(marker=str(marker)), encoding="utf-8")
        path = letter(tmp_path)
        monkeypatch.chdir(poisoned)
        monkeypatch.setenv("PYTHONPATH", str(poisoned))

        assert rendered(path, 72).px_width == 612
        assert not marker.exists()

    def test_the_worker_imports_from_the_callers_sys_path(self, tmp_path, monkeypatch):
        path = letter(tmp_path)
        # The standard library only: the worker reads its sys.path from the caller, so datagrunt is not found.
        monkeypatch.setattr(sys, "path", [os.path.dirname(os.__file__)])

        with pytest.raises(RenderFailed) as caught:
            rendered(path, 72)
        message = str(caught.value)
        assert message.startswith(f"{path}: page 1: the render worker exited with code 1 without a reply; ")
        assert "its stderr ends:" in message and "No module named 'datagrunt'" in message

    @pytest.mark.parametrize(
        "attribute,value,why",
        [
            pytest.param("frozen", True, "a frozen application has no Python interpreter", id="frozen"),
            pytest.param("executable", "", "sys.executable is empty", id="no-executable"),
        ],
    )
    def test_no_interpreter_to_start_fails_before_anything_starts(
        self, tmp_path, monkeypatch, started, attribute, value, why
    ):
        monkeypatch.setattr(sys, attribute, value, raising=False)

        with pytest.raises(RenderFailed, match=re.escape(why)):
            rendered(letter(tmp_path), 72)
        assert started == []

    def test_a_worker_that_cannot_start_is_a_render_failure(self, tmp_path, monkeypatch):
        def refused(*args, **kwargs):
            raise OSError(8, "Exec format error")

        monkeypatch.setattr(subprocess, "Popen", refused)

        with pytest.raises(
            RenderFailed, match=re.escape("page 1: cannot start a render worker: [Errno 8] Exec format")
        ):
            rendered(letter(tmp_path), 72)

    @pytest.mark.parametrize(
        "written,tail",
        [
            pytest.param(b"", "", id="empty"),
            pytest.param(b"boom\n", "boom", id="short"),
            pytest.param(b"x" * 5000 + b"\xffend", "x" * 4092 + "\\xffend", id="long"),
        ],
    )
    def test_the_stderr_tail_is_its_last_4_kib(self, tmp_path, written, tail):
        with open(tmp_path / "stderr", "w+b") as errors:
            errors.write(written)

            assert _process.stderr_tail(errors) == tail

    def test_a_failure_without_a_reply_ends_with_the_workers_stderr(self):
        request = render._Request(
            source="/x.pdf", page_index=0, dpi=72, password=None, memory_cap_bytes=9, timeout_s=1.0
        )

        with pytest.raises(RenderFailed) as caught:
            render._outcome(None, False, 1, request, "Traceback: boom")
        message = (
            "/x.pdf: page 1: the render worker exited with code 1 without a reply; its stderr ends: Traceback: boom"
        )
        assert str(caught.value) == message


class TestEndings:
    """Why a worker ended without a reply, in words."""

    REQUEST = render._Request(source="/x.pdf", page_index=0, dpi=72, password=None, memory_cap_bytes=99, timeout_s=2.5)

    @pytest.mark.parametrize(
        "exitcode,timed_out,expected",
        [
            pytest.param(-9, True, "the render timed out after 2.5 s; its worker was killed", id="timeout"),
            pytest.param(
                render._CAP_EXIT, False, "the render worker reached its 99-byte memory cap and was ended", id="cap"
            ),
            pytest.param(-11, False, "the render worker was killed by signal 11", id="signal"),
            pytest.param(1, False, "the render worker exited with code 1 without a reply", id="exit"),
            pytest.param(0, False, "the render worker exited with code 0 without a reply", id="exit-0"),
            pytest.param(None, False, "the render worker ended without a reply and could not be reaped", id="unreaped"),
        ],
    )
    def test_each_ending(self, exitcode, timed_out, expected):
        assert render._ending(exitcode, timed_out, self.REQUEST) == expected


class TestRenderCallsStayInRender:
    """No v5 module but the guarded render module calls a pdfium render function (#369 plan)."""

    # FPDF_RenderPage*, FPDF*_GetRendered* (an object drawn to a bitmap), form-field drawing, and
    # pypdfium2's PdfPage.render(): called, not named in prose (a call has no space before its parenthesis).
    RENDER_CALL = re.compile(r"(?:FPDF_RenderPage\w*|FPDF\w*_GetRendered\w*|FPDF_FFLDraw|\brender)\(")

    def test_only_the_render_module_renders(self):
        sources = sorted(Path(render.__file__).parent.glob("**/*.py"))
        calls = {path.name: self.RENDER_CALL.findall(path.read_text(encoding="utf-8")) for path in sources}

        assert {name: found for name, found in calls.items() if found} == {"render.py": ["FPDF_RenderPageBitmap("]}

    @pytest.mark.parametrize(
        "line",
        [
            "page.render(scale=2)",
            "pdfium_c.FPDF_RenderPageBitmap(bitmap, page, 0, 0, 1, 1, 0, 0)",
            "pdfium_c.FPDF_RenderPageBitmap_Start(bitmap)",
            "pdfium_c.FPDFImageObj_GetRenderedBitmap(document, page, image)",
            "pdfium_c.FPDF_FFLDraw(form, bitmap, page)",
        ],
    )
    def test_the_pattern_finds_each_render_call(self, line):
        assert self.RENDER_CALL.search(line)
