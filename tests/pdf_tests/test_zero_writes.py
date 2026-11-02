"""The v5 PDF package's child processes write nothing to disk (#372, A8b).

A worker's or tesseract's stderr is drained by a thread into a 4 KiB ring
(``datagrunt.pdf._process.StderrTail``), never a temporary file: each test
here refuses every ``tempfile`` call, and checks that each path still ends
its drain thread (success, failure, kill and timeout).
"""

import re
import shutil
import subprocess
import tempfile
import threading

import pytest

from datagrunt.pdf import DatagruntPDFError, PDFCorruptError, _process, ocr
from datagrunt.pdf._executables import trusted_executable
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.render import RenderFailed, render_page, render_pages
from pdf_tests.builders import PageSpec, TextRun, break_page, build_pdf
from pdf_tests.test_ocr import fake_tesseract, needs_tesseract, posix_only
from pdf_tests.test_pipeline import mixed_page, run_page_workers_as, started_workers
from pdf_tests.test_render_worker import slow_page

CAP = 1 << 30
TIMEOUT_S = 120.0
TEMPFILE_CALLS = ("TemporaryFile", "NamedTemporaryFile", "SpooledTemporaryFile", "mkstemp", "mkdtemp", "mktemp")


@pytest.fixture
def no_temporary_files(monkeypatch):
    """Every ``tempfile`` call fails the test: nothing here may make a temporary file."""

    def refused(*args, **kwargs):
        raise AssertionError("a temporary file was made")

    for name in TEMPFILE_CALLS:
        monkeypatch.setattr(tempfile, name, refused)
    yield
    assert stderr_threads() == []


def stderr_threads() -> list[threading.Thread]:
    """The stderr drain threads still running: none once a call returns or raises."""
    return [thread for thread in threading.enumerate() if thread.name == _process.STDERR_THREAD]


def letter(tmp_path, name: str = "letter.pdf"):
    return build_pdf(tmp_path / name, [PageSpec(items=(TextRun("Hello, world", 72.0, 72.0, size=12.0),))])


@pytest.mark.usefixtures("no_temporary_files")
class TestRenderWritesNothing:
    """A5a's render worker: its stderr is drained into the ring on every path."""

    def test_a_page_renders(self, tmp_path):
        page = render_page(letter(tmp_path), 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert (page.px_width, page.px_height) == (612, 792)

    def test_a_batch_renders(self, tmp_path):
        pages = render_pages(letter(tmp_path), ((0, 72), (0, 36)), memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert [(page.px_width, page.px_height) for page in pages] == [(612, 792), (306, 396)]

    def test_a_failed_worker_quotes_its_stderr(self, tmp_path, monkeypatch):
        path = letter(tmp_path)
        monkeypatch.setattr("sys.path", [shutil.__file__.rsplit("/", 1)[0]])  # the standard library only

        with pytest.raises(RenderFailed) as caught:
            render_page(path, 0, dpi=72, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        message = str(caught.value)
        assert "its stderr ends:" in message and "No module named 'datagrunt'" in message

    def test_a_batch_whose_worker_fails_quotes_its_stderr(self, tmp_path, monkeypatch):
        path = letter(tmp_path)
        monkeypatch.setattr("sys.path", [shutil.__file__.rsplit("/", 1)[0]])

        (result,) = render_pages(path, ((0, 72),), memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)

        assert isinstance(result, RenderFailed) and "No module named 'datagrunt'" in str(result)

    def test_a_worker_killed_at_its_timeout_ends_its_drain(self, tmp_path):
        with pytest.raises(RenderFailed, match=re.escape("page 1: the render timed out after 1.0 s")):
            render_page(slow_page(tmp_path), 0, dpi=600, memory_cap_bytes=CAP, timeout_s=1.0)


@pytest.mark.usefixtures("no_temporary_files")
class TestTesseractWritesNothing:
    """A5a's tesseract calls: stdin and stdout piped, stderr drained into the ring."""

    @needs_tesseract
    def test_tesseract_runs(self):
        call = ocr._call([trusted_executable("tesseract"), "--version"], (), 60.0)

        assert call.status == "ok" and b"tesseract" in call.stdout.lower() + call.stderr.encode().lower()

    @posix_only
    def test_a_failed_call_quotes_its_stderr(self, tmp_path, monkeypatch):
        directory = fake_tesseract(tmp_path, monkeypatch, 'cat > /dev/null\necho "Error: no osd" >&2\nexit 3')

        call = ocr._call([str(directory / "tesseract")], (b"P5\n1 1\n255\n", b"\x00"), 60.0)

        assert (call.status, call.reason) == ("failed", "tesseract exited with code 3; its stderr ends: Error: no osd")

    @posix_only
    def test_a_call_killed_at_its_timeout_ends_its_drain(self, tmp_path, monkeypatch):
        directory = fake_tesseract(tmp_path, monkeypatch, 'echo "warming up" >&2\nexec sleep 60')

        call = ocr._call([str(directory / "tesseract")], (), 0.5)

        assert call.status == "timeout"
        assert call.reason == "tesseract did not finish within 0.5 s and was killed; its stderr ends: warming up"

    @posix_only
    def test_a_call_whose_stderr_cannot_be_drained_is_unavailable_and_reaped(self, tmp_path, monkeypatch):
        directory = fake_tesseract(tmp_path, monkeypatch, "exec sleep 60")
        started = []
        real = subprocess.Popen

        def recorded(*args, **kwargs):
            started.append(real(*args, **kwargs))
            return started[-1]

        def refused(stream):
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(subprocess, "Popen", recorded)
        monkeypatch.setattr(_process, "StderrTail", refused)
        binary = str(directory / "tesseract")

        call = ocr._call([binary], (), 60.0)

        assert (call.status, call.reason) == (
            "unavailable",
            f"cannot drain tesseract's stderr ({binary}): can't start new thread",
        )
        ((process,),) = [started]
        assert process.returncode is not None  # killed and reaped


@pytest.mark.usefixtures("no_temporary_files")
class TestThePoolWritesNothing:
    """A8a's page workers (``workers > 1``): each one's stderr is drained into the ring on every path."""

    def test_the_pool_extracts(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)

        assert extract_document(path, workers=2, ocr="never") == extract_document(path, ocr="never")

    def test_a_crashed_worker_quotes_its_stderr(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        run_page_workers_as(monkeypatch, "complain_and_crash_on_page_3")

        pooled = extract_document(path, workers=2, ocr="never")

        ended = "the worker process extracting pages 3-4 ended (exit code 3) before returning them"
        assert [page.error for page in pooled.pages[2:]] == [f"{ended}; its stderr ends: pdfium: boom"] * 2

    def test_a_worker_killed_at_a_strict_failure_ends_its_drain(self, tmp_path, monkeypatch):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4), 1)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "hang_on_page_3")

        with pytest.raises(PDFCorruptError, match=re.escape("page 1 is unreadable")):
            extract_document(path, workers=2, strict=True)

        assert len(started) == 2

    def test_a_worker_lost_under_strict_is_raised_with_its_stderr(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        run_page_workers_as(monkeypatch, "complain_and_crash_on_page_3")

        with pytest.raises(DatagruntPDFError, match=re.escape("its stderr ends: pdfium: boom")):
            extract_document(path, workers=2, ocr="never", strict=True)
