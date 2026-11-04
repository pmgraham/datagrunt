"""Tests for the v5 source-file check (#371): ``datagrunt.pdf._source.checked_pdf_file``; and ``_opened_source`` (#453).

PDFs are ReportLab builds (``pdf_tests.builders``); a header after leading
junk is that build with bytes put before it. Where the check claims to agree
with pdfium, pdfium (pypdfium2) opens the same file in the test.
"""

import os
import re
import sys
import threading
from collections.abc import Callable
from pathlib import Path

import pypdfium2 as pdfium
import pytest

from datagrunt.pdf import _source
from datagrunt.pdf._source import checked_pdf_file
from pdf_tests.builders import PageSpec, TextRun, build_pdf


@pytest.fixture(scope="module")
def pdf_bytes(tmp_path_factory) -> bytes:
    """A one-page PDF's bytes: it starts with ``%PDF-1.4``."""
    path = build_pdf(tmp_path_factory.mktemp("pdf") / "base.pdf", [PageSpec(items=(TextRun("Hello", 72.0, 100.0),))])
    data = path.read_bytes()
    assert data.startswith(b"%PDF-1.4\n")
    return data


def pdfium_opens(path: Path) -> bool:
    """True if pdfium loads the file at ``path`` as a PDF."""
    try:
        document = pdfium.PdfDocument(str(path))
    except pdfium.PdfiumError:
        return False
    document.close()
    return True


class TestAccepted:
    def test_a_pdf_is_returned_as_it_is(self, tmp_path, pdf_bytes):
        path = tmp_path / "doc.pdf"
        path.write_bytes(pdf_bytes)
        assert checked_pdf_file(path, "source") is path

    def test_a_symlink_to_a_pdf_is_followed(self, tmp_path, pdf_bytes):
        target = tmp_path / "doc.pdf"
        target.write_bytes(pdf_bytes)
        link = tmp_path / "link.pdf"
        link.symlink_to(target)
        assert checked_pdf_file(link, "source") is link

    def test_the_name_does_not_matter(self, tmp_path, pdf_bytes):
        path = tmp_path / "scan.bin"
        path.write_bytes(pdf_bytes)
        assert checked_pdf_file(path, "source") is path


class TestHeaderAgreesWithPdfium:
    """pdfium looks for ``%PDF`` starting at any offset from 0 to 1024: the check accepts exactly what it opens."""

    @pytest.mark.parametrize(
        ("junk", "opens"),
        [(0, True), (1, True), (500, True), (1019, True), (1020, True), (1024, True), (1025, False), (1100, False)],
    )
    def test_a_header_after_leading_junk(self, tmp_path, pdf_bytes, junk, opens):
        path = tmp_path / "doc.pdf"
        path.write_bytes(b"x" * junk + pdf_bytes)
        assert pdfium_opens(path) is opens
        if opens:
            assert checked_pdf_file(path, "source") is path
        else:
            with pytest.raises(ValueError, match="source is not a PDF: no %PDF header starts in its first 1025 bytes"):
                checked_pdf_file(path, "source")

    @pytest.mark.parametrize(
        ("header", "opens"),
        [(b"%PDF-1.4", True), (b"%PDF", True), (b"%PDFX1.4", True), (b"%pdf-1.4", False), (b"%PD-1.4", False)],
    )
    def test_the_header_is_pdfiums_four_bytes(self, tmp_path, pdf_bytes, header, opens):
        path = tmp_path / "doc.pdf"
        path.write_bytes(header + pdf_bytes[len(b"%PDF-1.4") :])
        assert pdfium_opens(path) is opens
        if opens:
            assert checked_pdf_file(path, "source") is path
        else:
            with pytest.raises(ValueError, match="source is not a PDF"):
                checked_pdf_file(path, "source")

    @pytest.mark.parametrize("data", [b"", b"%PD", b"plain text, not a PDF\n" * 100])
    def test_a_file_without_a_header_is_refused(self, tmp_path, data):
        path = tmp_path / "doc.pdf"
        path.write_bytes(data)
        assert not pdfium_opens(path)
        with pytest.raises(ValueError, match="source is not a PDF"):
            checked_pdf_file(path, "source")


class TestRefused:
    def test_a_missing_path_is_file_not_found_naming_the_argument(self, tmp_path):
        path = tmp_path / "missing.pdf"
        with pytest.raises(FileNotFoundError, match="source does not exist") as caught:
            checked_pdf_file(path, "source")
        assert caught.value.filename == str(path)

    def test_a_path_through_a_file_is_file_not_found(self, tmp_path):
        (tmp_path / "file").write_bytes(b"x")
        with pytest.raises(FileNotFoundError, match="pdf does not exist"):
            checked_pdf_file(tmp_path / "file" / "doc.pdf", "pdf")

    def test_a_directory_is_not_a_regular_file(self, tmp_path):
        with pytest.raises(ValueError, match="source is not a regular file"):
            checked_pdf_file(tmp_path, "source")

    @pytest.mark.skipif(sys.platform == "win32", reason="no named pipes as files on Windows")
    def test_a_named_pipe_is_refused_without_blocking(self, tmp_path):
        pipe = tmp_path / "doc.pdf"
        os.mkfifo(pipe)
        outcome: list = []

        def check():
            try:
                checked_pdf_file(pipe, "source")
            except ValueError as exc:
                outcome.append(str(exc))

        thread = threading.Thread(target=check, daemon=True)
        thread.start()
        thread.join(10.0)
        assert not thread.is_alive(), "the check blocked opening a named pipe"
        assert len(outcome) == 1 and "source is not a regular file" in outcome[0]

    def test_a_pipe_swapped_in_after_the_stat_is_refused_by_fstat(self, tmp_path, monkeypatch, pdf_bytes):
        path = tmp_path / "doc.pdf"
        path.write_bytes(pdf_bytes)
        real_open = os.open
        read_fd, write_fd = os.pipe()
        os.close(write_fd)

        def swapped(target, flags, *args):
            if Path(target) == path:
                return os.dup(read_fd)
            return real_open(target, flags, *args)

        monkeypatch.setattr(_source.os, "open", swapped)
        try:
            with pytest.raises(ValueError, match="source is not a regular file"):
                checked_pdf_file(path, "source")
        finally:
            os.close(read_fd)

    @pytest.mark.parametrize(
        ("path", "name", "error", "message"),
        [
            ("doc.pdf", "source", TypeError, "path must be a Path, got str"),
            (None, "source", TypeError, "path must be a Path, got NoneType"),
            (Path("doc.pdf"), " ", ValueError, "name must be a non-blank str"),
            (Path("doc.pdf"), 3, TypeError, "name must be a str"),
        ],
    )
    def test_bad_arguments_are_refused(self, path, name, error, message):
        with pytest.raises(error, match=message):
            checked_pdf_file(path, name)


class TestRead:
    def test_it_reads_at_most_the_header_window_and_closes_the_file(self, tmp_path, monkeypatch, pdf_bytes):
        path = tmp_path / "doc.pdf"
        path.write_bytes(b"x" * 1024 + pdf_bytes + b"y" * (1 << 20))
        opened, closed, read = [], [], []
        real_open, real_close, real_read = os.open, os.close, os.read

        def spy_open(target, flags, *args):
            descriptor = real_open(target, flags, *args)
            opened.append(descriptor)
            return descriptor

        def spy_read(descriptor, size):
            data = real_read(descriptor, size)
            read.append(len(data))
            return data

        def spy_close(descriptor):
            closed.append(descriptor)
            real_close(descriptor)

        monkeypatch.setattr(_source.os, "open", spy_open)
        monkeypatch.setattr(_source.os, "read", spy_read)
        monkeypatch.setattr(_source.os, "close", spy_close)
        checked_pdf_file(path, "source")
        assert sum(read) == 1028 and len(opened) == 1 and closed == opened

    def test_short_reads_are_continued_up_to_the_window(self, tmp_path, monkeypatch, pdf_bytes):
        path = tmp_path / "doc.pdf"
        path.write_bytes(b"x" * 1024 + pdf_bytes)
        real_read = os.read
        monkeypatch.setattr(_source.os, "read", lambda descriptor, size: real_read(descriptor, min(size, 7)))
        assert checked_pdf_file(path, "source") is path

    @pytest.mark.parametrize("failure", ["header", "read"])
    def test_the_file_is_closed_when_the_check_fails(self, tmp_path, monkeypatch, failure):
        path = tmp_path / "doc.pdf"
        path.write_bytes(b"not a pdf")
        opened, closed = [], []
        real_open, real_close = os.open, os.close

        def spy_open(target, flags, *args):
            opened.append(real_open(target, flags, *args))
            return opened[-1]

        def spy_close(descriptor):
            closed.append(descriptor)
            real_close(descriptor)

        def broken(descriptor, size):
            raise OSError(5, "I/O error")

        monkeypatch.setattr(_source.os, "open", spy_open)
        monkeypatch.setattr(_source.os, "close", spy_close)
        if failure == "read":
            monkeypatch.setattr(_source, "_read_at_most", broken)
        with pytest.raises(OSError if failure == "read" else ValueError):
            checked_pdf_file(path, "source")
        monkeypatch.undo()
        assert len(opened) == 1 and closed == opened


def promptly(call: Callable[[], object], pipe: Path) -> BaseException | None:
    """What ``call()`` raised (None if it returned), run in a thread given 10 s: a call still blocked fails the test.

    A call blocked opening the named pipe ``pipe`` for reading is let go first (the pipe opened for writing, held
    until the call ends), so no thread is left blocked.
    """
    raised: list[BaseException | None] = []

    def run() -> None:
        try:
            returned = call()
        except Exception as exc:
            raised.append(exc)
            return
        raised.append(None)
        if hasattr(returned, "close"):
            returned.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(10.0)
    if not thread.is_alive():
        return raised[0]
    writer = os.open(pipe, os.O_WRONLY | os.O_NONBLOCK)  # a reader blocked opening it holds it open: this succeeds
    try:
        thread.join(10.0)
    finally:
        os.close(writer)
    pytest.fail(f"opening the named pipe {pipe} blocked")


def stat_of_a_regular_file_at(monkeypatch, path: Path, regular: Path) -> None:
    """Make ``os.stat`` of ``path`` (``is_file``'s too) see the regular file ``regular``: what is there came later."""
    real = os.stat

    def stat(target, *args, **kwargs):
        named = isinstance(target, (str, os.PathLike)) and Path(target) == path
        return real(regular if named else target, *args, **kwargs)

    monkeypatch.setattr(_source.os, "stat", stat)


class TestOpenedSource:
    """``_opened_source``: the source opened once for its hash and identity, as checked_pdf_file opens it (#453)."""

    def test_a_pdf_is_opened_for_reading_as_bytes(self, tmp_path, pdf_bytes):
        path = tmp_path / "doc.pdf"
        path.write_bytes(pdf_bytes)

        with _source._opened_source(path) as stream:
            assert os.fstat(stream.fileno()).st_ino == path.stat().st_ino
            assert stream.read(5) == pdf_bytes[:5] and stream.read() == pdf_bytes[5:]

    @pytest.mark.parametrize("missing", ["missing.pdf", "file/doc.pdf"], ids=["missing", "through-a-file"])
    def test_a_missing_path_is_file_not_found(self, tmp_path, missing):
        (tmp_path / "file").write_bytes(b"x")
        path = tmp_path / missing

        with pytest.raises(FileNotFoundError, match="not an existing file") as caught:
            _source._opened_source(path)
        assert caught.value.filename == str(path)

    @pytest.mark.skipif(sys.platform == "win32", reason="a directory does not open for reading on Windows")
    def test_a_directory_is_refused_and_its_descriptor_closed(self, tmp_path, monkeypatch):
        opened, closed = [], []
        real_open, real_close = os.open, os.close

        def spy_open(target, flags, *args):
            opened.append(real_open(target, flags, *args))
            return opened[-1]

        def spy_close(descriptor):
            closed.append(descriptor)
            real_close(descriptor)

        monkeypatch.setattr(_source.os, "open", spy_open)
        monkeypatch.setattr(_source.os, "close", spy_close)
        with pytest.raises(ValueError, match=f"^source is not a regular file: {re.escape(str(tmp_path))}$"):
            _source._opened_source(tmp_path)
        monkeypatch.undo()
        assert len(opened) == 1 and closed == opened

    @pytest.mark.skipif(sys.platform == "win32", reason="no named pipes as files on Windows")
    def test_a_named_pipe_put_at_the_path_after_its_stat_is_refused_without_blocking(
        self, tmp_path, monkeypatch, pdf_bytes
    ):
        pipe, regular = tmp_path / "doc.pdf", tmp_path / "regular.pdf"
        regular.write_bytes(pdf_bytes)
        os.mkfifo(pipe)
        stat_of_a_regular_file_at(monkeypatch, pipe, regular)

        raised = promptly(lambda: _source._opened_source(pipe), pipe)

        assert isinstance(raised, ValueError) and str(raised) == f"source is not a regular file: {pipe}"
