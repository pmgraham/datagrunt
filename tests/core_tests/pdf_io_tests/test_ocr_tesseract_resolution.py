"""v4 OCR runs tesseract by an absolute path found in an absolute PATH directory."""

import os
import stat
import sys

import pytest
from PIL import Image

pytesseract = pytest.importorskip("pytesseract")

from datagrunt.core.pdf_io.extraction import ocr  # noqa: E402

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="executable shell scripts are a POSIX fixture")


def fake_tesseract(directory, marker):
    """An executable ``tesseract`` script in ``directory`` that writes ``marker`` when run."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "tesseract"
    path.write_text(f"#!/bin/sh\necho ran > '{marker}'\nexit 1\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


@pytest.fixture
def default_command(monkeypatch):
    """pytesseract's command as installed (the bare name), restored after the test."""
    monkeypatch.setattr(pytesseract.pytesseract, "tesseract_cmd", "tesseract")


@posix_only
class TestOcrRunsATrustedTesseract:
    def test_a_tesseract_in_the_working_directory_is_never_run(self, tmp_path, monkeypatch, default_command):
        marker = tmp_path / "fake-ran"
        fake_tesseract(tmp_path / "cwd", marker)
        monkeypatch.chdir(tmp_path / "cwd")
        monkeypatch.setenv("PATH", os.pathsep.join([".", os.environ.get("PATH", "")]))

        try:
            ocr.ocr_data_to_blocks(Image.new("L", (40, 20), 255), 300)
        except Exception:  # noqa: BLE001 - only whether that program ran matters here
            pass

        assert not marker.exists()

    def test_the_command_run_is_the_absolute_trusted_path(self, tmp_path, monkeypatch, default_command):
        program = fake_tesseract(tmp_path / "bin", tmp_path / "unused")
        monkeypatch.setenv("PATH", str(tmp_path / "bin"))
        seen = []
        monkeypatch.setattr(
            pytesseract, "image_to_data", lambda *a, **k: seen.append(pytesseract.pytesseract.tesseract_cmd) or EMPTY
        )

        ocr.ocr_data_to_blocks(Image.new("L", (40, 20), 255), 300)

        assert seen == [str(program)]

    def test_with_no_trusted_tesseract_ocr_raises_and_runs_nothing(self, tmp_path, monkeypatch, default_command):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("PATH", str(tmp_path / "empty"))
        calls = []
        monkeypatch.setattr(pytesseract, "image_to_data", lambda *a, **k: calls.append(1) or EMPTY)

        with pytest.raises(pytesseract.TesseractNotFoundError):
            ocr.ocr_data_to_blocks(Image.new("L", (40, 20), 255), 300)
        assert calls == []

    def test_an_absolute_command_the_caller_set_is_kept(self, tmp_path, monkeypatch):
        program = fake_tesseract(tmp_path / "chosen", tmp_path / "unused")
        monkeypatch.setattr(pytesseract.pytesseract, "tesseract_cmd", str(program))
        monkeypatch.setenv("PATH", str(tmp_path / "empty"))
        seen = []
        monkeypatch.setattr(
            pytesseract, "image_to_data", lambda *a, **k: seen.append(pytesseract.pytesseract.tesseract_cmd) or EMPTY
        )

        ocr.ocr_data_to_blocks(Image.new("L", (40, 20), 255), 300)

        assert seen == [str(program)]

    def test_a_relative_command_the_caller_set_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pytesseract.pytesseract, "tesseract_cmd", os.path.join("bin", "tesseract"))
        calls = []
        monkeypatch.setattr(pytesseract, "image_to_data", lambda *a, **k: calls.append(1) or EMPTY)

        with pytest.raises(pytesseract.TesseractNotFoundError):
            ocr.ocr_data_to_blocks(Image.new("L", (40, 20), 255), 300)
        assert calls == []


EMPTY = {key: [] for key in ("text", "conf", "block_num", "par_num", "line_num", "left", "top", "width", "height")}
