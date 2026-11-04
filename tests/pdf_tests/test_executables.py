"""``trusted_executable`` finds a program only in an absolute PATH directory."""

import os
import stat
import sys

import pytest

from datagrunt.pdf import _executables
from datagrunt.pdf._executables import trusted_executable

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="executable shell scripts are a POSIX fixture")


def fake_program(directory, name="tesseract"):
    """An executable script named ``name`` in ``directory``; returns its path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text("#!/bin/sh\nexit 0\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


@posix_only
class TestTrustedExecutable:
    def test_a_program_in_an_absolute_path_directory_is_found_by_its_absolute_path(self, tmp_path, monkeypatch):
        program = fake_program(tmp_path / "bin")
        monkeypatch.setenv("PATH", str(tmp_path / "bin"))

        found = trusted_executable("tesseract")

        assert found == str(program)
        assert os.path.isabs(found)

    @pytest.mark.parametrize("entry", [".", "", "local"], ids=["dot", "empty", "relative-dir"])
    def test_a_program_reached_only_through_a_relative_path_entry_is_not_found(self, tmp_path, monkeypatch, entry):
        # An empty or "." entry means the working directory, and a relative entry is under it.
        fake_program(tmp_path / entry if entry not in ("", ".") else tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("PATH", os.pathsep.join([entry, str(tmp_path / "empty-absolute")]))

        assert trusted_executable("tesseract") is None

    def test_an_absolute_directory_after_a_relative_one_is_still_searched(self, tmp_path, monkeypatch):
        fake_program(tmp_path / "cwd")
        program = fake_program(tmp_path / "bin")
        monkeypatch.chdir(tmp_path / "cwd")
        monkeypatch.setenv("PATH", os.pathsep.join([".", str(tmp_path / "bin")]))

        assert trusted_executable("tesseract") == str(program)

    def test_a_match_outside_the_absolute_directories_is_refused(self, tmp_path, monkeypatch):
        # Windows' search adds the working directory to any PATH: a match there is refused.
        elsewhere = fake_program(tmp_path / "cwd")
        fake_program(tmp_path / "bin")
        monkeypatch.setenv("PATH", str(tmp_path / "bin"))
        monkeypatch.setattr(_executables.shutil, "which", lambda name, path=None: str(elsewhere))

        assert trusted_executable("tesseract") is None

    @pytest.mark.parametrize("entry", ["linked", "homebrew/bin"], ids=["symlinked-directory", "real-directory"])
    def test_a_symlinked_binary_is_found_by_the_path_entrys_path_not_its_target(self, tmp_path, monkeypatch, entry):
        # As Homebrew links /opt/homebrew/bin/tesseract to the binary in its Cellar; the PATH entry
        # may itself be a symlink to that bin directory.
        target = fake_program(tmp_path / "Cellar" / "tesseract" / "5.5.3" / "bin")
        (tmp_path / "homebrew" / "bin").mkdir(parents=True)
        (tmp_path / "homebrew" / "bin" / "tesseract").symlink_to(target)
        (tmp_path / "linked").symlink_to(tmp_path / "homebrew" / "bin", target_is_directory=True)
        monkeypatch.setenv("PATH", str(tmp_path / entry))

        found = trusted_executable("tesseract")

        assert found == str(tmp_path / entry / "tesseract")
        assert os.path.realpath(found) == os.path.realpath(target)

    def test_no_path_finds_nothing(self, monkeypatch):
        monkeypatch.delenv("PATH", raising=False)

        assert trusted_executable("tesseract") is None


class TestArguments:
    @pytest.mark.parametrize(
        "name,error",
        [
            pytest.param("", ValueError, id="empty"),
            pytest.param("bin/tesseract", ValueError, id="relative-path"),
            pytest.param("/usr/bin/tesseract", ValueError, id="absolute-path"),
            pytest.param(None, TypeError, id="none"),
        ],
    )
    def test_name_must_be_a_bare_program_name(self, name, error):
        with pytest.raises(error, match="name"):
            trusted_executable(name)
