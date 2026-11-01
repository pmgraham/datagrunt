"""Tests for the v5 PDF package's shared Python worker launcher (#369 A5a, #372 A8a): ``datagrunt.pdf._process``.

The launcher starts ``python -I -c BOOTSTRAP``; the bootstrap reads one
bounded JSON request, installs the caller's ``sys.path`` and runs the worker
the request names by a key of the fixed allowlist, nothing else.
"""

import io
import json
import re
import subprocess
import sys
from types import MappingProxyType

import pytest

from datagrunt.pdf import _process

ALLOWLIST = {"render": ("datagrunt.pdf.render", "_worker_main"), "pages": ("datagrunt.pdf.pipeline", "_worker_main")}

FAKE_WORKER = """
def _worker_main(request, replies):
    print("noise on stdout")
    replies.write(repr(request).encode())
    replies.flush()
"""


def run_bootstrap(stdin: bytes) -> subprocess.CompletedProcess:
    """BOOTSTRAP in ``python -I``, fed ``stdin``."""
    command = [sys.executable, "-I", "-c", _process.BOOTSTRAP]
    return subprocess.run(command, input=stdin, capture_output=True, check=False, timeout=60)


def fake_package(tmp_path):
    """A ``datagrunt.pdf`` whose ``pipeline`` and ``render`` workers each echo their request."""
    package = tmp_path / "datagrunt" / "pdf"
    package.mkdir(parents=True)
    (tmp_path / "datagrunt" / "__init__.py").write_text('print("noise at import")\n', encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    for name in ("pipeline", "render"):
        (package / f"{name}.py").write_text(FAKE_WORKER.replace("request).encode", f"('{name}', request)).encode"))
    return tmp_path


class TestAllowlist:
    """A request names its worker by a key of a fixed allowlist: never a module or a function."""

    def test_the_allowlist_is_exactly_the_two_workers_and_read_only(self):
        assert isinstance(_process.WORKERS, MappingProxyType)
        assert dict(_process.WORKERS) == ALLOWLIST

    @pytest.mark.parametrize("worker", ["render", "pages"])
    def test_a_named_worker_runs_with_the_sent_sys_path_and_stdout_kept_for_replies(self, tmp_path, worker):
        root = fake_package(tmp_path)
        request = {"sys_path": [str(root)], "worker": worker, "request": {"page_index": 0}}

        done = run_bootstrap(json.dumps(request).encode())

        module = ALLOWLIST[worker][0].rsplit(".", 1)[1]
        assert (done.returncode, done.stdout) == (0, repr((module, {"page_index": 0})).encode()), done.stderr
        assert b"noise at import" in done.stderr and b"noise on stdout" in done.stderr

    @pytest.mark.parametrize(
        "worker",
        [None, "os.system", "datagrunt.pdf.pipeline:_worker_main", ["datagrunt.pdf.render", "_worker_main"]],
        ids=["missing", "module", "module-function", "pair"],
    )
    def test_a_request_naming_no_allowlisted_worker_is_refused_before_any_import(self, tmp_path, worker):
        root = fake_package(tmp_path)
        request = {"sys_path": [str(root)], "request": {}} | ({} if worker is None else {"worker": worker})

        done = run_bootstrap(json.dumps(request).encode())

        assert done.returncode == 1
        assert b"the worker request names no worker of ('render', 'pages')" in done.stderr
        assert b"noise at import" not in done.stderr  # nothing of the caller's sys.path was imported

    def test_a_request_over_its_bound_is_refused_unread(self):
        done = run_bootstrap(b" " * (_process.MAX_REQUEST_BYTES + 1))

        assert done.returncode == 1
        assert b"the worker request is over 1048576 bytes" in done.stderr

    @pytest.mark.parametrize("path", ["/a/path", ["/a/path", 3]], ids=["str", "not-all-str"])
    def test_a_sys_path_that_is_not_a_list_of_str_is_refused(self, path):
        done = run_bootstrap(json.dumps({"sys_path": path, "worker": "pages", "request": {}}).encode())

        assert done.returncode == 1
        assert b"the worker request's sys_path is not a list of str" in done.stderr


class TestLaunch:
    """The command, the request and the start: ``python -I``, JSON on stdin, stdout and stdin piped."""

    def test_the_command_is_python_isolated_running_the_bootstrap(self):
        assert _process.worker_command("a page worker") == [sys.executable, "-I", "-c", _process.BOOTSTRAP]

    @pytest.mark.parametrize(
        "attribute,value,message",
        [
            pytest.param(
                "frozen",
                True,
                "a frozen application has no Python interpreter to start a page worker with",
                id="frozen",
            ),
            pytest.param(
                "executable",
                "",
                "sys.executable is empty: there is no Python interpreter for a page worker",
                id="empty",
            ),
        ],
    )
    def test_no_interpreter_is_refused_naming_the_worker(self, monkeypatch, attribute, value, message):
        monkeypatch.setattr(sys, attribute, value, raising=False)

        with pytest.raises(_process.NoInterpreter, match=re.escape(message)):
            _process.worker_command("a page worker")

    def test_the_request_is_json_with_the_callers_sys_path_and_the_workers_key(self):
        payload = json.loads(_process.request_payload("pages", {"pages": [0, 2]}, 1 << 20))

        assert payload == {"sys_path": sys.path, "worker": "pages", "request": {"pages": [0, 2]}}

    def test_a_request_over_its_bound_is_refused_before_a_worker_starts(self):
        with pytest.raises(ValueError, match=r"the pages request is \d+ bytes, over the 10-byte bound"):
            _process.request_payload("pages", {"pages": [0, 2]}, 10)

    def test_a_request_for_no_allowlisted_worker_is_refused(self):
        with pytest.raises(ValueError, match=re.escape("worker must be one of ('render', 'pages'), got 'os'")):
            _process.request_payload("os", {}, 1 << 20)

    def test_a_worker_starts_with_its_stdin_and_stdout_piped_and_stderr_to_the_file(self, tmp_path, monkeypatch):
        seen = []
        monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: seen.append((args, kwargs)) or "child")

        with open(tmp_path / "stderr", "w+b") as errors:
            assert _process.start_worker(["python"], errors) == "child"

            assert seen == [((["python"],), {"stdin": subprocess.PIPE, "stdout": subprocess.PIPE, "stderr": errors})]


class TestMessages:
    """Replies are length-prefixed raw bytes, each read within a bound."""

    def test_a_message_is_sent_whole_after_its_length_and_flushed(self):
        replies = io.BytesIO()

        _process.send_message(replies, b"P page")

        assert replies.getvalue() == _process.LENGTH.pack(6) + b"P page"

    def test_a_message_reads_back_whole(self):
        stream = io.BytesIO(_process.LENGTH.pack(6) + b"P page" + _process.LENGTH.pack(1) + b"D")

        assert [_process.read_message(stream, 6), _process.read_message(stream, 6)] == [b"P page", b"D"]
        assert _process.read_message(stream, 6) is None  # the end

    @pytest.mark.parametrize(
        "data",
        [_process.LENGTH.pack(6)[:3], _process.LENGTH.pack(6) + b"P pa"],
        ids=["part-of-a-length", "part-of-a-message"],
    )
    def test_a_message_cut_short_is_none(self, data):
        assert _process.read_message(io.BytesIO(data), 6) is None

    def test_a_message_over_its_bound_is_refused_unread(self):
        stream = io.BytesIO(_process.LENGTH.pack(7) + b"P pages")

        with pytest.raises(_process.MessageTooLong, match=re.escape("a 7-byte message, over the 6-byte bound")):
            _process.read_message(stream, 6)

        assert stream.tell() == _process.LENGTH.size  # the message itself was not read
