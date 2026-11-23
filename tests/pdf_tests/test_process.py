"""Tests for the v5 PDF package's shared Python worker launcher (#369 A5a, #372 A8a): ``datagrunt.pdf._process``.

The launcher starts ``python -I -c BOOTSTRAP``; the bootstrap reads one
bounded JSON request, installs the caller's ``sys.path`` and runs the worker
the request names by a key of the fixed allowlist, nothing else.
"""

import io
import json
import os
import re
import subprocess
import sys
import threading
import time
from types import MappingProxyType

import pytest

from datagrunt.pdf import _process

ALLOWLIST = {
    "figures": ("datagrunt.pdf._figure_worker", "_worker_main"),
    "pages": ("datagrunt.pdf._page_worker", "_worker_main"),
    "render": ("datagrunt.pdf._render_worker", "_worker_main"),
}

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
    """A ``datagrunt.pdf`` whose figure, page and render workers each echo their request."""
    package = tmp_path / "datagrunt" / "pdf"
    package.mkdir(parents=True)
    (tmp_path / "datagrunt" / "__init__.py").write_text('print("noise at import")\n', encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    for name in ("_figure_worker", "_page_worker", "_render_worker"):
        (package / f"{name}.py").write_text(FAKE_WORKER.replace("request).encode", f"('{name}', request)).encode"))
    return tmp_path


class TestAllowlist:
    """A request names its worker by a key of a fixed allowlist: never a module or a function."""

    def test_the_allowlist_is_exactly_the_three_workers_and_read_only(self):
        assert isinstance(_process.WORKERS, MappingProxyType)
        assert dict(_process.WORKERS) == ALLOWLIST

    @pytest.mark.parametrize("worker", ["figures", "pages", "render"])
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
        assert b"the worker request names no worker of ('figures', 'pages', 'render')" in done.stderr
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
        assert _process.worker_command() == [sys.executable, "-I", "-c", _process.BOOTSTRAP]

    @pytest.mark.parametrize(
        "attribute,value,message",
        [
            pytest.param(
                "frozen",
                True,
                "this application is frozen (sys.frozen): datagrunt reads PDFs in worker processes and cannot start "
                "one here",
                id="frozen",
            ),
            pytest.param(
                "executable",
                "",
                "sys.executable is empty: datagrunt reads PDFs in worker processes and cannot start one here",
                id="empty",
            ),
        ],
    )
    def test_no_interpreter_is_refused_naming_the_cause(self, monkeypatch, attribute, value, message):
        monkeypatch.setattr(sys, attribute, value, raising=False)

        with pytest.raises(_process.NoInterpreter) as caught:
            _process.worker_command()

        assert str(caught.value) == message

    def test_the_request_is_json_with_the_callers_sys_path_and_the_workers_key(self):
        payload = json.loads(_process.request_payload("pages", {"pages": [0, 2]}, 1 << 20))

        assert payload == {"sys_path": sys.path, "worker": "pages", "request": {"pages": [0, 2]}}

    def test_a_request_over_its_bound_is_refused_before_a_worker_starts(self):
        with pytest.raises(ValueError, match=r"the pages request is \d+ bytes, over the 10-byte bound"):
            _process.request_payload("pages", {"pages": [0, 2]}, 10)

    def test_a_request_for_no_allowlisted_worker_is_refused(self):
        with pytest.raises(
            ValueError, match=re.escape("worker must be one of ('figures', 'pages', 'render'), got 'os'")
        ):
            _process.request_payload("os", {}, 1 << 20)

    @pytest.mark.parametrize("own_group", [False, True], ids=["callers-group", "own-group"])
    def test_a_worker_starts_with_its_stdin_stdout_and_stderr_piped(self, monkeypatch, own_group):
        seen = []
        real = subprocess.Popen

        def recorded(*args, **kwargs):
            seen.append((args, kwargs))
            return real(*args, **kwargs)

        monkeypatch.setattr(subprocess, "Popen", recorded)
        command = [sys.executable, "-c", "import sys; sys.stderr.write('started')"]

        child, stderr = _process.start_worker(command, own_group=own_group)

        session = own_group and os.name == "posix"
        piped = {"stdin": subprocess.PIPE, "stdout": subprocess.PIPE, "stderr": subprocess.PIPE}
        assert seen == [((command,), {"start_new_session": session, **piped})]
        assert _process.reaped(child, 60.0) == 0
        _process.close_pipes(child)
        assert stderr.finish(_process.REAP_TIMEOUT_S) == "started"

    def test_a_worker_whose_stderr_cannot_be_drained_is_killed_and_reaped(self, monkeypatch):
        started = []
        real = subprocess.Popen

        def recorded(*args, **kwargs):
            started.append(real(*args, **kwargs))
            return started[-1]

        def refused(thread):
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(subprocess, "Popen", recorded)
        monkeypatch.setattr(threading.Thread, "start", refused)

        with pytest.raises(RuntimeError, match="can't start new thread"):
            _process.start_worker([sys.executable, "-c", "import time; time.sleep(60)"])

        ((child,),) = [started]
        assert child.returncode is not None  # killed and reaped, not left running
        assert (child.stdin.closed, child.stdout.closed, child.stderr.closed) == (True, True, True)


def child_writing(data: bytes) -> "subprocess.Popen[bytes]":
    """A child that writes ``data`` to its stderr, in 1000-byte writes, then exits."""
    script = "import sys\ndata = sys.stdin.buffer.read()\nfor i in range(0, len(data), 1000):\n"
    script += "    sys.stderr.buffer.write(data[i:i + 1000])\n    sys.stderr.buffer.flush()\n"
    child = subprocess.Popen([sys.executable, "-c", script], stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    child.stdin.write(data)
    child.stdin.close()
    return child


def drained_child(data: bytes) -> tuple["subprocess.Popen[bytes]", _process.StderrTail]:
    child = child_writing(data)
    return child, _process.drained(child)


def stderr_threads() -> list[threading.Thread]:
    """The stderr drain threads still running."""
    return [thread for thread in threading.enumerate() if thread.name == _process.STDERR_THREAD]


# A child that starts a descendant (it stays in the child's process group) and writes its pid to argv[1].
WITH_A_DESCENDANT = """
import subprocess, sys, time
descendant = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(sys.argv[1], "w") as out:
    out.write(str(descendant.pid))
time.sleep(60)
"""


def written_pid(path) -> int:
    deadline = time.monotonic() + 30.0
    while not path.exists() or not path.read_text():
        assert time.monotonic() < deadline, "the child did not start its descendant"
        time.sleep(0.02)
    return int(path.read_text())


def ends_within(pid: int, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.02)
    return False


@pytest.mark.skipif(sys.platform == "win32", reason="a worker leads a process group on POSIX only")
class TestKillGroup:
    """kill_group ends a worker started with own_group and every process that stayed in its group."""

    def test_a_workers_descendant_ends_with_it(self, tmp_path):
        child, stderr = _process.start_worker(
            [sys.executable, "-c", WITH_A_DESCENDANT, str(tmp_path / "pid")], own_group=True
        )
        descendant = written_pid(tmp_path / "pid")

        _process.kill_group(child)

        assert _process.reaped(child, 5.0) == -9
        _process.close_pipes(child)
        assert ends_within(descendant, 5.0)
        assert stderr.finish(5.0) == ""  # the descendant held the stderr pipe too: it ended

    def test_a_reaped_worker_is_never_signalled(self, monkeypatch):
        child, stderr = _process.start_worker([sys.executable, "-c", "pass"], own_group=True)
        assert _process.reaped(child, 60.0) == 0
        _process.close_pipes(child)
        stderr.finish(5.0)
        monkeypatch.setattr(os, "killpg", lambda *args: pytest.fail("a reaped worker's group was signalled"))

        _process.kill_group(child)

    def test_a_refused_group_signal_falls_back_to_the_worker(self, monkeypatch):
        # macOS refuses (EPERM) to signal a group whose only process is the exited, unreaped worker.
        child, stderr = _process.start_worker([sys.executable, "-c", "import time; time.sleep(60)"], own_group=True)

        def refused(*args):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(os, "killpg", refused)
        _process.kill_group(child)

        assert _process.reaped(child, 5.0) == -9
        _process.close_pipes(child)
        stderr.finish(5.0)


class TestStderrTail:
    """A child's stderr is drained by a thread of its own into a 4 KiB ring: nothing is written to disk."""

    @pytest.mark.parametrize(
        "written,tail",
        [
            pytest.param(b"", "", id="empty"),
            pytest.param(b"boom\n", "boom", id="short"),
            pytest.param(b"x" * 5000 + b"\xffend", "x" * 4092 + "\\xffend", id="long"),
            pytest.param(b"y" * (1 << 20) + b"last words", "y" * 4086 + "last words", id="1-mib"),
        ],
    )
    def test_the_tail_is_the_last_4_kib_the_child_wrote(self, written, tail):
        child, stderr = drained_child(written)

        assert _process.reaped(child, 60.0) == 0
        assert stderr.finish(_process.REAP_TIMEOUT_S) == tail

    def test_at_most_4_kib_is_held_however_much_the_child_writes(self):
        child, stderr = drained_child(b"z" * (1 << 20))

        assert _process.reaped(child, 60.0) == 0
        stderr.finish(_process.REAP_TIMEOUT_S)
        assert len(stderr._kept) == 4096

    def test_finish_waits_for_the_drain_to_reach_the_end(self):
        read_end, write_end = os.pipe()
        stderr = _process.StderrTail(os.fdopen(read_end, "rb"))

        def late():
            os.write(write_end, b"late words")
            os.close(write_end)

        threading.Timer(0.5, late).start()

        assert stderr.finish(_process.REAP_TIMEOUT_S) == "late words"  # written after finish began waiting
        assert not stderr._thread.is_alive()

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_finish_is_bounded_when_the_stderr_stays_open(self):
        read_end, write_end = os.pipe()
        stream = os.fdopen(read_end, "rb")
        stderr = _process.StderrTail(stream)
        os.write(write_end, b"so far")
        try:
            began = time.monotonic()
            said = stderr.finish(0.2)
            elapsed = time.monotonic() - began
        finally:
            os.close(write_end)  # the drain then ends on its own, and closes the stream

        assert said == "so far (its stderr was still open after 0.2 s)"
        assert elapsed < 1.5  # its bound, 0.2 s, with over 6 times the headroom
        assert stderr.finish(_process.REAP_TIMEOUT_S) == "so far"
        assert stream.closed

    def test_a_drain_still_running_is_said_and_left_to_end_on_its_own(self):
        read_end, write_end = os.pipe()
        stream = os.fdopen(read_end, "rb")
        stderr = _process.StderrTail(stream)
        try:
            said = stderr.finish(0.0)
        finally:
            os.close(write_end)

        assert said == "(its stderr was still open after 0 s)"
        assert stderr.finish(_process.REAP_TIMEOUT_S) == ""
        assert stream.closed  # by the drain, at its end: never by finish

    def test_the_drain_closes_the_stream_at_its_end_and_finish_is_idempotent(self):
        child, stderr = drained_child(b"once")
        stream = child.stderr

        assert _process.reaped(child, 60.0) == 0
        assert [stderr.finish(_process.REAP_TIMEOUT_S) for _ in range(2)] == ["once", "once"]
        assert stream.closed
        assert stderr_threads() == []

    def test_a_read_that_fails_is_said_never_swallowed(self):
        class Unreadable:
            closed = False

            def fileno(self):
                return -1  # os.read raises EBADF: no descriptor of this process is read

            def close(self):
                self.closed = True

        stream = Unreadable()

        said = _process.StderrTail(stream).finish(_process.REAP_TIMEOUT_S)

        assert said.startswith("(its stderr could not be read to its end: [Errno 9] Bad file descriptor")
        assert stream.closed

    def test_a_child_without_a_stderr_pipe_is_refused(self):
        child = subprocess.Popen([sys.executable, "-c", ""], stdout=subprocess.DEVNULL)
        try:
            with pytest.raises(ValueError, match="the child's stderr must be a pipe"):
                _process.drained(child)
        finally:
            _process.reaped(child, 60.0)


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


# The CSV, Excel and Parquet stacks, and polars and pyarrow, which only a caller's frames and tables use: no
# worker's work uses them (#439). Each package, and every module inside it.
NOT_IN_A_WORKER = (
    "datagrunt.core",
    "datagrunt.csv_api",
    "datagrunt.excel_api",
    "datagrunt.parquet_api",
    "duckdb",
    "fastexcel",
    "polars",
    "pyarrow",
    "xlsxwriter",
)
# The bootstrap's last line hands the request to the worker. The probe runs there instead: it replies with the
# names in sys.modules as the worker finds them when it begins its request, its module imported, its entry found.
ENTRY = 'entry(message.get("request"), replies)'
PROBE = "replies.write(json.dumps(sorted(sys.modules)).encode()); replies.flush()"


def inside(name: str, packages: tuple[str, ...]) -> bool:
    """True if the module ``name`` is one of ``packages``, or inside one."""
    return any(name == package or name.startswith(f"{package}.") for package in packages)


class TestWorkerImports:
    """A worker starts with what its work imports, and nothing of the CSV, Excel or Parquet stacks (#439)."""

    @pytest.mark.parametrize("worker", sorted(ALLOWLIST))
    def test_a_worker_begins_its_request_without_the_csv_excel_or_parquet_stacks(self, worker):
        assert _process.BOOTSTRAP.count(ENTRY) == 1
        command = [sys.executable, "-I", "-c", _process.BOOTSTRAP.replace(ENTRY, PROBE)]
        payload = _process.request_payload(worker, None, _process.MAX_REQUEST_BYTES)  # the caller's own sys.path

        done = subprocess.run(command, input=payload, capture_output=True, check=False, timeout=60)

        assert done.returncode == 0, done.stderr
        loaded = json.loads(done.stdout)
        assert ALLOWLIST[worker][0] in loaded  # the probe ran where the request is handled
        assert [name for name in loaded if inside(name, NOT_IN_A_WORKER)] == []


FROZEN_WORKER_SCRIPT = """
import sys
sys.frozen = True
from datagrunt.pdf import freeze_support
freeze_support()
print("MAIN_RAN")
sys.exit(99)
"""

FROZEN_WORKER_ECHO_SCRIPT = """
import sys
from types import MappingProxyType
sys.frozen = True
from datagrunt.pdf import _process, freeze_support


def _echo_worker(request, replies):
    print("frozen noise on stdout")
    _process.send_message(replies, repr(request).encode("utf-8"))


_process.WORKERS = MappingProxyType({**_process.WORKERS, "pages": ("__main__", "_echo_worker")})
freeze_support()
print("MAIN_RAN")
sys.exit(99)
"""

FROZEN_MISSING_EXTRA_SCRIPT = """
import sys
sys.frozen = True
sys.modules["pypdfium2"] = None
from datagrunt.pdf import freeze_support
freeze_support()
print("MAIN_RAN")
sys.exit(99)
"""


def run_frozen_worker(tmp_path, script: str, stdin: bytes) -> subprocess.CompletedProcess:
    """Run ``script`` as a frozen worker subprocess invoked with ``WORKER_FLAG``."""
    entry = tmp_path / "frozen_entry.py"
    entry.write_text(script, encoding="utf-8")
    command = [sys.executable, str(entry), _process.WORKER_FLAG]
    return subprocess.run(command, input=stdin, capture_output=True, check=False, timeout=60)


class TestFreezeSupport:
    """Explicit opt-in freeze_support() enables allowlisted worker processes in frozen apps (#440)."""

    def test_worker_flag_and_freeze_support_are_exported(self):
        assert _process.WORKER_FLAG == "--datagrunt-pdf-worker"
        assert "WORKER_FLAG" in _process.__all__
        assert "freeze_support" in _process.__all__

    def test_freeze_support_is_a_no_op_in_a_non_frozen_process(self, monkeypatch):
        monkeypatch.setattr(_process, "_FROZEN_SUPPORT", threading.Event())

        _process.freeze_support()

        assert _process.worker_command() == [sys.executable, "-I", "-c", _process.BOOTSTRAP]
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        with pytest.raises(_process.NoInterpreter, match="this application is frozen"):
            _process.worker_command()

    def test_freeze_support_ignores_worker_flag_when_not_frozen(self, monkeypatch):
        monkeypatch.setattr(_process, "_FROZEN_SUPPORT", threading.Event())
        monkeypatch.setattr(sys, "argv", ["app.py", _process.WORKER_FLAG])

        _process.freeze_support()

        assert _process.worker_command() == [sys.executable, "-I", "-c", _process.BOOTSTRAP]

    def test_freeze_support_enables_frozen_worker_command(self, monkeypatch):
        monkeypatch.setattr(_process, "_FROZEN_SUPPORT", threading.Event())
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "argv", ["my-app"])

        with pytest.raises(_process.NoInterpreter, match="this application is frozen"):
            _process.worker_command()

        _process.freeze_support()

        assert _process.worker_command() == [sys.executable, _process.WORKER_FLAG]

    def test_freeze_support_in_frozen_process_still_rejects_empty_executable(self, monkeypatch):
        monkeypatch.setattr(_process, "_FROZEN_SUPPORT", threading.Event())
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "executable", "")
        monkeypatch.setattr(sys, "argv", ["my-app"])

        _process.freeze_support()

        with pytest.raises(_process.NoInterpreter, match="sys.executable is empty"):
            _process.worker_command()

    def test_freeze_support_ignores_extra_argv_tokens_with_worker_flag(self, monkeypatch):
        monkeypatch.setattr(_process, "_FROZEN_SUPPORT", threading.Event())
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "argv", ["my-app", _process.WORKER_FLAG, "--extra"])

        _process.freeze_support()

        assert _process.worker_command() == [sys.executable, _process.WORKER_FLAG]

    def test_install_sys_path_preserves_meipass_entries_first(self, monkeypatch):
        monkeypatch.setattr(sys, "_MEIPASS", "/tmp/_MEI_child", raising=False)
        monkeypatch.setattr(sys, "path", ["/tmp/_MEI_child", "/tmp/_MEI_child/lib", "/other"])

        _process._install_sys_path(["/tmp/_MEI_parent", "/custom", "/tmp/_MEI_child"])

        assert sys.path == ["/tmp/_MEI_child", "/tmp/_MEI_child/lib", "/tmp/_MEI_parent", "/custom"]

    def test_install_sys_path_replaces_path_without_meipass(self, monkeypatch):
        monkeypatch.delattr(sys, "_MEIPASS", raising=False)
        monkeypatch.setattr(sys, "path", ["/old"])

        _process._install_sys_path(["/new/a", "/new/b"])

        assert sys.path == ["/new/a", "/new/b"]

    def test_read_worker_request_validates_bound_json_and_fields(self):
        with pytest.raises(ValueError, match="bound must be at least 1"):
            _process._read_worker_request(io.BytesIO(b"{}"), 0)
        with pytest.raises(SystemExit, match="over 4 bytes"):
            _process._read_worker_request(io.BytesIO(b"12345"), 4)
        with pytest.raises(SystemExit, match="not valid JSON"):
            _process._read_worker_request(io.BytesIO(b"{bad"), 1024)
        with pytest.raises(SystemExit, match="must be a JSON object"):
            _process._read_worker_request(io.BytesIO(b"[]"), 1024)
        with pytest.raises(SystemExit, match="sys_path is not a list of str"):
            _process._read_worker_request(io.BytesIO(b'{"sys_path": "x", "worker": "pages"}'), 1024)
        with pytest.raises(SystemExit, match="names no worker"):
            _process._read_worker_request(io.BytesIO(b'{"sys_path": [], "worker": "bad"}'), 1024)
        valid = json.dumps({"sys_path": ["/a"], "worker": "render", "request": {"k": 1}}).encode()
        assert _process._read_worker_request(io.BytesIO(valid), 1024) == (["/a"], "render", {"k": 1})

    def test_load_worker_entry_validates_allowlist_extra_and_callability(self, monkeypatch):
        with pytest.raises(SystemExit, match="names no worker"):
            _process._load_worker_entry("os.system", io.BytesIO())
        monkeypatch.setattr(
            _process.importlib,
            "import_module",
            lambda _m: (_ for _ in ()).throw(ModuleNotFoundError("no pypdfium2", name="pypdfium2")),
        )
        replies = io.BytesIO()
        assert _process._load_worker_entry("pages", replies) is None
        msg = _process.read_message(io.BytesIO(replies.getvalue()), 1 << 20)
        assert msg is not None and msg[:1] == _process.ERROR
        monkeypatch.setattr(
            _process.importlib,
            "import_module",
            lambda _m: (_ for _ in ()).throw(ModuleNotFoundError("no other", name="other_mod")),
        )
        with pytest.raises(ModuleNotFoundError, match="no other"):
            _process._load_worker_entry("pages", io.BytesIO())

    def test_load_worker_entry_rejects_non_callable_attribute(self, monkeypatch):
        class FakeModule:
            _worker_main = 42

        monkeypatch.setattr(_process.importlib, "import_module", lambda _m: FakeModule)
        with pytest.raises(TypeError, match="not callable"):
            _process._load_worker_entry("pages", io.BytesIO())

        def fake_worker(request, replies):
            return None

        FakeModule._worker_main = fake_worker
        assert _process._load_worker_entry("pages", io.BytesIO()) is fake_worker

    def test_redirect_stdout_to_stderr_closes_dup_fd_on_dup2_failure(self, monkeypatch):
        closed: list[int] = []
        with monkeypatch.context() as scoped:
            scoped.setattr(os, "dup", lambda fd: 99)
            scoped.setattr(os, "dup2", lambda src, dst: (_ for _ in ()).throw(OSError("dup2 failed")))
            scoped.setattr(os, "close", lambda fd: closed.append(fd))
            with pytest.raises(OSError, match="dup2 failed"):
                _process._redirect_stdout_to_stderr()

        assert closed == [99]

    def test_redirect_stdout_to_stderr_returns_stream_on_success(self, monkeypatch):
        calls: list[tuple[str, object]] = []
        sentinel = io.BytesIO()
        with monkeypatch.context() as scoped:
            scoped.setattr(os, "dup", lambda fd: calls.append(("dup", fd)) or 99)
            scoped.setattr(os, "dup2", lambda src, dst: calls.append(("dup2", (src, dst))))
            scoped.setattr(os, "fdopen", lambda fd, mode: calls.append(("fdopen", (fd, mode))) or sentinel)
            stream = _process._redirect_stdout_to_stderr()

        assert stream is sentinel
        assert calls == [("dup", 1), ("dup2", (2, 1)), ("fdopen", (99, "wb"))]

    def test_run_frozen_worker_handles_missing_stdin_and_in_process_dispatch(self, monkeypatch):
        monkeypatch.setattr(sys, "stdin", None)
        with pytest.raises(SystemExit, match="no binary stdin stream"):
            _process._run_frozen_worker()

        class FakeStdin:
            def __init__(self, data: bytes) -> None:
                self.buffer = io.BytesIO(data)

        seen: list[object] = []
        payload = json.dumps({"sys_path": list(sys.path), "worker": "pages", "request": {"ok": True}}).encode()
        monkeypatch.setattr(sys, "stdin", FakeStdin(payload))
        monkeypatch.setattr(_process, "_redirect_stdout_to_stderr", lambda: io.BytesIO())
        monkeypatch.setattr(_process, "_install_sys_path", lambda _path: None)
        monkeypatch.setattr(_process, "_load_worker_entry", lambda _name, _rep: lambda req, rep: seen.append(req))

        _process._run_frozen_worker()
        assert seen == [{"ok": True}]

        monkeypatch.setattr(sys, "stdin", FakeStdin(payload))
        monkeypatch.setattr(_process, "_load_worker_entry", lambda _name, _rep: None)
        _process._run_frozen_worker()

    def test_freeze_support_invokes_frozen_worker_and_exits_zero(self, monkeypatch):
        ran: list[bool] = []
        monkeypatch.setattr(_process, "_FROZEN_SUPPORT", threading.Event())
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "argv", ["app", _process.WORKER_FLAG])
        monkeypatch.setattr(_process, "_run_frozen_worker", lambda: ran.append(True))

        with pytest.raises(SystemExit) as caught:
            _process.freeze_support()

        assert caught.value.code == 0
        assert ran == [True]
        assert _process._FROZEN_SUPPORT.is_set()

    def test_frozen_worker_dispatches_allowlisted_worker_redirects_stdout_and_exits_zero(self, tmp_path):
        payload = _process.request_payload("pages", {"page": 7}, _process.MAX_REQUEST_BYTES)

        done = run_frozen_worker(tmp_path, FROZEN_WORKER_ECHO_SCRIPT, payload)

        assert done.returncode == 0, done.stderr
        assert _process.read_message(io.BytesIO(done.stdout), 1024) == b"{'page': 7}"
        assert b"frozen noise on stdout" in done.stderr
        assert b"MAIN_RAN" not in done.stdout and b"MAIN_RAN" not in done.stderr

    def test_frozen_worker_refuses_request_over_bound_before_main(self, tmp_path):
        done = run_frozen_worker(tmp_path, FROZEN_WORKER_SCRIPT, b" " * (_process.MAX_REQUEST_BYTES + 1))

        assert done.returncode == 1
        assert b"the worker request is over 1048576 bytes" in done.stderr
        assert b"MAIN_RAN" not in done.stdout and b"MAIN_RAN" not in done.stderr

    @pytest.mark.parametrize(
        ("stdin", "expected"),
        [
            pytest.param(b"{not-json", b"not valid JSON", id="bad-json"),
            pytest.param(b"[]", b"must be a JSON object", id="non-dict"),
            pytest.param(
                json.dumps({"sys_path": "/not/a/list", "worker": "pages", "request": {}}).encode(),
                b"the worker request's sys_path is not a list of str",
                id="bad-sys-path-str",
            ),
            pytest.param(
                json.dumps({"sys_path": ["/ok", 3], "worker": "pages", "request": {}}).encode(),
                b"the worker request's sys_path is not a list of str",
                id="bad-sys-path-int",
            ),
            pytest.param(
                json.dumps({"sys_path": [], "worker": "os.system", "request": {}}).encode(),
                b"the worker request names no worker of ('figures', 'pages', 'render')",
                id="disallowed-worker",
            ),
            pytest.param(
                json.dumps({"sys_path": [], "request": {}}).encode(),
                b"the worker request names no worker of ('figures', 'pages', 'render')",
                id="missing-worker",
            ),
        ],
    )
    def test_frozen_worker_refuses_invalid_requests_without_running_main(self, tmp_path, stdin, expected):
        done = run_frozen_worker(tmp_path, FROZEN_WORKER_SCRIPT, stdin)

        assert done.returncode == 1
        assert expected in done.stderr
        assert b"MAIN_RAN" not in done.stdout and b"MAIN_RAN" not in done.stderr

    def test_frozen_worker_reports_missing_pdf_extra_as_error_reply(self, tmp_path):
        payload = _process.request_payload("pages", {}, _process.MAX_REQUEST_BYTES)

        done = run_frozen_worker(tmp_path, FROZEN_MISSING_EXTRA_SCRIPT, payload)

        assert done.returncode == 0, done.stderr
        reply = _process.read_message(io.BytesIO(done.stdout), 1 << 20)
        assert reply is not None and reply[:1] == _process.ERROR
        error = _process.read_error(reply[1:], "frozen worker")
        assert isinstance(error, ImportError)
        assert "pypdfium2" in str(error)
        assert b"MAIN_RAN" not in done.stdout and b"MAIN_RAN" not in done.stderr
