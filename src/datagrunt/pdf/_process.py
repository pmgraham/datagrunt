"""Helpers for the child processes the v5 PDF package starts (#369 A5a, #372 A8a): Python workers and tesseract.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Each wait is
bounded, and each read of a child's stderr too.

The Python worker launcher, shared by the render worker (``render.py``) and
the page workers (``pipeline.py``):

- The worker is a plain subprocess, ``python -I -c BOOTSTRAP``
  (:func:`worker_command`): it never imports the caller's main module, so a
  caller's script runs once, however it is written. ``-I`` (isolated mode)
  keeps the caller's working directory and the ``PYTHON*`` environment
  (``PYTHONPATH``, ``PYTHONSTARTUP``, ...) out of the worker, so nothing
  from either runs before the request is read.
- The request is one JSON message on the worker's stdin
  (:func:`request_payload`), never in its argv or environment, within
  :data:`MAX_REQUEST_BYTES`: the caller's exact ``sys.path`` (installed by the
  bootstrap, so the worker imports the same datagrunt and pdfium the caller
  has, wherever they were found), the worker's key and its request.
- The key picks the worker from :data:`WORKERS`, a fixed allowlist written
  into the bootstrap's text: a request never names a module or a function.
  An unknown key is refused before anything of the caller's ``sys.path`` is
  imported.
- The worker keeps its real stdout for replies and points file descriptor
  1 at its stderr first, so a stray print cannot corrupt a reply. Replies are
  length-prefixed messages (:func:`send_message`, :func:`read_message`),
  each read within a bound its reader sets.
- The parent starts the worker with stdin, stdout and stderr piped
  (:func:`start_worker`). Its stderr is drained by a thread of its own into
  a ring that keeps the last 4 KiB (:class:`StderrTail`), which ends any
  failure without a reply.

The stderr ring writes nothing to disk (no temporary file: the reader reads
a PDF with zero filesystem writes, #372) and holds at most
:data:`_MAX_STDERR_TAIL` bytes, however much the child writes. tesseract's
calls (:mod:`datagrunt.pdf.ocr`) drain their stderr the same way
(:func:`drained`). The drain thread reads the pipe until its end, which comes
when the child has exited (each caller bounds its child's life by a timer
that kills it); :meth:`StderrTail.finish` joins it within the bound it is
given, on every path (a reply, a failure, a kill, a timeout). A drain still
running then (something else holds the pipe open) is left to end on its own
and close the pipe itself, and the tail says so: the parent never closes a
pipe another thread is reading.
"""

import json
import mmap
import os
import struct
import subprocess
import sys
import threading
from types import MappingProxyType
from typing import IO

__all__ = [
    "BOOTSTRAP",
    "LENGTH",
    "MAX_REQUEST_BYTES",
    "REAP_TIMEOUT_S",
    "STDERR_THREAD",
    "WORKERS",
    "MessageTooLong",
    "NoInterpreter",
    "StderrTail",
    "close_pipes",
    "drained",
    "read_message",
    "reaped",
    "request_payload",
    "send_message",
    "start_worker",
    "worker_command",
]

REAP_TIMEOUT_S = 5.0  # a child that finished, or was killed, is reaped within this
_MAX_STDERR_TAIL = 4096  # the bytes of a child's stderr kept: its last
_STDERR_READ_BYTES = 4096  # each read of a child's stderr
STDERR_THREAD = "datagrunt-stderr"  # the name of each stderr drain thread

# The workers a request may name, by key: (module, function). The function takes the request and the replies stream.
WORKERS = MappingProxyType(
    {"render": ("datagrunt.pdf.render", "_worker_main"), "pages": ("datagrunt.pdf.pipeline", "_worker_main")}
)
MAX_REQUEST_BYTES = 1 << 20  # the JSON request on a worker's stdin
LENGTH = struct.Struct("<Q")  # each reply's length, before it

# The worker's program: ``python -I -c BOOTSTRAP``. It keeps the real stdout
# for replies and points file descriptor 1 at stderr, reads one bounded JSON
# message from stdin, refuses a worker key outside the allowlist written here,
# installs the caller's sys.path, and hands the request to the named worker.
# Fixed text: nothing from the caller or the PDF is in it.
BOOTSTRAP = f"""
import importlib, json, os, sys
replies = os.fdopen(os.dup(1), "wb")
os.dup2(2, 1)
data = sys.stdin.buffer.read({MAX_REQUEST_BYTES} + 1)
if len(data) > {MAX_REQUEST_BYTES}:
    raise SystemExit("the worker request is over {MAX_REQUEST_BYTES} bytes")
message = json.loads(data)
path = message["sys_path"]
if not isinstance(path, list) or not all(isinstance(entry, str) for entry in path):
    raise SystemExit("the worker request's sys_path is not a list of str")
workers = {dict(WORKERS)!r}
name = message.get("worker")
if not isinstance(name, str) or name not in workers:
    raise SystemExit("the worker request names no worker of {tuple(WORKERS)!r}")
sys.path[:] = path
module, function = workers[name]
getattr(importlib.import_module(module), function)(message.get("request"), replies)
"""


class NoInterpreter(Exception):
    """There is no Python interpreter to start a worker with: nothing is started."""


class MessageTooLong(Exception):
    """A reply's length is over its reader's bound: it is not read."""


def worker_command(label: str) -> list[str]:
    """The worker's command line, which holds nothing of the request; ``label`` names the worker in an error.

    Reads :data:`BOOTSTRAP` when called.

    Raises:
        NoInterpreter: In a frozen application, or when ``sys.executable`` is empty.
    """
    if getattr(sys, "frozen", False):
        raise NoInterpreter(f"a frozen application has no Python interpreter to start {label} with")
    if not sys.executable:
        raise NoInterpreter(f"sys.executable is empty: there is no Python interpreter for {label}")
    return [sys.executable, "-I", "-c", BOOTSTRAP]


def request_payload(worker: str, request: object, bound: int) -> bytes:
    """The JSON a worker reads from its stdin: the caller's exact sys.path, the worker's key and its request.

    Raises:
        ValueError: If ``worker`` is not a key of :data:`WORKERS`, or the
            payload is over ``bound`` bytes.
    """
    if worker not in WORKERS:
        raise ValueError(f"worker must be one of {tuple(WORKERS)!r}, got {worker!r}")
    payload = json.dumps({"sys_path": list(sys.path), "worker": worker, "request": request}).encode("ascii")
    if len(payload) > bound:
        raise ValueError(f"the {worker} request is {len(payload)} bytes, over the {bound}-byte bound")
    return payload


def start_worker(command: list[str]) -> tuple["subprocess.Popen[bytes]", "StderrTail"]:
    """Start the worker, its stdin, stdout and stderr piped to this process; it and its stderr's drain.

    Raises:
        OSError: If the OS refuses to start it.
        RuntimeError: If its stderr's drain thread cannot start (it is then killed and reaped).
    """
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return child, drained(child)


def drained(child: "subprocess.Popen[bytes]") -> "StderrTail":
    """A :class:`StderrTail` draining ``child``'s stderr pipe.

    Raises:
        ValueError: If ``child``'s stderr is not a pipe.
        RuntimeError: If the drain thread cannot start: ``child`` is then
            killed and reaped, and its pipes closed, before this raises.
    """
    stream = child.stderr
    if stream is None:
        raise ValueError("the child's stderr must be a pipe")
    try:
        return StderrTail(stream)
    except BaseException:  # no drain runs: this process kills and reaps the child, and closes its pipes
        child.kill()
        reaped(child, REAP_TIMEOUT_S)
        close_pipes(child)
        stream.close()
        raise


def send_message(replies: IO[bytes], message: bytes | mmap.mmap) -> None:
    """Write one message after its 8-byte length, and flush it."""
    replies.write(LENGTH.pack(len(message)))
    replies.write(message)
    replies.flush()


def read_message(stream: IO[bytes], bound: int) -> bytes | None:
    """One length-prefixed message from ``stream``; None at its end or cut short.

    Raises:
        MessageTooLong: If its length is over ``bound``; the message is not read.
    """
    header = stream.read(LENGTH.size)
    if len(header) != LENGTH.size:
        return None
    (length,) = LENGTH.unpack(header)
    if length > bound:
        raise MessageTooLong(f"a {length}-byte message, over the {bound}-byte bound")
    message = stream.read(length)
    return message if len(message) == length else None


def close_pipes(child: "subprocess.Popen[bytes]") -> None:
    """Close the parent's ends of the child's stdin and stdout."""
    for pipe in (child.stdin, child.stdout):
        if pipe is not None:
            pipe.close()


def reaped(child: "subprocess.Popen[bytes]", wait_s: float) -> int | None:
    """Give the child ``wait_s`` seconds to exit, kill it if it has not, reap it; its exit code (None: unreaped)."""
    try:
        return child.wait(wait_s)
    except subprocess.TimeoutExpired:
        child.kill()
    try:
        return child.wait(REAP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return None


class StderrTail:
    """A child's stderr, drained from its pipe by a thread of its own that keeps the last 4 KiB (see the module notes).

    The thread starts at construction. It reads the pipe until its end, keeps
    at most :data:`_MAX_STDERR_TAIL` bytes (the last), and closes the pipe
    when it ends: nothing else closes it.
    """

    __slots__ = ("_error", "_kept", "_lock", "_thread")

    def __init__(self, stream: IO[bytes]) -> None:
        self._kept = bytearray()
        self._lock = threading.Lock()
        self._error: OSError | None = None
        self._thread = threading.Thread(target=self._drain, args=(stream,), name=STDERR_THREAD, daemon=True)
        self._thread.start()

    def _drain(self, stream: IO[bytes]) -> None:
        """The thread: read ``stream`` to its end, keeping its last bytes; a read that fails is recorded; then close it.

        Each read returns as soon as the child has written anything, up to
        :data:`_STDERR_READ_BYTES`; the loop ends at the pipe's end, which
        comes once the child has exited (its caller bounds its life).
        """
        try:
            for chunk in iter(lambda: os.read(stream.fileno(), _STDERR_READ_BYTES), b""):
                self._keep(chunk)
        except OSError as exc:  # recorded, and said by finish: never swallowed
            self._error = exc
        finally:
            stream.close()

    def _keep(self, chunk: bytes) -> None:
        """Append ``chunk``, then drop what is before the last :data:`_MAX_STDERR_TAIL` bytes."""
        with self._lock:
            self._kept += chunk
            del self._kept[: max(0, len(self._kept) - _MAX_STDERR_TAIL)]

    def finish(self, wait_s: float) -> str:
        """Wait up to ``wait_s`` seconds for the drain to end, then the last 4 KiB the child wrote, as text.

        Called on every path that ends the child, once it is reaped; it may
        be called again. A drain that has not ended within ``wait_s`` (the
        pipe is still open) is left to end on its own, and a read that
        failed is said: either is noted after the text, in parentheses.
        """
        self._thread.join(wait_s)
        with self._lock:
            text = bytes(self._kept).decode("utf-8", errors="backslashreplace").strip()
        if self._thread.is_alive():
            return _noted(text, f"its stderr was still open after {wait_s:g} s")
        if self._error is not None:
            return _noted(text, f"its stderr could not be read to its end: {self._error}")
        return text


def _noted(text: str, note: str) -> str:
    """``text``, then ``note`` in parentheses."""
    return f"{text} ({note})" if text else f"({note})"
