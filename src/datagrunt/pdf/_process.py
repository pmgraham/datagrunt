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
- The parent starts the worker with stdin and stdout piped and stderr to a
  file (:func:`start_worker`), whose last 4 KiB (:func:`stderr_tail`) end
  any failure without a reply.
"""

import json
import mmap
import os
import struct
import subprocess
import sys
from types import MappingProxyType
from typing import IO

__all__ = [
    "BOOTSTRAP",
    "LENGTH",
    "MAX_REQUEST_BYTES",
    "REAP_TIMEOUT_S",
    "WORKERS",
    "MessageTooLong",
    "NoInterpreter",
    "close_pipes",
    "read_message",
    "reaped",
    "request_payload",
    "send_message",
    "start_worker",
    "stderr_tail",
    "worker_command",
]

REAP_TIMEOUT_S = 5.0  # a child that finished, or was killed, is reaped within this
_MAX_STDERR_TAIL = 4096

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


def start_worker(command: list[str], errors: IO[bytes]) -> "subprocess.Popen[bytes]":
    """Start the worker: stdin and stdout piped to this process, stderr to the file ``errors``."""
    return subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)


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


def stderr_tail(errors: IO[bytes]) -> str:
    """The last 4 KiB the child wrote to its stderr (the file ``errors``), as text."""
    end = errors.seek(0, os.SEEK_END)
    errors.seek(max(0, end - _MAX_STDERR_TAIL))
    return errors.read(_MAX_STDERR_TAIL).decode("utf-8", errors="backslashreplace").strip()
