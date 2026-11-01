"""Helpers for the child processes the v5 PDF package starts (#369, A5a): the render worker and tesseract.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Each wait is
bounded, and each read of a child's stderr too.
"""

import os
import subprocess
from typing import IO

__all__ = ["REAP_TIMEOUT_S", "close_pipes", "reaped", "stderr_tail"]

REAP_TIMEOUT_S = 5.0  # a child that finished, or was killed, is reaped within this
_MAX_STDERR_TAIL = 4096


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
