"""The v5 source-file check (#371): a PDF path, checked at the boundary (spec invariant 1).

Internal: shared by A7b's ``PDFWriter`` and A8b's ``PDFReader``, which call
:func:`checked_pdf_file` at construction, so a bad path fails before any
work and both classes refuse the same paths the same way:

- a path that does not exist (or runs through a file) raises
  ``FileNotFoundError``;
- a path that is not a regular file (a directory, a named pipe, a device)
  raises ``ValueError``, without being opened (opening a named pipe would
  block);
- a file without a PDF header where pdfium looks for one raises
  ``ValueError``.

The header is pdfium's: its parser looks for the four bytes ``%PDF``
(case-sensitive; the PDF spec's ``%PDF-`` but for its dash, which pdfium
does not require) starting at any offset from 0 to :data:`_MAX_HEADER_OFFSET`
(1024), so a file may lead with up to 1024 bytes of anything. Measured with
pypdfium2 here, and pinned by the tests against pdfium itself: a header
after 1024 bytes of junk opens, after 1025 it does not; ``%PDF`` and
``%PDFX`` open, ``%pdf-`` does not. The check accepts exactly those files,
so it never refuses a PDF pdfium reads. It reads at most
:data:`_HEADER_WINDOW` (1028) bytes: a header starting at offset 1024 ends
at byte 1028.

The file is opened once (``O_NONBLOCK``: a named pipe swapped in after the
``stat`` cannot block the open), checked again through ``fstat`` of that
descriptor to be a regular file, read, and closed on every path. Symlinks
are followed: a link to a PDF is that PDF.

Errors name the argument (``name``) and the path. No password reaches this
module, so none can reach its messages.
"""

import errno
import os
import stat
from pathlib import Path

from datagrunt.pdf._validate import non_blank_str

__all__ = ["checked_pdf_file"]

_HEADER = b"%PDF"
# The last offset pdfium's parser looks for the header at (see the module notes).
_MAX_HEADER_OFFSET = 1024
_HEADER_WINDOW = _MAX_HEADER_OFFSET + len(_HEADER)
# Never blocking on a named pipe (O_NONBLOCK, which a regular file ignores), as bytes (O_BINARY, Windows). Each
# flag is 0 where it does not exist.
_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)


def checked_pdf_file(path: Path, name: str) -> Path:
    """``path``, if it is an existing regular file holding pdfium's PDF header (see the module notes).

    Args:
        path: The file to check.
        name: The argument ``path`` came from (``"source"``), named in every error.

    Returns:
        ``path``, as it is.

    Raises:
        TypeError: If ``path`` is not a Path, or ``name`` not a str.
        ValueError: If ``name`` is blank; ``path`` is not a regular file, or holds no
            ``%PDF`` header starting in its first 1025 bytes.
        FileNotFoundError: If ``path`` does not exist (or runs through a file).
        OSError: If ``path`` cannot be stat'ed, opened or read (``PermissionError``, say).
    """
    if not isinstance(path, Path):
        raise TypeError(f"path must be a Path, got {type(path).__name__}")
    non_blank_str("name", name)
    try:
        status = path.stat()
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise FileNotFoundError(errno.ENOENT, f"{name} does not exist", str(path)) from exc
    _require_regular(status, name, path)
    if _HEADER not in _head(path, name):
        raise ValueError(
            f"{name} is not a PDF: no %PDF header starts in its first {_MAX_HEADER_OFFSET + 1} bytes: {path}"
        )
    return path


def _require_regular(status: os.stat_result, name: str, path: Path) -> None:
    if not stat.S_ISREG(status.st_mode):
        raise ValueError(f"{name} is not a regular file: {path}")


def _head(path: Path, name: str) -> bytes:
    """The first :data:`_HEADER_WINDOW` bytes of the regular file at ``path`` (fewer if it is shorter)."""
    descriptor = os.open(path, _OPEN_FLAGS)
    try:
        _require_regular(os.fstat(descriptor), name, path)
        return _read_at_most(descriptor, _HEADER_WINDOW)
    finally:
        os.close(descriptor)


def _read_at_most(descriptor: int, size: int) -> bytes:
    """Up to ``size`` bytes from ``descriptor``: reads are continued until ``size`` or the end of the file."""
    chunks: list[bytes] = []
    remaining = size
    for _ in range(size):  # each read that is not the end returns at least one byte
        block = os.read(descriptor, remaining)
        if not block:
            break
        chunks.append(block)
        remaining -= len(block)
        if not remaining:
            break
    return b"".join(chunks)
