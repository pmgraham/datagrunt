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

:class:`SourceIdentity` is which file a path named, from one ``stat``: the
pipeline takes it when it hashes the source (A8a), and every later opening
of the path is checked against it (the page workers', the bundle's, a page
image's draws).

The source is hashed the one way, by the pipeline, the bundle and a page
image's draw: opened once, as :func:`checked_pdf_file` opens it
(:func:`_opened_source`: ``O_NONBLOCK``, then ``fstat`` of the descriptor
must show a regular file, else ``ValueError``), its identity taken
through that descriptor, then its first ``st_size`` bytes streamed through
SHA-256 in reads of at most :data:`_HASH_CHUNK_BYTES`
(:func:`_stream_sha256`), a file cut short meanwhile raising
``PDFSourceChangedError``. A page image's loaders are bound to the file
their document was extracted from by its identity and its sha256 together
(:func:`check_binding`): the identity is the fast check, the sha256 the
one that decides.
"""

import errno
import hashlib
import io
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from datagrunt.pdf._validate import int_at_least, non_blank_str, sha256_hex
from datagrunt.pdf.exceptions import PDFSourceChangedError

__all__ = ["SourceIdentity", "check_binding", "checked_pdf_file"]

_HEADER = b"%PDF"
# The last offset pdfium's parser looks for the header at (see the module notes).
_MAX_HEADER_OFFSET = 1024
_HEADER_WINDOW = _MAX_HEADER_OFFSET + len(_HEADER)
# Never blocking on a named pipe (O_NONBLOCK, which a regular file ignores), as bytes (O_BINARY, Windows). Each
# flag is 0 where it does not exist.
_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
# The source is hashed in reads of at most this many bytes.
_HASH_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceIdentity:
    """The identity of the file a document's hash was read through, from one ``fstat`` (see the module notes).

    Attributes:
        device: ``st_dev``.
        inode: ``st_ino``: with ``device``, which file the path named.
        size: ``st_size``, in bytes.
        mtime_ns: ``st_mtime_ns``.
    """

    device: int
    inode: int
    size: int
    mtime_ns: int

    def __post_init__(self) -> None:
        for name in ("device", "inode", "size", "mtime_ns"):
            int_at_least(name, getattr(self, name), 0)

    @classmethod
    def of(cls, status: os.stat_result) -> "SourceIdentity":
        """The identity in a stat result."""
        return cls(device=status.st_dev, inode=status.st_ino, size=status.st_size, mtime_ns=status.st_mtime_ns)

    def changes(self, other: "SourceIdentity") -> tuple[str, ...]:
        """What differs in ``other``, in words: its inode, size and mtime, in that order; empty when nothing does."""
        found: list[str] = []
        if (self.device, self.inode) != (other.device, other.inode):
            found.append(f"inode {self.device}:{self.inode} -> {other.device}:{other.inode}")
        if self.size != other.size:
            found.append(f"size {self.size} -> {other.size} bytes")
        if self.mtime_ns != other.mtime_ns:
            found.append(f"mtime {self.mtime_ns} -> {other.mtime_ns} ns")
        return tuple(found)


def check_binding(identity: object, sha256: object) -> None:
    """Require the file a document was extracted from as its identity and its sha256 together, or neither (None).

    Raises:
        TypeError: If ``identity`` is not a SourceIdentity or None, or ``sha256`` not a str or None.
        ValueError: If ``sha256`` is not 64 lowercase hex characters, or only one of the two is given.
    """
    if identity is not None and not isinstance(identity, SourceIdentity):
        raise TypeError(f"identity must be a SourceIdentity or None, got {type(identity).__name__}")
    if sha256 is not None:
        sha256_hex("sha256", sha256)
    if (identity is None) != (sha256 is None):
        given = "sha256" if identity is None else "identity"
        raise ValueError(f"identity and sha256 must both be given or both be None, got only the {given}")


def _opened_source(path: Path) -> BinaryIO:
    """``path`` opened for reading, once: the file the hash is read through and the identity taken from.

    Opened as :func:`checked_pdf_file` opens it: with :data:`_OPEN_FLAGS`
    (``O_NONBLOCK``: a named pipe at the path, however late it was put
    there, cannot block the open), then checked through ``fstat`` of that
    descriptor to be a regular file. The descriptor is closed on every path
    but the one that returns it, in a stream that closes it.

    Raises:
        FileNotFoundError: If ``path`` does not exist (or runs through a file).
        ValueError: If ``path`` is not a regular file (a directory, a named
            pipe, a device), as :func:`checked_pdf_file` raises it.
        OSError: If ``path`` cannot be opened for another reason
            (``PermissionError``, say).
    """
    try:
        descriptor = os.open(path, _OPEN_FLAGS)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise FileNotFoundError(errno.ENOENT, "not an existing file", str(path)) from exc
    try:
        _require_regular(os.fstat(descriptor), "source", path)
        raw = io.FileIO(descriptor, "r")  # a FileIO that fails to start leaves the descriptor to this close
    except BaseException:
        os.close(descriptor)
        raise
    return io.BufferedReader(raw)


def _stream_sha256(stream: BinaryIO, size: int, path: Path) -> str:
    """The SHA-256 of the first ``size`` bytes of ``stream``, read in chunks of :data:`_HASH_CHUNK_BYTES`.

    Raises:
        PDFSourceChangedError: If fewer than ``size`` bytes can be read (the file was cut short), naming ``path``.
    """
    digest = hashlib.sha256()
    remaining = size
    for _ in range(-(-size // _HASH_CHUNK_BYTES)):
        block = _read_chunk(stream, min(_HASH_CHUNK_BYTES, remaining))
        if not block:
            break
        digest.update(block)
        remaining -= len(block)
    if remaining:
        raise PDFSourceChangedError(
            f"{path} changed while it was hashed: {size - remaining} of its {size} bytes could be read"
        )
    return digest.hexdigest()


def _read_chunk(stream: BinaryIO, size: int) -> bytes:
    """The next at most ``size`` bytes of ``stream`` (fewer only at its end)."""
    return stream.read(size)


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
