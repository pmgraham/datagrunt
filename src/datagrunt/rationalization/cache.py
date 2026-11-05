"""The response cache: the request key, in-run deduplication and the persistent store (plan §4.4, R9, R10).

A request is identified by :func:`request_key`: the SHA-256 hex digest of the canonical JSON
(:func:`~datagrunt.rationalization._canonical.canonical_json`) of ``{"format": CACHE_FORMAT, "provider_kind",
"model_id", "generation_params", "envelope": Envelope.key_material(), "provider_schema", "schema_version"}``. The
envelope's key material holds its nonce-free components (the envelope format, the system rules, the caller's
prompt, the directive and user layout, the context, the payload and the header ids), so the key never depends on
a request's nonce, changes with any part of what is sent, and is the same in every process, whatever its
``PYTHONHASHSEED`` or its dicts' insertion order. ``generation_params`` and ``provider_schema`` must be exactly
JSON (:func:`~datagrunt.rationalization._canonical.canonical_copy`): canonical JSON would write ``{1: "x"}`` as
``{"1": "x"}`` and a tuple as a list, so two different requests would share a key; the key fails closed instead.
Any change to this composition must change :data:`CACHE_FORMAT`.

Two stores take the same calls: ``get(key) -> CacheLookup`` and ``put(key, response, meta) -> bool``. A
:class:`CacheLookup` is a hit (``response`` set), a miss, or a corrupt miss (``corrupt=True`` with a
:data:`CorruptReason` code, never document text). A ``put`` that returns ``False`` wrote nothing; the caller
reports it (nothing is silent).

- :class:`MemoryCache` deduplicates within one run: a dict behind a lock, holding each response as canonical JSON
  and handing out a fresh copy on each ``get``, so no caller changes what another reads. It never touches the
  filesystem and holds at most :data:`MEMORY_MAX_ENTRIES` keys: a ``put`` of a new key past that returns
  ``False`` (overwriting a held key still works).
- :class:`FileCache` is the persistent store, used only when the caller passes ``cache_dir`` (R9: no default
  location). The rules:

  1. **Layout.** One entry per key at ``<cache_dir>/<key[:2]>/<key>.json``. A key must be 64 lowercase hex
     characters, which is also what keeps every path inside ``cache_dir``.
  2. **Directories.** A missing ``cache_dir`` is created with its parents, the leaf ``0o700`` (``chmod`` after
     ``mkdir``, past the umask); an existing one is never re-permissioned, and one that exists but is not a
     directory raises. Sub-directories are created ``0o700`` the same way. A sub-directory path that is a symlink
     or not a directory raises: a tampered cache fails loud. Concurrent creation is fine (``FileExistsError``).
     The ``chmod`` after ``mkdir`` goes through an ``O_DIRECTORY | O_NOFOLLOW`` descriptor of the new directory on
     POSIX (by path only when the umask took the owner's own read bit), so a symlink swapped in between never has
     its target re-permissioned. Each read and write first ``lstat``-s the sub-directory, and that identity (device
     and inode) is what it binds to (6).
  3. **Writes.** The entry is canonical JSON, ``{"format": CACHE_FORMAT, "key": key, "response":
     ProviderResponse.to_json(), "meta": CacheMeta.to_json()}``, at most :data:`ENTRY_MAX_BYTES` (8 MiB) in UTF-8;
     a larger one is not written (``put`` returns ``False``). It goes to a new temp file in the sub-directory,
     named ``.<random>.tmp`` (hidden, and never matched by a ``*.json`` glob, so a temp file left by a killed
     writer never reaches DuckDB's ``read_json('<cache_dir>/*/*.json')``), set ``0o600`` (on POSIX, past the
     umask), written, flushed and ``fsync``-ed, then ``os.replace``-d onto the entry, and the directory is
     ``fsync``-ed where the OS allows it (not on Windows; a filesystem refusing it with ``EINVAL`` or ``ENOTSUP``
     is skipped). Any failure removes the temp file; an ``OSError`` raises
     :class:`~datagrunt.rationalization.RationalizationError` (chained), an interrupt propagates as is.
  4. **Reads.** The entry is opened ``O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC`` (each where the OS has it;
     without ``O_NOFOLLOW`` the path is checked with ``lstat`` first), so a symlink is refused and a planted FIFO
     never blocks. It must be a regular file (``fstat``) of at most 8 MiB, read up to 8 MiB plus one byte, strict
     UTF-8 JSON with no NaN or infinity and no repeated key, an object of exactly the four fields, of this
     ``format`` and this ``key``, whose ``meta`` and ``response`` check. A missing entry is a miss. Anything else
     wrong with the entry is a corrupt miss, never an error (no value in it, a number too large for a float
     included, makes ``get`` raise): the runner emits ``cache_corrupt`` and the next ``put`` overwrites it (a
     directory planted at the entry path cannot be, and that ``put`` raises). A ``get`` never writes.
  5. **Concurrency.** Safe across threads and processes without locks: a reader sees a whole old entry or a whole
     new one (atomic replace). Two processes missing one key both call the model; the last write wins, and each
     run stays self-consistent.
  6. **A swapped sub-directory.** Where the OS has ``dir_fd`` (POSIX), a read or a write opens the sub-directory
     itself, ``O_DIRECTORY | O_NOFOLLOW``, requires the inode it checked, and works only at that descriptor: the
     temp file is made there ``O_CREAT | O_EXCL | O_NOFOLLOW``, renamed onto the entry there, and removed there on
     failure. A sub-directory swapped for a symlink (or anything else) between the check and the write therefore
     can never redirect a write or a read out of ``cache_dir``. Without ``dir_fd`` (Windows) the cache works by
     path, and checks the identity again once the temp file exists (a write) or after the read. Either way, a
     ``put`` whose sub-directory is not the one it checked once the entry is written raises: a swap fails loud.

An entry's ``meta`` (:class:`CacheMeta`) is audit data for whoever reads the directory (DuckDB's ``read_json``
reads it whole): ids, names, hashes, the latency and the time, never document text beyond ``doc_id`` and
``unit_id``, which are ids. The prompt appears only as its SHA-256. The ``response`` does hold document data (the
model's answer and raw text), which is why entries are ``0o600`` in ``0o700`` directories (R9).

Bounds: 8 MiB per entry read or written, :data:`MEMORY_MAX_ENTRIES` in memory; no loop runs past its input and
nothing recurses (the JSON coder aside, bounded by the recursion limit). The 8 MiB bounds the bytes read, not the
memory their parse takes: an entry dense with small objects or arrays can take a few hundred MiB while it is parsed
and checked (about 400 MiB measured for 8 MiB of empty objects), then frees it. The module imports only the standard
library and its sibling modules (the envelope's for :class:`~datagrunt.rationalization.envelope.Envelope`): no
pydantic, no httpx, nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

import contextlib
import errno
import json
import math
import os
import re
import secrets
import stat
import sys
import tempfile
import threading
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, NoReturn, TypeAlias, get_args

from datagrunt.rationalization._canonical import canonical_copy, canonical_json, sha256_hex
from datagrunt.rationalization.envelope import Envelope
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.wire import ProviderResponse

CACHE_FORMAT = "datagrunt.rationalize.cache/1"
"""The key's and the entry's version: change it with any change to the key's composition or the entry's layout."""

ENTRY_MAX_BYTES = 8 * 1024 * 1024
"""The most UTF-8 bytes one cache entry may take, written or read (plan §5)."""

MEMORY_MAX_ENTRIES = 100_000
"""The most keys a :class:`MemoryCache` holds."""

CorruptReason: TypeAlias = Literal[
    "symlink",
    "not_regular_file",
    "unreadable",
    "too_large",
    "not_json",
    "not_an_entry",
    "wrong_format",
    "wrong_key",
    "invalid_meta",
    "invalid_response",
]
"""Why an entry is corrupt: the entry path is a symlink, is no regular file (a FIFO, a device, a directory), cannot
be opened or read, passes 8 MiB, is not strict UTF-8 JSON, is not an object of exactly the four fields, has another
format or key, or holds a ``meta`` or ``response`` that does not check."""

CORRUPT_REASONS: tuple[CorruptReason, ...] = get_args(CorruptReason)

_KEY = re.compile(r"[0-9a-f]{64}")
_ENTRY_FIELDS = frozenset({"format", "key", "response", "meta"})
# A temp file is ".<random>.tmp": hidden, and never matched by a "*.json" glob (DuckDB's read_json over the cache).
_TEMP_PREFIX = "."
_TEMP_SUFFIX = ".tmp"
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | _O_CLOEXEC | getattr(os, "O_BINARY", 0)
_CREATE_FLAGS = os.O_CREAT | os.O_EXCL | os.O_WRONLY | _O_NOFOLLOW | _O_CLOEXEC
# Reads and writes go through the checked sub-directory's descriptor wherever the OS has dir_fd for opening,
# renaming (os.replace uses the same renameat) and unlinking, with O_NOFOLLOW and O_DIRECTORY; elsewhere (Windows)
# by path, the directory's identity checked again.
_DIR_FD = bool(_O_NOFOLLOW and _O_DIRECTORY) and {os.open, os.rename, os.unlink} <= os.supports_dir_fd
# O_NOFOLLOW refuses a symlink with ELOOP (EMLINK on FreeBSD).
_SYMLINK_ERRNOS = frozenset({errno.ELOOP, errno.EMLINK})
# Opening a symlink or a file with O_DIRECTORY | O_NOFOLLOW fails with one of these, by OS.
_NOT_A_DIRECTORY_ERRNOS = frozenset({errno.ENOTDIR, errno.ELOOP, errno.EMLINK})
# What a filesystem that cannot fsync a directory answers.
_FSYNC_UNSUPPORTED = frozenset({errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP})


# ---------------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------------


def _check_text(name: str, value: object) -> str:
    """Require a non-blank ``str`` that UTF-8 can encode."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"{name} must be non-blank")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"{name} must be valid Unicode text: it holds a lone surrogate at {error.start}") from error
    return value


def _check_hex(name: str, value: object) -> str:
    """Require 64 lowercase hex characters (a SHA-256 digest, or a key)."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    if _KEY.fullmatch(value) is None:
        raise ValueError(f"{name} must be 64 lowercase hex characters (a SHA-256 hex digest)")
    return value


def _check_key(key: object) -> str:
    return _check_hex("key", key)


def _check_json_object(name: str, value: object) -> dict[str, Any]:
    """Require a mapping that is exactly JSON (``canonical_copy``), so no two requests share a key; return its copy.

    Raises:
        TypeError: If ``value`` is not a ``Mapping``.
        RationalizationError: If it is not canonical JSON, or canonical JSON would fold it (a key that is not a
            ``str``, a tuple).
    """
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a JSON object (a Mapping), got {type(value).__name__}")
    copy: dict[str, Any] = canonical_copy(dict(value), subject=name)
    return copy


def _check_stored(response: object, meta: object) -> None:
    if not isinstance(response, ProviderResponse):
        raise TypeError(f"response must be a ProviderResponse, got {type(response).__name__}")
    if not isinstance(meta, CacheMeta):
        raise TypeError(f"meta must be a CacheMeta, got {type(meta).__name__}")


# ---------------------------------------------------------------------------------------------------------------
# The key
# ---------------------------------------------------------------------------------------------------------------


def request_key(
    *,
    provider_kind: str,
    model_id: str,
    generation_params: Mapping[str, Any],
    envelope: Envelope,
    provider_schema: Mapping[str, Any],
    schema_version: str,
) -> str:
    """Return the cache key of one request: 64 lowercase hex characters (module notes).

    Args:
        provider_kind: The adapter's kind (``"anthropic"``, ``"openai-compatible"``, ...).
        model_id: The provider-qualified model id.
        generation_params: What the adapter sends besides the messages and the schema (temperature, seed,
            max_output_tokens, ...), as a JSON object.
        envelope: The unit's envelope; only its nonce-free :meth:`~Envelope.key_material` is keyed.
        provider_schema: The JSON schema the adapter actually sends, as a JSON object.
        schema_version: The caller's schema version.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If a text argument is blank or not valid Unicode.
        RationalizationError: If ``generation_params`` or ``provider_schema`` is not exactly JSON (a key that is
            not a ``str`` or a tuple would canonicalize like another request's ``str`` key or list).
    """
    if not isinstance(envelope, Envelope):
        raise TypeError(f"envelope must be an Envelope, got {type(envelope).__name__}")
    request = {
        "format": CACHE_FORMAT,
        "provider_kind": _check_text("provider_kind", provider_kind),
        "model_id": _check_text("model_id", model_id),
        "generation_params": _check_json_object("generation_params", generation_params),
        "envelope": envelope.key_material(),
        "provider_schema": _check_json_object("provider_schema", provider_schema),
        "schema_version": _check_text("schema_version", schema_version),
    }
    return sha256_hex(canonical_json(request, subject="the request"))


# ---------------------------------------------------------------------------------------------------------------
# Lookups and entry metadata
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CacheLookup:
    """What a ``get`` found: a hit (``response``), a miss, or a corrupt miss (``corrupt`` and its ``reason``).

    Args:
        response: The cached response, or ``None``.
        corrupt: Whether the entry was there but unusable (then ``response`` is ``None``).
        reason: The :data:`CorruptReason` when corrupt, else ``None``.

    Raises:
        TypeError: If ``response`` is not a ``ProviderResponse`` or ``None``, or ``corrupt`` not a ``bool``.
        ValueError: If a corrupt lookup holds a response or no known reason, or a sound one has a reason.
    """

    response: ProviderResponse | None = None
    corrupt: bool = False
    reason: CorruptReason | None = None

    def __post_init__(self) -> None:
        if self.response is not None and not isinstance(self.response, ProviderResponse):
            raise TypeError(f"response must be a ProviderResponse or None, got {type(self.response).__name__}")
        if not isinstance(self.corrupt, bool):
            raise TypeError(f"corrupt must be a bool, got {type(self.corrupt).__name__}")
        if not self.corrupt:
            if self.reason is not None:
                raise ValueError("only a corrupt lookup has a reason")
            return
        if self.response is not None:
            raise ValueError("a corrupt lookup holds no response")
        if self.reason not in CORRUPT_REASONS:
            raise ValueError(f"a corrupt lookup's reason must be one of {CORRUPT_REASONS}")

    @property
    def hit(self) -> bool:
        """Whether a response was found."""
        return self.response is not None


def _check_latency(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"latency_ms must be an int or float, got {type(value).__name__}")
    try:
        latency = float(value)
    except OverflowError as error:  # an int past the float range
        raise ValueError("latency_ms must be finite and >= 0") from error
    if not math.isfinite(latency) or latency < 0:
        raise ValueError(f"latency_ms must be finite and >= 0, got {latency}")
    return latency


def _check_created_at(value: object) -> None:
    """Require an ISO-8601 ``str`` with a UTC offset of zero (``datetime.now(timezone.utc).isoformat()``)."""
    if not isinstance(value, str):
        raise TypeError(f"created_at must be an ISO-8601 str, got {type(value).__name__}")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("created_at must be an ISO-8601 date and time") from error
    if moment.utcoffset() != timedelta(0):
        raise ValueError("created_at must be in UTC (offset +00:00 or Z)")


@dataclass(frozen=True, slots=True, kw_only=True)
class CacheMeta:
    """The audit fields stored beside a cached response (module notes): ids, names, hashes, latency, time.

    Args:
        model_id: The provider-qualified model id.
        unit_id: The unit's id.
        doc_id: The document's id.
        schema_name: The schema model's name.
        schema_version: The caller's schema version.
        schema_sha256: The response wrapper's hash (``PreparedSchema.sha256``).
        prompt_sha256: The SHA-256 of the caller's prompt (never the prompt).
        latency_ms: The request's latency in milliseconds, finite and >= 0 as a float (held as one).
        created_at: When the response arrived, ISO-8601 in UTC.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a text is blank or not valid Unicode, a hash is not 64 lowercase hex characters, the
            latency is negative or not finite, or ``created_at`` is not ISO-8601 in UTC.
    """

    model_id: str
    unit_id: str
    doc_id: str
    schema_name: str
    schema_version: str
    schema_sha256: str
    prompt_sha256: str
    latency_ms: float
    created_at: str

    def __post_init__(self) -> None:
        for name in ("model_id", "unit_id", "doc_id", "schema_name", "schema_version"):
            _check_text(name, getattr(self, name))
        _check_hex("schema_sha256", self.schema_sha256)
        _check_hex("prompt_sha256", self.prompt_sha256)
        object.__setattr__(self, "latency_ms", _check_latency(self.latency_ms))
        _check_created_at(self.created_at)

    def to_json(self) -> dict[str, Any]:
        """Return the fields as a new JSON object, in declaration order."""
        return {item.name: getattr(self, item.name) for item in fields(self)}

    @classmethod
    def from_json(cls, data: object) -> "CacheMeta":
        """Build the metadata from :meth:`to_json`'s object: exactly its fields, each checked.

        Raises:
            TypeError: If ``data`` is not a ``Mapping``, or a field has the wrong type.
            ValueError: If a field is missing or extra, or a value is out of range.
        """
        if not isinstance(data, Mapping):
            raise TypeError(f"CacheMeta.from_json needs a JSON object, got {type(data).__name__}")
        names = [item.name for item in fields(cls)]
        missing = [name for name in names if name not in data]
        extra = sorted(str(name)[:40] for name in data if name not in names)
        if missing or extra:
            raise ValueError(f"CacheMeta fields must be exactly {names}: missing {missing}, extra {extra[:5]}")
        return cls(**{name: data[name] for name in names})


def _corrupt(reason: CorruptReason) -> CacheLookup:
    return CacheLookup(None, corrupt=True, reason=reason)


# ---------------------------------------------------------------------------------------------------------------
# MemoryCache
# ---------------------------------------------------------------------------------------------------------------


class MemoryCache:
    """In-run deduplication: responses by key, in memory, thread-safe (module notes).

    Args:
        max_entries: The most keys it holds, 1-:data:`MEMORY_MAX_ENTRIES` (the default).

    Raises:
        TypeError: If ``max_entries`` is not an ``int``.
        ValueError: If it is out of range.
    """

    __slots__ = ("_entries", "_lock", "_max_entries")

    def __init__(self, *, max_entries: int = MEMORY_MAX_ENTRIES) -> None:
        if type(max_entries) is not int:
            raise TypeError(f"max_entries must be an int, got {type(max_entries).__name__}")
        if not 1 <= max_entries <= MEMORY_MAX_ENTRIES:
            raise ValueError(f"max_entries must be in 1-{MEMORY_MAX_ENTRIES}, got {max_entries}")
        self._max_entries = max_entries
        self._entries: dict[str, str] = {}
        self._lock = threading.Lock()

    @property
    def max_entries(self) -> int:
        """The most keys it holds."""
        return self._max_entries

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def get(self, key: str) -> CacheLookup:
        """Return a hit with a fresh copy of the response held for ``key``, or a miss.

        Raises:
            TypeError: If ``key`` is not a ``str``.
            ValueError: If it is not 64 lowercase hex characters.
        """
        _check_key(key)
        with self._lock:
            text = self._entries.get(key)
        if text is None:
            return CacheLookup()
        return CacheLookup(ProviderResponse.from_json(json.loads(text)))

    def put(self, key: str, response: ProviderResponse, meta: CacheMeta) -> bool:
        """Hold ``response`` for ``key`` (replacing what it held); ``False`` for a new key past ``max_entries``.

        Raises:
            TypeError: If ``key``, ``response`` or ``meta`` has the wrong type.
            ValueError: If ``key`` is not 64 lowercase hex characters.
        """
        _check_key(key)
        _check_stored(response, meta)
        text = canonical_json(response.to_json(), subject="the response")
        with self._lock:
            if key not in self._entries and len(self._entries) >= self._max_entries:
                return False
            self._entries[key] = text
        return True


# ---------------------------------------------------------------------------------------------------------------
# FileCache: directories
# ---------------------------------------------------------------------------------------------------------------


def _check_cache_dir(cache_dir: object) -> Path:
    """Require a non-blank ``str`` or path-like ``cache_dir``; return it absolute."""
    if not isinstance(cache_dir, (str, os.PathLike)):
        raise TypeError(f"cache_dir must be a str or os.PathLike, got {type(cache_dir).__name__}")
    text = os.fspath(cache_dir)
    if not isinstance(text, str):
        raise TypeError(f"cache_dir must be a str path, got {type(text).__name__}")
    if not text.strip():
        raise ValueError("cache_dir must be non-blank")
    if "\x00" in text:
        raise ValueError("cache_dir must not hold a NUL character")
    return Path(os.path.abspath(text))


def _restrict(path: Path) -> None:
    """Set the directory just made at ``path`` to ``0o700`` (mkdir's mode passes through the umask), never through
    a symlink swapped in meanwhile: on POSIX through an ``O_DIRECTORY | O_NOFOLLOW`` descriptor of it.

    Raises:
        RationalizationError: If a symlink or a file is there now.
    """
    if not (_O_NOFOLLOW and _O_DIRECTORY):
        os.chmod(path, 0o700)  # Windows: chmod sets the read-only flag only
        return
    try:
        descriptor = os.open(path, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC)
    except PermissionError:
        # The umask took the owner's own read bit, so there is no descriptor to change it through: by path, then,
        # once it is checked to be a directory (the caller checks it again afterwards).
        if _directory_status(path) is not None:
            os.chmod(path, 0o700)
        return
    except OSError as error:
        if error.errno in _NOT_A_DIRECTORY_ERRNOS:
            raise _tampered(path) from error
        raise
    try:
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)


def _make_directory(path: Path, *, parents: bool) -> None:
    """Create ``path``, ``0o700`` past the umask; one that appeared meanwhile is left as it is (and checked)."""
    try:
        path.mkdir(mode=0o700, parents=parents)
    except FileExistsError:
        return  # made by another thread or process, or something else is there: the caller checks which
    _restrict(path)


def _make_root(root: Path) -> None:
    """Create a missing ``cache_dir`` with its parents; raise when it is not a directory (module notes, 2)."""
    try:
        if not root.is_dir():
            _make_directory(root, parents=True)
        is_directory = root.is_dir()
    except OSError as error:
        raise RationalizationError(f"cache_dir {root} cannot be created: {error}") from error
    if not is_directory:
        raise RationalizationError(f"cache_dir {root} exists but is not a directory")


def _directory_status(directory: Path) -> os.stat_result | None:
    """The sub-directory's ``lstat`` (the identity a read or write binds to), or ``None`` when it is missing.

    Raises:
        RationalizationError: If ``directory`` is a symlink or not a directory, or cannot be checked.
    """
    try:
        status = os.lstat(directory)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise RationalizationError(f"cache sub-directory {directory} cannot be checked: {error}") from error
    if stat.S_ISLNK(status.st_mode):
        raise RationalizationError(f"cache sub-directory {directory} is a symlink: the cache was tampered with")
    if not stat.S_ISDIR(status.st_mode):
        raise RationalizationError(f"cache sub-directory {directory} is not a directory: the cache was tampered with")
    return status


def _subdirectory(directory: Path) -> os.stat_result:
    """The identity of the entry's sub-directory, which is made ``0o700`` when missing.

    Raises:
        RationalizationError: If a symlink or no directory is there, made meanwhile or not.
        FileNotFoundError: If it is gone again as soon as it is made.
    """
    identity = _directory_status(directory)
    if identity is not None:
        return identity
    _make_directory(directory, parents=False)
    identity = _directory_status(directory)  # what is there now, made here or meanwhile, must be a directory
    if identity is None:
        raise FileNotFoundError(errno.ENOENT, "the cache sub-directory is gone as soon as it is made", str(directory))
    return identity


def _tampered(directory: Path) -> RationalizationError:
    return RationalizationError(
        f"cache sub-directory {directory} was replaced while in use (by a symlink, a file or another directory): "
        "the cache was tampered with"
    )


def _same_directory(directory: Path, identity: os.stat_result) -> bool:
    """Whether ``directory`` is still the directory ``identity`` describes (a symlink there never is)."""
    try:
        status = os.lstat(directory)
    except OSError:
        return False
    return (status.st_dev, status.st_ino) == (identity.st_dev, identity.st_ino)


def _open_directory(directory: Path, identity: os.stat_result) -> int:
    """A descriptor of the checked sub-directory itself: opened without following a symlink, of the same inode.

    Raises:
        RationalizationError: If a symlink, a file or another directory is there now.
        OSError: If it cannot be opened (``FileNotFoundError`` when it is gone).
    """
    try:
        descriptor = os.open(directory, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC)
    except OSError as error:
        if error.errno in _NOT_A_DIRECTORY_ERRNOS:  # a symlink or a file (ENOTDIR or ELOOP, by OS)
            raise _tampered(directory) from error
        raise
    try:
        status = os.fstat(descriptor)
        if (status.st_dev, status.st_ino) != (identity.st_dev, identity.st_ino):
            raise _tampered(directory)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


# ---------------------------------------------------------------------------------------------------------------
# FileCache: writes
# ---------------------------------------------------------------------------------------------------------------


def _write_descriptor(descriptor: int, data: bytes) -> None:
    """Write ``data`` to the temp file's ``descriptor``, ``0o600``, flushed and fsynced; closes the descriptor."""
    try:
        if sys.platform != "win32":
            os.fchmod(descriptor, 0o600)  # the 0o600 it was made with passes through the umask
        stream = open(descriptor, "wb")
    except BaseException:
        os.close(descriptor)
        raise
    with stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _fsync_open_directory(descriptor: int) -> None:
    """Fsync a directory's descriptor, so a replace in it is durable; a filesystem that cannot is skipped."""
    try:
        os.fsync(descriptor)
    except OSError as error:
        if error.errno not in _FSYNC_UNSUPPORTED:
            raise


def _fsync_directory(directory: Path) -> None:
    """Fsync ``directory`` by path where the OS allows it (not Windows, which cannot open a directory)."""
    if sys.platform == "win32":
        return
    descriptor = os.open(directory, os.O_RDONLY | _O_DIRECTORY | _O_CLOEXEC)
    try:
        _fsync_open_directory(descriptor)
    finally:
        os.close(descriptor)


def _write_in(directory_fd: int, name: str, data: bytes) -> None:
    """Write ``data`` to a new temp file at ``directory_fd``, then replace ``name`` there with it (module notes, 3)."""
    temporary = f"{_TEMP_PREFIX}{secrets.token_hex(16)}{_TEMP_SUFFIX}"
    descriptor = os.open(temporary, _CREATE_FLAGS, 0o600, dir_fd=directory_fd)
    try:
        _write_descriptor(descriptor, data)
        os.replace(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=directory_fd)
        raise
    _fsync_open_directory(directory_fd)


def _write_bound(directory: Path, identity: os.stat_result, name: str, data: bytes) -> None:
    """Write through a descriptor of the checked sub-directory, so no swap of its path can redirect the write."""
    directory_fd = _open_directory(directory, identity)
    try:
        _write_in(directory_fd, name, data)
    finally:
        os.close(directory_fd)


def _write_by_path(directory: Path, identity: os.stat_result, name: str, data: bytes) -> None:
    """The fallback without ``dir_fd``: by path, the directory's identity checked again once the temp file exists."""
    descriptor, temporary = tempfile.mkstemp(dir=directory, prefix=_TEMP_PREFIX, suffix=_TEMP_SUFFIX)
    try:
        if not _same_directory(directory, identity):
            os.close(descriptor)
            raise _tampered(directory)
        _write_descriptor(descriptor, data)
        os.replace(temporary, directory / name)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise
    _fsync_directory(directory)


def _write_entry(directory: Path, identity: os.stat_result, name: str, data: bytes) -> None:
    """Write the entry into the checked sub-directory (module notes, 3), and fail loud if it was swapped meanwhile."""
    if _DIR_FD:
        _write_bound(directory, identity, name, data)
    else:
        _write_by_path(directory, identity, name, data)
    if not _same_directory(directory, identity):
        raise _tampered(directory)


# ---------------------------------------------------------------------------------------------------------------
# FileCache: reads
# ---------------------------------------------------------------------------------------------------------------


def _read_descriptor(descriptor: int) -> bytes | CorruptReason:
    """The bytes of an open entry, when it is a regular file of at most :data:`ENTRY_MAX_BYTES`."""
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        return "not_regular_file"
    with open(descriptor, "rb", closefd=False) as stream:
        data = stream.read(ENTRY_MAX_BYTES + 1)  # one byte past the bound tells a larger file
    return "too_large" if len(data) > ENTRY_MAX_BYTES else data


def _read_entry(path: str | Path, directory_fd: int | None) -> bytes | CorruptReason | None:
    """The entry's bytes (``path`` at ``directory_fd``, or a whole path), ``None`` when there is none, or why it
    cannot be read (module notes, 4)."""
    if directory_fd is None and not _O_NOFOLLOW and os.path.islink(path):
        return "symlink"  # no O_NOFOLLOW (Windows): the path is checked before it is opened instead
    try:
        descriptor = os.open(path, _READ_FLAGS | _O_NOFOLLOW, dir_fd=directory_fd)
    except FileNotFoundError:
        return None
    except OSError as error:
        return "symlink" if error.errno in _SYMLINK_ERRNOS else "unreadable"
    try:
        return _read_descriptor(descriptor)
    except OSError:
        return "unreadable"
    finally:
        os.close(descriptor)


def _read_bound(directory: Path, identity: os.stat_result, name: str) -> bytes | CorruptReason | None:
    """Read the entry through a descriptor of the checked sub-directory, so no swap of its path redirects it."""
    try:
        directory_fd = _open_directory(directory, identity)
    except FileNotFoundError:
        return None  # removed since the check: there is nothing to read
    except OSError as error:
        raise RationalizationError(f"cache sub-directory {directory} cannot be opened: {error}") from error
    try:
        return _read_entry(name, directory_fd)
    finally:
        os.close(directory_fd)


def _read_by_path(directory: Path, identity: os.stat_result, name: str) -> bytes | CorruptReason | None:
    """The fallback without ``dir_fd``: by path, the directory's identity checked again after the read."""
    data = _read_entry(directory / name, None)
    if data is not None and not _same_directory(directory, identity):
        raise _tampered(directory)
    return data


def _refuse_constant(name: str) -> NoReturn:
    raise ValueError(f"{name} is not JSON")


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """A JSON object from its pairs, refusing a repeated key (a file ``put`` wrote never repeats one)."""
    found = dict(pairs)
    if len(found) != len(pairs):
        raise ValueError("an object repeats a key")
    return found


def _parse_entry(data: bytes, key: str) -> CacheLookup:
    """Check an entry's bytes whole (module notes, 4): a hit, or a corrupt miss with its reason, never an error."""
    try:
        entry = json.loads(data.decode("utf-8"), parse_constant=_refuse_constant, object_pairs_hook=_unique_keys)
    except (ValueError, RecursionError):  # a UnicodeDecodeError or a JSONDecodeError is a ValueError
        return _corrupt("not_json")
    if not isinstance(entry, dict):
        return _corrupt("not_an_entry")
    if entry.get("format") != CACHE_FORMAT:
        return _corrupt("wrong_format")
    if entry.keys() != _ENTRY_FIELDS:
        return _corrupt("not_an_entry")
    if entry["key"] != key:
        return _corrupt("wrong_key")
    try:
        CacheMeta.from_json(entry["meta"])
    except (TypeError, ValueError, ArithmeticError):  # ArithmeticError: no number check may escape as one
        return _corrupt("invalid_meta")
    try:
        response = ProviderResponse.from_json(entry["response"])
    except (TypeError, ValueError, ArithmeticError):
        return _corrupt("invalid_response")
    return CacheLookup(response)


# ---------------------------------------------------------------------------------------------------------------
# FileCache
# ---------------------------------------------------------------------------------------------------------------


class FileCache:
    """The persistent store under ``cache_dir``: one atomic JSON file per entry (module notes).

    Args:
        cache_dir: The cache's directory, created (with its parents) when missing.

    Raises:
        TypeError: If ``cache_dir`` is not a ``str`` or ``str`` path-like.
        ValueError: If it is blank or holds a NUL character.
        RationalizationError: If it exists but is not a directory, or cannot be created.
    """

    __slots__ = ("_root",)

    def __init__(self, cache_dir: str | os.PathLike[str]) -> None:
        self._root = _check_cache_dir(cache_dir)
        _make_root(self._root)

    @property
    def cache_dir(self) -> Path:
        """The cache's directory, absolute."""
        return self._root

    def get(self, key: str) -> CacheLookup:
        """Return the entry for ``key``: a hit, a miss, or a corrupt miss (never an error for a bad entry).

        It writes nothing.

        Raises:
            TypeError: If ``key`` is not a ``str``.
            ValueError: If it is not 64 lowercase hex characters.
            RationalizationError: If the entry's sub-directory is a symlink or not a directory, cannot be opened, or
                is swapped while it is read.
        """
        _check_key(key)
        directory = self._root / key[:2]
        identity = _directory_status(directory)
        if identity is None:
            return CacheLookup()
        name = f"{key}.json"
        data = _read_bound(directory, identity, name) if _DIR_FD else _read_by_path(directory, identity, name)
        if data is None:
            return CacheLookup()
        if isinstance(data, str):
            return _corrupt(data)
        return _parse_entry(data, key)

    def put(self, key: str, response: ProviderResponse, meta: CacheMeta) -> bool:
        """Write the entry for ``key`` atomically, replacing what is there; ``False`` (nothing written) past 8 MiB.

        Raises:
            TypeError: If ``key``, ``response`` or ``meta`` has the wrong type.
            ValueError: If ``key`` is not 64 lowercase hex characters.
            RationalizationError: If the entry's sub-directory is a symlink or not a directory, or is swapped while
                the entry is written, or the write fails (the ``OSError`` chained; a directory planted at the entry
                path is one such failure).
        """
        _check_key(key)
        _check_stored(response, meta)
        entry = {"format": CACHE_FORMAT, "key": key, "response": response.to_json(), "meta": meta.to_json()}
        data = canonical_json(entry, subject="the cache entry").encode("utf-8")
        if len(data) > ENTRY_MAX_BYTES:
            return False
        directory = self._root / key[:2]
        try:
            _write_entry(directory, _subdirectory(directory), f"{key}.json", data)
        except OSError as error:
            raise RationalizationError(
                f"cannot write cache entry {key} under cache_dir {self._root}: {error}"
            ) from error
        return True
