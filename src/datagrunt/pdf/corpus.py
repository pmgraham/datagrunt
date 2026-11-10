"""Stage 1 batch PDF corpus extraction and loading (#463, 5.2): ``datagrunt.pdf.corpus``.

:func:`extract_corpus` extracts a batch of PDFs (a directory, a single path, or
a sequence of paths) into a corpus directory of verified v5 artifact bundles
plus a ``corpus_manifest.json`` index, with incremental skip (`reused=True`)
when ``overwrite=False`` and per-document fault isolation when ``strict=False``.

:func:`load_corpus` and :class:`CorpusManifest` are pure stdlib/core readers and
never require the ``[pdf]`` extra (``pypdfium2`` and ``Pillow``).
"""

import errno
import json
import os
import subprocess
import sys
import threading
import uuid
import warnings
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import nullcontext
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import IO, Final, NamedTuple, cast

import datagrunt
from datagrunt.pdf._bundle_load import (
    _MAX_ARTIFACT_BYTES,
    BUNDLE_FORMAT_VERSION,
    _loaded,
    _opened,
    _read_fd,
    _regular,
    _same_output_params,
    load_bundle,
)
from datagrunt.pdf._extra import load as _load_extra
from datagrunt.pdf._options import (
    DPI,
    MAX_RENDER_PIXELS,
    MIN_IMAGE_DIMENSION,
    OcrMode,
    RenderPagesMode,
    checked_render_pages,
)
from datagrunt.pdf._process import (
    ERROR,
    MAX_REQUEST_BYTES,
    REAP_TIMEOUT_S,
    MessageTooLong,
    StderrTail,
    close_pipes,
    error_reply,
    kill_group,
    next_reply,
    read_error,
    read_message,
    reaped,
    send_message,
    start_worker,
)
from datagrunt.pdf._source import _opened_source, _stream_sha256, checked_pdf_file
from datagrunt.pdf._validate import (
    checked_password,
    checked_path,
    int_at_least,
    non_blank_str,
    sha256_hex,
    strict_bool,
    typed_tuple,
)
from datagrunt.pdf.codec import _array, _object, contained_path, parse_json
from datagrunt.pdf.elements import DocumentMetadata, PDFDocument
from datagrunt.pdf.exceptions import BundleCorruptError, DatagruntPDFError
from datagrunt.pdf.pipeline import PageWorkerPool, PipelineOptions, _pool_size, extract_document

_default_extract_document = extract_document
_default_load_extra = _load_extra

__all__ = [
    "CORPUS_FORMAT_VERSION",
    "CORPUS_MANIFEST",
    "CorpusEntry",
    "CorpusFailure",
    "CorpusManifest",
    "extract_corpus",
    "load_corpus",
]

CORPUS_FORMAT_VERSION: Final[str] = "5.2"
CORPUS_MANIFEST: Final[str] = "corpus_manifest.json"
_MAX_CORPUS_WORKERS: Final[int] = 64
_MANIFEST_KEYS: Final[tuple[str, ...]] = ("corpus_format_version", "entries", "failures")
_ENTRY_KEYS: Final[tuple[str, ...]] = (
    "doc_id",
    "source_name",
    "source_sha256",
    "bundle_relpath",
    "page_count",
    "integrity_verified",
    "degraded_pages",
    "reused",
)
_FAILURE_KEYS: Final[tuple[str, ...]] = ("source_path", "error_type", "message")


class _SourceItem(NamedTuple):
    """A validated input PDF ready for corpus extraction."""

    path: Path
    source_name: str
    sha256: str
    doc_id: str


@dataclass(frozen=True, slots=True)
class CorpusEntry:
    """One extracted or reused document bundle in a corpus manifest."""

    doc_id: str
    source_name: str
    source_sha256: str
    bundle_relpath: str
    page_count: int
    integrity_verified: bool
    degraded_pages: tuple[int, ...] = ()
    reused: bool = False

    def __post_init__(self) -> None:
        non_blank_str("doc_id", self.doc_id)
        non_blank_str("source_name", self.source_name)
        sha256_hex("source_sha256", self.source_sha256)
        non_blank_str("bundle_relpath", self.bundle_relpath)
        int_at_least("page_count", self.page_count, 1)
        strict_bool("integrity_verified", self.integrity_verified)
        if not isinstance(self.degraded_pages, tuple):
            raise TypeError(f"degraded_pages must be a tuple, got {type(self.degraded_pages).__name__}")
        for index, page in enumerate(self.degraded_pages):
            int_at_least(f"degraded_pages[{index}]", page, 1)
        strict_bool("reused", self.reused)


@dataclass(frozen=True, slots=True)
class CorpusFailure:
    """One failed source PDF recorded during lenient corpus extraction."""

    source_path: str
    error_type: str
    message: str

    def __post_init__(self) -> None:
        non_blank_str("source_path", self.source_path)
        non_blank_str("error_type", self.error_type)
        non_blank_str("message", self.message)


@dataclass(frozen=True, slots=True)
class CorpusManifest:
    """Manifest of an extracted PDF corpus directory."""

    corpus_format_version: str
    output_dir: Path
    entries: tuple[CorpusEntry, ...]
    failures: tuple[CorpusFailure, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.corpus_format_version, str):
            raise TypeError(f"corpus_format_version must be a str, got {type(self.corpus_format_version).__name__}")
        if self.corpus_format_version != CORPUS_FORMAT_VERSION:
            raise ValueError(
                f"corpus_format_version {self.corpus_format_version!r} is not supported: "
                f"expected {CORPUS_FORMAT_VERSION!r}"
            )
        if not isinstance(self.output_dir, Path):
            raise TypeError(f"output_dir must be a Path, got {type(self.output_dir).__name__}")
        typed_tuple("entries", self.entries, CorpusEntry)
        typed_tuple("failures", self.failures, CorpusFailure)

    @property
    def bundle_paths(self) -> tuple[Path, ...]:
        """Resolved bundle directories for every entry in ``entries``."""
        return tuple(self.output_dir / entry.bundle_relpath for entry in self.entries)

    def load_documents(self) -> tuple[PDFDocument, ...]:
        """Load and verify every bundle in ``entries`` via :func:`load_bundle`."""
        return tuple(_verified_entry_document(self.output_dir, entry) for entry in self.entries)


def extract_corpus(
    inputs: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
    output_dir: str | os.PathLike[str],
    *,
    password: str | None = None,
    max_workers: int = 1,
    workers: int = 1,
    ocr: OcrMode = "auto",
    dpi: int = DPI,
    vision_dpi: int = 300,
    max_render_pixels: int = MAX_RENDER_PIXELS,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
    strict: bool = False,
    overwrite: bool = False,
    include_images: bool = True,
    render_pages: RenderPagesMode = True,
    sandbox: bool = False,
    orientation_fallback: bool = False,
) -> CorpusManifest:
    """Extract a batch of PDFs into verified bundles under ``output_dir`` and write ``corpus_manifest.json``."""
    int_at_least("max_workers", max_workers, 1)
    if max_workers > _MAX_CORPUS_WORKERS:
        raise ValueError(f"max_workers must be <= {_MAX_CORPUS_WORKERS}, got {max_workers}")
    replace = strict_bool("overwrite", overwrite)
    checked_include_images = strict_bool("include_images", include_images)
    checked_render_pgs = checked_render_pages(render_pages)
    checked_pwd = checked_password(password)
    options = PipelineOptions(
        workers=workers,
        ocr=ocr,
        dpi=dpi,
        vision_dpi=vision_dpi,
        max_render_pixels=max_render_pixels,
        min_image_dimension=min_image_dimension,
        strict=strict,
        sandbox=sandbox,
        orientation_fallback=orientation_fallback,
    )
    target = _checked_corpus_dir(output_dir)
    items = _resolve_inputs(inputs)
    write_bundle_fn = _load_extra("write_bundle")
    target.mkdir(parents=True, exist_ok=True)
    batch_workers = min(max_workers, len(items))
    can_spawn_procs = (
        batch_workers > 1
        and not options.sandbox
        and not getattr(sys, "frozen", False)
        and bool(sys.executable)
        and extract_document is _default_extract_document
        and _load_extra is _default_load_extra
        and getattr(write_bundle_fn, "__module__", "") == "datagrunt.pdf.bundle"
    )
    if can_spawn_procs:
        outcomes = _run_spawn_batch(
            items,
            target,
            checked_pwd,
            options,
            replace,
            include_images=checked_include_images,
            render_pages=checked_render_pgs,
            workers=batch_workers,
        )
    else:
        use_pool = options.sandbox or _pool_size(options.workers) > 1
        with PageWorkerPool() if use_pool else nullcontext() as worker_pool:
            outcomes = _run_batch(
                items,
                lambda item: _process_item(
                    item,
                    target,
                    checked_pwd,
                    options,
                    replace,
                    write_bundle_fn,
                    include_images=checked_include_images,
                    render_pages=checked_render_pgs,
                    worker_pool=worker_pool,
                ),
                batch_workers,
            )
    entries = tuple(res for res in outcomes if isinstance(res, CorpusEntry))
    failures = tuple(res for res in outcomes if isinstance(res, CorpusFailure))
    manifest = CorpusManifest(
        corpus_format_version=CORPUS_FORMAT_VERSION,
        output_dir=target,
        entries=entries,
        failures=failures,
    )
    _write_corpus_manifest(target, manifest)
    return manifest


def load_corpus(path: str | os.PathLike[str]) -> CorpusManifest:
    """Load and verify the corpus manifest and its bundles at ``path`` (directory or ``corpus_manifest.json``)."""
    root = _corpus_root(path)
    raw = _read_corpus_manifest_bytes(root)
    try:
        manifest = _corpus_manifest_of(parse_json(raw, CORPUS_MANIFEST), root)
    except (TypeError, ValueError) as exc:
        raise BundleCorruptError(f"{CORPUS_MANIFEST}: {exc}") from exc
    for entry in manifest.entries:
        _verified_entry_document(root, entry)
    return manifest


def _checked_corpus_dir(output_dir: object) -> Path:
    """Validate ``output_dir`` without creating it; return its resolved ``Path``."""
    raw = checked_path(output_dir, "output_dir").absolute()
    if raw.is_symlink() or (raw.exists() and not raw.is_dir()):
        raise FileExistsError(errno.EEXIST, "output_dir exists and is not a directory", str(raw))
    if not raw.parent.exists():
        raise FileNotFoundError(errno.ENOENT, "output_dir's parent does not exist", str(raw.parent))
    return raw.resolve()


def _resolve_inputs(inputs: object) -> tuple[_SourceItem, ...]:
    """Resolve and validate ``inputs`` (directory, single file, or sequence of files) before any writes."""
    raw_paths = _discover_paths(inputs)
    items: list[_SourceItem] = []
    seen_paths: set[Path] = set()
    seen_doc_ids: set[str] = set()
    for path in raw_paths:
        resolved = path.resolve()
        if resolved in seen_paths:
            raise ValueError(f"duplicate input path in inputs: {resolved}")
        seen_paths.add(resolved)
        sha256 = _file_sha256(resolved)
        doc_id = DocumentMetadata.make_doc_id(resolved.name, sha256)
        if doc_id in seen_doc_ids:
            raise ValueError(f"duplicate doc_id {doc_id!r} across inputs: {resolved}")
        seen_doc_ids.add(doc_id)
        items.append(_SourceItem(path=resolved, source_name=resolved.name, sha256=sha256, doc_id=doc_id))
    return tuple(items)


def _discover_paths(inputs: object) -> list[Path]:
    """Discover raw PDF paths from a single path, directory, or non-empty sequence."""
    if isinstance(inputs, (str, os.PathLike)):
        given = checked_path(inputs, "inputs")
        if not given.exists():
            raise FileNotFoundError(errno.ENOENT, "input path does not exist", str(given))
        if given.is_dir():
            pdfs = sorted(p for p in given.iterdir() if p.is_file() and p.suffix.lower() == ".pdf")
            if not pdfs:
                raise ValueError(f"inputs directory {given} contains no PDF files")
            return pdfs
        return [given]
    if isinstance(inputs, Sequence) and not isinstance(inputs, (bytes, bytearray)):
        if not inputs:
            raise ValueError("inputs must not be empty")
        paths: list[Path] = []
        for item in inputs:
            path = checked_path(item, "inputs")
            if not path.exists():
                raise FileNotFoundError(errno.ENOENT, "input path does not exist", str(path))
            paths.append(path)
        return paths
    raise TypeError(f"inputs must be a path or Sequence of paths, got {type(inputs).__name__}")


def _file_sha256(path: Path) -> str:
    """Stream the SHA-256 of the regular file at ``path``."""
    with _opened_source(path) as stream:
        size = os.fstat(stream.fileno()).st_size
        return _stream_sha256(stream, size, path)


_CORPUS_DOC_TIMEOUT_S: Final[float] = 1800.0
_MAX_WORKER_DOCS: Final[int] = 100_000
_OK_TAG: Final[bytes] = b"O"
_CORPUS_POOL_BOOTSTRAP: Final[str] = (
    "import json, os, sys; "
    f"raw_path = sys.stdin.buffer.readline({MAX_REQUEST_BYTES}); "
    "sys.path[:] = json.loads(raw_path); "
    "out = os.fdopen(os.dup(1), 'wb'); "
    "os.dup2(2, 1); "
    "sys.stdout = open(1, 'w', encoding='utf-8', closefd=False); "
    "from datagrunt.pdf.corpus import _corpus_worker_main; "
    "_corpus_worker_main(out)"
)


class _CorpusProcPool:
    """Warm isolated child processes (``python -I -c``) for parallel multi-document corpus extraction."""

    __slots__ = ("_closed", "_idle", "_lock")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._idle: list[tuple[subprocess.Popen[bytes], StderrTail, int]] = []
        self._closed = False

    def acquire(self) -> tuple["subprocess.Popen[bytes]", StderrTail, int]:
        """Return ``(child, stderr, uses_so_far)`` for a healthy worker or start a new isolated worker."""
        with self._lock:
            if self._closed:
                raise ValueError("_CorpusProcPool is closed")
            while self._idle:
                child, stderr, uses = self._idle.pop()
                if uses < _MAX_WORKER_DOCS and child.poll() is None:
                    return child, stderr, uses
                close_pipes(child)
                kill_group(child)
                reaped(child, REAP_TIMEOUT_S)
                stderr.finish(REAP_TIMEOUT_S)
        if not sys.executable:
            raise DatagruntPDFError("sys.executable is empty: cannot start corpus worker")
        child, stderr = start_worker([sys.executable, "-I", "-c", _CORPUS_POOL_BOOTSTRAP], own_group=True)
        if child.stdin is not None:
            try:
                resolved = [os.path.abspath(entry) if entry else os.getcwd() for entry in sys.path]
                child.stdin.write((json.dumps(resolved) + "\n").encode("utf-8"))
                child.stdin.flush()
            except OSError:
                pass
        return child, stderr, 0

    def release(
        self, child: "subprocess.Popen[bytes]", stderr: StderrTail, uses: int, *, healthy: bool
    ) -> tuple[int | None, str]:
        """Return ``child`` to idle pool when ``healthy`` and under retirement cap, else terminate and reap."""
        if healthy and uses < _MAX_WORKER_DOCS and child.poll() is None:
            with self._lock:
                if not self._closed:
                    self._idle.append((child, stderr, uses))
                    return None, ""
        close_pipes(child)
        kill_group(child)
        code = reaped(child, REAP_TIMEOUT_S)
        return code, stderr.finish(REAP_TIMEOUT_S)

    def close(self) -> None:
        """Close, terminate, and reap all idle corpus workers."""
        with self._lock:
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
        for child, stderr, _uses in idle:
            close_pipes(child)
            kill_group(child)
            reaped(child, REAP_TIMEOUT_S)
            stderr.finish(REAP_TIMEOUT_S)

    def __enter__(self) -> "_CorpusProcPool":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _entry_dict(entry: CorpusEntry) -> dict[str, object]:
    """Serialize ``entry`` to a JSON-compatible dictionary."""
    return {
        "doc_id": entry.doc_id,
        "source_name": entry.source_name,
        "source_sha256": entry.source_sha256,
        "bundle_relpath": entry.bundle_relpath,
        "page_count": entry.page_count,
        "integrity_verified": entry.integrity_verified,
        "degraded_pages": list(entry.degraded_pages),
        "reused": entry.reused,
    }


def _failure_dict(failure: CorpusFailure) -> dict[str, object]:
    """Serialize ``failure`` to a JSON-compatible dictionary."""
    return {
        "source_path": failure.source_path,
        "error_type": failure.error_type,
        "message": failure.message,
    }


def _expected_params_for(
    options: PipelineOptions, *, include_images: bool, render_pages: RenderPagesMode
) -> tuple[dict[str, object], dict[str, object]]:
    """Build ``(extra_params, expected_params)`` for bundle manifest matching."""
    extra_params: dict[str, object] = {
        "ocr": options.ocr,
        "min_image_dimension": options.min_image_dimension,
        "strict": options.strict,
        **({"orientation_fallback": True} if options.orientation_fallback else {}),
    }
    expected_params: dict[str, object] = {
        "dpi": options.dpi,
        "vision_dpi": options.vision_dpi,
        "max_render_pixels": options.max_render_pixels,
        "workers": options.workers,
        **extra_params,
    }
    if options.sandbox:
        expected_params["sandbox"] = True
    if not include_images:
        expected_params["include_images"] = False
    if render_pages is False:
        expected_params["render_pages"] = False
    elif render_pages == "vision_only":
        expected_params["render_pages"] = "vision_only"
    return extra_params, expected_params


def _encode_corpus_request(
    item: _SourceItem,
    target: Path,
    password: str | None,
    options: PipelineOptions,
    overwrite: bool,
    *,
    include_images: bool,
    render_pages: RenderPagesMode,
) -> bytes:
    """Encode one document extraction request as bounded UTF-8 JSON."""
    payload = {
        "path": str(item.path),
        "source_name": item.source_name,
        "sha256": item.sha256,
        "doc_id": item.doc_id,
        "target": str(target),
        "password": password,
        "overwrite": overwrite,
        "include_images": include_images,
        "render_pages": render_pages,
        "options": {
            "workers": options.workers,
            "ocr": options.ocr,
            "dpi": options.dpi,
            "vision_dpi": options.vision_dpi,
            "max_render_pixels": options.max_render_pixels,
            "min_image_dimension": options.min_image_dimension,
            "strict": options.strict,
            "sandbox": options.sandbox,
            "orientation_fallback": options.orientation_fallback,
        },
    }
    raw = json.dumps(payload, allow_nan=False).encode("utf-8")
    if len(raw) > MAX_REQUEST_BYTES:
        raise DatagruntPDFError(f"corpus request exceeds {MAX_REQUEST_BYTES} bytes")
    return raw


def _run_encoded_corpus_request(raw: bytes) -> bytes:
    """Execute one encoded corpus document request inside the child worker and return the reply frame."""
    req = cast(dict[str, object], json.loads(raw))
    opts_raw = cast(dict[str, object], req["options"])
    item = _SourceItem(
        path=Path(cast(str, req["path"])),
        source_name=cast(str, req["source_name"]),
        sha256=cast(str, req["sha256"]),
        doc_id=cast(str, req["doc_id"]),
    )
    options = PipelineOptions(
        workers=cast(int, opts_raw["workers"]),
        ocr=cast(OcrMode, opts_raw["ocr"]),
        dpi=cast(int, opts_raw["dpi"]),
        vision_dpi=cast(int, opts_raw["vision_dpi"]),
        max_render_pixels=cast(int, opts_raw["max_render_pixels"]),
        min_image_dimension=cast(int, opts_raw["min_image_dimension"]),
        strict=cast(bool, opts_raw["strict"]),
        sandbox=cast(bool, opts_raw["sandbox"]),
        orientation_fallback=cast(bool, opts_raw["orientation_fallback"]),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", UserWarning)
        outcome = _process_item(
            item,
            Path(cast(str, req["target"])),
            cast(str | None, req["password"]),
            options,
            cast(bool, req["overwrite"]),
            _default_load_extra("write_bundle"),
            include_images=cast(bool, req["include_images"]),
            render_pages=checked_render_pages(req["render_pages"]),
            worker_pool=None,
        )
    user_notices = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
    if isinstance(outcome, CorpusEntry):
        body = {"kind": "entry", "pid": os.getpid(), "data": _entry_dict(outcome), "warnings": user_notices}
    else:
        body = {"kind": "failure", "pid": os.getpid(), "data": _failure_dict(outcome), "warnings": user_notices}
    return _OK_TAG + json.dumps(body, allow_nan=False).encode("utf-8")


def _corpus_worker_main(replies: IO[bytes]) -> None:
    """Child process loop reading document requests on stdin and sending length-prefixed replies."""
    # Bound is generous (+1) so parent-driven retirement closes stdin cleanly before child loop ends
    for _ in range(_MAX_WORKER_DOCS + 1):
        raw = read_message(sys.stdin.buffer, MAX_REQUEST_BYTES)
        if raw is None:
            return
        try:
            reply = _run_encoded_corpus_request(raw)
        except Exception as exc:  # sent to parent for strict raise; never swallowed
            try:
                send_message(replies, ERROR + error_reply(exc))
            except OSError:
                return
            continue
        try:
            send_message(replies, reply)
        except OSError:
            return


def _decode_corpus_reply(reply: bytes, target: Path) -> tuple[int, CorpusEntry | CorpusFailure]:
    """Decode one child worker reply frame into ``(child_pid, outcome)``, re-emitting UserWarnings."""
    if not isinstance(reply, bytes) or len(reply) < 2:
        raise DatagruntPDFError("the corpus worker sent a truncated reply")
    tag, body = reply[:1], reply[1:]
    if tag == ERROR:
        raise read_error(body, "the corpus worker")
    if tag != _OK_TAG:
        raise DatagruntPDFError("the corpus worker sent an unknown reply tag")
    msg = _object("corpus_reply", json.loads(body), ("kind", "pid", "data", "warnings"))
    for idx, item in enumerate(_array("corpus_reply.warnings", msg["warnings"])):
        if not isinstance(item, str):
            raise ValueError(f"corpus_reply.warnings[{idx}] must be str")
        warnings.warn(item, UserWarning, stacklevel=3)
    pid = int_at_least("corpus_reply.pid", msg["pid"], 1)
    if msg["kind"] == "entry":
        return pid, _entry_of("corpus_reply.data", msg["data"], target)
    if msg["kind"] == "failure":
        return pid, _failure_of("corpus_reply.data", msg["data"])
    raise DatagruntPDFError(f"unknown corpus reply kind: {msg['kind']!r}")


def _ipc_failure(item: _SourceItem, strict: bool, exc: Exception) -> tuple[int, CorpusEntry | CorpusFailure]:
    """Raise ``exc`` when ``strict`` is True, else record a :class:`CorpusFailure`."""
    if strict:
        if isinstance(exc, MessageTooLong):
            raise DatagruntPDFError(str(exc)) from exc
        raise exc
    err_name = "DatagruntPDFError" if isinstance(exc, MessageTooLong) else type(exc).__name__
    return 0, CorpusFailure(source_path=str(item.path), error_type=err_name, message=str(exc) or err_name)


def _run_in_corpus_worker(
    proc_pool: _CorpusProcPool,
    item: _SourceItem,
    target: Path,
    password: str | None,
    options: PipelineOptions,
    overwrite: bool,
    *,
    include_images: bool,
    render_pages: RenderPagesMode,
) -> tuple[int, CorpusEntry | CorpusFailure]:
    """Send ``item`` to a warm child process in ``proc_pool`` and decode its reply."""
    try:
        request_bytes = _encode_corpus_request(
            item, target, password, options, overwrite, include_images=include_images, render_pages=render_pages
        )
    except (DatagruntPDFError, ValueError) as exc:
        return _ipc_failure(item, options.strict, exc)
    child, stderr, uses = proc_pool.acquire()
    healthy = False
    reply: bytes | None = None
    expired = False
    try:
        if child.stdin is None or child.stdout is None:
            raise DatagruntPDFError("corpus worker pipes are missing")
        try:
            send_message(child.stdin, request_bytes)
        except OSError:
            pass
        reply, expired = next_reply(child, child.stdout, _CORPUS_DOC_TIMEOUT_S, MAX_REQUEST_BYTES, kill=kill_group)
        healthy = reply is not None and not expired
    except (MessageTooLong, DatagruntPDFError) as exc:
        return _ipc_failure(item, options.strict, exc)
    finally:
        code, tail = proc_pool.release(child, stderr, uses + 1, healthy=healthy)
    if reply is None:
        reason = "corpus worker timed out" if expired else f"corpus worker exited (code {code})"
        detail = f"{reason}: {tail}" if tail else reason
        return _ipc_failure(item, options.strict, DatagruntPDFError(detail))
    try:
        return _decode_corpus_reply(reply, target)
    except (DatagruntPDFError, OSError, ValueError) as exc:
        return _ipc_failure(item, options.strict, exc)


def _safe_file_size(path: Path) -> int:
    """Return file byte size for LPT scheduling, or 0 if stat fails."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _lpt_todo_indices(items: tuple[_SourceItem, ...], todo_indices: list[int], *, strict: bool) -> list[int]:
    """Order multi-worker work items largest-file-first (Graham LPT) when ``strict`` is False."""
    if not isinstance(todo_indices, list):
        raise TypeError(f"todo_indices must be a list, got {type(todo_indices).__name__}")
    strict_bool("strict", strict)
    if strict or len(todo_indices) <= 1:
        return list(todo_indices)
    return sorted(todo_indices, key=lambda idx: (-_safe_file_size(items[idx].path), idx))


def _run_spawn_batch(
    items: tuple[_SourceItem, ...],
    target: Path,
    password: str | None,
    options: PipelineOptions,
    overwrite: bool,
    *,
    include_images: bool,
    render_pages: RenderPagesMode,
    workers: int,
) -> list[CorpusEntry | CorpusFailure]:
    """Verify reusable bundles first, then extract remaining PDFs across isolated worker processes."""
    outcomes: dict[int, CorpusEntry | CorpusFailure] = {}
    todo_indices: list[int] = []
    _, expected_params = _expected_params_for(options, include_images=include_images, render_pages=render_pages)
    for index, item in enumerate(items):
        if not overwrite and (target / item.doc_id).is_dir():
            reused = _reused_entry(target / item.doc_id, item, item.doc_id, expected_params)
            if reused is not None:
                outcomes[index] = reused
                continue
        todo_indices.append(index)
    if not todo_indices:
        return [outcomes[i] for i in range(len(items))]
    if len(todo_indices) == 1:
        idx = todo_indices[0]
        outcomes[idx] = _process_item(
            items[idx],
            target,
            password,
            options,
            overwrite,
            _default_load_extra("write_bundle"),
            include_images=include_images,
            render_pages=render_pages,
        )
        return [outcomes[i] for i in range(len(items))]
    ordered_todo = _lpt_todo_indices(items, todo_indices, strict=options.strict)
    todo_items = tuple(items[i] for i in ordered_todo)
    pool_size = min(workers, len(todo_items))
    with _CorpusProcPool() as proc_pool:
        todo_outcomes = _run_batch(
            todo_items,
            lambda item: _run_in_corpus_worker(
                proc_pool,
                item,
                target,
                password,
                options,
                overwrite,
                include_images=include_images,
                render_pages=render_pages,
            )[1],
            pool_size,
        )
    for idx, res in zip(ordered_todo, todo_outcomes, strict=True):
        outcomes[idx] = res
    return [outcomes[i] for i in range(len(items))]


def _run_batch(
    items: tuple[_SourceItem, ...],
    task: Callable[[_SourceItem], CorpusEntry | CorpusFailure],
    workers: int,
) -> list[CorpusEntry | CorpusFailure]:
    """Execute ``task`` across ``items`` with bounded concurrency ``workers``, preserving input order."""
    if workers <= 1 or len(items) <= 1:
        return [task(item) for item in items]
    outcomes: dict[int, CorpusEntry | CorpusFailure] = {}
    queued = iter(range(len(items)))
    running: dict[Future[CorpusEntry | CorpusFailure], int] = {}
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="datagrunt-corpus")
    try:
        for _ in range(len(items) + 1):
            for index in islice(queued, workers - len(running)):
                running[pool.submit(task, items[index])] = index
            if not running:
                break
            done, _pending = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                outcomes[running.pop(future)] = future.result()
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    return [outcomes[index] for index in range(len(items))]


def _process_item(
    item: _SourceItem,
    target: Path,
    password: str | None,
    options: PipelineOptions,
    overwrite: bool,
    write_bundle_fn: Callable[..., object],
    *,
    include_images: bool = True,
    render_pages: RenderPagesMode = True,
    worker_pool: PageWorkerPool | None = None,
) -> CorpusEntry | CorpusFailure:
    """Extract or reuse one PDF into ``target / item.doc_id``; isolate errors when ``options.strict`` is False."""
    bundle_relpath = item.doc_id
    bundle_dir = target / bundle_relpath
    extra_params: dict[str, object] = {
        "ocr": options.ocr,
        "min_image_dimension": options.min_image_dimension,
        "strict": options.strict,
        **({"orientation_fallback": True} if options.orientation_fallback else {}),
    }
    expected_params: dict[str, object] = {
        "dpi": options.dpi,
        "vision_dpi": options.vision_dpi,
        "max_render_pixels": options.max_render_pixels,
        "workers": options.workers,
        **extra_params,
    }
    if options.sandbox:
        expected_params["sandbox"] = True
    if not include_images:
        expected_params["include_images"] = False
    if render_pages is False:
        expected_params["render_pages"] = False
    elif render_pages == "vision_only":
        expected_params["render_pages"] = "vision_only"
    if not overwrite and bundle_dir.is_dir():
        reused = _reused_entry(bundle_dir, item, bundle_relpath, expected_params)
        if reused is not None:
            return reused
    try:
        checked_pdf_file(item.path, "source")
        extract_kwargs: dict[str, object] = {
            "password": password,
            "workers": options.workers,
            "ocr": options.ocr,
            "dpi": options.dpi,
            "vision_dpi": options.vision_dpi,
            "max_render_pixels": options.max_render_pixels,
            "min_image_dimension": options.min_image_dimension,
            "strict": options.strict,
            **({"sandbox": True} if options.sandbox else {}),
            **({"orientation_fallback": True} if options.orientation_fallback else {}),
        }
        if worker_pool is not None and extract_document is _default_extract_document:
            extract_kwargs["_worker_pool"] = worker_pool
        doc = extract_document(item.path, **extract_kwargs)  # type: ignore[arg-type]
        written = cast(
            PDFDocument,
            write_bundle_fn(
                doc,
                bundle_dir,
                source=item.path,
                password=password,
                dpi=options.dpi,
                vision_dpi=options.vision_dpi,
                max_render_pixels=options.max_render_pixels,
                workers=options.workers,
                extraction_params=extra_params,
                overwrite=True,
                include_images=include_images,
                render_pages=render_pages,
                **({"sandbox": True} if options.sandbox else {}),
            ),
        )
    except (DatagruntPDFError, OSError, ValueError) as exc:
        if options.strict:
            raise
        return CorpusFailure(
            source_path=str(item.path),
            error_type=type(exc).__name__,
            message=str(exc) or type(exc).__name__,
        )
    return CorpusEntry(
        doc_id=written.metadata.doc_id,
        source_name=written.metadata.source_name,
        source_sha256=written.metadata.sha256,
        bundle_relpath=bundle_relpath,
        page_count=written.metadata.page_count,
        integrity_verified=written.integrity_verified,
        degraded_pages=written.degraded_pages,
        reused=False,
    )


def _reused_entry(
    bundle_dir: Path,
    item: _SourceItem,
    bundle_relpath: str,
    expected_params: dict[str, object],
) -> CorpusEntry | None:
    """Return a ``reused=True`` entry if ``bundle_dir`` holds a valid matching bundle.

    Matching means this datagrunt's version and bundle format (an older format, 5.0, records no image-only
    reasons: #470), the item's source sha256, doc id and name, and the expected extraction parameters.
    """
    try:
        manifest, doc = _loaded(bundle_dir)
    except (BundleCorruptError, OSError, ValueError, TypeError):
        return None
    if manifest.datagrunt_version != datagrunt.__version__:
        return None
    if manifest.bundle_format_version != BUNDLE_FORMAT_VERSION:
        return None
    if manifest.source_sha256 != item.sha256 or not _same_output_params(manifest.extraction_params, expected_params):
        return None
    if doc.metadata.doc_id != item.doc_id or doc.metadata.source_name != item.source_name:
        return None
    return CorpusEntry(
        doc_id=doc.metadata.doc_id,
        source_name=doc.metadata.source_name,
        source_sha256=doc.metadata.sha256,
        bundle_relpath=bundle_relpath,
        page_count=doc.metadata.page_count,
        integrity_verified=doc.integrity_verified,
        degraded_pages=doc.degraded_pages,
        reused=True,
    )


def _write_corpus_manifest(target: Path, manifest: CorpusManifest) -> None:
    """Atomically write ``corpus_manifest.json`` into ``target``."""
    payload = {
        "corpus_format_version": manifest.corpus_format_version,
        "entries": [_entry_dict(e) for e in manifest.entries],
        "failures": [_failure_dict(f) for f in manifest.failures],
    }
    raw = json.dumps(payload, ensure_ascii=True, allow_nan=False, indent=2).encode("ascii")
    staging = target / f".tmp_{uuid.uuid4().hex}"
    try:
        with staging.open("xb") as handle:
            handle.write(raw)
        os.replace(staging, target / CORPUS_MANIFEST)
    except BaseException:
        if staging.exists():
            staging.unlink()
        raise


def _corpus_root(path: object) -> Path:
    """Resolve the corpus directory from ``path`` (directory or ``corpus_manifest.json``)."""
    given = checked_path(path, "path").absolute()
    if given.is_dir():
        root = given.resolve()
    elif given.name == CORPUS_MANIFEST and given.is_file():
        root = given.parent.resolve()
    elif not given.exists():
        raise FileNotFoundError(errno.ENOENT, "no corpus here", str(given))
    else:
        raise ValueError(f"{given} is not a corpus directory or its {CORPUS_MANIFEST}")
    if not (root / CORPUS_MANIFEST).exists():
        raise FileNotFoundError(errno.ENOENT, f"no {CORPUS_MANIFEST}: not a corpus directory", str(root))
    return root


def _read_corpus_manifest_bytes(root: Path) -> bytes:
    """Read ``corpus_manifest.json`` safely within the asset limit."""
    descriptor = _opened(root, CORPUS_MANIFEST)
    try:
        _regular(descriptor, CORPUS_MANIFEST)
        total, _digest, raw = _read_fd(descriptor, _MAX_ARTIFACT_BYTES, keep=True)
    finally:
        os.close(descriptor)
    if total > _MAX_ARTIFACT_BYTES:
        raise BundleCorruptError(f"{CORPUS_MANIFEST}: it is over the {_MAX_ARTIFACT_BYTES}-byte asset limit")
    return raw


def _corpus_manifest_of(data: object, root: Path) -> CorpusManifest:
    """Validate and build a :class:`CorpusManifest` from parsed JSON ``data``."""
    top = _object("corpus_manifest", data, _MANIFEST_KEYS)
    version = top["corpus_format_version"]
    if not isinstance(version, str) or version != CORPUS_FORMAT_VERSION:
        raise ValueError(f"corpus_format_version {version!r} is not supported: expected {CORPUS_FORMAT_VERSION!r}")
    entries = tuple(_entry_of(f"entries[{i}]", item, root) for i, item in enumerate(_array("entries", top["entries"])))
    failures = tuple(_failure_of(f"failures[{i}]", item) for i, item in enumerate(_array("failures", top["failures"])))
    return CorpusManifest(
        corpus_format_version=version,
        output_dir=root,
        entries=entries,
        failures=failures,
    )


def _entry_of(name: str, value: object, root: Path) -> CorpusEntry:
    """Validate one JSON entry object in ``corpus_manifest.json``."""
    data = _object(name, value, _ENTRY_KEYS)
    contained_path(f"{name}.bundle_relpath", data["bundle_relpath"], root)
    pages = tuple(
        int_at_least(f"{name}.degraded_pages[{i}]", p, 1)
        for i, p in enumerate(_array(f"{name}.degraded_pages", data["degraded_pages"]))
    )
    return CorpusEntry(
        doc_id=non_blank_str(f"{name}.doc_id", data["doc_id"]),
        source_name=non_blank_str(f"{name}.source_name", data["source_name"]),
        source_sha256=sha256_hex(f"{name}.source_sha256", data["source_sha256"]),
        bundle_relpath=non_blank_str(f"{name}.bundle_relpath", data["bundle_relpath"]),
        page_count=int_at_least(f"{name}.page_count", data["page_count"], 1),
        integrity_verified=strict_bool(f"{name}.integrity_verified", data["integrity_verified"]),
        degraded_pages=pages,
        reused=strict_bool(f"{name}.reused", data["reused"]),
    )


def _failure_of(name: str, value: object) -> CorpusFailure:
    """Validate one JSON failure object in ``corpus_manifest.json``."""
    data = _object(name, value, _FAILURE_KEYS)
    return CorpusFailure(
        source_path=non_blank_str(f"{name}.source_path", data["source_path"]),
        error_type=non_blank_str(f"{name}.error_type", data["error_type"]),
        message=non_blank_str(f"{name}.message", data["message"]),
    )


def _verified_entry_document(root: Path, entry: CorpusEntry) -> PDFDocument:
    """Verify containment of ``entry.bundle_relpath``, load its bundle, and verify its metadata matches ``entry``."""
    try:
        bundle_dir = contained_path("bundle_relpath", entry.bundle_relpath, root)
    except (TypeError, ValueError) as exc:
        raise BundleCorruptError(f"{CORPUS_MANIFEST}: {exc}") from exc
    try:
        doc = load_bundle(bundle_dir)
    except (FileNotFoundError, ValueError) as exc:
        raise BundleCorruptError(f"{entry.bundle_relpath}: {exc}") from exc
    if doc.metadata.doc_id != entry.doc_id or doc.metadata.sha256 != entry.source_sha256:
        raise BundleCorruptError(
            f"{CORPUS_MANIFEST}: entry {entry.doc_id!r} disagrees with bundle {entry.bundle_relpath!r}"
        )
    if doc.metadata.page_count != entry.page_count or doc.integrity_verified != entry.integrity_verified:
        raise BundleCorruptError(f"{CORPUS_MANIFEST}: entry {entry.doc_id!r} page/integrity facts disagree with bundle")
    return doc
