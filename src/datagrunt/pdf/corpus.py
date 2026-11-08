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
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Final, NamedTuple, cast

import datagrunt
from datagrunt.pdf._bundle_load import (
    _MAX_ARTIFACT_BYTES,
    BUNDLE_FORMAT_VERSION,
    _loaded,
    _opened,
    _read_fd,
    _regular,
    load_bundle,
)
from datagrunt.pdf._extra import load as _load_extra
from datagrunt.pdf._options import DPI, MAX_RENDER_PIXELS, MIN_IMAGE_DIMENSION, WORKERS, OcrMode
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
from datagrunt.pdf.pipeline import PageWorkerPool, PipelineOptions, extract_document

_default_extract_document = extract_document

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
    workers: int = WORKERS,
    ocr: OcrMode = "never",
    dpi: int = DPI,
    vision_dpi: int = 300,
    max_render_pixels: int = MAX_RENDER_PIXELS,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
    strict: bool = False,
    overwrite: bool = False,
    include_images: bool = True,
    render_pages: bool = True,
) -> CorpusManifest:
    """Extract a batch of PDFs into verified bundles under ``output_dir`` and write ``corpus_manifest.json``."""
    int_at_least("max_workers", max_workers, 1)
    if max_workers > _MAX_CORPUS_WORKERS:
        raise ValueError(f"max_workers must be <= {_MAX_CORPUS_WORKERS}, got {max_workers}")
    replace = strict_bool("overwrite", overwrite)
    checked_include_images = strict_bool("include_images", include_images)
    checked_render_pages = strict_bool("render_pages", render_pages)
    checked_pwd = checked_password(password)
    options = PipelineOptions(
        workers=workers,
        ocr=ocr,
        dpi=dpi,
        vision_dpi=vision_dpi,
        max_render_pixels=max_render_pixels,
        min_image_dimension=min_image_dimension,
        strict=strict,
    )
    target = _checked_corpus_dir(output_dir)
    items = _resolve_inputs(inputs)
    write_bundle_fn = _load_extra("write_bundle")
    target.mkdir(parents=True, exist_ok=True)
    with PageWorkerPool() as worker_pool:
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
                render_pages=checked_render_pages,
                worker_pool=worker_pool,
            ),
            min(max_workers, len(items)),
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
    render_pages: bool = True,
    worker_pool: PageWorkerPool | None = None,
) -> CorpusEntry | CorpusFailure:
    """Extract or reuse one PDF into ``target / item.doc_id``; isolate errors when ``options.strict`` is False."""
    bundle_relpath = item.doc_id
    bundle_dir = target / bundle_relpath
    extra_params: dict[str, object] = {
        "ocr": options.ocr,
        "min_image_dimension": options.min_image_dimension,
        "strict": options.strict,
    }
    expected_params: dict[str, object] = {
        "dpi": options.dpi,
        "vision_dpi": options.vision_dpi,
        "max_render_pixels": options.max_render_pixels,
        "workers": options.workers,
        **extra_params,
    }
    if not include_images:
        expected_params["include_images"] = False
    if not render_pages:
        expected_params["render_pages"] = False
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
    if manifest.source_sha256 != item.sha256 or manifest.extraction_params != expected_params:
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
        "entries": [
            {
                "doc_id": e.doc_id,
                "source_name": e.source_name,
                "source_sha256": e.source_sha256,
                "bundle_relpath": e.bundle_relpath,
                "page_count": e.page_count,
                "integrity_verified": e.integrity_verified,
                "degraded_pages": list(e.degraded_pages),
                "reused": e.reused,
            }
            for e in manifest.entries
        ],
        "failures": [
            {
                "source_path": f.source_path,
                "error_type": f.error_type,
                "message": f.message,
            }
            for f in manifest.failures
        ],
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
