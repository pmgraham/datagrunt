"""The v5 artifact bundle (#371, A7a): an atomic write and a verified load.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. A7b's
``PDFWriter.write_bundle`` and A8b's ``PDFReader`` stand on it.

Layout (paths relative to the bundle, joined with ``/``):

- ``json/document.json``: the document, by :mod:`datagrunt.pdf.codec`
  (ASCII; its paths relative to the bundle).
- ``markdown/document.md``: the document's Markdown, UTF-8, each lone
  surrogate written as its escape (:func:`~datagrunt.pdf.frame.utf8_text`);
  a figure links to ``../images/<sha256>.png``, relative to the file.
- ``images/<sha256>.png``: each distinct figure, once: its PNG from
  ``Figure.read_bytes`` (its ``image_path``, else its loader).
- ``page_images/page_NNNN.png``: every page's image (origin):
  - a page whose ``page_image`` is set (``"document"``: A5b's vision
    pages, rendered at the effective ``vision_dpi`` and normalized): its
    own PNG, from ``PageImage.read_bytes`` (its loader), as it is, at its
    own DPI; never rendered again;
  - every other page (``"render"``): rendered at
    :func:`~datagrunt.pdf.render.effective_dpi` of ``dpi`` (and
    ``max_render_pixels``) in A5a's guarded worker, 8-bit grey as drawn.
  A loaded document written again keeps each page image's origin: its
  reader carries the manifest's, so an earlier render is copied as a
  render (no bands expected), at its own DPI.
- ``page_images/bands/page_NNNN_band_NN.png``: a page's own image's bands,
  numbered from 1, from ``PageImage.read_bands`` (A5b's: the page as OCR
  read it, righted, which is what a vision model needs). A rendered page
  has no bands. ``vision_dpi`` is recorded, not rendered at: a vision
  page's own image carries its DPI.
- ``manifest.json``: :data:`BUNDLE_FORMAT_VERSION`, the datagrunt version,
  the source's sha256, the extraction parameters (``dpi``, ``vision_dpi``,
  ``max_render_pixels``, ``workers``, then the caller's
  ``extraction_params``), ``integrity_verified`` and ``degraded_pages``,
  each page image's origin, DPI and ``contrast_normalized`` (by page),
  every other file's path, size and sha256 (by path), and each artifact
  that is missing and why (by page).

PNGs are encoded with Pillow (the bundle's renders here; figures and own
page images by their loaders); PDF content is decoded only by A1's loaders
and A5a's guarded render. A figure's or own page image's bytes must start
with the PNG signature. Each file is created exclusively and streamed
through a SHA-256 as it is written.

:func:`write_bundle` is atomic. Every check comes first (the options, as
``PipelineOptions`` checks them; the document; ``output_dir``, which must
not exist or be an empty directory: a non-empty one, a file or a symlink is
a FileExistsError, never merged into or deleted; its parent; the source,
which must hash to the document's sha256). The bundle is then written into
a new staging directory, ``.tmp_<uuid4 hex>`` in ``output_dir``'s parent
(the same filesystem), ``output_dir`` is checked again and the staging
directory renamed onto it (``os.replace``). On any failure, an interrupt
included, the staging directory is removed and the error raised. A writer
killed outright leaves its staging directory, the only trace: no
``output_dir``; the next write makes its own and ignores it.

Pages are written in chunks of :func:`_chunk_size` (``max(4, 32 //
workers)``) pages, on ``workers`` threads (each render is a worker process
of A5a's, as is each own page image's loader). Each page's PNGs are written
in its own thread, so no page's pixels outlive it; a chunk is done before
the next starts.

The returned document is the input with paths set: each figure's
``image_path`` (in ``sections`` too), and each page's ``page_image``. A
page's own image keeps its ``dpi``, size and ``contrast_normalized`` (so it
equals the input's) and gains its paths; a rendered page's image is the
render's (its effective ``dpi`` and size, ``contrast_normalized`` False, no
bands). Its figures and page images read their bundle files as a loaded
document's do (verified on every read; see below).

Failures are recorded, never silent: a page whose render is refused or
fails (``RenderRefused``, ``RenderFailed``), or whose own image cannot be
read (a ``DatagruntPDFError``, render errors included, or an ``OSError``)
or is not a PNG, has no page image (nor bands); a page whose own bands
cannot be read, or one of them is not a PNG, keeps its image and has no
bands; a figure whose bytes cannot be read or are not a PNG is not written
(its figures keep no ``image_path``). Each is a ``missing`` entry naming
the artifact, the page, the figure's element id (figures) and why. Any
other error fails the write.

The page images are bound to the hashed source. The source's identity
(A8a's ``SourceIdentity``: device, inode, size, mtime) is taken with its
hash, and checked again once every artifact is written, before the rename:
a source changed, replaced or removed meanwhile raises
``PDFSourceChangedError`` and nothing is published (the staging directory
is removed). A page's own image is written only when its loaders are bound
to that file (:func:`_unbound`): each renders from ``source`` itself
(A5b's ``PageRender``, compared by resolved path) or is a bundle's
verifying reader. One that renders from another file, names no source, or
an image with no loader is not read: the page has no page image, a
``missing`` entry says why, and a ``UserWarning`` is emitted once the
bundle is published. What this cannot see is the A8a limit: a write that
keeps the size and restores the mtime.

Known limitations, documented:

- No ``fsync`` before the rename: a write survives a killed process, not
  a power loss; a bundle torn by one fails its checksums on load.
- ``output_dir`` filled by another process between the publish check and
  the rename makes ``os.replace`` fail with ``OSError`` (``ENOTEMPTY``),
  not ``FileExistsError``; it is never merged into, and staging is removed.
- Figures are read one loader call each (each reopens the PDF), not
  batched by page through one handle.

:func:`load_bundle` takes a bundle directory or its ``manifest.json``
and returns the document. Nothing is trusted before it is checked:

1. The manifest (opened as an artifact is, read within the asset limit,
   strict JSON): its version (:data:`BUNDLE_FORMAT_VERSION`), its keys
   (exactly, at every level) and values; every artifact path through
   :func:`~datagrunt.pdf.codec.contained_path` (plain relative parts,
   resolved inside the bundle: no ``..``, absolute path or symlink out),
   listed once, ``json/document.json`` and ``markdown/document.md`` among
   them, each no larger than the asset limit.
2. Every artifact, opened safely (:func:`_opened`): its path through
   ``contained_path`` again, no directory on it below the bundle a symlink,
   and the file opened with ``O_NOFOLLOW`` (a symlink is refused) and
   ``O_NONBLOCK`` (a named pipe cannot block the open). ``fstat`` of that
   descriptor must show a regular file of the manifest's size; its bytes
   are read from the same descriptor, in chunks, at most that size plus one
   byte, and must hash to the manifest's sha256. ``json/document.json`` is
   decoded from the bytes verified, not read again.
3. The document (the codec, its paths joined to the bundle by the same
   helper): the manifest's source sha256, ``integrity_verified`` and
   ``degraded_pages`` must be its; every path it names must be a verified
   artifact; the manifest's page images must be its (the same pages, DPI
   and ``contrast_normalized``; a rendered one without bands); and its
   missing page images, own bands and figures must be exactly the
   manifest's ``missing`` entries.

What is guaranteed: every byte a loaded document returns matches the
manifest at the moment it is read. Its figures and page images keep their
paths (for information: a Markdown link, a caller's own use), and read
through loaders (:class:`_ArtifactReader`, :class:`_BandsReader`; A1 reads
a loader before a path) that repeat step 2 on every read against the
manifest's size and sha256. A bundle changed after it was loaded (a file
rewritten, replaced by a symlink or a named pipe, deleted) fails loud on
the next read, never silently. The document :func:`write_bundle` returns
reads the same way. A figure has no raw loader once loaded: the bundle
holds its PNG, not its raw stream.

A failure is a :class:`~datagrunt.pdf.exceptions.BundleCorruptError` naming
the artifact. Nothing is held open: each read opens and closes its file, so
there is nothing to close.
"""

import dataclasses
import errno
import hashlib
import importlib.metadata
import json
import math
import os
import shutil
import stat
import uuid
import warnings
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, BinaryIO, NamedTuple, cast

from PIL import Image

from datagrunt.pdf._validate import (
    choice_str,
    int_at_least,
    non_blank_str,
    optional_str,
    positive_float,
    sha256_hex,
    strict_bool,
)
from datagrunt.pdf.codec import (
    _array,
    _object,
    contained_path,
    document_from_json_bytes,
    document_to_json_bytes,
    parse_json,
)
from datagrunt.pdf.elements import _MAX_ASSET_BYTES, Figure, Page, PageImage, PDFDocument
from datagrunt.pdf.exceptions import BundleCorruptError, DatagruntPDFError, PDFSourceChangedError
from datagrunt.pdf.frame import utf8_text
from datagrunt.pdf.pdfium_engine import _checked_path
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, SourceIdentity, _opened_source, _stream_sha256
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page

__all__ = ["BUNDLE_FORMAT_VERSION", "load_bundle", "write_bundle"]

BUNDLE_FORMAT_VERSION = "5.0"
MANIFEST = "manifest.json"
DOCUMENT_JSON = "json/document.json"
DOCUMENT_MARKDOWN = "markdown/document.md"

_STAGING_PREFIX = ".tmp_"
# A render worker's memory past its start, and its time: A5b's vision route's.
_MEMORY_CAP_BYTES = 2 << 30
_RENDER_TIMEOUT_S = 120.0
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

# The most bytes of any one file a load reads (the manifest, an artifact): A1's asset limit.
_MAX_ARTIFACT_BYTES = _MAX_ASSET_BYTES
_READ_CHUNK_BYTES = 1 << 20
# How every bundle file is opened: never through a symlink (O_NOFOLLOW), never blocking on a named pipe
# (O_NONBLOCK, which a regular file ignores), as bytes (O_BINARY, Windows). Each flag is 0 where it does not exist.
_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
_MANIFEST_KEYS = (
    "bundle_format_version",
    "datagrunt_version",
    "source_sha256",
    "extraction_params",
    "integrity_verified",
    "degraded_pages",
    "page_images",
    "artifacts",
    "missing",
)
_PARAM_KEYS = ("dpi", "vision_dpi", "max_render_pixels", "workers")
_MISSING_ARTIFACTS = ("bands", "figure", "page_image")
# Where a page image came from: the document's own (its loaders), or the bundle's render at dpi.
_ORIGINS = ("document", "render")


class _Artifact(NamedTuple):
    """A file the write put in the bundle: its relative path, size and sha256."""

    path: str
    size: int
    sha256: str


class _Missing(NamedTuple):
    """An artifact the write could not make, and why."""

    artifact: str
    page_number: int
    element_id: str | None
    reason: str


class _Written(NamedTuple):
    """A page image, written: where it came from, its PageImage's facts and relative paths."""

    page_number: int
    origin: str
    dpi: float
    width_px: int
    height_px: int
    contrast_normalized: bool
    image: str
    bands: tuple[str, ...]


class _PageOutcome(NamedTuple):
    """What one page's render task wrote, what it could not, and a warning for the caller ("" for none)."""

    written: _Written | None
    artifacts: tuple[_Artifact, ...]
    missing: tuple[_Missing, ...]
    warning: str = ""


@dataclass
class _Ledger:
    """What one write put in its staging directory, and what it could not (local to the write)."""

    artifacts: list[_Artifact] = field(default_factory=list)
    missing: list[_Missing] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class _Sink:
    """A write-only stream into an open file that hashes and counts what passes through it.

    It has no ``fileno``: Pillow then writes through :meth:`write`, not to
    the descriptor behind it.
    """

    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle
        self._digest = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:
        written = self._handle.write(data)
        self._digest.update(data)
        self.size += len(data)
        return written

    def flush(self) -> None:
        self._handle.flush()

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def write_bundle(
    document: PDFDocument,
    output_dir: str | os.PathLike[str],
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    dpi: int = 200,
    vision_dpi: int = 300,
    max_render_pixels: int = 16_000_000,
    workers: int = 1,
    extraction_params: Mapping[str, object] | None = None,
) -> PDFDocument:
    """Write ``document``, extracted from ``source``, as a bundle at ``output_dir``, atomically.

    See the module notes for the layout, the checks, the atomic write, the
    chunks and the failures recorded. ``password`` opens ``source`` for the
    renders. ``extraction_params`` are the document's other extraction
    parameters (``ocr``, ``min_image_dimension``, ``strict``: what A7b's
    ``PDFWriter`` extracted with), recorded in the manifest after the
    writer's own (see :func:`_checked_params`).

    Returns:
        ``document`` with paths set (see the module notes).

    Raises:
        TypeError, ValueError: If an option is invalid (as ``PipelineOptions``
            raises it), an extraction parameter is (:func:`_checked_params`),
            ``document`` is not a PDFDocument, ``output_dir`` or ``source`` is
            not a path, or ``source`` does not hash to the document's sha256.
        FileExistsError: If ``output_dir`` exists and is not an empty
            directory (a symlink is refused too), before or after the write.
        FileNotFoundError: If ``source`` or ``output_dir``'s parent is missing.
        PDFSourceChangedError: If ``source`` changed (or was removed) between
            its hash and the publish: the bundle is not published.
        OSError: If a file cannot be written, or the rename fails.

    Warns:
        UserWarning: For each page whose own image is not written because its
            loaders are not bound to ``source`` (see the module notes).
    """
    options = PipelineOptions(workers=workers, dpi=dpi, vision_dpi=vision_dpi, max_render_pixels=max_render_pixels)
    params = _checked_params(options, extraction_params)
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    target = _checked_target(output_dir)
    pdf = PDFSource(path=_checked_path(source, "source"), password=password)
    identity = _check_source(pdf, document.metadata.sha256)
    staging = target.parent / f"{_STAGING_PREFIX}{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        written, notices = _staged(document, staging, target, pdf, options, params)
        _check_unchanged(pdf, identity)
        _refuse_existing(target)
        os.replace(staging, target)
    except BaseException:
        shutil.rmtree(staging)
        raise
    for notice in notices:
        warnings.warn(notice, UserWarning, stacklevel=2)
    return written


def _checked_params(options: PipelineOptions, extra: object) -> dict[str, object]:
    """The manifest's ``extraction_params``: the writer's own, then ``extra``'s, each a JSON scalar.

    Raises:
        TypeError: If ``extra`` is not a Mapping or None, a key is not a str,
            or a value is not a str, int, float, bool or None.
        ValueError: If a key is blank or one of the writer's own, or a float
            is not finite.
    """
    params: dict[str, object] = {name: getattr(options, name) for name in _PARAM_KEYS}
    if extra is None:
        return params
    if not isinstance(extra, Mapping):
        raise TypeError(f"extraction_params must be a Mapping or None, got {type(extra).__name__}")
    for key, value in extra.items():
        if not isinstance(key, str):
            raise TypeError(f"extraction_params keys must be str, got {type(key).__name__}")
        if not key.strip():
            raise ValueError(f"extraction_params keys must be non-blank, got {key!r}")
        if key in _PARAM_KEYS:
            raise ValueError(f"extraction_params[{key!r}] is write_bundle's own parameter: pass it as {key}=")
        params[key] = _json_scalar(f"extraction_params[{key!r}]", value)
    return params


def _json_scalar(name: str, value: object) -> object:
    """``value`` if it is a JSON scalar: a str, int, finite float, bool or None."""
    if value is None or isinstance(value, (str, int)):
        return value
    if not isinstance(value, float):
        raise TypeError(f"{name} must be a str, int, float, bool or None, got {type(value).__name__}")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    return value


def _checked_target(output_dir: object) -> Path:
    """``output_dir`` made absolute, if it may be written: absent or an empty directory, in an existing one."""
    if not isinstance(output_dir, (str, os.PathLike)):
        raise TypeError(f"output_dir must be a str or os.PathLike, got {type(output_dir).__name__}")
    target = Path(output_dir).absolute()
    _refuse_existing(target)
    if not target.parent.is_dir():
        raise FileNotFoundError(errno.ENOENT, "output_dir's parent is not an existing directory", str(target.parent))
    return target


def _refuse_existing(target: Path) -> None:
    """Raise FileExistsError unless ``target`` is absent or an empty directory (a symlink is refused)."""
    if target.is_symlink():
        raise FileExistsError(errno.EEXIST, "output_dir is a symlink: a bundle never replaces one", str(target))
    if not target.exists():
        return
    if not target.is_dir():
        raise FileExistsError(errno.EEXIST, "output_dir exists and is not a directory", str(target))
    with os.scandir(target) as entries:
        if next(entries, None) is not None:
            message = "output_dir exists and is not empty: a bundle is never merged into it"
            raise FileExistsError(errno.EEXIST, message, str(target))


def _check_source(source: PDFSource, expected: str) -> SourceIdentity:
    """The identity of the source, taken with its hash; a source whose bytes are not the document's is refused."""
    with _opened_source(source.path) as stream:
        identity = SourceIdentity.of(os.fstat(stream.fileno()))
        digest = _stream_sha256(stream, identity.size, source.path)
    if digest != expected:
        raise ValueError(
            f"source {source.path} has sha256 {digest}, not the document's {expected}: "
            "its renders would not be the document's pages"
        )
    return identity


def _check_unchanged(source: PDFSource, identity: SourceIdentity) -> None:
    """Refuse to publish if the source is not the file hashed: its page images could be of other bytes."""
    try:
        now = SourceIdentity.of(os.stat(source.path))
    except FileNotFoundError as exc:
        raise PDFSourceChangedError(f"{source.path} was removed while its bundle was written") from exc
    changes = identity.changes(now)
    if changes:
        raise PDFSourceChangedError(
            f"{source.path} changed while its bundle was written: {', '.join(changes)}; "
            "its page images and its sha256 could disagree, so the bundle is not published"
        )


def _staged(
    document: PDFDocument,
    staging: Path,
    target: Path,
    source: PDFSource,
    options: PipelineOptions,
    params: Mapping[str, object],
) -> tuple[PDFDocument, tuple[str, ...]]:
    """Write every artifact, then the manifest, into ``staging``: the document (paths in ``target``), and warnings."""
    ledger = _Ledger()
    figures = _write_figures(document, staging, ledger)
    renders = _write_renders(document, staging, source, options, ledger)
    as_json = document_to_json_bytes(_with_paths(document, figures, renders, Path()))
    ledger.artifacts.append(_write_bytes(staging, DOCUMENT_JSON, as_json))
    markdown = utf8_text(_with_paths(document, figures, renders, Path("..")).to_markdown())
    ledger.artifacts.append(_write_bytes(staging, DOCUMENT_MARKDOWN, markdown.encode("utf-8")))
    _write_bytes(staging, MANIFEST, _manifest(document, params, renders, ledger))
    by_path = {artifact.path: artifact for artifact in ledger.artifacts}
    origins = {number: written.origin for number, written in renders.items()}
    returned = _with_readers(_with_paths(document, figures, renders, target), target, by_path, origins)
    return returned, tuple(ledger.warnings)


def _write_figures(document: PDFDocument, staging: Path, ledger: _Ledger) -> dict[str, str]:
    """Write each distinct figure (by sha256) once; its relative path by sha256."""
    written: dict[str, str] = {}
    tried: set[str] = set()
    for figure in document.figures:
        if figure.sha256 in tried:
            continue
        tried.add(figure.sha256)
        data, why = _png(figure.read_bytes)
        if data is None:
            reason = f"figure {figure.sha256} was not written: {why}"
            ledger.missing.append(_Missing("figure", figure.page_number, figure.element_id, reason))
            continue
        relative = f"images/{figure.sha256}.png"
        ledger.artifacts.append(_write_bytes(staging, relative, data))
        written[figure.sha256] = relative
    return written


def _png(read: Callable[[], bytes]) -> tuple[bytes | None, str]:
    """The PNG bytes ``read`` returns (a figure's or a page image's), or None and why they cannot be read."""
    try:
        data = read()
    except (DatagruntPDFError, OSError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not data.startswith(_PNG_SIGNATURE):
        return None, "its bytes are not a PNG"
    return data, ""


def _chunk_size(workers: int) -> int:
    """Pages a chunk holds: the spec's ``max(4, 32 // workers)``."""
    return max(4, 32 // workers)


def _chunks(page_count: int, workers: int) -> tuple[range, ...]:
    """The page indices, in order, in runs of :func:`_chunk_size` (the last may be shorter)."""
    size = _chunk_size(workers)
    return tuple(range(start, min(start + size, page_count)) for start in range(0, page_count, size))


def _write_renders(
    document: PDFDocument, staging: Path, source: PDFSource, options: PipelineOptions, ledger: _Ledger
) -> dict[int, _Written]:
    """Write every page's image, chunk by chunk on ``workers`` threads; each page image written, by number."""
    renders: dict[int, _Written] = {}
    pages = document.pages

    def task(page: Page) -> _PageOutcome:
        return _page_outcome(page, staging, source, options)

    pool = ThreadPoolExecutor(max_workers=options.workers, thread_name_prefix="datagrunt-bundle")
    try:
        for chunk in _chunks(len(pages), options.workers):
            for outcome in pool.map(task, [pages[index] for index in chunk]):
                _record(outcome, renders, ledger)
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    return renders


def _record(outcome: _PageOutcome, renders: dict[int, _Written], ledger: _Ledger) -> None:
    ledger.artifacts.extend(outcome.artifacts)
    ledger.missing.extend(outcome.missing)
    if outcome.written is not None:
        renders[outcome.written.page_number] = outcome.written
    if outcome.warning:
        ledger.warnings.append(outcome.warning)


def _page_outcome(page: Page, staging: Path, source: PDFSource, options: PipelineOptions) -> _PageOutcome:
    """Write the page's own image and bands when it has one, else render it at ``dpi``; a failure is recorded."""
    if page.page_image is not None:
        return _own_outcome(page.page_image, staging, source)
    return _rendered_outcome(page, staging, source, options)


def _rendered_outcome(page: Page, staging: Path, source: PDFSource, options: PipelineOptions) -> _PageOutcome:
    """Render the page at ``dpi`` and write its PNG (no bands); a refusal or failure is recorded."""
    number = page.page_number
    try:
        dpi, drawn = _drawn(source, page, options.dpi, options.max_render_pixels)
    except (RenderRefused, RenderFailed) as exc:
        reason = f"the page was not rendered: {type(exc).__name__}: {exc}"
        return _PageOutcome(None, (), (_Missing("page_image", number, None, reason),))
    image = f"page_images/page_{number:04d}.png"
    artifact = _write_png(staging, image, drawn)
    written = _Written(number, "render", float(dpi), drawn.px_width, drawn.px_height, False, image, ())
    return _PageOutcome(written, (artifact,), ())


def _own_outcome(image: PageImage, staging: Path, source: PDFSource) -> _PageOutcome:
    """Write the page's own image and bands (A5b's vision render) through its loaders, as they are.

    Only when they are bound to the hashed source (:func:`_unbound`): else no
    image is written, nor read, and the page gets a missing entry and a warning.
    """
    number = image.page_number
    unbound = _unbound(image, source.path)
    if unbound:
        reason = f"the page's own image {unbound}: its bytes could not be bound to the hashed source"
        notice = f"page {number}'s own image {unbound}: no page image is written for it"
        return _PageOutcome(None, (), (_Missing("page_image", number, None, reason),), notice)
    data, why = _png(image.read_bytes)
    if data is None:
        reason = f"the page's own image was not written: {why}"
        return _PageOutcome(None, (), (_Missing("page_image", number, None, reason),))
    path = f"page_images/page_{number:04d}.png"
    artifact = _write_bytes(staging, path, data)
    del data  # written: it need not outlive the page's bands
    origin = _origin_of(image)
    band_artifacts, missing = _own_bands(image, staging) if origin == "document" else ((), ())
    bands = tuple(item.path for item in band_artifacts)
    facts = (image.dpi, image.width_px, image.height_px, image.contrast_normalized)
    written = _Written(number, origin, *facts, path, bands)
    return _PageOutcome(written, (artifact, *band_artifacts), missing)


def _origin_of(image: PageImage) -> str:
    """``"render"`` for a page image an earlier bundle rendered (read back by its reader), else ``"document"``.

    A rendered image has no bands, so none is expected of it: it is written
    again as the render it is, at its own DPI.
    """
    loader = image._loader
    return "render" if isinstance(loader, _ArtifactReader) and loader.origin == "render" else "document"


def _unbound(image: PageImage, source: Path) -> str:
    """Why the page image's bytes are not bound to ``source``, or ``""`` if they are.

    Its image must come from a loader (a bare path is read unverified). Each
    loader it has must render from ``source`` itself, compared by resolved
    path (A5b's ``PageRender`` names the PDF it renders from as ``source``),
    or be a bundle's verifying reader (its bytes are checked against a
    manifest whose source sha256 is the document's). Any other loader names
    no source, so nothing ties it to the hashed bytes.
    """
    if image._loader is None:
        return "has no loader"
    target = source.resolve()
    for loader in (image._loader, image._band_loader):
        owner = getattr(loader, "__self__", loader)
        if loader is None or isinstance(owner, (_ArtifactReader, _BandsReader)):
            continue
        origin = getattr(owner, "source", None)
        if not isinstance(origin, (str, os.PathLike)):
            return "has a loader that names no source"
        resolved = Path(origin).resolve()
        if resolved != target:
            return f"renders from {resolved}, not from the source {target}"
    return ""


def _own_bands(image: PageImage, staging: Path) -> tuple[tuple[_Artifact, ...], tuple[_Missing, ...]]:
    """Write the page image's own bands (OCR's righted page), each a PNG; a failure is recorded, no band written."""
    number = image.page_number
    why = ""
    try:
        cut = image.read_bands()
    except (DatagruntPDFError, OSError) as exc:
        cut, why = (), f"{type(exc).__name__}: {exc}"
    not_png = [index for index, band in enumerate(cut, start=1) if not band.startswith(_PNG_SIGNATURE)]
    if not_png:
        why = f"band {not_png[0]}'s bytes are not a PNG"
    if why:
        return (), (_Missing("bands", number, None, f"the page's own bands were not written: {why}"),)
    written = tuple(
        _write_bytes(staging, f"page_images/bands/page_{number:04d}_band_{index:02d}.png", band)
        for index, band in enumerate(cut, start=1)
    )
    return written, ()


def _drawn(source: PDFSource, page: Page, target_dpi: int, max_pixels: int) -> tuple[int, RenderedPage]:
    """The page drawn at the effective DPI of ``target_dpi`` in A5a's guarded worker, and that DPI."""
    dpi = effective_dpi(target_dpi, max_pixels, page.width_pt, page.height_pt)
    drawn = render_page(
        source.path,
        page.page_number - 1,
        dpi=dpi,
        password=source.password,
        memory_cap_bytes=_MEMORY_CAP_BYTES,
        timeout_s=_RENDER_TIMEOUT_S,
    )
    return dpi, drawn


def _write_png(staging: Path, relative: str, image: RenderedPage) -> _Artifact:
    """Write 8-bit grey pixels as a PNG, encoded by Pillow straight into the file."""
    picture = Image.frombytes("L", (image.px_width, image.px_height), image.pixels)
    # Pillow needs only write() of a file object: a cast, not a conversion.
    return _write_stream(staging, relative, lambda sink: picture.save(cast(IO[bytes], sink), format="PNG"))


def _write_bytes(staging: Path, relative: str, data: bytes) -> _Artifact:
    return _write_stream(staging, relative, lambda sink: sink.write(data))


def _write_stream(staging: Path, relative: str, fill: Callable[[_Sink], object]) -> _Artifact:
    """Create the file at ``relative`` (exclusively) and let ``fill`` write it through a hashing sink."""
    path = staging.joinpath(*relative.split("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        sink = _Sink(handle)
        fill(sink)
    return _Artifact(relative, sink.size, sink.hexdigest())


def _with_paths(
    document: PDFDocument, figures: Mapping[str, str], renders: Mapping[int, _Written], base: Path
) -> PDFDocument:
    """``document`` with its figures' and pages' paths at ``base`` (sections hold the same figures)."""
    pages = tuple(_page_with_paths(page, figures, renders.get(page.page_number), base) for page in document.pages)
    return _with_pages(document, pages)


def _with_pages(document: PDFDocument, pages: tuple[Page, ...]) -> PDFDocument:
    """``document`` with ``pages`` (the same elements, rebuilt), its sections holding the new elements."""
    by_id = {element.element_id: element for page in pages for element in page.elements}
    sections = tuple(
        dataclasses.replace(section, elements=tuple(by_id[element.element_id] for element in section.elements))
        for section in document.sections
    )
    return dataclasses.replace(document, pages=pages, sections=sections)


def _page_with_paths(page: Page, figures: Mapping[str, str], written: _Written | None, base: Path) -> Page:
    """The page with its figures' paths and the page image written (None if none was), its paths at ``base``."""
    elements = tuple(
        _figure_with_path(element, figures, base) if isinstance(element, Figure) else element
        for element in page.elements
    )
    image = None
    if written is not None:
        image = PageImage(
            page_number=written.page_number,
            dpi=written.dpi,
            width_px=written.width_px,
            height_px=written.height_px,
            image_path=_joined(base, written.image),
            band_paths=tuple(_joined(base, band) for band in written.bands),
            contrast_normalized=written.contrast_normalized,
        )
    return dataclasses.replace(page, elements=elements, page_image=image)


def _figure_with_path(figure: Figure, figures: Mapping[str, str], base: Path) -> Figure:
    relative = figures.get(figure.sha256)
    return dataclasses.replace(figure, image_path=None if relative is None else _joined(base, relative))


def _joined(base: Path, relative: str) -> Path:
    return base.joinpath(*relative.split("/"))


def _datagrunt_version() -> str:
    return importlib.metadata.version("datagrunt")


def _missing_order(entry: _Missing) -> tuple[int, str, str]:
    return entry.page_number, entry.artifact, entry.element_id or ""


def _manifest(
    document: PDFDocument, params: Mapping[str, object], renders: Mapping[int, _Written], ledger: _Ledger
) -> bytes:
    """The manifest's bytes: ASCII JSON (see the module notes)."""
    images = [
        {"page_number": n, "origin": w.origin, "dpi": w.dpi, "contrast_normalized": w.contrast_normalized}
        for n, w in sorted(renders.items())
    ]
    data = {
        "bundle_format_version": BUNDLE_FORMAT_VERSION,
        "datagrunt_version": _datagrunt_version(),
        "source_sha256": document.metadata.sha256,
        "extraction_params": dict(params),
        "integrity_verified": document.integrity_verified,
        "degraded_pages": list(document.degraded_pages),
        "page_images": images,
        "artifacts": [entry._asdict() for entry in sorted(ledger.artifacts)],
        "missing": [entry._asdict() for entry in sorted(ledger.missing, key=_missing_order)],
    }
    return json.dumps(data, ensure_ascii=True, allow_nan=False, indent=2).encode("ascii")


class _ImageEntry(NamedTuple):
    """A manifest's record of one page image: where it came from, its DPI and normalization."""

    page_number: int
    origin: str
    dpi: float
    contrast_normalized: bool


class _Manifest(NamedTuple):
    """A manifest's values, checked (see the module notes)."""

    source_sha256: str
    integrity_verified: bool
    degraded_pages: tuple[int, ...]
    page_images: tuple[_ImageEntry, ...]
    artifacts: tuple[_Artifact, ...]
    missing: tuple[_Missing, ...]


def load_bundle(path: str | os.PathLike[str]) -> PDFDocument:
    """The document of the bundle at ``path`` (its directory or its ``manifest.json``), verified.

    See the module notes for every check.

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike.
        FileNotFoundError: If ``path`` does not exist, or the directory has
            no ``manifest.json``.
        ValueError: If ``path`` is a file other than a ``manifest.json``.
        BundleCorruptError: If the bundle is not what its manifest says
            (naming the artifact).
    """
    root = _bundle_root(path)
    manifest = _read_manifest(root)
    raw = b""
    for entry in manifest.artifacts:
        if entry.path == DOCUMENT_JSON:
            raw = _verified(root, entry, keep=True)
        else:
            _verified(root, entry, keep=False)
    try:
        document = document_from_json_bytes(raw, base_dir=root)
    except (TypeError, ValueError) as exc:
        raise BundleCorruptError(f"{DOCUMENT_JSON}: {exc}") from exc
    by_path = {entry.path: entry for entry in manifest.artifacts}
    _check_facts(document, manifest)
    _check_paths(document, set(by_path), root)
    _check_page_images(document, manifest.page_images)
    origins = {entry.page_number: entry.origin for entry in manifest.page_images}
    _check_missing(document, manifest.missing, origins)
    return _with_readers(document, root, by_path, origins)


def _bundle_root(path: object) -> Path:
    """The bundle directory ``path`` names (itself, or a ``manifest.json``'s), made absolute."""
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError(f"path must be a str or os.PathLike, got {type(path).__name__}")
    given = Path(path).absolute()
    if given.is_dir():
        root = given
    elif given.name == MANIFEST and given.is_file():
        root = given.parent
    elif not given.exists():
        raise FileNotFoundError(errno.ENOENT, "no bundle here", str(given))
    else:
        raise ValueError(f"{given} is not a bundle directory or its {MANIFEST}")
    if not (root / MANIFEST).exists():
        raise FileNotFoundError(errno.ENOENT, f"no {MANIFEST}: not a bundle directory", str(root))
    return root


def _read_manifest(root: Path) -> _Manifest:
    """The manifest, read within the asset limit and checked; any fault is a BundleCorruptError naming it."""
    descriptor = _opened(root, MANIFEST)
    try:
        _regular(descriptor, MANIFEST)
        total, _digest, raw = _read_fd(descriptor, _MAX_ARTIFACT_BYTES, keep=True)
    finally:
        os.close(descriptor)
    if total > _MAX_ARTIFACT_BYTES:
        raise BundleCorruptError(f"{MANIFEST}: it is over the {_MAX_ARTIFACT_BYTES}-byte asset limit")
    try:
        return _manifest_of(parse_json(raw, MANIFEST), root)
    except (TypeError, ValueError) as exc:
        raise BundleCorruptError(f"{MANIFEST}: {exc}") from exc


def _manifest_of(data: object, root: Path) -> _Manifest:
    """The manifest's values, each checked: TypeError or ValueError naming the first that is wrong."""
    top = _object("manifest", data, _MANIFEST_KEYS)
    version = top["bundle_format_version"]
    if not isinstance(version, str) or version != BUNDLE_FORMAT_VERSION:
        message = f"bundle_format_version {version!r} is not supported: this datagrunt reads {BUNDLE_FORMAT_VERSION!r}"
        raise ValueError(message)
    non_blank_str("datagrunt_version", top["datagrunt_version"])
    _check_params(top["extraction_params"])
    pages = enumerate(_array("degraded_pages", top["degraded_pages"]))
    missing = enumerate(_array("missing", top["missing"]))
    return _Manifest(
        source_sha256=sha256_hex("source_sha256", top["source_sha256"]),
        integrity_verified=strict_bool("integrity_verified", top["integrity_verified"]),
        degraded_pages=tuple(int_at_least(f"degraded_pages[{i}]", page, 1) for i, page in pages),
        page_images=_image_entries(top["page_images"]),
        artifacts=_listed_artifacts(top["artifacts"], root),
        missing=tuple(_missing_entry(f"missing[{i}]", item) for i, item in missing),
    )


def _check_params(value: object) -> None:
    """The writer's own parameters, as ``PipelineOptions`` checks them, then any other as a JSON scalar."""
    if not isinstance(value, dict):
        raise TypeError(f"extraction_params must be a JSON object, got {type(value).__name__}")
    missing = [key for key in _PARAM_KEYS if key not in value]
    if missing:
        raise ValueError(f"extraction_params has missing key(s) {missing}")
    PipelineOptions(**{key: value[key] for key in _PARAM_KEYS})
    for key, item in value.items():
        if key not in _PARAM_KEYS:
            _json_scalar(f"extraction_params[{key!r}]", item)


def _image_entries(value: object) -> tuple[_ImageEntry, ...]:
    """The manifest's page images: each checked, each page once."""
    entries: list[_ImageEntry] = []
    seen: set[int] = set()
    for index, item in enumerate(_array("page_images", value)):
        name = f"page_images[{index}]"
        data = _object(name, item, _ImageEntry._fields)
        entry = _ImageEntry(
            page_number=int_at_least(f"{name}.page_number", data["page_number"], 1),
            origin=choice_str(f"{name}.origin", data["origin"], _ORIGINS),
            dpi=positive_float(f"{name}.dpi", data["dpi"]),
            contrast_normalized=strict_bool(f"{name}.contrast_normalized", data["contrast_normalized"]),
        )
        if entry.page_number in seen:
            raise ValueError(f"page_images lists page {entry.page_number} twice")
        seen.add(entry.page_number)
        entries.append(entry)
    return tuple(entries)


def _listed_artifacts(value: object, root: Path) -> tuple[_Artifact, ...]:
    """The manifest's artifacts: each checked, each path once, the document's JSON and Markdown among them."""
    listed: list[_Artifact] = []
    seen: set[str] = set()
    for index, item in enumerate(_array("artifacts", value)):
        artifact = _artifact_entry(f"artifacts[{index}]", item, root)
        if artifact.path in seen:
            raise ValueError(f"artifacts lists {artifact.path!r} twice")
        seen.add(artifact.path)
        listed.append(artifact)
    for required in (DOCUMENT_JSON, DOCUMENT_MARKDOWN):
        if required not in seen:
            raise ValueError(f"artifacts lists no {required}")
    return tuple(listed)


def _artifact_entry(name: str, value: object, root: Path) -> _Artifact:
    data = _object(name, value, _Artifact._fields)
    contained_path(f"{name}.path", data["path"], root)
    size = int_at_least(f"{name}.size", data["size"], 0)
    if size > _MAX_ARTIFACT_BYTES:
        raise ValueError(f"{name} ({data['path']!r}): size {size} is over the {_MAX_ARTIFACT_BYTES}-byte asset limit")
    return _Artifact(path=data["path"], size=size, sha256=sha256_hex(f"{name}.sha256", data["sha256"]))


def _missing_entry(name: str, value: object) -> _Missing:
    data = _object(name, value, _Missing._fields)
    return _Missing(
        artifact=choice_str(f"{name}.artifact", data["artifact"], _MISSING_ARTIFACTS),
        page_number=int_at_least(f"{name}.page_number", data["page_number"], 1),
        element_id=optional_str(f"{name}.element_id", data["element_id"]),
        reason=non_blank_str(f"{name}.reason", data["reason"]),
    )


def _opened(root: Path, relative: str) -> int:
    """A descriptor of the bundle file at ``relative``, opened safely (see the module notes); the caller closes it.

    The path must pass :func:`~datagrunt.pdf.codec.contained_path` and no
    directory on it below ``root`` may be a symlink; the file is opened with
    :data:`_OPEN_FLAGS`, so a symlink is refused and a named pipe does not block.
    """
    try:
        file = contained_path("path", relative, root)
    except ValueError as exc:
        raise BundleCorruptError(f"{relative}: {exc}") from exc
    _refuse_symlinked_directories(root, relative)
    try:
        return os.open(file, _OPEN_FLAGS)
    except FileNotFoundError as exc:
        raise BundleCorruptError(f"{relative}: it is missing") from exc
    except OSError as exc:
        if file.is_symlink():
            raise BundleCorruptError(f"{relative}: it is a symlink, which a bundle file is never read through") from exc
        raise BundleCorruptError(f"{relative}: it cannot be opened: {exc}") from exc


def _refuse_symlinked_directories(root: Path, relative: str) -> None:
    """Refuse a path whose directories below ``root`` include a symlink (O_NOFOLLOW guards only the file)."""
    directory = root
    for part in relative.split("/")[:-1]:
        directory = directory / part
        if directory.is_symlink():
            raise BundleCorruptError(f"{relative}: its directory {part!r} is a symlink")


def _regular(descriptor: int, name: str) -> int:
    """The size of the open file, which must be a regular file (``fstat`` of the descriptor read from)."""
    status = os.fstat(descriptor)
    if not stat.S_ISREG(status.st_mode):
        raise BundleCorruptError(f"{name}: it is not a regular file")
    return status.st_size


def _read_fd(descriptor: int, limit: int, *, keep: bool) -> tuple[int, str, bytes]:
    """Read at most ``limit + 1`` bytes in chunks: the bytes read, their sha256, and them if ``keep``."""
    digest = hashlib.sha256()
    parts: list[bytes] = []
    total = 0
    for _ in range(limit // _READ_CHUNK_BYTES + 2):
        block = os.read(descriptor, min(_READ_CHUNK_BYTES, limit + 1 - total))
        if not block:
            break
        digest.update(block)
        total += len(block)
        if keep:
            parts.append(block)
    return total, digest.hexdigest(), b"".join(parts)


def _verified(root: Path, entry: _Artifact, *, keep: bool) -> bytes:
    """Open the artifact safely and check it is the manifest's (size, then sha256); its bytes if ``keep``, else b"".

    The size is ``fstat``'s and the bytes are read from the same descriptor.
    """
    descriptor = _opened(root, entry.path)
    try:
        size = _regular(descriptor, entry.path)
        if size != entry.size:
            raise BundleCorruptError(f"{entry.path}: it is {size} bytes, the manifest says {entry.size}")
        total, digest, data = _read_fd(descriptor, entry.size, keep=keep)
    finally:
        os.close(descriptor)
    if total != entry.size:
        raise BundleCorruptError(f"{entry.path}: it is {total} bytes, the manifest says {entry.size}")
    if digest != entry.sha256:
        raise BundleCorruptError(f"{entry.path}: its sha256 is {digest}, the manifest's is {entry.sha256}")
    return data


@dataclass(frozen=True, slots=True)
class _ArtifactReader:
    """A loaded figure's or page image's loader: the artifact's bytes, verified against the manifest on each read.

    Picklable (a path and the manifest's entry), so a document crosses a process pool. A page image's
    reader also keeps the image's origin in its bundle (``"document"`` or ``"render"``; ``""`` for a
    figure), so a bundle written again from a loaded document keeps it.
    """

    root: Path
    entry: _Artifact
    origin: str = ""

    def __call__(self) -> bytes:
        return _verified(self.root, self.entry, keep=True)


@dataclass(frozen=True, slots=True)
class _BandsReader:
    """A loaded page image's band loader: each band verified on each read, within the asset limit in total."""

    root: Path
    entries: tuple[_Artifact, ...]

    def __call__(self) -> tuple[bytes, ...]:
        total = sum(entry.size for entry in self.entries)
        if total > _MAX_ARTIFACT_BYTES:
            limit = f"over the {_MAX_ARTIFACT_BYTES}-byte asset limit"
            raise BundleCorruptError(f"{self.entries[0].path} and its page's other bands total {total} bytes, {limit}")
        return tuple(_verified(self.root, entry, keep=True) for entry in self.entries)


def _with_readers(
    document: PDFDocument, root: Path, by_path: Mapping[str, _Artifact], origins: Mapping[int, str]
) -> PDFDocument:
    """``document`` whose figures and page images (paths in ``root``, kept) read through verifying loaders.

    ``origins`` is each page image's origin, by page (the manifest's).
    """

    def entry_of(path: Path) -> _Artifact:
        return by_path[path.relative_to(root).as_posix()]

    pages = []
    for page in document.pages:
        elements = tuple(
            dataclasses.replace(element, _loader=_ArtifactReader(root, entry_of(element.image_path)))
            if isinstance(element, Figure) and element.image_path is not None
            else element
            for element in page.elements
        )
        image = _image_with_readers(page, root, entry_of, origins.get(page.page_number, ""))
        pages.append(dataclasses.replace(page, elements=elements, page_image=image))
    return _with_pages(document, tuple(pages))


def _image_with_readers(page: Page, root: Path, entry_of: Callable[[Path], _Artifact], origin: str) -> PageImage | None:
    image = page.page_image
    if image is None:
        return None
    loader = None if image.image_path is None else _ArtifactReader(root, entry_of(image.image_path), origin)
    bands = _BandsReader(root, tuple(entry_of(path) for path in image.band_paths)) if image.band_paths else None
    return dataclasses.replace(image, _loader=loader, _band_loader=bands)


def _check_facts(document: PDFDocument, manifest: _Manifest) -> None:
    """The manifest's source sha256, integrity and degraded pages must be the document's."""
    if manifest.source_sha256 != document.metadata.sha256:
        sha = document.metadata.sha256
        raise BundleCorruptError(f"{MANIFEST}: source_sha256 {manifest.source_sha256} is not the document's {sha}")
    if manifest.integrity_verified != document.integrity_verified:
        verified = document.integrity_verified
        message = f"integrity_verified {manifest.integrity_verified} is not the document's {verified}"
        raise BundleCorruptError(f"{MANIFEST}: {message}")
    if manifest.degraded_pages != document.degraded_pages:
        mine, theirs = list(manifest.degraded_pages), list(document.degraded_pages)
        raise BundleCorruptError(f"{MANIFEST}: degraded_pages {mine} are not the document's {theirs}")


def _document_paths(document: PDFDocument) -> Iterator[Path]:
    """Every path the document names: its figures', its page images' and their bands'."""
    for figure in document.figures:
        if figure.image_path is not None:
            yield figure.image_path
    for image in document.page_images:
        if image.image_path is not None:
            yield image.image_path
        yield from image.band_paths


def _check_paths(document: PDFDocument, listed: set[str], root: Path) -> None:
    """Every path the document names must be an artifact the manifest lists (so it was verified)."""
    for path in _document_paths(document):
        relative = path.relative_to(root).as_posix()
        if relative not in listed:
            raise BundleCorruptError(f"{DOCUMENT_JSON}: {relative!r} is not an artifact the manifest lists")


def _check_page_images(document: PDFDocument, entries: tuple[_ImageEntry, ...]) -> None:
    """The manifest's page images must be the document's: the same pages, DPI and normalization; no rendered bands."""
    listed = {entry.page_number: entry for entry in entries}
    imaged = {page.page_number: page.page_image for page in document.pages if page.page_image is not None}
    unlisted, absent = sorted(set(imaged) - set(listed)), sorted(set(listed) - set(imaged))
    if unlisted:
        raise BundleCorruptError(f"{MANIFEST}: page_images: pages {unlisted} have an image the manifest does not list")
    if absent:
        raise BundleCorruptError(
            f"{MANIFEST}: page_images: pages {absent} are listed but have no image in the document"
        )
    for number, image in imaged.items():
        entry = listed[number]
        if (entry.dpi, entry.contrast_normalized) != (image.dpi, image.contrast_normalized):
            mine = f"dpi {entry.dpi}, contrast_normalized {entry.contrast_normalized}"
            theirs = f"dpi {image.dpi}, contrast_normalized {image.contrast_normalized}"
            raise BundleCorruptError(f"{MANIFEST}: page_images: page {number} is {mine}; the document's is {theirs}")
        if entry.origin == "render" and image.band_paths:
            message = f"page_images: page {number} was rendered by the bundle, which writes no bands"
            raise BundleCorruptError(f"{MANIFEST}: {message}")


def _unwritten(document: PDFDocument, origins: Mapping[int, str]) -> set[tuple[str, int, str | None]]:
    """What the document lacks, as missing entries' keys: page images, own bands, figures (by first figure).

    Only a page's own image (origin ``"document"``) has bands to lack: the
    bundle's render has none.
    """
    keys: set[tuple[str, int, str | None]] = set()
    for page in document.pages:
        image = page.page_image
        if image is None:
            keys.add(("page_image", page.page_number, None))
        elif origins.get(page.page_number) == "document" and not image.band_paths:
            keys.add(("bands", page.page_number, None))
    first: dict[str, Figure] = {}
    for figure in document.figures:
        owner = first.setdefault(figure.sha256, figure)
        if figure.image_path is None:
            keys.add(("figure", owner.page_number, owner.element_id))
    return keys


def _key_order(key: tuple[str, int, str | None]) -> tuple[str, int, str]:
    return key[0], key[1], key[2] or ""


def _check_missing(document: PDFDocument, missing: tuple[_Missing, ...], origins: Mapping[int, str]) -> None:
    """The manifest's missing entries must be exactly what the document lacks, each once."""
    recorded = {(entry.artifact, entry.page_number, entry.element_id) for entry in missing}
    if len(recorded) != len(missing):
        raise BundleCorruptError(f"{MANIFEST}: missing lists an artifact twice")
    expected = _unwritten(document, origins)
    unrecorded, stale = expected - recorded, recorded - expected
    if unrecorded or stale:
        raise BundleCorruptError(
            f"{MANIFEST}: its missing artifacts are not the document's; "
            f"unrecorded: {sorted(unrecorded, key=_key_order)}; recorded but present: {sorted(stale, key=_key_order)}"
        )
