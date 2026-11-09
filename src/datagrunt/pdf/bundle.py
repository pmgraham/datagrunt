"""The v5 artifact bundle (#371, A7a): an atomic write and a verified load.

:func:`load_bundle` and :func:`write_bundle` are public since 5.0 (#373):
``datagrunt.pdf.load_bundle`` and ``datagrunt.pdf.write_bundle``, imported
on first access (:mod:`datagrunt.pdf._extra`); the rest of the module is
internal. A7b's ``PDFWriter.write_bundle`` and A8b's ``PDFReader`` stand on
it.

Layout (paths relative to the bundle, joined with ``/``):

- ``json/document.json``: the document, by :mod:`datagrunt.pdf.codec`
  (ASCII; its paths relative to the bundle).
- ``json/elements.jsonl``: the element frame's rows, one compact UTF-8 JSON
  object a line, as ``PDFWriter.write_jsonl`` writes them (hidden elements
  included, flagged by ``hidden_reasons``); a figure's ``markdown`` links to
  its picture relative to the bundle, as ``document.json`` names it.
- ``markdown/document.md``: the document's Markdown, UTF-8, each lone
  surrogate written as its escape (:func:`~datagrunt.pdf.frame.utf8_text`),
  hidden elements omitted (``PDFDocument.to_markdown``'s default: the JSON
  keeps them); a figure links to its own picture, ``../images/<name>.png``,
  relative to the file.
- ``markdown/pages/page_NNNN.md``: each page's Markdown, written as the
  document's is (hidden elements omitted), a figure linking to
  ``../../images/<name>.png``.
- ``images/<sha256>.png``: each distinct picture, once: a figure's PNG from
  ``Figure.read_bytes`` (its ``image_path``, else its loader), named by
  the figure's ``sha256`` (its raw stream's). Every figure is read, and
  its PNG decides whether it is a picture already written: one stream under
  two image dictionaries (a colour space, a palette, a ``/Decode``) is two
  pictures, the second ``images/<sha256>-<its PNG's sha256>.png``
  (:class:`_Pictures`). Each figure's ``image_path`` is its own picture.
  Why every figure: its ``sha256`` hashes the stream, not the dictionary
  that decodes it, and nothing else the document records tells two such
  pictures apart. Keying on the stream alone gave a figure another
  figure's picture; reading each costs one read a figure (a logo repeated
  on every page is read on every page), and correctness comes first.
- ``page_images/page_NNNN.png``: every page's image (origin):
  - a page whose ``page_image`` is set: its own PNG, from
    ``PageImage.read_bytes`` (its loader), as it is, at its own DPI; never
    rendered again. A5b's vision pages' (``"document"``: rendered at the
    effective ``vision_dpi`` and normalized), and each digital or hybrid
    page's (``"render"``, no bands: A4b's ``PageDraw``, P2-A, which draws
    the page on read, 8-bit grey as drawn: the bytes this bundle's own
    render of the page writes). A drawn page is written at the effective
    DPI of this write's ``dpi`` (and ``max_render_pixels``): through its
    own loader when that is its DPI, else by its ``PageDraw`` at that DPI
    (the same page, source and render worker), so the manifest's ``dpi``
    is always its image's;
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
- ``manifest.json``: :data:`BUNDLE_FORMAT_VERSION`, the datagrunt version
  (``datagrunt.__version__``),
  the source's sha256, the extraction parameters (``dpi``, ``vision_dpi``,
  ``max_render_pixels``, ``workers``, then the caller's
  ``extraction_params``), ``integrity_verified``, ``degraded_pages`` and
  ``image_only_pages`` (#470: ``PDFDocument.image_only_pages``, the pages
  whose content lives only in their page image, which the element rows
  cannot name when a page has no elements), each page image's origin, DPI
  and ``contrast_normalized`` (by page), every other file's path, size and
  sha256 (by path), and each artifact that is missing and why (by page).

Formats: :data:`BUNDLE_FORMAT_VERSION`, ``"5.3"`` (#470), is written: its
manifest holds ``image_only_pages`` and its ``json/document.json`` is codec
version 2 (each page's ``image_only_reasons``). ``"5.0"``, which datagrunt
5.0 to 5.2 wrote, still loads: its manifest holds exactly its own keys (no
``image_only_pages``) and its document JSON must be codec version 1, so
each page's ``image_only_reasons`` reads None (not recorded); the rest is
checked as a 5.3 bundle's is. Each format's document JSON must be its own
codec version. A loaded 5.0 bundle's document writes as format 5.3 (its
pages' reasons null, ``image_only_pages`` empty). Any other format is
refused.

PNGs are encoded with Pillow (the bundle's renders here; figures and own
page images by their loaders); PDF content is decoded only by A1's loaders
and A5a's guarded render. A figure's or own page image's bytes must start
with the PNG signature. Each file is created exclusively and streamed
through a SHA-256 as it is written.

:func:`write_bundle` is atomic. Every check comes first (the options, as
``PipelineOptions`` checks them; the document; ``overwrite``;
``output_dir``, which must not exist, or be an empty directory or one
holding a bundle's manifest (a ``manifest.json`` that reads and checks as
:func:`load_bundle`'s step 1 checks it): a file, a symlink or any other
directory, one holding some other ``manifest.json`` included, is a
FileExistsError, never merged into, replaced or deleted, whatever
``overwrite`` says; its parent; the source, which must hash to the
document's sha256).

A bundle already at ``output_dir`` is reused when it is this write's
(:func:`_existing`): it loads and verifies (:func:`load_bundle`), its
format is :data:`BUNDLE_FORMAT_VERSION` (a 5.0 bundle, which loads, is
never this write's), its manifest's datagrunt version is this one's, its
source sha256 the document's, its extraction parameters this write's, and
its document this one (the pages the bundle rendered compared without their
page image).
Its document is then returned as :func:`load_bundle` returns it, and
nothing is written, nor any of a write's warnings emitted (its manifest
still records what the write that made it recorded). Any other bundle
there is a FileExistsError naming why,
unless ``overwrite`` is True (a reusable bundle is reused either way).

The bundle is then written into a new staging directory, ``.tmp_<uuid4
hex>`` in ``output_dir``'s parent (the same filesystem), ``output_dir`` is
checked again and the staging directory renamed onto it (``os.replace``).
With ``overwrite``, a bundle there is first renamed aside, to ``.old_<uuid4
hex>`` beside it; the staging directory is renamed in, and the old bundle
removed. If that second rename fails, the old bundle is renamed back. On
any failure before the swap, an interrupt included, the staging directory
is removed and the error raised, and an old bundle is untouched; one after
the rename (an interrupt) is raised as it is, the new bundle in place (with
``overwrite``, the old bundle may be left aside as ``.old_<hex>``). A writer
killed outright leaves its staging directory (or, between the two
renames, the old bundle aside), the only trace: never a half-written
``output_dir``; the next write makes its own and ignores it. If removing
the old bundle fails, the error is raised: the new bundle is in place.

Pages are written on ``workers`` threads (each render is a worker process
of A5a's, as is each own page image's loader), in a sliding window
(:func:`_in_page_order`): at most ``workers`` pages are in flight, and the
next page starts as soon as one ends, so a slow page holds one thread,
never the others. Each page's PNGs are written in its own thread, so no
page's pixels outlive it: at most ``workers`` pages' pixels are held at
once. The outcomes are recorded in page order.

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
or is not a PNG, has no page image (nor bands); a page image drawn on read
that fails is recorded as the bundle's render of it is ("the page was not
rendered: ..."); a page whose own bands
cannot be read, or one of them is not a PNG, keeps its image and has no
bands; a figure whose bytes cannot be read or are not a PNG, or that is
not bound to the source (see below), is not written (it keeps no
``image_path``; a later figure of its stream is still read).
Each is a ``missing`` entry naming the artifact, the page, the figure's
element id (one entry a figure) and why. Any other error fails the write.

An image-only page (``PDFDocument.image_only_pages``, #470: its content
lives only in its page image) whose page image is not written, whatever
the cause (``render_pages=False``, ``include_images=False``, a render
refused or failed, an own image unread or unbound), is in no artifact as
a page: the source PDF is its only full copy (#471). A figure on it (a
scan's own picture) is still written as any figure is, unless
``include_images=False``: a part of the page, never the page. The opt-out is honoured and nothing more is
written; instead its ``missing`` page image entry's reason ends
``"; its content lives only in its page image (<its reasons, joined by
', '>): the source PDF is its only full copy"`` (every other entry is as it
would be), and one ``UserWarning`` a write names those pages, the
document (its ``source_name``), the bundle's path and the cause (the
option, or the manifest's ``missing`` entries for the failures),
emitted once the bundle is published (:func:`_mark_only_copies`).

The page images are bound to the hashed source. The source's identity
(A8a's ``SourceIdentity``: device, inode, size, mtime) is taken with its
hash, and checked again once every artifact is written, before the rename:
a source changed, replaced or removed meanwhile raises
``PDFSourceChangedError`` and nothing is published (the staging directory
is removed). A page's own image is written only when its loaders are bound
to that file (:func:`_unbound`): each renders from ``source`` itself
(A5b's ``PageRender`` or A4b's ``PageDraw``, compared by resolved path) or is a bundle's
verifying reader. One that renders from another file, names no source, or
an image with no loader is not read: the page has no page image, a
``missing`` entry says why, and a ``UserWarning`` is emitted once the
bundle is published. A ``PageRender`` or ``PageDraw`` draws only a file
whose bytes are its document's (#453; :mod:`datagrunt.pdf._loaders`): a
file of the identity it holds at once, a file of another identity once it
hashes to its sha256. The source hashed here to the document's sha256 is
such a file, so the write draws through a copy of each loader bound to the
document's bytes, holding the identity taken with the hash
(:func:`_draws_bound`): a source touched or rewritten with those same bytes
(another inode, another mtime) is drawn, and the write hashes its source
once, never again a page. A figure is bound the same way
(:func:`_figure_unbound`): its PNG loader reads from ``source`` (A5a's
``FigureRead``) or is a bundle's verifying reader. A figure with no loader
(a bare ``image_path``, which would be read unverified, whatever file it
names) or another loader is not read: it has no image, a ``missing`` entry
says why, and a ``UserWarning`` is emitted. What this cannot see is the A8a
limit: a write that keeps the size and restores the mtime.

Known limitations, documented:

- No ``fsync`` before the rename: a write survives a killed process, not
  a power loss; a bundle torn by one fails its checksums on load.
- ``output_dir`` filled by another process between the publish check and
  the rename makes ``os.replace`` fail with ``OSError`` (``ENOTEMPTY``),
  not ``FileExistsError``; it is never merged into, and staging is removed.
- Figures are read in batches of up to 64 a figure worker
  (:func:`~datagrunt.pdf._loaders.figure_pngs`), each read reopening the
  PDF, not batched by page through one handle. Every figure is read, the
  figures of one stream too: only its PNG shows it is the same picture
  (see ``images/`` above).

:func:`load_bundle` takes a bundle directory or its ``manifest.json``
and returns the document. Nothing is trusted before it is checked:

1. The manifest (opened as an artifact is, read within the asset limit,
   strict JSON): its version (a format above), its keys (exactly its
   format's, at every level) and values; every artifact path through
   :func:`~datagrunt.pdf.codec.contained_path` (plain relative parts,
   resolved inside the bundle: no ``..``, absolute path or symlink out),
   listed once, ``json/document.json``, ``json/elements.jsonl`` and
   ``markdown/document.md`` among them, each no larger than the asset
   limit.
2. Every artifact, opened safely (:func:`_opened`): its path through
   ``contained_path`` again, no directory on it below the bundle a symlink,
   and the file opened with ``O_NOFOLLOW`` (a symlink is refused) and
   ``O_NONBLOCK`` (a named pipe cannot block the open). ``fstat`` of that
   descriptor must show a regular file of the manifest's size; its bytes
   are read from the same descriptor, in chunks, at most that size plus one
   byte, and must hash to the manifest's sha256. ``json/document.json`` is
   decoded from the bytes verified, not read again.
3. The document (the codec, of its format's version only; its paths joined
   to the bundle by the same helper): the manifest's source sha256,
   ``integrity_verified``, ``degraded_pages`` and (format 5.3)
   ``image_only_pages`` must be its; every path it names must be a verified
   artifact; the manifest's page images must be its (the same pages, DPI
   and ``contrast_normalized``; a rendered one without bands); the
   manifest must list a ``markdown/pages/page_NNNN.md`` for each of its
   pages and no other; and its missing page images, own bands and figures
   must be exactly the manifest's ``missing`` entries.

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
import json
import os
import shutil
import uuid
import warnings
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from types import MethodType
from typing import IO, BinaryIO, NamedTuple, TypeVar, cast

import polars as pl
from PIL import Image

import datagrunt
from datagrunt.pdf._bundle_load import (
    _MAX_ARTIFACT_BYTES as _MAX_ARTIFACT_BYTES,
)
from datagrunt.pdf._bundle_load import (
    _PARAM_KEYS,
    BUNDLE_FORMAT_VERSION,
    DOCUMENT_JSON,
    DOCUMENT_MARKDOWN,
    ELEMENTS_JSONL,
    MANIFEST,
    _Artifact,
    _ArtifactReader,
    _BandsReader,
    _json_scalar,
    _loaded,
    _Missing,
    _page_markdown,
    _read_manifest,
    _with_pages,
    _with_readers,
    load_bundle,
)
from datagrunt.pdf._bundle_load import (
    _READ_CHUNK_BYTES as _READ_CHUNK_BYTES,
)
from datagrunt.pdf._bundle_load import (
    _read_fd as _read_fd,
)
from datagrunt.pdf._loaders import FigureRead, PageDraw, PageRender, figure_pngs
from datagrunt.pdf._options import RenderPagesMode, checked_render_pages
from datagrunt.pdf._source import SourceIdentity, _opened_source, _stream_sha256
from datagrunt.pdf._validate import checked_path as _checked_path
from datagrunt.pdf._validate import strict_bool
from datagrunt.pdf.codec import document_to_json_bytes
from datagrunt.pdf.elements import Figure, Page, PageImage, PDFDocument
from datagrunt.pdf.exceptions import BundleCorruptError, DatagruntPDFError, PDFSourceChangedError
from datagrunt.pdf.frame import element_frame, utf8_text
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page

_default_render_page = render_page

__all__ = ["BUNDLE_FORMAT_VERSION", "load_bundle", "write_bundle"]

_STAGING_PREFIX = ".tmp_"
# An old bundle renamed aside while an overwrite swaps the new one in.
_ASIDE_PREFIX = ".old_"
# A render worker's memory past its start, and its time: A5b's vision route's.
_MEMORY_CAP_BYTES = 2 << 30
_RENDER_TIMEOUT_S = 120.0
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_T = TypeVar("_T")


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
    overwrite: bool = False,
    include_images: bool = True,
    render_pages: RenderPagesMode = True,
    sandbox: bool = False,
) -> PDFDocument:
    """Write ``document``, extracted from ``source``, as a bundle at ``output_dir``, atomically.

    See the module notes for the layout, the checks, the reuse of a bundle
    already there, the atomic write (and swap, with ``overwrite``), the
    page window and the failures recorded. ``password`` opens ``source``
    for the renders. ``extraction_params`` are the document's other
    extraction parameters (``ocr``, ``min_image_dimension``, ``strict``:
    what A7b's ``PDFWriter`` extracted with), recorded in the manifest after
    the writer's own (see :func:`_checked_params`). ``overwrite`` replaces a
    bundle at ``output_dir`` that is not this write's. ``include_images=False``
    skips writing figure PNGs and rendering page PNGs; ``render_pages=False``
    skips rendering page PNGs while keeping figure PNGs.

    Returns:
        ``document`` with paths set (see the module notes), or the reused
        bundle's document, as :func:`load_bundle` returns it.

    Raises:
        TypeError, ValueError: If an option is invalid (as ``PipelineOptions``
            raises it), an extraction parameter is (:func:`_checked_params`),
            ``document`` is not a PDFDocument, ``overwrite`` is not a bool,
            ``output_dir`` or ``source`` is not a path, or ``source`` is not a
            regular file or does not hash to the document's sha256.
        FileExistsError: If ``output_dir`` is a file, a symlink or a
            non-empty directory holding no bundle; or holds a bundle that is
            not this write's and ``overwrite`` is False (before or after the
            write).
        FileNotFoundError: If ``source`` or ``output_dir``'s parent is missing.
        PDFSourceChangedError: If ``source`` changed (or was removed) between
            its hash and the publish: the bundle is not published.
        OSError: If a file cannot be written, or the rename fails.

    Warns:
        UserWarning: For each page whose own image is not written because its
            loaders are not bound to ``source`` (see the module notes); and
            once, naming the image-only pages (``document.image_only_pages``)
            whose page image is not written, whatever the cause: the source
            PDF is their only full copy (#471).
    """
    options = PipelineOptions(
        workers=workers,
        dpi=dpi,
        vision_dpi=vision_dpi,
        max_render_pixels=max_render_pixels,
        sandbox=sandbox,
    )
    include_imgs = strict_bool("include_images", include_images)
    render_pgs = checked_render_pages(render_pages)
    params = _checked_params(options, extraction_params, include_images=include_imgs, render_pages=render_pgs)
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    replace = strict_bool("overwrite", overwrite)
    target = _checked_target(output_dir)
    pdf = PDFSource(path=_checked_path(source, "source"), password=password)
    identity = _check_source(pdf, document.metadata.sha256)
    pdf = dataclasses.replace(pdf, identity=identity, sha256=document.metadata.sha256)  # the file hashed
    reused = _reused(target, document, params, replace)
    if reused is not None:
        return reused
    staging = target.parent / f"{_STAGING_PREFIX}{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        written, notices = _staged(
            document,
            staging,
            target,
            pdf,
            options,
            params,
            include_images=include_imgs,
            render_pages=render_pgs,
        )
        _check_unchanged(pdf, identity)
        aside = _publish(staging, target, replace)
    except BaseException:
        if os.path.lexists(staging):  # gone once _publish renamed it: the error raised is the one to keep
            shutil.rmtree(staging)
        raise
    if aside is not None:
        shutil.rmtree(aside)
    for notice in notices:
        warnings.warn(notice, UserWarning, stacklevel=2)
    return written


def _checked_params(
    options: PipelineOptions,
    extra: object,
    *,
    include_images: bool = True,
    render_pages: RenderPagesMode = True,
) -> dict[str, object]:
    """The manifest's ``extraction_params``: the writer's own, then ``extra``'s, each a JSON scalar.

    Raises:
        TypeError: If ``extra`` is not a Mapping or None, a key is not a str,
            or a value is not a str, int, float, bool or None.
        ValueError: If a key is blank or one of the writer's own, or a float
            is not finite.
    """
    params: dict[str, object] = {name: getattr(options, name) for name in _PARAM_KEYS}
    if extra is not None:
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
    if options.sandbox:
        params["sandbox"] = True
    if not include_images:
        params["include_images"] = False
    if render_pages is False:
        params["render_pages"] = False
    elif render_pages == "vision_only":
        params["render_pages"] = "vision_only"
    return params


def _checked_target(output_dir: object) -> Path:
    """``output_dir`` made absolute, if it may hold the bundle: absent, empty or a bundle's, in a directory."""
    if not isinstance(output_dir, (str, os.PathLike)):
        raise TypeError(f"output_dir must be a str or os.PathLike, got {type(output_dir).__name__}")
    target = Path(output_dir).absolute()
    _refuse_unusable(target)
    if not target.parent.is_dir():
        raise FileNotFoundError(errno.ENOENT, "output_dir's parent is not an existing directory", str(target.parent))
    return target


def _refuse_unusable(target: Path) -> None:
    """Raise FileExistsError unless ``target`` is absent, an empty directory, or a bundle's (:func:`_holds_bundle`).

    A symlink, a file and any other directory are refused: a bundle is never
    merged into one, nor replaces one, whatever ``overwrite`` says.
    """
    if target.is_symlink() or not target.exists() or not target.is_dir():
        _refuse_existing(target)
        return
    if _is_empty(target) or _holds_bundle(target):
        return
    message = "output_dir exists and is not empty, and holds no bundle: a bundle never merges into it, nor replaces it"
    raise FileExistsError(errno.EEXIST, message, str(target))


def _holds_bundle(directory: Path) -> bool:
    """True if ``directory`` holds a bundle's manifest: one that reads and checks as :func:`load_bundle`'s does."""
    try:
        _read_manifest(directory)
    except BundleCorruptError:
        return False
    return True


def _is_empty(directory: Path) -> bool:
    with os.scandir(directory) as entries:
        return next(entries, None) is None


def _refuse_existing(target: Path) -> None:
    """Raise FileExistsError unless ``target`` is absent or an empty directory (a symlink is refused)."""
    if target.is_symlink():
        raise FileExistsError(errno.EEXIST, "output_dir is a symlink: a bundle never replaces one", str(target))
    if not target.exists():
        return
    if not target.is_dir():
        raise FileExistsError(errno.EEXIST, "output_dir exists and is not a directory", str(target))
    if not _is_empty(target):
        message = "output_dir exists and is not empty: a bundle is never merged into it"
        raise FileExistsError(errno.EEXIST, message, str(target))


def _reused(target: Path, document: PDFDocument, params: Mapping[str, object], replace: bool) -> PDFDocument | None:
    """The bundle at ``target`` if it is this write's (:func:`_existing`); None if there is none, or it may be replaced.

    Raises:
        FileExistsError: If ``target`` holds a bundle that is not this write's and ``replace`` is False.
    """
    if not target.is_dir() or _is_empty(target):
        return None
    loaded, why = _existing(target, document, params)
    if loaded is not None:
        return loaded
    if replace:
        return None
    message = f"output_dir holds a bundle that is not this write's ({why}): pass overwrite=True to replace it"
    raise FileExistsError(errno.EEXIST, message, str(target))


def _existing(target: Path, document: PDFDocument, params: Mapping[str, object]) -> tuple[PDFDocument | None, str]:
    """The bundle at ``target`` and ``""`` if this write would make it (see the module notes); else None and why."""
    try:
        manifest, loaded = _loaded(target)
    except (BundleCorruptError, OSError) as exc:
        return None, f"it is not a valid bundle: {exc}"
    if manifest.bundle_format_version != BUNDLE_FORMAT_VERSION:
        return None, f"its bundle format is {manifest.bundle_format_version!r}, not {BUNDLE_FORMAT_VERSION!r}"
    version = _datagrunt_version()
    if manifest.datagrunt_version != version:
        return None, f"it was written by datagrunt {manifest.datagrunt_version}, not {version}"
    if manifest.source_sha256 != document.metadata.sha256:
        return None, f"its source_sha256 is {manifest.source_sha256}, not the document's {document.metadata.sha256}"
    if manifest.extraction_params != dict(params):
        return None, f"its extraction_params {manifest.extraction_params} are not this write's {dict(params)}"
    rendered = {entry.page_number for entry in manifest.page_images if entry.origin == "render"}
    strip_figures = params.get("include_images") is False
    if strip_figures or params.get("render_pages") is False:
        rendered |= {page.page_number for page in document.pages}
    elif params.get("render_pages") == "vision_only":
        rendered |= {page.page_number for page in document.pages if not _needs_vision_render(page)}
    if _without_renders(loaded, rendered, strip_figures=strip_figures) != _without_renders(
        document, rendered, strip_figures=strip_figures
    ):
        return None, "its document is not this one"
    return loaded, ""


def _without_renders(document: PDFDocument, rendered: set[int], *, strip_figures: bool = False) -> PDFDocument:
    """``document`` whose pages in ``rendered`` (the bundle's renders) have no page image: what a write compares."""
    if strip_figures:
        return _with_paths(document, {}, {}, Path())
    pages = tuple(
        dataclasses.replace(page, page_image=None) if page.page_number in rendered else page for page in document.pages
    )
    return dataclasses.replace(document, pages=pages)


def _publish(staging: Path, target: Path, replace: bool) -> Path | None:
    """Rename ``staging`` onto ``target``; with ``replace``, a bundle there is renamed aside first, and returned.

    The caller removes what is returned once the new bundle is in place. If
    the rename onto ``target`` fails, the old bundle is renamed back.
    """
    _refuse_unusable(target)
    if not replace or not target.is_dir() or _is_empty(target):
        _refuse_existing(target)
        os.replace(staging, target)
        return None
    aside = target.parent / f"{_ASIDE_PREFIX}{uuid.uuid4().hex}"
    os.replace(target, aside)
    try:
        os.replace(staging, target)
    except BaseException:
        os.replace(aside, target)
        raise
    return aside


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


def _needs_vision_render(page: Page) -> bool:
    """True when ``render_pages='vision_only'`` must persist this page's image."""
    return (page.route == "vision" and page.status != "error") or bool(page.image_only_reasons)


def _staged(
    document: PDFDocument,
    staging: Path,
    target: Path,
    source: PDFSource,
    options: PipelineOptions,
    params: Mapping[str, object],
    *,
    include_images: bool = True,
    render_pages: RenderPagesMode = True,
) -> tuple[PDFDocument, tuple[str, ...]]:
    """Write every artifact, then the manifest, into ``staging``: the document (paths in ``target``), and warnings."""
    ledger = _Ledger()
    if include_images:
        figures = _write_figures(document, staging, source.path, ledger, sandbox=options.sandbox)
    else:
        figures = _skip_figures(document, ledger)
    skipped = _opted_out(include_images=include_images, render_pages=render_pages)
    if skipped:
        renders = _skip_renders(document, ledger, skipped)
    else:
        renders = _write_renders(document, staging, source, options, ledger, render_pages=render_pages)
    _mark_only_copies(document, renders, ledger, skipped, target)
    _write_texts(document, figures, renders, staging, ledger)
    _write_bytes(staging, MANIFEST, _manifest(document, params, renders, ledger))
    by_path = {artifact.path: artifact for artifact in ledger.artifacts}
    origins = {number: written.origin for number, written in renders.items()}
    returned = _with_readers(_with_paths(document, figures, renders, target), target, by_path, origins)
    return returned, tuple(ledger.warnings)


def _skip_figures(document: PDFDocument, ledger: _Ledger) -> dict[str, str]:
    """Record each figure as skipped when ``include_images=False``."""
    for figure in document.figures:
        reason = f"figure {figure.sha256} was not written: include_images=False"
        ledger.missing.append(_Missing("figure", figure.page_number, figure.element_id, reason))
    return {}


def _opted_out(*, include_images: bool, render_pages: RenderPagesMode) -> str:
    """The option that skips every page image (``include_images=False`` first), or ``""`` if none does."""
    if not include_images:
        return "include_images=False"
    return "render_pages=False" if render_pages is False else ""


def _skip_renders(document: PDFDocument, ledger: _Ledger, why: str) -> dict[int, _Written]:
    """Record each page render as skipped when ``include_images=False`` or ``render_pages=False``."""
    for page in document.pages:
        ledger.missing.append(_Missing("page_image", page.page_number, None, f"the page was not rendered: {why}"))
    return {}


def _mark_only_copies(
    document: PDFDocument, renders: Mapping[int, _Written], ledger: _Ledger, skipped: str, target: Path
) -> None:
    """Say that the source PDF is the only full copy of each unwritten image-only page: its missing entry, one warning.

    An image-only page (#470) holds its content only in its page image: one
    the bundle does not write, whatever the cause, leaves the source PDF its
    only full copy (#471): a figure on the page, if written, is a part of it.
    Every other entry is left as it is. ``skipped`` is the option that
    skipped every page image, or ``""``. The warning names the document and
    the bundle (``target``): Python's default filter shows one warning a
    text and a place, so a corpus of such documents still warns for each.
    """
    unwritten = {
        page.page_number: page.image_only_reasons
        for page in document.pages
        if page.image_only_reasons and page.page_number not in renders
    }
    if not unwritten:
        return
    for index, entry in enumerate(ledger.missing):
        marks = unwritten.get(entry.page_number)
        if entry.artifact != "page_image" or marks is None:
            continue
        clause = (
            f"; its content lives only in its page image ({', '.join(marks)}): the source PDF is its only full copy"
        )
        ledger.missing[index] = entry._replace(reason=entry.reason + clause)
    unsaved = f"does not write ({skipped})" if skipped else "could not write (manifest.json's missing entries say why)"
    notice = _only_copy_notice(document.metadata.source_name, sorted(unwritten), f"the bundle at {target} {unsaved}")
    ledger.warnings.append(notice)


def _only_copy_notice(source_name: str, pages: list[int], unsaved: str) -> str:
    """The warning that image-only ``pages`` of ``source_name`` were not written (``unsaved`` says where, and why)."""
    held = f"pages {pages} of {source_name} hold content only in their page images"
    return f"{held}, which {unsaved}; the source PDF is their only full copy"


@dataclass
class _Pictures:
    """The distinct figure pictures written so far: each one's file name, by its stream's and its PNG's sha256.

    A figure's ``sha256`` hashes its raw stream, but its PNG depends on its
    image dictionary too (a colour space, a palette, a ``/Decode``), so one
    stream can be two pictures: a picture is its stream's sha256 and its
    PNG's. The first picture of a stream is ``<sha256>.png``; another
    picture of it, ``<sha256>-<its PNG's sha256>.png``.
    """

    names: dict[tuple[str, str], str] = field(default_factory=dict)
    streams: set[str] = field(default_factory=set)

    def name(self, sha256: str, digest: str) -> tuple[str, bool]:
        """The file name of the PNG whose sha256 is ``digest``, of stream ``sha256``; and whether it is unwritten."""
        known = self.names.get((sha256, digest))
        if known is not None:
            return known, False
        return (f"{sha256}-{digest}.png" if sha256 in self.streams else f"{sha256}.png"), True

    def record(self, sha256: str, digest: str, name: str) -> None:
        """Note the picture written, as ``name`` (once its file is)."""
        self.names[(sha256, digest)] = name
        self.streams.add(sha256)


def _write_texts(
    document: PDFDocument, figures: Mapping[str, str], renders: Mapping[int, _Written], staging: Path, ledger: _Ledger
) -> None:
    """Write the document's JSON, its frame's rows, its Markdown and each page's, its paths relative to each file."""
    relative = _with_paths(document, figures, renders, Path())
    ledger.artifacts.append(_write_bytes(staging, DOCUMENT_JSON, document_to_json_bytes(relative)))
    rows = element_frame(relative)
    ledger.artifacts.append(_write_stream(staging, ELEMENTS_JSONL, lambda sink: _write_rows(rows, sink.write)))
    markdown = utf8_text(_with_paths(document, figures, renders, Path("..")).to_markdown())
    ledger.artifacts.append(_write_bytes(staging, DOCUMENT_MARKDOWN, markdown.encode("utf-8")))
    for page in _with_paths(document, figures, renders, Path("../..")).pages:
        text = utf8_text(page.to_markdown(include_hidden=False))
        ledger.artifacts.append(_write_bytes(staging, _page_markdown(page.page_number), text.encode("utf-8")))


def _write_rows(frame: pl.DataFrame, write: Callable[[bytes], object]) -> None:
    """Write each row of ``frame`` as a line of compact UTF-8 JSON, in order (``PDFWriter.write_jsonl``'s lines)."""
    for row in frame.iter_rows(named=True):
        line = json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        write(line.encode("utf-8") + b"\n")


def _write_figures(
    document: PDFDocument, staging: Path, source: Path, ledger: _Ledger, *, sandbox: bool = False
) -> dict[str, str]:
    """Write each bound figure's PNG, each distinct picture once (:class:`_Pictures`); each written one's path, by id.

    A figure not bound to ``source`` (:func:`_figure_unbound`) is not read: it gets a missing entry and a warning.
    """
    paths: dict[str, str] = {}
    pictures = _Pictures()
    figures = _bound_figures(document.figures, source, ledger, sandbox=sandbox)
    for figure, read in zip(figures, figure_pngs(figures), strict=True):  # read in batches, each in its turn
        data, why = _png(read)
        if data is None:
            reason = f"figure {figure.sha256} was not written: {why}"
            ledger.missing.append(_Missing("figure", figure.page_number, figure.element_id, reason))
            continue
        paths[figure.element_id] = _write_picture(staging, figure.sha256, data, pictures, ledger)
    return paths


def _rebound_figure(figure: Figure, *, sandbox: bool) -> Figure:
    if not sandbox:
        return figure
    owner = getattr(figure._loader, "__self__", None)
    if (
        isinstance(owner, FigureRead)
        and not owner.sandbox
        and getattr(figure._loader, "__func__", None) is FigureRead.png_bytes
    ):
        rebound = dataclasses.replace(owner, sandbox=True)
        return dataclasses.replace(figure, _loader=rebound.png_bytes, _raw_loader=rebound.raw_bytes)
    return figure


def _bound_figures(
    figures: tuple[Figure, ...], source: Path, ledger: _Ledger, *, sandbox: bool = False
) -> tuple[Figure, ...]:
    """The figures bound to ``source`` (:func:`_figure_unbound`); each other is unread: a missing entry, a warning."""
    bound: list[Figure] = []
    for figure in figures:
        unbound = _figure_unbound(figure, source)
        if not unbound:
            bound.append(_rebound_figure(figure, sandbox=sandbox))
            continue
        reason = f"figure {figure.sha256} {unbound}: its bytes could not be bound to the hashed source"
        ledger.missing.append(_Missing("figure", figure.page_number, figure.element_id, reason))
        ledger.warnings.append(f"figure {figure.element_id} {unbound}: no image is written for it")
    return tuple(bound)


def _write_picture(staging: Path, sha256: str, data: bytes, pictures: _Pictures, ledger: _Ledger) -> str:
    """The relative path of the picture ``data`` (of stream ``sha256``): written now unless it already is."""
    digest = hashlib.sha256(data).hexdigest()
    name, unwritten = pictures.name(sha256, digest)
    relative = f"images/{name}"
    if unwritten:
        ledger.artifacts.append(_write_bytes(staging, relative, data))
        pictures.record(sha256, digest, name)
    return relative


def _png(read: Callable[[], bytes]) -> tuple[bytes | None, str]:
    """The PNG bytes ``read`` returns (a figure's or a page image's), or None and why they cannot be read."""
    try:
        data = read()
    except (DatagruntPDFError, OSError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not data.startswith(_PNG_SIGNATURE):
        return None, "its bytes are not a PNG"
    return data, ""


def _in_page_order(task: Callable[[Page], _T], pages: tuple[Page, ...], workers: int, prefix: str) -> list[_T]:
    """``task`` of every page on ``workers`` threads, at most ``workers`` in flight; the outcomes, in page order.

    A sliding window: the next page is submitted as soon as one ends, so a
    slow page holds one thread, never the others, and no more than
    ``workers`` pages are ever submitted and unfinished. An error a task
    raises fails the call, once the pages in flight have ended.
    """
    outcomes: dict[int, _T] = {}
    queued = iter(range(len(pages)))
    running: dict[Future[_T], int] = {}
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=prefix)
    try:
        for _ in range(len(pages) + 1):  # bounded: each pass collects at least one page, but the last
            for index in islice(queued, workers - len(running)):
                running[pool.submit(task, pages[index])] = index
            if not running:
                break
            done, _pending = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                outcomes[running.pop(future)] = future.result()
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    return [outcomes[index] for index in range(len(pages))]


def _write_renders(
    document: PDFDocument,
    staging: Path,
    source: PDFSource,
    options: PipelineOptions,
    ledger: _Ledger,
    *,
    render_pages: RenderPagesMode = True,
) -> dict[int, _Written]:
    """Write selected page images on ``workers`` threads (:func:`_in_page_order`); each written, by number."""
    renders: dict[int, _Written] = {}

    def task(page: Page) -> _PageOutcome:
        if render_pages == "vision_only" and not _needs_vision_render(page):
            reason = "the page was not rendered: render_pages='vision_only'"
            return _PageOutcome(None, (), (_Missing("page_image", page.page_number, None, reason),))
        return _page_outcome(page, staging, source, options)

    for outcome in _in_page_order(task, document.pages, options.workers, "datagrunt-bundle"):
        _record(outcome, renders, ledger)
    return renders


def _record(outcome: _PageOutcome, renders: dict[int, _Written], ledger: _Ledger) -> None:
    ledger.artifacts.extend(outcome.artifacts)
    ledger.missing.extend(outcome.missing)
    if outcome.written is not None:
        renders[outcome.written.page_number] = outcome.written
    if outcome.warning:
        ledger.warnings.append(outcome.warning)


def _page_outcome(page: Page, staging: Path, source: PDFSource, options: PipelineOptions) -> _PageOutcome:
    """Write the page's own image and bands when it has one, else render it at ``dpi``; a failure is recorded.

    A page image drawn on read is written at the effective DPI of ``dpi``
    (:func:`_at_write_dpi`): its own when that is its DPI, else drawn again at it.
    """
    image = page.page_image
    if image is None:
        return _rendered_outcome(page, staging, source, options)
    image = _draws_bound(image, source, sandbox=options.sandbox)
    if _page_draw(image) is None:
        return _own_outcome(image, staging, source)
    try:
        drawn = _at_write_dpi(image, page, options)
    except RenderRefused as exc:
        reason = f"the page was not rendered: {type(exc).__name__}: {exc}"
        return _PageOutcome(None, (), (_Missing("page_image", page.page_number, None, reason),))
    return _own_outcome(drawn, staging, source)


def _at_write_dpi(image: PageImage, page: Page, options: PipelineOptions) -> PageImage:
    """A drawn page image at the effective DPI of the write's ``dpi``: itself at that DPI, else its draw at it.

    The draw at another DPI is its :class:`~datagrunt.pdf._loaders.PageDraw`
    with that DPI: the same page, source, password and render path.

    Raises:
        RenderRefused: If no DPI draws the page within ``max_render_pixels``.
    """
    dpi = effective_dpi(options.dpi, options.max_render_pixels, page.width_pt, page.height_pt)
    draw = _page_draw(image)
    if draw is None or image.dpi == dpi:
        return image
    redrawn = dataclasses.replace(draw, dpi=dpi)
    width, height = redrawn.size
    return dataclasses.replace(image, dpi=float(dpi), width_px=width, height_px=height, _loader=redrawn.png_bytes)


def _draws_bound(image: PageImage, source: PDFSource, *, sandbox: bool = False) -> PageImage:
    """``image`` whose PageDraw or PageRender loaders draw through the file this write hashed, by its identity.

    The write hashed the file at the source to the document's sha256 and took
    its identity with the hash (:func:`_check_source`): a loader bound to the
    document's bytes under another identity (the file it was extracted from,
    since touched or rewritten with the same bytes) is bound to that identity
    on a copy, so each draw takes the identity's fast path instead of hashing
    the file again. Any other loader, and an image with none, is as it is.
    """
    loader = _rebound(image._loader, source, sandbox=sandbox)
    bands = _rebound(image._band_loader, source, sandbox=sandbox)
    if loader is image._loader and bands is image._band_loader:
        return image
    return dataclasses.replace(image, _loader=loader, _band_loader=bands)


def _rebound(loader: Callable[[], _T] | None, source: PDFSource, *, sandbox: bool = False) -> Callable[[], _T] | None:
    """``loader`` on a copy of its owner holding the hashed file's identity (and ``sandbox``), if needed."""
    owner = getattr(loader, "__self__", None)
    if not isinstance(owner, (PageDraw, PageRender)):
        return loader
    rebind_id = owner.sha256 == source.sha256 and owner.identity != source.identity
    rebind_sandbox = owner.sandbox != sandbox
    if not rebind_id and not rebind_sandbox:
        return loader
    new_id = source.identity if rebind_id else owner.identity
    updated = dataclasses.replace(owner, identity=new_id, sandbox=sandbox)
    method: Callable[[], _T] = MethodType(getattr(loader, "__func__"), updated)
    return method


def _rendered_outcome(page: Page, staging: Path, source: PDFSource, options: PipelineOptions) -> _PageOutcome:
    """Render the page at ``dpi`` and write its PNG (no bands); a refusal or failure is recorded."""
    number = page.page_number
    try:
        dpi, drawn = _drawn(source, page, options.dpi, options.max_render_pixels, sandbox=options.sandbox)
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
        failed = "the page was not rendered" if _drawn_on_read(image) else "the page's own image was not written"
        return _PageOutcome(None, (), (_Missing("page_image", number, None, f"{failed}: {why}"),))
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
    """``"render"`` for a render: a page image drawn on read, or one an earlier bundle rendered; else ``"document"``.

    A digital or hybrid page's image (P2-A) is drawn on read by its
    :class:`~datagrunt.pdf._loaders.PageDraw`; an earlier bundle's render is
    read back by its reader. A render has no bands, so none is expected of
    it: it is written as the render it is, at its own DPI.
    """
    loader = image._loader
    reread = isinstance(loader, _ArtifactReader) and loader.origin == "render"
    return "render" if _drawn_on_read(image) or reread else "document"


def _drawn_on_read(image: PageImage) -> bool:
    """True if the page image is drawn when read: its loader is a PageDraw's (P2-A)."""
    return _page_draw(image) is not None


def _page_draw(image: PageImage) -> PageDraw | None:
    """The PageDraw whose ``png_bytes`` is the page image's loader, or None."""
    owner = getattr(image._loader, "__self__", None)
    return owner if isinstance(owner, PageDraw) else None


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
    return _loaders_unbound((image._loader, image._band_loader), source, "renders")


def _figure_unbound(figure: Figure, source: Path | None) -> str:
    """Why the figure's PNG is not bound to ``source``, or ``""`` if it is: as :func:`_unbound` binds a page image.

    Its PNG must come from a loader (a bare ``image_path`` is read
    unverified, whatever file it names) that reads from ``source`` itself
    (A5a's ``FigureRead`` names the PDF it reads as ``source``; compared by
    resolved path) or is a bundle's verifying reader. With no ``source`` (a
    ``PDFWriter`` built from a document has none) the loader must still name
    the PDF it reads, or be a bundle's verifying reader.
    """
    if figure._loader is None:
        return "has no loader"
    return _loaders_unbound((figure._loader,), source, "reads")


def _loaders_unbound(loaders: tuple[object, ...], source: Path | None, verb: str) -> str:
    """Why one of ``loaders`` (None ones skipped) is not bound to ``source``, or ``""``; ``verb`` says what it does."""
    target = None if source is None else source.resolve()
    for loader in loaders:
        owner = getattr(loader, "__self__", loader)
        if loader is None or isinstance(owner, (_ArtifactReader, _BandsReader)):
            continue
        origin = getattr(owner, "source", None)
        if not isinstance(origin, (str, os.PathLike)):
            return "has a loader that names no source"
        resolved = Path(origin).resolve()
        if target is not None and resolved != target:
            return f"{verb} from {resolved}, not from the source {target}"
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


def _drawn(
    source: PDFSource, page: Page, target_dpi: int, max_pixels: int, *, sandbox: bool = False
) -> tuple[int, RenderedPage]:
    """The page drawn at the effective DPI of ``target_dpi``, and that DPI."""
    dpi = effective_dpi(target_dpi, max_pixels, page.width_pt, page.height_pt)
    if not sandbox and render_page is _default_render_page:
        from datagrunt.pdf import _render_worker

        drawn = _render_worker.draw_page_in_process(
            source.path,
            page.page_number - 1,
            dpi=dpi,
            password=source.password,
            memory_cap_bytes=_MEMORY_CAP_BYTES,
            annotations=True,
        )
        return dpi, drawn
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
    """``document`` with its figures' (``figures``: by element id) and pages' paths at ``base`` (sections too)."""
    pages = tuple(_page_with_paths(page, figures, renders.get(page.page_number), base) for page in document.pages)
    return _with_pages(document, pages)


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
    relative = figures.get(figure.element_id)
    return dataclasses.replace(figure, image_path=None if relative is None else _joined(base, relative))


def _joined(base: Path, relative: str) -> Path:
    return base.joinpath(*relative.split("/"))


def _datagrunt_version() -> str:
    """The extractor's version: ``datagrunt.__version__``, the code's own.

    Never the installed package metadata: an editable install keeps the
    version it was installed at while the code moves on, and reuse compares
    this version.
    """
    return datagrunt.__version__


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
        "image_only_pages": list(document.image_only_pages),
        "page_images": images,
        "artifacts": [entry._asdict() for entry in sorted(ledger.artifacts)],
        "missing": [entry._asdict() for entry in sorted(ledger.missing, key=_missing_order)],
    }
    return json.dumps(data, ensure_ascii=True, allow_nan=False, indent=2).encode("ascii")
