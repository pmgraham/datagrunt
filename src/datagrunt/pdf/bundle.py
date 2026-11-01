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
- ``page_images/page_NNNN.png``: every page, rendered at
  :func:`~datagrunt.pdf.render.effective_dpi` of ``dpi`` (and
  ``max_render_pixels``) in A5a's guarded worker, 8-bit grey as drawn.
- ``page_images/bands/page_NNNN_band_NN.png``: each vision page (route
  ``"vision"``) rendered again at the effective ``vision_dpi``, normalized
  (:func:`~datagrunt.pdf.ocr.normalized`) and cut into bands
  (:func:`~datagrunt.pdf.ocr.bands`), numbered from 1.
- ``manifest.json``: :data:`BUNDLE_FORMAT_VERSION`, the datagrunt version,
  the source's sha256, the extraction parameters (``dpi``, ``vision_dpi``,
  ``max_render_pixels``, ``workers``), ``integrity_verified`` and
  ``degraded_pages``, every other file's path, size and sha256 (by path),
  and each artifact that is missing and why (by page).

PNGs are encoded with Pillow; PDF content is decoded only by A1's loaders
and A5a's guarded render. Each file is created exclusively and streamed
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

Pages render in chunks of :func:`_chunk_size` (``max(4, 32 // workers)``)
pages, on ``workers`` threads (each render is a worker process of A5a's).
Each page's PNGs are written in its own thread, so no page's pixels outlive
its render; a chunk is done before the next starts.

The returned document is the input with paths set: each figure's
``image_path`` (in ``sections`` too), and each page's ``page_image``, which
the bundle's render replaces: its ``dpi``, size and ``image_path`` are the
page render's, ``contrast_normalized`` is False, and ``band_paths`` are the
vision bands (A1's PageImage has one DPI: the bands' is the effective
``vision_dpi``, from the manifest's parameters).

Failures are recorded, never silent: a page whose render is refused or
fails (``RenderRefused``, ``RenderFailed``) has no page image (nor bands);
a vision page whose band render is refused or fails has no bands; a figure
whose bytes cannot be read (a ``DatagruntPDFError`` or ``OSError``) or are
not a PNG is not written (its figures keep no ``image_path``). Each is a
``missing`` entry naming the artifact, the page, the figure's element id
(figures) and why. Any other error fails the write.
"""

import dataclasses
import errno
import hashlib
import importlib.metadata
import json
import os
import shutil
import uuid
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, BinaryIO, NamedTuple, cast

from PIL import Image

from datagrunt.pdf.codec import document_to_json_bytes
from datagrunt.pdf.elements import Figure, Page, PageImage, PDFDocument
from datagrunt.pdf.exceptions import DatagruntPDFError
from datagrunt.pdf.frame import utf8_text
from datagrunt.pdf.ocr import Band, bands, normalized
from datagrunt.pdf.pdfium_engine import _checked_path
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, _opened_source, _stream_sha256
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page

__all__ = ["BUNDLE_FORMAT_VERSION", "write_bundle"]

BUNDLE_FORMAT_VERSION = "5.0"
MANIFEST = "manifest.json"
DOCUMENT_JSON = "json/document.json"
DOCUMENT_MARKDOWN = "markdown/document.md"

_STAGING_PREFIX = ".tmp_"
# A render worker's memory past its start, and its time: A5b's vision route's.
_MEMORY_CAP_BYTES = 2 << 30
_RENDER_TIMEOUT_S = 120.0
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


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


class _Render(NamedTuple):
    """A page's render, written: its PageImage's facts and relative paths."""

    page_number: int
    dpi: int
    width_px: int
    height_px: int
    image: str
    bands: tuple[str, ...]


class _PageOutcome(NamedTuple):
    """What one page's render task wrote, and what it could not."""

    render: _Render | None
    artifacts: tuple[_Artifact, ...]
    missing: tuple[_Missing, ...]


@dataclass
class _Ledger:
    """What one write put in its staging directory, and what it could not (local to the write)."""

    artifacts: list[_Artifact] = field(default_factory=list)
    missing: list[_Missing] = field(default_factory=list)


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
) -> PDFDocument:
    """Write ``document``, extracted from ``source``, as a bundle at ``output_dir``, atomically.

    See the module notes for the layout, the checks, the atomic write, the
    chunks and the failures recorded. ``password`` opens ``source`` for the
    renders.

    Returns:
        ``document`` with paths set (see the module notes).

    Raises:
        TypeError, ValueError: If an option is invalid (as ``PipelineOptions``
            raises it), ``document`` is not a PDFDocument, ``output_dir`` or
            ``source`` is not a path, or ``source`` does not hash to the
            document's sha256.
        FileExistsError: If ``output_dir`` exists and is not an empty
            directory (a symlink is refused too), before or after the write.
        FileNotFoundError: If ``source`` or ``output_dir``'s parent is missing.
        OSError: If a file cannot be written, or the rename fails.
    """
    options = PipelineOptions(workers=workers, dpi=dpi, vision_dpi=vision_dpi, max_render_pixels=max_render_pixels)
    if not isinstance(document, PDFDocument):
        raise TypeError(f"document must be a PDFDocument, got {type(document).__name__}")
    target = _checked_target(output_dir)
    pdf = PDFSource(path=_checked_path(source, "source"), password=password)
    _check_source(pdf, document.metadata.sha256)
    staging = target.parent / f"{_STAGING_PREFIX}{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        written = _staged(document, staging, target, pdf, options)
        _refuse_existing(target)
        os.replace(staging, target)
    except BaseException:
        shutil.rmtree(staging)
        raise
    return written


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


def _check_source(source: PDFSource, expected: str) -> None:
    """Refuse a source whose bytes are not the document's: its renders would be another PDF's pages."""
    with _opened_source(source.path) as stream:
        digest = _stream_sha256(stream, os.fstat(stream.fileno()).st_size, source.path)
    if digest != expected:
        raise ValueError(
            f"source {source.path} has sha256 {digest}, not the document's {expected}: "
            "its renders would not be the document's pages"
        )


def _staged(
    document: PDFDocument, staging: Path, target: Path, source: PDFSource, options: PipelineOptions
) -> PDFDocument:
    """Write every artifact, then the manifest, into ``staging``; the document with paths in ``target``."""
    ledger = _Ledger()
    figures = _write_figures(document, staging, ledger)
    renders = _write_renders(document, staging, source, options, ledger)
    as_json = document_to_json_bytes(_with_paths(document, figures, renders, Path()))
    ledger.artifacts.append(_write_bytes(staging, DOCUMENT_JSON, as_json))
    markdown = utf8_text(_with_paths(document, figures, renders, Path("..")).to_markdown())
    ledger.artifacts.append(_write_bytes(staging, DOCUMENT_MARKDOWN, markdown.encode("utf-8")))
    _write_bytes(staging, MANIFEST, _manifest(document, options, ledger))
    return _with_paths(document, figures, renders, target)


def _write_figures(document: PDFDocument, staging: Path, ledger: _Ledger) -> dict[str, str]:
    """Write each distinct figure (by sha256) once; its relative path by sha256."""
    written: dict[str, str] = {}
    tried: set[str] = set()
    for figure in document.figures:
        if figure.sha256 in tried:
            continue
        tried.add(figure.sha256)
        data, why = _figure_png(figure)
        if data is None:
            reason = f"figure {figure.sha256} was not written: {why}"
            ledger.missing.append(_Missing("figure", figure.page_number, figure.element_id, reason))
            continue
        relative = f"images/{figure.sha256}.png"
        ledger.artifacts.append(_write_bytes(staging, relative, data))
        written[figure.sha256] = relative
    return written


def _figure_png(figure: Figure) -> tuple[bytes | None, str]:
    """The figure's PNG bytes, or None and why they cannot be read."""
    try:
        data = figure.read_bytes()
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
) -> dict[int, _Render]:
    """Render and write every page, chunk by chunk on ``workers`` threads; each page's render by number."""
    renders: dict[int, _Render] = {}
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


def _record(outcome: _PageOutcome, renders: dict[int, _Render], ledger: _Ledger) -> None:
    ledger.artifacts.extend(outcome.artifacts)
    ledger.missing.extend(outcome.missing)
    if outcome.render is not None:
        renders[outcome.render.page_number] = outcome.render


def _page_outcome(page: Page, staging: Path, source: PDFSource, options: PipelineOptions) -> _PageOutcome:
    """Render the page (and a vision page's bands) and write their PNGs; a refusal or failure is recorded."""
    number = page.page_number
    try:
        dpi, drawn = _drawn(source, page, options.dpi, options.max_render_pixels)
    except (RenderRefused, RenderFailed) as exc:
        reason = f"the page was not rendered: {type(exc).__name__}: {exc}"
        return _PageOutcome(None, (), (_Missing("page_image", number, None, reason),))
    image = f"page_images/page_{number:04d}.png"
    width, height = drawn.px_width, drawn.px_height
    artifacts = [_write_png(staging, image, drawn)]
    del drawn  # its pixels are written: they need not outlive the page's bands
    band_artifacts: tuple[_Artifact, ...] = ()
    missing: tuple[_Missing, ...] = ()
    if page.route == "vision":
        band_artifacts, missing = _write_bands(page, staging, source, options)
    render = _Render(number, dpi, width, height, image, tuple(item.path for item in band_artifacts))
    return _PageOutcome(render, (*artifacts, *band_artifacts), missing)


def _write_bands(
    page: Page, staging: Path, source: PDFSource, options: PipelineOptions
) -> tuple[tuple[_Artifact, ...], tuple[_Missing, ...]]:
    """Render the vision page at the vision DPI, normalize it, cut it into bands and write them."""
    number = page.page_number
    try:
        _dpi, drawn = _drawn(source, page, options.vision_dpi, options.max_render_pixels)
    except (RenderRefused, RenderFailed) as exc:
        reason = f"the page's bands were not rendered: {type(exc).__name__}: {exc}"
        return (), (_Missing("bands", number, None, reason),)
    cut = bands(normalized(drawn))
    del drawn
    written = tuple(
        _write_png(staging, f"page_images/bands/page_{number:04d}_band_{index:02d}.png", band)
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


def _write_png(staging: Path, relative: str, image: RenderedPage | Band) -> _Artifact:
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
    document: PDFDocument, figures: Mapping[str, str], renders: Mapping[int, _Render], base: Path
) -> PDFDocument:
    """``document`` with its figures' and pages' paths at ``base`` (sections hold the same figures)."""
    pages = tuple(_page_with_paths(page, figures, renders.get(page.page_number), base) for page in document.pages)
    by_id = {element.element_id: element for page in pages for element in page.elements}
    sections = tuple(
        dataclasses.replace(section, elements=tuple(by_id[element.element_id] for element in section.elements))
        for section in document.sections
    )
    return dataclasses.replace(document, pages=pages, sections=sections)


def _page_with_paths(page: Page, figures: Mapping[str, str], render: _Render | None, base: Path) -> Page:
    elements = tuple(
        _figure_with_path(element, figures, base) if isinstance(element, Figure) else element
        for element in page.elements
    )
    image = None
    if render is not None:
        image = PageImage(
            page_number=render.page_number,
            dpi=float(render.dpi),
            width_px=render.width_px,
            height_px=render.height_px,
            image_path=_joined(base, render.image),
            band_paths=tuple(_joined(base, band) for band in render.bands),
            contrast_normalized=False,
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


def _manifest(document: PDFDocument, options: PipelineOptions, ledger: _Ledger) -> bytes:
    """The manifest's bytes: ASCII JSON (see the module notes)."""
    data = {
        "bundle_format_version": BUNDLE_FORMAT_VERSION,
        "datagrunt_version": _datagrunt_version(),
        "source_sha256": document.metadata.sha256,
        "extraction_params": {
            "dpi": options.dpi,
            "vision_dpi": options.vision_dpi,
            "max_render_pixels": options.max_render_pixels,
            "workers": options.workers,
        },
        "integrity_verified": document.integrity_verified,
        "degraded_pages": list(document.degraded_pages),
        "artifacts": [entry._asdict() for entry in sorted(ledger.artifacts)],
        "missing": [entry._asdict() for entry in sorted(ledger.missing, key=_missing_order)],
    }
    return json.dumps(data, ensure_ascii=True, allow_nan=False, indent=2).encode("ascii")
