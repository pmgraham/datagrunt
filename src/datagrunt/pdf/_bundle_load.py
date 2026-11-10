"""Stdlib/core-only bundle reader (#463, 5.2): :func:`load_bundle` and its verification helpers.

Internal: ``datagrunt.pdf.bundle`` re-exports :func:`load_bundle` and the
shared manifest/artifact structures from this module so that loading a bundle
never imports ``Pillow`` or ``pypdfium2``. See :mod:`datagrunt.pdf.bundle` for
the full bundle layout, its formats (``"5.3"`` written; ``"5.0"`` still
read: :data:`_FORMATS`) and verification invariants.
"""

import dataclasses
import errno
import hashlib
import math
import os
import stat
import sys
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import NamedTuple, TypeVar

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
    parse_json,
)
from datagrunt.pdf.elements import _MAX_ASSET_BYTES, Figure, Page, PageImage, PDFDocument
from datagrunt.pdf.exceptions import BundleCorruptError
from datagrunt.pdf.pipeline import PipelineOptions

__all__ = [
    "BUNDLE_FORMAT_VERSION",
    "DOCUMENT_JSON",
    "DOCUMENT_MARKDOWN",
    "ELEMENTS_JSONL",
    "MANIFEST",
    "load_bundle",
]

# The bundle format written: "5.3" (#470) adds the manifest's image_only_pages and holds document JSON version 2.
BUNDLE_FORMAT_VERSION = "5.3"
# The format datagrunt 5.0 to 5.2 wrote, which still loads (document JSON version 1).
_FORMAT_5_0 = "5.0"
MANIFEST = "manifest.json"
DOCUMENT_JSON = "json/document.json"
ELEMENTS_JSONL = "json/elements.jsonl"
DOCUMENT_MARKDOWN = "markdown/document.md"
_PAGE_MARKDOWN_DIR = "markdown/pages/"

# The most bytes of any one file a load reads (the manifest, an artifact): A1's asset limit.
_MAX_ARTIFACT_BYTES = _MAX_ASSET_BYTES
_READ_CHUNK_BYTES = 1 << 20
# How every bundle file is opened: never through a symlink (O_NOFOLLOW), never blocking on a named pipe
# (O_NONBLOCK, which a regular file ignores), as bytes (O_BINARY, Windows). Each flag is 0 where it does not exist.
_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
# A manifest's keys under format 5.0, then under 5.3, which adds image_only_pages after degraded_pages.
_MANIFEST_KEYS_5_0 = (
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
_MANIFEST_KEYS = (
    "bundle_format_version",
    "datagrunt_version",
    "source_sha256",
    "extraction_params",
    "integrity_verified",
    "degraded_pages",
    "image_only_pages",
    "page_images",
    "artifacts",
    "missing",
)
_PARAM_KEYS = ("dpi", "vision_dpi", "max_render_pixels", "workers")
_RUN_PARAM_KEYS = ("workers",)
_MISSING_ARTIFACTS = ("bands", "figure", "page_image")
# Where a page image came from: the document's own (its loaders), or the bundle's render at dpi.
_ORIGINS = ("document", "render")
_T = TypeVar("_T")


class _Format(NamedTuple):
    """A bundle format this datagrunt reads: its manifest's keys (exactly) and its document JSON's codec version."""

    manifest_keys: tuple[str, ...]
    codec_version: int


_FORMATS: Mapping[str, _Format] = MappingProxyType(
    {_FORMAT_5_0: _Format(_MANIFEST_KEYS_5_0, 1), BUNDLE_FORMAT_VERSION: _Format(_MANIFEST_KEYS, 2)}
)


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


class _ImageEntry(NamedTuple):
    """A manifest's record of one page image: where it came from, its DPI and normalization."""

    page_number: int
    origin: str
    dpi: float
    contrast_normalized: bool


class _Manifest(NamedTuple):
    """A manifest's values, checked (see the module notes); ``image_only_pages`` is None under format 5.0."""

    bundle_format_version: str
    datagrunt_version: str
    source_sha256: str
    extraction_params: dict[str, object]
    integrity_verified: bool
    degraded_pages: tuple[int, ...]
    image_only_pages: tuple[int, ...] | None
    page_images: tuple[_ImageEntry, ...]
    artifacts: tuple[_Artifact, ...]
    missing: tuple[_Missing, ...]


def _bundle_attr(name: str, default: _T) -> _T:
    """Return ``name`` from ``datagrunt.pdf.bundle`` when loaded (honouring monkeypatches), else ``default``."""
    module = sys.modules.get("datagrunt.pdf.bundle")
    if module is None:
        return default
    return getattr(module, name, default)


def _json_scalar(name: str, value: object) -> object:
    """``value`` if it is a JSON scalar: a str, int, finite float, bool or None."""
    if value is None or isinstance(value, (str, int)):
        return value
    if not isinstance(value, float):
        raise TypeError(f"{name} must be a str, int, float, bool or None, got {type(value).__name__}")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    return value


def _page_markdown(page_number: int) -> str:
    """A page's Markdown file in the bundle."""
    return f"{_PAGE_MARKDOWN_DIR}page_{page_number:04d}.md"


def _with_pages(document: PDFDocument, pages: tuple[Page, ...]) -> PDFDocument:
    """``document`` with ``pages`` (the same elements, rebuilt), its sections holding the new elements."""
    by_id = {element.element_id: element for page in pages for element in page.elements}
    sections = tuple(
        dataclasses.replace(section, elements=tuple(by_id[element.element_id] for element in section.elements))
        for section in document.sections
    )
    return dataclasses.replace(document, pages=pages, sections=sections)


def load_bundle(path: str | os.PathLike[str]) -> PDFDocument:
    """The document of the bundle at ``path`` (its directory or its ``manifest.json``), verified.

    See :mod:`datagrunt.pdf.bundle` for every check.

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike.
        FileNotFoundError: If ``path`` does not exist, or the directory has
            no ``manifest.json``.
        ValueError: If ``path`` is a file other than a ``manifest.json``.
        BundleCorruptError: If the bundle is not what its manifest says
            (naming the artifact).
    """
    _manifest, document = _loaded(_bundle_root(path))
    return document


def _loaded(root: Path) -> tuple[_Manifest, PDFDocument]:
    """The bundle at ``root``'s manifest and its document, every check done (see the module notes)."""
    manifest = _read_manifest(root)
    raw = b""
    for entry in manifest.artifacts:
        if entry.path == DOCUMENT_JSON:
            raw = _verified(root, entry, keep=True)
        else:
            _verified(root, entry, keep=False)
    try:
        codec_version = _FORMATS[manifest.bundle_format_version].codec_version
        document = document_from_json_bytes(raw, base_dir=root, version=codec_version)
    except (TypeError, ValueError) as exc:
        raise BundleCorruptError(f"{DOCUMENT_JSON}: {exc}") from exc
    by_path = {entry.path: entry for entry in manifest.artifacts}
    _check_facts(document, manifest)
    _check_paths(document, set(by_path), root)
    _check_page_markdown(document, set(by_path))
    _check_page_images(document, manifest.page_images)
    origins = {entry.page_number: entry.origin for entry in manifest.page_images}
    _check_missing(document, manifest.missing, origins)
    return manifest, _with_readers(document, root, by_path, origins)


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
    max_bytes = _bundle_attr("_MAX_ARTIFACT_BYTES", _MAX_ARTIFACT_BYTES)
    read_fd = _bundle_attr("_read_fd", _read_fd)
    descriptor = _opened(root, MANIFEST)
    try:
        _regular(descriptor, MANIFEST)
        total, _digest, raw = read_fd(descriptor, max_bytes, keep=True)
    finally:
        os.close(descriptor)
    if total > max_bytes:
        raise BundleCorruptError(f"{MANIFEST}: it is over the {max_bytes}-byte asset limit")
    try:
        return _manifest_of(parse_json(raw, MANIFEST), root)
    except (TypeError, ValueError) as exc:
        raise BundleCorruptError(f"{MANIFEST}: {exc}") from exc


def _manifest_of(data: object, root: Path) -> _Manifest:
    """The manifest's values, each checked: TypeError or ValueError naming the first that is wrong."""
    version = _format_version(data)
    top = _object("manifest", data, _FORMATS[version].manifest_keys)
    datagrunt_version = non_blank_str("datagrunt_version", top["datagrunt_version"])
    _check_params(top["extraction_params"])
    missing = enumerate(_array("missing", top["missing"]))
    return _Manifest(
        bundle_format_version=version,
        datagrunt_version=datagrunt_version,
        source_sha256=sha256_hex("source_sha256", top["source_sha256"]),
        extraction_params=dict(top["extraction_params"]),
        integrity_verified=strict_bool("integrity_verified", top["integrity_verified"]),
        degraded_pages=_page_numbers("degraded_pages", top["degraded_pages"]),
        image_only_pages=None if version == _FORMAT_5_0 else _page_numbers("image_only_pages", top["image_only_pages"]),
        page_images=_image_entries(top["page_images"]),
        artifacts=_listed_artifacts(top["artifacts"], root),
        missing=tuple(_missing_entry(f"missing[{i}]", item) for i, item in missing),
    )


def _format_version(data: object) -> str:
    """The manifest's ``bundle_format_version``, if it is a format this datagrunt reads (:data:`_FORMATS`)."""
    if not isinstance(data, dict):
        raise TypeError(f"manifest must be a JSON object, got {type(data).__name__}")
    if "bundle_format_version" not in data:
        raise ValueError("manifest has missing key(s) ['bundle_format_version']")
    version = data["bundle_format_version"]
    if not isinstance(version, str) or version not in _FORMATS:
        known = ", ".join(repr(name) for name in _FORMATS)
        raise ValueError(f"bundle_format_version {version!r} is not supported: this datagrunt reads {known}")
    return version


def _page_numbers(name: str, value: object) -> tuple[int, ...]:
    """A manifest's list of 1-based page numbers (``degraded_pages``, ``image_only_pages``), each checked."""
    return tuple(int_at_least(f"{name}[{i}]", page, 1) for i, page in enumerate(_array(name, value)))


def _same_output_params(recorded: Mapping[str, object], wanted: Mapping[str, object]) -> bool:
    """True when ``recorded`` and ``wanted`` agree on every output-affecting parameter (ignoring ``workers``)."""
    if not isinstance(recorded, Mapping) or not isinstance(wanted, Mapping):
        raise TypeError("extraction_params must be Mappings")
    return {k: v for k, v in recorded.items() if k not in _RUN_PARAM_KEYS} == {
        k: v for k, v in wanted.items() if k not in _RUN_PARAM_KEYS
    }


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
    for required in (DOCUMENT_JSON, ELEMENTS_JSONL, DOCUMENT_MARKDOWN):
        if required not in seen:
            raise ValueError(f"artifacts lists no {required}")
    return tuple(listed)


def _artifact_entry(name: str, value: object, root: Path) -> _Artifact:
    max_bytes = _bundle_attr("_MAX_ARTIFACT_BYTES", _MAX_ARTIFACT_BYTES)
    data = _object(name, value, _Artifact._fields)
    contained_path(f"{name}.path", data["path"], root)
    size = int_at_least(f"{name}.size", data["size"], 0)
    if size > max_bytes:
        raise ValueError(f"{name} ({data['path']!r}): size {size} is over the {max_bytes}-byte asset limit")
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
    """A descriptor of the bundle file at ``relative``, opened safely (see the module notes); the caller closes it."""
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
    chunk_bytes = _bundle_attr("_READ_CHUNK_BYTES", _READ_CHUNK_BYTES)
    digest = hashlib.sha256()
    parts: list[bytes] = []
    total = 0
    for _ in range(limit // chunk_bytes + 2):
        block = os.read(descriptor, min(chunk_bytes, limit + 1 - total))
        if not block:
            break
        digest.update(block)
        total += len(block)
        if keep:
            parts.append(block)
    return total, digest.hexdigest(), b"".join(parts)


def _verified(root: Path, entry: _Artifact, *, keep: bool) -> bytes:
    """Open the artifact safely and check it is the manifest's (size, then sha256); its bytes if ``keep``, else b""."""
    read_fd = _bundle_attr("_read_fd", _read_fd)
    descriptor = _opened(root, entry.path)
    try:
        size = _regular(descriptor, entry.path)
        if size != entry.size:
            raise BundleCorruptError(f"{entry.path}: it is {size} bytes, the manifest says {entry.size}")
        total, digest, data = read_fd(descriptor, entry.size, keep=keep)
    finally:
        os.close(descriptor)
    if total != entry.size:
        raise BundleCorruptError(f"{entry.path}: it is {total} bytes, the manifest says {entry.size}")
    if digest != entry.sha256:
        raise BundleCorruptError(f"{entry.path}: its sha256 is {digest}, the manifest's is {entry.sha256}")
    return data


@dataclass(frozen=True, slots=True)
class _ArtifactReader:
    """A loaded figure's or page image's loader: the artifact's bytes, verified against the manifest on each read."""

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
        max_bytes = _bundle_attr("_MAX_ARTIFACT_BYTES", _MAX_ARTIFACT_BYTES)
        total = sum(entry.size for entry in self.entries)
        if total > max_bytes:
            limit = f"over the {max_bytes}-byte asset limit"
            raise BundleCorruptError(f"{self.entries[0].path} and its page's other bands total {total} bytes, {limit}")
        return tuple(_verified(self.root, entry, keep=True) for entry in self.entries)


def _with_readers(
    document: PDFDocument, root: Path, by_path: Mapping[str, _Artifact], origins: Mapping[int, str]
) -> PDFDocument:
    """``document`` whose figures and page images (paths in ``root``, kept) read through verifying loaders."""

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
    """The manifest's source sha256, integrity, degraded and (format 5.3) image-only pages must be the document's."""
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
    if manifest.image_only_pages is not None and manifest.image_only_pages != document.image_only_pages:
        mine, theirs = list(manifest.image_only_pages), list(document.image_only_pages)
        raise BundleCorruptError(f"{MANIFEST}: image_only_pages {mine} are not the document's {theirs}")


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


def _check_page_markdown(document: PDFDocument, listed: set[str]) -> None:
    """The manifest must list each page's Markdown, and no Markdown for a page the document does not have."""
    expected = {_page_markdown(page.page_number) for page in document.pages}
    present = {path for path in listed if path.startswith(_PAGE_MARKDOWN_DIR)}
    unlisted, stray = sorted(expected - present), sorted(present - expected)
    if unlisted:
        raise BundleCorruptError(f"{MANIFEST}: artifacts lists no {unlisted[0]}")
    if stray:
        raise BundleCorruptError(f"{MANIFEST}: {stray[0]} is not one of the document's pages' Markdown")


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
    """What the document lacks, as missing entries' keys: page images, own bands, figures (each its own)."""
    keys: set[tuple[str, int, str | None]] = set()
    for page in document.pages:
        image = page.page_image
        if image is None:
            keys.add(("page_image", page.page_number, None))
        elif origins.get(page.page_number) == "document" and not image.band_paths:
            keys.add(("bands", page.page_number, None))
    for figure in document.figures:
        if figure.image_path is None:
            keys.add(("figure", figure.page_number, figure.element_id))
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


def _doc_page_count(self: PDFDocument) -> int:
    """Return the document's page count from its metadata."""
    return self.metadata.page_count


if not hasattr(PDFDocument, "page_count"):
    setattr(PDFDocument, "page_count", property(_doc_page_count))

load_bundle.__module__ = "datagrunt.pdf.bundle"
