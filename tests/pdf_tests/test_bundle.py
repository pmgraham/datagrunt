"""Tests for the v5 bundle write (#371, A7a): ``datagrunt.pdf.bundle.write_bundle``.

The source is a ReportLab PDF of ``rich_document``'s page sizes, and the
document is ``rich_document`` bound to it (its sha256), with figure loaders
and page 3's own page-image loaders returning fixed PNGs. Renders are real
(A5a's guarded worker) where their bytes are checked; elsewhere
``bundle.render_page`` is a fast stand-in (:func:`fake_render`). A5b's vision
pages come from ``extract_document(ocr="always")`` (tesseract needed). A
failing stage is a ``monkeypatch`` of the bundle's module name for it.
"""

import dataclasses
import hashlib
import importlib.metadata
import json
import os
import re
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from datagrunt.pdf import _loaders, bundle
from datagrunt.pdf.bundle import BUNDLE_FORMAT_VERSION, load_bundle, write_bundle
from datagrunt.pdf.codec import document_from_json_bytes
from datagrunt.pdf.elements import Figure, PDFDocument
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFSourceChangedError
from datagrunt.pdf.frame import utf8_text
from datagrunt.pdf.ocr import tesseract_available
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page, render_size
from datagrunt.pdf.writer import PDFWriter
from pdf_tests.builders import PageSpec, TextRun, build_pdf, hand_written_pdf, solid_png
from pdf_tests.documents import FIGURE_A_SHA, FIGURE_B_SHA, PAGE_SIZES, metadata, rich_document, surrogate_document
from pdf_tests.test_layout_stream_tables import body_text

PNG_A = solid_png(64, 32, (0.2, 0.4, 0.6))
PNG_B = solid_png(16, 16, (0.9, 0.1, 0.1))
# Page 3's own page image (as A5b's loaders would give it) and its bands.
OWN_IMAGE = solid_png(100, 130, (0.5, 0.5, 0.5))
OWN_BANDS = (solid_png(100, 70, (0.1, 0.1, 0.1)), solid_png(100, 70, (0.9, 0.9, 0.9)))
DPI, VISION_DPI = 72, 100
STAGING = re.compile(r"\.tmp_[0-9a-f]{32}")
CAP, TIMEOUT = 2 << 30, 120.0


def source_pdf(directory: Path, *, user_password=None, name="report.pdf") -> Path:
    """A PDF of rich_document's page sizes, one line of text a page."""
    pages = [
        PageSpec(width=width, height=height, items=(TextRun(f"Page {number}", 72.0, 100.0),))
        for number, (width, height) in enumerate(PAGE_SIZES, 1)
    ]
    return build_pdf(directory / name, pages, user_password=user_password)


def sha_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclasses.dataclass(frozen=True)
class OwnRender:
    """A stand-in for A5b's PageRender: the PDF it renders from, and the image and bands it gives (or raises)."""

    source: Path
    image: object = OWN_IMAGE
    bands: object = OWN_BANDS

    def png_bytes(self) -> bytes:
        return self._given(self.image)

    def band_png_bytes(self):
        return self._given(self.bands)

    @staticmethod
    def _given(value):
        if isinstance(value, BaseException):
            raise value
        return value


@dataclasses.dataclass(frozen=True)
class OwnFigure:
    """A stand-in for A5a's FigureRead: the PDF it reads a figure from, and what its PNG read gives."""

    source: Path
    read: Callable[[], bytes]

    def png_bytes(self) -> bytes:
        return self.read()


def bound_document(
    path: Path,
    figure_loader=lambda: PNG_A,
    other_loader=lambda: PNG_B,
    *,
    image=OWN_IMAGE,
    bands=OWN_BANDS,
    own_source: Path | None = None,
) -> PDFDocument:
    """rich_document as the document of the PDF at ``path``; page 3 has its own page image.

    Its figures read through an :class:`OwnFigure` of ``path`` calling ``figure_loader`` (figure A, twice)
    and ``other_loader`` (figure B); None gives a figure no loader. Page 3's loaders are an
    :class:`OwnRender` of ``own_source`` (default: ``path``); ``bands`` None gives it no band loader.
    """
    render = OwnRender(own_source or path, image, bands)
    return rich_document(
        None if figure_loader is None else OwnFigure(path, figure_loader).png_bytes,
        None if other_loader is None else OwnFigure(path, other_loader).png_bytes,
        sha=sha_of(path),
        source_name=path.name,
        image_loader=render.png_bytes,
        band_loader=None if bands is None else render.band_png_bytes,
    )


# One 64 x 64 image stream, which each dictionary below reads as another picture (or none): one sha256, many PNGs.
SHARED_STREAM = bytes([10, 200, 10]) * (64 * 64)
UNREADABLE = b"/Width 64 /Height 64 /ColorSpace /NoSuchSpace /BitsPerComponent 8"
AS_RGB = b"/Width 64 /Height 64 /ColorSpace /DeviceRGB /BitsPerComponent 8"
AS_GREY = b"/Width 64 /Height 64 /ColorSpace /DeviceGray /BitsPerComponent 8"


def shared_stream_pdf(path: Path, first: bytes, second: bytes) -> Path:
    """Two pages, each drawing one image: the same stream bytes under the image dictionaries ``first``, ``second``."""
    page = (
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 5 0 R "
        b"/Resources << /XObject << /Im0 %d 0 R >> >> >>"
    )

    def stream(body: bytes, extra: bytes = b"") -> bytes:
        return b"<< /Length %d %s >>\nstream\n%s\nendstream" % (len(body), extra, body)

    image = b"/Type /XObject /Subtype /Image "
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R 4 0 R] /Count 2 >>",
        page % 6,
        page % 7,
        stream(b"q 200 0 0 200 100 400 cm /Im0 Do Q"),
        stream(SHARED_STREAM, image + first),
        stream(SHARED_STREAM, image + second),
    ]
    return hand_written_pdf(path, objects)


def with_element(document: PDFDocument, element_id: str, **changes) -> PDFDocument:
    """``document`` with its element ``element_id`` changed by ``changes``; its sections hold the new one."""
    pages = tuple(
        dataclasses.replace(
            page,
            elements=tuple(
                dataclasses.replace(item, **changes) if item.element_id == element_id else item
                for item in page.elements
            ),
        )
        for page in document.pages
    )
    by_id = {item.element_id: item for page in pages for item in page.elements}
    sections = tuple(
        dataclasses.replace(section, elements=tuple(by_id[item.element_id] for item in section.elements))
        for section in document.sections
    )
    return dataclasses.replace(document, pages=pages, sections=sections)


def files_in(root: Path) -> set[str]:
    return {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}


def stagings(directory: Path) -> list[Path]:
    return [path for path in directory.iterdir() if STAGING.fullmatch(path.name)]


def write(document, out, source, **options):
    options.setdefault("dpi", DPI)
    options.setdefault("vision_dpi", VISION_DPI)
    return write_bundle(document, out, source=source, **options)


def fake_render(source, page_index, *, dpi, password=None, memory_cap_bytes, timeout_s):
    """A fast stand-in for render_page: a grey page dpi/10 x dpi/5 px."""
    width, height = max(1, dpi // 10), max(1, dpi // 5)
    return RenderedPage(pixels=bytes([200]) * (width * height), px_width=width, px_height=height)


@pytest.fixture
def fast_render(monkeypatch):
    monkeypatch.setattr(bundle, "render_page", fake_render)


@pytest.fixture
def source(tmp_path):
    return source_pdf(tmp_path)


def manifest_of(out: Path) -> dict:
    return json.loads((out / "manifest.json").read_text(encoding="ascii"))


def png_pixels(path: Path) -> tuple[str, tuple[int, int], bytes]:
    with Image.open(path) as image:
        return image.mode, image.size, image.tobytes()


def with_figure_base(document: PDFDocument, base: Path) -> PDFDocument:
    """``document`` with each figure's image_path set to base / images / <sha>.png."""
    pages = []
    for page in document.pages:
        elements = tuple(
            dataclasses.replace(e, image_path=base / "images" / f"{e.sha256}.png") if isinstance(e, Figure) else e
            for e in page.elements
        )
        pages.append(dataclasses.replace(page, elements=elements, page_image=None))
    return dataclasses.replace(document, pages=tuple(pages))


@pytest.fixture(scope="module")
def written(tmp_path_factory):
    """One bundle of rich_document, rendered for real at 72 / 100 DPI."""
    directory = tmp_path_factory.mktemp("written")
    path = source_pdf(directory)
    calls = {"a": 0, "b": 0}

    def loader_a():
        calls["a"] += 1
        return PNG_A

    def loader_b():
        calls["b"] += 1
        return PNG_B

    document = bound_document(path, loader_a, loader_b)
    out = directory / "bundle"
    returned = write(document, out, path)
    return SimpleNamespace(source=path, document=document, out=out, returned=returned, calls=calls)


EXPECTED_FILES = {
    "manifest.json",
    "json/document.json",
    "json/elements.jsonl",
    "markdown/document.md",
    *(f"markdown/pages/page_000{n}.md" for n in (1, 2, 3, 4)),
    f"images/{FIGURE_A_SHA}.png",
    f"images/{FIGURE_B_SHA}.png",
    *(f"page_images/page_000{n}.png" for n in (1, 2, 3, 4)),
    *(f"page_images/bands/page_0003_band_0{k}.png" for k in (1, 2)),
}
# The pages without their own page image, which the bundle renders.
RENDERED = (1, 2, 4)


class TestLayout:
    def test_bundle_holds_exactly_the_planned_files(self, written):
        assert files_in(written.out) == EXPECTED_FILES

    def test_nothing_else_is_left_beside_it(self, written):
        assert sorted(path.name for path in written.out.parent.iterdir()) == ["bundle", "report.pdf"]


class TestManifest:
    def test_manifest_keys(self, written):
        assert list(manifest_of(written.out)) == [
            "bundle_format_version",
            "datagrunt_version",
            "source_sha256",
            "extraction_params",
            "integrity_verified",
            "degraded_pages",
            "page_images",
            "artifacts",
            "missing",
        ]

    def test_manifest_values(self, written):
        manifest = manifest_of(written.out)
        assert manifest["bundle_format_version"] == BUNDLE_FORMAT_VERSION == "5.0"
        assert manifest["datagrunt_version"] == importlib.metadata.version("datagrunt")
        assert manifest["source_sha256"] == sha_of(written.source) == written.document.metadata.sha256
        params = {"dpi": DPI, "vision_dpi": VISION_DPI, "max_render_pixels": 16_000_000, "workers": 1}
        assert manifest["extraction_params"] == params
        assert manifest["integrity_verified"] is False is written.document.integrity_verified
        assert manifest["degraded_pages"] == [3, 4]
        assert manifest["missing"] == []

    def test_each_page_image_records_its_origin_dpi_and_normalization(self, written):
        render = {"origin": "render", "dpi": float(DPI), "contrast_normalized": False}
        own = {"origin": "document", "dpi": 150.5, "contrast_normalized": True}
        assert manifest_of(written.out)["page_images"] == [
            {"page_number": 1, **render},
            {"page_number": 2, **render},
            {"page_number": 3, **own},
            {"page_number": 4, **render},
        ]

    def test_every_artifact_has_its_path_size_and_sha256(self, written):
        artifacts = manifest_of(written.out)["artifacts"]
        assert [entry["path"] for entry in artifacts] == sorted(EXPECTED_FILES - {"manifest.json"})
        for entry in artifacts:
            assert list(entry) == ["path", "size", "sha256"]
            data = (written.out / entry["path"]).read_bytes()
            assert (entry["size"], entry["sha256"]) == (len(data), hashlib.sha256(data).hexdigest()), entry["path"]

    def test_manifest_is_ascii_json(self, written):
        (written.out / "manifest.json").read_bytes().decode("ascii")


class TestDocument:
    def test_returned_document_is_the_input_but_its_page_images(self, written):
        returned, document = written.returned, written.document
        assert (returned.metadata, returned.sections, returned.header_context) == (
            document.metadata,
            document.sections,
            document.header_context,
        )
        for mine, theirs in zip(returned.pages, document.pages):
            assert dataclasses.replace(mine, page_image=None) == dataclasses.replace(theirs, page_image=None)

    def test_figures_point_into_the_bundle_everywhere(self, written):
        out = written.out
        figures = [*written.returned.figures, *(f for s in written.returned.sections for f in s.figures)]
        assert len(figures) == 6
        for figure in figures:
            assert figure.image_path == out / "images" / f"{figure.sha256}.png"
        assert [figure.read_bytes() for figure in written.returned.figures] == [PNG_A, PNG_A, PNG_B]

    def test_each_figure_is_read_through_its_loader_and_each_picture_written_once(self, written):
        assert written.calls == {"a": 2, "b": 1}  # figure A twice: its PNG, not its sha256, decides it is the same
        assert (written.out / "images" / f"{FIGURE_A_SHA}.png").read_bytes() == PNG_A

    def test_pages_without_their_own_image_are_rendered_at_the_effective_dpi(self, written):
        for page in (written.returned.pages[n - 1] for n in RENDERED):
            image = page.page_image
            dpi = effective_dpi(DPI, 16_000_000, page.width_pt, page.height_pt)
            path = written.out / "page_images" / f"page_{page.page_number:04d}.png"
            assert (image.image_path, image.dpi, image.contrast_normalized) == (path, float(dpi), False)
            assert (image.width_px, image.height_px) == render_size(page.width_pt, page.height_pt, dpi)
            drawn = render_page(written.source, page.page_number - 1, dpi=dpi, memory_cap_bytes=CAP, timeout_s=TIMEOUT)
            assert png_pixels(path) == ("L", (drawn.px_width, drawn.px_height), drawn.pixels)

    def test_a_page_over_max_render_pixels_is_rendered_below_dpi(self, tmp_path, source, monkeypatch):
        calls = spied(monkeypatch)
        returned = write(
            bound_document(source), tmp_path / "out", source, dpi=200, vision_dpi=300, max_render_pixels=262_144
        )
        pages = [returned.pages[n - 1] for n in RENDERED]
        expected = [effective_dpi(200, 262_144, page.width_pt, page.height_pt) for page in pages]
        assert all(dpi < 200 for dpi in expected)  # the case: the pixel bound lowers each page's dpi
        assert sorted(calls) == [(n - 1, dpi) for n, dpi in zip(RENDERED, expected, strict=True)]
        recorded = {entry["page_number"]: entry["dpi"] for entry in manifest_of(tmp_path / "out")["page_images"]}
        for page, dpi in zip(pages, expected, strict=True):
            image = page.page_image
            width, height = render_size(page.width_pt, page.height_pt, dpi)
            assert (image.dpi, recorded[page.page_number], image.width_px, image.height_px) == (dpi, dpi, width, height)
            assert png_pixels(image.image_path)[1] == (width, height) and width * height <= 262_144

    def test_a_pages_own_image_is_kept_and_written_through_its_loaders(self, written):
        mine, theirs = written.returned.pages[2], written.document.pages[2]
        assert mine == theirs  # the page image compares equal: its dpi, size and normalization are kept
        image, out = mine.page_image, written.out
        assert (image.dpi, image.width_px, image.height_px, image.contrast_normalized) == (150.5, 100, 130, True)
        assert image.image_path == out / "page_images" / "page_0003.png"
        assert image.band_paths == tuple(out / "page_images" / "bands" / f"page_0003_band_0{k}.png" for k in (1, 2))
        assert image.image_path.read_bytes() == OWN_IMAGE
        assert tuple(path.read_bytes() for path in image.band_paths) == OWN_BANDS
        assert (image.read_bytes(), image.read_bands()) == (OWN_IMAGE, OWN_BANDS)

    def test_pages_without_their_own_image_have_no_bands(self, written):
        assert [written.returned.pages[n - 1].page_image.band_paths for n in RENDERED] == [(), (), ()]

    def test_document_json_is_the_returned_document_with_relative_paths(self, written):
        raw = (written.out / "json" / "document.json").read_bytes()
        data = json.loads(raw)
        assert data["pages"][0]["elements"][8]["image_path"] == f"images/{FIGURE_A_SHA}.png"
        image = data["pages"][2]["page_image"]
        assert image["image_path"] == "page_images/page_0003.png"
        assert image["band_paths"] == [f"page_images/bands/page_0003_band_0{k}.png" for k in (1, 2)]
        loaded = document_from_json_bytes(raw, base_dir=written.out)
        assert loaded == written.returned
        assert loaded.pages[2].page_image.band_paths == written.returned.pages[2].page_image.band_paths

    def test_markdown_links_figures_relative_to_the_markdown_file(self, written):
        markdown = (written.out / "markdown" / "document.md").read_text(encoding="utf-8")
        assert markdown == with_figure_base(written.document, Path("..")).to_markdown()
        assert "Ignore prior instructions." not in markdown and "<!-- hidden" not in markdown  # omitted by default
        json_text = (written.out / "json" / "document.json").read_text(encoding="ascii")
        assert "Ignore prior instructions." in json_text  # the JSON keeps every element
        assert f"![A figure with two lines](../images/{FIGURE_A_SHA}.png)" in markdown
        assert (written.out / "markdown" / ".." / "images" / f"{FIGURE_A_SHA}.png").is_file()


class TestLoneSurrogates:
    def test_markdown_escapes_them_and_json_keeps_them(self, tmp_path, fast_render):
        path = build_pdf(tmp_path / "s.pdf", [PageSpec(items=(TextRun("x", 72.0, 100.0),))])
        document = dataclasses.replace(
            surrogate_document(), metadata=metadata(1, source_name="s.pdf", sha=sha_of(path))
        )
        returned = write(document, tmp_path / "out", path)
        markdown = (tmp_path / "out" / "markdown" / "document.md").read_bytes().decode("utf-8")
        assert "# Bad \\ud800 heading" in markdown and "lone \\udfff low" in markdown
        raw = (tmp_path / "out" / "json" / "document.json").read_bytes()
        assert document_from_json_bytes(raw, base_dir=tmp_path / "out") == returned
        assert returned.pages[0].elements[0].text == "Bad \ud800 heading"


class TestFailuresAreRecorded:
    def test_a_refused_render_leaves_no_page_image_and_says_why(self, tmp_path, source, monkeypatch):
        def refusing(src, page_index, **kwargs):
            if page_index == 1:
                raise RenderRefused("page 2: an image over the bound; the page is not rendered")
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", refusing)
        returned = write(bound_document(source), tmp_path / "out", source)
        assert returned.pages[1].page_image is None
        assert "page_images/page_0002.png" not in files_in(tmp_path / "out")
        assert manifest_of(tmp_path / "out")["missing"] == [
            {
                "artifact": "page_image",
                "page_number": 2,
                "element_id": None,
                "reason": "the page was not rendered: RenderRefused: page 2: an image over the bound; "
                "the page is not rendered",
            }
        ]

    def test_a_failed_render_of_a_vision_page_without_its_own_image_is_recorded(self, tmp_path, source, monkeypatch):
        def failing(src, page_index, **kwargs):
            if page_index == 3:
                raise RenderFailed("the render worker timed out")
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", failing)
        returned = write(bound_document(source), tmp_path / "out", source)
        assert returned.pages[3].page_image is None
        assert not any(name.startswith("page_images/bands/page_0004") for name in files_in(tmp_path / "out"))
        [missing] = manifest_of(tmp_path / "out")["missing"]
        assert (missing["artifact"], missing["page_number"]) == ("page_image", 4)
        assert missing["reason"] == "the page was not rendered: RenderFailed: the render worker timed out"

    @pytest.mark.parametrize(
        ("image", "reason"),
        [
            (RenderFailed("the render worker timed out"), "RenderFailed: the render worker timed out"),
            (PermissionError(13, "Permission denied"), "PermissionError: [Errno 13] Permission denied"),
            (b"GIF89a", "its bytes are not a PNG"),
        ],
    )
    def test_an_own_image_that_cannot_be_read_leaves_no_page_image(self, tmp_path, source, fast_render, image, reason):
        returned = write(bound_document(source, image=image), tmp_path / "out", source)
        assert returned.pages[2].page_image is None
        assert not any(name.startswith("page_images/bands/page_0003") for name in files_in(tmp_path / "out"))
        assert manifest_of(tmp_path / "out")["missing"] == [
            {
                "artifact": "page_image",
                "page_number": 3,
                "element_id": None,
                "reason": f"the page's own image was not written: {reason}",
            }
        ]

    @pytest.mark.parametrize(
        ("bands", "reason"),
        [
            (RenderRefused("page 3: an image over the bound"), "RenderRefused: page 3: an image over the bound"),
            ((OWN_BANDS[0], b"not a png"), "band 2's bytes are not a PNG"),
            (None, "DatagruntPDFError: page image 3 has no bands: no band_paths and no band loader"),
        ],
    )
    def test_own_bands_that_cannot_be_read_keep_the_page_image(self, tmp_path, source, fast_render, bands, reason):
        returned = write(bound_document(source, bands=bands), tmp_path / "out", source)
        image = returned.pages[2].page_image
        assert image.image_path == tmp_path / "out" / "page_images" / "page_0003.png" and image.band_paths == ()
        assert manifest_of(tmp_path / "out")["missing"] == [
            {
                "artifact": "bands",
                "page_number": 3,
                "element_id": None,
                "reason": f"the page's own bands were not written: {reason}",
            }
        ]
        assert load_bundle(tmp_path / "out") == returned

    def test_an_effective_dpi_refusal_is_recorded(self, tmp_path, source, monkeypatch, fast_render):
        real = bundle.effective_dpi

        def refusing(target, pixels, width, height):
            if width == 595.5:
                raise RenderRefused("a 595.5 x 842.25 pt page renders over the bound")
            return real(target, pixels, width, height)

        monkeypatch.setattr(bundle, "effective_dpi", refusing)
        returned = write(bound_document(source), tmp_path / "out", source)
        assert returned.pages[1].page_image is None
        [missing] = manifest_of(tmp_path / "out")["missing"]
        assert (missing["artifact"], missing["page_number"]) == ("page_image", 2)

    @pytest.mark.parametrize(
        ("loader", "reason"),
        [
            (DatagruntPDFError("report.pdf: page 1: the file changed"), "DatagruntPDFError: report.pdf: page 1"),
            (FileNotFoundError(2, "No such file or directory"), "FileNotFoundError: [Errno 2]"),
            (b"GIF89a not a png", "its bytes are not a PNG"),
        ],
    )
    def test_a_figure_whose_bytes_cannot_be_read_is_recorded(self, tmp_path, source, fast_render, loader, reason):
        def broken():
            if isinstance(loader, Exception):
                raise loader
            return loader

        document = bound_document(source, broken)
        returned = write(document, tmp_path / "out", source)
        assert [figure.image_path for figure in returned.figures] == [
            None,
            None,
            tmp_path / "out" / "images" / f"{FIGURE_B_SHA}.png",
        ]
        assert f"images/{FIGURE_A_SHA}.png" not in files_in(tmp_path / "out")
        first, second = manifest_of(tmp_path / "out")["missing"]  # each figure of the stream: its own entry
        assert (first["artifact"], first["page_number"], first["element_id"]) == ("figure", 1, "p0001_e008")
        assert (second["artifact"], second["page_number"], second["element_id"]) == ("figure", 2, "p0002_e003")
        for missing in (first, second):
            assert (
                missing["reason"].startswith(f"figure {FIGURE_A_SHA} was not written: ") and reason in missing["reason"]
            )
        assert load_bundle(tmp_path / "out") == returned

    def test_missing_entries_are_in_page_order(self, tmp_path, source, monkeypatch):
        monkeypatch.setattr(bundle, "render_page", lambda *a, **k: (_ for _ in ()).throw(RenderFailed("no")))
        write(bound_document(source, None), tmp_path / "out", source)
        missing = manifest_of(tmp_path / "out")["missing"]
        assert [(m["page_number"], m["artifact"]) for m in missing] == [
            (1, "figure"),
            (1, "page_image"),
            (2, "figure"),
            (2, "page_image"),
            (4, "page_image"),
        ]


class TestAtomicWrite:
    def test_an_existing_non_empty_output_dir_is_refused_untouched(self, tmp_path, source, monkeypatch):
        out = tmp_path / "out"
        out.mkdir()
        (out / "keep.txt").write_text("mine")
        work = []
        monkeypatch.setattr(bundle, "render_page", lambda *args, **kwargs: work.append("render"))
        with pytest.raises(FileExistsError, match="not empty"):
            write(bound_document(source, lambda: work.append("figure") or PNG_A), out, source)
        assert work == []  # refused before any work
        assert files_in(out) == {"keep.txt"} and (out / "keep.txt").read_text() == "mine"
        assert stagings(tmp_path) == []

    def test_an_existing_file_is_refused(self, tmp_path, source, fast_render):
        (tmp_path / "out").write_text("mine")
        with pytest.raises(FileExistsError, match="not a directory"):
            write(bound_document(source), tmp_path / "out", source)
        assert (tmp_path / "out").read_text() == "mine"

    def test_a_symlink_is_refused_even_to_an_empty_directory(self, tmp_path, source, fast_render):
        (tmp_path / "empty").mkdir()
        (tmp_path / "out").symlink_to(tmp_path / "empty", target_is_directory=True)
        with pytest.raises(FileExistsError, match="symlink"):
            write(bound_document(source), tmp_path / "out", source)
        assert (tmp_path / "out").is_symlink() and list((tmp_path / "empty").iterdir()) == []

    def test_an_existing_empty_directory_is_written(self, tmp_path, source, fast_render):
        (tmp_path / "out").mkdir()
        write(bound_document(source), tmp_path / "out", source)
        assert (tmp_path / "out" / "manifest.json").is_file()

    def test_a_missing_parent_is_refused(self, tmp_path, source, fast_render):
        with pytest.raises(FileNotFoundError):
            write(bound_document(source), tmp_path / "no" / "out", source)
        assert not (tmp_path / "no").exists()

    def test_the_output_dir_appears_only_when_complete(self, tmp_path, source, fast_render, monkeypatch):
        out, seen = tmp_path / "out", []
        real = bundle.document_to_json_bytes

        def watched(document):
            seen.append((out.exists(), [path.name for path in stagings(tmp_path)]))
            return real(document)

        monkeypatch.setattr(bundle, "document_to_json_bytes", watched)
        write(bound_document(source), out, source)
        [(existed, staging)] = seen
        assert existed is False and len(staging) == 1
        assert stagings(tmp_path) == [] and (out / "manifest.json").is_file()

    @pytest.mark.parametrize(
        ("stage", "error"),
        [
            ("document_to_json_bytes", RuntimeError("json stage broke")),
            ("render_page", KeyboardInterrupt()),
            ("render_page", MemoryError("render broke")),
            ("utf8_text", RuntimeError("markdown stage broke")),
            ("_datagrunt_version", LookupError("no version")),
        ],
    )
    def test_staging_is_removed_on_any_failure(self, tmp_path, source, fast_render, monkeypatch, stage, error):
        def raising(*args, **kwargs):
            raise error

        monkeypatch.setattr(bundle, stage, raising)
        with pytest.raises(type(error)):
            write(bound_document(source), tmp_path / "out", source)
        assert not (tmp_path / "out").exists()
        assert stagings(tmp_path) == []

    def test_staging_is_removed_when_the_rename_fails(self, tmp_path, source, fast_render, monkeypatch):
        def failing(src, dst):
            raise OSError(18, "Invalid cross-device link")

        monkeypatch.setattr(os, "replace", failing)
        with pytest.raises(OSError, match="cross-device"):
            write(bound_document(source), tmp_path / "out", source)
        assert stagings(tmp_path) == [] and not (tmp_path / "out").exists()

    def test_an_unexpected_loader_error_fails_the_write_cleanly(self, tmp_path, source, fast_render):
        with pytest.raises(TypeError, match="_loader result must be bytes, got str"):
            write(bound_document(source, lambda: "not bytes"), tmp_path / "out", source)
        assert stagings(tmp_path) == [] and not (tmp_path / "out").exists()

    def test_an_output_dir_filled_during_the_write_is_never_merged_into(
        self, tmp_path, source, fast_render, monkeypatch
    ):
        out = tmp_path / "out"
        real = bundle.document_to_json_bytes

        def racing(document):
            out.mkdir()
            (out / "theirs.txt").write_text("theirs")
            return real(document)

        monkeypatch.setattr(bundle, "document_to_json_bytes", racing)
        with pytest.raises(FileExistsError, match="not empty"):
            write(bound_document(source), out, source)
        assert files_in(out) == {"theirs.txt"} and stagings(tmp_path) == []

    def test_a_leftover_staging_directory_is_ignored(self, tmp_path, source, fast_render):
        leftover = tmp_path / ".tmp_0123456789abcdef0123456789abcdef"
        leftover.mkdir()
        (leftover / "partial.png").write_bytes(b"half")
        write(bound_document(source), tmp_path / "out", source)
        assert (tmp_path / "out" / "manifest.json").is_file()
        assert files_in(leftover) == {"partial.png"}

    def test_a_writer_killed_mid_run_leaves_no_output_dir(self, tmp_path, fast_render):
        pdf = build_pdf(tmp_path / "six.pdf", [PageSpec(items=(TextRun(f"Page {n}", 72.0, 100.0),)) for n in range(6)])
        out = tmp_path / "out"
        script = (
            "import sys, time\n"
            "from datagrunt.pdf import bundle\n"
            "from datagrunt.pdf.pipeline import extract_document\n"
            "real = bundle.render_page\n"
            "def slow(*args, **kwargs):\n"
            "    time.sleep(0.5)\n"
            "    return real(*args, **kwargs)\n"
            "bundle.render_page = slow\n"
            "document = extract_document(sys.argv[1])\n"
            "bundle.write_bundle(document, sys.argv[2], source=sys.argv[1], dpi=72, vision_dpi=72)\n"
        )
        writer = subprocess.Popen([sys.executable, "-c", script, str(pdf), str(out)], cwd=tmp_path)
        try:
            deadline = time.monotonic() + 60.0
            while not list(tmp_path.glob(".tmp_*/page_images/page_*.png")):
                assert writer.poll() is None, "the writer ended before it was killed"
                assert time.monotonic() < deadline, "no page image was staged within 60 s"
                time.sleep(0.02)
        finally:
            writer.kill()
            writer.wait(timeout=30)
        assert not out.exists()
        [leftover] = stagings(tmp_path)
        assert not (leftover / "manifest.json").exists()
        # The next write ignores the leftover, the only trace.
        write(extract_document(pdf), out, pdf)
        assert (out / "manifest.json").is_file() and stagings(tmp_path) == [leftover]


class TestValidation:
    def test_the_source_must_be_the_documents_pdf(self, tmp_path, source, fast_render):
        other = build_pdf(tmp_path / "other.pdf", [PageSpec()])
        with pytest.raises(ValueError, match=f"sha256 {sha_of(other)}.*not the document's"):
            write(bound_document(source), tmp_path / "out", other)
        assert not (tmp_path / "out").exists() and stagings(tmp_path) == []

    def test_a_missing_source_is_refused(self, tmp_path, source, fast_render):
        with pytest.raises(FileNotFoundError):
            write(bound_document(source), tmp_path / "out", tmp_path / "gone.pdf")
        assert not (tmp_path / "out").exists()

    @pytest.mark.parametrize(
        ("options", "error", "message"),
        [
            ({"dpi": 35}, ValueError, "dpi must be >= 36"),
            ({"dpi": 300, "vision_dpi": 200}, ValueError, "vision_dpi must be >= dpi"),
            ({"max_render_pixels": 1000}, ValueError, "max_render_pixels must be >= 262144"),
            ({"workers": 0}, ValueError, "workers must be >= 1"),
            ({"workers": True}, TypeError, "workers must be an int"),
            ({"password": 5}, TypeError, "password"),
        ],
    )
    def test_bad_options_are_refused_before_anything_is_written(self, tmp_path, source, options, error, message):
        with pytest.raises(error, match=message):
            write(bound_document(source), tmp_path / "out", source, **options)
        assert sorted(path.name for path in tmp_path.iterdir()) == ["report.pdf"]

    def test_document_must_be_a_pdf_document(self, tmp_path, source):
        with pytest.raises(TypeError, match="document must be a PDFDocument, got dict"):
            write({}, tmp_path / "out", source)

    def test_output_dir_must_be_a_path(self, tmp_path, source):
        with pytest.raises(TypeError, match="output_dir must be a str or os.PathLike, got int"):
            write(bound_document(source), 3, source)

    def test_defaults_are_recorded(self, tmp_path, source, fast_render):
        write_bundle(bound_document(source), tmp_path / "out", source=source)
        params = {"dpi": 200, "vision_dpi": 300, "max_render_pixels": 16_000_000, "workers": 1}
        assert manifest_of(tmp_path / "out")["extraction_params"] == params

    def test_the_render_gets_the_password_cap_and_timeout(self, tmp_path, monkeypatch):
        path = source_pdf(tmp_path, user_password="pw")
        calls = []

        def spy(src, page_index, **kwargs):
            calls.append((Path(src), page_index, kwargs["password"], kwargs["memory_cap_bytes"], kwargs["timeout_s"]))
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", spy)
        write(bound_document(path), tmp_path / "out", path, password="pw")
        assert sorted(calls) == [(path, index, "pw", CAP, TIMEOUT) for index in (0, 1, 3)]

    def test_an_encrypted_source_renders_with_its_password(self, tmp_path):
        path = source_pdf(tmp_path, user_password="pw")
        returned = write(bound_document(path), tmp_path / "out", path, password="pw")
        assert manifest_of(tmp_path / "out")["missing"] == []
        assert png_pixels(returned.pages[0].page_image.image_path)[0] == "L"


class TestFiguresSharingAStream:
    """Figures of one raw stream (one sha256) under other image dictionaries: each its own PNG, or its own entry."""

    def bundled(self, tmp_path: Path, first: bytes, second: bytes):
        path = shared_stream_pdf(tmp_path / "shared.pdf", first, second)
        document = extract_document(path, ocr="never")
        assert [figure.element_id for figure in document.figures] == ["p0001_e000", "p0002_e000"]
        assert len({figure.sha256 for figure in document.figures}) == 1
        returned = write_bundle(document, tmp_path / "out", source=path, dpi=72)
        return document, returned

    def test_one_stream_read_as_two_pictures_is_written_as_two(self, tmp_path):
        document, returned = self.bundled(tmp_path, AS_RGB, AS_GREY)
        own = [figure.read_bytes() for figure in document.figures]
        assert own[0] != own[1]
        sha = document.figures[0].sha256
        later = hashlib.sha256(own[1]).hexdigest()
        out = tmp_path / "out"
        assert [figure.image_path for figure in returned.figures] == [
            out / "images" / f"{sha}.png",
            out / "images" / f"{sha}-{later}.png",
        ]
        assert [figure.image_path.read_bytes() for figure in returned.figures] == own
        assert [png_pixels(figure.image_path)[0] for figure in returned.figures] == ["RGB", "L"]
        assert manifest_of(out)["missing"] == []
        assert [figure.read_bytes() for figure in load_bundle(out).figures] == own

    def test_a_figure_is_read_though_an_earlier_one_of_its_stream_failed(self, tmp_path):
        document, returned = self.bundled(tmp_path, UNREADABLE, AS_RGB)
        sha, out = document.figures[0].sha256, tmp_path / "out"
        assert [figure.image_path for figure in returned.figures] == [None, out / "images" / f"{sha}.png"]
        assert returned.figures[1].image_path.read_bytes() == document.figures[1].read_bytes()
        [missing] = manifest_of(out)["missing"]
        assert (missing["artifact"], missing["page_number"], missing["element_id"]) == ("figure", 1, "p0001_e000")
        assert load_bundle(out) == returned

    def test_each_figure_not_written_has_its_own_missing_entry(self, tmp_path):
        _, returned = self.bundled(tmp_path, UNREADABLE, UNREADABLE)
        assert [figure.image_path for figure in returned.figures] == [None, None]
        missing = manifest_of(tmp_path / "out")["missing"]
        assert [(m["artifact"], m["page_number"], m["element_id"]) for m in missing] == [
            ("figure", 1, "p0001_e000"),
            ("figure", 2, "p0002_e000"),
        ]
        assert load_bundle(tmp_path / "out") == returned


def paged_pdf(directory: Path, count: int) -> Path:
    """A PDF of ``count`` letter pages, a line of text each: digital pages, which a bundle renders."""
    pages = [PageSpec(items=(TextRun(f"Page {number}", 72.0, 100.0),)) for number in range(1, count + 1)]
    return build_pdf(directory / "pages.pdf", pages)


class InFlight:
    """A ThreadPoolExecutor stand-in's record: the most tasks submitted and not yet done at any submit."""

    def __init__(self) -> None:
        self.lock, self.futures, self.most = threading.Lock(), [], 0

    def executor(self, *args, **kwargs) -> ThreadPoolExecutor:
        record = self

        class Counting(ThreadPoolExecutor):
            def submit(self, fn, /, *call_args, **call_kwargs):
                with record.lock:
                    live = sum(not future.done() for future in record.futures) + 1
                    record.most = max(record.most, live)
                    future = super().submit(fn, *call_args, **call_kwargs)
                    record.futures.append(future)
                return future

        return Counting(*args, **kwargs)


def gathered(parties: int):
    """A render stand-in whose first ``parties`` pages wait for each other: the call fails unless they run together."""
    together = threading.Barrier(parties, timeout=10.0)

    def render(src, page_index, **kwargs):
        if page_index < parties:
            together.wait()  # BrokenBarrierError, failing the call, unless the pages are in flight at once
        return fake_render(src, page_index, **kwargs)

    return render


def last_first(count: int):
    """A render stand-in whose page 1 ends only once the other ``count - 1`` pages have; and the order pages end in."""
    others, lock, ended = threading.Event(), threading.Lock(), []

    def render(src, page_index, **kwargs):
        if page_index == 0:
            assert others.wait(timeout=10.0), "page 1 held up the pages after it"
        drawn = fake_render(src, page_index, **kwargs)
        with lock:
            ended.append(page_index)
            if len(ended) == count - 1 and 0 not in ended:
                others.set()
        return drawn

    return render, ended


class TestWorkers:
    """Pages render on ``workers`` threads in a sliding window: at most ``workers`` in flight, the next as one ends."""

    @pytest.mark.parametrize("workers", [2, 8])
    def test_workers_pages_render_together_and_no_more_are_in_flight(self, tmp_path, monkeypatch, workers):
        path = paged_pdf(tmp_path, 10)
        document = without_page_images(extract_document(path, ocr="never"))  # each page rendered by the bundle
        flight = InFlight()
        monkeypatch.setattr(bundle, "render_page", gathered(workers))
        monkeypatch.setattr(bundle, "ThreadPoolExecutor", flight.executor)
        write(document, tmp_path / "out", path, workers=workers)
        assert flight.most == workers
        assert manifest_of(tmp_path / "out")["missing"] == []

    def test_a_slow_page_holds_one_thread_never_the_others(self, tmp_path, monkeypatch):
        path = paged_pdf(tmp_path, 5)
        document = without_page_images(extract_document(path, ocr="never"))  # each page rendered by the bundle
        render, ended = last_first(5)
        monkeypatch.setattr(bundle, "render_page", render)
        returned = write(document, tmp_path / "out", path, workers=2)
        assert ended == [1, 2, 3, 4, 0]  # one thread held page 1; the other rendered the rest, one after another
        assert [page.page_image.image_path.name for page in returned.pages] == [
            f"page_000{number}.png" for number in range(1, 6)
        ]

    def test_workers_write_the_same_bundle(self, tmp_path, written):
        out = tmp_path / "out"
        returned = write(bound_document(written.source), out, written.source, workers=3)
        mine, theirs = manifest_of(out), manifest_of(written.out)
        assert mine["artifacts"] == theirs["artifacts"] and mine["missing"] == theirs["missing"] == []
        assert returned == written.returned


def spied(monkeypatch, real=None) -> list:
    """Record each bundle render_page call (its page index and dpi), and let it run (or ``real`` run)."""
    calls: list = []
    run = real or bundle.render_page

    def spy(src, page_index, **kwargs):
        calls.append((page_index, kwargs["dpi"]))
        return run(src, page_index, **kwargs)

    monkeypatch.setattr(bundle, "render_page", spy)
    return calls


class TestVisionRoute:
    """A5b's vision pages carry their own page image: the bundle writes it through its loaders, never re-renders."""

    @pytest.mark.skipif(not tesseract_available(), reason="tesseract is not installed")
    def test_an_ocr_page_is_bundled_from_its_own_image_and_bands(self, tmp_path, monkeypatch):
        path = build_pdf(
            tmp_path / "scan.pdf",
            [PageSpec(items=(TextRun("Bundle the righted bands", 72.0, 120.0, size=24.0),)) for _ in range(2)],
        )
        document = extract_document(path, ocr="always")
        assert all(page.route == "vision" and page.page_image is not None for page in document.pages)
        calls = spied(monkeypatch)
        written = write_bundle(document, tmp_path / "out", source=path)
        assert calls == []  # nothing re-rendered
        for mine, theirs in zip(written.pages, document.pages):
            image = mine.page_image
            assert image.image_path.read_bytes() == theirs.page_image.read_bytes()
            assert tuple(band.read_bytes() for band in image.band_paths) == theirs.page_image.read_bands()
            assert image.band_paths and (image.contrast_normalized, image.dpi) == (True, theirs.page_image.dpi)
        assert written == document
        loaded = load_bundle(tmp_path / "out")
        assert loaded == written == document
        assert loaded.pages[1].page_image.read_bands() == document.pages[1].page_image.read_bands()
        dpi = document.pages[0].page_image.dpi
        assert manifest_of(tmp_path / "out")["page_images"][0] == {
            "page_number": 1,
            "origin": "document",
            "dpi": dpi,
            "contrast_normalized": True,
        }

    def test_a_digital_page_is_bundled_from_its_drawn_image_at_its_own_dpi(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "text.pdf", [PageSpec(items=tuple(body_text(100.0)))])
        document = extract_document(path)
        assert (document.pages[0].route, document.pages[0].page_image.dpi) == ("digital", 200.0)
        calls = spied(monkeypatch)
        written = write(document, tmp_path / "out", path)  # dpi=72: a page with an image keeps its own
        assert calls == []  # drawn through its own loader, never rendered by the bundle
        image = written.pages[0].page_image
        assert (image.dpi, image.contrast_normalized, image.band_paths) == (200.0, False, ())
        assert manifest_of(tmp_path / "out")["page_images"][0] == {
            "page_number": 1,
            "origin": "render",
            "dpi": 200.0,
            "contrast_normalized": False,
        }
        assert load_bundle(tmp_path / "out") == written == document


def without_page_images(document: PDFDocument) -> PDFDocument:
    """``document`` whose pages have no page image: each one the bundle renders, as every page was before P2-A."""
    return dataclasses.replace(
        document, pages=tuple(dataclasses.replace(page, page_image=None) for page in document.pages)
    )


def contents(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` and its bytes, by relative path."""
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def digital_pdf(directory: Path) -> Path:
    """Two digital pages of prose: a letter page and a smaller landscape one."""
    pages = [
        PageSpec(items=tuple(body_text(100.0))),
        PageSpec(width=400.0, height=300.0, items=tuple(body_text(60.0))),
    ]
    return build_pdf(directory / "digital.pdf", pages)


class TestDrawnPageImages:
    """A digital or hybrid page's own image is drawn when read (P2-A): the bundle writes it through its loader."""

    def test_a_bundle_of_drawn_pages_is_byte_identical_to_one_that_renders_them(self, tmp_path, monkeypatch):
        path = digital_pdf(tmp_path)
        document = extract_document(path)
        assert all(isinstance(page.page_image._loader.__self__, _loaders.PageDraw) for page in document.pages)
        calls = spied(monkeypatch)

        drawn = write_bundle(document, tmp_path / "drawn", source=path)
        assert calls == []  # each page read through its own loader
        rendered = write_bundle(without_page_images(document), tmp_path / "rendered", source=path)

        assert sorted(calls) == [(0, 200), (1, 200)]  # each page rendered by the bundle, as before
        assert contents(tmp_path / "drawn") == contents(tmp_path / "rendered")
        assert drawn == rendered == document
        assert load_bundle(tmp_path / "drawn") == drawn

    def test_a_drawn_page_image_is_recorded_as_a_render_without_bands(self, tmp_path):
        path = digital_pdf(tmp_path)
        document = extract_document(path)
        assert [type(page.page_image._loader.__self__) for page in document.pages] == [_loaders.PageDraw] * 2

        written = write_bundle(document, tmp_path / "out", source=path)

        render = {"origin": "render", "dpi": 200.0, "contrast_normalized": False}
        assert manifest_of(tmp_path / "out")["page_images"] == [{"page_number": n, **render} for n in (1, 2)]
        assert [page.page_image.band_paths for page in written.pages] == [(), ()]
        assert not (tmp_path / "out" / "page_images" / "bands").exists()

    def test_a_draw_that_fails_is_recorded_as_a_failed_render_is(self, tmp_path, monkeypatch):
        path = digital_pdf(tmp_path)
        document = extract_document(path)
        assert [type(page.page_image._loader.__self__) for page in document.pages] == [_loaders.PageDraw] * 2

        def failing(source, page_index, **kwargs):
            raise RenderFailed(f"{source}: page {page_index + 1}: the render worker reached its memory cap")

        monkeypatch.setattr(_loaders, "render_page", failing)
        monkeypatch.setattr(bundle, "render_page", failing)

        write_bundle(document, tmp_path / "drawn", source=path)
        write_bundle(without_page_images(document), tmp_path / "rendered", source=path)

        assert contents(tmp_path / "drawn") == contents(tmp_path / "rendered")
        assert [entry["reason"] for entry in manifest_of(tmp_path / "drawn")["missing"]] == [
            f"the page was not rendered: RenderFailed: {path}: page {n}: the render worker reached its memory cap"
            for n in (1, 2)
        ]

    def test_a_draw_from_another_file_is_not_written(self, tmp_path):
        path = digital_pdf(tmp_path)
        copy = tmp_path / "copy.pdf"
        copy.write_bytes(path.read_bytes())
        document = extract_document(copy)

        with pytest.warns(UserWarning) as caught:
            written = write_bundle(document, tmp_path / "out", source=path)

        renders = f"renders from {copy.resolve()}, not from the source {path.resolve()}"
        assert [str(item.message) for item in caught] == [
            f"page {n}'s own image {renders}: no page image is written for it" for n in (1, 2)
        ]
        assert [page.page_image for page in written.pages] == [None, None]
        assert [entry["page_number"] for entry in manifest_of(tmp_path / "out")["missing"]] == [1, 2]


class TestExtractionParams:
    EXTRA = {"ocr": "always", "min_image_dimension": 40, "strict": False, "note": None, "ratio": 0.5}

    def test_extra_parameters_are_merged_after_the_writers_own(self, tmp_path, source, fast_render):
        write(bound_document(source), tmp_path / "out", source, extraction_params=self.EXTRA)
        params = manifest_of(tmp_path / "out")["extraction_params"]
        assert params == {
            "dpi": DPI,
            "vision_dpi": VISION_DPI,
            "max_render_pixels": 16_000_000,
            "workers": 1,
            **self.EXTRA,
        }
        assert list(params)[:4] == ["dpi", "vision_dpi", "max_render_pixels", "workers"]
        assert load_bundle(tmp_path / "out").metadata.sha256 == sha_of(source)

    @pytest.mark.parametrize(
        ("extra", "error", "message"),
        [
            (
                {"ocr": ["always"]},
                TypeError,
                r"extraction_params\['ocr'\] must be a str, int, float, bool or None, got list",
            ),
            ({"nested": {"a": 1}}, TypeError, "got dict"),
            ({"ratio": float("nan")}, ValueError, r"extraction_params\['ratio'\] must be finite"),
            ({"ratio": float("inf")}, ValueError, "must be finite"),
            ({"thing": object()}, TypeError, "got object"),
            ({"dpi": 300}, ValueError, r"extraction_params\['dpi'\] is write_bundle's own parameter"),
            ({3: "x"}, TypeError, "extraction_params keys must be str, got int"),
            ({" ": "x"}, ValueError, "extraction_params keys must be non-blank"),
            (["ocr"], TypeError, "extraction_params must be a Mapping or None, got list"),
        ],
    )
    def test_a_bad_parameter_is_refused_before_anything_is_written(self, tmp_path, source, extra, error, message):
        with pytest.raises(error, match=message):
            write(bound_document(source), tmp_path / "out", source, extraction_params=extra)
        assert sorted(path.name for path in tmp_path.iterdir()) == ["report.pdf"]


class TestBoundToTheSource:
    """The page images must be of the hashed source: own loaders render from it, and it does not change."""

    def test_an_own_image_rendering_from_another_file_is_not_written(self, tmp_path, source, fast_render):
        other = build_pdf(tmp_path / "other.pdf", [PageSpec()])
        never = AssertionError("an own loader of another file must not be called")
        document = bound_document(source, image=never, bands=never, own_source=other)
        with pytest.warns(UserWarning, match="page 3's own image renders from .*other.pdf, not from the source"):
            returned = write(document, tmp_path / "out", source)
        assert returned.pages[2].page_image is None
        assert not any(name.startswith("page_images/") and "page_0003" in name for name in files_in(tmp_path / "out"))
        reason = (
            f"the page's own image renders from {other.resolve()}, not from the source {source.resolve()}: "
            "its bytes could not be bound to the hashed source"
        )
        assert manifest_of(tmp_path / "out")["missing"] == [
            {"artifact": "page_image", "page_number": 3, "element_id": None, "reason": reason}
        ]
        assert load_bundle(tmp_path / "out") == returned

    def test_an_own_band_loader_rendering_from_another_file_is_refused_too(self, tmp_path, source, fast_render):
        other = build_pdf(tmp_path / "other.pdf", [PageSpec()])
        document = bound_document(source)
        image = document.pages[2].page_image
        stray = OwnRender(other)
        pages = list(document.pages)
        pages[2] = dataclasses.replace(
            pages[2], page_image=dataclasses.replace(image, _band_loader=stray.band_png_bytes)
        )
        with pytest.warns(UserWarning, match="renders from .*other.pdf"):
            returned = write(dataclasses.replace(document, pages=tuple(pages)), tmp_path / "out", source)
        assert returned.pages[2].page_image is None

    def test_an_own_loader_with_no_source_is_not_written(self, tmp_path, source, fast_render):
        document = rich_document(
            sha=sha_of(source), source_name=source.name, image_loader=lambda: OWN_IMAGE, band_loader=lambda: OWN_BANDS
        )
        with pytest.warns(UserWarning, match="page 3's own image has a loader that names no source"):
            returned = write(document, tmp_path / "out", source)
        assert returned.pages[2].page_image is None
        [missing] = [m for m in manifest_of(tmp_path / "out")["missing"] if m["page_number"] == 3]
        assert missing["reason"] == (
            "the page's own image has a loader that names no source: its bytes could not be bound to the hashed source"
        )

    def test_an_own_image_with_no_loader_is_not_written(self, tmp_path, source, fast_render):
        document = bound_document(source)
        pages = list(document.pages)
        image = dataclasses.replace(pages[2].page_image, _loader=None, _band_loader=None, image_path=tmp_path / "x.png")
        pages[2] = dataclasses.replace(pages[2], page_image=image)
        with pytest.warns(UserWarning, match="has no loader"):
            returned = write(dataclasses.replace(document, pages=tuple(pages)), tmp_path / "out", source)
        assert returned.pages[2].page_image is None

    def test_a_figure_with_only_a_path_is_not_read_whatever_file_it_names(self, tmp_path, source, fast_render):
        secret = tmp_path / "secret.png"
        secret.write_bytes(solid_png(8, 8, (0.3, 0.3, 0.3)))
        document = with_element(bound_document(source), "p0002_e004", _loader=None, image_path=secret)
        with pytest.warns(UserWarning, match="figure p0002_e004 has no loader: no image is written for it"):
            returned = write(document, tmp_path / "out", source)
        assert returned.figures[2].image_path is None
        assert files_in(tmp_path / "out") == EXPECTED_FILES - {f"images/{FIGURE_B_SHA}.png"}
        held = [path.read_bytes() for path in (tmp_path / "out").rglob("*") if path.is_file()]
        assert secret.read_bytes() not in held
        reason = f"figure {FIGURE_B_SHA} has no loader: its bytes could not be bound to the hashed source"
        assert manifest_of(tmp_path / "out")["missing"] == [
            {"artifact": "figure", "page_number": 2, "element_id": "p0002_e004", "reason": reason}
        ]
        assert load_bundle(tmp_path / "out") == returned

    def test_a_figure_reading_another_file_is_not_read(self, tmp_path, source, fast_render):
        other = build_pdf(tmp_path / "other.pdf", [PageSpec()])
        never = OwnFigure(other, lambda: pytest.fail("a figure loader of another file must not be called"))
        document = with_element(bound_document(source), "p0002_e004", _loader=never.png_bytes)
        with pytest.warns(UserWarning, match="figure p0002_e004 reads from .*other.pdf, not from the source"):
            returned = write(document, tmp_path / "out", source)
        assert returned.figures[2].image_path is None
        [missing] = manifest_of(tmp_path / "out")["missing"]
        assert missing["reason"] == (
            f"figure {FIGURE_B_SHA} reads from {other.resolve()}, not from the source {source.resolve()}: "
            "its bytes could not be bound to the hashed source"
        )

    def test_a_figure_loader_naming_no_source_is_not_read(self, tmp_path, source, fast_render):
        document = with_element(
            bound_document(source), "p0002_e004", _loader=lambda: pytest.fail("an unbound loader was called")
        )
        with pytest.warns(UserWarning, match="figure p0002_e004 has a loader that names no source"):
            returned = write(document, tmp_path / "out", source)
        assert returned.figures[2].image_path is None
        [missing] = manifest_of(tmp_path / "out")["missing"]
        assert (missing["artifact"], missing["element_id"]) == ("figure", "p0002_e004")

    def test_a_loaded_bundles_figures_are_bound_by_its_verifying_reader(self, written, tmp_path, recwarn):
        loaded = load_bundle(written.out)
        returned = write(loaded, tmp_path / "again", written.source)
        assert [figure.read_bytes() for figure in returned.figures] == [PNG_A, PNG_A, PNG_B]
        assert manifest_of(tmp_path / "again")["missing"] == [] and [str(w.message) for w in recwarn] == []

    def test_the_same_file_by_another_path_is_the_source(self, tmp_path, source, fast_render):
        (tmp_path / "link.pdf").symlink_to(source)
        returned = write(bound_document(source, own_source=tmp_path / "link.pdf"), tmp_path / "out", source)
        assert returned.pages[2].page_image.read_bytes() == OWN_IMAGE

    @pytest.mark.parametrize("change", ["append", "replace"])
    def test_a_source_changed_during_the_write_is_never_published(self, tmp_path, source, monkeypatch, change):
        def changing(src, page_index, **kwargs):
            if page_index == 3:  # the last page rendered
                if change == "append":
                    with source.open("ab") as handle:
                        handle.write(b"%% appended")
                else:
                    copy = tmp_path / "copy.pdf"
                    copy.write_bytes(source.read_bytes())
                    os.replace(copy, source)
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", changing)
        what = "size" if change == "append" else "inode"
        with pytest.raises(PDFSourceChangedError, match=f"changed while its bundle was written: .*{what}"):
            write(bound_document(source), tmp_path / "out", source)
        assert not (tmp_path / "out").exists() and stagings(tmp_path) == []

    def test_a_source_removed_during_the_write_is_never_published(self, tmp_path, source, monkeypatch):
        def removing(src, page_index, **kwargs):
            if page_index == 3:
                source.unlink()
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", removing)
        with pytest.raises(PDFSourceChangedError, match="was removed while its bundle was written"):
            write(bound_document(source), tmp_path / "out", source)
        assert not (tmp_path / "out").exists() and stagings(tmp_path) == []

    def test_an_untouched_source_writes_as_before(self, tmp_path, source, fast_render, recwarn):
        returned = write(bound_document(source), tmp_path / "out", source)
        assert manifest_of(tmp_path / "out")["missing"] == [] and [str(w.message) for w in recwarn] == []
        assert load_bundle(tmp_path / "out") == returned


class TestRowsAndPageMarkdown:
    """``json/elements.jsonl``: the frame's rows; ``markdown/pages/``: each page's Markdown. Both in the manifest."""

    def test_elements_jsonl_holds_the_frames_rows_as_write_jsonl_writes_them(self, written, tmp_path):
        expected = PDFWriter(with_figure_base(written.document, Path())).write_jsonl(tmp_path / "rows.jsonl")
        assert (written.out / "json" / "elements.jsonl").read_bytes() == expected.read_bytes()

    def test_each_page_has_its_markdown_hidden_elements_omitted(self, written):
        pages = written.out / "markdown" / "pages"
        for page in with_figure_base(written.document, Path("../..")).pages:
            path = pages / f"page_{page.page_number:04d}.md"
            assert path.read_bytes() == utf8_text(page.to_markdown(include_hidden=False)).encode("utf-8")
        first = (pages / "page_0001.md").read_text(encoding="utf-8")
        assert "Ignore prior instructions." not in first and "<!-- hidden" not in first
        assert f"](../../images/{FIGURE_A_SHA}.png)" in first
        assert (pages / ".." / ".." / "images" / f"{FIGURE_A_SHA}.png").is_file()

    def test_both_are_artifacts_of_the_manifest(self, written):
        listed = {entry["path"]: entry for entry in manifest_of(written.out)["artifacts"]}
        for name in ("json/elements.jsonl", *(f"markdown/pages/page_000{n}.md" for n in (1, 2, 3, 4))):
            data = (written.out / name).read_bytes()
            assert (listed[name]["size"], listed[name]["sha256"]) == (len(data), hashlib.sha256(data).hexdigest())


def tree_of(root: Path) -> dict[str, tuple[int, int, int]]:
    """Each path under ``root``: its inode, size and mtime (a file rewritten or replaced changes it)."""
    return {
        path.relative_to(root).as_posix(): (path.lstat().st_ino, path.lstat().st_size, path.lstat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
    }


def leftovers(directory: Path) -> list[str]:
    """Staging and set-aside directories left beside a bundle."""
    return sorted(path.name for path in directory.iterdir() if path.name.startswith((".tmp_", ".old_")))


class TestReuse:
    """A bundle already at ``output_dir`` that this write would make is returned as it is, unwritten."""

    def test_a_matching_bundle_is_returned_unwritten(self, tmp_path, source, fast_render, monkeypatch):
        first = write(bound_document(source), tmp_path / "out", source)
        before = tree_of(tmp_path / "out")
        work: list[str] = []
        monkeypatch.setattr(bundle, "render_page", lambda *args, **kwargs: work.append("render"))

        again = write(bound_document(source, lambda: work.append("figure") or PNG_A), tmp_path / "out", source)

        assert work == [] and tree_of(tmp_path / "out") == before and leftovers(tmp_path) == []
        assert again == first == load_bundle(tmp_path / "out")
        assert [figure.read_bytes() for figure in again.figures] == [PNG_A, PNG_A, PNG_B]

    def test_overwrite_reuses_a_matching_bundle_too(self, tmp_path, source, fast_render):
        write(bound_document(source), tmp_path / "out", source)
        before = tree_of(tmp_path / "out")
        write(bound_document(source), tmp_path / "out", source, overwrite=True)
        assert tree_of(tmp_path / "out") == before

    @pytest.mark.parametrize(
        ("change", "why"),
        [
            (lambda out: None, "its extraction_params"),
            (lambda out: _set_manifest(out, "datagrunt_version", "0.0.1"), "written by datagrunt 0.0.1"),
            (lambda out: flip_byte(out / "markdown" / "document.md"), "it is not a valid bundle"),
        ],
        ids=["other-parameters", "other-datagrunt", "corrupt"],
    )
    def test_any_other_bundle_is_refused_untouched(self, tmp_path, source, fast_render, change, why):
        write(bound_document(source), tmp_path / "out", source, dpi=100, vision_dpi=100)
        change(tmp_path / "out")
        before = tree_of(tmp_path / "out")
        with pytest.raises(FileExistsError, match=f"output_dir holds a bundle that is not this write's.*{why}"):
            write(bound_document(source), tmp_path / "out", source)
        assert tree_of(tmp_path / "out") == before and leftovers(tmp_path) == []

    def test_another_documents_bundle_is_refused(self, tmp_path, source, fast_render):
        write(bound_document(source), tmp_path / "out", source)
        changed = with_element(bound_document(source), "p0001_e003", text="Other text.")
        with pytest.raises(FileExistsError, match="its document is not this one"):
            write(changed, tmp_path / "out", source)


def _set_manifest(out: Path, key: str, value) -> None:
    data = manifest_of(out)
    data[key] = value
    (out / "manifest.json").write_text(json.dumps(data, indent=2), encoding="ascii")


def flip_byte(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))


class TestOverwrite:
    """``overwrite=True`` replaces a bundle that is not this write's: staged, then swapped in, the old one removed."""

    def test_a_bundle_of_other_parameters_is_replaced(self, tmp_path, source, fast_render):
        write(bound_document(source), tmp_path / "out", source, dpi=100, vision_dpi=100)
        returned = write(bound_document(source), tmp_path / "out", source, overwrite=True)
        assert manifest_of(tmp_path / "out")["extraction_params"]["dpi"] == DPI
        assert load_bundle(tmp_path / "out") == returned and leftovers(tmp_path) == []
        assert files_in(tmp_path / "out") == EXPECTED_FILES

    def test_a_directory_that_is_not_a_bundle_is_never_replaced(self, tmp_path, source, fast_render):
        (tmp_path / "out").mkdir()
        (tmp_path / "out" / "keep.txt").write_text("mine")
        with pytest.raises(FileExistsError, match="holds no bundle"):
            write(bound_document(source), tmp_path / "out", source, overwrite=True)
        assert files_in(tmp_path / "out") == {"keep.txt"} and leftovers(tmp_path) == []

    def test_a_directory_whose_manifest_is_not_a_bundles_is_never_replaced(self, tmp_path, source, fast_render):
        out = tmp_path / "out"
        out.mkdir()
        (out / "manifest.json").write_text('{"name": "web app", "start_url": "/"}', encoding="ascii")
        (out / "index.html").write_text("<p>mine</p>", encoding="ascii")
        before = tree_of(out)
        with pytest.raises(FileExistsError, match="holds no bundle"):
            write(bound_document(source), out, source, overwrite=True)
        assert tree_of(out) == before and leftovers(tmp_path) == []

    def test_a_symlink_is_never_replaced(self, tmp_path, source, fast_render):
        write(bound_document(source), tmp_path / "real", source, dpi=100, vision_dpi=100)
        (tmp_path / "out").symlink_to(tmp_path / "real", target_is_directory=True)
        with pytest.raises(FileExistsError, match="symlink"):
            write(bound_document(source), tmp_path / "out", source, overwrite=True)
        assert (tmp_path / "out").is_symlink() and manifest_of(tmp_path / "real")["extraction_params"]["dpi"] == 100

    def test_a_failed_write_leaves_the_old_bundle(self, tmp_path, source, fast_render, monkeypatch):
        old = write(bound_document(source), tmp_path / "out", source, dpi=100, vision_dpi=100)
        before = tree_of(tmp_path / "out")
        monkeypatch.setattr(bundle, "render_page", lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt()))
        with pytest.raises(KeyboardInterrupt):
            write(bound_document(source), tmp_path / "out", source, overwrite=True)
        assert tree_of(tmp_path / "out") == before and load_bundle(tmp_path / "out") == old
        assert leftovers(tmp_path) == []

    def test_a_failed_swap_puts_the_old_bundle_back(self, tmp_path, source, fast_render, monkeypatch):
        old = write(bound_document(source), tmp_path / "out", source, dpi=100, vision_dpi=100)
        before = tree_of(tmp_path / "out")
        real = os.replace

        def failing(src, dst):
            if Path(src).name.startswith(".tmp_"):
                raise OSError(5, "Input/output error")
            return real(src, dst)

        monkeypatch.setattr(os, "replace", failing)
        with pytest.raises(OSError, match="Input/output error"):
            write(bound_document(source), tmp_path / "out", source, overwrite=True)
        assert tree_of(tmp_path / "out") == before and load_bundle(tmp_path / "out") == old
        assert leftovers(tmp_path) == []

    @pytest.mark.parametrize("value", [1, None, "yes"])
    def test_overwrite_must_be_a_bool(self, tmp_path, source, value):
        with pytest.raises(TypeError, match="overwrite must be a bool"):
            write(bound_document(source), tmp_path / "out", source, overwrite=value)
        assert not (tmp_path / "out").exists()
