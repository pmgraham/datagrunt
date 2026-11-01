"""Tests for the v5 bundle write (#371, A7a): ``datagrunt.pdf.bundle.write_bundle``.

The source is a ReportLab PDF of ``rich_document``'s page sizes, and the
document is ``rich_document`` bound to it (its sha256), with figure loaders
returning fixed PNGs. Renders are real (A5a's guarded worker) where their
bytes are checked; elsewhere ``bundle.render_page`` is a fast stand-in
(:func:`fake_render`). A failing stage is a ``monkeypatch`` of the bundle's
module name for it.
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
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from datagrunt.pdf import bundle
from datagrunt.pdf.bundle import BUNDLE_FORMAT_VERSION, write_bundle
from datagrunt.pdf.codec import document_from_json_bytes
from datagrunt.pdf.elements import Figure, PDFDocument
from datagrunt.pdf.exceptions import DatagruntPDFError
from datagrunt.pdf.ocr import bands, normalized
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page, render_size
from pdf_tests.builders import PageSpec, Rect, TextRun, build_pdf, solid_png
from pdf_tests.documents import FIGURE_A_SHA, FIGURE_B_SHA, PAGE_SIZES, metadata, rich_document, surrogate_document

PNG_A = solid_png(64, 32, (0.2, 0.4, 0.6))
PNG_B = solid_png(16, 16, (0.9, 0.1, 0.1))
DPI, VISION_DPI = 72, 100
STAGING = re.compile(r"\.tmp_[0-9a-f]{32}")
CAP, TIMEOUT = 2 << 30, 120.0


def source_pdf(directory: Path, *, user_password=None, name="report.pdf") -> Path:
    """A PDF of rich_document's page sizes, one line of text a page.

    Pages 3 and 4 (the vision pages) also hold two grey fills, so their
    1 % autocontrast changes their pixels.
    """
    greys = (
        Rect(0.0, 0.0, 612.0, 300.0, fill_rgb=(0.6, 0.6, 0.6), stroke=False),
        Rect(0.0, 300.0, 612.0, 300.0, fill_rgb=(0.3, 0.3, 0.3), stroke=False),
    )
    pages = [
        PageSpec(
            width=width,
            height=height,
            items=(TextRun(f"Page {number}", 72.0, 100.0), *(greys if number > 2 else ())),
        )
        for number, (width, height) in enumerate(PAGE_SIZES, 1)
    ]
    return build_pdf(directory / name, pages, user_password=user_password)


def sha_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def bound_document(path: Path, figure_loader=lambda: PNG_A, other_loader=lambda: PNG_B) -> PDFDocument:
    """rich_document as the document of the PDF at ``path``."""
    return rich_document(figure_loader, other_loader, sha=sha_of(path), source_name=path.name)


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
    "markdown/document.md",
    f"images/{FIGURE_A_SHA}.png",
    f"images/{FIGURE_B_SHA}.png",
    *(f"page_images/page_000{n}.png" for n in (1, 2, 3, 4)),
    *(f"page_images/bands/page_000{n}_band_0{k}.png" for n in (3, 4) for k in (1, 2)),
}


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

    def test_each_distinct_figure_is_read_once_through_its_loader(self, written):
        assert written.calls == {"a": 1, "b": 1}
        assert (written.out / "images" / f"{FIGURE_A_SHA}.png").read_bytes() == PNG_A

    def test_page_images_are_renders_at_the_effective_dpi(self, written):
        for page in written.returned.pages:
            image = page.page_image
            dpi = effective_dpi(DPI, 16_000_000, page.width_pt, page.height_pt)
            path = written.out / "page_images" / f"page_{page.page_number:04d}.png"
            assert (image.image_path, image.dpi, image.contrast_normalized) == (path, float(dpi), False)
            assert (image.width_px, image.height_px) == render_size(page.width_pt, page.height_pt, dpi)
            drawn = render_page(written.source, page.page_number - 1, dpi=dpi, memory_cap_bytes=CAP, timeout_s=TIMEOUT)
            assert png_pixels(path) == ("L", (drawn.px_width, drawn.px_height), drawn.pixels)

    def test_an_input_page_image_is_replaced_by_the_bundles(self, written):
        assert written.document.pages[2].page_image.dpi == 150.5
        assert written.returned.pages[2].page_image.dpi == float(DPI)

    def test_vision_pages_have_normalized_bands_at_the_vision_dpi(self, written):
        for page in written.returned.pages[2:]:
            n = page.page_number
            dpi = effective_dpi(VISION_DPI, 16_000_000, page.width_pt, page.height_pt)
            drawn = render_page(written.source, n - 1, dpi=dpi, memory_cap_bytes=CAP, timeout_s=TIMEOUT)
            expected = bands(normalized(drawn))
            assert normalized(drawn).pixels != drawn.pixels  # the fixture tells normalized from not
            paths = page.page_image.band_paths
            assert paths == tuple(written.out / "page_images" / "bands" / f"page_{n:04d}_band_0{k}.png" for k in (1, 2))
            assert len(expected) == 2
            for path, band in zip(paths, expected):
                assert png_pixels(path) == ("L", (band.px_width, band.px_height), band.pixels)

    def test_digital_and_hybrid_pages_have_no_bands(self, written):
        assert [page.page_image.band_paths for page in written.returned.pages[:2]] == [(), ()]

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

    def test_a_failed_render_of_a_vision_page_leaves_no_image_and_no_bands(self, tmp_path, source, monkeypatch):
        def failing(src, page_index, **kwargs):
            if page_index == 2:
                raise RenderFailed("the render worker timed out")
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", failing)
        returned = write(bound_document(source), tmp_path / "out", source)
        assert returned.pages[2].page_image is None
        names = files_in(tmp_path / "out")
        assert not any(name.startswith("page_images/bands/page_0003") for name in names)
        assert "page_images/bands/page_0004_band_01.png" in names
        [missing] = manifest_of(tmp_path / "out")["missing"]
        assert (missing["artifact"], missing["page_number"]) == ("page_image", 3)
        assert missing["reason"] == "the page was not rendered: RenderFailed: the render worker timed out"

    def test_a_failed_band_render_keeps_the_page_image_and_says_why(self, tmp_path, source, monkeypatch):
        def failing_bands(src, page_index, *, dpi, **kwargs):
            if page_index == 3 and dpi == VISION_DPI:
                raise RenderFailed("the render worker reached its memory cap")
            return fake_render(src, page_index, dpi=dpi, **kwargs)

        monkeypatch.setattr(bundle, "render_page", failing_bands)
        returned = write(bound_document(source), tmp_path / "out", source)
        image = returned.pages[3].page_image
        assert image.image_path == tmp_path / "out" / "page_images" / "page_0004.png" and image.band_paths == ()
        assert manifest_of(tmp_path / "out")["missing"] == [
            {
                "artifact": "bands",
                "page_number": 4,
                "element_id": None,
                "reason": "the page's bands were not rendered: RenderFailed: the render worker reached its memory cap",
            }
        ]

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
            (None, "DatagruntPDFError: figure p0001_e008 has no image bytes"),
        ],
    )
    def test_a_figure_whose_bytes_cannot_be_read_is_recorded(self, tmp_path, source, fast_render, loader, reason):
        def broken():
            if isinstance(loader, Exception):
                raise loader
            return loader

        document = bound_document(source, None if loader is None else broken)
        returned = write(document, tmp_path / "out", source)
        assert [figure.image_path for figure in returned.figures] == [
            None,
            None,
            tmp_path / "out" / "images" / f"{FIGURE_B_SHA}.png",
        ]
        assert f"images/{FIGURE_A_SHA}.png" not in files_in(tmp_path / "out")
        [missing] = manifest_of(tmp_path / "out")["missing"]
        assert (missing["artifact"], missing["page_number"], missing["element_id"]) == ("figure", 1, "p0001_e008")
        assert missing["reason"].startswith(f"figure {FIGURE_A_SHA} was not written: ") and reason in missing["reason"]

    def test_missing_entries_are_in_page_order(self, tmp_path, source, monkeypatch):
        monkeypatch.setattr(bundle, "render_page", lambda *a, **k: (_ for _ in ()).throw(RenderFailed("no")))
        write(bound_document(source, None), tmp_path / "out", source)
        missing = manifest_of(tmp_path / "out")["missing"]
        assert [(m["page_number"], m["artifact"]) for m in missing] == [
            (1, "figure"),
            (1, "page_image"),
            (2, "page_image"),
            (3, "page_image"),
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
        assert sorted(calls) == sorted(
            [(path, index, "pw", CAP, TIMEOUT) for index in range(4)]
            + [(path, index, "pw", CAP, TIMEOUT) for index in (2, 3)]
        )

    def test_an_encrypted_source_renders_with_its_password(self, tmp_path):
        path = source_pdf(tmp_path, user_password="pw")
        returned = write(bound_document(path), tmp_path / "out", path, password="pw")
        assert manifest_of(tmp_path / "out")["missing"] == []
        assert png_pixels(returned.pages[0].page_image.image_path)[0] == "L"


class TestChunks:
    @pytest.mark.parametrize(("workers", "size"), [(1, 32), (2, 16), (3, 10), (4, 8), (8, 4), (9, 4), (64, 4)])
    def test_chunk_size_is_the_specs(self, workers, size):
        assert bundle._chunk_size(workers) == size

    def test_chunks_cover_the_pages_in_order(self):
        assert bundle._chunks(70, 1) == (range(0, 32), range(32, 64), range(64, 70))
        assert bundle._chunks(9, 8) == (range(0, 4), range(4, 8), range(8, 9))
        assert bundle._chunks(0, 1) == ()

    def test_a_chunk_ends_before_the_next_begins_and_workers_run_together(self, tmp_path, source, monkeypatch):
        events, lock = [], threading.Lock()
        running = {"now": 0, "most": 0}

        def tracked(src, page_index, *, dpi, **kwargs):
            with lock:
                running["now"] += 1
                running["most"] = max(running["most"], running["now"])
                events.append(("start", page_index))
            time.sleep(0.05 if page_index == 0 else 0.3)
            with lock:
                running["now"] -= 1
                events.append(("end", page_index))
            return fake_render(src, page_index, dpi=dpi, **kwargs)

        monkeypatch.setattr(bundle, "render_page", tracked)
        monkeypatch.setattr(bundle, "_chunk_size", lambda workers: 2)
        write(bound_document(source), tmp_path / "out", source, workers=2)
        assert running["most"] == 2
        first_chunk_end = max(i for i, event in enumerate(events) if event[0] == "end" and event[1] in (0, 1))
        second_chunk_start = min(i for i, event in enumerate(events) if event[0] == "start" and event[1] in (2, 3))
        assert first_chunk_end < second_chunk_start

    def test_workers_write_the_same_bundle(self, tmp_path, written):
        out = tmp_path / "out"
        returned = write(bound_document(written.source), out, written.source, workers=3)
        mine, theirs = manifest_of(out), manifest_of(written.out)
        assert mine["artifacts"] == theirs["artifacts"] and mine["missing"] == theirs["missing"] == []
        assert returned == written.returned
