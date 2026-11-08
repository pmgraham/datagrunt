"""The image-only page marker end to end (#470, #472): ``extract_document`` and ``PDFReader`` on real PDFs.

``Page.image_only_reasons`` marks a page whose content lives only in its
page image. A3 records why a page's text cannot stand, in every OCR mode;
a page OCR reads is unmarked; every error page is ``("extraction_failed",)``.
So ``ocr="auto"`` sends to OCR (routes ``"vision"``) exactly the pages
``ocr="never"`` marks: the invariant pinned here, on PDFs built with the
ReportLab toolkit (``pdf_tests.builders``), a hand-written page read through
a broken ToUnicode map (``garbled_page``), a page pdfium cannot load
(``break_page``), a page whose worker passes its memory cap (the isolation
tests' inflating inline image) and a page past a walk bound (lowered in
the worker, as the pipeline tests lower it).

The invariant holds with or without tesseract: a vision page OCR cannot
read keeps its route. What OCR makes of a marked page under ``"auto"`` is
checked where tesseract is installed.
"""

import dataclasses
import json
import re
from pathlib import Path

import pytest

from datagrunt.pdf import bundle
from datagrunt.pdf.bundle import load_bundle, write_bundle
from datagrunt.pdf.codec import document_from_json_bytes
from datagrunt.pdf.elements import PDFDocument
from datagrunt.pdf.frame import element_frame
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.reader import PDFReader
from datagrunt.pdf.render import RenderFailed
from datagrunt.pdf.writer import PDFWriter
from pdf_tests.builders import Line, PageSpec, RasterImage, Rect, TextRun, break_page, build_pdf
from pdf_tests.old_formats import as_bundle_5_0
from pdf_tests.test_isolation import inline_content, pages_pdf, small_cap, text_content
from pdf_tests.test_ocr import needs_tesseract
from pdf_tests.test_ocr_scans import LETTER, LINES, png_of, rendered
from pdf_tests.test_pipeline import mixed_page, run_page_workers_as
from pdf_tests.test_routing import garbled_page

# A grey box: paint OCR could read, so a page of a few words over it is short of text (P2-C).
GREY_BOX = Rect(400.0, 600.0, 60.0, 40.0, fill_rgb=(0.5, 0.5, 0.5), stroke=False)
SHORT = PageSpec(items=(TextRun("Too short", 72.0, 100.0), GREY_BOX))

# Each document's pages ocr="never" marks (and ocr="auto" routes vision).
MARKED = {
    "digital": set(),
    "hybrid": set(),
    "scan": {1},
    "short": {1},
    "garbled": {1},
    "failed": {2},
    "mixed": {2, 4, 5},
}


@pytest.fixture(scope="module")
def pdfs(tmp_path_factory) -> dict[str, Path]:
    """Each document by name: a digital page, a searchable scan (hybrid), a scan, a short page, and the rest."""
    directory = tmp_path_factory.mktemp("image-only")
    digital = build_pdf(directory / "lines.pdf", [PageSpec(items=LINES)])
    scan = RasterImage(0.0, 0.0, *LETTER, png_of(rendered(digital)))
    layer = tuple(dataclasses.replace(line, render_mode=3) for line in LINES)  # the scan's OCR layer
    specs = {
        "digital": [mixed_page()],
        "hybrid": [PageSpec(items=(scan, *layer))],
        "scan": [PageSpec(items=(scan,))],
        "short": [SHORT],
        "failed": [mixed_page(), mixed_page()],
        "mixed": [mixed_page(), PageSpec(items=(scan,)), PageSpec(items=(scan, *layer)), SHORT, mixed_page()],
    }
    paths = {name: build_pdf(directory / f"{name}.pdf", pages) for name, pages in specs.items()}
    break_page(paths["failed"], 2)
    break_page(paths["mixed"], 5)
    garbled = directory / "garbled"
    garbled.mkdir()
    paths["garbled"] = garbled_page(garbled)
    return paths


@pytest.fixture(scope="module")
def extracted(pdfs):
    """extracted(name, ocr): extract_document of that document under ``ocr`` (once a pair)."""
    documents: dict[tuple[str, str], PDFDocument] = {}

    def extract(name: str, ocr: str) -> PDFDocument:
        if (name, ocr) not in documents:
            documents[name, ocr] = extract_document(pdfs[name], ocr=ocr)
        return documents[name, ocr]

    return extract


def marked(document: PDFDocument) -> set[int]:
    """The page numbers of the document's image-only pages."""
    return {page.page_number for page in document.pages if page.image_only_reasons}


def sent_to_ocr(document: PDFDocument) -> set[int]:
    """The page numbers of the document's vision pages."""
    return {page.page_number for page in document.pages if page.route == "vision"}


def facts(document: PDFDocument) -> list[tuple[str, str, tuple[str, ...] | None]]:
    """Each page's status, route and image-only reasons."""
    return [(page.status, page.route, page.image_only_reasons) for page in document.pages]


class TestAutoReadsByOcrExactlyThePagesNeverMarks:
    """``{pages ocr="never" marks} == {pages ocr="auto" routes vision}``, document by document (#472)."""

    @pytest.mark.parametrize("name", list(MARKED))
    def test_the_marked_pages_are_the_vision_pages(self, extracted, name):
        never, auto = extracted(name, "never"), extracted(name, "auto")

        assert marked(never) == sent_to_ocr(auto) == MARKED[name]
        assert set(never.image_only_pages) == MARKED[name]

    def test_a_page_past_its_workers_memory_cap_too(self, tmp_path, monkeypatch):
        # Its worker ends: the pipeline's error page (#470), whatever the mode. The other pages are short and clean.
        contents = [text_content("page one"), inline_content("page two"), text_content("page three")]
        path = pages_pdf(tmp_path / "doc.pdf", contents)
        small_cap(monkeypatch)

        never, auto = extract_document(path, ocr="never"), extract_document(path, ocr="auto")

        assert marked(never) == sent_to_ocr(auto) == {2}
        assert facts(never)[1] == facts(auto)[1] == ("error", "vision", ("extraction_failed",))

    def test_a_page_past_a_walk_bound_too(self, tmp_path, monkeypatch):
        # A2 cannot read page 2 (4 objects, the worker's bound lowered to 3): fallback_page's under either mode.
        rules = tuple(Line(20.0, 20.0 + 10.0 * index, 200.0, 20.0 + 10.0 * index) for index in range(4))
        pages = [
            PageSpec(items=(TextRun("One", 72.0, 72.0),)),
            PageSpec(items=rules),
            PageSpec(items=(TextRun("Three", 72.0, 72.0),)),
        ]
        path = build_pdf(tmp_path / "doc.pdf", pages)
        run_page_workers_as(monkeypatch, "with_a_3_object_walk_bound")

        never, auto = extract_document(path, workers=2, ocr="never"), extract_document(path, workers=2, ocr="auto")

        assert marked(never) == sent_to_ocr(auto) == {2}
        assert facts(never)[1] == ("error", "vision", ("extraction_failed",))


class TestNeverMarksEachPageWithItsReasons:
    """Under ``ocr="never"`` each page reads why its content lives only in its page image, or ``()``."""

    def test_each_page_of_a_mixed_document(self, extracted):
        document = extracted("mixed", "never")

        assert facts(document) == [
            ("ok", "digital", ()),  # a digital page
            ("ok", "digital", ("too_little_text",)),  # a scan: no text layer
            ("ok", "digital", ()),  # a searchable scan: its OCR layer stands
            ("ok", "digital", ("too_little_text",)),  # a few words over paint
            ("error", "vision", ("extraction_failed",)),  # a page pdfium cannot load
        ]
        assert document.image_only_pages == (2, 4, 5)

    def test_a_garbled_page(self, extracted):
        assert facts(extracted("garbled", "never")) == [("ok", "digital", ("garbled_text",))]

    @pytest.mark.parametrize("name", ["mixed", "garbled"])
    def test_pdfreader_agrees(self, pdfs, extracted, name):
        reader = PDFReader(pdfs[name], ocr="never")

        assert [page.image_only_reasons for page in reader.pages] == [
            page.image_only_reasons for page in extracted(name, "never").pages
        ]
        assert reader.document.image_only_pages == extracted(name, "never").image_only_pages
        # Pinned, so the comparison above cannot pass with both sides unmarked (#470 review).
        assert reader.document.image_only_pages == {"mixed": (2, 4, 5), "garbled": (1,)}[name]


@needs_tesseract
class TestAutoUnmarksThePagesOcrReads:
    """Under ``ocr="auto"`` OCR reads the pages ``"never"`` marks: only the page nothing read stays marked."""

    def test_each_page_of_a_mixed_document(self, extracted):
        document = extracted("mixed", "auto")

        assert facts(document) == [
            ("ok", "digital", ()),
            ("ok", "vision", ()),
            ("ok", "hybrid", ()),
            ("ok", "vision", ()),
            ("error", "vision", ("extraction_failed",)),
        ]
        assert document.image_only_pages == (5,)

    def test_a_garbled_page(self, extracted):
        assert facts(extracted("garbled", "auto")) == [("ok", "vision", ())]


class TestTheMarkerIsCarried:
    """The document JSON, the element frame and the bundle carry what ``ocr="never"`` marks, losslessly (#470)."""

    def test_the_json_round_trips_it(self, extracted, tmp_path):
        document = extracted("mixed", "never")
        path = PDFWriter(document).write_json(tmp_path / "doc.json")

        assert document_from_json_bytes(path.read_bytes()) == document

    def test_each_row_holds_its_pages_reasons(self, extracted):
        document = extracted("mixed", "never")
        frame = element_frame(document)

        pages = document.pages
        assert [row["image_only_reasons"] for row in frame.iter_rows(named=True)] == [
            list(pages[number - 1].image_only_reasons) for number in frame["page_number"].to_list()
        ]
        assert 5 not in frame["page_number"].to_list()  # the error page: no elements, so no row names it

    def test_the_bundle_records_and_reloads_it(self, pdfs, extracted, tmp_path):
        out = tmp_path / "out"
        returned = write_bundle(extracted("mixed", "never"), out, source=pdfs["mixed"], dpi=72)

        assert json.loads((out / "manifest.json").read_bytes())["image_only_pages"] == [2, 4, 5]
        assert "<!-- page 2 route=digital status=ok image_only=too_little_text -->" in (
            (out / "markdown" / "pages" / "page_0002.md").read_text(encoding="utf-8")
        )
        assert load_bundle(out) == returned
        assert PDFReader(out).document.image_only_pages == (2, 4, 5)

    def test_its_bundle_as_datagrunt_5_2_wrote_it_reads_not_recorded(self, pdfs, extracted, tmp_path):
        out = tmp_path / "out"
        write_bundle(extracted("mixed", "never"), out, source=pdfs["mixed"], dpi=72)
        as_bundle_5_0(out)

        loaded = load_bundle(out)

        assert "image_only=" not in (out / "markdown" / "document.md").read_text(encoding="utf-8")
        assert [page.image_only_reasons for page in loaded.pages] == [None] * 5
        assert loaded.image_only_pages == () and PDFReader(out).document == loaded


def only_copy(*reasons: str) -> str:
    """The clause #471 appends to an image-only page's missing page image reason."""
    return f"; its content lives only in its page image ({', '.join(reasons)}): the source PDF is its only copy"


def page_image_reasons(out: Path) -> list[tuple[int, str]]:
    """Each missing page image's page number and reason, in the manifest's order."""
    missing = json.loads((out / "manifest.json").read_bytes())["missing"]
    return [(entry["page_number"], entry["reason"]) for entry in missing if entry["artifact"] == "page_image"]


def only_copy_warnings(caught) -> list[str]:
    """The messages of the warnings #471 emits among ``caught``."""
    return [str(w.message) for w in caught if "hold content only in their page images" in str(w.message)]


# The warning a bundle of the mixed document under render_pages=False emits: pages 2, 4 and 5 are marked.
SKIPPED_MIXED = (
    "pages [2, 4, 5] hold content only in their page images, which this bundle does not write (render_pages=False); "
    "the source PDF is their only copy"
)


class TestABundleThatLeavesAnImageOnlyPageUnwrittenSaysSo:
    """#471: the opt-out is honoured, nothing more is written; the manifest and one warning say the PDF is the copy."""

    def test_render_pages_false(self, pdfs, extracted, tmp_path):
        out = tmp_path / "out"
        with pytest.warns(UserWarning, match=re.escape(SKIPPED_MIXED)) as caught:
            returned = write_bundle(extracted("mixed", "never"), out, source=pdfs["mixed"], dpi=72, render_pages=False)

        skipped = "the page was not rendered: render_pages=False"
        assert page_image_reasons(out) == [
            (1, skipped),
            (2, skipped + only_copy("too_little_text")),
            (3, skipped),
            (4, skipped + only_copy("too_little_text")),
            (5, skipped + only_copy("extraction_failed")),
        ]
        assert only_copy_warnings(caught) == [SKIPPED_MIXED]
        assert not (out / "page_images").exists() and load_bundle(out) == returned

    def test_pdfwriter_write_bundle_warns_as_write_bundle_does(self, pdfs, tmp_path):
        writer = PDFWriter(pdfs["mixed"], ocr="never", dpi=72)
        with pytest.warns(UserWarning, match=re.escape(SKIPPED_MIXED)) as caught:
            writer.write_bundle(tmp_path / "out", render_pages=False)

        assert only_copy_warnings(caught) == [SKIPPED_MIXED]
        assert page_image_reasons(tmp_path / "out")[4] == (
            5,
            "the page was not rendered: render_pages=False" + only_copy("extraction_failed"),
        )

    def test_a_failed_render(self, pdfs, extracted, tmp_path):
        # Page 5 is a page pdfium cannot load: its render fails too, so no output holds it but the PDF.
        expected = (
            "pages [5] hold content only in their page images, which this bundle could not write "
            "(manifest.json's missing entries say why); the source PDF is their only copy"
        )
        with pytest.warns(UserWarning, match=re.escape(expected)) as caught:
            write_bundle(extracted("mixed", "never"), tmp_path / "out", source=pdfs["mixed"], dpi=72)

        assert only_copy_warnings(caught) == [expected]
        [(number, reason)] = page_image_reasons(tmp_path / "out")
        assert number == 5 and reason.startswith(f"the page was not rendered: RenderFailed: {pdfs['mixed']}: page 5: ")
        assert reason.endswith(only_copy("extraction_failed")) and reason.count("; its content lives only") == 1

    @pytest.mark.parametrize("name", ["digital", "hybrid"])
    def test_a_document_with_no_image_only_page_warns_nothing(self, pdfs, extracted, tmp_path, recwarn, name):
        write_bundle(extracted(name, "never"), tmp_path / "out", source=pdfs[name], dpi=72, render_pages=False)

        assert page_image_reasons(tmp_path / "out") == [(1, "the page was not rendered: render_pages=False")]
        assert [str(w.message) for w in recwarn] == []


class TestPageRendersThatSkipAnImageOnlyPageSaySo:
    """#471: ``PDFWriter.render_pages_as_images`` skips a page it cannot render; an image-only one also warns."""

    def test_the_skipped_image_only_page_is_named_once(self, pdfs, tmp_path):
        expected = (
            "pages [5] hold content only in their page images, which render_pages_as_images could not render "
            "(the datagrunt.pdf.writer log says why); the source PDF is their only copy"
        )
        writer = PDFWriter(pdfs["mixed"], ocr="never")
        with pytest.warns(UserWarning, match=re.escape(expected)) as caught:
            paths = writer.render_pages_as_images(tmp_path / "pages", dpi=72)

        assert only_copy_warnings(caught) == [expected]
        assert [path.name for path in paths] == [f"page_000{number}.png" for number in (1, 2, 3, 4)]

    def test_a_skipped_page_that_is_not_image_only_is_only_logged(self, pdfs, tmp_path, monkeypatch, recwarn, caplog):
        def failing(*args, **kwargs):
            raise RenderFailed("the render worker failed")

        monkeypatch.setattr(bundle, "render_page", failing)  # the page renders, not extraction's workers

        assert PDFWriter(pdfs["digital"], ocr="never").render_pages_as_images(tmp_path / "pages", dpi=72) == []
        assert [str(w.message) for w in recwarn] == []
        assert "page 1 was not rendered: RenderFailed: the render worker failed" in caplog.text
