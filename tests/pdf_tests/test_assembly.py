"""Tests for v5 page assembly (#368, A4b group 3): figures, then the page's elements and Pass C.

Unit tests build A2 values (``RawImage``, ``PageImages``, ``PageGeometry``)
directly: boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.
End-to-end tests build PDFs with the ReportLab toolkit (``pdf_tests.builders``)
and read them through ``extract_page``.
"""

import dataclasses
import hashlib
import io
import os
import pickle
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from PIL import Image

from datagrunt.pdf import _loaders, _source, assembly, layout
from datagrunt.pdf._loaders import FigureRead
from datagrunt.pdf.assembly import (
    FigureDraft,
    PageFigures,
    assemble_page,
    check_assessment,
    conservation,
    page_elements,
    page_figures,
    pass_c,
)
from datagrunt.pdf.elements import BBox, Figure, Page, PageImage, PageQuality, Paragraph, Table
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFSourceChangedError
from datagrunt.pdf.layout import (
    MAX_CROSSINGS,
    LatticeCell,
    LatticeResult,
    LatticeTable,
    find_lattice_tables,
)
from datagrunt.pdf.pdfium_engine import (
    ImageStats,
    PageGeometry,
    PageImages,
    RawPage,
    extract_page,
    open_document,
    recover_dropped_text,
)
from datagrunt.pdf.pipeline import SourceIdentity
from datagrunt.pdf.quality import PageAssessment, assess_page
from datagrunt.pdf.reader import PDFReader
from datagrunt.pdf.render import RenderRefused, effective_dpi, render_page, render_size
from datagrunt.pdf.scope import apply_scope
from datagrunt.pdf.writer import PDFWriter
from pdf_tests.builders import PageSpec, RasterImage, Rect, TextRun, build_pdf, hand_written_pdf, solid_png
from pdf_tests.test_layout_apart import tiny_text_page
from pdf_tests.test_layout_lattice import rules as lattice_rules
from pdf_tests.test_layout_lattice import written
from pdf_tests.test_layout_stream import after
from pdf_tests.test_layout_stream_tables import XS as STREAM_XS
from pdf_tests.test_layout_stream_tables import body_text, staircase, table_runs
from pdf_tests.test_pdfium_engine import imported_modules
from pdf_tests.test_pdfium_images import j2k, xobject_page
from pdf_tests.test_pdfium_spans import content_page
from pdf_tests.test_quality import SHA, image
from pdf_tests.test_routing import garbled_page
from pdf_tests.test_source import promptly

W, H = 400.0, 300.0
GEOMETRY = PageGeometry(page_number=1, width_pt=W, height_pt=H, rotation_deg=0, crop_offset=(0.0, 0.0))
INSIDE = (60.0, 40.0, 180.0, 120.0)


def stats(**overrides) -> ImageStats:
    """ImageStats with every count 0 (or as overridden)."""
    fields = {
        "skipped_nonfinite_images": 0,
        "skipped_oversized_images": 0,
        "skipped_unsized_images": 0,
        "skipped_over_budget_images": 0,
        "skipped_overlong_images": 0,
        "max_cover_fraction": 0.0,
    }
    return ImageStats(**(fields | overrides))


def drafted(tmp_path, *images, geometry=GEOMETRY, image_stats=None, **options) -> PageFigures:
    """page_figures of ``images`` on a 400 x 300 pt page, read from ``tmp_path/doc.pdf`` (which need not exist)."""
    found = PageImages(images=images, stats=image_stats or stats())
    return page_figures(found, geometry, source=tmp_path / "doc.pdf", **options)


def sized(width: int, height: int, **overrides):
    """A RawImage of ``width`` x ``height`` px (decoded and declared) drawn into INSIDE."""
    return image(INSIDE, px_width=width, px_height=height, declared_px=(width, height), **overrides)


class TestModule:
    """The assembly module is v5 only and keeps no mutable state."""

    def test_imports_no_v4_and_no_pdf_library(self):
        banned = ("pypdfium2", "fitz", "pymupdf", "pdfplumber", "datagrunt.core.pdf_io", "datagrunt.pdf_api")

        assert not [name for name in imported_modules(assembly) if name.lower().startswith(banned)]

    def test_module_level_tables_are_immutable(self):
        mutable = (dict, list, set, bytearray)
        names = {name: value for name, value in vars(assembly).items() if not name.startswith("__")}

        assert {name for name, value in names.items() if isinstance(value, mutable)} == set()


class TestFigureDraft:
    """A FigureDraft is a frozen, slotted, picklable, validated record of one figure before its id."""

    def draft(self, tmp_path, **overrides) -> FigureDraft:
        raw = image(INSIDE)
        fields = {
            "bbox": BBox(x0=60.0, y0=40.0, x1=180.0, y1=120.0),
            "hidden_reasons": (),
            "sha256": SHA,
            "width_px": 1000,
            "height_px": 750,
            "loader": FigureRead(source=tmp_path / "doc.pdf", page_index=0, image=raw),
        }
        return FigureDraft(**(fields | overrides))

    def test_is_frozen_slotted_and_picklable(self, tmp_path):
        draft = self.draft(tmp_path)

        with pytest.raises(dataclasses.FrozenInstanceError):
            draft.width_px = 2
        assert not hasattr(draft, "__dict__")
        assert pickle.loads(pickle.dumps(draft)) == draft

    @pytest.mark.parametrize(
        "field,bad,error",
        [
            pytest.param("bbox", (0.0, 0.0, 1.0, 1.0), TypeError, id="bbox-tuple"),
            pytest.param("hidden_reasons", ("tiny_font",), ValueError, id="reason-not-off-page"),
            pytest.param("hidden_reasons", ["off_page"], TypeError, id="reasons-list"),
            pytest.param("sha256", "abc", ValueError, id="sha-short"),
            pytest.param("width_px", 0, ValueError, id="width-0"),
            pytest.param("height_px", True, TypeError, id="height-bool"),
            pytest.param("loader", None, TypeError, id="loader-none"),
        ],
    )
    def test_rejects_bad_fields(self, tmp_path, field, bad, error):
        with pytest.raises(error, match=field):
            self.draft(tmp_path, **{field: bad})

    def test_sha256_must_be_the_loaders(self, tmp_path):
        with pytest.raises(ValueError, match="sha256"):
            self.draft(tmp_path, sha256="1" * 64)

    def test_figure_carries_the_facts_and_binds_both_loaders(self, tmp_path):
        draft = self.draft(tmp_path)

        figure = draft.figure(page_number=2, reading_order=5)

        assert isinstance(figure, Figure)
        assert (figure.element_id, figure.page_number, figure.reading_order) == ("p0002_e005", 2, 5)
        assert (figure.bbox, figure.sha256, figure.width_px, figure.height_px) == (draft.bbox, SHA, 1000, 750)
        assert (figure.caption, figure.image_path, figure.scope_path, figure.hidden_reasons) == (None, None, (), ())
        assert figure._loader == draft.loader.png_bytes
        assert figure._raw_loader == draft.loader.raw_bytes

    def test_figure_keeps_the_off_page_reason(self, tmp_path):
        draft = self.draft(tmp_path, bbox=BBox(x0=-200.0, y0=10.0, x1=-100.0, y1=20.0), hidden_reasons=("off_page",))

        assert draft.figure(page_number=1, reading_order=0).hidden_reasons == ("off_page",)


class TestPageFigures:
    """page_figures: each RawImage becomes a FigureDraft with A2's facts; small ones are skipped as v4 skips them."""

    def test_an_image_becomes_a_draft_with_its_facts(self, tmp_path):
        raw = image(INSIDE, object_index=4)

        (draft,) = drafted(tmp_path, raw).drafts

        assert draft.bbox == BBox(x0=60.0, y0=40.0, x1=180.0, y1=120.0)
        assert (draft.sha256, draft.width_px, draft.height_px, draft.hidden_reasons) == (SHA, 1000, 750, ())
        assert draft.loader == FigureRead(source=tmp_path / "doc.pdf", page_index=0, image=raw)

    def test_the_loader_reads_the_geometrys_page_with_the_password(self, tmp_path):
        geometry = dataclasses.replace(GEOMETRY, page_number=3)

        (draft,) = drafted(tmp_path, image(INSIDE), geometry=geometry, password="secret").drafts

        assert (draft.loader.page_index, draft.loader.password) == (2, "secret")

    def test_images_keep_their_order(self, tmp_path):
        first, second = image(INSIDE, object_index=2), image((200.0, 40.0, 300.0, 120.0), object_index=7)

        drafts = drafted(tmp_path, first, second).drafts

        assert [draft.loader.object_index for draft in drafts] == [2, 7]

    def test_no_image_no_drafts_and_no_warnings(self, tmp_path):
        assert drafted(tmp_path) == PageFigures(drafts=(), warnings=())

    def test_no_loader_is_called(self, tmp_path, monkeypatch):
        def refuse(self):
            raise AssertionError("a loader was called")

        monkeypatch.setattr(FigureRead, "png_bytes", refuse)
        monkeypatch.setattr(FigureRead, "raw_bytes", refuse)

        (draft,) = drafted(tmp_path, image(INSIDE)).drafts
        figure = draft.figure(page_number=1, reading_order=0)

        # The figure holds the loaders, bound: reading it calls them (here, the refusal).
        with pytest.raises(AssertionError, match="a loader was called"):
            figure.read_bytes()
        with pytest.raises(AssertionError, match="a loader was called"):
            figure.read_raw_bytes()

    @pytest.mark.parametrize(
        "size,kept",
        [
            pytest.param((39, 40), False, id="39-wide"),
            pytest.param((40, 39), False, id="39-tall"),
            pytest.param((40, 40), True, id="40-square"),
            pytest.param((2000, 41), True, id="41-tall"),
        ],
    )
    def test_an_image_under_the_minimum_on_either_side_is_skipped(self, tmp_path, size, kept):
        found = drafted(tmp_path, sized(*size))

        assert (len(found.drafts), found.warnings) == (int(kept), ())

    def test_the_minimum_is_an_option(self, tmp_path):
        found = drafted(tmp_path, sized(99, 100), sized(100, 100), min_image_dimension=100)

        assert [(draft.width_px, draft.height_px) for draft in found.drafts] == [(100, 100)]

    def test_a_minimum_of_0_keeps_every_sized_image(self, tmp_path):
        found = drafted(tmp_path, sized(1, 1), min_image_dimension=0)

        assert [(draft.width_px, draft.height_px) for draft in found.drafts] == [(1, 1)]

    @pytest.mark.parametrize(
        "bad,error",
        [
            pytest.param(-1, ValueError, id="negative"),
            pytest.param(True, TypeError, id="bool"),
            pytest.param(40.0, TypeError, id="float"),
            pytest.param(None, TypeError, id="none"),
        ],
    )
    def test_the_minimum_must_be_an_int_of_at_least_0(self, tmp_path, bad, error):
        with pytest.raises(error, match="min_image_dimension"):
            drafted(tmp_path, image(INSIDE), min_image_dimension=bad)

    def test_an_oversized_image_is_a_figure_of_its_recorded_size(self, tmp_path):
        # A2 records a decode size over the pixel bound without pixel metadata; ImageLoader (its figure worker's)
        # refuses to decode it.
        huge = image(INSIDE, px_width=20000, px_height=20000, declared_px=(20000, 20000), bits_per_pixel=None)

        (draft,) = drafted(tmp_path, huge).drafts

        assert (draft.width_px, draft.height_px) == (20000, 20000)

    def test_an_unsized_image_is_no_figure_whatever_its_dictionary_declares(self, tmp_path):
        # A JPEG behind Flate that does not inflate within 1 MiB: A2 cannot read its frame, so it records px 0 x 0.
        # pdfium decodes a JPEG at its frame's size, never its /Width and /Height: those are not its size.
        behind_flate = image(
            INSIDE,
            px_width=0,
            px_height=0,
            declared_px=(158, 506),
            filters=("FlateDecode", "DCTDecode"),
            bits_per_pixel=None,
            colorspace=None,
            object_index=4,
        )

        found = drafted(tmp_path, behind_flate)

        assert found == PageFigures(
            drafts=(), warnings=("1 image(s) not emitted as figures: their pixel size is unknown (object 4)",)
        )

    def test_an_unsized_image_declared_under_the_minimum_is_named_not_skipped(self, tmp_path):
        small = image(
            INSIDE, px_width=0, px_height=0, declared_px=(20, 20), bits_per_pixel=None, colorspace=None, object_index=2
        )

        found = drafted(tmp_path, small)

        assert found.warnings == ("1 image(s) not emitted as figures: their pixel size is unknown (object 2)",)

    @pytest.mark.parametrize(
        "px,declared",
        [
            pytest.param((0, 0), (0, 0), id="neither"),
            pytest.param((0, 100), (0, 100), id="one-side-unknown"),
        ],
    )
    def test_an_image_of_no_known_size_is_no_figure_and_a_warning(self, tmp_path, px, declared):
        unknown = image(INSIDE, px_width=px[0], px_height=px[1], declared_px=declared, object_index=9)

        found = drafted(tmp_path, unknown)

        assert found.drafts == ()
        assert found.warnings == ("1 image(s) not emitted as figures: their pixel size is unknown (object 9)",)

    def test_an_image_not_read_is_no_figure_and_a_warning(self, tmp_path):
        unread = image(INSIDE, raw_sha256=None, object_index=3)

        found = drafted(tmp_path, unread)

        assert found.drafts == ()
        assert found.warnings == ("1 image(s) not emitted as figures: their streams were not read (object 3)",)

    def test_an_image_not_read_is_named_whatever_its_dictionary_declares(self, tmp_path):
        # A2 records an image it did not read at px 0 x 0: its declared 10 x 10 does not skip it unnamed.
        unread = image(INSIDE, px_width=0, px_height=0, declared_px=(10, 10), raw_sha256=None, object_index=5)

        found = drafted(tmp_path, unread)

        assert found.warnings == ("1 image(s) not emitted as figures: their streams were not read (object 5)",)

    def test_a_warning_names_at_most_5_objects(self, tmp_path):
        unread = [image(INSIDE, raw_sha256=None, object_index=index) for index in range(7)]

        (warning,) = drafted(tmp_path, *unread).warnings

        assert warning == (
            "7 image(s) not emitted as figures: their streams were not read (objects 0, 1, 2, 3, 4 and 2 more)"
        )

    @pytest.mark.parametrize(
        "count,expected",
        [
            pytest.param(
                {"skipped_nonfinite_images": 2},
                "2 image object(s) skipped: their placement on the page is not finite",
                id="nonfinite",
            ),
            pytest.param(
                {"skipped_oversized_images": 1},
                "1 image(s) too large to decode or read: never decoded",
                id="oversized",
            ),
            pytest.param(
                {"skipped_unsized_images": 3},
                "3 image(s) whose decode size cannot be known without decoding: never decoded",
                id="unsized",
            ),
            pytest.param(
                {"skipped_over_budget_images": 4},
                "4 image(s) not read: the page's image stream budget was spent",
                id="over-budget",
            ),
            pytest.param(
                {"skipped_overlong_images": 5},
                "5 image(s) whose stream decodes past their declared size, or cannot be checked: "
                "decoded only by a page render",
                id="overlong",
            ),
        ],
    )
    def test_a2s_image_counts_are_carried_as_warnings(self, tmp_path, count, expected):
        assert drafted(tmp_path, image_stats=stats(**count)).warnings == (expected,)

    def test_a2s_warnings_come_before_the_figures(self, tmp_path):
        found = drafted(tmp_path, image(INSIDE, raw_sha256=None), image_stats=stats(skipped_over_budget_images=1))

        assert found.warnings == (
            "1 image(s) not read: the page's image stream budget was spent",
            "1 image(s) not emitted as figures: their streams were not read (object 1)",
        )

    @pytest.mark.parametrize(
        "bbox,expected,reasons",
        [
            pytest.param(INSIDE, INSIDE, (), id="inside"),
            pytest.param((-0.4, 0.0, W + 0.4, H), (-0.4, 0.0, W + 0.4, H), (), id="within-tolerance"),
            pytest.param((-20.0, -10.0, W + 20.0, H + 10.0), (0.0, 0.0, W, H), (), id="bleed-clipped"),
            pytest.param((350.0, 100.0, 450.0, 200.0), (350.0, 100.0, W, 200.0), (), id="past-the-right-edge"),
            pytest.param((50.0, 50.0, 50.0, 90.0), (50.0, 50.0, 50.0, 90.0), (), id="no-width-inside"),
            pytest.param((-200.0, 10.0, -100.0, 20.0), (-200.0, 10.0, -100.0, 20.0), ("off_page",), id="left-of-page"),
            pytest.param((-100.0, 10.0, 0.0, 20.0), (-100.0, 10.0, 0.0, 20.0), ("off_page",), id="touching-the-edge"),
            pytest.param((10.0, H + 1.0, 30.0, H + 9.0), (10.0, H + 1.0, 30.0, H + 9.0), ("off_page",), id="below"),
        ],
    )
    def test_placement_follows_a1s_page_rule(self, tmp_path, bbox, expected, reasons):
        (draft,) = drafted(tmp_path, image(bbox)).drafts

        x0, y0, x1, y1 = expected
        assert (draft.bbox, draft.hidden_reasons) == (BBox(x0=x0, y0=y0, x1=x1, y1=y1), reasons)
        assert draft.bbox.within(W, H) == (not reasons)

    def test_rejects_wrong_argument_types(self, tmp_path):
        with pytest.raises(TypeError, match="images"):
            page_figures((), GEOMETRY, source=tmp_path / "doc.pdf")
        with pytest.raises(TypeError, match="geometry"):
            page_figures(PageImages(images=(), stats=stats()), None, source=tmp_path / "doc.pdf")
        with pytest.raises(TypeError, match="source"):
            page_figures(PageImages(images=(), stats=stats()), GEOMETRY, source=b"doc.pdf")


def read_page(path) -> RawPage:
    """Page 1's raw facts."""
    page = extract_page(open_document(path), 0)
    assert isinstance(page, RawPage)
    return page


class TestPageFiguresEndToEnd:
    """Real images, read through extract_page; a figure's loaders read the PDF only when asked."""

    def test_an_embedded_image_becomes_a_readable_figure(self, tmp_path):
        png = solid_png(120, 80, (0.2, 0.4, 0.6))
        path = build_pdf(
            tmp_path / "doc.pdf", [PageSpec(width=W, height=H, items=(RasterImage(*INSIDE[:2], 120.0, 80.0, png),))]
        )
        page = read_page(path)

        (draft,) = page_figures(page.images, page.geometry, source=path).drafts
        figure = draft.figure(page_number=1, reading_order=0)

        (raw,) = page.images.images
        assert (figure.width_px, figure.height_px, figure.sha256) == (120, 80, raw.raw_sha256)
        assert figure.bbox == pytest.approx(BBox(x0=60.0, y0=40.0, x1=180.0, y1=120.0))
        assert hashlib.sha256(figure.read_raw_bytes()).hexdigest() == raw.raw_sha256
        assert figure.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")

    @pytest.mark.parametrize("declared", [b"", b"/Width 16 /Height 16 ", b"/Width 2000 /Height 2000 "])
    def test_a_refused_image_is_a_figure_of_its_headers_size_and_says_why(self, tmp_path, declared):
        # A JPEG 2000 codestream of 2000 x 2000 x 5 components: refused (5 > 4), at its SIZ size whatever /Width says.
        entries = declared + b"/ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /JPXDecode"
        path = xobject_page(tmp_path, j2k((16, 8), siz=(2000, 2000), tile=(2000, 2000), components=5), entries)
        page = read_page(path)

        found = page_figures(page.images, page.geometry, source=path)

        (draft,) = found.drafts
        assert (draft.width_px, draft.height_px) == (2000, 2000)
        assert found.warnings == ("1 image(s) too large to decode or read: never decoded",)
        figure = draft.figure(page_number=1, reading_order=0)
        with pytest.raises(DatagruntPDFError, match=re.escape("object 0 has 5 components, over the 4-component bound")):
            figure.read_bytes()

    def test_a_jpeg_whose_header_cannot_be_read_is_no_figure(self, tmp_path):
        entries = b"/Width 200 /Height 200 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode"
        page = read_page(xobject_page(tmp_path, b"not a jpeg header at all" * 4, entries))

        found = page_figures(page.images, page.geometry, source=tmp_path / "hand.pdf")

        assert found == PageFigures(
            drafts=(),
            warnings=(
                "1 image(s) whose decode size cannot be known without decoding: never decoded",
                "1 image(s) not emitted as figures: their pixel size is unknown (object 0)",
            ),
        )

    def test_a_small_image_is_skipped(self, tmp_path):
        items = (RasterImage(10.0, 10.0, 30.0, 30.0, solid_png(30, 30, (0.0, 0.0, 0.0))),)
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(width=W, height=H, items=items)])
        page = read_page(path)

        assert page_figures(page.images, page.geometry, source=path) == PageFigures(drafts=(), warnings=())


# Page assembly (task 7): assemble_page and Pass C.

LETTER_W, LETTER_H = 612.0, 792.0
LATTICE_XS = (50.0, 150.0, 250.0, 350.0)
SPEC = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))
PARTS = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"), ("Washer", "100", "0.01"))


def letter(tmp_path, *items, rotate: int = 0, name: str = "doc.pdf"):
    """A one-page letter-size PDF of ``items``."""
    return build_pdf(tmp_path / name, [PageSpec(items=items, rotate=rotate)])


def inputs(path, index: int = 0):
    """Page ``index``'s raw facts, its assessment (with its recovery) and its lattice tables."""
    handle = open_document(path)
    page = extract_page(handle, index)
    recovered = recover_dropped_text(handle, index) if page.text.stats.dropped_text_objects else None
    assessment = assess_page(page, recovered=recovered)
    return page, assessment, find_lattice_tables(page.segments.segments, assessment.pieces)


def assembled(path, index: int = 0, **options) -> Page:
    """assemble_page of page ``index`` of ``path``."""
    return assemble_page(*inputs(path, index), source=path, **options)


def kinds(page: Page) -> list[str]:
    """Each element's class name."""
    return [type(element).__name__ for element in page.elements]


def mixed_items() -> tuple:
    """A heading, a paragraph, a key-value, a list, an image, a ruled table and a borderless one, top to bottom."""
    return (
        TextRun("Results", 72.0, 60.0, size=18.0),
        *body_text(90.0),
        TextRun("Invoice: 10442", 72.0, 150.0, size=10.0),
        TextRun("• first item", 72.0, 180.0, size=10.0),
        TextRun("• second item", 72.0, 194.0, size=10.0),
        RasterImage(72.0, 220.0, 120.0, 80.0, solid_png(120, 80, (0.2, 0.4, 0.6))),
        *lattice_rules(LATTICE_XS, (340.0, 360.0, 380.0, 400.0)),
        *written(SPEC, LATTICE_XS, (340.0, 360.0, 380.0, 400.0)),
        *table_runs(PARTS, top=460.0),
    )


class TestAssemblePage:
    """assemble_page: the page's elements in reading order, with canonical ids, its facts and Pass C."""

    def test_elements_come_in_reading_order_with_canonical_ids(self, tmp_path):
        page = assembled(letter(tmp_path, *mixed_items()))

        assert kinds(page) == ["Heading", "Paragraph", "KeyValue", "BulletList", "Figure", "Table", "Table"]
        assert [element.element_id for element in page.elements] == [f"p0001_e{index:03d}" for index in range(7)]
        assert [element.reading_order for element in page.elements] == list(range(7))

    def test_the_page_carries_its_facts_and_passes_pass_c(self, tmp_path):
        path = letter(tmp_path, *mixed_items())
        raw, assessment, lattice = inputs(path)

        page = assemble_page(raw, assessment, lattice, source=path)

        assert (page.page_number, page.width_pt, page.height_pt) == (1, LETTER_W, LETTER_H)
        assert (page.status, page.route, page.quality, page.page_image, page.error) == (
            "ok",
            "digital",
            assessment.quality,
            PageImage(page_number=1, dpi=200.0, width_px=1700, height_px=2200),
            None,
        )
        assert (page.integrity_verified, page.warnings) == (True, ())

    def test_text_elements_convert_their_fields(self, tmp_path):
        heading, paragraph, pair, bullets, *_ = assembled(letter(tmp_path, *mixed_items())).elements

        assert (heading.text, heading.level, heading.scope_path) == ("Results", 1, ("Results",))
        assert paragraph.text.startswith("The woodland is vast") and paragraph.scope_path == ()
        assert (pair.key, pair.value) == ("Invoice", "10442")
        assert (bullets.items, bullets.markers, bullets.ordered) == (("first item", "second item"), ("•", "•"), False)

    def test_a_heading_carries_its_tier(self, tmp_path):
        items = (TextRun("Clearings", 72.0, 300.0, font="Helvetica-Bold", size=10.0), *body_text(320.0))

        heading = assembled(letter(tmp_path, *mixed_items())).elements[0]
        bold = assembled(letter(tmp_path, *items, name="bold.pdf")).elements[0]

        assert (heading.text, heading.font_size_pt, heading.is_bold) == ("Results", 18.0, False)
        assert (bold.text, bold.font_size_pt, bold.is_bold) == ("Clearings", 10.0, True)

    def test_an_ordered_list_keeps_its_markers(self, tmp_path):
        items = (TextRun("1. one", 72.0, 100.0, size=10.0), TextRun("2. two", 72.0, 114.0, size=10.0))

        (bullets,) = assembled(letter(tmp_path, *body_text(300.0), *items)).elements[:1]

        assert (bullets.items, bullets.markers, bullets.ordered) == (("one", "two"), ("1.", "2."), True)

    def test_a_ruled_table_is_a_lattice_table_with_its_grid_columns_centres_and_intervals(self, tmp_path):
        tables = [
            element for element in assembled(letter(tmp_path, *mixed_items())).elements if isinstance(element, Table)
        ]

        ruled = tables[0]
        assert (ruled.extraction_method, ruled.headers, ruled.rows) == ("lattice", SPEC[0], SPEC[1:])
        assert ruled.column_centres == pytest.approx((100.0, 200.0, 300.0))
        # Each ruled column's interval is its span between its rules (#420).
        assert [value for pair in ruled.column_intervals for value in pair] == pytest.approx(
            (50.0, 150.0, 150.0, 250.0, 250.0, 350.0)
        )

    def test_a_borderless_table_is_a_stream_table_with_its_columns_alignment_edges_and_extents(self, tmp_path):
        # PARTS is written left-aligned at x 72, 220 and 360 from y 460 (table_runs): each column aligns on its
        # cells' starts, and spans from there to its widest cell's end, as pdfium places that cell's glyphs.
        path = letter(tmp_path, *mixed_items())
        raw, assessment, lattice = inputs(path)
        cells = [piece for piece in assessment.pieces if piece.bbox[1] >= 440.0]

        table = [
            element
            for element in assemble_page(raw, assessment, lattice, source=path).elements
            if isinstance(element, Table)
        ][1]

        assert (table.extraction_method, table.headers, table.rows) == ("stream", PARTS[0], PARTS[1:])
        assert table.column_centres == STREAM_XS
        ends = tuple(max(cell.bbox[2] for cell in cells if cell.bbox[0] == x) for x in STREAM_XS)
        assert table.column_intervals == tuple(zip(STREAM_XS, ends))
        assert all(start < end for start, end in table.column_intervals)  # each column has the width of its cells
        assert [len([cell for cell in cells if cell.bbox[0] == x]) for x in STREAM_XS] == [len(PARTS)] * 3

    @pytest.mark.parametrize("rotate", [90, 270])
    def test_a_turned_table_has_no_column_centres_or_intervals(self, tmp_path, rotate):
        ys = (340.0, 360.0, 380.0, 400.0)
        items = (*lattice_rules(LATTICE_XS, ys), *written(SPEC, LATTICE_XS, ys), *table_runs(PARTS, top=460.0))

        tables = [
            element
            for element in assembled(letter(tmp_path, *items, rotate=rotate)).elements
            if isinstance(element, Table)
        ]

        assert sorted(table.extraction_method for table in tables) == ["lattice", "stream"]
        assert [table.column_centres for table in tables] == [(), ()]
        assert [table.column_intervals for table in tables] == [(), ()]

    def test_a_figure_is_bound_and_never_loaded(self, tmp_path, monkeypatch):
        def refuse(self):
            raise AssertionError("a loader was called")

        monkeypatch.setattr(FigureRead, "png_bytes", refuse)
        monkeypatch.setattr(FigureRead, "raw_bytes", refuse)

        page = assembled(letter(tmp_path, *mixed_items()))

        (figure,) = [element for element in page.elements if isinstance(element, Figure)]
        assert (figure.width_px, figure.height_px) == (120, 80)
        assert figure.bbox == pytest.approx(BBox(x0=72.0, y0=220.0, x1=192.0, y1=300.0))
        with pytest.raises(AssertionError, match="a loader was called"):
            figure.read_bytes()

    def test_figures_follow_the_options(self, tmp_path):
        path = letter(tmp_path, *mixed_items())

        assert "Figure" not in kinds(assembled(path, min_image_dimension=121))
        (figure,) = [element for element in assembled(path, password="secret").elements if isinstance(element, Figure)]
        assert figure._loader.__self__.password == "secret"

    def test_hidden_and_recovered_text_become_hidden_paragraphs(self, tmp_path):
        page = assembled(tiny_text_page(tmp_path))

        hidden = [(element.text, element.hidden_reasons) for element in page.elements if element.is_hidden]
        assert hidden == [("ZERO", ("tiny_font",)), ("CENTI", ("tiny_font",)), ("FOUR", ("tiny_font",))]
        assert all(isinstance(element, Paragraph) for element in page.elements)
        assert page.integrity_verified is True

    def test_a_vision_page_keeps_its_digital_text_and_says_ocr_did_not_read_it(self, tmp_path):
        # A grey box under no text: paint OCR could read, so the short page routes vision (P2-C).
        box = Rect(400.0, 600.0, 60.0, 40.0, fill_rgb=(0.5, 0.5, 0.5), stroke=False)
        page = assembled(letter(tmp_path, TextRun("Too short", 72.0, 100.0), box))

        assert (page.route, page.status, page.integrity_verified) == ("vision", "ocr_unavailable", False)
        assert [element.text for element in page.elements] == ["Too short"]
        assert page.warnings == (
            "route 'vision': the page was not read by OCR; its digital text is kept as its elements",
        )


def short_page(tmp_path) -> Path:
    """Too little text over a grey box: paint OCR could read, so the page is short of text (P2-C)."""
    box = Rect(400.0, 600.0, 60.0, 40.0, fill_rgb=(0.5, 0.5, 0.5), stroke=False)
    return letter(tmp_path, TextRun("Too short", 72.0, 100.0), box)


def assembled_under(path, ocr: str) -> Page:
    """assemble_page of page 1 of ``path``, assessed under ``ocr``."""
    with open_document(path) as handle:
        raw = extract_page(handle, 0)
    assessment = assess_page(raw, ocr=ocr)
    return assemble_page(raw, assessment, find_lattice_tables(raw.segments.segments, assessment.pieces), source=path)


class TestAssemblePageImageOnly:
    """assemble_page carries A3's image-only reasons (#470): its page's content lives only in its page image."""

    @pytest.mark.parametrize(
        "ocr,route,status",
        [
            pytest.param("never", "digital", "ok", id="kept-digital-under-never"),
            pytest.param("auto", "vision", "ocr_unavailable", id="vision-not-read-by-ocr"),
            pytest.param("always", "vision", "ocr_unavailable", id="always-not-read-by-ocr"),
        ],
    )
    @pytest.mark.parametrize(
        "build,reasons",
        [
            pytest.param(short_page, ("too_little_text",), id="too-little-text"),
            pytest.param(garbled_page, ("garbled_text",), id="garbled-text"),
        ],
    )
    def test_a_page_whose_text_cannot_stand_is_marked(self, tmp_path, build, reasons, ocr, route, status):
        page = assembled_under(build(tmp_path), ocr)

        assert (page.route, page.status, page.image_only_reasons) == (route, status, reasons)

    @pytest.mark.parametrize("ocr", ["auto", "never"])
    def test_a_page_whose_text_stands_is_not(self, tmp_path, ocr):
        page = assembled_under(letter(tmp_path, *mixed_items()), ocr)

        assert (page.route, page.status, page.image_only_reasons) == ("digital", "ok", ())


# A render worker's memory cap past its start, and its time: what a PageDraw is given by default.
RENDER_CAP, RENDER_TIMEOUT = 2 << 30, 120.0


def grey_png(data: bytes) -> tuple[str, str, tuple[int, int], bytes]:
    """A PNG's format, mode, size and pixels."""
    with Image.open(io.BytesIO(data)) as picture:
        return picture.format, picture.mode, picture.size, picture.tobytes()


class TestPageDraw:
    """A digital or hybrid page's image loader (P2-A): picklable, holding no pixels, checked at construction."""

    def fields(self, tmp_path, **overrides) -> dict:
        values = {
            "source": tmp_path / "doc.pdf",
            "page_index": 0,
            "dpi": 72,
            "width_pt": LETTER_W,
            "height_pt": LETTER_H,
            "password": "secret",
        }
        return values | overrides

    def test_holds_an_absolute_path_and_its_size_and_hides_the_password(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        draw = _loaders.PageDraw(**self.fields(tmp_path, source=Path("doc.pdf")))

        assert draw.source == tmp_path / "doc.pdf"
        assert draw.size == (612, 792) == render_size(LETTER_W, LETTER_H, 72)
        assert (draw.memory_cap_bytes, draw.timeout_s) == (RENDER_CAP, RENDER_TIMEOUT)
        assert "secret" not in repr(draw)
        assert not hasattr(draw, "__dict__")
        assert pickle.loads(pickle.dumps(draw)) == draw

    @pytest.mark.parametrize(
        ("overrides", "error", "message"),
        [
            ({"source": b"doc.pdf"}, TypeError, "source"),
            ({"page_index": -1}, ValueError, "page_index"),
            ({"dpi": 0}, ValueError, "dpi"),
            ({"dpi": 72.0}, TypeError, "dpi"),
            ({"width_pt": 0.0}, ValueError, "width_pt"),
            ({"height_pt": float("nan")}, ValueError, "height_pt"),
            ({"width_pt": 0.5}, ValueError, "draws 0 px wide at 72 DPI"),
            ({"height_pt": 0.5}, ValueError, "draws 0 px tall at 72 DPI"),
            ({"password": "a\x00b"}, ValueError, "password"),
            ({"memory_cap_bytes": 0}, ValueError, "memory_cap_bytes"),
            ({"timeout_s": -1.0}, ValueError, "timeout_s"),
        ],
    )
    def test_rejects_bad_fields(self, tmp_path, overrides, error, message):
        with pytest.raises(error, match=message):
            _loaders.PageDraw(**self.fields(tmp_path, **overrides))

    def test_each_read_draws_the_page_again_in_the_render_worker(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *mixed_items())
        calls = []
        real = _loaders.render_page

        def counted(*args, **kwargs):
            calls.append((args, kwargs))
            return real(*args, **kwargs)

        monkeypatch.setattr(_loaders, "render_page", counted)
        draw = _loaders.PageDraw(source=path, page_index=0, dpi=72, width_pt=LETTER_W, height_pt=LETTER_H)

        first, second = draw.png_bytes(), draw.png_bytes()

        drawn = render_page(path, 0, dpi=72, memory_cap_bytes=RENDER_CAP, timeout_s=RENDER_TIMEOUT)
        assert first == second
        assert grey_png(first) == ("PNG", "L", (612, 792), drawn.pixels)  # as drawn: no contrast normalization
        options = {"dpi": 72, "password": None, "memory_cap_bytes": RENDER_CAP, "timeout_s": RENDER_TIMEOUT}
        assert calls == [((path, 0), options)] * 2

    def test_a_read_passes_its_password_cap_and_timeout(self, tmp_path, monkeypatch):
        calls = []

        def recorded(*args, **kwargs):
            calls.append(kwargs)
            raise RenderRefused("stop here")

        monkeypatch.setattr(_loaders, "render_page", recorded)
        draw = _loaders.PageDraw(**self.fields(tmp_path, memory_cap_bytes=1 << 20, timeout_s=5.0))

        with pytest.raises(RenderRefused, match="stop here"):
            draw.png_bytes()

        assert calls == [{"dpi": 72, "password": "secret", "memory_cap_bytes": 1 << 20, "timeout_s": 5.0}]

    @pytest.mark.parametrize(("width", "height"), [(600.0, LETTER_H), (LETTER_W, 800.0)])
    def test_a_draw_of_another_size_than_its_page_is_refused(self, tmp_path, width, height):
        path = letter(tmp_path, *mixed_items())
        draw = _loaders.PageDraw(source=path, page_index=0, dpi=72, width_pt=width, height_pt=height)
        expected = f"{int(width)} x {int(height)} px"

        with pytest.raises(DatagruntPDFError, match=f"page 1 draws 612 x 792 px, not the {expected} of its page"):
            draw.png_bytes()


# Why a page image of a file that is not its document's is refused: the end of each PDFSourceChangedError (#453).
NOT_RETURNED = "the page image and the document's pages could disagree, so it is not returned"
# A source identity that names no file: a binding's fields checked at construction.
SOME_IDENTITY = SourceIdentity(device=1, inode=2, size=3, mtime_ns=4)


def identity_of(path: Path) -> SourceIdentity:
    """The identity of the file at ``path`` now."""
    return SourceIdentity.of(os.stat(path))


def sha256_of(path: Path) -> str:
    """The SHA-256 of the bytes of the file at ``path`` now."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def binding(path: Path) -> dict:
    """The file at ``path`` now, as a page image's loaders are bound to it: its identity and its bytes' sha256."""
    return {"identity": identity_of(path), "sha256": sha256_of(path)}


def held_identity(path: Path, field: str) -> tuple[SourceIdentity, str]:
    """An identity of ``path`` whose ``field`` (``device``, ``inode``, ``size``, ``mtime_ns``) is one off; its change.

    A ``device`` one off is the file read on another host (``st_dev`` is the host's own): its change is the inode's.
    """
    now = identity_of(path)
    held = dataclasses.replace(now, **{field: getattr(now, field) + 1})
    inode = f"inode {held.device}:{held.inode} -> {now.device}:{now.inode}"
    words = {
        "device": inode,
        "inode": inode,
        "size": f"size {held.size} -> {now.size} bytes",
        "mtime_ns": f"mtime {held.mtime_ns} -> {now.mtime_ns} ns",
    }
    return held, words[field]


def not_its_bytes(path: Path, when: str, change: str, expected: str) -> str:
    """The PDFSourceChangedError of a draw ``when`` of the file at ``path``: ``change``, its bytes not ``expected``."""
    found = f"its bytes are not the document's (sha256 {sha256_of(path)}, not {expected})"
    return f"{path} changed {when}: {change}; {found}; {NOT_RETURNED}"


def touched(path: Path) -> None:
    """Move ``path``'s mtime on a second, whatever the clock's grain: its bytes and inode are as they were."""
    status = os.stat(path)
    os.utime(path, ns=(status.st_atime_ns, status.st_mtime_ns + 1_000_000_000))


def rewritten_with_its_own_bytes(path: Path) -> None:
    """Put another file of ``path``'s own bytes and mtime at ``path`` (a checkout, a sync): only its inode changes."""
    copy = path.with_name(f"{path.stem}-copy{path.suffix}")
    shutil.copy2(path, copy)
    os.replace(copy, path)


def rewritten_with_other_bytes(path: Path) -> None:
    """Rewrite ``path`` in place with another last byte, then move its mtime on: its size and inode are as they were."""
    data = path.read_bytes()
    path.write_bytes(data[:-1] + (b"\n" if data.endswith(b" ") else b" "))
    touched(path)


def no_draw(*args, **kwargs):
    raise AssertionError("the page was drawn")


def hashes(monkeypatch) -> list[Path]:
    """Record each path the loaders hash (through ``_stream_sha256``), and let it hash."""
    hashed: list[Path] = []
    real = _loaders._stream_sha256

    def recorded(stream, size, path):
        hashed.append(path)
        return real(stream, size, path)

    monkeypatch.setattr(_loaders, "_stream_sha256", recorded)
    return hashed


def after_the_draw(monkeypatch, change) -> None:
    """Run ``change()`` once the render worker has drawn the page, before the draw returns."""
    real = _loaders.render_page

    def changing(*args, **kwargs):
        drawn = real(*args, **kwargs)
        change()
        return drawn

    monkeypatch.setattr(_loaders, "render_page", changing)


def appended(path: Path) -> None:
    """Append 12 bytes to ``path``, in place."""
    with path.open("ab") as stream:
        stream.write(b"\n% appended\n")


def drawn_at_72(path: Path, page_index: int = 0) -> bytes:
    """The pixels of page ``page_index`` of ``path`` drawn at 72 DPI, as a PageDraw there draws them."""
    return render_page(path, page_index, dpi=72, memory_cap_bytes=RENDER_CAP, timeout_s=RENDER_TIMEOUT).pixels


class TestPageDrawSource:
    """A PageDraw bound to its document's file draws only the document's bytes: identity first, then content (#453)."""

    def draw(self, path: Path, page_index: int = 0, **bound) -> _loaders.PageDraw:
        return _loaders.PageDraw(
            source=path, page_index=page_index, dpi=72, width_pt=LETTER_W, height_pt=LETTER_H, **bound
        )

    def test_holds_its_identity_and_sha256_through_a_pickle_and_hides_the_password(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))

        draw = _loaders.PageDraw(**TestPageDraw().fields(tmp_path, source=path), **binding(path))
        copy = pickle.loads(pickle.dumps(draw))

        assert copy == draw
        assert (copy.identity, copy.sha256, copy.password) == (identity_of(path), sha256_of(path), "secret")
        assert "secret" not in repr(copy)
        unbound = _loaders.PageDraw(**TestPageDraw().fields(tmp_path))
        assert (unbound.identity, unbound.sha256) == (None, None)

    @pytest.mark.parametrize(
        ("bound", "error", "message"),
        [
            (
                {"identity": (1, 2, 3, 4), "sha256": SHA},
                TypeError,
                "identity must be a SourceIdentity or None, got tuple",
            ),
            ({"identity": SOME_IDENTITY, "sha256": 7}, TypeError, "sha256 must be a str, got 7 (int)"),
            ({"identity": SOME_IDENTITY, "sha256": "F" * 64}, ValueError, "sha256 must be 64 lowercase hex characters"),
            (
                {"identity": SOME_IDENTITY},
                ValueError,
                "identity and sha256 must both be given or both be None, got only the identity",
            ),
            (
                {"sha256": SHA},
                ValueError,
                "identity and sha256 must both be given or both be None, got only the sha256",
            ),
        ],
    )
    def test_rejects_a_binding_that_is_not_an_identity_and_its_sha256(self, tmp_path, bound, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}"):
            self.draw(tmp_path / "doc.pdf", **bound)

    def test_the_file_of_its_identity_draws_without_being_hashed(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        hashed = hashes(monkeypatch)

        data = self.draw(path, **binding(path)).png_bytes()

        assert grey_png(data) == ("PNG", "L", (612, 792), drawn_at_72(path))
        assert hashed == []

    @pytest.mark.parametrize("field", ["device", "inode", "size", "mtime_ns"])
    def test_a_file_of_another_identity_holding_the_documents_bytes_draws(self, tmp_path, monkeypatch, field):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(items=tuple(body_text(100.0)))] * 2)
        held, _ = held_identity(path, field)
        hashed = hashes(monkeypatch)

        data = self.draw(path, page_index=1, identity=held, sha256=sha256_of(path)).png_bytes()

        assert grey_png(data) == ("PNG", "L", (612, 792), drawn_at_72(path, 1))
        assert hashed == [path]  # the content decided, once

    @pytest.mark.parametrize("change", [touched, rewritten_with_its_own_bytes], ids=["touched", "own-bytes"])
    def test_its_own_bytes_at_its_path_draw_whatever_the_identity(self, tmp_path, change):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        change(path)

        data = self.draw(path, **bound).png_bytes()

        assert identity_of(path) != bound["identity"]
        assert grey_png(data) == ("PNG", "L", (612, 792), drawn_at_72(path))

    def test_other_bytes_of_its_size_are_refused_before_they_are_drawn(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(items=tuple(body_text(100.0)))] * 2)
        bound = binding(path)
        held = bound["identity"]
        rewritten_with_other_bytes(path)
        now = identity_of(path)
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, page_index=1, **bound).png_bytes()

        assert (now.inode, now.size) == (held.inode, held.size)
        change = f"mtime {held.mtime_ns} -> {now.mtime_ns} ns"
        assert str(caught.value) == not_its_bytes(path, "before page 2's image was drawn", change, bound["sha256"])

    @pytest.mark.parametrize("field", ["device", "inode", "size", "mtime_ns"])
    def test_a_file_of_another_identity_and_other_bytes_is_refused_naming_both(self, tmp_path, monkeypatch, field):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(items=tuple(body_text(100.0)))] * 2)
        held, change = held_identity(path, field)
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, page_index=1, identity=held, sha256=SHA).png_bytes()

        assert str(caught.value) == not_its_bytes(path, "before page 2's image was drawn", change, SHA)

    def test_a_file_changed_while_it_is_drawn_is_refused(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        held = bound["identity"]
        after_the_draw(monkeypatch, lambda: appended(path))

        changed = f"{path} changed while page 1's image was drawn: size {held.size} -> {held.size + 12} bytes"
        with pytest.raises(PDFSourceChangedError, match=f"^{re.escape(changed)}"):
            self.draw(path, **bound).png_bytes()

    def test_its_own_bytes_swapped_in_while_it_is_drawn_are_refused(self, tmp_path, monkeypatch):
        # After the render nothing is hashed: the file drawn must be the file stat'ed before it.
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        held = bound["identity"]
        after_the_draw(monkeypatch, lambda: rewritten_with_its_own_bytes(path))

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        now = identity_of(path)
        change = f"inode {held.device}:{held.inode} -> {now.device}:{now.inode}"
        assert str(caught.value) == f"{path} changed while page 1's image was drawn: {change}; {NOT_RETURNED}"

    def test_a_touched_file_changed_while_it_is_drawn_is_refused(self, tmp_path, monkeypatch):
        # The touch is the content's to decide, before the render; the change during it is refused after it.
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        touched(path)
        size = identity_of(path).size
        after_the_draw(monkeypatch, lambda: appended(path))

        changed = f"{path} changed while page 1's image was drawn: size {size} -> {size + 12} bytes"
        with pytest.raises(PDFSourceChangedError, match=f"^{re.escape(changed)}"):
            self.draw(path, **bound).png_bytes()

    def test_a_file_replaced_between_its_stat_and_its_hash_is_refused(self, tmp_path, monkeypatch):
        # The file hashed must be the file stat'ed (its fstat): even its own bytes, swapped in between, are refused.
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        touched(path)
        stated = identity_of(path)
        real = _loaders._opened_source

        def swapped_then_opened(source):
            rewritten_with_its_own_bytes(source)
            return real(source)

        monkeypatch.setattr(_loaders, "_opened_source", swapped_then_opened)
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        now = identity_of(path)
        change = f"inode {stated.device}:{stated.inode} -> {now.device}:{now.inode}"
        assert str(caught.value) == f"{path} changed before page 1's image was drawn: {change}; {NOT_RETURNED}"

    def test_a_file_removed_between_its_stat_and_its_hash_is_refused(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        touched(path)
        real = _loaders._opened_source

        def removed_then_opened(source):
            source.unlink()
            return real(source)

        monkeypatch.setattr(_loaders, "_opened_source", removed_then_opened)
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        assert str(caught.value) == f"{path} was removed before page 1's image was drawn; {NOT_RETURNED}"

    @pytest.mark.skipif(sys.platform == "win32", reason="a directory does not open for reading on Windows")
    def test_a_directory_put_there_between_its_stat_and_its_hash_is_refused_saying_so(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        touched(path)
        real = _loaders._opened_source

        def replaced_then_opened(source):
            source.unlink()
            source.mkdir()
            return real(source)

        monkeypatch.setattr(_loaders, "_opened_source", replaced_then_opened)
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        changed = f"{path} changed before page 1's image was drawn: it is not a regular file"
        assert str(caught.value) == f"{changed}; {NOT_RETURNED}"

    def test_a_file_cut_short_while_it_is_hashed_is_refused(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        touched(path)
        monkeypatch.setattr(_source, "_read_chunk", lambda stream, size: b"")
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        short = f"{path} changed while it was hashed: 0 of its {identity_of(path).size} bytes could be read"
        assert str(caught.value) == f"{short} before page 1's image was drawn; {NOT_RETURNED}"

    def test_a_file_removed_before_it_is_drawn_is_refused(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        path.unlink()
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        assert str(caught.value) == f"{path} was removed before page 1's image was drawn; {NOT_RETURNED}"

    def test_a_file_removed_while_it_is_drawn_is_refused(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)
        after_the_draw(monkeypatch, path.unlink)

        with pytest.raises(PDFSourceChangedError) as caught:
            self.draw(path, **bound).png_bytes()

        assert str(caught.value) == f"{path} was removed while page 1's image was drawn; {NOT_RETURNED}"

    @pytest.mark.parametrize("kind", ["directory", "named pipe"])
    @pytest.mark.parametrize("when", ["before", "while"])
    def test_a_path_that_is_not_a_regular_file_is_refused_saying_so(self, tmp_path, monkeypatch, when, kind):
        if kind == "named pipe" and sys.platform == "win32":
            pytest.skip("no named pipes as files on Windows")
        path = letter(tmp_path, *body_text(100.0))
        bound = binding(path)

        def replaced() -> None:
            path.unlink()
            if kind == "directory":
                path.mkdir()
            else:
                os.mkfifo(path)

        if when == "before":
            replaced()
            monkeypatch.setattr(_loaders, "render_page", no_draw)
        else:
            after_the_draw(monkeypatch, replaced)

        raised = promptly(lambda: self.draw(path, **bound).png_bytes(), path)

        assert isinstance(raised, PDFSourceChangedError)
        changed = f"{path} changed {when} page 1's image was drawn: it is not a regular file"
        assert str(raised) == f"{changed}; {NOT_RETURNED}"

    def test_without_a_binding_the_file_at_its_path_is_drawn_as_before(self, tmp_path, monkeypatch):
        path = letter(tmp_path, *body_text(100.0))
        draw = self.draw(path)
        os.replace(letter(tmp_path, *mixed_items(), name="other.pdf"), path)
        hashed = hashes(monkeypatch)

        data = draw.png_bytes()

        assert grey_png(data)[3] == drawn_at_72(path)
        assert hashed == []


class TestPageImage:
    """assemble_page gives the page an image drawn when read: 8-bit grey at the effective DPI (P2-A, spec §3.2)."""

    def test_the_page_image_is_bound_and_never_drawn(self, tmp_path, monkeypatch):
        def refuse(*args, **kwargs):
            raise AssertionError("a page was drawn")

        monkeypatch.setattr(_loaders, "render_page", refuse)
        path = letter(tmp_path, *mixed_items())

        image = assembled(path).page_image

        assert image == PageImage(page_number=1, dpi=200.0, width_px=1700, height_px=2200)
        assert (image.contrast_normalized, image.image_path, image.band_paths, image._band_loader) == (
            False,
            None,
            (),
            None,
        )
        draw = _loaders.PageDraw(source=path, page_index=0, dpi=200, width_pt=LETTER_W, height_pt=LETTER_H)
        assert (image._loader.__self__, image._loader.__func__) == (draw, _loaders.PageDraw.png_bytes)
        with pytest.raises(AssertionError, match="a page was drawn"):
            image.read_bytes()

    def test_its_bytes_are_the_page_drawn_at_its_dpi(self, tmp_path):
        path = letter(tmp_path, *mixed_items())

        image = assembled(path).page_image

        drawn = render_page(path, 0, dpi=200, memory_cap_bytes=RENDER_CAP, timeout_s=RENDER_TIMEOUT)
        assert grey_png(image.read_bytes()) == ("PNG", "L", (1700, 2200), drawn.pixels)
        with pytest.raises(DatagruntPDFError, match="has no bands"):
            image.read_bands()

    def test_dpi_and_the_pixel_bound_are_options(self, tmp_path):
        path = letter(tmp_path, *mixed_items())
        dpi = effective_dpi(100, 262_144, LETTER_W, LETTER_H)

        image = assembled(path, dpi=100, max_render_pixels=262_144).page_image

        assert dpi < 100  # the case: the pixel bound lowers the dpi
        assert (image.dpi, image.width_px, image.height_px) == (float(dpi), *render_size(LETTER_W, LETTER_H, dpi))
        assert image._loader.__self__.dpi == dpi

    def test_the_draw_holds_the_page_and_the_password(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(items=tuple(body_text(100.0)))] * 2)

        image = assembled(path, 1, password="secret").page_image

        draw = image._loader.__self__
        assert (image.page_number, draw.page_index, draw.password) == (2, 1, "secret")

    def test_the_draw_holds_the_source_identity_and_sha256_it_is_given(self, tmp_path):
        path = letter(tmp_path, *mixed_items())

        bound, unbound = assembled(path, **binding(path)), assembled(path)

        draw, bare = bound.page_image._loader.__self__, unbound.page_image._loader.__self__
        assert (draw.identity, draw.sha256) == (identity_of(path), sha256_of(path))
        assert (bare.identity, bare.sha256) == (None, None)

    def test_a_turned_page_is_drawn_as_displayed(self, tmp_path):
        image = assembled(letter(tmp_path, *mixed_items(), rotate=90)).page_image

        assert (image.width_px, image.height_px) == (2200, 1700)
        assert grey_png(image.read_bytes())[2] == (2200, 1700)

    def test_a_page_that_cannot_be_drawn_has_no_image_and_says_why(self, tmp_path, monkeypatch):
        def refused(*args):
            raise RenderRefused("a 612.0 x 792.0 pt page renders 0 px tall at 200 DPI")

        monkeypatch.setattr(assembly, "effective_dpi", refused)

        page = assembled(letter(tmp_path, *mixed_items()))

        assert page.page_image is None
        assert page.warnings[-1] == ("the page has no page image: a 612.0 x 792.0 pt page renders 0 px tall at 200 DPI")
        assert kinds(page) == ["Heading", "Paragraph", "KeyValue", "BulletList", "Figure", "Table", "Table"]

    @pytest.mark.parametrize(
        ("option", "bad", "error", "message"),
        [
            ("dpi", 35, ValueError, "dpi must be >= 36, got 35"),
            ("dpi", True, TypeError, "dpi must be an int"),
            ("max_render_pixels", 262_143, ValueError, "max_render_pixels must be >= 262144, got 262143"),
            ("max_render_pixels", 1.5e7, TypeError, "max_render_pixels must be an int"),
        ],
    )
    def test_bad_render_options_are_refused_before_any_work(self, tmp_path, monkeypatch, option, bad, error, message):
        inputs_of_page = inputs(letter(tmp_path, *mixed_items()))

        def no_work(*args, **kwargs):
            raise AssertionError("the page was worked on")

        monkeypatch.setattr(assembly, "page_figures", no_work)

        with pytest.raises(error, match=f"^{message}"):
            assemble_page(*inputs_of_page, source=tmp_path / "doc.pdf", **{option: bad})


def two_pages(path: Path, words: str, top: float) -> Path:
    """Two letter pages, each of one line of ``words`` at ``top``."""
    return build_pdf(path, [PageSpec(items=(TextRun(words, 72.0, top, size=24.0),))] * 2)


def reader_document(path: Path):
    return PDFReader(path).document


def writer_document(path: Path):
    return PDFWriter(path)._extracted()


class TestDrawnPageImageSource:
    """A document read from a PDF path draws its pages' images only while the file there holds its bytes (#453)."""

    @pytest.mark.parametrize("extracted", [reader_document, writer_document])
    def test_every_page_image_holds_the_identity_and_sha256_of_the_file_read(self, tmp_path, extracted):
        path = two_pages(tmp_path / "doc.pdf", "Alpha document text", 100.0)

        document = extracted(path)

        draws = [page.page_image._loader.__self__ for page in document.pages]
        assert [type(draw) for draw in draws] == [_loaders.PageDraw] * 2
        assert document.metadata.sha256 == sha256_of(path)
        assert [(draw.identity, draw.sha256) for draw in draws] == [(identity_of(path), sha256_of(path))] * 2

    def test_a_page_image_of_the_file_read_draws(self, tmp_path):
        path = two_pages(tmp_path / "doc.pdf", "Alpha document text", 100.0)
        document = PDFReader(path).document

        data = document.pages[1].page_image.read_bytes()

        drawn = render_page(path, 1, dpi=200, memory_cap_bytes=RENDER_CAP, timeout_s=RENDER_TIMEOUT)
        assert grey_png(data) == ("PNG", "L", (1700, 2200), drawn.pixels)

    @pytest.mark.parametrize("extracted", [reader_document, writer_document])
    @pytest.mark.parametrize("change", [touched, rewritten_with_its_own_bytes], ids=["touched", "own-bytes"])
    def test_a_page_image_of_the_documents_bytes_draws_whatever_the_identity(self, tmp_path, extracted, change):
        path = two_pages(tmp_path / "doc.pdf", "Alpha document text", 100.0)
        document = extracted(path)
        held = identity_of(path)

        change(path)

        data = document.pages[1].page_image.read_bytes()
        drawn = render_page(path, 1, dpi=200, memory_cap_bytes=RENDER_CAP, timeout_s=RENDER_TIMEOUT)
        assert identity_of(path) != held
        assert grey_png(data) == ("PNG", "L", (1700, 2200), drawn.pixels)

    @pytest.mark.parametrize("extracted", [reader_document, writer_document])
    def test_a_page_image_of_a_file_replaced_since_is_refused(self, tmp_path, extracted):
        path = two_pages(tmp_path / "doc.pdf", "Alpha document text", 100.0)
        other = two_pages(tmp_path / "other.pdf", "Bravo other words entirely", 400.0)
        document = extracted(path)
        held = identity_of(path)

        os.replace(other, path)

        now = identity_of(path)
        replaced = f"{path} changed before page 2's image was drawn: inode {held.device}:{held.inode} -> "
        with pytest.raises(PDFSourceChangedError, match=f"^{re.escape(replaced)}{now.device}:{now.inode}") as caught:
            document.pages[1].page_image.read_bytes()
        mismatch = f"its bytes are not the document's (sha256 {sha256_of(path)}, not {document.metadata.sha256})"
        assert str(caught.value).endswith(f"; {mismatch}; {NOT_RETURNED}")
        assert [element.text for element in document.pages[1].elements] == ["Alpha document text"]

    @pytest.mark.parametrize("extracted", [reader_document, writer_document])
    def test_a_page_image_of_other_bytes_of_its_size_is_refused(self, tmp_path, extracted, monkeypatch):
        path = two_pages(tmp_path / "doc.pdf", "Alpha document text", 100.0)
        document = extracted(path)
        held = identity_of(path)
        rewritten_with_other_bytes(path)
        monkeypatch.setattr(_loaders, "render_page", no_draw)

        with pytest.raises(PDFSourceChangedError) as caught:
            document.pages[0].page_image.read_bytes()

        change = f"mtime {held.mtime_ns} -> {identity_of(path).mtime_ns} ns"
        expected = not_its_bytes(path, "before page 1's image was drawn", change, document.metadata.sha256)
        assert str(caught.value) == expected


class TestFormHeaders:
    """A form header of Key: Value cells in two columns reads as key-values, so A6 takes it as header context."""

    def test_a_two_column_key_value_header_is_header_context(self, tmp_path):
        header = (
            TextRun("Invoice: 10442", 72.0, 100.0, size=10.0),
            TextRun("Date: 2024-03-01", 300.0, 100.0, size=10.0),
            TextRun("Terms: 30 days", 72.0, 114.0, size=10.0),
            TextRun("Due: 2024-03-31", 300.0, 114.0, size=10.0),
        )
        rows = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))

        page = assembled(letter(tmp_path, *header, *body_text(150.0), *table_runs(rows, top=200.0)))

        pairs = [("Invoice", "10442"), ("Date", "2024-03-01"), ("Terms", "30 days"), ("Due", "2024-03-31")]
        assert kinds(page) == ["KeyValue"] * 4 + ["Paragraph", "Table"]
        assert [(pair.key, pair.value) for pair in apply_scope((page,)).header_context] == pairs
        assert page.integrity_verified is True


class TestAssemblePageWarnings:
    """Warnings are carried from A2, A3 and A4a, then the page's own."""

    def test_a2s_counts_are_carried(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, lattice = inputs(path)
        text = dataclasses.replace(raw.text, stats=dataclasses.replace(raw.text.stats, skipped_nonfinite_objects=2))
        segments = dataclasses.replace(
            raw.segments, stats=dataclasses.replace(raw.segments.stats, skipped_nonfinite_paths=3)
        )
        images = dataclasses.replace(
            raw.images, stats=dataclasses.replace(raw.images.stats, skipped_nonfinite_images=1)
        )
        raw = dataclasses.replace(raw, text=text, segments=segments, images=images)

        page = assemble_page(raw, assessment, lattice, source=path)

        assert page.warnings == (
            "2 text object(s) skipped: pdfium places them at a non-finite position or size; "
            "their text is missing from this page",
            "3 path object(s) skipped: their geometry is not finite; their rules are missing from this page",
            "1 image object(s) skipped: their placement on the page is not finite",
        )

    def test_a3s_warnings_are_carried(self, tmp_path):
        path = tiny_text_page(tmp_path)
        handle = open_document(path)
        raw = extract_page(handle, 0)
        assessment = assess_page(raw)  # no recovery: A3 warns of the dropped object

        page = assemble_page(
            raw, assessment, find_lattice_tables(raw.segments.segments, assessment.pieces), source=path
        )

        assert page.warnings == assessment.warnings != ()

    @pytest.mark.parametrize(
        "lattice,expected",
        [
            pytest.param(
                LatticeResult(tables=(), used_pieces=(), skipped_grids=2),
                "2 ruled grid(s) left out of the lattice pass (over its per-page bounds): "
                "their text reads as stream text",
                id="skipped-grids",
            ),
            pytest.param(
                LatticeResult(tables=(), used_pieces=(), crossing_cap_reached=True),
                f"the page's rules cross more than {MAX_CROSSINGS} times: no lattice tables were read; "
                "its text reads as stream text",
                id="crossing-cap",
            ),
        ],
    )
    def test_a4as_bounds_are_carried(self, tmp_path, lattice, expected):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, _ = inputs(path)

        assert assemble_page(raw, assessment, lattice, source=path).warnings == (expected,)

    def test_figure_and_layout_warnings_are_carried(self, tmp_path, monkeypatch):
        monkeypatch.setattr(layout, "MAX_APART_PAIRS", 0)
        path = tiny_text_page(tmp_path)

        page = assembled(path)

        assert page.warnings == (
            "3 hidden or recovered line(s) read after the page's visible text: placing them by position "
            "would compare 3 pairs, over the bound of 0",
        )


class TestPassC:
    """Pass C: the multiset of the page's non-whitespace chars equals its elements'; a mismatch warns, never raises."""

    def test_every_element_kind_counts_its_chars(self, tmp_path):
        path = letter(tmp_path, *mixed_items())
        raw, assessment, lattice = inputs(path)
        page = assemble_page(raw, assessment, lattice, source=path)

        assert pass_c(assessment, page.elements) == (True, ())

    def test_dropping_an_element_fails_with_the_counts(self, tmp_path):
        path = letter(tmp_path, *mixed_items())
        raw, assessment, lattice = inputs(path)
        elements = assemble_page(raw, assessment, lattice, source=path).elements
        total = sum(1 for piece in assessment.pieces for char in piece.text if not char.isspace())

        verified, warnings = pass_c(assessment, elements[1:])

        assert verified is False
        assert warnings == (
            f"Pass C: the page's text holds {total} non-whitespace chars and its elements {total - 7}: "
            "7 missing, 0 extra",
        )

    def test_an_extra_char_fails(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, lattice = inputs(path)
        (paragraph,) = assemble_page(raw, assessment, lattice, source=path).elements
        total = sum(1 for piece in assessment.pieces for char in piece.text if not char.isspace())

        verified, warnings = pass_c(assessment, (dataclasses.replace(paragraph, text=paragraph.text + "!"),))

        assert (verified, warnings) == (
            False,
            (
                f"Pass C: the page's text holds {total} non-whitespace chars and its elements {total + 1}: "
                "0 missing, 1 extra",
            ),
        )

    def test_whitespace_never_counts(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, lattice = inputs(path)
        (paragraph,) = assemble_page(raw, assessment, lattice, source=path).elements

        spaced = dataclasses.replace(paragraph, text=" \t".join(paragraph.text) + "\n")

        assert pass_c(assessment, (spaced,)) == (True, ())

    def test_recovered_chars_count(self, tmp_path):
        path = tiny_text_page(tmp_path)
        raw, assessment, lattice = inputs(path)
        elements = assemble_page(raw, assessment, lattice, source=path).elements

        assert pass_c(assessment, elements) == (True, ())
        assert pass_c(dataclasses.replace(assessment, recovered_pieces=()), elements)[0] is False

    def test_a_page_dropping_an_element_is_not_verified(self, tmp_path, monkeypatch):
        real = assembly.page_layout

        def lose_the_first(*args, **kwargs):
            found = real(*args, **kwargs)
            return dataclasses.replace(found, items=found.items[1:])

        monkeypatch.setattr(assembly, "page_layout", lose_the_first)

        page = assembled(letter(tmp_path, *mixed_items()))

        assert page.integrity_verified is False
        assert page.warnings[-1].startswith("Pass C: the page's text holds ")
        assert page.warnings[-1].endswith(": 7 missing, 0 extra")

    def test_a_vision_page_is_never_verified_but_still_checked(self, tmp_path, monkeypatch):
        real = assembly.page_layout

        def lose_the_first(*args, **kwargs):
            found = real(*args, **kwargs)
            return dataclasses.replace(found, items=found.items[1:])

        monkeypatch.setattr(assembly, "page_layout", lose_the_first)

        page = assembled(letter(tmp_path, TextRun("Too short", 72.0, 100.0)))

        assert page.integrity_verified is False
        assert page.warnings[-1].endswith(": 8 missing, 0 extra")


class TestPageElements:
    """page_elements converts layout items to A1 elements (assemble_page's step 3, shared with A5b's vision route)."""

    def test_is_assemble_pages_conversion(self, tmp_path):
        path = letter(tmp_path, *mixed_items())
        raw, assessment, lattice = inputs(path)
        figures = page_figures(raw.images, raw.geometry, source=path)
        found = layout.page_layout(
            assessment.pieces,
            assessment.recovered_pieces,
            lattice,
            width_pt=raw.geometry.width_pt,
            height_pt=raw.geometry.height_pt,
            figures=tuple(draft.bbox for draft in figures.drafts),
        )

        elements = page_elements(found.items, figures.drafts, raw.geometry)

        assert elements == assemble_page(raw, assessment, lattice, source=path).elements
        assert (found.warnings, figures.warnings) == ((), ())
        assert [(type(element).__name__, element.element_id, element.reading_order) for element in elements] == [
            ("Heading", "p0001_e000", 0),
            ("Paragraph", "p0001_e001", 1),
            ("KeyValue", "p0001_e002", 2),
            ("BulletList", "p0001_e003", 3),
            ("Figure", "p0001_e004", 4),
            ("Table", "p0001_e005", 5),
            ("Table", "p0001_e006", 6),
        ]
        heading, paragraph, key_value, bullets, figure, ruled, borderless = elements
        assert (heading.text, heading.level) == ("Results", 1)
        assert (
            paragraph.text
            == "The woodland is vast and its creatures many, each with its own purpose and its own ambitions."
        )
        assert (key_value.key, key_value.value) == ("Invoice", "10442")
        assert bullets.items == ("first item", "second item")
        assert (figure.width_px, figure.height_px) == (120, 80)
        assert (ruled.extraction_method, ruled.headers, ruled.rows) == ("lattice", SPEC[0], SPEC[1:])
        assert (borderless.extraction_method, borderless.headers, borderless.rows) == ("stream", PARTS[0], PARTS[1:])
        assert not any(element.is_hidden for element in elements)

    def test_rejects_wrong_arguments(self):
        slot = layout.FigureSlot(index=0, bbox=BBox(x0=60.0, y0=40.0, x1=180.0, y1=120.0))

        with pytest.raises(TypeError, match="items"):
            page_elements([], (), GEOMETRY)
        with pytest.raises(TypeError, match=r"items\[0\]"):
            page_elements(("text",), (), GEOMETRY)
        with pytest.raises(TypeError, match="drafts"):
            page_elements((), (None,), GEOMETRY)
        with pytest.raises(TypeError, match="geometry"):
            page_elements((), (), None)
        with pytest.raises(ValueError, match="items\\[0\\] is figure 0, but there are 0 figure drafts"):
            page_elements((slot,), (), GEOMETRY)


class TestAssemblePageArguments:
    """assemble_page checks its arguments at its boundary."""

    def test_rejects_wrong_types(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, lattice = inputs(path)

        with pytest.raises(TypeError, match="raw_page"):
            assemble_page(None, assessment, lattice, source=path)
        with pytest.raises(TypeError, match="assessment"):
            assemble_page(raw, None, lattice, source=path)
        with pytest.raises(TypeError, match="lattice"):
            assemble_page(raw, assessment, None, source=path)

    def test_check_assessment_is_its_check_of_the_page_and_assessment(self, tmp_path):
        raw, assessment, _ = inputs(letter(tmp_path, *body_text(100.0)))
        other = inputs(letter(tmp_path, TextRun("Another page entirely", 72.0, 100.0), name="other.pdf"))[1]

        assert check_assessment(raw, assessment) is None
        with pytest.raises(TypeError, match="raw_page"):
            check_assessment(None, assessment)
        with pytest.raises(TypeError, match="assessment"):
            check_assessment(raw, None)
        with pytest.raises(ValueError, match="another page"):
            check_assessment(raw, other)

    def test_rejects_another_pages_assessment(self, tmp_path):
        first = letter(tmp_path, *body_text(100.0), name="first.pdf")
        second = letter(tmp_path, TextRun("A different page of other text", 72.0, 100.0), name="second.pdf")
        raw, _, lattice = inputs(first)
        _, other, _ = inputs(second)

        with pytest.raises(ValueError, match="assessment"):
            assemble_page(raw, other, lattice, source=first)

    def test_rejects_an_assessment_of_another_rotation(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, lattice = inputs(path)
        turned = dataclasses.replace(assessment, quality=dataclasses.replace(assessment.quality, rotation_deg=90))

        with pytest.raises(ValueError, match="turned 90, not 0"):
            assemble_page(raw, turned, lattice, source=path)

    def test_pass_c_checks_its_arguments(self, tmp_path):
        raw, assessment, _ = inputs(letter(tmp_path, *body_text(100.0)))

        with pytest.raises(TypeError, match="assessment"):
            pass_c(raw, ())
        with pytest.raises(TypeError, match=r"elements\[0\]"):
            pass_c(assessment, ("text",))
        with pytest.raises(TypeError, match="elements"):
            pass_c(assessment, [])

    def test_pass_c_is_the_conservation_of_every_piece(self, tmp_path):
        # A5b's vision route checks its own pieces (OCR words and the digital text kept apart) with the same rule.
        path = tiny_text_page(tmp_path)
        raw, assessment, lattice = inputs(path)
        elements = assemble_page(raw, assessment, lattice, source=path).elements
        every = (*assessment.pieces, *assessment.recovered_pieces)

        assert conservation(every, elements) == pass_c(assessment, elements) == (True, ())
        assert conservation(assessment.pieces, elements) == pass_c(
            dataclasses.replace(assessment, recovered_pieces=()), elements
        )

    def test_conservation_checks_its_arguments(self, tmp_path):
        raw, assessment, _ = inputs(letter(tmp_path, *body_text(100.0)))

        with pytest.raises(TypeError, match="pieces"):
            conservation(list(assessment.pieces), ())
        with pytest.raises(TypeError, match=r"pieces\[0\]"):
            conservation(("text",), ())
        with pytest.raises(TypeError, match=r"elements\[0\]"):
            conservation(assessment.pieces, ("text",))

    def test_a_ruled_table_reaching_past_the_page_is_clipped_to_it(self, tmp_path):
        path = letter(tmp_path, *body_text(100.0))
        raw, assessment, _ = inputs(path)
        reaching = LatticeTable(
            bbox=BBox(x0=-20.0, y0=700.0, x1=300.0, y1=800.0),
            n_rows=1,
            n_cols=2,
            cells=(
                LatticeCell(
                    row=0, col=0, row_span=1, col_span=1, bbox=BBox(x0=-20.0, y0=700.0, x1=100.0, y1=800.0), text=""
                ),
                LatticeCell(
                    row=0, col=1, row_span=1, col_span=1, bbox=BBox(x0=100.0, y0=700.0, x1=300.0, y1=800.0), text="x"
                ),
            ),
        )

        page = assemble_page(raw, assessment, LatticeResult(tables=(reaching,), used_pieces=()), source=path)

        (table,) = [element for element in page.elements if isinstance(element, Table)]
        assert table.bbox == BBox(x0=0.0, y0=700.0, x1=300.0, y1=LETTER_H)
        assert table.column_centres == (40.0, 200.0)
        # The box is clamped, the columns are not: they keep their raw coordinates.
        assert table.column_intervals == ((-20.0, 100.0), (100.0, 300.0))


def split_tables(tmp_path, tail, head) -> tuple[Table, Table]:
    """The tables of a 2-page PDF, after apply_scope: ``tail`` (runs) ending page 1, ``head`` starting page 2."""
    pages = [
        PageSpec(items=(TextRun("Parts list", 72.0, 60.0, size=18.0), *body_text(100.0), *tail)),
        PageSpec(items=(*head, *body_text(300.0))),
    ]
    path = build_pdf(tmp_path / "split.pdf", pages)
    scoped = apply_scope((assembled(path, 0), assembled(path, 1)))
    (first,) = [element for element in scoped.pages[0].elements if isinstance(element, Table)]
    (second,) = [element for element in scoped.pages[1].elements if isinstance(element, Table)]
    return first, second


def flush_right(rows, start: float, ends: tuple[float, ...], top: float) -> tuple[TextRun, ...]:
    """One 11 pt TextRun per cell, rows 14 pt apart from ``top``: column 0 from ``start``, others ending at ``ends``."""
    runs = []
    for index, row in enumerate(rows):
        y = top + 14.0 * index
        runs.append(TextRun(row[0], start, y))
        runs.extend(TextRun(text, end - after(text, 0.0), y) for text, end in zip(row[1:], ends))
    return tuple(runs)


def centred_runs(rows, middles: tuple[float, ...], top: float) -> tuple[TextRun, ...]:
    """One 11 pt TextRun per cell, rows 14 pt apart from ``top``, each column's cells centred on its middle."""
    return tuple(
        TextRun(text, middle - after(text, 0.0) / 2, top + 14.0 * index)
        for index, row in enumerate(rows)
        for text, middle in zip(row, middles)
    )


class TestSplitTablesLinkAcrossPages:
    """A6's apply_scope links a table split across two pages from real assemble_page output."""

    @pytest.mark.parametrize("kind", ["lattice", "stream"])
    def test_a_table_split_across_pages_is_linked(self, tmp_path, kind):
        if kind == "lattice":
            tail = (
                *lattice_rules(LATTICE_XS, (600.0, 620.0, 640.0, 660.0)),
                *written(SPEC, LATTICE_XS, (600.0, 620.0, 640.0, 660.0)),
            )
            head = (
                *lattice_rules(LATTICE_XS, (60.0, 80.0, 100.0)),
                *written(PARTS[3:] + (("Pin", "8", "0.02"),), LATTICE_XS, (60.0, 80.0, 100.0)),
            )
        else:
            tail = table_runs(PARTS, top=650.0)
            head = table_runs((("Washer", "100", "0.01"), ("Pin", "8", "0.02")), top=60.0)
        pages = [
            PageSpec(items=(TextRun("Parts list", 72.0, 60.0, size=18.0), *body_text(100.0), *tail)),
            PageSpec(items=(*head, *body_text(300.0))),
        ]
        path = build_pdf(tmp_path / "split.pdf", pages)

        first, second = assembled(path, 0), assembled(path, 1)
        scoped = apply_scope((first, second))

        (tail_table,) = [element for element in scoped.pages[0].elements if isinstance(element, Table)]
        (head_table,) = [element for element in scoped.pages[1].elements if isinstance(element, Table)]
        assert tail_table.extraction_method == head_table.extraction_method == kind
        assert head_table.continued_from_id == tail_table.element_id

    @pytest.mark.parametrize(
        "tail_rows,head_rows",
        [
            # #409's probe: a column is where its cells start, so the widest cells no longer move it.
            pytest.param(PARTS, (("Washer", "100", "0.01"), ("Pin", "8", "0.02")), id="head-repeats-the-widest-row"),
            pytest.param(PARTS, (("Pin", "8", "0.02"), ("Rivet", "20", "0.03")), id="head-without-the-widest-cells"),
            pytest.param(
                PARTS[:3] + (("Stainless hex bolt M8", "100", "0.01"),),
                (("Pin", "8", "0.02"), ("Rivet", "20", "0.03")),
                id="tail-with-a-long-part-name",
            ),
        ],
    )
    def test_the_parts_link_whatever_their_widest_cells(self, tmp_path, tail_rows, head_rows):
        tail, head = split_tables(tmp_path, table_runs(tail_rows, top=650.0), table_runs(head_rows, top=60.0))

        assert head.continued_from_id == tail.element_id

    def test_flush_right_amounts_link_by_their_end(self, tmp_path):
        # The amounts' widest cells differ by 34 pt, so their bands' middles lie 17 pt apart; their ends do not move.
        tail_rows = (("Item", "Qty", "Amount"), ("Bolt", "4", "1,234,567.00"), ("Nut", "12", "8.00"))
        head_rows = (("Pin", "8", "5.00"), ("Rivet", "20", "16.50"))

        tail, head = split_tables(
            tmp_path,
            flush_right(tail_rows, 72.0, (250.0, 400.0), 650.0),
            flush_right(head_rows, 72.0, (250.0, 400.0), 60.0),
        )

        assert head.continued_from_id == tail.element_id
        assert [round(centre) for centre in (*tail.column_centres, *head.column_centres)] == [72, 250, 400] * 2

    @pytest.mark.parametrize(
        "tail_rows,head_rows",
        [
            # #409's probe T5: the continuation's quantities and prices are each one width, so each sits at its start.
            pytest.param(
                (("Part", "Qty", "Price"), ("Bolt", "4", "10.10"), ("Nut", "12", "0.05"), ("Washer", "100", "0.01")),
                (("Pin", "8", "0.02"), ("Rivet", "9", "0.03")),
                id="continuation-one-width",
            ),
            # T8: a long amount on the tail, one-width amounts on the continuation.
            pytest.param(
                (("Item", "Qty", "Amount"), ("Bolt", "4", "1,234,567.00"), ("Nut", "12", "8.00")),
                (("Pin", "8", "5.00"), ("Rivet", "20", "6.00")),
                id="amounts-one-width",
            ),
            # The mirror: the first part's columns are one width, the continuation's flush right.
            pytest.param(
                (("Pin", "8", "0.02"), ("Rivet", "9", "0.03")),
                (("Bolt", "4", "10.10"), ("Nut", "12", "0.05"), ("Washer", "100", "0.01")),
                id="first-part-one-width",
            ),
        ],
    )
    def test_a_one_width_flush_right_part_links_by_its_end(self, tmp_path, tail_rows, head_rows):
        # #420: a column of one width tells no alignment and sits at its start (#409); its interval ends where the
        # other part's flush-right column sits.
        tail, head = split_tables(
            tmp_path,
            flush_right(tail_rows, 72.0, (250.0, 400.0), 650.0),
            flush_right(head_rows, 72.0, (250.0, 400.0), 60.0),
        )

        assert head.continued_from_id == tail.element_id
        assert round(tail.column_intervals[2][1]) == round(head.column_intervals[2][1]) == 400

    @pytest.mark.parametrize(
        "head",
        [
            # Columns set 12 pt further right: their middles lie 6 to 10 pt from the tail's, their starts 12 pt.
            pytest.param(
                table_runs((("Gasket", "3", "1.20"), ("Seal", "6", "0.80")), (84.0, 232.0, 372.0), 60.0), id="moved"
            ),
            # Columns centred on the tail's bands' middles (91.0, 229.2, 372.5): a table aligned another way.
            pytest.param(
                centred_runs((("Gasket ring", "36", "1.20"), ("O", "6", "0.8")), (91.0, 229.2, 372.5), 60.0),
                id="centred",
            ),
        ],
    )
    def test_a_different_table_with_similar_centres_is_not_linked(self, tmp_path, head):
        _, found = split_tables(tmp_path, table_runs(PARTS, top=650.0), head)

        assert found.continued_from_id is None

    def test_a_turned_split_table_is_not_linked(self, tmp_path):
        pages = [
            PageSpec(items=(*body_text(100.0), *table_runs(PARTS, top=650.0)), rotate=90),
            PageSpec(
                items=(*table_runs((("Washer", "100", "0.01"), ("Pin", "8", "0.02")), top=60.0), *body_text(300.0)),
                rotate=90,
            ),
        ]
        path = build_pdf(tmp_path / "split.pdf", pages)

        scoped = apply_scope((assembled(path, 0), assembled(path, 1)))

        assert [
            element.continued_from_id
            for page in scoped.pages
            for element in page.elements
            if isinstance(element, Table)
        ] == [None, None]

    @pytest.mark.parametrize("font", ["Helvetica-Bold", "Helvetica"], ids=["repeated-bold", "repeated-plain"])
    def test_a_repeated_all_number_header_row_is_the_continuations_header_row(self, tmp_path, font):
        # An all-number row is a header row only when set apart (bold, here), so page 2's plain repeat is read as
        # its first row. Bold or plain, the repeat is the continuation's header row and none of its rows.
        years = ("2023", "2024", "2025")
        numbers = tuple((str(100 + 10 * i), str(101 + 10 * i), str(102 + 10 * i)) for i in range(7))
        tail = (*table_runs((years,), top=650.0, font="Helvetica-Bold"), *table_runs(numbers[:4], top=664.0))
        head = (*table_runs((years,), top=60.0, font=font), *table_runs(numbers[4:], top=74.0))

        tail_table, head_table = split_tables(tmp_path, tail, head)

        assert head_table.continued_from_id == tail_table.element_id
        assert (head_table.headers, head_table.rows) == (years, numbers[4:])


STAIRCASE_SCRIPT = r"""
import os, resource, sys, threading, time

LIMIT = 1536 << 20  # bytes of peak RSS; past it the process ends at once


def peak() -> int:
    # Linux keeps ru_maxrss across fork + exec, so a child of a large process
    # (pytest) reports the parent's peak; VmHWM is this process's own, reset at exec.
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("no VmHWM line in /proc/self/status")
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # macOS reports bytes


def watch() -> None:
    while True:
        if peak() > LIMIT:
            sys.stdout.write(f"killed: peak RSS over {LIMIT >> 20} MiB\n")
            sys.stdout.flush()
            os._exit(99)
        time.sleep(0.005)


sys.path.insert(0, sys.argv[1])
from datagrunt.pdf import assembly
from datagrunt.pdf.layout import LatticeResult, page_layout
from datagrunt.pdf.pdfium_engine import PageGeometry
from pdf_tests.test_layout_stream_tables import staircase

print(f"imported: peak {peak() >> 20} MiB", flush=True)
threading.Thread(target=watch, daemon=True).start()
rows = int(sys.argv[2])
height = 10.0 + 1.5 * rows
found = page_layout(staircase(rows), (), LatticeResult(tables=(), used_pieces=()), width_pt=600.0, height_pt=height)
geometry = PageGeometry(page_number=1, width_pt=600.0, height_pt=height, rotation_deg=0, crop_offset=(0.0, 0.0))
elements = [assembly._element(item, order, (), geometry) for order, item in enumerate(found.items)]
print(f"elements {len(elements)}")
print(f"peak {peak() >> 20} MiB")
"""


def staircase_assessment(pieces) -> PageAssessment:
    """A3's assessment of a page whose pieces are ``pieces``, all visible."""
    quality = PageQuality(
        valid_char_ratio=1.0,
        char_count=sum(piece.end - piece.start for piece in pieces),
        vector_line_count=0,
        hidden_spans_flagged=0,
        rotation_deg=0,
        deskew_angle_deg=None,
        ocr_mean_confidence=None,
    )
    return PageAssessment(
        pieces=pieces, recovered_pieces=(), quality=quality, route="digital", unmapped_regions=(), warnings=()
    )


class TestStreamTableBoundsEndToEnd:
    """A staircase of cells (each line's in bands of their own) builds no grid past the page bound."""

    @pytest.mark.parametrize("rows", [4000, 10_000], ids=["8000-chars", "20000-chars"])
    def test_a_staircase_passes_pass_c(self, rows):
        pieces = staircase(rows)
        height = 10.0 + 1.5 * rows
        geometry = PageGeometry(page_number=1, width_pt=600.0, height_pt=height, rotation_deg=0, crop_offset=(0.0, 0.0))

        found = layout.page_layout(
            pieces, (), LatticeResult(tables=(), used_pieces=()), width_pt=600.0, height_pt=height
        )
        elements = tuple(assembly._element(item, order, (), geometry) for order, item in enumerate(found.items))

        assert pass_c(staircase_assessment(pieces), elements) == (True, ())
        cells = [len(element.rows) * len(element.rows[0]) for element in elements if isinstance(element, Table)]
        assert sum(cells) <= layout.MAX_STREAM_GRID_UNITS

    def test_a_staircase_of_20000_chars_stays_small_in_a_memory_limited_process(self):
        tests = Path(__file__).resolve().parent.parent
        done = subprocess.run(
            [sys.executable, "-c", STAIRCASE_SCRIPT, str(tests), "10000"],
            capture_output=True,
            check=False,
            timeout=300,
            text=True,
        )

        assert done.returncode == 0, done.stdout + done.stderr
        imported = int(re.search(r"imported: peak (\d+) MiB", done.stdout).group(1))
        assert int(re.search(r"^peak (\d+) MiB", done.stdout, re.MULTILINE).group(1)) - imported < 200, done.stdout


# Recovered drawn copies (#406): hand-written 400 x 300 pt pages in Helvetica (/F1), a line of body text first so
# the page routes digital and Pass C runs (a vision page is never verified).
BODY = b"BT /F1 12 Tf 20 270 Td (A digital page with enough visible text) Tj ET "
# "Total"'s codes read "Words" through this ToUnicode map (a simple font's one-byte codes).
RELABEL_CMAP = (
    b"/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n/CMapName /Test-UCS def\n/CMapType 2 def\n"
    b"1 begincodespacerange\n<00> <FF>\nendcodespacerange\n5 beginbfchar\n"
    b"<54> <0057>\n<6F> <006F>\n<74> <0072>\n<61> <0064>\n<6C> <0073>\nendbfchar\nendcmap\n"
    b"CMapName currentdict /CMap defineresource pop\nend\nend"
)


def hand_written(tmp_path, content: bytes) -> Path:
    """A hand-written page drawing the body line, then ``content``."""
    return content_page(tmp_path, BODY + content)


def relabelled_copy_page(tmp_path) -> Path:
    """ "Total" in Helvetica, then its codes in a Helvetica whose ToUnicode map reads them "Words", on it.

    pdfium drops the second object as a copy of the first (the same codes at the same size, on it), so
    recovery reads back "Words": text the page shows nowhere else.
    """
    content = BODY + b"BT /F1 12 Tf 72 100 Td (Total) Tj ET BT /F2 12 Tf 72 100 Td (Total) Tj ET"
    relabelled = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode 7 0 R >>"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R"
        b" /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        relabelled,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(RELABEL_CMAP), RELABEL_CMAP),
    ]
    return hand_written_pdf(tmp_path / "relabelled.pdf", objects)


def recovered_read(path) -> tuple[list, Page]:
    """Page 1's recovered pieces as ``(text, hidden_reasons)``, and its assembled page."""
    page, assessment, lattice = inputs(path)
    pieces = [(item.text, item.hidden_reasons) for item in assessment.recovered_pieces]
    return pieces, assemble_page(page, assessment, lattice, source=path)


def texts_shown(page: Page, *, hidden: bool) -> list:
    """The text of each hidden (with its reasons) or visible element."""
    return [(element.text, element.hidden_reasons) for element in page.elements if element.is_hidden == hidden]


class TestRecoveredDuplicatesEndToEnd:
    """A shadow or outline copy pdfium drops reads once: its recovered piece is a hidden ``duplicate`` (#406)."""

    @pytest.mark.parametrize("offset", [0.25, 2.0, 7.0])
    def test_a_shadow_title_reads_once(self, tmp_path, offset):
        # The grey shadow first, offset right and down (pdfium keeps it), then the black title it drops.
        shadow = b"q 0.6 g BT /F1 24 Tf %g %g Td (Walking) Tj ET Q " % (60 + offset, 150 - offset)
        path = hand_written(tmp_path, shadow + b"BT /F1 24 Tf 60 150 Td (Walking) Tj ET")

        pieces, page = recovered_read(path)

        assert pieces == [("Walking", ("duplicate",))]
        assert [text for text, _ in texts_shown(page, hidden=False)].count("Walking") == 1
        assert texts_shown(page, hidden=True) == [("Walking", ("duplicate",))]
        assert page.integrity_verified is True

    def test_a_copy_20_pt_away_stays_visible(self, tmp_path):
        # pdfium drops a copy within 0.9 of its last glyph's advance (a 30 pt "W": 25 pt); the window stops at 12 pt.
        path = hand_written(
            tmp_path, b"BT /F1 30 Tf 60 150 Td (SHOW) Tj ET q 0.6 g BT /F1 30 Tf 80 150 Td (SHOW) Tj ET Q"
        )

        pieces, page = recovered_read(path)

        assert pieces == [("SHOW", ())]
        assert [text for text, _ in texts_shown(page, hidden=False)].count("SHOW") == 2
        assert page.integrity_verified is True

    @pytest.mark.parametrize(
        "hidden,reasons",
        [
            # Squeezed under 1 pt tall by its matrix: pdfium keeps it and drops the drawn copy on it.
            pytest.param(b"BT /F1 12 Tf 1 0 0 0.05 72 100 Tm (Total) Tj ET", ("tiny_font",), id="tiny_font"),
            # Tr is graphics state: q / Q keeps it from the copy.
            pytest.param(b"q BT /F1 12 Tf 3 Tr 72 100 Td (Total) Tj ET Q", ("invisible",), id="invisible"),
        ],
    )
    def test_a_copy_of_hidden_text_stays_visible(self, tmp_path, hidden, reasons):
        path = hand_written(tmp_path, hidden + b" BT /F1 12 Tf 72 100 Td (Total) Tj ET")

        pieces, page = recovered_read(path)

        assert pieces == [("Total", ())]
        assert [text for text, _ in texts_shown(page, hidden=False)].count("Total") == 1
        assert texts_shown(page, hidden=True) == [("Total", reasons)]
        assert page.integrity_verified is True

    def test_recovered_unique_text_stays_visible(self, tmp_path):
        pieces, page = recovered_read(relabelled_copy_page(tmp_path))

        assert pieces == [("Words", ())]
        assert {"Total", "Words"} <= {text for text, _ in texts_shown(page, hidden=False)}
        assert texts_shown(page, hidden=True) == []
        assert page.integrity_verified is True
