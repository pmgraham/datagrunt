"""Tests for v5 page assembly (#368, A4b group 3): figures, then the page's elements and Pass C.

Unit tests build A2 values (``RawImage``, ``PageImages``, ``PageGeometry``)
directly: boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.
End-to-end tests build PDFs with the ReportLab toolkit (``pdf_tests.builders``)
and read them through ``extract_page``.
"""

import dataclasses
import hashlib
import pickle
import re
import subprocess
import sys
from pathlib import Path

import pytest

from datagrunt.pdf import assembly, layout
from datagrunt.pdf.assembly import FigureDraft, PageFigures, assemble_page, page_figures, pass_c
from datagrunt.pdf.elements import BBox, Figure, Page, PageQuality, Paragraph, Table
from datagrunt.pdf.layout import (
    MAX_CROSSINGS,
    LatticeCell,
    LatticeResult,
    LatticeTable,
    StreamTable,
    find_lattice_tables,
    stream_layout,
)
from datagrunt.pdf.pdfium_engine import (
    ImageLoader,
    ImageStats,
    PageGeometry,
    PageImages,
    RawPage,
    extract_page,
    open_document,
    recover_dropped_text,
)
from datagrunt.pdf.quality import PageAssessment, assess_page
from datagrunt.pdf.scope import apply_scope
from pdf_tests.builders import PageSpec, RasterImage, TextRun, build_pdf, solid_png
from pdf_tests.test_layout_apart import tiny_text_page
from pdf_tests.test_layout_lattice import rules as lattice_rules
from pdf_tests.test_layout_lattice import written
from pdf_tests.test_layout_stream_tables import body_text, staircase, table_runs
from pdf_tests.test_pdfium_engine import imported_modules
from pdf_tests.test_quality import SHA, image

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
            "loader": ImageLoader(source=tmp_path / "doc.pdf", page_index=0, image=raw),
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
        assert draft.loader == ImageLoader(source=tmp_path / "doc.pdf", page_index=0, image=raw)

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

        monkeypatch.setattr(ImageLoader, "png_bytes", refuse)
        monkeypatch.setattr(ImageLoader, "raw_bytes", refuse)

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
        # A2 records a decode size over the pixel bound without pixel metadata; ImageLoader refuses to decode it.
        huge = image(INSIDE, px_width=20000, px_height=20000, declared_px=(20000, 20000), bits_per_pixel=None)

        (draft,) = drafted(tmp_path, huge).drafts

        assert (draft.width_px, draft.height_px) == (20000, 20000)

    def test_an_unsized_image_is_a_figure_of_its_declared_size(self, tmp_path):
        # A JPEG behind Flate: its frame size cannot be read without inflating it, so A2 records px 0 x 0.
        behind_flate = image(
            INSIDE,
            px_width=0,
            px_height=0,
            declared_px=(158, 506),
            filters=("FlateDecode", "DCTDecode"),
            bits_per_pixel=None,
            colorspace=None,
        )

        (draft,) = drafted(tmp_path, behind_flate).drafts

        assert (draft.width_px, draft.height_px) == (158, 506)

    def test_an_unsized_image_declared_under_the_minimum_is_skipped(self, tmp_path):
        small = image(INSIDE, px_width=0, px_height=0, declared_px=(20, 20), bits_per_pixel=None, colorspace=None)

        assert drafted(tmp_path, small) == PageFigures(drafts=(), warnings=())

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

    def test_an_image_not_read_and_under_the_minimum_is_skipped_without_a_warning(self, tmp_path):
        assert drafted(tmp_path, sized(10, 10, raw_sha256=None)) == PageFigures(drafts=(), warnings=())

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
            None,
            None,
        )
        assert (page.integrity_verified, page.warnings) == (True, ())

    def test_text_elements_convert_their_fields(self, tmp_path):
        heading, paragraph, pair, bullets, *_ = assembled(letter(tmp_path, *mixed_items())).elements

        assert (heading.text, heading.level, heading.scope_path) == ("Results", 1, ("Results",))
        assert paragraph.text.startswith("The woodland is vast") and paragraph.scope_path == ()
        assert (pair.key, pair.value) == ("Invoice", "10442")
        assert (bullets.items, bullets.markers, bullets.ordered) == (("first item", "second item"), ("•", "•"), False)

    def test_an_ordered_list_keeps_its_markers(self, tmp_path):
        items = (TextRun("1. one", 72.0, 100.0, size=10.0), TextRun("2. two", 72.0, 114.0, size=10.0))

        (bullets,) = assembled(letter(tmp_path, *body_text(300.0), *items)).elements[:1]

        assert (bullets.items, bullets.markers, bullets.ordered) == (("one", "two"), ("1.", "2."), True)

    def test_a_ruled_table_is_a_lattice_table_with_its_grid_column_centres(self, tmp_path):
        tables = [
            element for element in assembled(letter(tmp_path, *mixed_items())).elements if isinstance(element, Table)
        ]

        ruled = tables[0]
        assert (ruled.extraction_method, ruled.headers, ruled.rows) == ("lattice", SPEC[0], SPEC[1:])
        assert ruled.column_centres == pytest.approx((100.0, 200.0, 300.0))

    def test_a_borderless_table_is_a_stream_table_with_its_band_centres(self, tmp_path):
        path = letter(tmp_path, *mixed_items())
        raw, assessment, lattice = inputs(path)
        geometry = raw.geometry
        (stream,) = [
            item
            for item in stream_layout(
                assessment.pieces, lattice, width_pt=geometry.width_pt, height_pt=geometry.height_pt
            )
            if isinstance(item, StreamTable)
        ]

        table = [
            element
            for element in assemble_page(raw, assessment, lattice, source=path).elements
            if isinstance(element, Table)
        ][1]

        assert (table.extraction_method, table.headers, table.rows) == ("stream", PARTS[0], PARTS[1:])
        assert table.column_centres == stream.column_centres

    @pytest.mark.parametrize("rotate", [90, 270])
    def test_a_turned_table_has_no_column_centres(self, tmp_path, rotate):
        ys = (340.0, 360.0, 380.0, 400.0)
        items = (*lattice_rules(LATTICE_XS, ys), *written(SPEC, LATTICE_XS, ys), *table_runs(PARTS, top=460.0))

        tables = [
            element
            for element in assembled(letter(tmp_path, *items, rotate=rotate)).elements
            if isinstance(element, Table)
        ]

        assert sorted(table.extraction_method for table in tables) == ["lattice", "stream"]
        assert [table.column_centres for table in tables] == [(), ()]

    def test_a_figure_is_bound_and_never_loaded(self, tmp_path, monkeypatch):
        def refuse(self):
            raise AssertionError("a loader was called")

        monkeypatch.setattr(ImageLoader, "png_bytes", refuse)
        monkeypatch.setattr(ImageLoader, "raw_bytes", refuse)

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

    def test_a_vision_page_keeps_its_digital_text_and_says_ocr_has_not_run(self, tmp_path):
        page = assembled(letter(tmp_path, TextRun("Too short", 72.0, 100.0)))

        assert (page.route, page.status, page.integrity_verified) == ("vision", "ocr_unavailable", False)
        assert [element.text for element in page.elements] == ["Too short"]
        assert page.warnings == (
            "route 'vision': OCR (A5) has not run on this page; its digital text is kept as its elements",
        )


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
