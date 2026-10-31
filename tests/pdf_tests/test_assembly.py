"""Tests for v5 page assembly (#368, A4b group 3): figures, then the page's elements and Pass C.

Unit tests build A2 values (``RawImage``, ``PageImages``, ``PageGeometry``)
directly: boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.
End-to-end tests build PDFs with the ReportLab toolkit (``pdf_tests.builders``)
and read them through ``extract_page``.
"""

import dataclasses
import hashlib
import pickle

import pytest

from datagrunt.pdf import assembly
from datagrunt.pdf.assembly import FigureDraft, PageFigures, page_figures
from datagrunt.pdf.elements import BBox, Figure
from datagrunt.pdf.pdfium_engine import (
    ImageLoader,
    ImageStats,
    PageGeometry,
    PageImages,
    RawPage,
    extract_page,
    open_document,
)
from pdf_tests.builders import PageSpec, RasterImage, build_pdf, solid_png
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
