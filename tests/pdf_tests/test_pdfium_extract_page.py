"""Tests for the v5 pypdfium2 engine's one-load page extraction (#365 T5).

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), whose
coordinates are top-left points in the unrotated page space. Page faults are
made with ``break_page`` (one page unloadable), a CropBox outside the
MediaBox, or a pdfium stand-in (``monkeypatch.setattr(pdfium_c, ...)``).
"""

import dataclasses
import pickle
import re
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import PDFCorruptError, pdfium_engine
from datagrunt.pdf.pdfium_engine import (
    ImageStats,
    PageFailure,
    PageGeometry,
    PageImages,
    PageSegments,
    PageText,
    RawPage,
    SegmentStats,
    TextStats,
    extract_page,
    open_document,
    page_geometry,
    page_images,
    page_segments,
    page_text,
)
from pdf_tests.builders import Line, PageSpec, RasterImage, TextRun, break_page, build_pdf, solid_png

PNG = solid_png(64, 32, (1.0, 0.0, 0.0))
# On a page holding all three, the text is object 0, the line 1 and the image 2.
TEXT = TextRun("Hello", x=72.0, y=60.0)
LINE = Line(20.0, 150.0, 220.0, 150.0)
IMAGE = RasterImage(250.0, 100.0, 100.0, 60.0, PNG)


def full_page(tmp_path: Path, rotate: int = 0) -> Path:
    """A one-page 400 x 300 pt PDF with a text run, a stroked line and a raster image."""
    spec = PageSpec(width=400.0, height=300.0, items=(TEXT, LINE, IMAGE), rotate=rotate)
    return build_pdf(tmp_path / "page.pdf", [spec])


def three_pages(tmp_path: Path) -> Path:
    """Three full pages whose text reads "Page 1" to "Page 3"."""
    pages = [PageSpec(items=(TextRun(f"Page {n}", x=72.0, y=100.0), LINE, IMAGE)) for n in (1, 2, 3)]
    return build_pdf(tmp_path / "three.pdf", pages)


def corrupt_error(read, handle, index: int) -> PDFCorruptError:
    """The PDFCorruptError ``read(handle, index)`` raises (a per-stage function)."""
    with pytest.raises(PDFCorruptError) as caught:
        read(handle, index)
    return caught.value


@pytest.fixture
def handle_closes(monkeypatch):
    """Record each explicit PdfPage / PdfTextPage close() call (not a parent's cascade), by type name.

    Refcounting alone would also free a dropped handle, so counting open
    handles cannot show that the engine closes them; counting close() calls can.
    """
    calls = []
    for cls in (pdfium.PdfPage, pdfium.PdfTextPage):

        def counting_close(self, *args, _original=cls.close, **kwargs):
            if not kwargs.get("_by_parent", False):
                calls.append(type(self).__name__)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(cls, "close", counting_close)
    return calls


def failing(value):
    """A pdfium stand-in that reports failure by returning ``value``."""
    return lambda *args: value


class TestGoodPage:
    """A readable page is every per-stage function's result, from one load."""

    @pytest.mark.parametrize("rotate", [0, 90])
    def test_equals_the_per_stage_functions(self, tmp_path, rotate):
        handle = open_document(full_page(tmp_path, rotate))

        got = extract_page(handle, 0)

        assert got == RawPage(
            geometry=page_geometry(handle, 0),
            text=page_text(handle, 0),
            segments=page_segments(handle, 0),
            images=page_images(handle, 0),
        )
        # Not vacuous: every stage found its item on the page.
        assert [item.text for item in got.text.spans] == ["Hello"]
        assert [item.object_index for item in got.segments.segments] == [1]
        assert [item.object_index for item in got.images.images] == [2]
        assert (got.page_number, got.geometry.rotation_deg) == (1, rotate)

    def test_page_number_is_one_based(self, tmp_path):
        handle = open_document(three_pages(tmp_path))

        assert [extract_page(handle, index).page_number for index in range(3)] == [1, 2, 3]

    def test_loads_the_page_once_and_closes_it(self, tmp_path, monkeypatch, handle_closes):
        handle = open_document(full_page(tmp_path))
        loaded, load_page_calls = [], []
        real_get_page, real_load_page = pdfium.PdfDocument.get_page, pdfium_engine._load_page

        def counting_get_page(self, index):
            page = real_get_page(self, index)
            loaded.append(page)
            return page

        def counting_load_page(handle_, index):
            load_page_calls.append(index)
            return real_load_page(handle_, index)

        monkeypatch.setattr(pdfium.PdfDocument, "get_page", counting_get_page)
        monkeypatch.setattr(pdfium_engine, "_load_page", counting_load_page)

        extract_page(handle, 0)

        assert load_page_calls == [0]
        assert len(loaded) == 1
        assert handle_closes == ["PdfTextPage", "PdfPage"]
        assert loaded[0].raw is None  # the one page loaded is the one closed
        assert not handle.closed


class TestFaultIsolation:
    """A page pdfium cannot read is a PageFailure; the document's other pages are still read."""

    def test_an_unloadable_page_fails_alone(self, tmp_path, handle_closes):
        handle = open_document(break_page(three_pages(tmp_path), 2))

        results = [extract_page(handle, index) for index in range(3)]

        # Pages 1 and 3 are closed, each after its textpage; page 2 never loaded.
        assert handle_closes == ["PdfTextPage", "PdfPage"] * 2
        assert [type(result) for result in results] == [RawPage, PageFailure, RawPage]
        first, failure, third = results
        assert (failure.page_number, failure.stage) == (2, "page")
        assert "Failed to load page" in failure.message
        assert failure.message == str(corrupt_error(page_geometry, handle, 1))
        assert [result.text.spans[0].text for result in (first, third)] == ["Page 1", "Page 3"]

    def test_a_page_without_a_visible_area_is_a_page_failure(self, tmp_path, handle_closes):
        # A CropBox outside the MediaBox: pdfium intersects them to nothing.
        pdf = pdfium.PdfDocument(full_page(tmp_path))
        pdf[0].set_cropbox(500.0, 500.0, 600.0, 600.0)
        path = tmp_path / "empty.pdf"
        pdf.save(path)
        handle = open_document(path)
        handle_closes.clear()

        got = extract_page(handle, 0)

        assert handle_closes == ["PdfPage"]  # no textpage was opened
        assert isinstance(got, PageFailure)
        assert (got.page_number, got.stage) == (1, "page")
        assert "no visible area" in got.message
        assert got.message == str(corrupt_error(page_geometry, handle, 0))


# (pdfium function, stand-in, stage, the per-stage function, message): extract_page
# reports each as a failure of that stage, with the error that function raises.
STAGE_FAILURES = [
    pytest.param(
        "FPDFText_CountChars",
        failing(-1),
        "text",
        page_text,
        "page 1: pdfium cannot count the text page's chars",
        id="text",
    ),
    pytest.param(
        "FPDFPath_GetDrawMode",
        failing(0),
        "objects",
        page_segments,
        "page 1: pdfium cannot read the draw mode of object 1",
        id="segments",
    ),
    pytest.param(
        "FPDFImageObj_GetImageFilterCount",
        failing(-1),
        "objects",
        page_images,
        "page 1: pdfium cannot count the filters of object 2",
        id="images",
    ),
    # The object walk serves text (object indices) and objects alike: text runs first.
    pytest.param(
        "FPDFPage_CountObjects",
        failing(-1),
        "text",
        page_text,
        "page 1: pdfium cannot count the objects of the page",
        id="object-walk",
    ),
]


class TestStageFailures:
    """A pdfium failure in a stage is a PageFailure of that stage, with that stage's own error message."""

    @pytest.mark.parametrize("function,stand_in,stage,reader,message", STAGE_FAILURES)
    def test_a_pdfium_failure_is_a_failure_of_its_stage(
        self, tmp_path, monkeypatch, handle_closes, function, stand_in, stage, reader, message
    ):
        handle = open_document(full_page(tmp_path))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        got = extract_page(handle, 0)

        assert got == PageFailure(page_number=1, stage=stage, message=message)
        assert handle_closes == ["PdfTextPage", "PdfPage"]
        assert str(corrupt_error(reader, handle, 0)) == message

    def test_a_textpage_that_cannot_load_is_a_text_failure(self, tmp_path, monkeypatch, handle_closes):
        def failing_textpage(self):
            raise pdfium.PdfiumError("Failed to load text page.")

        monkeypatch.setattr(pdfium.PdfPage, "get_textpage", failing_textpage)
        handle = open_document(full_page(tmp_path))

        got = extract_page(handle, 0)

        message = "page 1: pdfium cannot load the text page: Failed to load text page."
        assert got == PageFailure(page_number=1, stage="text", message=message)
        assert handle_closes == ["PdfPage"]

    @pytest.mark.parametrize(
        "function,stand_in,unaffected",
        [
            pytest.param("FPDFPath_GetDrawMode", failing(0), (page_text,), id="segments"),
            pytest.param("FPDFImageObj_GetImageFilterCount", failing(-1), (page_text, page_segments), id="images"),
        ],
    )
    def test_the_objects_stand_ins_leave_the_earlier_stages_working(
        self, tmp_path, monkeypatch, function, stand_in, unaffected
    ):
        # So an "objects" failure above is the objects stage's own, not an earlier stage's.
        handle = open_document(full_page(tmp_path))
        expected = [read(handle, 0) for read in unaffected]
        monkeypatch.setattr(pdfium_c, function, stand_in)

        assert [read(handle, 0) for read in unaffected] == expected

    def test_segments_are_read_before_images(self, tmp_path, monkeypatch):
        handle = open_document(full_page(tmp_path))
        monkeypatch.setattr(pdfium_c, "FPDFPath_GetDrawMode", failing(0))
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageFilterCount", failing(-1))

        got = extract_page(handle, 0)

        assert got == PageFailure(
            page_number=1, stage="objects", message="page 1: pdfium cannot read the draw mode of object 1"
        )


def raises_bug(*args, **kwargs):
    """A stand-in for engine code with a bug: it raises an error that is not a page fault."""
    raise ZeroDivisionError("bug")


class TestBugsPropagate:
    """An error that is not a PDFCorruptError is a bug, not a page fault: it raises, and the page is closed."""

    @pytest.mark.parametrize(
        "target,name,closes",
        [
            pytest.param(pdfium.PdfPage, "get_bbox", ["PdfPage"], id="geometry"),
            pytest.param(pdfium_engine, "collect_text", ["PdfTextPage", "PdfPage"], id="text"),
            pytest.param(pdfium_engine, "path_segments", ["PdfTextPage", "PdfPage"], id="segments"),
            pytest.param(pdfium_engine, "collect_images", ["PdfTextPage", "PdfPage"], id="images"),
        ],
    )
    def test_a_bug_in_a_stage_raises_and_closes_the_page(
        self, tmp_path, monkeypatch, handle_closes, target, name, closes
    ):
        handle = open_document(full_page(tmp_path))
        monkeypatch.setattr(target, name, raises_bug)

        with pytest.raises(ZeroDivisionError, match="^bug$"):
            extract_page(handle, 0)

        assert handle_closes == closes

    def test_a_corrupt_error_naming_no_page_is_not_a_page_failure(self, tmp_path, monkeypatch):
        # A PDFCorruptError without a page_number is about the document, not one page.
        def unreadable_document(handle_, index):
            raise PDFCorruptError("the document is unreadable")

        monkeypatch.setattr(pdfium_engine, "_load_page", unreadable_document)

        with pytest.raises(PDFCorruptError, match="^the document is unreadable$"):
            extract_page(open_document(full_page(tmp_path)), 0)


class TestArguments:
    """Argument errors raise, exactly as page_text raises them: they are the caller's, not the page's."""

    @pytest.mark.parametrize(
        "index,error,message",
        [
            pytest.param(-1, IndexError, "index must be in 0..2, got -1", id="negative"),
            pytest.param(3, IndexError, "index must be in 0..2, got 3", id="past-end"),
            pytest.param(True, TypeError, "index must be an int, got True (bool)", id="bool"),
            pytest.param(1.0, TypeError, "index must be an int, got 1.0 (float)", id="float"),
        ],
    )
    def test_invalid_index_raises(self, tmp_path, index, error, message):
        handle = open_document(three_pages(tmp_path))

        with pytest.raises(error, match=re.escape(message) + "$"):
            extract_page(handle, index)
        with pytest.raises(error, match=re.escape(message) + "$"):
            page_text(handle, index)

    def test_non_handle_raises(self):
        with pytest.raises(TypeError, match=re.escape("handle must be a DocumentHandle, got str")):
            extract_page("doc.pdf", 0)

    def test_closed_handle_raises(self, tmp_path):
        handle = open_document(three_pages(tmp_path))
        handle.close()

        with pytest.raises(ValueError, match="document handle is closed"):
            extract_page(handle, 0)


GEOMETRY = PageGeometry(page_number=3, width_pt=400.0, height_pt=300.0, rotation_deg=0, crop_offset=(0.0, 0.0))
EMPTY_TEXT = PageText(
    spans=(),
    stats=TextStats(
        char_count=0,
        generated_count=0,
        ownerless_count=0,
        unicode_map_error_count=0,
        skipped_nonfinite_objects=0,
        dropped_text_objects=0,
    ),
)
EMPTY_SEGMENTS = PageSegments(segments=(), stats=SegmentStats(skipped_nonfinite_paths=0))
EMPTY_IMAGES = PageImages(
    images=(),
    stats=ImageStats(
        skipped_nonfinite_images=0,
        skipped_oversized_images=0,
        skipped_unsized_images=0,
        skipped_over_budget_images=0,
        max_cover_fraction=0.0,
    ),
)


def raw_page(**overrides) -> RawPage:
    """A valid (empty) RawPage of page 3 with ``overrides`` applied."""
    fields = {"geometry": GEOMETRY, "text": EMPTY_TEXT, "segments": EMPTY_SEGMENTS, "images": EMPTY_IMAGES}
    return RawPage(**(fields | overrides))


def failure(**overrides) -> PageFailure:
    """A valid PageFailure with ``overrides`` applied."""
    fields = {"page_number": 2, "stage": "objects", "message": "page 2: pdfium cannot read the matrix of object 4"}
    return PageFailure(**(fields | overrides))


class TestRawPage:
    """RawPage is a frozen, slotted, picklable, validated record."""

    def test_pickle_round_trip_of_an_extracted_page(self, tmp_path):
        original = extract_page(open_document(full_page(tmp_path, rotate=90)), 0)

        assert isinstance(original, RawPage)
        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = raw_page()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.text = EMPTY_TEXT  # type: ignore[misc]
        assert not hasattr(value, "__dict__")

    def test_page_number_is_the_geometry_page_number(self):
        assert raw_page().page_number == 3

    def test_fields_are_keyword_only(self):
        with pytest.raises(TypeError):
            RawPage(GEOMETRY, EMPTY_TEXT, EMPTY_SEGMENTS, EMPTY_IMAGES)  # type: ignore[misc]

    @pytest.mark.parametrize(
        "overrides,message",
        [
            pytest.param({"geometry": "page 3"}, "geometry must be a PageGeometry, got str", id="geometry"),
            pytest.param({"text": None}, "text must be a PageText, got NoneType", id="text"),
            pytest.param({"segments": EMPTY_IMAGES}, "segments must be a PageSegments, got PageImages", id="segments"),
            pytest.param({"images": EMPTY_SEGMENTS}, "images must be a PageImages, got PageSegments", id="images"),
        ],
    )
    def test_invalid_field_raises(self, overrides, message):
        with pytest.raises(TypeError, match=re.escape(message) + "$"):
            raw_page(**overrides)


class TestPageFailure:
    """PageFailure is a frozen, slotted, picklable, validated record."""

    def test_pickle_round_trip_of_an_extracted_failure(self, tmp_path):
        original = extract_page(open_document(break_page(three_pages(tmp_path), 2)), 1)

        assert isinstance(original, PageFailure)
        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = failure()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.stage = "page"  # type: ignore[misc]
        assert not hasattr(value, "__dict__")

    @pytest.mark.parametrize("stage", ["page", "text", "objects"])
    def test_each_stage_is_accepted(self, stage):
        assert failure(stage=stage).stage == stage

    def test_fields_are_keyword_only(self):
        with pytest.raises(TypeError):
            PageFailure(2, "page", "page 2 is unreadable")  # type: ignore[misc]

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"page_number": 0}, ValueError, "page_number must be >= 1, got 0", id="page-0"),
            pytest.param({"page_number": True}, TypeError, "page_number must be an int, got True (bool)", id="bool"),
            pytest.param({"page_number": "2"}, TypeError, "page_number must be an int, got '2' (str)", id="page-str"),
            pytest.param(
                {"stage": "render"},
                ValueError,
                "stage must be 'page' or 'text' or 'objects', got 'render'",
                id="stage-unknown",
            ),
            pytest.param(
                {"stage": "Page"}, ValueError, "stage must be 'page' or 'text' or 'objects', got 'Page'", id="case"
            ),
            pytest.param({"stage": None}, TypeError, "stage must be a str, got None (NoneType)", id="stage-type"),
            pytest.param({"message": ""}, ValueError, "message must be a non-blank str, got ''", id="message-empty"),
            pytest.param({"message": " \n"}, ValueError, "message must be a non-blank str", id="message-blank"),
            pytest.param({"message": b"x"}, TypeError, "message must be a str, got b'x' (bytes)", id="message-type"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            failure(**overrides)
