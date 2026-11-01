"""Tests for the v5 document pipeline (#372, A8a): ``datagrunt.pdf.pipeline``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``): a
corrupt page with ``break_page``, an encrypted PDF with ``user_password``,
and a split table with A4b's own table fixtures. A stage that raises is a
``monkeypatch`` of the pipeline's module name for it.
"""

import os
import pickle
import re
from pathlib import Path

import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import PDFCorruptError, PDFEncryptedError, pipeline
from datagrunt.pdf.assembly import _VISION_NOTE, assemble_page
from datagrunt.pdf.elements import Figure, Page, Paragraph
from datagrunt.pdf.layout import find_lattice_tables
from datagrunt.pdf.pdfium_engine import extract_page, open_document, recover_dropped_text
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_page_at
from datagrunt.pdf.quality import assess_page
from pdf_tests.builders import PageSpec, RasterImage, TextRun, break_page, build_pdf, solid_png
from pdf_tests.test_layout_lattice import rules as lattice_rules
from pdf_tests.test_layout_lattice import written
from pdf_tests.test_layout_stream_tables import body_text, table_runs
from pdf_tests.test_pdfium_extract_page import failing

LATTICE_XS = (50.0, 150.0, 250.0, 350.0)
PARTS = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))
# A 64 x 32 px image: a figure at min_image_dimension 32, none at 40 (the default).
PNG = solid_png(64, 32, (0.2, 0.4, 0.6))
DEFAULT_SIZE_NOTE = "; 612 x 792 pt is pdfium's default page size, not this page's"


def mixed_page(top: float = 60.0) -> PageSpec:
    """A heading, prose, an image, a ruled table and a borderless one, top to bottom."""
    ys = tuple(top + offset for offset in (280.0, 300.0, 320.0, 340.0))
    return PageSpec(
        items=(
            TextRun("Parts list", 72.0, top, size=18.0),
            *body_text(top + 30.0),
            RasterImage(72.0, top + 90.0, 128.0, 64.0, PNG),
            *lattice_rules(LATTICE_XS, ys),
            *written(PARTS, LATTICE_XS, ys),
            *table_runs(PARTS, top=top + 420.0),
        )
    )


def tiny_text_page() -> PageSpec:
    """Prose and one text object at 0.002 pt, which pdfium's textpage drops (recovery reads it back)."""
    return PageSpec(items=(*body_text(100.0), TextRun("HIDDEN", 72.0, 300.0, size=0.002)))


def chain_page(path, index: int = 0, *, ocr="auto", password=None, min_image_dimension=40) -> Page:
    """The per-stage chain, run as the stages' own tests run it: A2, recovery, A3, A4a, A4b."""
    with open_document(path, password=password) as handle:
        raw = extract_page(handle, index)
        recovered = recover_dropped_text(handle, index) if raw.text.stats.dropped_text_objects else None
        assessment = assess_page(raw, ocr=ocr, recovered=recovered)
        lattice = find_lattice_tables(raw.segments.segments, assessment.pieces)
        return assemble_page(
            raw, assessment, lattice, source=path, password=password, min_image_dimension=min_image_dimension
        )


def page_at(path, index: int = 0, *, password=None, **options) -> Page:
    """extract_page_at of page ``index`` of ``path``."""
    return extract_page_at(PDFSource(path=path, password=password), index, PipelineOptions(**options))


def spy(monkeypatch, name: str) -> list:
    """Record each call of the pipeline's ``name`` (its arguments) and let it run."""
    calls: list = []
    real = getattr(pipeline, name)

    def recorded(*args, **kwargs):
        calls.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(pipeline, name, recorded)
    return calls


def raising(error: BaseException):
    """A stage stand-in that raises ``error``."""

    def stage(*args, **kwargs):
        raise error

    return stage


def error_page(page_number: int, error: str, *, size=(612.0, 792.0), warnings=()) -> Page:
    return Page(
        page_number=page_number,
        width_pt=size[0],
        height_pt=size[1],
        status="error",
        route="vision",
        quality=None,
        error=error,
        warnings=warnings,
    )


GOOD = {
    "workers": 1,
    "ocr": "auto",
    "dpi": 200,
    "vision_dpi": 300,
    "max_render_pixels": 16_000_000,
    "min_image_dimension": 40,
    "strict": False,
}


class TestPipelineOptions:
    """The spec's construction checks (invariant 2), each with its exact message."""

    def test_the_defaults_are_the_specs(self):
        assert PipelineOptions() == PipelineOptions(**GOOD)

    @pytest.mark.parametrize(
        "change,message",
        [
            pytest.param({"workers": 0}, "workers must be >= 1, got 0", id="workers"),
            pytest.param({"dpi": 35}, "dpi must be >= 36, got 35", id="dpi"),
            pytest.param({"vision_dpi": 199}, "vision_dpi must be >= dpi (200), got 199", id="vision-under-dpi"),
            pytest.param({"dpi": 36, "vision_dpi": 35}, "vision_dpi must be >= 36, got 35", id="vision-floor"),
            pytest.param(
                {"max_render_pixels": 262_143}, "max_render_pixels must be >= 262144, got 262143", id="pixels"
            ),
            pytest.param({"min_image_dimension": 0}, "min_image_dimension must be >= 1, got 0", id="min-image"),
            pytest.param({"ocr": "sometimes"}, "ocr must be 'auto' or 'always' or 'never', got 'sometimes'", id="ocr"),
        ],
    )
    def test_a_bad_value_raises_value_error(self, change, message):
        with pytest.raises(ValueError, match=re.escape(message)):
            PipelineOptions(**{**GOOD, **change})

    @pytest.mark.parametrize(
        "change,message",
        [
            pytest.param({"workers": 2.0}, "workers must be an int, got 2.0 (float)", id="workers"),
            pytest.param({"workers": True}, "workers must be an int, got True (bool)", id="workers-bool"),
            pytest.param({"dpi": "200"}, "dpi must be an int, got '200' (str)", id="dpi"),
            pytest.param({"vision_dpi": 300.0}, "vision_dpi must be an int, got 300.0 (float)", id="vision"),
            pytest.param(
                {"max_render_pixels": None}, "max_render_pixels must be an int, got None (NoneType)", id="pixels"
            ),
            pytest.param(
                {"min_image_dimension": 40.0}, "min_image_dimension must be an int, got 40.0 (float)", id="min-image"
            ),
            pytest.param({"ocr": None}, "ocr must be a str, got None (NoneType)", id="ocr"),
            pytest.param({"strict": 1}, "strict must be a bool, got 1 (int)", id="strict"),
        ],
    )
    def test_a_bad_type_raises_type_error(self, change, message):
        with pytest.raises(TypeError, match=re.escape(message)):
            PipelineOptions(**{**GOOD, **change})

    @pytest.mark.parametrize(
        "change",
        [
            pytest.param({"workers": 1, "dpi": 36, "vision_dpi": 36}, id="floors"),
            pytest.param({"max_render_pixels": 262_144, "min_image_dimension": 1}, id="pixel-floors"),
            pytest.param({"ocr": "always", "strict": True}, id="always-strict"),
            pytest.param({"ocr": "never"}, id="never"),
        ],
    )
    def test_values_at_the_bounds_are_accepted(self, change):
        options = PipelineOptions(**{**GOOD, **change})

        assert {name: getattr(options, name) for name in change} == change

    def test_options_are_frozen_and_picklable(self):
        options = PipelineOptions(workers=4, ocr="never", strict=True)

        assert pickle.loads(pickle.dumps(options)) == options
        with pytest.raises(AttributeError):
            options.workers = 2  # type: ignore[misc]


class TestPDFSource:
    """Where a worker opens the document: an absolute path, and the password (never shown)."""

    def test_a_relative_path_is_made_absolute(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        assert PDFSource(path="doc.pdf").path == tmp_path / "doc.pdf"
        assert PDFSource(path=Path("doc.pdf")).path.is_absolute()

    def test_the_password_is_kept_but_never_shown_or_compared(self, tmp_path):
        source = PDFSource(path=tmp_path / "doc.pdf", password="hunter2")

        assert source.password == "hunter2"
        assert "hunter2" not in repr(source)
        assert source == PDFSource(path=tmp_path / "doc.pdf", password="other")
        assert pickle.loads(pickle.dumps(source)).password == "hunter2"

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"path": b"doc.pdf"}, TypeError, "source must be a str or os.PathLike, got bytes", id="bytes"),
            pytest.param({"path": 3}, TypeError, "source must be a str or os.PathLike, got int", id="int"),
            pytest.param(
                {"path": "doc.pdf", "password": 1234}, TypeError, "password must be a str or None, got int", id="pw"
            ),
            pytest.param(
                {"path": "doc.pdf", "password": "a\x00b"},
                ValueError,
                "password must not contain NUL: pdfium would read only the part before it",
                id="pw-nul",
            ),
        ],
    )
    def test_bad_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            PDFSource(**fields)

    def test_a_password_error_never_shows_the_password(self):
        with pytest.raises(TypeError) as caught:
            PDFSource(path=os.devnull, password=b"s3cret")

        assert "s3cret" not in str(caught.value)


class TestExtractPageAt:
    """Steps 1-4 for one page: A2, recovery when the page drops text, A3, then the route's assembly."""

    @pytest.mark.parametrize("rotate", [0, 90])
    def test_a_page_is_the_per_stage_chains_page(self, tmp_path, rotate):
        spec = mixed_page()
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(items=spec.items, rotate=rotate)])

        page = page_at(path, min_image_dimension=32)

        assert page == chain_page(path, min_image_dimension=32)
        # Not vacuous: the page has text, a figure and both kinds of table.
        assert {type(element).__name__ for element in page.elements} >= {"Heading", "Paragraph", "Figure", "Table"}
        assert page.status == "ok"
        assert page.integrity_verified

    def test_dropped_text_is_recovered_on_exactly_the_pages_that_drop_some(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(items=body_text(100.0)), tiny_text_page()])
        recoveries = spy(monkeypatch, "recover_dropped_text")

        pages = [page_at(path, index) for index in (0, 1)]

        assert [args[1] for args in recoveries] == [1]
        assert pages == [chain_page(path, 0), chain_page(path, 1)]
        (hidden,) = [element for element in pages[1].elements if element.is_hidden]
        assert isinstance(hidden, Paragraph)
        assert (hidden.text, hidden.hidden_reasons) == ("HIDDEN", ("tiny_font",))
        assert not [warning for warning in pages[1].warnings if "not recovered" in warning]

    def test_a_vision_page_takes_the_vision_stand_in(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        visions = spy(monkeypatch, "_vision_page")

        page = page_at(path, ocr="always")

        assert len(visions) == 1
        assert page == chain_page(path, ocr="always")
        assert (page.route, page.status, page.integrity_verified) == ("vision", "ocr_unavailable", False)
        assert _VISION_NOTE in page.warnings

    @pytest.mark.parametrize("ocr", ["auto", "never"])
    def test_a_digital_page_never_takes_the_vision_stand_in(self, tmp_path, monkeypatch, ocr):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        visions = spy(monkeypatch, "_vision_page")

        page = page_at(path, ocr=ocr)

        assert visions == []
        assert page.route in ("digital", "hybrid")

    @pytest.mark.parametrize("minimum,figures", [(32, 1), (33, 0)])
    def test_min_image_dimension_reaches_assembly(self, tmp_path, minimum, figures):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])

        page = page_at(path, min_image_dimension=minimum)

        assert len([element for element in page.elements if isinstance(element, Figure)]) == figures

    def test_an_encrypted_page_is_read_with_its_password(self, tmp_path):
        path = build_pdf(tmp_path / "enc.pdf", [mixed_page()], user_password="pw")

        page = page_at(path, password="pw", min_image_dimension=32)

        assert page == chain_page(path, password="pw", min_image_dimension=32)
        (figure,) = [element for element in page.elements if isinstance(element, Figure)]
        assert figure.read_raw_bytes()  # the loader reopens the file with the password

    @pytest.mark.parametrize("password,message", [(None, "pass its password"), ("nope", "password is incorrect")])
    def test_an_encrypted_page_without_its_password_raises(self, tmp_path, password, message):
        path = build_pdf(tmp_path / "enc.pdf", [mixed_page()], user_password="pw")

        with pytest.raises(PDFEncryptedError, match=message):
            page_at(path, password=password)

    def test_a_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            page_at(tmp_path / "missing.pdf")

    @pytest.mark.parametrize(
        "index,error,message",
        [
            pytest.param(1, IndexError, "index must be in 0..0, got 1", id="past"),
            pytest.param(-1, IndexError, "index must be in 0..0, got -1", id="negative"),
            pytest.param(True, TypeError, "index must be an int, got True (bool)", id="bool"),
        ],
    )
    def test_a_bad_index_raises_and_is_no_error_page(self, tmp_path, index, error, message):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])

        with pytest.raises(error, match=re.escape(message)):
            page_at(path, index)

    @pytest.mark.parametrize(
        "source,options,message",
        [
            pytest.param("doc.pdf", PipelineOptions(), "handle_source must be a PDFSource, got str", id="source"),
            pytest.param(None, {"strict": True}, "options must be a PipelineOptions, got dict", id="options"),
        ],
    )
    def test_the_arguments_are_checked(self, tmp_path, source, options, message):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])

        with pytest.raises(TypeError, match=re.escape(message)):
            extract_page_at(source or PDFSource(path=path), 0, options)

    @pytest.mark.parametrize("strict", [False, True])
    def test_the_document_is_closed_on_every_path(self, tmp_path, monkeypatch, strict):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()]), 2)
        handles = []
        real_open = pipeline.open_document
        monkeypatch.setattr(
            pipeline, "open_document", lambda *a, **k: handles.append(real_open(*a, **k)) or handles[-1]
        )

        page_at(path, 0, strict=strict)
        if strict:
            with pytest.raises(PDFCorruptError):
                page_at(path, 1, strict=strict)
        else:
            page_at(path, 1, strict=strict)

        assert len(handles) == 2
        assert all(handle.closed for handle in handles)

    def test_a_page_and_its_figures_survive_a_pickle(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        page = page_at(path, min_image_dimension=32)

        # The test's own page: pickle is how pages cross the process pool.
        copy = pickle.loads(pickle.dumps(page))

        assert copy == page
        (figure,) = [element for element in copy.elements if isinstance(element, Figure)]
        (original,) = [element for element in page.elements if isinstance(element, Figure)]
        assert figure.read_raw_bytes() == original.read_raw_bytes()
        assert figure.read_bytes() == original.read_bytes()
        assert figure.read_bytes().startswith(b"\x89PNG")


class TestPageIsolation:
    """Step 5: a page that fails is an error page, never dropped; with ``strict`` it raises, naming the page."""

    def test_a_corrupt_page_is_an_error_page_and_its_neighbours_extract(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page(), mixed_page()]), 2)
        message = "page 2 is unreadable: Failed to load page."

        pages = [page_at(path, index) for index in (0, 1, 2)]

        size_note = f"the page's size could not be read (PDFCorruptError: {message}){DEFAULT_SIZE_NOTE}"
        assert pages[1] == error_page(2, f"PDFCorruptError: {message} (stage 'page')", warnings=(size_note,))
        assert [pages[0], pages[2]] == [chain_page(path, 0), chain_page(path, 2)]

    def test_strict_raises_a_corrupt_page_naming_it(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()]), 2)

        with pytest.raises(PDFCorruptError, match=re.escape("page 2 is unreadable: Failed to load page.")) as caught:
            page_at(path, 1, strict=True)

        assert caught.value.page_number == 2
        assert page_at(path, 0, strict=True) == chain_page(path, 0)

    def test_a_failed_text_stage_is_an_error_page_of_the_pages_own_size(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(width=400.0, height=300.0, items=body_text(100.0))])
        monkeypatch.setattr(pdfium_c, "FPDFText_CountChars", failing(-1))

        page = page_at(path)

        message = "page 1: pdfium cannot count the text page's chars"
        assert page == error_page(1, f"PDFCorruptError: {message} (stage 'text')", size=(400.0, 300.0))

    @pytest.mark.parametrize(
        "stage", ["extract_page", "recover_dropped_text", "assess_page", "find_lattice_tables", "assemble_page"]
    )
    def test_a_stage_that_raises_is_an_error_page_with_its_type_and_message(self, tmp_path, monkeypatch, stage):
        path = build_pdf(tmp_path / "doc.pdf", [tiny_text_page(), mixed_page()])
        monkeypatch.setattr(pipeline, stage, raising(RuntimeError(f"{stage} exploded")))

        first, second = page_at(path, 0), page_at(path, 1)

        assert first == error_page(1, f"RuntimeError: {stage} exploded")
        if stage == "recover_dropped_text":  # page 2 drops no text: recovery never runs there
            assert second == chain_page(path, 1)
        else:
            assert second == error_page(2, f"RuntimeError: {stage} exploded")

    def test_strict_raises_a_stage_error_as_a_corrupt_page_naming_it(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        cause = RuntimeError("lattice exploded")
        monkeypatch.setattr(pipeline, "find_lattice_tables", raising(cause))

        with pytest.raises(PDFCorruptError, match=re.escape("page 1: RuntimeError: lattice exploded")) as caught:
            page_at(path, strict=True)

        assert caught.value.page_number == 1
        assert caught.value.__cause__ is cause

    def test_strict_keeps_a_corrupt_error_that_already_names_the_page(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        error = PDFCorruptError("page 1: pdfium cannot read the lattice", page_number=1)
        monkeypatch.setattr(pipeline, "find_lattice_tables", raising(error))

        with pytest.raises(PDFCorruptError) as caught:
            page_at(path, strict=True)

        assert caught.value is error

    @pytest.mark.parametrize("named", [None, 3])
    def test_strict_names_the_page_of_a_corrupt_error_that_does_not(self, tmp_path, monkeypatch, named):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        error = PDFCorruptError("pdfium cannot read the lattice", page_number=named)
        monkeypatch.setattr(pipeline, "find_lattice_tables", raising(error))

        with pytest.raises(PDFCorruptError) as caught:
            page_at(path, strict=True)

        assert (str(caught.value), caught.value.page_number) == (
            "page 1: PDFCorruptError: pdfium cannot read the lattice",
            1,
        )
        assert caught.value.__cause__ is error

    def test_a_size_that_cannot_be_read_for_any_reason_still_makes_the_error_page(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        monkeypatch.setattr(pipeline, "find_lattice_tables", raising(RuntimeError("lattice exploded")))
        monkeypatch.setattr(pipeline, "page_geometry", raising(ValueError("geometry exploded")))

        page = page_at(path)

        note = f"the page's size could not be read (ValueError: geometry exploded){DEFAULT_SIZE_NOTE}"
        assert page == error_page(1, "RuntimeError: lattice exploded", warnings=(note,))

    @pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(3)])
    def test_an_interrupt_is_never_recorded_as_an_error_page(self, tmp_path, monkeypatch, error):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        monkeypatch.setattr(pipeline, "find_lattice_tables", raising(error))

        with pytest.raises(type(error)):
            page_at(path)
