"""Tests for the v5 document pipeline (#372, A8a): ``datagrunt.pdf.pipeline``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``): a
corrupt page with ``break_page``, an encrypted PDF with ``user_password``,
and a split table with A4b's own table fixtures. A stage that raises is a
``monkeypatch`` of the pipeline's module name for it.
"""

import dataclasses
import hashlib
import io
import json
import os
import pickle
import re
import subprocess
import sys
import time
from pathlib import Path

import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import DatagruntPDFError, PDFCorruptError, PDFEncryptedError, _process, pipeline
from datagrunt.pdf.assembly import _VISION_NOTE, assemble_page
from datagrunt.pdf.elements import DocumentMetadata, Figure, KeyValue, Page, Paragraph, PDFDocument
from datagrunt.pdf.exceptions import PDFSourceChangedError
from datagrunt.pdf.layout import find_lattice_tables
from datagrunt.pdf.pdfium_engine import PageFailure, extract_page, open_document, recover_dropped_text
from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, SourceIdentity, extract_document, extract_page_at
from datagrunt.pdf.quality import assess_page
from datagrunt.pdf.scope import apply_scope
from datagrunt.pdf.vision import VisionOptions, vision_page
from pdf_tests.builders import PageSpec, RasterImage, TextRun, break_page, build_pdf, solid_png
from pdf_tests.test_layout_lattice import rules as lattice_rules
from pdf_tests.test_layout_lattice import written
from pdf_tests.test_layout_stream_tables import body_text, table_runs
from pdf_tests.test_ocr import needs_tesseract
from pdf_tests.test_ocr_scans import LINES, TOKENS, png_of, rendered
from pdf_tests.test_pdfium_extract_page import failing
from pdf_tests.test_routing import BROKEN_LINE, MAPPED_LINE, garbled, garbled_page

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


def vision_chain_page(path, index: int = 0, *, ocr="auto", password=None, min_image_dimension=40) -> Page:
    """A vision page by the per-stage chain: A2, recovery, A3, then A5b's vision_page."""
    with open_document(path, password=password) as handle:
        raw = extract_page(handle, index)
        recovered = recover_dropped_text(handle, index) if raw.text.stats.dropped_text_objects else None
    assessment = assess_page(raw, ocr=ocr, recovered=recovered)
    return vision_page(raw, assessment, source=path, password=password, min_image_dimension=min_image_dimension)


def keyword_spy(monkeypatch, name: str) -> list:
    """Record each call of the pipeline's ``name`` (its positional and keyword arguments) and let it run."""
    calls: list = []
    real = getattr(pipeline, name)

    def recorded(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(pipeline, name, recorded)
    return calls


def no_tesseract(monkeypatch, tmp_path) -> None:
    """PATH holds no tesseract."""
    empty = tmp_path / "empty-path"
    empty.mkdir(exist_ok=True)
    monkeypatch.setenv("PATH", str(empty))


def element_texts(page: Page) -> list[str]:
    """The texts of the page's visible text elements."""
    return [element.text for element in page.elements if not element.is_hidden and hasattr(element, "text")]


def unloadable(message: str) -> str:
    """A5b's error for a page pdfium cannot load: its geometry cannot be read either, so it is not rendered."""
    return f"extraction failed at stage 'page': {message}; its geometry cannot be read either: {message}"


UNAVAILABLE = "tesseract is unavailable: no tesseract in an absolute PATH directory"
TEXT_FAILURE = "page 1: pdfium cannot count the text page's chars"


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


def split_table_pages() -> list[PageSpec]:
    """A ruled table ending page 1 and continuing at the top of page 2, its columns on the same rules."""
    tail_ys, head_ys = (600.0, 620.0, 640.0, 660.0), (60.0, 80.0, 100.0)
    tail = (*lattice_rules(LATTICE_XS, tail_ys), *written(PARTS, LATTICE_XS, tail_ys))
    rest = (("Washer", "100", "0.01"), ("Pin", "8", "0.02"))
    head = (*lattice_rules(LATTICE_XS, head_ys), *written(rest, LATTICE_XS, head_ys))
    return [
        PageSpec(items=(TextRun("Parts list", 72.0, 60.0, size=18.0), *body_text(100.0), *tail)),
        PageSpec(items=(*head, *body_text(300.0))),
    ]


def chain_document(path, *, password=None, title=None, author=None, **options) -> PDFDocument:
    """The per-stage chain's pages, A6's scope over them and the A1 document they make."""
    with open_document(path, password=password) as handle:
        count = handle.page_count
    scoped = apply_scope(tuple(chain_page(path, index, password=password, **options) for index in range(count)))
    digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
    name = Path(path).name
    metadata = DocumentMetadata(
        doc_id=DocumentMetadata.make_doc_id(name, digest),
        source_name=name,
        sha256=digest,
        page_count=count,
        title=title,
        author=author,
    )
    return PDFDocument(
        metadata=metadata, pages=scoped.pages, sections=scoped.sections, header_context=scoped.header_context
    )


def opened_handles(monkeypatch) -> list:
    """Record every handle the pipeline opens."""
    handles: list = []
    real_open = pipeline.open_document

    def recorded(*args, **kwargs):
        handles.append(real_open(*args, **kwargs))
        return handles[-1]

    monkeypatch.setattr(pipeline, "open_document", recorded)
    return handles


# The environment variables v4 read as a distributed runner (Spark, Beam, Flink, Celery).
DISTRIBUTED_KEYS = ("SPARK_ENV_LOADED", "SPARK_HOME", "BEAM_WORKER_ID", "FLINK_CONF_DIR", "CELERY_BROKER_URL")


# Page workers for tests: module level, so a worker started by the bootstrap imports them from this module by name.
def holds_page_3(message) -> bool:
    start, stop = message["pages"]
    return start <= 2 < stop


def crash_on_page_3(message, replies):
    """The worker holding page 3 dies at once (``os._exit``: no exception, no reply); the rest are the pipeline's."""
    if holds_page_3(message):
        os._exit(7)
    pipeline._worker_main(message, replies)


def complain_and_crash_on_page_3(message, replies):
    """The worker holding page 3 writes to its stderr, then dies; the rest are the pipeline's."""
    if holds_page_3(message):
        sys.stderr.write("pdfium: boom\n")
        sys.stderr.flush()
        os._exit(3)
    pipeline._worker_main(message, replies)


def hang_on_page_3(message, replies):
    """The worker holding page 3 never replies; the rest are the pipeline's."""
    if holds_page_3(message):
        time.sleep(60)
    pipeline._worker_main(message, replies)


def extracted(message):
    """The pages the pipeline's worker would send for ``message``."""
    return pipeline._extract_chunk(*pipeline._chunk_request(message))


def send_pages_reversed(message, replies):
    """A worker that replies with its own pages, last first, then done."""
    for page in reversed(extracted(message)):
        _process.send_message(replies, b"P" + pickle.dumps(page))
    _process.send_message(replies, b"D")


def send_a_tuple_as_a_page(message, replies):
    """A worker whose page reply is not a page."""
    _process.send_message(replies, b"P" + pickle.dumps(("page 1",)))


def send_more_pages(message, replies):
    """A worker that sends its pages and then one more instead of done."""
    pages = extracted(message)
    for page in (*pages, pages[-1]):
        _process.send_message(replies, b"P" + pickle.dumps(page))


def send_an_unknown_kind(message, replies):
    """A worker whose reply is of a kind the parent does not know."""
    _process.send_message(replies, b"Z what")


def send_a_str_as_an_error(message, replies):
    """A worker whose error reply holds no exception."""
    _process.send_message(replies, b"E" + pickle.dumps("not an exception"))


def swap_after_first_chunk(message, replies):
    """The first chunk's worker replaces the source with ``other.pdf`` beside it once its pages are extracted."""
    if message["pages"][0] == 0:
        real, source = pipeline._pages_in, Path(message["source"])

        def then_swap(*args):
            pages = real(*args)
            os.replace(source.with_name("other.pdf"), source)
            return pages

        pipeline._pages_in = then_swap  # this worker process only: it ends with the chunk
    pipeline._worker_main(message, replies)


def run_page_workers_as(monkeypatch, function: str) -> None:
    """Point the bootstrap's ``"pages"`` key at ``function`` of this module: its allowlist, edited for this test."""
    entry = repr(_process.WORKERS["pages"])
    assert _process.BOOTSTRAP.count(entry) == 1
    monkeypatch.setattr(_process, "BOOTSTRAP", _process.BOOTSTRAP.replace(entry, repr((__name__, function))))


NO_GUARD_SCRIPT = """
import sys

from datagrunt.pdf.pipeline import extract_document

with open(sys.argv[2], "a") as marker:
    marker.write("top level ran\\n")
document = extract_document(sys.argv[1], workers=2)
print(f"extracted {len(document.pages)} pages: {[page.status for page in document.pages]}")
"""


def identity_of(path) -> SourceIdentity:
    status = os.stat(path)
    return SourceIdentity(device=status.st_dev, inode=status.st_ino, size=status.st_size, mtime_ns=status.st_mtime_ns)


def append_in_place(path) -> None:
    """Append bytes to ``path`` (the same inode) and move its mtime on a second, whatever the clock's grain."""
    before = os.stat(path)
    with open(path, "ab") as stream:
        stream.write(b"\n% appended\n")
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))


def changed(path, when: str, before: SourceIdentity, after: SourceIdentity, *fields: str) -> str:
    """The PDFSourceChangedError message for ``fields`` (``inode``, ``size``, ``mtime``) changed ``when``."""
    words = {
        "inode": f"inode {before.device}:{before.inode} -> {after.device}:{after.inode}",
        "size": f"size {before.size} -> {after.size} bytes",
        "mtime": f"mtime {before.mtime_ns} -> {after.mtime_ns} ns",
    }
    found = ", ".join(words[name] for name in fields)
    return f"{path} changed {when}: {found}; its pages and its sha256 could disagree, so it is not extracted"


def after_hashing(monkeypatch, change) -> None:
    """Run ``change()`` once the source is hashed, before pdfium opens it."""
    real = pipeline._stream_sha256

    def hashed(*args):
        digest = real(*args)
        change()
        return digest

    monkeypatch.setattr(pipeline, "_stream_sha256", hashed)


def started_workers(monkeypatch) -> list:
    """Record each worker process the pool starts, by its pid."""
    started: list = []
    real = pipeline._started

    def recorded(*args, **kwargs):
        worker = real(*args, **kwargs)
        started.append(worker.child.pid)
        return worker

    monkeypatch.setattr(pipeline, "_started", recorded)
    return started


def assert_reaped(pids) -> None:
    """None of ``pids`` exists: each worker ended and was reaped, not left running or a zombie."""
    assert pids
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


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

    @needs_tesseract
    def test_a_vision_page_is_read_by_ocr(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        visions = spy(monkeypatch, "vision_page")

        page = page_at(path, ocr="always")

        assert len(visions) == 1
        assert page == vision_chain_page(path, ocr="always")
        assert (page.route, page.status, page.integrity_verified) == ("vision", "ok", False)
        assert page.quality.ocr_mean_confidence is not None and page.page_image is not None
        assert {"Parts", "list"} <= set(" ".join(element_texts(page)).split())
        assert _VISION_NOTE not in page.warnings

    def test_a_vision_page_without_tesseract_is_ocr_unavailable(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        no_tesseract(monkeypatch, tmp_path)

        page = page_at(path, ocr="always")

        digital = chain_page(path, ocr="always")
        assert (page.route, page.status, page.integrity_verified) == ("vision", "ocr_unavailable", False)
        assert page.elements == digital.elements
        assert page.warnings == (*digital.warnings, f"OCR could not read the page: {UNAVAILABLE}")
        assert _VISION_NOTE in page.warnings and page.page_image is not None

    @pytest.mark.parametrize("ocr", ["auto", "never"])
    def test_a_digital_page_never_takes_the_vision_route(self, tmp_path, monkeypatch, ocr):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        visions = spy(monkeypatch, "vision_page")

        page = page_at(path, ocr=ocr)

        assert visions == []
        assert page.route in ("digital", "hybrid")

    def test_the_vision_route_takes_the_pipelines_options(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "enc.pdf", [mixed_page()], user_password="pw")
        calls = keyword_spy(monkeypatch, "vision_page")
        no_tesseract(monkeypatch, tmp_path)

        page_at(path, password="pw", ocr="always", vision_dpi=250, max_render_pixels=1_000_000, min_image_dimension=32)

        ((args, kwargs),) = calls
        assert [type(arg).__name__ for arg in args] == ["RawPage", "PageAssessment"]
        assert kwargs == {
            "source": path,
            "password": "pw",
            "min_image_dimension": 32,
            "options": VisionOptions(vision_dpi=250, max_render_pixels=1_000_000),
        }

    @pytest.mark.parametrize("ocr", ["auto", "always", "never"])
    def test_the_fallback_route_takes_the_pipelines_options(self, tmp_path, monkeypatch, ocr):
        path = build_pdf(tmp_path / "enc.pdf", [PageSpec(items=body_text(100.0))], user_password="pw")
        monkeypatch.setattr(pdfium_c, "FPDFText_CountChars", failing(-1))
        calls = keyword_spy(monkeypatch, "fallback_page")
        no_tesseract(monkeypatch, tmp_path)

        page_at(path, password="pw", ocr=ocr, vision_dpi=250, max_render_pixels=1_000_000, min_image_dimension=32)

        ((args, kwargs),) = calls
        assert [type(arg).__name__ for arg in args] == ["DocumentHandle", "PageFailure"]
        assert kwargs == {
            "source": path,
            "password": "pw",
            "ocr": ocr,
            "options": VisionOptions(vision_dpi=250, max_render_pixels=1_000_000),
        }

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
        assert pages[1] == error_page(2, unloadable(message), warnings=(size_note,))
        assert [pages[0], pages[2]] == [chain_page(path, 0), chain_page(path, 2)]

    def test_strict_raises_a_corrupt_page_naming_it(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()]), 2)

        with pytest.raises(PDFCorruptError, match=re.escape("page 2 is unreadable: Failed to load page.")) as caught:
            page_at(path, 1, strict=True)

        assert caught.value.page_number == 2
        assert page_at(path, 0, strict=True) == chain_page(path, 0)

    @needs_tesseract
    def test_a_failed_text_stage_is_read_by_ocr_of_its_render(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(width=400.0, height=300.0, items=body_text(100.0))])
        monkeypatch.setattr(pdfium_c, "FPDFText_CountChars", failing(-1))
        fallbacks = spy(monkeypatch, "fallback_page")

        page = page_at(path)

        assert len(fallbacks) == 1
        assert (page.route, page.status, page.integrity_verified) == ("vision", "ocr_fallback", False)
        assert (page.width_pt, page.height_pt) == (400.0, 300.0)
        assert {"woodland", "creatures", "ambitions."} <= set(" ".join(element_texts(page)).split())
        assert page.warnings[0].startswith(
            f"extraction failed at stage 'text': {TEXT_FAILURE}; the page was read by OCR"
        )

    def test_a_failed_text_stage_without_tesseract_is_an_error_page_of_the_pages_own_size(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(width=400.0, height=300.0, items=body_text(100.0))])
        monkeypatch.setattr(pdfium_c, "FPDFText_CountChars", failing(-1))
        no_tesseract(monkeypatch, tmp_path)

        page = page_at(path)

        why = f"extraction failed at stage 'text': {TEXT_FAILURE}; OCR could not read the page: {UNAVAILABLE}"
        assert dataclasses.replace(page, page_image=None) == error_page(1, why, size=(400.0, 300.0))
        assert page.page_image is not None  # its render was made: OCR could not read it

    def test_a_failed_page_reads_nothing_by_ocr_under_ocr_never(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(width=400.0, height=300.0, items=body_text(100.0))])
        monkeypatch.setattr(pdfium_c, "FPDFText_CountChars", failing(-1))

        page = page_at(path, ocr="never")

        why = f"extraction failed at stage 'text': {TEXT_FAILURE}; ocr='never', so the page is not read by OCR"
        assert page == error_page(1, why, size=(400.0, 300.0))
        assert page.page_image is None

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

    @pytest.mark.parametrize("route", ["vision_page", "fallback_page"])
    def test_an_ocr_route_that_raises_is_isolated_as_any_step(self, tmp_path, monkeypatch, route):
        path = build_pdf(tmp_path / "doc.pdf", [PageSpec(width=400.0, height=300.0, items=body_text(100.0))])
        if route == "fallback_page":
            monkeypatch.setattr(pdfium_c, "FPDFText_CountChars", failing(-1))
        cause = RuntimeError(f"{route} exploded")
        monkeypatch.setattr(pipeline, route, raising(cause))

        page = page_at(path, ocr="always")

        assert page == error_page(1, f"RuntimeError: {route} exploded", size=(400.0, 300.0))
        if route == "vision_page":  # under strict a failed page raises before its fallback: see the corrupt-page tests
            with pytest.raises(PDFCorruptError, match=re.escape(f"page 1: RuntimeError: {route} exploded")) as caught:
                page_at(path, ocr="always", strict=True)
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


@pytest.fixture(scope="module")
def scan_png(tmp_path_factory) -> bytes:
    """A letter page of four sentences (A5a's scan fixture), rendered at 300 DPI, as PNG: a scan with no text layer."""
    digital = build_pdf(tmp_path_factory.mktemp("scan") / "digital.pdf", [PageSpec(items=LINES)])
    return png_of(rendered(digital))


def scan_page(png: bytes, *items) -> PageSpec:
    """A letter page of the scan, then ``items`` drawn on it."""
    return PageSpec(items=(RasterImage(0.0, 0.0, 612.0, 792.0, png), *items))


def failing_at(monkeypatch, index: int) -> None:
    """A2's extraction of the page at ``index`` fails at its text stage, as a corrupt textpage's does."""
    real = pipeline.extract_page

    def extract(handle, at):
        if at == index:
            return PageFailure(page_number=at + 1, stage="text", message=f"page {at + 1}: {TEXT_FAILURE[8:]}")
        return real(handle, at)

    monkeypatch.setattr(pipeline, "extract_page", extract)


def read_tokens(page: Page) -> set[str]:
    return set(" ".join(element_texts(page)).split())


class TestVisionEndToEnd:
    """extract_document reads a scanned page by OCR, a page whose extraction raises by OCR of its render."""

    @needs_tesseract
    def test_a_scan_and_a_failed_page_are_read_by_ocr_and_the_rest_extract(self, tmp_path, monkeypatch, scan_png):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page(), scan_page(scan_png), PageSpec(items=LINES)])
        failing_at(monkeypatch, 2)

        document = extract_document(path, min_image_dimension=32)

        digital, scanned, failed = document.pages
        assert digital == apply_scope((chain_page(path, 0, min_image_dimension=32),)).pages[0]
        assert (scanned.status, scanned.route, scanned.integrity_verified) == ("ok", "vision", False)
        assert len(set(TOKENS) & read_tokens(scanned)) >= 0.9 * len(set(TOKENS))
        assert (failed.status, failed.route, failed.integrity_verified) == ("ocr_fallback", "vision", False)
        assert len(set(TOKENS) & read_tokens(failed)) >= 0.9 * len(set(TOKENS))
        assert document.degraded_pages == (3,)
        # A1 verifies a document over its pages that can be (a vision page never is), and none is an error page.
        assert document.integrity_verified

    @needs_tesseract
    def test_a_page_of_mostly_garbled_text_is_read_by_ocr_and_its_digital_text_superseded(self, tmp_path):
        # 20 valid chars, but 27 of its 47 visible chars valid: the ratio routes it vision (#417).
        document = extract_document(garbled_page(tmp_path))

        (page,) = document.pages
        assert (page.status, page.route, page.integrity_verified) == ("ok", "vision", False)
        assert page.quality.valid_char_ratio == pytest.approx(27 / 47)
        hidden = [element for element in page.elements if element.is_hidden]
        assert {element.hidden_reasons for element in hidden} == {("superseded",)}
        digital = f"{MAPPED_LINE} {garbled(BROKEN_LINE)}".split()
        assert " ".join(element.text for element in hidden).split() == digital
        # What OCR read, not the garbled text layer, is the page's visible text.
        assert read_tokens(page) == set(f"{MAPPED_LINE} {BROKEN_LINE}".split())

    def test_without_tesseract_a_scan_is_ocr_unavailable_and_a_failed_page_an_error(
        self, tmp_path, monkeypatch, scan_png
    ):
        caption = TextRun("Page 7", 500.0, 760.0, size=10.0)
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page(), scan_page(scan_png, caption), PageSpec(items=LINES)])
        failing_at(monkeypatch, 2)
        no_tesseract(monkeypatch, tmp_path)

        document = extract_document(path, min_image_dimension=32)

        digital, scanned, failed = document.pages
        assert digital.status == "ok"
        assert (scanned.status, scanned.route) == ("ocr_unavailable", "vision")
        assert element_texts(scanned) == ["Page 7"]
        assert scanned.warnings[-1] == f"OCR could not read the page: {UNAVAILABLE}"
        assert (failed.status, failed.elements, failed.quality) == ("error", (), None)
        assert failed.error.endswith(f"; OCR could not read the page: {UNAVAILABLE}")
        assert document.degraded_pages == (2, 3)


class TestExtractDocument:
    """Every page's steps in page order, then A6's scope pass, then the A1 document (steps 6-7)."""

    def test_a_document_is_its_pages_scoped(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page(), *split_table_pages()], title="Parts", author="Ada")

        document = extract_document(path, min_image_dimension=32)

        assert document == chain_document(path, title="Parts", author="Ada", min_image_dimension=32)
        # Not vacuous: the pages hold elements and the scope pass set paths and sections.
        assert [page.page_number for page in document.pages] == [1, 2, 3]
        assert document.sections and all(page.elements for page in document.pages)
        assert document.integrity_verified

    def test_a_table_split_across_two_pages_is_linked(self, tmp_path):
        path = build_pdf(tmp_path / "split.pdf", split_table_pages())

        document = extract_document(path)

        tail, head = document.tables
        assert (tail.page_number, head.page_number) == (1, 2)
        assert head.continued_from_id == tail.element_id
        assert tail.headers == ("Part", "Qty", "Price")

    def test_the_header_context_is_page_ones_leading_key_values(self, tmp_path):
        ys = (200.0, 220.0, 240.0, 260.0)
        items = (
            TextRun("Invoice: 10442", 72.0, 60.0, size=10.0),
            *body_text(100.0),
            *lattice_rules(LATTICE_XS, ys),
            *written(PARTS, LATTICE_XS, ys),
        )
        path = build_pdf(tmp_path / "invoice.pdf", [PageSpec(items=items)])

        document = extract_document(path)

        (context,) = document.header_context
        assert isinstance(context, KeyValue)
        assert (context.key, context.value, context.page_number) == ("Invoice", "10442", 1)

    def test_the_metadata_names_the_source(self, tmp_path):
        path = build_pdf(tmp_path / "Q3 report.pdf", [mixed_page()], title="Quarterly", author="Ada")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        metadata = extract_document(path).metadata

        assert metadata == DocumentMetadata(
            doc_id=f"Q3_report__{digest[:16]}",
            source_name="Q3 report.pdf",
            sha256=digest,
            page_count=1,
            title="Quarterly",
            author="Ada",
        )

    @pytest.mark.parametrize("blank", ["", "   "])
    def test_a_blank_title_and_author_are_none(self, tmp_path, blank):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()], title=blank, author=blank)

        metadata = extract_document(path).metadata

        assert (metadata.title, metadata.author) == (None, None)

    def test_the_hash_is_streamed_in_bounded_chunks(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        reads: list[int] = []
        monkeypatch.setattr(pipeline, "_HASH_CHUNK_BYTES", 1000)
        real_read = pipeline._read_chunk
        monkeypatch.setattr(pipeline, "_read_chunk", lambda stream, size: reads.append(size) or real_read(stream, size))

        metadata = extract_document(path).metadata

        size = path.stat().st_size
        assert metadata.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
        # Exactly the size at open, in full chunks and one last partial one.
        assert reads == [1000] * (size // 1000) + ([size % 1000] if size % 1000 else [])

    def test_a_file_that_shrinks_while_it_is_hashed_fails_loud(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        monkeypatch.setattr(pipeline, "_read_chunk", lambda stream, size: b"")

        message = f"{path} changed while it was hashed: 0 of its {path.stat().st_size} bytes could be read"
        with pytest.raises(PDFSourceChangedError, match=re.escape(message)):
            extract_document(path)

    def test_a_corrupt_page_is_an_error_page_and_the_rest_extract(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page(), mixed_page()]), 2)

        document = extract_document(path)

        assert [page.status for page in document.pages] == ["ok", "error", "ok"]
        assert document.pages[1].error == unloadable("page 2 is unreadable: Failed to load page.")
        assert document.degraded_pages == (2,)
        assert not document.integrity_verified
        expected = apply_scope((chain_page(path, 0), page_at(path, 1), chain_page(path, 2)))
        assert document.pages == expected.pages

    def test_strict_raises_on_a_corrupt_page_naming_it(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page(), mixed_page()]), 2)

        with pytest.raises(PDFCorruptError, match=re.escape("page 2 is unreadable: Failed to load page.")) as caught:
            extract_document(path, strict=True)

        assert caught.value.page_number == 2

    def test_a_page_whose_layout_raises_is_an_error_page_and_the_rest_extract(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page(), mixed_page()])
        real, calls = pipeline.find_lattice_tables, []

        def lattice(segments, pieces):
            calls.append(segments)
            if len(calls) == 2:  # page 2's: pages are extracted in order
                raise ValueError("no grid fits")
            return real(segments, pieces)

        monkeypatch.setattr(pipeline, "find_lattice_tables", lattice)

        document = extract_document(path)

        assert [page.status for page in document.pages] == ["ok", "error", "ok"]
        assert document.pages[1].error == "ValueError: no grid fits"
        assert document.pages[1].warnings == ()  # its size was read
        assert document.degraded_pages == (2,)

    def test_an_encrypted_document_opens_with_its_password(self, tmp_path):
        path = build_pdf(tmp_path / "enc.pdf", [mixed_page()], user_password="pw", title="Secret", author="Ada")

        document = extract_document(path, password="pw", min_image_dimension=32)

        assert document == chain_document(path, password="pw", title="Secret", author="Ada", min_image_dimension=32)
        assert document.figures[0].read_raw_bytes()

    @pytest.mark.parametrize("password,message", [(None, "pass its password"), ("nope", "password is incorrect")])
    def test_an_encrypted_document_without_its_password_raises(self, tmp_path, password, message):
        path = build_pdf(tmp_path / "enc.pdf", [mixed_page()], user_password="pw")

        with pytest.raises(PDFEncryptedError, match=message):
            extract_document(path, password=password)

    def test_a_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract_document(tmp_path / "missing.pdf")

    def test_a_file_that_is_no_pdf_raises(self, tmp_path):
        path = tmp_path / "notes.pdf"
        path.write_text("not a PDF")

        with pytest.raises(PDFCorruptError, match="is not a readable PDF") as caught:
            extract_document(path)

        assert caught.value.page_number is None

    @pytest.mark.parametrize(
        "options,message",
        [
            pytest.param({"workers": 0}, "workers must be >= 1, got 0", id="workers"),
            pytest.param({"dpi": 35}, "dpi must be >= 36, got 35", id="dpi"),
            pytest.param({"vision_dpi": 100}, "vision_dpi must be >= dpi (200), got 100", id="vision"),
            pytest.param({"max_render_pixels": 1}, "max_render_pixels must be >= 262144, got 1", id="pixels"),
            pytest.param({"min_image_dimension": 0}, "min_image_dimension must be >= 1, got 0", id="min-image"),
            pytest.param({"ocr": "maybe"}, "ocr must be 'auto' or 'always' or 'never', got 'maybe'", id="ocr"),
        ],
    )
    def test_the_construction_checks_come_before_any_work(self, tmp_path, monkeypatch, options, message):
        handles = opened_handles(monkeypatch)

        with pytest.raises(ValueError, match=re.escape(message)):
            extract_document(tmp_path / "missing.pdf", **options)

        assert handles == []

    def test_the_source_and_password_are_checked_before_any_work(self, tmp_path, monkeypatch):
        handles = opened_handles(monkeypatch)

        with pytest.raises(TypeError, match="source must be a str or os.PathLike, got bytes"):
            extract_document(b"doc.pdf")
        with pytest.raises(TypeError, match="password must be a str or None, got int"):
            extract_document(tmp_path / "missing.pdf", password=1234)

        assert handles == []

    @pytest.mark.parametrize("strict", [False, True])
    def test_every_document_it_opens_is_closed(self, tmp_path, monkeypatch, strict):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()]), 2)
        handles = opened_handles(monkeypatch)

        if strict:
            with pytest.raises(PDFCorruptError):
                extract_document(path, strict=True)
        else:
            extract_document(path)

        assert handles and all(handle.closed for handle in handles)


class TestWorkers:
    """``workers > 1``: contiguous chunks, one worker process each, pages back in page order; the pool always ends."""

    @pytest.fixture(autouse=True)
    def _no_distributed_runner(self, monkeypatch):
        for key in DISTRIBUTED_KEYS:
            monkeypatch.delenv(key, raising=False)

    @pytest.mark.parametrize(
        "count,workers,chunks",
        [
            pytest.param(10, 4, ((0, 3), (3, 6), (6, 8), (8, 10)), id="10-by-4"),
            pytest.param(4, 2, ((0, 2), (2, 4)), id="4-by-2"),
            pytest.param(3, 8, ((0, 1), (1, 2), (2, 3)), id="more-workers-than-pages"),
            pytest.param(7, 7, tuple((index, index + 1) for index in range(7)), id="one-page-each"),
        ],
    )
    def test_chunks_are_contiguous_and_near_equal(self, count, workers, chunks):
        assert pipeline._chunks(count, workers) == tuple(range(*bounds) for bounds in chunks)

    @pytest.mark.parametrize("workers", [2, 3, 4, 8])
    def test_pooled_pages_equal_sequential_pages_in_page_order(self, tmp_path, monkeypatch, workers):
        specs = [mixed_page(), *split_table_pages(), tiny_text_page(), mixed_page(120.0)]
        path = break_page(build_pdf(tmp_path / "doc.pdf", specs, title="Parts"), 4)
        sequential = extract_document(path, min_image_dimension=32)
        started = started_workers(monkeypatch)

        pooled = extract_document(path, workers=workers, min_image_dimension=32)

        assert len(started) == min(workers, 5)
        assert pooled == sequential
        assert [page.status for page in pooled.pages] == ["ok", "ok", "ok", "error", "ok"]
        (tail,), (head,) = ([table for table in pooled.tables if table.page_number == n] for n in (2, 3))
        assert head.continued_from_id == tail.element_id  # linked after the pool, whichever chunks held its parts
        assert pooled.figures[0].read_raw_bytes() == sequential.figures[0].read_raw_bytes()
        assert_reaped(started)

    @needs_tesseract
    def test_pooled_pages_equal_sequential_pages_with_a_vision_page(self, tmp_path, monkeypatch, scan_png):
        caption = TextRun("Page 7", 500.0, 760.0, size=10.0)
        specs = [mixed_page(), scan_page(scan_png), scan_page(scan_png, caption), mixed_page(120.0)]
        path = build_pdf(tmp_path / "doc.pdf", specs)
        sequential = extract_document(path, min_image_dimension=32)
        started = started_workers(monkeypatch)

        pooled = extract_document(path, workers=2, min_image_dimension=32)

        assert len(started) == 2
        assert pooled == sequential
        assert [(page.status, page.route) for page in pooled.pages][1:3] == [("ok", "vision"), ("ok", "vision")]
        assert pooled.pages[2].page_image.read_bytes() == sequential.pages[2].page_image.read_bytes()
        assert_reaped(started)

    def test_an_encrypted_document_is_pooled_with_its_password(self, tmp_path):
        path = build_pdf(tmp_path / "enc.pdf", [mixed_page(), mixed_page()], user_password="pw")

        pooled = extract_document(path, password="pw", workers=2, min_image_dimension=32)

        assert pooled == extract_document(path, password="pw", min_image_dimension=32)
        assert pooled.figures[1].read_raw_bytes()

    def test_strict_raises_from_a_worker_naming_the_page(self, tmp_path, monkeypatch):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4), 3)
        started = started_workers(monkeypatch)

        with pytest.raises(PDFCorruptError, match=re.escape("page 3 is unreadable: Failed to load page.")) as caught:
            extract_document(path, workers=2, strict=True)

        assert caught.value.page_number == 3
        assert_reaped(started)

    def test_a_worker_crash_is_error_pages_for_its_chunk_and_the_rest_extract(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        sequential = extract_document(path)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "crash_on_page_3")

        pooled = extract_document(path, workers=2)

        reason = "the worker process extracting pages 3-4 ended (exit code 7) before returning them"
        note = f"the page's size could not be read (its worker process returned nothing){DEFAULT_SIZE_NOTE}"
        assert pooled.pages[2:] == (error_page(3, reason, warnings=(note,)), error_page(4, reason, warnings=(note,)))
        assert pooled.pages[:2] == sequential.pages[:2]
        assert pooled.degraded_pages == (3, 4)
        assert_reaped(started)

    def test_strict_raises_on_a_worker_crash_naming_its_pages(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "crash_on_page_3")

        message = "the worker process extracting pages 3-4 ended (exit code 7) before returning them"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            extract_document(path, workers=2, strict=True)

        assert_reaped(started)

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_worker_past_its_time_is_killed_and_its_chunk_is_error_pages(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "hang_on_page_3")
        monkeypatch.setattr(pipeline, "_WORKER_START_S", 4.0)
        monkeypatch.setattr(pipeline, "_WORKER_PAGE_S", 1.0)

        began = time.monotonic()
        pooled = extract_document(path, workers=2)
        elapsed = time.monotonic() - began

        reason = "the worker process extracting pages 3-4 did not return them within 6 s: it was killed"
        assert [page.error for page in pooled.pages] == [None, None, reason, reason]
        # The budget is 4 + 1 x 2 = 6 s from the pool's start; the hung worker sleeps 60 s.
        assert 6.0 <= elapsed < 40.0  # about 6 s here; the hung worker would reply at 60 s
        assert_reaped(started)

    @pytest.mark.parametrize(
        "worker,message",
        [
            pytest.param("send_a_tuple_as_a_page", "pages 1-1 sent a tuple, not a page", id="not-a-page"),
            pytest.param("send_more_pages", "pages 1-1 sent more than its 1 page(s)", id="more-pages"),
            pytest.param("send_an_unknown_kind", "pages 1-1 sent a reply of unknown kind b'Z'", id="unknown-kind"),
            pytest.param("send_a_str_as_an_error", "pages 1-1 sent a str as its error", id="error-not-exception"),
        ],
    )
    def test_a_worker_whose_reply_is_not_its_pages_fails_loud(self, tmp_path, monkeypatch, worker, message):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, worker)

        with pytest.raises(DatagruntPDFError, match=re.escape(f"the worker process extracting {message}")):
            extract_document(path, workers=2)

        assert_reaped(started)

    def test_a_reply_over_its_bound_is_refused_unread_and_its_chunk_is_error_pages(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        started = started_workers(monkeypatch)
        monkeypatch.setattr(pipeline, "_MAX_PAGE_MESSAGE_BYTES", 100)

        pooled = extract_document(path, workers=2)

        pattern = (
            r"the worker process extracting pages \d-\d sent a \d+-byte message, over the 100-byte bound: it was killed"
        )
        assert all(re.fullmatch(pattern, page.error) for page in pooled.pages), [page.error for page in pooled.pages]
        assert_reaped(started)

    def test_a_crashed_workers_stderr_ends_its_pages_error(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        run_page_workers_as(monkeypatch, "complain_and_crash_on_page_3")

        pooled = extract_document(path, workers=2)

        ended = "the worker process extracting pages 3-4 ended (exit code 3) before returning them"
        assert [page.error for page in pooled.pages[2:]] == [f"{ended}; its stderr ends: pdfium: boom"] * 2

    def test_a_page_worker_is_python_isolated_and_reads_its_request_on_stdin(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2, user_password="pw-6b0d")
        seen = []
        real = pipeline.start_worker
        monkeypatch.setattr(
            pipeline, "start_worker", lambda command, errors: seen.append(command) or real(command, errors)
        )
        written = []
        real_send = pipeline._send_request
        monkeypatch.setattr(
            pipeline, "_send_request", lambda worker, payload: written.append(payload) or real_send(worker, payload)
        )

        extract_document(path, password="pw-6b0d", workers=2)

        assert seen == [[sys.executable, "-I", "-c", _process.BOOTSTRAP]] * 2
        requests = [json.loads(payload) for payload in written]
        assert [(request["worker"], request["request"]["pages"]) for request in requests] == [
            ("pages", [0, 1]),
            ("pages", [1, 2]),
        ]
        assert all(
            request["sys_path"] == sys.path and request["request"]["password"] == "pw-6b0d" for request in requests
        )
        assert not any("pw-6b0d" in part for part in seen[0])

    def test_page_workers_ignore_the_callers_cwd_and_python_environment(self, tmp_path, monkeypatch):
        poisoned = tmp_path / "poisoned"
        poisoned.mkdir()
        marker = tmp_path / "poison.txt"
        (poisoned / "json.py").write_text(
            f"open({str(marker)!r}, 'a').write('poisoned')\nraise ImportError('poisoned json')\n", encoding="utf-8"
        )
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        sequential = extract_document(path)
        monkeypatch.chdir(poisoned)
        monkeypatch.setenv("PYTHONPATH", str(poisoned))

        assert extract_document(path, workers=2) == sequential
        assert not marker.exists()

    def test_a_script_without_a_main_guard_extracts_and_runs_its_top_level_once(self, tmp_path):
        script = tmp_path / "no_guard.py"
        script.write_text(NO_GUARD_SCRIPT, encoding="utf-8")
        marker = tmp_path / "marker.txt"
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 3)

        done = subprocess.run(
            [sys.executable, str(script), str(path), str(marker)],
            capture_output=True,
            check=False,
            timeout=180,
            text=True,
        )

        assert (done.returncode, done.stdout) == (0, "extracted 3 pages: ['ok', 'ok', 'ok']\n"), done.stderr
        assert marker.read_text(encoding="utf-8").splitlines() == ["top level ran"]

    @pytest.mark.parametrize(
        "attribute,value,why",
        [
            pytest.param(
                "frozen",
                True,
                "a frozen application has no Python interpreter to start a page worker with",
                id="frozen",
            ),
            pytest.param(
                "executable",
                "",
                "sys.executable is empty: there is no Python interpreter for a page worker",
                id="empty",
            ),
        ],
    )
    def test_no_interpreter_fails_before_any_worker_starts(self, tmp_path, monkeypatch, attribute, value, why):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        started = started_workers(monkeypatch)
        monkeypatch.setattr(sys, attribute, value, raising=False)

        with pytest.raises(DatagruntPDFError, match=re.escape(f"{why}: pass workers=1 to extract in this process")):
            extract_document(path, workers=2)

        assert started == []

    def test_a_worker_that_returns_its_pages_out_of_order_fails_loud(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "send_pages_reversed")

        message = "the worker process extracting pages 1-2 returned pages [2, 1], not pages [1, 2]"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            extract_document(path, workers=2)

        assert_reaped(started)

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_strict_failure_kills_the_workers_still_running(self, tmp_path, monkeypatch):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4), 1)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "hang_on_page_3")

        began = time.monotonic()
        with pytest.raises(PDFCorruptError, match=re.escape("page 1 is unreadable")):
            extract_document(path, workers=2, strict=True)
        elapsed = time.monotonic() - began

        # Pages 3-4's worker sleeps 60 s: it was killed, not waited for (under 1 s here).
        assert elapsed < 30.0
        assert_reaped(started)

    def test_the_pool_is_shut_down_when_a_worker_fails_to_start(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 3)
        started = started_workers(monkeypatch)
        recorded = pipeline._started

        def second_fails(*args, **kwargs):
            if started:
                raise OSError("no more processes")
            return recorded(*args, **kwargs)

        monkeypatch.setattr(pipeline, "_started", second_fails)

        with pytest.raises(OSError, match="no more processes"):
            extract_document(path, workers=3)

        assert len(started) == 1
        assert_reaped(started)

    @pytest.mark.parametrize("key", DISTRIBUTED_KEYS)
    def test_a_distributed_runner_extracts_sequentially(self, tmp_path, monkeypatch, key):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 3)
        monkeypatch.setenv(key, "1")
        monkeypatch.setattr(pipeline, "_pooled_pages", raising(AssertionError("the pool ran")))

        assert extract_document(path, workers=4) == extract_document(path)

    @pytest.mark.parametrize("pages,workers", [pytest.param(1, 4, id="one-page"), pytest.param(3, 1, id="one-worker")])
    def test_one_page_or_one_worker_extracts_sequentially(self, tmp_path, monkeypatch, pages, workers):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * pages)
        monkeypatch.setattr(pipeline, "_pooled_pages", raising(AssertionError("the pool ran")))

        assert len(extract_document(path, workers=workers).pages) == pages


class TestSourceIdentity:
    """The hash is bound to the bytes extracted: the identity, taken once, is checked at every open and at the end.

    The identity is ``(st_dev, st_ino, st_size, st_mtime_ns)`` of the file the
    hash was read through. A change raises PDFSourceChangedError, strict or
    not: the whole document would be inconsistent, not one page.
    """

    @pytest.fixture(autouse=True)
    def _no_distributed_runner(self, monkeypatch):
        for key in DISTRIBUTED_KEYS:
            monkeypatch.delenv(key, raising=False)

    def test_the_error_is_a_datagrunt_pdf_error(self):
        assert issubclass(PDFSourceChangedError, DatagruntPDFError)

    @pytest.mark.parametrize("workers", [1, 2])
    def test_an_untouched_file_extracts_and_its_hash_is_its_bytes(self, tmp_path, workers):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 3)

        document = extract_document(path, workers=workers)

        assert document.metadata.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
        assert [page.status for page in document.pages] == ["ok"] * 3

    @pytest.mark.parametrize("strict", [False, True])
    def test_a_file_appended_to_before_pdfium_opens_it_raises_naming_size_and_mtime(
        self, tmp_path, monkeypatch, strict
    ):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        before = identity_of(path)
        after_hashing(monkeypatch, lambda: append_in_place(path))

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_document(path, strict=strict)

        assert str(caught.value) == changed(path, "when pdfium opened it", before, identity_of(path), "size", "mtime")

    def test_a_file_replaced_before_pdfium_opens_it_raises_naming_the_inode(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        other = build_pdf(tmp_path / "other.pdf", [mixed_page()] * 2)
        os.utime(other, ns=(os.stat(path).st_atime_ns, os.stat(path).st_mtime_ns))
        before = identity_of(path)
        after_hashing(monkeypatch, lambda: os.replace(other, path))

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_document(path)

        # The same bytes (the same spec builds the same file) and the same mtime: only the inode tells.
        assert identity_of(path).size == before.size
        assert str(caught.value) == changed(path, "when pdfium opened it", before, identity_of(path), "inode")

    def test_a_file_appended_to_during_extraction_raises_at_the_end(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 2)
        before = identity_of(path)
        real = pipeline._pages_in

        def then_append(*args):
            pages = real(*args)
            append_in_place(path)
            return pages

        monkeypatch.setattr(pipeline, "_pages_in", then_append)

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_document(path)

        when = "by the end of its extraction"
        assert str(caught.value) == changed(path, when, before, identity_of(path), "size", "mtime")

    def test_a_file_replaced_before_the_workers_open_it_raises_in_the_first(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        other = build_pdf(tmp_path / "other.pdf", [mixed_page()] * 4)
        os.utime(other, ns=(os.stat(path).st_atime_ns, os.stat(path).st_mtime_ns))
        before = identity_of(path)
        started = started_workers(monkeypatch)
        real = pipeline._pooled_pages

        def swapped_first(*args):
            os.replace(other, path)
            return real(*args)

        monkeypatch.setattr(pipeline, "_pooled_pages", swapped_first)

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_document(path, workers=2)

        when = "when the worker for pages 1-2 opened it"
        assert str(caught.value) == changed(path, when, before, identity_of(path), "inode")
        assert_reaped(started)

    def test_a_file_replaced_while_a_worker_extracts_raises_when_it_finishes(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 4)
        build_pdf(tmp_path / "other.pdf", [mixed_page()] * 4)
        os.utime(tmp_path / "other.pdf", ns=(os.stat(path).st_atime_ns, os.stat(path).st_mtime_ns))
        before = identity_of(path)
        started = started_workers(monkeypatch)
        run_page_workers_as(monkeypatch, "swap_after_first_chunk")

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_document(path, workers=2)

        when = "by the time the worker for pages 1-2 finished them"
        assert str(caught.value) == changed(path, when, before, identity_of(path), "inode")
        assert_reaped(started)

    def test_a_page_opened_from_a_stale_identity_raises(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        other = build_pdf(tmp_path / "other.pdf", [PageSpec(items=body_text(100.0))])
        os.utime(path, ns=(os.stat(other).st_atime_ns, os.stat(other).st_mtime_ns))
        stale = identity_of(other)
        source = PDFSource(path=path, identity=stale)

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_page_at(source, 0, PipelineOptions())

        assert str(caught.value) == changed(path, "when pdfium opened it", stale, identity_of(path), "inode", "size")

    def test_a_page_whose_file_is_replaced_while_it_extracts_raises(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        other = build_pdf(tmp_path / "other.pdf", [mixed_page()])
        os.utime(other, ns=(os.stat(path).st_atime_ns, os.stat(path).st_mtime_ns))
        before = identity_of(path)
        real = pipeline._isolated_page

        def then_swap(*args):
            page = real(*args)
            os.replace(other, path)
            return page

        monkeypatch.setattr(pipeline, "_isolated_page", then_swap)

        with pytest.raises(PDFSourceChangedError) as caught:
            extract_page_at(PDFSource(path=path, identity=before), 0, PipelineOptions())

        when = "by the time page 1 was extracted"
        assert str(caught.value) == changed(path, when, before, identity_of(path), "inode")

    def test_a_page_without_an_identity_is_not_checked(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])

        assert extract_page_at(PDFSource(path=path), 0, PipelineOptions()) == chain_page(path)

    def test_the_identity_checks_its_fields(self):
        with pytest.raises(ValueError, match=re.escape("size must be >= 0, got -1")):
            SourceIdentity(device=1, inode=2, size=-1, mtime_ns=0)
        with pytest.raises(TypeError, match=re.escape("inode must be an int, got 2.0 (float)")):
            SourceIdentity(device=1, inode=2.0, size=1, mtime_ns=0)
        with pytest.raises(TypeError, match=re.escape("identity must be a SourceIdentity or None, got tuple")):
            PDFSource(path="doc.pdf", identity=(1, 2, 3, 4))


class TestPageWorker:
    """The page worker in this process: its request in; its pages, then done (or the error that stopped it), out."""

    def request(self, path, chunk=(0, 2), **changes):
        source = PDFSource(path=path)
        request = json.loads(
            _process.request_payload(
                "pages", pipeline._chunk_payload(source, range(*chunk), PipelineOptions()), 1 << 20
            )
        )["request"]
        return request | changes

    def replies(self, message):
        stream = io.BytesIO()
        pipeline._worker_main(message, stream)
        stream.seek(0)
        found = []
        for _ in range(10):
            reply = _process.read_message(stream, 1 << 30)
            if reply is None:
                return found
            found.append(reply)
        raise AssertionError("more than 10 replies")

    def test_it_sends_each_page_pickled_in_order_then_done(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page(), mixed_page()])

        replies = self.replies(self.request(path, (1, 3)))

        assert [reply[:1] for reply in replies] == [b"P", b"P", b"D"]
        assert [pickle.loads(reply[1:]) for reply in replies[:2]] == [chain_page(path, 1), chain_page(path, 2)]

    def test_an_error_that_stops_the_chunk_is_sent_pickled_instead(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()]), 2)
        message = self.request(path, options=dataclasses.asdict(PipelineOptions(strict=True)))

        (reply,) = self.replies(message)

        assert reply[:1] == b"E"
        error = pickle.loads(reply[1:])
        assert (type(error), str(error), error.page_number) == (
            PDFCorruptError,
            "page 2 is unreadable: Failed to load page.",
            2,
        )

    @pytest.mark.parametrize(
        "change,error",
        [
            pytest.param(
                {"pages": [2, 1]},
                "ValueError: pages must be [start, stop] with 0 <= start < stop, got [2, 1]",
                id="pages",
            ),
            pytest.param(
                {"pages": [0, True]},
                "ValueError: pages must be [start, stop] with 0 <= start < stop, got [0, True]",
                id="pages-bool",
            ),
            pytest.param({"options": {"workers": 0}}, "ValueError: workers must be >= 1, got 0", id="options"),
            pytest.param({"options": {"threads": 2}}, "TypeError", id="options-unknown"),
            pytest.param({"extra": 1}, "ValueError: the page worker's request must hold exactly", id="keys"),
        ],
    )
    def test_a_request_it_cannot_read_is_sent_back_as_its_error(self, tmp_path, change, error):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])

        (reply,) = self.replies(self.request(path, (0, 1), **change))

        sent = pickle.loads(reply[1:])
        assert reply[:1] == b"E" and f"{type(sent).__name__}: {sent}".startswith(error)
