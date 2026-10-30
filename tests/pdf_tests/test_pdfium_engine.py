"""Tests for the v5 pypdfium2 engine (#365): document open and page geometry.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), whose
coordinates are top-left points in the unrotated page space.
"""

import ast
import dataclasses
import errno
import gc
import os
import pickle
import re
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import PDFCorruptError, PDFEncryptedError, pdfium_engine
from datagrunt.pdf.pdfium_engine import DocumentHandle, PageGeometry, open_document, page_geometry, to_display_box
from pdf_tests.builders import PageSpec, Rect, TextRun, break_page, build_pdf, hand_written_pdf

# Position tolerance in page points.
TOL = 1.0

USER_PW = "user-pw"
OWNER_PW = "owner-pw"

# A 400 x 300 pt page; the rect is PDF (60, 240)-(90, 260), top-left (60, 40)-(90, 60).
FULL_PAGE = (0.0, 0.0, 400.0, 300.0)
# CropBox (50, 40)-(350, 280): the visible area is offset from the origin.
OFFSET_CROP = (50.0, 40.0, 350.0, 280.0)
RECT = (60.0, 240.0, 90.0, 260.0)


def three_pages(tmp_path: Path) -> Path:
    """A plain three-page PDF with one line of text per page."""
    pages = [PageSpec(items=(TextRun(f"Page {n}", x=72.0, y=100.0),)) for n in (1, 2, 3)]
    return build_pdf(tmp_path / "three.pdf", pages)


def encrypted(tmp_path: Path) -> Path:
    """A one-page RC4-128 PDF that needs USER_PW (or OWNER_PW) to open."""
    page = PageSpec(items=(TextRun("Secret", x=72.0, y=100.0),))
    return build_pdf(tmp_path / "enc.pdf", [page], user_password=USER_PW, owner_password=OWNER_PW, strength=128)


def zero_pages(tmp_path: Path) -> Path:
    """A well-formed PDF whose page tree is empty."""
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [] /Count 0 >>"]
    return hand_written_pdf(tmp_path / "zero.pdf", objects)


def open_pdfium_documents() -> int:
    """Count pypdfium2 documents whose handle is still open, without collecting.

    A document counts as closed only once its finalizer has run, so this
    exposes any handle left for the cyclic garbage collector to reclaim.
    """
    return sum(
        1
        for obj in gc.get_objects()
        if isinstance(obj, pdfium.PdfDocument) and obj._finalizer is not None and obj._finalizer.alive
    )


@pytest.fixture
def gc_disabled():
    """Run a test with the cyclic GC off, so only refcounting can close handles."""
    was_enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()


class TestModuleImports:
    """The engine stands alone: no v4 PDF stack, no PyMuPDF or pdfplumber."""

    def test_imports_only_allowed_modules(self):
        tree = ast.parse(Path(pdfium_engine.__file__).read_text(encoding="utf-8"))
        imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        imported |= {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
        banned = ("fitz", "pymupdf", "pdfplumber", "datagrunt.core.pdf_io", "datagrunt.pdf_api")
        assert not [name for name in imported if name.lower().startswith(banned)]


class TestToDisplayBox:
    """pdfium's bottom-left, unrotated boxes map to top-left displayed boxes."""

    @pytest.mark.parametrize(
        "rotation,page_box,expected",
        [
            pytest.param(0, FULL_PAGE, (60.0, 40.0, 90.0, 60.0), id="rot0"),
            pytest.param(90, FULL_PAGE, (240.0, 60.0, 260.0, 90.0), id="rot90"),
            pytest.param(180, FULL_PAGE, (310.0, 240.0, 340.0, 260.0), id="rot180"),
            pytest.param(270, FULL_PAGE, (40.0, 310.0, 60.0, 340.0), id="rot270"),
            pytest.param(0, OFFSET_CROP, (10.0, 20.0, 40.0, 40.0), id="rot0-crop"),
            pytest.param(90, OFFSET_CROP, (200.0, 10.0, 220.0, 40.0), id="rot90-crop"),
            pytest.param(180, OFFSET_CROP, (260.0, 200.0, 290.0, 220.0), id="rot180-crop"),
            pytest.param(270, OFFSET_CROP, (20.0, 260.0, 40.0, 290.0), id="rot270-crop"),
        ],
    )
    def test_hand_computed_cases(self, rotation, page_box, expected):
        assert to_display_box(RECT, rotation_deg=rotation, page_box=page_box) == pytest.approx(expected)

    def test_off_page_box_keeps_negative_coordinates(self):
        # Preserve, not clip: an off-page span must stay detectable downstream.
        assert to_display_box((-50.0, 400.0, -10.0, 420.0), rotation_deg=0, page_box=FULL_PAGE) == pytest.approx(
            (-50.0, -120.0, -10.0, -100.0)
        )

    @pytest.mark.parametrize(
        "box,rotation,page_box,error,message",
        [
            pytest.param(RECT, 45, FULL_PAGE, ValueError, "rotation_deg must be one of (0, 90, 180, 270), got 45"),
            pytest.param(RECT, -90, FULL_PAGE, ValueError, "rotation_deg must be one of (0, 90, 180, 270), got -90"),
            pytest.param(RECT, 90.0, FULL_PAGE, TypeError, "rotation_deg must be an int, got 90.0 (float)"),
            pytest.param(RECT, True, FULL_PAGE, TypeError, "rotation_deg must be an int, got True (bool)"),
            pytest.param([60.0, 240.0, 90.0, 260.0], 0, FULL_PAGE, TypeError, "box must be a tuple, got list"),
            pytest.param((60.0, 240.0, 90.0), 0, FULL_PAGE, ValueError, "box must have 4 values, got 3"),
            pytest.param((60.0, float("nan"), 90.0, 260.0), 0, FULL_PAGE, ValueError, "box[1] must be finite"),
            pytest.param((90.0, 240.0, 60.0, 260.0), 0, FULL_PAGE, ValueError, "box must have left <= right"),
            pytest.param((60.0, 260.0, 90.0, 240.0), 0, FULL_PAGE, ValueError, "box must have left <= right"),
            pytest.param(RECT, 0, (0.0, 0.0, 0.0, 0.0), ValueError, "page_box must have left < right"),
            pytest.param(RECT, 0, (0.0, 0.0, 400.0, float("inf")), ValueError, "page_box[3] must be finite"),
        ],
    )
    def test_invalid_input_raises(self, box, rotation, page_box, error, message):
        with pytest.raises(error, match=re.escape(message)):
            to_display_box(box, rotation_deg=rotation, page_box=page_box)


class BytesPathLike:
    """An os.PathLike whose path is bytes, which pypdfium2 cannot take."""

    def __fspath__(self) -> bytes:
        return b"doc.pdf"


class TestOpenDocument:
    """open_document opens a PDF or raises one datagrunt error per failure kind."""

    @pytest.mark.parametrize("as_type", [str, Path], ids=["str", "path"])
    def test_opens_a_plain_pdf(self, tmp_path, as_type):
        handle = open_document(as_type(three_pages(tmp_path)))

        assert isinstance(handle, DocumentHandle)
        assert handle.page_count == 3
        assert not handle.closed

    @pytest.mark.parametrize("password", [USER_PW, OWNER_PW], ids=["user", "owner"])
    def test_opens_an_encrypted_pdf_with_its_password(self, tmp_path, password):
        handle = open_document(encrypted(tmp_path), password=password)

        assert handle.page_count == 1

    def test_missing_password_raises_encrypted_error(self, tmp_path):
        with pytest.raises(PDFEncryptedError, match="is encrypted; pass its password"):
            open_document(encrypted(tmp_path))

    def test_wrong_password_raises_encrypted_error_without_echoing_it(self, tmp_path):
        with pytest.raises(PDFEncryptedError, match="the password is incorrect") as caught:
            open_document(encrypted(tmp_path), password="hunter2")
        assert "hunter2" not in str(caught.value)

    def test_empty_password_is_an_incorrect_password(self, tmp_path):
        with pytest.raises(PDFEncryptedError, match="the password is incorrect"):
            open_document(encrypted(tmp_path), password="")

    @pytest.mark.parametrize("password", [b"s3cret", bytearray(b"s3cret")], ids=["bytes", "bytearray"])
    def test_non_str_password_raises_type_error_without_echoing_it(self, tmp_path, password):
        with pytest.raises(
            TypeError, match=re.escape(f"password must be a str or None, got {type(password).__name__}") + "$"
        ) as caught:
            open_document(three_pages(tmp_path), password=password)
        assert "s3cret" not in str(caught.value)

    def test_unsupported_security_handler_raises_encrypted_error(self, tmp_path):
        path = encrypted(tmp_path)
        data = path.read_bytes()
        assert data.count(b"/Filter /Standard") == 1
        # Same length, so every xref offset stays valid: only the handler name changes.
        path.write_bytes(data.replace(b"/Filter /Standard", b"/Filter /Foobarxx"))

        with pytest.raises(PDFEncryptedError, match="security handler pdfium does not support"):
            open_document(path, password=USER_PW)

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"col_a,col_b\n1,2\n", id="csv"),
            pytest.param(b"", id="empty"),
            pytest.param(b"%PDF-1.4\n", id="header-only"),
        ],
    )
    def test_non_pdf_raises_corrupt_error_without_a_page(self, tmp_path, content):
        path = tmp_path / "bad.pdf"
        path.write_bytes(content)

        with pytest.raises(PDFCorruptError, match="Data format error") as caught:
            open_document(path)
        assert caught.value.page_number is None

    def test_truncated_pdf_raises_corrupt_error_without_a_page(self, tmp_path):
        path = three_pages(tmp_path)
        data = path.read_bytes()
        path.write_bytes(data[: len(data) // 2])

        with pytest.raises(PDFCorruptError, match="Data format error") as caught:
            open_document(path)
        assert caught.value.page_number is None

    @pytest.mark.parametrize("earlier_failure", [None, "password", "format"])
    def test_zero_page_pdf_raises_corrupt_error_whatever_failed_before(self, tmp_path, earlier_failure):
        # pdfium's last-error code is never reset by a successful parse, so a
        # zero-page file must not be classified by it (it would read as the
        # previous failure: an unencrypted file reported as encrypted).
        if earlier_failure == "password":
            with pytest.raises(PDFEncryptedError):
                open_document(encrypted(tmp_path))
        if earlier_failure == "format":
            (tmp_path / "bad.pdf").write_bytes(b"not a pdf")
            with pytest.raises(PDFCorruptError):
                open_document(tmp_path / "bad.pdf")

        with pytest.raises(PDFCorruptError, match="has no pages") as caught:
            open_document(zero_pages(tmp_path))
        assert caught.value.page_number is None

    def test_zero_page_pdf_is_closed_not_leaked(self, tmp_path, monkeypatch):
        closed = []
        original = pdfium_c.FPDF_CloseDocument
        monkeypatch.setattr(pdfium_c, "FPDF_CloseDocument", lambda raw: closed.append(raw) or original(raw))

        with pytest.raises(PDFCorruptError, match="has no pages"):
            open_document(zero_pages(tmp_path))
        assert len(closed) == 1

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            open_document(tmp_path / "absent.pdf")

    @pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="needs a non-root POSIX user")
    def test_unreadable_file_raises_permission_error_not_corrupt_error(self, tmp_path):
        path = three_pages(tmp_path)
        path.chmod(0)
        try:
            with pytest.raises(PermissionError, match="File access error") as caught:
                open_document(path)
        finally:
            path.chmod(0o600)
        assert caught.value.errno == errno.EACCES
        assert caught.value.filename == str(path)

    @pytest.mark.parametrize(
        "path,password,error,message",
        [
            pytest.param(b"doc.pdf", None, TypeError, "path must be a str or os.PathLike, got bytes", id="bytes-path"),
            pytest.param(3, None, TypeError, "path must be a str or os.PathLike, got int", id="int-path"),
            pytest.param(
                BytesPathLike(),
                None,
                TypeError,
                "path must be a str or os.PathLike, got a bytes path",
                id="bytes-fspath",
            ),
            pytest.param("doc.pdf", 123, TypeError, "password must be a str or None, got int", id="int-pw"),
            pytest.param("doc.pdf", "a\x00b", ValueError, "password must not contain NUL", id="nul-pw"),
        ],
    )
    def test_invalid_arguments_raise(self, path, password, error, message):
        with pytest.raises(error, match=re.escape(message)):
            open_document(path, password=password)


class TestDocumentHandle:
    """The handle closes its pdfium document on exit, close(), or last reference."""

    def test_context_manager_closes(self, tmp_path):
        with open_document(three_pages(tmp_path)) as handle:
            assert not handle.closed

        assert handle.closed

    def test_close_is_idempotent(self, tmp_path):
        handle = open_document(three_pages(tmp_path))

        handle.close()
        handle.close()

        assert handle.closed

    def test_closed_handle_refuses_use(self, tmp_path):
        handle = open_document(three_pages(tmp_path))
        handle.close()

        with pytest.raises(ValueError, match="document handle is closed"):
            _ = handle.page_count

    def test_refuses_to_pickle_and_says_to_reopen(self, tmp_path):
        handle = open_document(three_pages(tmp_path))

        with pytest.raises(TypeError, match="cannot be pickled.*reopen it from its path"):
            pickle.dumps(handle)
        assert not handle.closed

    def test_wraps_only_a_pdfium_document(self):
        with pytest.raises(TypeError, match=re.escape("pdf must be a pypdfium2 PdfDocument, got str")):
            DocumentHandle("doc.pdf")

    def test_dropping_the_last_reference_closes_without_gc(self, tmp_path, gc_disabled):
        path = three_pages(tmp_path)
        before = open_pdfium_documents()

        handle = open_document(path)
        assert open_pdfium_documents() == before + 1
        del handle

        assert open_pdfium_documents() == before


def geometry(**overrides) -> PageGeometry:
    """A valid PageGeometry with ``overrides`` applied."""
    fields = {"page_number": 1, "width_pt": 400.0, "height_pt": 300.0, "rotation_deg": 0, "crop_offset": (0.0, 0.0)}
    return PageGeometry(**(fields | overrides))


class TestPageGeometry:
    """PageGeometry is a validated, immutable, picklable value."""

    def test_pickle_round_trip(self):
        original = geometry(rotation_deg=90, crop_offset=(50.0, 40.0))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = geometry()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.width_pt = 1.0
        assert not hasattr(value, "__dict__")

    def test_int_sizes_and_offsets_are_stored_as_floats(self):
        value = geometry(width_pt=400, height_pt=300, crop_offset=(50, 40))

        assert (type(value.width_pt), type(value.height_pt)) == (float, float)
        assert [type(part) for part in value.crop_offset] == [float, float]

    @pytest.mark.parametrize(
        "rotation,width,height,offset,page_box",
        [
            pytest.param(0, 300.0, 240.0, (50.0, 40.0), (50.0, 40.0, 350.0, 280.0), id="rot0"),
            pytest.param(90, 240.0, 300.0, (50.0, 40.0), (50.0, 40.0, 350.0, 280.0), id="rot90"),
            pytest.param(180, 300.0, 240.0, (0.0, 0.0), (0.0, 0.0, 300.0, 240.0), id="rot180"),
            pytest.param(270, 240.0, 300.0, (-10.0, 5.0), (-10.0, 5.0, 290.0, 245.0), id="rot270-negative"),
        ],
    )
    def test_page_box_is_the_unrotated_visible_box(self, rotation, width, height, offset, page_box):
        value = geometry(rotation_deg=rotation, width_pt=width, height_pt=height, crop_offset=offset)

        assert value.page_box == pytest.approx(page_box)

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"page_number": 0}, ValueError, "page_number must be >= 1, got 0", id="page-0"),
            pytest.param({"page_number": True}, TypeError, "page_number must be an int", id="page-bool"),
            pytest.param({"width_pt": 0.0}, ValueError, "width_pt must be > 0, got 0.0", id="width-0"),
            pytest.param({"height_pt": float("nan")}, ValueError, "height_pt must be finite", id="height-nan"),
            pytest.param({"rotation_deg": 45}, ValueError, "rotation_deg must be one of", id="rotation-45"),
            pytest.param({"crop_offset": [0.0, 0.0]}, TypeError, "crop_offset must be a tuple, got list", id="list"),
            pytest.param({"crop_offset": (0.0,)}, ValueError, "crop_offset must have 2 values, got 1", id="len-1"),
            pytest.param(
                {"crop_offset": (0.0, float("inf"))}, ValueError, "crop_offset[1] must be finite", id="offset-inf"
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            geometry(**overrides)


@pytest.fixture
def page_closes(monkeypatch):
    """Record every explicit PdfPage.close() call (not the parent's cascade).

    Refcounting alone would also free a dropped page, so counting open pages
    cannot show that the engine closes them; counting close() calls can.
    """
    calls = []
    original = pdfium.PdfPage.close

    def counting_close(self, *args, **kwargs):
        if not kwargs.get("_by_parent", False):
            calls.append(self)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pdfium.PdfPage, "close", counting_close)
    return calls


ROTATIONS = [0, 90, 180, 270]
# Top-left (50, 20)-(350, 260) on the 400 x 300 page: PDF CropBox (50, 40)-(350, 280).
CROPS = [pytest.param(None, id="no-crop"), pytest.param((50.0, 20.0, 350.0, 260.0), id="offset-crop")]


def one_page(tmp_path: Path, *items, rotate: int = 0, crop_box=None) -> Path:
    """A one-page 400 x 300 pt PDF holding ``items``."""
    spec = PageSpec(width=400.0, height=300.0, items=items, rotate=rotate, crop_box=crop_box)
    return build_pdf(tmp_path / "page.pdf", [spec])


def one_page_pdf(tmp_path: Path, pages_entries: bytes = b"", page_entries: bytes = b"") -> Path:
    """A hand-written one-page PDF with extra /Pages and /Page dictionary entries.

    For what ReportLab cannot write: boxes inherited from /Pages, an empty
    MediaBox, /UserUnit.
    """
    content = b"BT /F1 20 Tf 100 100 Td (T) Tj ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 " + pages_entries + b" >>",
        b"<< /Type /Page /Parent 2 0 R /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> " + page_entries + b" >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    return hand_written_pdf(tmp_path / "hand.pdf", objects)


class TestPageGeometryOfAPage:
    """page_geometry reads one page's displayed size, rotation and crop offset."""

    @pytest.mark.parametrize("crop_box", CROPS)
    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_matches_the_built_page(self, tmp_path, rotate, crop_box):
        path = one_page(tmp_path, rotate=rotate, crop_box=crop_box)
        visible, offset = ((300.0, 240.0), (50.0, 40.0)) if crop_box else ((400.0, 300.0), (0.0, 0.0))
        width, height = visible[::-1] if rotate in (90, 270) else visible

        got = page_geometry(open_document(path), 0)

        assert got == PageGeometry(
            page_number=1, width_pt=width, height_pt=height, rotation_deg=rotate, crop_offset=offset
        )
        assert (got.width_pt, got.height_pt) == pytest.approx(pdfium.PdfDocument(path)[0].get_size())

    def test_boxes_and_rotation_inherited_from_the_page_tree_are_honoured(self, tmp_path):
        path = one_page_pdf(tmp_path, pages_entries=b"/MediaBox [0 0 400 300] /CropBox [50 40 350 280] /Rotate 90")
        # Why geometry reads get_bbox(): pypdfium2's get_mediabox() does not
        # inherit, and falls back to US Letter for this page.
        assert pdfium.PdfDocument(path)[0].get_mediabox() == (0, 0, 612, 792)

        got = page_geometry(open_document(path), 0)

        assert got == PageGeometry(
            page_number=1, width_pt=240.0, height_pt=300.0, rotation_deg=90, crop_offset=(50.0, 40.0)
        )

    @pytest.mark.parametrize(
        "page_entries,size",
        [
            pytest.param(b"/MediaBox [0 0 0 0]", (612.0, 792.0), id="empty-media-box-is-letter"),
            pytest.param(b"/MediaBox [0 0 400 300] /UserUnit 2", (400.0, 300.0), id="user-unit-ignored"),
        ],
    )
    def test_documented_pdfium_size_rules(self, tmp_path, page_entries, size):
        path = one_page_pdf(tmp_path, page_entries=page_entries)

        got = page_geometry(open_document(path), 0)

        assert (got.width_pt, got.height_pt) == size
        assert pdfium.PdfDocument(path)[0].render(scale=1).to_pil().size == tuple(map(round, size))

    def test_page_number_is_one_based(self, tmp_path):
        handle = open_document(three_pages(tmp_path))

        assert [page_geometry(handle, index).page_number for index in range(3)] == [1, 2, 3]

    def test_closes_every_page_handle(self, tmp_path, page_closes):
        handle = open_document(three_pages(tmp_path))

        for index in range(3):
            page_geometry(handle, index)

        assert len(page_closes) == 3
        assert all(page.raw is None for page in page_closes)
        assert not handle.closed

    def test_closes_the_page_when_reading_it_fails(self, tmp_path, page_closes, monkeypatch):
        def failing_bbox(self):
            raise pdfium.PdfiumError("Failed to get page bounding box.")

        monkeypatch.setattr(pdfium.PdfPage, "get_bbox", failing_bbox)
        handle = open_document(three_pages(tmp_path))

        # pytest keeps the traceback, and with it the page: only close() frees it.
        with pytest.raises(PDFCorruptError, match="page 1 is unreadable") as caught:
            page_geometry(handle, 0)

        assert caught.value.page_number == 1
        assert len(page_closes) == 1

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

        with pytest.raises(error, match=re.escape(message)):
            page_geometry(handle, index)

    def test_non_handle_raises(self):
        with pytest.raises(TypeError, match=re.escape("handle must be a DocumentHandle, got str")):
            page_geometry("doc.pdf", 0)

    def test_closed_handle_raises(self, tmp_path):
        handle = open_document(three_pages(tmp_path))
        handle.close()

        with pytest.raises(ValueError, match="document handle is closed"):
            page_geometry(handle, 0)

    def test_unloadable_page_raises_corrupt_error_for_that_page_only(self, tmp_path, page_closes):
        path = break_page(three_pages(tmp_path), 2)
        handle = open_document(path)

        with pytest.raises(PDFCorruptError, match="Failed to load page") as caught:
            page_geometry(handle, 1)

        assert caught.value.page_number == 2
        assert [page_geometry(handle, index).page_number for index in (0, 2)] == [1, 3]
        assert len(page_closes) == 2  # pages 1 and 3; page 2 never loaded

    def test_empty_visible_box_raises_corrupt_error(self, tmp_path):
        # A CropBox outside the MediaBox: pdfium intersects them to nothing.
        pdf = pdfium.PdfDocument(one_page(tmp_path))
        pdf[0].set_cropbox(500.0, 500.0, 600.0, 600.0)
        path = tmp_path / "empty.pdf"
        pdf.save(path)

        with pytest.raises(PDFCorruptError, match="no visible area") as caught:
            page_geometry(open_document(path), 0)
        assert caught.value.page_number == 1


def rotate_point(u: float, v: float, rotation: int, width: float, height: float) -> tuple[float, float]:
    """Turn top-left point (u, v) of an unrotated ``width`` x ``height`` page clockwise."""
    return {0: (u, v), 90: (height - v, u), 180: (width - u, height - v), 270: (v, width - u)}[rotation]


# Which displayed corner, as (x index, y index) into (x0, y0, x1, y1), a
# glyph's unrotated (left, baseline) corner becomes after each rotation.
BASELINE_CORNER = {0: (0, 3), 90: (0, 1), 180: (2, 1), 270: (2, 3)}


class TestDisplayMapping:
    """page_geometry + to_display_box put pdfium boxes where the page shows them."""

    @pytest.mark.parametrize("crop_box", CROPS)
    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_text_run_lands_at_its_rotated_position(self, tmp_path, rotate, crop_box):
        x, y = 120.0, 150.0
        path = one_page(tmp_path, TextRun("T", x=x, y=y, size=20.0), rotate=rotate, crop_box=crop_box)
        geometry_ = page_geometry(open_document(path), 0)
        charbox = pdfium.PdfDocument(path)[0].get_textpage().get_charbox(0)

        box = to_display_box(charbox, rotation_deg=geometry_.rotation_deg, page_box=geometry_.page_box)

        crop_x0, crop_y0, crop_x1, crop_y1 = crop_box or (0.0, 0.0, 400.0, 300.0)
        expected = rotate_point(x - crop_x0, y - crop_y0, rotate, crop_x1 - crop_x0, crop_y1 - crop_y0)
        x_index, y_index = BASELINE_CORNER[rotate]
        assert (box[x_index], box[y_index]) == pytest.approx(expected, abs=TOL)

    @pytest.mark.parametrize("crop_box", CROPS)
    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_rect_lands_on_its_rendered_pixels(self, tmp_path, rotate, crop_box):
        # pdfium's renderer is the independent oracle for the displayed page.
        rect = Rect(100.0, 60.0, 40.0, 30.0, fill_rgb=(0.0, 0.0, 0.0), stroke=False)
        path = one_page(tmp_path, rect, rotate=rotate, crop_box=crop_box)
        geometry_ = page_geometry(open_document(path), 0)
        page = pdfium.PdfDocument(path)[0]
        (path_object,) = page.get_objects(filter=(pdfium_c.FPDF_PAGEOBJ_PATH,))

        box = to_display_box(path_object.get_bounds(), rotation_deg=geometry_.rotation_deg, page_box=geometry_.page_box)

        image = page.render(scale=1).to_pil().convert("L")
        assert image.size == (round(geometry_.width_pt), round(geometry_.height_pt))
        assert box == pytest.approx(image.point(lambda p: 255 if p < 128 else 0).getbbox(), abs=TOL)
