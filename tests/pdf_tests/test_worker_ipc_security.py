"""Security regression tests for zero-pickle JSON worker IPC (DG-01)."""

from __future__ import annotations

import errno
import io
import json
import pickle
from pathlib import Path

import pytest

from datagrunt.pdf import _loaders, _process, pipeline, render
from datagrunt.pdf._image_facts import RawImage
from datagrunt.pdf._source import SourceIdentity
from datagrunt.pdf.elements import BBox, Figure, Page, PageImage, PageQuality, Paragraph, make_element_id
from datagrunt.pdf.exceptions import (
    BundleCorruptError,
    DatagruntPDFError,
    PDFCorruptError,
    PDFEncryptedError,
    PDFSourceChangedError,
)


class _CustomWorkerException(Exception):
    """An exception outside the IPC allowlist."""


class TestJsonErrorReply:
    """_process.error_reply / read_error use strict allowlisted JSON and never unpickle."""

    def test_error_reply_is_valid_json_not_pickle(self) -> None:
        raw = _process.error_reply(PDFCorruptError("corrupt page", page_number=3))
        parsed = json.loads(raw.decode("ascii"))
        assert isinstance(parsed, dict)
        assert parsed["type"] == "PDFCorruptError"
        assert parsed["message"] == "corrupt page"
        assert parsed["page_number"] == 3

    @pytest.mark.parametrize(
        "exc",
        [
            DatagruntPDFError("generic pdf error"),
            PDFEncryptedError("encrypted"),
            PDFCorruptError("bad page", page_number=4),
            PDFCorruptError("bad doc", page_number=None),
            PDFSourceChangedError("changed"),
            BundleCorruptError("bad manifest"),
            FileNotFoundError(errno.ENOENT, "No such file", "/tmp/missing.pdf"),
            PermissionError(errno.EACCES, "Permission denied", "/tmp/locked.pdf"),
            ImportError("missing pypdfium2", name="pypdfium2"),
            ModuleNotFoundError("no PIL", name="PIL"),
            ValueError("bad value"),
            TypeError("bad type"),
            IndexError("out of range"),
            RuntimeError("runtime boom"),
        ],
    )
    def test_allowlisted_exceptions_round_trip_with_attributes(self, exc: Exception) -> None:
        raw = _process.error_reply(exc)
        restored = _process.read_error(raw, "worker")
        assert type(restored) is type(exc)
        assert str(restored) == str(exc)
        if isinstance(exc, PDFCorruptError):
            assert isinstance(restored, PDFCorruptError)
            assert restored.page_number == exc.page_number
        if isinstance(exc, OSError):
            assert isinstance(restored, OSError)
            assert restored.errno == exc.errno
            assert restored.filename == exc.filename
        if isinstance(exc, ImportError):
            assert isinstance(restored, ImportError)
            assert restored.name == exc.name

    def test_unlisted_exception_crosses_as_datagrunt_pdf_error(self) -> None:
        raw = _process.error_reply(_CustomWorkerException("boom"))
        restored = _process.read_error(raw, "worker")
        assert type(restored) is DatagruntPDFError
        assert str(restored) == "_CustomWorkerException: boom"

    def test_read_error_never_invokes_pickle_loads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        called: list[bytes] = []

        def _forbid(data: bytes, *_args: object, **_kwargs: object) -> object:
            called.append(data)
            raise AssertionError("pickle.loads must never be called on worker IPC")

        monkeypatch.setattr(pickle, "loads", _forbid)
        payload = pickle.dumps(ValueError("from pickle"))
        result = _process.read_error(payload, "render worker")
        assert not called
        assert type(result) is DatagruntPDFError
        assert "render worker" in str(result)

    def test_render_read_reply_rejects_pickled_error_without_unpickling(self, monkeypatch: pytest.MonkeyPatch) -> None:
        called: list[bytes] = []

        def _forbid(data: bytes, *_args: object, **_kwargs: object) -> object:
            called.append(data)
            raise AssertionError("pickle.loads must never be called by render._read_reply")

        monkeypatch.setattr(pickle, "loads", _forbid)
        stream = io.BytesIO()
        _process.send_message(stream, _process.ERROR + pickle.dumps(RuntimeError("forged")))
        stream.seek(0)
        with pytest.raises(DatagruntPDFError, match="the render worker"):
            render._read_reply(stream, 1 << 20, 1)
        assert not called

    @pytest.mark.parametrize(
        "bad_json",
        [
            b'{"type": "SystemExit", "message": "x"}',
            b'{"module": "os", "type": "system", "message": "x"}',
            b'{"module": "datagrunt.pdf.exceptions", "type": "PDFCorruptError", "message": "x", "page_number": true}',
            b'{"module": "datagrunt.pdf.exceptions", "type": "PDFCorruptError", "message": "x", "extra": 1}',
            b'"not a dict"',
            b"{not json",
        ],
    )
    def test_read_error_rejects_malformed_or_disallowed_json(self, bad_json: bytes) -> None:
        result = _process.read_error(bad_json, "page worker")
        assert type(result) is DatagruntPDFError
        assert "page worker" in str(result)


class TestJsonPageReply:
    """Page replies between _page_worker and pipeline use strict JSON and never unpickle."""

    def test_pipeline_reply_page_never_invokes_pickle_loads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _forbid(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("pickle.loads must never be called by pipeline._reply_page")

        monkeypatch.setattr(pickle, "loads", _forbid)
        with pytest.raises(DatagruntPDFError, match="worker"):
            pipeline._reply_page(b"P" + pickle.dumps(("not", "a", "page")), "worker")

    def test_page_ipc_round_trips_loaders_without_sending_password(self, tmp_path: Path) -> None:
        from datagrunt.pdf import _page_ipc

        pdf_path = tmp_path / "secret.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n")
        identity = SourceIdentity(device=1, inode=2, size=9, mtime_ns=123456)
        doc_sha = "a" * 64
        fig_sha = "b" * 64
        raw_img = RawImage(
            bbox=(10.0, 20.0, 110.0, 120.0),
            px_width=100,
            px_height=100,
            declared_px=(100, 100),
            object_index=2,
            raw_sha256=fig_sha,
            filters=("FlateDecode",),
            bits_per_pixel=24,
            colorspace="DeviceRGB",
        )
        fig_read = _loaders.FigureRead(
            source=pdf_path,
            page_index=0,
            image=raw_img,
            password="top-secret-password",
            memory_cap_bytes=1 << 26,
            sandbox=True,
        )
        page_draw = _loaders.PageDraw(
            source=pdf_path,
            page_index=0,
            dpi=150,
            width_pt=612.0,
            height_pt=792.0,
            password="top-secret-password",
            memory_cap_bytes=1 << 26,
            timeout_s=30.0,
            identity=identity,
            sha256=doc_sha,
            sandbox=True,
        )
        page = Page(
            page_number=1,
            width_pt=612.0,
            height_pt=792.0,
            status="ok",
            route="digital",
            quality=PageQuality(
                valid_char_ratio=1.0,
                char_count=12,
                vector_line_count=0,
                hidden_spans_flagged=0,
                rotation_deg=0,
                deskew_angle_deg=0.0,
                ocr_mean_confidence=None,
            ),
            elements=(
                Paragraph(
                    element_id=make_element_id(1, 0),
                    page_number=1,
                    reading_order=0,
                    bbox=BBox(x0=10.0, y0=10.0, x1=100.0, y1=30.0),
                    text="Surrogate \ud800 preserved",
                ),
                Figure(
                    element_id=make_element_id(1, 1),
                    page_number=1,
                    reading_order=1,
                    bbox=BBox(x0=10.0, y0=40.0, x1=110.0, y1=140.0),
                    sha256=fig_sha,
                    width_px=100,
                    height_px=100,
                    _loader=fig_read.png_bytes,
                    _raw_loader=fig_read.raw_bytes,
                ),
            ),
            page_image=PageImage(
                page_number=1,
                dpi=150.0,
                width_px=1275,
                height_px=1650,
                _loader=page_draw.png_bytes,
            ),
            integrity_verified=True,
        )

        wire = _page_ipc.encode_page_reply(page)
        assert b"top-secret-password" not in wire  # password never crosses worker stdout

        restored = _page_ipc.decode_page_reply(wire, expected_source=pdf_path, password="top-secret-password")
        assert restored == page
        assert restored.page_image is not None and restored.page_image._loader is not None
        restored_draw = getattr(restored.page_image._loader, "__self__", None)
        assert isinstance(restored_draw, _loaders.PageDraw)
        assert restored_draw.password == "top-secret-password"
        assert restored_draw.sandbox is True
        restored_fig = restored.elements[1]
        assert isinstance(restored_fig, Figure)
        assert restored_fig._loader is not None and restored_fig._raw_loader is not None
        restored_fig_read = getattr(restored_fig._loader, "__self__", None)
        assert isinstance(restored_fig_read, _loaders.FigureRead)
        assert restored_fig_read.password == "top-secret-password"
        assert restored_fig_read.sandbox is True

        # Mismatched expected_source must be rejected to prevent loader redirection.
        with pytest.raises(ValueError, match="expected source"):
            _page_ipc.decode_page_reply(wire, expected_source=tmp_path / "other.pdf", password=None)

    def test_page_ipc_round_trips_page_render_vision_loader(self, tmp_path: Path) -> None:
        from datagrunt.pdf import _page_ipc

        pdf_path = tmp_path / "scan.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n")
        identity = SourceIdentity(device=3, inode=4, size=9, mtime_ns=987654)
        doc_sha = "c" * 64
        page_render = _loaders.PageRender(
            source=pdf_path,
            page_index=0,
            dpi=200,
            rotation=90,
            password="vision-secret-password",
            memory_cap_bytes=1 << 26,
            timeout_s=15.0,
            identity=identity,
            sha256=doc_sha,
            sandbox=True,
        )
        page = Page(
            page_number=1,
            width_pt=612.0,
            height_pt=792.0,
            status="ocr_unavailable",
            route="vision",
            quality=PageQuality(
                valid_char_ratio=None,
                char_count=0,
                vector_line_count=0,
                hidden_spans_flagged=0,
                rotation_deg=90,
                deskew_angle_deg=0.0,
                ocr_mean_confidence=None,
            ),
            elements=(),
            page_image=PageImage(
                page_number=1,
                dpi=200.0,
                width_px=1700,
                height_px=2200,
                _loader=page_render.png_bytes,
                _band_loader=page_render.band_png_bytes,
            ),
            integrity_verified=False,
        )

        wire = _page_ipc.encode_page_reply(page)
        assert b"vision-secret-password" not in wire

        restored = _page_ipc.decode_page_reply(wire, expected_source=pdf_path, password="vision-secret-password")
        assert restored == page
        assert restored.page_image is not None
        assert restored.page_image._loader is not None
        assert restored.page_image._band_loader is not None
        restored_render = getattr(restored.page_image._loader, "__self__", None)
        assert isinstance(restored_render, _loaders.PageRender)
        assert getattr(restored.page_image._band_loader, "__self__", None) is restored_render
        assert restored_render.password == "vision-secret-password"
        assert restored_render.rotation == 90
        assert restored_render.sandbox is True
