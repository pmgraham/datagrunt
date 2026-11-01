"""Tests for the v5 page-level types: ``PageImage``, ``Page`` and ``Section``."""

import dataclasses
import hashlib
import math
import os
import re
from pathlib import Path

import pytest

import datagrunt.pdf.elements as elements_module
from datagrunt.pdf import (
    BBox,
    BulletList,
    DatagruntPDFError,
    Figure,
    Heading,
    KeyValue,
    Page,
    PageImage,
    PageQuality,
    Paragraph,
    Section,
    Table,
)
from datagrunt.pdf.elements import make_element_id

PNG = b"page-png"
BANDS = (b"band-one", b"band-two")
FIGURE_SHA = hashlib.sha256(b"figure-bytes").hexdigest()


def _quality():
    return PageQuality(
        valid_char_ratio=0.98,
        char_count=1200,
        vector_line_count=14,
        hidden_spans_flagged=0,
        rotation_deg=0,
        deskew_angle_deg=0.0,
        ocr_mean_confidence=None,
    )


def _bbox(**overrides):
    kwargs = {"x0": 72, "y0": 72, "x1": 540, "y1": 90}
    kwargs.update(overrides)
    return BBox(**kwargs)


def _ids(index, *, page=1, scope_path=()):
    return {
        "element_id": make_element_id(page, index),
        "page_number": page,
        "reading_order": index,
        "bbox": _bbox(),
        "scope_path": scope_path,
    }


def _paragraph(index, *, page=1, **overrides):
    kwargs = {
        "element_id": make_element_id(page, index),
        "page_number": page,
        "reading_order": index,
        "bbox": _bbox(),
        "text": f"Paragraph {index}.",
    }
    kwargs.update(overrides)
    return Paragraph(**kwargs)


def _page(**overrides):
    kwargs = {
        "page_number": 1,
        "width_pt": 612,
        "height_pt": 792,
        "status": "ok",
        "route": "digital",
        # An error page measured nothing, so it has no quality signals.
        "quality": None if overrides.get("status") == "error" else _quality(),
    }
    kwargs.update(overrides)
    return Page(**kwargs)


OFF_PAGE_BBOX = BBox(x0=-500, y0=-20, x1=-400, y1=-10)


def _page_image(**overrides):
    kwargs = {"page_number": 1, "dpi": 300, "width_px": 2550, "height_px": 3300}
    kwargs.update(overrides)
    return PageImage(**kwargs)


def _write(tmp_path, name, data):
    path = tmp_path / name
    path.write_bytes(data)
    return path


class TestPageImageFields:
    def test_defaults(self):
        image = _page_image()
        assert (image.image_path, image.band_paths, image.contrast_normalized) == (None, (), False)

    def test_all_fields(self):
        image = _page_image(
            image_path=Path("page_0001.png"),
            band_paths=(Path("b1.png"), Path("b2.png")),
            contrast_normalized=True,
        )
        assert image.image_path == Path("page_0001.png")
        assert image.band_paths == (Path("b1.png"), Path("b2.png"))
        assert image.contrast_normalized is True

    @pytest.mark.parametrize(("field", "minimum"), [("page_number", 1), ("width_px", 1), ("height_px", 1)])
    def test_int_fields_accept_minimum(self, field, minimum):
        assert getattr(_page_image(**{field: minimum}), field) == minimum

    @pytest.mark.parametrize(("field", "minimum"), [("page_number", 1), ("width_px", 1), ("height_px", 1)])
    def test_int_fields_reject_minimum_minus_one(self, field, minimum):
        with pytest.raises(ValueError, match=f"{field} must be >= {minimum}"):
            _page_image(**{field: minimum - 1})

    @pytest.mark.parametrize("field", ["page_number", "width_px", "height_px"])
    @pytest.mark.parametrize("bad", [True, 300.0, "300"])
    def test_int_fields_wrong_type_rejected(self, field, bad):
        with pytest.raises(TypeError, match=f"{field} must be an int"):
            _page_image(**{field: bad})

    @pytest.mark.parametrize("dpi", [300, 72.5, 35, 0.25, 1e-9])
    def test_dpi_is_any_positive_finite_number_coerced_to_float(self, dpi):
        # Pixel-bounded renders give fractional dpi, and a huge page can render below 36 dpi.
        image = _page_image(dpi=dpi)
        assert image.dpi == float(dpi)
        assert type(image.dpi) is float

    @pytest.mark.parametrize(
        ("bad", "match"),
        [
            (0, "dpi must be > 0"),
            (-72, "dpi must be > 0"),
            (math.nan, "dpi must be finite"),
            (math.inf, "dpi must be finite"),
        ],
    )
    def test_dpi_must_be_positive_and_finite(self, bad, match):
        with pytest.raises(ValueError, match=match):
            _page_image(dpi=bad)

    @pytest.mark.parametrize("bad", [True, "300", None])
    def test_dpi_wrong_type_rejected(self, bad):
        with pytest.raises(TypeError, match="dpi must be an int or float"):
            _page_image(dpi=bad)

    def test_image_path_must_be_a_path(self):
        with pytest.raises(TypeError, match="image_path must be a pathlib.Path or None"):
            _page_image(image_path="page.png")

    @pytest.mark.parametrize(
        ("bad", "match"),
        [
            ([Path("b1.png")], "band_paths must be a tuple"),
            (Path("b1.png"), "band_paths must be a tuple"),
            ((Path("b1.png"), "b2.png"), r"band_paths\[1\] must be a pathlib.Path"),
        ],
    )
    def test_band_paths_must_be_tuple_of_paths(self, bad, match):
        with pytest.raises(TypeError, match=match):
            _page_image(band_paths=bad)

    @pytest.mark.parametrize("bad", [1, 0, None, "yes"])
    def test_contrast_normalized_must_be_bool(self, bad):
        with pytest.raises(TypeError, match="contrast_normalized must be a bool"):
            _page_image(contrast_normalized=bad)

    @pytest.mark.parametrize("field", ["_loader", "_band_loader"])
    def test_loaders_must_be_callable(self, field):
        with pytest.raises(TypeError, match=f"{field} must be callable or None"):
            _page_image(**{field: PNG})

    def test_storage_paths_excluded_from_equality_and_hash(self):
        # Where the image and bands were written is storage, not identity.
        stored = _page_image(image_path=Path("a/p.png"), band_paths=(Path("a/b1.png"),))
        moved = _page_image(image_path=Path("b/p.png"), band_paths=(Path("b/b1.png"), Path("b/b2.png")))
        assert stored == moved == _page_image()
        assert hash(stored) == hash(moved) == hash(_page_image())
        assert "band_paths=(" in repr(stored)

    def test_loaders_excluded_from_equality_hash_and_repr(self):
        with_loaders = _page_image(_loader=lambda: PNG, _band_loader=lambda: BANDS)
        assert with_loaders == _page_image()
        assert hash(with_loaders) == hash(_page_image())
        assert "_loader" not in repr(with_loaders)
        assert "_band_loader" not in repr(with_loaders)

    def test_is_frozen_slotted_and_keyword_only(self):
        image = _page_image()
        with pytest.raises(dataclasses.FrozenInstanceError):
            image.dpi = 72
        assert not hasattr(image, "__dict__")
        # Every field supplied positionally: only kw_only can make this a TypeError.
        positional = [getattr(image, f.name) for f in dataclasses.fields(image)]
        with pytest.raises(TypeError, match="positional argument"):
            PageImage(*positional)

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="dpi must be > 0"):
            dataclasses.replace(_page_image(), dpi=0)


class TestPageImageReadBytes:
    def test_reads_file(self, tmp_path):
        assert _page_image(image_path=_write(tmp_path, "p.png", PNG)).read_bytes() == PNG

    def test_loader_takes_precedence_over_file(self, tmp_path):
        # A bundle's loaded page image keeps image_path for information; its loader verifies each read.
        image = _page_image(image_path=_write(tmp_path, "p.png", b"unverified"), _loader=lambda: PNG)
        assert image.read_bytes() == PNG

    def test_reads_loader_when_no_path(self):
        assert _page_image(_loader=lambda: PNG).read_bytes() == PNG

    def test_no_byte_source_raises(self):
        with pytest.raises(DatagruntPDFError, match="page image 1 has no image bytes"):
            _page_image().read_bytes()

    def test_bands_are_not_a_source_for_the_full_image(self, tmp_path):
        image = _page_image(band_paths=(_write(tmp_path, "b1.png", BANDS[0]),), _band_loader=lambda: BANDS)
        with pytest.raises(DatagruntPDFError, match="no image bytes"):
            image.read_bytes()

    def test_missing_file_fails_loud(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _page_image(image_path=tmp_path / "gone.png").read_bytes()

    @pytest.mark.parametrize("bad", [None, "page-png", bytearray(PNG)])
    def test_loader_returning_non_bytes_raises(self, bad):
        with pytest.raises(TypeError, match="_loader result must be bytes"):
            _page_image(_loader=lambda: bad).read_bytes()

    def test_directory_path_rejected_before_opening(self, tmp_path):
        with pytest.raises(DatagruntPDFError, match="is not a regular file"):
            _page_image(image_path=tmp_path).read_bytes()

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs POSIX named pipes")
    def test_named_pipe_rejected_without_blocking(self, tmp_path):
        fifo = tmp_path / "p.png"
        os.mkfifo(fifo)
        with pytest.raises(DatagruntPDFError, match="is not a regular file"):
            _page_image(image_path=fifo).read_bytes()


class TestPageImageReadBands:
    def test_reads_band_files_in_order(self, tmp_path):
        paths = (_write(tmp_path, "b1.png", BANDS[0]), _write(tmp_path, "b2.png", BANDS[1]))
        assert _page_image(band_paths=paths).read_bands() == BANDS

    def test_band_loader_takes_precedence_over_band_files(self, tmp_path):
        image = _page_image(band_paths=(_write(tmp_path, "b1.png", b"unverified"),), _band_loader=lambda: BANDS)
        assert image.read_bands() == BANDS

    def test_reads_band_loader_when_no_band_paths(self):
        assert _page_image(_band_loader=lambda: BANDS).read_bands() == BANDS

    def test_no_band_source_raises(self):
        with pytest.raises(DatagruntPDFError, match="page image 1 has no bands"):
            _page_image().read_bands()

    def test_full_image_is_not_a_source_for_bands(self, tmp_path):
        image = _page_image(image_path=_write(tmp_path, "p.png", PNG), _loader=lambda: PNG)
        with pytest.raises(DatagruntPDFError, match="no bands"):
            image.read_bands()

    def test_missing_band_file_fails_loud(self, tmp_path):
        paths = (_write(tmp_path, "b1.png", BANDS[0]), tmp_path / "gone.png")
        with pytest.raises(FileNotFoundError):
            _page_image(band_paths=paths).read_bands()

    @pytest.mark.parametrize("bad", [list(BANDS), BANDS[0], None])
    def test_band_loader_returning_non_tuple_raises(self, bad):
        with pytest.raises(TypeError, match="_band_loader must return a tuple of bytes"):
            _page_image(_band_loader=lambda: bad).read_bands()

    def test_directory_band_path_rejected_before_opening(self, tmp_path):
        paths = (_write(tmp_path, "b1.png", BANDS[0]), tmp_path)
        with pytest.raises(DatagruntPDFError, match="is not a regular file"):
            _page_image(band_paths=paths).read_bands()

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs POSIX named pipes")
    def test_named_pipe_band_rejected_without_blocking(self, tmp_path):
        fifo = tmp_path / "b2.png"
        os.mkfifo(fifo)
        paths = (_write(tmp_path, "b1.png", BANDS[0]), fifo)
        with pytest.raises(DatagruntPDFError, match="is not a regular file"):
            _page_image(band_paths=paths).read_bands()

    def test_band_loader_returning_non_bytes_band_raises(self):
        with pytest.raises(TypeError, match=r"_band_loader result\[1\] must be bytes"):
            _page_image(_band_loader=lambda: (BANDS[0], "band-two")).read_bands()


@pytest.mark.parametrize(
    "method", [Figure.read_bytes, PageImage.read_bytes, PageImage.read_bands], ids=lambda m: m.__qualname__
)
def test_asset_read_docstrings_list_every_raised_error(method):
    raises = method.__doc__.split("Raises:", 1)[1]
    for error in ("DatagruntPDFError", "OSError", "TypeError"):
        assert f"{error}:" in raises, f"{method.__qualname__} docstring does not list {error}"


class TestPageImageReadsBounded:
    """Asset reads are capped at ``_MAX_ASSET_BYTES`` (patched small here, never a real 1 GiB file)."""

    def test_image_file_at_limit_is_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(PNG))
        assert _page_image(image_path=_write(tmp_path, "p.png", PNG)).read_bytes() == PNG

    def test_image_file_over_limit_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(PNG) - 1)
        with pytest.raises(DatagruntPDFError, match=f"{len(PNG)} bytes, over the {len(PNG) - 1}-byte asset limit"):
            _page_image(image_path=_write(tmp_path, "p.png", PNG)).read_bytes()

    def test_loader_result_at_limit_is_returned(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(PNG))
        assert _page_image(_loader=lambda: PNG).read_bytes() == PNG

    def test_loader_result_over_limit_rejected(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(PNG) - 1)
        with pytest.raises(DatagruntPDFError, match=f"page image 1: _loader result is {len(PNG)} bytes"):
            _page_image(_loader=lambda: PNG).read_bytes()

    def test_band_file_over_limit_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(BANDS[0]) - 1)
        paths = (_write(tmp_path, "b1.png", BANDS[0]),)
        with pytest.raises(DatagruntPDFError, match=f"{len(BANDS[0])} bytes, over the"):
            _page_image(band_paths=paths).read_bands()

    def test_band_files_totalling_exactly_the_limit_are_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(BANDS[0]) + len(BANDS[1]))
        paths = (_write(tmp_path, "b1.png", BANDS[0]), _write(tmp_path, "b2.png", BANDS[1]))
        assert _page_image(band_paths=paths).read_bands() == BANDS

    def test_band_files_total_over_limit_rejected(self, tmp_path, monkeypatch):
        # Each band fits on its own; together they exceed the limit.
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(BANDS[0]))
        paths = (_write(tmp_path, "b1.png", BANDS[0]), _write(tmp_path, "b2.png", BANDS[1]))
        with pytest.raises(DatagruntPDFError, match="page image 1: band files total 16 bytes, over the 8-byte"):
            _page_image(band_paths=paths).read_bands()

    def test_band_files_growing_past_limit_while_read_rejected(self, tmp_path, monkeypatch):
        # stat() understates the size: the running cap must still stop the read.
        class _StaleStatPath(type(tmp_path)):
            def stat(self, *args, **kwargs):
                return os.stat_result((0o100644, 0, 0, 1, 0, 0, 0, 0, 0, 0))

        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(BANDS[0]) + 1)
        paths = tuple(_StaleStatPath(_write(tmp_path, f"b{i}.png", band)) for i, band in enumerate(BANDS))
        with pytest.raises(DatagruntPDFError, match="page image 1: band files grew past the 9-byte asset limit"):
            _page_image(band_paths=paths).read_bands()

    def test_band_loader_results_totalling_exactly_the_limit_are_returned(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(BANDS[0]) + len(BANDS[1]))
        assert _page_image(_band_loader=lambda: BANDS).read_bands() == BANDS

    def test_band_loader_results_total_over_limit_rejected(self, monkeypatch):
        monkeypatch.setattr(elements_module, "_MAX_ASSET_BYTES", len(BANDS[0]))
        with pytest.raises(
            DatagruntPDFError, match="page image 1: _band_loader results total 16 bytes, over the 8-byte"
        ):
            _page_image(_band_loader=lambda: BANDS).read_bands()

    def test_band_loader_returning_no_bands_raises(self):
        with pytest.raises(DatagruntPDFError, match="page image 1: _band_loader returned no bands"):
            _page_image(_band_loader=lambda: ()).read_bands()


def _one_of_each(page=1):
    """One element of every DocumentElement type, in reading order on ``page``."""
    base = {"page_number": page, "bbox": _bbox()}
    return (
        Heading(
            element_id=make_element_id(page, 0), reading_order=0, level=1, text="Report", scope_path=("Report",), **base
        ),
        Paragraph(element_id=make_element_id(page, 1), reading_order=1, text="# Body.", **base),
        KeyValue(element_id=make_element_id(page, 2), reading_order=2, key="Invoice", value="42", **base),
        Table(
            element_id=make_element_id(page, 3),
            reading_order=3,
            headers=("a", "b"),
            rows=(("1", "2"),),
            extraction_method="lattice",
            **base,
        ),
        BulletList(element_id=make_element_id(page, 4), reading_order=4, items=("x", "y"), ordered=False, **base),
        Figure(
            element_id=make_element_id(page, 5), reading_order=5, sha256=FIGURE_SHA, width_px=8, height_px=8, **base
        ),
    )


class TestPageFields:
    def test_defaults(self):
        page = _page()
        assert page.elements == ()
        assert page.page_image is None
        assert page.integrity_verified is False
        assert page.error is None
        assert page.warnings == ()

    def test_dimensions_coerced_to_float(self):
        page = _page(width_pt=612, height_pt=792)
        assert (page.width_pt, page.height_pt) == (612.0, 792.0)
        assert type(page.width_pt) is float and type(page.height_pt) is float

    @pytest.mark.parametrize("field", ["width_pt", "height_pt"])
    def test_dimensions_accept_small_positive(self, field):
        assert getattr(_page(**{field: 0.001}), field) == 0.001

    @pytest.mark.parametrize("field", ["width_pt", "height_pt"])
    @pytest.mark.parametrize(("bad", "shown"), [(0, "0.0"), (0.0, "0.0"), (-1, "-1.0"), (-0.5, "-0.5")])
    def test_dimensions_must_be_positive(self, field, bad, shown):
        with pytest.raises(ValueError, match=rf"^{field} must be > 0, got {re.escape(shown)}$"):
            _page(**{field: bad})

    @pytest.mark.parametrize("field", ["width_pt", "height_pt"])
    @pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
    def test_dimensions_must_be_finite(self, field, bad):
        with pytest.raises(ValueError, match=f"{field} must be finite"):
            _page(**{field: bad})

    @pytest.mark.parametrize("field", ["width_pt", "height_pt"])
    @pytest.mark.parametrize("bad", [True, "612", None])
    def test_dimensions_wrong_type_rejected(self, field, bad):
        with pytest.raises(TypeError, match=f"{field} must be an int or float"):
            _page(**{field: bad})

    def test_page_number_zero_rejected(self):
        with pytest.raises(ValueError, match="page_number must be >= 1"):
            _page(page_number=0)

    @pytest.mark.parametrize("bad", [True, 1.0])
    def test_page_number_wrong_type_rejected(self, bad):
        with pytest.raises(TypeError, match="page_number must be an int"):
            _page(page_number=bad)

    @pytest.mark.parametrize(("bad", "exc"), [("failed", ValueError), ("OK", ValueError), (None, TypeError)])
    def test_bad_status_rejected(self, bad, exc):
        with pytest.raises(exc, match="status must be"):
            _page(status=bad)

    @pytest.mark.parametrize("route", ["digital", "hybrid", "vision"])
    def test_routes_accepted(self, route):
        assert _page(route=route).route == route

    @pytest.mark.parametrize(("bad", "exc"), [("ocr", ValueError), ("", ValueError), (1, TypeError)])
    def test_bad_route_rejected(self, bad, exc):
        with pytest.raises(exc, match="route must be"):
            _page(route=bad)

    def test_quality_must_be_page_quality(self):
        with pytest.raises(TypeError, match="quality must be a PageQuality"):
            _page(quality={"char_count": 1})

    @pytest.mark.parametrize(
        ("bad", "match"),
        [
            ([], "elements must be a tuple"),
            ((_bbox(),), r"elements\[0\] must be a document element"),
            (("text",), r"elements\[0\] must be a document element"),
        ],
    )
    def test_elements_must_be_tuple_of_document_elements(self, bad, match):
        with pytest.raises(TypeError, match=match):
            _page(elements=bad)

    def test_page_image_must_be_page_image(self):
        with pytest.raises(TypeError, match="page_image must be a PageImage or None"):
            _page(page_image=Path("page.png"))

    @pytest.mark.parametrize("bad", [1, 0, None])
    def test_integrity_verified_must_be_bool(self, bad):
        with pytest.raises(TypeError, match="integrity_verified must be a bool"):
            _page(integrity_verified=bad)

    def test_error_must_be_str_or_none(self):
        with pytest.raises(TypeError, match="error must be a str or None"):
            _page(status="error", error=500)

    def test_warnings_are_kept(self):
        assert _page(warnings=("tesseract not installed",)).warnings == ("tesseract not installed",)

    @pytest.mark.parametrize(
        ("bad", "exc", "match"),
        [
            (["w"], TypeError, r"^warnings must be a tuple of str, got list$"),
            (("w", 1), TypeError, r"^warnings\[1\] must be a str, got 1 \(int\)$"),
            (("w", " "), ValueError, r"^warnings\[1\] must be a non-blank str, got ' '$"),
        ],
    )
    def test_bad_warnings_rejected(self, bad, exc, match):
        with pytest.raises(exc, match=match):
            _page(warnings=bad)

    def test_is_frozen_slotted_and_keyword_only(self):
        page = _page()
        with pytest.raises(dataclasses.FrozenInstanceError):
            page.status = "error"
        assert not hasattr(page, "__dict__")
        # Every field supplied positionally: only kw_only can make this a TypeError.
        positional = [getattr(page, f.name) for f in dataclasses.fields(page)]
        with pytest.raises(TypeError, match="positional argument"):
            Page(*positional)

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="status 'error' requires a non-blank error"):
            dataclasses.replace(_page(), status="error")

    def test_equal_and_hashable(self):
        assert _page(elements=_one_of_each()) == _page(elements=_one_of_each())
        assert hash(_page(elements=_one_of_each())) == hash(_page(elements=_one_of_each()))


class TestPageStatusInvariants:
    def test_error_page(self):
        page = _page(status="error", error="pdfium: bad xref", warnings=("retried once",))
        assert (page.status, page.error, page.elements) == ("error", "pdfium: bad xref", ())

    @pytest.mark.parametrize("route", ["digital", "hybrid", "vision"])
    def test_error_page_on_any_route(self, route):
        assert _page(status="error", route=route, error="boom").route == route

    @pytest.mark.parametrize("error", [None, "", " \n"])
    def test_error_status_requires_non_blank_error(self, error):
        with pytest.raises(ValueError, match="status 'error' requires a non-blank error message"):
            _page(status="error", error=error)

    @pytest.mark.parametrize(
        ("status", "route"), [("ok", "digital"), ("ocr_fallback", "vision"), ("ocr_unavailable", "vision")]
    )
    @pytest.mark.parametrize("error", ["boom", ""])
    def test_error_message_only_on_error_status(self, status, route, error):
        with pytest.raises(ValueError, match="error must be None unless status is 'error'"):
            _page(status=status, route=route, error=error)

    def test_error_page_cannot_have_elements(self):
        with pytest.raises(ValueError, match="an error page must have no elements, got 1"):
            _page(status="error", error="boom", elements=(_paragraph(0),))

    def test_error_page_cannot_be_integrity_verified(self):
        with pytest.raises(ValueError, match="an error page cannot be integrity_verified"):
            _page(status="error", error="boom", integrity_verified=True)

    def test_error_page_has_no_quality(self):
        assert _page(status="error", error="boom").quality is None

    def test_error_page_with_quality_rejected(self):
        # Nothing was extracted, so any quality signals would be invented.
        with pytest.raises(ValueError, match="an error page must have quality None, got a PageQuality"):
            _page(status="error", error="boom", quality=_quality())

    @pytest.mark.parametrize(("status", "route"), [("ok", "digital"), ("ok", "vision"), ("ocr_fallback", "vision")])
    def test_non_error_page_requires_quality(self, status, route):
        with pytest.raises(ValueError, match=f"status {status!r} requires a PageQuality, got quality=None"):
            _page(status=status, route=route, quality=None)

    def test_ocr_fallback_on_vision_route(self):
        page = _page(status="ocr_fallback", route="vision", elements=(_paragraph(0),))
        assert (page.status, page.route) == ("ocr_fallback", "vision")

    @pytest.mark.parametrize("route", ["digital", "hybrid"])
    def test_ocr_fallback_on_non_vision_route_rejected(self, route):
        with pytest.raises(ValueError, match=f"status 'ocr_fallback' requires route 'vision', got route='{route}'"):
            _page(status="ocr_fallback", route=route)

    @pytest.mark.parametrize("route", ["digital", "hybrid"])
    def test_non_vision_page_can_be_integrity_verified(self, route):
        assert _page(route=route, integrity_verified=True).integrity_verified is True

    @pytest.mark.parametrize("status", ["ok", "ocr_fallback", "ocr_unavailable"])
    def test_vision_page_cannot_be_integrity_verified(self, status):
        with pytest.raises(ValueError, match="a vision page cannot be integrity_verified"):
            _page(status=status, route="vision", integrity_verified=True)

    def test_vision_page_without_ocr_is_ocr_unavailable_and_keeps_digital_spans(self):
        page = _page(
            status="ocr_unavailable",
            route="vision",
            elements=(_paragraph(0),),
            warnings=("OCR unavailable: tesseract not found",),
        )
        assert (page.status, page.route, page.elements) == ("ocr_unavailable", "vision", (_paragraph(0),))
        assert page.warnings == ("OCR unavailable: tesseract not found",)

    @pytest.mark.parametrize("route", ["digital", "hybrid"])
    def test_ocr_unavailable_on_non_vision_route_rejected(self, route):
        with pytest.raises(ValueError, match=f"status 'ocr_unavailable' requires route 'vision', got route='{route}'"):
            _page(status="ocr_unavailable", route=route)


class TestPageElements:
    def test_every_element_type_accepted_in_order(self):
        elements = _one_of_each()
        assert _page(elements=elements).elements == elements

    def test_reading_order_may_skip_values(self):
        elements = (_paragraph(2), _paragraph(5), _paragraph(9))
        assert _page(elements=elements).elements == elements

    def test_element_page_number_mismatch_rejected(self):
        with pytest.raises(ValueError, match=r"elements\[0\] \(p0002_e000\) has page_number 2, expected 1"):
            _page(elements=(_paragraph(0, page=2),))

    def test_duplicate_element_ids_rejected(self):
        # An id encodes (page, reading_order), so equal ids mean an equal reading order too.
        twins = (_paragraph(0), _paragraph(0))
        with pytest.raises(ValueError, match="duplicate element_id 'p0001_e000'"):
            _page(elements=twins)

    @pytest.mark.parametrize("second_order", [2, 0])
    def test_non_increasing_reading_order_rejected(self, second_order):
        elements = (_paragraph(3), _paragraph(second_order))
        with pytest.raises(
            ValueError, match=rf"elements\[1\] \(p0001_e00{second_order}\) reading_order {second_order} must be"
        ):
            _page(elements=elements)

    def test_off_page_element_rejected_unless_flagged(self):
        with pytest.raises(ValueError, match=r"elements\[0\] \(p0001_e000\) bbox lies outside the 612.0 x 792.0 pt"):
            _page(elements=(_paragraph(0, bbox=OFF_PAGE_BBOX),))

    @pytest.mark.parametrize("reasons", [("off_page",), ("off_page", "tiny_font")])
    def test_off_page_element_allowed_when_flagged(self, reasons):
        element = _paragraph(0, bbox=OFF_PAGE_BBOX, hidden_reasons=reasons)
        assert _page(elements=(element,)).elements == (element,)

    @pytest.mark.parametrize("reasons", [("invisible",), ("tiny_font", "transparent")])
    def test_other_hidden_reasons_do_not_excuse_an_off_page_bbox(self, reasons):
        with pytest.raises(ValueError, match="bbox lies outside"):
            _page(elements=(_paragraph(0, bbox=OFF_PAGE_BBOX, hidden_reasons=reasons),))

    @pytest.mark.parametrize("reasons", [("off_page",), ("off_page", "invisible")])
    @pytest.mark.parametrize("bbox", [BBox(x0=72, y0=72, x1=540, y1=90), BBox(x0=-0.5, y0=0, x1=612.5, y1=10)])
    def test_off_page_flag_on_an_on_page_bbox_rejected(self, reasons, bbox):
        with pytest.raises(
            ValueError,
            match=r"elements\[0\] \(p0001_e000\) is flagged 'off_page' but its bbox lies within the 612.0 x 792.0 pt",
        ):
            _page(elements=(_paragraph(0, bbox=bbox, hidden_reasons=reasons),))

    def test_bbox_on_page_edge_within_tolerance_accepted(self):
        element = _paragraph(0, bbox=BBox(x0=-0.5, y0=0, x1=612.5, y1=792.5))
        assert _page(elements=(element,)).elements == (element,)

    def test_bbox_just_beyond_tolerance_rejected(self):
        with pytest.raises(ValueError, match="bbox lies outside"):
            _page(elements=(_paragraph(0, bbox=BBox(x0=0, y0=0, x1=612.51, y1=10)),))

    def test_page_image_for_this_page(self):
        image = _page_image(page_number=3)
        assert _page(page_number=3, page_image=image).page_image == image

    def test_page_image_page_mismatch_rejected(self):
        with pytest.raises(ValueError, match="page_image.page_number 2 must equal page_number 1"):
            _page(page_image=_page_image(page_number=2))


class TestPageMarkdown:
    def test_empty_page_is_the_header_comment(self):
        assert _page().to_markdown() == "<!-- page 1 route=digital status=ok -->"

    def test_elements_joined_by_blank_lines(self):
        page = _page(page_number=2, route="hybrid", elements=_one_of_each(page=2))
        assert page.to_markdown() == (
            "<!-- page 2 route=hybrid status=ok -->\n\n"
            "# Report\n\n"
            "\\# Body.\n\n"
            "**Invoice:** 42\n\n"
            "| a | b |\n| --- | --- |\n| 1 | 2 |\n\n"
            "- x\n- y\n\n"
            f"<!-- figure p0002_e005 sha256={FIGURE_SHA[:8]} -->"
        )

    def test_error_page_renders_header_only(self):
        page = _page(page_number=3, status="error", error="pdfium: bad xref -->")
        assert page.to_markdown() == "<!-- page 3 route=digital status=error -->"

    def test_ocr_unavailable_header(self):
        page = _page(status="ocr_unavailable", route="vision", elements=(_paragraph(0),))
        assert page.to_markdown() == "<!-- page 1 route=vision status=ocr_unavailable -->\n\nParagraph 0."

    def test_ocr_fallback_header(self):
        page = _page(status="ocr_fallback", route="vision", elements=(_paragraph(0),))
        assert page.to_markdown() == "<!-- page 1 route=vision status=ocr_fallback -->\n\nParagraph 0."

    def test_hidden_elements_are_rendered_with_a_marker_not_dropped(self):
        # Hidden spans are flagged, never dropped (spec review D2-b); the marker lets consumers filter them.
        hidden = _paragraph(1, bbox=OFF_PAGE_BBOX, hidden_reasons=("off_page",))
        page = _page(elements=(_paragraph(0), hidden))
        assert page.to_markdown() == (
            "<!-- page 1 route=digital status=ok -->\n\nParagraph 0.\n\n<!-- hidden: off_page -->\nParagraph 1."
        )

    def test_comment_in_text_cannot_swallow_following_elements(self):
        page = _page(elements=(_paragraph(0, text="<!-- draft"), _paragraph(1)))
        assert page.to_markdown() == "<!-- page 1 route=digital status=ok -->\n\n\\<!-- draft\n\nParagraph 1."

    def test_consecutive_lists_separated_by_empty_comment(self):
        lists = (
            BulletList(**_ids(0), items=("a",), ordered=False),
            BulletList(**_ids(1), items=("b",), ordered=True),
            BulletList(**_ids(2), items=("c",), ordered=True),
        )
        assert _page(elements=lists).to_markdown() == (
            "<!-- page 1 route=digital status=ok -->\n\n- a\n\n<!-- -->\n\n1. b\n\n<!-- -->\n\n1. c"
        )

    def test_lists_apart_get_no_separator(self):
        elements = (
            BulletList(**_ids(0), items=("a",), ordered=False),
            _paragraph(1),
            BulletList(**_ids(2), items=("b",), ordered=False),
        )
        assert _page(elements=elements).to_markdown() == (
            "<!-- page 1 route=digital status=ok -->\n\n- a\n\nParagraph 1.\n\n- b"
        )


# The scope of the sections built here: a heading's own scope_path ends with its text.
SCOPE = ("Report", "Findings")


def _heading(index=0, *, page=1, text="Findings", **overrides):
    kwargs = {
        "element_id": make_element_id(page, index),
        "page_number": page,
        "reading_order": index,
        "bbox": _bbox(),
        "level": 2,
        "text": text,
        "scope_path": ("Report", text),
    }
    kwargs.update(overrides)
    return Heading(**kwargs)


def _table(index, *, page=1):
    return Table(
        element_id=make_element_id(page, index),
        page_number=page,
        reading_order=index,
        bbox=_bbox(),
        headers=("k", "v"),
        rows=((f"t{page}.{index}", "1"),),
        extraction_method="stream",
    )


def _figure(index, *, page=1):
    return Figure(
        element_id=make_element_id(page, index),
        page_number=page,
        reading_order=index,
        bbox=_bbox(),
        sha256=FIGURE_SHA,
        width_px=8,
        height_px=8,
    )


def _section(**overrides):
    heading = _heading()
    kwargs = {
        "heading": heading,
        "scope_path": SCOPE,
        "page_span": (1, 2),
        "elements": (heading, _paragraph(1, scope_path=SCOPE), _paragraph(0, page=2, scope_path=SCOPE)),
    }
    kwargs.update(overrides)
    return Section(**kwargs)


class TestSectionFields:
    def test_valid_section_with_heading(self):
        section = _section()
        assert section.heading.text == "Findings"
        assert section.scope_path == ("Report", "Findings")
        assert section.page_span == (1, 2)
        assert [e.element_id for e in section.elements] == ["p0001_e000", "p0001_e001", "p0002_e000"]

    def test_section_without_heading(self):
        section = _section(heading=None, scope_path=(), page_span=(1, 1), elements=(_paragraph(0),))
        assert section.heading is None

    def test_scope_path_without_heading_need_not_end_with_a_heading_text(self):
        section = _section(heading=None, scope_path=("Report",), elements=(_paragraph(1, scope_path=("Report",)),))
        assert section.scope_path == ("Report",)

    @pytest.mark.parametrize("scope_path", [(), ("Report",), ("Report", "Findings", "Detail"), ("Other", "Findings")])
    def test_element_scope_must_equal_section_scope(self, scope_path):
        # Sections are flat: a nested scope is its own section, linked by the scope_path prefix.
        with pytest.raises(
            ValueError,
            match=re.escape(
                f"elements[1] (p0001_e001) has scope_path {scope_path!r}, but the section's scope_path is {SCOPE!r}"
            ),
        ):
            _section(elements=(_heading(), _paragraph(1, scope_path=scope_path)))

    def test_element_scope_checked_without_heading_too(self):
        with pytest.raises(ValueError, match=r"elements\[0\] \(p0001_e000\) has scope_path \('Report',\)"):
            _section(heading=None, scope_path=(), page_span=(1, 1), elements=(_paragraph(0, scope_path=("Report",)),))

    def test_heading_scope_must_equal_section_scope(self):
        heading = _heading(scope_path=("Other", "Findings"))
        with pytest.raises(
            ValueError,
            match=re.escape(
                "heading (p0001_e000) has scope_path ('Other', 'Findings'), "
                "but the section's scope_path is ('Report', 'Findings')"
            ),
        ):
            _section(heading=heading, elements=())

    def test_heading_in_elements_must_be_first(self):
        heading = _heading(1)
        with pytest.raises(
            ValueError, match=r"elements\[1\] \(p0001_e001\) is the section heading, so it must be elements\[0\]"
        ):
            _section(heading=heading, elements=(_paragraph(0, scope_path=SCOPE), heading))

    def test_heading_first_in_elements_accepted(self):
        heading = _heading(1)
        elements = (heading, _paragraph(2, scope_path=SCOPE))
        assert _section(heading=heading, elements=elements).elements == elements

    def test_empty_elements_allowed(self):
        assert _section(elements=()).elements == ()

    def test_heading_must_be_a_heading(self):
        with pytest.raises(TypeError, match="heading must be a Heading or None, got Paragraph"):
            _section(heading=_paragraph(0))

    def test_heading_requires_non_empty_scope_path(self):
        with pytest.raises(ValueError, match="scope_path must be non-empty when heading is set"):
            _section(scope_path=())

    @pytest.mark.parametrize("scope_path", [("Report", "Other"), ("Findings", "Report"), ("Report", "Findings ")])
    def test_scope_path_must_end_with_heading_text(self, scope_path):
        with pytest.raises(ValueError, match=r"scope_path\[-1\] .* must equal heading.text 'Findings'"):
            _section(scope_path=scope_path)

    @pytest.mark.parametrize(("bad", "exc"), [(["Report", "Findings"], TypeError), (("", "Findings"), ValueError)])
    def test_bad_scope_path_rejected(self, bad, exc):
        with pytest.raises(exc, match="scope_path"):
            _section(scope_path=bad)

    @pytest.mark.parametrize("span", [(1, 1), (1, 2), (3, 3), (1, 9999)])
    def test_page_span_bounds_inclusive(self, span):
        assert _section(heading=None, scope_path=(), elements=(), page_span=span).page_span == span

    def test_elements_on_both_span_end_pages(self):
        elements = (_paragraph(0, page=2), _paragraph(0, page=4))
        assert _section(heading=None, scope_path=(), page_span=(2, 4), elements=elements).elements == elements

    @pytest.mark.parametrize(
        ("span", "exc", "match"),
        [
            ([1, 2], TypeError, "page_span must be a tuple"),
            ((1,), ValueError, r"page_span must be a \(start, end\) pair, got 1 item"),
            ((1, 2, 3), ValueError, r"page_span must be a \(start, end\) pair, got 3 item"),
            ((0, 2), ValueError, r"page_span\[0\] must be >= 1"),
            ((1, 0), ValueError, r"page_span\[1\] must be >= 1"),
            ((True, 2), TypeError, r"page_span\[0\] must be an int"),
            ((1, 2.0), TypeError, r"page_span\[1\] must be an int"),
            ((3, 2), ValueError, "page_span start 3 must be <= end 2"),
        ],
    )
    def test_bad_page_span_rejected(self, span, exc, match):
        with pytest.raises(exc, match=match):
            _section(heading=None, scope_path=(), elements=(), page_span=span)

    @pytest.mark.parametrize(
        ("bad", "match"),
        [([], "elements must be a tuple"), ((_bbox(),), r"elements\[0\] must be a document element")],
    )
    def test_elements_must_be_tuple_of_document_elements(self, bad, match):
        with pytest.raises(TypeError, match=match):
            _section(elements=bad)

    @pytest.mark.parametrize(("span", "page"), [((2, 3), 1), ((1, 2), 3)])
    def test_element_outside_page_span_rejected(self, span, page):
        with pytest.raises(
            ValueError, match=rf"\(p000{page}_e000\) is on page {page}, outside page_span \({span[0]}, "
        ):
            _section(heading=None, scope_path=(), page_span=span, elements=(_paragraph(0, page=page),))

    @pytest.mark.parametrize(
        "elements",
        [
            (_paragraph(1), _paragraph(0)),  # same page, reading order goes back
            (_paragraph(0, page=2), _paragraph(5)),  # later page before an earlier one
            (_paragraph(0), _paragraph(0)),  # the same element twice
        ],
    )
    def test_elements_out_of_document_order_rejected(self, elements):
        with pytest.raises(ValueError, match=r"elements\[1\] .* is not after elements\[0\] .* in document order"):
            _section(heading=None, scope_path=(), elements=elements)

    def test_duplicate_element_ids_rejected(self):
        # Unreachable through validated elements (an id encodes its page and
        # reading order, which strictly increase here), so a forged element
        # stands in for corrupted input: the defensive check must still fire.
        forged = _paragraph(1)
        object.__setattr__(forged, "element_id", "p0001_e000")
        with pytest.raises(ValueError, match="elements has duplicate element_id 'p0001_e000'"):
            _section(heading=None, scope_path=(), elements=(_paragraph(0), forged))

    @pytest.mark.parametrize(
        "impostor",
        [_paragraph(0, text="body"), _heading(text="Other"), _heading(level=3)],
        ids=["paragraph", "heading-other-text", "heading-other-level"],
    )
    def test_element_sharing_heading_id_must_equal_heading(self, impostor):
        with pytest.raises(
            ValueError, match=r"elements\[0\] \(p0001_e000\) has the heading's element_id but differs from heading"
        ):
            _section(elements=(impostor,))

    @pytest.mark.parametrize("span", [(2, 3), (1, 1)])
    def test_heading_outside_page_span_rejected(self, span):
        heading = _heading(page=2) if span == (1, 1) else _heading()
        page = heading.page_number
        with pytest.raises(
            ValueError, match=rf"heading \(p000{page}_e000\) is on page {page}, outside page_span \({span[0]}, "
        ):
            _section(heading=heading, page_span=span, elements=())

    def test_heading_on_page_span_end_accepted(self):
        heading = _heading(page=3)
        assert _section(heading=heading, page_span=(2, 3), elements=()).heading == heading

    def test_is_frozen_slotted_and_keyword_only(self):
        section = _section()
        with pytest.raises(dataclasses.FrozenInstanceError):
            section.page_span = (1, 1)
        assert not hasattr(section, "__dict__")
        # Every field supplied positionally: only kw_only can make this a TypeError.
        positional = [getattr(section, f.name) for f in dataclasses.fields(section)]
        with pytest.raises(TypeError, match="positional argument"):
            Section(*positional)

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="outside page_span"):
            dataclasses.replace(_section(), page_span=(1, 1))

    def test_equal_and_hashable(self):
        assert _section() == _section()
        assert hash(_section()) == hash(_section())


class TestSectionDerived:
    def test_tables_and_figures_in_document_order(self):
        elements = (_table(0), _figure(1), _paragraph(2), _figure(0, page=2), _table(1, page=2))
        section = _section(heading=None, scope_path=(), elements=elements)
        assert section.tables == (elements[0], elements[4])
        assert section.figures == (elements[1], elements[3])

    def test_no_tables_or_figures(self):
        section = _section()
        assert (section.tables, section.figures) == ((), ())


class TestSectionMarkdown:
    def test_heading_among_elements_rendered_once(self):
        assert _section().to_markdown() == "## Findings\n\nParagraph 1.\n\nParagraph 0."

    def test_heading_not_among_elements_rendered_first(self):
        section = _section(elements=(_paragraph(1, scope_path=SCOPE),))
        assert section.to_markdown() == "## Findings\n\nParagraph 1."

    def test_heading_only_section(self):
        assert _section(elements=()).to_markdown() == "## Findings"

    def test_section_without_heading_renders_elements(self):
        section = _section(heading=None, scope_path=(), elements=(_paragraph(0), _table(1)))
        assert section.to_markdown() == "Paragraph 0.\n\n| k | v |\n| --- | --- |\n| t1.1 | 1 |"

    def test_comment_in_text_cannot_swallow_following_elements(self):
        section = _section(
            elements=(_heading(), _paragraph(1, text="<!-- draft", scope_path=SCOPE), _paragraph(2, scope_path=SCOPE))
        )
        assert section.to_markdown() == "## Findings\n\n\\<!-- draft\n\nParagraph 2."

    def test_consecutive_lists_separated_by_empty_comment(self):
        lists = (
            BulletList(**_ids(1, scope_path=SCOPE), items=("a",), ordered=True),
            BulletList(**_ids(2, scope_path=SCOPE), items=("b",), ordered=True),
        )
        assert _section(elements=lists).to_markdown() == "## Findings\n\n1. a\n\n<!-- -->\n\n1. b"

    def test_empty_section_renders_empty_string(self):
        assert _section(heading=None, scope_path=(), elements=()).to_markdown() == ""
