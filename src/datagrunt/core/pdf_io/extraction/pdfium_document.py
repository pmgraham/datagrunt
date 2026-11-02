"""Object wrappers over pypdfium2 for safe, single-open page access.

Encapsulates ALL raw pdfium + ctypes usage so backends never touch raw handles.
``import datagrunt`` must work without the ``[pdf]`` extra, so pypdfium2 is
imported lazily.
"""

import ctypes
import hashlib
import logging
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from datagrunt.core.pdf_io.extraction.config import _DEFAULT_MIN_IMAGE_DIMENSION
from datagrunt.core.pdf_io.extraction.display_frame import display_edges
from datagrunt.core.pdf_io.extraction.image_guard import (
    _MAX_FILTER_ARRAY_ITEMS,
    _MAX_RAW_HEADER_BYTES,
    GuardDecision,
    inspect_image,
    normalize_filter,
)
from datagrunt.core.pdf_io.extraction.shapes import BBox, ImageBlock, TextItem
from datagrunt.core.pdf_io.extraction.stream_check import has_simple_filter, stream_size_problem
from datagrunt.core.pdf_io.filenames import embedded_image_filename

if TYPE_CHECKING:
    from datagrunt.core.pdf_io.extraction.render_guard import Jbig2Agreement

# Given an image's raw stream, filters and pixel size, what is wrong with its
# stream for its declared size (None when nothing is); see ``stream_check``.
StreamProblem = Callable[[bytes, list[str], "tuple[int, int] | None"], "str | None"]

logger = logging.getLogger(__name__)

# PDF names are at most 127 bytes in practice; this bound, NUL included, only
# stops a length pdfium reports from sizing the buffer.
_MAX_FILTER_NAME_BYTES = 4096

# Image objects can be nested inside form XObjects; walk deep enough to reach a
# bomb hidden behind a few layers of forms before a render would decode it.
_IMAGE_WALK_DEPTH = 200

PDF_EXTRA_HINT = "PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]"

# Substring present in pdfium's load error when a PDF needs a password.
_PDFIUM_PASSWORD_ERROR = "password"

ENCRYPTED_PDF_MESSAGE = "The PDF is encrypted or password-protected and cannot be opened without the correct password."


def _import_pdfium():
    """Import pypdfium2 lazily with a helpful error if the extra is missing."""
    try:
        import pypdfium2 as pdfium
        import pypdfium2.raw as raw
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pdfium, raw


def pdfium_reads_one_sound_revision(pdf_handle, raw_module) -> bool:
    """True when pdfium read the document's cross-reference table as valid and finds one trailer end.

    One trailer end is one revision: an incremental update adds one, whether
    its cross-reference is a table or a stream (observed: 1 and 2 for
    either). A table pdfium had to rebuild reads as not valid.
    """
    if not raw_module.FPDF_DocumentHasValidCrossReferenceTable(pdf_handle):
        return False
    return raw_module.FPDF_GetTrailerEnds(pdf_handle, None, 0) == 1


def pdfium_reads_one_sound_revision_for_path(filepath) -> bool:
    """:func:`pdfium_reads_one_sound_revision` of the file at ``filepath``; False when pdfium cannot open it."""
    try:
        pdfium, raw = _import_pdfium()
        pdf = pdfium.PdfDocument(str(filepath))
    except Exception:  # noqa: BLE001 - unreadable here -> not one sound revision
        return False
    try:
        return pdfium_reads_one_sound_revision(pdf.raw, raw)
    finally:
        pdf.close()


class PdfiumPage:
    """A single page; exposes text/image/render primitives in datagrunt shapes."""

    def __init__(
        self,
        page,
        raw_module,
        jbig2_decodable: Callable[[str, frozenset[str]], bool] | None = None,
        stream_problem: StreamProblem | None = None,
    ):
        """Wrap a pypdfium2 page handle.

        Args:
            page: A pypdfium2 page object.
            raw_module: The ``pypdfium2.raw`` module.
            jbig2_decodable: Given a JBIG2 image's raw-stream SHA-256 (hex) and
                those of every JBIG2 image pdfium finds on this page, whether
                PyMuPDF agrees it may be decoded (pdfium's API exposes no
                image's decode parameters; see ``render_guard.jbig2_agreement``).
                None: no JBIG2 image is decoded.
            stream_problem: Checks an image's stream against its declared size
                (see ``stream_check``), as the document caches it. None: each
                image is checked here, uncached.
        """
        self._page = page
        self._raw = raw_module
        self._jbig2_decodable = jbig2_decodable
        self._stream_problem = stream_problem
        self._jbig2_page_streams: frozenset[str] | None = None
        # If textpage acquisition fails (corrupt page), close the page handle
        # before re-raising — otherwise the constructor aborts before any
        # PdfiumPage is handed to the `with` block, so close()/__exit__ never
        # run and the page handle leaks for the document's lifetime.
        try:
            self._textpage = page.get_textpage()
        except Exception:
            page.close()
            raise

    def __enter__(self) -> "PdfiumPage":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        """Release the page and textpage handles (textpage first, then page).

        pypdfium2 leaves these handles open until the cyclic GC runs; closing
        them explicitly reclaims them deterministically. Safe to call more than
        once: ``close()`` on an already-closed handle is a no-op in pypdfium2.
        """
        self._textpage.close()
        self._page.close()

    def size(self) -> tuple:
        """Return the visible box's rotation-aware ``(width, height)`` in points."""
        return self._page.get_size()

    def _visible_box(self) -> tuple:
        """Return the visible box ``(left, bottom, right, top)`` in PDF user space.

        This is pdfium's page bounding box: the CropBox intersected with the
        MediaBox, both inherited from the page tree. It is the region the page
        renders and the frame ``size()`` measures. pypdfium2's
        ``get_mediabox()`` / ``get_cropbox()`` read only the page's own
        dictionary, so they miss inherited boxes.
        """
        return self._page.get_bbox()

    def rotation(self) -> int:
        """Return the page rotation in degrees."""
        return self._page.get_rotation()

    def full_text(self) -> str:
        """Return the page's full text."""
        return self._textpage.get_text_bounded()

    def _object_text(self, obj) -> str:
        """Return a text object's OWN text via FPDFTextObj_GetText (no overlap)."""
        n = self._raw.FPDFTextObj_GetText(obj.raw, self._textpage.raw, None, 0)
        if n <= 0:
            return ""
        buf = ctypes.create_string_buffer(n * 2)
        self._raw.FPDFTextObj_GetText(obj.raw, self._textpage.raw, ctypes.cast(buf, ctypes.POINTER(ctypes.c_ushort)), n)
        return buf.raw[: n * 2].decode("utf-16-le").rstrip("\x00").strip()

    @staticmethod
    def _display_box(left, bottom, right, top, rotation, visible_box) -> BBox:
        """Display-space ``BBox`` (rounded) for an image; see ``display_edges``."""
        x0, y_top, x1, y_bot = display_edges(left, bottom, right, top, rotation, visible_box)
        return BBox(x=round(x0, 2), y=round(y_top, 2), w=round(x1 - x0, 2), h=round(y_bot - y_top, 2))

    def text_items(self):
        """Yield a ``TextItem`` per text object (own text, matrix-scaled size)."""
        from math import hypot

        visible_box = self._visible_box()
        rotation = self.rotation()
        for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_TEXT,), max_depth=15):
            text = self._object_text(obj)
            if not text:
                continue
            # pdfium reports bottom-left bounds in UNROTATED PDF user space.
            left, bottom, right, top = obj.get_bounds()
            x0, y_top, x1, y_bot = display_edges(left, bottom, right, top, rotation, visible_box)
            font_name, weight = "", 400
            try:
                font = obj.get_font()
                font_name = font.get_family_name() or ""
                weight = font.get_weight() or 400
            except Exception:  # noqa: BLE001 - font metadata is best-effort
                pass
            try:
                m = obj.get_matrix()
                scale = hypot(m.b, m.d) or 1.0
            except Exception:  # noqa: BLE001
                scale = 1.0
            lower = font_name.lower()
            yield TextItem(
                text=text,
                x0=round(x0, 2),
                x1=round(x1, 2),
                y_top=round(y_top, 2),
                y_bot=round(y_bot, 2),
                size=round(obj.get_font_size() * scale, 1),
                font=font_name,
                is_bold=weight >= 600,
                is_italic=("italic" in lower or "oblique" in lower),
            )

    def has_paths(self) -> bool:
        """Return True if the page has any vector path objects (lines/rules)."""
        for _ in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_PATH,), max_depth=15):
            return True
        return False

    def count_objects(self) -> tuple:
        """Return ``(text_object_count, image_object_count)``."""
        texts = images = 0
        for obj in self._page.get_objects(
            filter=(self._raw.FPDF_PAGEOBJ_TEXT, self._raw.FPDF_PAGEOBJ_IMAGE), max_depth=15
        ):
            if obj.type == self._raw.FPDF_PAGEOBJ_TEXT:
                texts += 1
            else:
                images += 1
        return texts, images

    def _image_filters(self, obj) -> list[str] | None:
        """Return an image object's filter names (decoding order); None when pdfium cannot read them.

        The count, each name's size and each copy are checked; a name that is
        not UTF-8, or more than ``_MAX_FILTER_ARRAY_ITEMS`` filters, is unreadable too.
        """
        count = self._raw.FPDFImageObj_GetImageFilterCount(obj.raw)
        if not 0 <= count <= _MAX_FILTER_ARRAY_ITEMS:
            return None
        names = []
        for index in range(count):
            name = self._image_filter(obj, index)
            if name is None:
                return None
            names.append(name)
        return names

    def _image_filter(self, obj, index: int) -> str | None:
        """Return filter ``index`` of an image object, or None when pdfium cannot read it."""
        size = self._raw.FPDFImageObj_GetImageFilter(obj.raw, index, None, 0)
        if not 1 <= size <= _MAX_FILTER_NAME_BYTES:
            return None
        buffer = ctypes.create_string_buffer(size)
        if self._raw.FPDFImageObj_GetImageFilter(obj.raw, index, buffer, size) != size:
            return None
        try:
            return buffer.raw[: size - 1].decode("utf-8")
        except UnicodeDecodeError:
            return None

    def _raw_image_stream(self, obj) -> bytes | None:
        """Return an image object's raw (still-encoded) stream, or None if unreadable/too large.

        The raw stream is COPIED, not decoded; the size is checked first so a
        hostile ``/Length`` cannot force a copy beyond the header-read bound.
        """
        size = self._raw.FPDFImageObj_GetImageDataRaw(obj.raw, None, 0)
        if size <= 0 or size > _MAX_RAW_HEADER_BYTES:
            return None
        import ctypes as _ctypes

        buffer = _ctypes.create_string_buffer(size)
        if self._raw.FPDFImageObj_GetImageDataRaw(obj.raw, buffer, size) != size:
            return None
        return buffer.raw

    def _guard(self, obj, page_number: int, idx: int):
        """Return the guard decision for an image object (raw stream + filters, no decode).

        A JBIG2 image's decode parameters are checked by its raw stream: it is
        decoded only when the document draws that stream with no ``/JBIG2Globals``.
        An image with a decoding filter (FlateDecode, LZWDecode, ...) is decoded
        only when its stream decodes to no more than its declared size.
        """
        label = f"page {page_number + 1} image {idx}"
        filters = self._image_filters(obj)
        if filters is None:
            return GuardDecision(True, f"{label}: image filters could not be read; skipped (not decoded)")
        raw = self._raw_image_stream(obj)
        is_jbig2 = bool(filters) and normalize_filter(filters[-1]) == "JBIG2Decode"
        unchecked = is_jbig2 and not self._jbig2_agreed(raw)
        decision = inspect_image(raw, filters, label=label, jbig2_globals_missing=unchecked)
        if decision.skip or not has_simple_filter(filters):
            return decision
        problem = self._stream_size_problem(obj, raw, filters)
        return decision if problem is None else GuardDecision(True, f"{label}: {problem}")

    def _stream_size_problem(self, obj, raw: bytes | None, filters: list[str]) -> str | None:
        """What is wrong with an image's stream for its declared size; None when nothing is (see ``_guard``)."""
        if raw is None or self._stream_problem is None:
            return stream_size_problem(raw, filters, self._pixel_size(obj))
        return self._stream_problem(raw, filters, self._pixel_size(obj))

    def _pixel_size(self, obj) -> tuple[int, int] | None:
        """The image's ``/Width`` x ``/Height`` as pdfium reads them (no decode); None when pdfium cannot."""
        width, height = ctypes.c_uint(), ctypes.c_uint()
        if not self._raw.FPDFImageObj_GetImagePixelSize(obj.raw, ctypes.byref(width), ctypes.byref(height)):
            return None
        return width.value, height.value

    def _jbig2_agreed(self, raw: bytes | None) -> bool:
        """True when PyMuPDF agrees the JBIG2 image whose raw stream is ``raw`` may be decoded."""
        if raw is None or self._jbig2_decodable is None:
            return False
        on_page = self._jbig2_streams_on_page()
        return on_page is not None and self._jbig2_decodable(hashlib.sha256(raw).hexdigest(), on_page)

    def _jbig2_streams_on_page(self) -> frozenset[str] | None:
        """The raw-stream SHA-256 of every JBIG2 image pdfium finds on the page (forms included); None if unreadable."""
        if self._jbig2_page_streams is None:
            digests = set()
            for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_IMAGE,), max_depth=_IMAGE_WALK_DEPTH):
                filters = self._image_filters(obj)
                if filters is None:
                    return None
                if filters and normalize_filter(filters[-1]) == "JBIG2Decode":
                    raw = self._raw_image_stream(obj)
                    if raw is None:
                        return None
                    digests.add(hashlib.sha256(raw).hexdigest())
            self._jbig2_page_streams = frozenset(digests)
        return self._jbig2_page_streams

    def guarded_image_reasons(self, page_number: int = 0) -> list:
        """Warning reasons for every image on the page a decode bound would skip.

        Walks the page's image objects (forms included) and runs the guard on
        each WITHOUT decoding, so a render can be skipped when any image would
        make a decoder allocate at its codestream size rather than its declared
        size.
        """
        reasons = []
        for i, obj in enumerate(
            self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_IMAGE,), max_depth=_IMAGE_WALK_DEPTH)
        ):
            decision = self._guard(obj, page_number, i)
            if decision.skip:
                reasons.append(decision.reason)
        return reasons

    def image_items(
        self,
        output_dir: str | None = None,
        name_prefix: str = "page",
        page_number: int = 0,
        min_image_dimension: int = _DEFAULT_MIN_IMAGE_DIMENSION,
        warnings: list | None = None,
    ):
        """Yield an ``ImageBlock`` per embedded image whose pixel sides are both
        >= ``min_image_dimension``.

        An image a decode bound would skip (a JPEG 2000 / JBIG2 whose codestream
        describes far more than its declared size, a stream that decodes past
        its declared size, or one behind an unrecognised filter) is NOT
        touched: no ``get_px_size``, extract or bitmap call is made for it. Its
        reason is appended to ``warnings`` (when given) so the page records it,
        and it is never emitted as an image.
        """
        visible_box = self._visible_box()
        rotation = self.rotation()
        idx = 0
        for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_IMAGE,), max_depth=15):
            decision = self._guard(obj, page_number, idx)
            if decision.skip:
                if warnings is not None:
                    warnings.append(decision.reason)
                continue
            px_w, px_h = obj.get_px_size()
            if px_w < min_image_dimension or px_h < min_image_dimension:
                continue
            left, bottom, right, top = obj.get_bounds()
            bbox = self._display_box(left, bottom, right, top, rotation, visible_box)
            file_path, fmt = None, "png"
            if output_dir:
                image_stem = embedded_image_filename(name_prefix, page_number, idx)
                written = self._extract_image(obj, Path(output_dir) / image_stem)
                if written is not None:
                    file_path, fmt = str(written), (written.suffix.lstrip(".") or "png")
            yield ImageBlock(bbox=bbox, file_path=file_path, width_px=px_w, height_px=px_h, fmt=fmt)
            idx += 1

    def _extract_image(self, obj, base: Path):
        """Write an image object to disk preserving format; return path or None."""
        import glob

        from PIL import Image

        pdfium, _ = _import_pdfium()
        base.parent.mkdir(parents=True, exist_ok=True)

        extracted = False
        try:
            obj.extract(str(base))
            extracted = True
        except pdfium.PdfiumError:
            try:
                obj.extract(str(base), fb_render=True)
                extracted = True
            except pdfium.PdfiumError:
                pass

        if extracted:
            matches = glob.glob(str(base) + ".*")
            if matches:
                path = Path(matches[0])
                suffix = path.suffix.lower()
                if suffix in (".png", ".jpg", ".jpeg", ".gif"):
                    return path

                # Try to convert using PIL
                try:
                    img: Image.Image
                    with Image.open(path) as img:
                        if img.mode not in ("RGB", "RGBA", "L"):
                            img = img.convert("RGB")
                        png_path = path.with_suffix(".png")
                        img.save(png_path, "PNG")
                    path.unlink()
                    return png_path
                except Exception:  # noqa: BLE001 - PIL raises many types; fall back to bitmap
                    # PIL failed (e.g. missing format plugin), clean up and fall back to bitmap
                    logger.debug("PIL conversion of %s failed; falling back to bitmap", path, exc_info=True)
                    try:
                        path.unlink()
                    except OSError:
                        pass

        # Fallback to direct bitmap extraction if raw extraction or conversion failed
        try:
            bmp = obj.get_bitmap()
            if bmp is not None:
                img = bmp.to_pil()
                if img.mode not in ("RGB", "RGBA", "L"):
                    img = img.convert("RGB")
                png_path = base.with_suffix(".png")
                img.save(png_path, "PNG")
                return png_path
        except Exception:  # noqa: BLE001 - bitmap fallback is best-effort; return no image
            logger.debug("Bitmap image extraction failed for %s; no image emitted", base, exc_info=True)

        return None

    def render_pil(self, dpi: int = 300):
        """Render the page to a PIL RGB image at the given dpi."""
        return self._page.render(scale=dpi / 72.0).to_pil().convert("RGB")


class PdfiumDocument:
    """Context manager over a pypdfium2 ``PdfDocument`` (opened once)."""

    def __init__(self, filepath):
        """Open the document.

        Args:
            filepath (str or Path): Path to the PDF file.
        """
        pdfium, raw = _import_pdfium()
        self._raw = raw
        self._filepath = filepath
        self._jbig2_agreement: Jbig2Agreement | None = None
        self._stream_problems: dict[tuple[str, tuple[str, ...], tuple[int, int] | None], str | None] = {}
        try:
            self._pdf = pdfium.PdfDocument(str(filepath))
        except pdfium.PdfiumError as exc:
            # Translate pdfium's raw "Incorrect password error" into a clear,
            # catchable datagrunt error. Re-raise any other load failure as-is
            # so unrelated problems are not masked.
            if _PDFIUM_PASSWORD_ERROR in str(exc).lower():
                raise ValueError(ENCRYPTED_PDF_MESSAGE) from exc
            raise

    def __enter__(self) -> "PdfiumDocument":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __len__(self) -> int:
        return len(self._pdf)

    def page(self, page_number: int) -> PdfiumPage:
        """Return the wrapped page at ``page_number`` (0-indexed)."""
        return PdfiumPage(
            self._pdf[page_number], self._raw, partial(self._jbig2_decodable, page_number), self._stream_size_problem
        )

    def _stream_size_problem(self, raw: bytes, filters: list[str], size: tuple[int, int] | None) -> str | None:
        """What is wrong with an image's stream for its declared size, checked once a stream; None if nothing."""
        key = (hashlib.sha256(raw).hexdigest(), tuple(filters), size)
        if key not in self._stream_problems:
            self._stream_problems[key] = stream_size_problem(raw, filters, size)
        return self._stream_problems[key]

    def _jbig2_decodable(self, page_number: int, digest: str, on_page: frozenset[str]) -> bool:
        """True when pdfium reads one sound revision and PyMuPDF agrees: ``digest`` may be decoded and page
        ``page_number`` holds exactly ``on_page``."""
        if not pdfium_reads_one_sound_revision(self._pdf.raw, self._raw):
            return False
        agreement = self._jbig2_agreement_with_pymupdf()
        if digest not in agreement.streams or page_number >= len(agreement.page_streams):
            return False
        return agreement.page_streams[page_number] == on_page

    def _jbig2_agreement_with_pymupdf(self) -> "Jbig2Agreement":
        """The document's :class:`~render_guard.Jbig2Agreement`, read with PyMuPDF on first use."""
        if self._jbig2_agreement is None:
            from datagrunt.core.pdf_io.extraction.render_guard import jbig2_agreement_for_path

            self._jbig2_agreement = jbig2_agreement_for_path(self._filepath)
        return self._jbig2_agreement

    def close(self) -> None:
        """Close the underlying document."""
        self._pdf.close()
