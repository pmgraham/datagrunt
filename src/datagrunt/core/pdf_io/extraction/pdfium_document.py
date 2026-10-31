"""Object wrappers over pypdfium2 for safe, single-open page access.

Encapsulates ALL raw pdfium + ctypes usage so backends never touch raw handles.
``import datagrunt`` must work without the ``[pdf]`` extra, so pypdfium2 is
imported lazily.
"""

import ctypes
import logging
from pathlib import Path

from datagrunt.core.pdf_io.extraction.config import _DEFAULT_MIN_IMAGE_DIMENSION
from datagrunt.core.pdf_io.extraction.display_frame import display_edges
from datagrunt.core.pdf_io.extraction.image_guard import _MAX_RAW_HEADER_BYTES, inspect_image
from datagrunt.core.pdf_io.extraction.shapes import BBox, ImageBlock, TextItem
from datagrunt.core.pdf_io.filenames import embedded_image_filename

logger = logging.getLogger(__name__)

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


class PdfiumPage:
    """A single page; exposes text/image/render primitives in datagrunt shapes."""

    def __init__(self, page, raw_module):
        """Wrap a pypdfium2 page handle.

        Args:
            page: A pypdfium2 page object.
            raw_module: The ``pypdfium2.raw`` module.
        """
        self._page = page
        self._raw = raw_module
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

    def _image_filters(self, obj) -> list:
        """Return an image object's filter names (decoding order); ``[]`` on failure."""
        try:
            return list(obj.get_filters())
        except Exception:  # noqa: BLE001 - a missing/odd filter list must not fail the page
            return []

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
        """Return the guard decision for an image object (raw stream + filters, no decode)."""
        filters = self._image_filters(obj)
        raw = self._raw_image_stream(obj)
        return inspect_image(raw, filters, label=f"page {page_number + 1} image {idx}")

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
        describes far more than its declared size, or one behind an unrecognised
        filter) is NOT touched: no ``get_px_size``, extract or bitmap call is
        made for it. Its reason is appended to ``warnings`` (when given) so the
        page records it, and it is never emitted as an image.
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
        return PdfiumPage(self._pdf[page_number], self._raw)

    def close(self) -> None:
        """Close the underlying document."""
        self._pdf.close()
