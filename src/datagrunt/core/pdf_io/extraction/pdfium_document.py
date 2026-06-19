"""Object wrappers over pypdfium2 for safe, single-open page access.

Encapsulates ALL raw pdfium + ctypes usage so backends never touch raw handles.
``import datagrunt`` must work without the ``[pdf]`` extra, so pypdfium2 is
imported lazily.
"""

import ctypes
import logging
from pathlib import Path

from datagrunt.core.pdf_io.extraction.shapes import BBox, ImageBlock, TextItem

logger = logging.getLogger(__name__)

PDF_EXTRA_HINT = "PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]"

# Substring present in pdfium's load error when a PDF needs a password.
_PDFIUM_PASSWORD_ERROR = "password"

ENCRYPTED_PDF_MESSAGE = (
    "The PDF is encrypted or password-protected and cannot be opened without "
    "the correct password."
)

# Minimum image dimension (px) to keep; smaller images are layout artifacts.
MIN_IMAGE_DIMENSION = 40


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
        """Return ``(width, height)`` in points."""
        return self._page.get_size()

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
    def _display_edges(left, bottom, right, top, rotation, disp_w, disp_h):
        """Map pdfium's unrotated bounds to display-space ``(x0, y_top, x1, y_bot)``.

        ``obj.get_bounds()`` always reports bounds in UNROTATED page space, while
        ``get_size()`` reports ROTATION-AWARE width/height. Flipping y with the
        rotation-aware height alone yields negative / out-of-range coordinates on
        /Rotate 90|270 pages, so apply the page rotation to land every edge within
        ``[0, disp_w] x [0, disp_h]`` (top-left origin). Rotation 0 reduces to the
        original ``x0=left, x1=right, y_top=disp_h-top, y_bot=disp_h-bottom``,
        leaving unrotated pages byte-for-byte unchanged. Returns unrounded floats
        so callers round exactly once.
        """
        r = rotation % 360
        if r == 90:
            return bottom, left, top, right
        if r == 180:
            return disp_w - right, bottom, disp_w - left, top
        if r == 270:
            return disp_w - top, disp_h - right, disp_w - bottom, disp_h - left
        # rotation 0 (and any unexpected value) -- original behavior
        return left, disp_h - top, right, disp_h - bottom

    @classmethod
    def _display_box(cls, left, bottom, right, top, rotation, disp_w, disp_h) -> BBox:
        """Display-space ``BBox`` (rounded) for an image; see ``_display_edges``."""
        x0, y_top, x1, y_bot = cls._display_edges(left, bottom, right, top, rotation, disp_w, disp_h)
        return BBox(x=round(x0, 2), y=round(y_top, 2), w=round(x1 - x0, 2), h=round(y_bot - y_top, 2))

    def text_items(self):
        """Yield a ``TextItem`` per text object (own text, matrix-scaled size)."""
        from math import hypot

        disp_w, disp_h = self.size()
        rotation = self.rotation()
        for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_TEXT,), max_depth=15):
            text = self._object_text(obj)
            if not text:
                continue
            left, bottom, right, top = obj.get_bounds()
            x0, y_top, x1, y_bot = self._display_edges(left, bottom, right, top, rotation, disp_w, disp_h)
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

    def image_items(self, output_dir: str = None, name_prefix: str = "page", page_number: int = 0):
        """Yield an ``ImageBlock`` per embedded image >= MIN_IMAGE_DIMENSION."""
        disp_w, disp_h = self.size()
        rotation = self.rotation()
        idx = 0
        for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_IMAGE,), max_depth=15):
            px_w, px_h = obj.get_px_size()
            if px_w < MIN_IMAGE_DIMENSION or px_h < MIN_IMAGE_DIMENSION:
                continue
            left, bottom, right, top = obj.get_bounds()
            bbox = self._display_box(left, bottom, right, top, rotation, disp_w, disp_h)
            file_path, fmt = None, "png"
            if output_dir:
                written = self._extract_image(obj, Path(output_dir) / f"{name_prefix}_page{page_number}_img{idx}")
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
