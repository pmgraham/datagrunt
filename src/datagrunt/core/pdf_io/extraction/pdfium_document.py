"""Object wrappers over pypdfium2 for safe, single-open page access.

Encapsulates ALL raw pdfium + ctypes usage so backends never touch raw handles.
``import datagrunt`` must work without the ``[pdf]`` extra, so pypdfium2 is
imported lazily.
"""

import ctypes
from pathlib import Path

from datagrunt.core.pdf_io.extraction.shapes import BBox, ImageBlock, TextItem

PDF_EXTRA_HINT = "PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]"

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
        self._textpage = page.get_textpage()

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

    def text_items(self):
        """Yield a ``TextItem`` per text object (own text, matrix-scaled size)."""
        from math import hypot

        _, height = self.size()
        for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_TEXT,), max_depth=15):
            text = self._object_text(obj)
            if not text:
                continue
            left, bottom, right, top = obj.get_bounds()
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
                x0=round(left, 2),
                x1=round(right, 2),
                y_top=round(height - top, 2),
                y_bot=round(height - bottom, 2),
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
        _, height = self.size()
        idx = 0
        for obj in self._page.get_objects(filter=(self._raw.FPDF_PAGEOBJ_IMAGE,), max_depth=15):
            px_w, px_h = obj.get_px_size()
            if px_w < MIN_IMAGE_DIMENSION or px_h < MIN_IMAGE_DIMENSION:
                continue
            left, bottom, right, top = obj.get_bounds()
            bbox = BBox.from_pdfium_bounds(left, bottom, right, top, height)
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
                except Exception:
                    # PIL failed (e.g. missing format plugin), clean up and fall back to bitmap
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
        except Exception:
            pass

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
        self._pdf = pdfium.PdfDocument(str(filepath))

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
