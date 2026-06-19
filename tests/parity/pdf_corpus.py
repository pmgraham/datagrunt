"""Seeded PDF builders for cross-backend parity tests.

Each builder writes one PDF under a shared corpus directory. Builders mirror
real-world shapes (text, images, multipage markers) without relying on the
git-ignored ``data/pdfs/`` corpus used by ``test_engine_comparison.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pymupdf

Builder = Callable[[Path], None]


def build_text_and_image(dest: Path) -> None:
    """One page: header, body lines, and an embedded image."""
    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 72), "Quarterly Report", fontsize=24)
    page.insert_text((72, 120), "This is body text for testing.", fontsize=11)
    page.insert_text((72, 140), "Body line two for the report.", fontsize=11)
    page.insert_text((72, 160), "Body line three with details.", fontsize=11)
    page.insert_text((72, 180), "Body line four wraps up the text.", fontsize=11)

    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
    pix.set_rect(pix.irect, (255, 0, 0))
    page.insert_image(pymupdf.Rect(72, 200, 172, 300), stream=pix.tobytes("png"))

    doc.save(str(dest))
    doc.close()


def build_multipage_markers(dest: Path) -> None:
    """Three pages with distinct marker strings."""
    doc = pymupdf.open()
    for n in range(1, 4):
        page = doc.new_page(width=612, height=792)
        page.insert_text((72, 72), f"Page Marker {n}", fontsize=18)
        page.insert_text((72, 110), f"Body content for page {n}.", fontsize=11)
    doc.save(str(dest))
    doc.close()


def build_body_text_only(dest: Path) -> None:
    """One page with only body text (no images)."""
    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 72), "Simple paragraph for parity.", fontsize=12)
    page.insert_text((72, 96), "Second line of plain text.", fontsize=12)
    doc.save(str(dest))
    doc.close()


BUILDERS: dict[str, Builder] = {
    "text_and_image.pdf": build_text_and_image,
    "multipage_markers.pdf": build_multipage_markers,
    "body_text_only.pdf": build_body_text_only,
}


def write_corpus(root: Path) -> dict[str, Path]:
    """Materialize every seeded PDF under ``root`` and return name -> path."""
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name, builder in BUILDERS.items():
        path = root / name
        builder(path)
        paths[name] = path
    return paths
