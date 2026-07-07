"""Output-filename builders for the PDF domain.

Single source of truth for the numbering convention: every number embedded in
an output filename is 1-based and zero-padded to two digits so directory
listings sort in page order (``page_02`` < ``page_10``).
"""


def _ordinal(index: int) -> str:
    """Return the 1-based, zero-padded filename label for a 0-based index."""
    return f"{index + 1:02d}"


def page_image_filename(name_prefix: str, page_index: int, ext: str) -> str:
    """Return the filename for a full-page render, e.g. ``report_page_01.png``."""
    return f"{name_prefix}_page_{_ordinal(page_index)}.{ext}"


def embedded_image_filename(name_prefix: str, page_index: int, image_index: int, ext: str | None = None) -> str:
    """Return the filename for an extracted embedded image, e.g. ``report_page01_img01.png``.

    With ``ext=None`` the bare stem is returned, for writers that append the
    extension themselves (pdfium's image extractor).
    """
    stem = f"{name_prefix}_page{_ordinal(page_index)}_img{_ordinal(image_index)}"
    return f"{stem}.{ext}" if ext else stem
