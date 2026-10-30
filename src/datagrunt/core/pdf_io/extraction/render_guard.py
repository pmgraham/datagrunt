"""Document-wide decision on whether a PDF may be rendered, via PyMuPDF.

A page render decodes every image the page draws, at the image's codestream
size for JPEG 2000 / JBIG2 (ignoring the declared ``/Width`` ``/Height``), and
reaches images in page resources at any depth, in Form XObjects, annotation
appearance streams, tiling patterns and Type3 glyph procedures, plus each
image's ``/SMask`` and ``/Mask``. Rather than walk every such container, this
scans the whole document once: every stream object whose ``/Subtype`` is
``/Image`` is read (raw bytes + ``/Filter``, never decoded) and run through the
image guard, and every content stream is scanned for inline images. If any
image anywhere in the document is guarded, no page of the document is rendered
(fail closed). The scan is bounded by a byte and object budget; exceeding it
also blocks rendering.

The result depends only on the file, so a caller scans once per document and
reuses the reason.
"""

import logging
import re

from datagrunt.core.pdf_io.extraction.image_guard import _NON_IMAGE_FILTERS, inspect_image, normalize_filter

logger = logging.getLogger(__name__)

# Total raw image/content bytes this scan copies out of one document. A copy is
# not a decode; this only stops a hostile document from forcing an unbounded
# read. Exceeding it blocks rendering (fail closed).
_MAX_SCAN_BYTES = 2 << 30  # 2 GiB
# Most objects (xref entries) a document may have for this scan to run; past it
# the document is refused rather than scanned.
_MAX_SCAN_OBJECTS = 500_000
# An inline image's dictionary (between ``BI`` and ``ID``) is tiny; its filter
# is read within this many bytes of the ``BI``.
_MAX_INLINE_DICT_BYTES = 4096
# The most content-stream bytes scanned past an inline image's ``ID`` for its
# ``EI``; a JPEG 2000 / JBIG2 header sits right after ``ID``, so this need not
# span a whole bomb.
_MAX_INLINE_DATA_BYTES = 1 << 20

_NAME_TOKEN = r"/([^\s/\[\]<>(){}]+)"
_FILTER_VALUE = re.compile(r"/(?:Filter|F)\b\s*(\[[^\]]*\]|/[^\s/\[\]<>(){}]+)")


def _parse_filter_value(value) -> list:
    """Parse a PDF ``/Filter`` entry into filter names (abbreviations kept; guard normalises)."""
    if not value:
        return []
    return re.findall(_NAME_TOKEN, str(value))


def _jbig2_globals(doc, xref: int) -> tuple[bytes | None, bool]:
    """Return ``(/JBIG2Globals stream bytes, missing)`` for image ``xref``.

    ``missing`` is True when a globals stream is referenced but cannot be read,
    so the guard fails closed; ``(None, False)`` means no globals are referenced.
    """
    try:
        kind, value = doc.xref_get_key(xref, "DecodeParms/JBIG2Globals")
    except Exception:  # noqa: BLE001 - absent or array-valued DecodeParms
        return None, False
    if kind != "xref":
        return None, False
    try:
        globals_xref = int(str(value).split()[0])
        raw = doc.xref_stream_raw(globals_xref)
    except Exception:  # noqa: BLE001 - referenced but unreadable -> fail closed
        return None, True
    if raw is None:
        return None, True
    return raw, False


def _image_block_reason(doc, xref: int) -> tuple[str | None, int]:
    """Guard decision for the image at ``xref`` and the raw bytes it read (for the budget)."""
    try:
        _, filter_value = doc.xref_get_key(xref, "Filter")
    except Exception:  # noqa: BLE001
        filter_value = None
    filters = _parse_filter_value(filter_value)
    globals_data, globals_missing = _jbig2_globals(doc, xref)
    try:
        raw = doc.xref_stream_raw(xref)
    except Exception:  # noqa: BLE001 - unreadable raw stream -> fail closed
        raw = None
    read = (len(raw) if raw else 0) + (len(globals_data) if globals_data else 0)
    decision = inspect_image(
        raw, filters, label=f"xref {xref}", jbig2_globals=globals_data, jbig2_globals_missing=globals_missing
    )
    return (decision.reason if decision.skip else None), read


def _inline_block_reason(content: bytes) -> str | None:
    """Scan one decoded content stream for an inline image a render must not decode.

    Only an inline image whose filter is an image codec the guard does not
    positively accept (JPEG 2000 / JBIG2 / unknown) is examined; its data
    (between ``ID`` and the next ``EI``) is run through the guard. A simple
    filter (or none) is bounded by ``/W`` ``/H`` and left alone. If the data
    cannot be delimited within the bound, the render is blocked (fail closed).
    """
    start = 0
    while True:
        bi = content.find(b"BI", start)
        if bi < 0:
            return None
        id_at = content.find(b"ID", bi, bi + _MAX_INLINE_DICT_BYTES)
        if id_at < 0:
            start = bi + 2
            continue
        dict_text = content[bi + 2 : id_at]
        match = _FILTER_VALUE.search(dict_text.decode("latin-1"))
        filters = _parse_filter_value(match.group(1)) if match else []
        codec = normalize_filter(filters[-1]) if filters else ""
        if not filters or codec in _NON_IMAGE_FILTERS:
            start = id_at + 2  # a safe or filter-less inline image: bounded by /W /H
            continue
        data_start = id_at + 3  # one whitespace byte follows ID
        ei = content.find(b"EI", data_start, data_start + _MAX_INLINE_DATA_BYTES)
        data = content[data_start:ei] if ei >= 0 else content[data_start : data_start + _MAX_INLINE_DATA_BYTES]
        decision = inspect_image(data if data else None, filters, label="an inline image")
        if decision.skip:
            return decision.reason
        if ei < 0:
            return "an inline image could not be delimited within the scan bound; render skipped"
        start = ei + 2


def document_block_reason(doc) -> str | None:
    """Return the reason a document must not be rendered, or None if every image is safe.

    Scans every ``/Subtype /Image`` stream object and every page/Form content
    stream for inline images, within the byte and object budget. The first
    guarded image returns its reason; exceeding the budget returns a
    fail-closed reason.
    """
    try:
        count = doc.xref_length()
    except Exception as exc:  # noqa: BLE001
        return f"document object table unreadable ({exc}); render skipped"
    if count > _MAX_SCAN_OBJECTS:
        return f"document has {count} objects, over the {_MAX_SCAN_OBJECTS}-object scan bound; render skipped"

    budget = _MAX_SCAN_BYTES
    content_xrefs = []  # Form XObjects and tiling patterns: streams that may hold inline images
    for xref in range(1, count):
        try:
            subtype = doc.xref_get_key(xref, "Subtype")
        except Exception:  # noqa: BLE001 - not a dictionary object
            continue
        if subtype == ("name", "/Image"):
            reason, read = _image_block_reason(doc, xref)
            if reason is not None:
                return reason
            budget -= read
            if budget < 0:
                return "image stream scan exceeded the per-document byte budget; render skipped"
        elif subtype == ("name", "/Form"):
            content_xrefs.append(xref)  # annotation appearance /N streams are Form XObjects too
        elif subtype == ("name", "/Type3"):
            content_xrefs.extend(_charproc_xrefs(doc, xref))  # glyph procedures may hold inline images
        else:
            try:
                is_pattern = doc.xref_get_key(xref, "PatternType") != ("null", "null")
            except Exception:  # noqa: BLE001
                is_pattern = False
            if is_pattern:
                content_xrefs.append(xref)

    # Inline images live in content streams, not in xref /Subtype: scan every
    # page's content and every Form XObject / tiling-pattern stream.
    for source in _content_streams(doc, content_xrefs):
        budget -= len(source)
        if budget < 0:
            return "content stream scan exceeded the per-document byte budget; render skipped"
        reason = _inline_block_reason(source)
        if reason is not None:
            return reason
    return None


def _charproc_xrefs(doc, font_xref: int) -> list:
    """The stream xrefs of a Type3 font's ``/CharProcs`` glyph procedures.

    ``/CharProcs`` is a dictionary of glyph name -> content stream; its content
    streams can draw inline images, so each is scanned. Handles both an inline
    dictionary value and a reference to a dictionary object.
    """
    try:
        kind, value = doc.xref_get_key(font_xref, "CharProcs")
    except Exception:  # noqa: BLE001
        return []
    if kind == "dict":
        return [int(ref) for ref in re.findall(r"(\d+)\s+\d+\s+R", str(value))]
    if kind == "xref":
        try:
            obj = int(str(value).split()[0])
            keys = doc.xref_get_keys(obj)
        except Exception:  # noqa: BLE001
            return []
        xrefs = []
        for key in keys:
            try:
                k, v = doc.xref_get_key(obj, key)
            except Exception:  # noqa: BLE001
                continue
            if k == "xref":
                xrefs.append(int(str(v).split()[0]))
        return xrefs
    return []


def _content_streams(doc, content_xrefs):
    """Yield the decoded bytes of every page content stream and Form/pattern stream."""
    for page in doc:
        try:
            yield page.read_contents()
        except Exception:  # noqa: BLE001 - a page whose content cannot be read has nothing to scan
            continue
    for xref in content_xrefs:
        try:
            stream = doc.xref_stream(xref)
        except Exception:  # noqa: BLE001
            stream = None
        if stream:
            yield stream


def document_block_reason_for_path(filepath) -> str | None:
    """Open ``filepath`` with PyMuPDF and return its render-block reason (None if safe)."""
    from datagrunt.core.pdf_io.extraction.pymupdf_backend import _import_pymupdf

    pymupdf = _import_pymupdf()
    doc = pymupdf.open(filepath)
    try:
        return document_block_reason(doc)
    finally:
        doc.close()
