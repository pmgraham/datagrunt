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

from datagrunt.core.pdf_io.extraction.image_guard import (
    _ARRAY_OUTER_RE,
    _MAX_FILTER_ARRAY_ITEMS,
    _MAX_RAW_HEADER_BYTES,
    _NON_IMAGE_FILTERS,
    _SINGLE_NAME_RE,
    _UNREADABLE_FILTER,
    extract_xref_filters,
    inspect_image,
    normalize_filter,
    parse_filter_names,
    resolve_indirect_text,
    unescape_pdf_names,
)

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

_FILTER_KEY_RE = re.compile(r"/(?:Filter|F)\b")
_FILTER_VALUE = re.compile(r"/(?:Filter|F)\b\s*(\[[^\]]*\]|/[^\s/\[\]<>(){}]+)")
_BI_TOKEN = re.compile(rb"(?<![^\s\]>)}])BI(?=[\s/])")
_ID_TOKEN = re.compile(rb"(?<![^\s\]>)}])ID[\s]")
_EI_TOKEN = re.compile(rb"[\s]EI(?=$|[\s/>])")
_JBIG2_GLOBALS_KEY_RE = re.compile(r"/JBIG2Globals\b")
_JBIG2_GLOBALS_NULL_RE = re.compile(r"/JBIG2Globals\s+null\b")
_JBIG2_GLOBALS_REF_RE = re.compile(r"/JBIG2Globals\s+(\d+)\s+\d+\s+R")
_DICT_OUTER_RE = re.compile(r"^\s*<<.*>>\s*$", re.DOTALL)
_DECODE_PARMS_ARRAY_TOKEN_RE = re.compile(r"<<.*?>>|(\d+)\s+\d+\s+R|null\b", re.DOTALL)


def _parse_filter_value(value) -> list:
    """Parse a PDF ``/Filter`` entry into filter names (abbreviations kept; guard normalises)."""
    return parse_filter_names(value)


def _jbig2_globals_from_dict(doc, dict_text: str) -> tuple[bytes | None, bool]:
    """Read ``/JBIG2Globals`` from a single ``/DecodeParms`` dictionary text."""
    text = unescape_pdf_names(dict_text)
    if _JBIG2_GLOBALS_KEY_RE.search(text) is None:
        return None, False
    if _JBIG2_GLOBALS_NULL_RE.search(text) is not None:
        return None, False
    match = _JBIG2_GLOBALS_REF_RE.search(text)
    if match is None:
        return None, True
    try:
        globals_xref = int(match.group(1))
        raw = doc.xref_stream_raw(globals_xref)
    except Exception:  # noqa: BLE001 - referenced but unreadable -> fail closed
        return None, True
    if raw is None or len(raw) > _MAX_RAW_HEADER_BYTES:
        return None, True
    return raw, False


def _jbig2_globals_from_array(doc, array_text: str) -> tuple[bytes | None, bool]:
    """Read ``/JBIG2Globals`` from a ``/DecodeParms [...]`` array text."""
    outer = _ARRAY_OUTER_RE.match(array_text)
    if outer is None:
        return None, True
    interior = outer.group(1)
    pos = 0
    found_raw: bytes | None = None
    for _ in range(_MAX_FILTER_ARRAY_ITEMS):
        while pos < len(interior) and interior[pos].isspace():
            pos += 1
        if pos >= len(interior):
            break
        match = _DECODE_PARMS_ARRAY_TOKEN_RE.match(interior, pos)
        if match is None:
            return None, True
        pos = match.end()
        token = match.group(0)
        ref_num = match.group(1)
        if token == "null":
            continue
        if ref_num is not None:
            resolved = resolve_indirect_text(doc, f"{ref_num} 0 R")
            if resolved is None:
                return None, True
            if resolved == "null":
                continue
            if _DICT_OUTER_RE.match(resolved) is None:
                return None, True
            token = resolved
        raw, missing = _jbig2_globals_from_dict(doc, token)
        if missing:
            return None, True
        if raw is not None:
            found_raw = raw
    else:
        return None, True
    return found_raw, False


def _jbig2_globals(doc, xref: int) -> tuple[bytes | None, bool]:
    """Return ``(/JBIG2Globals stream bytes, missing)`` for image ``xref``.

    ``missing`` is True when a globals stream is referenced (or ``/DecodeParms``
    is malformed/unresolvable) and cannot be read, so the guard fails closed;
    ``(None, False)`` means no globals are referenced.
    """
    try:
        kind, value = doc.xref_get_key(xref, "DecodeParms")
        if kind == "null":
            kind, value = doc.xref_get_key(xref, "DP")
    except Exception:  # noqa: BLE001
        return None, True
    if kind == "null":
        return None, False
    text = str(value)
    if kind == "xref":
        resolved = resolve_indirect_text(doc, text)
        if resolved is None:
            return None, True
        if resolved == "null":
            return None, False
        if _DICT_OUTER_RE.match(resolved) is not None:
            return _jbig2_globals_from_dict(doc, resolved)
        if _ARRAY_OUTER_RE.match(resolved) is not None:
            return _jbig2_globals_from_array(doc, resolved)
        return None, True
    if kind == "dict":
        return _jbig2_globals_from_dict(doc, text)
    if kind == "array":
        return _jbig2_globals_from_array(doc, text)
    return None, True


def _image_block_reason(doc, xref: int) -> tuple[str | None, int]:
    """Guard decision for the image at ``xref`` and the raw bytes it read (for the budget)."""
    filters = extract_xref_filters(doc, xref)
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


def _inline_dict_filters(raw_dict: str) -> list[str]:
    """Extract inline image filter names from an unescaped dictionary text."""
    match = _FILTER_VALUE.search(raw_dict)
    if match is not None:
        names = _parse_filter_value(match.group(1))
        return names if names else [_UNREADABLE_FILTER]
    if _FILTER_KEY_RE.search(raw_dict) is not None:
        return [_UNREADABLE_FILTER]
    return []


def _inline_block_reason(content: bytes) -> str | None:
    """Scan one decoded content stream for an inline image a render must not decode.

    Only an inline image whose filter is an image codec the guard does not
    positively accept (JPEG 2000 / JBIG2 / unknown) is examined; its data
    (between ``ID`` and the next ``EI``) is run through the guard. A simple
    filter (or none) is bounded by ``/W`` ``/H`` and left alone. If the data
    cannot be delimited within the bound, the render is blocked (fail closed).
    """
    start = 0
    max_steps = len(content) // 2 + 1
    for _ in range(max_steps):
        if start >= len(content):
            return None
        bi_match = _BI_TOKEN.search(content, start)
        if bi_match is None:
            return None
        bi_end = bi_match.end()
        dict_window = content[bi_end : bi_end + _MAX_INLINE_DICT_BYTES]
        id_match = _ID_TOKEN.search(dict_window)
        if id_match is None:
            if _FILTER_KEY_RE.search(unescape_pdf_names(dict_window.decode("latin-1"))):
                return "inline image: stream truncated before ID; render skipped"
            start = bi_end
            continue
        raw_dict = unescape_pdf_names(dict_window[: id_match.start()].decode("latin-1"))
        filters = _inline_dict_filters(raw_dict)
        codec = normalize_filter(filters[-1]) if filters else ""
        if not filters or codec in _NON_IMAGE_FILTERS:
            start = bi_end + id_match.end()  # a safe or filter-less inline image: bounded by /W /H
            continue
        data_start = bi_end + id_match.end()  # one whitespace byte follows ID
        data_window = content[data_start : data_start + _MAX_INLINE_DATA_BYTES]
        ei_match = _EI_TOKEN.search(data_window)
        data = data_window[: ei_match.start()] if ei_match is not None else data_window
        decision = inspect_image(data if data else None, filters, label="an inline image")
        if decision.skip:
            return decision.reason
        if ei_match is None:
            return "an inline image could not be delimited within the scan bound; render skipped"
        start = data_start + ei_match.end()
    return None


def _resolve_subtype(doc, xref: int) -> tuple[str | None, bool]:
    """Return ``(unescaped_subtype_name, unreadable)`` for ``xref``."""
    try:
        kind, value = doc.xref_get_key(xref, "Subtype")
    except Exception:  # noqa: BLE001 - not a dictionary object
        return None, False
    if kind == "null":
        return None, False
    if kind == "name":
        unescaped = unescape_pdf_names(str(value).strip())
        return (unescaped, False) if _SINGLE_NAME_RE.match(unescaped) else (None, True)
    if kind == "xref":
        resolved = resolve_indirect_text(doc, str(value))
        if resolved is None:
            return None, True
        unescaped = unescape_pdf_names(resolved.strip())
        return (unescaped, False) if _SINGLE_NAME_RE.match(unescaped) else (None, True)
    return None, True


def _is_pattern_object(doc, xref: int) -> bool:
    """Return True if ``xref`` declares a ``/PatternType`` entry."""
    try:
        return doc.xref_get_key(xref, "PatternType") != ("null", "null")
    except Exception:  # noqa: BLE001
        return False


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
    content_xrefs: list[int] = []  # Form XObjects and tiling patterns: streams that may hold inline images
    for xref in range(1, count):
        subtype_name, subtype_unreadable = _resolve_subtype(doc, xref)
        if subtype_unreadable:
            return f"xref {xref}: unreadable /Subtype; render skipped"
        if subtype_name == "/Image":
            reason, read = _image_block_reason(doc, xref)
            if reason is not None:
                return reason
            budget -= read
            if budget < 0:
                return "image stream scan exceeded the per-document byte budget; render skipped"
        elif subtype_name == "/Form":
            content_xrefs.append(xref)  # annotation appearance /N streams are Form XObjects too
        elif subtype_name == "/Type3":
            content_xrefs.extend(_charproc_xrefs(doc, xref))  # glyph procedures may hold inline images
        elif _is_pattern_object(doc, xref):
            content_xrefs.append(xref)

    # Inline images live in content streams, not in xref /Subtype: scan every
    # page's content and every Form XObject / tiling-pattern stream.
    for source, error_reason in _content_streams(doc, content_xrefs):
        if error_reason is not None or source is None:
            return error_reason or "content stream unreadable; render skipped"
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
    """Yield ``(stream_bytes, error_reason)`` for every page and Form/pattern content stream."""
    for page_index, page in enumerate(doc):
        try:
            yield page.read_contents(), None
        except Exception:  # noqa: BLE001 - unreadable page content stream -> fail closed
            page_num = getattr(page, "number", page_index) + 1
            yield None, f"page {page_num}: content stream unreadable; render skipped"
    for xref in content_xrefs:
        try:
            stream = doc.xref_stream(xref)
        except Exception:  # noqa: BLE001 - unreadable form/pattern content stream -> fail closed
            yield None, f"xref {xref}: form content stream unreadable; render skipped"
            continue
        if stream:
            yield stream, None


def document_block_reason_for_path(filepath) -> str | None:
    """Open ``filepath`` with PyMuPDF and return its render-block reason (None if safe)."""
    from datagrunt.core.pdf_io.extraction.pymupdf_backend import _import_pymupdf

    pymupdf = _import_pymupdf()
    doc = pymupdf.open(filepath)
    try:
        return document_block_reason(doc)
    finally:
        doc.close()
