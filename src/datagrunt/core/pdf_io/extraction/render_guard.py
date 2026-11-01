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

import hashlib
import logging
import re
from collections import Counter
from typing import NamedTuple

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
        # An inline image's /DP is not resolved here: any /JBIG2Globals in it is not checked.
        globals_unchecked = _JBIG2_GLOBALS_KEY_RE.search(raw_dict) is not None
        decision = inspect_image(
            data if data else None, filters, label="an inline image", jbig2_globals_missing=globals_unchecked
        )
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
    except Exception:  # noqa: BLE001 - a non-dictionary reads as null; a failure is unreadable
        return None, True
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


def _is_pattern_object(doc, xref: int) -> bool | None:
    """Return True if ``xref`` declares a ``/PatternType`` entry; None if it cannot be read."""
    try:
        return doc.xref_get_key(xref, "PatternType") != ("null", "null")
    except Exception:  # noqa: BLE001 - unreadable -> the caller blocks the render
        return None


def _object_content_xrefs(doc, xref: int, subtype_name: str | None) -> list[int] | str:
    """The content streams of non-image object ``xref`` that may hold inline images, or why they cannot be read."""
    if subtype_name == "/Form":
        return [xref]  # annotation appearance /N streams are Form XObjects too
    if subtype_name == "/Type3":
        glyphs = _charproc_xrefs(doc, xref)  # glyph procedures may hold inline images
        return glyphs if glyphs is not None else f"xref {xref}: unreadable /CharProcs; render skipped"
    is_pattern = _is_pattern_object(doc, xref)
    if is_pattern is None:
        return f"xref {xref}: unreadable /PatternType; render skipped"
    return [xref] if is_pattern else []


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
        else:
            found = _object_content_xrefs(doc, xref, subtype_name)
            if isinstance(found, str):
                return found
            content_xrefs.extend(found)

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


class Jbig2Agreement(NamedTuple):
    """What PyMuPDF reads of a document's JBIG2 images, for the pdfium engine to agree with.

    pdfium's API exposes no image's decode parameters, so the pdfium engine
    decodes a JBIG2 image only when its raw stream's SHA-256 is in
    ``streams`` and the JBIG2 streams it finds on the page are exactly that
    page's entry in ``page_streams`` (see :func:`jbig2_agreement`).
    """

    streams: frozenset[str]
    page_streams: tuple[frozenset[str], ...]


_NO_AGREEMENT = Jbig2Agreement(frozenset(), ())


class _StreamFact(NamedTuple):
    """One stream object as the agreement scan reads it: its raw stream's SHA-256 and the bytes read."""

    xref: int
    digest: str
    is_image: bool
    is_jbig2: bool
    has_globals: bool
    read: int


def jbig2_agreement(doc) -> Jbig2Agreement:
    """The JBIG2 image streams the pdfium engine may decode, and each page's; none on any doubt.

    None is decoded (``_NO_AGREEMENT``) unless: PyMuPDF opened the file
    without repairing it and finds a single revision (no incremental
    update); no content stream (pages, forms, patterns, Type3 glyphs) holds
    an inline JBIG2 image; and every JBIG2 ``/Image`` object's raw stream is
    the stream of exactly one ``/Image`` object, with no stream object of
    those bytes carrying ``/JBIG2Globals`` (or decode parameters that cannot
    be read). Any read failure, or the scan's object or byte bound, is doubt.
    """
    if not _single_unrepaired_revision(doc):
        return _NO_AGREEMENT
    scanned = _stream_facts(doc)
    if scanned is None:
        return _NO_AGREEMENT
    facts, content_xrefs, budget = scanned
    if _holds_inline_jbig2(doc, content_xrefs, budget):
        return _NO_AGREEMENT
    jbig2 = {fact.xref: fact.digest for fact in facts if fact.is_jbig2}
    images = Counter(fact.digest for fact in facts if fact.is_image)
    with_globals = {fact.digest for fact in facts if fact.has_globals}
    if any(images[digest] != 1 or digest in with_globals for digest in jbig2.values()):
        return _NO_AGREEMENT
    page_streams = _page_jbig2_streams(doc, jbig2)
    if page_streams is None:
        return _NO_AGREEMENT
    return Jbig2Agreement(frozenset(jbig2.values()), page_streams)


def _single_unrepaired_revision(doc) -> bool:
    """True when PyMuPDF opened the file without repair and finds one revision (False if it cannot tell)."""
    try:
        return doc.is_repaired is False and doc.version_count == 1
    except Exception:  # noqa: BLE001 - cannot tell -> treated as several revisions
        return False


def _stream_facts(doc) -> tuple[list[_StreamFact], list[int], int] | None:
    """Every stream object's facts, the content streams that may hold inline images, and the byte budget left.

    None when the object table or an object cannot be read, or the scan
    passes its object or byte bound.
    """
    try:
        count = doc.xref_length()
    except Exception:  # noqa: BLE001 - unreadable object table
        return None
    if count > _MAX_SCAN_OBJECTS:
        return None
    budget = _MAX_SCAN_BYTES
    facts: list[_StreamFact] = []
    content_xrefs: list[int] = []
    for xref in range(1, count):
        subtype_name, subtype_unreadable = _resolve_subtype(doc, xref)
        if subtype_unreadable:
            return None
        found = [] if subtype_name == "/Image" else _object_content_xrefs(doc, xref, subtype_name)
        if isinstance(found, str):
            return None
        fact = _stream_fact(doc, xref, subtype_name == "/Image")
        if fact is None:
            return None
        content_xrefs.extend(found)
        budget -= fact.read if isinstance(fact, _StreamFact) else 0
        if budget < 0:
            return None
        if isinstance(fact, _StreamFact):
            facts.append(fact)
    return facts, content_xrefs, budget


def _stream_fact(doc, xref: int, is_image: bool) -> _StreamFact | bool | None:
    """``xref``'s facts if it is a stream, False if it is not, None if it cannot be read."""
    try:
        if not doc.xref_is_stream(xref):
            return False
        raw = doc.xref_stream_raw(xref)
    except Exception:  # noqa: BLE001 - unreadable stream -> doubt
        return None
    if raw is None:
        return None
    filters = extract_xref_filters(doc, xref) if is_image else []
    is_jbig2 = any(name == _UNREADABLE_FILTER or normalize_filter(name) == "JBIG2Decode" for name in filters)
    globals_data, globals_missing = _jbig2_globals(doc, xref)
    has_globals = globals_missing or globals_data is not None
    read = len(raw) + len(globals_data or b"")
    return _StreamFact(xref, hashlib.sha256(raw).hexdigest(), is_image, is_jbig2, has_globals, read)


def _holds_inline_jbig2(doc, content_xrefs: list[int], budget: int) -> bool:
    """True when a page's or listed content stream holds an inline JBIG2 image, or cannot be read within ``budget``."""
    for source, error_reason in _content_streams(doc, content_xrefs):
        if error_reason is not None or source is None:
            return True
        budget -= len(source)
        if budget < 0 or _has_inline_jbig2(source):
            return True
    return False


def _has_inline_jbig2(content: bytes) -> bool:
    """True when an inline image (``BI``) in ``content`` names JBIG2Decode, or a filter that cannot be read."""
    start = 0
    for _ in range(len(content) // 2 + 1):
        bi_match = _BI_TOKEN.search(content, start)
        if bi_match is None:
            return False
        window = content[bi_match.end() : bi_match.end() + _MAX_INLINE_DICT_BYTES]
        id_match = _ID_TOKEN.search(window)
        raw_dict = unescape_pdf_names((window[: id_match.start()] if id_match else window).decode("latin-1"))
        filters = _inline_dict_filters(raw_dict)
        if any(name == _UNREADABLE_FILTER or normalize_filter(name) == "JBIG2Decode" for name in filters):
            return True
        start = bi_match.end()
    return True  # unreachable (each step passes a 2-byte token); at the bound, doubt


def _page_jbig2_streams(doc, jbig2: dict[int, str]) -> tuple[frozenset[str], ...] | None:
    """For each page, the raw-stream SHA-256 of the JBIG2 ``/Image`` objects PyMuPDF finds on it, forms included."""
    try:
        return tuple(
            frozenset(jbig2[info[0]] for info in page.get_images(full=True) if info[0] in jbig2) for page in doc
        )
    except Exception:  # noqa: BLE001 - a page's images cannot be listed -> doubt
        return None


def jbig2_agreement_for_path(filepath) -> Jbig2Agreement:
    """Open ``filepath`` with PyMuPDF and return :func:`jbig2_agreement` (no agreement if it cannot)."""
    from datagrunt.core.pdf_io.extraction.pymupdf_backend import _import_pymupdf

    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(filepath)
    except Exception as exc:  # noqa: BLE001 - unreadable here -> no JBIG2 image is decoded
        logger.warning("JBIG2 decode parameters of %s could not be read: %s", filepath, exc)
        return _NO_AGREEMENT
    try:
        return jbig2_agreement(doc)
    finally:
        doc.close()


def _charproc_xrefs(doc, font_xref: int) -> list[int] | None:
    """The stream xrefs of a Type3 font's ``/CharProcs`` glyph procedures; None if they cannot be read.

    ``/CharProcs`` is a dictionary of glyph name -> content stream; its content
    streams can draw inline images, so each is scanned. Handles both an inline
    dictionary value and a reference to a dictionary object.
    """
    try:
        kind, value = doc.xref_get_key(font_xref, "CharProcs")
        if kind == "dict":
            return [int(ref) for ref in re.findall(r"(\d+)\s+\d+\s+R", str(value))]
        if kind != "xref":
            return []
        obj = int(str(value).split()[0])
        xrefs = []
        for key in doc.xref_get_keys(obj):
            k, v = doc.xref_get_key(obj, key)
            if k == "xref":
                xrefs.append(int(str(v).split()[0]))
        return xrefs
    except Exception:  # noqa: BLE001 - unreadable glyph procedures -> the caller blocks the render
        return None


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
