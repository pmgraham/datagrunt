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
import zlib
from collections import Counter
from typing import NamedTuple

from datagrunt.core.pdf_io.extraction.content_lexer import (
    GLOBALS_TOKEN,
    JBIG2_DECODE_TOKEN,
    JPX_DECODE_TOKEN,
    InlineImage,
    count_tokens,
    inline_images,
)
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
# The codec names the byte-level counts look for (JPXDecode and JBIG2Decode have no abbreviation).
_CODEC_NAMES = frozenset({"JPXDecode", "JBIG2Decode"})
# The file is read in chunks of this many bytes to count name tokens; each
# chunk keeps this many bytes of the one before (a #-escaped /JBIG2Globals is 37).
_TOKEN_CHUNK_BYTES = 1 << 20
_TOKEN_OVERLAP_BYTES = 64

_JBIG2_GLOBALS_KEY_RE = re.compile(r"/JBIG2Globals\b")
_JBIG2_GLOBALS_NULL_RE = re.compile(r"/JBIG2Globals\s+null\b")
_JBIG2_GLOBALS_REF_RE = re.compile(r"/JBIG2Globals\s+(\d+)\s+\d+\s+R")
_DICT_OUTER_RE = re.compile(r"^\s*<<.*>>\s*$", re.DOTALL)
_DECODE_PARMS_ARRAY_TOKEN_RE = re.compile(r"<<.*?>>|(\d+)\s+\d+\s+R|null\b", re.DOTALL)


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


class _ImageScan(NamedTuple):
    """One image object as the render scan reads it: why it blocks the render (or None), and what it counted."""

    reason: str | None
    read: int
    is_jbig2: bool


def _image_block_reason(doc, xref: int) -> _ImageScan:
    """Guard decision for the image at ``xref``, the raw bytes it read (for the budget), and its JBIG2 facts."""
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
    is_jbig2 = _may_be_jbig2(filters)
    return _ImageScan(decision.reason if decision.skip else None, read, is_jbig2)


def _may_be_jbig2(filters: list[str]) -> bool:
    """True when ``filters`` name JBIG2Decode, or cannot be read."""
    return any(name == _UNREADABLE_FILTER or normalize_filter(name) == "JBIG2Decode" for name in filters)


def _inline_block_reason(content: bytes) -> str | None:
    """Why a render must not decode one of ``content``'s inline images, or None when it may.

    Every inline image is read by :func:`~content_lexer.inline_images`; one it
    cannot read for certain blocks the render. Each is run through the guard
    (a simple filter, or none, is bounded by ``/W`` ``/H``); an image codec
    whose data no ``EI`` ends blocks it too. A JPXDecode or JBIG2Decode name
    anywhere in the stream (strings and comments included) that is not in an
    image the lexer read blocks it: the lexer may have misplaced where some
    image's data ends.
    """
    found = inline_images(content)
    if isinstance(found, str):
        return f"an inline image {found}; render skipped"
    for image in found:
        # The lexer has unescaped each name once, as a reader does: escape "#" so the guard's unescape keeps it.
        filters = [name.replace("#", "#23") for name in image.filters]
        decision = inspect_image(
            image.data or None, filters, label="an inline image", jbig2_globals_missing=image.has_globals
        )
        if decision.skip:
            return decision.reason
        if filters and normalize_filter(filters[-1]) not in _NON_IMAGE_FILTERS and not image.delimited:
            return "an inline image could not be delimited; render skipped"
    named = sum(_codec_names(image) for image in found)
    if count_tokens(JPX_DECODE_TOKEN, content) + count_tokens(JBIG2_DECODE_TOKEN, content) > named:
        return "a JPXDecode or JBIG2Decode name lies outside the inline images read; render skipped"
    return None


def _codec_names(image: InlineImage) -> int:
    """How many of an inline image's filter names (unescaped once) are JPXDecode or JBIG2Decode.

    Only its filter value counts: a codec name under any other key is not one
    the guard inspected, so it is left to count against the stream's.
    """
    return sum(name in _CODEC_NAMES for name in image.filters)


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
    fail-closed reason. Then, for a document holding a JBIG2 image, or whose
    bytes (the file's, its decoded object streams' or its content streams')
    name JBIG2Decode at all, both readers must find one sound revision
    (:func:`_one_sound_revision`); and those bytes may hold no
    ``/JBIG2Globals`` name. (A JBIG2 image whose globals PyMuPDF resolves is
    refused above, so no such name is ever one the scan checked.)
    """
    try:
        count = doc.xref_length()
    except Exception as exc:  # noqa: BLE001
        return f"document object table unreadable ({exc}); render skipped"
    if count > _MAX_SCAN_OBJECTS:
        return f"document has {count} objects, over the {_MAX_SCAN_OBJECTS}-object scan bound; render skipped"
    scanned = _scan_objects(doc, count)
    if isinstance(scanned, str):
        return scanned
    budget = _MAX_SCAN_BYTES - scanned.read
    content_tokens, inline_jbig2 = 0, 0
    # Inline images live in content streams, not in xref /Subtype: scan every
    # page's content and every Form XObject / tiling-pattern stream.
    for source, error_reason in _content_streams(doc, scanned.content_xrefs):
        if error_reason is not None or source is None:
            return error_reason or "content stream unreadable; render skipped"
        budget -= len(source)
        if budget < 0:
            return "content stream scan exceeded the per-document byte budget; render skipped"
        reason = _inline_block_reason(source)
        if reason is not None:
            return reason
        content_tokens += count_tokens(GLOBALS_TOKEN, source)
        inline_jbig2 += count_tokens(JBIG2_DECODE_TOKEN, source)
    outside = _tokens_outside_content(doc)
    if outside is None:
        return "the file's JBIG2 names could not be counted; render skipped"
    globals_outside, jbig2_outside = outside
    if (scanned.jbig2_images or inline_jbig2 or jbig2_outside) and not _one_sound_revision(doc):
        return "a JBIG2 image is in a file both readers do not read as one sound revision; render skipped"
    if globals_outside + content_tokens:
        return "a /JBIG2Globals name is in the file; render skipped"
    return None


class _ObjectScan(NamedTuple):
    """The render scan's pass over every object: content streams to read next, and its counts."""

    content_xrefs: list[int]
    read: int
    jbig2_images: int


def _scan_objects(doc, count: int) -> _ObjectScan | str:
    """Every image object guarded and every content stream found, or the first reason to block the render."""
    content_xrefs: list[int] = []  # Form XObjects and tiling patterns: streams that may hold inline images
    read, jbig2_images = 0, 0
    for xref in range(1, count):
        subtype_name, subtype_unreadable = _resolve_subtype(doc, xref)
        if subtype_unreadable:
            return f"xref {xref}: unreadable /Subtype; render skipped"
        if subtype_name != "/Image":
            found = _object_content_xrefs(doc, xref, subtype_name)
            if isinstance(found, str):
                return found
            content_xrefs.extend(found)
            continue
        image = _image_block_reason(doc, xref)
        if image.reason is not None:
            return image.reason
        read += image.read
        jbig2_images += image.is_jbig2
        if read > _MAX_SCAN_BYTES:
            return "image stream scan exceeded the per-document byte budget; render skipped"
    return _ObjectScan(content_xrefs, read, jbig2_images)


def _one_sound_revision(doc) -> bool:
    """True when PyMuPDF and pdfium each read the file as one revision with a sound cross-reference table.

    Asked after the scan has read every object (PyMuPDF repairs a file when it
    first reads an object it cannot find).
    """
    from datagrunt.core.pdf_io.extraction.pdfium_document import pdfium_reads_one_sound_revision_for_path

    path = getattr(doc, "name", "") or ""
    return _single_unrepaired_revision(doc) and bool(path) and pdfium_reads_one_sound_revision_for_path(path)


def _tokens_outside_content(doc) -> tuple[int, int] | None:
    """``(/JBIG2Globals names, JBIG2Decode names)`` in the file's bytes and decoded object streams, any context.

    None when they cannot be counted (no file name, an unreadable file or
    object stream, or the byte budget): an over-count fails closed.
    """
    patterns = (GLOBALS_TOKEN, JBIG2_DECODE_TOKEN)
    path = getattr(doc, "name", "") or ""
    in_file = _file_token_counts(path, patterns) if path else None
    in_object_streams = _object_stream_token_counts(doc, patterns)
    if in_file is None or in_object_streams is None:
        return None
    return in_file[0] + in_object_streams[0], in_file[1] + in_object_streams[1]


def _file_token_counts(path, patterns) -> tuple[int, ...] | None:
    """How many of each of ``patterns`` the file at ``path`` holds, read in chunks; None if unreadable or too long."""
    counts, tail, read = [0] * len(patterns), b"", 0
    try:
        with open(path, "rb") as source:
            for _ in range(_MAX_SCAN_BYTES // _TOKEN_CHUNK_BYTES + 2):
                chunk = source.read(_TOKEN_CHUNK_BYTES)
                if not chunk:
                    return tuple(counts)
                read += len(chunk)
                if read > _MAX_SCAN_BYTES:
                    return None
                window = tail + chunk
                for index, pattern in enumerate(patterns):
                    # A match ending within the kept tail was counted with the chunk before.
                    counts[index] += sum(1 for match in pattern.finditer(window) if match.end() > len(tail))
                tail = window[-_TOKEN_OVERLAP_BYTES:]
    except OSError:
        return None
    return None


def _object_stream_token_counts(doc, patterns) -> tuple[int, ...] | None:
    """How many of each of ``patterns`` the document's object streams hold, decoded; None if one cannot be decoded.

    The object streams are :func:`_object_stream_xrefs`'. Only an unfiltered
    or FlateDecode one without decode parameters is decoded (zlib, its output
    capped by the scan's byte budget); any other is doubt.
    """
    try:
        count = doc.xref_length()
    except Exception:  # noqa: BLE001 - unreadable object table
        return None
    streams = _object_stream_xrefs(doc, count)
    if streams is None:
        return None
    counts, budget = [0] * len(patterns), _MAX_SCAN_BYTES
    for xref in sorted(streams):
        decoded = _decoded_object_stream(doc, xref, budget)
        if decoded is None:
            return None
        budget -= len(decoded)
        for index, pattern in enumerate(patterns):
            counts[index] += count_tokens(pattern, decoded)
    return tuple(counts)


def _object_stream_xrefs(doc, count: int) -> set[int] | None:
    """The document's object streams; None when a stream's ``/Type`` cannot be read or resolved.

    Those PyMuPDF's xref entries hold compressed objects in, and every stream
    whose ``/Type``, direct or indirect (pdfium resolves it), is ``/ObjStm``.
    When PyMuPDF cannot list its xref entries, every stream that is not an
    image and has no ``/Type`` is taken for one too.
    """
    compressed = _compressed_object_streams(doc, count)
    found = set(compressed or ())
    for xref in range(1, count):
        try:
            is_stream = doc.xref_is_stream(xref)
        except Exception:  # noqa: BLE001 - unreadable object -> doubt
            return None
        if not is_stream:
            continue
        kind = _resolved_type(doc, xref)
        if kind is None:
            return None
        untyped = compressed is None and kind == "" and _resolve_subtype(doc, xref)[0] != "/Image"
        if kind == "/ObjStm" or untyped:
            found.add(xref)
    return found


def _compressed_object_streams(doc, count: int) -> set[int] | None:
    """The object streams PyMuPDF's xref entries (type 2) hold objects in; None when PyMuPDF cannot list them."""
    try:
        import pymupdf

        pdf = pymupdf._as_pdf_document(doc).m_internal
        entries = [pymupdf.mupdf.ll_pdf_get_xref_entry_no_null(pdf, xref) for xref in range(1, count)]
        return {int(entry.ofs) for entry in entries if entry.type == "o"}
    except Exception:  # noqa: BLE001 - this PyMuPDF does not expose them
        return None


def _resolved_type(doc, xref: int) -> str | None:
    """Object ``xref``'s ``/Type`` name, unescaped and resolved if indirect; "" when it has none; None if unreadable."""
    try:
        kind, value = doc.xref_get_key(xref, "Type")
    except Exception:  # noqa: BLE001 - unreadable /Type -> doubt
        return None
    if kind == "xref":
        resolved = resolve_indirect_text(doc, str(value))
        if resolved is None:
            return None
        kind, value = ("name", resolved) if _SINGLE_NAME_RE.match(unescape_pdf_names(resolved)) else ("other", "")
    if kind != "name":
        return ""
    return unescape_pdf_names(str(value).strip())


def _decoded_object_stream(doc, xref: int, budget: int) -> bytes | None:
    """Object stream ``xref`` decoded within ``budget`` bytes, or None when it cannot be."""
    filters = [normalize_filter(name) for name in extract_xref_filters(doc, xref)]
    try:
        raw = doc.xref_stream_raw(xref)
        parms = doc.xref_get_key(xref, "DecodeParms")
    except Exception:  # noqa: BLE001 - unreadable stream -> doubt
        return None
    if raw is None or parms != ("null", "null"):
        return None
    if not filters:
        return raw
    if filters != ["FlateDecode"]:
        return None
    inflater = zlib.decompressobj()
    try:
        decoded = inflater.decompress(raw, budget + 1)
    except zlib.error:
        return None
    return decoded if inflater.eof and len(decoded) <= budget else None


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
    update; pdfium's own reading is checked by the pdfium engine); no
    content stream (pages, forms, patterns, Type3 glyphs) names JBIG2Decode
    or holds an inline image the lexer cannot read; no ``/JBIG2Globals``
    name is in the file's bytes, its decoded object streams or its content
    streams; and every JBIG2 ``/Image`` object's raw stream is the stream of
    exactly one ``/Image`` object, with no stream object of those bytes
    carrying ``/JBIG2Globals`` (or decode parameters that cannot be read).
    Any read failure, or the scan's object or byte bound, is doubt.
    """
    if not _single_unrepaired_revision(doc):
        return _NO_AGREEMENT
    scanned = _stream_facts(doc)
    if scanned is None:
        return _NO_AGREEMENT
    facts, content_xrefs, budget = scanned
    outside = _tokens_outside_content(doc)
    if _holds_inline_jbig2(doc, content_xrefs, budget) or outside is None or outside[0]:
        return _NO_AGREEMENT
    jbig2 = {fact.xref: fact.digest for fact in facts if fact.is_jbig2}
    images = Counter(fact.digest for fact in facts if fact.is_image)
    with_globals = {fact.digest for fact in facts if fact.has_globals}
    if any(images[digest] != 1 or digest in with_globals for digest in jbig2.values()):
        return _NO_AGREEMENT
    page_streams = _page_jbig2_streams(doc, jbig2)
    # PyMuPDF repairs a file when it first reads an object it cannot find: ask again after every read.
    if page_streams is None or not _single_unrepaired_revision(doc):
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
    """True when a page's or listed content stream may hold an inline JBIG2 image or a /JBIG2Globals name.

    Also True when one cannot be read within ``budget``, or the lexer cannot
    read its inline images for certain.
    """
    for source, error_reason in _content_streams(doc, content_xrefs):
        if error_reason is not None or source is None:
            return True
        budget -= len(source)
        if budget < 0 or isinstance(inline_images(source), str):
            return True
        if count_tokens(JBIG2_DECODE_TOKEN, source) or count_tokens(GLOBALS_TOKEN, source):
            return True
    return False


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
