"""Hybrid-page unmapped-region OCR (#437, #479 Option 3).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Runs only when a
page is routed ``"hybrid"``, ``ocr != "never"``, and ``assessment.unmapped_regions``
is non-empty.

Each invalid-char token (including unmapped non-AGL ligature control codes such as
``\\x07`` and ``\\x1f`` flagged in ``RawSpan.map_errors``) is cropped from a
bounded render at ``vision_dpi`` (within ``max_render_pixels`` and
``memory_cap_bytes``), padded with a white margin, and read with ``tesseract``
within the page's shared OCR budget. Returned OCR words are mapped back to page
coordinates and merged into the page's pieces before lattice detection and page
assembly; every replaced original span segment is flagged ``"superseded"`` and
never dropped.
"""

import dataclasses
import math
import os
import time
from bisect import bisect_left
from typing import NamedTuple

from datagrunt.pdf import _render_worker
from datagrunt.pdf._loaders import PageDraw
from datagrunt.pdf._options import (
    _MIN_DPI,
    _MIN_RENDER_PIXELS,
    DPI,
    MAX_RENDER_PIXELS,
    MIN_IMAGE_DIMENSION,
    VisionOptions,
)
from datagrunt.pdf._pdfium_common import Box, union_box
from datagrunt.pdf._source import SourceIdentity, check_binding
from datagrunt.pdf._validate import int_at_least, strict_bool
from datagrunt.pdf.assembly import (
    assemble_page,
    check_assessment,
    conservation,
    page_elements,
    page_figures,
    raw_page_warnings,
)
from datagrunt.pdf.elements import BBox, HiddenReason, Page, PageImage, PageQuality
from datagrunt.pdf.layout import MAX_CROSSINGS, LatticeResult, find_lattice_tables, page_layout
from datagrunt.pdf.ocr import OCRWord, bands, normalized, ocr_words
from datagrunt.pdf.pdfium_engine import PageGeometry, RawPage, RawSpan, _checked_path
from datagrunt.pdf.quality import PageAssessment, SpanPiece, raster_cover_fraction, valid_char
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page
from datagrunt.pdf.vision import LEAST_SIZE_PT

__all__ = [
    "MAX_HYBRID_REGIONS",
    "crop_region",
    "hybrid_page",
    "is_valid_offset",
    "map_word_to_page",
    "partition_piece",
]

# Explicit per-page bound on unmapped regions OCR'd on one hybrid page.
MAX_HYBRID_REGIONS = 32
# White pixel margin added on each side of a cropped region so tesseract does not clip edge glyphs.
_CROP_PAD_PX = 16
_POINTS_PER_INCH = 72.0
_SUPERSEDED: tuple[HiddenReason, ...] = ("superseded",)
_PAINTED = (0, 0, 0, 255)
_FILL = 0
_SCAN_COVER_THRESHOLD = 0.50
_MIN_PAINTED_DIGITAL_CHARS = 20

_default_render_page = render_page


class _RegionTarget(NamedTuple):
    """One unmapped token (or standalone region box) queued for region OCR."""

    piece_index: int
    start: int
    end: int
    bbox: Box
    span: RawSpan | None


class _RegionCrop(NamedTuple):
    """One cropped region image with its pixel origin and white border padding."""

    image: RenderedPage
    px0: int
    py0: int
    crop_w: int
    crop_h: int
    pad: int


class _Drawn(NamedTuple):
    """A rendered hybrid page at its effective DPI, or why it was not rendered."""

    page: RenderedPage | None
    dpi: int
    failure: str


class _RegionOutcome(NamedTuple):
    """Replacements, standalone OCR words, warnings, and word confidences for one hybrid page."""

    replacements: dict[tuple[int, int, int], tuple[OCRWord, ...]]
    standalone: tuple[tuple[Box, tuple[OCRWord, ...]], ...]
    warnings: tuple[str, ...]
    confidences: tuple[float, ...]


class _AssemblyConfig(NamedTuple):
    """Source binding and rendering parameters shared by fallback and merged hybrid assembly."""

    source: str | os.PathLike[str]
    password: str | None
    identity: SourceIdentity | None
    sha256: str | None
    min_image_dimension: int
    dpi: int
    max_render_pixels: int
    sandbox: bool


def is_valid_offset(span: RawSpan, offset: int) -> bool:
    """True unless ``span``'s char at ``offset`` is a map error or invalid Unicode (matching ``quality._valid``)."""
    if not isinstance(span, RawSpan):
        raise TypeError(f"span must be a RawSpan, got {type(span).__name__}")
    int_at_least("offset", offset, 0)
    if offset >= len(span.text):
        raise ValueError(f"offset {offset} out of range for span of length {len(span.text)}")
    errors = span.map_errors
    at = bisect_left(errors, offset)
    if at < len(errors) and errors[at] == offset:
        return False
    return valid_char(span.text[offset])


def _is_valid_separator(span: RawSpan, offset: int) -> bool:
    """True only for valid whitespace (keeps unmapped control codes like ``\\x1f`` inside their word)."""
    return span.text[offset].isspace() and is_valid_offset(span, offset)


def _clamped_box(box: Box, page_size: tuple[float, float]) -> Box:
    """Clamp ``box`` to ``[0, width_pt] x [0, height_pt]`` while preserving ``x0 <= x1`` and ``y0 <= y1``."""
    width_pt, height_pt = page_size
    x0 = min(max(box[0], 0.0), width_pt)
    y0 = min(max(box[1], 0.0), height_pt)
    x1 = min(max(box[2], x0), width_pt)
    y1 = min(max(box[3], y0), height_pt)
    return (x0, y0, x1, y1)


def _token_runs(piece: SpanPiece) -> list[tuple[int, int, bool]]:
    """Partition ``piece`` into ``(start, end, has_invalid)`` runs along valid-whitespace boundaries."""
    span = piece.span
    runs: list[tuple[int, int, bool]] = []
    pos = piece.start
    while pos < piece.end:
        is_sep = _is_valid_separator(span, pos)
        end = pos + 1
        while end < piece.end and _is_valid_separator(span, end) == is_sep:
            end += 1
        has_invalid = False if is_sep else any(not is_valid_offset(span, idx) for idx in range(pos, end))
        runs.append((pos, end, has_invalid))
        pos = end
    return runs


def partition_piece(
    piece: SpanPiece, piece_index: int, page_size: tuple[float, float]
) -> tuple[tuple[SpanPiece, ...], tuple[_RegionTarget, ...]]:
    """Split a visible piece containing invalid chars into valid segments and invalid-token OCR targets."""
    if not isinstance(piece, SpanPiece):
        raise TypeError(f"piece must be a SpanPiece, got {type(piece).__name__}")
    int_at_least("piece_index", piece_index, 0)
    span = piece.span
    if piece.hidden_reasons or (not span.map_errors and span.text.isascii()):
        return (piece,), ()
    if all(is_valid_offset(span, offset) for offset in range(piece.start, piece.end)):
        return (piece,), ()
    segments: list[SpanPiece] = []
    targets: list[_RegionTarget] = []
    valid_start: int | None = None
    for run_start, run_end, has_invalid in _token_runs(piece):
        if not has_invalid:
            valid_start = run_start if valid_start is None else valid_start
            continue
        if valid_start is not None:
            segments.append(SpanPiece(span=span, start=valid_start, end=run_start, hidden_reasons=()))
            valid_start = None
        segments.append(SpanPiece(span=span, start=run_start, end=run_end, hidden_reasons=()))
        raw_box = union_box(span.char_boxes[run_start:run_end] + span.tight_boxes[run_start:run_end])
        targets.append(_RegionTarget(piece_index, run_start, run_end, _clamped_box(raw_box, page_size), span))
    if valid_start is not None:
        segments.append(SpanPiece(span=span, start=valid_start, end=piece.end, hidden_reasons=()))
    return tuple(segments), tuple(targets)


def _collect_targets(
    pieces: tuple[SpanPiece, ...], unmapped_regions: tuple[Box, ...], page_size: tuple[float, float]
) -> tuple[_RegionTarget, ...]:
    """Collect piece-backed targets in piece order, falling back to standalone boxes for synthetic assessments."""
    targets: list[_RegionTarget] = []
    for piece_index, piece in enumerate(pieces):
        _, piece_targets = partition_piece(piece, piece_index, page_size)
        targets.extend(piece_targets)
    if targets:
        return tuple(targets)
    return tuple(_RegionTarget(-1, 0, 0, _clamped_box(box, page_size), None) for box in unmapped_regions)


def crop_region(page: RenderedPage, region: Box, *, dpi: int, max_render_pixels: int) -> _RegionCrop:
    """Crop ``region`` (page points) from 8-bit grayscale ``page`` and pad with a white border within pixel bounds."""
    if not isinstance(page, RenderedPage) or page.channels != 1:
        raise ValueError("page must be an 8-bit grayscale RenderedPage")
    int_at_least("dpi", dpi, 1)
    int_at_least("max_render_pixels", max_render_pixels, 1)
    scale = dpi / _POINTS_PER_INCH
    px0 = max(0, min(page.px_width - 1, math.floor(region[0] * scale)))
    py0 = max(0, min(page.px_height - 1, math.floor(region[1] * scale)))
    px1 = max(px0 + 1, min(page.px_width, math.ceil(region[2] * scale)))
    py1 = max(py0 + 1, min(page.px_height, math.ceil(region[3] * scale)))
    crop_w, crop_h = px1 - px0, py1 - py0
    pad = _CROP_PAD_PX if (crop_w + 2 * _CROP_PAD_PX) * (crop_h + 2 * _CROP_PAD_PX) <= max_render_pixels else 0
    out_w, out_h = crop_w + 2 * pad, crop_h + 2 * pad
    buf = bytearray([255]) * (out_w * out_h)
    src = page.pixels
    for row_idx in range(crop_h):
        src_start = (py0 + row_idx) * page.px_width + px0
        dst_start = (pad + row_idx) * out_w + pad
        buf[dst_start : dst_start + crop_w] = src[src_start : src_start + crop_w]
    cropped = RenderedPage(pixels=bytes(buf), px_width=out_w, px_height=out_h)
    return _RegionCrop(image=cropped, px0=px0, py0=py0, crop_w=crop_w, crop_h=crop_h, pad=pad)


def _mapped_axis(
    w0: float, w1: float, pad_pt: float, origin_pt: float, span_pt: float, fb0: float, fb1: float, limit: float
) -> tuple[float, float]:
    """Map one axis of a padded crop box back to page points, falling back cleanly if inside padding."""
    rel0 = min(max(w0 - pad_pt, 0.0), span_pt)
    rel1 = min(max(w1 - pad_pt, rel0), span_pt)
    if rel1 <= rel0:
        lo = min(max(fb0, 0.0), limit)
        return lo, min(max(fb1, lo), limit)
    lo = min(max(origin_pt + rel0, 0.0), limit)
    hi = min(max(origin_pt + rel1, lo), limit)
    return lo, hi


def map_word_to_page(
    word: OCRWord, crop: _RegionCrop, *, dpi: int, fallback: Box, page_size: tuple[float, float]
) -> OCRWord:
    """Map an ``OCRWord`` from crop-local points back to clamped displayed page coordinates."""
    if not isinstance(word, OCRWord):
        raise TypeError(f"word must be an OCRWord, got {type(word).__name__}")
    int_at_least("dpi", dpi, 1)
    scale = _POINTS_PER_INCH / dpi
    pad_pt = crop.pad * scale
    x0, x1 = _mapped_axis(
        word.bbox.x0,
        word.bbox.x1,
        pad_pt,
        crop.px0 * scale,
        crop.crop_w * scale,
        fallback[0],
        fallback[2],
        page_size[0],
    )
    y0, y1 = _mapped_axis(
        word.bbox.y0,
        word.bbox.y1,
        pad_pt,
        crop.py0 * scale,
        crop.crop_h * scale,
        fallback[1],
        fallback[3],
        page_size[1],
    )
    return dataclasses.replace(word, bbox=BBox(x0=x0, y0=y0, x1=x1, y1=y1))


def _draw_hybrid_page(
    source: str | os.PathLike[str],
    geometry: PageGeometry,
    password: str | None,
    options: VisionOptions,
    deadline: float,
) -> _Drawn:
    """Render the hybrid page at effective ``vision_dpi`` within pixel, memory, and time bounds."""
    try:
        draw_dpi = effective_dpi(options.vision_dpi, options.max_render_pixels, geometry.width_pt, geometry.height_pt)
        left = deadline - time.monotonic()
        if left <= 0.0:
            spent = f"the page's {options.page_budget_s:g} s budget is spent"
            return _Drawn(None, 0, f"hybrid OCR: the page was not rendered: {spent}")
        if not options.sandbox and render_page is _default_render_page:
            rendered = _render_worker.draw_page_in_process(
                source,
                geometry.page_number - 1,
                dpi=draw_dpi,
                password=password,
                memory_cap_bytes=options.memory_cap_bytes,
                annotations=True,
            )
        else:
            rendered = render_page(
                source,
                geometry.page_number - 1,
                dpi=draw_dpi,
                password=password,
                memory_cap_bytes=options.memory_cap_bytes,
                timeout_s=min(options.render_timeout_s, left),
            )
    except (RenderRefused, RenderFailed) as exc:
        return _Drawn(None, 0, f"hybrid OCR: the page was not rendered: {exc}")
    return _Drawn(rendered, draw_dpi, "")


def _ocr_single_region(
    grey: RenderedPage,
    target: _RegionTarget,
    region_number: int,
    total_regions: int,
    dpi: int,
    page_size: tuple[float, float],
    options: VisionOptions,
    deadline: float,
) -> tuple[tuple[OCRWord, ...], str, bool]:
    """Run OCR on one target crop; return ``(mapped_words, warning, abort_remaining)``."""
    spent = f"the page's {options.page_budget_s:g} s budget is spent"
    if deadline - time.monotonic() <= 0.0:
        return (), f"hybrid OCR stopped before region {region_number} of {total_regions}: {spent}", True
    crop = crop_region(grey, target.bbox, dpi=dpi, max_render_pixels=options.max_render_pixels)
    scale = _POINTS_PER_INCH / dpi
    crop_size_pt = ((crop.image.px_width + 0.5) * scale, (crop.image.px_height + 0.5) * scale)
    result = ocr_words(
        bands(crop.image),
        dpi=dpi,
        rotation=0,
        page_size_pt=crop_size_pt,
        timeout_s=options.ocr_timeout_s,
        language=options.language,
        deadline=deadline,
    )
    if result.status == "unavailable":
        return (), f"hybrid OCR skipped: tesseract is unavailable: {result.reason}", True
    if result.status == "spent":
        return (), f"hybrid OCR stopped at region {region_number} of {total_regions}: {spent}", True
    if result.status != "ok":
        return (), f"hybrid OCR region {region_number} of {total_regions} {result.status}: {result.reason}", False
    mapped = tuple(
        map_word_to_page(word, crop, dpi=dpi, fallback=target.bbox, page_size=page_size) for word in result.words
    )
    return mapped, "", False


def _record_region_words(
    target: _RegionTarget,
    words: tuple[OCRWord, ...],
    replacements: dict[tuple[int, int, int], tuple[OCRWord, ...]],
    standalone: list[tuple[Box, tuple[OCRWord, ...]]],
    confidences: list[float],
) -> None:
    """Store accepted OCR words and their confidences for ``target``."""
    confidences.extend(word.confidence for word in words)
    if target.piece_index >= 0:
        replacements[(target.piece_index, target.start, target.end)] = words
        return
    standalone.append((target.bbox, words))


def _ocr_targets(
    grey: RenderedPage,
    targets: tuple[_RegionTarget, ...],
    dpi: int,
    page_size: tuple[float, float],
    options: VisionOptions,
    deadline: float,
    max_regions: int,
) -> _RegionOutcome:
    """OCR up to ``max_regions`` unmapped targets within ``deadline``, collecting replacements and warnings."""
    warnings: list[str] = []
    if len(targets) > max_regions:
        extra = len(targets) - max_regions
        warnings.append(
            f"hybrid OCR capped at {max_regions} of {len(targets)} unmapped region(s); "
            f"the remaining {extra} keep their digital text"
        )
    capped = targets[:max_regions]
    replacements: dict[tuple[int, int, int], tuple[OCRWord, ...]] = {}
    standalone: list[tuple[Box, tuple[OCRWord, ...]]] = []
    confidences: list[float] = []
    for number, target in enumerate(capped, start=1):
        words, note, abort = _ocr_single_region(grey, target, number, len(capped), dpi, page_size, options, deadline)
        if note:
            warnings.append(note)
        if words:
            _record_region_words(target, words, replacements, standalone, confidences)
        if abort:
            break
    return _RegionOutcome(replacements, tuple(standalone), tuple(warnings), tuple(confidences))


def _ocr_word_piece(word: OCRWord, object_index: int, template: RawSpan | None) -> SpanPiece:
    """Build one visible ``SpanPiece`` for ``word`` inheriting font metrics from ``template`` when available."""
    box = word.bbox
    count = len(word.text)
    edges = [box.x0 + (box.x1 - box.x0) * idx / count for idx in range(count)] + [box.x1]
    chars = tuple((edges[idx], box.y0, edges[idx + 1], box.y1) for idx in range(count))
    box_height = max(box.y1 - box.y0, LEAST_SIZE_PT)
    size = max(abs(template.font_size_pt), LEAST_SIZE_PT) if template is not None else box_height
    em_width = template.em_width_pt if (template is not None and template.em_width_pt >= LEAST_SIZE_PT) else size
    span = RawSpan(
        text=word.text,
        bbox=(box.x0, box.y0, box.x1, box.y1),
        char_boxes=chars,
        tight_boxes=chars,
        font_size_pt=size,
        em_width_pt=em_width,
        base_font_name=template.base_font_name if template is not None else "",
        is_bold=template.is_bold if template is not None else False,
        is_italic=template.is_italic if template is not None else False,
        render_mode=_FILL,
        fill_rgba=_PAINTED,
        stroke_rgba=_PAINTED,
        object_index=object_index,
        generated_breaks=(),
        map_errors=(),
    )
    return SpanPiece(span=span, start=0, end=count, hidden_reasons=())


def _merge_piece(
    piece: SpanPiece,
    piece_index: int,
    replacements: dict[tuple[int, int, int], tuple[OCRWord, ...]],
    next_index: list[int],
    page_size: tuple[float, float],
) -> list[SpanPiece]:
    """Return ``piece`` unchanged, or split with replaced targets flagged ``superseded`` and OCR words inserted."""
    segments, _ = partition_piece(piece, piece_index, page_size)
    if not any((piece_index, seg.start, seg.end) in replacements for seg in segments):
        return [piece]
    merged: list[SpanPiece] = []
    for seg in segments:
        words = replacements.get((piece_index, seg.start, seg.end))
        if words is None:
            merged.append(seg)
            continue
        merged.append(SpanPiece(span=seg.span, start=seg.start, end=seg.end, hidden_reasons=_SUPERSEDED))
        for word in words:
            merged.append(_ocr_word_piece(word, next_index[0], seg.span))
            next_index[0] += 1
    return merged


def _merge_pieces(
    pieces: tuple[SpanPiece, ...],
    outcome: _RegionOutcome,
    start_object_index: int,
    page_size: tuple[float, float],
) -> tuple[SpanPiece, ...]:
    """Merge OCR replacement words into ``pieces`` while flagging each replaced span ``superseded``."""
    next_index = [start_object_index]
    merged: list[SpanPiece] = []
    for piece_index, piece in enumerate(pieces):
        merged.extend(_merge_piece(piece, piece_index, outcome.replacements, next_index, page_size))
    for _box, words in outcome.standalone:
        for word in words:
            merged.append(_ocr_word_piece(word, next_index[0], None))
            next_index[0] += 1
    return tuple(merged)


def _is_scan_mosaic_page(raw_page: RawPage, pieces: tuple[SpanPiece, ...]) -> bool:
    """Match ``assembly._is_scan_mosaic_page`` for figure mosaic suppression on a hybrid page."""
    painted = sum(
        1
        for piece in pieces
        if not piece.hidden_reasons and piece.span.render_mode not in (3, 7)
        for ch in piece.text
        if not ch.isspace() and valid_char(ch)
    )
    if painted >= _MIN_PAINTED_DIGITAL_CHARS:
        return False
    return (
        raster_cover_fraction(raw_page.images.images, raw_page.geometry.width_pt, raw_page.geometry.height_pt)
        > _SCAN_COVER_THRESHOLD
    )


def _lattice_warnings(lattice: LatticeResult) -> tuple[str, ...]:
    """Match ``assembly._lattice_warnings`` for bounded lattice grids."""
    if lattice.crossing_cap_reached:
        return (
            f"the page's rules cross more than {MAX_CROSSINGS} times: no lattice tables were read; "
            "its text reads as stream text",
        )
    if lattice.skipped_grids:
        grids = f"{lattice.skipped_grids} ruled grid(s) left out of the lattice pass (over its per-page bounds)"
        return (f"{grids}: their text reads as stream text",)
    return ()


def _hybrid_page_image(geometry: PageGeometry, cfg: _AssemblyConfig) -> tuple[PageImage | None, tuple[str, ...]]:
    """Build the lazy ``PageImage`` for a hybrid page, matching ``assembly.assemble_page``."""
    try:
        drawn_at = effective_dpi(cfg.dpi, cfg.max_render_pixels, geometry.width_pt, geometry.height_pt)
    except RenderRefused as exc:
        return None, (f"the page has no page image: {exc}",)
    draw = PageDraw(
        source=_checked_path(cfg.source, "source"),
        page_index=geometry.page_number - 1,
        dpi=drawn_at,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        password=cfg.password,
        identity=cfg.identity,
        sha256=cfg.sha256,
        sandbox=cfg.sandbox,
    )
    width, height = draw.size
    image = PageImage(
        page_number=geometry.page_number, dpi=float(drawn_at), width_px=width, height_px=height, _loader=draw.png_bytes
    )
    return image, ()


def _fallback_assemble(
    raw_page: RawPage, assessment: PageAssessment, extra_warnings: tuple[str, ...], cfg: _AssemblyConfig
) -> Page:
    """Assemble the untouched digital assessment with ``extra_warnings`` appended."""
    updated = (
        dataclasses.replace(assessment, warnings=(*assessment.warnings, *extra_warnings))
        if extra_warnings
        else assessment
    )
    lattice = find_lattice_tables(raw_page.segments.segments, updated.pieces)
    return assemble_page(
        raw_page,
        updated,
        lattice,
        source=cfg.source,
        password=cfg.password,
        identity=cfg.identity,
        sha256=cfg.sha256,
        min_image_dimension=cfg.min_image_dimension,
        dpi=cfg.dpi,
        max_render_pixels=cfg.max_render_pixels,
        sandbox=cfg.sandbox,
    )


def _merged_quality(
    assessment: PageAssessment, merged_pieces: tuple[SpanPiece, ...], confidences: tuple[float, ...]
) -> PageQuality:
    """Update ``hidden_spans_flagged`` and ``ocr_mean_confidence`` after hybrid region replacement."""
    flagged_spans = len({id(p.span) for p in (*merged_pieces, *assessment.recovered_pieces) if p.hidden_reasons})
    mean_conf = sum(confidences) / len(confidences) if confidences else None
    return dataclasses.replace(assessment.quality, hidden_spans_flagged=flagged_spans, ocr_mean_confidence=mean_conf)


def _hybrid_warnings(
    raw_page: RawPage,
    figures_warnings: tuple[str, ...],
    assessment: PageAssessment,
    outcome: _RegionOutcome,
    lattice: LatticeResult,
    layout_warnings: tuple[str, ...],
    mismatch: tuple[str, ...],
    undrawn: tuple[str, ...],
) -> tuple[str, ...]:
    """Collect warnings for a merged hybrid page in canonical stage order."""
    return (
        *raw_page_warnings(raw_page),
        *figures_warnings,
        *assessment.warnings,
        *outcome.warnings,
        *_lattice_warnings(lattice),
        *layout_warnings,
        *mismatch,
        *undrawn,
    )


def _assemble_merged_hybrid(
    raw_page: RawPage, assessment: PageAssessment, outcome: _RegionOutcome, cfg: _AssemblyConfig
) -> Page:
    """Run lattice detection, layout, Pass C conservation, and page assembly over merged hybrid pieces."""
    geometry = raw_page.geometry
    page_size = (geometry.width_pt, geometry.height_pt)
    start_index = max((span.object_index for span in raw_page.text.spans), default=-1) + 1
    merged_pieces = _merge_pieces(assessment.pieces, outcome, start_index, page_size)
    lattice = find_lattice_tables(raw_page.segments.segments, merged_pieces)
    figures = page_figures(
        raw_page.images,
        geometry,
        source=cfg.source,
        password=cfg.password,
        min_image_dimension=cfg.min_image_dimension,
        sandbox=cfg.sandbox,
        image_only=_is_scan_mosaic_page(raw_page, assessment.pieces),
    )
    layout = page_layout(
        merged_pieces,
        assessment.recovered_pieces,
        lattice,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        figures=tuple(draft.bbox for draft in figures.drafts),
    )
    elements = page_elements(layout.items, figures.drafts, geometry)
    verified, mismatch = conservation((*merged_pieces, *assessment.recovered_pieces), elements)
    image, undrawn = _hybrid_page_image(geometry, cfg)
    warnings = _hybrid_warnings(
        raw_page, figures.warnings, assessment, outcome, lattice, layout.warnings, mismatch, undrawn
    )
    return Page(
        page_number=geometry.page_number,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        status="ok",
        route="hybrid",
        quality=_merged_quality(assessment, merged_pieces, outcome.confidences),
        elements=elements,
        page_image=image,
        integrity_verified=verified,
        warnings=warnings,
        image_only_reasons=assessment.image_only_reasons,
    )


def hybrid_page(
    raw_page: RawPage,
    assessment: PageAssessment,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    identity: SourceIdentity | None = None,
    sha256: str | None = None,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
    dpi: int = DPI,
    max_render_pixels: int = MAX_RENDER_PIXELS,
    sandbox: bool = False,
    options: VisionOptions = VisionOptions(),
    max_regions: int = MAX_HYBRID_REGIONS,
) -> Page:
    """OCR a hybrid page's unmapped regions, merge words by position, and assemble the A1 ``Page``."""
    check_assessment(raw_page, assessment)
    check_binding(identity, sha256)
    if assessment.route != "hybrid":
        raise ValueError(f"assessment routes the page {assessment.route!r}, not 'hybrid'")
    if not isinstance(options, VisionOptions):
        raise TypeError(f"options must be a VisionOptions, got {type(options).__name__}")
    int_at_least("dpi", dpi, _MIN_DPI)
    int_at_least("max_render_pixels", max_render_pixels, _MIN_RENDER_PIXELS)
    int_at_least("max_regions", max_regions, 1)
    strict_bool("sandbox", sandbox)
    cfg = _AssemblyConfig(source, password, identity, sha256, min_image_dimension, dpi, max_render_pixels, sandbox)
    if not assessment.unmapped_regions:
        return _fallback_assemble(raw_page, assessment, (), cfg)
    geometry = raw_page.geometry
    page_size = (geometry.width_pt, geometry.height_pt)
    targets = _collect_targets(assessment.pieces, assessment.unmapped_regions, page_size)
    deadline = time.monotonic() + options.page_budget_s
    drawn = _draw_hybrid_page(source, geometry, password, options, deadline)
    if drawn.page is None:
        return _fallback_assemble(raw_page, assessment, (drawn.failure,), cfg)
    grey = normalized(drawn.page)
    outcome = _ocr_targets(grey, targets, drawn.dpi, page_size, options, deadline, max_regions)
    if not outcome.replacements and not outcome.standalone:
        return _fallback_assemble(raw_page, assessment, outcome.warnings, cfg)
    return _assemble_merged_hybrid(raw_page, assessment, outcome, cfg)
