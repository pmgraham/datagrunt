"""Pure-Python reference implementations of the PDF compute kernels.

Mirrors the Rust functions in ``datagrunt._native`` for differential parity testing
and the ``DATAGRUNT_DISABLE_RUST=1`` diagnostic toggle.
"""

import math
from collections.abc import Iterator, Sequence
from types import MappingProxyType

_BGR_BYTES = 3
_MAX_SAMPLES_PER_AXIS = 32
_OFF_PAGE_TOLERANCE_PT = 0.5
_CHUNK = 1 << 16
_LZW_STACK = 4000
_LZW_TABLE = 4094
_LZW_WIDTHS = MappingProxyType({254: 10, 766: 11, 1790: 12})


def pdf_char_bgr_medians(
    pixels: bytes,
    px_width: int,
    px_height: int,
    scale_x: float,
    scale_y: float,
    boxes: Sequence[tuple[float, float, float, float]],
    /,
) -> list[tuple[int, int, int]]:
    """Per-character BGR lower medians over the backdrop's sampled pixels under each box."""
    row_bytes = px_width * _BGR_BYTES
    results: list[tuple[int, int, int]] = []
    for x0, y0, x1, y1 in boxes:
        columns = _pixel_run(x0 * scale_x, x1 * scale_x, px_width)
        rows = _pixel_run(y0 * scale_y, y1 * scale_y, px_height)
        step = columns.step * _BGR_BYTES
        first, last = columns.start * _BGR_BYTES, columns[-1] * _BGR_BYTES
        channels: tuple[list[int], list[int], list[int]] = ([], [], [])
        for row in rows:
            base = row * row_bytes
            for channel, values in enumerate(channels):
                values.extend(pixels[base + first + channel : base + last + channel + 1 : step])
        blue = sorted(channels[0])[(len(channels[0]) - 1) // 2]
        green = sorted(channels[1])[(len(channels[1]) - 1) // 2]
        red = sorted(channels[2])[(len(channels[2]) - 1) // 2]
        results.append((blue, green, red))
    return results


def _pixel_run(low: float, high: float, size: int) -> range:
    first, last = max(0, math.ceil(low - 0.5)), min(size - 1, math.floor(high - 0.5))
    if first > last:
        nearest = min(size - 1, max(0, math.floor((low + high) / 2)))
        return range(nearest, nearest + 1)
    step = -(-(last - first + 1) // _MAX_SAMPLES_PER_AXIS)
    return range(first, last + 1, step)


def pdf_union_area(
    boxes: Sequence[tuple[float, float, float, float]],
    /,
) -> float:
    """Exact area of the union of non-empty axis-aligned boxes via x-sweep and y-cover tree."""
    if not boxes:
        return 0.0
    edges = sorted({y for box in boxes for y in (box[1], box[3])})
    if len(edges) < 2:
        return 0.0
    rank = {y: index for index, y in enumerate(edges)}
    opening = [(box[0], 1, rank[box[1]], rank[box[3]]) for box in boxes]
    closing = [(box[2], -1, rank[box[1]], rank[box[3]]) for box in boxes]
    events = sorted(opening + closing)
    tree = _CoverTree(edges)
    area, previous_x = 0.0, events[0][0]
    for x, delta, low, high in events:
        area += tree.covered() * (x - previous_x)
        tree.add(low, high, delta)
        previous_x = x
    return area


class _CoverTree:
    __slots__ = ("_count", "_covered", "_length", "_size")

    def __init__(self, edges: list[float]) -> None:
        gaps = len(edges) - 1
        size = 1 << max(0, (gaps - 1).bit_length())
        length = [0.0] * (2 * size)
        for index in range(gaps):
            length[size + index] = edges[index + 1] - edges[index]
        for node in range(size - 1, 0, -1):
            length[node] = length[2 * node] + length[2 * node + 1]
        self._size = size
        self._length = length
        self._count = [0] * (2 * size)
        self._covered = [0.0] * (2 * size)

    def covered(self) -> float:
        return self._covered[1]

    def add(self, low: int, high: int, delta: int) -> None:
        left, right = low + self._size, high + self._size
        first, last = left, right - 1
        while left < right:
            if left & 1:
                self._count[left] += delta
                self._pull(left)
                left += 1
            if right & 1:
                right -= 1
                self._count[right] += delta
                self._pull(right)
            left, right = left >> 1, right >> 1
        for node in (first, last):
            node >>= 1
            while node:
                self._pull(node)
                node >>= 1

    def _pull(self, node: int) -> None:
        if self._count[node] > 0:
            self._covered[node] = self._length[node]
        elif node >= self._size:
            self._covered[node] = 0.0
        else:
            self._covered[node] = self._covered[2 * node] + self._covered[2 * node + 1]


def pdf_char_reason_runs(
    char_boxes: Sequence[tuple[float, float, float, float]],
    width: float,
    height: float,
    no_paint: bool,
    under: Sequence[tuple[float, float, float, float]],
    /,
) -> list[tuple[int, int, bool, bool]]:
    """Run-length encoded ``(start, end, is_off_page, is_invisible)`` runs across ``char_boxes``."""
    if not char_boxes:
        return []
    tol = _OFF_PAGE_TOLERANCE_PT
    max_x = width + tol
    max_y = height + tol
    runs: list[tuple[int, int, bool, bool]] = []
    start = 0
    prev_off = False
    prev_inv = False
    for index, (x0, y0, x1, y1) in enumerate(char_boxes):
        is_off = not (x0 >= -tol and y0 >= -tol and x1 <= max_x and y1 <= max_y)
        if no_paint:
            cx, cy = (x0 + x1) / 2.0, (y0 + y1) / 2.0
            is_inv = not any(rx0 <= cx <= rx1 and ry0 <= cy <= ry1 for rx0, ry0, rx1, ry1 in under)
        else:
            is_inv = False
        if index == 0:
            prev_off, prev_inv = is_off, is_inv
        elif is_off != prev_off or is_inv != prev_inv:
            runs.append((start, index, prev_off, prev_inv))
            start = index
            prev_off, prev_inv = is_off, is_inv
    runs.append((start, len(char_boxes), prev_off, prev_inv))
    return runs


def pdf_snap_and_merge_segments(
    segments: Sequence[tuple[float, float, float]],
    snap_pt: float,
    /,
) -> list[tuple[float, float, float]]:
    """Snap position-clustered ``(position, start, end)`` segments to cluster mean and merge along axis."""
    if not segments:
        return []
    ordered = sorted(segments, key=lambda item: item[0])
    lines: list[tuple[float, float, float]] = []
    cluster: list[tuple[float, float, float]] = []
    for item in ordered:
        if cluster and item[0] - cluster[-1][0] > snap_pt:
            _emit_cluster(cluster, snap_pt, lines)
            cluster = []
        cluster.append(item)
    if cluster:
        _emit_cluster(cluster, snap_pt, lines)
    return lines


def _emit_cluster(
    cluster: list[tuple[float, float, float]],
    snap_pt: float,
    out: list[tuple[float, float, float]],
) -> None:
    total = 0.0
    for pos, _, _ in cluster:
        total += pos
    position = total / len(cluster)
    extents = sorted((start, end) for _, start, end in cluster)
    start, end = extents[0]
    for next_start, next_end in extents[1:]:
        if next_start - end <= snap_pt:
            end = max(end, next_end)
            continue
        out.append((position, start, end))
        start, end = next_start, next_end
    out.append((position, start, end))


def pdf_lzw_decode(
    data: bytes,
    early: int,
    max_output: int,
    /,
) -> list[bytes]:
    """Decode PDF LZW ``data`` in ``_CHUNK``-sized pieces, stopping once total output exceeds ``max_output``."""
    decoder = _LzwDecoder(early)
    chunks: list[bytes] = []
    emitted = 0
    for piece in decoder.feed(data):
        if piece:
            chunks.append(piece)
            emitted += len(piece)
            if emitted > max_output:
                return chunks
    for piece in decoder.finish():
        if piece:
            chunks.append(piece)
            emitted += len(piece)
            if emitted > max_output:
                return chunks
    return chunks


class _LzwDecoder:
    def __init__(self, early: int) -> None:
        self.done = False
        self._early = early
        self._width = 9
        self._prefix: list[int] = []
        self._char: list[int] = []
        self._text: list[bytes | None] = []
        self._old = -1
        self._last = 0
        self._bits = 0
        self._bit_count = 0
        self._out = bytearray()

    def feed(self, data: bytes) -> Iterator[bytes]:
        for byte in data:
            if self.done:
                return
            self._bits = (self._bits << 8) | byte
            self._bit_count += 8
            if self._bit_count >= self._width:
                self._bit_count -= self._width
                code = self._bits >> self._bit_count
                self._bits &= (1 << self._bit_count) - 1
                self._code(code)
            if len(self._out) >= _CHUNK:
                for _ in range(len(self._out) // _CHUNK):
                    piece = bytes(self._out[:_CHUNK])
                    del self._out[:_CHUNK]
                    yield piece

    def finish(self) -> Iterator[bytes]:
        for _ in range(len(self._out) // _CHUNK):
            piece = bytes(self._out[:_CHUNK])
            del self._out[:_CHUNK]
            yield piece
        if self._out:
            yield bytes(self._out)
            self._out.clear()

    def _code(self, code: int) -> None:
        if code < 256:
            self._out.append(code)
            self._last = code
            if self._old >= 0:
                self._add(self._old, code)
            self._old = code
            return
        if code == 256:
            self._width = 9
            self._prefix.clear()
            self._char.clear()
            self._text.clear()
            self._old = -1
            return
        if code == 257 or self._old < 0:
            self.done = True
            return
        if code - 258 >= len(self._prefix):
            text = self._string(self._old, _LZW_STACK - 1) + bytes([self._last])
        else:
            text = self._string(code, _LZW_STACK)
        self._out += text
        self._last = text[0]
        if self._old >= 258 and self._old - 258 >= len(self._prefix):
            self.done = True
            return
        self._add(self._old, self._last)
        self._old = code

    def _string(self, code: int, room: int) -> bytes:
        index = code - 258
        if index < 0 or index >= len(self._prefix):
            return bytes([code & 0xFF])
        fixed = self._text[index]
        if fixed is not None:
            return fixed[-room:]
        return self._walked(code, room)

    def _walked(self, code: int, room: int) -> bytes:
        pushed = bytearray()
        for _ in range(room):
            index = code - 258
            if index < 0 or index >= len(self._prefix):
                pushed.append(code & 0xFF)
                break
            pushed.append(self._char[index])
            code = self._prefix[index]
        pushed.reverse()
        return bytes(pushed)

    def _add(self, prefix: int, char: int) -> None:
        index = len(self._prefix)
        if index + self._early >= _LZW_TABLE:
            return
        if prefix < 256:
            text: bytes | None = bytes([prefix, char])
        else:
            before = prefix - 258
            known = self._text[before] if 0 <= before < index else None
            text = (known + bytes([char]))[-_LZW_STACK:] if known is not None else None
        self._prefix.append(prefix)
        self._char.append(char)
        self._text.append(text)
        self._width = _LZW_WIDTHS.get(index + 1 + self._early, self._width)
