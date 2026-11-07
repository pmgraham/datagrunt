//! Pure-Rust PDF compute kernels for Stage 1 extraction, quality assessment, and layout.
//!
//! Mirrors `src/datagrunt/pdf/_compute_python.py` bit-for-bit. No PyO3 here.

const BGR_BYTES: usize = 3;
const MAX_SAMPLES_PER_AXIS: usize = 32;
const OFF_PAGE_TOLERANCE_PT: f64 = 0.5;
const CHUNK: usize = 1 << 16;
const LZW_STACK: usize = 4000;
const LZW_TABLE: usize = 4094;

pub type Box4 = (f64, f64, f64, f64);
pub type Segment3 = (f64, f64, f64);

fn pixel_run(low: f64, high: f64, size: usize) -> (usize, usize, usize) {
    let size_i = size as isize;
    let first_i = ((low - 0.5).ceil() as isize).max(0);
    let last_i = ((high - 0.5).floor() as isize).min(size_i - 1);
    if first_i > last_i {
        let nearest = (((low + high) / 2.0).floor() as isize).clamp(0, size_i - 1) as usize;
        return (nearest, nearest, 1);
    }
    let first = first_i as usize;
    let last = last_i as usize;
    let count = last - first + 1;
    let step = count.div_ceil(MAX_SAMPLES_PER_AXIS);
    (first, last, step)
}

/// Per-character BGR lower medians over the backdrop's sampled pixels under each box.
pub fn pdf_char_bgr_medians(
    pixels: &[u8],
    px_width: usize,
    px_height: usize,
    scale_x: f64,
    scale_y: f64,
    boxes: &[Box4],
) -> Vec<(u8, u8, u8)> {
    if px_width == 0 || px_height == 0 {
        return vec![(0, 0, 0); boxes.len()];
    }
    let row_bytes = px_width.saturating_mul(BGR_BYTES);
    let mut results = Vec::with_capacity(boxes.len());
    let mut b_buf = [0u8; MAX_SAMPLES_PER_AXIS * MAX_SAMPLES_PER_AXIS];
    let mut g_buf = [0u8; MAX_SAMPLES_PER_AXIS * MAX_SAMPLES_PER_AXIS];
    let mut r_buf = [0u8; MAX_SAMPLES_PER_AXIS * MAX_SAMPLES_PER_AXIS];

    for &(x0, y0, x1, y1) in boxes {
        let (col_first, col_last, col_step) = pixel_run(x0 * scale_x, x1 * scale_x, px_width);
        let (row_first, row_last, row_step) = pixel_run(y0 * scale_y, y1 * scale_y, px_height);
        let mut n = 0usize;
        let mut row = row_first;
        while row <= row_last {
            let base = row.saturating_mul(row_bytes);
            let mut col = col_first;
            while col <= col_last {
                let idx = base.saturating_add(col.saturating_mul(BGR_BYTES));
                if let (Some(px), Some(b_slot), Some(g_slot), Some(r_slot)) = (
                    pixels.get(idx..idx + 3),
                    b_buf.get_mut(n),
                    g_buf.get_mut(n),
                    r_buf.get_mut(n),
                ) {
                    *b_slot = px[0];
                    *g_slot = px[1];
                    *r_slot = px[2];
                    n += 1;
                }
                col = col.saturating_add(col_step);
            }
            row = row.saturating_add(row_step);
        }
        if n == 0 {
            results.push((0, 0, 0));
            continue;
        }
        let mid = (n - 1) / 2;
        let (_, b_med, _) = b_buf[..n].select_nth_unstable(mid);
        let (_, g_med, _) = g_buf[..n].select_nth_unstable(mid);
        let (_, r_med, _) = r_buf[..n].select_nth_unstable(mid);
        results.push((*b_med, *g_med, *r_med));
    }
    results
}

struct CoverTree {
    size: usize,
    length: Vec<f64>,
    count: Vec<i32>,
    covered: Vec<f64>,
}

impl CoverTree {
    fn new(edges: &[f64]) -> Self {
        let gaps = edges.len().saturating_sub(1);
        let v = gaps.saturating_sub(1);
        let bits = (usize::BITS - v.leading_zeros()) as usize;
        let size = 1usize << bits;
        let mut length = vec![0.0; 2 * size];
        for index in 0..gaps {
            if let (Some(&e0), Some(&e1), Some(slot)) = (
                edges.get(index),
                edges.get(index + 1),
                length.get_mut(size + index),
            ) {
                *slot = e1 - e0;
            }
        }
        for node in (1..size).rev() {
            let left_len = length.get(2 * node).copied().unwrap_or(0.0);
            let right_len = length.get(2 * node + 1).copied().unwrap_or(0.0);
            if let Some(slot) = length.get_mut(node) {
                *slot = left_len + right_len;
            }
        }
        Self {
            size,
            length,
            count: vec![0; 2 * size],
            covered: vec![0.0; 2 * size],
        }
    }

    fn covered(&self) -> f64 {
        self.covered.get(1).copied().unwrap_or(0.0)
    }

    fn add(&mut self, low: usize, high: usize, delta: i32) {
        if low >= high {
            return;
        }
        let mut left = low + self.size;
        let mut right = high + self.size;
        let first = left;
        let last = right - 1;
        while left < right {
            if (left & 1) == 1 {
                if let Some(c) = self.count.get_mut(left) {
                    *c += delta;
                }
                self.pull(left);
                left += 1;
            }
            if (right & 1) == 1 {
                right -= 1;
                if let Some(c) = self.count.get_mut(right) {
                    *c += delta;
                }
                self.pull(right);
            }
            left >>= 1;
            right >>= 1;
        }
        for start_node in [first, last] {
            let mut node = start_node >> 1;
            while node > 0 {
                self.pull(node);
                node >>= 1;
            }
        }
    }

    fn pull(&mut self, node: usize) {
        let cnt = self.count.get(node).copied().unwrap_or(0);
        let val = if cnt > 0 {
            self.length.get(node).copied().unwrap_or(0.0)
        } else if node >= self.size {
            0.0
        } else {
            let l = self.covered.get(2 * node).copied().unwrap_or(0.0);
            let r = self.covered.get(2 * node + 1).copied().unwrap_or(0.0);
            l + r
        };
        if let Some(slot) = self.covered.get_mut(node) {
            *slot = val;
        }
    }
}

/// Exact area of the union of non-empty axis-aligned boxes via x-sweep and y-cover tree.
pub fn pdf_union_area(boxes: &[Box4]) -> f64 {
    if boxes.is_empty() {
        return 0.0;
    }
    let mut edges = Vec::with_capacity(boxes.len() * 2);
    for &(_, y0, _, y1) in boxes {
        edges.push(y0);
        edges.push(y1);
    }
    edges.sort_by(|a, b| a.total_cmp(b));
    edges.dedup_by(|a, b| *a == *b);
    if edges.len() < 2 {
        return 0.0;
    }
    let mut events = Vec::with_capacity(boxes.len() * 2);
    for &(x0, y0, _x1, y1) in boxes {
        let low = edges.binary_search_by(|y| y.total_cmp(&y0)).unwrap_or(0);
        let high = edges.binary_search_by(|y| y.total_cmp(&y1)).unwrap_or(0);
        events.push((x0, 1i32, low, high));
    }
    for &(x0, y0, x1, y1) in boxes {
        let _ = x0;
        let low = edges.binary_search_by(|y| y.total_cmp(&y0)).unwrap_or(0);
        let high = edges.binary_search_by(|y| y.total_cmp(&y1)).unwrap_or(0);
        events.push((x1, -1i32, low, high));
    }
    events.sort_by(|a, b| {
        a.0.total_cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.3.cmp(&b.3))
    });
    let mut tree = CoverTree::new(&edges);
    let mut area = 0.0;
    let mut previous_x = events.first().map(|e| e.0).unwrap_or(0.0);
    for (x, delta, low, high) in events {
        area += tree.covered() * (x - previous_x);
        tree.add(low, high, delta);
        previous_x = x;
    }
    area
}

/// Run-length encoded `(start, end, is_off_page, is_invisible)` runs across `char_boxes`.
pub fn pdf_char_reason_runs(
    char_boxes: &[Box4],
    width: f64,
    height: f64,
    no_paint: bool,
    under: &[Box4],
) -> Vec<(usize, usize, bool, bool)> {
    if char_boxes.is_empty() {
        return Vec::new();
    }
    let tol = OFF_PAGE_TOLERANCE_PT;
    let max_x = width + tol;
    let max_y = height + tol;
    let mut runs = Vec::new();
    let mut start = 0usize;
    let mut prev_off = false;
    let mut prev_inv = false;

    for (index, &(x0, y0, x1, y1)) in char_boxes.iter().enumerate() {
        let is_off = !(x0 >= -tol && y0 >= -tol && x1 <= max_x && y1 <= max_y);
        let is_inv = if no_paint {
            let cx = (x0 + x1) / 2.0;
            let cy = (y0 + y1) / 2.0;
            !under
                .iter()
                .any(|&(rx0, ry0, rx1, ry1)| rx0 <= cx && cx <= rx1 && ry0 <= cy && cy <= ry1)
        } else {
            false
        };
        if index == 0 {
            prev_off = is_off;
            prev_inv = is_inv;
        } else if is_off != prev_off || is_inv != prev_inv {
            runs.push((start, index, prev_off, prev_inv));
            start = index;
            prev_off = is_off;
            prev_inv = is_inv;
        }
    }
    runs.push((start, char_boxes.len(), prev_off, prev_inv));
    runs
}

/// Snap position-clustered `(position, start, end)` segments to cluster mean and merge along axis.
pub fn pdf_snap_and_merge_segments(segments: &[Segment3], snap_pt: f64) -> Vec<Segment3> {
    if segments.is_empty() {
        return Vec::new();
    }
    let mut ordered = segments.to_vec();
    ordered.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut lines = Vec::new();
    let mut cluster: Vec<Segment3> = Vec::new();
    for item in ordered {
        if let Some(last) = cluster.last() {
            if item.0 - last.0 > snap_pt {
                emit_cluster(&cluster, snap_pt, &mut lines);
                cluster.clear();
            }
        }
        cluster.push(item);
    }
    if !cluster.is_empty() {
        emit_cluster(&cluster, snap_pt, &mut lines);
    }
    lines
}

fn emit_cluster(cluster: &[Segment3], snap_pt: f64, out: &mut Vec<Segment3>) {
    if cluster.is_empty() {
        return;
    }
    let mut total = 0.0;
    for &(pos, _, _) in cluster {
        total += pos;
    }
    let position = total / (cluster.len() as f64);
    let mut extents: Vec<(f64, f64)> = cluster.iter().map(|&(_, s, e)| (s, e)).collect();
    extents.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));
    let Some(&(first_start, first_end)) = extents.first() else {
        return;
    };
    let mut start = first_start;
    let mut end = first_end;
    for &(next_start, next_end) in extents.iter().skip(1) {
        if next_start - end <= snap_pt {
            if next_end > end {
                end = next_end;
            }
            continue;
        }
        out.push((position, start, end));
        start = next_start;
        end = next_end;
    }
    out.push((position, start, end));
}

/// Decode PDF LZW `data` in 64 KiB pieces, stopping once total output exceeds `max_output`.
pub fn pdf_lzw_decode(data: &[u8], early: i32, max_output: usize) -> Vec<Vec<u8>> {
    let mut decoder = LzwDecoder::new(early);
    let mut chunks = Vec::new();
    let mut emitted = 0usize;

    for &byte in data {
        if decoder.done {
            break;
        }
        decoder.feed_byte(byte);
        while decoder.out.len() >= CHUNK {
            let piece: Vec<u8> = decoder.out.drain(..CHUNK).collect();
            emitted = emitted.saturating_add(piece.len());
            chunks.push(piece);
            if emitted > max_output {
                return chunks;
            }
        }
    }
    while decoder.out.len() >= CHUNK {
        let piece: Vec<u8> = decoder.out.drain(..CHUNK).collect();
        emitted = emitted.saturating_add(piece.len());
        chunks.push(piece);
        if emitted > max_output {
            return chunks;
        }
    }
    if !decoder.out.is_empty() {
        let piece = std::mem::take(&mut decoder.out);
        chunks.push(piece);
    }
    chunks
}

struct LzwDecoder {
    done: bool,
    early: usize,
    width: u32,
    prefix: Vec<u16>,
    char_table: Vec<u8>,
    text: Vec<Option<Vec<u8>>>,
    old: i32,
    last: u8,
    bits: u32,
    bit_count: u32,
    out: Vec<u8>,
}

impl LzwDecoder {
    fn new(early: i32) -> Self {
        Self {
            done: false,
            early: early.max(0) as usize,
            width: 9,
            prefix: Vec::new(),
            char_table: Vec::new(),
            text: Vec::new(),
            old: -1,
            last: 0,
            bits: 0,
            bit_count: 0,
            out: Vec::new(),
        }
    }

    fn feed_byte(&mut self, byte: u8) {
        self.bits = (self.bits << 8) | (byte as u32);
        self.bit_count += 8;
        if self.bit_count >= self.width {
            self.bit_count -= self.width;
            let code = (self.bits >> self.bit_count) as usize;
            self.bits &= (1u32 << self.bit_count) - 1;
            self.decode_code(code);
        }
    }

    fn decode_code(&mut self, code: usize) {
        if code < 256 {
            let b = code as u8;
            self.out.push(b);
            self.last = b;
            if self.old >= 0 {
                self.add(self.old as usize, b);
            }
            self.old = code as i32;
            return;
        }
        if code == 256 {
            self.width = 9;
            self.prefix.clear();
            self.char_table.clear();
            self.text.clear();
            self.old = -1;
            return;
        }
        if code == 257 || self.old < 0 {
            self.done = true;
            return;
        }
        let old_code = self.old as usize;
        let decoded = if code.saturating_sub(258) >= self.prefix.len() {
            let mut s = self.decode_string(old_code, LZW_STACK - 1);
            s.push(self.last);
            s
        } else {
            self.decode_string(code, LZW_STACK)
        };
        if let Some(&first_byte) = decoded.first() {
            self.last = first_byte;
        }
        self.out.extend_from_slice(&decoded);
        if old_code >= 258 && (old_code - 258) >= self.prefix.len() {
            self.done = true;
            return;
        }
        self.add(old_code, self.last);
        self.old = code as i32;
    }

    fn decode_string(&self, code: usize, room: usize) -> Vec<u8> {
        if code < 258 {
            return vec![(code & 0xFF) as u8];
        }
        let index = code - 258;
        if index >= self.prefix.len() {
            return vec![(code & 0xFF) as u8];
        }
        if let Some(Some(fixed)) = self.text.get(index) {
            let start = fixed.len().saturating_sub(room);
            return fixed[start..].to_vec();
        }
        self.walked(code, room)
    }

    fn walked(&self, mut code: usize, room: usize) -> Vec<u8> {
        let mut pushed = Vec::with_capacity(room.min(64));
        for _ in 0..room {
            if code < 258 {
                pushed.push((code & 0xFF) as u8);
                break;
            }
            let index = code - 258;
            if index >= self.prefix.len() {
                pushed.push((code & 0xFF) as u8);
                break;
            }
            if let (Some(&ch), Some(&pref)) = (self.char_table.get(index), self.prefix.get(index)) {
                pushed.push(ch);
                code = pref as usize;
            } else {
                break;
            }
        }
        pushed.reverse();
        pushed
    }

    fn add(&mut self, prefix: usize, ch: u8) {
        let index = self.prefix.len();
        if index + self.early >= LZW_TABLE {
            return;
        }
        let entry_text = if prefix < 256 {
            Some(vec![prefix as u8, ch])
        } else {
            let before = prefix.saturating_sub(258);
            if prefix >= 258 && before < index {
                if let Some(Some(known)) = self.text.get(before) {
                    let mut combined = Vec::with_capacity(known.len().min(LZW_STACK) + 1);
                    let start = (known.len() + 1).saturating_sub(LZW_STACK);
                    if start == 0 {
                        combined.extend_from_slice(known);
                    } else if let Some(tail) = known.get(start - 1..) {
                        combined.extend_from_slice(tail);
                    }
                    combined.push(ch);
                    Some(combined)
                } else {
                    None
                }
            } else {
                None
            }
        };
        self.prefix.push(prefix as u16);
        self.char_table.push(ch);
        self.text.push(entry_text);
        let next_size = index + 1 + self.early;
        self.width = match next_size {
            254 => 10,
            766 => 11,
            1790 => 12,
            _ => self.width,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bgr_medians_white_and_black() {
        let pixels = vec![255u8; 10 * 10 * 3];
        let boxes = [(1.0, 1.0, 5.0, 5.0)];
        assert_eq!(
            pdf_char_bgr_medians(&pixels, 10, 10, 1.0, 1.0, &boxes),
            vec![(255, 255, 255)]
        );
    }

    #[test]
    fn test_union_area_overlapping_boxes() {
        let boxes = [(0.0, 0.0, 10.0, 10.0), (5.0, 0.0, 15.0, 10.0)];
        assert_eq!(pdf_union_area(&boxes), 150.0);
    }

    #[test]
    fn test_char_reason_runs_groups_runs() {
        let boxes = [
            (-5.0, 10.0, 2.0, 20.0),
            (10.0, 10.0, 20.0, 20.0),
            (20.0, 10.0, 30.0, 20.0),
        ];
        let runs = pdf_char_reason_runs(&boxes, 100.0, 100.0, false, &[]);
        assert_eq!(runs, vec![(0, 1, true, false), (1, 3, false, false)]);
    }

    #[test]
    fn test_snap_and_merge_segments() {
        let segs = [(10.0, 0.0, 50.0), (11.0, 49.0, 100.0), (50.0, 0.0, 20.0)];
        let merged = pdf_snap_and_merge_segments(&segs, 2.0);
        assert_eq!(merged, vec![(10.5, 0.0, 100.0), (50.0, 0.0, 20.0)]);
    }
}
