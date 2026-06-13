# Rust vs Python: csvcomponents benchmark

Date: 2026-06-12. Warm cache, same process, fresh objects per call.
Python row_count/dialect include their internal delimiter inference;
Rust pairings replicate that for a like-for-like comparison.

## Scale: small (12,000 rows, 1.1 MB, median of 15)

| Component | Python | Rust | Speedup |
|---|---|---|---|
| infer_delimiter | 0.057 ms | 0.052 ms | 1.1x |
| first_row | 0.021 ms | 0.008 ms | 2.5x |
| leading_rows(5) | 0.021 ms | 0.009 ms | 2.4x |
| count_leading_comments | 0.019 ms | 0.008 ms | 2.3x |
| sniff_dialect | 0.167 ms | 0.116 ms | 1.4x |
| row_count_with_header | 4.785 ms | 1.223 ms | 3.9x |
| check_ragged | 4.025 ms | 0.946 ms | 4.3x |
| normalize_columns(5000) | 3.748 ms | 0.741 ms | 5.1x |

## Scale: medium (1,200,000 rows, 109.9 MB, median of 5)

| Component | Python | Rust | Speedup |
|---|---|---|---|
| infer_delimiter | 0.043 ms | 0.011 ms | 3.8x |
| first_row | 0.022 ms | 0.009 ms | 2.6x |
| leading_rows(5) | 0.022 ms | 0.009 ms | 2.5x |
| count_leading_comments | 0.019 ms | 0.008 ms | 2.3x |
| sniff_dialect | 0.147 ms | 0.072 ms | 2.0x |
| row_count_with_header | 498.708 ms | 118.205 ms | 4.2x |
| check_ragged | 4.031 ms | 0.962 ms | 4.2x |
| normalize_columns(5000) | 3.745 ms | 0.731 ms | 5.1x |

## Scale: large (24,000,000 rows, 2,265.3 MB, median of 3)

| Component | Python | Rust | Speedup |
|---|---|---|---|
| infer_delimiter | 0.045 ms | 0.011 ms | 4.0x |
| first_row | 0.023 ms | 0.009 ms | 2.5x |
| leading_rows(5) | 0.022 ms | 0.009 ms | 2.4x |
| count_leading_comments | 0.019 ms | 0.009 ms | 2.3x |
| sniff_dialect | 0.176 ms | 0.102 ms | 1.7x |
| row_count_with_header | 10.288 s | 2.366 s | 4.3x |
| check_ragged | 4.148 ms | 0.969 ms | 4.3x |
| normalize_columns(5000) | 3.692 ms | 0.722 ms | 5.1x |

## Conclusion

**Rust wins or ties every component at every scale; nothing is slower.**
Hardware: Apple Silicon (aarch64), local SSD, warm cache.

**Where Rust pays for itself:**

- **`row_count_with_header`** — the only true full-file scan — is the headline:
  3.9x / 4.2x / 4.3x at 1 MB / 100 MB / 2 GB. At 2 GB that is 10.3 s -> 2.4 s
  per call, and the speedup *grows* with file size. This is the component
  that justifies integration.
- **`check_ragged`** (bounded 10k-row scan): consistent ~4.3x (4 ms -> 1 ms),
  flat across scales because both sides stop early.
- **`normalize_columns`** (pure CPU, 5,000 headers): consistent 5.1x.

**Where Rust does not matter:**

- The header-only probes (`first_row`, `leading_rows`, `count_leading_comments`,
  `infer_delimiter`, `sniff_dialect`) run in 8-120 microseconds in BOTH
  languages at every scale. Rust is 1.1-4x faster, but the absolute saving is
  tens of microseconds per call — immaterial for any real workload. These
  numbers are honest only because the Rust port streams lazily (an earlier
  eager-read draft made Rust look 150x *slower* on sniffing at 100 MB; see
  FINDINGS.md).
- The small-scale `infer_delimiter` tie (1.1x) is the `is_blank` probe
  strictly reading the whole sub-10 MB file on both sides — a parity
  behavior, not overhead.
- PyO3 call overhead measured ~2-5 microseconds per call: visible only in the
  microsecond-scale probes, irrelevant everywhere else.

**Memory:** the streaming decoder keeps Rust at O(64 KiB) resident for every
component, matching Python's lazy file iteration. No multi-GB allocations at
the 2 GB scale.

**Recommendation:** integrate the full-file/bounded scan paths
(`row_count_with_header`, `check_ragged`) and `normalize_columns` as an
optional Rust acceleration behind a Python fallback. Do NOT port the
header-only probes for performance reasons — port them only if a full-port
decision is made for architectural reasons (they cost little to maintain and
the parity suite already proves them). The effort evidence for a full-port
decision is in `rust/parity/FINDINGS.md`.
