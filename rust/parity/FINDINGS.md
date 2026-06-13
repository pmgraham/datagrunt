# Parity Findings: Rust port of csvcomponents

**Verdict:** Full behavioral parity achieved on all tested surfaces; two narrow, documented contract differences (below), neither reachable through datagrunt's own call paths.

## Coverage
- 397 differential parity tests: 36-file edge-case corpus x 11 exposed functions (probes, delimiter, row counting, ragged, dialect x2 variants, normalize).
- 36 cargo unit tests (incl. 8 chunk-boundary tests for the streaming decoder).
- Dialect sniffer: 2,326-sample differential fuzz vs CPython csv.Sniffer — 0 mismatches.
- Streaming decoder (DecodedReader): 4,200-assertion adversarial stress vs eager reference — 0 mismatches.
- Per-component adversarial probes during review: row counting (24 cases incl. unclosed quotes, NUL bytes, quoted comments), ragged boundaries (10k cap edge), delimiter (safe-vs-frequent precedence, unicode delimiters), dialect sample construction (blank-line discriminator, BOM+comments, 5-line cap).
- Existing datagrunt test suite: 524 passed — unaffected.

## Contract differences (documented, not parity bugs)
1. **Non-ASCII delimiters**: Python's csv module accepts any 1-char delimiter (e.g. '€'); the Rust bindings reject non-ASCII with ValueError (the csv crate is byte-oriented). datagrunt's own inference only ever produces ASCII delimiters, so this is unreachable via the library.
2. **Error site for unreadable files in dialect sniffing**: Python raises inside the is_blank probe (`FileProperties._validate_path` → `FileNotFoundError`); Rust's `is_blank` returns false on IO error and the same exception class surfaces one call later in sample reading (`sniff_sample`). Same exception type (`FileNotFoundError`) from the caller's perspective, different internal call site.

## CPython quirks faithfully reproduced (worth knowing for a full port)
- csv.Sniffer sniffs a SPACE delimiter (not ',') for `id,note\n1,"say ""hi"" now"\n2,"ok"\n` — the quote-adjacency regex sees the space before the quote.
- Sniffer never errors on `justoneword` — frequency analysis picks 'o' as the delimiter.
- Sniffer hardcodes lineterminator '\r\n' and QUOTE_MINIMAL; CSVDialect's QUOTING_MAP labels QUOTE_MINIMAL (0) as "no quoting" — mirrored exactly.
- Python's row-count has no try/except (a csv.Error would propagate) while the ragged check swallows everything — the Rust ports mirror each separately.

## Notes for a full port (effort-estimate evidence)
- The Sniffer port was the hardest component (~500 lines Rust vs ~180 Python): backreference regexes require fancy-regex; Python dict insertion-order semantics require Vec-based ordered maps; float-loop and negative-count edge cases matter.
- The rust csv crate matched Python csv.reader record semantics on every probe (flexible mode); no hand-rolled parser needed.
- Python's lazy text-mode IO had to be replicated with a custom streaming decoder (DecodedReader: utf-8-sig + errors=ignore + universal newlines as a Read adapter) to keep the benchmark comparison honest — eager whole-file reads made header-probes look pathologically slow on large files. This is the single biggest "hidden cost" a full port should budget for.
- PyO3 abi3 + maturin worked without friction; macOS needs `-undefined dynamic_lookup` link flags for plain cargo builds (rust/.cargo/config.toml, cwd-sensitive).
