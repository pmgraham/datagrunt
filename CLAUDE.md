# CLAUDE.md — the datagrunt playbook

Project-specific guidance for Claude Code (in addition to the global `~/.claude/CLAUDE.md` clean-code standards). This file is the source of truth for **how we work on datagrunt**. When asked to do something "the usual way" / "per the usual" / via `/ship`, follow this.

datagrunt is a CSV/PDF data utility. CSV delimiter/dialect/row inference runs in a **required Rust extension** (`datagrunt._native`, PyO3/maturin) with a pure-Python mirror (`_compute_python.py`) as the parity oracle. PDF parsing is **pure Python** (pymupdf + pdfium + pdfplumber + tesseract OCR).

---

## First principles (non-negotiable)

- **Python-first, then port to Rust.** For CSV compute, change/add logic in `src/datagrunt/core/csv_io/_compute_python.py` first, validate, *then* mirror it in the Rust crate and prove agreement with the differential-parity suite. Python leads; Rust follows. If it's inefficient in Python it's inefficient in Rust — fix Python, then fix Rust. `_compute_python` must keep mirroring the `_native` function API (it is both the toggle target and the parity oracle).
- **Rust core is the required default (4.0+).** `_compute.backend()` returns `_native` by default; `_compute_python` is the parity oracle + a hidden diagnostics toggle (`DATAGRUNT_DISABLE_RUST` / `rust_disabled()`), **not** an automatic runtime fallback. `_compute.py` hard-imports `_native`. Do **not** add a silent pure-Python fallback.
- **Automatic resource lifecycle — never require the user to call `.close()`.** Reuse by default (cache the engine per public object); cleanup is automatic via reference-counted release on scope exit. `close()` / `with` are optional power-user tools for *early* release only; the default path must be correct and fast without them.
- **DRY + single-responsibility, within each domain — never across.** Deduplicate inside the CSV domain and inside the PDF domain; do **not** create a shared CSV/PDF base (they are separate subsystems). Don't over-abstract.
- **Preserve, not transform.** datagrunt never silently sanitizes/mutates data for hypothetical risks. SQL via `query_data` and CSV/Excel formula-injection (values starting with `=,+,-,@`) are **documented app-layer concerns** — document and defer; do not "fix" by mutating data.
- **PDF extraction must be 100% complete.** Never propose selective or stage-skipping extraction; preserve per-page error isolation (a bad page is recorded and skipped, never drops a batch).

---

## The workflow ("the usual")

For any substantive change, follow this end-to-end. (Trivial 1–2 line fixes may skip the formal plan doc but still get tests, a PR, and CI.)

1. **Track it as a labeled GitHub issue, up front.** Every enhancement/optimization/bug gets a `gh issue create --label <…>` *before* implementation. Labels: `performance`, `code-quality`, `bug`, `documentation`, `security`, `enhancement`. Apply multiple when it spans categories. If expanded scope is discovered mid-work, log it as its own issue and defer it — don't balloon the current PR.
2. **Branch as `<type>/<kebab-case>`** — `feature/…`, `refactor/…`, `perf/…`, `fix/…`, `docs/…`. Never date-prefixed snake_case. Never commit straight to `main`.
3. **Plan** with the `superpowers:writing-plans` skill (save under the gitignored `docs/superpowers/plans/`).
4. **Implement via `superpowers:subagent-driven-development`:** a fresh implementer subagent per task (TDD — failing test first), then a per-task spec+quality review; fix Critical/Important findings before moving on.
5. **Final whole-branch review** (most capable model) for non-trivial changes; address findings.
6. **Open a PR** with a clear body and `Closes #<issue>`. Merge (squash, delete branch) **only after CI is green**, then sync `main`.
7. **Release/publish needs explicit consent.** Version bumps and real-PyPI publishes require the maintainer's express approval; CI/TestPyPI testing without a version change is fine.

### Test gates — all must pass before merge
- `uv run pytest tests/ -q` — Rust backend (default)
- `DATAGRUNT_DISABLE_RUST=1 uv run pytest tests/ -q` — Python backend
- When Rust changed: `cd rust && cargo test`, plus the differential parity suite `uv run pytest tests/parity/ -q` (Rust == Python over the corpus)
- `ruff check src/datagrunt/core/csv_io` (CI gates on this; check the broader tree too)
- `ruff format --check .` (CI gates on this, blocking)
- `mypy` (CI gates on this, blocking; scoped to the compute-backend contract — see #314)
- After Rust changes, **rebuild the extension** so tests see it: `uv pip install -e ".[dev,pdf]"` (or `maturin develop`)

### Hygiene
- Use **`uv`** for all Python package/env work (never plain `pip`/`python`); `maturin`/`cargo` for Rust.
- **Keep PRs clean:** no dev/test/benchmark/scratch cruft in the repo or diff. Dev scripts go in the gitignored `scripts/`. Never commit non-documentation markdown (code-review/validation/scratch notes).
- Commit messages end with a `Co-Authored-By:` trailer naming **the model that actually did the work** — e.g. `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`. Don't copy the version forward from an older commit: this line read `Opus 4.8` well after that stopped being true, so the trailer and the playbook disagreed.
- PR bodies end with: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`

---

## Architecture map (orient fast)

- **CSV compute:** `core/csv_io/_compute.py` dispatches `backend()` → `_native` (Rust) or `_compute_python` (oracle/toggle). Both expose the same function set (`infer_delimiter`, `sniff_dialect`, `probe_csv_header`, `leading_rows`, `first_row`, `count_leading_comments`, `row_count_with_header`, `check_ragged`, `normalize_columns`, …).
- **CSV API:** `CSVReader`/`CSVWriter` share `csv_api/_engine_backed.py::_CSVEngineBacked` (cached `_engine` + optional `close()`/context-manager). Engines (`core/csv_io/engines.py`) share `_DuckDBBackedEngine`; DuckDB access via `core/databases/databases.py::DuckDBQueries`.
- **PDF (pure Python):** `PDFReader`/`PDFWriter` share `pdf_api/_engine_backed.py::_PDFEngineBacked` (cached `_engine`). `core/pdf_io/pdfcomponents.py::DocumentAssembler.parse_page` does single-pass per-page extraction; backends under `core/pdf_io/extraction/` (pymupdf, pdfium, OCR, tables, layout) expose `extract_page`, which `parse_page` calls. PDFium parallelism uses a process pool (per-page batching was investigated and declined — see issue #216).

---

## Rust crate (`rust/`)
- `datagrunt-core` = pure-Rust logic (delimiter, dialect, io, normalize, ragged, rows); `datagrunt-python` = the PyO3 `_native` bindings. Unit tests are in-file `#[cfg(test)]` modules.
- Mirror the existing line-iteration helpers (`io::universal_lines`, `DecodedReader`) for parity; avoid `unwrap()/expect()/panic!` reachable from user input (a panic across the PyO3 boundary is a DoS). Run `cargo clippy` on changes.
