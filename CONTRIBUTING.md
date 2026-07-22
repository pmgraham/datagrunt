# Contributing to Datagrunt

Thanks for your interest in contributing! Datagrunt is a CSV/Excel/Parquet/PDF data utility with a Rust-accelerated CSV core. This guide covers the local setup, the test gates every change must pass, and the conventions PRs are expected to follow.

## Development setup

Datagrunt uses [uv](https://docs.astral.sh/uv/) for all Python package and environment management — never plain `pip`/`python` — and `maturin`/`cargo` for the Rust extension.

```bash
git clone https://github.com/pmgraham/datagrunt.git
cd datagrunt
uv venv
uv pip install -e ".[dev,pdf]"   # builds the Rust extension via maturin
```

Requirements: Python >= 3.10, a Rust toolchain (stable), and — for the PDF OCR tests — [Tesseract](https://github.com/tesseract-ocr/tesseract) (`brew install tesseract` / `apt-get install tesseract-ocr`).

After changing Rust code, rebuild the extension so tests see it:

```bash
uv pip install -e ".[dev,pdf]"
```

## Architecture in one paragraph

CSV delimiter/dialect/row inference runs in a required Rust extension (`datagrunt._native`, PyO3/maturin) with a pure-Python mirror (`src/datagrunt/core/csv_io/_compute_python.py`) serving as the differential-parity oracle. **Python leads, Rust follows**: change compute logic in the Python mirror first, validate it, then port to Rust and prove agreement via the parity suite. The pure-Python path is a diagnostics toggle (`DATAGRUNT_DISABLE_RUST=1`), not an automatic fallback. PDF parsing is pure Python (PDFium by default, PyMuPDF as an alternative, pdfplumber tables, Tesseract OCR).

## Test gates — all must pass before merge

```bash
uv run pytest tests/ -q                            # Rust backend (default)
DATAGRUNT_DISABLE_RUST=1 uv run pytest tests/ -q   # Python backend
ruff check src tests                               # lint (CI-blocking)
ruff format --check .                              # format (CI-blocking)
```

When Rust changed, additionally:

```bash
cd rust && cargo test && cargo clippy --all-targets -- -D warnings
uv run pytest tests/parity/ -q                     # Rust == Python over the corpus
```

## Conventions

- **Branches**: `<type>/<kebab-case>` — `feature/…`, `refactor/…`, `perf/…`, `fix/…`, `docs/…`. Never commit directly to `main` (branch protection enforces PRs with green CI; the maintainer's direct-push bypass is reserved for documentation-only changes).
- **Issues first**: substantive enhancements/optimizations/bugs get a labeled GitHub issue before implementation; link the PR with `Closes #<issue>`.
- **Merges**: squash only; head branches auto-delete.
- **Keep PRs clean**: no dev/test/benchmark/scratch files in the diff; no non-documentation markdown (review notes, validation scratch).
- **Preserve, not transform**: datagrunt never silently sanitizes or mutates user data for hypothetical risks (SQL via `query_data`, CSV formula-injection characters, etc. are documented app-layer concerns).
- **PDF extraction is 100% complete**: never introduce selective or stage-skipping extraction; per-page error isolation must be preserved.
- **Resource lifecycle is automatic**: never require callers to invoke `.close()`; `close()`/context managers remain optional power-user tools for early release.

## Releases

Versioning and publishing are automated (bumpver + trusted publishing) and gated on maintainer approval. Release notes live on [GitHub Releases](https://github.com/pmgraham/datagrunt/releases) and the [datagrunt.io release-notes blog](https://www.datagrunt.io/blog/release-notes/). Contributors never need to touch versions or tags.

## Security issues

Please do not open public issues for vulnerabilities — see [SECURITY.md](SECURITY.md).
