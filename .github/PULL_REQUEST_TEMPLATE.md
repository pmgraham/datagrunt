## Summary

<!-- What does this change and why? -->

Closes #

## Test gates

- [ ] `uv run pytest tests/ -q` (Rust backend)
- [ ] `DATAGRUNT_DISABLE_RUST=1 uv run pytest tests/ -q` (Python backend)
- [ ] `ruff check src tests` and `ruff format --check .`
- [ ] Rust changed: `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `uv run pytest tests/parity/ -q`

## Checklist

- [ ] Branch named `<type>/<kebab-case>` (feature/refactor/perf/fix/docs)
- [ ] Linked to a labeled issue (`Closes #N`)
- [ ] No dev/test/benchmark/scratch files or non-documentation markdown in the diff
- [ ] Python-first: CSV compute changes land in `_compute_python.py` first, then the Rust mirror, with parity proven
