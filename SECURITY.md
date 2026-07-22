# Security Policy

## Supported versions

Only the latest release line receives security fixes.

| Version | Supported |
|---------|-----------|
| 4.x (latest release) | ✅ |
| < 4.0 | ❌ |

## Reporting a vulnerability

Please report vulnerabilities privately via GitHub's **[private vulnerability reporting](https://github.com/pmgraham/datagrunt/security/advisories/new)** (Security tab → "Report a vulnerability"). Do **not** open a public issue for security problems.

You can expect an acknowledgment within a few days. Please include a minimal reproduction where possible. Fixes are developed privately and disclosed alongside a patched release.

## Scope notes

- The Rust extension (`datagrunt._native`) treats panics reachable from user input as bugs (a panic across the PyO3 boundary is a denial-of-service) — such reports are in scope.
- Datagrunt deliberately **preserves data rather than transforming it**: SQL passed to `query_data` and CSV/Excel formula-injection payloads (values starting with `=`, `+`, `-`, `@`) are documented application-layer concerns, not vulnerabilities in datagrunt itself.
