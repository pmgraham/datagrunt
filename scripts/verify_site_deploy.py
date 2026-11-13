#!/usr/bin/env python3
"""Verify that the current Datagrunt release blog post is live on datagrunt.io and heal dropped webhooks."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Final
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

SEMVER_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
ALLOWED_HTTPS_HOSTS: Final[frozenset[str]] = frozenset({"www.datagrunt.io", "datagrunt.io"})
LOOPBACK_HTTP_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "localhost"})
DEFAULT_BASE_URL: Final[str] = "https://www.datagrunt.io"
MAX_HTTP_BYTES: Final[int] = 524_288
HTTP_TIMEOUT_SECONDS: Final[float] = 10.0
GIT_TIMEOUT_SECONDS: Final[float] = 30.0
MAX_POLLS_CAP: Final[int] = 30
MAX_RETRIGGERS_CAP: Final[int] = 3
MAX_BLOG_FILES: Final[int] = 10_000


def _validate_version(version: str) -> str:
    if not isinstance(version, str) or not version.strip():
        raise ValueError("version must be a non-empty string")
    cleaned = version.strip()
    if SEMVER_RE.fullmatch(cleaned) is None:
        raise ValueError(f"Invalid semver version: {version!r}")
    return cleaned


def _validate_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty URL string")
    normalized = base_url.strip().rstrip("/")
    parsed = urlparse(normalized)
    host = (parsed.hostname or "").lower()
    if parsed.scheme == "https" and host in ALLOWED_HTTPS_HOSTS:
        return normalized
    if parsed.scheme == "http" and host in LOOPBACK_HTTP_HOSTS:
        return normalized
    raise ValueError(f"Disallowed base_url (must be https on datagrunt.io or loopback http): {base_url!r}")


def read_current_version(pyproject_path: Path) -> str:
    """Read and validate current_version / project.version from pyproject.toml."""
    if not isinstance(pyproject_path, Path) or not pyproject_path.is_file():
        raise FileNotFoundError(f"pyproject.toml not found: {pyproject_path}")
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    raw = data.get("tool", {}).get("bumpver", {}).get("current_version") or data.get("project", {}).get("version")
    if not isinstance(raw, str):
        raise ValueError(f"Missing version string in {pyproject_path}")
    return _validate_version(raw)


def resolve_post_url(site_dir: Path, version: str, base_url: str = DEFAULT_BASE_URL) -> str:
    """Locate the Hugo Markdown post for `version` and return its canonical live URL."""
    checked_version = _validate_version(version)
    checked_base = _validate_base_url(base_url)
    if not isinstance(site_dir, Path) or not site_dir.is_dir():
        raise ValueError(f"site_dir must be an existing directory: {site_dir!r}")

    content_dir = (site_dir / "content").resolve()
    blog_dir = (content_dir / "blog").resolve()
    if not blog_dir.is_dir():
        raise FileNotFoundError(f"Missing Hugo blog directory: {blog_dir}")

    slug_prefix = f"datagrunt-{checked_version.replace('.', '-')}-"
    matches: list[Path] = []
    scanned = 0
    for candidate in sorted(blog_dir.rglob("*.md")):
        scanned += 1
        if scanned > MAX_BLOG_FILES:
            raise RuntimeError(f"Exceeded MAX_BLOG_FILES ({MAX_BLOG_FILES}) scanning {blog_dir}")
        resolved = candidate.resolve()
        if not resolved.is_relative_to(blog_dir):
            raise ValueError(f"Refusing path outside blog_dir: {resolved}")
        if resolved.name.startswith(slug_prefix):
            matches.append(resolved)

    if not matches:
        raise FileNotFoundError(f"No release blog post matching '{slug_prefix}*.md' for version {checked_version}")

    rel_no_suffix = matches[-1].relative_to(content_dir).with_suffix("").as_posix()
    return f"{checked_base}/{rel_no_suffix}/"


def probe_live_post(url: str, version: str, timeout_seconds: float = HTTP_TIMEOUT_SECONDS) -> tuple[bool, str]:
    """Perform one bounded HTTP GET and verify HTTP 200 with `version` present in the body."""
    checked_version = _validate_version(version)
    if not isinstance(url, str) or not url.startswith(("https://", "http://127.0.0.1:", "http://localhost:")):
        raise ValueError(f"Untrusted probe URL: {url!r}")
    if timeout_seconds <= 0 or timeout_seconds > 60:
        raise ValueError(f"timeout_seconds out of bounds (0, 60]: {timeout_seconds}")

    req = Request(url, headers={"User-Agent": "datagrunt-site-verifier/1.0", "Cache-Control": "no-cache"})
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
            status = getattr(resp, "status", 200)
            body = resp.read(MAX_HTTP_BYTES).decode("utf-8", errors="replace")
            if 200 <= status < 300 and checked_version in body:
                return True, f"HTTP {status} (verified version {checked_version})"
            return False, f"HTTP {status} (version {checked_version} not yet in body)"
    except HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except URLError as exc:
        return False, f"URLError: {exc.reason}"
    except TimeoutError:
        return False, "TimeoutError"


def _run_git(site_dir: Path, args: list[str]) -> str:
    if not isinstance(site_dir, Path) or not (site_dir / ".git").exists():
        raise ValueError(f"Not a git checkout: {site_dir}")
    if not args or any(not isinstance(a, str) or not a for a in args):
        raise ValueError(f"Invalid git arguments: {args!r}")

    proc = subprocess.run(
        ["git", "-C", str(site_dir), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=GIT_TIMEOUT_SECONDS,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or f"exit code {proc.returncode}"
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout.strip()


def retrigger_empty_commit(site_dir: Path, version: str) -> str:
    """Push one empty fast-forward commit to main so Cloudflare Pages receives a fresh push webhook."""
    checked_version = _validate_version(version)
    if not isinstance(site_dir, Path) or not (site_dir / ".git").exists():
        raise ValueError(f"site_dir must be a valid git repository: {site_dir!r}")

    _run_git(site_dir, ["pull", "--rebase", "origin", "main"])
    _run_git(
        site_dir,
        [
            "-c",
            "user.name=github-actions[bot]",
            "-c",
            "user.email=github-actions[bot]@users.noreply.github.com",
            "commit",
            "--allow-empty",
            "-m",
            f"chore: retrigger Cloudflare Pages deploy for version {checked_version}",
        ],
    )
    try:
        _run_git(site_dir, ["push", "origin", "main"])
    except RuntimeError:
        _run_git(site_dir, ["pull", "--rebase", "origin", "main"])
        _run_git(site_dir, ["push", "origin", "main"])
    return _run_git(site_dir, ["rev-parse", "HEAD"])


def _poll_until_live(url: str, version: str, max_polls: int, poll_interval_seconds: float) -> tuple[bool, str]:
    last_detail = "no probes executed"
    for attempt in range(1, max_polls + 1):
        ok, detail = probe_live_post(url, version)
        last_detail = detail
        print(f"[verify-site] Probe {attempt}/{max_polls} {url} -> {detail}", flush=True)
        if ok:
            return True, detail
        if attempt < max_polls:
            time.sleep(poll_interval_seconds)
    return False, last_detail


def verify_or_heal_deploy(
    *,
    site_dir: Path,
    version: str,
    base_url: str = DEFAULT_BASE_URL,
    max_polls: int = 12,
    poll_interval_seconds: float = 10.0,
    max_retriggers: int = 1,
    retrigger_fn: Callable[[Path, str], str] = retrigger_empty_commit,
) -> None:
    """Poll the live release blog URL; if missed by Cloudflare Pages, push bounded empty-commit retriggers."""
    checked_version = _validate_version(version)
    if not (1 <= max_polls <= MAX_POLLS_CAP):
        raise ValueError(f"max_polls must be in [1, {MAX_POLLS_CAP}], got {max_polls}")
    if not (0.0 < poll_interval_seconds <= 60.0):
        raise ValueError(f"poll_interval_seconds must be in (0, 60], got {poll_interval_seconds}")
    if not (0 <= max_retriggers <= MAX_RETRIGGERS_CAP):
        raise ValueError(f"max_retriggers must be in [0, {MAX_RETRIGGERS_CAP}], got {max_retriggers}")

    post_url = resolve_post_url(site_dir, checked_version, base_url)
    ok, last_detail = _poll_until_live(post_url, checked_version, max_polls, poll_interval_seconds)
    if ok:
        return

    for retrigger_idx in range(1, max_retriggers + 1):
        print(
            f"[verify-site] Release {checked_version} not live after {max_polls} polls ({last_detail}); "
            f"retrigger {retrigger_idx}/{max_retriggers} via empty commit...",
            flush=True,
        )
        new_sha = retrigger_fn(site_dir, checked_version)
        print(f"[verify-site] Pushed retrigger commit {new_sha}; polling {post_url}...", flush=True)
        ok, last_detail = _poll_until_live(post_url, checked_version, max_polls, poll_interval_seconds)
        if ok:
            return

    raise RuntimeError(
        f"Live site deployment verification failed for Datagrunt {checked_version} at {post_url} "
        f"(last probe: {last_detail})"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-dir", type=Path, required=True, help="Path to checked-out datagrunt-site repository.")
    parser.add_argument("--pyproject", type=Path, required=True, help="Path to datagrunt pyproject.toml.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Live site base URL.")
    parser.add_argument("--max-polls", type=int, default=12, help="Max HTTP polls per deployment attempt.")
    parser.add_argument("--poll-interval", type=float, default=10.0, help="Seconds between HTTP polls.")
    parser.add_argument("--max-retriggers", type=int, default=1, help="Max empty-commit webhook retriggers.")
    args = parser.parse_args(argv)

    version = read_current_version(args.pyproject.resolve())
    verify_or_heal_deploy(
        site_dir=args.site_dir.resolve(),
        version=version,
        base_url=args.base_url,
        max_polls=args.max_polls,
        poll_interval_seconds=args.poll_interval,
        max_retriggers=args.max_retriggers,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[verify-site] ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
