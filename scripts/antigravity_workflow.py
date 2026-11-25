#!/usr/bin/env python3
"""Antigravity workflow script to run the AI agent that updates the doc site and publishes the release blog post."""

from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import os
import re
import subprocess
import sys
import textwrap
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Final

# Try importing dotenv
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_MONTH_NAMES: Final[tuple[str, ...]] = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)
_PYTHON_FENCE_LANGS: Final[frozenset[str]] = frozenset({"python", "py", "python3"})
_SCHEME_RE: Final[re.Pattern[str]] = re.compile(r"^([a-zA-Z][a-zA-Z0-9+.-]*):")
_FENCE_OPEN_RE: Final[re.Pattern[str]] = re.compile(r"^([ \t]*)(`{3,}|~{3,})([^\n]*)$")
_REF_LINK_RE: Final[re.Pattern[str]] = re.compile(r"^[ ]{0,3}\[(?!\^)([^\]]+)\]:[ \t]*(?:<([^>\n]+)>|(\S+))")
_AUTOLINK_RE: Final[re.Pattern[str]] = re.compile(r"<([a-zA-Z][a-zA-Z0-9+.-]*:[^<>\s]+)>")
_HTML_ATTR_RE: Final[re.Pattern[str]] = re.compile(r"""\b(?:href|src)\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.IGNORECASE)
_RAW_FILE_URI_RE: Final[re.Pattern[str]] = re.compile(r"\bfile://[^\s<>)\"'`]*", re.IGNORECASE)
_RAW_FORBIDDEN_SCHEME_RE: Final[re.Pattern[str]] = re.compile(
    r"(?<![a-zA-Z0-9+.-])(?:(?:http|ftp)://[^\s<>)\"'`]*|javascript:[^\s<>)\"'`]*)",
    re.IGNORECASE,
)


_GIT_REF_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._/@~^][A-Za-z0-9._/@~^-]*$")
_GIT_TIMEOUT_SECONDS: Final[float] = 30.0


def validate_git_ref(ref: str) -> str:
    """Validate a git commit/ref argument, rejecting leading dashes, whitespace, and control characters."""
    if not isinstance(ref, str) or not ref or len(ref) > 256:
        raise ValueError(f"Invalid git ref: {ref!r}")
    if ref.startswith("-") or ".." in ref or _GIT_REF_RE.fullmatch(ref) is None:
        raise ValueError(f"Invalid git ref: {ref!r}")
    return ref


def run_command(cmd: Sequence[str], cwd: Path | str | None = None) -> tuple[int, str, str]:
    """Run a subprocess argv sequence with ``shell=False`` and return ``(returncode, stdout, stderr)``."""
    if isinstance(cmd, (str, bytes)) or not isinstance(cmd, Sequence) or not cmd:
        raise TypeError("cmd must be a non-empty sequence of string arguments, not a shell string")
    argv = list(cmd)
    if any(not isinstance(part, str) or not part for part in argv):
        raise ValueError("every command argument must be a non-empty string")
    res = subprocess.run(argv, shell=False, text=True, capture_output=True, cwd=cwd, timeout=_GIT_TIMEOUT_SECONDS)
    return res.returncode, res.stdout.strip(), res.stderr.strip()


def get_version(repo_dir):
    """Read the current version of the project from pyproject.toml."""
    # A simple parser for pyproject.toml version
    try:
        pyproject_path = repo_dir / "pyproject.toml"
        if pyproject_path.exists():
            with open(pyproject_path, "r") as f:
                for line in f:
                    if line.strip().startswith("version ="):
                        # Extract the string, e.g., version = "3.1.6"
                        val = line.split("=")[1].strip().strip('"').strip("'")
                        return val
    except Exception as e:
        print(f"[!] Warning: Failed to parse version from pyproject.toml: {e}")
    return "unknown"


def version_slug(version: str) -> str:
    """Convert a version string (e.g. '5.4.1') into the dashed filename slug ('5-4-1')."""
    if not isinstance(version, str) or not version.strip():
        raise ValueError("version must be a non-empty string")
    return version.strip().replace(".", "-")


def expected_blog_dir(site_dir: Path, now: datetime | None = None) -> Path:
    """Compute the Hugo blog directory for the current (or given) year and month."""
    current = now if now is not None else datetime.now().astimezone()
    return site_dir / "content" / "blog" / f"{current.year}" / _MONTH_NAMES[current.month - 1]


def normalize_release_post_filename(post_path: Path, version: str) -> Path:
    """Enforce the dashed `datagrunt-{slug}-*.md` filename convention for a release post.

    If `post_path` uses the dotted form `datagrunt-{version}-<desc>.md`, rename it cleanly to
    `datagrunt-{slug}-<desc>.md`. Reject any other filename with a `ValueError`.
    """
    cleaned_version = version.strip()
    slug = version_slug(cleaned_version)
    expected_prefix = f"datagrunt-{slug}-"
    dotted_prefix = f"datagrunt-{cleaned_version}-"

    if post_path.suffix != ".md":
        raise ValueError(
            f"Invalid release blog post filename '{post_path.name}': expected '{expected_prefix}<short-description>.md'"
        )

    if post_path.name.startswith(expected_prefix) and len(post_path.stem) > len(expected_prefix):
        return post_path

    if "." in cleaned_version and post_path.name.startswith(dotted_prefix) and len(post_path.stem) > len(dotted_prefix):
        normalized = post_path.with_name(f"{expected_prefix}{post_path.name[len(dotted_prefix) :]}")
        if normalized.exists():
            raise ValueError(
                f"Cannot normalize dotted release post filename '{post_path.name}' to "
                f"'{normalized.name}': destination already exists"
            )
        post_path.rename(normalized)
        return normalized

    raise ValueError(
        f"Invalid release blog post filename '{post_path.name}': expected '{expected_prefix}<short-description>.md'"
    )


def normalize_and_find_release_posts(
    site_dir: Path,
    version: str,
    *,
    changed_files: Iterable[Path] | None = None,
) -> list[Path]:
    """Normalize dotted release-post filenames and verify at least one valid post exists for `version`."""
    slug = version_slug(version)
    cleaned_version = version.strip()
    expected_prefix = f"datagrunt-{slug}-"
    dotted_prefix = f"datagrunt-{cleaned_version}-"

    blog_dir = (site_dir / "content" / "blog").resolve()
    if not blog_dir.is_dir():
        raise ValueError(f"Missing Hugo blog directory: {blog_dir}")

    if changed_files is not None:
        for path in changed_files:
            resolved = path.resolve()
            if resolved.is_relative_to(blog_dir) and resolved.name != "_index.md" and resolved.exists():
                normalize_release_post_filename(resolved, cleaned_version)
    elif "." in cleaned_version:
        for candidate in sorted(blog_dir.rglob("*.md")):
            resolved = candidate.resolve()
            if resolved.name.startswith(dotted_prefix) and len(resolved.stem) > len(dotted_prefix):
                normalize_release_post_filename(resolved, cleaned_version)

    matches = [
        candidate.resolve()
        for candidate in sorted(blog_dir.rglob("*.md"))
        if candidate.name.startswith(expected_prefix) and len(candidate.stem) > len(expected_prefix)
    ]
    if not matches:
        raise ValueError(
            f"No release blog post matching '{expected_prefix}*.md' found under {blog_dir} "
            f"for version {cleaned_version}"
        )
    return matches


def _strip_fenced_code_blocks(content: str) -> str:
    """Replace fenced code blocks with blank lines while preserving line numbering."""
    out_lines: list[str] = []
    in_fence = False
    fence_char = ""
    fence_len = 0

    for line in content.splitlines():
        if not in_fence:
            match = _FENCE_OPEN_RE.match(line)
            if match:
                fence = match.group(2)
                info = match.group(3)
                # CommonMark forbids backticks in the info string of a backtick fence.
                if fence[0] != "`" or "`" not in info:
                    in_fence = True
                    fence_char = fence[0]
                    fence_len = len(fence)
                    out_lines.append("")
                    continue
            out_lines.append(line)
        else:
            stripped = line.lstrip(" \t")
            if stripped.startswith(fence_char * fence_len) and stripped.rstrip(" \t") == fence_char * len(
                stripped.rstrip(" \t")
            ):
                in_fence = False
            out_lines.append("")

    return "\n".join(out_lines)


def _mask_inline_code_spans(text: str) -> str:
    """Replace inline backtick code spans with spaces while preserving line breaks."""
    chars = list(text)
    n = len(chars)
    i = 0
    while i < n:
        if chars[i] != "`":
            i += 1
            continue
        start = i
        while i < n and chars[i] == "`":
            i += 1
        tick_len = i - start
        j = i
        found_end = -1
        while j < n:
            if chars[j] == "`":
                run_start = j
                while j < n and chars[j] == "`":
                    j += 1
                if j - run_start == tick_len:
                    found_end = j
                    break
            else:
                j += 1
        if found_end != -1:
            for k in range(start, found_end):
                if chars[k] != "\n":
                    chars[k] = " "
            i = found_end
    return "".join(chars)


def _iter_inline_markdown_links(text: str) -> list[tuple[int, str]]:
    """Extract `(line_no, target)` pairs from inline Markdown `[text](target)` links."""
    results: list[tuple[int, str]] = []
    n = len(text)
    i = 0
    while i < n:
        idx = text.find("](", i)
        if idx == -1:
            break
        line_no = text.count("\n", 0, idx) + 1
        j = idx + 2
        while j < n and text[j] in " \t":
            j += 1
        if j >= n:
            break
        if text[j] == "<":
            k = j + 1
            while k < n and text[k] not in ">\n":
                if text[k] == "\\" and k + 1 < n:
                    k += 2
                else:
                    k += 1
            if k < n and text[k] == ">":
                results.append((line_no, text[j + 1 : k].strip()))
                i = k + 1
                continue
        k = j
        paren_depth = 0
        while k < n:
            ch = text[k]
            if ch == "\\" and k + 1 < n:
                k += 2
                continue
            if ch in " \t\r\n":
                break
            if ch == "(":
                paren_depth += 1
            elif ch == ")":
                if paren_depth == 0:
                    break
                paren_depth -= 1
            k += 1
        target = text[j:k].strip()
        if target:
            results.append((line_no, target))
        i = max(k, idx + 2)
    return results


def _check_link_target(target: str) -> str | None:
    """Validate a single link destination; allow relative paths, anchors, and `https://` only."""
    cleaned = target.strip()
    if cleaned.startswith("<") and cleaned.endswith(">"):
        cleaned = cleaned[1:-1].strip()
    if not cleaned or cleaned.startswith(("#", "/", "./", "../")):
        return None
    scheme_match = _SCHEME_RE.match(cleaned)
    if scheme_match is None:
        return None
    scheme = scheme_match.group(1).lower()
    if scheme == "https":
        if cleaned.lower().startswith("https://"):
            return None
        return f"Malformed https link (missing 'https://'): {cleaned!r}"
    if scheme == "file":
        return f"Forbidden file:// URI: {cleaned!r}"
    return f"Disallowed non-https link scheme '{scheme}:' in {cleaned!r}"


def validate_markdown_links(content: str) -> list[str]:
    """Lint generated Markdown for `file://` URIs and non-`https://` absolute link schemes."""
    errors: list[str] = []
    seen_errors: set[str] = set()
    reported_uris: dict[int, list[str]] = {}

    def _record(line_no: int, message: str, uri: str | None = None) -> None:
        formatted = f"Line {line_no}: {message}"
        if formatted not in seen_errors:
            seen_errors.add(formatted)
            errors.append(formatted)
        if uri is not None:
            reported_uris.setdefault(line_no, []).append(uri)

    prose_with_inline_code = _strip_fenced_code_blocks(content)
    prose = _mask_inline_code_spans(prose_with_inline_code)

    # 1. Inline markdown links: [text](target) / ![alt](target)
    for line_no, target in _iter_inline_markdown_links(prose):
        err = _check_link_target(target)
        if err is not None:
            _record(line_no, err, target)

    # 2. Reference link definitions, autolinks, HTML href/src
    for line_no, line in enumerate(prose.splitlines(), start=1):
        ref_match = _REF_LINK_RE.match(line)
        if ref_match:
            target = (ref_match.group(2) or ref_match.group(3) or "").strip()
            err = _check_link_target(target)
            if err is not None:
                _record(line_no, err, target)

        for auto_match in _AUTOLINK_RE.finditer(line):
            target = auto_match.group(1).strip()
            err = _check_link_target(target)
            if err is not None:
                _record(line_no, err, target)

        for attr_match in _HTML_ATTR_RE.finditer(line):
            target = (attr_match.group(1) if attr_match.group(1) is not None else attr_match.group(2)).strip()
            err = _check_link_target(target)
            if err is not None:
                _record(line_no, err, target)

    # 3. Raw `file://` anywhere outside fenced code blocks (including inline code spans)
    for line_no, line in enumerate(prose_with_inline_code.splitlines(), start=1):
        already = reported_uris.get(line_no, [])
        for raw_match in _RAW_FILE_URI_RE.finditer(line):
            uri = raw_match.group(0)
            if any(uri in existing or existing in uri for existing in already):
                continue
            _record(line_no, f"Forbidden file:// URI: {uri!r}", uri)

    # 4. Raw non-https schemes (`http://`, `ftp://`, `javascript:`) in bare prose
    for line_no, line in enumerate(prose.splitlines(), start=1):
        already = reported_uris.get(line_no, [])
        for raw_match in _RAW_FORBIDDEN_SCHEME_RE.finditer(line):
            uri = raw_match.group(0)
            if any(uri in existing or existing in uri for existing in already):
                continue
            err = _check_link_target(uri)
            if err is not None:
                _record(line_no, err, uri)

    return errors


def extract_python_code_blocks(content: str) -> list[tuple[int, str]]:
    """Extract `(start_line, dedented_code)` for every fenced Python code block in Markdown."""
    blocks: list[tuple[int, str]] = []
    in_fence = False
    fence_char = ""
    fence_len = 0
    is_python = False
    block_start_line = 0
    block_lines: list[str] = []

    for line_no, line in enumerate(content.splitlines(), start=1):
        if not in_fence:
            match = _FENCE_OPEN_RE.match(line)
            if match:
                fence = match.group(2)
                info = match.group(3)
                if fence[0] == "`" and "`" in info:
                    continue
                in_fence = True
                fence_char = fence[0]
                fence_len = len(fence)
                lang = info.strip().split(maxsplit=1)[0].lower() if info.strip() else ""
                is_python = lang in _PYTHON_FENCE_LANGS
                block_start_line = line_no + 1
                block_lines = []
        else:
            stripped = line.lstrip(" \t")
            if stripped.startswith(fence_char * fence_len) and stripped.rstrip(" \t") == fence_char * len(
                stripped.rstrip(" \t")
            ):
                if is_python:
                    blocks.append((block_start_line, textwrap.dedent("\n".join(block_lines))))
                in_fence = False
                is_python = False
                block_lines = []
            elif is_python:
                block_lines.append(line)

    if in_fence and is_python:
        blocks.append((block_start_line, textwrap.dedent("\n".join(block_lines))))

    return blocks


def _resolve_datagrunt_src_root(repo_dir: Path | None = None) -> Path:
    """Locate the `src` directory containing the checked-out `datagrunt` package."""
    if repo_dir is not None:
        candidate_src = (repo_dir / "src").resolve()
        if (candidate_src / "datagrunt").is_dir():
            return candidate_src
        candidate_direct = repo_dir.resolve()
        if (candidate_direct / "datagrunt").is_dir():
            return candidate_direct
    return (Path(__file__).resolve().parent.parent / "src").resolve()


def _locate_module_file(src_root: Path, module_name: str) -> tuple[Path, bool] | None:
    """Resolve a dotted `datagrunt` module name to `(file_path, is_package)` under `src_root`."""
    parts = module_name.split(".")
    if not parts or parts[0] != "datagrunt" or not all(part.isidentifier() for part in parts):
        return None

    for depth in range(1, len(parts)):
        if not src_root.joinpath(*parts[:depth], "__init__.py").is_file():
            return None

    pkg_init = src_root.joinpath(*parts, "__init__.py")
    if pkg_init.is_file():
        return pkg_init, True

    mod_py = src_root.joinpath(*parts[:-1], f"{parts[-1]}.py")
    if mod_py.is_file():
        return mod_py, False

    mod_pyi = src_root.joinpath(*parts[:-1], f"{parts[-1]}.pyi")
    if mod_pyi.is_file():
        return mod_pyi, False

    return None


def _is_type_checking_guard(test: ast.expr) -> bool:
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    if isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
        return True
    return False


def _collect_target_names(target: ast.expr, names: set[str]) -> None:
    if isinstance(target, ast.Name):
        names.add(target.id)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for elt in target.elts:
            _collect_target_names(elt, names)


def _extract_string_sequence(value: ast.expr) -> set[str]:
    strings: set[str] = set()
    if isinstance(value, (ast.List, ast.Tuple, ast.Set)):
        for elt in value.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                strings.add(elt.value)
    return strings


def _collect_bindings(stmts: list[ast.stmt], names: set[str]) -> None:
    for stmt in stmts:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(stmt.name)
        elif isinstance(stmt, ast.Import):
            for alias in stmt.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(stmt, ast.ImportFrom):
            for alias in stmt.names:
                if alias.name != "*":
                    names.add(alias.asname or alias.name)
        elif isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                _collect_target_names(target, names)
                if isinstance(target, ast.Name) and target.id == "__all__":
                    names.update(_extract_string_sequence(stmt.value))
        elif isinstance(stmt, ast.AnnAssign):
            _collect_target_names(stmt.target, names)
            if isinstance(stmt.target, ast.Name) and stmt.target.id == "__all__" and stmt.value is not None:
                names.update(_extract_string_sequence(stmt.value))
        elif isinstance(stmt, ast.AugAssign):
            if isinstance(stmt.target, ast.Name) and stmt.target.id == "__all__":
                names.update(_extract_string_sequence(stmt.value))
        elif isinstance(stmt, ast.If):
            if not _is_type_checking_guard(stmt.test):
                _collect_bindings(stmt.body, names)
            _collect_bindings(stmt.orelse, names)
        elif isinstance(stmt, ast.Try):
            _collect_bindings(stmt.body, names)
            for handler in stmt.handlers:
                _collect_bindings(handler.body, names)
            _collect_bindings(stmt.orelse, names)
            _collect_bindings(stmt.finalbody, names)
        elif hasattr(ast, "TryStar") and isinstance(stmt, ast.TryStar):
            _collect_bindings(stmt.body, names)
            for handler in stmt.handlers:
                _collect_bindings(handler.body, names)
            _collect_bindings(stmt.orelse, names)
            _collect_bindings(stmt.finalbody, names)
        elif isinstance(stmt, (ast.With, ast.AsyncWith)):
            for item in stmt.items:
                if item.optional_vars is not None:
                    _collect_target_names(item.optional_vars, names)
            _collect_bindings(stmt.body, names)
        elif hasattr(ast, "TypeAlias") and isinstance(stmt, ast.TypeAlias):
            names.add(stmt.name.id)


def _collect_module_exports(mod_file: Path, is_package: bool) -> set[str]:
    """Collect module-level symbols and package submodules exposed by `mod_file` via static AST inspection."""
    tree = ast.parse(mod_file.read_text(encoding="utf-8"), filename=str(mod_file))
    names: set[str] = set()
    _collect_bindings(tree.body, names)

    if is_package:
        pkg_dir = mod_file.parent
        for child in pkg_dir.iterdir():
            if child.is_file() and child.suffix in (".py", ".pyi") and child.name != "__init__.py":
                names.add(child.stem)
            elif child.is_dir() and (child / "__init__.py").is_file():
                names.add(child.name)

    return names


def validate_python_code_samples(content: str, *, src_root: Path | None = None) -> list[str]:
    """Verify `datagrunt` imports and syntax in fenced Python code blocks without executing them."""
    resolved_src = _resolve_datagrunt_src_root(src_root)
    if not (resolved_src / "datagrunt").is_dir():
        return [f"Unable to locate checked-out datagrunt package under {resolved_src}"]

    errors: list[str] = []
    exports_cache: dict[str, set[str] | None] = {}

    def _get_exports(module_name: str) -> set[str] | None:
        if module_name not in exports_cache:
            located = _locate_module_file(resolved_src, module_name)
            if located is None:
                exports_cache[module_name] = None
            else:
                exports_cache[module_name] = _collect_module_exports(*located)
        return exports_cache[module_name]

    for start_line, code in extract_python_code_blocks(content):
        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            line_detail = f"line {start_line + (exc.lineno or 1) - 1}"
            errors.append(f"Python code block syntax error at {line_detail}: {exc.msg}")
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "datagrunt" or alias.name.startswith("datagrunt."):
                        if _get_exports(alias.name) is None:
                            err_line = start_line + node.lineno - 1
                            errors.append(
                                f"Line {err_line}: Invalid datagrunt import: module '{alias.name}' does not exist"
                            )
            elif isinstance(node, ast.ImportFrom):
                if node.level != 0 or node.module is None:
                    continue
                if node.module != "datagrunt" and not node.module.startswith("datagrunt."):
                    continue
                err_line = start_line + node.lineno - 1
                exports = _get_exports(node.module)
                if exports is None:
                    errors.append(f"Line {err_line}: Invalid datagrunt import: module '{node.module}' does not exist")
                    continue
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    if alias.name not in exports:
                        errors.append(
                            f"Line {err_line}: Invalid datagrunt import: "
                            f"'{alias.name}' is not exported by '{node.module}'"
                        )

    return errors


def validate_generated_markdown(
    content: str,
    *,
    allow_import_check: bool = True,
    src_root: Path | None = None,
) -> list[str]:
    """Validate generated Markdown for broken/forbidden links and invalid `datagrunt` code-sample imports."""
    errors = validate_markdown_links(content)
    if allow_import_check:
        errors.extend(validate_python_code_samples(content, src_root=src_root))
    return errors


def _snapshot_markdown_files(content_dir: Path) -> dict[Path, bytes]:
    if not content_dir.is_dir():
        return {}
    return {
        path.resolve(): hashlib.sha256(path.read_bytes()).digest()
        for path in sorted(content_dir.rglob("*.md"))
        if path.is_file()
    }


def validate_site_changes(
    *,
    site_dir: Path,
    version: str,
    before_snapshot: dict[Path, bytes],
    repo_dir: Path | None = None,
) -> list[Path]:
    """Normalize release post filenames and validate all newly created/modified Markdown files under `site_dir`."""
    content_dir = (site_dir / "content").resolve()
    blog_dir = (content_dir / "blog").resolve()
    after_snapshot = _snapshot_markdown_files(content_dir)
    changed_paths = [path for path, digest in after_snapshot.items() if before_snapshot.get(path) != digest]

    # Normalize any newly created/modified release blog posts and verify a valid dashed-slug post exists.
    normalized_changed: list[Path] = []
    for path in changed_paths:
        if blog_dir.is_dir() and path.is_relative_to(blog_dir) and path.name != "_index.md":
            normalized_changed.append(normalize_release_post_filename(path, version))
        else:
            normalized_changed.append(path)

    normalize_and_find_release_posts(site_dir, version, changed_files=normalized_changed)

    src_root = _resolve_datagrunt_src_root(repo_dir)
    all_errors: list[str] = []
    for md_path in normalized_changed:
        file_errors = validate_generated_markdown(
            md_path.read_text(encoding="utf-8"),
            allow_import_check=True,
            src_root=src_root,
        )
        for err in file_errors:
            all_errors.append(f"{md_path}: {err}")

    if all_errors:
        details = "\n".join(f"  - {err}" for err in all_errors)
        raise ValueError(f"Generated release documentation failed validation:\n{details}")

    return normalized_changed


def build_system_instructions(
    version: str,
    repo_dir: Path,
    site_dir: Path,
    *,
    now: datetime | None = None,
) -> str:
    """Build hardened system instructions for the release-post generator agent."""
    current = now if now is not None else datetime.now().astimezone()
    release_timestamp = current.isoformat(timespec="seconds")
    slug = version_slug(version)
    blog_dir = expected_blog_dir(site_dir, now=current)
    filename_prefix = f"datagrunt-{slug}-"

    return (
        "You are a technical writer and release coordination agent for Datagrunt.\n"
        f"The current local date-time is: {release_timestamp}.\n\n"
        "Your goal is to:\n"
        "1. Analyze the git diff, file statistics, and commit log of the datagrunt codebase "
        f"(located at `{repo_dir}`).\n"
        "2. Identify new features, bug fixes, performance improvements, or breaking changes.\n"
        "3. Update the documentation in the Hugo site located at "
        f"`{site_dir / 'content' / 'docs' / '_index.md'}` or other files. You must read the "
        "existing documentation first if you want to update it accurately. Ensure you format links and "
        "structures using Markdown.\n"
        f"4. Write a release blog post under `{blog_dir}/`. "
        f"You MUST use the exact dashed version filename prefix `{filename_prefix}<short-description>.md` "
        f"(for example `{filename_prefix}release-highlights.md`). Never use dots in the version slug "
        f"(never `datagrunt-{version}-...`), because CI's idempotency guard and deployment verifier "
        f"match `{filename_prefix}*.md`.\n"
        "   - The blog post must have front matter at the top:\n"
        "     ---\n"
        f'     title: "Datagrunt {version}: <engaging headline>"\n'
        f"     date: {release_timestamp}\n"
        "     draft: false\n"
        '     author: "Martin Graham"\n'
        '     author_email: "datagrunt@datagrunt.io"\n'
        '     description: "<engaging description of release highlights>"\n'
        '     tags: ["python", "csv", "data-engineering", "release-notes"]\n'
        "     ---\n"
        "   - The post should have a structured, premium technical style with code snippets explaining "
        "how to use new features.\n"
        "5. Output a summary of the files updated and created (including their absolute paths) and a brief "
        "overview of the content generated.\n"
        "6. Publish the post immediately (draft: false). Never future-date it; Hugo skips posts dated after "
        "the build.\n\n"
        "Hard quality rules (enforced automatically after generation — violations fail the build):\n"
        "- Links: NEVER emit `file://` URIs, local checkout paths as links, or `http://`, `ftp://`, or "
        "`javascript:` URLs. Use only `https://` URLs or site-relative paths (such as `/docs/...` or `#anchor`).\n"
        "- Python code samples: Only import documented public symbols verified directly against "
        f"`{repo_dir / 'src' / 'datagrunt'}` (check `src/datagrunt/__init__.py` vs submodules such as "
        "`datagrunt.core` before writing any `import` or `from ... import ...` statement). Every fenced Python "
        "code block must be syntactically valid Python.\n"
        "- Upstream dependency grounding: NEVER invent, speculate, or extrapolate upstream dependency "
        "release-note claims, features, or internal behaviors (for bumped Rust crates or Python packages) "
        "beyond what is explicitly present in the provided git diff, commit log, or checked-in files in "
        f"`{repo_dir}`. If a dependency bump has no changelog details in the diff or commit log, state only "
        "the version bump itself and what the diff directly shows."
    )


async def run_agent(version, commit_log, file_stat, git_diff, repo_dir):
    """Run the Antigravity Agent to update docs and write the blog post."""
    from google.antigravity import Agent, LocalAgentConfig
    from google.antigravity.hooks import policy

    # Define directories
    save_dir = str(repo_dir / ".antigravity_sessions")
    app_data_dir = str(repo_dir / ".antigravity_brain")

    # The sister Hugo site lives next to this repo by convention (both locally
    # and in the CI checkout layout); override with DATAGRUNT_SITE_DIR.
    site_dir = Path(os.environ.get("DATAGRUNT_SITE_DIR", repo_dir.parent / "datagrunt-site")).resolve()

    # Ensure directories exist
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(app_data_dir, exist_ok=True)

    # Full ISO 8601 timestamp, not a bare date: same-day releases must carry
    # distinct front-matter dates or Hugo breaks the listing tie by title,
    # rendering the newer release below the older one (#288).
    now = datetime.now().astimezone()
    system_instructions = build_system_instructions(version=version, repo_dir=repo_dir, site_dir=site_dir, now=now)

    # Check if using Vertex AI
    use_vertex = os.getenv("USE_VERTEX", "").lower() in ("true", "1", "yes")
    gcp_project = os.getenv("GCP_PROJECT")
    gcp_location = os.getenv("GCP_LOCATION", "us-central1")

    config_kwargs: dict[str, object] = {
        "model": "gemini-4-hydrogen",
        "system_instructions": system_instructions,
        "app_data_dir": app_data_dir,
        "save_dir": save_dir,
        # Allow all tools so we can read and write files in both repos and run git log
        "policies": [policy.allow_all()],
    }
    if use_vertex:
        config_kwargs["vertex"] = True
        if gcp_project:
            config_kwargs["project"] = gcp_project
        if gcp_location:
            config_kwargs["location"] = gcp_location

    config = LocalAgentConfig(**config_kwargs)

    prompt = f"""
Please process the release changes for Datagrunt version {version}.

--- COMMIT LOG ---
{commit_log}

--- FILE STATISTICS ---
{file_stat}

--- FULL GIT DIFF ---
{git_diff[:25000]}  # Limit diff size if it is extremely long
"""

    content_dir = site_dir / "content"
    before_snapshot = _snapshot_markdown_files(content_dir)

    print("[*] Starting Antigravity Agent...")
    async with Agent(config) as agent:
        response = await agent.chat(prompt)
        async for token in response:
            print(token, end="", flush=True)
        print()

        # Save conversation ID
        conv_id = agent.conversation_id
        session_file = repo_dir / ".antigravity_latest_session"
        with open(session_file, "w") as f:
            f.write(conv_id)

        print("\n================================================================================")
        print("[+] Antigravity session saved.")
        print(f"[*] Conversation ID: {conv_id}")
        print("To review the blog post locally and guide updates, run:")
        print("  python scripts/review_blog.py")
        print("================================================================================")

    if content_dir.is_dir():
        validate_site_changes(
            site_dir=site_dir,
            version=version,
            before_snapshot=before_snapshot,
            repo_dir=repo_dir,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Run Antigravity Agent to write docs and blog post draft based on git changes."
    )
    parser.add_argument(
        "--pre-merge-commit",
        default="HEAD~1",
        help="The commit/ref before the merge. Used to compute the diff. Defaults to HEAD~1.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the script (analyze git but do not call the Agent).",
    )
    args = parser.parse_args()

    repo_dir = Path(__file__).parent.parent.resolve()

    # Load version
    version = get_version(repo_dir)
    print(f"[*] Project Version: {version}")

    # Gather git log, stats, and diff
    pre_merge_commit = validate_git_ref(args.pre_merge_commit)
    print(f"[*] Gathering git changes between {pre_merge_commit} and HEAD...")
    code_log, commit_log, _ = run_command(
        ["git", "log", f"{pre_merge_commit}..HEAD", "--oneline", "--no-merges"], cwd=repo_dir
    )
    code_stat, file_stat, _ = run_command(["git", "diff", "--stat", pre_merge_commit, "HEAD"], cwd=repo_dir)
    code_diff, git_diff, _ = run_command(["git", "diff", pre_merge_commit, "HEAD"], cwd=repo_dir)

    if code_log != 0 or code_diff != 0:
        print(f"[!] Warning: Git commands failed. Check if '{pre_merge_commit}' is a valid commit.")
        print("[*] Falling back to analyzing the latest commit (HEAD~1..HEAD)...")
        _, commit_log, _ = run_command(["git", "log", "-n", "5", "--oneline", "--no-merges"], cwd=repo_dir)
        _, file_stat, _ = run_command(["git", "diff", "--stat", "HEAD~1", "HEAD"], cwd=repo_dir)
        _, git_diff, _ = run_command(["git", "diff", "HEAD~1", "HEAD"], cwd=repo_dir)

    print(f"[*] Commits found:\n{commit_log}\n")
    print(f"[*] Files changed:\n{file_stat}\n")

    if args.dry_run:
        print("[Dry-Run] Git changes analyzed. Exiting dry run mode.")
        sys.exit(0)

    # Check for GEMINI_API_KEY or Vertex AI configurations
    use_vertex = os.getenv("USE_VERTEX", "").lower() in ("true", "1", "yes")
    if not use_vertex and not os.getenv("GEMINI_API_KEY"):
        print("[!] Error: No authentication configured.")
        print("[!] Please configure one of the following in your .env file:")
        print("    1. Google AI Studio (Free Tier): set GEMINI_API_KEY")
        print("    2. Vertex AI (GCP): set USE_VERTEX=True and GCP_PROJECT=your-project-id")
        sys.exit(1)

    # Run the async agent
    asyncio.run(
        run_agent(
            version=version,
            commit_log=commit_log,
            file_stat=file_stat,
            git_diff=git_diff,
            repo_dir=repo_dir,
        )
    )


if __name__ == "__main__":
    main()
