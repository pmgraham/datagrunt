"""GitHub Actions workflow invariants: deduplication, release guards, allowlist + SHA pinning, and multi-core CI."""

import asyncio
import importlib.util
import os
import re
import subprocess
import sys
import threading
import tomllib
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTIONS = ROOT / ".github" / "actions"
PYPROJECT = ROOT / "pyproject.toml"
SHA_PINNED_USES_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*@[0-9a-f]{40}$")
ALLOWED_ACTION_PREFIXES = (
    "./.github/",
    "actions/",
    "github/",
    "pmgraham/",
    "PyO3/maturin-action@",
    "astral-sh/setup-uv@",
    "dtolnay/rust-toolchain@",
    "pypa/gh-action-pypi-publish@",
)
MAX_WORKFLOW_FILES = 64


def _load_yaml_mapping(path: Path) -> dict:
    if not isinstance(path, Path) or path.suffix not in (".yml", ".yaml"):
        raise ValueError(f"Expected a .yml/.yaml path, got {path!r}")
    if not path.is_file():
        raise FileNotFoundError(f"YAML file missing: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise TypeError(f"Expected mapping in {path.name}, got {type(loaded).__name__}")
    return loaded


def _load_workflow(filename: str) -> dict:
    if not filename or not filename.endswith((".yml", ".yaml")):
        raise ValueError(f"Expected a .yml/.yaml workflow filename, got {filename!r}")
    return _load_yaml_mapping(WORKFLOWS / filename)


def _triggers(doc: dict) -> dict:
    if not isinstance(doc, dict) or not doc:
        raise ValueError("Expected non-empty workflow mapping")
    # PyYAML 1.1 parses bare `on:` as boolean True unless quoted.
    triggers = doc.get("on") if "on" in doc else doc.get(True)
    if not isinstance(triggers, dict):
        raise TypeError(f"Expected mapping for workflow triggers, got {type(triggers).__name__}")
    return triggers


def _job_steps(ci: dict, job_id: str) -> list[dict]:
    if not isinstance(ci, dict) or "jobs" not in ci:
        raise ValueError("Workflow mapping must contain jobs")
    if not job_id or job_id not in ci["jobs"]:
        raise KeyError(f"Missing workflow job {job_id!r}")
    steps = ci["jobs"][job_id].get("steps")
    if not isinstance(steps, list) or not steps:
        raise TypeError(f"Expected non-empty step list in job {job_id!r}")
    return [step for step in steps if isinstance(step, dict)]


def _collect_uses_refs(doc: dict, label: str) -> list[str]:
    if not isinstance(doc, dict) or not label:
        raise ValueError("Expected valid YAML document and label")
    refs: list[str] = []
    jobs = doc.get("jobs")
    if isinstance(jobs, dict):
        for job in jobs.values():
            if isinstance(job, dict) and "uses" in job:
                refs.append(str(job["uses"]))
            for step in (job.get("steps") or []) if isinstance(job, dict) else []:
                if isinstance(step, dict) and "uses" in step:
                    refs.append(str(step["uses"]))
    runs = doc.get("runs")
    if isinstance(runs, dict):
        for step in runs.get("steps") or []:
            if isinstance(step, dict) and "uses" in step:
                refs.append(str(step["uses"]))
    return refs


def test_ci_supports_manual_dispatch_and_cancels_superseded_runs() -> None:
    ci = _load_workflow("ci.yml")
    triggers = _triggers(ci)
    assert "pull_request" in triggers
    assert "push" in triggers
    assert "workflow_dispatch" in triggers

    concurrency = ci.get("concurrency")
    assert isinstance(concurrency, dict)
    assert concurrency.get("cancel-in-progress") is True
    assert "pull_request.number" in str(concurrency.get("group", ""))


def test_ci_runs_core_gate_on_push_but_skips_duplicate_wheel_and_compat_builds() -> None:
    ci = _load_workflow("ci.yml")
    jobs = ci["jobs"]

    # Core `test` job is gated only on `changes.outputs.run_ci` (never skipped on `push`
    # when CI-relevant files change, so merged `main` and README badge stay verified).
    assert jobs["test"].get("needs") == ["changes"]
    test_if = str(jobs["test"].get("if", ""))
    assert "needs.changes.outputs.run_ci == 'true'" in test_if
    assert "push" not in test_if

    # Cross-OS wheel matrix, 3-version Python matrix, and advisory audit already ran on the PR
    # (and Publish to PyPI builds release wheels after Bump Version).
    for job_id in ("python-compat", "build", "audit"):
        assert jobs[job_id].get("needs") == ["changes"]
        condition = str(jobs[job_id].get("if", ""))
        assert "github.event_name != " in condition and "push" in condition, f"{job_id} missing push dedupe guard"
        assert "needs.changes.outputs.run_ci == 'true'" in condition, f"{job_id} missing run_ci path-filter guard"

    # Single always-run ruleset gate verifies required jobs when CI ran, and passes on skipped docs-only PRs.
    gate = jobs["ci-gate"]
    assert gate["name"] == "CI gate"
    assert gate.get("if") == "always()"
    assert gate.get("needs") == ["changes", "test", "build"]


def test_publish_to_pypi_guards_both_build_and_publish_on_bump_success() -> None:
    publish_wf = _load_workflow("publish-to-pypi.yml")
    jobs = publish_wf["jobs"]

    for job_id in ("build", "publish"):
        condition = str(jobs[job_id].get("if", ""))
        assert "github.event.workflow_run.conclusion == " in condition and "success" in condition, (
            f"publish-to-pypi.yml job {job_id!r} must not run when Bump Version fails"
        )
        assert "github.event_name == " in condition and "workflow_dispatch" in condition


def test_bump_version_serializes_runs_and_skips_already_tagged_head() -> None:
    bump = _load_workflow("bump-version.yml")
    concurrency = bump.get("concurrency")
    assert isinstance(concurrency, dict)
    assert concurrency.get("cancel-in-progress") is False

    steps = _job_steps(bump, "bump-version")
    step_ids = {step.get("id"): step for step in steps if "id" in step}
    assert "tagcheck" in step_ids
    assert "git tag --points-at HEAD" in str(step_ids["tagcheck"].get("run", ""))

    bump_steps = [s for s in steps if s.get("name") == "Bump version"]
    assert len(bump_steps) == 1
    assert "steps.tagcheck.outputs.already_tagged != " in str(bump_steps[0].get("if", ""))


def test_pyproject_configures_xdist_no_cover_marker_and_rust_uv_cache_keys() -> None:
    pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    dev_deps = pyproject["project"]["optional-dependencies"]["dev"]
    assert [dep for dep in dev_deps if dep.startswith("pytest-xdist")] == ["pytest-xdist>=3.8.0,<4"]

    markers = pyproject["tool"]["pytest"]["ini_options"]["markers"]
    assert any(m.startswith("no_cover:") for m in markers)

    cache_keys = pyproject["tool"]["uv"]["cache-keys"]
    assert cache_keys == [
        {"file": "pyproject.toml"},
        {"file": "rust/Cargo.toml"},
        {"file": "rust/Cargo.lock"},
        {"file": "rust/**/*.rs"},
    ]


def test_ci_preserves_required_jobs_and_runs_disjoint_xdist_plus_benchmark_passes() -> None:
    ci = _load_workflow("ci.yml")
    jobs = ci["jobs"]
    assert jobs["test"]["name"] == "cargo + pytest (both backends)"
    assert jobs["python-compat"]["name"] == "python compat"
    assert jobs["python-compat"]["strategy"]["matrix"]["python-version"] == ["3.11", "3.13", "3.14"]
    assert jobs["build"]["name"] == "wheel matrix"
    assert jobs["audit"]["name"] == "dependency audit (advisory)"

    for job_id in ("test", "python-compat"):
        steps = _job_steps(ci, job_id)
        pytest_steps = [s for s in steps if str(s.get("name", "")).startswith("pytest ")]
        assert [s["name"] for s in pytest_steps] == [
            "pytest (Rust backend)",
            "pytest (Python fallback backend)",
        ]
        assert pytest_steps[1].get("env", {}).get("DATAGRUNT_DISABLE_RUST") == "1"
        for step in pytest_steps:
            cmd = str(step.get("run", ""))
            assert "-n logical --dist=worksteal" in cmd
            assert '-m "not corpus and not live and not no_cover"' in cmd
            assert "--cov-fail-under=95" in cmd
            assert '-p no:cov -o addopts="" -m "no_cover and not corpus and not live"' in cmd


def test_ci_uses_sysmon_coverage_core_for_py312_plus() -> None:
    ci = _load_workflow("ci.yml")
    test_env = ci["jobs"]["test"].get("env", {})
    compat_env = ci["jobs"]["python-compat"].get("env", {})
    assert test_env.get("COVERAGE_CORE") == "sysmon"
    compat_core = str(compat_env.get("COVERAGE_CORE", ""))
    assert "sysmon" in compat_core and "ctrace" in compat_core and "3.11" in compat_core


def test_ci_enables_fresh_pyo3_uv_rust_and_cargo_audit_caches() -> None:
    ci = _load_workflow("ci.yml")
    for job_id in ("test", "python-compat", "audit"):
        steps = _job_steps(ci, job_id)
        uv_steps = [s for s in steps if "astral-sh/setup-uv@" in str(s.get("uses", ""))]
        assert len(uv_steps) == 1
        uv_with = uv_steps[0].get("with", {})
        assert str(uv_with.get("enable-cache")).lower() == "true"
        glob_str = str(uv_with.get("cache-dependency-glob", ""))
        assert "pyproject.toml" in glob_str and "rust/Cargo.lock" in glob_str
        install_cmds = [str(s.get("run", "")) for s in steps if "uv pip install" in str(s.get("run", ""))]
        assert install_cmds and all("--reinstall-package datagrunt" in cmd for cmd in install_cmds)

    for job_id in ("test", "python-compat"):
        cache_steps = [s for s in _job_steps(ci, job_id) if "actions/cache@" in str(s.get("uses", ""))]
        assert len(cache_steps) == 1
        assert "rust/target" in str(cache_steps[0].get("with", {}).get("path", ""))

    audit_steps = _job_steps(ci, "audit")
    audit_cache = [s for s in audit_steps if "actions/cache@" in str(s.get("uses", ""))]
    assert len(audit_cache) == 1
    assert "cargo-audit" in str(audit_cache[0].get("with", {}).get("path", ""))
    audit_run = [s for s in audit_steps if s.get("name") == "cargo audit"]
    assert len(audit_run) == 1
    assert "command -v cargo-audit" in str(audit_run[0].get("run", ""))


def test_all_workflow_actions_match_repo_allowlist_and_commit_sha_pinning() -> None:
    yaml_files = sorted([*WORKFLOWS.glob("*.yml"), *WORKFLOWS.glob("*.yaml")])
    if ACTIONS.is_dir():
        yaml_files.extend(sorted([*ACTIONS.rglob("action.yml"), *ACTIONS.rglob("action.yaml")]))
    assert 0 < len(yaml_files) <= MAX_WORKFLOW_FILES
    for wf_path in yaml_files:
        doc = _load_yaml_mapping(wf_path)
        for ref in _collect_uses_refs(doc, wf_path.name):
            assert ref.startswith(ALLOWED_ACTION_PREFIXES), (
                f"Action {ref!r} in {wf_path.name} is outside repository allowlist"
            )
            is_local = ref.startswith("./.github/")
            assert is_local or SHA_PINNED_USES_RE.fullmatch(ref) is not None, (
                f"Unpinned action {ref!r} in {wf_path.name}"
            )


def test_ci_tesseract_apt_install_has_non_ubuntu_com_mirror_fallback() -> None:
    ci = _load_workflow("ci.yml")
    for job_id in ("test", "python-compat"):
        steps = [s for s in _job_steps(ci, job_id) if s.get("name") == "Install tesseract (for PDF OCR tests)"]
        assert len(steps) == 1
        cmd = str(steps[0].get("run", ""))
        assert "tesseract-ocr" in cmd
        assert "https://mirrors.edge.kernel.org/ubuntu/" in cmd
        assert "/etc/apt/apt-mirrors.txt" in cmd


def _load_verify_site_deploy_module() -> ModuleType:
    script_path = ROOT / "scripts" / "verify_site_deploy.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"Missing deploy verifier script: {script_path}")
    spec = importlib.util.spec_from_file_location("verify_site_deploy", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_auto_publish_site_fetches_full_history_and_verifies_live_deploy() -> None:
    wf = _load_workflow("auto-publish-site.yml")
    steps = _job_steps(wf, "update-docs")

    site_checkouts = [s for s in steps if s.get("name") == "Checkout datagrunt-site"]
    assert len(site_checkouts) == 1
    assert site_checkouts[0].get("with", {}).get("fetch-depth") == 0

    verify_steps = [s for s in steps if s.get("name") == "Verify and heal live site deployment"]
    assert len(verify_steps) == 1
    assert "if" not in verify_steps[0], "Live site verification must also run when postcheck skips regeneration"
    verify_cmd = str(verify_steps[0].get("run", ""))
    assert "python3 datagrunt/scripts/verify_site_deploy.py" in verify_cmd
    assert "--site-dir datagrunt-site" in verify_cmd
    assert "--pyproject datagrunt/pyproject.toml" in verify_cmd


def test_auto_publish_site_extracts_version_once_and_passes_via_env() -> None:
    wf = _load_workflow("auto-publish-site.yml")
    steps = _job_steps(wf, "update-docs")

    extract_indices = [idx for idx, step in enumerate(steps) if "current_version" in str(step.get("run", ""))]
    assert len(extract_indices) == 1, "current_version must be extracted in exactly one step"
    extract_idx = extract_indices[0]
    extract_step = steps[extract_idx]
    step_id = extract_step.get("id")
    assert step_id == "meta"
    extract_run = str(extract_step.get("run", ""))
    assert 'echo "version=$VERSION" >> "$GITHUB_OUTPUT"' in extract_run

    expected_expr = f"${{{{ steps.{step_id}.outputs.version }}}}"
    consumer_names = (
        "Check whether this version is already posted",
        "Run Antigravity Workflow",
        "Commit and Push Docs Update",
    )
    for name in consumer_names:
        matching = [(idx, s) for idx, s in enumerate(steps) if s.get("name") == name]
        assert len(matching) == 1, f"Expected exactly one step named {name!r}"
        consumer_idx, consumer = matching[0]
        assert consumer_idx > extract_idx, f"{name!r} must run after version extraction"
        assert consumer.get("env", {}).get("VERSION") == expected_expr
        run_cmd = str(consumer.get("run", ""))
        assert "current_version" not in run_cmd
        assert expected_expr not in run_cmd, f"{name!r} must read $VERSION from env, not inline expression"
        assert "$VERSION" in run_cmd


def test_verify_site_deploy_resolves_post_url_and_validates_boundaries(tmp_path: Path) -> None:
    mod = _load_verify_site_deploy_module()

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.bumpver]\ncurrent_version = "5.3.9"\n', encoding="utf-8")
    assert mod.read_current_version(pyproject) == "5.3.9"

    bad_pyproject = tmp_path / "bad.toml"
    bad_pyproject.write_text('current_version = "not-semver"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="version"):
        mod.read_current_version(bad_pyproject)

    site_dir = tmp_path / "datagrunt-site"
    post_dir = site_dir / "content" / "blog" / "2026" / "november"
    post_dir.mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="5.3.9"):
        mod.resolve_post_url(site_dir, "5.3.9", "https://www.datagrunt.io")

    post = post_dir / "datagrunt-5-3-9-wave-2a-optimizations.md"
    post.write_text("---\ntitle: Test\n---\nBody\n", encoding="utf-8")
    assert (
        mod.resolve_post_url(site_dir, "5.3.9", "https://www.datagrunt.io/")
        == "https://www.datagrunt.io/blog/2026/november/datagrunt-5-3-9-wave-2a-optimizations/"
    )

    with pytest.raises(ValueError, match="base_url"):
        mod.resolve_post_url(site_dir, "5.3.9", "http://evil.example.com")


def test_verify_site_deploy_retriggers_empty_commit_on_dropped_webhook_and_fails_loud(tmp_path: Path) -> None:
    mod = _load_verify_site_deploy_module()

    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-b", "main", str(origin)], check=True, timeout=15)

    site_dir = tmp_path / "site"
    subprocess.run(["git", "clone", str(origin), str(site_dir)], check=True, timeout=15)
    subprocess.run(["git", "-C", str(site_dir), "checkout", "-B", "main"], check=True, timeout=15)

    post_path = site_dir / "content" / "blog" / "2026" / "november" / "datagrunt-5-3-9-test.md"
    post_path.parent.mkdir(parents=True)
    post_path.write_text("# Datagrunt 5.3.9\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(site_dir), "add", "content/"], check=True, timeout=15)
    subprocess.run(
        [
            "git",
            "-C",
            str(site_dir),
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "docs: release blog post for version 5.3.9",
        ],
        check=True,
        timeout=15,
    )
    subprocess.run(["git", "-C", str(site_dir), "push", "-u", "origin", "main"], check=True, timeout=15)
    initial_sha = subprocess.run(
        ["git", f"--git-dir={origin}", "rev-parse", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.strip()

    state = {"live": False}

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if state["live"] and self.path == "/blog/2026/november/datagrunt-5-3-9-test/":
                body = b"<html><body>Datagrunt 5.3.9 release notes</body></html>"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"

        # Scenario 1: Dropped webhook (404 across initial window) -> empty-commit push flips live -> succeeds.
        orig_retrigger = mod.retrigger_empty_commit

        def _retrigger_and_flip(repo: Path, ver: str) -> str:
            sha = orig_retrigger(repo, ver)
            state["live"] = True
            return sha

        mod.verify_or_heal_deploy(
            site_dir=site_dir,
            version="5.3.9",
            base_url=base_url,
            max_polls=2,
            poll_interval_seconds=0.01,
            max_retriggers=1,
            retrigger_fn=_retrigger_and_flip,
        )

        healed_sha = subprocess.run(
            ["git", f"--git-dir={origin}", "rev-parse", "refs/heads/main"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        assert healed_sha != initial_sha
        commit_msg = subprocess.run(
            ["git", f"--git-dir={origin}", "log", "-1", "--format=%s", healed_sha],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        assert "5.3.9" in commit_msg

        # Scenario 2: Already live -> returns immediately without pushing another commit.
        mod.verify_or_heal_deploy(
            site_dir=site_dir,
            version="5.3.9",
            base_url=base_url,
            max_polls=2,
            poll_interval_seconds=0.01,
            max_retriggers=1,
        )
        after_noop_sha = subprocess.run(
            ["git", f"--git-dir={origin}", "rev-parse", "refs/heads/main"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        assert after_noop_sha == healed_sha

        # Scenario 3: Persistent 404 even after retrigger -> fails loud with RuntimeError.
        state["live"] = False
        with pytest.raises(RuntimeError, match="5.3.9"):
            mod.verify_or_heal_deploy(
                site_dir=site_dir,
                version="5.3.9",
                base_url=base_url,
                max_polls=2,
                poll_interval_seconds=0.01,
                max_retriggers=1,
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _load_antigravity_workflow_module() -> ModuleType:
    script_path = ROOT / "scripts" / "antigravity_workflow.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"Missing antigravity workflow script: {script_path}")
    spec = importlib.util.spec_from_file_location("antigravity_workflow", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_auto_publish_site_pins_antigravity_sdk_for_gemini_4_hydrogen() -> None:
    wf = _load_workflow("auto-publish-site.yml")
    steps = _job_steps(wf, "update-docs")

    install_steps = [s for s in steps if s.get("name") == "Install dependencies"]
    assert len(install_steps) == 1
    install_cmd = str(install_steps[0].get("run", ""))
    assert "google-antigravity==0.1.24" in install_cmd
    assert "google-antigravity==0.1.7" not in install_cmd
    assert "python-dotenv==1.2.4" in install_cmd
    assert "python-dotenv==1.2.2" not in install_cmd


def test_antigravity_workflow_uses_gemini_4_hydrogen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    allow_all_sentinel = object()

    def _local_agent_config(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return dict(kwargs)

    class _StubAgent:
        conversation_id = "conv-test-123"

        def __init__(self, config: object) -> None:
            self.config = config

        async def __aenter__(self) -> "_StubAgent":
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        async def chat(self, prompt: str) -> AsyncIterator[str]:
            async def _tokens() -> AsyncIterator[str]:
                yield "ok"

            return _tokens()

    google_mod = ModuleType("google")
    antigravity_mod = ModuleType("google.antigravity")
    hooks_mod = ModuleType("google.antigravity.hooks")
    setattr(hooks_mod, "policy", SimpleNamespace(allow_all=lambda: allow_all_sentinel))
    setattr(antigravity_mod, "Agent", _StubAgent)
    setattr(antigravity_mod, "LocalAgentConfig", _local_agent_config)
    setattr(antigravity_mod, "hooks", hooks_mod)
    setattr(google_mod, "antigravity", antigravity_mod)

    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.antigravity", antigravity_mod)
    monkeypatch.setitem(sys.modules, "google.antigravity.hooks", hooks_mod)

    mod = _load_antigravity_workflow_module()

    monkeypatch.delenv("USE_VERTEX", raising=False)
    monkeypatch.setenv("DATAGRUNT_SITE_DIR", str(tmp_path / "datagrunt-site"))
    repo_dir = tmp_path / "datagrunt"
    repo_dir.mkdir()

    asyncio.run(
        mod.run_agent(
            version="5.4.1",
            commit_log="abc feat",
            file_stat="1 file",
            git_diff="diff",
            repo_dir=repo_dir,
        )
    )

    assert captured["model"] == "gemini-4-hydrogen"
    assert Path(captured["app_data_dir"]).is_absolute()
    assert Path(captured["save_dir"]).is_absolute()
    assert captured["policies"] == [allow_all_sentinel]
    assert (repo_dir / ".antigravity_latest_session").read_text(encoding="utf-8") == "conv-test-123"


def test_antigravity_workflow_version_slug_dynamic_blog_dir_and_filename_normalization(tmp_path: Path) -> None:
    mod = _load_antigravity_workflow_module()
    site_dir = tmp_path / "datagrunt-site"

    assert mod.version_slug("5.4.1") == "5-4-1"
    assert mod.version_slug("4.6.2") == "4-6-2"
    with pytest.raises(ValueError, match="non-empty"):
        mod.version_slug("   ")

    march_2027 = datetime(2027, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    nov_2026 = datetime(2026, 11, 21, 17, 48, 0, tzinfo=timezone.utc)
    assert mod.expected_blog_dir(site_dir, now=march_2027) == site_dir / "content" / "blog" / "2027" / "march"
    assert mod.expected_blog_dir(site_dir, now=nov_2026) == site_dir / "content" / "blog" / "2026" / "november"

    blog_dir = mod.expected_blog_dir(site_dir, now=nov_2026)
    blog_dir.mkdir(parents=True)

    # Dotted filename (the #336 regression) is cleanly normalized to the dashed slug.
    dotted_post = blog_dir / "datagrunt-4.6.2-regex-updates.md"
    dotted_post.write_text("# Datagrunt 4.6.2\n", encoding="utf-8")
    normalized = mod.normalize_release_post_filename(dotted_post, "4.6.2")
    assert normalized == blog_dir / "datagrunt-4-6-2-regex-updates.md"
    assert normalized.is_file()
    assert not dotted_post.exists()

    # Already-dashed filename is preserved without error.
    assert mod.normalize_release_post_filename(normalized, "4.6.2") == normalized
    assert mod.normalize_and_find_release_posts(site_dir, "4.6.2") == [normalized.resolve()]

    # Collision with an existing dashed file fails loud instead of clobbering.
    duplicate_dotted = blog_dir / "datagrunt-4.6.2-regex-updates.md"
    duplicate_dotted.write_text("# Duplicate\n", encoding="utf-8")
    with pytest.raises(ValueError, match="destination already exists"):
        mod.normalize_release_post_filename(duplicate_dotted, "4.6.2")
    duplicate_dotted.unlink()

    # Unrelated filename or missing short-description suffix is rejected.
    bad_name = blog_dir / "release-notes.md"
    bad_name.write_text("# Bad\n", encoding="utf-8")
    with pytest.raises(ValueError, match="datagrunt-4-6-2-"):
        mod.normalize_release_post_filename(bad_name, "4.6.2")

    no_desc = blog_dir / "datagrunt-4-6-2.md"
    no_desc.write_text("# No desc\n", encoding="utf-8")
    with pytest.raises(ValueError, match="datagrunt-4-6-2-"):
        mod.normalize_release_post_filename(no_desc, "4.6.2")

    # Missing release post for a version fails loud.
    with pytest.raises(ValueError, match="datagrunt-9-9-9-"):
        mod.normalize_and_find_release_posts(site_dir, "9.9.9")


def test_antigravity_workflow_validates_markdown_links() -> None:
    mod = _load_antigravity_workflow_module()

    valid_md = """---
title: "Datagrunt 5.4.1: Release Highlights"
date: 2026-11-21T17:48:43+00:00
draft: false
author: "Martin Graham"
author_email: "datagrunt@datagrunt.io"
description: "Highlights for 5.4.1"
tags: ["python", "csv"]
---

See the [CSV guide](/docs/csv/), jump to [benchmarks](#benchmarks), read a
[sibling note](./other-post.md), visit [Datagrunt](https://www.datagrunt.io/docs/ "Docs"),
or read [RFC 4180](https://en.wikipedia.org/wiki/Comma-separated_values_(RFC_4180)).

[gh]: https://github.com/pmgraham/datagrunt
[^1]: Footnote with colon: safe text.

```bash
curl http://localhost:8000/health
```
"""
    assert mod.validate_generated_markdown(valid_md, allow_import_check=False) == []

    # Reject file:// in inline links, angle links, reference links, HTML attrs, and raw text.
    file_link_md = (
        "Inspect [dialect.rs](file:///home/runner/work/datagrunt/rust/src/dialect.rs).\n"
        "Also [spaced](<file:///home/runner/my dir/file.py>).\n"
        "[ref]: file:///tmp/local.py\n"
        '<a href="file:///etc/hosts">hosts</a>\n'
        "Raw path: file:///home/runner/work/datagrunt/README.md\n"
    )
    file_errors = mod.validate_generated_markdown(file_link_md, allow_import_check=False)
    assert len(file_errors) == 5
    assert all("file://" in err for err in file_errors)

    # Reject non-https absolute schemes: http://, ftp://, javascript:, autolink, raw http://.
    bad_scheme_md = (
        "[insecure](http://www.datagrunt.io/docs)\n"
        "[ftp](ftp://files.example.com/sample.csv)\n"
        "[xss](javascript:alert(1))\n"
        "<http://example.com/auto>\n"
        "Bare link: http://example.com/bare\n"
    )
    scheme_errors = mod.validate_generated_markdown(bad_scheme_md, allow_import_check=False)
    assert len(scheme_errors) == 5
    assert any("http:" in err for err in scheme_errors)
    assert any("ftp:" in err for err in scheme_errors)
    assert any("javascript:" in err for err in scheme_errors)


def test_antigravity_workflow_verifies_python_code_sample_imports() -> None:
    mod = _load_antigravity_workflow_module()

    valid_samples = """
```python
from datagrunt import CSVReader, PDFReader, Key, rationalize
from datagrunt.core import CSVComponents
import datagrunt.core.csv_io

# Must not be executed during static validation.
raise RuntimeError("snippet body should never execute")
```

```py {hl_lines=[1]}
from datagrunt.pdf import BBox, PDFDocument
```
"""
    assert mod.validate_generated_markdown(valid_samples) == []

    broken_top_level = """
```python
from datagrunt import CSVComponents
```
"""
    top_errors = mod.validate_generated_markdown(broken_top_level)
    assert len(top_errors) == 1
    assert "CSVComponents" in top_errors[0]
    assert "'datagrunt'" in top_errors[0]

    # Disabling import checks skips code-sample import validation.
    assert mod.validate_generated_markdown(broken_top_level, allow_import_check=False) == []

    broken_module_and_syntax = """
```python
import datagrunt.does_not_exist
from datagrunt.fake_submod import Nothing
```

```python
from datagrunt import CSVReader
def broken_syntax(
```
"""
    mixed_errors = mod.validate_generated_markdown(broken_module_and_syntax)
    assert any("datagrunt.does_not_exist" in err for err in mixed_errors)
    assert any("datagrunt.fake_submod" in err for err in mixed_errors)
    assert any("syntax error" in err.lower() for err in mixed_errors)


def test_antigravity_workflow_hardened_instructions_and_run_agent_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod = _load_antigravity_workflow_module()
    site_dir = tmp_path / "datagrunt-site"
    repo_dir = tmp_path / "datagrunt"
    repo_dir.mkdir()

    # Verify system instructions carry dynamic year/month, dashed slug, no-file://, import & anti-fabrication rules.
    march_2027 = datetime(2027, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    instructions = mod.build_system_instructions(
        version="5.4.1",
        repo_dir=repo_dir,
        site_dir=site_dir,
        now=march_2027,
    )
    assert str(site_dir / "content" / "blog" / "2027" / "march") in instructions
    assert "2026/june" not in instructions
    assert "datagrunt-5-4-1-" in instructions
    assert "file://" in instructions
    assert "src/datagrunt" in instructions
    assert "NEVER invent" in instructions

    # Wire stub Agent to verify run_agent normalizes dotted filenames and rejects invalid generated posts.
    allow_all_sentinel = object()
    blog_dir = site_dir / "content" / "blog" / "2026" / "november"
    blog_dir.mkdir(parents=True)

    written_payload: dict[str, tuple[str, str]] = {}

    class _WritingStubAgent:
        conversation_id = "conv-validate-456"

        def __init__(self, config: object) -> None:
            self.config = config

        async def __aenter__(self) -> "_WritingStubAgent":
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        async def chat(self, prompt: str) -> AsyncIterator[str]:
            filename, body = written_payload["post"]
            (blog_dir / filename).write_text(body, encoding="utf-8")

            async def _tokens() -> AsyncIterator[str]:
                yield "done"

            return _tokens()

    google_mod = ModuleType("google")
    antigravity_mod = ModuleType("google.antigravity")
    hooks_mod = ModuleType("google.antigravity.hooks")
    setattr(hooks_mod, "policy", SimpleNamespace(allow_all=lambda: allow_all_sentinel))
    setattr(antigravity_mod, "Agent", _WritingStubAgent)
    setattr(antigravity_mod, "LocalAgentConfig", lambda **kwargs: dict(kwargs))
    setattr(antigravity_mod, "hooks", hooks_mod)
    setattr(google_mod, "antigravity", antigravity_mod)

    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.antigravity", antigravity_mod)
    monkeypatch.setitem(sys.modules, "google.antigravity.hooks", hooks_mod)
    monkeypatch.delenv("USE_VERTEX", raising=False)
    monkeypatch.setenv("DATAGRUNT_SITE_DIR", str(site_dir))

    # Case 1: Agent writes dotted filename with valid links and valid imports -> normalized & accepted.
    written_payload["post"] = (
        "datagrunt-5.4.1-release-highlights.md",
        """---
title: "Datagrunt 5.4.1: Highlights"
date: 2026-11-21T17:48:43+00:00
draft: false
---

Read [the docs](/docs/csv/) and [GitHub](https://github.com/pmgraham/datagrunt).

```python
from datagrunt import CSVReader
from datagrunt.core import CSVComponents
```
""",
    )
    asyncio.run(
        mod.run_agent(
            version="5.4.1",
            commit_log="abc feat",
            file_stat="1 file",
            git_diff="diff",
            repo_dir=repo_dir,
        )
    )
    normalized_path = blog_dir / "datagrunt-5-4-1-release-highlights.md"
    assert normalized_path.is_file()
    assert not (blog_dir / "datagrunt-5.4.1-release-highlights.md").exists()

    # Case 2: Agent writes broken file:// link and bad import -> run_agent fails loud with ValueError.
    written_payload["post"] = (
        "datagrunt-5-4-1-release-highlights.md",
        """---
title: "Datagrunt 5.4.1: Broken"
date: 2026-11-21T17:48:43+00:00
draft: false
---

See [dialect.rs](file:///home/runner/work/datagrunt/rust/src/dialect.rs).

```python
from datagrunt import CSVComponents
```
""",
    )
    with pytest.raises(ValueError, match="failed validation") as exc_info:
        asyncio.run(
            mod.run_agent(
                version="5.4.1",
                commit_log="abc feat",
                file_stat="1 file",
                git_diff="diff",
                repo_dir=repo_dir,
            )
        )
    assert "file://" in str(exc_info.value)
    assert "CSVComponents" in str(exc_info.value)


def test_ci_path_filter_skips_only_non_packaged_docs_and_fails_closed(tmp_path: Path) -> None:
    ci = _load_workflow("ci.yml")
    steps = _job_steps(ci, "changes")
    filter_steps = [s for s in steps if s.get("id") == "filter"]
    assert len(filter_steps) == 1
    run_script = str(filter_steps[0].get("run", ""))
    assert "<<'PY'\n" in run_script
    py_body = run_script.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]

    repo = tmp_path / "repo"
    repo.mkdir()
    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "test",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "test",
        "GIT_COMMITTER_EMAIL": "test@example.com",
    }

    def _git(*args: str) -> str:
        res = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
            env=git_env,
            timeout=15,
        )
        return res.stdout.strip()

    def _commit(files: dict[str, str], message: str) -> str:
        for rel, content in files.items():
            target = repo / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
            _git("add", rel)
        _git("commit", "-m", message)
        return _git("rev-parse", "HEAD")

    def _classify(*, event: str, before: str = "", head: str = "") -> str:
        out_file = tmp_path / "github_output.txt"
        out_file.write_text("", encoding="utf-8")
        env = {
            **git_env,
            "GITHUB_OUTPUT": str(out_file),
            "EVENT_NAME": event,
            "PUSH_BEFORE": before,
            "HEAD_SHA": head,
        }
        res = subprocess.run(
            [sys.executable, "-c", py_body],
            cwd=repo,
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert res.returncode == 0
        return out_file.read_text(encoding="utf-8").strip()

    _git("init", "-b", "main")
    base = _commit({"README.md": "# init\n", "src/app.py": "x = 1\n"}, "init")

    # Non-packaged repo docs/meta edits skip heavy CI.
    docs_sha = _commit(
        {
            "GEMINI.md": "# playbook\n",
            "CLAUDE.md": "# playbook\n",
            ".gitignore": "*.tmp\n",
            ".github/ISSUE_TEMPLATE/bug.yml": "name: Bug\n",
            "docs/notes.md": "notes\n",
        },
        "docs only",
    )
    assert _classify(event="push", before=base, head=docs_sha) == "run_ci=false"

    # Packaged files (README.md, LICENSE, THIRD_PARTY_NOTICES.txt), Auto Document Updater,
    # workflows, source, and unknown new files must always run full CI.
    prev = docs_sha
    for rel in (
        "README.md",
        "LICENSE",
        "THIRD_PARTY_NOTICES.txt",
        "scripts/antigravity_workflow.py",
        ".github/workflows/auto-publish-site.yml",
        "unknown_new_file.txt",
    ):
        nxt = _commit({rel: f"change {rel}\n"}, f"touch {rel}")
        assert _classify(event="push", before=prev, head=nxt) == "run_ci=true", f"{rel} must trigger CI"
        prev = nxt

    # Mixed docs + code commit runs CI.
    mixed = _commit({"GEMINI.md": "updated\n", "src/app.py": "x = 2\n"}, "mixed")
    assert _classify(event="push", before=prev, head=mixed) == "run_ci=true"

    # Simulated GitHub PR merge commit (HEAD^1 = base, HEAD^2 = feature branch) skips on docs-only PR.
    _git("checkout", "-b", "docs-pr", mixed)
    _commit({"GEMINI.md": "pr edit\n"}, "pr docs edit")
    _git("checkout", "main")
    _git("merge", "--no-ff", "docs-pr", "-m", "merge docs-pr")
    assert _classify(event="pull_request") == "run_ci=false"

    # Simulated GitHub PR merge commit touching README runs CI.
    _git("checkout", "-b", "readme-pr", mixed)
    _commit({"README.md": "# updated\n"}, "pr readme edit")
    _git("checkout", "main")
    _git("merge", "--no-ff", "readme-pr", "-m", "merge readme-pr")
    assert _classify(event="pull_request") == "run_ci=true"

    # Manual workflow_dispatch and fail-closed fallbacks always run CI.
    assert _classify(event="workflow_dispatch") == "run_ci=true"
    assert _classify(event="push", before="0" * 40, head=mixed) == "run_ci=true"
    _git("checkout", docs_sha)
    assert _classify(event="pull_request") == "run_ci=true"
