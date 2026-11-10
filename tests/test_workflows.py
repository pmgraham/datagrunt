"""GitHub Actions workflow invariants: deduplication, release guards, allowlist + SHA pinning, and multi-core CI."""

import re
import tomllib
from pathlib import Path

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

    # Core `test` job stays unguarded on `push` so merged `main` and README badge stay verified.
    assert "if" not in jobs["test"]

    # Cross-OS wheel matrix, 3-version Python matrix, and advisory audit already ran on the PR
    # (and Publish to PyPI builds release wheels after Bump Version).
    for job_id in ("python-compat", "build", "audit"):
        condition = str(jobs[job_id].get("if", ""))
        assert "github.event_name != " in condition and "push" in condition, f"{job_id} missing push dedupe guard"


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
