"""GitHub Actions workflow invariants: no duplicate post-merge wheel builds or unguarded release jobs."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def _load_workflow(filename: str) -> dict:
    if not filename or not filename.endswith(".yml"):
        raise ValueError(f"Expected a .yml workflow filename, got {filename!r}")
    path = WORKFLOWS / filename
    if not path.is_file():
        raise FileNotFoundError(f"Workflow file missing: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise TypeError(f"Expected mapping in {filename}, got {type(loaded).__name__}")
    return loaded


def _triggers(doc: dict) -> dict:
    # PyYAML 1.1 parses bare `on:` as boolean True unless quoted.
    triggers = doc.get("on") if "on" in doc else doc.get(True)
    if not isinstance(triggers, dict):
        raise TypeError(f"Expected mapping for workflow triggers, got {type(triggers).__name__}")
    return triggers


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
        assert "github.event_name != 'push'" in condition, f"{job_id} missing push dedupe guard"


def test_publish_to_pypi_guards_both_build_and_publish_on_bump_success() -> None:
    publish_wf = _load_workflow("publish-to-pypi.yml")
    jobs = publish_wf["jobs"]

    for job_id in ("build", "publish"):
        condition = str(jobs[job_id].get("if", ""))
        assert "github.event.workflow_run.conclusion == 'success'" in condition, (
            f"publish-to-pypi.yml job {job_id!r} must not run when Bump Version fails"
        )
        assert "github.event_name == 'workflow_dispatch'" in condition


def test_bump_version_serializes_runs_and_skips_already_tagged_head() -> None:
    bump = _load_workflow("bump-version.yml")
    concurrency = bump.get("concurrency")
    assert isinstance(concurrency, dict)
    assert concurrency.get("cancel-in-progress") is False

    steps = bump["jobs"]["bump-version"]["steps"]
    step_ids = {step.get("id"): step for step in steps if isinstance(step, dict) and "id" in step}
    assert "tagcheck" in step_ids
    assert "git tag --points-at HEAD" in str(step_ids["tagcheck"].get("run", ""))

    bump_steps = [s for s in steps if isinstance(s, dict) and s.get("name") == "Bump version"]
    assert len(bump_steps) == 1
    assert "steps.tagcheck.outputs.already_tagged != 'true'" in str(bump_steps[0].get("if", ""))
