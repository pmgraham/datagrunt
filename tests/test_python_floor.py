"""Datagrunt 5.0 requires Python 3.11 (#373): every statement of the floor agrees, and no gate below it is left."""

import ast
import re
import tomllib
from pathlib import Path

import pytest

import datagrunt

ROOT = Path(__file__).resolve().parents[1]
SRC = Path(datagrunt.__file__).resolve().parent
FLOOR = (3, 11)
SUPPORTED = ["3.11", "3.12", "3.13", "3.14"]


def _toml(relative: str) -> dict:
    return tomllib.loads((ROOT / relative).read_text(encoding="utf-8"))


def test_requires_python_is_the_floor():
    assert _toml("pyproject.toml")["project"]["requires-python"] == ">=3.11"


def test_classifiers_list_the_floor_and_newer():
    classifiers = _toml("pyproject.toml")["project"]["classifiers"]
    minor = re.compile(r"Programming Language :: Python :: (3\.\d+)")

    assert [match[1] for match in map(minor.fullmatch, classifiers) if match] == SUPPORTED


def test_ruff_and_mypy_target_the_floor():
    tool = _toml("pyproject.toml")["tool"]

    assert (tool["ruff"]["target-version"], tool["mypy"]["python_version"]) == ("py311", "3.11")


def test_the_native_extension_targets_the_floors_stable_abi():
    features = _toml("rust/datagrunt-python/Cargo.toml")["dependencies"]["pyo3"]["features"]

    assert [feature for feature in features if feature.startswith("abi3")] == ["abi3-py311"]


@pytest.mark.parametrize("workflow", sorted((ROOT / ".github" / "workflows").glob("*.yml")), ids=lambda path: path.name)
def test_no_workflow_runs_python_below_the_floor(workflow):
    lines = [line for line in workflow.read_text(encoding="utf-8").splitlines() if "python-version:" in line]
    versions = [version for line in lines for version in re.findall(r"\b3\.\d+\b", line)]

    assert [version for version in versions if tuple(map(int, version.split("."))) < FLOOR] == []


def _version_tuples(node: ast.Compare) -> list[tuple[int, ...]]:
    """The literal int tuples ``node`` compares, if it reads ``sys.version_info``; else none."""
    if not any(isinstance(item, ast.Attribute) and item.attr == "version_info" for item in ast.walk(node)):
        return []
    tuples = [item for item in ast.walk(node) if isinstance(item, ast.Tuple)]
    return [
        tuple(element.value for element in item.elts)
        for item in tuples
        if all(isinstance(element, ast.Constant) and isinstance(element.value, int) for element in item.elts)
    ]


def test_no_source_gates_on_a_version_at_or_below_the_floor():
    # Below 3.11 nothing runs, so a sys.version_info comparison with 3.11 or earlier is constant: a dead branch.
    found = sorted(
        f"{path.relative_to(SRC)}:{node.lineno}"
        for path in SRC.rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Compare) and any(version <= FLOOR for version in _version_tuples(node))
    )

    assert found == []
