"""``import datagrunt`` imports the CSV, Excel and Parquet APIs at first use (#439), each name the object it was.

A PDF worker imports ``datagrunt``, its package's root, as it starts, and uses none of those APIs: each is
imported on the first access of one of its names (PEP 562). Each case runs in a fresh isolated interpreter, so
the modules it finds loaded are its own.
"""

import importlib.util
import json
import subprocess
import sys
import textwrap

import pytest

# Each name the top-level package imports at its first access, and the module that defines it.
API_NAMES = {
    "CSVReader": "datagrunt.csv_api",
    "CSVWriter": "datagrunt.csv_api",
    "ExcelReader": "datagrunt.excel_api",
    "ExcelWriter": "datagrunt.excel_api",
    "ParquetReader": "datagrunt.parquet_api",
    "ParquetWriter": "datagrunt.parquet_api",
}
# The subpackages ``import datagrunt`` bound as its attributes before #439, which it still resolves.
SUBPACKAGES = ("core", "csv_api", "excel_api", "parquet_api")
# The CSV, Excel and Parquet stacks, polars and pyarrow (the PDF model imports them where a table becomes a
# frame), and the [rationalize] extra's pydantic and httpx (#360): each package, and every module inside it.
STACKS = (
    "datagrunt.core",
    "datagrunt.csv_api",
    "datagrunt.excel_api",
    "datagrunt.parquet_api",
    "duckdb",
    "fastexcel",
    "polars",
    "pyarrow",
    "xlsxwriter",
    "pydantic",
    "httpx",
)
TIMEOUT_S = 120


def run_python(code: str) -> dict:
    """Run ``code`` in a fresh isolated interpreter; the JSON object it prints last."""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def inside(names: list[str], packages: tuple[str, ...]) -> list[str]:
    """Those of the module ``names`` that are one of ``packages``, or inside one."""
    return [name for name in names if any(name == package or name.startswith(f"{package}.") for package in packages)]


def test_import_datagrunt_loads_none_of_the_stacks_and_dir_lists_every_name():
    result = run_python(
        """
        import json, sys
        import datagrunt
        print(json.dumps({"loaded": sorted(sys.modules), "dir": dir(datagrunt), "all": datagrunt.__all__}))
        """
    )

    assert inside(result["loaded"], STACKS) == []
    assert set(result["all"]) | set(SUBPACKAGES) <= set(result["dir"])


@pytest.mark.parametrize("package", ["pydantic", "httpx"])
def test_the_rationalize_extra_is_installed_so_the_stack_checks_can_fail(package):
    # Without the extra, a module importing pydantic or httpx would never show up in sys.modules, and the stack
    # checks above and below would pass vacuously: the suite runs with `.[dev,pdf,rationalize]` installed.
    assert importlib.util.find_spec(package) is not None, f'{package} is missing: uv pip install -e ".[rationalize]"'


def test_import_datagrunt_rationalization_loads_none_of_the_stacks_and_its_key_is_the_packages():
    result = run_python(
        """
        import json, sys
        import datagrunt.rationalization
        loaded = sorted(sys.modules)
        import datagrunt
        from datagrunt import Key
        print(json.dumps({
            "loaded": loaded,
            "same": datagrunt.Key is Key is datagrunt.rationalization.Key,
            "exported": "Key" in datagrunt.__all__,
        }))
        """
    )

    assert inside(result["loaded"], STACKS) == []
    assert result["same"] is True
    assert result["exported"] is True


@pytest.mark.parametrize("name", list(API_NAMES))
def test_a_names_first_access_imports_its_api_and_is_the_apis_object(name):
    module = API_NAMES[name]
    result = run_python(
        f"""
        import json, sys
        import datagrunt
        before = {module!r} in sys.modules
        value = datagrunt.{name}
        from datagrunt import {name} as imported
        print(json.dumps({{
            "before": before,
            "same": value is imported is getattr(sys.modules[{module!r}], {name!r}),
            "module": value.__module__,
        }}))
        """
    )

    assert result["before"] is False
    assert result["same"] is True
    assert result["module"].startswith(f"{module}.")


@pytest.mark.parametrize("name", SUBPACKAGES)
def test_a_subpackage_is_an_attribute_of_the_package_as_before(name):
    result = run_python(
        f"""
        import json, sys
        import datagrunt
        print(json.dumps({{"same": datagrunt.{name} is sys.modules["datagrunt.{name}"]}}))
        """
    )

    assert result["same"] is True


def test_star_import_binds_every_name_in_a_fresh_interpreter():
    result = run_python(
        """
        import json
        import datagrunt
        from datagrunt import *
        print(json.dumps({name: globals()[name] is getattr(datagrunt, name) for name in datagrunt.__all__}))
        """
    )

    assert set(result.values()) == {True}


@pytest.mark.parametrize("name", list(API_NAMES))
def test_a_names_first_access_keeps_it_as_the_packages_attribute(name):
    result = run_python(
        f"""
        import json
        import datagrunt
        before = {name!r} in vars(datagrunt)
        value = datagrunt.{name}
        print(json.dumps({{"before": before, "kept": vars(datagrunt).get({name!r}) is value}}))
        """
    )

    assert result == {"before": False, "kept": True}


# A fresh interpreter's first lines: datagrunt's Rust extension, ``datagrunt._native``, cannot be imported in it.
BLOCK_NATIVE = """
import importlib.abc, json, sys


class Blocked(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "datagrunt._native":
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


sys.meta_path.insert(0, Blocked())
"""


def test_without_the_rust_extension_import_datagrunt_works_and_csv_reader_raises_import_error():
    result = run_python(
        BLOCK_NATIVE
        + """
import datagrunt
errors = []
for _ in range(2):
    try:
        datagrunt.CSVReader
    except ImportError as exc:
        errors.append(str(exc))
print(json.dumps({"errors": errors, "kept": "CSVReader" in vars(datagrunt)}))
"""
    )

    assert len(result["errors"]) == 2  # no pure-Python fallback, at the first access or a later one
    assert all("_native" in error for error in result["errors"])
    assert result["kept"] is False


# The rest of a race's code, after its ``setup`` and its ``targets`` and ``use``: one thread per target, barrier-synced
# so each makes its first use of its target at once ("import" imports the module; else it is an attribute of
# datagrunt). Every error a thread raises is reported, and so is a thread still running at the bound.
RACE = """
import importlib, json, threading

barrier, errors = threading.Barrier(len(targets), timeout=60), []


def first_use(target):
    try:
        barrier.wait()
        importlib.import_module(target) if use == "import" else getattr(sys.modules["datagrunt"], target)
    except BaseException as exc:
        errors.append(f"{target}: {type(exc).__name__}: {exc}")


threads = [threading.Thread(target=first_use, args=(target,), daemon=True) for target in targets]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join(timeout=90)
errors += [f"{thread.name} is still running" for thread in threads if thread.is_alive()]
print(json.dumps({"errors": errors}))
"""
# Fresh interpreters per race: a deadlock between two first imports shows in nearly every one.
RACE_ROUNDS = 10


def race(setup: str, use: str, targets: tuple[str, ...]) -> list[list[str]]:
    """The errors of each round that had any: ``RACE_ROUNDS`` fresh interpreters, each racing ``targets``."""
    code = f"import sys\n{setup}\ntargets, use = {targets!r}, {use!r}\n{RACE}"
    rounds = (run_python(code)["errors"] for _ in range(RACE_ROUNDS))
    return [errors for errors in rounds if errors]


@pytest.mark.parametrize(
    "targets",
    [("ExcelReader", "ParquetReader"), ("CSVReader", "ExcelReader", "ParquetReader")],
    ids=["excel-parquet", "csv-excel-parquet"],
)
def test_threads_first_accessing_api_names_at_once_all_succeed(targets):
    assert race("import datagrunt", "attribute", targets) == []


@pytest.mark.parametrize(
    ("setup", "targets"),
    [
        ("", ("datagrunt.excel_api", "datagrunt.parquet_api")),
        ("import datagrunt", ("datagrunt.excel_api", "datagrunt.parquet_api")),
        ("", ("datagrunt.csv_api", "datagrunt.excel_api", "datagrunt.parquet_api")),
    ],
    ids=["excel-parquet-cold", "excel-parquet-after-import-datagrunt", "csv-excel-parquet-cold"],
)
def test_threads_importing_api_packages_at_once_all_succeed(setup, targets):
    assert race(setup, "import", targets) == []
