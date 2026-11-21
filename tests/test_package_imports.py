"""``import datagrunt`` imports the CSV, Excel and Parquet APIs at first use (#439), each name the object it was.

A PDF worker imports ``datagrunt``, its package's root, as it starts, and uses none of those APIs: each is
imported on the first access of one of its names (PEP 562). Each case runs in a fresh isolated interpreter, so
the modules it finds loaded are its own.
"""

import importlib.util
import json
import os
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
# The subpackages ``import datagrunt.core`` bound as its attributes before #458, which it still resolves.
CORE_SUBPACKAGES = ("csv_io", "databases", "excel_io", "file_io", "parquet_io")
# Each name ``datagrunt.core`` imports at its first access (#458), and the subpackage that defines it.
CORE_NAMES = {
    "CSVEngineFactory": "datagrunt.core.csv_io",
    "CSVReaderDuckDBEngine": "datagrunt.core.csv_io",
    "CSVReaderPolarsEngine": "datagrunt.core.csv_io",
    "CSVReaderPyArrowEngine": "datagrunt.core.csv_io",
    "CSVWriterDuckDBEngine": "datagrunt.core.csv_io",
    "CSVWriterPolarsEngine": "datagrunt.core.csv_io",
    "CSVWriterPyArrowEngine": "datagrunt.core.csv_io",
    "CSVEngineProperties": "datagrunt.core.csv_io",
    "CSVComponents": "datagrunt.core.csv_io",
    "CSVColumnNameNormalizer": "datagrunt.core.csv_io",
    "CSVColumns": "datagrunt.core.csv_io",
    "CSVDelimiter": "datagrunt.core.csv_io",
    "CSVDialect": "datagrunt.core.csv_io",
    "CSVRows": "datagrunt.core.csv_io",
    "CSVStringSample": "datagrunt.core.csv_io",
    "DuckDBQueries": "datagrunt.core.databases",
    "ExcelComponents": "datagrunt.core.excel_io",
    "ExcelEngineFactory": "datagrunt.core.excel_io",
    "ExcelEngineProperties": "datagrunt.core.excel_io",
    "ExcelReaderEngine": "datagrunt.core.excel_io",
    "ExcelWriterEngine": "datagrunt.core.excel_io",
    "FileProperties": "datagrunt.core.file_io",
    "ParquetComponents": "datagrunt.core.parquet_io",
    "ParquetEngineFactory": "datagrunt.core.parquet_io",
    "ParquetEngineProperties": "datagrunt.core.parquet_io",
    "ParquetReaderEngine": "datagrunt.core.parquet_io",
    "ParquetWriterEngine": "datagrunt.core.parquet_io",
}
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
CORE_STACKS = (
    "datagrunt.core.csv_io",
    "datagrunt.core.databases",
    "datagrunt.core.excel_io",
    "datagrunt.core.file_io",
    "datagrunt.core.parquet_io",
    "duckdb",
    "fastexcel",
    "polars",
    "pyarrow",
    "xlsxwriter",
)
TIMEOUT_S = 120


def run_python(code: str) -> dict:
    """Run ``code`` in a fresh isolated interpreter; the JSON object it prints last."""
    body = textwrap.dedent(code)
    pythonpath = os.environ.get("PYTHONPATH", "")
    if pythonpath:
        entries = [entry for entry in pythonpath.split(os.pathsep) if entry]
        body = f"import sys\nsys.path[:0] = {entries!r}\n{body}"
    completed = subprocess.run(
        [sys.executable, "-I", "-c", body],
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


# The rest of a race's code, after its ``setup`` and its ``targets``, ``use`` and ``owner``: one thread per target,
# barrier-synced so each makes its first use of its target at once ("import" imports the module; else it is an
# attribute of ``owner``). Every error a thread raises is reported, and so is a thread still running at the bound.
RACE = """
import importlib, json, threading

barrier, errors = threading.Barrier(len(targets), timeout=60), []


def first_use(target):
    try:
        barrier.wait()
        importlib.import_module(target) if use == "import" else getattr(sys.modules[owner], target)
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


def race(setup: str, use: str, targets: tuple[str, ...], owner: str = "datagrunt") -> list[list[str]]:
    """The errors of each round that had any: ``RACE_ROUNDS`` fresh interpreters, each racing ``targets``."""
    code = f"import sys\n{setup}\ntargets, use, owner = {targets!r}, {use!r}, {owner!r}\n{RACE}"
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


def test_import_datagrunt_core_loads_none_of_its_subpackages_and_dir_lists_every_name():
    result = run_python(
        """
        import json, sys
        import datagrunt.core
        print(json.dumps({
            "loaded": sorted(sys.modules),
            "dir": dir(datagrunt.core),
            "all": datagrunt.core.__all__,
        }))
        """
    )

    assert inside(result["loaded"], CORE_STACKS) == []
    assert set(result["all"]) == set(CORE_NAMES)
    assert set(result["all"]) | set(CORE_SUBPACKAGES) <= set(result["dir"])


@pytest.mark.parametrize(
    ("subpackage", "unloaded"),
    [
        ("datagrunt.core.excel_io", ("datagrunt.core.parquet_io",)),
        ("datagrunt.core.parquet_io", ("datagrunt.core.excel_io",)),
        (
            "datagrunt.core.file_io",
            (
                "datagrunt.core.csv_io",
                "datagrunt.core.databases",
                "datagrunt.core.excel_io",
                "datagrunt.core.parquet_io",
            ),
        ),
    ],
    ids=["excel-leaves-parquet-unloaded", "parquet-leaves-excel-unloaded", "file-io-leaves-siblings-unloaded"],
)
def test_importing_one_core_subpackage_does_not_load_unrelated_siblings(subpackage, unloaded):
    result = run_python(
        f"""
        import importlib, json, sys
        importlib.import_module({subpackage!r})
        print(json.dumps({{"loaded": sorted(sys.modules)}}))
        """
    )

    assert inside(result["loaded"], unloaded) == []


@pytest.mark.parametrize(
    "name",
    ["CSVEngineFactory", "DuckDBQueries", "ExcelEngineFactory", "FileProperties", "ParquetEngineFactory"],
)
def test_core_names_first_access_imports_subpackage_and_caches_attribute(name):
    module = CORE_NAMES[name]
    result = run_python(
        f"""
        import json, sys
        import datagrunt.core
        before_mod = {module!r} in sys.modules
        before_attr = {name!r} in vars(datagrunt.core)
        value = getattr(datagrunt.core, {name!r})
        from datagrunt.core import {name} as imported
        print(json.dumps({{
            "before_mod": before_mod,
            "before_attr": before_attr,
            "kept": vars(datagrunt.core).get({name!r}) is value,
            "same": value is imported is getattr(sys.modules[{module!r}], {name!r}),
        }}))
        """
    )

    assert result == {"before_mod": False, "before_attr": False, "kept": True, "same": True}


@pytest.mark.parametrize("name", CORE_SUBPACKAGES)
def test_core_subpackage_is_an_attribute_as_before(name):
    result = run_python(
        f"""
        import json, sys
        import datagrunt.core
        print(json.dumps({{"same": getattr(datagrunt.core, {name!r}) is sys.modules["datagrunt.core.{name}"]}}))
        """
    )

    assert result["same"] is True


def test_core_star_import_binds_every_name_in_a_fresh_interpreter():
    result = run_python(
        f"""
        import importlib, json
        import datagrunt.core
        from datagrunt.core import *
        mapping = {CORE_NAMES!r}
        print(json.dumps({{
            name: (
                globals()[name]
                is getattr(datagrunt.core, name)
                is getattr(importlib.import_module(mapping[name]), name)
            )
            for name in datagrunt.core.__all__
        }}))
        """
    )

    assert set(result) == set(CORE_NAMES)
    assert set(result.values()) == {True}


def test_core_exports_dir_and_unknown_attribute_error():
    import datagrunt.core as core

    assert set(core.__all__) == set(CORE_NAMES)
    assert set(core.__all__) | set(CORE_SUBPACKAGES) <= set(dir(core))
    for name, module_name in CORE_NAMES.items():
        value = getattr(core, name)
        assert value is getattr(importlib.import_module(module_name), name)
    for subpkg in CORE_SUBPACKAGES:
        expected = importlib.import_module(f"datagrunt.core.{subpkg}")
        assert getattr(core, subpkg) is expected
        assert core.__getattr__(subpkg) is expected
    with pytest.raises(AttributeError, match="module 'datagrunt.core' has no attribute 'does_not_exist'"):
        getattr(core, "does_not_exist")


@pytest.mark.parametrize(
    ("setup", "targets"),
    [
        ("", ("datagrunt.core.excel_io", "datagrunt.core.parquet_io")),
        ("import datagrunt", ("datagrunt.core.excel_io", "datagrunt.core.parquet_io")),
        ("", ("datagrunt.core.csv_io", "datagrunt.core.excel_io", "datagrunt.core.parquet_io")),
        ("", ("datagrunt.core.csv_io", "datagrunt.core.databases")),
    ],
    ids=[
        "core-excel-parquet-cold",
        "core-excel-parquet-after-import-datagrunt",
        "core-csv-excel-parquet-cold",
        "core-csv-databases-cold",
    ],
)
def test_threads_importing_core_subpackages_at_once_all_succeed(setup, targets):
    assert race(setup, "import", targets) == []


@pytest.mark.parametrize(
    "targets",
    [
        ("ExcelEngineFactory", "ParquetEngineFactory"),
        ("CSVEngineFactory", "ExcelEngineFactory", "ParquetEngineFactory"),
    ],
    ids=["core-attr-excel-parquet", "core-attr-csv-excel-parquet"],
)
def test_threads_first_accessing_core_names_at_once_all_succeed(targets):
    assert race("import datagrunt.core", "attribute", targets, owner="datagrunt.core") == []
