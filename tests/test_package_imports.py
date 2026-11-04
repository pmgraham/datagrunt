"""``import datagrunt`` imports the CSV, Excel and Parquet APIs at first use (#439), each name the object it was.

A PDF worker imports ``datagrunt``, its package's root, as it starts, and uses none of those APIs: each is
imported on the first access of one of its names (PEP 562). Each case runs in a fresh isolated interpreter, so
the modules it finds loaded are its own.
"""

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
# The CSV, Excel and Parquet stacks, and polars and pyarrow (the PDF model imports them where a table becomes a
# frame): each package, and every module inside it.
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
