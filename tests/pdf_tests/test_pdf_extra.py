"""The [pdf] extra (#373): ``import datagrunt`` never needs it; a PDF class used without it names the install.

Each case runs in a fresh interpreter, so the modules it imports (or blocks) are its own. A missing extra is
simulated as Python documents it: ``sys.modules[name] = None`` makes ``import name`` raise ModuleNotFoundError.
Deleting that entry installs it again, as a notebook's ``%pip install`` does mid-session. An extra that goes
missing after construction is simulated in the workers only: a worker installs its caller's ``sys.path``, so a
directory put first on it, holding a package of the extra's name that fails to import as a missing one does,
hides the real one from every worker started after.
"""

import importlib.metadata
import json
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from pdf_tests.builders import PageSpec, RasterImage, TextRun, build_pdf, solid_png

HINT = 'Install PDF support with: uv pip install "datagrunt[pdf]"'
EXTRA_MODULES = ("pypdfium2", "PIL")  # Pillow imports as PIL
PACKAGES = {"pypdfium2": "pypdfium2", "PIL": "Pillow"}
CLASS_PATHS = ("datagrunt.PDFReader", "datagrunt.PDFWriter", "datagrunt.pdf.PDFReader", "datagrunt.pdf.PDFWriter")
TIMEOUT_S = 120  # a cold interpreter imports polars, pyarrow and duckdb in about 1 s here
PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"
# v4's PDF libraries, which 5.0 removed (#373), by their distribution and import names.
REMOVED = frozenset({"pymupdf", "pdfplumber", "pytesseract", "fitz"})


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


def blocking(modules: tuple[str, ...]) -> str:
    """Source that makes each of ``modules`` unimportable, as if it were not installed."""
    return "import sys\n" + "".join(f"sys.modules[{name!r}] = None\n" for name in modules)


def test_import_datagrunt_loads_no_pdf_extra_module():
    result = run_python(
        """
        import json, sys
        import datagrunt
        print(json.dumps(sorted(name for name in sys.modules if name.split(".")[0] in ("pypdfium2", "PIL"))))
        """
    )

    assert result == []


@pytest.mark.parametrize("missing", [("pypdfium2",), ("PIL",), EXTRA_MODULES], ids=["pypdfium2", "Pillow", "both"])
def test_without_the_extra_import_works_and_first_use_raises_the_hint(missing):
    result = run_python(
        blocking(missing)
        + textwrap.dedent(
            """
            import json
            import datagrunt
            import datagrunt.pdf
            from datagrunt import *
            from datagrunt import PDFReader, PDFWriter

            outcome = {"star": sorted(name for name in datagrunt.__all__ if name not in globals())}
            # Each access builds its stand-in afresh (none is kept), each the same class by name, module and hint.
            stand_ins = (datagrunt.PDFReader, datagrunt.pdf.PDFReader, PDFReader)
            outcome["alike"] = len({(cls.__name__, cls.__module__, cls.__doc__) for cls in stand_ins}) == 1
            classes = {
                "datagrunt.PDFReader": datagrunt.PDFReader,
                "datagrunt.PDFWriter": datagrunt.PDFWriter,
                "datagrunt.pdf.PDFReader": datagrunt.pdf.PDFReader,
                "datagrunt.pdf.PDFWriter": datagrunt.pdf.PDFWriter,
            }
            for path, cls in classes.items():
                try:
                    cls("report.pdf")
                except ImportError as exc:
                    outcome[path] = [str(exc), exc.name, type(exc.__cause__).__name__]
                else:
                    outcome[path] = None
            print(json.dumps(outcome))
            """
        )
    )

    assert result.pop("star") == []
    assert result.pop("alike") is True
    assert set(result) == set(CLASS_PATHS)
    for path in CLASS_PATHS:
        assert result[path] is not None, f"{path}(...) raised nothing"
        message, name, cause = result[path]
        assert HINT in message, path
        assert path.rsplit(".", 1)[1] in message
        assert all(PACKAGES[module] in message for module in missing), message
        assert name.split(".")[0] in missing
        assert cause == "ModuleNotFoundError"


@pytest.mark.parametrize("missing", [("pypdfium2",), ("PIL",), EXTRA_MODULES], ids=["pypdfium2", "Pillow", "both"])
def test_without_the_extra_the_bundle_functions_raise_the_hint_when_called(missing):
    result = run_python(
        blocking(missing)
        + textwrap.dedent(
            """
            import json
            import datagrunt.pdf
            from datagrunt.pdf import *

            outcome = {"star": sorted(name for name in datagrunt.pdf.__all__ if name not in globals())}
            for name in ("load_bundle", "write_bundle"):
                function = getattr(datagrunt.pdf, name)
                try:
                    function("bundle")
                except ImportError as exc:
                    outcome[name] = [function.__name__, str(exc), exc.name, type(exc.__cause__).__name__]
                else:
                    outcome[name] = None
            print(json.dumps(outcome))
            """
        )
    )

    assert result.pop("star") == []
    for name in ("load_bundle", "write_bundle"):
        assert result[name] is not None, f"{name}(...) raised nothing"
        function_name, message, missing_name, cause = result[name]
        assert function_name == name
        assert HINT in message and name in message
        assert all(PACKAGES[module] in message for module in missing), message
        assert missing_name.split(".")[0] in missing
        assert cause == "ModuleNotFoundError"


# What a later install adds: a package of the extra that was not installed, or a module of Pillow it lacked.
LATER_INSTALLS = {
    "pypdfium2": ("pypdfium2",),
    "Pillow": ("PIL",),
    "both": EXTRA_MODULES,
    "Pillow's PIL.Image": ("PIL.Image",),
}


@pytest.mark.parametrize("installed", list(LATER_INSTALLS.values()), ids=list(LATER_INSTALLS))
def test_the_extra_installed_after_a_stand_in_was_taken_loads_the_real_names(installed):
    result = run_python(
        blocking(installed)
        + f"INSTALLED = {installed!r}\n"
        + textwrap.dedent(
            """
            import importlib, json, sys
            import datagrunt
            import datagrunt.pdf
            from datagrunt.pdf._extra import LAZY_CLASSES, LAZY_FUNCTIONS

            modules = {**LAZY_CLASSES, **LAZY_FUNCTIONS}
            paths = [("datagrunt.pdf", name) for name in modules] + [("datagrunt", name) for name in LAZY_CLASSES]

            def refusal(value):
                try:
                    value("report.pdf")
                except Exception as exc:
                    return [type(exc).__name__, "datagrunt[pdf]" in str(exc)]
                return None

            before = {f"{package}.{name}": refusal(getattr(sys.modules[package], name)) for package, name in paths}
            for module in INSTALLED:
                del sys.modules[module]
            after = {}
            for package, name in paths:
                first, second = getattr(sys.modules[package], name), getattr(sys.modules[package], name)
                real = getattr(importlib.import_module(modules[name]), name)
                after[f"{package}.{name}"] = [first is real, second is first]
            print(json.dumps({"before": before, "after": after}))
            """
        )
    )

    expected = [f"datagrunt.pdf.{name}" for name in ("PDFReader", "PDFWriter", "load_bundle", "write_bundle")]
    expected += ["datagrunt.PDFReader", "datagrunt.PDFWriter"]
    assert result["before"] == dict.fromkeys(expected, ["ImportError", True])
    assert result["after"] == dict.fromkeys(expected, [True, True])


def test_a_package_installed_after_its_directory_was_listed_is_found(tmp_path):
    # The import system's finders keep each sys.path directory's listing, read again only when the directory's
    # modification time changes: an install within the file system's time granularity leaves it unchanged.
    site = tmp_path / "site"
    site.mkdir()
    result = run_python(
        f"""
        import importlib.util, json, os, sys
        from pathlib import Path
        from types import MappingProxyType
        from datagrunt.pdf import _extra
        from datagrunt.pdf.reader import PDFReader

        site = Path({str(site)!r})
        sys.path.insert(0, str(site))
        _extra._EXTRA_PACKAGES = MappingProxyType({{"later_package": "later-package"}})  # not installed yet
        before = _extra.load("PDFReader")
        listed = site.stat()
        (site / "later_package").mkdir()
        (site / "later_package" / "__init__.py").write_text("", encoding="utf-8")
        os.utime(site, ns=(listed.st_atime_ns, listed.st_mtime_ns))
        unlisted = importlib.util.find_spec("later_package") is None
        print(json.dumps([before is PDFReader, unlisted, _extra.load("PDFReader") is PDFReader]))
        """
    )

    assert result == [False, True, True]


@pytest.fixture(scope="module")
def figure_pdf(tmp_path_factory) -> Path:
    """A one-page PDF of text and one figure: its pages, its figure and its render each need a worker."""
    page = PageSpec(
        items=(
            TextRun("Quarterly Report", 72, 72, size=24),
            TextRun("This is body text for testing the figure below it.", 72, 120),
            RasterImage(72, 160, 144, 144, solid_png(120, 120, (0.8, 0.1, 0.1))),
        )
    )
    return build_pdf(tmp_path_factory.mktemp("extra") / "figure.pdf", [page])


def shadowing(tmp_path: Path, module: str) -> Path:
    """A directory holding a package ``module`` whose import fails as a missing package's does."""
    root = tmp_path / "shadow"
    (root / module).mkdir(parents=True)
    failure = f"raise ModuleNotFoundError({f'No module named {module!r}'!r}, name={module!r})\n"
    (root / module / "__init__.py").write_text(failure, encoding="utf-8")
    return root


# Each worker: what runs while the extra is there (the PDF read first where a writer needs its document), then the
# call that starts that worker after it is gone.
WORKER_CALLS = {
    "pages": ("reader = datagrunt.PDFReader(pdf)", "reader.to_dataframe()"),
    "figures": ("writer = datagrunt.PDFWriter(pdf); writer.write_json(out / 'doc.json')", "writer.extract_images(out)"),
    "render": (
        "writer = datagrunt.PDFWriter(pdf); writer.write_json(out / 'doc.json')",
        "writer.render_pages_as_images(out)",
    ),
}


def _raised_after_shadowing(pdf: Path, out: Path, shadow: Path, setup: str, call: str) -> list | None:
    """What ``call`` raises once ``shadow`` heads the caller's sys.path (so every later worker's): or None."""
    return run_python(
        f"""
        import json, sys
        from pathlib import Path
        import datagrunt
        from datagrunt.pdf import DatagruntPDFError
        pdf, out = Path({str(pdf)!r}), Path({str(out)!r})
        {setup}
        sys.path.insert(0, {str(shadow)!r})
        try:
            {call}
        except Exception as exc:
            found = [type(exc).__name__, isinstance(exc, DatagruntPDFError), str(exc), getattr(exc, "name", None)]
            print(json.dumps(found))
        else:
            print(json.dumps(None))
        """
    )


@pytest.mark.parametrize("worker", sorted(WORKER_CALLS))
@pytest.mark.parametrize("module", EXTRA_MODULES, ids=["pypdfium2", "Pillow"])
def test_an_extra_package_gone_after_construction_raises_the_hint_from_the_worker(tmp_path, figure_pdf, worker, module):
    setup, call = WORKER_CALLS[worker]

    raised = _raised_after_shadowing(figure_pdf, tmp_path / "out", shadowing(tmp_path, module), setup, call)

    assert raised is not None, f"{call} raised nothing"
    kind, is_pdf_error, message, name = raised
    assert (kind, is_pdf_error) == ("ImportError", False), message
    assert HINT in message and PACKAGES[module] in message
    assert name == module


def test_a_worker_failing_on_another_import_is_not_the_hint(tmp_path, figure_pdf):
    # A module a page worker imports that is not the extra: its failure in a worker stays the worker's failure,
    # unmasked. statistics, which the page worker's layout and vision import and the caller never does (polars was
    # the example until no worker imported it, #439).
    setup, call = WORKER_CALLS["pages"]

    raised = _raised_after_shadowing(figure_pdf, tmp_path / "out", shadowing(tmp_path, "statistics"), setup, call)

    assert raised is not None
    kind, is_pdf_error, message, _name = raised
    assert is_pdf_error, kind
    assert "statistics" in message and HINT not in message


def test_every_worker_protocol_raises_an_e_reply():
    from datagrunt.pdf import _loaders, _process, pipeline, render

    assert pipeline._ERROR == _loaders._ERROR == render._ERROR == _process.ERROR == b"E"


def test_without_the_extra_csv_and_parquet_still_work(tmp_path):
    source = tmp_path / "data.csv"
    source.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    result = run_python(
        blocking(EXTRA_MODULES)
        + textwrap.dedent(
            f"""
            import json
            import datagrunt
            reader = datagrunt.CSVReader({str(source)!r})
            frame = reader.to_dataframe()
            parquet = {str(tmp_path / "data.parquet")!r}
            datagrunt.CSVWriter({str(source)!r}).write_parquet(out_filename=parquet)
            rows = datagrunt.ParquetReader(parquet).to_dataframe().shape[0]
            print(json.dumps([reader.delimiter, frame.shape[0], rows]))
            """
        )
    )

    assert result == [",", 2, 2]


def test_another_import_failure_propagates_unmasked():
    # Only a missing extra becomes the hint: a broken datagrunt module surfaces as itself.
    result = run_python(
        blocking(("datagrunt.pdf.pipeline",))
        + textwrap.dedent(
            """
            import json
            import datagrunt
            try:
                datagrunt.PDFReader
            except ImportError as exc:
                print(json.dumps([type(exc).__name__, exc.name, "datagrunt[pdf]" in str(exc)]))
            else:
                print(json.dumps(None))
            """
        )
    )

    assert result == ["ModuleNotFoundError", "datagrunt.pdf.pipeline", False]


def _pyproject() -> dict:
    """pyproject.toml, parsed."""
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def test_pdf_extra_is_exactly_pypdfium2_and_pillow():
    assert _pyproject()["project"]["optional-dependencies"]["pdf"] == ["pypdfium2>=5.9.0", "Pillow>=12.2.0"]


def test_core_dependencies_are_unchanged():
    assert _pyproject()["project"]["dependencies"] == [
        "duckdb>=1.5.3",
        "polars>=1.41.2",
        "pyarrow>=24.0.0",
        "XlsxWriter>=3.2.9",
        "fastexcel>=0.20.2",
    ]


def test_no_mypy_override_names_a_removed_library():
    overrides = _pyproject()["tool"]["mypy"]["overrides"]
    modules = [module for override in overrides for module in override["module"]]

    assert [module for module in modules if module.split(".")[0] in REMOVED] == []


def test_installed_distribution_requires_no_removed_library():
    requirements = [Requirement(line) for line in importlib.metadata.requires("datagrunt") or []]
    pdf_extra = sorted(
        f"{canonicalize_name(requirement.name)}{requirement.specifier}"
        for requirement in requirements
        if requirement.marker is not None and requirement.marker.evaluate({"extra": "pdf"})
    )

    assert [requirement.name for requirement in requirements if canonicalize_name(requirement.name) in REMOVED] == []
    assert pdf_extra == ["pillow>=12.2.0", "pypdfium2>=5.9.0"]
