"""The [pdf] extra (#373): ``import datagrunt`` never needs it; a PDF class used without it names the install.

Each case runs in a fresh interpreter, so the modules it imports (or blocks) are its own. A missing extra is
simulated as Python documents it: ``sys.modules[name] = None`` makes ``import name`` raise ModuleNotFoundError.
"""

import json
import subprocess
import sys
import textwrap

import pytest

HINT = 'Install PDF support with: uv pip install "datagrunt[pdf]"'
EXTRA_MODULES = ("pypdfium2", "PIL")  # Pillow imports as PIL
CLASS_PATHS = ("datagrunt.PDFReader", "datagrunt.PDFWriter", "datagrunt.pdf.PDFReader", "datagrunt.pdf.PDFWriter")
TIMEOUT_S = 120  # a cold interpreter imports polars, pyarrow and duckdb in about 1 s here


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
            outcome["same"] = datagrunt.PDFReader is datagrunt.pdf.PDFReader is PDFReader
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
    assert result.pop("same") is True
    assert set(result) == set(CLASS_PATHS)
    for path in CLASS_PATHS:
        assert result[path] is not None, f"{path}(...) raised nothing"
        message, name, cause = result[path]
        assert HINT in message, path
        assert path.rsplit(".", 1)[1] in message
        assert name.split(".")[0] in missing
        assert cause == "ModuleNotFoundError"


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
