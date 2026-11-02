"""The public PDF surface (5.0, #373): ``datagrunt`` and ``datagrunt.pdf`` export v5's reader, writer and model."""

import ast
import importlib.util
from pathlib import Path

import pytest

import datagrunt
import datagrunt.pdf
import datagrunt.pdf.elements
import datagrunt.pdf.exceptions
import datagrunt.pdf.reader
import datagrunt.pdf.writer

# Pinned literals (plan order): a name added to or dropped from a public surface must show up here, not just
# stay self-consistent.
V5_MODEL_NAMES = [
    "BBox",
    "PageQuality",
    "DocumentMetadata",
    "Heading",
    "Paragraph",
    "KeyValue",
    "Table",
    "BulletList",
    "Figure",
    "PageImage",
    "Page",
    "Section",
    "PDFDocument",
    "DocumentElement",
    "HiddenReason",
    "HIDDEN_REASONS",
    "PageStatus",
    "PageRoute",
    "ExtractionMethod",
    "make_element_id",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
]
PDF_PACKAGE_NAMES = [*V5_MODEL_NAMES, "PDFReader", "PDFWriter"]
# Spec §2.1: the classes, the element types and the exceptions (rationalize's names arrive in 5.1).
TOP_LEVEL_PDF_NAMES = [
    "PDFReader",
    "PDFWriter",
    "PDFDocument",
    "BBox",
    "DocumentMetadata",
    "PageQuality",
    "Page",
    "Section",
    "Heading",
    "Paragraph",
    "KeyValue",
    "Table",
    "BulletList",
    "Figure",
    "PageImage",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
]
TOP_LEVEL_NAMES = [
    "CSVReader",
    "CSVWriter",
    "ExcelReader",
    "ExcelWriter",
    "ParquetReader",
    "ParquetWriter",
    *TOP_LEVEL_PDF_NAMES,
]
EXCEPTION_NAMES = {"DatagruntPDFError", "PDFEncryptedError", "PDFCorruptError"}
CLASS_MODULES = {"PDFReader": datagrunt.pdf.reader, "PDFWriter": datagrunt.pdf.writer}


def _defining_module(name: str):
    if name in CLASS_MODULES:
        return CLASS_MODULES[name]
    return datagrunt.pdf.exceptions if name in EXCEPTION_NAMES else datagrunt.pdf.elements


def test_pdf_package_exports_exactly_the_v5_names():
    assert datagrunt.pdf.__all__ == PDF_PACKAGE_NAMES


def test_top_level_package_exports_exactly_the_pinned_names():
    assert datagrunt.__all__ == TOP_LEVEL_NAMES


@pytest.mark.parametrize("name", PDF_PACKAGE_NAMES)
def test_pdf_package_name_is_the_defining_modules_object(name):
    assert getattr(datagrunt.pdf, name) is getattr(_defining_module(name), name)


@pytest.mark.parametrize("name", TOP_LEVEL_PDF_NAMES)
def test_top_level_pdf_name_is_the_pdf_packages_object(name):
    assert getattr(datagrunt, name) is getattr(datagrunt.pdf, name)


@pytest.mark.parametrize("name", sorted(set(V5_MODEL_NAMES) - set(TOP_LEVEL_PDF_NAMES)))
def test_model_helpers_stay_in_the_pdf_package(name):
    assert name not in datagrunt.__all__
    assert not hasattr(datagrunt, name)


@pytest.mark.parametrize("name", ["PDFReader", "PDFWriter"])
def test_top_level_reader_and_writer_are_v5(name):
    cls = getattr(datagrunt, name)
    assert cls is getattr(CLASS_MODULES[name], name)
    assert cls.__module__ == f"datagrunt.pdf.{name[3:].lower()}"


@pytest.mark.parametrize("package", [datagrunt, datagrunt.pdf], ids=["datagrunt", "datagrunt.pdf"])
def test_lazy_classes_are_listed_by_dir(package):
    assert {"PDFReader", "PDFWriter"} <= set(dir(package))


@pytest.mark.parametrize("package", [datagrunt, datagrunt.pdf], ids=["datagrunt", "datagrunt.pdf"])
def test_star_import_binds_every_exported_name(package):
    namespace: dict[str, object] = {}
    exec(f"from {package.__name__} import *", namespace)

    assert {name: namespace[name] for name in package.__all__} == {
        name: getattr(package, name) for name in package.__all__
    }


@pytest.mark.parametrize("package", [datagrunt, datagrunt.pdf], ids=["datagrunt", "datagrunt.pdf"])
def test_unknown_name_is_an_attribute_error(package):
    with pytest.raises(AttributeError, match="PDFBatchWriter"):
        package.PDFBatchWriter
    assert "PDFBatchWriter" not in package.__all__


# v4's packages and its PDF libraries, which the 5.0 flip (#373) deleted.
V4_PACKAGES = ("datagrunt.pdf_api", "datagrunt.core.pdf_io")
V4_LIBRARIES = ("fitz", "pymupdf", "pdfplumber", "pytesseract")
SRC = Path(datagrunt.__file__).resolve().parent
TESTS = Path(__file__).resolve().parents[1]
# datagrunt.core's names after the flip: its v4 PDF names went with core.pdf_io.
CORE_NAMES = [
    "CSVEngineFactory",
    "CSVReaderDuckDBEngine",
    "CSVReaderPolarsEngine",
    "CSVReaderPyArrowEngine",
    "CSVWriterDuckDBEngine",
    "CSVWriterPolarsEngine",
    "CSVWriterPyArrowEngine",
    "CSVEngineProperties",
    "CSVComponents",
    "CSVColumnNameNormalizer",
    "CSVColumns",
    "CSVDelimiter",
    "CSVDialect",
    "CSVRows",
    "CSVStringSample",
    "DuckDBQueries",
    "ExcelComponents",
    "ExcelEngineFactory",
    "ExcelEngineProperties",
    "ExcelReaderEngine",
    "ExcelWriterEngine",
    "FileProperties",
    "ParquetComponents",
    "ParquetEngineFactory",
    "ParquetEngineProperties",
    "ParquetReaderEngine",
    "ParquetWriterEngine",
]


def _imported_names(path: Path) -> set[str]:
    """Every module the file at ``path`` names in an import statement (nested ones too), ``from x import y`` as x.y."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = "." * node.level + (node.module or "")
            found.add(module)
            found.update(f"{module}.{alias.name}" for alias in node.names)
    return found


def _is_v4(name: str) -> bool:
    """True if the module ``name`` is (or is inside) a v4 package or a v4 PDF library."""
    return (
        name.startswith(tuple(f"{package}." for package in V4_PACKAGES))
        or name in V4_PACKAGES
        or (name.split(".")[0] in V4_LIBRARIES)
    )


@pytest.mark.parametrize("name", V4_PACKAGES)
def test_v4_package_is_gone(name):
    assert importlib.util.find_spec(name) is None


@pytest.mark.parametrize("root", [SRC, TESTS], ids=["src", "tests"])
def test_no_module_imports_v4_or_its_pdf_libraries(root):
    found = sorted(
        f"{path.relative_to(root)}: {name}"
        for path in sorted(root.rglob("*.py"))
        for name in _imported_names(path)
        if _is_v4(name)
    )

    assert found == []


def test_core_exports_no_v4_pdf_names():
    import datagrunt.core

    assert datagrunt.core.__all__ == CORE_NAMES
