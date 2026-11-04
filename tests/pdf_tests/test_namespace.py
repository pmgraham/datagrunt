"""The public PDF surface (5.0, #373): ``datagrunt`` and ``datagrunt.pdf`` export v5's reader, writer and model."""

import ast
import importlib.util
from pathlib import Path
from typing import get_args

import pytest

import datagrunt
import datagrunt.pdf
import datagrunt.pdf.bundle
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
    "PAGE_ROUTES",
    "ExtractionMethod",
    "make_element_id",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
    "PDFSourceChangedError",
    "BundleCorruptError",
]
PDF_PACKAGE_NAMES = [*V5_MODEL_NAMES, "PDFReader", "PDFWriter", "load_bundle", "write_bundle"]
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
EXCEPTION_NAMES = {
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
    "PDFSourceChangedError",
    "BundleCorruptError",
}
CLASS_MODULES = {"PDFReader": datagrunt.pdf.reader, "PDFWriter": datagrunt.pdf.writer}
FUNCTION_MODULES = {"load_bundle": datagrunt.pdf.bundle, "write_bundle": datagrunt.pdf.bundle}


def _defining_module(name: str):
    if name in CLASS_MODULES:
        return CLASS_MODULES[name]
    if name in FUNCTION_MODULES:
        return FUNCTION_MODULES[name]
    return datagrunt.pdf.exceptions if name in EXCEPTION_NAMES else datagrunt.pdf.elements


def test_pdf_package_exports_exactly_the_v5_names():
    assert datagrunt.pdf.__all__ == PDF_PACKAGE_NAMES


def test_top_level_package_exports_exactly_the_pinned_names():
    assert datagrunt.__all__ == TOP_LEVEL_NAMES


def test_page_routes_is_exported_as_the_routes_of_page_route():
    # #456: public, like HIDDEN_REASONS, so the routes a page can take are importable from datagrunt.pdf.
    from datagrunt.pdf import PAGE_ROUTES, PageRoute

    assert PAGE_ROUTES == get_args(PageRoute) == ("digital", "hybrid", "vision")


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


@pytest.mark.parametrize("name", sorted(EXCEPTION_NAMES))
def test_every_exception_is_a_datagrunt_pdf_error(name):
    assert issubclass(getattr(datagrunt.pdf, name), datagrunt.pdf.DatagruntPDFError)


def _raises_section(member: object) -> str:
    """The text after the ``Raises:`` heading of ``member``'s docstring ("" if it has none)."""
    doc = getattr(member, "__doc__", None) or ""
    return doc.split("Raises:", 1)[1] if "Raises:" in doc else ""


READER, WRITER = datagrunt.pdf.reader.PDFReader, datagrunt.pdf.writer.PDFWriter
WRITER_METHODS = [
    "write_json",
    "write_jsonl",
    "write_markdown",
    "write_parquet",
    "write_csv",
    "extract_images",
    "render_pages_as_images",
    "write_bundle",
]
# Each public member that raises an exported exception names it under Raises: the reader's document loads a
# bundle (verified) or extracts the PDF (checked against changes); each writer method extracts on first use.
RAISERS = [
    ("PDFReader.document", READER.__dict__["document"], "BundleCorruptError"),
    ("PDFReader.document", READER.__dict__["document"], "PDFSourceChangedError"),
    *((f"PDFWriter.{method}", getattr(WRITER, method), "PDFSourceChangedError") for method in WRITER_METHODS),
]


@pytest.mark.parametrize(
    ("member", "name"), [(member, name) for _, member, name in RAISERS], ids=[f"{at}-{name}" for at, _, name in RAISERS]
)
def test_public_raisers_name_the_exception_under_raises(member, name):
    assert name in _raises_section(member)


@pytest.mark.parametrize("package", [datagrunt, datagrunt.pdf], ids=["datagrunt", "datagrunt.pdf"])
def test_lazy_classes_are_listed_by_dir(package):
    assert {"PDFReader", "PDFWriter"} <= set(dir(package))


def test_bundle_functions_are_listed_by_dir_and_stay_in_the_pdf_package():
    assert set(FUNCTION_MODULES) <= set(dir(datagrunt.pdf))
    for name in FUNCTION_MODULES:
        assert name not in datagrunt.__all__
        assert not hasattr(datagrunt, name)


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
