"""The public PDF surface (5.0, #373): ``datagrunt`` and ``datagrunt.pdf`` export v5's reader, writer and model."""

import ast
import importlib.util
import pkgutil
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


# v4's packages, which the 5.0 flip (#373) deletes: no v5 module may reach them by any chain of imports.
V4_PACKAGES = ("datagrunt.core.pdf_io", "datagrunt.pdf_api")


def _source_imports(name: str) -> set[str]:
    """The modules the source of module ``name`` imports, read from its AST (every import statement, nested too)."""
    spec = importlib.util.find_spec(name)
    assert spec is not None and spec.origin is not None, name
    tree = ast.parse(Path(spec.origin).read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"{name} uses a relative import: the walk below reads absolute ones only"
            assert node.module is not None
            found.add(node.module)
            # ``from package import module`` imports the submodule too.
            if _is_package(node.module):
                found.update(f"{node.module}.{alias.name}" for alias in node.names)
    return found


def _is_package(name: str) -> bool:
    """True if ``name`` is a datagrunt package (its submodules can be imported from it)."""
    if not name.startswith("datagrunt"):
        return False
    spec = importlib.util.find_spec(name)
    return spec is not None and spec.submodule_search_locations is not None


def _with_v5_parents(name: str) -> list[str]:
    """``name`` and the ``datagrunt.pdf`` packages above it: importing a submodule runs each one's ``__init__``."""
    if not name.startswith("datagrunt.pdf."):
        return [name]
    parts = name.split(".")
    return [".".join(parts[:end]) for end in range(2, len(parts) + 1)]


def _v5_modules() -> list[str]:
    """Every module of the ``datagrunt.pdf`` package."""
    return ["datagrunt.pdf", *(info.name for info in pkgutil.walk_packages(datagrunt.pdf.__path__, "datagrunt.pdf."))]


@pytest.mark.parametrize("start", _v5_modules())
def test_no_v5_module_imports_v4_by_any_chain(start):
    # The import statements, followed transitively through datagrunt's own modules (bounded: each module once).
    seen, stack, chains = {start}, [start], {start: (start,)}
    while stack:
        name = stack.pop()
        for target in sorted({parent for found in _source_imports(name) for parent in _with_v5_parents(found)}):
            if not target.startswith("datagrunt.") or target in seen or importlib.util.find_spec(target) is None:
                continue
            chain = (*chains[name], target)
            assert not target.startswith(V4_PACKAGES), " -> ".join(chain)
            seen.add(target)
            chains[target] = chain
            stack.append(target)
