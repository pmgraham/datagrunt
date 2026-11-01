"""The v5 ``datagrunt.pdf`` namespace is additive: the top-level ``datagrunt`` API is unchanged."""

import ast
import importlib.util
import pkgutil
from pathlib import Path

import pytest

import datagrunt
import datagrunt.pdf
import datagrunt.pdf.elements
import datagrunt.pdf.exceptions
import datagrunt.pdf_api

# Pinned literal (plan order): a name added to or dropped from the public
# surface must show up here, not just stay self-consistent.
V5_NAMES = [
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
EXCEPTION_NAMES = {"DatagruntPDFError", "PDFEncryptedError", "PDFCorruptError"}


def test_pdf_package_exports_exactly_the_v5_names():
    assert datagrunt.pdf.__all__ == V5_NAMES


@pytest.mark.parametrize("name", V5_NAMES)
def test_exported_name_is_the_defining_modules_object(name):
    source = datagrunt.pdf.exceptions if name in EXCEPTION_NAMES else datagrunt.pdf.elements
    assert getattr(datagrunt.pdf, name) is getattr(source, name)


@pytest.mark.parametrize("name", V5_NAMES)
def test_top_level_package_does_not_export_v5_name(name):
    assert name not in datagrunt.__all__
    assert not hasattr(datagrunt, name)


def test_top_level_pdf_reader_and_writer_are_still_v4():
    assert datagrunt.PDFReader is datagrunt.pdf_api.PDFReader
    assert datagrunt.PDFWriter is datagrunt.pdf_api.PDFWriter
    assert datagrunt.PDFReader.__module__ == "datagrunt.pdf_api.pdfreader"
    assert {"PDFReader", "PDFWriter"} <= set(datagrunt.__all__)


def test_v5_package_defines_no_reader_or_writer_yet():
    assert not hasattr(datagrunt.pdf, "PDFReader")
    assert not hasattr(datagrunt.pdf, "PDFWriter")


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
    # ``import datagrunt`` itself loads v4 (PDFReader), so sys.modules cannot tell; the statements can.
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
