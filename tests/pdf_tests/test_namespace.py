"""The v5 ``datagrunt.pdf`` namespace is additive: the top-level ``datagrunt`` API is unchanged."""

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
