"""Guards that PDFBatchWriter is fully removed from the public API.

PDFBatchWriter was never officially advertised and was hard-removed; these
tests lock the removal so the symbol cannot silently reappear in the package
surface.
"""

import importlib

import pytest


def test_not_exported_from_top_level_package():
    import datagrunt

    assert not hasattr(datagrunt, "PDFBatchWriter")
    assert "PDFBatchWriter" not in getattr(datagrunt, "__all__", [])


def test_not_exported_from_pdf_api():
    import datagrunt.pdf_api as pdf_api

    assert not hasattr(pdf_api, "PDFBatchWriter")
    assert "PDFBatchWriter" not in getattr(pdf_api, "__all__", [])


def test_batch_module_is_gone():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("datagrunt.pdf_api.batch")
