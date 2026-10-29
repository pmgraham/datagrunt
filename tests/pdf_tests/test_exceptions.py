"""Tests for the v5 PDF exception hierarchy (``datagrunt.pdf.exceptions``)."""

import pickle

import pytest

from datagrunt.pdf import DatagruntPDFError, PDFCorruptError, PDFEncryptedError


class TestHierarchy:
    def test_base_is_an_exception(self):
        assert issubclass(DatagruntPDFError, Exception)

    def test_encrypted_is_a_datagrunt_error(self):
        assert issubclass(PDFEncryptedError, DatagruntPDFError)

    def test_encrypted_is_a_value_error_for_v4_callers(self):
        # v4 raised ValueError for encrypted PDFs; `except ValueError` must keep working.
        with pytest.raises(ValueError, match="password required"):
            raise PDFEncryptedError("password required")

    def test_corrupt_is_a_datagrunt_error_not_a_value_error(self):
        assert issubclass(PDFCorruptError, DatagruntPDFError)
        assert not issubclass(PDFCorruptError, ValueError)

    def test_base_catches_both_subclasses(self):
        for exc in (PDFEncryptedError("locked"), PDFCorruptError("broken")):
            with pytest.raises(DatagruntPDFError):
                raise exc


class TestPDFCorruptError:
    def test_page_number_defaults_to_none(self):
        err = PDFCorruptError("bad xref table")
        assert err.page_number is None
        assert str(err) == "bad xref table"

    @pytest.mark.parametrize("page", [1, 2, 10_000])
    def test_stores_valid_page_number(self, page):
        err = PDFCorruptError("bad page", page_number=page)
        assert err.page_number == page
        assert str(err) == "bad page"

    @pytest.mark.parametrize("bad", [0, -1])
    def test_rejects_page_number_below_one(self, bad):
        with pytest.raises(ValueError, match="page_number"):
            PDFCorruptError("bad page", page_number=bad)

    @pytest.mark.parametrize("bad", [True, False])
    def test_rejects_bool_page_number(self, bad):
        with pytest.raises(ValueError, match="page_number"):
            PDFCorruptError("bad page", page_number=bad)

    @pytest.mark.parametrize("bad", [1.0, "1", [1]])
    def test_rejects_non_int_page_number(self, bad):
        with pytest.raises(ValueError, match="page_number"):
            PDFCorruptError("bad page", page_number=bad)

    def test_page_number_is_keyword_only(self):
        with pytest.raises(TypeError):
            PDFCorruptError("bad page", 3)

    def test_pickle_round_trip_keeps_page_number(self):
        # Same-process round-trip of a locally-built instance: errors must survive
        # a process-pool worker boundary with their page number intact.
        restored = pickle.loads(pickle.dumps(PDFCorruptError("bad page", page_number=4)))
        assert isinstance(restored, PDFCorruptError)
        assert restored.page_number == 4
        assert str(restored) == "bad page"
