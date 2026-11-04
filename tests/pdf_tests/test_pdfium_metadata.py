"""Tests for the v5 engine's document metadata reader (#372 A8a): ``pdfium_engine.document_metadata``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``); a
pdfium stand-in (``monkeypatch.setattr(pdfium_c, ...)``) supplies the bytes
ReportLab cannot write.
"""

import ctypes
import pickle
import re

import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import DatagruntPDFError, pdfium_engine
from datagrunt.pdf.pdfium_engine import DocumentInfo, document_metadata, open_document
from pdf_tests.builders import PageSpec, TextRun, build_pdf, hand_written_pdf

PAGE = PageSpec(items=(TextRun("Body", x=72.0, y=100.0),))
TITLE = "Quarterly — Résumé \U0001f600"


def info_of(path, **options) -> DocumentInfo:
    with open_document(path, **options) as handle:
        return document_metadata(handle)


def no_info_pdf(tmp_path):
    """A one-page PDF whose trailer has no /Info dictionary."""
    return hand_written_pdf(
        tmp_path / "bare.pdf",
        [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 100] >>",
        ],
    )


def stand_in(monkeypatch, written: bytes):
    """Make FPDF_GetMetaText report ``written`` (UTF-16LE, terminator included) for every tag."""

    def get_meta_text(document, tag, buffer, length):
        if buffer is not None and length >= len(written):
            ctypes.memmove(buffer, written, len(written))
        return len(written)

    monkeypatch.setattr(pdfium_c, "FPDF_GetMetaText", get_meta_text)


class TestDocumentMetadata:
    """Title and author come from pdfium's FPDF_GetMetaText, decoded from UTF-16LE; blank is None."""

    def test_title_and_author_are_read_verbatim(self, tmp_path):
        path = build_pdf(tmp_path / "doc.pdf", [PAGE], title=TITLE, author="  Ada Lovelace  ")

        assert info_of(path) == DocumentInfo(title=TITLE, author="  Ada Lovelace  ")

    @pytest.mark.parametrize("blank", ["", " ", "\t 　"])
    def test_a_blank_entry_is_none(self, tmp_path, blank):
        path = build_pdf(tmp_path / "doc.pdf", [PAGE], title=blank, author=blank)

        assert info_of(path) == DocumentInfo(title=None, author=None)

    def test_a_missing_info_dictionary_is_none(self, tmp_path):
        assert info_of(no_info_pdf(tmp_path)) == DocumentInfo(title=None, author=None)

    def test_an_encrypted_documents_entries_are_decrypted(self, tmp_path):
        path = build_pdf(tmp_path / "enc.pdf", [PAGE], user_password="pw", title=TITLE, author="Ada")

        assert info_of(path, password="pw") == DocumentInfo(title=TITLE, author="Ada")

    def test_a_lone_surrogate_is_kept_not_replaced(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PAGE])
        stand_in(monkeypatch, "\ud800A".encode("utf-16-le", errors="surrogatepass") + b"\x00\x00")

        assert info_of(path) == DocumentInfo(title="\ud800A", author="\ud800A")

    def test_an_entry_at_the_bound_is_read(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PAGE], title="abcd", author="abcd")
        monkeypatch.setattr(pdfium_engine, "_MAX_META_TEXT_BYTES", 10)  # "abcd" is 8 bytes, 10 with its terminator

        assert info_of(path) == DocumentInfo(title="abcd", author="abcd")

    def test_an_entry_over_the_bound_fails_loud_unread(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [PAGE], title="abcde", author="ab")
        monkeypatch.setattr(pdfium_engine, "_MAX_META_TEXT_BYTES", 10)  # "abcde" is 12 bytes with its terminator

        message = "the document's /Title is 12 bytes (UTF-16LE), over the 10-byte bound: it is not read"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            info_of(path)

    def test_the_handle_is_checked(self, tmp_path):
        handle = open_document(build_pdf(tmp_path / "doc.pdf", [PAGE]))
        handle.close()

        with pytest.raises(TypeError, match="handle must be a DocumentHandle, got str"):
            document_metadata("doc.pdf")
        with pytest.raises(ValueError, match="document handle is closed"):
            document_metadata(handle)

    def test_the_info_is_frozen_and_picklable(self):
        info = DocumentInfo(title=TITLE, author=None)

        assert pickle.loads(pickle.dumps(info)) == info
        with pytest.raises(AttributeError):
            info.title = "x"  # type: ignore[misc]

    @pytest.mark.parametrize(
        "fields,message",
        [
            pytest.param({"title": 1, "author": None}, "title must be a str or None, got 1 (int)", id="title"),
            pytest.param(
                {"title": None, "author": b"x"}, "author must be a str or None, got b'x' (bytes)", id="author"
            ),
        ],
    )
    def test_the_info_checks_its_fields(self, fields, message):
        with pytest.raises(TypeError, match=re.escape(message)):
            DocumentInfo(**fields)
