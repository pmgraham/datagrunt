"""Tests for the v5 PDFReader (#372, A8b): ``datagrunt.pdf.reader``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``): a
digital PDF of two mixed pages (heading, prose, an image, a ruled table and
a borderless one), its bundle (A7a's ``write_bundle``) and its document
JSON (A7a's codec). A stage the reader calls is a ``monkeypatch`` of the
reader module's name for it.
"""

import copy
import errno
import os
import re
import sys
from types import MappingProxyType, SimpleNamespace

import pytest

from datagrunt.pdf import reader as reader_module
from datagrunt.pdf.bundle import load_bundle, write_bundle
from datagrunt.pdf.codec import document_from_json, document_to_json
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.reader import PDFReader
from pdf_tests.builders import build_pdf
from pdf_tests.test_pipeline import mixed_page

# A figure at 32 px (mixed_page's image is 64 x 32 px): every element kind the page holds is read.
OPTIONS = {"min_image_dimension": 32, "ocr": "never"}
TEXT = b"Name,Qty\nBolt,4\n"


@pytest.fixture(scope="module")
def made(tmp_path_factory):
    """A digital PDF of two mixed pages, its document, its bundle, and its document JSON."""
    directory = tmp_path_factory.mktemp("reader")
    path = build_pdf(directory / "parts list.pdf", [mixed_page(), mixed_page(top=100.0)])
    document = extract_document(path, **OPTIONS)
    bundle = directory / "bundle"
    write_bundle(document, bundle, source=path)
    return SimpleNamespace(path=path, document=document, bundle=bundle, json=document_to_json(document))


def calls_of(monkeypatch, name: str) -> list:
    """Record each call of the reader module's ``name`` (its arguments) and let it run."""
    calls: list = []
    real = getattr(reader_module, name)

    def recorded(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(reader_module, name, recorded)
    return calls


def refuse_work(monkeypatch) -> None:
    """Fail the test if the reader extracts or loads anything."""

    def refused(*args, **kwargs):
        raise AssertionError("work was done before the checks")

    for name in ("extract_document", "load_bundle", "document_from_json"):
        monkeypatch.setattr(reader_module, name, refused)


class TestSources:
    """A ``.pdf`` path is extracted (A8a), a bundle loaded (A7a), a Mapping decoded (A7a's codec)."""

    @pytest.mark.parametrize("kind", [str, os.PathLike], ids=["str", "path"])
    def test_a_pdf_path_is_extracted_with_the_readers_options(self, made, kind):
        source = str(made.path) if kind is str else made.path

        assert PDFReader(source, **OPTIONS).document == made.document

    def test_every_option_and_the_password_reach_the_extraction(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")
        options = {
            "workers": 2,
            "ocr": "never",
            "dpi": 100,
            "vision_dpi": 150,
            "max_render_pixels": 1_000_000,
            "min_image_dimension": 32,
            "strict": True,
        }

        PDFReader(made.path, password="not-needed", **options).document

        assert calls == [((made.path.absolute(),), {"password": "not-needed", **options})]

    def test_a_bundle_directory_is_loaded(self, made):
        assert PDFReader(made.bundle).document == load_bundle(made.bundle)

    def test_a_bundles_manifest_is_loaded(self, made):
        assert PDFReader(made.bundle / "manifest.json").document == load_bundle(made.bundle)

    @pytest.mark.parametrize("wrap", [dict, MappingProxyType], ids=["dict", "mapping"])
    def test_a_mapping_is_the_codecs_document_json(self, made, wrap):
        assert PDFReader(wrap(copy.deepcopy(made.json))).document == document_from_json(made.json)

    def test_a_mapping_changed_after_construction_does_not_change_the_reader(self, made):
        data = copy.deepcopy(made.json)
        reader = PDFReader(data)

        data["pages"].clear()

        assert reader.document == made.document

    def test_a_path_is_made_absolute_at_construction(self, made, monkeypatch, tmp_path):
        monkeypatch.chdir(made.path.parent)
        reader = PDFReader(made.path.name, **OPTIONS)

        monkeypatch.chdir(tmp_path)

        assert reader.document == made.document

    def test_a_pdf_with_bytes_before_its_header_is_a_pdf(self, made, tmp_path):
        path = tmp_path / "prefixed.pdf"
        path.write_bytes(b"\x00" * 1000 + made.path.read_bytes())

        reader = PDFReader(path, **OPTIONS)

        assert reader.document.pages == made.document.pages


class TestExtraction:
    """The document is read on first use, then cached; construction does no work."""

    def test_construction_does_not_extract(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")

        PDFReader(made.path, **OPTIONS)

        assert calls == []

    def test_the_document_is_extracted_once(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")
        reader = PDFReader(made.path, **OPTIONS)

        first, second = reader.document, reader.document

        assert first is second
        assert len(calls) == 1

    def test_a_bundle_is_loaded_on_first_use_then_cached(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "load_bundle")
        reader = PDFReader(made.bundle)

        assert calls == []
        assert reader.document is reader.document
        assert len(calls) == 1

    def test_a_failed_extraction_is_not_cached(self, made, monkeypatch):
        failures = iter([OSError("disk gone")])
        real = reader_module.extract_document

        def once_failing(*args, **kwargs):
            for failure in failures:
                raise failure
            return real(*args, **kwargs)

        monkeypatch.setattr(reader_module, "extract_document", once_failing)
        reader = PDFReader(made.path, **OPTIONS)

        with pytest.raises(OSError, match="disk gone"):
            reader.document
        assert reader.document == made.document


class TestConstructionChecks:
    """Spec invariants 1-2: a bad source or option raises at construction, before any work."""

    def test_a_missing_path_is_file_not_found(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)

        with pytest.raises(FileNotFoundError) as caught:
            PDFReader(tmp_path / "missing.pdf")
        assert caught.value.errno == errno.ENOENT

    def test_a_missing_manifest_is_file_not_found(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)

        with pytest.raises(FileNotFoundError):
            PDFReader(tmp_path / "manifest.json")

    @pytest.mark.parametrize(
        "content",
        [TEXT, b"", b" " * 1025 + b"%PDF-1.7\n", b"%pdf-1.7\n", b"%PD" + b" " * 2000],
        ids=["csv", "empty", "header-at-offset-1025", "lower-case", "cut-marker"],
    )
    def test_a_file_that_is_not_a_pdf_is_a_value_error(self, tmp_path, monkeypatch, content):
        refuse_work(monkeypatch)
        path = tmp_path / "parts.pdf"
        path.write_bytes(content)

        message = f"source {path} is not a PDF: no %PDF at any offset from 0 to 1024"
        with pytest.raises(ValueError, match=re.escape(message)):
            PDFReader(path)

    @pytest.mark.parametrize("offset", [0, 1, 1020, 1024], ids=lambda offset: f"offset-{offset}")
    @pytest.mark.parametrize("marker", [b"%PDF-1.7\n", b"%PDF"], ids=["versioned", "bare"])
    def test_a_file_with_the_marker_at_offsets_0_to_1024_passes_the_check(self, tmp_path, monkeypatch, offset, marker):
        refuse_work(monkeypatch)
        path = tmp_path / "parts.pdf"
        path.write_bytes(b" " * offset + marker)

        PDFReader(path)  # checked, not read: extraction is on first use

    def test_at_most_1028_bytes_are_read_to_check_the_header(self, tmp_path, monkeypatch):
        path = tmp_path / "parts.pdf"
        path.write_bytes(b" " * 4096)
        reads = []
        real = os.read
        monkeypatch.setattr(reader_module.os, "read", lambda fd, size: reads.append(size) or real(fd, size))

        with pytest.raises(ValueError, match="is not a PDF"):
            PDFReader(path)
        assert reads == [1028]

    def test_a_directory_without_a_manifest_is_a_value_error(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)

        with pytest.raises(ValueError, match=re.escape(f"source {tmp_path} is a directory without a manifest.json")):
            PDFReader(tmp_path)

    @pytest.mark.parametrize("target", ["directory", "manifest"])
    def test_a_bundle_whose_manifest_is_not_valid_is_a_value_error(self, made, tmp_path, monkeypatch, target):
        refuse_work(monkeypatch)
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "manifest.json").write_bytes(b'{"bundle_format_version": "4.0"}')

        source = bundle if target == "directory" else bundle / "manifest.json"
        with pytest.raises(ValueError, match=re.escape(f"source {bundle} is not a valid bundle: manifest.json: ")):
            PDFReader(source)

    @pytest.mark.skipif(sys.platform == "win32", reason="named pipes are POSIX")
    def test_a_named_pipe_is_refused_without_blocking(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)
        fifo = tmp_path / "pipe.pdf"
        os.mkfifo(fifo)

        with pytest.raises(ValueError, match=re.escape(f"source {fifo} is not a regular file")):
            PDFReader(fifo)

    @pytest.mark.parametrize(
        "source,kind",
        [(42, "int"), (b"parts.pdf", "bytes"), (None, "NoneType"), (["parts.pdf"], "list")],
        ids=["int", "bytes", "none", "list"],
    )
    def test_a_source_of_another_type_is_a_type_error(self, monkeypatch, source, kind):
        refuse_work(monkeypatch)

        with pytest.raises(TypeError, match=re.escape(f"source must be a str, os.PathLike or Mapping, got {kind}")):
            PDFReader(source)

    def test_a_mapping_that_is_not_a_document_is_refused(self, made):
        with pytest.raises(ValueError, match=re.escape("document has missing key(s)")):
            PDFReader({"format": "datagrunt.pdf.document"})
        with pytest.raises(ValueError, match=re.escape("format must be 'datagrunt.pdf.document', got 'other'")):
            PDFReader({**made.json, "format": "other"})

    @pytest.mark.parametrize(
        "option,value,error,message",
        [
            ("workers", 0, ValueError, "workers must be >= 1, got 0"),
            ("dpi", 35, ValueError, "dpi must be >= 36, got 35"),
            ("vision_dpi", 199, ValueError, "vision_dpi must be >= dpi (200), got 199"),
            ("max_render_pixels", 262_143, ValueError, "max_render_pixels must be >= 262144, got 262143"),
            ("min_image_dimension", 0, ValueError, "min_image_dimension must be >= 1, got 0"),
            ("ocr", "sometimes", ValueError, "ocr must be 'auto' or 'always' or 'never', got 'sometimes'"),
            ("strict", 1, TypeError, "strict must be a bool"),
            ("workers", True, TypeError, "workers must be an int"),
            ("password", b"pw", TypeError, "password must be a str or None, got bytes"),
            ("password", "p\x00w", ValueError, "password must not contain NUL"),
        ],
    )
    @pytest.mark.parametrize("source", ["pdf", "bundle", "mapping", "missing"])
    def test_an_option_out_of_bounds_raises_first_for_every_source(
        self, made, tmp_path, monkeypatch, source, option, value, error, message
    ):
        refuse_work(monkeypatch)
        given = {"pdf": made.path, "bundle": made.bundle, "mapping": made.json, "missing": tmp_path / "gone.pdf"}

        with pytest.raises(error, match=re.escape(message)):
            PDFReader(given[source], **{option: value})
