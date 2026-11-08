"""Tests for Stage 1 batch PDF corpus extraction and loading (#463, 5.2): ``datagrunt.pdf.corpus``."""

import hashlib
import json
from pathlib import Path

import pytest

import datagrunt
import datagrunt.pdf
from datagrunt.pdf import corpus
from datagrunt.pdf.corpus import (
    CORPUS_FORMAT_VERSION,
    CORPUS_MANIFEST,
    CorpusEntry,
    CorpusFailure,
    CorpusManifest,
    extract_corpus,
    load_corpus,
)
from datagrunt.pdf.exceptions import BundleCorruptError, PDFCorruptError
from pdf_tests.builders import PageSpec, TextRun, build_pdf
from pdf_tests.old_formats import as_bundle_5_0


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def sample_pdfs(tmp_path_factory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("corpus_pdfs")
    pdf_a = build_pdf(root / "alpha.pdf", [PageSpec(items=(TextRun("Alpha Report", 72.0, 100.0),))])
    pdf_b = build_pdf(
        root / "beta.pdf",
        [
            PageSpec(items=(TextRun("Beta Page 1", 72.0, 100.0),)),
            PageSpec(items=(TextRun("Beta Page 2", 72.0, 100.0),)),
        ],
    )
    return pdf_a, pdf_b


def test_corpus_constants_and_reexports():
    assert CORPUS_FORMAT_VERSION == "5.2"
    assert CORPUS_MANIFEST == "corpus_manifest.json"
    assert datagrunt.pdf.extract_corpus is extract_corpus
    assert datagrunt.pdf.load_corpus is load_corpus
    assert datagrunt.extract_corpus is extract_corpus
    assert datagrunt.load_corpus is load_corpus
    assert datagrunt.CorpusManifest is CorpusManifest
    assert datagrunt.CorpusEntry is CorpusEntry
    assert datagrunt.CorpusFailure is CorpusFailure


class TestModelValidation:
    def test_corpus_entry_validates_fields(self):
        sha = "a" * 64
        entry = CorpusEntry(
            doc_id="alpha_" + sha[:8],
            source_name="alpha.pdf",
            source_sha256=sha,
            bundle_relpath="alpha_" + sha[:8],
            page_count=1,
            integrity_verified=True,
            degraded_pages=(),
            reused=False,
        )
        assert entry.page_count == 1
        with pytest.raises(ValueError, match="doc_id"):
            CorpusEntry(
                doc_id="   ",
                source_name="alpha.pdf",
                source_sha256=sha,
                bundle_relpath="alpha",
                page_count=1,
                integrity_verified=True,
                degraded_pages=(),
                reused=False,
            )
        with pytest.raises(ValueError, match="source_sha256"):
            CorpusEntry(
                doc_id="alpha",
                source_name="alpha.pdf",
                source_sha256="not-a-sha",
                bundle_relpath="alpha",
                page_count=1,
                integrity_verified=True,
                degraded_pages=(),
                reused=False,
            )
        with pytest.raises(TypeError, match="reused"):
            CorpusEntry(
                doc_id="alpha",
                source_name="alpha.pdf",
                source_sha256=sha,
                bundle_relpath="alpha",
                page_count=1,
                integrity_verified=True,
                degraded_pages=(),
                reused=1,  # type: ignore[arg-type]
            )

    def test_corpus_failure_validates_fields(self):
        failure = CorpusFailure(source_path="broken.pdf", error_type="PDFCorruptError", message="bad pdf")
        assert failure.error_type == "PDFCorruptError"
        with pytest.raises(ValueError, match="error_type"):
            CorpusFailure(source_path="broken.pdf", error_type="", message="bad pdf")


class TestInputValidation:
    def test_empty_sequence_is_rejected_before_creating_output_dir(self, tmp_path):
        out = tmp_path / "out"
        with pytest.raises(ValueError, match="inputs"):
            extract_corpus([], out)
        assert not out.exists()

    def test_missing_input_path_raises_file_not_found(self, tmp_path):
        out = tmp_path / "out"
        with pytest.raises(FileNotFoundError):
            extract_corpus(tmp_path / "missing.pdf", out)
        assert not out.exists()

    def test_empty_input_directory_is_rejected(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        out = tmp_path / "out"
        with pytest.raises(ValueError, match="no PDF"):
            extract_corpus(empty_dir, out)
        assert not out.exists()

    def test_duplicate_input_paths_or_colliding_doc_ids_are_rejected(self, tmp_path, sample_pdfs):
        pdf_a, _ = sample_pdfs
        out = tmp_path / "out"
        with pytest.raises(ValueError, match="duplicate"):
            extract_corpus([pdf_a, pdf_a], out)
        assert not out.exists()

    @pytest.mark.parametrize(
        ("kwargs", "error", "pattern"),
        [
            ({"max_workers": 0}, ValueError, "max_workers"),
            ({"max_workers": -1}, ValueError, "max_workers"),
            ({"max_workers": 65}, ValueError, "max_workers"),
            ({"max_workers": True}, TypeError, "max_workers"),
            ({"overwrite": 1}, TypeError, "overwrite"),
            ({"strict": "yes"}, TypeError, "strict"),
            ({"dpi": 10}, ValueError, "dpi"),
        ],
    )
    def test_invalid_options_are_rejected_before_creating_output_dir(
        self, tmp_path, sample_pdfs, kwargs, error, pattern
    ):
        pdf_a, _ = sample_pdfs
        out = tmp_path / "out"
        with pytest.raises(error, match=pattern):
            extract_corpus(pdf_a, out, **kwargs)
        assert not out.exists()


class TestBatchExtractionAndLoading:
    def test_extract_corpus_from_directory_single_path_and_sequence(self, tmp_path, sample_pdfs):
        pdf_a, pdf_b = sample_pdfs
        out_seq = tmp_path / "corpus_seq"
        manifest = extract_corpus([pdf_a, pdf_b], out_seq, max_workers=2, dpi=72, vision_dpi=72)

        assert isinstance(manifest, CorpusManifest)
        assert manifest.corpus_format_version == CORPUS_FORMAT_VERSION
        assert manifest.output_dir == out_seq.resolve()
        assert manifest.failures == ()
        assert len(manifest.entries) == 2
        assert [e.source_name for e in manifest.entries] == ["alpha.pdf", "beta.pdf"]
        assert [e.source_sha256 for e in manifest.entries] == [_sha256(pdf_a), _sha256(pdf_b)]
        assert [e.page_count for e in manifest.entries] == [1, 2]
        assert all(not e.reused for e in manifest.entries)
        assert manifest.bundle_paths == tuple(out_seq.resolve() / e.bundle_relpath for e in manifest.entries)
        assert (out_seq / CORPUS_MANIFEST).is_file()

        docs = manifest.load_documents()
        assert len(docs) == 2
        assert [d.metadata.source_name for d in docs] == ["alpha.pdf", "beta.pdf"]
        assert [d.page_count for d in docs] == [1, 2]

        # Single path input
        out_single = tmp_path / "corpus_single"
        single_manifest = extract_corpus(pdf_a, out_single, dpi=72, vision_dpi=72)
        assert len(single_manifest.entries) == 1
        assert single_manifest.entries[0].source_name == "alpha.pdf"

        # Directory input
        out_dir = tmp_path / "corpus_dir"
        dir_manifest = extract_corpus(pdf_a.parent, out_dir, dpi=72, vision_dpi=72)
        assert [e.source_name for e in dir_manifest.entries] == ["alpha.pdf", "beta.pdf"]

    def test_incremental_skip_and_overwrite(self, tmp_path, sample_pdfs):
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus"
        first = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72)
        assert all(not e.reused for e in first.entries)

        second = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, overwrite=False)
        assert all(e.reused for e in second.entries)
        assert [e.doc_id for e in second.entries] == [e.doc_id for e in first.entries]

        third = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, overwrite=True)
        assert all(not e.reused for e in third.entries)
        assert third.load_documents() == first.load_documents()

    def test_a_rerun_with_another_workers_count_reuses_the_bundles(self, tmp_path, sample_pdfs):
        # workers changes no output (its default is the machine's CPU count): a corpus extracted on a machine
        # with other CPUs is reused, not extracted again.
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus"
        first = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, workers=2)

        second = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, workers=1)

        assert [e.reused for e in second.entries] == [True, True]
        assert second.load_documents() == first.load_documents()

    def test_the_default_workers_are_shared_by_the_documents_extracted_at_once(
        self, tmp_path, sample_pdfs, monkeypatch
    ):
        # Each of the max_workers documents in flight gets its share of the CPUs, not all of them: otherwise
        # max_workers x CPUs page workers (each memory-capped) would run at once. An explicit workers is kept.
        monkeypatch.setattr(corpus, "WORKERS", 8)

        def recorded(manifest):
            return [
                json.loads((manifest.output_dir / e.bundle_relpath / "manifest.json").read_bytes())[
                    "extraction_params"
                ]["workers"]
                for e in manifest.entries
            ]

        shared = extract_corpus(list(sample_pdfs), tmp_path / "shared", max_workers=2, dpi=72, vision_dpi=72)
        alone = extract_corpus(list(sample_pdfs), tmp_path / "alone", dpi=72, vision_dpi=72)
        given = extract_corpus(list(sample_pdfs), tmp_path / "given", max_workers=2, workers=3, dpi=72, vision_dpi=72)

        assert (recorded(shared), recorded(alone), recorded(given)) == ([4, 4], [8, 8], [3, 3])

    def test_a_bundle_of_the_older_format_is_not_reused(self, tmp_path, sample_pdfs):
        # A format 5.0 bundle (datagrunt 5.0 to 5.2) of this version and parameters records no image-only
        # reasons: it is written again, never reused (#470 review; bundle._existing's rule).
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus"
        first = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72)
        for entry in first.entries:
            as_bundle_5_0(out / entry.bundle_relpath)

        second = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, overwrite=False)

        assert [e.reused for e in second.entries] == [False, False]
        formats = [
            json.loads((out / e.bundle_relpath / "manifest.json").read_bytes())["bundle_format_version"]
            for e in second.entries
        ]
        assert formats == ["5.3", "5.3"]

    def test_fault_isolation_strict_false_vs_strict_true(self, tmp_path, sample_pdfs):
        pdf_a, _ = sample_pdfs
        corrupt_pdf = tmp_path / "corrupt.pdf"
        corrupt_pdf.write_bytes(b"%PDF-corrupt")

        out_strict = tmp_path / "strict_corpus"
        with pytest.raises(PDFCorruptError):
            extract_corpus([pdf_a, corrupt_pdf], out_strict, dpi=72, vision_dpi=72, strict=True)

        out_lenient = tmp_path / "lenient_corpus"
        manifest = extract_corpus([pdf_a, corrupt_pdf], out_lenient, dpi=72, vision_dpi=72, strict=False)
        assert len(manifest.entries) == 1
        assert manifest.entries[0].source_name == "alpha.pdf"
        assert len(manifest.failures) == 1
        assert manifest.failures[0].error_type == "PDFCorruptError"
        assert "corrupt.pdf" in manifest.failures[0].source_path

        reloaded = load_corpus(out_lenient)
        assert reloaded == manifest
        assert len(reloaded.load_documents()) == 1

    def test_load_corpus_and_corruption_detection(self, tmp_path, sample_pdfs):
        pdf_a, _ = sample_pdfs
        out = tmp_path / "corpus"
        manifest = extract_corpus(pdf_a, out, dpi=72, vision_dpi=72)
        manifest_path = out / CORPUS_MANIFEST

        assert load_corpus(out) == manifest
        assert load_corpus(manifest_path) == manifest

        raw = json.loads(manifest_path.read_text(encoding="ascii"))

        # Tamper format version
        bad_version = {**raw, "corpus_format_version": "4.0"}
        manifest_path.write_text(json.dumps(bad_version), encoding="ascii")
        with pytest.raises(BundleCorruptError, match="corpus_format_version"):
            load_corpus(out)

        # Tamper bundle_relpath traversal
        traversal = {**raw, "entries": [{**raw["entries"][0], "bundle_relpath": "../outside"}]}
        manifest_path.write_text(json.dumps(traversal), encoding="ascii")
        with pytest.raises(BundleCorruptError):
            load_corpus(out)

        # Restore manifest, corrupt inner bundle file
        manifest_path.write_text(json.dumps(raw), encoding="ascii")
        bundle_doc_json = out / manifest.entries[0].bundle_relpath / "json" / "document.json"
        bundle_doc_json.write_bytes(b"{corrupt")
        with pytest.raises(BundleCorruptError):
            load_corpus(out)

    def test_extract_corpus_without_page_renders_or_images(self, tmp_path, sample_pdfs):
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus_no_images"
        first = extract_corpus(
            [pdf_a, pdf_b],
            out,
            dpi=72,
            vision_dpi=72,
            include_images=False,
            render_pages=False,
        )
        assert len(first.entries) == 2
        assert all(not e.reused for e in first.entries)
        for bundle_path in first.bundle_paths:
            assert not (bundle_path / "page_images").exists()
            assert not (bundle_path / "images").exists()
            assert (bundle_path / "json" / "document.json").is_file()

        second = extract_corpus(
            [pdf_a, pdf_b],
            out,
            dpi=72,
            vision_dpi=72,
            include_images=False,
            render_pages=False,
            overwrite=False,
        )
        assert all(e.reused for e in second.entries)
        docs = second.load_documents()
        assert len(docs) == 2
        assert all(page.page_image is None for doc in docs for page in doc.pages)

    def test_page_worker_pool_reuses_worker_process_across_documents(self, tmp_path, sample_pdfs):
        from datagrunt.pdf.pipeline import PageWorkerPool, extract_document

        pdf_a, pdf_b = sample_pdfs
        with PageWorkerPool() as pool:
            doc_a = extract_document(pdf_a, workers=1, dpi=72, vision_dpi=72, _worker_pool=pool)
            assert len(pool._idle) == 1
            first_pid = pool._idle[0][0].pid
            doc_b = extract_document(pdf_b, workers=1, dpi=72, vision_dpi=72, _worker_pool=pool)
            assert len(pool._idle) == 1
            assert pool._idle[0][0].pid == first_pid
        assert doc_a.page_count == 1
        assert doc_b.page_count == 2
        assert pool._idle == []
