"""Tests for Stage 1 batch PDF corpus extraction and loading (#463, 5.2): ``datagrunt.pdf.corpus``."""

import ast
import hashlib
import json
import os
import threading
import time
from pathlib import Path

import pytest

import datagrunt
import datagrunt.pdf
from datagrunt.pdf import _bundle_load
from datagrunt.pdf import corpus as corpus_module
from datagrunt.pdf.corpus import (
    CORPUS_FORMAT_VERSION,
    CORPUS_MANIFEST,
    CorpusEntry,
    CorpusFailure,
    CorpusManifest,
    extract_corpus,
    load_corpus,
    load_corpus_documents,
)
from datagrunt.pdf.exceptions import BundleCorruptError, PDFCorruptError
from pdf_tests.builders import PageSpec, Rect, TextRun, break_page, build_pdf
from pdf_tests.old_formats import as_bundle_5_0, as_corpus_5_2


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
    assert CORPUS_FORMAT_VERSION == "5.3"
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
        assert entry.image_only_pages == ()
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

    @pytest.mark.parametrize(
        ("field_name", "bad_value", "error", "pattern"),
        [
            ("image_only_pages", [1], TypeError, "image_only_pages must be a tuple or None"),
            ("image_only_pages", "1", TypeError, "image_only_pages must be a tuple or None"),
            ("image_only_pages", 1, TypeError, "image_only_pages must be a tuple or None"),
            ("image_only_pages", (True,), TypeError, r"image_only_pages\[0\] must be an int"),
            ("image_only_pages", ("1",), TypeError, r"image_only_pages\[0\] must be an int"),
            ("image_only_pages", (0,), ValueError, r"image_only_pages\[0\] must be >= 1"),
            ("image_only_pages", (-1,), ValueError, r"image_only_pages\[0\] must be >= 1"),
            ("image_only_pages", (4,), ValueError, r"image_only_pages\[0\].*page_count"),
            ("image_only_pages", (2, 2), ValueError, "image_only_pages must be strictly increasing"),
            ("image_only_pages", (3, 1), ValueError, "image_only_pages must be strictly increasing"),
            ("degraded_pages", (4,), ValueError, r"degraded_pages\[0\].*page_count"),
            ("degraded_pages", (2, 1), ValueError, "degraded_pages must be strictly increasing"),
        ],
    )
    def test_corpus_entry_validates_image_only_and_degraded_pages(self, field_name, bad_value, error, pattern):
        sha = "a" * 64
        valid = CorpusEntry(
            doc_id="alpha",
            source_name="alpha.pdf",
            source_sha256=sha,
            bundle_relpath="alpha",
            page_count=3,
            integrity_verified=True,
            degraded_pages=(3,),
            image_only_pages=(1, 3),
            reused=False,
        )
        assert valid.image_only_pages == (1, 3)
        unrecorded = CorpusEntry(
            doc_id="alpha",
            source_name="alpha.pdf",
            source_sha256=sha,
            bundle_relpath="alpha",
            page_count=3,
            integrity_verified=True,
            degraded_pages=(),
            image_only_pages=None,
            reused=False,
        )
        assert unrecorded.image_only_pages is None
        kwargs = {
            "doc_id": "alpha",
            "source_name": "alpha.pdf",
            "source_sha256": sha,
            "bundle_relpath": "alpha",
            "page_count": 3,
            "integrity_verified": True,
            "degraded_pages": (),
            "image_only_pages": (),
            "reused": False,
            field_name: bad_value,
        }
        with pytest.raises(error, match=pattern):
            CorpusEntry(**kwargs)  # type: ignore[arg-type]

    def test_corpus_manifest_validates_format_versions_and_image_only_none(self, tmp_path):
        sha = "a" * 64
        entry_none = CorpusEntry(
            doc_id="alpha",
            source_name="alpha.pdf",
            source_sha256=sha,
            bundle_relpath="alpha",
            page_count=1,
            integrity_verified=True,
            image_only_pages=None,
        )
        m52 = CorpusManifest(corpus_format_version="5.2", output_dir=tmp_path, entries=(entry_none,))
        assert m52.corpus_format_version == "5.2"
        with pytest.raises(ValueError, match="image_only_pages"):
            CorpusManifest(corpus_format_version="5.3", output_dir=tmp_path, entries=(entry_none,))

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
            doc_a = extract_document(pdf_a, dpi=72, vision_dpi=72, _worker_pool=pool)
            assert len(pool._idle) == 1
            first_pid = pool._idle[0][0].pid
            doc_b = extract_document(pdf_b, dpi=72, vision_dpi=72, _worker_pool=pool)
            assert len(pool._idle) == 1
            assert pool._idle[0][0].pid == first_pid
        assert doc_a.page_count == 1
        assert doc_b.page_count == 2
        assert pool._idle == []

    def test_extract_corpus_render_pages_vision_only(self, tmp_path, sample_pdfs, monkeypatch):
        from datagrunt.pdf import pipeline

        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus_vision_only"
        with pytest.raises(ValueError, match="render_pages"):
            extract_corpus([pdf_a], out, render_pages="bogus")  # type: ignore[arg-type]
        assert not out.exists()

        spawned: list = []
        monkeypatch.setattr(pipeline, "start_worker", lambda *a, **kw: spawned.append(a))

        first = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, render_pages="vision_only")
        assert spawned == []
        assert len(first.entries) == 2
        assert all(not e.reused for e in first.entries)
        for bundle_path in first.bundle_paths:
            assert not (bundle_path / "page_images").exists()
        second = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, render_pages="vision_only", overwrite=False)
        assert all(e.reused for e in second.entries)
        docs = second.load_documents()
        assert all(page.page_image is None for doc in docs for page in doc.pages)


class TestSinglePassAndParallelCorpusLoading:
    def test_load_corpus_documents_exported_and_avoids_double_bundle_read(self, tmp_path, sample_pdfs, monkeypatch):
        assert "load_corpus_documents" in corpus_module.__all__
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus_single_pass"
        extracted = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, render_pages=False)

        load_calls: list[Path] = []
        lock = threading.Lock()
        real_load_bundle = corpus_module.load_bundle

        def spy_load_bundle(path):
            with lock:
                load_calls.append(Path(path))
            return real_load_bundle(path)

        monkeypatch.setattr(corpus_module, "load_bundle", spy_load_bundle)

        # 1. load_corpus_documents(path) verifies & loads each bundle once; subsequent load_documents() re-reads 0x.
        manifest, docs = load_corpus_documents(out)
        assert manifest == extracted
        assert [d.metadata.source_name for d in docs] == ["alpha.pdf", "beta.pdf"]
        assert len(load_calls) == 2
        assert manifest.load_documents() == docs
        assert len(load_calls) == 2

        # 2. load_corpus(path) followed by manifest.load_documents() verifies & reads each bundle once, not twice.
        load_calls.clear()
        loaded_manifest = load_corpus(out / CORPUS_MANIFEST)
        assert loaded_manifest == extracted
        assert len(load_calls) == 2
        docs_from_manifest = loaded_manifest.load_documents()
        assert docs_from_manifest == docs
        assert len(load_calls) == 2

        # 3. Uncached manifest (from extract_corpus) caches after first load_documents() call; empty & 1-entry work.
        load_calls.clear()
        first_docs = extracted.load_documents()
        assert len(load_calls) == 2
        assert extracted.load_documents() == first_docs
        assert len(load_calls) == 2

        corrupt_pdf = tmp_path / "only_corrupt.pdf"
        corrupt_pdf.write_bytes(b"%PDF-broken")
        out_empty = tmp_path / "corpus_zero_entries"
        extract_corpus(corrupt_pdf, out_empty, dpi=72, vision_dpi=72, strict=False)
        empty_manifest, empty_docs = load_corpus_documents(out_empty)
        assert empty_manifest.entries == ()
        assert empty_docs == ()
        assert empty_manifest.load_documents() == ()

    def test_cached_manifest_still_detects_post_load_bundle_tampering(self, tmp_path, sample_pdfs):
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus_tamper_after_load"
        extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, render_pages=False)

        # Same-size byte corruption in document.json after load_corpus() must invalidate stamp and raise.
        m1 = load_corpus(out)
        doc_json = out / m1.entries[1].bundle_relpath / "json" / "document.json"
        original = doc_json.read_bytes()
        corrupted = bytearray(original)
        idx = original.index(b"Beta Page 1")
        corrupted[idx : idx + 4] = b"XXXX"
        assert len(corrupted) == len(original)
        doc_json.write_bytes(bytes(corrupted))
        st = doc_json.stat()
        os.utime(doc_json, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
        with pytest.raises(BundleCorruptError):
            m1.load_documents()

        # Restore document.json, reload cleanly, then delete an artifact file after load_corpus().
        doc_json.write_bytes(original)
        m2 = load_corpus(out)
        md_file = out / m2.entries[0].bundle_relpath / "markdown" / "document.md"
        md_file.unlink()
        with pytest.raises(BundleCorruptError):
            m2.load_documents()

    def test_parallel_entry_verification_preserves_order_and_deterministic_first_error(self, tmp_path, monkeypatch):
        pdf_dir = tmp_path / "five_pdfs"
        pdf_dir.mkdir()
        pdfs = [
            build_pdf(pdf_dir / f"doc_{i:02d}.pdf", [PageSpec(items=(TextRun(f"Doc {i}", 72.0, 100.0),))])
            for i in range(5)
        ]
        out = tmp_path / "corpus_parallel"
        extracted = extract_corpus(pdfs, out, dpi=72, vision_dpi=72, render_pages=False)

        active = 0
        peak_active = 0
        lock = threading.Lock()
        all_started = threading.Event()
        real_load = _bundle_load._loaded

        def overlapping_load(root: Path):
            nonlocal active, peak_active
            with lock:
                active += 1
                peak_active = max(peak_active, active)
                if active >= 2:
                    all_started.set()
            all_started.wait(timeout=2.0)
            try:
                return real_load(root)
            finally:
                with lock:
                    active -= 1

        monkeypatch.setattr(_bundle_load, "_loaded", overlapping_load)
        manifest, docs = load_corpus_documents(out)
        assert peak_active >= 2
        assert [d.metadata.source_name for d in docs] == [f"doc_{i:02d}.pdf" for i in range(5)]
        assert manifest == extracted
        monkeypatch.undo()

        # Deterministic error order: entry 1 fails slower than entry 3, yet entry 1's error is always raised!
        entry1_rel = extracted.entries[1].bundle_relpath
        entry3_rel = extracted.entries[3].bundle_relpath
        real_lb = corpus_module.load_bundle

        def fail_two(path):
            p = Path(path)
            if p.name == entry1_rel:
                time.sleep(0.05)
                raise BundleCorruptError("corrupt_entry_1")
            if p.name == entry3_rel:
                raise BundleCorruptError("corrupt_entry_3")
            return real_lb(path)

        monkeypatch.setattr(corpus_module, "load_bundle", fail_two)
        with pytest.raises(BundleCorruptError, match="corrupt_entry_1"):
            load_corpus(out)

    def test_corpus_functions_obey_length_and_nesting_bounds(self):
        compound = (ast.If, ast.For, ast.AsyncFor, ast.While, ast.With, ast.AsyncWith, ast.Try, ast.Match)

        def max_nesting(node: ast.AST, depth: int = 0) -> int:
            best = depth
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    continue
                nxt = depth + (1 if isinstance(child, compound) else 0)
                best = max(best, max_nesting(child, nxt))
            return best

        checked_names = {
            "load_documents",
            "_can_spawn_corpus_procs",
            "_execute_corpus_batch",
            "load_corpus_documents",
            "load_corpus",
            "_resolve_inputs",
            "_partition_reusable_items",
            "_run_spawn_batch",
            "_verified_bundle_doc_raw",
            "_reused_entry",
            "_file_stat",
            "_bundle_artifact_paths",
            "_capture_bundle_stamp",
            "_stamps_valid",
            "_load_one_entry",
            "_submit_map_window",
            "_drain_done_map",
            "_map_bounded",
            "_load_verified_entries",
        }
        tree = ast.parse(Path(corpus_module.__file__).read_text(encoding="utf-8"))
        seen: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in checked_names:
                seen.add(node.name)
                span = (node.end_lineno or node.lineno) - node.lineno + 1
                assert span <= 50, f"{node.name} is {span} lines (> 50)"
                assert max_nesting(node) <= 3, f"{node.name} exceeds AST nesting 3"
        assert seen == checked_names


class TestWave2Rank6ManifestFirstAndParallelReuse:
    def test_manifest_first_reused_entry_skips_artifacts_on_mismatch_and_skips_full_doc_decode_on_hit(
        self, tmp_path, sample_pdfs, monkeypatch
    ):
        from datagrunt.pdf import codec

        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus_reuse_fast"
        first = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72)
        assert len(first.entries) == 2

        verified_calls: list[str] = []
        decode_calls: list[int] = []
        lock = threading.Lock()
        real_verified = _bundle_load._verified
        real_decode = codec.document_from_json_bytes

        def spy_verified(root: Path, entry, *, keep: bool):
            with lock:
                verified_calls.append(entry.path)
            return real_verified(root, entry, keep=keep)

        def spy_decode(*args, **kwargs):
            with lock:
                decode_calls.append(1)
            return real_decode(*args, **kwargs)

        monkeypatch.setattr(corpus_module, "_verified", spy_verified, raising=False)
        monkeypatch.setattr(_bundle_load, "_verified", spy_verified)
        monkeypatch.setattr(codec, "document_from_json_bytes", spy_decode)
        monkeypatch.setattr(_bundle_load, "document_from_json_bytes", spy_decode)

        # 1. Param mismatch (dpi=150 vs 72): _reused_entry rejects from manifest.json alone without reading artifacts!
        opts_mismatch = corpus_module.PipelineOptions(dpi=150, vision_dpi=150)
        items = corpus_module._resolve_inputs([pdf_a, pdf_b])
        _, exp_mismatch = corpus_module._expected_params_for(opts_mismatch, include_images=True, render_pages=True)
        for it in items:
            assert corpus_module._reused_entry(out / it.doc_id, it, it.doc_id, exp_mismatch) is None
        assert verified_calls == []
        assert decode_calls == []

        # 2. Clean match: SHA-256-verifies artifacts via _verified, but skips full document_from_json_bytes decode!
        opts_match = corpus_module.PipelineOptions(dpi=72, vision_dpi=72)
        _, exp_match = corpus_module._expected_params_for(opts_match, include_images=True, render_pages=True)
        reused = [corpus_module._reused_entry(out / it.doc_id, it, it.doc_id, exp_match) for it in items]
        assert all(r is not None and r.reused for r in reused)
        assert [(r.doc_id, r.page_count, r.degraded_pages) for r in reused if r is not None] == [
            (e.doc_id, e.page_count, e.degraded_pages) for e in first.entries
        ]
        assert any(p.endswith(".png") for p in verified_calls)
        assert decode_calls == []

    def test_reused_entry_preserves_sha256_symlink_and_metadata_tamper_rejections(self, tmp_path, sample_pdfs):
        pdf_a, pdf_b = sample_pdfs
        out = tmp_path / "corpus_tamper_reuse"
        first = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72)
        b0 = out / first.entries[0].bundle_relpath
        b1 = out / first.entries[1].bundle_relpath
        items = corpus_module._resolve_inputs([pdf_a, pdf_b])
        opts = corpus_module.PipelineOptions(dpi=72, vision_dpi=72)
        _, exp = corpus_module._expected_params_for(opts, include_images=True, render_pages=True)

        # 1. Same-size in-place byte corruption of page PNG in bundle 0 -> rejected and cleanly re-extracted!
        png_path = b0 / "page_images" / "page_0001.png"
        orig_png = png_path.read_bytes()
        bad_png = bytearray(orig_png)
        bad_png[-4:] = bytes(b ^ 0xFF for b in bad_png[-4:])
        png_path.write_bytes(bytes(bad_png))

        rebuilt = extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, overwrite=False)
        assert [e.reused for e in rebuilt.entries] == [False, True]
        assert len(load_corpus(out).entries) == 2

        # 2. Symlinked artifact file or symlinked intermediate directory -> _reused_entry returns None!
        md_path = b1 / "markdown" / "document.md"
        outside = tmp_path / "outside.md"
        outside.write_bytes(md_path.read_bytes())
        md_path.unlink()
        md_path.symlink_to(outside)
        assert corpus_module._reused_entry(b1, items[1], items[1].doc_id, exp) is None

        # 3. Symlinked subdirectory inside bundle -> _reused_entry returns None!
        md_path.unlink()
        md_path.write_bytes(outside.read_bytes())
        assert corpus_module._reused_entry(b1, items[1], items[1].doc_id, exp) is not None
        real_md_dir = tmp_path / "moved_markdown"
        os.rename(b1 / "markdown", real_md_dir)
        (b1 / "markdown").symlink_to(real_md_dir)
        assert corpus_module._reused_entry(b1, items[1], items[1].doc_id, exp) is None

    def test_parallel_resolve_inputs_and_reused_entry_overlap_and_preserve_error_order(self, tmp_path, monkeypatch):
        pdf_dir = tmp_path / "par_pdfs"
        pdf_dir.mkdir()
        pdfs = [
            build_pdf(pdf_dir / f"p_{i:02d}.pdf", [PageSpec(items=(TextRun(f"P {i}", 72.0, 100.0),))]) for i in range(4)
        ]
        out = tmp_path / "par_out"
        extract_corpus(pdfs, out, dpi=72, vision_dpi=72, render_pages=False)

        # 1. _resolve_inputs runs _file_sha256_and_identity in bounded parallel
        active_hash = 0
        peak_hash = 0
        hash_started = threading.Event()
        lock = threading.Lock()
        real_hash = corpus_module._file_sha256_and_identity

        def overlap_hash(path: Path):
            nonlocal active_hash, peak_hash
            with lock:
                active_hash += 1
                peak_hash = max(peak_hash, active_hash)
                if active_hash >= 2:
                    hash_started.set()
            hash_started.wait(timeout=2.0)
            try:
                return real_hash(path)
            finally:
                with lock:
                    active_hash -= 1

        monkeypatch.setattr(corpus_module, "_file_sha256_and_identity", overlap_hash)
        resolved_items = corpus_module._resolve_inputs(pdfs)
        assert peak_hash >= 2
        assert [it.source_name for it in resolved_items] == [f"p_{i:02d}.pdf" for i in range(4)]

        # Deterministic error precedence in _resolve_inputs: index 1 slower than index 3, index 1 wins!
        def fail_hash(path: Path):
            if path.name == "p_01.pdf":
                time.sleep(0.05)
                raise OSError("io_err_01")
            if path.name == "p_03.pdf":
                raise OSError("io_err_03")
            return real_hash(path)

        monkeypatch.setattr(corpus_module, "_file_sha256_and_identity", fail_hash)
        with pytest.raises(OSError, match="io_err_01"):
            corpus_module._resolve_inputs(pdfs)
        monkeypatch.setattr(corpus_module, "_file_sha256_and_identity", real_hash)

        # 2. extract_corpus(..., overwrite=False) runs _reused_entry in bounded parallel
        active_reuse = 0
        peak_reuse = 0
        reuse_started = threading.Event()
        real_reuse = corpus_module._reused_entry

        def overlap_reuse(bdir: Path, item, rel: str, exp: dict[str, object]):
            nonlocal active_reuse, peak_reuse
            with lock:
                active_reuse += 1
                peak_reuse = max(peak_reuse, active_reuse)
                if active_reuse >= 2:
                    reuse_started.set()
            reuse_started.wait(timeout=2.0)
            try:
                return real_reuse(bdir, item, rel, exp)
            finally:
                with lock:
                    active_reuse -= 1

        monkeypatch.setattr(corpus_module, "_reused_entry", overlap_reuse)
        second = extract_corpus(pdfs, out, max_workers=4, dpi=72, vision_dpi=72, render_pages=False, overwrite=False)
        assert peak_reuse >= 2
        assert all(e.reused for e in second.entries)


@pytest.fixture(scope="module")
def mixed_pdfs(tmp_path_factory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("corpus_image_only")
    grey_box = Rect(400.0, 600.0, 60.0, 40.0, fill_rgb=(0.5, 0.5, 0.5), stroke=False)
    short = PageSpec(items=(TextRun("Too short", 72.0, 100.0), grey_box))
    digital = PageSpec(items=(TextRun("Digital page with ordinary readable body text.", 72.0, 100.0),))
    pdf_a = build_pdf(root / "a_mixed.pdf", [digital, short])
    pdf_b = build_pdf(root / "b_mixed.pdf", [short, digital, digital])
    break_page(pdf_b, 3)
    return pdf_a, pdf_b


class TestImageOnlyPages:
    """Corpus index carries ``image_only_pages`` alongside ``degraded_pages`` with 5.2 back-compat (#474)."""

    def test_single_worker_extract_reuse_and_round_trip(self, tmp_path, mixed_pdfs):
        pdf_a, pdf_b = mixed_pdfs
        out = tmp_path / "corpus_single"
        first = extract_corpus([pdf_a, pdf_b], out, max_workers=1, dpi=72, vision_dpi=72, ocr="never")

        assert first.corpus_format_version == "5.3"
        assert [(e.degraded_pages, e.image_only_pages, e.reused) for e in first.entries] == [
            ((), (2,), False),
            ((3,), (1, 3), False),
        ]

        raw = json.loads((out / CORPUS_MANIFEST).read_text(encoding="ascii"))
        assert raw["corpus_format_version"] == "5.3"
        assert list(raw["entries"][0]) == [
            "doc_id",
            "source_name",
            "source_sha256",
            "bundle_relpath",
            "page_count",
            "integrity_verified",
            "degraded_pages",
            "image_only_pages",
            "reused",
        ]
        assert [e["image_only_pages"] for e in raw["entries"]] == [[2], [1, 3]]

        reloaded, docs = load_corpus_documents(out)
        assert reloaded == first
        assert load_corpus(out) == first
        assert [d.image_only_pages for d in docs] == [(2,), (1, 3)]

        reused = extract_corpus([pdf_a, pdf_b], out, max_workers=1, dpi=72, vision_dpi=72, ocr="never", overwrite=False)
        assert [(e.degraded_pages, e.image_only_pages, e.reused) for e in reused.entries] == [
            ((), (2,), True),
            ((3,), (1, 3), True),
        ]
        assert load_corpus(out) == reused

    def test_multi_worker_spawn_pool_and_reuse_carry_image_only_pages(self, tmp_path, mixed_pdfs, monkeypatch):
        pdf_a, pdf_b = mixed_pdfs
        out = tmp_path / "corpus_spawn"
        spawn_pids: list[int] = []
        real_run_in_worker = corpus_module._run_in_corpus_worker

        def spy_worker(*args, **kwargs):
            pid, outcome = real_run_in_worker(*args, **kwargs)
            spawn_pids.append(pid)
            return pid, outcome

        monkeypatch.setattr(corpus_module, "_run_in_corpus_worker", spy_worker)
        first = extract_corpus([pdf_a, pdf_b], out, max_workers=2, dpi=72, vision_dpi=72, ocr="never")

        assert len(spawn_pids) == 2
        assert all(pid != os.getpid() for pid in spawn_pids)
        assert [(e.degraded_pages, e.image_only_pages, e.reused) for e in first.entries] == [
            ((), (2,), False),
            ((3,), (1, 3), False),
        ]
        raw = json.loads((out / CORPUS_MANIFEST).read_text(encoding="ascii"))
        assert [e["image_only_pages"] for e in raw["entries"]] == [[2], [1, 3]]
        assert load_corpus(out) == first

        reused = extract_corpus([pdf_a, pdf_b], out, max_workers=2, dpi=72, vision_dpi=72, ocr="never", overwrite=False)
        assert [(e.degraded_pages, e.image_only_pages, e.reused) for e in reused.entries] == [
            ((), (2,), True),
            ((3,), (1, 3), True),
        ]
        assert load_corpus(out) == reused

    def test_reused_entry_rejects_bundle_with_invalid_image_only_pages_in_manifest(self, tmp_path, mixed_pdfs):
        pdf_a, _ = mixed_pdfs
        out = tmp_path / "corpus_tampered_bundle"
        first = extract_corpus([pdf_a], out, dpi=72, vision_dpi=72, ocr="never")
        bundle_manifest = out / first.entries[0].bundle_relpath / "manifest.json"
        data = json.loads(bundle_manifest.read_text(encoding="ascii"))
        data["image_only_pages"] = [99]
        bundle_manifest.write_text(json.dumps(data), encoding="ascii")

        rebuilt = extract_corpus([pdf_a], out, dpi=72, vision_dpi=72, ocr="never", overwrite=False)
        assert rebuilt.entries[0].reused is False
        assert rebuilt.entries[0].image_only_pages == (2,)

    @pytest.mark.parametrize("downgrade_bundles", [False, True])
    def test_older_5_2_corpus_manifest_loads_without_image_only_pages(self, tmp_path, mixed_pdfs, downgrade_bundles):
        pdf_a, pdf_b = mixed_pdfs
        out = tmp_path / f"corpus_5_2_{downgrade_bundles}"
        extract_corpus([pdf_a, pdf_b], out, dpi=72, vision_dpi=72, ocr="never")
        as_corpus_5_2(out, downgrade_bundles=downgrade_bundles)

        raw = json.loads((out / CORPUS_MANIFEST).read_text(encoding="ascii"))
        assert raw["corpus_format_version"] == "5.2"
        assert all("image_only_pages" not in entry for entry in raw["entries"])

        loaded, docs = load_corpus_documents(out)
        assert loaded.corpus_format_version == "5.2"
        assert [e.image_only_pages for e in loaded.entries] == [None, None]
        assert [e.degraded_pages for e in loaded.entries] == [(), (3,)]
        expected_doc_pages = [(), ()] if downgrade_bundles else [(2,), (1, 3)]
        assert [d.image_only_pages for d in docs] == expected_doc_pages
        assert loaded.load_documents() == docs

    @pytest.mark.parametrize(
        ("mutate", "pattern"),
        [
            (lambda e: e.pop("image_only_pages"), r"missing key\(s\) \['image_only_pages'\]"),
            (lambda e: e.update(image_only_pages=None), "image_only_pages must be a JSON array"),
            (lambda e: e.update(image_only_pages="2"), "image_only_pages must be a JSON array"),
            (lambda e: e.update(image_only_pages=[0]), r"image_only_pages\[0\] must be >= 1"),
            (lambda e: e.update(image_only_pages=[True]), r"image_only_pages\[0\] must be an int"),
            (lambda e: e.update(image_only_pages=[99]), r"image_only_pages\[0\].*page_count"),
            (lambda e: e.update(image_only_pages=[2, 1]), "image_only_pages must be strictly increasing"),
            (lambda e: e.update(image_only_pages=[1, 1]), "image_only_pages must be strictly increasing"),
            (lambda e: e.update(image_only_pages=[]), r"image_only_pages \[\] disagree with bundle \[2\]"),
            (lambda e: e.update(image_only_pages=[1]), r"image_only_pages \[1\] disagree with bundle \[2\]"),
        ],
    )
    def test_corrupt_image_only_pages_in_5_3_manifest_is_rejected(self, tmp_path, mixed_pdfs, mutate, pattern):
        pdf_a, _ = mixed_pdfs
        out = tmp_path / "corpus_bad_manifest"
        extract_corpus([pdf_a], out, dpi=72, vision_dpi=72, ocr="never")
        manifest_path = out / CORPUS_MANIFEST
        raw = json.loads(manifest_path.read_text(encoding="ascii"))
        mutate(raw["entries"][0])
        manifest_path.write_text(json.dumps(raw), encoding="ascii")

        with pytest.raises(BundleCorruptError, match=pattern):
            load_corpus(out)

    def test_5_2_corpus_manifest_rejects_unknown_image_only_pages_key(self, tmp_path, mixed_pdfs):
        pdf_a, _ = mixed_pdfs
        out = tmp_path / "corpus_5_2_extra_key"
        extract_corpus([pdf_a], out, dpi=72, vision_dpi=72, ocr="never")
        as_corpus_5_2(out)
        manifest_path = out / CORPUS_MANIFEST
        raw = json.loads(manifest_path.read_text(encoding="ascii"))
        raw["entries"][0]["image_only_pages"] = [2]
        manifest_path.write_text(json.dumps(raw), encoding="ascii")

        with pytest.raises(BundleCorruptError, match=r"unknown key\(s\) \['image_only_pages'\]"):
            load_corpus(out)
