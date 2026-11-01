"""Tests for the v5 bundle load (#371, A7a): ``datagrunt.pdf.bundle.load_bundle``.

A bundle is written once per module (real renders) and copied per test; a
corrupt bundle is that copy with one file or one manifest value changed.
:func:`resigned` re-records an artifact's size and sha256 in the manifest,
to reach the checks behind the checksums.
"""

import dataclasses
import hashlib
import json
import os
import pickle
import shutil
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from datagrunt.pdf import bundle
from datagrunt.pdf.bundle import load_bundle
from datagrunt.pdf.exceptions import BundleCorruptError, DatagruntPDFError
from pdf_tests.builders import PageSpec, TextRun, build_pdf
from pdf_tests.documents import FIGURE_A_SHA, FIGURE_B_SHA, metadata, surrogate_document
from pdf_tests.test_bundle import PNG_A, PNG_B, bound_document, fake_render, sha_of, source_pdf, write

A_PNG = f"images/{FIGURE_A_SHA}.png"


@pytest.fixture(scope="module")
def written(tmp_path_factory):
    directory = tmp_path_factory.mktemp("load")
    path = source_pdf(directory)
    out = directory / "bundle"
    returned = write(bound_document(path), out, path)
    return SimpleNamespace(source=path, out=out, returned=returned)


@pytest.fixture
def copy(written, tmp_path):
    """A fresh copy of the module's bundle (symlinks kept as symlinks)."""
    target = tmp_path / "bundle"
    shutil.copytree(written.out, target, symlinks=True)
    return target


def manifest(out: Path) -> dict:
    return json.loads((out / "manifest.json").read_text(encoding="ascii"))


def save_manifest(out: Path, data: dict) -> None:
    (out / "manifest.json").write_text(json.dumps(data, indent=2), encoding="ascii")


def edit_manifest(out: Path, change) -> None:
    data = manifest(out)
    change(data)
    save_manifest(out, data)


def entry_of(data: dict, path: str) -> dict:
    [entry] = [item for item in data["artifacts"] if item["path"] == path]
    return entry


def resigned(out: Path, path: str, *, file: Path | None = None) -> None:
    """Record ``path``'s artifact entry as the size and sha256 of ``file`` (default: the file at ``path``)."""
    data = (file or out / path).read_bytes()

    def change(manifest_data):
        entry = entry_of(manifest_data, path)
        entry["size"], entry["sha256"] = len(data), hashlib.sha256(data).hexdigest()

    edit_manifest(out, change)


def edit_document_json(out: Path, change) -> None:
    path = out / "json" / "document.json"
    data = json.loads(path.read_bytes())
    change(data)
    path.write_bytes(json.dumps(data).encode("ascii"))
    resigned(out, "json/document.json")


class TestRoundTrip:
    def test_load_of_a_written_bundle_is_the_written_document(self, written):
        loaded = load_bundle(written.out)
        assert loaded == written.returned
        assert loaded.metadata == written.returned.metadata

    def test_the_manifest_path_and_a_str_load_it_too(self, written):
        assert load_bundle(written.out / "manifest.json") == written.returned
        assert load_bundle(str(written.out)) == written.returned

    def test_paths_point_into_the_bundle(self, written):
        loaded = load_bundle(written.out)
        for mine, theirs in zip(loaded.figures, written.returned.figures):
            assert mine.image_path == theirs.image_path
        for mine, theirs in zip(loaded.pages, written.returned.pages):
            assert (mine.page_image.image_path, mine.page_image.band_paths) == (
                theirs.page_image.image_path,
                theirs.page_image.band_paths,
            )

    def test_figure_and_page_image_bytes_are_equal(self, written):
        loaded = load_bundle(written.out)
        assert [figure.read_bytes() for figure in loaded.figures] == [PNG_A, PNG_A, PNG_B]
        for mine, theirs in zip(loaded.pages, written.returned.pages):
            assert mine.page_image.read_bytes() == theirs.page_image.read_bytes()
            assert mine.page_image.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
        for page in loaded.pages[2:]:
            assert len(page.page_image.read_bands()) == 2

    def test_sections_hold_the_pages_figures(self, written):
        loaded = load_bundle(written.out)
        assert [f.image_path for s in loaded.sections for f in s.figures] == [f.image_path for f in loaded.figures]

    def test_a_bundle_with_missing_artifacts_round_trips(self, tmp_path, monkeypatch):
        source = source_pdf(tmp_path)

        def failing(src, page_index, **kwargs):
            if page_index == 1:
                raise bundle.RenderFailed("timed out")
            return fake_render(src, page_index, **kwargs)

        monkeypatch.setattr(bundle, "render_page", failing)
        returned = write(bound_document(source, None), tmp_path / "out", source)
        loaded = load_bundle(tmp_path / "out")
        assert loaded == returned
        assert loaded.pages[1].page_image is None and loaded.figures[0].image_path is None

    def test_lone_surrogates_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bundle, "render_page", fake_render)
        path = build_pdf(tmp_path / "s.pdf", [PageSpec(items=(TextRun("x", 72.0, 100.0),))])
        document = dataclasses.replace(
            surrogate_document(), metadata=metadata(1, source_name="s.pdf", sha=sha_of(path))
        )
        returned = write(document, tmp_path / "out", path)
        loaded = load_bundle(tmp_path / "out")
        assert loaded == returned and loaded.elements == document.elements
        assert loaded.pages[0].elements[1].text == "lone \udfff low"

    def test_a_loaded_document_writes_again(self, written, tmp_path, monkeypatch):
        monkeypatch.setattr(bundle, "render_page", fake_render)
        loaded = load_bundle(written.out)
        again = write(loaded, tmp_path / "again", written.source)
        assert (tmp_path / "again" / "images" / f"{FIGURE_B_SHA}.png").read_bytes() == PNG_B
        assert load_bundle(tmp_path / "again") == again


class TestChecksums:
    ARTIFACTS = [
        "json/document.json",
        "markdown/document.md",
        A_PNG,
        "page_images/page_0001.png",
        "page_images/bands/page_0004_band_02.png",
    ]

    @pytest.mark.parametrize("path", ARTIFACTS)
    def test_a_changed_byte_is_refused_naming_the_artifact(self, copy, path):
        target = copy / path
        data = bytearray(target.read_bytes())
        data[len(data) // 2] ^= 0xFF
        target.write_bytes(bytes(data))
        with pytest.raises(BundleCorruptError, match=f"{path}: its sha256 is [0-9a-f]{{64}}, the manifest's is"):
            load_bundle(copy)

    @pytest.mark.parametrize("path", ARTIFACTS)
    def test_a_changed_size_is_refused_naming_the_artifact(self, copy, path):
        with (copy / path).open("ab") as handle:
            handle.write(b"\x00")
        with pytest.raises(BundleCorruptError, match=f"{path}: it is [0-9]+ bytes, the manifest says [0-9]+"):
            load_bundle(copy)

    def test_a_grown_file_is_refused_by_its_fstat_size_before_it_is_read(self, copy, monkeypatch):
        size = entry_of(manifest(copy), A_PNG)["size"]
        with (copy / A_PNG).open("ab") as handle:
            handle.write(b"\x00" * 1000)
        read = []
        real = bundle._read_fd
        monkeypatch.setattr(
            bundle, "_read_fd", lambda fd, limit, *, keep: read.append(limit) or real(fd, limit, keep=keep)
        )
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is {size + 1000} bytes, the manifest says {size}"):
            load_bundle(copy)
        assert size not in read

    def test_a_manifest_size_that_disagrees_is_refused(self, copy):
        edit_manifest(copy, lambda data: entry_of(data, A_PNG).update(size=entry_of(data, A_PNG)["size"] - 1))
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is"):
            load_bundle(copy)

    def test_a_manifest_sha256_that_disagrees_is_refused(self, copy):
        edit_manifest(copy, lambda data: entry_of(data, A_PNG).update(sha256="0" * 64))
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: its sha256"):
            load_bundle(copy)

    def test_a_missing_artifact_is_refused(self, copy):
        (copy / "page_images" / "page_0002.png").unlink()
        with pytest.raises(BundleCorruptError, match="page_images/page_0002.png: it is missing"):
            load_bundle(copy)

    def test_a_directory_in_an_artifacts_place_is_refused(self, copy):
        (copy / A_PNG).unlink()
        (copy / A_PNG).mkdir()
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is not a regular file"):
            load_bundle(copy)

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="no named pipes here")
    def test_a_named_pipe_is_refused_without_opening_it(self, copy):
        (copy / A_PNG).unlink()
        os.mkfifo(copy / A_PNG)
        outcome = calls_without_blocking(lambda: load_bundle(copy))
        assert isinstance(outcome.get("error"), BundleCorruptError)
        assert f"{A_PNG}: it is not a regular file" in str(outcome["error"])

    def test_an_artifact_over_the_asset_limit_is_refused_unread(self, copy):
        edit_manifest(copy, lambda data: entry_of(data, A_PNG).update(size=(1 << 30) + 1))
        message = rf"manifest\.json: artifacts\[0\] \('{A_PNG}'\): size 1073741825 is over the 1073741824-byte"
        with pytest.raises(BundleCorruptError, match=message):
            load_bundle(copy)

    def test_a_manifest_over_the_asset_limit_is_refused(self, copy, monkeypatch):
        size = (copy / "manifest.json").stat().st_size
        monkeypatch.setattr(bundle, "_MAX_ARTIFACT_BYTES", size - 1)
        with pytest.raises(BundleCorruptError, match=f"manifest.json: it is over the {size - 1}-byte asset limit"):
            load_bundle(copy)

    @pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="needs file permissions that bind")
    def test_an_artifact_that_cannot_be_opened_is_refused(self, copy):
        (copy / A_PNG).chmod(0)
        try:
            with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it cannot be opened: .*Permission denied"):
                load_bundle(copy)
        finally:
            (copy / A_PNG).chmod(0o644)

    def test_a_file_that_shrinks_while_it_is_read_is_refused(self, copy, monkeypatch):
        real = bundle._read_fd

        def short(descriptor, limit, *, keep):
            total, digest, data = real(descriptor, limit, keep=keep)
            return total - 1, digest, data

        monkeypatch.setattr(bundle, "_read_fd", short)
        size = entry_of(manifest(copy), A_PNG)["size"]
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is {size - 1} bytes, the manifest says {size}"):
            load_bundle(copy)

    def test_the_artifact_limit_is_the_asset_limit(self):
        assert bundle._MAX_ARTIFACT_BYTES == 1 << 30


class TestContainment:
    @pytest.mark.parametrize(
        "bad", ["../outside.png", "/etc/hosts", "images/../../outside.png", "images/C:evil.png", "images/con.png"]
    )
    def test_a_manifest_path_out_of_the_bundle_is_refused(self, copy, bad):
        (copy.parent / "outside.png").write_bytes(PNG_A)
        edit_manifest(copy, lambda data: entry_of(data, A_PNG).update(path=bad))
        with pytest.raises(BundleCorruptError, match=r"manifest\.json") as info:
            load_bundle(copy)
        assert repr(bad) in str(info.value)

    def test_a_symlinked_artifact_out_of_the_bundle_is_refused_though_its_bytes_match(self, copy):
        outside = copy.parent / "outside.png"
        outside.write_bytes((copy / A_PNG).read_bytes())
        (copy / A_PNG).unlink()
        (copy / A_PNG).symlink_to(outside)
        with pytest.raises(BundleCorruptError, match="resolves outside"):
            load_bundle(copy)

    def test_a_symlinked_directory_out_of_the_bundle_is_refused(self, copy):
        outside = copy.parent / "elsewhere"
        shutil.move(copy / "images", outside)
        (copy / "images").symlink_to(outside, target_is_directory=True)
        with pytest.raises(BundleCorruptError, match="resolves outside"):
            load_bundle(copy)

    def test_a_symlinked_manifest_out_of_the_bundle_is_refused(self, copy):
        outside = copy.parent / "manifest.json"
        shutil.move(copy / "manifest.json", outside)
        (copy / "manifest.json").symlink_to(outside)
        with pytest.raises(BundleCorruptError, match="resolves outside"):
            load_bundle(copy)

    def test_a_document_path_must_be_a_verified_artifact(self, copy):
        shutil.copy(copy / A_PNG, copy / "images" / "extra.png")

        def change(data):
            data["pages"][0]["elements"][8]["image_path"] = "images/extra.png"

        edit_document_json(copy, change)
        with pytest.raises(BundleCorruptError, match="'images/extra.png' is not an artifact the manifest lists"):
            load_bundle(copy)

    def test_a_document_path_out_of_the_bundle_is_refused(self, copy):
        def change(data):
            data["pages"][0]["elements"][8]["image_path"] = "../outside.png"

        edit_document_json(copy, change)
        with pytest.raises(BundleCorruptError, match=r"json/document\.json: .*'\.\./outside\.png'"):
            load_bundle(copy)


class TestManifest:
    @pytest.mark.parametrize("version", ["6.0", "4.0", 5.0, None, ""])
    def test_an_unknown_bundle_version_is_refused(self, copy, version):
        edit_manifest(copy, lambda data: data.update(bundle_format_version=version))
        with pytest.raises(BundleCorruptError, match=r"manifest\.json: bundle_format_version .* is not supported"):
            load_bundle(copy)

    @pytest.mark.parametrize(
        ("change", "message"),
        [
            (lambda d: d.update(surprise=1), r"unknown key\(s\) \['surprise'\]"),
            (lambda d: d.pop("artifacts"), r"missing key\(s\) \['artifacts'\]"),
            (lambda d: d.pop("missing"), r"missing key\(s\) \['missing'\]"),
            (lambda d: d.update(datagrunt_version=4), "datagrunt_version must be a str"),
            (lambda d: d.update(datagrunt_version=" "), "datagrunt_version must be a non-blank str"),
            (lambda d: d.update(source_sha256="abc"), "source_sha256 must be 64 lowercase hex"),
            (lambda d: d["extraction_params"].update(dpi=10), "dpi must be >= 36"),
            (lambda d: d["extraction_params"].update(workers=True), "workers must be an int"),
            (lambda d: d["extraction_params"].pop("workers"), r"missing key\(s\) \['workers'\]"),
            (lambda d: d.update(integrity_verified=0), "integrity_verified must be a bool"),
            (lambda d: d.update(degraded_pages="3,4"), "degraded_pages must be a JSON array"),
            (lambda d: d.update(degraded_pages=[0]), r"degraded_pages\[0\] must be >= 1"),
            (lambda d: d.update(artifacts={}), "artifacts must be a JSON array"),
            (lambda d: d["artifacts"][0].update(size=-1), r"artifacts\[0\]\.size must be >= 0"),
            (lambda d: d["artifacts"][0].update(size=True), r"artifacts\[0\]\.size must be an int"),
            (lambda d: d["artifacts"][0].update(sha256="X" * 64), r"artifacts\[0\]\.sha256 must be 64 lowercase"),
            (lambda d: d["artifacts"][0].update(extra=1), r"artifacts\[0\] has unknown key"),
            (lambda d: d["artifacts"].append(dict(d["artifacts"][0])), "lists .* twice"),
            (
                lambda d: d.update(artifacts=[a for a in d["artifacts"] if a["path"] != "json/document.json"]),
                r"lists no json/document\.json",
            ),
            (
                lambda d: d["missing"].append({"artifact": "x", "page_number": 1, "element_id": None, "reason": "r"}),
                r"missing\[0\]\.artifact must be",
            ),
            (
                lambda d: d["missing"].append(
                    {"artifact": "bands", "page_number": 1, "element_id": None, "reason": " "}
                ),
                r"missing\[0\]\.reason must be a non-blank str",
            ),
            (
                lambda d: d["missing"].append({"artifact": "bands", "page_number": 1, "element_id": 3, "reason": "r"}),
                r"missing\[0\]\.element_id must be a str or None",
            ),
        ],
    )
    def test_a_bad_manifest_is_refused_naming_it(self, copy, change, message):
        edit_manifest(copy, change)
        with pytest.raises(BundleCorruptError, match=rf"manifest\.json: .*{message}"):
            load_bundle(copy)

    @pytest.mark.parametrize(
        ("raw", "message"),
        [
            (b"{", "not valid JSON"),
            (b'{"a": NaN}', "NaN is not JSON"),
            (b'{"a": 1, "a": 1}', "duplicate key 'a'"),
            (b"[]", "manifest must be a JSON object, got list"),
            (b"\xff", "not UTF-8"),
        ],
    )
    def test_a_manifest_that_is_not_strict_json_is_refused(self, copy, raw, message):
        (copy / "manifest.json").write_bytes(raw)
        with pytest.raises(BundleCorruptError, match=rf"manifest\.json: .*{message}"):
            load_bundle(copy)

    def test_bundle_corrupt_error_is_a_datagrunt_pdf_error(self):
        assert issubclass(BundleCorruptError, DatagruntPDFError)


class TestDocumentAgainstManifest:
    @pytest.mark.parametrize(
        ("change", "message"),
        [
            (lambda d: d.update(source_sha256="0" * 64), "source_sha256 .* is not the document's"),
            (lambda d: d.update(integrity_verified=True), "integrity_verified True is not the document's False"),
            (lambda d: d.update(degraded_pages=[3]), r"degraded_pages \[3\] are not the document's \[3, 4\]"),
        ],
    )
    def test_the_manifests_facts_must_be_the_documents(self, copy, change, message):
        edit_manifest(copy, change)
        with pytest.raises(BundleCorruptError, match=message):
            load_bundle(copy)

    def test_a_document_json_the_codec_refuses_is_refused_naming_it(self, copy):
        edit_document_json(copy, lambda data: data.update(version=2))
        with pytest.raises(BundleCorruptError, match=r"json/document\.json: version 2 is not supported"):
            load_bundle(copy)

    def test_a_page_without_its_image_needs_a_missing_entry(self, copy):
        edit_document_json(copy, lambda data: data["pages"][1].update(page_image=None))
        with pytest.raises(BundleCorruptError, match=r"unrecorded: \[\('page_image', 2, None\)\]"):
            load_bundle(copy)

    def test_a_vision_page_without_bands_needs_a_missing_entry(self, copy):
        edit_document_json(copy, lambda data: data["pages"][2]["page_image"].update(band_paths=[]))
        with pytest.raises(BundleCorruptError, match=r"unrecorded: \[\('bands', 3, None\)\]"):
            load_bundle(copy)

    def test_a_figure_without_its_image_needs_a_missing_entry(self, copy):
        def change(data):
            data["pages"][1]["elements"][4]["image_path"] = None

        edit_document_json(copy, change)
        with pytest.raises(BundleCorruptError, match=r"unrecorded: \[\('figure', 2, 'p0002_e004'\)\]"):
            load_bundle(copy)

    def test_a_missing_entry_listed_twice_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bundle, "render_page", lambda *a, **k: (_ for _ in ()).throw(bundle.RenderFailed("no")))
        source = source_pdf(tmp_path)
        write(bound_document(source), tmp_path / "out", source)
        edit_manifest(tmp_path / "out", lambda data: data["missing"].append(dict(data["missing"][0])))
        with pytest.raises(BundleCorruptError, match="missing lists an artifact twice"):
            load_bundle(tmp_path / "out")

    def test_a_missing_entry_for_what_is_there_is_refused(self, copy):
        stale = {"artifact": "page_image", "page_number": 1, "element_id": None, "reason": "not rendered"}
        edit_manifest(copy, lambda data: data["missing"].append(stale))
        with pytest.raises(BundleCorruptError, match=r"recorded but present: \[\('page_image', 1, None\)\]"):
            load_bundle(copy)


class TestArguments:
    def test_a_missing_path_is_refused(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_bundle(tmp_path / "nothing")

    def test_a_directory_without_a_manifest_is_refused(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="manifest.json"):
            load_bundle(tmp_path)

    def test_a_file_that_is_not_a_manifest_is_refused(self, tmp_path):
        (tmp_path / "doc.pdf").write_bytes(b"%PDF")
        with pytest.raises(ValueError, match="not a bundle directory or its manifest.json"):
            load_bundle(tmp_path / "doc.pdf")

    def test_path_must_be_a_path(self):
        with pytest.raises(TypeError, match="path must be a str or os.PathLike, got int"):
            load_bundle(3)


def flipped(path: Path) -> None:
    """Change one byte of the file at ``path`` in place: its size stays, its sha256 does not."""
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))


def calls_without_blocking(read):
    """Run ``read`` in a thread: a read that blocks (a named pipe opened for reading) fails the test."""
    outcome: dict = {}

    def run():
        try:
            outcome["value"] = read()
        except BaseException as exc:  # noqa: BLE001 - handed to the test thread
            outcome["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=10.0)
    assert not thread.is_alive(), "the read blocked"
    return outcome


class TestReadTime:
    """Every byte a loaded (or written) document returns matches the manifest when it is read."""

    def test_an_untouched_bundle_reads_its_bytes(self, copy):
        loaded = load_bundle(copy)
        assert loaded.figures[0].read_bytes() == PNG_A
        assert loaded.pages[3].page_image.read_bands() == tuple(
            path.read_bytes() for path in loaded.pages[3].page_image.band_paths
        )

    def test_paths_are_kept_for_information(self, copy):
        loaded = load_bundle(copy)
        assert loaded.figures[0].image_path == copy / A_PNG
        assert loaded.pages[0].page_image.image_path == copy / "page_images" / "page_0001.png"

    def test_an_image_overwritten_after_load_is_refused_naming_it(self, copy):
        loaded = load_bundle(copy)
        flipped(copy / A_PNG)
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: its sha256 is"):
            loaded.figures[0].read_bytes()

    def test_an_image_grown_after_load_is_refused(self, copy):
        loaded = load_bundle(copy)
        with (copy / A_PNG).open("ab") as handle:
            handle.write(b"x")
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is [0-9]+ bytes, the manifest says"):
            loaded.figures[1].read_bytes()

    def test_an_image_replaced_by_a_symlink_out_of_the_bundle_is_refused(self, copy):
        loaded = load_bundle(copy)
        outside = copy.parent / "outside.png"
        outside.write_bytes((copy / A_PNG).read_bytes())
        (copy / A_PNG).unlink()
        (copy / A_PNG).symlink_to(outside)
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: .*resolves outside"):
            loaded.figures[0].read_bytes()

    def test_an_image_replaced_by_a_symlink_to_its_twin_in_the_bundle_is_refused(self, copy):
        # Its bytes match and it stays in the bundle: only O_NOFOLLOW refuses it.
        loaded = load_bundle(copy)
        shutil.copy(copy / A_PNG, copy / "images" / "twin.png")
        (copy / A_PNG).unlink()
        (copy / A_PNG).symlink_to(copy / "images" / "twin.png")
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is a symlink"):
            loaded.figures[0].read_bytes()

    def test_a_symlinked_directory_inside_the_bundle_is_refused(self, copy):
        loaded = load_bundle(copy)
        shutil.move(copy / "images", copy / "real_images")
        (copy / "images").symlink_to(copy / "real_images", target_is_directory=True)
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: its directory 'images' is a symlink"):
            loaded.figures[0].read_bytes()
        with pytest.raises(BundleCorruptError, match="its directory 'images' is a symlink"):
            load_bundle(copy)

    def test_a_page_image_overwritten_after_load_is_refused(self, copy):
        loaded = load_bundle(copy)
        flipped(copy / "page_images" / "page_0002.png")
        with pytest.raises(BundleCorruptError, match="page_images/page_0002.png: its sha256"):
            loaded.pages[1].page_image.read_bytes()

    def test_a_band_overwritten_after_load_is_refused(self, copy):
        loaded = load_bundle(copy)
        flipped(copy / "page_images" / "bands" / "page_0003_band_02.png")
        with pytest.raises(BundleCorruptError, match="page_images/bands/page_0003_band_02.png: its sha256"):
            loaded.pages[2].page_image.read_bands()

    def test_a_band_replaced_by_a_symlink_is_refused(self, copy):
        loaded = load_bundle(copy)
        band = copy / "page_images" / "bands" / "page_0004_band_01.png"
        outside = copy.parent / "band.png"
        outside.write_bytes(band.read_bytes())
        band.unlink()
        band.symlink_to(outside)
        with pytest.raises(BundleCorruptError, match="page_0004_band_01.png: .*resolves outside"):
            loaded.pages[3].page_image.read_bands()

    def test_a_deleted_artifact_is_refused_at_read(self, copy):
        loaded = load_bundle(copy)
        (copy / "page_images" / "page_0001.png").unlink()
        with pytest.raises(BundleCorruptError, match="page_images/page_0001.png: it is missing"):
            loaded.pages[0].page_image.read_bytes()

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="no named pipes here")
    def test_a_named_pipe_planted_after_load_is_refused_without_blocking(self, copy):
        loaded = load_bundle(copy)
        (copy / A_PNG).unlink()
        os.mkfifo(copy / A_PNG)
        outcome = calls_without_blocking(loaded.figures[0].read_bytes)
        assert isinstance(outcome.get("error"), BundleCorruptError)
        assert f"{A_PNG}: it is not a regular file" in str(outcome["error"])

    def test_a_directory_planted_after_load_is_refused(self, copy):
        loaded = load_bundle(copy)
        (copy / A_PNG).unlink()
        (copy / A_PNG).mkdir()
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: it is not a regular file"):
            loaded.figures[0].read_bytes()

    def test_bands_over_the_asset_limit_in_total_are_refused(self, copy, monkeypatch):
        loaded = load_bundle(copy)
        entries = loaded.pages[2].page_image._band_loader.entries
        monkeypatch.setattr(bundle, "_MAX_ARTIFACT_BYTES", sum(entry.size for entry in entries) - 1)
        with pytest.raises(BundleCorruptError, match="page_0003_band_01.png and its page's other bands total"):
            loaded.pages[2].page_image.read_bands()

    def test_a_pickled_loaded_document_still_verifies(self, copy):
        loaded = pickle.loads(pickle.dumps(load_bundle(copy)))
        assert loaded.figures[2].read_bytes() == PNG_B
        flipped(copy / "images" / f"{FIGURE_B_SHA}.png")
        with pytest.raises(BundleCorruptError, match="its sha256"):
            loaded.figures[2].read_bytes()

    def test_the_written_documents_reads_are_verified_too(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bundle, "render_page", fake_render)
        source = source_pdf(tmp_path)
        returned = write(bound_document(source), tmp_path / "out", source)
        assert returned.figures[0].read_bytes() == PNG_A
        flipped(tmp_path / "out" / A_PNG)
        with pytest.raises(BundleCorruptError, match=f"{A_PNG}: its sha256"):
            returned.figures[0].read_bytes()
        flipped(tmp_path / "out" / "page_images" / "page_0001.png")
        with pytest.raises(BundleCorruptError, match="page_0001.png: its sha256"):
            returned.pages[0].page_image.read_bytes()
