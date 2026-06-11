"""Tests for the shared byte-identical image-file de-duplication helper.

Includes a security regression test for issue #101: image ``file_path`` values
are attacker-controlled file *content* (parsed-document JSON is a documented
input), so dedupe must never ``os.remove`` a path outside the caller-provided
output directory, nor follow a symlink.
"""

import os

from datagrunt.core.pdf_io.extraction.image_dedupe import dedupe_image_files


def _records(*paths):
    """Build mutable image records keyed on ``file_path``."""
    return [{"file_path": p} for p in paths]


def _get(record):
    return record.get("file_path")


def _set(record, path):
    record["file_path"] = path


def test_in_run_duplicates_inside_output_dir_are_collapsed(tmp_path):
    output_dir = tmp_path / "images"
    output_dir.mkdir()
    first = output_dir / "img_0.png"
    second = output_dir / "img_1.png"
    payload = b"identical-bytes"
    first.write_bytes(payload)
    second.write_bytes(payload)

    records = _records(str(first), str(second))
    removed = dedupe_image_files(records, _get, _set, allowed_dir=str(output_dir))

    assert removed == 1
    assert second.exists() is False
    assert first.exists() is True
    # The duplicate record is repointed at the surviving file.
    assert records[1]["file_path"] == str(first)


def test_paths_outside_output_dir_are_not_deleted(tmp_path):
    """Issue #101: attacker-named paths outside the run's dir must survive."""
    output_dir = tmp_path / "images"
    output_dir.mkdir()

    outside_first = tmp_path / "victim_a"
    outside_second = tmp_path / "victim_b"
    # Two byte-identical (empty) files mimic the empty-file hash collision repro.
    outside_first.write_bytes(b"")
    outside_second.write_bytes(b"")

    records = _records(str(outside_first), str(outside_second))
    removed = dedupe_image_files(records, _get, _set, allowed_dir=str(output_dir))

    assert removed == 0
    assert outside_first.exists() is True
    assert outside_second.exists() is True


def test_symlink_inside_output_dir_is_not_followed(tmp_path):
    """A symlink whose real target sits outside the dir must not be deleted."""
    output_dir = tmp_path / "images"
    output_dir.mkdir()

    real_image = output_dir / "img_0.png"
    real_image.write_bytes(b"")

    victim = tmp_path / "important.lock"
    victim.write_bytes(b"")

    # Symlink lives inside the output dir but points at the outside victim.
    link = output_dir / "img_1.png"
    link.symlink_to(victim)

    records = _records(str(real_image), str(link))
    removed = dedupe_image_files(records, _get, _set, allowed_dir=str(output_dir))

    assert removed == 0
    assert victim.exists() is True
    assert os.path.islink(link) is True
