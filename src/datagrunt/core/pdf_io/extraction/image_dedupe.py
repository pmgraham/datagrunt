"""Shared byte-identical image-file de-duplication."""

import hashlib
import os


def dedupe_image_files(images, get_path, set_path) -> int:
    """Collapse byte-identical files among image records; return count removed.

    Args:
        images: Iterable of mutable image records.
        get_path: Callable ``(record) -> path | None``.
        set_path: Callable ``(record, path) -> None`` repointing a record.

    Returns:
        The number of duplicate files removed from disk.
    """
    seen = {}  # md5 digest -> first path that produced it
    removed = 0
    for record in images:
        path = get_path(record)
        if not path or not os.path.isfile(path):
            continue
        with open(path, "rb") as f:
            digest = hashlib.md5(f.read()).hexdigest()
        first = seen.get(digest)
        if first is None:
            seen[digest] = path
            continue
        if first == path:
            continue
        set_path(record, first)
        try:
            os.remove(path)
        except OSError:
            pass
        else:
            removed += 1
    return removed
