"""Shared byte-identical image-file de-duplication."""

import hashlib
import os
import stat
from typing import Callable, Iterable, Optional


def _is_deletion_safe(path: str, allowed_root: Optional[str]) -> bool:
    """Return True only when ``path`` is a regular file this run may delete.

    Deletion is a destructive primitive, and image ``file_path`` values come
    from the (untrusted) parsed-document JSON, so a path is only safe when:

    * it is not a symlink (we refuse to follow links into other directories),
    * its resolved real path sits inside ``allowed_root`` (the caller-provided
      image output directory), and
    * an ``allowed_root`` was actually supplied (no root means delete nothing).

    See issue #101: without these guards, attacker-controlled paths echoed from
    the input document could be deleted (arbitrary file deletion).
    """
    if not allowed_root:
        return False
    try:
        if stat.S_ISLNK(os.lstat(path).st_mode):
            return False
    except OSError:
        return False
    real_path = os.path.realpath(path)
    real_root = os.path.realpath(allowed_root)
    # ``commonpath`` raises on differing drives/relative-vs-absolute mixes; treat
    # any such case as "not contained" rather than letting it escape.
    try:
        return os.path.commonpath([real_path, real_root]) == real_root
    except ValueError:
        return False


def dedupe_image_files(
    images: Iterable[dict],
    get_path: Callable[[dict], Optional[str]],
    set_path: Callable[[dict, str], None],
    allowed_dir: Optional[str] = None,
) -> int:
    """Collapse byte-identical files among image records; return count removed.

    Args:
        images: Iterable of mutable image records.
        get_path: Callable ``(record) -> path | None``.
        set_path: Callable ``(record, path) -> None`` repointing a record.
        allowed_dir: The image output directory produced by this extraction run.
            Only files resolving inside this directory are eligible for removal;
            symlinks are never followed. When ``None`` (no run-owned directory),
            no file is deleted, though records are still repointed at the first
            occurrence so duplicate references collapse.

    Returns:
        The number of duplicate files removed from disk.
    """
    seen: dict = {}  # md5 digest -> first path that produced it
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
        if not _is_deletion_safe(path, allowed_dir):
            continue
        try:
            os.remove(path)
        except OSError:
            pass
        else:
            removed += 1
    return removed
