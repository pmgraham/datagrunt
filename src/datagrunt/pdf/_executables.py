"""Programs datagrunt runs, found only in absolute ``PATH`` directories.

A program is run by the absolute path this finds, never by its bare name:
starting a bare name also searches the working directory on Windows (before
``PATH``), and on any platform a relative ``PATH`` entry (``""``, ``"."``,
``"bin"``) resolves against it. Both are skipped here, so the program run is
always one in a directory that ``PATH`` names absolutely.
"""

import os
import shutil

__all__ = ["trusted_executable"]


def trusted_executable(name: str) -> str | None:
    """The absolute path of program ``name`` in an absolute ``PATH`` directory, or None if there is none.

    Only ``PATH``'s absolute entries are searched, in order. A match outside
    them (the working directory Windows' search adds) is refused.

    Raises:
        TypeError: If ``name`` is not a str.
        ValueError: If ``name`` is empty or holds a path separator.
    """
    if not isinstance(name, str):
        raise TypeError(f"name must be a str, got {type(name).__name__}")
    separators = [separator for separator in (os.sep, os.altsep) if separator]
    if not name or any(separator in name for separator in separators):
        raise ValueError(f"name must be a bare program name, got {name!r}")
    directories = [entry for entry in os.environ.get("PATH", "").split(os.pathsep) if os.path.isabs(entry)]
    found = shutil.which(name, path=os.pathsep.join(directories)) if directories else None
    if found is None:
        return None
    allowed = {os.path.normcase(os.path.realpath(directory)) for directory in directories}
    where = os.path.normcase(os.path.realpath(os.path.dirname(os.path.abspath(found))))
    return os.path.abspath(found) if where in allowed else None
