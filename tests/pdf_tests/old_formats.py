"""Outputs as datagrunt 5.0 to 5.2 wrote them, made from this datagrunt's (#470): older outputs stay loadable.

Datagrunt 5.0 to 5.2.2 wrote document JSON version 1 (no page
``image_only_reasons`` key), bundle format ``"5.0"`` (no manifest
``image_only_pages`` key), element rows without an ``image_only_reasons``
column, and page Markdown comments without an ``image_only=`` token. Each
helper rewrites this datagrunt's output into that one, encoded as the old
writer encoded it (the codec's compact ASCII JSON, the manifest's indented
ASCII JSON, the rows' compact UTF-8 lines). For one document and source,
the result is byte for byte what datagrunt 5.2.2 (commit bfa8369) wrote:
checked file by file when #470 added these helpers.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path

from datagrunt.pdf.codec import document_to_json
from datagrunt.pdf.elements import PDFDocument

MANIFEST = "manifest.json"
DOCUMENT_JSON = "json/document.json"
ELEMENTS_JSONL = "json/elements.jsonl"
# A page's Markdown comment with the image-only token #470 added; the old writer wrote it without one.
_MARKED_HEADER = re.compile(r"^(<!-- page \d+ route=\w+ status=\w+) image_only=[a-z_,]+( -->)$", re.MULTILINE)


def as_version_1(data: dict) -> dict:
    """A copy of the document JSON value ``data`` (version 2) as version 1 held it: no page ``image_only_reasons``."""
    old = copy.deepcopy(data)
    old["version"] = 1
    for page in old["pages"]:
        del page["image_only_reasons"]
    return old


def version_1_bytes(document: PDFDocument) -> bytes:
    """``document``'s JSON as datagrunt 5.2.2's ``document_to_json_bytes`` wrote it."""
    return _codec_bytes(as_version_1(document_to_json(document)))


def as_bundle_5_0(out: Path) -> None:
    """Rewrite the bundle at ``out`` (format 5.3) in place as datagrunt 5.2.2 wrote it (format 5.0).

    Its document JSON becomes version 1, its rows lose the column, its
    Markdown the token; each file changed is signed again in the manifest,
    which loses ``image_only_pages``.
    """
    manifest = json.loads((out / MANIFEST).read_bytes())
    rewritten = {
        DOCUMENT_JSON: _codec_bytes(as_version_1(json.loads((out / DOCUMENT_JSON).read_bytes()))),
        ELEMENTS_JSONL: b"".join(_old_row(line) for line in (out / ELEMENTS_JSONL).read_bytes().splitlines()),
    }
    for entry in manifest["artifacts"]:
        if entry["path"].endswith(".md"):
            text = (out / entry["path"]).read_text(encoding="utf-8")
            rewritten[entry["path"]] = _MARKED_HEADER.sub(r"\1\2", text).encode("utf-8")
    for entry in manifest["artifacts"]:
        data = rewritten.get(entry["path"])
        if data is not None:
            (out / entry["path"]).write_bytes(data)
            entry["size"], entry["sha256"] = len(data), hashlib.sha256(data).hexdigest()
    manifest["bundle_format_version"] = "5.0"
    del manifest["image_only_pages"]
    (out / MANIFEST).write_bytes(json.dumps(manifest, ensure_ascii=True, allow_nan=False, indent=2).encode("ascii"))


def _codec_bytes(data: dict) -> bytes:
    """A document JSON value encoded as the codec encodes it."""
    return json.dumps(data, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")


def _old_row(line: bytes) -> bytes:
    """An element row without its ``image_only_reasons``, encoded as the bundle encodes a row."""
    row = json.loads(line)
    del row["image_only_reasons"]
    return json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8") + b"\n"
