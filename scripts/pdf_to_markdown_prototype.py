#!/usr/bin/env python3
"""Prototype: render datagrunt PDF extraction to Markdown instead of JSON.

This is an exploratory renderer, NOT a production writer. It consumes the
lossless ``to_dicts()`` output (the source of truth) and renders a Markdown
*view* of it -- demonstrating the two engines' very different fit for Markdown:

  * pymupdf - structured: classification -> headings, table elements -> real
              Markdown tables, images -> references. Near 1:1 with Markdown.
  * pdfium  - complete text, but flat: rendered from each page's full text
              (no heading/table structure without further inference).

Usage:
    uv run python scripts/pdf_to_markdown_prototype.py <file.pdf> [pymupdf|pdfium]
    uv run python scripts/pdf_to_markdown_prototype.py <file.pdf> both   # default
"""

from __future__ import annotations

import sys
from pathlib import Path

from datagrunt import PDFReader

_HEADING = {"header": "# ", "subheader": "## "}


def _md_cell(value) -> str:
    """Sanitize a table cell for Markdown (collapse newlines, escape pipes)."""
    text = "" if value is None else str(value)
    return text.replace("\n", " ").replace("|", "\\|").strip()


def _render_table(content: list, has_header: bool) -> str:
    if not content:
        return ""
    rows = [[_md_cell(c) for c in row] for row in content]
    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]
    if has_header:
        header, body = rows[0], rows[1:]
    else:
        header, body = [""] * width, rows
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(["---"] * width) + " |"]
    lines += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(lines)


def render_pymupdf(doc: dict) -> str:
    """Render the unified element schema into structured Markdown."""
    blocks: list[str] = []
    for page in doc.get("document", {}).get("pages", []):
        # Approximate reading order top-to-bottom, then left-to-right.
        elements = sorted(
            page.get("elements", []),
            key=lambda el: (el.get("position", {}).get("y", 0.0), el.get("position", {}).get("x", 0.0)),
        )
        for el in elements:
            etype = el.get("type")
            content = el.get("content")
            meta = el.get("metadata") or {}
            if etype in _HEADING:
                blocks.append(f"{_HEADING[etype]}{content}")
            elif etype == "caption":
                blocks.append(f"*{content}*")
            elif etype == "table":
                blocks.append(_render_table(content, meta.get("has_header_row", False)))
            elif etype == "image":
                path = meta.get("file_path") or ""
                blocks.append(f"![{Path(path).name or 'image'}]({path})")
            elif isinstance(content, str) and content.strip():
                blocks.append(content)
    return "\n\n".join(blocks) + "\n"


def render_pdfium(doc: dict) -> str:
    """Render the native schema from each page's complete text (flat)."""
    blocks: list[str] = []
    pages = doc.get("document", {}).get("pages", [])
    for page in pages:
        text = (page.get("text") or "").strip()
        if not text:
            continue
        # Each blank-line-separated chunk becomes a paragraph.
        for para in text.split("\n\n"):
            para = para.strip()
            if para:
                blocks.append(para)
    return "\n\n".join(blocks) + "\n"


_RENDERERS = {"pymupdf": render_pymupdf, "pdfium": render_pdfium}


def to_markdown(path: str, engine: str) -> str:
    doc = PDFReader(path, engine=engine).to_dicts()
    return _RENDERERS[engine](doc)


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    path = args[0]
    which = args[1].lower() if len(args) > 1 else "both"
    engines = ("pymupdf", "pdfium") if which == "both" else (which,)
    for engine in engines:
        md = to_markdown(path, engine)
        print(f"\n{'=' * 78}\n# Markdown via {engine}  ({Path(path).name})\n{'=' * 78}\n")
        print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
