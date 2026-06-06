#!/usr/bin/env python3
"""Compare datagrunt PDF engines (pymupdf vs pdfium) on the same documents.

For each (document, engine) it reports a summary line plus three deep-dive
sections:

  * Summary       - pages, parse seconds (incl. image extraction), text units,
                    raw vs deduped image count, table count (pymupdf only),
                    total text chars, word count, pages needing OCR.
  * Per-page text - per-page char counts for both engines and the delta, with
                    the pages where one engine extracts substantially more
                    surfaced (the "completeness" view).
  * Text overlap  - token-set Jaccard similarity between the two engines' text.

The engines emit different schemas, so metrics are computed schema-aware and
reported side by side rather than asserted equal.

Usage:
    uv run python scripts/compare_pdf_engines.py                 # default: data/pdfs/Root_Base*
    uv run python scripts/compare_pdf_engines.py path/to/a.pdf path/to/b.pdf
    uv run python scripts/compare_pdf_engines.py --fast          # skip image extraction/dedupe
"""

from __future__ import annotations

import re
import sys
import tempfile
import time
from glob import glob
from pathlib import Path

from datagrunt import PDFReader
from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.core.pdf_io.extraction import PdfiumNativeReader

ENGINES = ("pymupdf", "pdfium")
DEFAULT_GLOB = "data/pdfs/Root_Base*.pdf"
GAP_TOP_N = 6  # pages with the largest text gap to surface in the deep-dive
_TOKEN = re.compile(r"\w+")


# --------------------------------------------------------------------------- #
# Schema-aware extraction: per-page text + image-file paths, for each engine.
# --------------------------------------------------------------------------- #
def _pages_text_pymupdf(doc: dict) -> list[str]:
    out = []
    for pg in doc.get("document", {}).get("pages", []):
        parts = [el.get("content") for el in pg.get("elements", []) if isinstance(el.get("content"), str)]
        out.append(" ".join(parts))
    return out


def _pages_text_pdfium(doc: dict) -> list[str]:
    return [pg.get("text", "") or "" for pg in doc.get("document", {}).get("pages", [])]


def _structure_pymupdf(doc: dict) -> dict:
    pages = doc.get("document", {}).get("pages", [])
    text_units = images = tables = ocr_pages = 0
    for pg in pages:
        if pg.get("classification") == "scanned":
            ocr_pages += 1
        for el in pg.get("elements", []):
            t = el.get("type")
            if t == "image":
                images += 1
            elif t == "table":
                tables += 1
            else:
                text_units += 1
    return {"text_units": text_units, "images": images, "tables": tables, "ocr_pages": ocr_pages}


def _structure_pdfium(doc: dict) -> dict:
    pages = doc.get("document", {}).get("pages", [])
    return {
        "text_units": sum(len(pg.get("text_objects", [])) for pg in pages),
        "images": sum(len(pg.get("images", [])) for pg in pages),
        "tables": 0,
        "ocr_pages": sum(1 for pg in pages if pg.get("ocr")),
    }


def _unique_images_pymupdf(doc: dict) -> int:
    pdfcomponents.ParsedDocument(doc).dedupe_images()
    paths = set()
    for pg in doc.get("document", {}).get("pages", []):
        for el in pg.get("elements", []):
            if el.get("type") == "image":
                fp = (el.get("metadata") or {}).get("file_path")
                if fp:
                    paths.add(fp)
    return len(paths)


def _unique_images_pdfium(doc: dict) -> int:
    PdfiumNativeReader.dedupe_images(doc)
    paths = set()
    for pg in doc.get("document", {}).get("pages", []):
        for img in pg.get("images", []):
            if img.get("file"):
                paths.add(img["file"])
    return len(paths)


_PAGES_TEXT = {"pymupdf": _pages_text_pymupdf, "pdfium": _pages_text_pdfium}
_STRUCTURE = {"pymupdf": _structure_pymupdf, "pdfium": _structure_pdfium}
_UNIQUE_IMAGES = {"pymupdf": _unique_images_pymupdf, "pdfium": _unique_images_pdfium}


def run_engine(path: str, engine: str, image_dir: str | None) -> dict:
    """Parse one document with one engine; return metrics, per-page text, timing."""
    unified = engine == "pdfium" and "--structured" in sys.argv[1:]
    pages_text_fn = _pages_text_pymupdf if unified else _PAGES_TEXT[engine]
    structure_fn = _structure_pymupdf if unified else _STRUCTURE[engine]
    unique_fn = _unique_images_pymupdf if unified else _UNIQUE_IMAGES[engine]
    start = time.perf_counter()
    try:
        doc = PDFReader(path, engine=engine, structured=unified).to_dicts(image_output_dir=image_dir)
    except Exception as e:  # noqa: BLE001 - report, don't abort the sweep
        return {"error": f"{type(e).__name__}: {e}", "seconds": round(time.perf_counter() - start, 2)}
    elapsed = time.perf_counter() - start

    pages_text = pages_text_fn(doc)
    full_text = "\n".join(pages_text)
    metrics = structure_fn(doc)
    metrics.update(
        seconds=round(elapsed, 2),
        pages=len(pages_text),
        text_chars=sum(len(t) for t in pages_text),
        words=len(full_text.split()),
        unique_images=unique_fn(doc) if image_dir else None,
        pages_text=pages_text,
        tokens=set(_TOKEN.findall(full_text.lower())),
        error=None,
    )
    return metrics


SUMMARY_COLS = (
    "pages", "seconds", "text_units", "images", "unique_images",
    "tables", "text_chars", "words", "ocr_pages",
)


def _print_summary(results: dict) -> None:
    header = f"{'engine':<10}" + "".join(f"{c:>13}" for c in SUMMARY_COLS)
    print(header)
    print("-" * len(header))
    for engine in ENGINES:
        r = results[engine]
        if r.get("error"):
            print(f"{engine:<10}  ERROR: {r['error']}  ({r['seconds']}s)")
            continue
        print(f"{engine:<10}" + "".join(f"{('' if r.get(c) is None else r.get(c)):>13}" for c in SUMMARY_COLS))


def _print_per_page_gap(results: dict) -> None:
    mu, pdf = results["pymupdf"], results["pdfium"]
    if mu.get("error") or pdf.get("error"):
        return
    mu_t, pdf_t = mu["pages_text"], pdf["pages_text"]
    n = min(len(mu_t), len(pdf_t))
    rows = [(i + 1, len(mu_t[i]), len(pdf_t[i]), len(pdf_t[i]) - len(mu_t[i])) for i in range(n)]
    gaps = sorted(rows, key=lambda r: r[3], reverse=True)
    total_gap = sum(r[3] for r in rows if r[3] > 0)
    print(f"\nPer-page text chars (pdfium - pymupdf); total pages where pdfium > pymupdf adds +{total_gap} chars")
    print(f"  {'page':>4}{'pymupdf':>10}{'pdfium':>10}{'delta':>10}")
    for page_no, m, p, d in gaps[:GAP_TOP_N]:
        flag = "  <-- pymupdf shortfall" if d > 0 else ("  <-- pdfium shortfall" if d < 0 else "")
        print(f"  {page_no:>4}{m:>10}{p:>10}{d:>+10}{flag}")
    neg = [r for r in gaps if r[3] < 0]
    if neg:
        worst = min(neg, key=lambda r: r[3])
        print(f"  (largest pdfium shortfall: page {worst[0]} {worst[3]:+d} chars)")


def _print_overlap(results: dict) -> None:
    mu, pdf = results["pymupdf"], results["pdfium"]
    if mu.get("error") or pdf.get("error"):
        return
    a, b = mu["tokens"], pdf["tokens"]
    union = len(a | b) or 1
    jaccard = len(a & b) / union
    print(
        f"\nText overlap: Jaccard(token sets) = {jaccard:.3f}  "
        f"| pymupdf-only tokens: {len(a - b)}  pdfium-only tokens: {len(b - a)}  shared: {len(a & b)}"
    )


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    fast = "--fast" in sys.argv[1:]
    paths = args if args else sorted(glob(DEFAULT_GLOB))
    if not paths:
        print(f"No PDFs found (pattern: {DEFAULT_GLOB!r}). Pass paths explicitly.", file=sys.stderr)
        return 2

    mode = "fast (no image extraction)" if fast else "full (image extraction + dedupe)"
    print(f"Comparing engines {ENGINES} over {len(paths)} document(s) -- mode: {mode}.")
    for path in paths:
        if not Path(path).is_file():
            print(f"[skip] not a file: {path}", file=sys.stderr)
            continue
        print(f"\n{'=' * 78}\n{Path(path).name}\n{'=' * 78}")
        results = {}
        for engine in ENGINES:
            if fast:
                results[engine] = run_engine(path, engine, image_dir=None)
            else:
                with tempfile.TemporaryDirectory(prefix=f"cmp_{engine}_") as tmp:
                    results[engine] = run_engine(path, engine, image_dir=tmp)
        _print_summary(results)
        _print_per_page_gap(results)
        _print_overlap(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
