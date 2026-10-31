"""Regression for #389: v4 element fonts must not depend on the string-hash seed.

Each line of the fixture is two runs in different fonts, so every element's
dominant font is a tie. Before the fix the tie went to whichever font set
iteration met first, which follows ``PYTHONHASHSEED``; the same file then gave
different ``metadata.font`` values in different processes. Extraction runs in
fresh subprocesses (the seed is fixed at interpreter start) and the JSON must be
byte-identical across seeds on both engines.
"""

import json
import os
import subprocess
import sys

import pytest

_SEEDS = ("1", "2", "3", "4", "5")
_ENGINES = ("pymupdf", "pdfium")
_CHILD_TIMEOUT_SECONDS = 120
# One line per pair: "Alpha" in the first font, "Beta" in the second.
_FONT_PAIRS = (
    ("helv", "tiro"),
    ("tiro", "cour"),
    ("cour", "helv"),
    ("tiro", "helv"),
    ("cour", "tiro"),
    ("helv", "cour"),
)

# ``processing_id`` is a wall-clock timestamp by design, so it is dropped before
# comparing; everything else in the document must match byte for byte.
_CHILD_SCRIPT = """
import json, sys
from datagrunt.pdf_api.pdfreader import PDFReader

path, engines = sys.argv[1], sys.argv[2:]
result = {}
for engine in engines:
    out = PDFReader(path, engine=engine).to_dicts()
    out["document"].pop("processing_id")
    result[engine] = out
print(json.dumps(result))
"""


@pytest.fixture
def tied_font_pdf(tmp_path):
    """One page of six well-separated lines, each two runs in different base-14 fonts."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    for row, (first, second) in enumerate(_FONT_PAIRS):
        y = 72 + 60 * row
        page.insert_text((72, y), "Alpha", fontname=first, fontsize=11)
        page.insert_text((110, y), "Beta", fontname=second, fontsize=11)
    path = tmp_path / "tied_fonts.pdf"
    doc.save(str(path))
    doc.close()
    return str(path)


def _assert_every_pymupdf_block_ties(pdf_path):
    """Guard: each pymupdf block has two fonts with one non-blank span each."""
    import pymupdf

    with pymupdf.open(pdf_path) as doc:
        blocks = [b for b in doc[0].get_text("dict", sort=True)["blocks"] if b.get("type") == 0]
    assert len(blocks) == len(_FONT_PAIRS)
    for block in blocks:
        fonts = [s["font"] for ln in block["lines"] for s in ln["spans"] if s["text"].strip()]
        assert len(fonts) == 2 and fonts[0] != fonts[1], fonts


def _assert_every_pdfium_line_ties(pdf_path):
    """Guard: each pdfium line is two text objects with different family names.

    Text objects come back in content-stream order, so items pair up per line;
    the two runs' tops differ by a font's ascent, well inside the builder's
    same-line tolerance (``max(2.0, size * 0.5)``).
    """
    from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument

    with PdfiumDocument(pdf_path) as doc:
        items = list(doc.page(0).text_items())
    assert len(items) == 2 * len(_FONT_PAIRS)
    for first, second in zip(items[0::2], items[1::2]):
        assert (first.text, second.text) == ("Alpha", "Beta")
        assert abs(first.y_top - second.y_top) < 2.0
        assert first.font != second.font, (first.font, second.font)


def _extract_under_seed(pdf_path, seed):
    """Run both engines in a fresh interpreter under ``PYTHONHASHSEED=seed``."""
    env = {**os.environ, "PYTHONHASHSEED": seed}
    result = subprocess.run(
        [sys.executable, "-c", _CHILD_SCRIPT, pdf_path, *_ENGINES],
        env=env,
        capture_output=True,
        text=True,
        timeout=_CHILD_TIMEOUT_SECONDS,
        check=False,
    )
    assert result.returncode == 0, f"seed {seed} child failed:\n{result.stderr}"
    return result.stdout


def _fonts_by_engine(output):
    """``{engine: [metadata.font, ...]}`` for an assertion message."""
    parsed = json.loads(output)
    return {
        engine: [el["metadata"]["font"] for page in doc["document"]["pages"] for el in page["elements"]]
        for engine, doc in parsed.items()
    }


def test_tied_dominant_font_is_identical_across_hash_seeds(tied_font_pdf):
    _assert_every_pymupdf_block_ties(tied_font_pdf)
    _assert_every_pdfium_line_ties(tied_font_pdf)

    outputs = {seed: _extract_under_seed(tied_font_pdf, seed) for seed in _SEEDS}

    baseline = outputs[_SEEDS[0]]
    differing = [seed for seed, out in outputs.items() if out != baseline]
    fonts = {seed: _fonts_by_engine(out) for seed, out in outputs.items()}
    assert not differing, f"output differs from seed {_SEEDS[0]} under seeds {differing}; fonts per seed: {fonts}"
