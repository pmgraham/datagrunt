"""The v5 PDFReader writes nothing to the filesystem, in any public method (#372, A8b: the issue's done-when).

Each check runs in a fresh subprocess (:data:`GUARD`), so its audit hook
cannot leak into the test session:

- In that process, an audit hook (``sys.addaudithook``, installed after
  the imports and before the first reader is built) records every
  filesystem write Python makes: an ``open`` with a write mode or write
  flags (``os.open`` included; an existing descriptor, such as a pipe, is
  not a path), ``os.mkdir``, ``os.rename`` (``os.replace`` too),
  ``os.remove``, ``os.rmdir``, the other ``os`` calls that change a path,
  ``tempfile``'s and every ``shutil.*`` event. It records, then refuses.
- Writes no hook can see (DuckDB's and pdfium's C code, and every child
  process: page workers, render workers, tesseract) are caught where they
  would land: the process runs with ``HOME``, ``TMPDIR`` (``TMP`` and
  ``TEMP`` too) and its working directory each a fresh empty directory,
  and each must be empty afterwards, its mtime unchanged (a file made and
  removed again moves it). The fixtures' directory must be unchanged too.

The process runs with ``-B``: Python's own bytecode cache is the
interpreter's, not the reader's.

Each reader is driven through every public method and property, the A1
loaders its figures and page images read their bytes through, and its
tables' and pages' own views. Covered: a digital PDF; a scanned page,
read by tesseract (the vision route, its page image rendered); a bundle;
a dict of the document JSON; and a PDF of both pages with ``workers=2``.
Two controls prove the guard sees a write: one in the process, one in a
child.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from datagrunt.pdf.bundle import write_bundle
from datagrunt.pdf.codec import document_to_json
from datagrunt.pdf.pipeline import extract_document
from pdf_tests.builders import PageSpec, RasterImage, build_pdf
from pdf_tests.test_ocr import needs_tesseract
from pdf_tests.test_ocr_scans import LETTER, LINES, png_of, rendered
from pdf_tests.test_pipeline import mixed_page

GUARD_TIMEOUT_S = 900.0
# A figure at 32 px (mixed_page's image is 64 x 32 px): every element kind the page holds is read.
DIGITAL_OPTIONS = {"min_image_dimension": 32}

# The guarded process: argv[1] is a JSON list of cases, each {"name", "source", "json", "options", "control"}.
GUARD = r"""
import gc, json, os, sys

WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND | getattr(os, "O_EXCL", 0)
PATH_EVENTS = {
    "os.mkdir", "os.rename", "os.remove", "os.rmdir", "os.truncate", "os.link", "os.symlink", "os.mkfifo",
    "os.mknod", "os.chmod", "os.chown", "os.utime", "os.chflags", "os.lchflags", "os.setxattr", "os.removexattr",
    "tempfile.mkstemp", "tempfile.mkdtemp",
}
violations = []


class WriteRefused(Exception):
    pass


def writes(mode, flags):
    if isinstance(mode, str) and set(mode) & set("wax+"):
        return True
    return isinstance(flags, int) and flags & WRITE_FLAGS != 0


def hook(event, args):
    if event == "open":
        path, mode, flags = args
        if isinstance(path, int) or not writes(mode, flags):
            return
    elif event not in PATH_EVENTS and not event.startswith("shutil."):
        return
    violations.append(f"{event} {args!r}")
    raise WriteRefused(f"{event} {args!r}")


import subprocess, tempfile
import polars, pyarrow, duckdb, PIL.Image, PIL.PngImagePlugin
from datagrunt.pdf.reader import PDFReader
from datagrunt.pdf.exceptions import DatagruntPDFError

COLLECTIONS = ("pages", "headings", "paragraphs", "key_values", "tables", "lists", "figures", "page_images",
               "sections", "header_context", "metadata", "document")


def loaded(read):
    try:
        data = read()
    except (DatagruntPDFError, OSError) as exc:
        return f"{type(exc).__name__}: {exc}"
    return sum(len(part) for part in data) if isinstance(data, tuple) else len(data)


def exercise(reader):
    for name in COLLECTIONS:
        getattr(reader, name)
    frame = reader.to_dataframe()
    reader.to_arrow_table(), reader.to_dicts(), reader.get_sample(), reader.get_sample(3), reader.to_markdown()
    queried = reader.query_data(f"SELECT count(*) AS n FROM {reader.db_table}").dicts()[0]["n"]
    whole = reader.query_data(f"SELECT * FROM {reader.db_table} ORDER BY page_number, reading_order")
    whole.pl(), whole.pa(), whole.dicts()
    figures = [loaded(figure.read_bytes) for figure in reader.figures]
    raw = [loaded(figure.read_raw_bytes) for figure in reader.figures]
    images = [(loaded(image.read_bytes), loaded(image.read_bands)) for image in reader.page_images]
    for table in reader.tables:
        table.to_dataframe(), table.to_arrow_table(), table.to_dicts(), table.to_markdown()
    for item in (*reader.pages, *reader.sections, *reader.document.elements):
        item.to_markdown()
    pages = reader.pages
    reader.close()
    reader.close()
    return {
        "elements": len(reader.document.elements),
        "frame_rows": frame.height,
        "query_rows": queried,
        "routes": [page.route for page in pages],
        "statuses": [page.status for page in pages],
        "ocr_confidence": [page.quality.ocr_mean_confidence if page.quality else None for page in pages],
        "figures": figures,
        "raw_figures": raw,
        "page_images": images,
    }


def run(case):
    if case["control"] == "in-process":
        tempfile.TemporaryFile()
    if case["control"] == "child":
        subprocess.run([sys.executable, "-c", "import os; open(os.environ['HOME'] + '/x', 'w').close()"], check=True)
    if case["json"]:
        with open(case["source"], "rb") as stream:
            source = json.load(stream)
    else:
        source = case["source"]
    summary = exercise(PDFReader(source, **case["options"]))
    with PDFReader(source, **case["options"]) as again:
        again.query_data(f"SELECT kind, count(*) AS n FROM {again.db_table} GROUP BY kind").pl()
    return summary


sys.addaudithook(hook)
results = {}
for case in json.loads(sys.argv[1]):
    try:
        results[case["name"]] = run(case)
    except WriteRefused as exc:
        results[case["name"]] = {"refused": str(exc)}
gc.collect()
print("GUARD " + json.dumps({"violations": violations, "results": results}))
"""


def snapshot(directory: Path) -> tuple[int, tuple[tuple[str, int, int], ...]]:
    """``directory``'s mtime, and each file under it with its size and mtime."""
    entries = []
    for path in sorted(directory.rglob("*")):
        status = path.lstat()
        entries.append((str(path.relative_to(directory)), status.st_size, status.st_mtime_ns))
    return directory.stat().st_mtime_ns, tuple(entries)


def guarded(cases: list[dict], tmp_path: Path, fixtures: Path) -> SimpleNamespace:
    """Run :data:`GUARD` over ``cases`` with fresh empty HOME, TMPDIR and working directory; what it saw.

    Returns the hook's violations, each case's summary, and each fresh
    directory's and the fixtures' state before and after.
    """
    places = {name: tmp_path / name for name in ("home", "tmp", "cwd")}
    for place in places.values():
        place.mkdir()
    environment = {**os.environ, "HOME": str(places["home"]), **{key: str(places["tmp"]) for key in TMP_KEYS}}
    before = {name: snapshot(place) for name, place in {**places, "fixtures": fixtures}.items()}
    done = subprocess.run(
        [sys.executable, "-I", "-B", "-c", GUARD, json.dumps(cases)],
        cwd=places["cwd"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=GUARD_TIMEOUT_S,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-4000:]
    (line,) = [line for line in done.stdout.splitlines() if line.startswith("GUARD ")]
    seen = json.loads(line.removeprefix("GUARD "))
    after = {name: snapshot(place) for name, place in {**places, "fixtures": fixtures}.items()}
    return SimpleNamespace(violations=seen["violations"], results=seen["results"], before=before, after=after)


TMP_KEYS = ("TMPDIR", "TMP", "TEMP")


def case(name: str, source: Path, *, json_source: bool = False, control: str = "", **options) -> dict:
    return {"name": name, "source": str(source), "json": json_source, "options": options, "control": control}


@pytest.fixture(scope="module")
def fixtures(tmp_path_factory) -> SimpleNamespace:
    """A digital PDF, a scanned page, a PDF of both, the digital PDF's bundle and its document JSON."""
    directory = tmp_path_factory.mktemp("guarded")
    digital = build_pdf(directory / "digital.pdf", [mixed_page(), mixed_page(top=100.0)])
    text = build_pdf(tmp_path_factory.mktemp("text") / "text.pdf", [PageSpec(items=LINES)])
    scan = PageSpec(items=(RasterImage(0.0, 0.0, *LETTER, png_of(rendered(text))),))
    scanned = build_pdf(directory / "scanned.pdf", [scan])
    both = build_pdf(directory / "both.pdf", [mixed_page(), scan])
    document = extract_document(both, **DIGITAL_OPTIONS)
    bundle = directory / "bundle"
    write_bundle(document, bundle, source=both)
    data = directory / "document.json"
    data.write_text(json.dumps(document_to_json(extract_document(digital, **DIGITAL_OPTIONS))), encoding="ascii")
    return SimpleNamespace(directory=directory, digital=digital, scanned=scanned, both=both, bundle=bundle, data=data)


@pytest.fixture(scope="module")
def seen(fixtures, tmp_path_factory) -> SimpleNamespace:
    """The guard over every source, once for the module."""
    cases = [
        case("digital", fixtures.digital, **DIGITAL_OPTIONS),
        case("vision", fixtures.scanned),
        case("bundle", fixtures.bundle),
        case("manifest", fixtures.bundle / "manifest.json"),
        case("dict", fixtures.data, json_source=True),
        case("workers", fixtures.both, workers=2, **DIGITAL_OPTIONS),
    ]
    return guarded(cases, tmp_path_factory.mktemp("run"), fixtures.directory)


@needs_tesseract
class TestTheReaderWritesNothing:
    """Every public method, on every source: no write in the process, none where a child's would land."""

    def test_no_write_is_seen_in_the_process(self, seen):
        assert seen.violations == []

    @pytest.mark.parametrize("place", ["home", "tmp", "cwd", "fixtures"])
    def test_no_write_lands_where_a_child_or_a_library_would_write(self, seen, place):
        assert seen.after[place] == seen.before[place]

    def test_every_source_was_read_whole(self, seen):
        for name, result in seen.results.items():
            assert result["elements"] > 0, name
            assert result["frame_rows"] == result["query_rows"] == result["elements"], name

    def test_the_scanned_page_was_read_by_ocr_and_its_image_rendered(self, seen):
        vision = seen.results["vision"]

        assert (vision["routes"], vision["statuses"]) == (["vision"], ["ok"])
        assert vision["ocr_confidence"][0] is not None
        assert all(isinstance(size, int) and size > 0 for pair in vision["page_images"] for size in pair)

    def test_the_pool_read_both_pages_one_by_ocr(self, seen):
        pooled = seen.results["workers"]

        assert pooled["routes"] == ["digital", "vision"]
        assert pooled["ocr_confidence"][1] is not None

    @pytest.mark.parametrize("name", ["digital", "workers", "bundle"])
    def test_figures_read_their_bytes_through_their_loaders(self, seen, name):
        figures = seen.results[name]["figures"]

        assert figures and all(isinstance(size, int) and size > 0 for size in figures)

    def test_a_bundles_page_images_read_their_files_and_a_vision_pages_its_bands(self, seen):
        (rendered_page, rendered_bands), (vision_page, vision_bands) = seen.results["bundle"]["page_images"]

        assert all(isinstance(size, int) and size > 0 for size in (rendered_page, vision_page, vision_bands))
        assert rendered_bands == "DatagruntPDFError: page image 1 has no bands: no band_paths and no band loader"

    def test_a_document_json_has_no_bytes_to_read_and_says_so(self, seen):
        figures = seen.results["dict"]["figures"]

        assert figures and all(why.startswith("DatagruntPDFError: figure p") for why in figures)


class TestTheGuardSeesWrites:
    """The guard is not blind: a write in the process is refused and recorded, and a child's lands where it looks."""

    def test_a_temporary_file_in_the_process_is_refused(self, fixtures, tmp_path):
        seen = guarded([case("control", fixtures.digital, control="in-process")], tmp_path, fixtures.directory)

        assert seen.results["control"]["refused"].startswith("open (")
        assert len(seen.violations) == 1

    def test_a_childs_write_lands_where_the_guard_looks(self, fixtures, tmp_path):
        seen = guarded([case("control", fixtures.digital, control="child")], tmp_path, fixtures.directory)

        assert seen.violations == []
        assert seen.after["home"] != seen.before["home"]
