"""pdfium runs only in worker processes whose memory is capped; the calling process never loads it.

The calling process starts workers and reads their replies: it hashes the
file, builds the document and writes outputs. Every pdfium call on the PDF
(its open, page count and metadata, each page's extraction, figure reads,
renders) is a worker's, under the memory cap the render worker set first.

Fixtures are hand-written: a page whose inline image inflates to
``INFLATED`` bytes when pdfium loads the page, and a document whose catalog
sits in an object stream padded to ``INFLATED`` bytes, which pdfium inflates
when it opens the document. Each is read under a cap of ``SMALL_CAP``, with ``ocr="never"``: a page
read by OCR holds its render's pixels (~37 MiB at 300 DPI), a text page a
few MiB.
"""

import dataclasses
import hashlib
import importlib
import io
import json
import struct
import subprocess
import sys
import zlib
from pathlib import Path

import pytest

from datagrunt.pdf import DatagruntPDFError, PDFCorruptError, _process, pipeline
from datagrunt.pdf.elements import Page
from datagrunt.pdf.frame import element_frame
from datagrunt.pdf.pdfium_engine import ImageLoader, open_document, page_images
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.render import render_page
from pdf_tests.builders import PageSpec, RasterImage, build_pdf, hand_written_pdf, solid_png
from pdf_tests.test_layout_stream_tables import body_text
from pdf_tests.test_pipeline import DISTRIBUTED_KEYS, chain_document, mixed_page

INFLATED = 128 << 20  # what the inline image and the object stream inflate to: ~2 x this at pdfium's peak
SMALL_CAP = 32 << 20  # a worker's memory over its start: a text page needs a few MiB
CAP_WORDS = f"reached its {SMALL_CAP}-byte memory cap"

# The modules only a worker imports: each imports pypdfium2, directly or through another.
WORKER_ONLY = (
    "datagrunt.pdf._figure_worker",
    "datagrunt.pdf._page_worker",
    "datagrunt.pdf._pdfium_common",
    "datagrunt.pdf._pdfium_images",
    "datagrunt.pdf._pdfium_recover",
    "datagrunt.pdf._pdfium_text",
    "datagrunt.pdf._render_worker",
    "datagrunt.pdf.assembly",
    "datagrunt.pdf.layout",
    "datagrunt.pdf.pdfium_engine",
    "datagrunt.pdf.quality",
    "datagrunt.pdf.vision",
)
FROZEN = "this application is frozen (sys.frozen): datagrunt reads PDFs in worker processes and cannot start one here"
NO_EXECUTABLE = "sys.executable is empty: datagrunt reads PDFs in worker processes and cannot start one here"


def deflated(size: int) -> bytes:
    """``size`` zero bytes, Flate-compressed in 1 MiB blocks."""
    packer = zlib.compressobj(9)
    block = bytes(1 << 20)
    parts = [packer.compress(block[: min(len(block), size - start)]) for start in range(0, size, len(block))]
    return b"".join(parts) + packer.flush()


def text_content(words: str) -> bytes:
    return b"BT /F1 12 Tf 72 720 Td (%s) Tj ET" % words.encode("ascii")


def inline_content(words: str, inflated: int = INFLATED) -> bytes:
    """``words``, then a 64 x 64 inline image whose Flate data inflates to ``inflated`` bytes at page load."""
    image = b" BI /W 64 /H 64 /CS /G /BPC 8 /F /Fl ID " + deflated(inflated) + b" EI"
    return text_content(words) + image


def pages_pdf(path: Path, contents: list[bytes]) -> Path:
    """A hand-written PDF of letter pages, each drawing its content stream (Flate-compressed) in Helvetica."""
    count = len(contents)
    kids = b" ".join(b"%d 0 R" % (4 + 2 * index) for index in range(count))
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [%s] /Count %d >>" % (kids, count),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    for index, content in enumerate(contents):
        packed = zlib.compress(content)
        objects.append(
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents %d 0 R "
            b"/Resources << /Font << /F1 3 0 R >> >> >>" % (5 + 2 * index)
        )
        objects.append(b"<< /Filter /FlateDecode /Length %d >>\nstream\n%s\nendstream" % (len(packed), packed))
    return hand_written_pdf(path, objects)


def object_stream_pdf(path: Path, padding: int) -> Path:
    """A one-page PDF 1.5 whose catalog, page tree and page sit in an object stream, ``padding`` spaces after them."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>",
    ]
    offsets, body = [], b""
    for item in objects:
        offsets.append(len(body))
        body += item + b"\n"
    header = b" ".join(b"%d %d" % (number, offset) for number, offset in zip((1, 2, 3), offsets)) + b"\n"
    packer = zlib.compressobj(9)
    block = b" " * (1 << 20)
    parts = [packer.compress(header + body)]
    parts += [packer.compress(block[: min(len(block), padding - start)]) for start in range(0, padding, len(block))]
    stream = b"".join(parts) + packer.flush()
    data = bytearray(b"%PDF-1.5\n")
    stream_at = len(data)
    data += b"4 0 obj\n<< /Type /ObjStm /N 3 /First %d /Filter /FlateDecode /Length %d >>\nstream\n" % (
        len(header),
        len(stream),
    )
    data += stream + b"\nendstream\nendobj\n"
    xref_at = len(data)
    rows = [struct.pack(">BIH", 0, 0, 65535)] + [struct.pack(">BIH", 2, 4, index) for index in range(3)]
    rows += [struct.pack(">BIH", 1, stream_at, 0), struct.pack(">BIH", 1, xref_at, 0)]
    xref = b"".join(rows)
    data += b"5 0 obj\n<< /Type /XRef /Size 6 /W [1 4 2] /Root 1 0 R /Length %d >>\nstream\n" % len(xref)
    data += xref + b"\nendstream\nendobj\nstartxref\n%d\n%%%%EOF\n" % xref_at
    path.write_bytes(bytes(data))
    return path


def small_cap(monkeypatch) -> None:
    """Page workers capped at SMALL_CAP: the caller's vision options (the cap it sends), the rest the defaults'."""
    real = pipeline._vision
    monkeypatch.setattr(
        pipeline, "_vision", lambda options: dataclasses.replace(real(options), memory_cap_bytes=SMALL_CAP)
    )


def frame_sha(document) -> str:
    """The frame's schema and values, hashed: not its IPC bytes, whose buffer layout can differ for equal frames."""
    frame = element_frame(document)
    values = json.dumps(frame.to_dicts(), sort_keys=True, default=str)
    return hashlib.sha256((str(frame.schema) + values).encode()).hexdigest()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def text_of(page: Page) -> str:
    return " ".join(getattr(element, "text", "") for element in page.elements)


def figures_pdf(path: Path, count: int) -> Path:
    """A digital page of prose and ``count`` distinct 64 x 32 images below it (figures at min_image_dimension 32)."""
    images = tuple(
        RasterImage(72.0, 200.0 + 48.0 * index, 64.0, 32.0, solid_png(64, 32, (0.1 * index, 0.4, 0.6)))
        for index in range(count)
    )
    return build_pdf(path, [PageSpec(items=(*body_text(72.0), *images))])


# A calling process's first lines: pypdfium2 cannot be imported in it.
BLOCK_PYPDFIUM2 = """
import hashlib, importlib.abc, io, json, sys
from pathlib import Path


class Blocked(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "pypdfium2" or name.startswith("pypdfium2."):
            raise ImportError(f"{name} is not importable in this process")
        return None


sys.meta_path.insert(0, Blocked())
"""

# The calling process, pypdfium2 blocked: it reads, writes every PDFWriter output and reads a figure's bytes.
BLOCKED_CALLER = (
    BLOCK_PYPDFIUM2
    + """
from datagrunt.pdf.exceptions import PDFEncryptedError
from datagrunt.pdf.reader import PDFReader
from datagrunt.pdf.writer import PDFWriter

source, out, encrypted, worker_only = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], json.loads(sys.argv[4])
sha = lambda data: hashlib.sha256(data).hexdigest()
result = {}
for workers in (1, 2):
    reader = PDFReader(source, workers=workers, min_image_dimension=32)
    frame = reader.to_dataframe()  # hashed by schema and values, as frame_sha hashes it, never by IPC bytes
    values = json.dumps(frame.to_dicts(), sort_keys=True, default=str)
    result[f"frame_{workers}"] = sha((str(frame.schema) + values).encode())
figure = reader.figures[0]
result["figure"] = [figure.sha256, sha(figure.read_bytes()), sha(figure.read_raw_bytes())]
writer = PDFWriter(source, min_image_dimension=32)
files = [
    writer.write_json(out / "doc.json"),
    writer.write_jsonl(out / "doc.jsonl"),
    writer.write_markdown(out / "doc.md"),
    writer.write_parquet(out / "doc.parquet"),
    writer.write_csv(out / "doc.csv"),
    *writer.extract_images(out / "images"),
    *writer.render_pages_as_images(out / "renders"),
]
result["files"] = sorted(path.relative_to(out).as_posix() for path in files if path.stat().st_size > 0)
result["images"] = sorted(sha(path.read_bytes()) for path in (out / "images").iterdir())
bundled = writer.write_bundle(out / "bundle")
result["bundle"] = sorted(path.relative_to(out / "bundle").as_posix() for path in (out / "bundle").rglob("*.png"))
try:
    PDFReader(encrypted).pages
except PDFEncryptedError as exc:
    result["encrypted"] = type(exc).__name__
result["loaded"] = sorted(name for name in sys.modules if name.split(".")[0] == "pypdfium2" or name in worker_only)
print(json.dumps(result))
"""
)

# The calling process, pypdfium2 blocked: it reads a digital page's image, drawn in the render worker (P2-A).
BLOCKED_PAGE_IMAGE = (
    BLOCK_PYPDFIUM2
    + """
from PIL import Image

from datagrunt.pdf.reader import PDFReader

source, worker_only = Path(sys.argv[1]), json.loads(sys.argv[2])
image = PDFReader(source).pages[0].page_image
data = image.read_bytes()
with Image.open(io.BytesIO(data)) as picture:
    drawn = [picture.format, picture.mode, list(picture.size), hashlib.sha256(picture.tobytes()).hexdigest()]
loaded = sorted(name for name in sys.modules if name.split(".")[0] == "pypdfium2" or name in worker_only)
print(json.dumps({"image": [image.dpi, image.width_px, image.height_px], "drawn": drawn, "loaded": loaded}))
"""
)


class TestTheCallerNeverLoadsPdfium:
    """A caller that cannot import pypdfium2 still reads, writes and renders: every pdfium call is a worker's."""

    def test_a_caller_without_pypdfium2_reads_writes_and_renders_through_workers(self, tmp_path):
        source = build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()])
        encrypted = build_pdf(tmp_path / "locked.pdf", [mixed_page()], user_password="secret")
        out = tmp_path / "out"

        done = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                BLOCKED_CALLER,
                str(source),
                str(out),
                str(encrypted),
                json.dumps(WORKER_ONLY),
            ],
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

        assert done.returncode == 0, done.stderr
        result = json.loads(done.stdout)
        expected = frame_sha(chain_document(source, min_image_dimension=32))
        assert (result["frame_1"], result["frame_2"]) == (expected, expected)
        (image,) = [item for item in page_images(open_document(source), 0).images if item.raw_sha256]
        engine = ImageLoader(source=source, page_index=0, image=image)
        assert result["figure"] == [image.raw_sha256, sha(engine.png_bytes()), sha(engine.raw_bytes())]
        assert result["files"] == [
            "doc.csv",
            "doc.json",
            "doc.jsonl",
            "doc.md",
            "doc.parquet",
            f"images/{image.raw_sha256}.png",
            "renders/page_0001.png",
            "renders/page_0002.png",
        ]
        assert result["images"] == [sha(engine.png_bytes())]
        assert result["bundle"] == [
            f"images/{image.raw_sha256}.png",
            "page_images/page_0001.png",
            "page_images/page_0002.png",
        ]
        assert result["encrypted"] == "PDFEncryptedError"
        assert result["loaded"] == []

    def test_a_caller_without_pypdfium2_reads_a_digital_pages_image_through_the_render_worker(self, tmp_path):
        source = build_pdf(tmp_path / "doc.pdf", [mixed_page()])

        done = subprocess.run(
            [sys.executable, "-I", "-c", BLOCKED_PAGE_IMAGE, str(source), json.dumps(WORKER_ONLY)],
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

        assert done.returncode == 0, done.stderr
        result = json.loads(done.stdout)
        drawn = render_page(source, 0, dpi=200, memory_cap_bytes=2 << 30, timeout_s=120.0)
        assert result["image"] == [200.0, 1700, 2200]
        assert result["drawn"] == ["PNG", "L", [1700, 2200], sha(drawn.pixels)]
        assert result["loaded"] == []


# The CSV, Excel and Parquet stacks' packages, which no worker imports (#439): polars and pyarrow are only a
# caller's, for its frames and tables.
NOT_IN_A_WORKER = ("duckdb", "fastexcel", "polars", "pyarrow", "xlsxwriter")

# A calling process whose workers cannot import NOT_IN_A_WORKER: it imports its own first, then puts first on
# sys.path (which each worker installs, and passes on to the render workers it starts) a directory holding a
# package of each name that refuses to import. It reads the document in two page workers, a figure's bytes in a
# figure worker and a page's image in a render worker.
BLOCKED_IN_WORKERS = """
import importlib, json, sys
from pathlib import Path

source, blocked, names = Path(sys.argv[1]), Path(sys.argv[2]), json.loads(sys.argv[3])
for name in names:
    importlib.import_module(name)  # the caller's own, before the directory hides it
    (blocked / name).mkdir(parents=True)
    refusal = f"raise ImportError('a PDF worker imported {name}', name={name!r})\\n"
    (blocked / name / "__init__.py").write_text(refusal, encoding="utf-8")
sys.path.insert(0, str(blocked))

from datagrunt.pdf.reader import PDFReader

reader = PDFReader(source, workers=2, min_image_dimension=32)
result = {"pages": [[page.status, page.error] for page in reader.document.pages]}
result["figure"] = len(reader.figures[0].read_bytes())
result["page_image"] = len(reader.pages[0].page_image.read_bytes())
print(json.dumps(result))
"""


class TestWorkersNeverLoadTheTabularStacks:
    """Every worker a read starts does its work without the CSV, Excel and Parquet stacks (#439)."""

    def test_a_read_whose_workers_cannot_import_them_reads_every_page_figure_and_image(self, tmp_path):
        source = build_pdf(tmp_path / "doc.pdf", [mixed_page(), mixed_page()])
        command = [sys.executable, "-I", "-c", BLOCKED_IN_WORKERS, str(source), str(tmp_path / "blocked")]

        done = subprocess.run(
            [*command, json.dumps(NOT_IN_A_WORKER)], capture_output=True, text=True, timeout=300, check=False
        )

        assert done.returncode == 0, done.stderr
        result = json.loads(done.stdout)
        assert result["pages"] == [["ok", None], ["ok", None]]
        assert result["figure"] > 0 and result["page_image"] > 0


class TestNoWorkerCanStart:
    """Where no worker can start, reading a PDF fails closed, saying why: nothing is read in the caller."""

    @pytest.mark.parametrize("workers", [1, 2])
    def test_a_frozen_application_is_told_plainly(self, tmp_path, monkeypatch, workers):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        monkeypatch.setattr(sys, "frozen", True, raising=False)

        with pytest.raises(DatagruntPDFError) as raised:
            extract_document(path, workers=workers)

        assert str(raised.value) == FROZEN

    @pytest.mark.parametrize("key", DISTRIBUTED_KEYS)
    def test_a_frozen_application_under_a_distributed_runner_is_told_plainly(self, tmp_path, monkeypatch, key):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        monkeypatch.setenv(key, "1")
        monkeypatch.setattr(sys, "frozen", True, raising=False)

        with pytest.raises(DatagruntPDFError) as raised:
            extract_document(path)

        assert str(raised.value) == FROZEN

    def test_an_empty_executable_is_told_plainly(self, tmp_path, monkeypatch):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()])
        monkeypatch.setattr(sys, "executable", "")

        with pytest.raises(DatagruntPDFError) as raised:
            extract_document(path)

        assert str(raised.value) == NO_EXECUTABLE


def started(monkeypatch, module) -> list:
    """Record each worker ``module`` starts (its command line), and start it."""
    commands: list = []
    real = module.start_worker

    def recorded(command, **options):
        commands.append(command)
        return real(command, **options)

    monkeypatch.setattr(module, "start_worker", recorded)
    return commands


class TestOneWorker:
    """workers=1, a one-page document and a distributed runner each extract in one page worker."""

    @pytest.mark.parametrize("pages,workers", [pytest.param(1, 4, id="one-page"), pytest.param(3, 1, id="one-worker")])
    def test_one_page_or_one_worker_is_one_page_worker(self, tmp_path, monkeypatch, pages, workers):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * pages)
        commands = started(monkeypatch, pipeline)

        document = extract_document(path, workers=workers)

        assert len(commands) == 1
        assert document == chain_document(path, title="untitled", author="anonymous")

    @pytest.mark.parametrize("key", DISTRIBUTED_KEYS)
    def test_a_distributed_runner_extracts_in_one_page_worker(self, tmp_path, monkeypatch, key):
        path = build_pdf(tmp_path / "doc.pdf", [mixed_page()] * 3)
        monkeypatch.setenv(key, "1")
        commands = started(monkeypatch, pipeline)

        document = extract_document(path, workers=4)

        assert len(commands) == 1
        assert document == chain_document(path, title="untitled", author="anonymous")


class TestPageWorkerCap:
    """A page worker past its cap ends: the page it was on is an error page, and the pages after it extract."""

    @pytest.mark.parametrize("workers", [1, 2])
    def test_a_page_past_the_cap_is_an_error_page_and_the_rest_extract(self, tmp_path, monkeypatch, workers):
        contents = [text_content("page one"), inline_content("page two"), text_content("page three")]
        path = pages_pdf(tmp_path / "doc.pdf", [*contents, text_content("page four")])
        small_cap(monkeypatch)

        document = extract_document(path, workers=workers, ocr="never")

        assert [page.status for page in document.pages] == ["ok", "error", "ok", "ok"]
        assert [text_of(page) for page in document.pages] == ["page one", "", "page three", "page four"]
        failed = document.pages[1]
        assert CAP_WORDS in failed.error and "page 2" in failed.error
        assert (failed.width_pt, failed.height_pt) == (612.0, 792.0)

    def test_strict_raises_naming_the_page_past_the_cap(self, tmp_path, monkeypatch):
        path = pages_pdf(tmp_path / "doc.pdf", [text_content("page one"), inline_content("page two")])
        small_cap(monkeypatch)

        with pytest.raises(DatagruntPDFError, match=f"{CAP_WORDS}.*page 2"):
            extract_document(path, strict=True, ocr="never")

    def test_a_document_whose_open_passes_the_cap_raises_a_corrupt_error_naming_it(self, tmp_path, monkeypatch):
        path = object_stream_pdf(tmp_path / "doc.pdf", INFLATED)
        small_cap(monkeypatch)

        with pytest.raises(PDFCorruptError) as raised:
            extract_document(path)

        assert raised.value.page_number is None
        assert CAP_WORDS in str(raised.value) and "the document is not read" in str(raised.value)

    def test_the_same_document_unpadded_extracts(self, tmp_path, monkeypatch):
        path = object_stream_pdf(tmp_path / "doc.pdf", 0)
        small_cap(monkeypatch)

        assert [page.status for page in extract_document(path, ocr="never").pages] == ["ok"]


class TestFigureReads:
    """A figure's bytes are read in a capped figure worker, in batches: the engine's own read, never the caller's."""

    def test_a_figure_read_is_the_engines_read(self, tmp_path):
        path = figures_pdf(tmp_path / "doc.pdf", 1)

        (figure,) = extract_document(path, min_image_dimension=32).figures

        (image,) = [item for item in page_images(open_document(path), 0).images if item.raw_sha256]
        engine = ImageLoader(source=path, page_index=0, image=image)
        assert (figure.read_bytes(), figure.read_raw_bytes()) == (engine.png_bytes(), engine.raw_bytes())

    def test_a_figure_past_its_cap_fails_naming_it(self, tmp_path):
        from datagrunt.pdf._loaders import FigureRead

        path = figures_pdf(tmp_path / "doc.pdf", 1)
        (figure,) = extract_document(path, min_image_dimension=32).figures
        read = dataclasses.replace(figure._loader.__self__, memory_cap_bytes=1)

        with pytest.raises(DatagruntPDFError, match="reached its 1-byte memory cap"):
            read.png_bytes()
        assert isinstance(read, FigureRead)

    def test_extract_images_reads_its_figures_in_one_worker_a_batch(self, tmp_path, monkeypatch):
        from datagrunt.pdf import _loaders
        from datagrunt.pdf.writer import PDFWriter

        path = figures_pdf(tmp_path / "doc.pdf", 5)
        writer = PDFWriter(path, min_image_dimension=32)
        figures = writer._extracted().figures
        monkeypatch.setattr(_loaders, "FIGURE_BATCH", 2)
        commands = started(monkeypatch, _loaders)

        written = writer.extract_images(tmp_path / "images")

        assert len(written) == 5 and len(commands) == 3
        assert [path.read_bytes() for path in written] == [figure.read_bytes() for figure in figures]

    def test_a_figure_that_ends_its_worker_fails_alone_and_the_rest_are_read(self, tmp_path):
        from datagrunt.pdf._loaders import read_figures

        path = figures_pdf(tmp_path / "doc.pdf", 3)
        reads = [figure._loader.__self__ for figure in extract_document(path, min_image_dimension=32).figures]
        capped = [reads[0], dataclasses.replace(reads[1], memory_cap_bytes=1), reads[2]]

        outcomes = list(read_figures(capped, "png"))

        assert outcomes[0] == reads[0].png_bytes() and outcomes[2] == reads[2].png_bytes()
        assert isinstance(outcomes[1], DatagruntPDFError) and "reached its 1-byte memory cap" in str(outcomes[1])


class TestEveryWorkerIsCapped:
    """Each worker the launcher can start caps its memory before it opens the PDF."""

    @pytest.mark.parametrize("key", sorted(_process.WORKERS))
    def test_each_worker_caps_itself_before_it_opens_the_pdf(self, tmp_path, monkeypatch, key):
        from datagrunt.pdf import pdfium_engine
        from datagrunt.pdf._memory_cap import Lifetime

        path = figures_pdf(tmp_path / "doc.pdf", 1)
        module = importlib.import_module(_process.WORKERS[key][0])
        events: list[str] = []
        monkeypatch.setattr(module, "apply_memory_cap", lambda *args: events.append("cap") or Lifetime(60.0))
        real_load = pdfium_engine._load_raw_document
        monkeypatch.setattr(
            pdfium_engine, "_load_raw_document", lambda *args: events.append("open") or real_load(*args)
        )
        request = json.loads(worker_payload(key, path))["request"]

        module._worker_main(request, io.BytesIO())

        assert events[:2] == ["cap", "open"]


def worker_payload(key: str, path: Path) -> bytes:
    """A request each worker can serve on ``path``, as its caller writes it."""
    from datagrunt.pdf import _loaders, render

    if key == "render":
        request = render._BatchRequest(
            source=str(path),
            pages=((0, 72),),
            password=None,
            memory_cap_bytes=1 << 30,
            timeout_s=60.0,
            colour=False,
            without_text=False,
        )
        return render._batch_payload(request)
    if key == "pages":
        return _process.request_payload(
            "pages",
            pipeline._chunk_payload(pipeline.PDFSource(path=path), None, 1, pipeline.PipelineOptions()),
            1 << 20,
        )
    (figure,) = extract_document(path, min_image_dimension=32).figures
    return _loaders._batch_payload([figure._loader.__self__], "png")
