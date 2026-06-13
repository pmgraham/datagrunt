"""Document-level batch PDF processing across worker processes.

Parallelizes across documents (not pages): each PDF is processed in its own
process, single-threaded, writing its outputs per document. This is the scaling
model for large corpora — per-page threads are GIL-bound and do not speed up
extraction. Inside Apache Beam/Dataflow, do NOT use this; map the per-document
work in a DoFn with ``PDFWriter(path, workers=1)`` and let the runner own
cross-document fan-out.

Each output kind is written to its own subdirectory of the batch output dir, so
text and images never mingle::

    output_dir/
        json/      <stem>.json     (json=True, default)
        jsonl/     <stem>.jsonl    (jsonl=True)
        markdown/  <stem>.md       (markdown=True)
        images/    <stem>/...      (images=True, default)
"""

# standard library
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class _BatchOptions:
    """Per-document processing options passed to worker processes."""

    output_dir: str
    engine: str
    json: bool
    jsonl: bool
    markdown: bool
    images: bool
    dedupe_images: bool
    drop_layout_tables: bool


def _process_one_pdf(path, options):
    """Process a single PDF in a worker process.

    Module-level and picklable so it works under the ``spawn`` start method.
    Returns a result record; never raises (errors are captured per document).
    """
    source = str(path)
    try:
        # Imported here so child processes only load the PDF stack when used.
        from datagrunt.pdf_api.pdfwriter import PDFWriter

        stem = Path(source).stem
        writer = PDFWriter(source, engine=options.engine, workers=1)
        result = {"source": source, "status": "success"}

        # Embedded images go to their own per-document directory under images/.
        # When at least one text format is requested, that writer extracts the
        # images while serializing; otherwise we extract them on their own.
        images_dir = os.path.join(options.output_dir, "images", stem) if options.images else None
        if images_dir:
            os.makedirs(images_dir, exist_ok=True)
            result["images_dir"] = images_dir

        if options.json:
            json_path = os.path.join(options.output_dir, "json", f"{stem}.json")
            writer.write_json(
                export_filename=json_path,
                image_output_dir=images_dir,
                dedupe_images=options.dedupe_images,
                drop_layout_tables=options.drop_layout_tables,
            )
            result["json_path"] = json_path

        if options.jsonl:
            jsonl_path = os.path.join(options.output_dir, "jsonl", f"{stem}.jsonl")
            writer.write_json_newline_delimited(
                export_filename=jsonl_path,
                image_output_dir=images_dir,
                dedupe_images=options.dedupe_images,
                drop_layout_tables=options.drop_layout_tables,
            )
            result["jsonl_path"] = jsonl_path

        if options.markdown:
            markdown_path = os.path.join(options.output_dir, "markdown", f"{stem}.md")
            writer.write_markdown(
                export_filename=markdown_path,
                image_output_dir=images_dir,
                dedupe_images=options.dedupe_images,
                drop_layout_tables=options.drop_layout_tables,
            )
            result["markdown_path"] = markdown_path

        # Images-only batch: no text writer ran, so extract images directly.
        if images_dir and not (options.json or options.jsonl or options.markdown):
            writer.extract_images(output_dir=images_dir, dedupe=options.dedupe_images)

        return result
    except Exception as e:  # noqa: BLE001 - per-document isolation
        return {"source": source, "status": "error", "error": str(e)}


class PDFBatchWriter:
    """Write JSON, JSONL, Markdown, and images for many PDFs across processes.

    Output options that apply to every document in the batch are set once on the
    instance; :meth:`process` then runs a corpus of PDFs against those settings,
    writing each output kind to its own subdirectory of the batch output dir
    (text never mingles with images). This mirrors the per-document
    :class:`~datagrunt.pdf_api.pdfwriter.PDFWriter` so the batch surface reads
    the same way as the rest of the package.

    Example:
        writer = PDFBatchWriter(markdown=True)        # JSON + Markdown + images
        results = writer.process(["a.pdf", "b.pdf"], "out/")

    Args:
        engine: PDF engine to use (currently ``"pymupdf"``).
        json: Write ``output_dir/json/<stem>.json`` per document.
        jsonl: Write ``output_dir/jsonl/<stem>.jsonl`` per document.
        markdown: Write ``output_dir/markdown/<stem>.md`` per document.
        images: Write embedded images to ``output_dir/images/<stem>/``.
        dedupe_images: Collapse byte-identical duplicate images per document.
        drop_layout_tables: Drop 1xN / Nx1 layout-box "tables".

    Raises:
        ValueError: If no output kind is enabled (all of ``json``, ``jsonl``,
            ``markdown``, and ``images`` are False).
    """

    def __init__(
        self,
        *,
        engine="pymupdf",
        json=True,
        jsonl=False,
        markdown=False,
        images=True,
        dedupe_images=True,
        drop_layout_tables=False,
    ):
        if not (json or jsonl or markdown or images):
            raise ValueError(
                "PDFBatchWriter has nothing to write: enable at least one of "
                "json, jsonl, markdown, or images."
            )
        self.engine = engine
        self.json = json
        self.jsonl = jsonl
        self.markdown = markdown
        self.images = images
        self.dedupe_images = dedupe_images
        self.drop_layout_tables = drop_layout_tables

    def process(self, paths, output_dir, *, max_workers=None):
        """Process many PDFs concurrently, writing the enabled outputs per document.

        Args:
            paths: Iterable of PDF file paths (str or Path).
            output_dir: Directory where per-document outputs are written. Created
                if it does not exist, with a subdirectory per enabled output kind
                (``json/``, ``jsonl/``, ``markdown/``, ``images/``).
            max_workers: Number of worker processes. ``None`` uses the
                ProcessPoolExecutor default (``os.cpu_count()``).

        Returns:
            A list of result records (input order preserved). On success:
            ``{"source", "status": "success", ...}`` with a ``json_path`` /
            ``jsonl_path`` / ``markdown_path`` / ``images_dir`` key for each
            enabled output. On failure: ``{"source", "status": "error", "error"}``.
        """
        sources = [str(p) for p in paths]
        if not sources:
            return []
        output_dir = str(output_dir)
        for enabled, subdir in (
            (self.json, "json"),
            (self.jsonl, "jsonl"),
            (self.markdown, "markdown"),
            (self.images, "images"),
        ):
            if enabled:
                os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
        options = _BatchOptions(
            output_dir=output_dir,
            engine=self.engine,
            json=self.json,
            jsonl=self.jsonl,
            markdown=self.markdown,
            images=self.images,
            dedupe_images=self.dedupe_images,
            drop_layout_tables=self.drop_layout_tables,
        )
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(_process_one_pdf, sources, [options] * len(sources)))
