"""Document-level batch PDF processing across worker processes.

Parallelizes across documents (not pages): each PDF is processed in its own
process, single-threaded, writing one JSON (and optionally images) per document.
This is the scaling model for large corpora — per-page threads are GIL-bound and
do not speed up extraction. Inside Apache Beam/Dataflow, do NOT use this; map the
per-document work in a DoFn with ``PDFWriter(path, workers=1)`` and let the
runner own cross-document fan-out.
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
        json_path = os.path.join(options.output_dir, f"{stem}.json")
        image_output_dir = os.path.join(options.output_dir, f"{stem}_images") if options.images else None

        writer = PDFWriter(source, engine=options.engine, workers=1)
        written = writer.write_json(
            export_filename=json_path,
            image_output_dir=image_output_dir,
            dedupe_images=options.dedupe_images,
            drop_layout_tables=options.drop_layout_tables,
        )
        return {"source": source, "status": "success", "json_path": written}
    except Exception as e:  # noqa: BLE001 - per-document isolation
        return {"source": source, "status": "error", "error": str(e)}


class PDFBatchWriter:
    """Write JSON (and optionally images) for many PDFs across worker processes.

    Extraction options that apply to every document in the batch are set once on
    the instance; :meth:`process` then runs a corpus of PDFs against those
    settings. This mirrors the per-document
    :class:`~datagrunt.pdf_api.pdfwriter.PDFWriter` so the batch surface reads
    the same way as the rest of the package.

    Example:
        writer = PDFBatchWriter(images=True, dedupe_images=True)
        results = writer.process(["a.pdf", "b.pdf"], "out/")

    Args:
        engine: PDF engine to use (currently ``"pymupdf"``).
        images: Whether to extract embedded images per document.
        dedupe_images: Collapse byte-identical duplicate images per document.
        drop_layout_tables: Drop 1xN / Nx1 layout-box "tables".
    """

    def __init__(
        self,
        *,
        engine="pymupdf",
        images=True,
        dedupe_images=True,
        drop_layout_tables=False,
    ):
        self.engine = engine
        self.images = images
        self.dedupe_images = dedupe_images
        self.drop_layout_tables = drop_layout_tables

    def process(self, paths, output_dir, *, max_workers=None):
        """Process many PDFs concurrently, writing one set of outputs per document.

        Args:
            paths: Iterable of PDF file paths (str or Path).
            output_dir: Directory where per-document outputs are written. Created
                if it does not exist. Each PDF yields ``<stem>.json`` and, when
                ``images`` is True, an ``<stem>_images/`` directory.
            max_workers: Number of worker processes. ``None`` uses the
                ProcessPoolExecutor default (``os.cpu_count()``).

        Returns:
            A list of result records (input order preserved), each one of:
            ``{"source", "status": "success", "json_path"}`` or
            ``{"source", "status": "error", "error"}``.
        """
        os.makedirs(output_dir, exist_ok=True)
        options = _BatchOptions(
            output_dir=str(output_dir),
            engine=self.engine,
            images=self.images,
            dedupe_images=self.dedupe_images,
            drop_layout_tables=self.drop_layout_tables,
        )
        sources = [str(p) for p in paths]
        if not sources:
            return []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(_process_one_pdf, sources, [options] * len(sources)))
