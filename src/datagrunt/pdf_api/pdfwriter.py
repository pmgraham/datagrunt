"""Module for writing parsed PDF output (JSON + image files)."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core import PDFComponents, PDFEngineFactory


class PDFWriter(PDFComponents):
    """Class to unify the interface for writing parsed PDF output."""

    def __init__(self, filepath, engine="pymupdf", workers=4):
        """Initialize the PDF Writer class.

        Args:
            filepath (str or Path): Path to the PDF file to parse.
            engine (str, default 'pymupdf'): Parsing engine to instantiate.
            workers (int, default 4): Number of concurrent per-page workers.
        """
        filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers

    def _create_writer(self):
        """Create a writer engine instance."""
        return PDFEngineFactory(self.filepath, self.engine, self.workers).create_writer()

    def write_json(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write the unified document JSON to disk.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written there and referenced in the JSON.

        Returns:
            str: The path of the written JSON file.
        """
        return self._create_writer().write_json(export_filename, image_output_dir)

    def write_json_newline_delimited(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write one flattened element per line (JSONL).

        Args:
            export_filename (optional, str): Output path; defaults to output.jsonl.
            image_output_dir (optional, str): If provided, embedded images are
                written there.

        Returns:
            str: The path of the written JSONL file.
        """
        return self._create_writer().write_json_newline_delimited(
            export_filename, image_output_dir
        )

    def extract_images(self, output_dir=None):
        """Parse the PDF and write embedded image files to disk.

        Args:
            output_dir (optional, str): Output directory; defaults to
                output_images.

        Returns:
            list: Paths of the written image files.
        """
        return self._create_writer().extract_images(output_dir)
