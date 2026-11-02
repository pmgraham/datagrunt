# Changelog

Release notes for datagrunt are published in two places:

- **[GitHub Releases](https://github.com/pmgraham/datagrunt/releases)** — per-version notes generated from merged pull requests (from 4.5.4 onward).
- **[datagrunt.io release-notes blog](https://www.datagrunt.io/blog/release-notes/)** — narrative release posts with usage examples.

Versions follow semantic versioning with bare-semver tags (e.g. `4.5.4`). Every release to [PyPI](https://pypi.org/project/datagrunt/) corresponds to a tag and a GitHub Release.

## Highlights

- **5.x** — Requires Python 3.11 or newer. PDF extraction on pypdfium2 alone: a typed document model (headings, paragraphs, key-value pairs, tables, lists, figures) in reading order with section scope, hidden text flagged (never dropped), and per-page routes (`digital` / `hybrid` / `vision`) and statuses; `PDFReader` reads with zero filesystem writes, and `PDFWriter` adds atomic, checksummed bundles; OCR through the system `tesseract` CLI; PyMuPDF, pdfplumber and pytesseract removed (see the [migration guide](https://www.datagrunt.io/docs/guides/migrating-to-5/)).
- **4.x** — Rust-accelerated CSV core (`datagrunt._native`, required default) with a pure-Python differential-parity oracle; PDF parsing (PDFium default, PyMuPDF alternative, tables, OCR); Excel and Parquet read/write; automatic resource lifecycle.
