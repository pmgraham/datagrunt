"""This module contains shared fixtures for pytest."""

import os
import shutil
import subprocess
import tempfile
import warnings

import pytest

from datagrunt.core import CSVEngineFactory


@pytest.fixture(autouse=True, scope="session")
def _ensure_tesseract_tmpdir():
    """Redirect pytesseract's temp files to a path the tesseract binary can read.

    In sandboxed environments TMPDIR may be set to a restricted path that the
    tesseract binary cannot open (e.g. /tmp/claude-501).  This fixture detects
    that condition and points ``tempfile.tempdir`` to the macOS user temp dir
    (/var/folders/…) for the duration of the test session.
    """
    probe_dir = tempfile.gettempdir()
    probe_input = os.path.join(probe_dir, "_tess_probe_input.PNG")

    tess_bin = shutil.which("tesseract")
    if tess_bin is None:
        yield
        return

    # Write a tiny dummy PNG to probe whether tesseract can open the current tmpdir.
    try:
        from PIL import Image  # noqa: PLC0415

        img = Image.new("RGB", (10, 10), color="white")
        img.save(probe_input, format="PNG")
        result = subprocess.run(
            [tess_bin, probe_input, probe_input[:-4], "tsv"],
            capture_output=True,
            timeout=5,
        )
        tesseract_can_read = result.returncode == 0
    except Exception as exc:
        warnings.warn(
            f"tesseract tmpdir probe failed ({exc!r}); assuming OK",
            stacklevel=2,
        )
        tesseract_can_read = True  # assume OK; let the real test surface the error
    finally:
        if os.path.exists(probe_input):
            os.unlink(probe_input)
        for ext in (".tsv", ".txt"):
            candidate = probe_input[:-4] + ext
            if os.path.exists(candidate):
                os.unlink(candidate)

    if not tesseract_can_read:
        # Fall back to macOS user-temp (/var/folders/…) which tesseract CAN open.
        fallback = os.path.join(os.path.expanduser("~"), ".pytest_ocr_tmp")
        os.makedirs(fallback, exist_ok=True)
        original = tempfile.tempdir
        tempfile.tempdir = fallback
        try:
            yield
        finally:
            tempfile.tempdir = original
            shutil.rmtree(fallback, ignore_errors=True)
    else:
        yield


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing."""
    csv_content = "name,age,city\nJohn,30,New York\nJane,25,Boston"
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)


@pytest.fixture
def empty_csv(tmp_path):
    """Create an empty CSV file for testing."""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("")
    return str(csv_file)


@pytest.fixture
def blank_csv(tmp_path):
    """Create a blank CSV file (only whitespace) for testing."""
    csv_file = tmp_path / "blank.csv"
    csv_file.write_text("   \n  \n")
    return str(csv_file)


@pytest.fixture
def completely_empty_csv(tmp_path):
    """Create a completely empty CSV file for testing."""
    csv_file = tmp_path / "completely_empty.csv"
    csv_file.touch()  # Creates an empty file
    return str(csv_file)


@pytest.fixture
def engine_factory(sample_csv):
    """Create an EngineFactory instance."""
    return CSVEngineFactory(sample_csv, "duckdb")


@pytest.fixture
def sample_files(tmp_path):
    """Create sample test files."""
    # Create various test files
    files = {
        "empty.csv": "",
        "blank.csv": "   \n   \n",
        "data.csv": "a,b,c\n1,2,3",
        "test.xlsx": "dummy excel content",
        "test.json": '{"key": "value"}',
        "test.parquet": "dummy parquet content",
        "test.txt": "plain text content",
    }

    created_files = {}
    for filename, content in files.items():
        file_path = tmp_path / filename
        file_path.write_text(content)
        created_files[filename] = file_path

    return created_files


@pytest.fixture
def sample_csv_files(tmp_path):
    """Create sample CSV files for testing."""
    files = {
        "comma.csv": "Name,Age,City\nJohn,30,New York\nJane,25,London",
        "semicolon.csv": "Name;Age;City\nJohn;30;New York\nJane;25;London",
        "tab.csv": "Name\tAge\tCity\nJohn\t30\tNew York\nJane\t25\tLondon",
        "empty.csv": "",
        "blank.csv": "   \n   \n",
        "quoted.csv": '"Name","Age","City"\n"John","30","New York"\n"Jane","25","London"',
        "messy_headers.csv": "First Name!,#Age@,(City)",
    }

    created_files = {}
    for filename, content in files.items():
        file_path = tmp_path / filename
        file_path.write_text(content)
        created_files[filename] = str(file_path)

    return created_files


@pytest.fixture
def sample_pdf(tmp_path):
    """Create a one-page PDF with native text and an embedded image."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)  # US Letter
    page.insert_text((72, 72), "Quarterly Report", fontsize=24)  # header (large)
    # Several body lines at 11pt so the median font size is small and the 24pt
    # title is correctly classified as a header (header threshold = median*1.6).
    page.insert_text((72, 120), "This is body text for testing.", fontsize=11)
    page.insert_text((72, 140), "Body line two for the report.", fontsize=11)
    page.insert_text((72, 160), "Body line three with details.", fontsize=11)
    page.insert_text((72, 180), "Body line four wraps up the text.", fontsize=11)

    # Build a 100x100 red PNG and embed it on the page.
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
    pix.set_rect(pix.irect, (255, 0, 0))
    img_bytes = pix.tobytes("png")
    page.insert_image(pymupdf.Rect(72, 200, 172, 300), stream=img_bytes)

    pdf_path = tmp_path / "report.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def encrypted_pdf(tmp_path):
    """Create a one-page AES-256 encrypted (password-protected) PDF."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 72), "Confidential", fontsize=12)

    pdf_path = tmp_path / "encrypted.pdf"
    doc.save(
        str(pdf_path),
        encryption=pymupdf.PDF_ENCRYPT_AES_256,
        owner_pw="owner-secret",
        user_pw="user-secret",
    )
    doc.close()
    return str(pdf_path)


@pytest.fixture
def empty_pdf(tmp_path):
    """Create a 0-byte PDF file."""
    pdf_path = tmp_path / "empty.pdf"
    pdf_path.touch()
    return str(pdf_path)


@pytest.fixture
def scanned_pdf(tmp_path):
    """Create a one-page PDF whose only content is a rendered-text image.

    The text is rendered to a raster at 150 dpi and re-embedded as an image, so
    the page has NO extractable text layer (pdfium returns empty text) and only
    OCR can recover the words. Used to exercise the OCR fallback path.
    """
    import pymupdf

    src = pymupdf.open()
    src_page = src.new_page(width=612, height=792)
    src_page.insert_text((72, 100), "HELLO WORLD", fontsize=48)
    pix = src_page.get_pixmap(dpi=150)
    src.close()

    img_path = tmp_path / "_scan.png"
    pix.save(str(img_path))

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_image(pymupdf.Rect(0, 0, 612, 792), filename=str(img_path))
    pdf_path = tmp_path / "scanned.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def multipage_scanned_pdf(tmp_path):
    """Create a 2-page PDF whose pages are each a rendered-text image.

    Mirrors ``scanned_pdf`` (no extractable text layer, only OCR can recover
    the words) but with two pages, so a ``workers>1`` process pool actually
    splits OCR work across page workers instead of falling back to the
    single-page sequential path.
    """
    import pymupdf

    doc = pymupdf.open()
    for n in (1, 2):
        src = pymupdf.open()
        src_page = src.new_page(width=612, height=792)
        src_page.insert_text((72, 100), f"HELLO WORLD {n}", fontsize=48)
        pix = src_page.get_pixmap(dpi=150)
        src.close()

        img_path = tmp_path / f"_scan_{n}.png"
        pix.save(str(img_path))

        page = doc.new_page(width=612, height=792)
        page.insert_image(pymupdf.Rect(0, 0, 612, 792), filename=str(img_path))
    pdf_path = tmp_path / "multipage_scanned.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def multipage_pdf(tmp_path):
    """Create a 3-page PDF with distinct, identifiable text on each page."""
    import pymupdf

    doc = pymupdf.open()
    for n in range(1, 4):
        page = doc.new_page(width=612, height=792)
        page.insert_text((72, 72), f"Page Marker {n}", fontsize=18)
        page.insert_text((72, 110), f"Body content for page {n}.", fontsize=11)
    pdf_path = tmp_path / "multipage.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def small_image_pdf(tmp_path):
    """Create a one-page PDF whose only image is below the 40px ignore threshold."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 20, 20))
    pix.set_rect(pix.irect, (0, 0, 255))
    page.insert_image(pymupdf.Rect(72, 72, 92, 92), stream=pix.tobytes("png"))
    pdf_path = tmp_path / "small_image.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def two_page_image_pdf(tmp_path):
    """Create a 2-page PDF with one 100px image on each page."""
    import pymupdf

    doc = pymupdf.open()
    for _ in range(2):
        page = doc.new_page(width=612, height=792)
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
        pix.set_rect(pix.irect, (255, 0, 0))
        page.insert_image(pymupdf.Rect(72, 200, 172, 300), stream=pix.tobytes("png"))
    pdf_path = tmp_path / "imagepages.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def multipage_small_image_pdf(tmp_path):
    """Create a 2-page PDF; each page's only image is below the 40px threshold."""
    import pymupdf

    doc = pymupdf.open()
    for _ in range(2):
        page = doc.new_page(width=612, height=792)
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 20, 20))
        pix.set_rect(pix.irect, (0, 0, 255))
        page.insert_image(pymupdf.Rect(72, 72, 92, 92), stream=pix.tobytes("png"))
    pdf_path = tmp_path / "multipage_small_image.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def tesseract_available():
    """Return True if the tesseract system binary is on PATH."""
    return shutil.which("tesseract") is not None


@pytest.fixture
def sample_xlsx(tmp_path):
    """Create a 3-sheet workbook: People, Products, and Messy (raw headers)."""
    import polars as pl
    import xlsxwriter

    path = tmp_path / "test.xlsx"
    with xlsxwriter.Workbook(str(path)) as wb:
        pl.DataFrame({"name": ["John", "Jane"], "age": [30, 25], "city": ["New York", "Boston"]}).write_excel(
            workbook=wb, worksheet="People"
        )
        pl.DataFrame({"product": ["A", "B"], "price": [1.5, 2.5]}).write_excel(workbook=wb, worksheet="Products")
        pl.DataFrame({"First Name!": ["x"], "#Age@": [1]}).write_excel(workbook=wb, worksheet="Messy")
    return str(path)


@pytest.fixture
def empty_xlsx(tmp_path):
    """Create a 0-byte .xlsx file."""
    path = tmp_path / "empty.xlsx"
    path.touch()
    return str(path)


@pytest.fixture
def nonexistent_xlsx(tmp_path):
    """Return a path to an .xlsx that does not exist."""
    return str(tmp_path / "missing.xlsx")


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a small Parquet file with clean and messy column names."""
    import polars as pl

    path = tmp_path / "test.parquet"
    pl.DataFrame(
        {
            "name": ["John", "Jane", "Amir", "Mei"],
            "age": [30, 25, 41, 38],
            "city": ["New York", "Boston", "Cairo", "Taipei"],
        }
    ).write_parquet(str(path))
    return str(path)


@pytest.fixture
def messy_parquet(tmp_path):
    """Create a Parquet file with non-normalized column names."""
    import polars as pl

    path = tmp_path / "messy.parquet"
    pl.DataFrame({"First Name!": ["x", "y"], "#Age@": [1, 2]}).write_parquet(str(path))
    return str(path)


@pytest.fixture
def empty_parquet(tmp_path):
    """Create a 0-byte .parquet file."""
    path = tmp_path / "empty.parquet"
    path.touch()
    return str(path)


@pytest.fixture
def nonexistent_parquet(tmp_path):
    """Return a path to a .parquet that does not exist."""
    return str(tmp_path / "missing.parquet")
