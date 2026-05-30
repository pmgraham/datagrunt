"""This module contains shared fixtures for pytest."""

import shutil

import pytest

from datagrunt.core import CSVEngineFactory


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
def empty_pdf(tmp_path):
    """Create a 0-byte PDF file."""
    pdf_path = tmp_path / "empty.pdf"
    pdf_path.touch()
    return str(pdf_path)


@pytest.fixture
def tesseract_available():
    """Return True if the tesseract system binary is on PATH."""
    return shutil.which("tesseract") is not None
