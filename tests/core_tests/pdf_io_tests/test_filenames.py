"""Tests for the PDF output-filename builders (single source of the numbering convention)."""

from datagrunt.core.pdf_io.filenames import embedded_image_filename, page_image_filename


class TestPageImageFilename:
    def test_first_page_is_one_based_and_zero_padded(self):
        assert page_image_filename("report", 0, "png") == "report_page_01.png"

    def test_tenth_page_sorts_after_second(self):
        names = [page_image_filename("report", i, "png") for i in range(10)]
        assert names[-1] == "report_page_10.png"
        assert sorted(names) == names


class TestEmbeddedImageFilename:
    def test_page_and_image_numbers_are_one_based_and_zero_padded(self):
        assert embedded_image_filename("report", 0, 0, "png") == "report_page01_img01.png"
        assert embedded_image_filename("report", 1, 2, "jpg") == "report_page02_img03.jpg"

    def test_without_extension_returns_stem(self):
        assert embedded_image_filename("report", 9, 10) == "report_page10_img11"

    def test_names_sort_in_page_then_image_order(self):
        names = [embedded_image_filename("doc", p, i, "png") for p in range(11) for i in range(11)]
        assert sorted(names) == names
