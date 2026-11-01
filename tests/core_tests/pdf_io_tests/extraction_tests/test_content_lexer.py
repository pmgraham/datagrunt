"""Tests for the content-stream lexer (``core.pdf_io.extraction.content_lexer``).

It reads every inline image's dictionary token by token up to ``ID``, skips
strings and comments, and fails closed (returns a reason) on anything it
cannot read for certain.
"""

import pytest

from datagrunt.core.pdf_io.extraction.content_lexer import (
    GLOBALS_TOKEN,
    JBIG2_DECODE_TOKEN,
    InlineImage,
    count_tokens,
    inline_images,
)


def one(content: bytes) -> InlineImage:
    found = inline_images(content)
    assert not isinstance(found, str), found
    (image,) = found
    return image


class TestInlineImages:
    def test_reads_filters_parameters_and_data(self):
        image = one(b"q BI /W 2 /H 1 /CS /G /BPC 8 /F [/AHx /Fl] /DP [null << /Predictor 2 >>] ID 0aff> EI Q")

        assert image[:4] == (("AHx", "Fl"), False, b"0aff>", True)

    def test_reads_a_single_filter_name_and_globals_in_the_parameters(self):
        image = one(b"BI /F /JBIG2Decode /DecodeParms << /JBIG2Glob#61ls 6 0 R >> ID \x00\x01 EI")

        assert (image.filters, image.has_globals, image.data) == (("JBIG2Decode",), True, b"\x00\x01")

    @pytest.mark.parametrize("sep", [b"\x00", b"\t", b"\n", b"\x0c", b"\r", b" "], ids=repr)
    def test_every_pdf_whitespace_separates_tokens(self, sep):
        content = sep.join([b"q", b"BI", b"/F", b"/JPXDecode", b"ID", b"data", b"EI", b"Q"])

        image = one(content)

        assert (image.filters, image.data, image.delimited) == (("JPXDecode",), b"data", True)

    def test_a_dictionary_past_4_kb_is_read_whole(self):
        image = one(b"BI /Pad (" + b"a" * 5000 + b") /F /JPXDecode ID x EI")

        assert image.filters == ("JPXDecode",)

    def test_keys_are_unescaped(self):
        assert one(b"BI /#46 /JPX#44ecode ID x EI").filters == ("JPXDecode",)

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"BT (the BI team) Tj ET", id="string"),
            pytest.param(b"BT (a (nested BI\\) paren) BI) Tj ET", id="nested-and-escaped"),
            pytest.param(b"BT <4249> Tj ET % BI ID\nQ", id="hex-and-comment"),
            pytest.param(b"/BI BDC EMC", id="a-name"),
        ],
    )
    def test_bi_outside_an_operator_is_not_an_inline_image(self, content):
        assert inline_images(content) == ()

    def test_scanning_resumes_after_ei_and_finds_every_image(self):
        found = inline_images(b"BI /F /Fl ID a EI BI /F /DCT ID b EI")

        assert [(image.filters, image.data) for image in found] == [(("Fl",), b"a"), (("DCT",), b"b")]

    def test_data_without_ei_runs_to_the_end(self):
        image = one(b"BI /F /Fl ID abc")

        assert (image.data, image.delimited) == (b"abc", False)

    @pytest.mark.parametrize(
        "content,reason",
        [
            pytest.param(b"BI /F /Fl /F /JPXDecode ID x EI", "names its filter twice", id="repeated-f"),
            pytest.param(b"BI /Filter /Fl /F /JPXDecode ID x EI", "names its filter twice", id="filter-and-f"),
            pytest.param(b"BI /DP null /DecodeParms null ID x EI", "names its decode parameters twice", id="dp-twice"),
            pytest.param(
                b"BI /F (JPXDecode) ID x EI",
                "has a filter that is neither a name nor an array of names",
                id="string-filter",
            ),
            pytest.param(
                b"BI /F [/Fl 5] ID x EI",
                "has a filter that is neither a name nor an array of names",
                id="array-with-a-number",
            ),
            pytest.param(
                b"BI /F [[/Fl]] ID x EI",
                "has a filter that is neither a name nor an array of names",
                id="nested-array",
            ),
            pytest.param(b"q BI /W 16 /F /JPXDecode", "has no ID before its content stream ends", id="no-id"),
            pytest.param(
                b"q BI /W 16 /F /JPXDecode Q",
                "has a dictionary that cannot be read (a word where a key belongs)",
                id="operator-before-id",
            ),
            pytest.param(b"BI /F 5 0 R ID x EI", "has a filter that is neither a name nor an array of names", id="ref"),
            pytest.param(b"BI /W (unended ID x EI", "has a string that does not end", id="open-string"),
            pytest.param(b"BI /W <4142 ID x EI", "has a hex string that does not end", id="open-hex"),
            pytest.param(
                b"BI /DP << /K 1 ID x EI",
                "has a dictionary that does not end",
                id="open-dict",
            ),
            pytest.param(b"BI /DP [" + b"[" * 40 + b" ID", "has a dictionary nested past 32 levels", id="too-deep"),
        ],
    )
    def test_what_cannot_be_read_for_certain_is_a_reason(self, content, reason):
        assert inline_images(content) == reason

    @pytest.mark.parametrize(
        "after", [b"/Q", b"(x)", b"[", b"<<", b"%c"], ids=["name", "string", "array", "dict", "comment"]
    )
    def test_ei_followed_by_a_delimiter_is_a_reason(self, after):
        assert inline_images(b"BI /F /Fl ID abc EI" + after) == "has an EI followed by a delimiter, not whitespace"

    def test_ei_at_the_end_of_the_stream_ends_the_data(self):
        assert one(b"BI /F /Fl ID abc EI").data == b"abc"

    def test_the_dictionary_bytes_are_kept(self):
        assert one(b"q BI /W 2 /F /JPX#2344ecode ID x EI").dictionary == b" /W 2 /F /JPX#2344ecode "

    def test_a_filter_array_of_100k_names_is_a_reason_and_fast(self):
        import time

        content = b"BI /F [" + b"/Fl " * 100_000 + b"] ID x EI"
        started = time.perf_counter()

        assert inline_images(content) == "has an array or dictionary of more than 64 entries"
        assert time.perf_counter() - started < 2.0

    def test_a_dictionary_of_many_entries_is_a_reason(self):
        content = b"BI /DP << " + b"/K 1 " * 65 + b">> ID x EI"

        assert inline_images(content) == "has an array or dictionary of more than 64 entries"

    def test_an_image_dictionary_of_many_entries_is_a_reason(self):
        content = b"BI " + b"/K 1 " * 65 + b"ID x EI"

        assert inline_images(content) == "has an array or dictionary of more than 64 entries"

    def test_an_array_of_64_names_is_read(self):
        assert len(one(b"BI /F [" + b"/Fl " * 64 + b"] ID x EI").filters) == 64

    def test_an_unended_string_outside_an_image_is_a_reason(self):
        assert inline_images(b"BT (open Tj ET") == "has a string that does not end"


class TestTokens:
    @pytest.mark.parametrize(
        "data,count",
        [
            pytest.param(b"/JBIG2Globals 6 0 R", 1, id="literal"),
            pytest.param(b"/JBIG2#47lobals /#4a#42IG2Globals", 2, id="escaped-either-case"),
            pytest.param(b"(/JBIG2Globals) % /JBIG2Globals", 2, id="in-strings-and-comments"),
            pytest.param(b"JBIG2Globals /JBIG2Global", 0, id="not-a-name"),
        ],
    )
    def test_counts_globals_names(self, data, count):
        assert count_tokens(GLOBALS_TOKEN, data) == count

    def test_counts_jbig2_decode_names(self):
        assert count_tokens(JBIG2_DECODE_TOKEN, b"/F /JBIG2#44ecode /JBIG2Decode") == 2
