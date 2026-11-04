"""Markdown escaping in v5: link destinations (#221) and element text, end to end through write_markdown (#376).

#221: escape_markdown_link_target must handle whitespace and newlines
      (angle-bracket wrapping for space/tab; stripping for newlines).
#376: text that opens Markdown block syntax must render back to itself. v5's
      elements escape their own text; the end-to-end cases check it through
      PDFWriter.write_markdown on reportlab-built PDFs.
"""

import pytest
from markdown_it import MarkdownIt

from datagrunt.pdf import _markdown_escape
from datagrunt.pdf._markdown_escape import escape_markdown_link_target
from datagrunt.pdf.reader import PDFReader
from datagrunt.pdf.writer import PDFWriter
from pdf_tests.builders import PageSpec, TextRun, build_pdf

_COMMONMARK = MarkdownIt("commonmark")


# Lines that open Markdown block syntax at their start but which the v5 layout reads as plain paragraph text.
_PARAGRAPH_LINES = [
    "# not a heading",
    "> not a quote",
    "= not an underline",
    "+ not a bullet",
    "| a | b |",
    "``` not a fence",
    "~~~ not a fence",
    "<div> not html",
    "_x_ not emphasis",
]


def _clause_pdf(tmp_path, line):
    """One page: a 24pt title and three 11pt body lines, the middle one ``line``."""
    page = PageSpec(
        items=(
            TextRun("Quarterly Report", 72, 72, size=24),
            TextRun("This is body text for testing.", 72, 120),
            TextRun(line, 72, 160),
            TextRun("Body line four wraps up the text.", 72, 200),
        )
    )
    return build_pdf(tmp_path / "clause.pdf", [page])


def _markdown_blocks(path):
    """The CommonMark file at ``path``: its top-level block types, and each inline's (opener type, visible text)."""
    tokens = _COMMONMARK.parse(path.read_text(encoding="utf-8"))
    top_level = [token.type for token in tokens if token.level == 0 and token.nesting != -1]
    inlines = [
        (tokens[index - 1].type, "".join(child.content for child in token.children or []))
        for index, token in enumerate(tokens)
        if token.type == "inline"
    ]
    return top_level, inlines


class TestWriteMarkdownEndToEnd:
    """A PDF line that opens Markdown syntax comes out of the v5 ``PDFWriter.write_markdown`` as its own text.

    v4's end-to-end case (#376) was a paragraph starting ``2) ``: v5's layout reads such a line as an ordered
    list item instead, keeping its marker (the last test). The escape is checked end to end on the lines v5
    keeps as paragraphs.
    """

    @pytest.mark.parametrize("line", _PARAGRAPH_LINES)
    def test_block_syntax_line_renders_as_its_paragraph(self, tmp_path, line):
        pdf = _clause_pdf(tmp_path, line)
        assert [element.text for element in PDFReader(pdf).paragraphs if element.text == line] == [line]

        top_level, inlines = _markdown_blocks(PDFWriter(pdf).write_markdown(tmp_path / "out.md"))

        # The page's marker comment, the title, then three paragraphs: the line opened no block of its own.
        assert top_level == ["html_block", "heading_open", "paragraph_open", "paragraph_open", "paragraph_open"]
        assert inlines == [
            ("heading_open", "Quarterly Report"),
            ("paragraph_open", "This is body text for testing."),
            ("paragraph_open", line),
            ("paragraph_open", "Body line four wraps up the text."),
        ]

    def test_numbered_clause_is_an_ordered_list_item_whose_marker_is_kept(self, tmp_path):
        pdf = _clause_pdf(tmp_path, "2) The second clause applies.")
        reader = PDFReader(pdf)
        (found,) = reader.lists
        assert (found.items, found.ordered, found.markers) == (("The second clause applies.",), True, ("2)",))
        assert not [element for element in reader.paragraphs if "clause" in element.text]

        top_level, inlines = _markdown_blocks(PDFWriter(pdf).write_markdown(tmp_path / "out.md"))

        assert top_level == ["html_block", "heading_open", "paragraph_open", "ordered_list_open", "paragraph_open"]
        assert ("paragraph_open", "The second clause applies.") in inlines


# ---------------------------------------------------------------------------
# Existing escape_markdown_link_target behaviour that must continue to pass
# ---------------------------------------------------------------------------


class TestEscapeMarkdownLinkTargetExistingBehaviour:
    def test_backslash_escaped(self):
        assert escape_markdown_link_target("a\\b") == "a\\\\b"

    def test_close_paren_escaped(self):
        assert escape_markdown_link_target("a)b") == "a\\)b"

    def test_open_paren_escaped(self):
        assert escape_markdown_link_target("a(b") == "a\\(b"

    def test_open_bracket_escaped(self):
        assert escape_markdown_link_target("a[b") == "a\\[b"

    def test_no_whitespace_plain_url(self):
        assert escape_markdown_link_target("https://example.com") == "https://example.com"

    def test_non_string_returned_unchanged(self):
        assert escape_markdown_link_target(None) is None
        assert escape_markdown_link_target(42) == 42


# ---------------------------------------------------------------------------
# #221 — whitespace / newline handling
# ---------------------------------------------------------------------------


class TestEscapeMarkdownLinkTargetIssue221:
    """Whitespace and newlines in link destinations must be handled."""

    # --- space → angle-bracket wrap ---

    def test_space_triggers_angle_wrap(self):
        result = escape_markdown_link_target("/my path/file.pdf")
        assert result == "</my path/file.pdf>"

    def test_multiple_spaces_angle_wrap(self):
        result = escape_markdown_link_target("/a b c")
        assert result == "</a b c>"

    # --- tab → angle-bracket wrap ---

    def test_tab_triggers_angle_wrap(self):
        result = escape_markdown_link_target("/my\tpath/file.pdf")
        assert result == "</my\tpath/file.pdf>"

    # --- angle-bracket wrap: interior < and > are escaped ---

    def test_angle_wrap_escapes_inner_less_than(self):
        # The '>' after '1' is also inside the angle-bracket wrapper, so it is
        # escaped too. The space triggers angle-bracket form for the whole string.
        result = escape_markdown_link_target("/path <1>/file")
        assert result == "</path \\<1\\>/file>"

    def test_angle_wrap_escapes_inner_greater_than(self):
        result = escape_markdown_link_target("/path >1/file")
        assert result == "</path \\>1/file>"

    def test_angle_wrap_escapes_both_angle_chars(self):
        result = escape_markdown_link_target("/a <b> c")
        assert result == "</a \\<b\\> c>"

    def test_angle_wrap_escapes_backslash_inside(self):
        result = escape_markdown_link_target("/a\\b c")
        assert result == "</a\\\\b c>"

    # --- In the angle-bracket form, ( ) [ ] are literal — no escaping ---

    def test_angle_wrap_parens_not_escaped(self):
        result = escape_markdown_link_target("/a (b) c")
        assert result == "</a (b) c>"

    def test_angle_wrap_brackets_not_escaped(self):
        result = escape_markdown_link_target("/a [b] c")
        assert result == "</a [b] c>"

    # --- newline / carriage-return stripped ---

    def test_newline_stripped(self):
        result = escape_markdown_link_target("https://example.com\npath")
        # newline removed → "https://example.compath" → no spaces → bare form, unchanged
        assert result == "https://example.compath"
        assert "\n" not in result

    def test_carriage_return_stripped(self):
        # carriage return removed → "https://example.compath" → no spaces → bare form, unchanged
        assert escape_markdown_link_target("https://example.com\rpath") == "https://example.compath"

    def test_newline_stripped_whitespace_remains_wraps(self):
        # newline removed but a space still present → angle-bracket wrap of the rest, every character kept
        assert escape_markdown_link_target("/my path\nfile") == "</my pathfile>"

    def test_only_newline_no_space_bare_form(self):
        # After stripping newline, no whitespace remains → bare form, every other character kept
        assert escape_markdown_link_target("https://example.com\n/path") == "https://example.com/path"


class TestModule:
    """Only the link-destination escape is left: v5 escapes element text itself (elements.py)."""

    def test_defines_only_the_link_target_escape_and_its_helpers(self):
        functions = {
            name
            for name, value in vars(_markdown_escape).items()
            if callable(value) and getattr(value, "__module__", None) == _markdown_escape.__name__
        }

        assert functions == {"escape_markdown_link_target", "_contains_whitespace", "_bare_form", "_angle_bracket_form"}
