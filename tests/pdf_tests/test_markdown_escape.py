"""Tests for markdown_escape helpers — issues #220, #221 and #376.

#220: escape_leading_markdown must escape leading '=', '|', and code fences
      (``` / ~~~).
#221: escape_markdown_link_target must handle whitespace and newlines
      (angle-bracket wrapping for space/tab; stripping for newlines).
#376: escape_leading_markdown must also neutralise 'N)' list markers, '_'
      thematic breaks / emphasis, 4+ column indentation (indented code) and
      HTML block openers — and the escaped text must render back to itself.
"""

import pytest
from markdown_it import MarkdownIt

from datagrunt.pdf._markdown_escape import (
    escape_leading_markdown,
    escape_markdown_link_target,
)
from datagrunt.pdf.reader import PDFReader
from datagrunt.pdf.writer import PDFWriter
from pdf_tests.builders import PageSpec, TextRun, build_pdf

_COMMONMARK = MarkdownIt("commonmark")


def _rendered_paragraph_text(markdown):
    """Visible text of the single plain paragraph *markdown* renders to.

    Fails if the Markdown renders as anything else: a list, rule, code block,
    raw HTML block, or a paragraph with inline structure (emphasis, HTML).
    """
    tokens = _COMMONMARK.parse(markdown)
    assert [token.type for token in tokens] == ["paragraph_open", "inline", "paragraph_close"], markdown
    children = tokens[1].children or []
    assert [child.type for child in children] == ["text"], markdown
    return children[0].content


# ---------------------------------------------------------------------------
# Existing behaviour that must continue to pass
# ---------------------------------------------------------------------------


class TestEscapeLeadingMarkdownExistingBehaviour:
    def test_heading_escaped(self):
        assert escape_leading_markdown("# Heading") == "\\# Heading"

    def test_blockquote_escaped(self):
        assert escape_leading_markdown("> quote") == "\\> quote"

    def test_bullet_star_escaped(self):
        assert escape_leading_markdown("* item") == "\\* item"

    def test_bullet_plus_escaped(self):
        assert escape_leading_markdown("+ item") == "\\+ item"

    def test_bullet_dash_escaped(self):
        assert escape_leading_markdown("- item") == "\\- item"

    def test_ordered_list_dot_escaped(self):
        assert escape_leading_markdown("1. item") == "1\\. item"

    def test_ordered_list_large_number(self):
        assert escape_leading_markdown("42. foo") == "42\\. foo"

    def test_plain_text_untouched(self):
        assert escape_leading_markdown("just plain text") == "just plain text"

    def test_non_string_returned_unchanged(self):
        assert escape_leading_markdown(None) is None
        assert escape_leading_markdown(42) == 42

    def test_leading_indent_preserved(self):
        assert escape_leading_markdown("   # indented") == "   \\# indented"


# ---------------------------------------------------------------------------
# #220 — new cases: '=', '|', code fences
# ---------------------------------------------------------------------------


class TestEscapeLeadingMarkdownIssue220:
    """Leading '=', '|', and fence markers must be backslash-escaped."""

    # --- setext H1/H2 underline ---

    def test_equals_sign_escaped(self):
        assert escape_leading_markdown("=== underline") == "\\=== underline"

    def test_single_equals_escaped(self):
        assert escape_leading_markdown("= foo") == "\\= foo"

    def test_equals_with_leading_whitespace(self):
        assert escape_leading_markdown("  === underline") == "  \\=== underline"

    # --- table row ---

    def test_pipe_escaped(self):
        assert escape_leading_markdown("| a | b |") == "\\| a | b |"

    def test_pipe_with_leading_whitespace(self):
        assert escape_leading_markdown("  | col |") == "  \\| col |"

    # --- backtick code fence ---

    def test_backtick_fence_three_escaped(self):
        result = escape_leading_markdown("```python")
        assert result == "\\```python"

    def test_backtick_fence_four_escaped(self):
        result = escape_leading_markdown("````")
        assert result == "\\````"

    def test_backtick_fence_with_leading_whitespace(self):
        result = escape_leading_markdown("  ```py")
        assert result == "  \\```py"

    # --- tilde code fence ---

    def test_tilde_fence_three_escaped(self):
        result = escape_leading_markdown("~~~")
        assert result == "\\~~~"

    def test_tilde_fence_four_escaped(self):
        result = escape_leading_markdown("~~~~sh")
        assert result == "\\~~~~sh"

    def test_tilde_fence_with_leading_whitespace(self):
        result = escape_leading_markdown("  ~~~")
        assert result == "  \\~~~"

    # --- one/two backticks are NOT fences; must not be escaped ---

    def test_single_backtick_not_escaped(self):
        # A single backtick is inline code, not a fence — outside scope of #220.
        # The contract only escapes fences (3+). Single backtick should be left as-is.
        result = escape_leading_markdown("`code`")
        assert result == "`code`"

    def test_two_backticks_not_escaped(self):
        result = escape_leading_markdown("``code``")
        assert result == "``code``"


# ---------------------------------------------------------------------------
# #376 — 'N)' markers, '_' runs, indented code, HTML block openers
# ---------------------------------------------------------------------------


class TestEscapeLeadingMarkdownIssue376:
    """Each escape is exact AND renders back to the input as one plain paragraph."""

    # --- row 1: ordered-list marker with ')' ---

    @pytest.mark.parametrize(("text", "expected"), [("2) foo", "2\\) foo"), ("42) foo", "42\\) foo")])
    def test_paren_ordered_list_marker_escaped(self, text, expected):
        escaped = escape_leading_markdown(text)
        assert escaped == expected
        assert _rendered_paragraph_text(escaped) == text

    def test_dash_before_paren_marker_escapes_only_the_dash(self):
        escaped = escape_leading_markdown("- 2) foo")
        assert escaped == "\\- 2) foo"
        assert _rendered_paragraph_text(escaped) == "- 2) foo"

    # --- row 2: '_' thematic break / emphasis ---

    @pytest.mark.parametrize(
        ("text", "expected"),
        [("___", "\\___"), ("_ _ _", "\\_ _ _"), ("_x_", "\\_x_")],
    )
    def test_leading_underscore_escaped(self, text, expected):
        escaped = escape_leading_markdown(text)
        assert escaped == expected
        assert _rendered_paragraph_text(escaped) == text

    # --- row 3: 4+ columns of indentation (indented code block) ---

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("    four spaces code", "&#32;   four spaces code"),
            ("\tTab start", "&#9;Tab start"),
            # Space, space, then a tab that advances to column 4.
            ("  \tx", "&#32; \tx"),
            # The line is no longer indented, so the later '#' stays verbatim.
            ("    # not code", "&#32;   # not code"),
        ],
    )
    def test_code_indentation_first_char_becomes_char_reference(self, text, expected):
        escaped = escape_leading_markdown(text)
        assert escaped == expected
        assert _rendered_paragraph_text(escaped) == text

    @pytest.mark.parametrize(
        ("text", "expected"),
        [("\n    code", "\n&#32;   code"), ("\r    code", "\r&#32;   code"), ("\r\n\tcode", "\r\n&#9;code")],
    )
    def test_indentation_measured_after_last_line_break(self, text, expected):
        escaped = escape_leading_markdown(text)
        assert escaped == expected
        assert _rendered_paragraph_text(escaped) == text.lstrip("\r\n")

    def test_blank_lines_then_shallow_indent_not_escaped(self):
        # Four leading whitespace chars in all, but only 2 columns on the text's line.
        assert escape_leading_markdown("\n\n  x") == "\n\n  x"
        assert _rendered_paragraph_text("\n\n  x") == "x"

    def test_three_space_indent_not_escaped(self):
        assert escape_leading_markdown("   x") == "   x"
        assert _rendered_paragraph_text("   x") == "x"

    @pytest.mark.parametrize("text", ["    ", "\n    ", "\t"])
    def test_whitespace_only_text_not_escaped(self, text):
        # A whitespace-only line is blank, never an indented code block.
        assert escape_leading_markdown(text) == text

    # --- row 4: HTML block openers ---

    @pytest.mark.parametrize(
        "text",
        ["<div>", "</p>", "<!-- note", "<?php echo 1;", "<!DOCTYPE html>", "<script>"],
    )
    def test_html_block_opener_escaped(self, text):
        escaped = escape_leading_markdown(text)
        assert escaped == "\\" + text
        assert _rendered_paragraph_text(escaped) == text

    @pytest.mark.parametrize("text", ["< 5 apples", "<3 hearts"])
    def test_less_than_not_opening_html_untouched(self, text):
        assert escape_leading_markdown(text) == text
        assert _rendered_paragraph_text(text) == text


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
