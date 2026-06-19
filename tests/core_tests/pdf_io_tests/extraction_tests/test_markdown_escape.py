"""Tests for markdown_escape helpers — issues #220 and #221.

#220: escape_leading_markdown must escape leading '=', '|', and code fences
      (``` / ~~~).
#221: escape_markdown_link_target must handle whitespace and newlines
      (angle-bracket wrapping for space/tab; stripping for newlines).
"""

from datagrunt.core.pdf_io.extraction.markdown_escape import (
    escape_leading_markdown,
    escape_markdown_link_target,
)

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
        result = escape_markdown_link_target("https://example.com\rpath")
        assert "\r" not in result

    def test_newline_stripped_whitespace_remains_wraps(self):
        # newline removed but a space still present → angle-bracket wrap
        result = escape_markdown_link_target("/my path\nfile")
        assert result.startswith("<")
        assert result.endswith(">")
        assert "\n" not in result

    def test_only_newline_no_space_bare_form(self):
        # After stripping newline, no whitespace remains → bare form
        result = escape_markdown_link_target("https://example.com\n/path")
        assert "\n" not in result
        assert not result.startswith("<")
