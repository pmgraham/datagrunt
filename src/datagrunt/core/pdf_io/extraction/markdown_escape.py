"""Escaping helpers for rendering extracted text into Markdown safely."""

import re

# A leading markdown metacharacter or structural token that would cause a
# CommonMark renderer to reinterpret verbatim body text as document structure.
#
# Captured groups (by index in the match):
#   1 — leading whitespace (preserved verbatim)
#   2 — single block-structural char: # > * + - = | _, or a '<' that opens an
#       HTML block (followed by a letter, '/', '!' or '?'; "< 5" is untouched)
#   3 — digits of an ordered-list marker (e.g. "1", "42")
#   4 — delimiter of an ordered-list marker ("." or ")")
#   5 — fence opener: 3+ backticks or 3+ tildes
#
# Only the *first* structural character is escaped; the rest of the line is
# left untouched, which is the minimum intervention needed for safe rendering.
_LEADING_MARKDOWN_METACHAR = re.compile(
    r"^(\s*)(?:"
    r"([#>*+=|_]|[-]|<(?=[A-Za-z/!?]))"  # group 2 — single structural char
    r"|(\d+)([.)])"  # groups 3+4 — ordered-list marker "N." or "N)"
    r"|(```+|~~~+)"  # group 5 — code-fence opener (3+ backticks or tildes)
    r")"
)

# CommonMark: 4+ columns of indentation open an indented code block.
_CODE_INDENT_COLUMNS = 4


def escape_leading_markdown(text: str) -> str:
    """Backslash-escape a leading markdown metacharacter in body/caption text.

    Prevents extracted text such as ``# rm -rf is not a heading`` from being
    reinterpreted as markdown structure (an H1, list item, blockquote, setext
    underline, table row, thematic break, code fence, indented code block, or
    raw HTML block).

    Only the leading structural character is escaped; the rest of the text is
    left untouched, so the escaped text renders back to the input text.
    Non-string input is returned unchanged.

    Extended in #220 to cover:
    - ``=`` (setext H1/H2 underline when repeated)
    - ``|`` (table row opener)
    - `` ``` `` / ``~~~`` (fenced code block — only 3+ chars trigger a fence)

    Extended in #376 to cover:
    - ``N)`` (ordered-list marker with a parenthesis) → ``N\\)``
    - ``_`` (``___`` / ``_ _ _`` thematic break, ``_x_`` emphasis) → ``\\_``
    - 4+ columns of indentation on the text's first line (indented code
      block; a tab advances to the next multiple of 4): the first indentation
      char becomes its character reference (``&#32;`` / ``&#9;``), so the line
      is no longer indented and no later token on it is structural
    - ``<`` followed by a letter, ``/``, ``!`` or ``?`` (HTML block) → ``\\<``
    """
    if not isinstance(text, str):
        return text
    unindented = _escape_code_indent(text)
    if unindented is not None:
        return unindented

    def _escape(match: re.Match) -> str:
        indent, block_char, digits, delimiter, fence = match.groups()
        if block_char is not None:
            return f"{indent}\\{block_char}"
        if fence is not None:
            # Escape only the first character of the fence opener so the
            # remaining backticks/tildes no longer form a 3-char run.
            return f"{indent}\\{fence}"
        return f"{indent}{digits}\\{delimiter}"

    return _LEADING_MARKDOWN_METACHAR.sub(_escape, text, count=1)


def _escape_code_indent(text: str) -> str | None:
    """Neutralise an indented-code-block start; ``None`` if *text* is not one.

    Indentation is measured on the line the text starts on — after the last
    line break in the leading whitespace, since the text may open with blank
    lines. Whitespace-only text is a blank line, never code, so it is left as is.
    """
    lead_length = len(text) - len(text.lstrip())
    if lead_length == len(text):
        return None
    lead = text[:lead_length]
    line_start = max(lead.rfind("\n"), lead.rfind("\r")) + 1
    if _indent_columns(lead[line_start:]) < _CODE_INDENT_COLUMNS:
        return None
    # Only a space or tab can reach 4 columns, so this is "&#32;" or "&#9;".
    return f"{text[:line_start]}&#{ord(text[line_start])};{text[line_start + 1 :]}"


def _indent_columns(whitespace: str) -> int:
    """CommonMark indentation width of *whitespace*'s leading spaces and tabs."""
    columns = 0
    for char in whitespace:
        if char == "\t":
            columns += _CODE_INDENT_COLUMNS - columns % _CODE_INDENT_COLUMNS
        elif char == " ":
            columns += 1
        else:
            break
    return columns


def escape_markdown_link_text(text: str) -> str:
    """Escape ``[`` / ``]`` / ``\\`` in markdown link *label* text (image alt)."""
    if not isinstance(text, str):
        return text
    return text.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def escape_markdown_link_target(target: str) -> str:
    """Escape a markdown link *destination* (URL or file path) for safe rendering.

    Two forms are produced depending on whether the target contains whitespace,
    following CommonMark § 6.3 link-destination rules:

    Bare form (no whitespace after stripping):
        Escape ``\\ ) ( [`` with backslashes, unchanged from original behaviour.
        Example: ``/path/(file)`` → ``/path/\\(file\\)``

    Angle-bracket form (space or tab present):
        Wrap the entire destination in ``<...>``.  Inside that wrapper only
        ``\\ < >`` need escaping; ``( ) [ ]`` are ordinary characters.
        Example: ``/my path/file`` → ``</my path/file>``

    Newlines (``\\n`` / ``\\r``) are invalid in any CommonMark link destination
    and are stripped before the whitespace test is applied.  If stripping them
    leaves no whitespace, the bare form is used; if whitespace remains, the
    angle-bracket form is used.

    This approach (angle-bracket wrapping) preserves the literal path characters
    and is the CommonMark-recommended way to embed spaces in a link destination.
    """
    if not isinstance(target, str):
        return target

    # Strip characters that are unconditionally invalid in any link destination.
    sanitized = target.replace("\n", "").replace("\r", "")

    if _contains_whitespace(sanitized):
        return _angle_bracket_form(sanitized)

    return _bare_form(sanitized)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _contains_whitespace(text: str) -> bool:
    """Return True if *text* contains a space or tab."""
    return " " in text or "\t" in text


def _bare_form(target: str) -> str:
    """Escape structural chars for a bare (non-angle-bracket) link destination."""
    return target.replace("\\", "\\\\").replace(")", "\\)").replace("(", "\\(").replace("[", "\\[")


def _angle_bracket_form(target: str) -> str:
    """Wrap *target* in ``<...>`` and escape interior ``\\ < >``."""
    # Inside an angle-bracket destination only \, <, > are special.
    interior = target.replace("\\", "\\\\").replace("<", "\\<").replace(">", "\\>")
    return f"<{interior}>"
