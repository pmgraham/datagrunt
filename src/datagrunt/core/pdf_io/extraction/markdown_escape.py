"""Escaping helpers for rendering extracted text into Markdown safely."""

import re

# A leading markdown metacharacter ('#', '>', '*', '+', '-') or an ordered-list
# marker ('1.') turns verbatim body text into markdown structure. Match the
# leading whitespace plus either a block metacharacter or the dot of an
# ordered-list marker so the structural character can be backslash-escaped.
_LEADING_MARKDOWN_METACHAR = re.compile(r"^(\s*)(?:([#>*+-])|(\d+)(\.))")


def escape_leading_markdown(text: str) -> str:
    """Backslash-escape a leading markdown metacharacter in body/caption text.

    Prevents extracted text such as ``# rm -rf is not a heading`` from being
    reinterpreted as markdown structure (an H1, list item, or blockquote).
    Only the leading structural character is escaped; the rest of the text is
    left untouched. Non-string input is returned unchanged.
    """
    if not isinstance(text, str):
        return text

    def _escape(match: re.Match) -> str:
        indent, block_char, digits, dot = match.groups()
        if block_char is not None:
            return f"{indent}\\{block_char}"
        return f"{indent}{digits}\\{dot}"

    return _LEADING_MARKDOWN_METACHAR.sub(_escape, text, count=1)


def escape_markdown_link_text(text: str) -> str:
    """Escape ``[`` / ``]`` / ``\\`` in markdown link *label* text (image alt)."""
    if not isinstance(text, str):
        return text
    return text.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def escape_markdown_link_target(target: str) -> str:
    """Escape ``(`` / ``)`` / ``[`` / ``\\`` in markdown link *destination* URLs/paths."""
    if not isinstance(target, str):
        return target
    return (
        target.replace("\\", "\\\\")
        .replace(")", "\\)")
        .replace("(", "\\(")
        .replace("[", "\\[")
    )
