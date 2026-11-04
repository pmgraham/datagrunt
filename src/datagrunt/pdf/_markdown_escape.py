"""Escaping for a Markdown link destination: a figure's image path in ``Figure.to_markdown``.

Element text is escaped by ``elements.py`` itself. v4 also escaped leading Markdown syntax and link
labels here; the 5.0 flip (#373) removed those helpers with v4.
"""


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
