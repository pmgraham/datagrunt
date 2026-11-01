"""Inline images and name tokens in PDF bytes, read by a small lexer.

The render guard and the pdfium engine's JBIG2 agreement read each content
stream's inline images (``BI`` ... ``ID`` data ``EI``) here, and count name
tokens such as ``/JBIG2Globals`` in raw bytes. The lexer skips literal
strings (nested parentheses, backslash escapes), hex strings and comments;
tokens are separated by PDF whitespace (NUL, tab, line feed, form feed,
carriage return, space) and the delimiters ``()<>[]{}/%``. An inline
image's dictionary is read token by token up to ``ID``, with no byte window:
the stream's length is the bound.

It fails closed: anything it cannot read for certain is a reason (a str)
rather than a result, and the caller blocks the render or decodes no JBIG2.

Pure: no pdfium / pymupdf import. Every loop advances through the data or is
bounded by a nesting depth.
"""

import re
from typing import NamedTuple

from datagrunt.core.pdf_io.extraction.image_guard import _MAX_FILTER_ARRAY_ITEMS, unescape_pdf_names

_WHITESPACE = b"\x00\t\n\x0c\r "
_DELIMITERS = b"()<>[]{}/%"
_STOPS = frozenset(_WHITESPACE + _DELIMITERS)
# Nesting of arrays and dictionaries inside one inline image's dictionary, and
# the entries of each (an array's elements; a dictionary's keys and values each count).
_MAX_DEPTH = 32
_MAX_ENTRIES = _MAX_FILTER_ARRAY_ITEMS
_TOO_MANY = f"has an array or dictionary of more than {_MAX_ENTRIES} entries"
# A whitespace byte, EI, then whitespace, a delimiter or the end: where inline data ends.
_EI_TOKEN = re.compile(rb"[\x00\t\n\x0c\r ]EI(?=[\x00\t\n\x0c\r ()<>\[\]{}/%]|$)")
_LINE_END = re.compile(rb"[\r\n]")
_FILTER_KEYS = frozenset({"F", "Filter"})
_PARMS_KEYS = frozenset({"DP", "DecodeParms"})
_GLOBALS = "JBIG2Globals"


class InlineImage(NamedTuple):
    """One inline image: its filter names (#-unescaped once, in order), a /JBIG2Globals in its parameters, its data.

    ``data`` runs from the byte after ``ID``'s whitespace to the whitespace
    before ``EI``; ``delimited`` is False when no ``EI`` follows (the data is
    then the rest of the stream). ``dictionary`` is the bytes between ``BI``
    and ``ID``, as written.
    """

    filters: tuple[str, ...]
    has_globals: bool
    data: bytes
    delimited: bool
    dictionary: bytes = b""


class _Token(NamedTuple):
    kind: str  # "name", "word", "string", "<<", ">>", "[", "]", "{", "}", "end", "error"
    value: str
    end: int


def inline_images(content: bytes) -> tuple[InlineImage, ...] | str:
    """Every inline image in ``content``, in order; or why the stream cannot be read for certain.

    The reason completes "an inline image ...": no ``ID`` before the stream
    ends, a filter or decode-parameter key given twice (or both of its
    names), a filter value that is neither a name nor an array of names, a
    dictionary that cannot be read, or a string, hex string or nesting that
    does not end.
    """
    images: list[InlineImage] = []
    position = 0
    while True:  # each step consumes at least one byte, or ends
        token = _next_token(content, position)
        if token.kind == "end":
            return tuple(images)
        if token.kind == "error":
            return token.value
        position = token.end
        if token.kind == "word" and token.value == "BI":
            read = _inline_image(content, position)
            if isinstance(read, str):
                return read
            image, position = read
            images.append(image)
            if not image.delimited:
                return tuple(images)


def _inline_image(content: bytes, position: int) -> tuple[InlineImage, int] | str:
    """The inline image whose dictionary starts at ``position`` (after ``BI``), and where scanning resumes."""
    start = position
    entries: dict[str, list[str] | bool] = {}
    for _ in range(_MAX_ENTRIES + 1):  # each step consumes a key and its value, or ends
        key = _next_token(content, position)
        if key.kind in ("end", "error"):
            return "has no ID before its content stream ends" if key.kind == "end" else key.value
        if key.kind == "word" and key.value == "ID":
            return _image_data(content, key.end, entries, content[start : key.end - 2])
        if key.kind != "name":
            return f"has a dictionary that cannot be read (a {key.kind} where a key belongs)"
        value = _value(content, key.end, 0)
        if isinstance(value, str):
            return value
        reason = _add_entry(entries, key.value, value)
        if reason is not None:
            return reason
        position = value.end
    return _TOO_MANY


def _add_entry(entries: dict[str, list[str] | bool], key: str, value: "_Value") -> str | None:
    """Record a filter or decode-parameter entry; why the dictionary cannot be read, or None."""
    if key in _FILTER_KEYS:
        if "filters" in entries:
            return "names its filter twice"
        if value.names is None:
            return "has a filter that is neither a name nor an array of names"
        entries["filters"] = value.names
    elif key in _PARMS_KEYS:
        if "globals" in entries:
            return "names its decode parameters twice"
        entries["globals"] = value.has_globals
    return None


def _image_data(
    content: bytes, id_end: int, entries: dict[str, list[str] | bool], dictionary: bytes
) -> tuple[InlineImage, int] | str:
    """The image whose ``ID`` ends at ``id_end``: its data up to ``EI``, and where scanning resumes.

    ``EI`` must be followed by whitespace or the end of the stream: one
    followed by a delimiter may end the data for one reader and not another.
    """
    start = id_end + 1 if id_end < len(content) and content[id_end] in _WHITESPACE else id_end
    ei_match = _EI_TOKEN.search(content, start)
    filters = entries.get("filters", [])
    has_globals = bool(entries.get("globals", False))
    names = tuple(filters) if isinstance(filters, list) else ()
    if ei_match is None:
        return InlineImage(names, has_globals, content[start:], False, dictionary), len(content)
    if ei_match.end() < len(content) and content[ei_match.end()] not in _WHITESPACE:
        return "has an EI followed by a delimiter, not whitespace"
    return InlineImage(names, has_globals, content[start : ei_match.start()], True, dictionary), ei_match.end()


class _Value(NamedTuple):
    """A dictionary value: its names when it is a name or an array of names (else None), any /JBIG2Globals in it."""

    names: list[str] | None
    has_globals: bool
    end: int


def _value(content: bytes, position: int, depth: int) -> _Value | str:
    """The value starting at ``position``; or why it cannot be read."""
    token = _next_token(content, position)
    if token.kind == "name":
        return _Value([token.value], token.value == _GLOBALS, token.end)
    if token.kind in ("word", "string"):
        return _Value(None, False, token.end)
    if token.kind in ("[", "<<"):
        if depth >= _MAX_DEPTH:
            return f"has a dictionary nested past {_MAX_DEPTH} levels"
        return _container(content, token, depth + 1)
    return token.value if token.kind == "error" else f"has a dictionary that cannot be read (a {token.kind})"


def _container(content: bytes, opening: _Token, depth: int) -> _Value | str:
    """The array or dictionary that ``opening`` starts; names only when an array of names."""
    closing = "]" if opening.kind == "[" else ">>"
    names: list[str] | None = [] if opening.kind == "[" else None
    has_globals = False
    position = opening.end
    for _ in range(_MAX_ENTRIES + 1):  # each step consumes one element, or ends
        token = _next_token(content, position)
        if token.kind == closing:
            return _Value(names, has_globals, token.end)
        if token.kind in ("end", "error"):
            return "has a dictionary that does not end" if token.kind == "end" else token.value
        element = _value(content, position, depth)
        if isinstance(element, str):
            return element
        has_globals = has_globals or element.has_globals
        if names is not None and element.names is not None and token.kind == "name":
            names.extend(element.names)
        else:
            names = None
        position = element.end
    return _TOO_MANY


def _next_token(content: bytes, position: int) -> _Token:
    """The token at or after ``position``, past whitespace and comments."""
    position = _skip_space(content, position)
    if position >= len(content):
        return _Token("end", "", position)
    byte = content[position]
    if byte == 0x28:  # (
        return _literal_string(content, position)
    if content.startswith(b"<<", position) or content.startswith(b">>", position):
        return _Token(content[position : position + 2].decode(), "", position + 2)
    if byte == 0x3C:  # <
        end = content.find(b">", position)
        return _Token("string", "", end + 1) if end >= 0 else _Token("error", "has a hex string that does not end", 0)
    if byte in b"[]{}>)":
        return _Token(chr(byte), "", position + 1)
    end = _regular_end(content, position + 1 if byte == 0x2F else position)
    if byte == 0x2F:  # /
        return _Token("name", unescape_pdf_names(content[position + 1 : end].decode("latin-1")), end)
    return _Token("word", content[position:end].decode("latin-1"), end)


def _skip_space(content: bytes, position: int) -> int:
    """The first position at or after ``position`` that is not whitespace or a comment."""
    while position < len(content):
        if content[position] in _WHITESPACE:
            position += 1
        elif content[position] == 0x25:  # % runs to the end of the line
            line_end = _LINE_END.search(content, position)
            position = line_end.end() if line_end else len(content)
        else:
            break
    return position


def _regular_end(content: bytes, position: int) -> int:
    """Where the regular characters starting at ``position`` end."""
    while position < len(content) and content[position] not in _STOPS:
        position += 1
    return position


def _literal_string(content: bytes, position: int) -> _Token:
    """The literal string starting at ``position`` (its ``(``): balanced parentheses, backslash escapes."""
    depth = 0
    index = position
    while index < len(content):
        byte = content[index]
        if byte == 0x5C:  # \ escapes the next byte
            index += 2
            continue
        if byte == 0x28:
            depth += 1
        elif byte == 0x29:
            depth -= 1
            if depth == 0:
                return _Token("string", "", index + 1)
        index += 1
    return _Token("error", "has a string that does not end", 0)


def _name_pattern(name: str) -> re.Pattern[bytes]:
    """A name token ``/name``, each character literal or #-escaped (either hex case)."""
    parts = []
    for char in name:
        code = f"{ord(char):02x}"
        cases = "".join(f"[{c.lower()}{c.upper()}]" if c.isalpha() else c for c in code)
        parts.append(f"(?:{re.escape(char)}|#{cases})")
    return re.compile(("/" + "".join(parts)).encode())


GLOBALS_TOKEN = _name_pattern(_GLOBALS)
JBIG2_DECODE_TOKEN = _name_pattern("JBIG2Decode")
JPX_DECODE_TOKEN = _name_pattern("JPXDecode")


def count_tokens(pattern: re.Pattern[bytes], data: bytes) -> int:
    """How many times ``pattern`` (see ``_name_pattern``) occurs in ``data``: in any context, strings included."""
    return sum(1 for _ in pattern.finditer(data))
