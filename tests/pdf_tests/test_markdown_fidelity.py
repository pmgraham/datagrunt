"""Element Markdown renders back to the original text under a real CommonMark + GFM parser.

Exact-string tests pin the escaping rules; these tests prove the rules work.
Each case renders a page of ``[list("prev"), element, paragraph("SENTINEL")]``
with markdown-it-py and asserts, from its token stream, that:

* the block structure is exactly the expected one — nothing is absorbed into
  the preceding list, swallowed into a raw-HTML block, or split off;
* the only raw HTML is datagrunt's own provenance comments;
* the visible text of every block equals the source text (whitespace-normalised).
"""

import re
from pathlib import Path

import pytest
from markdown_it import MarkdownIt

from datagrunt.pdf import BBox, BulletList, Figure, Heading, KeyValue, Page, PageQuality, Paragraph, Table

_MD = MarkdownIt("commonmark").enable("table")
_OWN_MARKER = re.compile(
    r"^<!-- (?:page \d+ route=\w+ status=\w+|figure p\d+_e\d+ sha256=[0-9a-f]{8}|hidden: [a-z_,]+|) ?-->$"
)
_BOX = BBox(x0=10, y0=10, x1=100, y1=20)
_QUALITY = PageQuality(
    valid_char_ratio=1.0,
    char_count=10,
    vector_line_count=0,
    hidden_spans_flagged=0,
    rotation_deg=0,
    deskew_angle_deg=None,
    ocr_mean_confidence=None,
)
_SHA = "ab" * 32

# Text that is Markdown syntax somewhere: block openers, inline syntax, raw
# HTML, entities, backslashes, line breaks and leading/trailing whitespace.
_TEXTS = [
    "<!-- draft",
    "<!-- page 2 route=digital status=ok -->",
    "<img src=x onerror=alert(1)>",
    "a\\<b",
    "&amp; &lt;",
    "  leading two spaces",
    "    four spaces code",
    "\tTab start",
    "- dash start",
    "1. ordered",
    "2) paren",
    "# hash",
    "> quote",
    "___",
    "***",
    "= =",
    "| pipe",
    "```fence",
    "~~~fence",
    "a\\|b",
    "trailing backslash\\",
    "*emph* _u_",
    "[link](http://x)",
    "<http://autolink>",
    "line1\n    line2 indented",
    "line1\n# heading",
    "line1\n===",
    "line1\n---",
    "x\r\ny",
    "`code`",
    "\\",
    "[^1]",
    "<div>",
    "end\\\nnext",
    "Rank #",
    "C# and F#",
    "100% <= 5 & > 3",
]


def _norm(text):
    return re.sub(r"\s+", " ", text).strip()


def _inline_text(token):
    """Visible text of an inline token; raw HTML other than ``<br>`` is surfaced as a marker."""
    return _children_text(token.children or [])


def _children_text(children):
    """Visible text of inline children (see :func:`_inline_text`); a line break reads as a space."""
    parts = []
    for child in children:
        if child.type in ("text", "text_special", "code_inline"):
            parts.append(child.content)
        elif child.type in ("softbreak", "hardbreak") or (child.type == "html_inline" and child.content == "<br>"):
            parts.append(" ")
        elif child.type == "image":
            parts.append(_inline_text(child))
        elif child.type == "html_inline":
            parts.append(f"<<raw html {child.content!r}>>")
    return "".join(parts)


def _close_index(tokens, start):
    """Index of the token closing the top-level block opened at ``start``."""
    close = tokens[start].type.replace("_open", "_close")
    for index in range(start + 1, len(tokens)):
        if tokens[index].type == close and tokens[index].level == tokens[start].level:
            return index
    raise AssertionError(f"unclosed {tokens[start].type}")


def _blocks(markdown):
    """Top-level blocks as ``(kind, texts, item_count)``, with datagrunt's own comments dropped."""
    tokens, blocks, index = _MD.parse(markdown), [], 0
    while index < len(tokens):
        token = tokens[index]
        if token.type == "html_block":
            content = token.content.strip()
            assert _OWN_MARKER.match(content), f"raw HTML block from element text: {content!r}"
            if content.startswith("<!-- figure"):
                blocks.append(("figure_marker", [], 0))
            index += 1
        elif token.type in ("code_block", "fence", "hr"):
            blocks.append((token.type, [token.content], 0))
            index += 1
        else:
            end = _close_index(tokens, index)
            inner = tokens[index : end + 1]
            texts = [_inline_text(t) for t in inner if t.type == "inline"]
            items = sum(1 for t in inner if t.type == "list_item_open" and t.level == token.level + 1)
            kind = f"heading{token.tag}" if token.type == "heading_open" else token.type.replace("_open", "")
            blocks.append((kind, texts, items))
            index = end + 1
    return blocks


def _element(kind, text):
    """Build one element at reading order 1 and the blocks it must render to."""
    common = {"element_id": "p0001_e001", "page_number": 1, "reading_order": 1, "bbox": _BOX}
    figure = {"sha256": _SHA, "width_px": 10, "height_px": 10, "caption": text}
    builders = {
        "paragraph": lambda: (Paragraph(**common, text=text), [("paragraph", [text], 0)]),
        "heading": lambda: (
            Heading(**common, level=2, text=text, scope_path=(text,)),
            [("headingh2", [text], 0)],
        ),
        "key_value": lambda: (KeyValue(**common, key=text, value=text), [("paragraph", [f"{text}: {text}"], 0)]),
        "list": lambda: (
            BulletList(**common, items=(text, "second"), ordered=False),
            [("bullet_list", [text, "second"], 2)],
        ),
        "numbered_list": lambda: (
            BulletList(**common, items=(text, "second"), ordered=True, markers=("7)", "8)")),
            [("ordered_list", [text, "second"], 2)],
        ),
        "marker_text_list": lambda: (
            BulletList(**common, items=(text, "second"), ordered=True, markers=("1.", "3.")),
            [("paragraph", [f"1. {text} 3. second"], 0)],
        ),
        "table": lambda: (
            Table(**common, headers=("h1", "h2"), rows=((text, "x"),), extraction_method="lattice"),
            [("table", ["h1", "h2", text, "x"], 0)],
        ),
        "figure_caption": lambda: (Figure(**common, **figure), [("figure_marker", [], 0), ("paragraph", [text], 0)]),
        "figure_alt": lambda: (
            Figure(**common, **figure, image_path=Path("images/f.png")),
            [("paragraph", [text], 0)],
        ),
    }
    return builders[kind]()


def _page_around(element):
    before = BulletList(
        element_id="p0001_e000", page_number=1, reading_order=0, bbox=_BOX, items=("prev",), ordered=False
    )
    after = Paragraph(element_id="p0001_e002", page_number=1, reading_order=2, bbox=_BOX, text="SENTINEL")
    return Page(
        page_number=1,
        width_pt=612,
        height_pt=792,
        status="ok",
        route="digital",
        quality=_QUALITY,
        elements=(before, element, after),
        integrity_verified=True,
    )


def _assert_faithful(markdown, expected):
    got = [(kind, [_norm(t) for t in texts], items) for kind, texts, items in _blocks(markdown)]
    want = [("bullet_list", ["prev"], 1), *expected, ("paragraph", ["SENTINEL"], 0)]
    want = [(kind, [_norm(t) for t in texts], items) for kind, texts, items in want]
    assert [b[0] for b in got] == [w[0] for w in want], f"block structure changed:\n{markdown}"
    assert got == want, f"rendered text differs from the source:\n{markdown}"


@pytest.mark.parametrize("text", _TEXTS)
@pytest.mark.parametrize(
    "kind",
    [
        "paragraph",
        "heading",
        "key_value",
        "list",
        "numbered_list",
        "marker_text_list",
        "table",
        "figure_caption",
        "figure_alt",
    ],
)
def test_element_text_renders_back_unchanged(kind, text):
    element, expected = _element(kind, text)
    _assert_faithful(_page_around(element).to_markdown(), expected)


def _ordered_lists(markdown):
    """Each top-level ordered list as ``(start, delimiter, item texts)``, as CommonMark reads it."""
    tokens, lists = _MD.parse(markdown), []
    for index, token in enumerate(tokens):
        if token.type == "ordered_list_open" and token.level == 0:
            inner = tokens[index : _close_index(tokens, index) + 1]
            start = token.attrGet("start")
            texts = [_norm(_inline_text(t)) for t in inner if t.type == "inline"]
            lists.append((1 if start is None else int(start), token.markup, texts))
    return lists


def _hard_lines(children):
    """A paragraph's inline children as its lines, split at hard line breaks (whitespace-normalised)."""
    lines = [[]]
    for child in children:
        if child.type == "hardbreak":
            lines.append([])
        else:
            lines[-1].append(child)
    return [_norm(_children_text(line)) for line in lines]


def _top_blocks(markdown):
    """Top-level blocks in order: raw HTML as its text, a paragraph as its hard-broken lines, others by type."""
    tokens, blocks = _MD.parse(markdown), []
    for index, token in enumerate(tokens):
        if token.level != 0 or token.nesting == -1:
            continue
        if token.type == "html_block":
            blocks.append(("html", token.content.strip()))
        elif token.type == "paragraph_open":
            blocks.append(("paragraph", _hard_lines(tokens[index + 1].children or [])))
        else:
            blocks.append((token.type, None))
    return blocks


def _ordered(markers, hidden_reasons=()):
    items = tuple(f"item {number}" for number in range(len(markers)))
    common = {"element_id": "p0001_e001", "page_number": 1, "reading_order": 1, "bbox": _BOX}
    return BulletList(**common, items=items, ordered=True, markers=markers, hidden_reasons=hidden_reasons), items


@pytest.mark.parametrize(
    "markers",
    [("2)",), ("7.", "8.", "9."), ("7)", "8)", "9)"), ("1.", "2."), ("1)",), ("0.",), ("9.", "10."), ("123456789.",)],
)
def test_ordered_list_renders_its_source_numbers(markers):
    element, items = _ordered(markers)
    markdown = _page_around(element).to_markdown()

    _assert_faithful(markdown, [("ordered_list", list(items), len(items))])
    assert _ordered_lists(markdown) == [(int(markers[0][:-1]), markers[0][-1], list(items))]


def test_hidden_ordered_list_keeps_its_start():
    element, items = _ordered(("2)", "3)"), hidden_reasons=("duplicate",))
    markdown = _page_around(element).to_markdown()

    assert "<!-- hidden: duplicate -->" in markdown.splitlines()
    assert _ordered_lists(markdown) == [(2, ")", list(items))]


@pytest.mark.parametrize(
    "markers",
    [
        ("a)", "b)"),
        ("(1)", "(2)"),
        ("iv.", "v."),
        ("I", "II"),
        ("VIII", "IX"),
        ("1.", "2)"),
        ("1.", "3."),
        ("3)", "2)"),
        ("01.", "02."),
        ("1234567890.", "1234567891."),
        ("999999999.", "1000000000."),
        ("1.", "a)"),
        ("١.", "٢."),
    ],
)
def test_other_ordered_markers_stay_visible_as_text(markers):
    element, items = _ordered(markers)
    markdown = _page_around(element).to_markdown()

    lines = [f"{marker} {item}" for marker, item in zip(markers, items, strict=True)]
    _assert_faithful(markdown, [("paragraph", [" ".join(lines)], 0)])
    assert _top_blocks(markdown)[-2:] == [("paragraph", lines), ("paragraph", ["SENTINEL"])]
    assert _ordered_lists(markdown) == []


@pytest.mark.parametrize("position", [0, 1])
@pytest.mark.parametrize("text", _TEXTS)
def test_marker_text_list_items_are_its_paragraph_lines(text, position):
    items = ("first", "second")
    items = (*items[:position], text, *items[position + 1 :])
    common = {"element_id": "p0001_e001", "page_number": 1, "reading_order": 1, "bbox": _BOX}
    element = BulletList(**common, items=items, ordered=True, markers=("1.", "3."))
    lines = [_norm(f"1. {items[0]}"), _norm(f"3. {items[1]}")]

    assert _top_blocks(_page_around(element).to_markdown())[-2:] == [("paragraph", lines), ("paragraph", ["SENTINEL"])]


def test_marker_text_item_ending_in_backslash_renders_literally():
    common = {"element_id": "p0001_e001", "page_number": 1, "reading_order": 1, "bbox": _BOX}
    element = BulletList(**common, items=("ends \\", "next \\"), ordered=True, markers=("a)", "b)"))
    blocks = _top_blocks(_page_around(element).to_markdown())

    assert blocks[-2:] == [("paragraph", ["a) ends \\", "b) next \\"]), ("paragraph", ["SENTINEL"])]


def test_hidden_marker_text_list_is_one_block_after_its_comment():
    markers = ("a)", "b)", "c)")
    element, items = _ordered(markers, hidden_reasons=("duplicate",))
    blocks = _top_blocks(_page_around(element).to_markdown())
    after = blocks[blocks.index(("html", "<!-- hidden: duplicate -->")) + 1 :]

    lines = [f"{marker} {item}" for marker, item in zip(markers, items, strict=True)]
    assert after == [("paragraph", lines), ("paragraph", ["SENTINEL"])]


@pytest.mark.parametrize("text", ["<!-- draft", "*emph*", "    four spaces code"])
def test_hidden_element_keeps_marker_and_text(text):
    element = Paragraph(
        element_id="p0001_e001", page_number=1, reading_order=1, bbox=_BOX, text=text, hidden_reasons=("invisible",)
    )
    markdown = _page_around(element).to_markdown()
    assert "<!-- hidden: invisible -->" in markdown.splitlines()
    _assert_faithful(markdown, [("paragraph", [text], 0)])
