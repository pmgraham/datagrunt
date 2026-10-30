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
    parts = []
    for child in token.children or []:
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
@pytest.mark.parametrize("kind", ["paragraph", "heading", "key_value", "list", "table", "figure_caption", "figure_alt"])
def test_element_text_renders_back_unchanged(kind, text):
    element, expected = _element(kind, text)
    _assert_faithful(_page_around(element).to_markdown(), expected)


@pytest.mark.parametrize("text", ["<!-- draft", "*emph*", "    four spaces code"])
def test_hidden_element_keeps_marker_and_text(text):
    element = Paragraph(
        element_id="p0001_e001", page_number=1, reading_order=1, bbox=_BOX, text=text, hidden_reasons=("invisible",)
    )
    markdown = _page_around(element).to_markdown()
    assert "<!-- hidden: invisible -->" in markdown.splitlines()
    _assert_faithful(markdown, [("paragraph", [text], 0)])
