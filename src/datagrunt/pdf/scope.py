"""Cross-page scope for the v5 PDF document model (#370, A6: spec §3.3 step 5).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads A1 :class:`~datagrunt.pdf.elements.Page` values only and calls no
pdfium function. A1 types are frozen, so changed elements and pages are
rebuilt with :func:`dataclasses.replace`; element ids and reading order never
change.

:func:`apply_scope` applies four rules to pages 1..N in document order, each in one walk:

1. **Heading stack.** A visible level-L ``Heading`` pops every heading of
   level >= L and pushes itself. Every element's ``scope_path`` becomes the
   stack's texts, outermost first; a visible heading's includes its own text
   (A1: a heading opens its own scope). A hidden heading never pushes or
   pops, so hidden text cannot re-scope visible content; its own
   ``scope_path`` is the current path plus its text (A1 requires a heading's
   path to end with its text), and every other hidden element gets the
   current path.
2. **Header context.** Page 1's visible ``KeyValue`` elements that come, in
   reading order, before its first visible ``Table`` or ``Heading`` (an
   invoice's number and date above its line items, say).
3. **Split tables.** When page p's last visible element is a ``Table`` and
   page p+1's first visible element is a ``Table`` with as many columns,
   each column centre within :data:`SPLIT_TABLE_TOLERANCE_PT` of the
   other's, the second continues the first: its ``continued_from_id`` names
   the first, and when it has no headers it takes the first's (headers it
   has, repeated or its own, are kept). Both tables need
   ``column_centres``; without them there is no link. Chains link pairwise
   (p -> p+1 -> p+2). A link already on a table is left alone.
4. **Sections.** One flat A1 ``Section`` per visible heading, holding that
   heading (as ``elements[0]``) and everything after it up to the next
   visible heading of any level, plus a leading heading-less section for
   what comes before the first visible heading (only if there is any).
   A1 sections do not nest (every element shares its section's
   ``scope_path``), so a subsection is its own section and nesting is read
   from ``scope_path`` prefixes. ``page_span`` runs from the section's first
   element's page to its last's.

The stack holds at most six entries (levels 1-6 strictly increase down it),
so each walk is O(total elements); rule 3 adds O(columns) per page boundary.

Known limitations:

- A visible element above a continuation (a running page header, say)
  makes it not page p+1's first visible element, so it does not link.
- A hidden heading is in no section: A1 makes a heading's ``scope_path``
  end with its own text and a section's elements share its path, so a
  heading that opens no scope fits none. It stays in its page's elements.
"""

import dataclasses
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeAlias

from datagrunt.pdf._validate import typed_tuple
from datagrunt.pdf.elements import DocumentElement, Heading, KeyValue, Page, Section, Table

# How far apart, in points, the same column's centres may lie on the two parts of a split table (spec §3.3).
SPLIT_TABLE_TOLERANCE_PT = 8.0

# The open headings as (level, text), outermost first; levels strictly increase.
_Stack: TypeAlias = tuple[tuple[int, str], ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class ScopeResult:
    """What :func:`apply_scope` derives from a document's pages.

    Attributes:
        pages: The pages, each element carrying its ``scope_path``.
        sections: Flat, heading-scoped runs of the pages' own elements, in
            document order (see rule 4).
        header_context: Page 1's visible key-values that head the document,
            in reading order: the elements of ``pages[0]`` themselves.
    """

    pages: tuple[Page, ...]
    sections: tuple[Section, ...]
    header_context: tuple[KeyValue, ...]

    def __post_init__(self) -> None:
        typed_tuple("pages", self.pages, Page)
        typed_tuple("sections", self.sections, Section)
        typed_tuple("header_context", self.header_context, KeyValue)


def apply_scope(pages: tuple[Page, ...]) -> ScopeResult:
    """Return ``pages`` scoped (``scope_path``, split-table links), plus their sections and header context.

    Args:
        pages: The document's pages, numbered 1..N in order.

    Raises:
        TypeError: If ``pages`` is not a tuple of ``Page``.
        ValueError: If the pages are not numbered 1..N in order.
    """
    _check_pages(pages)
    scoped = _link_split_tables(_scope_pages(pages))
    return ScopeResult(pages=scoped, sections=_sections(scoped), header_context=_header_context(scoped))


def _check_pages(pages: object) -> None:
    """Require a tuple of ``Page`` numbered 1..N in order (document order is page order)."""
    checked = typed_tuple("pages", pages, Page)
    for index, item in enumerate(checked):
        if item.page_number != index + 1:
            raise ValueError(f"pages[{index}] has page_number {item.page_number}, expected {index + 1}")


def _scope_pages(pages: tuple[Page, ...]) -> tuple[Page, ...]:
    """Return the pages with each element's ``scope_path`` set from the heading stack (rule 1)."""
    stack: _Stack = ()
    scoped: list[Page] = []
    for item in pages:
        elements: list[DocumentElement] = []
        for element in item.elements:
            stack, path = _step(stack, element)
            elements.append(element if element.scope_path == path else dataclasses.replace(element, scope_path=path))
        scoped.append(dataclasses.replace(item, elements=tuple(elements)))
    return tuple(scoped)


def _step(stack: _Stack, element: DocumentElement) -> tuple[_Stack, tuple[str, ...]]:
    """Return the stack after ``element`` and the element's own ``scope_path``."""
    path = tuple(text for _, text in stack)
    if not isinstance(element, Heading):
        return stack, path
    if element.is_hidden:
        return stack, (*path, element.text)
    pushed = (*(entry for entry in stack if entry[0] < element.level), (element.level, element.text))
    return pushed, tuple(text for _, text in pushed)


def _link_split_tables(pages: tuple[Page, ...]) -> tuple[Page, ...]:
    """Return the pages with each split table's continuation linked to its previous part (rule 3).

    Each page is linked against the already-linked previous page, so headers
    a continuation took pass on down a chain.
    """
    linked: list[Page] = []
    for item in pages:
        linked.append(_continue_from(linked[-1], item) if linked else item)
    return tuple(linked)


def _continue_from(previous: Page, item: Page) -> Page:
    """Return ``item`` with its leading table linked to ``previous``'s trailing table if they are one table."""
    tail = _first_visible(reversed(previous.elements))
    head = _first_visible(item.elements)
    if not isinstance(tail, Table) or not isinstance(head, Table) or not _same_columns(tail, head):
        return item
    joined = dataclasses.replace(head, continued_from_id=tail.element_id, headers=head.headers or tail.headers)
    return dataclasses.replace(
        item, elements=tuple(joined if element is head else element for element in item.elements)
    )


def _first_visible(elements: Iterable[DocumentElement]) -> DocumentElement | None:
    """Return the first element that is not hidden, or None."""
    return next((element for element in elements if not element.is_hidden), None)


def _same_columns(first: Table, second: Table) -> bool:
    """True if both tables have column centres, as many columns, and each centre within the tolerance."""
    if not first.column_centres or len(first.column_centres) != len(second.column_centres):
        return False
    pairs = zip(first.column_centres, second.column_centres, strict=True)
    return all(abs(left - right) <= SPLIT_TABLE_TOLERANCE_PT for left, right in pairs)


def _sections(pages: tuple[Page, ...]) -> tuple[Section, ...]:
    """Return a flat section per visible heading, after a leading heading-less one if needed (rule 4)."""
    found: list[Section] = []
    heading: Heading | None = None
    members: list[DocumentElement] = []
    for element in (element for item in pages for element in item.elements):
        if not isinstance(element, Heading):
            members.append(element)
        elif not element.is_hidden:
            found.extend(_section(heading, members))
            heading, members = element, [element]
    found.extend(_section(heading, members))
    return tuple(found)


def _section(heading: Heading | None, members: list[DocumentElement]) -> tuple[Section, ...]:
    """Return the section of ``members`` under ``heading``, or nothing when there are no members."""
    if not members:
        return ()
    return (
        Section(
            heading=heading,
            scope_path=() if heading is None else heading.scope_path,
            page_span=(members[0].page_number, members[-1].page_number),
            elements=tuple(members),
        ),
    )


def _header_context(pages: tuple[Page, ...]) -> tuple[KeyValue, ...]:
    """Return page 1's visible key-values before its first visible table or heading (rule 2)."""
    if not pages:
        return ()
    context: list[KeyValue] = []
    for element in pages[0].elements:
        if element.is_hidden:
            continue
        if isinstance(element, (Table, Heading)):
            break
        if isinstance(element, KeyValue):
            context.append(element)
    return tuple(context)
