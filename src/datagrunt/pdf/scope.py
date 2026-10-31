"""Cross-page scope for the v5 PDF document model (#370, A6: spec §3.3 step 5).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads A1 :class:`~datagrunt.pdf.elements.Page` values only and calls no
pdfium function. A1 types are frozen, so changed elements and pages are
rebuilt with :func:`dataclasses.replace`; element ids and reading order never
change.

:func:`apply_scope` walks pages 1..N once, in document order:

1. **Heading stack.** A visible level-L ``Heading`` pops every heading of
   level >= L and pushes itself. Every element's ``scope_path`` becomes the
   stack's texts, outermost first; a visible heading's includes its own text
   (A1: a heading opens its own scope). A hidden heading never pushes or
   pops, so hidden text cannot re-scope visible content; its own
   ``scope_path`` is the current path plus its text (A1 requires a heading's
   path to end with its text), and every other hidden element gets the
   current path.

The stack holds at most six entries (levels 1-6 strictly increase down it),
so the walk is O(total elements).
"""

import dataclasses
from dataclasses import dataclass
from typing import TypeAlias

from datagrunt.pdf._validate import typed_tuple
from datagrunt.pdf.elements import DocumentElement, Heading, Page

# The open headings as (level, text), outermost first; levels strictly increase.
_Stack: TypeAlias = tuple[tuple[int, str], ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class ScopeResult:
    """What :func:`apply_scope` derives from a document's pages.

    Attributes:
        pages: The pages, each element carrying its ``scope_path``.
    """

    pages: tuple[Page, ...]

    def __post_init__(self) -> None:
        typed_tuple("pages", self.pages, Page)


def apply_scope(pages: tuple[Page, ...]) -> ScopeResult:
    """Return ``pages`` with every element's ``scope_path`` set by the heading stack.

    Args:
        pages: The document's pages, numbered 1..N in order.

    Raises:
        TypeError: If ``pages`` is not a tuple of ``Page``.
        ValueError: If the pages are not numbered 1..N in order.
    """
    _check_pages(pages)
    return ScopeResult(pages=_scope_pages(pages))


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
