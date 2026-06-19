"""Shared helpers for PDF backend parity assertions."""

from __future__ import annotations

ELEMENT_SCHEMA_KEYS = frozenset({"id", "type", "content", "position", "metadata"})

TEXT_FLOOR_RATIO = 0.5


def elements_by_type(doc: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    for page in doc.get("document", {}).get("pages", []):
        for element in page.get("elements", []):
            element_type = element["type"]
            counts[element_type] = counts.get(element_type, 0) + 1
    return counts


def table_cells(doc: dict) -> list:
    cells = []
    for page in doc.get("document", {}).get("pages", []):
        for element in page.get("elements", []):
            if element.get("type") == "table":
                cells.append(element.get("content"))
    return cells


def text_chars(doc: dict) -> int:
    total = 0
    for page in doc.get("document", {}).get("pages", []):
        for element in page.get("elements", []):
            content = element.get("content")
            if isinstance(content, str):
                total += len(content)
    return total


def collect_schema_violations(doc: dict) -> list[str]:
    violations = []
    for page in doc.get("document", {}).get("pages", []):
        for element in page.get("elements", []):
            missing = ELEMENT_SCHEMA_KEYS - element.keys()
            if missing:
                violations.append(f"page {page.get('page_number')}: missing {sorted(missing)}")
    return violations


def text_within_floor(left: int, right: int, floor: float = TEXT_FLOOR_RATIO) -> bool:
    lo, hi = sorted((left, right))
    return lo >= floor * hi
