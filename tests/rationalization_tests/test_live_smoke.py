"""Opt-in live smoke tests against provider endpoints (plan §6 T16).

Deselected by default in ``pyproject.toml`` (``-m 'not corpus and not live'``).
Run explicitly with::

    .venv/bin/pytest tests/rationalization_tests/test_live_smoke.py -m live -v --no-cov

Each test skips cleanly when its required API key environment variable is unset.
"""

from __future__ import annotations

import os
from typing import Annotated

import pytest
from pydantic import BaseModel

from datagrunt.pdf.elements import (
    BBox,
    DocumentMetadata,
    Page,
    PageQuality,
    Paragraph,
    PDFDocument,
    make_element_id,
)
from datagrunt.rationalization import Key, rationalize

pytestmark = pytest.mark.live

BOX = BBox(x0=72.0, y0=72.0, x1=540.0, y1=120.0)
DOC_SHA = "e" * 64


class SmokePart(BaseModel):
    """Minimal schema for live provider smoke tests."""

    part: Annotated[str, Key]
    qty: int


def _smoke_document() -> PDFDocument:
    para = Paragraph(
        element_id=make_element_id(1, 0),
        page_number=1,
        reading_order=0,
        bbox=BOX,
        text="Line item: Part PN-104 has quantity 12 units in stock.",
    )
    quality = PageQuality(
        valid_char_ratio=1.0,
        char_count=len(para.text),
        vector_line_count=0,
        hidden_spans_flagged=0,
        rotation_deg=0,
        deskew_angle_deg=0.0,
        ocr_mean_confidence=None,
    )
    page = Page(
        page_number=1,
        width_pt=612.0,
        height_pt=792.0,
        status="ok",
        route="digital",
        quality=quality,
        elements=(para,),
        integrity_verified=True,
    )
    meta = DocumentMetadata(
        doc_id=DocumentMetadata.make_doc_id("live_smoke.pdf", DOC_SHA),
        source_name="live_smoke.pdf",
        sha256=DOC_SHA,
        page_count=1,
    )
    return PDFDocument(metadata=meta, pages=(page,))


def _has_env(var: str) -> bool:
    val = os.environ.get(var)
    return bool(val and val.strip())


def test_live_smoke_openai() -> None:
    if not _has_env("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set")

    result = rationalize(
        _smoke_document(),
        schema=SmokePart,
        prompt="Extract the part identifier and integer quantity.",
        models=["openai:gpt-4o-mini"],
        min_families=1,
        expect_non_empty=True,
    )
    assert any(r.part == "PN-104" and r.qty == 12 for r in result.records)


def test_live_smoke_anthropic() -> None:
    if not _has_env("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")

    result = rationalize(
        _smoke_document(),
        schema=SmokePart,
        prompt="Extract the part identifier and integer quantity.",
        models=["anthropic:claude-haiku-4-5"],
        min_families=1,
        expect_non_empty=True,
    )
    assert any(r.part == "PN-104" and r.qty == 12 for r in result.records)


def test_live_smoke_gemini() -> None:
    if not _has_env("GEMINI_API_KEY"):
        pytest.skip("GEMINI_API_KEY not set")

    result = rationalize(
        _smoke_document(),
        schema=SmokePart,
        prompt="Extract the part identifier and integer quantity.",
        models=["gemini:gemini-2.5-flash"],
        min_families=1,
        expect_non_empty=True,
    )
    assert any(r.part == "PN-104" and r.qty == 12 for r in result.records)


def test_live_smoke_cross_family_consensus() -> None:
    candidates = [
        ("OPENAI_API_KEY", "openai:gpt-4o-mini"),
        ("ANTHROPIC_API_KEY", "anthropic:claude-haiku-4-5"),
        ("GEMINI_API_KEY", "gemini:gemini-2.5-flash"),
    ]
    available = [model for env_var, model in candidates if _has_env(env_var)]
    if len(available) < 2:
        pytest.skip("At least two of OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY are required")

    result = rationalize(
        _smoke_document(),
        schema=SmokePart,
        prompt="Extract the part identifier and integer quantity.",
        models=available[:2],
        min_families=2,
        expect_non_empty=True,
    )
    assert any(r.part == "PN-104" and r.qty == 12 for r in result.records)
    assert any(item.status == "consensus" for item in result.items)
