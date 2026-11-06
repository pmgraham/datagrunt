"""Tests for ``datagrunt.rationalization.grounding``: the anti-hallucination gate over consensus items.

Documents hold real elements (built with ``pdf_tests.documents``), units and envelopes come from the real builders,
and items from consensus over hand-built responses, so every quote is checked against exactly the text a model read
(``element_text``).
"""

import ast
import dataclasses
import gc
import math
import sys
import time
import unicodedata
from collections.abc import Callable
from datetime import date
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, Literal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pdf_tests.documents import VISION_QUALITY, ids, metadata, page
from pydantic import BaseModel

from datagrunt.pdf.elements import (
    BulletList,
    DocumentElement,
    Figure,
    Heading,
    KeyValue,
    Paragraph,
    PDFDocument,
    Section,
    Table,
)
from datagrunt.rationalization import Key
from datagrunt.rationalization import grounding as grounding_module
from datagrunt.rationalization.consensus import ConsensusResult, build_consensus, normalized_text
from datagrunt.rationalization.envelope import Envelope, build_envelopes, element_text, render_element
from datagrunt.rationalization.grounding import (
    GROUNDED_STATUSES,
    GROUNDING_REASONS,
    QUOTE_MIN_TOKENS,
    VISION_CONTAINMENT,
    GroundedItem,
    GroundingReason,
    GroundingResult,
    ground,
    numeric_values,
    token_containment,
    unescape_markdown,
)
from datagrunt.rationalization.runner import FailedUnit, RunOutcome
from datagrunt.rationalization.schema import PreparedSchema, prepare
from datagrunt.rationalization.units import Unit, build_units
from datagrunt.rationalization.wire import ProviderResponse

BIG = 1_000_000
GOOGLE = ("gemini:gemini-3", "google", "gemini")
ANTHROPIC = ("anthropic:claude", "anthropic", "anthropic")
PAIR = (GOOGLE, ANTHROPIC)
G, A = GOOGLE[0], ANTHROPIC[0]

# ---------------------------------------------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------------------------------------------


class Part(BaseModel):
    number: Annotated[str, Key]
    title: str


class Counted(BaseModel):
    count: Annotated[int, Key]


class Priced(BaseModel):
    price: Annotated[Decimal, Key]


class Shade(Enum):
    RED = "red"
    BLUE = "blue"


class Dated(BaseModel):
    day: Annotated[date, Key]


class Shaded(BaseModel):
    shade: Annotated[Shade, Key]


class Sized(BaseModel):
    size: Annotated[Literal["S", "M"], Key]


class Line(BaseModel):
    """A root without keys."""

    text: str


PART = prepare(Part)
COUNTED = prepare(Counted)
PRICED = prepare(Priced)
DATED = prepare(Dated)
SHADED = prepare(Shaded)
SIZED = prepare(Sized)
LINE = prepare(Line)

# ---------------------------------------------------------------------------------------------------------------
# Elements, documents, units and envelopes
# ---------------------------------------------------------------------------------------------------------------


def para(page_number: int, index: int, text: str) -> Paragraph:
    return Paragraph(**ids(page_number, index), text=text)


def pair(page_number: int, index: int, key: str, value: str) -> KeyValue:
    return KeyValue(**ids(page_number, index), key=key, value=value)


def grid(page_number: int, index: int, headers: tuple[str, ...], *rows: tuple[str, ...]) -> Table:
    return Table(**ids(page_number, index), headers=headers, rows=rows, extraction_method="lattice")


def bullets(page_number: int, index: int, *items: str) -> BulletList:
    return BulletList(**ids(page_number, index), items=items, ordered=False)


def titled(page_number: int, index: int, text: str) -> Heading:
    return Heading(**ids(page_number, index, (text,)), level=1, text=text)


def captioned(page_number: int, index: int, caption: str) -> Figure:
    return Figure(**ids(page_number, index), sha256="0" * 64, width_px=8, height_px=8, caption=caption)


def document(*pages: Any, sections: tuple[Section, ...] = (), header_context: tuple[KeyValue, ...] = ()) -> PDFDocument:
    return PDFDocument(metadata=metadata(len(pages)), pages=pages, sections=sections, header_context=header_context)


@dataclasses.dataclass(frozen=True)
class Prepared:
    """A document's units and envelopes, by unit id in document order."""

    document: PDFDocument
    units: dict[str, Unit]
    envelopes: dict[str, Envelope]

    @property
    def unit_ids(self) -> tuple[str, ...]:
        return tuple(self.units)


def documents_of(*runs: Prepared) -> dict[str, PDFDocument]:
    """The documents of ``runs`` by doc id, as :func:`ground` takes them."""
    return {run.document.metadata.doc_id: run.document for run in runs}


def prepared(doc: PDFDocument, limit: int = BIG) -> Prepared:
    plan = build_units(doc, max_unit_chars=limit, render=render_element)
    envelopes = build_envelopes(plan.units, doc, prompt="Extract.", context=None, schema_json=PART.wrapper_json)
    return Prepared(
        document=doc,
        units={unit.unit_id: unit for unit in plan.units},
        envelopes={envelope.unit_id: envelope for envelope in envelopes},
    )


# Page 1: every element type, with Markdown escapes, a <br> cell break, an R8 token and an NFD accent.
PAGE_ONE: tuple[DocumentElement, ...] = (
    titled(1, 0, "Parts"),
    para(1, 1, "Part PN-104 is the <M8> bolt, *zinc* \\ plated."),
    pair(1, 2, "Invoice", "1,200"),
    grid(1, 3, ("Part", "Qty", "Price"), ("PN-104", "1,200", "12.50"), ("PN-7\nrev B", "-42", "1.200,50")),
    bullets(1, 4, "first item", "second item"),
    captioned(1, 5, "Figure 1: the PN-104 bolt"),
    para(1, 6, "Left ‮ right"),
    para(1, 7, unicodedata.normalize("NFD", "Café menu")),
)
# Page 2: one paragraph, another unit's element.
PAGE_TWO: tuple[DocumentElement, ...] = (para(2, 0, "Part PN-999 lives elsewhere."),)
DOC = document(page(1, PAGE_ONE), page(2, PAGE_TWO))
RUN = prepared(DOC)
U1, U2 = RUN.unit_ids
E = {element.element_id: element for element in (*PAGE_ONE, *PAGE_TWO)}
HEADING, PROSE, INVOICE, TABLE, LIST, FIGURE, BIDI, NFD = (element.element_id for element in PAGE_ONE)
(ELSEWHERE,) = (element.element_id for element in PAGE_TWO)

# ---------------------------------------------------------------------------------------------------------------
# Responses, outcomes and consensus
# ---------------------------------------------------------------------------------------------------------------


def entry(record: dict[str, Any], quotes: tuple[str, ...], element_ids: tuple[str, ...]) -> dict[str, Any]:
    return {"record": record, "grounding_quotes": list(quotes), "source_element_ids": list(element_ids)}


def response(*entries: dict[str, Any], finish: str = "complete") -> ProviderResponse:
    return ProviderResponse(
        content={"records": list(entries)},
        raw_text="",
        finish=finish,  # type: ignore[arg-type]
        input_tokens=None,
        output_tokens=None,
    )


def outcome_of(
    responses: dict[tuple[str, str], ProviderResponse],
    models: tuple[tuple[str, str, str], ...],
    failed: tuple[FailedUnit, ...] = (),
) -> RunOutcome:
    return RunOutcome(
        responses=responses,
        failed_units=failed,
        cache_hits=0,
        cache_misses=len(responses),
        cache_corrupt=0,
        cache_write_failures=0,
        usage={},
        models=models,
    )


def grounded(
    responses: dict[tuple[str, str], ProviderResponse],
    *,
    models: tuple[tuple[str, str, str], ...] = (GOOGLE,),
    schema: PreparedSchema = PART,
    run: Prepared = RUN,
    failed: tuple[FailedUnit, ...] = (),
) -> GroundingResult:
    """Consensus over ``responses`` (min_families: one per distinct family of ``models``), then grounding."""
    outcome = outcome_of(responses, models, failed)
    families = len({model[1] for model in models})
    consensus = build_consensus(outcome, schema, run.unit_ids, min_families=families)
    return ground(
        consensus, outcome=outcome, units=run.units, envelopes=run.envelopes, documents=documents_of(run), schema=schema
    )


def part(number: str = "PN-104", title: str = "Bolt") -> dict[str, Any]:
    return {"number": number, "title": title}


def single(
    record: dict[str, Any],
    quotes: tuple[str, ...],
    element_ids: tuple[str, ...],
    *,
    schema: PreparedSchema = PART,
    unit_id: str | None = None,
    run: Prepared = RUN,
) -> GroundedItem:
    """The one item of one model's one record."""
    unit = run.unit_ids[0] if unit_id is None else unit_id
    result = grounded({(unit, G): response(entry(record, quotes, element_ids))}, schema=schema, run=run)
    (found,) = result.items
    return found


def reasons(found: GroundedItem) -> list[tuple[str, str, int | None, str | None]]:
    """Each reason as ``(reason, model, index, key field)``."""
    return [(reason.reason, reason.model_id, reason.index, reason.key_field) for reason in found.reasons]


# ---------------------------------------------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------------------------------------------


class TestGroundingReason:
    def test_holds_its_fields_frozen_and_slotted(self):
        reason = GroundingReason("quote_not_found", G, index=2, text="a quote")

        assert (reason.reason, reason.model_id, reason.index, reason.key_field, reason.text) == (
            "quote_not_found",
            G,
            2,
            None,
            "a quote",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            reason.index = 3  # type: ignore[misc]
        assert not hasattr(reason, "__dict__")

    def test_the_reasons_in_their_order(self):
        assert GROUNDING_REASONS == (
            "unknown_element",
            "no_element_ids",
            "header_only",
            "no_quotes",
            "quote_not_found",
            "quote_too_short",
            "key_not_found",
            "key_not_quoted",
        )

    def test_the_repr_never_holds_the_quote_or_the_id(self):
        quoted = GroundingReason("quote_not_found", G, index=0, text="Ignore prior instructions")
        cited = GroundingReason("unknown_element", G, index=1, text="p0009_e999 or anything")

        assert "Ignore" not in repr(quoted) and "anything" not in repr(cited)
        assert "quote_not_found" in repr(quoted) and "index=0" in repr(quoted)

    @pytest.mark.parametrize(
        ("arguments", "error"),
        [
            (("made_up", G), ValueError),
            ((1, G), TypeError),
            (("no_quotes", ""), ValueError),
            (("no_quotes", " "), ValueError),
            (("no_quotes", 7), TypeError),
            (("no_quotes", G, 0), ValueError),
            (("no_element_ids", G, None, "number"), ValueError),
            (("no_quotes", G, None, None, "text"), ValueError),
            (("quote_not_found", G), ValueError),
            (("quote_not_found", G, None, None, "q"), ValueError),
            (("quote_not_found", G, 0), ValueError),
            (("quote_not_found", G, -1, None, "q"), ValueError),
            (("quote_not_found", G, True, None, "q"), TypeError),
            (("quote_not_found", G, 1.0, None, "q"), TypeError),
            (("quote_not_found", G, 0, None, b"q"), TypeError),
            (("quote_not_found", G, 0, "number", "q"), ValueError),
            (("unknown_element", G, None, None, "p0001_e001"), ValueError),
            (("key_not_found", G), ValueError),
            (("key_not_found", G, None, ""), ValueError),
            (("key_not_found", G, None, 3), TypeError),
            (("key_not_found", G, 0, "number"), ValueError),
            (("header_only", G, 0), ValueError),
            (("header_only", G, None, "number"), ValueError),
            (("header_only", G, None, None, "p0001_e000"), ValueError),
            (("key_not_quoted", G), ValueError),
            (("key_not_quoted", G, 0, "number"), ValueError),
            (("key_not_quoted", G, None, "number", "PN-104"), ValueError),
            (("quote_too_short", G, 0), ValueError),
            (("quote_too_short", G, None, "number"), ValueError),
            (("quote_too_short", G, None, None, "e"), ValueError),
        ],
    )
    def test_rejects(self, arguments, error):
        with pytest.raises(error):
            GroundingReason(*arguments)

    def test_takes_each_reason_in_its_shape(self):
        GroundingReason("unknown_element", G, index=0, text="p0001_e001")
        GroundingReason("no_element_ids", G)
        GroundingReason("header_only", G)
        GroundingReason("no_quotes", G)
        GroundingReason("quote_not_found", G, index=4, text="")
        GroundingReason("quote_too_short", G)
        GroundingReason("key_not_found", G, key_field="number")
        GroundingReason("key_not_quoted", G, key_field="number")


def consensus_item(status: str = "single_model", models: tuple[str, ...] = (G,), **overrides: Any) -> Any:
    """A consensus item built by hand, of ``status``, each model with one vote."""
    outcome = outcome_of({(U1, G): response(entry(part(), ("PN-104",), (PROSE,)))}, (GOOGLE,))
    (from_consensus,) = build_consensus(outcome, PART, RUN.unit_ids, min_families=1).items
    votes = {model: (from_consensus.model_votes[G][0],) for model in models}
    fields: dict[str, Any] = {
        "unit_id": U1,
        "status": status,
        "key": from_consensus.key,
        "record": from_consensus.record if status in ("consensus", "single_model") else None,
        "disputed_fields": ("title",) if status == "field_disagreement" else (),
        "notes": (),
        "model_votes": votes,
        "quotes_by_model": {model: ("PN-104",) for model in models},
        "element_ids_by_model": {model: (PROSE,) for model in models},
        "families": {model: "google" for model in models},
        **overrides,
    }
    return dataclasses.replace(from_consensus, **fields)


NO_QUOTES = GroundingReason("no_quotes", G)


class TestGroundedItem:
    def test_a_grounded_item_keeps_its_status(self):
        item = consensus_item("single_model")

        found = GroundedItem(item=item, status="single_model", reasons=())

        assert (found.item, found.status, found.reasons, found.grounded) == (item, "single_model", (), True)
        assert not hasattr(found, "__dict__")

    @pytest.mark.parametrize("status", ["consensus", "single_model"])
    def test_a_clean_item_with_a_reason_is_ungrounded_value(self, status):
        models = (G,) if status == "single_model" else (G, A)
        found = GroundedItem(item=consensus_item(status, models), status="ungrounded_value", reasons=(NO_QUOTES,))

        assert (found.status, found.grounded) == ("ungrounded_value", False)

    @pytest.mark.parametrize("status", ["missing_in_some_models", "field_disagreement"])
    def test_a_review_item_with_a_reason_keeps_its_status(self, status):
        found = GroundedItem(item=consensus_item(status, (G, A)), status=status, reasons=(NO_QUOTES,))  # type: ignore[arg-type]

        assert (found.status, found.grounded) == (status, False)

    def test_the_statuses_are_consensus_s_and_ungrounded_value(self):
        assert GROUNDED_STATUSES == (
            "consensus",
            "single_model",
            "missing_in_some_models",
            "field_disagreement",
            "unmatched",
            "ungrounded_value",
        )

    @pytest.mark.parametrize(
        ("fields", "error"),
        [
            ({"item": "not an item", "status": "single_model", "reasons": ()}, TypeError),
            ({"status": "made_up", "reasons": ()}, ValueError),
            ({"status": "single_model", "reasons": [NO_QUOTES]}, TypeError),
            ({"status": "single_model", "reasons": ("no_quotes",)}, TypeError),
            ({"status": "single_model", "reasons": (NO_QUOTES,)}, ValueError),
            ({"status": "ungrounded_value", "reasons": ()}, ValueError),
            ({"status": "consensus", "reasons": ()}, ValueError),
            ({"status": "ungrounded_value", "reasons": (GroundingReason("no_quotes", A),)}, ValueError),
        ],
    )
    def test_rejects(self, fields, error):
        with pytest.raises(error):
            GroundedItem(**{"item": consensus_item("single_model"), **fields})

    def test_a_review_item_never_becomes_ungrounded_value(self):
        with pytest.raises(ValueError):
            GroundedItem(
                item=consensus_item("field_disagreement", (G, A)), status="ungrounded_value", reasons=(NO_QUOTES,)
            )

    def test_the_repr_holds_no_quote_or_document_text(self):
        reason = GroundingReason("quote_not_found", G, index=0, text="Part PN-104 is the bolt")
        found = GroundedItem(item=consensus_item("single_model"), status="ungrounded_value", reasons=(reason,))

        assert "PN-104" not in repr(found) and "bolt" not in repr(found)
        assert "quote_not_found" in repr(found) and "ungrounded_value" in repr(found)


class TestGroundingResult:
    def test_holds_tuples(self):
        found = GroundedItem(item=consensus_item(), status="single_model", reasons=())

        result = GroundingResult(items=(found,), empty_units=(U2,))

        assert (result.items, result.empty_units) == ((found,), (U2,))

    @pytest.mark.parametrize(
        ("fields", "error"),
        [
            ({"items": [], "empty_units": ()}, TypeError),
            ({"items": ("item",), "empty_units": ()}, TypeError),
            ({"items": (), "empty_units": [U1]}, TypeError),
            ({"items": (), "empty_units": (1,)}, TypeError),
            ({"items": (), "empty_units": ("",)}, ValueError),
            ({"items": (), "empty_units": (U1, U1)}, ValueError),
        ],
    )
    def test_rejects(self, fields, error):
        with pytest.raises(error):
            GroundingResult(**fields)


# ---------------------------------------------------------------------------------------------------------------
# The helpers
# ---------------------------------------------------------------------------------------------------------------


class TestUnescapeMarkdown:
    @pytest.mark.parametrize(
        ("element", "written"),
        [
            (E[PROSE], "Part PN-104 is the <M8> bolt, *zinc* \\ plated."),
            (pair(1, 9, "Invoice #", "-42"), "**Invoice #:** -42"),
            (para(1, 9, "1. Start\n- dash\n+ plus\n= eq\n12) x"), "1. Start\n- dash\n+ plus\n= eq\n12) x"),
            (para(1, 9, "a_b [c] `d` ~e~ |f| &g; #h"), "a_b [c] `d` ~e~ |f| &g; #h"),
            (titled(1, 9, "T #1"), "# T #1"),
            (E[FIGURE], "Figure 1: the PN-104 bolt"),
        ],
    )
    def test_gives_back_the_characters_of_real_element_text(self, element, written):
        assert "\\" in element_text(element) or element is E[FIGURE]
        assert unescape_markdown(element_text(element)) == written

    def test_a_table_cells_br_is_a_line_break(self):
        assert element_text(E[TABLE]).splitlines()[3] == "| PN-7<br>rev B | \\-42 | 1.200,50 |"
        assert unescape_markdown(element_text(E[TABLE])).splitlines()[3:5] == ["| PN-7", "rev B | -42 | 1.200,50 |"]

    def test_a_documents_own_br_stays_literal_text(self):
        text = element_text(grid(1, 9, ("h",), ("a <br> b",)))

        assert "\\<br\\>" in text
        assert unescape_markdown(text).endswith("| a <br> b |")

    def test_it_scans_once_left_to_right(self):
        assert unescape_markdown("\\\\<br>") == "\\\n"
        assert unescape_markdown("\\\\\\*") == "\\*"

    def test_r8_tokens_stay_as_written(self):
        assert element_text(E[BIDI]) == "Left [U+202E] right"
        assert unescape_markdown(element_text(E[BIDI])) == "Left [U+202E] right"
        assert unescape_markdown(element_text(para(1, 9, "a [U+202E] b"))) == "a [U+202E] b"

    @pytest.mark.parametrize("text", ["\\a", "\\é", "end \\", "hard\\\nbreak", "\\ space", "<b>", "<br/>"])
    def test_a_backslash_before_anything_but_ascii_punctuation_stays(self, text):
        assert unescape_markdown(text) == text

    @given(st.text(alphabet=st.characters(codec="utf-8", exclude_categories=("Cc", "Cs", "Zl", "Zp")), min_size=1))
    @settings(max_examples=200, deadline=None)
    def test_any_paragraph_gives_back_its_stripped_lines(self, text):
        if not text.strip() or any("\u202a" <= char <= "\u202e" or "\u2066" <= char <= "\u2069" for char in text):
            return
        if any(0xE0000 <= ord(char) <= 0xE007F or 0x80 <= ord(char) <= 0x9F for char in text):
            return
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        assert unescape_markdown(element_text(para(1, 9, text))) == "\n".join(lines)

    def test_rejects_a_non_str(self):
        with pytest.raises(TypeError):
            unescape_markdown(b"text")  # type: ignore[arg-type]


def numbers(*values: str) -> frozenset[Decimal]:
    return frozenset(Decimal(value) for value in values)


def best_of(work: Callable[[], object], runs: int = 5) -> float:
    """The fastest of ``runs`` runs of ``work``, in seconds, the garbage collector paused while each runs."""
    best = math.inf
    for _ in range(runs):
        gc.collect()
        gc.disable()
        try:
            started = time.perf_counter()
            work()
            best = min(best, time.perf_counter() - started)
        finally:
            gc.enable()
    return best


class TestNumericValues:
    @pytest.mark.parametrize(
        ("text", "found"),
        [
            ("1200", numbers("1200")),
            ("1,200", numbers("1200")),
            ("1,200.50", numbers("1200.5")),
            ("12.50", numbers("12.5")),
            ("0012", numbers("12")),
            ("1,234,567", numbers("1234567")),
            ("$1,200.", numbers("1200")),
            ("1,200% (1,200)", numbers("1200")),
            ("a 1, 2 and 3", numbers("1", "2", "3")),
            ("a 12\n34 b", numbers("12", "34")),
            ("#42", numbers("42")),
            ("+5", numbers("5")),
            ("Qty 5, 6", numbers("5", "6")),
        ],
    )
    def test_plain_and_comma_grouped_numbers(self, text, found):
        assert numeric_values(text) == found

    @pytest.mark.parametrize("text", ["word,5", "Qty,5", "a,1,200", "x,12.50"])
    def test_a_number_glued_after_a_comma_is_not_read(self, text):
        assert numeric_values(text) == frozenset()

    def test_reading_numbers_takes_linear_time(self):
        small, large = "1," * 20_000 + "1x", "1," * 40_000 + "1x"  # 40k and 80k characters, no number in either

        small_s, large_s = best_of(lambda: numeric_values(small)), best_of(lambda: numeric_values(large))

        assert numeric_values(small) == numeric_values(large) == frozenset()
        assert large_s <= 3 * small_s, f"40k chars: {small_s:.4f} s; 80k chars: {large_s:.4f} s"

    @pytest.mark.parametrize(
        "text",
        [
            "1.200,50",
            "12,5",
            "1,2000",
            "1,20",
            "1.2.3",
            "PN104",
            "12V",
            "10⁶",
            "_12",
            ".5",
            "v1.2",
            "1,2x",
            "１２",
            "١٢",
        ],
    )
    def test_what_is_not_recognized(self, text):
        assert numeric_values(text) == frozenset()

    def test_a_dot_grouping_reads_as_a_decimal_point(self):
        assert numeric_values("1.200") == numbers("1.2")

    @pytest.mark.parametrize("minus", ["-", "−"])
    def test_a_minus_before_a_number_gives_its_negative_too(self, minus):
        assert numeric_values(f"{minus}5") == numbers("5", "-5")
        assert numeric_values(f"PN{minus}104") == numbers("104", "-104")

    def test_values_compare_by_value_with_ints_and_decimals(self):
        found = numeric_values("1,200.00 and 12.50")

        assert 1200 in found and Decimal("1200") in found and Decimal("12.5") in found
        assert 12 not in found and Decimal("1200.01") not in found

    def test_a_long_number_and_its_negative_stay_exact(self):
        digits = "1234567890" * 4

        assert numeric_values(f"-{digits}") == frozenset({Decimal(digits), Decimal(f"-{digits}")})

    def test_rejects_a_non_str(self):
        with pytest.raises(TypeError):
            numeric_values(1200)  # type: ignore[arg-type]


def words(count: int, prefix: str = "w") -> str:
    return " ".join(f"{prefix}{index}" for index in range(count))


class TestTokenContainment:
    def test_the_share_of_the_quotes_tokens_the_text_holds(self):
        assert token_containment("a b c d", "x a b c d e") == 1
        assert token_containment(f"{words(42)} {words(8, 'z')}", words(60)) == Fraction(42, 50)
        assert token_containment(f"{words(43)} {words(7, 'z')}", words(60)) == Fraction(43, 50)

    def test_each_occurrence_is_matched_once(self):
        assert token_containment("A A A A Z", "A") == Fraction(1, 5)
        assert token_containment("A A Z", "A A") == Fraction(2, 3)

    def test_a_quote_without_tokens_has_none(self):
        assert token_containment("— $ \\* ...", "— $ \\* ...") == 0
        assert token_containment("", "text") == 0

    def test_tokens_are_runs_of_letters_and_digits(self):
        assert token_containment("PN\\-104 a_b", "PN 104 a b") == 1
        assert token_containment("[U+202E] <br>", "U 202E br") == 1
        assert token_containment("Café ñ", "Café ñ") == 1

    @pytest.mark.parametrize(
        ("quote", "text", "pieces"),
        [
            ("हुन्दा चोलोन", "हिन्दी चालान", ["हिन्दी", "चालान"]),
            ("ใบแจ๊งหนี๋เลขทิ่", "ใบแจ้งหนี้เลขที่", ["ใบแจ้งหนี้เลขที่"]),
            ("فِاتَورُة رُقِم", "فَاتُورَة رَقْم", ["فَاتُورَة", "رَقْم"]),
        ],
    )
    def test_a_token_keeps_its_combining_marks(self, quote, text, pieces):
        assert token_containment(quote, text) == 0
        assert token_containment(text, " ".join(pieces)) == 1

    @pytest.mark.parametrize("text", ["東京都港区六本木", "とうきょうと", "トウキョウ", "ใบแจ้งหนี้เลขที่"])
    def test_a_run_of_an_unspaced_script_is_one_token(self, text):
        assert token_containment(text, text) == 1
        assert token_containment(text[:2], text) == 0

    def test_tokens_compare_under_nfc_only(self):
        assert token_containment(unicodedata.normalize("NFD", "Café"), "Café") == 1
        assert token_containment("ABC", "abc") == 0
        assert token_containment("ｆｕｌｌ", "full") == 0
        assert token_containment("10⁶", "106") == 0

    def test_rejects_a_non_str(self):
        with pytest.raises(TypeError):
            token_containment(b"a", "a")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            token_containment("a", None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# ground()
# ---------------------------------------------------------------------------------------------------------------


class TestGroundedItems:
    def test_a_record_whose_evidence_holds_is_grounded_and_keeps_its_status(self):
        found = single(part(), ("PN-104",), (PROSE,))

        assert (found.status, found.grounded, found.reasons) == ("single_model", True, ())
        assert found.item.status == "single_model" and found.item.record == Part(number="PN-104", title="Bolt")

    def test_items_come_in_consensus_order_each_with_its_own_item(self):
        responses = {
            (U1, G): response(entry(part("PN-7"), ("PN-7",), (TABLE,)), entry(part(), ("PN-104",), (PROSE,))),
            (U2, G): response(entry(part("PN-999"), ("PN-999",), (ELSEWHERE,))),
        }
        outcome = outcome_of(responses, (GOOGLE,))
        consensus = build_consensus(outcome, PART, RUN.unit_ids, min_families=1)

        result = ground(
            consensus,
            outcome=outcome,
            units=RUN.units,
            envelopes=RUN.envelopes,
            documents=documents_of(RUN),
            schema=PART,
        )

        assert [found.item for found in result.items] == list(consensus.items)
        assert [found.item.key for found in result.items] == [("PN-104",), ("PN-7",), ("PN-999",)]
        assert all(found.grounded for found in result.items) and result.empty_units == ()

    def test_agreeing_families_that_each_ground_keep_consensus(self):
        answer = response(entry(part(), ("PN-104",), (PROSE,)))

        (found,) = grounded({(U1, G): answer, (U1, A): answer}, models=PAIR).items

        assert (found.item.status, found.status, found.grounded) == ("consensus", "consensus", True)


HIDDEN_DOC = document(
    page(
        1,
        (
            para(1, 0, "Part PN-104 is shown."),
            dataclasses.replace(para(1, 1, "Part PN-104 is hidden."), hidden_reasons=("tiny_font",)),
        ),
    )
)
HIDDEN_RUN = prepared(HIDDEN_DOC)


class TestCitableElements:
    def test_an_id_of_another_unit_is_unknown_and_named_by_its_position(self):
        found = single(part(), ("PN-104",), (PROSE, ELSEWHERE))

        assert reasons(found) == [("unknown_element", G, 1, None)]
        assert found.reasons[0].text == ELSEWHERE
        assert (found.status, found.grounded) == ("ungrounded_value", False)

    @pytest.mark.parametrize("cited", ["p0001_e099", "p0002_e000x", "anything at all", ""])
    def test_an_id_of_no_element_is_unknown(self, cited):
        found = single(part(), ("PN-104",), (cited, PROSE))

        assert reasons(found) == [("unknown_element", G, 0, None)]
        assert found.reasons[0].text == cited

    def test_a_hidden_elements_id_is_unknown(self):
        (unit_id,) = HIDDEN_RUN.unit_ids

        found = single(part(), ("PN-104",), ("p0001_e000", "p0001_e001"), unit_id=unit_id, run=HIDDEN_RUN)

        assert reasons(found) == [("unknown_element", G, 1, None)]

    def test_every_element_of_the_unit_may_be_cited(self):
        found = single(part(), ("PN-104",), (HEADING, PROSE, INVOICE, TABLE, LIST, FIGURE, BIDI, NFD))

        assert found.grounded

    def test_a_model_that_cites_no_element_is_no_element_ids(self):
        found = single(part(), ("PN-104",), ())

        assert reasons(found)[0] == ("no_element_ids", G, None, None)
        assert found.status == "ungrounded_value"


def header_document(vendor: str = "Acme") -> PDFDocument:
    """Page 1 opens with two header key-values in a section of their own; a "Parts" section follows."""
    scope = ("Parts",)
    invoice, supplier = pair(1, 0, "Invoice", "42"), pair(1, 1, "Vendor", vendor)
    heading = titled(1, 2, "Parts")
    bolt = Paragraph(**ids(1, 3, scope), text="Part PN-104 is the bolt.")
    nut = Paragraph(**ids(2, 0, scope), text="Part PN-7 is a nut.")
    sections = (
        Section(heading=None, scope_path=(), page_span=(1, 1), elements=(invoice, supplier)),
        Section(heading=heading, scope_path=scope, page_span=(1, 2), elements=(heading, bolt, nut)),
    )
    return document(
        page(1, (invoice, supplier, heading, bolt)),
        page(2, (nut,)),
        sections=sections,
        header_context=(invoice, supplier),
    )


HEADER_RUN = prepared(header_document())
H_OWN, H_PARTS = HEADER_RUN.unit_ids
H_INVOICE, H_VENDOR, H_HEADING, H_BOLT = "p0001_e000", "p0001_e001", "p0001_e002", "p0001_e003"
H_NUT = "p0002_e000"


def capped_header_document() -> PDFDocument:
    """Page 1: a ~3,000-character "Terms" header block, a "Supplier" section with a ~2,000-character "Vendor" header
    block, then a "Parts" section. Each section is a unit; together the two blocks pass the header copy's cap."""
    terms = pair(1, 0, "Terms", "T" * 3_000)
    vendor = KeyValue(**ids(1, 2, ("Supplier",)), key="Vendor", value="Acme " + "V" * 2_000)
    supplier_heading, parts_heading = titled(1, 1, "Supplier"), titled(1, 3, "Parts")
    supplier = Paragraph(**ids(1, 4, ("Parts",)), text="Part PN-104 is the bolt.")
    sections = (
        Section(heading=None, scope_path=(), page_span=(1, 1), elements=(terms,)),
        Section(
            heading=supplier_heading, scope_path=("Supplier",), page_span=(1, 1), elements=(supplier_heading, vendor)
        ),
        Section(heading=parts_heading, scope_path=("Parts",), page_span=(1, 1), elements=(parts_heading, supplier)),
    )
    elements = (terms, supplier_heading, vendor, parts_heading, supplier)
    return document(page(1, elements), sections=sections, header_context=(terms, vendor))


CAPPED_RUN = prepared(capped_header_document())
C_TERMS_UNIT, C_SUPPLIER_UNIT, C_PARTS_UNIT = CAPPED_RUN.unit_ids
C_TERMS, C_VENDOR, C_BOLT = "p0001_e000", "p0001_e002", "p0001_e004"


class TestHeaderContext:
    def test_a_header_block_another_units_envelope_shows_but_this_units_cap_cuts_is_unknown(self):
        shown = {unit_id: CAPPED_RUN.envelopes[unit_id].header_element_ids for unit_id in CAPPED_RUN.unit_ids}
        assert shown == {C_TERMS_UNIT: (C_VENDOR,), C_SUPPLIER_UNIT: (C_TERMS,), C_PARTS_UNIT: (C_TERMS,)}

        found = single(part(), ("PN-104",), (C_BOLT, C_VENDOR), unit_id=C_PARTS_UNIT, run=CAPPED_RUN)
        shown_here = single(part(), ("PN-104",), (C_BOLT, C_TERMS), unit_id=C_PARTS_UNIT, run=CAPPED_RUN)

        assert reasons(found) == [("unknown_element", G, 1, None)]
        assert shown_here.grounded

    def test_the_units_envelope_shows_the_header_blocks(self):
        assert HEADER_RUN.envelopes[H_PARTS].header_element_ids == (H_INVOICE, H_VENDOR)
        assert HEADER_RUN.envelopes[H_OWN].header_element_ids == ()

    def test_a_model_citing_only_header_blocks_is_header_only(self):
        result = grounded({(H_PARTS, G): response(entry(part("42", "Invoice"), ("42",), (H_INVOICE,)))}, run=HEADER_RUN)

        (found,) = result.items
        assert reasons(found) == [("header_only", G, None, None)]
        assert (found.item.status, found.status, found.item.key) == ("single_model", "ungrounded_value", ("42",))

    def test_a_header_block_the_envelope_shows_may_be_cited_with_one_of_the_units_own(self):
        found = single(part("42", "Invoice"), ("42",), (H_BOLT, H_INVOICE), unit_id=H_PARTS, run=HEADER_RUN)

        assert (found.status, found.reasons) == ("single_model", ())

    def test_a_model_citing_only_unknown_ids_is_not_header_only(self):
        found = single(part("42", "Invoice"), ("42",), ("p0009_e000",), unit_id=H_PARTS, run=HEADER_RUN)

        assert reasons(found) == [
            ("unknown_element", G, 0, None),
            ("quote_not_found", G, 0, None),
            ("key_not_found", G, None, "number"),
        ]

    def test_one_family_citing_only_header_blocks_demotes_a_consensus_record(self):
        own = entry(part("42", "Invoice"), ("42",), (H_BOLT, H_INVOICE))
        header_only = entry(part("42", "Invoice"), ("42",), (H_INVOICE,))
        responses = {(H_PARTS, G): response(own), (H_PARTS, A): response(header_only)}

        (found,) = grounded(responses, models=PAIR, run=HEADER_RUN).items

        assert (found.item.status, found.status) == ("consensus", "ungrounded_value")
        assert reasons(found) == [("header_only", A, None, None)]

    def test_header_and_own_blocks_may_be_cited_together(self):
        found = single(part(), ("PN-104", "Acme"), (H_BOLT, H_VENDOR), unit_id=H_PARTS, run=HEADER_RUN)

        assert found.grounded

    def test_the_unit_holding_the_header_blocks_cites_them_as_its_own(self):
        found = single(part("42", "Invoice"), ("42",), (H_INVOICE, H_VENDOR), unit_id=H_OWN, run=HEADER_RUN)

        assert found.grounded

    def test_the_header_units_envelope_shows_no_other_units_block(self):
        found = single(part("42", "Invoice"), ("42",), (H_INVOICE, H_BOLT), unit_id=H_OWN, run=HEADER_RUN)

        assert reasons(found) == [("unknown_element", G, 1, None)]

    def test_a_header_block_the_envelope_does_not_show_is_unknown(self):
        run = prepared(header_document("A" * 5_000))  # past the header copy's cap: the vendor block is not shown
        _, parts_unit = run.unit_ids
        assert run.envelopes[parts_unit].header_element_ids == (H_INVOICE,)

        found = single(part("42", "Invoice"), ("42",), (H_INVOICE, H_VENDOR), unit_id=parts_unit, run=run)
        with_own = single(part("42", "Invoice"), ("42",), (H_BOLT, H_INVOICE, H_VENDOR), unit_id=parts_unit, run=run)

        assert reasons(found) == [("unknown_element", G, 1, None), ("header_only", G, None, None)]
        assert reasons(with_own) == [("unknown_element", G, 2, None)]


class TestQuotes:
    def test_a_model_that_gives_no_quote_is_no_quotes(self):
        found = single(part(), (), (PROSE,))

        assert reasons(found) == [("no_quotes", G, None, None)]
        assert found.status == "ungrounded_value"


class TestQuoteText:
    def test_a_fabricated_quote_is_not_found_and_named_by_its_position(self):
        found = single(part(), ("PN-104", "PN-105 is the nut"), (PROSE,))

        assert reasons(found) == [("quote_not_found", G, 1, None)]
        assert found.reasons[0].text == "PN-105 is the nut"
        assert found.status == "ungrounded_value"

    def test_a_quote_in_an_element_the_model_did_not_cite_is_not_found(self):
        found = single(part(), ("PN-104", "first item"), (PROSE,))

        assert reasons(found) == [("quote_not_found", G, 1, None)]

    def test_a_quote_from_another_unit_is_not_found_even_when_cited(self):
        found = single(part(), ("PN-104", "lives elsewhere"), (PROSE, ELSEWHERE))

        assert reasons(found) == [("unknown_element", G, 1, None), ("quote_not_found", G, 1, None)]

    def test_a_quote_spanning_two_elements_is_not_found(self):
        prose, invoice = element_text(E[PROSE]), element_text(E[INVOICE])
        spanning = f"{prose[-7:]} {invoice[:12]}"
        assert spanning == "plated. **Invoice:**"

        found = single(part(), ("PN-104", spanning), (PROSE, INVOICE))

        assert reasons(found) == [("quote_not_found", G, 1, None)]

    @pytest.mark.parametrize(
        ("element_id", "quote"),
        [
            (HEADING, "# Parts"),
            (PROSE, "the \\<M8\\> bolt, \\*zinc\\* \\\\ plated."),
            (INVOICE, "**Invoice:** 1,200"),
            (TABLE, "| PN-7<br>rev B | \\-42 |"),
            (LIST, "- second item"),
            (FIGURE, "Figure 1: the PN-104 bolt"),
            (BIDI, "Left [U+202E] right"),
        ],
    )
    def test_a_quote_copied_exactly_as_written_is_found(self, element_id, quote):
        assert quote in element_text(E[element_id])

        found = single(part(), ("PN-104", quote), (PROSE, element_id))

        assert found.grounded

    @pytest.mark.parametrize(
        ("element_id", "quote"),
        [
            (PROSE, "the <M8> bolt, *zinc* \\ plated."),
            (TABLE, "| PN-7 rev B | -42 |"),
            (BIDI, "Left ‮ right"),
        ],
    )
    def test_a_quote_that_drops_the_payloads_escapes_is_not_found(self, element_id, quote):
        found = single(part(), ("PN-104", quote), (PROSE, element_id))

        assert reasons(found) == [("quote_not_found", G, 1, None)]

    @pytest.mark.parametrize("form", ["NFC", "NFD"])
    def test_canonically_equal_quotes_are_found(self, form):
        assert element_text(E[NFD]) == unicodedata.normalize("NFD", "Café menu")

        found = single(part(), ("PN-104", unicodedata.normalize(form, "Café menu")), (PROSE, NFD))

        assert found.grounded

    @pytest.mark.parametrize("quote", ["ＰＮ-104", "pn-104", "PART PN-104"])
    def test_compatibility_forms_and_case_are_not_folded(self, quote):
        found = single(part(), ("PN-104", quote), (PROSE,))

        assert reasons(found) == [("quote_not_found", G, 1, None)]

    def test_whitespace_runs_compare_as_one_space(self):
        across_lines = "| Part | Qty | Price |\n\n| --- |"
        assert "| Part | Qty | Price |\n| --- |" in element_text(E[TABLE])

        found = single(part(), ("  Part \t PN-104\n is the", across_lines), (PROSE, TABLE))

        assert found.grounded

    @pytest.mark.parametrize("quote", ["", "   ", "\n\t"])
    def test_a_blank_quote_is_never_found(self, quote):
        found = single(part(), ("PN-104", quote), (PROSE,))

        assert reasons(found) == [("quote_not_found", G, 1, None)]
        assert found.reasons[0].text == quote

    def test_quotes_of_a_header_block_are_found_in_its_text(self):
        found = single(
            part("42", "Invoice"), ("**Invoice:** 42", "Acme"), (H_INVOICE, H_VENDOR), unit_id=H_PARTS, run=HEADER_RUN
        )
        fabricated = single(
            part("42", "Invoice"), ("42", "**Invoice:** 43"), (H_INVOICE,), unit_id=H_PARTS, run=HEADER_RUN
        )
        with_own = single(
            part("42", "Invoice"),
            ("**Invoice:** 42", "Acme"),
            (H_BOLT, H_INVOICE, H_VENDOR),
            unit_id=H_PARTS,
            run=HEADER_RUN,
        )

        assert reasons(found) == [("header_only", G, None, None)]  # its quotes are found: only the citation fails
        assert reasons(fabricated) == [("header_only", G, None, None), ("quote_not_found", G, 1, None)]
        assert with_own.grounded


def scan_run(route: str) -> Prepared:
    """One page of two paragraphs, sixty words and ten words; route ``vision`` makes its unit a vision unit."""
    elements = (para(1, 0, words(60)), para(1, 1, words(10, "v")))
    quality = VISION_QUALITY if route == "vision" else page(1).quality
    return prepared(document(page(1, elements, route=route, quality=quality)))


VISION_RUN, DIGITAL_RUN = scan_run("vision"), scan_run("digital")
SCAN, SCAN_TWO = "p0001_e000", "p0001_e001"


def scanned(quote: str, run: Prepared = VISION_RUN, cited: tuple[str, ...] = (SCAN,)) -> GroundedItem:
    return single(part("w0", "Scan"), (quote,), cited, run=run)


def contained(found: int, total: int) -> str:
    """A quote of ``total`` tokens, ``found`` of them the paragraph's (and not one substring of it)."""
    return f"{words(found)} {words(total - found, 'z')}"


class TestVisionContainment:
    def test_the_fixture_units_are_vision_and_digital(self):
        assert [unit.vision for unit in VISION_RUN.units.values()] == [True]
        assert [unit.vision for unit in DIGITAL_RUN.units.values()] == [False]

    @pytest.mark.parametrize(("found", "total", "grounds"), [(42, 50, False), (17, 20, True), (43, 50, True)])
    def test_a_quote_grounds_from_a_containment_of_0_85(self, found, total, grounds):
        assert token_containment(contained(found, total), words(60)) == Fraction(found, total)
        assert (Fraction(found, total) >= VISION_CONTAINMENT) is grounds

        assert scanned(contained(found, total)).grounded is grounds

    def test_a_digital_unit_takes_no_containment(self):
        found = scanned(contained(43, 50), run=DIGITAL_RUN)

        assert reasons(found) == [("quote_not_found", G, 0, None)]

    def test_an_exact_substring_grounds_even_when_it_cuts_tokens(self):
        for quote in ("0 w1 w", "w1 w2"):
            assert single(part("w0", "Scan"), ("w0", quote), (SCAN,), run=VISION_RUN).grounded

    def test_a_quote_without_tokens_grounds_only_as_a_substring(self):
        run = prepared(document(page(1, (para(1, 0, "Total — $"),), route="vision", quality=VISION_QUALITY)))

        assert single(part("Total"), ("Total", "— $"), (SCAN,), run=run).grounded
        assert reasons(single(part("Total"), ("Total", "$ —"), (SCAN,), run=run)) == [("quote_not_found", G, 1, None)]

    def test_repeating_a_word_the_element_holds_once_does_not_pad_the_share(self):
        assert not scanned("w5 " * 9 + "zz").grounded

    @pytest.mark.parametrize(
        ("written", "key", "variant"),
        [
            ("हिन्दी चालान", "हिन्दी", "हुन्दा चोलोन"),
            ("ใบแจ้งหนี้เลขที่ 5", "5", "ใบแจ๊งหนี๋เลขทิ่ 5"),
            ("فَاتُورَة رَقْم", "رَقْم", "فِاتَورُة رُقِم"),
            ("東京都港区六本木一丁目", "東京都", "東京都港区六本木一丁日"),
        ],
    )
    def test_a_quote_whose_vowels_or_one_character_differ_is_not_found(self, written, key, variant):
        run = prepared(document(page(1, (para(1, 0, written),), route="vision", quality=VISION_QUALITY)))
        assert variant not in written

        found = single(part(key), (key, variant), (SCAN,), run=run)

        assert reasons(found) == [("quote_not_found", G, 1, None)]

    def test_containment_is_per_element_never_across_two(self):
        quote = f"{words(5)} {words(5, 'v')}"
        assert token_containment(quote, f"{words(60)} {words(10, 'v')}") == 1

        found = scanned(quote, cited=(SCAN, SCAN_TWO))

        assert reasons(found) == [("quote_not_found", G, 0, None)]


def mixed_document(header: tuple[KeyValue, ...] = ()) -> PDFDocument:
    """A "Scan" section across a digital page 1 and a vision page 2, sixty words on each; ``header`` opens page 1."""
    scope = ("Scan",)
    heading = titled(1, len(header), "Scan")
    digital = Paragraph(**ids(1, len(header) + 1, scope), text=words(60))
    scan = Paragraph(**ids(2, 0, scope), text=words(60, "v"))
    sections = (
        *((Section(heading=None, scope_path=(), page_span=(1, 1), elements=header),) if header else ()),
        Section(heading=heading, scope_path=scope, page_span=(1, 2), elements=(heading, digital, scan)),
    )
    return document(
        page(1, (*header, heading, digital)),
        page(2, (scan,), route="vision", quality=VISION_QUALITY),
        sections=sections,
        header_context=header,
    )


MIXED_RUN = prepared(mixed_document())
(MIXED,) = MIXED_RUN.unit_ids
ON_DIGITAL, ON_VISION = "p0001_e001", "p0002_e000"
HEADED_RUN = prepared(mixed_document((pair(1, 0, "Invoice", words(20)),)))
HEADED_HEADER, HEADED_SCAN = "p0001_e000", "p0002_e000"


def near(prefix: str) -> str:
    """A quote of fifty tokens, 43 of them (0.86) from the sixty words ``words(60, prefix)``, not a substring."""
    return f"{words(43, prefix)} {words(7, 'z')}"


class TestVisionLeniencyIsPerCitedElement:
    def test_the_fixture_is_one_unit_over_a_digital_and_a_vision_page(self):
        assert [(unit.page_numbers, unit.vision) for unit in MIXED_RUN.units.values()] == [((1, 2), True)]
        assert [unit.page_numbers for unit in HEADED_RUN.units.values()][-1] == (1, 2)
        assert HEADED_RUN.envelopes[HEADED_RUN.unit_ids[-1]].header_element_ids == (HEADED_HEADER,)

    def test_an_element_of_a_vision_page_takes_containment(self):
        found = single(part("v0", "Scan"), (near("v"),), (ON_VISION,), unit_id=MIXED, run=MIXED_RUN)

        assert found.grounded

    def test_an_element_of_a_digital_page_takes_the_exact_check_in_a_vision_unit(self):
        found = single(part("w0", "Scan"), (near("w"),), (ON_DIGITAL,), unit_id=MIXED, run=MIXED_RUN)

        assert reasons(found) == [("quote_not_found", G, 0, None)]

    def test_citing_a_vision_element_too_gives_no_containment_in_the_digital_one(self):
        found = single(part("w0", "Scan"), (near("w"),), (ON_DIGITAL, ON_VISION), unit_id=MIXED, run=MIXED_RUN)

        assert reasons(found) == [("quote_not_found", G, 0, None)]

    def test_a_header_block_of_a_digital_page_takes_the_exact_check_in_a_vision_unit(self):
        unit_id = HEADED_RUN.unit_ids[-1]
        header_quote = f"Invoice {words(19)} zz"
        assert token_containment(header_quote, element_text(HEADED_RUN.document.header_context[0])) == Fraction(20, 21)

        found = single(
            part("v0", "Scan"), ("v0 v1", header_quote), (HEADED_SCAN, HEADED_HEADER), unit_id=unit_id, run=HEADED_RUN
        )

        assert reasons(found) == [("quote_not_found", G, 1, None)]


def keyed_run(*texts: str) -> Prepared:
    """One unit of one paragraph per text."""
    return prepared(document(page(1, tuple(para(1, index, text) for index, text in enumerate(texts)))))


class TestStringKeys:
    def test_a_fabricated_key_is_key_not_found_naming_its_field(self):
        found = single(part("PN-105"), ("PN-104",), (PROSE,))  # a true quote cannot hold a fabricated key

        assert reasons(found) == [("key_not_found", G, None, "number"), ("key_not_quoted", G, None, "number")]
        assert (found.status, found.reasons[0].text) == ("ungrounded_value", None)

    def test_a_key_in_no_element_the_model_cited_is_not_found(self):
        found = single(part("PN-7"), ("| PN-7<br>rev B |",), (PROSE,))

        assert reasons(found) == [("quote_not_found", G, 0, None), ("key_not_found", G, None, "number")]

    def test_a_key_only_another_units_element_holds_is_not_found(self):
        found = single(part("PN-999"), ("PN-104", "Part PN-999 lives elsewhere."), (PROSE, ELSEWHERE))

        assert reasons(found) == [
            ("unknown_element", G, 1, None),
            ("quote_not_found", G, 1, None),
            ("key_not_found", G, None, "number"),
        ]

    @pytest.mark.parametrize(
        ("written", "key", "quote"),
        [
            ("Part A*1 ships.", "A*1", "A\\*1"),
            ("Part a\\b ships.", "a\\b", "a\\\\b"),
            ("Part <M8> ships.", "<M8>", "\\<M8\\>"),
            ("Part #4_b [x] ships.", "#4_b [x]", "\\#4\\_b \\[x\\]"),
            ("- PN-1 is first.", "- PN-1", "\\- PN-1"),
        ],
    )
    def test_a_key_is_read_as_characters_from_the_escaped_text(self, written, key, quote):
        run = keyed_run(written)
        assert quote in element_text(next(iter(run.units.values())).elements[0])

        found = single(part(key), (quote,), ("p0001_e000",), run=run)

        assert found.grounded

    @pytest.mark.parametrize("key", ["PN-7 rev B", "PN-7\nrev B", "PN-7  rev B"])
    def test_a_cells_br_reads_as_a_line_break(self, key):
        assert "PN-7<br>rev B" in element_text(E[TABLE])

        assert single(part(key), ("PN-7<br>rev B",), (TABLE,)).grounded

    def test_a_key_compares_under_nfc(self):
        for form in ("NFC", "NFD"):
            assert single(part(unicodedata.normalize(form, "Café menu")), (element_text(E[NFD]),), (NFD,)).grounded

    @pytest.mark.parametrize("key", ["ＰＮ-104", "pn-104", "Pn-104"])
    def test_a_key_is_not_compatibility_folded_or_casefolded(self, key):
        found = single(part(key), ("PN-104",), (PROSE,))

        assert reasons(found) == [
            ("key_not_found", G, None, "number"),
            ("key_not_quoted", G, None, "number"),
        ]  # folded neither against the element nor against the quote

    def test_an_r8_token_in_a_key_is_compared_as_written(self):
        quote = ("Left [U+202E] right",)

        assert single(part("Left [U+202E] right"), quote, (BIDI,)).grounded
        assert reasons(single(part("Left ‮ right"), quote, (BIDI,))) == [
            ("key_not_found", G, None, "number"),
            ("key_not_quoted", G, None, "number"),
        ]

    @pytest.mark.parametrize(
        ("key", "written"),
        [
            ("PN-10", "Part PN-104 ships."),
            ("1234", "Invoice 12345"),
            ("2345", "Invoice 12345"),
            ("A", "Acme Ltd"),
            ("1", "Page 10"),
            ("PN-104", "Part PN-104A ships."),
            ("104", "Part PN104 ships."),
        ],
    )
    def test_a_key_glued_to_an_ascii_letter_or_digit_is_not_found(self, key, written):
        found = single(part(key), (written,), ("p0001_e000",), run=keyed_run(written))

        assert reasons(found) == [
            ("key_not_found", G, None, "number"),
            ("key_not_quoted", G, None, "number"),
        ]  # the same rule against the element and against the quote
        assert found.status == "ungrounded_value"

    @pytest.mark.parametrize(
        ("key", "written"),
        [
            ("PN-104", "Part (PN-104) ships."),
            ("PN-104", "PN-104"),
            ("PN-104", "Parts PN-104, PN-105."),
            ("A", "Grade: A"),
            ("1", "Page 1 of 10"),
            ("12345", "Invoice 12345"),
            ("PN-10", "PN-104 and PN-10 both"),
            ("-104", "Part PN-104"),
        ],
    )
    def test_a_key_set_off_from_ascii_letters_and_digits_is_found(self, key, written):
        assert single(part(key), (written,), ("p0001_e000",), run=keyed_run(written)).grounded

    @pytest.mark.parametrize(
        ("key", "written"),
        [
            ("東京都", "東京都港区六本木"),
            ("港区", "東京都港区六本木"),
            ("12345", "請求書番号は12345です"),
            ("ใบแจ้งหนี้", "ใบแจ้งหนี้เลขที่"),
            ("PN", "PN東京"),
        ],
    )
    def test_a_key_in_an_unspaced_script_is_found_as_a_substring(self, key, written):
        assert single(part(key), (written,), ("p0001_e000",), run=keyed_run(written)).grounded

    @pytest.mark.parametrize("key", ["", "   "])
    def test_a_blank_key_is_never_found(self, key):
        found = single(part(key), ("PN-104",), (PROSE,))

        assert reasons(found) == [("key_not_found", G, None, "number"), ("key_not_quoted", G, None, "number")]

    def test_a_key_in_a_header_block_is_found(self):
        found = single(part("Acme", "Vendor"), ("Acme",), (H_BOLT, H_VENDOR), unit_id=H_PARTS, run=HEADER_RUN)
        missing = single(part("Acme", "Vendor"), ("Acme",), (H_BOLT,), unit_id=H_PARTS, run=HEADER_RUN)

        assert found.grounded
        assert reasons(missing) == [("quote_not_found", G, 0, None), ("key_not_found", G, None, "number")]

    def test_a_model_citing_nothing_grounds_nothing(self):
        found = single(part(), ("PN-104",), ())

        assert reasons(found) == [
            ("no_element_ids", G, None, None),
            ("quote_not_found", G, 0, None),
            ("key_not_found", G, None, "number"),
        ]


def visible(text: str) -> bool:
    """Whether ``text`` holds no code point R8 makes a token (bidi, tag, C1), so it is shown as written."""
    return not any(
        "\u202a" <= char <= "\u202e"
        or "\u2066" <= char <= "\u2069"
        or 0xE0000 <= ord(char) <= 0xE007F
        or 0x80 <= ord(char) <= 0x9F
        for char in text
    )


# A field's text as a document writes it: any printable text R8 leaves as written, line breaks included.
CELL_TEXT = st.text(
    alphabet=st.characters(codec="utf-8", exclude_categories=("Cc", "Cs", "Zl", "Zp"), include_characters="\n"),
    max_size=12,
).filter(visible)


def as_read(text: str) -> str:
    """What a reader reads of ``text`` in a block: its stripped lines, joined by spaces."""
    return " ".join(line.strip() for line in text.splitlines() if line.strip())


HIDDEN_TABLE_RUN = prepared(
    document(
        page(
            1,
            (
                para(1, 0, "Part PN-104 is shown."),
                dataclasses.replace(grid(1, 1, ("Part",), ("PN-555",)), hidden_reasons=("tiny_font",)),
            ),
        )
    )
)
LISTS_RUN = prepared(
    document(
        page(
            1,
            (
                BulletList(**ids(1, 0), items=("Bolt", "Nut"), ordered=True),
                BulletList(**ids(1, 1), items=("Bolt PN-1", "Nut PN-2"), ordered=True, markers=("a)", "b)")),
                BulletList(**ids(1, 2), items=("PN-7\nrev B", "PN-8"), ordered=False),
            ),
        )
    )
)
NUMBERED, LETTERED, MULTILINE = "p0001_e000", "p0001_e001", "p0001_e002"


class TestKeysMatchWithinOneField:
    """A key lies within one field of what the envelope showed: a cell, a key or a value, a heading's text, an item."""

    @pytest.mark.parametrize(
        ("key", "quote", "element_id"),
        [
            ("PN-104 | 1,200", "| PN-104 | 1,200 |", TABLE),
            ("Part | Qty", "| Part | Qty |", TABLE),
            ("1,200 | 12.50", "| 1,200 | 12.50 |", TABLE),
            ("| --- |", "| --- |", TABLE),
            ("**Invoice:** 1,200", "**Invoice:** 1,200", INVOICE),
            ("Invoice:** 1,200", "**Invoice:** 1,200", INVOICE),
            ("# Parts", "# Parts", HEADING),
            ("first item - second item", "- first item\n- second item", LIST),
            ("- second item", "- second item", LIST),
        ],
    )
    def test_a_key_spanning_fields_or_holding_the_renderers_markup_is_not_found(self, key, quote, element_id):
        assert quote in element_text(E[element_id])

        found = single(part(key), (quote,), (element_id,))

        assert reasons(found) == [("key_not_found", G, None, "number")]

    @pytest.mark.parametrize(
        ("key", "quote", "element_id"),
        [
            ("PN-104", "| PN-104 |", TABLE),
            ("Part", "| Part |", TABLE),
            ("Price", "| Price |", TABLE),
            ("PN-7 rev B", "PN-7<br>rev B", TABLE),
            ("Invoice", "**Invoice:**", INVOICE),
            ("1,200", "**Invoice:** 1,200", INVOICE),
            ("Parts", "# Parts", HEADING),
            ("second item", "- second item", LIST),
            ("Figure 1: the PN-104 bolt", "Figure 1: the PN-104 bolt", FIGURE),
        ],
    )
    def test_a_key_within_one_field_is_found(self, key, quote, element_id):
        assert single(part(key), (quote,), (element_id,)).grounded

    def test_an_items_continuation_lines_are_its_own(self):
        assert element_text(LISTS_RUN.document.pages[0].elements[2]) == "- PN-7\n  rev B\n- PN-8"

        assert single(part("PN-7 rev B"), ("- PN-7\n  rev B",), (MULTILINE,), run=LISTS_RUN).grounded
        assert reasons(single(part("rev B - PN-8"), ("rev B\n- PN-8",), (MULTILINE,), run=LISTS_RUN)) == [
            ("key_not_found", G, None, "number")
        ]

    def test_a_list_shown_as_literal_text_keeps_each_entry_with_its_marker(self):
        shown = element_text(LISTS_RUN.document.pages[0].elements[1])
        assert shown == "a) Bolt PN-1\\\nb) Nut PN-2"

        assert single(part("PN-1"), ("Bolt PN-1",), (LETTERED,), run=LISTS_RUN).grounded
        assert single(part("a) Bolt PN-1"), ("a) Bolt PN-1",), (LETTERED,), run=LISTS_RUN).grounded
        assert reasons(single(part("PN-1 b) Nut"), ("Nut PN-2",), (LETTERED,), run=LISTS_RUN)) == [
            ("key_not_found", G, None, "number"),
            ("key_not_quoted", G, None, "number"),
        ]

    def test_a_number_key_is_read_from_the_fields_never_from_a_lists_own_numbering(self):
        assert element_text(LISTS_RUN.document.pages[0].elements[0]) == "1. Bolt\n2. Nut"

        found = single({"count": 2}, ("2. Nut",), (NUMBERED,), schema=COUNTED, run=LISTS_RUN)

        assert reasons(found) == [("key_not_found", G, None, "count")]

    def test_a_number_spanning_two_cells_is_not_read(self):
        run = prepared(document(page(1, (grid(1, 0, ("Qty", "Unit"), ("1", "200")),))))

        assert single({"count": 200}, ("| 1 | 200 |",), ("p0001_e000",), schema=COUNTED, run=run).grounded
        assert reasons(single({"count": 1200}, ("| 1 | 200 |",), ("p0001_e000",), schema=COUNTED, run=run)) == [
            ("key_not_found", G, None, "count"),
            ("key_not_quoted", G, None, "count"),  # nor does the quote, read with the same rule
        ]

    def test_a_key_only_a_hidden_tables_cell_holds_is_not_found(self):
        (unit_id,) = HIDDEN_TABLE_RUN.unit_ids
        quotes = ("Part PN-104 is shown.", "| PN-555 |")

        found = single(part("PN-555"), quotes, ("p0001_e000", "p0001_e001"), unit_id=unit_id, run=HIDDEN_TABLE_RUN)

        assert reasons(found) == [
            ("unknown_element", G, 1, None),
            ("quote_not_found", G, 1, None),
            ("key_not_found", G, None, "number"),
        ]

    def test_a_split_tables_part_shows_the_header_cells_and_only_its_own_rows(self):
        header = single(part("Qty"), ("| Part | Qty |",), ("p0001_e000",), unit_id=PART_TWO, run=SPLIT_RUN)
        other = single(part("PN-005"), ("| PN-005 | 5 |",), ("p0001_e000",), unit_id=PART_TWO, run=SPLIT_RUN)

        assert header.grounded
        assert reasons(other) == [("quote_not_found", G, 0, None), ("key_not_found", G, None, "number")]

    @pytest.mark.parametrize(
        ("element_id", "shown"),
        [
            (TABLE, "| Part | Qty | Price |"),
            (TABLE, "| Part | Qty | Price |\n| --- | --- | --- |\n| PN-104 | 1,200 |"),
            (TABLE, "| Part | Qty | Price |\n| --- | --- | --- |\nPN-104 | 1,200 | 12.50"),
            (INVOICE, "Invoice: 1,200"),
            (HEADING, "Parts"),
            (LIST, "- first item\nsecond item"),
            (LIST, "- first item"),
        ],
    )
    def test_a_text_its_element_model_does_not_render_fails_loud(self, element_id, shown):
        with pytest.raises(RuntimeError, match=f"element {element_id}'s text"):
            grounding_module._fields(E[element_id], shown)

    @given(st.lists(st.lists(CELL_TEXT, min_size=2, max_size=2), min_size=1, max_size=3))
    @settings(max_examples=100, deadline=None)
    def test_each_cell_of_any_table_is_a_field(self, rows):
        run = prepared(document(page(1, (grid(1, 0, ("Part", "Qty"), *(tuple(row) for row in rows)),))))
        lines = element_text(next(iter(run.units.values())).elements[0]).split("\n")

        for row, line in zip(rows, lines[2:], strict=True):
            for cell in row:
                if cell.strip():
                    assert single(part(as_read(cell)), (line,), ("p0001_e000",), run=run).grounded, (cell, line)

    @given(st.lists(CELL_TEXT.filter(str.strip), min_size=1, max_size=3), st.booleans())
    @settings(max_examples=100, deadline=None)
    def test_each_item_of_any_list_is_a_field(self, items, ordered):
        run = prepared(document(page(1, (BulletList(**ids(1, 0), items=tuple(items), ordered=ordered),))))
        shown = element_text(next(iter(run.units.values())).elements[0])

        for item in items:
            assert single(part(as_read(item)), (shown,), ("p0001_e000",), run=run).grounded, (item, shown)

    @given(CELL_TEXT.filter(str.strip), CELL_TEXT)
    @settings(max_examples=100, deadline=None)
    def test_the_key_and_the_value_of_any_key_value_are_fields(self, key, value):
        run = prepared(document(page(1, (pair(1, 0, key, value),))))
        shown = element_text(next(iter(run.units.values())).elements[0])

        for written in (key, value):
            if written.strip():
                assert single(part(as_read(written)), (shown,), ("p0001_e000",), run=run).grounded, (written, shown)


def counted(count: int, *texts: str, quote: str | None = None) -> GroundedItem:
    run = keyed_run(*texts)
    cited = tuple(f"p0001_e{index:03d}" for index in range(len(texts)))
    return single({"count": count}, (quote or texts[0],), cited, schema=COUNTED, run=run)


def priced(price: str, *texts: str) -> GroundedItem:
    run = keyed_run(*texts)
    cited = tuple(f"p0001_e{index:03d}" for index in range(len(texts)))
    return single({"price": price}, (texts[0],), cited, schema=PRICED, run=run)


NUMBER_NOT_WRITTEN = [("key_not_found", G, None, "count"), ("key_not_quoted", G, None, "count")]
"""A count no number of its element or its quote (the same text here) equals: the same rule against both."""


class TestNumericKeys:
    @pytest.mark.parametrize("written", ["Qty 1,200 units", "Qty 1200 units", "Qty 1,200.00", "#1200", "1,200."])
    def test_an_int_key_matches_a_plain_or_grouped_number(self, written):
        assert counted(1200, written).grounded

    @pytest.mark.parametrize("written", ["Qty 1,201 units", "Qty 12,00", "1.200,50", "PN1200", "1200V", "12 00"])
    def test_an_int_key_no_number_of_the_text_equals_is_not_found(self, written):
        assert reasons(counted(1200, written)) == NUMBER_NOT_WRITTEN

    @pytest.mark.parametrize(
        ("price", "written"), [("12.50", "Price 12.50"), ("12.5", "Price 12.50"), ("12.50", "12.5")]
    )
    def test_a_decimal_key_matches_by_value(self, price, written):
        assert priced(price, written).grounded

    def test_a_decimal_key_that_differs_in_a_digit_is_not_found(self):
        price_not_written = [("key_not_found", G, None, "price"), ("key_not_quoted", G, None, "price")]

        assert reasons(priced("12.51", "Price 12.50")) == price_not_written
        assert reasons(priced("1200.50", "1.200,50")) == price_not_written

    def test_a_negative_key_needs_its_minus(self):
        assert counted(-42, "Delta -42").grounded
        assert counted(42, "Delta -42").grounded
        assert reasons(counted(-42, "Delta 42")) == NUMBER_NOT_WRITTEN

    def test_a_number_is_read_from_the_escaped_text_and_a_tables_cells(self):
        assert element_text(E[TABLE]).splitlines()[3].endswith("| \\-42 | 1.200,50 |")

        assert single({"count": -42}, ("\\-42",), (TABLE,), schema=COUNTED).grounded
        assert single({"count": 1200}, ("1,200",), (INVOICE,), schema=COUNTED).grounded

    def test_grounding_a_long_run_of_comma_joined_digits_takes_linear_time(self):
        def run_of(length: int) -> Prepared:
            return prepared(document(page(1, (para(1, 0, "1," * (length // 2) + "1x"),))), limit=100_000)

        small, large = run_of(40_000), run_of(80_000)

        def ground_count(run: Prepared) -> GroundedItem:
            return single({"count": 7}, ("1,1",), ("p0001_e000",), schema=COUNTED, run=run)

        small_s, large_s = best_of(lambda: ground_count(small), 3), best_of(lambda: ground_count(large), 3)

        assert reasons(ground_count(large)) == NUMBER_NOT_WRITTEN
        assert large_s <= 3 * small_s, f"40k chars: {small_s:.4f} s; 80k chars: {large_s:.4f} s"

    def test_a_number_in_an_element_the_model_did_not_cite_is_not_found(self):
        found = single({"count": 1200}, ("**Invoice:** 1,200",), (PROSE,), schema=COUNTED)

        assert reasons(found) == [("quote_not_found", G, 0, None), ("key_not_found", G, None, "count")]


class TestKeysLieInTheModelsOwnQuotes:
    @pytest.mark.parametrize("quote", ["is the", "e", "."])
    def test_a_key_its_element_holds_but_no_quote_of_the_model_does_is_key_not_quoted(self, quote):
        found = single(part(), (quote,), (PROSE,))

        assert reasons(found) == [("key_not_quoted", G, None, "number")]
        assert (found.status, found.reasons[0].text) == ("ungrounded_value", None)

    def test_a_key_in_any_one_of_the_quotes_is_quoted(self):
        assert single(part(), ("is the", "Part PN-104"), (PROSE,)).grounded

    def test_a_key_is_read_from_the_quote_as_characters(self):
        run = keyed_run("Part <M8> ships.")

        assert single(part("<M8>"), ("\\<M8\\>",), ("p0001_e000",), run=run).grounded

    def test_a_key_glued_to_an_ascii_letter_or_digit_in_the_quote_is_not_quoted(self):
        run = keyed_run("Parts PN-10 and PN-104.")

        found = single(part("PN-10"), ("PN-104.",), ("p0001_e000",), run=run)

        assert reasons(found) == [("key_not_quoted", G, None, "number")]

    def test_a_number_key_is_read_from_the_quote_as_a_number(self):
        assert counted(1200, "Ships today.", "Qty 1,200 units", quote="Qty 1,200").grounded
        assert reasons(counted(1200, "Ships today.", "Qty 1,200 units", quote="Ships today.")) == [
            ("key_not_quoted", G, None, "count")
        ]
        assert reasons(counted(1200, "Qty 1,200 units", "PN1200 x", quote="Qty 1,200 units")) == []
        assert reasons(counted(1200, "Qty 1,2000 units", "Qty 1,200 units", quote="Qty 1,2000 units")) == [
            ("key_not_quoted", G, None, "count")
        ]

    def test_citing_every_element_quotes_no_key(self):
        cited = (HEADING, PROSE, INVOICE, TABLE, LIST, FIGURE, BIDI, NFD)

        found = single(part("PN-7", "x"), ("first item",), cited)  # its key is the table's, its quote the list's

        assert reasons(found) == [("key_not_quoted", G, None, "number")]

    def test_another_models_quote_never_quotes_a_models_key(self):
        (found,) = both(GOOD, entry(part(), ("is the",), (PROSE,)))

        assert reasons(found) == [("key_not_quoted", A, None, "number")]

    def test_a_blank_key_is_neither_found_nor_quoted(self):
        found = single(part(" "), ("PN-104",), (PROSE,))

        assert reasons(found) == [("key_not_found", G, None, "number"), ("key_not_quoted", G, None, "number")]


class TestKeysGroundedByQuotesOnly:
    @pytest.mark.parametrize(
        ("schema", "record", "written", "quote"),
        [
            (DATED, {"day": "2026-11-05"}, "Shipped 5 Nov 2026.", "5 Nov 2026"),
            (SHADED, {"shade": "red"}, "A Red finish.", "A Red finish"),
            (SIZED, {"size": "M"}, "Size: medium, fits most.", "Size: medium, fits"),
        ],
    )
    def test_a_key_the_document_writes_otherwise_grounds_through_its_quotes(self, schema, record, written, quote):
        found = single(record, (quote,), ("p0001_e000",), schema=schema, run=keyed_run(written))

        assert (found.status, found.reasons) == ("single_model", ())

    @pytest.mark.parametrize(
        ("schema", "record"), [(DATED, {"day": "2026-11-05"}), (SHADED, {"shade": "red"}), (SIZED, {"size": "M"})]
    )
    def test_without_grounded_quotes_it_is_ungrounded_by_its_quotes_alone(self, schema, record):
        found = single(record, ("a quote nowhere",), ("p0001_e000",), schema=schema, run=keyed_run("Some text."))

        assert reasons(found) == [("quote_not_found", G, 0, None)]
        assert found.status == "ungrounded_value"


UNSPACED_RUN = keyed_run("東京都港区六本木一丁目", "ใบแจ้งหนี้เลขที่ 5", "請求書番号は12345です", "ใบ ๕ ฉบับ")


def line(quotes: tuple[str, ...], cited: tuple[str, ...] = (PROSE,), run: Prepared = RUN) -> GroundedItem:
    """The item of one keyless record (no key is checked), quoting ``quotes`` and citing ``cited``."""
    return single({"text": "a line"}, quotes, cited, schema=LINE, run=run)


class TestQuoteLengthWithoutACheckedKey:
    def test_the_minimum_is_three_tokens(self):
        assert QUOTE_MIN_TOKENS == 3

    @pytest.mark.parametrize("quote", ["e", ".", "a", "PN-104", "\\<M8\\>"])
    def test_a_keyless_record_whose_quotes_are_all_shorter_than_three_tokens_is_quote_too_short(self, quote):
        assert quote in element_text(E[PROSE])

        found = line((quote,))

        assert reasons(found) == [("quote_too_short", G, None, None)]
        assert (found.item.status, found.status) == ("single_model", "ungrounded_value")

    @pytest.mark.parametrize(
        ("schema", "record", "written"),
        [
            (DATED, {"day": "2026-11-05"}, "Shipped 5 Nov 2026."),
            (SHADED, {"shade": "red"}, "A Red finish."),
            (SIZED, {"size": "M"}, "Size: medium."),
        ],
    )
    def test_a_record_whose_keys_get_no_check_needs_a_quote_of_three_tokens(self, schema, record, written):
        found = single(record, ("e",), ("p0001_e000",), schema=schema, run=keyed_run(written))

        assert reasons(found) == [("quote_too_short", G, None, None)]

    def test_three_tokens_in_one_quote_is_enough(self):
        assert line(("Part PN-104",)).grounded
        assert line(("e", "Part PN-104")).grounded
        assert reasons(line(("Part PN", "PN-104"))) == [("quote_too_short", G, None, None)]

    @pytest.mark.parametrize(
        ("quote", "element_id"),
        [
            ("東京都港区六本木", "p0001_e000"),
            ("東京都", "p0001_e000"),
            ("ใบแจ้งหนี้", "p0001_e001"),
            ("請求書番号は12345です", "p0001_e002"),
            ("は12345で", "p0001_e002"),
            ("ใบ ๕", "p0001_e003"),
        ],
    )
    def test_a_quote_in_an_unspaced_script_counts_each_letter(self, quote, element_id):
        assert line((quote,), (element_id,), UNSPACED_RUN).grounded

    @pytest.mark.parametrize(
        ("quote", "element_id"), [("港区", "p0001_e000"), ("จ้ง", "p0001_e001"), ("は12345", "p0001_e002")]
    )
    def test_a_quote_of_two_letters_of_an_unspaced_script_is_too_short(self, quote, element_id):
        assert [unicodedata.category(char) for char in "จ้ง"] == ["Lo", "Mn", "Lo"]  # a tone mark is no letter

        assert reasons(line((quote,), (element_id,), UNSPACED_RUN)) == [("quote_too_short", G, None, None)]

    def test_a_record_with_a_checked_key_takes_no_length_rule(self):
        assert single(part(), ("PN-104",), (PROSE,)).grounded

    def test_a_model_without_a_quote_is_no_quotes_alone(self):
        assert reasons(line(())) == [("no_quotes", G, None, None)]

    def test_quote_too_short_comes_after_each_quote_not_found(self):
        assert reasons(line(("e", "nowhere"))) == [("quote_not_found", G, 1, None), ("quote_too_short", G, None, None)]


def both(google: dict[str, Any], anthropic: dict[str, Any], schema: PreparedSchema = PART) -> list[GroundedItem]:
    """The items of one entry from each family on unit 1."""
    responses = {(U1, G): response(google), (U1, A): response(anthropic)}
    return list(grounded(responses, models=PAIR, schema=schema).items)


GOOD = entry(part(), ("PN-104",), (PROSE,))


class TestEverySupportingModelGroundsItsOwnEvidence:
    def test_each_family_may_ground_from_its_own_elements(self):
        (found,) = both(GOOD, entry(part(), ("| PN-104 |",), (TABLE,)))

        assert (found.status, found.grounded) == ("consensus", True)

    @pytest.mark.parametrize(
        ("anthropic", "expected"),
        [
            (entry(part(), ("PN-104 is the nut",), (PROSE,)), [("quote_not_found", A, 0, None)]),
            (entry(part(), ("PN-104",), (PROSE, "p0001_e099")), [("unknown_element", A, 1, None)]),
            (entry(part(), (), (PROSE,)), [("no_quotes", A, None, None)]),
            (
                entry(part(), ("1,200",), (INVOICE,)),
                [("key_not_found", A, None, "number"), ("key_not_quoted", A, None, "number")],
            ),
        ],
    )
    def test_one_models_ungrounded_evidence_demotes_a_consensus_record(self, anthropic, expected):
        (found,) = both(GOOD, anthropic)

        assert found.item.status == "consensus"
        assert (found.status, reasons(found)) == ("ungrounded_value", expected)

    def test_another_models_evidence_never_vouches_for_a_model(self):
        (found,) = both(GOOD, entry(part(), ("first item",), (LIST,)))  # its element holds no part number
        (borrowed,) = both(GOOD, entry(part(), ("PN-104",), (LIST,)))  # its element holds neither

        assert reasons(found) == [("key_not_found", A, None, "number"), ("key_not_quoted", A, None, "number")]
        assert reasons(borrowed) == [("quote_not_found", A, 0, None), ("key_not_found", A, None, "number")]

    def test_reasons_come_in_model_order(self):
        (found,) = both(entry(part(), ("PN-104 nowhere",), (PROSE,)), entry(part(), (), (PROSE,)))

        assert reasons(found) == [("quote_not_found", G, 0, None), ("no_quotes", A, None, None)]

    def test_a_model_without_a_record_in_the_item_is_not_checked(self):
        found = both(GOOD, entry(part("PN-7"), ("PN-7",), (TABLE,)))

        assert [(each.item.key, each.item.status, each.status, each.grounded) for each in found] == [
            (("PN-104",), "missing_in_some_models", "missing_in_some_models", True),
            (("PN-7",), "missing_in_some_models", "missing_in_some_models", True),
        ]
        assert found[0].item.quotes_by_model[A] == () and found[0].item.element_ids_by_model[A] == ()


class TestStatuses:
    def test_a_single_model_record_that_fails_is_ungrounded_value(self):
        found = single(part("PN-105"), ("PN-104",), (PROSE,))

        assert (found.item.status, found.status, found.grounded) == ("single_model", "ungrounded_value", False)

    def test_a_missing_in_some_models_item_keeps_its_status_and_gains_reasons(self):
        found = both(entry(part(), ("PN-104 nowhere",), (PROSE,)), entry(part("PN-7"), ("PN-7",), (TABLE,)))[0]

        assert (found.item.status, found.status, found.grounded) == (
            "missing_in_some_models",
            "missing_in_some_models",
            False,
        )
        assert reasons(found) == [("quote_not_found", G, 0, None)]

    def test_a_field_disagreement_keeps_its_status_and_gains_reasons(self):
        (found,) = both(GOOD, entry(part(title="Nut"), ("PN-104 nowhere",), (PROSE,)))

        assert (found.item.status, found.status) == ("field_disagreement", "field_disagreement")
        assert reasons(found) == [("quote_not_found", A, 0, None)]

    def test_an_unmatched_item_keeps_its_status_and_gains_reasons(self):
        lines = (
            entry({"text": "a"}, ("Part PN-104 is",), (PROSE,)),
            entry({"text": "b"}, ("a quote nowhere",), (PROSE,)),
        )
        responses = {(U1, G): response(*lines), (U1, A): response(lines[0])}

        found = grounded(responses, models=PAIR, schema=LINE).items

        assert [(each.item.status, each.status, reasons(each)) for each in found] == [
            ("unmatched", "unmatched", []),
            ("unmatched", "unmatched", [("quote_not_found", G, 0, None)]),
            ("unmatched", "unmatched", []),
        ]

    def test_a_root_without_keys_is_grounded_by_its_quotes_and_ids(self):
        answer = entry({"text": "a"}, ("Part PN-104 is",), (PROSE,))

        (found,) = both(answer, answer, LINE)

        assert (found.item.key, found.status, found.grounded) == ((), "consensus", True)


SPLIT_TABLE = Table(
    **ids(1, 0),
    headers=("Part", "Qty"),
    rows=tuple((f"PN-{index:03d}", str(index)) for index in range(40)),
    extraction_method="lattice",
)
SPLIT_RUN = prepared(document(page(1, (SPLIT_TABLE,))), limit=400)
PART_ONE, PART_TWO = SPLIT_RUN.unit_ids


class TestSplitTables:
    def test_the_fixture_splits_one_table_into_two_units(self):
        assert [unit.part_of for unit in SPLIT_RUN.units.values()] == [((1, 2),), ((2, 2),)]
        assert [unit.elements[0].element_id for unit in SPLIT_RUN.units.values()] == ["p0001_e000"] * 2

    def test_a_quote_and_key_from_the_part_the_unit_holds_ground(self):
        found = single(part("PN-025"), ("| PN-025 | 25 |",), ("p0001_e000",), unit_id=PART_TWO, run=SPLIT_RUN)

        assert found.grounded

    def test_a_quote_or_key_from_the_other_part_is_not_found(self):
        found = single(part("PN-005"), ("| PN-005 | 5 |",), ("p0001_e000",), unit_id=PART_TWO, run=SPLIT_RUN)

        assert reasons(found) == [("quote_not_found", G, 0, None), ("key_not_found", G, None, "number")]


THREE_RUN = prepared(document(*(page(number, (para(number, 0, f"Page {number} text."),)) for number in (1, 2, 3))))
T1, T2, T3 = THREE_RUN.unit_ids
NOTHING = response()


def failure(unit_id: str, model: tuple[str, str, str]) -> FailedUnit:
    return FailedUnit(unit_id=unit_id, model_id=model[0], status_code=503, error_message="unavailable", attempts=3)


def empty_units(
    responses: dict[tuple[str, str], ProviderResponse],
    models: tuple[tuple[str, str, str], ...] = PAIR,
    failed: tuple[FailedUnit, ...] = (),
) -> tuple[str, ...]:
    return grounded(responses, models=models, run=THREE_RUN, failed=failed).empty_units


class TestEmptyUnits:
    def test_a_unit_every_answering_model_found_nothing_in_is_empty(self):
        record = response(entry(part("Page"), ("Page 1",), ("p0001_e000",)))

        assert empty_units({(T1, G): record, (T1, A): record, (T2, G): NOTHING, (T2, A): NOTHING}) == (T2,)

    def test_empty_units_come_in_the_order_of_units(self):
        responses = {(unit_id, model[0]): NOTHING for unit_id in (T3, T1, T2) for model in PAIR}
        run = THREE_RUN
        outcome = outcome_of(responses, PAIR)
        consensus = build_consensus(outcome, PART, run.unit_ids, min_families=2)
        reordered = {unit_id: run.units[unit_id] for unit_id in (T2, T3, T1)}

        result = ground(
            consensus,
            outcome=outcome,
            units=reordered,
            envelopes=run.envelopes,
            documents=documents_of(run),
            schema=PART,
        )

        assert result.empty_units == (T2, T3, T1) and result.items == ()

    def test_a_unit_no_model_answered_is_not_empty(self):
        assert empty_units({(T1, G): NOTHING, (T1, A): NOTHING}) == (T1,)

    def test_a_unit_every_model_failed_on_is_a_failed_unit_not_an_empty_one(self):
        assert empty_units({}, failed=(failure(T1, GOOGLE), failure(T1, ANTHROPIC))) == ()

    def test_a_unit_one_model_found_nothing_in_and_another_failed_on_is_empty(self):
        assert empty_units({(T1, G): NOTHING}, failed=(failure(T1, ANTHROPIC),)) == (T1,)

    @pytest.mark.parametrize(
        "other",
        [
            response(finish="truncated"),
            response(finish="refused"),
            ProviderResponse(
                content=None, raw_text="not json", finish="complete", input_tokens=None, output_tokens=None
            ),
            response(entry({"number": 7}, ("q",), ("p0001_e000",))),
            response({"record": {}, "quotes": []}),
        ],
    )
    def test_a_unit_with_a_quarantined_response_or_record_is_not_empty(self, other):
        result = grounded({(T1, G): NOTHING, (T1, A): other}, models=PAIR, run=THREE_RUN)

        assert result.empty_units == () and result.items == ()

    def test_a_unit_with_a_record_from_one_model_is_not_empty(self):
        record = response(entry(part("Page"), ("Page 1",), ("p0001_e000",)))

        assert empty_units({(T1, G): NOTHING, (T1, A): record}) == ()

    def test_a_single_model_run_finds_empty_units_too(self):
        assert empty_units({(T1, G): NOTHING, (T2, G): NOTHING}, models=(GOOGLE,)) == (T1, T2)


def call_ground(**overrides: Any) -> GroundingResult:
    """``ground`` over one grounded record of unit 1, with any argument replaced."""
    outcome = outcome_of({(U1, G): response(GOOD)}, (GOOGLE,))
    arguments: dict[str, Any] = {
        "consensus": build_consensus(outcome, PART, RUN.unit_ids, min_families=1),
        "outcome": outcome,
        "units": RUN.units,
        "envelopes": RUN.envelopes,
        "documents": documents_of(RUN),
        "schema": PART,
        **overrides,
    }
    consensus = arguments.pop("consensus")
    return ground(consensus, **arguments)


def consensus_of(responses: dict[tuple[str, str], ProviderResponse], schema: PreparedSchema = PART) -> ConsensusResult:
    return build_consensus(outcome_of(responses, (GOOGLE,)), schema, RUN.unit_ids, min_families=1)


class TestGroundArguments:
    def test_the_defaults_ground(self):
        (found,) = call_ground().items

        assert found.grounded

    @pytest.mark.parametrize(
        "overrides",
        [
            {"consensus": ()},
            {"outcome": {}},
            {"units": list(RUN.units.values())},
            {"units": {1: RUN.units[U1], U2: RUN.units[U2]}},
            {"units": {U1: "unit", U2: RUN.units[U2]}},
            {"envelopes": list(RUN.envelopes.values())},
            {"envelopes": {U1: "envelope", U2: RUN.envelopes[U2]}},
            {"schema": Part},
        ],
    )
    def test_rejects_a_wrong_type(self, overrides):
        with pytest.raises(TypeError):
            call_ground(**overrides)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"units": {U1: RUN.units[U2], U2: RUN.units[U1]}}, "is not its unit's id"),
            ({"envelopes": {U1: RUN.envelopes[U2], U2: RUN.envelopes[U1]}}, "is not its envelope's id"),
            ({"envelopes": {U1: RUN.envelopes[U1]}}, "the same unit ids"),
            ({"consensus": ConsensusResult(items=(), quarantined=())}, "names the unit"),
        ],
    )
    def test_rejects_mappings_that_do_not_fit(self, overrides, message):
        if "consensus" in overrides:  # a response for a unit units does not name
            overrides["outcome"] = outcome_of({("doc:p0009_e000-p0009_e000", G): NOTHING}, (GOOGLE,))
        with pytest.raises(ValueError, match=message):
            call_ground(**overrides)

    def test_rejects_an_item_of_a_unit_units_does_not_name(self):
        consensus = consensus_of({(U2, G): response(entry(part("PN-999"), ("PN-999",), (ELSEWHERE,)))})

        with pytest.raises(ValueError, match="names the unit"):
            call_ground(consensus=consensus, units={U1: RUN.units[U1]}, envelopes={U1: RUN.envelopes[U1]})

    def test_rejects_a_quarantined_item_of_a_unit_units_does_not_name(self):
        consensus = consensus_of({(U2, G): response(finish="truncated")})

        with pytest.raises(ValueError, match="names the unit"):
            call_ground(consensus=consensus, units={U1: RUN.units[U1]}, envelopes={U1: RUN.envelopes[U1]})

    def test_rejects_items_of_other_models_than_the_outcomes(self):
        other = outcome_of({(U1, A): response(GOOD)}, (ANTHROPIC,))

        with pytest.raises(ValueError, match="models"):
            call_ground(outcome=other)

    @pytest.mark.parametrize("schema", [LINE, COUNTED])
    def test_rejects_items_of_another_schema(self, schema):
        with pytest.raises(ValueError, match="schema"):
            call_ground(schema=schema)

    def test_rejects_a_key_value_of_the_wrong_type(self):
        (item,) = call_ground().items
        wrong = dataclasses.replace(item.item, key=(1200,))

        with pytest.raises(ValueError, match="key"):
            call_ground(consensus=ConsensusResult(items=(wrong,), quarantined=()))

    def test_rejects_a_header_block_no_unit_given_holds(self):
        units = {H_PARTS: HEADER_RUN.units[H_PARTS]}
        envelopes = {H_PARTS: HEADER_RUN.envelopes[H_PARTS]}
        outcome = outcome_of({}, (GOOGLE,))

        with pytest.raises(ValueError, match="no unit given"):
            ground(
                ConsensusResult(items=(), quarantined=()),
                outcome=outcome,
                units=units,
                envelopes=envelopes,
                documents=documents_of(HEADER_RUN),
                schema=PART,
            )


class ItemsOnly:
    """A stand-in with a mapping's ``items()`` that is no Mapping."""

    def __init__(self, inner: dict[str, Any]) -> None:
        self._inner = inner

    def items(self) -> Any:
        return self._inner.items()


DOC_ID = DOC.metadata.doc_id
EMPTY = ConsensusResult(items=(), quarantined=())


def ground_nothing(run: Prepared, documents: Any) -> GroundingResult:
    """``ground`` over no item of ``run``'s units, with ``documents``."""
    outcome = outcome_of({}, (GOOGLE,))
    return ground(EMPTY, outcome=outcome, units=run.units, envelopes=run.envelopes, documents=documents, schema=PART)


class TestGroundDocuments:
    @pytest.mark.parametrize(
        "documents",
        [
            ItemsOnly({DOC_ID: DOC}),
            {1: DOC, DOC_ID: DOC},
            {DOC_ID: SimpleNamespace(metadata=DOC.metadata, pages=DOC.pages)},
            {DOC_ID: DOC.metadata},
            [DOC],
        ],
    )
    def test_rejects_a_wrong_type(self, documents):
        with pytest.raises(TypeError, match="documents"):
            call_ground(documents=documents)

    def test_rejects_a_document_under_another_id(self):
        with pytest.raises(ValueError, match="is not its document's id"):
            call_ground(documents={"report-0000": DOC, DOC_ID: DOC})

    def test_rejects_documents_without_a_units_document(self):
        with pytest.raises(ValueError, match="documents does not hold"):
            ground_nothing(RUN, {})

    def test_rejects_a_unit_of_a_page_its_document_does_not_have(self):
        one_page = DIGITAL_RUN.document  # the same doc id, and a digital page 1 as RUN's: page 2 alone is missing
        assert one_page.metadata.doc_id == DOC_ID and [page.route for page in one_page.pages] == ["digital"]

        with pytest.raises(ValueError, match="not of its document's pages"):
            ground_nothing(RUN, documents_of(DIGITAL_RUN))

    @pytest.mark.parametrize(("run", "other"), [(VISION_RUN, DIGITAL_RUN), (DIGITAL_RUN, VISION_RUN)])
    def test_rejects_a_unit_whose_vision_flag_its_documents_pages_do_not_give(self, run, other):
        with pytest.raises(ValueError, match="not of its document's pages"):
            ground_nothing(run, documents_of(other))

    def test_a_document_no_unit_is_of_may_be_given(self):
        other = header_run_of("other.pdf", "42")

        assert ground_nothing(RUN, {**documents_of(RUN), **documents_of(other)}).items == ()


def header_run_of(source_name: str, invoice: str) -> Prepared:
    """A one-page document whose header key-value holds ``invoice``, then a "Parts" section."""
    scope = ("Parts",)
    header = pair(1, 0, "Invoice", invoice)
    heading = titled(1, 1, "Parts")
    bolt = Paragraph(**ids(1, 2, scope), text="Part PN-104 is the bolt.")
    sections = (
        Section(heading=None, scope_path=(), page_span=(1, 1), elements=(header,)),
        Section(heading=heading, scope_path=scope, page_span=(1, 1), elements=(heading, bolt)),
    )
    doc = PDFDocument(
        metadata=metadata(1, source_name=source_name),
        pages=(page(1, (header, heading, bolt)),),
        sections=sections,
        header_context=(header,),
    )
    return prepared(doc)


class TestSeveralDocuments:
    def test_a_header_block_grounds_against_its_own_documents_text(self):
        first, second = header_run_of("first.pdf", "42"), header_run_of("second.pdf", "43")
        units = {**first.units, **second.units}
        envelopes = {**first.envelopes, **second.envelopes}
        (_, first_parts), (_, second_parts) = first.unit_ids, second.unit_ids
        assert first.envelopes[first_parts].header_element_ids == second.envelopes[second_parts].header_element_ids
        responses = {
            (first_parts, G): response(
                entry(part("42", "Invoice"), ("**Invoice:** 42",), ("p0001_e002", "p0001_e000"))
            ),
            (second_parts, G): response(
                entry(part("43", "Invoice"), ("**Invoice:** 43",), ("p0001_e002", "p0001_e000"))
            ),
        }
        outcome = outcome_of(responses, (GOOGLE,))
        consensus = build_consensus(outcome, PART, tuple(units), min_families=1)

        result = ground(
            consensus,
            outcome=outcome,
            units=units,
            envelopes=envelopes,
            documents=documents_of(first, second),
            schema=PART,
        )

        assert [(found.item.unit_id, found.grounded) for found in result.items] == [
            (first_parts, True),
            (second_parts, True),
        ]


class Other(BaseModel):
    """A schema whose key has Part's shape (one str), but another model."""

    number: Annotated[str, Key]
    size: int


OTHER = prepare(Other)


class TestItemsOfAnotherSchema:
    def test_rejects_a_record_of_another_model_with_the_same_key_shape(self):
        with pytest.raises(ValueError, match="not the schema's Other"):
            call_ground(schema=OTHER)

    def test_rejects_a_key_of_another_length_when_the_item_has_no_record(self):
        (found,) = both(GOOD, entry(part(title="Nut"), ("PN-104",), (PROSE,)))
        assert (found.item.status, found.item.record) == ("field_disagreement", None)
        outcome = outcome_of({(U1, G): response(GOOD), (U1, A): response(GOOD)}, PAIR)
        consensus = ConsensusResult(items=(found.item,), quarantined=())

        with pytest.raises(ValueError, match="key has 1 part"):
            ground(
                consensus,
                outcome=outcome,
                units=RUN.units,
                envelopes=RUN.envelopes,
                documents=documents_of(RUN),
                schema=LINE,
            )

    def test_a_hand_built_items_key_is_compared_under_the_same_rule(self):
        (found,) = call_ground().items
        unnormalized = dataclasses.replace(found.item, key=("  PN-104 ",))

        (regrounded,) = call_ground(consensus=ConsensusResult(items=(unnormalized,), quarantined=())).items

        assert regrounded.grounded


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_the_element_model_and_its_siblings_and_nothing_inside_functions():
    tree = ast.parse(Path(grounding_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    allowed = {
        "datagrunt.pdf.elements",
        "datagrunt.rationalization.consensus",
        "datagrunt.rationalization.envelope",
        "datagrunt.rationalization.runner",
        "datagrunt.rationalization.schema",
        "datagrunt.rationalization.units",
    }

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert [m for m in modules if m not in allowed and m.split(".")[0] not in sys.stdlib_module_names] == []


def test_imports_public_normalized_text_from_consensus_rather_than_private_normalized():
    assert not hasattr(grounding_module, "_normalized")
    assert grounding_module.normalized_text is normalized_text


def test_docstrings_match_key_field_reasons_and_vision_page_tokens():
    reason_doc = GroundingReason.__doc__ or ""
    key_field_lines = [line for line in reason_doc.splitlines() if "key_field:" in line]
    tokens_doc = grounding_module._UnitTexts.tokens.__doc__ or ""

    assert len(key_field_lines) == 1
    assert "key_not_found" in key_field_lines[0] and "key_not_quoted" in key_field_lines[0]
    assert "vision page" in tokens_doc and "vision unit" not in tokens_doc
