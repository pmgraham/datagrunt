"""Grounding: the anti-hallucination gate over consensus items (plan §4.8 as amended, the 2026-11-05 decisions).

:func:`ground` takes a :class:`~datagrunt.rationalization.consensus.ConsensusResult` and checks, for each item, that
what every model backing it claimed is written where that model said it is. Each item becomes a
:class:`GroundedItem`: its consensus item, its status after grounding, and why it is not grounded (its reasons,
``()`` when it is). Nothing is dropped. A *supporting* model of an item is one with a record in its
``model_votes``; each is checked on its own, from its own quotes and element ids (they are per model, and not
paired with each other). The rules, in order:

1. **Citable elements.** A model may cite the elements of the item's unit (for a split table or list, the part of
   it the unit holds) and the header-context blocks the unit's envelope shows (``Envelope.header_element_ids``).
   Each other id it cites is ``unknown_element`` (naming its position among the model's ids) and gives no text. A
   model that cites no id at all is ``no_element_ids``. A model must cite at least one of the unit's own elements:
   one whose citable ids are all header blocks is ``header_only``. A header block repeats in every unit's envelope,
   so a record grounded only there would ground in every unit, and SYSTEM_RULES rule 1 asks for the records the
   unit's own blocks show (header blocks only complete their fields).
2. **Quotes.** A supporting model gives at least one quote, else ``no_quotes``. Each quote must lie within the text
   of one element that same model cited (any of them), never across two (envelope rule 3: each quote within one
   block), else ``quote_not_found`` (naming its position among the model's quotes). An element's text is
   :func:`~datagrunt.rationalization.envelope.element_text`, exactly the block body the model read, with the
   payload's Markdown backslash escapes, datagrunt's ``<br>`` cell breaks and R8's ``[U+XXXX]`` tokens: envelope
   rules 6 and 7 ask for quotes copied exactly as written, so a quote that drops an escape is not found (the safe
   direction: its record goes to review). Both sides are compared under consensus's string rule (NFC, stripped,
   every run of Unicode whitespace one space), never a compatibility fold (NFKC) or a casefold. In a cited element
   on a vision page (its page's route in ``documents``) a quote is also found when its token containment in that
   element (:func:`token_containment`) is at least :data:`VISION_CONTAINMENT`; in an element of any other page
   only the substring counts. The cited element's own page decides, never the unit's: in a unit of digital and
   vision pages, a digital page's element, or a page-1 header block of a digital page, gets the exact check. A
   quote that is blank once normalized is never found. When no key of the record is checked (a root without keys,
   or only date, datetime, Enum or Literal keys: see 3), one of the model's quotes must hold at least
   :data:`QUOTE_MIN_TOKENS` tokens of :func:`token_containment`'s kind, else ``quote_too_short``, so ``"e"``
   grounds nothing. For this count only, a token of a script written without spaces (Han, Hiragana, Katakana, Thai,
   Lao, Khmer, Myanmar, by Unicode block) counts each of that script's letters (category L, never a mark) and each
   run of its other characters, so ``東京都`` counts 3 and ``請求書番号は12345です`` 9; containment keeps whole
   tokens.
3. **Key values.** Each root key value (the item's normalized key, consensus's
   :func:`~datagrunt.rationalization.consensus.normalized_key`) must be written within one *field* of an element the
   model cited. A field is cut from the text that element showed (its :func:`element_text`, never the element's own
   fields): a table's cell (its header row's included, its ``| --- |`` rule never; a split table's part shows its
   own rows only), a key-value's key or its value, a heading's text without its ``#`` marker, a list's item without
   its ``-`` or ``1.`` marker or indent (a list shown as literal text, under markers such as ``a)``, keeps each
   entry with its marker), and a paragraph's text or a figure's caption whole. So a key never spans two cells, a
   key and its value or two items, and never holds the renderer's markup (``**Invoice:** 1,200``, ``# Parts``).
   Quotes are verbatim copies of the payload and may span cells (2). A key is written in a field:

   - a ``str`` key (not an Enum or Literal) as a substring of the field read as characters
     (:func:`unescape_markdown`: backslash escapes removed, ``<br>`` a line break), both sides normalized as in 2,
     set off from ASCII letters and digits: on each side where the key's own edge character is an ASCII letter or
     digit, the text's neighbouring character must not be one, so ``PN-10`` is not found in ``PN-104``, nor ``A``
     in ``Acme``, nor ``PN-104`` in ``PN-104A``, while ``(PN-104)`` holds ``PN-104``. Other characters never
     bound a key, so a key in an unspaced script (``港区`` in ``東京都港区六本木``) is found as a plain substring. A
     blank key is never found;
   - an ``int`` or ``Decimal`` key as one of the field's numbers (:func:`numeric_values`), equal by value, so a
     list's own numbering (``1.``) is no number;
   - a ``date``, datetime, Enum or Literal key through the model's quotes only: documents rarely write them as the
     schema does (``2026-11-05`` against ``5 Nov 2026``, an Enum's value against its label), so they get no check
     of their own, and a record whose keys are all such needs a quote of :data:`QUOTE_MIN_TOKENS` tokens (2).

   Otherwise the key is ``key_not_found``, naming the key field. A key gets no token containment, on a vision page
   either. Each checked key must also lie within one of that model's own quotes, read as characters
   (:func:`unescape_markdown`) and normalized as in 2, by the same rule (a ``str`` key set off from ASCII letters
   and digits, a number as one of the quote's numbers), else ``key_not_quoted``, naming the key field; a quote is
   read whole, so here a key may span cells. Only root keys are checked; other fields and child keys are left to
   the quotes.
4. **Status.** An item with no reason is grounded and keeps its status. A ``consensus`` or ``single_model`` item
   with a reason becomes ``ungrounded_value``, a review status; any other item keeps its status (it is already in
   review) and carries its reasons. Reasons come in model order; within a model, element ids (each
   ``unknown_element`` in its order, then ``header_only``; or ``no_element_ids``), then quotes (each
   ``quote_not_found`` in its order, then ``quote_too_short``; or ``no_quotes``), then keys, key field by key field
   (``key_not_found``, then ``key_not_quoted``). ``key_not_quoted`` and ``quote_too_short`` judge the quotes a model
   gave: a model that gave none is ``no_quotes`` alone for them.
5. **Empty units.** :attr:`GroundingResult.empty_units` names, in the order of ``units``, each unit at least one
   model answered (``outcome.responses``) that has no consensus item and no quarantined item: every model that
   answered it returned a complete, valid wrapper with no record. A unit every model failed on is a failed unit,
   not an empty one; a unit with a quarantined response is not empty either (its records are unknown).

Why every supporting model must ground its own evidence (not one per family): consensus already asks agreement
across families; grounding asks each backer to show where its values are written. A model citing elements that do
not hold its quotes may have invented its record, and another model's evidence must not vouch for it, so the item
goes to review (defense in depth).

What is not recognized as a number: a run of ASCII digits joined only by ``,`` and ``.`` is one number when it is
plain (``1200``, ``1200.50``) or groups thousands with commas (``1,200``, ``1,200.50``); any other run gives no
number (``1.200,50``, ``12,5``, ``1,2000``), and ``1.200`` reads as 1.2. A run glued to a letter, a digit or an
underscore on either side (``PN104``, ``12V``, ``10⁶``), or right after a ``.`` or a ``,`` (``v.5``, ``word,5``),
gives none; ``Qty 5, 6`` reads both. A ``-`` or ``−`` (U+2212) right before a number gives its negative too
(``-5`` holds both 5 and -5: a hyphen is no sure sign); accounting parentheses are not a sign (``(1,200)`` holds
1200, never -1200), and digits of other scripts are not read. Such a value goes to review.

Bounds: not linear per record. Each supporting model's quotes are each compared with each element it cited, and
its checked keys with each field of each element it cited and with each of its quotes, so a record costs its quotes
times its cited elements (and its keys times their fields and its quotes), each comparison linear in the texts it
reads but a key's, which can take the field's length times the key's when the key recurs glued to ASCII letters or
digits. Per-record caps on quotes and element ids come with the security sweep (plan T15, at validation). An
element's texts, fields, numbers and tokens are computed once per unit, on its first citation; numbers are read
in one linear pass (no run starts inside another); the token classes are built once, at import. The module imports
only the standard library, the element model and its sibling modules: nothing from ``datagrunt.core``, and no
import inside a function (invariant 9).
"""

import re
import string
import sys
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from fractions import Fraction
from typing import Any, Literal, TypeAlias, cast, get_args

from datagrunt.pdf.elements import BulletList, DocumentElement, Figure, Heading, KeyValue, PDFDocument, Table
from datagrunt.rationalization.consensus import ConsensusItem, ConsensusResult, alnum_upper_text, normalized_text
from datagrunt.rationalization.envelope import Envelope, element_text, escape
from datagrunt.rationalization.runner import RunOutcome
from datagrunt.rationalization.schema import MAX_DEPTH, ModelSpec, PreparedSchema
from datagrunt.rationalization.units import Unit

GroundedStatus: TypeAlias = Literal[
    "consensus", "single_model", "missing_in_some_models", "field_disagreement", "unmatched", "ungrounded_value"
]
"""An item's status after grounding: its consensus status, or ``ungrounded_value`` (module notes, 4)."""

GROUNDED_STATUSES: tuple[GroundedStatus, ...] = get_args(GroundedStatus)

GroundingReasonCode: TypeAlias = Literal[
    "unknown_element",
    "no_element_ids",
    "header_only",
    "no_quotes",
    "quote_not_found",
    "quote_too_short",
    "key_not_found",
    "key_not_quoted",
]
"""Why a model's evidence for an item does not ground it (module notes, 1-3)."""

GROUNDING_REASONS: tuple[GroundingReasonCode, ...] = get_args(GroundingReasonCode)

VISION_CONTAINMENT = Fraction(17, 20)
"""The least token containment (0.85) that grounds a quote in an element of a vision page (plan §0 correction 4)."""

_INDEXED: frozenset[str] = frozenset({"unknown_element", "quote_not_found"})
"""The reasons that name a position (and keep the text there)."""

_KEYED: frozenset[str] = frozenset({"key_not_found", "key_not_quoted"})
"""The reasons that name a key field."""

_CLEAN_STATUSES: frozenset[str] = frozenset({"consensus", "single_model"})
"""The consensus statuses a reason demotes to ``ungrounded_value``."""

_KeyKind: TypeAlias = Literal["text", "number"]
"""How a key value is looked for: as a substring of the element's characters, or as one of its numbers."""

# A Markdown backslash escape (a backslash before an ASCII punctuation character), or datagrunt's cell break.
_ESCAPE_OR_BREAK = re.compile(r"\\([!-/:-@\[-`{-~])|<br>")
# A run of ASCII digits joined by "," and ".", not glued to a word character (or after a "." or a ","): atomic, so
# a run that fails its lookahead is never cut short into a shorter one ("1,2x" gives nothing, not "1"). No run
# starts inside another (each of its digits follows a digit, "." or ","), so a failed run is never rescanned: linear.
_NUMBER_RUN = re.compile(r"(?<![\w.,])(?>[0-9](?:[0-9.,]*[0-9])?)(?!\w)")
# The runs that are numbers: plain, or with comma-grouped thousands; then an optional fraction.
_NUMBER = re.compile(r"(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?")
_MINUS = frozenset("-\u2212")
_ASCII_ALNUM = frozenset(string.ascii_letters + string.digits)
# The renderer's markup around an element's fields (module notes, 3). A cell is escaped text, so its own "|" is
# "\\|" and each backslash pairs with the character after it; a key-value's key holds no unescaped "*".
_TABLE_CELL = re.compile(r" ((?:\\.|[^\\|])*?) \|")
_TABLE_ROW = re.compile(r"\|(?: (?:\\.|[^\\|])*? \|)+")
_TABLE_RULE = re.compile(r"\| ---(?: \| ---)* \|")
_KEY_VALUE = re.compile(r"\*\*((?:\\.|[^\\*])+):\*\* (.*)", re.DOTALL)
_LIST_ENTRY = re.compile(r"(?:-|[0-9]+[.)]) ")
_ASCII_PUNCT_CHARS = frozenset(string.punctuation + string.whitespace)
_CODE_BRIDGES = frozenset("-_./ ")


_UNSPACED_BLOCKS: tuple[tuple[int, int], ...] = (
    (0x0E00, 0x0E7F),  # Thai
    (0x0E80, 0x0EFF),  # Lao
    (0x1000, 0x109F),  # Myanmar
    (0x1780, 0x17FF),  # Khmer
    (0x19E0, 0x19FF),  # Khmer Symbols
    (0x3040, 0x309F),  # Hiragana
    (0x30A0, 0x30FF),  # Katakana
    (0x31F0, 0x31FF),  # Katakana Phonetic Extensions
    (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs
    (0xA9E0, 0xA9FF),  # Myanmar Extended-B
    (0xAA60, 0xAA7F),  # Myanmar Extended-A
    (0xF900, 0xFAFF),  # CJK Compatibility Ideographs
    (0xFF66, 0xFF9F),  # Halfwidth Katakana
    (0x116D0, 0x116FF),  # Myanmar Extended-C
    (0x1AFF0, 0x1B16F),  # Kana Extended-B, Kana Supplement, Kana Extended-A, Small Kana Extension
    (0x20000, 0x323AF),  # CJK Unified Ideographs Extensions B to H, CJK Compatibility Ideographs Supplement
)
"""The Unicode blocks of the scripts written without spaces between words (Thai, Lao, Myanmar, Khmer, Hiragana,
Katakana, Han), ascending: blocks stand in for scripts, which the standard library does not name."""


def _class_of(points: Iterable[int], categories: str) -> str:
    """A regular-expression class of the ``points`` (ascending) whose Unicode category starts with one of
    ``categories``, from :mod:`unicodedata`: each run of consecutive ones becomes one range."""
    ranges: list[str] = []
    start = end = -2
    for point in points:
        if unicodedata.category(chr(point))[0] not in categories:
            continue
        if point != end + 1:
            if start >= 0:
                ranges.append(f"\\U{start:08X}-\\U{end:08X}")
            start = point
        end = point
    ranges.append(f"\\U{start:08X}-\\U{end:08X}")
    return "[" + "".join(ranges) + "]"


# Built once, at import, reading each code point once (about 1.1 million: tens of milliseconds). A token is a
# maximal run of letters, marks and numbers (categories L, M, N), so a combining vowel sign stays in its word.
_TOKEN = re.compile(_class_of(range(sys.maxunicode + 1), "LMN") + "+")
_UNSPACED_LETTER = re.compile(
    _class_of((point for low, high in _UNSPACED_BLOCKS for point in range(low, high + 1)), "L")
)
_SPACED_RUN = re.compile("[^" + "".join(f"\\U{low:08X}-\\U{high:08X}" for low, high in _UNSPACED_BLOCKS) + "]+")
QUOTE_MIN_TOKENS = 3
"""The tokens one of a model's quotes must hold when no key of its record is checked (module notes, 2)."""


# ---------------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------------


def _check_str(name: str, value: object) -> str:
    """Require a ``str``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    return value


def _check_text(name: str, value: object) -> str:
    """Require a non-blank ``str``."""
    text = _check_str(name, value)
    if not text.strip():
        raise ValueError(f"{name} must be non-blank")
    return text


def _check_reason_shape(reason: str, index: object, key_field: object, text: object) -> None:
    """Require ``index`` and ``text`` exactly for a reason naming a position, ``key_field`` exactly for a key's."""
    indexed = reason in _INDEXED
    if (index is not None) != indexed or (text is not None) != indexed:
        raise ValueError(f"a {reason} reason {'names' if indexed else 'has no'} index and text")
    if (key_field is not None) != (reason in _KEYED):
        raise ValueError(f"a {reason} reason {'names' if reason in _KEYED else 'has no'} key field")
    if index is not None:
        if type(index) is not int:
            raise TypeError(f"index must be an int, got {type(index).__name__}")
        if index < 0:
            raise ValueError(f"index must be >= 0, got {index}")
        _check_str("text", text)
    if key_field is not None:
        _check_text("key_field", key_field)


@dataclass(frozen=True, slots=True)
class GroundingReason:
    """One reason a supporting model's evidence does not ground an item.

    Args:
        reason: One of :data:`GROUNDING_REASONS`.
        model_id: The model whose evidence it is.
        index: For ``unknown_element``, the id's position among the model's element ids; for ``quote_not_found``,
            the quote's position among its quotes (0-based); else ``None``.
        key_field: For ``key_not_found`` and ``key_not_quoted``, the key field's attribute name; else ``None``.
        text: The id or quote at ``index``, else ``None``: model-chosen or document text, so the ``repr`` leaves
            it out; it stays in the data, for review.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If ``reason`` is unknown, ``model_id`` blank, ``index`` negative, or ``index``, ``key_field``
            or ``text`` is given for a reason that has none or missing for one that has it.
    """

    reason: GroundingReasonCode
    model_id: str
    index: int | None = None
    key_field: str | None = None
    text: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if _check_str("reason", self.reason) not in GROUNDING_REASONS:
            raise ValueError(f"reason must be one of {GROUNDING_REASONS}, got {self.reason!r}")
        _check_text("model_id", self.model_id)
        _check_reason_shape(self.reason, self.index, self.key_field, self.text)


@dataclass(frozen=True, slots=True)
class GroundedItem:
    """A consensus item after grounding (module notes, 4).

    Args:
        item: The consensus item.
        status: Its status after grounding: ``item.status`` when it is grounded or already in review, else
            ``ungrounded_value``.
        reasons: Why it is not grounded, in the order of module notes, 4; ``()`` when it is.

    Attributes:
        grounded: Whether it has no reason (derived).

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If ``status`` is not the one ``item`` and ``reasons`` give, or a reason names a model the item
            does not.
    """

    item: ConsensusItem
    status: GroundedStatus
    reasons: tuple[GroundingReason, ...]
    grounded: bool = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.item, ConsensusItem):
            raise TypeError(f"item must be a ConsensusItem, got {type(self.item).__name__}")
        if _check_str("status", self.status) not in GROUNDED_STATUSES:
            raise ValueError(f"status must be one of {GROUNDED_STATUSES}, got {self.status!r}")
        if not isinstance(self.reasons, tuple) or not all(isinstance(each, GroundingReason) for each in self.reasons):
            raise TypeError("reasons must be a tuple of GroundingReason")
        foreign = [each.model_id for each in self.reasons if each.model_id not in self.item.model_votes]
        if foreign:
            raise ValueError(f"reasons name models the item does not: {foreign}")
        expected = _status(self.item.status, bool(self.reasons))
        if self.status != expected:
            raise ValueError(f"a {self.item.status} item with {len(self.reasons)} reason(s) is {expected}")
        object.__setattr__(self, "grounded", not self.reasons)


@dataclass(frozen=True, slots=True)
class GroundingResult:
    """Every consensus item after grounding, in consensus order, and the empty units (module notes, 5).

    Raises:
        TypeError: If ``items`` is not a tuple of :class:`GroundedItem` or ``empty_units`` not a tuple of ``str``.
    """

    items: tuple[GroundedItem, ...]
    empty_units: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple) or not all(isinstance(each, GroundedItem) for each in self.items):
            raise TypeError("items must be a tuple of GroundedItem")
        if not isinstance(self.empty_units, tuple):
            raise TypeError(f"empty_units must be a tuple of unit ids, got {type(self.empty_units).__name__}")
        for unit_id in self.empty_units:
            _check_text("empty_units entry", unit_id)
        if len(set(self.empty_units)) != len(self.empty_units):
            raise ValueError("empty_units must not repeat a unit id")


def _status(status: str, has_reasons: bool) -> GroundedStatus:
    """An item's status after grounding: ``ungrounded_value`` for a clean one with a reason, else its own."""
    if has_reasons and status in _CLEAN_STATUSES:
        return "ungrounded_value"
    return cast(GroundedStatus, status)


def unescape_markdown(text: str) -> str:
    """The characters a payload text stands for: each Markdown backslash escape (a backslash before an ASCII
    punctuation character) gives that character, and datagrunt's ``<br>`` (a table cell's line join) a line break.

    ``[U+XXXX]`` tokens stay as written (envelope rule 7 asks for them in values too). It scans once, left to right,
    so ``\\\\<br>`` is a backslash then a line break, and ``\\<br\\>`` the literal text ``<br>``.

    Raises:
        TypeError: If ``text`` is not a ``str``.
    """
    source = _check_str("text", text)
    if "\\" not in source and "<br>" not in source:
        return source
    return _ESCAPE_OR_BREAK.sub(_unescaped, source)


def _unescaped(match: re.Match[str]) -> str:
    """What one :data:`_ESCAPE_OR_BREAK` match stands for: the escaped character, or a line break."""
    escaped = match.group(1)
    return "\n" if escaped is None else escaped


def numeric_values(text: str) -> frozenset[Decimal]:
    """The numbers ``text`` writes, by value (module notes, what is not recognized as a number).

    A number preceded by ``-`` or ``−`` (U+2212) gives its negative as well as itself.

    Raises:
        TypeError: If ``text`` is not a ``str``.
    """
    source = _check_str("text", text)
    values: set[Decimal] = set()
    for match in _NUMBER_RUN.finditer(source):
        if _NUMBER.fullmatch(match.group(0)) is None:
            continue
        value = Decimal(match.group(0).replace(",", ""))  # exact: no context rounding
        values.add(value)
        if match.start() and source[match.start() - 1] in _MINUS:
            values.add(value.copy_negate())  # exact, unlike unary minus
    return frozenset(values)


def token_containment(quote: str, text: str) -> Fraction:
    """The share of ``quote``'s tokens found in ``text``'s tokens, each token counted as often as it occurs.

    Both are normalized as module notes, 2 say; a token is a maximal run of letters, marks and numbers (Unicode
    categories L, M and N), compared exactly, so a combining vowel sign or tone mark stays in its word (``हिन्दी`` is
    one token, never its bare consonants), and a run of an unspaced script (Han, kana, Thai) is one token. Each
    occurrence in the quote is matched by at most one occurrence in the text, so repeating a word the text holds
    once cannot pad the share. A quote without a token has containment 0.

    Raises:
        TypeError: If ``quote`` or ``text`` is not a ``str``.
    """
    wanted = _tokens(_comparable(_check_str("quote", quote)))
    return _containment(wanted, _tokens(_comparable(_check_str("text", text))))


def _containment(wanted: Counter[str], held: Counter[str]) -> Fraction:
    """The share of the ``wanted`` token occurrences that ``held`` matches, each at most once; 0 without any."""
    total = sum(wanted.values())
    if not total:
        return Fraction(0)
    return Fraction(sum(min(count, held[token]) for token, count in wanted.items()), total)


def _comparable(text: str) -> str:
    """``text`` under consensus's string rule (NFC, stripped, every run of whitespace one space): what is compared."""
    return normalized_text(text)


def _tokens(text: str) -> Counter[str]:
    """The tokens of ``text`` (already comparable), each with its count."""
    return Counter(_TOKEN.findall(text))


def ground(
    consensus: ConsensusResult,
    *,
    outcome: RunOutcome,
    units: Mapping[str, Unit],
    envelopes: Mapping[str, Envelope],
    documents: Mapping[str, PDFDocument],
    schema: PreparedSchema,
) -> GroundingResult:
    """Ground every item of ``consensus`` (module notes).

    Args:
        consensus: The consensus result of the run.
        outcome: The run's outcome, whose responses say which units a model answered (module notes, 5).
        units: Every unit of the run by its id, in document order (the order of ``empty_units``).
        envelopes: Each unit's envelope by its id: the same ids as ``units``.
        documents: Each document of the run by its ``doc_id``: its pages' routes say which elements a vision page
            holds (module notes, 2).
        schema: The prepared schema the run used.

    Returns:
        The :class:`GroundingResult`.

    Raises:
        TypeError: If an argument has the wrong type.
        ValueError: If a mapping's id is not its value's, ``envelopes`` and ``units`` name other ids, an item, a
            quarantined item or a response names a unit ``units`` does not, an item's models are not the
            outcome's, its key does not fit the schema, a header-context id an envelope shows is the element of
            no unit of its document, a document is under another id, or a unit is not of the pages of a document
            given (its document or a page of it missing, or its ``vision`` flag not what their routes give).
    """
    _check_arguments(consensus, outcome, units, envelopes, schema)
    headers = _header_elements(units, envelopes)
    vision_pages = _vision_pages(documents, units, envelopes)
    checks = _key_checks(schema.spec)
    root_norms = _root_key_normalizers(schema.spec)
    child_specs = _child_key_specs(schema.spec)
    model_ids = tuple(model[0] for model in outcome.models)
    texts: dict[str, _UnitTexts] = {}
    items: list[GroundedItem] = []
    for item in consensus.items:
        _check_item(item, model_ids, schema, checks)
        unit_texts = texts.get(item.unit_id)
        if unit_texts is None:
            unit = units[item.unit_id]
            env = envelopes[item.unit_id]
            vision = vision_pages[unit.doc_id]
            witness = _multimodal_witness_elements(unit, env, documents[unit.doc_id])
            unit_texts = texts[item.unit_id] = _UnitTexts(unit, env, headers, vision, witness)
        found = _item_reasons(item, unit_texts, checks, root_norms=root_norms, child_specs=child_specs)
        items.append(GroundedItem(item=item, status=_status(item.status, bool(found)), reasons=found))
    return GroundingResult(items=tuple(items), empty_units=_empty_units(consensus, outcome, units))


def _multimodal_witness_elements(
    unit: Unit, envelope: Envelope, document: PDFDocument
) -> Mapping[str, tuple[DocumentElement, ...]]:
    """Return out-of-prompt OCR witness elements keyed by anchor id for a multimodal unit, or ``{}``."""
    if not envelope.images or len(unit.page_numbers) != 1 or len(unit.elements) != 1:
        return {}
    pno = unit.page_numbers[0]
    page = next((p for p in document.pages if p.page_number == pno), None)
    if page is None or page.image_only_reasons:
        return {unit.elements[0].element_id: ()}
    witness = tuple(
        el
        for el in page.elements
        if not el.hidden_reasons and not (isinstance(el, Figure) and not (el.caption and el.caption.strip()))
    )
    return {unit.elements[0].element_id: witness}


def _vision_pages(
    documents: object,
    units: Mapping[str, Unit],
    envelopes: Mapping[str, Envelope] | None = None,
) -> dict[str, frozenset[int]]:
    """The numbers of each document's pages whose route is ``vision``, by its doc id (module notes, 2).

    Raises:
        TypeError: If ``documents`` is not a mapping of doc ids to documents.
        ValueError: If a document is under another id than its own, or a unit is not of the pages of a document
            given: its document is missing, a page of it is, or its ``vision`` flag is not what their routes give.
    """
    if not isinstance(documents, Mapping):
        raise TypeError(f"documents must be a Mapping of doc ids to documents, got {type(documents).__name__}")
    routes: dict[str, dict[int, bool]] = {}
    for doc_id, document in documents.items():
        if not isinstance(doc_id, str) or not isinstance(document, PDFDocument):
            raise TypeError(f"documents must map doc ids (str) to PDFDocuments, got a {type(document).__name__}")
        if document.metadata.doc_id != doc_id:
            raise ValueError(f"documents key {doc_id!r} is not its document's id ({document.metadata.doc_id!r})")
        routes[doc_id] = {page.page_number: page.route == "vision" for page in document.pages}
    if envelopes is not None:
        for unit_id, unit in units.items():
            env = envelopes.get(unit_id)
            if env is not None and env.images and unit.doc_id in routes:
                for pno in unit.page_numbers:
                    if pno in routes[unit.doc_id]:
                        routes[unit.doc_id][pno] = True
    for unit in units.values():
        pages = routes.get(unit.doc_id)
        if pages is None:
            raise ValueError(f"the unit {unit.unit_id!r} is of a document documents does not hold")
        if any(number not in pages for number in unit.page_numbers) or unit.vision != any(
            pages[number] for number in unit.page_numbers
        ):
            raise ValueError(f"the unit {unit.unit_id!r} is not of its document's pages (their numbers and routes)")
    return {doc_id: frozenset(number for number, vision in pages.items() if vision) for doc_id, pages in routes.items()}


def _check_mapping(name: str, value: object, kind: type, what: str) -> None:
    """Require a mapping of unit ids to ``kind`` instances, each under its own ``unit_id``."""
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a Mapping of unit ids to {what}s, got {type(value).__name__}")
    for unit_id, entry in value.items():
        if not isinstance(unit_id, str):
            raise TypeError(f"{name} keys must be unit ids (str), got {type(unit_id).__name__}")
        if not isinstance(entry, kind) or not isinstance(entry, (Unit, Envelope)):
            raise TypeError(f"{name}[{unit_id!r}] must be a {kind.__name__}, got {type(entry).__name__}")
        if entry.unit_id != unit_id:
            raise ValueError(f"{name} key {unit_id!r} is not its {what}'s id ({entry.unit_id!r})")


def _check_arguments(consensus: object, outcome: object, units: object, envelopes: object, schema: object) -> None:
    """Require each argument's type, and the units to name every unit the others name (the items' checked later)."""
    if not isinstance(consensus, ConsensusResult):
        raise TypeError(f"consensus must be a ConsensusResult, got {type(consensus).__name__}")
    if not isinstance(outcome, RunOutcome):
        raise TypeError(f"outcome must be a RunOutcome, got {type(outcome).__name__}")
    if not isinstance(schema, PreparedSchema):
        raise TypeError(f"schema must be a PreparedSchema, got {type(schema).__name__}")
    _check_mapping("units", units, Unit, "unit")
    _check_mapping("envelopes", envelopes, Envelope, "envelope")
    known = set(cast(Mapping[str, Unit], units))
    if set(cast(Mapping[str, Envelope], envelopes)) != known:
        raise ValueError("envelopes must name the same unit ids as units")
    named = (
        *(("an item", item.unit_id) for item in consensus.items),
        *(("a quarantined item", item.unit_id) for item in consensus.quarantined),
        *(("a response", unit_id) for unit_id, _ in outcome.responses),
    )
    for what, unit_id in named:
        if unit_id not in known:
            raise ValueError(f"{what} names the unit {unit_id!r}, which units does not")


def _check_item(
    item: ConsensusItem,
    model_ids: tuple[str, ...],
    schema: PreparedSchema,
    checks: tuple[tuple[int, str, _KeyKind], ...],
) -> None:
    """Require an item of the outcome's models and of the schema: its record's model, its key's length and types."""
    if tuple(item.model_votes) != model_ids:
        raise ValueError(f"an item's models must be the outcome's {model_ids}, got {tuple(item.model_votes)}")
    if item.record is not None and not isinstance(item.record, schema.model):
        raise ValueError(
            f"an item's record is a {type(item.record).__name__}, not the schema's {schema.model.__name__}"
        )
    if len(item.key) != len(schema.spec.keys.fields):
        raise ValueError(f"an item's key has {len(item.key)} part(s), the schema's {len(schema.spec.keys.fields)}")
    for position, name, kind in checks:
        value = item.key[position]
        fits = type(value) is str if kind == "text" else type(value) is int or type(value) is Decimal
        if not fits:
            raise ValueError(f"an item's key value for {name!r} is a {type(value).__name__}, not its schema type")


def _empty_units(consensus: ConsensusResult, outcome: RunOutcome, units: Mapping[str, Unit]) -> tuple[str, ...]:
    """The units a model answered with neither an item nor a quarantined item, in the order of ``units``."""
    answered = {unit_id for unit_id, _ in outcome.responses}
    used = {item.unit_id for item in consensus.items} | {item.unit_id for item in consensus.quarantined}
    return tuple(unit_id for unit_id in units if unit_id in answered and unit_id not in used)


# ---------------------------------------------------------------------------------------------------------------
# One unit's texts
# ---------------------------------------------------------------------------------------------------------------


class _UnitTexts:
    """One unit's citable elements (module notes, 1) and their texts, each text computed once, on first use.

    The parts of one split element share its id: a unit holds the parts it was given, each a text of its own.
    """

    __slots__ = (
        "_elements",
        "_multimodal",
        "_numbers",
        "_own",
        "_plain",
        "_plain_sets",
        "_quoted",
        "_raw",
        "_tokens",
        "_vision_pages",
        "_witness",
    )

    def __init__(
        self,
        unit: Unit,
        envelope: Envelope,
        headers: Mapping[tuple[str, str], DocumentElement],
        vision_pages: frozenset[int],
        witness: Mapping[str, tuple[DocumentElement, ...]] | None = None,
    ) -> None:
        elements: dict[str, list[DocumentElement]] = {}
        for element in unit.elements:
            elements.setdefault(element.element_id, []).append(element)
        self._own = frozenset(elements)
        for element_id in envelope.header_element_ids:  # never one of the unit's own: the envelope leaves those out
            elements[element_id] = [headers[(unit.doc_id, element_id)]]
        self._elements = elements
        self._multimodal = bool(envelope.images)
        self._witness: Mapping[str, tuple[DocumentElement, ...]] = witness or {}
        self._raw: dict[str, tuple[str, ...]] = {}
        self._quoted: dict[str, tuple[str, ...]] = {}
        self._plain: dict[str, tuple[str, ...]] = {}
        self._plain_sets: dict[str, frozenset[str]] = {}
        self._numbers: dict[str, frozenset[Decimal]] = {}
        self._tokens: dict[str, tuple[Counter[str], ...]] = {}
        self._vision_pages = vision_pages

    @property
    def multimodal(self) -> bool:
        return self._multimodal

    def citable(self, element_id: str) -> bool:
        """Whether a model may cite ``element_id`` in this unit: one of its elements, or a header block it shows."""
        return element_id in self._elements

    def own(self, element_id: str) -> bool:
        """Whether ``element_id`` is one of the unit's own elements, not a header block its envelope shows."""
        return element_id in self._own

    def lenient(self, element_id: str) -> bool:
        """Whether the cited ``element_id`` lies on a vision page of its document: its quotes take containment."""
        return self._elements[element_id][0].page_number in self._vision_pages

    def raw(self, element_id: str) -> tuple[str, ...]:
        """The :func:`element_text` of each element (or out-of-prompt OCR witness) ``element_id`` names here."""
        found = self._raw.get(element_id)
        if found is None:
            if element_id in self._witness:
                els = self._witness[element_id]
                parts = tuple(element_text(element) for element in els)
                found = (*parts, "\n".join(parts)) if len(parts) > 1 else parts
            else:
                found = tuple(element_text(element) for element in self._elements[element_id])
            self._raw[element_id] = found
        return found

    def quoted(self, element_id: str) -> tuple[str, ...]:
        """Each of :meth:`raw`'s texts, comparable: what quotes match."""
        found = self._quoted.get(element_id)
        if found is None:
            found = self._quoted[element_id] = tuple(_comparable(text) for text in self.raw(element_id))
        return found

    def plain(self, element_id: str) -> tuple[str, ...]:
        """The fields of each of :meth:`raw`'s texts (:func:`_fields`), read as characters (:func:`unescape_markdown`)
        and comparable: what keys match, each within one field."""
        found = self._plain.get(element_id)
        if found is None:
            if element_id in self._witness:
                els = self._witness[element_id]
                found = tuple(
                    _comparable(unescape_markdown(each))
                    for element in els
                    for each in _fields(element, element_text(element))
                )
            else:
                elem_list = self._elements[element_id]
                if len(elem_list) == 1 and isinstance(elem_list[0], Table):
                    tbl = elem_list[0]
                    hdr = tbl.headers if tbl.headers else ("",) * len(tbl.rows[0])
                    found = tuple(
                        _comparable(c if (c.isascii() and c.isprintable()) else escape(c))
                        for row in (hdr, *tbl.rows)
                        for c in row
                    )
                else:
                    found = tuple(
                        _comparable(unescape_markdown(each))
                        for element, text in zip(elem_list, self.raw(element_id), strict=True)
                        for each in _fields(element, text)
                    )
            self._plain[element_id] = found
        return found

    def plain_set(self, element_id: str) -> frozenset[str]:
        """Frozenset of :meth:`plain`'s texts for O(1) exact field-equality key checks."""
        found = self._plain_sets.get(element_id)
        if found is None:
            found = self._plain_sets[element_id] = frozenset(self.plain(element_id))
        return found

    def numbers(self, element_id: str) -> frozenset[Decimal]:
        """The numbers of :meth:`plain`'s texts (:func:`numeric_values`): what an int or Decimal key matches."""
        found = self._numbers.get(element_id)
        if found is None:
            found = self._numbers[element_id] = frozenset().union(*map(numeric_values, self.plain(element_id)))
        return found

    def tokens(self, element_id: str) -> tuple[Counter[str], ...]:
        """The tokens of each of :meth:`quoted`'s texts, with their counts: what a vision page's quotes match."""
        found = self._tokens.get(element_id)
        if found is None:
            found = self._tokens[element_id] = tuple(_tokens(text) for text in self.quoted(element_id))
        return found


# ---------------------------------------------------------------------------------------------------------------
# An element's fields, read from the text it showed
# ---------------------------------------------------------------------------------------------------------------


def _fields(element: DocumentElement, shown: str) -> tuple[str, ...]:
    """The fields of ``shown``, the :func:`element_text` of ``element`` (module notes, 3): a key lies within one.

    A table's cells, its header row's included (never its ``| --- |`` rule); a key-value's key and its value; a
    heading's text (never its ``#`` marker); each item of a list; a paragraph's text or a figure's caption whole.
    Each is cut from the text as shown, escapes and R8 tokens included, never read from the element's own fields,
    which the text renders: only their counts check the cut.

    Raises:
        RuntimeError: If ``shown`` is not the shape the element model renders (unreachable: it rendered it).
    """
    found, expected = _cut(element, shown)
    if len(found) != expected:
        raise RuntimeError(f"element {element.element_id}'s text is not the shape its model renders")
    return tuple(found)


def _cut(element: DocumentElement, shown: str) -> tuple[list[str], int]:
    """The fields cut from ``shown`` by ``element``'s kind (``[]`` when it is not that kind's shape), and how many
    the element's own fields say there are."""
    if isinstance(element, Table):
        width = len(element.headers) if element.headers else len(element.rows[0])
        return _table_cells(shown), width * (len(element.rows) + 1)
    if isinstance(element, BulletList):
        return _list_items(shown), len(element.items)
    if isinstance(element, KeyValue):
        pair = _KEY_VALUE.fullmatch(shown)
        return ([] if pair is None else [pair.group(1), pair.group(2)]), 2
    if isinstance(element, Heading):
        marker = "#" * element.level + " "
        return ([shown[len(marker) :]] if shown.startswith(marker) else []), 1
    return [shown], 1  # a paragraph's text, or a figure's caption


def _table_cells_plain(lines: list[str]) -> list[str]:
    """Fast path for :func:`_table_cells` when ``shown`` contains no backslash escapes."""
    cells: list[str] = []
    for line in (lines[0], *lines[2:]):
        parts = line.split("|")
        if len(parts) < 3 or parts[0] or parts[-1]:
            return []
        middle = parts[1:-1]
        if not all(len(p) >= 2 and p[0] == " " and p[-1] == " " for p in middle):
            return []
        cells.extend(p[1:-1] for p in middle)
    return cells


def _table_cells(shown: str) -> list[str]:
    """The cells of a table's shown text, row by row: its header row, then its body rows (its rule left out).

    Each row is ``| cell | cell |``, and a cell's ``|`` is always escaped, so each unescaped ``|`` ends a cell.
    Returns ``[]`` when the text is not that shape (the caller raises).
    """
    lines = shown.split("\n")
    if len(lines) < 2 or _TABLE_RULE.fullmatch(lines[1]) is None:
        return []
    if "\\" not in shown:
        return _table_cells_plain(lines)
    cells: list[str] = []
    for line in (lines[0], *lines[2:]):
        if _TABLE_ROW.fullmatch(line) is None:
            return []
        cells.extend(_TABLE_CELL.findall(line))
    return cells


def _list_items(shown: str) -> list[str]:
    """The items of a list's shown text (module notes, 3), in order; ``[]`` when it is not a list's shape.

    A list of entries opens each with its marker (``-``, ``1.``) and a space, and indents the item's other lines;
    the item is its lines without the marker or the indent. A list shown as literal text (markers such as ``a)``)
    is one paragraph whose entries end in a hard break (a line ending in an odd run of backslashes: every literal
    backslash is doubled): there an item is its entry, marker included, without that break.
    """
    lines = shown.split("\n")
    if _LIST_ENTRY.match(lines[0]) is None:
        return _literal_entries(lines)
    items: list[list[str]] = []
    for line in lines:
        entry = _LIST_ENTRY.match(line)
        if entry is not None:
            items.append([line[entry.end() :]])
        elif line.startswith(" "):
            items[-1].append(line.lstrip(" "))
        else:
            return []
    return ["\n".join(item) for item in items]


def _literal_entries(lines: list[str]) -> list[str]:
    """The entries of a list shown as one paragraph: each ends at a hard break, which it leaves out."""
    entries: list[str] = []
    current: list[str] = []
    for line in lines:
        breaks = (len(line) - len(line.rstrip("\\"))) % 2 == 1
        current.append(line[:-1] if breaks else line)
        if breaks:
            entries.append("\n".join(current))
            current = []
    if current:
        entries.append("\n".join(current))
    return entries


def _header_elements(
    units: Mapping[str, Unit], envelopes: Mapping[str, Envelope]
) -> dict[tuple[str, str], DocumentElement]:
    """The header-context element each envelope shows, by ``(doc_id, element_id)``: a unit of its document holds it.

    Raises:
        ValueError: If no unit of its document holds one of them.
    """
    wanted = {
        (units[unit_id].doc_id, element_id)
        for unit_id, envelope in envelopes.items()
        for element_id in envelope.header_element_ids
    }
    found: dict[tuple[str, str], DocumentElement] = {}
    for unit in units.values():
        for element in unit.elements:
            if (unit.doc_id, element.element_id) in wanted:
                found[(unit.doc_id, element.element_id)] = element
    missing = sorted(wanted - found.keys())
    if missing:
        raise ValueError(f"header-context blocks an envelope shows are the elements of no unit given: {missing}")
    return found


# ---------------------------------------------------------------------------------------------------------------
# One item's reasons
# ---------------------------------------------------------------------------------------------------------------


def _key_checks(spec: ModelSpec) -> tuple[tuple[int, str, _KeyKind], ...]:
    """The root key fields checked against the text (module notes, 3): ``(position in the key, field name, kind)``.

    A ``str`` key is looked for as text and an ``int`` or ``Decimal`` key as a number; a key of another type, or an
    Enum or a Literal, is left to the quotes.
    """
    scalars = {field_spec.name: field_spec.scalar for field_spec in spec.fields}
    checks: list[tuple[int, str, _KeyKind]] = []
    for position, name in enumerate(spec.keys.fields):
        scalar = scalars[name]
        if scalar is None or scalar.enum is not None or scalar.literal is not None:
            continue
        if scalar.base is str:
            checks.append((position, name, "text"))
        elif scalar.base is int or scalar.base is Decimal:
            checks.append((position, name, "number"))
    return tuple(checks)


def _peer_family_quotes(item: ConsensusItem, model_id: str) -> tuple[str, ...]:
    """Quotes supplied by supporting models from distinct known model families (#473 visual witness)."""
    own_family = item.families.get(model_id, "unknown")
    if own_family == "unknown":
        return ()
    return tuple(
        quote
        for peer_id, peer_votes in item.model_votes.items()
        if peer_votes
        and item.families.get(peer_id, "unknown") != "unknown"
        and item.families.get(peer_id, "unknown") != own_family
        for quote in item.quotes_by_model[peer_id]
    )


_ChildKeySpec: TypeAlias = tuple[tuple[tuple[str, bool], ...], str, _KeyKind, str | None]


def _root_key_normalizers(spec: ModelSpec) -> Mapping[str, str | None]:
    by_name = {f.name: f.key_normalizer for f in spec.fields}
    return {name: by_name.get(name) for name in spec.keys.fields}


def _child_key_specs(
    spec: ModelSpec, prefix: tuple[tuple[str, bool], ...] = (), depth: int = 1
) -> tuple[_ChildKeySpec, ...]:
    """Collect checked scalar keys declared on nested ``model`` and ``model_list`` fields."""
    if depth > MAX_DEPTH:
        return ()
    out: list[_ChildKeySpec] = []
    for field_spec in spec.fields:
        child = field_spec.child
        if child is None:
            continue
        step = (*prefix, (field_spec.name, field_spec.kind == "model_list"))
        if field_spec.kind == "model_list" and child.keys.fields:
            by_name = {cf.name: cf for cf in child.fields}
            for _, k_name, k_kind in _key_checks(child):
                dotted = ".".join([*(n for n, _ in step), k_name])
                out.append(((*step, (k_name, False)), dotted, k_kind, by_name[k_name].key_normalizer))
        out.extend(_child_key_specs(child, step, depth + 1))
    return tuple(out)


def _extract_path_values(node: object, steps: tuple[tuple[str, bool], ...]) -> list[object]:
    """Extract scalar key values along ``steps`` from one JSON-mode vote dict."""
    current: list[object] = [node]
    for attr_name, is_list in steps:
        nxt: list[object] = []
        for obj in current:
            if not isinstance(obj, Mapping) or attr_name not in obj or obj[attr_name] is None:
                continue
            val = obj[attr_name]
            if is_list:
                if isinstance(val, Sequence) and not isinstance(val, (str, bytes)):
                    nxt.extend(val)
            else:
                nxt.append(val)
        current = nxt
    return current


def _coerce_key_val(raw: object, kind: _KeyKind) -> object:
    if kind == "number":
        return raw if type(raw) in (int, Decimal) else Decimal(str(raw))
    return str(raw)


def _extract_two_step_values(
    votes: Sequence[Mapping[str, Any]], list_attr: str, key_attr: str, kind: _KeyKind
) -> tuple[object, ...]:
    """Fast path for depth-1 ``model_list`` child keys (e.g. ``components[].part_number``)."""
    if len(votes) == 1 and type(votes[0]) is dict and kind == "text":
        rows_fast = votes[0].get(list_attr)
        if type(rows_fast) is list:
            fast = tuple(r[key_attr] for r in rows_fast if type(r) is dict and type(r.get(key_attr)) is str)
            if len(fast) == len(rows_fast):
                return fast
    vals: list[object] = []
    for vote in votes:
        rows = vote.get(list_attr) if isinstance(vote, Mapping) else None
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            continue
        vals.extend(
            _coerce_key_val(row[key_attr], kind)
            for row in rows
            if isinstance(row, Mapping) and row.get(key_attr) is not None
        )
    return tuple(dict.fromkeys(vals))


def _extract_spec_values(
    votes: Sequence[Mapping[str, Any]], steps: tuple[tuple[str, bool], ...], kind: _KeyKind
) -> tuple[object, ...]:
    """Extract distinct coerced child key values for ``steps`` across ``votes``."""
    if len(steps) == 2 and steps[0][1] and not steps[1][1]:
        return _extract_two_step_values(votes, steps[0][0], steps[1][0], kind)
    return tuple(dict.fromkeys(_coerce_key_val(x, kind) for v in votes for x in _extract_path_values(v, steps)))


def _raw_root_values(votes: Sequence[Mapping[str, Any]], name: str, kind: _KeyKind) -> tuple[object, ...]:
    """Extract distinct raw root key values across ``votes`` when ``key_normalizer`` is set."""
    return tuple(dict.fromkeys(_coerce_key_val(v[name], kind) for v in votes if isinstance(v, Mapping) and name in v))


def _item_reasons(
    item: ConsensusItem,
    texts: _UnitTexts,
    checks: tuple[tuple[int, str, _KeyKind], ...],
    *,
    root_norms: Mapping[str, str | None] | None = None,
    child_specs: tuple[_ChildKeySpec, ...] = (),
) -> tuple[GroundingReason, ...]:
    """Every supporting model's reasons, in model order (module notes, 4)."""
    norms = root_norms or {}
    has_norm = any(norms.values())
    found: list[GroundingReason] = []
    seen: dict[tuple[object, ...], list[GroundingReason]] = {}
    for model_id, votes in item.model_votes.items():
        if not votes:
            continue
        element_ids, quotes = item.element_ids_by_model[model_id], item.quotes_by_model[model_id]
        cache_tag = item.families.get(model_id, "") if texts.multimodal else ""
        root_sig = (
            tuple(_raw_root_values(votes, name, kind) if norms.get(name) else () for _, name, kind in checks)
            if has_norm
            else ()
        )
        child_vals = tuple(_extract_spec_values(votes, steps, kind) for steps, _, kind, _ in child_specs)
        evidence = (element_ids, quotes, cache_tag, root_sig, child_vals)
        cached = seen.get(evidence)
        if cached is not None:
            found.extend(replace(reason, model_id=model_id) for reason in cached)
            continue
        cited = [element_id for element_id in element_ids if texts.citable(element_id)]
        peer_quotes = _peer_family_quotes(item, model_id) if texts.multimodal and cited else ()
        own: list[GroundingReason] = []
        own.extend(_cited_reasons(model_id, element_ids, texts))
        own.extend(_quote_reasons(model_id, quotes, cited, texts, keyless=not checks, peer_quotes=peer_quotes))
        own.extend(
            _key_reasons(
                model_id,
                item.key,
                checks,
                cited,
                quotes,
                texts,
                peer_quotes=peer_quotes,
                root_raw=root_sig,
                root_norms=norms,
                child_specs=child_specs,
                child_vals=child_vals,
            )
        )
        seen[evidence] = own
        found.extend(own)
    return tuple(found)


def _cited_reasons(model_id: str, element_ids: tuple[str, ...], texts: _UnitTexts) -> list[GroundingReason]:
    """One model's reasons about the ids it cites (module notes, 1): each unknown id, then ``header_only``."""
    if not element_ids:
        return [GroundingReason("no_element_ids", model_id)]
    found = [
        GroundingReason("unknown_element", model_id, index=index, text=element_id)
        for index, element_id in enumerate(element_ids)
        if not texts.citable(element_id)
    ]
    citable = [element_id for element_id in element_ids if texts.citable(element_id)]
    if citable and not any(texts.own(element_id) for element_id in citable):
        found.append(GroundingReason("header_only", model_id))
    return found


def _quote_reasons(
    model_id: str,
    quotes: tuple[str, ...],
    cited: list[str],
    texts: _UnitTexts,
    *,
    keyless: bool,
    peer_quotes: tuple[str, ...] = (),
) -> list[GroundingReason]:
    """One model's reasons about its quotes (module notes, 2): each must lie within one element it cited; when no
    key is checked (``keyless``), one of them must hold :data:`QUOTE_MIN_TOKENS` tokens."""
    if not quotes:
        return [GroundingReason("no_quotes", model_id)]
    comparable = [_comparable(quote) for quote in quotes]
    peer_comp = tuple(_comparable(pq) for pq in peer_quotes) if peer_quotes else ()
    found = [
        GroundingReason("quote_not_found", model_id, index=index, text=quote)
        for index, quote in enumerate(quotes)
        if not _quote_found(comparable[index], cited, texts, peer_comp)
    ]
    if keyless and all(_length(quote) < QUOTE_MIN_TOKENS for quote in comparable):
        found.append(GroundingReason("quote_too_short", model_id))
    return found


def _length(quote: str) -> int:
    """How many tokens the comparable ``quote`` counts for the length rule (module notes, 2): one per token, but
    a token holding an unspaced script counts each letter of that script, and each run of its other characters,
    once (at least one)."""
    return sum(
        max(1, len(_UNSPACED_LETTER.findall(token)) + len(_SPACED_RUN.findall(token)))
        for token in _TOKEN.findall(quote)
    )


def _quote_found(quote: str, cited: list[str], texts: _UnitTexts, peer_comp: tuple[str, ...] = ()) -> bool:
    """Whether the comparable ``quote`` lies within the text of one of the ``cited`` elements (module notes, 2).

    In one of them on a vision page, a token containment of at least :data:`VISION_CONTAINMENT` counts too.
    """
    if not quote:
        return False
    if any(quote in text for element_id in cited for text in texts.quoted(element_id)):
        return True
    lenient = [element_id for element_id in cited if texts.lenient(element_id)]
    if not lenient:
        return False
    wanted = _tokens(quote)
    if any(
        _containment(wanted, held) >= VISION_CONTAINMENT for element_id in lenient for held in texts.tokens(element_id)
    ):
        return True
    if not peer_comp:
        return False
    return any(quote in pq or _containment(wanted, _tokens(pq)) >= VISION_CONTAINMENT for pq in peer_comp)


def _valid_code_gap(gap: str) -> bool:
    """Whether ``gap`` may bridge characters inside one identifier (never across ``,``, ``:``, ``;``, ``()``)."""
    if not gap:
        return True
    return all(ch in _CODE_BRIDGES for ch in gap) and sum(ch != " " for ch in gap) <= 1


def _code_edge_glued(text: str, start: int, end: int) -> bool:
    """Whether ``text[start:end + 1]`` is glued to adjacent ASCII alnum directly or across ``-`` / ``_``."""
    if start > 0 and text[start - 1] in _ASCII_ALNUM:
        return True
    if end + 1 < len(text) and text[end + 1] in _ASCII_ALNUM:
        return True
    if start >= 2 and text[start - 1] in "-_" and text[start - 2] in _ASCII_ALNUM:
        return True
    return end + 2 < len(text) and text[end + 1] in "-_" and text[end + 2] in _ASCII_ALNUM


def _set_off_alnum_upper(wanted: str, text: str) -> bool:
    """Boundary-safe ``alnum_upper`` match against code tokens in ``text``."""
    target = alnum_upper_text(wanted)
    if not target:
        return False
    kept = [(idx, uch) for idx, ch in enumerate(text) if ch not in _ASCII_PUNCT_CHARS for uch in ch.upper()]
    joined = "".join(ch for _, ch in kept)
    klen = len(target)
    pos = joined.find(target)
    while pos >= 0:
        start_idx, end_idx = kept[pos][0], kept[pos + klen - 1][0]
        gaps_ok = all(_valid_code_gap(text[kept[k][0] + 1 : kept[k + 1][0]]) for k in range(pos, pos + klen - 1))
        if gaps_ok and not _code_edge_glued(text, start_idx, end_idx):
            return True
        pos = joined.find(target, pos + 1)
    return False


def _key_reasons(
    model_id: str,
    key: tuple[object, ...],
    checks: tuple[tuple[int, str, _KeyKind], ...],
    cited: list[str],
    quotes: tuple[str, ...],
    texts: _UnitTexts,
    *,
    peer_quotes: tuple[str, ...] = (),
    root_raw: tuple[tuple[object, ...], ...] = (),
    root_norms: Mapping[str, str | None] | None = None,
    child_specs: tuple[_ChildKeySpec, ...] = (),
    child_vals: tuple[tuple[object, ...], ...] = (),
) -> list[GroundingReason]:
    """One model's reasons about the item's root and child-list key values (module notes, 3)."""
    if not checks and not child_specs:
        return []
    norms = root_norms or {}
    read = tuple(_comparable(unescape_markdown(quote)) for quote in quotes)
    peer_plain = tuple(_comparable(unescape_markdown(pq)) for pq in peer_quotes) if peer_quotes else ()
    has_num = any(kind == "number" for *_, kind in checks) or any(kind == "number" for _, _, kind, _ in child_specs)
    peer_numbers = frozenset().union(*map(numeric_values, peer_plain)) if has_num and peer_plain else None
    quote_num_cache: list[frozenset[Decimal] | None] = [None] * len(read)
    found: list[GroundingReason] = []
    for idx, (position, name, kind) in enumerate(checks):
        norm = norms.get(name)
        raw_vals = root_raw[idx] if idx < len(root_raw) and root_raw[idx] else (key[position],)
        if not all(_key_found(val, kind, cited, texts, peer_plain, peer_numbers, norm) for val in raw_vals):
            found.append(GroundingReason("key_not_found", model_id, key_field=name))
        if quotes and not all(_key_quoted(val, kind, read, quote_num_cache, norm) for val in raw_vals):
            found.append(GroundingReason("key_not_quoted", model_id, key_field=name))
    for (_, dotted, kind, norm), c_vals in zip(child_specs, child_vals, strict=True):
        if c_vals and not all(_key_found(val, kind, cited, texts, peer_plain, peer_numbers, norm) for val in c_vals):
            found.append(GroundingReason("key_not_found", model_id, key_field=dotted))
    return found


def _quote_has_number(value: object, read: tuple[str, ...], cache: list[frozenset[Decimal] | None]) -> bool:
    """Lazily evaluate ``numeric_values`` quote-by-quote so an early match skips later quotes."""
    for idx, quote in enumerate(read):
        nums = cache[idx]
        if nums is None:
            nums = cache[idx] = numeric_values(quote)
        if value in nums:
            return True
    return False


def _key_quoted(
    value: object,
    kind: _KeyKind,
    read: tuple[str, ...],
    quote_num_cache: list[frozenset[Decimal] | None],
    normalizer: str | None = None,
) -> bool:
    """Whether one key value lies within one of the model's quotes, ``read`` as characters (comparable)."""
    if kind == "number":
        return _quote_has_number(value, read, quote_num_cache)
    wanted = _comparable(cast(str, value))
    if not wanted:
        return False
    if any(_set_off(wanted, text) for text in read):
        return True
    return normalizer == "alnum_upper" and any(_set_off_alnum_upper(wanted, text) for text in read)


def _key_found(
    value: object,
    kind: _KeyKind,
    cited: list[str],
    texts: _UnitTexts,
    peer_plain: tuple[str, ...] = (),
    peer_numbers: frozenset[Decimal] | None = None,
    normalizer: str | None = None,
) -> bool:
    """Whether one key value is written in one of the ``cited`` elements: as text, or as one of its numbers."""
    if kind == "number":
        if any(value in texts.numbers(element_id) for element_id in cited):
            return True
        return peer_numbers is not None and value in peer_numbers
    wanted = _comparable(cast(str, value))
    if not wanted:
        return False
    if any(wanted in texts.plain_set(element_id) for element_id in cited):
        return True
    if any(_set_off(wanted, text) for element_id in cited for text in texts.plain(element_id)):
        return True
    if normalizer == "alnum_upper" and any(
        _set_off_alnum_upper(wanted, text) for element_id in cited for text in texts.plain(element_id)
    ):
        return True
    if peer_plain and any(_set_off(wanted, pq) for pq in peer_plain):
        return True
    return (
        bool(peer_plain) and normalizer == "alnum_upper" and any(_set_off_alnum_upper(wanted, pq) for pq in peer_plain)
    )


def _set_off(wanted: str, text: str) -> bool:
    """Whether ``wanted`` (non-empty) occurs in ``text`` set off from ASCII letters and digits (module notes, 3).

    An occurrence counts when, on each side where ``wanted``'s own edge character is an ASCII letter or digit, the
    text's neighbouring character is not one (or there is none). It tries each occurrence, at most ``len(text)``.
    """
    left, right = wanted[0] in _ASCII_ALNUM, wanted[-1] in _ASCII_ALNUM
    start = text.find(wanted)
    for _ in range(len(text)):
        if start < 0:
            return False
        end = start + len(wanted)
        glued_left = left and start > 0 and text[start - 1] in _ASCII_ALNUM
        glued_right = right and end < len(text) and text[end] in _ASCII_ALNUM
        if not glued_left and not glued_right:
            return True
        start = text.find(wanted, start + 1)
    return False
