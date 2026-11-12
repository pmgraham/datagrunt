"""Local ``$ref`` references of a response schema: the one reading every schema transform shares (plan §4.1, §4.5).

:mod:`~datagrunt.rationalization.schema` and the three wire adapters (Anthropic, OpenAI-compatible, Gemini) read
references with these functions, so a schema one accepts is never read differently by another. The rules:

1. **Names** (:func:`def_name`). A reference is local, ``#/$defs/<name>`` (:data:`DEFS_PREFIX`), and the name is
   one JSON-pointer reference token (RFC 6901, section 4): ``~1`` stands for ``/`` and ``~0`` for ``~``, unescaped
   in that order, so ``~01`` is ``~1``. Refused: a name holding an unescaped ``/`` (it would point past the entry);
   a ``~`` followed by anything but ``0`` or ``1``, a final ``~`` included (RFC 6901 allows no other escape); and a
   ``%`` anywhere in it. A ``$ref`` is a URI reference whose fragment RFC 6901 (section 6) percent-encodes, so
   ``a%20b`` may mean the entry ``a b`` as well as ``a%20b``; neither reading is guessed, the reference is refused
   (fail closed). Pydantic never writes a ``%`` in a ``$defs`` name: it rewrites every character of a model's name
   outside ``A-Z``, ``a-z``, ``0-9``, ``.``, ``_`` and ``-`` to ``_``, so a prepared schema never meets this.
2. **Pointers** (:func:`json_pointer`). Every pointer a transform writes, into a refusal's message or a cycle's
   path, escapes its token the same way: ``~`` as ``~0`` first, then ``/`` as ``~1``, so a name read by
   :func:`def_name` and written by :func:`json_pointer` round-trips.
3. **Cycles** (:func:`ref_cycle`). The Anthropic transform, OpenAI's strict transform and both Gemini transforms
   refuse a schema whose ``$defs`` entries refer to one another in a cycle (a recursive schema), whether objects,
   arrays, ``anyOf`` members or a ``$ref`` alone sit on it. ``ref_cycle`` reads a ``$ref`` in exactly these places
   of an entry's body: a ``properties`` value, ``items`` (one schema), an ``anyOf`` member, a schema-valued
   ``additionalProperties``, and the same keywords beside a ``$ref`` (its siblings: the Gemini API keeps them,
   Google Cloud's ``responseSchema`` merges them into the entry it inlines), at any depth. It reads nothing else:
   not a ``$defs`` below the root (a local reference names the root's entries only), not a value (``default``,
   ``enum``, ``const``, ``examples``), not a tuple ``items``, and not the keywords of :data:`UNREAD_KEYWORDS`
   (``oneOf``, ``allOf``, ``not``, ``if``, ``prefixItems``, ``patternProperties``, ``$id``, ...), which
   ``schema.prepare()`` and every one of those transforms refuse by name, so no ``$ref`` hides under one. An entry
   no ``$ref`` reaches is checked too. OpenAI's ``strict=False`` is exempt: it sends the schema verbatim, with no
   check at all (``openai_compatible`` module notes, 4). The first cycle in name order is returned as the names on
   it, the first repeated at the end; :func:`cycle_text` writes it as the path ``#/$defs/A -> #/$defs/B ->
   #/$defs/A``, at most :data:`CYCLE_HOPS_SHOWN` hops and then ``-> … (N entries)``, each name cut at
   :data:`NAME_CHARS` characters, so a refusal stays short however long the cycle or its names. A transform refuses
   at ``#/$defs``, in its own words. :func:`~datagrunt.rationalization.schema.prepare` already refuses a recursive
   model, so this is defense in depth for a direct caller of a transform.

Bounds: :func:`ref_cycle` reads at most ``max_nodes`` nodes of the entries' bodies, each once (a ``$ref`` is not
followed), iteratively, and past them raises :class:`NodeBoundError`, which each transform re-raises in its own words
at ``#/$defs`` (a ``$ref``'s siblings count, though Anthropic and OpenAI drop them: refusing there fails closed). The
cycle search then takes one step per reference read or entry finished, never an entry twice, and its loop stops at
``2 * (entries + references) + 1`` steps, raising ``RuntimeError`` (a bug) rather than walking every path of a shared
graph. The module imports only the standard library and the package's error: no pydantic, no httpx, nothing from
``datagrunt.core``, and no import inside a function (invariant 9).
"""

import re
from collections.abc import Mapping, Sequence
from typing import Any

from datagrunt.rationalization.exceptions import RationalizationError

DEFS_PREFIX = "#/$defs/"
"""How a local reference to an entry of the root's ``$defs`` starts."""

DEFS_POINTER = "#/$defs"
"""The JSON pointer of the root's ``$defs``: an entry's pointer is ``json_pointer(DEFS_POINTER, name)``."""

CYCLE_HOPS_SHOWN = 10
"""The most hops of a cycle :func:`cycle_text` writes out; a longer cycle ends with its size instead."""

NAME_CHARS = 120
"""The most characters of a ``$defs`` name :func:`cycle_text` shows."""

UNREAD_KEYWORDS = frozenset(
    {
        # Combine or condition schemas.
        "oneOf",
        "allOf",
        "not",
        "if",
        "then",
        "else",
        # Hold subschemas no transform reads.
        "patternProperties",
        "prefixItems",
        "additionalItems",
        "contains",
        "propertyNames",
        "dependentSchemas",
        "dependencies",
        "unevaluatedProperties",
        "unevaluatedItems",
        "contentSchema",
        "definitions",
        # Change what a $ref resolves to.
        "$dynamicRef",
        "$recursiveRef",
        "$id",
        "$anchor",
        "$dynamicAnchor",
        "$recursiveAnchor",
    }
)
"""The keywords :func:`ref_cycle` does not read, which ``schema.prepare()`` and every transform refuse by name
(module notes, 3): dropping one would loosen the schema silently, and a ``$ref`` under one would escape the check."""

_BAD_ESCAPE = re.compile("~(?![01])")  # RFC 6901 escapes only "~" (as ~0) and "/" (as ~1)


class NodeBoundError(RationalizationError):
    """:func:`ref_cycle`'s refusal of ``$defs`` bodies past its node bound. A transform catches it and refuses in its
    own words at ``#/$defs``; uncaught, it still fails closed as a ``RationalizationError``."""


def def_name(ref: object) -> str | None:
    """The ``$defs`` entry a local ``#/$defs/<name>`` reference names, RFC 6901 unescaped (module notes, 1).

    Args:
        ref: A ``$ref`` value, of any type.

    Returns:
        The entry's name; ``None`` when ``ref`` is not a ``str`` starting with :data:`DEFS_PREFIX`, or its name
        holds an unescaped ``/`` (a pointer past the entry, or a name not escaped), a ``~`` that is not ``~0`` or
        ``~1``, or a ``%`` (a percent-encoding is never guessed at).
    """
    if not isinstance(ref, str) or not ref.startswith(DEFS_PREFIX):
        return None
    token = ref[len(DEFS_PREFIX) :]
    if "/" in token or "%" in token:
        return None
    if "~" not in token:
        return token
    if _BAD_ESCAPE.search(token):
        return None
    return token.replace("~1", "/").replace("~0", "~")


def json_pointer(base: str, token: str) -> str:
    """``base`` extended by one JSON-pointer reference ``token``, RFC 6901 escaped (module notes, 2).

    Args:
        base: A JSON pointer (``"#"``, ``"#/properties"``, ...).
        token: A property or ``$defs`` name, as written in the schema.

    Returns:
        ``base/token``, ``~`` written as ``~0`` and ``/`` as ``~1``.
    """
    if not isinstance(base, str) or not isinstance(token, str):
        raise TypeError("base and token must be str")
    if "~" not in token and "/" not in token:
        return f"{base}/{token}"
    return f"{base}/{token.replace('~', '~0').replace('/', '~1')}"


def ref_cycle(definitions: Mapping[str, Any], *, max_nodes: int) -> tuple[str, ...] | None:
    """The first ``$ref`` cycle among the root's ``$defs`` entries, in name order (module notes, 3).

    A reference that is malformed or names no entry is no edge here: the transform refuses it where it reads it.

    Args:
        definitions: The root's ``$defs``: entry names to their schemas.
        max_nodes: The most nodes of the entries' bodies to read.

    Returns:
        The names on the cycle, its first entry repeated at the end (``("A", "B", "A")``); ``None`` when there is
        none.

    Raises:
        TypeError: If ``definitions`` is not a ``Mapping`` or ``max_nodes`` not an ``int``.
        ValueError: If ``max_nodes`` is below 1.
        NodeBoundError: If the bodies hold more than ``max_nodes`` nodes (a transform re-raises it in its own words).
    """
    if not isinstance(definitions, Mapping):
        raise TypeError(f"definitions must be a Mapping, got {type(definitions).__name__}")
    if type(max_nodes) is not int:
        raise TypeError(f"max_nodes must be an int, got {type(max_nodes).__name__}")
    if max_nodes < 1:
        raise ValueError("max_nodes must be at least 1")
    if not definitions:
        return None
    edges = _edges(definitions, max_nodes)
    budget = 2 * (len(edges) + sum(len(targets) for targets in edges.values())) + 1
    done: set[str] = set()
    for start in sorted(edges):
        if start not in done:
            cycle = _cycle_from(start, edges, done, budget)
            if cycle is not None:
                return cycle
    return None


def cycle_text(cycle: Sequence[str]) -> str:
    """The cycle :func:`ref_cycle` found as a path of JSON pointers, bounded for a message (module notes, 3).

    Args:
        cycle: The names on the cycle, its first entry repeated at the end.

    Returns:
        ``#/$defs/A -> #/$defs/B -> #/$defs/A``: at most :data:`CYCLE_HOPS_SHOWN` hops, then ``-> … (N entries)``,
        each name past :data:`NAME_CHARS` characters cut and ending with ``…``.
    """
    shown = " -> ".join(json_pointer(DEFS_POINTER, _shown_name(name)) for name in cycle[: CYCLE_HOPS_SHOWN + 1])
    if len(cycle) <= CYCLE_HOPS_SHOWN + 1:
        return shown
    return f"{shown} -> … ({len(cycle) - 1:,} entries)"


def _shown_name(name: str) -> str:
    """``name``, or its first :data:`NAME_CHARS` characters and ``…`` when it is longer."""
    return name if len(name) <= NAME_CHARS else f"{name[:NAME_CHARS]}…"


def _push_subschemas(stack: list[tuple[str, object]], owner: str, node: Mapping[str, Any]) -> None:
    """Push the subschemas under ``node`` that a transform reads directly onto ``stack``."""
    properties = node.get("properties")
    if isinstance(properties, Mapping):
        for child in properties.values():
            stack.append((owner, child))
    items = node.get("items")
    if isinstance(items, Mapping):
        stack.append((owner, items))
    additional = node.get("additionalProperties")
    if isinstance(additional, Mapping):
        stack.append((owner, additional))
    members = node.get("anyOf")
    if isinstance(members, list):
        for member in members:
            stack.append((owner, member))


def _edges(definitions: Mapping[str, Any], max_nodes: int) -> dict[str, list[str]]:
    """Each entry's name and the entries its body refers to, sorted last first (so ``pop`` takes them in order).

    Raises:
        NodeBoundError: If the bodies hold more than ``max_nodes`` nodes.
    """
    found: dict[str, set[str]] = {name: set() for name in definitions}
    stack: list[tuple[str, object]] = list(definitions.items())
    for _ in range(max_nodes + 1):  # bounded: one node per pass, and a node is pushed once (no $ref is followed)
        if not stack:
            return {name: sorted(targets, reverse=True) for name, targets in found.items()}
        owner, node = stack.pop()
        if not isinstance(node, Mapping):
            continue
        target = def_name(node["$ref"]) if "$ref" in node else None
        if target is not None and target in found:
            found[owner].add(target)
        _push_subschemas(stack, owner, node)
    raise NodeBoundError(
        f"the response schema's $defs hold more than {max_nodes:,} schema node{'' if max_nodes == 1 else 's'}"
    )


def _cycle_from(start: str, edges: Mapping[str, list[str]], done: set[str], budget: int) -> tuple[str, ...] | None:
    """A depth-first search from ``start``, iterative: the first cycle it meets, or ``None``.

    Every entry it finishes joins ``done``, so neither it nor a later search enters it again: each step reads one
    reference or finishes one entry, at most ``entries + references`` steps in all. ``budget`` bounds the loop
    explicitly (:func:`ref_cycle` gives twice that, plus one), so a broken memory fails loud instead of walking every
    path of a shared graph.

    Raises:
        RuntimeError: If the search takes more than ``budget - 1`` steps (a bug: it never does).
    """
    path = [start]
    on_path = {start}
    pending = [list(edges[start])]
    for _ in range(budget):  # bounded: budget passes, one step each
        if not pending:
            return None
        if not pending[-1]:
            on_path.discard(path[-1])
            done.add(path.pop())
            pending.pop()
            continue
        following = pending[-1].pop()
        if following in on_path:
            return (*path[path.index(following) :], following)
        if following not in done:
            path.append(following)
            on_path.add(following)
            pending.append(list(edges[following]))
    raise RuntimeError(f"the $ref cycle search passed its bound of {budget} steps")
