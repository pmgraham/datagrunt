"""The ``Key`` marker: which fields of a caller's schema identify an entity (spec §4.1).

A schema marks each identity field with ``Key`` in its ``Annotated`` metadata, ``Annotated[str, Key]``: records
whose key values are equal describe the same entity. ``Key("part")`` also names the entity's namespace, so schemas
or corpora that mark their keys with the same namespace share one. A namespace is stripped and lower-cased
(``str.lower``) when it is named, so ``Key("Part") == Key(" part ")``.

A marker is an immutable value: hashable, compared by its namespace alone (``Key == _KeyMarker()``), and the same
after a copy or a pickle round trip. ``Key`` itself has no namespace.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class _KeyMarker:
    """Marks entity identity fields; call as Key('part') to share an entity namespace across schemas/corpora.

    Build markers through :data:`Key` (``Key``, ``Key("part")``). Direct construction is checked, not normalized:
    the namespace must already be what ``Key(namespace)`` makes of it.

    Args:
        namespace: ``None`` for the bare ``Key``, else a non-empty, stripped, lower-case namespace.

    Raises:
        TypeError: If ``namespace`` is neither ``None`` nor a ``str``.
        ValueError: If ``namespace`` is a ``str`` that is empty, not stripped, or not lower-case.
    """

    namespace: str | None = None

    def __post_init__(self) -> None:
        if self.namespace is None:
            return
        if not isinstance(self.namespace, str):
            raise TypeError(
                f"Key namespace must be a str or None, got {self.namespace!r} ({type(self.namespace).__name__})"
            )
        if not self.namespace or self.namespace != self.namespace.strip().lower():
            raise ValueError(
                f"Key namespace must be non-empty, stripped and lower-case, got {self.namespace!r}: "
                f"Key(namespace) normalizes one"
            )

    def __call__(self, namespace: str) -> "_KeyMarker":
        """The marker for ``namespace``, stripped and lower-cased: ``Key("Part") == Key("part")``.

        Raises:
            TypeError: If this marker has a namespace already (``Key("a")("b")``), or ``namespace`` is not a
                ``str``.
            ValueError: If ``namespace`` is empty or only whitespace.
        """
        if self.namespace is not None:
            raise TypeError(f"{self!r} already has a namespace: name one once, as Key({namespace!r})")
        if not isinstance(namespace, str):
            raise TypeError(f"Key namespace must be a str, got {namespace!r} ({type(namespace).__name__})")
        if not namespace or not namespace.strip():
            raise ValueError("Key namespace must be non-empty")
        return _KeyMarker(namespace=namespace.strip().lower())

    def __repr__(self) -> str:
        return "Key" if self.namespace is None else f"Key({self.namespace!r})"


# The marker for a schema's identity fields (module notes): ``Annotated[str, Key]``, or ``Key("part")``.
Key = _KeyMarker()
