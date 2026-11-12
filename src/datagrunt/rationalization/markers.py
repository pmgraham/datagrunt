"""The ``Key`` and ``Informational`` markers for caller extraction schemas (spec §4.1).

A schema marks each identity field with ``Key`` in its ``Annotated`` metadata, ``Annotated[str, Key]``: records
whose key values are equal describe the same entity. ``Key("part")`` also names the entity's namespace, and
``Key(normalizer="alnum_upper")`` normalizes ASCII punctuation/spaces and case during consensus and folding while
grounding still verifies each model's raw key text against its cited elements and quotes.

``Informational`` marks non-disputing descriptive fields (e.g. ``Annotated[str | None, Informational]``) whose
free-text wording variance never discards an otherwise agreed record.
"""

from dataclasses import dataclass
from typing import Literal, TypeAlias

KeyNormalizer: TypeAlias = Literal["alnum_upper"]
KEY_NORMALIZERS: tuple[KeyNormalizer, ...] = ("alnum_upper",)

_UNSET = object()

__all__ = ["Informational", "KEY_NORMALIZERS", "Key", "KeyNormalizer"]


def _check_normalizer(normalizer: object) -> KeyNormalizer | None:
    if normalizer is None:
        return None
    if not isinstance(normalizer, str):
        raise TypeError(f"Key normalizer must be a str or None, got {normalizer!r} ({type(normalizer).__name__})")
    if normalizer not in KEY_NORMALIZERS:
        raise ValueError(f"Key normalizer must be one of {KEY_NORMALIZERS} or None, got {normalizer!r}")
    return "alnum_upper"


def _check_normalized_namespace(namespace: object) -> None:
    """Require ``namespace`` to be ``None`` or a non-empty, stripped, lower-case ``str``."""
    if namespace is None:
        return
    if not isinstance(namespace, str):
        raise TypeError(f"Key namespace must be a str or None, got {namespace!r} ({type(namespace).__name__})")
    if not namespace or namespace != namespace.strip().lower():
        raise ValueError(
            f"Key namespace must be non-empty, stripped and lower-case, got {namespace!r}: "
            f"Key(namespace) normalizes one"
        )


@dataclass(frozen=True, slots=True)
class _KeyMarker:
    """Marks entity identity fields; call as Key('part') or Key(normalizer='alnum_upper')."""

    namespace: str | None = None
    normalizer: KeyNormalizer | None = None

    def __post_init__(self) -> None:
        _check_normalizer(self.normalizer)
        _check_normalized_namespace(self.namespace)

    def __call__(
        self,
        namespace: object = _UNSET,
        *,
        normalizer: KeyNormalizer | None = None,
    ) -> "_KeyMarker":
        """Return a marker with ``namespace`` and/or ``normalizer`` configured."""
        if self.namespace is not None and namespace is not _UNSET:
            raise TypeError(f"{self!r} already has a namespace: name one once, as Key({namespace!r})")
        if self.normalizer is not None and normalizer is not None:
            raise TypeError(f"{self!r} already has a normalizer: set normalizer once")
        checked_norm = self.normalizer if normalizer is None else _check_normalizer(normalizer)
        if namespace is _UNSET:
            return _KeyMarker(namespace=self.namespace, normalizer=checked_norm)
        if not isinstance(namespace, str):
            raise TypeError(f"Key namespace must be a str, got {namespace!r} ({type(namespace).__name__})")
        if not namespace or not namespace.strip():
            raise ValueError("Key namespace must be non-empty")
        return _KeyMarker(namespace=namespace.strip().lower(), normalizer=checked_norm)

    def __repr__(self) -> str:
        if self.namespace is None and self.normalizer is None:
            return "Key"
        if self.normalizer is None:
            return f"Key({self.namespace!r})"
        if self.namespace is None:
            return f"Key(normalizer={self.normalizer!r})"
        return f"Key({self.namespace!r}, normalizer={self.normalizer!r})"


@dataclass(frozen=True, slots=True)
class _InformationalMarker:
    """Marks non-disputing descriptive fields on extraction schemas."""

    def __call__(self) -> "_InformationalMarker":
        return self

    def __repr__(self) -> str:
        return "Informational"


Key = _KeyMarker()
Informational = _InformationalMarker()
