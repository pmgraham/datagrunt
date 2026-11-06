"""The [rationalize] extra's guard (#360): ``rationalize()``, result types and HTTP providers, loaded on first access.

Internal: ``datagrunt`` (``rationalize``, ``RationalizedResult``) and ``datagrunt.rationalization``
resolve extra-dependent names through :func:`load`, from their module ``__getattr__`` (PEP 562).

``datagrunt.rationalization``'s schema marker (:data:`Key`), exception (:class:`RationalizationError`),
wire types, unit planner, envelope builder, cache, and base/callable/Google Cloud provider helpers need
only datagrunt's core dependencies. ``rationalize()``, the result/consensus/grounding/folding/runner types,
and the HTTP wire providers need the [rationalize] extra (``pydantic`` and ``httpx``), so neither
``import datagrunt`` nor ``import datagrunt.rationalization`` imports them: each name is imported on its
first access. Without the extra, the name resolves to a stand-in of the same name (a class, or a function),
and constructing or calling it raises ``ImportError`` with the install hint, naming the package that is
missing.

A name that loads is cached, so it is the same object at every access. A stand-in is never kept: each access
checks for the extra again, after ``importlib.invalidate_caches()`` on a miss, so an extra installed later in
the session loads at the next access.
"""

import functools
import importlib
import importlib.util
import inspect
import sys
from collections.abc import Callable
from types import MappingProxyType
from typing import NoReturn

RATIONALIZE_EXTRA_HINT = 'Install rationalization support with: uv pip install "datagrunt[rationalize]"'

# Each lazily loaded class, and the module that defines it.
LAZY_CLASSES = MappingProxyType(
    {
        "RationalizedResult": "datagrunt.rationalization.result",
        "RationalizeTelemetry": "datagrunt.rationalization.result",
        "GroundedRecord": "datagrunt.rationalization.folding",
        "ElementSource": "datagrunt.rationalization.folding",
        "QuarantinedItem": "datagrunt.rationalization.consensus",
        "FailedUnit": "datagrunt.rationalization.runner",
        "TelemetryEvent": "datagrunt.rationalization.runner",
        "OpenAICompatibleProvider": "datagrunt.rationalization.providers.openai_compatible",
        "AnthropicProvider": "datagrunt.rationalization.providers.anthropic",
        "GeminiProvider": "datagrunt.rationalization.providers.gemini",
    }
)
# Each lazily loaded function, and the module that defines it.
LAZY_FUNCTIONS = MappingProxyType({"rationalize": "datagrunt.rationalization.api"})

# The [rationalize] extra's top-level import names, each with its package's name.
_EXTRA_PACKAGES = MappingProxyType({"pydantic": "pydantic", "httpx": "httpx"})
EXTRA_IMPORTS = tuple(_EXTRA_PACKAGES)


class _MissingExtra(Exception):
    """Raised by :func:`_loaded` when the [rationalize] extra is missing: which packages, and the cause."""

    def __init__(self, missing: tuple[str, ...], cause: ImportError) -> None:
        super().__init__(missing, cause)
        self.missing = missing
        self.cause = cause


def load(name: str) -> Callable[..., object]:
    """The class or function ``name`` (in :data:`LAZY_CLASSES` or :data:`LAZY_FUNCTIONS`), or its stand-in.

    The stand-in is returned when the [rationalize] extra is missing at two checks, the import system's caches
    invalidated between them, and is never kept: the next call looks for the extra again (see the module notes).

    Raises:
        KeyError: If ``name`` is not lazily loaded.
        ImportError: As the import raises it, unless the [rationalize] extra is what is missing.
        TypeError: If the module's ``name`` is not the class or function it should be.
    """
    try:
        return _loaded(name)
    except _MissingExtra:
        importlib.invalidate_caches()
    try:
        return _loaded(name)
    except _MissingExtra as missing:
        return _stand_in(name in LAZY_CLASSES, name, missing.missing, missing.cause)


@functools.cache
def _loaded(name: str) -> Callable[..., object]:
    """The class or function ``name``, imported at the first call that finds the [rationalize] extra, then cached.

    ``functools.cache`` keeps only a returned value, never a raised exception, so a missing extra is not cached.

    Raises:
        _MissingExtra: If the [rationalize] extra is missing.
        KeyError: If ``name`` is not lazily loaded.
        ImportError: As the import raises it, unless the [rationalize] extra is what is missing.
        TypeError: If the module's ``name`` is not the class or function it should be.
    """
    is_class = name in LAZY_CLASSES
    module_name = LAZY_CLASSES[name] if is_class else LAZY_FUNCTIONS[name]
    missing = missing_extra()
    if missing:
        raise _MissingExtra(missing, ModuleNotFoundError(f"No module named {missing[0]!r}", name=missing[0]))
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        if not _is_missing_extra(exc):
            raise
        raise _MissingExtra((_root(exc),), exc) from exc
    value = getattr(module, name)
    if not (isinstance(value, type) if is_class else inspect.isfunction(value)):
        raise TypeError(f"{module_name}.{name} is not a {'class' if is_class else 'function'}")
    return value


def missing_extra() -> tuple[str, ...]:
    """The [rationalize] extra's import names that are not installed, in order, found without importing them."""
    return tuple(module for module in _EXTRA_PACKAGES if not _installed(module))


def _installed(module: str) -> bool:
    """True if the top-level ``module`` can be imported: imported already, or a finder has its spec."""
    if module in sys.modules:
        return sys.modules[module] is not None
    return importlib.util.find_spec(module) is not None


def _root(exc: ImportError) -> str:
    return (exc.name or "").partition(".")[0]


def _is_missing_extra(exc: ImportError) -> bool:
    """True if ``exc`` failed to import pydantic or httpx (or a module inside one)."""
    return _root(exc) in _EXTRA_PACKAGES


def _hint(name: str, missing: tuple[str, ...], cause: ImportError) -> str:
    """The install hint for ``name``: which of the extra's packages is missing, and why."""
    packages = " and ".join(_EXTRA_PACKAGES[module] for module in missing)
    state = "is not installed" if len(missing) == 1 else "are not installed"
    return (
        f"{name} needs the [rationalize] extra (pydantic and httpx), and {packages} {state} ({cause}). "
        f"{RATIONALIZE_EXTRA_HINT}"
    )


def _stand_in(is_class: bool, name: str, missing: tuple[str, ...], cause: ImportError) -> Callable[..., object]:
    """The stand-in for ``name``: a class, or a function (see the module notes)."""
    if is_class:
        return _stand_in_class(name, missing, cause)
    return _stand_in_function(name, missing, cause)


def _stand_in_class(name: str, missing: tuple[str, ...], cause: ImportError) -> type:
    """A class named ``name`` whose construction raises ``ImportError`` with the install hint, from ``cause``."""
    message = _hint(name, missing, cause)

    def refuse(cls: type, *args: object, **kwargs: object) -> NoReturn:
        raise ImportError(message, name=missing[0]) from cause

    return type(
        name,
        (),
        {"__new__": refuse, "__doc__": message, "__module__": "datagrunt.rationalization", "__slots__": ()},
    )


def _stand_in_function(name: str, missing: tuple[str, ...], cause: ImportError) -> Callable[..., NoReturn]:
    """A function named ``name`` whose call raises ``ImportError`` with the install hint, from ``cause``."""
    message = _hint(name, missing, cause)

    def refuse(*args: object, **kwargs: object) -> NoReturn:
        raise ImportError(message, name=missing[0]) from cause

    refuse.__name__ = refuse.__qualname__ = name
    refuse.__doc__ = message
    refuse.__module__ = "datagrunt.rationalization"
    return refuse
