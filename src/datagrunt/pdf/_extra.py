"""The [pdf] extra's guard (#373): the reader, the writer and the bundle functions, loaded on first access.

Internal: ``datagrunt`` (``PDFReader``, ``PDFWriter``) and ``datagrunt.pdf``
(those and ``load_bundle``, ``write_bundle``) resolve these names through
:func:`load`, from their module ``__getattr__`` (PEP 562).

``datagrunt.pdf``'s element types and exceptions need only datagrunt's core
dependencies. Its reader, writer and bundle functions need the [pdf] extra,
pypdfium2 and Pillow, so neither ``import datagrunt`` nor
``import datagrunt.pdf`` imports them: each name is imported on its first
access. Without the extra, the name resolves to a stand-in of the same name
(a class, or a function), and constructing or calling it raises
``ImportError`` with the install hint, naming the package that is missing.
``from datagrunt import *`` and ``from datagrunt.pdf import load_bundle``
still work there; the error comes at first use.

The extra is missing when either import name (``pypdfium2``, ``PIL``) has no
module spec (``importlib.util.find_spec``, or ``None`` in ``sys.modules``):
checked before the module is imported, because the caller never imports
pypdfium2 (every pdfium call runs in a worker process, #443), so its absence
would otherwise first show in a worker. A package that is found but fails to
import is a missing extra too, by the ImportError's name. Only a missing
extra becomes the hint: any other import failure (a broken datagrunt module,
say) propagates unchanged.

A name that loads is cached, so it is the same object at every access. A
stand-in is never kept: each access checks for the extra again, so one
installed later in the session (a notebook's ``%pip install``) loads at the
next. A miss is believed only after ``importlib.invalidate_caches()`` and a
second check, since the import system's finders keep each directory's listing
until its modification time changes (an install within the file system's time
granularity does not change it), and never search a ``sys.path`` directory
created after they first looked for it.
"""

import functools
import importlib
import importlib.util
import inspect
import sys
from collections.abc import Callable
from types import MappingProxyType
from typing import NoReturn

PDF_EXTRA_HINT = 'Install PDF support with: uv pip install "datagrunt[pdf]"'

# Each lazily loaded class, and the module that defines it.
LAZY_CLASSES = MappingProxyType({"PDFReader": "datagrunt.pdf.reader", "PDFWriter": "datagrunt.pdf.writer"})
# Each lazily loaded function, and the module that defines it.
LAZY_FUNCTIONS = MappingProxyType({"write_bundle": "datagrunt.pdf.bundle", "extract_corpus": "datagrunt.pdf.corpus"})

# The [pdf] extra's top-level import names, each with its package's name: pypdfium2, and Pillow's PIL.
_EXTRA_PACKAGES = MappingProxyType({"pypdfium2": "pypdfium2", "PIL": "Pillow"})
EXTRA_IMPORTS = tuple(_EXTRA_PACKAGES)


class _MissingExtra(Exception):
    """Raised by :func:`_loaded` when the [pdf] extra is missing: which packages (import names), and the cause."""

    def __init__(self, missing: tuple[str, ...], cause: ImportError) -> None:
        super().__init__(missing, cause)
        self.missing = missing
        self.cause = cause


def load(name: str) -> Callable[..., object]:
    """The class or function ``name`` (in :data:`LAZY_CLASSES` or :data:`LAZY_FUNCTIONS`), or its stand-in.

    The stand-in is returned when the [pdf] extra is missing at two checks, the import system's caches invalidated
    between them, and is never kept: the next call looks for the extra again (see the module notes).

    Raises:
        KeyError: If ``name`` is not lazily loaded.
        ImportError: As the import raises it, unless the [pdf] extra is what is missing.
        TypeError: If the module's ``name`` is not the class or function it should be.
    """
    try:
        return _loaded(name)
    except _MissingExtra:
        importlib.invalidate_caches()  # a finder's directory listing can predate the install (module notes)
    try:
        return _loaded(name)
    except _MissingExtra as missing:
        return _stand_in(name in LAZY_CLASSES, name, missing.missing, missing.cause)


@functools.cache
def _loaded(name: str) -> Callable[..., object]:
    """The class or function ``name``, imported at the first call that finds the [pdf] extra, then cached.

    ``functools.cache`` keeps only a returned value, never a raised exception, so a missing extra is not cached.

    Raises:
        _MissingExtra: If the [pdf] extra is missing.
        KeyError: If ``name`` is not lazily loaded.
        ImportError: As the import raises it, unless the [pdf] extra is what is missing.
        TypeError: If the module's ``name`` is not the class or function it should be.
    """
    is_class = name in LAZY_CLASSES
    module_name = LAZY_CLASSES[name] if is_class else LAZY_FUNCTIONS[name]
    missing = missing_extra()
    if missing:
        raise _MissingExtra(missing, ModuleNotFoundError(f"No module named {missing[0]!r}", name=missing[0]))
    try:
        importlib.import_module("PIL.Image")
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
    """The [pdf] extra's import names that are not installed, in order, found without importing them."""
    return tuple(module for module in _EXTRA_PACKAGES if not _installed(module))


def _installed(module: str) -> bool:
    """True if the top-level ``module`` can be imported: imported already, or a finder has its spec."""
    if module in sys.modules:
        return sys.modules[module] is not None  # None: its import is blocked
    return importlib.util.find_spec(module) is not None


def missing_in_worker(exc: ImportError) -> ImportError | None:
    """The install hint, as an ImportError, for a worker that failed to import the extra (``exc``); else None.

    A worker reports it to its caller (:func:`datagrunt.pdf._process.missing_extra_reply`), which raises it:
    the extra was there when the reader or writer was built, and is gone where the worker runs.
    """
    if not _is_missing_extra(exc):
        return None
    module = _root(exc)
    message = (
        f"datagrunt reads PDFs in worker processes, and a worker could not import {_EXTRA_PACKAGES[module]}, "
        f"part of the [pdf] extra (pypdfium2 and Pillow): {exc}. {PDF_EXTRA_HINT}"
    )
    return ImportError(message, name=module)


def _root(exc: ImportError) -> str:
    return (exc.name or "").partition(".")[0]


def _is_missing_extra(exc: ImportError) -> bool:
    """True if ``exc`` failed to import pypdfium2 or Pillow (or a module inside one)."""
    return _root(exc) in _EXTRA_PACKAGES


def _hint(name: str, missing: tuple[str, ...], cause: ImportError) -> str:
    """The install hint for ``name``: which of the extra's packages is missing (``missing``, import names), and why."""
    packages = " and ".join(_EXTRA_PACKAGES[module] for module in missing)
    state = "is not installed" if len(missing) == 1 else "are not installed"
    return f"{name} needs the [pdf] extra (pypdfium2 and Pillow), and {packages} {state} ({cause}). {PDF_EXTRA_HINT}"


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

    return type(name, (), {"__new__": refuse, "__doc__": message, "__module__": "datagrunt.pdf", "__slots__": ()})


def _stand_in_function(name: str, missing: tuple[str, ...], cause: ImportError) -> Callable[..., NoReturn]:
    """A function named ``name`` whose call raises ``ImportError`` with the install hint, from ``cause``."""
    message = _hint(name, missing, cause)

    def refuse(*args: object, **kwargs: object) -> NoReturn:
        raise ImportError(message, name=missing[0]) from cause

    refuse.__name__ = refuse.__qualname__ = name
    refuse.__doc__ = message
    refuse.__module__ = "datagrunt.pdf"
    return refuse
