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
``ImportError`` with the install hint, chained from the import's own error.
``from datagrunt import *`` and ``from datagrunt.pdf import load_bundle``
still work there; the error comes at first use.

Only a missing extra becomes the hint: any other import failure (a broken
datagrunt module, say) propagates unchanged. Each name resolves once, then is
cached, so it is the same object at every access.
"""

import functools
import importlib
import inspect
from collections.abc import Callable
from types import MappingProxyType
from typing import NoReturn

PDF_EXTRA_HINT = 'Install PDF support with: uv pip install "datagrunt[pdf]"'

# Each lazily loaded class, and the module that defines it.
LAZY_CLASSES = MappingProxyType({"PDFReader": "datagrunt.pdf.reader", "PDFWriter": "datagrunt.pdf.writer"})
# Each lazily loaded function, and the module that defines it.
LAZY_FUNCTIONS = MappingProxyType({"load_bundle": "datagrunt.pdf.bundle", "write_bundle": "datagrunt.pdf.bundle"})

# The [pdf] extra's top-level import names: pypdfium2, and Pillow's PIL.
_EXTRA_IMPORTS = frozenset({"pypdfium2", "PIL"})


@functools.cache
def load(name: str) -> Callable[..., object]:
    """The class or function ``name`` (in :data:`LAZY_CLASSES` or :data:`LAZY_FUNCTIONS`), or its stand-in.

    The stand-in is returned when the [pdf] extra is missing (see the module notes).

    Raises:
        KeyError: If ``name`` is not lazily loaded.
        ImportError: As the import raises it, unless the [pdf] extra is what is missing.
        TypeError: If the module's ``name`` is not the class or function it should be.
    """
    is_class = name in LAZY_CLASSES
    module_name = LAZY_CLASSES[name] if is_class else LAZY_FUNCTIONS[name]
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        if not _is_missing_extra(exc):
            raise
        return _stand_in_class(name, exc) if is_class else _stand_in_function(name, exc)
    value = getattr(module, name)
    if not (isinstance(value, type) if is_class else inspect.isfunction(value)):
        raise TypeError(f"{module_name}.{name} is not a {'class' if is_class else 'function'}")
    return value


def _is_missing_extra(exc: ImportError) -> bool:
    """True if ``exc`` failed to import pypdfium2 or Pillow (or a module inside one)."""
    return (exc.name or "").partition(".")[0] in _EXTRA_IMPORTS


def _hint(name: str, cause: ImportError) -> str:
    return f"{name} needs the [pdf] extra (pypdfium2 and Pillow): {cause}. {PDF_EXTRA_HINT}"


def _stand_in_class(name: str, cause: ImportError) -> type:
    """A class named ``name`` whose construction raises ``ImportError`` with the install hint, from ``cause``."""
    message, missing = _hint(name, cause), cause.name

    def refuse(cls: type, *args: object, **kwargs: object) -> NoReturn:
        raise ImportError(message, name=missing) from cause

    return type(name, (), {"__new__": refuse, "__doc__": message, "__module__": "datagrunt.pdf", "__slots__": ()})


def _stand_in_function(name: str, cause: ImportError) -> Callable[..., NoReturn]:
    """A function named ``name`` whose call raises ``ImportError`` with the install hint, from ``cause``."""
    message, missing = _hint(name, cause), cause.name

    def refuse(*args: object, **kwargs: object) -> NoReturn:
        raise ImportError(message, name=missing) from cause

    refuse.__name__ = refuse.__qualname__ = name
    refuse.__doc__ = message
    refuse.__module__ = "datagrunt.pdf"
    return refuse
