"""The [pdf] extra's guard (#373): ``PDFReader`` and ``PDFWriter``, loaded on first access.

Internal: ``datagrunt`` and ``datagrunt.pdf`` resolve the two names through
:func:`load_class`, from their module ``__getattr__`` (PEP 562).

``datagrunt.pdf``'s element types and exceptions need only datagrunt's core
dependencies. Its reader and writer need the [pdf] extra, pypdfium2 and
Pillow, so neither ``import datagrunt`` nor ``import datagrunt.pdf`` imports
them: each class is imported on its first access. Without the extra, the
name resolves to a stand-in class of the same name, and constructing it
raises ``ImportError`` with the install hint, chained from the import's own
error. ``from datagrunt import *`` and ``from datagrunt import PDFReader``
still work there; the error comes at first use.

Only a missing extra becomes the hint: any other import failure (a broken
datagrunt module, say) propagates unchanged. Each name resolves once, then is
cached, so it is the same object at every access.
"""

import functools
import importlib
from types import MappingProxyType
from typing import NoReturn

PDF_EXTRA_HINT = 'Install PDF support with: uv pip install "datagrunt[pdf]"'

# Each lazily loaded class, and the module that defines it.
LAZY_CLASSES = MappingProxyType({"PDFReader": "datagrunt.pdf.reader", "PDFWriter": "datagrunt.pdf.writer"})

# The [pdf] extra's top-level import names: pypdfium2, and Pillow's PIL.
_EXTRA_IMPORTS = frozenset({"pypdfium2", "PIL"})


@functools.cache
def load_class(name: str) -> type:
    """The class ``name`` (one of :data:`LAZY_CLASSES`), or its stand-in when the [pdf] extra is missing.

    Raises:
        KeyError: If ``name`` is not a lazily loaded class.
        ImportError: As the import raises it, unless the [pdf] extra is what is missing.
    """
    module_name = LAZY_CLASSES[name]
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        if not _is_missing_extra(exc):
            raise
        return _stand_in(name, exc)
    cls = getattr(module, name)
    if not isinstance(cls, type):
        raise TypeError(f"{module_name}.{name} is not a class")
    return cls


def _is_missing_extra(exc: ImportError) -> bool:
    """True if ``exc`` failed to import pypdfium2 or Pillow (or a module inside one)."""
    return (exc.name or "").partition(".")[0] in _EXTRA_IMPORTS


def _stand_in(name: str, cause: ImportError) -> type:
    """A class named ``name`` whose construction raises ``ImportError`` with the install hint, from ``cause``."""
    message = f"{name} needs the [pdf] extra (pypdfium2 and Pillow): {cause}. {PDF_EXTRA_HINT}"
    missing = cause.name

    def refuse(cls: type, *args: object, **kwargs: object) -> NoReturn:
        raise ImportError(message, name=missing) from cause

    return type(name, (), {"__new__": refuse, "__doc__": message, "__module__": "datagrunt.pdf", "__slots__": ()})
