"""datagrunt's schema-driven extraction over v5 PDF elements (5.1, #360).

Its purpose is to read records of a caller's schema from the visible elements of a :class:`~datagrunt.pdf.PDFDocument`.
This package exports the schema marker :data:`Key`, :class:`RationalizationError`, the entry point :func:`rationalize`,
the result and provenance types (:class:`RationalizedResult`, :class:`GroundedRecord`, :class:`ElementSource`,
:class:`QuarantinedItem`, :class:`FailedUnit`, :class:`SkippedUnit`, :class:`RationalizeTelemetry`,
:class:`TelemetryEvent`), and the provider protocol, wire types and adapters (:class:`ModelRequest`,
:class:`ProviderResponse`, :class:`ModelProvider`, :class:`OpenAICompatibleProvider`, :class:`AnthropicProvider`,
:class:`GeminiProvider`, :class:`CallableProvider`, :class:`GoogleCloudAuth`, :class:`TransientProviderError`,
:class:`PermanentProviderError`). ``datagrunt`` re-exports :data:`Key`, :func:`rationalize` and
:class:`RationalizedResult`.

``Key`` and ``RationalizationError`` are imported eagerly; every other name is resolved on first access
(``__getattr__``, PEP 562), so importing this package never imports ``pydantic``, ``httpx``, ``polars`` or ``pyarrow``.
Without the [rationalize] extra, constructing an extra-dependent class or calling :func:`rationalize` raises
``ImportError`` with the install hint (:mod:`datagrunt.rationalization._extra`).
"""

from importlib import import_module as _import_module
from types import MappingProxyType as _MappingProxyType
from typing import TYPE_CHECKING

from datagrunt.rationalization._extra import LAZY_CLASSES as _LAZY_CLASSES
from datagrunt.rationalization._extra import LAZY_FUNCTIONS as _LAZY_FUNCTIONS
from datagrunt.rationalization._extra import load as _load
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.markers import Informational, Key

__all__ = [
    "Key",
    "Informational",
    "RationalizationError",
    "rationalize",
    "RationalizedResult",
    "GroundedRecord",
    "ElementSource",
    "QuarantinedItem",
    "FailedUnit",
    "SkippedUnit",
    "RationalizeTelemetry",
    "TelemetryEvent",
    "ModelRequest",
    "ProviderResponse",
    "ModelProvider",
    "OpenAICompatibleProvider",
    "AnthropicProvider",
    "GeminiProvider",
    "CallableProvider",
    "GoogleCloudAuth",
    "TransientProviderError",
    "PermanentProviderError",
    "BatchItem",
    "BatchPlan",
    "prepare_batch",
    "ingest_batch",
]

if TYPE_CHECKING:
    from datagrunt.rationalization.api import rationalize
    from datagrunt.rationalization.batch import BatchItem, BatchPlan, ingest_batch, prepare_batch
    from datagrunt.rationalization.consensus import QuarantinedItem
    from datagrunt.rationalization.folding import ElementSource, GroundedRecord
    from datagrunt.rationalization.providers.anthropic import AnthropicProvider
    from datagrunt.rationalization.providers.base import (
        ModelProvider,
        PermanentProviderError,
        TransientProviderError,
    )
    from datagrunt.rationalization.providers.callable import CallableProvider
    from datagrunt.rationalization.providers.gemini import GeminiProvider
    from datagrunt.rationalization.providers.google_cloud import GoogleCloudAuth
    from datagrunt.rationalization.providers.openai_compatible import OpenAICompatibleProvider
    from datagrunt.rationalization.result import RationalizedResult, RationalizeTelemetry
    from datagrunt.rationalization.runner import FailedUnit, TelemetryEvent
    from datagrunt.rationalization.units import SkippedUnit
    from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

_LAZY_NAMES = frozenset({*_LAZY_CLASSES, *_LAZY_FUNCTIONS})
_LIGHT_MODULES = _MappingProxyType(
    {
        "SkippedUnit": "datagrunt.rationalization.units",
        "ModelRequest": "datagrunt.rationalization.wire",
        "ProviderResponse": "datagrunt.rationalization.wire",
        "ModelProvider": "datagrunt.rationalization.providers.base",
        "CallableProvider": "datagrunt.rationalization.providers.callable",
        "GoogleCloudAuth": "datagrunt.rationalization.providers.google_cloud",
        "TransientProviderError": "datagrunt.rationalization.providers.base",
        "PermanentProviderError": "datagrunt.rationalization.providers.base",
    }
)


def __getattr__(name: str) -> object:
    """Resolve public rationalization symbols on first access (see module notes)."""
    if name in _LAZY_NAMES:
        return _load(name)
    if name in _LIGHT_MODULES:
        return getattr(_import_module(_LIGHT_MODULES[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
