"""The model providers rationalize() sends requests to (plan §4.5, R3).

This package exports the provider protocol :class:`ModelProvider`, :class:`CallableProvider` (a caller's own
function as a provider) and the provider errors: :class:`TransientProviderError` (worth a retry) and
:class:`PermanentProviderError`, both :class:`ProviderError`. The shared pieces live in
:mod:`~datagrunt.rationalization.providers.base` (model strings, endpoints, API keys) and
:mod:`~datagrunt.rationalization.providers.http` (the bounded HTTP helper the wire adapters use).

Importing this package imports neither httpx nor pydantic: only :mod:`~datagrunt.rationalization.providers.http`
imports httpx, and each wire adapter imports it itself.
"""

from datagrunt.rationalization.providers.base import (
    ModelProvider,
    PermanentProviderError,
    ProviderError,
    TransientProviderError,
)
from datagrunt.rationalization.providers.callable import CallableProvider

__all__ = ["CallableProvider", "ModelProvider", "PermanentProviderError", "ProviderError", "TransientProviderError"]
