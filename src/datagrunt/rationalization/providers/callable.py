"""A caller's own function as a model provider (plan §4.5, R3).

:class:`CallableProvider` turns ``fn(request)`` into a :class:`~datagrunt.rationalization.providers.base.ModelProvider`
of kind ``"callable"``: no network, no endpoint, ``generation_params`` empty, ``transform_schema`` the identity (a
fresh deep copy), and ``model_family`` the vendor lineage the caller declares with ``family=`` (the function's own
model: ``"meta"`` for a local Llama), else ``"unknown"``, which adds no family diversity to consensus. The rules of
``generate``:

1. ``fn`` gets the :class:`~datagrunt.rationalization.wire.ModelRequest` itself.
2. A :class:`~datagrunt.rationalization.wire.ProviderResponse` from ``fn`` is returned as is. A ``Mapping`` that is
   exactly JSON becomes ``ProviderResponse(content=<its copy>, raw_text=<its canonical JSON>, finish="complete")``
   with no token counts. Anything else, a mapping that is not exactly JSON included, is a developer error: a loud
   ``TypeError`` naming the provider.
3. Every exception ``fn`` raises propagates unchanged: a
   :class:`~datagrunt.rationalization.providers.base.TransientProviderError` (retried by the runner), a
   :class:`~datagrunt.rationalization.providers.base.PermanentProviderError` (which stops the run), or anything
   else (a bug, which stops it too).

:func:`as_provider` resolves one entry of ``models=`` that is not a string: a provider object is checked
(:func:`~datagrunt.rationalization.providers.base.check_provider`), an object holding only some provider members
is refused rather than treated as a callable, and a bare callable is wrapped under the id
``callable:<module>.<qualname>`` with the family ``"unknown"``. That id is not stable enough to key a persistent
cache (an edited function keeps its name), so a bare callable with ``cache_dir`` is refused: the caller wraps it in
``CallableProvider`` and so names, and versions, the model id. Model strings are resolved elsewhere (T14), never
here.

The module imports only the standard library and the package's own modules: no pydantic, no httpx, nothing from
``datagrunt.core``, and no import inside a function (invariant 9).
"""

from collections.abc import Callable, Mapping
from typing import Any

from datagrunt.rationalization._canonical import canonical_copy, canonical_json
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.providers.base import (
    PROVIDER_MEMBERS,
    UNKNOWN_FAMILY,
    ModelProvider,
    check_provider,
    family_problem,
    has_real_member,
    identity_problem,
)
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

ProviderFunction = Callable[[ModelRequest], object]
"""What a callable provider calls: a request in, a ``ProviderResponse`` or a JSON object out (``generate`` checks)."""


class CallableProvider:
    """A function as a model provider (module notes).

    Args:
        model_id: The model's id: a non-blank single-line ``str`` of at most 512 characters. With a persistent
            cache it keys every entry, so change it whenever ``fn`` answers differently.
        fn: Called with each :class:`~datagrunt.rationalization.wire.ModelRequest`.
        family: The model's vendor lineage (``"google"``, ``"anthropic"``, ``"openai"``, ``"meta"``, ...): exactly a
            ``str`` of 1-32 of ``a-z``, ``0-9`` and ``-``; ``"unknown"`` when not declared.

    Raises:
        TypeError: If ``model_id`` is not a ``str``, ``fn`` is not callable, or ``family`` is not exactly a ``str``.
        ValueError: If ``model_id`` is blank, longer than 512 characters, or not a single line, or ``family`` breaks
            its rule; no message quotes the family.
    """

    __slots__ = ("_family", "_fn", "_model_id")

    def __init__(self, model_id: str, fn: ProviderFunction, *, family: str = UNKNOWN_FAMILY) -> None:
        problem = identity_problem(model_id)
        if problem is not None:
            error = TypeError if not isinstance(model_id, str) else ValueError
            raise error(f"CallableProvider model_id {problem}")
        if not callable(fn):
            raise TypeError(f"CallableProvider fn must be callable, got {type(fn).__name__}")
        problem = family_problem(family)
        if problem is not None:
            refusal = TypeError if type(family) is not str else ValueError
            raise refusal(f"CallableProvider family {problem}")
        self._model_id = model_id
        self._fn = fn
        self._family = family

    @property
    def model_id(self) -> str:
        """The model id given."""
        return self._model_id

    @property
    def provider_kind(self) -> str:
        """Always ``"callable"``."""
        return "callable"

    @property
    def rate_key(self) -> str:
        """``"callable:"`` + the model id: one rate limit per callable model."""
        return f"callable:{self._model_id}"

    @property
    def model_family(self) -> str:
        """The family given, ``"unknown"`` by default."""
        return self._family

    @property
    def generation_params(self) -> dict[str, Any]:
        """A new empty mapping: a callable sends nothing that shapes its output."""
        return {}

    def transform_schema(self, schema: Mapping[str, Any]) -> dict[str, Any]:
        """Return ``schema`` as a fresh deep copy (equal to it).

        Raises:
            TypeError: If ``schema`` is not a ``Mapping``.
            RationalizationError: If it is not exactly JSON.
        """
        if not isinstance(schema, Mapping):
            raise TypeError(f"schema must be a Mapping, got {type(schema).__name__}")
        copy: dict[str, Any] = canonical_copy(dict(schema), subject="the response schema")
        return copy

    def generate(self, request: ModelRequest) -> ProviderResponse:
        """Answer ``request`` by calling the function (module notes, 1-3).

        Raises:
            TypeError: If ``request`` is not a ``ModelRequest``, or the function returns neither a
                ``ProviderResponse`` nor a mapping that is exactly JSON.
            ValueError: If the returned mapping is too large for a response (8 MiB of canonical JSON).
            Exception: Whatever the function raises, unchanged.
        """
        if not isinstance(request, ModelRequest):
            raise TypeError(f"generate needs a ModelRequest, got {type(request).__name__}")
        answer = self._fn(request)
        if isinstance(answer, ProviderResponse):
            return answer
        if not isinstance(answer, Mapping):
            raise TypeError(
                f"CallableProvider {self._model_id!r} returned {type(answer).__name__}: return a ProviderResponse "
                "or a JSON object (a Mapping)"
            )
        return self._wrap(answer)

    def _wrap(self, answer: Mapping[str, Any]) -> ProviderResponse:
        """A complete response holding ``answer``, which must be exactly JSON."""
        try:
            content: dict[str, Any] = canonical_copy(dict(answer), subject="the returned mapping")
        except RationalizationError as error:
            raise TypeError(
                f"CallableProvider {self._model_id!r} returned a mapping that is not JSON: {error}"
            ) from error
        try:
            return ProviderResponse(
                content=content,
                raw_text=canonical_json(content),
                finish="complete",
                input_tokens=None,
                output_tokens=None,
            )
        except ValueError as error:
            raise ValueError(f"CallableProvider {self._model_id!r} returned a mapping too large: {error}") from error

    def __repr__(self) -> str:
        return f"CallableProvider(model_id={self._model_id!r})"


def _bare_model_id(fn: object) -> str:
    """``callable:<module>.<qualname>`` of a function, or of its class when it has no qualname (a callable instance)."""
    module = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)
    if not isinstance(module, str) or not isinstance(qualname, str):
        module, qualname = type(fn).__module__, type(fn).__qualname__
    return f"callable:{module}.{qualname}"


def _attempts_a_provider(obj: object) -> bool:
    """Whether ``obj`` is meant as a provider: a real provider member, or a ``__getattr__`` answering one (a Mock)."""
    if any(has_real_member(obj, member) for member in PROVIDER_MEMBERS):
        return True
    if not has_real_member(type(obj), "__getattr__"):
        return False
    try:
        return any(hasattr(obj, member) for member in PROVIDER_MEMBERS)
    except Exception:  # a __getattr__ raising something else still meant to answer: check_provider names it
        return True


def as_provider(obj: object, *, persistent_cache: bool) -> ModelProvider:
    """Resolve one ``models=`` entry that is not a model string (module notes).

    Args:
        obj: A :class:`~datagrunt.rationalization.providers.base.ModelProvider`, or a bare callable taking a
            ``ModelRequest``.
        persistent_cache: Whether the run keeps a persistent cache (``cache_dir`` set).

    Returns:
        The checked provider, or a :class:`CallableProvider` wrapping the bare callable, of the family
        ``"unknown"``.

    Raises:
        TypeError: If ``persistent_cache`` is not a ``bool``.
        RationalizationError: If ``obj`` is an invalid or partial provider, a bare callable while
            ``persistent_cache`` is set, or neither a provider nor a callable.
    """
    if not isinstance(persistent_cache, bool):
        raise TypeError(f"persistent_cache must be a bool, got {type(persistent_cache).__name__}")
    if _attempts_a_provider(obj):
        return check_provider(obj)
    if isinstance(obj, str):
        raise RationalizationError("as_provider takes a ModelProvider or a callable: rationalize() resolves strings")
    if not callable(obj):
        raise RationalizationError(
            f"a model must be a model string, a ModelProvider or a callable, got {type(obj).__name__}"
        )
    model_id = _bare_model_id(obj)
    if identity_problem(model_id) is not None:
        raise RationalizationError(
            "the bare callable's derived id (callable:<module>.<qualname>) is not a usable model id (too long, or not "
            "one line): wrap it in CallableProvider(model_id=..., fn=...)"
        )
    if persistent_cache:
        raise RationalizationError(
            f"the bare callable {model_id} cannot key a persistent cache (its name outlives its edits): wrap it in "
            "CallableProvider(model_id=..., fn=...) and change the model_id whenever it answers differently"
        )
    return CallableProvider(model_id, obj, family=UNKNOWN_FAMILY)
