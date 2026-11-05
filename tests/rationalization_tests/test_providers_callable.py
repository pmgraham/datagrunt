"""Tests for ``datagrunt.rationalization.providers.callable``: a caller's own function as a model provider (plan §4.5).

``CallableProvider(model_id, fn)`` is a ``ModelProvider`` of kind ``"callable"`` whose ``generate`` calls
``fn(request)``: a ``ProviderResponse`` is returned as is, a JSON-object mapping is wrapped as a complete response, and
anything else is a loud ``TypeError``. Provider errors and every other exception from ``fn`` propagate unchanged.
``as_provider`` checks a provider object, wraps a bare callable under the id ``callable:<module>.<qualname>``, and
refuses a bare callable when a persistent cache would key on that unstable id.
"""

import ast
import functools
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization._canonical import canonical_json
from datagrunt.rationalization.providers import callable as callable_module
from datagrunt.rationalization.providers.base import (
    IDENTITY_MAX_CHARS,
    ModelProvider,
    PermanentProviderError,
    TransientProviderError,
    check_provider,
)
from datagrunt.rationalization.providers.callable import CallableProvider, as_provider
from datagrunt.rationalization.wire import ModelRequest, ProviderResponse

REQUEST = ModelRequest(
    unit_id="doc:p0001_e000-p0001_e002",
    model_id="callable:mine",
    system_prompt="rules",
    user_envelope="payload",
    json_schema={"type": "object", "properties": {"records": {"type": "array"}}},
    max_output_tokens=64,
    timeout_s=5.0,
)
SCHEMA = {"type": "object", "properties": {"records": {"type": "array", "items": {"type": "object"}}}}
RESPONSE = ProviderResponse(content={"records": []}, raw_text="{}", finish="truncated", input_tokens=3, output_tokens=4)


def answer(request):
    """A module-level bare callable."""
    return {
        "records": [{"record": {"part": "PN-1"}, "grounding_quotes": ["PN-1"], "source_element_ids": ["p0001_e000"]}]
    }


class Answerer:
    """A callable instance."""

    def __call__(self, request):
        return RESPONSE


# ---------------------------------------------------------------------------------------------------------------
# CallableProvider: its identity
# ---------------------------------------------------------------------------------------------------------------


def test_a_callable_provider_names_its_kind_rate_key_and_params():
    provider = CallableProvider("mine", answer)

    assert provider.model_id == "mine"
    assert provider.provider_kind == "callable"
    assert provider.rate_key == "callable:mine"
    assert provider.generation_params == {}


def test_its_generation_params_are_a_fresh_mapping_each_time():
    provider = CallableProvider("mine", answer)
    params = provider.generation_params
    params["temperature"] = 1.0  # type: ignore[index]

    assert provider.generation_params == {}


def test_it_is_a_model_provider_and_passes_the_preflight_check():
    provider = CallableProvider("mine", answer)

    assert isinstance(provider, ModelProvider)
    assert check_provider(provider) is provider


def test_its_repr_names_the_model_id_and_never_the_function():
    provider = CallableProvider("mine", lambda request: {"secret": "document text"})

    assert "mine" in repr(provider)
    assert "lambda" not in repr(provider)


@pytest.mark.parametrize("model_id", [1, None, b"mine"])
def test_a_model_id_that_is_not_a_str_is_a_type_error(model_id):
    with pytest.raises(TypeError, match="model_id"):
        CallableProvider(model_id, answer)  # type: ignore[arg-type]


@pytest.mark.parametrize("model_id", ["", "  ", "a\nb", "a\x00b", "a b", "x" * (IDENTITY_MAX_CHARS + 1)])
def test_a_blank_multiline_or_long_model_id_is_a_value_error(model_id):
    with pytest.raises(ValueError, match="model_id"):
        CallableProvider(model_id, answer)


def test_a_model_id_of_the_limit_is_accepted():
    assert CallableProvider("x" * IDENTITY_MAX_CHARS, answer).model_id == "x" * IDENTITY_MAX_CHARS


@pytest.mark.parametrize("fn", [None, "answer", 1])
def test_a_function_that_is_not_callable_is_a_type_error(fn):
    with pytest.raises(TypeError, match="fn"):
        CallableProvider("mine", fn)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# transform_schema
# ---------------------------------------------------------------------------------------------------------------


def test_transform_schema_is_the_identity_as_a_fresh_deep_copy():
    provider = CallableProvider("mine", answer)

    transformed = provider.transform_schema(SCHEMA)

    assert transformed == SCHEMA
    assert transformed is not SCHEMA
    transformed["properties"]["records"]["items"]["type"] = "string"  # type: ignore[index]
    assert SCHEMA["properties"]["records"]["items"]["type"] == "object"


def test_transform_schema_refuses_a_schema_that_is_not_json():
    with pytest.raises(RationalizationError):
        CallableProvider("mine", answer).transform_schema({"type": {"object"}})


# ---------------------------------------------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------------------------------------------


def test_generate_passes_the_request_and_returns_a_provider_response_as_is():
    seen = []

    def fn(request):
        seen.append(request)
        return RESPONSE

    assert CallableProvider("mine", fn).generate(REQUEST) is RESPONSE
    assert seen == [REQUEST]
    assert seen[0] is REQUEST


def test_generate_wraps_a_json_object_as_a_complete_response():
    response = CallableProvider("mine", answer).generate(REQUEST)

    assert response.content == answer(REQUEST)
    assert response.raw_text == canonical_json(answer(REQUEST))
    assert response.finish == "complete"
    assert response.input_tokens is None
    assert response.output_tokens is None


def test_the_wrapped_content_is_its_own_copy():
    returned = {"records": []}
    response = CallableProvider("mine", lambda request: returned).generate(REQUEST)
    returned["records"].append("late")

    assert response.content == {"records": []}


@pytest.mark.parametrize(
    "value",
    [{1: "x"}, {"a": float("nan")}, {"a": {1, 2}}, {"a": (1, 2)}, {"a": "\ud800"}, {"a": object()}],
    ids=["int-key", "nan", "set", "tuple", "surrogate", "object"],
)
def test_a_mapping_that_is_not_exactly_json_is_a_type_error_naming_the_provider(value):
    with pytest.raises(TypeError, match="mine"):
        CallableProvider("mine", lambda request: value).generate(REQUEST)


@pytest.mark.parametrize("value", [None, [1], "text", 1, b"{}", RESPONSE.to_json()["finish"]])
def test_anything_else_is_a_type_error_naming_the_provider(value):
    with pytest.raises(TypeError, match="mine"):
        CallableProvider("mine", lambda request: value).generate(REQUEST)


def test_a_mapping_too_large_for_a_response_is_refused_naming_the_provider():
    huge = {"text": "a" * (8 * 1024 * 1024)}

    with pytest.raises(ValueError, match="mine"):
        CallableProvider("mine", lambda request: huge).generate(REQUEST)


def test_generate_refuses_something_that_is_not_a_model_request():
    with pytest.raises(TypeError, match="ModelRequest"):
        CallableProvider("mine", answer).generate({"unit_id": "x"})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "error",
    [TransientProviderError(429, 2.0, "slow"), PermanentProviderError(400, "bad"), ValueError("bug"), KeyError("k")],
    ids=["transient", "permanent", "value-error", "key-error"],
)
def test_an_exception_from_the_function_propagates_unchanged(error):
    def fn(request):
        raise error

    with pytest.raises(type(error)) as caught:
        CallableProvider("mine", fn).generate(REQUEST)

    assert caught.value is error


# ---------------------------------------------------------------------------------------------------------------
# as_provider
# ---------------------------------------------------------------------------------------------------------------


class ObjectProvider:
    model_id = "custom:m"
    provider_kind = "custom"
    rate_key = "custom"
    generation_params = {"temperature": 0.0}

    def transform_schema(self, schema):
        return schema

    def generate(self, request):
        return RESPONSE


@pytest.mark.parametrize("persistent_cache", [False, True])
def test_a_provider_object_is_checked_and_returned(persistent_cache):
    provider = ObjectProvider()

    assert as_provider(provider, persistent_cache=persistent_cache) is provider


@pytest.mark.parametrize("persistent_cache", [False, True])
def test_a_callable_provider_is_returned_as_is(persistent_cache):
    provider = CallableProvider("mine", answer)

    assert as_provider(provider, persistent_cache=persistent_cache) is provider


def test_an_invalid_provider_object_is_refused():
    provider = ObjectProvider()
    provider.model_id = "  "

    with pytest.raises(RationalizationError, match="model_id"):
        as_provider(provider, persistent_cache=False)


def test_a_partial_provider_is_refused_never_wrapped_as_a_callable():
    class Partial:
        model_id = "custom:m"

        def __call__(self, request):
            return RESPONSE

    with pytest.raises(RationalizationError, match="generate"):
        as_provider(Partial(), persistent_cache=False)


def test_a_bare_function_is_wrapped_under_its_module_and_qualname():
    provider = as_provider(answer, persistent_cache=False)

    assert isinstance(provider, CallableProvider)
    assert provider.model_id == f"callable:{answer.__module__}.answer"
    assert provider.rate_key == f"callable:callable:{answer.__module__}.answer"
    assert provider.generate(REQUEST).content == answer(REQUEST)


def test_a_nested_function_keeps_its_qualname():
    def inner(request):
        return RESPONSE

    provider = as_provider(inner, persistent_cache=False)

    assert provider.model_id == f"callable:{__name__}.{inner.__qualname__}"
    assert "<locals>" in provider.model_id


def test_a_callable_instance_is_named_by_its_class():
    provider = as_provider(Answerer(), persistent_cache=False)

    assert provider.model_id == f"callable:{__name__}.Answerer"
    assert provider.generate(REQUEST) is RESPONSE


def test_a_partial_is_named_by_its_class():
    provider = as_provider(functools.partial(answer), persistent_cache=False)

    assert provider.model_id == "callable:functools.partial"


def _renamed(qualname):
    """A function whose qualname is ``qualname``."""

    def fn(request):
        return RESPONSE

    renamed = types.FunctionType(fn.__code__, {}, "fn")
    renamed.__module__ = __name__
    renamed.__qualname__ = qualname
    return renamed


@pytest.mark.parametrize("qualname", ["a" * 600, "a\nb", "a\u2028b"])
def test_a_bare_callable_whose_derived_id_is_no_identity_is_refused_with_the_remedy(qualname):
    with pytest.raises(RationalizationError, match=r"CallableProvider\(model_id="):
        as_provider(_renamed(qualname), persistent_cache=False)


@pytest.mark.parametrize("obj", [mock.Mock(), mock.MagicMock()], ids=["mock", "magicmock"])
def test_a_mock_is_refused_as_a_provider_without_real_attributes(obj):
    with pytest.raises(RationalizationError, match="not real attributes"):
        as_provider(obj, persistent_cache=False)


def test_a_mock_specced_as_a_function_is_a_bare_callable():
    provider = as_provider(mock.MagicMock(spec=lambda request: None, return_value={}), persistent_cache=False)

    assert provider.model_id == "callable:unittest.mock.MagicMock"


def test_a_bare_callable_with_a_persistent_cache_is_refused_with_the_remedy():
    with pytest.raises(RationalizationError, match=r"CallableProvider\(model_id="):
        as_provider(answer, persistent_cache=True)


@pytest.mark.parametrize("obj", ["openai:gpt-4o", 1, None, ["openai:gpt-4o"]])
def test_anything_else_is_refused(obj):
    with pytest.raises(RationalizationError, match="ModelProvider"):
        as_provider(obj, persistent_cache=False)


def test_a_model_string_is_refused_as_not_resolved_here():
    with pytest.raises(RationalizationError, match=r"rationalize\(\) resolves"):
        as_provider("openai:gpt-4o", persistent_cache=False)


@pytest.mark.parametrize("persistent_cache", [1, None, "yes"])
def test_persistent_cache_must_be_a_bool(persistent_cache):
    with pytest.raises(TypeError, match="persistent_cache"):
        as_provider(answer, persistent_cache=persistent_cache)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_the_package_and_nothing_inside_functions():
    tree = ast.parse(Path(callable_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    foreign = [
        m
        for m in modules
        if not m.startswith("datagrunt.rationalization.") and m.split(".")[0] not in sys.stdlib_module_names
    ]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert foreign == []
    assert [m for m in modules if m.startswith("datagrunt.rationalization.providers.http")] == []
