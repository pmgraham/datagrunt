"""Tests for ``datagrunt.rationalization.providers.google_cloud``: hosts, auth, URLs, identity and families (T8a).

The facts pinned here were read on 2026-11-05 from Google's documentation (the module docstring holds the table and
the URLs): the product is Gemini Enterprise Agent Platform (formerly Vertex AI) and its API host is still
``aiplatform.googleapis.com``; ``global`` uses that host, the ``us`` and ``eu`` multi-regions use
``aiplatform.{us|eu}.rep.googleapis.com``, a region ``{location}-aiplatform.googleapis.com``; express mode is
``/v1/publishers/google/models/{model}:{method}`` with an API key and no project; API keys are documented for
``generateContent`` and ``streamGenerateContent`` only, sent in the ``x-goog-api-key`` header; everything else takes
an OAuth Bearer token.

Nothing here touches a network: every unit is a pure function or an object that only builds text.
"""

import ast
import copy
import pickle
import re
import sys
import traceback
from itertools import product
from pathlib import Path
from types import MappingProxyType

import pytest

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization.providers import base as base_module
from datagrunt.rationalization.providers import google_cloud as google_cloud_module
from datagrunt.rationalization.providers.base import (
    IDENTITY_MAX_CHARS,
    MODEL_NAME_MAX_CHARS,
    UNKNOWN_FAMILY,
    PermanentProviderError,
    _Secret,
    family_problem,
)
from datagrunt.rationalization.providers.google_cloud import (
    ANTHROPIC_VERSION,
    API_KEY_ENV,
    API_KEY_HEADER,
    API_KEY_INTERFACES,
    API_VERSION,
    EXPRESS_METHODS,
    GLOBAL_HOST,
    INTERFACES,
    MODEL_ID_PREFIX,
    MULTI_REGION_HOSTS,
    OPENAI_ENDPOINT_ID,
    PUBLISHER_FAMILIES,
    PUBLISHER_METHODS,
    TOKEN_MAX_CHARS,
    GoogleCloudAuth,
    GoogleCloudTarget,
    check_access,
    check_model_segment,
    dedicated_model_id,
    family_for_publisher,
    host_for,
    model_id_for,
    rate_key_for,
    split_publisher_model,
)

KEY = "AIza-test-KEY-hunter2"
TOKEN = "ya29.test-TOKEN-hunter2"
SECRET_VALUES = (KEY, TOKEN, "hunter2")
PROJECT = "my-project"
MODEL = "gemini-2.5-pro"
LLAMA = "llama-4-maverick-17b-128e-instruct-maas"
FAMILY = re.compile(r"[a-z0-9-]{1,32}")

# Path inputs that would leave their segment, or the host, if they were let through.
BAD_SEGMENTS = ["a/b", "..", ".", "%2e", "%2E%2E", "", " ", "a b", "a?b", "a#b", "a\\b", "a@b", "a:b", "tök", None, 5]
# A model segment may carry one '@version'; these are still not one model segment.
BAD_MODEL_SEGMENTS = [
    *(value for value in BAD_SEGMENTS if value != "a@b"),
    "@",
    "a@",
    "@b",
    "a@b@c",
    "a@@b",
    "a@.",
    "a@..",
    ".@1",
    "..@1",
    "a@b/c",
    "a@b:c",
    "a@ b",
    "a@b?c",
    "a@%2e",
    "a@tök",
    "x" * (MODEL_NAME_MAX_CHARS - 1) + "@12",
]
VERSIONED_MODELS = ["claude-opus-4-5@20251101", "mistral-small-2503@001", "jamba-1.5-large@001", "m@a.b_c-9"]
BAD_LOCATIONS = [
    "a/b",
    "..",
    ".",
    "%2e",
    "evil.example",
    "a" * 53,
    "US-CENTRAL1",
    "Global",
    "",
    " us",
    "us central1",
    "-us",
    "us-",
    "us_central1",
    "us:443",
    None,
    5,
]
BAD_PROJECTS = ["a/b", "..", ".", "%2e", "evil.example", "My-Project", "", "my", "my-project-", "0123", None, 5]


# ---------------------------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------------------------


class TokenSource:
    """A token callable that counts its calls; its repr would show a token if anything printed it."""

    def __init__(self, *values):
        self.values = list(values) or [TOKEN]
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.values[min(self.calls, len(self.values)) - 1]

    def __repr__(self):
        return f"TokenSource({TOKEN})"


class TokenText(str):
    """A ``str`` subclass: a token must be an exact ``str``."""


def traceback_text(error):
    """The error's traceback with the locals of datagrunt's frames, as a log with ``capture_locals`` prints them.

    The test's own frame is left out: it is the caller, which holds what it passed.
    """
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def assert_secret_free(error):
    """No secret in the error's traceback locals, text or repr, and no chained exception that could hold one."""
    for text in (traceback_text(error), str(error), repr(error)):
        for secret in SECRET_VALUES:
            assert secret not in text
    assert error.__cause__ is None
    assert error.__context__ is None


def target(location="us-central1", project=PROJECT):
    return GoogleCloudTarget(project, location)


def token_failure(value):
    """The PermanentProviderError a token source returning ``value`` earns on ``headers()``."""
    auth = GoogleCloudAuth(token=lambda: value)
    with pytest.raises(PermanentProviderError) as raised:
        auth.headers()
    return raised.value


# ---------------------------------------------------------------------------------------------------------------
# Hosts and locations
# ---------------------------------------------------------------------------------------------------------------


class TestHosts:
    @pytest.mark.parametrize(
        ("location", "host"),
        [
            ("global", "aiplatform.googleapis.com"),
            ("us", "aiplatform.us.rep.googleapis.com"),
            ("eu", "aiplatform.eu.rep.googleapis.com"),
            ("us-central1", "us-central1-aiplatform.googleapis.com"),
            ("europe-west4", "europe-west4-aiplatform.googleapis.com"),
            ("us-east5", "us-east5-aiplatform.googleapis.com"),
            ("me-central2", "me-central2-aiplatform.googleapis.com"),
        ],
    )
    def test_golden_host_per_location(self, location, host):
        assert host_for(location) == host
        assert target(location).host == host

    def test_the_constants_match_the_goldens(self):
        assert GLOBAL_HOST == "aiplatform.googleapis.com"
        assert dict(MULTI_REGION_HOSTS) == {
            "us": "aiplatform.us.rep.googleapis.com",
            "eu": "aiplatform.eu.rep.googleapis.com",
        }
        assert API_VERSION == "v1"

    def test_the_multi_region_table_is_immutable(self):
        assert isinstance(MULTI_REGION_HOSTS, MappingProxyType)
        with pytest.raises(TypeError):
            MULTI_REGION_HOSTS["asia"] = "evil.example"  # type: ignore[index]

    def test_the_longest_location_still_makes_one_dns_label(self):
        location = "a" * 52
        host = host_for(location)
        assert host == f"{location}-aiplatform.googleapis.com"
        assert len(host.split(".")[0]) == 63

    @pytest.mark.parametrize("location", BAD_LOCATIONS)
    def test_a_bad_location_is_refused_naming_the_field_never_the_value(self, location):
        with pytest.raises(RationalizationError, match="location") as raised:
            host_for(location)
        if isinstance(location, str) and len(location) >= 6:
            assert location not in str(raised.value)

    @pytest.mark.parametrize("location", BAD_LOCATIONS)
    def test_a_bad_location_is_refused_by_the_target(self, location):
        with pytest.raises(RationalizationError, match="location"):
            GoogleCloudTarget(PROJECT, location)


# ---------------------------------------------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------------------------------------------


class TestTarget:
    @pytest.mark.parametrize("project", ["my-project", "123456789012", "example.com:my-project", "a" * 6, "a" * 30])
    def test_a_documented_project_form_is_accepted(self, project):
        made = GoogleCloudTarget(project, "us-central1")
        assert (made.project, made.location, made.host) == (
            project,
            "us-central1",
            "us-central1-aiplatform.googleapis.com",
        )
        assert not made.is_express

    @pytest.mark.parametrize("project", BAD_PROJECTS)
    def test_a_bad_project_is_refused_naming_the_field_never_the_value(self, project):
        with pytest.raises(RationalizationError, match="project") as raised:
            GoogleCloudTarget(project, "us-central1")
        if isinstance(project, str) and len(project) >= 4:
            assert project not in str(raised.value)

    def test_the_project_is_checked_before_a_bad_location(self):
        with pytest.raises(RationalizationError, match="project"):
            GoogleCloudTarget("a/b", "evil.example")

    def test_express_mode_has_no_project_and_the_global_host(self):
        made = GoogleCloudTarget.express()
        assert (made.project, made.location, made.host) == (None, "global", "aiplatform.googleapis.com")
        assert made.is_express
        assert made == GoogleCloudTarget(None, "global")

    @pytest.mark.parametrize("location", ["us-central1", "us", "eu", "evil.example", None])
    def test_express_mode_takes_only_the_global_location(self, location):
        with pytest.raises(RationalizationError, match="express"):
            GoogleCloudTarget(None, location)

    def test_a_target_is_frozen(self):
        made = target()
        with pytest.raises(AttributeError):
            made.host = "evil.example"  # type: ignore[misc]
        with pytest.raises(AttributeError):
            made.project = "other-project"  # type: ignore[misc]
        assert not hasattr(made, "__dict__")

    def test_the_host_is_derived_never_given(self):
        with pytest.raises(TypeError):
            GoogleCloudTarget(PROJECT, "us-central1", "evil.example")  # type: ignore[call-arg]

    def test_targets_compare_by_value_and_hold_no_secret(self):
        assert target() == target()
        assert target() != target("us")
        assert pickle.loads(pickle.dumps(target())) == target()
        assert repr(target()) == (
            "GoogleCloudTarget(project='my-project', location='us-central1', "
            "host='us-central1-aiplatform.googleapis.com')"
        )


# ---------------------------------------------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------------------------------------------


class TestPublisherModelUrl:
    @pytest.mark.parametrize(
        ("location", "url"),
        [
            (
                "us-central1",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/"
                "publishers/google/models/gemini-2.5-pro:generateContent",
            ),
            (
                "global",
                "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/"
                "publishers/google/models/gemini-2.5-pro:generateContent",
            ),
            (
                "us",
                "https://aiplatform.us.rep.googleapis.com/v1/projects/my-project/locations/us/"
                "publishers/google/models/gemini-2.5-pro:generateContent",
            ),
            (
                "eu",
                "https://aiplatform.eu.rep.googleapis.com/v1/projects/my-project/locations/eu/"
                "publishers/google/models/gemini-2.5-pro:generateContent",
            ),
        ],
    )
    def test_golden_gemini_url_per_location(self, location, url):
        assert target(location).publisher_model_url("google", MODEL, "generateContent") == url

    def test_golden_claude_raw_predict_url(self):
        assert target("us-east5").publisher_model_url("anthropic", "claude-opus-4-8", "rawPredict") == (
            "https://us-east5-aiplatform.googleapis.com/v1/projects/my-project/locations/us-east5/"
            "publishers/anthropic/models/claude-opus-4-8:rawPredict"
        )

    def test_golden_domain_scoped_project_url(self):
        made = GoogleCloudTarget("example.com:my-project", "europe-west4")
        assert made.publisher_model_url("mistralai", "mistral-medium-3", "rawPredict") == (
            "https://europe-west4-aiplatform.googleapis.com/v1/projects/example.com:my-project/locations/"
            "europe-west4/publishers/mistralai/models/mistral-medium-3:rawPredict"
        )

    def test_golden_express_url(self):
        assert GoogleCloudTarget.express().publisher_model_url("google", "gemini-2.5-flash", "generateContent") == (
            "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash:generateContent"
        )

    @pytest.mark.parametrize("method", sorted(PUBLISHER_METHODS))
    def test_every_documented_method_is_accepted(self, method):
        assert target().publisher_model_url("google", MODEL, method).endswith(f"/models/{MODEL}:{method}")

    def test_the_documented_methods(self):
        assert PUBLISHER_METHODS == frozenset(
            {
                "computeTokens",
                "countTokens",
                "embedContent",
                "generateContent",
                "predict",
                "rawPredict",
                "serverStreamingPredict",
                "streamGenerateContent",
                "streamRawPredict",
            }
        )
        assert EXPRESS_METHODS == frozenset({"countTokens", "generateContent", "streamGenerateContent"})

    @pytest.mark.parametrize("method", ["delete", "get", "GenerateContent", "generateContent/../x", "", None, "a@b"])
    def test_an_undocumented_method_is_refused(self, method):
        with pytest.raises(RationalizationError, match="method"):
            target().publisher_model_url("google", MODEL, method)

    @pytest.mark.parametrize("method", ["rawPredict", "streamRawPredict", "predict"])
    def test_express_mode_refuses_a_method_it_does_not_serve(self, method):
        with pytest.raises(RationalizationError, match="express"):
            GoogleCloudTarget.express().publisher_model_url("google", MODEL, method)

    @pytest.mark.parametrize("publisher", ["anthropic", "meta", "mistralai"])
    def test_express_mode_serves_only_google_models(self, publisher):
        with pytest.raises(RationalizationError, match="express"):
            GoogleCloudTarget.express().publisher_model_url(publisher, "m", "generateContent")

    @pytest.mark.parametrize("model", BAD_MODEL_SEGMENTS)
    def test_a_model_that_is_not_one_path_segment_is_refused(self, model):
        with pytest.raises(RationalizationError, match="model") as raised:
            target().publisher_model_url("google", model, "generateContent")
        if isinstance(model, str) and len(model) > 2:
            assert model not in str(raised.value)

    @pytest.mark.parametrize("publisher", BAD_SEGMENTS)
    def test_a_publisher_that_is_not_one_path_segment_is_refused(self, publisher):
        with pytest.raises(RationalizationError, match="publisher"):
            target().publisher_model_url(publisher, MODEL, "generateContent")

    @pytest.mark.parametrize("model", ["models/gemini-2.5-pro", "publishers/google/models/gemini-2.5-pro"])
    def test_a_model_resource_name_is_refused(self, model):
        with pytest.raises(RationalizationError, match="model"):
            target().publisher_model_url("google", model, "generateContent")

    @pytest.mark.parametrize("name", ["evil.example", "x.y.z", "gemini-2.5-pro"])
    def test_a_dotted_model_stays_in_its_segment_and_never_changes_the_host(self, name):
        url = target().publisher_model_url("google", name, "generateContent")
        assert url.startswith("https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/")
        assert url.endswith(f"/publishers/google/models/{name}:generateContent")

    @pytest.mark.parametrize("name", ["evil.example:443", "evil.example/x", "evil.example@x/y", "a@b@evil.example"])
    def test_a_model_with_a_port_a_slash_or_a_second_at_sign_is_refused(self, name):
        with pytest.raises(RationalizationError, match="model"):
            target().publisher_model_url("google", name, "generateContent")

    def test_golden_versioned_claude_url(self):
        assert target("us-east5").publisher_model_url("anthropic", "claude-opus-4-5@20251101", "rawPredict") == (
            "https://us-east5-aiplatform.googleapis.com/v1/projects/my-project/locations/us-east5/"
            "publishers/anthropic/models/claude-opus-4-5@20251101:rawPredict"
        )

    @pytest.mark.parametrize("model", VERSIONED_MODELS)
    def test_a_versioned_model_keeps_the_host_and_its_segment(self, model):
        url = target("eu").publisher_model_url("mistralai", model, "rawPredict")
        assert url == (
            f"https://aiplatform.eu.rep.googleapis.com/v1/projects/my-project/locations/eu/publishers/mistralai/"
            f"models/{model}:rawPredict"
        )
        assert url.count("@") == 1

    def test_express_takes_a_versioned_gemini_model(self):
        assert GoogleCloudTarget.express().publisher_model_url("google", "gemini-1.0-pro@001", "generateContent") == (
            "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-1.0-pro@001:generateContent"
        )

    @pytest.mark.parametrize("publisher", ["anthropic@1", "@", "a@b"])
    def test_a_publisher_never_carries_an_at_sign(self, publisher):
        with pytest.raises(RationalizationError, match="publisher"):
            target().publisher_model_url(publisher, "m", "rawPredict")

    @pytest.mark.parametrize("method", ["rawPredict@1", "@rawPredict"])
    def test_a_method_never_carries_an_at_sign(self, method):
        with pytest.raises(RationalizationError, match="method"):
            target().publisher_model_url("anthropic", "m", method)


class TestOpenAIAndEndpointUrls:
    @pytest.mark.parametrize(
        ("location", "url"),
        [
            (
                "us-central1",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/endpoints/openapi",
            ),
            ("global", "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/endpoints/openapi"),
            ("us", "https://aiplatform.us.rep.googleapis.com/v1/projects/my-project/locations/us/endpoints/openapi"),
            ("eu", "https://aiplatform.eu.rep.googleapis.com/v1/projects/my-project/locations/eu/endpoints/openapi"),
        ],
    )
    def test_golden_openai_base_url(self, location, url):
        assert target(location).openai_base_url() == url
        assert OPENAI_ENDPOINT_ID == "openapi"

    @pytest.mark.parametrize("endpoint_id", ["1234567890", "my-endpoint"])
    def test_golden_dedicated_endpoint_base_url(self, endpoint_id):
        assert target("us-east5").endpoint_base_url(endpoint_id) == (
            f"https://us-east5-aiplatform.googleapis.com/v1/projects/my-project/locations/us-east5/endpoints/{endpoint_id}"
        )

    @pytest.mark.parametrize("endpoint_id", BAD_SEGMENTS)
    def test_a_bad_endpoint_id_is_refused(self, endpoint_id):
        with pytest.raises(RationalizationError, match="endpoint"):
            target().endpoint_base_url(endpoint_id)

    @pytest.mark.parametrize("endpoint_id", ["123@1", "@123", "a@b"])
    def test_an_endpoint_id_never_carries_an_at_sign(self, endpoint_id):
        with pytest.raises(RationalizationError, match="endpoint"):
            target().endpoint_base_url(endpoint_id)
        with pytest.raises(RationalizationError, match="endpoint"):
            dedicated_model_id(target(), endpoint_id)

    def test_the_shared_openapi_endpoint_is_not_a_dedicated_one(self):
        with pytest.raises(RationalizationError, match="openai_base_url"):
            target().endpoint_base_url("openapi")

    def test_express_mode_has_neither_the_openai_endpoint_nor_dedicated_endpoints(self):
        express = GoogleCloudTarget.express()
        with pytest.raises(RationalizationError, match="express"):
            express.openai_base_url()
        with pytest.raises(RationalizationError, match="express"):
            express.endpoint_base_url("1234567890")


class TestSplitPublisherModel:
    @pytest.mark.parametrize(
        ("name", "parts"),
        [
            (f"meta/{LLAMA}", ("meta", LLAMA)),
            ("google/gemini-2.5-flash", ("google", "gemini-2.5-flash")),
            ("deepseek-ai/deepseek-v3.2-maas", ("deepseek-ai", "deepseek-v3.2-maas")),
            ("qwen/qwen3-next-80b-a3b-thinking-maas", ("qwen", "qwen3-next-80b-a3b-thinking-maas")),
            ("openai/gpt-oss-120b-maas", ("openai", "gpt-oss-120b-maas")),
            ("zai-org/glm-5.2-maas", ("zai-org", "glm-5.2-maas")),
            ("a/b@c", ("a", "b@c")),
            ("anthropic/claude-opus-4-5@20251101", ("anthropic", "claude-opus-4-5@20251101")),
        ],
    )
    def test_a_publisher_model_name_splits(self, name, parts):
        assert split_publisher_model(name) == parts

    @pytest.mark.parametrize(
        "name",
        [
            "llama",
            "a/b/c",
            "/x",
            "x/",
            "../x",
            "x/..",
            "./x",
            "a/%2e",
            " a/b",
            "a/b ",
            "a//b",
            "a/b@",
            "a/@1",
            "a@1/b",
            "a/b@c@d",
            "a/b@..",
            "",
            None,
            5,
        ],
    )
    def test_a_name_that_is_not_publisher_slash_model_is_refused(self, name):
        with pytest.raises(RationalizationError, match="publisher"):
            split_publisher_model(name)


class TestCheckModelSegment:
    @pytest.mark.parametrize(
        "value",
        [
            "a",
            "gemini-2.5-pro",
            *VERSIONED_MODELS,
            "x" * MODEL_NAME_MAX_CHARS,
            "x" * (MODEL_NAME_MAX_CHARS - 3) + "@12",
            "..a@1",
            "a@..1",
            "a.@.b",
        ],
    )
    def test_a_model_segment_with_at_most_one_version_passes(self, value):
        assert check_model_segment(value, name="a model") == value

    @pytest.mark.parametrize("value", BAD_MODEL_SEGMENTS)
    def test_anything_else_is_refused_naming_the_field_never_the_value(self, value):
        with pytest.raises(RationalizationError, match="the field-name") as raised:
            check_model_segment(value, name="the field-name")
        if isinstance(value, str) and len(value) >= 4:
            assert value not in str(raised.value)

    def test_the_bound_counts_the_version(self):
        assert len("x" * (MODEL_NAME_MAX_CHARS - 3) + "@12") == MODEL_NAME_MAX_CHARS
        with pytest.raises(RationalizationError):
            check_model_segment("x" * (MODEL_NAME_MAX_CHARS - 2) + "@12", name="a model")


# ---------------------------------------------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------------------------------------------


class TestApiKeyAuth:
    def test_the_key_goes_in_the_x_goog_api_key_header(self):
        auth = GoogleCloudAuth(api_key=KEY)
        headers = auth.headers()
        assert auth.kind == "api_key"
        assert list(headers) == ["x-goog-api-key"] == [API_KEY_HEADER]
        assert isinstance(headers["x-goog-api-key"], _Secret)
        assert headers["x-goog-api-key"].reveal() == KEY

    def test_a_secret_is_taken_as_it_is(self):
        secret = _Secret(KEY)
        assert GoogleCloudAuth(api_key=secret).headers()["x-goog-api-key"] is secret

    def test_each_call_returns_a_new_mapping(self):
        auth = GoogleCloudAuth(api_key=KEY)
        first = auth.headers()
        first["x-goog-api-key"] = "evil"
        first["extra"] = "x"
        assert list(auth.headers()) == ["x-goog-api-key"]
        assert auth.headers()["x-goog-api-key"].reveal() == KEY

    def test_the_key_never_reaches_a_url(self):
        auth = GoogleCloudAuth(api_key=KEY)
        made = target()
        urls = [
            made.publisher_model_url("google", MODEL, "generateContent"),
            made.openai_base_url(),
            made.endpoint_base_url("123"),
            GoogleCloudTarget.express().publisher_model_url("google", MODEL, "generateContent"),
        ]
        check_access(made, auth, interface="generateContent")
        for url in urls:
            assert KEY not in url
            assert "key=" not in url
            assert "?" not in url

    @pytest.mark.parametrize(
        "value",
        ["", "   ", f"{KEY}\n", f" {KEY}", "AIza-tök-hunter2", "AIza\x00hunter2", 5, b"AIza-hunter2", TokenText(KEY)],
    )
    def test_a_bad_key_is_refused_and_never_shown(self, value):
        with pytest.raises(RationalizationError, match="API key") as raised:
            GoogleCloudAuth(api_key=value)
        assert_secret_free(raised.value)

    def test_a_key_given_with_a_token_never_reaches_a_traceback_local(self):
        with pytest.raises(RationalizationError) as raised:
            GoogleCloudAuth(api_key=KEY, token=TokenSource())
        assert "exactly one" in str(raised.value)
        assert_secret_free(raised.value)

    def test_the_key_is_built_at_construction(self):
        with pytest.raises(RationalizationError, match="API key"):
            GoogleCloudAuth(api_key="")


class TestTokenAuth:
    def test_the_token_is_a_bearer_header(self):
        auth = GoogleCloudAuth(token=TokenSource())
        headers = auth.headers()
        assert auth.kind == "token"
        assert list(headers) == ["authorization"]
        assert isinstance(headers["authorization"], _Secret)
        assert headers["authorization"].reveal() == f"Bearer {TOKEN}"

    def test_the_token_is_called_once_per_headers_call_never_at_construction(self):
        source = TokenSource("ya29.first-hunter2", "ya29.second-hunter2")
        auth = GoogleCloudAuth(token=source)
        assert source.calls == 0
        assert auth.kind == "token"
        repr(auth)
        assert source.calls == 0
        assert auth.headers()["authorization"].reveal() == "Bearer ya29.first-hunter2"
        assert source.calls == 1
        assert auth.headers()["authorization"].reveal() == "Bearer ya29.second-hunter2"
        assert source.calls == 2

    @pytest.mark.parametrize("value", ["a", "ya29." + "x" * (TOKEN_MAX_CHARS - 5), "tok!#$%&'*+-.^_`|~"])
    def test_a_token_of_printable_ascii_without_whitespace_is_sent(self, value):
        assert GoogleCloudAuth(token=lambda: value).headers()["authorization"].reveal() == f"Bearer {value}"

    @pytest.mark.parametrize(
        "error", [RuntimeError(f"refresh failed for {TOKEN}"), ValueError(TOKEN), OSError(), LookupError("hunter2")]
    )
    def test_a_raising_token_provider_is_permanent_without_its_message(self, error):
        def token():
            raise error

        with pytest.raises(PermanentProviderError) as raised:
            GoogleCloudAuth(token=token).headers()

        assert raised.value.status_code is None
        assert raised.value.detail == f"the Google Cloud token provider failed ({type(error).__name__})"
        assert_secret_free(raised.value)

    @pytest.mark.parametrize(
        ("value", "named"),
        [(None, "NoneType"), (5, "int"), (TOKEN.encode(), "bytes"), (TokenText(TOKEN), "TokenText"), ([TOKEN], "list")],
    )
    def test_a_token_that_is_not_an_exact_str_is_permanent(self, value, named):
        error = token_failure(value)
        assert error.detail.startswith("the Google Cloud token provider failed (")
        assert named in error.detail
        assert_secret_free(error)

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "   ",
            "ya29 hunter2",
            "ya29\thunter2",
            f"{TOKEN}\n",
            f" {TOKEN}",
            "ya29.tök-hunter2",
            "ya29\x00hunter2",
            "ya29\x7fhunter2",
            "hunter2" * (TOKEN_MAX_CHARS // 7 + 1),
        ],
    )
    def test_an_invalid_token_is_permanent_and_never_shown(self, value):
        error = token_failure(value)
        assert error.detail.startswith("the Google Cloud token provider failed (")
        assert_secret_free(error)

    @pytest.mark.parametrize("value", [f"Bearer {TOKEN}", f"bearer {TOKEN}", f"BEARER {TOKEN}"])
    def test_a_token_carrying_its_scheme_is_refused(self, value):
        error = token_failure(value)
        assert "Bearer" in error.detail
        assert_secret_free(error)

    def test_the_longest_token_is_bounded(self):
        assert TOKEN_MAX_CHARS == 16_384
        assert "more than" in token_failure("x" * (TOKEN_MAX_CHARS + 1)).detail

    def test_a_keyboard_interrupt_in_the_token_provider_propagates(self):
        def token():
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            GoogleCloudAuth(token=token).headers()

    @pytest.mark.parametrize("token", [TOKEN, 5, _Secret(TOKEN), b"ya29"])
    def test_a_token_that_is_not_callable_is_refused_at_construction(self, token):
        with pytest.raises(RationalizationError, match="token must be a callable") as raised:
            GoogleCloudAuth(token=token)
        assert_secret_free(raised.value)


class TestAuthRules:
    def test_neither_key_nor_token_is_refused(self):
        with pytest.raises(RationalizationError, match="api_key=") as raised:
            GoogleCloudAuth()
        assert "token=" in str(raised.value)

    def test_both_key_and_token_are_refused(self):
        with pytest.raises(RationalizationError, match="exactly one") as raised:
            GoogleCloudAuth(api_key=KEY, token=TokenSource())
        assert_secret_free(raised.value)

    def test_arguments_are_keyword_only(self):
        with pytest.raises(TypeError):
            GoogleCloudAuth(KEY)  # type: ignore[misc]

    @pytest.mark.parametrize(
        "make", [lambda: GoogleCloudAuth(api_key=KEY), lambda: GoogleCloudAuth(token=TokenSource())]
    )
    def test_repr_and_str_never_show_a_secret(self, make):
        auth = make()
        for text in (repr(auth), str(auth), f"{auth}", f"{auth!r}"):
            for secret in SECRET_VALUES:
                assert secret not in text
        assert repr(auth) == f"GoogleCloudAuth(kind={auth.kind!r})"

    @pytest.mark.parametrize(
        "make", [lambda: GoogleCloudAuth(api_key=KEY), lambda: GoogleCloudAuth(token=TokenSource())]
    )
    def test_auth_never_pickles_or_copies(self, make):
        auth = make()
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            with pytest.raises(TypeError):
                pickle.dumps(auth, protocol=protocol)
        with pytest.raises(TypeError):
            copy.copy(auth)
        with pytest.raises(TypeError):
            copy.deepcopy(auth)
        with pytest.raises(TypeError):
            auth.__getstate__()

    def test_auth_is_frozen_and_slotted(self):
        auth = GoogleCloudAuth(api_key=KEY)
        with pytest.raises(AttributeError):
            auth._credential = _Secret("other-key")  # type: ignore[misc]
        with pytest.raises(AttributeError):
            del auth._credential
        with pytest.raises(AttributeError):
            auth.extra = 1  # type: ignore[attr-defined]
        assert not hasattr(auth, "__dict__")
        assert auth.headers()["x-goog-api-key"].reveal() == KEY

    def test_a_half_built_auth_still_has_a_safe_repr(self):
        half = GoogleCloudAuth.__new__(GoogleCloudAuth)
        assert repr(half) == "GoogleCloudAuth(kind='?')"


class TestAuthFromEnv:
    def test_the_documented_variable_is_the_default(self, monkeypatch):
        assert API_KEY_ENV == "GOOGLE_API_KEY"
        monkeypatch.setenv("GOOGLE_API_KEY", KEY)
        auth = GoogleCloudAuth.from_env()
        assert auth.kind == "api_key"
        assert auth.headers()["x-goog-api-key"].reveal() == KEY

    def test_another_variable_can_be_named(self, monkeypatch):
        monkeypatch.setenv("DATAGRUNT_TEST_GCP_KEY", KEY)
        assert GoogleCloudAuth.from_env("DATAGRUNT_TEST_GCP_KEY").headers()["x-goog-api-key"].reveal() == KEY

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_an_unset_or_blank_variable_is_refused_naming_it(self, monkeypatch, value):
        if value is None:
            monkeypatch.delenv("DATAGRUNT_TEST_GCP_KEY", raising=False)
        else:
            monkeypatch.setenv("DATAGRUNT_TEST_GCP_KEY", value)
        with pytest.raises(RationalizationError, match="DATAGRUNT_TEST_GCP_KEY") as raised:
            GoogleCloudAuth.from_env("DATAGRUNT_TEST_GCP_KEY")
        assert "token=" in str(raised.value)

    @pytest.mark.parametrize("value", [f"{KEY}\n", f" {KEY}", "AIza-tök-hunter2"])
    def test_a_bad_value_is_refused_and_never_reaches_a_traceback_local(self, monkeypatch, value):
        monkeypatch.setenv("DATAGRUNT_TEST_GCP_KEY", value)
        with pytest.raises(RationalizationError, match="DATAGRUNT_TEST_GCP_KEY") as raised:
            GoogleCloudAuth.from_env("DATAGRUNT_TEST_GCP_KEY")
        assert_secret_free(raised.value)


# ---------------------------------------------------------------------------------------------------------------
# Access rules: which auth each interface takes
# ---------------------------------------------------------------------------------------------------------------


class TestCheckAccess:
    def test_the_interfaces(self):
        assert INTERFACES == frozenset(
            {"generateContent", "streamGenerateContent", "rawPredict", "streamRawPredict", "chatCompletions"}
        )
        assert API_KEY_INTERFACES == frozenset({"generateContent", "streamGenerateContent"})

    @pytest.mark.parametrize("interface", sorted(INTERFACES))
    @pytest.mark.parametrize("location", ["us-central1", "global", "us", "eu"])
    def test_a_token_reaches_every_interface(self, interface, location):
        assert check_access(target(location), GoogleCloudAuth(token=TokenSource()), interface=interface) is None

    @pytest.mark.parametrize("interface", sorted(API_KEY_INTERFACES))
    def test_an_api_key_reaches_generate_content(self, interface):
        assert check_access(target(), GoogleCloudAuth(api_key=KEY), interface=interface) is None

    @pytest.mark.parametrize("interface", sorted(INTERFACES - API_KEY_INTERFACES))
    def test_an_api_key_is_refused_where_google_documents_none(self, interface):
        with pytest.raises(RationalizationError, match="token=") as raised:
            check_access(target(), GoogleCloudAuth(api_key=KEY), interface=interface)
        assert interface in str(raised.value)
        assert_secret_free(raised.value)

    @pytest.mark.parametrize("interface", sorted(API_KEY_INTERFACES))
    def test_express_mode_takes_an_api_key_for_generate_content(self, interface):
        assert check_access(GoogleCloudTarget.express(), GoogleCloudAuth(api_key=KEY), interface=interface) is None

    def test_express_mode_refuses_a_token(self):
        with pytest.raises(RationalizationError, match="express"):
            check_access(GoogleCloudTarget.express(), GoogleCloudAuth(token=TokenSource()), interface="generateContent")

    @pytest.mark.parametrize("interface", sorted(INTERFACES - API_KEY_INTERFACES))
    def test_express_mode_refuses_other_interfaces(self, interface):
        with pytest.raises(RationalizationError, match="express"):
            check_access(GoogleCloudTarget.express(), GoogleCloudAuth(api_key=KEY), interface=interface)

    @pytest.mark.parametrize("given", [None, "my-project", {"project": PROJECT, "location": "global"}])
    def test_a_target_that_is_not_a_target_is_refused(self, given):
        with pytest.raises(RationalizationError, match="GoogleCloudTarget"):
            check_access(given, GoogleCloudAuth(api_key=KEY), interface="generateContent")

    @pytest.mark.parametrize("given", [None, KEY, _Secret(KEY), TokenSource()])
    def test_an_auth_that_is_not_an_auth_is_refused(self, given):
        with pytest.raises(RationalizationError, match="GoogleCloudAuth") as raised:
            check_access(target(), given, interface="generateContent")
        assert_secret_free(raised.value)

    @pytest.mark.parametrize("interface", ["predict", "chat/completions", "GENERATECONTENT", "", None])
    def test_an_unknown_interface_is_a_programming_error(self, interface):
        with pytest.raises(ValueError, match="interface"):
            check_access(target(), GoogleCloudAuth(token=TokenSource()), interface=interface)


# ---------------------------------------------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------------------------------------------


class TestModelId:
    @pytest.mark.parametrize(
        ("made", "publisher", "model", "expected"),
        [
            (
                target(),
                "google",
                MODEL,
                "google-cloud:projects/my-project/locations/us-central1/publishers/google/models/gemini-2.5-pro",
            ),
            (
                target("global"),
                "anthropic",
                "claude-opus-4-8",
                "google-cloud:projects/my-project/locations/global/publishers/anthropic/models/claude-opus-4-8",
            ),
            (
                target("eu"),
                "meta",
                LLAMA,
                f"google-cloud:projects/my-project/locations/eu/publishers/meta/models/{LLAMA}",
            ),
            (
                GoogleCloudTarget.express(),
                "google",
                "gemini-2.5-flash",
                "google-cloud:publishers/google/models/gemini-2.5-flash",
            ),
        ],
    )
    def test_golden_model_id(self, made, publisher, model, expected):
        assert model_id_for(made, publisher, model) == expected
        assert MODEL_ID_PREFIX == "google-cloud:"

    @pytest.mark.parametrize("location", ["us-central1", "global", "us", "eu"])
    @pytest.mark.parametrize(("publisher", "method"), [("google", "generateContent"), ("anthropic", "rawPredict")])
    def test_the_model_id_is_the_resource_name_the_url_calls(self, location, publisher, method):
        made = target(location)
        url = made.publisher_model_url(publisher, "m-1", method)
        path = url.removeprefix(f"https://{made.host}/{API_VERSION}/").removesuffix(f":{method}")
        assert model_id_for(made, publisher, "m-1") == MODEL_ID_PREFIX + path

    def test_golden_versioned_model_id(self):
        assert model_id_for(target("us-east5"), "anthropic", "claude-opus-4-5@20251101") == (
            "google-cloud:projects/my-project/locations/us-east5/publishers/anthropic/models/claude-opus-4-5@20251101"
        )

    def test_golden_dedicated_endpoint_model_id(self):
        assert dedicated_model_id(target("us-east5"), "1234567890") == (
            "google-cloud:projects/my-project/locations/us-east5/endpoints/1234567890"
        )

    def test_model_ids_differ_across_project_location_publisher_and_model(self):
        projects = ["my-project", "other-project", "123456789012", "example.com:my-project", "express"]
        locations = ["us-central1", "global", "us", "eu", "europe-west4"]
        publishers = ["google", "anthropic", "meta"]
        models = ["m-1", "m-2", "m-1@1", "m-1@2"]
        ids = [
            model_id_for(GoogleCloudTarget(p, loc), pub, m)
            for p, loc, pub, m in product(projects, locations, publishers, models)
        ]
        ids += [model_id_for(GoogleCloudTarget.express(), "google", m) for m in models]
        endpoints = ["m-1", "m-2", "123"]
        ids += [
            dedicated_model_id(GoogleCloudTarget(p, loc), e)
            for p, loc in product(projects, locations)
            for e in endpoints
        ]
        assert len(ids) == len(set(ids))

    def test_a_publisher_named_endpoints_never_collides_with_a_dedicated_endpoint(self):
        assert model_id_for(target(), "endpoints", "1") != dedicated_model_id(target(), "1")

    def test_an_identity_past_the_bound_is_refused(self):
        domain = ".".join(["a" * 60] * 4)  # 243 characters
        made = GoogleCloudTarget(f"{domain}:my-project", "us-central1")
        with pytest.raises(RationalizationError, match="model id"):
            model_id_for(made, "google", "m" * 256)
        assert len(model_id_for(made, "google", "m")) <= IDENTITY_MAX_CHARS

    @pytest.mark.parametrize("given", [None, "my-project", object()])
    def test_a_target_that_is_not_a_target_is_refused(self, given):
        with pytest.raises(RationalizationError, match="GoogleCloudTarget"):
            model_id_for(given, "google", MODEL)
        with pytest.raises(RationalizationError, match="GoogleCloudTarget"):
            dedicated_model_id(given, "1")
        with pytest.raises(RationalizationError, match="GoogleCloudTarget"):
            rate_key_for(given)

    @pytest.mark.parametrize("model", BAD_MODEL_SEGMENTS)
    def test_a_bad_model_is_refused(self, model):
        with pytest.raises(RationalizationError, match="model"):
            model_id_for(target(), "google", model)

    def test_express_ids_serve_only_google(self):
        with pytest.raises(RationalizationError, match="express"):
            model_id_for(GoogleCloudTarget.express(), "anthropic", "claude-opus-4-8")
        with pytest.raises(RationalizationError, match="express"):
            dedicated_model_id(GoogleCloudTarget.express(), "1")


class TestRateKey:
    @pytest.mark.parametrize(
        ("made", "expected"),
        [
            (target(), "us-central1-aiplatform.googleapis.com/projects/my-project"),
            (target("global"), "aiplatform.googleapis.com/projects/my-project"),
            (target("us"), "aiplatform.us.rep.googleapis.com/projects/my-project"),
            (target("eu"), "aiplatform.eu.rep.googleapis.com/projects/my-project"),
            (
                GoogleCloudTarget("example.com:my-project", "eu"),
                "aiplatform.eu.rep.googleapis.com/projects/example.com:my-project",
            ),
            (GoogleCloudTarget.express(), "aiplatform.googleapis.com/express"),
        ],
    )
    def test_golden_rate_key(self, made, expected):
        assert rate_key_for(made) == expected

    def test_rate_keys_differ_across_project_and_location(self):
        projects = ["my-project", "other-project", "express"]
        locations = ["us-central1", "global", "us", "eu", "europe-west4"]
        keys = [rate_key_for(GoogleCloudTarget(p, loc)) for p, loc in product(projects, locations)]
        keys.append(rate_key_for(GoogleCloudTarget.express()))
        assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------------------------------------------
# Families
# ---------------------------------------------------------------------------------------------------------------


class TestFamilies:
    @pytest.mark.parametrize(
        ("publisher", "family"),
        [
            ("google", "google"),
            ("anthropic", "anthropic"),
            ("meta", "meta"),
            ("mistralai", "mistral"),
            ("ai21", "ai21"),
            ("xai", "xai"),
            ("deepseek-ai", "deepseek"),
            ("qwen", "qwen"),
            ("openai", "openai"),
            ("moonshotai", "moonshot"),
            ("moonshot-ai", "moonshot"),
            ("minimaxai", "minimax"),
            ("zai-org", "zai"),
        ],
    )
    def test_golden_family_per_publisher(self, publisher, family):
        assert family_for_publisher(publisher) == family
        assert PUBLISHER_FAMILIES[publisher] == family

    def test_the_table_is_exactly_the_documented_publishers(self):
        assert set(PUBLISHER_FAMILIES) == {
            "google",
            "anthropic",
            "meta",
            "mistralai",
            "ai21",
            "xai",
            "deepseek-ai",
            "qwen",
            "openai",
            "moonshotai",
            "moonshot-ai",
            "minimaxai",
            "zai-org",
        }

    @pytest.mark.parametrize("publisher", ["acme", "Google", "intfloat", "hf-community", "meta-llama"])
    def test_an_unmapped_publisher_is_unknown(self, publisher):
        assert family_for_publisher(publisher) == "unknown" == UNKNOWN_FAMILY

    def test_every_family_is_a_valid_family_name(self):
        assert all(FAMILY.fullmatch(family) for family in [*PUBLISHER_FAMILIES.values(), "unknown"])
        assert all(family_problem(family) is None for family in PUBLISHER_FAMILIES.values())
        assert UNKNOWN_FAMILY not in PUBLISHER_FAMILIES.values()

    def test_unknown_is_the_shared_constant(self):
        assert google_cloud_module.UNKNOWN_FAMILY is base_module.UNKNOWN_FAMILY
        assert not hasattr(google_cloud_module, "_UNKNOWN_FAMILY")

    def test_the_table_is_immutable(self):
        assert isinstance(PUBLISHER_FAMILIES, MappingProxyType)
        with pytest.raises(TypeError):
            PUBLISHER_FAMILIES["acme"] = "google"  # type: ignore[index]

    @pytest.mark.parametrize("publisher", BAD_SEGMENTS)
    def test_a_bad_publisher_is_refused(self, publisher):
        with pytest.raises(RationalizationError, match="publisher"):
            family_for_publisher(publisher)


# ---------------------------------------------------------------------------------------------------------------
# Constants and module hygiene
# ---------------------------------------------------------------------------------------------------------------


def test_the_anthropic_version_google_requires():
    assert ANTHROPIC_VERSION == "vertex-2023-10-16"


def test_the_module_imports_only_the_stdlib_base_and_exceptions_and_nothing_inside_functions():
    tree = ast.parse(Path(google_cloud_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    allowed = {"datagrunt.rationalization.providers.base", "datagrunt.rationalization.exceptions"}
    foreign = [m for m in modules if m not in allowed and m.split(".")[0] not in sys.stdlib_module_names]

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert foreign == []
    assert [m for m in modules if m.startswith("datagrunt.core") or m.startswith("httpx")] == []


def test_the_module_docstring_records_the_research_date_and_sources():
    doc = google_cloud_module.__doc__
    assert "2026-11-05" in doc
    for url in (
        "https://docs.cloud.google.com/gemini-enterprise-agent-platform/resources/locations",
        "https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/express-mode/api-reference",
        "https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/partner-models/claude/structured-outputs",
    ):
        assert url in doc
