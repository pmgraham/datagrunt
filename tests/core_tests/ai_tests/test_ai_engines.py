"""This module contains tests for the AI engine classes."""

from unittest.mock import Mock, patch

import pytest
from google.genai import types

from datagrunt.core.ai.engines import AIEngineProperties, BaseAIEngine, GoogleAIEngine


class TestAIEngineProperties:
    """Test suite for AIEngineProperties class."""

    def test_default_valid_engines(self):
        """Test that default valid engines tuple contains expected values."""
        properties = AIEngineProperties()
        assert properties.valid_engines == ("google",)

    def test_valid_engines_is_tuple(self):
        """Test that valid_engines is a tuple."""
        properties = AIEngineProperties()
        assert isinstance(properties.valid_engines, tuple)

    def test_custom_valid_engines(self):
        """Test that custom valid engines can be set."""
        properties = AIEngineProperties(valid_engines=("google", "openai"))
        assert properties.valid_engines == ("google", "openai")


class TestBaseAIEngine:
    """Test suite for BaseAIEngine class."""

    def test_abstract_methods_require_implementation(self):
        """Test that abstract classes cannot be instantiated without implementing all methods."""

        class PartialEngine(BaseAIEngine):
            def __init__(self, api_key=None):
                super().__init__(api_key)

            # Only implement one method
            def generate_embeddings(self, **kwargs):
                return "embeddings"

    def test_complete_implementation_works(self):
        """Test that a complete implementation of abstract class works."""

        class CompleteEngine(BaseAIEngine):
            def __init__(self, api_key=None):
                super().__init__(api_key)

            def generate_content(self, model, prompt, system_instruction=None, **kwargs):
                return "generated content"

            def generate_embeddings(self, **kwargs):
                return "embeddings"

        engine = CompleteEngine("test_key")
        assert engine.generate_content("model", "prompt") == "generated content"
        assert engine.generate_embeddings() == "embeddings"


class TestGoogleAIEngine:
    """Test suite for GoogleAIEngine class."""

    def test_init_with_api_key(self):
        """Test initialization with API key."""
        engine = GoogleAIEngine(api_key="test_key")
        assert engine.api_key == "test_key"
        assert engine.vertexai is False
        assert engine.max_tokens == GoogleAIEngine.MAX_OUTPUT_TOKENS
        assert engine.temperature == GoogleAIEngine.DEFAULT_TEMPERATURE

    def test_init_with_vertexai(self):
        """Test initialization with Vertex AI."""
        engine = GoogleAIEngine(vertexai=True, gcp_project="test_project", gcp_location="us-central1")
        assert engine.vertexai is True
        assert engine.gcp_project == "test_project"
        assert engine.gcp_location == "us-central1"

    def test_init_no_api_key_no_vertexai_raises_error(self):
        """Test that initialization without API key or Vertex AI raises error."""
        with pytest.raises(ValueError, match="Either api_key or vertexai must be provided"):
            GoogleAIEngine()

    def test_init_vertexai_without_project_raises_error(self):
        """Test that Vertex AI without project raises error."""
        with pytest.raises(ValueError, match="You must provide gcp_project and gcp_location"):
            GoogleAIEngine(vertexai=True, gcp_project="test_project")

    def test_init_vertexai_without_location_raises_error(self):
        """Test that Vertex AI without location raises error."""
        with pytest.raises(ValueError, match="You must provide gcp_project and gcp_location"):
            GoogleAIEngine(vertexai=True, gcp_location="us-central1")

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""
        safety_settings = [Mock()]
        engine = GoogleAIEngine(
            api_key="test_key",
            vertexai=False,
            prompt="test prompt",
            max_tokens=2048,
            temperature=0.8,
            top_p=1,
            seed=42,
            safety_settings=safety_settings,
            thinking_budget=1000,
            response_type="text/plain",
            ground_google_search=True,
        )

        assert engine.api_key == "test_key"
        assert engine.vertexai is False
        assert engine.prompt == "test prompt"
        assert engine.max_tokens == 2048
        assert engine.temperature == 0.8
        assert engine.top_p == 1
        assert engine.seed == 42
        assert engine.safety_settings == safety_settings
        assert engine.thinking_budget == 1000
        assert engine.response_type == "text/plain"
        assert engine.ground_google_search is True

    def test_init_with_default_safety_settings(self):
        """Test that default safety settings are created when not provided."""
        engine = GoogleAIEngine(api_key="test_key")
        assert engine.safety_settings is not None
        assert len(engine.safety_settings) == 4
        assert all(isinstance(setting, types.SafetySetting) for setting in engine.safety_settings)

    @patch("datagrunt.core.ai.engines.genai.Client")
    def test_client_with_api_key(self, mock_client_class):
        """Test _client method with API key."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        engine = GoogleAIEngine(api_key="test_key")
        client = engine._client()

        mock_client_class.assert_called_once_with(api_key="test_key")
        assert client == mock_client

    @patch("datagrunt.core.ai.engines.genai.Client")
    def test_client_with_vertexai(self, mock_client_class):
        """Test _client method with Vertex AI."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        engine = GoogleAIEngine(vertexai=True, gcp_project="test_project", gcp_location="us-central1")
        client = engine._client()

        mock_client_class.assert_called_once_with(vertexai=True, project="test_project", location="us-central1")
        assert client == mock_client

    def test_contents_creation(self):
        """Test _contents method creates proper Content objects."""
        engine = GoogleAIEngine(api_key="test_key")
        prompt = "Test prompt"

        contents = engine._contents(prompt)

        assert isinstance(contents, list)
        assert len(contents) == 1
        assert isinstance(contents[0], types.Content)
        assert contents[0].role == "user"

    def test_safety_settings_creation(self):
        """Test _safety_settings method creates proper safety settings."""
        engine = GoogleAIEngine(api_key="test_key")
        safety_settings = engine._safety_settings()

        assert isinstance(safety_settings, list)
        assert len(safety_settings) == 4

        categories = [setting.category for setting in safety_settings]
        expected_categories = [
            types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        ]

        for expected_category in expected_categories:
            assert expected_category in categories

    def test_ground_in_google_search(self):
        """Test _ground_in_google_search method."""
        engine = GoogleAIEngine(api_key="test_key")
        tools = engine._ground_in_google_search()

        assert isinstance(tools, list)
        assert len(tools) == 1
        assert isinstance(tools[0], types.Tool)

    def test_content_config_without_google_search(self):
        """Test _content_config method without Google Search."""
        engine = GoogleAIEngine(api_key="test_key", ground_google_search=False)
        config = engine._content_config("Test system instruction")

        assert isinstance(config, types.GenerateContentConfig)
        assert config.temperature == engine.temperature
        assert config.top_p == engine.top_p
        assert config.seed == engine.seed
        assert config.max_output_tokens == engine.max_tokens
        assert hasattr(config, "tools") is False or config.tools is None

    def test_content_config_with_google_search(self):
        """Test _content_config method with Google Search."""
        engine = GoogleAIEngine(api_key="test_key", ground_google_search=True)
        config = engine._content_config("Test system instruction")

        assert isinstance(config, types.GenerateContentConfig)
        assert config.tools is not None
        assert len(config.tools) == 1

    def test_content_config_default_system_instruction(self):
        """Test _content_config with default system instruction."""
        engine = GoogleAIEngine(api_key="test_key")
        config = engine._content_config()

        assert isinstance(config, types.GenerateContentConfig)
        # Should use default empty string for system instruction

    @patch("datagrunt.core.ai.engines.genai.Client")
    def test_generate_content_success(self, mock_client_class):
        """Test generate_content method with successful response."""
        # Setup mocks
        mock_response = Mock()
        mock_response.text = "Generated content"

        mock_client = Mock()
        mock_client.models.generate_content.return_value = mock_response
        mock_client_class.return_value = mock_client

        # Test
        engine = GoogleAIEngine(api_key="test_key")
        result = engine.generate_content(
            model="gemini-pro", prompt="Test prompt", system_instruction="Test instruction"
        )

        # Assertions
        assert result == "Generated content"
        mock_client.models.generate_content.assert_called_once()

    @patch("datagrunt.core.ai.engines.genai.Client")
    def test_generate_content_with_kwargs(self, mock_client_class):
        """Test generate_content method with additional kwargs."""
        mock_response = Mock()
        mock_response.text = "Generated content"

        mock_client = Mock()
        mock_client.models.generate_content.return_value = mock_response
        mock_client_class.return_value = mock_client

        engine = GoogleAIEngine(api_key="test_key")
        result = engine.generate_content(model="gemini-pro", prompt="Test prompt", extra_param="extra_value")

        assert result == "Generated content"

    def test_generate_embeddings_not_implemented(self):
        """Test that generate_embeddings raises NotImplementedError."""
        engine = GoogleAIEngine(api_key="test_key")

        with pytest.raises(NotImplementedError, match="Embedding generation is not yet implemented"):
            engine.generate_embeddings()

    def test_constants_are_defined(self):
        """Test that all expected constants are defined."""
        assert hasattr(GoogleAIEngine, "THINKING_BUDGET")
        assert hasattr(GoogleAIEngine, "DEFAULT_RESPONSE_JSON_MIME_TYPE")
        assert hasattr(GoogleAIEngine, "MAX_OUTPUT_TOKENS")
        assert hasattr(GoogleAIEngine, "DEFAULT_TEMPERATURE")
        assert hasattr(GoogleAIEngine, "DEFAULT_TOP_P")
        assert hasattr(GoogleAIEngine, "DEFAULT_SEED")
        assert hasattr(GoogleAIEngine, "DEFAULT_SYSTEM_INSTRUCTIONS")

    def test_constants_values(self):
        """Test that constants have expected values."""
        assert GoogleAIEngine.THINKING_BUDGET == -1
        assert GoogleAIEngine.DEFAULT_RESPONSE_JSON_MIME_TYPE == "application/json"
        assert GoogleAIEngine.MAX_OUTPUT_TOKENS == 8192
        assert GoogleAIEngine.DEFAULT_TEMPERATURE == 0.5
        assert GoogleAIEngine.DEFAULT_TOP_P == 1
        assert GoogleAIEngine.DEFAULT_SEED == 0
        assert GoogleAIEngine.DEFAULT_SYSTEM_INSTRUCTIONS == ""

    def test_inheritance(self):
        """Test that GoogleAIEngine properly inherits from BaseAIEngine."""
        engine = GoogleAIEngine(api_key="test_key")
        assert isinstance(engine, BaseAIEngine)

    @patch("datagrunt.core.ai.engines.genai.Client")
    def test_generate_content_api_call_structure(self, mock_client_class):
        """Test that generate_content makes the API call with correct structure."""
        mock_response = Mock()
        mock_response.text = "Generated content"

        mock_client = Mock()
        mock_client.models.generate_content.return_value = mock_response
        mock_client_class.return_value = mock_client

        engine = GoogleAIEngine(api_key="test_key")
        engine.generate_content("gemini-pro", "Test prompt")

        # Verify the API call was made with the right parameters
        call_args = mock_client.models.generate_content.call_args
        assert call_args is not None
        assert "model" in call_args.kwargs
        assert "contents" in call_args.kwargs
        assert "config" in call_args.kwargs
        assert call_args.kwargs["model"] == "gemini-pro"
