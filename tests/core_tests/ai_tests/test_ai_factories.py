"""This module contains tests for the AI factory classes."""

from unittest.mock import Mock

import pytest

from datagrunt.core.ai.engines import GoogleAIEngine
from datagrunt.core.ai.factories import AIEngineFactory


class TestAIEngineFactory:
    """Test suite for AIEngineFactory class."""

    def test_init_with_valid_engine(self):
        """Test initialization with a valid engine."""
        factory = AIEngineFactory(api_key="test_key", engine="google")
        assert factory.api_key == "test_key"
        assert factory.engine == "google"
        assert factory.kwargs == {}

    def test_init_with_valid_engine_and_kwargs(self):
        """Test initialization with valid engine and additional kwargs."""
        kwargs = {"temperature": 0.7, "max_tokens": 1000}
        factory = AIEngineFactory(api_key="test_key", engine="google", **kwargs)
        assert factory.api_key == "test_key"
        assert factory.engine == "google"
        assert factory.kwargs == kwargs

    def test_init_with_case_insensitive_engine(self):
        """Test initialization handles case insensitive engine names."""
        factory = AIEngineFactory(api_key="test_key", engine="GOOGLE")
        assert factory.engine == "google"

        factory = AIEngineFactory(api_key="test_key", engine="Google")
        assert factory.engine == "google"

    def test_init_with_spaces_in_engine_name(self):
        """Test initialization handles spaces in engine names."""
        factory = AIEngineFactory(api_key="test_key", engine="goog le")
        assert factory.engine == "google"

        factory = AIEngineFactory(api_key="test_key", engine=" google ")
        assert factory.engine == "google"

    def test_init_with_invalid_engine(self):
        """Test initialization raises error for invalid engine."""
        with pytest.raises(ValueError, match="Unsupported AI engine: invalid"):
            AIEngineFactory(api_key="test_key", engine="invalid")

    def test_init_with_empty_engine(self):
        """Test initialization raises error for empty engine."""
        with pytest.raises(ValueError, match="Unsupported AI engine: "):
            AIEngineFactory(api_key="test_key", engine="")

    def test_create_engine_google(self):
        """Test creating a Google AI engine."""
        mock_engine_instance = Mock()
        mock_google_engine = Mock(return_value=mock_engine_instance)

        factory = AIEngineFactory(api_key="test_key", engine="google")
        # Replace the engine class in the factory's AI_ENGINES dict
        original_engine = factory.AI_ENGINES['google']
        factory.AI_ENGINES['google'] = mock_google_engine

        try:
            result = factory.create_engine()
            mock_google_engine.assert_called_once_with("test_key")
            assert result == mock_engine_instance
        finally:
            # Restore the original engine
            factory.AI_ENGINES['google'] = original_engine

    def test_create_engine_with_kwargs(self):
        """Test creating an engine with additional kwargs."""
        mock_engine_instance = Mock()
        mock_google_engine = Mock(return_value=mock_engine_instance)

        kwargs = {"temperature": 0.7, "max_tokens": 1000}
        factory = AIEngineFactory(api_key="test_key", engine="google", **kwargs)
        # Replace the engine class in the factory's AI_ENGINES dict
        original_engine = factory.AI_ENGINES['google']
        factory.AI_ENGINES['google'] = mock_google_engine

        try:
            result = factory.create_engine()
            mock_google_engine.assert_called_once_with("test_key", **kwargs)
            assert result == mock_engine_instance
        finally:
            # Restore the original engine
            factory.AI_ENGINES['google'] = original_engine

    def test_create_engine_unsupported_after_init(self):
        """Test error when trying to create an unsupported engine after modifying AI_ENGINES."""
        factory = AIEngineFactory(api_key="test_key", engine="google")
        # Simulate the scenario where the engine is removed from AI_ENGINES after init
        original_engines = factory.AI_ENGINES.copy()
        factory.AI_ENGINES.clear()

        try:
            with pytest.raises(ValueError, match="Unsupported AI engine: google"):
                factory.create_engine()
        finally:
            # Restore the original engines
            factory.AI_ENGINES.update(original_engines)

    def test_ai_engines_mapping(self):
        """Test that AI_ENGINES mapping contains expected engines."""
        assert 'google' in AIEngineFactory.AI_ENGINES
        assert AIEngineFactory.AI_ENGINES['google'] == GoogleAIEngine

    def test_factory_with_none_api_key(self):
        """Test factory initialization with None API key."""
        factory = AIEngineFactory(api_key=None, engine="google")
        assert factory.api_key is None
        assert factory.engine == "google"

    def test_create_engine_with_vertexai_params(self):
        """Test creating an engine with Vertex AI parameters."""
        mock_engine_instance = Mock()
        mock_google_engine = Mock(return_value=mock_engine_instance)

        factory = AIEngineFactory(
            api_key=None,
            engine="google",
            vertexai=True,
            gcp_project="test-project",
            gcp_location="us-central1"
        )
        # Replace the engine class in the factory's AI_ENGINES dict
        original_engine = factory.AI_ENGINES['google']
        factory.AI_ENGINES['google'] = mock_google_engine

        try:
            result = factory.create_engine()
            mock_google_engine.assert_called_once_with(
                None,
                vertexai=True,
                gcp_project="test-project",
                gcp_location="us-central1"
            )
            assert result == mock_engine_instance
        finally:
            # Restore the original engine
            factory.AI_ENGINES['google'] = original_engine

    def test_multiple_spaces_and_case_combinations(self):
        """Test various combinations of spaces and cases in engine names."""
        test_cases = [
            "  GOOGLE  ",
            "Go og le",
            "G O O G L E",
            "google",
            "GOOGLE",
            "Google",
            "gOoGlE"
        ]

        for engine_name in test_cases:
            factory = AIEngineFactory(api_key="test_key", engine=engine_name)
            assert factory.engine == "google"
