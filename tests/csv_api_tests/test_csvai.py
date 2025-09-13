"""This module contains tests for the CSV AI functionality."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from datagrunt.csv_api.csvai import CSVSchemaReportAIGenerated

# All supported AI engines for CSVSchemaReportAIGenerated
ALL_AI_ENGINES = ["google"]


class TestCSVSchemaReportAIGenerated:
    """Test suite for CSVSchemaReportAIGenerated class."""

    def test_init_with_valid_parameters(self):
        """Test initialization with valid parameters."""
        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")
        assert csv_ai.filepath == Path("test.csv")
        assert csv_ai.engine == ALL_AI_ENGINES[0]
        assert csv_ai.api_key == "test_key"
        assert csv_ai.kwargs == {}

    def test_init_with_kwargs(self):
        """Test initialization with additional kwargs."""
        kwargs = {"temperature": 0.7, "max_tokens": 1000}
        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key", **kwargs)
        assert csv_ai.kwargs == kwargs

    def test_init_with_case_insensitive_engine(self):
        """Test initialization handles case insensitive engine names."""
        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine="GOOGLE", api_key="test_key")
        assert csv_ai.engine == "google"

    def test_init_with_spaces_in_engine_name(self):
        """Test initialization handles spaces in engine names."""
        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine="goog le", api_key="test_key")
        assert csv_ai.engine == "google"

    def test_init_with_invalid_engine(self):
        """Test initialization raises error for invalid engine."""
        with pytest.raises(ValueError, match="Unsupported AI engine: invalid"):
            CSVSchemaReportAIGenerated(filepath="test.csv", engine="invalid", api_key="test_key")

    def test_init_with_ground_google_search_raises_error(self):
        """Test initialization raises error when ground_google_search is True."""
        with pytest.raises(ValueError, match="Grounding in Google Search is not supported"):
            CSVSchemaReportAIGenerated(
                filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key", ground_google_search=True
            )

    def test_init_without_api_key(self):
        """Test initialization without API key."""
        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0])
        assert csv_ai.api_key is None

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_create_engine(self, mock_factory_class):
        """Test _create_engine method."""
        mock_factory = Mock()
        mock_engine = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0],
                api_key="test_key", temperature=0.7)

        result = csv_ai._create_engine()

        mock_factory_class.assert_called_once_with("test_key", ALL_AI_ENGINES[0], temperature=0.7)
        mock_factory.create_engine.assert_called_once()
        assert result == mock_engine

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_get_ai_response_success(self, mock_factory_class):
        """Test _get_ai_response method with successful response."""
        # Setup mocks
        mock_engine = Mock()
        mock_engine.generate_content.return_value = '{"key": "value"}'

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        result = csv_ai._get_ai_response("model", "prompt", "instructions")

        assert result == {"key": "value"}
        mock_engine.generate_content.assert_called_once_with(
            model="model", prompt="prompt", system_instruction="instructions"
        )

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_get_ai_response_json_decode_error(self, mock_factory_class):
        """Test _get_ai_response method with JSON decode error."""
        mock_engine = Mock()
        mock_engine.generate_content.return_value = "invalid json"

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        with pytest.raises(ValueError) as exc_info:
            csv_ai._get_ai_response("model", "prompt", "instructions")

        assert "The model's response was not a valid JSON object" in str(exc_info.value)
        assert "max_tokens" in str(exc_info.value)
        assert "invalid json" in str(exc_info.value)

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_get_ai_response_none_response(self, mock_factory_class):
        """Test _get_ai_response method when model returns None."""
        mock_engine = Mock()
        mock_engine.generate_content.return_value = None

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        with pytest.raises(RuntimeError) as exc_info:
            csv_ai._get_ai_response("model", "prompt", "instructions")

        assert "The model returned None instead of a text response" in str(exc_info.value)

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_get_ai_response_type_error(self, mock_factory_class):
        """Test _get_ai_response method with TypeError."""
        mock_engine = Mock()
        mock_engine.generate_content.side_effect = TypeError("Type error occurred")

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        with pytest.raises(TypeError) as exc_info:
            csv_ai._get_ai_response("model", "prompt", "instructions")

        assert "The model did not return a text response" in str(exc_info.value)
        assert "max_tokens" in str(exc_info.value)

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_get_ai_response_runtime_error(self, mock_factory_class):
        """Test _get_ai_response method with generic exception."""
        mock_engine = Mock()
        mock_engine.generate_content.side_effect = Exception("API connection error")

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        with pytest.raises(RuntimeError) as exc_info:
            csv_ai._get_ai_response("model", "prompt", "instructions")

        assert "An unexpected error occurred" in str(exc_info.value)

    @patch("datagrunt.csv_api.csvai.CSVStringSample")
    @patch("datagrunt.csv_api.csvai.prompts")
    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_generate_csv_schema_report_default_prompt(self, mock_factory_class, mock_prompts, mock_csv_sample_class):
        """Test generate_csv_schema_report with default prompt."""
        # Setup mocks
        mock_csv_sample = Mock()
        mock_csv_sample.csv_string_sample_by_quality = "sample,csv,data\n1,2,3"
        mock_csv_sample_class.return_value = mock_csv_sample

        mock_prompts.CSV_SCHEMA_PROMPT.format.return_value = "formatted prompt"
        mock_prompts.CSV_SCHEMA_SYSTEM_INSTRUCTIONS = "system instructions"

        mock_engine = Mock()
        mock_engine.generate_content.return_value = '{"schema": "data"}'

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        result = csv_ai.generate_csv_schema_report("model")

        assert result == {"schema": "data"}
        mock_csv_sample_class.assert_called_once_with(Path("test.csv"))
        mock_prompts.CSV_SCHEMA_PROMPT.format.assert_called_once_with(csv_sample_string="sample,csv,data\n1,2,3")

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_generate_csv_schema_report_custom_prompt(self, mock_factory_class):
        """Test generate_csv_schema_report with custom prompt."""
        mock_engine = Mock()
        mock_engine.generate_content.return_value = '{"custom": "schema"}'

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        result = csv_ai.generate_csv_schema_report(
            "model", prompt="custom prompt", system_instructions="custom instructions"
        )

        assert result == {"custom": "schema"}
        mock_engine.generate_content.assert_called_once_with(
            model="model", prompt="custom prompt", system_instruction="custom instructions"
        )

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_generate_csv_schema_report_return_json(self, mock_factory_class):
        """Test generate_csv_schema_report with return_json=True."""
        mock_engine = Mock()
        mock_engine.generate_content.return_value = '{"key": "value"}'

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        result = csv_ai.generate_csv_schema_report("model", prompt="test prompt", return_json=True)

        expected_json = json.dumps({"key": "value"}, indent=4)
        assert result == expected_json

    @patch("datagrunt.csv_api.csvai.CSVStringSample")
    @patch("datagrunt.csv_api.csvai.prompts")
    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_generate_csv_schema_report_default_system_instructions(
        self, mock_factory_class, mock_prompts, mock_csv_sample_class
    ):
        """Test generate_csv_schema_report with default system instructions."""
        # Setup mocks
        mock_csv_sample = Mock()
        mock_csv_sample.csv_string_sample_by_quality = "data"
        mock_csv_sample_class.return_value = mock_csv_sample

        mock_prompts.CSV_SCHEMA_PROMPT.format.return_value = "prompt"
        mock_prompts.CSV_SCHEMA_SYSTEM_INSTRUCTIONS = "default instructions"

        mock_engine = Mock()
        mock_engine.generate_content.return_value = '{"test": "result"}'

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        csv_ai.generate_csv_schema_report("model", prompt="custom prompt")

        mock_engine.generate_content.assert_called_once_with(
            model="model", prompt="custom prompt", system_instruction="default instructions"
        )

    def test_filepath_attribute_access(self):
        """Test that filepath attribute can be accessed."""
        csv_ai = CSVSchemaReportAIGenerated(filepath="/path/to/test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")
        assert csv_ai.filepath == Path("/path/to/test.csv")

    def test_multiple_engine_name_normalizations(self):
        """Test various engine name normalizations."""
        test_cases = [
            ("Google", "google"),
            ("GOOGLE", "google"),
            ("  google  ", "google"),
            ("Go og le", "google"),
            ("gOoGlE", "google"),
        ]

        for input_engine, expected_engine in test_cases:
            csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=input_engine, api_key="test_key")
            assert csv_ai.engine == expected_engine

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_kwargs_passed_to_factory(self, mock_factory_class):
        """Test that kwargs are properly passed to the factory."""
        mock_factory = Mock()
        mock_engine = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        kwargs = {"temperature": 0.8, "max_tokens": 2048, "custom_param": "value"}

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key", **kwargs)

        csv_ai._create_engine()

        mock_factory_class.assert_called_once_with("test_key", ALL_AI_ENGINES[0], **kwargs)

    @patch("datagrunt.csv_api.csvai.AIEngineFactory")
    def test_json_loads_with_complex_response(self, mock_factory_class):
        """Test _get_ai_response with complex JSON response."""
        complex_response = {
            "schema": {
                "columns": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}],
                "metadata": {"rows": 100, "encoding": "utf-8"},
            }
        }

        mock_engine = Mock()
        mock_engine.generate_content.return_value = json.dumps(complex_response)

        mock_factory = Mock()
        mock_factory.create_engine.return_value = mock_engine
        mock_factory_class.return_value = mock_factory

        csv_ai = CSVSchemaReportAIGenerated(filepath="test.csv", engine=ALL_AI_ENGINES[0], api_key="test_key")

        result = csv_ai._get_ai_response("model", "prompt", "instructions")

        assert result == complex_response
        assert result["schema"]["columns"][0]["name"] == "id"
        assert result["schema"]["metadata"]["rows"] == 100
