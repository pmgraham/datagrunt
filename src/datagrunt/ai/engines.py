"""Module to create engines for interfacing with different large language models and providers."""

# standard library imports
from abc import ABC, abstractmethod
import json
import os

# third-party imports
import anthropic
from google import genai
from google.genai import types

# local imports
from datagrunt.ai import prompts
from datagrunt.core import CSVStringSample

class BaseAIEngine(ABC):
    """Abstract base class for AI providers."""

    @abstractmethod
    def __init__(self):
        """Initialize the AI provider."""
        pass

    @abstractmethod
    def generate_content(
        self,
        prompt,
        max_tokens,
        temperature,
        **kwargs
    ):
        """Generate text from a prompt.

        Args:
            prompt: The prompt to send to the model
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            **kwargs: Additional provider-specific parameters

        Returns:
            Generated text
        """
        pass

    @abstractmethod
    def generate_embeddings(
        self,
        text,
        **kwargs
    ):
        """Generate embeddings for text.

        Args:
            text: Text to embed
            **kwargs: Additional provider-specific parameters

        Returns:
            Embedding vector
        """
        pass

class Google(BaseAIEngine):
    """Class to interact with the Google GenAI API."""

    THINKING_BUDGET = -1  # Default thinking budget for the model
    RESPONSE_JSON_MIME_TYPE = "application/json"  # Default response MIME type
    MAX_OUTPUT_TOKENS = 8192  # Maximum output tokens for the model

    def __init__(self, file_path, api_key=None, **kwargs):
        """Initialize the GoogleGenAI.

        Args:
            api_key (str, optional): The API key for Google GenAI.
            gcp_project (str, optional): The GCP project ID for Vertex AI.
            gcp_location (str, optional): The GCP location for Vertex AI.
            vertexai (bool, optional): If True, use Vertex AI; otherwise use the API key.
        """
        self.file_path = file_path
        self.api_key = api_key
        self.vertexai = kwargs.pop('vertexai', False)
        self.gcp_project = kwargs.pop('gcp_project', None)
        self.gcp_location = kwargs.pop('gcp_location', None)

        if not self.api_key and not self.vertexai:
            raise ValueError("Either api_key or vertexai must be provided.")
        if not self.api_key and self.vertexai and (not self.gcp_project or not self.gcp_location):
            raise ValueError("You must provide gcp_project and gcp_location when using Vertex AI.")

    def _client(self):
        """Create and return a GenAI client."""
        if self.vertexai:
            return genai.Client(
                vertexai=self.vertexai,
                project=self.gcp_project,
                location=self.gcp_location
            )
        return genai.Client(api_key=self.api_key)

    def _contents(self, prompt):
        """Create content for the model based on the provided prompt.

        Args:
            prompt (str): The prompt to send to the model.
        Returns:
            list: A list of Content objects with the user role and parts.
        """
        return [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=prompt)],
            ),
        ]

    def _safety_settings(self):
        """Create safety settings for the model."""
        return [
            types.SafetySetting(
                category="HARM_CATEGORY_HATE_SPEECH",
                threshold="OFF"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_DANGEROUS_CONTENT",
                threshold="OFF"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                threshold="OFF"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_HARASSMENT",
                threshold="OFF"
            )
        ]

    def _content_config(self, system_instruction=None):
        if not system_instruction:
            system_instruction = ""
        config = types.GenerateContentConfig(
            temperature = 0.5,
            top_p = 1,
            seed = 0,
            max_output_tokens = self.MAX_OUTPUT_TOKENS,
            safety_settings = self._safety_settings(),
            system_instruction=[types.Part.from_text(text=system_instruction)],
            thinking_config=types.ThinkingConfig(
                thinking_budget=self.THINKING_BUDGET,
            ),
            response_mime_type = self.RESPONSE_JSON_MIME_TYPE,
        )
        return config


        pass

    def generate_content(self, model, prompt, system_instruction=None):
        """Generate content using the Google GenAI API.

        Args:
            model (str): The name of the model to use.
            prompt (str): The prompt to send to the model.
            system_instruction (str, optional): System instructions to guide the model's response.
        Returns:
            str: The generated response from the model.
        """
        contents = self._contents(prompt)
        generate_content_config = self._content_config(system_instruction)

        response = self._client().models.generate_content(
            model=model,
            contents=contents,
            config=generate_content_config,
        )

        return response.text

    def generate_embeddings(self):
        raise NotImplementedError("Embedding generation is not yet implemented for the Google provider.")

    def analyze_csv_schema(
            self,
            model,
            prompt=None,
            system_instructions=None,
            return_json=False
        ):
        """Generate a CSV schema from a string using the Google GenAI API.

        Args:
            model (str): The name of the model to use.
            prompt (str, optional): The prompt to send to the model. If not provided, a default prompt will be used.
            system_instructions (str, optional): System instructions to guide the model's response.
        Returns:
            dict: The generated schema from the CSV string.
        """
        if not prompt:
            csv_string = CSVStringSample(self.file_path).csv_string_sample_by_quality
            prompt = prompts.CSV_SCHEMA_PROMPT.format(csv_sample_string=csv_string)
        if not system_instructions:
            system_instructions = prompts.CSV_SCHEMA_SYSTEM_INSTRUCTIONS

        response_text = self.generate_content(
            model=model,
            prompt=prompt,
            system_instruction=system_instructions
        )

        try:
            csv_schema_report = json.loads(response_text)
        except json.JSONDecodeError:
            raise ValueError("The response is not a valid JSON string.")

        if return_json:
            return json.dumps(csv_schema_report, indent=4)

        return csv_schema_report

    def suggest_data_transformations(self):
        """Suggest data transformations based on user goals."""
        pass

    def generate_sql_query(self):
        """Generate SQL query from natural language."""
        pass

#TODO - walk through this class in detail and ensure it meets the requirements.
class AnthropicProvider(BaseAIProvider):
    """Anthropic AI provider for Claude models."""

    SUPPORTED_MODELS = {
        'claude-3-opus-20240229': {
            'name': 'Claude 3 Opus',
            'max_tokens': 4096,
            'description': 'Most capable model for complex tasks'
        },
        'claude-3-sonnet-20240229': {
            'name': 'Claude 3 Sonnet',
            'max_tokens': 4096,
            'description': 'Balanced performance and speed'
        },
        'claude-3-haiku-20240307': {
            'name': 'Claude 3 Haiku',
            'max_tokens': 4096,
            'description': 'Fastest model for simple tasks'
        },
        'claude-2.1': {
            'name': 'Claude 2.1',
            'max_tokens': 4096,
            'description': 'Previous generation model'
        },
        'claude-2.0': {
            'name': 'Claude 2.0',
            'max_tokens': 4096,
            'description': 'Previous generation model'
        }
    }

    DEFAULT_MODEL = 'claude-3-sonnet-20240229'

    def __init__(self, file_path, api_key, model=None):
        """Initialize Anthropic provider.

        Args:
            api_key: Anthropic API key. If not provided, will look for ANTHROPIC_API_KEY env var
            model: Model to use. Defaults to claude-3-sonnet-20240229
        """
        super().__init__()
        self.file_path = file_path
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError(
                "Anthropic API key not provided. "
                "Set ANTHROPIC_API_KEY environment variable or pass api_key parameter"
            )

        self.model = model or self.DEFAULT_MODEL
        if self.model not in self.SUPPORTED_MODELS:
            raise ValueError(
                f"Unsupported model: {self.model}. "
                f"Supported models: {list(self.SUPPORTED_MODELS.keys())}"
            )

        self.client = anthropic.Anthropic(api_key=self.api_key)

    def generate_content(
        self,
        prompt,
        max_tokens=None,
        temperature=0.5,
        **kwargs
    ):
        """Generate text using Anthropic's Claude models.

        Args:
            prompt: The prompt to send to the model
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature (0-1)
            **kwargs: Additional parameters to pass to the API

        Returns:
            Generated text response
        """
        max_tokens = max_tokens or self.SUPPORTED_MODELS[self.model]['max_tokens']

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                **kwargs
            )

            return response.content[0].text

        except Exception as e:
            raise RuntimeError(f"Error generating text with Anthropic: {str(e)}")

    def generate_embeddings(
        self,
        text,
        **kwargs
    ):
        """Generate embeddings for text.

        Note: Anthropic does not currently provide embedding models.
        This method raises NotImplementedError.

        Args:
            text: Text to embed
            **kwargs: Additional parameters

        Returns:
            Embedding vector

        Raises:
            NotImplementedError: Anthropic doesn't provide embedding models
        """
        raise NotImplementedError(
            "Anthropic does not currently provide embedding models. "
            "Consider using OpenAI or Google for embeddings."
        )

    def analyze_csv_schema(
        self,
        csv_sample,
        additional_context=None
    ):
        """Analyze CSV schema using Claude.

        Args:
            csv_sample: Sample of CSV data
            additional_context: Additional context about the data

        Returns:
            Dictionary containing schema analysis
        """
        prompt = self._build_schema_analysis_prompt(csv_sample, additional_context)

        response = self.generate_content(
            prompt=prompt,
            temperature=0.3,  # Lower temperature for more structured output
            max_tokens=2000
        )

        return self._parse_schema_response(response)

    def suggest_data_transformations(
        self,
        csv_sample,
        user_goal
    ):
        """Suggest data transformations based on user goals.

        Args:
            csv_sample: Sample of CSV data
            user_goal: What the user wants to achieve

        Returns:
            List of suggested transformations
        """
        prompt = prompts.SUGGEST_DATA_TRANSFORMATIONS.format(
            csv_sample=csv_sample,
            user_goal=user_goal
        )

        response = self.generate_content(
            prompt=prompt,
            temperature=0.5,
            max_tokens=1000
        )

        # Parse numbered list from response
        suggestions = []
        for line in response.split('\n'):
            line = line.strip()
            if line and (line[0].isdigit() or line.startswith('-')):
                # Remove numbering and bullets
                suggestion = line.lstrip('0123456789.-) ').strip()
                if suggestion:
                    suggestions.append(suggestion)

        return suggestions

    def generate_sql_query(
        self,
        csv_schema,
        natural_language_query,
        table_name
    ):
        """Generate SQL query from natural language.

        Args:
            csv_schema: Schema information about the CSV
            natural_language_query: Natural language description of desired query
            table_name: Name of the table in SQL context

        Returns:
            SQL query string
        """
        schema_description = self._format_schema_for_prompt(csv_schema)

        prompt = prompts.GENERATE_SQL_QUERY.format(
            table_name=table_name,
            schema_description=schema_description,
            natural_language_query=natural_language_query
        )

        response = self.generate_content(
            prompt=prompt,
            temperature=0.2,  # Low temperature for precise SQL
            max_tokens=500
        )

        # Clean up the response to extract just the SQL
        sql = response.strip()
        # Remove markdown code blocks if present
        if sql.startswith('```'):
            sql = sql.split('```')[1]
            if sql.startswith('sql'):
                sql = sql[3:]
        sql = sql.strip().rstrip('```')

        return sql

    def _build_schema_analysis_prompt(
        self,
        csv_sample,
        additional_context
    ):
        """Build prompt for schema analysis.

        Args:
            csv_sample: Sample of CSV data
            additional_context: Additional context about the data

        Returns:
            Prompt string
        """
        prompt = prompts.CSV_SCHEMA_PROMPT.format(
            csv_sample_string=csv_sample
        )

        return prompt

    def _parse_schema_response(self, response):
        """Parse schema analysis response.

        Args:
            response: Raw response string from the model

        Returns:
            Dictionary containing parsed schema information
        """
        import json

        try:
            # Try to extract JSON from response
            response = response.strip()
            if response.startswith('```'):
                response = response.split('```')[1]
                if response.startswith('json'):
                    response = response[4:]
            response = response.strip().rstrip('```')

            return json.loads(response)
        except json.JSONDecodeError:
            # Fallback to basic parsing if JSON parsing fails
            return {
                "columns": [],
                "error": "Failed to parse response",
                "raw_response": response
            }

    def _format_schema_for_prompt(self, schema):
        """Format schema information for prompts.

        Args:
            schema: Dictionary containing schema information

        Returns:
            Formatted string representation of the schema
        """
        lines = []

        if 'columns' in schema:
            lines.append("Columns:")
            for col in schema['columns']:
                col_desc = f"  - {col['name']}: {col.get('detected_type', 'unknown')}"
                if col.get('description'):
                    col_desc += f" ({col['description']})"
                lines.append(col_desc)

        return '\n'.join(lines)