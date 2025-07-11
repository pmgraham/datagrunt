"""Module to create engines for interfacing with different large language models and providers."""

# standard library imports
from abc import ABC, abstractmethod
from dataclasses import dataclass
import json

# third-party imports
import anthropic
from google import genai
from google.genai import types

# local imports
from datagrunt.ai import prompts
from datagrunt.core import CSVStringSample

@dataclass
class EngineProperties:
    """Base properties for CSV operations."""
    valid_engines: tuple = ('google')

class BaseAIEngine(ABC):
    """Abstract base class for AI providers."""

    def __init__(self, api_key=None, **kwargs):
        """Initialize the AI provider."""
        self.api_key = api_key
        self.vertexai = kwargs.pop('vertexai', False)
        self.gcp_project = kwargs.pop('gcp_project', None)
        self.gcp_location = kwargs.pop('gcp_location', None)
        if not self.api_key and not self.vertexai:
            raise ValueError("Either api_key or vertexai must be provided.")
        if not self.api_key and self.vertexai and (not self.gcp_project or not self.gcp_location):
            raise ValueError("You must provide gcp_project and gcp_location when using Vertex AI.")

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

class GoogleEngine(BaseAIEngine):
    """Class to interact with the Google GenAI API."""

    THINKING_BUDGET = -1  # Default thinking budget for the model
    RESPONSE_JSON_MIME_TYPE = "application/json"  # Default response MIME type
    MAX_OUTPUT_TOKENS = 8192  # Maximum output tokens for the model

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
