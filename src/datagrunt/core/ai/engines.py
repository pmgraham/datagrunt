"""Module to create engines for interfacing with different large language models and providers."""

# standard library imports
from abc import ABC, abstractmethod
from dataclasses import dataclass

# third-party imports
from google import genai
from google.genai import types

@dataclass
class AIEngineProperties:
    """Base properties for AI Engines."""
    valid_engines: tuple = ('google')

class BaseAIEngine(ABC):
    """Abstract base class for AI Engines."""

    def __init__(self, api_key=None):
        """Initialize the AI provider."""
        self.api_key = api_key

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

class GoogleAIEngine(BaseAIEngine):
    """Class to interact with the Google GenAI API."""

    THINKING_BUDGET = -1  # Default thinking budget for the model. -1 means "auto".
    DEFAULT_RESPONSE_JSON_MIME_TYPE = "application/json"  # Default response MIME type
    MAX_OUTPUT_TOKENS = 8192  # Maximum output tokens for the model
    DEFAULT_TEMPERATURE = 0.5  # Default temperature for the model
    DEFAULT_TOP_P = 1  # Default top_p for the model
    DEFAULT_SEED = 0  # Default seed for the model
    DEFAULT_SYSTEM_INSTRUCTIONS = ""  # Default system instructions for the model

    def __init__(
            self,
            api_key=None,
            vertexai=False,
            gcp_project=None,
            gcp_location=None,
            prompt=None,
            max_tokens=MAX_OUTPUT_TOKENS,
            temperature=DEFAULT_TEMPERATURE,
            top_p=DEFAULT_TOP_P,
            seed=DEFAULT_SEED,
            safety_settings=None,
            thinking_budget=THINKING_BUDGET,
            response_type=DEFAULT_RESPONSE_JSON_MIME_TYPE,
            ground_google_search=False
            ):

        """Initialize the Google AI provider."""
        super().__init__(api_key)
        self.vertexai = vertexai
        if not self.api_key and not self.vertexai:
            raise ValueError("Either api_key or vertexai must be provided.")
        self.gcp_project = gcp_project
        self.gcp_location = gcp_location
        self.prompt = prompt
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.top_p = top_p
        self.seed = seed
        self.safety_settings = safety_settings if safety_settings is not None else self._safety_settings()
        self.thinking_budget = thinking_budget
        self.response_type = response_type
        self.ground_google_search = ground_google_search
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

    def _ground_in_google_search(self):
        """Ground the model in Google Search."""
        return [
                 types.Tool(google_search=types.GoogleSearch()),
            ]

    def _content_config(self, system_instruction=None):
        """Content configuration for API calls to the LLM."""
        if not system_instruction:
            system_instruction = self.DEFAULT_SYSTEM_INSTRUCTIONS

        if self.ground_google_search:
            config = types.GenerateContentConfig(
                temperature = self.temperature,
                top_p = self.top_p,
                seed = self.seed,
                max_output_tokens = self.max_tokens,
                safety_settings = self.safety_settings,
                system_instruction=[types.Part.from_text(text=system_instruction)],
                thinking_config=types.ThinkingConfig(
                    thinking_budget=self.thinking_budget,
                ),
                response_mime_type = self.response_type,
                tools = self._ground_in_google_search()
            )
        else:
            config = types.GenerateContentConfig(
                temperature = self.temperature,
                top_p = self.top_p,
                seed = self.seed,
                max_output_tokens = self.max_tokens,
                safety_settings = self.safety_settings,
                system_instruction=[types.Part.from_text(text=system_instruction)],
                thinking_config=types.ThinkingConfig(
                    thinking_budget=self.thinking_budget,
                ),
                response_mime_type = self.response_type,
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
