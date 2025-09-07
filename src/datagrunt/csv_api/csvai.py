"""Module for using AI tools to analyze CSV files."""

# standard library imports
import json

# local imports
from datagrunt.core import AIEngineFactory, AIEngineProperties, CSVStringSample, prompts


class CSVSchemaReportAIGenerated:
    """Class to generate a CSV schema report using AI tools."""

    def __init__(self, filepath, engine, api_key=None, **kwargs):
        """Initialize the CSV Schema Report class."""
        self.filepath = filepath
        self.engine = engine.lower().replace(" ", "")
        self.api_key = api_key
        self.kwargs = kwargs
        if self.engine not in AIEngineProperties.valid_engines:
            raise ValueError(f"Unsupported AI engine: {self.engine}")
        if self.kwargs.pop("ground_google_search", False):
            raise ValueError("Grounding in Google Search is not supported for this class.")

    def _create_engine(self):
        """Create an AI engine instance."""
        return AIEngineFactory(self.api_key, self.engine, **self.kwargs).create_engine()

    def _get_ai_response(self, model, prompt, system_instructions):
        """
        Send a request to the AI engine and handle potential errors.

        Args:
            model (str): The name of the model to use.
            prompt (str): The prompt to send to the model.
            system_instructions (str): System instructions to guide the model's
            response.

        Returns:
            dict: The JSON response from the AI engine.

        Raises:
            ValueError: If the response is not valid JSON.
            TypeError: If the response is not a text string.
            RuntimeError: For any other unexpected errors.
        """
        response_text = None
        try:
            response_text = self._create_engine().generate_content(
                model=model, prompt=prompt, system_instruction=system_instructions
            )
            if response_text is None:
                raise ValueError("The model returned None instead of a text response.")
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            error_message = (
                "The model's response was not a valid JSON object. "
                "This can happen if the `max_tokens` parameter is too low, "
                f"cutting off the response. Details: {e}\n"
                f"Model Response: {response_text[:500] if response_text else 'None'}..."
            )
            raise ValueError(error_message) from e
        except TypeError as e:
            error_message = (
                "The model did not return a text response that could be "
                "processed. This can happen if the `max_tokens` parameter is "
                f"too low, cutting off the response. Details: {e}\n"
                f"Model Response: {response_text if response_text else 'None'}"
            )
            raise TypeError(error_message) from e
        except Exception as e:
            # Catching other potential errors, such as API connection issues
            error_message = f"An unexpected error occurred: {e}"
            raise RuntimeError(error_message) from e

    def generate_csv_schema_report(self, model, prompt=None, system_instructions=None, return_json=False):
        """
        Generate a CSV schema from a string using the Google GenAI API.

        Args:
            model (str): The name of the model to use.
            prompt (str, optional): The prompt to send to the model. If not
            provided, a default prompt will be used.
            system_instructions (str, optional): System instructions to guide
            the model's response.
            return_json (bool, optional): Whether to return the response as a
            JSON string or leave as a dict.

        Returns:
            dict: The generated schema from the CSV string.
        """
        if not prompt:
            csv_string = CSVStringSample(self.filepath).csv_string_sample_by_quality
            prompt = prompts.CSV_SCHEMA_PROMPT.format(csv_sample_string=csv_string)
        if not system_instructions:
            system_instructions = prompts.CSV_SCHEMA_SYSTEM_INSTRUCTIONS

        csv_schema_report = self._get_ai_response(model, prompt, system_instructions)

        if return_json:
            return json.dumps(csv_schema_report, indent=4)

        return csv_schema_report
