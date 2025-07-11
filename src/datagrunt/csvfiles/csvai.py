"""Module for using AI tools to analyze CSV files."""

# standard library imports
import json

# third-party imports

# local imports
from datagrunt.ai import prompts
from datagrunt.ai import AIEngineFactory
from datagrunt.core import CSVStringSample

class CSVSchemaReportAIGenerated:
    """Class to generate a CSV schema report using AI tools."""

    def __init__(self, filepath, engine, api_key=None, **kwargs):
        """Initialize the CSV Schema Report class."""
        self.filepath = filepath
        self.engine = engine.lower().replace(' ', '')
        self.api_key = api_key
        self.kwargs = kwargs

    def _create_engine(self):
        """Create an AI engine instance."""
        return AIEngineFactory(self.api_key, self.engine, **self.kwargs).create_engine()

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
            return_json (bool, optional): Whether to return the response as a JSON string or leave as a dict.

        Returns:
            dict: The generated schema from the CSV string.
        """
        if not prompt:
            csv_string = CSVStringSample(self.filepath).csv_string_sample_by_quality
            prompt = prompts.CSV_SCHEMA_PROMPT.format(csv_sample_string=csv_string)
        if not system_instructions:
            system_instructions = prompts.CSV_SCHEMA_SYSTEM_INSTRUCTIONS

        response_text = self._create_engine().generate_content(
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
