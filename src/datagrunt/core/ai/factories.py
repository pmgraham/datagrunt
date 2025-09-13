"""Factory module for creating AI factory instances."""

# local libraries
from datagrunt.core.ai.engines import AIEngineProperties, GoogleAIEngine


class AIEngineFactory:
    """Factory class for creating AI engine instances."""

    AI_ENGINES = {
        "google": GoogleAIEngine,
    }

    def __init__(self, api_key, engine, **kwargs):
        """
        Initialize the AI Engine Factory class.

        Args:
            api_key (str): API key for the AI provider.
            engine (str): type of engine to create by the factory.
            **kwargs: Additional parameters for the AI provider.
        """
        self.api_key = api_key
        self.engine = engine.lower().replace(" ", "")
        self.kwargs = kwargs
        if self.engine not in AIEngineProperties.valid_engines:
            raise ValueError(f"Unsupported AI engine: {self.engine}")

    def create_engine(self):
        """Create an AI engine instance."""
        engine_class = self.AI_ENGINES.get(self.engine)
        if engine_class:
            return engine_class(self.api_key, **self.kwargs)
        else:
            raise ValueError(f"Unsupported AI engine: {self.engine}")
