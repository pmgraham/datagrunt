"""Shared engine lifecycle for the Excel public API (ExcelReader/ExcelWriter)."""

from functools import cached_property

from datagrunt.core import ExcelComponents, ExcelEngineFactory


class _ExcelEngineBacked(ExcelComponents):
    """Engine ownership + lifecycle shared by ExcelReader and ExcelWriter.

    Owns the single cached engine plus optional ``close()``/context-manager
    semantics. Subclasses set ``_engine_role`` to choose the factory builder.
    ``.sheets`` is inherited from ``ExcelComponents`` and needs no engine.
    """

    _engine_role = None  # "reader" or "writer"

    def __init__(self, filepath, normalize_columns=False, **read_options):
        self.normalize_columns = normalize_columns
        self._read_options = read_options
        super().__init__(filepath)  # validates Excel extension

    @cached_property
    def _engine(self):
        """Build this object's engine once and reuse it (released on GC)."""
        factory = ExcelEngineFactory(self.filepath, normalize_columns=self.normalize_columns, **self._read_options)
        if self._engine_role == "reader":
            return factory.create_reader()
        return factory.create_writer()

    def close(self):
        """Release the engine's DuckDB connection if one was built. Idempotent."""
        engine = self.__dict__.get("_engine")
        if engine is not None:
            engine.close()

    def __enter__(self):
        """Enter a ``with`` block, returning this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release the engine on leaving the block; lets exceptions propagate."""
        self.close()
