"""Shared engine lifecycle for the Parquet public API (ParquetReader/Writer)."""

from functools import cached_property
from typing import ClassVar

from datagrunt.core import ParquetComponents, ParquetEngineFactory


class _ParquetEngineBacked(ParquetComponents):
    """Engine ownership + lifecycle shared by ParquetReader and ParquetWriter.

    Owns the single cached engine plus optional ``close()``/context-manager
    semantics. Subclasses set ``_engine_role`` to choose the factory builder.
    """

    _engine_role: ClassVar[str | None] = None  # "reader" or "writer"

    def __init__(self, filepath, normalize_columns=False, **read_options):
        self.normalize_columns = normalize_columns
        self._read_options = read_options
        super().__init__(filepath)  # validates Parquet extension

    @cached_property
    def _engine(self):
        """Build this object's engine once and reuse it (released on GC)."""
        factory = ParquetEngineFactory(self.filepath, normalize_columns=self.normalize_columns, **self._read_options)
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
