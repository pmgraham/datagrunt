"""Shared engine lifecycle for the CSV public API (CSVReader/CSVWriter)."""

from functools import cached_property

from datagrunt.core import CSVComponents, CSVEngineFactory


class _CSVEngineBacked(CSVComponents):
    """Engine ownership + lifecycle shared by CSVReader and CSVWriter.

    Owns the single cached engine (which holds the DuckDB connection and import)
    plus the optional ``close()``/context-manager semantics, so neither subclass
    duplicates resource handling. Subclasses set ``_engine_role`` to choose the
    factory builder and add any role-specific state in their own ``__init__``.
    """

    _engine_role = None  # "reader" or "writer"

    def __init__(self, filepath, engine, lenient=False, normalize_columns=False):
        self.lenient = lenient
        self.normalize_columns = normalize_columns
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        CSVEngineFactory.validate_engine(self.engine)

    @cached_property
    def _engine(self):
        """Build this object's engine once and reuse it.

        The engine owns the DuckDB connection (and, for the duckdb engine, the
        imported table), so caching it means repeated operations reuse a single
        import. Released when this object goes out of scope (reference-counted),
        or earlier via the optional ``close()``/``with``.
        """
        factory = CSVEngineFactory(
            self.filepath,
            self.engine,
            lenient=self.lenient,
            normalize_columns=self.normalize_columns,
        )
        if self._engine_role == "reader":
            return factory.create_reader()
        return factory.create_writer()

    def close(self):
        """Release the engine's DuckDB connection, if one was built.

        Idempotent; never forces the engine into existence (so a reader/writer
        that has run nothing has nothing to close); the object stays usable
        afterward — a later call transparently rebuilds. Optional: datagrunt
        also releases on scope exit.
        """
        engine = self.__dict__.get("_engine")
        if engine is not None:
            engine.close()

    def __enter__(self):
        """Enter a ``with`` block, returning this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release the engine on leaving the block; lets exceptions propagate."""
        self.close()
