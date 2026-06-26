"""Shared Parquet components: file validation, normalization, table naming."""

# standard library
import re
from pathlib import Path

# local libraries
from datagrunt.core.csv_io import _compute
from datagrunt.core.file_io import FileProperties


def normalize_parquet_columns(columns):
    """Normalize Parquet column names via the shared compute backend.

    Reuses the same ``normalize_columns`` implementation as the CSV subsystem so
    Parquet column normalization is byte-for-byte identical (lowercase,
    non-alphanumeric runs collapsed to ``_``, leading-digit prefix, then
    collision-safe uniquification).
    """
    return _compute.backend().normalize_columns(list(columns))


def parquet_table_name(filepath):
    """Return a deterministic, SQL-safe table name for a Parquet file.

    ``query_data`` registers the file's single table under this name, so callers
    can always reference ``reader.db_table``.
    """
    safe = re.sub(r"[^0-9a-zA-Z_]", "_", Path(filepath).stem)
    return f"tbl_{safe}"


class ParquetComponents(FileProperties):
    """Combine file properties with Parquet-specific validation."""

    def __init__(self, filepath):
        """Initialize and validate the path points at a Parquet file.

        Args:
            filepath (str or Path): Path to the Parquet file.

        Raises:
            ValueError: If the file extension is not ``.parquet``.
        """
        super().__init__(filepath)  # validates existence; sets self._ext
        if not self.is_parquet:
            raise ValueError(
                f"'{self.filepath}' is not a Parquet file. "
                f"Expected one of: {', '.join(sorted(self._ext.parquet_extensions))}."
            )
