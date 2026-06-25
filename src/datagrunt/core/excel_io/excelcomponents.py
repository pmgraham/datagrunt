"""Shared Excel components: sheet metadata, normalization, table naming."""

# standard library
import re
from functools import cached_property
from pathlib import Path

# third party libraries
import fastexcel

# local libraries
from datagrunt.core.csv_io import _compute
from datagrunt.core.file_io import FileProperties


def normalize_excel_columns(columns):
    """Normalize Excel column names via the shared compute backend.

    Reuses the same ``normalize_columns`` implementation as the CSV subsystem so
    Excel column normalization is byte-for-byte identical (lowercase,
    non-alphanumeric runs collapsed to ``_``, leading-digit prefix, then
    collision-safe uniquification).
    """
    return _compute.backend().normalize_columns(list(columns))


def excel_table_name(filepath):
    """Return a deterministic, SQL-safe table name for a workbook.

    Sheet-independent: ``query_data`` registers whichever sheet is selected
    under this single stable name, so callers can always reference
    ``reader.db_table`` regardless of which sheet they query.
    """
    safe = re.sub(r"[^0-9a-zA-Z_]", "_", Path(filepath).stem)
    return f"tbl_{safe}"


def resolve_sheet(sheets, sheet):
    """Resolve a sheet selector (None | name | index) to a worksheet name.

    Args:
        sheets (list[str]): Worksheet names in workbook order.
        sheet (str | int | None): ``None`` selects the first sheet; an ``int``
            selects by position; a ``str`` selects by name.

    Returns:
        str: The resolved worksheet name.

    Raises:
        ValueError: If the workbook has no sheets, or the name/index is invalid.
    """
    if not sheets:
        raise ValueError("Workbook has no sheets.")
    if sheet is None:
        return sheets[0]
    if isinstance(sheet, bool):
        raise ValueError(f"Sheet selector must be a name or index, not a bool: {sheet!r}.")
    if isinstance(sheet, int):
        try:
            return sheets[sheet]
        except IndexError:
            raise ValueError(
                f"Sheet index {sheet} out of range; workbook has {len(sheets)} sheet(s): {sheets}."
            ) from None
    if sheet in sheets:
        return sheet
    raise ValueError(f"Sheet {sheet!r} not found. Available sheets: {sheets}.")


class ExcelComponents(FileProperties):
    """Combine Excel file properties with workbook sheet metadata."""

    def __init__(self, filepath):
        """Initialize and validate the path points at an Excel workbook.

        Args:
            filepath (str or Path): Path to the Excel file.

        Raises:
            ValueError: If the file extension is not an Excel extension.
        """
        super().__init__(filepath)  # validates existence; sets self._ext
        if not self.is_excel:
            raise ValueError(
                f"'{self.filepath}' is not an Excel file. "
                f"Expected one of: {', '.join(sorted(self._ext.excel_extensions))}."
            )

    @cached_property
    def sheets(self):
        """Return the worksheet names in workbook order (``[]`` if empty/blank).

        Reads only workbook metadata (no full cell parse) via fastexcel.
        """
        if self.is_empty or self.is_blank:
            return []
        return list(fastexcel.read_excel(str(self.filepath)).sheet_names)
