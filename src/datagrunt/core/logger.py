"""Module for showing logging messages."""

import logging

LARGE_FILE_WARNING = "File is large and may load into memory slowly or exceed memory capacity."
DUCKDB_ENGINE_ERROR = """DuckDB engine failed due to the following error: {error}. \
    Switching to Polars."""


def show_warning(message):
    """Show a warning message.

    Args:
        message (str): The message to show.
    """
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    return logging.warning(message)


def show_info_message(message):
    """Show an info message.

    Args:
        message (str): The message to show.
    """
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    return logging.info(message)


def show_large_file_warning():
    """Show a warning message if the file is large."""
    show_warning(LARGE_FILE_WARNING)


def duckdb_query_error(error_message):
    """Show error message if duckdb query fails."""
    message = DUCKDB_ENGINE_ERROR.format(error=error_message)
    return show_warning(message)


def show_dataframe_sample(dataframe):
    """Show dataframe output.

    Args:
        dataframe (dataframe): The dataframe to show.
    """
    return show_info_message(dataframe)
