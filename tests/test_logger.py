import pytest
import logging
from src.datagrunt.core.logger import *

# @pytest.fixture
# def caplog(caplog):
#     """Fixture to capture log messages."""
#     caplog.set_level(logging.INFO)
#     return caplog

# def test_show_warning(caplog):
#     """Test the show_warning function."""
#     message = "Test warning message"
#     show_warning(message)
#     assert logging.warning(message) in caplog.text

# def test_show_info_message(caplog):
#     """Test the show_info_message function."""
#     message = "Test info message"
#     show_info_message(message)
#     assert "INFO - Test info message" in caplog.text

# def test_show_large_file_warning(caplog):
#     """Test the show_large_file_warning function."""
#     show_large_file_warning()
#     assert f"WARNING - {LARGE_FILE_WARNING}" in caplog.text

# def test_duckdb_query_error(caplog):
#     """Test the duckdb_query_error function."""
#     error_message = "Test error"
#     duckdb_query_error(error_message)
#     expected_message = DUCKDB_ENGINE_ERROR.format(error=error_message)
#     assert f"WARNING - {expected_message}" in caplog.text

# def test_show_dataframe_sample(caplog):
#     """Test the show_dataframe_sample function."""
#     dataframe = "Sample dataframe"
#     show_dataframe_sample(dataframe)
#     assert "INFO - Sample dataframe" in caplog.text

# def test_logging_levels():
#     """Test that logging levels are set correctly."""
#     with pytest.raises(AttributeError):
#         # This should raise an AttributeError because no handlers are set yet
#         logging.getLogger().handlers[0].level

#     show_warning("Test")
#     assert logging.getLogger().handlers[0].level == logging.WARNING

#     show_info_message("Test")
#     assert logging.getLogger().handlers[0].level == logging.INFO
