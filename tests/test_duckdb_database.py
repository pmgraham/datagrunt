"""Unit tests for DuckDBDatabase."""

import os
import pytest
import duckdb

from src.datagrunt.core.databases import DuckDBDatabase

@pytest.fixture
def filepath():
    """Fixture to create a sample file path."""
    return 'path/to/--my_file.csv'

@pytest.fixture
def expected_db_filename():
    """Fixture for expected database filename."""
    return 'myfile.db'

@pytest.fixture
def expected_db_table():
    """Fixture for expected database table name."""
    return 'myfile'

@pytest.fixture
def db(filepath):
    """Fixture to create and teardown a DuckDBDatabase instance."""
    db_instance = DuckDBDatabase(filepath)
    yield db_instance
    db_filename = f'{filepath.replace("/", "_").replace(".", "_")}.db'
    if os.path.exists(db_filename):
        os.remove(db_filename)

def test_init(db, filepath, expected_db_filename, expected_db_table):
    """Test if the DuckDBDatabase class initializes correctly."""
    assert db.filepath == filepath
    assert db.database_filename == expected_db_filename
    assert db.database_table_name == expected_db_table
    assert isinstance(db.database_connection, duckdb.DuckDBPyConnection)

def test_del(filepath):
    """Test if the database file is deleted when the object is destroyed."""
    db = DuckDBDatabase(filepath)
    db_filename = db.database_filename
    assert os.path.exists(db_filename)
    del db
    assert not os.path.exists(db_filename)

def test_format_filename_string(db, expected_db_table):
    """Test if the filename is formatted correctly."""
    assert db._format_filename_string() == expected_db_table

def test_set_database_filename(db, expected_db_filename):
    """Test if the database filename is set correctly."""
    assert db._set_database_filename() == expected_db_filename

def test_set_database_table_name(db, expected_db_table):
    """Test if the database table name is set correctly."""
    assert db._set_database_table_name() == expected_db_table

def test_set_database_connection(db):
    """Test if the database connection is established correctly."""
    assert isinstance(db._set_database_connection(), duckdb.DuckDBPyConnection)
