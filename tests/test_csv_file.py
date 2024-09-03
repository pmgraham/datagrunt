"""Unit tests for datagrunt."""

# standard library
import os
from pathlib import Path
import sys
import unittest
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../datagrunt')  # Add the parent directory to the search path

# third party libraries
import duckdb
import polars as pl

# local libraries
from datagrunt.csvfile import *
from datagrunt.core.queries import *
from datagrunt.core.logger import *
from datagrunt.core.fileproperties import *




if __name__ == '__main__':
    unittest.main()
