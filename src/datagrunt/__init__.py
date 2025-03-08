"""
Datagrunt

A Python library designed to simplify the way you work with CSV files.

This module provides inferred CSV delimiters and helper methods for reading and writing CSV files.

Example:
    A simple example of how to use the main functionality of your package:

    from datagrunt.csvfile import CSVReader

    csv_file = 'electric_vehicle_population_data.csv'
    engine = 'duckdb'

    dg = CSVReader(csv_file, engine=engine)

    dg.get_sample()

Attributes:
    __version__: A string representing the version of this module.
    __author__: The name of the package author.
    __license__: The license under which the package is released.
"""

__version__ = "1.0.4"
__author__ = "Martin Graham"
__license__ = "MIT"

# Import key classes, functions, or submodules that should be available at the package level
from datagrunt.csvfiles import CSVReader, CSVWriter


# You can define __all__ to specify what gets imported with "from package import *"
__all__ = ['CSVReader', 'CSVWriter']

# Optionally, you can include a logger for your package
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
