"""Module to handle data wrangling tasks."""

# standard library
import csv
import os
from pathlib import Path

# third party libraries
import pandas as pd

# local modules


class FileAttributes:
    """Class to get file attributes."""

    def __init__(self, file_path):
        """
        Initialize the FileAttributes class.
        
        Args:
            file_path (str): Path to the file to read.
        """
        self.path = file_path
        self.base_filename = Path(file_path).stem
        self.extension = Path(file_path).suffix
        self.name = Path(file_path).name
        self.size_in_bytes = os.path.getsize(file_path)
        self.size_in_kb = self.size_in_bytes / 1024
        self.size_in_mb = self.size_in_kb / 1024
        self.size_in_gb = self.size_in_mb / 1024
        self.size_in_tb = self.size_in_gb / 1024
        self.modified = os.path.getmtime(file_path)
        self.accessed = os.path.getatime(file_path)
        self.created = os.path.getctime(file_path)
        self.is_directory = os.path.isdir(file_path)
        self.is_file = os.path.isfile(file_path)

    def __str__(self):
        """Return a string representation of the FileAttributes object."""
        return f"File: {self.name}\n"
    
    def is_google_cloud_storage_file(self):
        """Check if the file is a Google Cloud Storage file."""
        return self.name.startswith("gs://")

    def is_local_file(self):
        """Check if the file is a local file."""
        return self.name.startswith("/")

    def is_remote_file(self):
        """Check if the file is a remote file."""
        return not self.is_local_file() and not self.is_google_cloud_storage_file()

    def is_empty(self):
        """Check if the file is empty."""
        return self.size_in_bytes == 0

    def is_large(self):
        """Check if the file is large."""
        return self.size_in_gb > 1

class CSVAttributes:
    """Class to get CSV file attributes."""

    VALID_FILE_TYPES = ['.csv']

    def __init__(self, file_path):
        """
        Initialize the CSVAttributes class.
        
        Args:
            file_path (str): Path to the CSV file to read.
        """

        if not file_path.lower() not in self.VALID_FILE_TYPES:
            raise ValueError("Invalid file type. Please provide a CSV file.")
        
        if FileAttributes(file_path).is_empty():
            raise ValueError("CSV file is empty. Cannot obtain attributes from an empty file.")

        self.path = file_path
        self.attributes = FileAttributes(file_path)
    
    def __str__(self):
        """Return a string representation of the CSVAttributes object."""
        return f"CSV File: {self.attributes.name}\n"
    
    def columns(self):
        """Return the first row of the CSV file."""
        with open(self.path, 'r') as file:
            return file.readline().strip()

    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.path, 'r') as file:
            return sum(1 for _ in file)
    
    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        with open(self.path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            return sum(1 for _ in reader) 

    # def column_count(self):
    #     """Return the number of columns in the CSV file."""
    #     with open(self.path, 'r') as file:
    #         reader = csv.reader(file)
    #         return len(next(reader))
    
class ExcelAttributes:
    """Class to get Excel file attributes."""

    VALID_FILE_TYPES = [
        '.xlsx',
        '.xlsm',
        '.xlsb',
        '.xltx',
        '.xltm',
        '.xls',
        '.xlt',
        '.xls'
    ]

    def __init__(self, file_path):
        """
        Initialize the ExcelAttributes class.
        
        Args:
            file_path (str): Path to the Excel file to read.
        """

        if not file_path.lower() not in self.VALID_FILE_TYPES:
            raise ValueError("Invalid file type. Please provide a CSV file.")
        
        if FileAttributes(file_path).is_empty():
            raise ValueError("CSV file is empty. Cannot obtain attributes from an empty file.")

        self.path = file_path
        self.attributes = FileAttributes(file_path)

    @property
    def sheet_names(self):
        with pd.ExcelFile(self.file_path) as xls:
            sheet_names = xls.sheet_names
        return sheet_names
    
    def __str__(self):
        """Return a string representation of the ExcelAttributes object."""
        return f"Excel File: {self.attributes.name}\n"

    def columns(self):
        """Return the first row of the Excel file."""
        df = pd.read_excel(self.path)
        return df.columns.tolist()
    
    def row_count_with_header(self, sheet_name):
        """Return the number of lines in the Excel file including the header."""
        with pd.ExcelFile(self.path) as xls:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            total_rows = len(df)
            return total_rows
    
    def row_count_without_header(self, sheet_name):
        """Return the number of lines in the Excel file excluding the header."""
        with pd.ExcelFile(self.path) as xls:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            total_rows = len(df) - 1
            return total_rows

    def column_count(self):
        df = pd.read_excel(self.path)
        return len(df.columns)


csv = CSVAttributes('test.csv')

print(csv.columns())
# print(csv.row_count_with_header())
# print(csv.row_count_without_header())
# print(csv.column_count())