"""Class to represent and define CSV properties."""

# standard library

# third party libraries

# local libraries
from src.datagrunt.core.fileproperties import FileProperties
from src.datagrunt.core.csvcomponents import (
    CSVColumns,
    CSVDelimiter,
    CSVDialect,
    CSVRows
)

class CSVProperties(FileProperties):
    """Class to represent and define CSV properties."""

    def __init__(self, filepath):
        """Initialize the CSVProperties object."""
        super().__init__(filepath)
        self.filepath = filepath
        self.delimiter = CSVDelimiter(filepath).delimiter
        if not self.is_csv:
            raise ValueError(f"File extension '{self.extension_string}' is not a valid CSV file extension.")

    def _handle_empty_or_blank_file(self, property):
        """Handle empty or blank CSV file properties."""
        if self.is_empty or self.is_blank:
            return None
        return property

    @property
    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        return self._handle_empty_or_blank_file(CSVRows(self.filepath).row_count_with_header)

    @property
    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self._handle_empty_or_blank_file(CSVRows(self.filepath).row_count_without_header)

    @property
    def columns(self):
        """Return the columns in the CSV file."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns)

    @property
    def columns_normalized(self):
        """Return the normalized column names in the CSV file."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_normalized)

    @property
    def columns_string(self):
        """Return the first row of a CSV file as a string."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_string)

    @property
    def columns_normalized_string(self):
        """Return the normalized columns in the CSV file as a string."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_normalized_string)

    @property
    def columns_normalized_list(self):
        """Return the normalized columns in the CSV file as a list."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_normalized)

    @property
    def columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_byte_string)

    @property
    def columns_normalized_byte_string(self):
        """Return the normalized first row in the CSV file as bytes."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_normalized_byte_string)

    @property
    def columns_to_normalized_mapping(self):
        """Return the mapping of original column names to normalized column names."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_to_normalized_mapping)

    @property
    def columns_count(self):
        """Return the number of columns in the CSV file."""
        return self._handle_empty_or_blank_file(CSVColumns(self.filepath).columns_count)

    @property
    def quotechar(self):
        """Return the quote character used in the CSV file."""
        return self._handle_empty_or_blank_file(CSVDialect(self.filepath).quotechar)

    @property
    def escapechar(self):
        """Return the escape character used in the CSV file."""
        return self._handle_empty_or_blank_file(CSVDialect(self.filepath).escapechar)

    @property
    def newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self._handle_empty_or_blank_file(CSVDialect(self.filepath).newline_delimiter)


# class CSVProperties(FileProperties):
#     """Class for parsing CSV files. Mostly determining the delimiter."""

#     DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z_ "-]'
#     DEFAULT_DELIMITER = ','
#     DEFAULT_SAMPLE_ROWS = 1
#     CSV_SNIFF_SAMPLE_ROWS = 5
#     DATAFRAME_SAMPLE_ROWS = 20

#     QUOTING_MAP = {
#         0: 'no quoting',
#         1: 'quote all',
#         2: 'quote minimal',
#         3: 'quote non-numeric'
#     }

#     def __init__(self, filepath):
#         """
#         Initialize the CSVParser class.

#         Args:
#             filepath (str): Path to the file to read.
#         """
#         super().__init__(filepath)
#         self.first_row = self._get_first_row_from_file()
#         self.delimiter = self._infer_csv_file_delimiter()
#         self.formatter = CSVColumnNameNormalizer(filepath)
#         if not self.is_csv:
#             raise ValueError(
#                 f"File extension '{self.extension_string}' is not a valid CSV file extension."
#                              )

#     def _return_empty_file_attributes(self):
#         """Return an empty file object."""
#         return {
#             'delimiter': self.DEFAULT_DELIMITER,
#             'quotechar': '"',
#             'escapechar': None,
#             'doublequote': True,
#             'newline_delimiter': '\n',
#             'skipinitialspace': False,
#             'quoting': 'quote all',
#             'columns_schema': {},
#             'columns_original_format': '',
#             'columns_list': [],
#             'columns_string': '',
#             'columns_byte_string': b'',
#             'column_count': 0
#         }

#     def _get_first_row_from_file(self):
#         """Reads and returns the first line of a file.

#         Args:
#             filename: The path to the file.

#         Returns:
#             The first line of the file, stripped of leading/trailing whitespace,
#             or None if the file is empty.
#         """
#         with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
#             first_line = csv_file.readline().strip()
#         return first_line

#     def _get_most_common_non_alpha_numeric_character_from_string(self):
#         """Get the most common non-alpha-numeric character from a given string.

#         Args:
#             text (str): The string to get the most common non-alpha-numeric character from.

#         Returns:
#             str: The most common non-alpha-numeric character from the string.
#         """
#         columns_no_spaces = self.first_row.replace(' ', '')
#         regex = re.compile(self.DELIMITER_REGEX_PATTERN)
#         counts = Counter(char for char in regex.findall(columns_no_spaces))
#         most_common = counts.most_common()
#         return most_common

#     def _infer_csv_file_delimiter(self):
#         """Infer the delimiter of a CSV file.

#         Args:
#             csv_file (str): The path to the CSV file.

#         Returns:
#             str: The delimiter of the CSV file.
#         """
#         delimiter_candidates = self._get_most_common_non_alpha_numeric_character_from_string()

#         if self.is_empty or self.is_blank:
#             delimiter = self.DEFAULT_DELIMITER
#         elif len(delimiter_candidates) == 0:
#             delimiter = ' '
#         else:
#             delimiter = delimiter_candidates[0][0]
#         return delimiter

#     def _get_attributes(self):
#         """Generate a dictionary of CSV attributes."""
#         if self.is_empty or self.is_blank:
#             attributes = self._return_empty_file_attributes()
#         else:
#             columns_list = self.first_row.split(self.delimiter)
#             normalized_columns_list = self.formatter.normalize_column_names(columns_list)
#             columns = {c: 'VARCHAR' for c in normalized_columns_list}
#             with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csvfile:
#                 # Sniff the file to detect parameters
#                 dialect = csv.Sniffer().sniff(csvfile.read(self.CSV_SNIFF_SAMPLE_ROWS))
#                 csvfile.seek(0)  # Reset file pointer to the beginning
#                 attributes = {
#                         'delimiter': self.delimiter,
#                         'quotechar': dialect.quotechar,
#                         'escapechar': dialect.escapechar,
#                         'doublequote': dialect.doublequote,
#                         'newline_delimiter': dialect.lineterminator,
#                         'skipinitialspace': dialect.skipinitialspace,
#                         'quoting': self.QUOTING_MAP.get(dialect.quoting),
#                         'columns_schema': columns,
#                         'columns_original_format': self.first_row,
#                         'columns_list': columns_list,
#                         'columns_list_normalized': normalized_columns_list,
#                         'columns_list_normalized_string': ", ".join(normalized_columns_list),
#                         'columns_string': ", ".join(columns_list),
#                         'columns_byte_string': ", ".join(columns_list).encode(),
#                         'columns_list_normalized_byte_string': ", ".join(normalized_columns_list).encode(),
#                         'column_count': len(columns_list)
#                     }

#         return attributes

#     @property
#     @lru_cache()
#     def row_count_with_header(self):
#         """Return the number of lines in the CSV file including the header."""
#         with open(self.filepath, 'rb') as csv_file:
#             return sum(1 for _ in csv_file)

#     @property
#     def row_count_without_header(self):
#         """Return the number of lines in the CSV file excluding the header."""
#         return self.row_count_with_header - 1

#     @property
#     def columns(self):
#         """Return the columns in the CSV file."""
#         return self._get_attributes()['columns_list']

#     @property
#     def columns_normalized(self):
#         """Return the normalized column names in the CSV file."""
#         return self._get_attributes()['columns_list_normalized']

#     @property
#     def columns_string(self):
#         """Return the first row of a CSV file as a string."""
#         return self._get_attributes()['columns_string']

#     @property
#     def columns_normalized_string(self):
#         """Return the normalized columns in the CSV file as a string."""
#         return self._get_attributes()['columns_list_normalized_string']

#     @property
#     def columns_byte_string(self):
#         """Return the first row of the CSV file as bytes."""
#         return self._get_attributes()['columns_byte_string']

#     @property
#     def columns_normalized_byte_string(self):
#         """Return the normalized first row in the CSV file as bytes."""
#         return self._get_attributes()['columns_list_normalized_byte_string']

#     @property
#     def columns_to_normalized_mapping(self):
#         """Return the mapping of original column names to normalized column names."""
#         return dict(OrderedDict(zip(self.columns, self.columns_normalized)))

#     @property
#     def column_count(self):
#         """Return the number of columns in the CSV file."""
#         return self._get_attributes()['column_count']

#     @property
#     def quotechar(self):
#         """Return the quote character used in the CSV file."""
#         return self._get_attributes()['quotechar']

#     @property
#     def escapechar(self):
#         """Return the escape character used in the CSV file."""
#         return self._get_attributes()['escapechar']

#     @property
#     def newline_delimiter(self):
#         """Return the newline delimiter used in the CSV file."""
#         return self._get_attributes()['newline_delimiter']
