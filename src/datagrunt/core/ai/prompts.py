"""This module contains prompts for the AI models."""

CSV_SCHEMA_SYSTEM_INSTRUCTIONS = """
You are a data engineering agent.
Your job is as follows:
    * evaluate data samples to identify columns
    * determine data types
    * classify dimensions from measures
    * determine if the data has a column header or not
    * determine if the data is structured or unstructured
    * determine if the data has a delimiter and to identify that delimiter.
Categorize these finding into their own section of the response format.
"""

CSV_SCHEMA_PROMPT = """
For the given data string identify the columns, return a list of columns,
the column data types, and whether or not they should be categorized as a
dimension or a measure. Also return a normalized version of the data column
name in all lower case separated by underscores. If the column name starts
with a number move the number to the end of the column name. Anything that
could be categorized as a special number could lose a leading zero if
converted from text to a numeric type, be sure in those cases to classify
the column as a dimension and make it a string type. Here are some examples
of special values that could be numeric typeable but should be classified as
dimensions and string types:

<special values>
    * ZIP Codes / Postal Codes
    * Product SKUs / Item Numbers
    * Employee IDs / Customer IDs / User IDs
    * Phone Numbers (if stored numerically)
    * Bank Account Numbers
    * Serial Numbers
    * Course Codes / Class IDs
    * Lot Numbers / Batch Numbers
    * Dates / Times (if represented as a single number without separators,
      e.g., MMDD)
    * Social Security Numbers (SSNs)
    * Transaction Codes
    * Any number with a leading zero in the original data

If unsure if a data value should be categorized as a dimension or measure,
for safety sake, categorize as a dimension and string type.
Return a map of original column names to normalized column names.

<example measures>
    * Sales Amount
    * Quantity Sold
    * Revenue
    * Profit Margin
    * Temperature
    * Distance

If `has_column_header` is false, then recommend column header names based on
the the data that are nicely formatted in lowercase and underscore separated.
After generating a list of recommended columns, add another element grouping
those recommendations into dimension and measures.
Be sure to reserve measures only for quantifiable values and not anything
that could be categorized as a `special value`.

Return a response format like this:

<response format>

{{
 "has_column_header": true or false ,
 "is_structured": true or false ,
 "has_delimiter": true or false,
 "delimiter": "delimiter",
 "is_tabular": true or false,
 "encoding": "utf-8", "latin1", "latin2", "utf-16", etc.,
 "total_column_count": 0,
 "total_dimension_column_count": 0,
 "total_measure_column_count": 0,
 "schema": [
  {{
   "name": "column name",
   "normalized_name": "normalized_column_name",
   "data_type": "data_type",
   "classification": "dimension or measure"
  }}
 ],
 "dimensions": ["colmn_name_1", "column_name_2", ...],
 "measures": ["column_name_1", "column_name_2", ...],
"columns_rename_map": {{
    "column_name": "normalized_column_name"
}}
}}

<data string>
```{csv_sample_string}```
"""

SUGGEST_DATA_TRANSFORMATIONS = """
Given this CSV data sample:

{csv_sample}

The user wants to: {user_goal}

Please suggest specific data transformations that would help achieve this goal.
Provide your suggestions as a numbered list of clear, actionable steps.
Focus on transformations that can be implemented using SQL or dataframe operations.
"""

GENERATE_SQL_QUERY = """
Given a table named '{table_name}' with the following schema:

{schema_description}

Generate a SQL query for this request: {natural_language_query}

Return ONLY the SQL query without any explanation or markdown formatting.
The query should be compatible with DuckDB syntax.
"""
