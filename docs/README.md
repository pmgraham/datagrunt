# Welcome To Datagrunt
Datagrunt is a Python library designed to simplify the way you work with CSV files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why Datagrunt?
Born out of real-world frustration, Datagrunt eliminates the need for repetitive coding when handling CSV files. Whether you're a data analyst, data engineer, or data scientist, Datagrunt empowers you to focus on insights, not tedious data wrangling.

### What Datagrunt Is Not
Datagrunt is not an extension of or a replacement for DuckDB or Polars, nor a comprehensive data processing solution. It is not designed to be a comprehensive one stop shop for all of your CSV processing needs. It's designed to simplify the way you
work with CSV files and to help solve the pain point of inferring delimiters when a file structure is unknown. We grant an easy way to convert CSV files to dataframes and to export them to various formats. One of Datagrunt's value propositions is its relative
simplicity and ease of use. We will extend functionaltiy where it makes sense to do so, but we will be selective and strategic in our approach to add or extend functionality.

### Key Features

- **Intelligent Delimiter Inference:**  Datagrunt automatically detects and applies the correct delimiter for your CSV files.
- **Seamless Data Processing:** Leverage the robust capabilities of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) to perform advanced data processing tasks directly on your CSV data.
- **Flexible Transformation:** Easily convert your processed CSV data into various formats to suit your needs.
- **AI-Powered Schema Analysis:** Use Google's Gemini models to automatically generate detailed schema reports for your CSV files, including data types, column classifications, and data quality checks.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with a simple Python API |
| [Polars](https://pola.rs) | Multi-threaded query engine written in Rust, optimized for modern processors |
| [Google Gemini](https://deepmind.google/technologies/gemini/) | A powerful family of generative AI models. |

### Datagrunt's Role

Datagrunt is not an extension of DuckDB or Polars, nor a comprehensive data processing solution. Its primary functions are:

1. Accurately inferring CSV delimiters
2. Providing helper methods for common data tasks
3. Facilitating CSV file loading into Polars dataframes
4. Enabling conversion to various output formats
5. Generating AI-powered schema reports

### Flexibility and Integration

While Datagrunt uses Polars dataframes by default, it doesn't limit your options:

- Convert to Pandas dataframes using the `to_pandas` method
- Use Datagrunt's output in other contexts within your script or application
- Leverage only specific features (e.g., delimiter inference) as needed
- Datagrunt complements your existing data processing toolkit, offering helpful utilities without restricting your workflow choices.

# Installation

Get started with Datagrunt in seconds using pip:

```bash
pip install datagrunt
```

# How to Use Datagrunt
Even though Datagrunt is a fairly simple library, it may not be obvious where to start. Here's a quick guide to help you naviate Datagrunt.

## Datagrunt Engines
Datagrunt provides two engines for working with CSV files: DuckDB and Polars. When instantiating the `CSVReader` or the `CSVWriter` class, you can specify which engine to use. The default engine for `CSVReader` is `polars`, while the default engine for `CSVWriter` is `duckdb`.
The reason `polars` is the default engine for `CSVReader` is because it is a powerful and fast dataframe library that is well-suited for working with CSV files. When reading CSV files, it's a common pattern to use Dataframes to process the data.
Once the data is in a dataframe, you can leverage the powerful data manipulation capabilities of a dataframe library such as [Pandas](https://pandas.pydata.org) or [Polars](https://pola.rs). Also, in early testing, we found that `polars` is faster than `duckdb` for certain operations when reading CSV files.

Conversely, `duckdb` is the default engine for `CSVWriter` because it is a powerful and fast in process OLAPSQL database that is well-suited for working with CSV files. Once the data is in a SQL database, you can leverage the powerful data manipulation capabilities of [DuckDB](https://duckdb.org).
Also, in early testing, we found that `duckdb` is faster than `polars` for certain operations when writing CSV files.
The other reason that `duckdb` is the default engine for `CSVWriter` is because when writing data to JSON format in particular, we found that `duckdb` was not only faster than `polars`, but also wrote the data with better formatting and was less error prone with larger
sets of data. When writing JSON data to a file using `duckdb`, the file was structured correctly and had consistent formatting. Sometimes when writing JSON data to a file using `polars`, the file was not structured correctly and had inconsistent formatting, causing
downstream issues when reading the output.

## Artificial Intelligence Features
As of Datagrunt version 2.0.1 integration with Large Language Models (LLMs) is available. Currently only Google Gemini is available. We plan to add more LLMs in the future.

### Artificial Intelligence (AI) Engines
As of Datagrunt version 2.0.1 we introduced a factory pattern to support multiple LLM providers. Currently, the only engine available is Google Gemini. In order to access Gemini, you need either a Gemini API key or you need to be authenticated with a Google Cloud account so that you can use Vertex AI. Both are supported in the same interface depending on the set of paramaters you pass into the `CSVSchemaReportAIGenerated` class.

### Google Gemini
Currently there is only one class that supports integration with Google Gemini: `CSVSchemaReportAIGenerated`. It is exposed as part of the facade pattern along with the `CSVReader` and `CSVWriter` classes. See below under the `Primary Classes` section for more details.

## A Word About Pandas
Pandas is a powerful data manipulation library that is widely used in the data science community. It provides a wide range of tools for data cleaning, transformation, and analysis.
However, when working with large datasets, Pandas can be slow and memory-intensive. In contrast, DuckDB and Polars are designed to handle large datasets efficiently and are optimized for performance. In fact, when testing with large datasets, we found both DuckDB and Polars to be orders of magnitude faster than Pandas.
With that said, Pandas is still a valuable tool for data manipulation and analysis and it is not our goal to replace it nor limit its usage. We encourage users to continue using Pandas for their data manipulation needs whenever it best suits their needs.

Any Polars dataframe object can be easily converted to a Pandas dataframe using the `to_pandas()` method. This allows users to leverage the power of both libraries for their data manipulation needs. Likewise, if you have a Pandas dataframe object, you can convert it to a Polars dataframe
by instantiating a new Polars dataframe object from the Pandas dataframe as follows:

```python
import polars as pl
import pandas as pd

# Convert a Pandas dataframe to a Polars dataframe
df_pandas = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
df_polars = pl.DataFrame(df_pandas)
```
To convert a Polars dataframe to a Pandas dataframe, use the `to_pandas()` method:

```python
import polars as pl

df = pl.read_csv(csv_file, separator=',').to_pandas() # note you are required to have Pandas installed even if it's not imported
```

## Usage Examples
### Reading and Querying CSV Data
```python
from datagrunt import CSVReader

# Load your CSV file
csv_file = 'examples/data/electric_vehicle_population_data.csv'
engine = 'duckdb'

# Set duckdb as the processing engine. Engine set to 'polars' by default
reader = CSVReader(csv_file, engine=engine)

# return sample of the data to get a peek at the schema
reader.get_sample()
┌────────────┬───────────┬──────────────┬───┬──────────────────────┬──────────────────────┬───────────────────┐
│ VIN (1-10) │  County   │     City     │ … │   Vehicle Location   │   Electric Utility   │ 2020 Census Tract │
│  varchar   │  varchar  │   varchar    │   │       varchar        │       varchar        │      varchar      │
├────────────┼───────────┼──────────────┼───┼──────────────────────┼──────────────────────┼───────────────────┤
│ 5YJSA1E28K │ Snohomish │ Mukilteo     │ … │ POINT (-122.29943 …  │ PUGET SOUND ENERGY…  │ 53061042001       │
│ 1C4JJXP68P │ Yakima    │ Yakima       │ … │ POINT (-120.468875…  │ PACIFICORP           │ 53077001601       │
│ WBY8P6C05L │ Kitsap    │ Kingston     │ … │ POINT (-122.517835…  │ PUGET SOUND ENERGY…  │ 53035090102       │
│ JTDKARFP1J │ Kitsap    │ Port Orchard │ … │ POINT (-122.653005…  │ PUGET SOUND ENERGY…  │ 53035092802       │
│ 5UXTA6C09N │ Snohomish │ Everett      │ … │ POINT (-122.203234…  │ PUGET SOUND ENERGY…  │ 53061041605       │
│ 5YJYGDEF8L │ King      │ Seattle      │ … │ POINT (-122.378886…  │ CITY OF SEATTLE - …  │ 53033004703       │
│ JTMAB3FV7P │ Thurston  │ Rainier      │ … │ POINT (-122.677141…  │ PUGET SOUND ENERGY…  │ 53067012530       │
│ JN1AZ0CPXC │ King      │ Kirkland     │ … │ POINT (-122.192596…  │ PUGET SOUND ENERGY…  │ 53033022402       │
│ JN1AZ0CP7B │ King      │ Kirkland     │ … │ POINT (-122.192596…  │ PUGET SOUND ENERGY…  │ 53033022603       │
│ 1N4AZ0CP0F │ Thurston  │ Olympia      │ … │ POINT (-122.86491 …  │ PUGET SOUND ENERGY…  │ 53067010300       │
│     ·      │   ·       │    ·         │ · │          ·           │          ·           │      ·            │
│     ·      ���   ·       │    ·         │ · │          ·           │          ·           │      ·            │
│     ·      │   ·       │    ·         │ · │          ·           │          ·           │      ·            │
│ 5YJYGDEE7M │ Clark     │ Vancouver    │ … │ POINT (-122.515805…  │ BONNEVILLE POWER A…  │ 53011041310       │
│ 7SAYGAEE0P │ Snohomish │ Monroe       │ … │ POINT (-121.968385…  │ PUGET SOUND ENERGY…  │ 53061052203       │
│ 2C4RC1N75P │ King      │ Burien       │ … │ POINT (-122.347227…  │ CITY OF SEATTLE - …  │ 53033027600       │
│ 1FTVW1EVXP │ King      │ Kirkland     │ … │ POINT (-122.202653…  │ PUGET SOUND ENERGY…  │ 53033022300       │
│ 4JGGM1CB2P │ King      │ Seattle      │ … │ POINT (-122.2453 4…  │ CITY OF SEATTLE - …  │ 53033011700       │
│ 1N4BZ0CP0G │ King      │ Seattle      │ … │ POINT (-122.334079…  │ CITY OF SEATTLE - …  │ 53033008300       │
│ 7SAYGDEF2N │ King      │ Bellevue     │ … │ POINT (-122.144149…  │ PUGET SOUND ENERGY…  │ 53033024704       │
│ 1N4BZ1DP7L │ King      │ Bellevue     │ … │ POINT (-122.144149…  │ PUGET SOUND ENERGY…  │ 53033024902       │
...
├───────────��┴───────────┴──────────────┴───┴──────────────────────┴──────────────────────┴───────────────────┤
│ ? rows (>9999 rows, 20 shown)                                                          17 columns (6 shown) │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

# Construct your SQL query. The `db_table` property is provided by the CSVReader class automatically.
query = f"""
WITH core AS (
    SELECT
        City AS city,
        "VIN (1-10)" AS vin
    FROM {reader.db_table}
)
SELECT
    city,
    COUNT(vin) AS vehicle_count
FROM core
GROUP BY 1
ORDER BY 2 DESC
"""

# Execute the query and get results as a Polars DataFrame
df = reader.query_data(query).pl() # the .pl() method is used to convert the results from a DuckDBPyRelation object to a Polars DataFrame
print(df)
┌────────────────┬───────────────┐
│ city           ┆ vehicle_count │
│ ---            ┆ ---           │
│ str            ┆ i64           │
╞════════════════╪═══════════════╡
│ Seattle        ┆ 32602         │
│ Bellevue       ┆ 9960          │
│ Redmond        ┆ 7165          │
│ Vancouver      ┆ 7081          │
│ Bothell        ┆ 6602          │
│ …              ┆ …             │
│ Glenwood       ┆ 1             │
│ Walla Walla Co ┆ 1             │
│ Pittsburg      ┆ 1             │
│ Decatur        ┆ 1             │
│ Redwood City   ┆ 1             │
└────────────────┴───────────────┘
```

### Generating an AI-Powered Schema Report
```python
from datagrunt import CSVSchemaReportAIGenerated
import os

# Load your CSV file
csv_file = 'examples/data/electric_vehicle_population_data.csv'

# Make sure to set your API Key as an environment variable
api_key = os.environ.get("GEMINI_API_KEY")

# Instantiate the report generator with an API key
report_generator = CSVSchemaReportAIGenerated(
    filepath=csv_file,
    engine='google',
    api_key=api_key
)

# Optionally instantiate the report generator with Vertex AI features
report_generator = CSVSchemaReportAIGenerated(
    filepath=csv_file,
    engine='google',
    vertexai=True,
    gcp_project='my-gcp-project-id',
    gcp_location='global'
)

# Generate the report using a powerful model
# Note: You must have access to the model you specify.
schema_report = report_generator.generate_csv_schema_report(
    model='gemini-2.5-flash',
    return_json=True
)

print(schema_report)
```
This will produce a detailed JSON report analyzing the CSV's schema, data types, and column classifications.

### Combine Datagrunt With Other Libraries

Datagrunt can be combined with other libraries. For example, you could use Datagrunt to instantiate the `CSVReader` class, and then use the provided `delimiter` attribute with other libraries.

```python
from datagrunt import CSVReader
import pandas as pd

reader = CSVReader('path/to/file.csv')
df = pd.read_csv(reader.filepath, sep=reader.delimiter) # filepath and delimiter are attributes of the CSVReader class.
```

### Reassign the Delimiter

Sometimes, the delimiter may not be correctly identified by Datagrunt. In such cases, you can reassign the delimiter attribute to correct it.
```python
from datagrunt import CSVReader
reader = CSVReader('path/to/file.csv')

# let's assume the delimiter is wrong and was inferred incorrectly as a space. Reassign the delimiter to correct it.
reader.delimiter = ','
df = pd.read_csv(reader.filepath, sep=reader.delimiter)
```

By updating the delimiter attribute, you can ensure the `CSVReader` object will read the file correctly if you choose to use any of its methods down the line.

## Primary Classes
Datagrunt provides three primary classes for interacting with data: `CSVReader`, `CSVWriter`, and `CSVSchemaReportAIGenerated`. These classes are designed to simplify the process of reading, writing, and analyzing CSV files.

### CSVReader
The `CSVReader` class is used to read data from a CSV file. It provides a simple interface for reading data from a CSV file and converting it into a DataFrame. You instantiate the `CSVReader` class as follows:
```python
from datagrunt import CSVReader
reader = CSVReader('path/to/file.csv')
```

You may optionally specify the engine to use for reading the CSV file. The two options are `duckdb` and `polars`. The default engine is `polars`.
```python
reader = CSVReader('path/to/file.csv', engine='duckdb') # don't pass any engine if you want to use the default engine.
```

The primary methods of the `CSVReader` class are:
- `get_sample(normalize_columns=False)`: Returns a sample of the data in the CSV file (20 rows).
- `to_dataframe(normalize_columns=False)`: Converts the data in the CSV file into a Polars DataFrame.
- `to_arrow_table(normalize_columns=False)`: Converts the data in the CSV file into a PyArrow Table.
- `to_dicts(normalize_columns=False)`: Converts the data in the CSV file into a list of dictionaries.
- `query_data(sql_query, normalize_columns=False)`: Executes a SQL query on the data in the CSV file.

### CSVWriter
The `CSVWriter` class is used to write data to a CSV file. It provides a simple interface for writing data to a CSV.
The primary methods of the `CSVWriter` class are:
- `write_csv(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a CSV file.
- `write_excel(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to an Excel file.
- `write_json(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a JSON file.
- `write_json_newline_delimited(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a JSON file with newline delimiters.
- `write_parquet(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a Parquet file.

### CSVSchemaReportAIGenerated
The `CSVSchemaReportAIGenerated` class is used to generate a report on the schema of a CSV file. It provides a simple interface for generating a report on the schema of a CSV file.
It is currently configured to run only with Google's Gemini and takes either an `api_key` or can access Vertex AI if you pass in the following paramaters:
- `vertexai=True`
- `gcp_project=my-gcp-project-id`
- `gcp_location=global` or a supported Google Cloud region such as `us-central1`

The primary methods of the `CSVSchemaReportAIGenerated` class are:
- `generate_csv_schema_report(self, model, prompt=None, system_instructions=None, return_json=False)`: Generates a report on the schema of a CSV file.
    - model: any supported Gemini model. We did not set a default model by design. Here is a link to the available Google Gemini models: [Gemini Models](https://cloud.google.com/vertex-ai/docs/generative-ai/models)
    - prompt: optional system prompt. A default prompt is utilized if no prompt is passed in.
    - system_instructions: optional system instructions. Default system instructions are utilized if no system instructions are passed in.
    - return_json: set to `False` by default and returns a Python dict. Set to `True` in order to have an indented JSON response returned.

Here is a list of optional keyword params you may pass in along with their default values:
- vertexai=False,
- gcp_project=None,
- gcp_location=None,
- max_tokens=8192,
- temperature=0.5,
- top_p=1,
- seed=0,
- safety_settings=None, # I'll list the default safety settings below.
- thinking_budget=-1, # set to automatic mode
- response_type="application/json",
- ground_google_search=False

There is a default prompt that is built into Datagrunt that enables this method to operate. You may optionally pass in your own prompt if you wish. If you do not pass in a prompt, Datagrunt will use the default system prompt. Otherwise, it will use the prompt you pass in.

### Generative AI Default Prompt and System Instructions
Below is the default prompt and system instructions that are built into Datagrunt. Both the system instructions and the prompt are used by default to generate the report. It's very important to note that in the prompt below the JSON schema is defined so that the LLM responds with a consistent output every time. This defined schema, in conjunction with setting the MIME type output to `application/json` ensures proper JSON formatting.

If you pass in your own prompt without defining a schema, or if you pass in your own prompt with a different schema, just be aware you are responsible for the output and for controlling its formatting.

```python
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
For the given data string identify the columns, return a list of columns, the column data types, and whether or not they should be categorized as a dimension or a measure.
Also return a normalized version of the data column name in all lower case separated by underscores. If the column name starts with a number move the number to the end of the column name.
Anything that could be categorized as a special number could lose a leading zero if converted from text to a numeric type, be sure in those cases to classify the column as a dimension and make it a string type.
Here are some examples of special values that could be numeric typeable but should be classified as dimensions and string types:

<special values>
    * ZIP Codes / Postal Codes
    * Product SKUs / Item Numbers
    * Employee IDs / Customer IDs / User IDs
    * Phone Numbers (if stored numerically)
    * Bank Account Numbers
    * Serial Numbers
    * Course Codes / Class IDs
    * Lot Numbers / Batch Numbers
    * Dates / Times (if represented as a single number without separators, e.g., MMDD)
    * Social Security Numbers (SSNs)
    * Transaction Codes
    * Any number with a leading zero in the original data

If unsure if a data value should be categorized as a dimension or measure, for safety sake, categorize as a dimension and string type.
Return a map of original column names to normalized column names.

<example measures>
    * Sales Amount
    * Quantity Sold
    * Revenue
    * Profit Margin
    * Temperature
    * Distance

If `has_column_header` is false, then recommend column header names based on the the data that are nicely formatted in lowercase and underscore separated.
After generating a list of recommended columns, add another element grouping those recommendations into dimension and measures.
Be sure to reserve measures only for quantifiable values and not anything that could be categorized as a `special value`.

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

```

Here are the default safety settings. You may pass in your own list but these are set by default in Datagrunt:

```python
[
    types.SafetySetting(
        category="HARM_CATEGORY_HATE_SPEECH",
        threshold="OFF"
    ),
    types.SafetySetting(
        category="HARM_CATEGORY_DANGEROUS_CONTENT",
        threshold="OFF"
    ),
    types.SafetySetting(
        category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
        threshold="OFF"
    ),
    types.SafetySetting(
        category="HARM_CATEGORY_HARASSMENT",
        threshold="OFF"
    )
]
```
#### Grounding in Google Search
Grounding in Google Search will be supported in the future. The implementation is already built into the AI Engines pattern, but the only class that utilizes AI right now does not allow for grounding in Google Search. Again, future implementations will utilize this feature.

### No AI Agents At This Time
The current implementation leveraging a LLM to evaluate a CSV file is a simple API call to the LLM provider (currently Google Gemini). To be clear this is not an AI agent nor this is an agentic component of Datagrunt. Again, this is a simple API call to Gemini.

AI Agents may be added in the future but that is currently being debated among the maintainers of Datagrunt. We will post more details on this decision in the future.

## File and CSV Attributes
Exposed in both the `CSVReader` and `CSVWriter` classes are a number of attributes that allow you to access and manipulate file and CSV-specific information:

**File Attributes:**
- `filepath`: The absolute path to the file.
- `filename`: The name of the file.
- `extension`: The file extension (e.g., `.csv`).
- `size_in_bytes`, `size_in_kb`, `size_in_mb`, `size_in_gb`, `size_in_tb`: File size in various units.
- `modified_time`: The last modification time of the file.
- `encoding`: The character encoding of the file (defaults to 'utf-8').

**File Type Booleans:**
- `is_structured`, `is_semi_structured`, `is_unstructured`: Checks for data structure type.
- `is_standard`, `is_proprietary`: Checks for standard vs. proprietary file formats.
- `is_csv`, `is_excel`, `is_tsv`, `is_apache`, `is_tabular`: Checks for specific file types.
- `is_empty`, `is_blank`: Checks if the file is empty or contains only whitespace.
- `is_large`: Checks if the file is 1GB or larger.

**CSV-Specific Attributes:**
- `delimiter`: The inferred delimiter character.
- `quotechar`: The character used for quoting fields.
- `escapechar`: The character used for escaping.
- `doublequote`: Boolean indicating if the quote character is doubled to be escaped.
- `newline_delimiter`: The line terminator sequence.
- `skipinitialspace`: Boolean, true if whitespace immediately following a delimiter is ignored.
- `quoting`: The quoting style used.
- `row_count_with_header`, `row_count_without_header`: The number of rows.
- `columns`, `columns_normalized`: Lists of original and normalized column names.
- `columns_count`: The number of columns.
- `columns_to_normalized_mapping`: A dictionary mapping original names to normalized names.
- `csv_string_sample`, `csv_string_sample_by_quality`: String samples of the CSV data.

# Known Issues
Please report any bugs or issues on the [Github Issue Tracker](https://github.com/pmgraham/datagrunt/issues).

# License
This project is licensed under the [MIT License](https://opensource.org/license/mit)

# Acknowledgements
A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) for their fantastic libraries that power Datagrunt.

# Source Repository
[https://github.com/pmgraham/datagrunt](https://github.com/pmgraham/datagrunt)