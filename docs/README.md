# Welcome To Datagrunt
Datagrunt is a Python library designed to simplify the way you work with CSV files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why Datagrunt?
Born out of real-world frustration, Datagrunt eliminates the need for repetitive coding when handling CSV files. Whether you're a data analyst, data engineer, or data scientist, Datagrunt empowers you to focus on insights, not tedious data wrangling.

### What Datagrunt Is Not
Datagrunt is not an extension of or a replacement for DuckDB or Polars, nor is it a comprehensive data processing solution. It is not designed to be a comprehensive one-stop shop for all of your CSV processing needs. Instead, it's designed to simplify the way you work with CSV files and to help solve the pain point of inferring delimiters when a file structure is unknown. Datagrunt provides an easy way to convert CSV files to dataframes and export them to various formats. One of Datagrunt's value propositions is its relative
simplicity and ease of use. We will extend functionality where it makes sense to do so, but we will be selective and strategic in our approach to adding or extending functionality.

### Key Features

- **Intelligent Delimiter Inference:** Datagrunt automatically detects and applies the correct delimiter for your CSV files.
- **Path Object Support:** Full support for both string paths and `pathlib.Path` objects for modern, cross-platform file handling.
- **Multiple Processing Engines:** Choose from three powerful engines - [DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [PyArrow](https://arrow.apache.org/docs/python/) - to handle your data processing needs.
- **Flexible Data Transformation:** Easily convert your processed CSV data into various formats including CSV, Excel, JSON, JSONL, and Parquet.
- **AI-Powered Schema Analysis:** Use Google's Gemini models to automatically generate detailed schema reports for your CSV files, including data types, column classifications, and data quality checks.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with excellent SQL support |
| [Polars](https://pola.rs) | Multi-threaded DataFrame library written in Rust, optimized for performance |
| [PyArrow](https://arrow.apache.org/docs/python/) | Python bindings for Apache Arrow with efficient columnar data processing |
| [Google Gemini](https://deepmind.google/technologies/gemini/) | A powerful family of generative AI models for schema analysis |

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

### A Word About Pandas
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

# Installation

We recommend using [UV](https://docs.astral.sh/uv/). However, you may get started with Datagrunt in seconds using UV or pip.

Get started with UV:

```bash
uv pip install datagrunt
```

Get started with pip:

```bash
pip install datagrunt
```

# Getting Started with Datagrunt
This section provides a comprehensive guide to using Datagrunt effectively. Here's what you'll learn:

- **[Path Object Support](#path-object-support)** - Modern file path handling with pathlib
- **[Datagrunt Engines](#datagrunt-engines)** - Choose the right engine for your needs
- **[Column Name Normalization](#normalizing-column-names)** - Clean and standardize column names
- **[AI-Powered Analysis](#artificial-intelligence-features)** - Generate schema reports with AI
- **[Usage Examples](#usage-examples)** - Practical code examples
- **[Primary Classes](#primary-classes)** - Detailed API reference

## Path Object Support

Datagrunt fully supports both string paths and `pathlib.Path` objects, providing modern, cross-platform file handling capabilities. All classes (`CSVReader`, `CSVWriter`, `CSVSchemaReportAIGenerated`) seamlessly accept either input type.

### Benefits of Path Objects
- **Cross-platform compatibility**: Automatic handling of path separators (`/` vs `\`)
- **Better readability**: Intuitive path operations like `path.stem`, `path.suffix`
- **Type safety**: Clear distinction between strings and paths
- **Modern Python standards**: Following current best practices

### Usage Examples

```python
from datagrunt import CSVReader, CSVWriter
from pathlib import Path

# All of these work identically
csv_reader_str = CSVReader('data/file.csv')                    # String path
csv_reader_path = CSVReader(Path('data/file.csv'))             # Path object
csv_reader_resolved = CSVReader(Path('data').resolve() / 'file.csv')  # Complex Path

# Writers work the same way
csv_writer = CSVWriter(Path('output/results.csv'))

# Mixing is fine too
input_path = Path('input.csv')
reader = CSVReader(input_path)
writer = CSVWriter('output.csv')  # String path
```

### Internal Conversion
When you pass a string path to any Datagrunt class, it's automatically converted to a `Path` object internally:

```python
from datagrunt import CSVReader
from pathlib import Path

reader = CSVReader('my_file.csv')  # String input
print(type(reader.filepath))       # <class 'pathlib.PosixPath'> (or WindowsPath)
print(reader.filepath.name)        # 'my_file.csv'
print(reader.filepath.suffix)      # '.csv'
```

### Backward Compatibility
✅ **Fully backward compatible** - all existing string-based code continues to work unchanged while gaining the benefits of Path objects under the hood.

## Datagrunt Engines

Datagrunt provides three powerful engines for working with CSV files: Polars, DuckDB, and PyArrow. Each engine has its own strengths and is optimized for different use cases.

### Engine Selection
When instantiating the `CSVReader` or `CSVWriter` class, you can specify which engine to use:
- **CSVReader** default engine: `polars`
- **CSVWriter** default engine: `duckdb`

```python
# Using different engines
reader_polars = CSVReader('file.csv', engine='polars')    # Default
reader_duckdb = CSVReader('file.csv', engine='duckdb')
reader_pyarrow = CSVReader('file.csv', engine='pyarrow')  # New!

writer_duckdb = CSVWriter('file.csv', engine='duckdb')   # Default
writer_polars = CSVWriter('file.csv', engine='polars')
writer_pyarrow = CSVWriter('file.csv', engine='pyarrow') # New!
```

### Polars Engine
The **Polars** engine is the default for `CSVReader` because it excels at fast DataFrame operations and memory efficiency. Polars is built in Rust and optimized for modern processors, making it ideal for:
- Fast CSV reading and initial data exploration
- DataFrame-style data manipulation
- Integration with existing Polars workflows
- Memory-efficient processing of medium to large datasets

### DuckDB Engine
The **DuckDB** engine is the default for `CSVWriter` because it provides superior SQL capabilities and optimized file exports. DuckDB excels at:
- Complex SQL queries and analytics
- Efficient data export operations, especially to JSON and Parquet formats
- Handling large datasets with minimal memory usage
- Consistent formatting across different output formats

### PyArrow Engine (New!)
The **PyArrow** engine leverages Apache Arrow's columnar memory format for optimal performance with certain data processing patterns. PyArrow is particularly well-suited for:
- Interoperability with other Arrow-based tools and systems
- Efficient columnar data processing
- Direct integration with Parquet and other Arrow-native formats
- Zero-copy data sharing between different processing libraries

PyArrow maintains string data types throughout processing to prevent data loss, making it an excellent choice when data fidelity is paramount.

### Engine Comparison

| Feature | Polars | DuckDB | PyArrow |
|---------|--------|--------|---------|
| **Best for** | DataFrame operations | SQL queries & analytics | Arrow ecosystem integration |
| **Performance** | Fast in-memory processing | Excellent for large datasets | Optimized columnar operations |
| **Default for** | CSVReader | CSVWriter | - |
| **Data Types** | Type inference | All varchar import | String preservation |
| **Export Quality** | Good | Excellent (especially JSON) | Native Parquet support |
| **Memory Usage** | Efficient | Very efficient | Columnar efficiency |

### Google Gemini Engine
As of Datagrunt version 2.0.1, engines have been added for Generative AI LLM providers. Currently only Google Gemini is supported, but with the factory pattern we can easily add more providers in the future. How we approach this is being debated among the Datagrunt maintainers. We'll post more updates in the future regarding this topic.

## Normalizing Column Names
When working with data from various sources, column names can be inconsistent, contain special characters, or include spaces. This can make them difficult to work with in databases or dataframes. Datagrunt provides a convenient way to clean and standardize these names.

**Why Normalize?**
- **Compatibility:** Ensures column names are valid identifiers for databases (e.g., DuckDB) and can be used as attributes in dataframes (e.g., `df.my_column` instead of `df['My Column']`).
- **Consistency:** Creates a uniform naming convention across your data, making your code cleaner and more predictable.

**The Normalization Process**
When you set `normalize_columns=True`, Datagrunt applies the following rules to each column name:
1.  Converts the name to lowercase.
2.  Replaces any sequence of spaces or special characters (anything not a letter or number) with a single underscore (`_`).
3.  Removes any leading or trailing underscores.
4.  If a name starts with a number, it prepends an underscore (e.g., `2020_census_tract` becomes `_2020_census_tract`).
5.  If normalization results in duplicate column names, it appends a number to make them unique (e.g., `column`, `column_1`, `column_2`).

You can access the original columns via the `.columns` attribute and the normalized list via the `.columns_normalized` attribute. A mapping is also available in the `.columns_to_normalized_mapping` dictionary.

## Artificial Intelligence Features
As of Datagrunt version 2.0.1 integration with Large Language Models (LLMs) is available. Currently only Google Gemini is available. We plan to add more LLMs in the future.

### Artificial Intelligence (AI) Engines
As of Datagrunt version 2.0.1 we introduced a factory pattern to support multiple LLM providers. Currently, the only engine available is Google Gemini. In order to access Gemini, you need either a Gemini API key or you need to be authenticated with a Google Cloud account so that you can use Vertex AI. Both are supported in the same interface depending on the set of parameters you pass into the `CSVSchemaReportAIGenerated` class.

### Google Gemini
Currently there is only one class that supports integration with Google Gemini: `CSVSchemaReportAIGenerated`. It is exposed as part of the facade pattern along with the `CSVReader` and `CSVWriter` classes. See below under the `Primary Classes` section for more details.

## Usage Examples
### Reading and Querying CSV Data
```python
from datagrunt import CSVReader
from pathlib import Path

# Load your CSV file (accepts both string and Path objects)
csv_file = Path('examples/data/electric_vehicle_population_data.csv')

# Choose your engine: 'polars' (default), 'duckdb', or 'pyarrow'
reader = CSVReader(csv_file, engine='duckdb')

# Example with PyArrow engine (new!)
# reader_arrow = CSVReader(csv_file, engine='pyarrow')

# Return a sample of the data to get a peek at the schema
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
│     ·      │   ·       │    ·         │ · │          ·           │          ·           │      ·            │
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
├────────────┴───────────┴──────────────┴───┴──────────────────────┴──────────────────────┴───────────────────┤
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
df = reader.query_data(query).pl()  # The .pl() method converts DuckDBPyRelation to Polars DataFrame
print(df)
��────────────────┬───────────────┐
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
from pathlib import Path
import os

# Load your CSV file (accepts both string and Path objects)
csv_file = Path('examples/data/electric_vehicle_population_data.csv')

# --- Option 1: Use a Google Gemini API Key ---
# Make sure to set your API Key as an environment variable
api_key = os.environ.get("GEMINI_API_KEY")

# Instantiate the report generator with an API key
report_generator_api = CSVSchemaReportAIGenerated(
    filepath=csv_file,
    engine='google',
    api_key=api_key
)

# --- Option 2: Use Google Cloud Vertex AI ---
# This assumes you have authenticated with Google Cloud CLI (gcloud auth application-default login)
report_generator_vertex = CSVSchemaReportAIGenerated(
    filepath=csv_file,
    engine='google',
    vertexai=True,
    gcp_project='my-gcp-project-id',  # Change to your project ID
    gcp_location='us-central1'       # Change to your GCP location
)

# Generate the report using a powerful model (choose one of the generators from above)
# Note: You must have access to the model you specify.
schema_report = report_generator_api.generate_csv_schema_report(
    model='gemini-2.5-flash',
    return_json=True
)

print(schema_report)
```
This will produce a detailed JSON report analyzing the CSV's schema, data types, and column classifications. Here is an output example:

```json
{
    "has_column_header": true,
    "is_structured": true,
    "has_delimiter": true,
    "delimiter": ",",
    "is_tabular": true,
    "encoding": "utf-8",
    "total_column_count": 17,
    "total_dimension_column_count": 15,
    "total_measure_column_count": 2,
    "schema": [
        {
            "name": "VIN (1-10)",
            "normalized_name": "vin_1_10",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "County",
            "normalized_name": "county",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "City",
            "normalized_name": "city",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "State",
            "normalized_name": "state",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Postal Code",
            "normalized_name": "postal_code",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Model Year",
            "normalized_name": "model_year",
            "data_type": "integer",
            "classification": "dimension"
        },
        {
            "name": "Make",
            "normalized_name": "make",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Model",
            "normalized_name": "model",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Electric Vehicle Type",
            "normalized_name": "electric_vehicle_type",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Clean Alternative Fuel Vehicle (CAFV) Eligibility",
            "normalized_name": "clean_alternative_fuel_vehicle_cafv_eligibility",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Electric Range",
            "normalized_name": "electric_range",
            "data_type": "integer",
            "classification": "measure"
        },
        {
            "name": "Base MSRP",
            "normalized_name": "base_msrp",
            "data_type": "integer",
            "classification": "measure"
        },
        {
            "name": "Legislative District",
            "normalized_name": "legislative_district",
            "data_type": "integer",
            "classification": "dimension"
        },
        {
            "name": "DOL Vehicle ID",
            "normalized_name": "dol_vehicle_id",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Vehicle Location",
            "normalized_name": "vehicle_location",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "Electric Utility",
            "normalized_name": "electric_utility",
            "data_type": "string",
            "classification": "dimension"
        },
        {
            "name": "2020 Census Tract",
            "normalized_name": "census_tract_2020",
            "data_type": "string",
            "classification": "dimension"
        }
    ],
    "dimensions": [
        "vin_1_10",
        "county",
        "city",
        "state",
        "postal_code",
        "model_year",
        "make",
        "model",
        "electric_vehicle_type",
        "clean_alternative_fuel_vehicle_cafv_eligibility",
        "legislative_district",
        "dol_vehicle_id",
        "vehicle_location",
        "electric_utility",
        "census_tract_2020"
    ],
    "measures": [
        "electric_range",
        "base_msrp"
    ],
    "columns_rename_map": {
        "VIN (1-10)": "vin_1_10",
        "County": "county",
        "City": "city",
        "State": "state",
        "Postal Code": "postal_code",
        "Model Year": "model_year",
        "Make": "make",
        "Model": "model",
        "Electric Vehicle Type": "electric_vehicle_type",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "clean_alternative_fuel_vehicle_cafv_eligibility",
        "Electric Range": "electric_range",
        "Base MSRP": "base_msrp",
        "Legislative District": "legislative_district",
        "DOL Vehicle ID": "dol_vehicle_id",
        "Vehicle Location": "vehicle_location",
        "Electric Utility": "electric_utility",
        "2020 Census Tract": "census_tract_2020"
    }
}
```

### Using the PyArrow Engine
```python
from datagrunt import CSVReader, CSVWriter
from pathlib import Path

# Reading with PyArrow engine (accepts both strings and Path objects)
reader = CSVReader(Path('path/to/file.csv'), engine='pyarrow')

# Get data as PyArrow table (native format)
arrow_table = reader.to_arrow_table(normalize_columns=True)

# Convert to other formats
polars_df = reader.to_dataframe()  # Returns Polars DataFrame
dict_list = reader.to_dicts()      # Returns list of dictionaries

# Writing with PyArrow engine
writer = CSVWriter(Path('path/to/file.csv'), engine='pyarrow')

# Export to various formats with PyArrow's optimized writers
writer.write_parquet('output.parquet')  # Efficient native Parquet export
writer.write_csv('output.csv')          # Fast CSV export
writer.write_json('output.json')        # JSON export
```

### Combine Datagrunt With Other Libraries

Datagrunt can be combined with other libraries. For example, you could use Datagrunt to instantiate the `CSVReader` class, and then use the provided `delimiter` attribute with other libraries.

```python
from datagrunt import CSVReader
from pathlib import Path
import pandas as pd

reader = CSVReader(Path('path/to/file.csv'))
df = pd.read_csv(reader.filepath, sep=reader.delimiter)  # filepath and delimiter are CSVReader attributes
```

### Reassign the Delimiter

Sometimes, the delimiter may not be correctly identified by Datagrunt. In such cases, you can reassign the delimiter attribute to correct it.
```python
from datagrunt import CSVReader
from pathlib import Path
import pandas as pd

reader = CSVReader(Path('path/to/file.csv'))

# Let's assume the delimiter was incorrectly inferred as a space. Reassign it to correct the issue.
reader.delimiter = ','
df = pd.read_csv(reader.filepath, sep=reader.delimiter)
```

By updating the delimiter attribute, you ensure the `CSVReader` object will read the file correctly when you use any of its methods subsequently.

## Primary Classes
Datagrunt provides three primary classes for interacting with data: `CSVReader`, `CSVWriter`, and `CSVSchemaReportAIGenerated`. These classes are designed to simplify the process of reading, writing, and analyzing CSV files.

### CSVReader
The `CSVReader` class is used to read data from a CSV file. It accepts both string paths and `pathlib.Path` objects, providing a simple interface for reading data from a CSV file and converting it into various formats. You instantiate the `CSVReader` class as follows:
```python
from datagrunt import CSVReader
from pathlib import Path

reader = CSVReader('path/to/file.csv')          # String path
reader = CSVReader(Path('path/to/file.csv'))    # Path object
```

You may optionally specify the engine to use for reading the CSV file. The three options are `polars` (default), `duckdb`, and `pyarrow`.
```python
reader = CSVReader(Path('path/to/file.csv'), engine='duckdb')   # DuckDB engine
reader = CSVReader('path/to/file.csv', engine='pyarrow')        # PyArrow engine
reader = CSVReader(Path('path/to/file.csv'))                    # Default: Polars engine
```

The primary methods of the `CSVReader` class are:
- `get_sample(normalize_columns=False)`: Returns a sample of the data in the CSV file (20 rows).
- `to_dataframe(normalize_columns=False)`: Converts the data in the CSV file into a Polars DataFrame.
- `to_arrow_table(normalize_columns=False)`: Converts the data in the CSV file into a PyArrow Table.
- `to_dicts(normalize_columns=False)`: Converts the data in the CSV file into a list of dictionaries.
- `query_data(sql_query, normalize_columns=False)`: Executes a SQL query on the data in the CSV file.

### CSVWriter
The `CSVWriter` class is used to convert and export CSV data to various file formats. It accepts both string paths and `pathlib.Path` objects and supports three engines: `duckdb` (default), `polars`, and `pyarrow`.

```python
from datagrunt import CSVWriter
from pathlib import Path

writer = CSVWriter(Path('path/to/file.csv'), engine='duckdb')   # Default: DuckDB engine
writer = CSVWriter('path/to/file.csv', engine='polars')        # Polars engine
writer = CSVWriter(Path('path/to/file.csv'), engine='pyarrow') # PyArrow engine
```

The primary methods of the `CSVWriter` class are:
- `write_csv(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a CSV file.
- `write_excel(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to an Excel file.
- `write_json(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a JSON file.
- `write_json_newline_delimited(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a JSON file with newline delimiters.
- `write_parquet(self, out_filename=None, normalize_columns=False)`: Writes the data in the CSV file to a Parquet file.

### CSVSchemaReportAIGenerated
The `CSVSchemaReportAIGenerated` class generates detailed schema reports for CSV files using AI. It provides a simple interface for analyzing CSV file structure and data types.
It is currently configured to work only with Google's Gemini and accepts either an `api_key` or can access Vertex AI if you pass in the following parameters:
- `vertexai=True`
- `gcp_project=my-gcp-project-id`
- `gcp_location=global` or a supported Google Cloud region such as `us-central1`

The primary method of the `CSVSchemaReportAIGenerated` class is:
- `generate_csv_schema_report(self, model, prompt=None, system_instructions=None, return_json=False)`: Generates a comprehensive schema report for a CSV file.
    - **model**: Any supported Gemini model. No default model is set by design. See [available Google Gemini models](https://cloud.google.com/vertex-ai/docs/generative-ai/models)
    - **prompt**: Optional custom prompt. Uses a default prompt if none provided.
    - **system_instructions**: Optional system instructions. Uses default instructions if none provided.
    - **return_json**: Returns a Python dict by default (`False`). Set to `True` to return formatted JSON string.

Here are the optional keyword parameters you may pass in along with their default values:
- **vertexai**: `False` - Enable Vertex AI instead of direct API access
- **gcp_project**: `None` - Google Cloud project ID (required for Vertex AI)
- **gcp_location**: `None` - Google Cloud region (required for Vertex AI)
- **max_tokens**: `8192` - Maximum output tokens (current limit ~65k)
- **temperature**: `0.5` - Controls randomness in responses (0.0-1.0)
- **top_p**: `1` - Controls diversity of token selection (0.0-1.0)
- **seed**: `0` - Random seed for reproducible outputs
- **safety_settings**: `None` - Custom safety settings (defaults listed below)
- **thinking_budget**: `-1` - Thinking time budget (-1 for automatic)
- **response_type**: `"application/json"` - Output MIME type
- **ground_google_search**: `False` - Enable Google Search grounding (planned feature)

There is a default prompt that is built into Datagrunt that enables this method to operate. You may optionally pass in your own prompt if you wish. If you do not pass in a prompt, Datagrunt will use the default system prompt. Otherwise, it will use the prompt you pass in.

#### Generative AI Default Prompts
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
 "dimensions": ["column_name_1", "column_name_2", ...],
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

#### No AI Agents At This Time
The current implementation leveraging an LLM to evaluate a CSV file is a simple API call to the LLM provider (currently Google Gemini). To be clear, this is not an AI agent nor an agentic component of Datagrunt—it is simply an API call to Gemini.

AI Agents may be added in the future, but this is currently being debated among the maintainers of Datagrunt. We will post more details on this decision in the future.

## Migration Guide

### Path Object Changes (Version 2.1.1+)

As of version 2.1.0, Datagrunt uses `pathlib.Path` objects internally for all file operations, providing better cross-platform compatibility and more intuitive path handling.

#### What Changed
- All filepath parameters now accept both string paths and `pathlib.Path` objects
- Internally, string paths are automatically converted to `Path` objects
- The `filepath` attribute on all classes now returns a `Path` object instead of a string

#### Backward Compatibility
✅ **No breaking changes** - all existing code continues to work:

```python
# This still works exactly the same
reader = CSVReader('my_file.csv')
writer = CSVWriter('output.csv')
```

#### What You Might Notice
If you access the `filepath` attribute directly, it now returns a `Path` object:

```python
reader = CSVReader('my_file.csv')
print(type(reader.filepath))  # <class 'pathlib.PosixPath'> (was <class 'str'>)
print(reader.filepath.name)   # 'my_file.csv' - now available directly
```

#### Benefits
- Better cross-platform path handling
- More intuitive path operations (`path.name`, `path.suffix`, etc.)
- No changes required to existing code

## File and CSV Attributes
Exposed in both the `CSVReader` and `CSVWriter` classes are a number of attributes that allow you to access and manipulate file and CSV-specific information:

**File Attributes:**
- `filepath`: The `pathlib.Path` object representing the file path.
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
There are no known issues at this time. Please report any bugs or issues on the [Github Issue Tracker](https://github.com/pmgraham/datagrunt/issues).

# License
This project is licensed under the [MIT License](https://opensource.org/license/mit)

# Acknowledgements
A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) for their fantastic libraries that power Datagrunt.

# Source Repository
[https://github.com/pmgraham/datagrunt](https://github.com/pmgraham/datagrunt)
