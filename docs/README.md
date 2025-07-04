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
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with a simple Python API |
| [Polars](https://pola.rs) | Multi-threaded query engine written in Rust, optimized for modern processors |

### Datagrunt's Role

Datagrunt is not an extension of DuckDB or Polars, nor a comprehensive data processing solution. Its primary functions are:

1. Accurately inferring CSV delimiters
2. Providing helper methods for common data tasks
3. Facilitating CSV file loading into Polars dataframes
4. Enabling conversion to various output formats

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
```python
from datagrunt import CSVReader

# Load your CSV file
csv_file = 'electric_vehicle_population_data.csv'
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
```
###  SQL Queries For CSV Data
```python
from datagrunt import CSVReader

csv_file = 'electric_vehicle_population_data.csv'
engine = 'duckdb'

reader = CSVReader(csv_file, engine=engine)

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
Datagrunt provides two primary classes for interacting with data: `CSVReader` and `CSVWriter`. These classes are designed to simplify the process of reading and writing CSV files.

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


### File Attributes And CSV Attributes
Exposed in both the 'CSVReader` and `CSVWriter` classes are a number of attributes that allow you to access and manipulate file and CSV-specific information. Some examples of them are as follows (a more complete list is forthcoming soon):
- `filepath`: The path to the CSV file being read or written.
- `filesize`: The size of the file in bytes.
- `delimiter`: The delimiter used in the CSV file.
- `quotechar`: The quote character used in the CSV file.
- `escapechar`: The escape character used in the CSV file.
- `encoding`: The encoding used in the CSV file.
- `size_in_bytes`: The size of the file in bytes.
- `size_in_mb`: The size of the file in megabytes.
- `size_in_gb`: The size of the file in gigabytes.
- `size_in_tb`: The size of the file in terabytes.

# Known Issues
These are a list of known issues that will be added to the [Github Issue Tracker](https://github.com/pmgraham/datagrunt/issues).

# License
This project is licensed under the [MIT License](https://opensource.org/license/mit)

# Acknowledgements
A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) for their fantastic libraries that power Datagrunt.

# Source Repository
[https://github.com/pmgraham/datagrunt](https://github.com/pmgraham/datagrunt)
