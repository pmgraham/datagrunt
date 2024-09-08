# Welcome To Datagrunt

Datagrunt Is A Python Library Designed To Simplify The Way You Work With CSV files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why Datagrunt?

Born out of real-world frustration, Datagrunt Eliminates The Need For repetitive coding when handling CSV files. Whether you're a data analyst, data engineer, or data scientist, Datagrunt Empowers You To Focus On insights, not tedious data wrangling.

## Key Features

- **Intelligent Delimiter Inference:**  Datagrunt Automatically Detects and applies the correct delimiter for your CSV files using a custom algorithm and the power of DuckDB.
- **Seamless Data Processing:** Leverage the robust capabilities of DuckDB to perform advanced data processing tasks directly on your CSV data.
- **Flexible Transformation:** Easily convert your processed CSV data into various formats to suit your needs.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

## Installation

Get started with Datagrunt In Seconds Using Pip:

```bash
pip install Datagrunt
```

## Getting Started

```python
from datagrunt.Csvfile Import Csvreader

# Load your CSV file
csv_file = 'data/electric_vehicle_population_data.csv'
engine = 'duckdb'

# Set duckdb as the processing engine. Engine set to 'polars' by default
dg = CSVReader(csv_file, engine=engine)

# return sample of the data to get a peek at the schema
dg.get_sample()
```

##  DuckDB Integration for Performant SQL Queries
```python
from datagrunt.Csvfile Import Csvreader

csv_file = 'data/electric_vehicle_population_data.csv'
engine = 'duckdb'

dg = CSVReader(csv_file, engine=engine)

# Construct your SQL query
query = f"""
WITH core AS (
    SELECT
        City AS city,
        "VIN (1-10)" AS vin
    FROM {dg.db_table}
)
SELECT
    city,
    COUNT(vin) AS vehicle_count
FROM core
GROUP BY 1
ORDER BY 2 DESC
"""
# Execute the query and get results as a Polars DataFrame
df = dg.query_data(query).pl()
print(df.head())
```

## Contributing

## Support

## Roadmap

## License
This project is licensed under the [MIT License](https://opensource.org/license/mit)

## Acknowledgements
A HUGE thank you to the open-source community and the creators of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) for their fantastic libraries that power Datagrunt.