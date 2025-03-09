# Welcome To Datagrunt

Datagrunt is a Python library designed to simplify the way you work with CSV files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why Datagrunt?

Born out of real-world frustration, Datagrunt eliminates the need for repetitive coding when handling CSV files. Whether you're a data analyst, data engineer, or data scientist, Datagrunt empowers you to focus on insights, not tedious data wrangling.

## Key Features

- **Intelligent Delimiter Inference:**  Datagrunt automatically detects and applies the correct delimiter for your CSV files.
- **Seamless Data Processing:** Leverage the robust capabilities of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) to perform advanced data processing tasks directly on your CSV data.
- **Flexible Transformation:** Easily convert your processed CSV data into various formats to suit your needs.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

## Installation

Get started with Datagrunt in seconds using pip:

```bash
pip install datagrunt
```

## Getting Started

```python
from datagrunt import CSVReader

# Load your CSV file
csv_file = 'electric_vehicle_population_data.csv'
engine = 'duckdb'

# Set duckdb as the processing engine. Engine set to 'polars' by default
dg = CSVReader(csv_file, engine=engine)

# return sample of the data to get a peek at the schema
dg.get_sample()
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

##  DuckDB Integration for Performant SQL Queries
```python
from datagrunt import CSVReader

csv_file = 'electric_vehicle_population_data.csv'
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
## License
This project is licensed under the [MIT License](https://opensource.org/license/mit)

## Acknowledgements
A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) for their fantastic libraries that power Datagrunt.
