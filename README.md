# Welcome To datagrunt

datagrunt is a Python library designed to simplify the way you work with CSV files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why datagrunt?

Born out of real-world frustration, datagrunt eliminates the need for repetitive coding when handling CSV files. Whether you're a data analyst, data engineer, or data scientist, datagrunt empowers you to focus on insights, not tedious data wrangling.

## Key Features

- **Intelligent Delimiter Inference:**  Datagrunt automatically detects and applies the correct delimiter for your CSV files using a custom algorithm and the power of DuckDB.
- **Seamless Data Processing:** Leverage the robust capabilities of DuckDB to perform advanced data processing tasks directly on your CSV data.
- **Flexible Transformation:** Easily convert your processed CSV data into various formats to suit your needs.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

## Installation

Get started with datagrunt in seconds using pip:

```bash
pip install datagrunt
```

## Getting Started

```python
from datagrunt.csvfiles import CSVFile

# Load your CSV file
csv_file = CSVFile('myfile.csv')

# Access file information
print(f"File Size: {csv_file.size_in_mb} MB") 
print(f"CSV Attributes: {csv_file.attributes()}")

# Override inferred delimiter (if needed)
csv_file.delimiter = '|' 

```

##  DuckDB Integration for Powerful Queries
```python
from datagrunt.csvfiles import CSVFile

csv_file = CSVFile('myfile.csv')

# Construct your SQL query
query = f"""
    SELECT * 
    FROM {csv_file.duckdb_instance.database_table_name} 
    LIMIT 10
"""
# Execute the query and get results as a Polars DataFrame
df = csv_file.select_from_table(query)
print(df.head())
```

## Contributing
We welcome contributions from the community! If you'd like to contribute to datagrunt, please follow these steps:

    - Fork the repository.
    - Create a new branch for your feature or bug fix.
    - Write tests to cover your changes.
    - Ensure your code passes all tests and follows the project's style guidelines.
    - Submit a pull request for review.

## Support

## Roadmap

## License
This project is licensed under the [MIT License](https://opensource.org/license/mit)

## Acknowledgements
A HUGE thank you to the open-source community and the creators of [DuckDB](https://duckdb.org) and [Polars](https://pola.rs) for their fantastic libraries that power datagrunt.