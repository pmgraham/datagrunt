## datagrunt
Welcome to datagrunt! datagrunt is a library designed to help with reading, processing, and transforming CSV files into various other formats.

## Description
This project was born from my time at Google as a Principal Architect and data engineer. I became frustrated with having to write similar code over and over again from project to project or from customer engagemen to customer engagement. Datagrunt was born out of this frustration.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
`pip install datagrunt`

## Usage
Instantiate the `CSVFile` class and then call class attributes and methods. The delimiter and CSV properties are inferred through a combination of a custom algorithm combined with the power of duckdb.
If the inferred delimiter is incorrect or if you want control over which delimiter to use for the CSV file(s), simply instantiate the `CSVFile` class and reset the `delimiter` property. as follows:

```
csv_file = CSVFile('myfile.csv')
csv_file.delimiter = '|'
```

An example of normal use for the `CSVFile` class is as follows:

```
csv_file = CSVFile('myfile.csv')
csv_file.size_in_mb # class attribute
csv_file.attributes() # class method
```

Datagrunt uses duckdb to enable enhanced data processing and to provide a SQL query engine for CSV files. Here is an example of how to take advantage of this capability. SQL query results are returned as a Polars dataframe.

```
csv_file = CSVFile('myfile.csv')

query = f"""
SELECT * 
FROM {csv_file.duckdb_instance.database_table_name} 
LIMIT 10
"""

df = csv_file.select_from_table(query)
print(df.head())
```

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
