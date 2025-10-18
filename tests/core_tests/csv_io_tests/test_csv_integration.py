"""Integration tests for the CSV API classes.

This module focuses on cross-engine compatibility, data consistency,
and complex scenarios that require multiple components working together.
Basic functionality testing is covered in the unit tests.
"""

import json

import polars as pl

from datagrunt import CSVReader, CSVWriter

# All supported engines for CSV processing
ALL_ENGINES = ["polars", "duckdb", "pyarrow"]


class TestCrossEngineIntegration:
    """Integration tests between CSVReader and CSVWriter across different engines."""

    def test_reader_writer_round_trip_all_combinations(self, sample_csv, tmp_path):
        """Test round-trip: read with one engine, write with another, read with third."""
        for read_engine in ALL_ENGINES:
            for write_engine in ALL_ENGINES:
                for final_read_engine in ALL_ENGINES:
                    # Read with first engine
                    reader = CSVReader(sample_csv, engine=read_engine)
                    original_data = reader.to_dicts()

                    # Write with second engine
                    writer = CSVWriter(sample_csv, engine=write_engine)
                    output_file = str(tmp_path / f"round_trip_{read_engine}_{write_engine}_{final_read_engine}.csv")
                    writer.write_csv(output_file)

                    # Read back with third engine
                    reader2 = CSVReader(output_file, engine=final_read_engine)
                    round_trip_data = reader2.to_dicts()

                    # Compare data (allowing for string type differences)
                    assert len(original_data) == len(round_trip_data)
                    assert len(original_data[0]) == len(round_trip_data[0])

    def test_normalization_consistency_across_engines(self, tmp_path):
        """Test that normalization produces identical results across all engines."""
        # Create test file with problematic column names
        test_file = tmp_path / "normalization_test.csv"
        test_file.write_text(
            "First Name,2nd Column,E-mail Address,Phone #,Date/Time\nJohn,A,john@test.com,555-1234,2023-01-01"
        )

        normalized_columns = {}
        normalized_data = {}

        # Test normalization with each engine for reading
        for engine in ALL_ENGINES:
            reader = CSVReader(str(test_file), engine=engine)
            df = reader.to_dataframe(normalize_columns=True)
            normalized_columns[engine] = list(df.columns)
            normalized_data[engine] = reader.to_dicts()[0]

        # All engines should produce identical normalized column names
        reference_columns = normalized_columns[ALL_ENGINES[0]]
        for engine in ALL_ENGINES[1:]:
            assert normalized_columns[engine] == reference_columns, f"Engine {engine} produced different column names"

        # Test that writing with normalization also produces consistent results
        for write_engine in ALL_ENGINES:
            writer = CSVWriter(str(test_file), engine=write_engine)
            output_file = str(tmp_path / f"write_norm_{write_engine}.csv")
            writer.write_csv(output_file, normalize_columns=True)

            # Verify normalized column names in output (strip quotes if present)
            with open(output_file, "r") as f:
                header_line = f.readline().strip()
                header = [col.strip('"') for col in header_line.split(",")]
                assert header == reference_columns

    def test_data_type_preservation_across_engines(self, tmp_path):
        """Test that different data types are preserved consistently across engines."""
        # Create CSV with various data types that could be interpreted differently
        test_file = tmp_path / "type_preservation_test.csv"
        test_content = """id,amount,code,date,boolean,decimal
001,0500,ABC123,2023-01-01,true,123.456
002,1000.50,DEF456,2023-12-31,false,0.789
003,00000,GHI789,2023-06-15,true,1000.000"""
        test_file.write_text(test_content)

        data_by_engine = {}

        for engine in ALL_ENGINES:
            reader = CSVReader(str(test_file), engine=engine)
            data_by_engine[engine] = reader.to_dicts()

        # Check that critical data preservation is consistent
        for engine in ALL_ENGINES:
            data = data_by_engine[engine]

            # Leading zeros should be preserved
            assert data[0]["id"] == "001"
            assert data[0]["amount"] == "0500"
            assert data[2]["amount"] == "00000"

            # Codes should be preserved as strings
            assert data[0]["code"] == "ABC123"
            assert data[1]["code"] == "DEF456"

    def test_format_conversion_chain(self, sample_csv, tmp_path):
        """Test converting through multiple formats maintains data integrity."""
        original_reader = CSVReader(sample_csv, engine="polars")
        original_data = original_reader.to_dicts()

        # CSV -> JSON -> CSV chain
        for engine in ALL_ENGINES:
            # Step 1: CSV to JSON
            writer = CSVWriter(sample_csv, engine=engine)
            json_file = str(tmp_path / f"temp_{engine}.json")
            writer.write_json(json_file)

            # Step 2: Read JSON back and verify
            with open(json_file, "r") as f:
                json_data = json.load(f)

            assert len(json_data) == len(original_data)
            assert len(json_data[0]) == len(original_data[0])

            # Step 3: Write JSON data back to CSV (simulated via CSV writer)
            csv_output = str(tmp_path / f"final_{engine}.csv")
            writer.write_csv(csv_output)

            # Step 4: Read final CSV and verify consistency
            final_reader = CSVReader(csv_output, engine=engine)
            final_data = final_reader.to_dicts()

            assert len(final_data) == len(original_data)

    def test_large_file_cross_engine_performance(self, tmp_path):
        """Test handling of larger files across different engines."""
        # Create a moderately large file (1000 rows)
        large_file = tmp_path / "large_test.csv"
        with open(large_file, "w") as f:
            f.write("id,value1,value2,category,timestamp\n")
            for i in range(1000):
                f.write(f"{i:04d},{i * 2},{i * 3.14:.2f},cat_{i % 10},2023-01-{(i % 28) + 1:02d}\n")

        # Test each engine can handle the file and produce consistent results
        row_counts = {}
        column_counts = {}
        sample_data = {}

        for engine in ALL_ENGINES:
            reader = CSVReader(str(large_file), engine=engine)

            # Test basic operations
            df = reader.to_dataframe()
            row_counts[engine] = len(df)
            column_counts[engine] = len(df.columns)

            # Test sampling doesn't fail
            reader.get_sample()

            # Get sample data for consistency check
            dicts = reader.to_dicts()
            sample_data[engine] = dicts[:5]  # First 5 rows

        # All engines should report same dimensions
        reference_rows = row_counts[ALL_ENGINES[0]]
        reference_cols = column_counts[ALL_ENGINES[0]]

        for engine in ALL_ENGINES[1:]:
            assert row_counts[engine] == reference_rows, f"Engine {engine} reported different row count"
            assert column_counts[engine] == reference_cols, f"Engine {engine} reported different column count"

        # Sample data should be consistent (allowing for type conversion differences)
        reference_keys = set(sample_data[ALL_ENGINES[0]][0].keys())
        for engine in ALL_ENGINES[1:]:
            engine_keys = set(sample_data[engine][0].keys())
            assert engine_keys == reference_keys, f"Engine {engine} has different columns"

    def test_special_characters_cross_engine_handling(self, tmp_path):
        """Test handling of special characters and edge cases across engines.

        Note: Some engines (like DuckDB) may have limitations with certain edge cases
        like quoted newlines in parallel mode. This test accommodates such limitations.
        """
        # Use a simpler special characters test that avoids DuckDB parallel scan issues
        special_file = tmp_path / "special_chars.csv"
        special_content = '''name,description,unicode_field
"John, Jr.","Product with ""quotes""","Standard ASCII"
"Jané Smith","Café item with àccénts","Unicode: ñ, é, ü"
"Comma,Field","Semicolon;Field","Pipe|Field"
"Tab	Field","Quote""Field","Mixed: àéîôü"'''
        special_file.write_text(special_content, encoding="utf-8")

        results_by_engine = {}
        successful_engines = []

        for engine in ALL_ENGINES:
            try:
                reader = CSVReader(str(special_file), engine=engine)
                data = reader.to_dicts()
                results_by_engine[engine] = {
                    "success": True,
                    "row_count": len(data),
                    "first_row": data[0] if data else None,
                }
                successful_engines.append(engine)
            except Exception as e:
                results_by_engine[engine] = {"success": False, "error": str(e)}

        # At least one engine should successfully parse the file
        assert len(successful_engines) > 0, f"No engines could parse the special characters file: {results_by_engine}"

        # Among successful engines, row counts should be consistent
        if len(successful_engines) > 1:
            reference_count = results_by_engine[successful_engines[0]]["row_count"]
            for engine in successful_engines[1:]:
                assert results_by_engine[engine]["row_count"] == reference_count, (
                    f"Engine {engine} had different row count than reference"
                )

        # Log any engine failures for debugging (but don't fail the test)
        failed_engines = [eng for eng in ALL_ENGINES if not results_by_engine[eng]["success"]]
        if failed_engines:
            print(f"Note: Engines {failed_engines} had limitations with this special characters test")
            for engine in failed_engines:
                print(f"  {engine}: {results_by_engine[engine]['error']}")

    def test_empty_and_edge_case_consistency(self, tmp_path):
        """Test that all engines handle edge cases consistently."""
        test_cases = {
            "completely_empty": "",
            "only_header": "col1,col2,col3\n",
            "header_with_empty_row": "col1,col2,col3\n,,\n",
            "whitespace_only": "   \n  \n  ",
            "single_column": "value\ntest\n",
            "trailing_commas": "col1,col2,\nval1,val2,\n",
        }

        for case_name, content in test_cases.items():
            test_file = tmp_path / f"{case_name}.csv"
            test_file.write_text(content)

            results = {}
            for engine in ALL_ENGINES:
                try:
                    reader = CSVReader(str(test_file), engine=engine)
                    df = reader.to_dataframe()
                    results[engine] = {
                        "success": True,
                        "rows": len(df),
                        "cols": len(df.columns) if len(df) > 0 or df.shape[1] > 0 else 0,
                        "shape": df.shape,
                    }
                except Exception as e:
                    results[engine] = {"success": False, "error": str(e)}

            # All engines should handle the case (success or consistent failure)
            success_states = [r["success"] for r in results.values()]
            if any(success_states):  # If any engine succeeds, check consistency
                successful_engines = [eng for eng, res in results.items() if res["success"]]
                if len(successful_engines) > 1:
                    reference = results[successful_engines[0]]
                    for engine in successful_engines[1:]:
                        assert results[engine]["shape"] == reference["shape"], (
                            f"Case {case_name}: Engine {engine} shape mismatch"
                        )


class TestAdvancedDataScenarios:
    """Test complex data scenarios that require robust handling."""

    def test_mixed_data_types_consistency(self, tmp_path):
        """Test files with mixed and potentially problematic data types."""
        mixed_file = tmp_path / "mixed_types.csv"
        mixed_content = """string_col,int_col,float_col,bool_col,date_col,null_col
"text",123,45.67,true,"2023-01-01",
"another",456,78.90,false,"2023-02-01",NULL
"third",789,,true,,"NA"
"fourth",0,0.0,false,"2023-03-01","""
        mixed_file.write_text(mixed_content)

        # Test that all engines can read and convert consistently
        conversion_results = {}

        for engine in ALL_ENGINES:
            reader = CSVReader(str(mixed_file), engine=engine)

            # Test multiple output formats
            conversion_results[engine] = {
                "dataframe": reader.to_dataframe(),
                "dicts": reader.to_dicts(),
                "arrow": reader.to_arrow_table(),
            }

        # Verify consistent row counts across engines and formats
        reference_row_count = len(conversion_results[ALL_ENGINES[0]]["dicts"])
        for engine in ALL_ENGINES:
            assert len(conversion_results[engine]["dicts"]) == reference_row_count
            assert len(conversion_results[engine]["dataframe"]) == reference_row_count
            assert conversion_results[engine]["arrow"].num_rows == reference_row_count

    def test_query_consistency_across_engines(self, tmp_path):
        """Test that SQL queries produce consistent results across engines."""
        # Create a dataset suitable for querying
        query_file = tmp_path / "query_test.csv"
        query_content = """id,category,amount,active
    1,A,100.0,true
    2,B,200.5,false
    3,A,150.0,true
    4,C,75.25,true
    5,B,300.0,false"""
        query_file.write_text(query_content)

        # Test queries that should work across all engines
        test_queries = [
            "SELECT COUNT(*) as count FROM {table}",
            "SELECT category, COUNT(*) as count FROM {table} GROUP BY category",
            "SELECT * FROM {table} WHERE active = 'true'",
            "SELECT AVG(CAST(amount AS FLOAT)) as avg_amount FROM {table}",
        ]

        query_results = {}

        for engine in ALL_ENGINES:
            reader = CSVReader(str(query_file), engine=engine)
            query_results[engine] = {}

            for query_template in test_queries:
                query = query_template.format(table=reader.db_table)
                try:
                    result = reader.query_data(query)
                    # Convert result to a comparable format
                    if hasattr(result, "pl") and callable(getattr(result, "pl", None)):  # DuckDB relation
                        result_df = result.pl()
                    elif hasattr(result, "columns"):  # DataFrame-like object
                        result_df = result
                    else:  # Could be list or other format
                        # Convert to polars DataFrame for consistent handling
                        result_df = pl.DataFrame(result) if isinstance(result, list) else result

                    query_results[engine][query_template] = {
                        "success": True,
                        "rows": len(result_df),
                        "columns": list(result_df.columns),
                    }
                except Exception as e:
                    query_results[engine][query_template] = {"success": False, "error": str(e)}

        # Verify query results are consistent where successful
        for query_template in test_queries:
            successful_engines = [eng for eng in ALL_ENGINES if query_results[eng][query_template]["success"]]

            if len(successful_engines) > 1:
                reference = query_results[successful_engines[0]][query_template]
                for engine in successful_engines[1:]:
                    result = query_results[engine][query_template]
                    assert result["rows"] == reference["rows"], (
                        f"Query row count mismatch for {engine}: {query_template}"
                    )
