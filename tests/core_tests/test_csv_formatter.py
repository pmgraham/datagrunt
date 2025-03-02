class TestCSVFormatter:
    """Test suite for CSVFormatter class"""

    def test_normalize_single_column_name_basic(self, formatter):
        """Test basic column name normalization"""
        test_cases = [
            ("Column Name", "column_name"),
            ("UPPER CASE", "upper_case"),
            ("lower case", "lower_case"),
            ("MixedCase", "mixedcase")
        ]
        for input_name, expected in test_cases:
            assert formatter.normalize_single_column_name(input_name) == expected

    def test_normalize_single_column_name_special_characters(self, formatter):
        """Test normalization with special characters"""
        test_cases = [
            ("name!@#$%^&*()", "name"),
            ("special-chars", "special_chars"),
            ("multiple!!@@##spaces", "multiple_spaces"),
            ("email@address.com", "email_address_com")
        ]
        for input_name, expected in test_cases:
            assert formatter.normalize_single_column_name(input_name) == expected

    def test_normalize_single_column_name_leading_digits(self, formatter):
        """Test normalization of column names starting with digits"""
        test_cases = [
            ("123column", "_123column"),
            ("1st_place", "_1st_place"),
            ("2ndColumn", "_2ndcolumn"),
            ("42", "_42")
        ]
        for input_name, expected in test_cases:
            assert formatter.normalize_single_column_name(input_name) == expected

    def test_normalize_single_column_name_multiple_underscores(self, formatter):
        """Test handling of multiple underscores"""
        test_cases = [
            ("multiple___underscores", "multiple_underscores"),
            ("trailing__", "trailing"),
            ("__leading__", "leading"),
            ("__multiple___spaces__", "multiple_spaces")
        ]
        for input_name, expected in test_cases:
            assert formatter.normalize_single_column_name(input_name) == expected

    def test_make_unique_column_names(self, formatter):
        """Test making unique column names"""
        test_cases = [
            (
                ["name", "age", "name", "name", "age"],
                ["name", "age", "name_1", "name_2", "age_1"]
            ),
            (
                ["col", "col", "col"],
                ["col", "col_1", "col_2"]
            ),
            (
                ["unique1", "unique2", "unique3"],
                ["unique1", "unique2", "unique3"]
            ),
            (
                [],
                []
            )
        ]
        for input_list, expected in test_cases:
            assert formatter.make_unique_column_names(input_list) == expected

    def test_normalize_column_names_complete(self, formatter):
        """Test complete column name normalization process"""
        test_cases = [
            (
                ["User Name", "123Age", "User Name", "Email@Address"],
                ["user_name", "_123age", "user_name_1", "email_address"]
            ),
            (
                ["1st Col!", "2nd Col!", "1st Col!"],
                ["_1st_col", "_2nd_col", "_1st_col_1"]
            ),
            (
                ["No#Changes", "no#changes", "NO#CHANGES"],
                ["no_changes", "no_changes_1", "no_changes_2"]
            )
        ]
        for input_list, expected in test_cases:
            assert formatter.normalize_column_names(input_list) == expected

    def test_normalize_column_names_empty_input(self, formatter):
        """Test normalization with empty input"""
        assert formatter.normalize_column_names([]) == []

    def test_normalize_column_names_single_column(self, formatter):
        """Test normalization with a single column"""
        assert formatter.normalize_column_names(["Single Column!"]) == ["single_column"]

    def test_normalize_column_names_whitespace(self, formatter):
        """Test normalization with various whitespace patterns"""
        test_cases = [
            (
                ["  Leading Space", "Trailing Space  ", "  Both  Sides  "],
                ["leading_space", "trailing_space", "both_sides"]
            ),
            (
                ["Multiple    Spaces", "Tab\tSeparated", "\n\rNewlines\n\r"],
                ["multiple_spaces", "tab_separated", "newlines"]
            )
        ]
        for input_list, expected in test_cases:
            assert formatter.normalize_column_names(input_list) == expected

    def test_normalize_column_names_case_sensitivity(self, formatter):
        """Test case sensitivity in normalization"""
        test_cases = [
            (
                ["NAME", "name", "Name", "nAmE"],
                ["name", "name_1", "name_2", "name_3"]
            ),
            (
                ["UPPER", "lower", "Mixed", "mIxEd"],
                ["upper", "lower", "mixed", "mixed_1"]
            )
        ]
        for input_list, expected in test_cases:
            assert formatter.normalize_column_names(input_list) == expected
