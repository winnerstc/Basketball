# from gold_tests.utils.checks import (
#     assert_non_empty,
#     assert_has_columns,
#     assert_no_nulls,
#     assert_value_positive
# )
# from pyspark.sql.functions import col

# def test_double_digit_wins(gold_tables):
#     df = gold_tables["dd_wins"]

#     assert_non_empty(df)
#     assert_has_columns(df, ["game_year", "team", "double_digit_wins"])
#     assert_value_positive(df, "double_digit_wins")
