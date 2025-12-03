# from gold_tests.utils.checks import (
#     assert_non_empty,
#     assert_has_columns,
#     assert_no_nulls,
#     assert_value_positive
# )

# def test_assists_per_game(gold_tables):
#     df = gold_tables["assists_pg"]

#     assert_non_empty(df)
#     assert_has_columns(df, ["personId", "playerName", "assists_per_game"])

#     # No nulls in key columns
#     assert_no_nulls(df, ["personId", "assists_per_game"])

#     # assists_per_game should be >= 0
#     assert_value_positive(df, "assists_per_game")
