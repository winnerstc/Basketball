# from gold_tests.utils.checks import (
#     assert_non_empty,
#     assert_has_columns,
#     assert_no_nulls,
#     assert_value_positive
# )

# def test_points_per_game(gold_tables):
#     df = gold_tables["points_pg"]

#     assert_non_empty(df)
#     assert_has_columns(df, ["personId", "playerName", "points_per_game"])
#     assert_no_nulls(df, ["personId", "points_per_game"])
#     assert_value_positive(df, "points_per_game")
