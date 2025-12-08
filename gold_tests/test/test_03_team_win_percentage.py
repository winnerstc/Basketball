# from gold_tests.utils.checks import (
#     assert_non_empty,
#     assert_has_columns,
#     assert_no_nulls,
#     assert_value_positive
# )
# from pyspark.sql.functions import col

# def test_team_win_percentage(gold_tables):
#     df = gold_tables["win_pct"]

#     assert_non_empty(df)
#     assert_has_columns(df, ["year", "teamId", "win_percentage"])

#     # Check win_percentage is between 0 and 1
#     bad = df.filter((col("win_percentage") < 0) | (col("win_percentage") > 1)).count()
#     assert bad == 0, "Invalid win_percentage values"
