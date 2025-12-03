from gold_tests.utils.checks import (
    assert_non_empty,
    assert_has_columns,
    assert_no_nulls,
    assert_value_positive
)

def test_rebounds_per_game(gold_tables):
    df = gold_tables["rebounds_pg"]

    assert_non_empty(df)
    assert_has_columns(df, ["personId", "playerName", "rebounds_per_game"])
    assert_no_nulls(df, ["personId", "rebounds_per_game"])
    assert_value_positive(df, "rebounds_per_game")
