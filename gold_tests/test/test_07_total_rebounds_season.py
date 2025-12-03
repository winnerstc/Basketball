from gold_tests.utils.checks import (
    assert_non_empty,
    assert_has_columns,
    assert_no_nulls,
    assert_value_positive
)
def test_total_rebounds_season(gold_tables):
    df = gold_tables["reb_season"]

    assert_non_empty(df)
    assert_has_columns(df, ["game_year", "personId", "total_rebounds"])
    assert_value_positive(df, "total_rebounds")
