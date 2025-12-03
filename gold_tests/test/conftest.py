import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
            .appName("gold_layer_tests")
            .enableHiveSupport()
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# @pytest.fixture(scope="session")
# def gold_tables(spark):
#     gold_db = "nba_gold"

#     tables = {
#         "assists_pg": spark.table(f"{gold_db}.player_assists_per_game_gold"),
#         "rebounds_pg": spark.table(f"{gold_db}.player_rebounds_per_game_gold"),
#         "win_pct": spark.table(f"{gold_db}.team_win_percentage_gold"),
#         "points_pg": spark.table(f"{gold_db}.player_points_per_game_gold"),
#         "pts_season": spark.table(f"{gold_db}.player_total_points_per_season_gold"),
#         "ast_season": spark.table(f"{gold_db}.player_total_assists_per_season_gold"),
#         "reb_season": spark.table(f"{gold_db}.player_total_rebounds_per_season_gold"),
#         "dd_wins": spark.table(f"{gold_db}.team_double_digit_wins_per_season_gold"),
#     }
#     return tables