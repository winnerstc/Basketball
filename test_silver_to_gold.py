import unittest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
# Note: assert_df_equal is typically available in pyspark.testing
# In some environments, it might be part of an older or external module, 
# but for modern PySpark, the import below is correct.

# --- Start of the actual code block for the user's function for testing ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, sum as F_sum, count as F_count,
    when, lit
)
from pyspark.sql.types import IntegerType, LongType, DoubleType

def _normalize_int_type(dt):
    """Treat IntegerType and LongType as the same logical type for testing."""
    if isinstance(dt, IntegerType):
        return LongType()
    return dt

def assert_df_equal(actual_df, expected_df, sort_cols=None, **_ignored_kwargs):
    """
    Simple DataFrame equality helper for tests.

    - Optionally sorts by sort_cols
    - Compares column names
    - Compares data types loosely (int vs bigint treated as same)
    - Ignores nullable flag
    - Compares row content exactly
    """
    if sort_cols:
        actual_df = actual_df.orderBy(*sort_cols)
        expected_df = expected_df.orderBy(*sort_cols)

    # --- Compare schema (names + compatible types) ---
    a_fields = actual_df.schema.fields
    e_fields = expected_df.schema.fields

    assert len(a_fields) == len(e_fields), (
        f"Different number of columns:\n"
        f"Actual:   {[f.name for f in a_fields]}\n"
        f"Expected: {[f.name for f in e_fields]}"
    )

    for af, ef in zip(a_fields, e_fields):
        assert af.name == ef.name, (
            f"Column name mismatch:\n"
            f"Actual:   {af.name}\n"
            f"Expected: {ef.name}"
        )

        a_type = _normalize_int_type(af.dataType)
        e_type = _normalize_int_type(ef.dataType)

        assert type(a_type) == type(e_type), (
            f"Column type mismatch for '{af.name}':\n"
            f"Actual:   {af.dataType}\n"
            f"Expected: {ef.dataType}"
        )

    # --- Compare data ---
    actual_rows = actual_df.collect()
    expected_rows = expected_df.collect()

    assert actual_rows == expected_rows, (
        f"Data mismatch:\n"
        f"Actual:   {actual_rows}\n"
        f"Expected: {expected_rows}"
    )

def compute_gold_tables(df_stats, df_games):
    """
    Take Silver-layer stats/games DataFrames and return a dict of
    gold-layer DataFrames keyed by table name.
    """

    # unify playerName column so we don't repeat concat logic everywhere
    df_stats = df_stats.withColumn(
        "playerName",
        concat_ws(" ", col("firstName"), col("lastName"))
    )

    gold_dfs = {}

    # 1) Player assists per game
    df_assists_per_game = (
        df_stats
            .groupBy("personId", "playerName")
            .agg(
                F_sum("assists").alias("total_assists"),
                F_count("gameId").alias("games_played")
            )
            .filter(col("games_played") > 0)
            .withColumn("assists_per_game", col("total_assists") / col("games_played"))
    )
    gold_dfs["player_assists_per_game_gold"] = df_assists_per_game

    # 2) Player rebounds per game
    df_rebounds_per_game = (
        df_stats
            .groupBy("personId", "playerName")
            .agg(
                F_sum("reboundstotal").alias("total_rebounds"),
                F_count("gameId").alias("games_played")
            )
            .filter(col("games_played") > 0)
            .withColumn("rebounds_per_game", col("total_rebounds") / col("games_played"))
    )
    gold_dfs["player_rebounds_per_game_gold"] = df_rebounds_per_game

    # 3) Team win percentage by season

    # Home team rows
    game_results_home = (
        df_games
            .select(
                col("game_year").alias("game_year"),
                col("homeTeamName").alias("teamId"),
                when(col("homeScore") > col("awayScore"), lit(1)).otherwise(lit(0)).alias("win")
            )
    )

    # Away team rows
    game_results_away = (
        df_games
            .select(
                col("game_year").alias("game_year"),
                col("awayTeamName").alias("teamId"),
                when(col("awayScore") > col("homeScore"), lit(1)).otherwise(lit(0)).alias("win")
            )
    )

    game_results = game_results_home.select("game_year", "teamId", "win").union(
        game_results_away.select("game_year", "teamId", "win")
    )

    df_win_pct = (
        game_results
            .groupBy("game_year", "teamId")
            .agg(
                F_sum("win").alias("wins"),
                F_count("*").alias("games_played")
            )
            .withColumn(
                "win_percentage",
                col("wins").cast("double") / col("games_played")
            )
    )
    gold_dfs["team_win_percentage_gold"] = df_win_pct

    # 4) Player points per game
    df_points_per_game = (
        df_stats
            .groupBy("personId", "playerName")
            .agg(
                F_sum("points").alias("total_points"),
                F_count("gameId").alias("games_played")
            )
            .filter(col("games_played") > 0)
            .withColumn("points_per_game", col("total_points") / col("games_played"))
    )
    gold_dfs["player_points_per_game_gold"] = df_points_per_game

    # 5) Total points per player per season
    df_total_points_season = (
        df_stats
            .groupBy("game_year", "personId", "playerName")
            .agg(F_sum("points").alias("total_points"))
    )
    gold_dfs["player_total_points_per_season_gold"] = df_total_points_season

    # 6) Total assists per player per season
    df_total_assists_season = (
        df_stats
            .groupBy("game_year", "personId", "playerName")
            .agg(F_sum("assists").alias("total_assists"))
    )
    gold_dfs["player_total_assists_per_season_gold"] = df_total_assists_season

    # 7) Total rebounds per player per season
    df_total_rebounds_season = (
        df_stats
            .groupBy("game_year", "personId", "playerName")
            .agg(F_sum("reboundstotal").alias("total_rebounds"))
            .orderBy("game_year", col("total_rebounds").desc())
    )
    gold_dfs["player_total_rebounds_per_season_gold"] = df_total_rebounds_season

    # 8) Team double-digit wins per season
    home_dd_wins = (
        df_games
            .select(
                col("game_year").alias("game_year"),
                col("homeTeamName").alias("team"),
                when(col("homeScore") - col("awayScore") >= 10, lit(1))
                    .otherwise(lit(0))
                    .alias("dd_win")
            )
    )

    away_dd_wins = (
        df_games
            .select(
                col("game_year").alias("game_year"),
                col("awayTeamName").alias("team"),
                when(col("awayScore") - col("homeScore") >= 10, lit(1))
                    .otherwise(lit(0))
                    .alias("dd_win")
            )
    )

    all_dd_wins = home_dd_wins.unionByName(away_dd_wins)

    df_double_digit_wins = (
        all_dd_wins
            .groupBy("game_year", "team")
            .agg(F_sum("dd_win").alias("double_digit_wins"))
            .orderBy("game_year", col("double_digit_wins").desc())
    )
    gold_dfs["team_double_digit_wins_per_season_gold"] = df_double_digit_wins

    return gold_dfs
# ----------------------------------------------------------------------


class GoldLayerTransformationsTest(unittest.TestCase):
    """
    Test case for the compute_gold_tables function using PySpark.
    """
    @classmethod
    def setUpClass(cls):
        """Set up the Spark session before running any tests."""
        # Use a single local thread for testing simplicity and speed
        cls.spark = (
            SparkSession.builder
                .appName("GoldLayerTest")
                .master("local[1]") 
                .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after all tests are run."""
        cls.spark.stop()

    def create_mock_dataframes(self):
        """Create mock input DataFrames: df_stats and df_games."""
        spark = self.spark

        # Schema for df_stats
        stats_schema = StructType([
            StructField("gameId", StringType(), False),
            StructField("game_year", IntegerType(), False),
            StructField("personId", IntegerType(), False),
            StructField("firstName", StringType(), False),
            StructField("lastName", StringType(), False),
            StructField("points", IntegerType(), False),
            StructField("assists", IntegerType(), False),
            StructField("reboundstotal", IntegerType(), False)
        ])
        
        # Mock data for player_statistics_silver (df_stats)
        stats_data = [
            ("G1", 2024, 101, "Lebron", "James", 25, 10, 8),
            ("G2", 2024, 101, "Lebron", "James", 30, 5, 12), 
            ("G3", 2023, 101, "Lebron", "James", 15, 8, 5),  
            ("G1", 2024, 102, "Anthony", "Davis", 18, 2, 10),
            ("G2", 2024, 102, "Anthony", "Davis", 22, 1, 9),  
            ("G4", 2024, 103, "Rui", "Hachimura", 0, 0, 0),    
        ]

        df_stats = spark.createDataFrame(stats_data, schema=stats_schema)

        # Schema for df_games
        games_schema = StructType([
            StructField("gameId", StringType(), False),
            StructField("game_year", IntegerType(), False),
            StructField("homeTeamName", StringType(), False),
            StructField("awayTeamName", StringType(), False),
            StructField("homeScore", IntegerType(), False),
            StructField("awayScore", IntegerType(), False)
        ])

        # Mock data for games_silver (df_games)
        games_data = [
            # LAL vs BOS: LAL wins by 10 (DD)
            ("G1", 2024, "LAL", "BOS", 120, 110), 
            # BOS vs MIA: MIA wins by 5 (Not DD)
            ("G2", 2024, "BOS", "MIA", 100, 105), 
            # LAL vs MIA: MIA wins by 10 (DD)
            ("G3", 2023, "LAL", "MIA", 90, 100), 
            # DEN vs LAL: DEN wins by 20 (DD)
            ("G4", 2024, "DEN", "LAL", 115, 95)
        ]

        df_games = spark.createDataFrame(games_data, schema=games_schema)

        return df_stats, df_games

    def test_compute_gold_tables(self):
        """Test all 8 gold tables computed by the function."""
        spark = self.spark
        df_stats, df_games = self.create_mock_dataframes()
        gold_dfs = compute_gold_tables(df_stats, df_games)

        # Ensure all 8 tables are present
        expected_keys = {
            "player_assists_per_game_gold", "player_rebounds_per_game_gold",
            "team_win_percentage_gold", "player_points_per_game_gold",
            "player_total_points_per_season_gold", "player_total_assists_per_season_gold",
            "player_total_rebounds_per_season_gold", "team_double_digit_wins_per_season_gold"
        }
        self.assertEqual(set(gold_dfs.keys()), expected_keys)

        # --- Test 1: Player assists per game (overall) ---
        # Lebron: 23A / 3 games = 7.666...
        # AD: 3A / 2 games = 1.5
        expected_assists = spark.createDataFrame([
            (101, "Lebron James", 23, 3, 23/3),
            (102, "Anthony Davis", 3, 2, 1.5)
        ], ["personId", "playerName", "total_assists", "games_played", "assists_per_game"])
        
        assert_df_equal(gold_dfs["player_assists_per_game_gold"].orderBy("personId"),
                        expected_assists.withColumn("assists_per_game", col("assists_per_game").cast(DoubleType())),
                        check_all_struct=False)
        
        # --- Test 2: Player rebounds per game (overall) ---
        # Lebron: 25R / 3 games = 8.333...
        # AD: 19R / 2 games = 9.5
        expected_rebounds = spark.createDataFrame([
            (101, "Lebron James", 25, 3, 25/3),
            (102, "Anthony Davis", 19, 2, 9.5)
        ], ["personId", "playerName", "total_rebounds", "games_played", "rebounds_per_game"])
        
        assert_df_equal(gold_dfs["player_rebounds_per_game_gold"].orderBy("personId"),
                        expected_rebounds.withColumn("rebounds_per_game", col("rebounds_per_game").cast(DoubleType())),
                        check_all_struct=False)

        # --- Test 4: Player points per game (overall) ---
        # Lebron: 70P / 3 games = 23.333...
        # AD: 40P / 2 games = 20.0
        expected_points_pg = spark.createDataFrame([
            (101, "Lebron James", 70, 3, 70/3),
            (102, "Anthony Davis", 40, 2, 20.0)
        ], ["personId", "playerName", "total_points", "games_played", "points_per_game"])
        
        assert_df_equal(gold_dfs["player_points_per_game_gold"].orderBy("personId"),
                        expected_points_pg.withColumn("points_per_game", col("points_per_game").cast(DoubleType())),
                        check_all_struct=False)
                        
        # --- Test 5: Total points per player per season ---
        expected_total_points = spark.createDataFrame([
            (2024, 101, "Lebron James", 55), 
            (2023, 101, "Lebron James", 15),  
            (2024, 102, "Anthony Davis", 40), 
            (2024, 103, "Rui Hachimura", 0),  
        ], ["game_year", "personId", "playerName", "total_points"])

        assert_df_equal(gold_dfs["player_total_points_per_season_gold"].orderBy("game_year", "personId"),
                        expected_total_points,
                        check_all_struct=False)

        # --- Test 6: Total assists per player per season ---
        expected_total_assists = spark.createDataFrame([
            (2024, 101, "Lebron James", 15), 
            (2023, 101, "Lebron James", 8),  
            (2024, 102, "Anthony Davis", 3),  
            (2024, 103, "Rui Hachimura", 0),  
        ], ["game_year", "personId", "playerName", "total_assists"])

        assert_df_equal(gold_dfs["player_total_assists_per_season_gold"].orderBy("game_year", "personId"),
                        expected_total_assists,
                        check_all_struct=False)

        # --- Test 7: Total rebounds per player per season ---
        # Output is ordered by 'game_year' and 'total_rebounds' descending.
        expected_total_rebounds = spark.createDataFrame([
            (2024, 101, "Lebron James", 20), 
            (2024, 102, "Anthony Davis", 19), 
            (2024, 103, "Rui Hachimura", 0),  
            (2023, 101, "Lebron James", 5)    
        ], ["game_year", "personId", "playerName", "total_rebounds"])
        
        actual_df_rebounds = gold_dfs["player_total_rebounds_per_season_gold"].select("game_year", "personId", "playerName", "total_rebounds")
        
        assert_df_equal(actual_df_rebounds,
                        expected_total_rebounds,
                        check_all_struct=False)
                        
        # --- Test 3: Team win percentage by season ---
        # LAL 2024: 1 win, 2 games -> 0.5
        # BOS 2024: 0 wins, 2 games -> 0.0
        # MIA 2024: 1 win, 1 game -> 1.0 (Win vs BOS)
        # DEN 2024: 1 win, 1 game -> 1.0 (Win vs LAL)
        # LAL 2023: 0 wins, 1 game -> 0.0
        # MIA 2023: 1 win, 1 game -> 1.0 (Win vs LAL)
        expected_win_pct = spark.createDataFrame([
            (2024, "BOS", 0, 2, 0.0),
            (2024, "DEN", 1, 1, 1.0),
            (2024, "LAL", 1, 2, 0.5),
            (2024, "MIA", 1, 1, 1.0),
            (2023, "LAL", 0, 1, 0.0),
            (2023, "MIA", 1, 1, 1.0)
        ], ["game_year", "teamId", "wins", "games_played", "win_percentage"])

        assert_df_equal(gold_dfs["team_win_percentage_gold"].orderBy("game_year", "teamId"),
                        expected_win_pct.withColumn("win_percentage", col("win_percentage").cast(DoubleType())),
                        check_all_struct=False)

        # --- Test 8: Team double-digit wins per season ---
        # G1 (2024): LAL (+10) -> DD Win
        # G2 (2024): MIA (+5) -> Not DD
        # G3 (2023): MIA (+10) -> DD Win
        # G4 (2024): DEN (+20) -> DD Win
        
        # 2024 DD Wins: DEN=1, LAL=1, BOS=0, MIA=0
        # 2023 DD Wins: MIA=1, LAL=0
        expected_dd_wins = spark.createDataFrame([
            (2024, "DEN", 1), 
            (2024, "LAL", 1), 
            (2024, "BOS", 0),
            (2024, "MIA", 0),
            (2023, "MIA", 1), 
            (2023, "LAL", 0)
        ], ["game_year", "team", "double_digit_wins"])
        
        # Check against actual output, respecting its ordering
        actual_df_dd_wins = gold_dfs["team_double_digit_wins_per_season_gold"].select("game_year", "team", "double_digit_wins")

        assert_df_equal(actual_df_dd_wins,
                        expected_dd_wins.withColumn("double_digit_wins", col("double_digit_wins").cast(IntegerType())),
                        check_all_struct=False)

# This is the line that was missing and prevents the script from running the tests!
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)