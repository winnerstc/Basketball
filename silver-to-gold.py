# coding: utf-8
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, concat_ws, sum as F_sum, count as F_count,
#     when, lit
# )

# # ----------------------------------------------------------------------
# # Spark Session
# # ----------------------------------------------------------------------
# spark = (
#     SparkSession.builder
#         .appName("nba_silver_to_gold")
#         .enableHiveSupport()
#         .getOrCreate()
# )

# silver_db = "nba_silver"
# gold_db = "nba_gold"

# spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(gold_db))

# # ----------------------------------------------------------------------
# # NUKE EXISTING GOLD TABLES + HDFS DIRECTORIES
# # ----------------------------------------------------------------------
# gold_tables = [
#     "player_assists_per_game_gold",
#     "player_rebounds_per_game_gold",
#     "team_win_percentage_gold",
#     "player_points_per_game_gold",
#     "player_total_points_per_season_gold",
#     "player_total_assists_per_season_gold",
#     "player_total_rebounds_per_season_gold",
#     "team_double_digit_wins_per_season_gold"
# ]

# # 1) Drop tables in Hive if they exist
# for tbl in gold_tables:
#     spark.sql("DROP TABLE IF EXISTS {}.{}".format(gold_db, tbl))

# # 2) Delete underlying HDFS directories under /tmp/DE011025/NBA/gold/<table_name>
# hadoop_conf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
# base_path = "/tmp/DE011025/NBA/gold"

# for tbl in gold_tables:
#     path_str = "{}/{}".format(base_path, tbl)
#     path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
#     if fs.exists(path):
#         fs.delete(path, True)  # recursive delete

# # ----------------------------------------------------------------------
# # Load Silver tables once
# # ----------------------------------------------------------------------
# player_stats_table = "{}.player_statistics_silver".format(silver_db)
# games_table = "{}.games_silver".format(silver_db)

# df_stats = spark.table(player_stats_table)
# df_games = spark.table(games_table)

# # unify playerName column so we don't repeat concat logic everywhere
# df_stats = df_stats.withColumn(
#     "playerName",
#     concat_ws(" ", col("firstName"), col("lastName"))
# )

# # ----------------------------------------------------------------------
# # 1) Player assists per game (Gold)
# #    SUM(assists) / COUNT(gameId) AS assists_per_game
# # ----------------------------------------------------------------------
# df_assists_per_game = (
#     df_stats
#         .groupBy("personId", "playerName")
#         .agg(
#             F_sum("assists").alias("total_assists"),
#             F_count("gameId").alias("games_played")
#         )
#         .filter(col("games_played") > 0)
#         .withColumn("assists_per_game", col("total_assists") / col("games_played"))
# )

# df_assists_per_game.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.player_assists_per_game_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # 2) Player rebounds per game (Gold)
# #    SUM(reboundstotal) / COUNT(gameId) AS rebounds_per_game
# # ----------------------------------------------------------------------
# df_rebounds_per_game = (
#     df_stats
#         .groupBy("personId", "playerName")
#         .agg(
#             F_sum("reboundstotal").alias("total_rebounds"),
#             F_count("gameId").alias("games_played")
#         )
#         .filter(col("games_played") > 0)
#         .withColumn("rebounds_per_game", col("total_rebounds") / col("games_played"))
# )

# df_rebounds_per_game.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.player_rebounds_per_game_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # 3) Team win percentage by season (Gold)
# #    Home + away rows, then aggregate
# # ----------------------------------------------------------------------
# # Home team rows
# game_results_home = (
#     df_games
#         .select(
#             col("game_year").alias("game_year"),
#             col("homeTeamName").alias("teamId"),
#             when(col("homeScore") > col("awayScore"), lit(1)).otherwise(lit(0)).alias("win")
#         )
# )

# # Away team rows
# game_results_away = (
#     df_games
#         .select(
#             col("game_year").alias("game_year"),
#             col("awayTeamName").alias("teamId"),
#             when(col("awayScore") > col("homeScore"), lit(1)).otherwise(lit(0)).alias("win")
#         )
# )

# # Use standard union (schemas line up by position)
# game_results = game_results_home.select("game_year", "teamId", "win").union(
#     game_results_away.select("game_year", "teamId", "win")
# )

# df_win_pct = (
#     game_results
#         .groupBy("game_year", "teamId")
#         .agg(
#             F_sum("win").alias("wins"),
#             F_count("*").alias("games_played")
#         )
#         .withColumn(
#             "win_percentage",
#             col("wins").cast("double") / col("games_played")
#         )
# )

# df_win_pct.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.team_win_percentage_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # 4) Player points per game (Gold)
# #    SUM(points) / COUNT(gameId) AS points_per_game
# # ----------------------------------------------------------------------
# df_points_per_game = (
#     df_stats
#         .groupBy("personId", "playerName")
#         .agg(
#             F_sum("points").alias("total_points"),
#             F_count("gameId").alias("games_played")
#         )
#         .filter(col("games_played") > 0)
#         .withColumn("points_per_game", col("total_points") / col("games_played"))
# )

# df_points_per_game.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.player_points_per_game_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # 5) Total points per player per season (Gold)
# #    GROUP BY game_year, personId, playerName
# # ----------------------------------------------------------------------
# df_total_points_season = (
#     df_stats
#         .groupBy("game_year", "personId", "playerName")
#         .agg(
#             F_sum("points").alias("total_points")
#         )
# )

# df_total_points_season.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.player_total_points_per_season_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # 6) Total assists per player per season (Gold)
# #    GROUP BY game_year, personId, playerName
# # ----------------------------------------------------------------------
# df_total_assists_season = (
#     df_stats
#         .groupBy("game_year", "personId", "playerName")
#         .agg(
#             F_sum("assists").alias("total_assists")
#         )
# )

# df_total_assists_season.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.player_total_assists_per_season_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # 7) Total rebounds per player per season (Gold)
# # ----------------------------------------------------------------------
# df_total_rebounds_season = (
#     df_stats
#         .groupBy("game_year", "personId", "playerName")
#         .agg(
#             F_sum("reboundstotal").alias("total_rebounds")
#         )
#         .orderBy("game_year", col("total_rebounds").desc())
# )

# df_total_rebounds_season.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.player_total_rebounds_per_season_gold".format(gold_db)
# )

# # -------------------------------------------------------------
# # 8) Team double-digit wins per season (Gold)
# # -------------------------------------------------------------
# home_dd_wins = (
#     df_games
#         .select(
#             col("game_year").alias("game_year"),
#             col("homeTeamName").alias("team"),
#             when(col("homeScore") - col("awayScore") >= 10, lit(1))
#                 .otherwise(lit(0))
#                 .alias("dd_win")
#         )
# )

# away_dd_wins = (
#     df_games
#         .select(
#             col("game_year").alias("game_year"),
#             col("awayTeamName").alias("team"),
#             when(col("awayScore") - col("homeScore") >= 10, lit(1))
#                 .otherwise(lit(0))
#                 .alias("dd_win")
#         )
# )

# all_dd_wins = home_dd_wins.unionByName(away_dd_wins)

# df_double_digit_wins = (
#     all_dd_wins
#         .groupBy("game_year", "team")
#         .agg(
#             F_sum("dd_win").alias("double_digit_wins")
#         )
#         .orderBy("game_year", col("double_digit_wins").desc())
# )

# df_double_digit_wins.write.mode("overwrite").format("parquet").saveAsTable(
#     "{}.team_double_digit_wins_per_season_gold".format(gold_db)
# )

# # ----------------------------------------------------------------------
# # Done
# # ----------------------------------------------------------------------
# spark.stop()

# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, sum as F_sum, count as F_count,
    when, lit
)

# ----------------------------------------------------------------------
# PURE TRANSFORMATION LOGIC (no Spark session / I/O here)
# ----------------------------------------------------------------------
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

    # --------------------------------------------------
    # NEW: HAWKS-ONLY (playerTeamName == "Hawks") – 2024 SEASON
    # 5 Gold tables, all averages per game
    # --------------------------------------------------

    df_hawks_2024 = (
        df_stats
            .filter(
                (col("game_year") == 2024) &
                (col("playerteamName") == "Hawks")
            )
    )
    df_hawks_2024.print()

    # 9) Hawks player points per game (2024)
    df_hawks_points_per_game_2024 = (
        df_hawks_2024
            .groupBy("personId", "playerName")
            .agg(
                F_sum("points").alias("total_points_2024"),
                F_count("gameId").alias("games_played_2024")
            )
            .filter(col("games_played_2024") > 0)
            .withColumn(
                "points_per_game_2024",
                col("total_points_2024") / col("games_played_2024")
            )
    )
    gold_dfs["hawks_player_points_per_game_2024_gold"] = df_hawks_points_per_game_2024


    # 10) Hawks player assists per game (2024)
    df_hawks_assists_per_game_2024 = (
        df_hawks_2024
            .groupBy("personId", "playerName")
            .agg(
                F_sum("assists").alias("total_assists_2024"),
                F_count("gameId").alias("games_played_2024")
            )
            .filter(col("games_played_2024") > 0)
            .withColumn(
                "assists_per_game_2024",
                col("total_assists_2024") / col("games_played_2024")
            )
    )
    gold_dfs["hawks_player_assists_per_game_2024_gold"] = df_hawks_assists_per_game_2024


    # 11) Hawks player blocks per game (2024)
    df_hawks_blocks_per_game_2024 = (
        df_hawks_2024
            .groupBy("personId", "playerName")
            .agg(
                F_sum("blocks").alias("total_blocks_2024"),
                F_count("gameId").alias("games_played_2024")
            )
            .filter(col("games_played_2024") > 0)
            .withColumn(
                "blocks_per_game_2024",
                col("total_blocks_2024") / col("games_played_2024")
            )
    )
    gold_dfs["hawks_player_blocks_per_game_2024_gold"] = df_hawks_blocks_per_game_2024


    # 12) Hawks player steals per game (2024)
    df_hawks_steals_per_game_2024 = (
        df_hawks_2024
            .groupBy("personId", "playerName")
            .agg(
                F_sum("steals").alias("total_steals_2024"),
                F_count("gameId").alias("games_played_2024")
            )
            .filter(col("games_played_2024") > 0)
            .withColumn(
                "steals_per_game_2024",
                col("total_steals_2024") / col("games_played_2024")
            )
    )
    gold_dfs["hawks_player_steals_per_game_2024_gold"] = df_hawks_steals_per_game_2024


    # 13) Hawks combined per-game summary (2024)
    df_hawks_summary_per_game_2024 = (
        df_hawks_2024
            .groupBy("personId", "playerName")
            .agg(
                F_sum("points").alias("total_points_2024"),
                F_sum("assists").alias("total_assists_2024"),
                F_sum("blocks").alias("total_blocks_2024"),
                F_sum("steals").alias("total_steals_2024"),
                F_count("gameId").alias("games_played_2024")
            )
            .filter(col("games_played_2024") > 0)
            .withColumn("points_per_game_2024", col("total_points_2024") / col("games_played_2024")) \
            .withColumn("assists_per_game_2024", col("total_assists_2024") / col("games_played_2024")) \
            .withColumn("blocks_per_game_2024", col("total_blocks_2024") / col("games_played_2024")) \
            .withColumn("steals_per_game_2024", col("total_steals_2024") / col("games_played_2024"))
    )
    gold_dfs["hawks_player_summary_per_game_2024_gold"] = df_hawks_summary_per_game_2024


    return gold_dfs


# ----------------------------------------------------------------------
# SIDE-EFFECT / I/O HELPERS
# ----------------------------------------------------------------------
def nuke_gold_layer(spark, gold_db, gold_tables, base_path):
    """
    Drop existing Gold tables and delete their HDFS directories.
    base_path example: "/tmp/DE011025/NBA/gold"
    """
    # Drop tables
    for tbl in gold_tables:
        spark.sql("DROP TABLE IF EXISTS {}.{}".format(gold_db, tbl))

    # Delete underlying HDFS directories
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    for tbl in gold_tables:
        path_str = "{}/{}".format(base_path, tbl)
        path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
        if fs.exists(path):
            fs.delete(path, True)  # recursive delete


def main(spark, silver_db, gold_db, gold_base_path):
    """
    Main ETL driver:
      - loads silver tables
      - computes all gold DataFrames
      - nukes existing gold tables/dirs
      - writes gold tables
    """

    # Ensure Gold DB exists
    spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(gold_db))

    # Load Silver tables once
    player_stats_table = "{}.player_statistics_silver".format(silver_db)
    games_table = "{}.games_silver".format(silver_db)

    df_stats = spark.table(player_stats_table)
    df_games = spark.table(games_table)

    # Compute all Gold-layer DataFrames
    gold_dfs = compute_gold_tables(df_stats, df_games)

    gold_tables = list(gold_dfs.keys())

    # Nuke existing Gold
    nuke_gold_layer(spark, gold_db, gold_tables, gold_base_path)

    # Write out each Gold DF as a Hive table (Parquet)
    for tbl_name, df in gold_dfs.items():
        full_name = "{}.{}".format(gold_db, tbl_name)
        (
            df.write
              .mode("overwrite")
              .format("parquet")
              .saveAsTable(full_name)
        )
        print("Wrote Gold table:", full_name)


# ----------------------------------------------------------------------
# ENTRYPOINT
# ----------------------------------------------------------------------
if __name__ == "__main__":
    print("Starting nba_silver_to_gold job...")

    spark = (
        SparkSession.builder
            .appName("nba_silver_to_gold")
            .enableHiveSupport()
            .getOrCreate()
    )

    # Params (can be wired to args/env later)
    silver_db = "nba_silver"
    gold_db = "nba_gold"
    gold_base_path = "/tmp/DE011025/NBA/gold"

    try:
        main(spark, silver_db, gold_db, gold_base_path)
    finally:
        spark.stop()
        print("Spark session stopped.")
