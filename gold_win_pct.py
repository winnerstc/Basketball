# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, sum as F_sum, count as F_count,
    when, lit
)

# ----------------------------------------------------------------------
# Spark Session
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("nba_silver_to_gold")
        .enableHiveSupport()
        .getOrCreate()
)

silver_db = "nba_silver"
gold_db = "nba_gold"

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(gold_db))

player_stats_table = "{}.player_statistics_silver".format(silver_db)
games_table = "{}.games_silver".format(silver_db)

# ----------------------------------------------------------------------
# Load Silver tables once
# ----------------------------------------------------------------------
df_stats = spark.table(player_stats_table)
df_games = spark.table(games_table)

# unify playerName column so we don't repeat concat logic everywhere
df_stats = df_stats.withColumn(
    "playerName",
    concat_ws(" ", col("firstName"), col("lastName"))
)

# ----------------------------------------------------------------------
# 1) Player assists per game (Gold)
#    SUM(assists) / COUNT(gameId) AS assists_per_game
# ----------------------------------------------------------------------
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

df_assists_per_game.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_assists_per_game_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# 2) Player rebounds per game (Gold)
#    SUM(reboundstotal) / COUNT(gameId) AS rebounds_per_game
# ----------------------------------------------------------------------
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

df_rebounds_per_game.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_rebounds_per_game_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# 3) Team win percentage by season (Gold)
#    Home + away rows, then aggregate
# ----------------------------------------------------------------------
# Home team rows
game_results_home = (
    df_games
        .select(
            col("year").alias("year"),
            col("homeTeamName").alias("teamId"),
            when(col("homeScore") > col("awayScore"), lit(1)).otherwise(lit(0)).alias("win")
        )
)

# Away team rows
game_results_away = (
    df_games
        .select(
            col("year").alias("year"),
            col("awayTeamName").alias("teamId"),
            when(col("awayScore") > col("homeScore"), lit(1)).otherwise(lit(0)).alias("win")
        )
)

# Use standard union (works on older Spark) – schemas line up by position
game_results = game_results_home.select("year", "teamId", "win").union(
    game_results_away.select("year", "teamId", "win")
)

df_win_pct = (
    game_results
        .groupBy("year", "teamId")
        .agg(
            F_sum("win").alias("wins"),
            F_count("*").alias("games_played")
        )
        .withColumn(
            "win_percentage",
            col("wins").cast("double") / col("games_played")
        )
)

df_win_pct.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.team_win_percentage_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# 4) Player points per game (Gold)
#    SUM(points) / COUNT(gameId) AS points_per_game
# ----------------------------------------------------------------------
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

df_points_per_game.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_points_per_game_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# 5) Total points per player per season (Gold)
#    GROUP BY game_year, personId, playerName
# ----------------------------------------------------------------------
df_total_points_season = (
    df_stats
        .groupBy("game_year", "personId", "playerName")
        .agg(
            F_sum("points").alias("total_points")
        )
)

df_total_points_season.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_total_points_per_season_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# 6) Total assists per player per season (Gold)
#    GROUP BY game_year, personId, playerName
# ----------------------------------------------------------------------
df_total_assists_season = (
    df_stats
        .groupBy("game_year", "personId", "playerName")
        .agg(
            F_sum("assists").alias("total_assists")
        )
)

df_total_assists_season.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_total_assists_per_season_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# Transform to Gold: total rebounds per season per player
# ----------------------------------------------------------------------
df_total_rebounds_season = (
    df_stats
        .groupBy("game_year", "personId", "playerName")
        .agg(
            F_sum("reboundstotal").alias("total_rebounds")
        )
        .orderBy("game_year", col("total_rebounds").desc())
)

# ----------------------------------------------------------------------
# Write to Gold table
# ----------------------------------------------------------------------
df_total_rebounds_season.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_total_rebounds_per_season_gold".format(gold_db)
)


# -------------------------------------------------------------
# 1) Home double-digit wins
# CASE WHEN (homeScore - awayScore) >= 10 THEN 1 ELSE 0 END
# -------------------------------------------------------------
home_dd_wins = (
    df_games
        .select(
            col("season").alias("game_year"),
            col("homeTeamName").alias("team"),
            when(col("homeScore") - col("awayScore") >= 10, lit(1))
                .otherwise(lit(0))
                .alias("dd_win")
        )
)

# -------------------------------------------------------------
# 2) Away double-digit wins
# CASE WHEN (awayScore - homeScore) >= 10 THEN 1 ELSE 0 END
# -------------------------------------------------------------
away_dd_wins = (
    df_games
        .select(
            col("season").alias("game_year"),
            col("awayTeamName").alias("team"),
            when(col("awayScore") - col("homeScore") >= 10, lit(1))
                .otherwise(lit(0))
                .alias("dd_win")
        )
)

# -------------------------------------------------------------
# 3) UNION ALL home + away results
# -------------------------------------------------------------
all_dd_wins = home_dd_wins.unionByName(away_dd_wins)

# -------------------------------------------------------------
# 4) Aggregate by season + team
# -------------------------------------------------------------
df_double_digit_wins = (
    all_dd_wins
        .groupBy("game_year", "team")
        .agg(
            F_sum("dd_win").alias("double_digit_wins")
        )
        .orderBy("game_year", col("double_digit_wins").desc())
)

# -------------------------------------------------------------
# 5) Write to Gold table
# -------------------------------------------------------------
df_double_digit_wins.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.team_double_digit_wins_per_season_gold".format(gold_db)
)


# ----------------------------------------------------------------------
# Done
# ----------------------------------------------------------------------
spark.stop()
