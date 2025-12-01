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

# ----------------------------------------------------------------------
# NUKE EXISTING GOLD TABLES + HDFS DIRECTORIES
# ----------------------------------------------------------------------
gold_tables = [
    "player_assists_per_game_gold",
    "player_rebounds_per_game_gold",
    "team_win_percentage_gold",
    "player_points_per_game_gold",
    "player_total_points_per_season_gold",
    "player_total_assists_per_season_gold",
    "player_total_rebounds_per_season_gold",
    "team_double_digit_wins_per_season_gold"
]

# 1) Drop tables in Hive if they exist
for tbl in gold_tables:
    spark.sql("DROP TABLE IF EXISTS {}.{}".format(gold_db, tbl))

# 2) Delete underlying HDFS directories under /tmp/DE011025/NBA/gold/<table_name>
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
base_path = "/tmp/DE011025/NBA/gold"

for tbl in gold_tables:
    path_str = "{}/{}".format(base_path, tbl)
    path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    if fs.exists(path):
        fs.delete(path, True)  # recursive delete

# ----------------------------------------------------------------------
# Load Silver tables once
# ----------------------------------------------------------------------
player_stats_table = "{}.player_statistics_silver".format(silver_db)
games_table = "{}.games_silver".format(silver_db)

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

# Use standard union (schemas line up by position)
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
# 7) Total rebounds per player per season (Gold)
# ----------------------------------------------------------------------
df_total_rebounds_season = (
    df_stats
        .groupBy("game_year", "personId", "playerName")
        .agg(
            F_sum("reboundstotal").alias("total_rebounds")
        )
        .orderBy("game_year", col("total_rebounds").desc())
)

df_total_rebounds_season.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.player_total_rebounds_per_season_gold".format(gold_db)
)

# -------------------------------------------------------------
# 8) Team double-digit wins per season (Gold)
# -------------------------------------------------------------
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
        .agg(
            F_sum("dd_win").alias("double_digit_wins")
        )
        .orderBy("game_year", col("double_digit_wins").desc())
)

df_double_digit_wins.write.mode("overwrite").format("parquet").saveAsTable(
    "{}.team_double_digit_wins_per_season_gold".format(gold_db)
)

# ----------------------------------------------------------------------
# Done
# ----------------------------------------------------------------------
spark.stop()
