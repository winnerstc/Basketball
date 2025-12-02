# coding: utf-8
import os

# FORCE PYTHON 3.6 (CDH default)
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, quarter, weekofyear,
    date_format, lit, sequence, explode, to_date, when,
    row_number
)
from pyspark.sql.window import Window

# ======================================================
# Spark Session
# ======================================================
spark = SparkSession.builder.appName("NBA_Gold_Model").getOrCreate()

# ======================================================
# Paths
# ======================================================
silver_games = "/tmp/DE011025/NBA/silver/games"
silver_players = "/tmp/DE011025/NBA/silver/player_statistics"
silver_player_dim = "/tmp/DE011025/NBA/silver/players"
silver_teams = "/tmp/DE011025/NBA/silver/team_histories"
silver_team_stats = "/tmp/DE011025/NBA/silver/team_statistics"

gold_fact = "/tmp/DE011025/NBA/gold/fact_nba_stats"
gold_dim_games = "/tmp/DE011025/NBA/gold/dim_games"
gold_dim_players = "/tmp/DE011025/NBA/gold/dim_players"
gold_dim_teams = "/tmp/DE011025/NBA/gold/dim_teams"
gold_dim_date = "/tmp/DE011025/NBA/gold/dim_dates"

# ======================================================
# Load Silver Parquet Files
# ======================================================
df_games = spark.read.parquet(silver_games)
df_player_stats = spark.read.parquet(silver_players)
df_players = spark.read.parquet(silver_player_dim)
df_teams = spark.read.parquet(silver_teams)
df_team_stats = spark.read.parquet(silver_team_stats)

# Ensure bigint for joins
df_games = df_games.withColumn("gameid", col("gameid").cast("bigint"))
df_player_stats = df_player_stats.withColumn("gameid", col("gameid").cast("bigint"))
df_team_stats = df_team_stats.withColumn("gameid", col("gameid").cast("bigint"))

# ======================================================
# DIMENSION: GAMES
# ======================================================
dim_games = df_games.select(
    "gameid", "game_date", "game_year", "game_month",
    "hometeamcity", "hometeamname", "hometeamid",
    "awayteamcity", "awayteamname", "awayteamid",
    "gametype", "attendance", "arenaid"
)

dim_games.write.mode("overwrite").parquet(gold_dim_games)

# ======================================================
# DIMENSION: PLAYERS
# ======================================================
dim_players = df_players.select(
    "personid", "firstname", "lastname",
    "birth_year", "age_years",
    "primary_position", "country"
)

dim_players.write.mode("overwrite").parquet(gold_dim_players)

# ======================================================
# DIMENSION: TEAMS
# ======================================================
dim_teams = df_teams.select(
    "teamid", "teamcity", "teamname", "teamabbrev",
    "league", "team_full_name", "active_flag"
)

dim_teams.write.mode("overwrite").parquet(gold_dim_teams)

# ======================================================
# DIMENSION: DATE
# ======================================================
min_date = df_games.agg({"game_date": "min"}).collect()[0][0]
max_date = df_games.agg({"game_date": "max"}).collect()[0][0]

date_range = spark.createDataFrame([(min_date, max_date)], ["start", "end"]) \
    .select(explode(sequence(col("start"), col("end"))).alias("date"))

dim_dates = (
    date_range
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("week", weekofyear(col("date")))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
)

dim_dates.write.mode("overwrite").parquet(gold_dim_date)

# ======================================================
# FIX: DEDUP TEAM STATS (ONE ROW PER GAMEID)
# ======================================================
window_game = Window.partitionBy("gameid").orderBy(col("teamid"))

df_team_stats_clean = (
    df_team_stats
        .withColumn("rn", row_number().over(window_game))
        .filter(col("rn") == 1)
        .drop("rn")
)

# ======================================================
# FACT TABLE (NO DUPLICATES)
# ======================================================
fact = (
    df_player_stats.alias("ps")
        .join(df_games.alias("g"), ["gameid"], "inner")
        .join(df_team_stats_clean.alias("ts"), ["gameid"], "left")
        .select(
            col("ps.gameid"),
            col("ps.personid"),
            col("ps.playerteamname").alias("teamname"),
            col("ps.playerteamcity").alias("teamcity"),
            col("ps.opponentteamname").alias("opponentteamname"),
            col("ps.opponentteamcity").alias("opponentteamcity"),
            col("g.game_date"),
            col("g.game_year"),
            col("g.game_month"),
            col("ps.home"),
            col("ps.win"),
            col("ps.points"),
            col("ps.assists"),
            col("ps.reboundstotal").alias("rebounds"),
            col("ps.steals"),
            col("ps.blocks"),
            col("ps.numminutes"),
            col("ts.q1points"),
            col("ts.q4points"),
            col("ts.benchpoints"),
            col("ts.biggestlead"),
            col("ts.biggestscoringrun"),
            col("ts.leadchanges"),
            col("ts.score_diff"),
            col("ts.shooting_efficiency"),
            col("ps.fieldgoalspercentage"),
            col("ps.threepointerspercentage"),
            col("ps.freethrowspercentage")
        )
)

fact.write.mode("overwrite").parquet(gold_fact)

print(" GOLD Layer successfully created! (Duplicates removed, Fact table cleaned)")