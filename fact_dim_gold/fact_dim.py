# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("silver_to_gold_nba").getOrCreate()

# ----------------------------------------------------------
# HDFS Silver Paths
# ----------------------------------------------------------
games_path = "/tmp/DE011025/NBA/silver/games"
players_path = "/tmp/DE011025/NBA/silver/players"
player_stats_path = "/tmp/DE011025/NBA/silver/player_statistics"
team_histories_path = "/tmp/DE011025/NBA/silver/team_histories"
team_stats_path = "/tmp/DE011025/NBA/silver/team_statistics"

# ----------------------------------------------------------
# HDFS Gold Paths
# ----------------------------------------------------------
fact_path = "/tmp/DE011025/NBA/gold/fact_nba_stats"
dim_players_path = "/tmp/DE011025/NBA/gold/dim_players"
dim_teams_path = "/tmp/DE011025/NBA/gold/dim_teams"
dim_games_path = "/tmp/DE011025/NBA/gold/dim_games"
dim_dates_path = "/tmp/DE011025/NBA/gold/dim_dates"

# ----------------------------------------------------------
# READ SILVER TABLES
# ----------------------------------------------------------
games = spark.read.parquet(games_path)
players = spark.read.parquet(players_path)
player_stats = spark.read.parquet(player_stats_path)
team_histories = spark.read.parquet(team_histories_path)
team_stats = spark.read.parquet(team_stats_path)

# ----------------------------------------------------------
# DIMENSIONS
# ----------------------------------------------------------

# DIM PLAYERS
dim_players = players.select(
    "personid", "firstname", "lastname",
    "country", "height", "bodyweight",
    "age_years", "primary_position"
)
dim_players.write.mode("overwrite").parquet(dim_players_path)

# DIM TEAMS
dim_teams = team_histories.select(
    "teamid", "teamname", "teamcity",
    "team_full_name", "league"
)
dim_teams.write.mode("overwrite").parquet(dim_teams_path)

# DIM GAMES
dim_games = games.select(
    "gameid", "gametype", "attendance",
    "arenaid", "home_win", "away_win",
    "homescore", "awayscore", "winner"
)
dim_games.write.mode("overwrite").parquet(dim_games_path)

# DIM DATES
dim_dates = games.select(
    "game_date",
    "game_year",
    "game_month",
    weekofyear("game_date").alias("week"),
    quarter("game_date").alias("quarter_of_year")
)
dim_dates.write.mode("overwrite").parquet(dim_dates_path)

# ----------------------------------------------------------
# FACT TABLE
# ----------------------------------------------------------

# Join 1: player stats + team stats (teamid + gameid)
fact = (
    player_stats.alias("ps")
    .join(team_stats.alias("ts"),
          (col("ps.gameid") == col("ts.gameid")) &
          (col("ps.playerteamname") == col("ts.teamname")),
          "left")
    .join(games.alias("g"), "gameid", "left")
    .select(
        # Keys
        col("ps.gameid"),
        col("ps.personid"),
        col("ps.playerteamname").alias("teamname"),
        col("ps.playerteamcity").alias("teamcity"),
        col("ps.opponentteamname"),
        col("ps.opponentteamcity"),

        # Date
        col("ps.game_date"),
        col("ps.game_year"),
        col("ps.game_month"),

        # Basic KPIs
        col("ps.home"),
        col("ps.win"),
        col("ps.points"),
        col("ps.assists"),
        (col("ps.reboundsoffensive") + col("ps.reboundsdefensive")).alias("rebounds"),
        col("ps.steals"),
        col("ps.blocks"),
        col("ps.numminutes"),

        # Team stats KPIs
        col("ts.q1points"),
        col("ts.q4points"),
        col("ts.benchpoints"),
        col("ts.biggestlead"),
        col("ts.biggestscoringrun"),
        col("ts.leadchanges"),
        col("ts.score_diff"),
        col("ts.shooting_efficiency"),

        # Player shooting percentages
        col("ps.fieldgoalspercentage"),
        col("ps.threepointerspercentage"),
        col("ps.freethrowspercentage")
    )
)

fact.write.mode("overwrite").parquet(fact_path)

print("SILVER → GOLD Load Complete!")