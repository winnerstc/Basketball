CREATE DATABASE IF NOT EXISTS nba_silver;
USE nba_silver;

CREATE EXTERNAL TABLE IF NOT EXISTS games_silver (
    gameid BIGINT,
    game_ts TIMESTAMP,
    game_date DATE,
    game_year INT,
    game_month INT,
    
    hometeamcity STRING,
    hometeamname STRING,
    hometeamid BIGINT,

    awayteamcity STRING,
    awayteamname STRING,
    awayteamid BIGINT,

    homescore INT,
    awayscore INT,
    winner BIGINT,

    gametype STRING,
    attendance INT,
    arenaid INT,

    year INT,

    home_win INT,
    away_win INT,
    score_diff INT,

    silver_ingest_ts TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///tmp/DE011025/NBA/silver/games';