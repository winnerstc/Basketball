CREATE EXTERNAL TABLE IF NOT EXISTS nba_silver.games_silver (
    gameid BIGINT,
    gamedatetimeest STRING,
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
    gamelabel STRING,
    gamesublabel STRING,
    seriesgamenumber INT,
    season INT,

    -- Derived fields
    game_ts TIMESTAMP,
    game_date DATE,
    game_year INT,
    game_month INT,
    home_win INT,
    away_win INT,
    score_diff INT,

    -- Audit fields
    silver_ingest_ts TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///tmp/DE011025/NBA/silver/games'
TBLPROPERTIES ('parquet.compression'='SNAPPY');