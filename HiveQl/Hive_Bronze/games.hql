CREATE EXTERNAL TABLE IF NOT EXISTS nba_bronze.games (
    gameId BIGINT,
    gameDateTimeEst STRING,
    hometeamCity STRING,
    hometeamName STRING,
    hometeamId BIGINT,
    awayteamCity STRING,
    awayteamName STRING,
    awayteamId BIGINT,
    homeScore INT,
    awayScore INT,
    winner INT,
    gameType STRING,
    attendance INT,
    arenaId BIGINT,
    gameLabel STRING,
    gameSubLabel STRING,
    seriesGameNumber INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/DE011025/NBA/bronze/games';