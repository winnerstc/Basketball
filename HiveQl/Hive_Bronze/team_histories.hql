CREATE EXTERNAL TABLE IF NOT EXISTS nba_bronze.team_histories (
    teamId BIGINT,
    teamCity STRING,
    teamName STRING,
    teamAbbrev STRING,
    seasonFounded INT,
    seasonActiveTill INT,
    league STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/DE011025/NBA/bronze/team_histories';
