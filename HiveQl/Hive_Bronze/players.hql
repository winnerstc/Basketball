CREATE EXTERNAL TABLE IF NOT EXISTS nba_bronze.players (
    personId BIGINT,
    firstName STRING,
    lastName STRING,
    birthdate STRING,
    lastAttended STRING,
    country STRING,
    height DOUBLE,
    bodyWeight DOUBLE,
    guard STRING,
    forward STRING,
    center STRING,
    draftYear INT,
    draftRound INT,
    draftNumber INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/DE011025/NBA/bronze/players';
