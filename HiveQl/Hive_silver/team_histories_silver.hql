CREATE EXTERNAL TABLE IF NOT EXISTS nba_silver.team_histories_silver (
    teamid BIGINT,
    teamcity STRING,
    teamname STRING,
    teamabbrev STRING,
    seasonfounded INT,
    seasonactivetill INT,
    league STRING,
    team_full_name STRING,
    active_flag INT,
    silver_ingest_ts TIMESTAMP,
    source_file STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs:///tmp/DE011025/NBA/silver/team_histories'
TBLPROPERTIES ('parquet.compression'='SNAPPY');