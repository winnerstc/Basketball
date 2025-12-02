
CREATE EXTERNAL TABLE IF NOT EXISTS players_silver (
    personid            BIGINT,
    firstname           STRING,
    lastname            STRING,
    birthdate           STRING,
    lastattended        STRING,
    country             STRING,
    height              DOUBLE,
    bodyweight          DOUBLE,
    guard               STRING,
    forward             STRING,
    center              STRING,
    draftyear           INT,
    draftround          INT,
    draftnumber         INT,
    birth_date          DATE,
    birth_year          INT,
    age_years           DOUBLE,
    guard_flag          INT,
    forward_flag        INT,
    center_flag         INT,
    primary_position    STRING,
    silver_ingest_ts    TIMESTAMP,
    source_file         STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///tmp/DE011025/NBA/silver/players/';