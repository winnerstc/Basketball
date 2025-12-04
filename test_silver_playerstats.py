# test_silver_player_statistics.py

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, DoubleType
)
from pyspark.sql.functions import col

from silver_playerstats import transform_player_statistics


# ---------------------------------------------
# Bronze schema for player_statistics
# ---------------------------------------------
schema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("personId", LongType(), True),
    StructField("gameId", LongType(), True),
    StructField("gameDateTimeEst", StringType(), True),
    StructField("playerteamCity", StringType(), True),
    StructField("playerteamName", StringType(), True),
    StructField("opponentteamCity", StringType(), True),
    StructField("opponentteamName", StringType(), True),
    StructField("gameType", StringType(), True),
    StructField("gameLabel", StringType(), True),
    StructField("gameSubLabel", StringType(), True),
    StructField("seriesGameNumber", IntegerType(), True),
    StructField("win", IntegerType(), True),
    StructField("home", IntegerType(), True),
    StructField("numMinutes", DoubleType(), True),
    StructField("points", DoubleType(), True),
    StructField("assists", DoubleType(), True),
    StructField("blocks", DoubleType(), True),
    StructField("steals", DoubleType(), True),
    StructField("fieldGoalsAttempted", DoubleType(), True),
    StructField("fieldGoalsMade", DoubleType(), True),
    StructField("fieldGoalsPercentage", DoubleType(), True),
    StructField("threePointersAttempted", DoubleType(), True),
    StructField("threePointersMade", DoubleType(), True),
    StructField("threePointersPercentage", DoubleType(), True),
    StructField("freeThrowsAttempted", DoubleType(), True),
    StructField("freeThrowsMade", DoubleType(), True),
    StructField("freeThrowsPercentage", DoubleType(), True),
    StructField("reboundsDefensive", DoubleType(), True),
    StructField("reboundsOffensive", DoubleType(), True),
    StructField("reboundsTotal", DoubleType(), True),
    StructField("foulsPersonal", DoubleType(), True),
    StructField("turnovers", DoubleType(), True),
    StructField("plusMinusPoints", DoubleType(), True),
])


def make_df(spark_session, data):
    return spark_session.createDataFrame(data, schema)


# ---------------------------------------------
# 1) Null Replacement + Trim + Uppercase
# ---------------------------------------------
def test_null_trim_upper(spark_session):

    data = [
        (
            " nul ",   # firstName -> None, trimmed
            " james ", 1, 100, "2023-01-01 10:00:00",
            " la ",     # trimmed
            " lakers ", # upper -> LAKERS
            " ny ",
            " knicks ",
            " reg ",     
            "L1", "S1", 1, 1, 1,
            30.0, 20.0, 5.0, 1.0, 1.0,
            10.0, 5.0, 50.0,
            6.0, 2.0, 33.3,
            4.0, 4.0, 100.0,
            5.0, 2.0, 7.0,
            2.0, 1.0, 4.0
        )
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    row = df_silver.first()

    assert row["firstName"] is None
    assert row["playerteamCity"] == "la".strip()
    assert row["playerteamName"] == "LAKERS"
    assert row["opponentteamName"] == "KNICKS"
    assert row["gameType"] == "REG"


# ---------------------------------------------
# 2) Timestamp parsing + date parts
# ---------------------------------------------
def test_timestamp_and_date_parts(spark_session):

    data = [
        (
            "John", "Doe", 1, 200, "2023-01-05 15:30:00",
            "LA", "LAKERS", "NY", "KNICKS", "REG",
            "L", "S", 1, 1, 1,
            25.0, 15.0, 5.0, 2.0, 3.0,
            10.0, 6.0, 60.0,
            5.0, 2.0, 40.0,
            4.0, 4.0, 100.0,
            5.0, 2.0, 7.0,
            1.0, 2.0, 4.0
        )
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    row = df_silver.first()

    assert str(row["game_date"]) == "2023-01-05"
    assert row["game_year"] == 2023
    assert row["game_month"] == 1


# ---------------------------------------------
# 3) Missing key rows dropped
# ---------------------------------------------
def test_missing_business_keys_are_dropped(spark_session):

    data = [
        ("A", "B", None, 10, "2023-01-01", "C","D","E","F","REG","L","S",1,1,1,20,10,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,2,1,4),
        ("A", "B", 1, None, "2023-01-01", "C","D","E","F","REG","L","S",1,1,1,20,10,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,2,1,4),
        ("A", "B", 2, 20, "2023-01-01", "C","D","E","F","REG","L","S",1,1,1,20,10,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,2,1,4),
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    # Only the row with both personId and gameId present survives
    assert df_silver.count() == 1
    assert df_silver.filter(col("personId") == 2).count() == 1


# ---------------------------------------------
# 4) Numeric validation rules
# ---------------------------------------------
def test_numeric_validation(spark_session):

    data = [
        # valid
        ("J","D",1,10,"2023-01-01","C","D","E","F","REG","L","S",1,1,1,
         30,20,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,1,2,3),

        # invalid: negative points
        ("J","D",2,20,"2023-01-01","C","D","E","F","REG","L","S",1,1,1,
         30,-5,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,1,2,3),

        # invalid: FG% > 100
        ("J","D",3,30,"2023-01-01","C","D","E","F","REG","L","S",1,1,1,
         30,20,5,1,1,10,5,150,5,2,40,4,4,100,5,2,7,1,2,3),
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    assert df_silver.count() == 1
    assert df_silver.filter(col("personId") == 1).count() == 1


# ---------------------------------------------
# 5) Deduplication: keep latest gameDateTimeEst
# ---------------------------------------------
def test_deduplication(spark_session):

    data = [
        # Older version:
        ("A","B",10,50,"2023-01-01 10:00:00","C","D","E","F","REG","L","S",1,1,1,
         10,5,2,0,1,5,2,40,2,1,50,4,4,100,3,1,4,1,2,3),

        # Newer version (should be kept):
        ("A","B",10,50,"2023-01-01 12:00:00","C","D","E","F","REG","L","S",1,1,1,
         12,6,3,0,1,6,3,50,3,2,40,4,4,100,3,1,4,1,2,3),
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    assert df_silver.count() == 1
    row = df_silver.first()

    assert row["numMinutes"] == 12
    assert row["points"] == 6


# ---------------------------------------------
# 6) Derived metric: points_per_minute
# ---------------------------------------------
def test_points_per_minute(spark_session):

    data = [
        ("A","B",1,10,"2023-01-01","C","D","E","F","REG","L","S",1,1,1,
         30,15,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,1,2,3),

        ("A","B",2,11,"2023-01-01","C","D","E","F","REG","L","S",1,1,1,
         0,20,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,1,2,3),
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    r1 = df_silver.filter(col("personId") == 1).first()
    assert abs(r1["points_per_minute"] - 0.5) < 0.0001

    r2 = df_silver.filter(col("personId") == 2).first()
    assert r2["points_per_minute"] is None


# ---------------------------------------------
# 7) Metadata columns
# ---------------------------------------------
def test_metadata(spark_session):

    data = [
        ("A","B",1,10,"2023-01-01","C","D","E","F","REG","L","S",1,1,1,
         30,15,5,1,1,10,5,50,5,2,40,4,4,100,5,2,7,1,2,3),
    ]

    df_raw = make_df(spark_session, data)
    df_silver = transform_player_statistics(df_raw)

    row = df_silver.first()

    assert "silver_ingest_ts" in row.asDict()
    assert row["silver_ingest_ts"] is not None
    assert "source_file" in row.asDict()
