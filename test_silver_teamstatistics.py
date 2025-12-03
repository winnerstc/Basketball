# test_silver_team_statistics.py

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType
)
from pyspark.sql.functions import col

from silver_team_statistics import transform_team_statistics


# ------------------------------------------------------------
# Bronze schema for team_statistics
# ------------------------------------------------------------
schema = StructType([
    StructField("gameId", LongType(), True),
    StructField("gameDateTimeEst", StringType(), True),
    StructField("teamCity", StringType(), True),
    StructField("teamName", StringType(), True),
    StructField("teamId", LongType(), True),
    StructField("opponentTeamCity", StringType(), True),
    StructField("opponentTeamName", StringType(), True),
    StructField("opponentTeamId", LongType(), True),
    StructField("home", IntegerType(), True),
    StructField("win", IntegerType(), True),
    StructField("teamScore", IntegerType(), True),
    StructField("opponentScore", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("blocks", IntegerType(), True),
    StructField("steals", IntegerType(), True),
    StructField("fieldGoalsAttempted", IntegerType(), True),
    StructField("fieldGoalsMade", IntegerType(), True),
    StructField("fieldGoalsPercentage", DoubleType(), True),
    StructField("threePointersAttempted", IntegerType(), True),
    StructField("threePointersMade", IntegerType(), True),
    StructField("threePointersPercentage", DoubleType(), True),
    StructField("freeThrowsAttempted", IntegerType(), True),
    StructField("freeThrowsMade", IntegerType(), True),
    StructField("freeThrowsPercentage", DoubleType(), True),
    StructField("reboundsDefensive", IntegerType(), True),
    StructField("reboundsOffensive", IntegerType(), True),
    StructField("reboundsTotal", IntegerType(), True),
    StructField("foulsPersonal", IntegerType(), True),
    StructField("turnovers", IntegerType(), True),
    StructField("plusMinusPoints", IntegerType(), True),
    StructField("numMinutes", IntegerType(), True),
    StructField("q1Points", IntegerType(), True),
    StructField("q2Points", IntegerType(), True),
    StructField("q3Points", IntegerType(), True),
    StructField("q4Points", IntegerType(), True),
    StructField("benchPoints", IntegerType(), True),
    StructField("biggestLead", IntegerType(), True),
    StructField("biggestScoringRun", IntegerType(), True),
    StructField("leadChanges", IntegerType(), True),
    StructField("pointsFastBreak", IntegerType(), True),
    StructField("pointsFromTurnovers", IntegerType(), True),
    StructField("pointsInThePaint", IntegerType(), True),
    StructField("pointsSecondChance", IntegerType(), True),
    StructField("timesTied", IntegerType(), True),
    StructField("timeoutsRemaining", IntegerType(), True),
    StructField("seasonWins", IntegerType(), True),
    StructField("seasonLosses", IntegerType(), True),
    StructField("coachId", LongType(), True),
])


def make_df(spark_session, data):
    """Utility to create a test DF."""
    return spark_session.createDataFrame(data, schema)


# ------------------------------------------------------------
# 1) Null → real NULL, trim, uppercase
# ------------------------------------------------------------
def test_null_trim_upper(spark_session):

    data = [
        (
            10, "2023-01-01 12:00:00",
            " la ", " Lakers ", 100,
            " ny ", " knicks ", 200,
            1, 1, 110, 100,
            10, 5, 3,
            30, 15, 50.0,
            10, 5, 33.3,
            5, 5, 100.0,
            20, 10, 30,
            2, 3, 5,
            1, 20, 25, 30,
            40, 12, 5,
            8, 10, 12,
            14, 20, 15,
            2, 5, 10, 5,
            3, 10, 20
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    row = df_silver.first()

    assert row["teamCity"] == "LA"
    assert row["teamName"] == "LAKERS"
    assert row["opponentTeamName"] == "KNICKS"


# ------------------------------------------------------------
# 2) Timestamp parsing
# ------------------------------------------------------------
def test_timestamp_parsing(spark_session):

    data = [
        (
            10, "2024-02-01 15:00:00",
            "LA", "Lakers", 100,
            "NY", "Knicks", 200,
            1, 1, 120, 110,
            10, 3, 2,
            30, 12, 40.0,
            9, 3, 33.0,
            6, 6, 100.0,
            25, 8, 33,
            2, 3, 5,
            1, 10, 20, 25,
            15, 10, 4,
            3, 5, 18,
            12, 15, 6,
            3, 8, 12, 7,
            5, 12, 24
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    row = df_silver.first()

    assert str(row["game_date"]) == "2024-02-01"
    assert row["game_year"] == 2024
    assert row["game_month"] == 2


# ------------------------------------------------------------
# 3) Missing business keys → quarantine
# ------------------------------------------------------------
def test_missing_keys_quarantine(spark_session):

    data = [
        (None, "2023-01-01", "LA", "LAKERS", 100,
         "NY", "KNICKS", 200, 1,1,100,90,
         5,3,2,20,10,50,5,2,40,
         5,5,100,10,5,15,
         2,2,4,10,2,2,2,2,15,5,3,3,
         5,3,12,5,3,10,20),

        (10, "2023-01-01", "LA", "LAKERS", None,
         "NY","KNICKS",200, 1,1,90,80,
         5,5,5,20,10,40,5,2,40,
         5,5,100,10,3,13,
         1,2,5,7,2,1,1,1,10,4,3,2,
         4,3,10,5,2,8,14),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    assert df_silver.count() == 0
    assert df_quarantine.count() == 2
    assert set(df_quarantine.select("quarantine_reason").rdd.flatMap(lambda x: x).collect()) == {"MISSING_KEYS"}


# ------------------------------------------------------------
# 4) Numeric validation
# ------------------------------------------------------------
def test_numeric_validations(spark_session):

    data = [
        # valid
        (10,"2023-01-01","LA","LAKERS",100,
         "NY","KNICKS",200,1,1,120,100,
         10,5,3,30,10,50,5,2,33.0,
         4,4,100,15,8,23,
         2,5,10,10,3,5,7,8,11,7,5,3,
         4,4,7,3,5,10,20),

        # invalid fieldGoal% > 100
        (11,"2023-01-01","LA","LAKERS",101,
         "NY","KNICKS",200,1,1,120,100,
         10,5,3,30,10,150.0,5,2,33.0,
         4,4,100,15,8,23,
         2,5,10,10,3,5,7,8,11,7,5,3,
         4,4,7,3,5,10,20),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 1
    assert df_quarantine.first()["quarantine_reason"] == "INVALID_NUMERIC_RANGE"


# ------------------------------------------------------------
# 5) Deduplication logic
# ------------------------------------------------------------
def test_deduplication_keeps_latest(spark_session):

    data = [
        # older
        (10, "2023-01-01 10:00:00","LA","LAKERS",200,
         "NY","KNICKS",300,1,1,100,90,
         5,3,2,20,10,50,5,2,40,
         5,5,100,10,5,15,
         2,2,4,10,2,2,2,2,15,5,3,3,
         5,3,12,5,3,10,20),

        # newer (should be kept)
        (10, "2023-01-01 13:00:00","LA","LAKERS",200,
         "NY","KNICKS",300,1,1,120,100,
         5,4,3,22,11,55,7,3,43,
         6,6,100,12,6,18,
         1,2,5,9,3,3,2,3,14,4,3,2,
         4,3,11,4,3,12,24),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    assert df_silver.count() == 1
    row = df_silver.first()

    assert row["teamScore"] == 120
    assert row["opponentScore"] == 100


# ------------------------------------------------------------
# 6) Derived metrics
# ------------------------------------------------------------
def test_derived_metrics(spark_session):

    data = [
        (
            10,"2023-01-01","LA","LAKERS",100,
            "NY","KNICKS",200,
            1,1,
            120,110,
            10,5,3,20,10,50,6,2,33.0,
            5,5,100,
            10,5,15,
            2,3,5,
            10,2,2,2,2,
            15,5,3,3,
            5,3,12,5,
            3,10,20
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    row = df_silver.first()

    assert row["score_diff"] == 10     # 120 - 110
    assert abs(row["shooting_efficiency"] - 0.5) < 0.0001  # 10/20


# ------------------------------------------------------------
# 7) Columns should be dropped correctly
# ------------------------------------------------------------
def test_columns_dropped(spark_session):

    data = [
        (
            10,"2023-01-01","LA","LAKERS",100,
            "NY","KNICKS",200,
            1,1,120,110,
            10,5,3,20,10,50,6,2,33.0,
            5,5,100,
            10,5,15,
            2,3,5,
            10,2,2,2,2,
            15,5,3,3,
            5,3,12,5,
            3,10,20
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    dropped_cols = [
        "timeoutsRemaining","seasonWins","seasonLosses","coachId",
        "timesTied","pointsSecondChance","pointsInThePaint",
        "pointsFromTurnovers","pointsFastBreak","q2Points",
        "q3Points","plusMinusPoints","turnovers","foulsPersonal"
    ]

    for colname in dropped_cols:
        assert colname not in df_silver.columns


# ------------------------------------------------------------
# 8) Metadata columns on silver and quarantine
# ------------------------------------------------------------
def test_metadata_columns(spark_session):

    data = [
        (
            10,"2023-01-01","LA","LAKERS",100,
            "NY","KNICKS",200,
            1,1,120,110,
            10,5,3,20,10,50,6,2,33.0,
            5,5,100,
            10,5,15,
            2,3,5,
            10,2,2,2,2,
            15,5,3,3,
            5,3,12,5,
            3,10,20
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_statistics(df)

    srow = df_silver.first()
    assert "silver_ingest_ts" in srow.asDict()
    assert "source_file" in srow.asDict()

    # quarantine only applies if invalid, so create an invalid row
    data2 = [
        (None, None, None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None)
    ]
    df_bad = make_df(spark_session, data2)
    _, df_quarantine2 = transform_team_statistics(df_bad)

    qrow = df_quarantine2.first()
    assert "quarantine_ts" in qrow.asDict()
