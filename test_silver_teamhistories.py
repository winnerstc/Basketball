# test_silver_team_histories.py

import pytest
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType
)
from pyspark.sql.functions import col, year, current_date

from silver_teamhistories import transform_team_histories


# ---------------------------------------------------------
# Bronze schema for team_histories
# ---------------------------------------------------------
schema = StructType([
    StructField("teamId", LongType(), True),
    StructField("teamCity", StringType(), True),
    StructField("teamName", StringType(), True),
    StructField("teamAbbrev", StringType(), True),
    StructField("seasonFounded", IntegerType(), True),
    StructField("seasonActiveTill", IntegerType(), True),
    StructField("league", StringType(), True),
])


def make_df(spark_session, data):
    """Utility to create a DataFrame with the bronze schema."""
    return spark_session.createDataFrame(data, schema)


# ---------------------------------------------------------
# 1) Null replacement + trim + casing
# ---------------------------------------------------------
def test_null_trim_and_casing(spark_session):

    data = [
        (
            100,
            " los angeles ",
            " lakers ",
            " lal ",
            1947,
            None,
            " nba "
        )
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_histories(df_raw)

    row = df_silver.first()

    assert row["teamCity"] == "Los Angeles"
    assert row["teamName"] == "LAKERS"
    assert row["teamAbbrev"] == "LAL"
    assert row["league"] == "NBA"


# ---------------------------------------------------------
# 2) team_full_name derivation
# ---------------------------------------------------------
def test_team_full_name(spark_session):

    data = [
        (
            200,
            "Dallas",
            "Mavericks",
            "DAL",
            1980,
            None,
            "NBA"
        )
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_histories(df_raw)

    row = df_silver.first()

    assert row["team_full_name"] == "Dallas MAVERICKS"


# ---------------------------------------------------------
# 3) active_flag logic
# ---------------------------------------------------------
def test_active_flag(spark_session):

    current_year = year(current_date()).cast("int")

    data = [
        # active (seasonActiveTill NULL)
        (1, "LA", "LAKERS", "LAL", 1947, None, "NBA"),
        # active (active till >= current year)
        (2, "NY", "KNICKS", "NYK", 1946, 2030, "NBA"),
        # inactive (active till < current season)
        (3, "SEA", "SONICS", "SEA", 1970, 2008, "NBA"),
    ]

    df_raw = make_df(spark_session, data)
    df_silver, _ = transform_team_histories(df_raw)

    r1 = df_silver.filter(col("teamId") == 1).first()
    r2 = df_silver.filter(col("teamId") == 2).first()
    r3 = df_silver.filter(col("teamId") == 3).first()

    assert r1["active_flag"] == 1
    assert r2["active_flag"] == 1
    assert r3["active_flag"] == 0


# ---------------------------------------------------------
# 4) Numeric validation (invalid season ranges)
# ---------------------------------------------------------
def test_invalid_season_range_quarantine(spark_session):

    data = [
        # valid
        (1, "LA", "LAKERS", "LAL", 1950, 2025, "NBA"),

        # invalid: seasonFounded < 1900
        (2, "NY", "KNICKS", "NYK", 1800, 2000, "NBA"),

        # invalid: seasonActiveTill < seasonFounded
        (3, "CHI", "BULLS", "CHI", 1990, 1980, "NBA"),
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_histories(df_raw)

    assert df_silver.count() == 1   # only teamId=1
    assert df_quarantine.count() == 2

    reasons = set(df_quarantine.select("quarantine_reason").rdd.flatMap(lambda x: x).collect())
    assert reasons == {"INVALID_SEASON_RANGE"}


# ---------------------------------------------------------
# 5) Missing business keys → quarantine
# ---------------------------------------------------------
def test_missing_business_keys(spark_session):

    data = [
        (None, "LA", "LAKERS", "LAL", 1950, 2025, "NBA"),
        (100, "LA", None, "LAL", 1950, 2025, "NBA"),
        (200, "NY", "KNICKS", "NYK", 1946, 2025, "NBA"),  # valid
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_histories(df_raw)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 2

    reasons = set(df_quarantine.select("quarantine_reason").rdd.flatMap(lambda x: x).collect())
    assert "MISSING_KEYS" in reasons


# ---------------------------------------------------------
# 6) Deduplication by teamId (latest seasonActiveTill)
# ---------------------------------------------------------
def test_deduplication(spark_session):

    data = [
        (300, "LA", "LAKERS", "LAL", 1947, 2000, "NBA"),  # older
        (300, "LA", "LAKERS", "LAL", 1947, 2020, "NBA"),  # newer → keep
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_histories(df_raw)

    assert df_silver.count() == 1

    row = df_silver.first()
    assert row["seasonActiveTill"] == 2020


# ---------------------------------------------------------
# 7) Metadata fields present
# ---------------------------------------------------------
def test_metadata_fields(spark_session):

    data = [
        (400, "Dallas", "Mavericks", "DAL", 1980, 2025, "NBA"),
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_team_histories(df_raw)

    srow = df_silver.first()
    assert "silver_ingest_ts" in srow.asDict()
    assert "source_file" in srow.asDict()

    # invalid row to check quarantine metadata
    data_bad = [
        (None, None, None, None, None, None, None)
    ]
    df_raw2 = make_df(spark_session, data_bad)
    _, df_quarantine2 = transform_team_histories(df_raw2)

    qrow = df_quarantine2.first()
    assert "quarantine_ts" in qrow.asDict()
