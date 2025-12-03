# test_silver_players.py

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType
)
from pyspark.sql.functions import col

from silver_players import transform_players_data


# ------------------------------------------------------------
# SCHEMA for Bronze players
# ------------------------------------------------------------
schema = StructType([
    StructField("personId", LongType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("dateOfBirth", StringType(), True),
    StructField("collegeName", StringType(), True),
    StructField("country", StringType(), True),
    StructField("height", DoubleType(), True),
    StructField("weight", DoubleType(), True),
    StructField("rookie", StringType(), True),
    StructField("active", StringType(), True),
    StructField("draftYear", StringType(), True),
    StructField("nbaDebutYear", IntegerType(), True),
    StructField("yearsPro", IntegerType(), True),
    StructField("teamId", LongType(), True),
])


def create_players_df(spark_session, data):
    return spark_session.createDataFrame(data, schema)


# ------------------------------------------------------------
# 1) Trim, uppercase, null clean
# ------------------------------------------------------------
def test_null_trim_upper(spark_session):
    data = [
        (1, "  john ", " Doe ", "1990-01-01", " col ", " usa ",
         200.0, 100.0, "true", "false", None, 2010, 1, 1)
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    row = df_silver.first()
    assert row["firstName"] == "JOHN"
    assert row["lastName"] == "DOE"
    assert row["collegeName"] == "COL"
    assert row["country"] == "USA"


# ------------------------------------------------------------
# 2) Timestamp and date parts
# ------------------------------------------------------------
def test_timestamp_parsing(spark_session):
    data = [
        (1, "John", "Doe", "1990-05-20", "COL1", "USA",
         200.0, 100.0, "true", "false", "2008", 2010, 1, 1),
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    r = df_silver.first()
    assert r["dob_year"] == 1990
    assert r["dob_month"] == 5
    assert r["dob_day"] == 20


# ------------------------------------------------------------
# 3) Missing business key → quarantine
# ------------------------------------------------------------
def test_missing_business_keys(spark_session):
    data = [
        (None, "John", "Doe", "1991-01-01", "COL1", "USA",
         190.0, 80.0, "true", "false", None, 2010, 1, 1)
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    assert df_silver.count() == 0
    assert df_quarantine.count() == 1
    assert df_quarantine.first()["quarantine_reason"] == "MISSING_KEYS"


# ------------------------------------------------------------
# 4) Numeric validity
# ------------------------------------------------------------
def test_numeric_validation(spark_session):
    data = [
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         -10.0, 100.0, "true", "false", "2008", 2010, 1, 1),   # BAD
        (2, "A", "B", "1991-01-01", "COL", "USA",
         200.0, 80.0, "true", "false", "2009", 2011, 2, 2)    # GOOD
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 1
    assert df_quarantine.first()["quarantine_reason"] == "INVALID_NUMERIC_RANGE"


# ------------------------------------------------------------
# 5) Deduplication
# ------------------------------------------------------------
def test_deduplication(spark_session):

    data = [
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         200.0, 100.0, "true", "false", None, 2010, 1, 1),

        # duplicate but later year → keep this one
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         210.0, 110.0, "true", "false", None, 2012, 3, 1),
    ]

    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    assert df_silver.count() == 1
    r = df_silver.first()
    assert r["nbaDebutYear"] == 2012


# ------------------------------------------------------------
# 6) Columns dropped
# ------------------------------------------------------------
def test_columns_dropped(spark_session):
    data = [
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         200.0, 100.0, "true", "false", "2008", 2010, 1, 1)
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    dropped = ["draftYear"]
    for c in dropped:
        assert c not in df_silver.columns


# ------------------------------------------------------------
# 7) Metadata columns
# ------------------------------------------------------------
def test_metadata_columns_present(spark_session):
    """
    Your transformation uses:
        source_file = "test_source"
    So the test must expect that.
    """

    data = [
        (1, "John", "Doe", "1990-01-01", "COL1", "USA",
         200.0, 100.0, "true", "false", None, 2010, 1, 1),

        (None, "No", "Id", "1991-01-01", "COL2", "USA",
         185.0, 80.0, "false", "true", None, 2011, 1, 2),
    ]

    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    silver_row = df_silver.filter(col("personId") == 1).first()

    assert "record_source" in silver_row.asDict()
    assert "silver_ingest_ts" in silver_row.asDict()
    assert "source_file" in silver_row.asDict()

    assert silver_row["record_source"] == "NBA_PLAYERS_BRONZE"
    assert silver_row["source_file"] == "test_source"  # <-- MATCH YOUR CODE

    # Test quarantine also contains metadata
    df_bad = create_players_df(spark_session, [(None, None, None, None, None,
                                                None, None, None, None, None,
                                                None, None, None, None)])
    _, df_quarantine2 = transform_players_data(df_bad)

    q = df_quarantine2.first()
    assert "quarantine_ts" in q.asDict()
    assert "quarantine_reason" in q.asDict()
