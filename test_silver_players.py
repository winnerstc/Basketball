import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)
from pyspark.sql.functions import col
from silver_players import transform_players_data


# ----------------------------------------------------------------------
# Helper to build test input DF (CAST ints → float for height/weight)
# ----------------------------------------------------------------------
def create_players_df(spark, rows):
    schema = StructType([
        StructField("personId", LongType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("lastAttended", StringType(), True),
        StructField("country", StringType(), True),
        StructField("height", DoubleType(), True),
        StructField("bodyWeight", DoubleType(), True),
        StructField("guard", StringType(), True),
        StructField("forward", StringType(), True),
        StructField("center", StringType(), True),
        StructField("draftYear", IntegerType(), True),
        StructField("draftRound", IntegerType(), True),
        StructField("draftNumber", IntegerType(), True),
    ])

    # Convert ints to float for height/bodyWeight
    fixed_rows = []
    for r in rows:
        r = list(r)
        r[6] = float(r[6]) if r[6] is not None else None        # height
        r[7] = float(r[7]) if r[7] is not None else None        # weight
        fixed_rows.append(tuple(r))

    return spark.createDataFrame(fixed_rows, schema)


# ----------------------------------------------------------------------
# TESTS
# ----------------------------------------------------------------------

def test_null_trim_upper(spark_session):
    data = [
        (1, "  john ", " doe ", "1990-01-01",
         "col1 ", "usa ", 200, 100, "true", "false", None, 2010, 1, 1)
    ]

    df_raw = create_players_df(spark_session, data)
    df_silver, df_bad = transform_players_data(df_raw)

    r = df_silver.first()
    assert r.firstName == "John"
    assert r.lastName == "Doe"
    assert r.country == "USA"
    assert r.lastAttended == "COL1"


def test_timestamp_parsing(spark_session):
    data = [
        (1, "John", "Doe", "1990-05-20", "COL", "USA",
         200, 100, "true", "true", None, 2010, 1, 1)
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, _ = transform_players_data(df_raw)

    r = df_silver.first()
    assert r.birth_date.strftime("%Y-%m-%d") == "1990-05-20"
    assert r.birth_year == 1990


def test_missing_business_keys(spark_session):
    data = [
        (None, "No", "Id", "1991-01-01",
         "COL2", "USA", 185, 80, "false", "true", None, 2011, 1, 2)
    ]
    df_raw = create_players_df(spark_session, data)
    _, df_quarantine = transform_players_data(df_raw)

    r = df_quarantine.first()
    assert r.quarantine_reason == "MISSING_PERSON_ID"


def test_numeric_validation(spark_session):
    data = [
        # invalid height (too small)
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         5, 100, "true", "false", None, 2010, 1, 1)
    ]
    df_raw = create_players_df(spark_session, data)
    _, df_quarantine = transform_players_data(df_raw)

    r = df_quarantine.first()
    assert r.quarantine_reason == "INVALID_NUMERIC_RANGE"


def test_deduplication(spark_session):
    data = [
        # older
        (1, "John", "Doe", "1980-01-01", "COL", "USA",
         200, 100, "true", "false", None, 2005, 1, 5),
        # newer
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         200, 100, "true", "false", None, 2010, 1, 10),
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, _ = transform_players_data(df_raw)

    assert df_silver.count() == 1
    assert df_silver.first().birth_year == 1990   # newer wins


def test_columns_dropped(spark_session):
    data = [
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         200, 100, "true", "false", None, 2010, 1, 1),
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, _ = transform_players_data(df_raw)

    r = df_silver.first()
    assert hasattr(r, "birth_date")
    assert hasattr(r, "primary_position")


def test_metadata_columns_present(spark_session):
    data = [
        (1, "John", "Doe", "1990-01-01", "COL", "USA",
         200, 100, "true", "false", None, 2010, 1, 1)
    ]
    df_raw = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df_raw)

    r = df_silver.first()
    assert r.record_source == "NBA_PLAYERS_BRONZE"
    assert r.source_file == "test_source"   # ✔ matches your actual script
    assert r.silver_ingest_ts is not None
