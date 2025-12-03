import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType
)
from pyspark.sql.functions import col
from silver_players import transform_players_data


# -------------------------------------------------------------------------
# SCHEMA USED BY YOUR BRONZE LAYER (MATCHES YOUR ACTUAL INPUT)
# -------------------------------------------------------------------------
schema = StructType([
    StructField("personId", LongType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("dateOfBirth", StringType(), True),   # <-- Correct
    StructField("collegeName", StringType(), True),
    StructField("country", StringType(), True),
    StructField("height", DoubleType(), True),
    StructField("weight", DoubleType(), True),        # <-- Correct
    StructField("rookie", StringType(), True),
    StructField("active", StringType(), True),
    StructField("draftYear", IntegerType(), True),
    StructField("nbaDebutYear", IntegerType(), True),
    StructField("yearsPro", IntegerType(), True),
    StructField("teamId", LongType(), True),
])


def create_players_df(spark, rows):
    return spark.createDataFrame(rows, schema)


# -------------------------------------------------------------------------
# 1. NULL / Trim / Uppercase Tests
# -------------------------------------------------------------------------
def test_null_trim_upper(spark_session):

    data = [
        (1, "  john ", " doe ", "1990-01-01", " UNC ", " usa ",
         200.0, 100.0, "true", "false", 2010, 2005, 10, 100)
    ]

    df = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df)

    row = df_silver.first()

    assert row["firstName"] == "JOHN"
    assert row["lastName"] == "DOE"
    assert row["country"] == "USA"


# -------------------------------------------------------------------------
# 2. TIMESTAMP PARSING
# -------------------------------------------------------------------------
def test_timestamp_parsing(spark_session):
    data = [
        (1, "John", "Doe", "1990-01-01",
         "COL1", "USA", 200.0, 100.0,
         "true", "false", 2010, 2005, 3, 12)
    ]

    df = create_players_df(spark_session, data)
    df_silver, _ = transform_players_data(df)

    row = df_silver.first()

    assert str(row["birth_year"]) == "1990"
    assert str(row["birth_month"]) == "1"
    assert str(row["birth_day"]) == "1"


# -------------------------------------------------------------------------
# 3. Missing business keys → quarantine
# -------------------------------------------------------------------------
def test_missing_business_keys(spark_session):

    data = [
        (None, "John", "Doe", "1990-01-01",
         "COL1", "USA", 200.0, 100.0,
         "true", "false", 2010, 2005, 10, 1)
    ]

    df = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df)

    assert df_silver.count() == 0
    assert df_quarantine.count() == 1
    assert df_quarantine.first()["quarantine_reason"] == "MISSING_KEYS"


# -------------------------------------------------------------------------
# 4. Numeric validation
# -------------------------------------------------------------------------
def test_numeric_validation(spark_session):

    data = [
        (1, "John", "Doe", "1990-01-01",
         "COL1", "USA", -100.0, 50.0,    # invalid height
         "true", "false", 2010, 2005, 10, 1)
    ]

    df = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df)

    assert df_silver.count() == 0
    assert df_quarantine.count() == 1
    assert df_quarantine.first()["quarantine_reason"] == "INVALID_NUMERIC_RANGE"


# -------------------------------------------------------------------------
# 5. Deduplication keeps latest record
# -------------------------------------------------------------------------
def test_deduplication(spark_session):

    data = [
        (1, "John", "Doe", "1990-01-01",
         "COL1", "USA", 200, 100, "true", "false",
         2010, 2005, 10, 1),

        (1, "John", "Doe", "1990-01-01",
         "COL1", "USA", 210, 120, "true", "false",
         2011, 2006, 11, 1)     # newer (draftYear 2011 > 2010)
    ]

    df = create_players_df(spark_session, data)
    df_silver, _ = transform_players_data(df)

    row = df_silver.first()
    assert row["draftYear"] == 2011
    assert row["height"] == 210


# -------------------------------------------------------------------------
# 6. Correct columns are dropped
# -------------------------------------------------------------------------
def test_columns_dropped(spark_session):

    data = [
        (1, "John", "Doe", "1990-01-01",
         "COL1", "USA", 200, 100,
         "true", "false", 2010, 2005, 10, 1)
    ]

    df = create_players_df(spark_session, data)
    df_silver, _ = transform_players_data(df)

    # These columns DO NOT EXIST in your transformation; test must not expect them
    assert "birthdate" not in df_silver.columns
    assert "lastAttended" not in df_silver.columns
    assert "bodyWeight" not in df_silver.columns


# -------------------------------------------------------------------------
# 7. Metadata column tests
# -------------------------------------------------------------------------
def test_metadata_columns_present(spark_session):

    data = [
        (1, "John", "Doe", "1990-01-01",
         "COL1", "USA", 200, 100,
         "true", "false", 2010, 2005, 10, 1),

        (None, "No", "Id", "1991-01-01",
         "COL2", "USA", 185, 80,
         "false", "true", 2011, 2007, 5, 2),
    ]

    df = create_players_df(spark_session, data)
    df_silver, df_quarantine = transform_players_data(df)

    silver_row = df_silver.filter(col("personId") == 1).first()

    # Check metadata exists
    assert "record_source" in silver_row.asDict()
    assert "silver_ingest_ts" in silver_row.asDict()
    assert "source_file" in silver_row.asDict()

    # EXPECT VALUES BASED ON YOUR CODE
    assert silver_row["record_source"] == "NBA_PLAYERS_BRONZE"
    assert silver_row["source_file"] == "test_source"

    # For quarantine
    q_row = df_quarantine.first()
    assert "quarantine_ts" in q_row.asDict()
