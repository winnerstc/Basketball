import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType
)
from pyspark.sql.functions import col

from silver_playerstats import transform_playerstats_data   # <-- your ETL function


# ===================================================================
# FIXED make_df(): ensures DoubleType fields get float values
# ===================================================================
def make_df(spark, rows):
    """
    Converts integer values into floats for DoubleType columns before building the DataFrame.
    This avoids: DoubleType cannot accept object <int>
    """

    schema = StructType([
        StructField("teamCity", StringType(), True),
        StructField("teamName", StringType(), True),
        StructField("personId", IntegerType(), True),
        StructField("teamId", IntegerType(), True),
        StructField("gameDateTimeEst", StringType(), True),

        StructField("opponentTeamCity", StringType(), True),
        StructField("opponentTeamName", StringType(), True),
        StructField("opponentTeamId", StringType(), True),
        StructField("gameId", StringType(), True),
        StructField("gameType", StringType(), True),
        StructField("home", StringType(), True),
        StructField("win", StringType(), True),

        StructField("points", DoubleType(), True),
        StructField("assists", DoubleType(), True),
        StructField("rebounds", DoubleType(), True),
        StructField("numMinutes", DoubleType(), True),
        StructField("fieldGoalsMade", DoubleType(), True),
        StructField("fieldGoalsAttempted", DoubleType(), True),
        StructField("threePointersMade", DoubleType(), True),
        StructField("threePointersAttempted", DoubleType(), True),
        StructField("freeThrowsMade", DoubleType(), True),
        StructField("freeThrowsAttempted", DoubleType(), True),

        StructField("fieldGoalsPercentage", DoubleType(), True),
        StructField("threePointersPercentage", DoubleType(), True),
        StructField("freeThrowsPercentage", DoubleType(), True),

        StructField("reboundsDefensive", DoubleType(), True),
        StructField("reboundsOffensive", DoubleType(), True),
        StructField("reboundsTotal", DoubleType(), True),
        StructField("steals", DoubleType(), True),
        StructField("blocks", DoubleType(), True),
        StructField("turnovers", DoubleType(), True),
        StructField("plusMinusPoints", DoubleType(), True),
    ])

    fixed = []
    for row in rows:
        r = list(row)
        # Convert all Double columns (index >= 12) from int → float
        for i in range(12, len(r)):
            if isinstance(r[i], int):
                r[i] = float(r[i])
        fixed.append(tuple(r))

    return spark.createDataFrame(fixed, schema)


# ===================================================================
# TESTS START HERE
# ===================================================================

def test_null_trim_upper(spark_session):

    data = [
        (" la ", " Lakers ", 1, 10, "2023-01-01",
         " ny ", " knicks ", "200", "5", "REG", "1", "1",
         30, 10, 5, 20, 10, 15, 5, 8, 5, 2,
         50, 40, 75, 3, 2, 5, 1, 1, 5)
    ]

    df_raw = make_df(spark_session, data)
    df_silver, _ = transform_playerstats_data(df_raw)

    row = df_silver.first()

    assert row["teamCity"] == "LA"
    assert row["teamName"] == "LAKERS"
    assert row["opponentTeamCity"] == "NY"


def test_timestamp_and_date_parts(spark_session):

    data = [
        ("A", "B", 1, 10, "2024-02-01 14:00:00",
         "C", "D", "200", "10", "REG", "1", "1",
         20, 5, 5, 25, 10, 12, 4, 6, 5, 3,
         40, 33, 70, 3, 3, 6, 1, 1, 4)
    ]

    df_raw = make_df(spark_session, data)
    df_silver, _ = transform_playerstats_data(df_raw)
    row = df_silver.first()

    assert row["game_year"] == 2024
    assert row["game_month"] == 2
    assert str(row["game_date"]) == "2024-02-01"


def test_missing_business_keys_are_dropped(spark_session):

    data = [
        ("A", "B", None, 10, "2023-01-01",
         "C", "D", "E", "F", "REG", "L", "S",
         10, 5, 5, 25, 10, 12, 4, 6, 5, 3,
         40, 33, 70, 3, 3, 6, 1, 1, 4),

        ("A", "B", 1, None, "2023-01-01",
         "C", "D", "E", "F", "REG", "L", "S",
         10, 5, 5, 25, 10, 12, 4, 6, 5, 3,
         40, 33, 70, 3, 3, 6, 1, 1, 4),

        ("A", "B", 2, 20, "2023-01-01",
         "C", "D", "E", "F", "REG", "L", "S",
         10, 5, 5, 25, 10, 12, 4, 6, 5, 3,
         40, 33, 70, 3, 3, 6, 1, 1, 4),
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_playerstats_data(df_raw)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 2


def test_numeric_validation(spark_session):

    data = [
        # valid
        ("J", "D", 1, 10, "2023-01-01",
         "C", "D", "E", "F", "REG", "L", "S",
         30, 20, 5, 20, 10, 5, 1, 1, 10, 5,
         50, 5, 40, 4, 4, 100, 5, 2, 7),

        # invalid (negative points)
        ("J", "D", 2, 20, "2023-01-01",
         "C", "D", "E", "F", "REG", "L", "S",
         -10, 20, 5, 20, 10, 5, 1, 1, 10, 5,
         50, 5, 40, 4, 4, 100, 5, 2, 7)
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_playerstats_data(df_raw)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 1


def test_deduplication(spark_session):

    data = [
        # Older entry
        ("A", "B", 10, 50, "2023-01-01 10:00:00",
         "C", "D", "E", "F", "REG", "L", "S",
         10, 5, 2, 10, 5, 2, 1, 1, 5, 2,
         40, 4, 35, 3, 2, 5, 1, 1, 3),

        # Newer entry
        ("A", "B", 10, 50, "2023-01-01 12:00:00",
         "C", "D", "E", "F", "REG", "L", "S",
         12, 6, 3, 10, 5, 2, 1, 1, 5, 2,
         40, 4, 35, 3, 2, 5, 1, 1, 3),
    ]

    df_raw = make_df(spark_session, data)
    df_silver, _ = transform_playerstats_data(df_raw)
    row = df_silver.first()

    assert row["points"] == 12
    assert row["assists"] == 6


def test_points_per_minute(spark_session):

    data = [
        ("A", "B", 1, 10, "2023-01-01",
         "C", "D", "E", "F", "REG", "L", "S",
         30, 15, 5, 30, 10, 5, 1, 1, 10, 5,
         50, 4, 35, 2, 1, 3, 1, 1, 4),
    ]

    df_raw = make_df(spark_session, data)
    df_silver, _ = transform_playerstats_data(df_raw)

    row = df_silver.first()
    assert row["points_per_minute"] == pytest.approx(1.0, 0.01)


def test_metadata(spark_session):

    data = [
        ("A","B",1,10,"2023-01-01",
         "C","D","E","F","REG","L","S",
         30,15,5,20,10,5,1,1,10,5,
         50,5,40,4,4,100,5,2,7)
    ]

    df_raw = make_df(spark_session, data)
    df_silver, df_quarantine = transform_playerstats_data(df_raw)

    row = df_silver.first()

    assert "silver_ingest_ts" in row.asDict()
    assert "record_source" in row.asDict()
    assert "source_file" in row.asDict()
