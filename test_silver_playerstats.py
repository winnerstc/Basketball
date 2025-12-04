import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from silver_playerstats import transform_player_statistics


# ============================================================
# FIXED make_df()
# Converts int → float for DoubleType columns BEFORE Spark loads DF
# ============================================================
def make_df(spark, rows):
    schema = StructType([
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("personId", IntegerType(), True),
        StructField("gameId", IntegerType(), True),
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

    # convert int → float for DoubleType columns
    fixed = []
    for r in rows:
        r = list(r)
        for i in range(15, len(r)):  # from numMinutes onwards
            if isinstance(r[i], int):
                r[i] = float(r[i])
        fixed.append(tuple(r))

    return spark.createDataFrame(fixed, schema)


# ============================================================
# TEST 1 — Null trimming + uppercase
# ============================================================
def test_trim_and_upper(spark_session):
    data = [
        (" la ", " james ", 1, 10, "2023-01-01 10:00:00",
         " la ", " lakers ", " ny ", " knicks ",
         "reg", "L", "S", 1, 1, 1,
         30, 20, 5, 1, 1, 10, 5, 40, 5, 2, 35, 10, 5, 2, 3,
         5, 2, 7)
    ]

    df = make_df(spark_session, data)
    df_silver, _ = transform_player_statistics(df)
    row = df_silver.first()

    assert row["firstName"] == "la"
    assert row["playerteamName"] == "LAKERS"
    assert row["opponentteamName"] == "KNICKS"
    assert row["gameType"] == "REG"


# ============================================================
# TEST 2 — Timestamp parsing
# ============================================================
def test_timestamp_parsing(spark_session):
    data = [
        ("A", "B", 1, 10, "2024-02-01 14:00:00",
         "X", "TEAM", "Y", "OPP",
         "REG", "L", "S", 1, 1, 1,
         25, 10, 5, 1, 1, 10, 5, 50, 5, 2, 40, 10, 5, 2, 3,
         5, 2, 7)
    ]

    df = make_df(spark_session, data)
    df_silver, _ = transform_player_statistics(df)
    row = df_silver.first()

    assert row["game_year"] == 2024
    assert row["game_month"] == 2
    assert str(row["game_date"]) == "2024-02-01"


# ============================================================
# TEST 3 — Missing business keys → quarantine
# ============================================================
def test_missing_business_keys(spark_session):
    data = [
        ("A", "B", None, 10, "2023-01-01", "X", "T", "Y", "O",
         "REG", "L", "S", 1, 1, 1, 20, 10, 5, 1, 1, 10, 5, 40,
         5, 2, 35, 10, 5, 2, 3, 5, 2, 7),

        ("A", "B", 1, None, "2023-01-01", "X", "T", "Y", "O",
         "REG", "L", "S", 1, 1, 1, 20, 10, 5, 1, 1, 10, 5, 40,
         5, 2, 35, 10, 5, 2, 3, 5, 2, 7),

        ("A", "B", 2, 20, "2023-01-01", "X", "T", "Y", "O",
         "REG", "L", "S", 1, 1, 1, 20, 10, 5, 1, 1, 10, 5, 40,
         5, 2, 35, 10, 5, 2, 3, 5, 2, 7)
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 2


# ============================================================
# TEST 4 — Numeric validation
# ============================================================
def test_numeric_validation(spark_session):
    data = [
        # valid
        ("A","B",1,10,"2023-01-01","X","T","Y","O",
         "REG","L","S",1,1,1,
         20,10,5,1,1,10,5,40,5,2,35,10,5,2,3,5,2,7),

        # invalid (negative points)
        ("A","B",2,10,"2023-01-01","X","T","Y","O",
         "REG","L","S",1,1,1,
         20,-5,5,1,1,10,5,40,5,2,35,10,5,2,3,5,2,7)
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 1


# ============================================================
# TEST 5 — Deduplication by latest timestamp
# ============================================================
def test_deduplication(spark_session):
    data = [
        ("A","B",1,10,"2023-01-01 10:00:00","X","T","Y","O",
         "REG","L","S",1,1,1,
         20,10,5,1,1,10,5,40,5,2,35,10,5,2,3,5,2,7),

        ("A","B",1,10,"2023-01-01 12:00:00","X","T","Y","O",
         "REG","L","S",1,1,1,
         20,50,10,1,1,10,5,40,5,2,35,10,5,2,3,5,2,7)  # newer
    ]

    df = make_df(spark_session, data)
    df_silver, _ = transform_player_statistics(df)

    row = df_silver.first()
    assert row["points"] == 50
    assert row["assists"] == 10


# ============================================================
# TEST 6 — points_per_minute
# ============================================================
def test_points_per_minute(spark_session):
    data = [
        ("A","B",1,10,"2023-01-01","X","T","Y","O",
         "REG","L","S",1,1,1,
         30,15,5,1,1,10,5,40,5,2,35,10,5,2,3,5,2,7)
    ]

    df = make_df(spark_session, data)
    df_silver, _ = transform_player_statistics(df)
    row = df_silver.first()

    assert row["points_per_minute"] == pytest.approx(0.5, 0.01)


# ============================================================
# TEST 7 — Metadata
# ============================================================
def test_metadata(spark_session):
    data = [
        ("A","B",1,10,"2023-01-01","X","T","Y","O",
         "REG","L","S",1,1,1,
         30,15,5,1,1,10,5,40,5,2,35,10,5,2,3,5,2,7)
    ]

    df = make_df(spark_session, data)
    df_silver, _ = transform_player_statistics(df)
    row = df_silver.first()

    assert "silver_ingest_ts" in row.asDict()
    assert "source_file" in row.asDict()
