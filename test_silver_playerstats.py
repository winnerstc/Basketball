# test_silver_playerstats.py

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
)
from pyspark.sql.functions import col

from silver_playerstats import transform_player_statistics


# ----------------------------------------------------------------------
# Bronze schema for player_statistics (INCLUDING 'year' at the end)
# ----------------------------------------------------------------------
schema = StructType(
    [
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
        StructField("numMinutes", IntegerType(), True),
        StructField("points", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("blocks", IntegerType(), True),
        StructField("steals", IntegerType(), True),
        StructField("fieldGoalsAttempted", IntegerType(), True),
        StructField("fieldGoalsMade", IntegerType(), True),
        StructField("fieldGoalsPercentage", IntegerType(), True),
        StructField("threePointersAttempted", IntegerType(), True),
        StructField("threePointersMade", IntegerType(), True),
        StructField("threePointersPercentage", IntegerType(), True),
        StructField("freeThrowsAttempted", IntegerType(), True),
        StructField("freeThrowsMade", IntegerType(), True),
        StructField("freeThrowsPercentage", IntegerType(), True),
        StructField("reboundsDefensive", IntegerType(), True),
        StructField("reboundsOffensive", IntegerType(), True),
        StructField("reboundsTotal", IntegerType(), True),
        StructField("foulsPersonal", IntegerType(), True),
        StructField("turnovers", IntegerType(), True),
        StructField("plusMinusPoints", IntegerType(), True),
        StructField("year", IntegerType(), True),
    ]
)


def make_df(spark_session, rows):
    """
    Utility to create a test DataFrame.

    - Pads tuples with None if they are shorter than the schema
      (avoids 'Length of object ... does not match length of fields ...').
    """
    fixed = []
    n_fields = len(schema)
    for r in rows:
        r = list(r)
        if len(r) < n_fields:
            r = r + [None] * (n_fields - len(r))
        elif len(r) > n_fields:
            r = r[:n_fields]
        fixed.append(tuple(r))
    return spark_session.createDataFrame(fixed, schema)


# ----------------------------------------------------------------------
# 1) Trimming + upper-casing of text fields
# ----------------------------------------------------------------------
def test_trim_and_upper(spark_session):
    data = [
        (
            "  john  ",          # firstName
            "  doe  ",           # lastName
            1,                   # personId
            10,                  # gameId
            "2023-01-01 10:00:00",
            "  la ",             # playerteamCity
            " lakers ",          # playerteamName
            " ny ",              # opponentteamCity
            " knicks ",          # opponentteamName
            "reg ",              # gameType
            "LBL",               # gameLabel
            "SUB",               # gameSubLabel
            1, 1, 1,             # seriesGameNumber, win, home
            30, 20, 5, 1, 2,     # numMinutes, points, assists, blocks, steals
            10, 5, 40,           # FGA, FGM, FG%
            5, 2, 35,            # 3PA, 3PM, 3P%
            4, 4, 100,           # FTA, FTM, FT%
            5, 2, 7,             # reboundsDef, reboundsOff, reboundsTotal
            1, 2, 3,             # foulsPersonal, turnovers, plusMinus
            2023,                # year
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    row = df_silver.first()

    # firstName only trimmed (no case change)
    assert row["firstName"] == "john"
    assert row["lastName"] == "doe"

    # Names uppercased
    assert row["playerteamName"] == "LAKERS"
    assert row["opponentteamName"] == "KNICKS"

    # gameType uppercased
    assert row["gameType"] == "REG"


# ----------------------------------------------------------------------
# 2) Timestamp parsing → game_date, game_year, game_month
# ----------------------------------------------------------------------
def test_timestamp_parsing(spark_session):
    data = [
        (
            "A",
            "B",
            1,
            10,
            "2024-02-01 15:00:00",
            "X",
            "TEAMX",
            "Y",
            "TEAMY",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            20,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2024,
        )
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    row = df_silver.first()
    assert str(row["game_date"]) == "2024-02-01"
    assert row["game_year"] == 2024
    assert row["game_month"] == 2


# ----------------------------------------------------------------------
# 3) Missing business keys → quarantine with MISSING_KEYS
# ----------------------------------------------------------------------
def test_missing_business_keys(spark_session):
    data = [
        # Missing personId
        (
            "A",
            "B",
            None,
            10,
            "2023-01-01",
            "X",
            "TX",
            "Y",
            "OY",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            20,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2023,
        ),
        # Missing gameId
        (
            "A",
            "B",
            1,
            None,
            "2023-01-01",
            "X",
            "TX",
            "Y",
            "OY",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            20,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2023,
        ),
        # Fully valid row
        (
            "A",
            "B",
            2,
            20,
            "2023-01-01",
            "X",
            "TX",
            "Y",
            "OY",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            20,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2023,
        ),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 2

    reasons = set(
        df_quarantine.select("quarantine_reason").rdd.flatMap(lambda x: x).collect()
    )
    assert reasons == {"MISSING_KEYS"}


# ----------------------------------------------------------------------
# 4) Numeric validations → INVALID_NUMERIC_RANGE
# ----------------------------------------------------------------------
def test_numeric_validation(spark_session):
    data = [
        # valid
        (
            "J",
            "D",
            1,
            10,
            "2023-01-01",
            "C",
            "T",
            "O",
            "O2",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            20,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2023,
        ),
        # invalid: negative points
        (
            "J",
            "D",
            2,
            20,
            "2023-01-01",
            "C",
            "T",
            "O",
            "O2",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            -5,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2023,
        ),
        # invalid: FG% > 100
        (
            "J",
            "D",
            3,
            30,
            "2023-01-01",
            "C",
            "T",
            "O",
            "O2",
            "REG",
            "LBL",
            "SUB",
            1,
            1,
            1,
            30,
            20,
            5,
            1,
            1,
            10,
            5,
            150,  # invalid fieldGoalsPercentage
            5,
            2,
            35,
            4,
            4,
            100,
            5,
            2,
            7,
            1,
            2,
            3,
            2023,
        ),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    assert df_silver.count() == 1
    assert df_quarantine.count() == 2

    reasons = set(
        df_quarantine.select("quarantine_reason").rdd.flatMap(lambda x: x).collect()
    )
    assert reasons == {"INVALID_NUMERIC_RANGE"}


# ----------------------------------------------------------------------
# 5) Deduplication: keep latest gameDateTimeEst per (personId, gameId)
# ----------------------------------------------------------------------
def test_deduplication(spark_session):
    data = [
        # older record
        (
            "A",
            "B",
            1,
            10,
            "2023-01-01 10:00:00",
            "X",
            "T",
            "Y",
            "O",
            "REG",
            "L",
            "S",
            1,
            1,
            1,
            20,
            10,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            10,
            5,
            2,
            3,
            5,
            2,
            7,
            2023,
        ),
        # newer record (should be kept)
        (
            "A",
            "B",
            1,
            10,
            "2023-01-01 12:00:00",
            "X",
            "T",
            "Y",
            "O",
            "REG",
            "L",
            "S",
            1,
            1,
            1,
            20,
            50,  # different points
            10,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            10,
            5,
            2,
            3,
            5,
            2,
            7,
            2023,
        ),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    assert df_silver.count() == 1
    row = df_silver.first()
    assert row["points"] == 50  # newer record kept


# ----------------------------------------------------------------------
# 6) Derived metric: points_per_minute
# ----------------------------------------------------------------------
def test_points_per_minute(spark_session):
    data = [
        (
            "A",
            "B",
            1,
            10,
            "2023-01-01",
            "X",
            "T",
            "Y",
            "O",
            "REG",
            "L",
            "S",
            1,
            1,
            1,
            30,  # numMinutes
            15,  # points
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            10,
            5,
            2,
            3,
            5,
            2,
            7,
            2023,
        ),
        # numMinutes = 0 → points_per_minute should be null
        (
            "A",
            "B",
            2,
            11,
            "2023-01-01",
            "X",
            "T",
            "Y",
            "O",
            "REG",
            "L",
            "S",
            1,
            1,
            1,
            0,   # numMinutes
            20,  # points
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            10,
            5,
            2,
            3,
            5,
            2,
            7,
            2023,
        ),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    rows = {r.personId: r for r in df_silver.collect()}

    r1 = rows[1]
    assert abs(r1["points_per_minute"] - 0.5) < 1e-6

    r2 = rows[2]
    assert r2["points_per_minute"] is None


# ----------------------------------------------------------------------
# 7) Metadata columns
# ----------------------------------------------------------------------
def test_metadata(spark_session):
    data = [
        (
            "A",
            "B",
            1,
            10,
            "2023-01-01",
            "X",
            "T",
            "Y",
            "O",
            "REG",
            "L",
            "S",
            1,
            1,
            1,
            30,
            15,
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            35,
            10,
            5,
            2,
            3,
            5,
            2,
            7,
            2023,
        ),
        # Bad numeric row to land in quarantine
        (
            "A",
            "B",
            2,
            11,
            "2023-01-01",
            "X",
            "T",
            "Y",
            "O",
            "REG",
            "L",
            "S",
            1,
            1,
            1,
            30,
            -10,  # invalid points
            5,
            1,
            1,
            10,
            5,
            40,
            5,
            2,
            150,  # invalid FT%
            10,
            5,
            2,
            3,
            5,
            2,
            7,
            2023,
        ),
    ]

    df = make_df(spark_session, data)
    df_silver, df_quarantine = transform_player_statistics(df)

    srow = df_silver.first()
    sdict = srow.asDict()
    assert "silver_ingest_ts" in sdict
    assert "source_file" in sdict
    assert sdict["source_file"] == "test_source"

    qrow = df_quarantine.first()
    qdict = qrow.asDict()
    assert "silver_ingest_ts" in qdict
    assert "source_file" in qdict
    assert qdict["source_file"] == "test_source"
