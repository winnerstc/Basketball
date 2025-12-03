# test_nba_etl.py
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import col

# Assuming transform_games_data is in nba_etl_logic.py
from silver_games import transform_games_data 

### --- SCHEMA DEFINITION MATCHING BRONZE DDL --- ###
# Note: winner and arenaId are defined as INT and BIGINT respectively in the Bronze DDL.
test_schema = StructType([
    StructField("gameId", LongType(), True),
    StructField("gameDateTimeEst", StringType(), True),
    StructField("hometeamCity", StringType(), True),
    StructField("hometeamName", StringType(), True),
    StructField("hometeamId", LongType(), True),
    StructField("awayteamCity", StringType(), True),
    StructField("awayteamName", StringType(), True),
    StructField("awayteamId", LongType(), True),
    StructField("homeScore", IntegerType(), True),
    StructField("awayScore", IntegerType(), True),
    StructField("winner", IntegerType(), True),
    StructField("gameType", StringType(), True),
    StructField("attendance", IntegerType(), True),
    StructField("arenaId", LongType(), True),
    StructField("gameLabel", StringType(), True),
    StructField("gameSubLabel", StringType(), True),
    StructField("seriesGameNumber", IntegerType(), True)
])

# Utility function to create a basic DataFrame
def create_test_df(spark_session, data):
    """Creates a DataFrame with the defined test_schema."""
    return spark_session.createDataFrame(data, test_schema)

### --- UNIT TESTS --- ###

def test_null_replacement_and_timestamp_parsing(spark_session):
    """Tests steps 2 (null replacement) and 4 (timestamp parsing)."""
    # 17 columns in total
    data = [
        # gameId, gameDateTimeEst, hometeamCity, hometeamName, hometeamId, awayteamCity, awayteamName, awayteamId, homeScore, awayScore, winner, gameType, attendance, arenaId, gameLabel, gameSubLabel, seriesGameNumber
        (1, "2023-01-01 10:00:00", "CityA", "nul", 100, "CityB", "NULL", 200, 100, 90, 100, "REG", 15000, 1, "Label1", "Sub1", 1),
        (2, "Invalid-Time", "CityC", "NA", 300, "CityD", "", 400, 110, 120, 400, "PLAYOFFS", 20000, 2, "Label2", "Sub2", 1),
    ]
    df_raw = create_test_df(spark_session, data)
    df_silver, _ = transform_games_data(df_raw)
    
    # Check for NULL replacement and timestamp parsing
    row1 = df_silver.filter(col("gameid") == 1).first()

    # Row 1 Checks (nulls replaced, timestamp parsed)
    # Note: 'nul' -> NULL replacement happens before upper casing in the logic, resulting in None
    assert row1["hometeamName"] is None 
    assert row1["awayteamName"] is None 
    assert str(row1["game_ts"]).startswith("2023-01-01 10:00:00")
    assert row1["game_year"] == 2023
    
    # Row 2 Check (invalid timestamp is handled, leading to missing keys if gameid is null)
    # The row should be filtered out because the invalid timestamp makes game_ts NULL, and subsequently game_year NULL (failing quality step 9)
    # However, since the critical IDs (gameId, hometeamId, awayteamId, arenaId) are present, it passes Step 5 (quarantine) but fails Step 9 (quality).
    assert df_silver.filter(col("gameid") == 2).count() == 0 


def test_quarantine_missing_keys(spark_session):
    """Tests step 5 (quarantine logic)."""
    data = [
        # Good row: all key fields present (IDs are LongType, winner is IntType)
        (1, "2023-01-01", "C1", "N1", 1, "C2", "N2", 2, 100, 90, 1, "REG", 15000, 1, "L", "S", 1), 
        # Bad: missing hometeamId (LongType)
        (2, "2023-01-02", "C3", "N3", None, "C4", "N4", 4, 110, 120, 4, "REG", 20000, 2, "L", "S", 1), 
        # Bad: missing arenaId (LongType)
        (3, "2023-01-03", "C5", "N5", 5, "C6", "N6", 6, 130, 140, 6, "REG", 18000, None, "L", "S", 1), 
    ]
    df_raw = create_test_df(spark_session, data)

    df_silver, df_bad = transform_games_data(df_raw)

    # Check Silver DataFrame
    assert df_silver.count() == 1
    assert df_silver.filter(col("gameid") == 1).count() == 1

    # Check Quarantine DataFrame
    assert df_bad.count() == 2
    assert df_bad.filter(col("gameid") == 2).count() == 1
    assert df_bad.filter(col("gameid") == 3).count() == 1
    assert df_bad.select("quarantine_reason").distinct().collect()[0]["quarantine_reason"] == "MISSING_KEY_FIELDS"


def test_trimming_and_casing(spark_session):
    """Tests steps 6 (trim) and 7 (casing)."""
    data = [
        # gameDateTimeEst must be valid for it to pass Step 5 and 9
        (1, "2023-01-01", " LA ", " Lakers ", 1, " NY ", " Knicks ", 2, 100, 90, 1, " reg ", 15000, 1, "L", "S", 1),
    ]
    df_raw = create_test_df(spark_session, data)
    df_silver, _ = transform_games_data(df_raw)

    row = df_silver.first()

    # Check trimming (step 6)
    assert row["hometeamcity"] == "LA"
    assert row["awayteamcity"] == "NY"
    
    # Check casing (step 7)
    assert row["hometeamname"] == " LAKERS " # Name columns are only uppered, not trimmed
    assert row["awayteamname"] == " KNICKS "
    assert row["gametype"] == "REG" # Trimmed then uppered


def test_derived_metrics(spark_session):
    """Tests step 8 (derived metrics)."""
    data = [
        # Home Win
        (1, "2023-01-01", "C", "N", 1, "C", "N", 2, 110, 90, 1, "REG", 15000, 1, "L", "S", 1), 
        # Away Win
        (2, "2023-01-02", "C", "N", 3, "C", "N", 4, 80, 100, 4, "REG", 20000, 2, "L", "S", 1), 
        # Tie
        (3, "2023-01-03", "C", "N", 5, "C", "N", 6, 95, 95, 6, "REG", 18000, 3, "L", "S", 1), 
    ]
    df_raw = create_test_df(spark_session, data)
    df_silver, _ = transform_games_data(df_raw)

    # Home Win
    row1 = df_silver.filter(col("gameid") == 1).first()
    assert row1["home_win"] == 1
    assert row1["away_win"] == 0
    assert row1["score_diff"] == 20

    # Away Win
    row2 = df_silver.filter(col("gameid") == 2).first()
    assert row2["home_win"] == 0
    assert row2["away_win"] == 1
    assert row2["score_diff"] == -20

    # Tie
    row3 = df_silver.filter(col("gameid") == 3).first()
    assert row3["home_win"] == 0
    assert row3["away_win"] == 0
    assert row3["score_diff"] == 0


def test_numeric_quality_filters(spark_session):
    """Tests step 9 (numeric quality filters)."""
    data = [
        (1, "2023-01-01", "C", "N", 1, "C", "N", 2, 100, 90, 1, "REG", 15000, 1, "L", "S", 1), # Good
        (2, "2023-01-02", "C", "N", 3, "C", "N", 4, -10, 90, 1, "REG", 15000, 1, "L", "S", 1), # Bad: negative homescore
        (3, "2023-01-03", "C", "N", 5, "C", "N", 6, 100, 90, 1, "REG", 35000, 1, "L", "S", 1), # Bad: attendance too high
        (4, "2023-01-04", "C", "N", 7, "C", "N", 8, 100, 90, 1, "REG", 15000, 1, "L", "S", 1900), # Bad: game_year too low (using 1900 for seriesGameNumber, which isn't game_year) 
        (5, "1900-01-04", "C", "N", 9, "C", "N", 10, 100, 90, 1, "REG", 15000, 1, "L", "S", 1), # Bad: game_year too low (1900 < 1946)
    ]
    df_raw = create_test_df(spark_session, data)
    df_silver, _ = transform_games_data(df_raw)

    # Only rows 1 (Good) and 4 (Bad value in an unused column) should pass.
    # Row 5 fails the game_year filter. Row 2 and 3 fail score/attendance filters.
    assert df_silver.count() == 2
    assert df_silver.filter(col("gameid") == 1).count() == 1
    assert df_silver.filter(col("gameid") == 4).count() == 1
    assert df_silver.filter(col("gameid").isin([2, 3, 5])).count() == 0


def test_deduplication(spark_session):
    """Tests step 10 (deduplication by latest timestamp)."""
    data = [
        # Duplicate gameId 1, keep latest (second record)
        (1, "2023-01-01 10:00:00", "C", "N", 1, "C", "N", 2, 100, 90, 1, "REG", 10000, 1, "L", "S", 1), # Old record (should be dropped)
        (1, "2023-01-01 11:00:00", "C", "N", 1, "C", "N", 2, 105, 95, 1, "REG", 10000, 1, "L", "S", 1), # New record (should be kept)
        # Unique gameId 2
        (2, "2023-01-02 10:00:00", "C", "N", 3, "C", "N", 4, 120, 110, 3, "REG", 10000, 2, "L", "S", 1), 
    ]
    df_raw = create_test_df(spark_session, data)
    
    df_silver, _ = transform_games_data(df_raw)

    assert df_silver.count() == 2
    # Check that the kept record for gameId 1 is the one with score 105/95
    kept_record = df_silver.filter(col("gameid") == 1).first()
    assert kept_record["homescore"] == 105
    assert kept_record["awayscore"] == 95