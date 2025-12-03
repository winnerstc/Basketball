# # test_silver_players.py
# import pytest
# from pyspark.sql.types import (
#     StructType, StructField, StringType, LongType,
#     IntegerType, DoubleType
# )
# from pyspark.sql.functions import col

# from silver_players import transform_players_data


# # -------------------------------
# # Schema for Bronze players table
# # -------------------------------
# players_schema = StructType([
#     StructField("personId", LongType(), True),
#     StructField("firstName", StringType(), True),
#     StructField("lastName", StringType(), True),
#     StructField("birthdate", StringType(), True),
#     StructField("lastAttended", StringType(), True),
#     StructField("country", StringType(), True),
#     StructField("height", DoubleType(), True),
#     StructField("bodyWeight", DoubleType(), True),
#     StructField("guard", StringType(), True),
#     StructField("forward", StringType(), True),
#     StructField("center", StringType(), True),
#     StructField("draftYear", IntegerType(), True),
#     StructField("draftRound", IntegerType(), True),
#     StructField("draftNumber", IntegerType(), True),
# ])


# def create_players_df(spark_session, data):
#     """Utility to create a players DF with the bronze schema."""
#     return spark_session.createDataFrame(data, players_schema)


# # ---------------------------------------------
# # 1) Null replacement + type casting + casing
# # ---------------------------------------------
# def test_null_replacement_and_text_normalization(spark_session):
#     """
#     Tests:
#     - Transformation 1: fake nulls -> NULL
#     - Transformation 2: type casting
#     - Transformation 3 & 4: trim + casing rules
#     - Transformation 5 & 6: birth_date / birth_year / age_years populated
#     """
#     data = [
#         # personId, firstName, lastName, birthdate, lastAttended, country,
#         # height, bodyWeight, guard, forward, center, draftYear, draftRound, draftNumber
#         (
#             1,
#             "nul",         # fake null -> should become None
#             "  james  ",   # trimmed + initcap -> "James"
#             "1990-01-01",
#             "  duke   ",   # trimmed + upper -> "DUKE"
#             "usa",         # upper -> "USA"
#             200.5,
#             100.0,
#             "false",       # will be used later in flag normalization
#             "true",
#             None,
#             2010,
#             1,
#             1
#         ),
#     ]
#     df_raw = create_players_df(spark_session, data)

#     df_silver, df_quarantine = transform_players_data(df_raw)

#     # No quarantine expected
#     assert df_quarantine.count() == 0

#     row = df_silver.filter(col("personId") == 1).first()
#     assert row is not None

#     # firstName "nul" -> None by fake-null replacement
#     assert row["firstName"] is None

#     # lastName trimmed + initcap
#     assert row["lastName"] == "James"

#     # country uppercase
#     assert row["country"] == "USA"

#     # lastAttended trimmed + uppercase
#     assert row["lastAttended"] == "DUKE"

#     # birth_date parsed and birth_year set
#     assert str(row["birth_date"]) == "1990-01-01"
#     assert row["birth_year"] == 1990

#     # age_years should be > 0 and < 100 (approximate check)
#     assert row["age_years"] is not None
#     assert row["age_years"] > 0
#     assert row["age_years"] < 100

#     # Numeric columns should be cast correctly
#     assert isinstance(row["height"], float)
#     assert isinstance(row["bodyWeight"], float)
#     assert isinstance(row["draftYear"], int)


# # ---------------------------------------------
# # 2) Position flags + primary_position
# # ---------------------------------------------
# def test_position_flags_and_primary_position(spark_session):
#     """
#     Tests:
#     - Transformation 7: guard_flag / forward_flag / center_flag
#     - Transformation 8: primary_position derivation
#     """
#     data = [
#         # Guard only
#         (1, "John", "Doe", "1990-01-01", "COL1", "USA", 190.0, 90.0, "true", "false", None, 2010, 1, 1),
#         # Forward only
#         (2, "Jane", "Smith", "1991-01-01", "COL2", "USA", 185.0, 80.0, "false", "true", None, 2011, 1, 2),
#         # Center only
#         (3, "Bob", "Center", "1992-01-01", "COL3", "USA", 210.0, 110.0, "false", "false", "true", 2012, 1, 3),
#         # No flags
#         (4, "No", "Flag", "1993-01-01", "COL4", "USA", 200.0, 100.0, None, None, None, 2013, 1, 4),
#     ]
#     df_raw = create_players_df(spark_session, data)
#     df_silver, df_quarantine = transform_players_data(df_raw)

#     assert df_quarantine.count() == 0
#     assert df_silver.count() == 4

#     r1 = df_silver.filter(col("personId") == 1).first()
#     assert r1["guard_flag"] == 1
#     assert r1["forward_flag"] == 0
#     assert r1["center_flag"] == 0
#     assert r1["primary_position"] == "G"

#     r2 = df_silver.filter(col("personId") == 2).first()
#     assert r2["guard_flag"] == 0
#     assert r2["forward_flag"] == 1
#     assert r2["center_flag"] == 0
#     assert r2["primary_position"] == "F"

#     r3 = df_silver.filter(col("personId") == 3).first()
#     assert r3["guard_flag"] == 0
#     assert r3["forward_flag"] == 0
#     assert r3["center_flag"] == 1
#     assert r3["primary_position"] == "C"

#     r4 = df_silver.filter(col("personId") == 4).first()
#     assert r4["guard_flag"] == 0
#     assert r4["forward_flag"] == 0
#     assert r4["center_flag"] == 0
#     assert r4["primary_position"] is None


# # ---------------------------------------------
# # 3) Quarantine: missing keys
# # ---------------------------------------------
# def test_quarantine_missing_person_id(spark_session):
#     """
#     Tests Transformation 9:
#     - Rows with NULL personId go to quarantine with reason MISSING_PERSON_ID
#     """
#     data = [
#         (1, "John", "Doe", "1990-01-01", "COL1", "USA", 190.0, 90.0, "true", "false", None, 2010, 1, 1),
#         (None, "No", "Id", "1991-01-01", "COL2", "USA", 185.0, 80.0, "false", "true", None, 2011, 1, 2),
#     ]
#     df_raw = create_players_df(spark_session, data)
#     df_silver, df_quarantine = transform_players_data(df_raw)

#     # Only the row with personId=1 should be in silver
#     assert df_silver.filter(col("personId") == 1).count() == 1
#     assert df_silver.filter(col("personId").isNull()).count() == 0

#     # Quarantine has the row with personId NULL
#     assert df_quarantine.count() == 1
#     bad_row = df_quarantine.first()
#     assert bad_row["quarantine_reason"] == "MISSING_PERSON_ID"
#     assert bad_row["personId"] is None


# # ---------------------------------------------
# # 4) Numeric quality filters (height/weight)
# # ---------------------------------------------
# def test_numeric_quality_filters(spark_session):
#     """
#     Tests Transformation 10:
#     - height/bodyWeight range checks
#     - Out-of-range rows go to quarantine with INVALID_NUMERIC_RANGE
#     """
#     data = [
#         # Good numeric values
#         (1, "John", "Good", "1990-01-01", "COL1", "USA", 200.0, 100.0, "true", "false", None, 2010, 1, 1),
#         # Bad: height too small
#         (2, "Tiny", "Player", "1991-01-01", "COL2", "USA", 5.0, 80.0, "false", "true", None, 2011, 1, 2),
#         # Bad: height too large
#         (3, "Tall", "Player", "1992-01-01", "COL3", "USA", 500.0, 90.0, "false", "false", "true", 2012, 1, 3),
#         # Bad: weight too large
#         (4, "Heavy", "Player", "1993-01-01", "COL4", "USA", 210.0, 1500.0, "false", "false", None, 2013, 1, 4),
#     ]
#     df_raw = create_players_df(spark_session, data)
#     df_silver, df_quarantine = transform_players_data(df_raw)

#     # Only personId 1 should be valid
#     assert df_silver.count() == 1
#     assert df_silver.filter(col("personId") == 1).count() == 1

#     # Others should be in quarantine with INVALID_NUMERIC_RANGE
#     assert df_quarantine.count() == 3
#     reasons = {row["personId"]: row["quarantine_reason"] for row in df_quarantine.collect()}
#     assert reasons[2] == "INVALID_NUMERIC_RANGE"
#     assert reasons[3] == "INVALID_NUMERIC_RANGE"
#     assert reasons[4] == "INVALID_NUMERIC_RANGE"


# # ---------------------------------------------
# # 5) Deduplication on personId
# # ---------------------------------------------
# def test_deduplication_on_person_id(spark_session):
#     """
#     Tests Transformation 12:
#     - Deduplicate by personId using birth_date desc, draftYear desc
#     """
#     data = [
#         # Same personId, older birthdate
#         (1, "Old", "Record", "1985-01-01", "COL1", "USA", 200.0, 100.0, "true", "false", None, 2008, 1, 1),
#         # Same personId, newer birthdate -> should be kept
#         (1, "New", "Record", "1990-01-01", "COL2", "USA", 200.0, 100.0, "true", "false", None, 2010, 1, 2),
#         # Different personId
#         (2, "Other", "Player", "1992-01-01", "COL3", "USA", 210.0, 110.0, "false", "true", None, 2012, 1, 3),
#     ]
#     df_raw = create_players_df(spark_session, data)
#     df_silver, df_quarantine = transform_players_data(df_raw)

#     # No quarantine expected
#     assert df_quarantine.count() == 0

#     assert df_silver.count() == 2
#     # For personId=1, the kept record should be the one with newer birthdate + draftYear
#     row1 = df_silver.filter(col("personId") == 1).first()
#     assert row1["firstName"] == "New"
#     assert row1["draftYear"] == 2010

#     # personId=2 remains as-is
#     assert df_silver.filter(col("personId") == 2).count() == 1


# # ---------------------------------------------
# # 6) Metadata columns on silver/quarantine
# # ---------------------------------------------
# def test_metadata_columns_present(spark_session):
#     """
#     Tests Transformation 11 & 13:
#     - record_source, silver_ingest_ts, source_file exist on both silver & quarantine
#     - record_source and source_file values on silver are as expected
#     """
#     data = [
#         # One good row
#         (1, "John", "Doe", "1990-01-01", "COL1", "USA", 200.0, 100.0, "true", "false", None, 2010, 1, 1),
#         # One bad row (missing personId)
#         (None, "No", "Id", "1991-01-01", "COL2", "USA", 185.0, 80.0, "false", "true", None, 2011, 1, 2),
#     ]
#     df_raw = create_players_df(spark_session, data)
#     df_silver, df_quarantine = transform_players_data(df_raw)

#     # Silver: metadata columns
#     silver_row = df_silver.filter(col("personId") == 1).first()
#     assert "record_source" in silver_row.asDict()
#     assert "silver_ingest_ts" in silver_row.asDict()
#     assert "source_file" in silver_row.asDict()

#     assert silver_row["record_source"] == "NBA_PLAYERS_BRONZE"
#     assert silver_row["source_file"] == "hive_table:nba_bronze.players"
#     assert silver_row["silver_ingest_ts"] is not None

#     # Quarantine: metadata columns
#     q_row = df_quarantine.first()
#     assert "record_source" in q_row.asDict()
#     assert "silver_ingest_ts" in q_row.asDict()
#     assert "source_file" in q_row.asDict()
#     assert q_row["record_source"] == "NBA_PLAYERS_BRONZE"
#     assert q_row["silver_ingest_ts"] is not None
