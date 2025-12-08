# coding: utf-8
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, trim, upper, to_timestamp, to_date, year, month,
#     current_timestamp, input_file_name, when, row_number
# )
# from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType
# from pyspark.sql.window import Window

# spark = SparkSession.builder.appName("bronze_to_silver_player_statistics").getOrCreate()

# # ------------------------------------------------------------------
# # Paths
# # ------------------------------------------------------------------
# bronze_path = "hdfs:///tmp/DE011025/NBA/bronze/player_statistics"
# silver_path = "hdfs:///tmp/DE011025/NBA/silver/player_statistics"
# quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/player_statistics"

# # ------------------------------------------------------------------
# # 0. Read Bronze CSV (raw)
# # ------------------------------------------------------------------
# spark.sql("USE nba_bronze")
# df = spark.table("player_statistics")
# df = spark.sql("SELECT * FROM player_statistics")

# columns = [
#     "firstName", "lastName", "personId", "gameId", "gameDateTimeEst",
#     "playerteamCity", "playerteamName",
#     "opponentteamCity", "opponentteamName",
#     "gameType", "gameLabel", "gameSubLabel",
#     "seriesGameNumber", "win", "home",
#     "numMinutes", "points", "assists", "blocks", "steals",
#     "fieldGoalsAttempted", "fieldGoalsMade", "fieldGoalsPercentage",
#     "threePointersAttempted", "threePointersMade", "threePointersPercentage",
#     "freeThrowsAttempted", "freeThrowsMade", "freeThrowsPercentage",
#     "reboundsDefensive", "reboundsOffensive", "reboundsTotal",
#     "foulsPersonal", "turnovers", "plusMinusPoints"
# ]

# df_bronze = df

# # ------------------------------------------------------------------
# # Transformation 1: Normalize fake nulls
# #   ("nul", "NULL", "NA", "") -> real null
# # ------------------------------------------------------------------
# df_norm_nulls = df_bronze.replace(["nul", "NULL", "NA", ""], None)

# # ------------------------------------------------------------------
# # Transformation 2: Enforce correct data types (schema casting)
# # ------------------------------------------------------------------
# df_casted = df_norm_nulls
# # df_casted = df_norm_nulls.select(
# #     col("firstName").cast(StringType()),
# #     col("lastName").cast(StringType()),
# #     col("personId").cast(LongType()),
# #     col("gameId").cast(LongType()),
# #     col("gameDateTimeEst").cast(StringType()),
# #     col("playerteamCity").cast(StringType()),
# #     col("playerteamName").cast(StringType()),
# #     col("opponentteamCity").cast(StringType()),
# #     col("opponentteamName").cast(StringType()),
# #     col("gameType").cast(StringType()),
# #     col("gameLabel").cast(StringType()),
# #     col("gameSubLabel").cast(StringType()),
# #     col("seriesGameNumber").cast(IntegerType()),
# #     col("win").cast(IntegerType()),
# #     col("home").cast(IntegerType()),
# #     col("numMinutes").cast(DoubleType()),
# #     col("points").cast(DoubleType()),
# #     col("assists").cast(DoubleType()),
# #     col("blocks").cast(DoubleType()),
# #     col("steals").cast(DoubleType()),
# #     col("fieldGoalsAttempted").cast(DoubleType()),
# #     col("fieldGoalsMade").cast(DoubleType()),
# #     col("fieldGoalsPercentage").cast(DoubleType()),
# #     col("threePointersAttempted").cast(DoubleType()),
# #     col("threePointersMade").cast(DoubleType()),
# #     col("threePointersPercentage").cast(DoubleType()),
# #     col("freeThrowsAttempted").cast(DoubleType()),
# #     col("freeThrowsMade").cast(DoubleType()),
# #     col("freeThrowsPercentage").cast(DoubleType()),
# #     col("reboundsDefensive").cast(DoubleType()),
# #     col("reboundsOffensive").cast(DoubleType()),
# #     col("reboundsTotal").cast(DoubleType()),
# #     col("foulsPersonal").cast(DoubleType()),
# #     col("turnovers").cast(DoubleType()),
# #     col("plusMinusPoints").cast(DoubleType())
# # )

# # ------------------------------------------------------------------
# # Transformation 3: Trim whitespace on important text columns
# # ------------------------------------------------------------------
# df_trimmed = df_casted.withColumn("firstName", trim(col("firstName"))) \
#                       .withColumn("lastName", trim(col("lastName"))) \
#                       .withColumn("playerteamCity", trim(col("playerteamCity"))) \
#                       .withColumn("playerteamName", trim(col("playerteamName"))) \
#                       .withColumn("opponentteamCity", trim(col("opponentteamCity"))) \
#                       .withColumn("opponentteamName", trim(col("opponentteamName"))) \
#                       .withColumn("gameType", trim(col("gameType")))

# # ------------------------------------------------------------------
# # Transformation 4: Standardize casing for categorical text
# # ------------------------------------------------------------------
# df_text_norm = df_trimmed.withColumn("gameType", upper(col("gameType"))) \
#                          .withColumn("playerteamName", upper(col("playerteamName"))) \
#                          .withColumn("opponentteamName", upper(col("opponentteamName")))

# # ------------------------------------------------------------------
# # Transformation 5: Parse gameDateTimeEst to timestamp + date parts
# # ------------------------------------------------------------------
# df_time = df_text_norm.withColumn("gameDateTimeEst", to_timestamp("gameDateTimeEst")) \
#                       .withColumn("game_date", to_date("gameDateTimeEst")) \
#                       .withColumn("game_year", year("gameDateTimeEst")) \
#                       .withColumn("game_month", month("gameDateTimeEst"))

# # ------------------------------------------------------------------
# # Transformation 6: Drop rows with null business keys (personId, gameId)
# # ------------------------------------------------------------------
# required_keys = ["personId", "gameId"]
# df_non_null_keys = df_time.dropna(subset=required_keys)

# # ------------------------------------------------------------------
# # Transformation 7: Business rule checks on numeric ranges
# #   - minutes, points, etc. >= 0
# #   - shooting percentages between 0 and 100 if not null
# # ------------------------------------------------------------------
# valid_numeric = (
#     (col("numMinutes").isNull() | (col("numMinutes") >= 0)) &
#     (col("points").isNull() | (col("points") >= 0)) &
#     (col("assists").isNull() | (col("assists") >= 0)) &
#     (col("blocks").isNull() | (col("blocks") >= 0)) &
#     (col("steals").isNull() | (col("steals") >= 0)) &
#     (col("fieldGoalsPercentage").isNull() | ((col("fieldGoalsPercentage") >= 0) & (col("fieldGoalsPercentage") <= 100))) &
#     (col("threePointersPercentage").isNull() | ((col("threePointersPercentage") >= 0) & (col("threePointersPercentage") <= 100))) &
#     (col("freeThrowsPercentage").isNull() | ((col("freeThrowsPercentage") >= 0) & (col("freeThrowsPercentage") <= 100)))
# )

# df_valid_nums = df_non_null_keys.filter(valid_numeric)
# df_bad_nums = df_non_null_keys.filter(~valid_numeric)

# # ------------------------------------------------------------------
# # Transformation 8: Deduplicate by (personId, gameId)
# #   Keep the latest record by game_ts if duplicates
# # ------------------------------------------------------------------
# w = Window.partitionBy("personId", "gameId").orderBy(col("gameDateTimeEst").desc_nulls_last())

# df_deduped = df_valid_nums.withColumn("rn", row_number().over(w)) \
#                           .filter(col("rn") == 1) \
#                           .drop("rn")

# # ------------------------------------------------------------------
# # Transformation 9: Add derived metric (points per minute)
# # ------------------------------------------------------------------
# df_enriched = df_deduped.withColumn(
#     "points_per_minute",
#     when((col("numMinutes").isNotNull()) & (col("numMinutes") > 0),
#          col("points") / col("numMinutes")
#     ).otherwise(None)
# )

# # ------------------------------------------------------------------
# # Transformation 10: Add audit / lineage columns
# #   - silver_ingest_ts
# #   - source_file (input_file_name)
# # ------------------------------------------------------------------
# df_silver = df_enriched.withColumn("silver_ingest_ts", current_timestamp()) \
#                        .withColumn("source_file", input_file_name())

# # ------------------------------------------------------------------
# # Build quarantine DF from bad keys, bad numeric, bad timestamp
# # ------------------------------------------------------------------
# from pyspark.sql.functions import lit



# # ------------------------------------------------------------------
# # Write Silver and Quarantine
# # ------------------------------------------------------------------
# df_silver.write \
#     .mode("overwrite") \
#     .parquet("hdfs:///tmp/DE011025/NBA/silver/player_statistics")


# print("Bronze count:", df_bronze.count())
# print("Silver count:", df_silver.count())

# silver_player_statistics.py

# from pyspark.sql.functions import (
#     col, trim, upper, to_timestamp, to_date, year, month,
#     current_timestamp, when, row_number, lit
# )
# from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, to_date, year, month,
    current_timestamp, input_file_name, when, lit, row_number
)
from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bronze_to_silver_player_statistics").getOrCreate()

def transform_player_statistics(df):
    """
    Clean & transform Bronze player_statistics → Silver + Quarantine.
    Mirrors the structure of transform_games_data() for pytest.
    """

    # ------------------------------------------------------------------
    # 1. Normalize fake nulls
    # ------------------------------------------------------------------
    df_clean = df.replace(["nul", "NULL", "NA", ""], None)

    # ------------------------------------------------------------------
    # 2. Type casting (kept identical)
    # ------------------------------------------------------------------
    df_casted = df_clean

    # ------------------------------------------------------------------
    # 3. Trim text fields
    # ------------------------------------------------------------------
    df_trimmed = (
        df_casted
        .withColumn("firstName", trim(col("firstName")))
        .withColumn("lastName", trim(col("lastName")))
        .withColumn("playerteamCity", trim(col("playerteamCity")))
        .withColumn("playerteamName", trim(col("playerteamName")))
        .withColumn("opponentteamCity", trim(col("opponentteamCity")))
        .withColumn("opponentteamName", trim(col("opponentteamName")))
        .withColumn("gameType", trim(col("gameType")))
    )

    # ------------------------------------------------------------------
    # 4. Standardize casing
    # ------------------------------------------------------------------
    df_text_norm = (
        df_trimmed
        .withColumn("gameType", upper(col("gameType")))
        .withColumn("playerteamName", upper(col("playerteamName")))
        .withColumn("opponentteamName", upper(col("opponentteamName")))
    )

    # ------------------------------------------------------------------
    # 5. Parse timestamp & date
    # ------------------------------------------------------------------
    df_time = (
        df_text_norm
        .withColumn("gameDateTimeEst", to_timestamp("gameDateTimeEst"))
        .withColumn("game_date", to_date("gameDateTimeEst"))
        .withColumn("game_year", year("gameDateTimeEst"))
        .withColumn("game_month", month("gameDateTimeEst"))
    )

    # ------------------------------------------------------------------
    # 6. Missing business keys - quarantine
    # ------------------------------------------------------------------
    required_keys = ["personId", "gameId"]

    df_bad_keys = (
        df_time
        .filter(col("personId").isNull() | col("gameId").isNull())
        .withColumn("quarantine_reason", lit("MISSING_KEYS"))
    )

    df_good_keys = df_time.dropna(subset=required_keys)

    # ------------------------------------------------------------------
    # 7. Numeric rule checks
    # ------------------------------------------------------------------
    valid_numeric = (
        (col("numMinutes").isNull() | (col("numMinutes") >= 0)) &
        (col("points").isNull() | (col("points") >= 0)) &
        (col("assists").isNull() | (col("assists") >= 0)) &
        (col("blocks").isNull() | (col("blocks") >= 0)) &
        (col("steals").isNull() | (col("steals") >= 0)) &
        (col("fieldGoalsPercentage").isNull() |
         ((col("fieldGoalsPercentage") >= 0) & (col("fieldGoalsPercentage") <= 100))) &
        (col("threePointersPercentage").isNull() |
         ((col("threePointersPercentage") >= 0) & (col("threePointersPercentage") <= 100))) &
        (col("freeThrowsPercentage").isNull() |
         ((col("freeThrowsPercentage") >= 0) & (col("freeThrowsPercentage") <= 100)))
    )

    df_valid_nums = df_good_keys.filter(valid_numeric)

    df_bad_numeric = (
        df_good_keys
        .filter(~valid_numeric)
        .withColumn("quarantine_reason", lit("INVALID_NUMERIC_RANGE"))
    )

    # ------------------------------------------------------------------
    # 8. Deduplicate by (personId, gameId)
    # ------------------------------------------------------------------
    w = Window.partitionBy("personId", "gameId") \
              .orderBy(col("gameDateTimeEst").desc_nulls_last())

    df_deduped = (
        df_valid_nums
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ------------------------------------------------------------------
    # 9. Derived metric: points per minute
    # ------------------------------------------------------------------
    df_enriched = (
        df_deduped
        .withColumn(
            "points_per_minute",
            when((col("numMinutes") > 0), col("points") / col("numMinutes"))
        )
    )

    # ------------------------------------------------------------------
    # 10. Add audit fields
    # ------------------------------------------------------------------
    df_silver = (
        df_enriched
        .withColumn("silver_ingest_ts", current_timestamp())
        .withColumn("source_file", lit("test_source"))
    )

    # ------------------------------------------------------------------
    # Final Quarantine DF
    # ------------------------------------------------------------------
    df_quarantine = (
        df_bad_keys
        .unionByName(df_bad_numeric)
        .withColumn("silver_ingest_ts", current_timestamp())
        .withColumn("source_file", lit("test_source"))
    )

    return df_silver, df_quarantine


def main(spark, source_db, source_table, silver_path, quarantine_path):
    """Main function to execute the ETL process."""
    print("starting job")
    spark.sql("USE nba_bronze")
    df = spark.table("player_statistics")
    df = spark.sql("SELECT * FROM player_statistics")
    
    # Run the transformation logic
    df_silver, df_bad = transform_player_statistics(df)
    # df_silver.write.mode("overwrite").format("parquet").saveAsTable("player_statistics_silver")
    df_silver.repartition(10)
    df_silver.write \
        .mode("overwrite") \
        .parquet("hdfs:///tmp/DE011025/NBA/silver/player_statistics")
    # #df_bad.write.mode("overwrite").parquet(quarantine_path)
    spark.stop()
    
if __name__ == "__main__":
    print("starting job")
    spark = (
    SparkSession.builder
        .appName("list_hive_dbs")
        .enableHiveSupport()   # make sure this is here
        .getOrCreate()
)

    
    # Initialize Spark Session
    # Define parameters (can be passed via sys.argv for a production script)
    # For simplicity, we hardcode the target paths here, as in the original code
    source_db = "nba_bronze"
    source_table = "player_statistics"
    silver_path = "hdfs:///tmp/DE011025/NBA/silver/player_statistics"
    quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/player_statistics" # Example path

    try:
        main(spark, source_db, source_table, silver_path, quarantine_path)
    finally:
        spark.stop()