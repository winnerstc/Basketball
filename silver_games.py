# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, to_timestamp, to_date,
    year, month, current_timestamp, input_file_name, row_number, lit
)
from pyspark.sql.types import LongType, IntegerType, StringType
from pyspark.sql.window import Window

# # ------------------------------------------------------------------
# # HDFS Paths
# # ------------------------------------------------------------------

# # ------------------------------------------------------------------
# # 1. Read Bronze CSV (raw)
# # ------------------------------------------------------------------

# # ------------------------------------------------------------------
# # 2. Normalize fake nulls
# # ------------------------------------------------------------------
# df_clean = df.replace(["nul", "NULL", "NA", ""], None)

# df_typed = df_clean
# # ------------------------------------------------------------------
# # 3. Cast to proper data types
# # ------------------------------------------------------------------
# # df_typed = df_clean.select(
# #     col("gameid").cast(LongType()),
# #     col("gamedatetimeest").cast(StringType()),
# #     col("hometeamcity").cast(StringType()),
# #     col("hometeamname").cast(StringType()),
# #     col("hometeamid").cast(LongType()),
# #     col("awayteamcity").cast(StringType()),
# #     col("awayteamname").cast(StringType()),
# #     col("awayteamid").cast(LongType()),
# #     col("homescore").cast(IntegerType()),
# #     col("awayscore").cast(IntegerType()),
# #     col("winner").cast(LongType()),
# #     col("gametype").cast(StringType()),
# #     col("attendance").cast(IntegerType()),
# #     col("arenaid").cast(IntegerType()),
# #     col("gamelabel").cast(StringType()),
# #     col("gamesublabel").cast(StringType()),
# #     col("seriesgamenumber").cast(IntegerType()),
# #     col("season").cast(IntegerType())
# # )

# # ------------------------------------------------------------------
# # 4. Parse timestamps + date fields
# # ------------------------------------------------------------------
# df_time = df_typed.withColumn("game_ts", to_timestamp("gamedatetimeest")) \
#                   .withColumn("game_date", to_date(col("game_ts"))) \
#                   .withColumn("game_year", year("game_ts")) \
#                   .withColumn("game_month", month("game_ts"))

# # ------------------------------------------------------------------
# # 5. Drop rows with missing critical IDs → Silver vs Quarantine
# # ------------------------------------------------------------------
# required = ["gameid", "hometeamid", "awayteamid", "arenaid"]

# df_silver_base = df_time.dropna(subset=required)

# df_bad = df_time.filter(
#     col("gameid").isNull() |
#     col("hometeamid").isNull() |
#     col("awayteamid").isNull() |
#     col("arenaid").isNull()
# ).withColumn("quarantine_reason", lit("MISSING_KEY_FIELDS"))

# # ------------------------------------------------------------------
# # 6. Trim and normalize text columns
# # ------------------------------------------------------------------
# df_silver_base = (
#     df_silver_base
#     .withColumn("hometeamcity", trim(col("hometeamcity")))
#     .withColumn("awayteamcity", trim(col("awayteamcity")))
#     .withColumn("gametype", trim(col("gametype")))
# )

# # ------------------------------------------------------------------
# # 7. Standardize casing
# # ------------------------------------------------------------------
# df_silver_base = (
#     df_silver_base
#     .withColumn("gametype", upper(col("gametype")))
#     .withColumn("hometeamname", upper(col("hometeamname")))
#     .withColumn("awayteamname", upper(col("awayteamname")))
# )

# # ------------------------------------------------------------------
# # 8. Derived outcome metrics (home/away win, score diff)
# # ------------------------------------------------------------------
# df_silver_base = df_silver_base.withColumn(
#     "home_win", when(col("homescore") > col("awayscore"), 1).otherwise(0)
# ).withColumn(
#     "away_win", when(col("awayscore") > col("homescore"), 1).otherwise(0)
# ).withColumn(
#     "score_diff", col("homescore") - col("awayscore")
# )

# # ------------------------------------------------------------------
# # 9. Filter impossible numeric values (Silver quality metrics)
# # ------------------------------------------------------------------
# df_silver_base = (
#     df_silver_base
#     .filter(col("homescore") >= 0)
#     .filter(col("awayscore") >= 0)
#     .filter(col("attendance").isNull() | ((col("attendance") >= 0) & (col("attendance") <= 30000)))
#     .filter((col("game_year") >= 1946) & (col("game_year") <= 2100))
# )

# # ------------------------------------------------------------------
# # 10. Deduplicate by gameid (keep latest timestamp)
# # ------------------------------------------------------------------
# w = Window.partitionBy("gameid").orderBy(col("game_ts").desc_nulls_last())

# df_silver_base = (
#     df_silver_base
#     .withColumn("rn", row_number().over(w))
#     .filter(col("rn") == 1)
#     .drop("rn")
# )

# # ------------------------------------------------------------------
# # 11. Add audit fields
# # ------------------------------------------------------------------
# df_silver = (
#     df_silver_base
#     .withColumn("silver_ingest_ts", current_timestamp())
#     .withColumn("source_file", input_file_name())
# )
# print(df_silver)
# # ------------------------------------------------------------------
# # Write Silver + Quarantine
# # ------------------------------------------------------------------
# df_silver.write \
#     .mode("overwrite") \
#     .parquet("hdfs:///tmp/DE011025/NBA/silver/games")
# #df_bad.write.mode("overwrite").parquet(quarantine_path)
# spark.stop()

def transform_games_data(df):
    """Applies all cleaning and transformation steps to the raw games DataFrame."""
    print("starting job")
    # 2. Normalize fake nulls
    df_clean = df.replace(["nul", "NULL", "NA", ""], None)
    df_typed = df_clean # Assuming casting is handled upstream or not needed for testing raw types

    # 4. Parse timestamps + date fields
    df_time = df_typed.withColumn("game_ts", to_timestamp("gamedatetimeest")) \
                      .withColumn("game_date", to_date(col("game_ts"))) \
                      .withColumn("game_year", year("game_ts")) \
                      .withColumn("game_month", month("game_ts"))

    # 5. Drop rows with missing critical IDs (Silver vs Quarantine)
    required = ["gameid", "hometeamid", "awayteamid", "arenaid"]
    df_silver_base = df_time.dropna(subset=required)

    df_bad = df_time.filter(
        col("gameid").isNull() |
        col("hometeamid").isNull() |
        col("awayteamid").isNull() |
        col("arenaid").isNull()
    ).withColumn("quarantine_reason", lit("MISSING_KEY_FIELDS"))

    # 6. Trim and normalize text columns
    df_silver_base = (
        df_silver_base
        .withColumn("hometeamcity", trim(col("hometeamcity")))
        .withColumn("awayteamcity", trim(col("awayteamcity")))
        .withColumn("gametype", trim(col("gametype")))
    )

    # 7. Standardize casing
    df_silver_base = (
        df_silver_base
        .withColumn("gametype", upper(col("gametype")))
        .withColumn("hometeamname", upper(col("hometeamname")))
        .withColumn("awayteamname", upper(col("awayteamname")))
    )

    # 8. Derived outcome metrics (home/away win, score diff)
    df_silver_base = df_silver_base.withColumn(
        "home_win", when(col("homescore") > col("awayscore"), 1).otherwise(0)
    ).withColumn(
        "away_win", when(col("awayscore") > col("homescore"), 1).otherwise(0)
    ).withColumn(
        "score_diff", col("homescore") - col("awayscore")
    )

    # 9. Filter impossible numeric values
    df_silver_base = (
        df_silver_base
        .filter(col("homescore") >= 0)
        .filter(col("awayscore") >= 0)
        .filter(col("attendance").isNull() | ((col("attendance") >= 0) & (col("attendance") <= 30000)))
        .filter((col("game_year") >= 1946) & (col("game_year") <= 2100))
    )

    # 10. Deduplicate by gameid (keep latest timestamp)
    w = Window.partitionBy("gameid").orderBy(col("game_ts").desc_nulls_last())

    df_silver_base = (
        df_silver_base
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # 11. Add audit fields
    df_silver = (
        df_silver_base
        .withColumn("silver_ingest_ts", current_timestamp())
        .withColumn("source_file", lit("test_file.csv")) # Use lit for testing
    )
    
    return df_silver, df_bad

def main(spark, source_db, source_table, silver_path, quarantine_path):
    """Main function to execute the ETL process."""
    print("starting job")
    spark.sql("USE nba_bronze")
    df = spark.table("games")
    df = spark.sql("SELECT * FROM games")
    
    # Run the transformation logic
    df_silver, df_bad = transform_games_data(df)

    df_silver.write \
        .mode("overwrite") \
        .parquet("hdfs:///tmp/DE011025/NBA/silver/games")
    #df_bad.write.mode("overwrite").parquet(quarantine_path)
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
    source_table = "games"
    silver_path = "hdfs:///tmp/DE011025/NBA/silver/games"
    quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/games" # Example path

    try:
        main(spark, source_db, source_table, silver_path, quarantine_path)
    finally:
        spark.stop()