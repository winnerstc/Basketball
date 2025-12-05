# coding: utf-8
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, trim, upper, to_timestamp, to_date, year, month,
#     current_timestamp, input_file_name, when, row_number, lit, initcap, concat_ws, current_date
# )
# from pyspark.sql.types import LongType, IntegerType, StringType
# from pyspark.sql.window import Window

# spark = SparkSession.builder.appName("bronze_to_silver_team_histories").getOrCreate()

# # ----------------------------------------------------------------------
# # Paths
# # ----------------------------------------------------------------------
# bronze_path = "hdfs:///tmp/DE011025/NBA/bronze/team_histories"
# silver_path = "hdfs:///tmp/DE011025/NBA/silver/team_histories"
# quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/team_histories"

# # ----------------------------------------------------------------------
# # 0. Read Bronze TEXTFILE as CSV (comma-separated, no header)
# # ----------------------------------------------------------------------
# spark.sql("USE nba_bronze")
# df = spark.table("team_histories")
# df = spark.sql("SELECT * FROM team_histories")
# columns = [
#     "teamId",
#     "teamCity",
#     "teamName",
#     "teamAbbrev",
#     "seasonFounded",
#     "seasonActiveTill",
#     "league"
# ]

# df_bronze = df

# # ----------------------------------------------------------------------
# # Transformation 1: Normalize fake nulls to real NULL
# # ----------------------------------------------------------------------
# df_norm_nulls = df_bronze.replace(["nul", "NULL", "NA", ""], None)

# # ----------------------------------------------------------------------
# # Transformation 2: Enforce correct data types (schema casting)
# # ----------------------------------------------------------------------
# df_casted = df_norm_nulls.select(
#     col("teamId").cast(LongType()).alias("teamId"),
#     col("teamCity").cast(StringType()).alias("teamCity"),
#     col("teamName").cast(StringType()).alias("teamName"),
#     col("teamAbbrev").cast(StringType()).alias("teamAbbrev"),
#     col("seasonFounded").cast(IntegerType()).alias("seasonFounded"),
#     col("seasonActiveTill").cast(IntegerType()).alias("seasonActiveTill"),
#     col("league").cast(StringType()).alias("league")
# )

# # ----------------------------------------------------------------------
# # Transformation 3: Trim whitespace on string columns
# # ----------------------------------------------------------------------
# df_trimmed = (
#     df_casted
#     .withColumn("teamCity", trim(col("teamCity")))
#     .withColumn("teamName", trim(col("teamName")))
#     .withColumn("teamAbbrev", trim(col("teamAbbrev")))
#     .withColumn("league", trim(col("league")))
# )

# # ----------------------------------------------------------------------
# # Transformation 4: Standardize casing on text
# #   - City / Name to InitCap / UPPER
# #   - Abbrev & League to UPPER
# # ----------------------------------------------------------------------
# df_text_norm = (
#     df_trimmed
#     .withColumn("teamCity", initcap(col("teamCity")))
#     .withColumn("teamName", upper(col("teamName")))
#     .withColumn("teamAbbrev", upper(col("teamAbbrev")))
#     .withColumn("league", upper(col("league")))
# )

# # ----------------------------------------------------------------------
# # Transformation 5: Derive team_full_name (CITY + NAME)
# # ----------------------------------------------------------------------
# df_with_fullname = df_text_norm.withColumn(
#     "team_full_name",
#     concat_ws(" ", col("teamCity"), col("teamName"))
# )

# # ----------------------------------------------------------------------
# # Transformation 6: Derive active_flag
# #   - If seasonActiveTill is NULL or >= current season => active
# #   - Else inactive
# # ----------------------------------------------------------------------
# current_season = year(current_date())   # rough, but fine for demo

# df_with_active = df_with_fullname.withColumn(
#     "active_flag",
#     (col("seasonActiveTill").isNull() | (col("seasonActiveTill") >= current_season)).cast("int")
# )

# # ----------------------------------------------------------------------
# # Transformation 7: Business rules on numeric ranges
# #   - seasonFounded: [1900, current_season]
# #   - seasonActiveTill >= seasonFounded (if both not null)
# # ----------------------------------------------------------------------
# valid_numeric = (
#     (col("seasonFounded").isNull() |
#      ((col("seasonFounded") >= 1900) & (col("seasonFounded") <= current_season))) &
#     (
#         col("seasonActiveTill").isNull() |
#         col("seasonFounded").isNull() |
#         (col("seasonActiveTill") >= col("seasonFounded"))
#     )
# )

# df_numeric_ok = df_with_active.filter(valid_numeric)
# df_bad_numeric = df_with_active.filter(~valid_numeric) \
#                                .withColumn("quarantine_reason", lit("INVALID_SEASON_RANGE"))

# # ----------------------------------------------------------------------
# # Transformation 8: Drop rows with missing business keys (teamId or teamName)
# # ----------------------------------------------------------------------
# df_bad_keys = df_numeric_ok.filter(
#     col("teamId").isNull() | col("teamName").isNull()
# ).withColumn("quarantine_reason", lit("MISSING_KEYS"))

# df_keys_ok = df_numeric_ok.filter(
#     col("teamId").isNotNull() & col("teamName").isNotNull()
# )

# # ----------------------------------------------------------------------
# # Transformation 9: Deduplicate by teamId
# #   Keep the row with:
# #     - latest seasonActiveTill (greatest)
# #     - if tie, latest seasonFounded
# # ----------------------------------------------------------------------
# w = Window.partitionBy("teamId").orderBy(
#     col("seasonActiveTill").desc_nulls_last(),
#     col("seasonFounded").desc_nulls_last()
# )

# df_deduped = (
#     df_keys_ok
#     .withColumn("rn", row_number().over(w))
#     .filter(col("rn") == 1)
#     .drop("rn")
# )

# # ----------------------------------------------------------------------
# # Transformation 10: Add audit / lineage columns
# # ----------------------------------------------------------------------
# df_silver = (
#     df_deduped
#     .withColumn("silver_ingest_ts", current_timestamp())
#     .withColumn("source_file", input_file_name())
# )

# # ----------------------------------------------------------------------
# # Build final Quarantine DF (numeric issues + missing keys)
# # ----------------------------------------------------------------------
# df_quarantine = (
#     df_bad_numeric
#     .unionByName(df_bad_keys)
#     .withColumn("quarantine_ts", current_timestamp())
# )

# # ----------------------------------------------------------------------
# # Write Silver and Quarantine
# # ----------------------------------------------------------------------
# df_silver.write.mode("overwrite").parquet(silver_path)
# df_quarantine.write.mode("overwrite").parquet(quarantine_path)

# print("Bronze count:", df_bronze.count())
# print("Silver count:", df_silver.count())
# print("Quarantine count:", df_quarantine.count())


# silver_team_histories.py

# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, to_date, year, month,
    current_timestamp, input_file_name, when, row_number, lit, initcap, concat_ws, current_date
)
from pyspark.sql.types import LongType, IntegerType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bronze_to_silver_team_histories").getOrCreate()


def transform_team_histories(df):
    """
    Applies all Bronze → Silver transformations for team_histories.
    Returns: df_silver, df_quarantine
    """

    # ----------------------------------------------------------------------
    # 1. Normalize fake nulls
    # ----------------------------------------------------------------------
    df_norm_nulls = df.replace(["nul", "NULL", "NA", ""], None)

    # ----------------------------------------------------------------------
    # 2. Enforce schema (cast types)
    # ----------------------------------------------------------------------
    df_casted = df_norm_nulls.select(
        col("teamId").cast(LongType()).alias("teamId"),
        col("teamCity").cast(StringType()).alias("teamCity"),
        col("teamName").cast(StringType()).alias("teamName"),
        col("teamAbbrev").cast(StringType()).alias("teamAbbrev"),
        col("seasonFounded").cast(IntegerType()).alias("seasonFounded"),
        col("seasonActiveTill").cast(IntegerType()).alias("seasonActiveTill"),
        col("league").cast(StringType()).alias("league")
    )

    # ----------------------------------------------------------------------
    # 3. Trim whitespace
    # ----------------------------------------------------------------------
    df_trimmed = (
        df_casted
        .withColumn("teamCity", trim(col("teamCity")))
        .withColumn("teamName", trim(col("teamName")))
        .withColumn("teamAbbrev", trim(col("teamAbbrev")))
        .withColumn("league", trim(col("league")))
    )

    # ----------------------------------------------------------------------
    # 4. Standardize casing
    # ----------------------------------------------------------------------
    df_text_norm = (
        df_trimmed
        .withColumn("teamCity", initcap(col("teamCity")))
        .withColumn("teamName", upper(col("teamName")))
        .withColumn("teamAbbrev", upper(col("teamAbbrev")))
        .withColumn("league", upper(col("league")))
    )

    # ----------------------------------------------------------------------
    # 5. team_full_name
    # ----------------------------------------------------------------------
    df_with_fullname = df_text_norm.withColumn(
        "team_full_name",
        concat_ws(" ", col("teamCity"), col("teamName"))
    )

    # ----------------------------------------------------------------------
    # 6. active_flag based on seasonActiveTill
    # ----------------------------------------------------------------------
    current_season = year(current_date())

    df_with_active = df_with_fullname.withColumn(
        "active_flag",
        (
            col("seasonActiveTill").isNull() |
            (col("seasonActiveTill") >= current_season)
        ).cast("int")
    )

    # ----------------------------------------------------------------------
    # 7. Business rules – numeric validations
    # ----------------------------------------------------------------------
    valid_numeric = (
        (col("seasonFounded").isNull() |
         ((col("seasonFounded") >= 1900) & (col("seasonFounded") <= current_season)))
        &
        (
            col("seasonActiveTill").isNull() |
            col("seasonFounded").isNull() |
            (col("seasonActiveTill") >= col("seasonFounded"))
        )
    )

    df_numeric_ok = df_with_active.filter(valid_numeric)

    df_bad_numeric = (
        df_with_active
        .filter(~valid_numeric)
        .withColumn("quarantine_reason", lit("INVALID_SEASON_RANGE"))
    )

    # ----------------------------------------------------------------------
    # 8. Missing business keys (teamId, teamName)
    # ----------------------------------------------------------------------
    df_bad_keys = (
        df_numeric_ok
        .filter(col("teamId").isNull() | col("teamName").isNull())
        .withColumn("quarantine_reason", lit("MISSING_KEYS"))
    )

    df_keys_ok = df_numeric_ok.filter(
        col("teamId").isNotNull() & col("teamName").isNotNull()
    )

    # ----------------------------------------------------------------------
    # 9. Deduplicate based on teamId
    #    Priority: seasonActiveTill desc, seasonFounded desc
    # ----------------------------------------------------------------------
    w = Window.partitionBy("teamId").orderBy(
        col("seasonActiveTill").desc_nulls_last(),
        col("seasonFounded").desc_nulls_last()
    )

    df_deduped = (
        df_keys_ok
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ----------------------------------------------------------------------
    # 10. Add audit metadata
    # ----------------------------------------------------------------------
    df_silver = (
        df_deduped
        .withColumn("silver_ingest_ts", current_timestamp())
        .withColumn("source_file", lit("test_source"))
    )

    # ----------------------------------------------------------------------
    # Build final Quarantine DF
    # ----------------------------------------------------------------------
    df_quarantine = (
        df_bad_numeric
        .unionByName(df_bad_keys)
        .withColumn("quarantine_ts", current_timestamp())
        .withColumn("source_file", lit("test_source"))
    )

    return df_silver, df_quarantine

def main(spark, source_db, source_table, silver_path, quarantine_path):
    """Main function to execute the ETL process."""
    print("starting job")
    spark.sql("USE nba_bronze")
    df = spark.table("team_histories")
    df = spark.sql("SELECT * FROM team_histories")
    
    # Run the transformation logic
    df_silver, df_bad = transform_team_histories(df)

    df_silver.write \
        .mode("overwrite") \
        .parquet("hdfs:///tmp/DE011025/NBA/silver/team_histories")
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
    source_table = "team_histories"
    silver_path = "hdfs:///tmp/DE011025/NBA/silver/team_histories"
    quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/team_histories" # Example path

    try:
        main(spark, source_db, source_table, silver_path, quarantine_path)
    finally:
        spark.stop()