# # coding: utf-8
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, trim, upper, to_timestamp, to_date, year, month,
#     current_timestamp, input_file_name, when, row_number, lit
# )
# from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType
# from pyspark.sql.window import Window

# spark = SparkSession.builder.appName("bronze_to_silver_team_statistics").getOrCreate()

# # ----------------------------------------------------------------------
# # Paths
# # ----------------------------------------------------------------------
# bronze_path = "hdfs:///tmp/DE011025/NBA/bronze/team_statistics"
# silver_path = "hdfs:///tmp/DE011025/NBA/silver/team_statistics"
# quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/team_statistics"

# # ----------------------------------------------------------------------
# # 0. Read Bronze CSV
# # ----------------------------------------------------------------------
# spark.sql("USE nba_bronze")
# df = spark.table("team_statistics")
# df = spark.sql("SELECT * FROM team_statistics")

# columns = [
#     "gameId","gameDateTimeEst","teamCity","teamName","teamId",
#     "opponentTeamCity","opponentTeamName","opponentTeamId",
#     "home","win","teamScore","opponentScore","assists","blocks","steals",
#     "fieldGoalsAttempted","fieldGoalsMade","fieldGoalsPercentage",
#     "threePointersAttempted","threePointersMade","threePointersPercentage",
#     "freeThrowsAttempted","freeThrowsMade","freeThrowsPercentage",
#     "reboundsDefensive","reboundsOffensive","reboundsTotal",
#     "foulsPersonal","turnovers","plusMinusPoints","numMinutes",
#     "q1Points","q2Points","q3Points","q4Points","benchPoints",
#     "biggestLead","biggestScoringRun","leadChanges",
#     "pointsFastBreak","pointsFromTurnovers","pointsInThePaint","pointsSecondChance",
#     "timesTied","timeoutsRemaining","seasonWins","seasonLosses","coachId"
# ]

# df_bronze = df

# # ----------------------------------------------------------------------
# # 1. Normalize fake nulls
# # ----------------------------------------------------------------------
# df_norm = df_bronze.replace(["nul", "NULL", "NA", ""], None)

# # ----------------------------------------------------------------------
# # 2. Enforce schema
# # ----------------------------------------------------------------------
# def cast_int(colname):  return col(colname).cast(IntegerType()).alias(colname)
# def cast_long(colname): return col(colname).cast(LongType()).alias(colname)
# def cast_double(colname): return col(colname).cast(DoubleType()).alias(colname)
# def cast_str(colname):  return col(colname).cast(StringType()).alias(colname)

# df_casted = df_norm.select(
#     cast_long("gameId"), cast_str("gameDateTimeEst"),
#     cast_str("teamCity"), cast_str("teamName"), cast_long("teamId"),
#     cast_str("opponentTeamCity"), cast_str("opponentTeamName"), cast_long("opponentTeamId"),
#     cast_int("home"), cast_int("win"),
#     cast_int("teamScore"), cast_int("opponentScore"),
#     cast_int("assists"), cast_int("blocks"), cast_int("steals"),
#     cast_int("fieldGoalsAttempted"), cast_int("fieldGoalsMade"), cast_double("fieldGoalsPercentage"),
#     cast_int("threePointersAttempted"), cast_int("threePointersMade"), cast_double("threePointersPercentage"),
#     cast_int("freeThrowsAttempted"), cast_int("freeThrowsMade"), cast_double("freeThrowsPercentage"),
#     cast_int("reboundsDefensive"), cast_int("reboundsOffensive"), cast_int("reboundsTotal"),
#     cast_int("foulsPersonal"), cast_int("turnovers"), cast_int("plusMinusPoints"),
#     cast_int("numMinutes"),
#     cast_int("q1Points"), cast_int("q2Points"), cast_int("q3Points"), cast_int("q4Points"),
#     cast_int("benchPoints"), cast_int("biggestLead"), cast_int("biggestScoringRun"),
#     cast_int("leadChanges"),
#     cast_int("pointsFastBreak"), cast_int("pointsFromTurnovers"),
#     cast_int("pointsInThePaint"), cast_int("pointsSecondChance"),
#     cast_int("timesTied"), cast_int("timeoutsRemaining"),
#     cast_int("seasonWins"), cast_int("seasonLosses"),
#     cast_long("coachId")
# )

# # ----------------------------------------------------------------------
# # 3. Trim whitespace
# # ----------------------------------------------------------------------
# df_trim = (
#     df_casted
#     .withColumn("teamCity", trim(col("teamCity")))
#     .withColumn("teamName", trim(col("teamName")))
#     .withColumn("opponentTeamCity", trim(col("opponentTeamCity")))
#     .withColumn("opponentTeamName", trim(col("opponentTeamName")))
# )

# # ----------------------------------------------------------------------
# # 4. Standardize casing
# # ----------------------------------------------------------------------
# df_text_norm = (
#     df_trim
#     .withColumn("teamCity", upper(col("teamCity")))
#     .withColumn("teamName", upper(col("teamName")))
#     .withColumn("opponentTeamCity", upper(col("opponentTeamCity")))
#     .withColumn("opponentTeamName", upper(col("opponentTeamName")))
# )

# # ----------------------------------------------------------------------
# # 5. Parse timestamp + derive date fields
# # ----------------------------------------------------------------------
# df_time = (
#     df_text_norm
#     .withColumn("game_ts", to_timestamp("gameDateTimeEst"))
#     .withColumn("game_date", to_date(col("game_ts")))
#     .withColumn("game_year", year(col("game_ts")))
#     .withColumn("game_month", month(col("game_ts")))
# )

# # ----------------------------------------------------------------------
# # 6. Drop rows with missing business keys
# # ----------------------------------------------------------------------
# df_bad_keys = df_time.filter(col("gameId").isNull() | col("teamId").isNull()) \
#                      .withColumn("quarantine_reason", lit("MISSING_KEYS"))

# df_non_null = df_time.filter(col("gameId").isNotNull() & col("teamId").isNotNull())

# # ----------------------------------------------------------------------
# # 7. Numeric business rules
# # ----------------------------------------------------------------------
# valid_numeric = (
#     (col("teamScore") >= 0) &
#     (col("opponentScore") >= 0) &
#     ((col("fieldGoalsPercentage").isNull()) | ((col("fieldGoalsPercentage") >= 0) & (col("fieldGoalsPercentage") <= 100))) &
#     ((col("threePointersPercentage").isNull()) | ((col("threePointersPercentage") >= 0) & (col("threePointersPercentage") <= 100))) &
#     ((col("freeThrowsPercentage").isNull()) | ((col("freeThrowsPercentage") >= 0) & (col("freeThrowsPercentage") <= 100)))
# )

# df_valid_nums = df_non_null.filter(valid_numeric)
# df_bad_nums = df_non_null.filter(~valid_numeric) \
#                          .withColumn("quarantine_reason", lit("INVALID_NUMERIC_RANGE"))

# # ----------------------------------------------------------------------
# # 8. Deduplicate by (gameId, teamId)
# # ----------------------------------------------------------------------
# w = Window.partitionBy("gameId", "teamId").orderBy(col("game_ts").desc_nulls_last())

# df_deduped = (
#     df_valid_nums
#     .withColumn("rn", row_number().over(w))
#     .filter(col("rn") == 1)
#     .drop("rn")
# )

# # ----------------------------------------------------------------------
# # 9. Derived metrics
# # ----------------------------------------------------------------------
# df_enriched = (
#     df_deduped
#     .withColumn("score_diff", col("teamScore") - col("opponentScore"))
#     .withColumn("shooting_efficiency",
#                 when(col("fieldGoalsAttempted") > 0,
#                      col("fieldGoalsMade") / col("fieldGoalsAttempted"))
#                 .otherwise(None))
# )

# # ----------------------------------------------------------------------
# # 10. Drop unwanted columns
# # ----------------------------------------------------------------------
# cols_to_drop = [
#     "timeoutsRemaining","seasonWins","seasonLosses","coachId",
#     "timesTied","pointsSecondChance","pointsInThePaint",
#     "pointsFromTurnovers","pointsFastBreak",
#     "q2Points","q3Points","plusMinusPoints",
#     "turnovers","foulsPersonal"
# ]

# df_silver_base = df_enriched.drop(*cols_to_drop)

# # ----------------------------------------------------------------------
# # 11. Add audit fields
# # ----------------------------------------------------------------------
# df_silver = (
#     df_silver_base
#     .withColumn("silver_ingest_ts", current_timestamp())
#     .withColumn("source_file", input_file_name())
# )

# # ----------------------------------------------------------------------
# # Build Quarantine DF
# # ----------------------------------------------------------------------
# df_quarantine = (
#     df_bad_keys
#     .unionByName(df_bad_nums)
#     .withColumn("quarantine_ts", current_timestamp())
# )

# # ----------------------------------------------------------------------
# # Write output
# # ----------------------------------------------------------------------
# df_silver.write.mode("overwrite").parquet(silver_path)
# df_quarantine.write.mode("overwrite").parquet(quarantine_path)

# print("Bronze count:", df_bronze.count())
# print("Silver count:", df_silver.count())
# print("Quarantine count:", df_quarantine.count())


# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, to_date, year, month,
    current_timestamp, input_file_name, when, row_number, lit
)
from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bronze_to_silver_team_statistics").getOrCreate()


def transform_team_statistics(df):
    """
    Converts Bronze → Silver for team_statistics.
    Returns:
        df_silver, df_quarantine
    """

    # ===============================================================
    # 1. Normalize NULL-like values
    # ===============================================================
    df_norm = df.replace(["nul", "NULL", "NA", ""], None)

    # ===============================================================
    # 2. Schema enforcement (casting)
    # ===============================================================
    def cast_int(c): return col(c).cast(IntegerType()).alias(c)
    def cast_long(c): return col(c).cast(LongType()).alias(c)
    def cast_double(c): return col(c).cast(DoubleType()).alias(c)
    def cast_str(c): return col(c).cast(StringType()).alias(c)

    df_casted = df_norm.select(
        cast_long("gameId"), cast_str("gameDateTimeEst"),
        cast_str("teamCity"), cast_str("teamName"), cast_long("teamId"),
        cast_str("opponentTeamCity"), cast_str("opponentTeamName"), cast_long("opponentTeamId"),
        cast_int("home"), cast_int("win"),
        cast_int("teamScore"), cast_int("opponentScore"),
        cast_int("assists"), cast_int("blocks"), cast_int("steals"),
        cast_int("fieldGoalsAttempted"), cast_int("fieldGoalsMade"), cast_double("fieldGoalsPercentage"),
        cast_int("threePointersAttempted"), cast_int("threePointersMade"), cast_double("threePointersPercentage"),
        cast_int("freeThrowsAttempted"), cast_int("freeThrowsMade"), cast_double("freeThrowsPercentage"),
        cast_int("reboundsDefensive"), cast_int("reboundsOffensive"), cast_int("reboundsTotal"),
        cast_int("foulsPersonal"), cast_int("turnovers"), cast_int("plusMinusPoints"),
        cast_int("numMinutes"),
        cast_int("q1Points"), cast_int("q2Points"), cast_int("q3Points"), cast_int("q4Points"),
        cast_int("benchPoints"), cast_int("biggestLead"), cast_int("biggestScoringRun"),
        cast_int("leadChanges"),
        cast_int("pointsFastBreak"), cast_int("pointsFromTurnovers"),
        cast_int("pointsInThePaint"), cast_int("pointsSecondChance"),
        cast_int("timesTied"), cast_int("timeoutsRemaining"),
        cast_int("seasonWins"), cast_int("seasonLosses"),
        cast_long("coachId")
    )

    # ===============================================================
    # 3. Trim whitespace
    # ===============================================================
    df_trim = (
        df_casted
        .withColumn("teamCity", trim(col("teamCity")))
        .withColumn("teamName", trim(col("teamName")))
        .withColumn("opponentTeamCity", trim(col("opponentTeamCity")))
        .withColumn("opponentTeamName", trim(col("opponentTeamName")))
    )

    # ===============================================================
    # 4. Standardize casing
    # ===============================================================
    df_text_norm = (
        df_trim
        .withColumn("teamCity", upper(col("teamCity")))
        .withColumn("teamName", upper(col("teamName")))
        .withColumn("opponentTeamCity", upper(col("opponentTeamCity")))
        .withColumn("opponentTeamName", upper(col("opponentTeamName")))
    )

    # ===============================================================
    # 5. Parse timestamp + derive date attributes
    # ===============================================================
    df_time = (
        df_text_norm
        .withColumn("game_ts", to_timestamp("gameDateTimeEst"))
        .withColumn("game_date", to_date(col("game_ts")))
        .withColumn("game_year", year(col("game_ts")))
        .withColumn("game_month", month(col("game_ts")))
    )

    # ===============================================================
    # 6. Business keys validation
    # ===============================================================
    df_bad_keys = (
        df_time.filter(col("gameId").isNull() | col("teamId").isNull())
        .withColumn("quarantine_reason", lit("MISSING_KEYS"))
    )

    df_non_null = df_time.filter(col("gameId").isNotNull() & col("teamId").isNotNull())

    # ===============================================================
    # 7. Numeric validation rules
    # ===============================================================
    valid_numeric = (
        (col("teamScore") >= 0) &
        (col("opponentScore") >= 0) &
        (col("fieldGoalsPercentage").isNull() |
         ((col("fieldGoalsPercentage") >= 0) & (col("fieldGoalsPercentage") <= 100))) &
        (col("threePointersPercentage").isNull() |
         ((col("threePointersPercentage") >= 0) & (col("threePointersPercentage") <= 100))) &
        (col("freeThrowsPercentage").isNull() |
         ((col("freeThrowsPercentage") >= 0) & (col("freeThrowsPercentage") <= 100)))
    )

    df_valid_nums = df_non_null.filter(valid_numeric)

    df_bad_nums = (
        df_non_null.filter(~valid_numeric)
        .withColumn("quarantine_reason", lit("INVALID_NUMERIC_RANGE"))
    )

    # ===============================================================
    # 8. Deduplicate (gameId + teamId)
    # ===============================================================
    w = Window.partitionBy("gameId", "teamId").orderBy(col("game_ts").desc_nulls_last())

    df_deduped = (
        df_valid_nums
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ===============================================================
    # 9. Derived metrics
    # ===============================================================
    df_enriched = (
        df_deduped
        .withColumn("score_diff", col("teamScore") - col("opponentScore"))
        .withColumn(
            "shooting_efficiency",
            when(col("fieldGoalsAttempted") > 0,
                 col("fieldGoalsMade") / col("fieldGoalsAttempted"))
            .otherwise(None)
        )
    )

    # ===============================================================
    # 10. Drop unused columns
    # ===============================================================
    drop_cols = [
        "timeoutsRemaining", "seasonWins", "seasonLosses", "coachId",
        "timesTied", "pointsSecondChance", "pointsInThePaint",
        "pointsFromTurnovers", "pointsFastBreak",
        "q2Points", "q3Points", "plusMinusPoints",
        "turnovers", "foulsPersonal"
    ]

    df_silver_base = df_enriched.drop(*drop_cols)

    # ===============================================================
    # 11. Add audit fields
    # ===============================================================
    df_silver = (
        df_silver_base
        .withColumn("silver_ingest_ts", current_timestamp())
        .withColumn("source_file", lit("test_source"))
    )

    # ===============================================================
    # 12. Build quarantine
    # ===============================================================
    df_quarantine = (
        df_bad_keys
        .unionByName(df_bad_nums)
        .withColumn("quarantine_ts", current_timestamp())
    )

    return df_silver, df_quarantine

def main(spark, source_db, source_table, silver_path, quarantine_path):
    """Main function to execute the ETL process."""
    print("starting job")
    spark.sql("USE nba_bronze")
    df = spark.table("team_statistics")
    df = spark.sql("SELECT * FROM team_statistics")
    
    # Run the transformation logic
    df_silver, df_bad = transform_team_statistics(df)
    # df_silver.write.mode("overwrite").format("parquet").saveAsTable("team_statistics_silver")
    df_silver.write \
        .mode("overwrite") \
        .parquet("hdfs:///tmp/DE011025/NBA/silver/team_statistics")
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
    source_table = "team_statistics"
    silver_path = "hdfs:///tmp/DE011025/NBA/silver/team_statistics"
    quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/team_statistics" # Example path

    try:
        main(spark, source_db, source_table, silver_path, quarantine_path)
    finally:
        spark.stop()