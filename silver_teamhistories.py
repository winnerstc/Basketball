from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, initcap,
    current_timestamp, current_date, year,
    row_number, lit, concat_ws, greatest
)
from pyspark.sql.types import LongType, IntegerType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bronze_to_silver_team_histories").getOrCreate()

# ----------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------
bronze_path = "hdfs:///tmp/DE011025/NBA/bronze/team_histories"
silver_path = "hdfs:///tmp/DE011025/NBA/silver/team_histories"
quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/team_histories"

# ----------------------------------------------------------------------
# 0. Read Bronze TEXTFILE as CSV (comma-separated, no header)
# ----------------------------------------------------------------------
df_bronze_raw = spark.read.csv(
    bronze_path,
    sep=",",
    header=False,
    inferSchema=True,
    nullValue="null"    # "null" -> actual null
)

columns = [
    "teamId",
    "teamCity",
    "teamName",
    "teamAbbrev",
    "seasonFounded",
    "seasonActiveTill",
    "league"
]

df_bronze = df_bronze_raw.toDF(*columns)

# ----------------------------------------------------------------------
# Transformation 1: Normalize fake nulls to real NULL
# ----------------------------------------------------------------------
df_norm_nulls = df_bronze.replace(["nul", "NULL", "NA", ""], None)

# ----------------------------------------------------------------------
# Transformation 2: Enforce correct data types (schema casting)
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
# Transformation 3: Trim whitespace on string columns
# ----------------------------------------------------------------------
df_trimmed = (
    df_casted
    .withColumn("teamCity", trim(col("teamCity")))
    .withColumn("teamName", trim(col("teamName")))
    .withColumn("teamAbbrev", trim(col("teamAbbrev")))
    .withColumn("league", trim(col("league")))
)

# ----------------------------------------------------------------------
# Transformation 4: Standardize casing on text
#   - City / Name to InitCap / UPPER
#   - Abbrev & League to UPPER
# ----------------------------------------------------------------------
df_text_norm = (
    df_trimmed
    .withColumn("teamCity", initcap(col("teamCity")))
    .withColumn("teamName", upper(col("teamName")))
    .withColumn("teamAbbrev", upper(col("teamAbbrev")))
    .withColumn("league", upper(col("league")))
)

# ----------------------------------------------------------------------
# Transformation 5: Derive team_full_name (CITY + NAME)
# ----------------------------------------------------------------------
df_with_fullname = df_text_norm.withColumn(
    "team_full_name",
    concat_ws(" ", col("teamCity"), col("teamName"))
)

# ----------------------------------------------------------------------
# Transformation 6: Derive active_flag
#   - If seasonActiveTill is NULL or >= current season => active
#   - Else inactive
# ----------------------------------------------------------------------
current_season = year(current_date())   # rough, but fine for demo

df_with_active = df_with_fullname.withColumn(
    "active_flag",
    (col("seasonActiveTill").isNull() | (col("seasonActiveTill") >= current_season)).cast("int")
)

# ----------------------------------------------------------------------
# Transformation 7: Business rules on numeric ranges
#   - seasonFounded: [1900, current_season]
#   - seasonActiveTill >= seasonFounded (if both not null)
# ----------------------------------------------------------------------
valid_numeric = (
    (col("seasonFounded").isNull() |
     ((col("seasonFounded") >= 1900) & (col("seasonFounded") <= current_season))) &
    (
        col("seasonActiveTill").isNull() |
        col("seasonFounded").isNull() |
        (col("seasonActiveTill") >= col("seasonFounded"))
    )
)

df_numeric_ok = df_with_active.filter(valid_numeric)
df_bad_numeric = df_with_active.filter(~valid_numeric) \
                               .withColumn("quarantine_reason", lit("INVALID_SEASON_RANGE"))

# ----------------------------------------------------------------------
# Transformation 8: Drop rows with missing business keys (teamId or teamName)
# ----------------------------------------------------------------------
df_bad_keys = df_numeric_ok.filter(
    col("teamId").isNull() | col("teamName").isNull()
).withColumn("quarantine_reason", lit("MISSING_KEYS"))

df_keys_ok = df_numeric_ok.filter(
    col("teamId").isNotNull() & col("teamName").isNotNull()
)

# ----------------------------------------------------------------------
# Transformation 9: Deduplicate by teamId
#   Keep the row with:
#     - latest seasonActiveTill (greatest)
#     - if tie, latest seasonFounded
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
# Transformation 10: Add audit / lineage columns
# ----------------------------------------------------------------------
df_silver = (
    df_deduped
    .withColumn("silver_ingest_ts", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# ----------------------------------------------------------------------
# Build final Quarantine DF (numeric issues + missing keys)
# ----------------------------------------------------------------------
df_quarantine = (
    df_bad_numeric
    .unionByName(df_bad_keys, allowMissingColumns=True)
    .withColumn("quarantine_ts", current_timestamp())
)

# ----------------------------------------------------------------------
# Write Silver and Quarantine
# ----------------------------------------------------------------------
df_silver.write.mode("overwrite").parquet(silver_path)
df_quarantine.write.mode("overwrite").parquet(quarantine_path)

print("Bronze count:", df_bronze.count())
print("Silver count:", df_silver.count())
print("Quarantine count:", df_quarantine.count())
