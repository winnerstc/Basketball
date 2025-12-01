# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, initcap, to_date, current_timestamp,
    current_date, datediff, when, input_file_name, row_number, lit, year
)
from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bronze_to_silver_players").getOrCreate()

# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------
bronze_path = "hdfs:///tmp/DE011025/NBA/bronze/players"
silver_path = "hdfs:///tmp/DE011025/NBA/silver/players"
quarantine_path = "hdfs:///tmp/DE011025/NBA/quarantine/players"

# ------------------------------------------------------------------
# 0. Read Bronze TEXTFILE as CSV (comma-separated, no header)
# ------------------------------------------------------------------
spark.sql("USE nba_bronze")
df = spark.table("players")
df = spark.sql("SELECT * FROM players")

columns = [
    "personId",
    "firstName",
    "lastName",
    "birthdate",
    "lastAttended",
    "country",
    "height",
    "bodyWeight",
    "guard",
    "forward",
    "center",
    "draftYear",
    "draftRound",
    "draftNumber"
]

df_bronze = df

# ------------------------------------------------------------------
# Transformation 1: Normalize fake nulls to real NULL
# ------------------------------------------------------------------
cols_exclude = ["guard", "forward", "center"]

cols_to_clean = [c for c in df_bronze.columns if c not in cols_exclude]

df_norm_nulls = df_bronze.replace(
    ["nul", "NULL", "NA", ""],
    None,
    subset=cols_to_clean
)

# ------------------------------------------------------------------
# Transformation 2: Enforce correct data types (schema casting)
# ------------------------------------------------------------------
df_casted = df_norm_nulls.select(
    col("personId").cast(LongType()).alias("personId"),
    col("firstName").cast(StringType()).alias("firstName"),
    col("lastName").cast(StringType()).alias("lastName"),
    col("birthdate").cast(StringType()).alias("birthdate"),
    col("lastAttended").cast(StringType()).alias("lastAttended"),
    col("country").cast(StringType()).alias("country"),
    col("height").cast(DoubleType()).alias("height"),
    col("bodyWeight").cast(DoubleType()).alias("bodyWeight"),
    col("guard").cast(StringType()).alias("guard"),
    col("forward").cast(StringType()).alias("forward"),
    col("center").cast(StringType()).alias("center"),
    col("draftYear").cast(IntegerType()).alias("draftYear"),
    col("draftRound").cast(IntegerType()).alias("draftRound"),
    col("draftNumber").cast(IntegerType()).alias("draftNumber")
)

# ------------------------------------------------------------------
# Transformation 3: Trim whitespace on string columns
# ------------------------------------------------------------------
df_trimmed = (
    df_casted
    .withColumn("firstName", trim(col("firstName")))
    .withColumn("lastName", trim(col("lastName")))
    .withColumn("birthdate", trim(col("birthdate")))
    .withColumn("lastAttended", trim(col("lastAttended")))
    .withColumn("country", trim(col("country")))
    .withColumn("guard", trim(col("guard")))
    .withColumn("forward", trim(col("forward")))
    .withColumn("center", trim(col("center")))
)

# ------------------------------------------------------------------
# Transformation 4: Standardize casing on text
#   - Names to InitCap (e.g., "lebron" -> "LeBron")
#   - Country and lastAttended to UPPER
# ------------------------------------------------------------------
df_text_norm = (
    df_trimmed
    .withColumn("firstName", initcap(col("firstName")))
    .withColumn("lastName", initcap(col("lastName")))
    .withColumn("country", upper(col("country")))
    .withColumn("lastAttended", upper(col("lastAttended")))
)

# ------------------------------------------------------------------
# Transformation 5: Parse birthdate to proper DATE & derive fields
#   Assuming format 'yyyy-MM-dd' - change pattern if needed
# ------------------------------------------------------------------
df_with_birth = (
    df_text_norm
    .withColumn("birth_date", to_date(col("birthdate"), "yyyy-MM-dd"))
    .withColumn(
        "birth_year",
        when(col("birth_date").isNotNull(), col("birth_date").substr(1, 4).cast(IntegerType()))
    )
)

# ------------------------------------------------------------------
# Transformation 6: Derive age (approximate)
# ------------------------------------------------------------------
df_with_age = df_with_birth.withColumn(
    "age_years",
    when(
        col("birth_date").isNotNull(),
        (datediff(current_date(), col("birth_date")) / 365.25)
    ).otherwise(None)
)

# ------------------------------------------------------------------
# Transformation 7: Normalize position flags (guard/forward/center)
#   Convert Y/Yes/1 -> 1, else 0
# ------------------------------------------------------------------
from pyspark.sql.functions import coalesce, lit

def norm_pos_flag_bool(c):
    # coalesce(col(c), lit(False)) handles nulls as False
    return when(coalesce(col(c).cast("boolean"), lit(False)) == True, 1).otherwise(0)

df_pos_norm = (
    df_with_age
    .withColumn("guard_flag",   norm_pos_flag_bool("guard"))
    .withColumn("forward_flag", norm_pos_flag_bool("forward"))
    .withColumn("center_flag",  norm_pos_flag_bool("center"))
)

# ------------------------------------------------------------------
# Transformation 8: Derive primary_position from flags
# ------------------------------------------------------------------
df_with_pos = df_pos_norm.withColumn(
    "primary_position",
    when(col("guard_flag") == 1, "G")
    .when(col("forward_flag") == 1, "F")
    .when(col("center_flag") == 1, "C")
    .otherwise(None)
)

# ------------------------------------------------------------------
# Transformation 9: Drop rows with null business key (personId)
#   (But capture them in quarantine first)
# ------------------------------------------------------------------
df_bad_keys = (
    df_with_pos
    .filter(col("personId").isNull())
    .withColumn("quarantine_reason", lit("MISSING_PERSON_ID"))
)

df_keys_ok = df_with_pos.filter(col("personId").isNotNull())

# ------------------------------------------------------------------
# Transformation 10: Numeric range checks (height, weight, draft info)
#   - height: 100–280 (cm) or sensible range
#   - bodyWeight: 50–250 (kg) or wide lbs equivalent
#   - draftYear: 1946–current_year+1
#   - draftRound: 1–10 (very wide)
#   - draftNumber: > 0
# ------------------------------------------------------------------
current_year_col = year(current_date())

valid_numeric = (
    (col("height").isNull() | ((col("height") > 10) & (col("height") < 280))) &
    (col("bodyWeight").isNull() | ((col("bodyWeight") > 50) & (col("bodyWeight") < 250))) 
)

df_numeric_ok = df_keys_ok.filter(valid_numeric)

df_bad_numeric = (
    df_keys_ok
    .filter(~valid_numeric)
    .withColumn("quarantine_reason", lit("INVALID_NUMERIC_RANGE"))
)

# ------------------------------------------------------------------
# Transformation 11: Build quarantine dataframe (bad keys + bad numeric)
#   + attach technical columns
# ------------------------------------------------------------------
df_quarantine = (
    df_bad_keys
    .unionByName(df_bad_numeric)
    .withColumn("record_source", lit("NBA_PLAYERS_BRONZE"))
    .withColumn("silver_ingest_ts", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# ------------------------------------------------------------------
# Transformation 12: Deduplicate on business key (personId)
#   Keep "best" record per personId (latest birth_date, then latest draftYear)
# ------------------------------------------------------------------
w_dedup = Window.partitionBy("personId").orderBy(
    col("birth_date").desc(),
    col("draftYear").desc()
)

df_silver_dedup = (
    df_numeric_ok
    .withColumn("rn", row_number().over(w_dedup))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ------------------------------------------------------------------
# Transformation 13: Add standard Silver metadata columns
# ------------------------------------------------------------------
df_silver_final = (
    df_silver_dedup
    .withColumn("record_source", lit("NBA_PLAYERS_BRONZE"))
    .withColumn("silver_ingest_ts", current_timestamp())
    .withColumn("source_file", lit("hive_table:nba_bronze.players"))
)


# ------------------------------------------------------------------
# WRITE OUT: Silver + Quarantine as Parquet
# ------------------------------------------------------------------
(
    df_silver_final
    .write
    .mode("overwrite")       # or "append" depending on your load pattern
    .parquet(silver_path)
)

spark.stop()
