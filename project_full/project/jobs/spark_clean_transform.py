# ott_transform.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, split, explode, when,
    to_date, desc, current_date
)
from pyspark.sql.types import IntegerType, DoubleType

# HDFS Paths
BRONZE_TITLES = "/user/hadoop/ott_titles"
BRONZE_CREDITS = "/user/hadoop/ott_credits"
CURATED_BASE = "/user/hadoop/curated"

spark = SparkSession.builder \
    .appName("OTT Clean & Transform") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Ensure Hive DB
spark.sql("CREATE DATABASE IF NOT EXISTS ott")

# Helpers for HDFS checks
jvm = spark._jvm
hconf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
Path = jvm.org.apache.hadoop.fs.Path

def hdfs_exists(path: str) -> bool:
    return fs.exists(Path(path))

def hdfs_mkdirs(path: str):
    fs.mkdirs(Path(path))

# Validate inputs
for p in [BRONZE_TITLES, BRONZE_CREDITS]:
    if not hdfs_exists(p):
        hdfs_mkdirs(p)
        print(f"Missing input directory created: {p}")
        sys.exit(1)

# Load bronze
titles = spark.read.parquet(BRONZE_TITLES)
credits = spark.read.parquet(BRONZE_CREDITS)

# === Clean Titles ===
titles_clean = (titles
    .withColumn("id", trim(col("id")))
    .withColumn("title", trim(col("title")))
    .withColumn("show_type", upper(trim(col("show_type"))))
    .withColumn("description", trim(col("description")))
    .withColumn("age_certification", upper(trim(col("age_certification"))))
    .withColumn("genres", regexp_replace(trim(col("genres")), r'[\[\]\"]', ''))
    .withColumn("production_countries", regexp_replace(trim(col("production_countries")), r'[\[\]\"]', ''))
    .withColumn("seasons", col("seasons").cast(IntegerType()))
    .withColumn("release_year", col("release_year").cast(IntegerType()))
    .withColumn("runtime", col("runtime").cast(IntegerType()))
    .withColumn("imdb_score", col("imdb_score").cast(DoubleType()))
    .withColumn("imdb_votes", col("imdb_votes").cast(IntegerType()))
    .withColumn("tmdb_popularity", col("tmdb_popularity").cast(DoubleType()))
    .withColumn("tmdb_score", col("tmdb_score").cast(DoubleType()))
    .withColumn("platform", trim(col("platform")))
    .withColumn("ingestion_date", to_date(col("ingestion_timestamp")))
    .dropna(subset=["id", "title", "platform"])
    .dropDuplicates(["id", "platform"])
)

# === Clean Credits ===
credits_clean = (credits
    .withColumn("person_ID", trim(col("person_ID")))
    .withColumn("id", trim(col("id")))
    .withColumn("name", trim(col("name")))
    .withColumn("character_name", trim(col("character_name")))
    .withColumn("role", upper(trim(col("role"))))
    .withColumn("platform", trim(col("platform")))
    .dropna(subset=["person_ID", "id", "platform"])
    .dropDuplicates(["person_ID", "id", "role", "platform"])
)

# Persist curated tables
titles_clean.write.mode("overwrite").saveAsTable("ott.transformed_titles")
credits_clean.write.mode("overwrite").saveAsTable("ott.transformed_credits")

# Snapshots (append)
for df, name in [(titles_clean, "titles"), (credits_clean, "credits")]:
    df.withColumn("snapshot_date", current_date()) \
      .write.mode("append").partitionBy("snapshot_date") \
      .parquet(f"{CURATED_BASE}/snapshots/{name}")

# Star schema
dim_title = titles_clean.select(
    "id","title","show_type","release_year","age_certification","runtime",
    "imdb_id","imdb_score","imdb_votes","tmdb_popularity","tmdb_score",
    "platform","genres","production_countries"
)
dim_person = credits_clean.select("person_ID","name").dropDuplicates(["person_ID","name"])
fact_credits = credits_clean.select(
    col("person_ID").alias("person_id"), "id", "role", "platform", "character_name"
)

dim_title.write.mode("overwrite").saveAsTable("ott.dim_title")
dim_person.write.mode("overwrite").saveAsTable("ott.dim_person")
fact_credits.write.mode("overwrite").saveAsTable("ott.fact_credits")

# Curated marts
titles_clean.where(col("platform") == "Netflix") \
    .orderBy(desc("tmdb_popularity")).limit(100) \
    .write.mode("overwrite").saveAsTable("ott.curated_netflix_popularity")

titles_clean.where(col("platform") == "Disney") \
    .orderBy(desc("imdb_votes")).limit(100) \
    .write.mode("overwrite").saveAsTable("ott.curated_disney_trending")

# Views
spark.sql("""
CREATE OR REPLACE VIEW ott.v_popularity_by_year AS
SELECT platform, release_year,
       AVG(tmdb_popularity) AS avg_tmdb_popularity,
       AVG(imdb_score) AS avg_imdb_score
FROM ott.dim_title
GROUP BY platform, release_year
""")

print("✅ Transformation complete: curated Hive tables, star schema, marts, snapshots.")

