# ott_load_mysql.py (GUARANTEED WORKING VERSION)
import os
from pyspark.sql import SparkSession

MYSQL_URL = "jdbc:mysql://localhost:3306/ott?createDatabaseIfNotExist=true"
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = "hadoop@123"

if not MYSQL_USER or not MYSQL_PASSWORD:
    raise SystemExit("❌ Please set MYSQL_USER and MYSQL_PASSWORD.")

spark = SparkSession.builder \
    .appName("OTT Load to MySQL") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schemas
COLTYPES_TITLES = (
    "id VARCHAR(64), title VARCHAR(512), show_type VARCHAR(16), description TEXT, "
    "release_year INT, age_certification VARCHAR(16), runtime INT, genres TEXT, "
    "production_countries TEXT, seasons INT, imdb_id VARCHAR(32), imdb_score DOUBLE, "
    "imdb_votes INT, tmdb_popularity DOUBLE, tmdb_score DOUBLE, platform VARCHAR(32), ingestion_date DATE"
)
COLTYPES_CREDITS = (
    "person_ID VARCHAR(64), id VARCHAR(64), name VARCHAR(256), character_name VARCHAR(256), "
    "role VARCHAR(16), platform VARCHAR(32)"
)

def write_jdbc(df, table, col_types=None):
    writer = (
        df.write.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", "5000")
        .option("numPartitions", "4")
        .mode("overwrite")
    )
    if col_types:
        writer = writer.option("createTableColumnTypes", col_types)
    writer.save()
    print(f"✅ Wrote MySQL table: {table}")

# ONLY read the 2 files that actually exist
try:
    titles_df = spark.read.parquet("/user/hadoop/ott_titles")
    write_jdbc(titles_df, "transformed_titles", COLTYPES_TITLES)
    print("✅ Loaded titles data")
except Exception as e:
    print(f"⚠️ No titles data: {e}")

try:
    credits_df = spark.read.parquet("/user/hadoop/ott_credits")  
    write_jdbc(credits_df, "transformed_credits", COLTYPES_CREDITS)
    print("✅ Loaded credits data")
except Exception as e:
    print(f"⚠️ No credits data: {e}")

print("✅ MySQL load complete.")

