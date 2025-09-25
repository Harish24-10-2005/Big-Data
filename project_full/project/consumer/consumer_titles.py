from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

spark = SparkSession.builder \
    .appName("OTT Titles Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


titles_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("show_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("age_certification", StringType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("genres", StringType(), True),
    StructField("production_countries", StringType(), True),
    StructField("seasons", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("imdb_score", FloatType(), True),
    StructField("imdb_votes", IntegerType(), True),
    StructField("tmdb_popularity", FloatType(), True),
    StructField("tmdb_score", FloatType(), True),
    StructField("platform", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])


titles_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ott_titles") \
    .option("startingOffsets", "earliest") \
    .load()


titles_df = titles_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", titles_schema).alias("data")) \
    .select("data.*")

query = titles_df.writeStream \
    .format("parquet") \
    .option("path", "/user/hadoop/ott_titles") \
    .option("checkpointLocation", "/user/hadoop/checkpoints/titles") \
    .trigger(once=True) \
    .outputMode("append") \
    .start()

query.awaitTermination()
print("✅ Processed all available titles data")

