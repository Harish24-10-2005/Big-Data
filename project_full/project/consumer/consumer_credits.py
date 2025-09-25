from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder \
    .appName("OTT Credits Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


credits_schema = StructType([
    StructField("person_ID", StringType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("character_name", StringType(), True),
    StructField("role", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])


credits_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ott_credits") \
    .option("startingOffsets", "earliest") \
    .load()


credits_df = credits_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", credits_schema).alias("data")) \
    .select("data.*")


query = credits_df.writeStream \
    .format("parquet") \
    .option("path", "/user/hadoop/ott_credits") \
    .option("checkpointLocation", "/user/hadoop/checkpoints/credits") \
    .trigger(once=True) \
    .outputMode("append") \
    .start()

query.awaitTermination()
print("✅ Processed all available credits data")

