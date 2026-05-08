from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType

# ==============================
# PATHS
# ==============================
PROCESSED_PATH = "/mnt/d/real_time_data_pipeline/data/processed/"
AGG_PATH = "/mnt/d/real_time_data_pipeline/data/aggregated/"
CHECKPOINT = "/mnt/d/real_time_data_pipeline/data/checkpoints/"

# ==============================
# SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingPipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================
# SCHEMA
# ==============================
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("event", StringType()) \
    .add("amount", IntegerType()) \
    .add("timestamp", StringType())

# ==============================
# READ FROM KAFKA
# ==============================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (bytes → JSON string)
df_json = df_kafka.selectExpr("CAST(value AS STRING)")

# Parse JSON
df_parsed = df_json.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ==============================
# CLEANING
# ==============================
df_clean = df_parsed.filter(col("user_id").isNotNull())

df_clean = df_clean.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

# ==============================
# AGGREGATION
# ==============================
df_agg = df_clean \
    .withWatermark("timestamp", "1 minute") \
    .groupBy("event") \
    .sum("amount")

# ==============================
# WRITE CLEAN DATA
# ==============================
clean_query = df_clean.writeStream \
    .format("json") \
    .option("path", PROCESSED_PATH) \
    .option("checkpointLocation", CHECKPOINT + "clean/") \
    .outputMode("append") \
    .start()

# ==============================
# WRITE AGGREGATION
# ==============================
agg_query = df_agg.writeStream \
    .format("console") \
    .option("checkpointLocation", CHECKPOINT + "agg/") \
    .outputMode("complete") \
    .start()

# ==============================
# RUN STREAM
# ==============================
spark.streams.awaitAnyTermination()