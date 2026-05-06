from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType

spark = SparkSession.builder.appName("RealTimePipeline").getOrCreate()
RAW_PATH = "/mnt/d/real_time_data_pipeline/data/raw/"
PROCESSED_PATH = "/mnt/d/real_time_data_pipeline/data/processed/"
CHECKPOINT = "/mnt/d/real_time_data_pipeline/data/checkpoints/"
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("event", StringType()) \
    .add("amount", IntegerType()) \
    .add("timestamp", StringType())

df = spark.readStream.schema(schema).json(RAW_PATH)

df_clean = df.filter(col("user_id").isNotNull())

df_clean = df_clean.withColumn("timestamp", to_timestamp("timestamp"))

df_agg = df_clean \
    .withWatermark("timestamp", "1 minute") \
    .groupBy("event") \
    .sum("amount")

clean_query = df_clean.writeStream \
    .format("json") \
    .option("path", PROCESSED_PATH) \
    .option("checkpointLocation", CHECKPOINT + "clean/") \
    .outputMode("append") \
    .start()

agg_query = df_agg.writeStream \
    .format("console") \
    .option("checkpointLocation", CHECKPOINT + "agg/") \
    .outputMode("complete") \
    .start()

spark.streams.awaitAnyTermination()