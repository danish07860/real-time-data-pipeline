from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Paths (WSL)
RAW_PATH = "/mnt/d/real_time_data_pipeline/data/raw/events.json"
PROCESSED_PATH = "/mnt/d/real_time_data_pipeline/data/processed/"
AGG_PATH = "/mnt/d/real_time_data_pipeline/data/aggregated/"

# Create Spark session
spark = SparkSession.builder \
    .appName("RealTimePipeline") \
    .getOrCreate()

print("Reading raw data...")

# Read raw JSON
df = spark.read.json(RAW_PATH)

print("Raw data sample:")
df.show(5)

# Cleaning
df_clean = df.filter(col("user_id").isNotNull())

# Transformation
df_agg = df_clean.groupBy("event").sum("amount")

print("Aggregated result:")
df_agg.show()

# Write outputs
df_clean.write.mode("overwrite").json(PROCESSED_PATH)
df_agg.write.mode("overwrite").json(AGG_PATH)

print("Data written successfully")

spark.stop()