from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import *

builder = SparkSession.builder \
    .appName("Silver Layer - Cleaned & Enriched") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master(SPARK_MASTER)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"""
╔══════════════════════════════════════════════════════════╗
║                  SILVER LAYER PROCESSING                  ║
╠══════════════════════════════════════════════════════════╣
║  Reading from:  {BRONZE_PATH}/events                     ║
║  Writing to:    {SILVER_PATH}/events                     ║
╚══════════════════════════════════════════════════════════╝
""")

# Read from Bronze Delta Lake
bronze_events = spark \
    .readStream \
    .format("delta") \
    .load(f"{BRONZE_PATH}/events")

# Silver transformations
silver_events = bronze_events \
    .filter(col("event_id").isNotNull()) \
    .filter(col("timestamp").isNotNull()) \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("event_date", to_date(col("event_timestamp"))) \
    .withColumn("event_hour", hour(col("event_timestamp"))) \
    .withColumn("event_day_of_week", dayofweek(col("event_timestamp"))) \
    .withColumn("is_weekend", dayofweek(col("event_timestamp")).isin([1, 7])) \
    .withColumn("is_high_value", when(col("total_amount") > 500, True).otherwise(False)) \
    .withColumn("is_mobile", col("device") == "mobile") \
    .withWatermark("event_timestamp", "10 minutes") \
    .dropDuplicates(["event_id"]) \
    .select(
        "event_id",
        "event_type",
        "event_timestamp",
        "event_date",
        "event_hour",
        "event_day_of_week",
        "is_weekend",
        "is_mobile",
        "user_id",
        "session_id",
        "product_id",
        "product_name",
        "category",
        "price",
        "quantity",
        "order_id",
        "total_amount",
        "is_high_value",
        "payment_method",
        "device",
        "referrer",
        "page_type",
        "cart_value",
        "shipping_country",
        "is_first_purchase",
        "ingestion_timestamp"
    )

# Write to Silver with partitioning
query = silver_events \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/silver_events") \
    .partitionBy("event_date", "event_type") \
    .trigger(processingTime='15 seconds') \
    .start(f"{SILVER_PATH}/events")

print("✅ Silver layer streaming ACTIVE - Press Ctrl+C to stop")
query.awaitTermination()