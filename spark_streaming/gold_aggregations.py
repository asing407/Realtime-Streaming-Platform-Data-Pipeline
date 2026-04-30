from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import *

builder = SparkSession.builder \
    .appName("Gold Layer - Business Metrics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master(SPARK_MASTER)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"""
╔══════════════════════════════════════════════════════════╗
║                GOLD LAYER - BUSINESS METRICS              ║
╠══════════════════════════════════════════════════════════╣
║  Reading from:  {SILVER_PATH}/events                     ║
║  Writing to:    {GOLD_PATH}/                             ║
╚══════════════════════════════════════════════════════════╝
""")

# Read from Silver
silver_events = spark \
    .readStream \
    .format("delta") \
    .load(f"{SILVER_PATH}/events")

# === 1. Real-Time Funnel Metrics ===
funnel_metrics = silver_events \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window("event_timestamp", "5 minutes", "1 minute"),
        "event_type"
    ) \
    .agg(
        count("event_id").alias("event_count"),
        approx_count_distinct("user_id").alias("unique_users"),
        approx_count_distinct("session_id").alias("unique_sessions"),
        avg("total_amount").alias("avg_order_value"),
        sum(when(col("is_first_purchase") == True, 1).otherwise(0)).alias("first_time_buyers")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type",
        "event_count",
        "unique_users",
        "unique_sessions",
        "avg_order_value",
        "first_time_buyers",
        current_timestamp().alias("calculated_at")
    )

query1 = funnel_metrics \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold_funnel") \
    .trigger(processingTime='1 minute') \
    .start(f"{GOLD_PATH}/funnel_metrics")

# === 2. Trending Products ===
trending_products = silver_events \
    .filter(col("event_type").isin(["product_view", "add_to_cart", "purchase"])) \
    .withWatermark("event_timestamp", "5 minutes") \
    .groupBy(
        window("event_timestamp", "10 minutes"),
        "product_id",
        "product_name",
        "category"
    ) \
    .agg(
        count("event_id").alias("activity_count"),
        approx_count_distinct("user_id").alias("unique_viewers"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "product_id",
        "product_name",
        "category",
        "activity_count",
        "unique_viewers",
        "purchases",
        "cart_adds",
        current_timestamp().alias("calculated_at")
    )

query2 = trending_products \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold_trending") \
    .trigger(processingTime='2 minutes') \
    .start(f"{GOLD_PATH}/trending_products")

# === 3. Revenue Metrics ===
revenue_metrics = silver_events \
    .filter(col("event_type") == "purchase") \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window("event_timestamp", "5 minutes", "1 minute"),
        "category",
        "payment_method"
    ) \
    .agg(
        sum("total_amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("total_amount").alias("avg_order_value"),
        sum(when(col("is_high_value") == True, 1).otherwise(0)).alias("high_value_orders")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "category",
        "payment_method",
        "total_revenue",
        "order_count",
        "avg_order_value",
        "high_value_orders",
        current_timestamp().alias("calculated_at")
    )

query3 = revenue_metrics \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold_revenue") \
    .trigger(processingTime='1 minute') \
    .start(f"{GOLD_PATH}/revenue_metrics")

print("✅ Gold layer metrics ACTIVE - Press Ctrl+C to stop")
print("\n📊 Generating:")
print("  1. Funnel Metrics (5-min windows, 1-min updates)")
print("  2. Trending Products (10-min windows)")
print("  3. Revenue Metrics (5-min windows)")

# Wait for all queries
spark.streams.awaitAnyTermination()
