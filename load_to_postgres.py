from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.config import *

print("""
╔══════════════════════════════════════════════════════════╗
║        LOADING DELTA LAKE → POSTGRESQL                   ║
╚══════════════════════════════════════════════════════════╝
""")

builder = SparkSession.builder \
    .appName("Load to PostgreSQL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.jars", "jars/postgresql.jar") \
    .master("local[*]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://localhost:5432/ecommerce_analytics"
connection_properties = {
    "user": os.getenv("USER"),
    "driver": "org.postgresql.Driver"
}

print("\n1️⃣  Loading Funnel Metrics...")
try:
    funnel_df = spark.read.format("delta").load(f"{GOLD_PATH}/funnel_metrics")
    funnel_clean = funnel_df.select(
        "window_start", "window_end", "event_type", "event_count",
        "unique_users", "unique_sessions", "avg_order_value",
        "first_time_buyers", "calculated_at"
    )
    funnel_clean.write \
        .mode("append") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "funnel_metrics", properties=connection_properties)
    print(f"   ✅ Loaded {funnel_clean.count()} funnel metric records")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n2️⃣  Loading Trending Products...")
try:
    trending_df = spark.read.format("delta").load(f"{GOLD_PATH}/trending_products")
    trending_clean = trending_df.select(
        "window_start", "window_end", "product_id", "product_name",
        "category", "activity_count", "unique_viewers",
        "purchases", "cart_adds", "calculated_at"
    )
    trending_clean.write \
        .mode("append") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "trending_products", properties=connection_properties)
    print(f"   ✅ Loaded {trending_clean.count()} trending product records")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n3️⃣  Loading Revenue Metrics...")
try:
    revenue_df = spark.read.format("delta").load(f"{GOLD_PATH}/revenue_metrics")
    revenue_clean = revenue_df.select(
        "window_start", "window_end", "category", "payment_method",
        "total_revenue", "order_count", "avg_order_value",
        "high_value_orders", "calculated_at"
    )
    revenue_clean.write \
        .mode("append") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "revenue_metrics", properties=connection_properties)
    print(f"   ✅ Loaded {revenue_clean.count()} revenue records")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n4️⃣  Generating Hourly Summary...")
try:
    from pyspark.sql.functions import sum as spark_sum, count as spark_count
    
    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/events")
    hourly = silver_df \
        .withColumn("hour_start", date_trunc("hour", "event_timestamp")) \
        .groupBy("hour_start") \
        .agg(
            spark_count("*").alias("total_events"),
            spark_sum(when(col("total_amount").isNotNull(), col("total_amount")).otherwise(0)).alias("total_revenue"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_orders"),
            approx_count_distinct("user_id").alias("unique_users")
        ) \
        .withColumn("conversion_rate", 
                   (col("total_orders") / col("total_events") * 100)) \
        .withColumn("avg_order_value",
                   when(col("total_orders") > 0, col("total_revenue") / col("total_orders")).otherwise(0))
    
    hourly.write \
        .mode("append") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "hourly_summary", properties=connection_properties)
    
    print(f"   ✅ Generated {hourly.count()} hourly summary records")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "="*60)
print("✅ PostgreSQL load complete!")
print("="*60)

spark.stop()
