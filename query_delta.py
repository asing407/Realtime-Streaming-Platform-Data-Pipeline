from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.config import *

builder = SparkSession.builder \
    .appName("Query Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .master("local[*]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("""
╔══════════════════════════════════════════════════════════╗
║          DELTA LAKE DATA EXPLORER                         ║
╚══════════════════════════════════════════════════════════╝
""")

# Bronze stats
print("\n📦 BRONZE LAYER (Raw Events):")
print("="*60)
bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/events")
total = bronze.count()
print(f"Total events: {total:,}")
print("\nEvent distribution:")
bronze.groupBy("event_type").count().orderBy("count", ascending=False).show()

# Silver stats
print("\n🔧 SILVER LAYER (Cleaned & Enriched):")
print("="*60)
silver = spark.read.format("delta").load(f"{SILVER_PATH}/events")
silver_total = silver.count()
print(f"Total events (deduplicated): {silver_total:,}")
print(f"Deduplication rate: {((total - silver_total) / total * 100):.2f}%")
print("\nEvents by type and date:")
silver.groupBy("event_type", "event_date").count().orderBy("event_date", "event_type").show(20)

print("\nDevice breakdown:")
silver.groupBy("device").count().orderBy("count", ascending=False).show()

print("\nTop categories:")
silver.filter("category IS NOT NULL").groupBy("category").count().orderBy("count", ascending=False).show()

# Gold - Funnel
print("\n🏆 GOLD LAYER - Funnel Metrics (Real-time Conversion):")
print("="*60)
try:
    funnel = spark.read.format("delta").load(f"{GOLD_PATH}/funnel_metrics")
    print(f"Total metric rows: {funnel.count():,}")
    print("\nLatest funnel metrics:")
    funnel.orderBy("window_start", ascending=False).show(10, truncate=False)
except:
    print("⏳ No funnel data yet (Gold still aggregating...)")

# Gold - Trending Products
print("\n🔥 GOLD LAYER - Trending Products:")
print("="*60)
try:
    trending = spark.read.format("delta").load(f"{GOLD_PATH}/trending_products")
    print(f"Total product metrics: {trending.count():,}")
    print("\nTop trending products (all time):")
    trending.groupBy("product_name", "category") \
        .agg({"activity_count": "sum", "purchases": "sum"}) \
        .orderBy("sum(activity_count)", ascending=False) \
        .show(10, truncate=False)
except:
    print("⏳ No trending data yet (Gold still aggregating...)")

# Gold - Revenue
print("\n💰 GOLD LAYER - Revenue Metrics:")
print("="*60)
try:
    revenue = spark.read.format("delta").load(f"{GOLD_PATH}/revenue_metrics")
    print(f"Total revenue records: {revenue.count():,}")
    print("\nRevenue by category:")
    revenue.groupBy("category") \
        .agg({"total_revenue": "sum", "order_count": "sum"}) \
        .orderBy("sum(total_revenue)", ascending=False) \
        .show()
    
    print("\nRevenue by payment method:")
    revenue.groupBy("payment_method") \
        .agg({"total_revenue": "sum", "order_count": "sum"}) \
        .orderBy("sum(total_revenue)", ascending=False) \
        .show()
except:
    print("⏳ No revenue data yet (Gold still aggregating...)")

spark.stop()
