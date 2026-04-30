from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import *

# Initialize Spark with Delta Lake
builder = SparkSession.builder \
    .appName("Bronze Layer - Raw Event Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.jars", 
            "jars/spark-sql-kafka.jar,"
            "jars/kafka-clients.jar,"
            "jars/spark-token-provider.jar,"
            "jars/commons-pool2.jar") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .master(SPARK_MASTER)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"""
╔══════════════════════════════════════════════════════════╗
║                  BRONZE LAYER INGESTION                   ║
╠══════════════════════════════════════════════════════════╣
║  Spark Version: {spark.version}                          ║
║  Reading from:  Kafka ({KAFKA_BOOTSTRAP_SERVERS})        ║
║  Writing to:    {BRONZE_PATH}/events                     ║
╚══════════════════════════════════════════════════════════╝
""")

# Read from Kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPICS['events']) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON with flexible schema (Bronze = raw)
parsed_events = kafka_stream \
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp"),
        # Parse JSON into struct
        from_json(
            col("value").cast("string"),
            "event_type STRING, event_id STRING, timestamp STRING, " +
            "user_id STRING, session_id STRING, product_id STRING, " +
            "product_name STRING, category STRING, price DOUBLE, " +
            "quantity INT, order_id STRING, items STRING, " +
            "total_amount DOUBLE, payment_method STRING, " +
            "device STRING, referrer STRING, page_type STRING, " +
            "time_on_page_seconds INT, cart_value DOUBLE, " +
            "num_items INT, subtotal DOUBLE, discount DOUBLE, " +
            "shipping DOUBLE, shipping_country STRING, is_first_purchase BOOLEAN"
        ).alias("data")
    ) \
    .select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingestion_timestamp",
        "raw_json",
        "data.*"
    )

# Write to Bronze Delta Lake (append-only, no transformations)
query = parsed_events \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze_events") \
    .option("mergeSchema", "true") \
    .trigger(processingTime='10 seconds') \
    .start(f"{BRONZE_PATH}/events")

print("✅ Bronze layer streaming ACTIVE - Press Ctrl+C to stop")
query.awaitTermination()
