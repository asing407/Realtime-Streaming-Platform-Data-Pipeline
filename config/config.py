import os
from pathlib import Path
from datetime import datetime

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_ROOT = PROJECT_ROOT / "delta_lake"
LOGS_ROOT = PROJECT_ROOT / "logs"

# Delta Lake paths
BRONZE_PATH = str(DATA_ROOT / "bronze")
SILVER_PATH = str(DATA_ROOT / "silver")
GOLD_PATH = str(DATA_ROOT / "gold")
CHECKPOINT_PATH = str(DATA_ROOT / "checkpoints")

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = {
    'events': 'ecommerce-events',
    'inventory': 'inventory-updates',
    'sessions': 'user-sessions'
}

# Spark config
SPARK_APP_NAME = "EcommerceStreamingPipeline"
SPARK_MASTER = "local[*]"

# PostgreSQL config
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_analytics',
    'user': 'postgres',
    'password': 'postgres'  # Change this!
}

# Create directories
for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, CHECKPOINT_PATH, LOGS_ROOT]:
    os.makedirs(path, exist_ok=True)

# Logging config
LOG_FILE = LOGS_ROOT / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"