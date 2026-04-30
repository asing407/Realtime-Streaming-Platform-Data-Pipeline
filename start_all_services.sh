#!/bin/bash

echo "🚀 Starting E-Commerce Analytics Platform..."
echo ""

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Check if Kafka is running
if ! pgrep -f kafka > /dev/null; then
    echo "Starting Kafka & Zookeeper..."
    ./start_kafka.sh
    sleep 10
fi

# Create logs directory
mkdir -p logs

# Start Spark Streaming jobs with venv
echo "Starting Spark Streaming layers..."
source venv/bin/activate && python spark_streaming/bronze_ingestion.py > logs/bronze.log 2>&1 &
echo "  ✅ Bronze layer started (PID: $!)"

source venv/bin/activate && python spark_streaming/silver_transformations.py > logs/silver.log 2>&1 &
echo "  ✅ Silver layer started (PID: $!)"

source venv/bin/activate && python spark_streaming/gold_aggregations.py > logs/gold.log 2>&1 &
echo "  ✅ Gold layer started (PID: $!)"

# Start event generator with venv
echo "Starting event generator..."
source venv/bin/activate && python data_generator/event_producer.py > logs/events.log 2>&1 &
echo "  ✅ Event generator started (PID: $!)"

echo ""
echo "📊 Services running! Check logs in ./logs/"
echo ""
echo "  Airflow UI:   http://localhost:8080 (admin/admin)"
echo "  Dashboard:    http://localhost:8501"
echo ""
echo "Run 'streamlit run streamlit_app/dashboard.py' to start dashboard"
echo "Run './stop_all_services.sh' to stop all services"
