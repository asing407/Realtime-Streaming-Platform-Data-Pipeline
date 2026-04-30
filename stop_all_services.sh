#!/bin/bash

echo "🛑 Stopping all services..."

# Stop Spark jobs
pkill -f bronze_ingestion.py
pkill -f silver_transformations.py
pkill -f gold_aggregations.py

# Stop event generator
pkill -f event_producer.py

# Stop Kafka
./stop_kafka.sh

echo "✅ All services stopped"
