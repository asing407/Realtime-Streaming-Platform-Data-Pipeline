#!/bin/bash
# start_kafka.sh - Correct paths for Homebrew Kafka

echo "🚀 Starting Kafka Infrastructure..."

# Kill existing
pkill -f zookeeper 2>/dev/null
pkill -f kafka 2>/dev/null
sleep 3

# Correct paths
KAFKA_BIN="/usr/local/opt/kafka/bin"
KAFKA_CONFIG="/usr/local/etc/kafka"

# Start Zookeeper
echo "Starting Zookeeper..."
$KAFKA_BIN/zookeeper-server-start $KAFKA_CONFIG/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
ZOOKEEPER_PID=$!
echo "✅ Zookeeper started (PID: $ZOOKEEPER_PID)"
echo "   Waiting 15 seconds..."
sleep 15

# Start Kafka
echo ""
echo "Starting Kafka Broker..."
$KAFKA_BIN/kafka-server-start $KAFKA_CONFIG/server.properties > /tmp/kafka.log 2>&1 &
KAFKA_PID=$!
echo "✅ Kafka Broker started (PID: $KAFKA_PID)"
echo "   Waiting 30 seconds..."
sleep 30

# Test connection
echo ""
echo "Testing connection..."
if $KAFKA_BIN/kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "✅ Kafka broker is ready!"
else
    echo "❌ Kafka not responding. Checking logs..."
    tail -20 /tmp/kafka.log
    exit 1
fi

# Create topics
echo ""
echo "📋 Creating topics..."

$KAFKA_BIN/kafka-topics --create --topic ecommerce-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

$KAFKA_BIN/kafka-topics --create --topic inventory-updates \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

$KAFKA_BIN/kafka-topics --create --topic user-sessions \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

# List topics
echo ""
echo "📊 Available Topics:"
$KAFKA_BIN/kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "✅ Kafka infrastructure ready!"
echo "   Zookeeper PID: $ZOOKEEPER_PID"
echo "   Kafka PID: $KAFKA_PID"
echo ""
echo "Logs:"
echo "  Zookeeper: /tmp/zookeeper.log"
echo "  Kafka: /tmp/kafka.log"
echo ""
echo "To stop: pkill -f zookeeper && pkill -f kafka"