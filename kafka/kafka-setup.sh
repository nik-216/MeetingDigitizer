#!/bin/bash
set -e

echo "Waiting for Kafka to start on port 9092..."
while ! nc -z kafka 9092; do
  sleep 1
done
echo "✅ Kafka is up!"

echo "🔍 Checking for existing topics..."
EXISTING_TOPICS=$(/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 || true)

if [ -n "$EXISTING_TOPICS" ]; then
  echo "🧹 Deleting existing topics:"
  echo "$EXISTING_TOPICS"
  for topic in $EXISTING_TOPICS; do
    /opt/kafka/bin/kafka-topics.sh --delete --topic "$topic" --bootstrap-server kafka:9092 || true
  done
  echo "✅ All topics deleted."
else
  echo "ℹ️ No topics to delete."
fi

echo "🎉 Kafka setup complete."
