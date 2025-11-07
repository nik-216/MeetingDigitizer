#!/bin/bash

set -e

# Start Kafka in background
/opt/bitnami/kafka/bin/kafka-server-start.sh /opt/bitnami/kafka/config/server.properties &

# Wait for Kafka port 9092 to be ready
echo "Waiting for Kafka to be ready on port 9092..."
while ! nc -z localhost 9092; do
  sleep 1
done
echo "Kafka is up!"

echo "Purging all existing Kafka topics..."
EXISTING_TOPICS=$(/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 || true)

if [ -n "$EXISTING_TOPICS" ]; then
  echo "Deleting topics:"
  echo "$EXISTING_TOPICS"
  for topic in $EXISTING_TOPICS; do
    /opt/bitnami/kafka/bin/kafka-topics.sh --delete --topic "$topic" --bootstrap-server localhost:9092 || true
  done
  echo "All topics deleted."
else
  echo "No topics to delete."
fi