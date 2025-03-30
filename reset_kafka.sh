#!/bin/bash

echo "🛠 Removing old Kafka metadata..."
rm -rf /kafka/meta.properties  # Deletes metadata inside Kafka container

echo "✅ Starting Kafka..."
exec /usr/bin/start-kafka.sh  # Start Kafka normallyg
