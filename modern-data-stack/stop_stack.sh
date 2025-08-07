#!/bin/bash

echo "Stopping Modern Data Stack..."

# Stop Python processes
pkill -f "kafka_to_postgres.py"
pkill -f "stream_to_ducklake.py"
pkill -f "crypto_producer.py"
pkill -f "github_producer.py"
pkill -f "weather_producer.py"
pkill -f "dagster"

# Stop Docker services
docker-compose down

echo "All services stopped."
