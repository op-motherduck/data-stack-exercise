#!/bin/bash
# =============================================================================
# MODERN DATA STACK SHUTDOWN SCRIPT
# =============================================================================
# Purpose: Gracefully shuts down all components of the modern data stack including:
#   - Data producers (crypto, GitHub, weather)
#   - Data consumers and streaming processes
#   - Dagster orchestration platform
#   - Docker services (Kafka, PostgreSQL, Zookeeper, Kafka UI)
#   - Force cleanup of any remaining processes
# 
# This script ensures clean shutdown of all stack components and prevents
# resource conflicts when restarting the stack.
# =============================================================================

echo "Stopping Modern Data Stack..."

# Stop all Python processes related to our data stack
echo "Stopping data producers and consumers..."
pkill -f "crypto_producer.py"
pkill -f "github_producer.py"
pkill -f "weather_producer.py"
pkill -f "kafka_to_postgres.py"
pkill -f "stream_to_ducklake.py"
pkill -f "postgres_to_ducklake.py"

# Stop Dagster
echo "Stopping Dagster..."
pkill -f "dagster"

# Stop Docker services
echo "Stopping Docker services..."
docker-compose down

# Wait a moment for processes to stop
sleep 5

# Force kill any remaining processes
echo "Force stopping any remaining processes..."
kill -9 $(ps aux | grep -E "(crypto_producer|github_producer|weather_producer|kafka_to_postgres|stream_to_ducklake|dagster)" | grep -v grep | awk '{print $2}' 2>/dev/null) 2>/dev/null || true

echo ""
echo "========================================="
echo "Modern Data Stack stopped!"
echo "========================================="
echo ""
echo "All services have been stopped:"
echo "- Docker containers (Kafka, PostgreSQL, DuckLake)"
echo "- Data producers (crypto, GitHub, weather)"
echo "- Data consumers (Kafka to PostgreSQL)"
echo "- Streaming processes (PostgreSQL to DuckLake)"
echo "- Dagster orchestration"
echo ""
echo "To restart, run: ./start_stack.sh"
