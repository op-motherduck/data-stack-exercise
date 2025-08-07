#!/bin/bash

echo "Starting Modern Data Stack with DuckLake..."

# Platform detection
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Detected macOS"
    # Check if Docker Desktop is running
    if ! docker info > /dev/null 2>&1; then
        echo "Docker Desktop is not running. Please start Docker Desktop first."
        exit 1
    fi
fi

# Start Docker services
echo "Starting Kafka, PostgreSQL, and DuckLake metadata store..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Activate virtual environment if it exists
if [ -d "datastack-env" ]; then
    source datastack-env/bin/activate
fi

# Initialize DuckLake
echo "Initializing DuckLake..."
python scripts/setup_ducklake.py

# Start Kafka consumers
echo "Starting Kafka consumers..."
python scripts/kafka_to_postgres.py &
KAFKA_CONSUMER_PID=$!

# Start DuckLake streaming
echo "Starting DuckLake streaming..."
python scripts/stream_to_ducklake.py &
DUCKLAKE_STREAM_PID=$!

# Start Dagster
echo "Starting Dagster..."
cd dagster_project/datastack_orchestration
dagster dev &
DAGSTER_PID=$!
cd ../..

# Start data producers
echo "Starting data producers..."
python scripts/crypto_producer.py &
CRYPTO_PID=$!
python scripts/github_producer.py &
GITHUB_PID=$!
python scripts/weather_producer.py &
WEATHER_PID=$!

# Wait a bit for data to accumulate
sleep 60

# Run initial DuckLake migration
echo "Migrating data to DuckLake..."
python scripts/postgres_to_ducklake.py

# Sync to MotherDuck (optional, requires token)
if [ ! -z "$MOTHERDUCK_TOKEN" ]; then
    echo "Syncing to MotherDuck..."
    python scripts/sync_ducklake_to_motherduck.py
fi

echo ""
echo "========================================="
echo "Modern Data Stack is running!"
echo "========================================="
echo "Access points:"
echo "- Kafka UI: http://localhost:8080"
echo "- Dagster: http://localhost:3000"
echo "- PostgreSQL: localhost:5432"
echo "- DuckLake Metadata: localhost:5433"
echo "- Metabase: http://localhost:3001 (if started)"
echo "- MotherDuck: app.motherduck.com"
echo ""
echo "Data locations:"
echo "- Raw streaming: PostgreSQL"
echo "- Lakehouse: ~/modern-data-stack/data/ducklake/parquet/ (macOS)"
echo "- Cloud analytics: MotherDuck"
echo ""
echo "Process PIDs (for shutdown):"
echo "- Kafka Consumer: $KAFKA_CONSUMER_PID"
echo "- DuckLake Stream: $DUCKLAKE_STREAM_PID"
echo "- Dagster: $DAGSTER_PID"
echo "- Crypto Producer: $CRYPTO_PID"
echo "- GitHub Producer: $GITHUB_PID"
echo "- Weather Producer: $WEATHER_PID"
echo ""
echo "To stop all services, run: ./stop_stack.sh"
