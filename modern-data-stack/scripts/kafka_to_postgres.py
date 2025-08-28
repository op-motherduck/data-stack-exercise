# =============================================================================
# KAFKA TO POSTGRESQL DATA CONSUMER
# =============================================================================
# Purpose: Consumes streaming data from Kafka topics and stores in PostgreSQL:
#   - Consumes crypto_prices, github_events, and weather_data topics
#   - Creates and maintains database tables for raw data storage
#   - Handles multiple data formats and legacy compatibility
#   - Implements error handling and transaction management
#   - Runs multiple consumers in parallel threads for efficiency
# 
# This consumer bridges the streaming layer (Kafka) with the storage layer
# (PostgreSQL) and provides the foundation for data analytics.
# =============================================================================

import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import threading

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="datastack",
    user="dataeng",
    password="dataeng123"
)

# Create tables
with conn.cursor() as cur:
    # Crypto ticker data table (updated for Coincheck structure)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto_prices (
            id SERIAL PRIMARY KEY,
            type VARCHAR(50),
            pair VARCHAR(50),
            last DECIMAL(20, 8),
            bid DECIMAL(20, 8),
            ask DECIMAL(20, 8),
            high DECIMAL(20, 8),
            low DECIMAL(20, 8),
            volume DECIMAL(20, 8),
            timestamp TIMESTAMP,
            source VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # GitHub events table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_github_events (
            id VARCHAR(100) PRIMARY KEY,
            event_type VARCHAR(50),
            actor VARCHAR(100),
            repo VARCHAR(200),
            created_at TIMESTAMP,
            timestamp TIMESTAMP,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Weather data table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            temperature DECIMAL(5, 2),
            humidity INTEGER,
            pressure INTEGER,
            weather VARCHAR(50),
            description VARCHAR(200),
            wind_speed DECIMAL(5, 2),
            timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()

def consume_crypto_prices():
    consumer = KafkaConsumer(
        'crypto_prices',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        try:
            data = message.value
            
            # Handle new Coincheck ticker data structure
            if data.get('type') == 'ticker':
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raw_crypto_prices 
                        (type, pair, last, bid, ask, high, low, volume, timestamp, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        data.get('type'),
                        data.get('pair'),
                        data.get('last'),
                        data.get('bid'),
                        data.get('ask'),
                        data.get('high'),
                        data.get('low'),
                        data.get('volume'),
                        data.get('timestamp'),
                        data.get('source')
                    ))
                    conn.commit()
                    print(f"Inserted crypto ticker: {data.get('pair')} - Last: {data.get('last')}, Bid: {data.get('bid')}, Ask: {data.get('ask')}")
            
            # Handle legacy data structure for backward compatibility
            elif 'coin' in data and 'price' in data:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raw_crypto_prices 
                        (type, pair, last, bid, ask, high, low, volume, timestamp, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        'legacy',
                        data.get('coin'),
                        data.get('price'),
                        data.get('price'),  # Use price as both bid and ask for legacy data
                        data.get('price'),
                        data.get('price'),  # Use price as high and low for legacy data
                        data.get('price'),
                        0,  # No volume data in legacy format
                        data.get('timestamp'),
                        data.get('source', 'legacy')
                    ))
                    conn.commit()
                    print(f"Inserted legacy crypto price: {data.get('coin')} - ${data.get('price')}")
                    
        except Exception as e:
            print(f"Error inserting crypto data: {e}")
            print(f"Data: {data}")
            conn.rollback()

def consume_github_events():
    consumer = KafkaConsumer(
        'github_events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        try:
            data = message.value
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO raw_github_events (id, event_type, actor, repo, created_at, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    data['id'],
                    data['type'],
                    data['actor'],
                    data['repo'],
                    data['created_at'],
                    data['timestamp']
                ))
                conn.commit()
                print(f"Inserted GitHub event: {data['type']} by {data['actor']}")
        except Exception as e:
            print(f"Error inserting GitHub data: {e}")
            conn.rollback()

def consume_weather_data():
    consumer = KafkaConsumer(
        'weather_data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        try:
            data = message.value
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO raw_weather_data 
                    (city, temperature, humidity, pressure, weather, description, wind_speed, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['city'],
                    data['temperature'],
                    data['humidity'],
                    data['pressure'],
                    data['weather'],
                    data['description'],
                    data['wind_speed'],
                    data['timestamp']
                ))
                conn.commit()
                print(f"Inserted weather data for {data['city']}")
        except Exception as e:
            print(f"Error inserting weather data: {e}")
            conn.rollback()

if __name__ == "__main__":
    print("Starting Kafka consumers for crypto, GitHub, and weather data...")
    
    # Run consumers in separate threads
    threads = [
        threading.Thread(target=consume_crypto_prices),
        threading.Thread(target=consume_github_events),
        threading.Thread(target=consume_weather_data)
    ]
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
