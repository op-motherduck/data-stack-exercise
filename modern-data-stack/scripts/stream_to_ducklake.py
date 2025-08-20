#!/usr/bin/env python3
"""
Real-time streaming script that continuously moves data from PostgreSQL to DuckLake.
"""

import psycopg2
import duckdb
import time
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

def get_postgres_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )

def get_ducklake_connection():
    """Get DuckLake connection."""
    return duckdb.connect()

def setup_ducklake_tables(conn):
    """Setup DuckLake tables for streaming data."""
    
    # Create staging tables with new Coincheck structure
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_crypto_prices (
            id INTEGER,
            type VARCHAR,
            pair VARCHAR,
            last DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            high DOUBLE,
            low DOUBLE,
            volume DOUBLE,
            timestamp TIMESTAMP,
            source VARCHAR DEFAULT 'postgres_stream'
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_github_events (
            id INTEGER,
            event_type VARCHAR,
            actor VARCHAR,
            repo VARCHAR,
            created_at TIMESTAMP,
            timestamp TIMESTAMP,
            source VARCHAR DEFAULT 'postgres_stream'
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_weather_data (
            id INTEGER,
            city VARCHAR,
            temperature DOUBLE,
            humidity INTEGER,
            description VARCHAR,
            timestamp TIMESTAMP,
            source VARCHAR DEFAULT 'postgres_stream'
        )
    """)
    
    print("DuckLake staging tables created/verified")

def stream_data_to_ducklake():
    """Continuously stream data from PostgreSQL to DuckLake."""
    
    print("Starting real-time data streaming to DuckLake...")
    
    # Setup connections
    try:
        pg_conn = get_postgres_connection()
        dk_conn = get_ducklake_connection()
        print("Connected to both PostgreSQL and DuckLake")
    except Exception as e:
        print(f"Error connecting to databases: {e}")
        return False
    
    # Setup DuckLake tables
    setup_ducklake_tables(dk_conn)
    
    # Track last processed timestamps
    last_crypto = datetime.now() - timedelta(minutes=5)
    last_github = datetime.now() - timedelta(minutes=5)
    last_weather = datetime.now() - timedelta(minutes=5)
    
    try:
        while True:
            current_time = datetime.now()
            
            # Stream crypto prices with new structure
            try:
                with pg_conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, type, pair, last, bid, ask, high, low, volume, timestamp 
                        FROM raw_crypto_prices 
                        WHERE timestamp > %s
                        ORDER BY timestamp
                    """, (last_crypto,))
                    
                    crypto_data = cur.fetchall()
                    if crypto_data:
                        for row in crypto_data:
                            dk_conn.execute("""
                                INSERT INTO staging_crypto_prices 
                                (id, type, pair, last, bid, ask, high, low, volume, timestamp)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, row)
                        last_crypto = crypto_data[-1][9]  # Update timestamp (index 9 for timestamp)
                        print(f"Streamed {len(crypto_data)} crypto records")
            except Exception as e:
                print(f"Error streaming crypto data: {e}")
            
            # Stream GitHub events
            try:
                with pg_conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, event_type, actor, repo, created_at, timestamp
                        FROM raw_github_events 
                        WHERE timestamp > %s
                        ORDER BY timestamp
                    """, (last_github,))
                    
                    github_data = cur.fetchall()
                    if github_data:
                        for row in github_data:
                            dk_conn.execute("""
                                INSERT INTO staging_github_events (id, event_type, actor, repo, created_at, timestamp)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, row)
                        last_github = github_data[-1][5]  # Update timestamp
                        print(f"Streamed {len(github_data)} GitHub records")
            except Exception as e:
                print(f"Error streaming GitHub data: {e}")
            
            # Stream weather data
            try:
                with pg_conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, city, temperature, humidity, description, timestamp
                        FROM raw_weather_data 
                        WHERE timestamp > %s
                        ORDER BY timestamp
                    """, (last_weather,))
                    
                    weather_data = cur.fetchall()
                    if weather_data:
                        for row in weather_data:
                            dk_conn.execute("""
                                INSERT INTO staging_weather_data (id, city, temperature, humidity, description, timestamp)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, row)
                        last_weather = weather_data[-1][5]  # Update timestamp
                        print(f"Streamed {len(weather_data)} weather records")
            except Exception as e:
                print(f"Error streaming weather data: {e}")
            
            # Save to parquet files periodically
            if current_time.minute % 5 == 0:  # Every 5 minutes
                try:
                    data_dir = Path.home() / "modern-data-stack" / "data" / "ducklake" / "parquet"
                    data_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Export to parquet
                    dk_conn.execute(f"COPY staging_crypto_prices TO '{data_dir}/crypto_prices.parquet' (FORMAT PARQUET)")
                    dk_conn.execute(f"COPY staging_github_events TO '{data_dir}/github_events.parquet' (FORMAT PARQUET)")
                    dk_conn.execute(f"COPY staging_weather_data TO '{data_dir}/weather_data.parquet' (FORMAT PARQUET)")
                    
                    print(f"Exported data to parquet files at {current_time}")
                except Exception as e:
                    print(f"Error exporting to parquet: {e}")
            
            # Wait before next iteration
            time.sleep(10)  # Check every 10 seconds
            
    except KeyboardInterrupt:
        print("\nStopping data streaming...")
    except Exception as e:
        print(f"Error in streaming loop: {e}")
    finally:
        pg_conn.close()
        dk_conn.close()
        print("Streaming stopped")

if __name__ == "__main__":
    stream_data_to_ducklake() 