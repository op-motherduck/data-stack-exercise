#!/usr/bin/env python3
# =============================================================================
# POSTGRESQL TO DUCKLAKE DATA MIGRATION
# =============================================================================
# Purpose: Performs one-time migration of existing data from PostgreSQL to DuckLake:
#   - Migrates all historical data from PostgreSQL tables to DuckLake
#   - Creates parquet files for analytics and cloud storage
#   - Generates summary statistics and data lineage information
#   - Handles different data structures and formats
#   - Provides detailed migration reporting and validation
# 
# This script is used for initial data lake population and can be run
# periodically to sync historical data between systems.
# =============================================================================
"""
One-time migration script to move existing data from PostgreSQL to DuckLake.
"""

import psycopg2
import duckdb
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

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

def migrate_table(pg_conn, dk_conn, table_name, query):
    """Migrate a single table from PostgreSQL to DuckLake."""
    
    print(f"Migrating {table_name}...")
    
    try:
        # Read data from PostgreSQL
        df = pd.read_sql_query(query, pg_conn)
        
        if df.empty:
            print(f"No data found in {table_name}")
            return 0
        
        # Create table in DuckLake
        dk_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        dk_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        # Export to parquet
        data_dir = Path.home() / "modern-data-stack" / "data" / "ducklake" / "parquet"
        data_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_path = data_dir / f"{table_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        print(f"Migrated {len(df)} records from {table_name} to DuckLake and parquet")
        return len(df)
        
    except Exception as e:
        print(f"Error migrating {table_name}: {e}")
        return 0

def migrate_data():
    """Perform one-time migration of all data from PostgreSQL to DuckLake."""
    
    print("Starting PostgreSQL to DuckLake migration...")
    
    # Setup connections
    try:
        pg_conn = get_postgres_connection()
        dk_conn = get_ducklake_connection()
        print("Connected to both PostgreSQL and DuckLake")
    except Exception as e:
        print(f"Error connecting to databases: {e}")
        return False
    
    total_records = 0
    
    try:
        # Migrate crypto prices with new Coincheck structure
        crypto_query = """
            SELECT id, type, pair, last, bid, ask, high, low, volume, timestamp, source
            FROM raw_crypto_prices 
            ORDER BY timestamp
        """
        total_records += migrate_table(pg_conn, dk_conn, "raw_crypto_prices", crypto_query)
        
        # Migrate GitHub events
        github_query = """
            SELECT id, event_type, actor, repo, created_at, timestamp
            FROM raw_github_events 
            ORDER BY timestamp
        """
        total_records += migrate_table(pg_conn, dk_conn, "raw_github_events", github_query)
        
        # Migrate weather data
        weather_query = """
            SELECT id, city, temperature, humidity, description, timestamp
            FROM raw_weather_data 
            ORDER BY timestamp
        """
        total_records += migrate_table(pg_conn, dk_conn, "raw_weather_data", weather_query)
        
        # Create summary statistics
        print("\nCreating summary statistics...")
        dk_conn.execute("""
            CREATE TABLE IF NOT EXISTS data_summary (
                table_name VARCHAR,
                record_count INTEGER,
                last_updated TIMESTAMP,
                migration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Get counts for each table
        for table in ["raw_crypto_prices", "raw_github_events", "raw_weather_data"]:
            try:
                count = dk_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                dk_conn.execute(f"""
                    INSERT INTO data_summary (table_name, record_count, last_updated)
                    SELECT '{table}', {count}, MAX(timestamp) FROM {table}
                """)
            except Exception as e:
                print(f"Error creating summary for {table}: {e}")
        
        print(f"\nMigration completed! Total records migrated: {total_records}")
        
        # Show summary
        summary = dk_conn.execute("SELECT * FROM data_summary ORDER BY table_name").fetchdf()
        print("\nData Summary:")
        print(summary.to_string(index=False))
        
        # Show crypto data sample
        print("\nCrypto Data Sample:")
        crypto_sample = dk_conn.execute("""
            SELECT pair, last, bid, ask, volume, timestamp 
            FROM raw_crypto_prices 
            ORDER BY timestamp DESC 
            LIMIT 5
        """).fetchdf()
        print(crypto_sample.to_string(index=False))
        
    except Exception as e:
        print(f"Error during migration: {e}")
        return False
    
    finally:
        pg_conn.close()
        dk_conn.close()
    
    return True

if __name__ == "__main__":
    success = migrate_data()
    sys.exit(0 if success else 1) 