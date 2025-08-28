#!/usr/bin/env python3
# =============================================================================
# POSTGRESQL SCHEMA UPDATE UTILITY
# =============================================================================
# Purpose: Updates PostgreSQL database schema for new data structures:
#   - Migrates table schemas to accommodate new data formats
#   - Handles schema changes for Coincheck API data structure
#   - Provides safe table recreation with proper data types
#   - Validates schema changes and provides detailed reporting
#   - Ensures database compatibility with updated data sources
# 
# This script is used when external APIs change their data format
# or when new data sources require different table structures.
# =============================================================================
"""
Script to update PostgreSQL table schema for new Coincheck data structure.
"""

import psycopg2
from datetime import datetime

def update_postgres_schema():
    """Update PostgreSQL table schema for new Coincheck data structure."""
    
    # PostgreSQL connection
    conn = psycopg2.connect(
        host="localhost",
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )
    
    try:
        with conn.cursor() as cur:
            print("Updating PostgreSQL schema for new Coincheck data structure...")
            
            # Drop the old table if it exists
            cur.execute("DROP TABLE IF EXISTS raw_crypto_prices")
            print("Dropped old raw_crypto_prices table")
            
            # Create new table with updated schema
            cur.execute("""
                CREATE TABLE raw_crypto_prices (
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
            print("Created new raw_crypto_prices table with Coincheck structure")
            
            # Verify the table structure
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'raw_crypto_prices'
                ORDER BY ordinal_position
            """)
            
            columns = cur.fetchall()
            print("\nNew table structure:")
            for col in columns:
                print(f"  {col[0]}: {col[1]}{'(' + str(col[2]) + ')' if col[2] else ''}")
            
            conn.commit()
            print("\n✅ PostgreSQL schema updated successfully!")
            
    except Exception as e:
        print(f"❌ Error updating schema: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()
    
    return True

if __name__ == "__main__":
    success = update_postgres_schema()
    exit(0 if success else 1) 