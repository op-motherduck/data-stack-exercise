#!/usr/bin/env python3
# =============================================================================
# DUCKLAKE METADATA STORE SETUP
# =============================================================================
# Purpose: Initializes DuckLake metadata store and configuration:
#   - Creates necessary directories for parquet file storage
#   - Establishes connection to DuckLake metadata database
#   - Creates metadata tables for data lineage tracking
#   - Sets up data quality metrics and pipeline monitoring tables
#   - Initializes sample data for testing and validation
# 
# This script prepares the lakehouse infrastructure for data storage
# and provides metadata management for the analytics pipeline.
# =============================================================================
"""
Setup script for DuckLake metadata store and initial configuration.
"""

import duckdb
import os
import sys
from pathlib import Path

def setup_ducklake():
    """Initialize DuckLake metadata store and create necessary tables."""
    
    print("Setting up DuckLake metadata store...")
    
    # Create data directory if it doesn't exist
    data_dir = Path.home() / "modern-data-stack" / "data" / "ducklake" / "parquet"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckLake metadata store
    try:
        conn = duckdb.connect('ducklake:host=localhost port=5433 user=ducklake password=ducklake123 database=ducklake_catalog')
        print("Connected to DuckLake metadata store")
    except Exception as e:
        print(f"Error connecting to DuckLake: {e}")
        print("Make sure DuckLake is running on localhost:5433")
        return False
    
    # Create metadata tables
    try:
        # Create tables for tracking data lineage
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_lineage (
                id INTEGER,
                source_table VARCHAR,
                target_table VARCHAR,
                transformation_type VARCHAR,
                created_at TIMESTAMP,
                status VARCHAR
            )
        """)
        
        # Create tables for data quality metrics
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id INTEGER,
                table_name VARCHAR,
                metric_name VARCHAR,
                metric_value DOUBLE,
                threshold DOUBLE,
                status VARCHAR,
                created_at TIMESTAMP
            )
        """)
        
        # Create tables for pipeline monitoring
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER,
                pipeline_name VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status VARCHAR,
                records_processed INTEGER,
                error_message TEXT
            )
        """)
        
        print("Created metadata tables successfully")
        
        # Initialize with some sample data
        conn.execute("""
            INSERT INTO data_lineage (id, source_table, target_table, transformation_type)
            VALUES 
                (1, 'raw_crypto_prices', 'staging_crypto_prices', 'validation'),
                (2, 'raw_github_events', 'staging_github_events', 'validation'),
                (3, 'raw_weather_data', 'staging_weather_data', 'validation')
        """)
        
        print("Initialized with sample lineage data")
        
    except Exception as e:
        print(f"Error setting up metadata tables: {e}")
        return False
    
    finally:
        conn.close()
    
    print("DuckLake setup completed successfully!")
    return True

if __name__ == "__main__":
    success = setup_ducklake()
    sys.exit(0 if success else 1) 