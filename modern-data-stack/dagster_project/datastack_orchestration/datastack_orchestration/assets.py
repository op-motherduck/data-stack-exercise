# =============================================================================
# DAGSTER ASSETS FOR MODERN DATA STACK
# =============================================================================
# Purpose: Defines all data processing assets for the Dagster orchestration:
#   - start_kafka_producers: Manages data streaming from external sources
#   - crypto_price_summary: Analyzes cryptocurrency price trends and statistics
#   - developer_insights: Processes GitHub activity data for developer analytics
#   - data_quality_checks: Validates data integrity across all sources
#   - weather_analytics: Analyzes weather patterns and city-based statistics
# 
# These assets represent the core data transformations and analytics
# that run on a schedule to provide real-time insights.
# =============================================================================

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
import subprocess
import pandas as pd
import duckdb
import psycopg2
from datetime import datetime, timedelta
import os
import platform

# Platform-specific path handling
def get_ducklake_path():
    if platform.system() == 'Darwin':  # macOS
        return os.path.expanduser('~/modern-data-stack/data/ducklake/parquet')
    else:
        return os.path.abspath('data/ducklake/parquet')

@asset
def start_kafka_producers(context):
    """Start Kafka producers for streaming data"""
    # Use absolute paths on macOS
    script_dir = os.path.abspath('scripts')
    producers = [
        f"python {script_dir}/crypto_producer.py",
        f"python {script_dir}/github_producer.py",
        f"python {script_dir}/weather_producer.py"
    ]
    
    for producer in producers:
        # On macOS, use explicit python3 if needed
        if platform.system() == 'Darwin':
            producer = producer.replace('python ', 'python3 ')
        
        subprocess.Popen(producer.split(), shell=False)
        context.log.info(f"Started: {producer}")
    
    return "Producers started"

@asset
def crypto_price_summary(context) -> MaterializeResult:
    """Generate crypto price summary statistics"""
    # Connect to PostgreSQL directly
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )
    
    query = """
    SELECT 
        pair as coin,
        COUNT(*) as data_points,
        AVG(last) as avg_price,
        MIN(last) as min_price,
        MAX(last) as max_price,
        STDDEV(last) as price_volatility
    FROM raw_crypto_prices
    WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY pair
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Save to parquet for MotherDuck
    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'data', 'crypto_summary.parquet')
    df.to_parquet(output_path)
    
    return MaterializeResult(
        metadata={
            "num_coins": len(df),
            "preview": MetadataValue.md(df.to_markdown())
        }
    )

@asset
def developer_insights(context) -> MaterializeResult:
    """Analyze developer activity patterns"""
    # Connect to PostgreSQL instead of DuckDB
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )
    
    query = """
    WITH top_developers AS (
        SELECT 
            actor,
            COUNT(DISTINCT DATE(created_at)) as active_days,
            COUNT(*) as total_events,
            COUNT(DISTINCT repo) as unique_repos
        FROM raw_github_events
        WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
        GROUP BY actor
        ORDER BY total_events DESC
        LIMIT 100
    )
    SELECT * FROM top_developers
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Export for MotherDuck
    df.to_parquet(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'data', 'developer_insights.parquet'))
    
    return MaterializeResult(
        metadata={
            "top_developers": len(df),
            "most_active": df.iloc[0]['actor'] if len(df) > 0 else "N/A"
        }
    )

@asset
def data_quality_checks(context):
    """Run data quality checks"""
    # Connect to PostgreSQL instead of DuckDB
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )
    
    checks = {
        "crypto_nulls": """
            SELECT COUNT(*) FROM raw_crypto_prices 
            WHERE last IS NULL OR pair IS NULL
        """,
        "github_duplicates": """
            SELECT COUNT(*) - COUNT(DISTINCT id) 
            FROM raw_github_events
        """,
        "weather_freshness": """
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/3600 as hours_old
            FROM raw_weather_data
        """
    }
    
    results = {}
    for check_name, query in checks.items():
        result = pd.read_sql_query(query, conn).iloc[0, 0]
        results[check_name] = result
        
        if check_name == "crypto_nulls" and result > 0:
            context.log.warning(f"Found {result} null values in crypto prices")
        elif check_name == "github_duplicates" and result > 0:
            context.log.warning(f"Found {result} duplicate GitHub events")
        elif check_name == "weather_freshness" and result > 6:
            context.log.warning(f"Weather data is {result} hours old")
    
    conn.close()
    return results

@asset
def weather_analytics(context) -> MaterializeResult:
    """Analyze weather patterns and trends"""
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )
    
    query = """
    SELECT 
        city,
        COUNT(*) as data_points,
        AVG(temperature) as avg_temperature,
        MIN(temperature) as min_temperature,
        MAX(temperature) as max_temperature,
        AVG(humidity) as avg_humidity,
        AVG(pressure) as avg_pressure,
        MODE() WITHIN GROUP (ORDER BY weather) as most_common_weather,
        MODE() WITHIN GROUP (ORDER BY description) as most_common_description
    FROM raw_weather_data
    WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY city
    ORDER BY avg_temperature DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Save to parquet for MotherDuck
    df.to_parquet(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'data', 'weather_analytics.parquet'))
    
    return MaterializeResult(
        metadata={
            "num_cities": MetadataValue.int(int(len(df))),
            "total_data_points": MetadataValue.int(int(df['data_points'].sum())),
            "temperature_range": MetadataValue.text(f"{float(df['min_temperature'].min()):.1f}°C to {float(df['max_temperature'].max()):.1f}°C"),
            "avg_temperature": MetadataValue.text(f"{float(df['avg_temperature'].mean()):.1f}°C"),
            "weather_summary": MetadataValue.md(df.to_markdown())
        }
    )
