
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_dbt import dbt_assets, DbtCliResource
import subprocess
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import os
import platform

# Platform-specific path handling
def get_ducklake_path():
    if platform.system() == 'Darwin':  # macOS
        return os.path.expanduser('~/modern-data-stack/data/ducklake/parquet')
    else:
        return os.path.abspath('data/ducklake/parquet')

# Define dbt assets (update path for your system)
@asset
def datastack_dbt_assets(context: AssetExecutionContext):
    """Run dbt build to transform data"""
    yield from context.resources.dbt.cli(["build"], context=context).stream()

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
    # Connect to DuckLake with proper path
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake IF NOT EXISTS")
    conn.execute("INSTALL postgres IF NOT EXISTS")
    
    base_path = get_ducklake_path()
    conn.execute(f"""
        ATTACH 'ducklake:host=localhost port=5433 user=ducklake 
                password=ducklake123 database=ducklake_catalog' 
        AS data_lake (DATA_PATH '{base_path}/')
    """)
    
    query = """
    SELECT 
        coin,
        COUNT(*) as data_points,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        STDDEV(price) as price_volatility
    FROM data_lake.raw.crypto_prices
    WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY coin
    """
    
    df = conn.execute(query).fetchdf()
    
    # Save to parquet for MotherDuck
    output_path = os.path.join('data', 'crypto_summary.parquet')
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
    conn = duckdb.connect('datastack.duckdb')
    
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
    
    df = conn.execute(query).fetchdf()
    
    # Export for MotherDuck
    df.to_parquet('data/developer_insights.parquet')
    
    return MaterializeResult(
        metadata={
            "top_developers": len(df),
            "most_active": df.iloc[0]['actor'] if len(df) > 0 else "N/A"
        }
    )

@asset
def data_quality_checks(context):
    """Run data quality checks"""
    conn = duckdb.connect('datastack.duckdb')
    
    checks = {
        "crypto_nulls": """
            SELECT COUNT(*) FROM raw_crypto_prices 
            WHERE price IS NULL OR coin IS NULL
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
        result = conn.execute(query).fetchone()[0]
        results[check_name] = result
        
        if check_name == "crypto_nulls" and result > 0:
            context.log.warning(f"Found {result} null values in crypto prices")
        elif check_name == "github_duplicates" and result > 0:
            context.log.warning(f"Found {result} duplicate GitHub events")
        elif check_name == "weather_freshness" and result > 6:
            context.log.warning(f"Weather data is {result} hours old")
    
    return results
