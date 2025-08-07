import duckdb
import os

# Set your MotherDuck token
os.environ['motherduck_token'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Im9saXZpYUBtb3RoZXJkdWNrLmNvbSIsInNlc3Npb24iOiJvbGl2aWEubW90aGVyZHVjay5jb20iLCJwYXQiOiJUaF91R3FlYXlBdE9XX0lzMzhPTVpBUjhDTjFBWFNOd0poT2FsQ1NkRnQ4IiwidXNlcklkIjoiY2I3MmU2NjMtNWU1MC00Zjc5LWJmZTktODZmZDQ4Nzk0ODA3IiwiaXNzIjoibWRfcGF0IiwicmVhZE9ubHkiOmZhbHNlLCJ0b2tlblR5cGUiOiJyZWFkX3dyaXRlIiwiaWF0IjoxNzU0NTQwOTIyfQ.yo3mCM9Qy80KVMKLbpxCdm4AUXT4KR6LTO3Mnf_eizQ'

def sync_ducklake_to_motherduck():
    """Sync DuckLake tables to MotherDuck for collaborative analytics"""
    
    # Connect to MotherDuck
    md_conn = duckdb.connect('md:datastack')
    
    # Install and load DuckLake extension
    md_conn.execute("INSTALL ducklake")
    md_conn.execute("LOAD ducklake")
    
    # Attach local DuckLake to MotherDuck
    md_conn.execute("""
        ATTACH 'ducklake:host=your-remote-postgres port=5432 
                user=ducklake password=ducklake123 database=ducklake_catalog' 
        AS data_lake (DATA_PATH 's3://your-bucket/ducklake/')
    """)
    
    # Create schemas in MotherDuck
    md_conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    md_conn.execute("CREATE SCHEMA IF NOT EXISTS reporting")
    
    # Create materialized views in MotherDuck for fast queries
    md_conn.execute("""
        CREATE OR REPLACE TABLE analytics.crypto_summary AS
        SELECT 
            coin,
            DATE_TRUNC('day', timestamp) as day,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            STDDEV(price) as volatility,
            COUNT(*) as data_points,
            SUM(CASE WHEN price > ma_20 THEN 1 ELSE 0 END) / COUNT(*) as pct_above_ma20
        FROM data_lake.staging.stg_crypto_prices
        WHERE timestamp > CURRENT_DATE - INTERVAL '30 days'
        GROUP BY coin, DATE_TRUNC('day', timestamp)
    """)
    
    # Create real-time dashboard views
    md_conn.execute("""
        CREATE OR REPLACE VIEW reporting.crypto_dashboard AS
        WITH latest_prices AS (
            SELECT 
                coin,
                price as current_price,
                ma_20,
                ma_50,
                ROW_NUMBER() OVER (PARTITION BY coin ORDER BY timestamp DESC) as rn
            FROM data_lake.staging.stg_crypto_prices
        ),
        daily_stats AS (
            SELECT 
                coin,
                AVG(volatility) as avg_daily_volatility,
                AVG(avg_price) as avg_price_30d
            FROM analytics.crypto_summary
            GROUP BY coin
        )
        SELECT 
            lp.coin,
            lp.current_price,
            lp.ma_20,
            lp.ma_50,
            ds.avg_daily_volatility,
            ds.avg_price_30d,
            (lp.current_price - ds.avg_price_30d) / ds.avg_price_30d * 100 as pct_change_30d,
            CASE 
                WHEN lp.current_price > lp.ma_20 AND lp.ma_20 > lp.ma_50 THEN 'STRONG_BUY'
                WHEN lp.current_price > lp.ma_20 THEN 'BUY'
                WHEN lp.current_price < lp.ma_20 AND lp.ma_20 < lp.ma_50 THEN 'STRONG_SELL'
                WHEN lp.current_price < lp.ma_20 THEN 'SELL'
                ELSE 'HOLD'
            END as signal
        FROM latest_prices lp
        JOIN daily_stats ds ON lp.coin = ds.coin
        WHERE lp.rn = 1
    """)
    
    # Create shared analytical functions
    md_conn.execute("""
        CREATE OR REPLACE FUNCTION analytics.calculate_sharpe_ratio(
            coin_name VARCHAR,
            lookback_days INTEGER DEFAULT 30
        ) AS (
            WITH returns AS (
                SELECT 
                    (LEAD(avg_price) OVER (ORDER BY day) - avg_price) / avg_price as daily_return
                FROM analytics.crypto_summary
                WHERE coin = coin_name
                AND day > CURRENT_DATE - INTERVAL (lookback_days || ' days')
            )
            SELECT 
                AVG(daily_return) / NULLIF(STDDEV(daily_return), 0) * SQRT(365) as sharpe_ratio
            FROM returns
        )
    """)
    
    print("MotherDuck sync completed!")
    
    # Show available data
    result = md_conn.execute("""
        SELECT 
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema IN ('analytics', 'reporting')
        ORDER BY table_schema, table_name
    """).fetchall()
    
    print("\nAvailable tables and views in MotherDuck:")
    for schema, table, type in result:
        print(f"  {schema}.{table} ({type})")
    
    # Example query
    dashboard = md_conn.execute("""
        SELECT * FROM reporting.crypto_dashboard
    """).fetchdf()
    
    print("\nCrypto Dashboard Preview:")
    print(dashboard)
    
    md_conn.close()

if __name__ == "__main__":
    sync_ducklake_to_motherduck()
