import psycopg2
import time
from datetime import datetime, timedelta

def monitor_pipeline():
    conn = psycopg2.connect(
        host="localhost",
        database="datastack",
        user="dataeng",
        password="dataeng123"
    )
    
    while True:
        with conn.cursor() as cur:
            # Check data freshness
            cur.execute("""
                SELECT 
                    'crypto' as source,
                    COUNT(*) as records_last_hour,
                    MAX(timestamp) as latest_record
                FROM raw_crypto_prices
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                UNION ALL
                SELECT 
                    'github' as source,
                    COUNT(*) as records_last_hour,
                    MAX(timestamp) as latest_record
                FROM raw_github_events
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                UNION ALL
                SELECT 
                    'weather' as source,
                    COUNT(*) as records_last_hour,
                    MAX(timestamp) as latest_record
                FROM raw_weather_data
                WHERE timestamp > NOW() - INTERVAL '1 hour'
            """)
            
            results = cur.fetchall()
            
            print(f"\n=== Pipeline Status at {datetime.now()} ===")
            for source, count, latest in results:
                print(f"{source}: {count} records in last hour, latest: {latest}")
        
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    monitor_pipeline()
