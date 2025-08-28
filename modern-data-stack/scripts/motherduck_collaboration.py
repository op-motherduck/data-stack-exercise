# =============================================================================
# MOTHERDUCK COLLABORATIVE ANALYTICS WORKSPACE
# =============================================================================
# Purpose: Sets up collaborative analytics environment in MotherDuck cloud:
#   - Creates role-based schemas for different user types (engineers, analysts, executives)
#   - Establishes curated views and dashboards for each role
#   - Implements parameterized reports and macros for flexible analysis
#   - Provides KPI dashboards and trend analysis capabilities
#   - Enables team collaboration on data analytics and insights
# 
# This script creates a cloud-based collaborative workspace where
# different stakeholders can access appropriate data views and analytics.
# =============================================================================

import duckdb
import os

def setup_collaborative_workspace():
    """Set up collaborative analytics workspace in MotherDuck"""
    
    # Connect to MotherDuck
    conn = duckdb.connect('md:')
    
    # Create a shared database for the team
    conn.execute("CREATE DATABASE IF NOT EXISTS team_analytics")
    conn.execute("USE team_analytics")
    
    # Create role-based views
    conn.execute("""
        -- Data Engineer View: Full access to raw and staging data
        CREATE SCHEMA IF NOT EXISTS data_engineering;
        
        CREATE VIEW data_engineering.pipeline_health AS
        SELECT 
            'crypto_prices' as pipeline,
            COUNT(*) as total_records,
            MAX(timestamp) as latest_record,
            CURRENT_TIMESTAMP - MAX(timestamp) as lag,
            COUNT(DISTINCT DATE(timestamp)) as days_with_data
        FROM datastack.analytics.crypto_summary
        UNION ALL
        SELECT 
            'github_events' as pipeline,
            COUNT(*) as total_records,
            MAX(activity_date) as latest_record,
            CURRENT_TIMESTAMP - MAX(activity_date) as lag,
            COUNT(DISTINCT activity_date) as days_with_data
        FROM datastack.analytics.developer_activity;
        
        -- Analyst View: Curated datasets for analysis
        CREATE SCHEMA IF NOT EXISTS analyst;
        
        CREATE VIEW analyst.crypto_trends AS
        WITH daily_prices AS (
            SELECT 
                coin,
                day,
                avg_price,
                LAG(avg_price, 7) OVER (PARTITION BY coin ORDER BY day) as price_7d_ago,
                LAG(avg_price, 30) OVER (PARTITION BY coin ORDER BY day) as price_30d_ago
            FROM datastack.analytics.crypto_summary
        )
        SELECT 
            coin,
            day,
            avg_price,
            (avg_price - price_7d_ago) / NULLIF(price_7d_ago, 0) * 100 as weekly_change,
            (avg_price - price_30d_ago) / NULLIF(price_30d_ago, 0) * 100 as monthly_change
        FROM daily_prices
        WHERE day >= CURRENT_DATE - INTERVAL '90 days';
        
        -- Executive View: High-level KPIs
        CREATE SCHEMA IF NOT EXISTS executive;
        
        CREATE VIEW executive.kpi_dashboard AS
        SELECT 
            'Data Volume' as metric,
            'Total Records Processed' as description,
            TO_CHAR(SUM(data_points), '999,999,999') as value
        FROM datastack.analytics.crypto_summary
        UNION ALL
        SELECT 
            'Active Cryptocurrencies',
            'Unique coins tracked',
            COUNT(DISTINCT coin)::VARCHAR
        FROM datastack.analytics.crypto_summary
        UNION ALL
        SELECT 
            'Developer Activity',
            'Active developers (30d)',
            COUNT(DISTINCT actor)::VARCHAR
        FROM datastack.analytics.developer_activity
        WHERE activity_date > CURRENT_DATE - INTERVAL '30 days';
    """)
    
    # Create parameterized reports
    conn.execute("""
        CREATE OR REPLACE MACRO analyst.volatility_report(
            start_date DATE DEFAULT CURRENT_DATE - INTERVAL '7 days',
            end_date DATE DEFAULT CURRENT_DATE,
            min_volatility DECIMAL DEFAULT 0.05
        ) AS TABLE (
            SELECT 
                coin,
                AVG(volatility) as avg_volatility,
                MAX(volatility) as max_volatility,
                COUNT(*) as volatile_days,
                LIST(day ORDER BY volatility DESC) as most_volatile_days
            FROM datastack.analytics.crypto_summary
            WHERE day BETWEEN start_date AND end_date
            AND volatility > min_volatility
            GROUP BY coin
            ORDER BY avg_volatility DESC
        );
    """)
    
    print("Collaborative workspace created!")
    
    # Test the macro
    report = conn.execute("""
        SELECT * FROM analyst.volatility_report(
            start_date := CURRENT_DATE - INTERVAL '14 days'
        )
    """).fetchdf()
    
    print("\nVolatility Report (Last 14 days):")
    print(report)
    
    conn.close()

if __name__ == "__main__":
    setup_collaborative_workspace()
