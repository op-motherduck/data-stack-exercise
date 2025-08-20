{{
    config(
        materialized='table'
    )
}}

WITH hourly_prices AS (
    SELECT
        pair,
        DATE_TRUNC('hour', timestamp) AS hour,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(bid) AS avg_bid,
        AVG(ask) AS avg_ask,
        AVG(spread) AS avg_spread,
        AVG(spread_percentage) AS avg_spread_percentage,
        SUM(volume) AS total_volume,
        COUNT(*) AS data_points
    FROM {{ ref('stg_crypto_prices') }}
    GROUP BY 1, 2
),

price_changes AS (
    SELECT
        pair,
        hour,
        avg_price,
        avg_bid,
        avg_ask,
        avg_spread,
        avg_spread_percentage,
        total_volume,
        data_points,
        LAG(avg_price) OVER (PARTITION BY pair ORDER BY hour) AS prev_hour_price,
        (avg_price - LAG(avg_price) OVER (PARTITION BY pair ORDER BY hour)) / 
            NULLIF(LAG(avg_price) OVER (PARTITION BY pair ORDER BY hour), 0) * 100 AS pct_change,
        -- Volume change
        LAG(total_volume) OVER (PARTITION BY pair ORDER BY hour) AS prev_hour_volume,
        (total_volume - LAG(total_volume) OVER (PARTITION BY pair ORDER BY hour)) / 
            NULLIF(LAG(total_volume) OVER (PARTITION BY pair ORDER BY hour), 0) * 100 AS volume_pct_change,
        -- Spread change
        LAG(avg_spread_percentage) OVER (PARTITION BY pair ORDER BY hour) AS prev_hour_spread_pct,
        (avg_spread_percentage - LAG(avg_spread_percentage) OVER (PARTITION BY pair ORDER BY hour)) AS spread_pct_change
    FROM hourly_prices
),

daily_summary AS (
    SELECT
        pair,
        DATE_TRUNC('day', hour) AS day,
        AVG(avg_price) AS daily_avg_price,
        MIN(avg_price) AS daily_min_price,
        MAX(avg_price) AS daily_max_price,
        AVG(avg_spread_percentage) AS daily_avg_spread_pct,
        SUM(total_volume) AS daily_total_volume,
        COUNT(*) AS hourly_intervals
    FROM hourly_prices
    GROUP BY 1, 2
)

SELECT 
    pc.*,
    ds.daily_avg_price,
    ds.daily_min_price,
    ds.daily_max_price,
    ds.daily_avg_spread_pct,
    ds.daily_total_volume,
    ds.hourly_intervals,
    -- Volatility indicator
    CASE 
        WHEN ABS(pc.pct_change) > 5 THEN 'High'
        WHEN ABS(pc.pct_change) > 2 THEN 'Medium'
        ELSE 'Low'
    END AS volatility_level,
    -- Volume indicator
    CASE 
        WHEN pc.volume_pct_change > 50 THEN 'High'
        WHEN pc.volume_pct_change > 20 THEN 'Medium'
        ELSE 'Low'
    END AS volume_trend
FROM price_changes pc
LEFT JOIN daily_summary ds ON pc.pair = ds.pair AND DATE_TRUNC('day', pc.hour) = ds.day
