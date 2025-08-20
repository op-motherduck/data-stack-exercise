{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_crypto_prices') }}
),

renamed AS (
    SELECT
        type,
        pair,
        last AS price,  -- Use 'last' as the main price for compatibility
        bid,
        ask,
        high,
        low,
        volume,
        timestamp,
        source,
        -- Extract date components for partitioning
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(DAY FROM timestamp) AS day,
        -- Calculate spread (bid-ask spread)
        (ask - bid) AS spread,
        -- Calculate spread percentage
        CASE 
            WHEN bid > 0 THEN ((ask - bid) / bid) * 100 
            ELSE 0 
        END AS spread_percentage,
        -- Add technical indicators using 'last' price
        AVG(last) OVER (
            PARTITION BY pair 
            ORDER BY timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        AVG(last) OVER (
            PARTITION BY pair 
            ORDER BY timestamp 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50,
        -- Calculate price change from high to low
        (high - low) AS daily_range,
        -- Calculate volume-weighted average price (VWAP) approximation
        SUM(last * volume) OVER (
            PARTITION BY pair, DATE(timestamp)
            ORDER BY timestamp 
            ROWS UNBOUNDED PRECEDING
        ) / SUM(volume) OVER (
            PARTITION BY pair, DATE(timestamp)
            ORDER BY timestamp 
            ROWS UNBOUNDED PRECEDING
        ) AS vwap
    FROM source
    WHERE type = 'ticker'  -- Only process ticker data, not legacy data
)

SELECT * FROM renamed
