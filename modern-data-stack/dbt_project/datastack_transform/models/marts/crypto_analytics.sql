{{
    config(
        materialized='table'
    )
}}

WITH hourly_prices AS (
    SELECT
        coin,
        DATE_TRUNC('hour', timestamp) AS hour,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        COUNT(*) AS data_points
    FROM {{ ref('stg_crypto_prices') }}
    GROUP BY 1, 2
),

price_changes AS (
    SELECT
        coin,
        hour,
        avg_price,
        LAG(avg_price) OVER (PARTITION BY coin ORDER BY hour) AS prev_hour_price,
        (avg_price - LAG(avg_price) OVER (PARTITION BY coin ORDER BY hour)) / 
            NULLIF(LAG(avg_price) OVER (PARTITION BY coin ORDER BY hour), 0) * 100 AS pct_change
    FROM hourly_prices
)

SELECT * FROM price_changes
