{{
    config(
        materialized='view',
        database='data_lake',
        schema='staging'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('data_lake', 'crypto_prices') }}
),

renamed AS (
    SELECT
        coin,
        price,
        timestamp,
        source,
        year,
        month,
        day,
        -- Add technical indicators
        AVG(price) OVER (
            PARTITION BY coin 
            ORDER BY timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        AVG(price) OVER (
            PARTITION BY coin 
            ORDER BY timestamp 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50
    FROM source
)

SELECT * FROM renamed
