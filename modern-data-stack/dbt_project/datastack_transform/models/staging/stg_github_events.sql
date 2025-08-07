{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_github_events') }}
),

cleaned AS (
    SELECT
        id,
        event_type,
        actor,
        repo,
        created_at,
        timestamp,
        ingested_at,
        -- Extract org and repo name
        SPLIT_PART(repo, '/', 1) AS org_name,
        SPLIT_PART(repo, '/', 2) AS repo_name
    FROM source
)

SELECT * FROM cleaned
