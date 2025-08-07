{{
    config(
        materialized='table'
    )
}}

WITH daily_activity AS (
    SELECT
        DATE(created_at) AS activity_date,
        actor,
        COUNT(DISTINCT id) AS total_events,
        COUNT(DISTINCT repo) AS unique_repos,
        COUNT(DISTINCT CASE WHEN event_type = 'PushEvent' THEN id END) AS push_events,
        COUNT(DISTINCT CASE WHEN event_type = 'PullRequestEvent' THEN id END) AS pr_events,
        COUNT(DISTINCT CASE WHEN event_type = 'IssuesEvent' THEN id END) AS issue_events
    FROM {{ ref('stg_github_events') }}
    GROUP BY 1, 2
)

SELECT * FROM daily_activity
