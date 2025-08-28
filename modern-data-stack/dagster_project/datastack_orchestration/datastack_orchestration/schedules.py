# =============================================================================
# DAGSTER SCHEDULES AND JOB DEFINITIONS
# =============================================================================
# Purpose: Defines automated execution schedules for data processing:
#   - real_time_assets: Analytics assets that run every 5 minutes
#   - data_quality_checks: Data validation that runs every 15 minutes
#   - kafka_producers: Stream management that runs every 30 minutes
# 
# These schedules ensure continuous data processing and monitoring
# without manual intervention, providing real-time insights and alerts.
# =============================================================================

from dagster import ScheduleDefinition, define_asset_job, AssetSelection

# Define different asset groups for different schedules
real_time_assets = AssetSelection.keys("crypto_price_summary", "developer_insights", "weather_analytics")
data_quality_assets = AssetSelection.keys("data_quality_checks")
kafka_assets = AssetSelection.keys("start_kafka_producers")

# Job for real-time assets (every 5 minutes)
real_time_job = define_asset_job(
    name="real_time_assets_job",
    selection=real_time_assets,
    description="Materialize real-time analytics assets"
)

# Job for data quality checks (every 15 minutes)
data_quality_job = define_asset_job(
    name="data_quality_job", 
    selection=data_quality_assets,
    description="Run data quality checks"
)

# Job for Kafka producers (every 30 minutes - less frequent since they're long-running)
kafka_job = define_asset_job(
    name="kafka_producers_job",
    selection=kafka_assets,
    description="Manage Kafka producers"
)

# Schedule for real-time assets - every 5 minutes
real_time_schedule = ScheduleDefinition(
    job=real_time_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    name="real_time_assets_schedule",
    description="Materialize real-time analytics assets every 5 minutes"
)

# Schedule for data quality - every 15 minutes
data_quality_schedule = ScheduleDefinition(
    job=data_quality_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    name="data_quality_schedule", 
    description="Run data quality checks every 15 minutes"
)

# Schedule for Kafka producers - every 30 minutes
kafka_schedule = ScheduleDefinition(
    job=kafka_job,
    cron_schedule="*/30 * * * *",  # Every 30 minutes
    name="kafka_producers_schedule",
    description="Manage Kafka producers every 30 minutes"
)
