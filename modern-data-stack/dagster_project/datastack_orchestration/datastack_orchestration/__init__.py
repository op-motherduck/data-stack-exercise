from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition
from dagster_dbt import DbtCliResource
import os
import platform

from . import assets

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define jobs
streaming_job = define_asset_job(
    "streaming_pipeline",
    selection=AssetSelection.keys(["start_kafka_producers"])
)

analytics_job = define_asset_job(
    "analytics_pipeline",
    selection=AssetSelection.all() - AssetSelection.keys(["start_kafka_producers"])
)

# Define schedules
analytics_schedule = ScheduleDefinition(
    job=analytics_job,
    cron_schedule="0 * * * *",  # Every hour
)

# Platform-specific path handling
if platform.system() == 'Darwin':  # macOS
    dbt_project_dir = "/Users/op/Desktop/data-stack-exercise/data-stack-exercise/modern-data-stack/dbt_project/datastack_transform"
else:
    dbt_project_dir = os.path.abspath("../../dbt_project/datastack_transform")

# Define resources
resources = {
    "dbt": DbtCliResource(
        project_dir=dbt_project_dir,
    ),
}

defs = Definitions(
    assets=all_assets,
    jobs=[streaming_job, analytics_job],
    schedules=[analytics_schedule],
    resources=resources,
)
