# =============================================================================
# DAGSTER DEFINITIONS AND CONFIGURATION
# =============================================================================
# Purpose: Configures the Dagster application with:
#   - Asset definitions loaded from the assets module
#   - Job definitions for different processing workflows
#   - Schedule definitions for automated execution
#   - Resource configurations (dbt integration)
#   - Path configurations for external tools
# 
# This file serves as the main entry point for the Dagster application
# and defines how all components interact and execute.
# =============================================================================

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
import os

from datastack_orchestration import assets  # noqa: TID252
from datastack_orchestration.schedules import (
    real_time_job, data_quality_job, kafka_job,
    real_time_schedule, data_quality_schedule, kafka_schedule
)

all_assets = load_assets_from_modules([assets])

# Get the absolute path to the dbt project
current_dir = os.path.dirname(os.path.abspath(__file__))
dbt_project_dir = os.path.join(current_dir, "..", "..", "..", "dbt_project", "datastack_transform")

defs = Definitions(
    assets=all_assets,
    jobs=[real_time_job, data_quality_job, kafka_job],
    schedules=[real_time_schedule, data_quality_schedule, kafka_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir
        )
    }
)
