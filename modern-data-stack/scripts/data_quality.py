# =============================================================================
# DATA QUALITY VALIDATION WITH GREAT EXPECTATIONS
# =============================================================================
# Purpose: Implements data quality checks using Great Expectations framework:
#   - Validates data completeness, accuracy, and consistency
#   - Checks for null values, data ranges, and expected values
#   - Validates cryptocurrency price data against business rules
#   - Generates validation reports and alerts for data issues
#   - Integrates with the data pipeline for automated quality monitoring
# 
# This script ensures data reliability and helps maintain data integrity
# across the entire data stack.
# =============================================================================

import great_expectations as gx
import pandas as pd
import os

# Create context with platform-appropriate paths
if os.path.exists(os.path.expanduser("~/modern-data-stack")):
    base_dir = os.path.expanduser("~/modern-data-stack")
else:
    base_dir = "."

context = gx.get_context(project_root_dir=base_dir)

# Add data source
datasource = context.sources.add_pandas("crypto_data")

# Create expectations
crypto_asset = datasource.add_dataframe_asset(name="crypto_prices")

# Define expectations suite
suite = context.add_expectation_suite("crypto_validation")

# Add expectations
expectations = [
    crypto_asset.expect_column_values_to_not_be_null("coin"),
    crypto_asset.expect_column_values_to_not_be_null("price"),
    crypto_asset.expect_column_values_to_be_between("price", min_value=0),
    crypto_asset.expect_column_values_to_be_in_set("coin", ["bitcoin", "ethereum", "cardano", "polkadot"])
]

# Run validation
df = pd.read_parquet("data/crypto_summary.parquet")
validation_result = crypto_asset.validate(df, expectation_suite=suite)
print(validation_result)
