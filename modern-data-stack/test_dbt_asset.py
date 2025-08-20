#!/usr/bin/env python3
"""
Test script to verify dbt asset can run successfully
"""

import sys
import os
sys.path.append('dagster_project/datastack_orchestration')

from datastack_orchestration.assets import datastack_dbt_assets
from dagster import build_asset_context

def test_dbt_asset():
    """Test the dbt asset"""
    
    print("Testing datastack_dbt_assets...")
    try:
        context = build_asset_context()
        # This will yield events, so we need to iterate through them
        for event in datastack_dbt_assets(context):
            print(f"✅ dbt event: {event}")
        print("✅ datastack_dbt_assets completed successfully")
    except Exception as e:
        print(f"❌ datastack_dbt_assets failed: {e}")

if __name__ == "__main__":
    test_dbt_asset()
