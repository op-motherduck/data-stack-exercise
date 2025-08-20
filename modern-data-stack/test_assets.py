#!/usr/bin/env python3
"""
Test script to verify Dagster assets can run successfully
"""

import sys
import os
sys.path.append('dagster_project/datastack_orchestration')

from datastack_orchestration.assets import crypto_price_summary, developer_insights, data_quality_checks
from dagster import build_asset_context

def test_assets():
    """Test each asset individually"""
    
    print("Testing crypto_price_summary asset...")
    try:
        context = build_asset_context()
        result = crypto_price_summary(context)
        print(f"✅ crypto_price_summary: {result.metadata}")
    except Exception as e:
        print(f"❌ crypto_price_summary failed: {e}")
    
    print("\nTesting developer_insights asset...")
    try:
        context = build_asset_context()
        result = developer_insights(context)
        print(f"✅ developer_insights: {result.metadata}")
    except Exception as e:
        print(f"❌ developer_insights failed: {e}")
    
    print("\nTesting data_quality_checks asset...")
    try:
        context = build_asset_context()
        result = data_quality_checks(context)
        print(f"✅ data_quality_checks: {result}")
    except Exception as e:
        print(f"❌ data_quality_checks failed: {e}")

if __name__ == "__main__":
    test_assets()
