#!/usr/bin/env python3
# =============================================================================
# DAGSTER ASSETS TESTING UTILITY
# =============================================================================
# Purpose: Comprehensive testing framework for all Dagster assets:
#   - Tests individual assets in isolation for debugging
#   - Validates asset functionality and data processing logic
#   - Provides detailed error reporting and success metrics
#   - Supports testing specific assets or all assets together
#   - Ensures data pipeline reliability and correctness
# 
# This script is used for development, debugging, and validation
# of the data processing assets before deployment.
# =============================================================================
"""
Comprehensive test script for all Dagster assets
"""

import sys
import os
sys.path.append('dagster_project/datastack_orchestration')

from datastack_orchestration.assets import (
    crypto_price_summary, 
    developer_insights, 
    data_quality_checks,
    weather_analytics
)
from dagster import build_asset_context

def test_all_assets():
    """Test all assets comprehensively"""
    
    print("🧪 Testing All Dagster Assets")
    print("=" * 50)
    
    assets_to_test = [
        ("crypto_price_summary", crypto_price_summary),
        ("developer_insights", developer_insights), 
        ("weather_analytics", weather_analytics),
        ("data_quality_checks", data_quality_checks)
    ]
    
    results = {}
    
    for asset_name, asset_func in assets_to_test:
        print(f"\n🔍 Testing {asset_name}...")
        try:
            context = build_asset_context()
            result = asset_func(context)
            
            if hasattr(result, 'metadata'):
                print(f"✅ {asset_name}: {result.metadata}")
            else:
                print(f"✅ {asset_name}: {result}")
            
            results[asset_name] = "SUCCESS"
            
        except Exception as e:
            print(f"❌ {asset_name} failed: {e}")
            results[asset_name] = f"FAILED: {e}"
    
    # Summary
    print(f"\n📊 Test Summary:")
    print("=" * 30)
    success_count = sum(1 for result in results.values() if result == "SUCCESS")
    total_count = len(results)
    
    for asset_name, result in results.items():
        status = "✅" if result == "SUCCESS" else "❌"
        print(f"{status} {asset_name}: {result}")
    
    print(f"\n🎯 Overall: {success_count}/{total_count} assets working")
    
    if success_count == total_count:
        print("🎉 All assets are working correctly!")
    else:
        print("⚠️  Some assets need attention")

def test_specific_asset(asset_name):
    """Test a specific asset"""
    asset_map = {
        "crypto": crypto_price_summary,
        "developer": developer_insights,
        "weather": weather_analytics,
        "quality": data_quality_checks
    }
    
    if asset_name not in asset_map:
        print(f"❌ Unknown asset: {asset_name}")
        print(f"Available assets: {list(asset_map.keys())}")
        return
    
    print(f"🧪 Testing {asset_name} asset...")
    try:
        context = build_asset_context()
        result = asset_map[asset_name](context)
        
        if hasattr(result, 'metadata'):
            print(f"✅ {asset_name}: {result.metadata}")
        else:
            print(f"✅ {asset_name}: {result}")
            
    except Exception as e:
        print(f"❌ {asset_name} failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Test specific asset
        test_specific_asset(sys.argv[1])
    else:
        # Test all assets
        test_all_assets()
