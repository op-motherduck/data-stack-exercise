#!/usr/bin/env python3
"""
Test script to verify weather_analytics asset can run successfully
"""

import sys
import os
sys.path.append('dagster_project/datastack_orchestration')

from datastack_orchestration.assets import weather_analytics
from dagster import build_asset_context

def test_weather_asset():
    """Test the weather analytics asset"""
    
    print("Testing weather_analytics...")
    try:
        context = build_asset_context()
        result = weather_analytics(context)
        print(f"✅ weather_analytics: {result.metadata}")
    except Exception as e:
        print(f"❌ weather_analytics failed: {e}")

if __name__ == "__main__":
    test_weather_asset()
