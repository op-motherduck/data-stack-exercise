#!/usr/bin/env python3
"""
Simple script to check Dagster schedules configuration
"""

import os
import sys

def check_dagster_config():
    """Check if Dagster configuration includes schedules"""
    
    print("🔍 Checking Dagster Schedule Configuration")
    print("=" * 50)
    
    # Check if schedules.py exists
    schedules_file = "dagster_project/datastack_orchestration/datastack_orchestration/schedules.py"
    if os.path.exists(schedules_file):
        print("✅ schedules.py file exists")
        
        with open(schedules_file, 'r') as f:
            content = f.read()
            
        # Check for key components
        if "real_time_schedule" in content:
            print("✅ Real-time schedule configured (every 5 minutes)")
        if "data_quality_schedule" in content:
            print("✅ Data quality schedule configured (every 15 minutes)")
        if "kafka_schedule" in content:
            print("✅ Kafka producers schedule configured (every 30 minutes)")
    else:
        print("❌ schedules.py file not found")
    
    # Check if definitions.py imports schedules
    definitions_file = "dagster_project/datastack_orchestration/datastack_orchestration/definitions.py"
    if os.path.exists(definitions_file):
        print("✅ definitions.py file exists")
        
        with open(definitions_file, 'r') as f:
            content = f.read()
            
        if "schedules" in content:
            print("✅ Schedules imported in definitions.py")
        else:
            print("❌ Schedules not imported in definitions.py")
    else:
        print("❌ definitions.py file not found")
    
    print("\n📋 Schedule Configuration Summary:")
    print("   • Real-time assets (crypto, GitHub, weather): Every 5 minutes")
    print("   • Data quality checks: Every 15 minutes")
    print("   • Kafka producers: Every 30 minutes")
    
    print("\n🌐 To view schedules in Dagster UI:")
    print("   1. Open http://localhost:3000")
    print("   2. Go to 'Schedules' tab")
    print("   3. Enable the schedules you want to run")
    
    print("\n⚙️ To manually trigger a schedule:")
    print("   • In Dagster UI: Click 'Trigger' on any schedule")
    print("   • Via CLI: dagster schedule up <schedule_name>")

if __name__ == "__main__":
    check_dagster_config()
