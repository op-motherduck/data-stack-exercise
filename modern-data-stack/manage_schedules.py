#!/usr/bin/env python3
"""
Script to manage and monitor Dagster schedules
"""

import requests
import json
import time
from datetime import datetime

DAGSTER_URL = "http://localhost:3000"

def get_schedules():
    """Get all schedules from Dagster"""
    try:
        response = requests.get(f"{DAGSTER_URL}/graphql", json={
            "query": """
            query {
                schedulesOrError {
                    ... on Schedules {
                        results {
                            name
                            cronSchedule
                            description
                            scheduleState
                            executionInfo {
                                lastExecutionTime
                                lastRunStatus
                            }
                        }
                    }
                }
            }
            """
        })
        return response.json()
    except Exception as e:
        print(f"Error fetching schedules: {e}")
        return None

def get_recent_runs():
    """Get recent runs from Dagster"""
    try:
        response = requests.get(f"{DAGSTER_URL}/graphql", json={
            "query": """
            query {
                runsOrError(limit: 10) {
                    ... on Runs {
                        results {
                            runId
                            pipelineName
                            status
                            startTime
                            endTime
                        }
                    }
                }
            }
            """
        })
        return response.json()
    except Exception as e:
        print(f"Error fetching runs: {e}")
        return None

def print_schedule_status():
    """Print current schedule status"""
    print("🕐 Dagster Schedule Status")
    print("=" * 50)
    
    schedules_data = get_schedules()
    if not schedules_data:
        print("❌ Could not fetch schedules. Is Dagster running?")
        return
    
    schedules = schedules_data.get('data', {}).get('schedulesOrError', {}).get('results', [])
    
    for schedule in schedules:
        name = schedule.get('name', 'Unknown')
        cron = schedule.get('cronSchedule', 'Unknown')
        state = schedule.get('scheduleState', 'Unknown')
        description = schedule.get('description', 'No description')
        
        print(f"\n📅 Schedule: {name}")
        print(f"   Cron: {cron}")
        print(f"   State: {state}")
        print(f"   Description: {description}")
        
        # Get last execution info
        exec_info = schedule.get('executionInfo', {})
        if exec_info:
            last_time = exec_info.get('lastExecutionTime')
            last_status = exec_info.get('lastRunStatus')
            if last_time:
                print(f"   Last Run: {last_time}")
                print(f"   Status: {last_status}")

def print_recent_runs():
    """Print recent runs"""
    print("\n🔄 Recent Runs")
    print("=" * 30)
    
    runs_data = get_recent_runs()
    if not runs_data:
        print("❌ Could not fetch runs")
        return
    
    runs = runs_data.get('data', {}).get('runsOrError', {}).get('results', [])
    
    for run in runs[:5]:  # Show last 5 runs
        run_id = run.get('runId', 'Unknown')[:8]  # Short ID
        pipeline = run.get('pipelineName', 'Unknown')
        status = run.get('status', 'Unknown')
        start_time = run.get('startTime', 'Unknown')
        
        # Format status with emoji
        status_emoji = {
            'SUCCESS': '✅',
            'FAILURE': '❌',
            'STARTED': '🔄',
            'CANCELED': '⏹️'
        }.get(status, '❓')
        
        print(f"{status_emoji} {pipeline} ({run_id}) - {status}")
        if start_time != 'Unknown':
            print(f"   Started: {start_time}")

def main():
    """Main function"""
    print(f"🚀 Dagster Schedule Manager - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check if Dagster is running
    try:
        response = requests.get(f"{DAGSTER_URL}/graphql", timeout=5)
        if response.status_code == 200:
            print("✅ Dagster is running")
        else:
            print("❌ Dagster is not responding properly")
            return
    except:
        print("❌ Dagster is not running. Start it with: dagster dev")
        return
    
    print_schedule_status()
    print_recent_runs()
    
    print(f"\n📊 Schedule Summary:")
    print("   • Real-time assets: Every 5 minutes")
    print("   • Data quality checks: Every 15 minutes") 
    print("   • Kafka producers: Every 30 minutes")
    print(f"\n🌐 Access Dagster UI: {DAGSTER_URL}")

if __name__ == "__main__":
    main()
