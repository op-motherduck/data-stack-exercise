#!/usr/bin/env python3
# =============================================================================
# COINCHECK API CONNECTION TEST
# =============================================================================
# Purpose: Tests connectivity and functionality of the Coincheck REST API:
#   - Verifies API endpoint accessibility and response format
#   - Tests multiple cryptocurrency trading pairs
#   - Validates data structure and content for debugging
#   - Provides detailed error reporting for connection issues
#   - Helps diagnose problems before running the full crypto producer
# 
# This script is used for troubleshooting and validating the external
# data source before integrating it into the main data pipeline.
# =============================================================================
"""
Test script to verify Coincheck REST API connection.
This helps debug connection issues before running the full producer.
"""

import json
import asyncio
import aiohttp
from datetime import datetime

async def test_coincheck_connection():
    """Test connection to Coincheck REST API"""
    
    url = "https://coincheck.com/api/ticker?pair=btc_jpy"
    
    try:
        print(f"Connecting to {url}...")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    print("✅ Successfully connected to Coincheck REST API")
                    print(f"📨 Response: {json.dumps(data, indent=2)}")
                    
                    # Test multiple pairs
                    pairs = ['btc_jpy', 'eth_jpy', 'xrp_jpy']
                    print(f"\n🧪 Testing multiple pairs: {pairs}")
                    
                    for pair in pairs:
                        pair_url = f"https://coincheck.com/api/ticker?pair={pair}"
                        async with session.get(pair_url) as pair_response:
                            if pair_response.status == 200:
                                pair_data = await pair_response.json()
                                print(f"✅ {pair}: Last price = {pair_data.get('last', 'N/A')}")
                            else:
                                print(f"❌ {pair}: HTTP {pair_response.status}")
                    
                    print(f"\n✅ Test completed successfully!")
                    
                else:
                    print(f"❌ HTTP Error: {response.status}")
                    
    except aiohttp.ClientError as e:
        print(f"❌ Client error: {e}")
    except Exception as e:
        print(f"❌ Connection error: {e}")

if __name__ == "__main__":
    print("🧪 Testing Coincheck REST API connection...")
    asyncio.run(test_coincheck_connection()) 