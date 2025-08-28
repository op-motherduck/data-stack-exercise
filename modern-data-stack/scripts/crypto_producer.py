#!/usr/bin/env python3
# =============================================================================
# CRYPTOCURRENCY DATA PRODUCER
# =============================================================================
# Purpose: Streams real-time cryptocurrency price data from Coincheck API to Kafka:
#   - Fetches ticker data for multiple trading pairs (BTC, ETH, XRP, etc.)
#   - Publishes structured events to Kafka topic 'crypto_prices'
#   - Includes price, volume, bid/ask, and timestamp information
#   - Implements error handling and reconnection logic
#   - Respects API rate limits with appropriate delays
# 
# This producer feeds the real-time cryptocurrency analytics pipeline
# and provides data for price trend analysis and monitoring.
# =============================================================================
"""
Crypto data producer using Coincheck REST API.
Streams cryptocurrency ticker data to Kafka.
"""

import json
import asyncio
import aiohttp
from kafka import KafkaProducer
from datetime import datetime
import time

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def fetch_ticker_data(session, pair='btc_jpy'):
    """Fetch ticker data from Coincheck REST API"""
    url = f"https://coincheck.com/api/ticker?pair={pair}"
    
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Error fetching ticker: HTTP {response.status}")
                return None
    except Exception as e:
        print(f"Error fetching ticker data: {e}")
        return None

async def crypto_stream():
    """Stream crypto data from Coincheck REST API"""
    
    # Supported trading pairs
    pairs = ['btc_jpy', 'eth_jpy', 'etc_jpy', 'xrp_jpy', 'xem_jpy', 'ltc_jpy', 'bch_jpy', 'mona_jpy']
    
    async with aiohttp.ClientSession() as session:
        print("Connected to Coincheck REST API")
        
        while True:
            try:
                for pair in pairs:
                    ticker_data = await fetch_ticker_data(session, pair)
                    
                    if ticker_data:
                        # Create event with ticker data
                        event = {
                            'type': 'ticker',
                            'pair': pair,
                            'last': float(ticker_data.get('last', 0)),
                            'bid': float(ticker_data.get('bid', 0)),
                            'ask': float(ticker_data.get('ask', 0)),
                            'high': float(ticker_data.get('high', 0)),
                            'low': float(ticker_data.get('low', 0)),
                            'volume': float(ticker_data.get('volume', 0)),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'coincheck'
                        }
                        
                        producer.send('crypto_prices', value=event)
                        print(f"Ticker: {pair} - Last: {event['last']}, Bid: {event['bid']}, Ask: {event['ask']}")
                    
                    # Small delay between requests to be respectful to the API
                    await asyncio.sleep(0.5)
                
                # Wait before next polling cycle
                print("Completed polling cycle, waiting 30 seconds...")
                await asyncio.sleep(30)
                    
            except Exception as e:
                print(f"Error in crypto stream: {e}")
                await asyncio.sleep(10)

async def main():
    """Main function with reconnection logic"""
    while True:
        try:
            await crypto_stream()
        except Exception as e:
            print(f"Connection error: {e}")
            print("Reconnecting in 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    print("Starting Coincheck crypto producer...")
    asyncio.run(main())
