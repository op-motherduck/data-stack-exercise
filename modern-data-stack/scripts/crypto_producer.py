import json
import asyncio
import websockets
from kafka import KafkaProducer
from datetime import datetime

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def crypto_stream():
    uri = "wss://ws.coincap.io/prices?assets=bitcoin,ethereum,cardano,polkadot"
    
    async with websockets.connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                
                # Add timestamp and send to Kafka
                for coin, price in data.items():
                    event = {
                        'coin': coin,
                        'price': float(price),
                        'timestamp': datetime.utcnow().isoformat(),
                        'source': 'coincap'
                    }
                    
                    producer.send('crypto_prices', value=event)
                    print(f"Sent: {coin} - ${price}")
                    
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(crypto_stream())
