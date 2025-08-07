import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_github_events():
    """Stream GitHub public events"""
    url = "https://api.github.com/events"
    headers = {'Accept': 'application/vnd.github.v3+json'}
    
    last_event_id = None
    
    while True:
        try:
            response = requests.get(url, headers=headers)
            events = response.json()
            
            for event in events:
                if event['id'] != last_event_id:
                    # Extract relevant fields
                    github_event = {
                        'id': event['id'],
                        'type': event['type'],
                        'actor': event['actor']['login'],
                        'repo': event['repo']['name'],
                        'created_at': event['created_at'],
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    
                    producer.send('github_events', value=github_event)
                    print(f"Sent GitHub event: {event['type']} by {event['actor']['login']}")
            
            if events:
                last_event_id = events[0]['id']
            
            # GitHub rate limit friendly
            time.sleep(10)
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    stream_github_events()
