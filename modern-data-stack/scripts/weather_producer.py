import requests
import json
from kafka import KafkaProducer
from datetime import datetime
import time

# Get a free API key from: https://openweathermap.org/api
API_KEY = "your_openweather_api_key"  # Replace with your key
CITIES = ["London", "New York", "Tokyo", "Sydney", "Mumbai"]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather():
    """Fetch weather data for multiple cities"""
    for city in CITIES:
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url)
            data = response.json()
            
            weather_event = {
                'city': city,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'weather': data['weather'][0]['main'],
                'description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'timestamp': datetime.utcnow().isoformat()
            }
            
            producer.send('weather_data', value=weather_event)
            print(f"Sent weather data for {city}: {data['main']['temp']}°C")
            
        except Exception as e:
            print(f"Error fetching weather for {city}: {e}")
        
        time.sleep(1)  # Be nice to the API

if __name__ == "__main__":
    while True:
        fetch_weather()
        time.sleep(300)  # Fetch every 5 minutes
