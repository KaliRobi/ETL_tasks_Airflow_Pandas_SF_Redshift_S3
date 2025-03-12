import json
import time
import requests
from confluent_kafka import Producer

# Kafka Config
KAFKA_BROKER = "localhost:9092"  
KAFKA_TOPIC = "Viljandi_weather_stream"

# API Config (Modify as needed)
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=58.3639&longitude=25.59&hourly=cloud_cover,wind_speed_10m&forecast_days=1"  

# Kafka Producer Configuration
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "Viljandi_weather_stream_producer"
}
producer = Producer(producer_config)

def fetch_api_data():
        
    try:
        response = requests.get(API_URL,  timeout=10)
        response.raise_for_status()
        return response.json()  
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return None

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def stream_to_kafka():
    try:
        while True:
            data = fetch_api_data()
            if data:
                json_data = json.dumps(data)
                producer.produce(KAFKA_TOPIC, key="event", value=json_data, callback=delivery_report)
                producer.poll(0)  
            time.sleep(3600)  
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting

if __name__ == "__main__":
    stream_to_kafka()
