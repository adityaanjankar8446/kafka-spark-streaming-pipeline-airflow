import json
import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="broker:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

ENDPOINTS = {
    "orders":    "https://fakestoreapi.com/carts",
    "customers": "https://fakestoreapi.com/users",
    "products":  "https://fakestoreapi.com/products"
}

for topic, url in ENDPOINTS.items():
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        records = response.json()
        for record in records:
            producer.send(topic, record).get(timeout=30)
        print(f"Published {len(records)} records to topic '{topic}'")
    except Exception as e:
        print(f"Error publishing to {topic}: {e}")

producer.flush()
producer.close()