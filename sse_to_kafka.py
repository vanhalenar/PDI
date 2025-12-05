import sseclient
import requests
from kafka import KafkaProducer
import json
import time
import os

print("Starting Wikimedia SSE to Kafka producer...")

kafka_servers = os.environ.get('KAFKA_SERVERS', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=[kafka_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Connected to Kafka")

headers = {
    'User-Agent': 'Kafka SSE Client/1.0'
}

def connect_and_stream():
    """Connect to SSE stream and handle events"""
    response = requests.get(
        'https://stream.wikimedia.org/v2/stream/recentchange',
        stream=True,
        headers=headers,
        timeout=60
    )
    print(f"Connected to Wikimedia stream, status: {response.status_code}")
    
    client = sseclient.SSEClient(response)
    print("SSE client created, starting to read events...")
    
    for event in client.events():
        if event.data:
            producer.send('sse-topic', {
                'data': event.data,
                'event_type': event.event if event.event else 'message'
            })

# main loop with reconnection
while True:
    try:
        connect_and_stream()
    except (requests.exceptions.ChunkedEncodingError, 
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:
        print(f"Connection error: {e}")
        print("Reconnecting in 5 seconds...")
        time.sleep(5)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        producer.flush()
        producer.close()
        break
    except Exception as e:
        print(f"Unexpected error: {e}")
        print("Reconnecting in 5 seconds...")
        time.sleep(5)