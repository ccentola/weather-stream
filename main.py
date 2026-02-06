import os
import time
import threading
from datetime import datetime

from dotenv import load_dotenv

from weather import get_coords_from_zipcode, get_current_weather
from producer import create_kafka_producer
from consumer import consume_weather_data

load_dotenv()

API_KEY = os.getenv("API_KEY")
ZIPCODE = os.getenv("ZIPCODE")
COUNTRY_CODE = os.getenv("COUNTRY_CODE")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")


def stream_weather_data():
    """Stream weather data to Kafka"""
    lat, lon = get_coords_from_zipcode(ZIPCODE, COUNTRY_CODE, API_KEY)
    print(f"Streaming weather data for coordinates: {lat}, {lon}")

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

    error_count = 0
    max_errors = 5

    try:
        while True:
            weather_data = get_current_weather(lat, lon, API_KEY)

            if weather_data is None:
                error_count += 1
                print(f"Failed to fetch weather data. Error count: {error_count}/{max_errors}")

                if error_count >= max_errors:
                    print("Too many consecutive errors. Stopping...")
                    break

                time.sleep(5)
                continue

            error_count = 0

            weather_data['stream_timestamp'] = datetime.now().isoformat()

            producer.send(
                KAFKA_TOPIC,
                key=ZIPCODE,
                value=weather_data
            )

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent weather data to Kafka")
            print(f"  Temperature: {weather_data.get('main', {}).get('temp')} F")
            print(f"  Conditions: {weather_data.get('weather', [{}])[0].get('description')}")

            time.sleep(10)

    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_weather_data, daemon=True)
    consumer_thread.start()

    try:
        stream_weather_data()
    except KeyboardInterrupt:
        print("\nShutting down...")
