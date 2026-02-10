import os
import time
import itertools
import threading
from datetime import datetime

from dotenv import load_dotenv

from weather import get_coords_from_zipcode, get_current_weather
from producer import create_kafka_producer
from consumer import consume_weather_data

load_dotenv()

API_KEY = os.getenv("API_KEY")
ZIPCODE = os.getenv("ZIPCODE")
ZIPCODE_2 = os.getenv("ZIPCODE_2")
COUNTRY_CODE = os.getenv("COUNTRY_CODE")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")


def resolve_zipcodes():
    """Resolve coordinates for all configured zipcodes."""
    zipcodes = [ZIPCODE]
    if ZIPCODE_2:
        zipcodes.append(ZIPCODE_2)

    locations = []
    for zc in zipcodes:
        lat, lon = get_coords_from_zipcode(zc, COUNTRY_CODE, API_KEY)
        print(f"Resolved {zc} -> ({lat}, {lon})")
        locations.append((zc, lat, lon))

    return locations


def stream_weather_data():
    """Stream weather data to Kafka, round-robin across configured zipcodes."""
    locations = resolve_zipcodes()
    print(f"Streaming weather data for {len(locations)} location(s)")

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

    error_count = 0
    max_errors = 5

    try:
        for zipcode, lat, lon in itertools.cycle(locations):
            weather_data = get_current_weather(lat, lon, API_KEY)

            if weather_data is None:
                error_count += 1
                print(f"Failed to fetch weather data for {zipcode}. Error count: {error_count}/{max_errors}")

                if error_count >= max_errors:
                    print("Too many consecutive errors. Stopping...")
                    break

                time.sleep(5)
                continue

            error_count = 0

            weather_data['zipcode'] = zipcode
            weather_data['stream_timestamp'] = datetime.now().isoformat()

            producer.send(
                KAFKA_TOPIC,
                key=zipcode,
                value=weather_data
            )

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent weather data for {zipcode} to Kafka")
            print(f"  Temperature: {weather_data.get('main', {}).get('temp')} F")
            print(f"  Conditions: {weather_data.get('weather', [{}])[0].get('description')}")

            time.sleep(3)

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
