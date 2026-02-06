import os
import sqlite3
import json
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
DB_NAME = "weather_data.db"


def create_database():
    """Create SQLite database and weather table if they don't exist"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            stream_timestamp TEXT,
            location_name TEXT,
            zipcode TEXT,
            latitude REAL,
            longitude REAL,
            temperature REAL,
            feels_like REAL,
            temp_min REAL,
            temp_max REAL,
            pressure INTEGER,
            humidity INTEGER,
            weather_main TEXT,
            weather_description TEXT,
            wind_speed REAL,
            wind_deg INTEGER,
            clouds INTEGER,
            visibility INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_timestamp
        ON weather_readings(timestamp)
    ''')

    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' created/verified successfully")


def insert_weather_data(weather_data):
    """Insert weather data into SQLite database"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO weather_readings (
                timestamp, stream_timestamp, location_name, zipcode,
                latitude, longitude, temperature, feels_like,
                temp_min, temp_max, pressure, humidity,
                weather_main, weather_description,
                wind_speed, wind_deg, clouds, visibility
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.fromtimestamp(weather_data.get('dt')).isoformat(),
            weather_data.get('stream_timestamp'),
            weather_data.get('name'),
            weather_data.get('zipcode', 'N/A'),
            weather_data.get('coord', {}).get('lat'),
            weather_data.get('coord', {}).get('lon'),
            weather_data.get('main', {}).get('temp'),
            weather_data.get('main', {}).get('feels_like'),
            weather_data.get('main', {}).get('temp_min'),
            weather_data.get('main', {}).get('temp_max'),
            weather_data.get('main', {}).get('pressure'),
            weather_data.get('main', {}).get('humidity'),
            weather_data.get('weather', [{}])[0].get('main'),
            weather_data.get('weather', [{}])[0].get('description'),
            weather_data.get('wind', {}).get('speed'),
            weather_data.get('wind', {}).get('deg'),
            weather_data.get('clouds', {}).get('all'),
            weather_data.get('visibility')
        ))

        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def consume_weather_data():
    """Consume weather data from Kafka and store in SQLite"""
    create_database()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"Listening for weather data on topic '{KAFKA_TOPIC}'...")
    print(f"Storing data in '{DB_NAME}'")
    print("-" * 60)

    try:
        for message in consumer:
            weather_data = message.value

            record_id = insert_weather_data(weather_data)

            if record_id:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Saved to DB (ID: {record_id})")
                print(f"  Location: {weather_data.get('name')}")
                print(f"  Temperature: {weather_data.get('main', {}).get('temp')}°F")
                print(f"  Conditions: {weather_data.get('weather', [{}])[0].get('description')}")
                print(f"  Humidity: {weather_data.get('main', {}).get('humidity')}%")
                print("-" * 60)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    consume_weather_data()
