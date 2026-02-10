import os
import sqlite3
import tempfile
from unittest.mock import patch
from datetime import datetime

import consumer


def make_weather_data(zipcode="01701", temp=72.5):
    return {
        "dt": int(datetime(2025, 1, 15, 12, 0, 0).timestamp()),
        "stream_timestamp": "2025-01-15T12:00:01",
        "name": "TestCity",
        "zipcode": zipcode,
        "coord": {"lat": 42.3, "lon": -71.8},
        "main": {
            "temp": temp,
            "feels_like": 70.0,
            "temp_min": 68.0,
            "temp_max": 75.0,
            "pressure": 1013,
            "humidity": 45,
        },
        "weather": [{"main": "Clear", "description": "clear sky"}],
        "wind": {"speed": 5.0, "deg": 180},
        "clouds": {"all": 10},
        "visibility": 10000,
    }


class TestCreateDatabase:
    def test_creates_table_and_indexes(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with patch.object(consumer, "DB_NAME", db_path):
            consumer.create_database()

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='weather_readings'"
        )
        assert cursor.fetchone() is not None

        # Both indexes exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        )
        indexes = {row[0] for row in cursor.fetchall()}
        assert "idx_timestamp" in indexes
        assert "idx_zipcode_timestamp" in indexes

        conn.close()


class TestInsertWeatherData:
    def test_insert_and_retrieve(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with patch.object(consumer, "DB_NAME", db_path):
            consumer.create_database()
            data = make_weather_data()
            row_id = consumer.insert_weather_data(data)

        assert row_id is not None

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT zipcode, temperature, location_name FROM weather_readings WHERE id = ?", (row_id,))
        row = cursor.fetchone()
        conn.close()

        assert row[0] == "01701"
        assert row[1] == 72.5
        assert row[2] == "TestCity"

    def test_insert_both_zipcodes(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with patch.object(consumer, "DB_NAME", db_path):
            consumer.create_database()
            consumer.insert_weather_data(make_weather_data(zipcode="01701", temp=72.5))
            consumer.insert_weather_data(make_weather_data(zipcode="10001", temp=55.0))
            consumer.insert_weather_data(make_weather_data(zipcode="01701", temp=73.0))

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT zipcode, COUNT(*), AVG(temperature) FROM weather_readings GROUP BY zipcode ORDER BY zipcode"
        )
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 2
        assert rows[0] == ("01701", 2, 72.75)
        assert rows[1] == ("10001", 1, 55.0)
