# weather-stream

A real-time weather data streaming pipeline using Apache Kafka. Fetches current weather data from the OpenWeatherMap API, streams it through Kafka, and persists it to a SQLite database.

## Architecture

```
OpenWeatherMap API → Producer → Kafka → Consumer → SQLite
```

- **weather.py** - HTTP client for the OpenWeatherMap API (geocoding + current weather)
- **producer.py** - Kafka producer that serializes weather data to a topic
- **consumer.py** - Kafka consumer that reads from the topic and writes to SQLite
- **main.py** - Entry point that runs the producer and consumer together

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [Docker](https://www.docker.com/)

## Setup

1. Clone the repo and install dependencies:

   ```sh
   uv sync
   ```

2. Start Kafka:

   ```sh
   docker compose up -d
   ```

3. Create a `.env` file from the example and add your API key and at least one US zipcode:

   ```sh
   cp .env.example .env
   ```

   `ZIPCODE` is required. `ZIPCODE_2` is optional — set it to compare weather data between two locations.

## Usage

Run the pipeline:

```sh
uv run main.py
```

This starts both the producer and consumer in a single process. The producer round-robins through configured zipcodes, making one API call every 3 seconds. With two zipcodes, each location is polled roughly every 6 seconds (~20 calls/min), staying within the OpenWeatherMap free tier (60 calls/min, 1,000 calls/day). The consumer reads from Kafka and stores each reading in `weather_data.db`.

If only `ZIPCODE` is set, the pipeline runs in single-location mode.
