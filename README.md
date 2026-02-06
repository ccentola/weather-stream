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

3. Create a `.env` file from the example and enter the US zipcode of your choice.

## Usage

Run the pipeline:

```sh
uv run main.py
```

This starts both the producer and consumer in a single process. The producer fetches weather data every 10 seconds and publishes it to Kafka. The consumer reads from Kafka and stores each reading in `weather_data.db`.
