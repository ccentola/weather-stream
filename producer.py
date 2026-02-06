import json
from kafka import KafkaProducer


def create_kafka_producer(bootstrap_servers):
    """Create and return a Kafka producer instance"""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
