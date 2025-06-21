from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import time
import datetime
from datetime import timedelta

# Replace with your actual Kafka and Schema Registry details
kafka_config = {
    "bootstrap.servers": "<BOOTSTRAP_SERVER>",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "<KAFKA_USERNAME>",
    "sasl.password": "<KAFKA_PASSWORD>",
    "group.id": "<GROUP_ID>",
    "client.id": "<CLIENT_ID>",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "fetch.max.bytes": 52428800,
    "max.partition.fetch.bytes": 31457280,
    "fetch.wait.max.ms": 1000,
    "fetch.min.bytes": 100000,
}

schema_registry_config = {
    "url": "<SCHEMA_REGISTRY_URL>",
    "basic.auth.user.info": "<SCHEMA_REGISTRY_USER>:<SCHEMA_REGISTRY_PASSWORD>"
}

# Topic and schema subject
topic_name = "<TOPIC_NAME>"
schema_subject = "<SCHEMA_SUBJECT_NAME>"

# Initialize clients
schema_registry_client = SchemaRegistryClient(schema_registry_config)
schema = schema_registry_client.get_latest_version(schema_subject).schema.schema_str
deserializer = AvroDeserializer(schema_registry_client=schema_registry_client, schema_str=schema)

consumer = Consumer(kafka_config)
consumer.subscribe([topic_name])

print(f"Started consuming from topic: {topic_name}")

try:
    while True:
        messages = consumer.consume(num_messages=10, timeout=5.0)
        for msg in messages:
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record_value = msg.value()
            if record_value:
                deserialized_data = deserializer(record_value, None)
                print(f"Offset: {msg.offset()}, Partition: {msg.partition()}")
                print(f"Deserialized Message:\n{deserialized_data}\n")

except KeyboardInterrupt:
    print("Consumption interrupted by user.")

finally:
    consumer.close()
