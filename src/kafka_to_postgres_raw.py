import json
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

# Import configuration variables

from src.config import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# Function to parse ISO timestamp strings

def parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str)

# Main function to stream data from Kafka to Postgres

def main():

# Connect to Postgres database    

    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = True # enable auto-commit for streaming inserts

# Create Kafka consumer

    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

# Prepare SQL insert statement

    insert_sql = """
        INSERT INTO sensor_readings_raw
        (device_id, event_ts, temperature, vibration, pressure, payload)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
    """

# Start consuming messages from Kafka and inserting into Postgres

    print("Streaming Kafka -> Postgres (raw). Press Ctrl+C to stop.")
    try:
        with conn.cursor() as cur:
            for msg in consumer:
                payload = msg.value
                device_id = payload["device_id"]
                event_ts = parse_ts(payload["timestamp"])
                temperature = float(payload["temperature"])
                vibration = float(payload["vibration"])
                pressure = float(payload["pressure"])

# Insert the reading into Postgres

                cur.execute(
                    insert_sql,
                    (
                        device_id,
                        event_ts,
                        temperature,
                        vibration,
                        pressure,
                        json.dumps(payload),
                    ),
                )
                print(f"Inserted raw reading: {device_id} @ {event_ts.isoformat()}")

# Handle shutdown

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
