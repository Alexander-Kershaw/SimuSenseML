import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from src.config import KAFKA_BROKER, KAFKA_TOPIC


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_normal_reading(device_id: str) -> dict:
    """
    Normal operating ranges for simulated sensors:
      - temperature: 20–30 C (small noise)
      - vibration: 0.05–0.25 (small noise)
      - pressure: 0.95–1.05 bar (small noise)
    """
    # Generate normal readings with some noise

    temperature = random.normalvariate(25.0, 1.2) # mean 25C, sd 1.2
    vibration = max(0.0, random.normalvariate(0.12, 0.03)) # mean 0.12, sd 0.03
    pressure = random.normalvariate(1.00, 0.02) # mean 1.00 bar, sd 0.02

    # Return the sensor reading message

    return {
        "device_id": device_id, # unique device identifier
        "timestamp": now_utc_iso(), # ISO format with timezone
        "temperature": round(temperature, 3), # Celsius
        "vibration": round(vibration, 3), # arbitrary units
        "pressure": round(pressure, 3), # bar
        "label": "normal"  # for debugging purposes
    }

# Function to inject anomalies into sensor readings

def inject_anomaly(msg: dict) -> dict:
    """
    Create anomalies by spiking one variable significantly
    """
    msg = dict(msg)  # copy to avoid mutating original
    anomaly_type = random.choice(["temp_spike", "vibration_spike", "pressure_drop"]) # type of anomaly category

    # Inject the anomaly based on the selected type

    if anomaly_type == "temp_spike":
        msg["temperature"] = round(msg["temperature"] + random.uniform(10, 25), 3)
    elif anomaly_type == "vibration_spike":
        msg["vibration"] = round(msg["vibration"] + random.uniform(0.5, 2.0), 3)
    elif anomaly_type == "pressure_drop":
        msg["pressure"] = round(msg["pressure"] - random.uniform(0.2, 0.5), 3)

    msg["label"] = f"anomaly:{anomaly_type}"
    return msg

# Main simulator function

def run_simulator(num_devices: int = 5, anomaly_rate: float = 0.10):
    """
    anomaly_rate: probability any given message becomes anomalous 
    """
    device_ids = [f"sensor-{i:03d}" for i in range(1, num_devices + 1)] # e.g. sensor-001, sensor-002, ...

    # Set up Kafka producer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=10,
    )

    print(f"Producing to Kafka topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}")
    print(f"Devices: {device_ids}")
    print(f"Anomaly rate: {anomaly_rate*100:.1f}%")
    print("Press Ctrl+C to stop.\n")

    # streaming simulated data

    try:
        while True:
            device_id = random.choice(device_ids)
            msg = generate_normal_reading(device_id)

            if random.random() < anomaly_rate:
                msg = inject_anomaly(msg)

            # Send the message to Kafka

            producer.send(KAFKA_TOPIC, key=device_id, value=msg)

            print(msg)

            # sleep a bit to simulate real streams
            
            time.sleep(random.uniform(0.3, 1.2))

    # Handle shutdown

    except KeyboardInterrupt:
        print("\nStopping simulator...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_simulator(num_devices=5, anomaly_rate=0.10)
