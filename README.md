
--------------------------------------------------------------------------------------------------------------------
SimuSenseML — Real-Time IoT Meteorological Data Anomaly Detection Pipeline
--------------------------------------------------------------------------------------------------------------------

SimuSenseML is a real-time end-to-end machine learning and data engineering project that simulates N number of simulated meteorological data, processing it via a data streaming pipline, and performing anomaly detection using unsupervised machine learning (K-Means clustering).

This data engineering intensive project demonstrates the harmonious integration of modern data systems working synergenistically: Kafka -> Spark Structured Streaming -> PostgreSQL -> Streamlit Live Dashboard.

--------------------------------------------------------------------------------------------------------------------
PROJECT OVERVIEW
--------------------------------------------------------------------------------------------------------------------

- simulates real time weather sensor data (temperature, vibration, atmospheric pressure)
- streams data through Apache Kafka
- processes data in real time using Apache Spark Structures Streaming
- leverages unsupervised machine learning (K-Means clustering) for anomalous weather data detection
- stores raw and scored data in PostgreSQL
- Basic visualization of anomalies using Streamlit

--------------------------------------------------------------------------------------------------------------------
ARCHITECTURE
--------------------------------------------------------------------------------------------------------------------

┌──────────────────┐
│ Sensor Simulator │  (Python)
└─────────┬────────┘
          │
          ▼
┌──────────────────┐
│      Kafka       │  (Event Stream)
└─────────┬────────┘
          │
          ▼
┌──────────────────────────────┐
│ Spark Structured Streaming   │
│  - JSON parsing              │
│  - Feature scaling           │
│  - K-Means clustering        │
│  - Anomaly scoring           │
└─────────┬────────────────────┘
          │
          ▼
┌──────────────────┐
│   PostgreSQL     │
│  - raw readings  │
│  - scored events │
└─────────┬────────┘
          │
          ▼
┌──────────────────┐
│   Streamlit      │
│  Live Dashboard  │
└──────────────────┘

--------------------------------------------------------------------------------------------------------------------
TECH STACK
--------------------------------------------------------------------------------------------------------------------

- Python 3.11
- Docker & Docker Compose
- Apache Kafka + Zookeeper
- Apache Spark (PySpark, Structured Streaming)
- PostgreSQL
- scikit-learn (K-Means)
- NumPy / Pandas
- Streamlit (dashboard)

--------------------------------------------------------------------------------------------------------------------
PROJECT DIRECTORY STRUCTURE
--------------------------------------------------------------------------------------------------------------------

SimuSenseML/
├── docker-compose.yml
├── checkpoints/
│   ├── kafka_to_pg_raw/
│   └── kmeans_score/
├── models/
│   ├── kmeans.joblib
│   ├── scaler.joblib
│   └── threshold.npy
├── src/
│   ├── config.py
│   ├── sensor_simulator.py
│   ├── kafka_to_postgres_raw.py        # (optional, Python consumer)
│   ├── spark_kafka_to_postgres_raw.py  # Spark raw ingestion
│   ├── train_kmeans.py
│   ├── compute_threshold.py
│   ├── spark_score_kmeans.py
│   └── dashboard.py
└── README.md

--------------------------------------------------------------------------------------------------------------------
DATA FLOW
--------------------------------------------------------------------------------------------------------------------

1. SENSOR SIMULATION

- Generates synthetic weather sensor data
- Injects configurable anomalous data points
- Publishes JSON events to Kafka

--------------------------------------------------------------------------------------------------------------------

2. KAFKA

- Buffers and distributes streaming events
- Topic: iot_sensors

--------------------------------------------------------------------------------------------------------------------

3. SPARK STRUCTURED STREAMING

- Ingests Kafka events
- Parses JSON and extracts features
- Standardizes features using a trained scaler
- Assignss K-Means clusters
- Computes distance to nearest centroid
- Individualizes anomalous data using a learned threshold

--------------------------------------------------------------------------------------------------------------------

4. POSTGRESQL

- sensor_readings_raw : raw ingested data
- sensor_readings_scored : ML-scored events and anomalous data flags

--------------------------------------------------------------------------------------------------------------------

5. STREAMLIT DASHBOARD

- Live Metrics (row counts, anomaly rate)
- Latest anomalies
- Device Specific anomaly trends
- Distance-to-centroid visualization

--------------------------------------------------------------------------------------------------------------------
MACHINE LEARNING APPROACH
--------------------------------------------------------------------------------------------------------------------

- Model : K-Means clustering (unsupervised learning)
- Features : temperature, vibration, pressure
- Preprocessing : StandardScaler (Z-Score normalization)
- Anomaly Detection : Distance to nearest cluster centroid, threshold computed from training distribution (e.g. 99.5th percentile)

--------------------------------------------------------------------------------------------------------------------
RUN INSTRUCTIONS (DEMO)
--------------------------------------------------------------------------------------------------------------------

1. Start docker infrastructure

docker compose up -d

--------------------------------------------------------------------------------------------------------------------

2. Start Sensor Simulator

python -m src.sensor_simulator

--------------------------------------------------------------------------------------------------------------------

3. Execute model training

python -m src.train_kmeans
python -m src.compute_threshold

--------------------------------------------------------------------------------------------------------------------

4. Stream anomaly detection 

rm -rf checkpoints/kmeans_score
python -m src.spark_score_kmeans

rm -rf checkpoints/kmeans_score serves to 'reset' tracking of Kafka offsets and state

--------------------------------------------------------------------------------------------------------------------

5. Run dashboard (Open at http://localhost:8501)

streamlit run src/dashboard.py

--------------------------------------------------------------------------------------------------------------------

