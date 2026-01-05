import os
import numpy as np
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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

# Paths to the saved model and scaler

MODEL_PATH = "models/kmeans.joblib"
SCALER_PATH = "models/scaler.joblib"

# Define the schema for incoming sensor data

sensor_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("vibration", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("label", StringType(), True),
])

# Main function to score incoming data using KMeans model

def main():
    if not os.path.exists(MODEL_PATH) or not os.path.exists(SCALER_PATH):
        raise RuntimeError("Train first: python -m src.train_kmeans")

# Load the trained KMeans model and scaler

    kmeans = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    centers = kmeans.cluster_centers_  # scaled space

# Initialize Spark session

    spark = (
        SparkSession.builder
        .appName("IoT-Score-KMeans")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.postgresql:postgresql:42.7.3",
            ])
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

# Broadcast model parameters to worker nodes

    bc_centers = spark.sparkContext.broadcast(centers)
    bc_mean = spark.sparkContext.broadcast(scaler.mean_)
    bc_scale = spark.sparkContext.broadcast(scaler.scale_)

# Read streaming data from Kafka

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

# Parse the JSON messages and extract fields   

    parsed = (
        kafka_df
        .select(col("value").cast("string").alias("json_str"))
        .select(from_json(col("json_str"), sensor_schema).alias("data"))
        .select(
            col("data.device_id").alias("device_id"),
            to_timestamp(col("data.timestamp")).alias("event_ts"),
            col("data.temperature").alias("temperature"),
            col("data.vibration").alias("vibration"),
            col("data.pressure").alias("pressure"),
        )
    )

# Define JDBC URL for Postgres    

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Function to score each micro-batch

    def score_batch(batch_df, batch_id: int):

        print(f"\n=== score_batch called: batch_id={batch_id} ===")
        print(f"rows in batch_df = {batch_df.count()}")

# Skip empty batches

        if batch_df.rdd.isEmpty():
            return

# Collect batch data to driver for scoring

        pdf = batch_df.toPandas()
        X = pdf[["temperature", "vibration", "pressure"]].to_numpy(dtype=float)

# Standardize using saved scaler parameters
        
        Xs = (X - bc_mean.value) / bc_scale.value

# Compute distances to cluster centers and assign cluster IDs

        centers_local = bc_centers.value
        dists = np.linalg.norm(Xs[:, None, :] - centers_local[None, :, :], axis=2)
        cluster_id = np.argmin(dists, axis=1)
        dist_to_centroid = dists[np.arange(len(dists)), cluster_id]

# Load threshold for anomaly detection

        threshold = float(np.load("models/threshold.npy")[0])
        is_anomaly = dist_to_centroid > threshold

        print(f"Using threshold={threshold:.4f}")

# Append scoring results to the DataFrame

        pdf["temp_z"] = Xs[:, 0]
        pdf["vib_z"] = Xs[:, 1]
        pdf["pressure_z"] = Xs[:, 2]
        pdf["cluster_id"] = cluster_id.astype(int)
        pdf["distance_to_centroid"] = dist_to_centroid.astype(float)
        pdf["is_anomaly"] = is_anomaly.astype(bool)

# Convert back to Spark DataFrame

        out_df = spark.createDataFrame(pdf)

        out_df.show(5, truncate=False)

# Write scored results to Postgres

        (
            out_df.write
            .format("jdbc")
            .mode("append")
            .option("url", jdbc_url)
            .option("dbtable", "sensor_readings_scored")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .save()
        )

        print(
            f"Wrote scored batch {batch_id}: rows={len(pdf)} "
            f"anomalies={int(is_anomaly.sum())} threshold={threshold:.3f}"
        )

# Set up the streaming query to score incoming data

    query = (
        parsed.writeStream
        .foreachBatch(score_batch)
        .outputMode("append")
        .option("checkpointLocation", "./checkpoints/kmeans_score")
        .start()
    )

# Await termination of the streaming query

    query.awaitTermination()


if __name__ == "__main__":
    os.makedirs("./checkpoints/kmeans_score", exist_ok=True)
    main()
