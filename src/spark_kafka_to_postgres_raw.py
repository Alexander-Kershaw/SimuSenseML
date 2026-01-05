import os
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
# Define the schema for incoming sensor data
sensor_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("vibration", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("label", StringType(), True),
])

# Main function to stream data from Kafka to Postgres

def main():
    spark = (
        SparkSession.builder
        .appName("IoT-Kafka-To-Postgres-Raw")
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

# Read streaming data from Kafka

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

# Parse the JSON messages and extract fields

    parsed = (
        kafka_df
        .select(col("value").cast("string").alias("json_str"))
        .select(from_json(col("json_str"), sensor_schema).alias("data"), col("json_str"))
        .select(
            col("data.device_id").alias("device_id"),
            to_timestamp(col("data.timestamp")).alias("event_ts"),
            col("data.temperature").alias("temperature"),
            col("data.vibration").alias("vibration"),
            col("data.pressure").alias("pressure"),
            col("json_str").alias("payload")
        )
    )

# Define JDBC URL for Postgres

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Function to write each micro-batch to Postgres

    def write_batch(batch_df, batch_id: int):
        print(f"\n=== BATCH {batch_id} ===")
        batch_df.show(5, truncate=False)
          
        (
            batch_df.write
            .format("jdbc")
            .mode("append")
            .option("url", jdbc_url)
            .option("dbtable", "sensor_readings_raw")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .save()
        )
        print(f"Wrote batch {batch_id} rows={batch_df.count()}")

# Set up the streaming

    query = (
        parsed.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", "./checkpoints/kafka_to_pg_raw")
        .start()
    )
# Await termination of the streaming query

    query.awaitTermination()


if __name__ == "__main__":
    os.makedirs("./checkpoints/kafka_to_pg_raw", exist_ok=True)
    main()
