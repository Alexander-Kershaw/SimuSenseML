import os
import numpy as np
import pandas as pd
import joblib
from sqlalchemy import create_engine

from src.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD


# Paths to the saved model, scaler, and threshold
MODEL_PATH = "models/kmeans.joblib"
SCALER_PATH = "models/scaler.joblib"
THRESH_PATH = "models/threshold.npy"

# Compute anomaly detection threshold based on training data distances

def main():
    kmeans = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    # Create database engine

    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    # Retrieve recent sensor data for threshold computation

    df = pd.read_sql("""
        SELECT temperature, vibration, pressure
        FROM sensor_readings_raw
        ORDER BY id DESC
        LIMIT 8000
    """, engine)

    # Prepare data and compute distances to nearest cluster center

    X = df[["temperature", "vibration", "pressure"]].to_numpy(dtype=float)
    Xs = scaler.transform(X)

    centers = kmeans.cluster_centers_
    dists = np.linalg.norm(Xs[:, None, :] - centers[None, :, :], axis=2) # shape (n_samples, n_clusters)
    nearest = np.min(dists, axis=1) # distance to nearest center

    # choose percentile

    threshold = float(np.quantile(nearest, 0.995))  #  approx 0.5% anomalies on training dist

    os.makedirs("models", exist_ok=True) 
    np.save(THRESH_PATH, np.array([threshold], dtype=float))

    print(f"Saved {THRESH_PATH} threshold={threshold:.4f}")

if __name__ == "__main__":
    main()
