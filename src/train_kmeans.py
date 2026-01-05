import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import joblib

# Import database configuration
from src.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

MODEL_PATH = "models/kmeans.joblib"
SCALER_PATH = "models/scaler.joblib"

# KMeans Model Training
def main():
# Create database engine
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

# Retrieve recent sensor data for training
    df = pd.read_sql("""
        SELECT temperature, vibration, pressure
        FROM sensor_readings_raw
        ORDER BY id DESC
        LIMIT 5000
    """, engine)


# Condition to ensure enough data is available for training
    if len(df) < 200:
        raise RuntimeError(f"Not enough rows to train (got {len(df)}). Let Spark run longer.")
    
# Prepare data for training
    X = df[["temperature", "vibration", "pressure"]].values

# Scale features
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

# Choose K=3 for demo
    kmeans = KMeans(n_clusters=3, n_init=10, random_state=42)
    kmeans.fit(Xs)

# Save model and scaler
    import os
    os.makedirs("models", exist_ok=True)
    joblib.dump(kmeans, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)

    print("Saved:")
    print(" -", MODEL_PATH)
    print(" -", SCALER_PATH)
    print("Cluster centers (scaled space):")
    print(kmeans.cluster_centers_)


if __name__ == "__main__":
    main()
