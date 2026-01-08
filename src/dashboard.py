import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1] 
sys.path.append(str(PROJECT_ROOT))

import time
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from src.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Streamlit dashboard for monitoring anomalies in simulated meteorological data

st.set_page_config(page_title="Meteorological Data Streaming Anomaly Dashboard", layout="wide")

# Create database engine

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

st.title("Meteorological Data Streaming Anomaly Dashboard")

# Sidebar controls
refresh_seconds = st.sidebar.slider("Refresh (seconds)", 1, 10, 2)
window_rows = st.sidebar.slider("Recent rows window", 200, 5000, 1000, 200)

# select device from simulated (and scored) sensor readings
devices = pd.read_sql(
    "SELECT DISTINCT device_id FROM sensor_readings_scored ORDER BY device_id;",
    engine
)["device_id"].tolist()

selected_device = st.sidebar.selectbox("Device", devices if devices else ["(none yet)"])

# Main dashboard

while True:
    totals = pd.read_sql("""
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies
        FROM sensor_readings_scored;
    """, engine).iloc[0]

    # Compute metrics

    total = int(totals["total"])
    anomalies = int(totals["anomalies"] or 0)
    rate = (anomalies / max(total, 1)) * 100.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total scored rows", total)
    c2.metric("Total anomalies", anomalies)
    c3.metric("Anomaly rate", f"{rate:.2f}%")

    # Recent data table

    recent = pd.read_sql(f"""
        SELECT device_id, event_ts, temperature, vibration, pressure,
               cluster_id, distance_to_centroid, is_anomaly
        FROM sensor_readings_scored
        ORDER BY event_ts DESC
        LIMIT {window_rows};
    """, engine)

    left, right = st.columns([2, 1]) 

    # Left: time series for selected device

    with left:
        st.subheader("Distance to centroid (selected device)")
        if selected_device and selected_device != "(none yet)":
            dev = recent[recent["device_id"] == selected_device].sort_values("event_ts")
            if len(dev) > 0:
                st.line_chart(dev.set_index("event_ts")["distance_to_centroid"])
                st.dataframe(dev.tail(20).sort_values("event_ts", ascending=False), use_container_width=True)
            else:
                st.info("No rows for this device in the current window yet.")

    # Right: latest anomalies

    with right:
        st.subheader("Latest anomalies")
        anom = recent[recent["is_anomaly"] == True].copy()
        st.dataframe(
            anom.sort_values("event_ts", ascending=False).head(30),
            use_container_width=True
        )

        st.subheader("Top anomalous devices (recent window)")
        if len(anom) > 0:
            top = (anom.groupby("device_id")
                      .size()
                      .sort_values(ascending=False)
                      .head(10)
                      .reset_index(name="anomaly_count"))
            st.dataframe(top, use_container_width=True)
        else:
            st.info("No anomalies in the recent window (try increasing anomaly_rate).")

    # Pause before refreshing

    time.sleep(refresh_seconds)
    st.rerun()
