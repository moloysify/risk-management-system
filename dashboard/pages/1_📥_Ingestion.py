"""Ingestion Dashboard page."""
from __future__ import annotations
import sys, os
# Ensure dashboard dir is on path for db.py imports
_dashboard_dir = os.path.join(os.path.dirname(__file__), "..")
_project_root = os.path.join(os.path.dirname(__file__), "..", "..")
for p in [_dashboard_dir, _project_root]:
    if p not in sys.path:
        sys.path.insert(0, p)

import streamlit as st
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Ingestion Dashboard", layout="wide")
st.title("📥 Ingestion Dashboard")

SLOW_THRESHOLD_MS = 5000
hours = st.sidebar.number_input("Time window (hours)", min_value=1, max_value=168, value=24)

@st.cache_data(ttl=30)
def load_ingestion_log(window_hours):
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT log_id, batch_id, kafka_topic, data_type,
                   record_count, rejected_count, status,
                   processing_duration_ms, started_at, completed_at
              FROM INGESTION_LOG
             WHERE started_at >= DATEADD('hour', -%s, CURRENT_TIMESTAMP())
             ORDER BY started_at DESC
        """, conn, params=(window_hours,))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()

df = load_ingestion_log(hours)

st.header("Topic Status")
if df.empty:
    st.info("No ingestion data in the selected window.")
else:
    topic_status = df.sort_values("started_at").groupby("kafka_topic").last()[["status", "started_at"]].reset_index()
    for _, row in topic_status.iterrows():
        label = "🔴 Error" if row["status"] == "error" else ("🟢 Active" if row["status"] == "success" else "🟡 Idle")
        c1, c2 = st.columns([1, 3])
        c1.metric(row["kafka_topic"], label)
        if row["status"] == "error":
            c2.error(f"⚠️ Topic **{row['kafka_topic']}** entered error state at {row['started_at']}")

    st.header("Records Processed by Data Type")
    st.bar_chart(df.groupby("data_type")["record_count"].sum())

    st.header("Batch Counts by Data Type")
    st.dataframe(df.groupby("data_type")["batch_id"].nunique().rename("batches").reset_index())

    st.header("Processing Duration (ms) by Data Type")
    st.dataframe(df.groupby("data_type")["processing_duration_ms"].agg(["min", "max", "mean"]).rename(columns={"min": "Min", "max": "Max", "mean": "Avg"}).reset_index(), use_container_width=True)

    st.header("Processing Frequency (batches / hour)")
    st.dataframe((df.groupby("data_type")["batch_id"].nunique() / max(hours, 1)).rename("batches_per_hour").reset_index())

    st.header("Slow Ingestion Events (> 5,000 ms)")
    slow = df[df["processing_duration_ms"] > SLOW_THRESHOLD_MS]
    if slow.empty:
        st.success("No slow ingestion events detected.")
    else:
        st.warning(f"{len(slow)} slow batch(es) detected.")
        st.dataframe(slow[["batch_id", "kafka_topic", "data_type", "processing_duration_ms", "started_at"]], use_container_width=True)
