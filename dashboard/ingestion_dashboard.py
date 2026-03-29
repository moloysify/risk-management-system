"""Ingestion Dashboard — Streamlit app for monitoring Kafka ingestion health.

Run:  streamlit run dashboard/ingestion_dashboard.py

Requirements: 13.1–13.8
"""

import streamlit as st
import pandas as pd
from db import get_connection

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Ingestion Dashboard", layout="wide")
st.title("📥 Ingestion Dashboard")

SLOW_THRESHOLD_MS = 5000

# ---------------------------------------------------------------------------
# Sidebar — time-window selector
# ---------------------------------------------------------------------------
hours = st.sidebar.number_input(
    "Time window (hours)", min_value=1, max_value=168, value=24
)

# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_ingestion_log(window_hours: int) -> pd.DataFrame:
    """Fetch INGESTION_LOG rows within the configured time window."""
    conn = get_connection()
    try:
        query = """
            SELECT log_id, batch_id, kafka_topic, data_type,
                   record_count, rejected_count, status,
                   processing_duration_ms, started_at, completed_at
              FROM INGESTION_LOG
             WHERE started_at >= DATEADD('hour', -%s, CURRENT_TIMESTAMP())
             ORDER BY started_at DESC
        """
        df = pd.read_sql(query, conn, params=(window_hours,))
        # normalise column names to lowercase
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


df = load_ingestion_log(hours)

# ---------------------------------------------------------------------------
# 1. Per-topic status  (Req 13.1, 13.8)
# ---------------------------------------------------------------------------
st.header("Topic Status")

if df.empty:
    st.info("No ingestion data in the selected window.")
else:
    topic_status = (
        df.sort_values("started_at")
        .groupby("kafka_topic")
        .last()[["status", "started_at"]]
        .reset_index()
    )

    def _status_label(s: str) -> str:
        if s == "error":
            return "🔴 Error"
        if s == "success":
            return "🟢 Active"
        return "🟡 Idle"

    for _, row in topic_status.iterrows():
        label = _status_label(row["status"])
        col1, col2 = st.columns([1, 3])
        col1.metric(row["kafka_topic"], label)
        if row["status"] == "error":
            col2.error(
                f"⚠️ Topic **{row['kafka_topic']}** entered error state "
                f"at {row['started_at']}"
            )

    # ---------------------------------------------------------------------------
    # 2. Total records processed by data type  (Req 13.2)
    # ---------------------------------------------------------------------------
    st.header("Records Processed by Data Type")
    records_by_type = df.groupby("data_type")["record_count"].sum()
    st.bar_chart(records_by_type)

    # ---------------------------------------------------------------------------
    # 3. Batch counts per data type  (Req 13.3)
    # ---------------------------------------------------------------------------
    st.header("Batch Counts by Data Type")
    batch_counts = df.groupby("data_type")["batch_id"].nunique()
    st.dataframe(batch_counts.rename("batches").reset_index())

    # ---------------------------------------------------------------------------
    # 4. Processing duration metrics  (Req 13.4)
    # ---------------------------------------------------------------------------
    st.header("Processing Duration (ms) by Data Type")
    duration_stats = (
        df.groupby("data_type")["processing_duration_ms"]
        .agg(["min", "max", "mean"])
        .rename(columns={"min": "Min", "max": "Max", "mean": "Avg"})
        .reset_index()
    )
    st.dataframe(duration_stats, use_container_width=True)

    # ---------------------------------------------------------------------------
    # 5. Processing frequency — batches per hour  (Req 13.6)
    # ---------------------------------------------------------------------------
    st.header("Processing Frequency (batches / hour)")
    freq = batch_counts / max(hours, 1)
    st.dataframe(freq.rename("batches_per_hour").reset_index())

    # ---------------------------------------------------------------------------
    # 6. Slow ingestion events  (Req 13.7)
    # ---------------------------------------------------------------------------
    st.header("Slow Ingestion Events (> 5 000 ms)")
    slow = df[df["processing_duration_ms"] > SLOW_THRESHOLD_MS]
    if slow.empty:
        st.success("No slow ingestion events detected.")
    else:
        st.warning(f"{len(slow)} slow batch(es) detected.")
        st.dataframe(
            slow[
                [
                    "batch_id",
                    "kafka_topic",
                    "data_type",
                    "processing_duration_ms",
                    "started_at",
                ]
            ],
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# Auto-refresh every 30 seconds  (Req 13.5)
# ---------------------------------------------------------------------------
st.markdown(
    '<meta http-equiv="refresh" content="30">',
    unsafe_allow_html=True,
)
