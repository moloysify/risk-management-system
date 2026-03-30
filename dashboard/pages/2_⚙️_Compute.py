"""Compute Dashboard page."""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datetime as dt
import streamlit as st
import pandas as pd
from db import get_connection, call_procedure

st.set_page_config(page_title="Compute Dashboard", layout="wide")
st.title("⚙️ Compute Dashboard")

range_option = st.sidebar.radio("Time range", ["Last 1 hour", "Last 24 hours", "Custom"])
now = dt.datetime.utcnow()
if range_option == "Last 1 hour":
    start_dt, end_dt = now - dt.timedelta(hours=1), now
elif range_option == "Last 24 hours":
    start_dt, end_dt = now - dt.timedelta(hours=24), now
else:
    start_dt = st.sidebar.date_input("Start date", value=now - dt.timedelta(days=1))
    end_dt = st.sidebar.date_input("End date", value=now)
    if isinstance(start_dt, dt.date): start_dt = dt.datetime.combine(start_dt, dt.time.min)
    if isinstance(end_dt, dt.date): end_dt = dt.datetime.combine(end_dt, dt.time.max)

@st.cache_data(ttl=60)
def load_executions(start, end):
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM COMPUTE_EXECUTION_LOG WHERE execution_start BETWEEN %s AND %s ORDER BY execution_start DESC", conn, params=(start, end))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()

df = load_executions(str(start_dt), str(end_dt))

st.header("Compute Executions")
if df.empty:
    st.info("No executions found for the selected period.")
else:
    st.dataframe(df[["computation_name", "execution_start", "execution_duration_seconds", "status"]], use_container_width=True)
    failures = df[df["status"] == "failure"]
    if not failures.empty:
        st.subheader("Failed Executions")
        st.dataframe(failures[["computation_name", "execution_start", "error_message", "error_code"]], use_container_width=True)

# VaR Config
st.header("VaR Frequency Controls")

@st.cache_data(ttl=10)
def load_var_config():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT config_id, event_driven_enabled, schedule_interval_minutes, updated_by, updated_at FROM VAR_CONFIG WHERE config_id = '00000000-0000-0000-0000-000000000001'")
        row = cur.fetchone()
        return {"config_id": row[0], "event_driven_enabled": row[1], "schedule_interval_minutes": row[2], "updated_by": row[3], "updated_at": row[4]} if row else {}
    finally:
        conn.close()

config = load_var_config()
if config:
    c1, c2 = st.columns(2)
    c1.metric("Event-Driven", "Enabled" if config["event_driven_enabled"] else "Disabled")
    c2.metric("Schedule Interval", f"{config['schedule_interval_minutes']} min")
    st.caption(f"Last updated by **{config['updated_by']}** at {config['updated_at']}")

    new_ed = st.toggle("Enable event-driven VaR", value=bool(config["event_driven_enabled"]))
    new_int = st.selectbox("Schedule interval (minutes)", [5, 15, 30, 60], index=[5,15,30,60].index(config["schedule_interval_minutes"]) if config["schedule_interval_minutes"] in [5,15,30,60] else 0)
    if st.button("Apply Changes"):
        try:
            result = call_procedure("CALL UPDATE_VAR_CONFIG(%s, %s, %s, %s)", (new_ed, new_int, "dashboard_user", str(config["updated_at"])))
            st.success(result)
            load_var_config.clear()
            load_executions.clear()
        except Exception as exc:
            st.error(f"Failed: {exc}")

# On-Demand Controls
st.header("On-Demand Controls")
col_d, col_r = st.columns(2)
with col_d:
    if st.button("🛑 Disable All Scheduled Runs"):
        try:
            conn = get_connection()
            cur = conn.cursor()
            try: cur.execute("ALTER TASK VAR_EVENT_DRIVEN_TASK SUSPEND")
            except: pass
            try: cur.execute("ALTER TASK VAR_SCHEDULED_TASK SUSPEND")
            except: pass
            cur.execute("UPDATE VAR_CONFIG SET event_driven_enabled = FALSE, updated_by = 'dashboard_user', updated_at = CURRENT_TIMESTAMP() WHERE config_id = '00000000-0000-0000-0000-000000000001'")
            conn.close()
            st.success("All scheduled runs disabled.")
            load_var_config.clear()
        except Exception as exc:
            st.error(f"Failed: {exc}")
with col_r:
    if st.button("🚀 Run VaR Now", type="primary"):
        try:
            with st.spinner("Running COMPUTE_VAR()..."):
                result = call_procedure("CALL COMPUTE_VAR()")
            st.success(result)
            load_executions.clear()
        except Exception as exc:
            st.error(f"Failed: {exc}")
