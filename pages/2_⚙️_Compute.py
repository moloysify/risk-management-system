"""Compute Dashboard page."""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datetime as dt
import streamlit as st
import pandas as pd
from db import get_connection, call_procedure

st.set_page_config(page_title="Compute Dashboard", layout="wide")
st.title("⚙️ Compute Dashboard")

range_opt = st.sidebar.radio("Time range", ["Last 1 hour", "Last 24 hours", "Custom"])
now = dt.datetime.utcnow()
if range_opt == "Last 1 hour": start_dt, end_dt = now - dt.timedelta(hours=1), now
elif range_opt == "Last 24 hours": start_dt, end_dt = now - dt.timedelta(hours=24), now
else:
    start_dt = st.sidebar.date_input("Start", value=now - dt.timedelta(days=1))
    end_dt = st.sidebar.date_input("End", value=now)
    if isinstance(start_dt, dt.date): start_dt = dt.datetime.combine(start_dt, dt.time.min)
    if isinstance(end_dt, dt.date): end_dt = dt.datetime.combine(end_dt, dt.time.max)

@st.cache_data(ttl=60)
def load_exec(s, e):
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM COMPUTE_EXECUTION_LOG WHERE execution_start BETWEEN %s AND %s ORDER BY execution_start DESC", conn, params=(s, e))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally: conn.close()

df = load_exec(str(start_dt), str(end_dt))
st.header("Executions")
if df.empty: st.info("No executions found.")
else:
    st.dataframe(df[["computation_name","execution_start","execution_duration_seconds","status"]], use_container_width=True)
    fails = df[df["status"]=="failure"]
    if not fails.empty:
        st.subheader("Failures")
        st.dataframe(fails[["computation_name","execution_start","error_message","error_code"]], use_container_width=True)

st.header("On-Demand")
c1, c2 = st.columns(2)
with c1:
    if st.button("🛑 Disable Scheduled Runs"):
        try:
            conn = get_connection(); cur = conn.cursor()
            try: cur.execute("ALTER TASK VAR_EVENT_DRIVEN_TASK SUSPEND")
            except: pass
            try: cur.execute("ALTER TASK VAR_SCHEDULED_TASK SUSPEND")
            except: pass
            cur.execute("UPDATE VAR_CONFIG SET event_driven_enabled=FALSE, updated_by='cloud', updated_at=CURRENT_TIMESTAMP() WHERE config_id='00000000-0000-0000-0000-000000000001'")
            conn.close(); st.success("Disabled."); load_exec.clear()
        except Exception as e: st.error(str(e))
with c2:
    if st.button("🚀 Run VaR Now", type="primary"):
        try:
            with st.spinner("Running..."): r = call_procedure("CALL COMPUTE_VAR()")
            st.success(r); load_exec.clear()
        except Exception as e: st.error(str(e))
