"""Compute Dashboard — Streamlit app for compute execution monitoring
and VaR frequency controls.

Run:  streamlit run dashboard/compute_dashboard.py

Requirements: 14.1–14.8, 15.1–15.4
"""
from __future__ import annotations

import datetime as dt
import streamlit as st
import pandas as pd
from db import get_connection, call_procedure

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Compute Dashboard", layout="wide")
st.title("⚙️ Compute Dashboard")

# ---------------------------------------------------------------------------
# Sidebar — time-range selector  (Req 14.2)
# ---------------------------------------------------------------------------
range_option = st.sidebar.radio(
    "Time range", ["Last 1 hour", "Last 24 hours", "Custom"]
)

now = dt.datetime.utcnow()
if range_option == "Last 1 hour":
    start_dt = now - dt.timedelta(hours=1)
    end_dt = now
elif range_option == "Last 24 hours":
    start_dt = now - dt.timedelta(hours=24)
    end_dt = now
else:
    start_dt = st.sidebar.date_input("Start date", value=now - dt.timedelta(days=1))
    end_dt = st.sidebar.date_input("End date", value=now)
    # Convert date → datetime for consistency
    if isinstance(start_dt, dt.date):
        start_dt = dt.datetime.combine(start_dt, dt.time.min)
    if isinstance(end_dt, dt.date):
        end_dt = dt.datetime.combine(end_dt, dt.time.max)

# ---------------------------------------------------------------------------
# Load compute executions  (Req 14.1, 14.3, 14.5)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_executions(start: str, end: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        query = """
            SELECT execution_id, computation_name, execution_start,
                   execution_duration_seconds, status,
                   error_message, error_code
              FROM COMPUTE_EXECUTION_LOG
             WHERE execution_start BETWEEN %s AND %s
             ORDER BY execution_start DESC
        """
        df = pd.read_sql(query, conn, params=(start, end))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


df = load_executions(str(start_dt), str(end_dt))

# ---------------------------------------------------------------------------
# Execution list  (Req 14.1, 14.4, 14.5, 14.7)
# ---------------------------------------------------------------------------
st.header("Compute Executions")

if df.empty:
    st.info("No executions found for the selected period.")
else:
    # Display core columns
    display_cols = [
        "computation_name",
        "execution_start",
        "execution_duration_seconds",
        "status",
    ]
    st.dataframe(df[display_cols], use_container_width=True)

    # Show error details for failures  (Req 14.4)
    failures = df[df["status"] == "failure"]
    if not failures.empty:
        st.subheader("Failed Executions")
        st.dataframe(
            failures[
                [
                    "computation_name",
                    "execution_start",
                    "error_message",
                    "error_code",
                ]
            ],
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# VaR Frequency Control Panel  (Req 14.8, 15.1–15.4)
# ---------------------------------------------------------------------------
st.header("VaR Frequency Controls")


@st.cache_data(ttl=10)
def load_var_config() -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT config_id, event_driven_enabled,
                      schedule_interval_minutes, updated_by, updated_at
                 FROM VAR_CONFIG
                WHERE config_id = '00000000-0000-0000-0000-000000000001'"""
        )
        row = cur.fetchone()
        if row is None:
            return {}
        return {
            "config_id": row[0],
            "event_driven_enabled": row[1],
            "schedule_interval_minutes": row[2],
            "updated_by": row[3],
            "updated_at": row[4],
        }
    finally:
        conn.close()


config = load_var_config()

if not config:
    st.warning("VAR_CONFIG row not found.")
else:
    # Read-only current state  (Req 15.4)
    st.subheader("Current Configuration")
    c1, c2 = st.columns(2)
    c1.metric(
        "Event-Driven",
        "Enabled" if config["event_driven_enabled"] else "Disabled",
    )
    c2.metric("Schedule Interval", f"{config['schedule_interval_minutes']} min")

    st.caption(
        f"Last updated by **{config['updated_by']}** "
        f"at {config['updated_at']}"
    )

    # Editable controls
    st.subheader("Update Configuration")
    new_event_driven = st.toggle(
        "Enable event-driven VaR",
        value=bool(config["event_driven_enabled"]),
    )

    interval_options = [5, 15, 30, 60]
    current_idx = (
        interval_options.index(config["schedule_interval_minutes"])
        if config["schedule_interval_minutes"] in interval_options
        else 0
    )
    new_interval = st.selectbox(
        "Schedule interval (minutes)",
        options=interval_options,
        index=current_idx,
    )

    if st.button("Apply Changes"):
        try:
            result = call_procedure(
                "CALL UPDATE_VAR_CONFIG(%s, %s, %s, %s)",
                (
                    new_event_driven,
                    new_interval,
                    "dashboard_user",
                    str(config["updated_at"]),
                ),
            )
            st.success(result)
            # Clear caches so next render picks up new values
            load_var_config.clear()
            load_executions.clear()
        except Exception as exc:
            st.error(f"Failed to update config: {exc}")

# ---------------------------------------------------------------------------
# On-Demand Controls
# ---------------------------------------------------------------------------
st.header("On-Demand Controls")

col_disable, col_run = st.columns(2)

with col_disable:
    if st.button("🛑 Disable All Scheduled Runs", type="secondary"):
        try:
            conn = get_connection()
            cur = conn.cursor()
            # Suspend both tasks
            try:
                cur.execute("ALTER TASK VAR_EVENT_DRIVEN_TASK SUSPEND")
            except Exception:
                pass
            try:
                cur.execute("ALTER TASK VAR_SCHEDULED_TASK SUSPEND")
            except Exception:
                pass
            # Update config to reflect disabled state
            cur.execute("""
                UPDATE VAR_CONFIG
                SET event_driven_enabled = FALSE,
                    updated_by = 'dashboard_user',
                    updated_at = CURRENT_TIMESTAMP()
                WHERE config_id = '00000000-0000-0000-0000-000000000001'
            """)
            conn.close()
            st.success("All scheduled runs disabled. Use 'Run VaR Now' for on-demand execution.")
            load_var_config.clear()
            load_executions.clear()
        except Exception as exc:
            st.error(f"Failed to disable scheduled runs: {exc}")

with col_run:
    if st.button("🚀 Run VaR Now", type="primary"):
        try:
            with st.spinner("Running COMPUTE_VAR()..."):
                result = call_procedure("CALL COMPUTE_VAR()")
            st.success(result)
            load_executions.clear()
        except Exception as exc:
            st.error(f"VaR computation failed: {exc}")

# Auto-refresh disabled to preserve user state
