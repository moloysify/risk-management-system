"""Risk Management System — Home Dashboard

Run:  streamlit run dashboard/home_dashboard.py --server.port 8500
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Risk Management System", layout="wide")
st.title("🏦 Risk Management System")
st.caption("Mini VaR-based Risk Management Platform on Snowflake")


# ---------------------------------------------------------------------------
# Quick stats from Snowflake
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30)
def load_stats():
    conn = get_connection()
    try:
        cur = conn.cursor()
        stats = {}
        for table in ["TRADES", "MARKET_DATA", "SECURITY_MASTER", "ENTITY_MASTER", "VAR_RESULTS"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT run_id) FROM VAR_RESULTS")
        stats["VAR_RUNS"] = cur.fetchone()[0]
        cur.execute("SELECT MAX(run_timestamp) FROM VAR_RESULTS")
        row = cur.fetchone()
        stats["LAST_RUN"] = str(row[0]) if row[0] else "Never"
        return stats
    finally:
        conn.close()


try:
    stats = load_stats()

    st.header("System Overview")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Trades", f"{stats['TRADES']:,}")
    c2.metric("Market Data", f"{stats['MARKET_DATA']:,}")
    c3.metric("Securities", f"{stats['SECURITY_MASTER']:,}")
    c4.metric("Participants", f"{stats['ENTITY_MASTER']:,}")
    c5.metric("VaR Runs", f"{stats['VAR_RUNS']:,}")

    st.caption(f"Last VaR run: {stats['LAST_RUN']}")
except Exception:
    st.warning("Could not load stats from Snowflake. Check your connection.")

# ---------------------------------------------------------------------------
# Dashboard links
# ---------------------------------------------------------------------------
st.header("Dashboards")
st.caption("Use the sidebar to navigate between dashboards.")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### 📥 Ingestion Dashboard
    Monitor Kafka ingestion pipeline health — topic status, record counts,
    batch metrics, slow events, and error alerts.
    """)

    st.markdown("""
    ### ⚙️ Compute Dashboard
    View Snowflake compute execution history, manage VaR frequency controls,
    disable all scheduled runs, and trigger on-demand VaR computation.
    """)

with col2:
    st.markdown("""
    ### 🎲 Simulator & Stress Testing
    Historical stress testing against Gulf Wars, market crashes, and more.
    Portfolio simulation with Kafka publishing available locally.
    """)

    st.markdown("""
    ### 📊 Reports Dashboard
    View all risk reports — Exposure by Participant, Top CUSIPs, Top
    Participants, Concentration, and the Cortex market narrative.
    """)

# ---------------------------------------------------------------------------
# Quick actions
# ---------------------------------------------------------------------------
st.header("Quick Actions")

qa1, qa2, qa3 = st.columns(3)

with qa1:
    if st.button("🚀 Run VaR Now", type="primary"):
        try:
            from db import call_procedure
            with st.spinner("Running COMPUTE_VAR()..."):
                result = call_procedure("CALL COMPUTE_VAR()")
            st.success(result)
            load_stats.clear()
        except Exception as exc:
            st.error(f"Failed: {exc}")

with qa2:
    if st.button("🎲 Generate Sample Data"):
        try:
            import os, subprocess, sys
            env = os.environ.copy()
            env["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
            result = subprocess.run(
                [sys.executable, "-c", """
import os
os.environ['SNOWFLAKE_ACCOUNT'] = os.environ.get('SNOWFLAKE_ACCOUNT', '')
os.environ['SNOWFLAKE_USER'] = os.environ.get('SNOWFLAKE_USER', '')
os.environ['SNOWFLAKE_PASSWORD'] = os.environ.get('SNOWFLAKE_PASSWORD', '')
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
from simulator.config import SimulatorConfig
from simulator.simulator import DataSimulator
config = SimulatorConfig(mode='batch', dataset_size='small', volume=50)
DataSimulator(config).start()
print('Done')
"""],
                capture_output=True, text=True, timeout=60,
                cwd=os.path.dirname(os.path.dirname(__file__)),
            )
            if result.returncode == 0:
                st.success("Sample data generated and published to Kafka!")
                load_stats.clear()
            else:
                st.error(f"Error: {result.stderr[:200]}")
        except Exception as exc:
            st.error(f"Failed: {exc}")

with qa3:
    if st.button("🔄 Refresh Stats"):
        load_stats.clear()
        st.rerun()

# ---------------------------------------------------------------------------
# Danger Zone
# ---------------------------------------------------------------------------
st.header("Danger Zone")

with st.expander("⚠️ Wipe All Data from Snowflake", expanded=False):
    st.warning("This will permanently delete ALL data from every table. The schema and procedures will remain intact.")

    wipe_tables = [
        "TRADES", "MARKET_DATA", "SECURITY_MASTER", "ENTITY_MASTER",
        "VAR_RESULTS", "REJECTION_LOG", "INGESTION_LOG",
        "MARKET_NARRATIVES", "COMPUTE_EXECUTION_LOG", "SIMULATION_LOG",
        "REPORT_EXPOSURE_BY_PARTICIPANT", "REPORT_CONCENTRATION",
        "REPORT_HIGH_VOLUME_CUSIP", "REPORT_HIGH_EXPOSURE_PARTICIPANT",
        "REPORT_TOP_CUSIPS", "REPORT_TOP_PARTICIPANTS",
    ]

    confirm = st.text_input("Type **WIPE ALL** to confirm:", key="wipe_confirm")

    if st.button("🗑️ Wipe All Data", type="secondary", key="wipe_btn"):
        if confirm == "WIPE ALL":
            try:
                conn = get_connection()
                cur = conn.cursor()
                wiped = []
                for table in wipe_tables:
                    cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
                    wiped.append(table)
                # Re-seed VAR_CONFIG
                cur.execute("""
                    MERGE INTO VAR_CONFIG AS target
                    USING (SELECT '00000000-0000-0000-0000-000000000001' AS config_id,
                                  TRUE AS event_driven_enabled, 15 AS schedule_interval_minutes,
                                  'SYSTEM' AS updated_by, CURRENT_TIMESTAMP() AS updated_at) AS source
                    ON target.config_id = source.config_id
                    WHEN NOT MATCHED THEN INSERT VALUES (source.config_id, source.event_driven_enabled,
                        source.schedule_interval_minutes, source.updated_by, source.updated_at)
                """)
                conn.close()
                st.success(f"Wiped {len(wiped)} tables. VAR_CONFIG re-seeded with defaults.")
                load_stats.clear()
            except Exception as exc:
                st.error(f"Failed: {exc}")
        else:
            st.error("Please type WIPE ALL to confirm.")

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
st.markdown(
    '<meta http-equiv="refresh" content="60">',
    unsafe_allow_html=True,
)
