"""Simulator Dashboard page — redirects to the full simulator dashboard.

Note: The full simulator with Kafka publishing only works locally.
On Streamlit Cloud, this page shows the stress testing and portfolio
viewing features that work without Kafka.
"""
from __future__ import annotations
import sys, os
_dashboard_dir = os.path.join(os.path.dirname(__file__), "..")
_project_root = os.path.join(os.path.dirname(__file__), "..", "..")
for p in [_dashboard_dir, _project_root]:
    if p not in sys.path:
        sys.path.insert(0, p)

import streamlit as st
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Simulator", layout="wide")
st.title("🎲 Simulator & Stress Testing")

st.info("Portfolio creation and Kafka publishing require a local Kafka broker. Stress testing works from anywhere.")

# ---------------------------------------------------------------------------
# Historical Stress Testing (works without Kafka)
# ---------------------------------------------------------------------------
st.header("🔥 Historical Stress Testing")

from simulator.historical_events import HISTORICAL_EVENTS, run_stress_test

# Load portfolios from Snowflake
@st.cache_data(ttl=30)
def load_sf_portfolios():
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT t.participant_id,
                       COALESCE(em.participant_name, t.participant_id) AS name,
                       COALESCE(em.entity_type, 'unknown') AS desc,
                       LISTAGG(DISTINCT t.ticker, ',') WITHIN GROUP (ORDER BY t.ticker) AS tickers
                FROM TRADES t LEFT JOIN ENTITY_MASTER em ON t.participant_id = em.participant_id AND em.is_current = TRUE
                GROUP BY 1, 2, 3 ORDER BY 2
            """)
            return {row[1]: {"participant_id": row[0], "strategy_label": row[2], "tickers": [t.strip() for t in row[3].split(",") if t.strip()]} for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception:
        return {}

portfolios = load_sf_portfolios()

if portfolios:
    labels = {f"{n} — {p['strategy_label']} ({len(p['tickers'])} tickers)": n for n, p in portfolios.items()}
    selected = st.multiselect("Select portfolios", list(labels.keys()))
    selected_portfolios = {labels[l]: portfolios[labels[l]] for l in selected}
else:
    st.warning("No portfolios in Snowflake. Generate data locally first.")
    selected_portfolios = {}

# Events
if "stress_event_selection" not in st.session_state:
    st.session_state.stress_event_selection = []

event_options = {v["label"]: k for k, v in HISTORICAL_EVENTS.items()}

qc1, qc2, qc3 = st.columns(3)
with qc1:
    if st.button("All Gulf Wars"):
        st.session_state.stress_event_selection = [HISTORICAL_EVENTS[k]["label"] for k in ["yom_kippur_oil_embargo_1973","iranian_revolution_1979","iran_iraq_war","gulf_war_1","gulf_war_2","syrian_civil_war_houthi","iran_israel_2026"]]
        st.rerun()
with qc2:
    if st.button("All Crashes"):
        st.session_state.stress_event_selection = [HISTORICAL_EVENTS[k]["label"] for k in ["dotcom_crash","gfc_2008","covid_crash","black_monday_1987"]]
        st.rerun()
with qc3:
    if st.button("All Events"):
        st.session_state.stress_event_selection = list(event_options.keys())
        st.rerun()

sel_events = st.multiselect("Events", list(event_options.keys()), default=st.session_state.stress_event_selection, key="cloud_stress_events")
st.session_state.stress_event_selection = sel_events
sel_event_keys = [event_options[e] for e in sel_events]

horizon = st.slider("Horizon (months)", 1, 24, 6)
investment = st.number_input("Initial investment ($)", 10000, 100000000, 1000000, step=100000)

if st.button("🔥 Run Stress Test", type="primary"):
    if not selected_portfolios:
        st.error("Select at least one portfolio.")
    elif not sel_event_keys:
        st.error("Select at least one event.")
    else:
        for pname, pinfo in selected_portfolios.items():
            st.subheader(f"📁 {pname}")
            with st.spinner(f"Testing '{pname}'..."):
                try:
                    results = run_stress_test(pinfo["tickers"], sel_event_keys, horizon, investment)
                    rows = []
                    for ek, r in results.items():
                        if "error" in r:
                            rows.append({"Event": r["event"]["label"], "Return": "N/A", "Max DD": "N/A", "Final": "N/A", "Recovery": "N/A"})
                        else:
                            rows.append({"Event": r["event"]["label"], "Return": f"{r['total_return_pct']:+.1f}%", "Max DD": f"-{r['max_drawdown_pct']:.1f}%", "Final": f"${r['final_value']:,.0f}", "Recovery": f"{r['recovery_months']:.1f}mo" if r.get("recovery_months") else "N/A"})
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)

                    for ek, r in results.items():
                        if "error" not in r:
                            with st.expander(f"📉 {r['event']['label']}"):
                                ts_df = pd.DataFrame(r["portfolio_timeseries"])
                                ts_df["date"] = pd.to_datetime(ts_df["date"])
                                st.line_chart(ts_df.set_index("date")["portfolio_value"])
                                st.dataframe(pd.DataFrame(r["ticker_performance"]).sort_values("total_return_pct"), use_container_width=True)
                except Exception as exc:
                    st.error(f"Failed: {exc}")
            st.divider()
