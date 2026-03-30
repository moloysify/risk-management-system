"""Simulator & Stress Testing page."""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
from db import get_connection
from simulator.historical_events import HISTORICAL_EVENTS, run_stress_test

st.set_page_config(page_title="Simulator", layout="wide")
st.title("🎲 Simulator & Stress Testing")

st.header("🔥 Historical Stress Testing")

@st.cache_data(ttl=30)
def load_sf_portfolios():
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""SELECT t.participant_id, COALESCE(em.participant_name,t.participant_id), COALESCE(em.entity_type,'unknown'),
                LISTAGG(DISTINCT t.ticker,',') WITHIN GROUP (ORDER BY t.ticker)
                FROM TRADES t LEFT JOIN ENTITY_MASTER em ON t.participant_id=em.participant_id AND em.is_current=TRUE GROUP BY 1,2,3 ORDER BY 2""")
            return {row[1]:{"participant_id":row[0],"strategy_label":row[2],"tickers":[t.strip() for t in row[3].split(",") if t.strip()]} for row in cur.fetchall()}
        finally: conn.close()
    except: return {}

portfolios = load_sf_portfolios()
if portfolios:
    labels = {f"{n} — {p['strategy_label']} ({len(p['tickers'])} tickers)":n for n,p in portfolios.items()}
    selected = st.multiselect("Select portfolios", list(labels.keys()))
    sel_ports = {labels[l]:portfolios[labels[l]] for l in selected}
else:
    st.warning("No portfolios in Snowflake yet.")
    sel_ports = {}

if "stress_events" not in st.session_state: st.session_state.stress_events = []
event_opts = {v["label"]:k for k,v in HISTORICAL_EVENTS.items()}

q1,q2,q3 = st.columns(3)
with q1:
    if st.button("All Gulf Wars"):
        st.session_state.stress_events = [HISTORICAL_EVENTS[k]["label"] for k in ["yom_kippur_oil_embargo_1973","iranian_revolution_1979","iran_iraq_war","gulf_war_1","gulf_war_2","syrian_civil_war_houthi","iran_israel_2026"]]
        st.rerun()
with q2:
    if st.button("All Crashes"):
        st.session_state.stress_events = [HISTORICAL_EVENTS[k]["label"] for k in ["dotcom_crash","gfc_2008","covid_crash","black_monday_1987"]]
        st.rerun()
with q3:
    if st.button("All Events"):
        st.session_state.stress_events = list(event_opts.keys())
        st.rerun()

sel_ev = st.multiselect("Events", list(event_opts.keys()), default=st.session_state.stress_events, key="cloud_ev")
st.session_state.stress_events = sel_ev
sel_keys = [event_opts[e] for e in sel_ev]

horizon = st.slider("Horizon (months)", 1, 24, 6)
invest = st.number_input("Investment ($)", 10000, 100000000, 1000000, step=100000)

if st.button("🔥 Run Stress Test", type="primary"):
    if not sel_ports: st.error("Select portfolios.")
    elif not sel_keys: st.error("Select events.")
    else:
        for pn, pi in sel_ports.items():
            st.subheader(f"📁 {pn}")
            with st.spinner(f"Testing '{pn}'..."):
                try:
                    res = run_stress_test(pi["tickers"], sel_keys, horizon, invest)
                    rows = []
                    for ek,r in res.items():
                        if "error" in r: rows.append({"Event":r["event"]["label"],"Return":"N/A","Max DD":"N/A","Final":"N/A","Recovery":"N/A"})
                        else: rows.append({"Event":r["event"]["label"],"Return":f"{r['total_return_pct']:+.1f}%","Max DD":f"-{r['max_drawdown_pct']:.1f}%","Final":f"${r['final_value']:,.0f}","Recovery":f"{r['recovery_months']:.1f}mo" if r.get("recovery_months") else "N/A"})
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                    for ek,r in res.items():
                        if "error" not in r:
                            with st.expander(f"📉 {r['event']['label']}"):
                                ts = pd.DataFrame(r["portfolio_timeseries"]); ts["date"]=pd.to_datetime(ts["date"])
                                st.line_chart(ts.set_index("date")["portfolio_value"])
                                st.dataframe(pd.DataFrame(r["ticker_performance"]).sort_values("total_return_pct"), use_container_width=True)
                except Exception as e: st.error(str(e))
            st.divider()
