"""Reports Dashboard page."""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Risk Reports", layout="wide")
st.title("📊 Risk Reports Dashboard")

@st.cache_data(ttl=30)
def get_all_run_ids():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT run_id, MIN(run_timestamp) as ts FROM VAR_RESULTS GROUP BY run_id ORDER BY ts DESC LIMIT 20")
        return [(r[0], str(r[1])) for r in cur.fetchall()]
    finally:
        conn.close()

def load_report(table, run_id):
    conn = get_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {table} WHERE run_id = %s", conn, params=(run_id,))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()

def load_narrative(run_id):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT narrative_text FROM MARKET_NARRATIVES WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()

# Portfolio Summary (always visible)
st.header("📁 Portfolio Summary")

@st.cache_data(ttl=30)
def load_portfolio_summary():
    conn = get_connection()
    try:
        df = pd.read_sql("""
            WITH pt AS (
                SELECT COALESCE(em.participant_name, t.participant_id) AS portfolio,
                       COALESCE(em.entity_type, 'unknown') AS description,
                       COUNT(DISTINCT t.trade_id) AS positions, COUNT(DISTINCT t.cusip) AS securities,
                       SUM(t.price * t.quantity) AS gross_market_value,
                       SUM(CASE WHEN t.quantity >= 0 THEN t.price * t.quantity ELSE -1 * t.price * ABS(t.quantity) END) AS net_market_value,
                       t.participant_id
                FROM TRADES t LEFT JOIN ENTITY_MASTER em ON t.participant_id = em.participant_id AND em.is_current = TRUE
                GROUP BY 1, 2, t.participant_id),
            lv AS (SELECT participant_id, SUM(var_exposure) AS var_exposure FROM VAR_RESULTS
                   WHERE run_id = (SELECT run_id FROM VAR_RESULTS ORDER BY run_timestamp DESC LIMIT 1) GROUP BY participant_id)
            SELECT pt.*, COALESCE(lv.var_exposure, 0) AS var_exposure,
                   CASE WHEN pt.gross_market_value > 0 THEN ROUND(COALESCE(lv.var_exposure,0)/pt.gross_market_value*100,2) ELSE 0 END AS var_pct_gross,
                   CASE WHEN pt.net_market_value > 0 THEN ROUND(COALESCE(lv.var_exposure,0)/pt.net_market_value*100,2) ELSE 0 END AS var_pct_net
            FROM pt LEFT JOIN lv ON pt.participant_id = lv.participant_id ORDER BY pt.gross_market_value DESC
        """, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()

port_df = load_portfolio_summary()
if port_df.empty:
    st.info("No trades in the system yet.")
else:
    tc1, tc2, tc3 = st.columns(3)
    tc1.metric("Total Portfolios", len(port_df))
    tc2.metric("Total Gross MV", f"${port_df['gross_market_value'].sum():,.0f}")
    tc3.metric("Total Net MV", f"${port_df['net_market_value'].sum():,.0f}")
    ddf = port_df[["portfolio","description","positions","securities","gross_market_value","net_market_value","var_exposure","var_pct_gross","var_pct_net"]].copy()
    ddf["gross_market_value"] = ddf["gross_market_value"].apply(lambda x: f"${x:,.0f}")
    ddf["net_market_value"] = ddf["net_market_value"].apply(lambda x: f"${x:,.0f}")
    ddf["var_exposure"] = ddf["var_exposure"].apply(lambda x: f"${x:,.0f}")
    ddf["var_pct_gross"] = ddf["var_pct_gross"].apply(lambda x: f"{x:.2f}%")
    ddf["var_pct_net"] = ddf["var_pct_net"].apply(lambda x: f"{x:.2f}%")
    ddf.columns = ["Portfolio","Description","Positions","Securities","Gross MV","Net MV","VaR","% Gross MV","% Net MV"]
    st.dataframe(ddf, use_container_width=True)

# VaR Reports
runs = get_all_run_ids()
if not runs:
    st.info("No VaR runs found yet. Run COMPUTE_VAR() to see risk reports.")
else:
    run_options = {f"{rid} ({ts})": rid for rid, ts in runs}
    selected_label = st.sidebar.selectbox("VaR Run", list(run_options.keys()))
    run_id = run_options[selected_label]

    var_df = load_report("VAR_RESULTS", run_id)
    if not var_df.empty:
        st.header("VaR Summary")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total VaR", f"${var_df['var_exposure'].sum():,.2f}")
        c2.metric("Positions", len(var_df))
        c3.metric("CUSIPs", var_df['cusip'].nunique())
        c4.metric("Participants", var_df['participant_id'].nunique())

    for title, table, cols in [
        ("Exposure by Participant", "REPORT_EXPOSURE_BY_PARTICIPANT", ["rank","participant_name","total_var_exposure","open_trade_count","pct_of_total_var"]),
        ("Top 5 Participants", "REPORT_TOP_PARTICIPANTS", ["rank","participant_name","total_var_exposure","open_trade_count","pct_of_total_var"]),
        ("Top 3 CUSIPs", "REPORT_TOP_CUSIPS", ["rank","cusip","security_name","total_var_exposure","pct_of_total_var"]),
    ]:
        st.header(title)
        rdf = load_report(table, run_id)
        if rdf.empty: st.info("No data.")
        else: st.dataframe(rdf[cols], use_container_width=True)

    st.header("Concentration Report")
    conc_df = load_report("REPORT_CONCENTRATION", run_id)
    if not conc_df.empty:
        for etype in ["CUSIP", "Participant"]:
            sub = conc_df[conc_df["entity_type"] == etype].sort_values("concentration_pct", ascending=False)
            flagged = sub[sub["is_flagged"] == True]
            if not flagged.empty: st.warning(f"{len(flagged)} {etype}(s) flagged > 10%")
            st.dataframe(sub[["entity_name","concentration_pct","is_flagged"]], use_container_width=True)

    st.header("🤖 Market Narrative")
    narrative = load_narrative(run_id)
    if narrative: st.markdown(narrative)
    else: st.info("No narrative for this run.")
