"""Reports Dashboard page."""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Risk Reports", layout="wide")
st.title("📊 Risk Reports")

def load_report(table, run_id):
    conn = get_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {table} WHERE run_id = %s", conn, params=(run_id,))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally: conn.close()

# Portfolio Summary
st.header("📁 Portfolio Summary")
@st.cache_data(ttl=30)
def load_portfolios():
    conn = get_connection()
    try:
        df = pd.read_sql("""
            WITH pt AS (SELECT COALESCE(em.participant_name,t.participant_id) AS portfolio, COALESCE(em.entity_type,'unknown') AS description,
                COUNT(DISTINCT t.trade_id) AS positions, COUNT(DISTINCT t.cusip) AS securities,
                SUM(t.price*t.quantity) AS gmv, SUM(CASE WHEN t.quantity>=0 THEN t.price*t.quantity ELSE -t.price*ABS(t.quantity) END) AS nmv, t.participant_id
                FROM TRADES t LEFT JOIN ENTITY_MASTER em ON t.participant_id=em.participant_id AND em.is_current=TRUE GROUP BY 1,2,t.participant_id),
            lv AS (SELECT participant_id, SUM(var_exposure) AS var FROM VAR_RESULTS WHERE run_id=(SELECT run_id FROM VAR_RESULTS ORDER BY run_timestamp DESC LIMIT 1) GROUP BY 1)
            SELECT pt.*, COALESCE(lv.var,0) AS var, CASE WHEN gmv>0 THEN ROUND(COALESCE(lv.var,0)/gmv*100,2) ELSE 0 END AS vpg,
                CASE WHEN nmv>0 THEN ROUND(COALESCE(lv.var,0)/nmv*100,2) ELSE 0 END AS vpn
            FROM pt LEFT JOIN lv ON pt.participant_id=lv.participant_id ORDER BY gmv DESC""", conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally: conn.close()

pdf = load_portfolios()
if pdf.empty: st.info("No trades yet.")
else:
    c1,c2,c3 = st.columns(3)
    c1.metric("Portfolios", len(pdf)); c2.metric("Gross MV", f"${pdf['gmv'].sum():,.0f}"); c3.metric("Net MV", f"${pdf['nmv'].sum():,.0f}")
    d = pdf[["portfolio","description","positions","securities","gmv","nmv","var","vpg","vpn"]].copy()
    d["gmv"]=d["gmv"].apply(lambda x:f"${x:,.0f}"); d["nmv"]=d["nmv"].apply(lambda x:f"${x:,.0f}")
    d["var"]=d["var"].apply(lambda x:f"${x:,.0f}"); d["vpg"]=d["vpg"].apply(lambda x:f"{x:.2f}%"); d["vpn"]=d["vpn"].apply(lambda x:f"{x:.2f}%")
    d.columns=["Portfolio","Description","Positions","Securities","Gross MV","Net MV","VaR","% Gross","% Net"]
    st.dataframe(d, use_container_width=True)

# VaR Reports
@st.cache_data(ttl=30)
def get_runs():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT run_id, MIN(run_timestamp) FROM VAR_RESULTS GROUP BY 1 ORDER BY 2 DESC LIMIT 20")
        return [(r[0],str(r[1])) for r in cur.fetchall()]
    finally: conn.close()

runs = get_runs()
if not runs: st.info("No VaR runs yet.")
else:
    opts = {f"{r[0]} ({r[1]})":r[0] for r in runs}
    rid = opts[st.sidebar.selectbox("VaR Run", list(opts.keys()))]

    vdf = load_report("VAR_RESULTS", rid)
    if not vdf.empty:
        st.header("VaR Summary")
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total VaR", f"${vdf['var_exposure'].sum():,.2f}"); c2.metric("Positions", len(vdf))
        c3.metric("CUSIPs", vdf['cusip'].nunique()); c4.metric("Participants", vdf['participant_id'].nunique())

    for title, tbl, cols in [
        ("Exposure by Participant","REPORT_EXPOSURE_BY_PARTICIPANT",["rank","participant_name","total_var_exposure","open_trade_count","pct_of_total_var"]),
        ("Top 5 Participants","REPORT_TOP_PARTICIPANTS",["rank","participant_name","total_var_exposure","open_trade_count","pct_of_total_var"]),
        ("Top 3 CUSIPs","REPORT_TOP_CUSIPS",["rank","cusip","security_name","total_var_exposure","pct_of_total_var"]),
    ]:
        st.header(title)
        rdf = load_report(tbl, rid)
        if rdf.empty: st.info("No data.")
        else: st.dataframe(rdf[cols], use_container_width=True)

    st.header("🤖 Market Narrative")
    conn = get_connection()
    try:
        cur = conn.cursor(); cur.execute("SELECT narrative_text FROM MARKET_NARRATIVES WHERE run_id=%s",(rid,))
        row = cur.fetchone()
        if row: st.markdown(row[0])
        else: st.info("No narrative.")
    finally: conn.close()
