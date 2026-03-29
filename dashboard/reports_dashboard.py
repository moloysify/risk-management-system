"""Risk Reports Dashboard — Streamlit app for viewing VaR reports.

Run:  streamlit run dashboard/reports_dashboard.py --server.port 8504
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Risk Reports", layout="wide")
st.title("📊 Risk Reports Dashboard")


@st.cache_data(ttl=30)
def get_latest_run_id() -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT run_id FROM VAR_RESULTS ORDER BY run_timestamp DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


@st.cache_data(ttl=30)
def get_all_run_ids():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT run_id, MIN(run_timestamp) as ts
            FROM VAR_RESULTS GROUP BY run_id ORDER BY ts DESC LIMIT 20
        """)
        return [(r[0], str(r[1])) for r in cur.fetchall()]
    finally:
        conn.close()


def load_report(table: str, run_id: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {table} WHERE run_id = %s", conn, params=(run_id,))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


def load_narrative(run_id: str) -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT narrative_text, news_included FROM MARKET_NARRATIVES WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
        if row:
            return row[0]
        return ""
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Run selector
# ---------------------------------------------------------------------------
runs = get_all_run_ids()
has_var_runs = bool(runs)

if has_var_runs:
    run_options = {f"{rid} ({ts})": rid for rid, ts in runs}
    selected_label = st.sidebar.selectbox("VaR Run", list(run_options.keys()))
    run_id = run_options[selected_label]
    st.sidebar.caption(f"Run ID: `{run_id}`")
else:
    run_id = None
    st.sidebar.info("No VaR runs yet. Portfolio summary is shown below.")

# ---------------------------------------------------------------------------
# Portfolio Summary (pre-VaR view)
# ---------------------------------------------------------------------------
st.header("📁 Portfolio Summary")
st.caption("Current positions by participant before VaR computation.")


@st.cache_data(ttl=30)
def load_portfolio_summary():
    """Load portfolio-level summary from TRADES joined with reference and VaR data."""
    conn = get_connection()
    try:
        df = pd.read_sql("""
            WITH portfolio_trades AS (
                SELECT
                    COALESCE(em.participant_name, t.participant_id) AS portfolio,
                    COALESCE(em.entity_type, 'unknown')             AS description,
                    COUNT(DISTINCT t.trade_id)                      AS positions,
                    COUNT(DISTINCT t.cusip)                         AS securities,
                    SUM(t.price * t.quantity)                       AS gross_market_value,
                    SUM(CASE
                        WHEN t.quantity >= 0 THEN t.price * t.quantity
                        ELSE -1 * t.price * ABS(t.quantity)
                    END)                                            AS net_market_value,
                    t.participant_id
                FROM TRADES t
                LEFT JOIN ENTITY_MASTER em
                    ON t.participant_id = em.participant_id AND em.is_current = TRUE
                GROUP BY COALESCE(em.participant_name, t.participant_id),
                         COALESCE(em.entity_type, 'unknown'),
                         t.participant_id
            ),
            latest_var AS (
                SELECT participant_id, SUM(var_exposure) AS var_exposure
                FROM VAR_RESULTS
                WHERE run_id = (SELECT run_id FROM VAR_RESULTS ORDER BY run_timestamp DESC LIMIT 1)
                GROUP BY participant_id
            )
            SELECT
                pt.portfolio,
                pt.description,
                pt.positions,
                pt.securities,
                pt.gross_market_value,
                pt.net_market_value,
                COALESCE(lv.var_exposure, 0) AS var_exposure,
                CASE WHEN pt.gross_market_value > 0
                     THEN ROUND(COALESCE(lv.var_exposure, 0) / pt.gross_market_value * 100, 2)
                     ELSE 0 END AS var_pct_gross,
                CASE WHEN pt.net_market_value > 0
                     THEN ROUND(COALESCE(lv.var_exposure, 0) / pt.net_market_value * 100, 2)
                     ELSE 0 END AS var_pct_net,
                pt.participant_id
            FROM portfolio_trades pt
            LEFT JOIN latest_var lv ON pt.participant_id = lv.participant_id
            ORDER BY pt.gross_market_value DESC
        """, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


@st.cache_data(ttl=30)
def load_portfolio_positions(participant_id: str):
    """Load individual positions for a participant."""
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                t.cusip,
                COALESCE(sm.security_name, t.ticker) AS security_name,
                t.ticker,
                COALESCE(sm.sector, 'Unknown')       AS sector,
                SUM(t.quantity)                       AS total_quantity,
                ROUND(AVG(t.price), 2)                AS avg_price,
                ROUND(SUM(t.price * t.quantity), 2)   AS market_value,
                COUNT(*)                              AS trade_count,
                MIN(t.trade_timestamp)                AS first_trade,
                MAX(t.trade_timestamp)                AS last_trade
            FROM TRADES t
            LEFT JOIN SECURITY_MASTER sm
                ON t.cusip = sm.cusip AND sm.is_current = TRUE
            WHERE t.participant_id = %s
            GROUP BY t.cusip, COALESCE(sm.security_name, t.ticker), t.ticker, COALESCE(sm.sector, 'Unknown')
            ORDER BY market_value DESC
        """, conn, params=(participant_id,))
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


port_df = load_portfolio_summary()

if port_df.empty:
    st.info("No trades in the system yet. Generate data first.")
else:
    # Summary metrics
    tc1, tc2, tc3 = st.columns(3)
    tc1.metric("Total Portfolios", len(port_df))
    tc2.metric("Total Gross MV", f"${port_df['gross_market_value'].sum():,.0f}")
    tc3.metric("Total Net MV", f"${port_df['net_market_value'].sum():,.0f}")

    # Summary table
    display_df = port_df[["portfolio", "description", "positions", "securities", "gross_market_value", "net_market_value", "var_exposure", "var_pct_gross", "var_pct_net"]].copy()
    display_df["gross_market_value"] = display_df["gross_market_value"].apply(lambda x: f"${x:,.0f}")
    display_df["net_market_value"] = display_df["net_market_value"].apply(lambda x: f"${x:,.0f}")
    display_df["var_exposure"] = display_df["var_exposure"].apply(lambda x: f"${x:,.0f}")
    display_df["var_pct_gross"] = display_df["var_pct_gross"].apply(lambda x: f"{x:.2f}%")
    display_df["var_pct_net"] = display_df["var_pct_net"].apply(lambda x: f"{x:.2f}%")
    display_df.columns = ["Portfolio", "Description", "Positions", "Securities", "Gross MV", "Net MV", "VaR", "% of Gross MV", "% of Net MV"]
    st.dataframe(display_df, use_container_width=True)

    # Expandable per-portfolio detail
    for _, row in port_df.iterrows():
        with st.expander(f"📁 {row['portfolio']} — Gross: ${row['gross_market_value']:,.0f}"):
            positions = load_portfolio_positions(row["participant_id"])
            if positions.empty:
                st.info("No positions.")
            else:
                # Sector breakdown
                sector_mv = positions.groupby("sector")["market_value"].sum().sort_values(ascending=False)
                scol1, scol2 = st.columns([1, 2])
                with scol1:
                    st.caption("Sector breakdown:")
                    for sector, mv in sector_mv.items():
                        pct = mv / positions["market_value"].sum() * 100 if positions["market_value"].sum() > 0 else 0
                        st.text(f"  {sector}: ${mv:,.0f} ({pct:.1f}%)")
                with scol2:
                    st.bar_chart(sector_mv)

                # Positions table
                pos_display = positions[["security_name", "ticker", "sector", "total_quantity", "avg_price", "market_value", "trade_count"]].copy()
                pos_display["market_value"] = pos_display["market_value"].apply(lambda x: f"${x:,.0f}")
                pos_display.columns = ["Security", "Ticker", "Sector", "Quantity", "Avg Price", "Market Value", "Trades"]
                st.dataframe(pos_display, use_container_width=True)

# ---------------------------------------------------------------------------
# VaR Summary (only if runs exist)
# ---------------------------------------------------------------------------
if not has_var_runs:
    st.info("No VaR runs found yet. Generate data and run COMPUTE_VAR() to see risk reports below.")
else:
    st.header("VaR Summary")
    var_df = load_report("VAR_RESULTS", run_id)
    if not var_df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total VaR Exposure", f"${var_df['var_exposure'].sum():,.2f}")
        c2.metric("Positions", len(var_df))
        c3.metric("Unique CUSIPs", var_df['cusip'].nunique())
        c4.metric("Unique Participants", var_df['participant_id'].nunique())

    # Exposure by Participant
    st.header("Exposure by Participant")
    exp_df = load_report("REPORT_EXPOSURE_BY_PARTICIPANT", run_id)
    if exp_df.empty:
        st.info("No data.")
    else:
        st.dataframe(
            exp_df[["rank", "participant_name", "total_var_exposure", "open_trade_count", "pct_of_total_var"]],
            use_container_width=True,
        )
        st.bar_chart(exp_df.set_index("participant_name")["total_var_exposure"])

    # Top 5 Participants
    st.header("Top 5 Participants by Exposure")
    top_p = load_report("REPORT_TOP_PARTICIPANTS", run_id)
    if top_p.empty:
        st.info("No data.")
    else:
        st.dataframe(
            top_p[["rank", "participant_name", "total_var_exposure", "open_trade_count", "pct_of_total_var"]],
            use_container_width=True,
        )

    # Top 3 CUSIPs
    st.header("Top 3 CUSIPs by Exposure")
    top_c = load_report("REPORT_TOP_CUSIPS", run_id)
    if top_c.empty:
        st.info("No data.")
    else:
        st.dataframe(
            top_c[["rank", "cusip", "security_name", "total_var_exposure", "pct_of_total_var"]],
            use_container_width=True,
        )

    # Concentration Report
    st.header("Concentration Report")
    conc_df = load_report("REPORT_CONCENTRATION", run_id)
    if conc_df.empty:
        st.info("No data.")
    else:
        tab_cusip, tab_part = st.tabs(["By CUSIP", "By Participant"])
        with tab_cusip:
            cusip_conc = conc_df[conc_df["entity_type"] == "CUSIP"].sort_values("concentration_pct", ascending=False)
            flagged = cusip_conc[cusip_conc["is_flagged"] == True]
            if not flagged.empty:
                st.warning(f"{len(flagged)} CUSIP(s) flagged with concentration > 10%")
            st.dataframe(
                cusip_conc[["entity_name", "concentration_pct", "is_flagged"]],
                use_container_width=True,
            )
        with tab_part:
            part_conc = conc_df[conc_df["entity_type"] == "Participant"].sort_values("concentration_pct", ascending=False)
            flagged_p = part_conc[part_conc["is_flagged"] == True]
            if not flagged_p.empty:
                st.warning(f"{len(flagged_p)} participant(s) flagged with concentration > 10%")
            st.dataframe(
                part_conc[["entity_name", "concentration_pct", "is_flagged"]],
                use_container_width=True,
            )

    # High-Volume CUSIPs
    st.header("High-Volume CUSIPs")
    hv_df = load_report("REPORT_HIGH_VOLUME_CUSIP", run_id)
    if hv_df.empty:
        st.info("No high-volume CUSIPs detected for this run.")
    else:
        # Enrich with ticker, name, and current price from Snowflake
        conn = get_connection()
        try:
            enrich_df = pd.read_sql("""
                SELECT sm.cusip, t.ticker,
                       sm.security_name,
                       md.price AS current_price
                FROM SECURITY_MASTER sm
                LEFT JOIN (SELECT DISTINCT cusip, ticker FROM TRADES) t ON sm.cusip = t.cusip
                LEFT JOIN (
                    SELECT cusip, price
                    FROM MARKET_DATA
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY cusip ORDER BY data_date DESC, ingestion_timestamp DESC) = 1
                ) md ON sm.cusip = md.cusip
                WHERE sm.is_current = TRUE
            """, conn)
            enrich_df.columns = [c.lower() for c in enrich_df.columns]
            hv_enriched = hv_df.merge(enrich_df, on="cusip", how="left")
        except Exception:
            hv_enriched = hv_df
        finally:
            conn.close()

        display_cols = ["cusip"]
        if "ticker" in hv_enriched.columns:
            display_cols.append("ticker")
        if "security_name" in hv_enriched.columns:
            display_cols.append("security_name")
        if "current_price" in hv_enriched.columns:
            display_cols.append("current_price")
        display_cols.extend(["current_volume", "historical_avg_volume", "percentile_rank"])
        st.dataframe(hv_enriched[display_cols], use_container_width=True)

    # High-Exposure Participants
    st.header("High-Exposure Participants")
    he_df = load_report("REPORT_HIGH_EXPOSURE_PARTICIPANT", run_id)
    if he_df.empty:
        st.info("No high-exposure participants detected for this run.")
    else:
        st.dataframe(
            he_df[["participant_name", "total_var_exposure", "open_trade_count"]],
            use_container_width=True,
        )

    # Market Narrative (Cortex)
    st.header("🤖 Market Condition Narrative")
    narrative = load_narrative(run_id)
    if narrative:
        st.markdown(narrative)
    else:
        st.info("No narrative generated for this run.")

# Auto-refresh disabled to preserve user state
