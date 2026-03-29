"""Simulator Management Dashboard — Streamlit app for launching and managing
data simulators.

Run:  streamlit run dashboard/simulator_dashboard.py

Provides controls to:
- Launch individual simulators (Trade, Market Data, Security Master, Entity Master)
  or all at once
- Configure each simulator: mode, dataset size, volume, frequency
- View running simulator status and stop them
- View SIMULATION_LOG history
"""
from __future__ import annotations

import subprocess
import sys
import os
import signal
import datetime as dt

import streamlit as st
import pandas as pd

# Add project root to path so we can import dashboard.db
sys.path.insert(0, os.path.dirname(__file__))
from db import get_connection

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Simulator Manager", layout="wide")
st.title("🎲 Simulator Management Dashboard")

# ---------------------------------------------------------------------------
# Session state for tracking running simulators
# ---------------------------------------------------------------------------
if "simulators" not in st.session_state:
    st.session_state.simulators = {}  # key: data_type, value: {proc, config}

SIMULATOR_TYPES = {
    "trade": "Trade Simulator",
    "market_data": "Market Data Simulator",
    "security_master": "Security Master Simulator",
    "entity_master": "Entity Master Simulator",
}


# ---------------------------------------------------------------------------
# Helper: launch a simulator subprocess
# ---------------------------------------------------------------------------

def _build_cmd(data_type: str, mode: str, dataset_size: str,
               volume: int, frequency: int, kafka_servers: str) -> list[str]:
    """Build the CLI command to launch a simulator."""
    cmd = [
        sys.executable, "-m", "simulator.simulator",
        "--data-types", data_type,
        "--mode", mode,
        "--dataset-size", dataset_size,
        "--volume", str(volume),
        "--frequency", str(frequency),
    ]
    if kafka_servers:
        cmd.extend(["--kafka-bootstrap-servers", kafka_servers])
    return cmd


def launch_simulator(data_type: str, mode: str, dataset_size: str,
                     volume: int, frequency: int, kafka_servers: str) -> bool:
    """Launch a simulator subprocess. Returns True on success."""
    if data_type in st.session_state.simulators:
        proc = st.session_state.simulators[data_type].get("proc")
        if proc and proc.poll() is None:
            return False  # already running

    cmd = _build_cmd(data_type, mode, dataset_size, volume, frequency, kafka_servers)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.dirname(__file__)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        st.session_state.simulators[data_type] = {
            "proc": proc,
            "pid": proc.pid,
            "mode": mode,
            "dataset_size": dataset_size,
            "volume": volume,
            "frequency": frequency,
            "started_at": dt.datetime.utcnow().isoformat(),
        }
        return True
    except Exception:
        return False


def stop_simulator(data_type: str) -> bool:
    """Stop a running simulator. Returns True on success."""
    info = st.session_state.simulators.get(data_type)
    if not info:
        return False
    proc = info.get("proc")
    if proc is None or proc.poll() is not None:
        st.session_state.simulators.pop(data_type, None)
        return True
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    except Exception:
        pass
    st.session_state.simulators.pop(data_type, None)
    return True


def is_running(data_type: str) -> bool:
    """Check if a simulator is currently running."""
    info = st.session_state.simulators.get(data_type)
    if not info:
        return False
    proc = info.get("proc")
    return proc is not None and proc.poll() is None


# ---------------------------------------------------------------------------
# Sidebar — Global settings
# ---------------------------------------------------------------------------
st.sidebar.header("Global Settings")
kafka_servers = st.sidebar.text_input(
    "Kafka Bootstrap Servers",
    value=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
)

# ---------------------------------------------------------------------------
# Section 1: Individual Simulator Controls
# ---------------------------------------------------------------------------
st.header("Simulator Controls")

tabs = st.tabs(list(SIMULATOR_TYPES.values()) + ["Launch All"])

for i, (dtype, label) in enumerate(SIMULATOR_TYPES.items()):
    with tabs[i]:
        running = is_running(dtype)
        if running:
            st.success(f"🟢 {label} is running (PID: {st.session_state.simulators[dtype]['pid']})")
            info = st.session_state.simulators[dtype]
            st.caption(
                f"Mode: {info['mode']} | Size: {info['dataset_size']} | "
                f"Volume: {info['volume']} | Frequency: {info['frequency']}s | "
                f"Started: {info['started_at']}"
            )
            if st.button(f"Stop {label}", key=f"stop_{dtype}"):
                stop_simulator(dtype)
                st.rerun()
        else:
            st.info(f"⚪ {label} is stopped")
            col1, col2 = st.columns(2)
            with col1:
                mode = st.selectbox("Mode", ["realtime", "batch"], key=f"mode_{dtype}")
                dataset_size = st.selectbox(
                    "Dataset Size", ["small", "medium", "large"],
                    key=f"size_{dtype}",
                )
            with col2:
                volume_defaults = {"small": 50, "medium": 500, "large": 5000}
                volume = st.number_input(
                    "Volume (records/batch)",
                    min_value=1, max_value=100000,
                    value=volume_defaults.get(dataset_size, 50),
                    key=f"vol_{dtype}",
                )
                frequency = st.number_input(
                    "Frequency (seconds)",
                    min_value=1, max_value=3600,
                    value=10,
                    key=f"freq_{dtype}",
                )

            if st.button(f"Launch {label}", key=f"launch_{dtype}", type="primary"):
                ok = launch_simulator(dtype, mode, dataset_size, volume, frequency, kafka_servers)
                if ok:
                    st.success(f"Launched {label}")
                    st.rerun()
                else:
                    st.error(f"Failed to launch {label} (may already be running)")

# "Launch All" tab
with tabs[len(SIMULATOR_TYPES)]:
    st.subheader("Launch All Simulators")
    col1, col2 = st.columns(2)
    with col1:
        all_mode = st.selectbox("Mode", ["realtime", "batch"], key="all_mode")
        all_size = st.selectbox("Dataset Size", ["small", "medium", "large"], key="all_size")
    with col2:
        all_vol_defaults = {"small": 50, "medium": 500, "large": 5000}
        all_volume = st.number_input(
            "Volume", min_value=1, max_value=100000,
            value=all_vol_defaults.get(all_size, 50), key="all_vol",
        )
        all_freq = st.number_input(
            "Frequency (seconds)", min_value=1, max_value=3600,
            value=10, key="all_freq",
        )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🚀 Launch All", type="primary", key="launch_all"):
            launched = 0
            for dtype in SIMULATOR_TYPES:
                if not is_running(dtype):
                    ok = launch_simulator(dtype, all_mode, all_size, all_volume, all_freq, kafka_servers)
                    if ok:
                        launched += 1
            st.success(f"Launched {launched} simulator(s)")
            st.rerun()
    with c2:
        if st.button("🛑 Stop All", key="stop_all"):
            stopped = 0
            for dtype in list(st.session_state.simulators.keys()):
                if stop_simulator(dtype):
                    stopped += 1
            st.success(f"Stopped {stopped} simulator(s)")
            st.rerun()


# ---------------------------------------------------------------------------
# Section 2: Running Simulators Status
# ---------------------------------------------------------------------------
st.header("Running Simulators")

running_data = []
for dtype, info in st.session_state.simulators.items():
    proc = info.get("proc")
    status = "Running" if proc and proc.poll() is None else "Stopped"
    running_data.append({
        "Simulator": SIMULATOR_TYPES.get(dtype, dtype),
        "Status": "🟢 " + status if status == "Running" else "🔴 " + status,
        "PID": info.get("pid", "—"),
        "Mode": info.get("mode", "—"),
        "Dataset Size": info.get("dataset_size", "—"),
        "Volume": info.get("volume", "—"),
        "Frequency (s)": info.get("frequency", "—"),
        "Started At": info.get("started_at", "—"),
    })

if running_data:
    st.dataframe(pd.DataFrame(running_data), use_container_width=True)
else:
    st.info("No simulators are currently running.")


# ---------------------------------------------------------------------------
# Section 3: Portfolio Simulation (Yahoo Finance)
# ---------------------------------------------------------------------------
st.header("📈 Portfolio Simulation")
st.caption("Each portfolio = one participant/trading desk. Build portfolios manually or generate random ones.")

# Add project root to path for simulator imports
_project_root = os.path.dirname(os.path.dirname(__file__))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from simulator.portfolio_profiles import PORTFOLIO_PROFILES, generate_portfolio_data
import json
import random
import uuid

# Session state for portfolios
if "portfolios" not in st.session_state:
    st.session_state.portfolios = {}

# Two modes
mode_tab1, mode_tab2 = st.tabs(["📝 Option 1: Build Portfolios Manually", "🎲 Option 2: Generate Random Portfolios"])

# =========================================================================
# Option 1: Build portfolios one by one
# =========================================================================
with mode_tab1:
    st.subheader("Add a Portfolio")

    pcol_name, pcol_strategy = st.columns(2)
    with pcol_name:
        portfolio_name = st.text_input("Portfolio Name (= Participant)", placeholder="e.g. Alpha Tech Fund, Buffett Clone")
    with pcol_strategy:
        profile_options = {v["label"]: k for k, v in PORTFOLIO_PROFILES.items() if k != "custom"}
        profile_options["Custom (enter your own tickers)"] = "custom"
        selected_label = st.selectbox("Strategy", list(profile_options.keys()))
        selected_profile = profile_options[selected_label]

    custom_tickers_input = None
    if selected_profile == "custom":
        custom_tickers_input = st.text_input(
            "Enter tickers (comma-separated)",
            placeholder="AAPL, MSFT, GOOGL, TSLA, JPM",
            key="custom_tickers_new",
        )

    pcol1, pcol2 = st.columns(2)
    with pcol1:
        trades_per_ticker = st.number_input("Trades per ticker", min_value=1, max_value=100, value=10, key="port_tpt")
    with pcol2:
        history_days = st.number_input("History days (for market data)", min_value=5, max_value=365, value=30, key="port_hist")

    if st.button("➕ Add Portfolio", key="add_portfolio"):
        if not portfolio_name or not portfolio_name.strip():
            st.error("Please enter a portfolio name.")
        elif portfolio_name in st.session_state.portfolios:
            st.error(f"Portfolio '{portfolio_name}' already exists.")
        else:
            custom_tickers = None
            if selected_profile == "custom" and custom_tickers_input:
                custom_tickers = [t.strip().upper() for t in custom_tickers_input.split(",") if t.strip()]

            profile = PORTFOLIO_PROFILES.get(selected_profile, {})
            tickers = custom_tickers if (selected_profile == "custom" and custom_tickers) else profile.get("tickers", [])

            st.session_state.portfolios[portfolio_name] = {
                "strategy": selected_profile,
                "strategy_label": selected_label,
                "tickers": tickers,
                "participant_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, portfolio_name)),
                "trades_per_ticker": trades_per_ticker,
                "history_days": history_days,
                "status": "ready",
                "last_result": None,
            }
            st.success(f"Added portfolio '{portfolio_name}' with {len(tickers)} tickers ({selected_label})")
            st.rerun()

# =========================================================================
# Option 2: Generate random portfolios
# =========================================================================
with mode_tab2:
    st.subheader("Generate Random Portfolios")
    st.caption("Randomly assigns strategies and tickers to N portfolios.")

    rcol1, rcol2, rcol3 = st.columns(3)
    with rcol1:
        num_random = st.number_input("Number of portfolios", min_value=1, max_value=50, value=5, key="rand_num")
    with rcol2:
        rand_tpt = st.number_input("Trades per ticker", min_value=1, max_value=100, value=10, key="rand_tpt")
    with rcol3:
        rand_hist = st.number_input("History days", min_value=5, max_value=365, value=30, key="rand_hist")

    RANDOM_PORTFOLIO_NAMES = [
        "Alpha Capital", "Beta Trading", "Gamma Investments", "Delta Securities",
        "Epsilon Fund", "Zeta Partners", "Theta Asset Mgmt", "Iota Strategies",
        "Kappa Holdings", "Lambda Group", "Mu Ventures", "Nu Capital",
        "Xi Trading Desk", "Omicron Fund", "Pi Investments", "Rho Securities",
        "Sigma Partners", "Tau Asset Mgmt", "Upsilon Capital", "Phi Strategies",
        "Chi Holdings", "Psi Ventures", "Omega Fund", "Apex Trading",
        "Summit Capital", "Pinnacle Fund", "Horizon Partners", "Zenith Investments",
        "Meridian Securities", "Vanguard Desk", "Citadel Clone", "Renaissance Sim",
        "Bridgewater Lite", "Two Sigma Mini", "Point72 Echo", "Millennium Sim",
        "Balyasny Clone", "Sculptor Desk", "Viking Global Sim", "Tiger Cub Fund",
        "Coatue Sim", "D1 Capital Echo", "Lone Pine Sim", "Dragoneer Fund",
        "Altimeter Desk", "Whale Rock Sim", "Light Street Fund", "Maverick Sim",
        "Durable Capital", "Steadfast Fund",
    ]

    available_strategies = [k for k in PORTFOLIO_PROFILES.keys() if k != "custom"]

    if st.button("🎲 Generate Random Portfolios", type="primary", key="gen_random"):
        used_names = set(st.session_state.portfolios.keys())
        available_names = [n for n in RANDOM_PORTFOLIO_NAMES if n not in used_names]
        random.shuffle(available_names)

        created = 0
        for i in range(min(num_random, len(available_names))):
            name = available_names[i]
            strategy_key = random.choice(available_strategies)
            profile = PORTFOLIO_PROFILES[strategy_key]

            st.session_state.portfolios[name] = {
                "strategy": strategy_key,
                "strategy_label": profile["label"],
                "tickers": profile["tickers"],
                "participant_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, name)),
                "trades_per_ticker": rand_tpt,
                "history_days": rand_hist,
                "status": "ready",
                "last_result": None,
            }
            created += 1

        st.success(f"Created {created} random portfolios with random strategies.")
        st.rerun()

# =========================================================================
# Portfolio List (shared across both modes)
# =========================================================================
if st.session_state.portfolios:
    st.subheader(f"Your Portfolios ({len(st.session_state.portfolios)})")

    # Summary table
    summary_rows = []
    for pname, pinfo in st.session_state.portfolios.items():
        summary_rows.append({
            "Portfolio": pname,
            "Strategy": pinfo["strategy_label"],
            "Tickers": len(pinfo["tickers"]),
            "Trades/Ticker": pinfo["trades_per_ticker"],
            "Status": pinfo["status"],
        })
    st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

    # Expandable details per portfolio
    for pname, pinfo in list(st.session_state.portfolios.items()):
        with st.expander(f"📁 {pname} — {pinfo['strategy_label']}", expanded=False):
            st.text(f"Tickers: {', '.join(pinfo['tickers'][:20])}{'...' if len(pinfo['tickers']) > 20 else ''}")
            st.caption(f"Participant ID: {pinfo['participant_id']}")

            if pinfo.get("last_result"):
                r = pinfo["last_result"]
                st.info(f"Last run: {r['trades']} trades, {r['market_data']} market data, {r['ref_data']} ref data")

            bcol1, bcol2 = st.columns(2)
            with bcol1:
                if st.button(f"🚀 Generate & Publish", key=f"gen_{pname}", type="primary"):
                    with st.spinner(f"Fetching Yahoo Finance data for '{pname}'..."):
                        try:
                            data = generate_portfolio_data(
                                profile_key=pinfo["strategy"],
                                custom_tickers=pinfo["tickers"] if pinfo["strategy"] == "custom" else None,
                                num_participants=1,
                                trades_per_ticker=pinfo["trades_per_ticker"],
                                history_days=pinfo["history_days"],
                            )
                            # Override participant to be this portfolio
                            for trade in data["trades"]:
                                trade["payload"]["participant_id"] = pinfo["participant_id"]
                            data["entity_master"] = [{
                                "data_type": "entity_master",
                                "payload": {
                                    "participant_id": pinfo["participant_id"],
                                    "participant_name": pname,
                                    "entity_type": pinfo["strategy_label"],
                                    "risk_limit": 10000000,
                                },
                            }]

                            from confluent_kafka import Producer
                            producer = Producer({"bootstrap.servers": kafka_servers})
                            pub = {"trades": 0, "market_data": 0, "ref_data": 0}
                            for rec in data["security_master"]:
                                producer.produce("reference-data", value=json.dumps(rec).encode("utf-8"))
                                pub["ref_data"] += 1
                            for rec in data["entity_master"]:
                                producer.produce("reference-data", value=json.dumps(rec).encode("utf-8"))
                                pub["ref_data"] += 1
                            for rec in data["market_data"]:
                                producer.produce("market-data", value=json.dumps(rec).encode("utf-8"))
                                pub["market_data"] += 1
                            for rec in data["trades"]:
                                producer.produce("trade-ingestion", value=json.dumps(rec).encode("utf-8"))
                                pub["trades"] += 1
                            producer.flush(timeout=30)

                            pinfo["status"] = "published"
                            pinfo["last_result"] = pub
                            st.success(f"'{pname}' published: {pub['trades']} trades, {pub['market_data']} market data, {pub['ref_data']} ref data")
                        except Exception as exc:
                            st.error(f"Failed: {exc}")
            with bcol2:
                if st.button(f"🗑️ Remove", key=f"del_{pname}"):
                    del st.session_state.portfolios[pname]
                    st.rerun()

    # Bulk actions
    st.divider()
    bulk1, bulk2, bulk3 = st.columns(3)
    with bulk1:
        if st.button("🚀 Generate All Portfolios", type="primary", key="gen_all_portfolios"):
            from confluent_kafka import Producer
            producer = Producer({"bootstrap.servers": kafka_servers})
            total = {"trades": 0, "market_data": 0, "ref_data": 0}

            for pname, pinfo in st.session_state.portfolios.items():
                with st.spinner(f"Generating '{pname}'..."):
                    try:
                        data = generate_portfolio_data(
                            profile_key=pinfo["strategy"],
                            custom_tickers=pinfo["tickers"] if pinfo["strategy"] == "custom" else None,
                            num_participants=1,
                            trades_per_ticker=pinfo["trades_per_ticker"],
                            history_days=pinfo["history_days"],
                        )
                        for trade in data["trades"]:
                            trade["payload"]["participant_id"] = pinfo["participant_id"]
                        data["entity_master"] = [{
                            "data_type": "entity_master",
                            "payload": {
                                "participant_id": pinfo["participant_id"],
                                "participant_name": pname,
                                "entity_type": pinfo["strategy_label"],
                                "risk_limit": 10000000,
                            },
                        }]
                        for rec in data["security_master"]:
                            producer.produce("reference-data", value=json.dumps(rec).encode("utf-8"))
                            total["ref_data"] += 1
                        for rec in data["entity_master"]:
                            producer.produce("reference-data", value=json.dumps(rec).encode("utf-8"))
                            total["ref_data"] += 1
                        for rec in data["market_data"]:
                            producer.produce("market-data", value=json.dumps(rec).encode("utf-8"))
                            total["market_data"] += 1
                        for rec in data["trades"]:
                            producer.produce("trade-ingestion", value=json.dumps(rec).encode("utf-8"))
                            total["trades"] += 1
                        pinfo["status"] = "published"
                        pinfo["last_result"] = {
                            "trades": len(data["trades"]),
                            "market_data": len(data["market_data"]),
                            "ref_data": len(data["security_master"]) + len(data["entity_master"]),
                        }
                    except Exception as exc:
                        st.error(f"Failed for '{pname}': {exc}")

            producer.flush(timeout=30)
            st.success(f"All portfolios published: {total['trades']} trades, {total['market_data']} market data, {total['ref_data']} ref data")

    with bulk2:
        if st.button("🗑️ Remove All Portfolios", key="del_all_portfolios"):
            st.session_state.portfolios = {}
            st.rerun()

    with bulk3:
        st.metric("Total Portfolios", len(st.session_state.portfolios))

# ---------------------------------------------------------------------------
# Section 4: Historical Stress Testing
# ---------------------------------------------------------------------------
st.header("🔥 Historical Stress Testing")
st.caption("Test your portfolios against real historical market events using Yahoo Finance data.")

from simulator.historical_events import HISTORICAL_EVENTS, run_stress_test

# Pick portfolios to stress test — from session state OR from Snowflake
# Load portfolios from Snowflake (actual trades in the system)
@st.cache_data(ttl=30)
def load_snowflake_portfolios():
    """Load portfolios from Snowflake TRADES + ENTITY_MASTER with their tickers."""
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    t.participant_id,
                    COALESCE(em.participant_name, t.participant_id) AS portfolio_name,
                    COALESCE(em.entity_type, 'unknown') AS description,
                    LISTAGG(DISTINCT t.ticker, ',') WITHIN GROUP (ORDER BY t.ticker) AS tickers
                FROM TRADES t
                LEFT JOIN ENTITY_MASTER em
                    ON t.participant_id = em.participant_id AND em.is_current = TRUE
                GROUP BY t.participant_id, COALESCE(em.participant_name, t.participant_id), COALESCE(em.entity_type, 'unknown')
                ORDER BY portfolio_name
            """)
            rows = cur.fetchall()
            result = {}
            for row in rows:
                result[row[1]] = {
                    "participant_id": row[0],
                    "strategy_label": row[2],
                    "tickers": [t.strip() for t in row[3].split(",") if t.strip()],
                }
            return result
        finally:
            conn.close()
    except Exception:
        return {}

sf_portfolios = load_snowflake_portfolios()

# Merge: session state portfolios + Snowflake portfolios
# Build display labels with description
all_portfolios = {}
portfolio_labels = {}  # label -> key mapping
for pname, pinfo in sf_portfolios.items():
    key = f"[DB] {pname}"
    label = f"[DB] {pname} — {pinfo.get('strategy_label', pinfo.get('description', ''))}"
    all_portfolios[key] = pinfo
    portfolio_labels[label] = key
for pname, pinfo in st.session_state.portfolios.items():
    key = f"[Local] {pname}"
    label = f"[Local] {pname} — {pinfo.get('strategy_label', '')}"
    all_portfolios[key] = pinfo
    portfolio_labels[label] = key

if all_portfolios:
    stress_mode = st.radio("Stress test scope", ["Single portfolio", "Multiple portfolios", "All portfolios"], key="stress_mode", horizontal=True)

    if stress_mode == "Single portfolio":
        selected_label = st.selectbox(
            "Portfolio to stress test",
            list(portfolio_labels.keys()),
            key="stress_portfolio",
        )
        sel_key = portfolio_labels[selected_label]
        selected_portfolios = {sel_key: all_portfolios[sel_key]}
    elif stress_mode == "Multiple portfolios":
        selected_labels = st.multiselect(
            "Select portfolios",
            list(portfolio_labels.keys()),
            key="stress_multi_portfolio",
        )
        selected_portfolios = {portfolio_labels[l]: all_portfolios[portfolio_labels[l]] for l in selected_labels}
    else:
        selected_portfolios = dict(all_portfolios)
        st.caption(f"All {len(selected_portfolios)} portfolios selected.")

    if selected_portfolios:
        for pname, pinfo in selected_portfolios.items():
            st.caption(f"📁 {pname}: {', '.join(pinfo['tickers'][:10])}{'...' if len(pinfo['tickers']) > 10 else ''}")
else:
    selected_portfolios = {}
    st.warning("No portfolios found. Create portfolios above or generate data first.")

# Pick events — use session state so quick select buttons work
if "stress_event_selection" not in st.session_state:
    st.session_state.stress_event_selection = []

event_options = {v["label"]: k for k, v in HISTORICAL_EVENTS.items()}

# Shortcut buttons (must come BEFORE the multiselect so they update session state first)
st.caption("Quick select:")
qcol1, qcol2, qcol3 = st.columns(3)
with qcol1:
    if st.button("All Gulf Wars", key="quick_gulf"):
        st.session_state.stress_event_selection = [
            HISTORICAL_EVENTS["yom_kippur_oil_embargo_1973"]["label"],
            HISTORICAL_EVENTS["iranian_revolution_1979"]["label"],
            HISTORICAL_EVENTS["iran_iraq_war"]["label"],
            HISTORICAL_EVENTS["gulf_war_1"]["label"],
            HISTORICAL_EVENTS["gulf_war_2"]["label"],
            HISTORICAL_EVENTS["syrian_civil_war_houthi"]["label"],
            HISTORICAL_EVENTS["iran_israel_2026"]["label"],
        ]
        st.rerun()
with qcol2:
    if st.button("All Crashes", key="quick_crashes"):
        st.session_state.stress_event_selection = [
            HISTORICAL_EVENTS[k]["label"]
            for k in ["dotcom_crash", "gfc_2008", "covid_crash", "black_monday_1987"]
        ]
        st.rerun()
with qcol3:
    if st.button("All Events", key="quick_all"):
        st.session_state.stress_event_selection = list(event_options.keys())
        st.rerun()

selected_events = st.multiselect(
    "Historical events",
    list(event_options.keys()),
    default=st.session_state.stress_event_selection,
    key="stress_events",
)
# Sync back to session state
st.session_state.stress_event_selection = selected_events
selected_event_keys = [event_options[e] for e in selected_events]

horizon_months = st.slider("Analysis horizon after event (months)", min_value=1, max_value=24, value=6, key="stress_horizon")
initial_investment = st.number_input("Initial investment ($)", min_value=10000, max_value=100000000, value=1000000, step=100000, key="stress_invest")

if st.button("🔥 Run Stress Test", type="primary", key="run_stress"):
    if not selected_portfolios:
        st.error("No portfolios selected. Create portfolios first in the Portfolio Simulation section above.")
    elif not selected_event_keys:
        st.error("Select at least one historical event.")
    else:
        # Run stress test for each selected portfolio
        for pname, pinfo in selected_portfolios.items():
            st.subheader(f"📁 {pname} — {pinfo['strategy_label']}")
            stress_tickers = pinfo["tickers"]

            with st.spinner(f"Running stress test for '{pname}' across {len(selected_event_keys)} event(s)..."):
                try:
                    results = run_stress_test(
                        tickers=stress_tickers,
                        event_keys=selected_event_keys,
                        horizon_months=horizon_months,
                        initial_investment=initial_investment,
                    )

                    # Summary table for this portfolio
                    summary_rows = []
                    for ek, r in results.items():
                        if "error" in r:
                            summary_rows.append({
                                "Event": r["event"]["label"],
                                "Return": "N/A",
                                "Max Drawdown": "N/A",
                                "Final Value": "N/A",
                                "Recovery": r.get("error", "N/A"),
                                "Recovery (months)": "N/A",
                            })
                        else:
                            summary_rows.append({
                                "Event": r["event"]["label"],
                                "Return": f"{r['total_return_pct']:+.1f}%",
                                "Max Drawdown": f"-{r['max_drawdown_pct']:.1f}%",
                                "Final Value": f"${r['final_value']:,.0f}",
                                "Recovery": r["recovery_date"] or "Not recovered",
                                "Recovery (months)": f"{r['recovery_months']:.1f}" if r.get("recovery_months") else "N/A",
                            })
                    st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

                    # Detailed per-event results
                    for ek, r in results.items():
                        if "error" in r:
                            continue

                        with st.expander(f"📉 {pname} × {r['event']['label']}", expanded=len(selected_portfolios) == 1 and len(results) == 1):
                            st.caption(r["event"]["description"])

                            mc1, mc2, mc3, mc4 = st.columns(4)
                            mc1.metric("Total Return", f"{r['total_return_pct']:+.1f}%")
                            mc2.metric("Max Drawdown", f"-{r['max_drawdown_pct']:.1f}%")
                            mc3.metric("Final Value", f"${r['final_value']:,.0f}")
                            mc4.metric("Tickers Tested", r["tickers_tested"])

                            ts_df = pd.DataFrame(r["portfolio_timeseries"])
                            ts_df["date"] = pd.to_datetime(ts_df["date"])
                            ts_df = ts_df.set_index("date")
                            st.line_chart(ts_df["portfolio_value"])

                            st.caption("Per-ticker performance:")
                            tp_df = pd.DataFrame(r["ticker_performance"])
                            tp_df = tp_df.sort_values("total_return_pct")
                            st.dataframe(tp_df, use_container_width=True)

                except Exception as exc:
                    st.error(f"Stress test failed for '{pname}': {exc}")

            st.divider()

        # =================================================================
        # Cortex AI Insights — summarize all stress test results
        # =================================================================
        st.subheader("🤖 AI Stress Test Insights (Snowflake Cortex)")

        # Build a structured summary of all results for the prompt
        insight_lines = []
        for pname, pinfo in selected_portfolios.items():
            stress_tickers_for_prompt = pinfo["tickers"]
            try:
                pname_results = run_stress_test(
                    tickers=stress_tickers_for_prompt,
                    event_keys=selected_event_keys,
                    horizon_months=horizon_months,
                    initial_investment=initial_investment,
                )
                for ek, r in pname_results.items():
                    if "error" not in r:
                        insight_lines.append(
                            f"Portfolio '{pname}' during {r['event']['label']}: "
                            f"return={r['total_return_pct']:+.1f}%, "
                            f"max drawdown=-{r['max_drawdown_pct']:.1f}%, "
                            f"final value=${r['final_value']:,.0f}, "
                            f"recovery={'%.1f months' % r['recovery_months'] if r.get('recovery_months') else 'not recovered'}, "
                            f"tickers tested={r['tickers_tested']}"
                        )
            except Exception:
                pass

        if insight_lines:
            insight_data = "\n".join(insight_lines)

            with st.spinner("Generating AI insights via Snowflake Cortex..."):
                try:
                    conn = get_connection()
                    cur = conn.cursor()

                    prompt = (
                        "You are a senior risk analyst. Analyze the following historical stress test results "
                        "and provide actionable insights. Include: (1) which portfolios are most resilient and why, "
                        "(2) which are most vulnerable and to what types of events, (3) common patterns across events, "
                        "(4) diversification recommendations, (5) specific risk mitigation suggestions. "
                        "Be specific with numbers.\n\n"
                        "## Stress Test Results\n" + insight_data + "\n\n"
                        "Write 3-5 paragraphs in a professional tone."
                    )

                    cur.execute(
                        "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', %s)",
                        (prompt,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        st.markdown(row[0])
                    else:
                        st.info("Cortex returned empty response.")
                    conn.close()
                except Exception as exc:
                    st.warning(f"Could not generate Cortex insights: {exc}")
                    st.caption("Tip: Cortex requires Snowflake with Cortex LLM functions enabled.")
        else:
            st.info("No stress test data available for AI analysis.")

# ---------------------------------------------------------------------------
# Section 5: Simulation Log History
# ---------------------------------------------------------------------------
st.header("Simulation Log")

log_hours = st.selectbox("Time window", [1, 6, 24, 48], index=2, format_func=lambda h: f"Last {h} hours")


@st.cache_data(ttl=15)
def load_simulation_log(window_hours: int) -> pd.DataFrame:
    """Fetch SIMULATION_LOG rows within the configured time window."""
    try:
        conn = get_connection()
        try:
            query = """
                SELECT simulation_run_id, data_type, kafka_topic,
                       record_count, mode, dataset_size, status,
                       error_message, generated_at
                  FROM SIMULATION_LOG
                 WHERE generated_at >= DATEADD('hour', -%s, CURRENT_TIMESTAMP())
                 ORDER BY generated_at DESC
                 LIMIT 500
            """
            df = pd.read_sql(query, conn, params=(window_hours,))
            df.columns = [c.lower() for c in df.columns]
            return df
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame()


log_df = load_simulation_log(log_hours)

if log_df.empty:
    st.info("No simulation log entries found for the selected period.")
else:
    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Batches", len(log_df))
    c2.metric("Total Records", int(log_df["record_count"].sum()))
    c3.metric("Errors", int((log_df["status"] == "error").sum()))
    c4.metric("Unique Runs", log_df["simulation_run_id"].nunique())

    # Records by data type
    st.subheader("Records Generated by Data Type")
    by_type = log_df.groupby("data_type")["record_count"].sum()
    st.bar_chart(by_type)

    # Detailed log table
    st.subheader("Log Entries")
    st.dataframe(
        log_df[["data_type", "kafka_topic", "record_count", "mode",
                "dataset_size", "status", "error_message", "generated_at"]],
        use_container_width=True,
    )

    # Error details
    errors = log_df[log_df["status"] == "error"]
    if not errors.empty:
        st.subheader("Error Details")
        st.dataframe(
            errors[["data_type", "kafka_topic", "error_message", "generated_at"]],
            use_container_width=True,
        )

# Auto-refresh disabled to preserve user state during simulation
