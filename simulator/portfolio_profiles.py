"""Portfolio profiles and Yahoo Finance data fetcher.

Defines portfolio strategies (sector mixes, famous investor styles, etc.)
and fetches real tickers + market data from Yahoo Finance to drive
realistic trade simulation.
"""
from __future__ import annotations

import logging
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-defined portfolio profiles
# ---------------------------------------------------------------------------

PORTFOLIO_PROFILES: Dict[str, Dict[str, Any]] = {
    "tech_heavy": {
        "label": "Tech Heavy (80% Tech, 20% Finance)",
        "sectors": {"Technology": 0.80, "Financial Services": 0.20},
        "tickers": [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "CRM",
            "JPM", "BAC", "GS", "MS",
        ],
    },
    "tech_finance_50_50": {
        "label": "Tech + Finance 50-50",
        "sectors": {"Technology": 0.50, "Financial Services": 0.50},
        "tickers": [
            "AAPL", "MSFT", "GOOGL", "NVDA", "META",
            "JPM", "BAC", "GS", "MS", "C", "WFC", "BLK",
        ],
    },
    "aggressive_growth_us": {
        "label": "Aggressive Growth (All US Stock)",
        "sectors": {"Technology": 0.40, "Consumer Cyclical": 0.20, "Healthcare": 0.20, "Communication Services": 0.20},
        "tickers": [
            "AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "META", "GOOGL",
            "NFLX", "AMD", "SHOP", "SQ", "ROKU", "DKNG",
            "MRNA", "CRSP", "ISRG", "DXCM",
        ],
    },
    "warren_buffett": {
        "label": "Mimic Warren Buffett",
        "sectors": {"Financial Services": 0.35, "Technology": 0.25, "Consumer Defensive": 0.20, "Energy": 0.10, "Healthcare": 0.10},
        "tickers": [
            "AAPL", "BAC", "AXP", "KO", "CVX", "OXY", "KHC",
            "MCO", "DVA", "ALLY", "C", "JNJ", "PG",
        ],
    },
    "cathie_wood_ark": {
        "label": "Mimic Cathie Wood / ARK",
        "sectors": {"Technology": 0.50, "Healthcare": 0.25, "Communication Services": 0.15, "Industrials": 0.10},
        "tickers": [
            "TSLA", "ROKU", "SQ", "COIN", "PATH", "DKNG", "RBLX",
            "CRSP", "EXAS", "TDOC", "ZM", "U", "PLTR",
        ],
    },
    "conservative_dividend": {
        "label": "Conservative Dividend",
        "sectors": {"Consumer Defensive": 0.30, "Utilities": 0.25, "Healthcare": 0.25, "Financial Services": 0.20},
        "tickers": [
            "JNJ", "PG", "KO", "PEP", "MCD", "WMT",
            "NEE", "DUK", "SO", "D",
            "UNH", "ABT", "PFE",
            "JPM", "BLK",
        ],
    },
    "custom": {
        "label": "Custom (enter your own tickers)",
        "sectors": {},
        "tickers": [],
    },
}


# ---------------------------------------------------------------------------
# Yahoo Finance data fetcher
# ---------------------------------------------------------------------------

def fetch_ticker_info(tickers: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch current price, sector, name, and CUSIP-like ID for each ticker.

    Returns a dict keyed by ticker symbol.
    """
    result = {}
    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            hist = t.history(period="1d")
            current_price = float(hist["Close"].iloc[-1]) if not hist.empty else info.get("currentPrice", 0)

            result[ticker] = {
                "ticker": ticker,
                "cusip": _ticker_to_cusip(ticker),
                "name": info.get("shortName", info.get("longName", ticker)),
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "price": round(current_price, 2),
                "market_cap": info.get("marketCap", 0),
            }
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", ticker, exc)
            result[ticker] = {
                "ticker": ticker,
                "cusip": _ticker_to_cusip(ticker),
                "name": ticker,
                "sector": "Unknown",
                "industry": "Unknown",
                "price": 100.0,
                "market_cap": 0,
            }
    return result


def fetch_historical_prices(tickers: List[str], days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch historical daily prices for each ticker.

    Returns a dict keyed by ticker, each value is a list of
    {date, open, high, low, close, volume} dicts.
    """
    result = {}
    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=f"{days}d")
            records = []
            for date, row in hist.iterrows():
                records.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 2),
                    "high": round(float(row["High"]), 2),
                    "low": round(float(row["Low"]), 2),
                    "close": round(float(row["Close"]), 2),
                    "volume": int(row["Volume"]),
                })
            result[ticker] = records
        except Exception as exc:
            logger.warning("Failed to fetch history for %s: %s", ticker, exc)
            result[ticker] = []
    return result


def _ticker_to_cusip(ticker: str) -> str:
    """Generate a deterministic 9-char CUSIP-like ID from a ticker."""
    # Use a hash to create a stable mapping
    import hashlib
    h = hashlib.md5(ticker.encode()).hexdigest()[:8].upper()
    return h + "0"  # 9 chars


# ---------------------------------------------------------------------------
# Portfolio data generator
# ---------------------------------------------------------------------------

def generate_portfolio_data(
    profile_key: str,
    custom_tickers: Optional[List[str]] = None,
    num_participants: int = 5,
    trades_per_ticker: int = 10,
    history_days: int = 30,
) -> Dict[str, List[Dict[str, Any]]]:
    """Generate complete portfolio data (trades, market data, reference data)
    based on a portfolio profile and real Yahoo Finance data.

    Returns a dict with keys: trades, market_data, security_master, entity_master
    """
    profile = PORTFOLIO_PROFILES.get(profile_key, PORTFOLIO_PROFILES["tech_finance_50_50"])
    tickers = custom_tickers if (profile_key == "custom" and custom_tickers) else profile["tickers"]

    if not tickers:
        raise ValueError("No tickers specified")

    logger.info("Fetching Yahoo Finance data for %d tickers...", len(tickers))

    # Fetch real data
    ticker_info = fetch_ticker_info(tickers)
    historical = fetch_historical_prices(tickers, days=history_days)

    # Generate participants
    participant_names = [
        "Alpha Trading Desk", "Beta Capital", "Gamma Investments",
        "Delta Securities", "Epsilon Fund", "Zeta Partners",
        "Theta Asset Mgmt", "Iota Strategies", "Kappa Holdings", "Lambda Group",
    ]
    participants = []
    for i in range(min(num_participants, len(participant_names))):
        participants.append({
            "participant_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, participant_names[i])),
            "participant_name": participant_names[i],
            "entity_type": random.choice(["desk", "fund", "hedge_fund"]),
            "risk_limit": random.choice([1000000, 5000000, 10000000, 50000000]),
        })

    # Build security master from Yahoo data
    security_master = []
    for ticker, info in ticker_info.items():
        security_master.append({
            "data_type": "security_master",
            "payload": {
                "cusip": info["cusip"],
                "security_name": info["name"],
                "issuer": info["name"].split()[0] if info["name"] else ticker,
                "security_type": "equity",
                "sector": info["sector"],
            },
        })

    # Build entity master
    entity_master = []
    for p in participants:
        entity_master.append({
            "data_type": "entity_master",
            "payload": {
                "participant_id": p["participant_id"],
                "participant_name": p["participant_name"],
                "entity_type": p["entity_type"],
                "risk_limit": p["risk_limit"],
            },
        })

    # Build market data from historical prices
    market_data_records = []
    for ticker, history in historical.items():
        info = ticker_info.get(ticker, {})
        cusip = info.get("cusip", _ticker_to_cusip(ticker))
        for day in history:
            market_data_records.append({
                "payload": {
                    "cusip": cusip,
                    "price": day["close"],
                    "index_value": None,
                    "data_date": day["date"],
                },
            })

    # Build trades — distribute across participants based on sector weights
    trades = []
    now = datetime.now(timezone.utc)
    sector_weights = profile.get("sectors", {})

    for ticker, info in ticker_info.items():
        price = info["price"] if info["price"] > 0 else 100.0
        cusip = info["cusip"]

        for _ in range(trades_per_ticker):
            participant = random.choice(participants)
            quantity = random.randint(10, 10000)
            # Add some price variation
            trade_price = round(price * random.uniform(0.95, 1.05), 2)
            trade_time = now - timedelta(
                hours=random.randint(0, 24),
                minutes=random.randint(0, 59),
            )

            trades.append({
                "format": "bulk",
                "payload": {
                    "ticker": ticker,
                    "cusip": cusip,
                    "price": trade_price,
                    "quantity": quantity,
                    "participant_id": participant["participant_id"],
                    "trade_timestamp": trade_time.replace(tzinfo=None).isoformat(),
                },
            })

    return {
        "trades": trades,
        "market_data": market_data_records,
        "security_master": security_master,
        "entity_master": entity_master,
        "ticker_info": ticker_info,
        "participants": participants,
    }
