"""Historical market events for stress testing portfolios.

Defines major historical events with date ranges and fetches real
market data from Yahoo Finance for those periods to simulate how
a portfolio would have performed.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-defined historical events
# ---------------------------------------------------------------------------

HISTORICAL_EVENTS: Dict[str, Dict[str, Any]] = {
    "gulf_war_1": {
        "label": "Gulf War I (1990-1991)",
        "start": "1990-08-02",
        "end": "1991-02-28",
        "description": "Iraq invasion of Kuwait, oil price spike, US-led coalition response.",
    },
    "gulf_war_2": {
        "label": "Gulf War II / Iraq War (2003)",
        "start": "2003-03-20",
        "end": "2003-09-30",
        "description": "US invasion of Iraq, caused initial volatility and increased risk premiums, though impact was less intense than the 1990 crisis.",
    },
    "yom_kippur_oil_embargo_1973": {
        "label": "Yom Kippur War / Oil Embargo (1973-1974)",
        "start": "1973-10-06",
        "end": "1974-03-18",
        "description": "Triggered a five-month oil embargo, driving huge inflation and severe stock market declines. S&P 500 fell ~48% peak to trough.",
    },
    "iranian_revolution_1979": {
        "label": "Iranian Revolution (1979)",
        "start": "1979-01-16",
        "end": "1979-12-31",
        "description": "Caused widespread instability and another major oil supply shock that affected energy efficiency policies globally. Oil prices doubled.",
    },
    "iran_iraq_war": {
        "label": "Iran-Iraq War (1980-1988)",
        "start": "1980-09-22",
        "end": "1981-09-22",
        "description": "Disrupted regional oil production for years, leading to sustained price volatility. First year captured here for market impact analysis.",
    },
    "syrian_civil_war_houthi": {
        "label": "Syrian Civil War & Houthi-Iran Conflict (2015-2016)",
        "start": "2015-09-30",
        "end": "2016-06-30",
        "description": "Regional volatility including shipping threats in the Red Sea, sporadically affected energy transit costs and oil prices.",
    },
    "iran_israel_2026": {
        "label": "Iran-Israel Conflict (2026)",
        "start": "2026-01-15",
        "end": "2026-03-29",
        "description": "Triggered acute oil price spikes approaching $82/bbl, disrupted logistics through the Strait of Hormuz, caused severe volatility in Gulf region stock markets.",
    },
    "dotcom_crash": {
        "label": "Dot-com Crash (2000-2002)",
        "start": "2000-03-10",
        "end": "2002-10-09",
        "description": "NASDAQ peak to trough, tech bubble burst.",
    },
    "gfc_2008": {
        "label": "Global Financial Crisis (2008-2009)",
        "start": "2008-09-15",
        "end": "2009-03-09",
        "description": "Lehman Brothers collapse through market bottom.",
    },
    "covid_crash": {
        "label": "COVID-19 Crash (2020)",
        "start": "2020-02-19",
        "end": "2020-06-30",
        "description": "Pre-pandemic peak through initial recovery.",
    },
    "covid_recovery": {
        "label": "COVID-19 Recovery Rally (2020-2021)",
        "start": "2020-03-23",
        "end": "2021-12-31",
        "description": "Market bottom through stimulus-driven rally.",
    },
    "russia_ukraine": {
        "label": "Russia-Ukraine War (2022)",
        "start": "2022-02-24",
        "end": "2022-12-31",
        "description": "Russian invasion, energy crisis, inflation spike.",
    },
    "fed_rate_hikes_2022": {
        "label": "Fed Rate Hike Cycle (2022-2023)",
        "start": "2022-03-16",
        "end": "2023-07-26",
        "description": "Aggressive Fed tightening from 0.25% to 5.50%.",
    },
    "black_monday_1987": {
        "label": "Black Monday (1987)",
        "start": "1987-10-14",
        "end": "1988-01-31",
        "description": "Largest single-day percentage drop in DJIA history.",
    },
    "asian_financial_crisis": {
        "label": "Asian Financial Crisis (1997-1998)",
        "start": "1997-07-02",
        "end": "1998-06-30",
        "description": "Thai baht collapse, contagion across Asian markets.",
    },
    "sept_11": {
        "label": "September 11 Attacks (2001)",
        "start": "2001-09-10",
        "end": "2001-12-31",
        "description": "Market closure and sell-off following terrorist attacks.",
    },
    "euro_debt_crisis": {
        "label": "European Debt Crisis (2011-2012)",
        "start": "2011-07-01",
        "end": "2012-06-30",
        "description": "Greek debt crisis, contagion fears across eurozone.",
    },
    "china_devaluation_2015": {
        "label": "China Devaluation / Flash Crash (2015)",
        "start": "2015-08-10",
        "end": "2016-02-11",
        "description": "Yuan devaluation, global sell-off, oil price collapse.",
    },
    "svb_banking_crisis_2023": {
        "label": "SVB / Banking Crisis (2023)",
        "start": "2023-03-08",
        "end": "2023-06-30",
        "description": "Silicon Valley Bank collapse, regional banking contagion.",
    },
}


# ---------------------------------------------------------------------------
# Stress test engine
# ---------------------------------------------------------------------------

def run_stress_test(
    tickers: List[str],
    event_keys: List[str],
    horizon_months: int = 6,
    initial_investment: float = 1000000.0,
) -> Dict[str, Any]:
    """Run a historical stress test for a portfolio against one or more events.

    For each event, fetches real historical prices from Yahoo Finance for the
    event period (plus horizon_months after the event end), and computes:
    - Portfolio value over time (equal-weighted across tickers)
    - Max drawdown during the event
    - Recovery time
    - Total return over the horizon

    Returns a dict with results per event.
    """
    results = {}

    for event_key in event_keys:
        event = HISTORICAL_EVENTS.get(event_key)
        if not event:
            logger.warning("Unknown event: %s", event_key)
            continue

        event_start = datetime.strptime(event["start"], "%Y-%m-%d")
        event_end = datetime.strptime(event["end"], "%Y-%m-%d")
        # Extend by horizon_months after event end
        analysis_end = event_end + timedelta(days=horizon_months * 30)

        logger.info("Stress testing against %s (%s to %s)", event["label"], event["start"], analysis_end.strftime("%Y-%m-%d"))

        # Fetch historical data for all tickers
        ticker_data = {}
        valid_tickers = []
        for ticker in tickers:
            try:
                t = yf.Ticker(ticker)
                hist = t.history(start=event_start, end=analysis_end)
                if not hist.empty and len(hist) > 5:
                    ticker_data[ticker] = hist
                    valid_tickers.append(ticker)
                else:
                    logger.warning("No data for %s during %s", ticker, event["label"])
            except Exception as exc:
                logger.warning("Failed to fetch %s for %s: %s", ticker, event["label"], exc)

        if not valid_tickers:
            results[event_key] = {
                "event": event,
                "error": "No valid ticker data available for this period",
                "tickers_tested": 0,
            }
            continue

        # Build equal-weighted portfolio value over time
        # Normalize each ticker to start at 1.0, then average
        normalized = {}
        common_dates = None
        for ticker, hist in ticker_data.items():
            prices = hist["Close"]
            norm = prices / prices.iloc[0]  # normalize to 1.0 at start
            normalized[ticker] = norm
            if common_dates is None:
                common_dates = set(norm.index)
            else:
                common_dates = common_dates.intersection(set(norm.index))

        if not common_dates:
            results[event_key] = {
                "event": event,
                "error": "No overlapping dates across tickers",
                "tickers_tested": len(valid_tickers),
            }
            continue

        common_dates = sorted(common_dates)
        portfolio_values = []
        for date in common_dates:
            avg_norm = sum(normalized[t].loc[date] for t in valid_tickers) / len(valid_tickers)
            portfolio_values.append({
                "date": date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date),
                "portfolio_value": round(float(avg_norm * initial_investment), 2),
                "normalized": round(float(avg_norm), 4),
            })

        # Compute metrics
        values = [pv["portfolio_value"] for pv in portfolio_values]
        peak = values[0]
        max_drawdown = 0.0
        max_drawdown_date = portfolio_values[0]["date"]
        trough_value = peak

        for pv in portfolio_values:
            v = pv["portfolio_value"]
            if v > peak:
                peak = v
            dd = (peak - v) / peak if peak > 0 else 0
            if dd > max_drawdown:
                max_drawdown = dd
                max_drawdown_date = pv["date"]
                trough_value = v

        total_return = (values[-1] - values[0]) / values[0] if values[0] > 0 else 0

        # Find recovery date and compute recovery months
        recovery_date = None
        recovery_months = None
        past_trough = False
        trough_date = None
        for pv in portfolio_values:
            if pv["portfolio_value"] == trough_value:
                past_trough = True
                trough_date = pv["date"]
            if past_trough and pv["portfolio_value"] >= initial_investment:
                recovery_date = pv["date"]
                if trough_date:
                    td = datetime.strptime(trough_date, "%Y-%m-%d") if isinstance(trough_date, str) else trough_date
                    rd = datetime.strptime(recovery_date, "%Y-%m-%d") if isinstance(recovery_date, str) else recovery_date
                    recovery_months = round((rd - td).days / 30.44, 1)
                break

        # Per-ticker performance
        ticker_performance = []
        for ticker in valid_tickers:
            t_prices = ticker_data[ticker]["Close"]
            t_return = (float(t_prices.iloc[-1]) - float(t_prices.iloc[0])) / float(t_prices.iloc[0])
            t_min = float(t_prices.min())
            t_max_dd = (float(t_prices.iloc[0]) - t_min) / float(t_prices.iloc[0]) if float(t_prices.iloc[0]) > 0 else 0
            ticker_performance.append({
                "ticker": ticker,
                "start_price": round(float(t_prices.iloc[0]), 2),
                "end_price": round(float(t_prices.iloc[-1]), 2),
                "min_price": round(t_min, 2),
                "total_return_pct": round(t_return * 100, 2),
                "max_drawdown_pct": round(t_max_dd * 100, 2),
            })

        results[event_key] = {
            "event": event,
            "tickers_tested": len(valid_tickers),
            "trading_days": len(portfolio_values),
            "initial_investment": initial_investment,
            "final_value": values[-1],
            "total_return_pct": round(total_return * 100, 2),
            "max_drawdown_pct": round(max_drawdown * 100, 2),
            "max_drawdown_date": max_drawdown_date,
            "trough_value": trough_value,
            "recovery_date": recovery_date,
            "recovery_months": recovery_months,
            "portfolio_timeseries": portfolio_values,
            "ticker_performance": ticker_performance,
        }

    return results
