"""Synthetic data generators for the Data Simulator.

All generators produce dicts matching the Kafka message format expected
by the ingestion services (trade-ingestion, market-data, reference-data).

Requirements: 16.1, 16.6, 16.9, 16.10, 16.11
"""

from __future__ import annotations

import random
import string
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Realistic name pools
# ---------------------------------------------------------------------------

_SECURITY_NAMES = [
    "Apple Inc.", "Microsoft Corp.", "Amazon.com Inc.", "Alphabet Inc.",
    "Meta Platforms Inc.", "Tesla Inc.", "NVIDIA Corp.", "JPMorgan Chase",
    "Johnson & Johnson", "Visa Inc.", "Procter & Gamble", "UnitedHealth Group",
    "Home Depot Inc.", "Mastercard Inc.", "Bank of America", "Pfizer Inc.",
    "Chevron Corp.", "Cisco Systems", "Coca-Cola Co.", "PepsiCo Inc.",
    "Walt Disney Co.", "Intel Corp.", "Verizon Comm.", "Nike Inc.",
    "Merck & Co.", "Abbott Labs", "Salesforce Inc.", "Oracle Corp.",
    "Accenture PLC", "Costco Wholesale",
]

_ISSUERS = [
    "Apple Inc.", "Microsoft Corporation", "Amazon.com Inc.", "Alphabet Inc.",
    "Meta Platforms", "Tesla Inc.", "NVIDIA Corporation", "JPMorgan Chase & Co.",
    "Johnson & Johnson", "Visa Inc.", "Procter & Gamble Co.", "UnitedHealth Group Inc.",
]

_SECURITY_TYPES = ["equity", "bond", "etf", "preferred", "convertible"]
_SECTORS = [
    "Technology", "Healthcare", "Financials", "Consumer Discretionary",
    "Communication Services", "Industrials", "Consumer Staples",
    "Energy", "Utilities", "Real Estate", "Materials",
]

_PARTICIPANT_PREFIXES = [
    "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Theta",
    "Sigma", "Omega", "Lambda", "Kappa", "Rho", "Tau", "Phi", "Psi",
    "Apex", "Summit", "Pinnacle", "Vertex", "Zenith",
]

_PARTICIPANT_SUFFIXES = [
    "Capital", "Partners", "Advisors", "Management", "Securities",
    "Investments", "Trading", "Fund", "Group", "Holdings",
]

_ENTITY_TYPES = ["desk", "fund", "hedge_fund", "pension", "insurance", "proprietary"]

_TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM",
    "JNJ", "V", "PG", "UNH", "HD", "MA", "BAC", "PFE", "CVX", "CSCO",
    "KO", "PEP", "DIS", "INTC", "VZ", "NKE", "MRK", "ABT", "CRM",
    "ORCL", "ACN", "COST",
]


# ---------------------------------------------------------------------------
# Pool generators
# ---------------------------------------------------------------------------

def generate_cusip_pool(size: int) -> list[str]:
    """Generate a pool of realistic 9-character CUSIP identifiers.

    Each CUSIP consists of 6 alphanumeric issuer chars, 2 alphanumeric
    issue chars, and 1 check digit (0-9).
    """
    chars = string.ascii_uppercase + string.digits
    pool: list[str] = []
    seen: set[str] = set()
    while len(pool) < size:
        issuer = "".join(random.choices(chars, k=6))
        issue = "".join(random.choices(chars, k=2))
        check = str(random.randint(0, 9))
        cusip = issuer + issue + check
        if cusip not in seen:
            seen.add(cusip)
            pool.append(cusip)
    return pool


def generate_participant_pool(size: int) -> list[dict[str, str]]:
    """Generate a pool of participant dicts with ``id`` and ``name`` keys."""
    pool: list[dict[str, str]] = []
    seen_names: set[str] = set()
    while len(pool) < size:
        name = f"{random.choice(_PARTICIPANT_PREFIXES)} {random.choice(_PARTICIPANT_SUFFIXES)}"
        if name in seen_names:
            name = f"{name} {random.randint(1, 999)}"
        if name not in seen_names:
            seen_names.add(name)
            pool.append({
                "id": str(uuid.uuid4()),
                "name": name,
            })
    return pool


# ---------------------------------------------------------------------------
# Record generators
# ---------------------------------------------------------------------------

def generate_trades(
    cusip_pool: list[str],
    participant_pool: list[dict[str, str]],
    count: int,
    price_range: tuple[float, float] = (1.00, 5000.00),
    quantity_range: tuple[int, int] = (1, 100_000),
) -> list[dict[str, Any]]:
    """Generate trade records matching the trade-ingestion Kafka message format.

    Each returned dict is a complete Kafka message body with ``format``
    and ``payload`` keys.
    """
    records: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    formats = ["bulk", "snapshot", "voice"]

    for _ in range(count):
        cusip = random.choice(cusip_pool)
        participant = random.choice(participant_pool)
        ticker = random.choice(_TICKERS)
        price = round(random.uniform(*price_range), 6)
        quantity = random.randint(*quantity_range)
        ts = now - timedelta(seconds=random.randint(0, 300))

        records.append({
            "format": random.choice(formats),
            "payload": {
                "ticker": ticker,
                "cusip": cusip,
                "price": price,
                "quantity": quantity,
                "participant_id": participant["id"],
                "trade_timestamp": ts.isoformat(),
            },
        })
    return records


def generate_market_data(
    cusip_pool: list[str],
    count: int,
    price_range: tuple[float, float] = (1.00, 5000.00),
) -> list[dict[str, Any]]:
    """Generate market data records matching the market-data Kafka message format.

    Each returned dict is a complete Kafka message body with a ``payload``
    key containing the market data record.
    """
    records: list[dict[str, Any]] = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for _ in range(count):
        cusip = random.choice(cusip_pool)
        price = round(random.uniform(*price_range), 6)
        index_value = round(random.uniform(1000.0, 50000.0), 6) if random.random() > 0.3 else None

        records.append({
            "payload": {
                "cusip": cusip,
                "price": price,
                "index_value": index_value,
                "data_date": today,
            },
        })
    return records


def generate_security_master(cusip_pool: list[str]) -> list[dict[str, Any]]:
    """Generate Security Master reference data records.

    Each returned dict is a complete Kafka message body with ``data_type``
    and ``payload`` keys matching the reference-data topic format.
    """
    records: list[dict[str, Any]] = []
    for cusip in cusip_pool:
        name = random.choice(_SECURITY_NAMES) if len(_SECURITY_NAMES) > 0 else "Security"
        records.append({
            "data_type": "security_master",
            "payload": {
                "cusip": cusip,
                "security_name": f"{name} ({cusip[:4]})",
                "issuer": random.choice(_ISSUERS),
                "security_type": random.choice(_SECURITY_TYPES),
                "sector": random.choice(_SECTORS),
            },
        })
    return records


def generate_entity_master(
    participant_pool: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Generate Entity Master reference data records.

    Each returned dict is a complete Kafka message body with ``data_type``
    and ``payload`` keys matching the reference-data topic format.
    """
    records: list[dict[str, Any]] = []
    for participant in participant_pool:
        records.append({
            "data_type": "entity_master",
            "payload": {
                "participant_id": participant["id"],
                "participant_name": participant["name"],
                "entity_type": random.choice(_ENTITY_TYPES),
                "risk_limit": round(random.uniform(100_000, 10_000_000), 2),
            },
        })
    return records
