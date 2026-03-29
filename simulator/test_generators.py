"""Unit tests for simulator.generators.

Validates that synthetic data generators produce records matching the
Kafka message formats expected by the ingestion services.
"""

from __future__ import annotations

import re

import pytest

from simulator.generators import (
    generate_cusip_pool,
    generate_entity_master,
    generate_market_data,
    generate_participant_pool,
    generate_security_master,
    generate_trades,
)


# ---------------------------------------------------------------------------
# CUSIP pool
# ---------------------------------------------------------------------------

class TestGenerateCusipPool:
    def test_correct_count(self):
        pool = generate_cusip_pool(20)
        assert len(pool) == 20

    def test_unique(self):
        pool = generate_cusip_pool(50)
        assert len(set(pool)) == 50

    def test_format_9_chars_alphanumeric(self):
        pool = generate_cusip_pool(10)
        pattern = re.compile(r"^[A-Z0-9]{9}$")
        for cusip in pool:
            assert pattern.match(cusip), f"Invalid CUSIP format: {cusip}"


# ---------------------------------------------------------------------------
# Participant pool
# ---------------------------------------------------------------------------

class TestGenerateParticipantPool:
    def test_correct_count(self):
        pool = generate_participant_pool(8)
        assert len(pool) == 8

    def test_has_id_and_name(self):
        pool = generate_participant_pool(3)
        for p in pool:
            assert "id" in p and isinstance(p["id"], str) and len(p["id"]) > 0
            assert "name" in p and isinstance(p["name"], str) and len(p["name"]) > 0

    def test_unique_ids(self):
        pool = generate_participant_pool(15)
        ids = [p["id"] for p in pool]
        assert len(set(ids)) == 15


# ---------------------------------------------------------------------------
# Trade records
# ---------------------------------------------------------------------------

class TestGenerateTrades:
    def setup_method(self):
        self.cusips = generate_cusip_pool(5)
        self.participants = generate_participant_pool(3)

    def test_correct_count(self):
        trades = generate_trades(self.cusips, self.participants, 25)
        assert len(trades) == 25

    def test_message_format(self):
        """Each trade message must have 'format' and 'payload' keys."""
        trades = generate_trades(self.cusips, self.participants, 10)
        for t in trades:
            assert "format" in t
            assert t["format"] in {"bulk", "snapshot", "voice"}
            assert "payload" in t

    def test_payload_fields(self):
        """Payload must contain all fields expected by the trade ingestion service."""
        trades = generate_trades(self.cusips, self.participants, 10)
        required = {"ticker", "cusip", "price", "quantity", "participant_id", "trade_timestamp"}
        for t in trades:
            assert required.issubset(t["payload"].keys())

    def test_cusip_from_pool(self):
        trades = generate_trades(self.cusips, self.participants, 20)
        for t in trades:
            assert t["payload"]["cusip"] in self.cusips

    def test_participant_from_pool(self):
        pids = {p["id"] for p in self.participants}
        trades = generate_trades(self.cusips, self.participants, 20)
        for t in trades:
            assert t["payload"]["participant_id"] in pids

    def test_price_in_range(self):
        trades = generate_trades(self.cusips, self.participants, 50, price_range=(10.0, 100.0))
        for t in trades:
            assert 10.0 <= t["payload"]["price"] <= 100.0

    def test_quantity_in_range(self):
        trades = generate_trades(self.cusips, self.participants, 50, quantity_range=(5, 500))
        for t in trades:
            assert 5 <= t["payload"]["quantity"] <= 500


# ---------------------------------------------------------------------------
# Market data records
# ---------------------------------------------------------------------------

class TestGenerateMarketData:
    def setup_method(self):
        self.cusips = generate_cusip_pool(5)

    def test_correct_count(self):
        records = generate_market_data(self.cusips, 30)
        assert len(records) == 30

    def test_message_format(self):
        """Each message must have a 'payload' key with required fields."""
        records = generate_market_data(self.cusips, 10)
        for r in records:
            assert "payload" in r
            payload = r["payload"]
            assert "cusip" in payload
            assert "price" in payload
            assert "data_date" in payload

    def test_cusip_from_pool(self):
        records = generate_market_data(self.cusips, 20)
        for r in records:
            assert r["payload"]["cusip"] in self.cusips

    def test_price_positive(self):
        records = generate_market_data(self.cusips, 50)
        for r in records:
            assert r["payload"]["price"] > 0


# ---------------------------------------------------------------------------
# Security Master records
# ---------------------------------------------------------------------------

class TestGenerateSecurityMaster:
    def setup_method(self):
        self.cusips = generate_cusip_pool(5)

    def test_one_per_cusip(self):
        records = generate_security_master(self.cusips)
        assert len(records) == len(self.cusips)

    def test_message_format(self):
        records = generate_security_master(self.cusips)
        for r in records:
            assert r["data_type"] == "security_master"
            assert "payload" in r
            payload = r["payload"]
            for field in ("cusip", "security_name", "issuer", "security_type", "sector"):
                assert field in payload and payload[field]


# ---------------------------------------------------------------------------
# Entity Master records
# ---------------------------------------------------------------------------

class TestGenerateEntityMaster:
    def setup_method(self):
        self.participants = generate_participant_pool(4)

    def test_one_per_participant(self):
        records = generate_entity_master(self.participants)
        assert len(records) == len(self.participants)

    def test_message_format(self):
        records = generate_entity_master(self.participants)
        for r in records:
            assert r["data_type"] == "entity_master"
            assert "payload" in r
            payload = r["payload"]
            for field in ("participant_id", "participant_name", "entity_type", "risk_limit"):
                assert field in payload
