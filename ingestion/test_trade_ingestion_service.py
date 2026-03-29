"""Unit tests for the Trade Ingestion Service.

Tests cover validation logic, message parsing, and batch processing.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ingestion.trade_ingestion_service import (
    VALID_FORMATS,
    _parse_message,
    process_batch,
    validate_trade_record,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_trade(**overrides) -> dict[str, Any]:
    """Return a minimal valid trade record, with optional overrides."""
    base = {
        "ticker": "AAPL",
        "cusip": "037833100",
        "price": 150.25,
        "quantity": 100,
        "participant_id": str(uuid.uuid4()),
        "trade_timestamp": "2024-06-15T10:30:00",
    }
    base.update(overrides)
    return base


def _make_kafka_message(payload_wrapper: dict) -> MagicMock:
    """Create a mock Kafka Message whose .value() returns JSON bytes."""
    msg = MagicMock()
    msg.value.return_value = json.dumps(payload_wrapper).encode("utf-8")
    msg.error.return_value = None
    return msg


# ---------------------------------------------------------------------------
# validate_trade_record
# ---------------------------------------------------------------------------

class TestValidateTradeRecord:
    """Tests for the validate_trade_record function."""

    def test_valid_record_returns_none(self):
        assert validate_trade_record(_valid_trade()) is None

    @pytest.mark.parametrize("field", ["ticker", "price", "quantity", "participant_id", "trade_timestamp"])
    def test_missing_required_field(self, field):
        rec = _valid_trade()
        del rec[field]
        reason = validate_trade_record(rec)
        assert reason is not None
        assert field in reason

    @pytest.mark.parametrize("field", ["ticker", "participant_id", "trade_timestamp"])
    def test_empty_string_required_field(self, field):
        rec = _valid_trade(**{field: ""})
        reason = validate_trade_record(rec)
        assert reason is not None

    def test_price_zero_rejected(self):
        reason = validate_trade_record(_valid_trade(price=0))
        assert reason is not None
        assert "Price" in reason

    def test_price_negative_rejected(self):
        reason = validate_trade_record(_valid_trade(price=-10))
        assert reason is not None

    def test_price_non_numeric_rejected(self):
        reason = validate_trade_record(_valid_trade(price="abc"))
        assert reason is not None

    def test_quantity_zero_rejected(self):
        reason = validate_trade_record(_valid_trade(quantity=0))
        assert reason is not None

    def test_quantity_negative_rejected(self):
        reason = validate_trade_record(_valid_trade(quantity=-5))
        assert reason is not None

    def test_quantity_non_numeric_rejected(self):
        reason = validate_trade_record(_valid_trade(quantity="xyz"))
        assert reason is not None

    def test_invalid_timestamp_rejected(self):
        reason = validate_trade_record(_valid_trade(trade_timestamp="not-a-date"))
        assert reason is not None

    def test_none_field_rejected(self):
        reason = validate_trade_record(_valid_trade(ticker=None))
        assert reason is not None


# ---------------------------------------------------------------------------
# _parse_message
# ---------------------------------------------------------------------------

class TestParseMessage:
    def test_valid_json(self):
        data = {"format": "bulk", "payload": [_valid_trade()]}
        result = _parse_message(json.dumps(data).encode("utf-8"))
        assert result["format"] == "bulk"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _parse_message(b"not json")


# ---------------------------------------------------------------------------
# process_batch (with mocked Snowflake)
# ---------------------------------------------------------------------------

class TestProcessBatch:
    """Tests for process_batch using a mocked Snowflake connection."""

    def _mock_sf_conn(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_all_valid_records(self):
        conn, cursor = self._mock_sf_conn()
        trades = [_valid_trade() for _ in range(3)]
        msg = _make_kafka_message({"format": "bulk", "payload": trades})

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 3
        assert summary["rejected_count"] == 0
        assert summary["status"] == "success"
        assert "batch_id" in summary

    def test_all_invalid_records(self):
        conn, cursor = self._mock_sf_conn()
        bad_trades = [{"ticker": "X"}, {"price": -1}]
        msg = _make_kafka_message({"format": "snapshot", "payload": bad_trades})

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == summary["record_count"]
        assert summary["status"] == "error"

    def test_mixed_valid_and_invalid(self):
        conn, _ = self._mock_sf_conn()
        trades = [_valid_trade(), {"ticker": "BAD"}]
        msg = _make_kafka_message({"format": "voice", "payload": trades})

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 2
        assert summary["rejected_count"] == 1
        assert summary["status"] == "partial"

    def test_malformed_json_message(self):
        conn, _ = self._mock_sf_conn()
        msg = MagicMock()
        msg.value.return_value = b"{{bad json"
        msg.error.return_value = None

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == 1

    def test_missing_payload_field(self):
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"format": "bulk"})

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == 1

    def test_unknown_format_rejected(self):
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"format": "unknown", "payload": [_valid_trade()]})

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == 1

    def test_batch_id_is_unique_uuid(self):
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"format": "bulk", "payload": [_valid_trade()]})

        s1 = process_batch([msg], conn)
        s2 = process_batch([msg], conn)

        assert s1["batch_id"] != s2["batch_id"]
        # Verify they are valid UUIDs
        uuid.UUID(s1["batch_id"])
        uuid.UUID(s2["batch_id"])

    def test_processing_duration_is_positive(self):
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"format": "bulk", "payload": [_valid_trade()]})

        summary = process_batch([msg], conn)

        assert summary["processing_duration_ms"] >= 0

    def test_single_record_payload_not_list(self):
        """payload can be a single dict instead of a list."""
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"format": "bulk", "payload": _valid_trade()})

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 1
        assert summary["rejected_count"] == 0

    def test_null_message_value_skipped(self):
        conn, _ = self._mock_sf_conn()
        msg = MagicMock()
        msg.value.return_value = None
        msg.error.return_value = None

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 0
