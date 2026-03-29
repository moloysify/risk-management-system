"""Unit tests for the Market Data Ingestion Service.

Tests cover validation logic, message parsing, and batch processing.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.market_data_ingestion_service import (
    _parse_message,
    process_batch,
    validate_market_data_record,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_market_data(**overrides) -> dict[str, Any]:
    """Return a minimal valid market data record, with optional overrides."""
    base = {
        "cusip": "037833100",
        "price": 150.25,
        "index_value": 4500.00,
        "data_date": "2024-06-15",
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
# validate_market_data_record
# ---------------------------------------------------------------------------

class TestValidateMarketDataRecord:
    """Tests for the validate_market_data_record function."""

    def test_valid_record_returns_none(self):
        assert validate_market_data_record(_valid_market_data()) is None

    def test_valid_record_without_index_value(self):
        rec = _valid_market_data()
        del rec["index_value"]
        assert validate_market_data_record(rec) is None

    def test_valid_record_with_null_index_value(self):
        assert validate_market_data_record(_valid_market_data(index_value=None)) is None

    @pytest.mark.parametrize("field", ["cusip", "price", "data_date"])
    def test_missing_required_field(self, field):
        rec = _valid_market_data()
        del rec[field]
        reason = validate_market_data_record(rec)
        assert reason is not None
        assert field in reason

    @pytest.mark.parametrize("field", ["cusip", "data_date"])
    def test_empty_string_required_field(self, field):
        rec = _valid_market_data(**{field: ""})
        reason = validate_market_data_record(rec)
        assert reason is not None

    def test_price_zero_rejected(self):
        reason = validate_market_data_record(_valid_market_data(price=0))
        assert reason is not None
        assert "Price" in reason

    def test_price_negative_rejected(self):
        reason = validate_market_data_record(_valid_market_data(price=-10))
        assert reason is not None

    def test_price_non_numeric_rejected(self):
        reason = validate_market_data_record(_valid_market_data(price="abc"))
        assert reason is not None

    def test_invalid_data_date_rejected(self):
        reason = validate_market_data_record(_valid_market_data(data_date="not-a-date"))
        assert reason is not None

    def test_none_field_rejected(self):
        reason = validate_market_data_record(_valid_market_data(cusip=None))
        assert reason is not None


# ---------------------------------------------------------------------------
# _parse_message
# ---------------------------------------------------------------------------

class TestParseMessage:
    def test_valid_json(self):
        data = {"payload": [_valid_market_data()]}
        result = _parse_message(json.dumps(data).encode("utf-8"))
        assert "payload" in result

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
        records = [_valid_market_data() for _ in range(3)]
        msg = _make_kafka_message({"payload": records})

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 3
        assert summary["rejected_count"] == 0
        assert summary["status"] == "success"
        assert "batch_id" in summary

    def test_all_invalid_records(self):
        conn, cursor = self._mock_sf_conn()
        bad_records = [{"cusip": "X"}, {"price": -1}]
        msg = _make_kafka_message({"payload": bad_records})

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == summary["record_count"]
        assert summary["status"] == "error"

    def test_mixed_valid_and_invalid(self):
        conn, _ = self._mock_sf_conn()
        records = [_valid_market_data(), {"cusip": "BAD"}]
        msg = _make_kafka_message({"payload": records})

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

    def test_missing_payload_uses_wrapper_as_record(self):
        """When no 'payload' key exists, the wrapper itself is treated as a record."""
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message(_valid_market_data())

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 1
        assert summary["rejected_count"] == 0
        assert summary["status"] == "success"

    def test_batch_id_is_unique_uuid(self):
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"payload": [_valid_market_data()]})

        s1 = process_batch([msg], conn)
        s2 = process_batch([msg], conn)

        assert s1["batch_id"] != s2["batch_id"]
        uuid.UUID(s1["batch_id"])
        uuid.UUID(s2["batch_id"])

    def test_processing_duration_is_positive(self):
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"payload": [_valid_market_data()]})

        summary = process_batch([msg], conn)

        assert summary["processing_duration_ms"] >= 0

    def test_single_record_payload_not_list(self):
        """payload can be a single dict instead of a list."""
        conn, _ = self._mock_sf_conn()
        msg = _make_kafka_message({"payload": _valid_market_data()})

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

    def test_ingestion_timestamp_is_stamped(self):
        """Verify that _insert_market_data is called with a timestamp (ingestion_timestamp)."""
        conn, cursor = self._mock_sf_conn()
        msg = _make_kafka_message({"payload": [_valid_market_data()]})

        process_batch([msg], conn)

        # The first cursor call is for _insert_market_data
        assert cursor.executemany.called
        call_args = cursor.executemany.call_args
        rows = call_args[0][1]  # second positional arg is the rows list
        # Each row tuple: (market_data_id, cusip, price, index_value, data_date, ingestion_timestamp)
        assert len(rows) == 1
        ingestion_ts = rows[0][5]
        assert isinstance(ingestion_ts, datetime)

    def test_record_with_null_index_value(self):
        """index_value is nullable — None should pass through."""
        conn, cursor = self._mock_sf_conn()
        rec = _valid_market_data(index_value=None)
        msg = _make_kafka_message({"payload": [rec]})

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 1
        assert summary["rejected_count"] == 0
        rows = cursor.executemany.call_args[0][1]
        assert rows[0][3] is None  # index_value position

    def test_snowflake_write_failure_sets_error_status(self):
        conn, cursor = self._mock_sf_conn()
        cursor.executemany.side_effect = Exception("Snowflake down")
        msg = _make_kafka_message({"payload": [_valid_market_data()]})

        summary = process_batch([msg], conn)

        assert summary["status"] == "error"
