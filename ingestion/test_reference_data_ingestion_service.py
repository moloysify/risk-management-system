"""Unit tests for the Reference Data Ingestion Service.

Tests cover validation logic, message parsing, SCD2 versioning,
routing by data_type, and batch processing.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from ingestion.reference_data_ingestion_service import (
    VALID_DATA_TYPES,
    _expire_current_entity_master,
    _expire_current_security_master,
    _insert_entity_master,
    _insert_security_master,
    _parse_message,
    process_batch,
    validate_reference_data_record,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_security_master(**overrides) -> dict[str, Any]:
    """Return a minimal valid security master record."""
    base = {
        "cusip": "037833100",
        "security_name": "Apple Inc.",
        "issuer": "Apple Inc.",
        "security_type": "equity",
        "sector": "Technology",
    }
    base.update(overrides)
    return base


def _valid_entity_master(**overrides) -> dict[str, Any]:
    """Return a minimal valid entity master record."""
    base = {
        "participant_id": str(uuid.uuid4()),
        "participant_name": "Alpha Trading Desk",
        "entity_type": "desk",
        "risk_limit": 1000000.00,
    }
    base.update(overrides)
    return base


def _make_kafka_message(payload_wrapper: dict) -> MagicMock:
    """Create a mock Kafka Message whose .value() returns JSON bytes."""
    msg = MagicMock()
    msg.value.return_value = json.dumps(payload_wrapper).encode("utf-8")
    msg.error.return_value = None
    return msg


def _mock_sf_conn(fetchone_return=None):
    """Create a mock Snowflake connection with cursor.

    ``fetchone_return`` controls what cursor.fetchone() returns for
    SCD2 version lookups.  Defaults to None (no existing record).
    """
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_return
    conn.cursor.return_value = cursor
    return conn, cursor


# ---------------------------------------------------------------------------
# validate_reference_data_record — Security Master
# ---------------------------------------------------------------------------

class TestValidateSecurityMaster:
    """Tests for security_master validation."""

    def test_valid_record_returns_none(self):
        assert validate_reference_data_record(_valid_security_master(), "security_master") is None

    @pytest.mark.parametrize("field", ["cusip", "security_name", "issuer", "security_type", "sector"])
    def test_missing_required_field(self, field):
        rec = _valid_security_master()
        del rec[field]
        reason = validate_reference_data_record(rec, "security_master")
        assert reason is not None
        assert field in reason

    @pytest.mark.parametrize("field", ["cusip", "security_name", "issuer", "security_type", "sector"])
    def test_empty_string_required_field(self, field):
        rec = _valid_security_master(**{field: ""})
        reason = validate_reference_data_record(rec, "security_master")
        assert reason is not None

    @pytest.mark.parametrize("field", ["cusip", "security_name", "issuer", "security_type", "sector"])
    def test_none_required_field(self, field):
        rec = _valid_security_master(**{field: None})
        reason = validate_reference_data_record(rec, "security_master")
        assert reason is not None

    def test_unknown_data_type_rejected(self):
        reason = validate_reference_data_record(_valid_security_master(), "unknown_type")
        assert reason is not None
        assert "Unknown data_type" in reason


# ---------------------------------------------------------------------------
# validate_reference_data_record — Entity Master
# ---------------------------------------------------------------------------

class TestValidateEntityMaster:
    """Tests for entity_master validation."""

    def test_valid_record_returns_none(self):
        assert validate_reference_data_record(_valid_entity_master(), "entity_master") is None

    @pytest.mark.parametrize("field", ["participant_id", "participant_name", "entity_type", "risk_limit"])
    def test_missing_required_field(self, field):
        rec = _valid_entity_master()
        del rec[field]
        reason = validate_reference_data_record(rec, "entity_master")
        assert reason is not None
        assert field in reason

    @pytest.mark.parametrize("field", ["participant_id", "participant_name", "entity_type"])
    def test_empty_string_required_field(self, field):
        rec = _valid_entity_master(**{field: ""})
        reason = validate_reference_data_record(rec, "entity_master")
        assert reason is not None

    def test_risk_limit_non_numeric_rejected(self):
        rec = _valid_entity_master(risk_limit="abc")
        reason = validate_reference_data_record(rec, "entity_master")
        assert reason is not None
        assert "risk_limit" in reason

    def test_risk_limit_zero_accepted(self):
        """Zero is a valid risk_limit (no positivity constraint)."""
        assert validate_reference_data_record(_valid_entity_master(risk_limit=0), "entity_master") is None


# ---------------------------------------------------------------------------
# _parse_message
# ---------------------------------------------------------------------------

class TestParseMessage:
    def test_valid_json(self):
        data = {"data_type": "security_master", "payload": [_valid_security_master()]}
        result = _parse_message(json.dumps(data).encode("utf-8"))
        assert result["data_type"] == "security_master"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _parse_message(b"not json")


# ---------------------------------------------------------------------------
# SCD2 versioning helpers
# ---------------------------------------------------------------------------

class TestSCD2SecurityMaster:
    """Tests for SCD2 versioning on SECURITY_MASTER."""

    def test_new_record_gets_version_1(self):
        """When no existing current record, version should be 1."""
        conn, cursor = _mock_sf_conn(fetchone_return=None)

        _insert_security_master(cursor, "batch-1", [_valid_security_master(cusip="ABC123456")])

        # Should have queried for existing, then inserted
        assert cursor.execute.call_count == 2  # SELECT + INSERT
        insert_call = cursor.execute.call_args_list[1]
        insert_args = insert_call[0][1]
        # version is the last arg
        assert insert_args[-1] == 1  # version
        assert insert_args[-2] is True  # is_current

    def test_existing_record_gets_incremented_version(self):
        """When an existing current record exists at version 3, new should be 4."""
        conn, cursor = _mock_sf_conn(fetchone_return=(3,))

        _insert_security_master(cursor, "batch-1", [_valid_security_master(cusip="ABC123456")])

        # SELECT + UPDATE (expire) + INSERT
        assert cursor.execute.call_count == 3
        insert_call = cursor.execute.call_args_list[2]
        insert_args = insert_call[0][1]
        assert insert_args[-1] == 4  # version = old + 1
        assert insert_args[-2] is True  # is_current

    def test_expire_sets_valid_to_and_is_current_false(self):
        """The UPDATE call should set is_current=FALSE and valid_to."""
        conn, cursor = _mock_sf_conn(fetchone_return=(2,))

        _insert_security_master(cursor, "batch-1", [_valid_security_master(cusip="XYZ789012")])

        update_call = cursor.execute.call_args_list[1]
        update_sql = update_call[0][0]
        assert "is_current = FALSE" in update_sql
        assert "valid_to" in update_sql


class TestSCD2EntityMaster:
    """Tests for SCD2 versioning on ENTITY_MASTER."""

    def test_new_record_gets_version_1(self):
        conn, cursor = _mock_sf_conn(fetchone_return=None)
        pid = "PART-001"

        _insert_entity_master(cursor, "batch-1", [_valid_entity_master(participant_id=pid)])

        assert cursor.execute.call_count == 2  # SELECT + INSERT
        insert_call = cursor.execute.call_args_list[1]
        insert_args = insert_call[0][1]
        assert insert_args[-1] == 1  # version

    def test_existing_record_gets_incremented_version(self):
        conn, cursor = _mock_sf_conn(fetchone_return=(5,))
        pid = "PART-002"

        _insert_entity_master(cursor, "batch-1", [_valid_entity_master(participant_id=pid)])

        assert cursor.execute.call_count == 3  # SELECT + UPDATE + INSERT
        insert_call = cursor.execute.call_args_list[2]
        insert_args = insert_call[0][1]
        assert insert_args[-1] == 6  # version = 5 + 1

    def test_expire_sets_valid_to_and_is_current_false(self):
        conn, cursor = _mock_sf_conn(fetchone_return=(1,))
        pid = "PART-003"

        _insert_entity_master(cursor, "batch-1", [_valid_entity_master(participant_id=pid)])

        update_call = cursor.execute.call_args_list[1]
        update_sql = update_call[0][0]
        assert "is_current = FALSE" in update_sql
        assert "valid_to" in update_sql


# ---------------------------------------------------------------------------
# process_batch (with mocked Snowflake)
# ---------------------------------------------------------------------------

class TestProcessBatch:
    """Tests for process_batch using a mocked Snowflake connection."""

    def test_security_master_records_routed_correctly(self):
        conn, cursor = _mock_sf_conn(fetchone_return=None)
        records = [_valid_security_master()]
        msg = _make_kafka_message({
            "data_type": "security_master",
            "payload": records,
        })

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 1
        assert summary["rejected_count"] == 0
        assert summary["status"] == "success"

    def test_entity_master_records_routed_correctly(self):
        conn, cursor = _mock_sf_conn(fetchone_return=None)
        records = [_valid_entity_master()]
        msg = _make_kafka_message({
            "data_type": "entity_master",
            "payload": records,
        })

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 1
        assert summary["rejected_count"] == 0
        assert summary["status"] == "success"

    def test_invalid_data_type_rejected(self):
        conn, cursor = _mock_sf_conn()
        msg = _make_kafka_message({
            "data_type": "unknown",
            "payload": [_valid_security_master()],
        })

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == 1

    def test_missing_data_type_rejected(self):
        conn, cursor = _mock_sf_conn()
        msg = _make_kafka_message({
            "payload": [_valid_security_master()],
        })

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == 1

    def test_all_invalid_records(self):
        conn, cursor = _mock_sf_conn()
        bad_records = [{"cusip": "X"}, {"security_name": "only name"}]
        msg = _make_kafka_message({
            "data_type": "security_master",
            "payload": bad_records,
        })

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == summary["record_count"]
        assert summary["status"] == "error"

    def test_mixed_valid_and_invalid(self):
        conn, cursor = _mock_sf_conn(fetchone_return=None)
        records = [_valid_security_master(), {"cusip": "BAD"}]
        msg = _make_kafka_message({
            "data_type": "security_master",
            "payload": records,
        })

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 2
        assert summary["rejected_count"] == 1
        assert summary["status"] == "partial"

    def test_malformed_json_message(self):
        conn, _ = _mock_sf_conn()
        msg = MagicMock()
        msg.value.return_value = b"{{bad json"
        msg.error.return_value = None

        summary = process_batch([msg], conn)

        assert summary["rejected_count"] == 1

    def test_batch_id_is_unique_uuid(self):
        conn, _ = _mock_sf_conn(fetchone_return=None)
        msg = _make_kafka_message({
            "data_type": "security_master",
            "payload": [_valid_security_master()],
        })

        s1 = process_batch([msg], conn)
        s2 = process_batch([msg], conn)

        assert s1["batch_id"] != s2["batch_id"]
        uuid.UUID(s1["batch_id"])
        uuid.UUID(s2["batch_id"])

    def test_processing_duration_is_positive(self):
        conn, _ = _mock_sf_conn(fetchone_return=None)
        msg = _make_kafka_message({
            "data_type": "entity_master",
            "payload": [_valid_entity_master()],
        })

        summary = process_batch([msg], conn)

        assert summary["processing_duration_ms"] >= 0

    def test_single_record_payload_not_list(self):
        """payload can be a single dict instead of a list."""
        conn, _ = _mock_sf_conn(fetchone_return=None)
        msg = _make_kafka_message({
            "data_type": "entity_master",
            "payload": _valid_entity_master(),
        })

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 1
        assert summary["rejected_count"] == 0

    def test_null_message_value_skipped(self):
        conn, _ = _mock_sf_conn()
        msg = MagicMock()
        msg.value.return_value = None
        msg.error.return_value = None

        summary = process_batch([msg], conn)

        assert summary["record_count"] == 0

    def test_snowflake_write_failure_sets_error_status(self):
        conn, cursor = _mock_sf_conn()
        cursor.execute.side_effect = Exception("Snowflake down")
        msg = _make_kafka_message({
            "data_type": "security_master",
            "payload": [_valid_security_master()],
        })

        summary = process_batch([msg], conn)

        assert summary["status"] == "error"

    def test_mixed_data_types_in_separate_messages(self):
        """Two messages with different data_types in the same batch."""
        conn, cursor = _mock_sf_conn(fetchone_return=None)
        msg_sec = _make_kafka_message({
            "data_type": "security_master",
            "payload": [_valid_security_master()],
        })
        msg_ent = _make_kafka_message({
            "data_type": "entity_master",
            "payload": [_valid_entity_master()],
        })

        summary = process_batch([msg_sec, msg_ent], conn)

        assert summary["record_count"] == 2
        assert summary["rejected_count"] == 0
        assert summary["status"] == "success"
