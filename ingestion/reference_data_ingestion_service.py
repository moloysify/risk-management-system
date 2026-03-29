"""Reference Data Ingestion Service.

Consumes messages from the ``reference-data`` Kafka topic, validates
individual reference data records, routes them to the appropriate
Snowflake table (SECURITY_MASTER or ENTITY_MASTER), and maintains
version history using SCD Type 2 pattern.  Invalid records are routed
to the REJECTION_LOG table and batch metadata is recorded in the
INGESTION_LOG table.

Requirements: 3.1, 3.2, 3.3, 3.4
"""

from __future__ import annotations

import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import snowflake.connector
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from ingestion.config import (
    BATCH_MAX_MESSAGES,
    KAFKA_CONFIG,
    KAFKA_POLL_TIMEOUT,
    REFERENCE_DATA_TOPIC,
    SNOWFLAKE_CONFIG,
    TARGET_LATENCY_MS,
)

logger = logging.getLogger(__name__)

# Valid data_type values that determine routing
VALID_DATA_TYPES = {"security_master", "entity_master"}

# Required fields per data type
SECURITY_MASTER_REQUIRED_FIELDS = ("cusip", "security_name", "issuer", "security_type", "sector")
ENTITY_MASTER_REQUIRED_FIELDS = ("participant_id", "participant_name", "entity_type", "risk_limit")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_reference_data_record(record: dict[str, Any], data_type: str) -> str | None:
    """Validate a single reference data record.

    Returns ``None`` when the record is valid, or a human-readable rejection
    reason string when it is not.
    """
    if data_type == "security_master":
        required = SECURITY_MASTER_REQUIRED_FIELDS
    elif data_type == "entity_master":
        required = ENTITY_MASTER_REQUIRED_FIELDS
    else:
        return f"Unknown data_type: {data_type!r}"

    for field in required:
        if field not in record or record[field] is None or record[field] == "":
            return f"Missing or empty required field: {field}"

    # For entity_master, risk_limit must be numeric
    if data_type == "entity_master":
        try:
            float(record["risk_limit"])
        except (TypeError, ValueError):
            return f"Invalid risk_limit value: {record['risk_limit']!r} (must be numeric)"

    return None


def _parse_message(raw_value: bytes) -> dict[str, Any]:
    """Deserialise a Kafka message value from JSON bytes."""
    return json.loads(raw_value.decode("utf-8"))


# ---------------------------------------------------------------------------
# Snowflake write helpers
# ---------------------------------------------------------------------------

def _expire_current_security_master(cursor, cusip: str, now: datetime) -> int:
    """Set is_current=FALSE and valid_to=now on the current SECURITY_MASTER row.

    Returns the version number of the expired row, or 0 if no current row exists.
    """
    cursor.execute(
        """
        SELECT version FROM SECURITY_MASTER
        WHERE cusip = %s AND is_current = TRUE
        ORDER BY version DESC
        LIMIT 1
        """,
        (cusip,),
    )
    row = cursor.fetchone()
    if row is None:
        return 0

    old_version = row[0]
    cursor.execute(
        """
        UPDATE SECURITY_MASTER
        SET is_current = FALSE, valid_to = %s
        WHERE cusip = %s AND is_current = TRUE
        """,
        (now, cusip),
    )
    return old_version


def _insert_security_master(cursor, batch_id: str, records: list[dict[str, Any]]) -> None:
    """Insert security master records with SCD2 versioning."""
    if not records:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for rec in records:
        cusip = str(rec["cusip"])
        old_version = _expire_current_security_master(cursor, cusip, now)
        new_version = old_version + 1

        cursor.execute(
            """
            INSERT INTO SECURITY_MASTER (
                security_master_id, cusip, security_name, issuer,
                security_type, sector, valid_from, valid_to,
                is_current, version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                cusip,
                str(rec["security_name"]),
                str(rec["issuer"]),
                str(rec["security_type"]),
                str(rec["sector"]),
                now,
                None,
                True,
                new_version,
            ),
        )


def _expire_current_entity_master(cursor, participant_id: str, now: datetime) -> int:
    """Set is_current=FALSE and valid_to=now on the current ENTITY_MASTER row.

    Returns the version number of the expired row, or 0 if no current row exists.
    """
    cursor.execute(
        """
        SELECT version FROM ENTITY_MASTER
        WHERE participant_id = %s AND is_current = TRUE
        ORDER BY version DESC
        LIMIT 1
        """,
        (participant_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return 0

    old_version = row[0]
    cursor.execute(
        """
        UPDATE ENTITY_MASTER
        SET is_current = FALSE, valid_to = %s
        WHERE participant_id = %s AND is_current = TRUE
        """,
        (now, participant_id),
    )
    return old_version


def _insert_entity_master(cursor, batch_id: str, records: list[dict[str, Any]]) -> None:
    """Insert entity master records with SCD2 versioning."""
    if not records:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for rec in records:
        participant_id = str(rec["participant_id"])
        old_version = _expire_current_entity_master(cursor, participant_id, now)
        new_version = old_version + 1

        cursor.execute(
            """
            INSERT INTO ENTITY_MASTER (
                entity_master_id, participant_id, participant_name,
                entity_type, risk_limit, valid_from, valid_to,
                is_current, version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                participant_id,
                str(rec["participant_name"]),
                str(rec["entity_type"]),
                float(rec["risk_limit"]),
                now,
                None,
                True,
                new_version,
            ),
        )


def _insert_rejections(cursor, batch_id: str, rejections: list[tuple[dict, str]]) -> None:
    """Bulk-insert rejected records into the REJECTION_LOG table."""
    if not rejections:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    rows = []
    for raw_record, reason in rejections:
        rows.append((
            str(uuid.uuid4()),
            batch_id,
            "reference_data",
            json.dumps(raw_record),
            reason,
            now,
        ))

    cursor.executemany(
        """
        INSERT INTO REJECTION_LOG (
            rejection_id, batch_id, data_type, raw_record, rejection_reason, rejection_timestamp
        ) VALUES (%s, %s, %s, PARSE_JSON(%s), %s, %s)
        """,
        rows,
    )


def _insert_ingestion_log(
    cursor,
    batch_id: str,
    record_count: int,
    rejected_count: int,
    status: str,
    processing_duration_ms: int,
    started_at: datetime,
    completed_at: datetime,
) -> None:
    """Write a single row to the INGESTION_LOG table."""
    cursor.execute(
        """
        INSERT INTO INGESTION_LOG (
            log_id, batch_id, kafka_topic, data_type,
            record_count, rejected_count, status,
            processing_duration_ms, started_at, completed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(uuid.uuid4()),
            batch_id,
            REFERENCE_DATA_TOPIC,
            "reference_data",
            record_count,
            rejected_count,
            status,
            processing_duration_ms,
            started_at,
            completed_at,
        ),
    )


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_batch(messages: list[Message], sf_conn) -> dict[str, Any]:
    """Process a batch of Kafka messages.

    Parses each message, validates individual reference data records,
    routes valid records to SECURITY_MASTER or ENTITY_MASTER with SCD2
    versioning, inserts rejected records into REJECTION_LOG, and writes
    batch metadata to INGESTION_LOG.

    Returns a summary dict with batch_id, record_count, rejected_count,
    status, and processing_duration_ms.
    """
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    start_ts = time.monotonic()

    security_master_records: list[dict[str, Any]] = []
    entity_master_records: list[dict[str, Any]] = []
    rejections: list[tuple[dict, str]] = []

    for msg in messages:
        raw_value = msg.value()
        if raw_value is None:
            continue

        try:
            payload_wrapper = _parse_message(raw_value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            rejections.append(
                ({"raw": raw_value.decode("utf-8", errors="replace")},
                 f"Malformed message payload: {exc}")
            )
            continue

        # Extract data_type for routing
        data_type = payload_wrapper.get("data_type")
        if data_type not in VALID_DATA_TYPES:
            rejections.append(
                (payload_wrapper, f"Missing or invalid data_type: {data_type!r}")
            )
            continue

        # payload may be a wrapper with a "payload" key, or records at top level
        payload = payload_wrapper.get("payload", payload_wrapper)

        # payload may be a single record dict or a list of records
        records = payload if isinstance(payload, list) else [payload]

        for rec in records:
            reason = validate_reference_data_record(rec, data_type)
            if reason is None:
                if data_type == "security_master":
                    security_master_records.append(rec)
                else:
                    entity_master_records.append(rec)
            else:
                rejections.append((rec, reason))

    # Write to Snowflake
    total_count = len(security_master_records) + len(entity_master_records) + len(rejections)
    status = "success"
    valid_count = len(security_master_records) + len(entity_master_records)

    try:
        cursor = sf_conn.cursor()
        try:
            _insert_security_master(cursor, batch_id, security_master_records)
            _insert_entity_master(cursor, batch_id, entity_master_records)
            _insert_rejections(cursor, batch_id, rejections)
        finally:
            cursor.close()
    except Exception:
        logger.exception("Snowflake write failed for batch %s", batch_id)
        status = "error"

    if status != "error":
        if rejections and valid_count:
            status = "partial"
        elif rejections and not valid_count:
            status = "error"

    completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    duration_ms = int((time.monotonic() - start_ts) * 1000)

    if duration_ms > TARGET_LATENCY_MS:
        logger.warning(
            "Batch %s exceeded target latency: %d ms (target %d ms)",
            batch_id, duration_ms, TARGET_LATENCY_MS,
        )

    # Write ingestion log
    try:
        cursor = sf_conn.cursor()
        try:
            _insert_ingestion_log(
                cursor, batch_id, total_count, len(rejections),
                status, duration_ms, started_at, completed_at,
            )
        finally:
            cursor.close()
    except Exception:
        logger.exception("Failed to write INGESTION_LOG for batch %s", batch_id)

    return {
        "batch_id": batch_id,
        "record_count": total_count,
        "rejected_count": len(rejections),
        "status": status,
        "processing_duration_ms": duration_ms,
    }


# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

class ReferenceDataIngestionService:
    """Long-running Kafka consumer that ingests reference data into Snowflake."""

    def __init__(self, kafka_config: dict | None = None, sf_config: dict | None = None):
        self._kafka_config = kafka_config or KAFKA_CONFIG
        self._sf_config = sf_config or SNOWFLAKE_CONFIG
        self._running = False
        self._consumer: Consumer | None = None
        self._sf_conn = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Start consuming from the reference-data topic."""
        self._running = True
        self._consumer = Consumer(self._kafka_config)
        self._consumer.subscribe([REFERENCE_DATA_TOPIC])
        self._sf_conn = snowflake.connector.connect(**self._sf_config)

        logger.info("ReferenceDataIngestionService started on topic %s", REFERENCE_DATA_TOPIC)

        try:
            self._consume_loop()
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully shut down the consumer and Snowflake connection."""
        self._running = False
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                logger.exception("Error closing Kafka consumer")
            self._consumer = None
        if self._sf_conn is not None:
            try:
                self._sf_conn.close()
            except Exception:
                logger.exception("Error closing Snowflake connection")
            self._sf_conn = None
        logger.info("ReferenceDataIngestionService stopped")

    def _consume_loop(self) -> None:
        """Poll Kafka and process messages in batches."""
        while self._running:
            batch: list[Message] = []
            try:
                msg = self._consumer.poll(timeout=KAFKA_POLL_TIMEOUT)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                batch.append(msg)

                # Drain up to BATCH_MAX_MESSAGES without blocking
                while len(batch) < BATCH_MAX_MESSAGES:
                    extra = self._consumer.poll(timeout=0)
                    if extra is None:
                        break
                    if extra.error():
                        if extra.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(extra.error())
                    batch.append(extra)

            except KafkaException:
                logger.exception("Kafka consumer error")
                continue

            if batch:
                summary = process_batch(batch, self._sf_conn)
                logger.info(
                    "Batch %s processed: %d records, %d rejected, status=%s, %d ms",
                    summary["batch_id"],
                    summary["record_count"],
                    summary["rejected_count"],
                    summary["status"],
                    summary["processing_duration_ms"],
                )
                # Commit offsets after successful processing
                self._consumer.commit(asynchronous=False)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the Reference Data Ingestion Service as a standalone process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    service = ReferenceDataIngestionService()

    def _handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down …", signum)
        service.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    service.start()


if __name__ == "__main__":
    main()
