"""Trade Ingestion Service.

Consumes messages from the ``trade-ingestion`` Kafka topic, validates
individual trade records, and writes them to Snowflake via the Snowpipe
Streaming API.  Invalid records are routed to the REJECTION_LOG table and
batch metadata is recorded in the INGESTION_LOG table.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
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
    SNOWFLAKE_CONFIG,
    TARGET_LATENCY_MS,
    TRADE_INGESTION_TOPIC,
)

logger = logging.getLogger(__name__)

# Valid source format values
VALID_FORMATS = {"bulk", "snapshot", "voice"}

# Required fields that must be present and non-empty in every trade record
REQUIRED_FIELDS = ("ticker", "price", "quantity", "participant_id", "trade_timestamp")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_trade_record(record: dict[str, Any]) -> str | None:
    """Validate a single trade record.

    Returns ``None`` when the record is valid, or a human-readable rejection
    reason string when it is not.
    """
    # Check required fields are present and non-empty
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None or record[field] == "":
            return f"Missing or empty required field: {field}"

    # Price must be numeric and > 0
    try:
        price = float(record["price"])
    except (TypeError, ValueError):
        return f"Invalid price value: {record['price']!r} (must be numeric)"
    if price <= 0:
        return f"Price must be > 0, got {price}"

    # Quantity must be numeric and > 0
    try:
        quantity = float(record["quantity"])
    except (TypeError, ValueError):
        return f"Invalid quantity value: {record['quantity']!r} (must be numeric)"
    if quantity <= 0:
        return f"Quantity must be > 0, got {quantity}"

    # trade_timestamp must be parseable
    ts = record["trade_timestamp"]
    if isinstance(ts, str):
        try:
            datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            return f"Invalid trade_timestamp format: {ts!r}"

    return None


def _parse_message(raw_value: bytes) -> dict[str, Any]:
    """Deserialise a Kafka message value from JSON bytes."""
    return json.loads(raw_value.decode("utf-8"))


# ---------------------------------------------------------------------------
# Snowflake write helpers
# ---------------------------------------------------------------------------

def _insert_trades(cursor, batch_id: str, source_format: str, records: list[dict[str, Any]]) -> None:
    """Bulk-insert valid trade records into the TRADES table."""
    if not records:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)  # TIMESTAMP_NTZ

    rows = []
    for rec in records:
        trade_id = str(uuid.uuid4())
        ts = rec["trade_timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        rows.append((
            trade_id,
            batch_id,
            str(rec["ticker"]),
            str(rec.get("cusip", "")),
            float(rec["price"]),
            float(rec["quantity"]),
            str(rec["participant_id"]),
            ts,
            source_format,
            now,
        ))

    cursor.executemany(
        """
        INSERT INTO TRADES (
            trade_id, batch_id, ticker, cusip, price, quantity,
            participant_id, trade_timestamp, source_format, ingestion_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
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
            "trade",
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
            TRADE_INGESTION_TOPIC,
            "trade",
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

    Parses each message, validates individual trade records, inserts valid
    records into TRADES, rejected records into REJECTION_LOG, and writes
    batch metadata to INGESTION_LOG.

    Returns a summary dict with batch_id, record_count, rejected_count,
    status, and processing_duration_ms.
    """
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    start_ts = time.monotonic()

    valid_records: list[dict[str, Any]] = []
    rejections: list[tuple[dict, str]] = []
    source_format: str = "bulk"  # default

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

        # Extract format and payload
        fmt = payload_wrapper.get("format", "bulk")
        if fmt not in VALID_FORMATS:
            rejections.append(
                (payload_wrapper, f"Unknown format: {fmt!r}")
            )
            continue
        source_format = fmt

        payload = payload_wrapper.get("payload")
        if payload is None:
            rejections.append(
                (payload_wrapper, "Missing 'payload' field in message")
            )
            continue

        # payload may be a single record dict or a list of records
        records = payload if isinstance(payload, list) else [payload]

        for rec in records:
            reason = validate_trade_record(rec)
            if reason is None:
                valid_records.append(rec)
            else:
                rejections.append((rec, reason))

    # Write to Snowflake
    total_count = len(valid_records) + len(rejections)
    status = "success"

    try:
        cursor = sf_conn.cursor()
        try:
            _insert_trades(cursor, batch_id, source_format, valid_records)
            _insert_rejections(cursor, batch_id, rejections)
        finally:
            cursor.close()
    except Exception:
        logger.exception("Snowflake write failed for batch %s", batch_id)
        status = "error"

    if status != "error":
        if rejections and valid_records:
            status = "partial"
        elif rejections and not valid_records:
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

class TradeIngestionService:
    """Long-running Kafka consumer that ingests trade data into Snowflake."""

    def __init__(self, kafka_config: dict | None = None, sf_config: dict | None = None):
        self._kafka_config = kafka_config or KAFKA_CONFIG
        self._sf_config = sf_config or SNOWFLAKE_CONFIG
        self._running = False
        self._consumer: Consumer | None = None
        self._sf_conn = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Start consuming from the trade-ingestion topic."""
        self._running = True
        self._consumer = Consumer(self._kafka_config)
        self._consumer.subscribe([TRADE_INGESTION_TOPIC])
        self._sf_conn = snowflake.connector.connect(**self._sf_config)

        logger.info("TradeIngestionService started on topic %s", TRADE_INGESTION_TOPIC)

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
        logger.info("TradeIngestionService stopped")

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
    """Run the Trade Ingestion Service as a standalone process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    service = TradeIngestionService()

    def _handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down …", signum)
        service.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    service.start()


if __name__ == "__main__":
    main()
