"""Market Data Ingestion Service.

Consumes messages from the ``market-data`` Kafka topic, validates
individual market data records, and writes them to Snowflake via the
Snowpipe Streaming API.  Invalid records are routed to the REJECTION_LOG
table and batch metadata is recorded in the INGESTION_LOG table.

Requirements: 2.1, 2.2, 2.3
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
    MARKET_DATA_TOPIC,
    SNOWFLAKE_CONFIG,
    TARGET_LATENCY_MS,
)

logger = logging.getLogger(__name__)

# Required fields that must be present and non-empty in every market data record
REQUIRED_FIELDS = ("cusip", "price", "data_date")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_market_data_record(record: dict[str, Any]) -> str | None:
    """Validate a single market data record.

    Returns ``None`` when the record is valid, or a human-readable rejection
    reason string when it is not.
    """
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

    # data_date must be parseable as a date
    dd = record["data_date"]
    if isinstance(dd, str):
        try:
            datetime.strptime(dd, "%Y-%m-%d")
        except (ValueError, TypeError):
            return f"Invalid data_date format: {dd!r} (expected YYYY-MM-DD)"

    return None


def _parse_message(raw_value: bytes) -> dict[str, Any]:
    """Deserialise a Kafka message value from JSON bytes."""
    return json.loads(raw_value.decode("utf-8"))


# ---------------------------------------------------------------------------
# Snowflake write helpers
# ---------------------------------------------------------------------------

def _insert_market_data(cursor, batch_id: str, records: list[dict[str, Any]]) -> None:
    """Bulk-insert valid market data records into the MARKET_DATA table."""
    if not records:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)  # TIMESTAMP_NTZ

    rows = []
    for rec in records:
        market_data_id = str(uuid.uuid4())
        dd = rec["data_date"]
        if isinstance(dd, str):
            dd = datetime.strptime(dd, "%Y-%m-%d").date()
        rows.append((
            market_data_id,
            str(rec["cusip"]),
            float(rec["price"]),
            float(rec["index_value"]) if rec.get("index_value") is not None else None,
            dd,
            now,
        ))

    cursor.executemany(
        """
        INSERT INTO MARKET_DATA (
            market_data_id, cusip, price, index_value,
            data_date, ingestion_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s)
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
            "market_data",
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
            MARKET_DATA_TOPIC,
            "market_data",
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

    Parses each message, validates individual market data records, inserts
    valid records into MARKET_DATA, rejected records into REJECTION_LOG,
    and writes batch metadata to INGESTION_LOG.

    Returns a summary dict with batch_id, record_count, rejected_count,
    status, and processing_duration_ms.
    """
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    start_ts = time.monotonic()

    valid_records: list[dict[str, Any]] = []
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

        # payload may be a wrapper with a "payload" key, or a direct record/list
        payload = payload_wrapper.get("payload", payload_wrapper)

        # payload may be a single record dict or a list of records
        records = payload if isinstance(payload, list) else [payload]

        for rec in records:
            reason = validate_market_data_record(rec)
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
            _insert_market_data(cursor, batch_id, valid_records)
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

class MarketDataIngestionService:
    """Long-running Kafka consumer that ingests market data into Snowflake."""

    def __init__(self, kafka_config: dict | None = None, sf_config: dict | None = None):
        self._kafka_config = kafka_config or KAFKA_CONFIG
        self._sf_config = sf_config or SNOWFLAKE_CONFIG
        self._running = False
        self._consumer: Consumer | None = None
        self._sf_conn = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Start consuming from the market-data topic."""
        self._running = True
        self._consumer = Consumer(self._kafka_config)
        self._consumer.subscribe([MARKET_DATA_TOPIC])
        self._sf_conn = snowflake.connector.connect(**self._sf_config)

        logger.info("MarketDataIngestionService started on topic %s", MARKET_DATA_TOPIC)

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
        logger.info("MarketDataIngestionService stopped")

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
    """Run the Market Data Ingestion Service as a standalone process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    service = MarketDataIngestionService()

    def _handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down …", signum)
        service.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    service.start()


if __name__ == "__main__":
    main()
