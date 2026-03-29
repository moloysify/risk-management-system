"""Main Data Simulator service.

Publishes synthetic data to Kafka topics and logs activity to the
SIMULATION_LOG table in Snowflake.

Requirements: 16.2, 16.3, 16.5, 16.7, 16.8, 16.12
"""

from __future__ import annotations

import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

from simulator.config import SimulatorConfig, load_from_cli
from simulator.generators import (
    generate_cusip_pool,
    generate_entity_master,
    generate_market_data,
    generate_participant_pool,
    generate_security_master,
    generate_trades,
)

logger = logging.getLogger(__name__)

# Kafka topic mapping
TOPIC_MAP: dict[str, str] = {
    "trade": "trade-ingestion",
    "market_data": "market-data",
    "security_master": "reference-data",
    "entity_master": "reference-data",
}


class DataSimulator:
    """Generates synthetic data and publishes to Kafka topics.

    Supports real-time (continuous) and batch (single-shot) modes.
    """

    def __init__(self, config: SimulatorConfig) -> None:
        self._config = config
        self._running = False
        self._simulation_run_id = str(uuid.uuid4())
        self._producer: Producer | None = None
        self._sf_conn: Any = None

        # Pre-generate pools based on dataset size
        self._cusip_pool = generate_cusip_pool(config.cusip_pool_size)
        self._participant_pool = generate_participant_pool(config.participant_pool_size)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the simulator. Blocks until stopped or batch completes."""
        self._running = True
        self._producer = Producer({
            "bootstrap.servers": self._config.kafka_bootstrap_servers,
        })
        self._sf_conn = self._connect_snowflake()

        logger.info(
            "Simulator started: mode=%s, dataset_size=%s, volume=%d, frequency=%ds",
            self._config.mode,
            self._config.dataset_size,
            self._config.volume,
            self._config.frequency_seconds,
        )

        try:
            if self._config.mode == "batch":
                self._run_cycle()
            else:
                self._realtime_loop()
        finally:
            if self._producer is not None:
                self._producer.flush(timeout=5)
            self._close_snowflake()

    def stop(self) -> None:
        """Signal the simulator to stop. Returns immediately."""
        self._running = False
        logger.info("Simulator stop requested")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _realtime_loop(self) -> None:
        """Continuously generate data at the configured frequency."""
        while self._running:
            self._run_cycle()
            # Sleep in small increments so stop takes effect within 5s
            deadline = time.monotonic() + self._config.frequency_seconds
            while self._running and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))

    def _run_cycle(self) -> None:
        """Execute one generation cycle for all configured data types."""
        for data_type in self._config.data_types:
            if not self._running and self._config.mode == "realtime":
                break
            self._generate_and_publish(data_type)

    def _generate_and_publish(self, data_type: str) -> None:
        """Generate records for *data_type* and publish to Kafka."""
        topic = TOPIC_MAP.get(data_type)
        if topic is None:
            logger.warning("Unknown data_type %r, skipping", data_type)
            return

        records = self._generate_records(data_type)
        status = "success"
        error_message: str | None = None

        try:
            for rec in records:
                self._producer.produce(
                    topic,
                    value=json.dumps(rec).encode("utf-8"),
                )
            self._producer.flush(timeout=10)
        except Exception as exc:
            status = "error"
            error_message = f"Kafka publish failure on topic {topic}: {exc}"
            logger.error(error_message)

        self._log_simulation(
            data_type=data_type,
            kafka_topic=topic,
            record_count=len(records),
            status=status,
            error_message=error_message,
        )

    def _generate_records(self, data_type: str) -> list[dict[str, Any]]:
        """Dispatch to the appropriate generator."""
        cfg = self._config
        if data_type == "trade":
            return generate_trades(
                self._cusip_pool,
                self._participant_pool,
                cfg.volume,
                price_range=cfg.price_range,
                quantity_range=cfg.quantity_range,
            )
        elif data_type == "market_data":
            return generate_market_data(
                self._cusip_pool,
                cfg.volume,
                price_range=cfg.price_range,
            )
        elif data_type == "security_master":
            return generate_security_master(self._cusip_pool)
        elif data_type == "entity_master":
            return generate_entity_master(self._participant_pool)
        else:
            return []

    # ------------------------------------------------------------------
    # Snowflake logging
    # ------------------------------------------------------------------

    def _connect_snowflake(self) -> Any:
        """Open a Snowflake connection for logging, or return None."""
        sf_cfg = self._config.snowflake_config()
        if not sf_cfg.get("account"):
            logger.info("Snowflake account not configured; simulation logging disabled")
            return None
        try:
            import snowflake.connector
            return snowflake.connector.connect(**sf_cfg)
        except Exception:
            logger.warning("Could not connect to Snowflake; simulation logging disabled", exc_info=True)
            return None

    def _close_snowflake(self) -> None:
        if self._sf_conn is not None:
            try:
                self._sf_conn.close()
            except Exception:
                logger.debug("Error closing Snowflake connection", exc_info=True)
            self._sf_conn = None

    def _log_simulation(
        self,
        data_type: str,
        kafka_topic: str,
        record_count: int,
        status: str,
        error_message: str | None,
    ) -> None:
        """Write a row to the SIMULATION_LOG table."""
        if self._sf_conn is None:
            return
        try:
            cursor = self._sf_conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO SIMULATION_LOG (
                        simulation_log_id, simulation_run_id, data_type,
                        kafka_topic, record_count, mode, dataset_size,
                        status, error_message, generated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        self._simulation_run_id,
                        data_type,
                        kafka_topic,
                        record_count,
                        self._config.mode,
                        self._config.dataset_size,
                        status,
                        error_message,
                        datetime.now(timezone.utc).replace(tzinfo=None),
                    ),
                )
            finally:
                cursor.close()
        except Exception:
            logger.warning("Failed to write SIMULATION_LOG", exc_info=True)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for the Data Simulator."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_from_cli()
    simulator = DataSimulator(config)

    def _handle_signal(signum, frame):
        simulator.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    simulator.start()


if __name__ == "__main__":
    main()
