"""Configuration for Kafka and Snowflake connections.

Settings are read from environment variables with sensible defaults for
local development.  Import ``KAFKA_CONFIG`` and ``SNOWFLAKE_CONFIG`` from
this module wherever connection parameters are needed.
"""

import os


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_CONFIG: dict = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "risk-mgmt-ingestion"),
    "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    "enable.auto.commit": False,
}

TRADE_INGESTION_TOPIC: str = os.getenv("TRADE_INGESTION_TOPIC", "trade-ingestion")
MARKET_DATA_TOPIC: str = os.getenv("MARKET_DATA_TOPIC", "market-data")
REFERENCE_DATA_TOPIC: str = os.getenv("REFERENCE_DATA_TOPIC", "reference-data")

# Consumer poll timeout in seconds
KAFKA_POLL_TIMEOUT: float = float(os.getenv("KAFKA_POLL_TIMEOUT", "1.0"))

# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------
SNOWFLAKE_CONFIG: dict = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "RISK_MANAGEMENT"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "CORE"),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
}

# ---------------------------------------------------------------------------
# Ingestion tuning
# ---------------------------------------------------------------------------
# Maximum number of messages to consume per poll batch
BATCH_MAX_MESSAGES: int = int(os.getenv("BATCH_MAX_MESSAGES", "500"))

# Target latency ceiling in milliseconds (used for monitoring / alerting)
TARGET_LATENCY_MS: int = int(os.getenv("TARGET_LATENCY_MS", "5000"))
