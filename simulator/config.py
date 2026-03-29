"""Simulator configuration handling.

Provides a ``SimulatorConfig`` dataclass with sensible defaults, dataset
size presets, and loaders for CLI arguments and JSON config files.

Requirements: 16.1, 16.3, 16.4, 16.5, 16.6
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Dataset-size presets
# ---------------------------------------------------------------------------

DATASET_PRESETS: dict[str, dict[str, Any]] = {
    "small": {
        "cusip_pool_size": 10,
        "participant_pool_size": 5,
        "volume": 50,
    },
    "medium": {
        "cusip_pool_size": 100,
        "participant_pool_size": 25,
        "volume": 500,
    },
    "large": {
        "cusip_pool_size": 1000,
        "participant_pool_size": 100,
        "volume": 5000,
    },
}


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------

@dataclass
class SimulatorConfig:
    """Configuration for the Data Simulator."""

    data_types: list[str] = field(
        default_factory=lambda: ["trade", "market_data", "security_master", "entity_master"]
    )
    frequency_seconds: int = 10
    volume: int = 50
    mode: str = "batch"  # 'realtime' or 'batch'
    dataset_size: str = "small"  # 'small', 'medium', or 'large'

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )

    price_range: tuple[float, float] = (1.00, 5000.00)
    quantity_range: tuple[int, int] = (1, 100_000)

    # Snowflake connection (for SIMULATION_LOG)
    snowflake_account: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ACCOUNT", ""))
    snowflake_user: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_USER", ""))
    snowflake_password: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_PASSWORD", ""))
    snowflake_warehouse: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    snowflake_database: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", "RISK_MANAGEMENT"))
    snowflake_schema: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_SCHEMA", "CORE"))
    snowflake_role: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"))

    @property
    def cusip_pool_size(self) -> int:
        return DATASET_PRESETS[self.dataset_size]["cusip_pool_size"]

    @property
    def participant_pool_size(self) -> int:
        return DATASET_PRESETS[self.dataset_size]["participant_pool_size"]

    def snowflake_config(self) -> dict[str, str]:
        """Return a dict suitable for ``snowflake.connector.connect``."""
        return {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "password": self.snowflake_password,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
            "role": self.snowflake_role,
        }


def _apply_preset(cfg: SimulatorConfig) -> None:
    """Apply dataset-size preset defaults when volume was not explicitly set."""
    preset = DATASET_PRESETS.get(cfg.dataset_size)
    if preset is None:
        raise ValueError(f"Unknown dataset_size: {cfg.dataset_size!r}")


def load_from_json(path: str) -> SimulatorConfig:
    """Load configuration from a JSON file."""
    with open(path) as fh:
        raw: dict[str, Any] = json.load(fh)

    # Handle nested tuples stored as lists
    if "price_range" in raw and isinstance(raw["price_range"], list):
        raw["price_range"] = tuple(raw["price_range"])
    if "quantity_range" in raw and isinstance(raw["quantity_range"], list):
        raw["quantity_range"] = tuple(raw["quantity_range"])

    cfg = SimulatorConfig(**raw)
    _apply_preset(cfg)
    return cfg


def load_from_cli(args: list[str] | None = None) -> SimulatorConfig:
    """Parse CLI arguments and return a ``SimulatorConfig``."""
    parser = argparse.ArgumentParser(description="Risk Management Data Simulator")
    parser.add_argument("--config", type=str, help="Path to JSON config file")
    parser.add_argument(
        "--data-types", nargs="+",
        default=None,
        help="Data types to generate (trade market_data security_master entity_master)",
    )
    parser.add_argument("--frequency", type=int, default=None, help="Seconds between cycles")
    parser.add_argument("--volume", type=int, default=None, help="Records per batch per data type")
    parser.add_argument("--mode", choices=["realtime", "batch"], default=None)
    parser.add_argument("--dataset-size", choices=["small", "medium", "large"], default=None)
    parser.add_argument("--kafka-bootstrap-servers", type=str, default=None)

    parsed = parser.parse_args(args)

    # If a JSON config file is provided, start from that
    if parsed.config:
        cfg = load_from_json(parsed.config)
    else:
        cfg = SimulatorConfig()

    # CLI overrides
    if parsed.data_types is not None:
        cfg.data_types = parsed.data_types
    if parsed.frequency is not None:
        cfg.frequency_seconds = parsed.frequency
    if parsed.volume is not None:
        cfg.volume = parsed.volume
    if parsed.mode is not None:
        cfg.mode = parsed.mode
    if parsed.dataset_size is not None:
        cfg.dataset_size = parsed.dataset_size
        preset = DATASET_PRESETS[cfg.dataset_size]
        # Only apply preset volume if user didn't explicitly set volume
        if parsed.volume is None:
            cfg.volume = preset["volume"]
    if parsed.kafka_bootstrap_servers is not None:
        cfg.kafka_bootstrap_servers = parsed.kafka_bootstrap_servers

    _apply_preset(cfg)
    return cfg


def default_config() -> SimulatorConfig:
    """Return a ``SimulatorConfig`` with all defaults."""
    cfg = SimulatorConfig()
    _apply_preset(cfg)
    return cfg
