"""Launcher for all three ingestion services in separate threads."""
import os
import sys
import threading
import signal
import logging

# Set env vars before importing services — reads from environment
# Source setup_env.sh or set these env vars before running
required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing = [v for v in required_vars if not os.environ.get(v)]
if missing:
    print(f"Missing env vars: {', '.join(missing)}")
    print("Run: source setup_env.sh")
    sys.exit(1)

from ingestion.trade_ingestion_service import TradeIngestionService
from ingestion.market_data_ingestion_service import MarketDataIngestionService
from ingestion.reference_data_ingestion_service import ReferenceDataIngestionService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

services = []

def start_all():
    trade_svc = TradeIngestionService()
    market_svc = MarketDataIngestionService()
    ref_svc = ReferenceDataIngestionService()
    services.extend([trade_svc, market_svc, ref_svc])

    threads = [
        threading.Thread(target=trade_svc.start, name="TradeIngestion", daemon=True),
        threading.Thread(target=market_svc.start, name="MarketDataIngestion", daemon=True),
        threading.Thread(target=ref_svc.start, name="ReferenceDataIngestion", daemon=True),
    ]
    for t in threads:
        t.start()
        print(f"Started {t.name}")
    return threads

def stop_all(signum=None, frame=None):
    print("\nStopping all services...")
    for svc in services:
        svc.stop()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)
    threads = start_all()
    print("All ingestion services running. Press Ctrl+C to stop.")
    for t in threads:
        t.join()
