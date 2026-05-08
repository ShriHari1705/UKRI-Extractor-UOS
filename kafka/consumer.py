"""
kafka/consumer.py — Reads project events from Kafka and writes to Snowflake.

Runs as a long-lived process. Accumulates events into batches before writing
to Snowflake to reduce connector overhead (configurable via BATCH_SIZE).

Usage:
    python kafka/consumer.py

Stop cleanly with Ctrl+C — the current batch is flushed before exit.
"""
import json
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC_RAW, KAFKA_GROUP_ID
from pipeline.models import UKRIProjectRecord
from pipeline.transformer import tag_project
from pipeline.loader import load_to_snowflake

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BATCH_SIZE = 50   # flush to Snowflake every N projects


def flush_batch(batch_rows: list) -> list:
    """Write accumulated rows to Snowflake and return an empty list."""
    if not batch_rows:
        return []
    df = pd.DataFrame(batch_rows)
    log.info(f"Flushing {len(batch_rows)} rows to Snowflake…")
    load_to_snowflake(df, overwrite=False)
    return []


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=30_000,  # 30s idle timeout before loop exits
    )

    log.info(f"Consuming from '{KAFKA_TOPIC_RAW}' (group: {KAFKA_GROUP_ID})")

    batch_rows: list[dict] = []

    # Graceful shutdown on Ctrl+C / SIGTERM
    def _shutdown(sig, frame):
        log.info("Shutdown signal received — flushing final batch…")
        flush_batch(batch_rows)
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for message in consumer:
        raw = message.value
        try:
            project = UKRIProjectRecord(**raw)
            rows    = tag_project(project)
            batch_rows.extend(rows)
        except Exception as exc:
            log.warning(f"Failed to process project {raw.get('id', '?')}: {exc}")
            continue

        if len(batch_rows) >= BATCH_SIZE:
            batch_rows = flush_batch(batch_rows)

    # Flush any remaining rows when the topic goes quiet
    if batch_rows:
        flush_batch(batch_rows)

    log.info("Consumer finished.")


if __name__ == "__main__":
    main()
