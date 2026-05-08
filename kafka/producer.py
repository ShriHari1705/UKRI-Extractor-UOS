"""
kafka/producer.py — Publishes Innovate UK project records to a Kafka topic.

Use this when running in event-driven / streaming mode instead of batch CSV.
The consumer (consumer.py) subscribes to the same topic and writes to Snowflake.

Usage:
    python kafka/producer.py              # full run
    python kafka/producer.py --pages 5   # dev mode

Architecture:
    run_pipeline.py (batch)    →  CSV → Snowflake (one-shot)
    kafka/producer.py (stream) →  Kafka topic → consumer.py → Snowflake (real-time)
"""
import argparse
import json
import logging
import sys
from pathlib import Path

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC_RAW, TARGET_FUNDER
from pipeline.fetcher import fetch_all_pages
from pipeline.transformer import parse_projects, filter_by_funder

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="UKRI EWS Kafka Producer")
    parser.add_argument("--pages", type=int, default=None, help="Limit to N pages (dev mode)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",          # wait for all replicas to ack — no lost messages
        retries=5,
    )

    log.info(f"Publishing to Kafka topic: {KAFKA_TOPIC_RAW} @ {KAFKA_BOOTSTRAP}")
    sent = 0
    errors = 0

    for page in fetch_all_pages(max_pages=args.pages):
        projects = parse_projects(page)
        matched  = filter_by_funder(projects, TARGET_FUNDER)

        for proj in matched:
            try:
                future = producer.send(
                    KAFKA_TOPIC_RAW,
                    key=proj.id,
                    value={
                        "id":               proj.id,
                        "title":            proj.title,
                        "status":           proj.status,
                        "leadFunder":       proj.leadFunder,
                        "grantCategory":    proj.grantCategory,
                        "abstractText":     proj.abstractText,
                        "techAbstractText": proj.techAbstractText,
                        "potentialImpact":  proj.potentialImpact,
                        "start":            proj.start,
                        "end":              proj.end,
                    },
                )
                future.get(timeout=10)  # block to catch per-message errors
                sent += 1
            except KafkaError as exc:
                log.error(f"Failed to send project {proj.id}: {exc}")
                errors += 1

    producer.flush()
    log.info(f"Done — {sent} messages sent, {errors} errors. Topic: {KAFKA_TOPIC_RAW}")


if __name__ == "__main__":
    main()
