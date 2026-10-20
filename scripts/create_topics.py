"""Idempotent Kafka topic provisioning for the weather streaming path.

Creates only the topics that do not already exist, so it is safe to run
repeatedly (locally, from CI, or from a Makefile target).

Usage:
    python scripts/create_topics.py

Configuration is read from environment variables (a local ``.env`` file
is loaded automatically). See ``.env.example`` for the full list.
"""

from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("create_topics")

TOPIC_SPECS: dict[str, dict[str, int]] = {
    # Raw readings exactly as produced (partition key = station id).
    "KAFKA_TOPIC_RAW": {"num_partitions": 3},
    # Transformed/validated rows, shaped like the DB row dict.
    "KAFKA_TOPIC_PROCESSED": {"num_partitions": 3},
    # Rejected records (parse/validation failures). Single partition:
    # DLQ volume is expected to be tiny; ordering of failures matters
    # more than throughput here.
    "KAFKA_TOPIC_DLQ": {"num_partitions": 1},
}


def build_admin_client(bootstrap_servers: str) -> AdminClient:
    """Create an AdminClient for the given bootstrap servers.

    Args:
        bootstrap_servers: Comma-separated ``host:port`` list.

    Returns:
        A configured :class:`AdminClient` instance.
    """
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def create_missing_topics(admin: AdminClient, timeout_s: float = 30.0) -> int:
    """Create all configured topics that do not exist yet.

    Existing topics are left untouched, which makes this function
    idempotent and safe to call on every run.

    Args:
        admin: Connected :class:`AdminClient`.
        timeout_s: Seconds to wait for each creation result.

    Returns:
        Number of topics created.

    Raises:
        RuntimeError: If any topic creation fails.
    """
    topic_names = {
        env_key: (os.getenv(env_key) or f"weather.readings.{suffix}")
        for env_key, suffix in (
            ("KAFKA_TOPIC_RAW", "raw"),
            ("KAFKA_TOPIC_PROCESSED", "processed"),
            ("KAFKA_TOPIC_DLQ", "dlq"),
        )
    }

    existing = set(admin.list_topics(timeout=timeout_s).topics)
    logger.info("Existing topics: %s", sorted(existing) or "(none)")

    missing = [
        NewTopic(
            topic=name,
            num_partitions=TOPIC_SPECS[env_key]["num_partitions"],
            replication_factor=1,  # single local broker
        )
        for env_key, name in topic_names.items()
        if name not in existing
    ]

    if not missing:
        logger.info("All configured topics already exist - nothing to do")
        return 0

    futures = admin.create_topics(missing)
    created = 0
    failed: list[str] = []
    for topic, future in futures.items():
        try:
            future.result(timeout=timeout_s)
            logger.info("Created topic %s", topic)
            created += 1
        except Exception as exc:  # noqa: BLE001 - re-raised below with context
            logger.error("Failed to create topic %s: %s", topic, exc)
            failed.append(topic)

    if failed:
        raise RuntimeError(f"Topic creation failed for: {', '.join(failed)}")
    return created


def main() -> int:
    """Provision all configured topics.

    Returns:
        Process exit code: 0 on success, 1 on failure.
    """
    load_dotenv()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST") or "localhost:9092"
    logger.info("Connecting to Kafka at %s", bootstrap)

    try:
        admin = build_admin_client(bootstrap)
        created = create_missing_topics(admin)
        logger.info("Done. %d topic(s) created.", created)
        return 0
    except (RuntimeError, OSError) as exc:
        logger.error("Topic provisioning failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
