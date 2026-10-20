"""Full Kafka round trip: produce -> consume -> assert transformed rows.

Requires a live broker. These tests are skipped automatically unless
``KAFKA_BOOTSTRAP_SERVERS_HOST`` is set, so a fresh clone's plain
``pytest`` run never fails.

Run explicitly with::

    pytest -m integration
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from confluent_kafka import Consumer

from src.streaming.config import StreamingConfig
from src.streaming.consumer import WeatherEventConsumer
from src.streaming.handlers import process_reading
from src.streaming.producer import WeatherEventProducer

pytestmark = pytest.mark.integration

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "")


@pytest.fixture
def bootstrap() -> str:
    """Provide the host-visible bootstrap address or skip the test."""
    if not BOOTSTRAP:
        pytest.skip("KAFKA_BOOTSTRAP_SERVERS_HOST not set - skipping integration test")
    return BOOTSTRAP


def unique_topic(base: str) -> str:
    """Return a per-test topic name to avoid cross-run interference."""
    return f"{base}.itest.{uuid.uuid4().hex[:8]}"


def create_topic(bootstrap: str, topic: str, partitions: int = 1) -> None:
    """Create a topic, ignoring 'already exists' errors.

    Args:
        bootstrap: Broker address.
        topic: Topic name.
        partitions: Number of partitions.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    for _topic, future in futures.items():
        try:
            future.result(timeout=30.0)
        except Exception as exc:  # noqa: BLE001 - 'already exists' is fine
            if "already exists" not in str(exc).lower():
                raise


def read_dlq(bootstrap: str, topic: str, timeout_s: float = 20.0) -> list[dict[str, Any]]:
    """Read all currently available messages from a topic.

    Args:
        bootstrap: Broker address.
        topic: Topic to read.
        timeout_s: Maximum seconds to wait for messages.

    Returns:
        List of decoded JSON envelopes.
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"itest-dlq-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.assign([])
    from confluent_kafka import TopicPartition

    consumer.assign([TopicPartition(topic, 0, 0)])
    envelopes: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                continue
            envelopes.append(json.loads(msg.value().decode("utf-8")))
    finally:
        consumer.close()
    return envelopes


def wait_for(predicate: Any, timeout_s: float = 60.0) -> bool:
    """Wait until ``predicate()`` is truthy or the timeout expires."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return False


def run_consumer_until(
    consumer: WeatherEventConsumer,
    handler: Any,
    loader: Any,
    done: Any,
    timeout_s: float = 60.0,
) -> int:
    """Run the consumer in a thread until ``done()`` or timeout.

    Args:
        consumer: The consumer under test.
        handler: Record handler.
        loader: Row loader.
        done: Predicate signalling completion.
        timeout_s: Safety timeout.

    Returns:
        Total records processed (0 if the thread did not finish).
    """
    result: list[int] = []

    def target() -> None:
        result.append(consumer.run(handler=handler, loader=loader))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    wait_for(done, timeout_s)
    consumer.stop()
    thread.join(timeout=30.0)
    return result[0] if result else 0


class TestRoundTrip:
    def test_produce_consume_transform(self, bootstrap: str) -> None:
        """Valid events flow raw-topic -> consumer -> transformed rows."""
        topic = unique_topic("weather.readings.raw")
        create_topic(bootstrap, topic)

        config = StreamingConfig.from_env(env={
            "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
            "KAFKA_TOPIC_RAW": topic,
            "KAFKA_BATCH_SIZE": "10",
            "KAFKA_BATCH_TIMEOUT_S": "5",
        })

        # Produce 5 valid readings across 2 stations.
        producer = WeatherEventProducer(bootstrap, topic, client_id="itest-producer")
        now = datetime.now(timezone.utc)
        for i in range(5):
            station = "warsaw-1" if i % 2 == 0 else "krakow-2"
            producer.publish(key=station, payload={
                "station_id": station,
                "observed_at": (now - timedelta(minutes=10 - i)).isoformat(),
                "temp_max_c": 15.0 + i,
                "temp_min_c": 5.0 + i,
                "rain_mm": 0.0,
            })
        assert producer.flush(timeout=30.0) == 0

        # Consume with a capturing loader; stop once all rows arrive.
        loaded: list[dict[str, Any]] = []

        def loader(rows: list[dict[str, Any]]) -> int:
            loaded.extend(rows)
            return len(rows)

        consumer = WeatherEventConsumer(
            bootstrap_servers=bootstrap,
            group_id=f"itest-{uuid.uuid4().hex[:8]}",
            topics=[config.topic_raw],
            dlq_topic=unique_topic("weather.readings.dlq"),
            batch_size=10,
            batch_timeout_s=5.0,
        )
        total = run_consumer_until(
            consumer, process_reading, loader, lambda: len(loaded) >= 5
        )

        assert total == 5
        assert len(loaded) == 5
        stations = {row["station_id"] for row in loaded}
        assert stations == {"warsaw-1", "krakow-2"}
        assert all(isinstance(row["observed_at"], datetime) for row in loaded)

    def test_malformed_record_lands_in_dlq(self, bootstrap: str) -> None:
        """A malformed record goes to the DLQ; valid ones still load."""
        raw_topic = unique_topic("weather.readings.raw")
        dlq_topic = unique_topic("weather.readings.dlq")
        create_topic(bootstrap, raw_topic)
        create_topic(bootstrap, dlq_topic)

        producer = WeatherEventProducer(bootstrap, raw_topic, client_id="itest-producer")
        producer.publish("bad-station", {"not": "a valid event"})
        producer.publish("warsaw-1", {
            "station_id": "warsaw-1",
            "observed_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
            "temp_max_c": 15.0,
            "temp_min_c": 5.0,
            "rain_mm": 0.0,
        })
        assert producer.flush(timeout=30.0) == 0

        loaded: list[dict[str, Any]] = []

        def loader(rows: list[dict[str, Any]]) -> int:
            loaded.extend(rows)
            return len(rows)

        consumer = WeatherEventConsumer(
            bootstrap_servers=bootstrap,
            group_id=f"itest-{uuid.uuid4().hex[:8]}",
            topics=[raw_topic],
            dlq_topic=dlq_topic,
            batch_size=10,
            batch_timeout_s=5.0,
        )
        total = run_consumer_until(
            consumer, process_reading, loader, lambda: len(loaded) >= 1
        )

        assert total == 2
        assert len(loaded) == 1
        assert loaded[0]["station_id"] == "warsaw-1"

        envelopes = read_dlq(bootstrap, dlq_topic)
        assert len(envelopes) == 1
        assert "envelope" not in envelopes[0]
        assert envelopes[0]["error"]
        assert envelopes[0]["raw_value_b64"]
