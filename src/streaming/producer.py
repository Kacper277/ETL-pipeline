"""Kafka producer for weather readings.

Publishes JSON-encoded events to a configurable topic. Configured for
durability over latency: idempotent producer, ``acks=all`` and bounded
retries, so a record is either delivered exactly once (per producer
session) or the failure is logged.
"""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from src.streaming.serialization import encode_event

logger = logging.getLogger(__name__)

#: Hard cap on publish retries when the internal queue is full.
_MAX_QUEUE_RETRIES = 100


class PublishError(Exception):
    """Raised when a message could not be queued for delivery."""


class WeatherEventProducer:
    """Publish weather events to a Kafka topic.

    Attributes:
        topic: Destination topic name.
    """

    def __init__(
        self, bootstrap_servers: str, topic: str, client_id: str = "weather-producer"
    ) -> None:
        """Create the underlying confluent-kafka producer.

        Configuration rationale:
        - ``enable.idempotence`` + ``acks=all``: no duplicates from
          internal retries and no silent data loss on broker acks.
        - ``retries`` bounded so failures surface instead of hanging.
        - small ``linger.ms`` batches a demo burst without noticeable
          delay; ``compression.type`` shrinks JSON payloads.

        Args:
            bootstrap_servers: ``host:port`` list for Kafka clients.
            topic: Destination topic name.
            client_id: Client identifier for broker-side logging.

        Raises:
            PublishError: If the producer cannot be constructed.
        """
        self.topic = topic
        try:
            self._producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "linger.ms": 5,
                "compression.type": "lz4",
            })
        except Exception as exc:  # noqa: BLE001 - surfaced with context below
            raise PublishError(f"Failed to create Kafka producer: {exc}") from exc
        logger.info("Producer ready for topic %s", topic)

    def publish(self, key: str, payload: dict[str, Any]) -> None:
        """Encode and produce one message.

        The delivery report is asynchronous; failures are logged by
        :meth:`_delivery_report`. A full internal queue (``BufferError``)
        is handled by polling and retrying with a bounded attempt count.

        Args:
            key: Message key (station id) - controls partition choice
                and preserves per-station ordering.
            payload: JSON-serializable event dict.

        Raises:
            PublishError: If the message cannot be queued after
                repeated retries.
        """
        encoded_key = key.encode("utf-8")
        encoded_value = encode_event(payload)

        for attempt in range(_MAX_QUEUE_RETRIES):
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=encoded_key,
                    value=encoded_value,
                    on_delivery=self._delivery_report,
                )
                return
            except BufferError:
                logger.debug("Queue full (attempt %d) - polling and retrying", attempt + 1)
                self._producer.poll(0.1)
        raise PublishError(f"Could not queue message for key {key!r}: queue stayed full")

    @staticmethod
    def _delivery_report(err: KafkaError | None, msg: Message | None) -> None:
        """Log the asynchronous delivery result of one message.

        Args:
            err: Error instance on failure, ``None`` on success.
            msg: The delivered message (used for topic/partition info).
        """
        if err is not None:
            logger.error("Delivery failed for topic %s: %s", msg.topic() if msg else "?", err)
        elif msg is not None:
            logger.debug("Delivered to %s [%d] @ offset %d",
                         msg.topic(), msg.partition(), msg.offset())

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all queued messages to be delivered.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            Number of messages still unsent after the timeout.
        """
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.warning("%d message(s) still unsent after flush", remaining)
        else:
            logger.info("All messages delivered")
        return remaining
