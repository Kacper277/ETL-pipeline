"""Kafka consumer: micro-batched ingestion into PostgreSQL.

Delivery semantics (at-least-once):

- ``enable.auto.commit=False`` - offsets are never committed implicitly.
- The consumer accumulates a micro-batch, transforms each record, and
  only AFTER a successful database write commits the batch offsets.
- A crash between write and commit (or an aborted batch) causes the
  same records to be delivered again.
- Re-delivery is harmless because the destination table has a UNIQUE
  constraint on ``(station_id, observed_at)`` and the loader uses
  ``ON CONFLICT DO NOTHING``: duplicates collapse idempotently.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka import Producer

from src.streaming.serialization import SerializationError
from src.streaming.schemas import EventValidationError

logger = logging.getLogger(__name__)

Row = dict[str, Any]
Handler = Callable[[bytes | None], Row]
Loader = Callable[[list[Row]], int]


class WeatherEventConsumer:
    """Consume weather readings in micro-batches and load them.

    Attributes:
        batch_size: Maximum records per batch.
        batch_timeout_s: Seconds to wait before flushing a partial batch.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        dlq_topic: str,
        batch_size: int = 50,
        batch_timeout_s: float = 5.0,
    ) -> None:
        """Create the underlying confluent-kafka consumer.

        Configuration rationale:
        - ``enable.auto.commit=False``: offsets are committed manually,
          only after the database write succeeded (at-least-once).
        - ``auto.offset.reset=earliest``: a fresh group starts from the
          beginning instead of silently skipping retained records.
        - ``enable.partition.eof``: EOF events are surfaced so the
          poll loop can react instead of blocking on timeout.

        Args:
            bootstrap_servers: ``host:port`` list for Kafka clients.
            group_id: Consumer group id.
            topics: Topics to subscribe to.
            dlq_topic: Topic for rejected records.
            batch_size: Maximum records per micro-batch.
            batch_timeout_s: Seconds to wait before flushing a partial
                batch.

        Raises:
            KafkaException: If the consumer cannot be constructed.
        """
        self.batch_size = batch_size
        self.batch_timeout_s = batch_timeout_s
        self.topics = topics
        self.dlq_topic = dlq_topic
        self._stopping = False
        self._dlq_producer = Producer({"bootstrap.servers": bootstrap_servers})
        try:
            self._consumer = Consumer({
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": "weather-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
                "session.timeout.ms": 30000,
                "max.poll.interval.ms": 300000,
            })
        except Exception as exc:  # noqa: BLE001 - surfaced with context below
            raise KafkaException(f"Failed to create Kafka consumer: {exc}") from exc

    # ------------------------------------------------------------------
    # Kafka plumbing
    # ------------------------------------------------------------------
    def _log_assignment(
        self, _consumer: Consumer, partitions: list[Any]
    ) -> None:
        """Log which partitions were assigned to this consumer.

        Args:
            _consumer: The consumer instance (unused).
            partitions: Assigned :class:`TopicPartition` objects.
        """
        logger.info(
            "Partitions assigned: %s",
            [(p.topic, p.partition) for p in partitions] or "(none)",
        )

    def subscribe(self) -> None:
        """Subscribe to the configured topics."""
        self._consumer.subscribe(self.topics, on_assign=self._log_assignment)
        logger.info("Subscribed to %s (group offsets commit manually)", self.topics)

    def _collect_batch(self) -> list[Message]:
        """Poll until the batch is full or the timeout expires.

        Returns:
            List of Kafka messages (up to ``batch_size``); messages
            carrying transport-level errors are logged and skipped.
        """
        messages: list[Message] = []
        deadline = time.monotonic() + self.batch_timeout_s
        while len(messages) < self.batch_size and not self._stopping:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            msg = self._consumer.poll(timeout=min(0.2, remaining))
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue
            messages.append(msg)
        return messages

    def _send_to_dlq(self, raw_value: bytes | None, error: str) -> None:
        """Publish a rejected record to the dead-letter topic.

        The envelope wraps the original payload (base64, so binary or
        malformed bytes survive the round trip) together with the
        rejection reason and a timestamp.

        Args:
            raw_value: The original raw message value.
            error: Human-readable rejection reason.
        """
        encoded = base64.b64encode(raw_value or b"").decode("ascii")
        envelope = {
            "rejected_at": datetime.now(timezone.utc).isoformat(),
            "error": error,
            "raw_value_b64": encoded,
        }
        try:
            self._dlq_producer.produce(
                topic=self.dlq_topic,
                key=encoded[:64].encode("ascii", errors="replace"),
                value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
            )
            self._dlq_producer.poll(0)
            logger.warning("Record sent to DLQ %s: %s", self.dlq_topic, error)
        except BufferError:
            self._dlq_producer.poll(0.1)
            logger.error("DLQ queue full - record dropped after logging: %s", error)
        except Exception as exc:  # noqa: BLE001 - DLQ must never kill the batch
            logger.error("Failed to send record to DLQ: %s (original error: %s)", exc, error)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self, handler: Handler, loader: Loader) -> int:
        """Run the consume-transform-load loop until stopped.

        Per-record failures (malformed JSON, validation violations) go
        to the DLQ and do NOT abort the batch. Offsets are committed
        only after a successful database write; on a loader failure the
        batch is abandoned uncommitted so Kafka redelivers it later.

        Args:
            handler: Pure function mapping a raw record to a row dict;
                raises on invalid records.
            loader: Callable persisting a list of row dicts, returning
                the number of rows written.

        Returns:
            Total number of records processed (loaded + DLQ'd).
        """
        self.subscribe()
        processed = 0
        try:
            while not self._stopping:
                batch = self._collect_batch()
                if not batch:
                    continue

                rows: list[Row] = []
                rejected = 0
                for msg in batch:
                    try:
                        rows.append(handler(msg.value()))
                    except (SerializationError, EventValidationError) as exc:
                        rejected += 1
                        self._send_to_dlq(msg.value(), str(exc))

                if rows:
                    written = loader(rows)
                    logger.info(
                        "Batch: %d loaded, %d rejected, %d rows written",
                        len(rows), rejected, written,
                    )
                    self._consumer.commit(asynchronous=False)
                elif rejected:
                    # Nothing to load, but the DLQ'd records must not be
                    # redelivered forever - the rejection itself is the
                    # durable outcome.
                    self._consumer.commit(asynchronous=False)

                processed += len(rows) + rejected
        finally:
            self.close()
        logger.info("Consumer finished. Total records processed: %d", processed)
        return processed

    def stop(self, *_args: Any) -> None:
        """Request a graceful shutdown (safe as a signal handler).

        Args:
            *_args: Signal handler arguments (signal number, frame) -
                ignored; the loop finishes the current batch first.
        """
        logger.info("Shutdown requested - finishing current batch")
        self._stopping = True

    def close(self) -> None:
        """Leave the group cleanly and release all resources."""
        try:
            self._consumer.close()
            logger.info("Consumer closed")
        except Exception as exc:  # noqa: BLE001 - closing must never raise
            logger.error("Error while closing consumer: %s", exc)
